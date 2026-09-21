defmodule CodexPooler.Files.UploadLifecycleTest do
  use CodexPooler.DataCase, async: false

  import CodexPooler.PoolerFixtures

  alias CodexPooler.Files.{FileRecord, RequestMetadata, UploadLifecycle}

  setup do
    auth = active_api_key_fixture()
    now = DateTime.utc_now()

    opts =
      RequestMetadata.build(%{now: now, transport: "http_json"}, "/backend-api/files/uploaded")

    %{auth: auth, now: now, opts: opts}
  end

  test "invalid principals cannot mutate lifecycle state", %{now: now, opts: opts} do
    assert {:error, %{status: 400}} =
             UploadLifecycle.mark_uploaded_or_prepare_finalize(%{}, "file-missing", opts, now)

    assert {:error, %{status: 400}} =
             UploadLifecycle.record_finalize_result(%{}, "file-missing", opts, success())

    assert {:error, %{status: 400}} =
             UploadLifecycle.record_upload_failure(%{}, "file-missing", failure())

    assert Repo.aggregate(FileRecord, :count) == 0
  end

  test "finalize and upload lookup are scoped to the owning api key", %{
    auth: auth,
    now: now,
    opts: opts
  } do
    file = file(auth, now)
    other = active_api_key_fixture()

    for stranger <- [other, %{auth | api_key: other.api_key}] do
      assert {:error, %{code: :file_not_found}} =
               UploadLifecycle.mark_uploaded_or_prepare_finalize(
                 stranger,
                 file.file_id,
                 opts,
                 now
               )

      assert {:error, %{code: :file_not_found}} =
               UploadLifecycle.record_finalize_result(stranger, file.file_id, opts, success())
    end

    assert Repo.reload!(file).status == "pending_upload"
  end

  test "local upload completion is idempotent and records every acknowledgement", %{
    auth: auth,
    now: now,
    opts: opts
  } do
    file = file(auth, now)

    assert {:ok, %{file: completed, request: first}} =
             UploadLifecycle.mark_uploaded_or_prepare_finalize(auth, file.file_id, opts, now)

    assert completed.uploaded_at == now

    assert {:ok, %{file: repeated, request: second}} =
             UploadLifecycle.mark_uploaded_or_prepare_finalize(
               auth,
               file.file_id,
               opts,
               DateTime.add(now, 1)
             )

    assert repeated.uploaded_at == now
    refute first.id == second.id
    assert second.status == "succeeded"
  end

  test "upstream pending files prepare finalize without completing locally", context do
    file = upstream_file(context)

    assert {:finalize, pending} =
             UploadLifecycle.mark_uploaded_or_prepare_finalize(
               context.auth,
               file.file_id,
               context.opts,
               context.now
             )

    assert pending.status == "pending_upload"
    assert Repo.reload!(file).uploaded_at == nil
  end

  test "late finalize results respect expiry and terminal states", context do
    for status <- ["deleted", "abandoned"] do
      file = upstream_file(context, %{status: status})

      assert {:error, %{code: :file_not_uploadable}} =
               UploadLifecycle.record_finalize_result(
                 context.auth,
                 file.file_id,
                 context.opts,
                 success()
               )

      assert Repo.reload!(file).status == status

      assert {:error, %{code: :file_not_uploadable}} =
               UploadLifecycle.mark_uploaded_or_prepare_finalize(
                 context.auth,
                 file.file_id,
                 context.opts,
                 context.now
               )
    end

    for operation <- [:prepare, :finalize] do
      file = upstream_file(context, %{expires_at: context.now})

      result =
        if operation == :prepare,
          do:
            UploadLifecycle.mark_uploaded_or_prepare_finalize(
              context.auth,
              file.file_id,
              context.opts,
              context.now
            ),
          else:
            UploadLifecycle.record_finalize_result(
              context.auth,
              file.file_id,
              context.opts,
              success()
            )

      assert {:error, %{code: :file_expired}} = result
      assert Repo.reload!(file).deleted_at == context.now
      assert Repo.reload!(file).status == "expired"
    end
  end

  test "a late failed finalize cannot abandon an already uploaded file", context do
    file = upstream_file(context)

    assert {:ok, %{file: completed}} =
             UploadLifecycle.record_finalize_result(
               context.auth,
               file.file_id,
               context.opts,
               success()
             )

    assert {:ok, %{file: repeated}} =
             UploadLifecycle.record_finalize_result(
               context.auth,
               file.file_id,
               context.opts,
               {:error, failure()}
             )

    assert repeated.uploaded_at == completed.uploaded_at
    assert Repo.reload!(file).status == "uploaded"
  end

  test "local pending finalize does not depend on an upstream result", context do
    file = file(context.auth, context.now)

    assert {:ok, %{file: completed}} =
             UploadLifecycle.record_finalize_result(
               context.auth,
               file.file_id,
               context.opts,
               {:error, failure()}
             )

    assert completed.status == "uploaded"
  end

  test "all documented successful finalization statuses preserve route metadata", context do
    for status <- ["success", "UPLOADED", "Completed"] do
      file = upstream_file(context)
      {:ok, bridge} = success(status)
      bridge = Map.put(bridge, :route_metadata, %{"routing" => %{"route_class" => "file_upload"}})

      assert {:ok, %{file: completed, request: request}} =
               UploadLifecycle.record_finalize_result(
                 context.auth,
                 file.file_id,
                 context.opts,
                 {:ok, bridge}
               )

      assert completed.status == "uploaded"
      assert Repo.reload!(request).request_metadata["routing"]["route_class"] == "file_upload"
    end
  end

  test "incomplete upstream bodies abandon the file", context do
    for body <- [
          nil,
          %{},
          %{"status" => "success", "download_url" => "  "},
          %{"status" => "retry", "download_url" => "https://example.com/file"}
        ] do
      file = upstream_file(context)

      assert {:error, %{code: :upstream_file_finalize_incomplete}} =
               UploadLifecycle.record_finalize_result(
                 context.auth,
                 file.file_id,
                 context.opts,
                 {:ok, %{body: body}}
               )

      assert Repo.reload!(file).status == "abandoned"
      assert Repo.reload!(file).finalize_status == "failed"
    end
  end

  test "retry timeout keeps the pending upload available for another finalize", context do
    file = upstream_file(context)
    body = %{"status" => "retry"}

    assert {:ok, %{file: pending, request: request, body: ^body}} =
             UploadLifecycle.record_finalize_result(
               context.auth,
               file.file_id,
               context.opts,
               {:retry_timeout, %{body: body}}
             )

    assert pending.status == "pending_upload"
    assert request.status == "failed"

    assert {:ok, %{file: completed}} =
             UploadLifecycle.record_finalize_result(
               context.auth,
               file.file_id,
               context.opts,
               success()
             )

    assert completed.status == "uploaded"
  end

  test "bridge failure abandons the file and removes internal upstream details", context do
    file = upstream_file(context)

    assert {:error, result} =
             UploadLifecycle.record_finalize_result(
               context.auth,
               file.file_id,
               context.opts,
               {:error, failure()}
             )

    refute Map.has_key?(result, :upstream)
    assert Repo.reload!(file).status == "abandoned"
  end

  test "upload failure is scoped and persists abandonment with caller time", context do
    file = file(context.auth, context.now)
    other = active_api_key_fixture()
    assert {:error, _} = UploadLifecycle.record_upload_failure(other, file.file_id, failure())
    assert Repo.reload!(file).status == "pending_upload"

    assert {:error, result} =
             UploadLifecycle.record_upload_failure(context.auth, file.file_id, failure(), now: context.now)

    refute Map.has_key?(result, :upstream)
    assert Repo.reload!(file).status == "abandoned"
    assert Repo.reload!(file).updated_at == context.now
  end

  test "accounting failure rolls back file completion and abandonment", context do
    auth = context.auth
    opts = %{context.opts | transport: "unsupported"}

    for operation <- [:complete, :fail] do
      file = file(context.auth, context.now)

      assert_raise Ecto.ConstraintError, fn ->
        _result =
          if operation == :complete,
            do:
              UploadLifecycle.mark_uploaded_or_prepare_finalize(
                auth,
                file.file_id,
                opts,
                context.now
              ),
            else: UploadLifecycle.record_upload_failure(auth, file.file_id, failure(), opts)
      end

      assert Repo.reload!(file).status == "pending_upload"
    end
  end

  defp upstream_file(context, attrs \\ %{}) do
    %{assignment: assignment} = upstream_assignment_fixture(context.auth.pool)
    file(context.auth, context.now, Map.put(attrs, :pool_upstream_assignment_id, assignment.id))
  end

  defp file(auth, now, attrs \\ %{}) do
    %FileRecord{}
    |> FileRecord.changeset(
      Map.merge(
        %{
          pool_id: auth.pool.id,
          api_key_id: auth.api_key.id,
          file_id: "file-#{System.unique_integer([:positive])}",
          purpose: "user_data",
          filename: "sample.txt",
          byte_size: 1,
          status: "pending_upload",
          finalize_status: "pending",
          expires_at: DateTime.add(now, 60),
          metadata: %{},
          created_at: now,
          updated_at: now
        },
        attrs
      )
    )
    |> Repo.insert!()
  end

  defp success(status \\ "success"),
    do: {:ok, %{body: %{"status" => status, "download_url" => "https://example.com/file"}}}

  defp failure,
    do: %{status: 502, code: :upstream_failed, message: "upload failed", upstream: %{}}
end
