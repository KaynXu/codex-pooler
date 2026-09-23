defmodule CodexPooler.Files.RequestLogContractTest do
  use CodexPooler.DataCase, async: false
  import CodexPooler.PoolerFixtures
  import ExUnit.CaptureLog
  alias CodexPooler.AccountingBoundaryTrace
  alias CodexPooler.Files.{RequestLog, RequestMetadata}

  test "file operations never persist raw idempotency keys and retain bounded client metadata" do
    auth = active_api_key_fixture()

    for endpoint <- [
          "/backend-api/files",
          "/backend-api/files/uploaded",
          "/v1/files",
          "/v1/files/content",
          "/v1/files/delete",
          "/backend-api/codex/responses"
        ],
        client_id <- ["  ", "  sample-client  ", String.duplicate("a", 180)] do
      metadata =
        RequestMetadata.build(
          %{
            transport: "http_json",
            idempotency_key: "synthetic-private-key",
            client_request_id: client_id,
            request_bytes: 12,
            upload_bytes: 8,
            route_class: "file_upload"
          },
          endpoint
        )

      assert {:ok, request} =
               RequestLog.record_file_request(auth, "succeeded", 200, metadata, %{
                 "operation" => "create"
               })

      request = Repo.reload!(request)
      refute inspect(request) =~ "synthetic-private-key"
      assert request.request_metadata["request"]["request_bytes"] == 12
      assert request.request_metadata["request"]["upload_bytes"] == 8
      expected = client_id |> String.trim() |> String.slice(0, 160)

      if expected == "",
        do: refute(Map.has_key?(request.request_metadata, "client_request_id")),
        else: assert(request.request_metadata["client_request_id"] == expected)
    end
  end

  test "file operations omit the raw idempotency key at the accounting boundary" do
    auth = active_api_key_fixture()
    raw_key = "file-private-key-#{System.unique_integer([:positive])}"

    metadata =
      RequestMetadata.build(
        %{
          transport: "http_json",
          idempotency_key: raw_key,
          route_class: "file_upload"
        },
        "/backend-api/files"
      )

    {result, [_auth, attrs]} =
      AccountingBoundaryTrace.capture_call(
        {CodexPooler.Accounting, :record_metadata_request, 2},
        fn ->
          RequestLog.record_file_request(auth, "succeeded", 200, metadata, %{
            "operation" => "create"
          })
        end
      )

    assert {:ok, _request} = result
    refute Map.has_key?(attrs, :idempotency_key)
    refute inspect(attrs, limit: :infinity, printable_limit: :infinity) =~ raw_key
  end

  test "bridge metadata merges into persisted request and absent metadata is a no-op" do
    auth = active_api_key_fixture()
    metadata = RequestMetadata.build(%{transport: "http_json"}, "/v1/files")

    assert {:ok, request} =
             RequestLog.record_file_request(auth, "succeeded", 200, metadata, %{
               "operation" => "create"
             })

    assert {:ok, merged} =
             RequestLog.merge_bridge_route_metadata(request, %{
               route_metadata: %{"routing" => %{"route_class" => "file_upload"}}
             })

    assert Repo.reload!(merged).request_metadata["routing"]["route_class"] == "file_upload"
    assert merged.request_metadata["operation"] == "create"

    for result <- [nil, %{}, %{route_metadata: %{}}, %{route_metadata: nil}] do
      assert {:ok, ^merged} = RequestLog.merge_bridge_route_metadata(merged, result)
    end
  end

  test "bridge extraction excludes unrelated upstream data and handles missing maps" do
    assert %{"routing" => %{"route_class" => "file_upload"}} =
             RequestLog.bridge_route_metadata(%{
               upstream: %{
                 "routing" => %{"route_class" => "file_upload"},
                 "private" => "sentinel"
               }
             })

    for value <- [nil, %{}, %{upstream: nil}],
        do: assert(RequestLog.bridge_route_metadata(value) == %{})
  end

  test "invalid bridge request returns sanitized accounting failure" do
    log =
      capture_log(fn ->
        assert {:error, %{status: 500, code: "gateway_accounting_failed"}} =
                 RequestLog.merge_bridge_route_metadata(nil, %{
                   route_metadata: %{"operation" => "create"}
                 })
      end)

    assert log =~ "operation=merge_file_request_metadata"
  end
end
