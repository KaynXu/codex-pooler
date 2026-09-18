defmodule CodexPooler.Accounting.DeadExecutionResendRecoveryTest do
  use CodexPooler.DataCase, async: false

  import CodexPooler.AccountingTestSupport

  alias CodexPooler.Accounting

  alias CodexPooler.Accounting.{
    Attempt,
    ClientRetry,
    LedgerEntry,
    Request,
    RequestClientRetryLink
  }

  alias CodexPooler.Gateway.Persistence.{CodexSession, CodexTurn}
  alias CodexPooler.Repo

  @endpoint "/backend-api/codex/responses"
  @retry_prefix "codex-request-retry:"
  @detection_timeout_ms 15_000

  test "an exact terminal proof recovers the live predecessor inside the released-client resend" do
    setup = accounting_setup()
    session = insert_session!(setup)

    witness =
      ClientRetry.original_witness!(
        :crypto.strong_rand_bytes(32),
        setup.api_key.runtime_revocation_epoch
      )

    claim = "codex-turn:" <> Base.url_encode64(:crypto.strong_rand_bytes(32), padding: false)

    opts = %{
      endpoint: @endpoint,
      correlation_id: claim,
      codex_session: session,
      native_client_retry_witness: witness
    }

    assert {:ok, %{request: claimed}} =
             Accounting.claim_websocket_turn(setup.auth, setup.model, opts)

    assert {:ok, %{request: request}} =
             Accounting.reserve(
               setup.auth,
               setup.model,
               %{"model" => setup.model.exposed_model_id, "input" => []},
               %{
                 endpoint: @endpoint,
                 transport: "websocket",
                 correlation_id: claim,
                 turn_claim: claimed
               }
             )

    attempt = create_dead_attempt!(setup, request)
    turn = insert_turn!(session, request, attempt)
    CodexPooler.ExecutionProofSupport.publish_terminal!(attempt)

    wrong_witness =
      ClientRetry.original_witness!(
        :crypto.strong_rand_bytes(32),
        setup.api_key.runtime_revocation_epoch
      )

    assert {:error, %{code: :duplicate_request, resend_disposition: :terminal_predecessor}} =
             Accounting.claim_websocket_turn(
               setup.auth,
               setup.model,
               %{opts | native_client_retry_witness: wrong_witness}
             )

    assert %Request{status: "in_progress", completed_at: nil} = Repo.reload!(request)
    assert %Attempt{status: "in_progress", completed_at: nil} = Repo.reload!(attempt)
    assert %CodexTurn{status: "in_progress", completed_at: nil} = Repo.reload!(turn)
    assert ledger_kinds(request.id) == ["reservation"]

    attach_outcome_handler!()

    assert {:ok,
            %{
              request: successor,
              client_resend: %{
                predecessor_request_id: predecessor_id,
                predecessor_shape: :task_exception
              }
            }} = Accounting.claim_websocket_turn(setup.auth, setup.model, opts)

    assert predecessor_id == request.id
    assert String.starts_with?(successor.correlation_id, @retry_prefix)

    assert_receive {:dead_resend_outcome,
                    %{
                      outcome: "interrupted",
                      downstream_transport: "websocket",
                      upstream_transport: "websocket"
                    }, false}

    assert_recovered!(request, attempt, turn)
    assert ledger_kinds(request.id) == ["release", "reservation", "settlement"]

    assert Repo.aggregate(
             from(link in RequestClientRetryLink,
               where: link.predecessor_request_id == ^request.id
             ),
             :count
           ) == 1

    assert Repo.aggregate(from(r in Request, where: r.pool_id == ^setup.pool.id), :count) == 2
  end

  defp create_dead_attempt!(setup, request) do
    parent = self()

    owner_pid =
      spawn(fn ->
        assert {:ok, attempt} = Accounting.create_attempt(request, setup.assignment)
        send(parent, {:dead_resend_attempt, self(), attempt})

        receive do
          :finish -> :ok
        end
      end)

    on_exit(fn -> if Process.alive?(owner_pid), do: Process.exit(owner_pid, :kill) end)
    assert_receive {:dead_resend_attempt, ^owner_pid, %Attempt{} = attempt}, @detection_timeout_ms
    monitor = Process.monitor(owner_pid)
    Process.exit(owner_pid, :kill)
    assert_receive {:DOWN, ^monitor, :process, ^owner_pid, :killed}, @detection_timeout_ms
    attempt
  end

  defp insert_session!(setup) do
    now = db_now()

    Repo.insert!(%CodexSession{
      pool_id: setup.pool.id,
      api_key_id: setup.api_key.id,
      session_key: "dead-resend-#{System.unique_integer([:positive, :monotonic])}",
      pool_upstream_assignment_id: setup.assignment.id,
      status: "active",
      created_at: now,
      updated_at: now
    })
  end

  defp insert_turn!(session, request, attempt) do
    now = db_now()

    Repo.insert!(%CodexTurn{
      codex_session_id: session.id,
      request_id: request.id,
      turn_sequence: 1,
      transport_kind: "websocket",
      semantic_turn_digest: :crypto.strong_rand_bytes(32),
      status: "in_progress",
      final_attempt_id: attempt.id,
      started_at: now,
      created_at: now,
      updated_at: now
    })
  end

  defp attach_outcome_handler! do
    id = "dead-resend-#{System.unique_integer([:positive, :monotonic])}"

    :ok =
      :telemetry.attach(
        id,
        [:codex_pooler, :gateway, :stream, :outcome],
        fn _event, _measurements, metadata, test_pid ->
          send(test_pid, {:dead_resend_outcome, metadata, Repo.in_transaction?()})
        end,
        self()
      )

    on_exit(fn -> :telemetry.detach(id) end)
  end

  defp assert_recovered!(request, attempt, turn) do
    assert %Request{status: "failed", last_error_code: "dead_execution_recovered"} =
             Repo.reload!(request)

    assert %Attempt{
             status: "failed",
             network_error_code: "dead_execution_recovered",
             usage_status: "usage_unknown"
           } = Repo.reload!(attempt)

    assert %CodexTurn{status: "interrupted", error_code: "dead_execution_recovered"} =
             Repo.reload!(turn)
  end

  defp ledger_kinds(request_id) do
    Repo.all(
      from entry in LedgerEntry,
        where: entry.request_id == ^request_id,
        order_by: [asc: entry.entry_kind],
        select: entry.entry_kind
    )
  end

  defp db_now do
    %{rows: [[now]]} = Repo.query!("SELECT clock_timestamp()", [])
    now
  end
end
