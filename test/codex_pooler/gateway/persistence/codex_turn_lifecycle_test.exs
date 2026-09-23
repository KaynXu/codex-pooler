defmodule CodexPooler.Gateway.Persistence.CodexTurnLifecycleTest do
  use CodexPooler.DataCase, async: false

  import CodexPooler.PoolerFixtures
  import CodexPooler.AccountingTestSupport

  alias CodexPooler.Accounting

  alias CodexPooler.Gateway.Payloads.RequestOptions
  alias CodexPooler.Gateway.Persistence.{CodexSession, CodexTurn, SessionContinuity}
  alias CodexPooler.Gateway.Persistence.SessionContinuity.OwnerWitness
  alias CodexPooler.Gateway.Persistence.SessionContinuity.TurnLifecycle
  alias CodexPooler.Gateway.Runtime.Finalization.AttemptSettlement
  alias CodexPooler.Gateway.Websocket

  setup do
    %{pool: pool, api_key: api_key} = active_api_key_fixture()
    auth = %{pool: pool, api_key: api_key}

    {:ok, session} =
      Websocket.start_codex_session(auth, %{accepted_turn_state: Ecto.UUID.generate()})

    request = request_fixture(auth, %{status: "in_progress", completed_at: nil})

    {:ok, turn} =
      SessionContinuity.start_codex_turn(session, request, RequestOptions.for_websocket(%{}))

    %{request: request, turn: turn}
  end

  test "persisted in-progress turns become terminal and cannot be finalized twice", %{
    request: request,
    turn: turn
  } do
    assert CodexTurn.in_progress?(Repo.reload!(turn))
    assert CodexTurn.in_progress?("in_progress")

    result = {:ok, %{request: request}}
    assert ^result = SessionContinuity.complete_codex_turn(result, "failed", :upstream_timeout)
    completed = Repo.reload!(turn)
    refute CodexTurn.in_progress?(completed)
    assert completed.status == "failed"
    assert completed.error_code == "upstream_timeout"
    assert %DateTime{} = completed.completed_at

    assert ^result = SessionContinuity.complete_codex_turn(result, "succeeded", nil)
    assert Repo.reload!(turn) == completed
  end

  test "the durable session pin follows the attempt that served, not the last one dispatched to" do
    %{pool: pool, api_key: api_key} = active_api_key_fixture()
    auth = %{pool: pool, api_key: api_key}
    %{assignment: dispatched} = upstream_assignment_fixture(pool)
    %{assignment: served} = upstream_assignment_fixture(pool)

    {:ok, session} =
      Websocket.start_codex_session(auth, %{accepted_turn_state: Ecto.UUID.generate()})

    request = request_fixture(auth, %{status: "in_progress", completed_at: nil})

    {:ok, _turn} =
      SessionContinuity.start_codex_turn(
        session,
        request,
        RequestOptions.for_websocket(%{pool_upstream_assignment_id: dispatched.id})
      )

    # Turn start records liveness, not routing. It used to write the candidate
    # it was about to dispatch to, with no guard, so a turn refused by every
    # candidate left the session pinned to the last one it tried.
    assert is_nil(Repo.reload!(session).pool_upstream_assignment_id)

    attempt = attempt_fixture(request, served, %{status: "succeeded"})

    assert {:ok, _result} =
             SessionContinuity.complete_codex_turn(
               {:ok, %{request: request, attempt: attempt}},
               CodexTurn.succeeded_status(),
               nil
             )

    assert Repo.reload!(session).pool_upstream_assignment_id == served.id
  end

  test "an overload refusal cannot replace the durable assignment that served" do
    setup = accounting_setup()
    %{pool: pool, auth: auth} = setup
    %{assignment: prior} = active_upstream_assignment_fixture(pool)
    %{assignment: refused} = active_upstream_assignment_fixture(pool)

    opts =
      RequestOptions.build(
        %{session_header: Ecto.UUID.generate()},
        "/backend-api/codex/responses",
        %{}
      )

    {:ok, session} = SessionContinuity.start_codex_session(auth, opts)

    session =
      session |> Ecto.Changeset.change(pool_upstream_assignment_id: prior.id) |> Repo.update!()

    {:ok, witness} = OwnerWitness.new(session)

    opts =
      opts
      |> RequestOptions.put_continuity(codex_session: session)
      |> RequestOptions.put_session_owner_witness(witness)

    {:ok, reserved} =
      Accounting.reserve(
        auth,
        setup.model,
        %{"model" => setup.model.exposed_model_id, "max_output_tokens" => 10},
        %{correlation_id: Ecto.UUID.generate(), transport: "http_json"}
      )

    request = reserved.request
    {:ok, _turn} = SessionContinuity.start_codex_turn(session, request, opts)
    {:ok, attempt} = Accounting.create_attempt(request, refused, %{})

    assert {:ok, _result} =
             AttemptSettlement.finalize_failure(
               request,
               attempt,
               %{last_error_code: "server_is_overloaded", status_code: 503},
               witness
             )

    assert Repo.get!(CodexSession, session.id).pool_upstream_assignment_id == prior.id
  end

  test "terminal, absent and unknown statuses are never in progress" do
    for status <- ["succeeded", "failed", "interrupted", nil, "unknown"] do
      refute CodexTurn.in_progress?(status)
      refute CodexTurn.in_progress?(%CodexTurn{status: status})
    end
  end

  test "failed lifecycle results leave an in-progress turn untouched", %{turn: turn} do
    result = {:error, :reservation_failed}
    assert ^result = SessionContinuity.complete_codex_turn(result, "failed", :upstream_timeout)
    assert Repo.reload!(turn) == turn
  end

  test "legacy visibility is idempotent and malformed ownership cannot authorize output", %{
    request: request,
    turn: turn
  } do
    assert :ok = TurnLifecycle.mark_codex_turn_visible(request)
    visible = Repo.reload!(turn)
    assert %DateTime{} = visible.first_visible_output_at
    assert :ok = TurnLifecycle.mark_codex_turn_visible(request)
    assert Repo.reload!(turn) == visible

    for invalid <- [
          nil,
          %{},
          %{id: Ecto.UUID.generate(), request_id: request.id, replay_generation: "0"}
        ] do
      assert {:error, :stale_generation} =
               TurnLifecycle.authorize_codex_turn_visibility(request, invalid)

      assert Repo.reload!(turn) == visible
    end

    assert :ok = TurnLifecycle.mark_codex_turn_visible(nil)
    assert Repo.reload!(turn) == visible
  end
end
