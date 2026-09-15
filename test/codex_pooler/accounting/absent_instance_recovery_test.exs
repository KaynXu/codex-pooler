defmodule CodexPooler.Accounting.AbsentInstanceRecoveryTest do
  use CodexPooler.DataCase, async: false

  import CodexPooler.AccountingTestSupport
  import CodexPooler.UnboxedFixture, only: [register_unboxed_cleanup!: 1, run_unboxed: 1]
  import CodexPoolerWeb.Runtime.BackendCodexTestSupport, only: [cleanup_unboxed_pool!: 1]

  alias CodexPooler.Accounting
  alias CodexPooler.Accounting.{Attempt, Request}
  alias CodexPooler.Gateway.Persistence.{CodexSession, CodexTurn}
  alias CodexPooler.Gateway.Persistence.SessionContinuity
  alias CodexPooler.Gateway.Websocket
  alias CodexPooler.Platform.ExecutionIdentity
  alias CodexPooler.Platform.{InstanceHeartbeat, InstancePresence}
  alias CodexPooler.Platform.InstancePresence.{Identity, Instance}
  alias CodexPooler.Repo
  alias CodexPooler.Upstreams.Schemas.PoolUpstreamAssignment

  setup do
    boot_id = Identity.boot_id()
    on_exit(fn -> :persistent_term.put({Identity, :boot_id}, boot_id) end)
    :ok
  end

  describe "recover_absent_instance_attempts/2" do
    test "live completion after a stale heartbeat keeps request turn and ledger coherent" do
      setup = accounting_setup()
      now = now()
      stale = DateTime.add(now, -180, :second)

      {:ok, session} =
        Websocket.start_codex_session(setup.auth, %{
          accepted_turn_state: Ecto.UUID.generate()
        })

      {:ok, %{request: request}} =
        Accounting.reserve(
          setup.auth,
          setup.model,
          %{"model" => setup.model.exposed_model_id, "max_output_tokens" => 10},
          %{now: stale, correlation_id: unique_correlation_id(), transport: "http_sse"}
        )

      {:ok, attempt} = Accounting.create_attempt(request, setup.assignment, %{now: stale})
      {:ok, turn} = Websocket.start_codex_turn(session, request)
      {:ok, _} = InstancePresence.record_heartbeat(Identity.local(), stale)
      assert ExecutionIdentity.status(attempt) == :alive

      assert {:ok, %{absent_instance_attempts_recovered: 0}} =
               Accounting.recover_absent_instance_attempts(now)

      assert ledger_kinds(request) == ["reservation"]

      result =
        Accounting.finalize_request(request, attempt, %{
          request_status: "succeeded",
          attempt_status: "succeeded",
          response_status_code: 200,
          usage: %{status: "usage_unknown", source: "synthetic_completion"}
        })

      assert {:ok, _} =
               SessionContinuity.complete_codex_turn(
                 result,
                 "succeeded",
                 nil,
                 attempt,
                 nil
               )

      assert Repo.reload!(request).status == "succeeded"
      assert Repo.reload!(attempt).status == "succeeded"
      assert Repo.reload!(turn).status == "succeeded"
      assert ledger_kinds(request) == ["release", "reservation", "settlement"]
    end

    test "a stale observer cannot recover a stale legacy owner" do
      setup = accounting_setup()
      now = now()
      stale = DateTime.add(now, -180, :second)
      owner = Identity.new("sample-owner@remote", Ecto.UUID.generate())
      {:ok, _} = InstancePresence.record_heartbeat(owner, stale)
      {:ok, _} = InstancePresence.record_heartbeat(Identity.local(), stale)

      %{request: request, attempt: attempt} =
        dispatch_open_turn!(setup, stale, %{
          owner_instance_id: owner.node_name,
          owner_instance_boot_id: owner.boot_id,
          owner_process_id: nil,
          owner_execution_id: nil
        })

      assert {:ok, %{absent_instance_attempts_recovered: 0}} =
               Accounting.recover_absent_instance_attempts(now)

      assert Repo.reload!(request).status == "in_progress"
      assert Repo.reload!(attempt).status == "in_progress"
      assert ledger_kinds(request) == ["reservation"]

      {:ok, _} = InstancePresence.record_heartbeat()

      assert {:ok, %{absent_instance_attempts_recovered: 1}} =
               Accounting.recover_absent_instance_attempts(now)
    end

    test "an unreachable exact execution defers early recovery and retains the stale fallback" do
      setup = accounting_setup()
      now = now()
      stale = DateTime.add(now, -180, :second)
      owner = Identity.new("sample-owner@unreachable", Ecto.UUID.generate())
      {:ok, _} = InstancePresence.record_heartbeat(owner, stale)
      {:ok, _} = InstancePresence.record_heartbeat()
      %{request: request, attempt: attempt, turn: turn} = dispatch_open_turn!(setup, stale)
      # Adversarial mismatch: retain the real execution UUID/PID but make its
      # owner unreachable. This is an unknown-evidence control, not live proof.
      attempt =
        attempt
        |> Ecto.Changeset.change(
          owner_instance_id: owner.node_name,
          owner_instance_boot_id: owner.boot_id
        )
        |> Repo.update!()

      assert ExecutionIdentity.status(attempt) == :unknown

      assert {:ok, %{absent_instance_attempts_recovered: 0}} =
               Accounting.recover_absent_instance_attempts(now)

      assert Repo.reload!(request).status == "in_progress"
      assert Repo.reload!(attempt).status == "in_progress"
      assert Repo.reload!(turn).status == "in_progress"
      assert ledger_kinds(request) == ["reservation"]

      assert {:ok, %{stale_reservations_settled: 1}} =
               Accounting.recover_stale_reservations(DateTime.add(now, 7, :hour))

      assert Repo.reload!(request).last_error_code == "stale_reservation_recovered"
    end

    test "an instance that restarts in place does not keep its predecessor's legacy orphan alive" do
      setup = accounting_setup()
      now = now()
      dispatched_at = DateTime.add(now, -10, :minute)

      first = start_instance!(:absent_recovery_restart_first)

      %{request: request, attempt: attempt, turn: turn} =
        dispatch_open_turn!(setup, dispatched_at)

      # Legacy attempts lack an exact execution identity; their retained policy
      # uses stale presence and a fresh observer, unlike modern attempts.
      attempt =
        attempt
        |> Ecto.Changeset.change(owner_execution_id: nil, owner_process_id: nil)
        |> Repo.update!()

      # The owner came from the running instance, not from the test.
      assert attempt.owner_instance_id == first.node_name
      assert attempt.owner_instance_boot_id == first.boot_id

      assignment_before = Repo.get!(PoolUpstreamAssignment, setup.assignment.id)

      # The VM is halted from inside, so no drain runs and its row keeps
      # whatever it was last refreshed with. Place that refresh ten minutes back
      # through the same upsert the heartbeat uses rather than waiting out the
      # liveness window.
      {:ok, _stale} = InstancePresence.record_heartbeat(first, dispatched_at)
      end_instance!(:absent_recovery_restart_first)

      # Kubernetes restarts the container in place: same pod, same address, so
      # the same node name, with a new VM behind it.
      second = start_instance!(:absent_recovery_restart_second)
      assert second.node_name == first.node_name
      assert second.boot_id != first.boot_id

      assert {:ok, %{absent_instance_attempts_recovered: 1}} =
               Accounting.recover_absent_instance_attempts(now)

      # The successor must not have refreshed the row that proves its
      # predecessor is gone.
      assert Repo.get!(Instance, first.instance_id).last_seen_at == dispatched_at

      assert DateTime.compare(
               Repo.get!(Instance, second.instance_id).last_seen_at,
               dispatched_at
             ) == :gt

      assert %Request{
               status: "failed",
               usage_status: "usage_unknown",
               response_status_code: 499,
               last_error_code: "absent_instance_recovered",
               completed_at: %DateTime{}
             } = Repo.get!(Request, request.id)

      assert %Attempt{
               status: "failed",
               usage_status: "usage_unknown",
               network_error_code: "absent_instance_recovered",
               completed_at: %DateTime{}
             } = recovered_attempt = Repo.reload!(attempt)

      assert recovered_attempt.owner_instance_id == first.node_name
      assert recovered_attempt.owner_instance_boot_id == first.boot_id

      assert %CodexTurn{status: "interrupted", error_code: "absent_instance_recovered"} =
               Repo.reload!(turn)

      assert ledger_kinds(request) == ["release", "reservation", "settlement"]

      # A recovery is our own lifecycle event: the upstream said nothing at all,
      # so its assignment must come out of the pass untouched.
      assert Repo.get!(PoolUpstreamAssignment, setup.assignment.id) == assignment_before

      assert {:ok, %{absent_instance_attempts_recovered: 0}} =
               Accounting.recover_absent_instance_attempts(now)
    end

    # findings#207: an attempt that records its executor is never settled on
    # stale presence alone (findings#214), and the cleanup role has no BEAM
    # connectivity to app pods in production. A successor incarnation publishing
    # under the same node name is the distribution-free proof that the previous
    # VM is gone: one node name is held by one VM at a time.
    test "a successor incarnation under the same node name proves a modern orphan's executor dead" do
      setup = accounting_setup()
      now = now()
      dispatched_at = DateTime.add(now, -10, :minute)
      node_name = "codex_pooler@10.42.#{System.unique_integer([:positive])}.7"
      first = Identity.new(node_name, unique_boot_id())
      second = Identity.new(node_name, unique_boot_id())

      %{request: request, attempt: attempt, turn: turn} =
        modern_orphan!(setup, first, dispatched_at)

      # The predecessor's row is stale; no successor has published yet, so the
      # executor is unknown and the row is left alone.
      {:ok, _stale} = InstancePresence.record_heartbeat(first, dispatched_at)
      {:ok, _} = InstancePresence.record_heartbeat()
      assert ExecutionIdentity.status(attempt) == :unknown
      refute InstancePresence.superseded?(first)

      assert {:ok, %{absent_instance_attempts_recovered: 0}} =
               Accounting.recover_absent_instance_attempts(now)

      assert Repo.reload!(attempt).status == "in_progress"

      # The container restarts in place: same node name, new incarnation,
      # started after the predecessor. Its own open attempt must stay untouched.
      {:ok, _fresh} = InstancePresence.record_heartbeat(second, now)

      %{attempt: successor_attempt} =
        modern_orphan!(setup, second, DateTime.add(now, -9, :minute))

      assert InstancePresence.superseded?(first)
      refute InstancePresence.superseded?(second)

      assert {:ok, %{absent_instance_attempts_recovered: 1}} =
               Accounting.recover_absent_instance_attempts(now)

      assert %Request{status: "failed", last_error_code: "absent_instance_recovered"} =
               Repo.get!(Request, request.id)

      assert %Attempt{status: "failed", network_error_code: "absent_instance_recovered"} =
               Repo.reload!(attempt)

      assert %CodexTurn{status: "interrupted", error_code: "absent_instance_recovered"} =
               Repo.reload!(turn)

      assert ledger_kinds(request) == ["release", "reservation", "settlement"]
      assert Repo.reload!(successor_attempt).status == "in_progress"

      assert {:ok, %{absent_instance_attempts_recovered: 0}} =
               Accounting.recover_absent_instance_attempts(now)
    end

    test "an incarnation that started before the orphan's owner never supersedes it" do
      setup = accounting_setup()
      now = now()
      dispatched_at = DateTime.add(now, -10, :minute)
      node_name = "codex_pooler@10.42.#{System.unique_integer([:positive])}.8"
      older = Identity.new(node_name, unique_boot_id())
      owner = Identity.new(node_name, unique_boot_id())

      %{attempt: attempt} = modern_orphan!(setup, owner, dispatched_at)

      # A lingering row from an earlier incarnation of the same name, still
      # being refreshed (its VM is the one alive), does not prove the newer
      # owner dead: only a row that started later is a successor.
      {:ok, _} = InstancePresence.record_heartbeat(older, DateTime.add(dispatched_at, -1, :hour))
      {:ok, _} = InstancePresence.record_heartbeat(older, now)
      {:ok, _} = InstancePresence.record_heartbeat(owner, dispatched_at)
      {:ok, _} = InstancePresence.record_heartbeat()
      refute InstancePresence.superseded?(owner)

      assert {:ok, %{absent_instance_attempts_recovered: 0}} =
               Accounting.recover_absent_instance_attempts(now)

      assert Repo.reload!(attempt).status == "in_progress"
    end

    test "the anonymous nonode@nohost name never supersedes an orphan" do
      setup = accounting_setup()
      now = now()
      dispatched_at = DateTime.add(now, -10, :minute)
      first = Identity.new("nonode@nohost", unique_boot_id())
      second = Identity.new("nonode@nohost", unique_boot_id())

      %{attempt: attempt} = modern_orphan!(setup, first, dispatched_at)
      {:ok, _} = InstancePresence.record_heartbeat(first, dispatched_at)
      {:ok, _} = InstancePresence.record_heartbeat(second, now)
      {:ok, _} = InstancePresence.record_heartbeat()
      refute InstancePresence.superseded?(first)

      assert {:ok, %{absent_instance_attempts_recovered: 0}} =
               Accounting.recover_absent_instance_attempts(now)

      assert Repo.reload!(attempt).status == "in_progress"
    end

    test "an attempt owned by the live instance is untouched" do
      setup = accounting_setup()
      now = now()

      _live = start_instance!(:absent_recovery_live)

      %{request: request, attempt: attempt} =
        dispatch_open_turn!(setup, DateTime.add(now, -10, :minute))

      assert {:ok, %{absent_instance_attempts_recovered: 0}} =
               Accounting.recover_absent_instance_attempts(now)

      assert Repo.get!(Request, request.id).status == "in_progress"
      assert Repo.reload!(attempt).status == "in_progress"
      assert ledger_kinds(request) == ["reservation"]
    end

    test "an instance that reported inside the liveness window is untouched" do
      setup = accounting_setup()
      now = now()

      instance = start_instance!(:absent_recovery_inside_window)

      %{request: request, attempt: attempt} =
        dispatch_open_turn!(setup, DateTime.add(now, -10, :minute))

      {:ok, _recent} =
        InstancePresence.record_heartbeat(instance, DateTime.add(now, -30, :second))

      end_instance!(:absent_recovery_inside_window)

      assert {:ok, %{absent_instance_attempts_recovered: 0}} =
               Accounting.recover_absent_instance_attempts(now)

      assert Repo.get!(Request, request.id).status == "in_progress"
      assert Repo.reload!(attempt).status == "in_progress"
    end

    test "an attempt younger than the liveness window is untouched" do
      setup = accounting_setup()
      now = now()

      instance = start_instance!(:absent_recovery_young_attempt)

      %{request: request, attempt: attempt} =
        dispatch_open_turn!(setup, DateTime.add(now, -20, :second))

      {:ok, _stale} =
        InstancePresence.record_heartbeat(instance, DateTime.add(now, -10, :minute))

      end_instance!(:absent_recovery_young_attempt)

      assert {:ok, %{absent_instance_attempts_recovered: 0}} =
               Accounting.recover_absent_instance_attempts(now)

      assert Repo.get!(Request, request.id).status == "in_progress"
      assert Repo.reload!(attempt).status == "in_progress"
    end

    test "an instance that never published presence is left to the six-hour sweep" do
      setup = accounting_setup()
      now = now()

      # A VM that minted its incarnation and never got a heartbeat written: a
      # failed first write, or a role that could not reach the database.
      _boot_id = Identity.mint_boot_id!()
      identity = InstancePresence.local_identity()
      refute Repo.get(Instance, identity.instance_id)

      %{request: request, attempt: attempt} =
        dispatch_open_turn!(setup, DateTime.add(now, -7, :hour))

      assert {:ok, %{absent_instance_attempts_recovered: 0}} =
               Accounting.recover_absent_instance_attempts(now)

      assert Repo.get!(Request, request.id).status == "in_progress"

      assert {:ok, %{stale_reservations_settled: 1}} = Accounting.recover_stale_reservations(now)

      assert %Request{status: "failed", last_error_code: "stale_reservation_recovered"} =
               Repo.get!(Request, request.id)

      assert Repo.reload!(attempt).status == "failed"
    end

    test "an attempt written before incarnations existed is out of reach and stays with the sweep" do
      setup = accounting_setup()
      now = now()
      dispatched_at = DateTime.add(now, -7, :hour)
      node_name = "codex_pooler@10.42.0.#{System.unique_integer([:positive])}"

      # The shape the previous release wrote and that is still in production: a
      # presence row keyed by the node name alone, with no incarnation, long
      # past the liveness window. Its writer no longer exists in the tree, so
      # the row is inserted exactly as it left it.
      Repo.insert!(%Instance{
        instance_id: node_name,
        started_at: dispatched_at,
        last_seen_at: dispatched_at,
        updated_at: dispatched_at
      })

      %{request: request, attempt: attempt} =
        dispatch_open_turn!(setup, dispatched_at, %{owner_instance_id: node_name})

      assert attempt.owner_instance_id == node_name
      assert is_nil(attempt.owner_instance_boot_id)

      assert {:ok, %{absent_instance_attempts_recovered: 0}} =
               Accounting.recover_absent_instance_attempts(now)

      assert Repo.get!(Request, request.id).status == "in_progress"
      assert Repo.reload!(attempt).status == "in_progress"

      assert {:ok, %{stale_reservations_settled: 1}} = Accounting.recover_stale_reservations(now)

      assert Repo.get!(Request, request.id).last_error_code == "stale_reservation_recovered"
    end

    test "an attempt with no recorded owner is out of reach and stays with the six-hour sweep" do
      setup = accounting_setup()
      now = now()

      _live = start_instance!(:absent_recovery_no_owner)

      %{request: request, attempt: attempt} =
        dispatch_open_turn!(setup, DateTime.add(now, -7, :hour), %{owner_instance_id: nil})

      assert is_nil(attempt.owner_instance_id)
      assert is_nil(attempt.owner_instance_boot_id)

      assert {:ok, %{absent_instance_attempts_recovered: 0}} =
               Accounting.recover_absent_instance_attempts(now)

      assert Repo.get!(Request, request.id).status == "in_progress"

      assert {:ok, %{stale_reservations_settled: 1}} = Accounting.recover_stale_reservations(now)

      assert Repo.get!(Request, request.id).last_error_code == "stale_reservation_recovered"
    end
  end

  # findings#207: a persistently failing oldest candidate must not occupy the
  # head of every batch. The rows are committed and every pass runs unboxed,
  # because a settlement that raises inside this test's own sandbox transaction
  # would abort it; the failing settlement is a real PostgreSQL trigger on the
  # attempts row, so the failure comes from the database, not from a stub.
  # The trigger DDL takes ACCESS EXCLUSIVE on `attempts`, so this file can
  # never become async, and the committed in_progress attempts live until
  # `cleanup_unboxed_pool!/1` deletes the pool graph (attempts and ledger
  # entries included) in on_exit.
  describe "fairness across passes" do
    test "a persistently failing oldest candidate does not starve later candidates across passes" do
      graph = committed_graph!()
      absent = committed_absent_owner!()
      refresh_local_observer!()
      now = now()
      first_stale = DateTime.add(now, -180, :second)

      [failing, second, third] = committed_candidates!(graph, absent, first_stale, 3)
      install_failing_settlement!([failing.attempt.id])

      # Causal control: the same passes with the durable progress marker
      # cleared before each one reproduce the pre-fix oldest-first order. With
      # one row per batch the failing head is reselected on every pass and the
      # suffix never progresses.
      for pass <- 1..3 do
        clear_examined_marker!(failing.attempt.id)
        pass_now = DateTime.add(now, pass, :second)
        failing_id = failing.attempt.id

        assert {:error,
                {:absent_instance_candidates_failed,
                 [{^failing_id, {Postgrex.Error, :raise_exception}}]},
                %{absent_instance_attempts_recovered: 0}} = run_pass(pass_now, 1)

        assert examined_at(failing.attempt.id) == pass_now
      end

      assert attempt_status(second.attempt.id) == "in_progress"
      assert attempt_status(third.attempt.id) == "in_progress"

      # Regression: the marker the last control pass left on the failing head
      # sorts it behind the rows no pass has reached, so the next batch holds
      # both healthy rows and the failing one only comes back once they settled.
      assert {:ok, %{absent_instance_attempts_recovered: 2}} =
               run_pass(DateTime.add(now, 4, :second), 2)

      failing_id = failing.attempt.id

      assert {:error,
              {:absent_instance_candidates_failed,
               [{^failing_id, {Postgrex.Error, :raise_exception}}]},
              %{absent_instance_attempts_recovered: 0}} =
               run_pass(DateTime.add(now, 5, :second), 2)

      for candidate <- [second, third] do
        assert %Request{status: "failed", last_error_code: "absent_instance_recovered"} =
                 run_unboxed(fn -> Repo.get!(Request, candidate.request.id) end)

        assert %Attempt{status: "failed", network_error_code: "absent_instance_recovered"} =
                 run_unboxed(fn -> Repo.get!(Attempt, candidate.attempt.id) end)

        assert %CodexTurn{status: "interrupted", error_code: "absent_instance_recovered"} =
                 run_unboxed(fn -> Repo.get!(CodexTurn, candidate.turn.id) end)

        assert run_unboxed(fn -> ledger_kinds(candidate.request) end) ==
                 ["release", "reservation", "settlement"]
      end

      assert attempt_status(failing.attempt.id) == "in_progress"
      assert run_unboxed(fn -> ledger_kinds(failing.request) end) == ["reservation"]

      # Within one batch the failing head no longer halts the rest: a fresh
      # batch of stale candidates settles its healthy rows in the same pass the
      # head fails, and the head sorts behind the remainder on the next pass.
      second_stale = DateTime.add(now, -170, :second)

      [batch_failing, batch_second, batch_third] =
        committed_candidates!(graph, absent, second_stale, 3)

      install_failing_settlement!([batch_failing.attempt.id])
      batch_failing_id = batch_failing.attempt.id

      assert {:error,
              {:absent_instance_candidates_failed,
               [{^batch_failing_id, {Postgrex.Error, :raise_exception}}]},
              %{absent_instance_attempts_recovered: 1}} =
               run_pass(DateTime.add(now, 6, :second), 2)

      assert attempt_status(batch_second.attempt.id) == "failed"
      assert attempt_status(batch_third.attempt.id) == "in_progress"

      assert {:error,
              {:absent_instance_candidates_failed,
               [{^failing_id, {Postgrex.Error, :raise_exception}}]},
              %{absent_instance_attempts_recovered: 1}} =
               run_pass(DateTime.add(now, 7, :second), 2)

      assert attempt_status(batch_third.attempt.id) == "failed"
      assert attempt_status(batch_failing.attempt.id) == "in_progress"
      assert attempt_status(failing.attempt.id) == "in_progress"

      CodexPooler.TestDiagnostics.puts(
        "absent_instance_fairness control_passes=3 control_recovered=0 regression_recovered=4 failing_retained=2 terminal=ok"
      )
    end
  end

  describe "attempt ownership" do
    test "a dispatched attempt records the incarnation this instance publishes" do
      instance = start_instance!(:absent_recovery_ownership)
      setup = accounting_setup()

      {:ok, reserved} =
        Accounting.reserve(
          setup.auth,
          setup.model,
          %{"model" => setup.model.exposed_model_id, "stream" => true, "max_output_tokens" => 10},
          %{correlation_id: unique_correlation_id(), transport: "http_sse"}
        )

      assert {:ok, attempt} = Accounting.create_attempt(reserved.request, setup.assignment)
      assert attempt.owner_instance_id == instance.node_name
      assert attempt.owner_instance_boot_id == instance.boot_id

      # The recorded owner and the published row name the same VM. That link is
      # what the recovery join depends on, and nothing else in the pass restores
      # it if the two ever disagree.
      published = Repo.get!(Instance, instance.instance_id)
      assert published.node_name == attempt.owner_instance_id
      assert published.boot_id == attempt.owner_instance_boot_id
    end
  end

  # A VM start mints one incarnation and the real heartbeat publishes it. The
  # attempts below take their owner from that same identity through ordinary
  # dispatch, so nothing in these tests supplies the ownership it then asserts.
  defp start_instance!(name) do
    _boot_id = Identity.mint_boot_id!()
    identity = InstancePresence.local_identity()

    pid =
      start_supervised!(
        {InstanceHeartbeat, enabled: true, interval_ms: :timer.minutes(5), name: name},
        id: name
      )

    # The publish runs in the process's own continue, so one synchronous state
    # read is enough to know it has happened; no timer is waited out.
    _state = :sys.get_state(pid)

    assert %Instance{} = Repo.get(Instance, identity.instance_id)

    identity
  end

  defp end_instance!(name), do: :ok = stop_supervised!(name)

  # An open turn dispatched by `owner` that records an executor the way the
  # gateway does, so the pass must reach the exact-execution authority.
  defp modern_orphan!(setup, %Identity{} = owner, dispatched_at) do
    dispatched =
      dispatch_open_turn!(setup, dispatched_at, %{
        owner_instance_id: owner.node_name,
        owner_instance_boot_id: owner.boot_id
      })

    attempt =
      dispatched.attempt
      |> Ecto.Changeset.change(
        owner_process_id: "<0.#{System.unique_integer([:positive])}.0>",
        owner_execution_id: Ecto.UUID.generate()
      )
      |> Repo.update!()

    %{dispatched | attempt: attempt}
  end

  defp unique_boot_id,
    do: Base.encode32(:crypto.strong_rand_bytes(10), case: :lower, padding: false)

  defp dispatch_open_turn!(setup, dispatched_at, attempt_attrs \\ %{}) do
    {:ok, reserved} =
      Accounting.reserve(
        setup.auth,
        setup.model,
        %{"model" => setup.model.exposed_model_id, "stream" => true, "max_output_tokens" => 10},
        %{correlation_id: unique_correlation_id(), now: dispatched_at, transport: "http_sse"}
      )

    {:ok, attempt} =
      Accounting.create_attempt(
        reserved.request,
        setup.assignment,
        Map.put(attempt_attrs, :now, dispatched_at)
      )

    session = session_row(setup, dispatched_at)
    turn = turn_row(session, reserved.request, attempt, dispatched_at)

    %{request: reserved.request, attempt: attempt, session: session, turn: turn}
  end

  # The session carries no owner lease, so the request is not an active runtime
  # turn held by another replica; the recovery pass may reach it.
  defp session_row(setup, started_at) do
    %CodexSession{
      pool_id: setup.pool.id,
      api_key_id: setup.api_key.id,
      session_key: "session-#{System.unique_integer([:positive])}",
      pool_upstream_assignment_id: setup.assignment.id,
      status: "active",
      created_at: started_at,
      updated_at: started_at
    }
    |> Repo.insert!()
  end

  defp turn_row(session, request, attempt, started_at) do
    %CodexTurn{
      codex_session_id: session.id,
      request_id: request.id,
      turn_sequence: 1,
      transport_kind: "http_sse",
      status: CodexTurn.in_progress_status(),
      final_attempt_id: attempt.id,
      started_at: started_at,
      created_at: started_at,
      updated_at: started_at
    }
    |> Repo.insert!()
  end

  defp ledger_kinds(request) do
    request.id
    |> Accounting.list_ledger_entries_for_request()
    |> Enum.map(& &1.entry_kind)
    |> Enum.sort()
  end

  # -- committed fixtures for the multi-pass fairness regression ---------------

  # `accounting_setup/0` derives its unique keys while it commits, so the
  # cleanup is registered straight after the commit, as the replay Postgres
  # tests do; `cleanup_unboxed_pool!/1` removes the Pool graph, its pricing row,
  # its upstream identity and the fixture owner it committed.
  defp committed_graph! do
    setup = run_unboxed(fn -> accounting_setup() end)
    register_unboxed_cleanup!(fn -> cleanup_unboxed_pool!(setup) end)
    setup
  end

  # An incarnation that published presence once, ten minutes ago, and never
  # again: the shape every absent-instance candidate is judged against.
  defp committed_absent_owner! do
    unique = System.unique_integer([:positive])
    owner = Identity.new("codex_pooler@10.0.0.#{unique}", "boot#{unique}")

    register_unboxed_cleanup!(fn -> delete_presence!(owner.instance_id) end)

    {:ok, _presence} =
      run_unboxed(fn ->
        InstancePresence.record_heartbeat(owner, DateTime.add(now(), -600, :second))
      end)

    owner
  end

  # The pass only authorizes another incarnation's absence while this observer
  # is fresh. The heartbeat is disabled in the test environment, so the local
  # row is written here and removed again unless it already existed.
  defp refresh_local_observer! do
    local = Identity.local()
    existed? = run_unboxed(fn -> not is_nil(Repo.get(Instance, local.instance_id)) end)

    unless existed? do
      register_unboxed_cleanup!(fn -> delete_presence!(local.instance_id) end)
    end

    {:ok, _presence} = run_unboxed(fn -> InstancePresence.record_heartbeat(local) end)
    :ok
  end

  # `count` open legacy attempts owned by `owner`, dispatched one second apart
  # from `started_at` so the oldest-first order is unambiguous. Legacy rows
  # carry no execution identity, so the pass settles them through the plain
  # absent-instance finalizer rather than the exact-execution authority.
  defp committed_candidates!(setup, owner, started_at, count) do
    for index <- 0..(count - 1) do
      dispatched_at = DateTime.add(started_at, index, :second)

      run_unboxed(fn ->
        dispatch_open_turn!(setup, dispatched_at, %{
          owner_instance_id: owner.node_name,
          owner_instance_boot_id: owner.boot_id,
          owner_process_id: nil,
          owner_execution_id: nil
        })
      end)
    end
  end

  # A real database failure on the exact rows: settling any of `attempt_ids` to
  # `failed` raises inside the finalizer's transaction. The trigger is dropped
  # by a cleanup registered before it exists.
  defp install_failing_settlement!(attempt_ids) do
    suffix = System.unique_integer([:positive])
    function = "absent_recovery_test_fail_#{suffix}"
    ids = Enum.map_join(attempt_ids, ", ", &"'#{&1}'::uuid")

    register_unboxed_cleanup!(fn ->
      Repo.query!("DROP TRIGGER IF EXISTS #{function} ON attempts")
      Repo.query!("DROP FUNCTION IF EXISTS #{function}()")
    end)

    run_unboxed(fn ->
      Repo.query!("""
      CREATE FUNCTION #{function}() RETURNS trigger LANGUAGE plpgsql AS $$
      BEGIN
        IF NEW.id IN (#{ids}) AND NEW.status = 'failed' THEN
          RAISE EXCEPTION 'synthetic absent instance settlement failure';
        END IF;
        RETURN NEW;
      END $$
      """)

      Repo.query!(
        "CREATE TRIGGER #{function} BEFORE UPDATE ON attempts " <>
          "FOR EACH ROW EXECUTE FUNCTION #{function}()"
      )
    end)

    :ok
  end

  defp delete_presence!(instance_id) do
    Repo.delete_all(from instance in Instance, where: instance.instance_id == ^instance_id)
    :ok
  end

  defp run_pass(pass_now, limit),
    do: run_unboxed(fn -> Accounting.recover_absent_instance_attempts(pass_now, limit: limit) end)

  defp clear_examined_marker!(attempt_id) do
    run_unboxed(fn ->
      Repo.update_all(from(a in Attempt, where: a.id == ^attempt_id),
        set: [owner_execution_checked_at: nil]
      )
    end)

    :ok
  end

  defp examined_at(attempt_id),
    do: run_unboxed(fn -> Repo.get!(Attempt, attempt_id).owner_execution_checked_at end)

  defp attempt_status(attempt_id),
    do: run_unboxed(fn -> Repo.get!(Attempt, attempt_id).status end)

  defp unique_correlation_id, do: "corr-absent-#{System.unique_integer([:positive])}"

  defp now, do: DateTime.utc_now() |> DateTime.truncate(:microsecond)
end
