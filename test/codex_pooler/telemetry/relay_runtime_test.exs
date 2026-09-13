defmodule CodexPooler.Telemetry.RelayRuntimeTest do
  use CodexPooler.DataCase, async: false

  alias CodexPooler.Telemetry.{Relay, RelayEvent, RelayRuntime}
  alias Ecto.Adapters.SQL.Sandbox

  setup %{sandbox_owner: owner} do
    runtime =
      start_supervised!(
        {RelayRuntime,
         enabled: true,
         start_paused: true,
         name: {:global, {__MODULE__, make_ref()}},
         flush_ms: 60_000,
         drain_ms: 60_000}
      )

    Sandbox.allow(Repo, owner, runtime)
    :ok = GenServer.call(runtime, :activate)
    state = :sys.get_state(runtime)
    %{runtime: runtime, table: state.table, writer: state.owner, handler: state.handler}
  end

  test "captures each source contract with its bounded relay event", %{table: table} do
    events = [
      {[:codex_pooler, :quota, :cycle, :decision], "quota_cycle_decision", %{scope: :account}},
      {[:codex_pooler, :saved_reset, :convergence], "saved_reset_convergence", %{source: "x"}},
      {[:codex_pooler, :accounting, :reservation, :pre_attempt_release], "pre_attempt_release",
       %{phase: :reserve}},
      {[:codex_pooler, :gateway, :stream, :outcome], "stream_outcome", %{outcome: :ok}}
    ]

    Enum.each(events, fn {source, relay, metadata} ->
      :telemetry.execute(
        source,
        %{count: 2},
        Map.put(metadata, :oversized, String.duplicate("x", 200))
      )

      assert [{{^relay, _labels}, %{count: 2}}] =
               Enum.filter(:ets.tab2list(table), fn {{name, _}, _} -> name == relay end)
    end)
  end

  test "synchronized callbacks preserve every same-key count and numeric sum", %{table: table} do
    coordinator = self()
    gate = make_ref()
    writers = 32
    iterations = 200

    tasks =
      for _ <- 1..writers do
        Task.async(fn ->
          send(coordinator, {gate, self()})

          receive do
            ^gate -> :ok
          end

          for _ <- 1..iterations do
            :telemetry.execute(
              [:codex_pooler, :saved_reset, :convergence],
              %{count: 2, applied_to_canonical_ms: 0.5, applied_to_lifecycle_ms: 3},
              %{source: "runtime_headers", outcome: "confirmed_by_quota"}
            )
          end
        end)
      end

    for _ <- tasks do
      assert_receive {^gate, _pid}
    end

    Enum.each(tasks, &send(&1.pid, gate))
    Enum.each(tasks, &Task.await(&1, 10_000))

    assert [{{"saved_reset_convergence", _labels}, measurements}] = :ets.tab2list(table)
    assert measurements.count == writers * iterations * 2
    assert measurements.applied_to_canonical_ms == writers * iterations * 0.5
    assert measurements.applied_to_lifecycle_ms == writers * iterations * 3
  end

  test "flush persists rows and drain re-emits once without recursion", %{
    runtime: runtime,
    table: table
  } do
    ref = make_ref()
    test_pid = self()
    on_exit(fn -> :telemetry.detach(ref) end)

    :telemetry.attach(
      ref,
      [:codex_pooler, :quota, :cycle, :decision],
      fn _event, _measurements, _metadata, pid -> send(pid, :seen) end,
      test_pid
    )

    :telemetry.execute([:codex_pooler, :quota, :cycle, :decision], %{count: 1}, %{scope: "test"})
    send(runtime, :flush)
    :sys.get_state(runtime)
    assert_receive :seen, 1_000
    assert Repo.aggregate(RelayEvent, :count) == 1
    send(runtime, :drain)
    :sys.get_state(runtime)
    assert_receive :seen, 1_000
    refute_received :seen
    assert :ets.tab2list(table) == []
    assert Repo.aggregate(RelayEvent, :count) == 1
  end

  test "startup publishes an owned heartbeat and termination detaches the handler", %{
    runtime: runtime,
    writer: writer,
    handler: handler,
    table: table
  } do
    assert Relay.heartbeat_fresh?(writer)
    assert Process.whereis(RelayRuntime) == nil

    assert Enum.any?(
             :telemetry.list_handlers([:codex_pooler, :quota, :cycle, :decision]),
             &(&1.id == handler)
           )

    monitor = Process.monitor(runtime)
    stop_supervised!(RelayRuntime)
    assert_receive {:DOWN, ^monitor, :process, ^runtime, :shutdown}

    refute Enum.any?(
             :telemetry.list_handlers([:codex_pooler, :quota, :cycle, :decision]),
             &(&1.id == handler)
           )

    assert :ets.info(table) == :undefined
  end

  test "stale writer requeues until its own heartbeat is refreshed", %{
    runtime: runtime,
    writer: writer,
    table: table
  } do
    Repo.query!(
      "UPDATE telemetry_relay_heartbeats SET heartbeat_at = NOW() - INTERVAL '2 minutes' WHERE owner = $1",
      [writer]
    )

    :ok = Relay.refresh_heartbeat("another-writer")
    refute Relay.heartbeat_fresh?(writer)

    :telemetry.execute([:codex_pooler, :quota, :cycle, :decision], %{count: 2}, %{scope: :account})

    send(runtime, :flush)
    :sys.get_state(runtime)
    assert Repo.aggregate(RelayEvent, :count) == 0
    assert [{_, %{count: 2}}] = :ets.tab2list(table)
    send(runtime, :heartbeat)
    :sys.get_state(runtime)
    assert Relay.heartbeat_fresh?(writer)
    send(runtime, :flush)
    :sys.get_state(runtime)
    assert [%RelayEvent{count: 2}] = Repo.all(RelayEvent)
    assert :ets.tab2list(table) == []
  end

  test "cleanup failure schedules another cleanup pass" do
    parent = self()

    {:ok, runtime} =
      start_supervised(
        {RelayRuntime,
         start_paused: true,
         cleanup_interval_ms: 10,
         cleanup_fun: fn ->
           send(parent, :cleanup_attempt)
           raise "synthetic cleanup failure"
         end}
      )

    send(runtime, :cleanup)
    assert_receive :cleanup_attempt
    assert_receive :cleanup, 100
  end
end
