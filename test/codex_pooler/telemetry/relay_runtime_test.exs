defmodule CodexPooler.Telemetry.RelayRuntimeTest do
  use CodexPooler.DataCase, async: false

  alias CodexPooler.Telemetry.{Relay, RelayEvent, RelayRuntime}

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

    Ecto.Adapters.SQL.Sandbox.allow(Repo, owner, runtime)
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
end
