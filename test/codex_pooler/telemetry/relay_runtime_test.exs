defmodule CodexPooler.Telemetry.RelayRuntimeTest do
  use CodexPooler.DataCase, async: false

  alias CodexPooler.Telemetry.{RelayEvent, RelayRuntime}

  setup do
    :ets.delete_all_objects(RelayRuntime)
    Repo.delete_all(RelayEvent)
    :ok
  end

  test "captures each source contract with its bounded relay event" do
    events = [
      {[:codex_pooler, :quota, :cycle, :decision], "quota_cycle_decision", %{scope: :account}},
      {[:codex_pooler, :saved_reset, :convergence], "saved_reset_convergence", %{source: "x"}},
      {[:codex_pooler, :accounting, :reservation, :pre_attempt_release], "pre_attempt_release", %{phase: :reserve}},
      {[:codex_pooler, :gateway, :stream, :outcome], "stream_outcome", %{outcome: :ok}}
    ]

    Enum.each(events, fn {source, relay, metadata} ->
      :telemetry.execute(source, %{count: 2}, Map.put(metadata, :oversized, String.duplicate("x", 200)))
      assert [{{^relay, _labels}, %{count: 2}}] = Enum.filter(:ets.tab2list(RelayRuntime), fn {{name, _}, _} -> name == relay end)
    end)
  end

  test "flush persists rows and drain re-emits once without recursion" do
    ref = make_ref()
    test_pid = self()
    :telemetry.attach(ref, [:codex_pooler, :quota, :cycle, :decision], fn _event, _measurements, _metadata, pid -> send(pid, :seen) end, test_pid)
    on_exit(fn -> :telemetry.detach(ref) end)

    :telemetry.execute([:codex_pooler, :quota, :cycle, :decision], %{count: 1}, %{scope: "test"})
    send(RelayRuntime, :flush)
    :sys.get_state(RelayRuntime)
    assert_receive :seen, 1_000
    assert Repo.aggregate(RelayEvent, :count) == 1
    send(RelayRuntime, :drain)
    assert_receive :seen, 1_000
    refute_receive :seen, 100
    assert Repo.aggregate(RelayEvent, :count) == 1
  end
end
