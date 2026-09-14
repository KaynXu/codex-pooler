defmodule CodexPooler.InstancePresencePeerCleanupTest do
  use ExUnit.Case, async: true

  alias CodexPooler.InstancePresencePeer

  test "OS absence waits past peer termination until the kernel PID disappears" do
    {:ok, samples} =
      Agent.start_link(fn -> [{"", 0}, {"", 0}, {"kill: 123: No such process\n", 1}] end)

    on_exit(fn -> if Process.alive?(samples), do: Agent.stop(samples) end)

    probe = fn "owned-pid" ->
      Agent.get_and_update(samples, fn [next | rest] -> {next, rest} end)
    end

    assert :ok = InstancePresencePeer.assert_os_process_absent!("owned-pid", probe: probe)
    assert Agent.get(samples, & &1) == []
  end

  test "a surviving PID still fails instead of accepting a clean peer exit" do
    assert_raise ExUnit.AssertionError, ~r/owned peer OS process survived/, fn ->
      InstancePresencePeer.assert_os_process_absent!("owned-pid",
        budget_ms: 0,
        probe: fn _ -> {"", 0} end
      )
    end
  end

  for result <- [
        {"kill: 123: Operation not permitted\n", 1},
        {"kill: 123: Permission denied\n", 1},
        {"unknown failure", 2},
        {"", 1}
      ] do
    @tag probe_result: result
    test "probe error #{inspect(result)} never establishes absence", %{probe_result: result} do
      assert InstancePresencePeer.classify_os_process_probe(result) == :unknown

      assert_raise ExUnit.AssertionError, ~r/owned peer OS process survived/, fn ->
        InstancePresencePeer.assert_os_process_absent!("owned-pid",
          budget_ms: 0,
          probe: fn _ -> result end
        )
      end
    end
  end

  test "only explicit ESRCH diagnostics establish absence" do
    for output <- [
          "kill: 123: No such process\n",
          "kill: (123): No such process\n",
          "123: no such process\n"
        ] do
      assert InstancePresencePeer.classify_os_process_probe({output, 1}) == :absent
    end
  end
end
