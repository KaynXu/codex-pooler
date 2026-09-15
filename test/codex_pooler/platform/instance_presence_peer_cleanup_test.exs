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

  test "captures the exact owned process start identity" do
    snapshot =
      {:present, %{source: :proc, state: "S", parent_pid: 42, start_signature: "123456"}}

    assert %{pid: "owned-pid", source: :proc, start_signature: "123456"} =
             InstancePresencePeer.capture_os_process_identity!("owned-pid",
               probe: fn _ -> snapshot end
             )
  end

  test "owned process stop accepts absence, PID reuse, and a same-identity zombie" do
    identity = %{pid: "owned-pid", source: :proc, start_signature: "123456"}

    for result <- [
          :absent,
          {:present, %{source: :proc, state: "S", parent_pid: 1, start_signature: "999999"}},
          {:present, %{source: :proc, state: "Z", parent_pid: 1, start_signature: "123456"}}
        ] do
      assert :ok =
               InstancePresencePeer.assert_os_process_stopped!(identity,
                 budget_ms: 0,
                 probe: fn _ -> result end
               )
    end
  end

  test "owned process stop fails closed for a live identity and unknown inspection" do
    identity = %{pid: "owned-pid", source: :proc, start_signature: "123456"}

    for result <- [
          {:present, %{source: :proc, state: "S", parent_pid: 1, start_signature: "123456"}},
          {:present, %{source: :ps, state: "S", parent_pid: 1, start_signature: "same time"}},
          {:error, :permission_denied}
        ] do
      assert_raise ExUnit.AssertionError, ~r/owned peer OS process remained/, fn ->
        InstancePresencePeer.assert_os_process_stopped!(identity,
          budget_ms: 0,
          probe: fn _ -> result end
        )
      end
    end
  end

  test "parses Linux proc stat using state, parent, and start-time fields" do
    stat =
      "123 (beam.smp worker (owned)) Z 42 1 1 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 987654 0 0\n"

    assert {:present, %{source: :proc, state: "Z", parent_pid: 42, start_signature: "987654"}} =
             InstancePresencePeer.parse_linux_process_stat(stat)

    assert {:error, :invalid_proc_stat} =
             InstancePresencePeer.parse_linux_process_stat("invalid")
  end

  test "parses portable ps output without changing the backend identity" do
    output = "S    Tue Sep 15 02:33:48 2026     42\n"

    assert {:present,
            %{
              source: :ps,
              state: "S",
              parent_pid: 42,
              start_signature: "Tue Sep 15 02:33:48 2026"
            }} = InstancePresencePeer.parse_portable_process_output(output)

    assert {:error, :invalid_ps_output} =
             InstancePresencePeer.parse_portable_process_output("invalid")
  end
end
