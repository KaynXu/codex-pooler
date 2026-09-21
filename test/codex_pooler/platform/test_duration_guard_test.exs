defmodule CodexPooler.TestDurationGuardTest do
  use ExUnit.Case, async: false

  alias CodexPooler.TestDurationGuard

  @limits %{normal_us: 1_000_000, hard_us: 6_000_000}
  @guard Path.expand("test/support/test_duration_guard.ex")
  @probe Path.expand("scripts/verification/test_duration_guard_probe_test.exs")
  @local_env [{"CI", nil}, {"DRONE", nil}, {"GITHUB_ACTIONS", nil}]

  test "exact boundaries pass and both limits reject the first excess microsecond" do
    assert violation(1_000_000, %{}) == nil
    assert violation(1_000_001, %{}) =~ "exceeds 1000.0ms"
    assert violation(6_000_000, %{slow: "real process boundary"}) == nil
    assert violation(6_000_001, %{slow: "real process boundary"}) =~ "6000.0ms hard limit"
  end

  test "a slow exemption requires a nonempty string even when the test happens to be fast" do
    for invalid <- [true, "", " \n ", :slow, 42, %{reason: "boundary"}] do
      assert violation(1, %{slow: invalid}) =~ "reason must be a nonempty string"
    end

    assert violation(1, %{slow: false}) == nil
    assert violation(1, %{slow: nil}) == nil
    assert violation(1_000_001, %{slow: "real process boundary"}) == nil
  end

  test "skipped and excluded tests do not need slow exemptions" do
    for state <- [{:skipped, "fixture"}, {:excluded, "fixture"}, {:invalid, nil}] do
      assert TestDurationGuard.violation(%ExUnit.Test{state: state, tags: %{slow: true}}, @limits) ==
               nil
    end
  end

  for mode <- ["normal", "trace"],
      {scenario, expected_exit, diagnostic} <- [
        {"fast", 0, nil},
        {"allowed", 0, nil},
        {"ordinary", 1, "exceeds 1.0ms"},
        {"setup", 1, "exceeds 1.0ms"},
        {"invalid", 1, "reason must be a nonempty string"},
        {"hard", 1, "slow tags cannot waive it"},
        {"missing", 1, "formatter missing or incomplete"}
      ] do
    @tag slow: "boots an isolated BEAM VM to verify ExUnit exit status and teardown"
    test "#{mode} subprocess enforces #{scenario} and completes teardown" do
      {output, exit_code} =
        System.cmd(
          "elixir",
          ["--erl", "+S 2:2", "-r", @guard, @probe, unquote(scenario), unquote(mode)],
          env: @local_env,
          stderr_to_stdout: true
        )

      assert exit_code == unquote(expected_exit), output
      assert output =~ "probe teardown completed", output
      assert output =~ "guard receipts remaining=0", output
      refute output =~ "warning:", output

      assert_diagnostic(output, unquote(diagnostic))
    end
  end

  for mode <- ["normal", "trace"], scenario <- ["ordinary", "hard", "assertion"] do
    @tag slow:
           "boots an isolated BEAM VM to verify CI timing reports do not mask assertion failures"
    test "CI #{mode} reports #{scenario} without enforcing wall-clock budgets" do
      {output, exit_code} =
        System.cmd(
          "elixir",
          ["--erl", "+S 2:2", "-r", @guard, @probe, unquote(scenario), unquote(mode)],
          env: List.keystore(@local_env, "CI", 0, {"CI", "true"}),
          stderr_to_stdout: true
        )

      expected_exit = if unquote(scenario) == "assertion", do: 2, else: 0
      assert exit_code == expected_exit, output
      refute output =~ "test duration guard failed:", output
      assert output =~ "probe teardown completed", output
      assert output =~ "guard receipts remaining=0", output

      if unquote(scenario) != "assertion",
        do: assert(output =~ "test duration report (CI; non-blocking):", output)
    end
  end

  for mode <- [[], ["--trace"]] do
    @tag :tmp_dir
    @tag slow: "boots an isolated Mix project to verify CLI failure after test cleanup"
    test "mix test #{inspect(mode)} fails its exit status after duration violation", %{
      tmp_dir: dir
    } do
      File.mkdir_p!(Path.join(dir, "test"))

      File.write!(Path.join(dir, "mix.exs"), """
      defmodule DurationProbe.MixProject do
        use Mix.Project
        def project, do: [app: :duration_probe, version: "0.1.0"]
      end
      """)

      File.write!(Path.join(dir, "test/test_helper.exs"), """
      Code.require_file(#{inspect(@guard)})
      ExUnit.start()
      CodexPooler.TestDurationGuard.start!(normal_ms: 1, hard_ms: 1_000)
      """)

      File.write!(Path.join(dir, "test/duration_test.exs"), """
      defmodule DurationProbeTest do
        use ExUnit.Case
        test "body is too slow" do
          on_exit(fn -> IO.puts("probe teardown completed") end)
          receive do
          after
            20 -> :ok
          end
        end
      end
      """)

      {output, exit_code} =
        System.cmd("mix", ["test", "--no-color"] ++ unquote(mode),
          cd: dir,
          env: [{"ELIXIR_ERL_OPTIONS", "+S 2:2"} | @local_env],
          stderr_to_stdout: true
        )

      assert exit_code == 1, output
      assert output =~ "test duration guard failed:", output
      assert output =~ "exceeds 1.0ms", output
      assert output =~ "probe teardown completed", output
      refute output =~ "warning:", output
    end
  end

  defp violation(time, tags) do
    TestDurationGuard.violation(
      %ExUnit.Test{module: __MODULE__, name: :example, time: time, tags: tags},
      @limits
    )
  end

  defp assert_diagnostic(output, nil), do: refute(output =~ "test duration guard failed:", output)

  defp assert_diagnostic(output, diagnostic) do
    assert output =~ "test duration guard failed:", output
    assert output =~ diagnostic, output
  end
end
