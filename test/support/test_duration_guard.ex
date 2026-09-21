defmodule CodexPooler.TestDurationGuard do
  @moduledoc """
  Fails the invocation when an ordinary test exceeds one second or any test
  exceeds six seconds. A test may opt into the intermediate range with
  `@tag slow: "specific reason this boundary needs more than one second"`.

  Uses ExUnit.Test.time: setup, body and captured logging, excluding setup_all
  and on_exit. Measuring asynchronous formatter delivery would charge unrelated
  scheduler/formatter backlog to a test instead of its actual execution time.
  CI reports the same observations without failing on runner-dependent timing.
  """

  use GenServer

  @config_key :codex_pooler_test_duration_guard
  @type limits :: %{normal_us: pos_integer(), hard_us: pos_integer()}

  @spec start!(keyword()) :: :ok
  def start!(opts \\ []) do
    normal_ms = Keyword.get(opts, :normal_ms, 1_000)
    hard_ms = Keyword.get(opts, :hard_ms, 6_000)

    unless is_integer(normal_ms) and is_integer(hard_ms) and normal_ms > 0 and
             hard_ms >= normal_ms do
      raise ArgumentError, "duration limits must be positive integers with hard_ms >= normal_ms"
    end

    key = {__MODULE__, make_ref()}
    limits = %{normal_us: normal_ms * 1_000, hard_us: hard_ms * 1_000}
    :persistent_term.put(key, :awaiting_formatter)

    ExUnit.configure(
      formatters: Enum.uniq(ExUnit.configuration()[:formatters] ++ [__MODULE__]),
      codex_pooler_test_duration_guard: %{key: key, limits: limits}
    )

    # ExUnit drains/stops formatter servers before after_suite callbacks. The
    # receipt therefore outlives its server without leaving a process behind.
    report_only? = Enum.any?(~w(CI DRONE GITHUB_ACTIONS), &(System.get_env(&1) in ["1", "true", "TRUE"]))
    ExUnit.after_suite(fn _stats -> finish(key, report_only?) end)
    :ok
  end

  @impl true
  def init(opts) do
    config = Keyword.fetch!(opts, @config_key)
    :persistent_term.put(config.key, :running)
    {:ok, Map.put(config, :violations, [])}
  end

  @impl true
  def handle_cast({:test_finished, test}, state) do
    case violation(test, state.limits) do
      nil -> {:noreply, state}
      message -> {:noreply, %{state | violations: [message | state.violations]}}
    end
  end

  def handle_cast({:suite_finished, _times}, state) do
    :persistent_term.put(state.key, {:finished, Enum.reverse(state.violations)})
    {:noreply, state}
  end

  def handle_cast(_event, state), do: {:noreply, state}

  @doc false
  @spec violation(ExUnit.Test.t(), limits()) :: String.t() | nil
  def violation(%ExUnit.Test{state: {state, _}}, _limits)
      when state in [:skipped, :excluded, :invalid],
      do: nil

  def violation(%ExUnit.Test{} = test, limits) do
    slow = Map.get(test.tags, :slow, false)

    reason =
      cond do
        test.time > limits.hard_us ->
          "exceeds the #{milliseconds(limits.hard_us)}ms hard limit; slow tags cannot waive it"

        slow not in [false, nil] and not valid_reason?(slow) ->
          "requires @tag slow: \"specific reason\"; the reason must be a nonempty string"

        test.time > limits.normal_us and not valid_reason?(slow) ->
          "exceeds #{milliseconds(limits.normal_us)}ms; shorten the test or justify @tag slow: \"specific reason\""

        true ->
          nil
      end

    if reason do
      file = Map.get(test.tags, :file, "unknown")
      line = Map.get(test.tags, :line, 0)

      "#{file}:#{line} #{inspect(test.module)} #{test.name} (#{milliseconds(test.time)}ms): #{reason}"
    end
  end

  defp valid_reason?(reason) when is_binary(reason), do: String.trim(reason) != ""
  defp valid_reason?(_reason), do: false
  defp milliseconds(microseconds), do: :erlang.float_to_binary(microseconds / 1_000, decimals: 1)

  defp finish(key, report_only?) do
    receipt = :persistent_term.get(key, :missing)
    :persistent_term.erase(key)

    failures =
      case receipt do
        {:finished, violations} ->
          violations

        _missing_or_incomplete ->
          ["formatter missing or incomplete; duration enforcement did not run"]
      end

    if failures != [] do
      heading = if report_only?, do: "test duration report (CI; non-blocking):", else: "test duration guard failed:"

      IO.puts(
        :stderr,
        heading <> "\n" <> Enum.map_join(failures, "\n", &("  " <> &1))
      )

      # Let Mix finish coverage and the test task drop its owned database before
      # returning failure. This also applies to ExUnit's plain autorun mode.
      schedule_failure_exit(report_only?)
    end

    :ok
  end

  defp schedule_failure_exit(true), do: :ok
  defp schedule_failure_exit(false), do: System.at_exit(fn _status -> exit({:shutdown, 1}) end)
end
