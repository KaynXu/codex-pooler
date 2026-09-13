defmodule CodexPooler.Telemetry.RelayRuntime do
  @moduledoc false
  use GenServer

  alias CodexPooler.Telemetry.Relay

  @events %{
    [:codex_pooler, :quota, :cycle, :decision] => "quota_cycle_decision",
    [:codex_pooler, :saved_reset, :convergence] => "saved_reset_convergence",
    [:codex_pooler, :accounting, :reservation, :pre_attempt_release] =>
      "pre_attempt_release",
    [:codex_pooler, :gateway, :stream, :outcome] => "stream_outcome"
  }
  @source_events Map.new(@events, fn {source, event} -> {event, source} end)

  def start_link(opts \\ []), do: GenServer.start_link(__MODULE__, opts, name: __MODULE__)

  @impl true
  def init(opts) do
    table = :ets.new(__MODULE__, [:named_table, :public, :set, read_concurrency: true])

    :telemetry.attach_many(
      {__MODULE__, self()},
      Map.keys(@events),
      &__MODULE__.handle_event/4,
      self()
    )

    flush_ms = Keyword.get(opts, :flush_ms, 5_000)
    drain_ms = Keyword.get(opts, :drain_ms, 15_000)
    {:ok, %{table: table, flush_ms: flush_ms, drain_ms: drain_ms}, {:continue, :schedule}}
  end

  def handle_event(event, measurements, metadata, _config) do
    with false <- Process.get({__MODULE__, :draining}, false),
         relay_event when is_binary(relay_event) <- Map.get(@events, event),
         labels when is_map(labels) <- labels(metadata),
         count when is_integer(count) and count > 0 <- Map.get(measurements, :count, 1) do
      key = {relay_event, labels}
      prior = case :ets.lookup(__MODULE__, key) do [{^key, value}] -> value; [] -> %{} end
      value = Enum.reduce(measurements, Map.put(prior, :count, Map.get(prior, :count, 0) + count), fn
        {:count, _}, acc -> acc
        {k, v}, acc -> if is_number(v), do: Map.update(acc, k, v, &(&1 + v)), else: acc
      end)
      :ets.insert(__MODULE__, {key, value})
    else
      _ -> :ok
    end
  rescue
    _ -> :ok
  end

  @impl true
  def handle_continue(:schedule, state) do
    Process.send_after(self(), :flush, state.flush_ms)
    Process.send_after(self(), :drain, state.drain_ms)
    {:noreply, state}
  end

  @impl true
  def handle_info(:flush, state) do
    :ets.tab2list(state.table)
    |> Enum.each(fn {key = {event, labels}, measurements} ->
      case :ets.take(state.table, key) do
        [{^key, measurements}] ->
          case Relay.insert(event, labels, Map.get(measurements, :count, 1), measurements) do
            {:ok, _} -> :ok
            _ -> :ets.insert(state.table, {key, measurements})
          end

        [] -> :ok
      end
    end)
    Process.send_after(self(), :flush, state.flush_ms)
    {:noreply, state}
  rescue
    _ -> {:noreply, state}
  end

  def handle_info(:drain, state) do
    case Relay.claim(100, "relay-runtime") do
      {:ok, rows} -> Enum.each(rows, &safe_emit/1)
      _ -> :ok
    end

    Process.send_after(self(), :drain, state.drain_ms)
    {:noreply, state}
  rescue
    _ -> {:noreply, state}
  end

  defp emit(row) do
    case Map.get(@source_events, row.event) do
      nil -> :ok
      event -> :telemetry.execute(event, Map.merge(%{count: row.count}, row.measurements || %{}), Map.put(row.labels, "via", "job_relay"))
    end
  end

  defp safe_emit(row) do
    # Drained events must never be recaptured by our own telemetry handlers.
    Process.put({__MODULE__, :draining}, true)
    emit(row)
  rescue
    _ -> :ok
  after
    Process.delete({__MODULE__, :draining})
  end

  defp labels(metadata),
    do:
      metadata
      |> Map.take([:scope, :decision, :source, :outcome, :phase, :transport, :via])
      |> Map.put_new(:via, "in_process")
      |> Map.new(fn {k, v} -> {k, bounded(v)} end)

  defp bounded(v) when is_atom(v), do: Atom.to_string(v)
  defp bounded(v) when is_binary(v) and byte_size(v) <= 80, do: v
  defp bounded(_), do: "unknown"
end
