defmodule CodexPooler.Telemetry.RelayRuntime do
  @moduledoc false
  use GenServer

  alias CodexPooler.Telemetry.Relay

  @events %{
    [:codex_pooler, :gateway, :routing, :quota_cycle, :decision] => "quota_cycle_decision",
    [:codex_pooler, :upstreams, :saved_reset, :convergence] => "saved_reset_convergence",
    [:codex_pooler, :accounting, :reservation, :pre_attempt_release] => "stale_sweep",
    [:codex_pooler, :gateway, :stream, :interrupted] => "interrupted"
  }

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
    with relay_event when is_binary(relay_event) <- Map.get(@events, event),
         labels when is_map(labels) <- labels(metadata),
         count when is_integer(count) and count > 0 <- Map.get(measurements, :count, 1) do
      key = {relay_event, labels}
      :ets.update_counter(__MODULE__, key, {2, count}, {key, 0})
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
    |> Enum.each(fn {{event, labels}, count} -> _ = Relay.insert(event, labels, count) end)

    :ets.delete_all_objects(state.table)
    Process.send_after(self(), :flush, state.flush_ms)
    {:noreply, state}
  rescue
    _ -> {:noreply, state}
  end

  def handle_info(:drain, state) do
    case Relay.claim(100, "relay-runtime") do
      {:ok, rows} -> Enum.each(rows, &emit/1)
      _ -> :ok
    end

    Process.send_after(self(), :drain, state.drain_ms)
    {:noreply, state}
  rescue
    _ -> {:noreply, state}
  end

  defp emit(row),
    do:
      :telemetry.execute(
        String.split(row.event, ".") |> Enum.map(&String.to_atom/1),
        %{count: row.count},
        row.labels
      )

  defp labels(metadata),
    do:
      metadata
      |> Map.take([:scope, :decision, :source, :outcome, :phase, :transport, :via])
      |> Map.new(fn {k, v} -> {k, bounded(v)} end)

  defp bounded(v) when is_atom(v), do: Atom.to_string(v)
  defp bounded(v) when is_binary(v) and byte_size(v) <= 80, do: v
  defp bounded(_), do: "unknown"
end
