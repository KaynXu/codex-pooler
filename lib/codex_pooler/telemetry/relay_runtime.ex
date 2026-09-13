defmodule CodexPooler.Telemetry.RelayRuntime do
  @moduledoc false
  use GenServer

  alias CodexPooler.Telemetry.Relay

  @events %{
    [:codex_pooler, :quota, :cycle, :decision] => "quota_cycle_decision",
    [:codex_pooler, :saved_reset, :convergence] => "saved_reset_convergence",
    [:codex_pooler, :accounting, :reservation, :pre_attempt_release] => "pre_attempt_release",
    [:codex_pooler, :gateway, :stream, :outcome] => "stream_outcome"
  }
  @source_events Map.new(@events, fn {source, event} -> {event, source} end)

  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts \\ []) do
    # Sandboxed tests start and allow their own runtime before activating it.
    enabled =
      Keyword.get(opts, :enabled, CodexPooler.Repo.config()[:pool] != Ecto.Adapters.SQL.Sandbox)

    if enabled do
      GenServer.start_link(__MODULE__, opts, name: Keyword.get(opts, :name, __MODULE__))
    else
      :ignore
    end
  end

  @impl true
  def init(opts) do
    Process.flag(:trap_exit, true)
    table = :ets.new(__MODULE__, [:public, :set, read_concurrency: true])
    handler = {__MODULE__, self()}

    :telemetry.attach_many(
      handler,
      Map.keys(@events),
      &__MODULE__.handle_event/4,
      table
    )

    flush_ms = Keyword.get(opts, :flush_ms, 5_000)
    drain_ms = Keyword.get(opts, :drain_ms, 15_000)

    state = %{
      table: table,
      handler: handler,
      owner: Ecto.UUID.generate(),
      flush_ms: flush_ms,
      drain_ms: drain_ms
    }

    if Keyword.get(opts, :start_paused, false),
      do: {:ok, state},
      else: {:ok, state, {:continue, :schedule}}
  end

  def handle_event(event, measurements, metadata, table) do
    with false <- Process.get({__MODULE__, :draining}, false),
         relay_event when is_binary(relay_event) <- Map.get(@events, event),
         labels when is_map(labels) <- labels(metadata),
         count when is_integer(count) and count > 0 <- Map.get(measurements, :count, 1) do
      key = {relay_event, labels}

      values = Map.filter(measurements, fn {_key, value} -> is_number(value) end)
      accumulate(table, key, Map.put(values, :count, count))
    else
      _ -> :ok
    end
  rescue
    _ -> :ok
  end

  # Only a writer racing this same key retries. Keeping the key in the replacement
  # also lets ETS atomically reject a stale snapshot after a concurrent flush.
  defp accumulate(table, key, measurements) do
    case :ets.lookup(table, key) do
      [] ->
        unless :ets.insert_new(table, {key, measurements}),
          do: accumulate(table, key, measurements)

      [{^key, prior}] ->
        value = Map.merge(prior, measurements, fn _key, old, added -> old + added end)

        replacement = [
          {{:"$1", :"$2"}, [{:"=:=", :"$1", {:const, key}}, {:"=:=", :"$2", {:const, prior}}],
           [{{:"$1", {:const, value}}}]}
        ]

        if :ets.select_replace(table, replacement) == 0,
          do: accumulate(table, key, measurements)
    end
  end

  @impl true
  def handle_continue(:schedule, state) do
    :ok = Relay.refresh_heartbeat(state.owner)
    Process.send_after(self(), :heartbeat, 15_000)
    Process.send_after(self(), :flush, state.flush_ms)
    Process.send_after(self(), :drain, state.drain_ms)
    Process.send_after(self(), :cleanup, 60_000)
    {:noreply, state}
  end

  @impl true
  def handle_call(:activate, _from, state) do
    {:noreply, state} = handle_continue(:schedule, state)
    {:reply, :ok, state}
  end

  @impl true
  def terminate(_reason, state), do: :telemetry.detach(state.handler)

  @impl true
  def handle_info(:heartbeat, state) do
    :ok = Relay.refresh_heartbeat(state.owner)
    Process.send_after(self(), :heartbeat, 15_000)
    {:noreply, state}
  end

  def handle_info(:flush, state) do
    :ets.tab2list(state.table)
    |> Enum.each(fn {key = {event, labels}, _snapshot} ->
      case :ets.take(state.table, key) do
        [{^key, value}] ->
          flush_snapshot(state, key, event, labels, value)

        [] ->
          :ok
      end
    end)

    Process.send_after(self(), :flush, state.flush_ms)
    {:noreply, state}
  rescue
    _ -> {:noreply, state}
  end

  def handle_info(:drain, state) do
    case Relay.claim(100, state.owner) do
      {:ok, rows} -> Enum.each(rows, &safe_emit/1)
      _ -> :ok
    end

    Process.send_after(self(), :drain, state.drain_ms)
    {:noreply, state}
  rescue
    _ -> {:noreply, state}
  end

  @impl true
  def handle_info(:cleanup, state) do
    _expired = Relay.expire_counted()
    _pruned = Relay.prune()
    Process.send_after(self(), :cleanup, 60_000)
    {:noreply, state}
  rescue
    _ ->
      Process.send_after(self(), :cleanup, 60_000)
      {:noreply, state}
  end

  defp flush_snapshot(state, key, event, labels, value) do
    case Relay.insert(event, labels, Map.get(value, :count, 1), value, state.owner) do
      {:ok, _} -> :ok
      _ -> accumulate(state.table, key, value)
    end
  end

  defp emit(row) do
    case Map.get(@source_events, row.event) do
      nil ->
        :ok

      event ->
        measurements = normalize_map(row.measurements)
        labels = normalize_map(row.labels)

        :telemetry.execute(
          event,
          Map.merge(%{count: row.count}, measurements),
          Map.put(labels, :via, "job_relay")
        )
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

  defp normalize_map(map) when is_map(map) do
    Map.new(map, fn {key, value} ->
      normalized =
        case key do
          "count" -> :count
          "applied_to_canonical_ms" -> :applied_to_canonical_ms
          "canonical_to_lifecycle_ms" -> :canonical_to_lifecycle_ms
          "applied_to_lifecycle_ms" -> :applied_to_lifecycle_ms
          other -> other
        end

      {normalized, value}
    end)
  end

  defp normalize_map(_), do: %{}
end
