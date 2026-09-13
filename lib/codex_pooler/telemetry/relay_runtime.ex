defmodule CodexPooler.Telemetry.RelayRuntime do
  @moduledoc false
  use GenServer

  require Logger

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

    role = Keyword.get(opts, :role, System.get_env("OBAN_MODE", "all"))
    producer? = role in ["worker", "scheduler"]
    capacity = :atomics.new(2, signed: false)
    capture = {table, capacity, Keyword.get(opts, :max_series, 10_000)}

    if producer? do
      :telemetry.attach_many(handler, Map.keys(@events), &__MODULE__.handle_event/4, capture)
    end

    flush_ms = Keyword.get(opts, :flush_ms, 5_000)
    drain_ms = Keyword.get(opts, :drain_ms, 15_000)

    cleanup_fun =
      Keyword.get(opts, :cleanup_fun, fn ->
        Relay.expire_counted()
        Relay.prune()
      end)

    cleanup_interval_ms = Keyword.get(opts, :cleanup_interval_ms, 60_000)

    state = %{
      table: table,
      capture: capture,
      producer?: producer?,
      pending: [],
      drain_again?: false,
      claim_more?: false,
      heartbeat_ms: Keyword.get(opts, :heartbeat_ms, 15_000),
      handler: handler,
      owner: Ecto.UUID.generate(),
      flush_ms: flush_ms,
      drain_ms: drain_ms,
      cleanup_fun: cleanup_fun,
      insert_fun: Keyword.get(opts, :insert_fun, &Relay.insert/5),
      claim_fun: Keyword.get(opts, :claim_fun, &Relay.claim/2),
      heartbeat_fun: Keyword.get(opts, :heartbeat_fun, &Relay.refresh_heartbeat/1),
      cleanup_interval_ms: cleanup_interval_ms
    }

    if Keyword.get(opts, :start_paused, false),
      do: {:ok, state},
      else: {:ok, state, {:continue, :schedule}}
  end

  @spec handle_event([atom()], map(), map(), tuple()) :: :ok
  def handle_event(event, measurements, metadata, capture) do
    with false <- Process.get({__MODULE__, :draining}, false),
         relay_event when is_binary(relay_event) <- Map.get(@events, event) do
      values = sample_values(relay_event, measurements)

      # Counter metrics count emissions, while distributions need each original sample.
      accumulate(capture, {relay_event, labels(metadata), values}, 1)
    end

    :ok
  rescue
    _ -> :ok
  end

  defp sample_values("saved_reset_convergence", measurements) do
    measurements
    |> Map.take([:applied_to_canonical_ms, :canonical_to_lifecycle_ms, :applied_to_lifecycle_ms])
    |> Map.filter(fn {_key, value} -> is_number(value) end)
  end

  defp sample_values(_event, _measurements), do: %{}

  defp accumulate({table, capacity, max_series} = capture, key, count, reserved? \\ false) do
    case :ets.lookup(table, key) do
      [] ->
        if reserved? or :atomics.add_get(capacity, 1, 1) <= max_series do
          insert_reserved(capture, key, count)
        else
          :atomics.sub(capacity, 1, 1)
          :atomics.add(capacity, 2, count)
        end

      [{^key, prior}] ->
        replacement = [
          {{:"$1", :"$2"}, [{:"=:=", :"$1", {:const, key}}, {:"=:=", :"$2", prior}],
           [{{:"$1", prior + count}}]}
        ]

        if :ets.select_replace(table, replacement) == 0 do
          accumulate(capture, key, count, reserved?)
        else
          release_reserved(capacity, reserved?)
        end
    end
  end

  defp insert_reserved({table, _capacity, _max} = capture, key, count) do
    unless :ets.insert_new(table, {key, count}), do: accumulate(capture, key, count, true)
  end

  defp release_reserved(capacity, true), do: :atomics.sub(capacity, 1, 1)
  defp release_reserved(_capacity, false), do: :ok

  @impl true
  def handle_continue(:schedule, state) do
    if state.producer? do
      refresh_heartbeat(state)
      Process.send_after(self(), :heartbeat, state.heartbeat_ms)
      Process.send_after(self(), :flush, state.flush_ms)
    else
      Process.send_after(self(), :drain, state.drain_ms)
      Process.send_after(self(), :cleanup, state.cleanup_interval_ms)
    end

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
  def handle_info(:heartbeat, %{producer?: false} = state), do: {:noreply, state}

  def handle_info(:heartbeat, state) do
    refresh_heartbeat(state)
    Process.send_after(self(), :heartbeat, state.heartbeat_ms)
    {:noreply, state}
  end

  def handle_info(:flush, %{producer?: false} = state), do: {:noreply, state}

  def handle_info(:flush, state) do
    try do
      :ets.tab2list(state.table)
      |> Enum.each(fn {key, _count} ->
        case :ets.take(state.table, key) do
          [{^key, count}] -> flush_snapshot(state, key, count)
          [] -> :ok
        end
      end)

      {_table, capacity, _max} = state.capture
      dropped = :atomics.exchange(capacity, 2, 0)
      if dropped > 0, do: Logger.warning("telemetry relay buffer full dropped_events=#{dropped}")
    after
      Process.send_after(self(), :flush, state.flush_ms)
    end

    {:noreply, state}
  end

  def handle_info(:drain, %{producer?: true} = state), do: {:noreply, state}

  def handle_info(:drain, state) do
    state = drain(state)
    delay = if state.drain_again?, do: 0, else: state.drain_ms
    Process.send_after(self(), :drain, delay)
    {:noreply, state}
  end

  @impl true
  def handle_info(:cleanup, state) do
    state.cleanup_fun.()
    Process.send_after(self(), :cleanup, state.cleanup_interval_ms)
    {:noreply, state}
  rescue
    _ ->
      Process.send_after(self(), :cleanup, state.cleanup_interval_ms)
      {:noreply, state}
  end

  defp drain(state) do
    {pending, claim_more?} =
      if state.pending == [] do
        case state.claim_fun.(100, state.owner) do
          {:ok, rows} -> {rows, length(rows) == 100}
          _ -> {[], false}
        end
      else
        {state.pending, state.claim_more?}
      end

    remaining = emit_pending(pending, 100)

    %{
      state
      | pending: remaining,
        claim_more?: claim_more?,
        drain_again?: remaining != [] or claim_more?
    }
  rescue
    _ -> %{state | drain_again?: false}
  end

  defp emit_pending([], _budget), do: []
  defp emit_pending(rows, 0), do: rows

  defp emit_pending([%{count: count} = row | rest], budget) when count > 0 do
    safe_emit(%{row | count: 1})
    remaining = if count == 1, do: rest, else: [%{row | count: count - 1} | rest]
    emit_pending(remaining, budget - 1)
  end

  defp emit_pending([_row | rest], budget), do: emit_pending(rest, budget)

  defp refresh_heartbeat(state) do
    state.heartbeat_fun.(state.owner)
  rescue
    _ -> {:error, :unavailable}
  end

  defp flush_snapshot(state, {event, labels, values} = key, count) do
    case state.insert_fun.(event, labels, count, values, state.owner) do
      {:ok, _} ->
        {_table, capacity, _max} = state.capture
        :atomics.sub(capacity, 1, 1)

      _ ->
        accumulate(state.capture, key, count, true)
    end
  rescue
    _ -> accumulate(state.capture, key, count, true)
  end

  defp emit(row) do
    case Map.get(@source_events, row.event) do
      nil ->
        :ok

      event ->
        measurements = normalize_map(row.measurements)
        labels = normalize_labels(row.labels)

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
      |> Map.take([
        :scope,
        :decision,
        :source,
        :outcome,
        :phase,
        :transport,
        :downstream_transport,
        :upstream_transport,
        :via
      ])
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

  defp normalize_labels(map) when is_map(map) do
    keys = [
      :scope,
      :decision,
      :source,
      :outcome,
      :phase,
      :transport,
      :downstream_transport,
      :upstream_transport,
      :via
    ]

    Map.new(keys, fn key ->
      {key, bounded(Map.get(map, Atom.to_string(key), Map.get(map, key)))}
    end)
  end

  defp normalize_labels(_), do: %{}
end
