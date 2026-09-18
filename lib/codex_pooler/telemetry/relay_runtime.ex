defmodule CodexPooler.Telemetry.RelayRuntime do
  @moduledoc false
  use GenServer

  require Logger

  alias CodexPooler.Gateway.OperationalStatus
  alias CodexPooler.Telemetry.{Relay, RelayEvent}

  # `capacity` slot 1 counts live series, slot 2 samples dropped because the
  # buffer was full, and slot 3 samples the storage layer will never accept.
  @rejected_slot 3

  @events %{
    [:codex_pooler, :quota, :cycle, :decision] => "quota_cycle_decision",
    [:codex_pooler, :saved_reset, :convergence] => "saved_reset_convergence",
    [:codex_pooler, :accounting, :reservation, :pre_attempt_release] => "pre_attempt_release",
    [:codex_pooler, :gateway, :stream, :outcome] => "stream_outcome"
  }
  @source_events Map.new(@events, fn {source, event} -> {event, source} end)

  @spec quiesce(GenServer.server(), timeout()) :: :ok
  def quiesce(server \\ __MODULE__, timeout \\ 5_000) do
    case GenServer.whereis(server) do
      nil ->
        :ok

      pid ->
        close_consumer_gate(pid)
        GenServer.call(pid, :quiesce, timeout)
    end
  catch
    :exit, _ ->
      Logger.warning("telemetry relay quiesce acknowledgement unavailable; claim gate closed")
      :ok
  end

  # The table is owned by the runtime and disappears on any exit, including kill.
  # Lookup happens only at shutdown and avoids persistent state surviving a PID.
  defp close_consumer_gate(pid) do
    for table <- :ets.all(),
        :ets.info(table, :owner) == pid,
        :ets.info(table, :name) == :relay_callbacks,
        [{:producer, false}] == :ets.lookup(table, :producer) do
      :ets.insert(table, {:quiesced, true})
    end
  rescue
    error in ArgumentError ->
      if Process.alive?(pid), do: reraise(error, __STACKTRACE__), else: :ok
  end

  def child_spec(opts),
    do: %{id: __MODULE__, start: {__MODULE__, :start_link, [opts]}, shutdown: 6_000}

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
    quiesced? = not producer? and OperationalStatus.marker_draining?()
    capacity = :atomics.new(3, signed: false)
    capture = {table, capacity, Keyword.get(opts, :max_series, 10_000)}
    callbacks = :ets.new(:relay_callbacks, [:public, :set])
    max_pending = Keyword.get(opts, :max_pending_callbacks, 10_000)
    shards = min(max_pending, 64)
    for shard <- 0..(shards - 1), do: :ets.insert(callbacks, {shard, :open, %{}, 0})

    :ets.insert(callbacks, [{:producer, producer?}, {:quiesced, quiesced?}, {:capture_open, true}])

    handler_config = %{
      capture: capture,
      callbacks: callbacks,
      runtime: self(),
      before_complete: Keyword.get(opts, :before_capture_complete),
      max_pending: max_pending,
      shards: shards
    }

    if producer? do
      :telemetry.attach_many(
        handler,
        Map.keys(@events),
        &__MODULE__.handle_event/4,
        handler_config
      )
    end

    flush_ms = Keyword.get(opts, :flush_ms, 5_000)
    drain_ms = Keyword.get(opts, :drain_ms, 15_000)

    cleanup_fun =
      Keyword.get(opts, :cleanup_fun, fn ->
        Relay.expire_counted()
        Relay.prune()
        Relay.prune_heartbeats()
      end)

    cleanup_interval_ms = Keyword.get(opts, :cleanup_interval_ms, 60_000)

    state = %{
      table: table,
      capture: capture,
      callbacks: callbacks,
      callback_shards: shards,
      producer?: producer?,
      quiesced?: quiesced?,
      overflow_reported: 0,
      rejected_reported: 0,
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
      loss_fun: Keyword.get(opts, :loss_fun, &Relay.checkpoint_loss/3),
      claim_fun: Keyword.get(opts, :claim_fun, &Relay.claim/2),
      heartbeat_fun: Keyword.get(opts, :heartbeat_fun, &Relay.refresh_heartbeat/1),
      consumer_heartbeat_fun:
        Keyword.get(opts, :consumer_heartbeat_fun, &Relay.consumer_heartbeat/2),
      cleanup_interval_ms: cleanup_interval_ms
    }

    if Keyword.get(opts, :start_paused, false),
      do: {:ok, state},
      else: {:ok, state, {:continue, :schedule}}
  end

  @spec handle_event([atom()], map(), map(), map()) :: :ok
  def handle_event(event, measurements, metadata, config) do
    with false <- Process.get({__MODULE__, :draining}, false),
         relay_event when is_binary(relay_event) <- Map.get(@events, event) do
      values =
        sample_values(relay_event, measurements)
        |> Map.put(:count_weight, Map.get(measurements, :count, 1))

      labels = labels(metadata)

      # A sample the changeset will refuse can never be inserted and can never
      # be retried into existence, so capturing it would hold a `max_series`
      # slot forever with nothing to say the sample was lost. It is refused
      # here instead, and counted where it is refused.
      if RelayEvent.storable_measurements?(values) and RelayEvent.storable_labels?(labels),
        do: admit_sample(config, {relay_event, labels, values}),
        else: count_rejected_sample(config)
    end

    :ok
  rescue
    _ -> :ok
  end

  defp admit_sample(config, key) do
    token = make_ref()
    shard = :erlang.phash2(token, config.shards)

    shard_cap =
      div(config.max_pending, config.shards) +
        if(shard < rem(config.max_pending, config.shards), do: 1, else: 0)

    capture_callback(config, shard, token, key, shard_cap)
  end

  defp count_rejected_sample(config) do
    {_table, capacity, _max} = config.capture
    :atomics.add(capacity, @rejected_slot, 1)
  end

  defp capture_callback(config, shard, token, key, shard_cap) do
    case admit_callback(config.callbacks, shard, token, key, shard_cap) do
      :ok ->
        if is_function(config.before_complete, 0), do: config.before_complete.()
        complete_callback(config.callbacks, shard, token)
        send(config.runtime, {:capture_ready, shard})

      :overflow ->
        :ok

      :closed ->
        :ok
    end
  end

  defp admit_callback(table, shard, token, key, max_pending) do
    row =
      if :ets.lookup(table, :capture_open) == [{:capture_open, true}],
        do: :ets.lookup(table, shard),
        else: []

    case row do
      [{^shard, :open, pending, dropped} = old] when map_size(pending) < max_pending ->
        if replace_callbacks(
             table,
             old,
             {shard, :open, Map.put(pending, token, {:pending, key}), dropped}
           ), do: :ok, else: admit_callback(table, shard, token, key, max_pending)

      [{^shard, :open, pending, dropped} = old] ->
        if replace_callbacks(table, old, {shard, :open, pending, dropped + 1}),
          do: :overflow,
          else: admit_callback(table, shard, token, key, max_pending)

      _ ->
        :closed
    end
  end

  defp complete_callback(table, shard, token) do
    case :ets.lookup(table, shard) do
      [{^shard, :open, pending, dropped} = old] ->
        mark_ready(table, shard, token, old, pending, dropped)

      _ ->
        :ok
    end
  end

  defp mark_ready(table, shard, token, old, pending, dropped) do
    case Map.get(pending, token) do
      {:pending, key} ->
        unless replace_callbacks(
                 table,
                 old,
                 {shard, :open, Map.put(pending, token, {:ready, key}), dropped}
               ),
               do: complete_callback(table, shard, token)

      _ ->
        :ok
    end
  end

  defp replace_callbacks(
         table,
         {shard, mode, pending, dropped},
         {shard, next_mode, next_pending, next_dropped}
       ) do
    :ets.select_replace(table, [
      {{shard, :"$1", :"$2", :"$3"},
       [
         {:"=:=", :"$1", {:const, mode}},
         {:"=:=", :"$2", {:const, pending}},
         {:"=:=", :"$3", dropped}
       ], [{{shard, {:const, next_mode}, {:const, next_pending}, next_dropped}}]}
    ]) == 1
  end

  defp collect_callbacks(state, close? \\ false)

  defp collect_callbacks(state, true) do
    :ets.insert(state.callbacks, {:capture_open, false})

    Enum.reduce(0..(state.callback_shards - 1), 0, fn shard, lost ->
      lost + close_callback_shard(state, shard)
    end)
  end

  defp collect_callbacks(state, false) do
    for shard <- 0..(state.callback_shards - 1), do: collect_callback_shard(state, shard)
    0
  end

  defp close_callback_shard(state, shard) do
    [{^shard, _mode, pending, dropped}] = :ets.take(state.callbacks, shard)
    :ets.insert(state.callbacks, {shard, :closed, %{}, 0})
    {_table, capacity, _max} = state.capture
    :atomics.add(capacity, 2, dropped)

    Enum.reduce(pending, 0, fn
      {_, {:ready, key}}, lost ->
        accumulate(state.capture, key, 1)
        lost

      {_, {:pending, _key}}, lost ->
        lost + 1
    end)
  end

  defp collect_callback_shard(state, shard) do
    [{^shard, mode, pending, dropped} = old] = :ets.lookup(state.callbacks, shard)
    {ready, waiting} = Enum.split_with(pending, fn {_, {status, _}} -> status == :ready end)
    remaining = Map.new(waiting)

    if replace_callbacks(state.callbacks, old, {shard, mode, remaining, 0}) do
      {_table, capacity, _max} = state.capture
      :atomics.add(capacity, 2, dropped)
      for {_, {:ready, key}} <- ready, do: accumulate(state.capture, key, 1)
      0
    else
      collect_callback_shard(state, shard)
    end
  end

  # Taken, not filtered. A value the storage layer refuses used to be dropped
  # here while the rest of the sample was captured, which relays a convergence
  # with its duration silently missing. `handle_event/4` refuses the whole
  # sample instead, and says so.
  defp sample_values("saved_reset_convergence", measurements),
    do:
      Map.take(measurements, [
        :applied_to_canonical_ms,
        :canonical_to_lifecycle_ms,
        :applied_to_lifecycle_ms
      ])

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
      safe_consumer_heartbeat(state, false)
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

  def handle_call(:quiesce, _from, %{producer?: true} = state), do: {:reply, :ok, state}
  def handle_call(:quiesce, _from, %{quiesced?: true} = state), do: {:reply, :ok, state}

  def handle_call(:quiesce, _from, state) do
    send(self(), :quiesced_heartbeat)
    {:reply, :ok, %{state | quiesced?: true, drain_again?: false, claim_more?: false}}
  end

  @impl true
  def terminate(_reason, state) do
    :telemetry.detach(state.handler)

    if state.producer? do
      final_flush(state)
    else
      safe_consumer_heartbeat(state, true)
      remaining = Enum.reduce(state.pending, 0, &(&1.count + &2))
      if remaining > 0, do: safe_loss(state, "shutdown_unflushed", remaining)
    end

    :ok
  end

  @impl true
  def handle_info({:capture_ready, shard}, state) do
    collect_callback_shard(state, shard)
    {:noreply, state}
  end

  def handle_info(:quiesced_heartbeat, state) do
    safe_consumer_heartbeat(state, true)
    {:noreply, state}
  end

  def handle_info(:heartbeat, %{producer?: false} = state), do: {:noreply, state}

  def handle_info(:heartbeat, state) do
    refresh_heartbeat(state)
    Process.send_after(self(), :heartbeat, state.heartbeat_ms)
    {:noreply, state}
  end

  def handle_info(:flush, %{producer?: false} = state), do: {:noreply, state}

  def handle_info(:flush, state) do
    state = flush(state)
    Process.send_after(self(), :flush, state.flush_ms)
    {:noreply, state}
  end

  def handle_info(:drain, %{producer?: true} = state), do: {:noreply, state}
  def handle_info(:drain, %{quiesced?: true} = state), do: {:noreply, state}

  def handle_info(:drain, state) do
    if :ets.lookup(state.callbacks, :quiesced) == [{:quiesced, true}] do
      {:noreply, %{state | quiesced?: true}}
    else
      state = drain(state)
      safe_consumer_heartbeat(state, false)
      emit_health()
      delay = if state.drain_again?, do: 0, else: state.drain_ms
      Process.send_after(self(), :drain, delay)
      {:noreply, state}
    end
  end

  @impl true
  def handle_info(:cleanup, state) do
    state.cleanup_fun.()
    Process.send_after(self(), :cleanup, state.cleanup_interval_ms)
    {:noreply, state}
  rescue
    error ->
      # Expiry, loss accounting and pruning share one transaction; a failure
      # here silently disables relay retention until it succeeds, so it must
      # be visible. Only the exception module crosses into the log.
      Logger.warning("telemetry relay cleanup failed reason=#{inspect(error.__struct__)}")
      Process.send_after(self(), :cleanup, state.cleanup_interval_ms)
      {:noreply, state}
  end

  defp flush(state) do
    collect_callbacks(state)

    try do
      :ets.tab2list(state.table)
      |> Enum.each(fn {key, _count} ->
        case :ets.take(state.table, key) do
          [{^key, count}] -> flush_snapshot(state, key, count)
          [] -> :ok
        end
      end)

      {_table, capacity, _max} = state.capture
      dropped = :atomics.get(capacity, 2)

      if dropped > state.overflow_reported,
        do:
          Logger.warning(
            "telemetry relay buffer full dropped_events=#{dropped - state.overflow_reported}"
          )

      if dropped > 0, do: safe_loss(state, "buffer_overflow", dropped)

      # Read once, and act only on a change: the checkpoint is cumulative and
      # already holds what was reported, so writing it again every flush buys a
      # `SELECT … FOR UPDATE` every `flush_ms` for the life of the node. One
      # read also means the number logged is the number recorded.
      rejected = :atomics.get(capacity, @rejected_slot)

      if rejected > state.rejected_reported do
        Logger.warning(
          "telemetry relay refused unstorable samples=#{rejected - state.rejected_reported}"
        )

        safe_loss(state, "rejected_sample", rejected)
      end
    rescue
      _ -> :ok
    end

    {_table, capacity, _max} = state.capture

    %{
      state
      | overflow_reported: :atomics.get(capacity, 2),
        rejected_reported: :atomics.get(capacity, @rejected_slot)
    }
  end

  defp drain(state) do
    {pending, claim_more?} =
      if state.pending == [] and :ets.lookup(state.callbacks, :quiesced) != [{:quiesced, true}] and
           not OperationalStatus.marker_draining?() do
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

  defp safe_consumer_heartbeat(state, quiesced) do
    quiesced = quiesced or :ets.lookup(state.callbacks, :quiesced) == [{:quiesced, true}]
    state.consumer_heartbeat_fun.(state.owner, quiesced)
  rescue
    error ->
      # The consumer heartbeat feeds the fresh-consumer gauge; a silent write
      # failure would look exactly like a dead consumer.
      Logger.warning(
        "telemetry relay consumer heartbeat failed reason=#{inspect(error.__struct__)}"
      )

      :ok
  end

  defp safe_loss(state, reason, count) do
    case state.loss_fun.(state.owner, reason, count) do
      {:ok, :ok} ->
        :ok

      {:error, _} ->
        Logger.warning("telemetry relay loss persistence unavailable samples=#{count}")
    end
  rescue
    _ -> Logger.warning("telemetry relay loss persistence unavailable samples=#{count}")
  end

  defp emit_health do
    health = Relay.health()

    :telemetry.execute(
      [:codex_pooler, :telemetry_relay, :health],
      Map.drop(health, [:losses]),
      %{}
    )

    for [reason, rows, samples] <- health.losses do
      :telemetry.execute(
        [:codex_pooler, :telemetry_relay, :loss],
        %{rows: rows, samples: samples},
        %{reason: reason}
      )
    end
  rescue
    _ -> :ok
  end

  defp final_flush(state) do
    deadline = System.monotonic_time(:millisecond) + 3_000
    pending_loss = collect_callbacks(state, true)
    Process.put({__MODULE__, :flush_deadline}, deadline)
    state = flush(state)

    remaining =
      Enum.reduce(:ets.tab2list(state.table), pending_loss, fn {_key, count}, total ->
        total + count
      end)

    if remaining > 0 do
      safe_loss(state, "shutdown_unflushed", remaining)
      Logger.warning("telemetry relay shutdown unflushed_samples=#{remaining}")
    end
  rescue
    _ -> Logger.warning("telemetry relay shutdown loss persistence unavailable")
  after
    Process.delete({__MODULE__, :flush_deadline})
  end

  defp flush_snapshot(state, {event, labels, values} = key, count) do
    deadline = Process.get({__MODULE__, :flush_deadline})

    result =
      if is_integer(deadline) and System.monotonic_time(:millisecond) >= deadline,
        do: {:error, :shutdown_deadline},
        else: state.insert_fun.(event, labels, count, values, state.owner)

    {_table, capacity, _max} = state.capture

    case result do
      {:ok, _} -> :atomics.sub(capacity, 1, 1)
      {:error, reason} -> settle_failed_flush(state, key, count, reason)
      _ -> accumulate(state.capture, key, count, true)
    end
  rescue
    error -> settle_failed_flush(state, key, count, error)
  end

  # A refusal no retry can fix leaves the buffer and is counted; anything else
  # is re-accumulated, because the next flush may be the one that works.
  # Re-accumulating a refusal keeps the series slot and re-attempts the same
  # insert every flush for as long as the node lives, with no loss reason.
  defp settle_failed_flush(state, key, count, reason) do
    {_table, capacity, _max} = state.capture

    if permanent_refusal?(reason) do
      :atomics.sub(capacity, 1, 1)
      :atomics.add(capacity, @rejected_slot, count)
    else
      accumulate(state.capture, key, count, true)
    end
  end

  # PostgreSQL reserves the first two SQLSTATE bytes for the error class. These
  # classes describe failures of the connection, transaction, resources,
  # object readiness, operator, or server rather than a stable refusal of this
  # row, so every subclass must requeue. Unknown classes remain permanent until
  # deliberately classified; the policy does not claim that every other server
  # answer is intrinsically row-specific.
  @transient_postgres_classes ~w(08 40 53 55 57 58)

  defp permanent_refusal?(%Ecto.Changeset{}), do: true
  defp permanent_refusal?(%Ecto.ConstraintError{}), do: true
  defp permanent_refusal?(%Ecto.InvalidChangesetError{}), do: true

  defp permanent_refusal?(%Postgrex.Error{
         postgres: %{pg_code: <<class::binary-size(2), _::binary>>}
       }),
       do: class not in @transient_postgres_classes

  defp permanent_refusal?(%Postgrex.Error{}), do: false

  defp permanent_refusal?(_other), do: false

  defp emit(row) do
    case Map.get(@source_events, row.event) do
      nil ->
        # Unreachable while the storage allowlist and `@events` agree, which
        # `RelayContractTest` pins. If they ever drift again the row is still
        # lost, but it says so instead of vanishing.
        Logger.warning("telemetry relay drained an unmapped event=#{inspect(row.event)}")
        :ok

      event ->
        measurements = normalize_map(row.measurements)
        labels = normalize_labels(row.labels)

        :telemetry.execute(
          event,
          Map.merge(
            %{count: Map.get(measurements, :count_weight, 1)},
            Map.delete(measurements, :count_weight)
          ),
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

  # Every tag the relayed metric families declare. A relayed event carrying a
  # key outside this list would silently render as `unknown` on the consumer,
  # so the reporter tests assert each relayed metric's tags stay within it.
  @label_keys [
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

  @doc false
  @spec label_keys() :: [atom()]
  def label_keys, do: @label_keys

  @doc false
  @spec relayed_events() :: [[atom()]]
  def relayed_events, do: Map.keys(@events)

  @doc """
  The stored `event` names this runtime can map back to a telemetry event.

  A name the storage allowlist admits and this list omits is claimed on drain
  and then discarded by `emit/1` with no loss reason to count it, so the
  storage allowlist and this list must remain equal in both directions.

  `RelayContractTest` reads the live PostgreSQL `event_allowed` constraint and
  compares it with both this function and `RelayEvent.events/0`; adding a name
  to only one boundary therefore fails against the persisted contract rather
  than relying on this documentation claim.
  """
  @spec relay_event_names() :: [String.t()]
  def relay_event_names, do: Map.values(@events)

  defp labels(metadata),
    do:
      metadata
      |> Map.take(@label_keys)
      |> Map.put_new(:via, "in_process")
      |> Map.new(fn {k, v} -> {k, bounded(v)} end)

  # An atom label is bounded by the same 80 bytes as a binary one rather than
  # passed through: an atom longer than that produced a label value the SQL
  # function and the changeset both refuse, which is a captured sample the
  # storage layer can never accept.
  defp bounded(v) when is_atom(v), do: bounded(Atom.to_string(v))

  # PostgreSQL stores no NUL byte in `text` or `jsonb` (22P05) and no invalid
  # UTF-8 (22021), so a value carrying either is not a short label — it is a
  # value the storage layer refuses, and it used to pass every predicate here
  # and then raise at the insert. Bounded the same way an oversized or
  # non-binary value already is, so the sample it belongs to still counts.
  defp bounded(v) when is_binary(v) and byte_size(v) <= 80 do
    if String.valid?(v) and not String.contains?(v, <<0>>), do: v, else: "unknown"
  end

  defp bounded(_), do: "unknown"

  defp normalize_map(map) when is_map(map) do
    Map.new(map, fn {key, value} ->
      normalized =
        case key do
          "count" -> :count
          "count_weight" -> :count_weight
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
