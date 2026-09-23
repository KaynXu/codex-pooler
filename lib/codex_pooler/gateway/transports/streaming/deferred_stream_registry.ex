defmodule CodexPooler.Gateway.Transports.Streaming.DeferredStreamRegistry do
  @moduledoc """
  Node-local registry of deferred HTTP SSE streams for rollout drain.

  A deferred stream is the controller closure that runs `StreamRelay` inside
  the connection process after the gateway has already reserved the request and
  dispatched the attempt. Websocket work registers in
  `Gateway.Transports.Websocket.ActivityRegistry`, whose entries carry
  websocket-only concerns (cancellation/delivery-acknowledgement recipients,
  direct-cleanup receipts, and `:direct`/`:proxy`/`:local_owner` kinds). A
  deferred HTTP stream has none of those: it owns a `Plug.Conn`, it cannot be
  cancelled by an external process, and it must settle its own request and
  attempt. This sibling registry therefore tracks only what the drain needs —
  the stream process, its `request_id`/`attempt_id`, and one interruption
  signal the stream itself consumes.

  Registration is refcounted per process: a first-event retry builds a nested
  deferred stream in the same connection process and must keep the one token
  the relay already selects on.

  The registry is deliberately node-local. Postgres remains the durable
  authority; this only accelerates the local case so a rolling restart settles
  its own in-flight streams instead of leaving them to the stale-reservation
  sweep.
  """

  use GenServer

  @type outcome :: :completed | :aborted | :failed
  @type token :: reference()
  @type reason :: :owner_drained
  @type drain_entry :: %{
          required(:token) => token(),
          required(:pid) => pid(),
          required(:request_id) => String.t() | nil,
          required(:attempt_id) => String.t() | nil,
          required(:phase) => :admitted | :streaming,
          required(:status) => :active | {:finished, outcome()}
        }

  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts) do
    GenServer.start_link(__MODULE__, :ok, name: Keyword.get(opts, :name, __MODULE__))
  end

  def child_spec(opts) do
    name = Keyword.get(opts, :name, __MODULE__)

    %{
      id: {__MODULE__, name},
      start: {__MODULE__, :start_link, [opts]},
      type: :worker
    }
  end

  @doc """
  Registers the calling process as a deferred stream.

  Returns the token the relay selects on, or `nil` when no registry is running
  so a stream never fails because drain bookkeeping is unavailable. A stream
  that registers after the drain cutoff is signalled immediately, so no stream
  can escape a drain that already started.
  """
  @spec register(map(), keyword()) :: token() | nil
  def register(attrs, opts \\ []) when is_map(attrs) do
    case GenServer.whereis(server(opts)) do
      nil -> nil
      server -> GenServer.call(server, {:register, self(), attrs})
    end
  end

  @spec admit(keyword()) :: {:ok, token() | nil} | {:error, :owner_drained}
  def admit(opts \\ []) do
    case GenServer.whereis(server(opts)) do
      nil -> {:ok, nil}
      server -> GenServer.call(server, {:admit, self()})
    end
  end

  @spec checkpoint(keyword()) :: :ok | {:error, :owner_drained}
  def checkpoint(opts \\ []) do
    case GenServer.whereis(server(opts)) do
      nil -> :ok
      server -> GenServer.call(server, {:checkpoint, self()})
    end
  end

  @doc """
  Releases one registration depth for `token` and records `outcome` when the
  outermost deferred stream of that process returns.
  """
  @spec finish(token() | nil, outcome(), keyword()) :: :ok
  def finish(token, outcome, opts \\ [])

  def finish(nil, _outcome, _opts), do: :ok

  def finish(token, outcome, opts)
      when is_reference(token) and outcome in [:completed, :aborted, :failed] do
    case GenServer.whereis(server(opts)) do
      nil -> :ok
      server -> GenServer.call(server, {:finish, token, outcome})
    end
  end

  @spec interrupt(token(), reason(), keyword()) :: :ok
  def interrupt(token, reason \\ :owner_drained, opts \\ []) when is_reference(token) do
    GenServer.call(server(opts), {:interrupt, token, reason})
  end

  @spec begin_drain(keyword()) :: {reference(), [drain_entry()]}
  def begin_drain(opts \\ []),
    do: GenServer.call(server(opts), {:begin_drain, Keyword.get(opts, :deadline)})

  @spec drain_entries(keyword()) :: [drain_entry()]
  def drain_entries(opts \\ []), do: GenServer.call(server(opts), :drain_entries)

  @spec complete_drain(reference(), keyword()) :: :ok
  def complete_drain(epoch, opts \\ []) when is_reference(epoch) do
    GenServer.call(server(opts), {:complete_drain, epoch})
  end

  @spec status(token(), keyword()) :: {:active, :registered} | {:finished, outcome()} | :unknown
  def status(token, opts \\ []) when is_reference(token) do
    GenServer.call(server(opts), {:status, token})
  end

  @spec streams(keyword()) :: [drain_entry()]
  def streams(opts \\ []), do: GenServer.call(server(opts), :streams)

  @spec draining?(keyword()) :: boolean()
  def draining?(opts \\ []), do: GenServer.call(server(opts), :draining?)

  @doc """
  Builds the message a drained deferred stream receives. The relay selects on
  this exact shape, so a stale message from an earlier request on a reused
  keep-alive connection process can never match a later token.
  """
  @spec drain_message(token(), reason()) :: {:gateway_stream_drain, token(), reason()}
  def drain_message(token, reason \\ :owner_drained) when is_reference(token) do
    {:gateway_stream_drain, token, reason}
  end

  @impl GenServer
  def init(:ok) do
    {:ok, %{streams: %{}, pids: %{}, monitors: %{}, draining?: false, drain: nil, deadline: nil}}
  end

  @impl GenServer
  def handle_call({:admit, _pid}, _from, %{draining?: true} = state),
    do: {:reply, {:error, :owner_drained}, state}

  def handle_call({:admit, pid}, _from, state) do
    {token, state} = insert_registration(state, pid, %{phase: :admitted})
    {:reply, {:ok, token}, state}
  end

  def handle_call({:checkpoint, pid}, _from, state) do
    reply =
      if Map.has_key?(state.pids, pid) and cutoff?(state), do: {:error, :owner_drained}, else: :ok

    {:reply, reply, state}
  end

  def handle_call({:register, pid, attrs}, _from, state) do
    case Map.fetch(state.pids, pid) do
      {:ok, token} ->
        if cutoff?(state), do: send(pid, drain_message(token))
        {:reply, token, refresh_registration(state, token, attrs)}

      :error ->
        {token, state} = insert_registration(state, pid, attrs)
        if cutoff?(state), do: send(pid, drain_message(token))
        {:reply, token, state}
    end
  end

  def handle_call({:finish, token, outcome}, _from, state) do
    case Map.fetch(state.streams, token) do
      {:ok, %{depth: depth}} when depth > 1 ->
        state =
          update_in(state.streams[token], fn entry ->
            %{entry | depth: depth - 1, outcome: merge_outcome(entry.outcome, outcome)}
          end)

        {:reply, :ok, state}

      {:ok, _entry} ->
        {:reply, :ok, finish_stream(state, token, outcome, true)}

      :error ->
        {:reply, :ok, state}
    end
  end

  def handle_call({:interrupt, token, reason}, _from, state) do
    case Map.fetch(state.streams, token) do
      {:ok, %{pid: pid}} ->
        send(pid, drain_message(token, reason))
        {:reply, :ok, put_in(state.streams[token].interrupted?, true)}

      :error ->
        {:reply, :ok, state}
    end
  end

  def handle_call({:begin_drain, deadline}, _from, %{drain: nil} = state) do
    epoch = make_ref()
    drain = %{epoch: epoch, tokens: state.streams |> Map.keys() |> MapSet.new(), outcomes: %{}}
    state = %{state | draining?: true, drain: drain, deadline: state.deadline || deadline}
    {:reply, {epoch, entries(state)}, state}
  end

  def handle_call({:begin_drain, _deadline}, _from, state) do
    {:reply, {state.drain.epoch, entries(state)}, state}
  end

  def handle_call(:drain_entries, _from, state), do: {:reply, entries(state), state}

  def handle_call({:complete_drain, epoch}, _from, %{drain: %{epoch: epoch}} = state) do
    {:reply, :ok, %{state | drain: nil}}
  end

  def handle_call({:complete_drain, _epoch}, _from, state), do: {:reply, :ok, state}

  def handle_call({:status, token}, _from, state) do
    reply =
      case Map.get(state.streams, token) do
        %{} -> {:active, :registered}
        nil -> drain_outcome(state.drain, token)
      end

    {:reply, reply, state}
  end

  def handle_call(:streams, _from, state) do
    streams =
      for {token, %{phase: :streaming} = entry} <- state.streams,
          do: public_entry(token, entry)

    {:reply, streams, state}
  end

  def handle_call(:draining?, _from, state), do: {:reply, state.draining?, state}

  @impl GenServer
  def handle_info({:DOWN, monitor, :process, _pid, _reason}, state) do
    case Map.pop(state.monitors, monitor) do
      {nil, _monitors} ->
        {:noreply, state}

      {token, monitors} ->
        {:noreply, finish_stream(%{state | monitors: monitors}, token, :failed, false)}
    end
  end

  def handle_info(_message, state), do: {:noreply, state}

  defp insert_registration(state, pid, attrs) do
    token = make_ref()
    monitor = Process.monitor(pid)

    entry = %{
      pid: pid,
      monitor: monitor,
      depth: 1,
      interrupted?: false,
      phase: Map.get(attrs, :phase, :streaming),
      outcome: :completed,
      request_id: Map.get(attrs, :request_id),
      attempt_id: Map.get(attrs, :attempt_id)
    }

    state = %{
      state
      | streams: Map.put(state.streams, token, entry),
        pids: Map.put(state.pids, pid, token),
        monitors: Map.put(state.monitors, monitor, token)
    }

    state = if state.drain, do: update_in(state.drain.tokens, &MapSet.put(&1, token)), else: state

    {token, state}
  end

  # A first-event retry re-registers the same process against the newly
  # dispatched attempt. Keep the token the relay already selects on and move
  # the metadata forward.
  defp refresh_registration(state, token, attrs) do
    update_in(state.streams[token], fn entry ->
      %{
        entry
        | depth: entry.depth + 1,
          request_id: Map.get(attrs, :request_id, entry.request_id),
          attempt_id: Map.get(attrs, :attempt_id, entry.attempt_id),
          phase: :streaming
      }
    end)
  end

  defp finish_stream(state, token, outcome, demonitor?) do
    case Map.pop(state.streams, token) do
      {nil, _streams} ->
        state

      {entry, streams} ->
        if demonitor?, do: Process.demonitor(entry.monitor, [:flush])

        state = %{
          state
          | streams: streams,
            pids: Map.delete(state.pids, entry.pid),
            monitors: Map.delete(state.monitors, entry.monitor)
        }

        %{
          state
          | drain:
              record_drain_outcome(
                state.drain,
                token,
                entry,
                merge_outcome(entry.outcome, outcome)
              )
        }
    end
  end

  defp record_drain_outcome(nil, _token, _entry, _outcome), do: nil

  defp record_drain_outcome(drain, token, entry, outcome) do
    if MapSet.member?(drain.tokens, token) do
      put_in(drain.outcomes[token], %{entry: entry, outcome: outcome})
    else
      drain
    end
  end

  defp drain_outcome(nil, _token), do: :unknown

  defp drain_outcome(drain, token) do
    case Map.get(drain.outcomes, token) do
      %{outcome: outcome} -> {:finished, outcome}
      nil -> :unknown
    end
  end

  defp entries(%{drain: nil}), do: []

  defp entries(%{drain: drain} = state) do
    Enum.map(drain.tokens, fn token ->
      case Map.get(state.streams, token) do
        nil ->
          %{entry: entry, outcome: outcome} = Map.fetch!(drain.outcomes, token)
          token |> public_entry(entry) |> Map.put(:status, {:finished, outcome})

        entry ->
          public_entry(token, entry)
      end
    end)
  end

  defp public_entry(token, entry) do
    %{
      token: token,
      pid: entry.pid,
      request_id: entry.request_id,
      attempt_id: entry.attempt_id,
      phase: entry.phase,
      status: :active
    }
  end

  defp cutoff?(%{draining?: false}), do: false
  defp cutoff?(%{deadline: nil}), do: true
  defp cutoff?(%{deadline: %{at: at, now_ms: now_ms}}), do: now_ms.() >= at

  defp merge_outcome(:failed, _), do: :failed
  defp merge_outcome(_, :failed), do: :failed
  defp merge_outcome(:aborted, _), do: :aborted
  defp merge_outcome(_, outcome), do: outcome

  # Mirrors `RolloutDrain.configured_server_name/1`: tests point registration at
  # an isolated registry so a drain in one test can never flip the global
  # registry into draining and signal a later test's stream.
  defp server(opts) do
    Keyword.get(opts, :name) ||
      :codex_pooler
      |> Application.get_env(__MODULE__, [])
      |> Keyword.get(:server_name, __MODULE__)
  end
end
