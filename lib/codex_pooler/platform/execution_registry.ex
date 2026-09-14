defmodule CodexPooler.Platform.ExecutionRegistry do
  @moduledoc false
  use GenServer

  @retention_ms :timer.hours(6)

  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, nil, name: Keyword.get(opts, :name, __MODULE__))
  end

  @spec register(String.t(), GenServer.server()) :: :ok | :unknown
  def register(id, server \\ __MODULE__), do: call(server, {:register, id})

  @spec complete(String.t(), GenServer.server()) :: :ok | :unknown
  def complete(id, server \\ __MODULE__), do: call(server, {:complete, id})

  @spec status(String.t(), pid(), GenServer.server()) :: :alive | :dead | :unknown
  def status(id, pid, server \\ __MODULE__), do: call(server, {:status, id, pid})

  defp call(server, request) do
    GenServer.call(server, request, 1_000)
  catch
    :exit, _ -> :unknown
  end

  @impl true
  def init(nil), do: {:ok, %{entries: %{}, monitors: %{}}}

  @impl true
  def handle_call({:register, id}, {pid, _}, state) do
    case Map.get(state.entries, id) do
      nil ->
        ref = Process.monitor(pid)

        state = %{
          state
          | entries: Map.put(state.entries, id, {pid, ref}),
            monitors: Map.put(state.monitors, ref, id)
        }

        {:reply, :ok, state}

      {^pid, ref} when is_reference(ref) ->
        {:reply, :ok, state}

      _ ->
        {:reply, :unknown, state}
    end
  end

  def handle_call({:complete, id}, {pid, _}, state) do
    case Map.get(state.entries, id) do
      {^pid, ref} when is_reference(ref) -> {:reply, :ok, retire(state, id, pid, ref)}
      _ -> {:reply, :unknown, state}
    end
  end

  def handle_call({:status, id, pid}, _from, state) do
    case Map.get(state.entries, id) do
      {^pid, :dead} ->
        {:reply, :dead, state}

      {^pid, ref} when is_reference(ref) ->
        if Process.alive?(pid) do
          {:reply, :alive, state}
        else
          {:reply, :dead, retire(state, id, pid, ref)}
        end

      _ ->
        {:reply, :unknown, state}
    end
  end

  @impl true
  def handle_info({:DOWN, ref, :process, pid, _reason}, state) do
    case Map.get(state.monitors, ref) do
      nil -> {:noreply, state}
      id -> {:noreply, retire(state, id, pid, ref)}
    end
  end

  def handle_info({:expire, id}, state) do
    {:noreply, %{state | entries: Map.delete(state.entries, id)}}
  end

  defp retire(state, id, pid, ref) do
    Process.demonitor(ref, [:flush])
    Process.send_after(self(), {:expire, id}, @retention_ms)

    %{
      state
      | entries: Map.put(state.entries, id, {pid, :dead}),
        monitors: Map.delete(state.monitors, ref)
    }
  end
end
