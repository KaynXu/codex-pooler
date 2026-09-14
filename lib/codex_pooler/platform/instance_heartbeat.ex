defmodule CodexPooler.Platform.InstanceHeartbeat do
  @moduledoc """
  Publishes this instance's presence row on an interval.

  The heartbeat is a recovery hint, never a request-path dependency: a failed
  write is logged at warning and counted through telemetry, then retried on
  the next tick. Stale presence is not proof of death: database unavailability
  and pool starvation can prevent a live owner from publishing.

  The process runs in every release role, because any role that serves HTTP
  can own an in-flight stream.
  """

  use GenServer

  require Logger

  alias CodexPooler.Platform.InstancePresence

  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts) do
    if enabled?(opts) do
      GenServer.start_link(__MODULE__, opts, name: Keyword.get(opts, :name, __MODULE__))
    else
      :ignore
    end
  end

  @impl GenServer
  def init(opts) do
    state = %{
      identity: Keyword.get(opts, :identity, InstancePresence.local_identity()),
      interval_ms: Keyword.get(opts, :interval_ms, InstancePresence.heartbeat_interval_ms()),
      timer_ref: nil
    }

    {:ok, state, {:continue, :publish}}
  end

  @impl GenServer
  def handle_continue(:publish, state), do: {:noreply, publish(state)}

  @impl GenServer
  def handle_info(:publish, state), do: {:noreply, publish(state)}

  def handle_info(_message, state), do: {:noreply, state}

  defp publish(state) do
    _result = write(state.identity)
    schedule(state)
  end

  defp write(identity) do
    case InstancePresence.record_heartbeat(identity) do
      {:ok, _instance} -> :ok
      {:error, _reason} -> log_failure()
    end
  rescue
    _error -> log_failure()
  catch
    :exit, _reason -> log_failure()
  end

  # The instance id is left out of the log line on purpose; the count of missed
  # beats is what matters and the identity is already durable in the row.
  defp log_failure do
    Logger.warning("instance presence heartbeat write failed")
    :telemetry.execute([:codex_pooler, :instance_presence, :heartbeat], %{failures: 1}, %{})
    :error
  end

  defp schedule(state) do
    state = cancel_timer(state)
    %{state | timer_ref: Process.send_after(self(), :publish, state.interval_ms)}
  end

  defp cancel_timer(%{timer_ref: nil} = state), do: state

  defp cancel_timer(%{timer_ref: timer_ref} = state) do
    _cancelled = Process.cancel_timer(timer_ref)
    %{state | timer_ref: nil}
  end

  defp enabled?(opts) do
    Keyword.get(opts, :enabled, configured_enabled?())
  end

  defp configured_enabled? do
    :codex_pooler
    |> Application.get_env(__MODULE__, [])
    |> Keyword.get(:enabled, true)
  end
end
