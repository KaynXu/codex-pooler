defmodule CodexPooler.Platform.ExecutionProofPublisher do
  @moduledoc false
  use GenServer
  require Logger

  alias CodexPooler.Platform.{ExecutionRegistry, ExecutionTerminalProofs}
  @interval_ms 1_000

  @spec start_link(keyword()) :: GenServer.on_start() | :ignore
  def start_link(opts) do
    configured = Application.get_env(:codex_pooler, __MODULE__, [])

    if Keyword.get(opts, :enabled, Keyword.get(configured, :enabled, true)),
      do: GenServer.start_link(__MODULE__, opts, name: Keyword.get(opts, :name, __MODULE__)),
      else: :ignore
  end

  @impl true
  def init(opts),
    do: {:ok, %{registry: Keyword.get(opts, :registry, ExecutionRegistry), timer: nil, failed: false}, {:continue, :publish}}

  @impl true
  def handle_continue(:publish, state), do: {:noreply, publish(state)}

  @impl true
  def handle_info(:publish, state), do: {:noreply, publish(state)}

  defp publish(state) do
    if state.timer, do: Process.cancel_timer(state.timer)
    result = publish_pending(state.registry)

    if result == :error and not state.failed,
      do: Logger.warning("execution terminal proof publication unavailable; pending proofs retained")

    %{state | timer: Process.send_after(self(), :publish, @interval_ms), failed: result == :error}
  end

  defp publish_pending(registry) do
    if Process.whereis(CodexPooler.Repo), do: publish_available(registry), else: :error
  end

  defp publish_available(registry) do
    case ExecutionRegistry.pending(100, registry) do
      [] ->
        :ok

      proofs when is_list(proofs) ->
        case ExecutionTerminalProofs.publish(proofs) do
          {:ok, _} ->
            ExecutionRegistry.acknowledge(Enum.map(proofs, & &1.owner_execution_id), registry)

          {:error, _} ->
            :error
        end

      :unknown ->
        :error
    end
  rescue
    _error in [DBConnection.ConnectionError, Postgrex.Error] -> :error
  catch
    :exit, _ -> :error
  end
end
