defmodule CodexPooler.Gateway.Runtime.Finalization.InterruptionOutcome do
  @moduledoc false

  @doc false
  @spec emit(String.t(), String.t()) :: :ok
  def emit(downstream_transport, upstream_transport) do
    :telemetry.execute(
      [:codex_pooler, :gateway, :stream, :outcome],
      %{count: 1},
      %{
        outcome: "interrupted",
        downstream_transport: downstream_transport,
        upstream_transport: upstream_transport
      }
    )
  end
end
