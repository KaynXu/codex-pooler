defmodule CodexPooler.Gateway.Runtime.Finalization.InterruptionOutcome do
  @moduledoc false

  # The one vocabulary of the `interrupted` stream outcome.
  #
  # A turn is interrupted when it lost the party carrying it rather than
  # failing on its own terms: the client went away (`client_disconnected`), or
  # the Pooler-side owner did — drained by a rollout (`owner_drained`), its
  # lease lost (`owner_unavailable`), or the owner process gone with no drain
  # (`owner_crashed`). The same request would have completed had that party
  # stayed, which is what makes the four one family on the dashboard and one
  # family for the turn's terminal status.
  #
  # Every other terminal failure stays `failed`: a provider rejection, a
  # forwarding refusal (`stale_owner`, `owner_busy`, `owner_forward_timeout`,
  # `owner_forwarding_disabled`), or a connection the client's own newer
  # connection superseded (`duplicate_downstream`, `stale_downstream`). Those
  # answer the client with a status it can act on; nothing was cut mid-flight
  # on the Pooler's side.
  #
  # `owner_crashed` used to be classified `failed` by the stream finalizers
  # while the recovery paths already settled its turn `interrupted`, so the
  # panel counted one loss two ways (findings#228). The SSE finalizer, the
  # websocket owner-failure finalizer and the turn terminal status read this
  # list and nothing else; `InterruptionOutcomeTest` pins the membership.
  @interrupted_error_codes ~w(client_disconnected owner_drained owner_unavailable owner_crashed)

  @spec interrupted_error_codes() :: [String.t()]
  def interrupted_error_codes, do: @interrupted_error_codes

  @spec interrupted_error_code?(term()) :: boolean()
  def interrupted_error_code?(code) when is_binary(code), do: code in @interrupted_error_codes
  def interrupted_error_code?(code) when is_atom(code), do: interrupted_error_code?(Atom.to_string(code))
  def interrupted_error_code?(_code), do: false

  @doc false
  @spec outcome_for_code(term()) :: String.t()
  def outcome_for_code(code), do: if(interrupted_error_code?(code), do: "interrupted", else: "failed")

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
