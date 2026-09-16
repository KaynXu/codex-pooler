defmodule CodexPooler.Gateway.Payloads.NativeHttpTurnIdentity do
  @moduledoc false

  # The duplicate-turn fence was structurally websocket-only (findings#212): a
  # native Codex turn sent over `POST /backend-api/codex/responses` reserved
  # under a freshly generated UUID, so a resend of the same turn never met
  # `requests_correlation_id_uq`, never reached the resend policy, and bought a
  # second upstream dispatch. The same resend over a websocket is refused
  # `409 duplicate_turn`.
  #
  # The identity is not missing on HTTP, only the resolver was: the released
  # client sends the same `x-codex-turn-metadata` document it puts in a
  # websocket frame's `client_metadata`, as an inbound request header that is
  # already carried in `transport.forwarded_metadata_headers` and projected
  # upstream. This module resolves that header into the claim the websocket
  # path already derives, by handing the exact canonical shape to
  # `WebsocketTurnIdentity` rather than re-deriving anything: both transports
  # then name one turn the same way.
  #
  # HTTP always takes the payload-scoped request claim, never the bare
  # `codex-turn:` turn claim. One `turn_id` covers every model request of a
  # turn, including its tool-result continuations, so fencing on the turn claim
  # alone would collide two genuinely different requests of the same turn. The
  # websocket path reaches the same conclusion through
  # `ordinary_native_tool_continuation?/2`; HTTP has no turn-level admission to
  # pair a bare turn claim with, so it scopes every claim by payload.
  #
  # It fails open by construction. A non-native route, a translated `/v1`
  # request, a missing session, an absent header, a malformed document and a
  # document without a usable `turn_id` all return `:none`, which leaves the
  # random correlation id and today's behaviour exactly as they are.

  alias CodexPooler.Gateway.Payloads.RequestOptions
  alias CodexPooler.Gateway.Payloads.WebsocketTurnIdentity
  alias CodexPooler.Gateway.Persistence.CodexSession

  @metadata_header "x-codex-turn-metadata"

  # The routes that forward the metadata header upstream
  # (`UpstreamDispatch.@regular_runtime_metadata_endpoints`). Keep the two
  # lists together: a route that does not carry the header cannot be fenced by
  # it.
  @native_endpoints [
    "/backend-api/codex/responses",
    "/backend-api/codex/responses/compact"
  ]

  @doc """
  True when this request carries a native Codex turn identity the reservation
  can fence on. Payload-independent, so callers that only have request options
  (constraint classification, denial logging) can ask the same question.
  """
  @spec fenced?(RequestOptions.t()) :: boolean()
  def fenced?(%RequestOptions{} = request_options),
    do: is_binary(turn_metadata_header(request_options))

  def fenced?(_request_options), do: false

  @doc """
  Resolves the durable request claim for a native Codex HTTP turn, or `:none`
  when no usable identity is present.
  """
  @spec request_claim_key(RequestOptions.t(), map()) :: {:ok, String.t()} | :none
  def request_claim_key(%RequestOptions{} = request_options, payload) when is_map(payload) do
    with header when is_binary(header) <- turn_metadata_header(request_options),
         %CodexSession{id: session_id} when is_binary(session_id) <-
           Map.get(request_options.continuity, :codex_session),
         {:ok, %{semantic_turn_key: semantic_turn_key}} <-
           WebsocketTurnIdentity.resolve(canonical_payload(header), session_id) do
      {:ok, WebsocketTurnIdentity.request_claim_key(semantic_turn_key, payload)}
    else
      _fail_open -> :none
    end
  end

  def request_claim_key(_request_options, _payload), do: :none

  # `WebsocketTurnIdentity.resolve/2` reads the canonical document out of
  # `client_metadata`, decoding a JSON string exactly as the frame path does.
  # Only the header is offered, so an HTTP body that happens to carry a
  # top-level `turn_id`/`request_id` cannot become a turn identity through the
  # legacy fallbacks.
  defp canonical_payload(header), do: %{"client_metadata" => %{@metadata_header => header}}

  defp turn_metadata_header(%RequestOptions{
         transport: %{
           transport: transport,
           upstream_endpoint: endpoint,
           forwarded_metadata_headers: headers
         },
         openai_compatibility: %{source_endpoint: nil, openai_chat_payload: nil}
       })
       when transport != "websocket" and is_binary(transport) and is_list(headers) and
              endpoint in @native_endpoints do
    Enum.find_value(headers, fn
      {@metadata_header, value} when is_binary(value) and value != "" -> value
      _other -> nil
    end)
  end

  defp turn_metadata_header(%RequestOptions{}), do: nil
end
