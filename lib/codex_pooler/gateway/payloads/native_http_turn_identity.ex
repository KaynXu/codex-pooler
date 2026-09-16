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
  # client puts the same canonical `x-codex-turn-metadata` document in the HTTP
  # body's `client_metadata` that it puts in a websocket frame
  # (`codex-rs/core/src/client.rs:893`), and echoes a bounded copy as a request
  # header. Either source yields the same `turn_id`, and the derivation is
  # `WebsocketTurnIdentity`'s own, so both transports name one turn the same way.
  #
  # ## Three arms, because the turn id is not the request id
  #
  # The claim must survive a *rebuilt* retry body. The released client records
  # each completed output item into history as it arrives and rebuilds the
  # retry prompt from `clone_history()` with no rollback
  # (`stream_events_utils.rs:300-380`, `session/turn.rs:1578-1583`,
  # `responses_retry.rs`), so any cut that already delivered an item retries
  # with a LONGER body. A payload-scoped claim therefore misses exactly the
  # cohort the row measures -- turns still relaying ~85 s after preStop, which
  # by construction have delivered items.
  #
  # So this mirrors the websocket `cond` (`websocket_codec.ex:938-965`) rather
  # than inventing a second rule, and it mirrors ALL THREE of its arms. One
  # `turn_id` covers every request made about a turn, not just the turn itself:
  # the released client builds a compaction request's metadata from the same
  # `turn_metadata_state` as the turn it compacts (`session.rs:686-701`,
  # `turn_metadata.rs:169`), and the bare claim encodes no endpoint, so a
  # compaction that took the bare claim would collide with its own turn and
  # refuse a request that has no duplicate at all.
  #
  #   * a compaction request      -> the payload-scoped `codex-request:`
  #                                  compaction claim, whose own HMAC domain
  #                                  keeps it clear of the turn
  #   * a tool-result continuation -> the payload-scoped request claim, which is
  #                                  what keeps the several requests of one turn
  #                                  from colliding with each other
  #   * the turn itself            -> the BARE, payload-independent `codex-turn:`
  #                                  claim, which survives any rebuilt body
  #
  # The continuation discriminator is the shared `NativeTurnContinuation`
  # predicate, so the two transports cannot drift on it.
  #
  # Only an explicit `request_kind` of `turn` may reach the bare claim. A
  # `prewarm` or `memory` request, an unknown kind, and a document that omits
  # the field are all left unfenced rather than guessed at: they are requests
  # *about* a turn that can carry its `turn_id`, and the cost of being wrong
  # about one is a refusal of a request that never had a duplicate.
  #
  # ## Failing open
  #
  # A non-native route, a translated `/v1` request, a missing session, an absent
  # header and body document, a malformed document and a document without a
  # usable `turn_id` all return `:none`, which leaves the generated correlation
  # id and today's behaviour exactly as they are.

  alias CodexPooler.Gateway.Payloads.NativeTurnContinuation
  alias CodexPooler.Gateway.Payloads.RequestOptions
  alias CodexPooler.Gateway.Payloads.WebsocketTurnIdentity
  alias CodexPooler.Gateway.Persistence.CodexSession

  @metadata_key "x-codex-turn-metadata"

  # The routes that carry the canonical turn metadata
  # (`UpstreamDispatch.@regular_runtime_metadata_endpoints`). Keep the two lists
  # together: a route that does not carry the document cannot be fenced by it.
  @compact_endpoint "/backend-api/codex/responses/compact"
  @native_endpoints ["/backend-api/codex/responses", @compact_endpoint]

  @doc """
  True when this request is a native Codex HTTP turn, i.e. on a route this
  fence can apply to. Deliberately payload-independent and route-level: the
  identity itself usually lives in the body, which callers that only hold
  request options (constraint classification, rejection logging) do not have.
  """
  @spec fenced?(RequestOptions.t()) :: boolean()
  def fenced?(%RequestOptions{} = request_options), do: native_route?(request_options)

  def fenced?(_request_options), do: false

  @doc """
  Resolves the durable request claim for a native Codex HTTP turn, or `:none`
  when no usable identity is present.
  """
  @spec request_claim_key(RequestOptions.t(), map()) :: {:ok, String.t()} | :none
  def request_claim_key(%RequestOptions{} = request_options, payload) when is_map(payload) do
    with true <- native_route?(request_options),
         metadata when not is_nil(metadata) <- turn_metadata(request_options, payload),
         %CodexSession{id: session_id} when is_binary(session_id) <-
           Map.get(request_options.continuity, :codex_session),
         {:ok, identity} <-
           WebsocketTurnIdentity.resolve(canonical_payload(metadata), session_id),
         {:ok, claim} <- claim_for(identity, request_options, payload, metadata) do
      {:ok, claim}
    else
      _fail_open -> :none
    end
  end

  def request_claim_key(_request_options, _payload), do: :none

  defp claim_for(identity, request_options, payload, metadata) do
    cond do
      compaction_request?(request_options, metadata) ->
        {:ok, WebsocketTurnIdentity.compaction_claim_key(identity.semantic_turn_key, payload)}

      NativeTurnContinuation.ordinary_tool_continuation?(payload, request_options) ->
        {:ok, WebsocketTurnIdentity.request_claim_key(identity.semantic_turn_key, payload)}

      turn_request?(metadata) ->
        {:ok, identity.turn_claim_key}

      true ->
        :none
    end
  end

  # Either signal is enough, and neither is trusted alone: the canonical kind is
  # what the client declares, and the compact endpoint is what the request
  # actually is. A compaction that reached the turn arm would refuse its own
  # turn.
  defp compaction_request?(%RequestOptions{transport: %{upstream_endpoint: endpoint}}, metadata),
    do: endpoint == @compact_endpoint or request_kind(metadata) == "compaction"

  defp turn_request?(metadata), do: request_kind(metadata) == "turn"

  defp request_kind(metadata) do
    case NativeTurnContinuation.canonical_metadata_map(metadata) do
      %{"request_kind" => kind} when is_binary(kind) -> kind
      _absent -> nil
    end
  end

  # The body is the authoritative document -- it is what the websocket frame
  # carries, and the header copy is deliberately a bounded projection of it
  # (`responses_metadata.rs:354-372`). A client that sends only the header is
  # still fenced.
  defp turn_metadata(request_options, payload) do
    body_metadata(payload) || header_metadata(request_options)
  end

  defp body_metadata(%{"client_metadata" => %{@metadata_key => metadata}})
       when is_map(metadata) or (is_binary(metadata) and metadata != ""),
       do: metadata

  defp body_metadata(_payload), do: nil

  defp header_metadata(%RequestOptions{transport: %{forwarded_metadata_headers: headers}})
       when is_list(headers) do
    Enum.find_value(headers, fn
      {@metadata_key, value} when is_binary(value) and value != "" -> value
      _other -> nil
    end)
  end

  defp header_metadata(%RequestOptions{}), do: nil

  # Only the canonical document is offered to the resolver, never the request
  # body, so a body field named `turn_id`/`request_id` cannot become a turn
  # identity through `WebsocketTurnIdentity`'s legacy fallbacks.
  defp canonical_payload(metadata), do: %{"client_metadata" => %{@metadata_key => metadata}}

  defp native_route?(%RequestOptions{
         transport: %{transport: transport, upstream_endpoint: endpoint},
         openai_compatibility: %{source_endpoint: nil, openai_chat_payload: nil}
       })
       when is_binary(transport) and transport != "websocket",
       do: endpoint in @native_endpoints

  defp native_route?(%RequestOptions{}), do: false
end
