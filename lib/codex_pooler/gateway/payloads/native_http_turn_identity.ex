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
  # ## One turn id, several requests
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
  # But one `turn_id` covers every request made about a turn, not just the turn
  # itself: a compaction, a prewarm, every tool-result continuation and the
  # request that resumes the turn after a compaction all carry it, because they
  # are built from one `TurnMetadataState` (`session.rs:686-701`,
  # `turn_metadata.rs:169`). So the bare claim is reserved for the one request
  # that can be shown to have opened the turn, and everything else that shares
  # the `turn_id` is named by its own payload inside its own domain:
  #
  #   * a compaction request       -> the payload-scoped compaction claim, whose
  #                                   own HMAC domain keeps it clear of the turn
  #   * a `prewarm` or `memory`    -> a payload-scoped claim in a domain named
  #                                   by the declared kind
  #   * a tool-result continuation -> the payload-scoped request claim, which is
  #                                   what keeps the several tool rounds of one
  #                                   turn from colliding with each other
  #   * the request that RESUMES a
  #     turn from its compaction   -> a claim derived from the turn and an opaque
  #                                   digest of that compaction, and from nothing
  #                                   else in the body
  #   * the request that opened it -> the BARE, payload-independent
  #                                   `codex-turn:` claim, which survives any
  #                                   rebuilt body -- including for a turn in a
  #                                   session that has already compacted, which
  #                                   is what keeps this module agreeing with the
  #                                   websocket codec
  #
  # Only the middle three are payload-scoped, and that is deliberate rather than
  # incidental. Ledger row 212-20 is the record of why: a claim is an HMAC over a
  # projection, so whatever the projection includes is what a retry can move, and
  # the client rebuilds its whole `Prompt` from live session state on every
  # attempt rather than resending a serialized body. The two claims that must
  # survive a retry therefore include no body projection at all.
  #
  # Every discriminator above is `NativeTurnContinuation`'s, and this module
  # reaches only the ones that resolve the canonical document through
  # `canonical_document/2`, so a header-only client is classified exactly like a
  # body client. What the two transports do NOT share is more than the
  # discriminators suggest, and the difference is deliberate:
  #
  #   * the ORDER the arms are applied in. The websocket codec gives its native
  #     compaction bridge the bare turn claim on purpose, and its forwarded-final
  #     path depends on that collision to deduplicate -- running this module's
  #     order there was measured to buy a THIRD upstream dispatch where the path
  #     expects two. That is a change to the compaction bridge, not to this
  #     fence, and is tracked separately (findings#212, rows 212-51/212-58).
  #   * `request_kind`. The websocket compaction arm reads it from
  #     `%NativeCodexTurnMetadata{}`, parsed by exact string match, so this
  #     module's trimming and case folding does NOT reach it. A `"TURN"` frame is
  #     a hard `:unsupported_request_kind` rejection on websocket and fences
  #     normally here. The websocket behaviour is the stricter of the two.
  #   * the fail-open gate. A websocket frame always takes some claim because the
  #     claim feeds replay; an HTTP request with nothing to go on keeps its
  #     generated id.
  #
  # KNOWN MISS, inherited by the two payload-scoped claims: a tool-result
  # continuation or a compaction whose retry body has grown is a different claim
  # and is not fenced. That is the price of keeping the several requests of one
  # turn from colliding, and the websocket path has the same miss for the same
  # reason. The prefix arm deliberately does NOT inherit it.
  #
  # ## Failing open
  #
  # A non-native route, a translated `/v1` request, a missing session, an absent
  # header and body document, a malformed document, a document without a usable
  # `turn_id`, and a document declaring a `request_kind` this module has no rule
  # for all return `:none`, which leaves the generated correlation id and
  # today's behaviour exactly as they are.

  alias CodexPooler.Gateway.Payloads.NativeTurnContinuation
  alias CodexPooler.Gateway.Payloads.RequestOptions
  alias CodexPooler.Gateway.Payloads.WebsocketTurnIdentity
  alias CodexPooler.Gateway.Persistence.CodexSession

  @metadata_key "x-codex-turn-metadata"

  # Kinds that are about a turn rather than one of its model requests, and that
  # the released client sends at most once for a given turn. They are fenced in
  # their own domain so a duplicate still costs one dispatch; any other declared
  # kind is unknown and fails open rather than being guessed at.
  @kind_scoped_request_kinds ["prewarm", "memory"]

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
         metadata when not is_nil(metadata) <-
           NativeTurnContinuation.canonical_document(payload, request_options),
         %CodexSession{id: session_id} when is_binary(session_id) <-
           Map.get(request_options.continuity, :codex_session),
         {:ok, identity} <-
           WebsocketTurnIdentity.resolve(canonical_payload(metadata), session_id),
         {:ok, claim} <- claim_for(identity, request_options, payload) do
      {:ok, claim}
    else
      _fail_open -> :none
    end
  end

  def request_claim_key(_request_options, _payload), do: :none

  defp claim_for(identity, request_options, payload) do
    cond do
      NativeTurnContinuation.compaction_request?(payload, request_options) ->
        {:ok, WebsocketTurnIdentity.compaction_claim_key(identity.semantic_turn_key, payload)}

      turn_request?(payload, request_options) ->
        {:ok, turn_claim(identity, payload)}

      true ->
        kind_claim(identity, request_options, payload)
    end
  end

  # One rule, read from `NativeTurnContinuation`, so nothing here re-implements
  # the discriminator the module exists to own (findings#212, row 212-51).
  #
  # WHAT THIS DOES NOT COVER, enumerated because every round of this ticket has
  # closed on an unstated scope:
  #
  #   1. A tool-result continuation is named by its whole payload, so a retry
  #      whose body has grown is a different claim and is not fenced. That is
  #      the price of separating the several tool rounds of one turn, and the
  #      websocket path has the same miss.
  #   2. A compaction request is named by its whole payload, so a compaction
  #      resent with a changed body is not fenced.
  #   3. A request carrying a USER MESSAGE after the last compaction output item
  #      is treated as a turn's opening request, because in the released client
  #      that is a new turn with a new `turn_id`. A caller that reuses one
  #      `turn_id` across a user message is refused rather than served.
  #   4. Nothing here fences a websocket frame. The codec picks its own arms,
  #      and for a post-compaction resume it still picks the bare claim, which
  #      collides with that turn's opener (212-58).
  defp turn_claim(identity, payload) do
    case NativeTurnContinuation.turn_role(payload) do
      :opening ->
        identity.turn_claim_key

      :tool_continuation ->
        WebsocketTurnIdentity.request_claim_key(identity.semantic_turn_key, payload)

      {:post_compaction_resume, anchor} ->
        WebsocketTurnIdentity.resume_claim_key(identity.semantic_turn_key, anchor)
    end
  end

  defp kind_claim(identity, request_options, payload) do
    case NativeTurnContinuation.request_kind(payload, request_options) do
      kind when kind in @kind_scoped_request_kinds ->
        {:ok, WebsocketTurnIdentity.kind_claim_key(identity.semantic_turn_key, payload, kind)}

      _unknown_or_absent ->
        :none
    end
  end

  defp turn_request?(payload, request_options),
    do: NativeTurnContinuation.request_kind(payload, request_options) == "turn"

  # Only the canonical document is offered to the resolver, never the request
  # body, so a body field named `turn_id`/`request_id` cannot become a turn
  # identity through `WebsocketTurnIdentity`'s legacy fallbacks.
  defp canonical_payload(metadata), do: %{"client_metadata" => %{@metadata_key => metadata}}

  # The endpoint list is read at runtime rather than through a module
  # attribute: evaluating `NativeTurnContinuation.native_endpoints/0` at
  # compile time makes this module a compile-time dependent of that one, which
  # `mix quality.xref` refuses (it forces a recompile cascade on every edit to
  # the shared discriminator).
  defp native_route?(%RequestOptions{
         transport: %{transport: transport, upstream_endpoint: endpoint},
         openai_compatibility: %{source_endpoint: nil, openai_chat_payload: nil}
       })
       when is_binary(transport) and transport != "websocket",
       do: endpoint in NativeTurnContinuation.native_endpoints()

  defp native_route?(%RequestOptions{}), do: false
end
