defmodule CodexPooler.Gateway.Payloads.NativeTurnContinuation do
  @moduledoc false

  # One `turn_id` covers every model request of a Codex turn, so a turn claim
  # alone cannot separate a turn's first request from its tool-result
  # continuations, its compaction, or the request that resumes it afterwards.
  # This module owns those discriminators. They were written for the websocket
  # codec, and they are the same questions on native HTTP, whose body carries
  # the same `client_metadata`, `previous_response_id`, compaction and
  # tool-result shapes (`codex-rs/core/src/client.rs:893`). They live here so
  # both transports read one definition rather than drifting apart
  # (findings#212, rows 212-48/212-49/212-51).
  #
  # What lives here is every DISCRIMINATOR, not the claim selection itself: the
  # websocket codec applies its own arms in its own order because its native
  # compaction bridge deliberately puts a compaction on the turn's bare claim
  # and its forwarded-final path deduplicates on that collision. Anything that
  # changes what a compaction, a tool continuation, an opening request or a
  # `request_kind` IS belongs here and reaches both transports; anything that
  # changes which claim the websocket bridge picks belongs to that bridge.
  #
  # ## The canonical document has two carriers, and both are authoritative
  #
  # The body's `client_metadata` is what a websocket frame carries and what the
  # released client sends over HTTP; the `x-codex-turn-metadata` request header
  # is a bounded projection of it (`responses_metadata.rs:354-372`). Every reader
  # THE NATIVE HTTP CLAIM REACHES resolves body-or-header through
  # `canonical_document/2`, so a client that sends only the header is classified
  # exactly like one that sends the body. Reading the two carriers in different
  # places is what fenced a header-only client against its own turn (212-49).
  #
  # `ordinary_tool_continuation?/2` is the exception, and the exception is
  # enforced rather than described: it and the three private helpers beneath it
  # (`ordinary_turn_continuation?/1`, `final_compaction?/2` and
  # `previous_response_present?/1`) read the body alone, they are the websocket
  # codec's arm, and its head requires a websocket transport so an HTTP caller
  # cannot get a header-blind answer out of it. All three helpers are private, so
  # none of them can acquire a caller that bypasses that head. A websocket frame
  # always carries the document, so nothing is lost there.
  #
  # ## What separates a turn's opening request from its later requests
  #
  # Nothing in the canonical document does: a turn's compaction, its
  # continuations and its resume all carry one `turn_id` and one `request_kind`
  # because they are built from one `TurnMetadataState` (`session.rs:686-701`,
  # `turn_metadata.rs:169`). The input history is the only signal, and
  # `turn_role/1` is the whole rule -- the claim resolver and the tests both call
  # it, so there is no second copy to drift.
  #
  # Remote compaction REPLACES the session history
  # (`compact_remote_history.rs:118`, `compact_remote_v2.rs:510`) and pushes the
  # compaction output item last, so every turn for the rest of a session that
  # compacts once carries that item. It is therefore the pivot, and what follows
  # it is what decides -- a tool result, a user message, or nothing.
  #
  # Two earlier answers to this question are worth recording because each cost a
  # round. Treating any compaction item as proof that a request is NOT its turn's
  # opener refused every native HTTP turn that triggered a remote compaction, one
  # request after the compaction itself. Then naming those requests by their
  # payload -- whole, or narrowed to the prefix through the compaction item --
  # broke the two properties the bare claim exists for: it stopped agreeing with
  # the websocket codec, which gives such a frame the bare claim, so an HTTPS
  # fallback of a drained websocket turn bought a second billed dispatch; and it
  # made the claim move whenever any body field the client rebuilds moved, which
  # ledger row 212-20 had predicted in as many words. A turn's opener keeps the
  # bare claim in a compacted session for exactly the same reasons it keeps it in
  # an uncompacted one.
  #
  # Note that the compaction *trigger* is not a compaction output item. Remote
  # compaction V2 appends `ResponseItem::CompactionTrigger {}`
  # (`compact_remote_v2_attempt.rs:78`), which serialises as
  # `compaction_trigger`, and declares `request_kind: "compaction"`, so it is
  # caught by `compaction_request?/2` before the opening-request question is
  # ever asked.

  alias CodexPooler.Gateway.Payloads.RequestOptions
  alias CodexPooler.Gateway.Payloads.ToolResultShape

  @canonical_metadata_key "x-codex-turn-metadata"

  # The routes that carry the canonical turn metadata. `UpstreamDispatch` keeps
  # its own copy for a different question (which headers it forwards upstream);
  # every reader of the duplicate-turn fence reads these.
  @compact_endpoint "/backend-api/codex/responses/compact"
  @native_endpoints ["/backend-api/codex/responses", @compact_endpoint]

  # `ResponseItem::Compaction` and `ResponseItem::ContextCompaction`, plus the
  # `compaction_summary` serde alias. This wider list answers "has this thread
  # been compacted at all".
  @compaction_item_types ["compaction", "compaction_summary", "context_compaction"]

  # Deliberately narrower, and kept beside its sibling so the two lists a reader
  # is asked to compare are visible together. `final_compaction?/2` asks "did the
  # model's compaction output land at the end of this frame", which
  # `context_compaction` -- a durable input control rather than a compaction
  # result -- does not answer.
  @final_compaction_item_types ["compaction", "compaction_summary"]

  @anchor_domain "native_turn_compaction_anchor_v1"

  @type turn_role :: :opening | :tool_continuation | {:post_compaction_resume, <<_::256>>}

  @max_request_kind_bytes 128

  @doc "The native Codex compaction route."
  @spec compact_endpoint() :: String.t()
  def compact_endpoint, do: @compact_endpoint

  @doc "The native Codex routes that carry the canonical turn metadata document."
  @spec native_endpoints() :: [String.t()]
  def native_endpoints, do: @native_endpoints

  @doc """
  True for an ordinary native continuation of a turn already in flight: a
  tool-result round, rather than the request that opens the turn.
  """
  # This arm and its two helpers read the canonical document from the BODY only,
  # which is correct for the transport that reaches them and wrong for the other
  # one -- a websocket frame always carries the document, an HTTP request may
  # carry only the header. Rather than leave that as a comment a future caller
  # can miss, the head requires a websocket transport: an HTTP caller gets
  # `false` instead of a header-blind answer, which is the defect 212-49 fixed.
  @spec ordinary_tool_continuation?(map(), RequestOptions.t()) :: boolean()
  def ordinary_tool_continuation?(
        %{"input" => input} = payload,
        %RequestOptions{
          native_compaction_admission: nil,
          transport: %{transport: "websocket"},
          payload_context: %{compaction_trigger_bridge?: false},
          openai_compatibility: %{public_openai_responses_stream: false}
        }
      )
      when is_list(input) do
    ordinary_turn_continuation?(payload) and ToolResultShape.any?(input) and
      not final_compaction?(input, payload)
  end

  def ordinary_tool_continuation?(_payload, %RequestOptions{}), do: false

  @doc """
  True when this request is a compaction of a turn rather than a request of the
  turn itself.

  Either signal is enough and neither is trusted alone: the canonical kind is
  what the client declares, and the compact endpoint is what the request
  actually is. The released client has no `/compact` URL -- remote compaction V2
  is an ordinary Responses request declaring `request_kind: "compaction"`
  (`compact_remote_v2_attempt.rs:78`, `session.rs:685-701`) -- so the kind is
  the signal that carries real traffic and the endpoint covers the Pooler's own
  bridge-rewritten `upstream_endpoint`.
  """
  @spec compaction_request?(map(), RequestOptions.t()) :: boolean()
  def compaction_request?(payload, %RequestOptions{} = options) when is_map(payload) do
    upstream_endpoint(options) == @compact_endpoint or
      request_kind(payload, options) == "compaction"
  end

  def compaction_request?(_payload, _options), do: false

  @doc """
  Which request of its turn this is, from the payload alone.

  One `turn_id` covers every model request of a Codex turn, so this is the only
  thing that separates them, and it is the LIVE rule -- both the claim resolver
  and the tests call this function, so there is no second copy to drift
  (findings#212, row 212-51).

  The compaction output item is the pivot, because remote compaction replaces
  the session history with the retained items followed by that item
  (`compact_remote_v2.rs:510`, `compact_remote_history.rs:118`). Everything that
  matters is therefore in the segment AFTER the last such item:

    * a tool result there -> `:tool_continuation`. A previous request of this
      turn produced the call.
    * a user message there -> `:opening`. The user started something, which in
      the released client means a new turn with a new `turn_id`
      (`turn_metadata.rs` mints one per `TurnMetadataState`), so this is that
      turn's first request and it keeps the payload-independent claim. This is
      what makes a turn in a compacted session behave exactly like a turn in an
      uncompacted one -- including agreeing with the websocket codec, which
      gives such a frame the bare claim too.
    * neither -> `{:post_compaction_resume, anchor}`. The model is being asked
      to continue from the compaction it just produced. `anchor` is an opaque
      digest of the compaction items alone, so it is identical across every
      retry of that resume no matter what else in the body changed, and
      different from the compaction of any other turn.

  With no compaction output item the question is the older one: a tool result
  anywhere in the input is `:tool_continuation`, everything else is `:opening`.

  A payload with no list `input` is `:opening`, which is the FENCED direction
  rather than the open one -- two different requests of one turn with a non-list
  `input` would collide. That shape is unreachable through the native routes,
  which reject a non-list `input` with `400 invalid_request` before the fence is
  consulted; a future payload coercion that made it reachable would have to
  revisit this clause.
  """
  @spec turn_role(map()) :: turn_role()
  def turn_role(%{"input" => input}) when is_list(input) do
    case last_compaction_index(input) do
      nil ->
        if ToolResultShape.any?(input), do: :tool_continuation, else: :opening

      index ->
        compacted_turn_role(input, index)
    end
  end

  def turn_role(_payload), do: :opening

  defp compacted_turn_role(input, index) do
    tail = Enum.drop(input, index + 1)

    cond do
      ToolResultShape.any?(tail) -> :tool_continuation
      Enum.any?(tail, &user_message?/1) -> :opening
      true -> {:post_compaction_resume, compaction_anchor(input)}
    end
  end

  # An opaque digest of the compaction items, and nothing else in the body. This
  # is what makes the resume claim payload-independent: ledger row 212-20 says
  # the claim is an HMAC over the payload projection and that the projection is
  # where the fence lives, so the projection here is the one part of the body a
  # retry cannot regenerate. Raw input never leaves this module.
  defp compaction_anchor(input) do
    items = Enum.filter(input, &compaction_item?/1)

    :crypto.hash(
      :sha256,
      :erlang.term_to_binary({@anchor_domain, items}, [:deterministic])
    )
  end

  defp user_message?(%{"role" => "user"} = item),
    do: Map.get(item, "type", "message") == "message"

  defp user_message?(_item), do: false

  @doc """
  The declared `request_kind`, resolved from the body document or the header
  copy, trimmed and case folded.

  The released client emits the lowercase literal (`responses_metadata.rs:165-172`),
  but an exact byte comparison would let any intermediary that normalizes the
  document switch the whole fence off with `"TURN"` or a trailing space
  (findings#212, row 212-53). An oversized or blank value resolves to `nil`,
  which is the unfenced outcome, not a match.
  """
  @spec request_kind(map(), RequestOptions.t()) :: String.t() | nil
  def request_kind(payload, %RequestOptions{} = options) do
    case canonical_metadata_map(canonical_document(payload, options)) do
      %{"request_kind" => kind} when is_binary(kind) -> normalize_request_kind(kind)
      _absent -> nil
    end
  end

  def request_kind(_payload, _options), do: nil

  @doc """
  The canonical turn metadata document for this request, from the body's
  `client_metadata` or the forwarded `x-codex-turn-metadata` header, or `nil`.

  The body wins when both are present: it is what the websocket frame carries
  and the header is deliberately a bounded projection of it.
  """
  @spec canonical_document(map(), RequestOptions.t()) :: map() | String.t() | nil
  def canonical_document(payload, %RequestOptions{} = options) when is_map(payload),
    do: body_document(payload) || header_document(options)

  def canonical_document(_payload, %RequestOptions{} = options), do: header_document(options)

  def canonical_document(_payload, _options), do: nil

  @doc "Decodes the canonical turn metadata document, from a map or a JSON string."
  @spec canonical_metadata_map(term()) :: map()
  def canonical_metadata_map(metadata) when is_map(metadata), do: metadata

  def canonical_metadata_map(metadata) when is_binary(metadata) do
    case CodexPooler.JSON.decode(metadata) do
      {:ok, decoded} when is_map(decoded) -> decoded
      _invalid -> %{}
    end
  end

  def canonical_metadata_map(_metadata), do: %{}

  # Body-only, like the two helpers above it, and private so it cannot acquire
  # an HTTP caller that would get a body-blind answer out of it.
  defp previous_response_present?(%{"previous_response_id" => value}) when is_binary(value),
    do: String.trim(value) != ""

  defp previous_response_present?(_payload), do: false

  defp body_document(%{"client_metadata" => %{@canonical_metadata_key => metadata}})
       when is_map(metadata) or (is_binary(metadata) and metadata != ""),
       do: metadata

  defp body_document(_payload), do: nil

  # A native Codex client sends this header once. Two DIFFERENT values for it
  # mean an intermediary put them there, and nothing in the request says which
  # turn it belongs to -- so the document is treated as absent and the request
  # keeps its generated id, the same outcome as a malformed one. Silently taking
  # the first would let an injected header choose a turn's identity, and the
  # header is the only carrier a header-only client has (findings#212, 212-34).
  defp header_document(%RequestOptions{transport: %{forwarded_metadata_headers: headers}})
       when is_list(headers) do
    headers
    |> Enum.flat_map(fn
      {@canonical_metadata_key, value} when is_binary(value) and value != "" -> [value]
      _other -> []
    end)
    |> Enum.uniq()
    |> case do
      [value] -> value
      _absent_or_ambiguous -> nil
    end
  end

  defp header_document(%RequestOptions{}), do: nil

  defp upstream_endpoint(%RequestOptions{transport: %{upstream_endpoint: endpoint}}), do: endpoint
  defp upstream_endpoint(%RequestOptions{}), do: nil

  defp normalize_request_kind(kind) when byte_size(kind) <= @max_request_kind_bytes do
    case kind |> String.trim() |> String.downcase() do
      "" -> nil
      normalized -> normalized
    end
  end

  defp normalize_request_kind(_kind), do: nil

  defp compaction_item?(%{"type" => type}) when type in @compaction_item_types, do: true
  defp compaction_item?(_item), do: false

  defp last_compaction_index(input) do
    input
    |> Enum.with_index()
    |> Enum.reduce(nil, fn {item, index}, last ->
      if compaction_item?(item), do: index, else: last
    end)
  end

  defp ordinary_turn_continuation?(
         %{"client_metadata" => %{@canonical_metadata_key => metadata}} = payload
       ),
       do:
         match?(%{"request_kind" => "turn"}, canonical_metadata_map(metadata)) or
           previous_response_present?(payload)

  defp ordinary_turn_continuation?(payload), do: previous_response_present?(payload)

  defp final_compaction?(input, payload) do
    compaction? = &match?(%{"type" => type} when type in @final_compaction_item_types, &1)

    if Enum.any?(input, compaction?) do
      metadata = get_in(payload, ["client_metadata", @canonical_metadata_key])
      after_compaction = input |> Enum.reverse() |> Enum.take_while(&(not compaction?.(&1)))

      not (match?(%{"request_kind" => "turn"}, canonical_metadata_map(metadata)) and
             ToolResultShape.any?(after_compaction))
    else
      false
    end
  end
end
