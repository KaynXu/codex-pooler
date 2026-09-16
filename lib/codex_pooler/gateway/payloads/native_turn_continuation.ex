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
  # ## The canonical document has two carriers, and both are authoritative
  #
  # The body's `client_metadata` is what a websocket frame carries and what the
  # released client sends over HTTP; the `x-codex-turn-metadata` request header
  # is a bounded projection of it (`responses_metadata.rs:354-372`). Every
  # reader here resolves body-or-header through `canonical_document/2`, so a
  # client that sends only the header is classified exactly like one that sends
  # the body. Reading the two carriers in different places is what fenced a
  # header-only client against its own turn (212-49).
  #
  # ## What separates a turn's opening request from its later requests
  #
  # Nothing in the canonical document does: a turn's compaction, its
  # continuations and its resume all carry one `turn_id` and one `request_kind`
  # because they are built from one `TurnMetadataState` (`session.rs:686-701`,
  # `turn_metadata.rs:169`). The input history is the only signal, and exactly
  # two shapes prove a request cannot be the one that opened its turn:
  #
  #   * it carries a tool result -- the turn already ran a tool, so a previous
  #     request of it produced the call
  #   * it carries a compaction output item -- a compaction of this thread has
  #     already completed, and the turn is being resumed from its summary
  #     (`compact.rs:600-660` keeps that item last; `protocol/src/models.rs:1224`
  #     serialises it as `compaction` with the `compaction_summary` alias, and
  #     `context_compaction` is its sibling)
  #
  # A request with neither is treated as the request that opens its turn and is
  # named by the payload-independent claim. This is deliberately conservative in
  # one direction only: a turn opened in a thread that was compacted earlier
  # carries the older compaction item in its history, so it is named by its
  # payload rather than by the turn alone. That costs the bare claim's survival
  # of a rebuilt retry body for those turns -- the same KNOWN MISS a tool
  # continuation already has -- and it never refuses a request that has no
  # duplicate, which is what the alternative did: classifying the resume as an
  # opening request refused every native HTTP turn that triggered a remote
  # compaction, one request after the compaction itself (212-48).
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
  # `compaction_summary` serde alias.
  @compaction_item_types ["compaction", "compaction_summary", "context_compaction"]

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
  @spec ordinary_tool_continuation?(map(), RequestOptions.t()) :: boolean()
  def ordinary_tool_continuation?(
        %{"input" => input} = payload,
        %RequestOptions{
          native_compaction_admission: nil,
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
  True when this request can be the one that opened its turn: its input carries
  neither a tool result nor a compaction output item. See the module comment for
  why those two shapes, and only those two, are decisive.

  A payload with no list `input` cannot be shown to be a later request of its
  turn, so it fails open to the turn's own claim.
  """
  @spec turn_opening_request?(map(), RequestOptions.t()) :: boolean()
  def turn_opening_request?(%{"input" => input}, %RequestOptions{}) when is_list(input) do
    not (ToolResultShape.any?(input) or Enum.any?(input, &compaction_item?/1))
  end

  def turn_opening_request?(payload, %RequestOptions{}) when is_map(payload), do: true

  def turn_opening_request?(_payload, _options), do: true

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

  @doc "True when the payload carries a nonblank `previous_response_id` anchor."
  @spec previous_response_present?(map()) :: boolean()
  def previous_response_present?(%{"previous_response_id" => value}) when is_binary(value),
    do: String.trim(value) != ""

  def previous_response_present?(_payload), do: false

  defp body_document(%{"client_metadata" => %{@canonical_metadata_key => metadata}})
       when is_map(metadata) or (is_binary(metadata) and metadata != ""),
       do: metadata

  defp body_document(_payload), do: nil

  # A repeated header takes the first value, exactly as the forwarded header
  # list was built; a native Codex client sends it once.
  defp header_document(%RequestOptions{transport: %{forwarded_metadata_headers: headers}})
       when is_list(headers) do
    Enum.find_value(headers, fn
      {@canonical_metadata_key, value} when is_binary(value) and value != "" -> value
      _other -> nil
    end)
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

  defp ordinary_turn_continuation?(
         %{"client_metadata" => %{@canonical_metadata_key => metadata}} = payload
       ),
       do:
         match?(%{"request_kind" => "turn"}, canonical_metadata_map(metadata)) or
           previous_response_present?(payload)

  defp ordinary_turn_continuation?(payload), do: previous_response_present?(payload)

  defp final_compaction?(input, payload) do
    compaction? = &match?(%{"type" => type} when type in ["compaction", "compaction_summary"], &1)

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
