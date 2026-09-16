defmodule CodexPooler.Gateway.Payloads.NativeTurnContinuation do
  @moduledoc false

  # One `turn_id` covers every model request of a Codex turn, so a turn claim
  # alone cannot separate a turn's first request from its tool-result
  # continuations. This predicate is the discriminator: it was written for the
  # websocket codec, and it is the same question on native HTTP, whose body
  # carries the same `client_metadata`, `previous_response_id` and tool-result
  # shapes (`codex-rs/core/src/client.rs:893`). It lives here so both transports
  # read one definition rather than drifting apart.
  #
  # Extracted verbatim from `WebsocketCodec` (`ordinary_native_tool_continuation?/2`
  # and its helpers); the codec delegates here and its behaviour is unchanged.

  alias CodexPooler.Gateway.Payloads.RequestOptions
  alias CodexPooler.Gateway.Payloads.ToolResultShape

  @canonical_metadata_key "x-codex-turn-metadata"

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

  defp ordinary_turn_continuation?(
         %{"client_metadata" => %{@canonical_metadata_key => metadata}} = payload
       ),
       do:
         match?(%{"request_kind" => "turn"}, canonical_metadata_map(metadata)) or
           previous_response_present?(payload)

  defp ordinary_turn_continuation?(payload), do: previous_response_present?(payload)

  defp final_compaction?(input, payload) do
    compaction? =
      &match?(%{"type" => type} when type in ["compaction", "compaction_summary"], &1)

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
