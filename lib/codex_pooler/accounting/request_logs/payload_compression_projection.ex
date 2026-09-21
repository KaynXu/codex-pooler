defmodule CodexPooler.Accounting.RequestLogs.PayloadCompressionProjection do
  @moduledoc false

  alias CodexPooler.Accounting
  alias CodexPooler.Accounting.Attempt

  @spec normalize_metadata(map(), [Attempt.t()]) :: map()
  def normalize_metadata(metadata, attempts) when is_map(metadata) do
    metadata
    |> normalize_request_payload_compression()
    |> maybe_put_attempt_payload_compression(attempts)
  end

  @spec build(map()) :: map() | nil
  def build(%{"payload_compression" => compression}) when is_map(compression) do
    %{
      attempted: true,
      enabled: bool_value(Map.get(compression, "enabled")),
      status: safe_text(Map.get(compression, "status")),
      reason: safe_text(Map.get(compression, "reason")),
      route_class: safe_text(Map.get(compression, "route_class")),
      transport: safe_text(Map.get(compression, "transport")),
      tokenizer: safe_text(Map.get(compression, "tokenizer")),
      strategies: safe_strategies(Map.get(compression, "strategies")),
      candidate_count: non_negative_integer(Map.get(compression, "candidate_count")),
      compressed_count: non_negative_integer(Map.get(compression, "compressed_count")),
      skipped_count: non_negative_integer(Map.get(compression, "skipped_count")),
      tokenizer_input_skipped_count: non_negative_integer(Map.get(compression, "tokenizer_input_skipped_count")),
      original_bytes: non_negative_integer(Map.get(compression, "original_bytes")),
      compressed_bytes: non_negative_integer(Map.get(compression, "compressed_bytes")),
      saved_bytes: non_negative_integer(Map.get(compression, "saved_bytes")),
      byte_savings_percent: non_negative_number(Map.get(compression, "byte_savings_percent")),
      byte_compression_ratio: non_negative_number(Map.get(compression, "compression_ratio")),
      original_tokens: non_negative_integer(Map.get(compression, "original_tokens")),
      compressed_tokens: non_negative_integer(Map.get(compression, "compressed_tokens")),
      saved_tokens: non_negative_integer(Map.get(compression, "saved_tokens")),
      token_savings_percent: non_negative_number(Map.get(compression, "token_savings_percent"))
    }
    |> put_display_metrics()
  end

  def build(_metadata), do: nil

  defp maybe_put_attempt_payload_compression(metadata, attempts) do
    case Map.get(metadata, "payload_compression") do
      compression when is_map(compression) ->
        metadata

      _value ->
        case latest_attempt_payload_compression(attempts) do
          compression when is_map(compression) ->
            Map.put(metadata, "payload_compression", compression)

          nil ->
            metadata
        end
    end
  end

  defp latest_attempt_payload_compression(attempts) do
    attempts
    |> Enum.max_by(& &1.attempt_number, fn -> nil end)
    |> case do
      %Attempt{} = attempt ->
        attempt.response_metadata
        |> Accounting.sanitize_metadata()
        |> Map.get("payload_compression")
        |> normalize_payload_compression()

      nil ->
        nil
    end
  end

  defp normalize_request_payload_compression(metadata) do
    case Map.get(metadata, "payload_compression") do
      compression when is_map(compression) ->
        Map.put(metadata, "payload_compression", normalize_payload_compression(compression))

      _value ->
        metadata
    end
  end

  defp normalize_payload_compression(%{"attempted" => true} = compression) do
    compression
    |> maybe_put_saved_count("original_bytes", "compressed_bytes", "saved_bytes")
    |> maybe_put_saved_count("original_tokens", "compressed_tokens", "saved_tokens")
    |> maybe_put_savings_ratio("byte_savings", "original_bytes", "saved_bytes")
    |> maybe_put_savings_ratio("token_savings", "original_tokens", "saved_tokens")
    |> maybe_put_compression_ratio()
  end

  defp normalize_payload_compression(_compression), do: nil

  defp put_display_metrics(summary) do
    cond do
      metric_available?(
        summary.saved_tokens,
        summary.token_savings_percent,
        summary.original_tokens,
        summary.compressed_tokens
      ) ->
        Map.merge(summary, %{
          unit: "tokens",
          saved_count: summary.saved_tokens,
          savings_percent: summary.token_savings_percent,
          compression_ratio: compression_ratio(summary.original_tokens, summary.compressed_tokens)
        })

      metric_available?(
        summary.saved_bytes,
        summary.byte_savings_percent,
        summary.original_bytes,
        summary.compressed_bytes
      ) ->
        Map.merge(summary, %{
          unit: "bytes",
          saved_count: summary.saved_bytes,
          savings_percent: summary.byte_savings_percent,
          compression_ratio:
            summary.byte_compression_ratio ||
              compression_ratio(summary.original_bytes, summary.compressed_bytes)
        })

      true ->
        Map.merge(summary, %{
          unit: nil,
          saved_count: nil,
          savings_percent: nil,
          compression_ratio: nil
        })
    end
  end

  defp metric_available?(saved, percent, original, compressed) do
    is_integer(saved) and saved > 0 and is_number(percent) and percent > 0 and
      is_integer(original) and original > 0 and is_integer(compressed) and compressed >= 0 and
      compressed < original
  end

  defp maybe_put_saved_count(metadata, original_key, compressed_key, saved_key) do
    saved = non_negative_integer(Map.get(metadata, saved_key))
    original = non_negative_integer(Map.get(metadata, original_key))
    compressed = non_negative_integer(Map.get(metadata, compressed_key))

    cond do
      is_integer(saved) ->
        metadata

      is_integer(original) and is_integer(compressed) ->
        Map.put(metadata, saved_key, max(original - compressed, 0))

      true ->
        metadata
    end
  end

  defp maybe_put_savings_ratio(metadata, prefix, original_key, saved_key) do
    ratio_key = "#{prefix}_ratio"
    percent_key = "#{prefix}_percent"
    original = non_negative_integer(Map.get(metadata, original_key))
    saved = non_negative_integer(Map.get(metadata, saved_key))

    if is_integer(original) and original > 0 and is_integer(saved) do
      ratio =
        non_negative_number(Map.get(metadata, ratio_key)) || Float.round(saved / original, 4)

      percent = non_negative_number(Map.get(metadata, percent_key)) || Float.round(ratio * 100, 2)

      metadata
      |> Map.put(ratio_key, ratio)
      |> Map.put(percent_key, percent)
    else
      metadata
    end
  end

  defp maybe_put_compression_ratio(metadata) do
    original = non_negative_integer(Map.get(metadata, "original_bytes"))
    compressed = non_negative_integer(Map.get(metadata, "compressed_bytes"))

    if is_number(Map.get(metadata, "compression_ratio")) or not is_integer(original) or
         original == 0 or not is_integer(compressed) do
      metadata
    else
      Map.put(metadata, "compression_ratio", compression_ratio(original, compressed))
    end
  end

  defp compression_ratio(original, compressed)
       when is_integer(original) and original > 0 and is_integer(compressed),
       do: Float.round(compressed / original, 4)

  defp compression_ratio(_original, _compressed), do: nil

  defp bool_value(value) when is_boolean(value), do: value
  defp bool_value(_value), do: nil

  defp safe_text(value) when is_binary(value) and value != "[REDACTED]", do: value
  defp safe_text(_value), do: nil

  defp safe_strategies(strategies) when is_list(strategies) do
    strategies
    |> Enum.filter(&(is_binary(&1) and &1 != "[REDACTED]"))
    |> Enum.take(12)
  end

  defp safe_strategies(_strategies), do: []

  defp non_negative_integer(value) when is_integer(value) and value >= 0, do: value
  defp non_negative_integer(_value), do: nil

  defp non_negative_number(value) when is_integer(value) and value >= 0, do: value * 1.0
  defp non_negative_number(value) when is_float(value) and value >= 0, do: value
  defp non_negative_number(_value), do: nil
end
