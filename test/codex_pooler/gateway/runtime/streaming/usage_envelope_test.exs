defmodule CodexPooler.Gateway.Runtime.Streaming.UsageEnvelopeTest do
  use ExUnit.Case, async: true

  alias CodexPooler.Gateway.Runtime.Streaming.{StreamUsageObserver, UsageEnvelope}

  test "large unselected strings do not require per-byte usage extraction work" do
    payload =
      ~s({"type":"response.completed","response":{"output":[{"encrypted_content":") <>
        String.duplicate("A", 1_048_576) <>
        ~s("}],"usage":{"input_tokens":10,"output_tokens":2,"total_tokens":12},"service_tier":"priority"}})

    stream = "event: response.completed\ndata: " <> payload <> "\n\n"

    for chunk_size <- [1024, 4096, 65_536] do
      chunks = chunks(stream, chunk_size)

      observe = fn ->
        Enum.reduce(chunks, StreamUsageObserver.new(), &StreamUsageObserver.observe(&2, &1))
      end

      observe.()
      {:reductions, before_count} = Process.info(self(), :reductions)
      state = observe.()
      {:reductions, after_count} = Process.info(self(), :reductions)

      assert %{input_tokens: 10, output_tokens: 2, total_tokens: 12, service_tier: "priority"} =
               StreamUsageObserver.usage(state)

      assert after_count - before_count < 1_000_000
    end
  end

  test "unselected string spans preserve escape and UTF-8 validation across every split" do
    valid = [
      ~S(a\"b\\c\n\u0000\uD83D\uDE00),
      <<?a, 0xE9::utf8, 0x20AC::utf8, 0x1F600::utf8, ?z>>,
      <<127>>
    ]

    invalid = [
      <<0>>,
      <<31>>,
      <<128>>,
      <<192, 128>>,
      <<237, 160, 128>>,
      <<244, 144, 128, 128>>,
      <<245, 128, 128, 128>>,
      <<226, 130>>,
      ~S(\x),
      ~S(\uD800x),
      ~S(\uDC00),
      ~S(\uD800\u0041)
    ]

    for {values, expected_error} <- [{valid, nil}, {invalid, :malformed}], value <- values do
      payload =
        ~s({"ignored":"prefix) <>
          value <>
          ~s(suffix","usage":{"input_tokens":10,"output_tokens":2,"total_tokens":12}})

      for split_at <- 0..byte_size(payload) do
        <<first::binary-size(^split_at), second::binary>> = payload
        state = UsageEnvelope.new() |> UsageEnvelope.feed(first) |> UsageEnvelope.feed(second)
        assert state.error == expected_error

        if expected_error == nil do
          assert state.done?
          assert state.usage["total_tokens"] == 12
        else
          assert state.usage == nil
        end
      end
    end
  end

  defp chunks(data, size) do
    data
    |> Stream.unfold(fn
      "" ->
        nil

      data ->
        count = min(byte_size(data), size)
        <<chunk::binary-size(^count), rest::binary>> = data
        {chunk, rest}
    end)
    |> Enum.to_list()
  end
end
