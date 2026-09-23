defmodule CodexPooler.Gateway.Runtime.Streaming.ResponsesAPIEOFTest do
  use ExUnit.Case, async: true

  alias CodexPooler.Gateway.Payloads.RequestOptions
  alias CodexPooler.Gateway.Runtime.Streaming.DownstreamStream
  alias CodexPooler.Upstreams.ResponsesAPIHistory
  alias CodexPooler.Upstreams.ResponsesAPITools

  @endpoint "/backend-api/codex/responses"

  test "EOF preserves native delivery commitment, restored tools and continuation history" do
    {opts, history} = options()
    created = event("response.created", %{"status" => "in_progress"}) <> "\n\n"
    initial = DownstreamStream.initial_state(:relay, opts)

    assert {_created, state, %{commits?: false}} =
             DownstreamStream.normalize_delivery(created, @endpoint, opts, initial)

    completed =
      event("response.completed", %{
        "id" => "resp_api_eof",
        "status" => "completed",
        "output" => [
          %{
            "type" => "function_call",
            "name" => "exec",
            "arguments" => JSON.encode!(%{"input" => "await run()"})
          }
        ]
      })

    split = div(byte_size(completed), 2)
    <<prefix::binary-size(^split), suffix::binary>> = completed
    {"", state, _delivery} = DownstreamStream.normalize_delivery(prefix, @endpoint, opts, state)
    {"", state, _delivery} = DownstreamStream.normalize_delivery(suffix, @endpoint, opts, state)
    assert ResponsesAPIHistory.get(history.scope, "resp_api_eof") == :missing

    assert {output, state, %{commits?: true, data: output}} =
             DownstreamStream.flush_eof_delivery(@endpoint, opts, state)

    assert output =~ "response.completed"
    assert output =~ "custom_tool_call"
    assert DownstreamStream.terminal_outcome(state) == :completed
    assert {:ok, remembered} = ResponsesAPIHistory.get(history.scope, "resp_api_eof")
    assert [%{"type" => "custom_tool_call", "input" => "await run()"}] = remembered["input"]
    assert {"", _state, nil} = DownstreamStream.flush_eof_delivery(@endpoint, opts, state)
  end

  test "EOF does not commit or cache an incomplete API completion" do
    {opts, history} = options()
    initial = DownstreamStream.initial_state(:relay, opts)
    bytes = ~s(data: {"type":"response.completed","response":{"id":"resp_api_bad")

    {"", state, _delivery} = DownstreamStream.normalize_delivery(bytes, @endpoint, opts, initial)
    assert {"", state, nil} = DownstreamStream.flush_eof_delivery(@endpoint, opts, state)
    assert DownstreamStream.terminal_outcome(state) == nil
    assert ResponsesAPIHistory.get(history.scope, "resp_api_bad") == :missing
  end

  defp options do
    auth = %{pool: %{id: Ecto.UUID.generate()}, api_key: %{id: Ecto.UUID.generate()}}
    payload = %{"input" => [], "tools" => [%{"type" => "custom", "name" => "exec"}]}
    history = ResponsesAPIHistory.context(auth, payload)
    {_payload, bindings} = ResponsesAPITools.prepare(payload)

    opts =
      RequestOptions.build(%{}, @endpoint, %{"stream" => true})
      |> RequestOptions.put_payload_context(
        responses_api_tools: bindings,
        responses_api_history: history
      )

    {opts, history}
  end

  defp event(type, response),
    do: "event: " <> type <> "\ndata: " <> JSON.encode!(%{"type" => type, "response" => response})
end
