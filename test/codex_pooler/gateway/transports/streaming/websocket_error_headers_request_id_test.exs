defmodule CodexPooler.Gateway.Transports.Streaming.WebsocketErrorHeadersRequestIdTest do
  use ExUnit.Case, async: true

  alias CodexPooler.Gateway.Transports.Streaming.StreamProtocol.WebsocketErrorHeaders

  # The frame allowlist feeds the websocket attempt metadata writer; the
  # backend's `x-oai-request-id` must survive it like the other id names.
  test "an error frame's x-oai-request-id survives the allowlist" do
    frame = %{
      "type" => "response.failed",
      "headers" => %{
        "x-oai-request-id" => "req_frame_oai",
        "x-request-id" => "req_frame_x",
        "x-unknown-header" => "dropped"
      }
    }

    headers = WebsocketErrorHeaders.websocket_error_frame_headers(frame)
    assert %{"x-oai-request-id" => "req_frame_oai", "x-request-id" => "req_frame_x"} = headers
    refute Map.has_key?(headers, "x-unknown-header")
  end
end
