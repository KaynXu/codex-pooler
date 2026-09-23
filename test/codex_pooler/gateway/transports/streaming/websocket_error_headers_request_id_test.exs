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

  # The allowlist derives from the reader's own name list, in the reader's
  # order, so a name the metadata writer never reads is not admitted either:
  # `x-openai-request-id` used to be stored on attempts and read by nothing.
  test "the allowlist admits exactly the request id names the metadata writer reads" do
    assert WebsocketErrorHeaders.upstream_request_id_header_names() == [
             "x-request-id",
             "x-oai-request-id",
             "openai-request-id"
           ]

    frame = %{
      "type" => "response.failed",
      "headers" => %{"x-openai-request-id" => "req_dead_name"}
    }

    assert WebsocketErrorHeaders.websocket_error_frame_headers(frame) == %{}
  end
end
