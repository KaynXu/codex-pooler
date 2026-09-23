defmodule CodexPooler.Gateway.Runtime.Finalization.MetadataUpstreamRequestIdTest do
  use ExUnit.Case, async: true

  alias CodexPooler.Gateway.Payloads.RequestOptions
  alias CodexPooler.Gateway.Runtime.Finalization.Metadata

  @endpoint "/backend-api/codex/responses"

  # The Codex backend answers with `x-oai-request-id`; the writers used to read
  # only `x-request-id`/`openai-request-id`, so no attempt carried a provider
  # id (findings#218). The value is an opaque provider identifier, metadata only.
  test "the HTTP response writer records the provider's x-oai-request-id" do
    opts = RequestOptions.build(%{}, @endpoint, %{})
    response = %Req.Response{status: 200, headers: %{"x-oai-request-id" => ["req_synthetic_1"]}}

    assert %{"upstream_request_id" => "req_synthetic_1"} =
             Metadata.response_metadata(response, nil, opts)
  end

  test "the HTTP response writer prefers x-request-id like the released client, then x-oai-request-id" do
    opts = RequestOptions.build(%{}, @endpoint, %{})

    both = %Req.Response{
      status: 200,
      headers: %{"x-request-id" => ["req_a"], "x-oai-request-id" => ["req_b"]}
    }

    assert Metadata.response_metadata(both, nil, opts)["upstream_request_id"] == "req_a"

    # A blank first-choice header is absent and must not shadow a populated one.
    blank_first = %Req.Response{
      status: 200,
      headers: %{"x-request-id" => [""], "x-oai-request-id" => ["req_b"]}
    }

    assert Metadata.response_metadata(blank_first, nil, opts)["upstream_request_id"] == "req_b"

    none = %Req.Response{status: 200, headers: %{"content-type" => ["application/json"]}}
    refute Map.has_key?(Metadata.response_metadata(none, nil, opts), "upstream_request_id")
  end

  test "the websocket response writer records the provider's x-oai-request-id" do
    opts = RequestOptions.for_websocket(%{})
    headers = [{"x-oai-request-id", "req_synthetic_ws"}]

    assert %{"upstream_request_id" => "req_synthetic_ws"} =
             Metadata.websocket_response_metadata(headers, nil, opts)
  end
end
