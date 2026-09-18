defmodule CodexPooler.Gateway.Transports.UpstreamWebsocketProxyTest do
  use ExUnit.Case, async: false

  alias CodexPooler.Gateway.Transports.Websocket.UpstreamWebsocketSession
  alias CodexPooler.Gateway.Transports.Websocket.UpstreamWebsocketSession.Request
  alias CodexPooler.Platform.OutboundHTTP

  @timeouts %{connect_timeout_ms: 1_000, receive_timeout_ms: 1_000}

  setup do
    CodexPooler.TestAppEnv.restore_on_exit(OutboundHTTP)
    :ok
  end

  test "HTTPS upstream websocket sends CONNECT and Basic auth through https_proxy" do
    {proxy_port, proxy_task} = start_connect_proxy(self())
    authorization = "Basic " <> Base.encode64("proxy-user:proxy-pass")

    Application.put_env(:codex_pooler, OutboundHTTP,
      proxy_config: %{
        http: [],
        https: [
          proxy: {:http, "127.0.0.1", proxy_port, []},
          proxy_headers: [{"proxy-authorization", authorization}]
        ],
        no_proxy: []
      }
    )

    assert {:error, _reason} =
             UpstreamWebsocketSession.request_once(%Request{
               url: "https://unresolvable.invalid/backend-api/codex/responses",
               headers: [],
               payload: "{}",
               timeouts: @timeouts,
               writer: fn _text -> :ok end
             })

    assert_receive {:proxy_request, request}, 5_000
    assert request =~ "CONNECT unresolvable.invalid:443 HTTP/1.1\r\n"
    assert String.downcase(request) =~ "proxy-authorization: #{String.downcase(authorization)}"
    Task.await(proxy_task, 5_000)
  end

  defp start_connect_proxy(parent) do
    {:ok, listener} =
      :gen_tcp.listen(0, [
        :binary,
        packet: :raw,
        active: false,
        reuseaddr: true,
        ip: {127, 0, 0, 1}
      ])

    {:ok, {_address, port}} = :inet.sockname(listener)

    task =
      Task.async(fn ->
        {:ok, socket} = :gen_tcp.accept(listener, 5_000)
        request = recv_headers(socket, "")
        send(parent, {:proxy_request, request})
        :ok = :gen_tcp.send(socket, "HTTP/1.1 502 Bad Gateway\r\ncontent-length: 0\r\n\r\n")
        :gen_tcp.close(socket)
        :gen_tcp.close(listener)
      end)

    {port, task}
  end

  defp recv_headers(socket, buffer) do
    if String.contains?(buffer, "\r\n\r\n") do
      buffer
    else
      {:ok, chunk} = :gen_tcp.recv(socket, 0, 5_000)
      recv_headers(socket, buffer <> chunk)
    end
  end
end
