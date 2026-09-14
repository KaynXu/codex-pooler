defmodule CodexPooler.Platform.ExecutionHTTPLifecycleTest do
  use CodexPooler.DataCase, async: false
  import ExUnit.CaptureLog
  alias CodexPooler.Platform.ExecutionIdentity
  alias CodexPooler.Platform.InstancePresence.Identity

  defmodule LifecyclePlug do
    def init(opts), do: opts

    def call(conn, parent) do
      owner = Identity.local()

      identity =
        Map.merge(ExecutionIdentity.local(), %{
          owner_instance_id: owner.node_name,
          owner_instance_boot_id: owner.boot_id
        })

      send(parent, {:execution, self(), identity})

      case conn.request_path do
        "/raise" ->
          raise DBConnection.ConnectionError, message: "synthetic database outage"

        "/stream" ->
          CodexPoolerWeb.GatewayControllerHelpers.send_gateway_result(conn, %{
            status: 200,
            stream: fn _ ->
              send(parent, {:stream_liveness, ExecutionIdentity.status(identity)})
              {:error, :closed}
            end
          })

        "/public" ->
          CodexPoolerWeb.PublicGatewayResult.send(
            conn,
            {:ok, %{status: 200, raw_body: "{}"}},
            &Function.identity/1
          )
      end
    end
  end

  test "returned stream error retires execution while real HTTP keep-alive reuses its PID" do
    {socket, _server} = start_connection()

    logs =
      capture_log(fn ->
        :ok = :gen_tcp.send(socket, "GET /stream HTTP/1.1\r\nHost: localhost\r\n\r\n")
        assert_receive {:execution, pid, first}, 15_000
        response = receive_response(socket)
        assert response =~ "200 OK"
        assert_receive {:stream_liveness, :alive}
        assert Process.alive?(pid)
        assert ExecutionIdentity.status(first) == :dead
        CodexPooler.ExecutionProofSupport.publish_terminal!(first)
        :ok = :gen_tcp.send(socket, "GET /public HTTP/1.1\r\nHost: localhost\r\n\r\n")
        assert_receive {:execution, ^pid, second}, 15_000
        assert receive_response(socket) =~ "200 OK"
        assert second.owner_execution_id != first.owner_execution_id
        assert ExecutionIdentity.status(second) == :dead
        CodexPooler.ExecutionProofSupport.publish_terminal!(second)
      end)

    assert logs =~ "late gateway stream failed"
  end

  test "raised DBConnection error terminates actual Bandit connection and execution" do
    {socket, _server} = start_connection()

    logs =
      capture_log(fn ->
        :ok = :gen_tcp.send(socket, "GET /raise HTTP/1.1\r\nHost: localhost\r\n\r\n")
        assert_receive {:execution, pid, identity}, 15_000
        monitor = Process.monitor(pid)
        assert_receive {:DOWN, ^monitor, :process, ^pid, _}, 15_000
        assert ExecutionIdentity.status(identity) == :dead
        CodexPooler.ExecutionProofSupport.publish_terminal!(identity)
      end)

    assert logs =~ "synthetic database outage"
  end

  defp start_connection do
    server =
      start_supervised!(
        {Bandit, plug: {LifecyclePlug, self()}, port: 0, ip: {127, 0, 0, 1}, startup_log: false}
      )

    {:ok, {_ip, port}} = ThousandIsland.listener_info(server)
    {:ok, socket} = :gen_tcp.connect({127, 0, 0, 1}, port, [:binary, active: false])
    on_exit(fn -> :gen_tcp.close(socket) end)
    {socket, server}
  end

  defp receive_response(socket, buffer \\ "") do
    {:ok, data} = :gen_tcp.recv(socket, 0, 15_000)
    buffer = buffer <> data

    if String.ends_with?(buffer, "0\r\n\r\n") or String.ends_with?(buffer, "\r\n\r\n{}"),
      do: buffer,
      else: receive_response(socket, buffer)
  end
end
