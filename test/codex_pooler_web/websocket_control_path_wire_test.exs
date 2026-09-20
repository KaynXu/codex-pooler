defmodule CodexPoolerWeb.WebsocketControlPathWireTest do
  use CodexPooler.DataCase, async: false

  alias CodexPooler.Gateway.Payloads.RequestOptions
  alias CodexPoolerWeb.CodexResponsesSocket
  import CodexPooler.PoolerFixtures

  defmodule Endpoint do
    import Plug.Conn
    def init(opts), do: opts

    def call(conn, state) do
      conn |> WebSockAdapter.upgrade(__MODULE__.Socket, state, compress: false) |> halt()
    end

    defmodule Socket do
      @behaviour WebSock
      def init(state) do
        result = CodexResponsesSocket.init(state)

        if match?({:ok, _}, result),
          do: send(state.test_parent, {:socket_ready, self(), elem(result, 1)})

        result
      end

      def handle_in(frame, state), do: CodexResponsesSocket.handle_in(frame, state)
      def handle_info(message, state), do: CodexResponsesSocket.handle_info(message, state)
      def terminate(reason, state), do: CodexResponsesSocket.terminate(reason, state)
    end
  end

  @tag capture_log: true
  test "a stalled owner detach cannot hold the real downstream close frame" do
    CodexPooler.TestAppEnv.restore_on_exit(:websocket_owner_forwarding_enabled)
    Application.put_env(:codex_pooler, :websocket_owner_forwarding_enabled, true)

    %{api_key: key, pool: pool} = active_api_key_fixture()

    state = %{
      auth: %{api_key: key, pool: pool},
      test_parent: self(),
      opts: RequestOptions.for_websocket(%{})
    }

    server =
      start_supervised!(
        {Bandit, plug: {Endpoint, state}, port: 0, ip: {127, 0, 0, 1}, startup_log: false}
      )

    {:ok, {_, port}} = ThousandIsland.listener_info(server)
    {:ok, conn} = Mint.HTTP.connect(:http, "127.0.0.1", port, protocols: [:http1], mode: :passive)
    {:ok, conn, ref} = Mint.WebSocket.upgrade(:ws, conn, "/", [])
    {:ok, conn, responses} = Mint.WebSocket.recv(conn, 0, 15_000)
    {:headers, ^ref, headers} = Enum.find(responses, &match?({:headers, _, _}, &1))
    {:ok, conn, websocket} = Mint.WebSocket.new(conn, ref, 101, headers, mode: :passive)
    assert_receive {:socket_ready, socket, runtime}, 15_000
    parent = self()
    handler = make_ref()

    :telemetry.attach(
      handler,
      [:codex_pooler, :gateway, :websocket_control, :cleanup_finished],
      fn _, _, metadata, _ ->
        if metadata.caller == socket, do: send(parent, {:socket_cleanup_finished, handler})
      end,
      nil
    )

    on_exit(fn -> :telemetry.detach(handler) end)
    alias CodexPooler.Gateway.Transports.Websocket.WebsocketOwnerSession
    assert {:ok, owner} = WebsocketOwnerSession.lookup(runtime.codex_session.id)
    :ok = :sys.suspend(owner)

    try do
      {:ok, websocket, data} = Mint.WebSocket.encode(websocket, {:close, 1000, ""})
      {:ok, conn} = Mint.WebSocket.stream_request_body(conn, ref, data)
      {:ok, conn, responses} = Mint.WebSocket.recv(conn, 0, 2_000)
      data = for {:data, ^ref, data} <- responses, do: data
      {:ok, _, frames} = Mint.WebSocket.decode(websocket, IO.iodata_to_binary(data))
      assert [{:close, 1000, _}] = frames
      Mint.HTTP.close(conn)
    after
      :sys.resume(owner)
      assert_receive {:socket_cleanup_finished, ^handler}, 15_000
    end
  end

  @tag capture_log: true
  test "a real database failure during init sends a 1011 close after upgrade" do
    %{api_key: key, pool: pool} = active_api_key_fixture()

    Repo.query!("""
    CREATE FUNCTION pg_temp.reject_socket_start() RETURNS trigger LANGUAGE plpgsql AS $$
    BEGIN RAISE EXCEPTION 'synthetic socket database failure'; END $$
    """)

    Repo.query!(
      "CREATE TRIGGER reject_socket_start BEFORE INSERT ON codex_sessions FOR EACH ROW EXECUTE FUNCTION pg_temp.reject_socket_start()"
    )

    state = %{auth: %{api_key: key, pool: pool}, opts: RequestOptions.for_websocket(%{})}

    server =
      start_supervised!(
        {Bandit, plug: {Endpoint, state}, port: 0, ip: {127, 0, 0, 1}, startup_log: false}
      )

    {:ok, {_, port}} = ThousandIsland.listener_info(server)
    {:ok, conn} = Mint.HTTP.connect(:http, "127.0.0.1", port, protocols: [:http1], mode: :passive)
    {:ok, conn, ref} = Mint.WebSocket.upgrade(:ws, conn, "/", [])
    {:ok, conn, responses} = Mint.WebSocket.recv(conn, 0, 15_000)
    assert {:status, ^ref, 101} = Enum.find(responses, &match?({:status, _, _}, &1))
    {:headers, ^ref, headers} = Enum.find(responses, &match?({:headers, _, _}, &1))
    {:ok, conn, websocket} = Mint.WebSocket.new(conn, ref, 101, headers, mode: :passive)
    data = for {:data, ^ref, data} <- responses, do: data

    {conn, data} =
      if data == [] do
        {:ok, conn, more} = Mint.WebSocket.recv(conn, 0, 15_000)
        {conn, for({:data, ^ref, data} <- more, do: data)}
      else
        {conn, data}
      end

    {:ok, _, frames} = Mint.WebSocket.decode(websocket, IO.iodata_to_binary(data))
    assert [{:close, 1011, "websocket initialization unavailable"}] = frames
    assert Repo.aggregate(CodexPooler.Accounting.Request, :count) == 0
    assert Repo.aggregate(CodexPooler.Gateway.Persistence.CodexSession, :count) == 0
    Mint.HTTP.close(conn)
  end
end
