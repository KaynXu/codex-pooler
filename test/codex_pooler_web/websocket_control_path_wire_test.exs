defmodule CodexPoolerWeb.WebsocketControlPathWireTest do
  use CodexPooler.DataCase, async: false

  alias CodexPooler.Gateway.Payloads.RequestOptions
  alias CodexPooler.Gateway.Persistence.{BridgeOwnerLease, CodexSession}
  alias CodexPooler.Gateway.Transports.Websocket.WebsocketOwnerSession
  alias CodexPoolerWeb.CodexResponsesSocket
  import ExUnit.CaptureLog
  import CodexPooler.PoolerFixtures

  @shutdown_budget 15_000

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

    on_exit(fn ->
      for session <- Repo.all(from(s in CodexSession, where: s.pool_id == ^pool.id)) do
        case WebsocketOwnerSession.lookup(session.id) do
          {:ok, owner} ->
            :sys.resume(owner)
            stop_owner!(owner)

          {:error, :owner_unavailable} ->
            :ok
        end
      end
    end)

    state = %{
      auth: %{api_key: key, pool: pool},
      test_parent: self(),
      opts: RequestOptions.for_websocket(%{})
    }

    server =
      start_supervised!({Bandit, plug: {Endpoint, state}, port: 0, ip: {127, 0, 0, 1}, startup_log: false})

    {:ok, {_, port}} = ThousandIsland.listener_info(server)
    {:ok, conn} = Mint.HTTP.connect(:http, "127.0.0.1", port, protocols: [:http1], mode: :passive)
    {:ok, conn, ref} = Mint.WebSocket.upgrade(:ws, conn, "/", [])
    {:ok, conn, responses} = Mint.WebSocket.recv(conn, 0, 15_000)
    {:headers, ^ref, headers} = Enum.find(responses, &match?({:headers, _, _}, &1))
    {:ok, conn, websocket} = Mint.WebSocket.new(conn, ref, 101, headers, mode: :passive)
    assert_receive {:socket_ready, socket, runtime}, 15_000
    parent = self()
    handler = make_ref()

    on_exit(fn -> :telemetry.detach(handler) end)

    :telemetry.attach_many(
      handler,
      [
        [:codex_pooler, :gateway, :websocket_control, :cleanup_finished],
        [:codex_pooler, :gateway, :websocket_control, :failure]
      ],
      fn event, _, metadata, _ ->
        case List.last(event) do
          :cleanup_finished ->
            if metadata.caller == socket,
              do: send(parent, {:socket_cleanup_finished, handler, self()})

          :failure ->
            if self() == socket,
              do: send(parent, {:socket_cleanup_failure, handler, metadata})
        end
      end,
      nil
    )

    assert {:ok, owner} = WebsocketOwnerSession.lookup(runtime.codex_session.id)
    socket_monitor = Process.monitor(socket)
    :ok = :sys.suspend(owner)

    logs =
      capture_log(fn ->
        try do
          {:ok, websocket, data} = Mint.WebSocket.encode(websocket, {:close, 1000, ""})
          {:ok, conn} = Mint.WebSocket.stream_request_body(conn, ref, data)
          {:ok, conn, responses} = Mint.WebSocket.recv(conn, 0, 2_000)
          data = for {:data, ^ref, data} <- responses, do: data
          {:ok, _, frames} = Mint.WebSocket.decode(websocket, IO.iodata_to_binary(data))
          assert [{:close, 1000, _}] = frames
          Mint.HTTP.close(conn)

          assert_receive {:socket_cleanup_failure, ^handler, %{phase: :terminate, reason: :cleanup_deferred}},
                         @shutdown_budget

          assert Process.alive?(owner)
        after
          :sys.resume(owner)
          assert_receive {:socket_cleanup_finished, ^handler, cleanup}, @shutdown_budget
          await_down!(cleanup, Process.monitor(cleanup), @shutdown_budget)
          await_down!(socket, socket_monitor, @shutdown_budget)
          stop_owner!(owner)
        end
      end)

    assert ["websocket control path failed phase=terminate reason=cleanup_deferred"] =
             logs
             |> String.split("\n", trim: true)
             |> Enum.map(&String.replace(&1, ~r/^.*\[warning\] /, ""))

    assert {:error, :owner_unavailable} = WebsocketOwnerSession.lookup(runtime.codex_session.id)
    lease = Repo.get_by!(BridgeOwnerLease, codex_session_id: runtime.codex_session.id)
    assert lease.released_at
    assert Repo.aggregate(CodexPooler.Accounting.Request, :count) == 0

    CodexPooler.TestDiagnostics.puts("wire_cleanup caller_down=true task_down=true owner_down=true registry_absent=true lease_released=true requests=0 expected_deferral=1")
  end

  @tag capture_log: true
  test "a real database failure during init sends a 1011 close after upgrade" do
    %{api_key: key, pool: pool} = active_api_key_fixture()

    Repo.query!("""
    CREATE FUNCTION pg_temp.reject_socket_start() RETURNS trigger LANGUAGE plpgsql AS $$
    BEGIN RAISE EXCEPTION 'synthetic socket database failure'; END $$
    """)

    Repo.query!("CREATE TRIGGER reject_socket_start BEFORE INSERT ON codex_sessions FOR EACH ROW EXECUTE FUNCTION pg_temp.reject_socket_start()")

    state = %{auth: %{api_key: key, pool: pool}, opts: RequestOptions.for_websocket(%{})}

    server =
      start_supervised!({Bandit, plug: {Endpoint, state}, port: 0, ip: {127, 0, 0, 1}, startup_log: false})

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

  test "completion fence rejects a live process without a terminal signal" do
    monitor = Process.monitor(self())

    try do
      assert_raise ExUnit.AssertionError, fn -> await_down!(self(), monitor, 0) end
    after
      Process.demonitor(monitor, [:flush])
    end
  end

  test "completion fence waits for a delayed owner exit" do
    parent = self()
    owner = start_supervised!({Task, fn -> receive do: (:release -> :ok) end})

    waiter =
      Task.async(fn ->
        monitor = Process.monitor(owner)
        send(parent, :fence_ready)
        await_down!(owner, monitor, @shutdown_budget)
      end)

    waiter_monitor = Process.monitor(waiter.pid)
    assert_receive :fence_ready
    assert Process.alive?(owner)
    refute_received {_, :ok}
    send(owner, :release)
    assert :ok = Task.await(waiter, @shutdown_budget)
    await_down!(waiter.pid, waiter_monitor, @shutdown_budget)
  end

  defp stop_owner!(owner) do
    monitor = Process.monitor(owner)

    try do
      GenServer.stop(owner, :shutdown, @shutdown_budget)
    catch
      :exit, {:noproc, _} -> :ok
      :exit, {:normal, _} -> :ok
    end

    await_down!(owner, monitor, @shutdown_budget)
  end

  defp await_down!(pid, monitor, budget) do
    assert_receive {:DOWN, ^monitor, :process, ^pid, reason}, budget
    assert reason in [:normal, :shutdown, :noproc, {:shutdown, :local_closed}]
    refute Process.alive?(pid)
    :ok
  end
end
