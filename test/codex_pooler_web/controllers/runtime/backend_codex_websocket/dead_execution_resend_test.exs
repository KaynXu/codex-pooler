defmodule CodexPoolerWeb.Runtime.BackendCodexWebsocket.DeadExecutionResendTest do
  use CodexPoolerWeb.ConnCase, async: false

  import Ecto.Query
  import CodexPoolerWeb.Runtime.BackendCodexTestSupport
  import CodexPoolerWeb.Runtime.BackendCodexWebsocketSupport

  alias CodexPooler.Accounting.{Attempt, Request, RequestLifecycle}
  alias CodexPooler.FakeUpstream
  alias CodexPooler.Gateway.Persistence.CodexTurn
  alias CodexPooler.Gateway.Transports.Websocket.WebsocketOwnerSession
  alias CodexPooler.Platform.ExecutionIdentity
  alias CodexPooler.Repo

  @moduletag capture_log: true

  for forwarding <- [false, true] do
    @tag forwarding: forwarding
    test "socket forwarding=#{forwarding} resends an exactly recovered execution", %{
      forwarding: forwarding
    } do
      CodexPooler.TestAppEnv.restore_on_exit(:websocket_owner_forwarding_enabled)
      Application.put_env(:codex_pooler, :websocket_owner_forwarding_enabled, true)
      on_exit(&stop_registered_websocket_owner_sessions/0)
      barrier = make_ref()

      upstream =
        start_upstream(
          FakeUpstream.strict_sequence([
            FakeUpstream.expect_request(
              method: "WEBSOCKET",
              path: "/backend-api/codex/responses",
              respond:
                FakeUpstream.barrier_websocket_frames([], notify: self(), release_ref: barrier)
            ),
            FakeUpstream.expect_request(
              method: "WEBSOCKET",
              path: "/backend-api/codex/responses",
              respond:
                FakeUpstream.websocket_text_frames([
                  CodexPooler.JSON.encode!(%{
                    "type" => "response.completed",
                    "response" => %{"id" => "resp_recovered", "status" => "completed"}
                  })
                ])
            )
          ])
        )

      setup = gateway_setup(upstream)
      session_id = Ecto.UUID.generate()

      payload =
        CodexPooler.JSON.encode!(%{
          "type" => "response.create",
          "model" => setup.model.exposed_model_id,
          "input" => [],
          "stream" => true,
          "store" => false,
          "client_metadata" => %{
            "x-codex-turn-metadata" =>
              CodexPooler.JSON.encode!(%{
                "session_id" => session_id,
                "thread_id" => session_id,
                "turn_id" => Ecto.UUID.generate(),
                "request_kind" => "turn"
              })
          }
        })

      {_server, port} = start_public_endpoint_with_server!()
      {conn, websocket, ref} = connect!(port, setup, session_id)
      {conn, _websocket} = public_websocket_send_text!(conn, websocket, ref, payload)
      assert_receive {:fake_upstream_frame_barrier, 0, _handler, ^barrier}, 15_000
      request = Repo.one!(from r in Request, where: r.pool_id == ^setup.pool.id)
      attempt = Repo.one!(from a in Attempt, where: a.request_id == ^request.id)
      turn = Repo.one!(from t in CodexTurn, where: t.request_id == ^request.id)
      assert {:ok, owner} = WebsocketOwnerSession.lookup(turn.codex_session_id)
      %{downstream: %{pid: socket}} = :sys.get_state(owner)
      task = attempt.owner_process_id |> String.to_charlist() |> :erlang.list_to_pid()
      assert ExecutionIdentity.status(attempt) == :alive
      monitor = Process.monitor(task)
      :erlang.suspend_process(socket)

      try do
        Process.exit(task, :kill)
        assert_receive {:DOWN, ^monitor, :process, ^task, :killed}, 15_000
        assert ExecutionIdentity.status(attempt) == :dead

        assert {:ok, :recovered} =
                 RequestLifecycle.recover_dead_execution(request, attempt, DateTime.utc_now())
      after
        :erlang.resume_process(socket)
      end

      assert Repo.reload!(request).last_error_code == "dead_execution_recovered"
      assert :ok = FakeUpstream.release_remaining_frames(upstream, barrier)
      socket_monitor = Process.monitor(socket)
      Mint.HTTP.close(conn)
      assert_receive {:DOWN, ^socket_monitor, :process, ^socket, _}, 15_000

      Application.put_env(:codex_pooler, :websocket_owner_forwarding_enabled, forwarding)

      changed_payload =
        payload
        |> CodexPooler.JSON.decode!()
        |> Map.put("temperature", 0.5)
        |> CodexPooler.JSON.encode!()

      {changed_conn, changed_ws, changed_ref} = connect!(port, setup, session_id)

      {changed_conn, changed_ws} =
        public_websocket_send_text!(changed_conn, changed_ws, changed_ref, changed_payload)

      {changed_conn, _changed_ws, changed_frame} =
        public_websocket_receive_text!(changed_conn, changed_ws, changed_ref)

      Mint.HTTP.close(changed_conn)

      assert %{
               "type" => "error",
               "status" => 409,
               "error" => %{"code" => "duplicate_turn", "type" => "invalid_request_error"}
             } =
               CodexPooler.JSON.decode!(changed_frame)

      {retry_conn, retry_ws, retry_ref} = connect!(port, setup, session_id)
      parent = self()

      contender =
        if not forwarding do
          Task.async(fn ->
            {conn, ws, ref} = connect!(port, setup, session_id)
            send(parent, {:contender_ready, self()})

            receive do
              :send ->
                {conn, ws} = public_websocket_send_text!(conn, ws, ref, payload)
                {conn, _ws, frame} = public_websocket_receive_text!(conn, ws, ref)
                Mint.HTTP.close(conn)

                receive do
                  :result -> frame
                end
            end
          end)
        end

      if contender do
        contender_pid = contender.pid
        assert_receive {:contender_ready, ^contender_pid}, 15_000
        send(contender_pid, :send)
      end

      {retry_conn, retry_ws} =
        public_websocket_send_text!(retry_conn, retry_ws, retry_ref, payload)

      {retry_conn, _retry_ws, frame} =
        public_websocket_receive_text!(retry_conn, retry_ws, retry_ref)

      Mint.HTTP.close(retry_conn)

      if contender do
        send(contender.pid, :result)
        other_frame = Task.await(contender, 15_000)
        terminals = Enum.map([frame, other_frame], &CodexPooler.JSON.decode!/1)
        assert Enum.count(terminals, &(&1["type"] == "response.completed")) == 1

        assert [
                 %{
                   "status" => 409,
                   "error" => %{"code" => "duplicate_turn", "type" => "invalid_request_error"}
                 }
               ] =
                 Enum.filter(terminals, &(&1["type"] == "error"))
      else
        assert %{"type" => "response.completed"} = CodexPooler.JSON.decode!(frame)
      end

      assert Repo.aggregate(from(r in Request, where: r.pool_id == ^setup.pool.id), :count) == 2

      assert Repo.aggregate(
               from(l in CodexPooler.Accounting.RequestClientRetryLink,
                 where: l.predecessor_request_id == ^request.id
               ),
               :count
             ) == 1

      Application.put_env(:codex_pooler, :websocket_owner_forwarding_enabled, not forwarding)
      {again_conn, again_ws, again_ref} = connect!(port, setup, session_id)

      {again_conn, again_ws} =
        public_websocket_send_text!(again_conn, again_ws, again_ref, payload)

      {again_conn, _again_ws, again_frame} =
        public_websocket_receive_text!(again_conn, again_ws, again_ref)

      Mint.HTTP.close(again_conn)

      assert %{
               "status" => 409,
               "error" => %{"code" => "duplicate_turn", "type" => "invalid_request_error"}
             } = CodexPooler.JSON.decode!(again_frame)

      assert :ok = FakeUpstream.verify!(upstream)
    end
  end

  defp connect!(port, setup, session_id) do
    {:ok, conn} = Mint.HTTP.connect(:http, "127.0.0.1", port, protocols: [:http1])

    headers = [
      {"authorization", setup.authorization},
      {"session-id", session_id},
      {"x-request-id", session_id},
      {"user-agent", "codex_cli_rs/0.154.0"}
    ]

    {:ok, conn, ref} = Mint.WebSocket.upgrade(:ws, conn, "/backend-api/codex/responses", headers)
    {:ok, conn, status, response_headers} = await_public_websocket_upgrade(conn, ref)
    {conn, websocket} = mint_websocket_new!(conn, ref, status, response_headers)
    {conn, websocket, ref}
  end
end
