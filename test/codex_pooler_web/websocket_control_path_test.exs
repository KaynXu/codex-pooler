defmodule CodexPoolerWeb.WebsocketControlPathTest do
  use ExUnit.Case, async: true

  import ExUnit.CaptureLog
  alias CodexPoolerWeb.WebsocketControlPath

  test "failures emit one bounded event without raw exception data" do
    parent = self()
    id = make_ref()

    :telemetry.attach(
      id,
      [:codex_pooler, :gateway, :websocket_control, :failure],
      fn _, measurements, metadata, _ ->
        send(parent, {:control_failure, measurements, metadata})
      end,
      nil
    )

    on_exit(fn -> :telemetry.detach(id) end)

    logs =
      capture_log(fn ->
        WebsocketControlPath.run(:serve, fn ->
          raise DBConnection.ConnectionError, "private-value"
        end)
      end)

    assert_receive {:control_failure, %{count: 1}, %{phase: :serve, reason: :database_error}}
    refute_receive {:control_failure, _, _}
    refute logs =~ "private-value"
  end

  test "database and peer-call failures cannot escape the socket callback" do
    assert capture_log(fn ->
             assert {:error, :database_error} =
                      WebsocketControlPath.run(:init, fn ->
                        raise DBConnection.ConnectionError, "private database detail"
                      end)

             assert {:error, :process_exit} =
                      WebsocketControlPath.run(:terminate, fn -> exit({:timeout, :private}) end)
           end) =~ "phase=init reason=database_error"
  end

  test "slow cleanup survives the socket caller without delaying the close" do
    parent = self()

    caller =
      spawn(fn ->
        WebsocketControlPath.cleanup(fn ->
          send(parent, {:cleanup_started, self()})

          receive do
            :release -> send(parent, :cleanup_finished)
          end
        end)

        send(parent, :socket_can_close)
      end)

    monitor = Process.monitor(caller)
    assert_receive {:cleanup_started, cleanup}, 5_000

    try do
      assert_receive :socket_can_close, 5_000
      assert_receive {:DOWN, ^monitor, :process, ^caller, :normal}, 5_000
      assert Process.alive?(cleanup)
      send(cleanup, :release)
      assert_receive :cleanup_finished, 5_000
    after
      send(cleanup, :release)
      if Process.alive?(caller), do: Process.exit(caller, :kill)
    end
  end
end
