defmodule CodexPooler.Platform.InstancePresencePeerCleanupPostgresTest do
  use CodexPooler.DataCase, async: false

  alias CodexPooler.InstancePresencePeer
  alias CodexPooler.Platform.InstancePresence.Identity
  alias CodexPooler.Repo

  @detection_budget_ms 15_000

  test "a requested termination must become PostgreSQL absence before deleting a locked peer row" do
    boot_id = Ecto.UUID.generate()
    identity = Identity.new("cleanup-order@example.invalid", boot_id)
    instance_id = identity.instance_id
    application_name = InstancePresencePeer.peer_application_name(boot_id)
    peer = start_connection!(application_name)
    observer = start_connection!("peer_cleanup_order_observer")
    deleter = start_connection!("peer_cleanup_order_deleter")

    register_cleanup!(identity.instance_id, application_name)
    insert_presence!(observer, identity)
    Postgrex.query!(deleter, "SET lock_timeout = '250ms'", [])

    Postgrex.query!(peer, "BEGIN", [])

    assert %{rows: [[^instance_id]]} =
             Postgrex.query!(
               peer,
               "SELECT instance_id FROM instance_presences WHERE instance_id = $1 FOR UPDATE",
               [identity.instance_id]
             )

    %{rows: [[peer_backend]]} = Postgrex.query!(peer, "SELECT pg_backend_pid()", [])
    parent = self()

    terminator =
      Task.async(fn ->
        receive do
          :termination_requested ->
            send(parent, {:termination_request_accepted, self()})

            receive do
              :dispatch_termination ->
                Postgrex.query!(observer, "SELECT pg_terminate_backend($1)", [peer_backend])
            end
        end
      end)

    terminator_pid = terminator.pid

    assert :ok =
             InstancePresencePeer.purge_peer_state!(
               boot_id,
               fn ->
                 assert %{rows: [[^instance_id]]} =
                          Postgrex.query!(
                            deleter,
                            "DELETE FROM instance_presences WHERE instance_id = $1 RETURNING instance_id",
                            [identity.instance_id]
                          )
               end,
               budget_ms: @detection_budget_ms,
               terminate: fn ^boot_id ->
                 send(terminator_pid, :termination_requested)

                 assert_receive {:termination_request_accepted, ^terminator_pid},
                                @detection_budget_ms

                 :ok
               end,
               await: fn ^boot_id, @detection_budget_ms ->
                 assert backend_count(observer, application_name) == 1

                 assert %{rows: [[1]]} =
                          Repo.query!(
                            "SELECT count(*) FROM pg_stat_activity WHERE application_name = $1",
                            [application_name]
                          )

                 send(terminator_pid, :dispatch_termination)
                 assert %{rows: [[true]]} = Task.await(terminator, @detection_budget_ms)

                 InstancePresencePeer.assert_peer_connections_absent!(
                   boot_id,
                   @detection_budget_ms
                 )
               end
             )

    assert backend_count(observer, application_name) == 0
  end

  defp start_connection!(application_name) do
    options =
      Repo.config()
      |> Keyword.take([:hostname, :port, :username, :password, :database, :socket_dir])
      |> Keyword.put(:backoff_type, :stop)
      |> Keyword.put(:parameters, application_name: application_name)

    {:ok, connection} = Postgrex.start_link(options)
    Process.unlink(connection)

    on_exit(fn ->
      if Process.alive?(connection) do
        GenServer.stop(connection)
      end
    end)

    connection
  end

  defp insert_presence!(connection, identity) do
    assert %{num_rows: 1} =
             Postgrex.query!(
               connection,
               """
               INSERT INTO instance_presences (
                 instance_id, node_name, boot_id, started_at, last_seen_at, updated_at
               ) VALUES ($1, $2, $3, clock_timestamp(), clock_timestamp(), clock_timestamp())
               """,
               [identity.instance_id, identity.node_name, identity.boot_id]
             )
  end

  defp backend_count(connection, application_name) do
    %{rows: [[count]]} =
      Postgrex.query!(
        connection,
        "SELECT count(*) FROM pg_stat_activity WHERE application_name = $1",
        [application_name]
      )

    count
  end

  defp register_cleanup!(instance_id, application_name) do
    on_exit(fn ->
      options =
        Repo.config()
        |> Keyword.take([:hostname, :port, :username, :password, :database, :socket_dir])
        |> Keyword.put(:backoff_type, :stop)
        |> Keyword.put(:parameters, application_name: "peer_cleanup_order_cleanup")

      {:ok, connection} = Postgrex.start_link(options)

      try do
        Postgrex.query!(
          connection,
          "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE application_name = $1",
          [application_name]
        )

        await_backend_absent!(connection, application_name)

        Postgrex.query!(connection, "DELETE FROM instance_presences WHERE instance_id = $1", [
          instance_id
        ])
      after
        if Process.alive?(connection), do: GenServer.stop(connection)
      end
    end)
  end

  defp await_backend_absent!(connection, application_name) do
    deadline = System.monotonic_time(:millisecond) + @detection_budget_ms
    await_backend_absent!(connection, application_name, deadline)
  end

  defp await_backend_absent!(connection, application_name, deadline) do
    if backend_count(connection, application_name) > 0 do
      assert System.monotonic_time(:millisecond) < deadline,
             "peer backend survived cleanup detection budget"

      receive do
      after
        10 -> :ok
      end

      await_backend_absent!(connection, application_name, deadline)
    else
      :ok
    end
  end
end
