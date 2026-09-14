defmodule CodexPooler.Platform.InstancePresenceStarvationTest do
  use CodexPooler.DataCase, async: false

  alias CodexPooler.{Accounting, Repo, UnboxedFixture}
  alias CodexPooler.Platform.{ExecutionIdentity, InstancePresence}
  alias CodexPooler.Platform.InstancePresence.Instance

  # Real 15-second heartbeat cadence and the 120-second boundary are the
  # behavior under test. Each next observation is triggered by a failed write.
  @tag timeout: 180_000
  test "owner-only heartbeat starvation cannot finalize a living disconnected response task",
       context do
    start_distribution!()
    %{user: user} = CodexPooler.AccountsFixtures.committed_bootstrap_owner_fixture!()
    slug = "presence-starvation-#{Ecto.UUID.generate()}"
    observer = InstancePresence.local_identity()

    UnboxedFixture.register_unboxed_cleanup!(fn ->
      Repo.query!("DROP TRIGGER IF EXISTS presence_starvation_failure ON instance_presences")
      Repo.query!("DROP FUNCTION IF EXISTS presence_starvation_failure()")
      ids = Repo.all(from p in CodexPooler.Pools.Pool, where: p.slug == ^slug, select: p.id)
      CodexPooler.PoolerFixtures.delete_committed_pools!(ids)

      Repo.delete_all(
        from i in CodexPooler.Upstreams.Schemas.UpstreamIdentity, where: i.account_label == ^slug
      )

      Repo.delete_all(from i in Instance, where: i.instance_id == ^observer.instance_id)
    end)

    setup =
      UnboxedFixture.run_unboxed(fn ->
        pool = CodexPooler.PoolerFixtures.pool_fixture(%{slug: slug, created_by_user_id: user.id})

        %{api_key: key} =
          CodexPooler.PoolerFixtures.active_api_key_fixture(pool, %{created_by_user_id: user.id})

        model = CodexPooler.PoolerFixtures.model_fixture(pool)

        %{assignment: assignment} =
          CodexPooler.PoolerFixtures.upstream_assignment_fixture(pool, %{account_label: slug})

        %{auth: %{pool: pool, api_key: key}, model: model, assignment: assignment}
      end)

    parent = self()
    peer_name = :"presence_owner_#{System.unique_integer([:positive])}"
    on_exit(fn -> CodexPooler.PeerRegistry.assert_peer_absent!(peer_name) end)

    owner =
      start_supervised!(
        {Task,
         fn ->
           {:ok, peer, remote} =
             :peer.start_link(%{
               name: peer_name,
               connection: :standard_io,
               args: [~c"+S", ~c"2:2", ~c"-kernel", ~c"prevent_overlapping_partitions", ~c"false"]
             })

           send(parent, {:presence_peer, peer, remote})

           receive do
             :stop -> :peer.stop(peer)
           end
         end}
      )

    assert_receive {:presence_peer, peer, remote}, 15_000
    os_pid = :peer.call(peer, :os, :getpid, []) |> List.to_string()

    on_exit(fn ->
      assert not Process.alive?(peer)
      CodexPooler.InstancePresencePeer.assert_os_process_absent!(os_pid)
    end)

    :ok = :peer.call(peer, :code, :add_paths, [:code.get_path()])

    :ok =
      :peer.call(peer, CodexPooler.DisconnectedExecutionPeer, :bootstrap, [
        Application.get_all_env(:codex_pooler),
        Repo.config()
      ])

    identity = :peer.call(peer, InstancePresence, :local_identity, [])

    UnboxedFixture.register_unboxed_cleanup!(fn ->
      assert not Process.alive?(peer)
      Repo.delete_all(from i in Instance, where: i.instance_id == ^identity.instance_id)

      Repo.delete_all(
        from p in CodexPooler.Platform.ExecutionTerminalProof,
          where: p.owner_instance_boot_id == ^identity.boot_id
      )
    end)

    {:ok, _} = :peer.call(peer, InstancePresence, :record_heartbeat, [])

    {request, attempt, executor} =
      :peer.call(peer, CodexPooler.InstancePresencePeer, :start, [setup])

    assert :alive == :peer.call(peer, ExecutionIdentity, :status, [attempt])
    assert ExecutionIdentity.status(attempt) == :unknown

    # The long cadence exercise owns committed fixtures and separate peer
    # connections; release the unused sandbox before its ownership timeout.
    CodexPooler.DataCase.stop_sandbox(context.sandbox_owner, context.sandbox_settings_cache)

    UnboxedFixture.run_unboxed(fn ->
      Repo.query!("""
      CREATE FUNCTION presence_starvation_failure() RETURNS trigger LANGUAGE plpgsql AS $$
      BEGIN
        IF NEW.instance_id = '#{identity.instance_id}' THEN
          RAISE EXCEPTION 'synthetic heartbeat write failure';
        END IF;
        RETURN NEW;
      END $$
      """)

      Repo.query!(
        "CREATE TRIGGER presence_starvation_failure BEFORE INSERT OR UPDATE ON instance_presences FOR EACH ROW EXECUTE FUNCTION presence_starvation_failure()"
      )
    end)

    assert %{failures: failures, age_seconds: age, warned: true} =
             :peer.call(peer, CodexPooler.InstancePresencePeer, :await_starvation, [], 150_000)

    assert failures >= 8
    assert age > 120
    assert :alive == :peer.call(peer, ExecutionIdentity, :status, [attempt])
    assert [] == :peer.call(peer, Node, :list, [])

    UnboxedFixture.run_unboxed(fn ->
      {:ok, _} = InstancePresence.record_heartbeat()

      assert {:ok, %{absent_instance_attempts_recovered: 0}} =
               Accounting.recover_absent_instance_attempts(InstancePresence.database_now())

      assert Repo.reload!(request).status == "in_progress"
      assert Repo.reload!(attempt).status == "in_progress"

      assert Enum.map(Accounting.list_ledger_entries_for_request(request.id), & &1.entry_kind) ==
               ["reservation"]

      Repo.query!("DROP TRIGGER presence_starvation_failure ON instance_presences")
      Repo.query!("DROP FUNCTION presence_starvation_failure()")
    end)

    assert Node.connect(remote)
    assert ExecutionIdentity.status(attempt) == :alive

    UnboxedFixture.run_unboxed(fn ->
      assert {:ok, %{absent_instance_attempts_recovered: 0}} =
               Accounting.recover_absent_instance_attempts(InstancePresence.database_now())
    end)

    :ok = :peer.call(peer, CodexPooler.DisconnectedExecutionPeer, :finish, [executor, attempt])
    assert ExecutionIdentity.status(attempt) == :dead

    UnboxedFixture.run_unboxed(fn ->
      assert {:ok, %{absent_instance_attempts_recovered: 1}} =
               Accounting.recover_absent_instance_attempts(InstancePresence.database_now())

      assert Repo.reload!(request).last_error_code == "absent_instance_recovered"
    end)

    monitor = Process.monitor(owner)
    send(owner, :stop)
    assert_receive {:DOWN, ^monitor, :process, ^owner, :normal}, 15_000
    CodexPooler.PeerRegistry.assert_peer_absent!(peer_name, peer_node: remote)
    CodexPooler.InstancePresencePeer.assert_os_process_absent!(os_pid)

    CodexPooler.TestDiagnostics.puts(
      "presence starvation: failed_writes=#{failures} age_seconds=#{age} live_unknown_preserved=true terminal_recovered=true"
    )
  end

  defp start_distribution! do
    if node() == :nonode@nohost do
      previous = Application.fetch_env(:kernel, :prevent_overlapping_partitions)

      on_exit(fn ->
        :net_kernel.stop()

        restore_distribution_config(previous)
      end)

      {_, 0} = System.cmd("epmd", ["-daemon"])
      CodexPooler.PeerRegistry.assert_epmd_ready!()
      Application.put_env(:kernel, :prevent_overlapping_partitions, false)

      {:ok, _} =
        :net_kernel.start([
          :"presence_observer_#{System.unique_integer([:positive])}",
          :shortnames
        ])
    end
  end

  defp restore_distribution_config({:ok, value}),
    do: Application.put_env(:kernel, :prevent_overlapping_partitions, value)

  defp restore_distribution_config(:error),
    do: Application.delete_env(:kernel, :prevent_overlapping_partitions)
end
