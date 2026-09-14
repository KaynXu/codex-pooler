defmodule CodexPooler.Platform.ExecutionIdentityTest do
  use ExUnit.Case, async: false

  alias CodexPooler.Platform.ExecutionIdentity
  alias CodexPooler.Platform.InstancePresence.Identity

  test "a connected peer reports the exact process alive and dead, with unknown on RPC failure" do
    if node() == :nonode@nohost do
      previous = Application.fetch_env(:kernel, :prevent_overlapping_partitions)

      on_exit(fn ->
        :net_kernel.stop()

        case previous do
          {:ok, value} -> Application.put_env(:kernel, :prevent_overlapping_partitions, value)
          :error -> Application.delete_env(:kernel, :prevent_overlapping_partitions)
        end
      end)

      {_, 0} = System.cmd("epmd", ["-daemon"])
      CodexPooler.PeerRegistry.assert_epmd_ready!()
      Application.put_env(:kernel, :prevent_overlapping_partitions, false)

      {:ok, _} =
        :net_kernel.start([:"execution_local_#{System.unique_integer([:positive])}", :shortnames])
    end

    name = :"execution_peer_#{System.unique_integer([:positive])}"
    parent = self()

    peer_owner =
      start_supervised!(
        {Task,
         fn ->
           {:ok, peer, remote} =
             :peer.start_link(%{
               name: name,
               args: [~c"+S", ~c"2:2", ~c"-kernel", ~c"prevent_overlapping_partitions", ~c"false"]
             })

           send(parent, {:peer_started, peer, remote})

           receive do
             :stop -> :peer.stop(peer)
           end
         end}
      )

    assert_receive {:peer_started, peer, remote}, 15_000
    :ok = :erpc.call(remote, :code, :add_paths, [:code.get_path()])
    :erpc.call(remote, Identity, :mint_boot_id!, [])

    {:ok, _} =
      :erpc.call(remote, GenServer, :start, [
        CodexPooler.Platform.ExecutionRegistry,
        nil,
        [name: CodexPooler.Platform.ExecutionRegistry]
      ])

    {pid, _} =
      :erpc.call(remote, Code, :eval_string, [
        """
        spawn(fn ->
          owner = CodexPooler.Platform.InstancePresence.Identity.local()
          identity = Map.merge(CodexPooler.Platform.ExecutionIdentity.local(), %{owner_instance_id: owner.node_name, owner_instance_boot_id: owner.boot_id})
          send(parent, {:remote_identity, self(), identity})
          receive do
            :stop -> :ok
          end
        end)
        """,
        [parent: parent]
      ])

    assert_receive {:remote_identity, ^pid, identity}, 15_000
    assert ExecutionIdentity.status(identity) == :alive

    assert ExecutionIdentity.status(%{identity | owner_execution_id: Ecto.UUID.generate()}) ==
             :unknown

    assert ExecutionIdentity.status(%{identity | owner_instance_boot_id: Ecto.UUID.generate()}) ==
             :unknown

    monitor = Process.monitor(pid)
    send(pid, :stop)
    assert_receive {:DOWN, ^monitor, :process, ^pid, :normal}, 15_000
    assert ExecutionIdentity.status(identity) == :dead

    CodexPooler.TestDiagnostics.puts(
      "execution identity peer: connected=true active=alive unregistered_token=unknown wrong_boot=unknown monitored_exit=dead"
    )

    :erpc.call(remote, :code, :purge, [ExecutionIdentity])
    :erpc.call(remote, :code, :delete, [ExecutionIdentity])
    :erpc.call(remote, :code, :set_path, [[]])
    assert ExecutionIdentity.status(identity) == :unknown
    peer_monitor = Process.monitor(peer)
    send(peer_owner, :stop)
    assert_receive {:DOWN, ^peer_monitor, :process, ^peer, _}, 15_000
    CodexPooler.PeerRegistry.assert_peer_absent!(name, peer_node: remote)
    assert ExecutionIdentity.status(identity) == :unknown
  end

  test "identity is stable within a process and an unregistered UUID stays unknown" do
    local = ExecutionIdentity.local()
    assert ExecutionIdentity.local() == local
    owner = Identity.local()

    identity =
      Map.merge(local, %{
        owner_instance_id: owner.node_name,
        owner_instance_boot_id: owner.boot_id
      })

    assert ExecutionIdentity.status(identity) == :alive

    assert ExecutionIdentity.status(%{identity | owner_execution_id: Ecto.UUID.generate()}) ==
             :unknown

    assert Process.alive?(self())
  end

  test "explicit completion retires the exact execution on a live connection process" do
    owner = Identity.local()
    first = ExecutionIdentity.local()

    identity =
      Map.merge(first, %{
        owner_instance_id: owner.node_name,
        owner_instance_boot_id: owner.boot_id
      })

    assert ExecutionIdentity.status(identity) == :alive
    assert :ok == ExecutionIdentity.complete()
    assert Process.alive?(self())
    assert ExecutionIdentity.status(identity) == :dead
    assert ExecutionIdentity.local().owner_execution_id != first.owner_execution_id
  end

  test "registry loss and replacement cannot claim an unknown execution died" do
    alias CodexPooler.Platform.ExecutionRegistry

    registry =
      start_supervised!(
        Supervisor.child_spec({ExecutionRegistry, name: nil}, restart: :temporary)
      )

    id = Ecto.UUID.generate()
    assert :ok == ExecutionRegistry.register(id, registry)
    assert :alive == ExecutionRegistry.status(id, self(), registry)
    monitor = Process.monitor(registry)
    Process.exit(registry, :kill)
    assert_receive {:DOWN, ^monitor, :process, ^registry, :killed}, 15_000
    assert :unknown == ExecutionRegistry.status(id, self(), registry)
    replacement = start_supervised!({ExecutionRegistry, name: nil})
    assert :unknown == ExecutionRegistry.status(id, self(), replacement)
  end

  test "registry rejects mismatched completion and expires completed proof to unknown" do
    alias CodexPooler.Platform.ExecutionRegistry
    registry = start_supervised!({ExecutionRegistry, name: nil})
    id = Ecto.UUID.generate()
    assert :ok == ExecutionRegistry.register(id, registry)
    assert :ok == ExecutionRegistry.register(id, registry)
    assert :unknown == ExecutionRegistry.status(id, registry, registry)
    assert :unknown == ExecutionRegistry.complete(Ecto.UUID.generate(), registry)
    assert :ok == ExecutionRegistry.complete(id, registry)
    assert :dead == ExecutionRegistry.status(id, self(), registry)
    assert :unknown == ExecutionRegistry.register(id, registry)
    assert :unknown == ExecutionRegistry.complete(id, registry)
    assert :ok == ExecutionRegistry.acknowledge([id], registry)
    send(registry, {:expire, id})
    assert :unknown == ExecutionRegistry.status(id, self(), registry)
    send(registry, {:DOWN, make_ref(), :process, self(), :normal})
    assert :unknown == ExecutionRegistry.status(id, self(), registry)
  end

  test "sensitive production response tasks keep exact registration until death" do
    alias CodexPooler.Gateway.Websocket.ResponseTask
    parent = self()

    for kind <- [:direct, :proxy, :local_owner] do
      {:ok, pid} =
        ResponseTask.start(
          parent,
          kind,
          fn _ ->
            owner = Identity.local()

            identity =
              Map.merge(ExecutionIdentity.local(), %{
                owner_instance_id: owner.node_name,
                owner_instance_boot_id: owner.boot_id
              })

            send(
              parent,
              {:registered, self(), identity, {:sensitive, Process.flag(:sensitive, true)}}
            )

            receive do
              :stop -> :ok
            end
          end,
          fn _, _ -> :ok end
        )

      on_exit(fn -> if Process.alive?(pid), do: Process.exit(pid, :kill) end)
      monitor = Process.monitor(pid)
      assert_receive {:registered, ^pid, identity, {:sensitive, true}}, 15_000
      assert ExecutionIdentity.status(identity) == :alive
      Process.exit(pid, :kill)
      assert_receive {:DOWN, ^monitor, :process, ^pid, :killed}, 15_000
      assert ExecutionIdentity.status(identity) == :dead
    end
  end

  test "missing, malformed, foreign incarnation and disconnected node stay unknown" do
    owner = Identity.local()

    identity =
      Map.merge(ExecutionIdentity.local(), %{
        owner_instance_id: owner.node_name,
        owner_instance_boot_id: owner.boot_id
      })

    for candidate <- [
          %{},
          %{identity | owner_process_id: "malformed"},
          %{identity | owner_process_id: "<1.2.3>"},
          %{identity | owner_execution_id: nil},
          %{identity | owner_instance_boot_id: Ecto.UUID.generate()},
          %{identity | owner_instance_id: "disconnected@example.invalid"}
        ] do
      assert ExecutionIdentity.status(candidate) == :unknown
    end
  end
end
