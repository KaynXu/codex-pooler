defmodule CodexPooler.InstancePresencePeer do
  @moduledoc false
  import ExUnit.Callbacks
  import ExUnit.Assertions
  alias CodexPooler.Platform.InstancePresence.Identity
  @spec start_presence_peer!(atom()) :: map()
  def start_presence_peer!(name) do
    on_exit(fn -> CodexPooler.PeerRegistry.assert_peer_absent!(name) end)

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
        :net_kernel.start([:"lease_observer_#{System.unique_integer([:positive])}", :shortnames])
    end

    parent = self()

    owner =
      start_supervised!(
        {Task,
         fn ->
           {:ok, peer, remote} =
             :peer.start_link(%{
               name: name,
               args: [~c"+S", ~c"2:2", ~c"-kernel", ~c"prevent_overlapping_partitions", ~c"false"]
             })

           send(parent, {:presence_peer, self(), peer, remote})

           receive do
             :stop -> :peer.stop(peer)
           end
         end},
        id: make_ref()
      )

    assert_receive {:presence_peer, ^owner, peer, remote}, 15_000
    :ok = :erpc.call(remote, :code, :add_paths, [:code.get_path()])
    :erpc.call(remote, Identity, :mint_boot_id!, [])
    identity = :erpc.call(remote, Identity, :local, [])
    %{owner: owner, peer: peer, remote: remote, name: name, identity: identity}
  end

  @spec stop_presence_peer!(map()) :: :ok
  def stop_presence_peer!(peer) do
    monitor = Process.monitor(peer.owner)
    send(peer.owner, :stop)
    assert_receive {:DOWN, ^monitor, :process, _, :normal}, 15_000
    CodexPooler.PeerRegistry.assert_peer_absent!(peer.name, peer_node: peer.remote)
  end

  alias CodexPooler.{Accounting, Repo}
  alias CodexPooler.Gateway.Websocket.ResponseTask
  alias CodexPooler.Platform.{InstanceHeartbeat, InstancePresence}

  @spec start(map()) :: {struct(), struct(), pid()}
  def start(setup) do
    {:ok, registry} =
      GenServer.start(CodexPooler.Gateway.Transports.Websocket.ActivityRegistry, :ok, [])

    parent = self()

    {:ok, pid} =
      ResponseTask.start(
        parent,
        :local_owner,
        fn _ ->
          {:ok, reserved} =
            Accounting.reserve(
              setup.auth,
              setup.model,
              %{"model" => setup.model.exposed_model_id, "max_output_tokens" => 10},
              %{transport: "http_sse", correlation_id: Ecto.UUID.generate()}
            )

          {:ok, attempt} = Accounting.create_attempt(reserved.request, setup.assignment)
          send(parent, {:started, reserved.request, attempt, self()})

          receive do
            :finish -> :ok
          end
        end,
        fn _, _ -> :ok end,
        activity_registry: registry
      )

    receive do
      {:started, request, attempt, ^pid} -> {request, attempt, pid}
    after
      15_000 -> raise "presence execution did not start"
    end
  end

  @spec await_starvation() :: map()
  def await_starvation do
    handler = {__MODULE__, make_ref()}

    :ok =
      :telemetry.attach(
        handler,
        [:codex_pooler, :instance_presence, :heartbeat],
        &__MODULE__.capture/4,
        self()
      )

    try do
      {result, logs} =
        ExUnit.CaptureLog.with_log(fn ->
          {:ok, heartbeat} = InstanceHeartbeat.start_link(enabled: true)
          Process.unlink(heartbeat)

          try do
            await_failures(0)
          after
            GenServer.stop(heartbeat)
          end
        end)

      Map.put(result, :warned, String.contains?(logs, "instance presence heartbeat write failed"))
    after
      :telemetry.detach(handler)
    end
  end

  @doc false
  def capture(_event, %{failures: 1}, %{}, parent), do: send(parent, :heartbeat_failed)

  defp restore_distribution_config({:ok, value}),
    do: Application.put_env(:kernel, :prevent_overlapping_partitions, value)

  defp restore_distribution_config(:error),
    do: Application.delete_env(:kernel, :prevent_overlapping_partitions)

  defp await_failures(count) do
    receive do
      :heartbeat_failed ->
        identity = InstancePresence.local_identity()

        %{rows: [[age]]} =
          Repo.query!(
            "SELECT EXTRACT(EPOCH FROM clock_timestamp() - last_seen_at)::float8 FROM instance_presences WHERE instance_id=$1",
            [identity.instance_id]
          )

        if count + 1 >= 8 and age > InstancePresence.liveness_window_seconds(),
          do: %{failures: count + 1, age_seconds: age},
          else: await_failures(count + 1)
    after
      30_000 -> raise "production heartbeat did not report a failed write"
    end
  end
end
