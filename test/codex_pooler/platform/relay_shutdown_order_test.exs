defmodule CodexPooler.Platform.RelayShutdownOrderTest do
  use CodexPooler.DataCase, async: false
  alias CodexPooler.Gateway.OperationalStatus
  alias CodexPooler.Gateway.Transports.Streaming.DeferredStreamRegistry
  alias CodexPooler.Gateway.Transports.Websocket.{ActivityRegistry, RolloutDrain}
  alias CodexPooler.Release
  alias CodexPooler.Telemetry.RelayRuntime
  alias CodexPooler.TestAppEnv
  alias Ecto.Adapters.SQL.Sandbox

  test "supervised consumer restart under readiness marker cannot claim", context do
    marker = Path.join(System.tmp_dir!(), "relay-restart-#{Ecto.UUID.generate()}")
    on_exit(fn -> File.rm(marker) end)
    prior = TestAppEnv.restore_on_exit(OperationalStatus, [])

    Application.put_env(
      :codex_pooler,
      OperationalStatus,
      Keyword.put(prior, :drain_marker_path, marker)
    )

    parent = self()

    spec =
      {RelayRuntime,
       enabled: true,
       role: "web",
       name: nil,
       start_paused: true,
       claim_fun: fn _, _ ->
         send(parent, :claimed)
         {:ok, []}
       end}

    first = start_supervised!(spec)
    Sandbox.allow(Repo, context.sandbox_owner, first)
    send(first, :drain)
    :sys.get_state(first)
    assert_receive :claimed
    File.touch!(marker)
    stop_supervised!(RelayRuntime)
    replacement = start_supervised!(spec)
    Sandbox.allow(Repo, context.sandbox_owner, replacement)
    assert :sys.get_state(replacement).quiesced?
    send(replacement, :drain)
    :sys.get_state(replacement)
    refute_received :claimed
    stop_supervised!(RelayRuntime)
    File.rm!(marker)
    fresh = start_supervised!(spec)
    Sandbox.allow(Repo, context.sandbox_owner, fresh)
    send(fresh, :drain)
    :sys.get_state(fresh)
    assert_receive :claimed
  end

  test "only a shutdown drain quiesces the relay consumer", context do
    parent = self()

    runtime =
      start_supervised!(
        {RelayRuntime,
         enabled: true,
         role: "web",
         name: nil,
         start_paused: true,
         claim_fun: fn _, _ ->
           send(parent, :claimed)
           {:ok, []}
         end}
      )

    Sandbox.allow(Repo, context.sandbox_owner, runtime)
    activity_registry = :"relay-drain-activity-#{System.unique_integer([:positive])}"
    stream_registry = :"relay-drain-streams-#{System.unique_integer([:positive])}"
    drain_name = :"relay-drain-#{System.unique_integer([:positive])}"
    start_supervised!({ActivityRegistry, name: activity_registry})
    start_supervised!({DeferredStreamRegistry, name: stream_registry})

    start_supervised!(
      {RolloutDrain,
       name: drain_name,
       activity_registry: activity_registry,
       stream_registry: stream_registry,
       relay: runtime}
    )

    %{result: :ok} = RolloutDrain.start_drain(name: drain_name, timeout_ms: 1_000)
    refute :sys.get_state(runtime).quiesced?
    send(runtime, :drain)
    :sys.get_state(runtime)
    assert_receive :claimed

    %{result: :ok} = RolloutDrain.drain_for_shutdown(1_000, name: drain_name)
    assert :sys.get_state(runtime).quiesced?
    send(runtime, :drain)
    :sys.get_state(runtime)
    refute_received :claimed
  end

  test "release helper quiesces before readiness marker and charges the same budget", context do
    marker = Path.join(System.tmp_dir!(), "relay-shutdown-#{Ecto.UUID.generate()}")
    on_exit(fn -> File.rm(marker) end)
    parent = self()

    runtime =
      start_supervised!(
        {RelayRuntime,
         enabled: true,
         role: "web",
         name: nil,
         start_paused: true,
         claim_fun: fn _, _ ->
           send(parent, {:claim, self()})

           receive do
             :release -> {:ok, []}
           end
         end}
      )

    Sandbox.allow(Repo, context.sandbox_owner, runtime)
    callbacks = :sys.get_state(runtime).callbacks
    send(runtime, :drain)
    assert_receive {:claim, ^runtime}

    task =
      Task.async(fn ->
        Release.prepare_shutdown(
          relay: runtime,
          marker: marker,
          budget_ms: 1000,
          drain: fn remaining ->
            assert File.exists?(marker)
            assert :sys.get_state(runtime).quiesced?
            assert remaining in 1..1000
            %{remaining: remaining}
          end
        )
      end)

    await_gate_closed(callbacks, System.monotonic_time(:millisecond) + 15_000)
    refute File.exists?(marker)
    send(runtime, :release)
    assert %{remaining: _} = Task.await(task)
    send(runtime, :drain)
    :sys.get_state(runtime)
    refute_received {:claim, _}
  end

  defp await_gate_closed(table, deadline) do
    if :ets.lookup(table, :quiesced) != [{:quiesced, true}] do
      assert System.monotonic_time(:millisecond) < deadline

      receive do
      after
        1 -> :ok
      end

      await_gate_closed(table, deadline)
    end
  end
end
