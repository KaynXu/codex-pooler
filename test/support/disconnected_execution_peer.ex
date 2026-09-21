defmodule CodexPooler.DisconnectedExecutionPeer do
  @moduledoc false

  alias CodexPooler.Accounting
  alias CodexPooler.Gateway.Transports.WebsocketOwnerNodeHarness

  alias CodexPooler.Platform.{
    ExecutionIdentity,
    ExecutionProofPublisher,
    ExecutionRegistry,
    ExecutionTerminalProofs
  }

  alias CodexPooler.Platform.InstancePresence.Identity

  @spec bootstrap(keyword(), keyword()) :: :ok
  def bootstrap(env, config) do
    Enum.each(env, fn {key, value} -> Application.put_env(:codex_pooler, key, value) end)
    {:ok, _} = Application.ensure_all_started(:ecto_sql)
    {:ok, _} = Application.ensure_all_started(:postgrex)
    {:ok, _} = Application.ensure_all_started(:phoenix_pubsub)
    {:ok, _} = Application.ensure_all_started(:ex_unit)

    {:ok, supervisor} =
      Supervisor.start_link([{Phoenix.PubSub, name: CodexPooler.PubSub}], strategy: :one_for_one)

    Process.unlink(supervisor)
    boot_id = Identity.mint_boot_id!()

    WebsocketOwnerNodeHarness.start_repo(
      Keyword.merge(config,
        pool: DBConnection.ConnectionPool,
        log: false,
        parameters: [application_name: "execution_peer_" <> boot_id]
      )
    )

    {:ok, _} = GenServer.start(ExecutionRegistry, nil, name: ExecutionRegistry)
    {:ok, publisher} = ExecutionProofPublisher.start_link(enabled: true)
    Process.unlink(publisher)
    :ok
  end

  @spec start(map()) :: {struct(), struct(), pid()}
  def start(setup) do
    caller = self()

    pid =
      spawn(fn ->
        {:ok, reserved} =
          Accounting.reserve(
            setup.auth,
            setup.model,
            %{"model" => setup.model.exposed_model_id, "max_output_tokens" => 10},
            %{transport: "http_sse", correlation_id: Ecto.UUID.generate()}
          )

        {:ok, attempt} = Accounting.create_attempt(reserved.request, setup.assignment)
        send(caller, {:started, reserved.request, attempt, self()})

        receive do
          :finish -> :ok
        end
      end)

    receive do
      {:started, request, attempt, ^pid} -> {request, attempt, pid}
    after
      15_000 -> raise "execution did not start"
    end
  end

  @spec finish(pid(), struct()) :: :ok
  def finish(pid, attempt) do
    end_process(pid, attempt)
    CodexPooler.ExecutionProofSupport.await_terminal!(attempt)
  end

  @spec finish_without_database(pid(), struct()) :: map()
  def finish_without_database(pid, attempt) do
    Supervisor.stop(CodexPooler.Repo)

    logs =
      ExUnit.CaptureLog.capture_log(fn ->
        end_process(pid, attempt)
        publisher = CodexPooler.Platform.ExecutionProofPublisher
        send(publisher, :publish)
        %{failed: true} = :sys.get_state(publisher)
      end)

    %{
      queued: Enum.count(ExecutionRegistry.pending(100)),
      warned: String.contains?(logs, "publication unavailable"),
      publisher_alive: Process.alive?(Process.whereis(CodexPooler.Platform.ExecutionProofPublisher))
    }
  end

  @spec restore_database(struct()) :: :ok
  def restore_database(attempt) do
    WebsocketOwnerNodeHarness.start_repo(Application.fetch_env!(:codex_pooler, CodexPooler.Repo))

    send(CodexPooler.Platform.ExecutionProofPublisher, :publish)
    CodexPooler.ExecutionProofSupport.await_terminal!(attempt)
  end

  @spec finish_with_uncertain_commit(pid(), struct()) :: map()
  def finish_with_uncertain_commit(pid, attempt) do
    observer = self()
    handler = {__MODULE__, make_ref()}
    publisher = Process.whereis(CodexPooler.Platform.ExecutionProofPublisher)

    :ok =
      :telemetry.attach(
        handler,
        [:codex_pooler, :repo, :query],
        &__MODULE__.hold_commit/4,
        {publisher, observer}
      )

    try do
      end_process(pid, attempt)
      send(publisher, :publish)

      receive do
        {:proof_committed, ^publisher} -> :ok
      after
        15_000 -> raise "publisher commit was not observed"
      end

      monitor = Process.monitor(publisher)
      Process.exit(publisher, :kill)

      receive do
        {:DOWN, ^monitor, :process, ^publisher, :killed} -> :ok
      after
        15_000 -> raise "publisher did not stop"
      end

      persisted = ExecutionTerminalProofs.terminal?(attempt)
      pending = Enum.count(ExecutionRegistry.pending(100))
      {:ok, replacement} = ExecutionProofPublisher.start_link(enabled: true)
      Process.unlink(replacement)
      await_acknowledged(System.monotonic_time(:millisecond) + 15_000)

      %{
        committed_before_ack: persisted,
        pending_before_restart: pending,
        pending_after_restart: 0
      }
    after
      :telemetry.detach(handler)
    end
  end

  @doc false
  def hold_commit(_event, _measurements, metadata, {publisher, observer}) do
    if self() == publisher and metadata.query == "commit" do
      send(observer, {:proof_committed, self()})

      receive do
        :release_commit -> :ok
      after
        15_000 -> raise "publisher commit barrier not released"
      end
    end
  end

  defp await_acknowledged(deadline) do
    if ExecutionRegistry.pending(100) == [] do
      :ok
    else
      if System.monotonic_time(:millisecond) >= deadline, do: raise("proof was not acknowledged")

      receive do
      after
        10 -> :ok
      end

      await_acknowledged(deadline)
    end
  end

  defp end_process(pid, attempt) do
    monitor = Process.monitor(pid)
    send(pid, :finish)

    receive do
      {:DOWN, ^monitor, :process, ^pid, :normal} -> :ok
    after
      15_000 -> raise "execution did not stop"
    end

    :dead = ExecutionIdentity.status(attempt)
    :ok
  end
end
