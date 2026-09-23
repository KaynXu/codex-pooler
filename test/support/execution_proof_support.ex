defmodule CodexPooler.ExecutionProofSupport do
  @moduledoc false
  import ExUnit.Assertions
  alias CodexPooler.Platform.{ExecutionIdentity, ExecutionRegistry, ExecutionTerminalProofs}

  @spec publish_committed_terminal!(map()) :: :ok
  def publish_committed_terminal!(identity) do
    CodexPooler.UnboxedFixture.register_unboxed_cleanup!(fn ->
      case CodexPooler.Repo.get(
             CodexPooler.Platform.ExecutionTerminalProof,
             identity.owner_execution_id
           ) do
        nil -> :ok
        proof -> CodexPooler.Repo.delete!(proof)
      end
    end)

    CodexPooler.UnboxedFixture.run_unboxed(fn -> publish_terminal!(identity) end)
  end

  @spec publish_terminal!(map()) :: :ok
  def publish_terminal!(identity) do
    assert ExecutionIdentity.status(identity) == :dead

    proof =
      ExecutionRegistry.pending(10_000)
      |> Enum.find(&(&1.owner_execution_id == identity.owner_execution_id))

    if proof do
      assert {:ok, 1} = ExecutionTerminalProofs.publish([proof])
      assert :ok = ExecutionRegistry.acknowledge([proof.owner_execution_id])
    end

    assert ExecutionTerminalProofs.terminal?(identity)
    :ok
  end

  @spec await_terminal!(map(), pid() | nil) :: :ok
  def await_terminal!(identity, publisher \\ nil),
    do: await_terminal(identity, publisher, System.monotonic_time(:millisecond) + 15_000)

  defp await_terminal(identity, publisher, deadline) do
    if ExecutionTerminalProofs.terminal?(identity) do
      :ok
    else
      assert System.monotonic_time(:millisecond) < deadline,
             "terminal execution proof was not published"

      # Drive the owned production publisher on demand. Its default one-second
      # cadence otherwise charges every batch left by earlier sandbox tests to
      # this test, making serial and partitioned order produce different costs.
      if is_pid(publisher) do
        send(publisher, :publish)
        :sys.get_state(publisher)
      end

      receive do
      after
        10 -> :ok
      end

      await_terminal(identity, publisher, deadline)
    end
  end
end
