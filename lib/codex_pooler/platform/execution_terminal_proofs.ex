defmodule CodexPooler.Platform.ExecutionTerminalProofs do
  @moduledoc false
  import Ecto.Query

  alias CodexPooler.Platform.ExecutionTerminalProof
  alias CodexPooler.Repo

  @retention_seconds 6 * 60 * 60
  @type terminal :: %{
          owner_execution_id: Ecto.UUID.t(),
          owner_instance_id: String.t(),
          owner_instance_boot_id: String.t(),
          owner_process_id: String.t(),
          end_kind: String.t(),
          ended_at: DateTime.t()
        }

  @spec retention_seconds() :: pos_integer()
  def retention_seconds, do: @retention_seconds

  @spec terminal?(map()) :: boolean()
  def terminal?(identity) do
    if valid_identity?(identity) do
      Repo.exists?(
        from proof in ExecutionTerminalProof,
          where:
            proof.execution_id == ^identity.owner_execution_id and
              proof.owner_instance_id == ^identity.owner_instance_id and
              proof.owner_instance_boot_id == ^identity.owner_instance_boot_id and
              proof.owner_process_id == ^identity.owner_process_id
      )
    else
      false
    end
  end

  @spec publish([terminal()]) :: {:ok, non_neg_integer()} | {:error, atom()}
  def publish(proofs) when is_list(proofs) and length(proofs) <= 100 do
    if Enum.all?(proofs, &valid_terminal?/1) do
      rows =
        Enum.map(proofs, fn proof ->
          proof
          |> Map.take([
            :owner_instance_id,
            :owner_instance_boot_id,
            :owner_process_id,
            :end_kind,
            :ended_at
          ])
          |> Map.put(:execution_id, proof.owner_execution_id)
        end)

      publish_rows(rows)
    else
      {:error, :invalid_execution_terminal_proof}
    end
  end

  def publish(_proofs), do: {:error, :invalid_execution_terminal_proof}

  defp publish_rows(rows) do
    Repo.transact(fn ->
      Repo.insert_all(ExecutionTerminalProof, rows,
        on_conflict: :nothing,
        conflict_target: :execution_id
      )

      ids = Enum.map(rows, & &1.execution_id)
      persisted = Repo.all(from p in ExecutionTerminalProof, where: p.execution_id in ^ids)

      exact = Enum.all?(rows, &exact_row?(&1, persisted))

      if exact, do: {:ok, length(rows)}, else: {:error, :execution_terminal_proof_conflict}
    end)
  end

  defp exact_row?(row, persisted),
    do: Enum.any?(persisted, &(Map.take(&1, Map.keys(row)) == row))

  @spec prune(DateTime.t()) :: {:ok, %{execution_terminal_proofs_pruned: non_neg_integer()}}
  def prune(_now) do
    expired =
      from p in ExecutionTerminalProof,
        where:
          p.published_at <
            fragment("(statement_timestamp() AT TIME ZONE 'UTC') - interval '6 hours'"),
        order_by: [asc: p.published_at, asc: p.execution_id],
        limit: 1_000,
        select: p.execution_id

    {count, _} =
      Repo.delete_all(
        from p in ExecutionTerminalProof, where: p.execution_id in subquery(expired)
      )

    {:ok, %{execution_terminal_proofs_pruned: count}}
  end

  @spec valid_identity?(map()) :: boolean()
  def valid_identity?(identity) do
    valid_uuid?(Map.get(identity, :owner_execution_id)) and
      bounded?(Map.get(identity, :owner_instance_id), 255) and
      bounded?(Map.get(identity, :owner_instance_boot_id), 64) and
      bounded?(Map.get(identity, :owner_process_id), 64) and
      Regex.match?(~r/\A<0\.[0-9]+\.[0-9]+>\z/, identity.owner_process_id)
  end

  defp valid_terminal?(proof),
    do:
      valid_identity?(proof) and
        Map.get(proof, :end_kind) in ["completed", "process_down"] and
        match?(%DateTime{}, Map.get(proof, :ended_at))

  defp valid_uuid?(id) when is_binary(id), do: match?({:ok, ^id}, Ecto.UUID.cast(id))
  defp valid_uuid?(_), do: false
  defp bounded?(value, max), do: is_binary(value) and byte_size(value) in 1..max
end
