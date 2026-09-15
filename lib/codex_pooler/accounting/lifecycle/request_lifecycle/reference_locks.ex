defmodule CodexPooler.Accounting.RequestLifecycle.ReferenceLocks do
  @moduledoc false

  import Ecto.Query

  alias CodexPooler.Accounting.Metadata
  alias CodexPooler.Repo

  alias CodexPooler.Upstreams.Schemas.{
    PoolUpstreamAssignment,
    UpstreamIdentity
  }

  @type identity_id :: Ecto.UUID.t() | nil
  @type assignment_id :: Ecto.UUID.t() | nil

  @type locked_references :: %{
          required(:identity) => UpstreamIdentity.t() | nil,
          required(:assignment) => PoolUpstreamAssignment.t() | nil
        }

  @spec lock_and_validate!(identity_id(), assignment_id()) :: locked_references() | no_return()
  def lock_and_validate!(upstream_identity_id, pool_upstream_assignment_id) do
    case lock_and_validate(upstream_identity_id, pool_upstream_assignment_id) do
      {:ok, locked} -> locked
      {:error, reason} -> Repo.rollback(reason)
    end
  end

  @doc """
  `lock_and_validate!/2` without the rollback: a missing or mismatched pair is
  returned as `{:error, accounting_error}` so a caller running inside another
  transaction can degrade under its own savepoint instead of rolling back the
  enclosing work.
  """
  @spec lock_and_validate(identity_id(), assignment_id()) ::
          {:ok, locked_references()} | {:error, Metadata.accounting_error()}
  def lock_and_validate(upstream_identity_id, pool_upstream_assignment_id) do
    unless Repo.in_transaction?() do
      raise ArgumentError, "upstream reference locks require an active transaction"
    end

    lock_pair(upstream_identity_id, pool_upstream_assignment_id)
  end

  @doc false
  @spec await_assignment_lock_release(assignment_id()) ::
          :ok | {:error, Metadata.accounting_error()}
  def await_assignment_lock_release(pool_upstream_assignment_id) do
    if Repo.in_transaction?() do
      raise ArgumentError, "assignment release wait requires a fresh transaction"
    end

    Repo.transact(fn ->
      lock_assignment!(pool_upstream_assignment_id)
      {:ok, :ok}
    end)
    |> then(fn
      {:ok, :ok} -> :ok
      {:error, reason} -> {:error, reason}
    end)
  end

  defp lock_pair(nil, nil), do: {:ok, %{identity: nil, assignment: nil}}

  defp lock_pair(nil, _pool_upstream_assignment_id),
    do: {:error, error(:upstream_identity_not_found, "upstream identity was not found")}

  defp lock_pair(_upstream_identity_id, nil),
    do:
      {:error,
       error(:pool_upstream_assignment_not_found, "pool upstream assignment was not found")}

  defp lock_pair(upstream_identity_id, pool_upstream_assignment_id) do
    identity =
      Repo.one(
        from identity in UpstreamIdentity,
          where: identity.id == ^upstream_identity_id,
          lock: "FOR KEY SHARE"
      )

    assignment =
      Repo.one(
        from assignment in PoolUpstreamAssignment,
          where: assignment.id == ^pool_upstream_assignment_id,
          lock: "FOR SHARE"
      )

    cond do
      is_nil(identity) ->
        {:error, error(:upstream_identity_not_found, "upstream identity was not found")}

      is_nil(assignment) ->
        {:error,
         error(:pool_upstream_assignment_not_found, "pool upstream assignment was not found")}

      assignment.upstream_identity_id != identity.id ->
        {:error,
         error(
           :upstream_reference_mismatch,
           "pool upstream assignment does not belong to upstream identity"
         )}

      true ->
        {:ok, %{identity: identity, assignment: assignment}}
    end
  end

  # A fresh assignment-only transaction cannot participate in the historical
  # identity/assignment AB-BA cycle. Deadlock retries use it to let an
  # assignment-first holder finish before reacquiring the canonical pair.
  defp lock_assignment!(pool_upstream_assignment_id) do
    Repo.one(
      from assignment in PoolUpstreamAssignment,
        where: assignment.id == ^pool_upstream_assignment_id,
        lock: "FOR SHARE"
    ) || rollback!(:pool_upstream_assignment_not_found, "pool upstream assignment was not found")
  end

  defp rollback!(code, message), do: Repo.rollback(error(code, message))
  defp error(code, message), do: Metadata.accounting_error(code, message)
end
