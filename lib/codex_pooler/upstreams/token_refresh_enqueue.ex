defmodule CodexPooler.Upstreams.TokenRefreshEnqueue do
  @moduledoc false

  alias CodexPooler.Accounts.Scope
  alias CodexPooler.Jobs.UpstreamEnqueue
  alias CodexPooler.Repo

  alias CodexPooler.Upstreams.Assignments.PoolAssignments
  alias CodexPooler.Upstreams.Lifecycle.{AccountAudit, AccountLifecycle}
  alias CodexPooler.Upstreams.Schemas.UpstreamIdentity
  alias CodexPooler.Upstreams.Secrets

  @type lifecycle_error :: CodexPooler.Upstreams.lifecycle_error()
  @type identity_ref :: UpstreamIdentity.t() | Ecto.UUID.t()

  @spec enqueue_for_scope(Scope.t(), identity_ref(), keyword()) ::
          {:ok, map()} | {:error, lifecycle_error() | Ecto.Changeset.t()}
  def enqueue_for_scope(scope, identity_or_id, opts \\ [])

  def enqueue_for_scope(%Scope{} = scope, identity_or_id, opts) when is_list(opts) do
    trigger_kind = Keyword.get(opts, :trigger_kind, "admin_upstreams_live")

    with {:ok, identity} <- AccountLifecycle.authorize(scope, identity_or_id),
         {:ok, job} <-
           UpstreamEnqueue.enqueue_token_refresh(
             identity,
             Keyword.put(opts, :trigger_kind, trigger_kind)
           ) do
      result = %{
        status: if(job.conflict?, do: :already_queued, else: :queued),
        identity: Repo.reload!(identity),
        assignments: PoolAssignments.list_pool_assignments_for_identity(identity.id),
        secret_status: Secrets.secret_status(identity),
        job: job
      }

      {:ok, result}
      |> AccountAudit.record_change(scope, "upstream_account.refresh_enqueue",
        trigger_kind: trigger_kind,
        job_conflict?: job.conflict?
      )
    end
  end

  def enqueue_for_scope(_scope, _identity_or_id, _opts),
    do: {:error, CodexPooler.Upstreams.lifecycle_error(:invalid_request, "user scope is required")}
end
