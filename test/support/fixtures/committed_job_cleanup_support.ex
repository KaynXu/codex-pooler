defmodule CodexPooler.CommittedJobCleanupSupport do
  @moduledoc false

  import Ecto.Query
  import ExUnit.Assertions
  import CodexPooler.PoolerFixtures

  alias CodexPooler.Jobs.{SavedResetRedemptionWorker, TokenRefreshWorker}
  alias CodexPooler.Pools.Pool
  alias CodexPooler.Repo
  alias CodexPooler.UnboxedFixture
  alias CodexPooler.Upstreams.Assignments.PoolAssignments
  alias CodexPooler.Upstreams.Schemas.{PoolUpstreamAssignment, UpstreamIdentity}

  @spec assert_cleanup_jobs!(
          Pool.t(),
          UpstreamIdentity.t(),
          PoolUpstreamAssignment.t(),
          (-> term())
        ) :: :ok
  def assert_cleanup_jobs!(pool, identity, assignment, cleanup) do
    suffix = Ecto.UUID.generate()
    shared_account_id = "cleanup-shared-#{suffix}"
    other_pool_slug = "cleanup-sentinel-#{suffix}"

    # This fallback also removes deliberately leaked jobs during mutation proof. The
    # assertions below run before it, so it cannot hide an incorrect owning cleanup.
    UnboxedFixture.register_unboxed_cleanup!(fn ->
      shared_ids =
        Repo.all(
          from i in UpstreamIdentity,
            where: i.chatgpt_account_id == ^shared_account_id,
            select: i.id
        )

      identity_ids = [identity.id | shared_ids]

      Repo.delete_all(
        from job in Oban.Job,
          where:
            fragment("?->>'pool_upstream_assignment_id'", job.args) == ^assignment.id or
              fragment("?->>'upstream_identity_id'", job.args) in ^identity_ids
      )

      other_pool_ids = Repo.all(from p in Pool, where: p.slug == ^other_pool_slug, select: p.id)
      delete_committed_pools!(other_pool_ids)
      Repo.delete_all(from i in UpstreamIdentity, where: i.id in ^shared_ids)
    end)

    {owned_jobs, shared_job, shared_identity, other_assignment} =
      UnboxedFixture.run_unboxed(fn ->
        other_pool = pool_fixture(%{slug: other_pool_slug})
        shared_identity = upstream_identity_fixture(%{chatgpt_account_id: shared_account_id})

        assert {:ok, _assignment} =
                 PoolAssignments.create_pool_assignment(pool, shared_identity, %{})

        assert {:ok, other_assignment} =
                 PoolAssignments.create_pool_assignment(other_pool, shared_identity, %{})

        assignment_job =
          %{pool_upstream_assignment_id: assignment.id}
          |> SavedResetRedemptionWorker.new()
          |> Oban.insert!()

        identity_job =
          %{upstream_identity_id: identity.id}
          |> TokenRefreshWorker.new()
          |> Oban.insert!()

        shared_job =
          %{upstream_identity_id: shared_identity.id}
          |> TokenRefreshWorker.new()
          |> Oban.insert!()

        {[assignment_job.id, identity_job.id], shared_job, shared_identity, other_assignment}
      end)

    for _invocation <- 1..2 do
      cleanup.()

      # Check the committed database, not the test's sandbox snapshot.
      remaining_owned_jobs =
        UnboxedFixture.run_unboxed(fn ->
          Repo.all(from job in Oban.Job, where: job.id in ^owned_jobs, select: job.id)
        end)

      assert remaining_owned_jobs == [], "owning cleanup left committed Oban jobs behind"

      UnboxedFixture.run_unboxed(fn ->
        assert Repo.get(Oban.Job, shared_job.id)
        assert Repo.get(UpstreamIdentity, shared_identity.id)
        assert Repo.get(PoolUpstreamAssignment, other_assignment.id)
        refute Repo.get(Pool, pool.id)
        refute Repo.get(UpstreamIdentity, identity.id)
      end)
    end

    :ok
  end
end
