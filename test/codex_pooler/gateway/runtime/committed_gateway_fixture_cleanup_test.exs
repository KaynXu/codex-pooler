defmodule CodexPooler.Gateway.Runtime.CommittedGatewayFixtureCleanupTest do
  use CodexPooler.DataCase, async: false

  alias CodexPooler.Catalog.PricingSnapshot
  alias CodexPooler.FakeUpstream
  alias CodexPooler.Jobs.{SavedResetRedemptionWorker, TokenRefreshWorker}
  alias CodexPooler.Upstreams.Schemas.PoolUpstreamAssignment
  alias CodexPooler.Upstreams.Schemas.UpstreamIdentity
  alias CodexPoolerWeb.Runtime.BackendCodexTestSupport, as: Support
  alias Ecto.Adapters.SQL.Sandbox

  test "gateway cleanup removes assignment-only and identity-only jobs before child cascades" do
    fixture = CodexPooler.AccountingTestSupport.accounting_setup()

    assignment_job =
      %{pool_upstream_assignment_id: fixture.assignment.id}
      |> SavedResetRedemptionWorker.new()
      |> Repo.insert!()

    identity_job =
      %{upstream_identity_id: fixture.identity.id}
      |> TokenRefreshWorker.new()
      |> Repo.insert!()

    Support.cleanup_unboxed_pool!(fixture)

    assert Repo.all(
             from job in Oban.Job,
               where: job.id in ^[assignment_job.id, identity_job.id],
               select: job.id
           ) == []

    Support.cleanup_unboxed_pool!(fixture)
  end

  test "gateway cleanup preserves a shared identity and its identity-only job" do
    fixture = CodexPooler.AccountingTestSupport.accounting_setup()
    other_pool = CodexPooler.PoolerFixtures.pool_fixture()

    other_assignment =
      fixture.assignment
      |> Map.from_struct()
      |> Map.drop([:__meta__, :id])
      |> Map.put(:pool_id, other_pool.id)
      |> then(&struct(CodexPooler.Upstreams.Schemas.PoolUpstreamAssignment, &1))
      |> Repo.insert!()

    identity_job =
      %{upstream_identity_id: fixture.identity.id}
      |> TokenRefreshWorker.new()
      |> Repo.insert!()

    Support.cleanup_unboxed_pool!(fixture)

    assert Repo.get(UpstreamIdentity, fixture.identity.id)
    assert Repo.get(PoolUpstreamAssignment, other_assignment.id)
    assert Repo.get(Oban.Job, identity_job.id)
  end

  test "committed gateway cleanup deletes its exact pricing and preserves same-model fixtures" do
    {:ok, fake} = FakeUpstream.start_link({:json, 200, %{}})

    Sandbox.unboxed_run(Repo, fn ->
      fixture = Support.gateway_setup(fake)
      other = Support.gateway_setup(fake)
      on_exit(fn -> Sandbox.unboxed_run(Repo, fn -> Support.cleanup_unboxed_pool!(other) end) end)
      assert fixture.pricing.model_identifier == other.pricing.model_identifier
      refute fixture.pricing.price_version == other.pricing.price_version
      Support.cleanup_unboxed_pool!(fixture)
      refute Repo.get(PricingSnapshot, fixture.pricing.id)
      assert Repo.get!(PricingSnapshot, other.pricing.id)
      assert Repo.get!(UpstreamIdentity, other.identity.id)
      Support.cleanup_unboxed_pool!(fixture)
    end)
  end
end
