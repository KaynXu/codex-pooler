defmodule CodexPooler.Accounting.APIKeyDeletionHistoryTest do
  use CodexPooler.DataCase, async: false

  import CodexPooler.AccountingTestSupport
  import CodexPooler.AccountsFixtures

  alias CodexPooler.{Access, Accounting}

  alias CodexPooler.Accounting.{
    APIKeyUsageBucket,
    Attempt,
    LedgerEntry,
    Reporting,
    Request,
    Rollups
  }

  alias CodexPooler.Accounts.Scope

  test "an admitted HTTP attempt settles after its API key is deleted" do
    scope = Scope.for_user(bootstrap_owner_fixture().user, ["instance_owner"])
    setup = accounting_setup()

    assert {:ok, reserved} =
             Accounting.reserve(
               setup.auth,
               setup.model,
               %{"model" => setup.model.exposed_model_id, "input" => []},
               %{
                 endpoint: "/v1/responses",
                 transport: "http_json",
                 correlation_id: Ecto.UUID.generate()
               }
             )

    assert {:ok, attempt} = Accounting.create_attempt(reserved.request, setup.assignment)
    assert {:ok, _} = Access.delete_api_key(scope, setup.api_key)

    assert {:ok, finalized} =
             Accounting.finalize_request(reserved.request, attempt, %{
               response_status_code: 200,
               usage: %{
                 status: "usage_known",
                 source: "upstream",
                 input_tokens: 10,
                 output_tokens: 5,
                 total_tokens: 15
               }
             })

    assert finalized.request.status == "succeeded"
    assert finalized.settlement.api_key_id == nil
    assert finalized.settlement.total_tokens == 15
    refute Accounting.reservation_outstanding?(finalized.request)
  end

  test "deleting an API key preserves its settlement and pool usage history" do
    scope =
      Scope.for_user(bootstrap_owner_fixture().user, ["instance_owner"])

    setup = accounting_setup()

    assert {:ok, reserved} =
             Accounting.reserve(
               setup.auth,
               setup.model,
               %{"model" => setup.model.exposed_model_id, "input" => []},
               %{
                 endpoint: "/v1/responses",
                 transport: "http_json",
                 correlation_id: Ecto.UUID.generate()
               }
             )

    assert {:ok, attempt} = Accounting.create_attempt(reserved.request, setup.assignment)

    assert {:ok, finalized} =
             Accounting.finalize_request(reserved.request, attempt, %{
               response_status_code: 200,
               usage: %{
                 status: "usage_known",
                 source: "upstream",
                 input_tokens: 10,
                 output_tokens: 5,
                 total_tokens: 15
               }
             })

    started_at = DateTime.add(finalized.request.admitted_at, -60, :second)
    ended_at = DateTime.add(finalized.request.completed_at, 60, :second)
    pools = [setup.pool.id]
    before_usage = Reporting.token_totals_by_pool_ids(pools, started_at, ended_at)
    before_cost = Reporting.settled_cost_totals_by_pool_ids(pools, started_at, ended_at)
    assert before_usage == %{setup.pool.id => 15}
    assert before_cost[setup.pool.id] > 0

    assert {:ok, _deleted} = Access.delete_api_key(scope, setup.api_key)
    assert Repo.get!(Request, finalized.request.id).api_key_id == nil
    assert Repo.get!(Attempt, attempt.id).request_id == finalized.request.id
    assert %LedgerEntry{api_key_id: nil} = Repo.get(LedgerEntry, finalized.settlement.id)
    assert Reporting.token_totals_by_pool_ids(pools, started_at, ended_at) == before_usage
    assert Reporting.settled_cost_totals_by_pool_ids(pools, started_at, ended_at) == before_cost

    assert {:ok, 1} =
             Rollups.rebuild_for_date(DateTime.to_date(finalized.request.completed_at))

    request = Repo.reload!(finalized.request)
    settlement = Repo.reload!(finalized.settlement)
    assert :ok = Rollups.replace!(request, settlement, request, settlement)

    # Real trigger boundary: retained history can still be corrected or voided
    # without recreating a usage bucket for a missing key.
    settlement = Repo.get!(LedgerEntry, finalized.settlement.id)

    settlement
    |> Ecto.Changeset.change(%{amount_status: "voided"})
    |> Repo.update!()

    assert Reporting.token_totals_by_pool_ids(pools, started_at, ended_at) == %{}
    assert Reporting.settled_cost_totals_by_pool_ids(pools, started_at, ended_at) == %{}

    settlement
    |> Repo.reload!()
    |> Ecto.Changeset.change(%{amount_status: "recorded", total_tokens: 20})
    |> Repo.update!()

    assert Reporting.token_totals_by_pool_ids(pools, started_at, ended_at) == %{
             setup.pool.id => 20
           }

    assert Repo.aggregate(APIKeyUsageBucket, :count) == 0

    Repo.delete!(setup.pool)
    assert Repo.get(Request, finalized.request.id) == nil
    assert Repo.get(Attempt, attempt.id) == nil
    assert Repo.get(LedgerEntry, finalized.settlement.id) == nil
  end
end
