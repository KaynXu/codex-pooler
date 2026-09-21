defmodule CodexPooler.Gateway.Routing.QuotaRefresh.PlanTest do
  use ExUnit.Case, async: true

  alias CodexPooler.Gateway.Payloads.RequestOptions
  alias CodexPooler.Gateway.Persistence.CodexSession
  alias CodexPooler.Gateway.Routing.CandidateEligibility.FilterInput
  alias CodexPooler.Gateway.Routing.QuotaRefresh.Plan
  alias CodexPooler.Upstreams.Schemas.PoolUpstreamAssignment

  test "refresh is bounded to two candidates and preserves normal ordering" do
    candidates = candidates()
    plan = plan(candidates, RequestOptions.build(%{}, "/backend-api/codex/responses", %{}))
    assert Plan.refresh_candidates(plan) == Enum.take(candidates, 2)
  end

  test "the session assignment is refreshed first even outside the normal refresh budget" do
    candidates = candidates()
    {pinned, _identity} = List.last(candidates)

    options =
      RequestOptions.build(%{}, "/backend-api/codex/responses", %{})
      |> RequestOptions.put_continuity(codex_session: %CodexSession{pool_upstream_assignment_id: pinned.id})

    assert Plan.refresh_candidates(plan(candidates, options)) ==
             [List.last(candidates), hd(candidates)]
  end

  test "an absent session assignment does not reorder or admit candidates" do
    candidates = candidates()

    options =
      RequestOptions.build(%{}, "/backend-api/codex/responses", %{})
      |> RequestOptions.put_continuity(codex_session: %CodexSession{pool_upstream_assignment_id: Ecto.UUID.generate()})

    assert Plan.refresh_candidates(plan(candidates, options)) == Enum.take(candidates, 2)
    assert Plan.refresh_candidates(plan([], options)) == []
  end

  defp candidates do
    for _ <- 1..3, do: {%PoolUpstreamAssignment{id: Ecto.UUID.generate()}, nil}
  end

  defp plan(candidates, options) do
    %{filter_input: %FilterInput{request_options: options}, refreshable_candidates: candidates}
  end
end
