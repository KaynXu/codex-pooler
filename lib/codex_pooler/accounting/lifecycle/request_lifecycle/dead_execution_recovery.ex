defmodule CodexPooler.Accounting.RequestLifecycle.DeadExecutionRecovery do
  @moduledoc false
  import Ecto.Query

  alias CodexPooler.Accounting.{
    Attempt,
    LedgerEntry,
    Request,
    RequestLifecycle,
    RequestReplayEntitlement
  }

  alias CodexPooler.Platform.ExecutionIdentity
  alias CodexPooler.Repo

  @spec recover(DateTime.t(), keyword()) :: {:ok, map()} | {:error, term()}
  def recover(now, opts \\ []) do
    cutoff = DateTime.add(now, -Keyword.get(opts, :minimum_age_seconds, 120), :second)
    limit = Keyword.get(opts, :limit, 100)

    cutoff
    |> candidates(limit)
    |> Enum.reduce_while(
      {:ok, %{dead_execution_attempts_recovered: 0}},
      &recover_candidate(&1, &2, now)
    )
  end

  defp candidates(cutoff, limit) do
    # Keep eligibility checks correlated: flattening them into joins makes Postgres
    # scan and sort every open attempt before applying the batch limit.
    eligible =
      from a in Attempt,
        as: :attempt,
        where:
          a.status in ["queued", "in_progress"] and not is_nil(a.owner_execution_id) and
            a.started_at <= ^cutoff,
        where:
          fragment(
            "? IS TRUE",
            exists(
              from request in Request,
                where:
                  request.id == parent_as(:attempt).request_id and
                    request.status in ["accepted", "in_progress"],
                select: 1
            )
          ),
        where:
          fragment(
            "? IS TRUE",
            exists(
              from entry in LedgerEntry,
                where:
                  entry.request_id == parent_as(:attempt).request_id and
                    entry.entry_kind == "reservation" and entry.amount_status == "recorded",
                select: 1
            )
          ),
        where:
          not fragment(
            "? IS TRUE",
            exists(
              from entry in LedgerEntry,
                where:
                  entry.request_id == parent_as(:attempt).request_id and
                    entry.entry_kind == "release",
                select: 1
            )
          ),
        where:
          not fragment(
            "? IS TRUE",
            exists(
              from replay in RequestReplayEntitlement,
                where: replay.request_id == parent_as(:attempt).request_id,
                select: 1
            )
          ),
        order_by: [
          asc: fragment("COALESCE(?, ?)", a.owner_execution_checked_at, a.started_at),
          asc: a.id
        ],
        limit: ^limit

    Repo.all(
      from a in subquery(eligible),
        join: r in Request,
        on: r.id == a.request_id,
        order_by: [
          asc: fragment("COALESCE(?, ?)", a.owner_execution_checked_at, a.started_at),
          asc: a.id
        ],
        select: {r, a}
    )
  end

  defp recover_candidate({request, attempt}, {:ok, summary}, now) do
    # Persist scheduling progress so another replica or a later job advances
    # past active executions instead of repeatedly selecting the oldest batch.
    from(a in Attempt,
      where: a.id == ^attempt.id and a.owner_execution_id == ^attempt.owner_execution_id
    )
    |> Repo.update_all(set: [owner_execution_checked_at: now])

    result =
      if ExecutionIdentity.status(attempt) == :dead,
        do: RequestLifecycle.recover_dead_execution(request, attempt, now),
        else: {:ok, :noop}

    case result do
      {:ok, :recovered} ->
        {:cont,
         {:ok,
          %{dead_execution_attempts_recovered: summary.dead_execution_attempts_recovered + 1}}}

      {:ok, :noop} ->
        {:cont, {:ok, summary}}

      {:error, _} = error ->
        {:halt, error}
    end
  end
end
