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

  alias CodexPooler.Gateway.Runtime.Finalization.InterruptionOutcome
  alias CodexPooler.Platform.ExecutionTerminalProofs
  alias CodexPooler.Repo

  @type summary :: %{dead_execution_attempts_recovered: non_neg_integer()}
  @spec recover(DateTime.t(), keyword()) :: {:ok, summary()} | {:error, term(), summary()}
  def recover(now, opts \\ []) do
    cutoff = DateTime.add(now, -Keyword.get(opts, :minimum_age_seconds, 120), :second)
    limit = Keyword.get(opts, :limit, 100)

    {summary, failures} =
      cutoff
      |> candidates(limit)
      |> Enum.reduce(
        {%{dead_execution_attempts_recovered: 0}, []},
        &recover_candidate(&1, &2, now)
      )

    if failures == [],
      do: {:ok, summary},
      else: {:error, {:dead_execution_candidates_failed, Enum.reverse(failures)}, summary}
  end

  defp candidates(cutoff, limit) do
    # Keep eligibility checks correlated: flattening them into joins makes Postgres
    # scan and sort every open attempt before applying the batch limit.
    eligible =
      from a in Attempt,
        as: :attempt,
        where:
          a.status in ["queued", "in_progress"] and a.replay_generation == 0 and
            not is_nil(a.owner_execution_id) and
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

  defp recover_candidate({request, attempt}, {summary, failures}, now) do
    case recover_candidate(request, attempt, now) do
      {:ok, :recovered} ->
        {%{
           summary
           | dead_execution_attempts_recovered: summary.dead_execution_attempts_recovered + 1
         }, failures}

      {:ok, :noop} ->
        {summary, failures}

      {:error, reason} ->
        {summary, [{attempt.id, reason} | failures]}
    end
  end

  defp recover_candidate(request, attempt, now) do
    # Persist scheduling progress so another replica or a later job advances
    # past active executions instead of repeatedly selecting the oldest batch.
    from(a in Attempt,
      where: a.id == ^attempt.id and a.owner_execution_id == ^attempt.owner_execution_id
    )
    |> Repo.update_all(set: [owner_execution_checked_at: now])

    caller_owned_transaction? = Repo.in_transaction?()

    result =
      if ExecutionTerminalProofs.terminal?(attempt),
        do: RequestLifecycle.recover_dead_execution(request, attempt, now),
        else: {:ok, :noop}

    case result do
      {:ok, :recovered} ->
        unless caller_owned_transaction? do
          InterruptionOutcome.emit(
            bounded_transport(request.transport),
            bounded_transport(attempt.transport)
          )
        end

        result

      other ->
        other
    end
  rescue
    exception -> {:error, exception.__struct__}
  catch
    :exit, _reason -> {:error, :execution_recovery_unavailable}
  end

  defp bounded_transport(transport) when transport in ["http_sse", "websocket"], do: transport
  defp bounded_transport(_), do: "unknown"
end
