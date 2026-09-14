defmodule CodexPooler.Accounting.DeadExecutionCandidateQueryTest do
  use CodexPooler.DataCase, async: false

  import CodexPooler.AccountingTestSupport

  alias CodexPooler.Accounting.RequestLifecycle.DeadExecutionRecovery
  alias Ecto.Adapters.SQL

  test "candidate scan stops after the eligible batch and advances past ineligible rows" do
    setup = accounting_setup()
    seed_candidates(setup)
    handler = {__MODULE__, make_ref()}
    on_exit(fn -> :telemetry.detach(handler) end)

    :ok =
      :telemetry.attach(handler, [:codex_pooler, :repo, :query], &__MODULE__.capture/4, self())

    now = DateTime.utc_now()

    assert {:ok, %{dead_execution_attempts_recovered: 0}} =
             DeadExecutionRecovery.recover(now)

    assert_receive {:candidate_query, sql, params}
    :telemetry.detach(handler)

    # The first 300 open executions fail three independent eligibility checks.
    # They must not consume the result limit or starve the eligible suffix.
    assert %{rows: [[301, 400, 100]]} =
             query!(
               """
               SELECT min((r.request_metadata->>'fixture_ordinal')::int),
                      max((r.request_metadata->>'fixture_ordinal')::int),count(*)
               FROM requests r JOIN attempts a ON a.request_id=r.id
               WHERE a.owner_execution_checked_at IS NOT NULL
               """,
               []
             )

    # Explain the actual emitted production SQL, including its bound LIMIT.
    # Restoring the scheduler timestamp makes the plan use the original ordering.
    query!("UPDATE attempts SET owner_execution_checked_at=NULL", [])

    %{rows: [[[%{"Plan" => plan}]]]} =
      query!("EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) " <> sql, params)

    nodes = plan_nodes(plan)
    assert plan["Actual Rows"] == 100

    for node <- nodes, node["Node Type"] in ["Sort", "Incremental Sort"] do
      assert hd(node["Plans"])["Actual Rows"] <= 100
    end

    attempt_scan = Enum.find(nodes, &(&1["Index Name"] == "attempts_open_execution_index"))
    assert attempt_scan
    assert attempt_scan["Actual Rows"] + attempt_scan["Rows Removed by Filter"] == 400
    assert attempt_scan["Actual Loops"] == 1
    assert plan["Shared Hit Blocks"] + plan["Shared Read Blocks"] < 12_000
  end

  test "age, identity, reservation state, and replay exclusions retain the eligible controls" do
    setup = accounting_setup()
    now = DateTime.utc_now()

    attempts =
      for kind <- [:eligible, :accepted, :queued, :young, :legacy, :voided], into: %{} do
        request =
          CodexPooler.PoolerFixtures.request_fixture(setup.auth, %{
            model_id: setup.model.id,
            status: if(kind == :accepted, do: "accepted", else: "in_progress")
          })

        attempt =
          CodexPooler.PoolerFixtures.attempt_fixture(request, setup.assignment, %{
            status: if(kind == :queued, do: "queued", else: "in_progress")
          })
          |> Ecto.Changeset.change(%{
            started_at: if(kind == :young, do: now, else: DateTime.add(now, -300)),
            owner_execution_id: if(kind == :legacy, do: nil, else: Ecto.UUID.generate())
          })
          |> Repo.update!()

        CodexPooler.PoolerFixtures.ledger_entry_fixture(request, %{
          entry_kind: "reservation",
          amount_status: if(kind == :voided, do: "voided", else: "recorded")
        })

        {kind, attempt}
      end

    replay = CodexPooler.RequestReplayFixtures.replay_fixture(reservation?: true)
    CodexPooler.RequestReplayFixtures.insert_entitlement!(replay, %{})

    replay_attempt =
      CodexPooler.PoolerFixtures.attempt_fixture(replay.request, replay.assignment, %{
        attempt_number: 2,
        status: "in_progress"
      })
      |> Ecto.Changeset.change(%{
        started_at: DateTime.add(now, -300),
        owner_execution_id: Ecto.UUID.generate()
      })
      |> Repo.update!()

    assert {:ok, %{dead_execution_attempts_recovered: 0}} = DeadExecutionRecovery.recover(now)

    for {kind, attempt} <- attempts do
      assert not is_nil(Repo.reload!(attempt).owner_execution_checked_at) ==
               kind in [:eligible, :accepted, :queued]
    end

    assert is_nil(Repo.reload!(replay_attempt).owner_execution_checked_at)
  end

  @doc false
  def capture(_event, _measurements, metadata, parent) do
    if self() == parent and String.contains?(metadata.query, "owner_execution_checked_at") and
         String.starts_with?(metadata.query, "SELECT") do
      send(parent, {:candidate_query, metadata.query, metadata.params})
    end
  end

  defp query!(sql, params), do: SQL.query!(Repo, sql, params, timeout: 60_000)

  defp plan_nodes(plan), do: [plan | Enum.flat_map(Map.get(plan, "Plans", []), &plan_nodes/1)]

  defp seed_candidates(setup) do
    query!(
      """
      INSERT INTO requests(pool_id,api_key_id,model_id,requested_model,endpoint,transport,status,usage_status,correlation_id,request_metadata)
      SELECT $1,$2,$3,'synthetic-model','/backend-api/codex/responses','websocket',
      CASE WHEN n BETWEEN 201 AND 300 OR n>1000 THEN 'succeeded' ELSE 'in_progress' END,
      'usage_unknown','candidate-'||n,jsonb_build_object('fixture_ordinal',n)
      FROM generate_series(1,10000) n
      """,
      [
        Ecto.UUID.dump!(setup.pool.id),
        Ecto.UUID.dump!(setup.api_key.id),
        Ecto.UUID.dump!(setup.model.id)
      ]
    )

    query!(
      """
      INSERT INTO attempts(request_id,attempt_number,pool_upstream_assignment_id,model_id,upstream_model_id,transport,status,usage_status,started_at,owner_execution_id)
      SELECT id,1,$1,model_id,'synthetic-model','websocket',
      CASE WHEN (request_metadata->>'fixture_ordinal')::int<=1000 THEN 'in_progress' ELSE 'succeeded' END,
      'usage_unknown',now()-interval '1 day'+(request_metadata->>'fixture_ordinal')::int*interval '1 second',gen_random_uuid()
      FROM requests WHERE api_key_id=$2
      """,
      [Ecto.UUID.dump!(setup.assignment.id), Ecto.UUID.dump!(setup.api_key.id)]
    )

    query!(
      """
      INSERT INTO ledger_entries(pool_id,api_key_id,request_id,entry_kind,amount_status,currency_code,transport)
      SELECT pool_id,api_key_id,id,'reservation','recorded','USD','websocket'
      FROM requests WHERE api_key_id=$1 AND (request_metadata->>'fixture_ordinal')::int BETWEEN 101 AND 1000
      """,
      [Ecto.UUID.dump!(setup.api_key.id)]
    )

    query!(
      """
      INSERT INTO ledger_entries(pool_id,api_key_id,request_id,entry_kind,amount_status,currency_code,transport)
      SELECT pool_id,api_key_id,id,'release','recorded','USD','websocket'
      FROM requests WHERE api_key_id=$1 AND (request_metadata->>'fixture_ordinal')::int BETWEEN 101 AND 200
      """,
      [Ecto.UUID.dump!(setup.api_key.id)]
    )

    # A repeated reservation fact must not duplicate a candidate or consume
    # another slot in the scanner batch.
    query!(
      """
      INSERT INTO ledger_entries(pool_id,api_key_id,request_id,entry_kind,amount_status,currency_code,transport)
      SELECT pool_id,api_key_id,id,'reservation','recorded','USD','websocket'
      FROM requests WHERE api_key_id=$1 AND (request_metadata->>'fixture_ordinal')::int=301
      """,
      [Ecto.UUID.dump!(setup.api_key.id)]
    )

    for table <- ["attempts", "requests", "ledger_entries", "request_replay_entitlements"] do
      query!("ANALYZE " <> table, [])
    end
  end
end
