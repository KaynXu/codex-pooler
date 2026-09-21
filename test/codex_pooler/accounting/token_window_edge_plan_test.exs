defmodule CodexPooler.Accounting.TokenWindowEdgePlanTest do
  use CodexPooler.DataCase, async: false

  import CodexPooler.AccountingTestSupport

  alias CodexPooler.Accounting.RequestLifecycle.WindowUsage
  alias CodexPooler.TestDiagnostics

  setup tags do
    if tags[:statistics] == :empty do
      # Prime a physical page, then roll back its rows: unlike a pristine
      # zero-page table, this makes zero-row statistics underestimate bulk data.
      assert {:error, :primed} =
               Repo.transaction(fn ->
                 fixture = accounting_setup()

                 Repo.query!(
                   """
                   WITH request AS (
                     INSERT INTO requests(pool_id,api_key_id,requested_model,endpoint,transport,correlation_id,admitted_at)
                     VALUES ($1,$2,'synthetic-model','/v1/responses','http_json',gen_random_uuid()::text,$3)
                     RETURNING id,pool_id,api_key_id,admitted_at
                   )
                   INSERT INTO ledger_entries(pool_id,api_key_id,request_id,entry_kind,usage_status,total_tokens,request_count,occurred_at,transport)
                   SELECT pool_id,api_key_id,id,'reservation','usage_pending',512,1,admitted_at,'http_json' FROM request
                   """,
                   [
                     Ecto.UUID.dump!(fixture.pool.id),
                     Ecto.UUID.dump!(fixture.api_key.id),
                     ~U[2026-09-21 12:00:30.000000Z]
                   ]
                 )

                 Repo.rollback(:primed)
               end)

      analyze_tables()

      assert [[tuples, pages]] =
               Repo.query!(
                 "SELECT reltuples,relpages FROM pg_class WHERE oid='ledger_entries'::regclass"
               ).rows

      assert tuples == 0
      assert pages > 0
    end

    stats("before_seed")
    :ok
  end

  defp stats(stage) do
    if TestDiagnostics.enabled?() do
      rows =
        Repo.query!(
          "SELECT c.relname,c.reltuples,c.relpages,s.n_live_tup,s.n_dead_tup,s.n_mod_since_analyze,s.analyze_count,s.autoanalyze_count FROM pg_class c JOIN pg_stat_all_tables s ON s.relid=c.oid WHERE c.oid IN ('ledger_entries'::regclass,'api_key_usage_buckets'::regclass)"
        ).rows

      attributes =
        Repo.query!(
          "SELECT tablename,attname,null_frac,n_distinct,array_length(most_common_freqs,1) FROM pg_stats WHERE schemaname='public' AND tablename IN ('ledger_entries','api_key_usage_buckets') AND attname IN ('api_key_id','request_id','entry_kind','occurred_at','bucket_started_at') ORDER BY tablename,attname"
        ).rows

      TestDiagnostics.puts(
        CodexPooler.JSON.encode!(%{
          scenario: "table_stats",
          stage: stage,
          rows: rows,
          attributes: attributes
        })
      )
    end
  end

  for statistics <- [:fresh, :empty, :analyzed] do
    @tag statistics: statistics
    test "#{statistics} current-minute histories use a set projection with no per-request event function",
         %{statistics: statistics} do
      fixture = accounting_setup()
      at = ~U[2026-09-21 12:00:30.000000Z]
      key = Ecto.UUID.dump!(fixture.api_key.id)
      pool = Ecto.UUID.dump!(fixture.pool.id)

      Repo.query!(
        """
        INSERT INTO requests(pool_id,api_key_id,requested_model,endpoint,transport,correlation_id,admitted_at)
        SELECT $1,$2,'synthetic-model','/v1/responses','http_json','edge-plan-'||n,$3
        FROM generate_series(1,1000) n
        """,
        [pool, key, at]
      )

      for {kind, usage, tokens} <- [
            {"reservation", "usage_pending", 512},
            {"release", "usage_unknown", 512}
          ] do
        Repo.query!(
          """
          INSERT INTO ledger_entries(pool_id,api_key_id,request_id,entry_kind,usage_status,total_tokens,request_count,occurred_at,transport)
          SELECT pool_id,api_key_id,id,$1,$2,$3,1,admitted_at,'http_json'
          FROM requests WHERE api_key_id=$4
          """,
          [kind, usage, tokens, key]
        )
      end

      if statistics == :analyzed, do: analyze_tables()

      handler = "edge-plan-#{System.unique_integer([:positive])}"
      on_exit(fn -> :telemetry.detach(handler) end)

      :ok =
        :telemetry.attach(handler, [:codex_pooler, :repo, :query], &__MODULE__.capture/4, self())

      try do
        assert %{day: %{effective_request_count: 1000, effective_total_tokens: 0}} =
                 WindowUsage.window_usages(
                   fixture.api_key.id,
                   [day: DateTime.add(at, -86_400), minute: DateTime.add(at, -60)],
                   at
                 )

        assert_receive {:window_query, query, params}

        %{rows: [[[explain]]]} =
          Repo.query!("EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) " <> query, params)

        nodes = nodes(explain["Plan"])

        TestDiagnostics.puts(
          CodexPooler.JSON.encode!(%{
            scenario: "current_plan",
            statistics: statistics,
            plan: explain
          })
        )

        function_nodes = Enum.filter(nodes, &(&1["Function Name"] == "api_key_usage_events"))

        TestDiagnostics.puts(
          "edge_plan histories=1000 event_function_nodes=#{length(function_nodes)} execution_ms=#{explain["Execution Time"]}"
        )

        assert function_nodes == [],
               "edge projection must not invoke the event function once per retained request"

        comparisons = Enum.reduce(nodes, 0, &(&2 + Map.get(&1, "Rows Removed by Join Filter", 0)))
        TestDiagnostics.puts("edge_plan join_filter_comparisons=#{comparisons}")

        assert comparisons < 10_000,
               "fresh-table edge projection must not compare every terminal with every reservation"

        edge_history = Enum.find(nodes, &(&1["Subplan Name"] == "CTE edge_history"))

        assert edge_history["Actual Rows"] == 0,
               "fully included current-minute histories must use their additive bucket"
      after
        :telemetry.detach(handler)
      end
    end
  end

  def capture(_event, measurements, metadata, owner) do
    if self() == owner and String.starts_with?(metadata.query, "WITH bounds") do
      TestDiagnostics.puts(
        CodexPooler.JSON.encode!(%{scenario: "window_query_timing", measurements: measurements})
      )

      send(owner, {:window_query, metadata.query, metadata.params})
    end
  end

  for boundary <- [:none, :few], statistics <- [:fresh, :empty] do
    @tag boundary: boundary, statistics: statistics
    test "#{statistics} #{boundary} excluded boundaries stay bounded with retained finalized histories",
         %{
           boundary: boundary,
           statistics: statistics
         } do
      fixture = accounting_setup()
      other = accounting_setup()
      at = ~U[2026-09-21 12:00:30.000000Z]
      key = Ecto.UUID.dump!(fixture.api_key.id)

      for setup <- [fixture, other] do
        pool_id = Ecto.UUID.dump!(setup.pool.id)
        key_id = Ecto.UUID.dump!(setup.api_key.id)

        Repo.query!(
          """
          INSERT INTO requests(pool_id,api_key_id,requested_model,endpoint,transport,correlation_id,admitted_at)
          SELECT $1,$2,'synthetic-model','/v1/responses','http_json',gen_random_uuid()::text,$3
          FROM generate_series(1,10000) n
          """,
          [pool_id, key_id, at]
        )

        for kind <- ["reservation", "release"] do
          Repo.query!(
            """
            INSERT INTO ledger_entries(pool_id,api_key_id,request_id,entry_kind,usage_status,total_tokens,request_count,occurred_at,transport)
            SELECT pool_id,api_key_id,id,$1,'usage_pending',512,1,admitted_at,'http_json'
            FROM requests WHERE api_key_id=$2
            """,
            [kind, key_id]
          )
        end
      end

      handler = "retained-edge-plan-#{System.unique_integer([:positive])}"
      on_exit(fn -> :telemetry.detach(handler) end)

      :ok =
        :telemetry.attach(handler, [:codex_pooler, :repo, :query], &__MODULE__.capture/4, self())

      if boundary == :few, do: insert_boundary_releases(fixture, at)

      observations =
        try do
          stats("after_seed")

          for analyzed <- [false, true] do
            if analyzed do
              analyze_tables()
              stats("after_analyze")
            end

            usage =
              WindowUsage.window_usages(
                fixture.api_key.id,
                [
                  day: DateTime.add(at, -86_400),
                  minute: DateTime.add(at, -60),
                  same: DateTime.add(at, -10)
                ],
                at
              )

            assert usage.day.effective_request_count ==
                     if(boundary == :few, do: 10_001, else: 10_000)

            assert usage.minute.effective_request_count == 10_000
            assert usage.same.effective_request_count == 10_000
            assert usage.day.effective_total_tokens == 0
            assert Enum.all?(usage, fn {_window, values} -> values.pending_total_tokens == 0 end)
            assert_receive {:window_query, query, params}

            %{rows: [[[plan]]]} =
              Repo.query!("EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) " <> query, params)

            history = Enum.find(nodes(plan["Plan"]), &(&1["Subplan Name"] == "CTE edge_history"))
            pending = Enum.find(nodes(plan["Plan"]), &(&1["Subplan Name"] == "CTE pending"))
            pending_nodes = nodes(pending)
            comparisons = join_comparisons(pending_nodes)

            terminal_scans =
              pending_nodes
              |> Enum.filter(&(&1["Relation Name"] == "ledger_entries" and &1["Alias"] != "r"))
              |> scanned_rows()

            assert comparisons < 10_000,
                   "pending lookup must not compare every terminal with every reservation"

            assert terminal_scans <= 20_006,
                   "pending terminal lookup must remain linear in retained histories"

            scanned =
              history
              |> nodes()
              |> Enum.filter(&(&1["Relation Name"] == "ledger_entries"))
              |> Enum.map(
                &((&1["Actual Rows"] + Map.get(&1, "Rows Removed by Filter", 0)) *
                    &1["Actual Loops"])
              )
              |> Enum.sum()

            TestDiagnostics.puts(
              CodexPooler.JSON.encode!(%{
                scenario: "retained_edge_discovery",
                boundary: boundary,
                statistics: statistics,
                analyzed: analyzed,
                histories_per_key: 10_000,
                keys: 2,
                scanned_edge_rows: scanned,
                edge_history_rows: history["Actual Rows"],
                pending_join_comparisons: comparisons,
                pending_terminal_scans: terminal_scans,
                query_sha256: Base.encode16(:crypto.hash(:sha256, query), case: :lower),
                plan: plan
              })
            )

            {boundary, analyzed, scanned, history["Actual Rows"]}
          end
        after
          :telemetry.detach(handler)
        end

      assert Enum.all?(observations, fn {boundary, _analyzed, scanned, histories} ->
               scanned <= 50 and histories == if(boundary == :few, do: 6, else: 0)
             end),
             "excluded edges must not scan retained key history: #{inspect(observations)}"

      [[ledger_count]] =
        Repo.query!("SELECT count(*) FROM ledger_entries WHERE api_key_id=$1", [key]).rows

      assert ledger_count == if(boundary == :few, do: 20_006, else: 20_000)
    end
  end

  defp analyze_tables do
    Repo.query!("ANALYZE ledger_entries")
    Repo.query!("ANALYZE api_key_usage_buckets")
  end

  defp join_comparisons(nodes) do
    Enum.reduce(
      nodes,
      0,
      &(&2 + Map.get(&1, "Rows Removed by Join Filter", 0) * &1["Actual Loops"])
    )
  end

  defp scanned_rows(nodes) do
    Enum.reduce(nodes, 0, fn node, total ->
      total +
        (node["Actual Rows"] + Map.get(node, "Rows Removed by Filter", 0)) * node["Actual Loops"]
    end)
  end

  defp insert_boundary_releases(fixture, at) do
    key = Ecto.UUID.dump!(fixture.api_key.id)

    Repo.query!(
      """
      INSERT INTO requests(pool_id,api_key_id,requested_model,endpoint,transport,correlation_id,admitted_at)
      SELECT $1,$2,'synthetic-model','/v1/responses','http_json','bounded-extra-'||gen_random_uuid(),stamp
      FROM unnest($3::timestamptz[]) stamp
      """,
      [Ecto.UUID.dump!(fixture.pool.id), key, Enum.map([-86_410, -65, 10], &DateTime.add(at, &1))]
    )

    for kind <- ["reservation", "release"] do
      Repo.query!(
        """
        INSERT INTO ledger_entries(pool_id,api_key_id,request_id,entry_kind,usage_status,total_tokens,request_count,occurred_at,transport)
        SELECT pool_id,api_key_id,id,$1,'usage_pending',512,1,admitted_at,'http_json'
        FROM requests WHERE api_key_id=$2 AND correlation_id LIKE 'bounded-extra-%'
        """,
        [kind, key]
      )
    end
  end

  test "boundary subtraction matches event authority across corrections and both partial edges" do
    fixture = accounting_setup()
    origin = ~U[2026-09-20 23:59:00.000000Z]
    key = Ecto.UUID.dump!(fixture.api_key.id)
    pool = Ecto.UUID.dump!(fixture.pool.id)

    for {usage, tokens, terminal_second} <- [
          {"usage_known", 0, 0},
          {"not_applicable", 0, 15},
          {"usage_unknown", 512, 30},
          {"usage_known", 4096, 45},
          {"usage_unknown", 512, 60},
          {"usage_known", 8192, 75}
        ] do
      [[request]] =
        Repo.query!(
          """
          INSERT INTO requests(pool_id,api_key_id,requested_model,endpoint,transport,correlation_id,admitted_at)
          VALUES ($1,$2,'synthetic-model','/v1/responses','http_json',$3,$4) RETURNING id
          """,
          [pool, key, Ecto.UUID.generate(), origin]
        ).rows

      for {kind, status, amount, offset, value} <- [
            {"reservation", "usage_pending", "recorded", -30, 512},
            {"settlement", usage, "voided", terminal_second, tokens},
            {"release", usage, "recorded", terminal_second, 512},
            {"settlement", usage, "recorded", terminal_second + 120, tokens}
          ] do
        Repo.query!(
          """
          INSERT INTO ledger_entries(pool_id,api_key_id,request_id,entry_kind,usage_status,
            amount_status,total_tokens,request_count,occurred_at,transport,details)
          VALUES ($1,$2,$3,$4,$5,$6,$7,1,$8,'http_json','{"estimated_from_reserve":true}')
          """,
          [pool, key, request, kind, status, amount, value, DateTime.add(origin, offset)]
        )
      end
    end

    for {start, finish} <- [{0, 60}, {15, 45}, {30, 75}, {45, 60}, {-604_800, 75}, {60, 60}] do
      since = DateTime.add(origin, start)
      as_of = DateTime.add(origin, finish)

      [[known, provisional, admissions, cost]] =
        Repo.query!(
          """
          SELECT COALESCE(SUM(v.known_total_tokens),0)::bigint,
            COALESCE(SUM(v.provisional_total_tokens),0)::bigint,
            COALESCE(SUM(v.admission_count),0)::bigint, COALESCE(SUM(v.known_cost_micros),0)
          FROM (SELECT array_agg(e) AS entries FROM ledger_entries e WHERE api_key_id=$1 GROUP BY request_id) h
          CROSS JOIN LATERAL public.api_key_usage_events(h.entries) v
          WHERE v.occurred_at >= $2 AND v.occurred_at <= $3
          """,
          [key, since, as_of]
        ).rows

      actual = WindowUsage.window_usages(fixture.api_key.id, [window: since], as_of).window
      assert actual.known_total_tokens == known
      assert actual.provisional_total_tokens == provisional
      assert actual.effective_request_count == admissions
      assert Decimal.equal?(actual.effective_cost_micros, cost)
      assert actual.pending_total_tokens == 0
    end
  end

  defp nodes(plan), do: [plan | Enum.flat_map(Map.get(plan, "Plans", []), &nodes/1)]
end
