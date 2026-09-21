defmodule CodexPooler.Verification.BudgetRehearsal do
  @moduledoc false

  alias CodexPooler.Accounting.RequestLifecycle.WindowUsage
  alias CodexPooler.Repo

  @components 20_260_920_200_313
  @cap 20_260_920_200_550
  @since ~U[2026-01-02 00:00:00Z]
  @as_of ~U[2026-01-02 00:05:00Z]
  @old ~U[2026-01-01 23:59:00Z]
  @barrier 23_508

  @spec run(String.t(), pos_integer(), map()) :: :ok
  def run(scenario, rows, migration) do
    migration.migrate.(@components - 1)
    fixture = seed(rows)
    run_scenario(scenario, fixture, migration)
    :ok
  end

  defp run_scenario("budget_upgrade", fixture, migration) do
    before = retained_snapshot()
    [[legacy_tokens]] = legacy_window(fixture.key)
    true = Decimal.equal?(legacy_tokens, 2560)
    timed("budget_component_upgrade", fn -> migration.migrate.(:all) end)
    assert_current(fixture)
    assert_schema(:current)
    ^before = retained_snapshot()
    [] = migration.migrate.(:all)
    assert_rebuild()

    # A mixed release can execute the exact legacy SQL unchanged. Caps remain
    # unset until every admission node has switched to the current reader.
    [[nil]] = query("SELECT max_active_requests FROM api_keys WHERE id=$1", [fixture.key]).rows
    true = legacy_window(fixture.key) == [[legacy_tokens]]
    query("UPDATE api_keys SET max_active_requests=2 WHERE id=$1", [fixture.key])

    for cycle <- 1..2 do
      before_down = retained_snapshot()
      migration.down.(@cap)
      migration.down.(@components)
      assert_schema(:legacy)
      ^before_down = retained_snapshot()
      true = legacy_window(fixture.key) == [[legacy_tokens]]
      migration.migrate.(:all)
      assert_schema(:current)
      assert_current(fixture)
      [[nil]] = query("SELECT max_active_requests FROM api_keys WHERE id=$1", [fixture.key]).rows
      assert_rebuild()
      receipt("budget_up_down_up", %{cycle: cycle, retained_rows_and_legacy_values: true})
    end

    receipt("budget_upgrade", %{
      known: 4096,
      provisional: 512,
      pending: 512,
      effective: 5120,
      legacy_window_tokens: Decimal.to_integer(legacy_tokens),
      null_key_history: 1,
      legacy_reader_compatible: true,
      caps_unset_before_cutover: true,
      old_beam_executed: false,
      rollback_restores_weaker_semantics: true
    })
  end

  defp run_scenario("budget_locks", fixture, migration) do
    before = retained_snapshot()

    for relation <- ~w(ledger_entries api_key_usage_buckets) do
      with_connection(fn blocker ->
        held = hold_lock(blocker, "LOCK TABLE #{relation} IN ROW EXCLUSIVE MODE")
        started = System.monotonic_time(:millisecond)
        {:error, "55P03"} = migrate_result(migration, @components)
        elapsed = System.monotonic_time(:millisecond) - started
        true = elapsed < 5000
        assert_schema(:legacy)
        ^before = retained_snapshot()
        release(held)

        assert_relations_released()

        receipt("budget_nowait", %{
          relation: relation,
          elapsed_ms: elapsed,
          error: "55P03",
          atomic: true
        })
      end)
    end

    with_connection(fn blocker ->
      install_barrier()
      Postgrex.query!(blocker, "SELECT pg_advisory_lock($1)", [@barrier])
      task = migrate_async(migration, @components)
      pid = await_barrier()
      capture_locks("cancel_before_publish", [pid])
      [[true]] = query("SELECT pg_cancel_backend($1)", [pid]).rows
      {:error, "57014"} = Task.await(task, 15_000)
      Postgrex.query!(blocker, "SELECT pg_advisory_unlock($1)", [@barrier])
      remove_barrier()
      assert_schema(:legacy)
      ^before = retained_snapshot()
      receipt("budget_cancel", %{error: "57014", migration_backend: pid, atomic: true})
    end)

    migration.migrate.(@components)
    assert_current(fixture)

    assert_cap_lock(fn -> migration.migrate.(@cap) end, :up)

    migration.migrate.(:all)
    assert_current(fixture)
    assert_schema(:current)
    assert_cap_lock(fn -> migration.down.(@cap) end, :down)
    assert_schema(:current)

    with_connection(fn blocker ->
      held = hold_lock(blocker, "LOCK TABLE api_key_usage_buckets IN ROW EXCLUSIVE MODE")
      snapshot = component_snapshot()

      {:error, %Postgrex.Error{postgres: %{pg_code: "55P03"}}} =
        Repo.query("SELECT rebuild_api_key_usage_components()", [], log: false)

      ^snapshot = component_snapshot()
      release(held)
      assert_rebuild()
      receipt("budget_rebuild_lock", %{error: "55P03", old_projection_usable: true})
    end)
  end

  defp run_scenario("budget_traffic", fixture, migration) do
    with_connection(fn blocker ->
      install_barrier()
      Postgrex.query!(blocker, "SELECT pg_advisory_lock($1)", [@barrier])
      task = migrate_async(migration, @components)
      migration_pid = await_barrier()
      traffic = start_traffic(fixture)
      writer_pid = await_blocked("ledger_entries")
      reader = Task.async(&read_totals/0)
      reader_pid = await_blocked("api_key_usage_buckets")
      capture_locks("migration_traffic", [migration_pid, writer_pid, reader_pid])
      # This query uses only columns available to the old release.
      [[1]] =
        query("SELECT count(*) FROM api_keys WHERE id=$1 AND status='active'", [fixture.key]).rows

      Postgrex.query!(blocker, "SELECT pg_advisory_unlock($1)", [@barrier])
      {:ok, _} = Task.await(task, 15_000)
      finish_traffic(traffic)
      assert_totals(Task.await(reader, 15_000), fixture, 0..4)
      remove_barrier()
    end)

    with_connection(fn blocker ->
      held = hold_lock(blocker, "UPDATE api_keys SET status='active'")
      task = migrate_async(migration, @cap)
      pid = await_blocked("api_keys")
      capture_locks("cap_policy_traffic", [pid, held.backend])
      release(held)
      {:ok, _} = Task.await(task, 15_000)
      [[0]] = query("SELECT count(*) FROM api_keys WHERE max_active_requests IS NOT NULL").rows

      receipt("budget_cap_policy_traffic", %{
        policy_committed: true,
        migration_committed: true,
        caps_unset: true
      })
    end)

    assert_rebuild()

    with_connection(fn blocker ->
      # The rebuild has completed its writes but has not committed. Readers and
      # writers must wait and then see the whole committed projection.
      held = hold_lock(blocker, "SELECT rebuild_api_key_usage_components()")
      traffic = start_traffic(fixture)
      writer_pid = await_blocked("ledger_entries")
      reader = Task.async(&read_totals/0)
      reader_pid = await_blocked("api_key_usage_buckets")
      capture_locks("rebuild_traffic", [held.backend, writer_pid, reader_pid])
      release(held)
      finish_traffic(traffic)
      assert_totals(Task.await(reader, 15_000), fixture, 4..8)
    end)

    assert_rebuild()
    assert_totals(read_totals(), fixture, 8..8)
    [[1]] = query("SELECT count(*) FROM ledger_entries WHERE api_key_id IS NULL").rows

    receipt("budget_traffic", %{
      committed_lifecycles: 8,
      policy_writes: 2,
      deadlocks: 0,
      unexpected_lock_timeouts: 0,
      rebuild_equal: true
    })
  end

  defp seed(rows) do
    [[pool]] =
      query(
        "INSERT INTO pools(slug,name,status) VALUES ('synthetic-budget-upgrade','Synthetic budget upgrade','active') RETURNING id"
      ).rows

    keys =
      for n <- 1..3 do
        [[key]] =
          query(
            "INSERT INTO api_keys(pool_id,display_name,key_prefix,key_hash) VALUES ($1,$2,$2,$3) RETURNING id",
            [pool, "synthetic-budget-#{n}", :crypto.hash(:sha256, "synthetic-budget-#{n}")]
          ).rows

        key
      end

    [key, history_key, deleted_key] = keys
    fixture = %{pool: pool, key: key, history_key: history_key, retained: rows}

    for kind <- [:known, :unknown, :pending, :no_charge] do
      request = request(fixture, key)
      entry(fixture, key, request, "reservation", "usage_pending", 512, @old)

      case kind do
        :known ->
          entry(fixture, key, request, "settlement", "usage_known", 4096, @since)
          entry(fixture, key, request, "release", "usage_known", 512, @since)

        :unknown ->
          entry(fixture, key, request, "settlement", "usage_unknown", 512, @since)
          entry(fixture, key, request, "release", "usage_unknown", 512, @since)

        :no_charge ->
          entry(fixture, key, request, "release", "not_applicable", 512, @since)

        :pending ->
          :ok
      end
    end

    for _ <- 1..rows do
      request = request(fixture, history_key)
      entry(fixture, history_key, request, "reservation", "usage_pending", 512, @old)
      entry(fixture, history_key, request, "settlement", "usage_known", 1024, @since)
      entry(fixture, history_key, request, "release", "usage_known", 512, @since)
    end

    request = request(fixture, deleted_key)
    entry(fixture, deleted_key, request, "reservation", "usage_pending", 512, @old)
    query("DELETE FROM api_keys WHERE id=$1", [deleted_key])
    [[1]] = query("SELECT count(*) FROM ledger_entries WHERE api_key_id IS NULL").rows

    receipt("budget_seed", %{
      retained_requests: rows + 5,
      retained_ledger: rows * 3 + 10,
      null_key_rows: 1
    })

    fixture
  end

  defp request(fixture, key) do
    [[id]] =
      query(
        "INSERT INTO requests(pool_id,api_key_id,requested_model,endpoint,transport,correlation_id) VALUES ($1,$2,'synthetic-model','/v1/responses','http_json',$3) RETURNING id",
        [fixture.pool, key, Ecto.UUID.generate()]
      ).rows

    id
  end

  defp entry(fixture, key, request, kind, status, tokens, at) do
    [[id]] =
      query(
        """
        INSERT INTO ledger_entries(pool_id,api_key_id,request_id,entry_kind,usage_status,total_tokens,
          request_count,occurred_at,transport,details,settled_cost_micros)
        VALUES ($1,$2,$3,$4,$5,$6,1,$7,'http_json','{"estimated_from_reserve":true}',7) RETURNING id
        """,
        [fixture.pool, key, request, kind, status, tokens, at]
      ).rows

    id
  end

  defp assert_current(fixture) do
    %{window: usage, all: all} =
      WindowUsage.window_usages(Ecto.UUID.load!(fixture.key), [window: @since, all: @old], @as_of)

    %{
      known_total_tokens: 4096,
      provisional_total_tokens: 512,
      pending_total_tokens: 512,
      effective_total_tokens: 5120,
      effective_request_count: 0
    } = usage

    true = Decimal.equal?(usage.effective_cost_micros, 7)
    4 = all.effective_request_count
  end

  defp legacy_window(key) do
    query(
      "SELECT sum(effective_total_tokens) FROM api_key_usage_buckets WHERE api_key_id=$1 AND bucket_started_at >= $2 AND bucket_started_at <= $3",
      [key, @since, @as_of]
    ).rows
  end

  defp retained_snapshot do
    for {table, exclusions} <- [
          {"ledger_entries", "'{}'::text[]"},
          {"requests", "'{}'::text[]"},
          {"api_keys", "ARRAY['max_active_requests']"},
          {"api_key_usage_buckets",
           "ARRAY['known_total_tokens','provisional_total_tokens','admission_count','known_cost_micros','updated_at']"}
        ] do
      query(
        "SELECT count(*), bit_xor(hashtextextended((to_jsonb(t)-#{exclusions})::text,0))::text FROM #{table} t"
      ).rows
    end
  end

  defp component_snapshot do
    query(
      "SELECT api_key_id::text,bucket_started_at,known_total_tokens,provisional_total_tokens,admission_count,known_cost_micros FROM api_key_usage_buckets ORDER BY api_key_id,bucket_started_at"
    ).rows
  end

  defp assert_relations_released do
    {:ok, _} =
      Repo.transaction(fn ->
        query("LOCK TABLE ledger_entries, api_key_usage_buckets IN ACCESS EXCLUSIVE MODE NOWAIT")
      end)
  end

  defp assert_rebuild do
    snapshot = component_snapshot()

    for _ <- 1..2 do
      query("SELECT rebuild_api_key_usage_components()")
      ^snapshot = component_snapshot()
    end
  end

  defp assert_schema(mode) do
    expected = if mode == :current, do: [4, 3, 3, 1, 1, 5, 3], else: [0, 0, 0, 0, 0, 0, 0]

    [^expected] =
      query("""
      SELECT
        (SELECT count(*) FROM information_schema.columns WHERE table_name='api_key_usage_buckets' AND column_name IN ('known_total_tokens','provisional_total_tokens','admission_count','known_cost_micros')),
        (SELECT count(*) FROM pg_index WHERE indisvalid AND indexrelid IN (to_regclass('ledger_entries_reservation_key_occurred_idx'),to_regclass('ledger_entries_terminal_request_idx'),to_regclass('ledger_entries_key_occurred_idx'))),
        (SELECT count(*) FROM pg_trigger WHERE tgrelid='ledger_entries'::regclass AND tgname LIKE 'ledger_entries_usage_components_%'),
        (SELECT count(*) FROM information_schema.columns WHERE table_name='api_keys' AND column_name='max_active_requests' AND is_nullable='YES' AND column_default IS NULL),
        (SELECT count(*) FROM pg_constraint WHERE conname='api_keys_max_active_requests_positive'),
        (SELECT count(*) FROM pg_proc WHERE pronamespace='public'::regnamespace AND proname IN ('api_key_usage_events','rebuild_api_key_usage_components','sync_api_key_usage_components_insert','sync_api_key_usage_components_update','sync_api_key_usage_components_delete')),
        (SELECT count(*) FROM pg_class WHERE relnamespace='public'::regnamespace AND relname IN ('ledger_entries_reservation_key_occurred_idx','ledger_entries_terminal_request_idx','ledger_entries_key_occurred_idx'))
      """).rows

    [[legacy]] =
      query(
        "SELECT count(*) FROM pg_trigger WHERE tgrelid='ledger_entries'::regclass AND tgname='ledger_entries_sync_api_key_usage_buckets'"
      ).rows

    true = legacy == if(mode == :current, do: 0, else: 1)
  end

  defp start_traffic(fixture) do
    writer = Task.async(fn -> write_traffic(fixture) end)

    policy =
      Task.async(fn ->
        query("UPDATE api_keys SET status='paused' WHERE id=$1", [
          fixture.history_key
        ])

        :ok
      end)

    [writer, policy]
  end

  defp write_traffic(fixture) do
    elapsed =
      for n <- 1..4 do
        key = if rem(n, 2) == 0, do: fixture.history_key, else: fixture.key
        started = System.monotonic_time(:microsecond)
        {:ok, :ok} = Repo.transaction(fn -> write_lifecycle(fixture, key) end)
        System.monotonic_time(:microsecond) - started
      end

    receipt("budget_traffic_transactions", %{committed: 4, transaction_microseconds: elapsed})

    :ok
  end

  defp write_lifecycle(fixture, key) do
    query("SET LOCAL lock_timeout='10s'")
    request = request(fixture, key)
    entry(fixture, key, request, "reservation", "usage_pending", 512, @old)
    terminal = entry(fixture, key, request, "settlement", "usage_unknown", 512, @since)
    entry(fixture, key, request, "release", "usage_unknown", 512, @since)
    query("UPDATE ledger_entries SET amount_status='voided' WHERE id=$1", [terminal])
    entry(fixture, key, request, "settlement", "usage_known", 2048, @since)
    :ok
  end

  defp read_totals do
    [[known, provisional, admissions, cost]] =
      query(
        "SELECT sum(known_total_tokens)::bigint,sum(provisional_total_tokens)::bigint,sum(admission_count)::bigint,sum(known_cost_micros) FROM api_key_usage_buckets"
      ).rows

    {known, provisional, admissions, cost}
  end

  defp assert_totals({known, provisional, admissions, cost}, fixture, completed_range) do
    completed = admissions - fixture.retained - 4
    true = completed in completed_range
    true = known == 4096 + fixture.retained * 1024 + completed * 2048
    512 = provisional
    true = Decimal.equal?(cost, 7 * (fixture.retained + 1 + completed))

    receipt("budget_atomic_reader", %{
      completed_lifecycles: completed,
      known: known,
      provisional: provisional,
      admissions: admissions,
      no_partial_projection: true
    })
  end

  defp assert_cap_lock(fun, direction) do
    with_connection(fn blocker -> cap_lock(blocker, fun, direction) end)
  end

  defp cap_lock(blocker, fun, direction) do
    snapshot = retained_snapshot()
    held = hold_lock(blocker, "LOCK TABLE api_keys IN ROW SHARE MODE")
    started = System.monotonic_time(:millisecond)
    task = Task.async(fn -> migration_result(fun) end)
    pid = await_blocked("api_keys")
    capture_locks("cap_migration_lock", [pid, held.backend])
    # Exercise the migration's real five-second lock budget. The detector only
    # cancels an unbounded regression; it never supplies the product timeout.
    result =
      case Task.yield(task, 7000) do
        {:ok, result} ->
          result

        nil ->
          query("SELECT pg_cancel_backend($1)", [pid])
          Task.await(task, 15_000)
          {:error, :unbounded_cap_lock}
      end

    elapsed = System.monotonic_time(:millisecond) - started
    release(held)
    {:error, "55P03"} = result
    true = elapsed < 7000
    ^snapshot = retained_snapshot()
    expected = if direction == :up, do: 0, else: 1

    [[^expected, ^expected]] =
      query(
        "SELECT (SELECT count(*) FROM information_schema.columns WHERE table_name='api_keys' AND column_name='max_active_requests'),(SELECT count(*) FROM pg_constraint WHERE conname='api_keys_max_active_requests_positive')"
      ).rows

    {:ok, _} =
      Repo.transaction(fn -> query("LOCK TABLE api_keys IN ACCESS EXCLUSIVE MODE NOWAIT") end)

    receipt("budget_cap_lock", %{
      direction: direction,
      error: "55P03",
      elapsed_ms: elapsed,
      no_partial_schema: true,
      relation_lock_released: true
    })
  end

  defp migrate_async(migration, version),
    do: Task.async(fn -> migrate_result(migration, version) end)

  defp migrate_result(migration, version),
    do: migration_result(fn -> migration.migrate.(version) end)

  defp timed(stage, fun) do
    started = System.monotonic_time(:microsecond)
    result = fun.()
    receipt(stage, %{elapsed_microseconds: System.monotonic_time(:microsecond) - started})
    result
  end

  defp finish_traffic(tasks), do: Enum.each(tasks, fn task -> :ok = Task.await(task, 15_000) end)

  defp hold_lock(connection, sql) do
    parent = self()
    ref = make_ref()

    task =
      Task.async(fn ->
        {:ok, :ok} =
          Postgrex.transaction(
            connection,
            fn conn ->
              Postgrex.query!(conn, sql, [])
              [[backend]] = Postgrex.query!(conn, "SELECT pg_backend_pid()", []).rows
              send(parent, {ref, backend})

              receive do
                {^ref, :release} -> :ok
              after
                15_000 -> raise "owned database lock was not released"
              end
            end,
            timeout: 20_000
          )

        :ok
      end)

    receive do
      {^ref, backend} -> %{task: task, ref: ref, backend: backend}
    after
      15_000 -> raise "owned lock not acquired"
    end
  end

  defp release(held) do
    send(held.task.pid, {held.ref, :release})
    :ok = Task.await(held.task, 15_000)
  end

  defp with_connection(fun) do
    {:ok, conn} =
      Postgrex.start_link(
        Keyword.take(Repo.config(), [:hostname, :port, :username, :password, :database]) ++
          [ssl: false]
      )

    try do
      fun.(conn)
    after
      GenServer.stop(conn)
    end
  end

  defp migration_result(fun) do
    {:ok, fun.()}
  rescue
    e in Postgrex.Error -> {:error, e.postgres.pg_code}
  end

  defp install_barrier do
    query("""
    CREATE FUNCTION verification_budget_pause() RETURNS event_trigger LANGUAGE plpgsql AS $$
    BEGIN
      IF current_query() LIKE 'ALTER TABLE%api_key_usage_buckets%' THEN
        PERFORM pg_advisory_xact_lock(23508);
      END IF;
    END $$
    """)

    query(
      "CREATE EVENT TRIGGER verification_budget_pause ON ddl_command_end WHEN TAG IN ('ALTER TABLE') EXECUTE FUNCTION verification_budget_pause()"
    )
  end

  defp remove_barrier do
    query("DROP EVENT TRIGGER verification_budget_pause")
    query("DROP FUNCTION verification_budget_pause()")
  end

  defp await_barrier do
    await(fn ->
      query(
        "SELECT pid FROM pg_stat_activity WHERE datname=current_database() AND wait_event='advisory' AND query LIKE 'ALTER TABLE%api_key_usage_buckets%'"
      ).rows
    end)
  end

  defp await_blocked(relation) do
    await(fn ->
      query(
        "SELECT pid FROM pg_locks WHERE relation=$1::text::regclass AND NOT granted ORDER BY pid LIMIT 1",
        [relation]
      ).rows
    end)
  end

  defp await(fun), do: await(fun, System.monotonic_time(:millisecond) + 15_000)

  defp await(fun, deadline) do
    case fun.() do
      [[pid]] ->
        pid

      _ ->
        if System.monotonic_time(:millisecond) >= deadline,
          do: raise("database barrier timed out")

        receive do
        after
          10 -> await(fun, deadline)
        end
    end
  end

  defp capture_locks(stage, pids) do
    locks =
      query(
        "SELECT pid,locktype,coalesce(relation::regclass::text,''),mode,granted,pg_blocking_pids(pid) FROM pg_locks WHERE pid=ANY($1) ORDER BY pid,locktype,mode",
        [pids]
      ).rows

    true = Enum.any?(locks, fn [_, _, _, _, granted, _] -> not granted end)
    receipt(stage, %{backends: pids, locks: locks})
  end

  defp query(sql, params \\ []), do: Repo.query!(sql, params, log: false, timeout: 20_000)

  defp receipt(stage, fields),
    do: IO.puts(CodexPooler.JSON.encode!(Map.put(fields, :stage, stage)))
end
