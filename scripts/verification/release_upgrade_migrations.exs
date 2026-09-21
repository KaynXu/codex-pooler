Code.require_file("test/support/release_upgrade_migrations/budget_rehearsal.exs")

defmodule CodexPooler.Verification.ReleaseUpgradeMigrations do
  @moduledoc """
  Owned PostgreSQL release migration rehearsal. Run with MIX_ENV=test,
  MIX_TEST_PARTITION=1 and a fresh CODEX_POOLER_TEST_RUN_NAMESPACE:

      mise x -- mix run --no-start scripts/verification/release_upgrade_migrations.exs --scenario widths

  Scenarios: widths, fresh, head, invalid, invalid_owner, client_exit,
  locks, historical_indexes, validation, null_history, migration_lock,
  scale, index_conflicts, rollback_cancel, rollback_delete, budget_upgrade,
  budget_locks, budget_traffic, budget_online, budget_indexes, budget_missing, budget_plan. Optional --rows controls the
  synthetic request count. The database must not exist and is always dropped.
  """
  alias CodexPooler.Repo
  alias CodexPooler.Verification.BudgetRehearsal
  alias Ecto.Adapters.Postgres
  alias Ecto.Migrator

  @baseline 20_260_909_103_936
  @head 20_260_914_124_456
  @pinned_head "7946bbea078d1b72d889ea9775c5f4bb94a94dea"
  @migrations "priv/repo/migrations"
  @client_output_limit 4096

  @spec run([String.t()]) :: :ok
  def run(["--help"]), do: IO.puts(@moduledoc)

  def run(["--client-exit-builder", application_name, name, columns, predicate]) do
    _config = safe_config!()
    validate_client_exit_target!(application_name, name, columns, predicate)
    {:ok, _} = Application.ensure_all_started(:postgrex)

    options =
      connection_options()
      |> Keyword.put(:parameters, application_name: application_name)

    {:ok, builder} = Postgrex.start_link(options)

    Postgrex.query!(
      builder,
      "CREATE INDEX CONCURRENTLY #{name} ON attempts (#{columns}) WHERE #{predicate}",
      [],
      timeout: :infinity
    )
  end

  def run(args) do
    {opts, [], []} = OptionParser.parse(args, strict: [scenario: :string, rows: :integer])
    scenario = Keyword.fetch!(opts, :scenario)
    rows = Keyword.get(opts, :rows, 100)

    unless scenario in ~w(widths fresh head invalid invalid_owner client_exit locks historical_indexes validation null_history migration_lock scale index_conflicts rollback_cancel rollback_delete budget_upgrade budget_locks budget_traffic budget_online budget_indexes budget_missing budget_plan) and
             rows in 1..1_000_000,
           do: raise(ArgumentError, "invalid scenario or row count")

    config = safe_config!()
    database = Keyword.fetch!(config, :database)

    {:ok, _} = Application.ensure_all_started(:ecto_sql)
    {:ok, _} = Application.ensure_all_started(:postgrex)
    Process.flag(:trap_exit, true)
    Logger.configure(level: :warning)

    receipt("ownership", %{
      database: database,
      scenario: scenario,
      cleanup: "drop exact owned database"
    })

    :ok = Postgres.storage_up(Keyword.put(config, :timeout, 60_000))
    previous_config = Application.fetch_env(:codex_pooler, Repo)

    try do
      # This process owns the complete disposable database. A normal pool avoids
      # SQL Sandbox's per-owner lifetime budget expiring during large seed scans.
      Application.put_env(
        :codex_pooler,
        Repo,
        Keyword.put(config, :pool, DBConnection.ConnectionPool)
      )

      {:ok, :ok, _} =
        Migrator.with_repo(
          Repo,
          fn _ ->
            run_scenario(scenario, rows)
            :ok
          end,
          pool_size: 8
        )
    after
      case previous_config do
        {:ok, value} -> Application.put_env(:codex_pooler, Repo, value)
        :error -> Application.delete_env(:codex_pooler, Repo)
      end

      :ok = Postgres.storage_down(Keyword.merge(config, force_drop: true, timeout: 60_000))
      :down = Postgres.storage_status(config)
      receipt("cleanup", %{database_dropped: true, database: database})
    end

    :ok
  end

  defp safe_config! do
    config = Repo.config()
    database = Keyword.fetch!(config, :database)

    unless Mix.env() == :test and is_nil(Process.whereis(Repo)) and is_nil(config[:url]) and
             permitted_host?(Keyword.fetch!(config, :hostname)) and
             Regex.match?(~r/\Acodex_pooler_test_[0-9a-f]{8}_[0-9a-f]{16}_p1\z/, database),
           do:
             raise(
               ArgumentError,
               "requires an owned namespaced database on an explicit test host and --no-start"
             )

    config
  end

  defp permitted_host?(hostname) do
    hostname in ["localhost", "127.0.0.1"] or
      (is_binary(hostname) and hostname != "" and
         hostname == System.get_env("CODEX_POOLER_TEST_POSTGRES_HOST"))
  end

  defp run_scenario("widths", _rows) do
    migrate(@baseline)
    migrate(20_260_910_230_227)
    query("INSERT INTO openai_status_feed_states(singleton,etag) VALUES (true,repeat('x',300))")

    try do
      down(20_260_910_230_227)
      raise "rollback unexpectedly truncated an oversized etag"
    rescue
      e in Postgrex.Error -> :string_data_right_truncation = e.postgres.code
    end

    [[300]] = query("SELECT length(etag) FROM openai_status_feed_states").rows
    query("DELETE FROM openai_status_feed_states")
    down(20_260_910_230_227)

    [[11]] =
      query("""
      SELECT count(*) FROM information_schema.columns WHERE table_schema='public'
        AND ((table_name='openai_status_feed_states' AND column_name IN ('etag','last_modified','last_error_code','content_hash'))
        OR (table_name='openai_status_incidents' AND column_name IN ('guid','title','status','summary','component','link','content_hash')))
        AND character_maximum_length=255
      """).rows

    migrate(20_260_910_230_227)
    receipt("widths", %{rollback_widths: 255, restored_columns: 11, up_down_up: true})
  end

  defp run_scenario(scenario, rows)
       when scenario in ~w(budget_upgrade budget_locks budget_traffic budget_online budget_indexes budget_missing budget_plan) do
    BudgetRehearsal.run(scenario, rows, %{
      migrate: &migrate/1,
      down: &down/1
    })
  end

  defp run_scenario("fresh", rows) do
    migrate(@baseline)
    seed(rows)
    before = data_snapshot()
    migrate(:all)
    ^before = data_snapshot()
    assert_final_schema()
    receipt("fresh", %{rows_preserved: rows, schema: schema_snapshot()})
    # Reverse dependency order: indexes before columns, history checks before FK reversal.
    for {version, _} <-
          migrations() |> Enum.filter(fn {v, _} -> v > @baseline end) |> Enum.reverse(),
        do: down(version)

    ^before = data_snapshot()
    migrate(:all)
    ^before = data_snapshot()
    assert_final_schema()
    receipt("down_up", %{data_preserved: true, schema_valid: true})
  end

  defp run_scenario("scale", _rows) do
    Process.put({__MODULE__, :query_timeout}, 900_000)
    migrate(@baseline)
    {seed_us, _} = :timer.tc(fn -> seed(968_000, 809_000, 2_270_000) end)

    [[968_000, 809_000, 2_270_000]] =
      query(
        "SELECT (SELECT count(*) FROM requests),(SELECT count(*) FROM attempts),(SELECT count(*) FROM ledger_entries)"
      ).rows

    receipt("scale_seed", %{
      elapsed_ms: div(seed_us, 1000),
      request_rows: 968_000,
      attempt_rows: 809_000,
      ledger_rows: 2_270_000
    })

    before = data_snapshot()
    {ddl_us, _} = :timer.tc(fn -> migrate(@head - 1) end)
    ^before = data_snapshot()
    qualify_attempts()
    qualified = data_snapshot()
    {index_us, _} = :timer.tc(fn -> migrate(:all) end)
    ^qualified = data_snapshot()
    query("ANALYZE attempts")

    [[true], [true]] =
      query(
        "SELECT reltuples>0 FROM pg_class WHERE relname IN ('attempts_open_execution_index','attempts_open_owner_incarnation_idx') ORDER BY relname"
      ).rows

    [[809_000, 809_000]] =
      query(
        "SELECT count(*) FILTER (WHERE status IN ('queued','in_progress') AND owner_execution_id IS NOT NULL),count(*) FILTER (WHERE status IN ('queued','in_progress') AND owner_instance_boot_id IS NOT NULL) FROM attempts"
      ).rows

    assert_final_schema()

    receipt("scale", %{
      request_rows: 968_000,
      attempt_rows: 809_000,
      ledger_rows: 2_270_000,
      seed_ms: div(seed_us, 1000),
      upgrade_ms: div(ddl_us + index_us, 1000),
      additive_ddl_ms: div(ddl_us, 1000),
      concurrent_index_ms: div(index_us, 1000),
      rows_in_each_partial_index: 809_000,
      data_preserved: true,
      schema: schema_snapshot(),
      database_bytes: query("SELECT pg_database_size(current_database())").rows
    })
  end

  defp run_scenario("head", rows) do
    directory =
      Path.join(System.tmp_dir!(), "release-migrations-#{System.unique_integer([:positive])}")

    File.mkdir!(directory)

    try do
      # Frozen affected sources keep this baseline independent of the rewritten
      # migrations and runnable in shallow release/CI checkouts.
      for {version, file} <- migrations(), version <= @head do
        frozen = Path.join("test/support/release_upgrade_migrations", Path.basename(file))

        File.cp!(
          if(File.exists?(frozen), do: frozen, else: file),
          Path.join(directory, Path.basename(file))
        )
      end

      Migrator.run(Repo, directory, :up, all: true, log: false)
      seed(rows)
      before = head_data_snapshot()
      migrate(:all)
      ^before = head_data_snapshot()
      assert_final_schema()
      current_schema = schema_snapshot()
      current_physical = physical_snapshot()

      receipt("head", %{
        review_head_data_unchanged: true,
        pinned_revision: @pinned_head,
        upgraded_schema: current_schema
      })

      down(20_260_914_195_100)
      ^before = head_data_snapshot()
      ^current_physical = physical_snapshot()
      migrate(:all)
      ^current_physical = physical_snapshot()
      receipt("isolated_convergence_down", %{current_schema_physical_state_preserved: true})

      for {version, _} <-
            migrations() |> Enum.filter(fn {v, _} -> v > @baseline end) |> Enum.reverse(),
          do: down(version)

      migrate(:all)
      ^current_schema = schema_snapshot()
      receipt("schema_equivalence", %{rewritten_upgrade_reconverges_current_schema: true})
    after
      File.rm_rf!(directory)
    end
  end

  defp run_scenario(scenario, rows)
       when scenario in ["invalid", "invalid_owner", "client_exit"] do
    {version, name, columns, predicate} = index_target(scenario)
    migrate(version - 1)
    seed(rows)
    qualify_attempts()
    # A writer blocks CREATE INDEX CONCURRENTLY after it has committed its INVALID
    # catalog entry. Server cancellation and hard client exit exercise distinct
    # real interruption paths before the migration repairs the resulting state.
    {:ok, blocker} = Postgrex.start_link(connection_options())

    try do
      Postgrex.query!(blocker, "BEGIN", [])
      Postgrex.query!(blocker, "UPDATE attempts SET status=status", [])
      builder = start_index_builder(scenario, name, columns, predicate)

      try do
        {pid, task} = begin_index_build(builder, name, columns, predicate)

        await(fn ->
          query(
            "SELECT count(*) FROM pg_index WHERE indexrelid=to_regclass('#{name}') AND NOT indisvalid"
          ).rows == [[1]]
        end)

        interrupt_index(scenario, builder, pid, task, name)
        Postgrex.query!(blocker, "ROLLBACK", [])

        await(fn ->
          query("SELECT count(*) FROM pg_stat_activity WHERE pid=$1 AND state='active'", [pid]).rows ==
            [[0]]
        end)

        receipt("interrupted_index", %{
          name: name,
          real_cancel: scenario != "client_exit",
          catalog:
            query(
              "SELECT indisvalid,indisready FROM pg_index WHERE indexrelid='#{name}'::regclass"
            ).rows
        })

        migrate(:all)

        [[true]] =
          query(
            "SELECT indisvalid AND indisready FROM pg_index WHERE indexrelid='#{name}'::regclass"
          ).rows

        query("ANALYZE attempts")

        [[^rows]] =
          query("SELECT reltuples::bigint FROM pg_class WHERE oid='#{name}'::regclass").rows

        receipt("invalid_retry", %{name: name, valid: true, index_rows: rows})
      after
        stop_index_builder(builder)
      end
    after
      GenServer.stop(blocker)
    end
  end

  defp run_scenario("index_conflicts", _rows) do
    for {version, name} <- [
          {@head, "attempts_open_execution_index"},
          {20_260_914_195_100, "attempts_open_owner_incarnation_idx"}
        ] do
      migrate(version - 1)
      query("CREATE INDEX #{name} ON attempts(id)")
      before = physical_snapshot()

      try do
        migrate(version)
        raise "migration accepted a conflicting same-name index"
      rescue
        e in RuntimeError ->
          unless String.contains?(e.message, "conflicting index"), do: reraise(e, __STACKTRACE__)
      end

      ^before = physical_snapshot()
      query("DROP INDEX #{name}")
      migrate(version)
      receipt("index_conflict", %{name: name, rejected_without_mutation: true})
    end
  end

  defp run_scenario("locks", rows) do
    migrate(20_260_913_190_000 - 1)
    seed(rows)
    {:ok, blocker} = Postgrex.start_link(connection_options())

    try do
      Postgrex.query!(blocker, "BEGIN", [])
      Postgrex.query!(blocker, "LOCK TABLE requests IN ACCESS SHARE MODE", [])

      task =
        Task.async(fn ->
          try do
            migrate(20_260_913_190_000)
            :unexpected_success
          rescue
            e in Postgrex.Error -> e.postgres.code
          end
        end)

      result =
        case Task.yield(task, 15_000) do
          {:ok, result} ->
            result

          nil ->
            query("""
            SELECT pg_cancel_backend(pid) FROM pg_stat_activity
            WHERE datname=current_database() AND pid<>pg_backend_pid()
              AND wait_event_type='Lock' AND query LIKE '%ALTER TABLE public.requests%'
            """)

            Task.await(task, 5_000)
        end

      receipt("lock_result", %{result: result})
      :lock_not_available = result
      Postgrex.query!(blocker, "ROLLBACK", [])
      migrate(:all)
      receipt("lock_budget", %{error: "lock_not_available", retry_succeeded: true})
    after
      GenServer.stop(blocker)
    end
  end

  defp run_scenario("historical_indexes", rows) do
    migrate(@baseline)
    seed(rows)
    {:ok, blocker} = Postgrex.start_link(connection_options())
    {:ok, writer} = Postgrex.start_link(connection_options())

    try do
      query("""
      CREATE FUNCTION pause_historical_index() RETURNS event_trigger LANGUAGE plpgsql AS $$
      BEGIN
        IF current_query() LIKE '%attempts_open_owner_instance_idx%' THEN
          PERFORM pg_advisory_xact_lock(20901);
        END IF;
      END $$
      """)

      query(
        "CREATE EVENT TRIGGER pause_historical_index ON ddl_command_end WHEN TAG IN ('CREATE INDEX') EXECUTE FUNCTION pause_historical_index()"
      )

      Postgrex.query!(blocker, "SELECT pg_advisory_lock(20901)", [])
      task = Task.async(fn -> migrate(20_260_912_012_006) end)

      await(fn ->
        query(
          "SELECT count(*) FROM pg_stat_activity WHERE datname=current_database() AND wait_event='advisory' AND query LIKE '%attempts_open_owner_instance_idx%'"
        ).rows == [[1]] or
          query("SELECT count(*) FROM schema_migrations WHERE version=20260912012006").rows == [
            [1]
          ]
      end)

      if query("SELECT count(*) FROM schema_migrations WHERE version=20260912012006").rows == [
           [0]
         ] do
        Postgrex.query!(writer, "SET statement_timeout='500ms'", [])

        {:error, %Postgrex.Error{postgres: %{code: :query_canceled}}} =
          Postgrex.query(writer, "UPDATE attempts SET status=status", [])

        receipt("historical_writer", %{blocked_during_index_build: true})
      end

      Postgrex.query!(blocker, "SELECT pg_advisory_unlock(20901)", [])
      Task.await(task, 20_000)
      query("DROP EVENT TRIGGER pause_historical_index")
      query("DROP FUNCTION pause_historical_index()")
    after
      GenServer.stop(blocker)
      GenServer.stop(writer)
    end

    [[nil]] = query("SELECT to_regclass('attempts_open_owner_instance_idx')::text").rows
    migrate(20_260_912_023_423)
    [[nil]] = query("SELECT to_regclass('attempts_open_owner_incarnation_idx')::text").rows
    migrate(:all)
    assert_final_schema()
    receipt("historical_indexes", %{deferred_until_concurrent_migration: true})
  end

  defp run_scenario("null_history", rows) do
    migrate(:all)
    seed(rows)
    query("UPDATE ledger_entries SET api_key_id=NULL")
    before = physical_snapshot()

    try do
      down(20_260_913_220_612)
      raise "rollback accepted null-key ledger history"
    rescue
      e in Postgrex.Error -> :not_null_violation = e.postgres.code
    end

    ^before = physical_snapshot()

    receipt("null_history", %{rollback_refused_before_mutation: true, ledger_rows_preserved: rows})
  end

  defp run_scenario("rollback_cancel", rows) do
    migrate(:all)
    seed(rows)

    query("""
    CREATE FUNCTION pause_rollback_validation() RETURNS event_trigger LANGUAGE plpgsql AS $$
    BEGIN
      IF current_query() LIKE '%VALIDATE CONSTRAINT%' THEN
        PERFORM pg_advisory_xact_lock(20903);
      END IF;
    END $$
    """)

    query(
      "CREATE EVENT TRIGGER pause_rollback_validation ON ddl_command_end WHEN TAG IN ('ALTER TABLE') EXECUTE FUNCTION pause_rollback_validation()"
    )

    {:ok, blocker} = Postgrex.start_link(connection_options())

    try do
      Postgrex.query!(blocker, "SELECT pg_advisory_lock(20903)", [])

      task =
        Task.async(fn ->
          try do
            down(20_260_913_220_612)
          rescue
            e in Postgrex.Error -> e.postgres.code
          end
        end)

      await(fn ->
        query(
          "SELECT count(*) FROM pg_stat_activity WHERE datname=current_database() AND wait_event='advisory' AND query LIKE '%VALIDATE CONSTRAINT%'"
        ).rows == [[1]]
      end)

      [[true]] =
        query(
          "SELECT pg_cancel_backend(pid) FROM pg_stat_activity WHERE datname=current_database() AND wait_event='advisory' AND query LIKE '%VALIDATE CONSTRAINT%'"
        ).rows

      :query_canceled = Task.await(task, 20_000)
      Postgrex.query!(blocker, "SELECT pg_advisory_unlock(20903)", [])
      # A failed rollback cannot leave a helper constraint rejecting normal key deletion.
      query("DELETE FROM api_keys")

      [[0]] =
        query(
          "SELECT count(*) FROM pg_constraint WHERE conname='ledger_entries_restore_api_key_required'"
        ).rows

      receipt("rollback_cancel", %{normal_delete_succeeded: true, no_helper_constraint: true})
    after
      GenServer.stop(blocker)
    end

    query("DROP EVENT TRIGGER pause_rollback_validation")
    query("DROP FUNCTION pause_rollback_validation()")
    down(20_260_913_220_612)
    migrate(:all)
  end

  defp run_scenario("rollback_delete", rows) do
    migrate(:all)
    seed(rows)

    query("""
    CREATE FUNCTION pause_rollback_switch() RETURNS event_trigger LANGUAGE plpgsql AS $$
    BEGIN
      IF current_query() LIKE '%ADD CONSTRAINT ledger_entries_api_key_id_not_null%' THEN
        PERFORM pg_advisory_xact_lock(20904);
      ELSIF current_query() LIKE '%VALIDATE CONSTRAINT%' THEN
        PERFORM pg_advisory_xact_lock(20905);
      END IF;
    END $$
    """)

    query(
      "CREATE EVENT TRIGGER pause_rollback_switch ON ddl_command_start WHEN TAG IN ('ALTER TABLE') EXECUTE FUNCTION pause_rollback_switch()"
    )

    {:ok, blocker} = Postgrex.start_link(connection_options())
    {:ok, deleter} = Postgrex.start_link(connection_options())

    try do
      Postgrex.query!(blocker, "SELECT pg_advisory_lock(20904),pg_advisory_lock(20905)", [])

      task =
        Task.async(fn ->
          try do
            down(20_260_913_220_612)
          rescue
            e in Postgrex.Error -> e.postgres.code
          end
        end)

      await(fn ->
        query(
          "SELECT count(*) FROM pg_stat_activity WHERE datname=current_database() AND wait_event='advisory' AND query LIKE '%ADD CONSTRAINT ledger_entries_api_key_id_not_null%'"
        ).rows == [[1]]
      end)

      [[migration_pid]] =
        query(
          "SELECT pid FROM pg_stat_activity WHERE datname=current_database() AND wait_event='advisory' AND query LIKE '%ADD CONSTRAINT ledger_entries_api_key_id_not_null%'"
        ).rows

      [[false]] =
        query(
          "SELECT attnotnull FROM pg_attribute WHERE attrelid='ledger_entries'::regclass AND attname='api_key_id'"
        ).rows

      [[delete_pid]] = Postgrex.query!(deleter, "SELECT pg_backend_pid()", []).rows

      deletion =
        Task.async(fn ->
          Postgrex.query!(deleter, "DELETE FROM api_keys", [], timeout: 30_000)
        end)

      await(fn ->
        query("SELECT $1=ANY(pg_blocking_pids($2))", [migration_pid, delete_pid]).rows == [[true]]
      end)

      Postgrex.query!(blocker, "SELECT pg_advisory_unlock(20904)", [])
      %{num_rows: 1} = Task.await(deletion, 30_000)

      await(fn ->
        query(
          "SELECT count(*) FROM pg_stat_activity WHERE datname=current_database() AND wait_event='advisory' AND query LIKE '%VALIDATE CONSTRAINT%'"
        ).rows == [[1]]
      end)

      [[true, "c", 0]] =
        query("""
        SELECT (SELECT attnotnull FROM pg_attribute WHERE attrelid='ledger_entries'::regclass AND attname='api_key_id'),
          (SELECT confdeltype::text FROM pg_constraint WHERE conname='ledger_entries_api_key_id_fkey'),
          (SELECT count(*) FROM ledger_entries)
        """).rows

      [[true]] =
        query(
          "SELECT pg_cancel_backend(pid) FROM pg_stat_activity WHERE datname=current_database() AND wait_event='advisory' AND query LIKE '%VALIDATE CONSTRAINT%'"
        ).rows

      :query_canceled = Task.await(task, 20_000)
      Postgrex.query!(blocker, "SELECT pg_advisory_unlock(20905)", [])
      down(20_260_913_220_612)

      [[2]] =
        query(
          "SELECT count(*) FROM pg_constraint WHERE conname IN ('ledger_entries_api_key_id_not_null','ledger_entries_api_key_id_fkey') AND convalidated"
        ).rows

      receipt("rollback_delete", %{
        delete_waited_for_atomic_switch: true,
        delete_committed: true,
        null_row_slip: false,
        old_cascade_semantics: true,
        cancelled_validation_retry_succeeded: true
      })
    after
      GenServer.stop(blocker)
      GenServer.stop(deleter)
    end

    query("DROP EVENT TRIGGER pause_rollback_switch")
    query("DROP FUNCTION pause_rollback_switch()")
    migrate(:all)
  end

  defp run_scenario("migration_lock", _rows) do
    migrate(@baseline)
    {:ok, blocker} = Postgrex.start_link(connection_options())

    try do
      key = :erlang.phash2({:ecto, nil, Repo})
      Postgrex.query!(blocker, "SELECT pg_advisory_lock($1)", [key])

      task =
        Task.async(fn ->
          try do
            migrate(:all)
            :unexpected_success
          rescue
            e in RuntimeError -> e.message
          end
        end)

      "failed to obtain advisory lock. Tried 10 times waiting 1000ms between tries" =
        Task.await(task, 20_000)

      [[0]] = query("SELECT count(*) FROM schema_migrations WHERE version>$1", [@baseline]).rows
      Postgrex.query!(blocker, "SELECT pg_advisory_unlock($1)", [key])
      migrate(:all)

      receipt("migration_lock", %{
        bounded_refusal: true,
        concurrent_runner_excluded: true,
        retry_succeeded: true
      })
    after
      GenServer.stop(blocker)
    end
  end

  defp run_scenario("validation", rows) do
    migrate(20_260_913_190_000 - 1)
    seed(rows)
    # ddl_command_end runs after PostgreSQL has scanned the FK, while that
    # transaction still retains its locks. This makes the lock proof deterministic.
    query("""
    CREATE FUNCTION pause_fk_validation() RETURNS event_trigger LANGUAGE plpgsql AS $$
    BEGIN
      IF current_query() LIKE '%VALIDATE CONSTRAINT%' THEN
        PERFORM pg_advisory_xact_lock(20902);
      END IF;
    END $$
    """)

    query(
      "CREATE EVENT TRIGGER pause_fk_validation ON ddl_command_end WHEN TAG IN ('ALTER TABLE') EXECUTE FUNCTION pause_fk_validation()"
    )

    {:ok, blocker} = Postgrex.start_link(connection_options())
    {:ok, writer} = Postgrex.start_link(connection_options())

    try do
      for {version, table} <- [
            {20_260_913_190_000, "requests"},
            {20_260_913_220_612, "ledger_entries"}
          ] do
        Postgrex.query!(blocker, "SELECT pg_advisory_lock(20902)", [])

        task =
          Task.async(fn ->
            try do
              migrate(version)
              :unexpected_success
            rescue
              e in Postgrex.Error -> e.postgres.code
            end
          end)

        await(fn ->
          query(
            "SELECT count(*) FROM pg_stat_activity WHERE datname=current_database() AND wait_event='advisory' AND query LIKE '%VALIDATE CONSTRAINT%'"
          ).rows == [[1]]
        end)

        [[false, true]] =
          query(
            """
            SELECT bool_or(l.mode='AccessExclusiveLock'),bool_or(l.mode='ShareUpdateExclusiveLock')
            FROM pg_locks l JOIN pg_stat_activity a USING(pid)
            WHERE a.datname=current_database() AND a.wait_event='advisory'
              AND a.query LIKE '%VALIDATE CONSTRAINT%' AND l.relation=$1::text::regclass
            """,
            [table]
          ).rows

        Postgrex.query!(writer, "SET statement_timeout='1s'", [])
        Postgrex.query!(writer, "UPDATE #{table} SET api_key_id=api_key_id", [])

        [[true]] =
          query("""
          SELECT pg_cancel_backend(pid) FROM pg_stat_activity
          WHERE datname=current_database() AND wait_event='advisory' AND query LIKE '%VALIDATE CONSTRAINT%'
          """).rows

        :query_canceled = Task.await(task, 20_000)

        [[false, "n"]] =
          query("SELECT convalidated,confdeltype::text FROM pg_constraint WHERE conname=$1", [
            table <> "_api_key_id_fkey"
          ]).rows

        [[0]] = query("SELECT count(*) FROM schema_migrations WHERE version=$1", [version]).rows
        Postgrex.query!(blocker, "SELECT pg_advisory_unlock(20902)", [])
        migrate(version)

        [[true]] =
          query("SELECT convalidated FROM pg_constraint WHERE conname=$1", [
            table <> "_api_key_id_fkey"
          ]).rows

        receipt("validation", %{
          table: table,
          exclusive_lock: false,
          validation_lock: true,
          writer_committed: true,
          interrupted_not_valid_swap_preserved: true,
          retry_validated: true
        })
      end
    after
      GenServer.stop(blocker)
      GenServer.stop(writer)
    end

    query("DROP EVENT TRIGGER pause_fk_validation")
    query("DROP FUNCTION pause_fk_validation()")
  end

  defp index_target("invalid"),
    do:
      {@head, "attempts_open_execution_index",
       "COALESCE(owner_execution_checked_at, started_at), id",
       "status IN ('queued', 'in_progress') AND owner_execution_id IS NOT NULL"}

  defp index_target(_),
    do:
      {20_260_914_195_100, "attempts_open_owner_incarnation_idx",
       "owner_instance_id, owner_instance_boot_id, started_at",
       "status IN ('queued', 'in_progress') AND owner_instance_boot_id IS NOT NULL"}

  defp qualify_attempts do
    query(
      "UPDATE attempts SET status='in_progress',owner_instance_id='synthetic-node',owner_instance_boot_id='synthetic-boot',owner_execution_id=gen_random_uuid()"
    )
  end

  defp start_index_builder("client_exit", name, columns, predicate) do
    application_name = "migration_client_exit_#{System.unique_integer([:positive])}"
    mix = System.find_executable("mix") || raise "mix executable not found"

    port =
      Port.open({:spawn_executable, mix}, [
        :binary,
        :exit_status,
        :stderr_to_stdout,
        args: [
          "run",
          "--no-start",
          "--no-compile",
          __ENV__.file,
          "--client-exit-builder",
          application_name,
          name,
          columns,
          predicate
        ],
        cd: File.cwd!()
      ])

    %{kind: :external, connection: port, application_name: application_name}
  end

  defp start_index_builder(_scenario, _name, _columns, _predicate) do
    {:ok, connection} = Postgrex.start_link(connection_options())
    %{kind: :postgrex, connection: connection}
  end

  defp begin_index_build(%{kind: :external, application_name: application_name}, name, _, _) do
    pid =
      await_result("client-exit builder did not reach the blocked index phase", fn ->
        case external_builder_rows(application_name, name) do
          [[pid, "active", "Lock", true]] -> {:ok, pid}
          rows -> {:retry, rows}
        end
      end)

    {pid, nil}
  end

  defp begin_index_build(%{kind: :postgrex, connection: builder}, name, columns, predicate) do
    [[pid]] = Postgrex.query!(builder, "SELECT pg_backend_pid()", []).rows

    task =
      Task.async(fn ->
        Postgrex.query(
          builder,
          "CREATE INDEX CONCURRENTLY #{name} ON attempts (#{columns}) WHERE #{predicate}",
          [],
          timeout: 60_000
        )
      end)

    {pid, task}
  end

  defp external_builder_rows(application_name, name) do
    query(
      """
      SELECT pid,state,wait_event_type,
             cardinality(pg_blocking_pids(pid))>0
      FROM pg_stat_activity
      WHERE datname=current_database() AND application_name=$1
        AND query LIKE $2
      """,
      [application_name, "CREATE INDEX CONCURRENTLY #{name}%"]
    ).rows
  end

  defp interrupt_index("client_exit", %{kind: :external, connection: port}, pid, nil, name) do
    [[^pid, "active", "Lock", true]] =
      query(
        """
        SELECT pid,state,wait_event_type,
               cardinality(pg_blocking_pids(pid))>0
        FROM pg_stat_activity
        WHERE pid=$1 AND query LIKE $2
        """,
        [pid, "CREATE INDEX CONCURRENTLY #{name}%"]
      ).rows

    {:os_pid, os_pid} = Port.info(port, :os_pid)
    {_output, 0} = hard_kill(os_pid)
    {_output, exit_code} = collect_port_exit(port, "")
    true = exit_code != 0

    [["active", wait_event, true]] =
      query(
        "SELECT state,wait_event_type,cardinality(pg_blocking_pids(pid))>0 FROM pg_stat_activity WHERE pid=$1",
        [pid]
      ).rows

    receipt("client_exit", %{
      client_process_killed: true,
      server_backend_still_running: true,
      observed_wait_event: wait_event,
      fixture_pg_cancel_backend_used: false
    })
  end

  defp interrupt_index(_, %{kind: :postgrex}, pid, task, _name) do
    [[true]] = query("SELECT pg_cancel_backend($1)", [pid]).rows
    {:error, %Postgrex.Error{postgres: %{code: :query_canceled}}} = Task.await(task, 60_000)
  end

  defp stop_index_builder(%{kind: :postgrex, connection: builder}) do
    if Process.alive?(builder), do: GenServer.stop(builder)
  end

  defp stop_index_builder(%{kind: :external, connection: port}) do
    case Port.info(port, :os_pid) do
      {:os_pid, os_pid} ->
        _ = hard_kill(os_pid)
        _ = collect_port_exit(port, "")
        :ok

      nil ->
        :ok
    end
  end

  defp hard_kill(os_pid) do
    System.cmd("/bin/sh", [
      "-c",
      "kill -KILL \"$1\"",
      "migration-client",
      Integer.to_string(os_pid)
    ])
  end

  defp collect_port_exit(port, output),
    do: collect_port_exit(port, output, System.monotonic_time(:millisecond) + 15_000)

  defp collect_port_exit(port, output, deadline) do
    remaining = max(deadline - System.monotonic_time(:millisecond), 0)

    receive do
      {^port, {:data, data}} -> collect_port_exit(port, bounded_output(output, data), deadline)
      {^port, {:exit_status, exit_code}} -> {output, exit_code}
      {:EXIT, ^port, _reason} -> collect_port_exit(port, output, deadline)
    after
      remaining -> raise "client process did not exit"
    end
  end

  defp bounded_output(output, data) do
    combined = output <> data
    size = byte_size(combined)

    if size <= @client_output_limit,
      do: combined,
      else: binary_part(combined, size - @client_output_limit, @client_output_limit)
  end

  defp validate_client_exit_target!(application_name, name, columns, predicate) do
    {_version, expected_name, expected_columns, expected_predicate} = index_target("client_exit")

    unless Regex.match?(~r/\Amigration_client_exit_[1-9][0-9]*\z/, application_name) and
             {name, columns, predicate} ==
               {expected_name, expected_columns, expected_predicate},
           do: raise(ArgumentError, "invalid client-exit builder target")
  end

  defp seed(rows, attempt_rows \\ nil, ledger_rows \\ nil) do
    attempt_rows = attempt_rows || rows
    ledger_rows = ledger_rows || rows
    pool = CodexPooler.PoolerFixtures.pool_fixture()
    model = CodexPooler.PoolerFixtures.model_fixture(pool)
    %{assignment: assignment} = CodexPooler.PoolerFixtures.upstream_assignment_fixture(pool)
    pool_id = Ecto.UUID.dump!(pool.id)
    model_id = Ecto.UUID.dump!(model.id)
    key_id = Ecto.UUID.bingenerate()

    query(
      "INSERT INTO api_keys (id,pool_id,display_name,key_prefix,key_hash) VALUES ($1,$2,'Synthetic migration key','synthetic',digest('synthetic-key','sha256'))",
      [key_id, pool_id]
    )

    query(
      """
      INSERT INTO requests (pool_id,api_key_id,model_id,requested_model,endpoint,transport,status,usage_status,correlation_id)
      SELECT $1,$2,$3,'synthetic-model','/v1/responses','http_json','succeeded','usage_unknown','synthetic-migration-' || n
      FROM generate_series(1,$4::integer) n
      """,
      [pool_id, key_id, model_id, rows]
    )

    query(
      """
      INSERT INTO attempts (request_id,attempt_number,pool_upstream_assignment_id,model_id,upstream_model_id,transport,status,usage_status)
      SELECT id,1,$1,model_id,'synthetic-model','http_json','succeeded','usage_unknown' FROM requests ORDER BY id LIMIT $2
      """,
      [Ecto.UUID.dump!(assignment.id), attempt_rows]
    )

    # Bounded autocommit batches and distributed minute buckets model retained
    # history rather than millions of updates to one trigger-owned bucket row.
    query(
      "CREATE TABLE verification_seed_requests AS SELECT row_number() OVER (ORDER BY id) AS ordinal,id,pool_id,api_key_id FROM requests"
    )

    query("CREATE UNIQUE INDEX ON verification_seed_requests(ordinal)")

    for first <- 1..ledger_rows//10_000 do
      query(
        """
        INSERT INTO ledger_entries(pool_id,api_key_id,request_id,entry_kind,transport,occurred_at)
        SELECT r.pool_id,r.api_key_id,r.id,'reservation','http_json',
          timestamptz '2026-01-01 00:00:00+00' + (n / 100) * interval '1 minute'
        FROM generate_series($1::bigint,$2::bigint) n
        JOIN verification_seed_requests r ON r.ordinal=1+((n-1)%$3::bigint)
        """,
        [first, min(first + 9_999, ledger_rows), rows]
      )
    end

    query("DROP TABLE verification_seed_requests")
  end

  defp assert_final_schema do
    [[2]] =
      query(
        "SELECT count(*) FROM pg_constraint WHERE conname IN ('requests_api_key_id_fkey','ledger_entries_api_key_id_fkey') AND confdeltype='n' AND convalidated"
      ).rows

    [[2]] =
      query(
        "SELECT count(*) FROM pg_index i JOIN pg_class c ON c.oid=i.indexrelid WHERE c.relname IN ('attempts_open_owner_incarnation_idx','attempts_open_execution_index') AND i.indisvalid"
      ).rows
  end

  defp schema_snapshot do
    query("""
    SELECT md5(string_agg(value,E'\\n' ORDER BY value)) FROM (
      SELECT indexdef AS value FROM pg_indexes WHERE schemaname='public'
      UNION ALL SELECT conrelid::regclass::text || ':' || conname || ':' || pg_get_constraintdef(oid)
        FROM pg_constraint WHERE connamespace='public'::regnamespace
      UNION ALL SELECT table_name || ':' || column_name || ':' || data_type || ':' ||
        coalesce(character_maximum_length::text,'') || ':' || is_nullable || ':' || coalesce(column_default,'')
        FROM information_schema.columns WHERE table_schema='public'
      UNION ALL SELECT proname || ':' || md5(prosrc) FROM pg_proc WHERE pronamespace='public'::regnamespace
    ) definitions
    """).rows
  end

  defp data_snapshot do
    for table <- ~w(requests attempts ledger_entries) do
      query("""
      SELECT count(*),bit_xor(hashtextextended((to_jsonb(t)-ARRAY[
        'owner_instance_id','owner_instance_boot_id','owner_process_id',
        'owner_execution_id','owner_execution_checked_at'])::text,0))::text FROM #{table} t
      """).rows
    end
  end

  defp head_data_snapshot do
    for table <- ~w(requests attempts ledger_entries) do
      query("""
      SELECT count(*),bit_xor(hashtextextended(to_jsonb(t)::text,0))::text FROM #{table} t
      """).rows
    end
  end

  defp physical_snapshot do
    {data_snapshot(),
     query("""
     SELECT c.oid,c.relname,c.relfilenode,c.xmin::text FROM pg_class c
     JOIN pg_namespace n ON n.oid=c.relnamespace
     WHERE n.nspname='public' AND c.relname NOT LIKE 'schema_migrations%' ORDER BY c.oid
     """).rows,
     query(
       "SELECT oid,xmin::text,conname,pg_get_constraintdef(oid) FROM pg_constraint WHERE connamespace='public'::regnamespace ORDER BY oid"
     ).rows,
     query(
       "SELECT oid,xmin::text,md5(prosrc) FROM pg_proc WHERE pronamespace='public'::regnamespace ORDER BY oid"
     ).rows,
     for(
       table <- ~w(requests attempts ledger_entries),
       do: query("SELECT bit_xor(hashtextextended(xmin::text,0))::text FROM #{table}").rows
     )}
  end

  defp migrate(version) do
    unload_migrations()
    opts = if version == :all, do: [all: true], else: [to: version]
    Migrator.run(Repo, @migrations, :up, [log: false] ++ opts)
  end

  defp migrations do
    Path.wildcard(@migrations <> "/*.exs")
    |> Enum.map(fn path ->
      {String.to_integer(String.slice(Path.basename(path), 0, 14)), path}
    end)
    |> Enum.sort()
  end

  defp down(version) do
    unload_migrations()
    {^version, path} = Enum.find(migrations(), fn {v, _} -> v == version end)
    [{module, _}] = Code.compile_file(path)
    :ok = Migrator.down(Repo, version, module, log: false)
  end

  defp unload_migrations do
    for {module, _} <- :code.all_loaded(),
        String.starts_with?(Atom.to_string(module), "Elixir.CodexPooler.Repo.Migrations.") do
      :code.purge(module)
      :code.delete(module)
    end
  end

  defp connection_options,
    do: Keyword.take(Repo.config(), [:hostname, :port, :username, :password, :database])

  defp query(sql, params \\ []),
    do:
      Repo.query!(sql, params,
        log: false,
        timeout: Process.get({__MODULE__, :query_timeout}, 120_000)
      )

  defp receipt(stage, values),
    do: IO.puts(CodexPooler.JSON.encode!(Map.put(values, :stage, stage)))

  defp await(fun), do: await(fun, System.monotonic_time(:millisecond) + 15_000)

  defp await(fun, deadline) do
    unless fun.() do
      if System.monotonic_time(:millisecond) > deadline, do: raise("database barrier timed out")

      receive do
      after
        10 -> await(fun, deadline)
      end
    end
  end

  defp await_result(message, fun),
    do: await_result(message, fun, System.monotonic_time(:millisecond) + 15_000, nil)

  defp await_result(message, fun, deadline, previous) do
    case fun.() do
      {:ok, value} ->
        value

      {:retry, observed} ->
        if System.monotonic_time(:millisecond) > deadline,
          do: raise("#{message}: #{inspect(observed || previous)}")

        receive do
        after
          10 -> await_result(message, fun, deadline, observed)
        end
    end
  end
end

CodexPooler.Verification.ReleaseUpgradeMigrations.run(System.argv())
