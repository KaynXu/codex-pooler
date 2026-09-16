defmodule CodexPooler.Telemetry.RelayEventsMigrationTest do
  # The relay's own storage is the piece an operator may have to take back out:
  # it is new, it is the only table the telemetry path writes, and a release
  # that has to be rolled back must not strand it. `relay_health_migration_test`
  # proves that for the loss and consumer tables; this proves it for the event
  # table and the measurements column that was added to it afterwards, which is
  # the pair that has to come down in the right order.
  #
  # The rehearsal runs in its own schema against a real, unpooled repo so the
  # DDL is executed rather than rolled back by the sandbox, and an unrelated
  # populated table in that schema proves `down` removed the relay's objects
  # rather than the schema.
  use CodexPooler.DataCase, async: false

  alias CodexPooler.UnboxedFixture

  @events CodexPooler.Repo.Migrations.CreateTelemetryRelayEvents
  @events_version 20_260_913_071_033
  @measurements CodexPooler.Repo.Migrations.AddMeasurementsToTelemetryRelayEvents
  @measurements_version 20_260_913_080_000

  test "relay event storage migrates up, down and up again without touching its neighbours" do
    config = Repo.config() |> Keyword.merge(pool: DBConnection.ConnectionPool, log: false)
    start_supervised!({CodexPooler.ExecutionMigrationRepo, config})
    prefix = "relay_events_#{System.unique_integer([:positive])}"

    UnboxedFixture.register_unboxed_cleanup!(fn ->
      Repo.query!("DROP SCHEMA IF EXISTS #{prefix} CASCADE")
    end)

    ensure_loaded(
      @events,
      "priv/repo/migrations/20260913071033_create_telemetry_relay_events.exs"
    )

    ensure_loaded(
      @measurements,
      "priv/repo/migrations/20260913080000_add_measurements_to_telemetry_relay_events.exs"
    )

    rehearse(prefix)
  end

  defp ensure_loaded(migration, path) do
    unless Code.ensure_loaded?(migration), do: Code.require_file(path)
  end

  defp rehearse(prefix) do
    alias CodexPooler.ExecutionMigrationRepo, as: Repo

    Repo.query!("CREATE SCHEMA #{prefix}")
    Repo.query!("CREATE TABLE #{prefix}.retained(value integer)")
    Repo.query!("INSERT INTO #{prefix}.retained VALUES(11)")
    opts = [prefix: prefix, log: false]

    assert :ok = Ecto.Migrator.up(Repo, @events_version, @events, opts)
    assert :ok = Ecto.Migrator.up(Repo, @measurements_version, @measurements, opts)
    assert :already_up = Ecto.Migrator.up(Repo, @events_version, @events, opts)

    Repo.query!(
      "INSERT INTO #{prefix}.telemetry_relay_events(event,labels,count,measurements,inserted_at) VALUES('stale_sweep','{}',3,'{}',now())"
    )

    # The constraints are part of what `up` owes; a schema that came back
    # without them would still accept every insert below.
    assert {:error, %Postgrex.Error{postgres: %{code: :check_violation}}} =
             Repo.query(
               "INSERT INTO #{prefix}.telemetry_relay_events(event,labels,count,measurements,inserted_at) VALUES('not_allowlisted','{}',1,'{}',now())"
             )

    assert {:error, %Postgrex.Error{postgres: %{code: :check_violation}}} =
             Repo.query(
               "INSERT INTO #{prefix}.telemetry_relay_events(event,labels,count,measurements,inserted_at) VALUES('stale_sweep','{}',-1,'{}',now())"
             )

    # The later migration comes down first: it owns a column and a constraint on
    # the table the earlier one created.
    assert :ok = Ecto.Migrator.down(Repo, @measurements_version, @measurements, opts)

    assert %{rows: []} =
             Repo.query!(
               "SELECT column_name FROM information_schema.columns WHERE table_schema=$1 AND table_name='telemetry_relay_events' AND column_name='measurements'",
               [prefix]
             )

    assert %{rows: [[3]]} =
             Repo.query!("SELECT count FROM #{prefix}.telemetry_relay_events")

    assert %{rows: []} =
             Repo.query!(
               "SELECT c.conname FROM pg_constraint c JOIN pg_namespace n ON n.oid = c.connamespace WHERE c.conname = 'measurements_bounded' AND n.nspname = $1",
               [prefix]
             )

    assert :ok = Ecto.Migrator.down(Repo, @events_version, @events, opts)

    for table <- ["telemetry_relay_events", "telemetry_relay_heartbeats"] do
      assert %{rows: [[nil]]} = Repo.query!("SELECT to_regclass($1)", [prefix <> "." <> table])
    end

    assert %{rows: [[11]]} = Repo.query!("SELECT value FROM #{prefix}.retained")

    assert :ok = Ecto.Migrator.up(Repo, @events_version, @events, opts)
    assert :ok = Ecto.Migrator.up(Repo, @measurements_version, @measurements, opts)
    assert %{rows: [[0]]} = Repo.query!("SELECT count(*) FROM #{prefix}.telemetry_relay_events")

    assert {:error, %Postgrex.Error{postgres: %{code: :check_violation}}} =
             Repo.query(
               "INSERT INTO #{prefix}.telemetry_relay_events(event,labels,count,measurements,inserted_at) VALUES('not_allowlisted','{}',1,'{}',now())"
             )
  end
end
