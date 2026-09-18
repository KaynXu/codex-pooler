defmodule CodexPooler.Telemetry.RelayCountMigrationTest do
  use CodexPooler.DataCase, async: false

  alias CodexPooler.UnboxedFixture

  @events CodexPooler.Repo.Migrations.CreateTelemetryRelayEvents
  @events_version 20_260_913_071_033
  @measurements CodexPooler.Repo.Migrations.AddMeasurementsToTelemetryRelayEvents
  @measurements_version 20_260_913_080_000
  @health CodexPooler.Repo.Migrations.AddTelemetryRelayHealth
  @health_version 20_260_915_000_825
  @storage_bounds CodexPooler.Repo.Migrations.TightenTelemetryRelayStorageBounds
  @storage_bounds_version 20_260_916_014_136
  @loss_reason CodexPooler.Repo.Migrations.CountUnstorableRelaySamples
  @loss_reason_version 20_260_916_024_751
  @count CodexPooler.Repo.Migrations.BoundTelemetryRelayCount
  @count_version 20_260_918_025_952
  @max_count 10_000

  test "relay count migration clamps legacy rows, records loss, and reverses cleanly" do
    prefix = "relay_count_#{System.unique_integer([:positive])}"

    config =
      Repo.config()
      |> Keyword.merge(
        pool: DBConnection.ConnectionPool,
        log: false,
        parameters: [search_path: "#{prefix},public"]
      )

    start_supervised!({CodexPooler.ExecutionMigrationRepo, config})

    UnboxedFixture.register_unboxed_cleanup!(fn ->
      Repo.query!("DROP SCHEMA IF EXISTS #{prefix} CASCADE")
    end)

    for {migration, path} <- [
          {@events, "priv/repo/migrations/20260913071033_create_telemetry_relay_events.exs"},
          {@measurements,
           "priv/repo/migrations/20260913080000_add_measurements_to_telemetry_relay_events.exs"},
          {@health, "priv/repo/migrations/20260915000825_add_telemetry_relay_health.exs"},
          {@storage_bounds,
           "priv/repo/migrations/20260916014136_tighten_telemetry_relay_storage_bounds.exs"},
          {@loss_reason,
           "priv/repo/migrations/20260916024751_count_unstorable_relay_samples.exs"},
          {@count, "priv/repo/migrations/20260918025952_bound_telemetry_relay_count.exs"}
        ] do
      unless Code.ensure_loaded?(migration), do: Code.require_file(path)
    end

    rehearse(prefix)
  end

  defp rehearse(prefix) do
    alias CodexPooler.ExecutionMigrationRepo, as: Repo

    Repo.query!("CREATE SCHEMA #{prefix}")
    Repo.query!("CREATE TABLE #{prefix}.retained(value integer)")
    Repo.query!("INSERT INTO #{prefix}.retained VALUES(19)")
    opts = [prefix: prefix, log: false]

    assert :ok = Ecto.Migrator.up(Repo, @events_version, @events, opts)
    assert :ok = Ecto.Migrator.up(Repo, @measurements_version, @measurements, opts)
    assert :ok = Ecto.Migrator.up(Repo, @health_version, @health, opts)
    assert :ok = Ecto.Migrator.up(Repo, @storage_bounds_version, @storage_bounds, opts)
    assert :ok = Ecto.Migrator.up(Repo, @loss_reason_version, @loss_reason, opts)

    Repo.query!("""
    INSERT INTO #{prefix}.telemetry_relay_events(event,labels,count,inserted_at)
    VALUES('pre_attempt_release','{}',#{@max_count + 7},now())
    """)

    assert :ok = Ecto.Migrator.up(Repo, @count_version, @count, opts)
    assert :already_up = Ecto.Migrator.up(Repo, @count_version, @count, opts)

    assert %{rows: [[@max_count]]} =
             Repo.query!("SELECT count FROM #{prefix}.telemetry_relay_events")

    assert %{rows: [[7]]} =
             Repo.query!(
               "SELECT samples FROM #{prefix}.telemetry_relay_losses WHERE reason='rejected_sample'"
             )

    assert {:error, %Postgrex.Error{postgres: %{code: :check_violation}}} =
             Repo.query("UPDATE #{prefix}.telemetry_relay_events SET count=#{@max_count + 1}")

    assert :ok = Ecto.Migrator.down(Repo, @count_version, @count, opts)
    assert %{rows: [[19]]} = Repo.query!("SELECT value FROM #{prefix}.retained")

    assert %{num_rows: 1} =
             Repo.query!("UPDATE #{prefix}.telemetry_relay_events SET count=#{@max_count + 1}")

    assert :ok = Ecto.Migrator.up(Repo, @count_version, @count, opts)

    assert %{rows: [[@max_count]]} =
             Repo.query!("SELECT count FROM #{prefix}.telemetry_relay_events")

    assert %{rows: [[8]]} =
             Repo.query!(
               "SELECT samples FROM #{prefix}.telemetry_relay_losses WHERE reason='rejected_sample'"
             )
  end
end
