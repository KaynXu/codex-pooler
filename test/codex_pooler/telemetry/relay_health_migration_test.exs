defmodule CodexPooler.Telemetry.RelayHealthMigrationTest do
  use CodexPooler.DataCase, async: false
  alias CodexPooler.UnboxedFixture
  @migration CodexPooler.Repo.Migrations.AddTelemetryRelayHealth
  @version 20_260_915_000_825

  test "relay health migration up down up preserves an owned populated schema" do
    config = Repo.config() |> Keyword.merge(pool: DBConnection.ConnectionPool, log: false)
    start_supervised!({CodexPooler.ExecutionMigrationRepo, config})
    prefix = "relay_health_#{System.unique_integer([:positive])}"

    UnboxedFixture.register_unboxed_cleanup!(fn ->
      Repo.query!("DROP SCHEMA IF EXISTS #{prefix} CASCADE")
    end)

    unless Code.ensure_loaded?(@migration),
      do: Code.require_file("priv/repo/migrations/20260915000825_add_telemetry_relay_health.exs")

    rehearse(prefix)
  end

  defp rehearse(prefix) do
    alias CodexPooler.ExecutionMigrationRepo, as: Repo
    Repo.query!("CREATE SCHEMA #{prefix}")
    Repo.query!("CREATE TABLE #{prefix}.retained(value integer)")
    Repo.query!("INSERT INTO #{prefix}.retained VALUES(7)")
    opts = [prefix: prefix, log: false]
    assert :ok = Ecto.Migrator.up(Repo, @version, @migration, opts)
    assert :already_up = Ecto.Migrator.up(Repo, @version, @migration, opts)

    Repo.query!(
      "INSERT INTO #{prefix}.telemetry_relay_losses(reason,rows,samples) VALUES('expired_unclaimed',2,9)"
    )

    assert {:error, %Postgrex.Error{postgres: %{code: :check_violation}}} =
             Repo.query(
               "INSERT INTO #{prefix}.telemetry_relay_losses(reason,rows,samples) VALUES('invalid',0,0)"
             )

    assert :ok = Ecto.Migrator.down(Repo, @version, @migration, opts)

    assert %{rows: [[nil]]} =
             Repo.query!("SELECT to_regclass($1)", [prefix <> ".telemetry_relay_losses"])

    assert %{rows: [[7]]} = Repo.query!("SELECT value FROM #{prefix}.retained")
    assert :ok = Ecto.Migrator.up(Repo, @version, @migration, opts)
    assert %{rows: [[0]]} = Repo.query!("SELECT count(*) FROM #{prefix}.telemetry_relay_losses")
  end
end
