defmodule CodexPooler.Platform.ExecutionTerminalProofMigrationTest do
  use CodexPooler.DataCase, async: false

  alias CodexPooler.UnboxedFixture
  @version 20_260_914_205_307
  @migration CodexPooler.Repo.Migrations.CreateExecutionTerminalProofs

  test "terminal proof migration goes up down up on an owned populated schema" do
    config =
      CodexPooler.Repo.config() |> Keyword.merge(pool: DBConnection.ConnectionPool, log: false)

    start_supervised!({CodexPooler.ExecutionMigrationRepo, config})
    prefix = "execution_migration_#{System.unique_integer([:positive])}"

    UnboxedFixture.register_unboxed_cleanup!(fn ->
      Repo.query!("DROP SCHEMA IF EXISTS #{prefix} CASCADE")
    end)

    unless Code.ensure_loaded?(@migration),
      do:
        Code.require_file(
          "priv/repo/migrations/20260914205307_create_execution_terminal_proofs.exs"
        )

    rehearse(prefix)
  end

  defp rehearse(prefix) do
    alias CodexPooler.ExecutionMigrationRepo, as: Repo
    Repo.query!("CREATE SCHEMA #{prefix}")
    Repo.query!("CREATE TABLE #{prefix}.retained (value integer PRIMARY KEY)")
    Repo.query!("INSERT INTO #{prefix}.retained VALUES (7)")
    options = [prefix: prefix, log: false]
    assert :ok = Ecto.Migrator.up(Repo, @version, @migration, options)
    assert :already_up = Ecto.Migrator.up(Repo, @version, @migration, options)
    uuid = Ecto.UUID.generate()

    Repo.query!(
      """
      INSERT INTO #{prefix}.execution_terminal_proofs
        (execution_id, owner_instance_id, owner_instance_boot_id, owner_process_id, end_kind, ended_at)
      VALUES ($1, 'owner@example.invalid', 'synthetic-boot', '<0.1.0>', 'completed', now())
      """,
      [Ecto.UUID.dump!(uuid)]
    )

    assert %{rows: [[1]]} =
             Repo.query!("SELECT count(*) FROM #{prefix}.execution_terminal_proofs")

    assert %{rows: [[true]]} =
             Repo.query!(
               "SELECT published_at BETWEEN (statement_timestamp() AT TIME ZONE 'UTC') - interval '1 minute' AND (statement_timestamp() AT TIME ZONE 'UTC') FROM #{prefix}.execution_terminal_proofs"
             )

    assert {:error, %Postgrex.Error{postgres: %{code: :not_null_violation}}} =
             Repo.query("UPDATE #{prefix}.execution_terminal_proofs SET published_at=NULL")

    for {column, value, constraint} <- [
          {"end_kind", "live", "execution_terminal_proofs_end_kind_check"},
          {"owner_process_id", "malformed", "execution_terminal_proofs_process_check"},
          {"owner_instance_boot_id", "", "execution_terminal_proofs_owner_check"}
        ] do
      assert {:error,
              %Postgrex.Error{postgres: %{code: :check_violation, constraint: ^constraint}}} =
               Repo.query("UPDATE #{prefix}.execution_terminal_proofs SET #{column}=$1", [value])
    end

    assert :ok = Ecto.Migrator.down(Repo, @version, @migration, options)

    assert %{rows: [[nil]]} =
             Repo.query!("SELECT to_regclass($1)", [prefix <> ".execution_terminal_proofs"])

    assert %{rows: [[7]]} = Repo.query!("SELECT value FROM #{prefix}.retained")
    assert :ok = Ecto.Migrator.up(Repo, @version, @migration, options)

    assert %{rows: [[0]]} =
             Repo.query!("SELECT count(*) FROM #{prefix}.execution_terminal_proofs")

    assert %{rows: [[true]]} =
             Repo.query!(
               """
               SELECT bool_and(i.indisvalid) FROM pg_index i JOIN pg_class t ON t.oid=i.indrelid
               JOIN pg_namespace n ON n.oid=t.relnamespace
               WHERE n.nspname=$1 AND t.relname='execution_terminal_proofs'
               """,
               [prefix]
             )

    assert %{rows: [[true]]} =
             Repo.query!(
               "SELECT EXISTS(SELECT 1 FROM pg_indexes WHERE schemaname=$1 AND tablename='execution_terminal_proofs' AND indexdef LIKE '%(published_at, execution_id)%')",
               [prefix]
             )

    CodexPooler.TestDiagnostics.puts(
      "execution proof migration: up/down/up=true populated_schema_preserved=true constraints_reject_invalid=true indexes_valid=true"
    )
  end
end
