defmodule CodexPooler.Platform.ModelReferenceIndexesMigrationTest do
  use CodexPooler.DataCase, async: false

  alias CodexPooler.UnboxedFixture

  @migration CodexPooler.Repo.Migrations.AddModelForeignKeyReferenceIndexes
  @version 20_260_919_221_602
  @tables ~w(requests attempts ledger_entries daily_rollups request_replay_entitlements)

  test "concurrent model reference indexes survive up/down/up without changing rows" do
    config = Repo.config() |> Keyword.merge(pool: DBConnection.ConnectionPool, log: false)
    start_supervised!({CodexPooler.ExecutionMigrationRepo, config})
    prefix = "model_indexes_#{System.unique_integer([:positive])}"

    UnboxedFixture.register_unboxed_cleanup!(fn ->
      Repo.query!("DROP SCHEMA IF EXISTS #{prefix} CASCADE")
    end)

    unless Code.ensure_loaded?(@migration) do
      Code.require_file(
        "priv/repo/migrations/20260919221602_add_model_foreign_key_reference_indexes.exs"
      )
    end

    rehearse(prefix)
  end

  defp rehearse(prefix) do
    alias CodexPooler.ExecutionMigrationRepo, as: Repo
    Repo.query!("CREATE SCHEMA #{prefix}")

    for table <- @tables do
      Repo.query!("CREATE TABLE #{prefix}.#{table} (model_id uuid)")
      Repo.query!("INSERT INTO #{prefix}.#{table} VALUES ($1), (NULL)", [Ecto.UUID.bingenerate()])
    end

    options = [prefix: prefix, log: false]
    assert :ok = Ecto.Migrator.up(Repo, @version, @migration, options)
    assert_indexes(prefix)
    assert :ok = Ecto.Migrator.down(Repo, @version, @migration, options)

    for table <- @tables do
      assert %{rows: [[nil]]} =
               Repo.query!("SELECT to_regclass($1)", ["#{prefix}.#{table}_model_id_index"])

      assert %{rows: [[2]]} = Repo.query!("SELECT count(*) FROM #{prefix}.#{table}")
    end

    assert :ok = Ecto.Migrator.up(Repo, @version, @migration, options)
    assert_indexes(prefix)
  end

  defp assert_indexes(prefix) do
    for table <- @tables do
      assert %{rows: [[true, true, true]]} =
               CodexPooler.ExecutionMigrationRepo.query!(
                 """
                 SELECT i.indisvalid, i.indpred IS NULL, pg_get_indexdef(i.indexrelid, 1, true) = 'model_id'
                 FROM pg_index i WHERE i.indexrelid = to_regclass($1)
                 """,
                 ["#{prefix}.#{table}_model_id_index"]
               )
    end
  end
end
