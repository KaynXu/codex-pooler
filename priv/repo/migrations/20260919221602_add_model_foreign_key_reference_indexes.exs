defmodule CodexPooler.Repo.Migrations.AddModelForeignKeyReferenceIndexes do
  use Ecto.Migration

  @disable_ddl_transaction true
  @disable_migration_lock true

  def change do
    # Parent deletion must locate references without scanning the accounting history.
    for table <- [
          :requests,
          :attempts,
          :ledger_entries,
          :daily_rollups,
          :request_replay_entitlements
        ] do
      create index(table, [:model_id], concurrently: true)
    end
  end
end
