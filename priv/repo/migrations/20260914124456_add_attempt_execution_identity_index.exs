defmodule CodexPooler.Repo.Migrations.AddAttemptExecutionIdentityIndex do
  use Ecto.Migration

  @disable_ddl_transaction true
  @disable_migration_lock true

  def change do
    create index(:attempts, ["COALESCE(owner_execution_checked_at, started_at)", :id],
             name: :attempts_open_execution_index,
             where: "status IN ('queued', 'in_progress') AND owner_execution_id IS NOT NULL",
             concurrently: true
           )
  end
end
