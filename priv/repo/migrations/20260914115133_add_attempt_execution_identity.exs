defmodule CodexPooler.Repo.Migrations.AddAttemptExecutionIdentity do
  use Ecto.Migration

  def up do
    execute("SET LOCAL lock_timeout = '10s'")

    alter table(:attempts) do
      add :owner_process_id, :string, size: 64
      add :owner_execution_id, :uuid
      add :owner_execution_checked_at, :timestamptz
    end
  end

  def down do
    execute("SET LOCAL lock_timeout = '10s'")

    alter table(:attempts) do
      remove :owner_execution_checked_at
      remove :owner_execution_id
      remove :owner_process_id
    end
  end
end
