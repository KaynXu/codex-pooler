defmodule CodexPooler.Repo.Migrations.AddInstancePresenceAndAttemptOwner do
  use Ecto.Migration

  def up do
    execute("SET LOCAL lock_timeout = '10s'")

    create table(:instance_presences, primary_key: false) do
      add :instance_id, :string, primary_key: true
      add :started_at, :utc_datetime_usec, null: false
      add :last_seen_at, :utc_datetime_usec, null: false
      add :updated_at, :utc_datetime_usec, null: false, default: fragment("now()")
    end

    create constraint(:instance_presences, :instance_presences_instance_id_present_check, check: "length(btrim(instance_id)) > 0")

    create index(:instance_presences, [:last_seen_at])

    alter table(:attempts) do
      add :owner_instance_id, :string
    end

    # The final incarnation index is built concurrently after all ownership columns exist.
  end

  def down do
    execute("SET LOCAL lock_timeout = '10s'")

    alter table(:attempts) do
      remove :owner_instance_id
    end

    drop table(:instance_presences)
  end
end
