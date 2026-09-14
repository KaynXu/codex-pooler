defmodule CodexPooler.Repo.Migrations.CreateExecutionTerminalProofs do
  use Ecto.Migration

  def change do
    create table(:execution_terminal_proofs, primary_key: false) do
      add :execution_id, :uuid, primary_key: true
      add :owner_instance_id, :string, size: 255, null: false
      add :owner_instance_boot_id, :string, size: 64, null: false
      add :owner_process_id, :string, size: 64, null: false
      add :end_kind, :text, null: false
      add :ended_at, :utc_datetime_usec, null: false

      add :published_at, :utc_datetime_usec,
        null: false,
        default: fragment("(clock_timestamp() AT TIME ZONE 'UTC')")
    end

    create constraint(:execution_terminal_proofs, :execution_terminal_proofs_owner_check,
             check: "owner_instance_id <> '' AND owner_instance_boot_id <> ''"
           )

    create constraint(:execution_terminal_proofs, :execution_terminal_proofs_process_check,
             check: "owner_process_id ~ '^<0\\.[0-9]+\\.[0-9]+>$'"
           )

    create constraint(:execution_terminal_proofs, :execution_terminal_proofs_end_kind_check,
             check: "end_kind IN ('completed', 'process_down')"
           )

    create index(:execution_terminal_proofs, [:published_at, :execution_id])
  end
end
