defmodule CodexPooler.Repo.Migrations.CreateInstanceSettings do
  use Ecto.Migration

  def change do
    create table(:instance_settings, primary_key: false) do
      add :singleton, :boolean, primary_key: true, default: true, null: false
      add :gateway, :map, null: false, default: fragment("'{}'::jsonb")
      add :ingress, :map, null: false, default: fragment("'{}'::jsonb")
      add :files, :map, null: false, default: fragment("'{}'::jsonb")
      add :transcription, :map, null: false, default: fragment("'{}'::jsonb")
      add :operator, :map, null: false, default: fragment("'{}'::jsonb")
      add :metrics, :map, null: false, default: fragment("'{}'::jsonb")
      add :smtp, :map, null: false, default: fragment("'{}'::jsonb")
      add :metadata, :map, null: false, default: fragment("'{}'::jsonb")
      add :lock_version, :integer, null: false, default: 1
      add :updated_by_user_id, references(:users, type: :binary_id, on_delete: :nilify_all)

      timestamps(type: :utc_datetime_usec)
    end

    create constraint(:instance_settings, :instance_settings_singleton_true_check, check: "singleton = true")
  end
end
