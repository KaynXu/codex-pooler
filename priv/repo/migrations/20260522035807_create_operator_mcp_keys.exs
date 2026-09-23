defmodule CodexPooler.Repo.Migrations.CreateOperatorMcpKeys do
  use Ecto.Migration

  def change do
    create table(:operator_mcp_keys, primary_key: false) do
      add :id, :binary_id, primary_key: true
      add :operator_id, references(:users, type: :binary_id, on_delete: :delete_all), null: false
      add :label, :text, null: false
      add :key_prefix, :text, null: false
      add :key_hash, :binary, null: false

      timestamps(type: :utc_datetime_usec)
    end

    create unique_index(:operator_mcp_keys, [:key_prefix], name: :operator_mcp_keys_prefix_uq)
    create unique_index(:operator_mcp_keys, [:key_hash], name: :operator_mcp_keys_hash_uq)
    create index(:operator_mcp_keys, [:operator_id], name: :operator_mcp_keys_operator_id_idx)

    create constraint(:operator_mcp_keys, :operator_mcp_keys_label_not_blank, check: "length(btrim(label)) > 0")
  end
end
