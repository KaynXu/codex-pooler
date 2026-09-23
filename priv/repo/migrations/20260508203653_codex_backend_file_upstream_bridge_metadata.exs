defmodule CodexPooler.Repo.Migrations.CodexBackendFileUpstreamBridgeMetadata do
  use Ecto.Migration

  def change do
    drop table(:codex_file_uploads)

    alter table(:codex_files) do
      remove :storage_key, :text
      remove :sha256, :binary
      remove :upload_expires_at, :utc_datetime

      add :pool_upstream_assignment_id,
          references(:pool_upstream_assignments, type: :binary_id, on_delete: :nilify_all)

      add :upstream_identity_id,
          references(:upstream_identities, type: :binary_id, on_delete: :nilify_all)

      add :finalize_status, :text, null: false, default: "pending"
    end

    create index(:codex_files, [:pool_upstream_assignment_id], name: :codex_files_pool_upstream_assignment_id_idx)

    create index(:codex_files, [:upstream_identity_id], name: :codex_files_upstream_identity_id_idx)

    create constraint(:codex_files, :codex_files_finalize_status_check, check: "finalize_status IN ('pending', 'succeeded', 'failed')")
  end
end
