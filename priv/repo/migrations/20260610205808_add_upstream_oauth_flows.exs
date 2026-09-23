defmodule CodexPooler.Repo.Migrations.AddUpstreamOauthFlows do
  use Ecto.Migration

  def up do
    create table(:upstream_oauth_flows, primary_key: false) do
      add :id, :binary_id, primary_key: true, default: fragment("gen_random_uuid()")
      add :pool_id, references(:pools, type: :binary_id, on_delete: :delete_all), null: false

      add :upstream_identity_id,
          references(:upstream_identities, type: :binary_id, on_delete: :nilify_all)

      add :requested_by_user_id, references(:users, type: :binary_id), null: false
      add :flow_kind, :text, null: false
      add :purpose, :text, null: false
      add :status, :text, null: false, default: "pending"
      add :state_token_hash, :binary
      add :redirect_uri, :text
      add :code_verifier_ciphertext, :binary
      add :device_auth_id_ciphertext, :binary
      add :device_user_code, :text
      add :verification_uri, :text
      add :interval_seconds, :integer
      add :poll_after_at, :utc_datetime_usec
      add :expires_at, :utc_datetime_usec, null: false
      add :completed_at, :utc_datetime_usec
      add :cancelled_at, :utc_datetime_usec
      add :last_polled_at, :utc_datetime_usec

      add :result_upstream_identity_id,
          references(:upstream_identities, type: :binary_id, on_delete: :nilify_all)

      add :error_code, :text
      add :error_message, :text
      add :metadata, :map, null: false, default: %{}
      timestamps(type: :utc_datetime_usec)
    end

    create constraint(:upstream_oauth_flows, :upstream_oauth_flows_flow_kind_check, check: "flow_kind IN ('browser', 'device')")

    create constraint(:upstream_oauth_flows, :upstream_oauth_flows_purpose_check, check: "purpose IN ('link', 'relink')")

    create constraint(:upstream_oauth_flows, :upstream_oauth_flows_status_check, check: "status IN ('pending', 'completed', 'failed', 'cancelled', 'expired')")

    create constraint(:upstream_oauth_flows, :upstream_oauth_flows_metadata_shape_check, check: "jsonb_typeof(metadata) = 'object'")

    create constraint(:upstream_oauth_flows, :upstream_oauth_flows_interval_seconds_check, check: "interval_seconds IS NULL OR interval_seconds > 0")

    create constraint(:upstream_oauth_flows, :upstream_oauth_flows_state_hash_shape_check, check: "state_token_hash IS NULL OR octet_length(state_token_hash) = 32")

    create unique_index(:upstream_oauth_flows, [:state_token_hash],
             name: :upstream_oauth_flows_state_token_hash_uq,
             where: "state_token_hash IS NOT NULL"
           )

    create index(:upstream_oauth_flows, [:pool_id, :status, :expires_at], name: :upstream_oauth_flows_pool_status_expires_idx)

    create index(:upstream_oauth_flows, [:upstream_identity_id, :status, :expires_at], name: :upstream_oauth_flows_identity_status_expires_idx)

    create index(:upstream_oauth_flows, [:upstream_identity_id, :inserted_at, :id],
             name: :upstream_oauth_flows_identity_inserted_idx,
             where: "upstream_identity_id IS NOT NULL"
           )

    create index(:upstream_oauth_flows, [:pool_id, :purpose, :upstream_identity_id],
             name: :upstream_oauth_flows_pending_scope_idx,
             where: "status = 'pending'"
           )

    create index(:upstream_oauth_flows, [:expires_at],
             name: :upstream_oauth_flows_pending_expires_idx,
             where: "status = 'pending'"
           )

    create index(:upstream_oauth_flows, [:updated_at],
             name: :upstream_oauth_flows_terminal_updated_idx,
             where: "status IN ('completed', 'failed', 'cancelled', 'expired')"
           )

    create index(:upstream_oauth_flows, [:requested_by_user_id, :status, :inserted_at], name: :upstream_oauth_flows_requested_status_inserted_idx)
  end

  def down do
    drop_if_exists index(:upstream_oauth_flows, [:requested_by_user_id, :status, :inserted_at], name: :upstream_oauth_flows_requested_status_inserted_idx)

    drop_if_exists index(:upstream_oauth_flows, [:updated_at], name: :upstream_oauth_flows_terminal_updated_idx)

    drop_if_exists index(:upstream_oauth_flows, [:expires_at], name: :upstream_oauth_flows_pending_expires_idx)

    drop_if_exists index(:upstream_oauth_flows, [:pool_id, :purpose, :upstream_identity_id], name: :upstream_oauth_flows_pending_scope_idx)

    drop_if_exists index(
                     :upstream_oauth_flows,
                     [:upstream_identity_id, :inserted_at, :id],
                     name: :upstream_oauth_flows_identity_inserted_idx
                   )

    drop_if_exists index(:upstream_oauth_flows, [:upstream_identity_id, :status, :expires_at], name: :upstream_oauth_flows_identity_status_expires_idx)

    drop_if_exists index(:upstream_oauth_flows, [:pool_id, :status, :expires_at], name: :upstream_oauth_flows_pool_status_expires_idx)

    drop_if_exists index(:upstream_oauth_flows, [:state_token_hash], name: :upstream_oauth_flows_state_token_hash_uq)

    drop table(:upstream_oauth_flows)
  end
end
