defmodule CodexPooler.Repo.Migrations.SimplifyInvitesAndAddAcceptances do
  use Ecto.Migration

  def up do
    execute("ALTER TABLE public.invites DROP CONSTRAINT invites_status_check")
    execute("ALTER TABLE public.invites DROP CONSTRAINT invites_check")
    execute("ALTER TABLE public.invites DROP CONSTRAINT invites_max_redemptions_check")
    execute("ALTER TABLE public.invites DROP CONSTRAINT invites_redemptions_used_check")

    alter table(:invites) do
      add :accepted_at, :utc_datetime_usec
    end

    execute("""
    UPDATE public.invites
    SET status = 'accepted',
        accepted_at = COALESCE(last_consumed_at, updated_at),
        updated_at = COALESCE(last_consumed_at, updated_at)
    WHERE redemptions_used > 0 OR last_consumed_at IS NOT NULL
    """)

    alter table(:invites) do
      remove :max_redemptions
      remove :redemptions_used
      remove :last_consumed_at
      remove :metadata
    end

    execute("""
    ALTER TABLE public.invites
    ADD CONSTRAINT invites_status_check
    CHECK (status = ANY (ARRAY['active'::text, 'accepted'::text, 'revoked'::text, 'expired'::text]))
    """)

    execute("ALTER TABLE public.invite_redemptions RENAME TO invite_acceptances")

    execute("ALTER TABLE public.invite_acceptances RENAME COLUMN consumed_by_email TO accepted_by_email")

    execute("ALTER TABLE public.invite_acceptances RENAME COLUMN consumed_at TO accepted_at")

    execute("ALTER TABLE public.invite_acceptances DROP CONSTRAINT invite_redemptions_status_check")

    alter table(:invite_acceptances) do
      remove :status
      remove :error_message
    end

    rename_constraint(:invite_acceptances, :invite_redemptions_pkey, :invite_acceptances_pkey)

    rename_constraint(
      :invite_acceptances,
      :invite_redemptions_invite_id_fkey,
      :invite_acceptances_invite_id_fkey
    )

    rename_constraint(
      :invite_acceptances,
      :invite_redemptions_pool_id_fkey,
      :invite_acceptances_pool_id_fkey
    )

    rename_constraint(
      :invite_acceptances,
      :invite_redemptions_pool_upstream_assignment_id_fkey,
      :invite_acceptances_pool_upstream_assignment_id_fkey
    )

    rename_constraint(
      :invite_acceptances,
      :invite_redemptions_upstream_identity_id_fkey,
      :invite_acceptances_upstream_identity_id_fkey
    )

    rename_constraint(
      :invite_acceptances,
      :invite_redemptions_onboarding_method_check,
      :invite_acceptances_onboarding_method_check
    )

    rename_index(:invite_redemptions_pool_consumed_idx, :invite_acceptances_pool_accepted_idx)

    execute("DROP INDEX public.invite_redemptions_invite_identity_uq")

    execute("""
    WITH ranked AS (
      SELECT ctid,
             row_number() OVER (PARTITION BY invite_id ORDER BY accepted_at ASC, id ASC) AS row_number
      FROM public.invite_acceptances
    )
    DELETE FROM public.invite_acceptances acceptance
    USING ranked
    WHERE acceptance.ctid = ranked.ctid
      AND ranked.row_number > 1
    """)

    create unique_index(:invite_acceptances, [:invite_id], name: :invite_acceptances_invite_id_uq)
  end

  def down do
    drop index(:invite_acceptances, [:invite_id], name: :invite_acceptances_invite_id_uq)
    rename_index(:invite_acceptances_pool_accepted_idx, :invite_redemptions_pool_consumed_idx)

    rename_constraint(
      :invite_acceptances,
      :invite_acceptances_onboarding_method_check,
      :invite_redemptions_onboarding_method_check
    )

    rename_constraint(
      :invite_acceptances,
      :invite_acceptances_upstream_identity_id_fkey,
      :invite_redemptions_upstream_identity_id_fkey
    )

    rename_constraint(
      :invite_acceptances,
      :invite_acceptances_pool_upstream_assignment_id_fkey,
      :invite_redemptions_pool_upstream_assignment_id_fkey
    )

    rename_constraint(
      :invite_acceptances,
      :invite_acceptances_pool_id_fkey,
      :invite_redemptions_pool_id_fkey
    )

    rename_constraint(
      :invite_acceptances,
      :invite_acceptances_invite_id_fkey,
      :invite_redemptions_invite_id_fkey
    )

    rename_constraint(:invite_acceptances, :invite_acceptances_pkey, :invite_redemptions_pkey)

    alter table(:invite_acceptances) do
      add :status, :text, null: false, default: "completed"
      add :error_message, :text
    end

    execute("""
    ALTER TABLE public.invite_acceptances
    ADD CONSTRAINT invite_redemptions_status_check
    CHECK (status = ANY (ARRAY['completed'::text, 'noop'::text, 'failed'::text]))
    """)

    execute("ALTER TABLE public.invite_acceptances RENAME COLUMN accepted_at TO consumed_at")

    execute("ALTER TABLE public.invite_acceptances RENAME COLUMN accepted_by_email TO consumed_by_email")

    execute("ALTER TABLE public.invite_acceptances RENAME TO invite_redemptions")

    create unique_index(:invite_redemptions, [:invite_id, :upstream_identity_id], name: :invite_redemptions_invite_identity_uq)

    execute("ALTER TABLE public.invites DROP CONSTRAINT invites_status_check")

    alter table(:invites) do
      add :max_redemptions, :integer, null: false, default: 1
      add :redemptions_used, :integer, null: false, default: 0
      add :last_consumed_at, :utc_datetime_usec
      add :metadata, :map, null: false, default: %{}
    end

    execute("""
    UPDATE public.invites
    SET status = 'active',
        redemptions_used = 1,
        last_consumed_at = accepted_at,
        updated_at = COALESCE(accepted_at, updated_at)
    WHERE status = 'accepted'
    """)

    execute("""
    ALTER TABLE public.invites
    ADD CONSTRAINT invites_status_check
    CHECK (status = ANY (ARRAY['active'::text, 'revoked'::text, 'expired'::text]))
    """)

    execute("ALTER TABLE public.invites ADD CONSTRAINT invites_check CHECK (redemptions_used <= max_redemptions)")

    execute("ALTER TABLE public.invites ADD CONSTRAINT invites_max_redemptions_check CHECK (max_redemptions > 0)")

    execute("ALTER TABLE public.invites ADD CONSTRAINT invites_redemptions_used_check CHECK (redemptions_used >= 0)")

    alter table(:invites) do
      remove :accepted_at
    end
  end

  defp rename_constraint(table, from, to) do
    execute("ALTER TABLE public.#{table} RENAME CONSTRAINT #{from} TO #{to}")
  end

  defp rename_index(from, to) do
    execute("ALTER INDEX public.#{from} RENAME TO #{to}")
  end
end
