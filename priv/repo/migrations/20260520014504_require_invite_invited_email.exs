defmodule CodexPooler.Repo.Migrations.RequireInviteInvitedEmail do
  use Ecto.Migration

  def up do
    execute("""
    UPDATE public.invites
    SET invited_email = 'legacy-invite+' || replace(id::text, '-', '') || '@example.invalid',
        status = 'expired',
        updated_at = now()
    WHERE invited_email IS NULL OR btrim(invited_email) = ''
    """)

    alter table(:invites) do
      modify :invited_email, :text, null: false
    end

    create constraint(:invites, :invites_invited_email_not_blank_check, check: "btrim(invited_email) <> ''")
  end

  def down do
    drop constraint(:invites, :invites_invited_email_not_blank_check)

    alter table(:invites) do
      modify :invited_email, :text, null: true
    end
  end
end
