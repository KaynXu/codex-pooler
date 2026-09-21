defmodule CodexPooler.Repo.Migrations.AddSavedResetAutoRedeemThresholdPolicy do
  use Ecto.Migration

  def change do
    alter table(:upstream_identities) do
      add :saved_reset_auto_redeem_trigger_mode, :text, null: false, default: "blocked"
      add :saved_reset_auto_redeem_quota_threshold_percent, :integer, null: false, default: 95
    end

    create constraint(
             :upstream_identities,
             :upstream_identities_saved_reset_trigger_mode_check,
             check: "saved_reset_auto_redeem_trigger_mode IN ('blocked', 'threshold')"
           )

    create constraint(
             :upstream_identities,
             :upstream_identities_saved_reset_threshold_percent_check,
             check: "saved_reset_auto_redeem_quota_threshold_percent >= 1 AND saved_reset_auto_redeem_quota_threshold_percent <= 100"
           )
  end
end
