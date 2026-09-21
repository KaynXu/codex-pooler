defmodule CodexPooler.Repo.Migrations.AllowAlertDeliveryLifetimeAttemptNumbers do
  use Ecto.Migration

  def up do
    drop constraint(:alert_delivery_attempts, :alert_delivery_attempts_attempt_number_check)

    create constraint(:alert_delivery_attempts, :alert_delivery_attempts_attempt_number_check, check: "attempt_number >= 1")
  end

  def down do
    drop constraint(:alert_delivery_attempts, :alert_delivery_attempts_attempt_number_check)

    create constraint(:alert_delivery_attempts, :alert_delivery_attempts_attempt_number_check, check: "attempt_number >= 1 AND attempt_number <= max_attempts")
  end
end
