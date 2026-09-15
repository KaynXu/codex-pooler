defmodule CodexPooler.Repo.Migrations.AddTelemetryRelayHealth do
  use Ecto.Migration

  def change do
    create table(:telemetry_relay_losses, primary_key: false) do
      add :reason, :string, primary_key: true
      add :rows, :bigint, null: false, default: 0
      add :samples, :bigint, null: false, default: 0
    end

    create constraint(:telemetry_relay_losses, :relay_loss_reason,
             check: "reason IN ('expired_unclaimed','buffer_overflow','shutdown_unflushed')"
           )

    create constraint(:telemetry_relay_losses, :relay_loss_nonnegative,
             check: "rows >= 0 AND samples >= 0"
           )

    create table(:telemetry_relay_loss_checkpoints, primary_key: false) do
      add :owner, :string, primary_key: true
      add :reason, :string, primary_key: true
      add :samples, :bigint, null: false, default: 0
      add :updated_at, :utc_datetime_usec, null: false
    end

    create table(:telemetry_relay_consumers, primary_key: false) do
      add :owner, :string, primary_key: true
      add :heartbeat_at, :utc_datetime_usec, null: false
      add :quiesced, :boolean, null: false, default: false
    end

    create constraint(:telemetry_relay_loss_checkpoints, :relay_checkpoint_reason,
             check: "reason IN ('buffer_overflow','shutdown_unflushed') AND samples >= 0"
           )
  end
end
