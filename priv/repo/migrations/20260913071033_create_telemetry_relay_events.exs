defmodule CodexPooler.Repo.Migrations.CreateTelemetryRelayEvents do
  use Ecto.Migration

  def change do
    create table(:telemetry_relay_events) do
      add :event, :string, null: false
      add :labels, :map, null: false, default: %{}
      add :count, :bigint, null: false, default: 1
      add :inserted_at, :utc_datetime_usec, null: false
      add :claimed_at, :utc_datetime_usec
      add :claimed_by, :string
    end

    create constraint(:telemetry_relay_events, :event_allowed,
             check:
               "event IN ('stale_sweep','quota_cycle_decision','saved_reset_convergence','interrupted')"
           )

    create constraint(:telemetry_relay_events, :count_non_negative, check: "count >= 0")

    create constraint(:telemetry_relay_events, :labels_bounded,
             check: "jsonb_object_length(labels) <= 16"
           )

    create index(:telemetry_relay_events, [:inserted_at])
    create index(:telemetry_relay_events, [:claimed_at])

    create table(:telemetry_relay_heartbeats, primary_key: false) do
      add :owner, :string, primary_key: true
      add :heartbeat_at, :utc_datetime_usec, null: false
    end
  end
end
