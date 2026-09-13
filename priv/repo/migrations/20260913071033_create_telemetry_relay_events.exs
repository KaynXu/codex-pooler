defmodule CodexPooler.Repo.Migrations.CreateTelemetryRelayEvents do
  use Ecto.Migration

  def change do
    create table(:telemetry_relay_events, primary_key: false) do
      add :id, :binary_id, primary_key: true, default: fragment("gen_random_uuid()")
      add :event, :string, null: false
      add :labels, :map, null: false, default: %{}
      add :count, :bigint, null: false, default: 1
      add :inserted_at, :utc_datetime_usec, null: false
      add :claimed_at, :utc_datetime_usec
      add :claimed_by, :string
    end

    create constraint(:telemetry_relay_events, :event_allowed,
             check:
               "event IN ('stale_sweep','quota_cycle_decision','saved_reset_convergence','pre_attempt_release','stream_outcome','interrupted')"
           )

    create constraint(:telemetry_relay_events, :count_non_negative, check: "count >= 0")

    create constraint(:telemetry_relay_events, :labels_bounded,
             check: "jsonb_array_length(jsonb_path_query_array(labels, '$.*')) <= 16"
           )

    create index(:telemetry_relay_events, [:inserted_at])
    create index(:telemetry_relay_events, [:claimed_at])

    create table(:telemetry_relay_heartbeats, primary_key: false) do
      add :owner, :string, primary_key: true
      add :heartbeat_at, :utc_datetime_usec, null: false
    end
  end
end
