defmodule CodexPooler.Repo.Migrations.AddMeasurementsToTelemetryRelayEvents do
  use Ecto.Migration

  def change do
    alter table(:telemetry_relay_events) do
      add :measurements, :map, null: false, default: %{}
    end

    create constraint(:telemetry_relay_events, :measurements_bounded, check: "jsonb_array_length(jsonb_path_query_array(measurements, '$.*')) <= 8")
  end
end
