defmodule CodexPooler.Telemetry.RelayEvent do
  use CodexPooler.Schema
  import Ecto.Changeset
  @events ~w(stale_sweep quota_cycle_decision saved_reset_convergence pre_attempt_release stream_outcome interrupted)
  schema "telemetry_relay_events" do
    field :event, :string
    field :labels, :map, default: %{}
    field :count, :integer, default: 1
    field :measurements, :map, default: %{}
    field :inserted_at, :utc_datetime_usec
    field :claimed_at, :utc_datetime_usec
    field :claimed_by, :string
  end

  def changeset(s, attrs) do
    s
    |> cast(attrs, [:event, :labels, :count, :measurements, :inserted_at])
    |> validate_required([:event, :labels, :count, :measurements, :inserted_at])
    |> validate_inclusion(:event, @events)
    |> validate_number(:count, greater_than_or_equal_to: 0)
    |> validate_change(:labels, fn :labels, v ->
      if is_map(v) and map_size(v) <= 16, do: [], else: [labels: "must be a bounded map"]
    end)
  end

  def events, do: @events
end
