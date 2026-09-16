defmodule CodexPooler.Telemetry.RelayEvent do
  @moduledoc false
  use CodexPooler.Schema
  import Ecto.Changeset

  # Exactly the names `CodexPooler.Telemetry.RelayRuntime` maps back to a
  # telemetry event. A name this list holds and the runtime cannot replay is
  # claimed on drain and discarded, counted by no loss reason, so
  # `RelayContractTest` pins the two sets equal in both directions and the
  # database `event_allowed` constraint enumerates the same four.
  @events ~w(quota_cycle_decision saved_reset_convergence pre_attempt_release stream_outcome)

  # Mirrors `RelayRuntime.bounded/1`, which is what the producer applies before
  # a row is written; the database enforces the same bound for a writer that
  # bypasses this changeset.
  @label_value_bytes 80
  @label_key_bytes 40

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
      if is_map(v) and map_size(v) <= 16 and Enum.all?(v, &bounded_label?/1),
        do: [],
        else: [labels: "must be a bounded map of bounded strings"]
    end)
    # A relayed sample is a count or a millisecond duration. A negative or
    # fractional value is a corrupt sample that would be replayed into a
    # Prometheus series as if an emitter had produced it.
    |> validate_change(:measurements, fn :measurements, v ->
      if is_map(v) and map_size(v) <= 8 and Enum.all?(v, &bounded_measurement?/1),
        do: [],
        else: [measurements: "must be bounded non-negative integer measurements"]
    end)
  end

  defp bounded_measurement?({key, value}),
    do:
      is_atom(key) and byte_size(Atom.to_string(key)) <= @label_key_bytes and
        is_integer(value) and value >= 0 and value <= 1_000_000_000_000

  defp bounded_label?({key, value}) do
    key_bytes =
      case key do
        key when is_atom(key) -> byte_size(Atom.to_string(key))
        key when is_binary(key) -> byte_size(key)
        _ -> @label_key_bytes + 1
      end

    key_bytes <= @label_key_bytes and is_binary(value) and
      byte_size(value) <= @label_value_bytes
  end

  def events, do: @events
end
