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
  @max_count 10_000

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
    |> validate_number(:count, greater_than_or_equal_to: 0, less_than_or_equal_to: @max_count)
    |> validate_change(:labels, fn :labels, v ->
      if storable_labels?(v),
        do: [],
        else: [labels: "must be a bounded map of bounded strings"]
    end)
    |> validate_change(:measurements, fn :measurements, v ->
      if storable_measurements?(v),
        do: [],
        else: [measurements: "must be bounded non-negative integer measurements"]
    end)
    # Every CHECK the table carries is named here so a violation comes back as
    # `{:error, changeset}` rather than an `Ecto.ConstraintError` the caller has
    # to rescue. `RelayRuntime.flush_snapshot/3` treats either as a permanent
    # refusal, but a violation the changeset can name is one it can also
    # report.
    |> check_constraint(:event, name: :event_allowed)
    |> check_constraint(:count, name: :count_bounded)
    |> check_constraint(:labels, name: :labels_bounded)
    |> check_constraint(:labels, name: :labels_values_bounded)
    |> check_constraint(:measurements, name: :measurements_bounded)
    |> check_constraint(:measurements, name: :measurements_non_negative_integers)
  end

  @doc """
  Whether the storage layer will accept this measurement map.

  A relayed sample is a count or a millisecond duration: a negative, fractional
  or oversized value is a corrupt sample that would be replayed into a
  Prometheus series as if an emitter had produced it.

  `CodexPooler.Telemetry.RelayRuntime` asks this before it captures a sample,
  and the changeset asks it again before the row is written. It is one
  predicate on purpose: a sample the capture path admits and the changeset
  refuses cannot be inserted and cannot be retried into existence, so it would
  sit in the buffer forever.
  """
  @spec storable_measurements?(term()) :: boolean()
  def storable_measurements?(v),
    do: is_map(v) and map_size(v) <= 8 and Enum.all?(v, &bounded_measurement?/1)

  @doc """
  Whether the storage layer will accept this label map, for the same reason.
  """
  @spec storable_labels?(term()) :: boolean()
  def storable_labels?(v),
    do: is_map(v) and map_size(v) <= 16 and Enum.all?(v, &bounded_label?/1)

  @doc false
  @spec max_count() :: pos_integer()
  def max_count, do: @max_count

  defp bounded_measurement?({key, value}),
    do:
      is_atom(key) and byte_size(Atom.to_string(key)) <= @label_key_bytes and
        is_integer(value) and value >= 0 and value <= 1_000_000_000_000

  defp bounded_label?({key, value}) do
    key =
      case key do
        key when is_atom(key) -> Atom.to_string(key)
        key when is_binary(key) -> key
        _ -> nil
      end

    storable_text?(key, @label_key_bytes) and storable_text?(value, @label_value_bytes)
  end

  defp storable_text?(value, max_bytes) when is_binary(value),
    do:
      byte_size(value) <= max_bytes and String.valid?(value) and
        not String.contains?(value, <<0>>)

  defp storable_text?(_value, _max_bytes), do: false

  def events, do: @events
end
