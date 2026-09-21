defmodule CodexPooler.Repo.Migrations.BoundTelemetryRelayJsonColumns do
  use Ecto.Migration

  # `20260916014136` bounded the *contents* of `labels` and `measurements` and
  # left the columns' own shape unbounded, so both bounds were skipped whole
  # whenever the value was not a JSON object:
  #
  #   * `measurements_non_negative_integers` tests `$.*`, and SQL/JSON path lax
  #     mode unwraps arrays, so `{"count":[1,2]}` tested its *elements* and
  #     `measurements = [-1]`, `-1`, `"x"`, `true` and `null` matched nothing at
  #     all. `measurements_bounded` counts `$.*` the same way and returns 0 on a
  #     non-object, so neither constraint fired: every one of those stored.
  #   * `telemetry_relay_labels_bounded` called `jsonb_each` on whatever it was
  #     given, so a non-object `labels` raised `22023 cannot call jsonb_each on
  #     a non-object` instead of violating the check — an error the schema's
  #     `check_constraint/3` cannot map to a field, and the wrong kind of
  #     refusal for a storage bound.
  #
  # Both functions are now total: a non-object is refused by the constraint
  # rather than skipped by it or raised past it. The measurement rule also
  # moves into a plain SQL function, where each value is tested as itself
  # rather than through a path expression that silently descends into it, and
  # it now carries the same upper bound and key-byte bound the changeset
  # applies, so the two agree on every value rather than only on the ones the
  # shipped emitters happen to send.
  @measurements_function """
  CREATE FUNCTION telemetry_relay_measurements_bounded(measurements jsonb) RETURNS boolean
  LANGUAGE sql IMMUTABLE PARALLEL SAFE STRICT AS $fn$
    SELECT CASE
      WHEN jsonb_typeof(measurements) <> 'object' THEN false
      ELSE NOT EXISTS (
        SELECT 1 FROM jsonb_each(measurements) AS kv(key, value)
        WHERE octet_length(kv.key) > 40
           OR CASE
                WHEN jsonb_typeof(kv.value) <> 'number' THEN true
                WHEN (kv.value #>> '{}') !~ '^(0|[1-9][0-9]*)$' THEN true
                ELSE (kv.value #>> '{}')::numeric > 1000000000000
              END
      )
    END
  $fn$
  """

  # `CASE` is the guard, not `AND`/`OR`: PostgreSQL does not promise to skip
  # the other side of a boolean operator, and `jsonb_each` on a non-object and
  # `::numeric` on a non-numeric both raise rather than returning false.
  @labels_function """
  CREATE OR REPLACE FUNCTION telemetry_relay_labels_bounded(labels jsonb) RETURNS boolean
  LANGUAGE sql IMMUTABLE PARALLEL SAFE STRICT AS $fn$
    SELECT CASE
      WHEN jsonb_typeof(labels) <> 'object' THEN false
      ELSE NOT EXISTS (
        SELECT 1 FROM jsonb_each(labels) AS kv(key, value)
        WHERE jsonb_typeof(kv.value) <> 'string'
           OR octet_length(kv.key) > 40
           OR octet_length(kv.value #>> '{}') > 80
      )
    END
  $fn$
  """

  @previous_labels_function """
  CREATE OR REPLACE FUNCTION telemetry_relay_labels_bounded(labels jsonb) RETURNS boolean
  LANGUAGE sql IMMUTABLE PARALLEL SAFE STRICT AS $fn$
    SELECT NOT EXISTS (
      SELECT 1 FROM jsonb_each(labels) AS kv(key, value)
      WHERE jsonb_typeof(kv.value) <> 'string'
         OR octet_length(kv.key) > 40
         OR octet_length(kv.value #>> '{}') > 80
    )
  $fn$
  """

  @previous_measurements_check "NOT jsonb_path_exists(measurements, '$.* ? (@.type() != \"number\" || @ < 0 || @ != @.floor())')"

  def up do
    # Nothing writes a non-object into either column — `RelayEvent.changeset/2`
    # refuses one and every producer goes through it — so re-adding the
    # constraints validates the existing rows rather than rewriting them.
    execute("ALTER TABLE telemetry_relay_events DROP CONSTRAINT labels_values_bounded")
    execute(@labels_function)

    execute("ALTER TABLE telemetry_relay_events ADD CONSTRAINT labels_values_bounded CHECK (telemetry_relay_labels_bounded(labels))")

    execute("ALTER TABLE telemetry_relay_events DROP CONSTRAINT measurements_non_negative_integers")

    execute(@measurements_function)

    execute("ALTER TABLE telemetry_relay_events ADD CONSTRAINT measurements_non_negative_integers CHECK (telemetry_relay_measurements_bounded(measurements))")
  end

  def down do
    execute("ALTER TABLE telemetry_relay_events DROP CONSTRAINT measurements_non_negative_integers")

    execute("DROP FUNCTION telemetry_relay_measurements_bounded(jsonb)")

    execute("ALTER TABLE telemetry_relay_events ADD CONSTRAINT measurements_non_negative_integers CHECK (#{@previous_measurements_check})")

    execute("ALTER TABLE telemetry_relay_events DROP CONSTRAINT labels_values_bounded")
    execute(@previous_labels_function)

    execute("ALTER TABLE telemetry_relay_events ADD CONSTRAINT labels_values_bounded CHECK (telemetry_relay_labels_bounded(labels))")
  end
end
