defmodule CodexPooler.Repo.Migrations.TightenTelemetryRelayStorageBounds do
  use Ecto.Migration

  # The event names `CodexPooler.Telemetry.RelayRuntime` can actually replay.
  @replayable "'quota_cycle_decision','saved_reset_convergence','pre_attempt_release','stream_outcome'"

  # What the allowlist held before: `stale_sweep` is a `pre_attempt_release`
  # phase and `interrupted` a `stream_outcome` outcome, not events of their own.
  @previous "'stale_sweep','quota_cycle_decision','saved_reset_convergence','pre_attempt_release','stream_outcome','interrupted'"

  def up do
    # A row named `stale_sweep` or `interrupted` is claimable and, on drain,
    # falls through the source-event lookup: claimed, discarded, counted by no
    # loss reason. Nothing writes one today, but the allowlist admitted it.
    # Relay rows are pruned within 24 hours, so dropping any that exist loses
    # nothing an operator can still read.
    execute("DELETE FROM telemetry_relay_events WHERE event IN ('stale_sweep','interrupted')")

    execute("ALTER TABLE telemetry_relay_events DROP CONSTRAINT event_allowed")

    execute(
      "ALTER TABLE telemetry_relay_events ADD CONSTRAINT event_allowed CHECK (event IN (#{@replayable}))"
    )

    # `labels_bounded` bounds the number of keys and nothing else. A raw writer
    # could still store a non-string value or an unbounded string, which is
    # exactly the bypass the key-count constraint exists to close. This mirrors
    # `RelayRuntime.bounded/1` (80 bytes) at the database, so the bound holds
    # for a writer that never goes through the changeset.
    execute("""
    CREATE FUNCTION telemetry_relay_labels_bounded(labels jsonb) RETURNS boolean
    LANGUAGE sql IMMUTABLE PARALLEL SAFE STRICT AS $fn$
      SELECT NOT EXISTS (
        SELECT 1 FROM jsonb_each(labels) AS kv(key, value)
        WHERE jsonb_typeof(kv.value) <> 'string'
           OR octet_length(kv.key) > 40
           OR octet_length(kv.value #>> '{}') > 80
      )
    $fn$
    """)

    execute("""
    ALTER TABLE telemetry_relay_events
      ADD CONSTRAINT labels_values_bounded CHECK (telemetry_relay_labels_bounded(labels))
    """)

    # `measurements_bounded` bounds the number of measurement keys and leaves
    # every value unconstrained. A relayed sample is a count or a millisecond
    # duration: a negative or fractional value is a corrupt sample that would
    # be replayed into a Prometheus series as if it were real.
    execute("""
    ALTER TABLE telemetry_relay_events
      ADD CONSTRAINT measurements_non_negative_integers
      CHECK (NOT jsonb_path_exists(measurements, '$.* ? (@.type() != "number" || @ < 0 || @ != @.floor())'))
    """)
  end

  def down do
    execute(
      "ALTER TABLE telemetry_relay_events DROP CONSTRAINT measurements_non_negative_integers"
    )

    execute("ALTER TABLE telemetry_relay_events DROP CONSTRAINT labels_values_bounded")
    execute("DROP FUNCTION telemetry_relay_labels_bounded(jsonb)")
    execute("ALTER TABLE telemetry_relay_events DROP CONSTRAINT event_allowed")

    execute(
      "ALTER TABLE telemetry_relay_events ADD CONSTRAINT event_allowed CHECK (event IN (#{@previous}))"
    )
  end
end
