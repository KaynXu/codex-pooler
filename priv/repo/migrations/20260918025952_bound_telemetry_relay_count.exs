defmodule CodexPooler.Repo.Migrations.BoundTelemetryRelayCount do
  use Ecto.Migration

  @max_count 10_000

  def up do
    # The producer admits at most 10,000 callbacks per flush. A larger row can
    # therefore only be legacy or bypass-writer input, and replaying it in
    # batches of 100 would keep the consumer in an immediate re-drain loop.
    # Preserve the operational loss in the existing bounded loss series before
    # clamping the unsafe backlog row.
    execute("""
    INSERT INTO telemetry_relay_losses(reason, rows, samples)
    SELECT 'rejected_sample', 0,
           LEAST(COALESCE(SUM(count::numeric - #{@max_count}), 0), 9223372036854775807)::bigint
      FROM telemetry_relay_events
     WHERE count > #{@max_count}
    HAVING COALESCE(SUM(count::numeric - #{@max_count}), 0) > 0
    ON CONFLICT(reason) DO UPDATE
      SET samples = LEAST(
        telemetry_relay_losses.samples::numeric + EXCLUDED.samples,
        9223372036854775807
      )::bigint
    """)

    execute("UPDATE telemetry_relay_events SET count = #{@max_count} WHERE count > #{@max_count}")
    execute("ALTER TABLE telemetry_relay_events DROP CONSTRAINT count_non_negative")

    execute("ALTER TABLE telemetry_relay_events ADD CONSTRAINT count_bounded CHECK (count BETWEEN 0 AND #{@max_count})")
  end

  def down do
    execute("ALTER TABLE telemetry_relay_events DROP CONSTRAINT count_bounded")

    execute("ALTER TABLE telemetry_relay_events ADD CONSTRAINT count_non_negative CHECK (count >= 0)")
  end
end
