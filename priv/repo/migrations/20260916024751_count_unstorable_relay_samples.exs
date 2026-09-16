defmodule CodexPooler.Repo.Migrations.CountUnstorableRelaySamples do
  use Ecto.Migration

  # A sample the storage layer will never accept used to be re-accumulated by
  # `RelayRuntime.flush_snapshot/3` on every flush, forever, holding a
  # `max_series` slot and counted by no loss reason — claimed and vanished.
  # It is now refused where it is captured, or counted as lost at the insert,
  # and `rejected_sample` is the reason it is counted under. The checkpoint
  # table takes it too, because the counter is cumulative per owner like
  # `buffer_overflow` rather than a delta.
  @reasons "'expired_unclaimed','buffer_overflow','shutdown_unflushed','rejected_sample'"
  @previous_reasons "'expired_unclaimed','buffer_overflow','shutdown_unflushed'"
  @checkpoint_reasons "'buffer_overflow','shutdown_unflushed','rejected_sample'"
  @previous_checkpoint_reasons "'buffer_overflow','shutdown_unflushed'"

  def up do
    execute("ALTER TABLE telemetry_relay_losses DROP CONSTRAINT relay_loss_reason")

    execute(
      "ALTER TABLE telemetry_relay_losses ADD CONSTRAINT relay_loss_reason CHECK (reason IN (#{@reasons}))"
    )

    execute(
      "ALTER TABLE telemetry_relay_loss_checkpoints DROP CONSTRAINT relay_checkpoint_reason"
    )

    execute(
      "ALTER TABLE telemetry_relay_loss_checkpoints ADD CONSTRAINT relay_checkpoint_reason CHECK (reason IN (#{@checkpoint_reasons}) AND samples >= 0)"
    )
  end

  def down do
    # Rows counted under the new reason are removed rather than left to fail
    # the narrower constraint. They are bookkeeping totals, not events: a
    # rollback loses the count of refused samples and nothing else.
    execute("DELETE FROM telemetry_relay_losses WHERE reason = 'rejected_sample'")
    execute("DELETE FROM telemetry_relay_loss_checkpoints WHERE reason = 'rejected_sample'")
    execute("ALTER TABLE telemetry_relay_losses DROP CONSTRAINT relay_loss_reason")

    execute(
      "ALTER TABLE telemetry_relay_losses ADD CONSTRAINT relay_loss_reason CHECK (reason IN (#{@previous_reasons}))"
    )

    execute(
      "ALTER TABLE telemetry_relay_loss_checkpoints DROP CONSTRAINT relay_checkpoint_reason"
    )

    execute(
      "ALTER TABLE telemetry_relay_loss_checkpoints ADD CONSTRAINT relay_checkpoint_reason CHECK (reason IN (#{@previous_checkpoint_reasons}) AND samples >= 0)"
    )
  end
end
