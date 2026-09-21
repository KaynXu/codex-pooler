defmodule CodexPooler.Platform.HeartbeatWriteBudgetTest do
  use CodexPooler.DataCase, async: false
  import CodexPooler.UnboxedFixture
  alias CodexPooler.Platform.InstancePresence
  alias CodexPooler.Telemetry.Relay

  @tag capture_log: true
  @tag slow:
         "three real PostgreSQL heartbeat writes each exhaust their one-second production query budget"
  test "heartbeat writes release their checkout before a slow database operation completes" do
    suffix = System.unique_integer([:positive])
    function = "heartbeat_budget_#{suffix}"
    tables = ~w(instance_presences telemetry_relay_consumers telemetry_relay_heartbeats)

    register_unboxed_cleanup!(fn ->
      for table <- tables, do: Repo.query!("DROP TRIGGER IF EXISTS #{function} ON #{table}")
      Repo.query!("DROP FUNCTION IF EXISTS #{function}()")
    end)

    run_unboxed(fn ->
      Repo.query!(
        "CREATE FUNCTION #{function}() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN PERFORM pg_sleep(10); RETURN NEW; END $$"
      )

      for table <- tables,
          do:
            Repo.query!(
              "CREATE TRIGGER #{function} BEFORE INSERT ON #{table} FOR EACH ROW EXECUTE FUNCTION #{function}()"
            )
    end)

    calls = [
      fn ->
        InstancePresence.record_heartbeat(
          InstancePresence.Identity.new("budget-test", Ecto.UUID.generate())
        )
      end,
      fn -> Relay.refresh_heartbeat("budget-#{suffix}") end,
      fn -> Relay.consumer_heartbeat("budget-#{suffix}") end
    ]

    for call <- calls do
      started = System.monotonic_time(:millisecond)

      result =
        run_unboxed(fn ->
          try do
            call.()
          rescue
            _ in [DBConnection.ConnectionError, Postgrex.Error] -> :bounded_failure
          catch
            :exit, _ -> :bounded_failure
          end
        end)

      assert result == :bounded_failure or match?({:error, _}, result)
      assert System.monotonic_time(:millisecond) - started < 8_000
    end
  end
end
