defmodule CodexPooler.Upstreams.Quota.Windows.IdentityLockOrderTest do
  use ExUnit.Case, async: false
  use CodexPooler.CommittedWriteGuard

  import CodexPooler.PoolerFixtures
  import CodexPooler.UnboxedFixture, only: [register_unboxed_cleanup!: 1]

  alias CodexPooler.Repo
  alias CodexPooler.Upstreams.Lifecycle.IdentitySlotLock
  alias CodexPooler.Upstreams.Quota.Windows.EvidenceStore
  alias Ecto.Adapters.SQL.Sandbox

  @detection_timeout_ms 15_000

  setup do
    identity = unboxed(fn -> upstream_identity_fixture() end)
    register_unboxed_cleanup!(fn -> Repo.delete!(identity) end)
    %{identity: identity}
  end

  test "quota evidence waits for the identity advisory lock without holding its row", %{
    identity: identity
  } do
    assert_lock_order(identity, fn ->
      now = DateTime.utc_now() |> DateTime.truncate(:second)

      EvidenceStore.record_evidence(
        identity,
        %{
          quota_key: "account",
          quota_scope: "account",
          quota_family: "account",
          window_kind: "secondary",
          window_minutes: 10_080,
          used_percent: Decimal.new("22"),
          reset_at: DateTime.add(now, 604_800, :second),
          source: "codex_rate_limit_event",
          source_precision: "observed",
          freshness_state: "fresh",
          metadata: %{}
        },
        now
      )
    end)
  end

  test "lifecycle rows wait for the identity advisory lock before taking FOR UPDATE", %{
    identity: identity
  } do
    assert_lock_order(identity, fn ->
      Repo.transaction(fn -> IdentitySlotLock.lock_identity_rows!([identity]) end)
    end)
  end

  defp assert_lock_order(identity, writer) do
    parent = self()
    barrier = make_ref()

    {transaction_result, waiter} =
      unboxed(fn ->
        {:ok, {result, waiter}} =
          Repo.transaction(fn ->
            Repo.query!("SELECT pg_advisory_xact_lock(hashtextextended($1, 0))", [identity.id])
            %{rows: [[blocker_pid]]} = Repo.query!("SELECT pg_backend_pid()")

            waiter = start_writer(parent, barrier, writer)

            assert_receive {^barrier, :waiter, waiter_pid}, @detection_timeout_ms
            deadline = System.monotonic_time(:millisecond) + @detection_timeout_ms
            wait_result = wait_for_advisory(waiter_pid, blocker_pid, barrier, deadline)

            # The timeout only bounds the red path: an old writer holds KEY SHARE
            # while waiting for our advisory lock, so our FOR UPDATE cannot finish.
            Repo.query!("SET LOCAL lock_timeout = '250ms'")
            Repo.query!("SAVEPOINT row_lock_probe")

            row_result =
              Repo.query("SELECT id FROM upstream_identities WHERE id = $1 FOR UPDATE", [
                Ecto.UUID.dump!(identity.id)
              ])

            # Recover the transaction after an expected old-revision lock timeout.
            Repo.query!("ROLLBACK TO SAVEPOINT row_lock_probe")
            {{wait_result, row_result}, waiter}
          end)

        {result, waiter}
      end)

    assert {:ok, _} = Task.await(waiter, @detection_timeout_ms)
    assert {:waiting_on_identity_advisory, {:ok, %{num_rows: 1}}} = transaction_result
  end

  defp start_writer(parent, barrier, writer) do
    Task.async(fn ->
      unboxed(fn ->
        %{rows: [[waiter_pid]]} = Repo.query!("SELECT pg_backend_pid()")
        send(parent, {barrier, :waiter, waiter_pid})
        result = writer.()
        send(parent, {barrier, :completed})
        result
      end)
    end)
  end

  defp wait_for_advisory(waiter_pid, blocker_pid, barrier, deadline) do
    %{rows: rows} =
      Repo.query!(
        "SELECT wait_event, pg_blocking_pids(pid) FROM pg_stat_activity WHERE pid = $1",
        [waiter_pid]
      )

    cond do
      rows == [["advisory", [blocker_pid]]] ->
        :waiting_on_identity_advisory

      System.monotonic_time(:millisecond) >= deadline ->
        :advisory_wait_not_observed

      true ->
        receive do
          {^barrier, :completed} -> :writer_completed_without_identity_advisory
        after
          0 -> wait_for_advisory(waiter_pid, blocker_pid, barrier, deadline)
        end
    end
  end

  defp unboxed(fun), do: Sandbox.unboxed_run(Repo, fun)
end
