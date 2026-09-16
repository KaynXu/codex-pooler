defmodule CodexPooler.Telemetry.RelayStorageTest do
  use CodexPooler.DataCase, async: false

  alias CodexPooler.Telemetry.{Relay, RelayEvent}

  setup do
    :ok = Relay.refresh_heartbeat("relay-runtime")
  end

  test "expiry atomically counts lost rows and aggregate samples exactly once" do
    Repo.insert!(%RelayEvent{
      event: "stream_outcome",
      labels: %{},
      count: 7,
      inserted_at: DateTime.add(DateTime.utc_now(), -3700)
    })

    assert {1, _} = Relay.expire_counted()

    assert %{rows: [[1, 7]]} =
             Repo.query!(
               "SELECT rows, samples FROM telemetry_relay_losses WHERE reason='expired_unclaimed'"
             )

    assert {0, _} = Relay.expire_counted()

    assert %{rows: [[1, 7]]} =
             Repo.query!(
               "SELECT rows, samples FROM telemetry_relay_losses WHERE reason='expired_unclaimed'"
             )
  end

  test "loss checkpoints are idempotent and daily prune counts unclaimed samples" do
    assert {:ok, :ok} = Relay.checkpoint_loss("writer", "buffer_overflow", 5)
    assert {:ok, :ok} = Relay.checkpoint_loss("writer", "buffer_overflow", 5)
    assert {:ok, :ok} = Relay.checkpoint_loss("writer", "buffer_overflow", 8)

    assert %{rows: [[0, 8]]} =
             Repo.query!(
               "SELECT rows,samples FROM telemetry_relay_losses WHERE reason='buffer_overflow'"
             )

    Repo.insert!(%RelayEvent{
      event: "stream_outcome",
      count: 9,
      inserted_at: DateTime.add(DateTime.utc_now(), -90_000)
    })

    assert {1, _} = Relay.prune()

    assert %{rows: [[1, 9]]} =
             Repo.query!(
               "SELECT rows,samples FROM telemetry_relay_losses WHERE reason='expired_unclaimed'"
             )
  end

  test "expiry rollback restores rows and loss totals together" do
    row =
      Repo.insert!(%RelayEvent{
        event: "stream_outcome",
        count: 7,
        inserted_at: DateTime.add(DateTime.utc_now(), -3700)
      })

    assert {:error, :rollback} =
             Repo.transaction(fn ->
               Relay.expire_counted()
               Repo.rollback(:rollback)
             end)

    assert Repo.get!(RelayEvent, row.id)
    assert %{rows: [[0]]} = Repo.query!("SELECT count(*) FROM telemetry_relay_losses")
  end

  test "independent cleaners and claimer preserve disjoint ownership and exact loss" do
    alias CodexPooler.UnboxedFixture
    ids = for _ <- 1..3, do: Ecto.UUID.generate()

    UnboxedFixture.register_unboxed_cleanup!(fn ->
      Repo.delete_all(from e in RelayEvent, where: e.id in ^ids)
      Repo.query!("DELETE FROM telemetry_relay_losses WHERE reason='expired_unclaimed'")
    end)

    UnboxedFixture.run_unboxed(fn ->
      for {id, count, age} <- Enum.zip([ids, [7, 9, 5], [-3700, -3700, 0]]) do
        Repo.insert!(%RelayEvent{
          id: id,
          event: "stream_outcome",
          count: count,
          inserted_at: DateTime.add(DateTime.utc_now(), age)
        })
      end
    end)

    parent = self()
    first_id = hd(ids)

    first =
      Task.async(fn ->
        UnboxedFixture.run_unboxed(fn ->
          Repo.transaction(fn ->
            Repo.one!(from e in RelayEvent, where: e.id == ^first_id, lock: "FOR UPDATE")
            %{rows: [[backend]]} = Repo.query!("SELECT pg_backend_pid()")
            send(parent, {:locked, self(), backend})

            receive do
              :release -> Relay.expire_counted()
            end
          end)
        end)
      end)

    assert_receive {:locked, holder, first_backend}, 15_000

    on_exit(fn ->
      send(holder, :release)
      if Process.alive?(first.pid), do: Task.shutdown(first, :brutal_kill)
    end)

    second_backend =
      UnboxedFixture.run_unboxed(fn ->
        %{rows: [[backend]]} = Repo.query!("SELECT pg_backend_pid()")
        assert {1, _} = Relay.expire_counted()
        assert {:ok, [%RelayEvent{count: 5}]} = Relay.claim()
        backend
      end)

    refute first_backend == second_backend
    send(holder, :release)
    assert {:ok, {1, _}} = Task.await(first, 15_000)

    UnboxedFixture.run_unboxed(fn ->
      assert %{rows: [[2, 16]]} =
               Repo.query!(
                 "SELECT rows,samples FROM telemetry_relay_losses WHERE reason='expired_unclaimed'"
               )

      assert {:ok, []} = Relay.claim()
    end)
  end

  test "heartbeat pruning keeps live writer loss checkpoints" do
    assert {:ok, :ok} = Relay.checkpoint_loss("relay-runtime", "buffer_overflow", 2)
    Repo.query!("UPDATE telemetry_relay_loss_checkpoints SET updated_at=NOW()-interval '8 days'")
    assert :ok = Relay.prune_heartbeats()
    assert %{rows: [[1]]} = Repo.query!("SELECT count(*) FROM telemetry_relay_loss_checkpoints")
    Repo.query!("UPDATE telemetry_relay_heartbeats SET heartbeat_at=NOW()-interval '8 days'")
    assert :ok = Relay.prune_heartbeats()
    assert %{rows: [[0]]} = Repo.query!("SELECT count(*) FROM telemetry_relay_loss_checkpoints")
  end

  test "a missing or stale heartbeat refuses inserts without borrowing another writer" do
    assert {:error, :stale_heartbeat} =
             Relay.insert("pre_attempt_release", %{}, 1, %{}, "missing-writer")

    Repo.query!(
      "UPDATE telemetry_relay_heartbeats SET heartbeat_at = NOW() - INTERVAL '2 minutes' WHERE owner = $1",
      ["relay-runtime"]
    )

    :ok = Relay.refresh_heartbeat("another-writer")
    assert {:error, :stale_heartbeat} = Relay.insert("pre_attempt_release", %{})
    assert Repo.aggregate(RelayEvent, :count) == 0
    :ok = Relay.refresh_heartbeat("relay-runtime")
    assert {:ok, _} = Relay.insert("pre_attempt_release", %{})
  end

  test "inserts allowlisted bounded events and rejects invalid rows" do
    assert {:ok, %RelayEvent{event: "pre_attempt_release", count: 2}} =
             Relay.insert("pre_attempt_release", %{"via" => "in_process"}, 2)

    assert {:error, changeset} = Relay.insert("unknown", %{}, 1)
    assert %{event: ["is invalid"]} = errors_on(changeset)
    assert {:error, _} = Relay.insert("pre_attempt_release", Map.new(1..17, &{"k#{&1}", "v"}), 1)
    assert {:error, _} = Relay.insert("pre_attempt_release", %{}, -1)
  end

  test "claim marks rows and returns only unclaimed recent rows" do
    assert {:ok, _} = Relay.insert("quota_cycle_decision", %{}, 1)
    assert {:ok, rows} = Relay.claim(10, "owner-a")
    assert length(rows) == 1
    assert hd(rows).claimed_by == "owner-a"
    assert {:ok, []} = Relay.claim(10, "owner-b")
  end

  test "a consumer killed after committed claim never makes the row reclaimable" do
    alias CodexPooler.UnboxedFixture
    id = Ecto.UUID.generate()

    UnboxedFixture.register_unboxed_cleanup!(fn ->
      Repo.delete_all(from e in RelayEvent, where: e.id == ^id)
    end)

    UnboxedFixture.run_unboxed(fn ->
      Repo.insert!(%RelayEvent{
        id: id,
        event: "stream_outcome",
        count: 4,
        inserted_at: DateTime.utc_now()
      })
    end)

    parent = self()

    consumer =
      spawn(fn ->
        result = UnboxedFixture.run_unboxed(fn -> Relay.claim(100, "synthetic-dead-consumer") end)
        send(parent, {:claimed, self(), result})

        receive do
          :die -> exit(:kill)
        end
      end)

    on_exit(fn -> if Process.alive?(consumer), do: Process.exit(consumer, :kill) end)
    monitor = Process.monitor(consumer)
    assert_receive {:claimed, ^consumer, {:ok, [%RelayEvent{id: ^id}]}}, 15_000
    send(consumer, :die)
    assert_receive {:DOWN, ^monitor, :process, ^consumer, :kill}
    assert {:ok, []} = UnboxedFixture.run_unboxed(fn -> Relay.claim(100, "replacement") end)
  end

  test "claim leases exclude already claimed rows" do
    now = DateTime.utc_now()

    fresh =
      Repo.insert!(%RelayEvent{
        event: "pre_attempt_release",
        labels: %{},
        count: 1,
        inserted_at: now,
        claimed_at: now,
        claimed_by: "old"
      })

    stale =
      Repo.insert!(%RelayEvent{
        event: "pre_attempt_release",
        labels: %{},
        count: 1,
        inserted_at: now,
        claimed_at: DateTime.add(now, -61, :second),
        claimed_by: "old"
      })

    assert {:ok, []} = Relay.claim(10, "new")
    assert Repo.get!(RelayEvent, stale.id).claimed_by == "old"
    assert Repo.get!(RelayEvent, fresh.id).claimed_by == "old"
  end

  test "concurrent claimers receive disjoint rows" do
    for _ <- 1..4,
        do:
          Repo.insert!(%RelayEvent{
            event: "pre_attempt_release",
            labels: %{},
            count: 1,
            inserted_at: DateTime.utc_now()
          })

    parent = self()

    tasks =
      for owner <- ["a", "b"] do
        Task.async(fn -> send(parent, {:claimed, owner, Relay.claim(10, owner)}) end)
      end

    Enum.each(tasks, &Task.await(&1, 5_000))

    claims =
      for _ <- tasks do
        assert_receive {:claimed, _owner, result}
        result
      end

    ids = Enum.flat_map(claims, fn {:ok, rows} -> Enum.map(rows, & &1.id) end)
    assert length(ids) == length(Enum.uniq(ids))
  end

  test "expiry removes unclaimed rows older than one hour and prune removes day-old rows" do
    now = DateTime.utc_now()

    Repo.insert!(%RelayEvent{
      event: "pre_attempt_release",
      labels: %{},
      count: 1,
      inserted_at: DateTime.add(now, -3601, :second)
    })

    Repo.insert!(%RelayEvent{
      event: "pre_attempt_release",
      labels: %{},
      count: 1,
      inserted_at: DateTime.add(now, -86_401, :second)
    })

    assert {2, _} = Relay.expire_counted()
    assert {0, _} = Relay.prune()
  end

  test "prune removes old claimed rows and preserves fresh claimed rows" do
    now = DateTime.utc_now()

    old =
      Repo.insert!(%RelayEvent{
        event: "pre_attempt_release",
        labels: %{},
        count: 1,
        inserted_at: DateTime.add(now, -86_401, :second),
        claimed_at: DateTime.add(now, -86_400, :second),
        claimed_by: "old"
      })

    fresh =
      Repo.insert!(%RelayEvent{
        event: "pre_attempt_release",
        labels: %{},
        count: 1,
        inserted_at: now,
        claimed_at: now,
        claimed_by: "fresh"
      })

    assert {1, _} = Relay.prune()
    assert Repo.get(RelayEvent, old.id) == nil
    assert Repo.get(RelayEvent, fresh.id)
  end

  test "transaction rollback leaves no relay rows" do
    assert {:error, :rollback} =
             Repo.transaction(fn ->
               {:ok, _} = Relay.insert("stream_outcome", %{}, 1)
               Repo.rollback(:rollback)
             end)

    assert Repo.aggregate(RelayEvent, :count) == 0
  end
end
