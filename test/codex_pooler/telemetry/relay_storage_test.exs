defmodule CodexPooler.Telemetry.RelayStorageTest do
  use CodexPooler.DataCase, async: false

  alias CodexPooler.Telemetry.{Relay, RelayEvent}

  setup do
    :ok = Relay.refresh_heartbeat("relay-runtime")
  end

  test "a missing or stale heartbeat refuses inserts without borrowing another writer" do
    assert {:error, :stale_heartbeat} = Relay.insert("stale_sweep", %{}, 1, %{}, "missing-writer")

    Repo.query!(
      "UPDATE telemetry_relay_heartbeats SET heartbeat_at = NOW() - INTERVAL '2 minutes' WHERE owner = $1",
      ["relay-runtime"]
    )

    :ok = Relay.refresh_heartbeat("another-writer")
    assert {:error, :stale_heartbeat} = Relay.insert("stale_sweep", %{})
    assert Repo.aggregate(RelayEvent, :count) == 0
    :ok = Relay.refresh_heartbeat("relay-runtime")
    assert {:ok, _} = Relay.insert("stale_sweep", %{})
  end

  test "inserts allowlisted bounded events and rejects invalid rows" do
    assert {:ok, %RelayEvent{event: "stale_sweep", count: 2}} =
             Relay.insert("stale_sweep", %{"via" => "in_process"}, 2)

    assert {:error, changeset} = Relay.insert("unknown", %{}, 1)
    assert %{event: ["is invalid"]} = errors_on(changeset)
    assert {:error, _} = Relay.insert("stale_sweep", Map.new(1..17, &{"k#{&1}", "v"}), 1)
    assert {:error, _} = Relay.insert("stale_sweep", %{}, -1)
  end

  test "claim marks rows and returns only unclaimed recent rows" do
    assert {:ok, _} = Relay.insert("quota_cycle_decision", %{}, 1)
    assert {:ok, rows} = Relay.claim(10, "owner-a")
    assert length(rows) == 1
    assert hd(rows).claimed_by == "owner-a"
    assert {:ok, []} = Relay.claim(10, "owner-b")
  end

  test "claim leases exclude fresh rows and reclaim stale rows" do
    now = DateTime.utc_now()

    fresh =
      Repo.insert!(%RelayEvent{
        event: "stale_sweep",
        labels: %{},
        count: 1,
        inserted_at: now,
        claimed_at: now,
        claimed_by: "old"
      })

    stale =
      Repo.insert!(%RelayEvent{
        event: "stale_sweep",
        labels: %{},
        count: 1,
        inserted_at: now,
        claimed_at: DateTime.add(now, -61, :second),
        claimed_by: "old"
      })

    assert {:ok, [claimed]} = Relay.claim(10, "new")
    assert claimed.id == stale.id
    assert claimed.claimed_by == "new"
    assert Repo.get!(RelayEvent, fresh.id).claimed_by == "old"
  end

  test "concurrent claimers receive disjoint rows" do
    for _ <- 1..4,
        do:
          Repo.insert!(%RelayEvent{
            event: "stale_sweep",
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
      event: "stale_sweep",
      labels: %{},
      count: 1,
      inserted_at: DateTime.add(now, -3601, :second)
    })

    Repo.insert!(%RelayEvent{
      event: "stale_sweep",
      labels: %{},
      count: 1,
      inserted_at: DateTime.add(now, -86_401, :second)
    })

    assert {2, _} = Relay.expire_counted()
    assert {0, _} = Relay.prune()
  end

  test "transaction rollback leaves no relay rows" do
    assert {:error, :rollback} =
             Repo.transaction(fn ->
               {:ok, _} = Relay.insert("interrupted", %{}, 1)
               Repo.rollback(:rollback)
             end)

    assert Repo.aggregate(RelayEvent, :count) == 0
  end
end
