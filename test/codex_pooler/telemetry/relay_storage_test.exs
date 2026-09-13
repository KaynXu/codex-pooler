defmodule CodexPooler.Telemetry.RelayStorageTest do
  use CodexPooler.DataCase, async: false

  alias CodexPooler.Telemetry.{Relay, RelayEvent}

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

  test "expiry removes unclaimed rows older than one hour and prune removes day-old rows" do
    now = DateTime.utc_now()
    Repo.insert!(%RelayEvent{event: "stale_sweep", labels: %{}, count: 1, inserted_at: DateTime.add(now, -3601, :second)})
    Repo.insert!(%RelayEvent{event: "stale_sweep", labels: %{}, count: 1, inserted_at: DateTime.add(now, -86_401, :second)})
    assert {2, _} = Relay.expire_counted()
    assert {0, _} = Relay.prune()
  end

  test "transaction rollback leaves no relay rows" do
    assert {:error, :rollback} = Repo.transaction(fn ->
      {:ok, _} = Relay.insert("interrupted", %{}, 1)
      Repo.rollback(:rollback)
    end)
    assert Repo.aggregate(RelayEvent, :count) == 0
  end
end
