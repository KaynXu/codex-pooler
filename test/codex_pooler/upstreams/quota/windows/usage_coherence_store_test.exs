defmodule CodexPooler.Upstreams.Quota.Windows.UsageCoherenceStoreTest do
  use CodexPooler.DataCase, async: false

  import CodexPooler.PoolerFixtures

  alias CodexPooler.Upstreams.Quota.Windows
  alias CodexPooler.Upstreams.Quota.Windows.EvidenceStore
  alias CodexPooler.Upstreams.Quota.Windows.Routing
  alias CodexPooler.Upstreams.Quota.Windows.UsageCoherence

  @key "__quota_usage_coherence_v1"

  test "two coherent usage readings supersede a fresh header exhaustion in the effective view" do
    %{identity: identity} = active_upstream_assignment_fixture(pool_fixture(), %{})
    now = DateTime.utc_now() |> DateTime.truncate(:microsecond)
    reset_at = DateTime.add(now, 900, :second) |> DateTime.truncate(:second)

    assert {:ok, _headers} =
             record!(
               identity,
               "codex_response_headers",
               "100",
               reset_at,
               DateTime.add(now, -90, :second),
               %{}
             )

    assert [%{used_percent: exhausted}] = Windows.list_quota_windows(identity, now)
    assert Decimal.equal?(exhausted, Decimal.new("100"))

    assert {:ok, first} =
             record!(
               identity,
               "codex_usage_api",
               "20",
               reset_at,
               DateTime.add(now, -60, :second),
               safe_status()
             )

    assert first.metadata[@key]["count"] == 1
    refute UsageCoherence.confirmed?(first, now)

    # One lower reading is retained beside the exhausted row, which still wins.
    assert [%{source: "codex_response_headers"}] = Windows.list_quota_windows(identity, now)

    assert {:ok, second} =
             record!(
               identity,
               "codex_usage_api",
               "20",
               reset_at,
               DateTime.add(now, -30, :second),
               safe_status()
             )

    assert second.metadata[@key]["count"] == 2
    assert UsageCoherence.confirmed?(second, now)

    assert [%{source: "codex_usage_api", used_percent: recovered}] =
             Windows.list_quota_windows(identity, now)

    assert Decimal.equal?(recovered, Decimal.new("20"))

    # A denied reading clears the confirmation and the exhausted row wins again.
    assert {:ok, denied} =
             record!(identity, "codex_usage_api", "20", reset_at, now, %{
               "rate_limit_allowed" => false,
               "rate_limit_reached" => true
             })

    refute Map.has_key?(denied.metadata, @key)
    assert [%{source: "codex_response_headers"}] = Windows.list_quota_windows(identity, now)
  end

  test "a lower usage reading without provider permission facts never confirms" do
    %{identity: identity} = active_upstream_assignment_fixture(pool_fixture(), %{})
    now = DateTime.utc_now() |> DateTime.truncate(:microsecond)
    reset_at = DateTime.add(now, 900, :second) |> DateTime.truncate(:second)

    assert {:ok, _headers} =
             record!(
               identity,
               "codex_response_headers",
               "100",
               reset_at,
               DateTime.add(now, -90, :second),
               %{}
             )

    for offset <- [-60, -30] do
      assert {:ok, row} =
               record!(
                 identity,
                 "codex_usage_api",
                 "20",
                 reset_at,
                 DateTime.add(now, offset, :second),
                 %{}
               )

      refute Map.has_key?(row.metadata, @key)
    end

    assert [%{source: "codex_response_headers"}] = Windows.list_quota_windows(identity, now)
  end

  for {label, window_minutes} <- [{"5h", 300}, {"30d", 43_200}] do
    test "repeated permitted zero-percent #{label} account evidence stays fresh" do
      %{identity: identity} = active_upstream_assignment_fixture(pool_fixture(), %{})
      t0 = DateTime.utc_now() |> DateTime.add(-20, :minute) |> DateTime.truncate(:microsecond)
      reset_at = DateTime.add(t0, unquote(window_minutes), :minute)

      assert {:ok, first} =
               record!(
                 identity,
                 "codex_usage_api",
                 "0",
                 reset_at,
                 t0,
                 safe_status(),
                 unquote(window_minutes)
               )

      t1 = DateTime.add(t0, 10, :minute)
      refreshed_reset_at = DateTime.add(reset_at, 2, :minute)

      assert {:ok, second} =
               record!(
                 identity,
                 "codex_usage_api",
                 "0",
                 refreshed_reset_at,
                 t1,
                 safe_status(),
                 unquote(window_minutes)
               )

      assert second.id == first.id
      assert Decimal.equal?(second.used_percent, Decimal.new("0"))
      assert second.active_limit == nil
      assert second.credits == nil
      assert DateTime.compare(second.observed_at, t1) == :eq
      assert DateTime.compare(second.last_sync_at, t1) == :eq

      after_original_ttl = DateTime.add(t0, 16, :minute)

      assert %{eligible?: true, routing_state: :precise, exclusions: []} =
               Routing.eligibility_from_windows([second], at: after_original_ttl)
    end
  end

  test "a reset correction without explicit provider permission cannot refresh a primary zero" do
    %{identity: identity} = active_upstream_assignment_fixture(pool_fixture(), %{})
    t0 = DateTime.utc_now() |> DateTime.add(-20, :minute) |> DateTime.truncate(:microsecond)
    reset_at = DateTime.add(t0, 300, :minute)

    assert {:ok, first} =
             record!(identity, "codex_usage_api", "0", reset_at, t0, %{}, 300)

    t1 = DateTime.add(t0, 10, :minute)

    assert {:ok, retained} =
             record!(
               identity,
               "codex_usage_api",
               "0",
               DateTime.add(reset_at, 2, :minute),
               t1,
               %{},
               300
             )

    assert retained.id == first.id
    assert DateTime.compare(retained.observed_at, t0) == :eq
    assert DateTime.compare(retained.last_sync_at, t0) == :eq
  end

  test "permitted idle primary with a full-window sliding reset stays fresh beyond five minutes" do
    %{identity: identity} = active_upstream_assignment_fixture(pool_fixture(), %{})
    t0 = DateTime.utc_now() |> DateTime.add(-30, :minute) |> DateTime.truncate(:second)
    reset_at = DateTime.add(t0, 300, :minute)

    assert {:ok, first} =
             record!(identity, "codex_usage_api", "0", reset_at, t0, safe_status(), 300)

    for minute <- 1..20 do
      observed_at = DateTime.add(t0, minute, :minute)

      assert {:ok, current} =
               record!(
                 identity,
                 "codex_usage_api",
                 "0",
                 DateTime.add(observed_at, 300, :minute),
                 observed_at,
                 safe_status(),
                 300
               )

      assert current.id == first.id
      assert DateTime.compare(current.observed_at, observed_at) == :eq
      assert DateTime.compare(current.last_sync_at, observed_at) == :eq
      assert DateTime.compare(current.reset_at, reset_at) == :eq
      assert %{eligible?: true} = Routing.eligibility_from_windows([current], at: observed_at)
    end
  end

  test "large reset drift without full-window timing cannot refresh a zero primary" do
    %{identity: identity} = active_upstream_assignment_fixture(pool_fixture(), %{})
    t0 = DateTime.utc_now() |> DateTime.add(-30, :minute) |> DateTime.truncate(:second)
    reset_at = DateTime.add(t0, 300, :minute)
    assert {:ok, _} = record!(identity, "codex_usage_api", "0", reset_at, t0, safe_status(), 300)
    t1 = DateTime.add(t0, 6, :minute)

    assert {:ok, retained} =
             record!(
               identity,
               "codex_usage_api",
               "0",
               DateTime.add(reset_at, 12, :minute),
               t1,
               safe_status(),
               300
             )

    assert DateTime.compare(retained.observed_at, t0) == :eq
    assert DateTime.compare(retained.reset_at, reset_at) == :eq
  end

  test "full-window idle timing cannot erase positive primary consumption" do
    %{identity: identity} = active_upstream_assignment_fixture(pool_fixture(), %{})
    t0 = DateTime.utc_now() |> DateTime.add(-10, :minute) |> DateTime.truncate(:second)
    reset_at = DateTime.add(t0, 300, :minute)
    assert {:ok, _} = record!(identity, "codex_usage_api", "22", reset_at, t0, safe_status(), 300)
    t1 = DateTime.add(t0, 6, :minute)

    assert {:ok, retained} =
             record!(
               identity,
               "codex_usage_api",
               "0",
               DateTime.add(t1, 300, :minute),
               t1,
               safe_status(),
               300
             )

    assert Decimal.equal?(retained.used_percent, 22)
    assert DateTime.compare(retained.reset_at, reset_at) == :eq
  end

  defp record!(identity, source, used_percent, reset_at, observed_at, metadata),
    do: record!(identity, source, used_percent, reset_at, observed_at, metadata, 300)

  defp record!(
         identity,
         source,
         used_percent,
         reset_at,
         observed_at,
         metadata,
         window_minutes
       ) do
    EvidenceStore.record_evidence(
      identity,
      %{
        quota_key: "account",
        quota_scope: "account",
        quota_family: "account",
        window_kind: "primary",
        window_minutes: window_minutes,
        used_percent: Decimal.new(used_percent),
        reset_at: reset_at,
        observed_at: observed_at,
        last_sync_at: observed_at,
        source: source,
        source_precision: "observed",
        freshness_state: "fresh",
        metadata:
          Map.put(metadata, "reset_after_seconds", DateTime.diff(reset_at, observed_at, :second))
      },
      observed_at,
      observed_at
    )
  end

  defp safe_status, do: %{"rate_limit_allowed" => true, "rate_limit_reached" => false}
end
