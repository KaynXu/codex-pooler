defmodule CodexPooler.Accounting.Usage.RollingCostSummaryParityTest do
  use CodexPooler.DataCase, async: false

  import CodexPooler.PoolerFixtures

  alias CodexPooler.Accounting
  alias CodexPooler.Repo

  @as_of ~U[2026-09-17 12:00:00.000000Z]
  @in_window ~U[2026-09-10 09:30:00.000000Z]
  @micros_per_usd Decimal.new(1_000_000)

  setup do
    pool = pool_fixture()
    %{api_key: api_key} = active_api_key_fixture(pool)
    %{pool: pool, api_key: api_key, auth: %{pool: pool, api_key: api_key}}
  end

  test "no qualifying settlements stay unpriced", context do
    assert_parity(context, @as_of, 0)
  end

  test "the 28-day UTC window includes both calendar edges", context do
    start_date = @as_of |> DateTime.add(-27, :day) |> DateTime.to_date()
    end_date = DateTime.to_date(@as_of)
    start_at = DateTime.new!(start_date, ~T[00:00:00.000000], "Etc/UTC")
    end_before = DateTime.new!(Date.add(end_date, 1), ~T[00:00:00.000000], "Etc/UTC")

    for occurred_at <- [
          start_at,
          DateTime.add(start_at, -1, :microsecond),
          DateTime.add(end_before, -1, :microsecond),
          end_before
        ] do
      settlement(context.auth, occurred_at: occurred_at, settled_cost_micros: 250_000)
    end

    assert_parity(context, @as_of, 2)
  end

  test "zero, fractional, and voided priced settlements retain exact semantics", context do
    settlement(context.auth, settled_cost_micros: 0)
    settlement(context.auth, settled_cost_micros: "1234.567890123")
    settlement(context.auth, settled_cost_micros: "0.000000499")
    settlement(context.auth, amount_status: "voided", settled_cost_micros: 4_000_000)

    assert_parity(context, @as_of, 4)
  end

  test "unpriced rows, other kinds, and unknown usage stay excluded", context do
    settlement(context.auth, details: %{})
    settlement(context.auth, details: %{"settled_cost_micros" => nil})
    settlement(context.auth, entry_kind: "reservation")
    settlement(context.auth, usage_status: "usage_unknown")

    assert_parity(context, @as_of, 0)
  end

  test "other api keys and pools stay excluded", %{pool: pool} = context do
    %{api_key: other_key} = active_api_key_fixture(pool)
    other_pool = pool_fixture()

    settlement(%{pool: pool, api_key: other_key}, settled_cost_micros: 7_000_000)
    settlement(%{pool: other_pool, api_key: context.api_key}, settled_cost_micros: 9_000_000)
    settlement(context.auth, settled_cost_micros: 1_000_000)

    assert_parity(context, @as_of, 1)
  end

  defp settlement(auth, attrs) do
    attrs = Map.new(attrs)
    micros = Map.get(attrs, :settled_cost_micros, 1_250_000)
    request = request_fixture(auth)

    ledger_entry_fixture(
      request,
      attrs
      |> Map.put_new(:details, %{"settled_cost_micros" => to_string(micros)})
      |> Map.put(:settled_cost_micros, to_string(micros))
      |> Map.put_new(:occurred_at, @in_window)
    )
  end

  defp assert_parity(context, as_of, expected_rows) do
    {count, sum} = legacy_oracle(context.pool.id, context.api_key.id, as_of)
    assert count == expected_rows

    expected_status = if count > 0, do: "priced", else: "unpriced"

    expected_usd =
      if count > 0,
        do: sum |> Decimal.div(@micros_per_usd) |> Decimal.round(6),
        else: nil

    assert {:ok, usage} =
             Accounting.build_api_key_self_usage(context.pool, context.api_key, as_of: as_of)

    assert usage.total_cost_status == expected_status

    case expected_usd do
      nil -> assert usage.total_cost_usd == nil
      %Decimal{} -> assert Decimal.equal?(usage.total_cost_usd, expected_usd)
    end

    assert {:ok, v1} =
             Accounting.build_v1_usage_for_api_key(context.pool, context.api_key, as_of: as_of)

    assert v1.total_cost_status == expected_status
    assert v1.total_cost_usd == if(expected_usd, do: Decimal.to_float(expected_usd), else: 0.0)
  end

  defp legacy_oracle(pool_id, api_key_id, as_of) do
    start_date = as_of |> DateTime.add(-27, :day) |> DateTime.to_date()
    end_date = DateTime.to_date(as_of)
    Repo.query!("SET LOCAL TimeZone = 'UTC'")

    %{rows: [[count, sum]]} =
      Repo.query!(
        """
        SELECT count(entry.id), coalesce(sum(entry.settled_cost_micros), 0)
        FROM ledger_entries entry
        JOIN requests request ON request.id = entry.request_id
        WHERE request.pool_id = $1
          AND entry.api_key_id = $2
          AND entry.entry_kind = 'settlement'
          AND entry.usage_status = 'usage_known'
          AND entry.occurred_at::date >= $3
          AND entry.occurred_at::date <= $4
          AND (entry.details->>'settled_cost_micros') IS NOT NULL
        """,
        [Ecto.UUID.dump!(pool_id), Ecto.UUID.dump!(api_key_id), start_date, end_date]
      )

    {count, sum}
  end
end
