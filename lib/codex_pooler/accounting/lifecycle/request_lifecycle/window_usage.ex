defmodule CodexPooler.Accounting.RequestLifecycle.WindowUsage do
  @moduledoc false

  alias CodexPooler.Repo

  @type usage_window :: atom()
  @type windows :: keyword(DateTime.t()) | %{usage_window() => DateTime.t()}
  @type window_usage :: %{
          required(:effective_request_count) => non_neg_integer(),
          required(:known_total_tokens) => non_neg_integer(),
          required(:provisional_total_tokens) => non_neg_integer(),
          required(:pending_total_tokens) => non_neg_integer(),
          required(:effective_total_tokens) => non_neg_integer(),
          required(:effective_cost_micros) => Decimal.t()
        }

  @spec window_usages(Ecto.UUID.t(), windows()) :: %{usage_window() => window_usage()}
  def window_usages(api_key_id, windows),
    do: window_usages(api_key_id, windows, DateTime.utc_now())

  @spec window_usages(Ecto.UUID.t(), windows(), DateTime.t()) ::
          %{usage_window() => window_usage()}
  def window_usages(api_key_id, windows, %DateTime{} = as_of) do
    windows = Enum.reject(windows, fn {_window, since} -> is_nil(since) end)

    if windows == [] do
      %{}
    else
      api_key_id
      |> query_windows(Enum.map(windows, &elem(&1, 1)), as_of)
      |> Map.new(fn [ordinal, known, provisional, admissions, cost, pending] ->
        {window, _since} = Enum.at(windows, ordinal - 1)

        {window,
         %{
           effective_request_count: admissions,
           known_total_tokens: known,
           provisional_total_tokens: provisional,
           pending_total_tokens: pending,
           effective_total_tokens: known + provisional + pending,
           effective_cost_micros: cost
         }}
      end)
    end
  end

  defp query_windows(api_key_id, starts, as_of) do
    # Both minute edges, full buckets and the relational outstanding set use
    # one PostgreSQL snapshot. A release/settlement ends a reservation by
    # identity, including a voided terminal, never by a signed window delta.
    %{rows: rows} =
      Repo.query!(
        """
        WITH bounds AS (
          SELECT ordinal, since, $3::timestamptz AS as_of,
            CASE WHEN since = date_trunc('minute', since) THEN since
              ELSE date_trunc('minute', since) + interval '1 minute' END AS full_since,
            date_trunc('minute', $3::timestamptz) AS full_until
          FROM unnest($2::timestamptz[]) WITH ORDINALITY AS windows(since, ordinal)
        ), edge_requests AS (
          SELECT DISTINCT e.request_id FROM public.ledger_entries e CROSS JOIN bounds b
          WHERE e.api_key_id = $1::uuid AND e.occurred_at >= b.since AND e.occurred_at <= b.as_of
            AND (e.occurred_at < b.full_since OR e.occurred_at >= b.full_until)
        ), edge_events AS (
          SELECT b.ordinal, v.* FROM edge_requests r
          CROSS JOIN LATERAL (
            SELECT array_agg(e) AS entries FROM public.ledger_entries e WHERE e.request_id = r.request_id
          ) history
          CROSS JOIN LATERAL public.api_key_usage_events(history.entries) v
          CROSS JOIN bounds b
          WHERE v.api_key_id = $1::uuid AND v.occurred_at >= b.since AND v.occurred_at <= b.as_of
            AND (v.occurred_at < b.full_since OR v.occurred_at >= b.full_until)
        ), components AS (
          SELECT ordinal, known_total_tokens, provisional_total_tokens, admission_count, known_cost_micros
          FROM edge_events
          UNION ALL
          SELECT b.ordinal, k.known_total_tokens, k.provisional_total_tokens, k.admission_count, k.known_cost_micros
          FROM public.api_key_usage_buckets k CROSS JOIN bounds b
          WHERE k.api_key_id = $1::uuid AND k.bucket_started_at >= b.full_since
            AND k.bucket_started_at < b.full_until
        ), pending AS MATERIALIZED (
          SELECT COALESCE(SUM(r.total_tokens), 0)::bigint AS tokens
          FROM public.ledger_entries r
          WHERE r.api_key_id = $1::uuid AND r.entry_kind = 'reservation'
            AND r.amount_status = 'recorded' AND r.occurred_at <= $3::timestamptz
            AND NOT EXISTS (
              SELECT 1 FROM public.ledger_entries t WHERE t.request_id = r.request_id
                AND t.entry_kind IN ('release', 'settlement')
            )
        )
        SELECT b.ordinal, COALESCE(SUM(known_total_tokens), 0)::bigint,
          COALESCE(SUM(provisional_total_tokens), 0)::bigint,
          COALESCE(SUM(admission_count), 0)::bigint,
          COALESCE(SUM(known_cost_micros), 0), (SELECT tokens FROM pending)
        FROM bounds b LEFT JOIN components c ON c.ordinal = b.ordinal
        GROUP BY b.ordinal ORDER BY b.ordinal
        """,
        [Ecto.UUID.dump!(api_key_id), starts, as_of]
      )

    rows
  end
end
