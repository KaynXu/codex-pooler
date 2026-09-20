defmodule CodexPooler.Repo.Migrations.AddApiKeyUsageComponents do
  use Ecto.Migration

  def up do
    # NOWAIT makes failure atomic without retaining one relation lock while
    # waiting for another. The backfill and trigger switch publish together.
    execute("SET LOCAL lock_timeout = '5s'")
    execute("SET LOCAL statement_timeout = '30min'")
    execute("LOCK TABLE public.ledger_entries IN SHARE ROW EXCLUSIVE MODE NOWAIT")
    execute("LOCK TABLE public.api_key_usage_buckets IN ACCESS EXCLUSIVE MODE NOWAIT")

    alter table(:api_key_usage_buckets) do
      add :known_total_tokens, :bigint, null: false, default: 0
      add :provisional_total_tokens, :bigint, null: false, default: 0
      add :admission_count, :bigint, null: false, default: 0
      add :known_cost_micros, :decimal, precision: 30, scale: 9, null: false, default: 0
    end

    flush()
    execute(events_function())
    execute("DROP TRIGGER ledger_entries_sync_api_key_usage_buckets ON public.ledger_entries")

    for operation <- [:insert, :update, :delete] do
      execute(sync_function(operation))
      execute(sync_trigger(operation))
    end

    execute(rebuild_function())
    execute("SELECT public.rebuild_api_key_usage_components()")

    # The retained-history anti-join uses narrow covering indexes; the edge
    # index also includes voided original terminals used by late corrections.
    execute("""
    CREATE INDEX ledger_entries_reservation_key_occurred_idx
    ON public.ledger_entries (api_key_id, occurred_at) INCLUDE (request_id, total_tokens)
    WHERE entry_kind = 'reservation' AND amount_status = 'recorded'
    """)

    execute("""
    CREATE INDEX ledger_entries_terminal_request_idx ON public.ledger_entries (request_id)
    WHERE entry_kind IN ('release', 'settlement')
    """)

    execute("""
    CREATE INDEX ledger_entries_key_occurred_idx
    ON public.ledger_entries (api_key_id, occurred_at) INCLUDE (request_id)
    """)
  end

  def down do
    execute("SET LOCAL lock_timeout = '5s'")
    execute("LOCK TABLE public.ledger_entries IN SHARE ROW EXCLUSIVE MODE NOWAIT")
    execute("LOCK TABLE public.api_key_usage_buckets IN ACCESS EXCLUSIVE MODE NOWAIT")

    drop index(:ledger_entries, [:api_key_id, :occurred_at],
           name: :ledger_entries_reservation_key_occurred_idx
         )

    drop index(:ledger_entries, [:request_id], name: :ledger_entries_terminal_request_idx)

    drop index(:ledger_entries, [:api_key_id, :occurred_at],
           name: :ledger_entries_key_occurred_idx
         )

    for operation <- [:insert, :update, :delete] do
      execute(
        "DROP TRIGGER ledger_entries_usage_components_#{operation} ON public.ledger_entries"
      )

      execute("DROP FUNCTION public.sync_api_key_usage_components_#{operation}()")
    end

    execute("DROP FUNCTION public.rebuild_api_key_usage_components()")
    execute("DROP FUNCTION public.api_key_usage_events(public.ledger_entries[])")

    execute("""
    CREATE TRIGGER ledger_entries_sync_api_key_usage_buckets
    AFTER INSERT OR UPDATE OR DELETE ON public.ledger_entries
    FOR EACH ROW EXECUTE FUNCTION public.sync_api_key_usage_bucket_from_ledger_entry()
    """)

    alter table(:api_key_usage_buckets) do
      remove :known_total_tokens
      remove :provisional_total_tokens
      remove :admission_count
      remove :known_cost_micros
    end
  end

  # The edge reader and projection share this request-local event definition.
  # Pending is relational authority, never a cumulative bucket balance.
  defp events_function do
    """
    CREATE FUNCTION public.api_key_usage_events(p_entries public.ledger_entries[])
    RETURNS TABLE(api_key_id uuid, occurred_at timestamptz,
      known_total_tokens bigint, provisional_total_tokens bigint,
      admission_count bigint, known_cost_micros numeric)
    LANGUAGE sql STABLE SET search_path = pg_catalog, public AS $function$
      WITH entries AS MATERIALIZED (SELECT * FROM unnest(p_entries)),
      reservation AS (
        SELECT * FROM entries WHERE entry_kind = 'reservation' AND amount_status = 'recorded'
        ORDER BY occurred_at, created_at, id LIMIT 1
      ), terminal AS (
        SELECT * FROM entries
        WHERE amount_status = 'recorded' AND entry_kind IN ('settlement', 'release')
        ORDER BY CASE WHEN entry_kind = 'settlement' THEN 0 ELSE 1 END,
          occurred_at, created_at, id LIMIT 1
      ), terminal_time AS (
        SELECT min(occurred_at) AS occurred_at FROM entries
        WHERE entry_kind IN ('settlement', 'release')
      )
      SELECT r.api_key_id, r.occurred_at, 0::bigint, 0::bigint, 1::bigint, 0::numeric
      FROM reservation r
      UNION ALL
      SELECT t.api_key_id, tt.occurred_at,
        CASE WHEN t.entry_kind = 'settlement' AND t.usage_status = 'usage_known'
          THEN COALESCE(t.total_tokens, 0) ELSE 0 END,
        CASE WHEN t.usage_status <> 'not_applicable'
          AND (t.entry_kind = 'release' OR t.usage_status <> 'usage_known')
          AND (t.entry_kind <> 'release' OR NOT EXISTS (
            SELECT 1 FROM entries s WHERE s.entry_kind = 'settlement'
              AND s.usage_status IN ('usage_known', 'not_applicable')))
          AND (EXISTS (SELECT 1 FROM entries WHERE attempt_id IS NOT NULL)
            OR EXISTS (SELECT 1 FROM public.attempts a WHERE a.request_id = t.request_id)
            OR (t.entry_kind = 'settlement' AND t.details->>'estimated_from_reserve' = 'true'))
          THEN COALESCE(r.total_tokens,
            CASE WHEN t.entry_kind = 'settlement' AND
              t.details->>'estimated_from_reserve' = 'true' THEN t.total_tokens END, 0)
          ELSE 0 END,
        0::bigint,
        CASE WHEN t.entry_kind = 'settlement' AND t.usage_status = 'usage_known'
          THEN COALESCE(t.settled_cost_micros, 0) ELSE 0 END
      FROM terminal t CROSS JOIN terminal_time tt LEFT JOIN reservation r ON true
    $function$
    """
  end

  defp sync_trigger(operation) do
    transition =
      case operation do
        :insert -> "NEW TABLE AS new_entries"
        :update -> "OLD TABLE AS old_entries NEW TABLE AS new_entries"
        :delete -> "OLD TABLE AS old_entries"
      end

    """
    CREATE TRIGGER ledger_entries_usage_components_#{operation}
    AFTER #{operation |> Atom.to_string() |> String.upcase()} ON public.ledger_entries
    REFERENCING #{transition}
    FOR EACH STATEMENT EXECUTE FUNCTION public.sync_api_key_usage_components_#{operation}()
    """
  end

  defp sync_function(operation) do
    old_rows =
      if operation == :insert,
        do: "SELECT * FROM public.ledger_entries WHERE false",
        else: "SELECT * FROM old_entries"

    new_rows =
      if operation == :delete,
        do: "SELECT * FROM public.ledger_entries WHERE false",
        else: "SELECT * FROM new_entries"

    """
    CREATE FUNCTION public.sync_api_key_usage_components_#{operation}()
    RETURNS trigger LANGUAGE plpgsql SET search_path = pg_catalog, public AS $function$
    BEGIN
      WITH old_rows AS MATERIALIZED (#{old_rows}),
      new_rows AS MATERIALIZED (#{new_rows}),
      affected AS (SELECT request_id FROM old_rows UNION SELECT request_id FROM new_rows),
      current_rows AS MATERIALIZED (
        SELECT e.* FROM affected a JOIN public.ledger_entries e ON e.request_id = a.request_id
      ), before_rows AS (
        SELECT e.* FROM current_rows e WHERE NOT EXISTS (SELECT 1 FROM new_rows n WHERE n.id = e.id)
        UNION ALL SELECT * FROM old_rows
      ), before_events AS (
        SELECT v.* FROM (SELECT array_agg(e::public.ledger_entries) AS entries FROM before_rows e GROUP BY request_id) r
        CROSS JOIN LATERAL public.api_key_usage_events(r.entries) v
      ), after_events AS (
        SELECT v.* FROM (SELECT array_agg(e::public.ledger_entries) AS entries FROM current_rows e GROUP BY request_id) r
        CROSS JOIN LATERAL public.api_key_usage_events(r.entries) v
      ), deltas AS (
        SELECT api_key_id, occurred_at, 0::bigint AS requests, 0::bigint AS tokens, 0::numeric AS cost,
          -known_total_tokens AS known, -provisional_total_tokens AS provisional,
          -admission_count AS admissions, -known_cost_micros AS known_cost FROM before_events
        UNION ALL SELECT api_key_id, occurred_at, 0, 0, 0,
          known_total_tokens, provisional_total_tokens, admission_count, known_cost_micros FROM after_events
        UNION ALL #{legacy_delta("old_rows", -1)}
        UNION ALL #{legacy_delta("new_rows", 1)}
      ), grouped AS (
        SELECT d.api_key_id, date_trunc('minute', d.occurred_at) AS bucket_started_at,
          SUM(requests) AS requests, SUM(tokens) AS tokens, SUM(cost) AS cost,
          SUM(known) AS known, SUM(provisional) AS provisional,
          SUM(admissions) AS admissions, SUM(known_cost) AS known_cost
        FROM deltas d WHERE EXISTS (SELECT 1 FROM public.api_keys k WHERE k.id = d.api_key_id)
        GROUP BY d.api_key_id, date_trunc('minute', d.occurred_at)
      )
      INSERT INTO public.api_key_usage_buckets AS b
        (api_key_id, bucket_started_at, effective_request_count, effective_total_tokens,
         effective_cost_micros, known_total_tokens, provisional_total_tokens, admission_count,
         known_cost_micros, created_at, updated_at)
      SELECT api_key_id, bucket_started_at, requests, tokens, cost, known, provisional,
        admissions, known_cost, statement_timestamp(), statement_timestamp()
      FROM grouped ORDER BY api_key_id, bucket_started_at
      ON CONFLICT (api_key_id, bucket_started_at) DO UPDATE SET
        effective_request_count = b.effective_request_count + EXCLUDED.effective_request_count,
        effective_total_tokens = b.effective_total_tokens + EXCLUDED.effective_total_tokens,
        effective_cost_micros = b.effective_cost_micros + EXCLUDED.effective_cost_micros,
        known_total_tokens = b.known_total_tokens + EXCLUDED.known_total_tokens,
        provisional_total_tokens = b.provisional_total_tokens + EXCLUDED.provisional_total_tokens,
        admission_count = b.admission_count + EXCLUDED.admission_count,
        known_cost_micros = b.known_cost_micros + EXCLUDED.known_cost_micros,
        updated_at = statement_timestamp();
      RETURN NULL;
    END
    $function$
    """
  end

  defp legacy_delta(rows, sign) do
    """
    SELECT api_key_id, occurred_at,
      #{sign} * CASE WHEN entry_kind = 'release' THEN -request_count ELSE request_count END,
      #{sign} * CASE
        WHEN entry_kind = 'release' THEN -COALESCE(total_tokens, 0)
        WHEN entry_kind = 'settlement' AND usage_status <> 'usage_known' THEN 0
        ELSE COALESCE(total_tokens, 0) END,
      #{sign} * CASE
        WHEN entry_kind = 'release' THEN -estimated_cost_micros
        WHEN entry_kind = 'settlement' AND usage_status = 'usage_known' THEN settled_cost_micros
        WHEN entry_kind = 'settlement' THEN 0 ELSE estimated_cost_micros END,
      0, 0, 0, 0 FROM #{rows} WHERE amount_status = 'recorded'
    """
  end

  defp rebuild_function do
    """
    CREATE FUNCTION public.rebuild_api_key_usage_components()
    RETURNS void LANGUAGE plpgsql SET search_path = pg_catalog, public AS $function$
    BEGIN
      LOCK TABLE public.ledger_entries IN SHARE ROW EXCLUSIVE MODE NOWAIT;
      LOCK TABLE public.api_key_usage_buckets IN ACCESS EXCLUSIVE MODE NOWAIT;
      UPDATE public.api_key_usage_buckets SET known_total_tokens = 0,
        provisional_total_tokens = 0, admission_count = 0, known_cost_micros = 0;
      INSERT INTO public.api_key_usage_buckets AS b
        (api_key_id, bucket_started_at, known_total_tokens, provisional_total_tokens,
         admission_count, known_cost_micros, created_at, updated_at)
      SELECT v.api_key_id, date_trunc('minute', v.occurred_at), SUM(v.known_total_tokens),
        SUM(v.provisional_total_tokens), SUM(v.admission_count), SUM(v.known_cost_micros),
        statement_timestamp(), statement_timestamp()
      FROM (SELECT array_agg(e) AS entries FROM public.ledger_entries e GROUP BY request_id) r
      CROSS JOIN LATERAL public.api_key_usage_events(r.entries) v
      WHERE EXISTS (SELECT 1 FROM public.api_keys k WHERE k.id = v.api_key_id)
      GROUP BY v.api_key_id, date_trunc('minute', v.occurred_at)
      ORDER BY v.api_key_id, date_trunc('minute', v.occurred_at)
      ON CONFLICT (api_key_id, bucket_started_at) DO UPDATE SET
        known_total_tokens = EXCLUDED.known_total_tokens,
        provisional_total_tokens = EXCLUDED.provisional_total_tokens,
        admission_count = EXCLUDED.admission_count, known_cost_micros = EXCLUDED.known_cost_micros;
    END
    $function$
    """
  end
end
