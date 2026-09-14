defmodule CodexPooler.Repo.Migrations.PreserveLedgerHistoryWhenApiKeysDeleted do
  use Ecto.Migration

  def up do
    execute("SET LOCAL lock_timeout = '10s'")
    execute("SET LOCAL statement_timeout = '60s'")
    execute(delta_function(true))

    execute("""
    ALTER TABLE public.ledger_entries
      ALTER COLUMN api_key_id DROP NOT NULL,
      DROP CONSTRAINT ledger_entries_api_key_id_fkey,
      ADD CONSTRAINT ledger_entries_api_key_id_fkey
        FOREIGN KEY (api_key_id) REFERENCES public.api_keys(id) ON DELETE SET NULL
    """)
  end

  def down do
    execute("SET LOCAL lock_timeout = '10s'")
    execute("SET LOCAL statement_timeout = '60s'")

    # Once a key is deleted its history cannot regain the original required
    # owner. Refuse that rollback instead of deleting the retained ledger.
    execute("""
    ALTER TABLE public.ledger_entries
      ALTER COLUMN api_key_id SET NOT NULL,
      DROP CONSTRAINT ledger_entries_api_key_id_fkey,
      ADD CONSTRAINT ledger_entries_api_key_id_fkey
        FOREIGN KEY (api_key_id) REFERENCES public.api_keys(id) ON DELETE CASCADE
    """)

    execute(delta_function(false))
  end

  defp delta_function(preserve_history?) do
    guard =
      if preserve_history?,
        do: "WHERE EXISTS (SELECT 1 FROM public.api_keys WHERE id = p_api_key_id)",
        else: ""

    """
    CREATE OR REPLACE FUNCTION public.apply_api_key_usage_bucket_delta(
      p_api_key_id uuid,
      p_occurred_at timestamp with time zone,
      p_request_delta bigint,
      p_token_delta bigint,
      p_cost_delta numeric
    )
    RETURNS void
    LANGUAGE sql
    SET search_path = pg_catalog, public
    AS $function$
      INSERT INTO public.api_key_usage_buckets (
        api_key_id, bucket_started_at, effective_request_count,
        effective_total_tokens, effective_cost_micros, created_at, updated_at
      )
      SELECT p_api_key_id, date_trunc('minute', p_occurred_at),
        p_request_delta, p_token_delta, p_cost_delta,
        statement_timestamp(), statement_timestamp()
      #{guard}
      ON CONFLICT (api_key_id, bucket_started_at) DO UPDATE SET
        effective_request_count =
          public.api_key_usage_buckets.effective_request_count + EXCLUDED.effective_request_count,
        effective_total_tokens =
          public.api_key_usage_buckets.effective_total_tokens + EXCLUDED.effective_total_tokens,
        effective_cost_micros =
          public.api_key_usage_buckets.effective_cost_micros + EXCLUDED.effective_cost_micros,
        updated_at = statement_timestamp()
    $function$
    """
  end
end
