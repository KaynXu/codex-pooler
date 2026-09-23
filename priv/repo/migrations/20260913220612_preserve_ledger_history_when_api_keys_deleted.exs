defmodule CodexPooler.Repo.Migrations.PreserveLedgerHistoryWhenApiKeysDeleted do
  use Ecto.Migration

  @disable_ddl_transaction true

  def up do
    transaction(fn ->
      repo().query!(delta_function(true), [], log: false)

      repo().query!(
        """
        ALTER TABLE public.ledger_entries
          ALTER COLUMN api_key_id DROP NOT NULL,
          DROP CONSTRAINT ledger_entries_api_key_id_fkey,
          ADD CONSTRAINT ledger_entries_api_key_id_fkey
            FOREIGN KEY (api_key_id) REFERENCES public.api_keys(id) ON DELETE SET NULL NOT VALID
        """,
        [],
        log: false
      )
    end)

    validate("ledger_entries_api_key_id_fkey")
  end

  def down do
    # Once a key is deleted its history cannot regain the original required
    # owner. Refuse before any schema or function mutation.
    transaction(fn ->
      # Block deletions and ledger writers during the preflight, but permit
      # readers throughout its scan. The pair is acquired as one retryable
      # group: NOWAIT never queues behind a long application transaction, the
      # exception block releases the partial lock before each 50 ms retry, and
      # the ten-second deadline fails the migration before any schema change
      # (the lock-group convention the release runbook records).
      repo().query!(
        """
        DO $migration_lock$
        DECLARE
          deadline timestamptz := clock_timestamp() + interval '10 seconds';
        BEGIN
          LOOP
            BEGIN
              LOCK TABLE public.api_keys, public.ledger_entries
                IN SHARE ROW EXCLUSIVE MODE NOWAIT;
              EXIT;
            EXCEPTION WHEN lock_not_available THEN
              IF clock_timestamp() >= deadline THEN
                RAISE;
              END IF;
            END;
            PERFORM pg_sleep(0.05);
          END LOOP;
        END
        $migration_lock$
        """,
        [],
        log: false,
        timeout: :infinity
      )

      repo().query!(
        """
        DO $rollback$
        BEGIN
          IF EXISTS (SELECT 1 FROM public.ledger_entries WHERE api_key_id IS NULL) THEN
            RAISE EXCEPTION 'cannot restore required API key ownership with retained null-key history'
              USING ERRCODE = '23502';
          END IF;
        END
        $rollback$
        """,
        [],
        log: false,
        timeout: :infinity
      )

      # PostgreSQL 18 supports native NOT NULL NOT VALID. Atomically switch both
      # delete behavior and nullability without an exclusive scan or helper CHECK.
      # An interrupted validation leaves coherent old semantics and is restartable.
      repo().query!(
        """
        ALTER TABLE public.ledger_entries
          DROP CONSTRAINT IF EXISTS ledger_entries_api_key_id_not_null,
          ADD CONSTRAINT ledger_entries_api_key_id_not_null NOT NULL api_key_id NOT VALID,
          DROP CONSTRAINT ledger_entries_api_key_id_fkey,
          ADD CONSTRAINT ledger_entries_api_key_id_fkey
            FOREIGN KEY (api_key_id) REFERENCES public.api_keys(id) ON DELETE CASCADE NOT VALID
        """,
        [],
        log: false
      )

      repo().query!(delta_function(false), [], log: false)
    end)

    validate("ledger_entries_api_key_id_not_null")
    validate("ledger_entries_api_key_id_fkey")
  end

  defp transaction(fun) do
    execute(fn ->
      {:ok, _} =
        repo().transaction(
          fn ->
            repo().query!("SET LOCAL lock_timeout = '10s'", [], log: false)
            repo().query!("SET LOCAL statement_timeout = '30min'", [], log: false)
            fun.()
          end,
          timeout: :infinity
        )
    end)
  end

  defp validate(constraint) do
    transaction(fn ->
      repo().query!("ALTER TABLE public.ledger_entries VALIDATE CONSTRAINT #{constraint}", [],
        log: false,
        timeout: :infinity
      )
    end)
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
