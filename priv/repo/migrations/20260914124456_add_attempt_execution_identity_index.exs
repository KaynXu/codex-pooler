defmodule CodexPooler.Repo.Migrations.AddAttemptExecutionIdentityIndex do
  use Ecto.Migration

  @disable_ddl_transaction true
  @definition "CREATE INDEX attempts_open_execution_index ON public.attempts USING btree (COALESCE(owner_execution_checked_at, started_at), id) WHERE ((status = ANY (ARRAY['queued'::text, 'in_progress'::text])) AND (owner_execution_id IS NOT NULL))"
  def up do
    execute(fn ->
      repo().checkout(fn ->
        [[previous_timeout]] = repo().query!("SHOW lock_timeout", [], log: false).rows
        repo().query!("SET lock_timeout = '10s'", [], log: false)

        try do
          case repo().query!(
                 """
                 SELECT i.indisvalid, i.indisready, pg_get_indexdef(c.oid)
                 FROM pg_class c LEFT JOIN pg_index i ON i.indexrelid=c.oid
                 WHERE c.oid = to_regclass('public.attempts_open_execution_index')
                 """,
                 [],
                 log: false
               ).rows do
            [] ->
              :ok

            [[valid, ready, definition]] when definition == @definition ->
              if not (valid and ready) do
                repo().query!("DROP INDEX CONCURRENTLY public.attempts_open_execution_index", [],
                  log: false,
                  timeout: :infinity
                )
              end

            _ ->
              raise "conflicting index: attempts_open_execution_index"
          end

          repo().query!(
            """
            CREATE INDEX CONCURRENTLY IF NOT EXISTS attempts_open_execution_index
            ON public.attempts (COALESCE(owner_execution_checked_at, started_at), id)
            WHERE status IN ('queued', 'in_progress') AND owner_execution_id IS NOT NULL
            """,
            [],
            log: false,
            timeout: :infinity
          )

          [[true, true, @definition]] =
            repo().query!(
              "SELECT indisvalid,indisready,pg_get_indexdef(indexrelid) FROM pg_index WHERE indexrelid='public.attempts_open_execution_index'::regclass",
              [],
              log: false
            ).rows
        after
          repo().query!("SELECT set_config('lock_timeout', $1, false)", [previous_timeout],
            log: false
          )
        end
      end)
    end)
  end

  def down do
    execute(fn ->
      repo().checkout(fn ->
        [[previous_timeout]] = repo().query!("SHOW lock_timeout", [], log: false).rows
        repo().query!("SET lock_timeout = '10s'", [], log: false)

        try do
          repo().query!(
            "DROP INDEX CONCURRENTLY IF EXISTS public.attempts_open_execution_index",
            [],
            log: false,
            timeout: :infinity
          )
        after
          repo().query!("SELECT set_config('lock_timeout', $1, false)", [previous_timeout],
            log: false
          )
        end
      end)
    end)
  end
end
