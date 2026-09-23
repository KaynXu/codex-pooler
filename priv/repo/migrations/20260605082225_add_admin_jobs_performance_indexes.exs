defmodule CodexPooler.Repo.Migrations.AddAdminJobsPerformanceIndexes do
  use Ecto.Migration

  @disable_ddl_transaction true
  @disable_migration_lock true

  def up do
    execute("""
    CREATE INDEX CONCURRENTLY IF NOT EXISTS oban_jobs_admin_completed_target_resolution_idx
    ON public.oban_jobs USING btree (
      worker,
      COALESCE(args->>'pool_id', ''),
      COALESCE(args->>'pool_upstream_assignment_id', ''),
      COALESCE(args->>'upstream_identity_id', ''),
      COALESCE(args->>'api_key_id', ''),
      COALESCE(args->>'rollup_date', ''),
      inserted_at DESC,
      id DESC
    )
    WHERE state = 'completed'
    """)

    execute("""
    CREATE INDEX CONCURRENTLY IF NOT EXISTS oban_jobs_admin_unfinished_inserted_id_idx
    ON public.oban_jobs USING btree (inserted_at DESC, id DESC)
    WHERE state <> 'completed'
    """)
  end

  def down do
    execute("DROP INDEX CONCURRENTLY IF EXISTS public.oban_jobs_admin_unfinished_inserted_id_idx")

    execute("DROP INDEX CONCURRENTLY IF EXISTS public.oban_jobs_admin_completed_target_resolution_idx")
  end
end
