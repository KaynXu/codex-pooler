defmodule CodexPooler.Repo.Migrations.PreserveRequestsWhenApiKeysDeleted do
  use Ecto.Migration

  def up do
    execute("""
    ALTER TABLE public.requests
      DROP CONSTRAINT IF EXISTS requests_api_key_id_fkey,
      ADD CONSTRAINT requests_api_key_id_fkey
        FOREIGN KEY (api_key_id) REFERENCES public.api_keys(id) ON DELETE SET NULL
    """)
  end

  def down do
    execute("""
    ALTER TABLE public.requests
      DROP CONSTRAINT IF EXISTS requests_api_key_id_fkey,
      ADD CONSTRAINT requests_api_key_id_fkey
        FOREIGN KEY (api_key_id) REFERENCES public.api_keys(id) ON DELETE CASCADE
    """)
  end
end
