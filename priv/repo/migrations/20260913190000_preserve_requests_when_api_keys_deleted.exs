defmodule CodexPooler.Repo.Migrations.PreserveRequestsWhenApiKeysDeleted do
  use Ecto.Migration

  @disable_ddl_transaction true

  def up do
    swap("SET NULL")
  end

  def down do
    swap("CASCADE")
  end

  defp swap(action) do
    execute(fn ->
      {:ok, _} =
        repo().transaction(fn ->
          repo().query!("SET LOCAL lock_timeout = '10s'", [], log: false)
          repo().query!("SET LOCAL statement_timeout = '60s'", [], log: false)

          repo().query!(
            """
            ALTER TABLE public.requests
              DROP CONSTRAINT IF EXISTS requests_api_key_id_fkey,
              ADD CONSTRAINT requests_api_key_id_fkey
                FOREIGN KEY (api_key_id) REFERENCES public.api_keys(id) ON DELETE #{action} NOT VALID
            """,
            [],
            log: false
          )
        end)
    end)

    # Commit the short exclusive swap before scanning existing rows. On interruption,
    # rerunning the swap is safe and new writes remain checked throughout validation.
    execute(fn ->
      {:ok, _} =
        repo().transaction(
          fn ->
            repo().query!("SET LOCAL lock_timeout = '10s'", [], log: false)
            repo().query!("SET LOCAL statement_timeout = '30min'", [], log: false)

            repo().query!(
              "ALTER TABLE public.requests VALIDATE CONSTRAINT requests_api_key_id_fkey",
              [],
              log: false,
              timeout: :infinity
            )
          end,
          timeout: :infinity
        )
    end)
  end
end
