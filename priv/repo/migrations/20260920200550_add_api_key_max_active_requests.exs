defmodule CodexPooler.Repo.Migrations.AddApiKeyMaxActiveRequests do
  use Ecto.Migration

  def up do
    execute("SET LOCAL lock_timeout = '5s'")

    alter table(:api_keys) do
      add :max_active_requests, :integer
    end

    create constraint(:api_keys, :api_keys_max_active_requests_positive,
             check: "max_active_requests IS NULL OR max_active_requests > 0"
           )
  end

  def down do
    execute("SET LOCAL lock_timeout = '5s'")

    drop constraint(:api_keys, :api_keys_max_active_requests_positive)

    alter table(:api_keys) do
      remove :max_active_requests
    end
  end
end
