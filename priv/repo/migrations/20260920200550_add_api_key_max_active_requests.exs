defmodule CodexPooler.Repo.Migrations.AddApiKeyMaxActiveRequests do
  use Ecto.Migration

  def change do
    alter table(:api_keys) do
      add :max_active_requests, :integer
    end

    create constraint(:api_keys, :api_keys_max_active_requests_positive,
             check: "max_active_requests IS NULL OR max_active_requests > 0"
           )
  end
end
