defmodule CodexPooler.Repo.Migrations.RemoveRequestsIdempotencyKey do
  use Ecto.Migration

  def up do
    drop_if_exists(
      index(:requests, [:api_key_id, :idempotency_key], name: :requests_api_key_idempotency_uq)
    )

    alter table(:requests) do
      remove :idempotency_key
    end
  end

  def down do
    alter table(:requests) do
      add :idempotency_key, :text
    end

    create(
      unique_index(:requests, [:api_key_id, :idempotency_key],
        name: :requests_api_key_idempotency_uq,
        where: "idempotency_key IS NOT NULL"
      )
    )
  end
end
