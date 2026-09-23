defmodule CodexPooler.Repo.Migrations.RetireRequestsIdempotencyKeyIndex do
  use Ecto.Migration

  def up do
    drop_if_exists(index(:requests, [:api_key_id, :idempotency_key], name: :requests_api_key_idempotency_uq))
  end

  def down do
    create(
      unique_index(:requests, [:api_key_id, :idempotency_key],
        name: :requests_api_key_idempotency_uq,
        where: "idempotency_key IS NOT NULL"
      )
    )
  end
end
