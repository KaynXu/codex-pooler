defmodule CodexPooler.Repo.Migrations.AddAttemptsServedModel do
  use Ecto.Migration

  # The model identifier the provider declared on the response object for this
  # attempt, kept apart from `upstream_model_id` (the model the Pooler sent) so
  # a provider-side substitution stays visible per attempt.
  def up do
    execute("SET LOCAL lock_timeout = '5s'")

    alter table(:attempts) do
      add :served_model, :text
    end
  end

  def down do
    execute("SET LOCAL lock_timeout = '5s'")

    alter table(:attempts) do
      remove :served_model
    end
  end
end
