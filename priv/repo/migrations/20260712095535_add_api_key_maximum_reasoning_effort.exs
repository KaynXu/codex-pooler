defmodule CodexPooler.Repo.Migrations.AddApiKeyMaximumReasoningEffort do
  use Ecto.Migration

  def up do
    alter table(:api_keys) do
      add :maximum_reasoning_effort, :text
    end

    create constraint(:api_keys, :api_keys_maximum_reasoning_effort_check, check: "maximum_reasoning_effort IS NULL OR maximum_reasoning_effort = ANY (ARRAY['none'::text, 'minimal'::text, 'low'::text, 'medium'::text, 'high'::text, 'xhigh'::text, 'max'::text, 'ultra'::text])")

    create constraint(:api_keys, :api_keys_reasoning_effort_policy_mutual_exclusion_check, check: "enforced_reasoning_effort IS NULL OR maximum_reasoning_effort IS NULL")
  end

  def down do
    drop constraint(:api_keys, :api_keys_reasoning_effort_policy_mutual_exclusion_check)
    drop constraint(:api_keys, :api_keys_maximum_reasoning_effort_check)

    alter table(:api_keys) do
      remove :maximum_reasoning_effort
    end
  end
end
