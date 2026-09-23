defmodule CodexPooler.Repo.Migrations.CreateHourlyModelUsageRollups do
  use Ecto.Migration

  def change do
    create table(:hourly_model_usage_rollups, primary_key: false) do
      add :id, :binary_id, primary_key: true, default: fragment("gen_random_uuid()")
      add :bucket_started_at, :utc_datetime_usec, null: false

      add :pool_id,
          references(:pools, type: :binary_id, on_delete: :delete_all),
          null: false

      add :model_id, :binary_id
      add :model_code, :text, null: false

      add :request_count, :bigint, null: false, default: 0
      add :success_count, :bigint, null: false, default: 0
      add :failure_count, :bigint, null: false, default: 0
      add :retry_count, :bigint, null: false, default: 0
      add :input_tokens, :bigint, null: false, default: 0
      add :cached_input_tokens, :bigint, null: false, default: 0
      add :output_tokens, :bigint, null: false, default: 0
      add :reasoning_tokens, :bigint, null: false, default: 0
      add :total_tokens, :bigint, null: false, default: 0
      add :estimated_cost_micros, :decimal, precision: 30, scale: 9, null: false, default: 0
      add :settled_cost_micros, :decimal, precision: 30, scale: 9, null: false, default: 0

      timestamps(inserted_at: :created_at, type: :utc_datetime_usec)
    end

    create constraint(
             :hourly_model_usage_rollups,
             :hourly_model_usage_rollups_bucket_started_at_hour_check,
             check: "date_trunc('hour', bucket_started_at) = bucket_started_at"
           )

    create constraint(
             :hourly_model_usage_rollups,
             :hourly_model_usage_rollups_model_code_check,
             check: "btrim(model_code) <> ''"
           )

    create constraint(
             :hourly_model_usage_rollups,
             :hourly_model_usage_rollups_request_count_check,
             check: "request_count >= 0"
           )

    create constraint(
             :hourly_model_usage_rollups,
             :hourly_model_usage_rollups_success_count_check,
             check: "success_count >= 0"
           )

    create constraint(
             :hourly_model_usage_rollups,
             :hourly_model_usage_rollups_failure_count_check,
             check: "failure_count >= 0"
           )

    create constraint(
             :hourly_model_usage_rollups,
             :hourly_model_usage_rollups_retry_count_check,
             check: "retry_count >= 0"
           )

    create constraint(
             :hourly_model_usage_rollups,
             :hourly_model_usage_rollups_input_tokens_check,
             check: "input_tokens >= 0"
           )

    create constraint(
             :hourly_model_usage_rollups,
             :hourly_model_usage_rollups_cached_input_tokens_check,
             check: "cached_input_tokens >= 0"
           )

    create constraint(
             :hourly_model_usage_rollups,
             :hourly_model_usage_rollups_output_tokens_check,
             check: "output_tokens >= 0"
           )

    create constraint(
             :hourly_model_usage_rollups,
             :hourly_model_usage_rollups_reasoning_tokens_check,
             check: "reasoning_tokens >= 0"
           )

    create constraint(
             :hourly_model_usage_rollups,
             :hourly_model_usage_rollups_total_tokens_check,
             check: "total_tokens >= 0"
           )

    create constraint(
             :hourly_model_usage_rollups,
             :hourly_model_usage_rollups_estimated_cost_micros_check,
             check: "estimated_cost_micros >= 0"
           )

    create constraint(
             :hourly_model_usage_rollups,
             :hourly_model_usage_rollups_settled_cost_micros_check,
             check: "settled_cost_micros >= 0"
           )

    create unique_index(
             :hourly_model_usage_rollups,
             [:bucket_started_at, :pool_id, :model_code],
             name: :hourly_model_usage_rollups_bucket_pool_model_code_uq
           )

    create index(:hourly_model_usage_rollups, [:pool_id, :bucket_started_at, :model_code], name: :hourly_model_usage_rollups_pool_bucket_model_idx)

    create index(:hourly_model_usage_rollups, [:model_code, :bucket_started_at, :pool_id], name: :hourly_model_usage_rollups_model_bucket_pool_idx)
  end
end
