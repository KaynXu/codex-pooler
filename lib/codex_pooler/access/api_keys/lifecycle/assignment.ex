defmodule CodexPooler.Access.APIKeys.Assignment do
  @moduledoc false

  alias CodexPooler.Access.{APIKey, APIKeyPolicyBinding}
  alias CodexPooler.Access.APIKeys.{Errors, Policy, PolicyUpdate, Queries}
  alias CodexPooler.Accounts.Scope
  alias CodexPooler.Pools
  alias CodexPooler.Pools.Authorization, as: PoolAuthorization
  alias CodexPooler.Pools.Pool

  @type access_error :: %{required(:code) => atom(), required(:message) => String.t()}
  @type selection_error :: %{required(:message) => String.t()}

  @spec assign_api_keys_to_pool(Scope.t(), Pool.t() | Ecto.UUID.t(), [Ecto.UUID.t()]) ::
          :ok | {:error, Ecto.Changeset.t() | access_error() | selection_error()}
  def assign_api_keys_to_pool(%Scope{} = scope, pool_or_id, api_key_ids)
      when is_list(api_key_ids) do
    api_key_ids = selected_api_key_ids(api_key_ids)

    with %Pool{} = pool <- normalize_pool(pool_or_id),
         {:ok, _decision} <-
           PoolAuthorization.require_capability(
             scope,
             PoolAuthorization.capability(:pool_api_key_manage),
             pool_id: pool.id
           ),
         {:ok, api_keys} <- Queries.list_api_keys(scope),
         {:ok, selected_api_keys} <- selected_api_keys(api_keys, api_key_ids) do
      move_api_keys_to_pool(scope, pool, selected_api_keys)
    else
      nil -> {:error, Errors.access_error(:pool_not_found, "pool was not found")}
      {:error, _reason} = error -> error
    end
  end

  def assign_api_keys_to_pool(_scope, _pool_or_id, _api_key_ids),
    do: {:error, Errors.access_error(:invalid_request, "user scope, Pool, and API key ids are required")}

  defp selected_api_key_ids(api_key_ids) do
    api_key_ids
    |> Enum.filter(&is_binary/1)
    |> Enum.reject(&(&1 == ""))
    |> Enum.uniq()
  end

  defp selected_api_keys(api_keys, api_key_ids) do
    api_key_lookup = Map.new(api_keys, &{&1.id, &1})
    selected_api_key_ids = MapSet.new(api_key_ids)
    known_api_key_ids = api_key_lookup |> Map.keys() |> MapSet.new()

    if MapSet.subset?(selected_api_key_ids, known_api_key_ids) do
      {:ok, Enum.map(api_key_ids, &Map.fetch!(api_key_lookup, &1))}
    else
      {:error, %{message: "selected API keys are not available"}}
    end
  end

  defp move_api_keys_to_pool(scope, pool, api_keys) do
    Enum.reduce_while(api_keys, :ok, fn api_key, :ok ->
      case move_api_key_to_pool(scope, pool, api_key) do
        :ok -> {:cont, :ok}
        {:error, reason} -> {:halt, {:error, reason}}
      end
    end)
  end

  defp move_api_key_to_pool(_scope, %{id: pool_id}, %APIKey{pool_id: pool_id}), do: :ok

  defp move_api_key_to_pool(scope, pool, %APIKey{} = api_key) do
    result =
      with {:ok, %{api_key: api_key, policy_bindings: bindings}} <-
             Queries.get_api_key_with_policy(scope, api_key.id) do
        PolicyUpdate.update_api_key_with_policy(
          scope,
          api_key,
          api_key_policy_move_attrs(api_key, bindings, pool.id)
        )
      end

    case result do
      {:ok, _api_key} -> :ok
      {:error, reason} -> {:error, reason}
    end
  end

  defp api_key_policy_move_attrs(api_key, bindings, pool_id) do
    %{
      pool_id: pool_id,
      model_mode: Policy.allow_list_mode(api_key.allowed_model_identifiers, :models),
      allowed_model_identifiers: api_key.allowed_model_identifiers,
      enforced_model_identifier: api_key.enforced_model_identifier,
      enforced_reasoning_effort: api_key.enforced_reasoning_effort,
      maximum_reasoning_effort: api_key.maximum_reasoning_effort,
      enforced_service_tier: api_key.enforced_service_tier,
      default_policy: default_policy_attrs(bindings),
      model_policies: model_policy_attrs(bindings)
    }
  end

  defp default_policy_attrs(bindings) do
    bindings
    |> Enum.find(&(&1.binding_scope == "default"))
    |> binding_attrs("default")
  end

  defp model_policy_attrs(bindings) do
    bindings
    |> Enum.filter(&(&1.binding_scope == "model"))
    |> Enum.map(&binding_attrs(&1, "model"))
  end

  defp binding_attrs(%APIKeyPolicyBinding{} = binding, scope) do
    %{
      binding_scope: scope,
      model_identifier: binding.model_identifier,
      status: binding.status,
      max_requests_per_minute: binding.max_requests_per_minute,
      max_tokens_per_day: binding.max_tokens_per_day,
      max_tokens_per_week: binding.max_tokens_per_week,
      max_input_tokens_per_request: binding.max_input_tokens_per_request,
      max_output_tokens_per_request: binding.max_output_tokens_per_request
    }
  end

  defp binding_attrs(nil, "default"), do: %{}

  defp normalize_pool(%Pool{} = pool), do: pool
  defp normalize_pool(id) when is_binary(id), do: Pools.get_active_pool(id)
  defp normalize_pool(_pool_or_id), do: nil
end
