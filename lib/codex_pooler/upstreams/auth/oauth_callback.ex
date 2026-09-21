defmodule CodexPooler.Upstreams.Auth.OAuthCallback do
  @moduledoc false

  @callback_scheme "http"
  @callback_hosts ["localhost", "127.0.0.1"]
  @callback_port 1455
  @callback_path "/auth/callback"

  @failure_messages %{
    invalid_callback_url: "OAuth callback URL is invalid",
    invalid_callback_origin: "OAuth callback URL must use http://localhost:1455/auth/callback",
    missing_state: "OAuth callback is missing state",
    duplicate_callback_param: "OAuth callback contains duplicate parameters",
    missing_callback_result: "OAuth callback must include either an authorization code or provider error",
    provider_denied: "OpenAI denied the OAuth request",
    invalid_state: "OAuth callback state does not match a pending flow",
    expired_flow: "OAuth flow has expired",
    flow_not_pending: "OAuth flow is not pending",
    stale_flow: "OAuth flow was superseded",
    token_exchange_failed: "OAuth token exchange failed",
    identity_mismatch: "OAuth account does not match the selected upstream account",
    identity_conflict: "OAuth account conflicts with an existing upstream account",
    unauthorized_pool: "Pool authorization is required for this OAuth flow"
  }

  @failure_codes [
    :invalid_callback_url,
    :invalid_callback_origin,
    :missing_state,
    :duplicate_callback_param,
    :missing_callback_result,
    :provider_denied,
    :invalid_state,
    :expired_flow,
    :flow_not_pending,
    :stale_flow,
    :token_exchange_failed,
    :identity_mismatch,
    :identity_conflict,
    :unauthorized_pool
  ]

  @callback_control_keys ["state", "code", "error"]

  @type failure_code :: atom()
  @type safe_error :: %{
          required(:code) => failure_code(),
          required(:message) => String.t(),
          required(:status) => pos_integer(),
          optional(:state) => String.t()
        }
  @type callback_success :: %{required(:state) => String.t(), required(:code) => String.t()}
  @type parse_result :: {:ok, callback_success()} | {:error, safe_error()}

  @spec parse(term()) :: parse_result()
  def parse(callback_url) when is_binary(callback_url) do
    with {:ok, uri} <- parse_uri(callback_url),
         :ok <- validate_origin(uri),
         :ok <- reject_fragment(uri),
         {:ok, pairs} <- query_pairs(uri.query),
         :ok <- reject_duplicate_control_keys(pairs),
         {:ok, state} <- required_state(pairs),
         {:ok, result} <- callback_result(pairs) do
      parsed_callback(result, state)
    end
  end

  def parse(_callback_url), do: {:error, safe_error(:invalid_callback_url)}

  @spec failure_codes() :: [failure_code()]
  def failure_codes, do: @failure_codes

  @spec safe_error(failure_code(), keyword()) :: safe_error()
  def safe_error(code, opts \\ []) do
    code = known_failure_code(code)

    %{
      code: code,
      message: Map.fetch!(@failure_messages, code),
      status: failure_status(code)
    }
    |> maybe_put_state(Keyword.get(opts, :state))
  end

  defp parse_uri(callback_url) do
    case URI.parse(callback_url) do
      %URI{scheme: scheme, host: host} = uri when is_binary(scheme) and is_binary(host) ->
        {:ok, uri}

      %URI{} ->
        {:error, safe_error(:invalid_callback_url)}
    end
  rescue
    _error -> {:error, safe_error(:invalid_callback_url)}
  end

  defp validate_origin(%URI{} = uri) do
    if uri.scheme == @callback_scheme and uri.host in @callback_hosts and
         uri.port == @callback_port and uri.path == @callback_path and is_nil(uri.userinfo) do
      :ok
    else
      {:error, safe_error(:invalid_callback_origin)}
    end
  end

  defp reject_fragment(%URI{fragment: nil}), do: :ok
  defp reject_fragment(%URI{fragment: ""}), do: :ok
  defp reject_fragment(%URI{}), do: {:error, safe_error(:invalid_callback_url)}

  defp query_pairs(nil), do: {:ok, []}
  defp query_pairs(""), do: {:ok, []}

  defp query_pairs(query) when is_binary(query) do
    {:ok, Enum.to_list(URI.query_decoder(query))}
  rescue
    _error -> {:error, safe_error(:invalid_callback_url)}
  end

  defp reject_duplicate_control_keys(pairs) do
    duplicate? =
      pairs
      |> Enum.map(fn {key, _value} -> key end)
      |> Enum.filter(&(&1 in @callback_control_keys))
      |> frequencies()
      |> Enum.any?(fn {_key, count} -> count > 1 end)

    if duplicate? do
      {:error, safe_error(:duplicate_callback_param)}
    else
      :ok
    end
  end

  defp required_state(pairs) do
    case present_values(pairs, "state") do
      [state] -> {:ok, state}
      _missing_or_blank -> {:error, safe_error(:missing_state)}
    end
  end

  defp callback_result(pairs) do
    codes = present_values(pairs, "code")
    errors = present_values(pairs, "error")

    case {codes, errors} do
      {[code], []} -> {:ok, {:code, code}}
      {[], [_provider_error]} -> {:ok, :provider_denied}
      _invalid -> {:error, safe_error(:missing_callback_result)}
    end
  end

  defp parsed_callback({:code, code}, state), do: {:ok, %{state: state, code: code}}

  defp parsed_callback(:provider_denied, state) do
    {:error, safe_error(:provider_denied, state: state)}
  end

  defp present_values(pairs, key) do
    pairs
    |> Enum.filter(fn {pair_key, _value} -> pair_key == key end)
    |> Enum.map(fn {_key, value} -> present_string(value) end)
    |> Enum.reject(&is_nil/1)
  end

  defp frequencies(values) do
    Enum.reduce(values, %{}, fn value, acc -> Map.update(acc, value, 1, &(&1 + 1)) end)
  end

  defp present_string(value) when is_binary(value) do
    value = String.trim(value)
    if value == "", do: nil, else: value
  end

  defp present_string(_value), do: nil

  defp maybe_put_state(error, state) when is_binary(state), do: Map.put(error, :state, state)
  defp maybe_put_state(error, _state), do: error

  defp known_failure_code(code) when is_atom(code) do
    if Map.has_key?(@failure_messages, code), do: code, else: :invalid_callback_url
  end

  defp known_failure_code(_code), do: :invalid_callback_url

  defp failure_status(:unauthorized_pool), do: 403
  defp failure_status(:token_exchange_failed), do: 502

  defp failure_status(code)
       when code in [:identity_mismatch, :identity_conflict, :flow_not_pending, :stale_flow],
       do: 409

  defp failure_status(_code), do: 400
end
