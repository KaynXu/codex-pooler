defmodule CodexPooler.Upstreams.Schemas.OAuthFlow do
  @moduledoc """
  Persisted OpenAI OAuth flow state for upstream linking.

  Raw state tokens, PKCE verifiers, and device auth ids are accepted only as
  virtual changeset inputs and are transformed before persistence.
  """

  use CodexPooler.Schema

  import Ecto.Changeset

  alias CodexPooler.Upstreams.OAuthFlows
  alias CodexPooler.Upstreams.SecretBox

  @flow_kinds ~w(browser device)
  @purposes ~w(link relink)
  @statuses ~w(pending completed failed cancelled expired)
  @terminal_statuses ~w(completed failed cancelled expired)

  @type t :: %__MODULE__{}
  @type attrs :: map()
  @type flow_kind :: String.t()
  @type purpose :: String.t()
  @type status :: String.t()

  schema "upstream_oauth_flows" do
    field :pool_id, :binary_id
    field :upstream_identity_id, :binary_id
    field :requested_by_user_id, :binary_id
    field :flow_kind, :string
    field :purpose, :string
    field :status, :string
    field :state_token_hash, :binary
    field :redirect_uri, :string
    field :code_verifier_ciphertext, :binary, redact: true
    field :device_auth_id_ciphertext, :binary, redact: true
    field :device_user_code, :string
    field :verification_uri, :string
    field :interval_seconds, :integer
    field :poll_after_at, :utc_datetime_usec
    field :expires_at, :utc_datetime_usec
    field :completed_at, :utc_datetime_usec
    field :cancelled_at, :utc_datetime_usec
    field :last_polled_at, :utc_datetime_usec
    field :result_upstream_identity_id, :binary_id
    field :error_code, :string
    field :error_message, :string
    field :metadata, :map

    field :state_token, :string, virtual: true, redact: true
    field :code_verifier, :string, virtual: true, redact: true
    field :device_auth_id, :string, virtual: true, redact: true

    timestamps(type: :utc_datetime_usec)
  end

  @spec changeset(t() | Ecto.Changeset.t(), attrs()) :: Ecto.Changeset.t()
  def changeset(flow, attrs) do
    flow
    |> cast(attrs, [
      :pool_id,
      :upstream_identity_id,
      :requested_by_user_id,
      :flow_kind,
      :purpose,
      :status,
      :redirect_uri,
      :device_user_code,
      :verification_uri,
      :interval_seconds,
      :poll_after_at,
      :expires_at,
      :completed_at,
      :cancelled_at,
      :last_polled_at,
      :result_upstream_identity_id,
      :error_code,
      :error_message,
      :metadata,
      :inserted_at,
      :updated_at,
      :state_token,
      :code_verifier,
      :device_auth_id
    ])
    |> update_change(:flow_kind, &normalize_token/1)
    |> update_change(:purpose, &normalize_token/1)
    |> update_change(:status, &normalize_token/1)
    |> update_change(:redirect_uri, &normalize_optional_string/1)
    |> update_change(:device_user_code, &normalize_optional_string/1)
    |> update_change(:verification_uri, &normalize_optional_string/1)
    |> update_change(:error_code, &normalize_optional_token/1)
    |> update_change(:error_message, &normalize_optional_string/1)
    |> hash_state_token()
    |> encrypt_transient_secret(:code_verifier, :code_verifier_ciphertext)
    |> encrypt_transient_secret(:device_auth_id, :device_auth_id_ciphertext)
    |> validate_required([
      :pool_id,
      :requested_by_user_id,
      :flow_kind,
      :purpose,
      :status,
      :expires_at,
      :metadata
    ])
    |> validate_inclusion(:flow_kind, @flow_kinds)
    |> validate_inclusion(:purpose, @purposes)
    |> validate_inclusion(:status, @statuses)
    |> validate_number(:interval_seconds, greater_than: 0)
    |> validate_state_token_hash()
    |> unique_constraint(:state_token_hash, name: :upstream_oauth_flows_state_token_hash_uq)
    |> foreign_key_constraint(:pool_id)
    |> foreign_key_constraint(:upstream_identity_id)
    |> foreign_key_constraint(:requested_by_user_id)
    |> foreign_key_constraint(:result_upstream_identity_id)
    |> check_constraint(:flow_kind, name: :upstream_oauth_flows_flow_kind_check)
    |> check_constraint(:purpose, name: :upstream_oauth_flows_purpose_check)
    |> check_constraint(:status, name: :upstream_oauth_flows_status_check)
    |> check_constraint(:metadata, name: :upstream_oauth_flows_metadata_shape_check)
    |> check_constraint(:interval_seconds, name: :upstream_oauth_flows_interval_seconds_check)
    |> check_constraint(:state_token_hash, name: :upstream_oauth_flows_state_hash_shape_check)
  end

  @spec flow_kinds() :: [flow_kind()]
  def flow_kinds, do: @flow_kinds

  @spec purposes() :: [purpose()]
  def purposes, do: @purposes

  @spec statuses() :: [status()]
  def statuses, do: @statuses

  @spec terminal_statuses() :: [status()]
  def terminal_statuses, do: @terminal_statuses

  @spec pending_status() :: status()
  def pending_status, do: "pending"

  @spec expired_status() :: status()
  def expired_status, do: "expired"

  defp hash_state_token(changeset) do
    case get_change(changeset, :state_token) do
      state_token when is_binary(state_token) ->
        state_token = String.trim(state_token)

        if state_token == "" do
          add_error(changeset, :state_token, "can't be blank")
        else
          put_change(changeset, :state_token_hash, OAuthFlows.hash_state_token(state_token))
        end

      _value ->
        changeset
    end
  end

  defp validate_state_token_hash(changeset) do
    validate_change(changeset, :state_token_hash, fn
      :state_token_hash, hash when is_binary(hash) and byte_size(hash) == 32 -> []
      :state_token_hash, _hash -> [state_token_hash: "must be a SHA-256 digest"]
    end)
  end

  defp encrypt_transient_secret(changeset, virtual_field, ciphertext_field) do
    case get_change(changeset, virtual_field) do
      plaintext when is_binary(plaintext) ->
        plaintext = String.trim(plaintext)

        if plaintext == "" do
          add_error(changeset, virtual_field, "can't be blank")
        else
          put_encrypted_transient_secret(changeset, virtual_field, ciphertext_field, plaintext)
        end

      _value ->
        changeset
    end
  end

  defp put_encrypted_transient_secret(changeset, virtual_field, ciphertext_field, plaintext) do
    case SecretBox.encrypt_envelope(plaintext, transient_aad(changeset, virtual_field)) do
      {:ok, ciphertext} ->
        put_change(changeset, ciphertext_field, ciphertext)

      {:error, reason} ->
        add_error(changeset, virtual_field, reason.message)
    end
  end

  defp transient_aad(changeset, virtual_field) do
    %{
      "algorithm" => "AES-256-GCM",
      "key_env" => SecretBox.configured_key_env(),
      "domain" => "upstream_oauth_flow",
      "secret_kind" => Atom.to_string(virtual_field),
      "pool_id" => get_field(changeset, :pool_id),
      "upstream_identity_id" => get_field(changeset, :upstream_identity_id),
      "purpose" => get_field(changeset, :purpose),
      "flow_kind" => get_field(changeset, :flow_kind)
    }
  end

  defp normalize_token(value) when is_binary(value) do
    value
    |> String.trim()
    |> String.downcase()
  end

  defp normalize_token(value), do: value

  defp normalize_optional_token(value) when is_binary(value) do
    case normalize_token(value) do
      "" -> nil
      normalized -> normalized
    end
  end

  defp normalize_optional_token(value), do: value

  defp normalize_optional_string(value) when is_binary(value) do
    case String.trim(value) do
      "" -> nil
      normalized -> normalized
    end
  end

  defp normalize_optional_string(value), do: value
end
