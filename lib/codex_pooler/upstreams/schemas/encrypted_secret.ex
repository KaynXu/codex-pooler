defmodule CodexPooler.Upstreams.Schemas.EncryptedSecret do
  @moduledoc """
  Persisted encrypted upstream secret record.

  The `CodexPooler.Upstreams.Schemas.*` namespace is intentional for upstream
  database structs so runtime callers can distinguish schemas from the operator
  context facade.
  """
  use CodexPooler.Schema

  import Ecto.Changeset

  @secret_kinds ~w(access_token refresh_token device_code web_session api_key other)
  @statuses ~w(active superseded revoked)

  @type t :: %__MODULE__{}
  @type attrs :: map()

  # A failure dump or log that inspects a row or a changeset must never carry
  # the secret material: `redact: true` covers both (a manual `@derive` would
  # protect the struct only, while `Ecto.Changeset` inspects `changes` through
  # the schema's redacted fields). `aad` stays visible: it is bounded,
  # non-secret authenticated data (algorithm, key name, identity id, kind, key
  # version) that identifies the row (findings#215, #221).
  schema "encrypted_secrets" do
    field :upstream_identity_id, :binary_id
    field :secret_kind, :string
    field :key_version, :string
    field :ciphertext, :binary, redact: true
    field :nonce, :binary, redact: true
    field :aad, :map
    field :status, :string
    field :created_at, :utc_datetime_usec
    field :superseded_at, :utc_datetime_usec
  end

  @spec changeset(t() | Ecto.Changeset.t(), attrs()) :: Ecto.Changeset.t()
  def changeset(secret, attrs) do
    secret
    |> cast(attrs, [
      :upstream_identity_id,
      :secret_kind,
      :key_version,
      :ciphertext,
      :nonce,
      :aad,
      :status,
      :created_at,
      :superseded_at
    ])
    |> update_change(:secret_kind, &normalize_token/1)
    |> update_change(:key_version, &String.trim/1)
    |> validate_required([
      :upstream_identity_id,
      :secret_kind,
      :key_version,
      :ciphertext,
      :aad,
      :status,
      :created_at
    ])
    |> validate_inclusion(:secret_kind, @secret_kinds)
    |> validate_inclusion(:status, @statuses)
    |> validate_length(:key_version, min: 1)
    |> validate_change(:ciphertext, fn
      :ciphertext, ciphertext when is_binary(ciphertext) and byte_size(ciphertext) > 0 -> []
      :ciphertext, _ciphertext -> [ciphertext: "can't be blank"]
    end)
    |> unique_constraint(:secret_kind, name: :encrypted_secrets_active_kind_uq)
  end

  defp normalize_token(value) when is_binary(value) do
    value
    |> String.trim()
    |> String.downcase()
  end

  defp normalize_token(value), do: value
end
