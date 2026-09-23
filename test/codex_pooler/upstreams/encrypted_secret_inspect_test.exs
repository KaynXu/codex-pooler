defmodule CodexPooler.Upstreams.EncryptedSecretInspectTest do
  use ExUnit.Case, async: true

  alias CodexPooler.Upstreams.Schemas.EncryptedSecret

  @secret_material "MUST-NOT-RENDER"

  test "inspecting a stored secret renders identifying metadata but never the secret material" do
    secret = %EncryptedSecret{
      id: Ecto.UUID.generate(),
      secret_kind: "web_session",
      key_version: "v7",
      status: "active",
      ciphertext: "CIPHERTEXT-" <> @secret_material,
      nonce: "NONCE-" <> @secret_material,
      aad: %{"secret_kind" => "web_session", "key_version" => "v7"}
    }

    rendered = inspect(secret, limit: :infinity, printable_limit: :infinity)

    refute rendered =~ @secret_material
    # Ecto omits redacted fields from the rendering altogether.
    refute rendered =~ "ciphertext:"
    refute rendered =~ "nonce:"
    assert rendered =~ ~s(key_version: "v7")
    assert rendered =~ ~s(secret_kind: "web_session")
    # `aad` is bounded non-secret authenticated data and stays diagnosable.
    assert rendered =~ ~s(aad: %{"key_version" => "v7", "secret_kind" => "web_session"})
  end

  test "a failing changeset never renders the secret material in its changes" do
    changeset =
      Ecto.Changeset.change(%EncryptedSecret{},
        ciphertext: "CIPHERTEXT-" <> @secret_material,
        nonce: "NONCE-" <> @secret_material,
        key_version: "v7"
      )

    rendered = inspect(changeset, limit: :infinity, printable_limit: :infinity)

    refute rendered =~ @secret_material
    assert rendered =~ ~s(key_version: "v7")
  end
end
