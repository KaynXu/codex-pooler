defmodule CodexPooler.Accounts.TOTPSettingInspectTest do
  use ExUnit.Case, async: true

  alias CodexPooler.Accounts.TOTPSetting

  @secret_material "TOTP-SECRET-MUST-NOT-RENDER"

  test "inspecting a TOTP setting or its changeset never renders the secret ciphertext" do
    setting = %TOTPSetting{
      id: Ecto.UUID.generate(),
      secret_ciphertext: @secret_material,
      secret_key_version: "v3",
      status: "enrolled"
    }

    rendered = inspect(setting, limit: :infinity, printable_limit: :infinity)
    refute rendered =~ @secret_material
    refute rendered =~ "secret_ciphertext:"
    assert rendered =~ ~s(secret_key_version: "v3")

    changeset = Ecto.Changeset.change(%TOTPSetting{}, secret_ciphertext: @secret_material)
    refute inspect(changeset, limit: :infinity, printable_limit: :infinity) =~ @secret_material
  end
end
