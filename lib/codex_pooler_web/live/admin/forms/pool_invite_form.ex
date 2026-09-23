defmodule CodexPoolerWeb.Admin.PoolInviteForm do
  @moduledoc false

  import Phoenix.Component, only: [to_form: 2]

  @type receipt :: %{
          required(:pool_name) => String.t(),
          required(:invited_email) => String.t() | nil,
          required(:url) => String.t(),
          required(:emailed?) => boolean(),
          required(:email_error?) => boolean()
        }

  @spec empty_form() :: Phoenix.HTML.Form.t()
  def empty_form do
    to_form(%{"pool_id" => "", "invited_email" => "", "send_email" => "false"}, as: :invite)
  end

  @spec form_for_params(map()) :: Phoenix.HTML.Form.t()
  def form_for_params(params) when is_map(params), do: to_form(params, as: :invite)

  @spec form_for_changeset(Ecto.Changeset.t()) :: Phoenix.HTML.Form.t()
  def form_for_changeset(%Ecto.Changeset{} = changeset), do: to_form(changeset, as: :invite)

  @spec changeset(map(), term()) :: Ecto.Changeset.t()
  def changeset(params, pool) when is_map(params) do
    data = %{
      pool_id: params["pool_id"],
      invited_email: params["invited_email"],
      send_email: params["send_email"]
    }

    {%{}, %{pool_id: :string, invited_email: :string, send_email: :boolean}}
    |> Ecto.Changeset.cast(data, Map.keys(data))
    |> Ecto.Changeset.validate_required([:pool_id, :invited_email])
    |> validate_selected_pool(pool)
    |> Ecto.Changeset.validate_format(:invited_email, ~r/^[^\s@]+@[^\s@]+\.[^\s@]+$/, message: "must be a valid email")
    |> Map.put(:action, :validate)
  end

  @spec reinvite_params(String.t(), [term()]) :: map()
  def reinvite_params(pool_id, candidates \\ []) when is_binary(pool_id) do
    pool_id = String.trim(pool_id)
    params = %{"create" => "1", "pool_id" => pool_id}

    case reinvite_email(candidates, pool_id) do
      nil -> params
      invited_email -> Map.put(params, "invited_email", invited_email)
    end
  end

  @spec send_email?(map(), boolean()) :: boolean()
  def send_email?(params, true), do: params["send_email"] in ["true", true]
  def send_email?(_params, false), do: false

  @spec created_flash(map()) :: String.t()
  def created_flash(%{emailed?: true}), do: "Pool invite created and emailed"
  def created_flash(_result), do: "Pool invite created"

  @spec receipt(term(), map(), String.t(), map()) :: receipt()
  def receipt(pool, invite, invite_url, result) do
    %{
      pool_name: pool.name,
      invited_email: invite.invited_email,
      url: invite_url,
      emailed?: result.emailed?,
      email_error?: result.email_error?
    }
  end

  defp validate_selected_pool(changeset, nil),
    do: Ecto.Changeset.add_error(changeset, :pool_id, "must be an active Pool")

  defp validate_selected_pool(changeset, _pool), do: changeset

  defp reinvite_email(candidates, pool_id) do
    candidates
    |> Enum.map(&present_string/1)
    |> Enum.reject(&is_nil/1)
    |> Enum.find(&valid_reinvite_email?(&1, pool_id))
  end

  defp valid_reinvite_email?(candidate, pool_id) do
    %{"pool_id" => pool_id, "invited_email" => candidate, "send_email" => "false"}
    |> changeset(%{id: pool_id})
    |> Map.fetch!(:valid?)
  end

  defp present_string(value) when is_binary(value) do
    value = String.trim(value)
    if value == "", do: nil, else: value
  end

  defp present_string(_value), do: nil
end
