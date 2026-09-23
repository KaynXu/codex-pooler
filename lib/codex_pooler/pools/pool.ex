defmodule CodexPooler.Pools.Pool do
  @moduledoc false
  use CodexPooler.Schema

  import Ecto.Changeset

  @slug_format ~r/^[a-z0-9]+(?:[a-z0-9-]*[a-z0-9])?$/

  @type t :: %__MODULE__{}
  @type attrs :: map()

  schema "pools" do
    field :slug, :string
    field :name, :string
    field :status, :string
    field :created_by_user_id, :binary_id
    field :created_at, :utc_datetime_usec
    field :updated_at, :utc_datetime_usec
    field :disabled_at, :utc_datetime_usec
  end

  @spec changeset(t() | Ecto.Changeset.t(), attrs()) :: Ecto.Changeset.t()
  def changeset(pool, attrs) do
    pool
    |> cast(attrs, [
      :slug,
      :name,
      :status,
      :created_by_user_id,
      :created_at,
      :updated_at,
      :disabled_at
    ])
    |> update_change(:slug, &normalize_slug/1)
    |> update_change(:name, &String.trim/1)
    |> validate_required([:slug, :name, :status])
    |> validate_format(:slug, @slug_format, message: "must contain only lowercase letters, numbers, and hyphens")
    |> validate_inclusion(:status, ["active", "disabled", "archived"])
    |> unique_constraint(:slug, name: :pools_slug_uq)
  end

  defp normalize_slug(slug) do
    slug
    |> String.trim()
    |> String.downcase()
  end
end
