defmodule CodexPooler.Status.Freshness do
  @moduledoc "Shared freshness policy for provider status snapshots and their banner."

  @stale_after_seconds 900
  @banner_max_age_seconds 86_400

  @spec stale?(DateTime.t() | nil, DateTime.t()) :: boolean()
  def stale?(last_success_at, now \\ DateTime.utc_now())
  def stale?(nil, _now), do: true

  def stale?(%DateTime{} = last_success_at, now),
    do: DateTime.diff(now, last_success_at, :second) > @stale_after_seconds

  @spec banner_fresh?(DateTime.t() | nil, DateTime.t()) :: boolean()
  def banner_fresh?(last_success_at, now \\ DateTime.utc_now())
  def banner_fresh?(nil, _now), do: false

  def banner_fresh?(%DateTime{} = last_success_at, now),
    do: DateTime.diff(now, last_success_at, :second) <= @banner_max_age_seconds
end
