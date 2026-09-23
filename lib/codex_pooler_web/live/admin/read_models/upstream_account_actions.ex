defmodule CodexPoolerWeb.Admin.UpstreamAccountActions do
  @moduledoc false

  @assignment_required_reason "Assign this account to a Pool before using account actions."

  @type action :: %{required(:available?) => boolean(), required(:reason) => String.t() | nil}

  @spec assignment_unavailable_reason([map()]) :: String.t() | nil
  def assignment_unavailable_reason([]), do: @assignment_required_reason
  def assignment_unavailable_reason([_assignment | _assignments]), do: nil

  @spec require_assignment(action(), [map()]) :: action()
  def require_assignment(action, assignments) do
    case assignment_unavailable_reason(assignments) do
      nil -> action
      reason -> %{available?: false, reason: reason}
    end
  end
end
