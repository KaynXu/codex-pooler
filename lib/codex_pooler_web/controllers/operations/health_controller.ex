defmodule CodexPoolerWeb.Operations.HealthController do
  use CodexPoolerWeb, :controller

  require Logger

  alias CodexPooler.Gateway.OperationalStatus
  alias CodexPooler.Platform.Readiness

  # Liveness stays a process-level fact. Restarting this container cannot
  # reconnect a database or apply a migration, so nothing about the database
  # belongs on this path: putting it here would turn an outage into a crash
  # loop that outlives the outage.
  @spec health(Plug.Conn.t(), map()) :: Plug.Conn.t()
  def health(conn, _params) do
    json(conn, %{status: "ok"})
  end

  @spec readiness(Plug.Conn.t(), map()) :: Plug.Conn.t()
  def readiness(conn, _params) do
    if draining?() do
      unavailable(conn)
    else
      case Readiness.check() do
        :ready ->
          json(conn, %{status: "ready"})

        {:ready, :degraded, class} ->
          Logger.warning([
            "readiness probe degraded path=/readyz reason_class=",
            class
          ])

          json(conn, %{status: "ready"})

        {:not_ready, class} ->
          Logger.warning([
            "readiness probe failed path=/readyz reason_class=",
            class
          ])

          unavailable(conn)
      end
    end
  end

  @spec draining?() :: boolean()
  defp draining?, do: OperationalStatus.draining?()

  defp unavailable(conn) do
    conn
    |> put_status(:service_unavailable)
    |> json(%{status: "unavailable"})
  end
end
