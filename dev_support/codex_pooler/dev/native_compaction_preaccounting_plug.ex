defmodule CodexPooler.Dev.NativeCompactionPreaccounting.Plug do
  @moduledoc false
  @behaviour Plug
  import Plug.Conn
  alias CodexPooler.Access
  alias CodexPooler.Dev.NativeCompactionPreaccounting, as: Control
  alias CodexPoolerWeb.Plugs.TrustedProxyRemoteIp

  @impl true
  def init(opts), do: opts
  @impl true
  def call(conn, _opts) do
    with true <- conn.remote_ip == {127, 0, 0, 1},
         {127, 0, 0, 1} <- TrustedProxyRemoteIp.immediate_peer_ip(conn),
         [header] <- get_req_header(conn, "authorization"),
         {:ok, auth} <- Access.authenticate_authorization_header(header) do
      dispatch(conn, auth.pool_id)
    else
      _ -> json(conn, 403, %{error: "authorized_loopback_required"})
    end
  end

  defp dispatch(%{method: "GET", path_info: ["status"]} = conn, pool),
    do: reply(conn, Control.status(pool))

  defp dispatch(%{method: "POST", path_info: [action]} = conn, pool)
       when action in ["arm", "release", "disarm"] do
    with {:ok, params} <- body(conn),
         true <- params == %{} do
      result =
        case action do
          "arm" -> Control.arm(pool)
          "release" -> Control.release(pool)
          "disarm" -> Control.disarm(pool)
        end

      reply(conn, result)
    else
      _ -> json(conn, 400, %{error: "invalid_control"})
    end
  end

  defp dispatch(conn, _), do: json(conn, 404, %{error: "not_found"})

  defp body(%{body_params: %Plug.Conn.Unfetched{}} = conn) do
    with {:ok, raw, _conn} <- read_body(conn, length: 1024), do: CodexPooler.JSON.decode(raw)
  end

  defp body(%{body_params: params}), do: {:ok, params}

  defp reply(conn, {:ok, status}), do: json(conn, 200, status)
  defp reply(conn, {:error, reason}), do: json(conn, 409, %{error: reason})

  defp json(conn, status, body),
    do:
      conn
      |> put_resp_content_type("application/json")
      |> send_resp(status, CodexPooler.JSON.encode!(body))
end
