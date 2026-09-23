defmodule CodexPooler.Gateway.Admission do
  @moduledoc false

  alias CodexPooler.Gateway.Contracts
  alias CodexPooler.Gateway.Transports.Admission, as: TransportAdmission
  alias CodexPooler.Gateway.Transports.Streaming.DeferredStreamRegistry
  alias CodexPooler.RouteClass

  @type gateway_call_result ::
          {:ok, CodexPooler.Gateway.Contracts.gateway_result()}
          | {:error, CodexPooler.Gateway.Contracts.gateway_error()}
  @type admission_lease :: term()

  @spec run_admitted(String.t(), map(), (-> gateway_call_result())) :: gateway_call_result()
  def run_admitted(route_class, metadata, fun)
      when is_binary(route_class) and is_map(metadata) and is_function(fun, 0) do
    case TransportAdmission.acquire(route_class, metadata) do
      {:ok, lease} ->
        case admit_runtime(route_class) do
          {:ok, token} ->
            lease |> run_with_lease(fun, token) |> wrap_admitted_stream_result(lease, token)

          {:error, :owner_drained} ->
            TransportAdmission.release(lease)
            {:error, owner_drained_error()}
        end

      {:error, reason} ->
        {:error, TransportAdmission.overload_error(reason)}
    end
  end

  @spec admit_browser(map()) :: {:ok, admission_lease()} | {:error, Contracts.gateway_error()}
  def admit_browser(metadata) when is_map(metadata) do
    case TransportAdmission.acquire(RouteClass.admin_browser(), metadata) do
      {:ok, lease} -> {:ok, lease}
      {:error, reason} -> {:error, TransportAdmission.overload_error(reason)}
    end
  end

  @spec admit_mcp(map()) :: {:ok, admission_lease()} | {:error, Contracts.gateway_error()}
  def admit_mcp(metadata) when is_map(metadata) do
    metadata = Map.put(metadata, :route_class, RouteClass.mcp())

    case TransportAdmission.acquire(RouteClass.mcp(), metadata) do
      {:ok, lease} -> {:ok, lease}
      {:error, reason} -> {:error, TransportAdmission.overload_error(reason)}
    end
  end

  @spec release_admission(admission_lease()) :: :ok
  def release_admission(lease), do: TransportAdmission.release(lease)

  @spec checkpoint() :: :ok | {:error, Contracts.gateway_error()}
  def checkpoint do
    case DeferredStreamRegistry.checkpoint() do
      :ok -> :ok
      {:error, :owner_drained} -> {:error, owner_drained_error()}
    end
  end

  defp admit_runtime(route_class) when route_class in ["admin_browser", "mcp", "proxy_websocket"],
    do: {:ok, nil}

  defp admit_runtime(_route_class), do: DeferredStreamRegistry.admit()

  defp owner_drained_error do
    %{
      status: 503,
      code: "owner_drained",
      message: "runtime instance is draining; retry the request",
      accounting_disposition: :zero_work
    }
  end

  defp run_with_lease(lease, fun, token) do
    fun.()
  catch
    kind, reason ->
      TransportAdmission.release(lease)
      DeferredStreamRegistry.finish(token, :failed)
      :erlang.raise(kind, reason, __STACKTRACE__)
  end

  defp wrap_admitted_stream_result({:ok, %{stream: stream} = result}, lease, token) do
    wrapped = fn conn ->
      try do
        result = stream.(conn)
        DeferredStreamRegistry.finish(token, :completed)
        result
      catch
        kind, reason ->
          DeferredStreamRegistry.finish(token, :failed)
          :erlang.raise(kind, reason, __STACKTRACE__)
      after
        TransportAdmission.release(lease)
      end
    end

    {:ok, %{result | stream: wrapped}}
  end

  defp wrap_admitted_stream_result(result, lease, token) do
    TransportAdmission.release(lease)
    DeferredStreamRegistry.finish(token, :completed)
    result
  end
end
