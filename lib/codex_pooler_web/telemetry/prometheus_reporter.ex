defmodule CodexPoolerWeb.Telemetry.PrometheusReporter do
  @moduledoc false
  use GenServer

  @interval_ms 1_000

  def start_link(opts \\ []),
    do: GenServer.start_link(__MODULE__, opts, name: Keyword.get(opts, :name, __MODULE__))

  def scrape(name \\ __MODULE__), do: GenServer.call(name, :scrape, 30_000)

  @doc false
  @spec fold(GenServer.server()) :: :ok
  def fold(name \\ __MODULE__), do: GenServer.call(name, :fold, 30_000)

  @impl true
  def init(opts) do
    prometheus_name = Keyword.get(opts, :prometheus_name, :prometheus_metrics)
    state = %{body: TelemetryMetricsPrometheus.Core.scrape(prometheus_name)}

    state = %{
      body: state.body,
      interval_ms: Keyword.get(opts, :interval_ms, @interval_ms),
      fold_notify: Keyword.get(opts, :fold_notify),
      prometheus_name: prometheus_name,
      before_scrape: Keyword.get(opts, :before_scrape)
    }

    {:ok, state, {:continue, :schedule}}
  end

  @impl true
  def handle_continue(:schedule, state) do
    schedule(state.interval_ms)
    {:noreply, state}
  end

  @impl true
  def handle_call(:scrape, _from, state), do: {:reply, state.body, state}

  def handle_call(:fold, _from, state), do: {:reply, :ok, fold_now(state)}

  @impl true
  def handle_info(:fold, state), do: {:noreply, fold_now(state), {:continue, :schedule}}

  defp fold_now(state) do
    if is_function(state.before_scrape, 0), do: state.before_scrape.()
    body = TelemetryMetricsPrometheus.Core.scrape(state.prometheus_name)
    if is_pid(state.fold_notify), do: send(state.fold_notify, {:prometheus_folded, self()})
    %{state | body: body}
  end

  defp schedule(interval_ms), do: Process.send_after(self(), :fold, interval_ms)
end
