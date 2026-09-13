defmodule CodexPoolerWeb.Telemetry.PrometheusReporter do
  @moduledoc false
  use GenServer

  @interval_ms 1_000

  def start_link(opts \\ []),
    do: GenServer.start_link(__MODULE__, opts, name: Keyword.get(opts, :name, __MODULE__))

  def scrape(name \\ __MODULE__), do: GenServer.call(name, :scrape, 30_000)

  @impl true
  def init(opts) do
    state = %{body: TelemetryMetricsPrometheus.Core.scrape()}

    state = %{
      body: state.body,
      interval_ms: Keyword.get(opts, :interval_ms, @interval_ms),
      fold_notify: Keyword.get(opts, :fold_notify)
    }

    {:ok, state, {:continue, :schedule}}
  end

  @impl true
  def handle_continue(:schedule, state) do
    schedule(state.interval_ms)
    {:noreply, state}
  end

  @impl true
  def handle_call(:scrape, _from, state) do
    body = TelemetryMetricsPrometheus.Core.scrape()
    {:reply, body, %{state | body: body}}
  end

  @impl true
  def handle_info(:fold, state) do
    body = TelemetryMetricsPrometheus.Core.scrape()
    if is_pid(state.fold_notify), do: send(state.fold_notify, {:prometheus_folded, self()})
    {:noreply, %{state | body: body}, {:continue, :schedule}}
  end

  defp schedule(interval_ms), do: Process.send_after(self(), :fold, interval_ms)
end
