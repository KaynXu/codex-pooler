defmodule CodexPoolerWeb.Telemetry.PrometheusReporter do
  @moduledoc false
  use GenServer

  @interval_ms 1_000

  def start_link(opts \\ []),
    do: GenServer.start_link(__MODULE__, opts, name: Keyword.get(opts, :name, __MODULE__))

  def scrape(name \\ __MODULE__), do: GenServer.call(name, :scrape, 30_000)

  @impl true
  def init(opts) do
    prometheus_name = Keyword.get(opts, :prometheus_name, :prometheus_metrics)
    state = %{body: TelemetryMetricsPrometheus.Core.scrape(prometheus_name)}

    state = %{
      body: state.body,
      scrape_waiters: [],
      interval_ms: Keyword.get(opts, :interval_ms, @interval_ms),
      fold_notify: Keyword.get(opts, :fold_notify),
      prometheus_name: prometheus_name
    }

    {:ok, state, {:continue, :schedule}}
  end

  @impl true
  def handle_continue(:schedule, state) do
    schedule(state.interval_ms)
    {:noreply, state}
  end

  @impl true
  def handle_call(:scrape, from, state) do
    if state.scrape_waiters == [], do: send(self(), :scrape_batch)
    {:noreply, %{state | scrape_waiters: [from | state.scrape_waiters]}}
  end

  @impl true
  def handle_info(:scrape_batch, %{scrape_waiters: waiters} = state) do
    body = TelemetryMetricsPrometheus.Core.scrape(state.prometheus_name)
    Enum.each(waiters, &GenServer.reply(&1, body))
    {:noreply, %{state | body: body, scrape_waiters: []}}
  end

  @impl true
  def handle_info(:fold, state) do
    body = TelemetryMetricsPrometheus.Core.scrape(state.prometheus_name)
    if is_pid(state.fold_notify), do: send(state.fold_notify, {:prometheus_folded, self()})
    {:noreply, %{state | body: body}, {:continue, :schedule}}
  end

  defp schedule(interval_ms), do: Process.send_after(self(), :fold, interval_ms)
end
