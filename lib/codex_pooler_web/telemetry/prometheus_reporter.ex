defmodule CodexPoolerWeb.Telemetry.PrometheusReporter do
  @moduledoc false
  use GenServer

  @interval_ms 1_000

  def start_link(opts \\ []), do: GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  def scrape, do: GenServer.call(__MODULE__, :scrape, 30_000)

  @impl true
  def init(_opts) do
    state = %{body: TelemetryMetricsPrometheus.Core.scrape()}
    {:ok, state, {:continue, :schedule}}
  end

  @impl true
  def handle_continue(:schedule, state) do
    schedule()
    {:noreply, state}
  end

  @impl true
  def handle_call(:scrape, _from, state) do
    body = TelemetryMetricsPrometheus.Core.scrape()
    {:reply, body, %{state | body: body}}
  end

  @impl true
  def handle_info(:fold, state) do
    {:noreply, %{state | body: TelemetryMetricsPrometheus.Core.scrape()}, {:continue, :schedule}}
  end

  defp schedule, do: Process.send_after(self(), :fold, @interval_ms)
end
