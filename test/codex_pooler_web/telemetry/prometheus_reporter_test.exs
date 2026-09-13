defmodule CodexPoolerWeb.Telemetry.PrometheusReporterTest do
  use ExUnit.Case, async: false

  alias CodexPoolerWeb.Telemetry.PrometheusReporter

  test "periodically folds unscripted distribution samples and serializes concurrent scrapes" do
    name = unique_name()

    {:ok, pid} =
      start_supervised({PrometheusReporter, name: name, interval_ms: 10, fold_notify: self()})

    for _ <- 1..5 do
      :telemetry.execute(
        [:codex_pooler, :gateway, :stream, :buffer, :oversized],
        %{bytes: 65_536},
        %{buffer: "test", endpoint: "test", route_class: "proxy_stream", transport: "http_sse"}
      )
    end

    assert_receive {:prometheus_folded, ^pid}, 1_000
    tasks = for _ <- 1..8, do: Task.async(fn -> PrometheusReporter.scrape(name) end)
    bodies = Enum.map(tasks, &Task.await(&1, 1_000))
    assert Enum.uniq(bodies) |> length() == 1
    assert is_binary(hd(bodies))
    assert hd(bodies) == PrometheusReporter.scrape(name)
    assert hd(bodies) =~ "codex_pooler_gateway_admission_queued"
  end

  test "scrapes an isolated real Core registry and matches its direct output" do
    registry = unique_name()
    event = [:codex_pooler_test, :isolated_distribution, unique_event_atom()]

    metric =
      Telemetry.Metrics.distribution(event,
        event_name: event,
        measurement: :value,
        tags: [:kind],
        reporter_options: [buckets: [10, 20, 50]]
      )

    start_supervised!(
      {TelemetryMetricsPrometheus.Core, metrics: [metric], name: registry, start_async: false}
    )

    reporter = unique_name()

    start_supervised!(
      {PrometheusReporter, name: reporter, prometheus_name: registry, interval_ms: 60_000}
    )

    for value <- [5, 15, 40] do
      :telemetry.execute(event, %{value: value}, %{kind: "isolated"})
    end

    %{dist_table_id: dist_table, aggregates_table_id: aggregate_table} =
      TelemetryMetricsPrometheus.Core.Registry.config(registry)

    metric_name = metric.name

    assert :ets.lookup(dist_table, metric_name) |> length() == 3

    direct = TelemetryMetricsPrometheus.Core.scrape(registry)
    assert :ets.lookup(dist_table, metric_name) == []

    assert [{{^metric_name, %{kind: "isolated"}}, {buckets, 3, 60}}] =
             :ets.lookup(aggregate_table, {metric_name, %{kind: "isolated"}})

    assert buckets == [{"10", 1}, {"20", 2}, {"50", 3}, {"+Inf", 3}]
    assert PrometheusReporter.scrape(reporter) == direct
    assert direct =~ "codex_pooler_test_isolated_distribution"
    assert direct =~ "kind=\"isolated\""
  end

  defp unique_name, do: Module.concat(__MODULE__, "Reporter#{System.unique_integer([:positive])}")

  defp unique_event_atom, do: String.to_atom("event_#{System.unique_integer([:positive])}")
end
