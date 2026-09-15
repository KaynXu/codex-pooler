defmodule CodexPoolerWeb.Telemetry.PrometheusReporterTest do
  use ExUnit.Case, async: false

  alias CodexPoolerWeb.Telemetry.PrometheusReporter

  test "HTTP scrapes reuse the scheduled Core rendering" do
    registry = unique_name()
    event = [:codex_pooler_test, :cached, unique_event_atom()]
    metric = Telemetry.Metrics.sum(event, event_name: event, measurement: :count)

    start_supervised!(
      {TelemetryMetricsPrometheus.Core, metrics: [metric], name: registry, start_async: false}
    )

    reporter = unique_name()

    pid =
      start_supervised!(
        {PrometheusReporter, name: reporter, prometheus_name: registry, interval_ms: 60_000}
      )

    initial = PrometheusReporter.scrape(reporter)
    :telemetry.execute(event, %{count: 7}, %{})
    assert PrometheusReporter.scrape(reporter) == initial
    send(pid, :fold)
    :sys.get_state(pid)
    refute PrometheusReporter.scrape(reporter) == initial
  end

  test "fold/1 refreshes the cached rendering synchronously" do
    registry = unique_name()
    event = [:codex_pooler_test, :sync_fold, unique_event_atom()]
    metric = Telemetry.Metrics.sum(event, event_name: event, measurement: :count)

    start_supervised!(
      {TelemetryMetricsPrometheus.Core, metrics: [metric], name: registry, start_async: false}
    )

    reporter = unique_name()

    start_supervised!(
      {PrometheusReporter,
       name: reporter, prometheus_name: registry, interval_ms: 60_000, fold_notify: self()}
    )

    initial = PrometheusReporter.scrape(reporter)
    :telemetry.execute(event, %{count: 3}, %{})
    assert PrometheusReporter.scrape(reporter) == initial
    assert :ok = PrometheusReporter.fold(reporter)
    assert_received {:prometheus_folded, _pid}
    refute PrometheusReporter.scrape(reporter) == initial
  end

  test "bad tags are dropped on fold and cached scrapes do not repeat warnings" do
    registry = unique_name()
    event = [:codex_pooler_test, :bad_tag, unique_event_atom()]
    metric = Telemetry.Metrics.sum(event, event_name: event, measurement: :count, tags: [:kind])

    start_supervised!(
      {TelemetryMetricsPrometheus.Core, metrics: [metric], name: registry, start_async: false}
    )

    reporter = unique_name()

    pid =
      start_supervised!(
        {PrometheusReporter, name: reporter, prometheus_name: registry, interval_ms: 60_000}
      )

    :telemetry.execute(event, %{count: 1}, %{kind: %{invalid: true}})

    log =
      ExUnit.CaptureLog.capture_log(fn ->
        send(pid, :fold)
        :sys.get_state(pid)
      end)

    assert log =~ "bad tag value"

    refute ExUnit.CaptureLog.capture_log(fn ->
             for _ <- 1..3, do: PrometheusReporter.scrape(reporter)
             send(pid, :fold)
             :sys.get_state(pid)
           end) =~ "bad tag value"
  end

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
    send(reporter, :fold)
    :sys.get_state(reporter)
    assert PrometheusReporter.scrape(reporter) == direct
    assert direct =~ "codex_pooler_test_isolated_distribution"
    assert direct =~ "kind=\"isolated\""
  end

  test "serializes interleaved batches without losing updates" do
    registry = unique_name()
    event = [:codex_pooler_test, :interleaved, unique_event_atom()]

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

    parent = self()
    release = make_ref()
    scrape_count = :atomics.new(1, [])
    reporter = unique_name()

    start_supervised!(
      {PrometheusReporter,
       name: reporter,
       prometheus_name: registry,
       interval_ms: 60_000,
       before_scrape: fn ->
         if :atomics.add_get(scrape_count, 1, 1) == 1 do
           send(parent, {:scrape_barrier, self()})

           receive do
             {:release, ^release} -> :ok
           end
         end
       end}
    )

    for value <- [5, 15, 40], do: :telemetry.execute(event, %{value: value}, %{kind: "isolated"})
    send(reporter, :fold)
    callers = for _ <- 1..8, do: Task.async(fn -> PrometheusReporter.scrape(reporter) end)
    assert_receive {:scrape_barrier, _pid}
    for value <- [5, 15, 40], do: :telemetry.execute(event, %{value: value}, %{kind: "isolated"})
    send(reporter, {:release, release})
    bodies = Enum.map(callers, &Task.await(&1, 1_000))
    assert Enum.uniq(bodies) |> length() == 1
    body = hd(bodies)
    assert body =~ "_bucket{kind=\"isolated\",le=\"10\"} 2"
    assert body =~ "_bucket{kind=\"isolated\",le=\"20\"} 4"
    assert body =~ "_bucket{kind=\"isolated\",le=\"50\"} 6"
    assert body =~ "_sum{kind=\"isolated\"} 120"
    assert body =~ "_count{kind=\"isolated\"} 6"

    %{dist_table_id: dist_table} = TelemetryMetricsPrometheus.Core.Registry.config(registry)
    assert :ets.lookup(dist_table, metric.name) == []
  end

  defp unique_name, do: Module.concat(__MODULE__, "Reporter#{System.unique_integer([:positive])}")

  defp unique_event_atom, do: String.to_atom("event_#{System.unique_integer([:positive])}")
end
