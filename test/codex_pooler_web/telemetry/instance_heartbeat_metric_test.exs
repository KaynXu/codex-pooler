defmodule CodexPoolerWeb.Telemetry.InstanceHeartbeatMetricTest do
  use CodexPooler.DataCase, async: false

  alias CodexPooler.Platform.{InstanceHeartbeat, InstancePresence}
  alias CodexPoolerWeb.Telemetry

  test "actual heartbeat write failures reach the no-label Prometheus counter" do
    metric =
      Enum.find(
        Telemetry.prometheus_metrics(),
        &(&1.name == [:codex_pooler, :instance_presence, :heartbeat_failure, :count])
      )

    assert metric
    assert metric.tags == []
    registry = :heartbeat_failure_metric_test

    start_supervised!(
      {TelemetryMetricsPrometheus.Core, metrics: [metric], name: registry, start_async: false}
    )

    logs =
      ExUnit.CaptureLog.capture_log(fn ->
        pid =
          start_supervised!(
            {InstanceHeartbeat,
             enabled: true,
             name: :heartbeat_metric_writer,
             interval_ms: :timer.minutes(5),
             identity: %InstancePresence.Identity{
               instance_id: nil,
               node_name: "sample",
               boot_id: "sample"
             }}
          )

        :sys.get_state(pid)
        stop_supervised!(InstanceHeartbeat)
      end)

    assert logs =~ "instance presence heartbeat write failed"
    body = TelemetryMetricsPrometheus.Core.scrape(registry)
    assert body =~ "codex_pooler_instance_presence_heartbeat_failure_count 1"
    refute body =~ "codex_pooler_instance_presence_heartbeat_failure_count{"
  end
end
