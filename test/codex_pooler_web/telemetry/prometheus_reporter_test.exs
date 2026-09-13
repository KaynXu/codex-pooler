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

  defp unique_name, do: Module.concat(__MODULE__, "Reporter#{System.unique_integer([:positive])}")
end
