defmodule CodexPooler.Status.FeedClientTest do
  use ExUnit.Case, async: false

  alias CodexPooler.FakeUpstream
  alias CodexPooler.Status.FeedClient
  alias CodexPooler.UpstreamConnPoolTelemetry

  test "fetch parses a real-format provider response through Req" do
    xml = File.read!(Path.join(__DIR__, "fixtures/incident_io.rss"))

    {:ok, feed} =
      FakeUpstream.start_link(
        {:raw_body, 200, xml, [{"content-type", "application/rss+xml; charset=utf-8"}]}
      )

    on_exit(fn -> FakeUpstream.stop(feed) end)

    assert {:ok, %{items: [%{status: "Monitoring", component: component}], complete?: true}} =
             FeedClient.fetch(%{}, url: FakeUpstream.url(feed) <> "/feed.rss")

    assert component == "Responses (Operational) Codex (Operational)"
  end

  test "rejects a response beyond the feed byte bound" do
    {:ok, feed} =
      FakeUpstream.start_link(
        {:raw_body, 200, String.duplicate("x", 1_000_001),
         [{"content-type", "application/rss+xml"}]}
      )

    on_exit(fn -> FakeUpstream.stop(feed) end)

    assert {:error, %{code: :body_too_large}} =
             FeedClient.fetch(%{}, url: FakeUpstream.url(feed) <> "/feed.rss")
  end

  test "classifies network failures with bounded metadata" do
    assert {:error, %{code: :network_error, message: "feed transport failed"}} =
             FeedClient.fetch(%{}, url: "http://127.0.0.1:1/feed.rss", timeout: 100)
  end

  test "feed polls carry the outbound connection idle bound from settings" do
    {:ok, feed} = FakeUpstream.start_link({:raw_body, 304, "", [{"etag", "\"feed-v1\""}]})
    on_exit(fn -> FakeUpstream.stop(feed) end)
    url = FakeUpstream.url(feed) <> "/feed.rss"

    UpstreamConnPoolTelemetry.put_idle_bound!(0)
    UpstreamConnPoolTelemetry.attach!(url)

    assert {:not_modified, %{etag: "\"feed-v1\""}} = FeedClient.fetch(%{}, url: url)
    assert {:not_modified, %{etag: "\"feed-v1\""}} = FeedClient.fetch(%{}, url: url)

    assert FakeUpstream.count(feed) == 2
    assert UpstreamConnPoolTelemetry.drain_events() == [:conn_max_idle_time_exceeded]
  end
end
