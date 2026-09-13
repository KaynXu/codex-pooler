defmodule CodexPooler.Status.FeedParserTest do
  use ExUnit.Case, async: true
  alias CodexPooler.Status.FeedParser

  @now ~U[2026-09-10 00:00:00Z]

  test "normalizes numeric RSS timezone offsets to UTC" do
    for {date, expected} <- [
          {"Wed, 09 Sep 2026 12:00:00 +0000", ~U[2026-09-09 12:00:00Z]},
          {"Wed, 09 Sep 2026 14:30:00 +0230", ~U[2026-09-09 12:00:00Z]},
          {"Wed, 09 Sep 2026 07:00:00 -0500", ~U[2026-09-09 12:00:00Z]}
        ] do
      xml = """
      <rss><channel><item><guid>offset</guid><title>Incident</title>
      <status>Monitoring</status><description>Service recovering</description>
      <link>https://status.openai.com/incidents/offset</link><pubDate>#{date}</pubDate>
      </item></channel></rss>
      """

      assert {:ok, %{items: [%{published_at: published_at}]}} = FeedParser.parse(xml, now: @now)
      assert DateTime.compare(published_at, expected) == :eq
    end
  end

  test "rejects non-feed XML instead of reporting an empty successful feed" do
    for xml <- ["<html><body>Temporarily unavailable</body></html>", "<error>unavailable</error>"] do
      assert {:error, %{code: :invalid_feed}} = FeedParser.parse(xml, now: @now)
    end

    assert {:ok, %{items: []}} = FeedParser.parse("<rss><channel/></rss>", now: @now)
  end

  test "normalizes bounded RSS items, deduplicates guid, and clamps future dates" do
    xml = """
    <rss><channel>
      <item><guid>g1</guid><title>Outage</title><status>Investigating</status><description><![CDATA[hello]]></description><link>https://status.openai.com/incidents/g1</link><pubDate>2026-09-11T00:00:00Z</pubDate></item>
      <item><guid>g1</guid><title>Duplicate</title><status>Resolved</status><description>ignored</description><link>https://status.openai.com/incidents/g1</link><pubDate>2026-09-09T00:00:00Z</pubDate></item>
    </channel></rss>
    """

    assert {:ok, %{items: [item], content_hash: hash}} = FeedParser.parse(xml, now: @now)
    assert item.guid == "g1"
    assert item.status == "Investigating"
    assert item.summary == "hello"
    assert item.published_at == @now
    assert is_binary(hash) and byte_size(hash) == 64
  end

  test "rejects unsafe XML, missing fields, and unsafe links" do
    assert {:error, %{code: :unsafe_xml}} = FeedParser.parse("<!DOCTYPE rss><rss/>")

    xml =
      "<rss><channel><item><guid>x</guid><title>T</title><status>Monitoring</status><link>http://evil.example/x</link><pubDate>2026-09-09T00:00:00Z</pubDate></item></channel></rss>"

    assert {:error, %{code: :unsafe_link}} = FeedParser.parse(xml, now: @now)
  end

  test "unknown nonblank status is active safe and blank status is rejected" do
    base =
      "<rss><channel><item><guid>x</guid><title>T</title><description>D</description><link>https://status.openai.com/incidents/x</link><pubDate>2026-09-09T00:00:00Z</pubDate>"

    assert {:ok, %{items: [%{status: "Unknown", active?: true}]}} =
             FeedParser.parse(base <> "<status>Deferred</status></item></channel></rss>",
               now: @now
             )

    assert {:error, %{code: :missing_field}} =
             FeedParser.parse(base <> "</item></channel></rss>", now: @now)

    for blank_status <- [" ", "\n\t"] do
      assert {:error, %{code: :missing_status}} =
               FeedParser.parse(base <> "<status>#{blank_status}</status></item></channel></rss>",
                 now: @now
               )
    end
  end
end
