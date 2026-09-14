defmodule CodexPooler.Status.FeedParserTest do
  use ExUnit.Case, async: false
  alias CodexPooler.Status.FeedParser

  @now ~U[2026-09-10 00:00:00Z]

  test "parses incident.io CDATA, GMT, Unicode and double slash source links" do
    xml = File.read!(Path.join(__DIR__, "fixtures/incident_io.rss"))
    assert {:ok, %{items: [item], complete?: true}} = FeedParser.parse(xml, now: @now)
    assert item.status == "Monitoring"
    assert DateTime.compare(item.published_at, ~U[2026-09-09 12:00:00Z]) == :eq
    assert item.link == "https://status.openai.com/incidents/sample-incident"
    assert item.summary =~ "café"
    assert item.component == "Responses (Operational) Codex (Operational)"
  end

  test "bounds long component facts without substituting another component" do
    xml =
      entry("components", @now)
      |> String.replace(
        "components: API",
        "components: " <> String.duplicate("Responses (Operational) ", 30)
      )

    assert {:ok, %{items: [item]}} = FeedParser.parse(feed([xml]))
    assert String.starts_with?(item.component, "Responses (Operational)")
    assert String.length(item.component) <= 512
  end

  test "rejects DTD, custom entities and malformed XML while accepting UTF-8 BOM" do
    xml = feed([entry("safe", @now)])
    assert {:ok, _} = FeedParser.parse(<<239, 187, 191>> <> xml)

    for prefix <- [
          "<!DOCTYPE rss SYSTEM 'https://example.com/feed'>",
          "<!DOCTYPE rss [<!ENTITY value 'sample'>]>"
        ] do
      assert {:error, %{code: :unsafe_xml}} = FeedParser.parse(prefix <> xml)
    end

    assert {:error, _} = FeedParser.parse("<rss><channel><item>")
    assert {:error, _} = FeedParser.parse("<rss><channel>&undefined;</channel></rss>")
    assert {:error, %{code: :body_too_large}} = FeedParser.parse(String.duplicate("x", 1_000_001))
  end

  test "external element and attribute names do not grow the atom table after warming" do
    assert {:ok, _} = FeedParser.parse(feed([entry("warm", @now)]))
    assert {:ok, _} = FeedParser.parse("<rss><channel><warm attribute='value'/></channel></rss>")

    for n <- 1..200 do
      assert {:ok, _} =
               FeedParser.parse(
                 "<rss><channel><warm_name_#{n} warm_attribute_#{n}='value'/></channel></rss>"
               )
    end

    before_count = :erlang.system_info(:atom_count)

    for n <- 1..200 do
      assert {:ok, _} =
               FeedParser.parse(
                 "<rss><channel><external_name_#{n} external_attribute_#{n}='value'/></channel></rss>"
               )
    end

    assert :erlang.system_info(:atom_count) == before_count
    assert_raise ArgumentError, fn -> String.to_existing_atom("external_name_200") end
  end

  test "selects newest 100 independently of feed order and marks truncation" do
    entries = for n <- 1..105, do: entry("item-#{n}", DateTime.add(@now, -n, :second))
    assert {:ok, parsed} = FeedParser.parse(feed(Enum.reverse(entries)), now: @now)
    assert length(parsed.items) == 100
    assert hd(parsed.items).guid == "item-1"
    assert List.last(parsed.items).guid == "item-100"
    assert parsed.complete? == false
    assert parsed.skipped_count == 0
  end

  test "skips invalid entries while protecting incomplete snapshots" do
    assert {:ok, parsed} = FeedParser.parse(feed([entry("valid", @now), "<item/>"]))
    assert [%{guid: "valid"}] = parsed.items
    assert parsed.skipped_count == 1
    refute parsed.complete?
    assert {:error, _} = FeedParser.parse(feed(["<item/>"]))
  end

  test "resolves equal-date duplicates deterministically" do
    a = entry("same", @now)
    b = String.replace(a, "Incident", "Revised incident")
    assert {:ok, first} = FeedParser.parse(feed([a, b]))
    assert {:ok, second} = FeedParser.parse(feed([b, a]))
    assert first == second
  end

  test "equal provider timestamps use GUID ordering independent of document order" do
    entries = [entry("z-last", @now), entry("a-first", @now)]
    assert {:ok, parsed} = FeedParser.parse(feed(entries), now: @now)
    assert Enum.map(parsed.items, & &1.guid) == ["a-first", "z-last"]
    assert {:ok, ^parsed} = FeedParser.parse(feed(Enum.reverse(entries)), now: @now)
  end

  test "rejects non UTF-8 encodings before XML parsing" do
    xml = feed([entry("encoding", @now)])
    utf16 = :unicode.characters_to_binary(xml, :utf8, {:utf16, :little})
    assert {:error, %{code: :unsafe_xml}} = FeedParser.parse(<<255, 254>> <> utf16)

    assert {:error, %{code: :unsafe_xml}} =
             FeedParser.parse(~s(<?xml version="1.0" encoding="ISO-8859-1"?>) <> xml)
  end

  defp feed(entries), do: "<rss><channel>#{Enum.join(entries)}</channel></rss>"

  defp entry(guid, date) do
    "<item><guid>#{guid}</guid><title>Incident</title><description><![CDATA[<p>Status: Investigating</p><p>Affected components: API</p>]]></description><link>https://status.openai.com/incidents/#{guid}</link><pubDate>#{DateTime.to_iso8601(date)}</pubDate></item>"
  end

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

  test "normalizes bounded RSS items, deduplicates guid, and preserves allowed clock skew" do
    xml = """
    <rss><channel>
      <item><guid>g1</guid><title>Outage</title><status>Investigating</status><description><![CDATA[hello]]></description><link>https://status.openai.com/incidents/g1</link><pubDate>2026-09-10T00:01:00Z</pubDate></item>
      <item><guid>g1</guid><title>Duplicate</title><status>Resolved</status><description>ignored</description><link>https://status.openai.com/incidents/g1</link><pubDate>2026-09-09T00:00:00Z</pubDate></item>
    </channel></rss>
    """

    assert {:ok, %{items: [item], content_hash: hash}} = FeedParser.parse(xml, now: @now)
    assert item.guid == "g1"
    assert item.status == "Investigating"
    assert item.summary == "hello"
    assert item.published_at == DateTime.add(@now, 60, :second)
    assert is_binary(hash) and byte_size(hash) == 64
  end

  test "future chronology stays identical across polls within skew and absurd dates are skipped" do
    xml = feed([entry("skew", DateTime.add(@now, 60, :second))])
    assert {:ok, first} = FeedParser.parse(xml, now: @now)
    assert {:ok, second} = FeedParser.parse(xml, now: DateTime.add(@now, 30, :second))
    assert first == second

    assert {:error, %{code: :invalid_date}} =
             FeedParser.parse(feed([entry("future", DateTime.add(@now, 301, :second))]),
               now: @now
             )
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
