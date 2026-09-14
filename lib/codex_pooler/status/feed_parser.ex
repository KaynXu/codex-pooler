defmodule CodexPooler.Status.FeedParser do
  @moduledoc "Bounded, metadata-only parser for the OpenAI status RSS feed."

  @max_bytes 1_000_000
  @max_items 100
  @max_text 4_000
  @max_guid 512
  @max_link 2_048
  @future_skew_seconds 300

  @type item :: %{
          guid: String.t(),
          title: String.t(),
          status: String.t(),
          active?: boolean(),
          summary: String.t(),
          component: String.t() | nil,
          link: String.t(),
          published_at: DateTime.t()
        }

  @spec parse(binary(), keyword()) ::
          {:ok,
           %{
             items: [item()],
             content_hash: String.t(),
             skipped_count: non_neg_integer(),
             complete?: boolean()
           }}
          | {:error, map()}
  def parse(xml, opts \\ [])

  def parse(xml, opts) when is_binary(xml) do
    now = Keyword.get(opts, :now, DateTime.utc_now())

    cond do
      byte_size(xml) > @max_bytes ->
        error(:body_too_large, "feed body exceeds limit")

      byte_size(xml) == 0 ->
        error(:malformed_xml, "feed body is empty")

      not String.valid?(xml) or String.contains?(xml, <<0>>) ->
        error(:unsafe_xml, "feed must use UTF-8")

      Regex.match?(~r/<\?xml[^?]*encoding\s*=\s*["'](?!utf-8["'])[^"']+["']/i, xml) ->
        error(:unsafe_xml, "feed must use UTF-8")

      Regex.match?(~r/<!(?:DOCTYPE|ENTITY)\b/i, xml) ->
        error(:unsafe_xml, "doctype and entities are not accepted")

      true ->
        parse_xml(xml, now)
    end
  end

  def parse(_, _), do: error(:invalid_body, "feed body must be binary")

  defp parse_xml(xml, now) do
    # SAX preserves names as strings, unlike DOM scanning which interns provider names.
    initial = %{path: [], fields: %{}, items: [], channel?: false}

    case :xmerl_sax_parser.stream(xml, [
           :disallow_entities,
           {:external_entities, :none},
           {:fail_undeclared_ref, true},
           {:event_state, initial},
           {:event_fun, &sax_event/3}
         ]) do
      {:ok, %{channel?: true, items: nodes}, rest} ->
        if String.trim(to_string(rest)) == "",
          do: parse_nodes(nodes, now),
          else: error(:malformed_xml, "unexpected trailing XML")

      {:ok, _, _} ->
        error(:invalid_feed, "feed must contain an RSS channel")

      _ ->
        error(:malformed_xml, "feed XML could not be parsed")
    end
  catch
    :exit, _ -> error(:malformed_xml, "feed XML could not be parsed")
    _, _ -> error(:malformed_xml, "feed XML could not be parsed")
  end

  defp sax_event({:startElement, _, name, _, _}, _, state) do
    path = [List.to_string(name) | state.path]
    state = %{state | path: path, channel?: state.channel? or path == ["channel", "rss"]}
    if path == ["item", "channel", "rss"], do: %{state | fields: %{}}, else: state
  end

  defp sax_event({:endElement, _, _, _}, _, %{path: ["item", "channel", "rss"]} = state),
    do: %{state | path: ["channel", "rss"], items: [state.fields | state.items], fields: %{}}

  defp sax_event({:endElement, _, _, _}, _, %{path: [_ | rest]} = state),
    do: %{state | path: rest}

  defp sax_event({kind, text}, _, %{path: [field, "item", "channel", "rss"]} = state)
       when kind in [:characters, :ignorableWhitespace] do
    value = List.to_string(text)
    %{state | fields: Map.update(state.fields, field, value, &(&1 <> value))}
  end

  defp sax_event(_, _, state), do: state

  defp parse_nodes(nodes, now) do
    with {:ok, parsed, skipped} <- parse_items(nodes, now),
         {:ok, items} <- deduplicate(parsed) do
      complete? = skipped == 0 and length(items) <= @max_items
      items = Enum.take(items, @max_items)

      hash_fields =
        Enum.map(
          items,
          &Map.take(&1, [
            :guid,
            :title,
            :status,
            :summary,
            :component,
            :link,
            :hash_published_at
          ])
        )

      {:ok,
       %{
         items: Enum.map(items, &Map.delete(&1, :hash_published_at)),
         content_hash: hash(hash_fields),
         skipped_count: skipped,
         complete?: complete?
       }}
    end
  end

  defp parse_items(nodes, now) do
    {valid, errors} =
      Enum.reduce(nodes, {[], []}, fn children, {acc, errors} ->
        case parse_fields(children, now) do
          {:ok, item} -> {[item | acc], errors}
          {:error, reason} -> {acc, [reason | errors]}
        end
      end)

    case {valid, errors} do
      {[], [error | _]} -> {:error, error}
      _ -> {:ok, valid, length(errors)}
    end
  end

  defp parse_fields(fields, now) do
    with :ok <- validate_explicit_status(fields),
         {:ok, guid} <- required(fields, "guid", @max_guid),
         {:ok, title} <- required(fields, "title", @max_text),
         {:ok, published_raw} <- required(fields, "pubDate", @max_text),
         {:ok, published} <- parse_date(published_raw, now),
         {:ok, link} <- safe_link(Map.get(fields, "link", "")),
         {:ok, description} <- description(fields),
         {:ok, status_raw} <- extract_status(description, Map.get(fields, "status", "")),
         {:ok, status} <- normalize_status(status_raw),
         {:ok, summary} <- bounded_text(strip_html(description), @max_text) do
      {:ok,
       %{
         guid: guid,
         title: title,
         status: status,
         active?: status != "Resolved",
         summary: summary,
         component: extract_component(description),
         link: link,
         published_at: published,
         hash_published_at: published_raw
       }}
    end
  end

  defp validate_explicit_status(fields) do
    if Map.has_key?(fields, "status") and String.trim(Map.get(fields, "status", "")) == "" do
      error(:missing_status, "status is blank")
    else
      :ok
    end
  end

  defp required(fields, key, limit) do
    value = String.trim(Map.get(fields, key, ""))

    cond do
      value == "" -> error(:missing_field, "required feed field is missing")
      byte_size(value) > limit -> error(:field_too_large, "feed field exceeds limit")
      unsafe_text?(value) -> error(:unsafe_text, "feed field contains unsafe text")
      true -> {:ok, value}
    end
  end

  defp description(fields) do
    value = Map.get(fields, "description", "")
    fallback = Map.get(fields, "encoded", "")

    if String.trim(value) != "",
      do: {:ok, value},
      else:
        if(String.trim(fallback) != "",
          do: {:ok, fallback},
          else: error(:missing_description, "feed description is missing")
        )
  end

  defp extract_status(description, explicit_status) do
    plain = strip_html(description)

    case Regex.run(
           ~r/\bstatus\s*[:\-]?\s*(investigating|identified|monitoring|resolved)\b/i,
           plain
         ) do
      [_, status] ->
        {:ok, status}

      _ ->
        extract_unrecognized_status(plain, explicit_status)
    end
  end

  defp extract_unrecognized_status(plain, explicit_status) do
    case Regex.run(~r/\bstatus\s*[:\-]?\s*([A-Za-z][A-Za-z _-]{0,63})\b/i, plain) do
      [_, status] ->
        case String.trim(status) do
          "" -> error(:missing_status, "status is missing from feed description")
          trimmed -> {:ok, trimmed}
        end

      _ ->
        fallback_status(plain, explicit_status)
    end
  end

  defp fallback_status(plain, explicit_status) do
    case String.trim(explicit_status) do
      "" ->
        if Regex.match?(~r/\bstatus\b/i, plain),
          do: error(:missing_status, "status is missing from feed description"),
          else: error(:missing_field, "required feed field is missing")

      trimmed ->
        {:ok, trimmed}
    end
  end

  defp normalize_status(value) do
    case String.downcase(String.trim(value)) do
      "investigating" -> {:ok, "Investigating"}
      "identified" -> {:ok, "Identified"}
      "monitoring" -> {:ok, "Monitoring"}
      "resolved" -> {:ok, "Resolved"}
      "" -> error(:missing_status, "status is blank")
      _ -> {:ok, "Unknown"}
    end
  end

  defp extract_component(description) do
    plain = strip_html(description)

    case Regex.run(~r/affected\s+components?\s*:?\s+(.+)\z/iu, plain) do
      [_, value] ->
        value |> String.trim() |> String.slice(0, 512) |> blank_to_nil()

      _ ->
        nil
    end
  end

  defp safe_link(value) do
    value = String.trim(value)

    if byte_size(value) > @max_link,
      do: error(:field_too_large, "feed link exceeds limit"),
      else: do_safe_link(value)
  end

  defp do_safe_link(value) do
    case URI.parse(value) do
      %URI{
        scheme: "https",
        host: "status.openai.com",
        port: port,
        userinfo: nil,
        query: nil,
        fragment: nil,
        path: path
      }
      when is_binary(path) and path != "" ->
        safe_link_path(port, path)

      _ ->
        error(:unsafe_link, "feed link is not an allowed HTTPS status URL")
    end
  end

  defp safe_link_path(port, _path) when port not in [nil, 443],
    do: error(:unsafe_link, "feed link uses an unsafe port")

  defp safe_link_path(_port, path) do
    normalized = "/" <> String.trim_leading(path, "/")

    if normalized == "/",
      do: error(:unsafe_link, "feed link path is empty"),
      else: {:ok, "https://status.openai.com" <> normalized}
  end

  defp parse_date(value, now) do
    value = String.trim(value)
    parsed = DateTime.from_iso8601(value)
    parsed = if match?({:error, _}, parsed), do: rfc822(value), else: parsed

    case parsed do
      {:ok, dt, _} ->
        if DateTime.diff(dt, now, :second) <= @future_skew_seconds,
          do: {:ok, dt},
          else: error(:invalid_date, "feed date exceeds allowed clock skew")

      _ ->
        error(:invalid_date, "feed date is invalid")
    end
  end

  defp rfc822(value) do
    case Regex.run(
           ~r/^\w{3},\s+(\d{1,2})\s+(\w{3})\s+(\d{4})\s+(\d{2}):(\d{2}):(\d{2})\s+(GMT|UTC|[+-]\d{4})$/i,
           value
         ) do
      [_, day, month, year, hh, mm, ss, zone] ->
        months = %{
          "jan" => 1,
          "feb" => 2,
          "mar" => 3,
          "apr" => 4,
          "may" => 5,
          "jun" => 6,
          "jul" => 7,
          "aug" => 8,
          "sep" => 9,
          "oct" => 10,
          "nov" => 11,
          "dec" => 12
        }

        with {:ok, date} <- Date.new(to_int(year), months[String.downcase(month)], to_int(day)),
             {:ok, time} <- Time.new(to_int(hh), to_int(mm), to_int(ss), 0),
             {:ok, seconds} <- offset(String.upcase(zone)),
             {:ok, dt} <- DateTime.new(date, time, "Etc/UTC") do
          {:ok, DateTime.add(dt, -seconds, :second), seconds}
        else
          _ -> {:error, :invalid_date}
        end

      _ ->
        {:error, :invalid_date}
    end
  rescue
    _ -> {:error, :invalid_date}
  end

  defp offset(zone) when zone in ["GMT", "UTC"], do: {:ok, 0}

  defp offset(<<sign, hh::binary-size(2), mm::binary-size(2)>>) do
    hours = to_int(hh)
    minutes = to_int(mm)

    if hours < 24 and minutes < 60 do
      seconds = (hours * 60 + minutes) * 60
      {:ok, if(sign == ?-, do: -seconds, else: seconds)}
    else
      {:error, :invalid_date}
    end
  end

  defp to_int(v), do: String.to_integer(v)

  defp strip_html(value) do
    value
    |> String.replace(~r/<[^>]*>/u, " ")
    |> decode_entities()
    |> String.replace(~r/\s+/u, " ")
    |> String.trim()
  end

  defp decode_entities(value) do
    value
    |> String.replace("&amp;", "&")
    |> String.replace("&lt;", "<")
    |> String.replace("&gt;", ">")
    |> String.replace("&quot;", "\"")
    |> String.replace("&#39;", "'")
    |> then(&Regex.replace(~r/&#x([0-9a-fA-F]{1,6});/, &1, fn _, hex -> codepoint(hex, 16) end))
    |> then(&Regex.replace(~r/&#([0-9]{1,7});/, &1, fn _, dec -> codepoint(dec, 10) end))
  end

  defp codepoint(value, base) do
    case Integer.parse(value, base) do
      {n, ""} when n > 0 and n <= 0x10FFFF and n not in 0xD800..0xDFFF -> <<n::utf8>>
      _ -> " "
    end
  end

  defp bounded_text(value, limit),
    do:
      if(byte_size(value) > limit,
        do: error(:field_too_large, "feed text exceeds limit"),
        else: {:ok, value}
      )

  defp unsafe_text?(value), do: String.contains?(value, <<0>>) or not String.valid?(value)
  defp blank_to_nil(""), do: nil
  defp blank_to_nil(value), do: value

  defp deduplicate(items),
    do:
      {:ok,
       items
       |> Enum.group_by(& &1.guid)
       |> Enum.map(fn {_guid, xs} ->
         Enum.max_by(xs, &{DateTime.to_unix(&1.published_at, :microsecond), hash(&1)})
       end)
       |> Enum.sort_by(&{-DateTime.to_unix(&1.published_at, :microsecond), &1.guid})}

  defp hash(fields),
    do:
      :crypto.hash(:sha256, :erlang.term_to_binary(fields, [:deterministic]))
      |> Base.encode16(case: :lower)

  defp error(code, message), do: {:error, %{code: code, message: message}}
end
