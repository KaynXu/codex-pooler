defmodule CodexPoolerWeb.Admin.IncidentsLiveTest do
  use CodexPoolerWeb.ConnCase, async: false

  import Phoenix.LiveViewTest
  alias CodexPooler.OpenAIStatus
  alias CodexPooler.Repo
  alias CodexPooler.Status.Events
  alias CodexPooler.Status.Schemas.Incident
  alias CodexPooler.Status.Sync

  setup :register_and_log_in_user

  test "polling disabled is distinct from unavailable and stale", %{conn: conn} do
    settings = CodexPooler.InstanceSettings.ensure_singleton!()

    assert {:ok, _} =
             CodexPooler.InstanceSettings.update_system_settings(settings, %{
               "operator" => %{"openai_status_polling_enabled" => false}
             })

    {:ok, view, _} = live(conn, ~p"/admin/incidents")
    assert has_element?(view, "#admin-incidents-feed-state[data-state='disabled']")
    assert has_element?(view, "#admin-incidents-feed-disabled[role='status']")
    refute has_element?(view, "#admin-incidents-feed-unavailable")
    refute has_element?(view, "#admin-incidents-stale")
  end

  test "mount registers exactly one status subscription", %{conn: conn} do
    {:ok, view, _} = live(conn, ~p"/admin/incidents")

    entries =
      Registry.lookup(CodexPooler.PubSub, Events.topic())
      |> Enum.filter(fn {pid, _} -> pid == view.pid end)

    assert length(entries) == 1
  end

  test "real sync history is newest first beyond fifty rows", %{conn: conn} do
    now = ~U[2026-09-10 10:00:00.000000Z]

    items =
      for n <- 1..55 do
        %{
          guid: "history-sync-#{n}",
          title: "Historical incident #{n}",
          status: "Resolved",
          summary: "Service recovered",
          component: nil,
          link: "https://status.openai.com/incidents/sample-#{n}",
          published_at: DateTime.add(now, -n * 60, :second)
        }
      end

    assert {:ok, _} =
             Sync.sync(
               fetcher: fn _, _ -> {:ok, %{items: Enum.reverse(items)}} end,
               now: now
             )

    {:ok, view, _} = live(conn, ~p"/admin/incidents")

    rows =
      render(view)
      |> LazyHTML.from_fragment()
      |> LazyHTML.query("#admin-incidents-history-desktop tbody tr")

    assert Enum.count(rows) == 50
    assert rows |> Enum.at(0) |> LazyHTML.text() =~ "Historical incident 1"
    assert rows |> Enum.at(49) |> LazyHTML.text() =~ "Historical incident 50"
    assert has_element?(view, "#admin-incidents-history-overflow", "+5 more")
    assert has_element?(view, "#admin-incidents-history-desktop", "Unspecified")
  end

  test "a failed first poll remains unavailable and never claims no active incidents", %{
    conn: conn
  } do
    assert {:error, _} =
             Sync.sync(fetcher: fn _, _ -> {:error, %{code: :network_error}} end)

    {:ok, view, _} = live(conn, ~p"/admin/incidents")
    assert has_element?(view, "#admin-incidents-feed-state[data-state='unavailable']")
    refute has_element?(view, "#admin-incidents-active-empty", "No active incidents")
  end

  test "redirects unauthenticated operators to login" do
    assert {:error, {:redirect, %{to: "/login"}}} = live(build_conn(), ~p"/admin/incidents")
  end

  test "renders an empty read-only incidents page for authenticated operators", %{conn: conn} do
    {:ok, view, html} = live(conn, ~p"/admin/incidents")

    assert has_element?(view, "#admin-incidents-page")

    assert has_element?(
             view,
             "#admin-nav-incidents[href='/admin/incidents'][aria-current='page']"
           )

    assert has_element?(view, "#admin-incidents-active-section")
    assert has_element?(view, "#admin-incidents-history-section")
    assert has_element?(view, "#admin-incidents-active-empty")
    assert has_element?(view, "#admin-incidents-history-empty")
    assert has_element?(view, "#admin-incidents-feed-unavailable")

    assert has_element?(
             view,
             "#admin-incidents-feed-unavailable",
             "first successful refresh is still pending"
           )

    refute html =~ "last successful fetch was ."
    refute html =~ "Acknowledge"
    refute html =~ "Resolve incident"
    refute html =~ "Delete incident"
  end

  test "renders active and terminal incidents on equivalent desktop and mobile surfaces", %{
    conn: conn
  } do
    now = ~U[2026-09-10 12:00:00Z]
    active = incident_fixture("active-guid", "Investigating outage", "Investigating", now)

    resolved =
      incident_fixture(
        "resolved-guid",
        "Resolved outage",
        "Resolved",
        DateTime.add(now, -3600, :second)
      )

    retired =
      incident_fixture(
        "retired-guid",
        "Retired outage",
        "Monitoring",
        DateTime.add(now, -7200, :second)
      )

    Repo.update!(Incident.changeset(retired, %{retired_at: now, updated_at: now}))

    OpenAIStatus.upsert_feed_state(%{
      last_success_at: now,
      last_attempt_at: now,
      active_count: 1,
      aggregate_revision: 3,
      updated_at: now
    })

    {:ok, view, html} = live(conn, ~p"/admin/incidents")

    assert has_element?(view, "#admin-incidents-active-desktop")
    assert has_element?(view, "#admin-incidents-active-mobile")
    assert has_element?(view, "#admin-incidents-history-desktop")
    assert has_element?(view, "#admin-incidents-history-mobile")

    assert has_element?(
             view,
             "#admin-incidents-active-desktop-row-#{active.id}[data-role='openai-incident-row']"
           )

    assert has_element?(
             view,
             "#admin-incidents-active-mobile-card-#{active.id}[data-role='openai-incident-card']"
           )

    assert has_element?(
             view,
             "#admin-incidents-history-desktop-row-#{resolved.id}",
             "Resolved outage"
           )

    assert has_element?(
             view,
             "#admin-incidents-history-desktop-row-#{retired.id}",
             "Retired outage"
           )

    assert has_element?(
             view,
             "[data-role='incident-source-link'][href='https://status.openai.com/incidents/active-guid']"
           )

    assert has_element?(
             view,
             "[data-role='incident-status'][data-status='investigating']",
             "Investigating"
           )

    assert has_element?(view, "[data-role='incident-status'][data-status='resolved']", "Resolved")
    assert html =~ "Retired"
    refute html =~ "admin-alerts-incident"
    refute html =~ "phx-click=\"acknowledge"
    refute html =~ "phx-click=\"resolve"
  end

  test "shows stale and failed feed state while preserving incident metadata", %{conn: conn} do
    now = DateTime.utc_now()
    stale = DateTime.add(now, -1_800, :second)
    incident_fixture("stale-guid", "Stale outage", "Unknown", stale)

    assert {:ok, _state} =
             OpenAIStatus.upsert_feed_state(%{
               last_success_at: stale,
               last_attempt_at: now,
               last_error_at: now,
               last_error_code: "network_error",
               active_count: 1,
               aggregate_revision: 1,
               updated_at: now
             })

    {:ok, view, html} = live(conn, ~p"/admin/incidents")

    assert has_element?(view, "#admin-incidents-stale")
    refute has_element?(view, "#admin-incidents-stale", "was .")
    assert has_element?(view, "#admin-incidents-feed-error")
    assert has_element?(view, "#admin-incidents-feed-state[data-state='error']")
    assert has_element?(view, "#admin-incidents-active-desktop", "Stale outage")
    refute html =~ "<rss"
    refute html =~ "<item"
    refute html =~ "network_error"
  end

  test "shows unavailable when no successful fetch exists", %{conn: conn} do
    now = DateTime.utc_now()

    assert {:ok, _state} =
             OpenAIStatus.upsert_feed_state(%{
               last_success_at: nil,
               last_attempt_at: now,
               active_count: 0,
               aggregate_revision: 1,
               updated_at: now
             })

    {:ok, view, html} = live(conn, ~p"/admin/incidents")

    assert has_element?(
             view,
             "#admin-incidents-feed-unavailable",
             "first successful refresh is still pending"
           )

    refute html =~ "last successful fetch was ."
  end

  test "caps visible history and reports overflow", %{conn: conn} do
    now = DateTime.utc_now()

    for index <- 1..51 do
      incident_fixture(
        "history-#{index}",
        "History #{index}",
        "Resolved",
        DateTime.add(now, -index, :second)
      )
    end

    OpenAIStatus.upsert_feed_state(%{
      last_success_at: now,
      active_count: 0,
      aggregate_revision: 1,
      updated_at: now
    })

    {:ok, view, _html} = live(conn, ~p"/admin/incidents")

    assert has_element?(view, "#admin-incidents-history-overflow", "+1 more")

    assert 50 ==
             render(view)
             |> LazyHTML.from_fragment()
             |> LazyHTML.query("#admin-incidents-history-desktop tbody tr")
             |> Enum.count()
  end

  test "refreshes the page projection after a newer status event", %{conn: conn} do
    now = DateTime.utc_now()

    OpenAIStatus.upsert_feed_state(%{
      last_success_at: now,
      active_count: 0,
      aggregate_revision: 1,
      updated_at: now
    })

    {:ok, view, _html} = live(conn, ~p"/admin/incidents")
    refute has_element?(view, "[data-role='openai-incident-row']")

    incident_fixture("event-guid", "Event outage", "Investigating", now)

    assert :ok =
             Events.broadcast(%{
               event_version: 1,
               changed_count: 1,
               active_count: 1,
               aggregate_revision: 2,
               emitted_at: now
             })

    event = %{
      event_version: 1,
      changed_count: 1,
      active_count: 1,
      aggregate_revision: 2,
      emitted_at: now
    }

    assert {:ok, decoded_event} = Events.decode(event)
    send(view.pid, {:openai_status_updated, decoded_event})
    _ = render(view)
    assert has_element?(view, "[data-role='openai-incident-row']", "Event outage")
  end

  defp incident_fixture(guid, title, status, timestamp) do
    {:ok, incident} =
      OpenAIStatus.upsert_incident(
        %{
          guid: guid,
          title: title,
          status: status,
          summary: "safe incident summary",
          component: "Responses API",
          link: "https://status.openai.com/incidents/#{guid}",
          published_at: timestamp,
          content_hash: "hash-#{guid}"
        },
        timestamp
      )

    incident
  end
end
