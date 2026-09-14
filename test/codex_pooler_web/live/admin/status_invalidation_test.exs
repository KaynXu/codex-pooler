defmodule CodexPoolerWeb.Admin.StatusInvalidationTest do
  use CodexPoolerWeb.ConnCase, async: false

  import Phoenix.LiveViewTest
  import Ecto.Query
  import CodexPooler.UnboxedFixture, only: [register_unboxed_cleanup!: 1]

  alias CodexPooler.InstanceSettings
  alias CodexPooler.OpenAIStatus
  alias CodexPooler.Repo
  alias CodexPooler.Status.Schemas.{FeedState, Incident}
  alias CodexPooler.Status.Sync
  alias Ecto.Adapters.SQL.Sandbox

  setup :register_and_log_in_user

  test "unchanged 200 recovers stale mounted page and banner once", %{conn: conn} do
    assert_stale_recovery(conn, :ok)
  end

  test "unchanged 304 recovers stale mounted page and banner once", %{conn: conn} do
    assert_stale_recovery(conn, :not_modified)
  end

  test "successive unchanged 200 polls keep mounted freshness past fifteen minutes and a day", %{
    conn: conn
  } do
    assert_continuous_freshness(conn, :ok)
  end

  test "successive unchanged 304 polls keep mounted freshness past fifteen minutes and a day", %{
    conn: conn
  } do
    assert_continuous_freshness(conn, :not_modified)
  end

  defp assert_continuous_freshness(conn, tag) do
    now = DateTime.utc_now() |> DateTime.truncate(:microsecond)
    initial = DateTime.add(now, -90_000, :second)
    guid = "continuous-freshness-#{System.unique_integer([:positive])}"

    register_unboxed_cleanup!(fn ->
      Repo.delete_all(from(i in Incident, where: i.guid == ^guid))
      Repo.delete_all(FeedState)
    end)

    assert {:ok, %{aggregate_revision: 1}} =
             Sandbox.unboxed_run(Repo, fn ->
               Sync.sync(fetcher: fetcher(guid, initial), now: initial)
             end)

    {:ok, page, _} = live(conn, ~p"/admin/incidents")
    {:ok, banner, _} = live(conn, ~p"/admin/pools")
    before_incidents = OpenAIStatus.list_incidents()
    before_page = :sys.get_state(page.pid).socket.assigns.incidents_page
    before_banner = aggregate(banner)
    parent = self()
    handler_id = "mounted-freshness-#{System.unique_integer([:positive])}"
    on_exit(fn -> :telemetry.detach(handler_id) end)

    :ok =
      :telemetry.attach(
        handler_id,
        [:codex_pooler, :repo, :query],
        fn _, _, metadata, _ ->
          if self() in [page.pid, banner.pid] and
               metadata.source in [
                 "openai_status_incidents",
                 "openai_status_feed_states",
                 "openai_status_dismissals"
               ],
             do: send(parent, :mounted_status_query)
        end,
        nil
      )

    poll = if tag == :ok, do: fetcher(guid, initial), else: fn _, _ -> {:not_modified, %{}} end

    for step <- 1..300 do
      at = DateTime.add(initial, step * 300, :second)

      assert {^tag, %{aggregate_revision: 1, changed_count: 0}} =
               Sandbox.unboxed_run(Repo, fn -> Sync.sync(fetcher: poll, now: at) end)
    end

    assert_views(fn ->
      aggregate(page).last_success_at == now and aggregate(banner).last_success_at == now
    end)

    page_state = :sys.get_state(page.pid).socket.assigns.incidents_page
    assert page_state.last_success_at == now
    assert page_state.stale? == false
    assert page_state.available? == true

    assert Map.take(page_state, [:active, :history, :history_total, :history_overflow]) ==
             Map.take(before_page, [:active, :history, :history_total, :history_overflow])

    assert aggregate(banner).incidents == before_banner.incidents
    assert OpenAIStatus.list_incidents() == before_incidents
    assert OpenAIStatus.feed_state().aggregate_revision == 1
    refute_received :mounted_status_query
    send(page.pid, :openai_incidents_status_refresh)
    assert_receive :mounted_status_query, 15_000
    _ = :sys.get_state(page.pid)
    :telemetry.detach(handler_id)
    render_click(banner, "set_live_updates", %{"paused" => true})
    assert has_element?(banner, "#admin-openai-status-banner")
    refute has_element?(banner, "#admin-openai-status-stale")
    assert has_element?(page, "#admin-incidents-feed-state[data-state='current']")
  end

  test "a settings save in another tab refreshes page and banner without incident revisions", %{
    conn: conn
  } do
    now = DateTime.utc_now()
    assert {:ok, _} = Sync.sync(fetcher: fetcher("settings-tabs", now), now: now)
    {:ok, page, _} = live(conn, ~p"/admin/incidents")
    {:ok, banner, _} = live(conn, ~p"/admin/pools")
    {:ok, settings, _} = live(conn, ~p"/admin/system?#{%{"tab" => "gateway"}}")
    revision = OpenAIStatus.feed_state().aggregate_revision

    for {value, state} <- [{"false", "disabled"}, {"true", "current"}] do
      settings
      |> element("#instance-settings-operator-form")
      |> render_submit(%{
        "instance_settings" => %{"operator" => %{"openai_status_polling_enabled" => value}}
      })

      expected = value == "true"
      assert InstanceSettings.current().operator.openai_status_polling_enabled == expected

      assert_views(fn ->
        has_element?(page, "#admin-incidents-feed-state[data-state='#{state}']") and
          aggregate(banner).polling_enabled? == expected
      end)

      assert OpenAIStatus.feed_state().aggregate_revision == revision
    end

    assert {:ok, %{aggregate_revision: ^revision}} =
             Sync.sync(fetcher: fetcher("settings-tabs", now), now: DateTime.add(now, 1, :second))

    assert has_element?(page, "#admin-incidents-feed-state[data-state='current']")
    refute has_element?(banner, "#admin-openai-status-stale")
  end

  defp assert_stale_recovery(conn, tag) do
    now = DateTime.utc_now() |> DateTime.truncate(:microsecond)
    old = DateTime.add(now, -901, :second)
    guid = "stale-recovery-#{System.unique_integer([:positive])}"

    register_unboxed_cleanup!(fn ->
      Repo.delete_all(from(i in Incident, where: i.guid == ^guid))
      Repo.delete_all(FeedState)
    end)

    assert {:ok, _} =
             Sandbox.unboxed_run(Repo, fn -> Sync.sync(fetcher: fetcher(guid, old), now: old) end)

    {:ok, page, _} = live(conn, ~p"/admin/incidents")
    {:ok, banner, _} = live(conn, ~p"/admin/pools")
    assert has_element?(page, "#admin-incidents-feed-state[data-state='stale']")
    assert has_element?(banner, "#admin-openai-status-stale")
    before_incidents = OpenAIStatus.list_incidents()
    poll = if tag == :ok, do: fetcher(guid, old), else: fn _, _ -> {:not_modified, %{}} end

    assert {^tag, %{aggregate_revision: 2, changed_count: 0}} =
             Sandbox.unboxed_run(Repo, fn -> Sync.sync(fetcher: poll, now: now) end)

    assert_views(fn ->
      has_element?(page, "#admin-incidents-feed-state[data-state='current']") and
        not has_element?(banner, "#admin-openai-status-stale")
    end)

    assert OpenAIStatus.list_incidents() == before_incidents
    assert aggregate(page).last_success_at == now
    assert aggregate(banner).last_success_at == now

    later = DateTime.add(now, 1, :second)

    assert {^tag, %{aggregate_revision: 2, changed_count: 0}} =
             Sandbox.unboxed_run(Repo, fn -> Sync.sync(fetcher: poll, now: later) end)

    assert OpenAIStatus.feed_state().last_success_at == later

    assert_views(fn ->
      aggregate(page).last_success_at == later and aggregate(banner).last_success_at == later
    end)
  end

  defp fetcher(guid, published_at) do
    fn _, _ ->
      {:ok,
       %{
         items: [
           %{
             guid: guid,
             title: "Service recovering",
             status: "Monitoring",
             summary: "Recovery in progress",
             component: nil,
             link: "https://status.openai.com/incidents/#{guid}",
             published_at: published_at
           }
         ],
         complete?: true
       }}
    end
  end

  defp aggregate(view), do: :sys.get_state(view.pid).socket.assigns.openai_status_aggregate

  defp assert_views(predicate),
    do: await_views(predicate, System.monotonic_time(:millisecond) + 15_000)

  defp await_views(predicate, deadline) do
    if predicate.() do
      :ok
    else
      assert System.monotonic_time(:millisecond) < deadline,
             "mounted status surfaces did not receive canonical invalidation"

      receive do
      after
        10 -> await_views(predicate, deadline)
      end
    end
  end
end
