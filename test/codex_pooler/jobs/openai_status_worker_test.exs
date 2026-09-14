defmodule CodexPooler.Jobs.OpenAIStatusWorkerTest do
  use CodexPooler.DataCase, async: false

  import Ecto.Query

  alias CodexPooler.Jobs.{
    HealthPolicy,
    OpenAIStatusCleanupWorker,
    OpenAIStatusSyncWorker,
    Schedule
  }

  alias CodexPooler.OpenAIStatus
  alias CodexPooler.Repo
  alias CodexPooler.Status.Sync

  test "disabled polling cancels with zero fetches and resumes after the canonical setting changes" do
    alias CodexPooler.InstanceSettings
    settings = InstanceSettings.ensure_singleton!()
    assert settings.operator.openai_status_polling_enabled

    {:ok, disabled} =
      InstanceSettings.update_system_settings(settings, %{
        "operator" => %{"openai_status_polling_enabled" => false}
      })

    assert InstanceSettings.current().operator.openai_status_polling_enabled == false
    caller = self()

    Application.put_env(:codex_pooler, :openai_status_sync_fetcher, fn _, _ ->
      send(caller, :status_fetch_called)
      {:ok, %{items: [], complete?: true}}
    end)

    assert {:cancel, :openai_status_polling_disabled} = perform_job(OpenAIStatusSyncWorker, %{})
    refute_received :status_fetch_called
    assert OpenAIStatus.feed_state() == nil

    assert {:ok, _} =
             InstanceSettings.update_system_settings(disabled, %{
               "operator" => %{"openai_status_polling_enabled" => true}
             })

    assert :ok = perform_job(OpenAIStatusSyncWorker, %{})
    assert_received :status_fetch_called
    assert OpenAIStatus.feed_state().last_success_at
  end

  setup do
    Repo.delete_all(Oban.Job)
    previous = Application.get_env(:codex_pooler, :openai_status_sync_fetcher)

    on_exit(fn ->
      if previous,
        do: Application.put_env(:codex_pooler, :openai_status_sync_fetcher, previous),
        else: Application.delete_env(:codex_pooler, :openai_status_sync_fetcher)
    end)

    :ok
  end

  test "poll treats 200 and 304 as successful and keeps safe string-key args" do
    now = ~U[2026-09-10 10:00:00.000000Z]
    fetcher = fn _state, _opts -> {:ok, %{items: [], content_hash: "feed-1"}} end
    Application.put_env(:codex_pooler, :openai_status_sync_fetcher, fetcher)

    assert :ok = perform_job(OpenAIStatusSyncWorker, %{"now" => DateTime.to_iso8601(now)})

    Application.put_env(:codex_pooler, :openai_status_sync_fetcher, fn _state, _opts ->
      {:not_modified, %{etag: "e2"}}
    end)

    assert :ok = perform_job(OpenAIStatusSyncWorker, %{"now" => DateTime.to_iso8601(now)})
    assert OpenAIStatus.feed_state().last_success_at == now
    refute inspect(OpenAIStatus.feed_state()) =~ "fetcher"
  end

  test "expected upstream failures cancel without deleting state or creating active failures" do
    now = ~U[2026-09-10 10:00:00.000000Z]

    assert {:ok, _} =
             Sync.sync(
               now: now,
               fetcher: fn _state, _opts -> {:ok, %{items: [], content_hash: "feed-1"}} end
             )

    Application.put_env(:codex_pooler, :openai_status_sync_fetcher, fn _state, _opts ->
      {:error, %{code: :upstream_unavailable, message: "temporary"}}
    end)

    assert {:cancel, {:status_feed, "upstream_unavailable"}} =
             OpenAIStatusSyncWorker.perform(%Oban.Job{args: %{}})

    Application.put_env(:codex_pooler, :openai_status_sync_fetcher, fn _state, _opts ->
      {:error, %{code: :unsafe_xml, message: "permanent"}}
    end)

    assert {:cancel, {:status_feed, "unsafe_xml"}} =
             OpenAIStatusSyncWorker.perform(%Oban.Job{args: %{}})

    assert OpenAIStatus.feed_state().active_count == 0
  end

  test "network failures cancel until the next scheduled poll" do
    Application.put_env(:codex_pooler, :openai_status_sync_fetcher, fn _state, _opts ->
      {:error, %{code: :network_error, message: "transport unavailable"}}
    end)

    assert {:cancel, {:status_feed, "network_error"}} =
             OpenAIStatusSyncWorker.perform(%Oban.Job{args: %{}})

    assert {:ok, job} = OpenAIStatusSyncWorker.new(%{}) |> Oban.insert()
    assert %{cancelled: 1, failure: 0} = Oban.drain_queue(queue: :jobs)
    persisted = Repo.get!(Oban.Job, job.id)
    assert persisted.state == "cancelled"
    assert HealthPolicy.classify(persisted) == :cancelled
  end

  test "unexpected programming failures remain visible to Oban" do
    Application.put_env(:codex_pooler, :openai_status_sync_fetcher, fn _, _ ->
      raise ArgumentError, "invalid internal state"
    end)

    assert {:ok, job} = OpenAIStatusSyncWorker.new(%{}) |> Oban.insert()
    assert %{failure: 1} = Oban.drain_queue(queue: :jobs, with_safety: true)
    persisted = Repo.get!(Oban.Job, job.id)
    assert persisted.state == "retryable"
    assert HealthPolicy.classify(persisted) == :retry_pressure
  end

  test "worker returns a bounded cancellation for an invalid normalized item" do
    now = ~U[2026-09-10 10:00:00.000000Z]

    Application.put_env(:codex_pooler, :openai_status_sync_fetcher, fn _state, _opts ->
      {:ok,
       %{
         items: [
           %{
             guid: "invalid-worker-item",
             title: String.duplicate("x", 4_001),
             status: "Investigating",
             summary: "bounded",
             component: "api",
             link: "https://status.openai.com/incidents/invalid-worker-item",
             published_at: now
           }
         ],
         content_hash: "feed-invalid"
       }}
    end)

    assert {:cancel, {:status_feed, "invalid_feed"}} =
             OpenAIStatusSyncWorker.perform(%Oban.Job{args: %{}})

    assert OpenAIStatus.feed_state().last_error_code == "invalid_feed"
    assert OpenAIStatus.list_incidents() == []
  end

  test "poll and cleanup jobs are unique across incomplete states and have bounded policy" do
    assert OpenAIStatusSyncWorker.new(%{}).changes.max_attempts == 3
    assert OpenAIStatusSyncWorker.timeout(%Oban.Job{}) == :timer.seconds(30)
    assert OpenAIStatusCleanupWorker.new(%{}).changes.max_attempts == 3
    assert OpenAIStatusCleanupWorker.timeout(%Oban.Job{}) == :timer.seconds(30)

    for state <- ~w(available scheduled executing retryable) do
      Repo.delete_all(Oban.Job)
      assert {:ok, first} = OpenAIStatusSyncWorker.new(%{}) |> Oban.insert()

      {1, _} =
        Repo.update_all(from(job in Oban.Job, where: job.id == ^first.id), set: [state: state])

      assert {:ok, second} = OpenAIStatusSyncWorker.new(%{}) |> Oban.insert()
      assert second.conflict?
      assert first.id == second.id
    end

    assert {:ok, cleanup_job} =
             OpenAIStatusCleanupWorker.new(%{"retention_days" => 90}) |> Oban.insert()

    assert [enqueued] = all_enqueued(worker: OpenAIStatusCleanupWorker)
    assert enqueued.id == cleanup_job.id
    assert enqueued.queue == "jobs"
    assert enqueued.args == %{"retention_days" => 90}
  end

  test "cleanup worker uses a bounded batch and preserves active and young terminal rows" do
    now = ~U[2026-09-10 10:00:00.000000Z]
    old = DateTime.add(now, -91, :day)
    recent = DateTime.add(now, -1, :day)

    item = fn guid, status, timestamp ->
      %{
        guid: guid,
        title: guid,
        status: status,
        summary: "",
        component: nil,
        link: "https://status.openai.com/incidents/#{guid}",
        published_at: timestamp,
        content_hash: guid
      }
    end

    assert {:ok, _} = OpenAIStatus.upsert_incident(item.("old", "Resolved", old), old)
    assert {:ok, _} = OpenAIStatus.upsert_incident(item.("recent", "Resolved", recent), recent)
    assert {:ok, _} = OpenAIStatus.upsert_incident(item.("active", "Investigating", now), now)

    assert :ok =
             perform_job(OpenAIStatusCleanupWorker, %{
               "now" => DateTime.to_iso8601(now),
               "retention_days" => 90
             })

    refute Enum.any?(OpenAIStatus.list_incidents(), &(&1.guid == "old"))
    assert Enum.any?(OpenAIStatus.list_incidents(), &(&1.guid == "recent"))
    assert Enum.any?(OpenAIStatus.list_incidents(), &(&1.guid == "active"))
  end

  test "schedule exposes both workers and their UTC cadences" do
    assert {"*/5 * * * *", OpenAIStatusSyncWorker} in Schedule.oban_crontab()
    assert {"0 0 * * *", OpenAIStatusCleanupWorker} in Schedule.oban_crontab()

    assert %{
             workers: ["CodexPooler.Jobs.OpenAIStatusSyncWorker"],
             cadence: %{label: "Every 5 min"}
           } =
             Enum.find(Schedule.worker_groups(), &(&1.key == :openai_status_sync))

    assert %{
             workers: ["CodexPooler.Jobs.OpenAIStatusCleanupWorker"],
             cadence: %{label: "Daily at 00:00 UTC"}
           } =
             Enum.find(Schedule.worker_groups(), &(&1.key == :openai_status_cleanup))
  end
end
