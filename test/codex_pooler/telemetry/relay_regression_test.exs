defmodule CodexPooler.Telemetry.RelayRegressionTest do
  use CodexPooler.DataCase, async: false

  alias CodexPooler.Accounting.PreAttemptRelease
  alias CodexPooler.Gateway.Runtime.Finalization.InterruptionOutcome
  alias CodexPooler.Telemetry.{Relay, RelayEvent, RelayRuntime}
  alias CodexPooler.Upstreams.Quota.Windows.Routing
  alias CodexPooler.Upstreams.SavedResets.ConvergenceTelemetry
  alias Ecto.Adapters.SQL.Sandbox
  alias TelemetryMetricsPrometheus.Core

  @quota [:codex_pooler, :quota, :cycle, :decision]
  @stream [:codex_pooler, :gateway, :stream, :outcome]

  test "all four real emitters survive PostgreSQL and Core with equal labels and samples",
       context do
    worker = runtime(context, role: "worker")
    registry = reporter()
    now = DateTime.utc_now()
    alias CodexPooler.Upstreams.Quota.AccountQuotaWindow

    stale = %AccountQuotaWindow{
      quota_scope: "account",
      quota_key: "primary",
      window_kind: "primary",
      window_minutes: 300,
      observed_at: DateTime.add(now, -7200),
      last_sync_at: DateTime.add(now, -7200),
      reset_at: DateTime.add(now, -3600),
      source: "codex_usage_api"
    }

    current = %{
      stale
      | window_kind: "secondary",
        window_minutes: 10_080,
        observed_at: now,
        last_sync_at: now,
        reset_at: DateTime.add(now, 3600)
    }

    for _ <- 1..3 do
      Routing.reject_superseded_primary_windows(
        [stale, current],
        now
      )

      PreAttemptRelease.emit(
        "stale_sweep",
        "http_sse",
        "stale_reservation_recovered"
      )

      InterruptionOutcome.emit("http_sse", "websocket")

      ConvergenceTelemetry.emit(
        %{
          "convergence_source" => "reconciliation",
          "convergence_outcome" => "confirmed_by_quota",
          "consumed_at" => DateTime.to_iso8601(DateTime.add(now, -5)),
          "finished_at" => DateTime.to_iso8601(now),
          "confirmation_timing" => %{
            "version" => 1,
            "canonical_confirmed_at" => DateTime.to_iso8601(DateTime.add(now, -2))
          }
        },
        now
      )
    end

    sync(worker, :flush)

    assert Enum.sort(Enum.map(Repo.all(RelayEvent), & &1.event)) ==
             ~w(pre_attempt_release quota_cycle_decision saved_reset_convergence stream_outcome)

    stop_runtime(worker)
    web = runtime(context, role: "web")
    sync(web, :drain)
    lines = Core.scrape(registry) |> String.split("\n")

    direct =
      lines
      |> Enum.filter(&String.contains?(&1, "via=\"in_process\""))
      |> Enum.map(&String.replace(&1, "via=\"in_process\"", "via=\"normalized\""))
      |> Enum.sort()

    relayed =
      lines
      |> Enum.filter(&String.contains?(&1, "via=\"job_relay\""))
      |> Enum.map(&String.replace(&1, "via=\"job_relay\"", "via=\"normalized\""))
      |> Enum.sort()

    assert length(direct) > 4
    assert direct == relayed
  end

  test "worker and scheduler cannot consume rows before a web reporter observes them", context do
    for role <- ["worker", "scheduler"] do
      runtime = runtime(context, role: role)
      :telemetry.execute(@quota, %{count: 1}, quota_labels())
      sync(runtime, :flush)
      sync(runtime, :drain)
      assert Enum.all?(Repo.all(RelayEvent), &is_nil(&1.claimed_at))
      stop_runtime(runtime)
    end

    registry = reporter()
    web = runtime(context, role: "web")
    sync(web, :drain)
    assert sample(registry, "codex_pooler_quota_cycle_decision_count", via: "job_relay") == 2
    assert Enum.all?(Repo.all(RelayEvent), &(&1.claimed_by == :sys.get_state(web).owner))
  end

  test "web and all count directly without persisting a duplicate", context do
    registry = reporter()

    for role <- ["web", "all"] do
      runtime = runtime(context, role: role)
      :telemetry.execute(@quota, %{count: 1}, quota_labels())
      sync(runtime, :flush)
      sync(runtime, :drain)
      assert Repo.aggregate(RelayEvent, :count) == 0
      stop_runtime(runtime)
    end

    assert sample(registry, "codex_pooler_quota_cycle_decision_count", via: "in_process") == 2
    refute Core.scrape(registry) =~ "via=\"job_relay\""
  end

  test "JSONB roundtrip preserves final reporter labels for every relayed metric", context do
    worker = runtime(context, role: "worker")
    :telemetry.execute(@quota, %{count: 1}, quota_labels())

    :telemetry.execute(@stream, %{count: 1}, %{
      outcome: "interrupted",
      downstream_transport: "websocket",
      upstream_transport: "http_sse"
    })

    :telemetry.execute(
      [:codex_pooler, :accounting, :reservation, :pre_attempt_release],
      %{count: 1},
      %{
        phase: "stale_sweep",
        transport: "http_json"
      }
    )

    :telemetry.execute(
      [:codex_pooler, :saved_reset, :convergence],
      %{
        count: 1,
        applied_to_canonical_ms: 5_000,
        canonical_to_lifecycle_ms: 10_000,
        applied_to_lifecycle_ms: 15_000
      },
      %{source: "runtime_headers", outcome: "confirmed_by_quota"}
    )

    sync(worker, :flush)

    assert Enum.all?(
             Repo.all(RelayEvent),
             &Enum.all?(Map.keys(&1.labels), fn key -> is_binary(key) end)
           )

    stop_runtime(worker)
    registry = reporter()
    web = runtime(context, role: "web")
    sync(web, :drain)

    assert sample(registry, "codex_pooler_quota_cycle_decision_count",
             scope: "account",
             decision: "candidate",
             source: "runtime",
             via: "job_relay"
           ) == 1

    assert sample(registry, "codex_pooler_gateway_stream_outcome_count",
             outcome: "interrupted",
             downstream_transport: "websocket",
             upstream_transport: "http_sse",
             via: "job_relay"
           ) == 1

    assert sample(registry, "codex_pooler_accounting_reservation_pre_attempt_release_count",
             phase: "stale_sweep",
             transport: "http_json",
             via: "job_relay"
           ) == 1

    assert sample(
             registry,
             "codex_pooler_saved_reset_convergence_applied_to_canonical_seconds_sum",
             source: "runtime_headers",
             outcome: "confirmed_by_quota",
             via: "job_relay"
           ) == 5

    assert sample(
             registry,
             "codex_pooler_saved_reset_convergence_canonical_to_lifecycle_seconds_sum",
             source: "runtime_headers",
             outcome: "confirmed_by_quota",
             via: "job_relay"
           ) == 10

    assert sample(
             registry,
             "codex_pooler_saved_reset_convergence_applied_to_lifecycle_seconds_sum",
             source: "runtime_headers",
             outcome: "confirmed_by_quota",
             via: "job_relay"
           ) == 15
  end

  test "aggregation preserves event multiplicity and individual latency samples", context do
    worker = runtime(context, role: "worker")

    for value <- [5_000, 15_000] do
      :telemetry.execute(@quota, %{count: 1}, quota_labels())

      :telemetry.execute(
        [:codex_pooler, :saved_reset, :convergence],
        %{count: 1, applied_to_canonical_ms: value},
        %{source: "runtime_headers", outcome: "confirmed_by_quota"}
      )
    end

    sync(worker, :flush)
    stop_runtime(worker)
    registry = reporter()
    web = runtime(context, role: "web")
    sync(web, :drain)
    assert sample(registry, "codex_pooler_quota_cycle_decision_count", via: "job_relay") == 2
    assert sample(registry, "codex_pooler_saved_reset_convergence_count", via: "job_relay") == 2

    assert sample(
             registry,
             "codex_pooler_saved_reset_convergence_applied_to_canonical_seconds_count",
             via: "job_relay"
           ) == 2

    assert sample(
             registry,
             "codex_pooler_saved_reset_convergence_applied_to_canonical_seconds_sum",
             via: "job_relay"
           ) == 20

    assert sample(
             registry,
             "codex_pooler_saved_reset_convergence_applied_to_canonical_seconds_bucket",
             via: "job_relay",
             le: "5"
           ) == 1

    assert sample(
             registry,
             "codex_pooler_saved_reset_convergence_applied_to_canonical_seconds_bucket",
             via: "job_relay",
             le: "15"
           ) == 2
  end

  test "aggregate count weight never multiplies histogram observations", context do
    worker = runtime(context, role: "worker")
    registry = reporter()

    for _ <- 1..2 do
      :telemetry.execute(
        [:codex_pooler, :saved_reset, :convergence],
        %{count: 3, applied_to_canonical_ms: 5000},
        %{source: "runtime_headers", outcome: "confirmed_by_quota"}
      )
    end

    sync(worker, :flush)
    stop_runtime(worker)
    web = runtime(context, role: "web")
    sync(web, :drain)

    for via <- ["in_process", "job_relay"] do
      assert sample(registry, "codex_pooler_saved_reset_convergence_count", via: via) == 6

      assert sample(
               registry,
               "codex_pooler_saved_reset_convergence_applied_to_canonical_seconds_count",
               via: via
             ) == 2

      assert sample(
               registry,
               "codex_pooler_saved_reset_convergence_applied_to_canonical_seconds_sum",
               via: via
             ) ==
               10
    end
  end

  test "insert exception restores the taken snapshot and automatically retries", context do
    parent = self()

    insert = fn event, labels, count, values, owner ->
      attempt = Process.get(:insert_attempt, 0) + 1
      Process.put(:insert_attempt, attempt)
      send(parent, {:insert_attempt, attempt})
      if attempt == 1, do: raise("synthetic insert failure")
      result = Relay.insert(event, labels, count, values, owner)
      send(parent, {:insert_result, result})
      result
    end

    worker = runtime(context, role: "worker", insert_fun: insert, flush_ms: 100, activate: false)
    Relay.refresh_heartbeat(:sys.get_state(worker).owner)
    for _ <- 1..3, do: :telemetry.execute(@quota, %{count: 1}, quota_labels())
    sync(worker, :flush)
    assert_receive {:insert_attempt, 1}
    assert [{_, 3}] = :ets.tab2list(:sys.get_state(worker).table)
    assert_receive {:insert_attempt, 2}, 2_000
    assert_receive {:insert_result, {:ok, %RelayEvent{count: 3}}}, 2_000
    assert :ets.tab2list(:sys.get_state(worker).table) == []
    assert [%RelayEvent{count: 3}] = Repo.all(RelayEvent)
  end

  test "failed flush merges concurrent captures without consuming extra buffer capacity",
       context do
    parent = self()

    insert = fn event, labels, count, values, owner ->
      unless Process.get(:failed_once, false) do
        Process.put(:failed_once, true)
        send(parent, {:snapshot_taken, self()})

        receive do
          :fail_insert -> raise "synthetic insert failure"
        end
      end

      Relay.insert(event, labels, count, values, owner)
    end

    worker = runtime(context, role: "worker", insert_fun: insert, max_series: 2)
    :telemetry.execute(@quota, %{count: 1}, quota_labels())
    send(worker, :flush)
    assert_receive {:snapshot_taken, ^worker}
    :telemetry.execute(@quota, %{count: 1}, quota_labels())
    send(worker, :fail_insert)
    :sys.get_state(worker)
    assert [{_, 2}] = :ets.tab2list(:sys.get_state(worker).table)
    sync(worker, :flush)
    assert [%RelayEvent{count: 2}] = Repo.all(RelayEvent)
    :telemetry.execute(@quota, %{count: 1}, quota_labels())
    :telemetry.execute(@stream, %{count: 1}, %{outcome: "interrupted"})
    assert :ets.info(:sys.get_state(worker).table, :size) == 2
    sync(worker, :flush)
    assert Repo.aggregate(RelayEvent, :count) == 3
  end

  test "claim exception retries automatically and reaches the real reporter", context do
    Relay.refresh_heartbeat("fixture")
    assert {:ok, _} = Relay.insert("quota_cycle_decision", quota_labels(), 1, %{}, "fixture")
    parent = self()

    claim = fn limit, owner ->
      attempt = Process.get(:claim_attempt, 0) + 1
      Process.put(:claim_attempt, attempt)
      send(parent, {:claim_attempt, attempt})
      if attempt == 1, do: raise("synthetic claim failure")
      Relay.claim(limit, owner)
    end

    registry = reporter()
    web = runtime(context, role: "web", claim_fun: claim, drain_ms: 100, activate: false)
    sync(web, :drain)
    assert_receive {:claim_attempt, 1}
    assert_receive {:claim_attempt, 2}, 2_000
    :sys.get_state(web)
    assert sample(registry, "codex_pooler_quota_cycle_decision_count", via: "job_relay") == 1
  end

  test "each callback emits at most 100 samples and yields with the remainder", context do
    worker = runtime(context, role: "worker")
    for _ <- 1..205, do: :telemetry.execute(@quota, %{count: 1}, quota_labels())
    sync(worker, :flush)
    stop_runtime(worker)
    registry = reporter()
    web = runtime(context, role: "web", activate: false)
    state = :sys.get_state(web)
    {:noreply, state} = RelayRuntime.handle_info(:drain, state)
    assert sample(registry, "codex_pooler_quota_cycle_decision_count", via: "job_relay") == 100
    assert [%RelayEvent{count: 105}] = state.pending
    assert state.drain_again?
    {:noreply, state} = RelayRuntime.handle_info(:drain, state)
    assert sample(registry, "codex_pooler_quota_cycle_decision_count", via: "job_relay") == 200
    {:noreply, state} = RelayRuntime.handle_info(:drain, state)
    assert sample(registry, "codex_pooler_quota_cycle_decision_count", via: "job_relay") == 205
    assert state.pending == []
    refute state.drain_again?
    RelayRuntime.handle_info(:drain, state)
    assert sample(registry, "codex_pooler_quota_cycle_decision_count", via: "job_relay") == 205
  end

  test "backlog continues automatically without waiting for the idle drain interval", context do
    worker = runtime(context, role: "worker")
    # Both a weighted row and a full claim batch must yield then continue immediately.
    for _ <- 1..205, do: :telemetry.execute(@quota, %{count: 1}, quota_labels())
    sync(worker, :flush)
    writer = :sys.get_state(worker).owner
    stop_runtime(worker)
    for _ <- 1..100, do: Relay.insert("quota_cycle_decision", quota_labels(), 1, %{}, writer)
    registry = reporter()
    web = runtime(context, role: "web", activate: false, drain_ms: 60_000)
    ref = make_ref()
    parent = self()

    :telemetry.attach(
      ref,
      @quota,
      fn _event, _measurements, metadata, pid ->
        if metadata[:via] == "job_relay" do
          count = Process.get(ref, 0) + 1
          Process.put(ref, count)
          if count == 305, do: send(pid, :backlog_drained)
        end
      end,
      parent
    )

    on_exit(fn -> :telemetry.detach(ref) end)
    send(web, :drain)
    assert_receive :backlog_drained, 2_000
    assert sample(registry, "codex_pooler_quota_cycle_decision_count", via: "job_relay") == 305
    assert :sys.get_state(web).pending == []
  end

  test "buffer overflow is bounded under concurrent capture and recovers after flush", context do
    worker = runtime(context, role: "worker", max_series: 2)

    tasks =
      for value <- 1..20 do
        Task.async(fn ->
          :telemetry.execute(
            [:codex_pooler, :saved_reset, :convergence],
            %{count: 1, applied_to_canonical_ms: value},
            %{source: "runtime_headers", outcome: "confirmed_by_quota"}
          )
        end)
      end

    Enum.each(tasks, &Task.await/1)
    assert :ets.info(:sys.get_state(worker).table, :size) == 2
    log = ExUnit.CaptureLog.capture_log(fn -> sync(worker, :flush) end)
    assert log =~ "telemetry relay buffer full dropped_events=18"
    assert Repo.aggregate(RelayEvent, :count) == 2

    assert %{rows: [[18]]} =
             Repo.query!("SELECT samples FROM telemetry_relay_losses WHERE reason='buffer_overflow'")

    refute ExUnit.CaptureLog.capture_log(fn -> sync(worker, :flush) end) =~ "buffer full"

    assert %{rows: [[18]]} =
             Repo.query!("SELECT samples FROM telemetry_relay_losses WHERE reason='buffer_overflow'")

    :telemetry.execute(@quota, %{count: 1}, quota_labels())
    sync(worker, :flush)
    assert Repo.aggregate(RelayEvent, :count) == 3
    assert :ets.tab2list(:sys.get_state(worker).table) == []
  end

  test "heartbeat failure keeps the runtime alive and retries without a restart", context do
    parent = self()

    heartbeat = fn owner ->
      attempt = Process.get(:heartbeat_attempt, 0) + 1
      Process.put(:heartbeat_attempt, attempt)
      send(parent, {:heartbeat_attempt, attempt})

      case attempt do
        1 -> {:error, :unavailable}
        2 -> raise "synthetic heartbeat failure"
        _ -> Relay.refresh_heartbeat(owner)
      end
    end

    worker = runtime(context, role: "worker", heartbeat_fun: heartbeat, heartbeat_ms: 100)
    assert_receive {:heartbeat_attempt, 1}
    assert_receive {:heartbeat_attempt, 2}, 2_000
    assert_receive {:heartbeat_attempt, 3}, 2_000
    assert Process.alive?(worker)
    assert Relay.heartbeat_fresh?(:sys.get_state(worker).owner)
  end

  defp runtime(context, opts) do
    id = make_ref()

    pid =
      start_supervised!(%{
        id: id,
        start:
          {RelayRuntime, :start_link,
           [
             Keyword.merge(
               [enabled: true, start_paused: true, name: nil, flush_ms: 60_000, drain_ms: 60_000],
               opts
             )
           ]}
      })

    Sandbox.allow(Repo, context.sandbox_owner, pid)
    # Store the supervisor child identity outside the runtime under test.
    Process.put({:runtime_child, pid}, id)
    if Keyword.get(opts, :activate, true), do: GenServer.call(pid, :activate)
    pid
  end

  defp stop_runtime(pid) when is_pid(pid), do: super_stop(Process.get({:runtime_child, pid}))
  defp super_stop(id), do: ExUnit.Callbacks.stop_supervised!(id)

  defp reporter do
    registry = Module.concat(__MODULE__, "Registry#{System.unique_integer([:positive])}")

    start_supervised!({Core, metrics: CodexPoolerWeb.Telemetry.prometheus_metrics(), name: registry, start_async: false})

    registry
  end

  defp sync(pid, message) do
    send(pid, message)
    :sys.get_state(pid)
  end

  defp quota_labels,
    do: %{scope: "account", decision: "candidate", source: "runtime", via: "in_process"}

  defp sample(registry, name, labels) do
    lines = Core.scrape(registry) |> String.split("\n")

    matches =
      Enum.filter(lines, fn line ->
        String.starts_with?(line, name <> "{") and
          Enum.all?(labels, fn {key, value} -> String.contains?(line, "#{key}=\"#{value}\"") end)
      end)

    assert [line] = matches
    {value, ""} = line |> String.split(" ") |> List.last() |> Float.parse()
    value
  end

  test "every relayed metric family declares only labels the relay forwards" do
    relayed = MapSet.new(RelayRuntime.relayed_events())

    families =
      CodexPoolerWeb.Telemetry.prometheus_metrics()
      |> Enum.filter(&MapSet.member?(relayed, &1.event_name))

    assert MapSet.new(families, & &1.event_name) == relayed

    for metric <- families do
      # Without :via the in_process and job_relay shares would merge into one series.
      assert :via in metric.tags, "#{inspect(metric.event_name)} must declare :via"
      unknown = Enum.reject(metric.tags, &(&1 in RelayRuntime.label_keys()))

      assert unknown == [],
             "#{inspect(metric.event_name)} declares tags #{inspect(unknown)} that the relay drops"
    end
  end

  # Valid values, one per forwarded key, with the per-family vocabularies where
  # two families share a key name. A family whose `tag_values/1` derived a
  # declared tag from a key the relay does not forward would render that tag
  # the same for a forwarded-only metadata map and for an empty one.
  @forwarded_samples %{
    scope: "account",
    decision: "anchored_confirmed",
    source: "provider_usage",
    outcome: "interrupted",
    phase: "turn_interrupted",
    transport: "websocket",
    downstream_transport: "http_sse",
    upstream_transport: "websocket",
    via: "job_relay"
  }
  @family_samples %{
    [:codex_pooler, :saved_reset, :convergence] => %{
      source: "reconciliation",
      outcome: "confirmed_by_quota"
    }
  }

  test "every relayed metric family derives each declared tag from a forwarded key" do
    relayed = MapSet.new(RelayRuntime.relayed_events())
    assert Map.keys(@forwarded_samples) |> Enum.sort() == Enum.sort(RelayRuntime.label_keys())

    for metric <- CodexPoolerWeb.Telemetry.prometheus_metrics(),
        MapSet.member?(relayed, metric.event_name) do
      samples = Map.merge(@forwarded_samples, Map.get(@family_samples, metric.event_name, %{}))
      forwarded = metric.tag_values.(samples)
      absent = metric.tag_values.(%{})

      for tag <- metric.tags do
        # Every declared tag is itself a forwarded key, so its rendered value
        # must be exactly the sample under that key: a different value means
        # the sample is out of this family's vocabulary (fix the table) or the
        # tag reads another key, forwarded or not (fix `tag_values/1`).
        assert Map.get(forwarded, tag) == Map.fetch!(samples, tag),
               "#{inspect(metric.event_name)} tag #{tag} rendered #{inspect(Map.get(forwarded, tag))} for sample #{inspect(Map.fetch!(samples, tag))}: out-of-vocabulary sample or the tag reads a different key"

        assert Map.get(forwarded, tag) != Map.get(absent, tag),
               "#{inspect(metric.event_name)} tag #{tag} renders the same with and without its forwarded key"
      end
    end
  end
end
