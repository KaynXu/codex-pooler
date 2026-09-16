defmodule CodexPooler.Telemetry.RelayContractTest do
  # The relay is the only transport that carries an Oban job's telemetry to a
  # scraped reporter, so what it is allowed to carry, what the database refuses
  # to store, and what happens when nothing drains it are contract, not
  # implementation detail. `relay_storage_test.exs` covers the happy paths of
  # each function; this file pins the four properties the design comment on
  # findings#195 promised an operator and a reader of the schema:
  #
  #   * the row shape is metadata-only and bounded by the database, not only by
  #     a changeset a future caller could bypass;
  #   * an unknown event name is refused by the database itself;
  #   * two real PostgreSQL backends claiming at the same time split the work
  #     instead of double counting it;
  #   * every batch statement runs under a transaction-local timeout that does
  #     not outlive its transaction.
  #
  # It also states, as tests rather than prose, two things the ticket's own
  # comments corrected: the heartbeat that gates inserts is the *producer's*
  # own, so an absent consumer never stops a producer; and a relay outage is
  # invisible to the business transaction that emitted the event.
  use CodexPooler.DataCase, async: false

  import CodexPooler.PoolerFixtures
  import CodexPooler.UnboxedFixture

  alias CodexPooler.Telemetry.{Relay, RelayEvent, RelayRuntime}
  alias Ecto.Adapters.SQL.Sandbox

  # Failure-detection budgets, not behaviour timers.
  @task_budget 15_000

  setup do
    :ok = Relay.refresh_heartbeat("relay-runtime")
  end

  describe "the row a relay event is allowed to be" do
    test "the table carries no identifier column and no free-text column" do
      columns =
        Repo.query!(
          "SELECT column_name, data_type FROM information_schema.columns WHERE table_name = 'telemetry_relay_events' ORDER BY column_name"
        ).rows
        |> Map.new(fn [name, type] -> {name, type} end)

      # Every column is named here on purpose. A new one arrives in this
      # assertion before it can arrive in production, which is the point: the
      # relay must never grow a request, session, pool, account, identity or
      # key id, and `claimed_by` is the only string an app node writes.
      assert Map.keys(columns) |> Enum.sort() == [
               "claimed_at",
               "claimed_by",
               "count",
               "event",
               "id",
               "inserted_at",
               "labels",
               "measurements"
             ]

      assert columns["labels"] == "jsonb"
      assert columns["measurements"] == "jsonb"
      assert columns["count"] == "bigint"
    end

    test "the forwarded label vocabulary is a closed set of bounded category keys" do
      # `RelayRuntime.labels/1` takes only these keys off the emitted metadata,
      # so a metric that starts tagging by an identifier cannot reach the table
      # through the relay. Listing them here makes adding one a deliberate act.
      assert Enum.sort(RelayRuntime.label_keys()) == [
               :decision,
               :downstream_transport,
               :outcome,
               :phase,
               :scope,
               :source,
               :transport,
               :upstream_transport,
               :via
             ]

      forbidden = ~w(id request_id session_id pool_id account_id api_key_id upstream_identity_id)a

      assert RelayRuntime.label_keys() |> Enum.filter(&(&1 in forbidden)) == []
    end

    test "an over-wide label or measurement map is refused by the database, not only the changeset" do
      # The changeset bound is proven in relay_storage_test. This one bypasses
      # it entirely, because the check constraint is what still holds when a
      # future caller writes the row some other way.
      assert {:error, %Postgrex.Error{postgres: %{code: :check_violation}}} =
               raw_insert(%{labels: Map.new(1..17, &{"k#{&1}", "v"})})

      assert {:error, %Postgrex.Error{postgres: %{code: :check_violation}}} =
               raw_insert(%{measurements: Map.new(1..9, &{"m#{&1}", 1})})

      assert {:ok, _} = raw_insert(%{labels: Map.new(1..16, &{"k#{&1}", "v"})})
    end

    test "the database refuses an unknown event name and a negative count" do
      assert {:error, %Postgrex.Error{postgres: %{code: :check_violation, constraint: event}}} =
               raw_insert(%{event: "not_an_allowlisted_event"})

      assert event == "event_allowed"

      assert {:error, %Postgrex.Error{postgres: %{code: :check_violation, constraint: count}}} =
               raw_insert(%{count: -1})

      assert count == "count_non_negative"

      # The allowlist the database enforces is the one the schema declares, so
      # adding an event to one without the other cannot pass unnoticed.
      for event <- RelayEvent.events() do
        assert {:ok, _} = raw_insert(%{event: event}),
               "#{event} is on the schema allowlist but the database check refuses it"
      end
    end
  end

  describe "claiming across nodes" do
    test "two real backends claim disjointly and together claim everything" do
      # The sandboxed claim test runs both claimers over one connection, where
      # FOR UPDATE SKIP LOCKED can never be exercised. This one uses two real
      # PostgreSQL backends, asserts they are different backends, and asserts
      # the union is complete as well as disjoint: a lock that silently skipped
      # everything would be disjoint too.
      ids = for _ <- 1..8, do: Ecto.UUID.generate()

      register_unboxed_cleanup!(fn ->
        Repo.delete_all(from e in RelayEvent, where: e.id in ^ids)
      end)

      run_unboxed(fn ->
        for id <- ids do
          Repo.insert!(%RelayEvent{
            id: id,
            event: "stale_sweep",
            labels: %{},
            count: 1,
            inserted_at: DateTime.utc_now()
          })
        end
      end)

      parent = self()
      barrier = make_ref()

      claimers =
        for owner <- ["node-a", "node-b"] do
          Task.async(fn ->
            run_unboxed(fn ->
              %{rows: [[backend]]} = Repo.query!("SELECT pg_backend_pid()")
              send(parent, {:ready, barrier, self()})

              receive do
                {:go, ^barrier} -> :ok
              after
                @task_budget -> raise "claim barrier was never released"
              end

              # Each claimer may take at most half, so a complete union is only
              # possible if both of them claimed.
              {backend, Relay.claim(4, owner)}
            end)
          end)
        end

      for _ <- claimers do
        assert_receive {:ready, ^barrier, pid}, @task_budget
        send(pid, {:go, barrier})
      end

      results = Enum.map(claimers, &Task.await(&1, @task_budget))
      backends = Enum.map(results, &elem(&1, 0))

      assert length(Enum.uniq(backends)) == 2,
             "both claimers ran on one backend, so SKIP LOCKED was never exercised"

      claimed =
        Enum.flat_map(results, fn {_backend, {:ok, rows}} -> Enum.map(rows, & &1.id) end)

      assert Enum.sort(claimed) == Enum.sort(ids)
      assert length(Enum.uniq(claimed)) == length(claimed)

      run_unboxed(fn ->
        assert {:ok, []} = Relay.claim(8, "node-c")

        owners =
          Repo.all(from e in RelayEvent, where: e.id in ^ids, select: e.claimed_by)
          |> Enum.uniq()
          |> Enum.sort()

        assert owners == ["node-a", "node-b"]
      end)
    end

    test "a row another backend holds is skipped rather than waited on" do
      # Disjointness alone does not distinguish SKIP LOCKED from a plain
      # FOR UPDATE: a second claimer that blocks and then re-reads still ends up
      # disjoint, because the rows it waited for are no longer unclaimed. What
      # SKIP LOCKED buys is that a drain on one app pod is never held up by a
      # row another pod is sitting on, so this asserts the claim completes while
      # the lock is still held, and skips exactly the held row.
      ids = for _ <- 1..4, do: Ecto.UUID.generate()
      [held | rest] = ids

      register_unboxed_cleanup!(fn ->
        Repo.delete_all(from e in RelayEvent, where: e.id in ^ids)
      end)

      run_unboxed(fn ->
        for id <- ids do
          Repo.insert!(%RelayEvent{
            id: id,
            event: "stream_outcome",
            labels: %{},
            count: 1,
            inserted_at: DateTime.utc_now()
          })
        end
      end)

      parent = self()
      release = make_ref()

      holder =
        Task.async(fn ->
          run_unboxed(fn ->
            Repo.transaction(fn ->
              Repo.one!(from e in RelayEvent, where: e.id == ^held, lock: "FOR UPDATE")
              send(parent, {:held, self()})

              receive do
                {:release, ^release} -> :ok
              after
                @task_budget -> :timeout
              end
            end)
          end)
        end)

      assert_receive {:held, holder_pid}, @task_budget
      on_exit(fn -> send(holder_pid, {:release, release}) end)

      claimed =
        Task.async(fn ->
          run_unboxed(fn -> Relay.claim(10, "skipping-node") end)
        end)
        |> Task.await(@task_budget)

      assert {:ok, rows} = claimed
      assert Enum.sort(Enum.map(rows, & &1.id)) == Enum.sort(rest)

      send(holder_pid, {:release, release})
      assert {:ok, _} = Task.await(holder, @task_budget)

      run_unboxed(fn ->
        assert {:ok, [%RelayEvent{id: ^held}]} = Relay.claim(10, "later-node")
      end)
    end
  end

  describe "statement bounds" do
    test "each batch statement runs under a transaction-local timeout that does not outlive it" do
      # `SET LOCAL` is the whole claim: a session-level timeout would survive
      # the transaction and silently bound unrelated work on the same pooled
      # connection. The queries are read off repo telemetry rather than off the
      # source, and the session value is read back afterwards on the same
      # backend.
      parent = self()
      handler = {__MODULE__, make_ref()}
      on_exit(fn -> :telemetry.detach(handler) end)

      :ok =
        :telemetry.attach(
          handler,
          [:codex_pooler, :repo, :query],
          fn _event, _measurements, metadata, _config ->
            if Process.get({__MODULE__, :recording}),
              do: send(parent, {:query, metadata.query})
          end,
          nil
        )

      for {label, call} <- [
            {:claim, fn -> Relay.claim(1, "timeout-probe") end},
            {:expire, &Relay.expire_counted/0},
            {:prune, &Relay.prune/0}
          ] do
        session_default =
          run_unboxed(fn ->
            Process.put({__MODULE__, :recording}, true)
            before = show_statement_timeout()
            call.()
            after_call = show_statement_timeout()
            Process.delete({__MODULE__, :recording})

            assert before == after_call,
                   "#{label} left statement_timeout at #{after_call} on the session"

            after_call
          end)

        assert_receive {:query, "SET LOCAL statement_timeout = '5s'"}, @task_budget

        # The default is whatever the pooled connection was configured with;
        # what matters is that the five seconds did not become it.
        refute session_default == "5s"
        drain_recorded_queries()
      end
    end
  end

  describe "who the heartbeat is about" do
    test "no fresh consumer never stops a producer; the backlog is bounded by counted expiry" do
      # The ticket's 14:57Z entry read the heartbeat as consumer liveness. It
      # is not: `Relay.insert/5` gates on the *producer's own* freshness, so a
      # cluster with no draining reporter keeps producing. What bounds that
      # backlog is the one-hour counted expiry, and what makes the situation
      # visible is `fresh_consumers`. Both are asserted here so the corrected
      # semantics cannot quietly change back.
      #
      # Totals are read as deltas because focused suites share this database
      # and another one may hold committed relay rows of its own.
      baseline = Relay.health()
      before_expired = expired_loss()

      assert {:ok, %RelayEvent{}} = Relay.insert("stale_sweep", %{"via" => "job_relay"}, 3)

      after_insert = Relay.health()
      assert after_insert.backlog_rows - baseline.backlog_rows == 1
      assert after_insert.backlog_samples - baseline.backlog_samples == 3

      # A quiesced consumer is present but not draining, and must not read as
      # liveness.
      :ok = Relay.consumer_heartbeat("contract-quiesced", true)
      assert Relay.health().fresh_consumers == baseline.fresh_consumers

      :ok = Relay.consumer_heartbeat("contract-live", false)
      assert Relay.health().fresh_consumers == baseline.fresh_consumers + 1

      # A stale consumer heartbeat is not liveness either.
      Repo.query!(
        "UPDATE telemetry_relay_consumers SET heartbeat_at = clock_timestamp() - interval '120 seconds' WHERE owner = 'contract-live'"
      )

      assert Relay.health().fresh_consumers == baseline.fresh_consumers

      # Production continues regardless of any consumer's state: the only gate
      # is the producer's own heartbeat, proven by removing it.
      assert {:ok, %RelayEvent{}} = Relay.insert("stale_sweep", %{"via" => "job_relay"}, 2)
      Repo.query!("DELETE FROM telemetry_relay_heartbeats WHERE owner = 'relay-runtime'")
      assert {:error, :stale_heartbeat} = Relay.insert("stale_sweep", %{"via" => "job_relay"}, 1)

      # And an undrained backlog is reclaimed with its samples counted, not
      # silently dropped.
      aged =
        from(e in RelayEvent,
          where: is_nil(e.claimed_at) and e.inserted_at > ago(1, "hour")
        )

      {aged_rows, _} =
        Repo.update_all(aged,
          set: [inserted_at: DateTime.add(DateTime.utc_now(), -3700, :second)]
        )

      assert aged_rows >= 2
      assert {^aged_rows, _} = Relay.expire_counted()
      assert expired_loss() - before_expired >= 5
    end
  end

  describe "a relay outage" do
    test "does not reach the business transaction that emitted the event" do
      # Capture runs in the emitting process, inside whatever transaction that
      # process has open. If it could raise, or touch the database, a relay
      # outage would roll back real work. The outage here is the real one the
      # persistence boundary produces: the producer's heartbeat is gone, so
      # every insert is refused.
      owner = self()

      runtime =
        start_supervised!(
          {RelayRuntime,
           enabled: true,
           role: "worker",
           start_paused: true,
           name: {:global, {__MODULE__, make_ref()}},
           flush_ms: 60_000,
           drain_ms: 60_000}
        )

      Sandbox.allow(Repo, owner, runtime)
      :ok = GenServer.call(runtime, :activate)
      state = :sys.get_state(runtime)

      run_unboxed(fn ->
        Repo.query!("DELETE FROM telemetry_relay_heartbeats WHERE owner = $1", [state.owner])
      end)

      pool_name = "relay-outage-#{System.unique_integer([:positive])}"

      register_unboxed_cleanup!(fn ->
        Repo.delete_all(from p in CodexPooler.Pools.Pool, where: p.name == ^pool_name)
      end)

      committed =
        run_unboxed(fn ->
          Repo.transaction(fn ->
            pool = pool_fixture(%{name: pool_name})

            :telemetry.execute(
              [:codex_pooler, :accounting, :reservation, :pre_attempt_release],
              %{count: 1},
              %{
                phase: "stale_sweep",
                transport: "http_sse",
                outcome: "stale_reservation_recovered"
              }
            )

            pool.id
          end)
        end)

      assert {:ok, pool_id} = committed

      assert run_unboxed(fn -> Repo.get(CodexPooler.Pools.Pool, pool_id) end),
             "the business row was lost to a relay outage"

      # The event was captured in memory and the flush failed against the
      # database; neither reached the caller, and the handler is still attached.
      send(runtime, :flush)
      :sys.get_state(runtime)
      assert Process.alive?(runtime)

      assert Enum.any?(:telemetry.list_handlers([]), &(&1.id == state.handler)),
             "the failed flush detached the capture handler"

      # The outage is real rather than assumed: this producer's own writes are
      # still refused after the flush attempt.
      assert run_unboxed(fn ->
               Relay.insert("pre_attempt_release", %{}, 1, %{}, state.owner)
             end) == {:error, :stale_heartbeat}
    end
  end

  defp raw_insert(overrides) do
    attrs =
      Map.merge(
        %{event: "stale_sweep", labels: %{}, count: 1, measurements: %{}},
        overrides
      )

    Repo.query(
      "INSERT INTO telemetry_relay_events (event, labels, count, measurements, inserted_at) VALUES ($1, $2, $3, $4, $5)",
      [
        attrs.event,
        attrs.labels,
        attrs.count,
        attrs.measurements,
        DateTime.utc_now()
      ]
    )
  end

  defp expired_loss do
    %{rows: rows} =
      Repo.query!("SELECT samples FROM telemetry_relay_losses WHERE reason = 'expired_unclaimed'")

    case rows do
      [[samples]] -> samples
      [] -> 0
    end
  end

  defp show_statement_timeout do
    %{rows: [[value]]} = Repo.query!("SHOW statement_timeout")
    value
  end

  defp drain_recorded_queries do
    receive do
      {:query, _} -> drain_recorded_queries()
    after
      0 -> :ok
    end
  end
end
