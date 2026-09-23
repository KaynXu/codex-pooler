defmodule CodexPoolerWeb.Operations.HealthControllerTest do
  use CodexPoolerWeb.ConnCase, async: false

  import ExUnit.CaptureLog

  alias CodexPooler.Accounting.Request
  alias CodexPooler.Gateway.OperationalSettings
  alias CodexPooler.Gateway.OperationalStatus
  alias CodexPooler.Gateway.Transports.Streaming.DeferredStreamRegistry
  alias CodexPooler.Gateway.Transports.Websocket.{ActivityRegistry, RolloutDrain}
  alias CodexPooler.Platform.Readiness
  alias CodexPooler.Repo
  alias Ecto.Adapters.SQL

  setup do
    CodexPooler.TestAppEnv.restore_on_exit(Readiness)
    CodexPooler.TestAppEnv.restore_on_exit(OperationalStatus)
    CodexPooler.TestAppEnv.restore_on_exit(RolloutDrain)
    CodexPooler.TestAppEnv.restore_on_exit(OperationalSettings)

    # The grace window is a per-node fact that outlives a single test, so every
    # test here starts from a node that has never been ready.
    :ok = Readiness.reset_state!()
    on_exit(&Readiness.reset_state!/0)
  end

  test "GET /healthz returns a lightweight liveness response", %{conn: conn} do
    conn = get(conn, ~p"/healthz")

    assert json_response(conn, 200) == %{"status" => "ok"}
  end

  test "GET /readyz verifies database readiness", %{conn: conn} do
    conn = get(conn, ~p"/readyz")

    assert json_response(conn, 200) == %{"status" => "ready"}
  end

  test "GET /readyz withdraws readiness when this image's migrations are not applied",
       %{conn: conn} do
    assert conn |> get(~p"/readyz") |> json_response(200) == %{"status" => "ready"}

    # The database the pod was routed to no longer carries this image's schema.
    # Connectivity is untouched, which is exactly why `select 1` reported ready.
    Repo.query!("DELETE FROM schema_migrations")

    {conn, log} = with_log([level: :info], fn -> get(recycle(conn), ~p"/readyz") end)

    assert json_response(conn, 503) == %{"status" => "unavailable"}
    assert log =~ "readiness probe failed path=/readyz reason_class=migrations_missing"
  end

  test "GET /readyz withdraws readiness immediately when the schema is gone", %{conn: conn} do
    assert conn |> get(~p"/readyz") |> json_response(200) == %{"status" => "ready"}

    Application.put_env(:codex_pooler, Readiness, sql_probe: __MODULE__.MissingSchemaProbe)

    # A missing relation is permanent until an operator acts, so the grace
    # window that protects a connectivity blip must not apply to it.
    {conn, log} = with_log([level: :info], fn -> get(recycle(conn), ~p"/readyz") end)

    assert json_response(conn, 503) == %{"status" => "unavailable"}
    assert log =~ "readiness probe failed path=/readyz reason_class=undefined_table"
  end

  test "GET /readyz returns 503 when packaged migrations cannot be verified", %{conn: conn} do
    missing =
      Path.join(
        System.tmp_dir!(),
        "codex-pooler-missing-migrations-#{System.unique_integer([:positive])}"
      )

    Application.put_env(:codex_pooler, Readiness, migrations_path: missing)

    {conn, log} = with_log([level: :info], fn -> get(conn, ~p"/readyz") end)

    assert json_response(conn, 503) == %{"status" => "unavailable"}

    assert log =~
             "readiness probe failed path=/readyz reason_class=migration_directory_unreadable"
  end

  test "GET /readyz bounds query encoding exceptions as sanitized 503 responses", %{conn: conn} do
    Application.put_env(:codex_pooler, Readiness, sql_probe: __MODULE__.EncodingErrorProbe)

    {conn, log} = with_log([level: :info], fn -> get(conn, ~p"/readyz") end)

    assert json_response(conn, 503) == %{"status" => "unavailable"}
    assert log =~ "readiness probe failed path=/readyz reason_class=DBConnection.EncodeError"
    refute log =~ "example-secret"
  end

  test "GET /readyz keeps readiness through a brief connectivity failure", %{conn: conn} do
    assert conn |> get(~p"/readyz") |> json_response(200) == %{"status" => "ready"}

    Application.put_env(:codex_pooler, Readiness, sql_probe: __MODULE__.UnavailableReadinessProbe)

    # Every pod sees a database blip at the same instant. Withdrawing here
    # would empty the Service of endpoints for a fault that heals itself.
    {conn, log} = with_log([level: :info], fn -> get(recycle(conn), ~p"/readyz") end)

    assert json_response(conn, 200) == %{"status" => "ready"}

    assert log =~
             "readiness probe degraded path=/readyz reason_class=DBConnection.ConnectionError"

    refute log =~ "readiness probe failed"
  end

  test "GET /readyz withdraws readiness on connectivity failure before any success",
       %{conn: conn} do
    Application.put_env(:codex_pooler, Readiness, sql_probe: __MODULE__.UnavailableReadinessProbe)

    # A pod that has never reached the database holds no endpoint, so refusing
    # it costs the Service nothing and keeps a broken rollout from reporting success.
    {conn, log} = with_log([level: :info], fn -> get(conn, ~p"/readyz") end)

    assert json_response(conn, 503) == %{"status" => "unavailable"}

    assert log =~
             "readiness probe failed path=/readyz reason_class=DBConnection.ConnectionError"
  end

  @tag :capture_log
  test "health and readiness retain independent behavior while runtime settings are cold", %{
    conn: conn
  } do
    Application.put_env(:codex_pooler, OperationalSettings,
      settings: %OperationalSettings{
        source: :fallback_defaults,
        db_available?: false,
        secrets_available?: false
      },
      use_instance_settings?: false
    )

    Application.put_env(:codex_pooler, Readiness, sql_probe: __MODULE__.UnavailableReadinessProbe)

    assert conn |> get(~p"/healthz") |> json_response(200) == %{"status" => "ok"}

    assert conn |> recycle() |> get(~p"/readyz") |> json_response(503) == %{
             "status" => "unavailable"
           }
  end

  test "GET /readyz stays ready when configured drain marker is absent", %{conn: conn} do
    drain_marker_path = drain_marker_path()

    Application.put_env(:codex_pooler, OperationalStatus, drain_marker_path: drain_marker_path)

    Application.put_env(:codex_pooler, Readiness, sql_probe: __MODULE__.AvailableReadinessProbe)

    conn = get(conn, ~p"/readyz")

    assert json_response(conn, 200) == %{"status" => "ready"}
    assert_receive :available_readiness_probe_called
  end

  test "GET /readyz returns to ready after a configured drain marker is removed", %{conn: conn} do
    drain_marker_path = drain_marker_path()
    File.write!(drain_marker_path, "draining")

    Application.put_env(:codex_pooler, OperationalStatus, drain_marker_path: drain_marker_path)

    Application.put_env(:codex_pooler, Readiness, sql_probe: __MODULE__.AvailableReadinessProbe)

    assert conn |> get(~p"/readyz") |> json_response(503) == %{"status" => "unavailable"}
    refute_received :available_readiness_probe_called

    assert :ok = File.rm(drain_marker_path)

    assert conn |> recycle() |> get(~p"/readyz") |> json_response(200) == %{"status" => "ready"}
    assert_receive :available_readiness_probe_called
  end

  test "GET /readyz returns unavailable while drain marker exists without probing DB", %{
    conn: conn
  } do
    drain_marker_path = drain_marker_path()
    File.write!(drain_marker_path, "draining")
    on_exit(fn -> File.rm(drain_marker_path) end)

    Application.put_env(:codex_pooler, OperationalStatus, drain_marker_path: drain_marker_path)

    Application.put_env(:codex_pooler, Readiness, sql_probe: __MODULE__.UnexpectedReadinessProbe)

    {conn, log} = with_log([level: :info], fn -> get(conn, ~p"/readyz") end)

    assert json_response(conn, 503) == %{"status" => "unavailable"}
    refute log =~ "readiness probe failed"
    refute_received :unexpected_readiness_probe_called
  end

  test "GET /readyz returns unavailable while runtime rollout drain is active without marker",
       %{conn: conn} do
    activity_registry = :"health-rollout-activity-#{System.unique_integer([:positive])}"
    drain_name = :"health-rollout-drain-#{System.unique_integer([:positive])}"
    stream_registry = :"health-rollout-streams-#{System.unique_integer([:positive])}"
    start_supervised!({ActivityRegistry, name: activity_registry})
    # Draining marks a registry drained for good, so keep the deferred-stream
    # registry local rather than flipping the global one for later tests.
    start_supervised!({DeferredStreamRegistry, name: stream_registry})

    start_supervised!({RolloutDrain, name: drain_name, activity_registry: activity_registry, stream_registry: stream_registry})

    Application.put_env(:codex_pooler, RolloutDrain, server_name: drain_name)

    Application.put_env(:codex_pooler, OperationalStatus, drain_marker_path: drain_marker_path())

    Application.put_env(:codex_pooler, Readiness, sql_probe: __MODULE__.UnexpectedReadinessProbe)

    refute ActivityRegistry.draining?()

    assert %{result: :ok, owners_seen: 0} =
             RolloutDrain.start_drain(name: drain_name, timeout_ms: 100)

    assert ActivityRegistry.draining?(name: activity_registry)
    refute ActivityRegistry.draining?()
    assert RolloutDrain.draining?(name: drain_name)

    {conn, log} = with_log([level: :info], fn -> get(conn, ~p"/readyz") end)

    assert json_response(conn, 503) == %{"status" => "unavailable"}
    refute log =~ "readiness probe failed"
    refute_received :unexpected_readiness_probe_called
  end

  test "healthy probes disable endpoint info request logging and request rows", %{conn: conn} do
    before_count = Repo.aggregate(Request, :count)

    events =
      capture_endpoint_log_decisions(fn ->
        conn |> get(~p"/healthz") |> json_response(200)
        conn |> recycle() |> get(~p"/readyz") |> json_response(200)
      end)

    assert length(events) == 4
    assert Enum.all?(events, &(&1.log_level == false))

    assert Enum.map(events, & &1.path) |> Enum.sort() == [
             "/healthz",
             "/healthz",
             "/readyz",
             "/readyz"
           ]

    assert Repo.aggregate(Request, :count) == before_count
  end

  test "readiness failures emit sanitized warning and no accounting request row", %{conn: conn} do
    Application.put_env(:codex_pooler, Readiness, sql_probe: __MODULE__.UnavailableReadinessProbe)

    before_count = Repo.aggregate(Request, :count)

    {conn, log} =
      with_log([level: :info], fn ->
        get(conn, ~p"/readyz")
      end)

    assert json_response(conn, 503) == %{"status" => "unavailable"}

    assert log =~
             "readiness probe failed path=/readyz reason_class=DBConnection.ConnectionError"

    refute log =~ "database refused example-secret"
    refute log =~ "GET /readyz"
    refute log =~ "Sent 503"
    assert Repo.aggregate(Request, :count) == before_count
  end

  test "non-health runtime requests keep endpoint info logging", %{conn: conn} do
    events =
      capture_endpoint_log_decisions(fn ->
        conn |> get(~p"/backend-api/codex/models") |> response(401)
      end)

    assert length(events) == 2
    assert Enum.all?(events, &(&1.path == "/backend-api/codex/models"))
    assert Enum.all?(events, &(&1.log_level == :info))
  end

  defp capture_endpoint_log_decisions(fun) do
    test_pid = self()
    handler_id = {__MODULE__, test_pid, System.unique_integer([:positive])}

    # Also on_exit: a linked crash or the ExUnit timeout kills the test before `after` runs.
    on_exit(fn -> :telemetry.detach(handler_id) end)

    :ok =
      :telemetry.attach_many(
        handler_id,
        [[:phoenix, :endpoint, :start], [:phoenix, :endpoint, :stop]],
        fn event, _measurements, metadata, destination ->
          send(
            destination,
            {:endpoint_log_decision, event, metadata.conn.request_path, endpoint_log_level(metadata)}
          )
        end,
        test_pid
      )

    try do
      fun.()
      collect_endpoint_log_decisions([])
    after
      :telemetry.detach(handler_id)
    end
  end

  defp collect_endpoint_log_decisions(events) do
    receive do
      {:endpoint_log_decision, event, path, log_level} ->
        collect_endpoint_log_decisions([
          %{event: event, path: path, log_level: log_level} | events
        ])
    after
      0 -> Enum.reverse(events)
    end
  end

  defp endpoint_log_level(%{options: options, conn: conn}) do
    case Keyword.fetch!(options, :log) do
      {module, function, args} -> apply(module, function, [conn | args])
      level -> level
    end
  end

  defp drain_marker_path do
    Path.join(
      System.tmp_dir!(),
      "codex-pooler-drain-#{System.unique_integer([:positive])}"
    )
  end

  defmodule AvailableReadinessProbe do
    def query(repo, statement, params, opts) do
      send(self(), :available_readiness_probe_called)
      SQL.query(repo, statement, params, opts)
    end
  end

  defmodule UnexpectedReadinessProbe do
    def query(_repo, _statement, _params, _opts) do
      send(self(), :unexpected_readiness_probe_called)
      raise "drain marker should short-circuit readiness probe"
    end
  end

  defmodule MissingSchemaProbe do
    def query(_repo, _statement, _params, _opts) do
      {:error,
       %Postgrex.Error{
         postgres: %{
           code: :undefined_table,
           message: "relation \"public.schema_migrations\" does not exist"
         }
       }}
    end
  end

  defmodule UnavailableReadinessProbe do
    def query(_repo, _statement, _params, _opts) do
      {:error, %DBConnection.ConnectionError{message: "database refused example-secret"}}
    end
  end

  defmodule EncodingErrorProbe do
    def query(_repo, _statement, _params, _opts) do
      raise DBConnection.EncodeError, "raw value example-secret"
    end
  end
end
