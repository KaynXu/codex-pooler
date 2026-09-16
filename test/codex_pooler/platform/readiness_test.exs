defmodule CodexPooler.Platform.ReadinessTest do
  use CodexPooler.DataCase, async: false

  alias CodexPooler.Platform.Readiness
  alias CodexPooler.Release
  alias CodexPooler.Repo

  setup do
    previous = Application.get_env(:codex_pooler, Readiness)

    :ok = Readiness.reset_state!()

    on_exit(fn ->
      :ok = Readiness.reset_state!()

      if previous do
        Application.put_env(:codex_pooler, Readiness, previous)
      else
        Application.delete_env(:codex_pooler, Readiness)
      end
    end)

    :ok
  end

  defmodule UnreachableProbe do
    def query(_repo, _statement, _params, _opts) do
      {:error, %DBConnection.ConnectionError{message: "tcp connect: connection refused"}}
    end
  end

  defmodule MissingSchemaProbe do
    def query(_repo, _statement, _params, _opts) do
      {:error, %Postgrex.Error{postgres: %{code: :undefined_table, message: "does not exist"}}}
    end
  end

  test "a migrated database is ready over the real query" do
    assert Readiness.check() == :ready
  end

  test "an applied schema newer than this image stays ready" do
    # A rollout migrates before it replaces pods, so the pods still serving the
    # previous release see versions they do not carry. Containment, not
    # equality, is what keeps them in the Service.
    Repo.query!("INSERT INTO schema_migrations (version, inserted_at) VALUES ($1, now())", [
      99_999_999_999_999
    ])

    assert Readiness.check() == :ready
  end

  test "an unapplied schema is not ready over the real query" do
    Repo.query!("DELETE FROM schema_migrations")

    assert Readiness.check() == {:not_ready, "migrations_missing"}
  end

  test "a connectivity failure inside the grace window keeps a previously ready node ready" do
    now = System.monotonic_time(:millisecond)

    assert Readiness.check(now_ms: now) == :ready

    assert Readiness.check(now_ms: now + Readiness.grace_ms(), sql_probe: UnreachableProbe) ==
             {:ready, :degraded, "DBConnection.ConnectionError"}
  end

  test "a connectivity failure past the grace window withdraws readiness" do
    now = System.monotonic_time(:millisecond)

    assert Readiness.check(now_ms: now) == :ready

    assert Readiness.check(now_ms: now + Readiness.grace_ms() + 1, sql_probe: UnreachableProbe) ==
             {:not_ready, "DBConnection.ConnectionError"}
  end

  test "a connectivity failure before any success withdraws readiness at once" do
    assert Readiness.check(sql_probe: UnreachableProbe) ==
             {:not_ready, "DBConnection.ConnectionError"}
  end

  test "a missing schema is never graced, however recently the node was ready" do
    now = System.monotonic_time(:millisecond)

    assert Readiness.check(now_ms: now) == :ready

    assert Readiness.check(now_ms: now, sql_probe: MissingSchemaProbe) ==
             {:not_ready, "undefined_table"}
  end

  describe "release readiness check for roles without an HTTP listener" do
    test "returns :ok against a migrated database" do
      assert Release.readiness_check() == :ok
    end

    test "tolerates a connectivity blip the same way the HTTP probe does" do
      assert Readiness.check() == :ready

      Application.put_env(:codex_pooler, Readiness, sql_probe: UnreachableProbe)

      assert Release.readiness_check() == :ok
    end

    test "raises a sanitized class when the schema is not applied" do
      Repo.query!("DELETE FROM schema_migrations")

      assert_raise RuntimeError, "readiness check failed reason_class=migrations_missing", fn ->
        Release.readiness_check()
      end
    end

    test "never carries a database message into the raised reason" do
      Application.put_env(:codex_pooler, Readiness, sql_probe: MissingSchemaProbe)

      error =
        assert_raise RuntimeError, fn -> Release.readiness_check() end

      assert error.message == "readiness check failed reason_class=undefined_table"
      refute error.message =~ "does not exist"
    end
  end
end
