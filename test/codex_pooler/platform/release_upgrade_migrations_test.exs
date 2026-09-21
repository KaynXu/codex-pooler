defmodule CodexPooler.ReleaseUpgradeMigrationsTest do
  use ExUnit.Case, async: false

  alias Ecto.Adapters.Postgres

  @moduletag timeout: 180_000

  for scenario <-
        ~w(widths fresh head invalid invalid_owner client_exit locks historical_indexes validation null_history migration_lock index_conflicts rollback_cancel rollback_delete budget_upgrade budget_locks budget_traffic budget_online budget_indexes budget_missing budget_plan) do
    @tag scenario: scenario
    test "release upgrade rehearsal: #{scenario}" do
      namespace = :crypto.strong_rand_bytes(8) |> Base.encode16(case: :lower)
      config = CodexPooler.Repo.config()

      base =
        System.get_env("CODEX_POOLER_TEST_POSTGRES_DB") ||
          System.get_env("POSTGRES_TEST_DB", "codex_pooler_test")

      fingerprint =
        :crypto.hash(:sha256, base) |> Base.encode16(case: :lower) |> binary_part(0, 8)

      database = "codex_pooler_test_#{fingerprint}_#{namespace}_p1"
      assert database != config[:database]

      on_exit(fn ->
        options = Keyword.merge(config, database: database, force_drop: true, timeout: 60_000)

        case Postgres.storage_down(options) do
          :ok -> :ok
          {:error, :already_down} -> :ok
        end

        assert Postgres.storage_status(options) == :down
      end)

      # The parent test invocation already selected the project runtime. Reuse
      # that exact Mix executable so the isolated rehearsal also works in the
      # release CI image, which intentionally does not install mise.
      mix = System.find_executable("mix") || raise "mix executable not found"

      {output, rc} =
        System.cmd(
          mix,
          [
            "run",
            "--no-start",
            "--no-compile",
            "scripts/verification/release_upgrade_migrations.exs",
            "--scenario",
            unquote(scenario)
          ],
          env: [
            {"MIX_ENV", "test"},
            {"MIX_TEST_PARTITION", "1"},
            {"CODEX_POOLER_TEST_POSTGRES_HOST",
             Keyword.fetch!(CodexPooler.Repo.config(), :hostname)},
            {"CODEX_POOLER_TEST_RUN_NAMESPACE", namespace},
            {"DATABASE_URL", nil}
          ],
          stderr_to_stdout: true
        )

      CodexPooler.TestDiagnostics.puts(output)
      assert output =~ ~s("database_dropped":true), output
      assert rc == 0, output
      refute output =~ "warning:", output
    end
  end
end
