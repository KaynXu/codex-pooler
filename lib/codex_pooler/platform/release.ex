defmodule CodexPooler.Release do
  @moduledoc """
  Release-only tasks for production operations.

  These functions are intended to be invoked explicitly with the assembled
  release, for example:

      bin/codex_pooler eval "CodexPooler.Release.migrate()"
  """

  alias CodexPooler.Catalog
  alias CodexPooler.Gateway.Transports.Websocket.RolloutDrain
  alias CodexPooler.Telemetry.RelayRuntime

  @app :codex_pooler

  @doc "Quiesces relay claims before readiness withdrawal within the existing drain budget."
  @spec prepare_shutdown(keyword()) :: map()
  def prepare_shutdown(opts \\ []) do
    started = System.monotonic_time(:millisecond)

    budget =
      Keyword.get_lazy(
        opts,
        :budget_ms,
        &RolloutDrain.configured_timeout_ms/0
      )

    # Resolve the marker path before quiescing: quiesce is permanent for the
    # consumer, so a misconfigured path must fail before it, not after.
    marker =
      Keyword.get_lazy(opts, :marker, fn ->
        System.fetch_env!("CODEX_POOLER_DRAIN_MARKER_PATH")
      end)

    :ok =
      RelayRuntime.quiesce(
        Keyword.get(opts, :relay, RelayRuntime),
        min(budget, 5_000)
      )

    :ok = File.touch(marker)
    remaining = max(budget - (System.monotonic_time(:millisecond) - started), 1)

    drain =
      Keyword.get(
        opts,
        :drain,
        &RolloutDrain.drain_for_shutdown/1
      )

    drain.(remaining)
  end

  # A release task runs with the runtime config of whatever `OBAN_MODE` its
  # container carries (the migration job renders `web`); its own PostgreSQL
  # application_name keeps its backends distinguishable from serving pods.
  @task_application_names %{
    migrate: "codex_pooler_migrate",
    rollback: "codex_pooler_migrate",
    import_openai_pricing: "codex_pooler_pricing_import"
  }

  def migrate do
    load_app()

    for repo <- repos() do
      with_task_repo_config(repo, :migrate, fn ->
        {:ok, _apps, _fun_result} =
          Ecto.Migrator.with_repo(repo, &Ecto.Migrator.run(&1, :up, all: true))
      end)
    end
  end

  def rollback(repo, version) do
    load_app()

    with_task_repo_config(repo, :rollback, fn ->
      {:ok, _apps, _fun_result} =
        Ecto.Migrator.with_repo(repo, &Ecto.Migrator.run(&1, :down, to: version))
    end)
  end

  def import_openai_pricing_from_priv do
    load_app()

    for repo <- repos() do
      with_task_repo_config(repo, :import_openai_pricing, fn ->
        {:ok, result, _started} = Ecto.Migrator.with_repo(repo, &import_pricing/1)
        result
      end)
    end
  end

  @doc false
  @spec repo_config_for_task(keyword(), :migrate | :rollback | :import_openai_pricing) ::
          keyword()
  def repo_config_for_task(repo_config, task) when is_list(repo_config) do
    parameters =
      repo_config
      |> Keyword.get(:parameters, [])
      |> Keyword.put(:application_name, Map.fetch!(@task_application_names, task))

    Keyword.put(repo_config, :parameters, parameters)
  end

  # The task's connection name applies only while the task runs: a release
  # task VM exits afterwards, but callers in a running node (tests, remote
  # consoles) keep using the Repo config and must get the original back.
  defp with_task_repo_config(repo, task, fun) do
    previous = Application.fetch_env(@app, repo)

    Application.put_env(
      @app,
      repo,
      repo_config_for_task(Application.get_env(@app, repo, []), task)
    )

    try do
      fun.()
    after
      case previous do
        {:ok, config} -> Application.put_env(@app, repo, config)
        :error -> Application.delete_env(@app, repo)
      end
    end
  end

  defp import_pricing(_repo) do
    {:ok, import_result} = Catalog.import_openai_pricing_from_priv()
    import_result
  end

  defp repos do
    Application.fetch_env!(@app, :ecto_repos)
  end

  defp load_app do
    Application.load(@app)
  end
end
