defmodule CodexPooler.Platform.RepoApplicationNameTest do
  use CodexPooler.DataCase, async: false

  alias CodexPooler.Release

  test "Repo connections carry the configured PostgreSQL application_name" do
    assert Repo.config()[:parameters][:application_name] == "codex_pooler_test"

    assert %{rows: [["codex_pooler_test"]]} =
             Repo.query!("SELECT current_setting('application_name')", [])
  end

  test "release database tasks name their connections after the task and keep other parameters" do
    repo_config = [
      url: "ecto://user:pass@example.invalid/db",
      timeout: 1_234,
      parameters: [application_name: "codex_pooler_web", search_path: "public"]
    ]

    for {task, application_name} <- [
          migrate: "codex_pooler_migrate",
          rollback: "codex_pooler_migrate",
          import_openai_pricing: "codex_pooler_pricing_import"
        ] do
      config = Release.repo_config_for_task(repo_config, task)

      assert config[:url] == repo_config[:url]
      assert config[:parameters][:application_name] == application_name
      assert config[:parameters][:search_path] == "public"
      assert Keyword.get_values(config[:parameters], :application_name) == [application_name]
      assert byte_size(application_name) <= 63

      if task in [:migrate, :rollback] do
        assert config[:timeout] == :infinity
      else
        assert config[:timeout] == 1_234
      end
    end

    assert Release.repo_config_for_task([], :migrate) ==
             [timeout: :infinity, parameters: [application_name: "codex_pooler_migrate"]]
  end
end
