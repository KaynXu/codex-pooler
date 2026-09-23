defmodule Mix.Tasks.Dev.RoutingStrategyFixtureTest do
  use ExUnit.Case, async: false

  import ExUnit.CaptureIO

  alias CodexPooler.JSON
  alias Mix.Tasks.Dev.RoutingStrategyFixture, as: RoutingStrategyFixtureTask

  @receipt_env "CODEX_POOLER_ROUTING_STRATEGY_FIXTURE_RECEIPT_PATH"

  setup do
    previous = System.get_env(@receipt_env)

    on_exit(fn ->
      if previous,
        do: System.put_env(@receipt_env, previous),
        else: System.delete_env(@receipt_env)
    end)

    :ok
  end

  test "status rejects a private receipt parent that escapes through a symlink" do
    token = random_hex(8)
    fixture_root = Path.join([File.cwd!(), "tmp", "routing-strategy-fixture"])
    link = Path.join(fixture_root, "build-#{token}")
    outside = Path.join(System.tmp_dir!(), "cxp-routing-strategy-outside-#{token}")
    File.mkdir_p!(fixture_root)
    File.mkdir_p!(outside)
    File.ln_s!(outside, link)
    System.put_env(@receipt_env, Path.join([link, "fixture", "setup.json"]))

    on_exit(fn ->
      File.rm(link)
      File.rm_rf(outside)
    end)

    assert_raise Mix.Error, ~r/private receipt path is invalid/, fn ->
      RoutingStrategyFixtureTask.run(["status"])
    end
  end

  test "status omits the absolute private receipt path" do
    token = random_hex(8)
    root = Path.join([File.cwd!(), "tmp", "routing-strategy-fixture", "build-#{token}"])
    receipt_path = Path.join([root, "fixture", "setup.json"])
    File.mkdir_p!(Path.dirname(receipt_path))
    System.put_env(@receipt_env, receipt_path)
    on_exit(fn -> File.rm_rf(root) end)

    output = capture_io(fn -> RoutingStrategyFixtureTask.run(["status"]) end)
    status = JSON.decode!(output)

    assert status == %{"status" => "absent", "leases" => 0, "receipt" => "private"}
    refute output =~ File.cwd!()
    refute output =~ "build-#{token}"
  end

  test "status rejects a symlink at the receipt target" do
    token = random_hex(8)
    root = Path.join([File.cwd!(), "tmp", "routing-strategy-fixture", "build-#{token}"])
    receipt_path = Path.join([root, "fixture", "setup.json"])
    outside = Path.join(System.tmp_dir!(), "cxp-routing-strategy-target-#{token}")
    File.mkdir_p!(Path.dirname(receipt_path))
    File.write!(outside, "outside")
    File.ln_s!(outside, receipt_path)
    System.put_env(@receipt_env, receipt_path)

    on_exit(fn ->
      File.rm_rf(root)
      File.rm(outside)
    end)

    assert_raise Mix.Error, ~r/private receipt path is invalid/, fn ->
      RoutingStrategyFixtureTask.run(["status"])
    end
  end

  defp random_hex(bytes) do
    bytes |> :crypto.strong_rand_bytes() |> Base.encode16(case: :lower)
  end
end
