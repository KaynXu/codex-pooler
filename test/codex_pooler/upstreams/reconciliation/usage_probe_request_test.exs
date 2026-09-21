defmodule CodexPooler.Upstreams.Reconciliation.UsageProbeRequestTest do
  use CodexPooler.DataCase, async: false

  import CodexPooler.PoolerFixtures

  alias Ecto.Adapters.SQL.Sandbox

  alias CodexPooler.FakeUpstream
  alias CodexPooler.Repo
  alias CodexPooler.UpstreamConnPoolTelemetry
  alias CodexPooler.Upstreams.Quota.Windows
  alias CodexPooler.Upstreams.Reconciliation.PoolReconciliation
  alias CodexPooler.Upstreams.Reconciliation.UsageProbe

  @account_id "acct_usage_header_contract"
  @probe_detection_timeout_ms 15_000

  test "usage GETs match current Codex and omit an explicit JSON Accept header" do
    observed_at = DateTime.utc_now() |> DateTime.truncate(:second)

    payload = %{
      "rate_limit" => %{
        "primary_window" => %{
          "used_percent" => 1,
          "limit_window_seconds" => 18_000,
          "reset_after_seconds" => 3_600,
          "reset_at" => DateTime.to_unix(DateTime.add(observed_at, 3_600, :second))
        }
      }
    }

    {:ok, fake} =
      FakeUpstream.start_link(
        {:path_json,
         %{
           "/backend-api/wham/usage" => {404, %{}},
           "/backend-api/codex/usage" => {200, payload}
         }}
      )

    on_exit(fn -> FakeUpstream.stop(fake) end)

    %{identity: identity, assignment: assignment, access_token: access_token} =
      active_upstream_assignment_fixture(pool_fixture(), %{
        chatgpt_account_id: @account_id,
        metadata: %{"usage_base_url" => FakeUpstream.url(fake)}
      })

    assert {:ok, %UsageProbe.Result{usage_path: "/backend-api/codex/usage"}} =
             UsageProbe.fetch_from_identity(identity, assignment, observed_at, [])

    requests = FakeUpstream.requests(fake)

    assert Enum.map(requests, & &1.path) == [
             "/backend-api/wham/usage",
             "/backend-api/codex/usage"
           ]

    Enum.each(requests, fn request ->
      headers = Map.new(request.headers)

      assert headers["authorization"] == "Bearer #{access_token}"
      assert headers["chatgpt-account-id"] == @account_id
      refute Map.has_key?(headers, "accept")
    end)
  end

  test "usage and reset-credit GETs carry the upstream connection idle bound from settings" do
    observed_at = DateTime.utc_now() |> DateTime.truncate(:second)

    payload = %{
      "plan_type" => "plus",
      "rate_limit" => %{"allowed" => true, "limit_reached" => false},
      "rate_limit_reset_credits" => %{"available_count" => 1}
    }

    {:ok, fake} =
      FakeUpstream.start_link(
        {:path_json,
         %{
           "/backend-api/wham/usage" => {200, payload},
           "/backend-api/codex/usage" => {200, payload},
           "/backend-api/wham/rate-limit-reset-credits" => {200, %{"items" => []}}
         }}
      )

    on_exit(fn -> FakeUpstream.stop(fake) end)

    UpstreamConnPoolTelemetry.put_idle_bound!(0)
    UpstreamConnPoolTelemetry.attach!(FakeUpstream.url(fake))

    %{identity: identity, assignment: assignment} =
      active_upstream_assignment_fixture(pool_fixture(), %{
        chatgpt_account_id: @account_id,
        metadata: %{"usage_base_url" => FakeUpstream.url(fake)}
      })

    assert {:ok, %UsageProbe.Result{}} =
             UsageProbe.fetch_from_identity(identity, assignment, observed_at, [])

    paths = fake |> FakeUpstream.requests() |> Enum.map(& &1.path)
    assert "/backend-api/wham/rate-limit-reset-credits" in paths
    assert length(paths) >= 2

    assert UpstreamConnPoolTelemetry.drain_events() ==
             List.duplicate(:conn_max_idle_time_exceeded, length(paths) - 1)
  end

  for stage <- [:before_headers, :mid_stream], earlier_success? <- [false, true] do
    test "#{stage} timeout preserves only earlier successful coverage=#{earlier_success?}" do
      assert_timeout_coverage(unquote(stage), unquote(earlier_success?))
    end
  end

  defp assert_timeout_coverage(stage, earlier_success?) do
    observed_at = DateTime.utc_now()
    release_ref = make_ref()

    response = timeout_response(stage, release_ref)

    entries =
      if earlier_success? do
        [
          usage_request("/api/codex/usage", FakeUpstream.json_response(weekly_payload())),
          usage_request("/backend-api/codex/usage", response)
        ]
      else
        [usage_request("/api/codex/usage", response)]
      end

    {fake, identity, assignment} = probe_fixture(entries)
    task = start_probe(identity, assignment, observed_at, 200)

    assert_receive {:fake_upstream_timeout_barrier, ^stage, handler, ^release_ref},
                   @probe_detection_timeout_ms

    handler_monitor = Process.monitor(handler)

    try do
      # Exercise the real Finch receive timeout while the response is held.
      # The one-second budget lets an earlier healthy request complete under load.
      result = Task.await(task, @probe_detection_timeout_ms)

      if earlier_success? do
        assert {:ok, %UsageProbe.Result{} = probe} = result
        assert probe.usage_path == "/api/codex/usage"
        assert length(probe.windows) == 1
        assert MapSet.size(probe.covered_descriptors) == 1
      else
        assert {:error, %{reason: :timeout}} = result
      end

      assert FakeUpstream.count(fake) == length(entries)
      assert :ok = FakeUpstream.verify!(fake)
      send(handler, {:fake_upstream_release_timeout, release_ref})

      assert_receive {:DOWN, ^handler_monitor, :process, ^handler, _reason},
                     @probe_detection_timeout_ms
    after
      send(handler, {:fake_upstream_release_timeout, release_ref})
    end
  end

  test "canceling an in-flight probe cannot dispatch a fallback or apply its late response" do
    release_ref = make_ref()

    {fake, identity, assignment} =
      probe_fixture([
        usage_request(
          "/api/codex/usage",
          {:gated_json_headers, 200, primary_payload(), self(), release_ref}
        )
      ])

    owner = self()
    supervisor = start_supervised!(Task.Supervisor)

    task =
      Task.Supervisor.async_nolink(supervisor, fn ->
        Sandbox.allow(Repo, owner, self())
        PoolReconciliation.refresh_quota_from_usage(identity, assignment, receive_timeout: 30_000)
      end)

    assert_receive {:fake_upstream_gate, :before_headers, handler, ^release_ref},
                   @probe_detection_timeout_ms

    handler_monitor = Process.monitor(handler)

    try do
      assert nil == Task.shutdown(task, :brutal_kill)
      send(handler, {:fake_upstream_release_gate, release_ref})

      assert_receive {:DOWN, ^handler_monitor, :process, ^handler, _reason},
                     @probe_detection_timeout_ms

      assert FakeUpstream.count(fake) == 1
      assert :ok = FakeUpstream.verify!(fake)
      assert Windows.list_evidence(identity) == []
      assert Repo.reload!(identity).metadata["usage_probe_sequence"] == 1
      assert Repo.reload!(identity).metadata["usage_probe_applied_sequence"] == 0
    after
      send(handler, {:fake_upstream_release_gate, release_ref})
    end
  end

  for {label, first_response} <- [
        not_found: {:json, 404, %{}},
        rate_limited: {:json, 429, %{}},
        empty: {:json, 200, %{}},
        malformed: {:malformed_json, 200, "{"}
      ] do
    test "#{label} response falls back once without adding descriptor coverage" do
      {fake, identity, assignment} =
        probe_fixture([
          usage_request("/api/codex/usage", unquote(Macro.escape(first_response))),
          usage_request("/backend-api/codex/usage", FakeUpstream.json_response(primary_payload()))
        ])

      assert {:ok, %UsageProbe.Result{} = probe} =
               UsageProbe.fetch_from_identity(identity, assignment, DateTime.utc_now(), [])

      assert probe.usage_path == "/backend-api/codex/usage"
      assert length(probe.windows) == 1
      assert MapSet.size(probe.covered_descriptors) == 1
      assert FakeUpstream.count(fake) == 2
      assert :ok = FakeUpstream.verify!(fake)
    end
  end

  test "server failure stops probing and does not silently retry the GET" do
    {fake, identity, assignment} =
      probe_fixture([
        usage_request("/api/codex/usage", FakeUpstream.json_response(%{}, 503))
      ])

    assert {:error, {:upstream_status, 503}} =
             UsageProbe.fetch_from_identity(identity, assignment, DateTime.utc_now(), [])

    assert FakeUpstream.count(fake) == 1
    assert :ok = FakeUpstream.verify!(fake)
  end

  for {label, body} <- [array: "[]", null: "null", number: "42", string: "\"invalid\""] do
    test "#{label} reset-credit detail cannot discard a successful quota probe" do
      payload = Map.put(primary_payload(), "rate_limit_reset_credits", %{"available_count" => 1})

      invalid_detail =
        FakeUpstream.raw_response(unquote(body), headers: [{"content-type", "application/json"}])

      {fake, identity, assignment} =
        probe_fixture([
          usage_request("/api/codex/usage", FakeUpstream.json_response(payload)),
          usage_request("/backend-api/wham/rate-limit-reset-credits", invalid_detail),
          usage_request("/wham/rate-limit-reset-credits", invalid_detail)
        ])

      assert {:ok, %UsageProbe.Result{} = probe} =
               UsageProbe.fetch_from_identity(identity, assignment, DateTime.utc_now(), [])

      assert length(probe.windows) == 1
      assert MapSet.size(probe.covered_descriptors) == 1
      assert probe.payload["rate_limit_reset_credits"]["available_count"] == 1
      assert FakeUpstream.count(fake) == 3
      assert :ok = FakeUpstream.verify!(fake)
    end
  end

  defp start_probe(identity, assignment, observed_at, receive_timeout) do
    owner = self()
    supervisor = start_supervised!(Task.Supervisor)

    Task.Supervisor.async_nolink(supervisor, fn ->
      Sandbox.allow(Repo, owner, self())

      UsageProbe.fetch_from_identity(identity, assignment, observed_at, receive_timeout: receive_timeout)
    end)
  end

  defp probe_fixture(entries) do
    # provenance: synthetic_adversarial
    {:ok, fake} = FakeUpstream.start_link(FakeUpstream.strict_sequence(entries))
    on_exit(fn -> FakeUpstream.stop(fake) end)

    %{identity: identity, assignment: assignment} =
      active_upstream_assignment_fixture(pool_fixture(), %{
        chatgpt_account_id: @account_id,
        metadata: %{
          "usage_base_url" => FakeUpstream.url(fake),
          "usage_path" => "/api/codex/usage"
        }
      })

    {fake, identity, assignment}
  end

  defp usage_request(path, response),
    do: FakeUpstream.expect_request(method: "GET", path: path, respond: response)

  defp timeout_response(:before_headers, release_ref),
    do: {:timeout_before_headers, self(), release_ref}

  defp timeout_response(:mid_stream, release_ref),
    do: {:timeout_mid_stream, "{", self(), release_ref}

  defp weekly_payload, do: quota_payload(604_800)
  defp primary_payload, do: quota_payload(18_000)

  defp quota_payload(window_seconds) do
    %{
      "rate_limit" => %{
        "primary_window" => %{
          "used_percent" => 12,
          "limit_window_seconds" => window_seconds,
          "reset_after_seconds" => 3_600,
          "reset_at" => System.system_time(:second) + 3_600
        }
      }
    }
  end
end
