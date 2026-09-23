defmodule CodexPoolerWeb.Runtime.ResponsesAPIUpstreamTest do
  use CodexPoolerWeb.ConnCase, async: false

  import Ecto.Query
  import CodexPooler.AccountsFixtures
  import CodexPooler.PoolerFixtures

  import CodexPoolerWeb.Runtime.BackendCodexTestSupport,
    only: [
      native_text_input: 1,
      start_public_endpoint!: 0,
      public_websocket_connect!: 3,
      public_websocket_send_text!: 4,
      public_websocket_receive_text!: 3
    ]

  alias CodexPooler.Access
  alias CodexPooler.Accounting.{LedgerEntry, Request}
  alias CodexPooler.Accounts.Scope
  alias CodexPooler.FakeUpstream
  alias CodexPooler.Gateway.Routing.CandidateEligibility
  alias CodexPooler.Jobs.AccountReconciliationWorker
  alias CodexPooler.Platform.OutboundHTTP
  alias CodexPooler.Repo
  alias CodexPooler.Upstreams
  alias CodexPooler.Upstreams.Auth.TokenRefresh
  alias CodexPooler.Upstreams.EndpointMetadata
  alias CodexPooler.Upstreams.Lifecycle.CredentialFencing
  alias CodexPooler.Upstreams.Quota.Windows
  alias CodexPooler.Upstreams.Reconciliation.AccountReconciliation
  alias CodexPooler.Upstreams.Schemas.UpstreamIdentity
  alias CodexPooler.Upstreams.Secrets

  defmodule Provider do
    import Plug.Conn
    def init(owner), do: owner

    def call(conn, owner) do
      send(owner, {:provider_request, conn.method, conn.request_path, conn.req_headers})

      cond do
        get_req_header(conn, "authorization") != ["Bearer test-provider-key"] ->
          conn |> put_resp_content_type("application/json") |> send_resp(401, "{}")

        conn.request_path == "/models" ->
          conn
          |> put_resp_content_type("application/json")
          |> send_resp(200, JSON.encode!(%{"data" => [%{"id" => "api-model"}]}))

        conn.request_path == "/responses" ->
          {:ok, body, conn} = read_body(conn)
          payload = JSON.decode!(body)
          send(owner, {:provider_payload, payload})

          response = %{
            "id" => "resp_api_test",
            "object" => "response",
            "status" => "completed",
            "model" => payload["model"],
            "output" => [
              %{
                "type" => "message",
                "role" => "assistant",
                "content" => [%{"type" => "output_text", "text" => "API_OK"}]
              }
            ],
            "usage" => %{
              "input_tokens" => 100,
              "input_tokens_details" => %{"cached_tokens" => 60},
              "output_tokens" => 20,
              "total_tokens" => 120
            }
          }

          if payload["stream"] do
            events = [
              %{
                "type" => "response.created",
                "response" => Map.put(response, "status", "in_progress")
              },
              %{"type" => "response.output_text.delta", "delta" => "API_OK"},
              %{"type" => "response.completed", "response" => response}
            ]

            body = Enum.map_join(events, "", &("data: " <> JSON.encode!(&1) <> "\n\n"))
            conn |> put_resp_content_type("text/event-stream") |> send_resp(200, body)
          else
            conn
            |> put_resp_content_type("application/json")
            |> send_resp(200, JSON.encode!(response))
          end

        true ->
          send_resp(conn, 404, "")
      end
    end
  end

  setup do
    %{user: user} = bootstrap_owner_fixture()
    scope = Scope.for_user(user, ["instance_owner"])
    key = active_api_key_fixture()

    server =
      start_supervised!({Bandit, plug: {Provider, self()}, port: 0, ip: {127, 0, 0, 1}, startup_log: false})

    {:ok, {_ip, port}} = ThousandIsland.listener_info(server)

    attrs = %{
      label: "External API",
      base_url: "http://127.0.0.1:#{port}",
      api_key: "test-provider-key",
      models: [
        %{
          "id" => "api-model",
          "context_window" => 128_000,
          "supports_streaming" => true,
          "supports_tools" => true,
          "supports_reasoning" => true,
          "supports_parallel_tool_calls" => true,
          "input_modalities" => ["text", "image"],
          "default_reasoning_level" => "high",
          "supported_reasoning_levels" => [
            %{"effort" => "low", "description" => "Low"},
            %{"effort" => "high", "description" => "High"}
          ]
        }
      ]
    }

    %{scope: scope, key: key, attrs: attrs}
  end

  test "imports encrypted API credentials without touching an existing Codex identity", ctx do
    old = upstream_assignment_fixture(ctx.key.pool)
    assert {:ok, result} = Upstreams.import_responses_api(ctx.scope, ctx.key.pool, ctx.attrs)
    assert UpstreamIdentity.responses_api?(result.identity)
    refute UpstreamIdentity.authenticated_codex_chatgpt?(result.identity)
    assert result.identity.chatgpt_account_id == nil

    assert {:ok, "test-provider-key"} =
             Secrets.decrypt_active_secret(result.identity, "access_token")

    refute inspect(result.identity) =~ "test-provider-key"
    assert Repo.get!(UpstreamIdentity, old.identity.id).status == "active"
    assert {:ok, %{status: :noop}} = TokenRefresh.refresh_access_token(result.identity)

    assert {:ok, url} =
             EndpointMetadata.endpoint_url(
               result.identity,
               result.assignment,
               "/backend-api/codex/responses"
             )

    assert url == ctx.attrs.base_url <> "/responses"
  end

  test "API credential discovery uses the configured forward proxy", ctx do
    CodexPooler.TestAppEnv.restore_on_exit(OutboundHTTP)
    body = JSON.encode!(%{"data" => [%{"id" => "api-model"}]})
    {:ok, proxy} = FakeUpstream.start_link({:raw_body, 200, body, [{"content-type", "application/json"}]})
    on_exit(fn -> FakeUpstream.stop(proxy) end)
    proxy_uri = URI.parse(FakeUpstream.url(proxy))

    Application.put_env(:codex_pooler, OutboundHTTP,
      proxy_config: %{
        http: [proxy: {:http, proxy_uri.host, proxy_uri.port, []}],
        https: [],
        no_proxy: []
      }
    )

    attrs = %{ctx.attrs | base_url: "http://localhost:9"}
    assert {:ok, result} = Upstreams.import_responses_api(ctx.scope, ctx.key.pool, attrs)
    assert UpstreamIdentity.responses_api?(result.identity)
    assert FakeUpstream.count(proxy) > 0
  end

  test "API credential discovery honors the proxy bypass list", ctx do
    CodexPooler.TestAppEnv.restore_on_exit(OutboundHTTP)
    {:ok, proxy} = FakeUpstream.start_link({:raw_body, 502, "", []})
    on_exit(fn -> FakeUpstream.stop(proxy) end)
    proxy_uri = URI.parse(FakeUpstream.url(proxy))

    Application.put_env(:codex_pooler, OutboundHTTP,
      proxy_config: %{
        http: [proxy: {:http, proxy_uri.host, proxy_uri.port, []}],
        https: [],
        no_proxy: ["127.0.0.1"]
      }
    )

    assert {:ok, _result} = Upstreams.import_responses_api(ctx.scope, ctx.key.pool, ctx.attrs)
    assert FakeUpstream.count(proxy) == 0
  end

  test "rejects invalid credentials or missing models before storing identities", ctx do
    before = Repo.aggregate(UpstreamIdentity, :count)

    assert {:error, _} =
             Upstreams.import_responses_api(ctx.scope, ctx.key.pool, %{
               ctx.attrs
               | api_key: "wrong"
             })

    assert {:error, _} =
             Upstreams.import_responses_api(ctx.scope, ctx.key.pool, %{
               ctx.attrs
               | models: [%{"id" => "missing", "context_window" => 8192}]
             })

    assert Repo.aggregate(UpstreamIdentity, :count) == before
    assert {:error, _} = Upstreams.import_responses_api(nil, ctx.key.pool, ctx.attrs)
  end

  test "API account reconciliation and its worker succeed without Codex quota probes", ctx do
    assert {:ok, imported} = Upstreams.import_responses_api(ctx.scope, ctx.key.pool, ctx.attrs)

    assert {:ok, result} =
             AccountReconciliation.run(ctx.key.pool.id, imported.assignment.id, "scheduled")

    assert result.status == :succeeded
    assert result.health.status == :succeeded
    assert result.quota.status == :skipped
    assert result.quota.code == "quota_not_applicable"

    assert result.quota.expected_credential_epoch ==
             CredentialFencing.credential_epoch(imported.identity)

    assert Windows.list_evidence(imported.identity) == []

    summary = Repo.reload!(imported.assignment).metadata["last_reconciliation"]
    assert summary["status"] == "succeeded"

    assert Enum.any?(summary["steps"], fn step ->
             step["status"] == "skipped" and step["code"] == "quota_not_applicable"
           end)

    assert :ok =
             AccountReconciliationWorker.perform(%Oban.Job{
               args: %{
                 "pool_id" => ctx.key.pool.id,
                 "pool_upstream_assignment_id" => imported.assignment.id,
                 "trigger_kind" => "scheduled"
               }
             })

    assert Windows.list_evidence(imported.identity) == []

    assert Repo.reload!(imported.assignment).metadata["last_reconciliation"]["status"] ==
             "succeeded"

    receive do
      {:provider_request, _method, path, _headers} when path != "/models" ->
        flunk("API reconciliation unexpectedly requested #{path}")
    after
      100 -> :ok
    end
  end

  test "API reconciliation fences a credential replacement before its terminal summary", ctx do
    assert {:ok, imported} = Upstreams.import_responses_api(ctx.scope, ctx.key.pool, ctx.attrs)

    replace_credential = fn ->
      current = Repo.reload!(imported.identity)

      current
      |> Ecto.Changeset.change(metadata: CredentialFencing.advance_credential_epoch(current))
      |> Repo.update!()

      DateTime.utc_now()
    end

    assert {:ok, result} =
             AccountReconciliation.run(ctx.key.pool.id, imported.assignment.id, "scheduled", operation_clock: replace_credential)

    assert result.quota.status == :skipped
    assert result.quota.code == "quota_refresh_superseded"
    assert Repo.reload!(imported.assignment).metadata["last_reconciliation"] == nil
    assert Windows.list_evidence(imported.identity) == []
  end

  test "account reconciliation skips a deleted API identity without contacting the provider", ctx do
    assert {:ok, imported} = Upstreams.import_responses_api(ctx.scope, ctx.key.pool, ctx.attrs)
    assert_receive {:provider_request, "GET", "/models", _headers}
    assert_receive {:provider_request, "GET", "/models", _headers}

    imported.identity |> Ecto.Changeset.change(status: "deleted") |> Repo.update!()

    assert {:ok, %{status: :skipped}} =
             AccountReconciliation.run(ctx.key.pool.id, imported.assignment.id, "scheduled")

    refute_receive {:provider_request, _method, _path, _headers}
    assert Repo.reload!(imported.identity).status == "deleted"
    assert Windows.list_evidence(imported.identity) == []
  end

  test "an existing client key can enforce the API model and receive an accounted SSE response",
       %{conn: conn} = ctx do
    assert {:ok, _} = Upstreams.import_responses_api(ctx.scope, ctx.key.pool, ctx.attrs)

    hydration =
      CandidateEligibility.hydrate_model_visibility(ctx.key.pool)

    assert Enum.any?(hydration.visible_models, &(&1.exposed_model_id == "api-model")),
           inspect(hydration)

    assert {:ok, _} =
             Access.update_api_key_with_policy(ctx.scope, ctx.key.api_key, %{
               enforced_model_identifier: "api-model",
               enforced_reasoning_effort: "high"
             })

    conn =
      conn
      |> put_req_header("authorization", ctx.key.authorization)
      |> put_req_header("x-openai-sensitive-test", "not-for-api")
      |> post("/backend-api/codex/responses", %{
        "model" => "gpt-old-client-setting",
        "input" => native_text_input("synthetic API request"),
        "max_output_tokens" => 100,
        "stream" => true
      })

    assert conn.status == 200,
           conn.resp_body <> inspect(Repo.all(from r in Request, select: r.request_metadata))

    assert conn.resp_body =~ "API_OK"
    assert_receive {:provider_payload, %{"model" => "api-model", "max_output_tokens" => 100}}
    assert_receive {:provider_request, "POST", "/responses", headers}
    refute List.keymember?(headers, "chatgpt-account-id", 0)
    refute List.keymember?(headers, "originator", 0)
    refute List.keymember?(headers, "x-openai-sensitive-test", 0)
    assert [request] = Repo.all(from r in Request, where: r.pool_id == ^ctx.key.pool.id)
    assert request.status == "succeeded"

    assert [entry] =
             Repo.all(
               from e in LedgerEntry,
                 where: e.request_id == ^request.id and e.entry_kind == "settlement"
             )

    assert entry.total_tokens == 120
    assert entry.cached_input_tokens == 60
    refute conn.resp_body =~ "test-provider-key"
  end

  test "streamed API responses support incremental continuation and fail clearly if history expires",
       ctx do
    assert {:ok, _} = Upstreams.import_responses_api(ctx.scope, ctx.key.pool, ctx.attrs)

    first =
      build_conn()
      |> put_req_header("authorization", ctx.key.authorization)
      |> post("/backend-api/codex/responses", %{
        "model" => "api-model",
        "input" => native_text_input("first turn"),
        "stream" => true
      })

    assert first.status == 200, first.resp_body
    assert_receive {:provider_payload, %{"input" => first_input}}

    next =
      build_conn()
      |> put_req_header("authorization", ctx.key.authorization)
      |> post("/backend-api/codex/responses", %{
        "model" => "api-model",
        "previous_response_id" => "resp_api_test",
        "input" => native_text_input("next turn"),
        "stream" => true
      })

    assert next.status == 200, next.resp_body
    assert_receive {:provider_payload, forwarded}
    refute Map.has_key?(forwarded, "previous_response_id")
    assert length(forwarded["input"]) == length(first_input) + 2
    assert Enum.any?(forwarded["input"], &(&1["role"] == "assistant"))

    missing =
      build_conn()
      |> put_req_header("authorization", ctx.key.authorization)
      |> post("/backend-api/codex/responses", %{
        "model" => "api-model",
        "previous_response_id" => "expired",
        "input" => native_text_input("next turn"),
        "stream" => true
      })

    assert missing.status == 400, missing.resp_body
    assert missing.resp_body =~ "previous_response_not_found"
    refute_receive {:provider_payload, _}
  end

  test "existing backend websocket clients receive API SSE responses", ctx do
    assert {:ok, _} = Upstreams.import_responses_api(ctx.scope, ctx.key.pool, ctx.attrs)

    assert {:ok, _} =
             Access.update_api_key_with_policy(ctx.scope, ctx.key.api_key, %{
               enforced_model_identifier: "api-model",
               enforced_reasoning_effort: "high"
             })

    port = start_public_endpoint!()
    {conn, websocket, ref} = public_websocket_connect!(port, ctx.key, "api-ws-test")

    try do
      frame =
        JSON.encode!(%{
          "type" => "response.create",
          "model" => "gpt-old-client-setting",
          "input" => native_text_input("websocket API request"),
          "stream" => true,
          "generate" => true
        })

      {conn, websocket} = public_websocket_send_text!(conn, websocket, ref, frame)
      assert_receive {:provider_request, "POST", "/responses", _headers}
      {conn, _websocket, events} = receive_completed(conn, websocket, ref, [])
      assert Enum.any?(events, &(&1["type"] == "response.completed")), inspect(events)
      Mint.HTTP.close(conn)
    after
      Mint.HTTP.close(conn)
    end
  end

  test "compact context survives another API turn and is scoped to the client key",
       %{conn: conn} = ctx do
    assert {:ok, _} = Upstreams.import_responses_api(ctx.scope, ctx.key.pool, ctx.attrs)

    compact =
      conn
      |> put_req_header("authorization", ctx.key.authorization)
      |> post("/backend-api/codex/responses/compact", %{
        "model" => "api-model",
        "input" => native_text_input("Remember the task objective.")
      })

    assert compact.status == 200, compact.resp_body
    body = JSON.decode!(compact.resp_body)
    assert [%{"type" => "compaction", "encrypted_content" => capsule}] = body["output"]
    assert String.starts_with?(capsule, "cp-api-compact-v1:")
    refute capsule =~ "API_OK"

    next =
      build_conn()
      |> put_req_header("authorization", ctx.key.authorization)
      |> post("/backend-api/codex/responses", %{
        "model" => "api-model",
        "input" => body["output"],
        "stream" => true
      })

    assert next.status == 200, next.resp_body

    assert_receive {:provider_payload,
                    %{
                      "input" => [
                        %{
                          "type" => "message",
                          "content" => [%{"text" => "Previous conversation summary:\nAPI_OK"}]
                        }
                      ]
                    }}

    other = active_api_key_fixture(ctx.key.pool)

    rejected =
      build_conn()
      |> put_req_header("authorization", other.authorization)
      |> post("/backend-api/codex/responses", %{
        "model" => "api-model",
        "input" => body["output"],
        "stream" => true
      })

    assert rejected.status >= 400
  end

  defp receive_completed(conn, websocket, ref, events) when length(events) < 10 do
    {conn, websocket, frame} = public_websocket_receive_text!(conn, websocket, ref)
    event = JSON.decode!(frame)

    if event["type"] in ["response.completed", "response.failed", "error"],
      do: {conn, websocket, Enum.reverse([event | events])},
      else: receive_completed(conn, websocket, ref, [event | events])
  end
end
