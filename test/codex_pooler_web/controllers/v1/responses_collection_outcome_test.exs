defmodule CodexPoolerWeb.V1.ResponsesCollectionOutcomeTest do
  use CodexPoolerWeb.ConnCase, async: false

  import Ecto.Query

  import CodexPoolerWeb.Runtime.BackendCodexTestSupport,
    only: [auth: 2, gateway_setup: 1, start_upstream: 1]

  alias CodexPooler.Accounting.{Attempt, LedgerEntry, Request, RequestLogs}
  alias CodexPooler.FakeUpstream
  alias CodexPooler.Gateway.Persistence.RoutingCircuitState
  alias CodexPooler.Repo

  for transport <- [:json, :sse] do
    @transport transport
    test "#{transport} null response usage cannot borrow nested counters", %{conn: conn} do
      payload = %{
        "id" => "resp_synthetic_null_usage",
        "object" => "response",
        "status" => "completed",
        "usage" => nil,
        "output" => [
          %{
            "type" => "message",
            "content" => [%{"type" => "output_text", "text" => "synthetic output"}],
            "usage" => %{"input_tokens" => 100, "output_tokens" => 50, "total_tokens" => 150}
          }
        ]
      }

      upstream_response =
        case @transport do
          :json ->
            FakeUpstream.json_response(payload)

          :sse ->
            FakeUpstream.sse_stream([
              {"response.completed", %{"type" => "response.completed", "response" => payload}}
            ])
        end

      upstream = start_upstream(upstream_response)
      setup = gateway_setup(upstream)

      response =
        conn
        |> auth(setup)
        |> post("/v1/responses", %{
          "model" => setup.model.exposed_model_id,
          "input" => "synthetic request",
          "stream" => false
        })

      assert %{"object" => "response"} = json_response(response, 200)
      assert [request] = Repo.all(Request)
      assert request.status == "succeeded"
      assert request.usage_status == "usage_unknown"
      assert [attempt] = Repo.all(Attempt)
      assert attempt.usage_status == "usage_unknown"
      assert [settlement] = Repo.all(from(e in LedgerEntry, where: e.entry_kind == "settlement"))

      assert [reservation] =
               Repo.all(from(e in LedgerEntry, where: e.entry_kind == "reservation"))

      assert settlement.usage_status == "usage_unknown"
      assert settlement.input_tokens == reservation.input_tokens
      assert settlement.output_tokens == reservation.output_tokens
      assert settlement.total_tokens == reservation.total_tokens

      refute {settlement.input_tokens, settlement.output_tokens, settlement.total_tokens} ==
               {100, 50, 150}

      assert %{items: [log]} = RequestLogs.list(setup.pool, filters: %{request_id: request.id})
      assert log.usage_status == "usage_unknown"
      assert is_nil(log.token_counts.input_tokens)
      assert is_nil(log.token_counts.output_tokens)
      assert is_nil(log.token_counts.total_tokens)
      assert log.cost.status == "unpriced"
      assert is_nil(log.cost.usd)
    end
  end

  test "non-streaming response without a response envelope fails before successful settlement", %{
    conn: conn
  } do
    upstream =
      start_upstream(
        FakeUpstream.sse_stream([
          {"response.output_text.delta", %{"type" => "response.output_text.delta", "delta" => "synthetic output"}}
        ])
      )

    setup = gateway_setup(upstream)

    response =
      conn
      |> auth(setup)
      |> post("/v1/responses", %{
        "model" => setup.model.exposed_model_id,
        "input" => "synthetic request",
        "stream" => false
      })

    assert %{"error" => %{"code" => "upstream_response_missing"}} = json_response(response, 502)
    assert [request] = Repo.all(Request)
    assert request.status == "failed"
    assert request.response_status_code == 502
    assert request.last_error_code == "upstream_response_missing"
    assert [attempt] = Repo.all(Attempt)
    assert attempt.status == "failed"
    assert attempt.upstream_status_code == 200
    assert attempt.response_metadata["status_code"] == 200
    assert attempt.network_error_code == "upstream_response_missing"
    assert FakeUpstream.count(upstream) == 1

    assert Repo.aggregate(from(e in LedgerEntry, where: e.entry_kind == "settlement"), :count) ==
             1

    refute Repo.exists?(from(c in RoutingCircuitState, where: c.failure_count > 0))
  end
end
