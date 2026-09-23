defmodule CodexPoolerWeb.V1.UpstreamRequestIdPersistenceTest do
  use CodexPoolerWeb.ConnCase, async: false

  import Ecto.Query

  import CodexPoolerWeb.Runtime.BackendCodexTestSupport,
    only: [auth: 2, gateway_setup: 1, start_upstream: 1]

  alias CodexPooler.Accounting.{Attempt, Request}
  alias CodexPooler.FakeUpstream
  alias CodexPooler.Repo

  # End to end: the provider's `x-oai-request-id` response header reaches the
  # persisted attempt row. Unit tests of the writer cannot prove the field is
  # populated by the application (this field was NULL on every production
  # attempt while the writer read other names).
  test "a provider x-oai-request-id is persisted on the attempt", %{conn: conn} do
    upstream =
      start_upstream(
        FakeUpstream.json_response_with_headers(
          %{
            "id" => "resp_synthetic_e2e",
            "object" => "response",
            "status" => "completed",
            "output" => [],
            "usage" => %{"input_tokens" => 1, "output_tokens" => 1, "total_tokens" => 2}
          },
          [{"x-oai-request-id", "req_synthetic_e2e"}]
        )
      )

    setup = gateway_setup(upstream)

    response =
      conn
      |> auth(setup)
      |> post("/v1/responses", %{
        "model" => setup.model.exposed_model_id,
        "input" => "synthetic request id persistence"
      })

    assert response.status == 200
    assert [request] = Repo.all(from(r in Request, where: r.pool_id == ^setup.pool.id))
    assert [attempt] = Repo.all(from(a in Attempt, where: a.request_id == ^request.id))
    assert attempt.response_metadata["upstream_request_id"] == "req_synthetic_e2e"
  end
end
