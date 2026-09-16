defmodule CodexPoolerWeb.Runtime.BackendCodexHttpDuplicateTurnTest do
  # The duplicate-turn fence was structurally websocket-only
  # (icoretech/codex-pooler-findings#212). A native Codex turn sent over
  # `POST /backend-api/codex/responses` reserved under a freshly generated
  # UUID, so a resend of the same turn never met `requests_correlation_id_uq`,
  # never reached the resend policy, and bought a second upstream dispatch --
  # while the same resend over a websocket was refused `409 duplicate_turn`.
  #
  # Every assertion here is taken from the real HTTP surface: the identity
  # comes from the inbound `x-codex-turn-metadata` header the released client
  # already sends, the refusal is the public status and body, and the proof of
  # "no duplicated provider work" is the fake upstream's own request count,
  # not a fixture the test wrote.
  use CodexPoolerWeb.ConnCase, async: false

  import Ecto.Query
  import CodexPoolerWeb.Runtime.BackendCodexTestSupport

  alias CodexPooler.Accounting.{Attempt, Request}
  alias CodexPooler.FakeUpstream
  alias CodexPooler.Gateway.Persistence.CodexSession
  alias CodexPooler.Repo

  @moduletag capture_log: true

  @turn_id "turn_212_native_http"
  @session_header "session-id"
  @metadata_header "x-codex-turn-metadata"

  test "an identical native HTTP resend is refused and buys no second upstream dispatch", %{
    conn: conn
  } do
    upstream = start_upstream(FakeUpstream.json_response(%{"id" => "resp_duplicate_turn"}))
    setup = gateway_setup(upstream)
    session = session_id()

    first = post_turn(conn, setup, session, @turn_id)
    assert %{"id" => "resp_duplicate_turn"} = json_response(first, 200)
    assert FakeUpstream.count(upstream) == 1

    second = post_turn(conn, setup, session, @turn_id)

    assert %{"error" => %{"code" => "duplicate_turn"}} = json_response(second, 409)

    # The whole point of the row: the provider is not paid a second time, and
    # nothing was reserved, attempted or recorded for the refused resend.
    assert FakeUpstream.count(upstream) == 1
    assert [request] = pool_requests(setup)
    assert Repo.aggregate(from(a in Attempt, where: a.request_id == ^request.id), :count) == 1

    # The durable identity is the shared turn claim, not a random UUID.
    assert String.starts_with?(request.correlation_id, "codex-request:")
  end

  # The cohort in the row is streaming: a native Codex turn resolves to
  # `http_sse`, which is exactly the transport that got a fresh UUID and a
  # second dispatch. The refusal lands before any upstream work, so the resend
  # is answered with the pre-dispatch JSON error rather than an event stream.
  test "a streaming native HTTP turn is fenced on the http_sse transport", %{conn: conn} do
    upstream = start_upstream(stream_success_sse())
    setup = gateway_setup(upstream)
    session = session_id()

    first = post_turn(conn, setup, session, @turn_id, stream: true)
    assert response(first, 200)
    assert FakeUpstream.count(upstream) == 1

    second = post_turn(conn, setup, session, @turn_id, stream: true)
    assert %{"error" => %{"code" => "duplicate_turn"}} = json_response(second, 409)

    assert FakeUpstream.count(upstream) == 1
    assert [%Request{transport: "http_sse"} = request] = pool_requests(setup)
    assert Repo.aggregate(from(a in Attempt, where: a.request_id == ^request.id), :count) == 1
  end

  # The fence must not become a hard failure for ordinary error recovery. A
  # predecessor that ended in a provider verdict the resend policy already
  # admits on websocket is admitted on HTTP too, as a recorded successor of
  # that predecessor -- one upstream dispatch per genuine attempt, not a 409
  # that strands the user's turn.
  test "a resend after a provider-terminal failure is admitted as a successor", %{conn: conn} do
    upstream = start_upstream(first_event_terminal_sse("response.failed", "server_error"))
    setup = gateway_setup(upstream)
    session = session_id()

    first = post_turn(conn, setup, session, @turn_id, stream: true)
    assert response(first, 200)

    assert [predecessor] = pool_requests(setup)
    assert predecessor.status == "failed"
    assert predecessor.last_error_code == "server_error"
    dispatched = FakeUpstream.count(upstream)

    second = post_turn(conn, setup, session, @turn_id, stream: true)
    assert response(second, 200)

    assert FakeUpstream.count(upstream) > dispatched
    assert [^predecessor, successor] = pool_requests(setup)
    assert successor.correlation_id != predecessor.correlation_id

    assert successor.request_metadata["client_resend"] == %{
             "predecessor_request_id" => predecessor.id,
             "reason" => "failed_predecessor"
           }
  end

  # The cut cohort retries up to the client's whole budget, and a transport
  # switch resets that counter, so one turn can reach double digits of
  # dispatches. One refusal is not the contract; every resend refusing is.
  test "every further resend of one turn is refused, not only the second", %{conn: conn} do
    upstream = start_upstream(FakeUpstream.json_response(%{"id" => "resp_repeated_resend"}))
    setup = gateway_setup(upstream)
    session = session_id()

    assert json_response(post_turn(conn, setup, session, @turn_id), 200)

    for _resend <- 1..5 do
      assert %{"error" => %{"code" => "duplicate_turn"}} =
               json_response(post_turn(conn, setup, session, @turn_id), 409)
    end

    assert FakeUpstream.count(upstream) == 1
    assert length(pool_requests(setup)) == 1
  end

  test "two genuinely different native HTTP turns both dispatch", %{conn: conn} do
    upstream =
      start_upstream(
        FakeUpstream.strict_sequence([
          FakeUpstream.json_response(%{"id" => "resp_turn_one"}),
          FakeUpstream.json_response(%{"id" => "resp_turn_two"})
        ])
      )

    setup = gateway_setup(upstream)
    session = session_id()

    assert %{"id" => "resp_turn_one"} =
             json_response(post_turn(conn, setup, session, "turn_one"), 200)

    assert %{"id" => "resp_turn_two"} =
             json_response(post_turn(conn, setup, session, "turn_two"), 200)

    assert FakeUpstream.count(upstream) == 2
    requests = pool_requests(setup)
    assert length(requests) == 2
    assert requests |> Enum.map(& &1.correlation_id) |> Enum.uniq() |> length() == 2
  end

  # The fence must never reach a client that does not send the metadata. These
  # two shapes are the fail-open contract: behaviour is exactly what it was
  # before the fence existed, including the second dispatch.
  test "a native HTTP request with no turn metadata keeps today's behaviour", %{conn: conn} do
    assert_unfenced(conn, fn conn, setup, session ->
      conn
      |> auth(setup)
      |> put_req_header(@session_header, session)
      |> post("/backend-api/codex/responses", turn_payload(setup))
    end)
  end

  test "a native HTTP request with malformed turn metadata keeps today's behaviour", %{
    conn: conn
  } do
    assert_unfenced(conn, fn conn, setup, session ->
      conn
      |> auth(setup)
      |> put_req_header(@session_header, session)
      |> put_req_header(@metadata_header, "not-json-metadata")
      |> post("/backend-api/codex/responses", turn_payload(setup))
    end)
  end

  # `/v1` is a translated SDK surface, not a native Codex turn, and its clients
  # own their own retry semantics. A `x-codex-` header arriving there is
  # forwarded metadata, never a turn identity, so the fence must not reach it
  # even when the header is present and well formed.
  test "a translated /v1 request carrying turn metadata is not fenced", %{conn: conn} do
    upstream =
      start_upstream(
        FakeUpstream.strict_sequence([
          FakeUpstream.json_response(%{"id" => "resp_v1_one"}),
          FakeUpstream.json_response(%{"id" => "resp_v1_two"})
        ])
      )

    setup = gateway_setup(upstream)
    session = session_id()

    post_v1 = fn ->
      conn
      |> recycle()
      |> auth(setup)
      |> put_req_header(@session_header, session)
      |> put_req_header(@metadata_header, turn_metadata(@turn_id))
      |> post("/v1/responses", turn_payload(setup))
    end

    assert json_response(post_v1.(), 200)
    assert json_response(post_v1.(), 200)

    assert FakeUpstream.count(upstream) == 2
    assert length(pool_requests(setup)) == 2
  end

  # A turn identity is scoped by the codex session, exactly as the websocket
  # claim is, so the same `turn_id` under a different session is a different
  # turn and must not be fenced.
  test "the same turn id under a different session is a different turn", %{conn: conn} do
    upstream =
      start_upstream(
        FakeUpstream.strict_sequence([
          FakeUpstream.json_response(%{"id" => "resp_session_one"}),
          FakeUpstream.json_response(%{"id" => "resp_session_two"})
        ])
      )

    setup = gateway_setup(upstream)

    assert json_response(post_turn(conn, setup, session_id(), @turn_id), 200)
    assert json_response(post_turn(conn, setup, session_id(), @turn_id), 200)

    assert FakeUpstream.count(upstream) == 2
    assert length(pool_requests(setup)) == 2
    assert Repo.aggregate(CodexSession, :count) == 2
  end

  defp assert_unfenced(conn, request) do
    upstream =
      start_upstream(
        FakeUpstream.strict_sequence([
          FakeUpstream.json_response(%{"id" => "resp_unfenced_one"}),
          FakeUpstream.json_response(%{"id" => "resp_unfenced_two"})
        ])
      )

    setup = gateway_setup(upstream)
    session = session_id()

    assert json_response(request.(recycle(conn), setup, session), 200)
    assert json_response(request.(recycle(conn), setup, session), 200)

    assert FakeUpstream.count(upstream) == 2
    requests = pool_requests(setup)
    assert length(requests) == 2

    for %Request{correlation_id: correlation_id} <- requests do
      assert {:ok, _uuid} = Ecto.UUID.cast(correlation_id)
    end
  end

  defp post_turn(conn, setup, session, turn_id, opts \\ []) do
    conn
    |> recycle()
    |> auth(setup)
    |> put_req_header(@session_header, session)
    |> put_req_header(@metadata_header, turn_metadata(turn_id))
    |> post("/backend-api/codex/responses", turn_payload(setup, opts))
  end

  defp turn_payload(setup, opts \\ []) do
    payload = %{
      "model" => setup.model.exposed_model_id,
      "input" => native_text_input("duplicate turn fence")
    }

    if Keyword.get(opts, :stream, false), do: Map.put(payload, "stream", true), else: payload
  end

  defp turn_metadata(turn_id),
    do: CodexPooler.JSON.encode!(%{"turn_id" => turn_id, "request_kind" => "turn"})

  defp pool_requests(setup) do
    Repo.all(from(r in Request, where: r.pool_id == ^setup.pool.id, order_by: r.admitted_at))
  end

  defp session_id, do: "codex-session-" <> unique_suffix()
end
