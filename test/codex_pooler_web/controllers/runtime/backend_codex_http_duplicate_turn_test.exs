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
  alias CodexPooler.CompatibilityMatrix
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

    # A turn's opening request is named by the turn alone, exactly as the
    # websocket path names it, so the claim survives a rebuilt retry body.
    assert String.starts_with?(request.correlation_id, "codex-turn:")
  end

  # `409 duplicate_turn` is a public response on a runtime route, so the
  # machine-readable route/feature contract has to carry it for both transports
  # and has to be answerable from the route itself rather than from prose
  # (findings#212). The grown-body gap the entry records is proven behaviourally
  # by "a tool continuation resent with a grown body is NOT fenced" below.
  test "the compatibility matrix duplicate-turn entry is the refusal this route produces", %{
    conn: conn
  } do
    feature = CompatibilityMatrix.by_slug!(:duplicate_turn_fence)
    fixture = CompatibilityMatrix.fixture!(:duplicate_turn_fence)

    assert %{method: :post, path: "/backend-api/codex/responses"} in feature.routes

    assert %{method: :get, path: "/backend-api/codex/responses", transport: "websocket"} in feature.routes

    assert "websocket" in feature.duplicate_turn.public_error.transports
    assert "http_sse" in feature.duplicate_turn.public_error.transports

    upstream = start_upstream(FakeUpstream.json_response(%{"id" => "resp_matrix_turn"}))
    setup = gateway_setup(upstream)
    session = session_id()

    assert json_response(post_turn(conn, setup, session, @turn_id), 200)

    %{status: status, code: code} = feature.duplicate_turn.public_error

    assert %{"error" => %{"code" => ^code}} =
             json_response(post_turn(conn, setup, session, @turn_id), status)

    assert [request] = pool_requests(setup)
    assert String.starts_with?(request.correlation_id, fixture.claim_prefixes.turn)
    assert FakeUpstream.count(upstream) == 1
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

  # The fence must never become a hard failure for ordinary error recovery. It
  # refuses only a resend whose predecessor already delivered provider output
  # for the turn; a predecessor that was refused before producing anything has
  # no spend to protect, so the retry is served exactly as it is today. A
  # first-event `server_error` is that shape: the turn is marked visible because
  # the error event itself is written downstream, but no model output was ever
  # produced.
  test "a resend after a zero-output provider failure is served, not refused", %{conn: conn} do
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

    # Falling open steps OVER the zero-output predecessor rather than abandoning
    # the turn: the successor is still named by this turn, through the same
    # deterministic derivation the websocket resend chain uses. A fresh UUID
    # here would switch the fence off for this turn permanently.
    assert String.starts_with?(successor.correlation_id, "codex-request-retry:")
  end

  # The chain is what makes falling open safe. A turn whose first attempt bought
  # nothing is served; if a LATER attempt of that same turn delivers output and
  # is then resent, the fence must still be there to refuse it. With a fresh
  # UUID on fall-open it would not be.
  test "a turn served past a zero-output failure is still fenced once it delivers output", %{
    conn: conn
  } do
    upstream =
      start_upstream(
        FakeUpstream.strict_sequence([
          first_event_terminal_sse("response.failed", "server_error"),
          stream_success_sse()
        ])
      )

    setup = gateway_setup(upstream)
    session = session_id()

    # Attempt 1 buys nothing and is served.
    assert response(post_turn(conn, setup, session, @turn_id, stream: true), 200)
    # Attempt 2 is served past it, and this one really does deliver output.
    assert response(post_turn(conn, setup, session, @turn_id, stream: true), 200)

    assert [zero_output, delivered] = pool_requests(setup)
    assert zero_output.last_error_code == "server_error"
    assert delivered.status == "succeeded"
    dispatched = FakeUpstream.count(upstream)

    # Attempt 3 would be the second dispatch of work already delivered.
    assert %{"error" => %{"code" => "duplicate_turn"}} =
             json_response(post_turn(conn, setup, session, @turn_id, stream: true), 409)

    assert FakeUpstream.count(upstream) == dispatched
    assert length(pool_requests(setup)) == 2
  end

  # A predecessor left live by a killed node keeps `completed_at` null until the
  # `*/15` `runtime_cleanup` cron finalizes it. Refusing every retry in that
  # window would be a terminal error on the default transport with no duplicate
  # spend to prevent, so an unfinished predecessor falls open.
  test "a retry while the predecessor is still unfinished is served", %{conn: conn} do
    upstream =
      start_upstream(
        FakeUpstream.strict_sequence([
          FakeUpstream.json_response(%{"id" => "resp_live_one"}),
          FakeUpstream.json_response(%{"id" => "resp_live_two"})
        ])
      )

    setup = gateway_setup(upstream)
    session = session_id()

    assert json_response(post_turn(conn, setup, session, @turn_id), 200)

    # Strand the predecessor exactly as a killed node leaves it: accepted work,
    # no completion, nothing for `runtime_cleanup` to have swept yet.
    [predecessor] = pool_requests(setup)

    {1, _} =
      Repo.update_all(
        from(r in Request, where: r.id == ^predecessor.id),
        set: [status: "in_progress", completed_at: nil]
      )

    assert json_response(post_turn(conn, setup, session, @turn_id), 200)
    assert FakeUpstream.count(upstream) == 2
    assert length(pool_requests(setup)) == 2
  end

  # The released client sends the canonical turn metadata in the request body's
  # `client_metadata` (`codex-rs/core/src/client.rs:893`); the header is a
  # bounded copy. A client that sends only the body must still be fenced.
  test "the real client body shape is fenced without any turn metadata header", %{conn: conn} do
    upstream = start_upstream(FakeUpstream.json_response(%{"id" => "resp_body_metadata"}))
    setup = gateway_setup(upstream)
    session = session_id()

    body = %{
      "model" => setup.model.exposed_model_id,
      "input" => native_text_input("real client body shape"),
      "client_metadata" => %{
        "session_id" => "client-session",
        "thread_id" => "client-thread",
        "x-codex-window-id" => "client-window",
        "turn_id" => @turn_id,
        "x-codex-turn-metadata" => turn_metadata(@turn_id)
      }
    }

    post_body = fn ->
      conn
      |> recycle()
      |> auth(setup)
      |> put_req_header(@session_header, session)
      |> post("/backend-api/codex/responses", body)
    end

    assert json_response(post_body.(), 200)
    assert %{"error" => %{"code" => "duplicate_turn"}} = json_response(post_body.(), 409)

    assert FakeUpstream.count(upstream) == 1
    assert [request] = pool_requests(setup)
    assert String.starts_with?(request.correlation_id, "codex-turn:")
  end

  # A compaction request is built from the SAME `turn_metadata_state` as the turn
  # it compacts (`session.rs:686-701`, `turn_metadata.rs:169`), so both carry one
  # `turn_id`, and the bare claim encodes no endpoint. A compaction that reached
  # the turn arm would be refused as a duplicate of the turn it is compacting --
  # breaking every native HTTP turn that triggers a remote compaction, which is
  # strictly worse than the double spend the fence exists to stop.
  test "a turn and its own compaction are different requests, in both orders", %{conn: conn} do
    for {first, second} <- [{:turn, :compaction}, {:compaction, :turn}] do
      upstream =
        start_upstream(
          FakeUpstream.strict_sequence([
            FakeUpstream.json_response(%{"id" => "resp_pair_one"}),
            FakeUpstream.json_response(%{"id" => "resp_pair_two"})
          ])
        )

      setup = gateway_setup(upstream, compact?: true)
      session = session_id()

      assert json_response(post_kind(conn, setup, session, first), 200)
      assert json_response(post_kind(conn, setup, session, second), 200)

      assert FakeUpstream.count(upstream) == 2
      requests = pool_requests(setup)
      assert length(requests) == 2
      assert requests |> Enum.map(& &1.correlation_id) |> Enum.uniq() |> length() == 2
    end
  end

  # The compaction arm is a different claim, not an absent one: a compaction
  # resent identically is still fenced, under its own HMAC domain.
  test "an identical compaction resend is refused", %{conn: conn} do
    upstream = start_upstream(FakeUpstream.json_response(%{"id" => "resp_compaction"}))
    setup = gateway_setup(upstream, compact?: true)
    session = session_id()

    assert json_response(post_kind(conn, setup, session, :compaction), 200)

    assert %{"error" => %{"code" => "duplicate_turn"}} =
             json_response(post_kind(conn, setup, session, :compaction), 409)

    assert FakeUpstream.count(upstream) == 1
    assert [request] = pool_requests(setup)
    assert String.starts_with?(request.correlation_id, "codex-request:")
  end

  # Only an explicit `turn` kind reaches the bare claim. A request kind that is
  # about a turn rather than being one -- and a document that omits the field --
  # is left unfenced rather than guessed at.
  test "a prewarm sharing the turn id is not fenced against the turn", %{conn: conn} do
    upstream =
      start_upstream(
        FakeUpstream.strict_sequence([
          FakeUpstream.json_response(%{"id" => "resp_prewarm"}),
          FakeUpstream.json_response(%{"id" => "resp_turn_after_prewarm"})
        ])
      )

    setup = gateway_setup(upstream)
    session = session_id()

    prewarm =
      conn
      |> recycle()
      |> auth(setup)
      |> put_req_header(@session_header, session)
      |> put_req_header(
        @metadata_header,
        CodexPooler.JSON.encode!(%{"turn_id" => @turn_id, "request_kind" => "prewarm"})
      )
      |> post("/backend-api/codex/responses", turn_payload(setup))

    assert json_response(prewarm, 200)
    assert json_response(post_turn(conn, setup, session, @turn_id), 200)

    assert FakeUpstream.count(upstream) == 2
    assert length(pool_requests(setup)) == 2
  end

  # KNOWN MISS, documented deliberately. A tool-result continuation inside a
  # turn must be named by its payload, or the several requests of one turn would
  # collide with each other -- so a continuation whose retry body has grown is
  # NOT fenced. The websocket path has exactly the same miss for exactly the
  # same reason (`websocket_codec.ex:938-965`); this test pins the boundary so
  # it cannot change silently.
  test "a tool continuation resent with a grown body is NOT fenced (known miss)", %{conn: conn} do
    upstream =
      start_upstream(
        FakeUpstream.strict_sequence([
          FakeUpstream.json_response(%{"id" => "resp_continuation_one"}),
          FakeUpstream.json_response(%{"id" => "resp_continuation_two"})
        ])
      )

    setup = gateway_setup(upstream)
    session = session_id()

    post_continuation = fn input ->
      conn
      |> recycle()
      |> auth(setup)
      |> put_req_header(@session_header, session)
      |> post("/backend-api/codex/responses", %{
        "model" => setup.model.exposed_model_id,
        "input" => input,
        "previous_response_id" => "resp_continuation_anchor",
        "client_metadata" => %{"x-codex-turn-metadata" => turn_metadata(@turn_id)}
      })
    end

    tool_output = [
      %{
        "type" => "function_call_output",
        "call_id" => "call_212_continuation",
        "output" => "tool result"
      }
    ]

    assert json_response(post_continuation.(tool_output), 200)

    grown =
      tool_output ++
        [
          %{
            "type" => "message",
            "role" => "assistant",
            "content" => [%{"type" => "output_text", "text" => "delivered before the cut"}]
          }
        ]

    assert json_response(post_continuation.(grown), 200)

    # Two dispatches: the grown body is a different payload-scoped claim. This
    # is the residual, not a regression -- before the fence existed both of
    # these dispatched too.
    assert FakeUpstream.count(upstream) == 2
    requests = pool_requests(setup)
    assert length(requests) == 2

    for %Request{correlation_id: correlation_id} <- requests do
      assert String.starts_with?(correlation_id, "codex-request:")
    end
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

  # A brand-new path must not log under the old path's name. Triage greps for
  # "websocket replay rejection" and for `transport=websocket`; a native HTTP
  # refusal that claimed either would send an operator looking for a websocket
  # session that never existed.
  @tag capture_log: false
  test "a native HTTP refusal is logged as native http, not as websocket", %{conn: conn} do
    upstream = start_upstream(FakeUpstream.json_response(%{"id" => "resp_log_label"}))
    setup = gateway_setup(upstream)
    session = session_id()

    assert json_response(post_turn(conn, setup, session, @turn_id), 200)

    previous_level = Logger.level()
    Logger.configure(level: :info)
    on_exit(fn -> Logger.configure(level: previous_level) end)

    logs =
      ExUnit.CaptureLog.capture_log([level: :info], fn ->
        assert %{"error" => %{"code" => "duplicate_turn"}} =
                 json_response(post_turn(conn, setup, session, @turn_id), 409)
      end)

    assert logs =~ "native http replay rejection"
    assert logs =~ "stage=native_http_turn_claim"
    assert logs =~ "transport=http_json"
    refute logs =~ "websocket replay rejection"
    refute logs =~ "transport=websocket"
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

  defp post_kind(conn, setup, session, :turn),
    do: post_turn(conn, setup, session, @turn_id)

  defp post_kind(conn, setup, session, :compaction) do
    conn
    |> recycle()
    |> auth(setup)
    |> put_req_header(@session_header, session)
    |> put_req_header(
      @metadata_header,
      CodexPooler.JSON.encode!(%{
        "turn_id" => @turn_id,
        "request_kind" => "compaction",
        "window_id" => "compaction-window",
        "context_window_id" => Ecto.UUID.generate()
      })
    )
    |> post("/backend-api/codex/responses/compact", turn_payload(setup))
  end

  defp turn_metadata(turn_id),
    do: CodexPooler.JSON.encode!(%{"turn_id" => turn_id, "request_kind" => "turn"})

  defp pool_requests(setup) do
    Repo.all(from(r in Request, where: r.pool_id == ^setup.pool.id, order_by: r.admitted_at))
  end

  defp session_id, do: "codex-session-" <> unique_suffix()
end
