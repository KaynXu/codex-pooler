defmodule CodexPooler.Accounting.QuotaRejectionResendTest do
  use CodexPooler.DataCase, async: false

  import CodexPooler.AccountingTestSupport
  import CodexPooler.PoolerFixtures, only: [attempt_fixture: 3]

  alias CodexPooler.Accounting
  alias CodexPooler.Accounting.{ClientRetry, Request, RequestClientRetryLink}
  alias CodexPooler.Gateway.Persistence.{CodexSession, CodexTurn}
  alias CodexPooler.Repo

  @endpoint "/backend-api/codex/responses"

  for code <- ["usage_limit_reached", "usage_limit_exceeded"] do
    test "#{code} admits one semantic direct resend with the exact sealed witness" do
      %{setup: setup, opts: opts, request: predecessor} = seed = seed(unquote(code))

      assert {:ok, %{request: successor}} =
               Accounting.claim_websocket_turn(setup.auth, setup.model, opts)

      assert successor.id != predecessor.id

      assert successor.request_metadata["client_resend"]["predecessor_request_id"] ==
               predecessor.id

      assert {:error, %{code: :duplicate_request}} =
               Accounting.claim_websocket_turn(setup.auth, setup.model, opts)

      assert Repo.aggregate(Request, :count) == 2
      assert Repo.aggregate(RequestClientRetryLink, :count) == 1
      assert {:error, _reason} = claim_owner(seed)
    end

    test "#{code} admits one owner successor and refuses a second" do
      seed = seed(unquote(code))

      assert {:ok, %ClientRetry.SuccessorClaim{request: successor}} = claim_owner(seed)
      assert successor.id != seed.request.id
      assert {:error, _reason} = claim_owner(seed)
      assert Repo.aggregate(Request, :count) == 2
      assert Repo.aggregate(RequestClientRetryLink, :count) == 1

      assert {:error, %{code: :duplicate_request}} =
               Accounting.claim_websocket_turn(seed.setup.auth, seed.setup.model, seed.opts)
    end
  end

  for mutation <- [
        :missing_marker,
        :false_marker,
        :visible_output,
        :different_code,
        :replay_generation,
        :newer_attempt,
        :changed_witness,
        :changed_epoch,
        :expired,
        :anchor
      ] do
    test "#{mutation} keeps direct and owner quota resend fenced" do
      seed = seed("usage_limit_reached") |> mutate(unquote(mutation))
      counts = counts()

      assert {:error, %{code: :duplicate_request}} =
               Accounting.claim_websocket_turn(seed.setup.auth, seed.setup.model, seed.opts)

      assert {:error, _reason} = claim_owner(seed)
      assert counts() == counts
    end
  end

  defp seed(code) do
    setup = accounting_setup()
    %{rows: [[now]]} = Repo.query!("SELECT clock_timestamp()", [])
    digest = :crypto.strong_rand_bytes(32)
    semantic_digest = :crypto.strong_rand_bytes(32)
    witness = ClientRetry.original_witness!(digest, setup.api_key.runtime_revocation_epoch)

    session =
      Repo.insert!(%CodexSession{
        pool_id: setup.pool.id,
        api_key_id: setup.api_key.id,
        session_key: "quota-resend-#{System.unique_integer([:positive])}",
        status: "active",
        created_at: now,
        updated_at: now
      })

    opts = %{
      endpoint: @endpoint,
      correlation_id: "codex-turn:" <> Base.url_encode64(semantic_digest, padding: false),
      codex_session: session,
      native_client_retry_witness: witness
    }

    assert {:ok, %{request: request}} =
             Accounting.claim_websocket_turn(setup.auth, setup.model, opts)

    attempt =
      attempt_fixture(request, setup.assignment, %{
        status: "failed",
        network_error_code: code,
        transport: "websocket",
        usage_status: "usage_unknown",
        completed_at: now,
        response_metadata: %{"quota_rejection_before_output" => true}
      })

    request =
      request
      |> Ecto.Changeset.change(
        status: "failed",
        last_error_code: code,
        usage_status: "usage_unknown",
        completed_at: now
      )
      |> Repo.update!()

    turn =
      Repo.insert!(%CodexTurn{
        codex_session_id: session.id,
        request_id: request.id,
        turn_sequence: 1,
        semantic_turn_digest: semantic_digest,
        transport_kind: "websocket",
        status: "failed",
        error_code: code,
        final_attempt_id: attempt.id,
        started_at: now,
        completed_at: now,
        created_at: now,
        updated_at: now
      })

    owner_opts = %{
      endpoint: @endpoint,
      requested_model: setup.model.exposed_model_id,
      runtime_revocation_epoch: setup.api_key.runtime_revocation_epoch,
      codex_session: session,
      semantic_turn_digest: semantic_digest,
      replay_claim_digest: digest,
      reservation_estimate: %{
        input_tokens: 0,
        cached_input_tokens: 0,
        output_tokens: 0,
        reasoning_tokens: 0,
        total_tokens: 0,
        estimated_cost_micros: Decimal.new(0),
        strategy: "exact"
      }
    }

    %{
      setup: setup,
      opts: opts,
      owner_opts: owner_opts,
      request: request,
      attempt: attempt,
      turn: turn
    }
  end

  defp claim_owner(seed) do
    Accounting.claim_client_retry_successor(
      seed.setup.auth,
      seed.setup.model,
      %{"model" => seed.setup.model.exposed_model_id, "input" => []},
      seed.owner_opts
    )
  end

  defp mutate(seed, :missing_marker), do: update_row(seed, :attempt, response_metadata: %{})

  defp mutate(seed, :false_marker),
    do: update_row(seed, :attempt, response_metadata: %{"quota_rejection_before_output" => false})

  defp mutate(seed, :visible_output),
    do: update_row(seed, :turn, first_visible_output_at: seed.turn.completed_at)

  defp mutate(seed, :different_code),
    do: update_row(seed, :attempt, network_error_code: "server_error")

  defp mutate(seed, :replay_generation), do: update_row(seed, :attempt, replay_generation: 1)

  defp mutate(seed, :newer_attempt) do
    attempt_fixture(seed.request, seed.setup.assignment, %{
      attempt_number: seed.attempt.attempt_number + 1,
      status: "failed",
      network_error_code: "server_error",
      transport: "websocket",
      completed_at: seed.turn.completed_at
    })

    seed
  end

  defp mutate(seed, :changed_witness) do
    digest = :crypto.strong_rand_bytes(32)
    witness = ClientRetry.original_witness!(digest, seed.setup.api_key.runtime_revocation_epoch)

    %{
      seed
      | opts: Map.put(seed.opts, :native_client_retry_witness, witness),
        owner_opts: Map.put(seed.owner_opts, :replay_claim_digest, digest)
    }
  end

  defp mutate(seed, :changed_epoch) do
    epoch = seed.setup.api_key.runtime_revocation_epoch + 1
    witness = ClientRetry.original_witness!(seed.owner_opts.replay_claim_digest, epoch)

    %{
      seed
      | opts: Map.put(seed.opts, :native_client_retry_witness, witness),
        owner_opts: Map.put(seed.owner_opts, :runtime_revocation_epoch, epoch)
    }
  end

  defp mutate(seed, :expired),
    do: update_row(seed, :request, completed_at: DateTime.add(seed.request.completed_at, -31, :second))

  defp mutate(seed, :anchor) do
    %{
      seed
      | opts: Map.put(seed.opts, :anchor_present?, true),
        owner_opts: Map.put(seed.owner_opts, :anchor_present?, true)
    }
  end

  defp update_row(seed, key, attrs) do
    row = seed |> Map.fetch!(key) |> Ecto.Changeset.change(attrs) |> Repo.update!()
    Map.put(seed, key, row)
  end

  defp counts do
    {Repo.aggregate(Request, :count), Repo.aggregate(CodexTurn, :count), Repo.aggregate(RequestClientRetryLink, :count), Repo.aggregate(Accounting.LedgerEntry, :count)}
  end
end
