defmodule CodexPooler.Accounting.TaskExceptionArmedReplayTest do
  use CodexPooler.DataCase, async: false

  import CodexPooler.RequestReplayFixtures

  alias CodexPooler.Accounting
  alias CodexPooler.Accounting.{Attempt, LedgerEntry, PreAttemptRelease, Request}
  alias CodexPooler.Accounting.{RequestReplay, RequestReplayEntitlement}
  alias CodexPooler.Gateway.Persistence.CodexTurn
  alias CodexPooler.Gateway.Runtime.Finalization.Interruption
  alias CodexPooler.Repo

  @reason "owner_task_exception"

  # An armed replay entitlement leaves the predecessor attempt `retryable_failed`
  # at generation 0 with the reservation live and the entitlement at generation
  # 1. A task exception on that request reaches the interruption path's
  # reservation-outstanding arm, which used to finalize without a close status:
  # the finalizer then refused the stale generation, wrote nothing, and the
  # reservation leaked (findings#221).
  test "a task exception on an armed replay closes the entitlement and releases the reservation" do
    fixture = replay_fixture(owner?: true, reservation?: true)
    assert {:ok, armed} = RequestReplay.arm(arm_input(fixture))
    assert armed.replay_generation == 1

    attempt = Repo.reload!(fixture.attempt)
    assert %Attempt{status: "retryable_failed", replay_generation: 0} = attempt
    assert Repo.reload!(fixture.request).status == "in_progress"
    assert Accounting.reservation_outstanding?(fixture.request)

    receipt = %{
      session_id: fixture.session.id,
      request_id: fixture.request.id,
      correlation_id: fixture.request.correlation_id,
      api_key_id: fixture.api_key.id,
      owner_binding: nil,
      attempt_id: attempt.id,
      replay_generation: attempt.replay_generation
    }

    assert :ok = Interruption.finalize_task_exception_request(receipt, @reason)

    assert %Request{status: "failed", last_error_code: @reason, completed_at: %DateTime{}} =
             Repo.reload!(fixture.request)

    refute Accounting.reservation_outstanding?(fixture.request)

    # The 221-07 shape: the reservation is settled and released once, both
    # rows name the terminal attempt, and no pre-attempt phase is claimed.
    entries =
      Repo.all(
        from e in LedgerEntry, where: e.request_id == ^fixture.request.id, order_by: e.entry_kind
      )

    assert Enum.map(entries, & &1.entry_kind) == ["release", "reservation", "settlement"]
    release = Enum.find(entries, &(&1.entry_kind == "release"))
    settlement = Enum.find(entries, &(&1.entry_kind == "settlement"))
    assert release.attempt_id == attempt.id
    assert settlement.attempt_id == attempt.id
    refute Map.has_key?(release.details || %{}, PreAttemptRelease.detail_key())

    assert %RequestReplayEntitlement{status: "revoked", closed_at: %DateTime{}} =
             Repo.get!(RequestReplayEntitlement, armed.entitlement_id)

    assert %CodexTurn{status: "failed", error_code: @reason} = Repo.reload!(fixture.turn)

    # The predecessor attempt is preserved as the replay's evidence, not rewritten.
    assert Repo.reload!(attempt) == attempt

    # Idempotent: the second finalization finds nothing to do.
    request_after = Repo.reload!(fixture.request)
    assert :ok = Interruption.finalize_task_exception_request(receipt, @reason)
    assert Repo.reload!(fixture.request) == request_after
  end
end
