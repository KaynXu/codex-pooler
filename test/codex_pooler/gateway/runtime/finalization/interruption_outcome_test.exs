defmodule CodexPooler.Gateway.Runtime.Finalization.InterruptionOutcomeTest do
  use ExUnit.Case, async: true

  alias CodexPooler.Gateway.ErrorClassification
  alias CodexPooler.Gateway.Runtime.Finalization.InterruptionOutcome
  alias CodexPooler.Gateway.Transports.Websocket.OwnerErrorVocabulary

  # The interrupted family is the loss of the party carrying the turn: the
  # client, or the Pooler-side owner (drained, lease lost, crashed). A crashed
  # owner used to be the odd one out (findings#228).
  test "the interrupted outcome names the client loss and every owner loss" do
    assert InterruptionOutcome.interrupted_error_codes() == [
             "client_disconnected",
             "owner_drained",
             "owner_unavailable",
             "owner_crashed"
           ]

    for code <- InterruptionOutcome.interrupted_error_codes() do
      assert InterruptionOutcome.interrupted_error_code?(code)
      assert InterruptionOutcome.interrupted_error_code?(String.to_atom(code))
      assert InterruptionOutcome.outcome_for_code(code) == "interrupted"
    end
  end

  # Forwarding refusals and superseded downstreams answer the client with a
  # status it can act on; nothing was cut mid-flight, so they stay failed.
  test "every other owner and client error code is a failed outcome" do
    refusals =
      OwnerErrorVocabulary.owner_error_codes() --
        InterruptionOutcome.interrupted_error_codes()

    assert refusals != []

    for code <- refusals do
      refute InterruptionOutcome.interrupted_error_code?(code)
      assert InterruptionOutcome.outcome_for_code(code) == "failed"
    end

    for code <- ErrorClassification.client_error_codes() -- ["client_disconnected"] do
      assert InterruptionOutcome.outcome_for_code(code) == "failed"
    end

    assert InterruptionOutcome.outcome_for_code("stream_idle_timeout") == "failed"
    assert InterruptionOutcome.outcome_for_code(nil) == "failed"
    assert InterruptionOutcome.outcome_for_code(499) == "failed"
  end

  # Every interrupted code is an owner-vocabulary code or the client loss, so
  # a new owner code cannot land in the interrupted family unnoticed.
  test "the interrupted family is drawn from the owner vocabulary plus client_disconnected" do
    for code <- InterruptionOutcome.interrupted_error_codes() do
      assert code == "client_disconnected" or code in OwnerErrorVocabulary.owner_error_codes()
    end
  end
end
