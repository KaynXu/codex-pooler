defmodule CodexPooler.Telemetry.AfterCommitEmissionAuditTest do
  # A telemetry sample that describes a database write must not be emitted
  # before that write commits, or a rollback leaves a counter describing
  # something that never happened. Several emitters therefore ask
  # `Repo.in_transaction?/0` first, and the answer decides whether they emit,
  # hand the markers back, or drop them.
  #
  # findings#195 asked for an audit of every caller of that shape. Prose could
  # not keep it: the first audit found two sites of three, and the third
  # (`Interruption.emit_committed_recovery_outcomes/1`) stayed unexamined
  # because nothing enumerated it. This file is that audit as a test. It reads
  # the application source, attributes every `Repo.in_transaction?/0` call to
  # the function containing it, and requires the result to equal a list written
  # out by hand. A new call site, a moved one or a deleted one fails here, so
  # the next emitter of this shape is looked at rather than assumed.
  use ExUnit.Case, async: true

  @lib Path.expand("../../../lib", __DIR__)

  # Every `Repo.in_transaction?/0` call in the application, as `path => sorted
  # enclosing function names`. Most are not emitters: they assert that a caller
  # holds (or does not hold) a transaction. They are listed anyway, because the
  # point of the audit is that nothing of this shape is invisible, and telling
  # an assertion from an emission is exactly the reading this test forces.
  @call_sites %{
    "codex_pooler/access/api_keys/policies/policy_persistence.ex" => [
      "update_api_key_policy_in_transaction"
    ],
    "codex_pooler/access/api_keys/runtime_authorization.ex" => ["require_transaction!"],
    "codex_pooler/access/dashboard_sessions/lifecycle.ex" => ["run_in_transaction"],
    "codex_pooler/accounting/lifecycle/request_lifecycle.ex" => [
      "revoke_armed_replay_entitlement!"
    ],
    "codex_pooler/accounting/lifecycle/request_lifecycle/dead_execution_recovery.ex" => [
      "recover_candidate"
    ],
    "codex_pooler/accounting/lifecycle/request_lifecycle/reference_locks.ex" => [
      "await_assignment_lock_release",
      "lock_and_validate"
    ],
    "codex_pooler/accounting/request_replay.ex" => ["close_orphaned_lifecycle!"],
    "codex_pooler/events.ex" => ["broadcast_event"],
    "codex_pooler/gateway/persistence/session_continuity/turn_lifecycle.ex" => [
      "authorize_codex_turn_visibility"
    ],
    "codex_pooler/gateway/routing/bridge_ring.ex" => ["locked_side_effect"],
    "codex_pooler/gateway/runtime/finalization/interruption.ex" => [
      "emit_committed_recovery_outcomes",
      "interrupt_owner_request",
      "interrupt_session_turn"
    ],
    "codex_pooler/upstreams/import.ex" => ["import_codex_auth_json", "import_trusted_account"],
    "codex_pooler/upstreams/import_batch_planner.ex" => [
      "diagnose_prepared_batch_in_transaction",
      "plan_prepared_batch_in_transaction",
      "validate_prepared_batch_in_transaction"
    ],
    "codex_pooler/upstreams/lifecycle/identity_slot_lock.ex" => ["require_transaction!"],
    "codex_pooler/upstreams/quota/windows/evidence_store.ex" => ["record_evidence"],
    "codex_pooler/upstreams/saved_resets/convergence.ex" => ["converge"],
    "codex_pooler/upstreams/saved_resets/redemption.ex" => ["finalize_attempt"],
    "codex_pooler/upstreams/token_linking.ex" => [
      "link_prepared",
      "link_prepared_in_transaction",
      "link_tokens",
      "link_tokens_in_transaction",
      "persist_prepared",
      "publish_link_result"
    ]
  }

  test "every Repo.in_transaction?/0 caller in the application is one this audit has read" do
    assert call_sites() == @call_sites,
           """
           The set of `Repo.in_transaction?/0` call sites has changed.

           If the new one decides whether a telemetry sample is emitted, it is a
           dropped-marker caller: say in its own source what happens to the
           markers when a caller owns the transaction — emitted, handed back, or
           lost — and add it here. If it only asserts that a transaction is (or
           is not) held, add it here anyway, so the next reader does not have to
           re-derive the distinction.

           found:
           #{inspect(call_sites(), pretty: true)}
           """
  end

  test "the pre-attempt release counter is still the emitter without the guard" do
    # Not desired: recorded. `tap_pre_attempt_release_count/3` runs on the value
    # of `Repo.transaction/1`, which is a savepoint release rather than a commit
    # whenever a caller already holds a transaction, and it never asks
    # `Repo.in_transaction?/0`. `Interruption.interrupt_session_transaction/4`
    # maps `interrupt_turn!/5` over every in-progress turn inside one
    # transaction, so a later turn's rollback leaves an earlier turn's release
    # counted and unwritten. The defect is carried on findings#195 row 195-94.
    #
    # Fixing it means adding the guard and a post-commit marker to defer to, and
    # that changes this test. That is the point: this assertion is what tells
    # whoever closes 195-94 that row 195-12's second clause becomes satisfiable
    # at the same moment.
    source = File.read!(Path.join(@lib, "codex_pooler/accounting/lifecycle/request_lifecycle.ex"))
    body = function_body(source, "tap_pre_attempt_release_count")

    refute body =~ "Repo.in_transaction?",
           "tap_pre_attempt_release_count/3 now consults Repo.in_transaction?/0. If its markers " <>
             "are deferred to the outermost commit, findings#195 rows 195-12 and 195-94 are both " <>
             "satisfiable: update them and this test together."

    assert body =~ "PreAttemptRelease.emit(",
           "this test no longer reads the function that emits the pre-attempt release counter"
  end

  test "the recovery emitter hands its markers back rather than dropping them" do
    source =
      File.read!(Path.join(@lib, "codex_pooler/gateway/runtime/finalization/interruption.ex"))

    body = function_body(source, "emit_committed_recovery_outcomes")

    assert body =~ "{:deferred, markers}",
           "emit_committed_recovery_outcomes/1 decides on Repo.in_transaction?/0 and must say " <>
             "what becomes of the markers when a caller owns the transaction; returning :ok " <>
             "loses a recovery's outcomes with nothing to say so"
  end

  # `path relative to lib/` => sorted names of the functions holding a call.
  defp call_sites do
    for path <- Path.wildcard(Path.join(@lib, "**/*.ex")),
        source = File.read!(path),
        String.contains?(source, "Repo.in_transaction?"),
        into: %{} do
      {Path.relative_to(path, @lib), enclosing_functions(source)}
    end
  end

  defp enclosing_functions(source) do
    source
    |> String.split("\n")
    |> Enum.reduce({nil, []}, fn line, {current, found} ->
      # A one-line `defp x, do: ... Repo.in_transaction?() ...` is both a
      # definition and a call site, so the name is taken first and the same line
      # is still read for the call.
      current = definition_name(line) || current
      if call?(line), do: {current, [current | found]}, else: {current, found}
    end)
    |> elem(1)
    |> Enum.reject(&is_nil/1)
    |> Enum.uniq()
    |> Enum.sort()
  end

  # A comment naming the function is documentation, not a call site.
  defp call?(line),
    do: String.contains?(line, "Repo.in_transaction?") and not Regex.match?(~r/^\s*#/, line)

  defp definition_name(line) do
    case Regex.run(~r/^\s*defp?\s+([a-z_][A-Za-z0-9_]*[!?]?)/, line) do
      [_line, name] -> name
      nil -> nil
    end
  end

  # The source from a function's first clause up to the next definition, which is
  # enough to read what one small function does without compiling it.
  defp function_body(source, name) do
    lines = String.split(source, "\n")
    start = Enum.find_index(lines, &(definition_name(&1) == name))

    assert start, "no definition of #{name} found"

    rest = Enum.drop(lines, start + 1)

    stop =
      Enum.find_index(rest, fn line ->
        case definition_name(line) do
          nil -> false
          ^name -> false
          _other -> true
        end
      end)

    Enum.join([Enum.at(lines, start) | Enum.take(rest, stop || length(rest))], "\n")
  end
end
