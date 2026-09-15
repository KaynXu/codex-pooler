defmodule CodexPooler.QuotaEvidenceSupport do
  @moduledoc """
  Quota evidence attribute maps shared by tests that record evidence to drive
  locking or convergence.

  Callers rely on these invariants, which `PostResetEvidence.classify/4` reads:
  an `account` window (`quota_key`, `quota_scope`, `quota_family`), a parseable
  `observed` precision, a `fresh` state, a `used_percent` below exhaustion and a
  `reset_at` in the future, so the window counts as usable post-consume
  evidence when it is observed at or after the redemption's `consumed_at`.
  """

  @doc "A fresh, usable account-scoped secondary-window evidence map at `used_percent`."
  @spec account_secondary_evidence(String.t() | integer()) :: map()
  @spec account_secondary_evidence(String.t() | integer(), DateTime.t()) :: map()
  def account_secondary_evidence(used_percent, now \\ DateTime.utc_now()) do
    now = DateTime.truncate(now, :second)

    %{
      quota_key: "account",
      quota_scope: "account",
      quota_family: "account",
      window_kind: "secondary",
      window_minutes: 10_080,
      used_percent: Decimal.new(to_string(used_percent)),
      reset_at: DateTime.add(now, 604_800, :second),
      source: "codex_rate_limit_event",
      source_precision: "observed",
      freshness_state: "fresh",
      metadata: %{}
    }
  end
end
