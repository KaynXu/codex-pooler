defmodule CodexPoolerWeb.Admin.UpstreamAccountsReadModel.SavedResetConfirmationProjectionTest do
  use ExUnit.Case, async: true

  alias CodexPooler.Upstreams.Quota.AccountQuotaWindow
  alias CodexPoolerWeb.Admin.UpstreamAccountsReadModel.SavedResetConfirmationProjection

  test "monthly quota is the challenged window and five-hour exhaustion remains additional" do
    now = ~U[2026-07-14 03:30:00.000000Z]

    monthly = %AccountQuotaWindow{
      quota_key: "account",
      quota_scope: "account",
      quota_family: "account",
      window_kind: "primary",
      window_minutes: 43_200,
      source: "codex_usage_api",
      source_precision: "observed",
      used_percent: Decimal.new("100"),
      observed_at: now,
      last_sync_at: now,
      reset_at: DateTime.add(now, 20, :day),
      freshness_state: "fresh",
      metadata: %{}
    }

    primary = %{monthly | window_minutes: 300}

    result =
      SavedResetConfirmationProjection.project(
        %{"phase" => "consumed_pending_probe", "consumed_at" => DateTime.to_iso8601(now)},
        [monthly, primary],
        [monthly, primary],
        now
      )

    assert result.challenged_evidence_state == :exhausted
    assert result.additional_account_blocker_state == :exhausted
  end
end
