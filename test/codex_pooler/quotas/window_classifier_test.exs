defmodule CodexPooler.Quotas.WindowClassifierTest do
  use ExUnit.Case, async: true

  alias CodexPooler.Quotas.WindowClassifier
  alias CodexPooler.Upstreams.Quota.AccountQuotaWindow

  describe "classify/1" do
    test "describes account primary 300-minute evidence as primary 5h" do
      window = %AccountQuotaWindow{
        quota_key: "account",
        quota_scope: "account",
        quota_family: "account",
        window_kind: "primary",
        window_minutes: 300
      }

      assert WindowClassifier.classify(window) == :primary_5h
      assert WindowClassifier.primary_5h?(window)
    end

    test "describes account secondary 10080-minute evidence as weekly secondary" do
      window = account_window(%{window_kind: "secondary", window_minutes: 10_080})

      assert WindowClassifier.classify(window) == :weekly_secondary
      assert WindowClassifier.weekly_secondary?(window)
    end

    test "describes exact account primary 43200-minute evidence as monthly primary" do
      window =
        account_window(%{
          "window_kind" => "primary",
          "window_minutes" => "43200",
          "freshness_state" => "fresh",
          "reset_at" => "2026-07-07T00:00:00Z",
          "used_percent" => "42.5"
        })

      assert WindowClassifier.classify(window) == :monthly_primary
      assert WindowClassifier.monthly_primary?(window)
    end

    test "describes unknown long account primary duration as unknown account primary" do
      window = account_window(%{window_kind: "primary", window_minutes: 20_160})

      assert WindowClassifier.classify(window) == :unknown_account_primary
      assert WindowClassifier.unknown_account_primary?(window)
    end

    test "does not classify model or malformed primary windows as account primary evidence" do
      assert WindowClassifier.classify(%AccountQuotaWindow{
               quota_key: "gpt_test_model",
               quota_scope: "model",
               quota_family: "codex_model",
               window_kind: "primary",
               window_minutes: 300
             }) == :unknown

      assert WindowClassifier.classify(account_window(%{window_kind: "primary", window_minutes: nil})) ==
               :unknown_account_primary
    end

    test "describes raw shape without making stale resetless exhausted windows usable" do
      window =
        account_window(%{
          window_kind: "primary",
          window_minutes: 300,
          freshness_state: "stale",
          reset_at: nil,
          used_percent: Decimal.new("100")
        })

      assert WindowClassifier.classify(window) == :primary_5h
    end
  end

  test "normalizes atom descriptors and whitespace without inferring durations" do
    for {kind, minutes, expected} <- [
          {:primary, " 300 ", :primary_5h},
          {:secondary, "10080", :weekly_secondary},
          {:primary, "43200", :monthly_primary},
          {:primary, "10080", :unknown_account_primary},
          {:secondary, "300", :unknown}
        ] do
      assert WindowClassifier.classify(%{
               "quota_key" => " ACCOUNT ",
               "quota_scope" => :account,
               "quota_family" => :account,
               "window_kind" => kind,
               "window_minutes" => minutes
             }) == expected
    end
  end

  test "malformed durations remain unknown primary evidence and never become a known window" do
    for duration <- ["300x", "300.0", "", " ", 300.0, false, [], %{}, nil, -1, 0] do
      window = account_window(%{window_kind: "primary", window_minutes: duration})
      assert WindowClassifier.classify(window) == :unknown_account_primary
      refute WindowClassifier.primary_5h?(window)
      refute WindowClassifier.monthly_primary?(window)
      refute WindowClassifier.weekly_secondary?(window)
    end
  end

  test "each account identity dimension is required and invalid inputs stay unknown" do
    valid = account_window(%{window_kind: "primary", window_minutes: 300})

    for field <- [:quota_key, :quota_scope, :quota_family],
        value <- [nil, false, true, [], %{}, "model"] do
      assert WindowClassifier.classify(Map.put(valid, field, value)) == :unknown
    end

    for value <- [nil, false, [], "account", 300] do
      assert WindowClassifier.classify(value) == :unknown
      refute WindowClassifier.unknown_account_primary?(value)
    end
  end

  defp account_window(overrides) do
    Map.merge(
      %{
        quota_key: "account",
        quota_scope: "account",
        quota_family: "account"
      },
      overrides
    )
  end
end
