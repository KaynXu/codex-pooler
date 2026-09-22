defmodule CodexPooler.Quotas.CodexParsersAdditionalIdentityTest do
  use ExUnit.Case, async: true

  alias CodexPooler.Quotas.Evidence.CodexParsers

  @observed_at ~U[2026-08-25 10:00:00Z]

  test "windows-only compatibility accepts legacy selected windows while strict results reject them" do
    legacy_payload = %{
      "plan_type" => "sample_plan",
      "rate_limit" => %{
        "primary_window" => %{
          "used_percent" => 25,
          "limit_window_seconds" => 18_000,
          "reset_after_seconds" => 900
        }
      }
    }

    assert {:ok, [legacy_window]} =
             CodexParsers.parse_codex_usage_payload(legacy_payload, @observed_at)

    assert legacy_window.window_minutes == 300
    assert legacy_window.reset_at == DateTime.add(@observed_at, 900, :second)

    assert {:ok, %{windows: [], account_availability: availability}} =
             CodexParsers.parse_codex_usage_result(legacy_payload, @observed_at)

    assert availability.state == :unknown
    assert availability.basis == :conflict
    assert availability.account_windows == :unknown
  end

  test "same-label additional meters retain the shared legacy window identity" do
    assert {:ok, evidences} =
             CodexParsers.parse_codex_usage_payload(same_label_meter_payload(), @observed_at)

    assert length(evidences) == 2

    assert evidences
           |> MapSet.new(&{&1.quota_key, &1.window_kind, &1.window_minutes})
           |> MapSet.size() == 1
  end

  test "same-label additional meters retain distinct canonical identities" do
    assert {:ok, evidences} =
             CodexParsers.parse_codex_usage_payload(same_label_meter_payload(), @observed_at)

    assert Enum.map(evidences, &{&1.raw_limit_id, &1.raw_metered_feature, &1.used_percent}) == [
             {"meter_alpha", "meter_alpha", Decimal.new("31.0")},
             {"meter_beta", "meter_beta", Decimal.new("71.0")}
           ]
  end

  test "synthetic Reserve-shaped weekly payload preserves its wire identity without a model substitution" do
    payload = %{
      "additional_rate_limits" => [
        %{
          "limit_name" => "gpt-reserve",
          "metered_feature" => "base_model_inference",
          "rate_limit" => %{
            "primary_window" => %{
              "used_percent" => 25,
              "limit_window_seconds" => 604_800,
              "reset_after_seconds" => 604_800,
              "reset_at" => 1_778_000_000
            }
          }
        }
      ]
    }

    assert {:ok, [evidence]} = CodexParsers.parse_codex_usage_payload(payload, @observed_at)

    assert evidence.quota_key == "gpt_reserve"
    assert evidence.quota_scope == "model"
    assert evidence.model == "gpt-reserve"
    assert evidence.raw_limit_name == "gpt-reserve"
    assert evidence.raw_metered_feature == "base_model_inference"
    assert evidence.window_kind == "secondary"
    assert evidence.window_minutes == 10_080
  end

  test "exact duplicate meter identity deterministically retains highest pressure" do
    payload = %{
      "additional_rate_limits" => [
        additional_limit("meter_alpha", 83),
        additional_limit("meter_alpha", 19)
      ]
    }

    assert {:ok, [evidence]} = CodexParsers.parse_codex_usage_payload(payload, @observed_at)
    assert evidence.raw_metered_feature == "meter_alpha"
    assert Decimal.equal?(evidence.used_percent, Decimal.new("83.0"))
  end

  test "blank metered feature falls back to the trimmed raw limit id" do
    limit =
      "   "
      |> additional_limit(44)
      |> Map.put("limit_id", "  provider_meter_fallback  ")

    assert {:ok, [evidence]} =
             CodexParsers.parse_codex_usage_payload(
               %{"additional_rate_limits" => [limit]},
               @observed_at
             )

    assert evidence.raw_metered_feature == "provider_meter_fallback"
    assert evidence.raw_limit_id == "provider_meter_fallback"
    refute evidence.raw_limit_id == evidence.raw_limit_name
  end

  test "display label alone never becomes canonical meter identity" do
    limit = additional_limit("   ", 44)

    assert {:ok, [evidence]} =
             CodexParsers.parse_codex_usage_payload(
               %{"additional_rate_limits" => [limit]},
               @observed_at
             )

    assert evidence.raw_limit_name == "Shared weekly limit"
    assert evidence.raw_metered_feature == nil
    assert evidence.raw_limit_id == nil
  end

  # findings#238: a usage-body limit_name is a display label, bounded as
  # printable ASCII of at most 80 bytes; anything else is fingerprinted, never
  # erased, on the label, its identity part and the derived display label.
  test "a usage-body limit_name inside the label bound stays cleartext" do
    label = "Shared weekly limit (beta) v2.1"
    limit = "meter_clear" |> additional_limit(44) |> Map.put("limit_name", label)

    assert {:ok, [evidence]} =
             CodexParsers.parse_codex_usage_payload(
               %{"additional_rate_limits" => [limit]},
               @observed_at
             )

    assert evidence.raw_limit_name == label
    assert evidence.limit_name == label
    assert evidence.raw_metered_feature == "meter_clear"
  end

  test "a usage-body limit_name outside the label bound is fingerprinted" do
    overlong = "Shared weekly limit " <> String.duplicate("x", 80)
    control_characters = "Sharedweekly limit"

    for label <- [overlong, control_characters] do
      limit = "meter_bounded" |> additional_limit(44) |> Map.put("limit_name", label)

      assert {:ok, [evidence]} =
               CodexParsers.parse_codex_usage_payload(
                 %{"additional_rate_limits" => [limit]},
                 @observed_at
               )

      assert evidence.raw_limit_name == fingerprint(label)
      assert evidence.limit_name == fingerprint(label)
      assert evidence.raw_metered_feature == "meter_bounded"
      refute inspect(evidence) =~ "Shared"
    end
  end

  test "a label-only usage limit outside the bound keeps a fingerprinted identity" do
    overlong = "Label only meter " <> String.duplicate("y", 80)
    limit = "   " |> additional_limit(44) |> Map.put("limit_name", overlong)

    assert {:ok, [evidence]} =
             CodexParsers.parse_codex_usage_payload(
               %{"additional_rate_limits" => [limit]},
               @observed_at
             )

    assert evidence.raw_limit_name == fingerprint(overlong)
    refute inspect(evidence) =~ "Label only meter"
  end

  test "a rate-limit error limit_name takes the same label bound" do
    overlong = "Error dialect label " <> String.duplicate("z", 80)

    payload = %{
      "limit_name" => overlong,
      "metered_feature" => "meter_error",
      "reset_at" => 1_778_000_000,
      "window_minutes" => 300,
      "used_percent" => 50
    }

    assert [evidence] = CodexParsers.parse_rate_limit_error(payload, @observed_at)
    assert evidence.raw_limit_name == fingerprint(overlong)
    refute inspect(evidence) =~ "Error dialect label"

    assert [clear] =
             CodexParsers.parse_rate_limit_error(
               Map.put(payload, "limit_name", "Error dialect label"),
               @observed_at
             )

    assert clear.raw_limit_name == "Error dialect label"
  end

  defp fingerprint(value) do
    "sha256_" <>
      (:crypto.hash(:sha256, value)
       |> Base.encode16(case: :lower)
       |> String.slice(0, 12))
  end

  defp same_label_meter_payload do
    %{
      "additional_rate_limits" => [
        additional_limit("meter_alpha", 31),
        additional_limit("meter_beta", 71)
      ]
    }
  end

  defp additional_limit(metered_feature, used_percent) do
    %{
      "limit_name" => "Shared weekly limit",
      "metered_feature" => metered_feature,
      "rate_limit" => %{
        "primary_window" => %{
          "used_percent" => used_percent,
          "limit_window_seconds" => 604_800,
          "reset_after_seconds" => 604_800,
          "reset_at" => 1_778_000_000
        }
      }
    }
  end
end
