defmodule CodexPooler.Accounting.RequestSnapshotTest do
  use CodexPooler.DataCase, async: false

  alias CodexPooler.Access.APIKeys.ReasoningEffortPolicy.Decision
  alias CodexPooler.Accounting
  alias CodexPooler.Accounting.{Attempt, Request}
  alias CodexPooler.Gateway.Denials
  alias CodexPooler.Gateway.Payloads.{PayloadNormalizer, RequestOptions}
  alias CodexPooler.Repo

  import CodexPooler.AccountingTestSupport

  describe "gateway accounting request snapshots" do
    test "request snapshot keeps original upstream account fields after identity mutation" do
      setup =
        accounting_setup(%{
          account_label: "Operator account",
          account_email: "operator@example.com",
          plan_label: "Pro",
          plan_family: "paid"
        })

      assert {:ok, reserved} =
               Accounting.reserve(
                 setup.auth,
                 setup.model,
                 %{
                   "model" => setup.model.exposed_model_id,
                   "service_tier" => "auto",
                   "reasoning" => %{"effort" => "medium"}
                 },
                 %{correlation_id: "corr-snapshot-historical"}
               )

      assert {:ok, attempt} = Accounting.create_attempt(reserved.request, setup.assignment)

      assert {:ok, _result} =
               Accounting.finalize_success(
                 reserved.request,
                 attempt,
                 %{status: "usage_known", input_tokens: 1, output_tokens: 1, total_tokens: 2},
                 %{response_status_code: 200, attempt_metadata: %{"service_tier" => "priority"}}
               )

      setup.identity
      |> Ecto.Changeset.change(%{
        account_label: "changed@example.com",
        account_email: "changed@example.com",
        plan_label: "Team",
        updated_at: DateTime.utc_now() |> DateTime.truncate(:microsecond)
      })
      |> Repo.update!()

      persisted = Repo.get!(CodexPooler.Accounting.Request, reserved.request.id)

      assert persisted.upstream_account_label == "Operator account"
      assert persisted.upstream_account_email == "operator@example.com"
      assert persisted.upstream_account_plan_label == "Pro"
      assert persisted.upstream_account_plan_family == "paid"
    end

    test "request snapshot keeps attempt-time identity when identity mutates before finalization" do
      setup =
        accounting_setup(%{
          account_label: "Operator account",
          account_email: "operator@example.com",
          plan_label: "Pro",
          plan_family: "paid"
        })

      assert {:ok, reserved} =
               Accounting.reserve(
                 setup.auth,
                 setup.model,
                 %{"model" => setup.model.exposed_model_id},
                 %{correlation_id: "corr-snapshot-attempt-time"}
               )

      assert {:ok, attempt} = Accounting.create_attempt(reserved.request, setup.assignment)

      setup.identity
      |> Ecto.Changeset.change(%{
        account_label: "changed@example.com",
        account_email: "changed@example.com",
        plan_label: "Team",
        plan_family: "enterprise",
        updated_at: DateTime.utc_now() |> DateTime.truncate(:microsecond)
      })
      |> Repo.update!()

      assert {:ok, _result} =
               Accounting.finalize_success(
                 reserved.request,
                 attempt,
                 %{status: "usage_known", input_tokens: 1, output_tokens: 1, total_tokens: 2},
                 %{response_status_code: 200}
               )

      persisted = Repo.get!(CodexPooler.Accounting.Request, reserved.request.id)
      assert persisted.upstream_account_label == "Operator account"
      assert persisted.upstream_account_email == "operator@example.com"
      assert persisted.upstream_account_plan_label == "Pro"
      assert persisted.upstream_account_plan_family == "paid"
    end

    test "non-email upstream account labels do not populate upstream_account_email" do
      setup =
        accounting_setup(%{
          account_label: "Codex account",
          plan_label: "Team",
          plan_family: "paid"
        })

      assert {:ok, reserved} =
               Accounting.reserve(
                 setup.auth,
                 setup.model,
                 %{"model" => setup.model.exposed_model_id},
                 %{correlation_id: "corr-snapshot-non-email"}
               )

      assert {:ok, attempt} = Accounting.create_attempt(reserved.request, setup.assignment)

      assert {:ok, _result} =
               Accounting.finalize_success(
                 reserved.request,
                 attempt,
                 %{status: "usage_known", input_tokens: 1, output_tokens: 1, total_tokens: 2},
                 %{response_status_code: 200}
               )

      persisted = Repo.get!(CodexPooler.Accounting.Request, reserved.request.id)

      assert persisted.upstream_account_label == "Codex account"
      assert is_nil(persisted.upstream_account_email)
      assert persisted.upstream_account_plan_label == "Team"
      assert persisted.upstream_account_plan_family == "paid"
    end

    test "denied requests keep upstream snapshot fields nil" do
      setup = accounting_setup()

      assert {:ok, %{request: denied}} =
               Accounting.record_denied_request(setup.auth, setup.model, %{
                 correlation_id: "corr-denied-snapshot-nil",
                 endpoint: "/backend-api/codex/responses",
                 transport: "http_json",
                 request_metadata: %{"policy_denial" => %{"code" => "model_not_allowed"}}
               })

      persisted = Repo.get!(CodexPooler.Accounting.Request, denied.id)
      assert is_nil(persisted.upstream_account_email)
      assert is_nil(persisted.upstream_account_plan_label)
      assert is_nil(persisted.upstream_account_plan_family)

      assert %{items: [log], total: 1} = Accounting.list_request_logs(setup.pool)
      assert log.id == denied.id
      assert log.status == "rejected"
    end

    test "request snapshot stores model settings with effective tier preferring actual tier" do
      setup =
        accounting_setup(%{
          account_label: "Operator account",
          account_email: "operator@example.com",
          plan_label: "Pro"
        })

      raw_secret = "sk-cxp-123456789abc-SECRET-token"
      raw_prompt = "keep this prompt out of snapshots"

      assert {:ok, reserved} =
               Accounting.reserve(
                 setup.auth,
                 setup.model,
                 %{
                   "model" => setup.model.exposed_model_id,
                   "service_tier" => "auto",
                   "reasoning" => %{"effort" => "high"},
                   "input" => raw_prompt
                 },
                 %{
                   correlation_id: "corr-model-settings-snapshot",
                   request_metadata: %{
                     "authorization" => "Bearer " <> raw_secret,
                     "body" => %{"input" => raw_prompt}
                   }
                 }
               )

      assert {:ok, attempt} = Accounting.create_attempt(reserved.request, setup.assignment)

      assert {:ok, _result} =
               Accounting.finalize_success(
                 reserved.request,
                 attempt,
                 %{status: "usage_known", input_tokens: 2, output_tokens: 1, total_tokens: 3},
                 %{response_status_code: 200, attempt_metadata: %{"service_tier" => "priority"}}
               )

      persisted = Repo.get!(CodexPooler.Accounting.Request, reserved.request.id)
      assert persisted.reasoning_effort == "high"
      assert persisted.requested_service_tier == "auto"
      assert persisted.actual_service_tier == "priority"
      assert persisted.service_tier == "priority"

      refute inspect(persisted) =~ raw_secret
      refute inspect(persisted) =~ raw_prompt
    end

    test "successful reasoning evidence keeps only the approved policy snapshot fields" do
      setup = accounting_setup()

      payload = %{
        "model" => setup.model.exposed_model_id,
        "reasoning" => %{"effort" => "minimal"}
      }

      decision = %Decision{
        mode: :always_use,
        configured_effort: "minimal",
        requested_effort: "minimal",
        applied_effort: "minimal"
      }

      request_options =
        RequestOptions.build(
          %{reasoning_effort_decision: decision},
          "/backend-api/codex/responses",
          payload
        )

      assert {:ok, _encoded, request_options} =
               PayloadNormalizer.prepare_upstream_payload(
                 payload,
                 setup.model,
                 "/backend-api/codex/responses",
                 request_options
               )

      assert RequestOptions.reasoning_effort_attempt_metadata(request_options) == %{
               "reasoning" => %{
                 "policy_mode" => "always_use",
                 "configured_effort" => "minimal",
                 "requested_effort" => "minimal",
                 "applied_effort" => "minimal",
                 "effective_effort" => "low",
                 "source" => "api_key_policy",
                 "rewrite" => "minimal_to_low"
               }
             }

      assert {:ok, reserved} =
               Accounting.reserve(setup.auth, setup.model, payload, %{
                 correlation_id: "corr-minimal-reasoning-success"
               })

      assert {:ok, attempt} = Accounting.create_attempt(reserved.request, setup.assignment)

      assert {:ok, _result} =
               Accounting.finalize_success(
                 reserved.request,
                 attempt,
                 %{status: "usage_known", input_tokens: 1, output_tokens: 1, total_tokens: 2},
                 %{
                   response_status_code: 200,
                   attempt_metadata: RequestOptions.reasoning_effort_attempt_metadata(request_options)
                 }
               )

      persisted_attempt = Repo.get!(Attempt, attempt.id)

      assert persisted_attempt.response_metadata["reasoning"] == %{
               "policy_mode" => "always_use",
               "configured_effort" => "minimal",
               "requested_effort" => "minimal",
               "applied_effort" => "minimal",
               "effective_effort" => "low",
               "source" => "api_key_policy",
               "rewrite" => "minimal_to_low"
             }

      assert %{items: [log], total: 1} = Accounting.list_request_logs(setup.pool)
      refute Map.has_key?(log, :reasoning_policy_mode)
      refute Map.has_key?(log, :configured_reasoning_effort)
    end

    test "pre-reservation reasoning denial bounds unknown input and fabricates no attempt" do
      setup = accounting_setup()
      unknown_effort = String.duplicate("custom-", 512)

      payload = %{
        "model" => setup.model.exposed_model_id,
        "reasoning" => %{"effort" => unknown_effort}
      }

      reason = %{
        status: 400,
        code: "reasoning_effort_not_allowed",
        message: "reasoning effort is not available for this API key",
        param: "reasoning.effort",
        reasoning_policy: %{
          policy_mode: "allow_up_to",
          configured_effort: "medium",
          requested_effort: unknown_effort,
          applied_effort: nil,
          arbitrary_metadata: "excluded"
        },
        arbitrary_metadata: "excluded"
      }

      assert {:error, ^reason} =
               Denials.log_gateway(%Denials.Context{
                 auth: setup.auth,
                 model: setup.model,
                 reason: reason,
                 endpoint: "/backend-api/codex/responses",
                 payload: payload,
                 opts: RequestOptions.build(%{}, "/backend-api/codex/responses", payload)
               })

      assert [request] = Repo.all(Request)
      assert Repo.all(Attempt) == []

      assert request.request_metadata["gateway_denial"] == %{
               "code" => "reasoning_effort_not_allowed",
               "message" => "reasoning effort is not available for this API key",
               "param" => "reasoning.effort",
               "reasoning_policy" => %{
                 "policy_mode" => "allow_up_to",
                 "configured_effort" => "medium",
                 "requested_effort" => "unknown",
                 "applied_effort" => nil
               }
             }

      refute inspect(request.request_metadata) =~ unknown_effort
      refute inspect(request.request_metadata) =~ "arbitrary_metadata"
    end
  end
end
