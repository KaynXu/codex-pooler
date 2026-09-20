defmodule CodexPooler.Accounting.TokenWindowReservationSafetyTest do
  use CodexPooler.DataCase, async: false

  alias CodexPooler.Accounting
  alias CodexPooler.Accounting.{LedgerEntry, Request}
  alias CodexPooler.Accounting.RequestLifecycle.LedgerEntries

  import CodexPooler.AccountingTestSupport

  for {window, limit_field, seconds} <- [
        {:daily, :max_tokens_per_day, 86_400},
        {:weekly, :max_tokens_per_week, 604_800}
      ] do
    test "#{window} estimates admit outstanding requests whose actual usage exceeds the limit" do
      setup = accounting_setup()
      as_of = DateTime.utc_now() |> DateTime.truncate(:microsecond)
      window = unquote(window)
      limit_field = unquote(limit_field)
      windows = [{window, DateTime.add(as_of, -unquote(seconds), :second)}]
      payload = %{"model" => setup.model.exposed_model_id}

      update_default_policy!(
        setup.api_key,
        Map.put(
          %{max_tokens_per_day: nil, max_tokens_per_week: nil, max_requests_per_minute: 60},
          limit_field,
          1_024
        )
      )

      assert {:error, %{code: :invalid_request}} =
               Accounting.reserve(%{}, setup.model, payload, %{now: as_of})

      assert Repo.aggregate(from(r in Request, where: r.api_key_id == ^setup.api_key.id), :count) ==
               0

      reservations =
        for _ <- 1..2 do
          assert {:ok, reserved} =
                   Accounting.reserve(setup.auth, setup.model, payload, %{now: as_of})

          assert reserved.estimate.total_tokens == 512
          assert reserved.reservation.total_tokens == 512
          reserved
        end

      assert %{effective_request_count: 2, effective_total_tokens: 1_024} =
               Map.fetch!(LedgerEntries.window_usages(setup.api_key.id, windows), window)

      assert_limit_denied(setup, payload, as_of, limit_field)

      for reserved <- reservations do
        assert {:ok, attempt} = Accounting.create_attempt(reserved.request, setup.assignment)

        assert {:ok, result} =
                 Accounting.finalize_success(
                   reserved.request,
                   attempt,
                   %{
                     status: "usage_known",
                     input_tokens: 0,
                     cached_input_tokens: 0,
                     output_tokens: 4_096,
                     reasoning_tokens: 0,
                     total_tokens: 4_096,
                     recorded_at: as_of
                   },
                   %{now: as_of}
                 )

        assert result.request.status == "succeeded"
        assert result.settlement.total_tokens == 4_096
        assert result.release.total_tokens == 512
      end

      assert %{effective_request_count: 2, effective_total_tokens: 8_192} =
               Map.fetch!(LedgerEntries.window_usages(setup.api_key.id, windows), window)

      assert_limit_denied(setup, payload, as_of, limit_field)

      assert Repo.aggregate(from(r in Request, where: r.api_key_id == ^setup.api_key.id), :count) ==
               2

      assert Repo.aggregate(
               from(e in LedgerEntry,
                 where: e.api_key_id == ^setup.api_key.id and e.entry_kind == "reservation"
               ),
               :count
             ) == 2
    end

    test "#{window} unknown usage retains provisional token pressure after freeing its reservation" do
      setup = accounting_setup()
      as_of = DateTime.utc_now() |> DateTime.truncate(:microsecond)
      window = unquote(window)
      limit_field = unquote(limit_field)
      windows = [{window, DateTime.add(as_of, -unquote(seconds), :second)}]
      payload = %{"model" => setup.model.exposed_model_id}

      update_default_policy!(
        setup.api_key,
        Map.put(
          %{max_tokens_per_day: nil, max_tokens_per_week: nil, max_requests_per_minute: 60},
          limit_field,
          512
        )
      )

      assert {:ok, reserved} =
               Accounting.reserve(setup.auth, setup.model, payload, %{now: as_of})

      assert reserved.reservation.total_tokens == 512

      assert %{effective_request_count: 1, effective_total_tokens: 512} =
               Map.fetch!(LedgerEntries.window_usages(setup.api_key.id, windows), window)

      assert_limit_denied(setup, payload, as_of, limit_field)
      assert {:ok, attempt} = Accounting.create_attempt(reserved.request, setup.assignment)

      assert {:ok, result} =
               Accounting.finalize_failure(reserved.request, attempt, %{
                 last_error_code: "stream_interrupted",
                 usage: %{status: "usage_unknown", recorded_at: as_of},
                 now: as_of
               })

      assert result.request.status == "failed"
      assert result.settlement.usage_status == "usage_unknown"
      assert result.settlement.total_tokens == 512
      assert result.settlement.details["estimated_from_reserve"] == true
      assert result.release.total_tokens == 512

      assert %{
               effective_request_count: 1,
               effective_total_tokens: 512,
               known_total_tokens: 0,
               provisional_total_tokens: 512,
               pending_total_tokens: 0
             } =
               Map.fetch!(LedgerEntries.window_usages(setup.api_key.id, windows), window)

      refute Accounting.LedgerReads.reservation_outstanding?(reserved.request)
      assert_limit_denied(setup, payload, as_of, limit_field)
    end

    for {usage_status, effective_tokens} <- [{"usage_known", 4_096}, {"usage_unknown", 512}] do
      test "#{window} window retains old pending pressure and attributes #{usage_status} without release credit" do
        setup = accounting_setup()
        as_of = DateTime.utc_now() |> DateTime.truncate(:microsecond)
        window = unquote(window)

        since = window_start(window, as_of)
        reserved_at = DateTime.add(since, -1, :second)
        payload = %{"model" => setup.model.exposed_model_id}

        assert {:ok, reserved} =
                 Accounting.reserve(setup.auth, setup.model, payload, %{now: reserved_at})

        assert reserved.reservation.total_tokens == 512
        assert reserved.reservation.occurred_at == reserved_at

        assert %{effective_request_count: 0, effective_total_tokens: 512} =
                 Map.fetch!(
                   LedgerEntries.window_usages(setup.api_key.id, [{window, since}]),
                   window
                 )

        assert {:ok, attempt} = Accounting.create_attempt(reserved.request, setup.assignment)

        result = finalize_usage(reserved.request, attempt, unquote(usage_status), as_of)

        assert result.settlement.usage_status == unquote(usage_status)
        assert result.settlement.occurred_at == as_of
        assert result.release.total_tokens == 512
        assert result.release.occurred_at == as_of

        current =
          Map.fetch!(LedgerEntries.window_usages(setup.api_key.id, [{window, since}]), window)

        assert current.effective_request_count == 0
        assert current.effective_total_tokens == unquote(effective_tokens)

        including_reservation =
          Map.fetch!(
            LedgerEntries.window_usages(setup.api_key.id, [{window, reserved_at}]),
            window
          )

        assert including_reservation.effective_request_count == 1
        assert including_reservation.effective_total_tokens == unquote(effective_tokens)
      end
    end
  end

  defp window_start(:daily, as_of),
    do: DateTime.new!(DateTime.to_date(as_of), ~T[00:00:00.000000], "Etc/UTC")

  defp window_start(:weekly, as_of), do: DateTime.add(as_of, -7, :day)

  defp finalize_usage(request, attempt, "usage_known", as_of) do
    assert {:ok, result} =
             Accounting.finalize_success(
               request,
               attempt,
               %{
                 status: "usage_known",
                 input_tokens: 0,
                 cached_input_tokens: 0,
                 output_tokens: 4_096,
                 reasoning_tokens: 0,
                 total_tokens: 4_096,
                 recorded_at: as_of
               },
               %{now: as_of}
             )

    assert result.settlement.total_tokens == 4_096
    result
  end

  defp finalize_usage(request, attempt, "usage_unknown", as_of) do
    assert {:ok, result} =
             Accounting.finalize_failure(request, attempt, %{
               last_error_code: "stream_interrupted",
               usage: %{status: "usage_unknown", recorded_at: as_of},
               now: as_of
             })

    assert result.settlement.total_tokens == 512
    assert result.settlement.details["estimated_from_reserve"] == true
    result
  end

  test "per-request output policy checks the estimate but settlement records larger actual output" do
    setup = accounting_setup()
    as_of = DateTime.utc_now() |> DateTime.truncate(:microsecond)
    payload = %{"model" => setup.model.exposed_model_id}

    update_default_policy!(setup.api_key, %{
      max_tokens_per_day: nil,
      max_tokens_per_week: nil,
      max_output_tokens_per_request: 512,
      max_requests_per_minute: 60
    })

    assert {:error, error} =
             Accounting.reserve(
               setup.auth,
               setup.model,
               Map.put(payload, "max_output_tokens", 513),
               %{now: as_of}
             )

    assert error.code == :api_key_policy_limit_exceeded
    assert error.message =~ "max_output_tokens_per_request"

    assert {:ok, reserved} =
             Accounting.reserve(setup.auth, setup.model, payload, %{now: as_of})

    assert reserved.estimate.output_tokens == 512
    assert reserved.reservation.output_tokens == 512
    assert {:ok, attempt} = Accounting.create_attempt(reserved.request, setup.assignment)

    assert {:ok, result} =
             Accounting.finalize_success(
               reserved.request,
               attempt,
               %{
                 status: "usage_known",
                 input_tokens: 0,
                 cached_input_tokens: 0,
                 output_tokens: 4_096,
                 reasoning_tokens: 0,
                 total_tokens: 4_096,
                 recorded_at: as_of
               },
               %{now: as_of}
             )

    assert result.request.status == "succeeded"
    assert result.settlement.output_tokens == 4_096
    assert result.settlement.total_tokens == 4_096
    assert result.release.output_tokens == 512
  end

  defp assert_limit_denied(setup, payload, as_of, limit_field) do
    assert {:error, error} =
             Accounting.reserve(setup.auth, setup.model, payload, %{now: as_of})

    assert error.code == :api_key_policy_limit_exceeded
    assert error.message =~ Atom.to_string(limit_field)
  end

  test "pre-attempt and repeated releases retain one admission without provisional usage" do
    setup = accounting_setup()
    admitted_at = ~U[2026-08-02 00:00:30.000000Z]
    released_at = DateTime.add(admitted_at, 30, :second)

    {:ok, reserved} =
      Accounting.reserve(setup.auth, setup.model, %{"model" => setup.model.exposed_model_id}, %{
        now: admitted_at
      })

    assert {:ok, _} =
             Accounting.finalize_reservation_failure(
               reserved.request,
               %{
                 now: released_at,
                 usage_status: "usage_unknown",
                 pre_attempt_phase: "turn_interrupted"
               }
             )

    assert {:error, %{code: :request_already_finalized}} =
             Accounting.finalize_reservation_failure(reserved.request, %{now: released_at})

    assert %{
             effective_request_count: 1,
             known_total_tokens: 0,
             provisional_total_tokens: 0,
             pending_total_tokens: 0,
             effective_total_tokens: 0
           } =
             LedgerEntries.window_usages(setup.api_key.id, [window: admitted_at], released_at).window
  end

  test "post-attempt release retains one provisional estimate without a settlement" do
    setup = accounting_setup()
    admitted_at = ~U[2026-08-02 00:00:30.000000Z]
    released_at = DateTime.add(admitted_at, 90, :second)

    {:ok, reserved} =
      Accounting.reserve(setup.auth, setup.model, %{"model" => setup.model.exposed_model_id}, %{
        now: admitted_at
      })

    {:ok, attempt} = Accounting.create_attempt(reserved.request, setup.assignment)

    attempt =
      attempt
      |> Ecto.Changeset.change(status: "failed", completed_at: released_at)
      |> Repo.update!()

    assert {:ok, result} =
             Accounting.finalize_reservation_failure(
               reserved.request,
               %{now: released_at, usage_status: "usage_unknown", released_after_attempt: attempt}
             )

    assert result.release.attempt_id == attempt.id
    refute Accounting.reservation_outstanding?(reserved.request)

    assert %{
             effective_request_count: 1,
             known_total_tokens: 0,
             provisional_total_tokens: 512,
             pending_total_tokens: 0,
             effective_total_tokens: 512
           } =
             LedgerEntries.window_usages(setup.api_key.id, [window: admitted_at], released_at).window

    # Repeated unmatched releases cannot subtract another request's usage or
    # double this request's estimate, even when imported without source ids.
    result.release
    |> Map.put(:id, Ecto.UUID.generate())
    |> Map.put(:source_event_id, nil)
    |> Repo.insert!()

    assert %{provisional_total_tokens: 512, effective_request_count: 1} =
             LedgerEntries.window_usages(setup.api_key.id, [window: admitted_at], released_at).window
  end

  for status <- ["usage_known", "not_applicable", "usage_unknown"] do
    test "#{status} terminal usage distinguishes known zero, no-charge and provisional estimates" do
      setup = accounting_setup()
      admitted_at = ~U[2026-08-02 00:00:30.000000Z]
      terminal_at = DateTime.add(admitted_at, 90, :second)

      {:ok, reserved} =
        Accounting.reserve(setup.auth, setup.model, %{"model" => setup.model.exposed_model_id}, %{
          now: admitted_at
        })

      {:ok, attempt} = Accounting.create_attempt(reserved.request, setup.assignment)

      usage = %{
        status: unquote(status),
        total_tokens: 0,
        input_tokens: 0,
        output_tokens: 0,
        recorded_at: terminal_at
      }

      assert {:ok, _} =
               Accounting.finalize_failure(reserved.request, attempt, %{
                 now: DateTime.add(terminal_at, 60, :second),
                 usage: usage
               })

      expected_provisional = if unquote(status) == "usage_unknown", do: 512, else: 0

      window =
        LedgerEntries.window_usages(setup.api_key.id, [window: admitted_at], terminal_at).window

      assert window.effective_request_count == 1
      assert window.known_total_tokens == 0
      assert window.provisional_total_tokens == expected_provisional
      assert window.pending_total_tokens == 0
      assert Decimal.equal?(window.effective_cost_micros, Decimal.new(0))

      expired =
        LedgerEntries.window_usages(
          setup.api_key.id,
          [window: DateTime.add(terminal_at, 1, :microsecond)],
          DateTime.add(terminal_at, 60, :second)
        ).window

      assert expired.effective_total_tokens == 0
    end
  end

  test "invalid measured counters retain the reservation as uncertain usage" do
    setup = accounting_setup()
    admitted_at = ~U[2026-08-02 00:00:30.000000Z]
    terminal_at = DateTime.add(admitted_at, 60, :second)

    {:ok, reserved} =
      Accounting.reserve(setup.auth, setup.model, %{"model" => setup.model.exposed_model_id}, %{
        now: admitted_at
      })

    {:ok, attempt} = Accounting.create_attempt(reserved.request, setup.assignment)

    assert {:ok, result} =
             Accounting.finalize_success(
               reserved.request,
               attempt,
               %{
                 status: "usage_known",
                 input_tokens: -1,
                 output_tokens: 0,
                 total_tokens: -1,
                 recorded_at: terminal_at
               },
               %{now: terminal_at}
             )

    assert result.settlement.usage_status == "usage_unknown"

    assert %{
             known_total_tokens: 0,
             provisional_total_tokens: 512,
             pending_total_tokens: 0,
             effective_request_count: 1
           } =
             LedgerEntries.window_usages(setup.api_key.id, [window: admitted_at], terminal_at).window
  end

  test "voiding known settlement does not invent provisional spend from its release" do
    setup = accounting_setup()
    timestamp = ~U[2026-08-02 00:00:30.000000Z]

    {:ok, reserved} =
      Accounting.reserve(setup.auth, setup.model, %{"model" => setup.model.exposed_model_id}, %{
        now: timestamp
      })

    {:ok, attempt} = Accounting.create_attempt(reserved.request, setup.assignment)
    result = finalize_usage(reserved.request, attempt, "usage_known", timestamp)
    result.settlement |> Ecto.Changeset.change(amount_status: "voided") |> Repo.update!()

    assert %{
             known_total_tokens: 0,
             provisional_total_tokens: 0,
             pending_total_tokens: 0,
             effective_request_count: 1
           } =
             LedgerEntries.window_usages(setup.api_key.id, [window: timestamp], timestamp).window
  end
end
