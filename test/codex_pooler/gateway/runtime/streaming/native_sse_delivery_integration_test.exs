defmodule CodexPooler.Gateway.Runtime.Streaming.NativeSSEDeliveryIntegrationTest do
  use CodexPoolerWeb.ConnCase, async: false

  import Ecto.Query
  import CodexPoolerWeb.Runtime.BackendCodexTestSupport

  alias CodexPooler.Accounting.{Attempt, LedgerEntry, Request}
  alias CodexPooler.FakeUpstream
  alias CodexPooler.Repo

  @endpoint_path "/backend-api/codex/responses"
  @detection_timeout_ms 15_000

  for chunk_size <- [37, 4_093] do
    @chunk_size chunk_size

    test "native HTTP delivery preserves large reasoning and accounting in #{chunk_size}-byte chunks" do
      preamble =
        event("response.created", %{"type" => "response.created"}) <>
          event("response.in_progress", %{"type" => "response.in_progress"})

      reasoning =
        event("response.output_item.done", %{
          "type" => "response.output_item.done",
          "output_index" => 0,
          "item" => %{
            "type" => "reasoning",
            "id" => "rs_delivery_fixture",
            "summary" => [%{"type" => "summary_text", "text" => String.duplicate("x", 70_000)}]
          }
        })

      completed =
        event("response.completed", %{
          "type" => "response.completed",
          "response" => %{
            "id" => "resp_delivery_fixture",
            "status" => "completed",
            "usage" => %{
              "input_tokens" => 16,
              "input_tokens_details" => %{"cached_tokens" => 4},
              "output_tokens" => 5,
              "output_tokens_details" => %{"reasoning_tokens" => 2},
              "total_tokens" => 21
            }
          }
        })

      expected = preamble <> reasoning <> completed

      upstream =
        start_upstream(FakeUpstream.sse_stream(chunks(expected, @chunk_size), done: false))

      fixture = gateway_setup(upstream)
      port = start_public_endpoint!()
      correlation = "native-delivery-#{System.unique_integer([:positive])}"
      handler_id = {__MODULE__, correlation}
      on_exit(fn -> :telemetry.detach(handler_id) end)

      :ok =
        :telemetry.attach(
          handler_id,
          [:phoenix, :endpoint, :stop],
          &__MODULE__.endpoint_stopped/4,
          {self(), correlation}
        )

      response =
        Req.post!("http://127.0.0.1:#{port}#{@endpoint_path}",
          headers: [
            {"authorization", fixture.authorization},
            {"connection", "close"},
            {"x-request-id", correlation}
          ],
          json: %{
            "model" => fixture.model.exposed_model_id,
            "input" => native_text_input("synthetic delivery fixture"),
            "stream" => true
          },
          receive_timeout: @detection_timeout_ms,
          retry: false
        )

      assert response.status == 200
      assert response.body == expected

      assert_receive {:native_delivery_endpoint_stopped, ^correlation, endpoint_pid},
                     @detection_timeout_ms

      monitor = Process.monitor(endpoint_pid)
      assert_receive {:DOWN, ^monitor, :process, ^endpoint_pid, _reason}, @detection_timeout_ms

      assert [request] = Repo.all(from(r in Request, where: r.pool_id == ^fixture.pool.id))
      assert [attempt] = Repo.all(from(a in Attempt, where: a.request_id == ^request.id))
      assert request.status == "succeeded"
      assert attempt.status == "succeeded"
      assert request.usage_status == "usage_known"
      assert attempt.usage_status == "usage_known"
      assert request.retry_count == 0
      assert is_nil(request.last_error_code)
      assert is_nil(attempt.network_error_code)

      assert [settlement] =
               Repo.all(
                 from(l in LedgerEntry,
                   where: l.attempt_id == ^attempt.id and l.entry_kind == "settlement"
                 )
               )

      assert settlement.input_tokens == 16
      assert settlement.cached_input_tokens == 4
      assert settlement.output_tokens == 5
      assert settlement.reasoning_tokens == 2
      assert settlement.total_tokens == 21
      assert FakeUpstream.count(upstream) == 1
    end
  end

  def endpoint_stopped(_event, _measurements, %{conn: conn}, {parent, correlation}) do
    if conn.request_path == @endpoint_path and
         Plug.Conn.get_req_header(conn, "x-request-id") == [correlation] do
      send(parent, {:native_delivery_endpoint_stopped, correlation, self()})
    end
  end

  defp event(type, payload),
    do: "event: " <> type <> "\ndata: " <> CodexPooler.JSON.encode!(payload) <> "\n\n"

  defp chunks(<<>>, _size), do: []
  defp chunks(data, size) when byte_size(data) <= size, do: [data]

  defp chunks(data, size) do
    <<chunk::binary-size(^size), rest::binary>> = data
    [chunk | chunks(rest, size)]
  end
end
