defmodule CodexPooler.Gateway.Transports.Streaming.ReplayedPreambleTest do
  use ExUnit.Case, async: true

  alias CodexPooler.Gateway.Transports.Streaming.StreamProtocol

  @created ~S(event: response.created
data: {"type":"response.created"}

)
  @in_progress ~S(event: response.in_progress
data: {"type":"response.in_progress"}

)
  @metadata ~S(event: response.metadata
data: {"type":"response.metadata","response_id":"resp_fixture"}

)
  @delta ~S(event: response.output_text.delta
data: {"type":"response.output_text.delta"}

)
  @error ~S(event: error
data: {"type":"error","error":{"code":"server_error"}}

)

  describe "retry_window_preamble_event?/1" do
    test "names attempt-local zero-output events and excludes internal metadata" do
      assert StreamProtocol.retry_window_preamble_event?(%{data_type: "response.created"})
      assert StreamProtocol.retry_window_preamble_event?(%{data_type: "response.in_progress"})
      assert StreamProtocol.retry_window_preamble_event?(%{data_type: "response.metadata"})
      refute StreamProtocol.retry_window_preamble_event?(%{data_type: "codex.response.metadata"})

      refute StreamProtocol.retry_window_preamble_event?(%{
               data_type: "response.output_text.delta"
             })

      refute StreamProtocol.retry_window_preamble_event?(%{data_type: "error"})
      refute StreamProtocol.retry_window_preamble_event?(%{data_type: "response.completed"})
    end

    test "requires redundant SSE labels to agree before naming a preamble" do
      refute StreamProtocol.retry_window_preamble_event?(%{
               event_type: "response.created",
               data_type: "response.failed"
             })

      refute StreamProtocol.retry_window_preamble_event?(%{
               event_type: "response.failed",
               data_type: "response.created"
             })

      assert StreamProtocol.retry_window_preamble_event?(%{event_type: "response.created"})
      assert StreamProtocol.retry_window_preamble_event?(%{data_type: "response.in_progress"})
    end
  end

  describe "split_preamble_blocks/1" do
    test "drops preamble blocks and keeps everything else" do
      assert {"", true} =
               StreamProtocol.split_preamble_blocks(@created <> @in_progress <> @metadata)

      assert {@delta, true} = StreamProtocol.split_preamble_blocks(@created <> @delta)
      assert {@delta, false} = StreamProtocol.split_preamble_blocks(@delta)
    end

    test "keeps a terminal error that shares the chunk with the preamble" do
      # A fast provider failure arrives as one chunk: dropping the whole chunk
      # would swallow the error the client is owed.
      assert {@error, true} =
               StreamProtocol.split_preamble_blocks(@created <> @in_progress <> @error)
    end

    test "keeps blocks whose event and JSON labels contradict each other" do
      created_label_failed_data =
        "event: response.created\ndata: {\"type\":\"response.failed\"}\n\n"

      failed_label_created_data =
        "event: response.failed\ndata: {\"type\":\"response.created\"}\n\n"

      assert {^created_label_failed_data, false} =
               StreamProtocol.split_preamble_blocks(created_label_failed_data)

      assert {^failed_label_created_data, false} =
               StreamProtocol.split_preamble_blocks(failed_label_created_data)
    end

    test "keeps residue that is not yet a complete block" do
      residue = "event: response.output_text.delta\ndata: {\"type\":\"resp"

      assert {^residue, true} = StreamProtocol.split_preamble_blocks(@created <> residue)
    end

    test "leaves data alone when there is no preamble to strip" do
      assert {@error, false} = StreamProtocol.split_preamble_blocks(@error)
      assert {"", false} = StreamProtocol.split_preamble_blocks("")
    end
  end
end
