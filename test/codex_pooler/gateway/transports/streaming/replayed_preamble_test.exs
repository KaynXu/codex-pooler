defmodule CodexPooler.Gateway.Transports.Streaming.ReplayedPreambleTest do
  use ExUnit.Case, async: true

  alias CodexPooler.Gateway.Transports.Streaming.StreamProtocol

  @created "event: response.created\ndata: {\"type\":\"response.created\"}\n\n"
  @in_progress "event: response.in_progress\ndata: {\"type\":\"response.in_progress\"}\n\n"
  @delta "event: response.output_text.delta\ndata: {\"type\":\"response.output_text.delta\"}\n\n"
  @error "event: error\ndata: {\"type\":\"error\",\"error\":{\"code\":\"server_error\"}}\n\n"

  describe "retry_window_preamble_event?/1" do
    test "names the two zero-output events and nothing else" do
      assert StreamProtocol.retry_window_preamble_event?(%{data_type: "response.created"})
      assert StreamProtocol.retry_window_preamble_event?(%{data_type: "response.in_progress"})

      refute StreamProtocol.retry_window_preamble_event?(%{
               data_type: "response.output_text.delta"
             })

      refute StreamProtocol.retry_window_preamble_event?(%{data_type: "error"})
      refute StreamProtocol.retry_window_preamble_event?(%{data_type: "response.completed"})
    end
  end

  describe "split_preamble_blocks/1" do
    test "drops preamble blocks and keeps everything else" do
      assert {"", true} = StreamProtocol.split_preamble_blocks(@created <> @in_progress)
      assert {@delta, true} = StreamProtocol.split_preamble_blocks(@created <> @delta)
      assert {@delta, false} = StreamProtocol.split_preamble_blocks(@delta)
    end

    test "keeps a terminal error that shares the chunk with the preamble" do
      # A fast provider failure arrives as one chunk: dropping the whole chunk
      # would swallow the error the client is owed.
      assert {@error, true} =
               StreamProtocol.split_preamble_blocks(@created <> @in_progress <> @error)
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
