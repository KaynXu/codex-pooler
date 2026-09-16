defmodule CodexPooler.Gateway.Runtime.Streaming.TerminalOutcome do
  @moduledoc false

  alias CodexPooler.Gateway.Transports.Streaming.StreamProtocol

  # A preamble event does not close the window: the provider accepted the turn
  # and produced no output, so a terminal failure behind it is still safe to
  # serve on another candidate. A terminal outcome always closes it, preamble
  # or not, because it is the thing being classified.
  @spec retry_window_event(map()) :: {:ok, map()} | nil
  def retry_window_event(event) when is_map(event) do
    cond do
      not is_nil(StreamProtocol.terminal_outcome_event(event)) -> {:ok, event}
      StreamProtocol.retry_window_preamble_event?(event) -> nil
      StreamProtocol.downstream_visible_event?(event) -> {:ok, event}
      true -> nil
    end
  end

  @spec direct_retry_window_event({:ok, map()} | :incomplete) :: {:ok, map()} | :incomplete
  def direct_retry_window_event({:ok, event}) do
    if StreamProtocol.downstream_visible_event?(event) and
         not StreamProtocol.retry_window_preamble_event?(event),
       do: {:ok, event},
       else: :incomplete
  end

  def direct_retry_window_event(:incomplete), do: :incomplete
end
