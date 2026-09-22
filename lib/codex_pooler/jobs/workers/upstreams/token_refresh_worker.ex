defmodule CodexPooler.Jobs.TokenRefreshWorker do
  @moduledoc """
  Refreshes encrypted upstream access tokens for one upstream identity.
  """

  use Oban.Worker,
    queue: :jobs,
    max_attempts: 8,
    tags: ["token_refresh"],
    unique: [
      fields: [:args, :queue, :worker],
      keys: [:upstream_identity_id],
      states: :incomplete,
      period: {7, :days}
    ]

  alias CodexPooler.Upstreams.Auth.TokenRefresh

  # The ceiling the exponential backoff already tops out at, so a provider
  # interval cannot park a job for longer than an ordinary retry would.
  @max_snooze_seconds 3_600

  @impl Oban.Worker
  def timeout(%Oban.Job{}), do: :timer.seconds(45)

  @impl Oban.Worker
  def perform(%Oban.Job{args: %{"upstream_identity_id" => identity_id} = args}) do
    trigger_kind = Map.get(args, "trigger_kind", "manual")

    case TokenRefresh.refresh_access_token(identity_id,
           trigger_kind: trigger_kind
         ) do
      # A provider that named its own interval outranks the exponential backoff
      # below, which would otherwise burn attempts against a deadline we were
      # already told about.
      {:ok, %{retryable?: true, retry_after_seconds: seconds}} when is_integer(seconds) and seconds > 0 ->
        {:snooze, min(seconds, @max_snooze_seconds)}

      {:ok, %{retryable?: true, reason: reason}} ->
        {:error, reason}

      {:ok, %{status: :active}} ->
        :ok

      {:ok, %{status: status}} when status in [:reauth_required, :noop] ->
        :discard

      {:error, :refresh_in_progress, _metadata} ->
        {:snooze, 5}

      {:error, reason} ->
        {:error, reason}
    end
  end

  @impl Oban.Worker
  def backoff(%Oban.Job{attempt: attempt}) do
    min(trunc(:math.pow(2, attempt) * 30), 3_600)
  end
end
