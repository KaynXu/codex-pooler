defmodule CodexPooler.Telemetry.Relay do
  import Ecto.Query
  alias CodexPooler.{Repo, Telemetry.RelayEvent}

  @heartbeat_stale_seconds 60

  def refresh_heartbeat(owner) when is_binary(owner) do
    Repo.query("INSERT INTO telemetry_relay_heartbeats (owner, heartbeat_at) VALUES ($1, NOW()) ON CONFLICT (owner) DO UPDATE SET heartbeat_at = EXCLUDED.heartbeat_at", [owner])
    :ok
  end

  def heartbeat_fresh?(owner) when is_binary(owner) do
    case Repo.query("SELECT heartbeat_at > NOW() - INTERVAL '60 seconds' FROM telemetry_relay_heartbeats WHERE owner = $1", [owner]) do
      {:ok, %{rows: [[fresh]]}} -> fresh
      _ -> false
    end
  end

  def insert(event, labels, count \\ 1, measurements \\ %{}) do
    if not heartbeat_fresh?("relay-runtime"), do: {:error, :stale_heartbeat}, else: do_insert(event, labels, count, measurements)
  end

  defp do_insert(event, labels, count, measurements) do
    %RelayEvent{}
    |> RelayEvent.changeset(%{
      event: event,
      labels: labels,
      count: count,
      measurements: measurements,
      inserted_at: DateTime.utc_now()
    })
    |> Repo.insert()
  end

  def claim(limit \\ 100, owner \\ "relay") do
    Repo.transaction(fn ->
      Repo.query!("SET LOCAL statement_timeout = '5s'")

      from(e in RelayEvent,
        where: is_nil(e.claimed_at) and e.inserted_at > ago(1, "hour"),
        order_by: [asc: e.inserted_at],
        limit: ^limit,
        lock: "FOR UPDATE SKIP LOCKED"
      )
      |> Repo.all()
      |> Enum.map(
        &Repo.update!(
          Ecto.Changeset.change(&1, claimed_at: DateTime.utc_now(), claimed_by: owner)
        )
      )
    end)
  end

  def expire_counted do
    Repo.delete_all(
      from e in RelayEvent, where: is_nil(e.claimed_at) and e.inserted_at < ago(1, "hour")
    )
  end

  def prune do
    Repo.delete_all(from e in RelayEvent, where: e.inserted_at < ago(1, "day"))
  end
end
