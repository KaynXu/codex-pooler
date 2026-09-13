defmodule CodexPooler.Telemetry.Relay do
  @moduledoc false
  import Ecto.Query
  alias CodexPooler.{Repo, Telemetry.RelayEvent}

  @heartbeat_stale_seconds 60
  @claim_lease_seconds 60

  def refresh_heartbeat(owner) when is_binary(owner) do
    case Repo.query(
           "INSERT INTO telemetry_relay_heartbeats (owner, heartbeat_at) VALUES ($1, NOW()) ON CONFLICT (owner) DO UPDATE SET heartbeat_at = EXCLUDED.heartbeat_at",
           [owner]
         ) do
      {:ok, _} -> :ok
      {:error, error} -> {:error, error}
    end
  end

  def heartbeat_fresh?(owner) when is_binary(owner) do
    case Repo.query(
           "SELECT heartbeat_at > NOW() - ($2 * INTERVAL '1 second') FROM telemetry_relay_heartbeats WHERE owner = $1",
           [owner, @heartbeat_stale_seconds]
         ) do
      {:ok, %{rows: [[fresh]]}} -> fresh
      _ -> false
    end
  end

  def insert(event, labels, count \\ 1, measurements \\ %{}, owner \\ "relay-runtime") do
    if heartbeat_fresh?(owner),
      do: do_insert(event, labels, count, measurements),
      else: {:error, :stale_heartbeat}
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
        where:
          e.inserted_at > ago(1, "hour") and
            (is_nil(e.claimed_at) or e.claimed_at < ago(^@claim_lease_seconds, "second")),
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
    Repo.delete_all(
      from e in RelayEvent,
        where:
          e.inserted_at < ago(1, "day") and
            (is_nil(e.claimed_at) or e.claimed_at < ago(60, "second"))
    )
  end
end
