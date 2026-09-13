defmodule CodexPooler.Telemetry.Relay do
  import Ecto.Query
  alias CodexPooler.{Repo, Telemetry.RelayEvent}

  def insert(event, labels, count \\ 1) do
    %RelayEvent{}
    |> RelayEvent.changeset(%{
      event: event,
      labels: labels,
      count: count,
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
