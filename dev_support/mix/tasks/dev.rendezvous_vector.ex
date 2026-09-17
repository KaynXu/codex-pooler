defmodule Mix.Tasks.Dev.RendezvousVector do
  @moduledoc """
  Prints deterministic, metadata-only rendezvous scores from the Pooler router.

      MIX_ENV=test mix dev.rendezvous_vector

  The sibling smoke suite uses this task to compare its JavaScript rendezvous
  implementation with `CodexPooler.Gateway.Routing.BridgeRing.rendezvous_score/2`.
  """

  use Mix.Task

  alias CodexPooler.Gateway.Routing.BridgeRing

  @requirements ["app.config"]
  @shortdoc "Print deterministic bridge-ring rendezvous vectors"

  @seeds ["seed-a", "seed-b", "correlator-42", "smoke-vector"]
  @assignment_ids [
    "11111111-1111-4111-8111-111111111111",
    "22222222-2222-4222-8222-222222222222",
    "33333333-3333-4333-8333-333333333333",
    "44444444-4444-4444-8444-444444444444"
  ]

  @impl Mix.Task
  def run([]) do
    vectors =
      for seed <- @seeds, assignment_id <- @assignment_ids do
        %{
          seed: seed,
          assignment_id: assignment_id,
          score: seed |> BridgeRing.rendezvous_score(assignment_id) |> Integer.to_string()
        }
      end

    Mix.shell().info(CodexPooler.JSON.encode!(%{version: 1, vectors: vectors}))
  end

  def run(_args), do: Mix.raise("usage: mix dev.rendezvous_vector")
end
