defmodule Mix.Tasks.DevRendezvousVectorTest do
  use ExUnit.Case, async: false

  alias CodexPooler.Gateway.Routing.BridgeRing
  alias Mix.Tasks.Dev.RendezvousVector

  @seeds ["seed-a", "seed-b", "correlator-42", "smoke-vector"]
  @assignment_ids [
    "11111111-1111-4111-8111-111111111111",
    "22222222-2222-4222-8222-222222222222",
    "33333333-3333-4333-8333-333333333333",
    "44444444-4444-4444-8444-444444444444"
  ]

  test "task emits deterministic scores from the bridge-ring implementation" do
    previous_shell = Mix.shell()
    Mix.shell(Mix.Shell.Process)

    on_exit(fn ->
      Mix.shell(previous_shell)
      Mix.Task.reenable("dev.rendezvous_vector")
    end)

    Mix.Task.reenable("dev.rendezvous_vector")
    RendezvousVector.run([])

    assert_receive {:mix_shell, :info, [json]}

    assert %{"version" => 1, "vectors" => vectors} = CodexPooler.JSON.decode!(json)

    assert Enum.map(vectors, &{&1["seed"], &1["assignment_id"]}) ==
             for(seed <- @seeds, assignment_id <- @assignment_ids, do: {seed, assignment_id})

    assert Enum.all?(vectors, fn vector ->
             expected_score =
               vector["seed"]
               |> BridgeRing.rendezvous_score(vector["assignment_id"])
               |> Integer.to_string()

             vector["score"] == expected_score
           end)
  end
end
