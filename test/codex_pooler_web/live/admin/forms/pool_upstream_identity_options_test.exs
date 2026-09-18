defmodule CodexPoolerWeb.Admin.PoolUpstreamIdentityOptionsTest do
  use CodexPooler.DataCase, async: false

  import CodexPooler.AccountsFixtures
  import CodexPooler.PoolerFixtures

  alias CodexPooler.Accounts.Scope
  alias CodexPooler.Repo
  alias CodexPoolerWeb.Admin.PoolForm

  test "Pool forms offer paused accounts for reassignment and exclude deleted accounts" do
    %{user: owner} = bootstrap_owner_fixture(%{"email" => unique_user_email()})
    active = active_upstream_identity_fixture(%{account_label: "Active account"})
    paused = active_upstream_identity_fixture(%{account_label: "Paused account"})
    deleted = active_upstream_identity_fixture(%{account_label: "Deleted account"})

    paused
    |> Ecto.Changeset.change(%{status: "paused"})
    |> Repo.update!()

    deleted
    |> Ecto.Changeset.change(%{status: "deleted"})
    |> Repo.update!()

    assert {options, []} = PoolForm.upstream_identity_options(Scope.for_user(owner))

    assert Enum.any?(options, &(&1.value == active.id and &1.status == "active"))
    assert Enum.any?(options, &(&1.value == paused.id and &1.status == "paused"))
    refute Enum.any?(options, &(&1.value == deleted.id))
  end
end
