defmodule CodexPooler.SchemaSecretRedactionTest do
  use ExUnit.Case, async: true

  # Every persisted or virtual field that carries secret material must be
  # `redact: true`, so neither a struct nor a changeset inspect renders it.
  # Three schemas were found by hand-grep in one day (findings#215, #221);
  # this pins the class. Hashes, fingerprints and counts are not secrets.
  @secret_field ~r/(ciphertext|nonce|_secret$|^secret_|password|verifier|_token$|^token$|api_key$|auth_json)/
  @not_secret ~r/(_hash$|_prefix$|_fingerprint$|_count$|_at$|_id$|_version$|_kind$|_action$|_aad$|^aad$|_status$|_required$)/

  # The Pool traffic gate's owner token is a per-LiveView fencing correlator
  # that grants nothing without the authenticated operator scope it is always
  # paired with. Session and bridge lease tokens are not in that class: the
  # code compares them in constant time and persists their digest, never the
  # token, so they are redacted like every other capability.
  @internal_fencing_tokens [{CodexPooler.Admin.PoolTrafficGate, :owner_token}]

  {:ok, modules} = :application.get_key(:codex_pooler, :modules)

  schemas =
    Enum.filter(modules, fn module ->
      {:ok, {^module, [exports: exports]}} = :beam_lib.chunks(:code.which(module), [:exports])
      {:__schema__, 1} in exports
    end)

  for module <- schemas do
    @tag schema_module: module
    test "#{inspect(module)} redacts every secret-shaped schema field", %{schema_module: module} do
      offenders =
        for field <- module.__schema__(:fields) ++ module.__schema__(:virtual_fields),
            name = Atom.to_string(field),
            Regex.match?(@secret_field, name),
            not Regex.match?(@not_secret, name),
            field not in module.__schema__(:redact_fields),
            {module, field} not in @internal_fencing_tokens,
            do: {module, field}

      assert offenders == [],
             "secret-shaped fields without `redact: true`: #{inspect(offenders)}"
    end
  end
end
