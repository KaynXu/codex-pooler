defmodule CodexPooler.Platform.Readiness do
  @moduledoc """
  Whether this node can actually serve, as a database fact.

  One query against `schema_migrations` answers two different questions, and
  they are deliberately not treated the same way:

    * Schema state. Every migration version this image carries must be applied.
      A missing or incomplete schema is permanent until an operator acts (a
      replaced or wiped database, a migration job that never ran, a rollback),
      the node cannot serve a single request, and waiting changes nothing, so
      readiness is withdrawn immediately.

    * Connectivity. A dropped connection is usually transient and self-healing,
      and every node sees it at the same instant, so withdrawing readiness on
      the first failure turns a brief blip into a Service with no endpoints at
      all. Once this node has been ready, connectivity failures are tolerated
      for a fixed grace window before readiness is withdrawn. Before the first
      success they are not: a node that has never reached the database has
      never been in the Service, so refusing it costs no endpoints.

  The grace window is a constant rather than an operator control. It describes
  how long a probe is willing to wait for a fact it cannot yet read, and a node
  that cannot read that fact must not be configurable into claiming otherwise.

  A successful probe proves connectivity and schema together, so the schema
  check can never be starved by an unreachable database: an unreachable
  database is classified as connectivity, never as a missing schema.
  """

  alias CodexPooler.Repo

  # Long enough to ride out a PostgreSQL failover or a connection-pool stall
  # (both commonly 10-30s) without withdrawing every endpoint at once, short
  # enough that a node which is genuinely cut off stops claiming to serve.
  @grace_ms 30_000
  @probe_timeout_ms 1_000

  @migrations_table "schema_migrations"

  @state_key {__MODULE__, :probe_state}
  @ever_ready_slot 1
  @last_success_slot 2

  @schema_error_codes [:undefined_table, :undefined_column, :invalid_schema_name]

  @type class :: String.t()
  @type outcome :: :ready | {:ready, :degraded, class()} | {:not_ready, class()}

  @doc """
  Reports whether this node can serve.

  Returns `:ready`, `{:ready, :degraded, class}` when a connectivity failure is
  being tolerated inside the grace window, or `{:not_ready, class}`. The class
  is a bounded, sanitized token derived from an error module name, a PostgreSQL
  error code, or a fixed vocabulary; it never carries a database message.

  `:now_ms` and `:sql_probe` exist so the grace window and the probe outcome can
  be driven directly in tests; callers in the release pass neither.
  """
  @spec check(keyword()) :: outcome()
  def check(opts \\ []) do
    now_ms = Keyword.get(opts, :now_ms, System.monotonic_time(:millisecond))

    case probe(opts) do
      :ok ->
        record_success(now_ms)
        :ready

      {:error, :schema, class} ->
        {:not_ready, class}

      {:error, :connectivity, class} ->
        if within_grace?(now_ms) do
          {:ready, :degraded, class}
        else
          {:not_ready, class}
        end
    end
  end

  @doc """
  The grace window, in milliseconds, that a connectivity failure is tolerated
  for after this node has been ready at least once.
  """
  @spec grace_ms() :: pos_integer()
  def grace_ms, do: @grace_ms

  @doc false
  @spec reset_state!() :: :ok
  def reset_state! do
    ref = state()
    :atomics.put(ref, @ever_ready_slot, 0)
    :atomics.put(ref, @last_success_slot, 0)
    :ok
  end

  # A node that has never reached the database holds no endpoint, so there is
  # nothing to protect by tolerating its failures.
  defp within_grace?(now_ms) do
    ref = state()

    :atomics.get(ref, @ever_ready_slot) == 1 and
      now_ms - :atomics.get(ref, @last_success_slot) <= @grace_ms
  end

  defp record_success(now_ms) do
    ref = state()
    :atomics.put(ref, @last_success_slot, now_ms)
    :atomics.put(ref, @ever_ready_slot, 1)
    :ok
  end

  # Monotonic time is signed and its zero point is arbitrary, so the "has this
  # node ever been ready" fact needs its own slot rather than a sentinel value.
  defp state do
    case :persistent_term.get(@state_key, nil) do
      nil ->
        ref = :atomics.new(2, signed: true)
        :persistent_term.put(@state_key, ref)
        ref

      ref ->
        ref
    end
  end

  defp probe(opts) do
    {statement, params, required} = probe_statement(expected_versions())

    case sql_probe(opts).query(Repo, statement, params, timeout: @probe_timeout_ms) do
      {:ok, %{rows: [[count]]}} when is_integer(count) and count >= required ->
        :ok

      {:ok, %{rows: [[count]]}} when is_integer(count) ->
        {:error, :schema, "migrations_missing"}

      {:ok, _result} ->
        {:error, :schema, "migrations_unreadable"}

      {:error, reason} ->
        classify(reason)
    end
  end

  # Applied versions this image does not know about are a newer release's
  # migrations, which are normal mid-rollout and must not unready the pods that
  # are still serving the old one, so the check is containment, not equality.
  defp probe_statement([_ | _] = versions) do
    {"SELECT count(*) FROM #{@migrations_table} WHERE version = ANY($1)", [versions],
     length(versions)}
  end

  # No readable migration files: verify the schema exists and is not empty
  # rather than treating an unverifiable build as verified.
  defp probe_statement([]) do
    {"SELECT count(*) FROM #{@migrations_table}", [], 1}
  end

  defp classify(%Postgrex.Error{postgres: %{code: code}}) when code in @schema_error_codes,
    do: {:error, :schema, Atom.to_string(code)}

  defp classify(reason), do: {:error, :connectivity, reason_class(reason)}

  @doc """
  A bounded, sanitized token naming the kind of failure.

  Exception structs collapse to their module name and tagged reasons to their
  tag; database messages, which can quote statements and parameters, never
  reach a log line through this.
  """
  @spec reason_class(term()) :: class()
  def reason_class(%module{}) when is_atom(module), do: inspect(module)
  def reason_class(reason) when is_atom(reason), do: Atom.to_string(reason)
  def reason_class({reason, _details}) when is_atom(reason), do: Atom.to_string(reason)
  def reason_class(_reason), do: "unknown"

  defp sql_probe(opts) do
    Keyword.get_lazy(opts, :sql_probe, fn ->
      :codex_pooler
      |> Application.get_env(__MODULE__, [])
      |> Keyword.get(:sql_probe, Ecto.Adapters.SQL)
    end)
  end

  # Reading the migration directory is filesystem work on a path that cannot
  # change while the node runs, so the answer is memoized for the VM.
  defp expected_versions do
    case :persistent_term.get({__MODULE__, :expected_versions}, nil) do
      nil ->
        versions = read_expected_versions()
        :persistent_term.put({__MODULE__, :expected_versions}, versions)
        versions

      versions ->
        versions
    end
  end

  defp read_expected_versions do
    path = Ecto.Migrator.migrations_path(Repo)

    case File.ls(path) do
      {:ok, entries} ->
        entries
        |> Enum.flat_map(&migration_version/1)
        |> Enum.sort()

      {:error, _reason} ->
        []
    end
  end

  defp migration_version(entry) do
    case Integer.parse(entry) do
      {version, "_" <> rest} ->
        if String.ends_with?(rest, ".exs"), do: [version], else: []

      _other ->
        []
    end
  end
end
