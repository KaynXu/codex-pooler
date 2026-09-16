defmodule CodexPoolerWeb.Telemetry.RoleCoverage do
  @moduledoc """
  Declares which telemetry events this application emits from an `OBAN_MODE`
  role whose series never reach Prometheus.

  `CodexPoolerWeb.Telemetry.prometheus_reporter_enabled?/0` switches the
  Prometheus reporter off for `OBAN_MODE=worker` and `OBAN_MODE=scheduler`, and
  the public Helm chart's `ServiceMonitor` selects only the `app` pods, so on a
  split-role deployment an event emitted from a job never becomes a series. The
  event fires, in-process handlers see it, a focused test is green and a panel
  renders — and the graph is empty forever. An operator reads that as "this
  never happens" rather than "this is not measured here".

  Nothing at the point of declaration says so: a counter for a job-run event
  looks exactly like a counter for a request event. This module is the place
  where it does say so, and `CodexPoolerWeb.Telemetry.RoleCoverageTest` derives
  the same set from the compiled application and fails when the two disagree,
  so the next metric in this shape cannot arrive unnoticed.

  ## What a declaration asserts

  Each entry names the event, the Oban worker modules whose `perform/1` can
  reach an emission site for it, whether web traffic also emits it, and the
  durable rows an operator reads instead. Those `entrypoints` are checked
  against the derived call graph too, so a second worker reaching an
  already-declared event also fails the guard.

  ## Promotion

  `coverage` is the promotion state, and it decides which sentence the metric
  and its panels owe an operator. A `:partial` family's job share never becomes
  a series, so every description has to name `OBAN_MODE`. A `:relayed` family's
  job share arrives through the Postgres relay as `via="job_relay"`, so the
  `OBAN_MODE` caveat would then be false and the descriptions owe the relay
  marker instead. The guard reads that marker from the coverage state rather
  than from a fixed constant, so flipping one family to `:relayed` without
  rewriting its descriptions and panels fails.

  A `:partial` description already names the relay, because it has to say where
  the job share goes once something drains it, so *requiring* the relay marker
  on promotion demands nothing new. Each state therefore also names a marker its
  descriptions may not carry: a promoted family's may not say `OBAN_MODE` at
  all, which is what forces the false caveat out. Use `markers_correct?/2`, not
  `marker_present?/2`, wherever a guard decides whether a description is honest.

  Promotion is not a rename. A family moves to `:relayed` only once its
  real-worker emission test, its double-emission pins, its relay round trip and
  a live comparison have each passed for that family; the four shipped families
  stay `:partial` until then, and the phase-1 panels keep selecting
  `via="in_process"` so today's caveats stay true.

  ## What the derivation cannot see

  The derived call graph is intra-process: local calls, remote calls, function
  captures and inline closures, walked from each Oban worker's `perform/1`.
  That matches where `:telemetry.execute/3` runs, because telemetry is emitted
  in the calling process, and a `Task` started by a job still runs on the job's
  node. It deliberately does not follow a message to another process: a
  `GenServer.call/3` into a session owner is served wherever that owner lives,
  which is usually a web node, and treating it as a worker emission produced a
  false member during the audit. The cost of that choice is a blind spot for a
  process started by the supervision tree on every role and messaged from a
  job; when a metric is emitted from such a callback, declare it here by hand
  with `entrypoints: []` and say so in the note.

  The scheduler role runs Oban's cron, lifeline and pruner plugins and no
  application emitter of its own: cron *inserts* the jobs that the worker role
  then executes. Both roles are unscraped, so the conclusion is unchanged, but
  a declaration names the worker that runs the job rather than the scheduler
  that enqueued it.
  """

  @typedoc "How much of an event's traffic reaches Prometheus."
  @type coverage :: :partial | :unscraped_only | :relayed

  @typedoc "One declared event whose emissions cross the unscraped-role boundary."
  @type declaration :: %{
          entrypoints: [module()],
          coverage: coverage(),
          fallback: String.t(),
          note: String.t()
        }

  # Mirrors the modes `CodexPoolerWeb.Telemetry.prometheus_reporter_enabled?/0`
  # refuses to start the reporter for. The test asserts the two agree by calling
  # that function rather than by reading its source, so changing the gate
  # without revisiting this list fails.
  @unscraped_oban_modes ~w(worker scheduler)

  # A metric and panel description for a declared event has to name the gate
  # that empties it. `OBAN_MODE` is that name: it is what an operator greps for
  # and what the chart sets, and no honest caveat avoids it.
  @caveat_marker "OBAN_MODE"

  # A promoted family owes the opposite sentence. Its graph is no longer empty
  # on a split-role deployment, because the job share arrives through the
  # Postgres relay, so the description has to say which share an operator is
  # looking at and that the relayed one is best effort. `job_relay` is that
  # name: it is the `via` label value the relayed samples carry, so it is both
  # what an operator greps for and what a panel selector has to mention to
  # include or exclude the job share.
  @relay_marker "job_relay"

  # Marker owed per coverage state. `:partial` and `:unscraped_only` describe a
  # graph the job share never reaches, so they owe the `OBAN_MODE` caveat;
  # `:relayed` describes one it does reach, so it owes the relay marker instead.
  # Demanding both of a promoted family would keep the sentence an operator
  # reads as "this is not measured here" on a graph that now measures it.
  @markers %{
    partial: @caveat_marker,
    unscraped_only: @caveat_marker,
    relayed: @relay_marker
  }

  # The marker a coverage state's descriptions may not carry. Requiring the new
  # marker cannot by itself force a rewrite on promotion: a `:partial` family's
  # descriptions already name the relay, because they have to say where the job
  # share arrives once it is drained. Both markers are therefore present on the
  # day a family is promoted, `marker_present?/2` is satisfied, and the now-false
  # "`OBAN_MODE`=worker or scheduler ... run no reporter" sentence survives on a
  # graph that has started carrying that share. Naming what a promoted
  # description may *not* say is what makes the replacement mandatory.
  @forbidden_markers %{relayed: @caveat_marker}

  @unscraped_emissions %{
    [:codex_pooler, :instance_presence, :heartbeat] => %{
      entrypoints: [],
      coverage: :partial,
      fallback: "instance_presences.last_seen_at",
      note:
        "InstanceHeartbeat is supervised on every release role, outside the Oban perform call graph. " <>
          "OBAN_MODE=worker and scheduler do not run the reporter, so only web/all failures are exported."
    },
    [:codex_pooler, :accounting, :reservation, :pre_attempt_release] => %{
      entrypoints: [CodexPooler.Jobs.RuntimeStateCleanupWorker],
      coverage: :partial,
      fallback: "the request ledger's pre_attempt_phase detail",
      note:
        "The runtime cleanup job releases reservations no live turn ever reached and stamps " <>
          "them stale_sweep, so that phase is exported only under OBAN_MODE=all. Every other " <>
          "phase is released on the request path and is exported normally."
    },
    [:codex_pooler, :saved_reset, :convergence] => %{
      entrypoints: [
        CodexPooler.Jobs.AccountReconciliationWorker,
        CodexPooler.Jobs.SavedResetRedemptionWorker
      ],
      coverage: :partial,
      fallback: "the upstream identity saved_reset_redemption lifecycle metadata",
      note:
        "The redemption finalizer emits convergence, and it runs both on the request path and " <>
          "inside the redemption and reconciliation jobs. The rate and latency panels chart " <>
          "the web share only."
    },
    [:codex_pooler, :quota, :cycle, :decision] => %{
      entrypoints: [
        CodexPooler.Jobs.AccountReconciliationWorker,
        CodexPooler.Jobs.AlertEvaluationWorker,
        CodexPooler.Jobs.SavedResetRedemptionWorker
      ],
      coverage: :partial,
      fallback: "the account quota window rows and their evidence history",
      note:
        "Account reconciliation is the bulk writer of quota evidence and decides cycles while " <>
          "persisting it; saved-reset redemption classifies post-reset evidence and alert " <>
          "evaluation reads routing snapshots, and both reject superseded primary windows on " <>
          "the way. All three run as jobs, so the decision counter under-reports by however " <>
          "much reconciliation does."
    },
    [:codex_pooler, :gateway, :stream, :outcome] => %{
      entrypoints: [CodexPooler.Jobs.RuntimeStateCleanupWorker],
      coverage: :partial,
      fallback: "the accounting request and attempt rows for interrupted turns",
      note:
        "Recovering an expired owner lease settles the turns it abandoned and emits their " <>
          "interrupted outcome from the runtime cleanup job, so the interrupted slice is " <>
          "under-counted while the outcomes settled on the request path are complete."
    }
  }

  @doc "OBAN_MODE values whose processes run no Prometheus reporter."
  @spec unscraped_oban_modes() :: [String.t()]
  def unscraped_oban_modes, do: @unscraped_oban_modes

  @doc "Substring every metric and panel description for a declared event must contain."
  @spec caveat_marker() :: String.t()
  def caveat_marker, do: @caveat_marker

  @doc "The declared events, each with the roles that emit them and the durable fallback."
  @spec unscraped_emissions() :: %{[atom()] => declaration()}
  def unscraped_emissions, do: @unscraped_emissions

  @doc "Telemetry events declared to have at least one emission on an unscraped role."
  @spec declared_events() :: [[atom()]]
  def declared_events, do: Map.keys(@unscraped_emissions)

  @doc "Whether `event` is declared to emit on an unscraped role."
  @spec declared?([atom()]) :: boolean()
  def declared?(event) when is_list(event), do: Map.has_key?(@unscraped_emissions, event)

  @doc "Substring every metric and panel description for a promoted (relayed) event must contain."
  @spec relay_marker() :: String.t()
  def relay_marker, do: @relay_marker

  @doc "The declared coverage state of `event`, or `nil` when it is not declared."
  @spec coverage_for([atom()]) :: coverage() | nil
  def coverage_for(event) when is_list(event) do
    case Map.fetch(@unscraped_emissions, event) do
      {:ok, %{coverage: coverage}} -> coverage
      :error -> nil
    end
  end

  @doc "The substring a declaration in `coverage` owes its metric and panel descriptions."
  @spec required_marker(coverage()) :: String.t()
  def required_marker(coverage) when is_map_key(@markers, coverage),
    do: Map.fetch!(@markers, coverage)

  @doc "The substring `event`'s metric and panel descriptions owe, or `nil` when undeclared."
  @spec required_marker_for([atom()]) :: String.t() | nil
  def required_marker_for(event) when is_list(event) do
    case coverage_for(event) do
      nil -> nil
      coverage -> required_marker(coverage)
    end
  end

  @doc """
  Whether `description` carries the caveat a declared event's metric and panels owe an operator.
  """
  @spec caveat_present?(term()) :: boolean()
  def caveat_present?(description) when is_binary(description),
    do: String.contains?(description, @caveat_marker)

  def caveat_present?(_description), do: false

  @doc """
  Whether `description` carries the marker a declaration in `coverage` owes an operator.

  A `:partial` family owes the `OBAN_MODE` caveat, because its graph is missing
  whatever the job emitted. A `:relayed` family owes the relay marker instead,
  because its graph now carries the job share under `via="job_relay"` and the
  old caveat would misdescribe it.
  """
  @spec marker_present?(term(), coverage()) :: boolean()
  def marker_present?(description, coverage) when is_binary(description),
    do: String.contains?(description, required_marker(coverage))

  def marker_present?(_description, coverage) when is_map_key(@markers, coverage), do: false

  @doc """
  The substring a declaration in `coverage` may not carry, or `nil` when it may say anything else.
  """
  @spec forbidden_marker(coverage()) :: String.t() | nil
  def forbidden_marker(coverage) when is_map_key(@markers, coverage),
    do: Map.get(@forbidden_markers, coverage)

  @doc """
  Whether `description` both carries the marker `coverage` owes and omits the one it retires.

  This is the check a guard should use. `marker_present?/2` alone passes a
  promoted family whose description still tells an operator its job share is not
  measured here, because a `:partial` description names the relay already.
  """
  @spec markers_correct?(term(), coverage()) :: boolean()
  def markers_correct?(description, coverage) when is_map_key(@markers, coverage),
    do: marker_present?(description, coverage) and not marker_forbidden?(description, coverage)

  defp marker_forbidden?(description, coverage) do
    case forbidden_marker(coverage) do
      nil -> false
      marker -> String.contains?(description, marker)
    end
  end
end
