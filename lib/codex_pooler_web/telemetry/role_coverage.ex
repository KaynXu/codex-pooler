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
  a live comparison have each passed *for that family*; the four shipped families
  stay `:partial` until then, and the phase-1 panels keep selecting
  `via="in_process"` so today's caveats stay true.

  Those four are `promotion_gates/0`, and a `:relayed` declaration has to name
  the evidence for each of them in a `promotion_evidence` map:
  `unmet_promotion_gates/1` returns the ones it does not. The three test gates
  name the test that proves them for that family, by file and `test` name; the
  live comparison names the record instead, because no test can stand in for a
  measurement taken on a running deployment. A single tripwire that fails on any
  `:relayed` declaration said nothing about which family had which evidence, and
  was one line for a promoting author to delete.

  `unmet_promotion_gates/1` checks the shape of that evidence. The guard —
  `CodexPoolerWeb.Telemetry.RoleCoverageTest` — additionally resolves each test
  gate: it loads the named file and looks the name up among the `ExUnit.Test`
  structs the compiled module registers, so a gate naming a file nobody wrote or
  a test nobody named fails. What that binds is existence, not proof: no check
  here can tell whether the named test proves the gate, and review still has to.

  Every guard asks `coverage_class/1` rather than matching the states it happens
  to know about, so a state added to `coverage()` is either classified or raises.
  `:unscraped_only` used to be neither: it matched no guard clause, so a family
  moved to it could have its panels unpinned — the substantive act of promotion —
  while owing no evidence and keeping a now-false `OBAN_MODE` caveat.

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

  @typedoc """
  Evidence that a family has passed each promotion gate.

  The three test gates name a test as `{relative path, test name}`; the live
  comparison names the record of a measurement taken on a running deployment.
  """
  @type promotion_evidence :: %{
          real_worker_emission: {Path.t(), String.t()},
          double_emission: {Path.t(), String.t()},
          relay_round_trip: {Path.t(), String.t()},
          live_comparison: String.t()
        }

  @typedoc "One declared event whose emissions cross the unscraped-role boundary."
  @type declaration :: %{
          :entrypoints => [module()],
          :coverage => coverage(),
          :fallback => String.t(),
          :note => String.t(),
          optional(:promotion_evidence) => promotion_evidence()
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
  # Postgres relay, so the description has to name both shares that form the
  # promoted total. `job_relay` is the relayed `via` value and `in_process` is
  # the native share; either token alone leaves the panel's total ambiguous.
  @relay_marker "job_relay"
  @in_process_marker "in_process"

  # Marker owed per coverage state. `:partial` and `:unscraped_only` describe a
  # graph the job share never reaches, so they owe the `OBAN_MODE` caveat;
  # `:relayed` describes one it does reach, so it owes both share names instead.
  # Keeping the old caveat would leave the sentence an operator reads as "this
  # is not measured here" on a graph that now measures it.
  @markers %{
    partial: @caveat_marker,
    unscraped_only: @caveat_marker,
    relayed: @relay_marker
  }

  # What each coverage state means for a panel, and the single place a guard asks.
  # A `:shadowed` family's job share is kept out of operator totals, so its panels
  # pin one share and its descriptions owe the `OBAN_MODE` caveat; a `:promoted`
  # one's is in them, so its panels may pin nothing and it owes the four promotion
  # gates. Every guard classifies through this map instead of matching the atoms
  # it happens to know, because a state no guard recognises is a way to ship the
  # substantive act of promotion — unpinning the panels — with none of the
  # evidence promotion owes: `:unscraped_only` was exactly that. A state added to
  # `coverage()` without an entry here raises rather than being skipped.
  @coverage_classes %{
    partial: :shadowed,
    unscraped_only: :shadowed,
    relayed: :promoted
  }

  @coverage_states @coverage_classes |> Map.keys() |> Enum.sort()
  @marked_coverage_states @markers |> Map.keys() |> Enum.sort()

  # What a coverage state's descriptions may not say. Requiring the new marker
  # cannot by itself force a rewrite on promotion: a `:partial` family's
  # descriptions already name the relay, because they have to say where the job
  # share arrives once it is drained. Both markers are therefore present on the
  # day a family is promoted, `marker_present?/2` is satisfied, and the now-false
  # "`OBAN_MODE`=worker or scheduler ... run no reporter" sentence survives on a
  # graph that has started carrying that share. Naming what a promoted
  # description may *not* say is what makes the replacement mandatory.
  #
  # One literal token is not enough for that, because the false claim survives
  # every rewording that drops it: "when the pooler runs as worker or scheduler,
  # no reporter runs, so this graph is empty" says the whole caveat and never
  # writes `OBAN_MODE`. What the claim cannot avoid is naming the roles it is
  # about, so a promoted description may not name them either — it has nothing
  # true left to say about what `worker` or `scheduler` does not export, since
  # the relay now carries exactly that share. The role names come from
  # `@unscraped_oban_modes` rather than from a second hand-written list, so a
  # changed role set moves both halves together.
  #
  # Matching has to be looser than an exact token and tighter than a substring.
  # A bare word boundary is too tight: `workers or schedulers` is the more
  # natural English of the two and escaped it, and so did `oban_worker`, because
  # `_` was treated as a word character. A plural `s` is allowed after the word
  # and `_` is not part of it. What no word list can close is a caveat that
  # avoids the role nouns entirely ("the job pods run no reporter"); that
  # residual is recorded rather than papered over, because the answer to it is
  # review, not a longer list.
  @forbidden_markers %{relayed: [@caveat_marker | @unscraped_oban_modes]}

  # What a family owes before it may be declared `:relayed`, each proven for that
  # family rather than for the phase. The first three are tests; the fourth is a
  # measurement on a running deployment, which no test can take.
  @promotion_gates [:real_worker_emission, :double_emission, :relay_round_trip, :live_comparison]

  # Gates whose evidence is a test, named as `{path relative to the repository
  # root, the test's own name}` so the guard can resolve it.
  @test_gates [:real_worker_emission, :double_emission, :relay_round_trip]

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

  @doc "The gates a family passes individually before it may be declared `:relayed`."
  @spec promotion_gates() :: [atom()]
  def promotion_gates, do: @promotion_gates

  @doc """
  The gates whose evidence is a `{path, test name}` pair the guard resolves.

  `CodexPoolerWeb.Telemetry.RoleCoverageTest` loads each named file and looks the
  name up among the `ExUnit.Test` structs the compiled module registers, so a
  declaration naming a test nobody wrote fails there.
  """
  @spec test_promotion_gates() :: [atom()]
  def test_promotion_gates, do: @test_gates

  @doc "Every promotion state a declaration may carry."
  @spec coverage_states() :: [coverage()]
  def coverage_states, do: @coverage_states

  @doc """
  Every state the marker functions accept.

  The classification and the markers are separate maps, and the guard asserts
  they hold the same states: a state present in one and absent from the other is
  half-covered, which is how `:unscraped_only` read as supported while matching
  no guard clause.
  """
  @spec marked_coverage_states() :: [coverage()]
  def marked_coverage_states, do: @marked_coverage_states

  @doc """
  Whether `coverage` keeps the job share out of operator totals or puts it in them.

  This is the one place a guard turns a coverage state into a rule. An
  unrecognised state raises, because the alternative is a declaration that
  matches no guard clause and is silently skipped by all of them.
  """
  @spec coverage_class(coverage()) :: :shadowed | :promoted
  def coverage_class(coverage) when is_map_key(@coverage_classes, coverage),
    do: Map.fetch!(@coverage_classes, coverage)

  @doc "Whether a family in `coverage` still keeps its relayed job share out of operator totals."
  @spec shadowed?(coverage()) :: boolean()
  def shadowed?(coverage), do: coverage_class(coverage) == :shadowed

  @doc "Whether a family in `coverage` has been promoted, so its panels carry the job share."
  @spec promoted?(coverage()) :: boolean()
  def promoted?(coverage), do: coverage_class(coverage) == :promoted

  @doc """
  The promotion gates `declaration` does not carry evidence for.

  A declaration whose coverage state is not promoted owes nothing, so the list is
  empty. A promoted one owes all of `promotion_gates/0`: a test gate is met by a
  `{path, test name}` pair of non-empty strings, the live comparison by a
  non-empty string naming the record.

  This is the shape half. Whether the named test exists under that name in that
  file is resolved by `CodexPoolerWeb.Telemetry.RoleCoverageTest`, which loads the
  file and reads the registered `ExUnit.Test` names out of the compiled module —
  a check this module cannot make, because only the guard runs against the
  repository. Neither half claims the named test *proves* its gate; that is what
  review is for.

  An unrecognised coverage state raises rather than returning `[]`: a state no
  guard classifies is a way to unpin the panels while owing no evidence at all.
  """
  @spec unmet_promotion_gates(declaration()) :: [atom()]
  def unmet_promotion_gates(%{coverage: coverage} = declaration) do
    if promoted?(coverage) do
      evidence = Map.get(declaration, :promotion_evidence) || %{}
      Enum.reject(@promotion_gates, &gate_met?(&1, Map.get(evidence, &1)))
    else
      []
    end
  end

  defp gate_met?(gate, {path, name}) when gate in @test_gates,
    do: present?(path) and present?(name)

  defp gate_met?(:live_comparison, record), do: present?(record)
  defp gate_met?(_gate, _evidence), do: false

  defp present?(value), do: is_binary(value) and String.trim(value) != ""

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

  @doc "The primary marker a declaration in `coverage` owes its metric and panel descriptions."
  @spec required_marker(coverage()) :: String.t()
  def required_marker(coverage) when is_map_key(@markers, coverage),
    do: Map.fetch!(@markers, coverage)

  @doc """
  Whether `description` carries every marker a declaration in `coverage` owes an operator.

  A `:partial` family owes the exact `OBAN_MODE` caveat token, because its graph
  is missing whatever the job emitted. A `:relayed` family owes both share names:
  `in_process` and `job_relay`. Naming only the relayed share does not tell an
  operator whether the panel charts one share or their promoted total.
  """
  @spec marker_present?(term(), coverage()) :: boolean()
  def marker_present?(description, coverage) when is_binary(description) do
    coverage
    |> required_markers()
    |> Enum.all?(&exact_marker_present?(description, &1))
  end

  def marker_present?(_description, coverage) when is_map_key(@markers, coverage), do: false

  defp required_markers(coverage) do
    case coverage_class(coverage) do
      :shadowed -> [@caveat_marker]
      :promoted -> [@in_process_marker, @relay_marker]
    end
  end

  @doc """
  The words a declaration in `coverage` may not carry, empty when it may say anything else.

  Matched as whole words and case-insensitively, so a reworded caveat that drops
  the `OBAN_MODE` token but still tells an operator what `worker` or `scheduler`
  does not export is caught with it.
  """
  @spec forbidden_markers(coverage()) :: [String.t()]
  def forbidden_markers(coverage) when is_map_key(@markers, coverage),
    do: Map.get(@forbidden_markers, coverage, [])

  @doc """
  Whether `description` carries every marker `coverage` owes and omits the one it retires.

  This is the check a guard should use. `marker_present?/2` alone passes a
  promoted family whose description still tells an operator its job share is not
  measured here, because a `:partial` description names the relay already.
  """
  @spec markers_correct?(term(), coverage()) :: boolean()
  def markers_correct?(description, coverage) when is_map_key(@markers, coverage),
    do: marker_present?(description, coverage) and not marker_forbidden?(description, coverage)

  defp marker_forbidden?(description, coverage) do
    Enum.any?(forbidden_markers(coverage), &claim_word_present?(description, &1))
  end

  # Required markers are identifiers, not English nouns. Treat `_` as part of
  # the identifier so `OBAN_MODE_worker`, `in_process_total` and
  # `job_relay_total` cannot satisfy an exact-token contract.
  defp exact_marker_present?(description, marker) when is_binary(description) do
    Regex.match?(
      ~r/(?<![A-Za-z0-9_])#{Regex.escape(marker)}(?![A-Za-z0-9_])/i,
      description
    )
  end

  # Forbidden role words deliberately have looser English boundaries: plural
  # forms and snake_case compounds still express the false promotion caveat.
  defp claim_word_present?(description, word) when is_binary(description) do
    Regex.match?(~r/(?<![A-Za-z0-9])#{Regex.escape(word)}s?(?![A-Za-z0-9])/i, description)
  end

  defp claim_word_present?(_description, _word), do: false
end
