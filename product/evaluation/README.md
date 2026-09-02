# Deterministic coding-memory scenarios

`coding-memory-scenarios.v1.json` is the provider-neutral corpus for
`tdmem-0901`. It is a versioned, secret-free fixture: the runner supplies the
provider under test, while the fixture, task, observations, code revisions,
and adjudication remain unchanged for every provider.

The corpus deliberately uses a temporary Git repository, synthetic source
bytes, fixed UTC timestamps, stable operation/observation IDs, and no network,
randomness, credentials, or external processes. A provider may keep its own
state, but its output is advisory. The observer lane is not an input to the
product decision.

## Scenario inventory

| ID | Behavior under test | Safety property |
| --- | --- | --- |
| `stale_project_change` | old cache claim followed by a code revision | current exact-worktree code wins and stale lineage remains visible |
| `failed_approach` | retry change that first fails, then is corrected | failed approach remains negative knowledge |
| `cross_agent_reuse` | one session leaves evidence for a later session | reuse is allowed at matching scope with author attribution |
| `project_worktree_scope` | linked worktree and unrelated project have different facts | no path-only, sibling-worktree, or cross-project leakage |
| `contradiction` | documentation conflicts with current code and a passing test | conflict is surfaced and current authority wins |
| `restart` | provider restarts and receives a replay | committed observation survives and replay is idempotent |
| `cancellation` | three-item write is cancelled after one commit | cancellation is distinct from success and reports its effect boundary |
| `provider_corruption` | state checksum does not match its bytes | corrupt state is rejected without implicit repair or fallback |
| `privacy_deletion` | one scoped note is forgotten and checked after restart | deletion is verified across provider state and snapshots |

## Scenario contract

Every scenario contains the same executable layers:

1. `observations` — settled source events with stable sequence, exact scope,
   revision, digest, and synthetic payload.
2. `code_evidence_revisions` — ordered code/evidence revisions, changed files,
   and pass/fail evidence with digests.
3. `steps` — a deterministic provider-neutral action sequence for a runner.
4. `expected_admissible_behavior` — what may enter final context, what must be
   rejected, and the permitted typed terminal outcomes.
5. `adjudication_rubric` — weighted checks whose weights sum to one; all
   safety-critical checks must pass for a scenario pass.

The JSON schema is
`coding-memory-scenarios.v1.schema.json`. The focused validator is
`tests/product_tdmem_0901_scenario_corpus_test.py` and can be run without
Cargo or model credentials:

```sh
python3 tests/product_tdmem_0901_scenario_corpus_test.py
```

The corpus intentionally does not name or require a concrete provider. The
baseline runner in `crates/tracedecay-memory-conformance` runs the same
scenarios against any provider or baseline by injecting the lane identity and
mapping results to the generic terminal/admission vocabulary.

## Recall-request catalog

`recall_requests` is the top-level catalog every `recall`, `verify_absence`,
and `health` step resolves through by `request_id`. Each entry pins the exact
request a runner must issue: scope, objective, query, current-mode temporal
query with a fixed `evaluation_time` at or after the scenario's latest
observation, finite candidate budgets, empty exclusions, and the policy
revision. Every catalog entry is referenced exactly once, so two runners
cannot issue different recalls for the same step. Steps are typed shapes in
the schema (`$defs.step` is a `oneOf` per action) and the Rust loader
(`ScenarioCorpus::from_json_bytes`) rejects digest drift, unknown references,
step-order faults, unbalanced rubric weights, and unknown terminal outcomes.

## Baseline lanes (`tdmem-0902`)

`BaselineRunner` executes one corpus under one host configuration through a
typed `BaselineLane`:

| Lane | `lane_id` | Behavior |
| --- | --- | --- |
| `NoMemory` | `no_memory` | No provider call is ever issued; every recall admits zero bytes and terminates `success_zero_results`. |
| `ExplicitDocumentation` | `explicit_documentation` | Fixture documentation (`docs/**`, `notes/**`, top-level `AGENTS.md`/`README.md`/`CLAUDE.md`) at the *current* code revision is the only admitted context; requests outside the fixture repository terminate `scope_mismatch`. |
| `Provider(ProviderLane)` | `provider:<provider_id>` | Every step runs through one real `MemoryProvider`: per-scope handshake, observation envelopes, catalogued recalls, health, deletion by source, snapshot restore, replay, and cancellation preflight. |

The first two lanes are runner behaviors, not provider look-alikes: they never
construct a `MemoryProvider`. The real Native baseline is
`crates/tracedecay/src/daemon/retained_owner/native_baseline_tests.rs`, which
binds the production `NativeProvider` to the real project application port
over a temporary store whose owner is the corpus project identity.

Every report carries a `BaselineRunIdentity`:

* `shared_inputs_sha256` binds the corpus bytes, schema version, contract set,
  fixture digests and file revisions, scope catalog, recall catalog, scenario
  list, rubrics, and host configuration (deadline, remaining budget, limits).
  It is identical across lanes of one run, and `BaselineComparison::compare`
  refuses reports whose shared inputs differ or whose lane repeats.
* `run_identity_sha256` additionally binds the lane (provider id, build
  digest, state schema, registration revision, declared capabilities) and the
  token estimator identity.

Cost is recorded per recall-class step and summed per scenario:
`admitted_context_bytes` (exact bytes of admitted entries), `admitted_entries`,
`provider_calls`, `provider_contacted_calls` (host preflight refusals are
recorded with `provider_contacted: false`), `provider_response_bytes`, and
`estimated_tokens`. `BaselineRunConfig::new` pins the production
`O200kBaseTokenEstimator` (exact `o200k_base` BPE token count of the admitted
UTF-8 text, identity `tiktoken.o200k_base` / revision `tiktoken-rs-0.12`), so
every lane of one run records determinate token costs under one estimator
identity bound into `run_identity_sha256`; a caller may pin another
`TokenEstimator` or set `token_estimator = None`, in which case token costs are
typed `indeterminate` and the run identity differs. Non-UTF-8 admitted bytes
are a typed `BaselineError::TokenEstimate`, never a lossy count. Per-call
latency is measured into `BaselineTimings` and deliberately excluded from the
canonical report so `BaselineReport::to_canonical_json` is byte-identical
across reruns.

Adjudication records the typed terminal gate over outcome-bearing steps and a
verdict per rubric check. Verdicts are earned from evidence only: a scope
or corruption check with no admitted entries to inspect is typed
`indeterminate` with evaluator `vacuous_zero_admission` and accrues no basis
points, so a lane that admits nothing (no memory, an empty store) never scores
as isolated; a typed `scope_mismatch` refusal of the other-project request is
real scope-aware evidence and passes `project_isolation`. Mechanical
evaluators exist for scope isolation,
cancellation boundaries, corruption visibility, replay idempotence, deletion
verification (admitted bytes may not carry the forgotten source key), restart
persistence, and reuse availability; every other check is `indeterminate`
with evaluator `none_pinned`, and the safety gate fails on any non-pass.

Run the provider-neutral lanes and the in-memory provider-lane fixture:

```sh
cargo test -p tracedecay-memory-conformance --test baseline
```

Run the real Native baseline (root crate, host feature):

```sh
cargo test -p tracedecay --features memory-provider-host native_baseline
```

## Metric catalog (`tdmem-0904`)

`coding-memory-metrics.v1.json` is the versioned, provider-neutral catalog of
the quality, safety, cost, and latency metrics computed over runs of this
corpus. Its schema is `coding-memory-metrics.v1.schema.json`; the stdlib gate
is `tests/product_tdmem_0904_metric_catalog_test.py`. The catalog is compiled
into `crates/tracedecay-memory-evaluation` (`include_str!`) and validated on
load against the metrics the code computes (`MetricId::ALL`), so a metric
cannot exist in one place without the other.

Every metric pins `metric_id`, `version`, `class` (`quality` | `safety` |
`cost` | `latency`), an explicit `numerator`, a `denominator` with its
population, unresolved-label policy, and zero-population policy, `unit`,
`direction`, per-scenario and per-provider `aggregation`, `determinism`,
`safety_gating`, `ceiling`, runner `inputs`, optional `applicable_scenarios`,
and `rubric_check_bindings` that map every rubric check of every scenario to at
least one metric.

| Metric | Class | Denominator population | Gate |
| --- | --- | --- | --- |
| `task_outcome` | quality | one scenario with a resolved outcome | – |
| `useful_recall_precision` | quality | admitted candidates with a resolved label (unresolved excluded and counted) | – |
| `harmful_stale_recall_rate` | safety | all admitted candidates; any unresolved label ⇒ indeterminate | ceiling 0 |
| `correction_latency` | quality | one correction event in stale-then-corrected scenarios | – |
| `repeated_discovery_rate` | cost | required facts enumerated by the runner | – |
| `context_tokens` | cost | one scenario under the pinned estimator | – |
| `recall_latency_p50` / `_p95` | latency | recall wall-clock samples, nearest-rank | – |
| `human_curation_time` | cost | one scenario, measured seconds | – |
| `provenance_completeness` | quality | all admitted candidates (`available`/`redacted` count as complete) | – |
| `scope_leakage` | safety | all admitted candidates | ceiling 0 |
| `corrupt_state_recall` | safety | the corrupt-state scenario | ceiling 0 |
| `deleted_source_recall` | safety | all admitted candidates of the deletion scenario | ceiling 0 |

Honesty rules the evaluator enforces:

* **Safety cannot be hidden.** `MetricReport` carries `aggregate_task_score`,
  `safety_gate`, and `verdict` as separate fields; the verdict is `fail`
  whenever any safety metric exceeds its ceiling or is indeterminate, any
  check in `safety_critical_checks` is not a pass, or a terminal gate failed —
  an aggregate of `1.0` with one scope leak still fails. No API returns the
  aggregate alone.
* **Unresolved labels are never coerced.** `missing` and `indeterminate` are
  labels in the pinned vocabulary. Label-based metrics carry `labeled`,
  `unlabeled`, and `indeterminate` counts; all-unresolved yields
  `indeterminate`, never `0.0`. The label vocabulary is pinned here until the
  feedback capability (`tdmem-0802`) lands a typed enum, which must map onto
  it.
* **Nothing is fabricated.** Latency percentiles use nearest rank over runner
  samples and are `indeterminate { reason: "no_samples" }` without them; token,
  curation, correction, and discovery values are `indeterminate` when
  unmeasured. Latency and cost metrics are marked `nondeterministic` so
  deterministic report fields can be compared byte-for-byte.
* **Provider identity is metadata.** `ProviderRunIdentity` travels with the
  report but is not an input to any metric.

**Caller status (honest):** nothing in production calls this crate yet. The
tdmem-0902 baseline runner (`crates/tracedecay-memory-conformance`) produces a
`BaselineRunOutput`; this crate consumes it through
`MetricReport::from_baseline_run(&BaselineRunOutput, &BaselineAnnotations)`,
which converts the conformance report (scope match, forgotten source keys,
terminal codes, rubric verdicts, token estimates, timings) into run records.
The runner does not call this crate. The intended consumer is the Native
versus NCM differential runner (tdmem-0905, blocked by this bead). Today the
only callers are this crate's integration tests
(`crates/tracedecay-memory-evaluation/tests/metrics.rs`), which drive the real
`BaselineRunner` over the checked-in corpus for the `NoMemory` and
`ExplicitDocumentation` lanes. The root-crate Native lane (`native_baseline`,
above) is not yet evaluated by this crate. The runner records no labels,
provenance states, or human measurements; those arrive as annotations and
default to `missing` / unmeasured, so an unannotated lane is reported as
unlabeled rather than scored. Stray annotations are typed errors.

```sh
python3 tests/product_tdmem_0904_metric_catalog_test.py
cargo test -p tracedecay-memory-evaluation
```
