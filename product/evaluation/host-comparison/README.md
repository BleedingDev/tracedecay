# Host comparison preparation

This directory defines the frozen comparison inputs and the downstream fixture
connection. It contains **no measured Native/NCM comparison**. Controlled tests
use independent development truth; no held-out provider answers or paid model
runs were used. Downstream agent task benefit remains unmeasured.

`input-freeze.json` records the SHA-256 of each untouched held-out catalog and
the predeclared quality ceilings. The accepted protocol is
`.codex/plans/pm-comparison-protocol.md`. The schedule is 18 cases, 56 queries,
two hosts, four lanes and three paired trials: 432 case trials and 1,344 query
attempts. Every host/lane has 54 case trials and 168 queries. Positive-control
IDs identify assertions; they never request extra calls.

## Commands

Run from the checkout root. Output paths must not exist; commands preserve prior
artifacts and never replace a failed trial with a retry.

```sh
python3 -S -m unittest discover -s scripts/product/memory-comparison -p 'test_*.py' -v
python3 -S scripts/product/memory-comparison/runner.py prepare --output /chosen/output/plan.json
```

The preparation command only reads frozen inputs. It declares cold, warm,
observe, lifecycle, snapshot, queue, cancellation and observer populations in
addition to the held-out schedule. It runs no provider, model, daemon or task.

After `pm-comparison-connect` supplies and verifies a production factory:

```sh
python3 -S scripts/product/memory-comparison/runner.py replay --metadata /chosen/output/metadata.json --driver production_comparison:factory --output /chosen/output/captures.jsonl
python3 -S scripts/product/memory-comparison/runner.py assemble --metadata /chosen/output/metadata.json --captures /chosen/output/captures.jsonl --output /chosen/output/capture-report.json
python3 -S scripts/product/memory-comparison/adjudicate.py /chosen/output/capture-report.json /chosen/output/blinded-annotations.json --output-directory /chosen/output/metric-inputs
cargo run -p tracedecay-memory-evaluation --example host_comparison_metrics -- /chosen/output/metric-inputs/catalog.json /chosen/output/metric-inputs/claude-provider_ncm-0.json
```

Invoke the Rust example once for each of the 24 generated inputs and retain its
JSON output with the input's host/lane/trial. `join_reports.py` consumes an array
of `{host, lane, trial, report}` objects and the same capture/annotations to
produce the paired retrieval report:

```sh
python3 -S scripts/product/memory-comparison/join_reports.py /chosen/output/capture-report.json /chosen/output/blinded-annotations.json /chosen/output/metric-reports.json --output /chosen/output/retrieval-report.json
python3 -S scripts/product/memory-comparison/runner.py measurements --metadata /chosen/output/metadata.json --captures /chosen/output/measurement-captures.jsonl --output /chosen/output/performance-report.json
cargo test -p tracedecay-memory-evaluation --all-targets
```

Cargo/model/process scheduling belongs to the lead. The original nine-scenario
`BaselineRunOutput` regression and `MetricReport::from_baseline_run` remain
unchanged. The new held-out IDs use a catalog derived from the same definitions
with explicit scenario/check bindings; they are never sent to the embedded
nine-scenario catalog.

## Production connection API

`scripts/product/memory-comparison/runner.py` defines the only comparison seam:

```python
factory = production_comparison.factory(metadata)
with output_path.open("x") as output:
    runner.replay(plan, metadata, factory, JsonlEventSink(output))
```

The runner invokes one case/host/lane/trial at a time in the frozen counterbalanced
order. `invocation` has `case_trial_id`, `host`, `lane`, `trial`, `case_id`,
`case_input_sha256`, the **whole unchanged case**, the ordered `actions`, shared
`budgets`, tokenizer identity and reviewed `pins`. Each action has `action_id`,
the exact frozen `step`, and its full `source` and/or `query` if referenced.
Operation/idempotency IDs are determined by logical trace position. A replay
reuses the original keys. Identical logical inputs remain identical across
lanes; physical namespace/path mappings are execution metadata.

The fixture owns native host transcript projection, isolated profile/project
stores, startup/readiness, hook dispatch, durable settlement and final host
response capture. It must record `consumed_case` from the trace it actually
consumed, its `projection_sha256`, `canonical_start_sha256`, shared `budgets`,
actual initial `selected_provider` pin, `observer_enabled: false`, and `mode`.
Return `case_trial_id`, `status`, and one `actions` result per scheduled action.
`comparison-capture.schema.json` describes that interchange. Captures are
evidence from trusted fixture code, not provider self-reports.

Each action result includes the unchanged `input`, actual `selected_provider`,
`status` (`completed`, `censored`, `unexecuted`), canonical terminal or null,
`provider_contacted`, `successful_completion`, direct `phase`/`timing`, and
`delivery` or null. Preserve requested/effective budgets, deadline at dispatch,
remaining budget, durable effects, queue telemetry, source rejection reasons,
process identity and all internal spans in additional fields. Unsupported actions
and missing phase instrumentation remain unresolved. A field cannot turn a
backend reply into host evidence.

The runner passes the fixture a `Capture` bound to the durable event sink. Call
`capture.record_metadata(actual_metadata)` before the first action, then
`capture.record_action(result)` immediately after each observed action and before
starting the next action or cleanup. Each append flushes and fsyncs before
returning. Completed/censored rows returned without durable events are rejected.
The fixture must represent an in-flight
operation at its cutoff as censored, including elapsed-at-cutoff, before raising.
The runner has no authority to guess which action a crashed fixture had started.
It records every remaining scheduled action/query as unexecuted with the cause.
The JSONL stream contains `case_begin`, `case_metadata`, `action`, and
`case_finish` events. `event_log.read_captures` folds these into case captures.
It recovers complete action events when a process is killed before case finish
or during cleanup, retaining their original status, terminal and timing plus
actual fixture metadata. An unterminated final line is excluded only within the
16 MiB line bound and its byte count is reported. Malformed complete lines,
oversized fragments and rewritten/duplicate durable actions fail explicitly.
A final case must retain every durably recorded metadata field unchanged,
including canonical state/projection, namespace/path, process/build/mode and
selection. Omitted fields are recovered from the durable metadata. The original
`case_begin` invocation is retained as `requested_invocation`; a conflicting
final invocation is rejected.
The recovered case lacks verified cleanup; it cannot claim comparative success.
Partial/invalid captures remain in `raw_case_captures`; identifiable prior
attempts retain their timing and failure denominators and are excluded from
comparative conclusions.

`close()` returns `status: completed`, `remaining_owned_children: 0` and the
individual exit evidence. Fixture exceptions or incomplete cleanup stop further
process creation while retaining every remaining planned row. A verified safety
failure or exhausted budget can set `stop_remaining_reason` with its evidence.

The fixture handles in-flight cancellation while replaying the case; a `cancel`
step targets the existing recall and creates no second query. It delegates
physical actions to the reviewed conformance/host controls:

| Logical action | Existing producer boundary |
| --- | --- |
| `observe` | Native hook/canonical import and durable observation settlement |
| `recall` | Production selected provider, host admission/hydration/selection/packing and **final merge** |
| `correct`, `delete_source`, feedback, snapshot, replay | Reviewed provider-neutral host lifecycle controls and canonical source/disposition authority |
| `restart` | `FixtureEnvironmentAction::Restart` and physical restart evidence |
| `corrupt_state` | `CorruptPayloadPreservingDigest` and measured before/after artifact evidence |
| `switch_provider` | Existing host selection control, fenced canonical replay and readiness; role is relative to the initial lane |
| shutdown | Owned-process/worker join and actual exit evidence |

These are routing obligations. This harness creates no history grant, source
disposition checkpoint, synthetic authorization, provider adapter, host policy,
or substitute successful host reply. The common `FixtureEnvironmentEvidence`
must be retained by reference in action evidence. Switch steps validate the
actual per-action provider against the requested alternate/initial pin. Primary
observer input remains disabled. The no-memory/documentation lanes cannot call
an advisory provider. Documentation must retain a current revision in `docs/`,
`notes/`, or the permitted top-level Markdown files and use the same quota.

## Delivered content and annotations

`delivery` has exact UTF-8 `final_text`, raw SHA-256, tokenizer identity,
`final_tokens`, `canonical_tokens`, `advisory_tokens`, `candidate_body_tokens`,
ordered `sections`, and a complete `candidates` ledger. Sections concatenate
exactly to final delivery; canonical sections preserve their source refs.
Canonical bytes and attribution must match for paired lanes before a delta is
interpreted. Source scores and declared scales stay in raw evidence and are never
subtracted across providers.

Each candidate records its stable reference, emitted body/digest, source IDs,
revision, lineage, original origin scope, full original-source content digest,
host scope/provenance result, each `returned/admitted/selected/packed/delivered`
stage, and any withholding reason. The original canonical SourceAttribution and
host receipts should be retained in additional fields. A delivered advisory
section references exactly one delivered candidate; a body must occur in that
section, and the section text must equal that complete annotated body. Standalone
`framing` is restricted to ASCII whitespace; it cannot hide unannotated facts.
Non-whitespace production wrappers require an explicit reviewed mapping from
the actual renderer during connection work. Never remove wrapper bytes or token
counts to make a capture fit. Rejected, unhydrated or finally withheld candidates cannot receive a
metric annotation. Token counts are not additive across arbitrary text joins:
Rust independently recounts final text, joined canonical sections, joined
advisory sections and the sum of individual delivered bodies with the existing
`O200kBaseTokenEstimator` (`tiktoken.o200k_base`, `tiktoken-rs-0.12`).

Annotations are a JSON array. Each entry joins `(case_trial_id, request_id,
candidate_ref)` and includes the existing `label`, nonempty `reason`,
`reviewer_id`, `provider_blinded: true`, and `fact_evidence`. A useful label also
requires `prohibited_claims_absent: true`. A fact-evidence item has a frozen
`fact_index`, eligible `source_id`, exact `source_quote`, and `delivered_quote`.
The reviewer checks semantic support against frozen truth; the harness checks
that evidence references and quotes actually exist. It never assigns usefulness
from query-word overlap. Only faithful, correct-scope, provenance-complete,
eligible Useful facts cover a query's required facts. Irrelevant or unverifiable
annotations carrying fact references do not satisfy the query. Missing labels remain missing; disagreement remains
indeterminate. A Useful label means source-grounded retrieval, not measured task
benefit. The inherited `task_outcome` is explicitly a retrieval-rubric outcome.

Safety controls need useful positive delivery and physical challenge evidence.
`control_evidence` contains `exercised`, `verified`, and independent `evidence_refs`.
Deletion/corruption also bind `target_source_ids`, a prior `positive_action_id`,
and later `unaffected_action_id`. Each witness must be that exact action's
completed, comparison-valid, passing query adjudication with eligible Useful
source evidence. Useful output from another query cannot supply the witness.
Receipt targets must equal the actual frozen action's source selector or the
complete source set selected by its lineage. Receipts cannot choose a different
target to fit available positive evidence. A corruption action without an
identifiable frozen target remains indeterminate.
Corruption retains actual/claimed before/after
digests; deletion retains `deletion_fence_ref` and `derived_state_erased`.
Restart/switch retain `old_process_exited` and zero remaining old owned children.
Cancellation retains `cancellation_completed`, `released_permits`, and
`remaining_cancelled_children`. Missing evidence cannot pass a safety check.
Candidates after corrupt-state injection need the independently observed
`from_corrupt_state` state; absent attribution stays unmeasured.

Optional `correction_effectiveness` spans must identify the verifying recall,
phase `correction_submission_to_verified_recall`, direct start/end times and
elapsed time. Every correction needs a span; the single existing per-scenario
correction metric uses the largest complete span, while all individual spans
remain in raw measurements. Missing/censored correction spans stay unmeasured.

## Measurement and reporting boundaries

`Capture` exposes unchanged reviewed `AttemptTiming`, `ProcessTreeCapture`,
`ProcessIdentity`, and `capture_directories`. The fixture instruments actual
boundaries; the runner does not time `create()`/`replay()` and rename that time
host recall. The cold startup, first request and spawn-to-first-delivery spans
share the same 20 process trials per state. Provider/host spans of warm requests
also share attempts. They are separate phase observations, not extra executions.

`measurements.py::planned_attempts` declares every measured and warm-up row with
host/lane/population/phase/block/index. Submit exactly one capture per ID. All
missing IDs remain unexecuted. No retries, hidden expansion, sample clipping or
adaptive budgets are permitted. Keep source failure/refusal/deadline/cancellation
outcomes and producer effect accounting. A return with live cancelled children
does not complete cancellation cleanup.

Sample only owned process identities every 20 ms. Retain host/worker RSS, tree
RSS, high-water counters, actual gaps, CPU cost, worker counts/exits and shared-page
limitations. Preserve signed paired deltas, including negative RSS increments.
Disk captures retain distinct DB/WAL/SHM, snapshots, model/cache, canonical
stores/journals and host ledgers at before-startup, after-observation,
after-maintenance, after-snapshot and after-shutdown boundaries. Shared immutable
model bytes are counted once. Missing queue/disk/RSS values remain null with
reasons, not zero.

Every population publishes planned/attempted/provider-contacted/success/failure/
deadline/cancelled/censored/unexecuted/warm-up counts and their terminal cross-tab.
Successful-completion and all-terminal latencies are separate. Target ranks use
scheduled attempts: fast failures cannot pass; censored samples retain lower/
upper bounds. p50/p95/p99 use nearest rank, with min/max/MAD/IQR. Steady support
requires 100 completed successes. Paired process/case block intervals use 2,000
deterministic resamples, seed 6010317; missing pairs yield incomplete intervals.
Hosts, phases and populations remain separate. Case/query scheduled denominators
and unresolved counts accompany the existing metric ratios and descriptive
Wilson intervals. The 0.60 quality gate is distinct from existing safety gates.

Each evaluator run identity binds the canonical complete metric input before its
self-identity is assigned, the exact consumed case captures and annotations,
host/lane/trial, freeze and execution metadata. Changing a delivery or annotation
invalidates a previously generated metric report at the strict identity join.

Execution metadata requires commands, source revision, dirty diff digest,
build/features/runtime versions, hardware/OS/RAM/power/load, provider build/state
schema/effective limits, pinned MiniLM artifact hashes and tokenizer. Controlled
mode remains visibly `controlled_preparation`. Only the downstream verified
factory may report `production_host`; merely declaring that mode is not proof
that production host paths executed.

## Pending real-host connection handoff

The current reusable comparison boundary is `HostFactory`/`HostFixture` above.
There is not yet a callable `production_comparison:factory`. The real host code
available for extraction is
`crates/tracedecay-cli/tests/product_memory_provider_claude_host_journey.rs`.
Its `ClaudeHostJourney`, `ActiveProvider`, and methods are private to that test
target. The file exercises Claude and Codex with independently active Native and
real NCM, but the scenario assertions are not a comparison driver. In particular,
`assert_host_memory_journey_with_provider` adds a baseline context request,
replayed hooks, and multiple verification recalls. Calling that assertion from a
scheduled case would add unscheduled actions.

The minimal handoff from the host fixture owner is an executable entry point
that accepts one unchanged runner invocation and exposes the existing three
operations: create, replay, and close. The host fixture owns the actual operations
below; the Python connection only transports the invocation, durably records its
results through `Capture`, and preserves cleanup evidence. No second provider
API, replacement host renderer, or new authorization format is required.

| Required control | Existing code to reuse | Gap before comparison use |
| --- | --- | --- |
| Isolated setup and selected provider | `start`, `cli`, `commit_provider_gates`, `commit_real_ncm`, `initialize_registered_project` | Accept the invocation's lane and source projection; support no-memory and documentation controls; expose actual selection/effective limits rather than echoing requested pins. |
| Readiness without extra recalls | `wait_for_authority`, `await_startup_history`, `startup_sync_status` | Replace the assertion's baseline context call with verified mount/readiness evidence; retain any startup retries as setup evidence. |
| Native transcript projection and hook dispatch | `claude_turn`, `run_session_start_event`, `run_hook`, `run_codex_stop_hook` | Accept the scheduled source, session, revision and logical operation IDs; remove fixed journey text and fixed turn counts. Preserve a mapping to the exact canonical records the hook admitted. |
| Durable observation evidence | `await_settled_journal_for`, `capture_original_hook_sources`, `admitted_scope` | Return each scheduled action's actual admitted source identities, content/revision attribution and receipts; the current assertions assume exactly four messages. |
| Ordinary selected-provider recall | `tool_result` calling `tracedecay_context` | Return the unchanged raw result and actual final delivery/stage evidence for one scheduled query. Expose direct phase timings and terminal outcomes, including refusals and deadlines, rather than asserting success. |
| Restart and session rebinding | `stop_daemon`, `start_daemon`, `run_session_start_event` | Preserve exit/status evidence and prove old owned workers have ended; bind the requested session without creating an extra recall or observation. |
| Source/lifecycle challenges | Reviewed public source, feedback, provider-selection and environment controls | Supply their concrete callable entry points and actual target/result evidence. The private fixed journey does not expose a general correction, deletion, corruption, snapshot, replay or in-flight cancellation driver. |
| Final cleanup | `stop_daemon`, `Drop` | Return bounded daemon/worker joins and exit evidence; the current `kill`/`wait` results are discarded. |

For a subprocess entry point, the host owner must also specify its executable,
arguments, transport, and failure/cleanup behavior. It must publish the actual
case metadata before starting any scheduled action. After each action outcome,
the connection calls `capture.record_action` and completes its durable append
before allowing the fixture to begin another action or cleanup. A synchronous
one-action control channel or an explicit acknowledgment on an event stream can
provide that ordering. Merely tailing an independently advancing log cannot.
This transport is not another source of host authority.

The host fixture receives the entire ordered action list so an in-flight recall
and its scheduled cancellation can share the same operation. It records a
censored action at the actual cutoff before raising, and never retries a query
to obtain a successful result. A control it cannot execute returns an unresolved
outcome with the original action input and reason. It cannot substitute provider
conformance output for missing host output. The runner retains remaining
unexecuted rows and stops further creation if cleanup is not proved.

The response boundary needs explicit confirmation during extraction.
`tool_result` obtains the CLI's raw JSON stdout, while `tool` calls
`join_content_text`, which strips `tracedecay_metrics:` lines, joins text blocks,
and parses their JSON. Those convenience transformations cannot establish exact
final delivered bytes. The host owner must identify the actual delivered field
or block sequence, preserve the raw envelope separately, and expose the final
merge's candidate attribution. If actual delivery includes non-whitespace
renderer wrappers, their attribution requires review before connection; the
adapter must not strip them or label them as whitespace framing.

Once that concrete entry point exists, `production_comparison:factory` can bind
it without changing the frozen schedule or interchange. The bounded connection
smoke uses independent development inputs and one positive and negative control
per host/provider, verifies actual selected-provider attribution, nonempty
admitted evidence, real restart and exact tokenizer counts, and retains failed
attempts. Root schedules its build/process/model work. Held-out output and full
performance populations remain downstream work.
