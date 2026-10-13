# Biomem-based Rust NCM: complete implementation DAG

Prepared 2026-09-05. Planning only; no source implementation or tracker edits.

# Implementation DAG — real Rust NCM from Biomem

**Decision:** build a native Rust implementation under the existing NCM boundary. Do not swap in a Python subprocess, a flat-vector mock or another scaffold and call the provider complete.

**Scope:** `BleedingDev/tracedecay`, reviewed checkpoint `25778c7443cd0cfe257da363da01a56ea1d45d3f`. Reference: `BleedingDev/biomem` at `500847ff65b5d9548b3826fa29bf3ccf8d221147`. This package does not assume that `techsioCZ/ontos` has the same tree.

**Deliverable shape:** 26 implementation tasks, four external host gates, two new Rust packages, one existing NCM adapter extended. Tasks 001–022 can reach backend acceptance while another session fixes the host. Tasks 023–026 are deliberately blocked on host evidence.

The package contains a JSON DAG, one precise specification per task, a design contract, this overview, a full Mermaid graph, source manifest and a stdlib DAG validator. Nothing has been imported into GitHub/Beads or implemented by the planner.

## Findings that materially change the plan

The existing NCM integration was closed as descoped, not delivered; the earlier 0701 audit is a separate completed item. New Rust implementation work should use new tasks and reference those historical decisions. [S15]

Biomem is a useful executable reference, but not an unquestionable oracle. In inspected code, blur is a no-op; write-width configuration is not passed to the RBF call; text recall merges by key text; reads mutate counters; load failures are logged rather than propagated; the snapshot omits automatic-consolidation cadence. Projection alignment and terrain coordinates need discriminating tests before being called defects. [S03–S08]

The current NCM namespace includes session identity. Dropping that field to make cross-session recall work would change authorization semantics. Preserve the boundary; introduce an explicitly authorized replay/share bridge at the host join. [S14]

These findings are based on static source inspection. The DAG requires actual reference executions and negative controls, not a claim that every suspected runtime failure has already been reproduced.

## Work waves

| Wave | Tasks | Result / available parallelism |
|---|---|---|
| A. Establish truth | 001; then 002 and 003; then 004 | Pinned source/oracle, defect decisions, stable contracts and isolated workspace |
| B. Build independently | 005, 008, 012, 013 | Projection/numeric, terrain, real encoder and durable storage can run concurrently after their prerequisites |
| C. Complete core | 006 → 007; 009 → 010; 011 | Actual reads/writes, time, sleep and source-aware candidates |
| D. Make effects real | 014; then 015 and 017; 016 after 015; 018 | Durable engine, privacy, restart/snapshot, process isolation and real adapter |
| E. Earn backend acceptance | 019, 020, 021 in parallel; then 022 | Independent conformance, boundedness, fidelity/usefulness evidence; no host wait |
| F. Join safely | 023 → 024 → 025 → 026, each gated | Stabilized host, Observer, guarded active, installed release verdict |

Within one coding session, execute these in topological order. With subagents, assign one owner per ready file-disjoint slice. Do not spawn one worker per task unconditionally. The verifier and performance/evaluation tasks should be reviewed independently of the implementation they judge.

## Full task index

All IDs below have prefix `ncm-rs-`. Dependencies are blocking edges; historical Beads relationships are context, not completion edges.

| ID | Task | Depends on | External gate |
|---|---|---|---|
| [001](#ncm-rs-001) | Pin reference, research provenance, and new work authorization | — | — |
| [002](#ncm-rs-002) | Build an executable Python oracle and defect-disposition matrix | 001 | — |
| [003](#ncm-rs-003) | Freeze backend contracts, scope policy, effects and resource budgets | 001 | — |
| [004](#ncm-rs-004) | Create isolated Rust packages and compile-time interfaces | 003 | — |
| [005](#ncm-rs-005) | Implement numerical primitives and persisted projection bundle | 002, 004 | — |
| [006](#ncm-rs-006) | Implement center reads: RBF, hybrid selection and compound keys | 005 | — |
| [007](#ncm-rs-007) | Implement center writes, allocation and bounded source support | 006 | — |
| [008](#ncm-rs-008) | Implement actual 3D terrain, diffusion, sampling and Gaussian blur | 002, 004 | — |
| [009](#ncm-rs-009) | Implement explicit logical time, homeostasis and fatigue | 007, 008 | — |
| [010](#ncm-rs-010) | Implement consolidation, merge/prune and LTM-only survival | 009 | — |
| [011](#ncm-rs-011) | Implement record-aware recall, correction and provenance output | 007, 003 | — |
| [012](#ncm-rs-012) | Implement the real native multilingual text encoder | 002, 004 | — |
| [013](#ncm-rs-013) | Implement provider-owned durable store and source capsules | 004 | — |
| [014](#ncm-rs-014) | Implement end-to-end engine transactions and exactly-once effects | 010, 011, 012, 013 | — |
| [015](#ncm-rs-015) | Implement deletion through mixed state and bounded maintenance | 014 | — |
| [016](#ncm-rs-016) | Implement versioned export/restore with non-resurrection | 015 | — |
| [017](#ncm-rs-017) | Implement the supervised Rust worker and bounded wire transport | 014 | — |
| [018](#ncm-rs-018) | Implement the real NcmCognitiveSurface adapter | 016, 017 | — |
| [019](#ncm-rs-019) | Run real-provider conformance and adversarial scope/privacy tests | 018 | — |
| [020](#ncm-rs-020) | Measure bounded resource use, latency and saturation behavior | 018 | — |
| [021](#ncm-rs-021) | Evaluate scientific fidelity and actual coding-memory usefulness | 018, 002 | — |
| [022](#ncm-rs-022) | Accept the independently usable backend and package its evidence | 019, 020, 021 | — |
| [023](#ncm-rs-023) | Join the stabilized host and reconcile shared ownership | 022 | HOST-STABLE |
| [024](#ncm-rs-024) | Mount NCM in Observer mode and prove authorized session reuse | 023 | HOST-OBSERVE |
| [025](#ncm-rs-025) | Enable guarded active NCM and run joined comparative evaluation | 024 | HOST-RECALL |
| [026](#ncm-rs-026) | Verify installed artifacts, platform support and release decision | 025 | HOST-RELEASE |

## External join contracts

### HOST-STABLE

Owner: **Existing stabilization agent**. Initially unsatisfied. Required evidence:

- Accepted host commit/tree with retention live-source safety and eventual convergence demonstrated, including offline/unseated coverage.
- Architecture policy tests and real dependency graph checked; ownership/upstream convergence reconciled.
- Relevant cc-482/cc-483 groups, correctly selected pointer-history regression, workspace/features and follow-up performance checks executed; no empty test selections.

### HOST-OBSERVE

Owner: **Host observation/integration owner**. Initially unsatisfied. Required evidence:

- Current joined host genuinely ingests source observations and supports durable idempotent dispatch.
- Native real-process host journey passes, including negative controls against startup import.
- Approved authority for bounded historical-session replay/share; no raw session/worktree identity bypass.

### HOST-RECALL

Owner: **Host recall/provenance owner**. Initially unsatisfied. Required evidence:

- Actual read path propagates deadlines/cancellation through provenance lookup and retained trace persistence.
- Current equivalents of tdmem-0606, 0608 and 0609 are accepted by executed evidence; historical status alone is insufficient.
- Required host evidence survives packing; provider identity/fallback and typed failures are preserved.

### HOST-RELEASE

Owner: **Release/integration owner**. Initially unsatisfied. Required prerequisites:

- Accepted combined-tree production/feature/workspace results from 025; an existing packaging procedure and identified release owner.
- Required target runners, model-artifact access and signing/publishing permissions where required; an explicit supported OS/architecture claim.
- Attribution and artifact-specific rights resolved; active rollout remains opt-in. **New NCM packaging and installed-artifact verification are outputs of 026, not circular prerequisites.**

An external gate is satisfied by an identified commit/tree, actual selected test IDs and outcomes, not by another agent saying “fixed.” Joining changes the tested tree: rerun the relevant combined gates. The backend-only branch must not edit the current retention implementation or make failing host policy checks permissive.

## Existing work to reuse / map

| Existing area | Treatment |
|---|---|
| `tdmem-0304`, existing provider-ncm | Keep the adapter and protective tests; add a real opt-in backend |
| `tdmem-0701` | Recheck the completed audit against the pinned source; do not treat its source-marker tests as runtime proof |
| `tdmem-0700`, `0702–0711` | Preserve historical descoping; new work explicitly supersedes deferred intent |
| Observation journal/dispatcher | Reuse host admission and delivery; provider durable exactly-once handling remains required |
| `tdmem-0606`, `0608`, `0609` | Current host provenance/trace/read-journey evidence is an active-mode dependency; this DAG does not waive open findings |
| Existing memory conformance/evaluation | Reuse contracts, nine scenarios and baseline identities; add the real NCM implementation |
| Current recovery assignment | Independent host stream; no NCM kernel work should be added to its retention patch |

Do not rewrite old tasks' checked/unchecked boxes to manufacture completion. The coordinator reconciles new work and actual evidence in the existing tracker once IDs and branch ownership are agreed.

## Hard red controls

At least these deliberate failures must be detected: unchanged blur; random/hash embeddings; no-op observations; disabled consolidation masked by STM; fake Ready on corrupt state; repeated idempotent calls strengthening memory; cross-session namespace widening; deleting visible text while retaining mixed-state influence; stale snapshot resurrection; unknown commit reported as no effect; cancelled worker still consuming resources; zero selected tests marked green.

## Delivery verdict

**Backend accepted (022)** means an actual Rust worker can learn from real text, consolidate, restart, return supported LTM-only recall and delete a source within the declared resource envelope. It does not claim that host stabilization or active-mode usefulness is complete.

**Product accepted (026)** requires the stabilized host, real Observer and active journeys, safe provenance/packing, explicit routing/rollback, held-out evaluation and the installed artifact/platform evidence. No default active enablement is part of this assignment.

## Evidence limitations

Original paper PDFs could not be retrieved successfully during planning. Primary records/catalog and accessible supplementary text were inspected; no full-paper equation/figure review is claimed. Task 001/002 must verify remaining research details. No Biomem/Rust tests were run by the planner. `validation.json` proves only graph integrity and declared path-disjointness, not implementation correctness.

See `DESIGN_CONTRACT.md` for algorithm, source-deletion and numeric/resource details, `START_HERE.md` for the coding-session prompt and receipt, and `SOURCES.md` for pinned source links.


---

# Proposed design contract — `ncm-biomem-rs.v1`

This is a concrete starting specification, not an already approved or implemented backend. Task 003 freezes the contract; task 002 settles reference discrepancies. No node may silently widen it.

## 1. Product boundary

Implement **Biomem-based associative text memory in Rust**, not an official replacement binary for OpenTechLab's commercial product, and not a newly trained decoder with internal memory attention. The research describes decoder-level mechanisms; this product mounts advisory context through the existing host. These are different integration claims. [S10–S14]

Production path:

```text
Existing host observation / recall authorities
                |
Existing provider registry + invocation supervision
                |
Existing NcmProviderAdapter
                |
RustNcmSurface (new, opt-in adapter implementation)
                |
bounded local pipe -> supervised Rust worker
                |
provider-owned state + verified native text encoder
                |
tracedecay-memory-ncm-core
  projections / centers / terrain / dynamics / consolidation
```

Dependency direction: registry → existing NCM adapter → runtime client/wire → core. The runtime must not import the registry or NCM adapter. The worker's engine and inference implementation live in the runtime package; its client-only surface should avoid eagerly loading models or opening state in the host process.

Use exactly two new product-owned packages, not a crate for each mathematical concept. Existing test doubles stay test-only. Python is an oracle/tooling dependency, never the shipped runtime. Browser extensions, desktop GUI, PAM conversation prompting, telemetry, model training and legacy `.pt`/`.bdbm` conversion are outside this work package.

## 2. Algorithm inventory

The pinned code and configuration, not marketing descriptions, define the executable starting point. [S02–S08]

| Component | Reference baseline | Required v1 treatment |
|---|---|---|
| Text embedding | multilingual MiniLM-L12-v2, normalized 384D | Actual native inference; pinned tokenizer/model/pooling/normalization/truncation |
| LTM | 4,096 centers, 64D keys, 128D values, four emotion channels | Fixed numerical capacity, explicit text/source storage caps |
| STM | 512 centers, 16D keys | Real mutable learning; no synthetic echo store |
| Context / spatial projections | 16D context and 3D tanh coordinates | Persisted explicit matrices; defined axes and normalization |
| Plain RBF read | cosine kernel; intensity in normalized weights | Tested directly, including empty and singleton cases |
| Hybrid selection | cosine preselection of 64; p=0.5 dissimilarity weighted 0.7/0.3 | Preserve or explicitly correct batch normalization; do not assume a triangle inequality |
| Compound read | semantic/context/terrain weights 0.60/0.25/0.15 | Separate from plain hybrid read; exact contribution traces |
| Terrain | STM and LTM 48³ H + four E channels | Real splat, sampling, diffusion, leak, Gaussian blur and pour |
| Write strength / affect | novelty/surprise/salience sigmoid; four affect channels, neutral 1.0 | Frozen coefficients, presets and explicit optional Czech/English keyword heuristic; no invented prediction-error or human-emotion measurement |
| Sleep | fatigue, top-128 transfer, kappa 0.8, normalization | Persist cadence; prove LTM-only retrieval |
| Merge/prune | similarity 0.95, intensity 0.001 and minimum age 300 | Bounded work and full source lineage; no silent factual conflation |

For compatible kernels, use reference fixtures with explicit matrices/tensors. Start numerical tolerances at `atol=1e-6, rtol=1e-5` for single-step f32 kernels, then validate them against measured backend variation. Multi-step tolerances must be declared per fixture **before** inspecting candidate failures. Near-threshold branch decisions need dedicated exact-side tests; a tolerance is not permission to change which operation occurs. Counts, shapes, masks, operation codes and identities match exactly. Ties use a documented stable order or predefined tie groups.

Do not require equality to known defects. Maintain one corrected production profile and an expected-difference matrix against the unchanged Python reference. Any algorithm correction changes the algorithm/config identity and gets explicit persistence compatibility handling.

## 3. Source discrepancies to resolve, not hide

| ID | Evidence at pinned source | Classification / required action |
|---|---|---|
| D01 | `Terrain3D.blur()` constructs a kernel but returns H/E clones without convolution | Confirmed static no-op. Implement actual blur; asymmetric impulse negative control. [S04] |
| D02 | `MemoryCenters.write()` calls `compute_rbf_weights` without passing `sigma_write`; default uses `sigma_read` | Confirmed static behavior. Pin corrected write-width semantics with an expected-difference test. [S03] |
| D03 | Text recall calls compound reads without terrain queries | Confirmed call path. Do not claim a terrain prior affects text retrieval unless a tested v1 path wires it in. [S07] |
| D04 | `TextMemory.recall` deduplicates by key text and calls the selected weight confidence | Confirmed behavior. Preserve distinct record/source identities; activation is not calibrated truth probability. [S07] |
| D05 | Default reads increment usage/read counters | Confirmed behavior. Host Recall/Search/Inspect must be read-only; learning from use is explicit Feedback. [S03, S07] |
| D06 | `_load_impl` catches load exceptions and logs them | Confirmed behavior. Rust corrupt-state open fails closed; no healthy-empty fallback. [S07] |
| D07 | State serialization records fatigue but not `steps_since_consolidation` | Confirmed omission in inspected state writer. Reproduce schedule divergence, persist the complete scheduler state. [S05, S07] |
| D08 | Direct 384→64 and STM→64 projections initialize independently | Alignment risk, not a measured recall failure. Require LTM-only real-text retrieval. If correction is needed, retain/use keys in the canonical LTM basis rather than relying on STM answers. [S06, S07] |
| D09 | Terrain splat indexing and `grid_sample` coordinate order need reconciliation | Hypothesis. Test asymmetric coordinates before declaring a bug or choosing a corrected mapping. [S04] |
| D10 | README fatigue description, `update_fatigue`, `TextMemory.store` input and the automatic minimum interval are not interchangeable | Confirmed different levels of description. Freeze one explicit operation/tick schedule. [S05, S07] |
| D11 | Existing adapter namespace hashes session ID and resolved-scope digest | Confirmed isolation rule. Cross-session reuse requires authorized replay/share, not deleting hash inputs. [S14] |

The new implementation must expose whether terrain actually influences candidate selection. A deliberate conservative profile may keep terrain's effect limited, but cannot advertise unwired TerrainPrior behavior. If a corrected terrain-aware text-read policy is introduced, its scoring, enablement and ablation belong in the versioned profile. No decoder-hidden-state access is assumed.

## 4. State identity and transactional effects

Distinguish three identities: **implementation/model/algorithm compatibility**, **state/reset epoch**, and **ordinary mutation commit sequence**. Audit how the existing provider's `expected_state_generation` represents these before mapping it. The existing accepted-readiness descriptor equality must not accidentally make the next observation impossible after a normal commit. [S14]

A record identity survives center movement and consolidation. A center slot has a separate incarnation to prevent stale-handle reuse. Source handles carry exact host-resolvable provenance outside the opaque core; inability to hydrate is a typed unavailable state, never a generated citation.

One mutation binds `(namespace, idempotency_key, canonical_payload_digest)` to its committed effect receipt. Duplicate identical calls replay the receipt. Conflicting payloads under the same key fail. The durable commit and receipt are atomic; returning success before auto-save is prohibited. After acknowledged success, new reads observe at least the acknowledged sequence. If durable commit succeeds but live publication fails, return the truthful committed/unknown-effect state and recover; never report no effect and reapply the same learning. Cancellation before commit produces no effect; cancellation/transport failure after commit preserves the committed result or an honest unknown-effect reconciliation state.

A read uses a committed immutable view and has no implicit mutation. Snapshot creation and explicit maintenance remain separate bounded operations. Handshake is read-only compatibility/readiness validation; any creation/admission of a new state namespace occurs through an explicit authorized lifecycle operation beforehand, not an undisclosed handshake side effect.

## 5. Scope and cross-session reuse

Initial provider namespace derivation remains the existing complete exact-scope hash. Treat hashes as identifiers, not as a substitute for authorization. Do not expose host database handles or raw project/worktree paths to the core. [S14]

Cross-session memory is delivered by the host's explicitly authorized, bounded historical-source replay/share into the requesting namespace. Original source identity and origin remain available for provenance, correction and deletion. A source copied to multiple admitted namespaces must have a provider-owned index permitting complete authorized revocation across those copies.

No default sibling-worktree, unrelated-project or cross-profile sharing. If existing host contracts cannot safely express the replay/share request, an owner-reviewed extension is a **late integration dependency**. Standalone kernel work can finish; active cross-session release cannot be claimed complete.

## 6. Forgetting is not privacy deletion

Decay reduces numerical influence. It does not prove removal of source bytes, embeddings, mixed-center contributions, terrain influence or old snapshots.

Maintain a bounded, replay-complete provider-owned source/event basis for any state that needs exact source deletion. Because updates include nonlinear normalization, merging and diffusion, deleting one text row or subtracting one initial vector is insufficient. The v1 safe path fences the affected namespace, rebuilds from authorized retained inputs excluding revoked sources, and atomically publishes the sanitized state. During rebuild, unavailable is preferable to stale success.

A reconstruction after deletion is a declared sanitized replay, not an assertion that all remaining floating-point state equals the old trajectory. Preserve original logical ordering/time policy and document how excluding the source changes novelty and sleep decisions.

The privacy epoch/revocation authority is not supplied by an imported old snapshot. Every restore/replay checks current revocations. If the source basis or required revocation authority is unavailable, refuse serving/restore until a specific approved repair; never claim complete deletion. Bound the reconstruction basis and reject new ingestion at its quota rather than silently losing the ability to honor deletion.

State the erasure boundary precisely: controlled stores, caches and snapshots are covered; independently exported files and physical-storage remanence are not magically erased.

## 7. Initial resource and performance targets

These are **proposed engineering budgets, not measured results or paper claims**. Task 003 pins them to a named test machine, build profile and workload before acceptance. Any adjustment requires a visible rationale; safety/identity requirements cannot be weakened.

| Budget | Initial proposal |
|---|---|
| Worker count | One supervised Rust worker per admitted profile; no process per request |
| Resident namespace views | At most four namespaces; at most two live generations each, including rebuilding state |
| Total namespace catalog | At most 32 per profile; no silent cross-namespace eviction |
| Record payload | 16 KiB UTF-8 key+value maximum per record; larger inputs require explicit host segmentation or refusal |
| Batch / wire | At most 16 records; 256 KiB request and 1 MiB reply; validate before allocation |
| Mailbox | At most 32 requests and 8 MiB total queued payload, whichever binds first |
| Provider source basis | At most 64 MiB per namespace; retain reconstruction completeness or reject ingestion |
| Controlled storage | At most 256 MiB per namespace and 2 GiB per profile, including WAL, snapshots, staging and privacy metadata |
| Privacy/recovery reserve | Reserve at least 16 MiB inside the profile budget, unavailable to ordinary ingestion |
| Worker memory | Initial peak-RSS target ≤1 GiB with the real encoder and the declared four-namespace workload |
| Kernel recall | Proposed p95 ≤25 ms at full configured center capacity on a named CPU, pre-embedded input |
| Warm real-text recall | Proposed p95 ≤250 ms, p99 ≤500 ms, one 128-token query, single-client reference load |
| Durable observe | Proposed p95 ≤500 ms for one bounded key/value pair on named local storage; measure encoding and fsync separately |
| Deadline escalation | Stop/kill escalation target ≤250 ms after cancellation/deadline; unknown effects reconciled after restart |

Host deadlines always take precedence: effective budget is the minimum admitted remaining deadline and provider limit. OS scheduling makes timing a tested service target, not a mathematical guarantee. Loaded or quota-constrained conditions must yield bounded typed failures, not queue growth. Include repeat namespace churn, deleted-source replay, journal compaction, cache pressure and full-disk cases.

Safety properties: zero observed scope leaks, false commits, deleted-source resurrection and corrupt-state successful reads in the declared tests. This is finite test evidence, not a universal proof. Use matched hardware/workloads for Python comparisons and distinguish numeric-kernel latency from whole text-to-context latency.

## 8. Acceptance levels

**Numerical:** compatible algorithms match the oracle; intended corrections have independent tests. A no-op terrain or random embedder cannot pass.

**Backend:** real worker and model, complete transactions/recovery/deletion, bounded resource behavior and provider conformance. This is task 022 and does not need the host-fix session to finish.

**Observer:** real host delivery, no output influence, authorized cross-session reuse, no Native regression. This is task 024.

**Active experimental:** explicit routing, bounded host admission/hydration/packing, no implicit fallback, safe failure and rollback. Host acceptance gate is mandatory.

**Release:** joined held-out usefulness thresholds plus safety and installed-artifact/platform evidence pass. A useful threshold is predeclared by task 021; no improvement is assumed from the papers or algorithm complexity. If usefulness is inconclusive, retain experimental/Observer labeling rather than declaring the full active release delivered.

All source identifiers resolve in `SOURCES.md` and `task_dag.json`.


---

# Start here — Biomem-based Rust NCM

## Coding-session assignment

Implement a real, independently usable Rust NCM backend inside `BleedingDev/tracedecay`, using Biomem commit `500847ff65b5d9548b3826fa29bf3ccf8d221147` as the executable reference and the two identified Michal Seidl/OpenTechLab publications as research references. Start from product checkpoint `25778c7443cd0cfe257da363da01a56ea1d45d3f` on an isolated worktree/branch, suggested name `feat/ncm-biomem-rust-v1`.

This request supersedes the earlier decision to defer the Rust port. It does **not** authorize bypassing the other session's host-retention, CI, ownership, or acceptance repairs. Preserve the existing provider API, fabric, registry, NCM adapter protections and adversarial test doubles. Replace the absent production implementation with real learned state, real native model inference, durable effects and verified recovery—not a canned provider or Python wrapper.

Read `IMPLEMENTATION_DAG.md`, `DESIGN_CONTRACT.md`, then the current task file under `tasks/`. Use `task_dag.json` as the dependency/ownership interchange format. It is **not** a direct Beads import schema or a second authoritative tracker. The coordinator maps these planning IDs into new Beads records through the existing operation process; retain historical descoped closures.

## What may proceed now

Tasks **001–022** have no dependency on the other session's fixes. They produce a standalone Rust core, runtime/worker, real adapter, conformance/evaluation evidence and backend acceptance. They still depend on their own listed prerequisites and required local model/tooling resources.

Tasks **023–026** join the repaired host and have explicit external gates. They must not be represented as complete based on standalone backend results.

```bash
# Validate this planning package; no repository writes.
python3 validate_dag.py

# Initially only 001 is ready.
python3 validate_dag.py --ready

# Example after the foundation really has been verified:
python3 validate_dag.py \
  --completed ncm-rs-001,ncm-rs-002,ncm-rs-003,ncm-rs-004 \
  --ready
```

The last command should select 005, 008, 012 and 013: projections, terrain, real embeddings and durable storage. Pass `--satisfied-gates` only for externally supplied, reviewed evidence; this script checks graph consistency, not whether evidence is authentic or tests passed.

## Ownership and concurrency

Use a separate branch/worktree, disposable state roots and isolated profiles. Never run against the operator's installed daemon/profile. Do not reset another session's checkout, installed CLI, caches or tests.

The NCM coordinator alone edits **this branch's** `Cargo.toml`, `Cargo.lock`, central `lib.rs` declarations and task import metadata. Each worker edits only its node's owned paths. Consult the repository's current cargo contention policy; use its broker/cache rather than spawning competing unrestricted workspace builds or wiping caches.

Before join task 023, do not change:

- `crates/tracedecay-code-index-retention/**`, `crates/tracedecay-code-index-runtime/**`, `crates/tracedecay/src/daemon/store_maintenance/**`, or the retention fixture.
- Existing architecture checker/tests, current upstream convergence workflow, accepted upstream floor or existing host recall/observation composition.
- Shared `.beads/issues.jsonl` on the product branch or other workers' task status.

The two new packages and existing NCM adapter are independent ownership domains. Root manifest/lock changes are an explicit coordinator-owned exception on the isolated branch; their eventual merge is task 023. Proposed host module paths in 024/025 are **proposals**, not claims about existing layout: discover the concrete mount on the stabilized tree and obtain its owner's approval before editing it.

Parallelize only ready nodes with non-overlapping files. Read-only review may proceed in parallel; code changes to the same files require an ordered handoff. The validator checks the declared path prefixes, not undeclared imports, runtime resource contention or actual diffs.

## Definition of done for every node

A task is done only when its implementation and negative controls execute under its actual declared path. Source-marker checks alone do not establish runtime behavior. A stub compiling is not a working capability.

Attach this receipt:

```json
{
  "task_id": "ncm-rs-NNN",
  "source_commit": "actual commit",
  "source_tree": "actual tree",
  "allowed_path_diff": ["actual changed paths"],
  "implementation_summary": "what now works",
  "algorithm_config_identity": "actual version and digest, or not_applicable",
  "model_projection_identity": "actual identities, or not_applicable",
  "exact_commands": ["commands actually executed"],
  "selected_test_ids": ["actual executed test identifiers"],
  "pass_fail_skip_ignored_counts": {},
  "negative_controls": ["mutants/faults that the tests detect"],
  "binary_identity_when_applicable": "actual hash/features",
  "environment_and_resources": {},
  "limitations": [],
  "reviewer_verdict": "pass | fail | blocked"
}
```

This is a schema example, not a valid receipt with substitute values. No placeholder identity is accepted on completion. Discover test IDs before running filtered tests; assert the complete expected set and fail empty selection. Proposed new test names in this package must be created by their owning task and bound to executable commands. A skipped real-model test cannot make a release gate pass.

A reference discrepancy is resolved by a small red test and an approved entry in `deviations.json`. Do not enlarge float tolerances, weaken privacy requirements, remove contradictory tests, or broaden provider capabilities to make a task appear green.

## Deliverables at the first stopping point

At 022: a real Rust worker and adapter that can observe text, consolidate, restart, recall from LTM and delete a source, plus complete backend evidence. Hand the integration owner the exact branch/tree and narrow shared-file patch. Do not mount it into the product prematurely.

At 026: installed-product evidence for an explicitly opt-in active provider, with the separate scientific-fidelity, runtime-safety, platform and usefulness verdicts. Failure of any required verdict leaves that release blocked; it does not invalidate completed isolated kernel work.

## Evidence boundary of this plan

The planner inspected pinned source excerpts, primary publication records/catalog, a primary supplementary document and the model card. Original paper PDFs could not be retrieved successfully, so their figures, equations and full experimental claims were **not** verified. Reference code and Rust backend tests were **not** run by the planner. Task 001/002 closes those source/reference gaps. The planning DAG itself was programmatically validated.


---

## Individual task specifications

<a id="ncm-rs-001"></a>

# ncm-rs-001 — Pin reference, research provenance, and new work authorization

Status: planned, not executed.
Owner: Coordinator / reference reviewer.
Dependencies: none.
External gates: none.

## Objective

Establish exactly what is being reimplemented, without treating prior descoping or the Python README as delivery evidence.

## Owned paths

- `product/ncm/reference/source-manifest.json`
- `product/ncm/reference/NOTICE.md`
- `product/ncm/plan/`

## Implementation

1. Pin product base 25778c7443cd0cfe257da363da01a56ea1d45d3f and Biomem 500847ff65b5d9548b3826fa29bf3ccf8d221147; record tree/blob identities for every source file used. Do not follow moving branch names during implementation.
2. Record this request as authorization for a new Rust-backend work package. Preserve historic tdmem-0700/0702–0711 descoped closures; link new tasks rather than rewriting them as delivered. Reuse the 0701 audit only after checking its pinned source and evidence.
3. Acquire both original papers, verify title/author/version and file hashes, and map relevant sections. Record discrepancies between papers, supplementary documentation, README and executable code. Do not fabricate equation numbers or experimental results when full text is unavailable.
4. Preserve the Biomem MIT notice for reused substantial material. Audit provenance of any copied assets/code and model license. Do not copy CC BY-NC supplementary code into commercial artifacts on the assumption that the repository MIT notice relicenses it. Escalate unresolved rights for the specific artifact, not as a vague blocker to all independent work.

## Acceptance

1. A machine-readable manifest identifies immutable code and model inputs; unresolved artifact hashes are explicitly blocked, never placeholder digests.
2. All production implementation sources are categorized: MIT reference code, paper concepts, or original product work.
3. Full-paper-derived requirements are approved only after actual full-text review; otherwise that portion remains blocked and no paper-conformance claim is made.
4. The new task package is staged on the independent branch only; shared .beads/issues.jsonl is not edited by parallel workers.

## Verification targets

1. reference_manifest_test (planned stdlib validator)

## Sources

- [S01: Biomem pinned implementation](https://github.com/BleedingDev/biomem/tree/500847ff65b5d9548b3826fa29bf3ccf8d221147)
- [S09: Biomem MIT notice](https://github.com/BleedingDev/biomem/blob/500847ff65b5d9548b3826fa29bf3ccf8d221147/LICENSE)
- [S10: Persistent Memory for Decoder-Only Transformers: Latent Terrain, Diffusion, Homeostasis, and Emotional Stabilization](https://zenodo.org/records/18198327)
- [S11: Implementation of Persistent Latent Memory for Decoder Transformers](https://zenodo.org/records/18267378)
- [S12: OpenTechLab primary publication catalog](https://www.opentechlab.cz/publikace.html)
- [S13: BioCortexAI supplementary scientific documentation](https://zenodo.org/records/18198327/files/BioCortexAI_Documentation_EN.md?download=1)
- [S15: Versioned Beads plan at checkpoint](https://github.com/BleedingDev/tracedecay/blob/25778c7443cd0cfe257da363da01a56ea1d45d3f/.beads/issues.jsonl)

## Handoff

Commit one reviewable slice; attach the common completion receipt; do not change shared host paths outside the ownership agreement.

Use the common evidence receipt and ownership rules in `START_HERE.md`. These are task specifications, not claims that the test targets already exist.


---

<a id="ncm-rs-002"></a>

# ncm-rs-002 — Build an executable Python oracle and defect-disposition matrix

Status: planned, not executed.
Owner: Reference / numerical verifier.
Dependencies: ncm-rs-001.
External gates: none.

## Objective

Provide an independent numerical oracle without reproducing known no-ops or accepting untested claims.

## Owned paths

- `scripts/product/ncm/reference/`
- `product/ncm/reference/oracle/`
- `product/ncm/reference/deviations.json`

## Implementation

1. Run the pinned Python core in an isolated CPU environment. Pin Python, torch, sentence-transformers, tokenizer/model artifacts, thread count, dtype, and initial state. Classify tests using fake embeddings separately from real-model tests.
2. Export actual initialized projection matrices, tensors, masks, counters and ordered operations to a non-executable, bounded fixture format. A shared seed is insufficient for cross-language RNG parity. Export per-operation intermediate states, not just final answer strings.
3. Create red controls for blur returning unchanged grids, write sigma using read sigma, load errors being swallowed, missing consolidation cadence in snapshots, potential terrain axis mismatch, and STM→LTM key-space alignment. Each entry needs observed reference behavior, intended v1 behavior, rationale and a discriminating test.
4. Maintain one production algorithm profile. Preserve original behavior only in reference fixtures, not in a second shipped legacy engine. Mark deliberate corrections as expected differences, not tolerance exceptions.
5. Record exact _sanitize_emotion/preset/dictionary/keyword semantics, including neutral-on-None and Czech dictionary keys. Fixtures must distinguish caller-supplied affect from optional text heuristics; no inferred mental-state or decoder-surprise claims.

## Acceptance

1. Oracle fixtures regenerate deterministically after canonicalizing independently generated identities/timestamps with explicit harness inputs.
2. Wrong/no-op blur, constant-output recall, disabled consolidation and random projection substitutions are detected by negative controls.
3. Every intentional divergence has an approved expected result and independent oracle/analytic justification.
4. Fixtures include empty, singleton, >64-center hybrid selection, near-threshold, asymmetric terrain, merge, consolidation, and long sequential traces.

## Verification targets

1. reference_oracle_test
2. reference_negative_controls_test

## Sources

- [S02: Biomem configuration](https://github.com/BleedingDev/biomem/blob/500847ff65b5d9548b3826fa29bf3ccf8d221147/src/memory_module/config.py)
- [S03: Biomem center algorithms](https://github.com/BleedingDev/biomem/blob/500847ff65b5d9548b3826fa29bf3ccf8d221147/src/memory_module/memory_centers.py)
- [S04: Biomem terrain](https://github.com/BleedingDev/biomem/blob/500847ff65b5d9548b3826fa29bf3ccf8d221147/src/memory_module/terrain_3d.py)
- [S05: Biomem consolidation](https://github.com/BleedingDev/biomem/blob/500847ff65b5d9548b3826fa29bf3ccf8d221147/src/memory_module/consolidation.py)
- [S06: Biomem projections](https://github.com/BleedingDev/biomem/blob/500847ff65b5d9548b3826fa29bf3ccf8d221147/src/memory_module/projections.py)
- [S07: Biomem text-memory orchestration](https://github.com/BleedingDev/biomem/blob/500847ff65b5d9548b3826fa29bf3ccf8d221147/src/memory_module/text_memory.py)

## Handoff

Commit one reviewable slice; attach the common completion receipt; do not change shared host paths outside the ownership agreement.

Use the common evidence receipt and ownership rules in `START_HERE.md`. These are task specifications, not claims that the test targets already exist.


---

<a id="ncm-rs-003"></a>

# ncm-rs-003 — Freeze backend contracts, scope policy, effects and resource budgets

Status: planned, not executed.
Owner: Backend architect / integration liaison.
Dependencies: ncm-rs-001.
External gates: none.

## Objective

Give independent workers a stable internal contract and prevent scope or durability redesign late in implementation.

## Owned paths

- `product/ncm/spec/`

## Implementation

1. Specify typed opaque NamespaceId, SourceId, RecordId, CenterSlot+incarnation, AlgorithmIdentity, StateEpoch, CommitSequence, LogicalTick and operation receipts. Public record identity must not be a reusable array index.
2. Keep the current full exact-scope namespace in the first adapter. Cross-session reuse must come from an explicitly host-authorized replay/share bridge, preserving original-source provenance; never drop agent_session_id or resolved_scope_digest silently. Core math sees only opaque namespaces and input records, never Git or host stores.
3. Define Observe, read-only Recall/Search/Inspect, explicit Feedback/Correction, Advance/Maintain, Delete, Export/Restore and Close behavior, and map these to the actual existing ProviderOperation variants and payload schemas. Unsupported host operations return typed unsupported; do not invent a parallel public provider API.
4. Audit state_generation semantics against AcceptedReadiness. Separate compatibility/reset epoch from ordinary commit sequence so each observation neither invalidates readiness accidentally nor permits stale expected generations.
5. Choose supervised local Rust worker for production, pure Rust core for numerical testing, Python only as the offline reference. Reuse host supervision and admission. Specify crash-safe commit/publish order, deadline/kill behavior, cancellation effect states and bounded mailbox frames.
6. Freeze source-deletion reconstruction and quota policy before storage work. v1 uses bounded replay-complete source capsules/event history and a privacy epoch outside restorable snapshots; incomplete lineage means refuse serving until a truthful repair/reset, never claim exact deletion from metadata alone.
7. Freeze the real-encoder library/artifact-loading approach and dependency inventory before N04 updates Cargo manifests. Do not silently reinterpret public expected_state_generation: prove its actual contract with sequential mutations and readiness checks; keep internal epoch and commit sequence distinct.

## Acceptance

1. Schema examples for success, empty, rejected, busy, cancelled, effect-unknown, incompatible and corrupt are reviewed against existing contracts.
2. Budgets are finite for centers, record bytes, lineage, event log, namespace count, resident views, snapshots, queued bytes, compute operations, and privacy reserve; authoritative acknowledgements never rely on auto-save.
3. Read-only operations do not change h, usage, age, fatigue, centers, terrain or persistence sequence. Any learning-from-use is a separately admitted idempotent feedback effect.
4. Late host write paths and required external evidence are specified as proposals; final host-owner approval is deferred to N23, not a prerequisite for independent backend work. Production NCM remains default-off.

## Verification targets

1. contract_schema_test
2. operation_contract_examples_test

## Sources

- [S14: Existing NCM adapter boundary](https://github.com/BleedingDev/tracedecay/blob/25778c7443cd0cfe257da363da01a56ea1d45d3f/crates/tracedecay-memory-provider-ncm/src/lib.rs)
- [S15: Versioned Beads plan at checkpoint](https://github.com/BleedingDev/tracedecay/blob/25778c7443cd0cfe257da363da01a56ea1d45d3f/.beads/issues.jsonl)
- [S17: Existing provider-neutral evaluation](https://github.com/BleedingDev/tracedecay/blob/25778c7443cd0cfe257da363da01a56ea1d45d3f/product/evaluation/README.md)
- [S18: MiniLM primary model card](https://huggingface.co/sentence-transformers/paraphrase-multilingual-MiniLM-L12-v2)

## Handoff

Commit one reviewable slice; attach the common completion receipt; do not change shared host paths outside the ownership agreement.

Use the common evidence receipt and ownership rules in `START_HERE.md`. These are task specifications, not claims that the test targets already exist.


---

<a id="ncm-rs-004"></a>

# ncm-rs-004 — Create isolated Rust packages and compile-time interfaces

Status: planned, not executed.
Owner: NCM branch coordinator.
Dependencies: ncm-rs-003.
External gates: none.

## Objective

Make backend-only development possible without touching retention, host CI policy or current recall composition.

## Owned paths

- `crates/tracedecay-memory-ncm-core/Cargo.toml`
- `crates/tracedecay-memory-ncm-core/src/lib.rs`
- `crates/tracedecay-memory-ncm-core/src/types.rs`
- `crates/tracedecay-memory-ncm-runtime/Cargo.toml`
- `crates/tracedecay-memory-ncm-runtime/src/lib.rs`
- `crates/tracedecay-memory-ncm-runtime/src/ports.rs`
- `Cargo.toml`
- `Cargo.lock`
- `product/ncm/bootstrap/`

## Implementation

1. Add exactly two product-owned packages: core for numerical state and opaque record support; runtime for state I/O, actual embedding execution, worker and protocol. Retain the existing provider-ncm adapter.
2. Coordinator alone edits this branch’s workspace member list and lockfile. Predeclare module paths with empty private module files where needed; those files contain no implemented public capability or fake success. Their exact initial paths must be added to this task’s ownership manifest before creation. Later central manifest/interface adjustments are coordinator-owned integration commits, not concurrent worker edits. No temporary second workspace or duplicate lockfile.
3. Core has no Tokio, filesystem, process, network, host-store or concrete-provider dependency. Runtime must not depend on provider-ncm or registry, avoiding a dependency cycle when the adapter depends on its client/wire library.
4. Pin required numeric/serialization/inference dependencies to reviewed versions compatible with the repository toolchain. Reuse a matching existing Rust inference backend where feasible; do not silently choose a different embedding model.

## Acceptance

1. Both packages compile independently under the pinned workspace toolchain with warnings denied and owned unsafe code forbidden.
2. No NCM registration, active route, background process or state directory exists when the feature is off.
3. Backend branch diff contains only declared product-owned work plus the isolated manifest/lock patch.
4. Later workers have compile-time contracts and module paths; any newly discovered shared manifest/interface change is serialized through the coordinator and recorded as an explicit dependency change rather than edited concurrently.

## Verification targets

1. cargo check -p tracedecay-memory-ncm-core -p tracedecay-memory-ncm-runtime --locked
2. dependency_boundary_test

## Sources

- [S14: Existing NCM adapter boundary](https://github.com/BleedingDev/tracedecay/blob/25778c7443cd0cfe257da363da01a56ea1d45d3f/crates/tracedecay-memory-provider-ncm/src/lib.rs)
- [S19: Existing upstream convergence procedure](https://github.com/BleedingDev/tracedecay/blob/25778c7443cd0cfe257da363da01a56ea1d45d3f/product/upstream/README.md)

## Handoff

Commit one reviewable slice; attach the common completion receipt; do not change shared host paths outside the ownership agreement.

Use the common evidence receipt and ownership rules in `START_HERE.md`. These are task specifications, not claims that the test targets already exist.


---

<a id="ncm-rs-005"></a>

# ncm-rs-005 — Implement numerical primitives and persisted projection bundle

Status: planned, not executed.
Owner: Core / projections.
Dependencies: ncm-rs-002, ncm-rs-004.
External gates: none.

## Objective

Reproduce the real vector transformations with explicit numerical identity.

## Owned paths

- `crates/tracedecay-memory-ncm-core/src/numeric/`
- `crates/tracedecay-memory-ncm-core/src/projections/`
- `crates/tracedecay-memory-ncm-core/tests/numeric.rs`
- `crates/tracedecay-memory-ncm-core/tests/projections.rs`

## Implementation

1. Implement validated f32 operations, normalization, stable log-softmax, cosine distance, hybrid p=0.5 dissimilarity and deterministic top-k. Reject non-finite values and dimension mismatches before state mutation.
2. Load the oracle’s explicit projection matrices: 384→64 LTM, 384→16 STM/context, 384→128 values, key→3D tanh and 16→64 consolidation. Persist matrices and hashes; never reinitialize them on open/reset/replay without an explicit new epoch.
3. Define zero-vector handling, epsilon placement and tie groups. p=0.5 is not a mathematical metric; do not use triangle-inequality indexing assumptions.
4. Implement shared projected-record structures that preserve a canonical LTM-basis key when required by the approved alignment disposition.

## Acceptance

1. Reference-compatible primitive outputs satisfy the frozen float tolerances; shape/mask/identity values match exactly.
2. Exact ties are stable in Rust; reference nondeterministic ties are compared as predefined equivalence groups, not hidden by fuzzy ranking.
3. NaN, infinity, degenerate inputs and malformed matrix files produce typed errors without partial state.
4. Open and restore reuse identical projection identity.

## Verification targets

1. cargo test -p tracedecay-memory-ncm-core --test numeric --test projections --locked

## Sources

- [S03: Biomem center algorithms](https://github.com/BleedingDev/biomem/blob/500847ff65b5d9548b3826fa29bf3ccf8d221147/src/memory_module/memory_centers.py)
- [S06: Biomem projections](https://github.com/BleedingDev/biomem/blob/500847ff65b5d9548b3826fa29bf3ccf8d221147/src/memory_module/projections.py)

## Handoff

Commit one reviewable slice; attach the common completion receipt; do not change shared host paths outside the ownership agreement.

Use the common evidence receipt and ownership rules in `START_HERE.md`. These are task specifications, not claims that the test targets already exist.


---

<a id="ncm-rs-006"></a>

# ncm-rs-006 — Implement center reads: RBF, hybrid selection and compound keys

Status: planned, not executed.
Owner: Core / center reads.
Dependencies: ncm-rs-005.
External gates: none.

## Objective

Implement associative reading, not a renamed key-value lookup or flat cosine-only search.

## Owned paths

- `crates/tracedecay-memory-ncm-core/src/centers/read.rs`
- `crates/tracedecay-memory-ncm-core/tests/center_read.rs`

## Implementation

1. Reproduce active-mask filtering, cosine candidate preselection, hybrid reranking above the 64-candidate boundary, selected-center intensity weighting and weighted value/emotion reads.
2. Implement compound semantic/context/terrain scoring separately from the plain RBF path. The Python TextMemory recall uses compound reads; do not assume it uses the plain hybrid path.
3. Expose score components and selected source-support handles without labeling normalized weights as calibrated confidence.
4. Accept immutable read views and operation budgets. All read statistics are output metadata, not implicit learning effects.

## Acceptance

1. Empty, singleton, 64/65-center transitions, distant queries, intensity imbalance and context/terrain perturbation fixtures pass.
2. A caller can inspect actual contributions; a one-item softmax weight of 1 is not reported as certainty of relevance.
3. Batch decomposition behavior is specified; reference batch-global Minkowski normalization is recorded if corrected to per-query normalization.
4. Repeated reads leave the learning-state digest and commit sequence unchanged.

## Verification targets

1. cargo test -p tracedecay-memory-ncm-core --test center_read --locked

## Sources

- [S03: Biomem center algorithms](https://github.com/BleedingDev/biomem/blob/500847ff65b5d9548b3826fa29bf3ccf8d221147/src/memory_module/memory_centers.py)
- [S07: Biomem text-memory orchestration](https://github.com/BleedingDev/biomem/blob/500847ff65b5d9548b3826fa29bf3ccf8d221147/src/memory_module/text_memory.py)

## Handoff

Commit one reviewable slice; attach the common completion receipt; do not change shared host paths outside the ownership agreement.

Use the common evidence receipt and ownership rules in `START_HERE.md`. These are task specifications, not claims that the test targets already exist.


---

<a id="ncm-rs-007"></a>

# ncm-rs-007 — Implement center writes, allocation and bounded source support

Status: planned, not executed.
Owner: Core / center writes.
Dependencies: ncm-rs-006.
External gates: none.

## Objective

Implement genuine stateful learning with stable identities and complete mutation attribution.

## Owned paths

- `crates/tracedecay-memory-ncm-core/src/centers/write.rs`
- `crates/tracedecay-memory-ncm-core/src/centers/allocation.rs`
- `crates/tracedecay-memory-ncm-core/tests/center_write.rs`
- `crates/tracedecay-memory-ncm-core/src/signals/`
- `crates/tracedecay-memory-ncm-core/tests/signals.rs`

## Implementation

1. Implement novelty, surprise and salience weighting, STM admission, local updates of intensity/value/emotion/context/terrain position and fixed-capacity allocation.
2. Use the explicitly approved sigma-write behavior; the pinned reference write calls compute_rbf_weights without passing sigma_write. A corrected width needs a versioned divergence test.
3. Track every source that influences every updated center, not just the top center’s displayed text. Resolve capacity with a documented deterministic rule and a receipt; never silently overwrite a public record identity.
4. Distinguish same logical record from duplicate operation. Stable memory_id alone is not an exactly-once guard; durable request deduplication is N14.
5. Implement four-channel affect/salience handling: explicit neutral default [1,1,1,1], validated finite vectors, pinned named presets and dictionary spelling/alias policy. Port the reference Czech/English keyword extractor as an explicit optional signal source, not an implicit measurement of human emotion. Retain input source and policy identity. Surprise is an admitted signal; do not invent decoder prediction error.

## Acceptance

1. Reference-compatible mutation fixtures pass and approved corrections have negative controls.
2. Zero strength, malformed batches, exhausted record/lineage budgets and full capacity return explicit effects and do not leave partial mutations.
3. A reused center slot receives a new incarnation; prior source links cannot resolve to unrelated content.
4. All supporting-record and content allocations obey byte/count bounds.
5. Neutral, named, explicit-vector, dictionary-alias and optional Czech/English keyword paths are tested. Malformed/nonfinite affect is rejected rather than silently replaced. Keyword substrings/negation are characterized as heuristic limitations; affect cannot override scope, provenance or current-code authority.

## Verification targets

1. cargo test -p tracedecay-memory-ncm-core --test center_write --locked
2. cargo test -p tracedecay-memory-ncm-core --test signals --locked

## Sources

- [S02: Biomem configuration](https://github.com/BleedingDev/biomem/blob/500847ff65b5d9548b3826fa29bf3ccf8d221147/src/memory_module/config.py)
- [S03: Biomem center algorithms](https://github.com/BleedingDev/biomem/blob/500847ff65b5d9548b3826fa29bf3ccf8d221147/src/memory_module/memory_centers.py)
- [S07: Biomem text-memory orchestration](https://github.com/BleedingDev/biomem/blob/500847ff65b5d9548b3826fa29bf3ccf8d221147/src/memory_module/text_memory.py)
- [S08: Biomem real text embedding and emotion extraction](https://github.com/BleedingDev/biomem/blob/500847ff65b5d9548b3826fa29bf3ccf8d221147/src/memory_module/embedder.py)

## Handoff

Commit one reviewable slice; attach the common completion receipt; do not change shared host paths outside the ownership agreement.

Use the common evidence receipt and ownership rules in `START_HERE.md`. These are task specifications, not claims that the test targets already exist.


---

<a id="ncm-rs-008"></a>

# ncm-rs-008 — Implement actual 3D terrain, diffusion, sampling and Gaussian blur

Status: planned, not executed.
Owner: Core / terrain.
Dependencies: ncm-rs-002, ncm-rs-004.
External gates: none.

## Objective

Provide the spatial dynamics that the reference describes, including a real blur implementation.

## Owned paths

- `crates/tracedecay-memory-ncm-core/src/terrain/`
- `crates/tracedecay-memory-ncm-core/tests/terrain.rs`

## Implementation

1. Implement two 48³ fields with scalar H and four E channels, six-neighbor Laplacian, replicated boundaries, Gaussian splat, trilinear sampling and homeostasis.
2. Implement a real separable Gaussian blur with documented radius, normalization and boundary policy. Do not port Terrain3D.blur’s unchanged clones.
3. Audit torch grid_sample coordinate ordering against tensor layout and splat indexing. Choose one coherent v1 axis convention and record any reference deviation.
4. Use double buffers or immutable generations for diffusion; in-place neighbor sweeps must not introduce order-dependent math. Validate combined leak/diffusion coefficients and bounded work.

## Acceptance

1. An off-center asymmetric impulse spreads under blur; a constant field remains constant under normalized blur; interior impulse mass and symmetry match the chosen kernel.
2. Sampling a splatted asymmetric point catches x/z or stride swaps; test corners, faces and fractional positions.
3. H decays toward 0 and E toward 1 without NaN/negative excursions beyond the declared clamp policy. For the explicit combined update, require a sufficient stability bound such as lambda+6*alpha<=1.
4. Corrected blur fails against the original no-op output for the intended reason; all unchanged operations meet oracle tolerances.

## Verification targets

1. cargo test -p tracedecay-memory-ncm-core --test terrain --locked

## Sources

- [S04: Biomem terrain](https://github.com/BleedingDev/biomem/blob/500847ff65b5d9548b3826fa29bf3ccf8d221147/src/memory_module/terrain_3d.py)

## Handoff

Commit one reviewable slice; attach the common completion receipt; do not change shared host paths outside the ownership agreement.

Use the common evidence receipt and ownership rules in `START_HERE.md`. These are task specifications, not claims that the test targets already exist.


---

<a id="ncm-rs-009"></a>

# ncm-rs-009 — Implement explicit logical time, homeostasis and fatigue

Status: planned, not executed.
Owner: Core / dynamics.
Dependencies: ncm-rs-007, ncm-rs-008.
External gates: none.

## Objective

Make forgetting and sleep scheduling reproducible across retries, idle periods and restart.

## Owned paths

- `crates/tracedecay-memory-ncm-core/src/dynamics/`
- `crates/tracedecay-memory-ncm-core/tests/dynamics.rs`

## Implementation

1. Expose explicit logical Advance operations. Freeze the v1 Observe schedule as one learning tick per unique committed observation; any departure from the Python caller’s separate store/step schedule is recorded.
2. Implement separate center intensity/value/emotion leak rates, age counters, terrain evolution, fatigue leak/accumulation and consolidation eligibility.
3. Resolve the reference differences: documentation describes omega-based fatigue, TextMemory passes input intensity, and AutomaticConsolidator has a 100-step minimum interval. Pin actual selected behavior; do not infer wall-clock years from step-based coefficients.
4. Persist fatigue, tick, steps_since_consolidation, and last maintenance identity. Bound large Advance requests; never execute an unbounded per-microsecond loop.

## Acceptance

1. Duplicate observations and read-only calls do not advance time or fatigue.
2. Closed-form decay checks agree with explicit updates in their valid domain; no claim of calendar half-life exists without a documented clock mapping.
3. Split-run and uninterrupted runs have the same sleep boundaries and state under the same explicit schedule.
4. Threshold crossings and the 99/100/101 interval boundary are covered.

## Verification targets

1. cargo test -p tracedecay-memory-ncm-core --test dynamics --locked

## Sources

- [S02: Biomem configuration](https://github.com/BleedingDev/biomem/blob/500847ff65b5d9548b3826fa29bf3ccf8d221147/src/memory_module/config.py)
- [S05: Biomem consolidation](https://github.com/BleedingDev/biomem/blob/500847ff65b5d9548b3826fa29bf3ccf8d221147/src/memory_module/consolidation.py)
- [S07: Biomem text-memory orchestration](https://github.com/BleedingDev/biomem/blob/500847ff65b5d9548b3826fa29bf3ccf8d221147/src/memory_module/text_memory.py)

## Handoff

Commit one reviewable slice; attach the common completion receipt; do not change shared host paths outside the ownership agreement.

Use the common evidence receipt and ownership rules in `START_HERE.md`. These are task specifications, not claims that the test targets already exist.


---

<a id="ncm-rs-010"></a>

# ncm-rs-010 — Implement consolidation, merge/prune and LTM-only survival

Status: planned, not executed.
Owner: Core / consolidation.
Dependencies: ncm-rs-009.
External gates: none.

## Objective

Prove memories survive transfer to LTM rather than being answered indefinitely from STM.

## Owned paths

- `crates/tracedecay-memory-ncm-core/src/consolidation/`
- `crates/tracedecay-memory-ncm-core/tests/consolidation.rs`

## Implementation

1. Implement top-M STM selection, approved intensity floor, kappa transfer, source lineage propagation, both-layer normalization, terrain pour using actual blur and fatigue reduction.
2. Reproduce merge/prune mechanics where safe; preserve immutable source records and conflict lineage even when latent centers merge. Bound pairwise work or page it deterministically.
3. Run the projection alignment test before choosing the production transfer rule: independently initialized direct LTM projection and U*STM projection are not guaranteed to align. If it fails, use retained canonical LTM-basis keys or another explicitly reviewed mapping; no STM/Native fallback may make the test pass.
4. Specify resumable maintenance as build-then-publish state, not partially visible center/terrain updates. Runtime durability is provided by N14.

## Acceptance

1. Store→consolidate→exclude STM at a test seam→recall succeeds for intended associations with the real encoder later in N21.
2. Merge/prune preserve source-support closure and do not merge contradictory textual assertions into an unlabeled fact.
3. Sleep boundaries, terrain pour, normalization and fatigue reduction match reference or approved difference fixtures.
4. Interruptions between maintenance stages never expose mixed generations.

## Verification targets

1. cargo test -p tracedecay-memory-ncm-core --test consolidation --locked

## Sources

- [S03: Biomem center algorithms](https://github.com/BleedingDev/biomem/blob/500847ff65b5d9548b3826fa29bf3ccf8d221147/src/memory_module/memory_centers.py)
- [S05: Biomem consolidation](https://github.com/BleedingDev/biomem/blob/500847ff65b5d9548b3826fa29bf3ccf8d221147/src/memory_module/consolidation.py)
- [S06: Biomem projections](https://github.com/BleedingDev/biomem/blob/500847ff65b5d9548b3826fa29bf3ccf8d221147/src/memory_module/projections.py)
- [S07: Biomem text-memory orchestration](https://github.com/BleedingDev/biomem/blob/500847ff65b5d9548b3826fa29bf3ccf8d221147/src/memory_module/text_memory.py)

## Handoff

Commit one reviewable slice; attach the common completion receipt; do not change shared host paths outside the ownership agreement.

Use the common evidence receipt and ownership rules in `START_HERE.md`. These are task specifications, not claims that the test targets already exist.


---

<a id="ncm-rs-011"></a>

# ncm-rs-011 — Implement record-aware recall, correction and provenance output

Status: planned, not executed.
Owner: Core / record policy.
Dependencies: ncm-rs-007, ncm-rs-003.
External gates: none.

## Objective

Turn learned state into admissible advisory candidates without losing evidence or chronology.

## Owned paths

- `crates/tracedecay-memory-ncm-core/src/records/`
- `crates/tracedecay-memory-ncm-core/src/recall/`
- `crates/tracedecay-memory-ncm-core/tests/record_recall.rs`

## Implementation

1. Keep RecordId/SourceId and explicit valid/superseded/deleted state separate from center slots. Returned text must come from supported records, not decoding arbitrary 128D values into invented text.
2. Combine STM/LTM candidates by stable record/source identity. Do not copy Python’s key-text-only deduplication, which can hide distinct contradictory records or provenance.
3. Return layer, raw activation, distance components, support handles, exact validity metadata and explicit uncalibrated confidence. Define no-match admission using a predeclared relevance policy, not merely softmax rank.
4. Corrections use admitted host evidence and preserve supersession lineage. Provider recency/activation never overrides current code authority; ambiguous conflicts remain visible.

## Acceptance

1. Same key/different assertions remain distinct until an explicit valid correction links them.
2. Unrelated singleton queries do not become high-confidence facts; empty and unavailable are different.
3. Source absence is explicit; tests detect fabricated citations, stale ID reuse and missing support after merge.
4. Record budgets apply before hydration and serialization; no metadata or token overflow bypass.

## Verification targets

1. cargo test -p tracedecay-memory-ncm-core --test record_recall --locked

## Sources

- [S03: Biomem center algorithms](https://github.com/BleedingDev/biomem/blob/500847ff65b5d9548b3826fa29bf3ccf8d221147/src/memory_module/memory_centers.py)
- [S07: Biomem text-memory orchestration](https://github.com/BleedingDev/biomem/blob/500847ff65b5d9548b3826fa29bf3ccf8d221147/src/memory_module/text_memory.py)
- [S14: Existing NCM adapter boundary](https://github.com/BleedingDev/tracedecay/blob/25778c7443cd0cfe257da363da01a56ea1d45d3f/crates/tracedecay-memory-provider-ncm/src/lib.rs)
- [S17: Existing provider-neutral evaluation](https://github.com/BleedingDev/tracedecay/blob/25778c7443cd0cfe257da363da01a56ea1d45d3f/product/evaluation/README.md)

## Handoff

Commit one reviewable slice; attach the common completion receipt; do not change shared host paths outside the ownership agreement.

Use the common evidence receipt and ownership rules in `START_HERE.md`. These are task specifications, not claims that the test targets already exist.


---

<a id="ncm-rs-012"></a>

# ncm-rs-012 — Implement the real native multilingual text encoder

Status: planned, not executed.
Owner: Runtime / embedding.
Dependencies: ncm-rs-002, ncm-rs-004.
External gates: none.

## Objective

Ensure production uses actual model inference and the same input representation as the reference.

## Owned paths

- `crates/tracedecay-memory-ncm-runtime/src/embedding/`
- `crates/tracedecay-memory-ncm-runtime/tests/real_embeddings.rs`
- `product/ncm/reference/embedding-manifest.json`

## Implementation

1. Pin sentence-transformers/paraphrase-multilingual-MiniLM-L12-v2 model revision, tokenizer, weights/export, pooling, normalization and truncation. Use real native Rust inference through the reviewed backend; Python must not be launched by production.
2. Compare token IDs, attention mask, masked mean pooling and normalized 384D outputs against Python. The primary model card describes max_seq_length=128; do not substitute 512 based on unrelated host settings.
3. Load only verified local artifacts during operations. Explicit installation may fetch approved artifacts, but handshake/recall cannot download models or silently fall back online.
4. Port the inspected EmotionExtractor behavior only as a versioned heuristic; preserve explicit four-channel inputs and neutral defaults. Never call heuristic salience a measurement of a human emotional state.

## Acceptance

1. English/Czech, code identifiers, Unicode, empty input and truncation-boundary fixtures pass with real weights.
2. Missing model, corrupt tokenizer, dimension mismatch and incompatible projection identity return typed not-ready/incompatible outcomes.
3. Hash/random/token-count stand-ins are confined to named unit-test doubles and cannot satisfy real-model or release gates.
4. Warm/cold inference, cancellation/kill response, model memory and artifact identity are measured.

## Verification targets

1. cargo test -p tracedecay-memory-ncm-runtime --test real_embeddings --locked

## Sources

- [S02: Biomem configuration](https://github.com/BleedingDev/biomem/blob/500847ff65b5d9548b3826fa29bf3ccf8d221147/src/memory_module/config.py)
- [S06: Biomem projections](https://github.com/BleedingDev/biomem/blob/500847ff65b5d9548b3826fa29bf3ccf8d221147/src/memory_module/projections.py)
- [S08: Biomem real text embedding and emotion extraction](https://github.com/BleedingDev/biomem/blob/500847ff65b5d9548b3826fa29bf3ccf8d221147/src/memory_module/embedder.py)
- [S18: MiniLM primary model card](https://huggingface.co/sentence-transformers/paraphrase-multilingual-MiniLM-L12-v2)

## Handoff

Commit one reviewable slice; attach the common completion receipt; do not change shared host paths outside the ownership agreement.

Use the common evidence receipt and ownership rules in `START_HERE.md`. These are task specifications, not claims that the test targets already exist.


---

<a id="ncm-rs-013"></a>

# ncm-rs-013 — Implement provider-owned durable store and source capsules

Status: planned, not executed.
Owner: Runtime / storage.
Dependencies: ncm-rs-004.
External gates: none.

## Objective

Provide an atomic, replayable ownership boundary independent of TraceDecay’s stores.

## Owned paths

- `crates/tracedecay-memory-ncm-runtime/src/store/`
- `crates/tracedecay-memory-ncm-runtime/tests/store.rs`

## Implementation

1. Use an admitted provider state root and the repository’s approved private-filesystem/SQLite primitives where compatible. No paths are derived directly from request text, and no host database is opened.
2. Implement transactional storage for namespace metadata, source capsules, committed events, idempotency rows, compatibility identity, privacy epochs and state generation/checkpoint metadata.
3. Snapshots/checkpoints must contain all relevant tensors, matrices, masks, IDs, source support, record validity, counters, fatigue and consolidation scheduling. Choose one canonical durable authority and a precise fsync/commit point.
4. Bound snapshot bytes, journal growth, replay work and resident views. Never discard replay information still needed to remove a retained source’s influence. Reserved capacity must permit privacy and recovery operations under ingestion pressure.

## Acceptance

1. Fault injection around durable writes/reopen yields a complete prior state or complete committed state, never partial Ready.
2. Corrupt/incompatible state fails closed; fresh-empty is allowed only through an explicit new-store path, not a catch-all load exception.
3. Opening a second writer is refused or serialized; namespace paths cannot escape the admitted root.
4. Storage-accounting tests include WAL, temporary files, checkpoints, source capsules, tombstones and cached embeddings.

## Verification targets

1. cargo test -p tracedecay-memory-ncm-runtime --test store --locked

## Sources

- [S07: Biomem text-memory orchestration](https://github.com/BleedingDev/biomem/blob/500847ff65b5d9548b3826fa29bf3ccf8d221147/src/memory_module/text_memory.py)
- [S14: Existing NCM adapter boundary](https://github.com/BleedingDev/tracedecay/blob/25778c7443cd0cfe257da363da01a56ea1d45d3f/crates/tracedecay-memory-provider-ncm/src/lib.rs)

## Handoff

Commit one reviewable slice; attach the common completion receipt; do not change shared host paths outside the ownership agreement.

Use the common evidence receipt and ownership rules in `START_HERE.md`. These are task specifications, not claims that the test targets already exist.


---

<a id="ncm-rs-014"></a>

# ncm-rs-014 — Implement end-to-end engine transactions and exactly-once effects

Status: planned, not executed.
Owner: Runtime / engine.
Dependencies: ncm-rs-010, ncm-rs-011, ncm-rs-012, ncm-rs-013.
External gates: none.

## Objective

Connect real text input to learning, durable commit and read-only recall with correct retry semantics.

## Owned paths

- `crates/tracedecay-memory-ncm-runtime/src/engine/`
- `crates/tracedecay-memory-ncm-runtime/tests/engine_transactions.rs`

## Implementation

1. Translate admitted normalized observations into typed source capsules, key/value inputs, explicit signals, real embeddings and core transitions. Do not call the browser/PAM protocol or an LLM to manufacture source truth.
2. Bind namespace+idempotency key to a canonical payload digest. Same key/same payload replays the original receipt with no second effect; same key/different payload is a conflict.
3. Validate and compute a bounded candidate transition before commit; atomically persist effect/receipt/source mapping and publish the committed state. Cancellation before commit has no effect; loss of response after commit does not undo the commit.
4. Implement explicit feedback and correction effects separately from recall. Advance logical time only once per eligible committed event. Restart recovery must reconcile commit-to-ack ambiguity.
5. Keep durable publication coherent with live reads: after a successful mutation acknowledgement, new reads observe at least that commit; a crash or publication failure after commit yields truthful committed/effect-unknown handling and recovery, never a false no-effect retry that learns twice.

## Acceptance

1. A real-text observe→recall journey returns supported content with nonzero learned state and no Python subprocess.
2. Duplicate delivery changes neither intensity nor source count nor terrain/fatigue/tick a second time.
3. Crash before commit, after commit and before ack, after publication and during checkpoint all have exact effect outcomes.
4. A failed embed/readiness step never produces a successful observation receipt.

## Verification targets

1. cargo test -p tracedecay-memory-ncm-runtime --test engine_transactions --locked

## Sources

- [S03: Biomem center algorithms](https://github.com/BleedingDev/biomem/blob/500847ff65b5d9548b3826fa29bf3ccf8d221147/src/memory_module/memory_centers.py)
- [S05: Biomem consolidation](https://github.com/BleedingDev/biomem/blob/500847ff65b5d9548b3826fa29bf3ccf8d221147/src/memory_module/consolidation.py)
- [S07: Biomem text-memory orchestration](https://github.com/BleedingDev/biomem/blob/500847ff65b5d9548b3826fa29bf3ccf8d221147/src/memory_module/text_memory.py)
- [S14: Existing NCM adapter boundary](https://github.com/BleedingDev/tracedecay/blob/25778c7443cd0cfe257da363da01a56ea1d45d3f/crates/tracedecay-memory-provider-ncm/src/lib.rs)

## Handoff

Commit one reviewable slice; attach the common completion receipt; do not change shared host paths outside the ownership agreement.

Use the common evidence receipt and ownership rules in `START_HERE.md`. These are task specifications, not claims that the test targets already exist.


---

<a id="ncm-rs-015"></a>

# ncm-rs-015 — Implement deletion through mixed state and bounded maintenance

Status: planned, not executed.
Owner: Runtime / privacy and retention.
Dependencies: ncm-rs-014.
External gates: none.

## Objective

Remove a source’s recoverable and learned influence, not just its visible text row.

## Owned paths

- `crates/tracedecay-memory-ncm-runtime/src/privacy/`
- `crates/tracedecay-memory-ncm-runtime/src/maintenance/`
- `crates/tracedecay-memory-ncm-runtime/tests/deletion.rs`
- `crates/tracedecay-memory-ncm-runtime/tests/maintenance.rs`

## Implementation

1. Map deletion to all provider-owned copies, embeddings, center-support links, merged records, terrain influence, feedback and source capsules, including replicas created by authorized session replay.
2. For nonlinear mixed state, v1 atomically fences the affected namespace, rebuilds from authorized retained inputs excluding revoked sources, and publishes only the sanitized generation. Do not attempt unproven subtraction of a source from nonlinear normalization/diffusion.
3. Keep privacy epoch/revocation authority outside restorable checkpoints. Reapply it during every restore/replay and prevent stale journal events from resurrecting content. Bound rebuild work and report maintenance continuation.
4. Define deletion granularity and supported physical-erasure scope explicitly. Quarantine/unlink controlled old snapshots and caches as required; never claim to erase independent exported files or guarantee SSD-level secure erasure.

## Acceptance

1. Delete a source after consolidation and merge; LTM-only recall, explain, export, restart and allowed replay cannot expose it.
2. Deletion racing with observe/recall/checkpoint is linearized: after acknowledged logical deletion no new result contains revoked evidence.
3. Interrupted rebuild stays unavailable/maintenance-pending until safe; it does not serve stale state as healthy.
4. Privacy/reconstruction quota exhaustion is visible, preserves safety and never yields a false deletion-complete receipt.

## Verification targets

1. cargo test -p tracedecay-memory-ncm-runtime --test deletion --test maintenance --locked

## Sources

- [S03: Biomem center algorithms](https://github.com/BleedingDev/biomem/blob/500847ff65b5d9548b3826fa29bf3ccf8d221147/src/memory_module/memory_centers.py)
- [S04: Biomem terrain](https://github.com/BleedingDev/biomem/blob/500847ff65b5d9548b3826fa29bf3ccf8d221147/src/memory_module/terrain_3d.py)
- [S05: Biomem consolidation](https://github.com/BleedingDev/biomem/blob/500847ff65b5d9548b3826fa29bf3ccf8d221147/src/memory_module/consolidation.py)
- [S07: Biomem text-memory orchestration](https://github.com/BleedingDev/biomem/blob/500847ff65b5d9548b3826fa29bf3ccf8d221147/src/memory_module/text_memory.py)
- [S14: Existing NCM adapter boundary](https://github.com/BleedingDev/tracedecay/blob/25778c7443cd0cfe257da363da01a56ea1d45d3f/crates/tracedecay-memory-provider-ncm/src/lib.rs)

## Handoff

Commit one reviewable slice; attach the common completion receipt; do not change shared host paths outside the ownership agreement.

Use the common evidence receipt and ownership rules in `START_HERE.md`. These are task specifications, not claims that the test targets already exist.


---

<a id="ncm-rs-016"></a>

# ncm-rs-016 — Implement versioned export/restore with non-resurrection

Status: planned, not executed.
Owner: Runtime / snapshot compatibility.
Dependencies: ncm-rs-015.
External gates: none.

## Objective

Make restart and portable state transfer truthful and bounded.

## Owned paths

- `crates/tracedecay-memory-ncm-runtime/src/snapshot/`
- `crates/tracedecay-memory-ncm-runtime/tests/snapshot.rs`

## Implementation

1. Implement the new Rust snapshot format with explicit schema, algorithm/config/model/projection identity, lengths, checksums, namespace and privacy epoch.
2. Validate every section and quota into a staging location, then atomically publish. Check newer revocations before serving; imported state cannot supply its own trusted revocation authority.
3. Restore all scheduling and projection state, especially steps_since_consolidation and STM→LTM mapping. Cold restart must not silently instantiate new projection weights.
4. Do not support arbitrary .pt/pickle or claim .bdbm compatibility in v1. An eventual legacy importer needs a separately reviewed, isolated conversion path; own-format export/restore is mandatory now.

## Acceptance

1. Uninterrupted and restart/snapshot-split traces have equivalent next mutation, recall and consolidation timing.
2. Truncated, oversized, malicious-length, wrong-model, wrong-namespace and checksum-corrupt snapshots are refused without changing current state.
3. A pre-deletion snapshot restored after deletion cannot resurrect revoked sources.
4. Restoring into a fresh profile with no trusted deletion lineage is refused or requires explicitly authorized sanitized transfer; no blanket privacy claim.

## Verification targets

1. cargo test -p tracedecay-memory-ncm-runtime --test snapshot --locked

## Sources

- [S05: Biomem consolidation](https://github.com/BleedingDev/biomem/blob/500847ff65b5d9548b3826fa29bf3ccf8d221147/src/memory_module/consolidation.py)
- [S06: Biomem projections](https://github.com/BleedingDev/biomem/blob/500847ff65b5d9548b3826fa29bf3ccf8d221147/src/memory_module/projections.py)
- [S07: Biomem text-memory orchestration](https://github.com/BleedingDev/biomem/blob/500847ff65b5d9548b3826fa29bf3ccf8d221147/src/memory_module/text_memory.py)
- [S14: Existing NCM adapter boundary](https://github.com/BleedingDev/tracedecay/blob/25778c7443cd0cfe257da363da01a56ea1d45d3f/crates/tracedecay-memory-provider-ncm/src/lib.rs)

## Handoff

Commit one reviewable slice; attach the common completion receipt; do not change shared host paths outside the ownership agreement.

Use the common evidence receipt and ownership rules in `START_HERE.md`. These are task specifications, not claims that the test targets already exist.


---

<a id="ncm-rs-017"></a>

# ncm-rs-017 — Implement the supervised Rust worker and bounded wire transport

Status: planned, not executed.
Owner: Runtime / lifecycle.
Dependencies: ncm-rs-014.
External gates: none.

## Objective

Isolate native inference/storage failures while fitting the existing host supervision contract.

## Owned paths

- `crates/tracedecay-memory-ncm-runtime/src/worker/`
- `crates/tracedecay-memory-ncm-runtime/src/wire/`
- `crates/tracedecay-memory-ncm-runtime/src/client/`
- `crates/tracedecay-memory-ncm-runtime/src/bin/tracedecay-ncm-worker.rs`
- `crates/tracedecay-memory-ncm-runtime/tests/worker.rs`

## Implementation

1. Provide a real Rust worker executable with a versioned length-delimited local pipe protocol. Wire payloads contain opaque namespace/source handles and typed engine requests, not raw host scope or caller filesystem paths.
2. Use one bounded owner/mailbox and explicit state-root admission. Reuse host launch/cancellation/restart facilities through a narrow client contract; do not build a second general supervisor.
3. Bound process count, queued frames/bytes, stdout framing, stderr diagnostics, readers, loaded namespaces and model residency. Reject oversized frames before allocation.
4. On deadline/cancellation, stop cooperative work or terminate the worker within a fixed escalation budget. Return unknown effect if commit cannot be determined; reconcile by idempotency receipt after restart.

## Acceptance

1. Real process death, hangs, malformed replies, full pipe/backpressure, deadline expiry and restart budget exhaustion are exercised.
2. No orphan worker or unbounded detached task survives shutdown; native inference cannot monopolize a Tokio host thread.
3. The worker refuses wrong protocol/epoch/model identities and cannot launch when the provider is Disabled.
4. Process-alive is distinct from loaded-state and observe/recall readiness.

## Verification targets

1. cargo test -p tracedecay-memory-ncm-runtime --test worker --locked

## Sources

- [S14: Existing NCM adapter boundary](https://github.com/BleedingDev/tracedecay/blob/25778c7443cd0cfe257da363da01a56ea1d45d3f/crates/tracedecay-memory-provider-ncm/src/lib.rs)

## Handoff

Commit one reviewable slice; attach the common completion receipt; do not change shared host paths outside the ownership agreement.

Use the common evidence receipt and ownership rules in `START_HERE.md`. These are task specifications, not claims that the test targets already exist.


---

<a id="ncm-rs-018"></a>

# ncm-rs-018 — Implement the real NcmCognitiveSurface adapter

Status: planned, not executed.
Owner: Provider adapter owner.
Dependencies: ncm-rs-016, ncm-rs-017.
External gates: none.

## Objective

Replace an absent production implementation behind the existing surface, not the conformance doubles.

## Owned paths

- `crates/tracedecay-memory-provider-ncm/src/lib.rs`
- `crates/tracedecay-memory-provider-ncm/src/rust_backend/`
- `crates/tracedecay-memory-provider-ncm/tests/rust_backend.rs`
- `crates/tracedecay-memory-provider-ncm/Cargo.toml`
- `crates/tracedecay-memory-provider-ncm/README.md`

## Implementation

1. Implement RustNcmSurface against the worker client and retain NcmProviderAdapter’s scope projection, challenge proof, readiness epoch, capability, byte-budget and effect validation.
2. Keep runtime→core and adapter→runtime dependencies acyclic. Registry construction and host mounting remain N24/N25; backend tests instantiate the existing adapter directly.
3. Map each actually supported ProviderOperation to the real worker. Return truthful build/config/model/state identity; advertise capabilities only when their implementation and negotiated requirements are ready.
4. Preserve opaque extension handling and host-owned provenance hydration. No fallback to Native or a canned result. Keep fake providers in tests and reject their artifact identities from production readiness.

## Acceptance

1. Observe, recall, feedback/correction, maintenance, inspect, delete and own-format snapshot paths are exercised through NcmProviderAdapter with real durable state.
2. Readiness replacement invalidates old epochs; normal commits obey the frozen state-generation semantics.
3. Malformed post-dispatch mutation replies preserve effect_unknown and a reconciliation path, not false no-effect.
4. Adapter-only implementation is not counted as shipped host integration.

## Verification targets

1. cargo test -p tracedecay-memory-provider-ncm --features rust-backend --test rust_backend --locked

## Sources

- [S14: Existing NCM adapter boundary](https://github.com/BleedingDev/tracedecay/blob/25778c7443cd0cfe257da363da01a56ea1d45d3f/crates/tracedecay-memory-provider-ncm/src/lib.rs)

## Handoff

Commit one reviewable slice; attach the common completion receipt; do not change shared host paths outside the ownership agreement.

Use the common evidence receipt and ownership rules in `START_HERE.md`. These are task specifications, not claims that the test targets already exist.


---

<a id="ncm-rs-019"></a>

# ncm-rs-019 — Run real-provider conformance and adversarial scope/privacy tests

Status: planned, not executed.
Owner: Independent verification owner.
Dependencies: ncm-rs-018.
External gates: none.

## Objective

Earn conformance on the actual Rust process and stores.

## Owned paths

- `crates/tracedecay-memory-provider-ncm/tests/rust_backend_conformance.rs`
- `product/ncm/conformance/`

## Implementation

1. Reuse tracedecay-memory-conformance and existing adversarial fixtures with the real backend. Do not write a friendlier parallel conformance definition.
2. Test every supported operation under wrong profile/project/worktree/session/branch, stale epoch, replay, cancellation, corruption and contradictory inputs.
3. Exercise deletion through merged centers/terrain plus stale snapshot restore, real worker termination after commit-before-ack and bounded resource saturation.
4. Separate deterministic kernel doubles from real encoder/process evidence; unsupported required capabilities block readiness rather than produce skipped green tests.

## Acceptance

1. Every mandatory capability has executed test IDs and exact pass/fail/skip accounting on the built artifact.
2. Scope leaks, fabricated provenance, duplicate committed effects and deleted-source recall are zero observed violations in the declared test population.
3. A deliberately faulty backend is rejected by the same tests.
4. Conformance receipts name source tree, worker binary, algorithm, model and state schema identities.

## Verification targets

1. cargo test -p tracedecay-memory-provider-ncm --features rust-backend --test rust_backend_conformance --locked

## Sources

- [S14: Existing NCM adapter boundary](https://github.com/BleedingDev/tracedecay/blob/25778c7443cd0cfe257da363da01a56ea1d45d3f/crates/tracedecay-memory-provider-ncm/src/lib.rs)
- [S17: Existing provider-neutral evaluation](https://github.com/BleedingDev/tracedecay/blob/25778c7443cd0cfe257da363da01a56ea1d45d3f/product/evaluation/README.md)

## Handoff

Commit one reviewable slice; attach the common completion receipt; do not change shared host paths outside the ownership agreement.

Use the common evidence receipt and ownership rules in `START_HERE.md`. These are task specifications, not claims that the test targets already exist.


---

<a id="ncm-rs-020"></a>

# ncm-rs-020 — Measure bounded resource use, latency and saturation behavior

Status: planned, not executed.
Owner: Performance / reliability verifier.
Dependencies: ncm-rs-018.
External gates: none.

## Objective

Prove the fixed-capacity kernel does not hide unbounded logs, text, namespaces or worker memory.

## Owned paths

- `crates/tracedecay-memory-ncm-runtime/benches/ncm_scale.rs`
- `scripts/product/ncm/benchmark/`
- `product/ncm/performance/`

## Implementation

1. Benchmark empty, sparse, full 512/4096 center state; 1/4 resident namespaces; 10k/100k operation traces; varied record sizes and adversarial repeated unique IDs.
2. Measure pre-embedded kernels, real encoding, IPC, durable commit, end-to-end recall and maintenance separately. Include p50/p95/p99, peak RSS, allocated bytes, disk including WAL/snapshots, queue occupancy and cancellation completion.
3. Run mixed observe/recall/consolidate/delete/restart workloads under the frozen hardware/profile/feature set. Missing hardware/model is blocked environment, not a waived test.
4. Compare to the pinned Python reference and a simple flat-vector baseline only where workloads/representation/semantics match. Do not claim microsecond full-text recall from kernel-only results.

## Acceptance

1. Hard quotas hold or operations reject before exceeding them; state retention/replay obligations survive journal compaction.
2. The N03 budget manifest’s predeclared limits are met; any proposed limit change is a reviewed versioned decision before rerunning acceptance.
3. No warm read or maintenance job can starve host deadlines or create an unlimited backlog.
4. Long-lived tests include fixed kernel capacity and separately bounded ancillary storage; no infinite lossless-memory claim.

## Verification targets

1. cargo bench -p tracedecay-memory-ncm-runtime --bench ncm_scale --locked
2. ncm_saturation_journey (planned)

## Sources

- [S02: Biomem configuration](https://github.com/BleedingDev/biomem/blob/500847ff65b5d9548b3826fa29bf3ccf8d221147/src/memory_module/config.py)
- [S17: Existing provider-neutral evaluation](https://github.com/BleedingDev/tracedecay/blob/25778c7443cd0cfe257da363da01a56ea1d45d3f/product/evaluation/README.md)

## Handoff

Commit one reviewable slice; attach the common completion receipt; do not change shared host paths outside the ownership agreement.

Use the common evidence receipt and ownership rules in `START_HERE.md`. These are task specifications, not claims that the test targets already exist.


---

<a id="ncm-rs-021"></a>

# ncm-rs-021 — Evaluate scientific fidelity and actual coding-memory usefulness

Status: planned, not executed.
Owner: Evaluation owner independent of implementation.
Dependencies: ncm-rs-018, ncm-rs-002.
External gates: none.

## Objective

Distinguish a faithful implementation from a useful product and from unsupported scientific claims.

## Owned paths

- `product/ncm/evaluation/`
- `scripts/product/ncm/evaluate/`

## Implementation

1. Run matched numerical/behavioral fixtures against pinned Biomem, accounting for approved corrections. Add real-text store/recall, paraphrase, full-capacity interference, stale correction and LTM-only retention cases.
2. Reuse the existing nine-scenario corpus and metric definitions. Compare Rust NCM with Python Biomem, Native, no memory and explicit documentation under matched inputs; unavailable host baselines are pending until the joined host evaluation.
3. Predeclare dataset split, scoring rubric, minimum acceptable relevance, safety ceilings, task outcomes, latency and token budgets. Keep a held-out set; do not tune on final evaluation answers.
4. Ablate terrain contribution, corrected blur and consolidation. Test that the feature is actually invoked; lack of measured benefit is reported, never relabeled as proof of value. External prompt integration is not decoder-internal attention gating.

## Acceptance

1. Real-encoder LTM-only recall passes without STM, Native, keyword-only or source-file fallback.
2. Stale/cross-scope/deleted/corrupt content fails admission with visible reasons; indeterminate labels do not count as safety passes.
3. Kernel fidelity, algorithm corrections, provider behavior and task benefit have separate results.
4. Observer evaluation cannot claim improved agent outcomes; joined active-mode benefit is measured only in N25.

## Verification targets

1. ncm_reference_comparison (planned)
2. ncm_coding_memory_evaluation (planned)

## Sources

- [S10: Persistent Memory for Decoder-Only Transformers: Latent Terrain, Diffusion, Homeostasis, and Emotional Stabilization](https://zenodo.org/records/18198327)
- [S11: Implementation of Persistent Latent Memory for Decoder Transformers](https://zenodo.org/records/18267378)
- [S17: Existing provider-neutral evaluation](https://github.com/BleedingDev/tracedecay/blob/25778c7443cd0cfe257da363da01a56ea1d45d3f/product/evaluation/README.md)
- [S18: MiniLM primary model card](https://huggingface.co/sentence-transformers/paraphrase-multilingual-MiniLM-L12-v2)

## Handoff

Commit one reviewable slice; attach the common completion receipt; do not change shared host paths outside the ownership agreement.

Use the common evidence receipt and ownership rules in `START_HERE.md`. These are task specifications, not claims that the test targets already exist.


---

<a id="ncm-rs-022"></a>

# ncm-rs-022 — Accept the independently usable backend and package its evidence

Status: planned, not executed.
Owner: NCM branch coordinator / reviewer.
Dependencies: ncm-rs-019, ncm-rs-020, ncm-rs-021.
External gates: none.

## Objective

Deliver a real standalone backend without waiting for or pretending to complete host stabilization.

## Owned paths

- `.github/workflows/product-ncm-backend.yml`
- `scripts/product/ncm/check-backend.py`
- `product/ncm/receipts/backend/`
- `product/ncm/README.md`

## Implementation

1. Create a backend-only validation entrypoint and pinned CI lane for the two new packages and existing adapter. Run independent diagnostics without changing the host’s failing policy tests or weakening promotion requirements.
2. Verify the actual worker artifact includes the real encoder/backend and excludes Python-runtime/test-double substitutions. Document artifact installation and local model acquisition with hashes.
3. Assemble source/model/projection identities, numerical differences, conformance, crash/deletion, latency/storage and isolated text journey receipts.
4. Review the allowed-path diff. Leave registry registration, default features, accepted upstream floor and shared Beads updates untouched until N23.

## Acceptance

1. A fresh isolated state root supports real observe→consolidate→restart→recall→delete through the worker/adapter.
2. Backend gates execute nonempty expected tests and pass; missing real-model evidence is a blocker.
3. Status is explicitly backend-accepted, host-not-yet-integrated; no claim that the product checkpoint is demo-ready.
4. The integration owner receives a small manifest/lock patch, adapter patch, exact API requirements and all artifact identities.

## Verification targets

1. python3 scripts/product/ncm/check-backend.py (planned entrypoint)
2. backend_artifact_identity_test

## Sources

- [S14: Existing NCM adapter boundary](https://github.com/BleedingDev/tracedecay/blob/25778c7443cd0cfe257da363da01a56ea1d45d3f/crates/tracedecay-memory-provider-ncm/src/lib.rs)
- [S19: Existing upstream convergence procedure](https://github.com/BleedingDev/tracedecay/blob/25778c7443cd0cfe257da363da01a56ea1d45d3f/product/upstream/README.md)

## Handoff

Commit one reviewable slice; attach the common completion receipt; do not change shared host paths outside the ownership agreement.

Use the common evidence receipt and ownership rules in `START_HERE.md`. These are task specifications, not claims that the test targets already exist.


---

<a id="ncm-rs-023"></a>

# ncm-rs-023 — Join the stabilized host and reconcile shared ownership

Status: planned, not executed.
Owner: Shared integration owner.
Dependencies: ncm-rs-022.
External gates: HOST-STABLE.

## Objective

Merge independent work without undoing the other agent’s fixes or claiming their evidence applies automatically.

## Owned paths

- `Cargo.toml`
- `Cargo.lock`
- `product/upstream/convergence-map.json`
- `product/upstream/patch-footprint-policy.json`
- `product/architecture/memory-dependency-policy.json`
- `.beads/issues.jsonl`
- `product/ncm/receipts/integration/`

## Implementation

1. Consume a specific accepted host commit/tree from HOST-STABLE. Keep the new backend commits reviewable; do not pull a moving upstream head or rewrite shared history.
2. Reconcile workspace/dependency and ownership changes by semantic role. Preserve new deadline, retention and policy-test fixes from the stabilization branch.
3. Import the new task work package through the existing Beads process, linking older descoped records as superseded intent. Do not double count old closures or modify unrelated tasks.
4. Rerun backend gates and the relevant host feature/dependency/ownership/generated checks on the actual combined tree.

## Acceptance

1. All conflicts have owner, rationale and invariant-preservation evidence.
2. Current ownership and dependency checks pass without blanket allowlists.
3. The joined tree is recorded separately from backend-accepted and host-accepted source trees.
4. No obsolete result is reused solely because a branch name is unchanged.

## Verification targets

1. existing ownership and dependency gates
2. combined backend gate

## Sources

- [S15: Versioned Beads plan at checkpoint](https://github.com/BleedingDev/tracedecay/blob/25778c7443cd0cfe257da363da01a56ea1d45d3f/.beads/issues.jsonl)
- [S19: Existing upstream convergence procedure](https://github.com/BleedingDev/tracedecay/blob/25778c7443cd0cfe257da363da01a56ea1d45d3f/product/upstream/README.md)

## Handoff

Commit one reviewable slice; attach the common completion receipt; do not change shared host paths outside the ownership agreement.

Use the common evidence receipt and ownership rules in `START_HERE.md`. These are task specifications, not claims that the test targets already exist.


---

<a id="ncm-rs-024"></a>

# ncm-rs-024 — Mount NCM in Observer mode and prove authorized session reuse

Status: planned, not executed.
Owner: Host composition / observation owner.
Dependencies: ncm-rs-023.
External gates: HOST-OBSERVE.

## Objective

Exercise real host event delivery while proving Observer output cannot affect the agent.

## Owned paths

- `crates/tracedecay-memory-provider-registry/Cargo.toml`
- `crates/tracedecay-memory-provider-registry/src/ncm_mount/`
- `crates/tracedecay-memory-provider-registry/src/lib.rs`
- `crates/tracedecay/src/daemon/product_memory_provider/`
- `crates/tracedecay-cli/tests/product_memory_provider_ncm_observer_journey.rs`
- `product/ncm/receipts/observer/`
- `Cargo.toml`
- `Cargo.lock`
- `product/upstream/convergence-map.json`
- `product/upstream/patch-footprint-policy.json`
- `product/architecture/memory-dependency-policy.json`

## Implementation

1. Mount the real adapter/worker only through the provider registry and existing supervision. Add the smallest opt-in feature/configuration change; exact production composition paths must be discovered on the stabilized tree rather than assumed from the proposed new module name.
2. Drive real CLI/daemon hook ingestion after startup has quiesced, with a negative control against startup import. Inspect real NCM durable receipts, not a mock dispatcher counter.
3. Implement the N03 authorized-history replay/share bridge for same-worktree later-session use. Preserve the requesting session’s exact scope and original-source attribution; deny sibling/unrelated worktrees unless explicitly authorized by existing host policy.
4. Compare final prompt/native-fact/action outputs with NCM Observer enabled and disabled under deterministic inputs. Run shadow recall read-only, and account for resource consumption without letting it alter active selection.
5. The integration coordinator updates the exact ownership/feature/lock entries for this mount on this task’s tree. Shared policy metadata remains an ordered change, not a blanket permission or a waiver of failing checks.

## Acceptance

1. Each accepted observation has exactly one NCM effect across retries and daemon/worker restart.
2. Observer mode changes no prompt bytes, Native facts or externally visible agent actions; provider outputs cannot be selected through fallback.
3. A later authorized session recalls/replays legitimate source knowledge; forbidden sessions/worktrees do not. No silent namespace broadening.
4. If current host authority cannot express safe sharing/replay, active release is blocked pending a narrow owner-reviewed contract change, not faked by skipping session fields.

## Verification targets

1. product_memory_provider_ncm_observer_journey (planned real-process target)

## Sources

- [S14: Existing NCM adapter boundary](https://github.com/BleedingDev/tracedecay/blob/25778c7443cd0cfe257da363da01a56ea1d45d3f/crates/tracedecay-memory-provider-ncm/src/lib.rs)
- [S16: Existing host journey](https://github.com/BleedingDev/tracedecay/blob/25778c7443cd0cfe257da363da01a56ea1d45d3f/crates/tracedecay-cli/tests/product_memory_provider_claude_host_journey.rs)
- [S17: Existing provider-neutral evaluation](https://github.com/BleedingDev/tracedecay/blob/25778c7443cd0cfe257da363da01a56ea1d45d3f/product/evaluation/README.md)

## Handoff

Commit one reviewable slice; attach the common completion receipt; do not change shared host paths outside the ownership agreement.

Use the common evidence receipt and ownership rules in `START_HERE.md`. These are task specifications, not claims that the test targets already exist.


---

<a id="ncm-rs-025"></a>

# ncm-rs-025 — Enable guarded active NCM and run joined comparative evaluation

Status: planned, not executed.
Owner: Host recall owner / evaluation owner.
Dependencies: ncm-rs-024.
External gates: HOST-RECALL.

## Objective

Prove NCM output is admitted safely and earns its cost before offering active mode.

## Owned paths

- `crates/tracedecay-memory-provider-registry/src/ncm_mount/`
- `crates/tracedecay/src/daemon/product_memory_provider/`
- `crates/tracedecay-cli/tests/product_memory_provider_ncm_active_journey.rs`
- `product/ncm/receipts/active/`
- `Cargo.toml`
- `Cargo.lock`
- `product/upstream/convergence-map.json`
- `product/upstream/patch-footprint-policy.json`
- `product/architecture/memory-dependency-policy.json`

## Implementation

1. Use explicit opt-in provider selection. Preserve configured provider identity; Native canonical facts remain separate, never an undeclared fallback for failed NCM recall.
2. Exercise real host admission, provenance hydration, selection, tokenizer and pack/trace persistence with NCM results. Test cancellation/budget exhaustion at actual reachable host lookup paths.
3. Complete the matched Native/no-memory/documentation/Python/Rust coding evaluation on the joined tree, with held-out tasks and predeclared thresholds. Measure task benefit, harmful stale recall, scope/deletion safety, repeated discovery, latency and context cost.
4. Implement a tested disable/quarantine rollback that leaves Native authoritative and does not destroy valid NCM state. Do not change the default mode.
5. The integration coordinator updates the exact ownership/feature/lock entries for this mount on this task’s tree. Shared policy metadata remains an ordered change, not a blanket permission or a waiver of failing checks.

## Acceptance

1. Current code and required host evidence cannot be displaced by NCM volume, activation or unsupported provenance.
2. Stale correction, restart, provider corruption/failure, timeout, deletion and cross-worktree journeys pass with nonempty selections.
3. All safety criteria pass and usefulness meets the predeclared release threshold; inconclusive benefit leaves NCM experimental/Observer rather than falsely complete.
4. The active artifact, runtime mode, model and source identities match its evidence.

## Verification targets

1. product_memory_provider_ncm_active_journey (planned real-process target)
2. joined coding-memory comparison

## Sources

- [S14: Existing NCM adapter boundary](https://github.com/BleedingDev/tracedecay/blob/25778c7443cd0cfe257da363da01a56ea1d45d3f/crates/tracedecay-memory-provider-ncm/src/lib.rs)
- [S16: Existing host journey](https://github.com/BleedingDev/tracedecay/blob/25778c7443cd0cfe257da363da01a56ea1d45d3f/crates/tracedecay-cli/tests/product_memory_provider_claude_host_journey.rs)
- [S17: Existing provider-neutral evaluation](https://github.com/BleedingDev/tracedecay/blob/25778c7443cd0cfe257da363da01a56ea1d45d3f/product/evaluation/README.md)

## Handoff

Commit one reviewable slice; attach the common completion receipt; do not change shared host paths outside the ownership agreement.

Use the common evidence receipt and ownership rules in `START_HERE.md`. These are task specifications, not claims that the test targets already exist.


---

<a id="ncm-rs-026"></a>

# ncm-rs-026 — Verify installed artifacts, platform support and release decision

Status: planned, not executed.
Owner: Release owner / independent reviewer.
Dependencies: ncm-rs-025.
External gates: HOST-RELEASE.

## Objective

Finish with an installable, honestly labeled product capability rather than a green development harness.

## Owned paths

- `product/ncm/release/`
- `docs/product/ncm-rust.md`
- `scripts/product/ncm/verify-installed.py`

## Implementation

1. Use the repository’s existing packaging path to include the pinned Rust worker and model acquisition manifest. Do not start unrelated browser, desktop UI or packaging-system work.
2. Verify clean install, existing-profile upgrade, process restart, disabled rollback and artifact identity on every platform advertised as supported. Backend algorithm portability alone is not packaging proof.
3. Confirm test-transport and fake-provider artifacts cannot satisfy production readiness. Record actual binary paths/hashes, features and worker protocol compatibility.
4. Issue a go/no-go with separate numerical fidelity, backend conformance, host safety, platform, and usefulness verdicts. Keep failed or blocked gates visible.
5. At the host join, identify the existing packaging files that actually need worker inclusion and add their exact paths to this node after release-owner approval. The external HOST-RELEASE gate covers prerequisites and authority; new NCM packaging and installed-artifact results are outputs of this task, not circular prerequisites.

## Acceptance

1. The installed CLI and real Rust worker reproduce the accepted active journey on the declared supported platform matrix.
2. Disablement returns to Native-only behavior without accidental NCM fallback or state deletion.
3. Documentation describes a Biomem-based independent Rust implementation, not an official OpenTechLab product or a proven decoder-internal integration.
4. Every completed task is backed by evidence on the final joined tree or an explicitly justified unchanged-tree reuse.

## Verification targets

1. installed_ncm_journey (planned)
2. python3 scripts/product/ncm/verify-installed.py (planned entrypoint)

## Sources

- [S09: Biomem MIT notice](https://github.com/BleedingDev/biomem/blob/500847ff65b5d9548b3826fa29bf3ccf8d221147/LICENSE)
- [S14: Existing NCM adapter boundary](https://github.com/BleedingDev/tracedecay/blob/25778c7443cd0cfe257da363da01a56ea1d45d3f/crates/tracedecay-memory-provider-ncm/src/lib.rs)
- [S19: Existing upstream convergence procedure](https://github.com/BleedingDev/tracedecay/blob/25778c7443cd0cfe257da363da01a56ea1d45d3f/product/upstream/README.md)

## Handoff

Commit one reviewable slice; attach the common completion receipt; do not change shared host paths outside the ownership agreement.

Use the common evidence receipt and ownership rules in `START_HERE.md`. These are task specifications, not claims that the test targets already exist.


---

# Pinned sources and review boundary

Retrieved/reviewed for this plan on 2026-09-05. Source code was inspected, not executed. Original PDFs were not successfully retrieved. These links identify reference material; they are not test receipts.

## S01 — Biomem pinned implementation

https://github.com/BleedingDev/biomem/tree/500847ff65b5d9548b3826fa29bf3ccf8d221147

**commit:** 500847ff65b5d9548b3826fa29bf3ccf8d221147

**review:** README, config, projection, center read/write excerpts, terrain, consolidation, text store/recall/state, embedder, and LICENSE inspected; not executed.

## S02 — Biomem configuration

https://github.com/BleedingDev/biomem/blob/500847ff65b5d9548b3826fa29bf3ccf8d221147/src/memory_module/config.py

**git_blob:** 9b317cd8523991805993cc03fb84f22611840ea3

## S03 — Biomem center algorithms

https://github.com/BleedingDev/biomem/blob/500847ff65b5d9548b3826fa29bf3ccf8d221147/src/memory_module/memory_centers.py

**git_blob:** ed1fc3e6df41a0fe3e6c508928fc2b6385456ef6

## S04 — Biomem terrain

https://github.com/BleedingDev/biomem/blob/500847ff65b5d9548b3826fa29bf3ccf8d221147/src/memory_module/terrain_3d.py

**git_blob:** f4b3a936e2b316dc08826e6fe5794d0e42242c22

## S05 — Biomem consolidation

https://github.com/BleedingDev/biomem/blob/500847ff65b5d9548b3826fa29bf3ccf8d221147/src/memory_module/consolidation.py

**git_blob:** 968b4a2669181a190888ccff50eb9644e2adcbaa

## S06 — Biomem projections

https://github.com/BleedingDev/biomem/blob/500847ff65b5d9548b3826fa29bf3ccf8d221147/src/memory_module/projections.py

**git_blob:** 433267fa3ab98bf0b9f3e06490c558320897a63d

## S07 — Biomem text-memory orchestration

https://github.com/BleedingDev/biomem/blob/500847ff65b5d9548b3826fa29bf3ccf8d221147/src/memory_module/text_memory.py

**git_blob:** feeec785d0d5ad0b4416ed29e3512de941d9f49a

## S08 — Biomem real text embedding and emotion extraction

https://github.com/BleedingDev/biomem/blob/500847ff65b5d9548b3826fa29bf3ccf8d221147/src/memory_module/embedder.py

**git_blob:** 1a0784bf58030ea36c01e0639b15bef65f8d9994

## S09 — Biomem MIT notice

https://github.com/BleedingDev/biomem/blob/500847ff65b5d9548b3826fa29bf3ccf8d221147/LICENSE

**git_blob:** eba479bc5df694b05f9adb8ee248ae1243b876eb

## S10 — Persistent Memory for Decoder-Only Transformers: Latent Terrain, Diffusion, Homeostasis, and Emotional Stabilization

https://zenodo.org/records/18198327

**doi:** 10.5281/zenodo.18198327

**version:** v1, 2026-01-09

**review:** Primary record and abstract inspected; original PDF retrieval failed. Full-text verification and file checksums remain N01 work.

## S11 — Implementation of Persistent Latent Memory for Decoder Transformers

https://zenodo.org/records/18267378

**doi:** 10.5281/zenodo.18267378

**review:** Identified by the primary OpenTechLab publication page. Record rate-limited; PDF retrieval failed on size. No full-text or figure verification claimed.

## S12 — OpenTechLab primary publication catalog

https://www.opentechlab.cz/publikace.html

**review:** Lists both memory publications and their downloadable paper filenames.

## S13 — BioCortexAI supplementary scientific documentation

https://zenodo.org/records/18198327/files/BioCortexAI_Documentation_EN.md?download=1

**review:** Primary supplementary text inspected; its header states CC BY-NC 4.0. It is not a blanket commercial code license.

## S14 — Existing NCM adapter boundary

https://github.com/BleedingDev/tracedecay/blob/25778c7443cd0cfe257da363da01a56ea1d45d3f/crates/tracedecay-memory-provider-ncm/src/lib.rs

**git_blob:** 708f30b5c8f6a2f2350567b189a0aa487e648c50

**review:** NcmNamespace includes agent_session_id and resolved_scope_digest; NcmCognitiveSurface is synchronous; adapter owns scope/readiness checks.

## S15 — Versioned Beads plan at checkpoint

https://github.com/BleedingDev/tracedecay/blob/25778c7443cd0cfe257da363da01a56ea1d45d3f/.beads/issues.jsonl

**git_blob:** 9c27c2f0a3375bd2255e341c6696fe257e34afd8

**review:** tdmem-0700 and 0702 onward were descoped; 0701 audit was completed. 0606/0608/0609 remain open in inspected records.

## S16 — Existing host journey

https://github.com/BleedingDev/tracedecay/blob/25778c7443cd0cfe257da363da01a56ea1d45d3f/crates/tracedecay-cli/tests/product_memory_provider_claude_host_journey.rs

## S17 — Existing provider-neutral evaluation

https://github.com/BleedingDev/tracedecay/blob/25778c7443cd0cfe257da363da01a56ea1d45d3f/product/evaluation/README.md

## S18 — MiniLM primary model card

https://huggingface.co/sentence-transformers/paraphrase-multilingual-MiniLM-L12-v2

**review:** 384-dimensional output; masked mean pooling; SentenceTransformer max_seq_length 128; Apache-2.0 metadata. Exact model revision/artifact hashes still must be pinned.

## S19 — Existing upstream convergence procedure

https://github.com/BleedingDev/tracedecay/blob/25778c7443cd0cfe257da363da01a56ea1d45d3f/product/upstream/README.md



## Planning-package validation

```json
{
  "valid": true,
  "task_count": 26,
  "external_gate_count": 4,
  "backend_independent_tasks": 22,
  "ready": [
    "ncm-rs-001"
  ],
  "topological_order": [
    "ncm-rs-001",
    "ncm-rs-002",
    "ncm-rs-003",
    "ncm-rs-004",
    "ncm-rs-005",
    "ncm-rs-008",
    "ncm-rs-012",
    "ncm-rs-013",
    "ncm-rs-006",
    "ncm-rs-007",
    "ncm-rs-009",
    "ncm-rs-011",
    "ncm-rs-010",
    "ncm-rs-014",
    "ncm-rs-015",
    "ncm-rs-017",
    "ncm-rs-016",
    "ncm-rs-018",
    "ncm-rs-019",
    "ncm-rs-020",
    "ncm-rs-021",
    "ncm-rs-022",
    "ncm-rs-023",
    "ncm-rs-024",
    "ncm-rs-025",
    "ncm-rs-026"
  ],
  "declared_concurrent_write_collisions": 0
}
```

Eighteen planning-validator tests passed. No NCM, reference-engine, Rust, model or host tests were executed by this planner.
