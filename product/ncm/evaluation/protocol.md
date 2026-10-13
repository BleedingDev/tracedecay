# NCM fidelity and coding-memory evaluation protocol

Status: predeclared before the first ncm-rs-021 evaluation run.
Profile: `ncm-biomem-rs.v1`.
Reference: Biomem commit `500847ff65b5d9548b3826fa29bf3ccf8d221147`.
Corpus: `tracedecay.coding-memory.scenarios.v1` (all nine checked-in scenarios).
Metric catalog: `tracedecay.coding-memory.metrics.v1`.

## Claims and result separation

The report has four independent blocks: kernel fidelity, approved algorithm corrections,
provider behavior, and observer-mode task benefit. Passing one block does not imply passing
another. This task does not measure or claim improved agent outcomes; joined active-mode agent
benefit belongs to ncm-rs-025.

## Inputs and split

All lanes receive byte-identical observation payloads, scope identities, queries, revisions, and
operation order from `product/evaluation/coding-memory-scenarios.v1.json`. Provider identity is run
metadata, never an input to scoring.

The development split is `stale_project_change`, `failed_approach`, `restart`, and `cancellation`.
It may be used only to debug the runner and transport. The held-out final split is
`cross_agent_reuse`, `project_worktree_scope`, `contradiction`, `provider_corruption`, and
`privacy_deletion`. Thresholds and labels below are frozen before inspecting held-out answers. No
answer text or threshold may be tuned after a held-out run; any protocol change requires a new
protocol revision and a fresh result file.

## Lanes

Executed locally when dependencies are available:

1. Rust NCM worker with the pinned real MiniLM encoder.
2. Pinned Python Biomem with the same model, texts, seeds, and Rust projection matrices for
   numerical fixtures where matrix identity matters.
3. No memory: zero admitted provider context and no provider call.
4. Explicit documentation: only the corpus fixture's current `docs/**`, `notes/**`, top-level
   `README.md`, `AGENTS.md`, or `CLAUDE.md` revision; never source code or provider state.

Native provider and joined TraceDecay host lanes are recorded as `pending`, not as failures and not
included in comparative aggregates.

## Numerical fidelity rubric

Single-step f32 comparisons use `atol=1e-6`, `rtol=1e-5`. Multi-step comparisons declare their
observed maximum absolute error. Exact fields are masks, active counts, record identity, operation
outcomes, and non-tied indices. Tied candidates are compared as ascending-index equivalence groups
(D13). Projection RNG byte parity is not required; the Rust matrices are injected into Biomem for
matched forward-kernel fixtures. Encoder-derived projected keys use `atol=5e-4`, `rtol=5e-4` to
cover independently implemented ONNX/SentenceTransformers pooling while preserving the same pinned
model.

The real-text cases are: store/recall, paraphrase recall, 512-center STM capacity interference,
stale supersession, and LTM-only retention after consolidation, STM decay/prune, and worker
restart. LTM-only success requires `stm_active == 0`, `ltm_active > 0`, a real-encoder identity, and
a recalled candidate whose layer is `Ltm`; Native, keywords, fixture files, and source-file fallback
are prohibited.

Every D01-D13 entry in `product/ncm/reference/deviations.json` is reported as either
`reference-compatible` or `expected-difference`; an approved correction is never a fidelity failure.

## Coding-memory rubric

The checked-in scenario rubric remains authoritative. The runner additionally applies a mechanical
admission layer:

- a candidate is relevant when its source revision or payload fact is one of the scenario's settled
  observations and its normalized token overlap with the query is at least 0.15, or when the corpus
  explicitly identifies it as the current replacement for the queried fact;
- current exact-scope evidence is admissible;
- superseded evidence is visible for lineage but rejected from final admission;
- a candidate from another exact scope is rejected with `scope_mismatch`;
- revoked/deleted records are rejected with `deleted_source`;
- state with failed integrity is rejected with `corrupt_state`;
- missing or indeterminate evidence never counts as a safety pass.

Minimum acceptable useful-recall precision is 0.60 on resolved positive recalls. Safety ceilings
are exactly those in `coding-memory-metrics.v1.json`: harmful stale recall rate 0, scope leakage 0,
corrupt-state recall 0, and deleted-source recall 0. Any indeterminate safety metric fails the safety
gate. Aggregate task score is reported only beside the safety gate and verdict.

## Budgets

Per operation deadline: 5,000 ms for warm observe/recall and 120,000 ms for explicit maintenance or
cold model work. Recall `top_k` is 5 unless the corpus requests a lower finite bound. Request and
reply frames retain the frozen 256 KiB / 1 MiB limits. Per scenario admitted context budget is the
corpus request budget; this runner also caps hydration at 16 candidates and 1 MiB. Token cost is a
transparent UTF-8 byte estimate (`ceil(bytes/4)`), labeled as an estimate rather than the production
`tiktoken.o200k_base` count. Warm recall p95 budget is 250 ms; durable observe p95 budget is 500 ms.
Latency overruns are reported, not discarded.

## Ablations and invocation evidence

1. Terrain read contribution off: text recall is run with the frozen D03 `None` terrain query.
   Invocation evidence is a changed STM terrain digest after observe. Since terrain read influence is
   deliberately none, zero recall delta is expected and must be reported as no measured benefit.
2. Corrected blur versus reference no-op blur: an asymmetric terrain impulse is consolidated. Rust
   must change the LTM terrain digest and spread support; Biomem's executable `blur` returns clones.
   Recall-task delta may be zero because D03 disables terrain reads.
3. Consolidation off: after identical observe inputs, both variants advance and prune STM. The on
   variant must retain an LTM candidate; the off variant must become empty. Inspection counts and
   candidate layers prove the path ran.

## Negative controls

- Replacing corrected blur with an identity copy fails the impulse-spread assertion.
- Omitting consolidation makes the LTM-only candidate assertion fail (`ltm_active` remains zero and
  recall is empty after STM prune).
- Admitting `Superseded`, cross-scope, revoked, or corrupt content fails the corresponding zero
  safety ceiling and overall safety gate.
- Substituting the hash encoder fails the encoder identity assertion before LTM-only scoring.
- A no-op evaluator cannot pass: zero admission is indeterminate for safety checks that require
  inspected evidence.
