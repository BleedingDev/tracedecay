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
