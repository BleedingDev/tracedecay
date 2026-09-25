---
name: pm-native
overview: Complete Native's provider-local lifecycle and temporal behavior under the common profile without changing canonical fact trust or ownership.
todos:
  - id: native-source-state-and-time
    content: Migrate the existing v1 staged database in place, preserving original references and duplicate evidence while adding real source revisions, retained validity, correction lineage, feedback and durable generation.
    status: in_progress
  - id: native-recall-time-exclusions-provenance
    content: Implement every temporal mode and exclusion before ranking and truncation, validate stored content digests, and return truthful scan coverage and source provenance without treating admission time as historical validity.
    status: in_progress
  - id: native-feedback-correction-delete
    content: Implement real idempotent provider-local feedback, correction and source deletion with lineage, read-after-write behavior, tombstones and restart anti-resurrection checks.
    status: in_progress
  - id: native-inspect-maintain-recover
    content: Implement bounded redacted inspection, maintenance, snapshot export/restore and replay with truthful state generation and failure/effect semantics.
    status: in_progress
  - id: native-common-profile-tests
    content: Verify all required temporal modes, lifecycle operations, legacy store reopen and direct Native authority parity through the real application port.
    status: in_progress
isProject: false
---

# pm-native

## Execution Notes

Read `pm-finish-brief.md`, `pm-native-evidence.md` and settled shared contracts. Provider-local operations target staged/derived advisory state. Existing owner-bound canonical fact CRUD, trust, feedback and promotion retain their own authority and remain available under either selected provider. Use existing actor and bounded control paths; no unbounded synchronous call on the runtime. Retention or deleted historical data produces explicit coverage, never a current-answer substitute.

The audit identifies five concrete constraints: `canonical_payload.version` is not a proved source content revision; nonempty exclusions currently fail; all historical modes currently fail; generation is fixed at zero; retention tombstones are not privacy deletion records. Preserve old receipt identities and treat missing legacy validity as unknown. Test a real v1 populated database upgrade with rollback, not just a new store. Make deletion source-wide across authorized replay copies and check its fence before observe, correction, replay and restore, including delete-before-observe. Recall must rehash stored content before accepting its claimed digest. Cancellation must distinguish no mutation from a commit whose result is unknown.

Under the common advisory profile, recall contains provider-local staged/derived content; exclude canonical fact projections from this profile in every temporal mode. Preserve explicitly negotiated legacy fact projection separately. The host already reads canonical project facts for either provider. Native does not gain extra comparison credit or historical behavior by mixing in current facts, and this lane does not create canonical fact history search.

## Constraints

Own `crates/tracedecay-memory-provider-native/` plus root `retained_owner/native_provider.rs`, `native_staged_observations.rs`, their direct unit-test files and `native_baseline_tests.rs`. Do not edit `cognitive_recall.rs`, `observation_journey.rs`, shared contracts, registry, configuration, CLI fixtures or NCM. If a shared seam is missing, return a narrow request to root. Never mutate canonical fact trust from provider feedback.

## Operator Guidance

Dependency: `pm-contract`; stop with real common-profile behavior and non-vacuous tests. Pair tests with exact source/effect assertions, including cancel before mutation, commit with lost reply, reopen, delete then replay, and old snapshot restore after deletion. Root runs scoped Cargo; hand back changes after each coherent store/port slice so integration does not wait for a large final patch.

## Implementation checkpoint

The existing staged store now migrates v1 in place to `native-staged-v2`, preserving receipt/reference identities and leaving unsupported legacy source revision, original scope and validity unknown. The existing bounded actor executes common lifecycle transactions and applies fresh, call-bound host admission for history and restore. Temporal eligibility, all exclusion classes and checkout-wide privacy fences filter before the candidate window. Local feedback, correction overlays, maintenance, inspection, portable receipt/ack/fence snapshots and canonical replay are implemented. Every mutable reply is budget checked before commit; cancellation and lost replies distinguish no effect from reconciliation-required effects.

Ten populated direct common-provider tests plus inline real-v1 migration/rollback and dense eligibility regressions are written. They cover fresh restore namespaces, current attribution, cross-session privacy scrubbing, stored correction overlays, original duplicate evidence, real replay counters, committed lost replies, dry-run nonmutation and real maintenance effects. Old direct/legacy baseline assertions reflect durable generation and implemented capability dispatch while preserving the baseline's missing-original-source refusals.

Validation remains pending root-owned Cargo execution. No Cargo or model run has been launched by this implementation agent; rustfmt completed on the owned Rust files. No task is marked complete until compiler and relevant tests pass.
