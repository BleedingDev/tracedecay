---
name: pm-ncm
overview: Make the real NCM adapter and worker implement the complete common memory semantics instead of accepting a reduced private recall payload.
todos:
  - id: ncm-canonical-recall-translation
    content: Translate complete canonical recall requests into real NCM execution with exact scope, evaluation time, every required temporal mode, exclusions and budgets implemented; reject malformed inputs before dispatch.
    status: completed
  - id: ncm-admissible-recall-output
    content: Return canonical candidates with honest content digests, original-source provenance, validity, ordering, coverage and accepted state generation that pass host admission without Native fallback.
    status: completed
  - id: ncm-common-lifecycle
    content: Complete common feedback, correction, deletion, inspection, maintenance, snapshot and replay mappings using opaque restart-stable identities and truthful idempotent effects.
    status: completed
  - id: ncm-cancellation-and-resurrection-fences
    content: Carry live OperationControl into the existing cancellable worker call, fence revoked sources before new observation or replay, exclude superseded records by policy, and prevent snapshot restore from aliasing old public record references.
    status: completed
  - id: ncm-real-profile-tests
    content: Prove real-model recall and lifecycle behavior across worker restart, correction, deleted-source replay, corrupted state, cancellation and bounded resource pressure.
    status: completed
isProject: false
---

# pm-ncm

## Execution Notes

Read the brief, NCM audit and settled common contracts. Reuse the pinned Rust NCM algorithm, worker supervisor, namespace store and encoder. Backend record IDs and provider-native scores stay opaque; public identities must be rebound using actual stored provenance. Do not turn a backend success into a host-success claim before full canonical response validation. The runtime is host-blind: host authorization and canonical source lookup arrive through reviewed typed inputs.

Retain the original canonical observation/source metadata through the adapter and worker store; projecting down to role/content/session loses the evidence needed to construct admissible recall. Build the canonical response from actual retained metadata, not a relabeled kernel response. Use the existing `WorkerClient::call_cancellable` rather than adding a supervisor or checking cancellation only after synchronous completion. Durable source deletion covers provenance-bearing capsules and future ingestion with a fresh delivery key. Snapshot restore must preserve monotone live generation and stable public attribution even if backend record allocation rewinds. Tests for the inferred resurrection and record-alias paths must first demonstrate their actual behavior.

## Constraints

Own `crates/tracedecay-memory-provider-ncm/`, `crates/tracedecay-memory-ncm-runtime/` and their tests. The NCM core is read-only unless root approves an exact semantic producer change; do not retune model, projections or ranking to pass fixtures. Do not edit Native, registry, root host, canonical contract schemas or manifests. Preserve exact namespace derivation; later-session reuse is supplied by the host history lane.

## Operator Guidance

Dependency: `pm-contract`. The first checkpoint is a real nonempty NCM reply admitted by the common recall parser. Subsequent checkpoints cover lifecycle and persistent source mapping. Worker prepares test selections; root alone builds and runs real-model checks. Do not claim a HashEncoder or mock result as product evidence. Preserve existing crash/reconciliation tests; add only tests for changed behavior.

## Verification

Root ran the adapter/runtime library and integration tests with the production worker: 180 passed in cc-1332, and its sole remaining interrupted-deletion fixture passed after correction in cc-1337. All three explicitly selected real-model checks passed in cc-1336, including a nonempty canonical recall admitted without request rewriting and shared-worker namespace/readiness isolation. Replay/restore reconciliation, cancellation, source deletion, temporal selection and restart tests are included in that direct provider cohort. Host integration and matched measurements remain separate downstream work.
