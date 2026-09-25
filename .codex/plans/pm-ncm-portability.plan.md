---
name: pm-ncm-portability
overview: Finish canonical NCM snapshot and replay through the existing opaque adapter boundary and durable namespace store.
todos:
  - id: implement-canonical-portability
    content: Map snapshot export/restore and canonical replay with current source dispositions, original-source deduplication and exact partial accounting in two independently owned modules.
    status: in_progress
  - id: verify-portability-anti-resurrection
    content: Prove fresh-namespace restore and repeated replay cannot resurrect deleted sources or alias prior provider-local references, preserving explicit unknown outcomes.
    status: pending
isProject: false
---

# pm-ncm-portability

Own NEW crates/tracedecay-memory-provider-ncm/src/common/portability.rs and NEW crates/tracedecay-memory-ncm-runtime/src/engine/runtime/portability.rs only, plus focused new tests after coordinating factory helpers. Main pm-ncm worker retains adapter lib/common/lifecycle and runtime control/selection/runtime.rs/snapshot.rs/worker; give it exact unapplied integration hunks. Root owns manifests/builds. Coordinate signatures before editing. No source algorithm, provider authority, public wire changes, separate mapping DB, process owner or duplicated tombstone authority.

Reuse current shared canonical snapshot/replay contract and injected trusted AdvisoryAdmissionAuthority. Adapter receives validated call-bound current sources/dispositions and projects private numeric/opaque controls; runtime never authorizes host scope itself. Validate payload bytes/length/hash, provider-specific snapshot identity and actual source inventory, fresh destination current host deletion fences, replay receipt membership, stable source revision and current idempotency. Preserve destination next-record-id floor. Fresh delivery keys cannot duplicate original influence. Report applied/duplicate/source-already-applied/rejected/unknown partitions exactly. Core NCM algorithm unchanged.

Leaf worker; no child agent, Cargo/model run, commit/push or operator actions. Not alone: preserve other edits. Root owns status and final integration review. Stop at source plus focused fixture handoff ready for root compilation.
