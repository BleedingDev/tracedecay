---
name: pm-native-audit
overview: Establish the exact Native provider lifecycle gaps and safe provider-local implementation seams without changing canonical fact authority.
todos:
  - id: native-gap-map
    content: Produce a code-referenced operation matrix and bounded write plan for Native feedback, correction, deletion, temporal recall, inspection, maintenance, snapshots and replay.
    status: completed
isProject: false
---

# pm-native-audit

## Execution Notes

Read `pm-finish-brief.md`. Inspect the actual `NativeMemoryApplicationPort` implementation and staged store. Distinguish adapter behavior, canonical fact authority, and staged advisory state. Identify existing schema/version/tombstone/read-actor mechanisms to reuse. Output `.codex/plans/pm-native-evidence.md` with operation, present behavior, exact symbols, smallest safe extension, migration impact and discriminating test.

## Constraints

Read-only source lane. Own only the evidence document. Do not edit Rust, public contracts, configuration, manifests or other plans. Do not assume unsupported methods map to canonical fact operations. Do not expand into NCM or host routing.

## Operator Guidance

Root node; its completed evidence is a prerequisite of `pm-contract`. No Cargo or model run. Stop when each required operation has a concrete implementation seam or a precise missing producer. Prefer exact supplied files over broad discovery: `crates/tracedecay-memory-provider-native/src/lib.rs`, root `retained_owner/native_provider.rs`, `native_staged_observations.rs` and their tests.
