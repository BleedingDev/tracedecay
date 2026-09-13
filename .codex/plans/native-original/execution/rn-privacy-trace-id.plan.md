---
name: rn-privacy-trace-id
overview: "Accept the persisted recall explain-trace identity repair and prove hostile provider IDs never survive into stored traces."
todos:
  - id: consume-trace-fix
    content: "Review the host-context and registry changes that project provider candidate identities before explain-trace serialization."
    status: pending
  - id: run-hostile-id-gates
    content: "Exercise admission denial, deduplication, budget exclusion, hydration exclusion and final withholding with hostile IDs, then inspect serialized and reopened trace rows."
    status: pending
  - id: release-privacy-gate
    content: "Publish the accepted raw-ID absence and reconciliation evidence so rn-privacy-restore can revalidate the complete privacy boundary."
    status: pending
isProject: false
---

# Close the persisted recall explain-trace identity leak

## Execution Notes

Work only on `feat/pluggable-memory-providers-v2` in `/Users/satan/workspace/bleedingdev/projects/tracedecay/.worktrees/pluggable-memory-providers-v2`. Read `../READINESS-PLAN.md`, `../WORKER-RULES.md`, `../REVIEWED-DECISIONS.md` and `../execution-results/privacy-audit-57006f60.md`. The active original remains clean 570. Stay in Codex and use the existing Native host/registry owners; no alternate agent CLI, branch or worktree.

The privacy audit identified a source-level raw provider candidate ID leak in the persisted explain/audit trace. The implementation remains with the existing exclusive owners: `rn-host-context` owns `crates/tracedecay/src/daemon/retained_owner/cognitive_recall.rs`, and `rn-registry` owns `crates/tracedecay-memory-provider-registry/**`. This node is the explicit integration and hostile-ID acceptance gate after both predecessors return. It owns no overlapping production source.

## Ownership and Constraints

Own only `execution-results/privacy-trace-id.md` and privacy-safe hostile-ID fixtures/results below `target/test-profile/readiness/privacy/trace-id/`. Do not edit `cognitive_recall.rs`, `recall_explain_trace.rs`, the detector, typed Claude history admission, generic sanitizer, Native/LCM/NCM engines or generated contracts. If either predecessor does not contain the required projection, return the exact missing source owner to root; do not patch a shared file here.

The host-safe stand-in must replace raw IDs in admission/stage/dedup/host-withheld/selected/decision fields before serialization while retaining bounded reconciliation. Inspect both the emitted JSON and a reopened `RecallAdmissionLedgerV1` row. A correct rendered candidate alone is insufficient.

## Acceptance Checklist

- Host-context and registry fixes are reviewed and the raw hostile value is absent from every persisted explain-trace identity field.
- Deduplication, budget exclusion, hydration exclusion, pre-hardening withholding and final withholding all have nonzero exercised evidence.
- `rn-privacy-restore` is blocked until this gate is accepted; detector equality and typed Claude history checks remain separate subclaims.

## Operator Guidance

Retain failures and exact fixture/source/binary identities. Keep provider-controlled IDs out of the report payload itself where possible; record a digest and safe label for the hostile value. Return the precise missing case or source owner if any trace surface still stores the raw value.

## Current dependency contract

Prerequisites: `rn-host-context`, `rn-registry`. The exact active edges are in `../execution-selection.json`; this node gates `rn-privacy-restore` and does not duplicate either predecessor's source ownership.
