---
name: tdmem-0403
overview: "Run identical fixtures through current Native surfaces and the adapter route."
todos:
  - id: tdmem-0403-deliver
    content: "Deliver Bead tdmem-0403: Build direct-versus-provider Native golden parity tests; satisfy every recorded acceptance criterion, run the smallest behavioral dependency cone, then commit and push the green slice."
    status: pending
isProject: false
---

# tdmem-0403: Build direct-versus-provider Native golden parity tests

## Execution Notes

Beads issue: `tdmem-0403`. Current Beads status at generation: `open`.

Run identical fixtures through current Native surfaces and the adapter route.

Design authority:

Compare semantic outputs, identifiers, provenance, ordering, failure classifications, and committed receipts. Permit only documented envelope differences.

Acceptance authority:

- [ ] Positive and negative parity fixtures pass.
- [ ] Any intentional difference is ADR-backed.
- [ ] The tests cover profile/project scope, contradiction, stale validity, and feedback.

## Constraints

- Beads is the live source of truth. Re-read `br show tdmem-0403` and require `br ready` before a new claim.
- Unsatisfied blocking Beads dependencies at generation: tdmem-0402.
- Beads parent/hierarchy references: tdmem-0400. Parent readiness is enforced by `br`, not fabricated as a plan dependency.
- Build semantic producers/consumers and focused evidence before bookkeeping. Do not create hash-, receipt-, lock-, or presence-only gates.
- Root alone owns heavy Cargo execution, integration acceptance, commits, pushes, and Beads closure.
- Workers must stay inside their assigned write scope, preserve concurrent edits, and stop rather than widen scope.
- Do not contact upstream maintainers or mutate external work items.

## Operator Guidance

Intersect `plan-graph frontier` with live `br ready`; launch only Beads-ready or already-claimed nodes. Assign one coherent file/module owner, run focused checks, review the exact diff locally, then publish the green slice immediately. Close the Bead only after every acceptance item and real journey required by the issue are evidenced.
