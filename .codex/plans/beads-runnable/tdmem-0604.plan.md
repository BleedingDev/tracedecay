---
name: tdmem-0604
overview: "Avoid flooding context with semantically redundant memories or repeated reconstructions."
todos:
  - id: tdmem-0604-deliver
    content: "Deliver Bead tdmem-0604: Implement provider-candidate deduplication and diversity selection; satisfy every recorded acceptance criterion, run the smallest behavioral dependency cone, then commit and push the green slice."
    status: pending
isProject: false
---

# tdmem-0604: Implement provider-candidate deduplication and diversity selection

## Execution Notes

Beads issue: `tdmem-0604`. Current Beads status at generation: `open`.

Avoid flooding context with semantically redundant memories or repeated reconstructions.

Design authority:

Use stable refs where available and bounded content similarity otherwise. Apply deterministic diversity selection after hard admission.

Acceptance authority:

- [ ] Duplicate candidates do not consume repeated budget.
- [ ] Distinct negative and positive evidence are not incorrectly collapsed.
- [ ] Selection is deterministic and explainable.
- [ ] Provider-specific IDs are not assumed.

## Constraints

- Beads is the live source of truth. Re-read `br show tdmem-0604` and require `br ready` before a new claim.
- Unsatisfied blocking Beads dependencies at generation: tdmem-0602, tdmem-0603.
- Beads parent/hierarchy references: tdmem-0600. Parent readiness is enforced by `br`, not fabricated as a plan dependency.
- Build semantic producers/consumers and focused evidence before bookkeeping. Do not create hash-, receipt-, lock-, or presence-only gates.
- Root alone owns heavy Cargo execution, integration acceptance, commits, pushes, and Beads closure.
- Workers must stay inside their assigned write scope, preserve concurrent edits, and stop rather than widen scope.
- Do not contact upstream maintainers or mutate external work items.

## Operator Guidance

Intersect `plan-graph frontier` with live `br ready`; launch only Beads-ready or already-claimed nodes. Assign one coherent file/module owner, run focused checks, review the exact diff locally, then publish the green slice immediately. Close the Bead only after every acceptance item and real journey required by the issue are evidenced.
