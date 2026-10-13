---
name: tdmem-0808
overview: "Make all lifecycle mutations reviewable and reversible where policy permits."
todos:
  - id: tdmem-0808-deliver
    content: "Deliver Bead tdmem-0808: Add human/agent curation receipts, dry-run, apply, and undo; satisfy every recorded acceptance criterion, run the smallest behavioral dependency cone, then commit and push the green slice."
    status: pending
isProject: false
---

# tdmem-0808: Add human/agent curation receipts, dry-run, apply, and undo

## Execution Notes

Beads issue: `tdmem-0808`. Current Beads status at generation: `open`.

Make all lifecycle mutations reviewable and reversible where policy permits.

Design authority:

Follow propose → validate → apply. Apply is bound to an exact candidate digest/revision. Undo creates a new audited transition instead of history rewriting.

Acceptance authority:

- [ ] Dry-run has no side effects.
- [ ] Apply rejects stale candidate revisions.
- [ ] Undo is explicit and tested.
- [ ] Actor, reason, evidence, and prior/new state are recorded.

## Constraints

- Beads is the live source of truth. Re-read `br show tdmem-0808` and require `br ready` before a new claim.
- Unsatisfied blocking Beads dependencies at generation: tdmem-0805, tdmem-0806, tdmem-0807.
- Beads parent/hierarchy references: tdmem-0800. Parent readiness is enforced by `br`, not fabricated as a plan dependency.
- Build semantic producers/consumers and focused evidence before bookkeeping. Do not create hash-, receipt-, lock-, or presence-only gates.
- Root alone owns heavy Cargo execution, integration acceptance, commits, pushes, and Beads closure.
- Workers must stay inside their assigned write scope, preserve concurrent edits, and stop rather than widen scope.
- Do not contact upstream maintainers or mutate external work items.

## Operator Guidance

Intersect `plan-graph frontier` with live `br ready`; launch only Beads-ready or already-claimed nodes. Assign one coherent file/module owner, run focused checks, review the exact diff locally, then publish the green slice immediately. Close the Bead only after every acceptance item and real journey required by the issue are evidenced.
