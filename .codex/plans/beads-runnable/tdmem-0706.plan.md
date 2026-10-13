---
name: tdmem-0706
overview: "Expose the subset of lifecycle controls truly supported by NCM."
todos:
  - id: tdmem-0706-deliver
    content: "Deliver Bead tdmem-0706: Implement NCM feedback, maintenance, correction, and forgetting capabilities; satisfy every recorded acceptance criterion, run the smallest behavioral dependency cone, then commit and push the green slice."
    status: pending
isProject: false
---

# tdmem-0706: Implement NCM feedback, maintenance, correction, and forgetting capabilities

## Execution Notes

Beads issue: `tdmem-0706`. Current Beads status at generation: `open`.

Expose the subset of lifecycle controls truly supported by NCM.

Design authority:

Map helpful/harmful outcomes, consolidation/maintenance triggers, correction, and forget-by-source only where postconditions can be verified.

Acceptance authority:

- [ ] Each operation has an observable receipt.
- [ ] Unsupported controls stay unsupported.
- [ ] Forget-by-source proves removed influence on later recall.
- [ ] Maintenance is bounded and cancellable.

## Constraints

- Beads is the live source of truth. Re-read `br show tdmem-0706` and require `br ready` before a new claim.
- Unsatisfied blocking Beads dependencies at generation: tdmem-0704, tdmem-0705.
- Beads parent/hierarchy references: tdmem-0700. Parent readiness is enforced by `br`, not fabricated as a plan dependency.
- Build semantic producers/consumers and focused evidence before bookkeeping. Do not create hash-, receipt-, lock-, or presence-only gates.
- Root alone owns heavy Cargo execution, integration acceptance, commits, pushes, and Beads closure.
- Workers must stay inside their assigned write scope, preserve concurrent edits, and stop rather than widen scope.
- Do not contact upstream maintainers or mutate external work items.

## Operator Guidance

Intersect `plan-graph frontier` with live `br ready`; launch only Beads-ready or already-claimed nodes. Assign one coherent file/module owner, run focused checks, review the exact diff locally, then publish the green slice immediately. Close the Bead only after every acceptance item and real journey required by the issue are evidenced.
