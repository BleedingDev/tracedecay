---
name: tdmem-0701
overview: "Inventory callable operations, persistence, lifecycle, threading, errors, inspectability, and current platform behavior of the new licensed implementation."
todos:
  - id: tdmem-0701-deliver
    content: "Deliver Bead tdmem-0701: Audit the licensed Biomem/NCM surface against the provider contract; satisfy every recorded acceptance criterion, run the smallest behavioral dependency cone, then commit and push the green slice."
    status: pending
isProject: false
---

# tdmem-0701: Audit the licensed Biomem/NCM surface against the provider contract

## Execution Notes

Beads issue: `tdmem-0701`. Current Beads status at generation: `open`.

Inventory callable operations, persistence, lifecycle, threading, errors, inspectability, and current platform behavior of the new licensed implementation.

Design authority:

Map actual behavior to capabilities; do not infer support from marketing names. Identify contract gaps and distinguish adapter work from changes to NCM itself.

Acceptance authority:

- [ ] Every mandatory provider operation has a supported, adaptable, or blocking classification.
- [ ] Persistence and state compatibility are documented.
- [ ] Threading/cancellation limits are measured.
- [ ] No codebase-navigation responsibility is assigned to NCM.

## Constraints

- Beads is the live source of truth. Re-read `br show tdmem-0701` and require `br ready` before a new claim.
- Unsatisfied blocking Beads dependencies at generation: none.
- Beads parent/hierarchy references: tdmem-0700. Parent readiness is enforced by `br`, not fabricated as a plan dependency.
- Build semantic producers/consumers and focused evidence before bookkeeping. Do not create hash-, receipt-, lock-, or presence-only gates.
- Root alone owns heavy Cargo execution, integration acceptance, commits, pushes, and Beads closure.
- Workers must stay inside their assigned write scope, preserve concurrent edits, and stop rather than widen scope.
- Do not contact upstream maintainers or mutate external work items.

## Operator Guidance

Intersect `plan-graph frontier` with live `br ready`; launch only Beads-ready or already-claimed nodes. Assign one coherent file/module owner, run focused checks, review the exact diff locally, then publish the green slice immediately. Close the Bead only after every acceptance item and real journey required by the issue are evidenced.
