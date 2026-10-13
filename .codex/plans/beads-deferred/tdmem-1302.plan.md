---
name: tdmem-1302
overview: "Compare the approved OCEAN spec to the common provider contract."
todos:
  - id: tdmem-1302-deliver
    content: "Deliver Bead tdmem-1302: Map OCEAN capabilities and contract gaps; satisfy every recorded acceptance criterion, run the smallest behavioral dependency cone, then commit and push the green slice."
    status: pending
isProject: false
---

# tdmem-1302: Map OCEAN capabilities and contract gaps

## Execution Notes

Beads issue: `tdmem-1302`. Current Beads status at generation: `deferred`.

Compare the approved OCEAN spec to the common provider contract.

Design authority:

Solve true generic gaps through versioned contract evolution; reject provider-name special cases.

Acceptance authority:

- [ ] Capability map is complete.
- [ ] Contract changes have compatibility analysis.
- [ ] No existing provider semantics are weakened.

## Constraints

- Beads is the live source of truth. Re-read `br show tdmem-1302` and require `br ready` before a new claim.
- Unsatisfied blocking Beads dependencies at generation: tdmem-1301.
- Beads parent/hierarchy references: tdmem-1300. Parent readiness is enforced by `br`, not fabricated as a plan dependency.
- Build semantic producers/consumers and focused evidence before bookkeeping. Do not create hash-, receipt-, lock-, or presence-only gates.
- Root alone owns heavy Cargo execution, integration acceptance, commits, pushes, and Beads closure.
- Workers must stay inside their assigned write scope, preserve concurrent edits, and stop rather than widen scope.
- Do not contact upstream maintainers or mutate external work items.

## Operator Guidance

Intersect `plan-graph frontier` with live `br ready`; launch only Beads-ready or already-claimed nodes. Assign one coherent file/module owner, run focused checks, review the exact diff locally, then publish the green slice immediately. Close the Bead only after every acceptance item and real journey required by the issue are evidenced.
