---
name: tdmem-1403
overview: "Ensure existing TraceDecay users can install the derivative without accidental provider activation or data loss."
todos:
  - id: tdmem-1403-deliver
    content: "Deliver Bead tdmem-1403: Define config defaults, upgrade, migration, rollback, and reset behavior; satisfy every recorded acceptance criterion, run the smallest behavioral dependency cone, then commit and push the green slice."
    status: pending
isProject: false
---

# tdmem-1403: Define config defaults, upgrade, migration, rollback, and reset behavior

## Execution Notes

Beads issue: `tdmem-1403`. Current Beads status at generation: `open`.

Ensure existing TraceDecay users can install the derivative without accidental provider activation or data loss.

Design authority:

Memory Fabric and NCM start disabled or Native-only according to accepted policy. Config/state schemas are versioned; incompatible provider state has explicit repair.

Acceptance authority:

- [ ] Existing config loads safely.
- [ ] No provider activates implicitly.
- [ ] Upgrade and rollback journeys pass.
- [ ] Reset never deletes state without explicit confirmation/receipt.

## Constraints

- Beads is the live source of truth. Re-read `br show tdmem-1403` and require `br ready` before a new claim.
- Unsatisfied blocking Beads dependencies at generation: tdmem-0404, tdmem-0707.
- Beads parent/hierarchy references: tdmem-1400. Parent readiness is enforced by `br`, not fabricated as a plan dependency.
- Build semantic producers/consumers and focused evidence before bookkeeping. Do not create hash-, receipt-, lock-, or presence-only gates.
- Root alone owns heavy Cargo execution, integration acceptance, commits, pushes, and Beads closure.
- Workers must stay inside their assigned write scope, preserve concurrent edits, and stop rather than widen scope.
- Do not contact upstream maintainers or mutate external work items.

## Operator Guidance

Intersect `plan-graph frontier` with live `br ready`; launch only Beads-ready or already-claimed nodes. Assign one coherent file/module owner, run focused checks, review the exact diff locally, then publish the green slice immediately. Close the Bead only after every acceptance item and real journey required by the issue are evidenced.
