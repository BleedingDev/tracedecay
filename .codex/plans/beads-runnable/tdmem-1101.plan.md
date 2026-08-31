---
name: tdmem-1101
overview: "Add common status surfaces for configured, active, observer, and unavailable providers."
todos:
  - id: tdmem-1101-deliver
    content: "Deliver Bead tdmem-1101: Expose provider health, identity, capabilities, and readiness; satisfy every recorded acceptance criterion, run the smallest behavioral dependency cone, then commit and push the green slice."
    status: pending
isProject: false
---

# tdmem-1101: Expose provider health, identity, capabilities, and readiness

## Execution Notes

Beads issue: `tdmem-1101`. Current Beads status at generation: `open`.

Add common status surfaces for configured, active, observer, and unavailable providers.

Design authority:

Readiness is operation-specific. Surface provider build/state schema, queue/backlog, last acknowledged sequence, degradation, and repair action.

Acceptance authority:

- [ ] CLI and machine-facing status share one contract.
- [ ] False readiness is impossible for missing mandatory capability.
- [ ] Observer and active roles are explicit.

## Constraints

- Beads is the live source of truth. Re-read `br show tdmem-1101` and require `br ready` before a new claim.
- Unsatisfied blocking Beads dependencies at generation: tdmem-0505, tdmem-0703.
- Beads parent/hierarchy references: tdmem-1100. Parent readiness is enforced by `br`, not fabricated as a plan dependency.
- Build semantic producers/consumers and focused evidence before bookkeeping. Do not create hash-, receipt-, lock-, or presence-only gates.
- Root alone owns heavy Cargo execution, integration acceptance, commits, pushes, and Beads closure.
- Workers must stay inside their assigned write scope, preserve concurrent edits, and stop rather than widen scope.
- Do not contact upstream maintainers or mutate external work items.

## Operator Guidance

Intersect `plan-graph frontier` with live `br ready`; launch only Beads-ready or already-claimed nodes. Assign one coherent file/module owner, run focused checks, review the exact diff locally, then publish the green slice immediately. Close the Bead only after every acceptance item and real journey required by the issue are evidenced.
