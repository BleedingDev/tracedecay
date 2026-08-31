---
name: tdmem-0506
overview: "Recover provider delivery after host/provider restart and verify provider state aligns with acknowledged sequence."
todos:
  - id: tdmem-0506-deliver
    content: "Deliver Bead tdmem-0506: Implement recovery replay and exact-effect verification; satisfy every recorded acceptance criterion, run the smallest behavioral dependency cone, then commit and push the green slice."
    status: pending
isProject: false
---

# tdmem-0506: Implement recovery replay and exact-effect verification

## Execution Notes

Beads issue: `tdmem-0506`. Current Beads status at generation: `open`.

Recover provider delivery after host/provider restart and verify provider state aligns with acknowledged sequence.

Design authority:

Replay unacknowledged observations. Compare provider checkpoint/sequence to outbox. Incompatible state returns migration/reset required rather than silent reinitialization.

Acceptance authority:

- [ ] Restart during delivery converges without duplicate effects.
- [ ] Acknowledged sequence is monotonic.
- [ ] State incompatibility is typed.
- [ ] Recovery has a bounded operator repair path.

## Constraints

- Beads is the live source of truth. Re-read `br show tdmem-0506` and require `br ready` before a new claim.
- Unsatisfied blocking Beads dependencies at generation: tdmem-0503, tdmem-0504.
- Beads parent/hierarchy references: tdmem-0500. Parent readiness is enforced by `br`, not fabricated as a plan dependency.
- Build semantic producers/consumers and focused evidence before bookkeeping. Do not create hash-, receipt-, lock-, or presence-only gates.
- Root alone owns heavy Cargo execution, integration acceptance, commits, pushes, and Beads closure.
- Workers must stay inside their assigned write scope, preserve concurrent edits, and stop rather than widen scope.
- Do not contact upstream maintainers or mutate external work items.

## Operator Guidance

Intersect `plan-graph frontier` with live `br ready`; launch only Beads-ready or already-claimed nodes. Assign one coherent file/module owner, run focused checks, review the exact diff locally, then publish the green slice immediately. Close the Bead only after every acceptance item and real journey required by the issue are evidenced.
