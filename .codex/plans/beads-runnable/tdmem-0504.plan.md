---
name: tdmem-0504
overview: "Start, monitor, restart, and stop provider instances without crashing or wedging TraceDecay."
todos:
  - id: tdmem-0504-deliver
    content: "Deliver Bead tdmem-0504: Implement provider lifecycle supervision and crash isolation; satisfy every recorded acceptance criterion, run the smallest behavioral dependency cone, then commit and push the green slice."
    status: pending
isProject: false
---

# tdmem-0504: Implement provider lifecycle supervision and crash isolation

## Execution Notes

Beads issue: `tdmem-0504`. Current Beads status at generation: `open`.

Start, monitor, restart, and stop provider instances without crashing or wedging TraceDecay.

Design authority:

Supervision follows the selected ADR transport. Readiness comes from handshake and health, not process existence. Restart loops are bounded and visible.

Acceptance authority:

- [ ] Crash and failed handshake produce typed degradation.
- [ ] Restart budget prevents hot loops.
- [ ] Shutdown respects deadline and kills only after bounded grace.
- [ ] Host remains usable when provider is unavailable.

## Constraints

- Beads is the live source of truth. Re-read `br show tdmem-0504` and require `br ready` before a new claim.
- Unsatisfied blocking Beads dependencies at generation: tdmem-0503.
- Beads parent/hierarchy references: tdmem-0500. Parent readiness is enforced by `br`, not fabricated as a plan dependency.
- Build semantic producers/consumers and focused evidence before bookkeeping. Do not create hash-, receipt-, lock-, or presence-only gates.
- Root alone owns heavy Cargo execution, integration acceptance, commits, pushes, and Beads closure.
- Workers must stay inside their assigned write scope, preserve concurrent edits, and stop rather than widen scope.
- Do not contact upstream maintainers or mutate external work items.

## Operator Guidance

Intersect `plan-graph frontier` with live `br ready`; launch only Beads-ready or already-claimed nodes. Assign one coherent file/module owner, run focused checks, review the exact diff locally, then publish the green slice immediately. Close the Bead only after every acceptance item and real journey required by the issue are evidenced.
