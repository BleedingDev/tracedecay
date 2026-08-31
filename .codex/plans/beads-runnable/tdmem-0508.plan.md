---
name: tdmem-0508
overview: "Exercise failures at every boundary from host commit through provider ACK persistence."
todos:
  - id: tdmem-0508-deliver
    content: "Deliver Bead tdmem-0508: Prove crash safety between host commit and provider acknowledgement; satisfy every recorded acceptance criterion, run the smallest behavioral dependency cone, then commit and push the green slice."
    status: pending
isProject: false
---

# tdmem-0508: Prove crash safety between host commit and provider acknowledgement

## Execution Notes

Beads issue: `tdmem-0508`. Current Beads status at generation: `open`.

Exercise failures at every boundary from host commit through provider ACK persistence.

Design authority:

Use fault injection around outbox write, enqueue, provider receive, provider commit, ACK return, and ACK persistence.

Acceptance authority:

- [ ] No committed observation is lost.
- [ ] No rolled-back observation is learned.
- [ ] Retries do not create duplicate provider effects.
- [ ] Each injected failure has an auditable recovery receipt.

## Constraints

- Beads is the live source of truth. Re-read `br show tdmem-0508` and require `br ready` before a new claim.
- Unsatisfied blocking Beads dependencies at generation: tdmem-0505, tdmem-0506, tdmem-0507.
- Beads parent/hierarchy references: tdmem-0500. Parent readiness is enforced by `br`, not fabricated as a plan dependency.
- Build semantic producers/consumers and focused evidence before bookkeeping. Do not create hash-, receipt-, lock-, or presence-only gates.
- Root alone owns heavy Cargo execution, integration acceptance, commits, pushes, and Beads closure.
- Workers must stay inside their assigned write scope, preserve concurrent edits, and stop rather than widen scope.
- Do not contact upstream maintainers or mutate external work items.

## Operator Guidance

Intersect `plan-graph frontier` with live `br ready`; launch only Beads-ready or already-claimed nodes. Assign one coherent file/module owner, run focused checks, review the exact diff locally, then publish the green slice immediately. Close the Bead only after every acceptance item and real journey required by the issue are evidenced.
