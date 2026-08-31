---
name: tdmem-0601
overview: "Introduce the narrow application boundary that requests advisory recall from the fabric."
todos:
  - id: tdmem-0601-deliver
    content: "Deliver Bead tdmem-0601: Add a CognitiveRecallPort to the TraceDecay application layer; satisfy every recorded acceptance criterion, run the smallest behavioral dependency cone, then commit and push the green slice."
    status: pending
isProject: false
---

# tdmem-0601: Add a CognitiveRecallPort to the TraceDecay application layer

## Execution Notes

Beads issue: `tdmem-0601`. Current Beads status at generation: `open`.

Introduce the narrow application boundary that requests advisory recall from the fabric.

Design authority:

Follow V2 typed-port conventions. The port accepts exact resolved coding scope and returns typed candidates/degradation without owning context packing or canonical facts.

Acceptance authority:

- [ ] The port is transport-neutral.
- [ ] No provider-specific type enters TraceDecay application contracts.
- [ ] Deadline and cancellation propagate.
- [ ] Existing retrieval ports remain authoritative for their domains.

## Constraints

- Beads is the live source of truth. Re-read `br show tdmem-0601` and require `br ready` before a new claim.
- Unsatisfied blocking Beads dependencies at generation: tdmem-0406.
- Beads parent/hierarchy references: tdmem-0600. Parent readiness is enforced by `br`, not fabricated as a plan dependency.
- Build semantic producers/consumers and focused evidence before bookkeeping. Do not create hash-, receipt-, lock-, or presence-only gates.
- Root alone owns heavy Cargo execution, integration acceptance, commits, pushes, and Beads closure.
- Workers must stay inside their assigned write scope, preserve concurrent edits, and stop rather than widen scope.
- Do not contact upstream maintainers or mutate external work items.

## Operator Guidance

Intersect `plan-graph frontier` with live `br ready`; launch only Beads-ready or already-claimed nodes. Assign one coherent file/module owner, run focused checks, review the exact diff locally, then publish the green slice immediately. Close the Bead only after every acceptance item and real journey required by the issue are evidenced.
