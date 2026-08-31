---
name: tdmem-1108
overview: "Expose operational signals needed to debug and price the memory layer."
todos:
  - id: tdmem-1108-deliver
    content: "Deliver Bead tdmem-1108: Add latency, queue, storage, recall-quality, and maintenance telemetry; satisfy every recorded acceptance criterion, run the smallest behavioral dependency cone, then commit and push the green slice."
    status: pending
isProject: false
---

# tdmem-1108: Add latency, queue, storage, recall-quality, and maintenance telemetry

## Execution Notes

Beads issue: `tdmem-1108`. Current Beads status at generation: `open`.

Expose operational signals needed to debug and price the memory layer.

Design authority:

Telemetry is content-free by default and separates host, fabric, and provider time. Metrics are bounded-cardinality and include active/observer role.

Acceptance authority:

- [ ] p50/p95 recall and dispatch latency are available.
- [ ] Queue age/size and storage growth are visible.
- [ ] No raw memory content or high-cardinality secret identifiers are emitted.
- [ ] Telemetry overhead is measured.

## Constraints

- Beads is the live source of truth. Re-read `br show tdmem-1108` and require `br ready` before a new claim.
- Unsatisfied blocking Beads dependencies at generation: tdmem-0505, tdmem-0609, tdmem-0709.
- Beads parent/hierarchy references: tdmem-1100. Parent readiness is enforced by `br`, not fabricated as a plan dependency.
- Build semantic producers/consumers and focused evidence before bookkeeping. Do not create hash-, receipt-, lock-, or presence-only gates.
- Root alone owns heavy Cargo execution, integration acceptance, commits, pushes, and Beads closure.
- Workers must stay inside their assigned write scope, preserve concurrent edits, and stop rather than widen scope.
- Do not contact upstream maintainers or mutate external work items.

## Operator Guidance

Intersect `plan-graph frontier` with live `br ready`; launch only Beads-ready or already-claimed nodes. Assign one coherent file/module owner, run focused checks, review the exact diff locally, then publish the green slice immediately. Close the Bead only after every acceptance item and real journey required by the issue are evidenced.
