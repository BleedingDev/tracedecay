---
name: tdmem-0505
overview: "Prevent slow providers from consuming unbounded memory or silently losing observations."
todos:
  - id: tdmem-0505-deliver
    content: "Deliver Bead tdmem-0505: Implement backpressure, load shedding, and no-silent-drop invariants; satisfy every recorded acceptance criterion, run the smallest behavioral dependency cone, then commit and push the green slice."
    status: pending
isProject: false
---

# tdmem-0505: Implement backpressure, load shedding, and no-silent-drop invariants

## Execution Notes

Beads issue: `tdmem-0505`. Current Beads status at generation: `open`.

Prevent slow providers from consuming unbounded memory or silently losing observations.

Design authority:

When limits are reached, persist backlog, reject new optional work, or enter explicit degraded mode according to policy. Never report success for dropped observations.

Acceptance authority:

- [ ] Queue saturation is reproducible in tests.
- [ ] No observation disappears without a terminal receipt.
- [ ] Coding-agent foreground latency stays within the declared budget.
- [ ] Metrics expose backlog age and size.

## Constraints

- Beads is the live source of truth. Re-read `br show tdmem-0505` and require `br ready` before a new claim.
- Unsatisfied blocking Beads dependencies at generation: tdmem-0503, tdmem-0504.
- Beads parent/hierarchy references: tdmem-0500. Parent readiness is enforced by `br`, not fabricated as a plan dependency.
- Build semantic producers/consumers and focused evidence before bookkeeping. Do not create hash-, receipt-, lock-, or presence-only gates.
- Root alone owns heavy Cargo execution, integration acceptance, commits, pushes, and Beads closure.
- Workers must stay inside their assigned write scope, preserve concurrent edits, and stop rather than widen scope.
- Do not contact upstream maintainers or mutate external work items.

## Operator Guidance

Intersect `plan-graph frontier` with live `br ready`; launch only Beads-ready or already-claimed nodes. Assign one coherent file/module owner, run focused checks, review the exact diff locally, then publish the green slice immediately. Close the Bead only after every acceptance item and real journey required by the issue are evidenced.
