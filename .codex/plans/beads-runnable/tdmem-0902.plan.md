---
name: tdmem-0902
overview: "Provide strong baselines so NCM is measured against realistic alternatives rather than only no memory."
todos:
  - id: tdmem-0902-deliver
    content: "Deliver Bead tdmem-0902: Implement no-memory, explicit-doc, and Native baselines; satisfy every recorded acceptance criterion, run the smallest behavioral dependency cone, then commit and push the green slice."
    status: pending
isProject: false
---

# tdmem-0902: Implement no-memory, explicit-doc, and Native baselines

## Execution Notes

Beads issue: `tdmem-0902`. Current Beads status at generation: `open`.

Provide strong baselines so NCM is measured against realistic alternatives rather than only no memory.

Design authority:

Run the same host/task scenarios with memory disabled, AGENTS.md/explicit documentation, and TraceDecay Native.

Acceptance authority:

- [ ] All baselines use the same task and host configuration.
- [ ] Baseline context/token costs are recorded.
- [ ] Results are reproducible.

## Constraints

- Beads is the live source of truth. Re-read `br show tdmem-0902` and require `br ready` before a new claim.
- Unsatisfied blocking Beads dependencies at generation: tdmem-0406, tdmem-0901.
- Beads parent/hierarchy references: tdmem-0900. Parent readiness is enforced by `br`, not fabricated as a plan dependency.
- Build semantic producers/consumers and focused evidence before bookkeeping. Do not create hash-, receipt-, lock-, or presence-only gates.
- Root alone owns heavy Cargo execution, integration acceptance, commits, pushes, and Beads closure.
- Workers must stay inside their assigned write scope, preserve concurrent edits, and stop rather than widen scope.
- Do not contact upstream maintainers or mutate external work items.

## Operator Guidance

Intersect `plan-graph frontier` with live `br ready`; launch only Beads-ready or already-claimed nodes. Assign one coherent file/module owner, run focused checks, review the exact diff locally, then publish the green slice immediately. Close the Bead only after every acceptance item and real journey required by the issue are evidenced.
