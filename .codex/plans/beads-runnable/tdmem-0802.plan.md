---
name: tdmem-0802
overview: "Deliver outcome signals to capable providers and Native audit state."
todos:
  - id: tdmem-0802-deliver
    content: "Deliver Bead tdmem-0802: Implement helpful, harmful, ignored, and indeterminate feedback flow; satisfy every recorded acceptance criterion, run the smallest behavioral dependency cone, then commit and push the green slice."
    status: pending
isProject: false
---

# tdmem-0802: Implement helpful, harmful, ignored, and indeterminate feedback flow

## Execution Notes

Beads issue: `tdmem-0802`. Current Beads status at generation: `open`.

Deliver outcome signals to capable providers and Native audit state.

Design authority:

Do not infer helpfulness solely from retrieval count. Preserve human/agent/system source and reason. Harmful signals may carry higher policy weight but remain explicit.

Acceptance authority:

- [ ] All signal types round-trip with provenance.
- [ ] Providers receive only supported feedback.
- [ ] Ignored differs from harmful.
- [ ] Feedback retries are idempotent.

## Constraints

- Beads is the live source of truth. Re-read `br show tdmem-0802` and require `br ready` before a new claim.
- Unsatisfied blocking Beads dependencies at generation: tdmem-0801.
- Beads parent/hierarchy references: tdmem-0800. Parent readiness is enforced by `br`, not fabricated as a plan dependency.
- Build semantic producers/consumers and focused evidence before bookkeeping. Do not create hash-, receipt-, lock-, or presence-only gates.
- Root alone owns heavy Cargo execution, integration acceptance, commits, pushes, and Beads closure.
- Workers must stay inside their assigned write scope, preserve concurrent edits, and stop rather than widen scope.
- Do not contact upstream maintainers or mutate external work items.

## Operator Guidance

Intersect `plan-graph frontier` with live `br ready`; launch only Beads-ready or already-claimed nodes. Assign one coherent file/module owner, run focused checks, review the exact diff locally, then publish the green slice immediately. Close the Bead only after every acceptance item and real journey required by the issue are evidenced.
