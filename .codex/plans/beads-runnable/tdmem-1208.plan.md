---
name: tdmem-1208
overview: "Advance from the initial PR #707 floor to a later approved Zack checkpoint using only the documented workflow."
todos:
  - id: tdmem-1208-deliver
    content: "Deliver Bead tdmem-1208: Rehearse the first full Zack convergence train; satisfy every recorded acceptance criterion, run the smallest behavioral dependency cone, then commit and push the green slice."
    status: pending
isProject: false
---

# tdmem-1208: Rehearse the first full Zack convergence train

## Execution Notes

Beads issue: `tdmem-1208`. Current Beads status at generation: `open`.

Advance from the initial PR #707 floor to a later approved Zack checkpoint using only the documented workflow.

Design authority:

Do not choose the target until it is reviewable. Preserve product patch isolation and record all conflict/semantic decisions.

Acceptance authority:

- [ ] Candidate floor is pinned.
- [ ] Classification and conflict receipts are complete.
- [ ] Upstream parity and product regression gates pass.
- [ ] Patch footprint does not grow without approved reasons.
- [ ] Rollback to prior floor is proven.

## Constraints

- Beads is the live source of truth. Re-read `br show tdmem-1208` and require `br ready` before a new claim.
- Unsatisfied blocking Beads dependencies at generation: tdmem-1206.
- Beads parent/hierarchy references: tdmem-1200. Parent readiness is enforced by `br`, not fabricated as a plan dependency.
- Build semantic producers/consumers and focused evidence before bookkeeping. Do not create hash-, receipt-, lock-, or presence-only gates.
- Root alone owns heavy Cargo execution, integration acceptance, commits, pushes, and Beads closure.
- Workers must stay inside their assigned write scope, preserve concurrent edits, and stop rather than widen scope.
- Do not contact upstream maintainers or mutate external work items.

## Operator Guidance

Intersect `plan-graph frontier` with live `br ready`; launch only Beads-ready or already-claimed nodes. Assign one coherent file/module owner, run focused checks, review the exact diff locally, then publish the green slice immediately. Close the Bead only after every acceptance item and real journey required by the issue are evidenced.
