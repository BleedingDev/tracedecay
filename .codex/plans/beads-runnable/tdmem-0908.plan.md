---
name: tdmem-0908
overview: "Define gates that prevent unsafe provider regressions and premature active NCM rollout."
todos:
  - id: tdmem-0908-deliver
    content: "Deliver Bead tdmem-0908: Set CI regression and active-provider activation thresholds; satisfy every recorded acceptance criterion, run the smallest behavioral dependency cone, then commit and push the green slice."
    status: pending
isProject: false
---

# tdmem-0908: Set CI regression and active-provider activation thresholds

## Execution Notes

Beads issue: `tdmem-0908`. Current Beads status at generation: `open`.

Define gates that prevent unsafe provider regressions and premature active NCM rollout.

Design authority:

Safety-critical thresholds include zero cross-scope leakage, bounded failure behavior, harmful stale-recall ceiling, deletion correctness, and conformance. Outcome improvements may use confidence intervals.

Acceptance authority:

- [ ] Thresholds are versioned and justified.
- [ ] Safety failures block regardless of aggregate task score.
- [ ] Activation gate consumes exact report artifacts.
- [ ] Threshold changes require review.

## Constraints

- Beads is the live source of truth. Re-read `br show tdmem-0908` and require `br ready` before a new claim.
- Unsatisfied blocking Beads dependencies at generation: tdmem-0907.
- Beads parent/hierarchy references: tdmem-0900. Parent readiness is enforced by `br`, not fabricated as a plan dependency.
- Build semantic producers/consumers and focused evidence before bookkeeping. Do not create hash-, receipt-, lock-, or presence-only gates.
- Root alone owns heavy Cargo execution, integration acceptance, commits, pushes, and Beads closure.
- Workers must stay inside their assigned write scope, preserve concurrent edits, and stop rather than widen scope.
- Do not contact upstream maintainers or mutate external work items.

## Operator Guidance

Intersect `plan-graph frontier` with live `br ready`; launch only Beads-ready or already-claimed nodes. Assign one coherent file/module owner, run focused checks, review the exact diff locally, then publish the green slice immediately. Close the Bead only after every acceptance item and real journey required by the issue are evidenced.
