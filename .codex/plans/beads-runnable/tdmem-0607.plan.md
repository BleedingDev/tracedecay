---
name: tdmem-0607
overview: "Choose active provider and allowed degraded behavior without hidden substitutions."
todos:
  - id: tdmem-0607-deliver
    content: "Deliver Bead tdmem-0607: Implement explicit provider routing and fallback policy; satisfy every recorded acceptance criterion, run the smallest behavioral dependency cone, then commit and push the green slice."
    status: pending
isProject: false
---

# tdmem-0607: Implement explicit provider routing and fallback policy

## Execution Notes

Beads issue: `tdmem-0607`. Current Beads status at generation: `open`.

Choose active provider and allowed degraded behavior without hidden substitutions.

Design authority:

Routing is capability- and policy-driven. Native facts remain separately available; they are not a silent fallback for failed cognitive recall.

Acceptance authority:

- [ ] Configured provider identity appears in every result.
- [ ] Fallback requires an explicit configured rule.
- [ ] Unavailable and empty-success are distinguishable.
- [ ] Observer providers can never be selected for product output.

## Constraints

- Beads is the live source of truth. Re-read `br show tdmem-0607` and require `br ready` before a new claim.
- Unsatisfied blocking Beads dependencies at generation: tdmem-0601.
- Beads parent/hierarchy references: tdmem-0600. Parent readiness is enforced by `br`, not fabricated as a plan dependency.
- Build semantic producers/consumers and focused evidence before bookkeeping. Do not create hash-, receipt-, lock-, or presence-only gates.
- Root alone owns heavy Cargo execution, integration acceptance, commits, pushes, and Beads closure.
- Workers must stay inside their assigned write scope, preserve concurrent edits, and stop rather than widen scope.
- Do not contact upstream maintainers or mutate external work items.

## Operator Guidance

Intersect `plan-graph frontier` with live `br ready`; launch only Beads-ready or already-claimed nodes. Assign one coherent file/module owner, run focused checks, review the exact diff locally, then publish the green slice immediately. Close the Bead only after every acceptance item and real journey required by the issue are evidenced.
