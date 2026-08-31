---
name: tdmem-0800
overview: "Make recalled experience accountable to task outcomes. Useful experience should strengthen, harmful or stale experience should weaken, and provider-derived lessons must not silently become canonical project truth."
todos:
  - id: tdmem-0800-deliver
    content: "Deliver Bead tdmem-0800: M7 \u2014 Add outcome-grounded learning, curation, and forgetting; satisfy every recorded acceptance criterion, run the smallest behavioral dependency cone, then commit and push the green slice."
    status: pending
isProject: false
---

# tdmem-0800: M7 — Add outcome-grounded learning, curation, and forgetting

## Execution Notes

Beads issue: `tdmem-0800`. Current Beads status at generation: `open`.

Make recalled experience accountable to task outcomes. Useful experience should strengthen, harmful or stale
experience should weaken, and provider-derived lessons must not silently become canonical project truth.

Design authority:

Attribute outcomes to recalled items and context packs. Add explicit maturity and curation states inspired by
ACE/CASS and lifecycle lessons from Eidetic Engine, while preserving TraceDecay Native as the authority for
accepted explicit rules. Negative knowledge and blocked resurrection are first-class.

Acceptance authority:

- [ ] Helpful, harmful, and ignored outcomes are traceable to exact recalled items.
- [ ] Candidate lessons cannot become canonical rules without an audited promotion path.
- [ ] Supersession, contradiction, demotion, forgetting, undo, and blocked resurrection are testable.
- [ ] Provider-specific internal learning remains private to the provider while shared lifecycle events stay portable.

## Constraints

- Beads is the live source of truth. Re-read `br show tdmem-0800` and require `br ready` before a new claim.
- Unsatisfied blocking Beads dependencies at generation: tdmem-0609, tdmem-0709.
- Beads parent/hierarchy references: tdmem-0000. Parent readiness is enforced by `br`, not fabricated as a plan dependency.
- Build semantic producers/consumers and focused evidence before bookkeeping. Do not create hash-, receipt-, lock-, or presence-only gates.
- Root alone owns heavy Cargo execution, integration acceptance, commits, pushes, and Beads closure.
- Workers must stay inside their assigned write scope, preserve concurrent edits, and stop rather than widen scope.
- Do not contact upstream maintainers or mutate external work items.

## Operator Guidance

Intersect `plan-graph frontier` with live `br ready`; launch only Beads-ready or already-claimed nodes. Assign one coherent file/module owner, run focused checks, review the exact diff locally, then publish the green slice immediately. Close the Bead only after every acceptance item and real journey required by the issue are evidenced.
