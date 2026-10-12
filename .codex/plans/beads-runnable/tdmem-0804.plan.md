---
name: tdmem-0804
overview: "Preserve what failed, under which conditions, and the safer alternative instead of simply deleting bad advice."
todos:
  - id: tdmem-0804-deliver
    content: "Deliver Bead tdmem-0804: Represent negative knowledge and failed approaches as first-class candidates; satisfy every recorded acceptance criterion, run the smallest behavioral dependency cone, then commit and push the green slice."
    status: pending
isProject: false
---

# tdmem-0804: Represent negative knowledge and failed approaches as first-class candidates

## Execution Notes

Beads issue: `tdmem-0804`. Current Beads status at generation: `open`.

Preserve what failed, under which conditions, and the safer alternative instead of simply deleting bad advice.

Design authority:

Negative candidates retain source evidence and scope. They are surfaced only when relevant and cannot become universal prohibitions without validation.

Acceptance authority:

- [ ] Failures and anti-patterns retain condition and provenance.
- [ ] Negative candidates participate in context budgets separately.
- [ ] A harmful positive rule can produce a reviewable negative candidate.

## Constraints

- Beads is the live source of truth. Re-read `br show tdmem-0804` and require `br ready` before a new claim.
- Unsatisfied blocking Beads dependencies at generation: tdmem-0803.
- Beads parent/hierarchy references: tdmem-0800. Parent readiness is enforced by `br`, not fabricated as a plan dependency.
- Build semantic producers/consumers and focused evidence before bookkeeping. Do not create hash-, receipt-, lock-, or presence-only gates.
- Root alone owns heavy Cargo execution, integration acceptance, commits, pushes, and Beads closure.
- Workers must stay inside their assigned write scope, preserve concurrent edits, and stop rather than widen scope.
- Do not contact upstream maintainers or mutate external work items.

## Operator Guidance

Intersect `plan-graph frontier` with live `br ready`; launch only Beads-ready or already-claimed nodes. Assign one coherent file/module owner, run focused checks, review the exact diff locally, then publish the green slice immediately. Close the Bead only after every acceptance item and real journey required by the issue are evidenced.
