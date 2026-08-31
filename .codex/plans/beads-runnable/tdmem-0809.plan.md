---
name: tdmem-0809
overview: "Exercise recall \u2192 use \u2192 feedback \u2192 maturity/curation \u2192 later recall, including a harmful stale lesson."
todos:
  - id: tdmem-0809-deliver
    content: "Deliver Bead tdmem-0809: Run the complete learning and forgetting product journey; satisfy every recorded acceptance criterion, run the smallest behavioral dependency cone, then commit and push the green slice."
    status: pending
isProject: false
---

# tdmem-0809: Run the complete learning and forgetting product journey

## Execution Notes

Beads issue: `tdmem-0809`. Current Beads status at generation: `open`.

Exercise recall → use → feedback → maturity/curation → later recall, including a harmful stale lesson.

Design authority:

Use a real coding-agent host. Show one lesson strengthening and one lesson being corrected/superseded/forgotten with receipts.

Acceptance authority:

- [ ] Helpful experience becomes more likely/reliable according to declared policy.
- [ ] Harmful stale experience stops influencing later context.
- [ ] Negative knowledge or corrected replacement remains available where appropriate.
- [ ] All transitions are inspectable.

## Constraints

- Beads is the live source of truth. Re-read `br show tdmem-0809` and require `br ready` before a new claim.
- Unsatisfied blocking Beads dependencies at generation: tdmem-0802, tdmem-0808.
- Beads parent/hierarchy references: tdmem-0800. Parent readiness is enforced by `br`, not fabricated as a plan dependency.
- Build semantic producers/consumers and focused evidence before bookkeeping. Do not create hash-, receipt-, lock-, or presence-only gates.
- Root alone owns heavy Cargo execution, integration acceptance, commits, pushes, and Beads closure.
- Workers must stay inside their assigned write scope, preserve concurrent edits, and stop rather than widen scope.
- Do not contact upstream maintainers or mutate external work items.

## Operator Guidance

Intersect `plan-graph frontier` with live `br ready`; launch only Beads-ready or already-claimed nodes. Assign one coherent file/module owner, run focused checks, review the exact diff locally, then publish the green slice immediately. Close the Bead only after every acceptance item and real journey required by the issue are evidenced.
