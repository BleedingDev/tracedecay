---
name: tdmem-0609
overview: "Exercise host request through provider recall, admission, hydration, packing, and injection."
todos:
  - id: tdmem-0609-deliver
    content: "Deliver Bead tdmem-0609: Run the full cognitive-recall read-path product journey; satisfy every recorded acceptance criterion, run the smallest behavioral dependency cone, then commit and push the green slice."
    status: pending
isProject: false
---

# tdmem-0609: Run the full cognitive-recall read-path product journey

## Execution Notes

Beads issue: `tdmem-0609`. Current Beads status at generation: `open`.

Exercise host request through provider recall, admission, hydration, packing, and injection.

Design authority:

Use dummy or Native provider first. The test must cross actual daemon/host integration and inspect the final context consumed by a coding agent fixture.

Acceptance authority:

- [ ] Exact coding scope is preserved end to end.
- [ ] Selected context is bounded and cited.
- [ ] Provider timeout and cancellation variants pass.
- [ ] No silent fallback occurs.

## Constraints

- Beads is the live source of truth. Re-read `br show tdmem-0609` and require `br ready` before a new claim.
- Unsatisfied blocking Beads dependencies at generation: tdmem-0606, tdmem-0608.
- Beads parent/hierarchy references: tdmem-0600. Parent readiness is enforced by `br`, not fabricated as a plan dependency.
- Build semantic producers/consumers and focused evidence before bookkeeping. Do not create hash-, receipt-, lock-, or presence-only gates.
- Root alone owns heavy Cargo execution, integration acceptance, commits, pushes, and Beads closure.
- Workers must stay inside their assigned write scope, preserve concurrent edits, and stop rather than widen scope.
- Do not contact upstream maintainers or mutate external work items.

## Operator Guidance

Intersect `plan-graph frontier` with live `br ready`; launch only Beads-ready or already-claimed nodes. Assign one coherent file/module owner, run focused checks, review the exact diff locally, then publish the green slice immediately. Close the Bead only after every acceptance item and real journey required by the issue are evidenced.
