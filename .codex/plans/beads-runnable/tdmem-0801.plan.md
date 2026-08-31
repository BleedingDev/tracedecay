---
name: tdmem-0801
overview: "Create a durable link from an agent-visible provider item to later task feedback."
todos:
  - id: tdmem-0801-deliver
    content: "Deliver Bead tdmem-0801: Attribute context-pack outcomes to exact recalled items; satisfy every recorded acceptance criterion, run the smallest behavioral dependency cone, then commit and push the green slice."
    status: pending
isProject: false
---

# tdmem-0801: Attribute context-pack outcomes to exact recalled items

## Execution Notes

Beads issue: `tdmem-0801`. Current Beads status at generation: `open`.

Create a durable link from an agent-visible provider item to later task feedback.

Design authority:

Pack items carry stable trace/item identities. Outcome events distinguish shown, cited/used, helpful, harmful, ignored, and indeterminate.

Acceptance authority:

- [ ] Feedback resolves to exact provider/build/trace/item.
- [ ] Pack reorder or restart does not misattribute outcomes.
- [ ] Unknown/expired item refs fail explicitly.

## Constraints

- Beads is the live source of truth. Re-read `br show tdmem-0801` and require `br ready` before a new claim.
- Unsatisfied blocking Beads dependencies at generation: tdmem-0608, tdmem-0705.
- Beads parent/hierarchy references: tdmem-0800. Parent readiness is enforced by `br`, not fabricated as a plan dependency.
- Build semantic producers/consumers and focused evidence before bookkeeping. Do not create hash-, receipt-, lock-, or presence-only gates.
- Root alone owns heavy Cargo execution, integration acceptance, commits, pushes, and Beads closure.
- Workers must stay inside their assigned write scope, preserve concurrent edits, and stop rather than widen scope.
- Do not contact upstream maintainers or mutate external work items.

## Operator Guidance

Intersect `plan-graph frontier` with live `br ready`; launch only Beads-ready or already-claimed nodes. Assign one coherent file/module owner, run focused checks, review the exact diff locally, then publish the green slice immediately. Close the Bead only after every acceptance item and real journey required by the issue are evidenced.
