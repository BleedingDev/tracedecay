---
name: tdmem-1405
overview: "Define what can upgrade independently and how incompatibility is surfaced."
todos:
  - id: tdmem-1405-deliver
    content: "Deliver Bead tdmem-1405: Publish protocol, provider, state, and upstream compatibility policy; satisfy every recorded acceptance criterion, run the smallest behavioral dependency cone, then commit and push the green slice."
    status: pending
isProject: false
---

# tdmem-1405: Publish protocol, provider, state, and upstream compatibility policy

## Execution Notes

Beads issue: `tdmem-1405`. Current Beads status at generation: `open`.

Define what can upgrade independently and how incompatibility is surfaced.

Design authority:

Version provider protocol, capability revisions, provider state schema, observation journal, context-pack schema, and accepted Zack floor separately.

Acceptance authority:

- [ ] Compatibility matrix is machine-readable.
- [ ] Upgrade order is documented.
- [ ] Unsupported combinations fail before mutation.
- [ ] Deprecation policy exists.

## Constraints

- Beads is the live source of truth. Re-read `br show tdmem-1405` and require `br ready` before a new claim.
- Unsatisfied blocking Beads dependencies at generation: tdmem-0707, tdmem-1208.
- Beads parent/hierarchy references: tdmem-1400. Parent readiness is enforced by `br`, not fabricated as a plan dependency.
- Build semantic producers/consumers and focused evidence before bookkeeping. Do not create hash-, receipt-, lock-, or presence-only gates.
- Root alone owns heavy Cargo execution, integration acceptance, commits, pushes, and Beads closure.
- Workers must stay inside their assigned write scope, preserve concurrent edits, and stop rather than widen scope.
- Do not contact upstream maintainers or mutate external work items.

## Operator Guidance

Intersect `plan-graph frontier` with live `br ready`; launch only Beads-ready or already-claimed nodes. Assign one coherent file/module owner, run focused checks, review the exact diff locally, then publish the green slice immediately. Close the Bead only after every acceptance item and real journey required by the issue are evidenced.
