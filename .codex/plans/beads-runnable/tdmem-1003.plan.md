---
name: tdmem-1003
overview: "Validate Cursor through its supported TraceDecay V2 integration surface."
todos:
  - id: tdmem-1003-deliver
    content: "Deliver Bead tdmem-1003: Integrate and test Cursor memory context delivery; satisfy every recorded acceptance criterion, run the smallest behavioral dependency cone, then commit and push the green slice."
    status: pending
isProject: false
---

# tdmem-1003: Integrate and test Cursor memory context delivery

## Execution Notes

Beads issue: `tdmem-1003`. Current Beads status at generation: `open`.

Validate Cursor through its supported TraceDecay V2 integration surface.

Design authority:

Do not invent a separate runtime protocol. Use existing generated host bundle/rules/MCP path as supported by V2.

Acceptance authority:

- [ ] Context reaches Cursor through an upstream-supported route.
- [ ] The same provider and scope policy is reused.
- [ ] Known capability gaps are explicit.

## Constraints

- Beads is the live source of truth. Re-read `br show tdmem-1003` and require `br ready` before a new claim.
- Unsatisfied blocking Beads dependencies at generation: tdmem-0508, tdmem-0609.
- Beads parent/hierarchy references: tdmem-1000. Parent readiness is enforced by `br`, not fabricated as a plan dependency.
- Build semantic producers/consumers and focused evidence before bookkeeping. Do not create hash-, receipt-, lock-, or presence-only gates.
- Root alone owns heavy Cargo execution, integration acceptance, commits, pushes, and Beads closure.
- Workers must stay inside their assigned write scope, preserve concurrent edits, and stop rather than widen scope.
- Do not contact upstream maintainers or mutate external work items.

## Operator Guidance

Intersect `plan-graph frontier` with live `br ready`; launch only Beads-ready or already-claimed nodes. Assign one coherent file/module owner, run focused checks, review the exact diff locally, then publish the green slice immediately. Close the Bead only after every acceptance item and real journey required by the issue are evidenced.
