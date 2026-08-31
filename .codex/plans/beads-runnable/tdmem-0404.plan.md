---
name: tdmem-0404
overview: "Ensure users who do not enable the feature experience the pinned upstream V2 behavior."
todos:
  - id: tdmem-0404-deliver
    content: "Deliver Bead tdmem-0404: Prove disabled Memory Fabric mode is behaviorally inert; satisfy every recorded acceptance criterion, run the smallest behavioral dependency cone, then commit and push the green slice."
    status: pending
isProject: false
---

# tdmem-0404: Prove disabled Memory Fabric mode is behaviorally inert

## Execution Notes

Beads issue: `tdmem-0404`. Current Beads status at generation: `open`.

Ensure users who do not enable the feature experience the pinned upstream V2 behavior.

Design authority:

Compare CLI/MCP/SDK/dashboard/host outputs and state changes on selected product journeys with the feature absent or disabled.

Acceptance authority:

- [ ] No new state directories or background work are created.
- [ ] Tool discovery and generated contracts are unchanged.
- [ ] Context packs and memory results match pinned baseline fixtures.
- [ ] Performance overhead is within the declared zero/near-zero budget.

## Constraints

- Beads is the live source of truth. Re-read `br show tdmem-0404` and require `br ready` before a new claim.
- Unsatisfied blocking Beads dependencies at generation: tdmem-0403.
- Beads parent/hierarchy references: tdmem-0400. Parent readiness is enforced by `br`, not fabricated as a plan dependency.
- Build semantic producers/consumers and focused evidence before bookkeeping. Do not create hash-, receipt-, lock-, or presence-only gates.
- Root alone owns heavy Cargo execution, integration acceptance, commits, pushes, and Beads closure.
- Workers must stay inside their assigned write scope, preserve concurrent edits, and stop rather than widen scope.
- Do not contact upstream maintainers or mutate external work items.

## Operator Guidance

Intersect `plan-graph frontier` with live `br ready`; launch only Beads-ready or already-claimed nodes. Assign one coherent file/module owner, run focused checks, review the exact diff locally, then publish the green slice immediately. Close the Bead only after every acceptance item and real journey required by the issue are evidenced.
