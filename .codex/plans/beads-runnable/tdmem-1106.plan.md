---
name: tdmem-1106
overview: "Delete a prohibited source's future influence, including retained provider state."
todos:
  - id: tdmem-1106-deliver
    content: "Deliver Bead tdmem-1106: Implement verifiable privacy deletion across journal, provider, cache, and snapshots; satisfy every recorded acceptance criterion, run the smallest behavioral dependency cone, then commit and push the green slice."
    status: pending
isProject: false
---

# tdmem-1106: Implement verifiable privacy deletion across journal, provider, cache, and snapshots

## Execution Notes

Beads issue: `tdmem-1106`. Current Beads status at generation: `open`.

Delete a prohibited source's future influence, including retained provider state.

Design authority:

Use source-bound deletion receipts, snapshot rotation/rewrite policy, cache invalidation, and post-delete recall probes. Preserve only legally/operationally allowed audit tombstones.

Acceptance authority:

- [ ] Deleted source cannot affect later recall.
- [ ] Retained snapshots cannot restore it.
- [ ] Partial/unsupported deletion is explicit.
- [ ] Audit metadata does not retain prohibited content.

## Constraints

- Beads is the live source of truth. Re-read `br show tdmem-1106` and require `br ready` before a new claim.
- Unsatisfied blocking Beads dependencies at generation: tdmem-0502, tdmem-0706, tdmem-0707.
- Beads parent/hierarchy references: tdmem-1100. Parent readiness is enforced by `br`, not fabricated as a plan dependency.
- Build semantic producers/consumers and focused evidence before bookkeeping. Do not create hash-, receipt-, lock-, or presence-only gates.
- Root alone owns heavy Cargo execution, integration acceptance, commits, pushes, and Beads closure.
- Workers must stay inside their assigned write scope, preserve concurrent edits, and stop rather than widen scope.
- Do not contact upstream maintainers or mutate external work items.

## Operator Guidance

Intersect `plan-graph frontier` with live `br ready`; launch only Beads-ready or already-claimed nodes. Assign one coherent file/module owner, run focused checks, review the exact diff locally, then publish the green slice immediately. Close the Bead only after every acceptance item and real journey required by the issue are evidenced.
