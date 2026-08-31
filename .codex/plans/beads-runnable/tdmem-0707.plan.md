---
name: tdmem-0707
overview: "Make NCM state recoverable across process and host restarts."
todos:
  - id: tdmem-0707-deliver
    content: "Deliver Bead tdmem-0707: Implement NCM snapshot, restore, replay position, and state compatibility; satisfy every recorded acceptance criterion, run the smallest behavioral dependency cone, then commit and push the green slice."
    status: pending
isProject: false
---

# tdmem-0707: Implement NCM snapshot, restore, replay position, and state compatibility

## Execution Notes

Beads issue: `tdmem-0707`. Current Beads status at generation: `open`.

Make NCM state recoverable across process and host restarts.

Design authority:

Bind snapshots to provider build, state schema, configuration, sequence, checksum, and encryption metadata. Incompatible state never auto-resets silently.

Acceptance authority:

- [ ] Snapshot/restore round-trip preserves recall fixtures.
- [ ] Tail replay converges from a snapshot.
- [ ] Corrupt/incompatible state returns typed repair action.
- [ ] Privacy deletion includes retained snapshots.

## Constraints

- Beads is the live source of truth. Re-read `br show tdmem-0707` and require `br ready` before a new claim.
- Unsatisfied blocking Beads dependencies at generation: tdmem-0506, tdmem-0704.
- Beads parent/hierarchy references: tdmem-0700. Parent readiness is enforced by `br`, not fabricated as a plan dependency.
- Build semantic producers/consumers and focused evidence before bookkeeping. Do not create hash-, receipt-, lock-, or presence-only gates.
- Root alone owns heavy Cargo execution, integration acceptance, commits, pushes, and Beads closure.
- Workers must stay inside their assigned write scope, preserve concurrent edits, and stop rather than widen scope.
- Do not contact upstream maintainers or mutate external work items.

## Operator Guidance

Intersect `plan-graph frontier` with live `br ready`; launch only Beads-ready or already-claimed nodes. Assign one coherent file/module owner, run focused checks, review the exact diff locally, then publish the green slice immediately. Close the Bead only after every acceptance item and real journey required by the issue are evidenced.
