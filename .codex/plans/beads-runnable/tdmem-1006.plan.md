---
name: tdmem-1006
overview: "Run an agent in a fresh sandbox, restart provider/daemon, and continue without losing or duplicating experience."
todos:
  - id: tdmem-1006-deliver
    content: "Deliver Bead tdmem-1006: Prove coding-agent sandbox and provider restart lifecycle; satisfy every recorded acceptance criterion, run the smallest behavioral dependency cone, then commit and push the green slice."
    status: pending
isProject: false
---

# tdmem-1006: Prove coding-agent sandbox and provider restart lifecycle

## Execution Notes

Beads issue: `tdmem-1006`. Current Beads status at generation: `open`.

Run an agent in a fresh sandbox, restart provider/daemon, and continue without losing or duplicating experience.

Design authority:

Exercise install/discovery, handshake, state restore, outbox replay, context injection, and outcome attribution across restart.

Acceptance authority:

- [ ] Fresh sandbox connects without hidden manual state.
- [ ] Restart recovers exact acknowledged sequence.
- [ ] No duplicate observation effect occurs.
- [ ] Typed repair is shown for incompatible state.

## Constraints

- Beads is the live source of truth. Re-read `br show tdmem-1006` and require `br ready` before a new claim.
- Unsatisfied blocking Beads dependencies at generation: tdmem-0508, tdmem-0707, tdmem-1001.
- Beads parent/hierarchy references: tdmem-1000. Parent readiness is enforced by `br`, not fabricated as a plan dependency.
- Build semantic producers/consumers and focused evidence before bookkeeping. Do not create hash-, receipt-, lock-, or presence-only gates.
- Root alone owns heavy Cargo execution, integration acceptance, commits, pushes, and Beads closure.
- Workers must stay inside their assigned write scope, preserve concurrent edits, and stop rather than widen scope.
- Do not contact upstream maintainers or mutate external work items.

## Operator Guidance

Intersect `plan-graph frontier` with live `br ready`; launch only Beads-ready or already-claimed nodes. Assign one coherent file/module owner, run focused checks, review the exact diff locally, then publish the green slice immediately. Close the Bead only after every acceptance item and real journey required by the issue are evidenced.
