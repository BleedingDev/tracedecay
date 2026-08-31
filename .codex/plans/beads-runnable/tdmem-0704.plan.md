---
name: tdmem-0704
overview: "Translate admitted provider-neutral observations into NCM inputs."
todos:
  - id: tdmem-0704-deliver
    content: "Deliver Bead tdmem-0704: Implement NCM observation ingestion with idempotency and exact coding scope; satisfy every recorded acceptance criterion, run the smallest behavioral dependency cone, then commit and push the green slice."
    status: pending
isProject: false
---

# tdmem-0704: Implement NCM observation ingestion with idempotency and exact coding scope

## Execution Notes

Beads issue: `tdmem-0704`. Current Beads status at generation: `open`.

Translate admitted provider-neutral observations into NCM inputs.

Design authority:

Keep TraceDecay coding metadata in adapter-owned extensions. NCM receives useful content and salience/context signals without becoming coupled to Git or code graph internals.

Acceptance authority:

- [ ] Duplicate observations have one effect.
- [ ] Project/worktree/session scope is preserved in adapter state.
- [ ] Rejected observations return typed reasons.
- [ ] Secrets/transient noise do not reach NCM.

## Constraints

- Beads is the live source of truth. Re-read `br show tdmem-0704` and require `br ready` before a new claim.
- Unsatisfied blocking Beads dependencies at generation: tdmem-0507, tdmem-0703.
- Beads parent/hierarchy references: tdmem-0700. Parent readiness is enforced by `br`, not fabricated as a plan dependency.
- Build semantic producers/consumers and focused evidence before bookkeeping. Do not create hash-, receipt-, lock-, or presence-only gates.
- Root alone owns heavy Cargo execution, integration acceptance, commits, pushes, and Beads closure.
- Workers must stay inside their assigned write scope, preserve concurrent edits, and stop rather than widen scope.
- Do not contact upstream maintainers or mutate external work items.

## Operator Guidance

Intersect `plan-graph frontier` with live `br ready`; launch only Beads-ready or already-claimed nodes. Assign one coherent file/module owner, run focused checks, review the exact diff locally, then publish the green slice immediately. Close the Bead only after every acceptance item and real journey required by the issue are evidenced.
