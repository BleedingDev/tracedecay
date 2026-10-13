---
name: tdmem-1104
overview: "Allow a developer to intervene in memory lifecycle safely."
todos:
  - id: tdmem-1104-deliver
    content: "Deliver Bead tdmem-1104: Add manual correct, forget, pin, and quarantine controls with receipts; satisfy every recorded acceptance criterion, run the smallest behavioral dependency cone, then commit and push the green slice."
    status: pending
isProject: false
---

# tdmem-1104: Add manual correct, forget, pin, and quarantine controls with receipts

## Execution Notes

Beads issue: `tdmem-1104`. Current Beads status at generation: `open`.

Allow a developer to intervene in memory lifecycle safely.

Design authority:

Controls are capability-gated, scoped, revision-bound, and audited. Provider controls cannot silently mutate Native canonical facts.

Acceptance authority:

- [ ] Stale revision updates are rejected.
- [ ] Every control has actor/reason/prior/new state.
- [ ] Undo is available where supported.
- [ ] Unsupported controls are explicit.

## Constraints

- Beads is the live source of truth. Re-read `br show tdmem-1104` and require `br ready` before a new claim.
- Unsatisfied blocking Beads dependencies at generation: tdmem-0808, tdmem-1103.
- Beads parent/hierarchy references: tdmem-1100. Parent readiness is enforced by `br`, not fabricated as a plan dependency.
- Build semantic producers/consumers and focused evidence before bookkeeping. Do not create hash-, receipt-, lock-, or presence-only gates.
- Root alone owns heavy Cargo execution, integration acceptance, commits, pushes, and Beads closure.
- Workers must stay inside their assigned write scope, preserve concurrent edits, and stop rather than widen scope.
- Do not contact upstream maintainers or mutate external work items.

## Operator Guidance

Intersect `plan-graph frontier` with live `br ready`; launch only Beads-ready or already-claimed nodes. Assign one coherent file/module owner, run focused checks, review the exact diff locally, then publish the green slice immediately. Close the Bead only after every acceptance item and real journey required by the issue are evidenced.
