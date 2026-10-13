---
name: tdmem-0605
overview: "Integrate admitted candidates with existing code, session, evidence, and Native fact sections."
todos:
  - id: tdmem-0605-deliver
    content: "Deliver Bead tdmem-0605: Bridge cognitive candidates into the canonical token-budgeted context compiler; satisfy every recorded acceptance criterion, run the smallest behavioral dependency cone, then commit and push the green slice."
    status: pending
isProject: false
---

# tdmem-0605: Bridge cognitive candidates into the canonical token-budgeted context compiler

## Execution Notes

Beads issue: `tdmem-0605`. Current Beads status at generation: `open`.

Integrate admitted candidates with existing code, session, evidence, and Native fact sections.

Design authority:

Use explicit section quotas and priorities. Cognitive recall cannot crowd out required code truth or safety evidence. Compilation produces a deterministic pack receipt.

Acceptance authority:

- [ ] Token budgets are enforced with the canonical tokenizer.
- [ ] Required host evidence cannot be evicted by provider volume.
- [ ] Pack section and item provenance is preserved.
- [ ] The same inputs/config produce the same pack hash.

## Constraints

- Beads is the live source of truth. Re-read `br show tdmem-0605` and require `br ready` before a new claim.
- Unsatisfied blocking Beads dependencies at generation: tdmem-0604.
- Beads parent/hierarchy references: tdmem-0600. Parent readiness is enforced by `br`, not fabricated as a plan dependency.
- Build semantic producers/consumers and focused evidence before bookkeeping. Do not create hash-, receipt-, lock-, or presence-only gates.
- Root alone owns heavy Cargo execution, integration acceptance, commits, pushes, and Beads closure.
- Workers must stay inside their assigned write scope, preserve concurrent edits, and stop rather than widen scope.
- Do not contact upstream maintainers or mutate external work items.

## Operator Guidance

Intersect `plan-graph frontier` with live `br ready`; launch only Beads-ready or already-claimed nodes. Assign one coherent file/module owner, run focused checks, review the exact diff locally, then publish the green slice immediately. Close the Bead only after every acceptance item and real journey required by the issue are evidenced.
