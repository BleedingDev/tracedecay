---
name: tdmem-1109
overview: "Package enough state to diagnose provider and integration failures without sharing memory content by default."
todos:
  - id: tdmem-1109-deliver
    content: "Deliver Bead tdmem-1109: Create a redaction-safe support bundle and diagnostics workflow; satisfy every recorded acceptance criterion, run the smallest behavioral dependency cone, then commit and push the green slice."
    status: pending
isProject: false
---

# tdmem-1109: Create a redaction-safe support bundle and diagnostics workflow

## Execution Notes

Beads issue: `tdmem-1109`. Current Beads status at generation: `open`.

Package enough state to diagnose provider and integration failures without sharing memory content by default.

Design authority:

Include versions, manifests, capabilities, config digests, queue health, typed failures, selected receipts, test summaries, and optional explicit content export.

Acceptance authority:

- [ ] Bundle passes redaction checks.
- [ ] Content inclusion is explicit opt-in.
- [ ] A clean machine can validate bundle schema.
- [ ] Repair hints refer to real supported commands.

## Constraints

- Beads is the live source of truth. Re-read `br show tdmem-1109` and require `br ready` before a new claim.
- Unsatisfied blocking Beads dependencies at generation: tdmem-1101, tdmem-1102, tdmem-1108.
- Beads parent/hierarchy references: tdmem-1100. Parent readiness is enforced by `br`, not fabricated as a plan dependency.
- Build semantic producers/consumers and focused evidence before bookkeeping. Do not create hash-, receipt-, lock-, or presence-only gates.
- Root alone owns heavy Cargo execution, integration acceptance, commits, pushes, and Beads closure.
- Workers must stay inside their assigned write scope, preserve concurrent edits, and stop rather than widen scope.
- Do not contact upstream maintainers or mutate external work items.

## Operator Guidance

Intersect `plan-graph frontier` with live `br ready`; launch only Beads-ready or already-claimed nodes. Assign one coherent file/module owner, run focused checks, review the exact diff locally, then publish the green slice immediately. Close the Bead only after every acceptance item and real journey required by the issue are evidenced.
