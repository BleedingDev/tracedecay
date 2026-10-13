---
name: tdmem-1407
overview: "Ship NCM evaluation to selected users without allowing NCM recall into prompts."
todos:
  - id: tdmem-1407-deliver
    content: "Deliver Bead tdmem-1407: Cut and verify an NCM observer alpha release candidate; satisfy every recorded acceptance criterion, run the smallest behavioral dependency cone, then commit and push the green slice."
    status: pending
isProject: false
---

# tdmem-1407: Cut and verify an NCM observer alpha release candidate

## Execution Notes

Beads issue: `tdmem-1407`. Current Beads status at generation: `open`.

Ship NCM evaluation to selected users without allowing NCM recall into prompts.

Design authority:

Observer consent, resource limits, state location, diagnostics, and deletion are explicit.

Acceptance authority:

- [ ] Observer isolation passes in release artifact.
- [ ] NCM failures do not affect product outputs.
- [ ] Evaluation artifacts can be exported safely.
- [ ] Uninstall/disable preserves user control of state.

## Constraints

- Beads is the live source of truth. Re-read `br show tdmem-1407` and require `br ready` before a new claim.
- Unsatisfied blocking Beads dependencies at generation: tdmem-0710, tdmem-1106, tdmem-1406.
- Beads parent/hierarchy references: tdmem-1400. Parent readiness is enforced by `br`, not fabricated as a plan dependency.
- Build semantic producers/consumers and focused evidence before bookkeeping. Do not create hash-, receipt-, lock-, or presence-only gates.
- Root alone owns heavy Cargo execution, integration acceptance, commits, pushes, and Beads closure.
- Workers must stay inside their assigned write scope, preserve concurrent edits, and stop rather than widen scope.
- Do not contact upstream maintainers or mutate external work items.

## Operator Guidance

Intersect `plan-graph frontier` with live `br ready`; launch only Beads-ready or already-claimed nodes. Assign one coherent file/module owner, run focused checks, review the exact diff locally, then publish the green slice immediately. Close the Bead only after every acceptance item and real journey required by the issue are evidenced.
