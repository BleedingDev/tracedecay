---
name: tdmem-1406
overview: "Release the new provider architecture with only the proven Native path active."
todos:
  - id: tdmem-1406-deliver
    content: "Deliver Bead tdmem-1406: Cut and verify a Native-only alpha release candidate; satisfy every recorded acceptance criterion, run the smallest behavioral dependency cone, then commit and push the green slice."
    status: pending
isProject: false
---

# tdmem-1406: Cut and verify a Native-only alpha release candidate

## Execution Notes

Beads issue: `tdmem-1406`. Current Beads status at generation: `open`.

Release the new provider architecture with only the proven Native path active.

Design authority:

This validates packaging, upgrade, host integration, parity, diagnostics, and rollback before NCM affects product behavior.

Acceptance authority:

- [ ] Clean install and upgrade pass.
- [ ] Native parity and host journeys pass.
- [ ] NCM code is absent or inactive according to feature policy.
- [ ] Support bundle is usable.

## Constraints

- Beads is the live source of truth. Re-read `br show tdmem-1406` and require `br ready` before a new claim.
- Unsatisfied blocking Beads dependencies at generation: tdmem-1109, tdmem-1402, tdmem-1403, tdmem-1404, tdmem-1405.
- Beads parent/hierarchy references: tdmem-1400. Parent readiness is enforced by `br`, not fabricated as a plan dependency.
- Build semantic producers/consumers and focused evidence before bookkeeping. Do not create hash-, receipt-, lock-, or presence-only gates.
- Root alone owns heavy Cargo execution, integration acceptance, commits, pushes, and Beads closure.
- Workers must stay inside their assigned write scope, preserve concurrent edits, and stop rather than widen scope.
- Do not contact upstream maintainers or mutate external work items.

## Operator Guidance

Intersect `plan-graph frontier` with live `br ready`; launch only Beads-ready or already-claimed nodes. Assign one coherent file/module owner, run focused checks, review the exact diff locally, then publish the green slice immediately. Close the Bead only after every acceptance item and real journey required by the issue are evidenced.
