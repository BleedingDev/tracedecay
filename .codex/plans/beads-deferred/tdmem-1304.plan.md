---
name: tdmem-1304
overview: "Run OCEAN through the same mandatory, capability, isolation, and differential suites."
todos:
  - id: tdmem-1304-deliver
    content: "Deliver Bead tdmem-1304: Pass OCEAN conformance and observer-mode evaluation; satisfy every recorded acceptance criterion, run the smallest behavioral dependency cone, then commit and push the green slice."
    status: pending
isProject: false
---

# tdmem-1304: Pass OCEAN conformance and observer-mode evaluation

## Execution Notes

Beads issue: `tdmem-1304`. Current Beads status at generation: `deferred`.

Run OCEAN through the same mandatory, capability, isolation, and differential suites.

Design authority:

Observer mode only. No product influence until reports pass review.

Acceptance authority:

- [ ] Mandatory conformance passes.
- [ ] Observer isolation passes.
- [ ] Differential report is reproducible.
- [ ] Known gaps are explicit.

## Constraints

- Beads is the live source of truth. Re-read `br show tdmem-1304` and require `br ready` before a new claim.
- Unsatisfied blocking Beads dependencies at generation: tdmem-0906, tdmem-1303.
- Beads parent/hierarchy references: tdmem-1300. Parent readiness is enforced by `br`, not fabricated as a plan dependency.
- Build semantic producers/consumers and focused evidence before bookkeeping. Do not create hash-, receipt-, lock-, or presence-only gates.
- Root alone owns heavy Cargo execution, integration acceptance, commits, pushes, and Beads closure.
- Workers must stay inside their assigned write scope, preserve concurrent edits, and stop rather than widen scope.
- Do not contact upstream maintainers or mutate external work items.

## Operator Guidance

Intersect `plan-graph frontier` with live `br ready`; launch only Beads-ready or already-claimed nodes. Assign one coherent file/module owner, run focused checks, review the exact diff locally, then publish the green slice immediately. Close the Bead only after every acceptance item and real journey required by the issue are evidenced.
