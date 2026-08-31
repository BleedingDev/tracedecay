---
name: tdmem-0906
overview: "Ensure evaluation can add OCEAN without changing scenario or metric contracts."
todos:
  - id: tdmem-0906-deliver
    content: "Deliver Bead tdmem-0906: Reserve a generic future-provider differential slot; satisfy every recorded acceptance criterion, run the smallest behavioral dependency cone, then commit and push the green slice."
    status: pending
isProject: false
---

# tdmem-0906: Reserve a generic future-provider differential slot

## Execution Notes

Beads issue: `tdmem-0906`. Current Beads status at generation: `open`.

Ensure evaluation can add OCEAN without changing scenario or metric contracts.

Design authority:

Provider registration is capability-driven. No OCEAN implementation or provider-name branch is added.

Acceptance authority:

- [ ] A second dummy provider can enter the runner through registry only.
- [ ] Reports remain schema-compatible.
- [ ] Missing optional capabilities are handled explicitly.

## Constraints

- Beads is the live source of truth. Re-read `br show tdmem-0906` and require `br ready` before a new claim.
- Unsatisfied blocking Beads dependencies at generation: tdmem-0905.
- Beads parent/hierarchy references: tdmem-0900. Parent readiness is enforced by `br`, not fabricated as a plan dependency.
- Build semantic producers/consumers and focused evidence before bookkeeping. Do not create hash-, receipt-, lock-, or presence-only gates.
- Root alone owns heavy Cargo execution, integration acceptance, commits, pushes, and Beads closure.
- Workers must stay inside their assigned write scope, preserve concurrent edits, and stop rather than widen scope.
- Do not contact upstream maintainers or mutate external work items.

## Operator Guidance

Intersect `plan-graph frontier` with live `br ready`; launch only Beads-ready or already-claimed nodes. Assign one coherent file/module owner, run focused checks, review the exact diff locally, then publish the green slice immediately. Close the Bead only after every acceptance item and real journey required by the issue are evidenced.
