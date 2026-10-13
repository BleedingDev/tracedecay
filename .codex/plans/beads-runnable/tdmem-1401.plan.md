---
name: tdmem-1401
overview: "State supported OS/architecture, coding-agent hosts, provider modes, features, and known exclusions."
todos:
  - id: tdmem-1401-deliver
    content: "Deliver Bead tdmem-1401: Define the alpha support matrix and product defaults; satisfy every recorded acceptance criterion, run the smallest behavioral dependency cone, then commit and push the green slice."
    status: pending
isProject: false
---

# tdmem-1401: Define the alpha support matrix and product defaults

## Execution Notes

Beads issue: `tdmem-1401`. Current Beads status at generation: `open`.

State supported OS/architecture, coding-agent hosts, provider modes, features, and known exclusions.

Design authority:

Support only combinations exercised by release journeys. Native is default; NCM observer/active status follows gates.

Acceptance authority:

- [ ] Matrix names exact supported versions.
- [ ] Unsupported combinations fail or warn explicitly.
- [ ] Games, OntOS, and general-purpose standalone use are out of scope.

## Constraints

- Beads is the live source of truth. Re-read `br show tdmem-1401` and require `br ready` before a new claim.
- Unsatisfied blocking Beads dependencies at generation: tdmem-1002, tdmem-1003.
- Beads parent/hierarchy references: tdmem-1400. Parent readiness is enforced by `br`, not fabricated as a plan dependency.
- Build semantic producers/consumers and focused evidence before bookkeeping. Do not create hash-, receipt-, lock-, or presence-only gates.
- Root alone owns heavy Cargo execution, integration acceptance, commits, pushes, and Beads closure.
- Workers must stay inside their assigned write scope, preserve concurrent edits, and stop rather than widen scope.
- Do not contact upstream maintainers or mutate external work items.

## Operator Guidance

Intersect `plan-graph frontier` with live `br ready`; launch only Beads-ready or already-claimed nodes. Assign one coherent file/module owner, run focused checks, review the exact diff locally, then publish the green slice immediately. Close the Bead only after every acceptance item and real journey required by the issue are evidenced.
