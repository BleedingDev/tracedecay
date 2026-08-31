---
name: tdmem-0606
overview: "Resolve provider source refs to TraceDecay evidence when possible and prevent uncited synthesis from masquerading as evidence."
todos:
  - id: tdmem-0606-deliver
    content: "Deliver Bead tdmem-0606: Hydrate provenance and represent provenance-unavailable explicitly; satisfy every recorded acceptance criterion, run the smallest behavioral dependency cone, then commit and push the green slice."
    status: pending
isProject: false
---

# tdmem-0606: Hydrate provenance and represent provenance-unavailable explicitly

## Execution Notes

Beads issue: `tdmem-0606`. Current Beads status at generation: `open`.

Resolve provider source refs to TraceDecay evidence when possible and prevent uncited synthesis from masquerading as evidence.

Design authority:

Hydration happens through host authorities. A provider may explain internal activation, but only host-resolved evidence becomes cited grounding.

Acceptance authority:

- [ ] Resolvable refs point to exact source/session ranges or canonical records.
- [ ] Unresolvable provenance is visibly labeled.
- [ ] Policy can exclude provenance-unavailable candidates.
- [ ] Hydration failures are typed and bounded.

## Constraints

- Beads is the live source of truth. Re-read `br show tdmem-0606` and require `br ready` before a new claim.
- Unsatisfied blocking Beads dependencies at generation: tdmem-0603, tdmem-0605.
- Beads parent/hierarchy references: tdmem-0600. Parent readiness is enforced by `br`, not fabricated as a plan dependency.
- Build semantic producers/consumers and focused evidence before bookkeeping. Do not create hash-, receipt-, lock-, or presence-only gates.
- Root alone owns heavy Cargo execution, integration acceptance, commits, pushes, and Beads closure.
- Workers must stay inside their assigned write scope, preserve concurrent edits, and stop rather than widen scope.
- Do not contact upstream maintainers or mutate external work items.

## Operator Guidance

Intersect `plan-graph frontier` with live `br ready`; launch only Beads-ready or already-claimed nodes. Assign one coherent file/module owner, run focused checks, review the exact diff locally, then publish the green slice immediately. Close the Bead only after every acceptance item and real journey required by the issue are evidenced.
