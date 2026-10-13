---
name: tdmem-0401
overview: "Translate provider observations into existing Native memory operations only where semantics match."
todos:
  - id: tdmem-0401-deliver
    content: "Deliver Bead tdmem-0401: Implement Native observe mapping through existing application ports; satisfy every recorded acceptance criterion, run the smallest behavioral dependency cone, then commit and push the green slice."
    status: in_progress
isProject: false
---

# tdmem-0401: Implement Native observe mapping through existing application ports

## Execution Notes

Beads issue: `tdmem-0401`. Current Beads status at generation: `in_progress`.

Translate provider observations into existing Native memory operations only where semantics match.

Design authority:

Do not auto-convert arbitrary observations into canonical facts. Explicit remember/capture paths preserve current validation and authority.

Acceptance authority:

- [ ] Mapped observations preserve owner, provenance, trust, temporal state, and receipts.
- [ ] Non-equivalent observation types are explicitly unsupported or staged as candidates.
- [ ] Existing Native write tests remain green.

## Constraints

- Beads is the live source of truth. Re-read `br show tdmem-0401` and require `br ready` before a new claim.
- Unsatisfied blocking Beads dependencies at generation: none.
- Beads parent/hierarchy references: tdmem-0400. Parent readiness is enforced by `br`, not fabricated as a plan dependency.
- Build semantic producers/consumers and focused evidence before bookkeeping. Do not create hash-, receipt-, lock-, or presence-only gates.
- Root alone owns heavy Cargo execution, integration acceptance, commits, pushes, and Beads closure.
- Workers must stay inside their assigned write scope, preserve concurrent edits, and stop rather than widen scope.
- Do not contact upstream maintainers or mutate external work items.

## Operator Guidance

Intersect `plan-graph frontier` with live `br ready`; launch only Beads-ready or already-claimed nodes. Assign one coherent file/module owner, run focused checks, review the exact diff locally, then publish the green slice immediately. Close the Bead only after every acceptance item and real journey required by the issue are evidenced.
