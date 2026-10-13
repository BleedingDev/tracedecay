---
name: tdmem-0806
overview: "Detect conflicting explicit lessons or provider-derived candidates and propose keep, supersede, scope-split, or merge actions."
todos:
  - id: tdmem-0806-deliver
    content: "Deliver Bead tdmem-0806: Implement contradiction and supersession proposal flow; satisfy every recorded acceptance criterion, run the smallest behavioral dependency cone, then commit and push the green slice."
    status: pending
isProject: false
---

# tdmem-0806: Implement contradiction and supersession proposal flow

## Execution Notes

Beads issue: `tdmem-0806`. Current Beads status at generation: `open`.

Detect conflicting explicit lessons or provider-derived candidates and propose keep, supersede, scope-split, or merge actions.

Design authority:

Detection never silently rewrites canonical memory. Resolution is pending until validated and applied through the canonical curation path.

Acceptance authority:

- [ ] Contradictions retain both sides and evidence.
- [ ] Resolution type is explicit.
- [ ] No auto-apply occurs for ambiguous conflicts.
- [ ] Stale replaced lessons stop entering context after accepted supersession.

## Constraints

- Beads is the live source of truth. Re-read `br show tdmem-0806` and require `br ready` before a new claim.
- Unsatisfied blocking Beads dependencies at generation: tdmem-0803, tdmem-0804.
- Beads parent/hierarchy references: tdmem-0800. Parent readiness is enforced by `br`, not fabricated as a plan dependency.
- Build semantic producers/consumers and focused evidence before bookkeeping. Do not create hash-, receipt-, lock-, or presence-only gates.
- Root alone owns heavy Cargo execution, integration acceptance, commits, pushes, and Beads closure.
- Workers must stay inside their assigned write scope, preserve concurrent edits, and stop rather than widen scope.
- Do not contact upstream maintainers or mutate external work items.

## Operator Guidance

Intersect `plan-graph frontier` with live `br ready`; launch only Beads-ready or already-claimed nodes. Assign one coherent file/module owner, run focused checks, review the exact diff locally, then publish the green slice immediately. Close the Bead only after every acceptance item and real journey required by the issue are evidenced.
