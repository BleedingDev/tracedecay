---
name: tdmem-0402
overview: "Expose existing Native capabilities through provider-neutral calls."
todos:
  - id: tdmem-0402-deliver
    content: "Deliver Bead tdmem-0402: Implement Native recall, feedback, maintenance, and inspection mappings; satisfy every recorded acceptance criterion, run the smallest behavioral dependency cone, then commit and push the green slice."
    status: pending
isProject: false
---

# tdmem-0402: Implement Native recall, feedback, maintenance, and inspection mappings

## Execution Notes

Beads issue: `tdmem-0402`. Current Beads status at generation: `open`.

Expose existing Native capabilities through provider-neutral calls.

Design authority:

Retain Native retrieval/scoring and curation implementations. Map results to normalized provider candidates while retaining native score breakdown and provenance.

Acceptance authority:

- [ ] Recall preserves deterministic ordering and native explain data.
- [ ] Feedback maps to existing outcome semantics.
- [ ] Maintenance/correction/forgetting are capability-gated.
- [ ] Inspection does not expose secrets.

## Constraints

- Beads is the live source of truth. Re-read `br show tdmem-0402` and require `br ready` before a new claim.
- Unsatisfied blocking Beads dependencies at generation: tdmem-0401.
- Beads parent/hierarchy references: tdmem-0400. Parent readiness is enforced by `br`, not fabricated as a plan dependency.
- Build semantic producers/consumers and focused evidence before bookkeeping. Do not create hash-, receipt-, lock-, or presence-only gates.
- Root alone owns heavy Cargo execution, integration acceptance, commits, pushes, and Beads closure.
- Workers must stay inside their assigned write scope, preserve concurrent edits, and stop rather than widen scope.
- Do not contact upstream maintainers or mutate external work items.

## Operator Guidance

Intersect `plan-graph frontier` with live `br ready`; launch only Beads-ready or already-claimed nodes. Assign one coherent file/module owner, run focused checks, review the exact diff locally, then publish the green slice immediately. Close the Bead only after every acceptance item and real journey required by the issue are evidenced.
