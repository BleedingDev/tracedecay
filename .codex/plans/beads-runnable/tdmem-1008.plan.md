---
name: tdmem-1008
overview: "Create a valid lesson, change the project so it becomes wrong, then verify correction/forgetting."
todos:
  - id: tdmem-1008-deliver
    content: "Deliver Bead tdmem-1008: Prove stale-memory correction and non-resurrection; satisfy every recorded acceptance criterion, run the smallest behavioral dependency cone, then commit and push the green slice."
    status: pending
isProject: false
---

# tdmem-1008: Prove stale-memory correction and non-resurrection

## Execution Notes

Beads issue: `tdmem-1008`. Current Beads status at generation: `open`.

Create a valid lesson, change the project so it becomes wrong, then verify correction/forgetting.

Design authority:

The host detects current code/evidence change; provider and curation paths receive correction. The stale version must stop influencing later context and must not reappear under trivial paraphrase.

Acceptance authority:

- [ ] The stale lesson is initially reproducible.
- [ ] Correction latency is measured.
- [ ] Later context excludes or clearly warns on the stale lesson.
- [ ] Blocked resurrection test passes.
- [ ] The replacement retains provenance.

## Constraints

- Beads is the live source of truth. Re-read `br show tdmem-1008` and require `br ready` before a new claim.
- Unsatisfied blocking Beads dependencies at generation: tdmem-0805, tdmem-0806, tdmem-1005.
- Beads parent/hierarchy references: tdmem-1000. Parent readiness is enforced by `br`, not fabricated as a plan dependency.
- Build semantic producers/consumers and focused evidence before bookkeeping. Do not create hash-, receipt-, lock-, or presence-only gates.
- Root alone owns heavy Cargo execution, integration acceptance, commits, pushes, and Beads closure.
- Workers must stay inside their assigned write scope, preserve concurrent edits, and stop rather than widen scope.
- Do not contact upstream maintainers or mutate external work items.

## Operator Guidance

Intersect `plan-graph frontier` with live `br ready`; launch only Beads-ready or already-claimed nodes. Assign one coherent file/module owner, run focused checks, review the exact diff locally, then publish the green slice immediately. Close the Bead only after every acceptance item and real journey required by the issue are evidenced.
