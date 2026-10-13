---
name: tdmem-1408
overview: "Enable opt-in NCM context influence for a controlled alpha cohort."
todos:
  - id: tdmem-1408-deliver
    content: "Deliver Bead tdmem-1408: Cut guarded active-NCM alpha only after activation gates pass; satisfy every recorded acceptance criterion, run the smallest behavioral dependency cone, then commit and push the green slice."
    status: pending
isProject: false
---

# tdmem-1408: Cut guarded active-NCM alpha only after activation gates pass

## Execution Notes

Beads issue: `tdmem-1408`. Current Beads status at generation: `open`.

Enable opt-in NCM context influence for a controlled alpha cohort.

Design authority:

Artifact checks exact conformance/evaluation gate references and offers immediate rollback to Native-only operation.

Acceptance authority:

- [ ] Activation gates pass on release build.
- [ ] Provider identity and contributions are visible.
- [ ] Stale-correction, isolation, restart, deletion, and failure journeys pass.
- [ ] Rollback is one documented operation.

## Constraints

- Beads is the live source of truth. Re-read `br show tdmem-1408` and require `br ready` before a new claim.
- Unsatisfied blocking Beads dependencies at generation: tdmem-0711, tdmem-1008, tdmem-1009, tdmem-1407.
- Beads parent/hierarchy references: tdmem-1400. Parent readiness is enforced by `br`, not fabricated as a plan dependency.
- Build semantic producers/consumers and focused evidence before bookkeeping. Do not create hash-, receipt-, lock-, or presence-only gates.
- Root alone owns heavy Cargo execution, integration acceptance, commits, pushes, and Beads closure.
- Workers must stay inside their assigned write scope, preserve concurrent edits, and stop rather than widen scope.
- Do not contact upstream maintainers or mutate external work items.

## Operator Guidance

Intersect `plan-graph frontier` with live `br ready`; launch only Beads-ready or already-claimed nodes. Assign one coherent file/module owner, run focused checks, review the exact diff locally, then publish the green slice immediately. Close the Bead only after every acceptance item and real journey required by the issue are evidenced.
