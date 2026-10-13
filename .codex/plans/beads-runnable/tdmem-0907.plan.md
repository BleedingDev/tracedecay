---
name: tdmem-0907
overview: "Persist reports that can be compared across provider and upstream versions."
todos:
  - id: tdmem-0907-deliver
    content: "Deliver Bead tdmem-0907: Produce reproducible signed evaluation reports and artifacts; satisfy every recorded acceptance criterion, run the smallest behavioral dependency cone, then commit and push the green slice."
    status: pending
isProject: false
---

# tdmem-0907: Produce reproducible signed evaluation reports and artifacts

## Execution Notes

Beads issue: `tdmem-0907`. Current Beads status at generation: `open`.

Persist reports that can be compared across provider and upstream versions.

Design authority:

Bind report to source tree, upstream floor, provider build/state schema, scenario corpus digest, host config, and metric version.

Acceptance authority:

- [ ] Re-running fixed inputs produces the same deterministic portions.
- [ ] Nondeterministic timing fields are separated.
- [ ] Artifacts include raw traces and summarized metrics without secrets.

## Constraints

- Beads is the live source of truth. Re-read `br show tdmem-0907` and require `br ready` before a new claim.
- Unsatisfied blocking Beads dependencies at generation: tdmem-0905, tdmem-0906.
- Beads parent/hierarchy references: tdmem-0900. Parent readiness is enforced by `br`, not fabricated as a plan dependency.
- Build semantic producers/consumers and focused evidence before bookkeeping. Do not create hash-, receipt-, lock-, or presence-only gates.
- Root alone owns heavy Cargo execution, integration acceptance, commits, pushes, and Beads closure.
- Workers must stay inside their assigned write scope, preserve concurrent edits, and stop rather than widen scope.
- Do not contact upstream maintainers or mutate external work items.

## Operator Guidance

Intersect `plan-graph frontier` with live `br ready`; launch only Beads-ready or already-claimed nodes. Assign one coherent file/module owner, run focused checks, review the exact diff locally, then publish the green slice immediately. Close the Bead only after every acceptance item and real journey required by the issue are evidenced.
