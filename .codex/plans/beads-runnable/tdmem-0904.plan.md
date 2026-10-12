---
name: tdmem-0904
overview: "Measure provider usefulness and harm in coding work."
todos:
  - id: tdmem-0904-deliver
    content: "Deliver Bead tdmem-0904: Implement memory-quality, safety, cost, and latency metrics; satisfy every recorded acceptance criterion, run the smallest behavioral dependency cone, then commit and push the green slice."
    status: pending
isProject: false
---

# tdmem-0904: Implement memory-quality, safety, cost, and latency metrics

## Execution Notes

Beads issue: `tdmem-0904`. Current Beads status at generation: `open`.

Measure provider usefulness and harm in coding work.

Design authority:

Metrics include task outcome, useful recall precision, harmful stale recall, correction latency, repeated discovery, context tokens, p50/p95 recall, human curation time, provenance completeness, and scope leakage.

Acceptance authority:

- [ ] Metric definitions and denominators are versioned.
- [ ] Safety metrics cannot be hidden by aggregate task score.
- [ ] Missing/indeterminate labels are represented honestly.

## Constraints

- Beads is the live source of truth. Re-read `br show tdmem-0904` and require `br ready` before a new claim.
- Unsatisfied blocking Beads dependencies at generation: tdmem-0901.
- Beads parent/hierarchy references: tdmem-0900. Parent readiness is enforced by `br`, not fabricated as a plan dependency.
- Build semantic producers/consumers and focused evidence before bookkeeping. Do not create hash-, receipt-, lock-, or presence-only gates.
- Root alone owns heavy Cargo execution, integration acceptance, commits, pushes, and Beads closure.
- Workers must stay inside their assigned write scope, preserve concurrent edits, and stop rather than widen scope.
- Do not contact upstream maintainers or mutate external work items.

## Operator Guidance

Intersect `plan-graph frontier` with live `br ready`; launch only Beads-ready or already-claimed nodes. Assign one coherent file/module owner, run focused checks, review the exact diff locally, then publish the green slice immediately. Close the Bead only after every acceptance item and real journey required by the issue are evidenced.
