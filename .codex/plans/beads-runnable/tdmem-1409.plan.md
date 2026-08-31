---
name: tdmem-1409
overview: "Collect product evidence needed to decide further investment without ingesting private memory content."
todos:
  - id: tdmem-1409-deliver
    content: "Deliver Bead tdmem-1409: Establish post-alpha evidence and feedback loop; satisfy every recorded acceptance criterion, run the smallest behavioral dependency cone, then commit and push the green slice."
    status: pending
isProject: false
---

# tdmem-1409: Establish post-alpha evidence and feedback loop

## Execution Notes

Beads issue: `tdmem-1409`. Current Beads status at generation: `open`.

Collect product evidence needed to decide further investment without ingesting private memory content.

Design authority:

Track install success, provider readiness, active retention, opt-in quality labels, failures, latency, and curation burden. Separate GitHub interest from real usage.

Acceptance authority:

- [ ] Metrics are privacy-reviewed.
- [ ] Users can inspect and disable telemetry.
- [ ] A weekly evidence report drives build/iterate/pivot/kill decisions.
- [ ] No raw memory content is collected by default.

## Constraints

- Beads is the live source of truth. Re-read `br show tdmem-1409` and require `br ready` before a new claim.
- Unsatisfied blocking Beads dependencies at generation: tdmem-1108, tdmem-1406.
- Beads parent/hierarchy references: tdmem-1400. Parent readiness is enforced by `br`, not fabricated as a plan dependency.
- Build semantic producers/consumers and focused evidence before bookkeeping. Do not create hash-, receipt-, lock-, or presence-only gates.
- Root alone owns heavy Cargo execution, integration acceptance, commits, pushes, and Beads closure.
- Workers must stay inside their assigned write scope, preserve concurrent edits, and stop rather than widen scope.
- Do not contact upstream maintainers or mutate external work items.

## Operator Guidance

Intersect `plan-graph frontier` with live `br ready`; launch only Beads-ready or already-claimed nodes. Assign one coherent file/module owner, run focused checks, review the exact diff locally, then publish the green slice immediately. Close the Bead only after every acceptance item and real journey required by the issue are evidenced.
