---
name: tdmem-0709
overview: "Run NCM through the neutral conformance suite, including negative and recovery cases."
todos:
  - id: tdmem-0709-deliver
    content: "Deliver Bead tdmem-0709: Pass NCM mandatory and declared-capability conformance; satisfy every recorded acceptance criterion, run the smallest behavioral dependency cone, then commit and push the green slice."
    status: pending
isProject: false
---

# tdmem-0709: Pass NCM mandatory and declared-capability conformance

## Execution Notes

Beads issue: `tdmem-0709`. Current Beads status at generation: `open`.

Run NCM through the neutral conformance suite, including negative and recovery cases.

Design authority:

No NCM-specific exemptions outside documented capability semantics. Failures become provider work, not weakened common contracts.

Acceptance authority:

- [ ] Mandatory conformance passes.
- [ ] Declared optional capabilities pass their suites.
- [ ] Idempotency, cancellation, restart, corruption, deletion, and scope cases pass.
- [ ] Results record exact provider build/state/config identity.

## Constraints

- Beads is the live source of truth. Re-read `br show tdmem-0709` and require `br ready` before a new claim.
- Unsatisfied blocking Beads dependencies at generation: tdmem-0706, tdmem-0707, tdmem-0708.
- Beads parent/hierarchy references: tdmem-0700. Parent readiness is enforced by `br`, not fabricated as a plan dependency.
- Build semantic producers/consumers and focused evidence before bookkeeping. Do not create hash-, receipt-, lock-, or presence-only gates.
- Root alone owns heavy Cargo execution, integration acceptance, commits, pushes, and Beads closure.
- Workers must stay inside their assigned write scope, preserve concurrent edits, and stop rather than widen scope.
- Do not contact upstream maintainers or mutate external work items.

## Operator Guidance

Intersect `plan-graph frontier` with live `br ready`; launch only Beads-ready or already-claimed nodes. Assign one coherent file/module owner, run focused checks, review the exact diff locally, then publish the green slice immediately. Close the Bead only after every acceptance item and real journey required by the issue are evidenced.
