---
name: tdmem-0702
overview: "Choose in-process crate, isolated local process, or another bounded topology for the first TraceDecay integration."
todos:
  - id: tdmem-0702-deliver
    content: "Deliver Bead tdmem-0702: Select and document the first NCM integration topology; satisfy every recorded acceptance criterion, run the smallest behavioral dependency cone, then commit and push the green slice."
    status: pending
isProject: false
---

# tdmem-0702: Select and document the first NCM integration topology

## Execution Notes

Beads issue: `tdmem-0702`. Current Beads status at generation: `open`.

Choose in-process crate, isolated local process, or another bounded topology for the first TraceDecay integration.

Design authority:

Evaluate crash isolation, source protection, latency, cancellation, state ownership, monorepo ergonomics, and future standalone reuse. The decision is ADR-backed.

Acceptance authority:

- [ ] The selected topology satisfies provider lifecycle and persistence contracts.
- [ ] Rejected alternatives and migration path are documented.
- [ ] No host DB access is granted to NCM.

## Constraints

- Beads is the live source of truth. Re-read `br show tdmem-0702` and require `br ready` before a new claim.
- Unsatisfied blocking Beads dependencies at generation: tdmem-0504, tdmem-0701.
- Beads parent/hierarchy references: tdmem-0700. Parent readiness is enforced by `br`, not fabricated as a plan dependency.
- Build semantic producers/consumers and focused evidence before bookkeeping. Do not create hash-, receipt-, lock-, or presence-only gates.
- Root alone owns heavy Cargo execution, integration acceptance, commits, pushes, and Beads closure.
- Workers must stay inside their assigned write scope, preserve concurrent edits, and stop rather than widen scope.
- Do not contact upstream maintainers or mutate external work items.

## Operator Guidance

Intersect `plan-graph frontier` with live `br ready`; launch only Beads-ready or already-claimed nodes. Assign one coherent file/module owner, run focused checks, review the exact diff locally, then publish the green slice immediately. Close the Bead only after every acceptance item and real journey required by the issue are evidenced.
