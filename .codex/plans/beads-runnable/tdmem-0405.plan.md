---
name: tdmem-0405
overview: "Make Native behavior inspectable through the common provider status and explain surfaces."
todos:
  - id: tdmem-0405-deliver
    content: "Deliver Bead tdmem-0405: Expose Native provider capability and explanation metadata; satisfy every recorded acceptance criterion, run the smallest behavioral dependency cone, then commit and push the green slice."
    status: pending
isProject: false
---

# tdmem-0405: Expose Native provider capability and explanation metadata

## Execution Notes

Beads issue: `tdmem-0405`. Current Beads status at generation: `open`.

Make Native behavior inspectable through the common provider status and explain surfaces.

Design authority:

Report only real capabilities and readiness. Preserve native score components and evidence rather than fabricating cognitive explanations.

Acceptance authority:

- [ ] Capability report matches implemented operations.
- [ ] Unavailable projections are typed as unavailable.
- [ ] Explain output links to exact Native provenance and outcome history.

## Constraints

- Beads is the live source of truth. Re-read `br show tdmem-0405` and require `br ready` before a new claim.
- Unsatisfied blocking Beads dependencies at generation: tdmem-0402.
- Beads parent/hierarchy references: tdmem-0400. Parent readiness is enforced by `br`, not fabricated as a plan dependency.
- Build semantic producers/consumers and focused evidence before bookkeeping. Do not create hash-, receipt-, lock-, or presence-only gates.
- Root alone owns heavy Cargo execution, integration acceptance, commits, pushes, and Beads closure.
- Workers must stay inside their assigned write scope, preserve concurrent edits, and stop rather than widen scope.
- Do not contact upstream maintainers or mutate external work items.

## Operator Guidance

Intersect `plan-graph frontier` with live `br ready`; launch only Beads-ready or already-claimed nodes. Assign one coherent file/module owner, run focused checks, review the exact diff locally, then publish the green slice immediately. Close the Bead only after every acceptance item and real journey required by the issue are evidenced.
