---
name: tdmem-0501
overview: "Classify TraceDecay events by durability, sensitivity, relevance, and whether they may leave the canonical transaction boundary."
todos:
  - id: tdmem-0501-deliver
    content: "Deliver Bead tdmem-0501: Enumerate post-commit host events eligible for provider observation; satisfy every recorded acceptance criterion, run the smallest behavioral dependency cone, then commit and push the green slice."
    status: pending
isProject: false
---

# tdmem-0501: Enumerate post-commit host events eligible for provider observation

## Execution Notes

Beads issue: `tdmem-0501`. Current Beads status at generation: `open`.

Classify TraceDecay events by durability, sensitivity, relevance, and whether they may leave the canonical transaction boundary.

Design authority:

Start narrowly with completed/committed coding outcomes. Exclude speculative tool intent, rolled-back mutations, uncommitted secrets, and ephemeral run noise by default.

Acceptance authority:

- [ ] Every admitted event has a canonical commit point.
- [ ] Every excluded event has a reason.
- [ ] Coding scope and source provenance are derivable without heuristic path parsing.

## Constraints

- Beads is the live source of truth. Re-read `br show tdmem-0501` and require `br ready` before a new claim.
- Unsatisfied blocking Beads dependencies at generation: tdmem-0406.
- Beads parent/hierarchy references: tdmem-0500. Parent readiness is enforced by `br`, not fabricated as a plan dependency.
- Build semantic producers/consumers and focused evidence before bookkeeping. Do not create hash-, receipt-, lock-, or presence-only gates.
- Root alone owns heavy Cargo execution, integration acceptance, commits, pushes, and Beads closure.
- Workers must stay inside their assigned write scope, preserve concurrent edits, and stop rather than widen scope.
- Do not contact upstream maintainers or mutate external work items.

## Operator Guidance

Intersect `plan-graph frontier` with live `br ready`; launch only Beads-ready or already-claimed nodes. Assign one coherent file/module owner, run focused checks, review the exact diff locally, then publish the green slice immediately. Close the Bead only after every acceptance item and real journey required by the issue are evidenced.
