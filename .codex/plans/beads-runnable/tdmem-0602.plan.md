---
name: tdmem-0602
overview: "Convert provider outputs into a common candidate space suitable for host policy."
todos:
  - id: tdmem-0602-deliver
    content: "Deliver Bead tdmem-0602: Normalize provider recall candidates without erasing native semantics; satisfy every recorded acceptance criterion, run the smallest behavioral dependency cone, then commit and push the green slice."
    status: pending
isProject: false
---

# tdmem-0602: Normalize provider recall candidates without erasing native semantics

## Execution Notes

Beads issue: `tdmem-0602`. Current Beads status at generation: `open`.

Convert provider outputs into a common candidate space suitable for host policy.

Design authority:

Retain provider/native score and explanation while calculating a separately labeled normalized relevance. Never compare raw scores across providers.

Acceptance authority:

- [ ] Normalization is deterministic for fixed config.
- [ ] Native score remains visible.
- [ ] Missing confidence or stable ID is supported.
- [ ] NaN, infinity, and malformed scores are rejected.

## Constraints

- Beads is the live source of truth. Re-read `br show tdmem-0602` and require `br ready` before a new claim.
- Unsatisfied blocking Beads dependencies at generation: tdmem-0601.
- Beads parent/hierarchy references: tdmem-0600. Parent readiness is enforced by `br`, not fabricated as a plan dependency.
- Build semantic producers/consumers and focused evidence before bookkeeping. Do not create hash-, receipt-, lock-, or presence-only gates.
- Root alone owns heavy Cargo execution, integration acceptance, commits, pushes, and Beads closure.
- Workers must stay inside their assigned write scope, preserve concurrent edits, and stop rather than widen scope.
- Do not contact upstream maintainers or mutate external work items.

## Operator Guidance

Intersect `plan-graph frontier` with live `br ready`; launch only Beads-ready or already-claimed nodes. Assign one coherent file/module owner, run focused checks, review the exact diff locally, then publish the green slice immediately. Close the Bead only after every acceptance item and real journey required by the issue are evidenced.
