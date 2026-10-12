---
name: tdmem-0705
overview: "Translate recall requests into NCM retrieval and map latent results to provider candidates."
todos:
  - id: tdmem-0705-deliver
    content: "Deliver Bead tdmem-0705: Implement NCM recall and score/explanation mapping; satisfy every recorded acceptance criterion, run the smallest behavioral dependency cone, then commit and push the green slice."
    status: pending
isProject: false
---

# tdmem-0705: Implement NCM recall and score/explanation mapping

## Execution Notes

Beads issue: `tdmem-0705`. Current Beads status at generation: `open`.

Translate recall requests into NCM retrieval and map latent results to provider candidates.

Design authority:

Preserve NCM-native activation/strength semantics separately from normalized host relevance. Stable IDs and provenance are optional but explicit.

Acceptance authority:

- [ ] Recall obeys scope, budget, and deadline.
- [ ] NaN/malformed results fail safely.
- [ ] Native activation data is retained.
- [ ] Provenance-unavailable state is honest.

## Constraints

- Beads is the live source of truth. Re-read `br show tdmem-0705` and require `br ready` before a new claim.
- Unsatisfied blocking Beads dependencies at generation: tdmem-0602, tdmem-0703.
- Beads parent/hierarchy references: tdmem-0700. Parent readiness is enforced by `br`, not fabricated as a plan dependency.
- Build semantic producers/consumers and focused evidence before bookkeeping. Do not create hash-, receipt-, lock-, or presence-only gates.
- Root alone owns heavy Cargo execution, integration acceptance, commits, pushes, and Beads closure.
- Workers must stay inside their assigned write scope, preserve concurrent edits, and stop rather than widen scope.
- Do not contact upstream maintainers or mutate external work items.

## Operator Guidance

Intersect `plan-graph frontier` with live `br ready`; launch only Beads-ready or already-claimed nodes. Assign one coherent file/module owner, run focused checks, review the exact diff locally, then publish the green slice immediately. Close the Bead only after every acceptance item and real journey required by the issue are evidenced.
