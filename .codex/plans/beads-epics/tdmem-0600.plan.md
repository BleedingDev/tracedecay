---
name: tdmem-0600
overview: "Add cognitive recall as one input to TraceDecay's existing context and retrieval flow while keeping code truth, session evidence, and canonical Native facts distinct."
todos:
  - id: tdmem-0600-deliver
    content: "Deliver Bead tdmem-0600: M5 \u2014 Integrate provider recall into TraceDecay context assembly; satisfy every recorded acceptance criterion, run the smallest behavioral dependency cone, then commit and push the green slice."
    status: pending
isProject: false
---

# tdmem-0600: M5 — Integrate provider recall into TraceDecay context assembly

## Execution Notes

Beads issue: `tdmem-0600`. Current Beads status at generation: `open`.

Add cognitive recall as one input to TraceDecay's existing context and retrieval flow while keeping code
truth, session evidence, and canonical Native facts distinct.

Design authority:

Provider recall returns advisory candidates. A host-owned compiler validates exact scope, temporal validity,
provenance, score semantics, duplication, diversity, and token budgets before any item reaches an agent.
Fallback behavior is explicit and never silently substitutes one provider for another.

Acceptance authority:

- [ ] Recall requests carry exact project/worktree/session identity and deadlines.
- [ ] Candidates without sufficient scope or provenance are excluded or visibly degraded.
- [ ] The compiled context is bounded, deduplicated, explainable, and deterministic for fixed inputs.
- [ ] Provider failure never fabricates empty success or silently switches authority.

## Constraints

- Beads is the live source of truth. Re-read `br show tdmem-0600` and require `br ready` before a new claim.
- Unsatisfied blocking Beads dependencies at generation: tdmem-0406, tdmem-0506.
- Beads parent/hierarchy references: tdmem-0000. Parent readiness is enforced by `br`, not fabricated as a plan dependency.
- Build semantic producers/consumers and focused evidence before bookkeeping. Do not create hash-, receipt-, lock-, or presence-only gates.
- Root alone owns heavy Cargo execution, integration acceptance, commits, pushes, and Beads closure.
- Workers must stay inside their assigned write scope, preserve concurrent edits, and stop rather than widen scope.
- Do not contact upstream maintainers or mutate external work items.

## Operator Guidance

Intersect `plan-graph frontier` with live `br ready`; launch only Beads-ready or already-claimed nodes. Assign one coherent file/module owner, run focused checks, review the exact diff locally, then publish the green slice immediately. Close the Bead only after every acceptance item and real journey required by the issue are evidenced.
