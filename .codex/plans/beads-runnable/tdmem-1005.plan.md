---
name: tdmem-1005
overview: "Ensure a lesson tied to one branch/worktree revision is not blindly injected into an incompatible one."
todos:
  - id: tdmem-1005-deliver
    content: "Deliver Bead tdmem-1005: Prove branch and worktree validity boundaries; satisfy every recorded acceptance criterion, run the smallest behavioral dependency cone, then commit and push the green slice."
    status: pending
isProject: false
---

# tdmem-1005: Prove branch and worktree validity boundaries

## Execution Notes

Beads issue: `tdmem-1005`. Current Beads status at generation: `open`.

Ensure a lesson tied to one branch/worktree revision is not blindly injected into an incompatible one.

Design authority:

Use TraceDecay exact identity and code freshness. Provider cannot broaden scope. Stale code-coupled memories are flagged or denied according to policy.

Acceptance authority:

- [ ] Cross-worktree leakage is zero.
- [ ] Branch divergence produces explicit freshness/validity state.
- [ ] A globally applicable lesson can still be admitted through explicit scope.

## Constraints

- Beads is the live source of truth. Re-read `br show tdmem-1005` and require `br ready` before a new claim.
- Unsatisfied blocking Beads dependencies at generation: tdmem-0603, tdmem-1001, tdmem-1002.
- Beads parent/hierarchy references: tdmem-1000. Parent readiness is enforced by `br`, not fabricated as a plan dependency.
- Build semantic producers/consumers and focused evidence before bookkeeping. Do not create hash-, receipt-, lock-, or presence-only gates.
- Root alone owns heavy Cargo execution, integration acceptance, commits, pushes, and Beads closure.
- Workers must stay inside their assigned write scope, preserve concurrent edits, and stop rather than widen scope.
- Do not contact upstream maintainers or mutate external work items.

## Operator Guidance

Intersect `plan-graph frontier` with live `br ready`; launch only Beads-ready or already-claimed nodes. Assign one coherent file/module owner, run focused checks, review the exact diff locally, then publish the green slice immediately. Close the Bead only after every acceptance item and real journey required by the issue are evidenced.
