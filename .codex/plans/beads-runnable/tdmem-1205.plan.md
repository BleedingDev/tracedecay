---
name: tdmem-1205
overview: "Advance upstream in reviewable trains rather than continuous rebases on every commit."
todos:
  - id: tdmem-1205-deliver
    content: "Deliver Bead tdmem-1205: Define the isolated sync-train workflow and conflict receipts; satisfy every recorded acceptance criterion, run the smallest behavioral dependency cone, then commit and push the green slice."
    status: pending
isProject: false
---

# tdmem-1205: Define the isolated sync-train workflow and conflict receipts

## Execution Notes

Beads issue: `tdmem-1205`. Current Beads status at generation: `open`.

Advance upstream in reviewable trains rather than continuous rebases on every commit.

Design authority:

Create sync branch from product floor, merge/rebase candidate upstream according to policy, classify conflicts by owner, run gates, and publish one convergence receipt.

Acceptance authority:

- [ ] The workflow never force-updates the released product branch.
- [ ] Conflicts retain source and resolution rationale.
- [ ] Aborted trains leave no partial floor update.
- [ ] Successful train updates metadata atomically with code.

## Constraints

- Beads is the live source of truth. Re-read `br show tdmem-1205` and require `br ready` before a new claim.
- Unsatisfied blocking Beads dependencies at generation: tdmem-1204.
- Beads parent/hierarchy references: tdmem-1200. Parent readiness is enforced by `br`, not fabricated as a plan dependency.
- Build semantic producers/consumers and focused evidence before bookkeeping. Do not create hash-, receipt-, lock-, or presence-only gates.
- Root alone owns heavy Cargo execution, integration acceptance, commits, pushes, and Beads closure.
- Workers must stay inside their assigned write scope, preserve concurrent edits, and stop rather than widen scope.
- Do not contact upstream maintainers or mutate external work items.

## Operator Guidance

Intersect `plan-graph frontier` with live `br ready`; launch only Beads-ready or already-claimed nodes. Assign one coherent file/module owner, run focused checks, review the exact diff locally, then publish the green slice immediately. Close the Bead only after every acceptance item and real journey required by the issue are evidenced.
