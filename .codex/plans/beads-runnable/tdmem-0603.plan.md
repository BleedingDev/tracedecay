---
name: tdmem-0603
overview: "Prevent cross-project/worktree/session leakage and stale or revoked provider content from reaching context."
todos:
  - id: tdmem-0603-deliver
    content: "Deliver Bead tdmem-0603: Enforce exact scope, identity, temporal validity, and revocation before admission; satisfy every recorded acceptance criterion, run the smallest behavioral dependency cone, then commit and push the green slice."
    status: pending
isProject: false
---

# tdmem-0603: Enforce exact scope, identity, temporal validity, and revocation before admission

## Execution Notes

Beads issue: `tdmem-0603`. Current Beads status at generation: `open`.

Prevent cross-project/worktree/session leakage and stale or revoked provider content from reaching context.

Design authority:

Host authority validates provider claims against resolved TraceDecay identities. Provider assertions cannot expand their own scope.

Acceptance authority:

- [ ] Cross-repository and cross-worktree candidates are denied.
- [ ] Stale/unknown identity is typed.
- [ ] Validity windows and revocation are enforced rank-final.
- [ ] Denied candidates remain audit-visible without entering prompts.

## Constraints

- Beads is the live source of truth. Re-read `br show tdmem-0603` and require `br ready` before a new claim.
- Unsatisfied blocking Beads dependencies at generation: tdmem-0601.
- Beads parent/hierarchy references: tdmem-0600. Parent readiness is enforced by `br`, not fabricated as a plan dependency.
- Build semantic producers/consumers and focused evidence before bookkeeping. Do not create hash-, receipt-, lock-, or presence-only gates.
- Root alone owns heavy Cargo execution, integration acceptance, commits, pushes, and Beads closure.
- Workers must stay inside their assigned write scope, preserve concurrent edits, and stop rather than widen scope.
- Do not contact upstream maintainers or mutate external work items.

## Operator Guidance

Intersect `plan-graph frontier` with live `br ready`; launch only Beads-ready or already-claimed nodes. Assign one coherent file/module owner, run focused checks, review the exact diff locally, then publish the green slice immediately. Close the Bead only after every acceptance item and real journey required by the issue are evidenced.
