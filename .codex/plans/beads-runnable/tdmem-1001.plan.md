---
name: tdmem-1001
overview: "Exercise TraceDecay's real Claude Code integration with provider recall and observation."
todos:
  - id: tdmem-1001-deliver
    content: "Deliver Bead tdmem-1001: Integrate and test Claude Code memory hooks; satisfy every recorded acceptance criterion, run the smallest behavioral dependency cone, then commit and push the green slice."
    status: pending
isProject: false
---

# tdmem-1001: Integrate and test Claude Code memory hooks

## Execution Notes

Beads issue: `tdmem-1001`. Current Beads status at generation: `open`.

Exercise TraceDecay's real Claude Code integration with provider recall and observation.

Design authority:

Use existing V2 host bundle/hooks. Avoid a separate NCM-specific hook. Test session orientation, pre-edit recall, session-end capture, and failure behavior.

Acceptance authority:

- [ ] Install/update/undo is idempotent and preserves external settings.
- [ ] Recall is bounded and de-duplicated.
- [ ] Provider failure is fail-open with typed diagnostics.
- [ ] Exact project/worktree identity is asserted.

## Constraints

- Beads is the live source of truth. Re-read `br show tdmem-1001` and require `br ready` before a new claim.
- Unsatisfied blocking Beads dependencies at generation: tdmem-0508, tdmem-0609.
- Beads parent/hierarchy references: tdmem-1000. Parent readiness is enforced by `br`, not fabricated as a plan dependency.
- Build semantic producers/consumers and focused evidence before bookkeeping. Do not create hash-, receipt-, lock-, or presence-only gates.
- Root alone owns heavy Cargo execution, integration acceptance, commits, pushes, and Beads closure.
- Workers must stay inside their assigned write scope, preserve concurrent edits, and stop rather than widen scope.
- Do not contact upstream maintainers or mutate external work items.

## Operator Guidance

Intersect `plan-graph frontier` with live `br ready`; launch only Beads-ready or already-claimed nodes. Assign one coherent file/module owner, run focused checks, review the exact diff locally, then publish the green slice immediately. Close the Bead only after every acceptance item and real journey required by the issue are evidenced.
