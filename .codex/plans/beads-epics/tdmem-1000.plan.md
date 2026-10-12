---
name: tdmem-1000
overview: "Validate the integrated memory layer through TraceDecay's actual coding-agent host surfaces rather than synthetic direct calls."
todos:
  - id: tdmem-1000-deliver
    content: "Deliver Bead tdmem-1000: M9 \u2014 Prove real coding-agent host journeys; satisfy every recorded acceptance criterion, run the smallest behavioral dependency cone, then commit and push the green slice."
    status: pending
isProject: false
---

# tdmem-1000: M9 — Prove real coding-agent host journeys

## Execution Notes

Beads issue: `tdmem-1000`. Current Beads status at generation: `open`.

Validate the integrated memory layer through TraceDecay's actual coding-agent host surfaces rather than
synthetic direct calls.

Design authority:

Cover the hosts and lifecycle conditions that matter first: Claude Code, Codex, Cursor, multiple agents,
branches/worktrees, sandbox restart, provider failure, useful cross-session learning, and stale-memory
correction.

Acceptance authority:

- [ ] At least Claude Code and Codex pass end-to-end observe/recall/feedback journeys.
- [ ] No context leaks between repositories, worktrees, branches, agents, or test namespaces.
- [ ] Provider restart and typed degradation preserve the agent loop.
- [ ] A stale lesson is corrected and no longer harms a later session.

## Constraints

- Beads is the live source of truth. Re-read `br show tdmem-1000` and require `br ready` before a new claim.
- Unsatisfied blocking Beads dependencies at generation: tdmem-0709, tdmem-0809.
- Beads parent/hierarchy references: tdmem-0000. Parent readiness is enforced by `br`, not fabricated as a plan dependency.
- Build semantic producers/consumers and focused evidence before bookkeeping. Do not create hash-, receipt-, lock-, or presence-only gates.
- Root alone owns heavy Cargo execution, integration acceptance, commits, pushes, and Beads closure.
- Workers must stay inside their assigned write scope, preserve concurrent edits, and stop rather than widen scope.
- Do not contact upstream maintainers or mutate external work items.

## Operator Guidance

Intersect `plan-graph frontier` with live `br ready`; launch only Beads-ready or already-claimed nodes. Assign one coherent file/module owner, run focused checks, review the exact diff locally, then publish the green slice immediately. Close the Bead only after every acceptance item and real journey required by the issue are evidenced.
