---
name: tdmem-1004
overview: "Run concurrent coding agents in one repository without confusing sessions, outcomes, or provider namespaces."
todos:
  - id: tdmem-1004-deliver
    content: "Deliver Bead tdmem-1004: Prove multi-agent same-repository isolation and coordination; satisfy every recorded acceptance criterion, run the smallest behavioral dependency cone, then commit and push the green slice."
    status: pending
isProject: false
---

# tdmem-1004: Prove multi-agent same-repository isolation and coordination

## Execution Notes

Beads issue: `tdmem-1004`. Current Beads status at generation: `open`.

Run concurrent coding agents in one repository without confusing sessions, outcomes, or provider namespaces.

Design authority:

Share project memory where policy allows while preserving agent/session identity and avoiding outcome cross-attribution.

Acceptance authority:

- [ ] No session/outcome is attributed to the wrong agent.
- [ ] Shared candidates retain source agent/session provenance.
- [ ] Concurrent provider requests remain bounded and deterministic where specified.

## Constraints

- Beads is the live source of truth. Re-read `br show tdmem-1004` and require `br ready` before a new claim.
- Unsatisfied blocking Beads dependencies at generation: tdmem-1001, tdmem-1002.
- Beads parent/hierarchy references: tdmem-1000. Parent readiness is enforced by `br`, not fabricated as a plan dependency.
- Build semantic producers/consumers and focused evidence before bookkeeping. Do not create hash-, receipt-, lock-, or presence-only gates.
- Root alone owns heavy Cargo execution, integration acceptance, commits, pushes, and Beads closure.
- Workers must stay inside their assigned write scope, preserve concurrent edits, and stop rather than widen scope.
- Do not contact upstream maintainers or mutate external work items.

## Operator Guidance

Intersect `plan-graph frontier` with live `br ready`; launch only Beads-ready or already-claimed nodes. Assign one coherent file/module owner, run focused checks, review the exact diff locally, then publish the green slice immediately. Close the Bead only after every acceptance item and real journey required by the issue are evidenced.
