---
name: tdmem-0500
overview: "Deliver committed TraceDecay events to memory providers safely. The system must not remember rolled-back actions, lose committed observations, duplicate effects after retries, or stall the coding-agent loop."
todos:
  - id: tdmem-0500-deliver
    content: "Deliver Bead tdmem-0500: M4 \u2014 Build observation dispatch, provider lifecycle, and recovery; satisfy every recorded acceptance criterion, run the smallest behavioral dependency cone, then commit and push the green slice."
    status: pending
isProject: false
---

# tdmem-0500: M4 — Build observation dispatch, provider lifecycle, and recovery

## Execution Notes

Beads issue: `tdmem-0500`. Current Beads status at generation: `open`.

Deliver committed TraceDecay events to memory providers safely. The system must not remember rolled-back
actions, lose committed observations, duplicate effects after retries, or stall the coding-agent loop.

Design authority:

Emit provider observations only after the host transaction commits. Use an outbox/journal, idempotency keys,
bounded queues, deadlines, retry policy, provider supervision, and explicit degraded states. Preserve exact
repository/worktree/session identity.

Acceptance authority:

- [ ] Committed observations survive host/provider crashes and are replayed without duplicate effects.
- [ ] Rolled-back host actions never reach a provider.
- [ ] Backpressure and provider unavailability are bounded and observable.
- [ ] Secret-like and transient data are rejected, redacted, or quarantined before dispatch according to policy.

## Constraints

- Beads is the live source of truth. Re-read `br show tdmem-0500` and require `br ready` before a new claim.
- Unsatisfied blocking Beads dependencies at generation: tdmem-0406.
- Beads parent/hierarchy references: tdmem-0000. Parent readiness is enforced by `br`, not fabricated as a plan dependency.
- Build semantic producers/consumers and focused evidence before bookkeeping. Do not create hash-, receipt-, lock-, or presence-only gates.
- Root alone owns heavy Cargo execution, integration acceptance, commits, pushes, and Beads closure.
- Workers must stay inside their assigned write scope, preserve concurrent edits, and stop rather than widen scope.
- Do not contact upstream maintainers or mutate external work items.

## Operator Guidance

Intersect `plan-graph frontier` with live `br ready`; launch only Beads-ready or already-claimed nodes. Assign one coherent file/module owner, run focused checks, review the exact diff locally, then publish the green slice immediately. Close the Bead only after every acceptance item and real journey required by the issue are evidenced.
