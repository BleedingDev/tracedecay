---
name: tdmem-0503
overview: "Deliver outbox observations to configured providers with deadlines, retry policy, and receipts."
todos:
  - id: tdmem-0503-deliver
    content: "Deliver Bead tdmem-0503: Implement the idempotent bounded dispatcher; satisfy every recorded acceptance criterion, run the smallest behavioral dependency cone, then commit and push the green slice."
    status: pending
isProject: false
---

# tdmem-0503: Implement the idempotent bounded dispatcher

## Execution Notes

Beads issue: `tdmem-0503`. Current Beads status at generation: `open`.

Deliver outbox observations to configured providers with deadlines, retry policy, and receipts.

Design authority:

Use bounded concurrency and queue size. Retries reuse the idempotency key. Distinguish transient unavailable, timeout, cancellation, permanent rejection, and committed partial effect.

Acceptance authority:

- [ ] Duplicate delivery does not duplicate provider effects.
- [ ] Queue and retry limits are configurable and bounded.
- [ ] Every terminal attempt is recorded.
- [ ] Cancellation stops in-flight work within the declared bound.

## Constraints

- Beads is the live source of truth. Re-read `br show tdmem-0503` and require `br ready` before a new claim.
- Unsatisfied blocking Beads dependencies at generation: tdmem-0502.
- Beads parent/hierarchy references: tdmem-0500. Parent readiness is enforced by `br`, not fabricated as a plan dependency.
- Build semantic producers/consumers and focused evidence before bookkeeping. Do not create hash-, receipt-, lock-, or presence-only gates.
- Root alone owns heavy Cargo execution, integration acceptance, commits, pushes, and Beads closure.
- Workers must stay inside their assigned write scope, preserve concurrent edits, and stop rather than widen scope.
- Do not contact upstream maintainers or mutate external work items.

## Operator Guidance

Intersect `plan-graph frontier` with live `br ready`; launch only Beads-ready or already-claimed nodes. Assign one coherent file/module owner, run focused checks, review the exact diff locally, then publish the green slice immediately. Close the Bead only after every acceptance item and real journey required by the issue are evidenced.
