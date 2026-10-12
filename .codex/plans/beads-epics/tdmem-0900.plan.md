---
name: tdmem-0900
overview: "Measure whether a provider helps coding agents instead of merely retrieving plausible text. Compare no-memory, explicit documentation, Native memory, NCM, and later OCEAN on the same recorded scenarios."
todos:
  - id: tdmem-0900-deliver
    content: "Deliver Bead tdmem-0900: M8 \u2014 Build differential evaluation and regression gates; satisfy every recorded acceptance criterion, run the smallest behavioral dependency cone, then commit and push the green slice."
    status: pending
isProject: false
---

# tdmem-0900: M8 — Build differential evaluation and regression gates

## Execution Notes

Beads issue: `tdmem-0900`. Current Beads status at generation: `open`.

Measure whether a provider helps coding agents instead of merely retrieving plausible text. Compare no-memory,
explicit documentation, Native memory, NCM, and later OCEAN on the same recorded scenarios.

Design authority:

Keep the host, task, evidence, and context compiler fixed while varying providers. Observer outputs are logged
but excluded from product decisions. Metrics emphasize useful recall, harmful stale recall, correction latency,
repeated discovery, context cost, scope leakage, and task outcome.

Acceptance authority:

- [ ] A deterministic scenario corpus covers stale knowledge, failed approaches, cross-agent reuse, project scope, contradiction, restart, and cancellation.
- [ ] Observer isolation is mechanically proven.
- [ ] Reports are reproducible from versioned fixtures and carry provider/build/config identities.
- [ ] CI blocks regressions on safety-critical thresholds.

## Constraints

- Beads is the live source of truth. Re-read `br show tdmem-0900` and require `br ready` before a new claim.
- Unsatisfied blocking Beads dependencies at generation: tdmem-0406, tdmem-0609.
- Beads parent/hierarchy references: tdmem-0000. Parent readiness is enforced by `br`, not fabricated as a plan dependency.
- Build semantic producers/consumers and focused evidence before bookkeeping. Do not create hash-, receipt-, lock-, or presence-only gates.
- Root alone owns heavy Cargo execution, integration acceptance, commits, pushes, and Beads closure.
- Workers must stay inside their assigned write scope, preserve concurrent edits, and stop rather than widen scope.
- Do not contact upstream maintainers or mutate external work items.

## Operator Guidance

Intersect `plan-graph frontier` with live `br ready`; launch only Beads-ready or already-claimed nodes. Assign one coherent file/module owner, run focused checks, review the exact diff locally, then publish the green slice immediately. Close the Bead only after every acceptance item and real journey required by the issue are evidenced.
