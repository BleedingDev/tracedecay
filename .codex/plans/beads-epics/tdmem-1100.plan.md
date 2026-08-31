---
name: tdmem-1100
overview: "Let developers understand and control what providers received, recalled, strengthened, weakened, corrected, or forgot, without exposing secrets or pretending latent state is more explainable than it is."
todos:
  - id: tdmem-1100-deliver
    content: "Deliver Bead tdmem-1100: M10 \u2014 Add inspection, security, deletion, and operational visibility; satisfy every recorded acceptance criterion, run the smallest behavioral dependency cone, then commit and push the green slice."
    status: pending
isProject: false
---

# tdmem-1100: M10 — Add inspection, security, deletion, and operational visibility

## Execution Notes

Beads issue: `tdmem-1100`. Current Beads status at generation: `open`.

Let developers understand and control what providers received, recalled, strengthened, weakened, corrected,
or forgot, without exposing secrets or pretending latent state is more explainable than it is.

Design authority:

Provide one inspection shell with provider-specific capability panels. All controls generate receipts. Security
covers input sanitization, prompt-injection boundaries, provider process isolation, encrypted state, and deletion
across active state, journals, caches, and snapshots.

Acceptance authority:

- [ ] Provider health, capability, queue, latency, and degradation states are visible.
- [ ] Observation-to-recall-to-outcome traces are inspectable with provenance.
- [ ] Correction, forgetting, pinning, export, and deletion are capability-gated and audited.
- [ ] Deletion tests prove that prohibited data cannot influence later recall or survive retained snapshots.

## Constraints

- Beads is the live source of truth. Re-read `br show tdmem-1100` and require `br ready` before a new claim.
- Unsatisfied blocking Beads dependencies at generation: tdmem-0709, tdmem-0809.
- Beads parent/hierarchy references: tdmem-0000. Parent readiness is enforced by `br`, not fabricated as a plan dependency.
- Build semantic producers/consumers and focused evidence before bookkeeping. Do not create hash-, receipt-, lock-, or presence-only gates.
- Root alone owns heavy Cargo execution, integration acceptance, commits, pushes, and Beads closure.
- Workers must stay inside their assigned write scope, preserve concurrent edits, and stop rather than widen scope.
- Do not contact upstream maintainers or mutate external work items.

## Operator Guidance

Intersect `plan-graph frontier` with live `br ready`; launch only Beads-ready or already-claimed nodes. Assign one coherent file/module owner, run focused checks, review the exact diff locally, then publish the green slice immediately. Close the Bead only after every acceptance item and real journey required by the issue are evidenced.
