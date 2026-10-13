---
name: tdmem-0400
overview: "Wrap the existing TraceDecay project/profile fact memory behind the new provider boundary without changing its behavior, schema authority, or current callers."
todos:
  - id: tdmem-0400-deliver
    content: "Deliver Bead tdmem-0400: M3 \u2014 Adapt TraceDecay Native memory and prove parity; satisfy every recorded acceptance criterion, run the smallest behavioral dependency cone, then commit and push the green slice."
    status: pending
isProject: false
---

# tdmem-0400: M3 — Adapt TraceDecay Native memory and prove parity

## Execution Notes

Beads issue: `tdmem-0400`. Current Beads status at generation: `open`.

Wrap the existing TraceDecay project/profile fact memory behind the new provider boundary without changing
its behavior, schema authority, or current callers.

Design authority:

The Native provider remains the open, deterministic baseline and canonical authority for explicit durable
facts. The adapter maps provider requests to existing use cases instead of duplicating storage or retrieval.

Acceptance authority:

- [ ] Direct Native calls and provider-routed Native calls produce equivalent results for pinned fixtures.
- [ ] Disabling the memory fabric preserves existing TraceDecay behavior.
- [ ] Explainability, provenance, trust, temporal state, and typed failures survive the adapter.
- [ ] A real coding-agent product journey passes through the provider boundary.

## Constraints

- Beads is the live source of truth. Re-read `br show tdmem-0400` and require `br ready` before a new claim.
- Unsatisfied blocking Beads dependencies at generation: none.
- Beads parent/hierarchy references: tdmem-0000. Parent readiness is enforced by `br`, not fabricated as a plan dependency.
- Build semantic producers/consumers and focused evidence before bookkeeping. Do not create hash-, receipt-, lock-, or presence-only gates.
- Root alone owns heavy Cargo execution, integration acceptance, commits, pushes, and Beads closure.
- Workers must stay inside their assigned write scope, preserve concurrent edits, and stop rather than widen scope.
- Do not contact upstream maintainers or mutate external work items.

## Operator Guidance

Intersect `plan-graph frontier` with live `br ready`; launch only Beads-ready or already-claimed nodes. Assign one coherent file/module owner, run focused checks, review the exact diff locally, then publish the green slice immediately. Close the Bead only after every acceptance item and real journey required by the issue are evidenced.
