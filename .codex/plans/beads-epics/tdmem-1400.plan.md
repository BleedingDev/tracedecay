---
name: tdmem-1400
overview: "Produce a supportable alpha for coding-agent users without claiming broader platform readiness. Native memory is the safe baseline; NCM observer and active modes ship only at their proven maturity."
todos:
  - id: tdmem-1400-deliver
    content: "Deliver Bead tdmem-1400: M11 \u2014 Harden and release the first alpha; satisfy every recorded acceptance criterion, run the smallest behavioral dependency cone, then commit and push the green slice."
    status: pending
isProject: false
---

# tdmem-1400: M11 — Harden and release the first alpha

## Execution Notes

Beads issue: `tdmem-1400`. Current Beads status at generation: `open`.

Produce a supportable alpha for coding-agent users without claiming broader platform readiness. Native memory
is the safe baseline; NCM observer and active modes ship only at their proven maturity.

Design authority:

Define the supported host/platform matrix, upgrade behavior, configuration defaults, compatibility policy,
release artifacts, threat model, diagnostics, and post-release feedback loop. New memory behavior remains
reversible and opt-in during alpha.

Acceptance authority:

- [ ] A clean install and upgrade path works on the supported platform/host matrix.
- [ ] Default behavior is safe and preserves existing TraceDecay users.
- [ ] Release artifacts include compatibility metadata, checksums, diagnostics, and known limitations.
- [ ] Alpha telemetry and user feedback are explicit opt-in and do not expose memory content.

## Constraints

- Beads is the live source of truth. Re-read `br show tdmem-1400` and require `br ready` before a new claim.
- Unsatisfied blocking Beads dependencies at generation: tdmem-1009, tdmem-1109, tdmem-1208.
- Beads parent/hierarchy references: tdmem-0000. Parent readiness is enforced by `br`, not fabricated as a plan dependency.
- Build semantic producers/consumers and focused evidence before bookkeeping. Do not create hash-, receipt-, lock-, or presence-only gates.
- Root alone owns heavy Cargo execution, integration acceptance, commits, pushes, and Beads closure.
- Workers must stay inside their assigned write scope, preserve concurrent edits, and stop rather than widen scope.
- Do not contact upstream maintainers or mutate external work items.

## Operator Guidance

Intersect `plan-graph frontier` with live `br ready`; launch only Beads-ready or already-claimed nodes. Assign one coherent file/module owner, run focused checks, review the exact diff locally, then publish the green slice immediately. Close the Bead only after every acceptance item and real journey required by the issue are evidenced.
