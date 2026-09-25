---
name: pm-host-audit
overview: Locate Native-specific host assumptions and the existing authority paths needed for neutral selection, authorized history reuse and provenance.
todos:
  - id: host-seam-map
    content: Map registration through context packing and feedback, identify Native-only decisions, and specify a single-owner file map for neutral mounting and authorized cross-session reuse.
    status: completed
isProject: false
---

# pm-host-audit

## Execution Notes

Read `pm-finish-brief.md`. Start with registry `lib.rs`, `recall_port.rs`, `recall_admission.rs`, `recall_provenance_hydration.rs`, root `project_composition.rs`, `retained_owner/cognitive_recall.rs`, `observation_journey.rs` and configuration types. Establish which existing canonical event/source identity can authorize replay into a new exact session namespace. Output `.codex/plans/pm-host-evidence.md` with actual call chain, conflict hotspots, old-config behavior, public feedback seam and negative tests.

## Constraints

Read-only source lane. Own only the evidence document. No production edits, new route authority, provider-name switches in transports, weaker scope checks, operator config changes or daemon restart. No Cargo.

## Operator Guidance

Root node feeding `pm-contract`. Do not duplicate adapter internals covered by the other audit lanes. Stop with a concrete ownership/dependency map and explicit unresolved authority inputs. Confirm candidate provenance is host-resolvable or explicitly provider-attested; neither is invented from the requesting session.
