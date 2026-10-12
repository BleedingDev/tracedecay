---
name: pm-host-recall
overview: Wire either compatible provider through the actual host recall and feedback path with shared authority, provenance, budgets and lifecycle control.
todos:
  - id: shared-admission-provenance-pack
    content: Map shared temporal/exclusion inputs and carry both providers through the same source/time/scope admission, hydration, normalization and context selection, preserving typed common-fact identity and time-aware duplicate attribution.
    status: pending
  - id: provider-feedback-and-lifecycle-routing
    content: Route attributed feedback and required lifecycle operations to the producing provider with stable identity and no canonical fact mutation or silent provider fallback.
    status: pending
  - id: active-recall-cancellation-recovery
    content: Prove live cancellation and deadlines across provider execution, source lookup and trace persistence, with bounded shutdown/quarantine and truthful unavailable or partial outcomes.
    status: pending
isProject: false
---

# pm-host-recall

## Execution Notes

Read the brief, contracts and completed producer lanes. Reuse the existing cognitive recall port and host provenance authority. The requesting session and original source remain separate identities throughout admission and explanation. Source validation cannot be skipped for one provider, and provider-local attestation cannot be promoted to canonical evidence. Read-only recall cannot update NCM learning state or Native trust/telemetry.

Canonical project facts already enter through `graph.rs::handle_context` and `graph/context_support.rs::context_memory_matches`. Preserve that independent lane, its controls and equal inputs. Carry canonical owner/fact/revision identity through `ContextMemoryOutcome` into assembly; common fact duplicates contribute once and receive no provider usefulness credit. The matched advisory profile excludes provider-returned canonical fact projections. For a non-current host request, this owner propagates the requested time to the common fact contribution and withholds current-only projections with explicit coverage when target-time evidence is unavailable; no current fact may masquerade as a historical answer. Existing legacy explicit fact projection stays outside the matched profile.

Use the history lane's durable provider-source fence API before dispatching deletion, retain pending effects while a provider is unavailable, and distinguish host intent from verified erasure. Canonical disposition changes use their existing authority; provider-local deletion never changes canonical facts or transcripts. Old feedback targets resolve to the original provider state, including after selection changes. Recovery of pending NCM privacy work belongs to explicit readiness/lifecycle transition, before a read-only recall or health operation is admitted.

## Constraints

Single host integration owner for root `project_composition.rs`, `invocation_state.rs`, `retained_owner/cognitive_recall.rs`, narrowly needed MCP construction/context plumbing and registry recall port/admission/provenance/packing modules. These hotspot files are serialized after predecessor lanes. Do not change provider internals, shared contracts, history implementation or benchmark labels; return producer defects to their owners.

Explicit additional ownership: typed canonical-fact projection in `mcp/tools/handlers/graph.rs` and `graph/context_support.rs`; public provider-control dispatch in `application_surface/retained.rs`; a focused `retained_owner/provider_control.rs`; and their direct tests. Operations may expose the finished controls later but does not edit these dispatch/authority modules.

## Operator Guidance

Dependencies: `pm-native`, `pm-ncm`, `pm-registration`, `pm-history`, `pm-host-mount`. The neutral mount slice is independently executable after registration and remains with the same serialized host owner. Root dictates exact slices and reviews each seam before the next. First prove one real nonempty NCM context answer; then complete attributed feedback and adversarial paths. No approval gate for routine authorized integration. Do not widen deadlines, retry unknown mutations or hide NCM failure with Native output.
