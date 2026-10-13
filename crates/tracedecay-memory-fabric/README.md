# tracedecay-memory-fabric

Capability-driven, bounded orchestration over `tracedecay-memory-provider-api`.

The fabric registers concrete providers behind stable identities, checks exact registration revisions and declared capabilities, performs cancellation/deadline preflight, enforces finite registration and concurrent-call budgets, routes active calls, and returns structurally isolated observer receipts with no payload or extension channel into final context.

The crate has no provider implementation, provider-name conditional, persistence, TraceDecay DB/code-index/daemon/dashboard/host dependency, background worker, or queue. Native and NCM remain adapters outside this crate. Fallback is provider-neutral and fail-closed by default: `FallbackRule::Forbidden` keeps the original terminal, while `FallbackRule::ExplicitPinned` can admit one fresh target route only when the provider supplies the identical policy. The result retains the answering provider and the source failure's diagnostic when a pinned fallback is dispatched.
