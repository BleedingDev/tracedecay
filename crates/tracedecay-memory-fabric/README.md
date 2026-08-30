# tracedecay-memory-fabric

Capability-driven, bounded orchestration over `tracedecay-memory-provider-api`.

The fabric registers concrete providers behind stable identities, checks exact registration revisions and declared capabilities, performs cancellation/deadline preflight, enforces finite registration and concurrent-call budgets, routes active calls, and returns structurally isolated observer receipts with no payload or extension channel into final context.

The crate has no provider implementation, provider-name conditional, persistence, TraceDecay DB/code-index/daemon/dashboard/host dependency, background worker, queue, or fallback policy. Native and NCM remain adapters outside this crate.
