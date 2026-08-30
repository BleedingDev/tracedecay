# NCM Provider Boundary

This product-owned crate is the topology-neutral boundary for licensed NCM/Biomem integration. It does **not** contain NCM, copy NCM behavior, choose a process model, open state, or claim a usable provider.

The boundary:

- accepts only a surface declaring the reserved `ncm` provider identity;
- derives a stable SHA-256 namespace from the complete exact TraceDecay coding scope;
- derives namespace-bound opaque surface identifiers instead of forwarding caller request, operation, or idempotency identifiers;
- exposes only that opaque namespace—not profile, project, repository, worktree, branch, agent-session, or caller identifiers—to the licensed surface;
- accepts only the canonical contract ID for each operation and projects JSON-object payloads by removing every `exact_scope_identity` subtree before dispatch;
- retains opaque optional extensions inside the adapter and reattaches them only after a valid surface response;
- invalidates every prior readiness epoch before attempting a replacement handshake and keeps replacement handshakes linearizable with in-flight dispatch;
- enforces negotiated nonblocking concurrency admission, operation budgets, per-field limits, and complete canonical request/response envelope limits;
- preserves expected generation, required capabilities, absolute deadlines, live cancellation, and readiness identity;
- reattaches the original TraceDecay scope only after a compatible surface response;
- rejects wrong identities, undeclared capabilities, malformed scope/operation terminals, fake ready responses, and identity-bearing terminal metadata before product use;
- preserves complete structured committed-effect and fallback evidence when a response is valid;
- converts malformed post-dispatch mutation replies into `effect_unknown` rather than pretending no provider effect occurred, with an adapter-issued reconciliation receipt bound to the full validated public call, exact opaque dispatch, and observed malformed reply (including any authentic surface receipt). That receipt proves the uncertain interaction, not a provider commit, and names the receipt-keyed adapter reconciliation procedure.

The crate depends only on `tracedecay-memory-provider-api`, `serde_json`, and `sha2`. It has no TraceDecay store, database, code-index, daemon, host, dashboard, Native adapter, OCEAN, socket, process, or NCM implementation dependency. The payload projection is a conservative boundary, not a claim that Biomem implements TraceDecay's canonical contracts. The licensed surface audit and execution-topology decision remain owned by `tdmem-0701` and `tdmem-0702`.
