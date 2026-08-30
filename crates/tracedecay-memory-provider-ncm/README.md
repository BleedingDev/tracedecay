# NCM Provider Boundary

This product-owned crate is the topology-neutral boundary for licensed NCM/Biomem integration. It does **not** contain NCM, copy NCM behavior, choose a process model, open state, or claim a usable provider.

The boundary:

- accepts only a surface declaring the reserved `ncm` provider identity;
- derives a stable SHA-256 namespace from the complete exact TraceDecay coding scope;
- exposes only that opaque namespace—not profile, project, repository, worktree, branch, or agent-session identifiers—to the licensed surface;
- preserves canonical payload bytes, idempotency, expected generation, required capabilities, deadlines, cancellation, and readiness identity;
- reattaches the original TraceDecay scope only after a compatible surface response;
- rejects wrong identities, undeclared capabilities, malformed scope/operation terminals, and fake ready responses before product use;
- converts malformed post-effect replies into `effect_unknown` rather than pretending no provider effect occurred.

The crate depends only on `tracedecay-memory-provider-api` and `sha2`. It has no TraceDecay store, database, code-index, daemon, host, dashboard, Native adapter, OCEAN, socket, process, or NCM implementation dependency. The licensed surface audit and execution-topology decision remain owned by `tdmem-0701` and `tdmem-0702`.
