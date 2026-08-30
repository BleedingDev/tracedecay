# Coding-memory authority matrix

Bead: `tdmem-0104`

Machine-readable authority: [`coding-memory-authority-matrix.json`](./coding-memory-authority-matrix.json). Native implementation inventory: [`native-memory-surface-map.json`](./native-memory-surface-map.json).

## Decision

TraceDecay remains the coding-context authority. It owns exact project/repository/worktree/session identity, accepted Native facts, curated configuration, and final context assembly. A cognitive provider owns only its provider-local internal state and returns bounded advisory candidates through capability ports.

No durable domain has two canonical writers. No transport, dashboard, generated projection, provider adapter, or context renderer becomes a shadow authority.

## Namespace contract

Every request and durable effect carries the dimensions its source domain actually owns:

| Axis | Authority | Purpose |
|---|---|---|
| `profile_id` | TraceDecay profile identity | User/profile configuration, profile sessions, optional profile memory |
| `project_id` | Project enrollment and registry | Stable project owner |
| `repository_id` | Registered scope admission | Repository identity independent of path aliases |
| `worktree_id` | Registered scope admission | Isolation between linked worktrees |
| `branch_ref` | Git ref for the exact worktree | Branch-sensitive reads and provider state |
| `agent_session_id` | Host/session admission | Isolation between concurrent coding sessions |
| `provider_id` | Provider capability registry | Logical provider namespace |
| `provider_instance_id` | Provider lifecycle registry | Installation/process/account/store namespace |
| `request_id` | Application admission | Immutable context request identity |
| `operation_id` | Idempotency/effect identity | Retry-safe durable observations and writes |

Missing project, repository, or worktree identity fails closed. The current working directory is never a request-time identity source. A profile session uses its own reserved scope and cannot compare equal to a real project checkout.

## Authority domains

| Domain | Class | Sole owner/writer | Read interface | Consistency and failure |
|---|---|---|---|---|
| Current code truth | Canonical | Exact admitted worktree filesystem; TraceDecay publishes only through authorized atomic source edit | Source-read, source-edit preview, generation-pinned code graph/query ports | Filesystem bytes win. Unsafe path, race, stale identity, scope mismatch, cancellation, or deadline fails closed. A stale graph is partial/unavailable, never source truth. |
| Repository identity | Canonical identity | Project enrollment authority binding registry to repository marker | `RegisteredScopeResolver`, project registry, `ResolvedScope` | Path strings are insufficient. Missing/inconsistent enrollment or marker fails closed. |
| Worktree identity | Canonical identity | Registered-scope admission over canonical roots and proved linked-worktree topology | `RegisteredScopeResolver`, `ResolvedGitRoute`, `ResolvedScope` | Linked worktrees receive distinct IDs. Unrelated sibling roots are denied. |
| Branch identity | Canonical identity | Git ref state for the exact worktree | `current_branch`, `ResolvedGitRoute`, `ResolvedScope::reference` | No fabricated branch for detached/non-Git scope. No reuse of another worktree's active branch. |
| Session evidence | Canonical, separate domain | Host admission plus session/transcript ingest | `SessionApplicationRetrievalPortV1`, LCM/session temporal reads | Idempotent ingest/replay with source cursor and exact session scope. Unavailable is typed, not successful empty history. |
| Accepted explicit facts | Canonical | TraceDecay Native `MemoryApplication` over owner-bound fact store | Fact add/get/list/search/probe/related/reason/contradict/update/remove/feedback/status | Append-only lineage, provenance, idempotency, privacy admission, durable receipts. Provider output has no implicit promotion. |
| Provider observation journal | Planned canonical product domain | TraceDecay observation dispatcher | Delivery worker, inspection, reconciliation | Bounded durable outbox, at-least-once delivery, idempotent by operation/provider/scope. Observer failure cannot affect canonical behavior. |
| Provider cognitive state | Planned canonical external domain | Selected provider instance behind its adapter | Provider observe/feedback/maintenance/recall/health/inspection capabilities | Provider-defined persistence behind versioned contract. TraceDecay never depends on provider DB schemas or co-writes internals. |
| Provider recall candidates | Advisory ephemeral | Selected provider adapter produces one request result; no canonical writer | Recall capability invoked by context compiler | Provenance, scope, revision, freshness, budget, deadline, cancellation, typed degradation. No direct mutation. |
| Curated rules | Canonical | Transactional TraceDecay configuration control plane | Pinned effective configuration and managed-skill readers | Revisioned, authorized, audited, rollback-capable. Generated files are projections, not co-writers. |
| Final compiled context | Ephemeral assembly | TraceDecay context compiler | Code, session, Native fact, rule, and provider capability reads | One immutable request-scoped envelope with deterministic ordering, provenance, coverage, and typed partial/unavailable lanes. |

## Precedence in final context

1. **Current code truth** — authoritative current implementation evidence.
2. **Curated rules** — authoritative selection, safety, disclosure, and behavior policy; they still cannot rewrite code truth.
3. **Accepted Native facts** — durable project/profile knowledge with lineage and provenance.
4. **Session evidence** — admitted history for the exact session and coding scope.
5. **Provider recall candidates** — lowest advisory lane, always labelled and subject to every higher authority.

Conflicting or stale memory is excluded or explicitly marked. It never silently overrides current code.

## Write paths

### Source code

```text
admitted request
  → exact project/repository/worktree/branch scope
  → source-edit authorization and current-authority proof
  → expected content + file identity comparison
  → crash-safe atomic publication to the exact worktree
  → derived index refresh
```

The index is a projection. The exact worktree filesystem is the authority.

### Accepted facts

```text
authorized explicit command or reviewed automatic promotion
  → FactOwnerV1-bound MemoryApplication
  → Native DatabaseFactStore / ProjectMemoryFactStore
  → sanitization + idempotency + lineage + durable receipt
  → rebuildable graph/search/status projections
```

Session evidence or provider output requires this separate promotion path. Recall alone cannot create a fact.

### Provider observations

```text
canonical host ingest or operation settlement
  → normalized scoped observation
  → bounded idempotent product outbox
  → selected provider adapter
  → provider acknowledgement/terminal receipt
```

Observer mode has no prompt, source-edit, Native-fact, configuration, or externally visible effect. Capacity exhaustion and delivery failure are typed and inspectable; nothing is silently dropped.

### Curated rules

```text
explicitly authorized configuration mutation
  → scope resolution + revision check + transactional commit
  → audit/rollback receipt
  → pinned effective snapshot
  → optional generated managed-skill projection
```

A provider cannot activate itself, modify rules, or become a configuration writer.

## Read and context paths

```text
immutable request identity and exact coding scope
  → current code/source admission
  → pinned curated-rule snapshot
  → Native accepted-fact read
  → exact session-evidence read
  → capability registry selects configured provider adapters
  → bounded provider recall with deadline/cancellation
  → context compiler validates scope, freshness, provenance, disclosure, conflicts, and budgets
  → deterministic final context envelope with per-lane coverage/terminal status
```

The context compiler, not a provider, owns the final envelope. A provider never chooses its own placement, strips its provenance, or reports the whole request as complete.

## Failure rules

- **No silent fallback.** Provider failure does not switch to another provider or Native implicitly. Any future fallback policy must be explicit, pinned, observable, and separately accepted.
- **No fake empty success.** Zero results means the authority completed successfully with zero results. Unavailable, unsupported, stale, cancelled, timed out, partial, conflict, and effect-unknown stay distinct.
- **No authority escalation.** Provider recall cannot edit source, write Native facts, mutate rules, change Native trust, or write session evidence.
- **No scope weakening.** Repository-only bucketing is insufficient for mutable coding context; project and worktree are mandatory, with branch/session/provider dimensions when carried by the source domain.
- **No provider-name branching outside construction.** CLI, MCP, HTTP, dashboard, context compiler, and stores depend on capability contracts. Registry/adapters alone map configured names to implementations.

## Provider boundary

The approved insertion points remain above Native fact-store contracts:

1. normalized observation fan-out after host admission or canonical settlement;
2. advisory recall contribution before final context compilation;
3. post-settlement feedback/outcome fan-out;
4. capability registry and narrow daemon composition mount.

Rejected:

- implementing an external cognitive provider as `ProjectMemoryFactStore`;
- replacing `DatabaseFactStore`;
- provider-specific transport schemas;
- provider-name switches across public surfaces;
- provider output directly mutating canonical TraceDecay state.

## Verification

```bash
python3 scripts/product/check-coding-memory-authority-matrix.py \
  --repo . \
  --matrix product/architecture/coding-memory-authority-matrix.json
python3 tests/product_coding_memory_authority_matrix_test.py
```
