# M0 GO/NO-GO — Pluggable Cognitive Memory Providers

**Decision:** GO  
**Date:** 2026-08-30  
**Bead:** `tdmem-0107`  
**Next executable bead:** `tdmem-0201`

## Executive decision

Proceed with a capability-based cognitive-memory provider boundary **above** existing TraceDecay Native application ports.

The decision does not authorize replacing or weakening TraceDecay's canonical authorities. Current code, exact repository/worktree/branch/session identity, admitted session evidence, accepted Native facts, curated rules, host integration, and final context assembly remain TraceDecay-owned. Provider recall is advisory evidence. Provider state is separate. Observer execution is non-influential. All provider operations remain exact-scope, provenance-aware, bounded, cancellable, idempotent where mutating, crash-recoverable, and explicit about terminal outcomes.

No concrete NCM transport is selected in M0. The licensed surface audit (`tdmem-0701`) and follow-up topology ADR (`tdmem-0702`) are mandatory before production NCM transport code. OCEAN remains a reserved provider slot only until a versioned specification exists.

The machine-readable decision, evidence graph, risks, hard gates, no-go triggers, and implementation train are in `product/architecture/m0-go-no-go.json`.

## Evidence reviewed

| Evidence | Result | Decision relevance |
| --- | --- | --- |
| `product/upstream/pr707-floor.json` | Accepted | Pins immutable PR #707 floor `08fbe33a7c7f403191fd5d6e356c7b6681b96403`; no moving-branch inference. |
| `product/baseline/tracedecay-v2-pr707-linux.json` | Passed | Clean build and focused memory, retrieval, host, daemon, dashboard, generated-drift, and clean-tree lanes passed in Actions run `33299093667`; receipt commit `b391cd32ba51e5a8ad584740073554476bca5d8c`. |
| `product/architecture/native-memory-surface-map.json` | Complete | Maps production write, recall, feedback, maintenance, inspection, host-injection, CLI, MCP, SDK, dashboard, hooks, automation, persistence, and test paths. |
| `product/architecture/coding-memory-authority-matrix.json` | Accepted | Names one canonical writer per durable state domain and keeps provider recall advisory. |
| `product/upstream/patch-footprint-policy.json` | Enforced | Locks additive ownership, narrow mounts, forbidden zones, dependency directions, and hard quantitative caps. |
| `product/upstream/convergence-map.json` | Clean | At the M0 decision point, product implementation has no intentional edit to an existing Zack-owned production or test file. |
| `product/architecture/adr/manifest.json` | Accepted | Eight foundational decisions bind provider boundary, authority, crate layout, lifecycle topology gate, recovery, context compilation, observer isolation, and convergence. |

## Why GO

### The seam is above canonical Native storage

The Native memory surface already converges on owner-bound application/use-case operations with exact scope, validation, privacy controls, lineage, receipts, typed terminal outcomes, deadlines, cancellation, and recovery. A provider boundary can therefore describe capabilities and advisory evidence without asking NCM or future providers to implement `ProjectMemoryFactStore`, share Native fact tables, or impersonate accepted explicit facts.

### TraceDecay can remain the host and context authority

Repository/worktree identity, branch/session admission, current-code reads, host settlement, policy/configuration, and context compilation already have TraceDecay-owned boundaries. Provider results can enter only after exact-scope and provenance admission and can remain a separately labelled context lane beneath current code and policy.

### Native parity can be proved before default cutover

The direct Native path remains the oracle. The new Native adapter must map existing application ports and pass golden parity for successful results, zero-result behavior, errors, receipts, cancellation, deadlines, recovery, feedback, maintenance, inspection, and scope failures before it can become the default route.

### Observer delivery can be reliable without being influential

Provider observations can be appended after canonical settlement to a bounded durable outbox. At-least-once delivery with stable idempotency and typed partial-effect receipts permits replay and crash recovery while preventing provider latency/failure from changing canonical host results or prompts.

### Upstream convergence remains governable

Most work fits additive product-owned crates. The few expected mounts are predeclared and budgeted. Existing upstream-file edits require an active convergence-map entry, executable invariants, line caps, and a removal/rebase plan. Syncs advance only through isolated, reviewable convergence trains.

## Binding authority rules

1. Current source bytes in the exact admitted worktree remain highest-priority truth.
2. TraceDecay remains canonical for repository/worktree/branch/session identity and admitted host evidence.
3. TraceDecay Native remains canonical for accepted explicit facts, lineage, feedback, trust, privacy, and recovery.
4. Providers own only provider-local cognitive state.
5. Provider recall remains labelled advisory evidence; it cannot directly mutate code, Native facts, sessions, tools, approvals, configuration, or external actions.
6. Promotion into Native facts/rules is a separate explicit, idempotent, audited workflow.
7. TraceDecay alone assembles final request-scoped context.
8. Unsupported, unavailable, stale, partial, cancelled, timed-out, failed, and successful-zero-result states remain distinct; no silent fallback or fake readiness is allowed.

## Residual risks and mandatory mitigations

| Risk | Severity | Required mitigation/evidence |
| --- | --- | --- |
| Native semantics leak into the provider API | High | Capability-oriented M1 contracts and dummy-provider conformance: `tdmem-0201`, `tdmem-0209`, `tdmem-0306`. |
| Native behavior drifts behind the adapter | Critical | Direct-path oracle plus exact parity and rollback: `tdmem-0401`–`tdmem-0404`. |
| Crash occurs after provider commit but before acknowledgement | Critical | Stable idempotency, durable bounded journal, partial-effect receipts, replay, and injected crash journeys: `tdmem-0203`, `tdmem-0206`, `tdmem-0502`, `tdmem-0503`, `tdmem-0506`. |
| Stale, revoked, cross-worktree, sensitive, or injection-like recall enters context | Critical | Exact-scope/temporal/revocation admission, evidence formatting, deterministic budgets, and explain trace: `tdmem-0204`, `tdmem-0603`, `tdmem-0604`, `tdmem-0608`, `tdmem-0609`. |
| Observer output or failure influences product behavior | Critical | Result-unreachable observer route plus identical output/state hashes under healthy, slow, failing, and restarting observers: `tdmem-0305`, `tdmem-0505`, `tdmem-0703`, `tdmem-0903`. |
| Licensed NCM surface does not support assumed lifecycle/topology | High | Audit first, then select topology by ADR: `tdmem-0701`, `tdmem-0702`. |
| Future upstream sync becomes conflict-heavy or semantically unsafe | High | Additive ownership, hard caps, convergence mapping, isolated sync trains, conflict receipts, and parity gates: `tdmem-0308`, `tdmem-1203`, `tdmem-1205`, `tdmem-1206`, `tdmem-1208`. |

## Locked implementation order

### 1. M1 — Provider-neutral contracts and conformance model

Start at `tdmem-0201`. Define provider identity/capabilities, handshake/version/limits, observation/idempotency, recall candidates/provenance/warnings, feedback/maintenance/correction/snapshot contracts, typed terminal outcomes, and dummy-provider conformance. No concrete provider adapter or public provider-specific transport precedes this contract layer.

### 2. M2 — Product-owned crates, dependency guards, and narrow composition mount

Create additive provider API, registry/fabric, observation, context, Native adapter, NCM adapter, and conformance crates. Enforce dependency direction mechanically. Mount only through approved application/composition seams and register every existing upstream-file edit before closure.

### 3. M3 — TraceDecay Native adapter and exact parity

Map existing Native application ports through the provider-neutral boundary. Keep the direct path as oracle and rollback. The provider route cannot become default until parity and compatibility gates pass.

### 4. M4 — Durable observation delivery and provider lifecycle

Implement bounded durable outbox, idempotent dispatch/receipts/replay, health/supervision/bounded shutdown, observer non-interference, backpressure, and crash-recovery journeys.

### 5. M5 — Advisory recall admission and TraceDecay-owned context compilation

Add transport-neutral recall application ports, deterministic normalization, exact scope/provenance/freshness/revocation admission, deduplication, budgets, evidence formatting, explain traces, and real coding-agent context journeys.

### 6. M6 — NCM audit, topology decision, observer integration, then guarded active mode

Audit the licensed NCM surface (`tdmem-0701`), select topology by ADR (`tdmem-0702`), admit NCM as observer, prove isolation and recovery, evaluate stale/harmful memory and provider failures, then consider guarded active mode. No shortcut across this order is authorized.

### 7. Later trains — Lifecycle workflows, safety/evaluation, integrations, packaging, and convergence

Only after the boundary, Native parity, durable dispatch, and context admission are proven: implement feedback/correction/promotion/maintenance/inspection; broad safety and provider conformance; SDK/CLI/MCP/host integration and coding-agent journeys; packaging/operations; and the repeatable upstream convergence train.

## Hard gates

- No concrete provider implementation or public provider-specific surface before accepted M1 contracts and conformance.
- No Native default cutover without exact parity evidence and rollback.
- No production NCM transport before `tdmem-0701` and `tdmem-0702`.
- No observer-produced value reachable from prompts, canonical state, tools, approvals, or external actions.
- No active NCM mode before scope, staleness, crash, provider-failure, privacy, and observer-isolation gates.
- No silent provider fallback, fake readiness, unbounded queue, swallowed error, or undocumented authority.
- No existing upstream-owned file edit without a current convergence-map entry and hard-cap compliance.
- No OCEAN implementation counted as delivered before a versioned specification.

## NO-GO triggers

The program must stop and return to architecture review if any of the following becomes necessary:

- a provider claims canonical ownership of current code, exact scope identity, sessions, accepted Native facts, curated rules, or final context;
- a provider must directly implement `ProjectMemoryFactStore` or share/co-write Native fact tables;
- exact scope, provenance, deadlines, cancellation, limits, idempotency, typed terminal outcomes, bounded queues, or recovery cannot reach the concrete provider operation;
- observer execution changes any product-visible output or canonical result;
- Native parity cannot be proved or rolled back;
- NCM licensing/audited surface cannot support a compliant bounded adapter;
- required implementation exceeds the upstream patch budget without a reviewed exception ADR.

## Sign-off

M0 evidence supports a **GO** decision with the hard gates above. The first authorized implementation action is `tdmem-0201`: define provider identity and capability-registry contracts. Concrete adapters remain blocked until the relevant M1/M2 contracts and guards are accepted.
