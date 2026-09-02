# ADR-0009: Run each NCM exact scope in a supervised isolated local process

Status: Accepted topology decision; production admission blocked
Date: 2026-08-30

## Context

The `tdmem-0701` audit evaluated Biomem `0.0.2` at immutable revision
`500847ff65b5d9548b3826fa29bf3ccf8d221147`. The audited implementation is a
Python package with a daemon, a loopback HTTP v1 transport, an MCP stdio facade,
and direct Python objects. It does not expose a stable Rust API.

The loopback transport is a real bounded integration surface: it identifies the
Biomem product and protocol, limits request and response sizes, and supports
concurrent HTTP dispatch. Those facts prove a usable transport boundary, not a
production provider. Current health can report ready without verified loaded
state, mutations have no durable deduplication or effect reconciliation,
client timeout does not cancel server work, and persistence replaces the final
state file non-atomically.

The audit exercised source syntax and bounded synthetic requests through the
real HTTP fallback transport on Darwin arm64/Python 3.12.8. It did not exercise
the real model stack, full daemon startup, persistence mutation, or state
migration, so none of those behaviors are inferred from the transport probe.

The current daemon owns one configured state file and accepts no TraceDecay
scope on search. The MCP stdio facade is stateless, owns no state file, exposes
only a subset of the daemon, and explicitly describes store as non-idempotent.
The topology therefore has to supply exact-scope state ownership and lifecycle
isolation without pretending that transport presence closes the audited
semantic gaps.

## Decision

Select a **supervised isolated local process** as the first NCM integration
topology. This resolves the topology choice deferred by ADR-0004.
It does not admit NCM to production.

TraceDecay maps each admitted exact profile/project/repository/worktree/branch/
session scope to an opaque provider namespace. One supervised Biomem process,
one provider-owned state directory, one private endpoint, and one readiness
incarnation belong exclusively to that namespace. The child receives the
opaque namespace and exact-scope digest needed by the provider protocol, never
the raw scope fields. State directories are never shared across exact scopes.

The TraceDecay NCM adapter talks only through `NcmCognitiveSurface`. Its first
implementation may use Biomem's private loopback HTTP v1 transport, but HTTP
types, ports, process identifiers, and Python details remain behind that
surface. The endpoint binds loopback only, is not published as a user API, and
must use an instance-scoped authentication capability before production.
Provider recall may adapt Biomem's side-effect-free `search` primitive only;
`retrieve`, raw MCP tools, `CommandHandler`, `TextMemory`, and administrative
operations are not reachable through the adapter unless a later capability
contract admits their complete semantics.

`NcmCognitiveSurface` capability-gates health/status, recall, and any later
observation mapping independently. Mandatory observation remains blocking: the
existence of `store_record` grants no observe capability until durable dedupe,
cancellation, and effect semantics pass. The first topology exposes only the
selected HTTP endpoint to the adapter. Biomem's audited WebSocket listener is
disabled; if a pinned build cannot disable it, that build is not admitted.

The supervisor launches a pinned installed Biomem distribution with an
allowlisted environment and provider-owned working directory. Production
isolation requires an enforced child policy that grants only the installed
runtime assets, that exact scope's state directory, bounded temporary storage,
and explicitly declared provider dependencies. It grants no TraceDecay database,
repository or worktree files, host credentials, unrelated provider state, or
raw TraceDecay identity. A same-user process boundary without that
enforcement is crash containment only and must not be reported as authority or
data isolation. If the supervisor cannot install and verify the enforced child
policy, that provider instance fails closed before spawn or readiness.

Readiness is an incarnation-bound handshake result, not process or socket
existence. It must bind the provider instance, immutable build and effective
configuration, loaded-state result, state schema and generation, recovery
status, exact-scope digest, negotiated capabilities, and limits. Each value is
compared with the supervisor's pinned executable, configuration, namespace,
state-directory, scope, and requested contract values. A mismatch, corrupt or
incompatible state, incomplete recovery, or unverifiable value returns a typed
fail-closed unavailable/reset-required outcome.

Any process exit or restart invalidates the readiness result and ephemeral
transport-incarnation receipts. Durable operation identities, dedupe records,
committed outcomes, and effect-reconciliation evidence survive the restart and
remain queryable. The supervisor confirms the predecessor is dead before it
starts a replacement against the same immutable scope namespace and state
directory; overlapping scope owners are forbidden. The replacement starts
unavailable with a fresh incarnation and must complete the full handshake.

Restart and shutdown are bounded. Configuration supplies finite spawn,
handshake, request, cancellation/drain, graceful-stop, forced-kill, restart
window, attempt, and capped-backoff budgets; negotiated limits supply finite
request/response bytes and concurrency. Restart-budget exhaustion leaves a
typed unavailable provider while TraceDecay remains usable. Shutdown stops
admission, propagates the remaining deadline and cancellation to each operation,
drains only within the grace budget, requests a durable provider close, and
escalates termination only after that budget. An interrupted mutation is
`unknown` until a durable operation identity can reconcile its effect. It is
never blindly retried.

Every request receives one monotonic remaining budget and live cancellation
signal across readiness checks, transport, queueing, and provider execution.
Client timeout alone never proves server cancellation or a bounded mutation.

The TraceDecay side of the topology is implemented and behaviorally tested
against a conforming child process in
`crates/tracedecay-memory-provider-ncm/tests/ncm_local_process.rs`, which
exercises the real supervisor, the real private-loopback transport, and the
real durable effect store:

- the supervisor enforces child access denial through a probe-verified backend
  and fails closed when this host cannot prove it;
- exact-scope namespace isolation, one child, one state directory, one private
  loopback endpoint and one readiness incarnation per exact scope, with a
  bounded scope set that refuses rather than evicts;
- readiness identity is pinned to build, configuration, state schema,
  generation, loaded state, recovery, and a challenge-response proof, and any
  crash or restart invalidates it and requires a fresh handshake;
- bounded restart with typed unavailability on budget exhaustion, and bounded
  shutdown that closes admission and reaps the child;
- host-side durable dedupe, same-key/different-payload conflict detection,
  unknown-effect records for interrupted mutations, atomic publication, and
  refusal of corrupt or truncated records without a silent reset.

That evidence covers the host supervisor and adapter only. The selected
topology still requires, and does not yet have, all of the following from
Biomem before production admission:

- fail-closed loaded-state, build/configuration, schema, generation, and
  recovery identity in Biomem;
- server-side deadline/cancellation and mutation effect reconciliation in
  Biomem;
- durable idempotency/deduplication with same-key/different-payload conflict
  detection across restarts in Biomem;
- atomic crash-safe persistence and corrupt/incompatible load rejection in
  Biomem.

`tdmem-0504` remains a blocking dependency: the supervisor behavior above is
implemented and tested here, but its acceptance is recorded by that bead, and
`supervisor_capability` stays a production blocker until it closes.

## Consequences

- A Biomem crash, interpreter failure, or model failure does not crash the
  TraceDecay process, although OS resource exhaustion still needs supervisor
  limits.
- Exact scopes cannot accidentally share one daemon state file or readiness
  receipt.
- The selected boundary requires the supervisor to deny host database handles
  and repository, Git, session, fact, prompt, tool, approval, or final-context
  authority. This is a production requirement, not current implementation proof.
- Private loopback adds serialization and scheduling cost. The audit measured
  transport concurrency only, so end-to-end model and persistence latency
  remain unknown until a real bounded journey runs.
- The adapter remains testable against an in-memory `NcmCognitiveSurface`, and
  the local process can later become a standalone provider without changing
  provider-neutral callers.
- Packaging must provide a pinned Python runtime/distribution and explicit
  upgrade, rollback, state-schema, and recovery behavior.
- The installed MIT/Python distribution remains locally inspectable.
  Process isolation is not a source-protection or IP boundary; it is selected
  for crash, lifecycle, state, and authority containment.
- Selecting the topology does not clear the production blockers or imply that
  the planned supervisor and Biomem changes exist.

## Rejected alternatives

- **In-process Biomem integration.** Rejected for the first integration because
  the audited package has no stable Rust API, embedding Python would share
  TraceDecay's crash, hang, interpreter-lock, and address-space failure domains,
  and direct objects would not enforce exact-scope filesystem or authority
  isolation. Lower call latency does not outweigh the missing terminate/restart
  boundary. A future native library can be reconsidered only if it implements
  the same `NcmCognitiveSurface` and proves equivalent lifecycle, state, and
  isolation semantics.
- **MCP stdio as the provider transport.** Rejected because the audited facade
  is stateless, owns no state file or daemon lifecycle, exposes a partial
  operation set, and declares store non-idempotent. Framing commands over stdio
  does not supply readiness, loaded-state identity, cancellation, effect
  reconciliation, or crash-safe persistence.
- **Direct use of Biomem Python objects in the adapter.** Rejected because it
  couples the Rust adapter to interpreter and internal object lifecycles while
  retaining the in-process crash and authority boundary.
- **One shared local daemon for all scopes.** Rejected because the daemon owns
  one configured state file and search carries no TraceDecay scope; adapter
  tagging cannot undo cross-scope state mixing.
- **Treat loopback HTTP as the product contract.** Rejected because transport
  evolution must remain private and provider-neutral callers depend on typed
  NCM capabilities rather than endpoints.

## Migration path

1. Keep the current contract adapter against injected test implementations of
   `NcmCognitiveSurface`; this proves envelope mapping without claiming a live
   Biomem provider.
2. Add the required loaded-state identity, durable dedupe, cancellation/effect
   reconciliation, and atomic persistence at the Biomem boundary.
3. Implement and behaviorally test the access-enforcing supervisor and private
   loopback surface, then run handshake and failure conformance with all
   mutation capabilities disabled.
4. Enable scoped recall and observation only after their separate conformance,
   crash, deadline, privacy, and effect-reconciliation journeys pass. Rollout is
   observer-first and fail-closed; transport presence grants no capability.
5. Migrate existing provider state only while its exact scope is quiescent.
   An exclusive supervisor migration fence stops admission and restart, drains
   or reconciles every operation, and confirms the predecessor is dead. Verify
   source and destination provider/build/config/schema/generation/scope identity,
   create a restorable per-scope backup, convert into a temporary destination,
   validate item/count conservation plus recall and persistence postconditions,
   then atomically publish. Failure restores the prior build and state; it never
   crosses scopes or silently resets to empty state.
6. A future native or standalone transport may replace loopback behind the same
   versioned `NcmCognitiveSurface` handshake. It must re-prove every lifecycle,
   isolation, persistence, and effect invariant before cutover.

## Invariants

1. Exactly one process and one provider-owned state directory serve an active
   NCM instance for one exact scope; neither is shared across scope digests.
2. `NcmCognitiveSurface` is the only adapter-visible NCM boundary; loopback
   HTTP and process mechanics do not escape it.
3. The child has no TraceDecay DB, repository/worktree, credential, raw scope
   identity, canonical fact, prompt, tool, approval, or final-context access.
4. Process start, socket accept, and non-empty state are never readiness proof.
5. Every process restart invalidates readiness and ephemeral incarnation
   receipts, requires a new identity-bound handshake, and preserves durable
   operation/effect evidence needed for dedupe and reconciliation.
6. Restart, shutdown, cancellation, request/response bytes, concurrency, CPU,
   memory, temporary storage, and diagnostic output are bounded.
7. A timed-out or terminated mutation remains unknown until reconciled and is
   never converted into a safe retry by transport inference.
8. Provider persistence is scope-owned and crash-safe before readiness; corrupt
   or incompatible state cannot silently reset to an empty ready provider.
9. NCM remains advisory and production-blocked until every listed blocker has
   behavioral evidence through the selected topology.

## Verification

Executable evidence and planned dependency-cone checks:

- `tdmem-0701` — pinned-source audit, callable-surface inventory, HTTP transport
  probes, and explicit production blockers.
- `tdmem-0702` — this selection plus semantic ADR validation; it does not
  certify runtime implementation.
- `tdmem-0504` — planned supervisor behavior: crash isolation, bounded restart,
  fresh-handshake readiness, and bounded shutdown.
- `tdmem-0703` — planned NCM build/state identity, truthful capabilities, and
  fail-closed health.
- `tdmem-0704` — planned exact-scope observation mapping and durable
  idempotency/effect behavior.
- `tdmem-0707` — planned snapshot, restore, replay, compatibility, and
  crash-safe persistence journeys.

Focused runtime evidence must include two distinct exact scopes, attempted
cross-scope state access, child attempts to open TraceDecay DB/repository/
credential paths, crash during read and mutation, restart receipt rejection,
restart-budget exhaustion, cancellation before and after provider commit,
unknown-effect reconciliation, corrupt/truncated state, and bounded shutdown.
Restart checks reject old readiness/incarnation receipts while proving that
durable operation outcomes remain queryable across the new incarnation.
Production admission stays blocked until these tests exercise the real child
process and inspect outcomes, not merely file or endpoint presence.

Those runtime checks now exist for the host side of the topology, each as a
named behavioral test in
`crates/tracedecay-memory-provider-ncm/tests/ncm_local_process.rs`:

- two exact scopes and cross-scope denial —
  `two_exact_scopes_own_separate_namespaces_state_and_readiness`,
  `the_mountable_scope_set_isolates_two_exact_scopes_and_refuses_a_third`;
- child access denial —
  `the_child_cannot_open_host_database_repository_credential_or_sibling_scope_paths`;
- crash during read and restart receipt rejection —
  `a_crash_during_a_read_invalidates_readiness_and_requires_a_fresh_handshake`;
- restart-budget exhaustion — `restart_budget_exhaustion_leaves_a_typed_unavailable_provider`;
- cancellation before and after commit —
  `cancellation_of_a_read_returns_cancelled_and_tells_the_child_to_stop`,
  `a_cancelled_mutation_becomes_a_durable_unknown_effect`,
  `an_elapsed_deadline_returns_deadline_exceeded_without_waiting_for_the_child`;
- unknown-effect reconciliation — `a_crash_after_commit_records_a_reconcilable_unknown_effect`;
- durable dedupe and conflict —
  `a_committed_mutation_is_deduplicated_from_durable_state_across_a_restart`,
  `the_same_key_with_a_different_payload_is_a_conflict`;
- corrupt or truncated state — `a_corrupt_durable_record_is_refused_and_never_silently_reset`;
- fail-closed readiness identity —
  `settings_fail_closed_without_a_pinned_distribution`,
  `a_missing_child_program_never_reports_readiness`,
  `readiness_is_refused_when_the_child_reports_an_unpinned_build`,
  `readiness_is_refused_when_loaded_state_is_not_proven`,
  `readiness_is_refused_when_the_child_cannot_prove_the_challenge`;
- bounded shutdown — `shutdown_is_bounded_closes_admission_and_reaps_the_child`.

Each of those runs a real child process over the real private loopback
endpoint. None of them substitutes for the Biomem-side blockers above.

## Review triggers

Review when Biomem adds a stable native API, changes its daemon or state model,
requires a declared external model service, cannot satisfy server cancellation
or atomic persistence, or when measured loopback overhead threatens the product
budget. A topology migration keeps `NcmCognitiveSurface`, exact-scope ownership,
readiness invalidation, access denial, and effect truth stable while replacing
the private transport in a staged observer-first rollout with rollback to the
last compatible provider build and state snapshot.
