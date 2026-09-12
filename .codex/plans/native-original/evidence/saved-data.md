# Saved data and lifecycle boundary

## Short answer

The fixed V2 baseline identifies original Native memory with the existing
owner-bound project graph database and its `memory_v2_*` authority. That
authority contains facts, assertions, immutable payloads and FTS, supersession,
evidence, lineage, current projections, automatic-fact receipts, operation
receipts, and feedback history. The canonical session-temporal database and
host observation journal are separate host authorities: they settle session
records and own delivery attempts, acknowledgements, and replay cursors. The
restored Native implementation must continue to use those existing authorities
through their existing owner-bound paths.

The added `StagedObservationStore`, introduced by
`44f4fbc17357d9e9521770d2ce253acd1ce03828`, is a provider-local substitute. It
is opened at `<data-root>/provider-state/native/staged-observations-v1.sqlite3`
and stores staged session observations, provider evidence, advisory recall
content, and provider-local lifecycle receipts. Its module explicitly calls
this derivative advisory state and says it never writes canonical memory
([`native_staged_observations.rs:1-37`](../../../../crates/tracedecay/src/daemon/retained_owner/native_staged_observations.rs#L1)).
It is not original Native state.

The minimal non-destructive cutover is to stop constructing, opening, and
writing that staged store on the Native route. Leave its existing file and
SQLite sidecars exactly where they are, with no automatic copy, rename,
quarantine, decode, migration, replay-cursor reset, or import. They become
stale bytes with no reader in the restored Native path. Mount the original
Native implementation against the existing canonical graph/session
authorities and leave the host observation journal, its cursor, canonical
receipts, settings, and storage layout unchanged. On restart, the host follows
its existing startup and replay behavior; the cutover adds no replay migration.

`ResetRequired` applies only where the original final-store admission already
requires it. The cutover must not add a compatibility reader or a new reset
rule for the stale staged file. The V2 roadmap remains the guardrail: exact
final persisted shapes are admitted, and old persisted state is not converted,
backfilled, shadow-read, or dual-written
([`00-plan-set-index.md:15-22`](../../../../docs/plans/tracedecay-v2/00-plan-set-index.md#L15)).

## Evidence

| Original revision/path and symbol | Current path and symbol | Behavior established by the evidence | Difference or integration requirement |
| --- | --- | --- | --- |
| Fixed baseline `b3b43410e47115056f2066449aafa1822bbb6049`: `crates/tracedecay-runtime-core/src/db/memory_v2/schema/baseline.rs`, `BASELINE_SCHEMA`; `schema/final_authority.rs`, `FINAL_MEMORY_SUPPORT_SCHEMA` | [`crates/tracedecay/src/tracedecay/facts.rs:12-46`](../../../../crates/tracedecay/src/tracedecay/facts.rs#L12); [`crates/tracedecay-session-memory/src/fact_store/mod.rs:101-177`](../../../../crates/tracedecay-session-memory/src/fact_store/mod.rs#L101) | Original canonical memory state is owner-bound facts and assertions, immutable payloads/FTS, supersession, evidence, lineage, current state, automatic-fact receipts, operation receipts, and feedback history. `DatabaseFactStore` receives an already-open database and does not select a second path. | Native must bind to the same project memory owner and `graph_db_path`, preserving original reads, writes, feedback, lineage, retrieval, curation, payload access, and receipt behavior. No staged row is a fact or an input to these tables. |
| Fixed baseline `b3b43410e47115056f2066449aafa1822bbb6049`: `crates/tracedecay-session-temporal-store/src/schema_constants.rs`, `SESSION_TEMPORAL_SCHEMA_VERSION`/`TEMPORAL_TABLE_COLUMNS`; `crates/tracedecay-global-db/src/session_temporal_schema.rs`, `install_session_temporal_schema` | [`crates/tracedecay/src/daemon/project_composition.rs:1748-1797`](../../../../crates/tracedecay/src/daemon/project_composition.rs#L1748); [`crates/tracedecay-runtime-core/src/storage/paths_and_io.rs:960-997`](../../../../crates/tracedecay-runtime-core/src/storage/paths_and_io.rs#L960) | Canonical session state includes session generations, turns, threads, agents, occurrences, projection/relation receipts, refresh progress, observation effects, and FTS. Project and profile session databases are opened together and the project session graph is settled before serving. | Keep `sessions_db_path` and session settlement as host authorities. Native can consume their settled contract through the existing adapter, but must not replace the canonical transcript/session graph with staged rows. Existing session receipts remain in place and follow the existing admission behavior. |
| Fixed baseline storage identity: `crates/tracedecay-runtime-core/src/storage/layout.rs`, profile sharding and persisted-layout resolution | [`layout.rs:69-71,176-215`](../../../../crates/tracedecay-runtime-core/src/storage/layout.rs#L69); [`paths_and_io.rs:969-989`](../../../../crates/tracedecay-runtime-core/src/storage/paths_and_io.rs#L969) | The profile-sharded data root is `<profile-root>/projects/<project-id>`. The layout derives graph, config, session, manifest, and lock paths from that root; repository markers and persisted shard evidence resolve identity. | Preserve the resolved project/profile/repository/worktree/branch identity and all canonical paths. The provider-state filename is placement only and must not become Native identity or a cutover source. |
| No provider-local staged state appears in the fixed original memory/session authority. The added store is recorded by immutable history in `44f4fbc17357d9e9521770d2ce253acd1ce03828`. | [`native_staged_observations.rs:55-164`](../../../../crates/tracedecay/src/daemon/retained_owner/native_staged_observations.rs#L55); [`:533-617`](../../../../crates/tracedecay/src/daemon/retained_owner/native_staged_observations.rs#L533); [`:2842-2859`](../../../../crates/tracedecay/src/daemon/retained_owner/native_staged_observations.rs#L2842) | The added strict table stores all exact-scope fields, source identity, sanitized payload/digest, operation/request identities, provider reference, receipt, effect digest, sequence, and tombstone state. Its extra tables hold provider-local generation, operation receipts, deletion fences, and replay state. | Remove this store from the Native construction and dispatch path. Leave the existing database bytes and sidecars untouched. Do not treat its provider receipts, generation, deletion fences, or replay cursor as canonical state or as original Native recovery input. |
| No original Native behavior is established for the added scorer. | [`native_staged_observations.rs:700-716`](../../../../crates/tracedecay/src/daemon/retained_owner/native_staged_observations.rs#L700); [`native_provider.rs:337-350`](../../../../crates/tracedecay/src/daemon/retained_owner/native_provider.rs#L337) | The current staged path recalls checkout-matching rows with its own bounded scoring and stages a session message without writing a canonical fact. | Native restoration must use the fixed original backend's retrieval and state changes. It must not retain the custom lexical/recency result, narrow Native to staged messages, suppress a supported operation, or substitute empty results. |
| Current provider construction owns the staged store: [`native_provider.rs:186-235`](../../../../crates/tracedecay/src/daemon/retained_owner/native_provider.rs#L186) and [`:294-317`](../../../../crates/tracedecay/src/daemon/retained_owner/native_provider.rs#L294) | Native branch of [`project_composition.rs:347-435`](../../../../crates/tracedecay/src/daemon/project_composition.rs#L347) | Project open currently constructs the staged-backed Native port and fails that mount if the staged file cannot be opened. | Change only the Native mount/port dependency so the original backend is used. A stale staged file must have no effect on Native project open, and no new provider-state reader is needed. |
| Host authority split: [`observation_journey.rs:3-41`](../../../../crates/tracedecay/src/daemon/retained_owner/observation_journey.rs#L3) | [`observation_journey.rs:43-57`](../../../../crates/tracedecay/src/daemon/retained_owner/observation_journey.rs#L43); [`:4805-4819`](../../../../crates/tracedecay/src/daemon/retained_owner/observation_journey.rs#L4805) | Canonical session settlement precedes provider delivery. The journal owns attempts, acknowledgements, and replay position and is opened under the canonical data root. | Leave the journal, its cursor, and its existing replay/restart sequence unchanged. The Native cutover must not replay staged rows, reset the host cursor, or introduce a second recovery sequence. |
| Current settings authority: [`crates/tracedecay-runtime-core/src/config.rs:43-102`](../../../../crates/tracedecay-runtime-core/src/config.rs#L43) and daemon resolution [`crates/tracedecay/src/config.rs:287-325`](../../../../crates/tracedecay/src/config.rs#L287) | Same current paths and symbols | User data uses the profile data directory and the durable global registry (`global.db` by default or its explicit override). Daemon resolution adopts the durable registered revision and does not consult legacy `config.json`. | Preserve settings and the selected registration revision through the existing durable configuration authority. Do not derive settings from provider-state paths or staged payloads. |
| Roadmap authority [`00-plan-set-index.md:15-22`](../../../../docs/plans/tracedecay-v2/00-plan-set-index.md#L15) and [`:788-821`](../../../../docs/plans/tracedecay-v2/00-plan-set-index.md#L788) | Native cutover only | The final product admits exact final persisted shapes and refuses incompatible original stores with typed `ResetRequired`; it has no old-state conversion path. | Apply the existing rule at the original store boundaries. Do not expand the rule into an automatic reset, quarantine, or compatibility decision for the stale added staged file. |

The tester archive associated with the staged work is evidence of test handling,
not evidence of an independently released public compatibility promise. It does
not create a use-audit approval gate and does not change the minimal cutover:
the stale staged bytes remain untouched and unread.

## Proposed non-destructive cutover

1. Stop the daemon and let existing Native/provider calls drain under the normal
   lifecycle. Do not open the staged file to inspect it and do not copy or move
   it. Leave the database and any sidecars byte-for-byte in place.

2. Remove the staged store from Native construction and dispatch. The restored
   Native port binds the fixed original backend to the existing owner-bound
   project memory/session authorities. The canonical graph database,
   `sessions.db`, host observation journal, settings registry, and their
   receipts/cursors keep their existing paths and owners.

3. Keep host observation flow unchanged. Canonical session settlement still
   precedes delivery, and restart still uses the host journal's existing
   cursor/replay behavior. No staged row is replayed, and no provider-local
   cursor is copied into the host journal.

4. If the original store admission rejects an incompatible persisted shape,
   return the original typed `ResetRequired` outcome and require the existing
   explicit reset/recreation operation. Do not add a staged-state migration,
   compatibility reader, automatic reset, or backup copy as part of this
   cutover.

5. Resolve settings exactly as before from the durable configuration authority.
   Existing canonical receipts and settings are neither rewritten nor
   synthesized. A staged provider receipt is simply outside the restored Native
   recovery path.

This is reversible at the code/process boundary: before accepting the new
Native mount, stop it and restore the prior executable if needed. The stale
staged file remains at its original path throughout, while the canonical
stores are not rewritten by either direction.

## Exact future write ownership

* The **Native restoration lane** owns the adapter in
  `crates/tracedecay-memory-provider-native/src/lib.rs`, the host bridge in
  `crates/tracedecay/src/daemon/retained_owner/native_provider.rs`, and the
  Native branch of `crates/tracedecay/src/daemon/project_composition.rs`. Its
  narrow change is to remove staged construction/open/write/recall from that
  route and bind the fixed original backend to the existing canonical handles.
* The **host lifecycle owner** owns any shared change to
  `crates/tracedecay/src/daemon/retained_owner/observation_journey.rs`. No
  change is required for this cutover; if integration exposes an ordering
  issue, that owner must preserve the existing journal cursor and replay
  semantics.
* The **storage/schema owner** owns any shared change to canonical graph,
  session, journal, or configuration admission. This task does not add a
  migration, backfill, compatibility reader, or reset path for provider-state.

The staged database file and sidecars, canonical `memory_v2` schema and
fact-store ownership, canonical session-temporal schema, host journal schema,
settings authority, and operator databases remain untouched. The only
dependencies are the fixed baseline's Native API/behavior and the existing
owner-bound handles; no release/use approval gate is a prerequisite for simply
retiring the substitute route.

## Observable acceptance checklist

* A fresh Native project opens the fixed original canonical backend and does
  not create or open `provider-state/native/staged-observations-v1.sqlite3`.
* With an existing staged file present, Native project open, recall, observe,
  feedback, maintenance, snapshot/restore, delete, and replay behavior are
  unchanged from the fixed original backend. The staged file and sidecars
  remain byte-for-byte untouched.
* Existing canonical fact IDs, assertions, payload-access/current state,
  feedback history, operation receipts, session generations, and host journal
  acknowledgements/cursor remain observable through their original owners.
* A settled session observation follows the original Native behavior and
  receipt/effect semantics. It does not use the custom staged scorer or create
  a staged substitute row.
* After restart, the existing host journal resumes from its existing cursor;
  no staged row is replayed and no cursor is reset or advanced by the cutover.
* Durable settings resolve from the registered configuration authority exactly
  as before; legacy `config.json`, provider-state paths, and staged payloads do
  not supply settings.
* An incompatible original persisted shape returns the original typed
  `ResetRequired` outcome before interpretation. No new compatibility reader,
  migration, backfill, dual write, shadow read, automatic quarantine, or
  automatic reset is exercised.
* Every supported original Native lifecycle operation retains its original
  state changes, scope checks, failure behavior, and receipts across restart.

## Remaining uncertainty

The staged tester archive does not establish a released compatibility promise,
so no compatibility disposition is assigned to its bytes. The fixed baseline
and current source establish the ownership boundary needed for this minimal
cutover. Any future work that discovers a separate original Native artifact
must assign it to the fixed baseline's owner and preserve the same rule: keep
canonical authorities on their existing paths and do not use the added staged
file as conversion input.

## Bounded Luna Max implementation proposal

**Goal.** Retire the added staged session store as Native's substitute while
mounting the fixed original Native backend over the existing canonical
authorities and preserving normal restart, receipt, cursor, settings, scope,
and lifecycle behavior.

**Prerequisites.** Use the fixed baseline `b3b43410e471...`, the existing
owner-bound graph/session handles, and the current host journal/configuration
contracts. No live database access, archive approval, or use-audit gate is
required.

**Files and steps.** In the Native adapter/application-port and Native
composition branch, remove staged store construction/open/write/recall and
connect the original backend. Leave `native_staged_observations.rs` data files
and sidecars untouched. Preserve the existing observation journey, canonical
store admission, settings resolution, journal cursor, and receipts. Add direct
behavioral coverage for fresh open, stale-file presence, existing canonical
receipts, restart, scope, and every supported Native lifecycle operation.

**Verification.** Check that Native never touches the staged path, canonical
fact/session/journal/settings identity is unchanged, original state changes and
receipts survive restart, and the original `ResetRequired` boundary remains
the only persisted-shape refusal. Verify behavior, not source-string absence.

**Forbidden shortcuts and stop condition.** Do not copy, rename, quarantine,
decode, import, migrate, backfill, dual-write, shadow-read, or replay staged
data; do not reset or rewrite the host cursor; do not retain the custom scorer,
suppress an original operation, or return empty results as a substitute. Stop
if removing the substitute would require changing canonical store ownership or
original behavior; assign that shared change to its owner instead of inventing
new persistence handling.

