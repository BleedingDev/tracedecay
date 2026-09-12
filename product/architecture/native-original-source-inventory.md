# Native original source inventory

Reference commit: `b3b43410e47115056f2066449aafa1822bbb6049`.
Audited execution head: `571daf3a9612e5247443e4da3a107b542686c1ef`.

This exhaustive ledger is generated from the scoped `git diff --unified=0 --no-ext-diff b3 HEAD` and `git diff --name-status b3 HEAD` evidence selected by `SOURCE-BOUNDARY.md`. It records every changed hunk in that protected/native-adjacent surface: 57 modified paths, 28 added paths, and 278 hunks. Modified paths retain their b3 path; added paths have no b3 counterpart. Classifications are review dispositions, not a blanket allowlist.

The complete original Native implementation remains in the b3-equivalent session-memory, LCM, store/memory contracts, facts, memory-v2, retained-route, and session authorities. The only original-file restoration authorized by the reviewed decisions is the retrieval-anchor method. Existing shared host/privacy/source-identity/cursor/storage extensions remain product behavior and need separate focused regressions. The privacy detector is an explicit exception: its shared entropy algorithm changed and is recorded below as an escalated original algorithm delta.

## Protected unchanged areas

- `crates/tracedecay-session-memory/**` (including `fact_store/**`, `memory/**`, tests, and helpers; no direct scoped hunk, but the shared privacy detector delta reaches Native memory hygiene)
- `crates/tracedecay-lcm/**` (no direct scoped hunk, but the shared privacy detector delta reaches LCM sanitization)
- `crates/tracedecay-store/src/memory/**`
- `crates/tracedecay-runtime-core/src/db/memory_v2/**`
- `crates/tracedecay/src/tracedecay/facts.rs`
- `crates/tracedecay-session-temporal-store/**`
- retained session/LCM retrieval and effect authorities under `crates/tracedecay-session-runtime/src/{session_retrieval,lcm*,retained*,store_owner.rs}`

These unchanged claims come from the pinned b3-to-head comparison; they do not replace behavior tests. They describe direct file equality only. `crates/tracedecay-session-memory/src/memory/hygiene.rs` calls the shared `looks_high_entropy_token` algorithm, and `sanitize_memory_fact_payload` reaches the same privacy kernel for Native fact payloads. Host and LCM sanitization reach it through `tracedecay-privacy` (`high_entropy_ranges`) from `crates/tracedecay-lcm/src/{raw.rs,dag.rs}`. The exact `-sha256-<64 lowercase hex>` suffix peeling in `crates/tracedecay-privacy/src/detector_kernel.rs` is therefore an original shared algorithm delta, not an unchanged Native/LCM behavior claim. Restore that algorithm exactly or isolate it behind a reviewed product boundary before accepting the affected lane; run detector-kernel, Native hygiene/privacy, and LCM sanitization regressions.

## Hunk ledger

Each entry includes the exact raw zero-context hunk headers emitted by Git.

### 01. crates/tracedecay-global-db/src/configuration/registry.rs

- Status: `M`
- Original path (b3): `crates/tracedecay-global-db/src/configuration/registry.rs`
- Current path (head): `crates/tracedecay-global-db/src/configuration/registry.rs`
- Hunks (6): `@@ -15,15 +15,18 @@ use tracedecay_domain::configuration::{`; `@@ -448,0 +452 @@ struct ProjectDefaults {`; `@@ -518,0 +523 @@ impl Default for ProjectDefaults {`; `@@ -530,0 +536,15 @@ fn register_project_settings(`; `@@ -579,0 +600,18 @@ fn register_project_settings(`; `@@ -1169,0 +1208,41 @@ mod semantic_runtime_payload_tests {`
- Classification: **shared configuration extension**
- Original behavior: The original authority persists project/profile settings and validates durable revisions.
- Permitted action: Preserve transactional configuration, closed defaults, and restart semantics; Native cannot self-activate or derive settings from provider state.
- Associated check: Configuration registry/store defaults, validation, revision, and restart tests.

### 02. crates/tracedecay-global-db/src/configuration/store.rs

- Status: `M`
- Original path (b3): `crates/tracedecay-global-db/src/configuration/store.rs`
- Current path (head): `crates/tracedecay-global-db/src/configuration/store.rs`
- Hunks (2): `@@ -24,5 +24,6 @@ use tracedecay_domain::configuration::{`; `@@ -594,0 +596,3 @@ impl<'db> GlobalDbConfigurationControlStore<'db> {`
- Classification: **shared configuration extension**
- Original behavior: The original authority persists project/profile settings and validates durable revisions.
- Permitted action: Preserve transactional configuration, closed defaults, and restart semantics; Native cannot self-activate or derive settings from provider state.
- Associated check: Configuration registry/store defaults, validation, revision, and restart tests.

### 03. crates/tracedecay-global-db/src/configuration/store/tests/persistence.rs

- Status: `M`
- Original path (b3): `crates/tracedecay-global-db/src/configuration/store/tests/persistence.rs`
- Current path (head): `crates/tracedecay-global-db/src/configuration/store/tests/persistence.rs`
- Hunks (1): `@@ -517,0 +518,129 @@ async fn fresh_stores_resolve_without_the_retired_default_collection_setting() {`
- Classification: **shared configuration extension**
- Original behavior: The original authority persists project/profile settings and validates durable revisions.
- Permitted action: Preserve transactional configuration, closed defaults, and restart semantics; Native cannot self-activate or derive settings from provider state.
- Associated check: Configuration registry/store defaults, validation, revision, and restart tests.

### 04. crates/tracedecay-global-db/src/observation/codec.rs

- Status: `M`
- Original path (b3): `crates/tracedecay-global-db/src/observation/codec.rs`
- Current path (head): `crates/tracedecay-global-db/src/observation/codec.rs`
- Hunks (3): `@@ -35,0 +36 @@ pub(super) fn decode_repository_provenance_attachment(`; `@@ -45,0 +47,5 @@ pub(super) fn decode_repository_provenance_attachment(`; `@@ -51 +57,2 @@ pub(super) fn decode_repository_provenance_attachment(`
- Classification: **shared observation/provenance extension**
- Original behavior: The original observation authority settles sanitized, scoped host evidence and retention without becoming the explicit-fact store.
- Permitted action: Preserve source identity, privacy, scope, idempotence, retention, restore, and typed failures; Native consumes settled observations.
- Associated check: Observation batch, retention/restore, provenance mismatch, recent-window, and restart fixtures.

### 05. crates/tracedecay-global-db/src/observation/persist.rs

- Status: `M`
- Original path (b3): `crates/tracedecay-global-db/src/observation/persist.rs`
- Current path (head): `crates/tracedecay-global-db/src/observation/persist.rs`
- Hunks (3): `@@ -50,0 +51,3 @@ async fn read_observation_row(`; `@@ -64,0 +68 @@ async fn read_observation_row(`; `@@ -80 +84,2 @@ pub(super) async fn read_by_observation_id(`
- Classification: **shared observation/provenance extension**
- Original behavior: The original observation authority settles sanitized, scoped host evidence and retention without becoming the explicit-fact store.
- Permitted action: Preserve source identity, privacy, scope, idempotence, retention, restore, and typed failures; Native consumes settled observations.
- Associated check: Observation batch, retention/restore, provenance mismatch, recent-window, and restart fixtures.

### 06. crates/tracedecay-global-db/src/observation/retention.rs

- Status: `M`
- Original path (b3): `crates/tracedecay-global-db/src/observation/retention.rs`
- Current path (head): `crates/tracedecay-global-db/src/observation/retention.rs`
- Hunks (2): `@@ -803 +803,2 @@ async fn run_provenance_pass(`; `@@ -874 +875 @@ async fn run_provenance_pass(`
- Classification: **shared observation/provenance extension**
- Original behavior: The original observation authority settles sanitized, scoped host evidence and retention without becoming the explicit-fact store.
- Permitted action: Preserve source identity, privacy, scope, idempotence, retention, restore, and typed failures; Native consumes settled observations.
- Associated check: Observation batch, retention/restore, provenance mismatch, recent-window, and restart fixtures.

### 07. crates/tracedecay-global-db/src/observation/retention/restore.rs

- Status: `M`
- Original path (b3): `crates/tracedecay-global-db/src/observation/retention/restore.rs`
- Current path (head): `crates/tracedecay-global-db/src/observation/retention/restore.rs`
- Hunks (6): `@@ -63 +63,2 @@ pub fn replay_current_release_state_for_restore(`; `@@ -99 +100,2 @@ mod tests {`; `@@ -133 +135 @@ mod tests {`; `@@ -171 +173,2 @@ mod tests {`; `@@ -178,0 +182 @@ mod tests {`; `@@ -189,0 +194 @@ mod tests {`
- Classification: **shared observation/provenance extension**
- Original behavior: The original observation authority settles sanitized, scoped host evidence and retention without becoming the explicit-fact store.
- Permitted action: Preserve source identity, privacy, scope, idempotence, retention, restore, and typed failures; Native consumes settled observations.
- Associated check: Observation batch, retention/restore, provenance mismatch, recent-window, and restart fixtures.

### 08. crates/tracedecay-global-db/src/observation/retention/tests.rs

- Status: `M`
- Original path (b3): `crates/tracedecay-global-db/src/observation/retention/tests.rs`
- Current path (head): `crates/tracedecay-global-db/src/observation/retention/tests.rs`
- Hunks (3): `@@ -135,2 +135,2 @@ async fn seed_evidence(`; `@@ -142 +142,2 @@ async fn seed_evidence(`; `@@ -397,0 +399,8 @@ async fn superseded_and_deleted_dispositions_release_storage() -> Result<(), Str`
- Classification: **shared observation/provenance extension**
- Original behavior: The original observation authority settles sanitized, scoped host evidence and retention without becoming the explicit-fact store.
- Permitted action: Preserve source identity, privacy, scope, idempotence, retention, restore, and typed failures; Native consumes settled observations.
- Associated check: Observation batch, retention/restore, provenance mismatch, recent-window, and restart fixtures.

### 09. crates/tracedecay-global-db/src/observation/schema.rs

- Status: `M`
- Original path (b3): `crates/tracedecay-global-db/src/observation/schema.rs`
- Current path (head): `crates/tracedecay-global-db/src/observation/schema.rs`
- Hunks (4): `@@ -11 +11 @@ use super::super::global_db_operation_error;`; `@@ -13 +13,3 @@ use super::super::global_db_operation_error;`; `@@ -381,0 +384 @@ pub(super) const OBSERVATION_AUTHORITY_SCHEMA_SQL: &str =`; `@@ -455,0 +459,11 @@ pub async fn ensure_observation_schema(`
- Classification: **shared observation/provenance extension**
- Original behavior: The original observation authority settles sanitized, scoped host evidence and retention without becoming the explicit-fact store.
- Permitted action: Preserve source identity, privacy, scope, idempotence, retention, restore, and typed failures; Native consumes settled observations.
- Associated check: Observation batch, retention/restore, provenance mismatch, recent-window, and restart fixtures.

### 10. crates/tracedecay-global-db/src/observation_adapter.rs

- Status: `M`
- Original path (b3): `crates/tracedecay-global-db/src/observation_adapter.rs`
- Current path (head): `crates/tracedecay-global-db/src/observation_adapter.rs`
- Hunks (8): `@@ -28,12 +28,13 @@ use tracedecay_store::{`; `@@ -591,0 +593,10 @@ impl GlobalDbObservationStore {`; `@@ -737,0 +749 @@ struct PendingObservationAuthority {`; `@@ -803,0 +816 @@ impl ObservationBatchState {`; `@@ -1173 +1186 @@ const OBSERVATION_BATCH_ROW_PROJECTION: &str =`; `@@ -1305,0 +1319,9 @@ async fn read_stored_observations_from_snapshot(`; `@@ -1307,0 +1330 @@ async fn read_stored_observations_from_snapshot(`; `@@ -1545,0 +1569,28 @@ impl ObservationStore for GlobalDbObservationStore {`
- Classification: **shared observation/provenance extension**
- Original behavior: The original observation authority settles sanitized, scoped host evidence and retention without becoming the explicit-fact store.
- Permitted action: Preserve source identity, privacy, scope, idempotence, retention, restore, and typed failures; Native consumes settled observations.
- Associated check: Observation batch, retention/restore, provenance mismatch, recent-window, and restart fixtures.

### 11. crates/tracedecay-global-db/src/observation_batch_tests.rs

- Status: `M`
- Original path (b3): `crates/tracedecay-global-db/src/observation_batch_tests.rs`
- Current path (head): `crates/tracedecay-global-db/src/observation_batch_tests.rs`
- Hunks (1): `@@ -297,0 +298,191 @@ fn with_retrieval_alias(`
- Classification: **shared observation/provenance extension**
- Original behavior: The original observation authority settles sanitized, scoped host evidence and retention without becoming the explicit-fact store.
- Permitted action: Preserve source identity, privacy, scope, idempotence, retention, restore, and typed failures; Native consumes settled observations.
- Associated check: Observation batch, retention/restore, provenance mismatch, recent-window, and restart fixtures.

### 12. crates/tracedecay-global-db/src/schema_contract/definitions.rs

- Status: `M`
- Original path (b3): `crates/tracedecay-global-db/src/schema_contract/definitions.rs`
- Current path (head): `crates/tracedecay-global-db/src/schema_contract/definitions.rs`
- Hunks (1): `@@ -439,0 +440 @@ pub(super) const TABLES: &[Table] = &[`
- Classification: **shared observation/provenance extension**
- Original behavior: The original observation authority settles sanitized, scoped host evidence and retention without becoming the explicit-fact store.
- Permitted action: Preserve source identity, privacy, scope, idempotence, retention, restore, and typed failures; Native consumes settled observations.
- Associated check: Observation batch, retention/restore, provenance mismatch, recent-window, and restart fixtures.

### 13. crates/tracedecay-global-db/src/tests.rs

- Status: `M`
- Original path (b3): `crates/tracedecay-global-db/src/tests.rs`
- Current path (head): `crates/tracedecay-global-db/src/tests.rs`
- Hunks (1): `@@ -920,0 +921,171 @@ async fn parse_offset_pair_conflict_rolls_back_both_authorities() {`
- Classification: **shared observation/provenance extension**
- Original behavior: The original observation authority settles sanitized, scoped host evidence and retention without becoming the explicit-fact store.
- Permitted action: Preserve source identity, privacy, scope, idempotence, retention, restore, and typed failures; Native consumes settled observations.
- Associated check: Observation batch, retention/restore, provenance mismatch, recent-window, and restart fixtures.

### 14. crates/tracedecay-global-db/src/tests/harness.rs

- Status: `M`
- Original path (b3): `crates/tracedecay-global-db/src/tests/harness.rs`
- Current path (head): `crates/tracedecay-global-db/src/tests/harness.rs`
- Hunks (1): `@@ -1380 +1380,10 @@ async fn open_registered_test_database_with(`
- Classification: **shared observation/provenance extension**
- Original behavior: The original observation authority settles sanitized, scoped host evidence and retention without becoming the explicit-fact store.
- Permitted action: Preserve source identity, privacy, scope, idempotence, retention, restore, and typed failures; Native consumes settled observations.
- Associated check: Observation batch, retention/restore, provenance mismatch, recent-window, and restart fixtures.

### 15. crates/tracedecay-global-db/src/transcript.rs

- Status: `M`
- Original path (b3): `crates/tracedecay-global-db/src/transcript.rs`
- Current path (head): `crates/tracedecay-global-db/src/transcript.rs`
- Hunks (2): `@@ -11,0 +12,17 @@ impl RegisteredGlobalDb {`; `@@ -178,0 +196,280 @@ impl RegisteredGlobalDb {`
- Classification: **shared transcript/session extension**
- Original behavior: The original transcript authority owns canonical session metadata and persistence.
- Permitted action: Preserve locator identity/conflict/rollback/reopen behavior and transcript ownership; Native calls this mounted authority.
- Associated check: Live locator insert/fill/conflict/concurrency/profile-refusal and transcript catch-up tests.

### 16. crates/tracedecay-runtime-core/src/db/retrieval_anchor_authority.rs

- Status: `M`
- Original path (b3): `crates/tracedecay-runtime-core/src/db/retrieval_anchor_authority.rs`
- Current path (head): `crates/tracedecay-runtime-core/src/db/retrieval_anchor_authority.rs`
- Hunks (2): `@@ -512 +512,9 @@ impl RetrievalAnchorDispositionStore for super::Database {`; `@@ -514,2 +522,18 @@ impl RetrievalAnchorDispositionStore for super::Database {`
- Classification: **exact restoration**
- Original behavior: Original retrieval authority reads and validates the complete disposition history before selecting its latest entry, including invalid-history behavior.
- Permitted action: Only rn-restore-anchor may restore the method byte-for-byte to b3; no adapter or host lane may edit it.
- Associated check: Whole-file b3 equality plus ordinary, missing-history, and invalid-history fixtures.

### 17. crates/tracedecay-runtime-core/src/storage/paths_and_io.rs

- Status: `M`
- Original path (b3): `crates/tracedecay-runtime-core/src/storage/paths_and_io.rs`
- Current path (head): `crates/tracedecay-runtime-core/src/storage/paths_and_io.rs`
- Hunks (18): `@@ -191 +191,20 @@ impl PrivateStoreIo {`; `@@ -443,0 +463 @@ fn create_missing_directories_locked(`; `@@ -445,0 +466 @@ fn create_missing_directories_locked(`; `@@ -446,0 +468 @@ fn create_missing_directories_locked(`; `@@ -448,0 +471 @@ fn create_missing_directories_locked(`; `@@ -455,0 +479 @@ fn create_missing_directories_locked(`; `@@ -458,0 +483 @@ fn create_missing_directories_locked(`; `@@ -467 +492 @@ fn create_missing_directories_locked(`; `@@ -478,2 +503,5 @@ fn create_missing_directories_locked(`; `@@ -486 +514,4 @@ fn platform_create_dir_all_durable(path: &Path) -> io::Result<()> {`; `@@ -490,0 +522 @@ fn platform_create_dir_all_durable(path: &Path) -> io::Result<()> {`; `@@ -491,0 +524 @@ fn platform_create_dir_all_durable(path: &Path) -> io::Result<()> {`; `@@ -493,0 +527 @@ fn platform_create_dir_all_durable(path: &Path) -> io::Result<()> {`; `@@ -496,0 +531 @@ fn platform_create_dir_all_durable(path: &Path) -> io::Result<()> {`; `@@ -505 +540 @@ fn platform_create_dir_all_durable(path: &Path) -> io::Result<()> {`; `@@ -550 +585,5 @@ fn platform_create_dir_all_durable(path: &Path) -> io::Result<()> {`; `@@ -552,0 +592 @@ fn platform_create_dir_all_durable(path: &Path) -> io::Result<()> {`; `@@ -802,0 +843,33 @@ fn acquire_lock_file_blocking(lock_path: &Path, private: bool) -> io::Result<fs:`
- Classification: **shared storage extension**
- Original behavior: The original private storage layer creates durable directories, locks private paths, and preserves symlink/durability invariants.
- Permitted action: Preserve the ordinary path and filesystem behavior; this remains execution infrastructure, not a Native store.
- Associated check: Storage interruption and ordinary durability tests.

### 18. crates/tracedecay-runtime-core/src/storage/tests.rs

- Status: `M`
- Original path (b3): `crates/tracedecay-runtime-core/src/storage/tests.rs`
- Current path (head): `crates/tracedecay-runtime-core/src/storage/tests.rs`
- Hunks (1): `@@ -70,0 +71,57 @@ mod tests {`
- Classification: **shared storage extension**
- Original behavior: The original private storage layer creates durable directories, locks private paths, and preserves symlink/durability invariants.
- Permitted action: Preserve the ordinary path and filesystem behavior; this remains execution infrastructure, not a Native store.
- Associated check: Storage interruption and ordinary durability tests.

### 19. crates/tracedecay-session-runtime/src/session_sync.rs

- Status: `M`
- Original path (b3): `crates/tracedecay-session-runtime/src/session_sync.rs`
- Current path (head): `crates/tracedecay-session-runtime/src/session_sync.rs`
- Hunks (1): `@@ -90,0 +91,5 @@ pub struct DaemonSessionSyncConfig {`
- Classification: **shared session/refresh extension**
- Original behavior: The original session sync and temporal refresh state machines preserve scope, historical ingestion, frontiers, receipts, and cancellation.
- Permitted action: Preserve exact scope/provenance reuse, catch-up, refresh receipts/frontiers/cancellation, and no duplicate ingestion.
- Associated check: Session sync scope/provenance and refresh begin/status/cancel/restart fixtures.

### 20. crates/tracedecay-session-runtime/src/session_sync/git_topology.rs

- Status: `M`
- Original path (b3): `crates/tracedecay-session-runtime/src/session_sync/git_topology.rs`
- Current path (head): `crates/tracedecay-session-runtime/src/session_sync/git_topology.rs`
- Hunks (1): `@@ -57,9 +57 @@ impl SessionSyncProjectContext {`
- Classification: **shared session/refresh extension**
- Original behavior: The original session sync and temporal refresh state machines preserve scope, historical ingestion, frontiers, receipts, and cancellation.
- Permitted action: Preserve exact scope/provenance reuse, catch-up, refresh receipts/frontiers/cancellation, and no duplicate ingestion.
- Associated check: Session sync scope/provenance and refresh begin/status/cancel/restart fixtures.

### 21. crates/tracedecay-session-runtime/src/session_sync/project_lifecycle.rs

- Status: `M`
- Original path (b3): `crates/tracedecay-session-runtime/src/session_sync/project_lifecycle.rs`
- Current path (head): `crates/tracedecay-session-runtime/src/session_sync/project_lifecycle.rs`
- Hunks (5): `@@ -31,0 +32,3 @@ pub struct SessionSyncProjectContext {`; `@@ -37,0 +41,5 @@ pub struct SessionSyncProjectContext {`; `@@ -294,0 +303,11 @@ impl DaemonSessionSyncService {`; `@@ -306,0 +326 @@ impl DaemonSessionSyncService {`; `@@ -312,0 +333 @@ impl DaemonSessionSyncService {`
- Classification: **shared session/refresh extension**
- Original behavior: The original session sync and temporal refresh state machines preserve scope, historical ingestion, frontiers, receipts, and cancellation.
- Permitted action: Preserve exact scope/provenance reuse, catch-up, refresh receipts/frontiers/cancellation, and no duplicate ingestion.
- Associated check: Session sync scope/provenance and refresh begin/status/cancel/restart fixtures.

### 22. crates/tracedecay-session-runtime/src/session_sync/work.rs

- Status: `M`
- Original path (b3): `crates/tracedecay-session-runtime/src/session_sync/work.rs`
- Current path (head): `crates/tracedecay-session-runtime/src/session_sync/work.rs`
- Hunks (1): `@@ -558,0 +559,6 @@ impl SessionSyncProjectContext {`
- Classification: **shared session/refresh extension**
- Original behavior: The original session sync and temporal refresh state machines preserve scope, historical ingestion, frontiers, receipts, and cancellation.
- Permitted action: Preserve exact scope/provenance reuse, catch-up, refresh receipts/frontiers/cancellation, and no duplicate ingestion.
- Associated check: Session sync scope/provenance and refresh begin/status/cancel/restart fixtures.

### 23. crates/tracedecay-session-runtime/src/session_temporal_refresh_scheduler/history.rs

- Status: `M`
- Original path (b3): `crates/tracedecay-session-runtime/src/session_temporal_refresh_scheduler/history.rs`
- Current path (head): `crates/tracedecay-session-runtime/src/session_temporal_refresh_scheduler/history.rs`
- Hunks (4): `@@ -70,0 +71,5 @@ pub struct ProjectSessionHistoricalIngestor {`; `@@ -104,0 +110 @@ impl ProjectSessionHistoricalIngestor {`; `@@ -109,0 +116,10 @@ impl ProjectSessionHistoricalIngestor {`; `@@ -122,0 +139,4 @@ impl SessionHistoricalIngestor for ProjectSessionHistoricalIngestor {`
- Classification: **shared session/refresh extension**
- Original behavior: The original session sync and temporal refresh state machines preserve scope, historical ingestion, frontiers, receipts, and cancellation.
- Permitted action: Preserve exact scope/provenance reuse, catch-up, refresh receipts/frontiers/cancellation, and no duplicate ingestion.
- Associated check: Session sync scope/provenance and refresh begin/status/cancel/restart fixtures.

### 24. crates/tracedecay-sessions/src/admission/mod.rs

- Status: `M`
- Original path (b3): `crates/tracedecay-sessions/src/admission/mod.rs`
- Current path (head): `crates/tracedecay-sessions/src/admission/mod.rs`
- Hunks (1): `@@ -884,0 +885,18 @@ pub(crate) mod test_support {`
- Classification: **shared host admission/provenance extension**
- Original behavior: The original host admission and observation contracts settle scoped, sanitized evidence and preserve historical ingestion.
- Permitted action: Preserve exact provenance matching, sanitized diagnostics, scope, cancellation, and fail-open/deferral behavior; keep it outside the Native adapter.
- Associated check: Admission/provenance mismatch, fallback, cancellation, and historical catch-up fixtures.

### 25. crates/tracedecay-sessions/src/observation.rs

- Status: `M`
- Original path (b3): `crates/tracedecay-sessions/src/observation.rs`
- Current path (head): `crates/tracedecay-sessions/src/observation.rs`
- Hunks (7): `@@ -93,0 +94,2 @@ pub struct CaptureObservationRequest {`; `@@ -119,0 +122 @@ impl CaptureObservationRequest {`; `@@ -146,0 +150,10 @@ impl CaptureObservationRequest {`; `@@ -501,0 +515 @@ where`; `@@ -536,0 +551,5 @@ where`; `@@ -547,0 +567,10 @@ where`; `@@ -563,0 +593,6 @@ where`
- Classification: **shared host admission/provenance extension**
- Original behavior: The original host admission and observation contracts settle scoped, sanitized evidence and preserve historical ingestion.
- Permitted action: Preserve exact provenance matching, sanitized diagnostics, scope, cancellation, and fail-open/deferral behavior; keep it outside the Native adapter.
- Associated check: Admission/provenance mismatch, fallback, cancellation, and historical catch-up fixtures.

### 26. crates/tracedecay-sessions/src/observation_test.rs

- Status: `M`
- Original path (b3): `crates/tracedecay-sessions/src/observation_test.rs`
- Current path (head): `crates/tracedecay-sessions/src/observation_test.rs`
- Hunks (1): `@@ -251,0 +252,19 @@ impl ObservationStore for FakeStore {`
- Classification: **shared host admission/provenance extension**
- Original behavior: The original host admission and observation contracts settle scoped, sanitized evidence and preserve historical ingestion.
- Permitted action: Preserve exact provenance matching, sanitized diagnostics, scope, cancellation, and fail-open/deferral behavior; keep it outside the Native adapter.
- Associated check: Admission/provenance mismatch, fallback, cancellation, and historical catch-up fixtures.

### 27. crates/tracedecay-sessions/src/repository_provenance.rs

- Status: `M`
- Original path (b3): `crates/tracedecay-sessions/src/repository_provenance.rs`
- Current path (head): `crates/tracedecay-sessions/src/repository_provenance.rs`
- Hunks (5): `@@ -48,0 +49,19 @@ pub struct RepositoryProvenanceAdmissionContext {`; `@@ -79,0 +99,46 @@ pub struct CapturedRepositoryProvenanceV1 {`; `@@ -86,0 +152,81 @@ impl RepositoryProvenanceAdmissionContext {`; `@@ -103,0 +250 @@ impl RepositoryProvenanceAdmissionContext {`; `@@ -164,0 +312 @@ impl RepositoryProvenanceAdmissionContext {`
- Classification: **shared host admission/provenance extension**
- Original behavior: The original host admission and observation contracts settle scoped, sanitized evidence and preserve historical ingestion.
- Permitted action: Preserve exact provenance matching, sanitized diagnostics, scope, cancellation, and fail-open/deferral behavior; keep it outside the Native adapter.
- Associated check: Admission/provenance mismatch, fallback, cancellation, and historical catch-up fixtures.

### 28. crates/tracedecay-sessions/src/repository_provenance_test.rs

- Status: `M`
- Original path (b3): `crates/tracedecay-sessions/src/repository_provenance_test.rs`
- Current path (head): `crates/tracedecay-sessions/src/repository_provenance_test.rs`
- Hunks (1): `@@ -465,0 +466,158 @@ fn admission_capture_cache_reuses_exact_watermark_and_invalidates_on_index_chang`
- Classification: **shared host admission/provenance extension**
- Original behavior: The original host admission and observation contracts settle scoped, sanitized evidence and preserve historical ingestion.
- Permitted action: Preserve exact provenance matching, sanitized diagnostics, scope, cancellation, and fail-open/deferral behavior; keep it outside the Native adapter.
- Associated check: Admission/provenance mismatch, fallback, cancellation, and historical catch-up fixtures.

### 29. crates/tracedecay-sessions/src/runtime/hosts/codex.rs

- Status: `M`
- Original path (b3): `crates/tracedecay-sessions/src/runtime/hosts/codex.rs`
- Current path (head): `crates/tracedecay-sessions/src/runtime/hosts/codex.rs`
- Hunks (9): `@@ -106 +106 @@ pub use observation::{`; `@@ -110,0 +111 @@ pub use observation::{`; `@@ -1026,0 +1028,4 @@ const EXACT_HOOK_DISCOVERY_UNITS_PER_CALL: usize = 64;`; `@@ -1030 +1035 @@ const MAX_EXACT_HOOK_SESSION_REQUESTS: usize = 64;`; `@@ -1036,0 +1042,40 @@ pub(crate) struct CodexExactSessionLookupOutcome {`; `@@ -1220,0 +1266,248 @@ impl CodexSource {`; `@@ -1225 +1518 @@ impl CodexSource {`; `@@ -2433,0 +2727,54 @@ fn retained_scan_step(`; `@@ -2869,0 +3217,317 @@ impl TranscriptSource for CodexSource {`
- Classification: **shared Codex host extension**
- Original behavior: The original host route discovers bounded historical transcript sources and admits them under canonical session ownership.
- Permitted action: Preserve native IDs, cwd/root checks, bounds, generation/source replacement checks, deferral, privacy, and canonical cursor writes.
- Associated check: Codex live locator/sealed admission, ambiguity/rewrite/oversize/deadline, and catch-up tests.

### 30. crates/tracedecay-sessions/src/runtime/hosts/codex/meta.rs

- Status: `M`
- Original path (b3): `crates/tracedecay-sessions/src/runtime/hosts/codex/meta.rs`
- Current path (head): `crates/tracedecay-sessions/src/runtime/hosts/codex/meta.rs`
- Hunks (1): `@@ -78,0 +79,74 @@ pub(super) fn session_meta(path: &Path) -> Option<CodexMeta> {`
- Classification: **shared Codex host extension**
- Original behavior: The original host route discovers bounded historical transcript sources and admits them under canonical session ownership.
- Permitted action: Preserve native IDs, cwd/root checks, bounds, generation/source replacement checks, deferral, privacy, and canonical cursor writes.
- Associated check: Codex live locator/sealed admission, ambiguity/rewrite/oversize/deadline, and catch-up tests.

### 31. crates/tracedecay-sessions/src/runtime/hosts/codex/observation.rs

- Status: `M`
- Original path (b3): `crates/tracedecay-sessions/src/runtime/hosts/codex/observation.rs`
- Current path (head): `crates/tracedecay-sessions/src/runtime/hosts/codex/observation.rs`
- Hunks (15): `@@ -24,0 +25,2 @@ use tracedecay_store::observation::ObservationCoverageReason;`; `@@ -278,0 +281 @@ pub async fn try_admit_codex_jsonl_observations_for_project_with_admission(`; `@@ -289,0 +293 @@ pub async fn try_admit_codex_jsonl_observations_for_project_with_admission_and_c`; `@@ -298,0 +303 @@ pub async fn try_admit_codex_jsonl_observations_for_project_with_admission_and_c`; `@@ -373,0 +379 @@ pub(super) enum CodexObservationAdmission<'a> {`; `@@ -403 +409,10 @@ impl CodexObservationAdmission<'_> {`; `@@ -462,0 +478 @@ struct CodexAdmissionContext<'a> {`; `@@ -631,0 +648,44 @@ async fn try_admit_codex_jsonl_observations(`; `@@ -644 +704,4 @@ async fn try_admit_codex_jsonl_observations(`; `@@ -657,0 +721 @@ async fn try_admit_codex_jsonl_observations(`; `@@ -713,0 +778 @@ async fn admit_codex_jsonl_page(`; `@@ -738,0 +804,3 @@ async fn admit_codex_jsonl_page(`; `@@ -969,0 +1038 @@ mod replay_boundary_tests {`; `@@ -982,0 +1052 @@ mod replay_boundary_tests {`; `@@ -1036,0 +1107,121 @@ mod replay_boundary_tests {`
- Classification: **shared Codex host extension**
- Original behavior: The original host route discovers bounded historical transcript sources and admits them under canonical session ownership.
- Permitted action: Preserve native IDs, cwd/root checks, bounds, generation/source replacement checks, deferral, privacy, and canonical cursor writes.
- Associated check: Codex live locator/sealed admission, ambiguity/rewrite/oversize/deadline, and catch-up tests.

### 32. crates/tracedecay-sessions/src/runtime/hosts/codex/tests.rs

- Status: `M`
- Original path (b3): `crates/tracedecay-sessions/src/runtime/hosts/codex/tests.rs`
- Current path (head): `crates/tracedecay-sessions/src/runtime/hosts/codex/tests.rs`
- Hunks (2): `@@ -174,0 +175 @@ mod goal_event_tests {`; `@@ -180,0 +182 @@ mod goal_event_tests {`
- Classification: **shared Codex host extension**
- Original behavior: The original host route discovers bounded historical transcript sources and admits them under canonical session ownership.
- Permitted action: Preserve native IDs, cwd/root checks, bounds, generation/source replacement checks, deferral, privacy, and canonical cursor writes.
- Associated check: Codex live locator/sealed admission, ambiguity/rewrite/oversize/deadline, and catch-up tests.

### 33. crates/tracedecay-sessions/src/runtime/ingest.rs

- Status: `M`
- Original path (b3): `crates/tracedecay-sessions/src/runtime/ingest.rs`
- Current path (head): `crates/tracedecay-sessions/src/runtime/ingest.rs`
- Hunks (1): `@@ -9,0 +10,2 @@ mod user_provider;`
- Classification: **shared host admission/provenance extension**
- Original behavior: The original host admission and observation contracts settle scoped, sanitized evidence and preserve historical ingestion.
- Permitted action: Preserve exact provenance matching, sanitized diagnostics, scope, cancellation, and fail-open/deferral behavior; keep it outside the Native adapter.
- Associated check: Admission/provenance mismatch, fallback, cancellation, and historical catch-up fixtures.

### 34. crates/tracedecay-sessions/src/runtime/ingest/failure.rs

- Status: `M`
- Original path (b3): `crates/tracedecay-sessions/src/runtime/ingest/failure.rs`
- Current path (head): `crates/tracedecay-sessions/src/runtime/ingest/failure.rs`
- Hunks (2): `@@ -447,0 +448,18 @@ pub(super) fn warn_transcript_catch_up_failure(`; `@@ -450,0 +469,2 @@ pub(super) fn warn_transcript_catch_up_failure(`
- Classification: **shared host admission/provenance extension**
- Original behavior: The original host admission and observation contracts settle scoped, sanitized evidence and preserve historical ingestion.
- Permitted action: Preserve exact provenance matching, sanitized diagnostics, scope, cancellation, and fail-open/deferral behavior; keep it outside the Native adapter.
- Associated check: Admission/provenance mismatch, fallback, cancellation, and historical catch-up fixtures.

### 35. crates/tracedecay-sessions/src/runtime/ingest/project_provider.rs

- Status: `M`
- Original path (b3): `crates/tracedecay-sessions/src/runtime/ingest/project_provider.rs`
- Current path (head): `crates/tracedecay-sessions/src/runtime/ingest/project_provider.rs`
- Hunks (1): `@@ -277,0 +278 @@ impl<'a> ProjectProviderRun<'a> {`
- Classification: **shared host admission/provenance extension**
- Original behavior: The original host admission and observation contracts settle scoped, sanitized evidence and preserve historical ingestion.
- Permitted action: Preserve exact provenance matching, sanitized diagnostics, scope, cancellation, and fail-open/deferral behavior; keep it outside the Native adapter.
- Associated check: Admission/provenance mismatch, fallback, cancellation, and historical catch-up fixtures.

### 36. crates/tracedecay-sessions/src/runtime/ingest/tests.rs

- Status: `M`
- Original path (b3): `crates/tracedecay-sessions/src/runtime/ingest/tests.rs`
- Current path (head): `crates/tracedecay-sessions/src/runtime/ingest/tests.rs`
- Hunks (1): `@@ -66,0 +67 @@ async fn cancelled_codex_provider_stops_before_opening_the_next_jsonl_source() {`
- Classification: **shared host admission/provenance extension**
- Original behavior: The original host admission and observation contracts settle scoped, sanitized evidence and preserve historical ingestion.
- Permitted action: Preserve exact provenance matching, sanitized diagnostics, scope, cancellation, and fail-open/deferral behavior; keep it outside the Native adapter.
- Associated check: Admission/provenance mismatch, fallback, cancellation, and historical catch-up fixtures.

### 37. crates/tracedecay-sessions/src/runtime/observation/jsonl_observation_admission.rs

- Status: `M`
- Original path (b3): `crates/tracedecay-sessions/src/runtime/observation/jsonl_observation_admission.rs`
- Current path (head): `crates/tracedecay-sessions/src/runtime/observation/jsonl_observation_admission.rs`
- Hunks (7): `@@ -77,0 +78,135 @@ struct FlushPolicy<'policy> {`; `@@ -87,0 +223 @@ pub(in crate::runtime) struct JsonlObservationAdmissionRequest<'request> {`; `@@ -111,0 +248 @@ impl<'request> JsonlObservationAdmissionRequest<'request> {`; `@@ -136 +273,13 @@ impl<'request> JsonlObservationAdmissionRequest<'request> {`; `@@ -2138,0 +2288 @@ pub(in crate::runtime) async fn admit_jsonl_observations<State: Clone>(`; `@@ -2145,0 +2296,6 @@ pub(in crate::runtime) async fn admit_jsonl_observations<State: Clone>(`; `@@ -2206,0 +2363,44 @@ pub(in crate::runtime) async fn admit_jsonl_observations<State: Clone>(`
- Classification: **shared sealed ingest/cursor extension**
- Original behavior: The original source/ingest route owns bounded parsing, source identity, canonical cursors, exact CAS, cancellation, and no-write deferral.
- Permitted action: Preserve raw protection, scope, source replacement checks, checked cursor semantics, cancellation, and atomic storage; do not create a parallel Native stream.
- Associated check: Sealed admission/source replacement, cursor round-trip/reserved-frontier/CAS, and locator fixtures.

### 38. crates/tracedecay-sessions/src/runtime/observation/jsonl_observation_admission/tests.rs

- Status: `M`
- Original path (b3): `crates/tracedecay-sessions/src/runtime/observation/jsonl_observation_admission/tests.rs`
- Current path (head): `crates/tracedecay-sessions/src/runtime/observation/jsonl_observation_admission/tests.rs`
- Hunks (1): `@@ -1741,0 +1742,234 @@ async fn exact_hook_prepares_an_in_scope_window_concurrently() {`
- Classification: **shared sealed ingest/cursor extension**
- Original behavior: The original source/ingest route owns bounded parsing, source identity, canonical cursors, exact CAS, cancellation, and no-write deferral.
- Permitted action: Preserve raw protection, scope, source replacement checks, checked cursor semantics, cancellation, and atomic storage; do not create a parallel Native stream.
- Associated check: Sealed admission/source replacement, cursor round-trip/reserved-frontier/CAS, and locator fixtures.

### 39. crates/tracedecay-sessions/src/runtime/source.rs

- Status: `M`
- Original path (b3): `crates/tracedecay-sessions/src/runtime/source.rs`
- Current path (head): `crates/tracedecay-sessions/src/runtime/source.rs`
- Hunks (5): `@@ -903,3 +903,4 @@ pub use jsonl::{`; `@@ -1105 +1106 @@ fn should_resume_jsonl(prev: StoredCursor, file_size: u64, mtime: u64, file_id:`; `@@ -1108,0 +1110,35 @@ fn stable_jsonl_file_id(`; `@@ -1135 +1170,0 @@ fn stable_jsonl_file_id(`; `@@ -1140 +1175 @@ fn stable_jsonl_file_id(`
- Classification: **shared sealed ingest/cursor extension**
- Original behavior: The original source/ingest route owns bounded parsing, source identity, canonical cursors, exact CAS, cancellation, and no-write deferral.
- Permitted action: Preserve raw protection, scope, source replacement checks, checked cursor semantics, cancellation, and atomic storage; do not create a parallel Native stream.
- Associated check: Sealed admission/source replacement, cursor round-trip/reserved-frontier/CAS, and locator fixtures.

### 40. crates/tracedecay-sessions/src/runtime/source/jsonl.rs

- Status: `M`
- Original path (b3): `crates/tracedecay-sessions/src/runtime/source/jsonl.rs`
- Current path (head): `crates/tracedecay-sessions/src/runtime/source/jsonl.rs`
- Hunks (4): `@@ -19,3 +19,3 @@ use super::{`; `@@ -156,0 +157,42 @@ pub(in crate::runtime) fn jsonl_native_file_identity(`; `@@ -542,0 +585,229 @@ pub struct JsonlResumeState {`; `@@ -1855,0 +2127,297 @@ mod tests {`
- Classification: **shared sealed ingest/cursor extension**
- Original behavior: The original source/ingest route owns bounded parsing, source identity, canonical cursors, exact CAS, cancellation, and no-write deferral.
- Permitted action: Preserve raw protection, scope, source replacement checks, checked cursor semantics, cancellation, and atomic storage; do not create a parallel Native stream.
- Associated check: Sealed admission/source replacement, cursor round-trip/reserved-frontier/CAS, and locator fixtures.

### 41. crates/tracedecay-sessions/src/runtime/store_access/transcript.rs

- Status: `M`
- Original path (b3): `crates/tracedecay-sessions/src/runtime/store_access/transcript.rs`
- Current path (head): `crates/tracedecay-sessions/src/runtime/store_access/transcript.rs`
- Hunks (11): `@@ -11 +11,3 @@ use super::super::git_correlation::{`; `@@ -138,0 +141 @@ pub async fn get_parse_offset(`; `@@ -154,2 +157,2 @@ pub async fn get_parse_offset(`; `@@ -176,2 +179,2 @@ pub async fn get_parse_offset(`; `@@ -197,8 +200,62 @@ fn sqlite_missing_column(error: &tracedecay_runtime_core::db::engine::Error, col`; `@@ -246,0 +304 @@ pub async fn set_parse_offset(`; `@@ -256,2 +314,2 @@ pub async fn set_parse_offset(`; `@@ -275,0 +334,63 @@ impl<D: SessionRegisteredDb + Sync> SessionStoreAccess<'_, D> {`; `@@ -860,0 +982,4 @@ impl<D: SessionRegisteredDb + Sync> SessionStoreAccess<'_, D> {`; `@@ -874,2 +999,6 @@ impl<D: SessionRegisteredDb + Sync> SessionStoreAccess<'_, D> {`; `@@ -981,0 +1111,28 @@ mod tests {`
- Classification: **shared sealed ingest/cursor extension**
- Original behavior: The original source/ingest route owns bounded parsing, source identity, canonical cursors, exact CAS, cancellation, and no-write deferral.
- Permitted action: Preserve raw protection, scope, source replacement checks, checked cursor semantics, cancellation, and atomic storage; do not create a parallel Native stream.
- Associated check: Sealed admission/source replacement, cursor round-trip/reserved-frontier/CAS, and locator fixtures.

### 42. crates/tracedecay-store-runtime/src/retained_memory.rs

- Status: `M`
- Original path (b3): `crates/tracedecay-store-runtime/src/retained_memory.rs`
- Current path (head): `crates/tracedecay-store-runtime/src/retained_memory.rs`
- Hunks (1): `@@ -1048 +1048 @@ async fn execute_status_on_db(`
- Classification: **shared retained-route extension**
- Original behavior: The original retained route selects the owner-bound Native application and executes canonical operations.
- Permitted action: Preserve operation bodies and ownership; visibility-only wiring must not introduce a second store or alter semantics.
- Associated check: Direct retained route and canonical operation parity fixtures.

### 43. crates/tracedecay/src/daemon/project_composition.rs

- Status: `M`
- Original path (b3): `crates/tracedecay/src/daemon/project_composition.rs`
- Current path (head): `crates/tracedecay/src/daemon/project_composition.rs`
- Hunks (26): `@@ -19,0 +20,7 @@ use tracedecay_session_runtime::session_temporal_refresh_scheduler::{`; `@@ -22,0 +30,2 @@ mod future_size_tests;`; `@@ -28,0 +38,4 @@ use code_index_activation::{`; `@@ -43,0 +57,447 @@ pub(super) struct ProductionProjectComposition {`; `@@ -252,0 +713 @@ struct ProjectOpenInputs<'a> {`; `@@ -281,0 +743,43 @@ pub(super) async fn production_project_server(`; `@@ -292,0 +797 @@ pub(super) async fn production_project_server(`; `@@ -315,0 +821,2 @@ pub(super) async fn production_project_server(`; `@@ -317 +824,7 @@ pub(super) async fn production_project_server(`; `@@ -319 +832,6 @@ pub(super) async fn production_project_server(`; `@@ -326,0 +845,2 @@ pub(super) async fn production_project_server(`; `@@ -483,0 +1004,6 @@ struct ComposedCoreServer {`; `@@ -539,0 +1066,8 @@ impl ComposedCoreServer {`; `@@ -573,0 +1108,10 @@ struct PublishedFullServer {`; `@@ -756,0 +1301,3 @@ impl ProjectOpenInputs<'_> {`; `@@ -807,0 +1355,94 @@ impl ProjectOpenInputs<'_> {`; `@@ -891,0 +1533,6 @@ impl ProjectOpenInputs<'_> {`; `@@ -1152,0 +1800,27 @@ impl ProjectOpenInputs<'_> {`; `@@ -1191,0 +1866,200 @@ impl ProjectOpenInputs<'_> {`; `@@ -1240,9 +2114,12 @@ impl ProjectOpenInputs<'_> {`; `@@ -1267,15 +2144,19 @@ impl ProjectOpenInputs<'_> {`; `@@ -1375,0 +2257,7 @@ impl ProjectOpenInputs<'_> {`; `@@ -1406,0 +2295,2 @@ impl ProjectOpenInputs<'_> {`; `@@ -1531,0 +2422,3 @@ impl ProjectOpenInputs<'_> {`; `@@ -1533,0 +2427,25 @@ impl ProjectOpenInputs<'_> {`; `@@ -2120,0 +3039,192 @@ async fn retire_failed_project_open_owner(`
- Classification: **product composition**
- Original behavior: The existing composition root opens the owner-bound Native/session/LCM services and owns project-open lifecycle; the current delta adds provider selection/control and test interposition around that root.
- Permitted action: Preserve project-open ownership and canonical Native/session/LCM composition while keeping provider selection/lifecycle in this narrow root, removing staged Native construction, and binding Native to canonical owner-bound services.
- Associated check: Selected Native composition, one canonical delivery, restart, scope, and NCM coexistence journeys.

### 44. crates/tracedecay/src/daemon/retained_owner/claude_host_journey_tests.rs

- Status: `A`
- Original path (b3): `absent at b3`
- Current path (head): `crates/tracedecay/src/daemon/retained_owner/claude_host_journey_tests.rs`
- Hunks (1): `@@ -0,0 +1,1207 @@`
- Classification: **product host journey tests**
- Original behavior: No b3 counterpart; tests cover product host lifecycle and canonical admission/provider delivery.
- Permitted action: Keep host-neutral admission, fail-open behavior, exact scope, canonical receipts, and no provider persistence authority.
- Associated check: Real Claude lifecycle acceptance with idempotence, rollback, and attribution.

### 45. crates/tracedecay/src/daemon/retained_owner/cognitive_recall.rs

- Status: `A`
- Original path (b3): `absent at b3`
- Current path (head): `crates/tracedecay/src/daemon/retained_owner/cognitive_recall.rs`
- Hunks (1): `@@ -0,0 +1,9055 @@`
- Classification: **product provider context**
- Original behavior: No b3 counterpart; this layer coordinates provider-neutral advisory contributions and host delivery.
- Permitted action: Keep provider selection above canonical facts/session/LCM, enforce one canonical delivery marker, and avoid a second Native advisory invocation.
- Associated check: Exactly one canonical memory_matches for selected Native, zero extra Native provider calls, and NCM/injected-provider delivery.

### 46. crates/tracedecay/src/daemon/retained_owner/cognitive_recall/control_attribution.rs

- Status: `A`
- Original path (b3): `absent at b3`
- Current path (head): `crates/tracedecay/src/daemon/retained_owner/cognitive_recall/control_attribution.rs`
- Hunks (1): `@@ -0,0 +1,1280 @@`
- Classification: **product provider context**
- Original behavior: No b3 counterpart; this layer coordinates provider-neutral advisory contributions and host delivery.
- Permitted action: Keep provider selection above canonical facts/session/LCM, enforce one canonical delivery marker, and avoid a second Native advisory invocation.
- Associated check: Exactly one canonical memory_matches for selected Native, zero extra Native provider calls, and NCM/injected-provider delivery.

### 47. crates/tracedecay/src/daemon/retained_owner/cognitive_recall/test_context_evidence.rs

- Status: `A`
- Original path (b3): `absent at b3`
- Current path (head): `crates/tracedecay/src/daemon/retained_owner/cognitive_recall/test_context_evidence.rs`
- Hunks (1): `@@ -0,0 +1,653 @@`
- Classification: **product provider context**
- Original behavior: No b3 counterpart; this layer coordinates provider-neutral advisory contributions and host delivery.
- Permitted action: Keep provider selection above canonical facts/session/LCM, enforce one canonical delivery marker, and avoid a second Native advisory invocation.
- Associated check: Exactly one canonical memory_matches for selected Native, zero extra Native provider calls, and NCM/injected-provider delivery.

### 48. crates/tracedecay/src/daemon/retained_owner/native_baseline_tests.rs

- Status: `A`
- Original path (b3): `absent at b3`
- Current path (head): `crates/tracedecay/src/daemon/retained_owner/native_baseline_tests.rs`
- Hunks (1): `@@ -0,0 +1,367 @@`
- Classification: **product Native adapter/tests**
- Original behavior: No b3 counterpart; added adapter/test surfaces describe the product Native/provider envelope and provisional staged behavior.
- Permitted action: Delegate supported operations to the complete owner-bound original implementation, retain typed routes, and return typed unsupported for unmatched generic controls.
- Associated check: Independent b3-versus-product fact/session/LCM operation and effect fixtures.

### 49. crates/tracedecay/src/daemon/retained_owner/native_common_factory_tests.rs

- Status: `A`
- Original path (b3): `absent at b3`
- Current path (head): `crates/tracedecay/src/daemon/retained_owner/native_common_factory_tests.rs`
- Hunks (1): `@@ -0,0 +1,2842 @@`
- Classification: **product Native adapter/tests**
- Original behavior: No b3 counterpart; added adapter/test surfaces describe the product Native/provider envelope and provisional staged behavior.
- Permitted action: Delegate supported operations to the complete owner-bound original implementation, retain typed routes, and return typed unsupported for unmatched generic controls.
- Associated check: Independent b3-versus-product fact/session/LCM operation and effect fixtures.

### 50. crates/tracedecay/src/daemon/retained_owner/native_common_tests.rs

- Status: `A`
- Original path (b3): `absent at b3`
- Current path (head): `crates/tracedecay/src/daemon/retained_owner/native_common_tests.rs`
- Hunks (1): `@@ -0,0 +1,3376 @@`
- Classification: **product Native adapter/tests**
- Original behavior: No b3 counterpart; added adapter/test surfaces describe the product Native/provider envelope and provisional staged behavior.
- Permitted action: Delegate supported operations to the complete owner-bound original implementation, retain typed routes, and return typed unsupported for unmatched generic controls.
- Associated check: Independent b3-versus-product fact/session/LCM operation and effect fixtures.

### 51. crates/tracedecay/src/daemon/retained_owner/native_provider.rs

- Status: `A`
- Original path (b3): `absent at b3`
- Current path (head): `crates/tracedecay/src/daemon/retained_owner/native_provider.rs`
- Hunks (1): `@@ -0,0 +1,4080 @@`
- Classification: **product Native adapter/tests**
- Original behavior: No b3 counterpart; added adapter/test surfaces describe the product Native/provider envelope and provisional staged behavior.
- Permitted action: Delegate supported operations to the complete owner-bound original implementation, retain typed routes, and return typed unsupported for unmatched generic controls.
- Associated check: Independent b3-versus-product fact/session/LCM operation and effect fixtures.

### 52. crates/tracedecay/src/daemon/retained_owner/native_provider_parity_tests.rs

- Status: `A`
- Original path (b3): `absent at b3`
- Current path (head): `crates/tracedecay/src/daemon/retained_owner/native_provider_parity_tests.rs`
- Hunks (1): `@@ -0,0 +1,596 @@`
- Classification: **product Native adapter/tests**
- Original behavior: No b3 counterpart; added adapter/test surfaces describe the product Native/provider envelope and provisional staged behavior.
- Permitted action: Delegate supported operations to the complete owner-bound original implementation, retain typed routes, and return typed unsupported for unmatched generic controls.
- Associated check: Independent b3-versus-product fact/session/LCM operation and effect fixtures.

### 53. crates/tracedecay/src/daemon/retained_owner/native_provider_tests.rs

- Status: `A`
- Original path (b3): `absent at b3`
- Current path (head): `crates/tracedecay/src/daemon/retained_owner/native_provider_tests.rs`
- Hunks (1): `@@ -0,0 +1,3251 @@`
- Classification: **product Native adapter/tests**
- Original behavior: No b3 counterpart; added adapter/test surfaces describe the product Native/provider envelope and provisional staged behavior.
- Permitted action: Delegate supported operations to the complete owner-bound original implementation, retain typed routes, and return typed unsupported for unmatched generic controls.
- Associated check: Independent b3-versus-product fact/session/LCM operation and effect fixtures.

### 54. crates/tracedecay/src/daemon/retained_owner/native_staged_observations.rs

- Status: `A`
- Original path (b3): `absent at b3`
- Current path (head): `crates/tracedecay/src/daemon/retained_owner/native_staged_observations.rs`
- Hunks (1): `@@ -0,0 +1,5677 @@`
- Classification: **product staged substitute**
- Original behavior: No b3 counterpart; the added provider-local store owns staged observations, custom scoring, generations, receipts, and lifecycle state.
- Permitted action: Retire all Native construction/read/write paths and leave its existing database and sidecars byte-for-byte untouched; do not migrate, replay, or reset rows.
- Associated check: Stale-file presence/byte-preservation and Native canonical behavior fixtures.

### 55. crates/tracedecay/src/daemon/retained_owner/observation_journey.rs

- Status: `A`
- Original path (b3): `absent at b3`
- Current path (head): `crates/tracedecay/src/daemon/retained_owner/observation_journey.rs`
- Hunks (1): `@@ -0,0 +1,11236 @@`
- Classification: **product observer host**
- Original behavior: No b3 counterpart; the product observation journey coordinates canonical settlement, provider fan-out, lifecycle, and NCM observation.
- Permitted action: Keep canonical session/LCM/journal ownership and separate provider namespaces; Native consumes settled host services.
- Associated check: Host delivery, crash/restart, provider history, and NCM observer journeys.

### 56. crates/tracedecay/src/daemon/retained_owner/observation_journey/control_dispatch.rs

- Status: `A`
- Original path (b3): `absent at b3`
- Current path (head): `crates/tracedecay/src/daemon/retained_owner/observation_journey/control_dispatch.rs`
- Hunks (1): `@@ -0,0 +1,560 @@`
- Classification: **product observer host**
- Original behavior: No b3 counterpart; the product observation journey coordinates canonical settlement, provider fan-out, lifecycle, and NCM observation.
- Permitted action: Keep canonical session/LCM/journal ownership and separate provider namespaces; Native consumes settled host services.
- Associated check: Host delivery, crash/restart, provider history, and NCM observer journeys.

### 57. crates/tracedecay/src/daemon/retained_owner/observation_journey/tests/control_dispatch.rs

- Status: `A`
- Original path (b3): `absent at b3`
- Current path (head): `crates/tracedecay/src/daemon/retained_owner/observation_journey/tests/control_dispatch.rs`
- Hunks (1): `@@ -0,0 +1,410 @@`
- Classification: **product observer host**
- Original behavior: No b3 counterpart; the product observation journey coordinates canonical settlement, provider fan-out, lifecycle, and NCM observation.
- Permitted action: Keep canonical session/LCM/journal ownership and separate provider namespaces; Native consumes settled host services.
- Associated check: Host delivery, crash/restart, provider history, and NCM observer journeys.

### 58. crates/tracedecay/src/daemon/retained_owner/observation_journey/tests/crash_restart_fuzz.rs

- Status: `A`
- Original path (b3): `absent at b3`
- Current path (head): `crates/tracedecay/src/daemon/retained_owner/observation_journey/tests/crash_restart_fuzz.rs`
- Hunks (1): `@@ -0,0 +1,2473 @@`
- Classification: **product observer host**
- Original behavior: No b3 counterpart; the product observation journey coordinates canonical settlement, provider fan-out, lifecycle, and NCM observation.
- Permitted action: Keep canonical session/LCM/journal ownership and separate provider namespaces; Native consumes settled host services.
- Associated check: Host delivery, crash/restart, provider history, and NCM observer journeys.

### 59. crates/tracedecay/src/daemon/retained_owner/observation_journey/tests/provider_history.rs

- Status: `A`
- Original path (b3): `absent at b3`
- Current path (head): `crates/tracedecay/src/daemon/retained_owner/observation_journey/tests/provider_history.rs`
- Hunks (1): `@@ -0,0 +1,1551 @@`
- Classification: **product observer host**
- Original behavior: No b3 counterpart; the product observation journey coordinates canonical settlement, provider fan-out, lifecycle, and NCM observation.
- Permitted action: Keep canonical session/LCM/journal ownership and separate provider namespaces; Native consumes settled host services.
- Associated check: Host delivery, crash/restart, provider history, and NCM observer journeys.

### 60. crates/tracedecay/src/daemon/retained_owner/observation_journey/tests/real_ncm_observer.rs

- Status: `A`
- Original path (b3): `absent at b3`
- Current path (head): `crates/tracedecay/src/daemon/retained_owner/observation_journey/tests/real_ncm_observer.rs`
- Hunks (1): `@@ -0,0 +1,468 @@`
- Classification: **product observer host**
- Original behavior: No b3 counterpart; the product observation journey coordinates canonical settlement, provider fan-out, lifecycle, and NCM observation.
- Permitted action: Keep canonical session/LCM/journal ownership and separate provider namespaces; Native consumes settled host services.
- Associated check: Host delivery, crash/restart, provider history, and NCM observer journeys.

### 61. crates/tracedecay/src/daemon/retained_owner/provider_control.rs

- Status: `A`
- Original path (b3): `absent at b3`
- Current path (head): `crates/tracedecay/src/daemon/retained_owner/provider_control.rs`
- Hunks (1): `@@ -0,0 +1,1123 @@`
- Classification: **product provider controls**
- Original behavior: No b3 counterpart; these surfaces define provider-local control, feedback, portability, projection, source/state, and history receipts.
- Permitted action: Keep controls advisory/provider-local; generic success cannot stand in for an original Native operation or canonical receipt.
- Associated check: Provider control receipt, denial, cancellation, replay, and restart fixtures.

### 62. crates/tracedecay/src/daemon/retained_owner/provider_control/authority.rs

- Status: `A`
- Original path (b3): `absent at b3`
- Current path (head): `crates/tracedecay/src/daemon/retained_owner/provider_control/authority.rs`
- Hunks (1): `@@ -0,0 +1,843 @@`
- Classification: **product provider controls**
- Original behavior: No b3 counterpart; these surfaces define provider-local control, feedback, portability, projection, source/state, and history receipts.
- Permitted action: Keep controls advisory/provider-local; generic success cannot stand in for an original Native operation or canonical receipt.
- Associated check: Provider control receipt, denial, cancellation, replay, and restart fixtures.

### 63. crates/tracedecay/src/daemon/retained_owner/provider_control/feedback_receipt.rs

- Status: `A`
- Original path (b3): `absent at b3`
- Current path (head): `crates/tracedecay/src/daemon/retained_owner/provider_control/feedback_receipt.rs`
- Hunks (1): `@@ -0,0 +1,2087 @@`
- Classification: **product provider controls**
- Original behavior: No b3 counterpart; these surfaces define provider-local control, feedback, portability, projection, source/state, and history receipts.
- Permitted action: Keep controls advisory/provider-local; generic success cannot stand in for an original Native operation or canonical receipt.
- Associated check: Provider control receipt, denial, cancellation, replay, and restart fixtures.

### 64. crates/tracedecay/src/daemon/retained_owner/provider_control/outcome.rs

- Status: `A`
- Original path (b3): `absent at b3`
- Current path (head): `crates/tracedecay/src/daemon/retained_owner/provider_control/outcome.rs`
- Hunks (1): `@@ -0,0 +1,1453 @@`
- Classification: **product provider controls**
- Original behavior: No b3 counterpart; these surfaces define provider-local control, feedback, portability, projection, source/state, and history receipts.
- Permitted action: Keep controls advisory/provider-local; generic success cannot stand in for an original Native operation or canonical receipt.
- Associated check: Provider control receipt, denial, cancellation, replay, and restart fixtures.

### 65. crates/tracedecay/src/daemon/retained_owner/provider_control/portability.rs

- Status: `A`
- Original path (b3): `absent at b3`
- Current path (head): `crates/tracedecay/src/daemon/retained_owner/provider_control/portability.rs`
- Hunks (1): `@@ -0,0 +1,1981 @@`
- Classification: **product provider controls**
- Original behavior: No b3 counterpart; these surfaces define provider-local control, feedback, portability, projection, source/state, and history receipts.
- Permitted action: Keep controls advisory/provider-local; generic success cannot stand in for an original Native operation or canonical receipt.
- Associated check: Provider control receipt, denial, cancellation, replay, and restart fixtures.

### 66. crates/tracedecay/src/daemon/retained_owner/provider_control/projection.rs

- Status: `A`
- Original path (b3): `absent at b3`
- Current path (head): `crates/tracedecay/src/daemon/retained_owner/provider_control/projection.rs`
- Hunks (1): `@@ -0,0 +1,2675 @@`
- Classification: **product provider controls**
- Original behavior: No b3 counterpart; these surfaces define provider-local control, feedback, portability, projection, source/state, and history receipts.
- Permitted action: Keep controls advisory/provider-local; generic success cannot stand in for an original Native operation or canonical receipt.
- Associated check: Provider control receipt, denial, cancellation, replay, and restart fixtures.

### 67. crates/tracedecay/src/daemon/retained_owner/provider_control/source_controls.rs

- Status: `A`
- Original path (b3): `absent at b3`
- Current path (head): `crates/tracedecay/src/daemon/retained_owner/provider_control/source_controls.rs`
- Hunks (1): `@@ -0,0 +1,891 @@`
- Classification: **product provider controls**
- Original behavior: No b3 counterpart; these surfaces define provider-local control, feedback, portability, projection, source/state, and history receipts.
- Permitted action: Keep controls advisory/provider-local; generic success cannot stand in for an original Native operation or canonical receipt.
- Associated check: Provider control receipt, denial, cancellation, replay, and restart fixtures.

### 68. crates/tracedecay/src/daemon/retained_owner/provider_control/state_controls.rs

- Status: `A`
- Original path (b3): `absent at b3`
- Current path (head): `crates/tracedecay/src/daemon/retained_owner/provider_control/state_controls.rs`
- Hunks (1): `@@ -0,0 +1,312 @@`
- Classification: **product provider controls**
- Original behavior: No b3 counterpart; these surfaces define provider-local control, feedback, portability, projection, source/state, and history receipts.
- Permitted action: Keep controls advisory/provider-local; generic success cannot stand in for an original Native operation or canonical receipt.
- Associated check: Provider control receipt, denial, cancellation, replay, and restart fixtures.

### 69. crates/tracedecay/src/daemon/retained_owner/provider_control/tests/source_controls.rs

- Status: `A`
- Original path (b3): `absent at b3`
- Current path (head): `crates/tracedecay/src/daemon/retained_owner/provider_control/tests/source_controls.rs`
- Hunks (1): `@@ -0,0 +1,1105 @@`
- Classification: **product provider controls**
- Original behavior: No b3 counterpart; these surfaces define provider-local control, feedback, portability, projection, source/state, and history receipts.
- Permitted action: Keep controls advisory/provider-local; generic success cannot stand in for an original Native operation or canonical receipt.
- Associated check: Provider control receipt, denial, cancellation, replay, and restart fixtures.

### 70. crates/tracedecay/src/daemon/retained_owner/provider_history.rs

- Status: `A`
- Original path (b3): `absent at b3`
- Current path (head): `crates/tracedecay/src/daemon/retained_owner/provider_history.rs`
- Hunks (1): `@@ -0,0 +1,2111 @@`
- Classification: **product provider controls**
- Original behavior: No b3 counterpart; these surfaces define provider-local control, feedback, portability, projection, source/state, and history receipts.
- Permitted action: Keep controls advisory/provider-local; generic success cannot stand in for an original Native operation or canonical receipt.
- Associated check: Provider control receipt, denial, cancellation, replay, and restart fixtures.

### 71. crates/tracedecay-contracts/src/retained_surfaces.rs

- Status: `M`
- Original path (b3): `crates/tracedecay-contracts/src/retained_surfaces.rs`
- Current path (head): `crates/tracedecay-contracts/src/retained_surfaces.rs`
- Hunks (12): `@@ -31,0 +32 @@ mod memory;`; `@@ -37,0 +39 @@ pub use evidence::*;`; `@@ -57,0 +60,9 @@ pub enum RetainedSurfaceOperation {`; `@@ -86,0 +98 @@ pub enum SdkResultSemanticsV1 {`; `@@ -105 +117 @@ impl RetainedSurfaceOperation {`; `@@ -119,0 +132,9 @@ impl RetainedSurfaceOperation {`; `@@ -138 +159 @@ impl RetainedSurfaceOperation {`; `@@ -142 +163 @@ impl RetainedSurfaceOperation {`; `@@ -151,0 +173,36 @@ impl RetainedSurfaceOperation {`; `@@ -172,0 +230,9 @@ impl RetainedSurfaceOperation {`; `@@ -229,0 +296 @@ fn surface_specs() -> Vec<&'static RetainedSurfaceSpec> {`; `@@ -518,0 +586,63 @@ fn retained_surface_executable_schemas(`
- Classification: **shared/product retained contract extension**
- Original behavior: The existing retained catalog defines the typed Native application operations, result semantics, public bindings, and executable schemas.
- Permitted action: Preserve every original typed Native descriptor, schema, effect, and terminal meaning; provider-control additions remain explicit extensions and generic unsupported controls must fail closed.
- Associated check: Retained catalog, schema, SDK descriptor, typed-operation, and capability-unsupported contract fixtures.

### 72. crates/tracedecay-maintenance/src/generation.rs

- Status: `M`
- Original path (b3): `crates/tracedecay-maintenance/src/generation.rs`
- Current path (head): `crates/tracedecay-maintenance/src/generation.rs`
- Hunks (2): `@@ -21,4 +21,4 @@ use tracedecay_contracts::storage::compaction::CompactionThresholdConfig;`; `@@ -71 +71,2 @@ pub async fn run_project_generation_maintenance(`
- Classification: **shared maintenance safety extension**
- Original behavior: The original project-generation maintenance runner combines compaction and code-generation/vector retention outcomes into bounded continuation work.
- Permitted action: Preserve the `SemanticUnseated` no-authority handling and its quiet deferral semantics; this code-generation safety extension is outside the Native memory/LCM lifecycle and must not be used to justify changes to those authorities.
- Associated check: Generation maintenance, semantic-unseated, continuation, and compaction/vector-retention regression fixtures.

### 73. crates/tracedecay-maintenance/src/store_maintenance/mod.rs

- Status: `M`
- Original path (b3): `crates/tracedecay-maintenance/src/store_maintenance/mod.rs`
- Current path (head): `crates/tracedecay-maintenance/src/store_maintenance/mod.rs`
- Hunks (12): `@@ -24,0 +25,9 @@ use graph_replay::{defer_graph_replay_pool_busy, log_code_generation_retention_d`; `@@ -250,10 +259,8 @@ fn log_semantic_vector_retention_degraded(`; `@@ -426,0 +434,3 @@ pub enum CodeGenerationRetentionOutcomeV1 {`; `@@ -438,12 +448,5 @@ pub enum CodeGenerationRetentionOutcomeV1 {`; `@@ -494,27 +496,0 @@ pub async fn run_code_generation_retention(`; `@@ -557,17 +533,4 @@ pub async fn apply_code_generation_retention(`; `@@ -576,4 +539,3 @@ pub async fn apply_code_generation_retention(`; `@@ -708,3 +670,2 @@ pub async fn apply_code_generation_retention(`; `@@ -843 +804 @@ pub async fn apply_code_generation_retention(`; `@@ -845 +806 @@ pub async fn apply_code_generation_retention(`; `@@ -863,0 +825,3 @@ pub async fn apply_code_generation_retention(`; `@@ -874 +837,0 @@ pub async fn apply_code_generation_retention(`
- Classification: **shared maintenance safety extension**
- Original behavior: The original maintenance authority repairs/replays stores and applies bounded code-generation/vector retention under authoritative pins and publication fences.
- Permitted action: Preserve test-only replay helpers, `SemanticUnseated`/offline deletion deferral, the removal of the offline serving-pin sweep, and the writer freeze through blocking deletion; keep this shared maintenance safety behavior separate from Native memory and LCM lifecycle parity.
- Associated check: Code-generation retention with online, unseated, offline, census-scanning, refused, reset, and blocking-deletion/vector-writer fixtures.

### 74. crates/tracedecay-privacy/src/detector_kernel.rs

- Status: `M`
- Original path (b3): `crates/tracedecay-privacy/src/detector_kernel.rs`
- Current path (head): `crates/tracedecay-privacy/src/detector_kernel.rs`
- Hunks (2): `@@ -247,0 +248,17 @@ pub fn looks_high_entropy_token(token: &str) -> bool {`; `@@ -405,0 +423,44 @@ mod tests {`
- Classification: **escalated shared privacy algorithm delta**
- Original behavior: The shared privacy kernel classifies high-entropy tokens and computes redaction ranges for credential and payload sanitization.
- Permitted action: Do not treat Native memory or LCM callers as unchanged. The current implementation peels one exact `-sha256-<64 lowercase hex>` suffix before the original entropy predicate; restore that algorithm exactly or isolate the change behind a reviewed product boundary before accepting the affected lane.
- Associated check: Detector-kernel exact/malformed structural-digest tests, Native memory hygiene and `sanitize_memory_fact_payload` privacy tests, and LCM raw/DAG sanitization tests.

### 75. crates/tracedecay-store-runtime/src/session_registry/mounts.rs

- Status: `M`
- Original path (b3): `crates/tracedecay-store-runtime/src/session_registry/mounts.rs`
- Current path (head): `crates/tracedecay-store-runtime/src/session_registry/mounts.rs`
- Hunks (1): `@@ -115 +115,5 @@ impl DaemonSessionRuntimeRegistryV1 {`
- Classification: **shared runtime-composition seam**
- Original behavior: The session runtime registry opens the owner-bound session-maintenance authority and returns the assembled registry.
- Permitted action: Preserve registry identity, maintenance policy, errors, and lifecycle; the boxed bootstrap future is only a future-size/composition seam and must not alter opening or ownership semantics.
- Associated check: Registry-open, session-maintenance, future-size, and restart fixtures.

### 76. crates/tracedecay/src/daemon/retained_owner.rs

- Status: `M`
- Original path (b3): `crates/tracedecay/src/daemon/retained_owner.rs`
- Current path (head): `crates/tracedecay/src/daemon/retained_owner.rs`
- Hunks (3): `@@ -25,0 +26,24 @@ use crate::tracedecay::TraceDecay;`; `@@ -50,0 +75,4 @@ pub(crate) struct ProductionRetainedAuthoritiesV1 {`; `@@ -127,0 +156,4 @@ pub(crate) fn retained_surface_ports(`
- Classification: **shared retained-owner composition extension**
- Original behavior: The retained owner composes the existing owner-bound Native/session/LCM authorities and exposes their retained application ports.
- Permitted action: Preserve canonical owner wiring and typed retained routes; provider-control wiring may be mounted only as a narrow composition extension and cannot create a second Native owner or store.
- Associated check: Retained-owner construction, provider-control mount, direct Native route, restart, scope, and NCM coexistence fixtures.

### 77. crates/tracedecay/src/mcp/tools/handlers/hook_runtime/admission.rs

- Status: `M`
- Original path (b3): `crates/tracedecay/src/mcp/tools/handlers/hook_runtime/admission.rs`
- Current path (head): `crates/tracedecay/src/mcp/tools/handlers/hook_runtime/admission.rs`
- Hunks (9): `@@ -21,0 +22 @@ use super::envelope::{`; `@@ -90,0 +92,31 @@ fn hook_v2_admission_ledgers() -> &'static StdMutex<HookV2AdmissionLedgers> {`; `@@ -276,2 +308,11 @@ pub(crate) async fn admit_hook_v2_envelope(`; `@@ -287,0 +329 @@ async fn admit_hook_v2_envelope_with_lifecycle(`; `@@ -313,0 +356,23 @@ async fn admit_hook_v2_envelope_with_lifecycle(`; `@@ -488,0 +554 @@ pub(super) async fn hook_v2_admit(`; `@@ -493,0 +560 @@ pub(super) async fn hook_v2_admit(`; `@@ -501,0 +569,6 @@ pub(super) async fn hook_v2_admit(`; `@@ -533 +606,6 @@ pub(super) async fn hook_v2_admit(`
- Classification: **shared host live-origin/sealed-admission extension**
- Original behavior: Hook V2 admission validates producer binding, admission identity, lifecycle, and catch-up before transcript ingest.
- Permitted action: Preserve canonical producer/session identity, live-origin ledger admission, fail-open/deferral, sealed source rules, and typed catch-up disposition; no provider or Native write may be deferred behind this hook.
- Associated check: Hook admission ledger, binding conflict/catch-up, live-origin, cancellation, and source replacement fixtures.

### 78. crates/tracedecay/src/mcp/tools/handlers/hook_runtime/admission/tests.rs

- Status: `M`
- Original path (b3): `crates/tracedecay/src/mcp/tools/handlers/hook_runtime/admission/tests.rs`
- Current path (head): `crates/tracedecay/src/mcp/tools/handlers/hook_runtime/admission/tests.rs`
- Hunks (1): `@@ -270,0 +271,4 @@ fn hook_v2_catchup_response_propagates_transport_disposition() {`
- Classification: **shared host live-origin/sealed-admission tests**
- Original behavior: Hook admission tests verify typed binding and catch-up transport behavior.
- Permitted action: Keep tests aligned with canonical admission identity, live-origin outcomes, and catch-up dispositions; tests must not bless a second persistence path.
- Associated check: Hook V2 catch-up, binding conflict, live-origin, and no-write deferral fixtures.

### 79. crates/tracedecay/src/mcp/tools/handlers/hook_runtime/envelope.rs

- Status: `M`
- Original path (b3): `crates/tracedecay/src/mcp/tools/handlers/hook_runtime/envelope.rs`
- Current path (head): `crates/tracedecay/src/mcp/tools/handlers/hook_runtime/envelope.rs`
- Hunks (1): `@@ -42,0 +43,9 @@ pub(super) fn hook_v2_native_session_id(`
- Classification: **shared host live-origin/sealed-admission extension**
- Original behavior: Hook envelopes derive the protected producer/session identity used by admission and ingest.
- Permitted action: Preserve canonical session binding and add the Native start locator only as validated envelope provenance; it cannot widen scope or select an alternate source.
- Associated check: Native start-locator, session-binding, malformed-envelope, and scope fixtures.

### 80. crates/tracedecay/src/mcp/tools/handlers/hook_runtime/ingest.rs

- Status: `M`
- Original path (b3): `crates/tracedecay/src/mcp/tools/handlers/hook_runtime/ingest.rs`
- Current path (head): `crates/tracedecay/src/mcp/tools/handlers/hook_runtime/ingest.rs`
- Hunks (9): `@@ -68,11 +68,30 @@ fn host_admission_facade<'a>(`; `@@ -80,2 +99 @@ fn host_admission_facade<'a>(`; `@@ -119,0 +138 @@ async fn admit_codex_project_rollouts(`; `@@ -124,2 +143,15 @@ async fn admit_codex_project_rollouts(`; `@@ -131,0 +164 @@ async fn admit_codex_project_rollouts(`; `@@ -321,0 +355 @@ async fn admit_codex_rollouts_once(`; `@@ -577 +611,7 @@ pub(super) async fn ingest_transcript(`; `@@ -585,0 +626,24 @@ pub(crate) async fn ingest_transcript_with_cancellation(`; `@@ -629 +693,2 @@ pub(crate) async fn ingest_transcript_with_cancellation(`
- Classification: **shared host live-origin/sealed-admission extension**
- Original behavior: The hook runtime admits normalized host transcript material into canonical session/LCM observation authorities with bounded cancellation and lifecycle handling.
- Permitted action: Preserve host admission, exact provenance, canonical cursor/source ownership, cancellation, and no-write deferral while recording live-origin evidence after admission; Native facts remain outside hook ingest.
- Associated check: Claude/Codex/Cursor ingest, live-origin, cancellation, source replacement, and historical catch-up fixtures.

### 81. crates/tracedecay/src/mcp/tools/handlers/hook_runtime/ingest/kernels.rs

- Status: `M`
- Original path (b3): `crates/tracedecay/src/mcp/tools/handlers/hook_runtime/ingest/kernels.rs`
- Current path (head): `crates/tracedecay/src/mcp/tools/handlers/hook_runtime/ingest/kernels.rs`
- Hunks (6): `@@ -78,0 +79 @@ pub(super) struct TranscriptCaptureContext<'a> {`; `@@ -155,0 +157 @@ transcript_capture_kernels! {`; `@@ -203,0 +206,6 @@ const TRANSCRIPT_CAPTURE_KERNELS: &[(`; `@@ -283,0 +292,45 @@ async fn capture_claude_profile(`; `@@ -410,2 +462,0 @@ async fn capture_codex_project(`; `@@ -416,9 +467,24 @@ async fn capture_codex_project(`
- Classification: **shared host live-origin/sealed-admission extension**
- Original behavior: Capture kernels normalize supported host transcript sources and route them through the canonical observation admission path.
- Permitted action: Preserve per-host source identity, bounded capture, provenance, privacy, cancellation, and canonical settlement; live start metadata is evidence for the existing route, never a second ingest authority.
- Associated check: Capture-registry, Claude/Codex profile, live-start, source identity, cancellation, and no-write deferral fixtures.

### 82. crates/tracedecay/src/mcp/tools/handlers/hook_runtime/ingest/tests.rs

- Status: `M`
- Original path (b3): `crates/tracedecay/src/mcp/tools/handlers/hook_runtime/ingest/tests.rs`
- Current path (head): `crates/tracedecay/src/mcp/tools/handlers/hook_runtime/ingest/tests.rs`
- Hunks (2): `@@ -182,0 +183,4 @@ fn capture_registry_owns_every_supported_transcript_route() {`; `@@ -196 +199,0 @@ fn capture_registry_owns_every_supported_transcript_route() {`
- Classification: **shared host live-origin/sealed-admission tests**
- Original behavior: Ingest tests assert that the capture registry owns each supported transcript route.
- Permitted action: Keep registry ownership, live-origin metadata, cancellation, and canonical observation settlement explicit; no test should imply provider-owned or Native-fact ingest.
- Associated check: Capture registry completeness, source replacement, live-origin, cancellation, and canonical settlement fixtures.

### 83. crates/tracedecay/src/mcp/tools/handlers/hook_runtime/mod.rs

- Status: `M`
- Original path (b3): `crates/tracedecay/src/mcp/tools/handlers/hook_runtime/mod.rs`
- Current path (head): `crates/tracedecay/src/mcp/tools/handlers/hook_runtime/mod.rs`
- Hunks (3): `@@ -20,0 +21,3 @@ mod ingest;`; `@@ -156,0 +160 @@ pub(crate) async fn handle_projectless_hook_runtime(`; `@@ -184,0 +189 @@ pub(crate) async fn handle_projectless_hook_runtime(`
- Classification: **shared host live-origin/sealed-admission extension**
- Original behavior: The hook-runtime module dispatches project-bound and projectless host hook requests into admission and ingest.
- Permitted action: Keep live-origin handling below the existing dispatch boundary, preserve projectless scope and typed outcomes, and avoid provider-specific branching in the hook router.
- Associated check: Project-bound/projectless dispatch, scope, live-origin, cancellation, and catch-up fixtures.

### 84. crates/tracedecay/src/mcp/tools/handlers/hook_runtime/origin.rs

- Status: `A`
- Original path (b3): `absent at b3`
- Current path (head): `crates/tracedecay/src/mcp/tools/handlers/hook_runtime/origin.rs`
- Hunks (1): `@@ -0,0 +1,1250 @@`
- Classification: **shared host live-origin/sealed-admission extension**
- Original behavior: No b3 counterpart; this module records live hook origin, canonical locator/session/cursor/provenance, and typed ledger outcomes for the existing host route.
- Permitted action: Keep origin evidence sealed to the admitted source and session, preserve cancellation and no-write deferral, and never make the origin ledger a Native fact or provider persistence authority.
- Associated check: Live-origin admission/ledger, canonical locator, session/cursor/provenance, cancellation, restart, and no-write deferral fixtures.

### 85. crates/tracedecay/src/mcp/tools/handlers/hook_runtime/terminal.rs

- Status: `M`
- Original path (b3): `crates/tracedecay/src/mcp/tools/handlers/hook_runtime/terminal.rs`
- Current path (head): `crates/tracedecay/src/mcp/tools/handlers/hook_runtime/terminal.rs`
- Hunks (8): `@@ -11 +11 @@ use super::hermes::user_review;`; `@@ -22,0 +23 @@ pub(super) fn retain_codex_stop(`; `@@ -32,0 +34,14 @@ pub(super) fn retain_codex_stop(`; `@@ -39,4 +54,21 @@ pub(super) fn retain_codex_stop(`; `@@ -44,43 +76,13 @@ pub(super) fn retain_codex_stop(`; `@@ -88 +89,0 @@ pub(super) fn retain_codex_stop(`; `@@ -90 +91,58 @@ pub(super) fn retain_codex_stop(`; `@@ -105,0 +164,95 @@ pub(super) fn retain_codex_stop(`
- Classification: **shared host live-origin/sealed-admission extension**
- Original behavior: Hook terminal handling records host stop/terminal events and user-review outcomes against the admitted session.
- Permitted action: Preserve exact Codex session lookup/stop bounds, sealed source/home/cancellation checks, terminal follow-up, and canonical session evidence ownership; no terminal event may fabricate a Native fact or defer a required canonical write.
- Associated check: Codex exact-session stop, sealed source/home, cancellation, terminal follow-up, live-origin, and restart fixtures.

## Required verification split

The exact restoration must be checked by whole-file b3 equality and ordinary/missing/invalid-history fixtures. Shared extensions must be checked separately: sealed admission and source replacement, ordinary and reserved cursor round-trip, exact CAS, locator conflict/rollback/reopen, provenance mismatch/fallback, historical catch-up, refresh begin/status/cancel after restart, and storage interruption/durability. Product adapter/provider tests must compare every supported typed Native operation at the original boundary; output deduplication does not prove one canonical execution.

Any hunk that introduces an additional original algorithm, authority, schema, or lifecycle change is outside these dispositions and blocks the affected lane until root assigns an exact restoration or external integration correction.
