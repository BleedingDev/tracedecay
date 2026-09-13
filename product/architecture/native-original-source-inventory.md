# Native original source inventory

Active source reference: immutable upstream 57006f60cb45bcee8487e73a40d4fad1a12ee2b6 (unmodified upstream PR707 tip).
Current execution head captured for this ledger: 9c36fe6d62d25b47c39e4c78723e252e4cb1b597.
Historical labels retained for audit: b3b43410e47115056f2066449aafa1822bbb6049 was the older merge-side source; 571daf3a9612e5247443e4da3a107b542686c1ef was the August audited product head. The accepted August sync floor 5749e4fcfe268e17bd19a0e6ef90c646f7b37289 remains product sync metadata and is not the active Native baseline.

This ledger is generated from the scoped SOURCE-BOUNDARY.md comparison:
git diff --no-ext-diff --unified=0 57006f60cb45bcee8487e73a40d4fad1a12ee2b6 HEAD -- each protected/native-adjacent path named by SOURCE-BOUNDARY.md.
It records every changed path and every zero-context hunk in that surface: 29 modified paths, 27 added paths, and 192 hunks. Classifications are review dispositions, not a blanket allowlist.
No restoration, parity suite, or implementation-complete status is claimed by this source ledger.

## Protected unchanged areas

- The complete tracedecay-session-memory fact_store and memory authorities, tracedecay-lcm algorithms and schemas, tracedecay-store/src/memory contracts, tracedecay/src/tracedecay/facts.rs authorities, memory-v2 transactions, original retained LCM services, and runtime-core database/memory support have no scoped 570-to-current hunk.
- crates/tracedecay-runtime-core/src/db/retrieval_anchor_authority.rs is byte-equal to 570 (blob d976e2a23c19441a60412420a1db5e68f7ccc5ca); rn-restore-anchor owns the read-only equality gate and later ordinary/missing/invalid-history behavior checks.
- crates/tracedecay-tool-catalog/src/operation.rs and the session retrieval primitive remain protected route anchors; equality is source evidence only and does not prove selected Native composition.
- Retained LCM/session authorities under session-runtime and the original Native fact store remain protected. The route catalog may name their public boundaries without authorizing provider replacements.
- tracedecay-privacy has no current 570-to-head hunk in this scoped comparison. The b3-to-August detector finding remains historical evidence and must still be covered by the privacy audit; this ledger does not relabel that historical delta as a 570 change.

Direct file equality is necessary source evidence and is not behavioral proof. Original Native algorithm/schema parity, shared host extensions and product adapter behavior require separate checks.

## Classification keys

- O — original 570 implementation. Protected source is retained at its original boundary.
- X — exact restoration gate. Only the named owner may restore an exact 570 blob; no adapter may substitute an algorithm.
- H — pre-existing shared host, session, storage, contract or ingestion extension. Preserve it with focused regression evidence.
- C — product composition/catalog mount. It wires authorities but does not establish parity.
- P — product-added provider, host journey, staged substitute or receipt surface absent from 570. Keep it removable and advisory.

## Complete 570-to-current hunk ledger

### 01. crates/tracedecay-application/src/primitives/runtime.rs

- Original path (570): crates/tracedecay-application/src/primitives/runtime.rs.
- Current path (execution head): crates/tracedecay-application/src/primitives/runtime.rs.
- Status: M.
- Hunks (7): @@ -1901 +1901 @@ fn problem<T>( | @@ -1912,2 +1912,5 @@ fn diagnostics_unavailable_problem<T>( | @@ -2051,3 +2054,3 @@ mod tests { | @@ -2055 +2058 @@ mod tests { | @@ -2094,2 +2097,4 @@ mod tests { | @@ -2098,7 +2103 @@ mod tests { | @@ -2107,0 +2107,22 @@ mod tests {
- Classification: H — pre-existing shared application dispatch extension.
- Original behavior: The shared application runtime dispatches typed Native and session primitives and diagnostics.
- Permitted action: Preserve 570 route identity, scope admission, cancellation and typed outcomes; dispatch wiring is not proof that selected provider parity is complete.
- Associated check and owner: Source route check plus Native fact/session parity and no-effect fixtures; owner rn-verify-facts and rn-verify-sessions.

### 02. crates/tracedecay-contracts/src/retained_surfaces.rs

- Original path (570): crates/tracedecay-contracts/src/retained_surfaces.rs.
- Current path (execution head): crates/tracedecay-contracts/src/retained_surfaces.rs.
- Status: M.
- Hunks (12): @@ -33,0 +34 @@ mod memory; | @@ -39,0 +41 @@ pub use evidence::*; | @@ -59,0 +62,9 @@ pub enum RetainedSurfaceOperation { | @@ -88,0 +100 @@ pub enum SdkResultSemanticsV1 { | @@ -107 +119 @@ impl RetainedSurfaceOperation { | @@ -121,0 +134,9 @@ impl RetainedSurfaceOperation { | @@ -140 +161 @@ impl RetainedSurfaceOperation { | @@ -144 +165 @@ impl RetainedSurfaceOperation { | @@ -153,0 +175,36 @@ impl RetainedSurfaceOperation { | @@ -174,0 +232,9 @@ impl RetainedSurfaceOperation { | @@ -231,0 +298 @@ fn surface_specs() -> Vec<&'static RetainedSurfaceSpec> { | @@ -517,0 +585,63 @@ fn retained_surface_executable_schemas(
- Classification: H — pre-existing shared retained contract/catalog extension.
- Original behavior: The retained catalog owns operation IDs, request/result schemas, effects and terminal semantics.
- Permitted action: Preserve 570 typed schemas and effect semantics. FactStoreCurate stays the explicit RetainedSurfaceOperation route through automation/effect ledger, distinct from lower-level curation.
- Associated check and owner: Retained catalog/schema source check and fact/effect receipt fixtures; owner rn-verify-facts and rn-native-port.

### 03. crates/tracedecay-runtime-core/src/storage/paths_and_io.rs

- Original path (570): crates/tracedecay-runtime-core/src/storage/paths_and_io.rs.
- Current path (execution head): crates/tracedecay-runtime-core/src/storage/paths_and_io.rs.
- Status: M.
- Hunks (18): @@ -192 +192,20 @@ impl PrivateStoreIo { | @@ -444,0 +464 @@ fn create_missing_directories_locked( | @@ -446,0 +467 @@ fn create_missing_directories_locked( | @@ -447,0 +469 @@ fn create_missing_directories_locked( | @@ -449,0 +472 @@ fn create_missing_directories_locked( | @@ -456,0 +480 @@ fn create_missing_directories_locked( | @@ -459,0 +484 @@ fn create_missing_directories_locked( | @@ -468 +493 @@ fn create_missing_directories_locked( | @@ -479,2 +504,5 @@ fn create_missing_directories_locked( | @@ -487 +515,4 @@ fn platform_create_dir_all_durable(path: &Path) -> io::Result<()> { | @@ -491,0 +523 @@ fn platform_create_dir_all_durable(path: &Path) -> io::Result<()> { | @@ -492,0 +525 @@ fn platform_create_dir_all_durable(path: &Path) -> io::Result<()> { | @@ -494,0 +528 @@ fn platform_create_dir_all_durable(path: &Path) -> io::Result<()> { | @@ -497,0 +532 @@ fn platform_create_dir_all_durable(path: &Path) -> io::Result<()> { | @@ -506 +541 @@ fn platform_create_dir_all_durable(path: &Path) -> io::Result<()> { | @@ -551 +586,5 @@ fn platform_create_dir_all_durable(path: &Path) -> io::Result<()> { | @@ -553,0 +593 @@ fn platform_create_dir_all_durable(path: &Path) -> io::Result<()> { | @@ -803,0 +844,33 @@ fn acquire_lock_file_blocking(lock_path: &Path, private: bool) -> io::Result<fs:
- Classification: H — pre-existing shared storage extension.
- Original behavior: Storage path creation, locks, interruption and durability support shared Native and host stores.
- Permitted action: Keep ordinary no-callback behavior and durability identical to 570 while retaining the reviewed interruptible path; this is not a Native store or scorer.
- Associated check and owner: Storage interruption, lock and durability source checks; owner rn-verify-state.

### 04. crates/tracedecay-runtime-core/src/storage/tests.rs

- Original path (570): crates/tracedecay-runtime-core/src/storage/tests.rs.
- Current path (execution head): crates/tracedecay-runtime-core/src/storage/tests.rs.
- Status: M.
- Hunks (1): @@ -70,0 +71,57 @@ mod tests {
- Classification: H — pre-existing shared storage extension.
- Original behavior: Storage path creation, locks, interruption and durability support shared Native and host stores.
- Permitted action: Keep ordinary no-callback behavior and durability identical to 570 while retaining the reviewed interruptible path; this is not a Native store or scorer.
- Associated check and owner: Storage interruption, lock and durability source checks; owner rn-verify-state.

### 05. crates/tracedecay-session-runtime/src/session_sync.rs

- Original path (570): crates/tracedecay-session-runtime/src/session_sync.rs.
- Current path (execution head): crates/tracedecay-session-runtime/src/session_sync.rs.
- Status: M.
- Hunks (1): @@ -91,0 +92,5 @@ pub struct DaemonSessionSyncConfig {
- Classification: H — pre-existing shared session-runtime extension.
- Original behavior: Session sync, project lifecycle and historical refresh carry exact scope, provenance and temporal state for admitted host sessions.
- Permitted action: Preserve project authority, source identity, resolver and refresh behavior; do not re-derive scope or add a provider-local session authority.
- Associated check and owner: Sealed-source, provenance, refresh restart/cancel and scope fixtures; owner rn-verify-sessions and rn-host-fixtures.

### 06. crates/tracedecay-session-runtime/src/session_sync/git_topology.rs

- Original path (570): crates/tracedecay-session-runtime/src/session_sync/git_topology.rs.
- Current path (execution head): crates/tracedecay-session-runtime/src/session_sync/git_topology.rs.
- Status: M.
- Hunks (1): @@ -57,9 +57 @@ impl SessionSyncProjectContext {
- Classification: H — pre-existing shared session-runtime extension.
- Original behavior: Session sync, project lifecycle and historical refresh carry exact scope, provenance and temporal state for admitted host sessions.
- Permitted action: Preserve project authority, source identity, resolver and refresh behavior; do not re-derive scope or add a provider-local session authority.
- Associated check and owner: Sealed-source, provenance, refresh restart/cancel and scope fixtures; owner rn-verify-sessions and rn-host-fixtures.

### 07. crates/tracedecay-session-runtime/src/session_sync/project_lifecycle.rs

- Original path (570): crates/tracedecay-session-runtime/src/session_sync/project_lifecycle.rs.
- Current path (execution head): crates/tracedecay-session-runtime/src/session_sync/project_lifecycle.rs.
- Status: M.
- Hunks (6): @@ -11,0 +12 @@ use tracedecay_domain::{BrainId, ProjectId, UserProfileId, UtcMicros}; | @@ -29,0 +31,4 @@ pub struct SessionSyncProjectContext { | @@ -33,0 +39,6 @@ pub struct SessionSyncProjectContext { | @@ -290,0 +302,11 @@ impl DaemonSessionSyncService { | @@ -301,0 +324,2 @@ impl DaemonSessionSyncService { | @@ -305,0 +330,2 @@ impl DaemonSessionSyncService {
- Classification: H — pre-existing shared session-runtime extension.
- Original behavior: Session sync, project lifecycle and historical refresh carry exact scope, provenance and temporal state for admitted host sessions.
- Permitted action: Preserve project authority, source identity, resolver and refresh behavior; do not re-derive scope or add a provider-local session authority.
- Associated check and owner: Sealed-source, provenance, refresh restart/cancel and scope fixtures; owner rn-verify-sessions and rn-host-fixtures.

### 08. crates/tracedecay-session-runtime/src/session_temporal_refresh_scheduler/history.rs

- Original path (570): crates/tracedecay-session-runtime/src/session_temporal_refresh_scheduler/history.rs.
- Current path (execution head): crates/tracedecay-session-runtime/src/session_temporal_refresh_scheduler/history.rs.
- Status: M.
- Hunks (4): @@ -82,0 +83,5 @@ pub struct ProjectSessionHistoricalIngestor { | @@ -117,0 +123 @@ impl ProjectSessionHistoricalIngestor { | @@ -123,0 +130,10 @@ impl ProjectSessionHistoricalIngestor { | @@ -136,0 +153,4 @@ impl SessionHistoricalIngestor for ProjectSessionHistoricalIngestor {
- Classification: H — pre-existing shared session-runtime extension.
- Original behavior: Session sync, project lifecycle and historical refresh carry exact scope, provenance and temporal state for admitted host sessions.
- Permitted action: Preserve project authority, source identity, resolver and refresh behavior; do not re-derive scope or add a provider-local session authority.
- Associated check and owner: Sealed-source, provenance, refresh restart/cancel and scope fixtures; owner rn-verify-sessions and rn-host-fixtures.

### 09. crates/tracedecay-sessions/src/admission/mod.rs

- Original path (570): crates/tracedecay-sessions/src/admission/mod.rs.
- Current path (execution head): crates/tracedecay-sessions/src/admission/mod.rs.
- Status: M.
- Hunks (1): @@ -884,0 +885,18 @@ pub(crate) mod test_support {
- Classification: H — pre-existing shared host/session extension.
- Original behavior: Host admission, Codex lookup, JSONL identity, observation admission, cursor CAS, provenance and transcript store access protect canonical session evidence.
- Permitted action: Preserve privacy, cancellation, no-write deferral, sealed source replacement, exact cursor/CAS and atomic locator/transaction behavior; these extensions do not replace Native facts or LCM.
- Associated check and owner: Stable/mismatched sealed admission, source identity, cursor round-trip/CAS, locator rollback/reopen and ingest fixtures; owner rn-verify-sessions and rn-host-fixtures.

### 10. crates/tracedecay-sessions/src/observation.rs

- Original path (570): crates/tracedecay-sessions/src/observation.rs.
- Current path (execution head): crates/tracedecay-sessions/src/observation.rs.
- Status: M.
- Hunks (7): @@ -93,0 +94,2 @@ pub struct CaptureObservationRequest { | @@ -119,0 +122 @@ impl CaptureObservationRequest { | @@ -146,0 +150,10 @@ impl CaptureObservationRequest { | @@ -501,0 +515 @@ where | @@ -536,0 +551,5 @@ where | @@ -547,0 +567,10 @@ where | @@ -563,0 +593,6 @@ where
- Classification: H — pre-existing shared host/session extension.
- Original behavior: Host admission, Codex lookup, JSONL identity, observation admission, cursor CAS, provenance and transcript store access protect canonical session evidence.
- Permitted action: Preserve privacy, cancellation, no-write deferral, sealed source replacement, exact cursor/CAS and atomic locator/transaction behavior; these extensions do not replace Native facts or LCM.
- Associated check and owner: Stable/mismatched sealed admission, source identity, cursor round-trip/CAS, locator rollback/reopen and ingest fixtures; owner rn-verify-sessions and rn-host-fixtures.

### 11. crates/tracedecay-sessions/src/observation_test.rs

- Original path (570): crates/tracedecay-sessions/src/observation_test.rs.
- Current path (execution head): crates/tracedecay-sessions/src/observation_test.rs.
- Status: M.
- Hunks (1): @@ -253,0 +254,19 @@ impl ObservationStore for FakeStore {
- Classification: H — pre-existing shared host/session extension.
- Original behavior: Host admission, Codex lookup, JSONL identity, observation admission, cursor CAS, provenance and transcript store access protect canonical session evidence.
- Permitted action: Preserve privacy, cancellation, no-write deferral, sealed source replacement, exact cursor/CAS and atomic locator/transaction behavior; these extensions do not replace Native facts or LCM.
- Associated check and owner: Stable/mismatched sealed admission, source identity, cursor round-trip/CAS, locator rollback/reopen and ingest fixtures; owner rn-verify-sessions and rn-host-fixtures.

### 12. crates/tracedecay-sessions/src/repository_provenance.rs

- Original path (570): crates/tracedecay-sessions/src/repository_provenance.rs.
- Current path (execution head): crates/tracedecay-sessions/src/repository_provenance.rs.
- Status: M.
- Hunks (5): @@ -48,0 +49,19 @@ pub struct RepositoryProvenanceAdmissionContext { | @@ -79,0 +99,46 @@ pub struct CapturedRepositoryProvenanceV1 { | @@ -86,0 +152,81 @@ impl RepositoryProvenanceAdmissionContext { | @@ -103,0 +250 @@ impl RepositoryProvenanceAdmissionContext { | @@ -164,0 +312 @@ impl RepositoryProvenanceAdmissionContext {
- Classification: H — pre-existing shared host/session extension.
- Original behavior: Host admission, Codex lookup, JSONL identity, observation admission, cursor CAS, provenance and transcript store access protect canonical session evidence.
- Permitted action: Preserve privacy, cancellation, no-write deferral, sealed source replacement, exact cursor/CAS and atomic locator/transaction behavior; these extensions do not replace Native facts or LCM.
- Associated check and owner: Stable/mismatched sealed admission, source identity, cursor round-trip/CAS, locator rollback/reopen and ingest fixtures; owner rn-verify-sessions and rn-host-fixtures.

### 13. crates/tracedecay-sessions/src/repository_provenance_test.rs

- Original path (570): crates/tracedecay-sessions/src/repository_provenance_test.rs.
- Current path (execution head): crates/tracedecay-sessions/src/repository_provenance_test.rs.
- Status: M.
- Hunks (1): @@ -456,0 +457,158 @@ fn admission_capture_cache_reuses_exact_watermark_and_invalidates_on_index_chang
- Classification: H — pre-existing shared host/session extension.
- Original behavior: Host admission, Codex lookup, JSONL identity, observation admission, cursor CAS, provenance and transcript store access protect canonical session evidence.
- Permitted action: Preserve privacy, cancellation, no-write deferral, sealed source replacement, exact cursor/CAS and atomic locator/transaction behavior; these extensions do not replace Native facts or LCM.
- Associated check and owner: Stable/mismatched sealed admission, source identity, cursor round-trip/CAS, locator rollback/reopen and ingest fixtures; owner rn-verify-sessions and rn-host-fixtures.

### 14. crates/tracedecay-sessions/src/runtime/hosts/codex.rs

- Original path (570): crates/tracedecay-sessions/src/runtime/hosts/codex.rs.
- Current path (execution head): crates/tracedecay-sessions/src/runtime/hosts/codex.rs.
- Status: M.
- Hunks (9): @@ -107 +107 @@ pub use observation::{ | @@ -111,0 +112 @@ pub use observation::{ | @@ -1027,0 +1029,4 @@ const EXACT_HOOK_DISCOVERY_UNITS_PER_CALL: usize = 64; | @@ -1031 +1036 @@ const MAX_EXACT_HOOK_SESSION_REQUESTS: usize = 64; | @@ -1037,0 +1043,40 @@ pub(crate) struct CodexExactSessionLookupOutcome { | @@ -1221,0 +1267,248 @@ impl CodexSource { | @@ -1226 +1519 @@ impl CodexSource { | @@ -2434,0 +2728,54 @@ fn retained_scan_step( | @@ -2870,0 +3218,317 @@ impl TranscriptSource for CodexSource {
- Classification: H — pre-existing shared host/session extension.
- Original behavior: Host admission, Codex lookup, JSONL identity, observation admission, cursor CAS, provenance and transcript store access protect canonical session evidence.
- Permitted action: Preserve privacy, cancellation, no-write deferral, sealed source replacement, exact cursor/CAS and atomic locator/transaction behavior; these extensions do not replace Native facts or LCM.
- Associated check and owner: Stable/mismatched sealed admission, source identity, cursor round-trip/CAS, locator rollback/reopen and ingest fixtures; owner rn-verify-sessions and rn-host-fixtures.

### 15. crates/tracedecay-sessions/src/runtime/hosts/codex/meta.rs

- Original path (570): crates/tracedecay-sessions/src/runtime/hosts/codex/meta.rs.
- Current path (execution head): crates/tracedecay-sessions/src/runtime/hosts/codex/meta.rs.
- Status: M.
- Hunks (1): @@ -78,0 +79,74 @@ pub(super) fn session_meta(path: &Path) -> Option<CodexMeta> {
- Classification: H — pre-existing shared host/session extension.
- Original behavior: Host admission, Codex lookup, JSONL identity, observation admission, cursor CAS, provenance and transcript store access protect canonical session evidence.
- Permitted action: Preserve privacy, cancellation, no-write deferral, sealed source replacement, exact cursor/CAS and atomic locator/transaction behavior; these extensions do not replace Native facts or LCM.
- Associated check and owner: Stable/mismatched sealed admission, source identity, cursor round-trip/CAS, locator rollback/reopen and ingest fixtures; owner rn-verify-sessions and rn-host-fixtures.

### 16. crates/tracedecay-sessions/src/runtime/hosts/codex/observation.rs

- Original path (570): crates/tracedecay-sessions/src/runtime/hosts/codex/observation.rs.
- Current path (execution head): crates/tracedecay-sessions/src/runtime/hosts/codex/observation.rs.
- Status: M.
- Hunks (17): @@ -24,0 +25,2 @@ use tracedecay_store::observation::ObservationCoverageReason; | @@ -278,0 +281 @@ pub async fn try_admit_codex_jsonl_observations_for_project_with_admission( | @@ -289,0 +293 @@ pub async fn try_admit_codex_jsonl_observations_for_project_with_admission_and_c | @@ -298,0 +303 @@ pub async fn try_admit_codex_jsonl_observations_for_project_with_admission_and_c | @@ -311,0 +317 @@ pub(crate) async fn try_admit_codex_jsonl_observations_for_project_window( | @@ -320,0 +327 @@ pub(crate) async fn try_admit_codex_jsonl_observations_for_project_window( | @@ -400,0 +408 @@ pub(super) enum CodexObservationAdmission<'a> { | @@ -430 +438,10 @@ impl CodexObservationAdmission<'_> { | @@ -489,0 +507 @@ struct CodexAdmissionContext<'a> { | @@ -660,0 +679,47 @@ async fn try_admit_codex_jsonl_observations( | @@ -673 +738,4 @@ async fn try_admit_codex_jsonl_observations( | @@ -686,0 +755 @@ async fn try_admit_codex_jsonl_observations( | @@ -744,0 +814 @@ async fn admit_codex_jsonl_page( | @@ -772,0 +843,3 @@ async fn admit_codex_jsonl_page( | @@ -1003,0 +1077 @@ mod replay_boundary_tests { | @@ -1016,0 +1091 @@ mod replay_boundary_tests { | @@ -1072,0 +1148,122 @@ mod replay_boundary_tests {
- Classification: H — pre-existing shared host/session extension.
- Original behavior: Host admission, Codex lookup, JSONL identity, observation admission, cursor CAS, provenance and transcript store access protect canonical session evidence.
- Permitted action: Preserve privacy, cancellation, no-write deferral, sealed source replacement, exact cursor/CAS and atomic locator/transaction behavior; these extensions do not replace Native facts or LCM.
- Associated check and owner: Stable/mismatched sealed admission, source identity, cursor round-trip/CAS, locator rollback/reopen and ingest fixtures; owner rn-verify-sessions and rn-host-fixtures.

### 17. crates/tracedecay-sessions/src/runtime/hosts/codex/tests.rs

- Original path (570): crates/tracedecay-sessions/src/runtime/hosts/codex/tests.rs.
- Current path (execution head): crates/tracedecay-sessions/src/runtime/hosts/codex/tests.rs.
- Status: M.
- Hunks (4): @@ -182,0 +183 @@ mod goal_event_tests { | @@ -188,0 +190 @@ mod goal_event_tests { | @@ -769,0 +772 @@ mod goal_event_tests { | @@ -785,0 +789 @@ mod goal_event_tests {
- Classification: H — pre-existing shared host/session extension.
- Original behavior: Host admission, Codex lookup, JSONL identity, observation admission, cursor CAS, provenance and transcript store access protect canonical session evidence.
- Permitted action: Preserve privacy, cancellation, no-write deferral, sealed source replacement, exact cursor/CAS and atomic locator/transaction behavior; these extensions do not replace Native facts or LCM.
- Associated check and owner: Stable/mismatched sealed admission, source identity, cursor round-trip/CAS, locator rollback/reopen and ingest fixtures; owner rn-verify-sessions and rn-host-fixtures.

### 18. crates/tracedecay-sessions/src/runtime/ingest.rs

- Original path (570): crates/tracedecay-sessions/src/runtime/ingest.rs.
- Current path (execution head): crates/tracedecay-sessions/src/runtime/ingest.rs.
- Status: M.
- Hunks (1): @@ -9,0 +10,2 @@ mod user_provider;
- Classification: H — pre-existing shared host/session extension.
- Original behavior: Host admission, Codex lookup, JSONL identity, observation admission, cursor CAS, provenance and transcript store access protect canonical session evidence.
- Permitted action: Preserve privacy, cancellation, no-write deferral, sealed source replacement, exact cursor/CAS and atomic locator/transaction behavior; these extensions do not replace Native facts or LCM.
- Associated check and owner: Stable/mismatched sealed admission, source identity, cursor round-trip/CAS, locator rollback/reopen and ingest fixtures; owner rn-verify-sessions and rn-host-fixtures.

### 19. crates/tracedecay-sessions/src/runtime/ingest/failure.rs

- Original path (570): crates/tracedecay-sessions/src/runtime/ingest/failure.rs.
- Current path (execution head): crates/tracedecay-sessions/src/runtime/ingest/failure.rs.
- Status: M.
- Hunks (2): @@ -461,0 +462,18 @@ pub(super) fn warn_transcript_catch_up_failure( | @@ -464,0 +483,2 @@ pub(super) fn warn_transcript_catch_up_failure(
- Classification: H — pre-existing shared host/session extension.
- Original behavior: Host admission, Codex lookup, JSONL identity, observation admission, cursor CAS, provenance and transcript store access protect canonical session evidence.
- Permitted action: Preserve privacy, cancellation, no-write deferral, sealed source replacement, exact cursor/CAS and atomic locator/transaction behavior; these extensions do not replace Native facts or LCM.
- Associated check and owner: Stable/mismatched sealed admission, source identity, cursor round-trip/CAS, locator rollback/reopen and ingest fixtures; owner rn-verify-sessions and rn-host-fixtures.

### 20. crates/tracedecay-sessions/src/runtime/ingest/project_provider.rs

- Original path (570): crates/tracedecay-sessions/src/runtime/ingest/project_provider.rs.
- Current path (execution head): crates/tracedecay-sessions/src/runtime/ingest/project_provider.rs.
- Status: M.
- Hunks (1): @@ -277,0 +278 @@ impl<'a> ProjectProviderRun<'a> {
- Classification: H — pre-existing shared host/session extension.
- Original behavior: Host admission, Codex lookup, JSONL identity, observation admission, cursor CAS, provenance and transcript store access protect canonical session evidence.
- Permitted action: Preserve privacy, cancellation, no-write deferral, sealed source replacement, exact cursor/CAS and atomic locator/transaction behavior; these extensions do not replace Native facts or LCM.
- Associated check and owner: Stable/mismatched sealed admission, source identity, cursor round-trip/CAS, locator rollback/reopen and ingest fixtures; owner rn-verify-sessions and rn-host-fixtures.

### 21. crates/tracedecay-sessions/src/runtime/ingest/tests.rs

- Original path (570): crates/tracedecay-sessions/src/runtime/ingest/tests.rs.
- Current path (execution head): crates/tracedecay-sessions/src/runtime/ingest/tests.rs.
- Status: M.
- Hunks (1): @@ -66,0 +67 @@ async fn cancelled_codex_provider_stops_before_opening_the_next_jsonl_source() {
- Classification: H — pre-existing shared host/session extension.
- Original behavior: Host admission, Codex lookup, JSONL identity, observation admission, cursor CAS, provenance and transcript store access protect canonical session evidence.
- Permitted action: Preserve privacy, cancellation, no-write deferral, sealed source replacement, exact cursor/CAS and atomic locator/transaction behavior; these extensions do not replace Native facts or LCM.
- Associated check and owner: Stable/mismatched sealed admission, source identity, cursor round-trip/CAS, locator rollback/reopen and ingest fixtures; owner rn-verify-sessions and rn-host-fixtures.

### 22. crates/tracedecay-sessions/src/runtime/observation/jsonl_observation_admission.rs

- Original path (570): crates/tracedecay-sessions/src/runtime/observation/jsonl_observation_admission.rs.
- Current path (execution head): crates/tracedecay-sessions/src/runtime/observation/jsonl_observation_admission.rs.
- Status: M.
- Hunks (7): @@ -77,0 +78,135 @@ struct FlushPolicy<'policy> { | @@ -88,0 +224 @@ pub(in crate::runtime) struct JsonlObservationAdmissionRequest<'request> { | @@ -113,0 +250 @@ impl<'request> JsonlObservationAdmissionRequest<'request> { | @@ -143 +280,13 @@ impl<'request> JsonlObservationAdmissionRequest<'request> { | @@ -2210,0 +2360 @@ pub(in crate::runtime) async fn admit_jsonl_observations<State: Clone>( | @@ -2217,0 +2368,6 @@ pub(in crate::runtime) async fn admit_jsonl_observations<State: Clone>( | @@ -2279,0 +2436,44 @@ pub(in crate::runtime) async fn admit_jsonl_observations<State: Clone>(
- Classification: H — pre-existing shared host/session extension.
- Original behavior: Host admission, Codex lookup, JSONL identity, observation admission, cursor CAS, provenance and transcript store access protect canonical session evidence.
- Permitted action: Preserve privacy, cancellation, no-write deferral, sealed source replacement, exact cursor/CAS and atomic locator/transaction behavior; these extensions do not replace Native facts or LCM.
- Associated check and owner: Stable/mismatched sealed admission, source identity, cursor round-trip/CAS, locator rollback/reopen and ingest fixtures; owner rn-verify-sessions and rn-host-fixtures.

### 23. crates/tracedecay-sessions/src/runtime/observation/jsonl_observation_admission/tests.rs

- Original path (570): crates/tracedecay-sessions/src/runtime/observation/jsonl_observation_admission/tests.rs.
- Current path (execution head): crates/tracedecay-sessions/src/runtime/observation/jsonl_observation_admission/tests.rs.
- Status: M.
- Hunks (1): @@ -1474,0 +1475,234 @@ async fn exact_hook_prepares_an_in_scope_window_concurrently() {
- Classification: H — pre-existing shared host/session extension.
- Original behavior: Host admission, Codex lookup, JSONL identity, observation admission, cursor CAS, provenance and transcript store access protect canonical session evidence.
- Permitted action: Preserve privacy, cancellation, no-write deferral, sealed source replacement, exact cursor/CAS and atomic locator/transaction behavior; these extensions do not replace Native facts or LCM.
- Associated check and owner: Stable/mismatched sealed admission, source identity, cursor round-trip/CAS, locator rollback/reopen and ingest fixtures; owner rn-verify-sessions and rn-host-fixtures.

### 24. crates/tracedecay-sessions/src/runtime/source.rs

- Original path (570): crates/tracedecay-sessions/src/runtime/source.rs.
- Current path (execution head): crates/tracedecay-sessions/src/runtime/source.rs.
- Status: M.
- Hunks (5): @@ -904,3 +904,4 @@ pub use jsonl::{ | @@ -1106 +1107 @@ fn should_resume_jsonl(prev: StoredCursor, file_size: u64, mtime: u64, file_id: | @@ -1109,0 +1111,35 @@ fn stable_jsonl_file_id( | @@ -1136 +1171,0 @@ fn stable_jsonl_file_id( | @@ -1141 +1176 @@ fn stable_jsonl_file_id(
- Classification: H — pre-existing shared host/session extension.
- Original behavior: Host admission, Codex lookup, JSONL identity, observation admission, cursor CAS, provenance and transcript store access protect canonical session evidence.
- Permitted action: Preserve privacy, cancellation, no-write deferral, sealed source replacement, exact cursor/CAS and atomic locator/transaction behavior; these extensions do not replace Native facts or LCM.
- Associated check and owner: Stable/mismatched sealed admission, source identity, cursor round-trip/CAS, locator rollback/reopen and ingest fixtures; owner rn-verify-sessions and rn-host-fixtures.

### 25. crates/tracedecay-sessions/src/runtime/source/jsonl.rs

- Original path (570): crates/tracedecay-sessions/src/runtime/source/jsonl.rs.
- Current path (execution head): crates/tracedecay-sessions/src/runtime/source/jsonl.rs.
- Status: M.
- Hunks (4): @@ -19,3 +19,3 @@ use super::{ | @@ -156,0 +157,42 @@ pub(in crate::runtime) fn jsonl_native_file_identity( | @@ -542,0 +585,229 @@ pub struct JsonlResumeState { | @@ -1907,0 +2179,297 @@ mod tests {
- Classification: H — pre-existing shared host/session extension.
- Original behavior: Host admission, Codex lookup, JSONL identity, observation admission, cursor CAS, provenance and transcript store access protect canonical session evidence.
- Permitted action: Preserve privacy, cancellation, no-write deferral, sealed source replacement, exact cursor/CAS and atomic locator/transaction behavior; these extensions do not replace Native facts or LCM.
- Associated check and owner: Stable/mismatched sealed admission, source identity, cursor round-trip/CAS, locator rollback/reopen and ingest fixtures; owner rn-verify-sessions and rn-host-fixtures.

### 26. crates/tracedecay-sessions/src/runtime/store_access/transcript.rs

- Original path (570): crates/tracedecay-sessions/src/runtime/store_access/transcript.rs.
- Current path (execution head): crates/tracedecay-sessions/src/runtime/store_access/transcript.rs.
- Status: M.
- Hunks (11): @@ -11 +11,3 @@ use super::super::git_correlation::{ | @@ -138,0 +141 @@ pub async fn get_parse_offset( | @@ -154,2 +157,2 @@ pub async fn get_parse_offset( | @@ -176,2 +179,2 @@ pub async fn get_parse_offset( | @@ -197,8 +200,62 @@ fn sqlite_missing_column(error: &tracedecay_runtime_core::db::engine::Error, col | @@ -246,0 +304 @@ pub async fn set_parse_offset( | @@ -256,2 +314,2 @@ pub async fn set_parse_offset( | @@ -275,0 +334,63 @@ impl<D: SessionRegisteredDb + Sync> SessionStoreAccess<'_, D> { | @@ -860,0 +982,4 @@ impl<D: SessionRegisteredDb + Sync> SessionStoreAccess<'_, D> { | @@ -874,2 +999,6 @@ impl<D: SessionRegisteredDb + Sync> SessionStoreAccess<'_, D> { | @@ -981,0 +1111,28 @@ mod tests {
- Classification: H — pre-existing shared host/session extension.
- Original behavior: Host admission, Codex lookup, JSONL identity, observation admission, cursor CAS, provenance and transcript store access protect canonical session evidence.
- Permitted action: Preserve privacy, cancellation, no-write deferral, sealed source replacement, exact cursor/CAS and atomic locator/transaction behavior; these extensions do not replace Native facts or LCM.
- Associated check and owner: Stable/mismatched sealed admission, source identity, cursor round-trip/CAS, locator rollback/reopen and ingest fixtures; owner rn-verify-sessions and rn-host-fixtures.

### 27. crates/tracedecay-store-runtime/src/retained_memory.rs

- Original path (570): crates/tracedecay-store-runtime/src/retained_memory.rs.
- Current path (execution head): crates/tracedecay-store-runtime/src/retained_memory.rs.
- Status: M.
- Hunks (1): @@ -1051 +1051 @@ async fn execute_status_on_db(
- Classification: H — pre-existing shared retained-route extension.
- Original behavior: The retained memory route selects the owner-bound Native application and executes retained operations.
- Permitted action: Preserve the direct owner route and visibility seam; do not replace operation bodies or introduce a provider store.
- Associated check and owner: Owner routing, fact receipts, scope and no-cross-authority fixtures; owner rn-verify-facts and rn-native-port.

### 28. crates/tracedecay/src/daemon/project_composition.rs

- Original path (570): crates/tracedecay/src/daemon/project_composition.rs.
- Current path (execution head): crates/tracedecay/src/daemon/project_composition.rs.
- Status: M.
- Hunks (26): @@ -19,0 +20,7 @@ use tracedecay_session_runtime::session_temporal_refresh_scheduler::{ | @@ -22,0 +30,2 @@ mod future_size_tests; | @@ -28,0 +38,4 @@ use code_index_activation::{ | @@ -43,0 +57,447 @@ pub(super) struct ProductionProjectComposition { | @@ -256,0 +717 @@ struct ProjectOpenInputs<'a> { | @@ -285,0 +747,43 @@ pub(super) async fn production_project_server( | @@ -296,0 +801 @@ pub(super) async fn production_project_server( | @@ -319,0 +825,2 @@ pub(super) async fn production_project_server( | @@ -321 +828,7 @@ pub(super) async fn production_project_server( | @@ -323 +836,6 @@ pub(super) async fn production_project_server( | @@ -330,0 +849,2 @@ pub(super) async fn production_project_server( | @@ -487,0 +1008,6 @@ struct ComposedCoreServer { | @@ -543,0 +1070,8 @@ impl ComposedCoreServer { | @@ -577,0 +1112,10 @@ struct PublishedFullServer { | @@ -764,0 +1309,3 @@ impl ProjectOpenInputs<'_> { | @@ -815,0 +1363,94 @@ impl ProjectOpenInputs<'_> { | @@ -899,0 +1541,6 @@ impl ProjectOpenInputs<'_> { | @@ -1160,0 +1808,27 @@ impl ProjectOpenInputs<'_> { | @@ -1203,0 +1878,200 @@ impl ProjectOpenInputs<'_> { | @@ -1252,9 +2126,12 @@ impl ProjectOpenInputs<'_> { | @@ -1280,15 +2157,19 @@ impl ProjectOpenInputs<'_> { | @@ -1401,0 +2283,7 @@ impl ProjectOpenInputs<'_> { | @@ -1432,0 +2321,2 @@ impl ProjectOpenInputs<'_> { | @@ -1557,0 +2448,3 @@ impl ProjectOpenInputs<'_> { | @@ -1559,0 +2453,25 @@ impl ProjectOpenInputs<'_> { | @@ -2176,0 +3095,193 @@ async fn retire_failed_project_open_owner(
- Classification: C — product daemon composition mount.
- Original behavior: The product composition root mounts host/session authorities, provider capabilities and daemon lifecycle services.
- Permitted action: Keep composition wiring narrow and owner-bound. It is not evidence of Native parity; selected Native remains blocked until canonical facts/session/LCM ports are connected and staged construction is removed.
- Associated check and owner: Composition reachability, no staged DB construction and restart/no-effect checks; owner rn-native-port and rn-build.

### 29. crates/tracedecay/src/daemon/retained_owner/claude_host_journey_tests.rs

- Original path (570): absent at 570.
- Current path (execution head): crates/tracedecay/src/daemon/retained_owner/claude_host_journey_tests.rs.
- Status: A.
- Hunks (1): @@ -0,0 +1,1207 @@
- Classification: P — product Claude host journey fixture.
- Original behavior: The product Claude journey fixture has no 570 counterpart and exercises host/provider composition.
- Permitted action: Use only to validate the host boundary. It cannot stand in for original Native behavior or Hermes admission.
- Associated check and owner: Shipped Claude host/observer journey and output/receipt checks; owner rn-verify-claude.

### 30. crates/tracedecay/src/daemon/retained_owner/cognitive_recall.rs

- Original path (570): absent at 570.
- Current path (execution head): crates/tracedecay/src/daemon/retained_owner/cognitive_recall.rs.
- Status: A.
- Hunks (1): @@ -0,0 +1,9055 @@
- Classification: P — product cognitive-recall surface.
- Original behavior: The product cognitive-recall route coordinates advisory provider delivery and attribution; it has no 570 counterpart.
- Permitted action: Keep recall advisory, exact-scope admitted and single-settlement. It cannot write Native facts, trust, sessions or LCM.
- Associated check and owner: One canonical memory_matches settlement and no-Native-mutation checks; owner rn-host-context.

### 31. crates/tracedecay/src/daemon/retained_owner/cognitive_recall/control_attribution.rs

- Original path (570): absent at 570.
- Current path (execution head): crates/tracedecay/src/daemon/retained_owner/cognitive_recall/control_attribution.rs.
- Status: A.
- Hunks (1): @@ -0,0 +1,1280 @@
- Classification: P — product cognitive-recall surface.
- Original behavior: The product cognitive-recall route coordinates advisory provider delivery and attribution; it has no 570 counterpart.
- Permitted action: Keep recall advisory, exact-scope admitted and single-settlement. It cannot write Native facts, trust, sessions or LCM.
- Associated check and owner: One canonical memory_matches settlement and no-Native-mutation checks; owner rn-host-context.

### 32. crates/tracedecay/src/daemon/retained_owner/cognitive_recall/test_context_evidence.rs

- Original path (570): absent at 570.
- Current path (execution head): crates/tracedecay/src/daemon/retained_owner/cognitive_recall/test_context_evidence.rs.
- Status: A.
- Hunks (1): @@ -0,0 +1,653 @@
- Classification: P — product cognitive-recall surface.
- Original behavior: The product cognitive-recall route coordinates advisory provider delivery and attribution; it has no 570 counterpart.
- Permitted action: Keep recall advisory, exact-scope admitted and single-settlement. It cannot write Native facts, trust, sessions or LCM.
- Associated check and owner: One canonical memory_matches settlement and no-Native-mutation checks; owner rn-host-context.

### 33. crates/tracedecay/src/daemon/retained_owner/native_baseline_tests.rs

- Original path (570): absent at 570.
- Current path (execution head): crates/tracedecay/src/daemon/retained_owner/native_baseline_tests.rs.
- Status: A.
- Hunks (1): @@ -0,0 +1,367 @@
- Classification: P — product-added surface absent from 570.
- Original behavior: No 570 counterpart; this product surface describes provider, host or integration behavior.
- Permitted action: Keep the surface additive and removable. It cannot authorize a second Native authority or claim parity without an independent original-boundary check.
- Associated check and owner: Source ownership check plus focused route/effect/scope fixtures; owner rn-source-docs until delegated.

### 34. crates/tracedecay/src/daemon/retained_owner/native_common_factory_tests.rs

- Original path (570): absent at 570.
- Current path (execution head): crates/tracedecay/src/daemon/retained_owner/native_common_factory_tests.rs.
- Status: A.
- Hunks (1): @@ -0,0 +1,2842 @@
- Classification: P — product Native provider fixture/support surface.
- Original behavior: Product fixtures and common helpers describe adapter envelopes and provisional routes; they have no 570 counterpart.
- Permitted action: Use only as independent test scaffolding. Fixtures must compare original typed operations and may not bless staged state or fabricate a route.
- Associated check and owner: Immutable process/source/profile identity and no-empty-success fixtures; owner rn-native-tests and rn-reference.

### 35. crates/tracedecay/src/daemon/retained_owner/native_common_tests.rs

- Original path (570): absent at 570.
- Current path (execution head): crates/tracedecay/src/daemon/retained_owner/native_common_tests.rs.
- Status: A.
- Hunks (1): @@ -0,0 +1,3376 @@
- Classification: P — product Native provider fixture/support surface.
- Original behavior: Product fixtures and common helpers describe adapter envelopes and provisional routes; they have no 570 counterpart.
- Permitted action: Use only as independent test scaffolding. Fixtures must compare original typed operations and may not bless staged state or fabricate a route.
- Associated check and owner: Immutable process/source/profile identity and no-empty-success fixtures; owner rn-native-tests and rn-reference.

### 36. crates/tracedecay/src/daemon/retained_owner/native_provider.rs

- Original path (570): absent at 570.
- Current path (execution head): crates/tracedecay/src/daemon/retained_owner/native_provider.rs.
- Status: A.
- Hunks (1): @@ -0,0 +1,4080 @@
- Classification: P — product Native provider adapter/scaffold.
- Original behavior: The product adapter exposes a provider envelope around Native candidates; these files have no 570 counterpart.
- Permitted action: Delegate to owner-bound canonical Native facts/session/LCM ports and preserve typed unsupported/no-effect behavior; adapter tests cannot claim implementation complete.
- Associated check and owner: Independent 570-versus-candidate operation/effect/receipt parity; owner rn-native-port and rn-native-tests.

### 37. crates/tracedecay/src/daemon/retained_owner/native_provider_parity_tests.rs

- Original path (570): absent at 570.
- Current path (execution head): crates/tracedecay/src/daemon/retained_owner/native_provider_parity_tests.rs.
- Status: A.
- Hunks (1): @@ -0,0 +1,596 @@
- Classification: P — product Native provider adapter/scaffold.
- Original behavior: The product adapter exposes a provider envelope around Native candidates; these files have no 570 counterpart.
- Permitted action: Delegate to owner-bound canonical Native facts/session/LCM ports and preserve typed unsupported/no-effect behavior; adapter tests cannot claim implementation complete.
- Associated check and owner: Independent 570-versus-candidate operation/effect/receipt parity; owner rn-native-port and rn-native-tests.

### 38. crates/tracedecay/src/daemon/retained_owner/native_provider_tests.rs

- Original path (570): absent at 570.
- Current path (execution head): crates/tracedecay/src/daemon/retained_owner/native_provider_tests.rs.
- Status: A.
- Hunks (1): @@ -0,0 +1,3251 @@
- Classification: P — product Native provider adapter/scaffold.
- Original behavior: The product adapter exposes a provider envelope around Native candidates; these files have no 570 counterpart.
- Permitted action: Delegate to owner-bound canonical Native facts/session/LCM ports and preserve typed unsupported/no-effect behavior; adapter tests cannot claim implementation complete.
- Associated check and owner: Independent 570-versus-candidate operation/effect/receipt parity; owner rn-native-port and rn-native-tests.

### 39. crates/tracedecay/src/daemon/retained_owner/native_staged_observations.rs

- Original path (570): absent at 570.
- Current path (execution head): crates/tracedecay/src/daemon/retained_owner/native_staged_observations.rs.
- Status: A.
- Hunks (1): @@ -0,0 +1,5677 @@
- Classification: P — product staged Native substitute.
- Original behavior: The product-local staged observation route owns substitute bytes, lifecycle and replay state; it has no 570 counterpart.
- Permitted action: Retire selected Native construction/read/write while leaving existing SQLite/WAL/SHM bytes and mtimes untouched. No staged row may satisfy Native recovery.
- Associated check and owner: Staged-recovery negative control and fresh-start no-construction fixture; owner rn-native-port and rn-native-tests.

### 40. crates/tracedecay/src/daemon/retained_owner/observation_journey.rs

- Original path (570): absent at 570.
- Current path (execution head): crates/tracedecay/src/daemon/retained_owner/observation_journey.rs.
- Status: A.
- Hunks (1): @@ -0,0 +1,11247 @@
- Classification: P — product observation-journey surface.
- Original behavior: The product journey coordinates canonical host settlement, provider fan-out and lifecycle observation; it has no 570 counterpart.
- Permitted action: Keep canonical observation admission separate from provider-local observations and preserve crash/restart/idempotency partitions.
- Associated check and owner: Host journey, crash/restart, cancellation and duplicate-delivery fixtures; owner rn-session-delivery and rn-host-fixtures.

### 41. crates/tracedecay/src/daemon/retained_owner/observation_journey/control_dispatch.rs

- Original path (570): absent at 570.
- Current path (execution head): crates/tracedecay/src/daemon/retained_owner/observation_journey/control_dispatch.rs.
- Status: A.
- Hunks (1): @@ -0,0 +1,560 @@
- Classification: P — product observation-journey surface.
- Original behavior: The product journey coordinates canonical host settlement, provider fan-out and lifecycle observation; it has no 570 counterpart.
- Permitted action: Keep canonical observation admission separate from provider-local observations and preserve crash/restart/idempotency partitions.
- Associated check and owner: Host journey, crash/restart, cancellation and duplicate-delivery fixtures; owner rn-session-delivery and rn-host-fixtures.

### 42. crates/tracedecay/src/daemon/retained_owner/observation_journey/tests/control_dispatch.rs

- Original path (570): absent at 570.
- Current path (execution head): crates/tracedecay/src/daemon/retained_owner/observation_journey/tests/control_dispatch.rs.
- Status: A.
- Hunks (1): @@ -0,0 +1,410 @@
- Classification: P — product observation-journey surface.
- Original behavior: The product journey coordinates canonical host settlement, provider fan-out and lifecycle observation; it has no 570 counterpart.
- Permitted action: Keep canonical observation admission separate from provider-local observations and preserve crash/restart/idempotency partitions.
- Associated check and owner: Host journey, crash/restart, cancellation and duplicate-delivery fixtures; owner rn-session-delivery and rn-host-fixtures.

### 43. crates/tracedecay/src/daemon/retained_owner/observation_journey/tests/crash_restart_fuzz.rs

- Original path (570): absent at 570.
- Current path (execution head): crates/tracedecay/src/daemon/retained_owner/observation_journey/tests/crash_restart_fuzz.rs.
- Status: A.
- Hunks (1): @@ -0,0 +1,2473 @@
- Classification: P — product observation-journey surface.
- Original behavior: The product journey coordinates canonical host settlement, provider fan-out and lifecycle observation; it has no 570 counterpart.
- Permitted action: Keep canonical observation admission separate from provider-local observations and preserve crash/restart/idempotency partitions.
- Associated check and owner: Host journey, crash/restart, cancellation and duplicate-delivery fixtures; owner rn-session-delivery and rn-host-fixtures.

### 44. crates/tracedecay/src/daemon/retained_owner/observation_journey/tests/provider_history.rs

- Original path (570): absent at 570.
- Current path (execution head): crates/tracedecay/src/daemon/retained_owner/observation_journey/tests/provider_history.rs.
- Status: A.
- Hunks (1): @@ -0,0 +1,1551 @@
- Classification: P — product observation-journey surface.
- Original behavior: The product journey coordinates canonical host settlement, provider fan-out and lifecycle observation; it has no 570 counterpart.
- Permitted action: Keep canonical observation admission separate from provider-local observations and preserve crash/restart/idempotency partitions.
- Associated check and owner: Host journey, crash/restart, cancellation and duplicate-delivery fixtures; owner rn-session-delivery and rn-host-fixtures.

### 45. crates/tracedecay/src/daemon/retained_owner/observation_journey/tests/real_ncm_observer.rs

- Original path (570): absent at 570.
- Current path (execution head): crates/tracedecay/src/daemon/retained_owner/observation_journey/tests/real_ncm_observer.rs.
- Status: A.
- Hunks (1): @@ -0,0 +1,468 @@
- Classification: P — product observation-journey surface.
- Original behavior: The product journey coordinates canonical host settlement, provider fan-out and lifecycle observation; it has no 570 counterpart.
- Permitted action: Keep canonical observation admission separate from provider-local observations and preserve crash/restart/idempotency partitions.
- Associated check and owner: Host journey, crash/restart, cancellation and duplicate-delivery fixtures; owner rn-session-delivery and rn-host-fixtures.

### 46. crates/tracedecay/src/daemon/retained_owner/provider_control.rs

- Original path (570): absent at 570.
- Current path (execution head): crates/tracedecay/src/daemon/retained_owner/provider_control.rs.
- Status: A.
- Hunks (1): @@ -0,0 +1,1123 @@
- Classification: P — product provider-control surface.
- Original behavior: The product control, feedback, portability, projection and state receipts describe provider-local behavior; they have no 570 counterpart.
- Permitted action: Keep controls typed and provider-local. Unsupported controls must not become fake Native operations or mutate Native trust.
- Associated check and owner: Provider control/effect-ledger and unsupported/no-effect fixtures; owner rn-native-port and rn-native-tests.

### 47. crates/tracedecay/src/daemon/retained_owner/provider_control/authority.rs

- Original path (570): absent at 570.
- Current path (execution head): crates/tracedecay/src/daemon/retained_owner/provider_control/authority.rs.
- Status: A.
- Hunks (1): @@ -0,0 +1,843 @@
- Classification: P — product provider-control surface.
- Original behavior: The product control, feedback, portability, projection and state receipts describe provider-local behavior; they have no 570 counterpart.
- Permitted action: Keep controls typed and provider-local. Unsupported controls must not become fake Native operations or mutate Native trust.
- Associated check and owner: Provider control/effect-ledger and unsupported/no-effect fixtures; owner rn-native-port and rn-native-tests.

### 48. crates/tracedecay/src/daemon/retained_owner/provider_control/feedback_receipt.rs

- Original path (570): absent at 570.
- Current path (execution head): crates/tracedecay/src/daemon/retained_owner/provider_control/feedback_receipt.rs.
- Status: A.
- Hunks (1): @@ -0,0 +1,2087 @@
- Classification: P — product provider-control surface.
- Original behavior: The product control, feedback, portability, projection and state receipts describe provider-local behavior; they have no 570 counterpart.
- Permitted action: Keep controls typed and provider-local. Unsupported controls must not become fake Native operations or mutate Native trust.
- Associated check and owner: Provider control/effect-ledger and unsupported/no-effect fixtures; owner rn-native-port and rn-native-tests.

### 49. crates/tracedecay/src/daemon/retained_owner/provider_control/outcome.rs

- Original path (570): absent at 570.
- Current path (execution head): crates/tracedecay/src/daemon/retained_owner/provider_control/outcome.rs.
- Status: A.
- Hunks (1): @@ -0,0 +1,1453 @@
- Classification: P — product provider-control surface.
- Original behavior: The product control, feedback, portability, projection and state receipts describe provider-local behavior; they have no 570 counterpart.
- Permitted action: Keep controls typed and provider-local. Unsupported controls must not become fake Native operations or mutate Native trust.
- Associated check and owner: Provider control/effect-ledger and unsupported/no-effect fixtures; owner rn-native-port and rn-native-tests.

### 50. crates/tracedecay/src/daemon/retained_owner/provider_control/portability.rs

- Original path (570): absent at 570.
- Current path (execution head): crates/tracedecay/src/daemon/retained_owner/provider_control/portability.rs.
- Status: A.
- Hunks (1): @@ -0,0 +1,1981 @@
- Classification: P — product provider-control surface.
- Original behavior: The product control, feedback, portability, projection and state receipts describe provider-local behavior; they have no 570 counterpart.
- Permitted action: Keep controls typed and provider-local. Unsupported controls must not become fake Native operations or mutate Native trust.
- Associated check and owner: Provider control/effect-ledger and unsupported/no-effect fixtures; owner rn-native-port and rn-native-tests.

### 51. crates/tracedecay/src/daemon/retained_owner/provider_control/projection.rs

- Original path (570): absent at 570.
- Current path (execution head): crates/tracedecay/src/daemon/retained_owner/provider_control/projection.rs.
- Status: A.
- Hunks (1): @@ -0,0 +1,2675 @@
- Classification: P — product provider-control surface.
- Original behavior: The product control, feedback, portability, projection and state receipts describe provider-local behavior; they have no 570 counterpart.
- Permitted action: Keep controls typed and provider-local. Unsupported controls must not become fake Native operations or mutate Native trust.
- Associated check and owner: Provider control/effect-ledger and unsupported/no-effect fixtures; owner rn-native-port and rn-native-tests.

### 52. crates/tracedecay/src/daemon/retained_owner/provider_control/source_controls.rs

- Original path (570): absent at 570.
- Current path (execution head): crates/tracedecay/src/daemon/retained_owner/provider_control/source_controls.rs.
- Status: A.
- Hunks (1): @@ -0,0 +1,891 @@
- Classification: P — product provider-control surface.
- Original behavior: The product control, feedback, portability, projection and state receipts describe provider-local behavior; they have no 570 counterpart.
- Permitted action: Keep controls typed and provider-local. Unsupported controls must not become fake Native operations or mutate Native trust.
- Associated check and owner: Provider control/effect-ledger and unsupported/no-effect fixtures; owner rn-native-port and rn-native-tests.

### 53. crates/tracedecay/src/daemon/retained_owner/provider_control/state_controls.rs

- Original path (570): absent at 570.
- Current path (execution head): crates/tracedecay/src/daemon/retained_owner/provider_control/state_controls.rs.
- Status: A.
- Hunks (1): @@ -0,0 +1,312 @@
- Classification: P — product provider-control surface.
- Original behavior: The product control, feedback, portability, projection and state receipts describe provider-local behavior; they have no 570 counterpart.
- Permitted action: Keep controls typed and provider-local. Unsupported controls must not become fake Native operations or mutate Native trust.
- Associated check and owner: Provider control/effect-ledger and unsupported/no-effect fixtures; owner rn-native-port and rn-native-tests.

### 54. crates/tracedecay/src/daemon/retained_owner/provider_control/tests/source_controls.rs

- Original path (570): absent at 570.
- Current path (execution head): crates/tracedecay/src/daemon/retained_owner/provider_control/tests/source_controls.rs.
- Status: A.
- Hunks (1): @@ -0,0 +1,1105 @@
- Classification: P — product provider-control surface.
- Original behavior: The product control, feedback, portability, projection and state receipts describe provider-local behavior; they have no 570 counterpart.
- Permitted action: Keep controls typed and provider-local. Unsupported controls must not become fake Native operations or mutate Native trust.
- Associated check and owner: Provider control/effect-ledger and unsupported/no-effect fixtures; owner rn-native-port and rn-native-tests.

### 55. crates/tracedecay/src/daemon/retained_owner/provider_history.rs

- Original path (570): absent at 570.
- Current path (execution head): crates/tracedecay/src/daemon/retained_owner/provider_history.rs.
- Status: A.
- Hunks (1): @@ -0,0 +1,2111 @@
- Classification: P — product provider-history surface.
- Original behavior: The product provider history receipt surface has no 570 counterpart.
- Permitted action: Keep it advisory and separate from canonical Native fact/session/LCM history; preserve source, scope and receipt identity.
- Associated check and owner: History portability and scope/identity fixtures; owner rn-native-tests.

### 56. crates/tracedecay/src/mcp/tools/handlers/hook_runtime/ingest.rs

- Original path (570): crates/tracedecay/src/mcp/tools/handlers/hook_runtime/ingest.rs.
- Current path (execution head): crates/tracedecay/src/mcp/tools/handlers/hook_runtime/ingest.rs.
- Status: M.
- Hunks (9): @@ -68,11 +68,30 @@ fn host_admission_facade<'a>( | @@ -80,2 +99 @@ fn host_admission_facade<'a>( | @@ -119,0 +138 @@ async fn admit_codex_project_rollouts( | @@ -124,2 +143,15 @@ async fn admit_codex_project_rollouts( | @@ -131,0 +164 @@ async fn admit_codex_project_rollouts( | @@ -321,0 +355 @@ async fn admit_codex_rollouts_once( | @@ -576,0 +611,7 @@ pub(super) async fn ingest_transcript( | @@ -589,0 +631,24 @@ pub(crate) async fn ingest_transcript_with_cancellation( | @@ -633 +698,2 @@ pub(crate) async fn ingest_transcript_with_cancellation(
- Classification: H — pre-existing shared host-ingestion extension.
- Original behavior: Hook ingestion admits host material through canonical source, scope, privacy, cursor and receipt authorities.
- Permitted action: Preserve canonical admission and typed failure/no-write behavior. Hermes inline ingestion has no canonical observation bridge and remains an explicit integration gap.
- Associated check and owner: Host admission/origin/privacy/cursor checks plus Hermes fixture or typed deferred outcome; owner rn-session-delivery and rn-host-fixtures.

## Accepted route corrections

- Public session_lookup is routed by DaemonSessionLookupPrimitiveV1 through SessionApplicationRetrievalPortV1::retrieve_admitted. It is an exact-session application operation distinct from retained MessageSearch; preserving MessageSearch does not cover session_lookup.
- RetainedSurfaceOperation::FactStoreCurate is an explicit automation/effect-ledger route through RetainedAutomationExecutionPortV1 and the retained curator. ProjectMemoryFactStore::apply_project_memory_fact_curation is a lower-level curation transaction and does not prove the retained route.
- recent_sessions, session_providers and session_replay_slice remain lower-level RegisteredGlobalDb/SessionStoreAccess queries. They are absent from RetainedLcmRequestV1 and DirectRetainedLcmPortV1 and have no public CLI/MCP/HTTP/SDK binding.
- lcm_compress is daemon-internal mounted LCM lifecycle work. preflight and session_boundary are retired or daemon-internal helpers. None is a public CLI/MCP operation.
- SessionsFor and Workflows are project-authority-only. Profile authority must return typed unsupported; neither operation is an alias for session_lookup.

## LCM algorithm and schema parity

No scoped hunk changes the tracedecay-lcm crate between 570 and this execution head. The original LCM compression, raw protection, summary DAG, payload authority, query schemas, lifecycle state, retry and receipts remain protected. A parity check must compare algorithms and schemas at the original 570 boundary; source equality alone does not claim runtime parity or implementation completion.

## Hermes integration gap

Hermes inline ingestion currently has no canonical observation bridge into session/LCM admission. It cannot be treated as canonical Native evidence or silently staged as a provider observation. rn-session-delivery and rn-host-fixtures must connect Hermes inline events to canonical observation admission or return typed unsupported/deferred while preserving source, privacy, scope, cursor and receipt invariants.

## Required verification split

The exact restoration owner checks the retrieval-anchor blob against 570 and runs ordinary, missing-history and invalid-history fixtures. Shared host/session owners separately check stable and mismatched sealed admission, source identity, ordinary and reserved cursor round-trip, exact CAS, locator conflict/rollback/reopen, provenance mismatch/fallback, historical catch-up and refresh begin/status/cancel after restart.
Native parity owners compare independent 570 and candidate process/source/profile identities at every supported original fact, session and LCM operation boundary, including typed unsupported and no-effect outcomes. Provider or composition tests cannot substitute for these comparisons.
Storage owners check interruptible directory creation, lock and durability semantics. Privacy owners retain the historical detector audit. Host owners check canonical ingestion and Hermes gap handling.
This ledger is documentation evidence only. Unresolved implementation owners remain rn-restore-anchor, rn-native-port, rn-native-tests, rn-verify-facts, rn-verify-sessions, rn-verify-state, rn-session-delivery, rn-host-fixtures, rn-privacy-audit and rn-build; each must provide the named implementation or production evidence before the affected lane can close.
