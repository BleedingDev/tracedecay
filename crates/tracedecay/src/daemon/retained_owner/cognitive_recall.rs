//! Production mount of the cognitive-recall port over one project's provider
//! composition.
//!
//! The composition root owns three things the registry crate deliberately
//! leaves to the host, and this module is the only place they are supplied:
//!
//! * the **exact coding scope** of a recall is bound from the authoritative
//!   resolved scope the daemon resolved at project open, never from the
//!   request alone: a request whose scope disagrees with the mounted checkout
//!   on any identity field is refused before any provider contact, and the
//!   provider-qualified `agent_session_id` comes from the same derivation the
//!   observation journey uses ([`provider_agent_session_id`]), so one host
//!   session has exactly one provider identity per checkout;
//! * the **admission ledger** is durable: every admission report the port
//!   produces is retained in a project-owned SQLite ledger before the result
//!   is delivered, so denied candidates remain audit-visible even though the
//!   application result carries only admitted content; the ledger stores
//!   denial rows without any candidate content by construction;
//! * the **host budgets and policy revision** are product-owned constants,
//!   not provider claims;
//! * the **routing policy** — which provider may answer, under which
//!   registration revision, and whether any fallback rule is pinned — is
//!   built once by the composition root from the configured routing gate and
//!   handed to every session port unchanged, so no port and no provider can
//!   choose a different provider than the one the configuration named.
//!
//! The port is minted per session through
//! [`ProjectCognitiveRecallMountV1::port_for_session`]; the mount itself lives
//! for exactly one project-server lifetime.

use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use rusqlite::{Connection, OptionalExtension, params};
use tracedecay_application::{ResolvedScope, try_now_micros};
use tracedecay_domain::{ProjectId, UserProfileId};
use tracedecay_memory_provider_registry::{
    ActiveRoutingPolicy, CognitiveRecallPortError, CognitiveRecallPortInputsV1, ExactScopeBinding,
    ExactScopeBindingError, OwnedExactScope, ProjectCognitiveRecallPortV1,
    ProjectMemoryProviderComposition, ProviderLimits, RecallAdmissionAuditError,
    RecallAdmissionObserver, RecallAdmissionReport, RecallBudgetsV1, ScopeBinding,
};

use super::observation_journey::provider_agent_session_id;

/// File name of the project-owned recall admission ledger inside the
/// canonical store layout. Placement only; never an identity input.
const LEDGER_FILE_NAME: &str = "memory-recall-admission-ledger-v1.sqlite3";

/// Pinned recall policy revision carried in every request.
const PROJECT_RECALL_POLICY_REVISION: u64 = 1;

/// Product-owned per-request recall budgets. These are the budgets the
/// coding-memory evaluation scenarios declare for a project recall and stay
/// under the Native provider's negotiated `recall_candidates` limit.
const PROJECT_RECALL_BUDGETS: RecallBudgetsV1 = RecallBudgetsV1 {
    maximum_candidates: 8,
    maximum_candidate_content_bytes: 4_096,
    maximum_total_content_bytes: 8_192,
    maximum_source_refs_per_candidate: 8,
    maximum_trace_refs_per_candidate: 8,
    maximum_warnings: 8,
    maximum_extensions_per_candidate: 8,
};

/// Typed failure of mounting the recall port or minting a session port.
#[derive(Debug, thiserror::Error)]
pub enum CognitiveRecallMountError {
    /// The provider composition is disabled, so no recall route exists.
    #[error("memory-provider composition is disabled; no cognitive recall route exists")]
    CompositionDisabled,
    /// The provider host is enabled but the routing gate names no active
    /// provider, so every registered provider is an observer and no recall
    /// route exists. This is distinct from a disabled composition and from a
    /// provider that is unavailable.
    #[error(
        "memory provider recall routing names no active provider; observer-only composition has \
         no cognitive recall route"
    )]
    NoActiveProviderConfigured,
    /// The mount inputs disagree with the authoritative project identity.
    #[error(
        "cognitive recall mount inputs disagree with the authoritative scope on {field}: \
         expected {expected}, received {received}"
    )]
    ScopeDisagreement {
        /// Which identity disagreed.
        field: &'static str,
        /// The authoritative value.
        expected: String,
        /// The value the caller supplied.
        received: String,
    },
    /// The canonical session identity is not a usable identifier.
    #[error("canonical session identity is empty, untrimmed, or carries control characters")]
    SessionIdentityInvalid,
    /// The admission ledger could not be opened or initialised.
    #[error("recall admission ledger at {path} could not be opened: {source}")]
    LedgerOpen {
        /// Storage placement of the ledger, for diagnostics only.
        path: PathBuf,
        /// Underlying SQLite failure.
        #[source]
        source: rusqlite::Error,
    },
    /// The registry port refused the mount inputs.
    #[error("cognitive recall port refused the mount: {0}")]
    Port(#[source] CognitiveRecallPortError),
}

/// Typed failure of retaining one admission report.
#[derive(Debug, thiserror::Error)]
pub enum RecallAdmissionLedgerError {
    /// The host clock could not stamp the ledger row.
    #[error("host clock unavailable for the recall admission ledger: {0}")]
    Clock(#[source] tracedecay_application::ClockError),
    /// The report could not be serialised for its content digest.
    #[error("recall admission report could not be encoded: {0}")]
    Encode(#[source] serde_json::Error),
    /// A report with the same identity but different content is already
    /// retained. The ledger is append-only per request; a divergent replay is
    /// a defect, not something to overwrite.
    #[error(
        "recall admission report for request {request_id} under scope {exact_scope_sha256} is \
         already retained with different content"
    )]
    ConflictingReport {
        /// Request identity of the retained report.
        request_id: String,
        /// Exact-scope digest of the retained report.
        exact_scope_sha256: String,
    },
    /// The SQLite ledger refused the write.
    #[error("recall admission ledger write failed: {0}")]
    Sqlite(#[source] rusqlite::Error),
}

/// Outcome of retaining one report: the ledger is idempotent per
/// `(exact_scope_sha256, request_id)`, so a crash-recovery replay of an
/// identical report is a no-op rather than a duplicate.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RecallAdmissionLedgerWriteV1 {
    /// The report was retained by this write.
    Recorded,
    /// An identical report was already retained.
    AlreadyRecorded,
}

/// Durable, content-free ledger of recall admission reports.
///
/// One row per admission report plus one row per denied candidate. Neither
/// table has a content column:
/// [`DeniedRecallCandidate`](tracedecay_memory_provider_registry::DeniedRecallCandidate)
/// carries none, and admitted candidates are not retained at all.
pub struct RecallAdmissionLedgerV1 {
    path: PathBuf,
    connection: Mutex<Connection>,
}

impl std::fmt::Debug for RecallAdmissionLedgerV1 {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("RecallAdmissionLedgerV1")
            .field("path", &self.path)
            .finish_non_exhaustive()
    }
}

impl RecallAdmissionLedgerV1 {
    fn open(path: PathBuf) -> Result<Self, CognitiveRecallMountError> {
        let connection =
            Connection::open(&path).map_err(|source| CognitiveRecallMountError::LedgerOpen {
                path: path.clone(),
                source,
            })?;
        connection
            .execute_batch(
                "PRAGMA journal_mode = WAL;
                 PRAGMA synchronous = FULL;
                 PRAGMA foreign_keys = ON;
                 CREATE TABLE IF NOT EXISTS recall_admission_reports (
                     exact_scope_sha256 TEXT NOT NULL,
                     request_id TEXT NOT NULL,
                     report_sha256 TEXT NOT NULL,
                     temporal_mode TEXT NOT NULL,
                     evaluation_time TEXT NOT NULL,
                     unknown_validity_policy TEXT NOT NULL,
                     received_count INTEGER NOT NULL,
                     admitted_count INTEGER NOT NULL,
                     denied_count INTEGER NOT NULL,
                     degraded INTEGER NOT NULL,
                     warnings_json TEXT NOT NULL,
                     recorded_at_utc_micros INTEGER NOT NULL,
                     PRIMARY KEY (exact_scope_sha256, request_id)
                 ) STRICT;
                 CREATE TABLE IF NOT EXISTS recall_admission_denials (
                     exact_scope_sha256 TEXT NOT NULL,
                     request_id TEXT NOT NULL,
                     position INTEGER NOT NULL,
                     candidate_id TEXT NOT NULL,
                     stable_memory_ref TEXT,
                     reason_label TEXT NOT NULL,
                     reason_json TEXT NOT NULL,
                     provider_claimed_scope_binding TEXT NOT NULL,
                     provider_claimed_scope_sha256 TEXT,
                     provider_claimed_temporal_state TEXT NOT NULL,
                     PRIMARY KEY (exact_scope_sha256, request_id, position),
                     FOREIGN KEY (exact_scope_sha256, request_id)
                         REFERENCES recall_admission_reports (exact_scope_sha256, request_id)
                         ON DELETE CASCADE
                 ) STRICT;",
            )
            .map_err(|source| CognitiveRecallMountError::LedgerOpen {
                path: path.clone(),
                source,
            })?;
        // Ledgers written before candidates named a scope binding hold rows
        // whose candidates could only attest the full exact-scope shape, so
        // the historical claim is exactly `exact_coding_scope`.
        add_scope_binding_column_if_missing(&connection).map_err(|source| {
            CognitiveRecallMountError::LedgerOpen {
                path: path.clone(),
                source,
            }
        })?;
        Ok(Self {
            path,
            connection: Mutex::new(connection),
        })
    }

    /// Storage placement of the ledger.
    #[must_use]
    pub fn path(&self) -> &Path {
        &self.path
    }

    fn connection(&self) -> std::sync::MutexGuard<'_, Connection> {
        // A panic while holding the lock cannot leave a partial write behind:
        // every write is one SQLite transaction.
        match self.connection.lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        }
    }

    /// Retains one admission report and its denial rows atomically.
    pub fn record(
        &self,
        report: &RecallAdmissionReport,
    ) -> Result<RecallAdmissionLedgerWriteV1, RecallAdmissionLedgerError> {
        let recorded_at = try_now_micros().map_err(RecallAdmissionLedgerError::Clock)?;
        let report_bytes =
            serde_json::to_vec(report).map_err(RecallAdmissionLedgerError::Encode)?;
        let report_sha256 = tracedecay_domain::canonical_text::sha256_hex(&report_bytes);
        let warnings_json =
            serde_json::to_string(&report.warnings).map_err(RecallAdmissionLedgerError::Encode)?;
        let unknown_validity_policy = serde_json::to_value(report.unknown_validity_policy)
            .map_err(RecallAdmissionLedgerError::Encode)?;
        let unknown_validity_policy = unknown_validity_policy
            .as_str()
            .map(str::to_owned)
            .unwrap_or_else(|| unknown_validity_policy.to_string());
        let mut denial_rows = Vec::with_capacity(report.denied.len());
        for denied in &report.denied {
            let reason_json = serde_json::to_string(&denied.reason)
                .map_err(RecallAdmissionLedgerError::Encode)?;
            denial_rows.push((denied, reason_json));
        }

        let mut connection = self.connection();
        let transaction = connection
            .transaction()
            .map_err(RecallAdmissionLedgerError::Sqlite)?;
        let existing: Option<String> = transaction
            .query_row(
                "SELECT report_sha256 FROM recall_admission_reports
                 WHERE exact_scope_sha256 = ?1 AND request_id = ?2",
                params![report.exact_scope_sha256, report.request_id],
                |row| row.get(0),
            )
            .optional()
            .map_err(RecallAdmissionLedgerError::Sqlite)?;
        if let Some(existing) = existing {
            return if existing == report_sha256 {
                Ok(RecallAdmissionLedgerWriteV1::AlreadyRecorded)
            } else {
                Err(RecallAdmissionLedgerError::ConflictingReport {
                    request_id: report.request_id.clone(),
                    exact_scope_sha256: report.exact_scope_sha256.clone(),
                })
            };
        }
        transaction
            .execute(
                "INSERT INTO recall_admission_reports (
                     exact_scope_sha256, request_id, report_sha256, temporal_mode,
                     evaluation_time, unknown_validity_policy, received_count, admitted_count,
                     denied_count, degraded, warnings_json, recorded_at_utc_micros
                 ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12)",
                params![
                    report.exact_scope_sha256,
                    report.request_id,
                    report_sha256,
                    report.temporal_mode,
                    report.evaluation_time,
                    unknown_validity_policy,
                    i64::try_from(report.received_count).unwrap_or(i64::MAX),
                    i64::try_from(report.admitted_count).unwrap_or(i64::MAX),
                    i64::try_from(report.denied.len()).unwrap_or(i64::MAX),
                    i64::from(report.degraded),
                    warnings_json,
                    recorded_at.0,
                ],
            )
            .map_err(RecallAdmissionLedgerError::Sqlite)?;
        for (position, (denied, reason_json)) in denial_rows.iter().enumerate() {
            transaction
                .execute(
                    "INSERT INTO recall_admission_denials (
                         exact_scope_sha256, request_id, position, candidate_id,
                         stable_memory_ref, reason_label, reason_json,
                         provider_claimed_scope_binding, provider_claimed_scope_sha256,
                         provider_claimed_temporal_state
                     ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)",
                    params![
                        report.exact_scope_sha256,
                        report.request_id,
                        i64::try_from(position).unwrap_or(i64::MAX),
                        denied.candidate_id,
                        denied.stable_memory_ref,
                        denied.reason.label(),
                        reason_json,
                        denied.provider_claimed_scope_binding.as_wire(),
                        denied.provider_claimed_scope_sha256,
                        denied.provider_claimed_temporal_state,
                    ],
                )
                .map_err(RecallAdmissionLedgerError::Sqlite)?;
        }
        transaction
            .commit()
            .map_err(RecallAdmissionLedgerError::Sqlite)?;
        Ok(RecallAdmissionLedgerWriteV1::Recorded)
    }

    /// Number of retained reports.
    #[cfg(test)]
    fn report_count(&self) -> usize {
        self.connection()
            .query_row("SELECT COUNT(*) FROM recall_admission_reports", [], |row| {
                row.get::<_, i64>(0)
            })
            .ok()
            .and_then(|count| usize::try_from(count).ok())
            .unwrap_or(0)
    }

    /// Denied candidates retained for one report, in provider order.
    #[cfg(test)]
    fn denied_candidates(
        &self,
        exact_scope_sha256: &str,
        request_id: &str,
    ) -> Result<Vec<tracedecay_memory_provider_registry::DeniedRecallCandidate>, rusqlite::Error>
    {
        let connection = self.connection();
        let mut statement = connection.prepare(
            "SELECT candidate_id, stable_memory_ref, reason_json,
                    provider_claimed_scope_binding, provider_claimed_scope_sha256,
                    provider_claimed_temporal_state
             FROM recall_admission_denials
             WHERE exact_scope_sha256 = ?1 AND request_id = ?2
             ORDER BY position ASC",
        )?;
        let rows = statement.query_map(params![exact_scope_sha256, request_id], |row| {
            let reason_json: String = row.get(2)?;
            let reason = serde_json::from_str(&reason_json).map_err(|error| {
                rusqlite::Error::FromSqlConversionFailure(
                    2,
                    rusqlite::types::Type::Text,
                    Box::new(error),
                )
            })?;
            let binding_wire: String = row.get(3)?;
            let provider_claimed_scope_binding = ScopeBinding::from_wire(&binding_wire)
                .ok_or_else(|| {
                    rusqlite::Error::FromSqlConversionFailure(
                        3,
                        rusqlite::types::Type::Text,
                        format!("unknown recall scope binding {binding_wire:?}").into(),
                    )
                })?;
            Ok(tracedecay_memory_provider_registry::DeniedRecallCandidate {
                candidate_id: row.get(0)?,
                stable_memory_ref: row.get(1)?,
                reason,
                provider_claimed_scope_binding,
                provider_claimed_scope_sha256: row.get(4)?,
                provider_claimed_temporal_state: row.get(5)?,
            })
        })?;
        rows.collect()
    }
}

/// Adds `provider_claimed_scope_binding` to a denial ledger created before
/// candidates carried an explicit binding. Idempotent: a ledger that already
/// has the column is left untouched.
fn add_scope_binding_column_if_missing(connection: &Connection) -> Result<(), rusqlite::Error> {
    let has_column = connection
        .prepare("PRAGMA table_info(recall_admission_denials)")?
        .query_map([], |row| row.get::<_, String>(1))?
        .collect::<Result<Vec<_>, _>>()?
        .iter()
        .any(|name| name == "provider_claimed_scope_binding");
    if has_column {
        return Ok(());
    }
    connection.execute_batch(
        "ALTER TABLE recall_admission_denials
         ADD COLUMN provider_claimed_scope_binding TEXT NOT NULL DEFAULT 'exact_coding_scope';",
    )?;
    Ok(())
}

impl RecallAdmissionObserver for RecallAdmissionLedgerV1 {
    fn observe_admission(
        &self,
        report: &RecallAdmissionReport,
    ) -> Result<(), RecallAdmissionAuditError> {
        self.record(report)
            .map(|_| ())
            .map_err(|source| RecallAdmissionAuditError {
                request_id: report.request_id.clone(),
                source: Box::new(source),
            })
    }
}

/// Host authority binding one canonical session's recalls to the exact scope
/// of the mounted checkout.
struct SessionExactScopeBindingV1 {
    profile_id: UserProfileId,
    scope: ResolvedScope,
    canonical_session_id: String,
}

impl ExactScopeBinding for SessionExactScopeBindingV1 {
    fn bind_exact_scope(
        &self,
        scope: &ResolvedScope,
    ) -> Result<OwnedExactScope, ExactScopeBindingError> {
        // The request scope is compared with the authoritative scope on every
        // identity field; a provider is never asked about a checkout this
        // server is not mounted for.
        let disagreement = |field: &'static str, expected: &str, received: &str| {
            ExactScopeBindingError::ScopeDisagreement {
                field,
                expected: expected.to_owned(),
                received: received.to_owned(),
            }
        };
        if scope.project_id != self.scope.project_id {
            return Err(disagreement(
                "project_id",
                self.scope.project_id.as_str(),
                scope.project_id.as_str(),
            ));
        }
        if scope.repository_id != self.scope.repository_id {
            return Err(disagreement(
                "repository_id",
                self.scope.repository_id.as_str(),
                scope.repository_id.as_str(),
            ));
        }
        if scope.worktree_id != self.scope.worktree_id {
            return Err(disagreement(
                "worktree_id",
                self.scope.worktree_id.as_str(),
                scope.worktree_id.as_str(),
            ));
        }
        let reference = self.scope.reference.as_ref().ok_or_else(|| {
            ExactScopeBindingError::ReferenceUnavailable {
                project_id: self.scope.project_id.as_str().to_owned(),
            }
        })?;
        match scope.reference.as_ref() {
            Some(received) if received == reference => {}
            received => {
                return Err(disagreement(
                    "reference",
                    reference.as_str(),
                    received.map_or("", |received| received.as_str()),
                ));
            }
        }
        if scope.scope_digest != self.scope.scope_digest {
            return Err(disagreement(
                "scope_digest",
                self.scope.scope_digest.as_str(),
                scope.scope_digest.as_str(),
            ));
        }
        OwnedExactScope::new(
            self.profile_id.as_str(),
            self.scope.project_id.as_str(),
            self.scope.repository_id.as_str(),
            self.scope.worktree_id.as_str(),
            reference.as_str(),
            provider_agent_session_id(&self.profile_id, &self.scope, &self.canonical_session_id),
            self.scope.scope_digest.as_str(),
        )
        .map_err(ExactScopeBindingError::Contract)
    }
}

/// Inputs the composition root supplies to mount one project's recall route.
pub(crate) struct CognitiveRecallMountInputsV1 {
    /// Enabled provider composition. A disabled composition is refused at
    /// mount time.
    pub(crate) composition: Arc<ProjectMemoryProviderComposition>,
    /// Authoritative profile identity.
    pub(crate) profile_id: UserProfileId,
    /// Authoritative resolved scope, used verbatim.
    pub(crate) scope: ResolvedScope,
    /// The authoritative project identity the composition root resolved
    /// independently of the scope. Checked against the scope rather than
    /// trusted, so one mount can never straddle two projects.
    pub(crate) authoritative_project_id: ProjectId,
    /// Canonical store-owned data root. Storage placement only.
    pub(crate) store_data_root: PathBuf,
    /// Host-pinned routing policy built from the configured routing gate.
    pub(crate) routing: ActiveRoutingPolicy,
    /// Host limits the readiness handshake negotiates against.
    pub(crate) host_limits: ProviderLimits,
}

/// One project's mounted cognitive-recall route.
pub struct ProjectCognitiveRecallMountV1 {
    composition: Arc<ProjectMemoryProviderComposition>,
    profile_id: UserProfileId,
    scope: ResolvedScope,
    ledger: Arc<RecallAdmissionLedgerV1>,
    routing: ActiveRoutingPolicy,
    host_limits: ProviderLimits,
}

impl std::fmt::Debug for ProjectCognitiveRecallMountV1 {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ProjectCognitiveRecallMountV1")
            .field("project_id", &self.scope.project_id)
            .field("ledger", &self.ledger.path())
            .field("routing", &self.routing)
            .finish_non_exhaustive()
    }
}

impl ProjectCognitiveRecallMountV1 {
    /// Storage placement of the admission ledger.
    #[must_use]
    pub fn ledger_path(&self) -> &Path {
        self.ledger.path()
    }

    /// The host-pinned routing policy every session port routes under.
    #[must_use]
    pub fn routing(&self) -> &ActiveRoutingPolicy {
        &self.routing
    }

    /// Mints the recall port for one canonical host session.
    ///
    /// The port binds every request to the mounted checkout and to the
    /// provider-qualified identity of `canonical_session_id`; its admission
    /// reports are retained in the mount's ledger before any result is
    /// delivered.
    pub fn port_for_session(
        &self,
        canonical_session_id: &str,
    ) -> Result<ProjectCognitiveRecallPortV1, CognitiveRecallMountError> {
        if canonical_session_id.is_empty()
            || canonical_session_id.trim() != canonical_session_id
            || canonical_session_id.chars().any(char::is_control)
        {
            return Err(CognitiveRecallMountError::SessionIdentityInvalid);
        }
        ProjectCognitiveRecallPortV1::mount(CognitiveRecallPortInputsV1 {
            composition: Arc::clone(&self.composition),
            scope_binding: Arc::new(SessionExactScopeBindingV1 {
                profile_id: self.profile_id.clone(),
                scope: self.scope.clone(),
                canonical_session_id: canonical_session_id.to_owned(),
            }),
            admission_observer: Arc::clone(&self.ledger) as Arc<dyn RecallAdmissionObserver>,
            routing: self.routing.clone(),
            host_limits: self.host_limits,
            policy_revision: PROJECT_RECALL_POLICY_REVISION,
            budgets: PROJECT_RECALL_BUDGETS,
        })
        .map_err(CognitiveRecallMountError::Port)
    }
}

/// Mounts one project's cognitive-recall route.
///
/// Order is enforced by the argument list: the caller cannot reach this
/// function without an authoritative resolved scope and an enabled
/// composition. The ledger is opened here so an unwritable placement fails
/// project open rather than the first recall.
pub(crate) fn mount_project_cognitive_recall(
    inputs: CognitiveRecallMountInputsV1,
) -> Result<Arc<ProjectCognitiveRecallMountV1>, CognitiveRecallMountError> {
    inputs
        .composition
        .registry()
        .ok_or(CognitiveRecallMountError::CompositionDisabled)?;
    if inputs.scope.project_id != inputs.authoritative_project_id {
        return Err(CognitiveRecallMountError::ScopeDisagreement {
            field: "project_id",
            expected: inputs.authoritative_project_id.as_str().to_owned(),
            received: inputs.scope.project_id.as_str().to_owned(),
        });
    }
    let ledger = Arc::new(RecallAdmissionLedgerV1::open(
        inputs.store_data_root.join(LEDGER_FILE_NAME),
    )?);
    Ok(Arc::new(ProjectCognitiveRecallMountV1 {
        composition: inputs.composition,
        profile_id: inputs.profile_id,
        scope: inputs.scope,
        ledger,
        routing: inputs.routing,
        host_limits: inputs.host_limits,
    }))
}

#[cfg(test)]
mod tests {
    #![allow(clippy::expect_used, clippy::panic, clippy::unwrap_used)]

    use std::path::PathBuf;
    use std::sync::Arc;

    use tracedecay_application::memory::CognitiveRecallRequest;
    use tracedecay_application::{
        CancellationContext, Deadline, RequestId, ResolvedScope, now_micros,
    };
    use tracedecay_domain::{
        Confidence, FactCategoryV1, FactOwnerV1, ProjectId, RefId, RepositoryId, UserProfileId,
        UtcMicros, WorktreeId,
    };
    use tracedecay_memory_provider_registry::{
        ActiveRoutingPolicy, CognitiveRecallAdmittedOutcomeV1, DeniedRecallCandidate,
        EnabledProviderMode, FabricConfig, FallbackRule, NATIVE_PROVIDER_ID,
        NativeProviderActivation, OwnedProviderId, ProviderMode, RecallDenialReason,
        RecallScopeBindingsV1, ScopeField, UnknownValidityPolicy,
    };
    use tracedecay_session_memory::memory::{
        ProjectMemoryFactAddRequest, ProjectMemoryFactAddRequestOutcome,
    };
    use tracedecay_store::FactWriteControl;

    use super::*;
    use crate::tracedecay::{TraceDecay, TraceDecayOpenOptions};

    const PROJECT_ID: &str = "project.cognitive-recall";
    const SEEDED_CONTENT: &str = "cognitive recall ledger durable retrieval";

    struct StoreFixture {
        _temporary: tempfile::TempDir,
        project_root: PathBuf,
        ledger_root: PathBuf,
        graph: Arc<TraceDecay>,
        project_id: ProjectId,
    }

    async fn project_fixture() -> StoreFixture {
        let temporary = tempfile::tempdir().expect("cognitive recall fixture root");
        let project_root = temporary.path().join("project");
        let profile_root = temporary.path().join("profile");
        let ledger_root = temporary.path().join("ledger");
        std::fs::create_dir_all(&project_root).expect("project root");
        std::fs::create_dir_all(&profile_root).expect("profile root");
        std::fs::create_dir_all(&ledger_root).expect("ledger root");
        tracedecay_runtime_core::storage::pin_fixture_repository_identity(
            &project_root,
            PROJECT_ID,
        )
        .expect("project enrollment");
        let graph = Arc::new(
            TraceDecay::init_with_options(
                &project_root,
                TraceDecayOpenOptions {
                    global_db_path: Some(profile_root.join("global.db")),
                    profile_root: Some(profile_root),
                },
            )
            .await
            .expect("initialize cognitive recall fixture"),
        );
        let owner = graph.project_memory_owner().expect("project memory owner");
        let FactOwnerV1::Project { project_id } = owner else {
            panic!("cognitive recall fixture must have a project owner");
        };
        assert_eq!(project_id.as_str(), PROJECT_ID);
        StoreFixture {
            _temporary: temporary,
            project_root,
            ledger_root,
            graph,
            project_id,
        }
    }

    async fn seed_fixture(fixture: &StoreFixture) {
        let memory = fixture
            .graph
            .project_memory_application()
            .await
            .expect("project memory application");
        let preflight = memory
            .preflight_project_memory_fact_add(
                ProjectMemoryFactAddRequest {
                    content: SEEDED_CONTENT.to_owned(),
                    category: FactCategoryV1::Project,
                    source_label: Some("cognitive-recall-seed".to_owned()),
                    tags: vec!["cognitive".to_owned(), "recall".to_owned()],
                    entities: vec!["TraceDecay".to_owned()],
                    trust: Some(Confidence::new(0.91).expect("fact trust")),
                    metadata: serde_json::json!({"fixture": "cognitive-recall"}),
                },
                None,
            )
            .expect("preflight seeded fact");
        let outcome = memory
            .add_preflighted_project_memory_fact(
                preflight,
                &FactWriteControl::new(Arc::new(|| false), Arc::new(|| true)),
            )
            .await
            .expect("commit seeded fact");
        assert!(matches!(
            outcome,
            ProjectMemoryFactAddRequestOutcome::Applied(_)
        ));
    }

    fn resolved_scope(project_id: &ProjectId, worktree: &str) -> ResolvedScope {
        ResolvedScope::new(
            project_id.clone(),
            RepositoryId::new("repository.cognitive-recall").expect("repository id"),
            WorktreeId::new(worktree).expect("worktree id"),
            Some(RefId::new("refs/heads/cognitive-recall").expect("reference id")),
        )
        .expect("resolved scope")
    }

    fn request(scope: ResolvedScope, request_id: &str) -> CognitiveRecallRequest {
        let now = now_micros();
        CognitiveRecallRequest::new(
            scope,
            RequestId::new(request_id).expect("request id"),
            Deadline::new(UtcMicros(now.0.saturating_add(60_000_000))).expect("deadline"),
            CancellationContext::active("token.cognitive-recall").expect("active context"),
            "cognitive recall ledger",
            8,
        )
        .expect("recall request")
    }

    const MOUNTED_WORKTREE: &str = "worktree.cognitive-recall";
    const MOUNTED_PROFILE: &str = "profile.cognitive-recall";

    fn native_authorized_bindings() -> RecallScopeBindingsV1 {
        RecallScopeBindingsV1::new([ScopeBinding::ProjectFacts, ScopeBinding::ProfileFacts])
    }

    fn production_mount(
        fixture: &StoreFixture,
        mode: EnabledProviderMode,
        worktree: &str,
    ) -> Arc<ProjectCognitiveRecallMountV1> {
        let ledger_root = fixture.ledger_root.join(worktree);
        std::fs::create_dir_all(&ledger_root).expect("ledger root for mount");
        let graph_cell = Arc::new(tokio::sync::RwLock::new(Arc::clone(&fixture.graph)));
        let port = super::super::native_provider::project_native_memory_application_port(
            graph_cell,
            fixture.project_root.clone(),
            UserProfileId::new(MOUNTED_PROFILE).expect("profile id"),
        )
        .expect("construct project Native application port");
        let composition = Arc::new(
            ProjectMemoryProviderComposition::compose(NativeProviderActivation::Enabled {
                fabric_config: FabricConfig {
                    max_registered_providers: 1,
                    max_in_flight: 1,
                },
                port,
                registration_revision: 1,
                mode,
            })
            .expect("provider composition"),
        );
        mount_project_cognitive_recall(CognitiveRecallMountInputsV1 {
            composition,
            profile_id: UserProfileId::new(MOUNTED_PROFILE).expect("profile id"),
            scope: resolved_scope(&fixture.project_id, worktree),
            authoritative_project_id: fixture.project_id.clone(),
            store_data_root: ledger_root,
            routing: ActiveRoutingPolicy::new(
                OwnedProviderId::new(NATIVE_PROVIDER_ID).expect("native provider id"),
                1,
                FallbackRule::Forbidden,
            )
            .expect("routing policy"),
            host_limits: super::super::native_provider::native_provider_limits(),
        })
        .expect("mounted cognitive recall route")
    }

    fn ledger_report(
        request_id: &str,
        denied: Vec<DeniedRecallCandidate>,
    ) -> RecallAdmissionReport {
        RecallAdmissionReport {
            request_id: request_id.to_owned(),
            exact_scope_sha256: "b".repeat(64),
            temporal_mode: "current".to_owned(),
            evaluation_time: "2026-09-02T00:00:00.000000Z".to_owned(),
            unknown_validity_policy: UnknownValidityPolicy::Exclude,
            authorized_scope_bindings: native_authorized_bindings(),
            received_count: 1 + denied.len(),
            admitted_count: 1,
            denied,
            degraded: false,
            warnings: Vec::new(),
        }
    }

    /// Asserts one host admission of a Native candidate: the adapter attests
    /// the seeded fact as `project_facts` bound to its owner project and the
    /// mount profile, the host authorizes that binding from the registration
    /// record it holds for Native, and the ledger retains a report with no
    /// denial row and no content.
    fn assert_native_candidate_admitted_as_project_fact(
        mount: &ProjectCognitiveRecallMountV1,
        outcome: &CognitiveRecallAdmittedOutcomeV1,
        request_id: &str,
    ) -> RecallAdmissionReport {
        let candidates = outcome.result.candidates();
        assert_eq!(candidates.len(), 1, "{:?}", outcome.result);
        assert_eq!(candidates[0].content(), SEEDED_CONTENT);
        assert!(candidates[0].candidate_id().starts_with(request_id));
        let report = outcome.report.clone().expect("admission report");
        assert_eq!(report.request_id, request_id);
        assert_eq!(report.received_count, 1);
        assert_eq!(report.admitted_count, 1);
        assert!(report.denied.is_empty(), "{:?}", report.denied);
        assert!(!report.degraded);
        assert_eq!(
            report.authorized_scope_bindings,
            native_authorized_bindings()
        );
        assert!(
            mount
                .ledger
                .denied_candidates(&report.exact_scope_sha256, request_id)
                .expect("ledger denial rows")
                .is_empty()
        );
        let serialized = serde_json::to_string(&report).expect("serialize report");
        assert!(
            !serialized.contains(SEEDED_CONTENT),
            "report must not carry content: {serialized}"
        );
        report
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn production_native_route_admits_project_fact_and_refuses_cross_worktree_requests() {
        let fixture = project_fixture().await;
        seed_fixture(&fixture).await;
        let mount = production_mount(&fixture, EnabledProviderMode::Active, MOUNTED_WORKTREE);
        let port = mount
            .port_for_session("session.cognitive-recall")
            .expect("session port");
        let scope = resolved_scope(&fixture.project_id, MOUNTED_WORKTREE);

        // The production Native adapter returns the seeded project fact
        // attested as `project_facts`: the owner project from the fact record
        // and the daemon profile fixed at mount. The host authorizes that
        // binding from its registration record for Native and admits the
        // candidate without lending it this checkout's worktree, branch, or
        // session identity; the report is retained in the ledger.
        let request_id = "request.cognitive-recall.in-scope";
        let outcome = port
            .recall_admitted(request(scope.clone(), request_id))
            .await
            .expect("recall through the production Native port");
        let report = assert_native_candidate_admitted_as_project_fact(&mount, &outcome, request_id);
        assert_eq!(mount.ledger.report_count(), 1);
        assert!(
            mount.ledger_path().is_file(),
            "ledger at {}",
            mount.ledger_path().display()
        );

        // A request for another worktree of the same project is refused by
        // the host binding before any provider contact and leaves no trace in
        // the result or the ledger.
        let foreign = resolved_scope(&fixture.project_id, "worktree.other");
        let error = port
            .recall_admitted(request(foreign, "request.cognitive-recall.cross-worktree"))
            .await
            .expect_err("cross-worktree request is refused");
        match error {
            CognitiveRecallPortError::Scope(ExactScopeBindingError::ScopeDisagreement {
                field,
                expected,
                received,
            }) => {
                assert_eq!(field, "worktree_id");
                assert_eq!(expected, MOUNTED_WORKTREE);
                assert_eq!(received, "worktree.other");
            }
            other => panic!("expected a scope disagreement, got {other:?}"),
        }
        assert_eq!(mount.ledger.report_count(), 1);

        // Replaying the same request against the ledger is idempotent.
        assert_eq!(
            mount.ledger.record(&report).expect("replayed report"),
            RecallAdmissionLedgerWriteV1::AlreadyRecorded
        );
        assert_eq!(mount.ledger.report_count(), 1);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn production_native_route_admits_same_project_fact_from_another_worktree_as_project_fact()
     {
        let fixture = project_fixture().await;
        // The fact is committed while worktree A is the mounted checkout. The
        // Native commit path records only the owner project; under the
        // `project_facts` binding the adapter vouches for no worktree, so the
        // fact is a project-wide memory that any checkout of the project may
        // recall under its own exact scope.
        let mount_a = production_mount(&fixture, EnabledProviderMode::Active, "worktree.a");
        seed_fixture(&fixture).await;

        // Worktree B of the same project mounts the same project store under
        // its own authoritative scope; its request passes the host binding
        // (the mount is for worktree B) and reaches the production adapter.
        let mount_b = production_mount(&fixture, EnabledProviderMode::Active, "worktree.b");
        assert_ne!(mount_a.ledger_path(), mount_b.ledger_path());
        let port_b = mount_b
            .port_for_session("session.cognitive-recall.b")
            .expect("session port for worktree B");
        let request_id = "request.cognitive-recall.worktree-b";
        let outcome = port_b
            .recall_admitted(request(
                resolved_scope(&fixture.project_id, "worktree.b"),
                request_id,
            ))
            .await
            .expect("recall from worktree B through the production Native port");
        let report =
            assert_native_candidate_admitted_as_project_fact(&mount_b, &outcome, request_id);
        assert_eq!(report.exact_scope_sha256.len(), 64);
        assert_eq!(mount_b.ledger.report_count(), 1);
        assert_eq!(mount_a.ledger.report_count(), 0);
        assert!(
            mount_a
                .ledger
                .denied_candidates(&report.exact_scope_sha256, request_id)
                .expect("worktree A ledger rows")
                .is_empty()
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn observer_only_composition_yields_a_typed_error_and_records_nothing() {
        let fixture = project_fixture().await;
        let mount = production_mount(&fixture, EnabledProviderMode::Observer, MOUNTED_WORKTREE);
        let port = mount
            .port_for_session("session.cognitive-recall")
            .expect("session port");
        let scope = resolved_scope(&fixture.project_id, MOUNTED_WORKTREE);
        let error = port
            .recall_admitted(request(scope, "request.cognitive-recall.observer"))
            .await
            .expect_err("observer-only provider never answers a recall");
        assert!(
            matches!(
                &error,
                CognitiveRecallPortError::ProviderNotActive {
                    provider_id,
                    mode: ProviderMode::Observer,
                } if provider_id == NATIVE_PROVIDER_ID
            ),
            "{error:?}"
        );
        assert_eq!(mount.ledger.report_count(), 0);
        assert!(matches!(
            mount.port_for_session(" session"),
            Err(CognitiveRecallMountError::SessionIdentityInvalid)
        ));
    }

    #[test]
    fn ledger_retains_denials_without_content_and_refuses_divergent_replays() {
        let temporary = tempfile::tempdir().expect("ledger root");
        let ledger = RecallAdmissionLedgerV1::open(temporary.path().join(LEDGER_FILE_NAME))
            .expect("open ledger");
        let denied = vec![
            DeniedRecallCandidate {
                candidate_id: "request.ledger:cross-worktree".to_owned(),
                stable_memory_ref: Some("memory:cross-worktree".to_owned()),
                reason: RecallDenialReason::ScopeMismatch {
                    field: ScopeField::WorktreeIdentity,
                },
                provider_claimed_scope_binding: ScopeBinding::ExactCodingScope,
                provider_claimed_scope_sha256: Some("c".repeat(64)),
                provider_claimed_temporal_state: "current".to_owned(),
            },
            DeniedRecallCandidate {
                candidate_id: "request.ledger:revoked".to_owned(),
                stable_memory_ref: None,
                reason: RecallDenialReason::Revoked,
                provider_claimed_scope_binding: ScopeBinding::ProjectFacts,
                provider_claimed_scope_sha256: None,
                provider_claimed_temporal_state: "revoked".to_owned(),
            },
        ];
        let report = ledger_report("request.ledger", denied.clone());
        assert_eq!(
            ledger.record(&report).expect("first write"),
            RecallAdmissionLedgerWriteV1::Recorded
        );
        assert_eq!(
            ledger.record(&report).expect("identical replay"),
            RecallAdmissionLedgerWriteV1::AlreadyRecorded
        );
        assert_eq!(
            ledger
                .denied_candidates(&report.exact_scope_sha256, "request.ledger")
                .expect("denial rows"),
            denied
        );
        let divergent = ledger_report("request.ledger", Vec::new());
        assert!(matches!(
            ledger.record(&divergent),
            Err(RecallAdmissionLedgerError::ConflictingReport { .. })
        ));
        assert_eq!(ledger.report_count(), 1);

        // The ledger schema has no content column anywhere.
        let connection = ledger.connection();
        let mut statement = connection
            .prepare("SELECT name FROM pragma_table_info('recall_admission_denials')")
            .expect("table info");
        let columns: Vec<String> = statement
            .query_map([], |row| row.get(0))
            .expect("columns")
            .collect::<Result<_, _>>()
            .expect("column names");
        assert!(!columns.iter().any(|column| column.contains("content")));
    }
}
