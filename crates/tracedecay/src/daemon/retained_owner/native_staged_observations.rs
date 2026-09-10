//! Product-owned staged session observations for the Native memory provider.
//!
//! The Native adapter crate is an adapter: it owns no database and never opens
//! persistence. This module is the persistence the adapter's staging path
//! actually lands in, owned by `ProjectNativeMemoryApplicationPort` on the
//! product side and placed under the host-granted provider-state namespace as
//! `<provider-state>/native/staged-observations-v1.sqlite3`.
//!
//! What this store is, precisely:
//!
//! * **Derivative advisory state, never canonical memory.** Nothing here is a
//!   fact. Rows are provider-local staged copies of observations `TraceDecay` has
//!   already admitted and settled canonically; promotion to canonical facts
//!   remains a separate explicit path. No statement in this module touches
//!   `memory_v2` or any canonical fact table.
//! * **Durability before acknowledgement.** [`StagedObservationStore::stage_or_duplicate`]
//!   commits its transaction *before* it returns [`StagedOutcome::Committed`],
//!   so a `Success` terminal built from that outcome can never acknowledge an
//!   observation whose row does not exist.
//! * **Lifetime exactly-once, from two constraints.** The idempotency key alone
//!   is not enough: the journey derives a fresh key over
//!   `target.registration_revision`, so the key is stable only for redelivery of
//!   one admitted journal row, not across re-registration or journal
//!   reconstruction. The secondary unique index on
//!   `(exact_scope_sha256, source_authority, source_event_id, source_revision,
//!   payload_sha256)` is what makes one settled source event produce at most one
//!   row for the lifetime of the store.
//! * **Exact origin storage, checkout recall.** All seven identity fields and
//!   their digest remain immutable storage and idempotency authority. Recall
//!   reads only the newest bounded window matching the five checkout fields,
//!   then validates every row against its own stored origin digest. The Native
//!   candidate's checkout binding permits another session on that checkout;
//!   origin session identity remains provenance, never host citation authority.
//!
//! Blocking discipline: like `SqliteObservationJournal`, every method here is
//! synchronous and holds a `std::sync::Mutex` across a `SQLite` transaction. An
//! async caller must wrap a call in `tokio::task::spawn_blocking`, exactly as
//! `observation_journey.rs` already does for the journal.

// The store is constructible and testable before the composition owner wires
// the provider port to it; keep that dormant surface warning-free until the
// wiring lands.
#![allow(dead_code)]

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};
use std::sync::{Mutex, MutexGuard};
use std::time::Duration;

use rusqlite::{Connection, OpenFlags, OptionalExtension, TransactionBehavior, params};
use serde_json::Value;
use tracedecay_domain::canonical_text::{canonical_framed_sha256, sha256_hex};
use tracedecay_memory_provider_registry::{ApiError, OwnedExactScope};

/// Directory the Native provider owns inside the host-granted provider-state
/// root. Placement only: scope identity is never derived from a path.
const NATIVE_PROVIDER_STATE_DIR_NAME: &str = "native";

/// File name of the staged-observation store.
const STAGED_STORE_FILE_NAME: &str = "staged-observations-v1.sqlite3";

/// Schema version this build writes and understands.
const SCHEMA_VERSION: i64 = 2;

/// How long a writer waits on a busy database before reporting failure.
const BUSY_TIMEOUT_MILLIS: u64 = 250;

/// The single observation kind this store stages. Mirrors the
/// `session.message_committed.v1` entry of
/// `product/contracts/memory-provider-v1/provider-observation-contract.json`.
pub(crate) const STAGED_SESSION_OBSERVATION_KIND: &str = "session.message_committed.v1";

/// The payload contract that kind declares in the same contract document.
pub(crate) const STAGED_SESSION_PAYLOAD_CONTRACT: &str =
    "tracedecay.memory.observation.session-message.v1";

/// Prefix of every provider-local staged-row reference. It is a provider-local
/// name, deliberately unlike a host evidence ref.
pub(crate) const PROVIDER_REFERENCE_PREFIX: &str = "native-staged-observation-v1:";

/// Whether `reference` is a well-formed provider-local staged-row reference.
///
/// Syntax only, and deliberately so. A true answer says the text is one this
/// provider mints — the fixed prefix plus a lowercase 64-hex digest — never
/// that a row exists or that its content is trustworthy. The host uses it to
/// tell a provider-local reference apart from a malformed claim, and the
/// candidate stays *provider-attested* either way: it is never cited
/// grounding, and it never earns the host-confirmed trust tier.
#[must_use]
pub(crate) fn is_staged_provider_reference(reference: &str) -> bool {
    let Some(digest) = reference.strip_prefix(PROVIDER_REFERENCE_PREFIX) else {
        return false;
    };
    digest.len() == 64
        && digest
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
}

/// Digest domains. Each derivation is separated so no one digest can be
/// replayed as another.
const PROVIDER_REFERENCE_DIGEST_DOMAIN: &[u8] =
    b"tracedecay.native.staged-observation.provider-reference.v1";
const RECEIPT_DIGEST_DOMAIN: &[u8] = b"tracedecay.native.staged-observation.receipt.v1";
const EFFECT_DIGEST_DOMAIN: &[u8] = b"tracedecay.native.staged-observation.effect-digest.v1";

/// Default number of *content-bearing* rows retained per exact scope. Older
/// rows keep their identity and evidence and lose only their payload.
const DEFAULT_MAXIMUM_CONTENT_ROWS_PER_SCOPE: usize = 512;

/// Weight of recency in the staged recall score.
const RECENCY_WEIGHT: f64 = 0.5;

/// Weight of lexical overlap in the staged recall score.
const LEXICAL_WEIGHT: f64 = 0.5;

const SCHEMA_DDL: &str = "\
CREATE TABLE IF NOT EXISTS tdmem_native_staged_observation_v1 (
    exact_scope_sha256    TEXT    NOT NULL,
    idempotency_key       TEXT    NOT NULL,
    profile_id            TEXT    NOT NULL,
    project_id            TEXT    NOT NULL,
    repository_identity   TEXT    NOT NULL,
    worktree_identity     TEXT    NOT NULL,
    branch_identity       TEXT    NOT NULL,
    agent_session_id      TEXT    NOT NULL,
    resolved_scope_digest TEXT    NOT NULL,
    source_authority      TEXT    NOT NULL,
    source_event_id       TEXT    NOT NULL,
    source_revision       INTEGER NOT NULL CHECK (source_revision >= 0),
    observation_kind      TEXT    NOT NULL,
    payload_contract      TEXT    NOT NULL,
    sanitized_payload     BLOB,
    payload_sha256        TEXT    NOT NULL,
    operation_id          TEXT    NOT NULL,
    request_identity      TEXT    NOT NULL,
    provider_reference    TEXT    NOT NULL,
    receipt               TEXT    NOT NULL,
    effect_digest         TEXT    NOT NULL,
    admitted_sequence     INTEGER NOT NULL CHECK (admitted_sequence > 0),
    admitted_at_unix_ms   INTEGER NOT NULL,
    tombstone             INTEGER NOT NULL CHECK (tombstone IN (0, 1)),
    -- Eviction removes content and nothing else: a row without a payload is a
    -- tombstone, and a tombstone never carries a payload. Any other
    -- combination is a row no reader can decide, so it is refused here.
    CHECK ((sanitized_payload IS NULL) = (tombstone = 1)),
    PRIMARY KEY (exact_scope_sha256, idempotency_key)
) STRICT;

-- Lifetime exactly-once. The idempotency key is stable only for redelivery of
-- one admitted journal row; this index is what stops a replay under a fresh
-- key -- after a registration-revision change or a journal reconstruction --
-- from creating a second row for one settled source event.
CREATE UNIQUE INDEX IF NOT EXISTS tdmem_native_staged_observation_sequence_v1
    ON tdmem_native_staged_observation_v1 (admitted_sequence);

CREATE INDEX IF NOT EXISTS tdmem_native_staged_observation_recall_v1
    ON tdmem_native_staged_observation_v1 (exact_scope_sha256, tombstone, admitted_sequence);
CREATE INDEX IF NOT EXISTS tdmem_native_staged_observation_checkout_recall_v1
    ON tdmem_native_staged_observation_v1 (
        profile_id, project_id, repository_identity, worktree_identity, branch_identity,
        tombstone, admitted_sequence
    );
";

/// The seven canonical exact-scope identity fields a staged row is addressed
/// by. This is the host-admitted scope shape verbatim: the store stores every
/// field, so a candidate built from a row can attest the complete
/// `exact_coding_scope` claim rather than only a digest.
pub(crate) type ExactScopeFields = OwnedExactScope;

/// Per-scope retention bound. Content-bearing rows above the cap are evicted to
/// tombstones; identity, source identity, payload digest, and effect evidence
/// survive eviction so an evicted key still answers duplicate.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct StagedRetentionPolicyV1 {
    /// Content-bearing rows retained per exact scope. Must be at least one.
    pub(crate) maximum_content_rows_per_scope: usize,
}

impl Default for StagedRetentionPolicyV1 {
    fn default() -> Self {
        Self {
            maximum_content_rows_per_scope: DEFAULT_MAXIMUM_CONTENT_ROWS_PER_SCOPE,
        }
    }
}

/// One observation offered for staging.
///
/// `sanitized_payload` is the already-sanitized canonical payload the hygiene
/// pipeline produced at admission. It is stored verbatim: this module never
/// re-sanitizes, re-encodes, or reconstructs it, because the admission receipt
/// binds those exact bytes. Its digest is computed here from the stored bytes
/// rather than accepted as an input, so conflict detection compares what is
/// actually on disk.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct StagedObservationRecord {
    /// Host-admitted exact coding scope of the observation.
    pub(crate) scope: ExactScopeFields,
    /// Delivery idempotency key the host carried on this attempt.
    pub(crate) idempotency_key: String,
    /// Canonical settled source authority, e.g. `host_session`.
    pub(crate) source_authority: String,
    /// Canonical settled source event identity.
    pub(crate) source_event_id: String,
    /// Canonical settled source event revision.
    pub(crate) source_revision: Option<String>,
    /// Observation kind, e.g. `session.message_committed.v1`.
    pub(crate) observation_kind: String,
    /// Declared payload contract for that kind.
    pub(crate) payload_contract: String,
    /// Sanitized canonical payload bytes, stored verbatim.
    pub(crate) sanitized_payload: Vec<u8>,
    /// `call.operation_id` of the provider operation carrying this delivery.
    pub(crate) operation_id: String,
    /// Envelope `request_identity` of the same delivery. Persisted alongside
    /// `operation_id` so provenance can name which is which.
    pub(crate) request_identity: String,
    /// Host clock at admission, milliseconds since the Unix epoch. Audit only:
    /// recall recency is derived from `admitted_sequence`, never from a clock.
    pub(crate) admitted_at_unix_ms: i64,
}

/// Effect evidence of the row that actually committed one staged mutation.
///
/// Stored on the row, so a redelivery replies with the *original* evidence
/// rather than a freshly minted generic success. `provider_reference`,
/// `receipt`, and `effect_digest` are derived deterministically from
/// `(exact_scope_sha256, idempotency_key, payload_sha256, admitted_sequence)`.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct StagedEffectEvidence {
    /// Provider-local reference to the staged row.
    pub(crate) provider_reference: String,
    /// Provider receipt digest, bare lowercase SHA-256 hex.
    pub(crate) receipt: String,
    /// Verification digest of the committed partition, bare lowercase hex.
    pub(crate) effect_digest: String,
    /// Monotonic admission sequence of the committing row. Usable as the
    /// provider-local state generation.
    pub(crate) admitted_sequence: u64,
    /// Idempotency key the committing row was staged under.
    pub(crate) idempotency_key: String,
    /// Operation that actually committed the row.
    pub(crate) operation_id: String,
}

/// Why a staging attempt was refused rather than staged or deduplicated.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum StagedConflictReason {
    /// The key is already staged under a different payload.
    PayloadDiverged {
        /// Digest already on disk under this key.
        stored_payload_sha256: String,
        /// Digest offered by this attempt.
        offered_payload_sha256: String,
    },
    /// The key is already staged under a different observation kind.
    KindDiverged {
        /// Kind already on disk under this key.
        stored_observation_kind: String,
        /// Kind offered by this attempt.
        offered_observation_kind: String,
    },
    /// This settled source event is already staged under a *different*
    /// idempotency key. Answering duplicate would name somebody else's key,
    /// which the host refuses as delivery evidence, so the attempt is refused
    /// instead and the single existing row stands.
    SourceIdentityReused {
        /// The key the existing row was staged under.
        stored_idempotency_key: String,
    },
}

impl std::fmt::Display for StagedConflictReason {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::PayloadDiverged {
                stored_payload_sha256,
                offered_payload_sha256,
            } => write!(
                formatter,
                "idempotency key already staged with payload {stored_payload_sha256}, \
                 offered {offered_payload_sha256}"
            ),
            Self::KindDiverged {
                stored_observation_kind,
                offered_observation_kind,
            } => write!(
                formatter,
                "idempotency key already staged as kind {stored_observation_kind}, \
                 offered {offered_observation_kind}"
            ),
            Self::SourceIdentityReused {
                stored_idempotency_key,
            } => write!(
                formatter,
                "source event already staged under idempotency key {stored_idempotency_key}"
            ),
        }
    }
}

/// Outcome of one staging attempt.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum StagedOutcome {
    /// The row was durably committed by this attempt.
    Committed(StagedEffectEvidence),
    /// An earlier attempt already committed this exact mutation. Carries the
    /// evidence stored on that row.
    Duplicate(StagedEffectEvidence),
    /// The attempt contradicts what is already staged. Nothing was written.
    Conflict {
        /// The contradiction, for the refusal terminal.
        reason: StagedConflictReason,
    },
}

/// One staged row as recall returns it.
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct StagedRow {
    /// The seven attested exact-scope fields, read back from the row and
    /// verified to re-derive `exact_scope_sha256`.
    pub(crate) scope: ExactScopeFields,
    /// Immutable scope provenance digest stored with the row.
    pub(crate) exact_scope_sha256: String,
    /// Key the row was staged under.
    pub(crate) idempotency_key: String,
    /// Canonical settled source authority.
    pub(crate) source_authority: String,
    /// Canonical settled source event identity.
    pub(crate) source_event_id: String,
    /// Canonical settled source event revision.
    pub(crate) source_revision: Option<String>,
    /// Observation kind of the staged row.
    pub(crate) observation_kind: String,
    /// Declared payload contract.
    pub(crate) payload_contract: String,
    /// Digest of the stored sanitized payload bytes.
    pub(crate) payload_sha256: String,
    /// Human message text extracted contract-aware from the payload. Never the
    /// envelope JSON and never the raw payload.
    pub(crate) message_text: String,
    /// Operation that committed the row.
    pub(crate) operation_id: String,
    /// Envelope request identity of that delivery.
    pub(crate) request_identity: String,
    /// Provider-local reference to this row.
    pub(crate) provider_reference: String,
    /// Provider receipt digest stored on the row.
    pub(crate) receipt: String,
    /// Verification digest stored on the row.
    pub(crate) effect_digest: String,
    /// Monotonic admission sequence.
    pub(crate) admitted_sequence: u64,
    /// Host admission clock, milliseconds since the Unix epoch.
    pub(crate) admitted_at_unix_ms: i64,
    /// Score in `[0, 1]`: `0.5 * recency + 0.5 * lexical overlap`. Exposed so
    /// the caller can merge staged rows with canonical facts under one budget.
    pub(crate) score: f64,
    /// Recorded canonical attribution; absent for legacy rows.
    pub(crate) original_source: Option<Value>,
    /// Provider-local settled feedback, independent of canonical trust.
    pub(crate) feedback: f64,
    /// Provider-local validity overlays preserve immutable original attribution.
    pub(crate) validity: tracedecay_memory_provider_registry::RecordedValidity,
}

/// Typed failures of the staged-observation store.
#[derive(Debug, thiserror::Error)]
pub(crate) enum StagedStoreError {
    /// The provider-state directory could not be created.
    #[error("staged observation directory {path} could not be created: {source}")]
    CreateDirectory {
        /// Directory that could not be created.
        path: PathBuf,
        /// Underlying filesystem failure.
        #[source]
        source: std::io::Error,
    },
    /// The database file could not be opened or initialized.
    #[error("staged observation store {path} could not be opened: {source}")]
    Open {
        /// Database placement.
        path: PathBuf,
        /// Underlying `SQLite` failure.
        #[source]
        source: rusqlite::Error,
    },
    /// The on-disk schema is newer than this build understands.
    #[error("staged observation schema version {found} is ahead of supported {supported}")]
    SchemaAhead {
        /// Version found on disk.
        found: i64,
        /// Version this build writes.
        supported: i64,
    },
    /// A writer panicked while holding the connection.
    #[error("staged observation store lock was poisoned")]
    LockPoisoned,
    /// The offered scope is not a well-formed exact coding scope.
    #[error("staged observation scope is not a valid exact coding scope: {0}")]
    InvalidScope(#[source] ApiError),
    /// A record field that must carry a value was empty.
    #[error("staged observation record field {field} must not be empty")]
    EmptyField {
        /// The empty field.
        field: &'static str,
    },
    /// A value did not fit the column it addresses.
    #[error("staged observation value {field} is out of range")]
    ValueOutOfRange {
        /// The offending field.
        field: &'static str,
    },
    /// A stored row's seven scope columns do not re-derive its stored digest.
    /// Recall fails closed rather than returning a row whose scope claim
    /// cannot be trusted.
    #[error(
        "staged observation row {idempotency_key} scope columns do not re-derive \
         exact_scope_sha256 {stored_exact_scope_sha256}"
    )]
    ScopeDigestMismatch {
        /// Key of the offending row.
        idempotency_key: String,
        /// Digest stored on that row.
        stored_exact_scope_sha256: String,
    },
    /// A stored row's payload bytes do not match its stored digest.
    #[error(
        "staged observation row {idempotency_key} payload bytes do not match \
         payload_sha256 {stored_payload_sha256}"
    )]
    PayloadDigestMismatch {
        /// Key of the offending row.
        idempotency_key: String,
        /// Digest stored on that row.
        stored_payload_sha256: String,
    },
    /// SQLite could not prove whether its commit completed.
    #[error("Native advisory commit result is unknown: {0}")]
    CommitUnknown(rusqlite::Error),
    /// Control ended before the transaction committed.
    #[error("Native advisory operation control ended: {0:?}")]
    ControlEnded(tracedecay_memory_provider_registry::TerminalCode),
    /// Invalid or inconsistent common advisory metadata.
    #[error("invalid Native advisory value: {0}")]
    InvalidAdvisory(&'static str),
    /// A durable privacy fence forbids this source.
    #[error("Native advisory source is privacy deleted")]
    PrivacyDeleted,
    /// A lifecycle request conflicts with retained state.
    #[error("Native advisory lifecycle conflict: {0}")]
    LifecycleConflict(&'static str),
    /// Any other `SQLite` failure.
    #[error("staged observation store sqlite failure: {0}")]
    Sqlite(#[from] rusqlite::Error),
    /// Test-only durability fault injected between the insert and the commit.
    #[cfg(test)]
    #[error("staged observation store commit fault injected")]
    InjectedCommitFault,
    /// Test-only fault injected at the start of a recall attempt.
    #[cfg(test)]
    #[error("staged observation store recall fault injected")]
    InjectedRecallFault,
}

/// Thread each [`StagedObservationStore::open`] ran on, keyed by the store
/// path it opened.
///
/// Opening the store is blocking work (`create_dir_all`, a `SQLite` open, a
/// journal-mode change, `BEGIN IMMEDIATE`, DDL, a durable commit). The
/// composition root must therefore build the Native port through
/// `project_native_memory_application_port_off_runtime`, which runs it on a
/// blocking thread. Recording the opening thread is what lets a test prove
/// that placement instead of asserting it in a comment: a `spawn_blocking`
/// task never runs on the async caller's own thread, so an equal thread id is
/// exactly the defect — construction back on a runtime worker. Keying by path
/// keeps concurrently running tests from reading each other's opens.
#[cfg(test)]
static OPEN_THREADS: std::sync::OnceLock<
    Mutex<std::collections::BTreeMap<PathBuf, std::thread::ThreadId>>,
> = std::sync::OnceLock::new();

#[cfg(test)]
fn record_open_thread(path: &Path) {
    if let Ok(mut opens) = OPEN_THREADS
        .get_or_init(|| Mutex::new(std::collections::BTreeMap::new()))
        .lock()
    {
        opens.insert(path.to_path_buf(), std::thread::current().id());
    }
}

#[cfg(not(test))]
#[inline]
const fn record_open_thread(_path: &Path) {}

/// Placement of the staged store under one host-granted provider-state root.
///
/// Placement only, never an identity input: nothing derives scope from it.
#[must_use]
pub(crate) fn staged_store_path(provider_state_root: &Path) -> PathBuf {
    provider_state_root
        .join(NATIVE_PROVIDER_STATE_DIR_NAME)
        .join(STAGED_STORE_FILE_NAME)
}

/// The thread the store at `path` was opened on, if it was opened at all.
#[cfg(test)]
pub(crate) fn open_thread_id(path: &Path) -> Option<std::thread::ThreadId> {
    OPEN_THREADS
        .get_or_init(|| Mutex::new(std::collections::BTreeMap::new()))
        .lock()
        .ok()
        .and_then(|opens| opens.get(path).copied())
}

/// Durable provider-local staging store for Native session observations.
#[derive(Debug)]
pub(crate) struct StagedObservationStore {
    path: PathBuf,
    connection: Mutex<Connection>,
    retention: StagedRetentionPolicyV1,
    #[cfg(test)]
    fail_next_commit: std::sync::atomic::AtomicBool,
    #[cfg(test)]
    fail_next_recall: std::sync::atomic::AtomicBool,
    #[cfg(test)]
    lose_next_reply: std::sync::atomic::AtomicBool,
}

impl StagedObservationStore {
    /// Opens (creating if absent) the store under the host-granted
    /// provider-state root, migrating retained v1 state to schema v2.
    ///
    /// # Errors
    ///
    /// Returns [`StagedStoreError`] when the directory cannot be created, the
    /// database cannot be opened, or the on-disk schema is ahead of this build.
    pub(crate) fn open(provider_state_root: &Path) -> Result<Self, StagedStoreError> {
        Self::open_with_retention(provider_state_root, StagedRetentionPolicyV1::default())
    }

    /// [`Self::open`] with an explicit per-scope retention bound.
    ///
    /// # Errors
    ///
    /// As [`Self::open`].
    pub(crate) fn open_with_retention(
        provider_state_root: &Path,
        retention: StagedRetentionPolicyV1,
    ) -> Result<Self, StagedStoreError> {
        let directory = provider_state_root.join(NATIVE_PROVIDER_STATE_DIR_NAME);
        std::fs::create_dir_all(&directory).map_err(|source| {
            StagedStoreError::CreateDirectory {
                path: directory.clone(),
                source,
            }
        })?;
        let path = directory.join(STAGED_STORE_FILE_NAME);
        record_open_thread(&path);
        let mut connection = Connection::open_with_flags(
            &path,
            OpenFlags::SQLITE_OPEN_READ_WRITE
                | OpenFlags::SQLITE_OPEN_CREATE
                | OpenFlags::SQLITE_OPEN_NO_MUTEX,
        )
        .map_err(|source| StagedStoreError::Open {
            path: path.clone(),
            source,
        })?;
        initialize_schema(&mut connection).map_err(|error| match error {
            StagedStoreError::Sqlite(source) => StagedStoreError::Open {
                path: path.clone(),
                source,
            },
            other => other,
        })?;
        Ok(Self {
            path,
            connection: Mutex::new(connection),
            retention: StagedRetentionPolicyV1 {
                maximum_content_rows_per_scope: retention.maximum_content_rows_per_scope.max(1),
            },
            #[cfg(test)]
            fail_next_commit: std::sync::atomic::AtomicBool::new(false),
            #[cfg(test)]
            fail_next_recall: std::sync::atomic::AtomicBool::new(false),
            #[cfg(test)]
            lose_next_reply: std::sync::atomic::AtomicBool::new(false),
        })
    }

    /// Placement of the store on disk. Never an identity input.
    pub(crate) fn path(&self) -> &Path {
        &self.path
    }

    /// The retention bound in force.
    pub(crate) const fn retention(&self) -> StagedRetentionPolicyV1 {
        self.retention
    }

    /// Stages one observation, or reports that it is already staged.
    ///
    /// The whole decision runs in one immediate transaction: existing-key
    /// lookup, secondary source-identity check, insert, sequence allocation,
    /// and per-scope eviction. The transaction **commits before** this returns
    /// [`StagedOutcome::Committed`], so an acknowledgement built from that
    /// outcome can never outlive a rolled-back row.
    ///
    /// # Errors
    ///
    /// Returns [`StagedStoreError`] when the record is malformed or `SQLite`
    /// fails. A refusal that is not an error — a diverging payload, a diverging
    /// kind, or a reused source identity — is [`StagedOutcome::Conflict`].
    pub(crate) fn stage_or_duplicate(
        &self,
        record: StagedObservationRecord,
    ) -> Result<StagedOutcome, StagedStoreError> {
        self.stage_with_control(record, None)
    }

    pub(crate) fn stage_controlled(
        &self,
        record: StagedObservationRecord,
        call: &tracedecay_memory_provider_registry::ProviderCall,
    ) -> Result<StagedOutcome, StagedStoreError> {
        self.stage_with_control(record, Some(call))
    }

    fn stage_with_control(
        &self,
        record: StagedObservationRecord,
        call: Option<&tracedecay_memory_provider_registry::ProviderCall>,
    ) -> Result<StagedOutcome, StagedStoreError> {
        let mut guard = self.connection()?;
        let transaction = guard.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let common_source = serde_json::from_slice::<Value>(&record.sanitized_payload)
            .ok()
            .is_some_and(|envelope| {
                envelope
                    .pointer("/source_identity/original_source")
                    .is_some()
            });
        let outcome = stage_in_transaction(&transaction, record, self.retention)?;
        #[cfg(test)]
        if matches!(outcome, StagedOutcome::Committed(_))
            && self
                .fail_next_commit
                .swap(false, std::sync::atomic::Ordering::SeqCst)
        {
            return Err(StagedStoreError::InjectedCommitFault);
        }
        if let Some(call) = call {
            call.control
                .snapshot()
                .map_err(StagedStoreError::ControlEnded)?;
            match &outcome {
                StagedOutcome::Committed(evidence) => {
                    if common_source
                        && call.expected_state_generation
                            != evidence.admitted_sequence.saturating_sub(1)
                    {
                        return Err(StagedStoreError::LifecycleConflict("state generation"));
                    }
                    if !super::native_provider::staged_response_fits(
                        call,
                        evidence,
                        false,
                        evidence.admitted_sequence,
                    ) {
                        return Err(StagedStoreError::ValueOutOfRange {
                            field: "observation response bytes",
                        });
                    }
                }
                StagedOutcome::Duplicate(evidence) => {
                    if !super::native_provider::staged_response_fits(
                        call,
                        evidence,
                        true,
                        u64::try_from(generation_in(&transaction)?).unwrap_or(0),
                    ) {
                        return Err(StagedStoreError::ValueOutOfRange {
                            field: "duplicate response bytes",
                        });
                    }
                }
                StagedOutcome::Conflict { .. } => {}
            }
        }
        transaction
            .commit()
            .map_err(StagedStoreError::CommitUnknown)?;
        Ok(outcome)
    }

    /// Returns advisory rows from this checkout, best first, across sessions.
    ///
    /// The five checkout fields must match and content must survive. Before
    /// scoring, the query bounds the newest rows by the per-scope retention
    /// ceiling (512 by default), even when many origin sessions share a checkout.
    /// Each row must re-derive its own stored seven-field origin digest and
    /// payload digest. Rows with no contract-aware message text are skipped.
    /// Final ordering is `(score desc, admitted_sequence desc, idempotency_key asc)`.
    ///
    /// # Errors
    ///
    /// Returns [`StagedStoreError`] on `SQLite` failure, or
    /// [`StagedStoreError::ScopeDigestMismatch`] when a stored row's scope
    /// columns do not re-derive its stored digest, or
    /// [`StagedStoreError::PayloadDigestMismatch`] when its payload bytes do
    /// not match their stored digest.
    pub(crate) fn recall(
        &self,
        scope: &ExactScopeFields,
        query: &str,
        limit: usize,
    ) -> Result<Vec<StagedRow>, StagedStoreError> {
        self.recall_filtered(scope, query, limit, None, None, None, "")
    }

    pub(crate) fn recall_temporal(
        &self,
        scope: &ExactScopeFields,
        query: &str,
        temporal: &tracedecay_memory_provider_registry::OwnedTemporalQuery,
        history_grant: Option<&Value>,
        exclusions: &tracedecay_memory_provider_registry::OwnedRecallExclusions,
        request_id: &str,
    ) -> Result<Vec<StagedRow>, StagedStoreError> {
        self.recall_filtered(
            scope,
            query,
            self.retention.maximum_content_rows_per_scope,
            Some(temporal),
            history_grant,
            Some(exclusions),
            request_id,
        )
    }

    fn recall_filtered(
        &self,
        scope: &ExactScopeFields,
        query: &str,
        limit: usize,
        temporal: Option<&tracedecay_memory_provider_registry::OwnedTemporalQuery>,
        history_grant: Option<&Value>,
        exclusions: Option<&tracedecay_memory_provider_registry::OwnedRecallExclusions>,
        request_id: &str,
    ) -> Result<Vec<StagedRow>, StagedStoreError> {
        #[cfg(test)]
        {
            if self
                .fail_next_recall
                .swap(false, std::sync::atomic::Ordering::SeqCst)
            {
                return Err(StagedStoreError::InjectedRecallFault);
            }
        }
        if limit == 0 {
            return Ok(Vec::new());
        }
        let scan_limit =
            i64::try_from(self.retention.maximum_content_rows_per_scope).unwrap_or(i64::MAX);
        let query_tokens = normalized_tokens(query);
        use tracedecay_memory_provider_registry::{CommonUnknownValidityPolicy, TemporalMode};
        let mode = temporal.map_or("legacy", |query| match query.mode {
            TemporalMode::Current | TemporalMode::AsOf => "point",
            TemporalMode::Interval => "interval",
            TemporalMode::History => "history",
        });
        let from = temporal.map_or(0, |query| match query.mode {
            TemporalMode::Current | TemporalMode::History => query.evaluation_time_utc_nanos,
            TemporalMode::AsOf => query.as_of_utc_nanos.unwrap_or(0),
            TemporalMode::Interval => query.interval_start_utc_nanos.unwrap_or(0),
        });
        let until = temporal
            .and_then(|query| query.interval_end_utc_nanos)
            .unwrap_or(from);
        let unknown = temporal.is_none_or(|query| {
            query.unknown_validity_policy != CommonUnknownValidityPolicy::Exclude
        });
        let superseded = temporal.is_none_or(|query| query.include_superseded);
        let revoked = temporal.is_none_or(|query| query.include_revoked);

        let allowed: Vec<String> = history_grant
            .and_then(|grant| grant.get("sources"))
            .and_then(Value::as_array)
            .map(|sources| {
                sources
                    .iter()
                    .filter(|source| {
                        matches!(
                            source
                                .pointer("/current_disposition/state")
                                .and_then(Value::as_str),
                            Some("available" | "superseded" | "revoked")
                        )
                    })
                    .filter_map(|source| source.get("attribution"))
                    .map(Value::to_string)
                    .collect()
            })
            .unwrap_or_default();
        let allowed = serde_json::to_string(&allowed)
            .map_err(|_| StagedStoreError::InvalidAdvisory("history sources"))?;
        let exclusions = exclusions.cloned().unwrap_or_default();
        let exclusions=serde_json::json!({"stable_memory_refs":exclusions.stable_memory_refs,"candidate_ids":exclusions.candidate_ids,
            "source_refs":exclusions.source_refs,"trace_refs":exclusions.trace_refs,"observation_ids":exclusions.observation_ids,"content_sha256":exclusions.content_sha256}).to_string();
        let guard = self.connection()?;
        let mut statement = guard.prepare(
            "SELECT idempotency_key, profile_id, project_id, repository_identity, \
                    worktree_identity, branch_identity, agent_session_id, resolved_scope_digest, \
                    source_authority, source_event_id, source_revision, observation_kind, \
                    payload_contract, sanitized_payload, payload_sha256, operation_id, \
                    request_identity, provider_reference, receipt, effect_digest, \
                    admitted_sequence, admitted_at_unix_ms, exact_scope_sha256, actual_revision, original_source, feedback, validity_override \
             FROM tdmem_native_staged_observation_v1 \
             WHERE profile_id = ?1 AND project_id = ?2 AND repository_identity = ?3 \
               AND worktree_identity = ?4 AND branch_identity = ?5 AND tombstone = 0 \
               AND provider_reference NOT IN (SELECT value FROM json_each(?15,'$.stable_memory_refs')) \
               AND (?16 || ':' || provider_reference) NOT IN (SELECT value FROM json_each(?15,'$.candidate_ids')) \
               AND COALESCE(json_extract(original_source,'$.source.source_key'),'') NOT IN (SELECT value FROM json_each(?15,'$.source_refs')) \
               AND ('operation:' || operation_id) NOT IN (SELECT value FROM json_each(?15,'$.trace_refs')) \
               AND source_event_id NOT IN (SELECT value FROM json_each(?15,'$.observation_ids')) \
               AND COALESCE(json_extract(original_source,'$.source.observation_id'),'') NOT IN (SELECT value FROM json_each(?15,'$.observation_ids')) \
               AND COALESCE(projected_content_sha256,'') NOT IN (SELECT value FROM json_each(?15,'$.content_sha256')) \
               AND COALESCE(json_extract(original_source,'$.source.content_sha256'),'') NOT IN (SELECT value FROM json_each(?15,'$.content_sha256')) \
               AND (?7 != 'interval' OR valid_from_nanos IS NULL OR ((valid_until_nanos IS NULL OR valid_until_nanos > valid_from_nanos) \
                 AND (?11 OR superseded_nanos IS NULL OR superseded_nanos > valid_from_nanos) \
                 AND (?12 OR revoked_nanos IS NULL OR revoked_nanos > valid_from_nanos))) \
               AND NOT EXISTS(SELECT 1 FROM tdmem_native_deleted_source_v2 fence WHERE fence.checkout=?18 \
                 AND fence.source_key=COALESCE(json_extract(original_source,'$.source.source_key'),deleted_source_key,source_event_id)) \
               AND (?7 = 'legacy' OR feedback_suppressed = 0) \
               AND NOT EXISTS(SELECT 1 FROM json_each(?17) fresh WHERE json_extract(fresh.value,'$.attribution') = json(original_source) \
                 AND (json_extract(fresh.value,'$.current_disposition.state') NOT IN ('available','superseded','revoked') \
                   OR (json_extract(fresh.value,'$.current_disposition.state') = 'superseded' AND superseded_nanos IS NULL) \
                   OR (json_extract(fresh.value,'$.current_disposition.state') = 'revoked' AND revoked_nanos IS NULL))) \
               AND (?7 = 'legacy' OR exact_scope_sha256 = ?13 OR original_source IN (SELECT value FROM json_each(?14))) \
               AND (?7 = 'legacy' OR ( \
                 ((valid_from_nanos IS NULL AND ?10) OR \
                  (valid_from_nanos IS NOT NULL AND ((?7 = 'interval' AND valid_from_nanos < ?9) OR (?7 != 'interval' AND valid_from_nanos <= ?8)))) \
                 AND ((?7 = 'history' AND valid_from_nanos IS NOT NULL) OR valid_until_nanos IS NULL OR valid_until_nanos > ?8) \
                 AND (?11 OR superseded_nanos IS NULL OR superseded_nanos > ?8) \
                 AND (?12 OR revoked_nanos IS NULL OR revoked_nanos > ?8))) \
             ORDER BY admitted_sequence DESC LIMIT ?6",
        )?;
        let mut raw = Vec::new();
        let mut rows = statement.query(params![
            scope.profile_id,
            scope.project_id,
            scope.repository_identity,
            scope.worktree_identity,
            scope.branch_identity,
            scan_limit,
            mode,
            from,
            until,
            unknown,
            superseded,
            revoked,
            scope.exact_scope_sha256(),
            allowed,
            exclusions,
            request_id,
            history_grant
                .and_then(|grant| grant.get("sources"))
                .cloned()
                .unwrap_or_else(|| serde_json::json!([]))
                .to_string(),
            checkout_digest(scope),
        ])?;
        while let Some(row) = rows.next()? {
            let stored_scope = ExactScopeFields {
                profile_id: row.get(1)?,
                project_id: row.get(2)?,
                repository_identity: row.get(3)?,
                worktree_identity: row.get(4)?,
                branch_identity: row.get(5)?,
                agent_session_id: row.get(6)?,
                resolved_scope_digest: row.get(7)?,
            };
            let idempotency_key: String = row.get(0)?;
            let exact_scope_sha256: String = row.get(22)?;
            if stored_scope.exact_scope_sha256() != exact_scope_sha256 {
                return Err(StagedStoreError::ScopeDigestMismatch {
                    idempotency_key,
                    stored_exact_scope_sha256: exact_scope_sha256,
                });
            }
            let payload: Vec<u8> = row.get(13)?;
            let payload_sha256: String = row.get(14)?;
            if sha256_hex(&payload) != payload_sha256 {
                return Err(StagedStoreError::PayloadDigestMismatch {
                    idempotency_key,
                    stored_payload_sha256: payload_sha256,
                });
            }
            let Some(message_text) = extract_message_text(&payload) else {
                continue;
            };
            let source_revision: Option<String> = row.get(23)?;
            let original_source = row
                .get::<_, Option<String>>(24)?
                .map(|text| {
                    let value: Value = serde_json::from_str(&text).map_err(|_| {
                        StagedStoreError::InvalidAdvisory("stored source attribution")
                    })?;
                    validate_attribution(&value)?;
                    Ok::<_, StagedStoreError>(value)
                })
                .transpose()?;
            let admitted_sequence = u64::try_from(row.get::<_, i64>(20)?).map_err(|_| {
                StagedStoreError::ValueOutOfRange {
                    field: "admitted_sequence",
                }
            })?;
            raw.push(StagedRow {
                scope: stored_scope,
                exact_scope_sha256: exact_scope_sha256.clone(),
                idempotency_key,
                source_authority: row.get(8)?,
                source_event_id: row.get(9)?,
                source_revision,
                observation_kind: row.get(11)?,
                payload_contract: row.get(12)?,
                payload_sha256,
                message_text,
                operation_id: row.get(15)?,
                request_identity: row.get(16)?,
                provider_reference: row.get(17)?,
                receipt: row.get(18)?,
                effect_digest: row.get(19)?,
                admitted_sequence,
                admitted_at_unix_ms: row.get(21)?,
                score: 0.0,
                original_source: original_source.clone(),
                feedback: row.get(25)?,
                validity: match row.get::<_,Option<String>>(26)? {
                    Some(text) => recorded_validity(Some(&serde_json::json!({"validity":serde_json::from_str::<Value>(&text).map_err(|_|StagedStoreError::InvalidAdvisory("validity overlay"))?})))?,
                    None => recorded_validity(original_source.as_ref())?,
                },
            });
        }
        drop(rows);
        drop(statement);
        drop(guard);

        // SQL selects the newest bounded window. Restore oldest-first order
        // before assigning the existing deterministic recency ranks.
        raw.reverse();
        let total = raw.len();
        for (position, candidate) in raw.iter_mut().enumerate() {
            #[allow(clippy::cast_precision_loss)]
            let recency = (position as f64 + 1.0) / (total as f64);
            let lexical = lexical_overlap(&query_tokens, &candidate.message_text);
            candidate.score = (RECENCY_WEIGHT.mul_add(recency, LEXICAL_WEIGHT * lexical)
                + candidate.feedback * 0.5)
                .clamp(0.0, 1.0);
        }
        raw.sort_by(|left, right| {
            right
                .score
                .total_cmp(&left.score)
                .then(right.admitted_sequence.cmp(&left.admitted_sequence))
                .then(left.idempotency_key.cmp(&right.idempotency_key))
        });
        raw.truncate(limit);
        Ok(raw)
    }

    /// Test-only: makes the next [`Self::stage_or_duplicate`] fail after the
    /// insert and before the commit, proving durability-before-success.
    #[cfg(test)]
    pub(crate) fn lose_next_reply(&self) {
        self.lose_next_reply
            .store(true, std::sync::atomic::Ordering::SeqCst);
    }

    #[cfg(test)]
    pub(crate) fn take_lost_reply(&self) -> bool {
        self.lose_next_reply
            .swap(false, std::sync::atomic::Ordering::SeqCst)
    }

    #[cfg(test)]
    pub(crate) fn fail_next_commit(&self) {
        self.fail_next_commit
            .store(true, std::sync::atomic::Ordering::SeqCst);
    }

    /// Test-only: makes the next [`Self::recall`] fail before it reads any
    /// row, proving the port answers `provider_unavailable` when the staged
    /// store cannot be read.
    #[cfg(test)]
    pub(crate) fn fail_next_recall(&self) {
        self.fail_next_recall
            .store(true, std::sync::atomic::Ordering::SeqCst);
    }

    fn connection(&self) -> Result<MutexGuard<'_, Connection>, StagedStoreError> {
        self.connection
            .lock()
            .map_err(|_| StagedStoreError::LockPoisoned)
    }
}

impl StagedObservationRecord {
    fn validate(&self) -> Result<(), StagedStoreError> {
        self.scope
            .validate()
            .map_err(StagedStoreError::InvalidScope)?;
        for (field, value) in [
            ("idempotency_key", self.idempotency_key.as_str()),
            ("source_authority", self.source_authority.as_str()),
            ("source_event_id", self.source_event_id.as_str()),
            ("observation_kind", self.observation_kind.as_str()),
            ("payload_contract", self.payload_contract.as_str()),
            ("operation_id", self.operation_id.as_str()),
            ("request_identity", self.request_identity.as_str()),
        ] {
            if value.is_empty() {
                return Err(StagedStoreError::EmptyField { field });
            }
        }
        if self.sanitized_payload.is_empty() {
            return Err(StagedStoreError::EmptyField {
                field: "sanitized_payload",
            });
        }
        Ok(())
    }
}

fn initialize_schema(connection: &mut Connection) -> Result<(), StagedStoreError> {
    let _mode: String = connection.query_row("PRAGMA journal_mode = WAL", [], |row| row.get(0))?;
    connection.execute_batch(
        "PRAGMA synchronous = FULL;\n\
         PRAGMA foreign_keys = ON;\n\
         PRAGMA secure_delete = ON;\n\
         PRAGMA temp_store = MEMORY;",
    )?;
    connection.busy_timeout(Duration::from_millis(BUSY_TIMEOUT_MILLIS))?;

    let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
    let version: i64 = transaction.query_row("PRAGMA user_version", [], |row| row.get(0))?;
    if version > SCHEMA_VERSION {
        return Err(StagedStoreError::SchemaAhead {
            found: version,
            supported: SCHEMA_VERSION,
        });
    }
    transaction.execute_batch(SCHEMA_DDL)?;
    if version < 2 {
        transaction.execute_batch("ALTER TABLE tdmem_native_staged_observation_v1 ADD COLUMN actual_revision TEXT;
            ALTER TABLE tdmem_native_staged_observation_v1 ADD COLUMN semantic_sha256 TEXT;
            ALTER TABLE tdmem_native_staged_observation_v1 ADD COLUMN projected_content_sha256 TEXT;
            ALTER TABLE tdmem_native_staged_observation_v1 ADD COLUMN deleted_source_key TEXT;
            ALTER TABLE tdmem_native_staged_observation_v1 ADD COLUMN original_source TEXT;
            ALTER TABLE tdmem_native_staged_observation_v1 ADD COLUMN feedback REAL NOT NULL DEFAULT 0;
            ALTER TABLE tdmem_native_staged_observation_v1 ADD COLUMN feedback_suppressed INTEGER NOT NULL DEFAULT 0;
            ALTER TABLE tdmem_native_staged_observation_v1 ADD COLUMN validity_override TEXT;
            ALTER TABLE tdmem_native_staged_observation_v1 ADD COLUMN valid_from_nanos INTEGER;
            ALTER TABLE tdmem_native_staged_observation_v1 ADD COLUMN valid_until_nanos INTEGER;
            ALTER TABLE tdmem_native_staged_observation_v1 ADD COLUMN superseded_nanos INTEGER;
            ALTER TABLE tdmem_native_staged_observation_v1 ADD COLUMN revoked_nanos INTEGER;
            DROP INDEX IF EXISTS tdmem_native_staged_observation_source_v1;
            CREATE UNIQUE INDEX tdmem_native_staged_observation_source_v2 ON tdmem_native_staged_observation_v1
                (exact_scope_sha256, source_authority, source_event_id, COALESCE(actual_revision, ''), payload_sha256);")?;
    }
    if version < 2 {
        let mut statement=transaction.prepare("SELECT provider_reference,sanitized_payload FROM tdmem_native_staged_observation_v1 WHERE sanitized_payload IS NOT NULL")?;
        let mut rows = statement.query([])?;
        while let Some(row) = rows.next()? {
            let reference: String = row.get(0)?;
            let payload: Vec<u8> = row.get(1)?;
            let semantic = semantic_observation_digest(&payload)?;
            let content = extract_message_text(&payload).map(|text| sha256_hex(text.as_bytes()));
            transaction.execute("UPDATE tdmem_native_staged_observation_v1 SET semantic_sha256=?1,projected_content_sha256=?2 WHERE provider_reference=?3",params![semantic,content,reference])?;
        }
    }
    transaction.execute_batch(ADVISORY_DDL)?;
    transaction.execute("UPDATE tdmem_native_state_v2 SET generation = MAX(generation,
        (SELECT COALESCE(MAX(admitted_sequence), 0) FROM tdmem_native_staged_observation_v1)) WHERE singleton = 1", [])?;
    transaction.execute_batch(&format!("PRAGMA user_version = {SCHEMA_VERSION}"))?;
    transaction.commit()?;
    Ok(())
}

/// Evicts the content of every row of one scope beyond the newest `keep`
/// content-bearing rows. Identity, source identity, payload digest, and effect
/// evidence survive: an evicted key still answers duplicate.
fn evict_scope_overflow(
    transaction: &rusqlite::Transaction<'_>,
    exact_scope_sha256: &str,
    keep: usize,
) -> Result<(), StagedStoreError> {
    let keep = i64::try_from(keep.max(1)).unwrap_or(i64::MAX);
    transaction.execute(
        "UPDATE tdmem_native_staged_observation_v1 \
         SET sanitized_payload = NULL, tombstone = 1 \
         WHERE exact_scope_sha256 = ?1 AND tombstone = 0 \
           AND admitted_sequence NOT IN ( \
               SELECT admitted_sequence FROM tdmem_native_staged_observation_v1 \
               WHERE exact_scope_sha256 = ?1 AND tombstone = 0 \
               ORDER BY admitted_sequence DESC LIMIT ?2)",
        params![exact_scope_sha256, keep],
    )?;
    Ok(())
}

/// Derives the three deterministic evidence values from the committing row's
/// identity. The same inputs always produce the same evidence, so the stored
/// columns are the single source a redelivery answers from — never a freshly
/// minted acknowledgement. What the wire then carries is decided by the
/// committed-effect contract, not here.
fn derive_effect_evidence(
    exact_scope_sha256: &str,
    idempotency_key: &str,
    payload_sha256: &str,
    admitted_sequence: u64,
    operation_id: &str,
) -> StagedEffectEvidence {
    let sequence_bytes = admitted_sequence.to_be_bytes();
    let parts: [&[u8]; 4] = [
        exact_scope_sha256.as_bytes(),
        idempotency_key.as_bytes(),
        payload_sha256.as_bytes(),
        &sequence_bytes,
    ];
    let reference_digest = canonical_framed_sha256(PROVIDER_REFERENCE_DIGEST_DOMAIN, &parts);
    StagedEffectEvidence {
        provider_reference: format!("{PROVIDER_REFERENCE_PREFIX}{reference_digest}"),
        receipt: canonical_framed_sha256(RECEIPT_DIGEST_DOMAIN, &parts),
        effect_digest: canonical_framed_sha256(EFFECT_DIGEST_DOMAIN, &parts),
        admitted_sequence,
        idempotency_key: idempotency_key.to_owned(),
        operation_id: operation_id.to_owned(),
    }
}

/// Extracts the human message text of a `session.message_committed.v1`
/// observation from its sanitized canonical payload bytes.
///
/// The payload is the provider observation envelope
/// (`{canonical_payload, observation_kind, payload_contract}`); the message
/// text lives in the canonical payload's `facts` array, in every entry whose
/// `kind` is `message`. Returns `None` — never envelope JSON, never the raw
/// payload — when the bytes are not that contract, or carry no message text.
pub(crate) fn extract_message_text(payload: &[u8]) -> Option<String> {
    let envelope: Value = serde_json::from_slice(payload).ok()?;
    let object = envelope.as_object()?;
    let kind = object.get("observation_kind")?.as_str()?;
    let fields: &[&str] = match kind {
        "source.edit_settled.v1" => &["claim", "status", "assertion", "reason"],
        "test.execution_settled.v1" => &["assertion", "status", "approach", "outcome", "reason"],
        "feedback.outcome_settled.v1" => {
            &["approach", "outcome", "signal", "reason", "claim", "status"]
        }
        _ => &[],
    };
    if !fields.is_empty() {
        let expected = match kind {
            "source.edit_settled.v1" => "tracedecay.memory.observation.source-edit.v1",
            "test.execution_settled.v1" => "tracedecay.memory.observation.test-execution.v1",
            "feedback.outcome_settled.v1" => "tracedecay.memory.observation.feedback-outcome.v1",
            _ => return None,
        };
        if object.get("payload_contract")?.as_str()? != expected {
            return None;
        }
        let payload = object.get("canonical_payload")?.as_object()?;
        let mut lines = Vec::new();
        for field in fields {
            if let Some(value) = payload.get(*field) {
                let text = value.as_str()?;
                if text.len() > 8192 {
                    return None;
                }
                if !text.trim().is_empty() {
                    lines.push(format!("{field}: {text}"));
                }
            }
        }
        let text = lines.join("\n");
        return (!text.is_empty() && text.len() <= 32768).then_some(text);
    }
    if object.get("observation_kind").and_then(Value::as_str)
        != Some(STAGED_SESSION_OBSERVATION_KIND)
    {
        return None;
    }
    if object.get("payload_contract").and_then(Value::as_str)
        != Some(STAGED_SESSION_PAYLOAD_CONTRACT)
    {
        return None;
    }
    let canonical = object.get("canonical_payload")?;
    if canonical.get("facts").is_none()
        && envelope
            .pointer("/source_identity/original_source")
            .is_some()
    {
        if canonical
            .get("session_id")
            .and_then(Value::as_str)
            .is_none()
            || canonical
                .get("message_id")
                .and_then(Value::as_str)
                .is_none()
            || !matches!(
                canonical.get("role").and_then(Value::as_str),
                Some("user" | "assistant" | "system" | "tool")
            )
        {
            return None;
        }
        return canonical.get("content").and_then(message_content_text);
    }
    let facts = canonical.get("facts")?.as_array()?;
    let mut segments = Vec::new();
    for fact in facts {
        if fact.get("kind").and_then(Value::as_str) != Some("message") {
            continue;
        }
        if let Some(text) = fact.get("content").and_then(message_content_text) {
            segments.push(text);
        }
    }
    if segments.is_empty() {
        None
    } else {
        Some(segments.join("\n"))
    }
}

/// The text of one canonical message fact's `content`, across the three shapes
/// the canonical envelope permits: a bare string, `{"text": ...}`, or an array
/// of either.
fn message_content_text(content: &Value) -> Option<String> {
    match content {
        Value::String(text) => non_empty(text),
        Value::Object(map) => map.get("text").and_then(Value::as_str).and_then(non_empty),
        Value::Array(items) => {
            let segments: Vec<String> = items.iter().filter_map(message_content_text).collect();
            if segments.is_empty() {
                None
            } else {
                Some(segments.join("\n"))
            }
        }
        _ => None,
    }
}

fn non_empty(value: &str) -> Option<String> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        None
    } else {
        Some(trimmed.to_owned())
    }
}

/// The one fixed normalization staged recall scores under: lowercase, split on
/// every non-alphanumeric byte, deduplicated.
fn normalized_tokens(text: &str) -> BTreeSet<String> {
    text.split(|character: char| !character.is_alphanumeric())
        .filter(|token| !token.is_empty())
        .map(str::to_lowercase)
        .collect()
}

/// Fraction of the query's distinct tokens the text carries. An empty query
/// scores zero for every row, so an empty query orders purely by recency.
fn lexical_overlap(query_tokens: &BTreeSet<String>, text: &str) -> f64 {
    if query_tokens.is_empty() {
        return 0.0;
    }
    let text_tokens = normalized_tokens(text);
    let matched = query_tokens
        .iter()
        .filter(|token| text_tokens.contains(*token))
        .count();
    #[allow(clippy::cast_precision_loss)]
    let ratio = matched as f64 / query_tokens.len() as f64;
    ratio
}

#[cfg(test)]
mod tests {
    use super::*;

    use serde_json::json;
    use tempfile::TempDir;

    fn scope(session: &str) -> ExactScopeFields {
        ExactScopeFields {
            profile_id: "profile.fixture".to_owned(),
            project_id: "project.fixture".to_owned(),
            repository_identity: "repository.fixture".to_owned(),
            worktree_identity: "worktree.fixture".to_owned(),
            branch_identity: "refs/heads/master".to_owned(),
            agent_session_id: session.to_owned(),
            resolved_scope_digest: format!("sha256:{}", "a".repeat(64)),
        }
    }

    #[test]
    fn sqlite_counter_boundaries_reject_negative_reads_and_overflow_without_mutation() {
        let root = TempDir::new().expect("temp root");
        let staged = store(&root, 8);
        {
            let connection = staged.connection().expect("connection");
            assert!(matches!(
                connection.query_row("SELECT -1", [], |row| sqlite_u64(row, 0)),
                Err(rusqlite::Error::FromSqlConversionFailure(..))
            ));
            connection
                .execute(
                    "UPDATE tdmem_native_state_v2 SET generation = ?1 WHERE singleton = 1",
                    params![i64::MAX],
                )
                .expect("arrange exhausted SQLite generation");
        }
        assert!(matches!(
            staged.stage_or_duplicate(record(
                &scope("session.overflow"),
                "key.overflow",
                "event.overflow",
                1,
                "overflow beacon"
            )),
            Err(StagedStoreError::ValueOutOfRange {
                field: "admitted_sequence"
            })
        ));
        let connection = staged.connection().expect("connection after refusal");
        assert_eq!(
            generation_in(&connection).expect("generation preserved"),
            i64::MAX
        );
        assert_eq!(
            connection
                .query_row(
                    "SELECT COUNT(*) FROM tdmem_native_staged_observation_v1",
                    [],
                    |row| row.get::<_, i64>(0)
                )
                .expect("unchanged rows"),
            0
        );
        assert!(matches!(
            sqlite_i64(u64::MAX, "replay sequence"),
            Err(StagedStoreError::ValueOutOfRange {
                field: "replay sequence"
            })
        ));
    }

    fn envelope(text: &str) -> Vec<u8> {
        serde_json::to_vec(&json!({
            "canonical_payload": {
                "version": 1,
                "provider": "claude",
                "native_record_kind": "message",
                "stable_record_id": "record.fixture",
                "facts": [
                    {
                        "kind": "message",
                        "role": "assistant",
                        "content": {"text": text},
                        "model": "model.fixture",
                    }
                ],
            },
            "observation_kind": STAGED_SESSION_OBSERVATION_KIND,
            "payload_contract": STAGED_SESSION_PAYLOAD_CONTRACT,
        }))
        .expect("envelope bytes")
    }

    fn record(
        scope: &ExactScopeFields,
        key: &str,
        event_id: &str,
        revision: u64,
        text: &str,
    ) -> StagedObservationRecord {
        StagedObservationRecord {
            scope: scope.clone(),
            idempotency_key: key.to_owned(),
            source_authority: "host_session".to_owned(),
            source_event_id: event_id.to_owned(),
            source_revision: Some(revision.to_string()),
            observation_kind: STAGED_SESSION_OBSERVATION_KIND.to_owned(),
            payload_contract: STAGED_SESSION_PAYLOAD_CONTRACT.to_owned(),
            sanitized_payload: envelope(text),
            operation_id: format!("operation.{key}"),
            request_identity: format!("request.{key}"),
            admitted_at_unix_ms: 1_750_000_000_000,
        }
    }

    fn store(root: &TempDir, keep: usize) -> StagedObservationStore {
        StagedObservationStore::open_with_retention(
            root.path(),
            StagedRetentionPolicyV1 {
                maximum_content_rows_per_scope: keep,
            },
        )
        .expect("staged store")
    }

    fn row_count(store: &StagedObservationStore) -> i64 {
        let connection = store.connection().expect("connection");
        connection
            .query_row(
                "SELECT COUNT(*) FROM tdmem_native_staged_observation_v1",
                [],
                |row| row.get(0),
            )
            .expect("count")
    }

    fn lifecycle_call(
        scope: &ExactScopeFields,
        registration_revision: u64,
        operation: tracedecay_memory_provider_registry::ProviderOperation,
        key: &str,
        generation: u64,
        request: &Value,
    ) -> tracedecay_memory_provider_registry::ProviderCall {
        use tracedecay_memory_provider_registry::{
            CancellationToken, CanonicalPayload, OperationControl, OwnedProviderId,
            OwnedVersionedId, ProviderCall, ProviderCallParts,
        };
        let bytes = serde_json::to_vec(request).expect("canonical lifecycle bytes");
        let contract = if operation
            == tracedecay_memory_provider_registry::ProviderOperation::DeleteBySource
        {
            "tracedecay.memory.provider.deletion-by-source.v1".to_owned()
        } else {
            format!("tracedecay.memory.provider.{}.v1", operation.as_wire())
        };
        ProviderCall::new(ProviderCallParts {
            operation,
            provider_id: OwnedProviderId::new("tracedecay.native").expect("Native provider"),
            registration_revision,
            ready_receipt_sha256: "a".repeat(64),
            exact_scope: scope.clone(),
            request_id: format!("request.{key}"),
            operation_id: format!("operation.{key}"),
            expected_state_generation: generation,
            idempotency_key: operation.mutates_provider_state().then(|| key.to_owned()),
            control: OperationControl::new(i64::MAX, 1000, CancellationToken::new()),
            payload: CanonicalPayload::new(
                OwnedVersionedId::new(contract).expect("lifecycle contract"),
                bytes.clone(),
                sha256_hex(&bytes),
            )
            .expect("lifecycle payload"),
            required_capabilities: vec![
                OwnedVersionedId::new(operation.capability_id()).expect("operation capability"),
            ],
            extensions: Vec::new(),
        })
        .expect("lifecycle call")
    }

    #[test]
    fn maintenance_inspection_preserves_journal_evidence_and_refuses_corruption() {
        use tracedecay_memory_provider_registry::ProviderOperation;
        let root = TempDir::new().expect("root");
        let exact_scope = scope("session.maintenance-inspection");
        let staged = store(&root, 8);
        staged
            .stage_or_duplicate(record(
                &exact_scope,
                "maintenance-source",
                "maintenance-event",
                1,
                "maintenance beacon",
            ))
            .expect("populated staged store");
        let maintenance_request = json!({"task":"validate_state","dry_run":false,
            "maximum_items":64,"maximum_bytes":65536,"maximum_duration_millis":1000,"resume_cursor":null});
        let maintenance = lifecycle_call(
            &exact_scope,
            1,
            ProviderOperation::Maintenance,
            "maintenance.first",
            staged.generation().expect("generation"),
            &maintenance_request,
        );
        let first = staged
            .control(&maintenance, None)
            .expect("actual maintenance");
        assert_eq!(first.response["scanned_items"], 1);
        let original_response = first.response.to_string();
        let later = lifecycle_call(
            &exact_scope,
            1,
            ProviderOperation::Maintenance,
            "maintenance.later",
            first.generation_after,
            &maintenance_request,
        );
        let current = staged
            .control(&later, None)
            .expect("later maintenance")
            .generation_after;
        assert!(current > first.generation_after);
        drop(staged);
        let staged = store(&root, 8);
        let request = json!({"view":"maintenance_receipt",
            "selector":{"operation_id":maintenance.operation_id,"idempotency_key":"maintenance.first"},
            "maximum_items":64,"maximum_bytes":65536,"redaction_policy_revision":1,"cursor":null});
        let inspect = lifecycle_call(
            &exact_scope,
            1,
            ProviderOperation::Inspection,
            "inspect.maintenance",
            0,
            &request,
        );
        let inspected = staged
            .control(&inspect, None)
            .expect("persisted maintenance inspection");
        assert_eq!(inspected.generation_after, current);
        assert_eq!(inspected.response["state_generation"], current);
        assert_eq!(inspected.response["state_generation_before"], current);
        assert_eq!(inspected.response["state_generation_after"], current);
        assert_eq!(inspected.response["coverage"], "complete");
        assert_eq!(inspected.response["warnings"], first.response["warnings"]);
        let mut expected = first.response.clone();
        let fields = expected.as_object_mut().expect("maintenance object");
        for field in [
            "provider_receipt_digest",
            "state_generation_before",
            "state_generation_after",
            "warnings",
        ] {
            fields.remove(field);
        }
        expected["receipt"] = json!({"state_generation_before":first.generation_before,
            "state_generation_after":first.generation_after,"provider_receipt_digest":first.receipt});
        assert_eq!(
            inspected.response["items"],
            json!([{"operation_id":maintenance.operation_id,
            "idempotency_key":"maintenance.first","outcome":expected}])
        );

        let mut key_only = request.clone();
        key_only["selector"]
            .as_object_mut()
            .expect("selector")
            .remove("operation_id");
        let key_only_call = lifecycle_call(
            &exact_scope,
            1,
            ProviderOperation::Inspection,
            "inspect.key-only",
            current,
            &key_only,
        );
        assert_eq!(
            staged
                .control(&key_only_call, None)
                .expect("key-only inspection")
                .response["items"],
            inspected.response["items"]
        );
        for selector in [
            json!({"idempotency_key":"maintenance.missing"}),
            json!({"idempotency_key":"maintenance.first","operation_id":"operation.wrong"}),
        ] {
            let mut missing = request.clone();
            missing["selector"] = selector;
            let call = lifecycle_call(
                &exact_scope,
                1,
                ProviderOperation::Inspection,
                "inspect.missing",
                current,
                &missing,
            );
            let result = staged.control(&call, None).expect("honest missing receipt");
            assert_eq!(result.response["items"], json!([]));
            assert_eq!(result.response["coverage"], "complete");
        }
        let other_scope = scope("session.other-maintenance-inspection");
        let other = lifecycle_call(
            &other_scope,
            1,
            ProviderOperation::Inspection,
            "inspect.other-scope",
            current,
            &request,
        );
        assert_eq!(
            staged.control(&other, None).expect("other scope").response["items"],
            json!([])
        );
        let mut dry_run = maintenance_request.clone();
        dry_run["dry_run"] = true.into();
        let dry = lifecycle_call(
            &exact_scope,
            1,
            ProviderOperation::Maintenance,
            "maintenance.dry",
            current,
            &dry_run,
        );
        assert!(!staged.control(&dry, None).expect("actual dry run").changed);
        let mut dry_inspection = request.clone();
        dry_inspection["selector"] =
            json!({"operation_id":dry.operation_id,"idempotency_key":"maintenance.dry"});
        let dry_inspect = lifecycle_call(
            &exact_scope,
            1,
            ProviderOperation::Inspection,
            "inspect.dry",
            current,
            &dry_inspection,
        );
        assert_eq!(
            staged
                .control(&dry_inspect, None)
                .expect("dry run has no journal")
                .response["items"],
            json!([])
        );
        let mut summary_request = request.clone();
        summary_request["view"] = "state_summary".into();
        summary_request["selector"] = json!({});
        let summary = lifecycle_call(
            &exact_scope,
            1,
            ProviderOperation::Inspection,
            "inspect.summary",
            current,
            &summary_request,
        );
        let summary = staged
            .control(&summary, None)
            .expect("explicit state summary");
        assert_eq!(
            summary.response["items"]
                .as_array()
                .expect("summary rows")
                .len(),
            1
        );
        assert!(summary.response["items"][0]["stable_memory_ref"].is_string());
        summary_request["view"] = "snapshot_metadata".into();
        let snapshot = lifecycle_call(
            &exact_scope,
            1,
            ProviderOperation::Inspection,
            "inspect.snapshot",
            current,
            &summary_request,
        );
        assert!(matches!(
            staged.control(&snapshot, None),
            Err(StagedStoreError::InvalidAdvisory("inspection view"))
        ));

        for (field, invalid) in [
            ("task", json!("unknown_task")),
            ("dry_run", json!(true)),
            ("scanned_items", json!("1")),
            ("changed_items", json!(2)),
            ("proposed_changes", Value::Null),
            ("partial", json!("false")),
            ("resume_cursor", json!("unexpected")),
            ("warnings", json!([1])),
            ("provider_receipt_digest", json!("f".repeat(64))),
            (
                "state_generation_before",
                json!(first.generation_before + 1),
            ),
        ] {
            let mut corrupt = first.response.clone();
            corrupt[field] = invalid;
            {
                let connection = staged.connection().expect("connection");
                connection.execute("UPDATE tdmem_native_operation_v2 SET response=?1 WHERE scope=?2 AND idempotency_key=?3",
                    params![corrupt.to_string(), exact_scope.exact_scope_sha256(), "maintenance.first"]).expect("corrupt response fixture");
            }
            assert!(
                staged.control(&inspect, None).is_err(),
                "corrupt {field} must refuse"
            );
        }
        {
            let mut forged = first.response.clone();
            forged["provider_receipt_digest"] = "f".repeat(64).into();
            let connection = staged.connection().expect("connection");
            connection.execute("UPDATE tdmem_native_operation_v2 SET response=?1,receipt=?2 WHERE scope=?3 AND idempotency_key=?4",
                params![forged.to_string(), "f".repeat(64), exact_scope.exact_scope_sha256(), "maintenance.first"]).expect("corrupt consistent receipt claims");
        }
        assert!(staged.control(&inspect, None).is_err());
        {
            let connection = staged.connection().expect("connection");
            connection.execute("UPDATE tdmem_native_operation_v2 SET response=?1,receipt=?2 WHERE scope=?3 AND idempotency_key=?4",
                params![original_response, first.receipt, exact_scope.exact_scope_sha256(), "maintenance.first"]).expect("restore receipt evidence");
        }
        let mut too_small = request.clone();
        too_small["maximum_bytes"] = 1.into();
        let small_call = lifecycle_call(
            &exact_scope,
            1,
            ProviderOperation::Inspection,
            "inspect.small",
            current,
            &too_small,
        );
        assert!(matches!(
            staged.control(&small_call, None),
            Err(StagedStoreError::ValueOutOfRange { .. })
        ));
        assert_eq!(
            staged
                .control(&maintenance, None)
                .expect("original redelivery")
                .response
                .to_string(),
            original_response
        );
        assert_eq!(
            staged
                .generation()
                .expect("unchanged inspection generation"),
            current
        );
    }

    #[test]
    fn lifecycle_target_receipts_preserve_attribution_and_inspection_across_reopen() {
        use tracedecay_memory_conformance::compatibility::{
            T1, T2, T3, common_observation, scope_json,
        };
        use tracedecay_memory_provider_registry::ProviderOperation;
        let root = TempDir::new().expect("root");
        let origin = scope("session.origin");
        let later = scope("session.later");
        let observation =
            common_observation(&origin, 1, Some("r1"), "original beacon", Some(T1), None);
        let mut offered = record(&origin, "key.original", "event.original", 1, "unused");
        offered.source_revision = Some("r1".into());
        offered.sanitized_payload = serde_json::to_vec(&observation).expect("observation bytes");
        let staged = store(&root, 8);
        let StagedOutcome::Committed(original) =
            staged.stage_or_duplicate(offered).expect("stage original")
        else {
            panic!("expected original commit");
        };
        let target = json!({
            "provider_id":"tracedecay.native", "registration_revision":1,
            "original_scope":observation["source_identity"]["original_source"]["origin_scope"],
            "delivery_scope":scope_json(&origin),
            "source":observation["source_identity"]["original_source"]["source"],
            "reference":{"kind":"stable_memory_ref","reference":original.provider_reference},
        });
        let feedback = json!({"target":target,"signal":"helpful","weight":"0.5",
            "canonical_outcome_receipt":"fixture.settled","evidence_refs":[],"occurred_at":T1});
        let call = lifecycle_call(
            &origin,
            1,
            ProviderOperation::Feedback,
            "feedback.original",
            staged.generation().expect("generation"),
            &feedback,
        );
        let first = staged.control(&call, None).expect("first feedback");
        let full_digest =
            sha256_hex(&serde_json::to_vec(&target).expect("full admitted target bytes"));
        assert_eq!(first.response["target_digest"], full_digest);
        assert_ne!(
            first.response["target_digest"],
            sha256_hex(original.provider_reference.as_bytes())
        );
        assert_eq!(
            first.response["applied_effect"]["stable_memory_ref"],
            original.provider_reference
        );

        // A fixture in the previous receipt representation keeps exactly its
        // original SHA(reference), response bytes and receipt on redelivery.
        let mut original_response = first.response.clone();
        original_response["target_digest"] =
            sha256_hex(original.provider_reference.as_bytes()).into();
        original_response["applied_effect"]
            .as_object_mut()
            .expect("effect object")
            .remove("stable_memory_ref");
        let original_bytes = original_response.to_string();
        staged.connection().expect("fixture connection").execute(
            "UPDATE tdmem_native_operation_v2 SET response=?1 WHERE scope=?2 AND idempotency_key=?3",
            params![original_bytes, origin.exact_scope_sha256(), "feedback.original"],
        ).expect("install previous receipt representation");
        drop(staged);
        let staged = store(&root, 8);
        let duplicate = staged.control(&call, None).expect("original redelivery");
        assert!(duplicate.duplicate);
        assert_eq!(duplicate.response.to_string(), original_bytes);
        assert_eq!(duplicate.receipt, first.receipt);
        assert_eq!(duplicate.operation_id, first.operation_id);
        assert_eq!(duplicate.generation_before, first.generation_before);
        assert_eq!(duplicate.generation_after, first.generation_after);

        let mut later_target = target.clone();
        later_target["registration_revision"] = 2.into();
        later_target["delivery_scope"] = scope_json(&later);
        let mut later_feedback = feedback.clone();
        later_feedback["target"] = later_target.clone();
        let later_call = lifecycle_call(
            &later,
            2,
            ProviderOperation::Feedback,
            "feedback.later",
            staged.generation().expect("generation"),
            &later_feedback,
        );
        let second = staged
            .control(&later_call, None)
            .expect("later-session feedback");
        assert_eq!(
            second.response["target_digest"],
            sha256_hex(&serde_json::to_vec(&later_target).expect("later full target"))
        );
        assert_ne!(second.response["target_digest"], full_digest);
        let mut foreign = later.clone();
        foreign.worktree_identity = "worktree.foreign".into();
        let mut foreign_feedback = later_feedback.clone();
        foreign_feedback["target"]["delivery_scope"] = scope_json(&foreign);
        let foreign_call = lifecycle_call(
            &foreign,
            2,
            ProviderOperation::Feedback,
            "feedback.foreign",
            staged.generation().expect("generation"),
            &foreign_feedback,
        );
        assert!(matches!(
            staged.control(&foreign_call, None),
            Err(StagedStoreError::LifecycleConflict("target unknown"))
        ));

        let mut correction = json!({"target":target,"correction_kind":"change_validity",
            "replacement":{"valid_from":T1,"valid_until":T3},"expected_target_revision":"r1",
            "reason":"verified boundary","evidence_refs":[]});
        let validity_call = lifecycle_call(
            &origin,
            1,
            ProviderOperation::Correction,
            "correction.validity",
            staged.generation().expect("generation"),
            &correction,
        );
        let validity = staged
            .control(&validity_call, None)
            .expect("validity correction");
        assert_eq!(validity.response["target_digest"], full_digest);
        correction["correction_kind"] = "replace_content".into();
        correction["replacement"] =
            common_observation(&origin, 1, Some("r2"), "corrected beacon", Some(T2), None);
        let replacement_call = lifecycle_call(
            &origin,
            1,
            ProviderOperation::Correction,
            "correction.content",
            staged.generation().expect("generation"),
            &correction,
        );
        let replacement = staged
            .control(&replacement_call, None)
            .expect("content correction");
        assert_eq!(replacement.response["target_digest"], full_digest);
        assert_eq!(
            replacement.response["affected_provider_effects"]
                .as_array()
                .expect("effects")
                .len(),
            2
        );
        drop(staged);
        let reopened = store(&root, 8);
        let inspection = json!({"view":"source_influence","selector":{"source_key":target["source"]["source_key"]},
            "maximum_items":8,"maximum_bytes":16384,"redaction_policy_revision":1,"cursor":null});
        let inspect_call = lifecycle_call(
            &origin,
            3,
            ProviderOperation::Inspection,
            "inspection.current",
            reopened.generation().expect("generation"),
            &inspection,
        );
        let inspected = reopened
            .control(&inspect_call, None)
            .expect("inspect current attribution");
        let item = inspected.response["items"]
            .as_array()
            .expect("inspection items")
            .iter()
            .find(|item| item["target"]["reference"]["reference"] == original.provider_reference)
            .expect("original influence");
        assert_eq!(item["target"]["registration_revision"], 3);
        assert_eq!(
            item["settled_feedback"],
            json!({"helpful":2,"harmful":0,"ignored":0,"corrected":0,"superseded":0})
        );
        assert_eq!(item["last_feedback_receipt"], second.receipt);
        let repeated = reopened
            .control(&later_call, None)
            .expect("new receipt redelivery");
        assert!(repeated.duplicate);
        assert_eq!(repeated.response, second.response);
        assert_eq!(repeated.receipt, second.receipt);
        assert_eq!(
            reopened
                .control(&call, None)
                .expect("old receipt remains original")
                .response
                .to_string(),
            original_bytes
        );
    }

    #[test]
    fn deletion_hashes_query_string_bytes_and_rejects_non_strings_before_mutation() {
        use tracedecay_memory_conformance::compatibility::{T1, common_observation};
        use tracedecay_memory_provider_registry::ProviderOperation;
        let root = TempDir::new().expect("root");
        let scope = scope("session.deletion");
        let observation =
            common_observation(&scope, 1, Some("r1"), "deletion beacon", Some(T1), None);
        let mut offered = record(&scope, "key.deletion", "event.deletion", 1, "unused");
        offered.source_revision = Some("r1".into());
        offered.sanitized_payload = serde_json::to_vec(&observation).expect("observation bytes");
        let staged = store(&root, 8);
        staged.stage_or_duplicate(offered).expect("stage source");
        let generation = staged.generation().expect("generation");
        let original_rows = staged
            .recall(&scope, "deletion", 8)
            .expect("positive source recall");
        assert_eq!(original_rows.len(), 1);
        let request = json!({
            "forget_source_keys":[observation["source_identity"]["original_source"]["source"]["source_key"]],
            "mode":"hard_delete", "include_snapshots":true,
            "retention_lock_policy_revision":1, "verification_query":"abc",
        });
        let call = lifecycle_call(
            &scope,
            1,
            ProviderOperation::DeleteBySource,
            "delete.source",
            generation,
            &request,
        );
        for malformed in [
            None,
            Some(Value::Null),
            Some(json!(42)),
            Some(json!({"query":"abc"})),
        ] {
            let mut invalid = request.clone();
            if let Some(value) = malformed {
                invalid["verification_query"] = value;
            } else {
                invalid
                    .as_object_mut()
                    .expect("request object")
                    .remove("verification_query");
            }
            // Call the actual handler without an outer transaction: a late
            // validation followed by rollback cannot hide an attempted delete.
            {
                let connection = staged.connection().expect("connection");
                assert!(matches!(
                    delete_advisory(&connection, &call, &invalid, generation + 1, None),
                    Err(StagedStoreError::InvalidAdvisory("verification query"))
                ));
                assert_eq!(
                    connection
                        .query_row(
                            "SELECT COUNT(*) FROM tdmem_native_deleted_source_v2",
                            [],
                            |row| row.get::<_, i64>(0)
                        )
                        .expect("deletion fences"),
                    0
                );
                assert_eq!(
                    connection
                        .query_row(
                            "SELECT COUNT(*) FROM tdmem_native_operation_v2",
                            [],
                            |row| row.get::<_, i64>(0)
                        )
                        .expect("operation receipts"),
                    0
                );
            }
            assert_eq!(
                staged.generation().expect("unchanged generation"),
                generation
            );
            assert_eq!(
                staged
                    .recall(&scope, "deletion", 8)
                    .expect("source remains recallable"),
                original_rows
            );
        }
        let deleted = staged.control(&call, None).expect("valid deletion");
        assert_eq!(
            deleted.response["postcondition"]["verification_query_digest"],
            "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
        );
        assert_eq!(deleted.response["postcondition"]["removed_effects"], 1);
        assert!(
            staged
                .recall(&scope, "deletion", 8)
                .expect("deleted recall")
                .is_empty()
        );
        drop(staged);
        let reopened = store(&root, 8);
        let duplicate = reopened.control(&call, None).expect("deletion redelivery");
        assert!(duplicate.duplicate);
        assert_eq!(duplicate.response, deleted.response);
        assert_eq!(duplicate.receipt, deleted.receipt);
        assert!(
            reopened
                .recall(&scope, "deletion", 8)
                .expect("reopened deletion")
                .is_empty()
        );
    }

    #[test]
    fn open_places_the_store_under_the_native_provider_state_namespace() {
        let root = TempDir::new().expect("temp root");
        let store = store(&root, 8);
        assert_eq!(
            store.path(),
            root.path()
                .join("native")
                .join("staged-observations-v1.sqlite3")
        );
        let connection = store.connection().expect("connection");
        let version: i64 = connection
            .query_row("PRAGMA user_version", [], |row| row.get(0))
            .expect("user_version");
        assert_eq!(version, SCHEMA_VERSION);
        let mode: String = connection
            .query_row("PRAGMA journal_mode", [], |row| row.get(0))
            .expect("journal_mode");
        assert_eq!(mode.to_lowercase(), "wal");
    }

    #[test]
    fn redelivered_key_returns_byte_identical_committed_evidence() {
        let root = TempDir::new().expect("temp root");
        let store = store(&root, 8);
        let scope = scope("session.alpha");

        let first = store
            .stage_or_duplicate(record(&scope, "key.one", "event.one", 1, "first message"))
            .expect("stage");
        let StagedOutcome::Committed(committed) = first else {
            panic!("expected a committed first delivery, got {first:?}");
        };

        let second = store
            .stage_or_duplicate(record(&scope, "key.one", "event.one", 1, "first message"))
            .expect("redeliver");
        let StagedOutcome::Duplicate(duplicate) = second else {
            panic!("expected a duplicate redelivery, got {second:?}");
        };

        assert_eq!(duplicate, committed);
        assert!(
            duplicate
                .provider_reference
                .starts_with(PROVIDER_REFERENCE_PREFIX)
        );
        assert_eq!(duplicate.receipt.len(), 64);
        assert_eq!(duplicate.effect_digest.len(), 64);
        assert_ne!(duplicate.receipt, duplicate.effect_digest);
        assert_eq!(row_count(&store), 1);
    }

    #[test]
    fn same_key_with_a_different_payload_conflicts_instead_of_deduplicating() {
        let root = TempDir::new().expect("temp root");
        let store = store(&root, 8);
        let scope = scope("session.alpha");

        store
            .stage_or_duplicate(record(&scope, "key.one", "event.one", 1, "original text"))
            .expect("stage");
        let outcome = store
            .stage_or_duplicate(record(&scope, "key.one", "event.one", 1, "rewritten text"))
            .expect("second attempt");

        match outcome {
            StagedOutcome::Conflict {
                reason:
                    StagedConflictReason::PayloadDiverged {
                        stored_payload_sha256,
                        offered_payload_sha256,
                    },
            } => assert_ne!(stored_payload_sha256, offered_payload_sha256),
            other => panic!("expected a payload conflict, got {other:?}"),
        }
        assert_eq!(row_count(&store), 1);
    }

    #[test]
    fn same_key_with_a_different_kind_conflicts() {
        let root = TempDir::new().expect("temp root");
        let store = store(&root, 8);
        let scope = scope("session.alpha");

        store
            .stage_or_duplicate(record(&scope, "key.one", "event.one", 1, "original text"))
            .expect("stage");
        let mut divergent = record(&scope, "key.one", "event.one", 1, "original text");
        divergent.observation_kind = "tool.execution_settled.v1".to_owned();
        let outcome = store.stage_or_duplicate(divergent).expect("second attempt");

        assert!(
            matches!(
                outcome,
                StagedOutcome::Conflict {
                    reason: StagedConflictReason::KindDiverged { .. }
                }
            ),
            "expected a kind conflict, got {outcome:?}"
        );
        assert_eq!(row_count(&store), 1);
    }

    #[test]
    fn source_identity_blocks_a_second_row_under_a_fresh_idempotency_key() {
        let root = TempDir::new().expect("temp root");
        let store = store(&root, 8);
        let scope = scope("session.alpha");

        store
            .stage_or_duplicate(record(&scope, "key.rev1", "event.one", 1, "replayed text"))
            .expect("stage");
        // A registration-revision change re-derives a fresh key over the same
        // settled source event; only the secondary index can catch it.
        let outcome = store
            .stage_or_duplicate(record(&scope, "key.rev2", "event.one", 1, "replayed text"))
            .expect("replay");

        match outcome {
            StagedOutcome::Conflict {
                reason:
                    StagedConflictReason::SourceIdentityReused {
                        stored_idempotency_key,
                    },
            } => assert_eq!(stored_idempotency_key, "key.rev1"),
            other => panic!("expected a source-identity conflict, got {other:?}"),
        }
        assert_eq!(row_count(&store), 1);
    }

    #[test]
    fn evicted_content_leaves_a_tombstone_that_still_answers_duplicate() {
        let root = TempDir::new().expect("temp root");
        let store = store(&root, 1);
        let scope = scope("session.alpha");

        let first = store
            .stage_or_duplicate(record(&scope, "key.one", "event.one", 1, "oldest message"))
            .expect("stage first");
        let StagedOutcome::Committed(original) = first else {
            panic!("expected a committed first delivery, got {first:?}");
        };
        store
            .stage_or_duplicate(record(&scope, "key.two", "event.two", 1, "newest message"))
            .expect("stage second");

        // The cap is one content-bearing row per scope, so the oldest lost its
        // payload and kept its identity.
        let (tombstone, payload_present): (i64, bool) = {
            let connection = store.connection().expect("connection");
            connection
                .query_row(
                    "SELECT tombstone, sanitized_payload IS NOT NULL \
                     FROM tdmem_native_staged_observation_v1 WHERE idempotency_key = ?1",
                    params!["key.one"],
                    |row| Ok((row.get(0)?, row.get(1)?)),
                )
                .expect("evicted row")
        };
        assert_eq!(tombstone, 1);
        assert!(!payload_present, "evicted row still carries its payload");

        let redelivered = store
            .stage_or_duplicate(record(&scope, "key.one", "event.one", 1, "oldest message"))
            .expect("redeliver evicted key");
        match redelivered {
            StagedOutcome::Duplicate(evidence) => assert_eq!(evidence, original),
            other => panic!("expected the tombstone to answer duplicate, got {other:?}"),
        }
        assert_eq!(row_count(&store), 2);
        let mut next_session = scope.clone();
        next_session.agent_session_id = "session.beta".to_owned();
        let recalled = store
            .recall(&next_session, "oldest message", 8)
            .expect("cross-session recall");
        assert_eq!(recalled.len(), 1);
        assert_eq!(recalled[0].idempotency_key, "key.two");
    }

    #[test]
    fn a_fault_between_insert_and_commit_leaves_no_row_and_no_evidence() {
        let root = TempDir::new().expect("temp root");
        let store = store(&root, 8);
        let scope = scope("session.alpha");

        store.fail_next_commit();
        let error = store
            .stage_or_duplicate(record(&scope, "key.one", "event.one", 1, "lost message"))
            .expect_err("injected commit fault");
        assert!(
            matches!(error, StagedStoreError::InjectedCommitFault),
            "unexpected error {error:?}"
        );
        assert_eq!(row_count(&store), 0);
        assert!(
            store
                .recall(&scope, "lost message", 8)
                .expect("recall")
                .is_empty()
        );

        // The delivery is redeliverable: the retry stages cleanly.
        let retried = store
            .stage_or_duplicate(record(&scope, "key.one", "event.one", 1, "lost message"))
            .expect("retry");
        assert!(
            matches!(retried, StagedOutcome::Committed(_)),
            "retry did not commit: {retried:?}"
        );
        assert_eq!(row_count(&store), 1);
    }

    #[test]
    fn recall_excludes_other_scopes_and_tombstones_and_is_deterministic() {
        let root = TempDir::new().expect("temp root");
        let store = store(&root, 2);
        let alpha = scope("session.alpha");
        let mut beta = scope("session.beta");
        beta.worktree_identity = "worktree.other".to_owned();

        store
            .stage_or_duplicate(record(
                &alpha,
                "key.a1",
                "event.a1",
                1,
                "alpha rustfmt lint",
            ))
            .expect("stage a1");
        store
            .stage_or_duplicate(record(
                &alpha,
                "key.a2",
                "event.a2",
                1,
                "alpha sqlite schema",
            ))
            .expect("stage a2");
        store
            .stage_or_duplicate(record(
                &alpha,
                "key.a3",
                "event.a3",
                1,
                "alpha unrelated notes",
            ))
            .expect("stage a3");
        store
            .stage_or_duplicate(record(&beta, "key.b1", "event.b1", 1, "beta sqlite schema"))
            .expect("stage b1");

        let hits = store.recall(&alpha, "sqlite schema", 8).expect("recall");
        let keys: Vec<&str> = hits
            .iter()
            .map(|row| row.idempotency_key.as_str())
            .collect();
        // a1 was evicted by the cap of two, and b1 belongs to another worktree.
        assert_eq!(keys, vec!["key.a2", "key.a3"]);
        assert!(hits.iter().all(|row| row.scope == alpha));
        assert_eq!(hits[0].message_text, "alpha sqlite schema");
        assert!(hits[0].score > hits[1].score);

        let repeated = store.recall(&alpha, "sqlite schema", 8).expect("recall");
        assert_eq!(hits, repeated);

        let beta_hits = store
            .recall(&beta, "sqlite schema", 8)
            .expect("recall beta");
        assert_eq!(beta_hits.len(), 1);
        assert_eq!(beta_hits[0].idempotency_key, "key.b1");

        let limited = store.recall(&alpha, "sqlite schema", 1).expect("limited");
        assert_eq!(limited.len(), 1);
        assert_eq!(limited[0].idempotency_key, "key.a2");
    }

    #[test]
    fn an_empty_query_orders_staged_rows_newest_first() {
        let root = TempDir::new().expect("temp root");
        let store = store(&root, 8);
        let alpha = scope("session.alpha");

        store
            .stage_or_duplicate(record(&alpha, "key.a1", "event.a1", 1, "older"))
            .expect("stage a1");
        store
            .stage_or_duplicate(record(&alpha, "key.a2", "event.a2", 1, "newer"))
            .expect("stage a2");

        let hits = store.recall(&alpha, "", 8).expect("recall");
        let keys: Vec<&str> = hits
            .iter()
            .map(|row| row.idempotency_key.as_str())
            .collect();
        assert_eq!(keys, vec!["key.a2", "key.a1"]);
    }

    #[test]
    fn extract_message_text_reads_the_contract_shaped_payload_only() {
        let text = extract_message_text(&envelope("  staged session text  "))
            .expect("contract-shaped payload");
        assert_eq!(text, "staged session text");

        let mut wrong_contract: Value =
            serde_json::from_slice(&envelope("staged")).expect("envelope");
        wrong_contract["payload_contract"] = json!("tracedecay.memory.observation.diagnostic.v1");
        assert_eq!(
            extract_message_text(&serde_json::to_vec(&wrong_contract).expect("bytes")),
            None
        );

        let mut wrong_kind: Value = serde_json::from_slice(&envelope("staged")).expect("envelope");
        wrong_kind["observation_kind"] = json!("diagnostic.observed.v1");
        assert_eq!(
            extract_message_text(&serde_json::to_vec(&wrong_kind).expect("bytes")),
            None
        );

        let blocks = json!({
            "canonical_payload": {
                "facts": [
                    {"kind": "session", "title": "not a message"},
                    {
                        "kind": "message",
                        "role": "user",
                        "content": [
                            {"type": "text", "text": "first block"},
                            {"type": "text", "text": "second block"},
                        ],
                    },
                ],
            },
            "observation_kind": STAGED_SESSION_OBSERVATION_KIND,
            "payload_contract": STAGED_SESSION_PAYLOAD_CONTRACT,
        });
        assert_eq!(
            extract_message_text(&serde_json::to_vec(&blocks).expect("bytes")),
            Some("first block\nsecond block".to_owned())
        );

        assert_eq!(extract_message_text(b"not json"), None);
    }

    #[test]
    fn a_row_whose_scope_columns_do_not_re_derive_its_digest_fails_recall_closed() {
        let root = TempDir::new().expect("temp root");
        let store = store(&root, 8);
        let alpha = scope("session.alpha");
        store
            .stage_or_duplicate(record(&alpha, "key.a1", "event.a1", 1, "alpha text"))
            .expect("stage");

        {
            let connection = store.connection().expect("connection");
            connection
                .execute(
                    "UPDATE tdmem_native_staged_observation_v1 SET agent_session_id = ?1",
                    params!["session.tampered"],
                )
                .expect("tamper");
        }

        let error = store
            .recall(&alpha, "alpha", 8)
            .expect_err("tampered scope must fail closed");
        assert!(
            matches!(error, StagedStoreError::ScopeDigestMismatch { .. }),
            "unexpected error {error:?}"
        );
    }

    #[test]
    fn corrupt_payload_fails_recall_closed_after_reopen_without_changing_other_rows() {
        let root = TempDir::new().expect("temp root");
        let alpha = scope("session.alpha");
        let mut beta = scope("session.beta");
        beta.worktree_identity = "worktree.other".to_owned();
        let original = record(&alpha, "key.original", "event.original", 1, "original text");
        let intact = record(&beta, "key.intact", "event.intact", 1, "intact text");
        let mut staged = store(&root, 8);
        let StagedOutcome::Committed(original_evidence) = staged
            .stage_or_duplicate(original.clone())
            .expect("stage original")
        else {
            panic!("expected original commit");
        };
        let StagedOutcome::Committed(intact_evidence) = staged
            .stage_or_duplicate(intact.clone())
            .expect("stage intact")
        else {
            panic!("expected intact commit");
        };
        let original_rows = staged.recall(&alpha, "text", 8).expect("original recall");
        assert_eq!(original_rows.len(), 1);
        assert_eq!(original_rows[0].message_text, "original text");
        assert_eq!(
            original_rows[0].provider_reference,
            original_evidence.provider_reference
        );
        assert_eq!(original_rows[0].receipt, original_evidence.receipt);
        let intact_rows = staged.recall(&beta, "text", 8).expect("intact recall");
        assert_eq!(intact_rows.len(), 1);
        assert_eq!(intact_rows[0].message_text, "intact text");
        assert_eq!(
            intact_rows[0].provider_reference,
            intact_evidence.provider_reference
        );
        assert_eq!(intact_rows[0].receipt, intact_evidence.receipt);

        // Both valid message bytes and unparseable bytes must fail the digest
        // check, without replacing the stored claim or reporting empty success.
        for corrupt_payload in [envelope("forged text"), b"not json".to_vec()] {
            assert_ne!(
                sha256_hex(&corrupt_payload),
                original_rows[0].payload_sha256
            );
            {
                let connection = staged.connection().expect("connection");
                assert_eq!(
                    connection
                        .execute(
                            "UPDATE tdmem_native_staged_observation_v1 \
                             SET sanitized_payload = ?1 \
                             WHERE exact_scope_sha256 = ?2 AND idempotency_key = ?3",
                            params![
                                corrupt_payload,
                                alpha.exact_scope_sha256(),
                                original.idempotency_key,
                            ],
                        )
                        .expect("corrupt payload without changing its digest"),
                    1
                );
            }
            let error = staged
                .recall(&alpha, "text", 8)
                .expect_err("corrupt bytes must fail closed");
            assert!(matches!(
                error,
                StagedStoreError::PayloadDigestMismatch { .. }
            ));
            drop(staged);
            staged = store(&root, 8);
            let error = staged
                .recall(&alpha, "text", 8)
                .expect_err("reopen must not admit corrupt bytes");
            let StagedStoreError::PayloadDigestMismatch {
                idempotency_key,
                stored_payload_sha256,
            } = error
            else {
                panic!("unexpected error {error:?}");
            };
            assert_eq!(idempotency_key, original.idempotency_key);
            assert_eq!(stored_payload_sha256, original_rows[0].payload_sha256);
            assert_eq!(
                staged.recall(&beta, "text", 8).expect("unaffected recall"),
                intact_rows
            );
            assert_eq!(
                staged
                    .stage_or_duplicate(original.clone())
                    .expect("original duplicate"),
                StagedOutcome::Duplicate(original_evidence.clone())
            );
            assert_eq!(
                staged
                    .stage_or_duplicate(intact.clone())
                    .expect("intact duplicate"),
                StagedOutcome::Duplicate(intact_evidence.clone())
            );
            assert_eq!(row_count(&staged), 2);
        }
    }

    #[test]
    fn checkout_recall_survives_reopen_and_keeps_the_exact_origin() {
        let root = TempDir::new().expect("root");
        let alpha = scope("session.alpha");
        let mut beta = scope("session.beta");
        beta.resolved_scope_digest = format!("sha256:{}", "b".repeat(64));
        let evidence = {
            let store = store(&root, 8);
            store
                .stage_or_duplicate(record(
                    &alpha,
                    "key.origin",
                    "event.origin",
                    1,
                    "checkout message",
                ))
                .expect("stage")
        };
        let reopened = store(&root, 8);
        let rows = reopened
            .recall(&beta, "checkout", 8)
            .expect("other-session recall");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].scope, alpha);
        assert_eq!(rows[0].exact_scope_sha256, alpha.exact_scope_sha256());
        assert_ne!(rows[0].exact_scope_sha256, beta.exact_scope_sha256());
        let StagedOutcome::Committed(evidence) = evidence else {
            panic!("first commit");
        };
        assert_eq!(rows[0].provider_reference, evidence.provider_reference);
        assert_eq!(rows[0].receipt, evidence.receipt);
        for field in 0..5 {
            let mut foreign = beta.clone();
            match field {
                0 => foreign.profile_id = "profile.other".to_owned(),
                1 => foreign.project_id = "project.other".to_owned(),
                2 => foreign.repository_identity = "repository.other".to_owned(),
                3 => foreign.worktree_identity = "worktree.other".to_owned(),
                _ => foreign.branch_identity = "refs/heads/other".to_owned(),
            }
            assert!(
                reopened
                    .recall(&foreign, "checkout", 8)
                    .expect("foreign recall")
                    .is_empty(),
                "checkout field {field}"
            );
        }
    }

    #[test]
    fn checkout_recall_bounds_the_newest_cross_session_rows_before_scoring() {
        let root = TempDir::new().expect("root");
        let store = store(&root, 3);
        for index in 0..6 {
            let origin = scope(&format!("session.{index}"));
            let text = if index < 3 {
                "needle needle matching ancient message"
            } else {
                "recent message"
            };
            store
                .stage_or_duplicate(record(
                    &origin,
                    &format!("key.{index}"),
                    &format!("event.{index}"),
                    1,
                    text,
                ))
                .expect("stage");
        }
        // Per-exact-scope retention does not evict one-row sessions. The recall
        // query must impose its own checkout-wide bound before lexical scoring.
        assert_eq!(row_count(&store), 6);
        let request = scope("session.request");
        let hits = store
            .recall(&request, "needle", usize::MAX)
            .expect("bounded recall");
        assert_eq!(
            hits.iter()
                .map(|r| r.idempotency_key.as_str())
                .collect::<Vec<_>>(),
            vec!["key.5", "key.4", "key.3"]
        );
        assert_eq!(
            store
                .recall(&request, "needle", usize::MAX)
                .expect("repeat"),
            hits
        );
        assert_eq!(
            store.recall(&request, "needle", 1).expect("limit")[0],
            hits[0]
        );
    }

    #[test]
    fn a_request_digest_cannot_replace_the_stored_origin_digest() {
        let root = TempDir::new().expect("root");
        let store = store(&root, 8);
        let alpha = scope("session.alpha");
        let beta = scope("session.beta");
        store
            .stage_or_duplicate(record(
                &alpha,
                "key.origin",
                "event.origin",
                1,
                "origin content",
            ))
            .expect("stage");
        store
            .connection()
            .expect("connection")
            .execute(
                "UPDATE tdmem_native_staged_observation_v1 SET exact_scope_sha256 = ?1",
                params![beta.exact_scope_sha256()],
            )
            .expect("corrupt stored origin digest");
        assert!(matches!(
            store.recall(&beta, "origin", 8),
            Err(StagedStoreError::ScopeDigestMismatch { .. })
        ));
    }

    fn downgrade_fixture_to_real_v1(staged: StagedObservationStore) -> PathBuf {
        let path = staged.path().to_path_buf();
        drop(staged);
        let connection = Connection::open(&path).expect("open fixture");
        connection
            .execute_batch(
                "ALTER TABLE tdmem_native_staged_observation_v1 RENAME TO fixture_v2_copy;",
            )
            .expect("retain fixture");
        connection
            .execute_batch(SCHEMA_DDL)
            .expect("real v1 schema");
        let columns = SNAPSHOT_COLUMNS[..24].join(",");
        connection.execute(&format!("INSERT INTO tdmem_native_staged_observation_v1 ({columns}) SELECT {columns} FROM fixture_v2_copy"),[]).expect("legacy rows");
        connection.execute_batch("DROP TABLE fixture_v2_copy; DROP TABLE tdmem_native_state_v2; DROP TABLE tdmem_native_operation_v2;
            DROP TABLE tdmem_native_deleted_source_v2; DROP TABLE tdmem_native_replay_v2;
            UPDATE tdmem_native_staged_observation_v1 SET source_revision=99; PRAGMA user_version=1;").expect("v1 checkpoint");
        path
    }

    #[test]
    fn v1_migration_preserves_receipts_and_does_not_invent_revision_origin_or_validity() {
        let root = TempDir::new().expect("root");
        let source = scope("session.legacy");
        let staged = store(&root, 8);
        let offered = record(
            &source,
            "legacy.key",
            "legacy.event",
            99,
            "legacy canonical evidence",
        );
        let outcome = staged
            .stage_or_duplicate(offered.clone())
            .expect("stage legacy bytes");
        let StagedOutcome::Committed(evidence) = outcome else {
            panic!("committed fixture");
        };
        downgrade_fixture_to_real_v1(staged);
        let migrated = store(&root, 8);
        let rows = migrated
            .recall(&source, "legacy", 8)
            .expect("legacy recall");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].source_revision, None);
        assert_eq!(rows[0].original_source, None);
        assert_eq!(
            rows[0].validity,
            tracedecay_memory_provider_registry::RecordedValidity::default()
        );
        assert_eq!(rows[0].receipt, evidence.receipt);
        assert_eq!(rows[0].provider_reference, evidence.provider_reference);
        assert_eq!(
            rows[0].payload_sha256,
            sha256_hex(&offered.sanitized_payload)
        );
        assert_eq!(
            migrated.stage_or_duplicate(offered).expect("redelivery"),
            StagedOutcome::Duplicate(evidence)
        );
    }

    #[test]
    fn failed_v1_migration_rolls_back_columns_version_and_retained_rows() {
        let root = TempDir::new().expect("root");
        let staged = store(&root, 8);
        staged
            .stage_or_duplicate(record(
                &scope("session.legacy"),
                "legacy.key",
                "legacy.event",
                99,
                "intact",
            ))
            .expect("stage");
        let path = downgrade_fixture_to_real_v1(staged);
        let connection = Connection::open(&path).expect("open");
        connection
            .execute(
                "UPDATE tdmem_native_staged_observation_v1 SET sanitized_payload=?1",
                params![b"not-json".as_slice()],
            )
            .expect("legacy unsupported bytes");
        let before: (String, String) = connection
            .query_row(
                "SELECT receipt,payload_sha256 FROM tdmem_native_staged_observation_v1",
                [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .expect("before");
        drop(connection);
        assert!(StagedObservationStore::open(root.path()).is_err());
        let connection = Connection::open(&path).expect("reopen v1");
        assert_eq!(
            connection
                .query_row("PRAGMA user_version", [], |row| row.get::<_, i64>(0))
                .expect("version"),
            1
        );
        assert_eq!(connection.query_row("SELECT COUNT(*) FROM pragma_table_info('tdmem_native_staged_observation_v1') WHERE name='actual_revision'",[],|row|row.get::<_,i64>(0)).expect("column"),0);
        let after: (String, String) = connection
            .query_row(
                "SELECT receipt,payload_sha256 FROM tdmem_native_staged_observation_v1",
                [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .expect("after");
        assert_eq!(before, after);
        assert_eq!(
            connection
                .query_row(
                    "SELECT sanitized_payload FROM tdmem_native_staged_observation_v1",
                    [],
                    |row| row.get::<_, Vec<u8>>(0)
                )
                .expect("retained bytes"),
            b"not-json"
        );
    }

    #[test]
    fn common_exclusions_and_empty_effective_intervals_do_not_consume_the_candidate_window() {
        use tracedecay_memory_provider_registry::{
            CommonUnknownValidityPolicy, OwnedRecallExclusions, OwnedTemporalQuery, TemporalMode,
        };
        let root = TempDir::new().expect("root");
        let staged = store(&root, 2);
        let mut sources = Vec::new();
        let mut exclusions = OwnedRecallExclusions::default();
        let time = |value| super::super::native_provider::parse_rfc3339_nanos(value).expect("time");
        let start = "2025-01-01T00:00:00.000000001Z";
        let end = "2025-01-02T00:00:00.000000002Z";
        for index in 0..5 {
            let origin = scope(&format!("session.{index}"));
            let mut observation = tracedecay_memory_conformance::compatibility::common_observation(
                &origin,
                index + 1,
                Some("r1"),
                "compatibility beacon",
                Some(start),
                None,
            );
            if index == 1 || index == 4 {
                observation["source_identity"]["original_source"]["validity"]["revoked_at"] =
                    start.into();
            }
            if index == 2 || index == 3 {
                exclusions.source_refs.push(
                    observation["source_identity"]["original_source"]["source"]["source_key"]
                        .as_str()
                        .expect("source")
                        .into(),
                );
            }
            sources.push(json!({"attribution":observation["source_identity"]["original_source"],"current_disposition":{"state":"available"}}));
            let mut offered = record(
                &origin,
                &format!("key.{index}"),
                &format!("event.{index}"),
                1,
                "unused",
            );
            offered.source_revision = Some("r1".into());
            offered.sanitized_payload = serde_json::to_vec(&observation).expect("bytes");
            staged.stage_or_duplicate(offered).expect("staged");
        }
        let temporal = OwnedTemporalQuery {
            mode: TemporalMode::Interval,
            evaluation_time_utc_nanos: time(end),
            as_of_utc_nanos: None,
            interval_start_utc_nanos: Some(time(start)),
            interval_end_utc_nanos: Some(time(end)),
            include_superseded: false,
            include_revoked: false,
            unknown_validity_policy: CommonUnknownValidityPolicy::Exclude,
        };
        let grant = json!({"sources":sources});
        let rows = staged
            .recall_temporal(
                &scope("session.reader"),
                "compatibility beacon",
                &temporal,
                Some(&grant),
                &exclusions,
                "request.dense",
            )
            .expect("bounded recall");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].idempotency_key, "key.0");
    }
}

// Additive v2 state leaves the original row identities and receipt evidence intact.
const ADVISORY_DDL: &str = "
CREATE TABLE IF NOT EXISTS tdmem_native_state_v2 (
 singleton INTEGER PRIMARY KEY CHECK(singleton = 1), generation INTEGER NOT NULL CHECK(generation >= 0)
) STRICT;
INSERT OR IGNORE INTO tdmem_native_state_v2 VALUES(1, 0);
CREATE TABLE IF NOT EXISTS tdmem_native_operation_v2 (
 scope TEXT NOT NULL, idempotency_key TEXT NOT NULL, request_digest TEXT NOT NULL,
 operation_id TEXT NOT NULL, generation_before INTEGER NOT NULL, generation_after INTEGER NOT NULL,
 response TEXT NOT NULL, receipt TEXT NOT NULL, PRIMARY KEY(scope, idempotency_key)
) STRICT;
CREATE TABLE IF NOT EXISTS tdmem_native_deleted_source_v2 (
 checkout TEXT NOT NULL, source_key TEXT NOT NULL, generation INTEGER NOT NULL,
 PRIMARY KEY(checkout, source_key)
) STRICT;
CREATE TABLE IF NOT EXISTS tdmem_native_replay_v2 (
 scope TEXT PRIMARY KEY, acknowledged_sequence INTEGER NOT NULL
) STRICT;";

/// SQLite stores signed integers; reject negative counters at the read boundary.
fn sqlite_u64(row: &rusqlite::Row<'_>, index: usize) -> rusqlite::Result<u64> {
    let value = row.get::<_, i64>(index)?;
    u64::try_from(value).map_err(|error| {
        rusqlite::Error::FromSqlConversionFailure(
            index,
            rusqlite::types::Type::Integer,
            Box::new(error),
        )
    })
}

/// Preserve the full unsigned value or reject it before writing SQLite state.
fn sqlite_i64(value: u64, field: &'static str) -> Result<i64, StagedStoreError> {
    i64::try_from(value).map_err(|_| StagedStoreError::ValueOutOfRange { field })
}

fn generation_in(connection: &Connection) -> Result<i64, StagedStoreError> {
    Ok(connection.query_row(
        "SELECT generation FROM tdmem_native_state_v2 WHERE singleton = 1",
        [],
        |row| row.get(0),
    )?)
}

fn checkout_digest(scope: &ExactScopeFields) -> String {
    canonical_framed_sha256(
        b"tracedecay.native.checkout.v2",
        &[
            scope.profile_id.as_bytes(),
            scope.project_id.as_bytes(),
            scope.repository_identity.as_bytes(),
            scope.worktree_identity.as_bytes(),
            scope.branch_identity.as_bytes(),
        ],
    )
}

// A separate checkout domain keeps a typed digest disjoint from every legacy
// raw source key, including a raw key whose bytes happen to equal the digest.
fn source_fence_checkout_digest(scope: &ExactScopeFields) -> String {
    canonical_framed_sha256(
        b"tracedecay.native.canonical-source-fence-checkout.v1",
        &[checkout_digest(scope).as_bytes()],
    )
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct NativeSourceTarget {
    canonical_provider_id: String,
    canonical_session_id: String,
    source_key: String,
}

impl NativeSourceTarget {
    fn from_source(source: &Value) -> Result<Self, StagedStoreError> {
        Ok(Self {
            canonical_provider_id: required_text(source, "canonical_provider_id")?.to_owned(),
            canonical_session_id: required_text(source, "canonical_session_id")?.to_owned(),
            source_key: required_text(source, "source_key")?.to_owned(),
        })
    }

    fn digest(&self) -> String {
        canonical_framed_sha256(
            b"tracedecay.native.canonical-source-fence.v1",
            &[
                self.canonical_provider_id.as_bytes(),
                self.canonical_session_id.as_bytes(),
                self.source_key.as_bytes(),
            ],
        )
    }
}

fn recorded_origin_matches_checkout(attribution: &Value, scope: &ExactScopeFields) -> bool {
    attribution
        .pointer("/origin_scope/state")
        .and_then(Value::as_str)
        == Some("recorded")
        && attribution
            .pointer("/origin_scope/exact_scope_identity/profile_id")
            .and_then(Value::as_str)
            == Some(scope.profile_id.as_str())
        && attribution
            .pointer("/origin_scope/exact_scope_identity/project_id")
            .and_then(Value::as_str)
            == Some(scope.project_id.as_str())
}

fn legacy_source_deleted(
    connection: &Connection,
    scope: &ExactScopeFields,
    source_key: &str,
) -> Result<bool, StagedStoreError> {
    Ok(connection.query_row("SELECT EXISTS(SELECT 1 FROM tdmem_native_deleted_source_v2 WHERE checkout = ?1 AND source_key = ?2)",
        params![checkout_digest(scope), source_key], |row| row.get(0))?)
}

fn source_deleted(
    connection: &Connection,
    scope: &ExactScopeFields,
    source_key: &str,
    attribution: Option<&Value>,
) -> Result<bool, StagedStoreError> {
    let legacy = legacy_source_deleted(connection, scope, source_key)?;
    if legacy {
        return Ok(true);
    }
    let Some(attribution) = attribution else {
        return Ok(false);
    };
    // Recorded foreign origins never alias this checkout's original namespace.
    // Missing origin evidence cannot override an existing canonical-source fence.
    if attribution
        .pointer("/origin_scope/state")
        .and_then(Value::as_str)
        == Some("recorded")
        && !recorded_origin_matches_checkout(attribution, scope)
    {
        return Ok(false);
    }
    let target = NativeSourceTarget::from_source(&attribution["source"])?;
    Ok(connection.query_row("SELECT EXISTS(SELECT 1 FROM tdmem_native_deleted_source_v2 WHERE checkout = ?1 AND source_key = ?2)",
        params![source_fence_checkout_digest(scope), target.digest()], |row| row.get(0))?)
}

fn claimed_deletion_targets(
    call: &tracedecay_memory_provider_registry::ProviderCall,
    request: &Value,
    admission: Option<&tracedecay_memory_provider_registry::CurrentAdvisoryAdmission>,
) -> Result<Option<Vec<NativeSourceTarget>>, StagedStoreError> {
    use tracedecay_memory_provider_registry::{OriginScopeEvidence, ProviderOperation};
    if call.operation != ProviderOperation::DeleteBySource || call.history_grant().is_none() {
        return Ok(None);
    }
    let admission = admission.ok_or(StagedStoreError::LifecycleConflict(
        "deletion authority unavailable",
    ))?;
    admission
        .verify_for(call)
        .map_err(|_| StagedStoreError::LifecycleConflict("deletion authority binding"))?;
    let keys = request
        .get("forget_source_keys")
        .and_then(Value::as_array)
        .filter(|keys| !keys.is_empty() && keys.len() <= 1024)
        .ok_or(StagedStoreError::InvalidAdvisory("forget source keys"))?;
    let mut targets = BTreeSet::new();
    let mut unique = BTreeSet::new();
    for key in keys {
        let key = key
            .as_str()
            .filter(|key| !key.is_empty() && key.len() <= 1024)
            .ok_or(StagedStoreError::InvalidAdvisory("source key"))?;
        if !unique.insert(key) {
            return Err(StagedStoreError::InvalidAdvisory("duplicate source key"));
        }
        let mut matching = BTreeSet::new();
        for trusted in &admission.history_sources {
            if trusted.attribution.source.source_key != key {
                continue;
            }
            // The mounted authority guarantees this namespace; repeat the check
            // here because custom authorities implement the same public trait.
            let OriginScopeEvidence::Recorded { scope, .. } = &trusted.attribution.origin_scope
            else {
                return Err(StagedStoreError::LifecycleConflict(
                    "deletion origin unavailable",
                ));
            };
            if scope.profile_id != call.exact_scope.profile_id
                || scope.project_id != call.exact_scope.project_id
            {
                return Err(StagedStoreError::LifecycleConflict(
                    "deletion origin namespace",
                ));
            }
            matching.insert(NativeSourceTarget {
                canonical_provider_id: trusted
                    .attribution
                    .source
                    .canonical_provider_id
                    .as_str()
                    .to_owned(),
                canonical_session_id: trusted.attribution.source.canonical_session_id.clone(),
                source_key: trusted.attribution.source.source_key.clone(),
            });
        }
        if matching.len() != 1 {
            return Err(StagedStoreError::LifecycleConflict(
                "deletion source ambiguous or unavailable",
            ));
        }
        targets.extend(matching);
    }
    Ok(Some(targets.into_iter().collect()))
}

pub(crate) fn recorded_validity(
    attribution: Option<&Value>,
) -> Result<tracedecay_memory_provider_registry::RecordedValidity, StagedStoreError> {
    use tracedecay_memory_provider_registry::RecordedValidity;
    let Some(attribution) = attribution else {
        return Ok(RecordedValidity::default());
    };
    let validity = attribution
        .get("validity")
        .ok_or(StagedStoreError::InvalidAdvisory("validity"))?;
    let timestamp = |field| -> Result<Option<i64>, StagedStoreError> {
        match validity.get(field) {
            Some(Value::Null) => Ok(None),
            Some(Value::String(text)) => super::native_provider::parse_rfc3339_nanos(text)
                .map(Some)
                .ok_or(StagedStoreError::InvalidAdvisory("validity timestamp")),
            _ => Err(StagedStoreError::InvalidAdvisory("validity timestamp")),
        }
    };
    let validity = RecordedValidity {
        valid_from_utc_nanos: timestamp("valid_from")?,
        valid_until_utc_nanos: timestamp("valid_until")?,
        superseded_at_utc_nanos: timestamp("superseded_at")?,
        superseded_by: match validity.get("superseded_by") {
            Some(Value::Null) => None,
            Some(Value::String(text)) if !text.trim().is_empty() => Some(text.clone()),
            _ => return Err(StagedStoreError::InvalidAdvisory("superseded_by")),
        },
        revoked_at_utc_nanos: timestamp("revoked_at")?,
    };
    validity
        .validate()
        .map_err(|_| StagedStoreError::InvalidAdvisory("recorded validity"))?;
    Ok(validity)
}

fn validate_attribution(value: &Value) -> Result<(), StagedStoreError> {
    let source = value
        .get("source")
        .ok_or(StagedStoreError::InvalidAdvisory("original source"))?;
    for field in [
        "canonical_provider_id",
        "canonical_session_id",
        "source_key",
        "observation_id",
        "content_sha256",
    ] {
        let text = source
            .get(field)
            .and_then(Value::as_str)
            .ok_or(StagedStoreError::InvalidAdvisory("source identity"))?;
        if text.is_empty() || text.len() > 1024 || text.chars().any(char::is_control) {
            return Err(StagedStoreError::InvalidAdvisory("source identity"));
        }
    }
    for field in ["source_revision", "stable_record_id"] {
        match source.get(field) {
            Some(Value::Null) => {}
            Some(Value::String(text)) if !text.trim().is_empty() && text.len() <= 1024 => {}
            _ => {
                return Err(StagedStoreError::InvalidAdvisory(
                    "nullable source identity",
                ));
            }
        }
    }
    let digest = source["content_sha256"].as_str().unwrap_or_default();
    if digest.len() != 64
        || !digest
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return Err(StagedStoreError::InvalidAdvisory("source digest"));
    }
    match value.pointer("/origin_scope/state").and_then(Value::as_str) {
        Some("recorded") => {
            let scope = value
                .pointer("/origin_scope/exact_scope_identity")
                .ok_or(StagedStoreError::InvalidAdvisory("origin scope"))?;
            exact_scope_from_value(scope)?
                .validate()
                .map_err(StagedStoreError::InvalidScope)?;
            if value
                .pointer("/origin_scope/authority_ref")
                .and_then(Value::as_str)
                .is_none_or(str::is_empty)
            {
                return Err(StagedStoreError::InvalidAdvisory("origin authority"));
            }
        }
        Some("unavailable" | "ingestion_only") => {}
        _ => return Err(StagedStoreError::InvalidAdvisory("origin scope")),
    }
    if value
        .get("source_sequence")
        .and_then(Value::as_u64)
        .is_none()
    {
        return Err(StagedStoreError::InvalidAdvisory("source sequence"));
    }
    for field in ["occurred_at", "ingested_at"] {
        match value.get(field) {
            Some(Value::Null) if field == "occurred_at" => {}
            Some(Value::String(text))
                if super::native_provider::parse_rfc3339_nanos(text).is_some() => {}
            _ => return Err(StagedStoreError::InvalidAdvisory("source time")),
        }
    }
    recorded_validity(Some(value))?;
    Ok(())
}

pub(crate) fn exact_scope_from_value(value: &Value) -> Result<ExactScopeFields, StagedStoreError> {
    let text = |field| {
        value
            .get(field)
            .and_then(Value::as_str)
            .map(str::to_owned)
            .ok_or(StagedStoreError::InvalidAdvisory("exact scope"))
    };
    let scope = ExactScopeFields {
        profile_id: text("profile_id")?,
        project_id: text("project_id")?,
        repository_identity: text("repository_identity")?,
        worktree_identity: text("worktree_identity")?,
        branch_identity: text("branch_identity")?,
        agent_session_id: text("agent_session_id")?,
        resolved_scope_digest: text("resolved_scope_digest")?,
    };
    scope.validate().map_err(StagedStoreError::InvalidScope)?;
    Ok(scope)
}

/// Durable result of a bounded provider-local lifecycle transaction.
#[derive(Clone, Debug)]
pub(crate) struct StagedControlOutcome {
    pub(crate) response: Value,
    pub(crate) generation_before: u64,
    pub(crate) generation_after: u64,
    pub(crate) receipt: String,
    pub(crate) operation_id: String,
    pub(crate) duplicate: bool,
    pub(crate) changed: bool,
}

impl StagedObservationStore {
    pub(crate) fn generation(&self) -> Result<u64, StagedStoreError> {
        let guard = self.connection()?;
        u64::try_from(generation_in(&guard)?)
            .map_err(|_| StagedStoreError::InvalidAdvisory("generation"))
    }

    /// Executes lifecycle work under one bounded actor-owned transaction.
    pub(crate) fn control(
        &self,
        call: &tracedecay_memory_provider_registry::ProviderCall,
        admission: Option<&tracedecay_memory_provider_registry::CurrentAdvisoryAdmission>,
    ) -> Result<StagedControlOutcome, StagedStoreError> {
        use tracedecay_memory_provider_registry::ProviderOperation;
        let mut request: Value = serde_json::from_slice(&call.payload.bytes)
            .map_err(|_| StagedStoreError::InvalidAdvisory("lifecycle request"))?;
        let scope = call.exact_scope.exact_scope_sha256();
        let mut guard = self.connection()?;
        let transaction = guard.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let before = u64::try_from(generation_in(&transaction)?)
            .map_err(|_| StagedStoreError::InvalidAdvisory("generation"))?;
        let read = matches!(
            call.operation,
            ProviderOperation::Inspection
                | ProviderOperation::Health
                | ProviderOperation::SnapshotExport
        ) || (call.operation == ProviderOperation::Maintenance
            && request["dry_run"] == true);
        let key = call.idempotency_key.as_deref().unwrap_or("");
        // Idempotency describes the semantic operation, not its attempt ID or live generation.
        let mut semantic = request.clone();
        if let Some(object) = semantic.as_object_mut() {
            object.remove("common_request");
        }
        let deletion_targets = claimed_deletion_targets(call, &request, admission)?;
        let legacy_digest = sha256_hex(format!("{:?}:{}", call.operation, semantic).as_bytes());
        let digest = if let Some(targets) = &deletion_targets {
            let mut target_digests = targets
                .iter()
                .map(NativeSourceTarget::digest)
                .collect::<Vec<_>>();
            target_digests.sort();
            let encoded = serde_json::to_vec(&target_digests)
                .map_err(|_| StagedStoreError::InvalidAdvisory("deletion targets"))?;
            canonical_framed_sha256(
                b"tracedecay.native.claimed-deletion.v1",
                &[legacy_digest.as_bytes(), &encoded],
            )
        } else {
            legacy_digest
        };
        if !read {
            if key.is_empty() {
                return Err(StagedStoreError::InvalidAdvisory("idempotency key"));
            }
            let prior: Option<(String, String, String, u64, u64, String)> = transaction.query_row(
                "SELECT request_digest, response, operation_id, generation_before, generation_after, receipt FROM tdmem_native_operation_v2 WHERE scope = ?1 AND idempotency_key = ?2",
                params![scope, key], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, sqlite_u64(row, 3)?, sqlite_u64(row, 4)?, row.get(5)?))).optional()?;
            if let Some((
                stored,
                response,
                operation_id,
                generation_before,
                generation_after,
                receipt,
            )) = prior
            {
                if stored != digest {
                    return Err(StagedStoreError::LifecycleConflict("idempotency conflict"));
                }
                return Ok(StagedControlOutcome {
                    response: serde_json::from_str(&response)
                        .map_err(|_| StagedStoreError::InvalidAdvisory("operation receipt"))?,
                    generation_before,
                    generation_after,
                    receipt,
                    operation_id,
                    duplicate: true,
                    changed: false,
                });
            }
            if before != call.expected_state_generation {
                return Err(StagedStoreError::LifecycleConflict("state generation"));
            }
        }
        if let Some(admission) = admission {
            super::native_provider::apply_current_admission(&mut request, admission);
        }
        let (mut response, changed) = match call.operation {
            ProviderOperation::Health => {
                let count: u64 = transaction.query_row("SELECT COUNT(*) FROM tdmem_native_staged_observation_v1 WHERE exact_scope_sha256 = ?1", params![scope], |row| sqlite_u64(row, 0))?;
                (
                    serde_json::json!({"readiness":"ready", "state_generation":before, "staged_rows":count, "recovery_state":"complete", "warnings":[]}),
                    false,
                )
            }
            ProviderOperation::Inspection => (
                inspect_advisory(&transaction, call, &request, before)?,
                false,
            ),
            ProviderOperation::Feedback => feedback_advisory(&transaction, call, &request)?,
            ProviderOperation::Correction => {
                correct_advisory(&transaction, call, &request, self.retention)?
            }
            ProviderOperation::DeleteBySource => delete_advisory(
                &transaction,
                call,
                &request,
                before.saturating_add(1),
                deletion_targets.as_deref(),
            )?,
            ProviderOperation::Maintenance => maintain_advisory(&transaction, call, &request)?,
            ProviderOperation::SnapshotExport => {
                (export_snapshot(&transaction, call, before)?, false)
            }
            ProviderOperation::SnapshotRestore => {
                restore_snapshot(&transaction, call, &request, before, admission)?
            }
            ProviderOperation::Replay => {
                replay_advisory(&transaction, call, &request, self.retention)?
            }
            _ => return Err(StagedStoreError::InvalidAdvisory("lifecycle operation")),
        };
        let transaction_generation = if call.operation == ProviderOperation::Health {
            before
        } else {
            u64::try_from(generation_in(&transaction)?)
                .map_err(|_| StagedStoreError::InvalidAdvisory("generation"))?
        };
        let after = if changed {
            before
                .max(transaction_generation)
                .checked_add(1)
                .ok_or(StagedStoreError::InvalidAdvisory("generation overflow"))?
        } else {
            before
        };
        let receipt = sha256_hex(format!("{scope}:{key}:{digest}:{before}:{after}").as_bytes());
        response["state_generation_before"] = before.into();
        response["state_generation_after"] = after.into();
        if call.operation == ProviderOperation::DeleteBySource {
            response["postcondition"]["state_generation_before"] = before.into();
            response["postcondition"]["state_generation_after"] = after.into();
        }
        response["provider_receipt_digest"] = receipt.clone().into();
        if !read && changed {
            transaction.execute(
                "UPDATE tdmem_native_state_v2 SET generation = ?1 WHERE singleton = 1",
                params![sqlite_i64(after, "generation_after")?],
            )?;
            transaction.execute(
                "INSERT INTO tdmem_native_operation_v2 VALUES(?1,?2,?3,?4,?5,?6,?7,?8)",
                params![
                    scope,
                    key,
                    digest,
                    call.operation_id,
                    sqlite_i64(before, "generation_before")?,
                    sqlite_i64(after, "generation_after")?,
                    response.to_string(),
                    receipt
                ],
            )?;
        }
        let mut outcome = StagedControlOutcome {
            response,
            generation_before: before,
            generation_after: after,
            receipt,
            operation_id: call.operation_id.clone(),
            duplicate: false,
            changed,
        };
        if call.operation == ProviderOperation::Inspection
            && request["view"] == "maintenance_receipt"
        {
            paginate_inspection_evidence(
                &mut outcome,
                call,
                &request,
                super::native_provider::native_provider_limits().inspection_items,
                super::native_provider::native_provider_limits().response_bytes,
            )?;
        }
        if super::native_provider::native_control_reply(
            call,
            outcome.clone(),
            after,
            super::native_provider::native_provider_limits().response_bytes,
        )
        .terminal
        .terminal_code()
            != tracedecay_memory_provider_registry::TerminalCode::Success
        {
            return Err(StagedStoreError::ValueOutOfRange {
                field: "lifecycle response bytes",
            });
        }
        #[cfg(test)]
        if !read
            && self
                .fail_next_commit
                .swap(false, std::sync::atomic::Ordering::SeqCst)
        {
            return Err(StagedStoreError::InjectedCommitFault);
        }
        call.control
            .snapshot()
            .map_err(StagedStoreError::ControlEnded)?;
        transaction
            .commit()
            .map_err(StagedStoreError::CommitUnknown)?;
        Ok(outcome)
    }
}

fn required_text<'a>(value: &'a Value, field: &'static str) -> Result<&'a str, StagedStoreError> {
    value
        .get(field)
        .and_then(Value::as_str)
        .filter(|text| !text.trim().is_empty() && text.len() <= 32768)
        .ok_or(StagedStoreError::InvalidAdvisory(field))
}

fn bounded_integer(
    value: &Value,
    field: &'static str,
    ceiling: u64,
) -> Result<u64, StagedStoreError> {
    value
        .get(field)
        .and_then(Value::as_u64)
        .filter(|number| *number > 0 && *number <= ceiling)
        .ok_or(StagedStoreError::InvalidAdvisory(field))
}

/// Resolves only retained provider targets with independently matching source and delivery scope.
fn resolve_target(
    connection: &Connection,
    call: &tracedecay_memory_provider_registry::ProviderCall,
    request: &Value,
) -> Result<(String, Option<Value>, f64, String), StagedStoreError> {
    let target = request
        .get("target")
        .ok_or(StagedStoreError::InvalidAdvisory("target"))?;
    if required_text(target, "provider_id")? != call.provider_id.as_str()
        || target.get("registration_revision").and_then(Value::as_u64)
            != Some(call.registration_revision)
        || exact_scope_from_value(&target["delivery_scope"])? != call.exact_scope
    {
        return Err(StagedStoreError::LifecycleConflict("target attribution"));
    }
    if target.pointer("/reference/kind").and_then(Value::as_str) != Some("stable_memory_ref") {
        return Err(StagedStoreError::LifecycleConflict("target unknown"));
    }
    let reference = required_text(&target["reference"], "reference")?;
    let stored: Option<(Option<String>, String, String, f64)> = connection.query_row(
        "SELECT original_source, source_event_id, exact_scope_sha256, feedback FROM tdmem_native_staged_observation_v1
        WHERE provider_reference = ?1 AND profile_id = ?2 AND project_id = ?3 AND repository_identity = ?4
        AND worktree_identity = ?5 AND branch_identity = ?6 AND tombstone = 0",
        params![reference, call.exact_scope.profile_id, call.exact_scope.project_id, call.exact_scope.repository_identity,
            call.exact_scope.worktree_identity, call.exact_scope.branch_identity],
        |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?))).optional()?;
    let Some((attribution, source_event, stored_scope, feedback)) = stored else {
        return Err(StagedStoreError::LifecycleConflict("target unknown"));
    };
    let attribution = attribution
        .map(|text| {
            serde_json::from_str::<Value>(&text)
                .map_err(|_| StagedStoreError::InvalidAdvisory("target source"))
        })
        .transpose()?;
    if let Some(attribution) = &attribution {
        if attribution.get("source") != target.get("source")
            || attribution.get("origin_scope") != target.get("original_scope")
        {
            return Err(StagedStoreError::LifecycleConflict("target source"));
        }
    } else if stored_scope != call.exact_scope.exact_scope_sha256()
        || target
            .pointer("/source/stable_record_id")
            .and_then(Value::as_str)
            != Some(source_event.as_str())
        || target
            .pointer("/original_scope/state")
            .and_then(Value::as_str)
            != Some("unavailable")
    {
        return Err(StagedStoreError::LifecycleConflict("legacy target source"));
    }
    let source_key = target
        .pointer("/source/source_key")
        .and_then(Value::as_str)
        .ok_or(StagedStoreError::InvalidAdvisory("target source key"))?;
    if source_deleted(
        connection,
        &call.exact_scope,
        source_key,
        attribution.as_ref(),
    )? {
        return Err(StagedStoreError::PrivacyDeleted);
    }
    // Match NCM's digest of the complete admitted target JSON, before any
    // provider-local reference/source projection.
    let target_bytes = serde_json::to_vec(target)
        .map_err(|_| StagedStoreError::InvalidAdvisory("target serialization"))?;
    Ok((
        reference.to_owned(),
        attribution,
        feedback,
        sha256_hex(&target_bytes),
    ))
}

fn feedback_advisory(
    connection: &Connection,
    call: &tracedecay_memory_provider_registry::ProviderCall,
    request: &Value,
) -> Result<(Value, bool), StagedStoreError> {
    let (reference, _, previous, target_digest) = resolve_target(connection, call, request)?;
    let signal = required_text(request, "signal")?;
    let weight_text = required_text(request, "weight")?;
    if weight_text.len() > 20
        || !weight_text
            .bytes()
            .all(|byte| byte.is_ascii_digit() || byte == b'.')
    {
        return Err(StagedStoreError::InvalidAdvisory("weight"));
    }
    let weight = weight_text
        .parse::<f64>()
        .ok()
        .filter(|weight| weight.is_finite() && (0.0..=1.0).contains(weight))
        .ok_or(StagedStoreError::InvalidAdvisory("weight"))?;
    required_text(request, "canonical_outcome_receipt")?;
    if super::native_provider::parse_rfc3339_nanos(required_text(request, "occurred_at")?).is_none()
    {
        return Err(StagedStoreError::InvalidAdvisory("occurred_at"));
    }
    let delta = match signal {
        "helpful" => weight,
        "harmful" | "corrected" | "superseded" => -weight,
        "ignored" => 0.0,
        _ => return Err(StagedStoreError::InvalidAdvisory("feedback signal")),
    };
    let was_suppressed:bool=connection.query_row("SELECT feedback_suppressed FROM tdmem_native_staged_observation_v1 WHERE provider_reference=?1",params![reference],|row|row.get(0))?;
    let suppressed = if delta == 0.0 {
        was_suppressed
    } else {
        signal != "helpful"
    };
    let next = if delta == 0.0 {
        previous
    } else if signal == "helpful" {
        weight
    } else {
        -weight
    };
    connection.execute(
        "UPDATE tdmem_native_staged_observation_v1 SET feedback = ?1, feedback_suppressed = ?3 WHERE provider_reference = ?2",
        params![next, reference, suppressed],
    )?;
    // A settled event is a durable change even when its explicit neutral effect is zero.
    Ok((
        serde_json::json!({"target_digest":target_digest, "signal":signal,
        "applied_effect":{"stable_memory_ref":reference,"ranking_bias_before":previous, "ranking_bias_after":next, "ranking_bias_delta":next-previous,
            "recall_score_weight":"0.5", "feedback_suppressed_before":was_suppressed,"feedback_suppressed_after":suppressed,"neutral":delta == 0.0}, "warnings":[]}),
        true,
    ))
}

fn correct_advisory(
    connection: &rusqlite::Transaction<'_>,
    call: &tracedecay_memory_provider_registry::ProviderCall,
    request: &Value,
    retention: StagedRetentionPolicyV1,
) -> Result<(Value, bool), StagedStoreError> {
    let (reference, original, _, target_digest) = resolve_target(connection, call, request)?;
    let mut original = original.ok_or(StagedStoreError::LifecycleConflict(
        "unknown source revision",
    ))?;
    let expected = required_text(request, "expected_target_revision")?;
    if original
        .pointer("/source/source_revision")
        .and_then(Value::as_str)
        != Some(expected)
    {
        return Err(StagedStoreError::LifecycleConflict("source revision"));
    }
    let kind = required_text(request, "correction_kind")?;
    if !matches!(
        kind,
        "supersede" | "replace_content" | "change_validity" | "restrict_scope" | "mark_incorrect"
    ) {
        return Err(StagedStoreError::InvalidAdvisory("correction kind"));
    }
    required_text(request, "reason")?;
    let replacement = request
        .get("replacement")
        .ok_or(StagedStoreError::InvalidAdvisory("replacement"))?;
    if kind == "restrict_scope" {
        // A staged row already has the minimum exact scope. Equal scope is no restriction.
        exact_scope_from_value(&replacement["exact_scope_identity"])?;
        return Err(StagedStoreError::InvalidAdvisory(
            "scope does not narrow exact source scope",
        ));
    }
    if let Some(text)=connection.query_row("SELECT validity_override FROM tdmem_native_staged_observation_v1 WHERE provider_reference=?1",params![reference],|row|row.get::<_,Option<String>>(0))? {
        original["validity"]=serde_json::from_str(&text).map_err(|_|StagedStoreError::InvalidAdvisory("correction validity overlay"))?;
    }
    if matches!(kind, "change_validity" | "mark_incorrect") {
        if kind == "mark_incorrect" {
            let at = required_text(replacement, "revoked_at")?;
            super::native_provider::parse_rfc3339_nanos(at)
                .ok_or(StagedStoreError::InvalidAdvisory("revoked_at"))?;
            original["validity"]["revoked_at"] = at.into();
        } else {
            for field in ["valid_from", "valid_until"] {
                original["validity"][field] = replacement
                    .get(field)
                    .cloned()
                    .ok_or(StagedStoreError::InvalidAdvisory("replacement validity"))?;
            }
        }
        recorded_validity(Some(&original))?;
        connection.execute("UPDATE tdmem_native_staged_observation_v1 SET validity_override=?1 WHERE provider_reference=?2",params![original["validity"].to_string(),reference])?;
        sync_row_validity(connection, &reference, Some(&original))?;
        return Ok((
            serde_json::json!({"target_digest":target_digest,"correction_kind":kind,"affected_provider_effects":[reference],"warnings":[]}),
            true,
        ));
    }
    let replacement_source = replacement
        .pointer("/source_identity/original_source")
        .ok_or(StagedStoreError::InvalidAdvisory("replacement attribution"))?;
    validate_attribution(replacement_source)?;
    let revision = replacement_source
        .pointer("/source/source_revision")
        .and_then(Value::as_str)
        .filter(|revision| *revision != expected)
        .ok_or(StagedStoreError::LifecycleConflict("replacement revision"))?;
    for field in [
        "canonical_provider_id",
        "canonical_session_id",
        "source_key",
        "stable_record_id",
    ] {
        if original["source"].get(field) != replacement_source["source"].get(field) {
            return Err(StagedStoreError::LifecycleConflict("replacement source"));
        }
    }
    if original["origin_scope"] != replacement_source["origin_scope"] {
        return Err(StagedStoreError::LifecycleConflict("replacement origin"));
    }
    let at = recorded_validity(Some(replacement_source))?
        .valid_from_utc_nanos
        .ok_or(StagedStoreError::InvalidAdvisory(
            "replacement validity start",
        ))?;
    let previous_validity = recorded_validity(Some(&original))?;
    if previous_validity.superseded_at_utc_nanos.is_some()
        || previous_validity.revoked_at_utc_nanos.is_some()
        || previous_validity
            .valid_from_utc_nanos
            .is_some_and(|start| start >= at)
        || previous_validity
            .valid_until_utc_nanos
            .is_some_and(|until| until < at)
    {
        return Err(StagedStoreError::LifecycleConflict("correction validity"));
    }
    let at_wire = super::native_provider::format_rfc3339_nanos(at)
        .ok_or(StagedStoreError::InvalidAdvisory("correction time"))?;
    let payload = serde_json::to_vec(replacement)
        .map_err(|_| StagedStoreError::InvalidAdvisory("replacement"))?;
    if extract_message_text(&payload).is_none() {
        return Err(StagedStoreError::InvalidAdvisory("replacement evidence"));
    }
    let key = call
        .idempotency_key
        .as_deref()
        .ok_or(StagedStoreError::InvalidAdvisory("correction key"))?;
    let record = StagedObservationRecord {
        scope: call.exact_scope.clone(),
        idempotency_key: key.to_owned(),
        source_authority: "host_session".to_owned(),
        source_event_id: required_text(&replacement_source["source"], "observation_id")?.to_owned(),
        source_revision: Some(revision.to_owned()),
        observation_kind: required_text(replacement, "observation_kind")?.to_owned(),
        payload_contract: required_text(replacement, "payload_contract")?.to_owned(),
        sanitized_payload: payload,
        operation_id: call.operation_id.clone(),
        request_identity: call.request_id.clone(),
        admitted_at_unix_ms: super::native_provider::unix_millis_now(),
    };
    let evidence = match stage_in_transaction(connection, record, retention)? {
        StagedOutcome::Committed(evidence) => evidence,
        _ => {
            return Err(StagedStoreError::LifecycleConflict(
                "replacement source already applied",
            ));
        }
    };
    original["validity"]["valid_until"] = at_wire.clone().into();
    original["validity"]["superseded_at"] = at_wire.into();
    original["validity"]["superseded_by"] = evidence.provider_reference.clone().into();
    connection.execute("UPDATE tdmem_native_staged_observation_v1 SET validity_override = ?1 WHERE provider_reference = ?2",
        params![original["validity"].to_string(), reference])?;
    sync_row_validity(connection, &reference, Some(&original))?;
    sync_row_validity(
        connection,
        &evidence.provider_reference,
        Some(replacement_source),
    )?;
    Ok((
        serde_json::json!({"target_digest":target_digest,"correction_kind":kind,
        "affected_provider_effects":[reference,evidence.provider_reference], "warnings":[]}),
        true,
    ))
}

/// One checkout-wide privacy transition shared by deletion and restore. Retain
/// only the receipt/target identity needed to reconcile earlier deliveries.
fn fence_and_scrub_source(
    connection: &Connection,
    scope: &ExactScopeFields,
    source: &str,
    generation: u64,
) -> Result<u64, StagedStoreError> {
    connection.execute(
        "INSERT OR IGNORE INTO tdmem_native_deleted_source_v2 VALUES(?1,?2,?3)",
        params![
            checkout_digest(scope),
            source,
            sqlite_i64(generation, "deletion generation")?
        ],
    )?;
    Ok(connection.execute("UPDATE tdmem_native_staged_observation_v1 SET sanitized_payload=NULL,tombstone=1,feedback=0,feedback_suppressed=0,
        deleted_source_key=?6,original_source=NULL,validity_override=NULL,valid_from_nanos=NULL,valid_until_nanos=NULL,superseded_nanos=NULL,revoked_nanos=NULL,projected_content_sha256=NULL
        WHERE profile_id=?1 AND project_id=?2 AND repository_identity=?3 AND worktree_identity=?4 AND branch_identity=?5
        AND (json_extract(original_source,'$.source.source_key')=?6 OR deleted_source_key=?6 OR (original_source IS NULL AND source_event_id=?6))
        AND (sanitized_payload IS NOT NULL OR original_source IS NOT NULL OR validity_override IS NOT NULL OR feedback!=0 OR feedback_suppressed!=0)",
        params![scope.profile_id,scope.project_id,scope.repository_identity,scope.worktree_identity,scope.branch_identity,source])? as u64)
}

fn preflight_targeted_deletion(
    connection: &Connection,
    call: &tracedecay_memory_provider_registry::ProviderCall,
    targets: &[NativeSourceTarget],
) -> Result<(), StagedStoreError> {
    let scope = &call.exact_scope;
    for target in targets {
        call.control
            .snapshot()
            .map_err(StagedStoreError::ControlEnded)?;
        let mut statement = connection.prepare("SELECT original_source FROM tdmem_native_staged_observation_v1
            WHERE profile_id=?1 AND project_id=?2 AND repository_identity=?3 AND worktree_identity=?4 AND branch_identity=?5
            AND (json_extract(original_source,'$.source.source_key')=?6 OR deleted_source_key=?6 OR (original_source IS NULL AND source_event_id=?6))
            AND (sanitized_payload IS NOT NULL OR original_source IS NOT NULL OR validity_override IS NOT NULL OR feedback!=0 OR feedback_suppressed!=0)")?;
        let mut rows = statement.query(params![
            scope.profile_id,
            scope.project_id,
            scope.repository_identity,
            scope.worktree_identity,
            scope.branch_identity,
            target.source_key
        ])?;
        while let Some(row) = rows.next()? {
            call.control
                .snapshot()
                .map_err(StagedStoreError::ControlEnded)?;
            let text: Option<String> = row.get(0)?;
            let text = text.ok_or(StagedStoreError::LifecycleConflict(
                "deletion legacy source ambiguous",
            ))?;
            let attribution: Value = serde_json::from_str(&text)
                .map_err(|_| StagedStoreError::LifecycleConflict("deletion source unavailable"))?;
            if validate_attribution(&attribution).is_err()
                || !recorded_origin_matches_checkout(&attribution, scope)
                || NativeSourceTarget::from_source(&attribution["source"]).is_err()
            {
                return Err(StagedStoreError::LifecycleConflict(
                    "deletion original source ambiguous",
                ));
            }
        }
    }
    Ok(())
}

fn fence_and_scrub_target(
    connection: &Connection,
    scope: &ExactScopeFields,
    target: &NativeSourceTarget,
    generation: u64,
) -> Result<u64, StagedStoreError> {
    let digest = target.digest();
    connection.execute(
        "INSERT OR IGNORE INTO tdmem_native_deleted_source_v2 VALUES(?1,?2,?3)",
        params![
            source_fence_checkout_digest(scope),
            digest,
            sqlite_i64(generation, "deletion generation")?
        ],
    )?;
    Ok(connection.execute("UPDATE tdmem_native_staged_observation_v1 SET sanitized_payload=NULL,tombstone=1,feedback=0,feedback_suppressed=0,
        deleted_source_key=?6,original_source=NULL,validity_override=NULL,valid_from_nanos=NULL,valid_until_nanos=NULL,superseded_nanos=NULL,revoked_nanos=NULL,projected_content_sha256=NULL
        WHERE profile_id=?1 AND project_id=?2 AND repository_identity=?3 AND worktree_identity=?4 AND branch_identity=?5
        AND json_extract(original_source,'$.origin_scope.state')='recorded'
        AND json_extract(original_source,'$.origin_scope.exact_scope_identity.profile_id')=?1
        AND json_extract(original_source,'$.origin_scope.exact_scope_identity.project_id')=?2
        AND json_extract(original_source,'$.source.canonical_provider_id')=?7
        AND json_extract(original_source,'$.source.canonical_session_id')=?8
        AND json_extract(original_source,'$.source.source_key')=?9
        AND (sanitized_payload IS NOT NULL OR original_source IS NOT NULL OR validity_override IS NOT NULL OR feedback!=0 OR feedback_suppressed!=0)",
        params![scope.profile_id,scope.project_id,scope.repository_identity,scope.worktree_identity,scope.branch_identity,digest,
            target.canonical_provider_id,target.canonical_session_id,target.source_key])? as u64)
}

// A digest-only fence has no safe preimage for an unattributed resident row.
// Require complete resident source evidence before importing any such fence;
// otherwise another delivery session could retain unidentified source content.
fn preflight_imported_source_fences(
    connection: &Connection,
    call: &tracedecay_memory_provider_registry::ProviderCall,
) -> Result<(), StagedStoreError> {
    let scope = &call.exact_scope;
    let mut after = String::new();
    loop {
        call.control
            .snapshot()
            .map_err(StagedStoreError::ControlEnded)?;
        let batch = {
            let mut statement = connection.prepare("SELECT provider_reference,original_source FROM tdmem_native_staged_observation_v1
                WHERE profile_id=?1 AND project_id=?2 AND repository_identity=?3 AND worktree_identity=?4 AND branch_identity=?5
                AND provider_reference>?6
                AND (sanitized_payload IS NOT NULL OR original_source IS NOT NULL OR validity_override IS NOT NULL OR feedback!=0 OR feedback_suppressed!=0)
                ORDER BY provider_reference LIMIT 128")?;
            let mapped = statement.query_map(
                params![
                    scope.profile_id,
                    scope.project_id,
                    scope.repository_identity,
                    scope.worktree_identity,
                    scope.branch_identity,
                    after
                ],
                |row| Ok((row.get::<_, String>(0)?, row.get::<_, Option<String>>(1)?)),
            )?;
            mapped.collect::<Result<Vec<_>, _>>()?
        };
        if batch.is_empty() {
            break;
        }
        for (reference, text) in batch {
            call.control
                .snapshot()
                .map_err(StagedStoreError::ControlEnded)?;
            after = reference;
            let text = text.ok_or(StagedStoreError::LifecycleConflict(
                "snapshot legacy source ambiguous",
            ))?;
            let attribution: Value = serde_json::from_str(&text).map_err(|_| {
                StagedStoreError::LifecycleConflict("snapshot resident source unavailable")
            })?;
            if validate_attribution(&attribution).is_err()
                || attribution
                    .pointer("/origin_scope/state")
                    .and_then(Value::as_str)
                    != Some("recorded")
                || NativeSourceTarget::from_source(&attribution["source"]).is_err()
            {
                return Err(StagedStoreError::LifecycleConflict(
                    "snapshot resident origin unavailable",
                ));
            }
        }
    }
    Ok(())
}

// Imported privacy evidence retains a digest, never an invented source preimage.
// Walk resident attribution in bounded pages and scrub only exact digest matches.
fn fence_and_scrub_target_digest(
    connection: &Connection,
    call: &tracedecay_memory_provider_registry::ProviderCall,
    digest: &str,
    generation: u64,
) -> Result<(), StagedStoreError> {
    if digest.len() != 64
        || !digest
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return Err(StagedStoreError::InvalidAdvisory(
            "snapshot typed fence digest",
        ));
    }
    let scope = &call.exact_scope;
    let mut after = String::new();
    loop {
        call.control
            .snapshot()
            .map_err(StagedStoreError::ControlEnded)?;
        let batch = {
            let mut statement = connection.prepare("SELECT provider_reference,original_source FROM tdmem_native_staged_observation_v1
                WHERE profile_id=?1 AND project_id=?2 AND repository_identity=?3 AND worktree_identity=?4 AND branch_identity=?5
                AND original_source IS NOT NULL AND provider_reference>?6 ORDER BY provider_reference LIMIT 128")?;
            let values = statement
                .query_map(
                    params![
                        scope.profile_id,
                        scope.project_id,
                        scope.repository_identity,
                        scope.worktree_identity,
                        scope.branch_identity,
                        after
                    ],
                    |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?)),
                )?
                .collect::<Result<Vec<_>, _>>()?;
            values
        };
        if batch.is_empty() {
            break;
        }
        for (reference, text) in batch {
            call.control
                .snapshot()
                .map_err(StagedStoreError::ControlEnded)?;
            after = reference;
            let attribution: Value = serde_json::from_str(&text)
                .map_err(|_| StagedStoreError::InvalidAdvisory("resident source attribution"))?;
            validate_attribution(&attribution)?;
            let target = NativeSourceTarget::from_source(&attribution["source"])?;
            if target.digest() == digest {
                if !recorded_origin_matches_checkout(&attribution, scope) {
                    return Err(StagedStoreError::LifecycleConflict(
                        "snapshot fence origin unavailable",
                    ));
                }
                fence_and_scrub_target(connection, scope, &target, generation)?;
            }
        }
    }
    connection.execute(
        "INSERT OR IGNORE INTO tdmem_native_deleted_source_v2 VALUES(?1,?2,?3)",
        params![
            source_fence_checkout_digest(scope),
            digest,
            sqlite_i64(generation, "deletion generation")?
        ],
    )?;
    Ok(())
}

fn delete_advisory(
    connection: &Connection,
    call: &tracedecay_memory_provider_registry::ProviderCall,
    request: &Value,
    generation: u64,
    targets: Option<&[NativeSourceTarget]>,
) -> Result<(Value, bool), StagedStoreError> {
    let sources = request
        .get("forget_source_keys")
        .and_then(Value::as_array)
        .filter(|sources| !sources.is_empty() && sources.len() <= 1024)
        .ok_or(StagedStoreError::InvalidAdvisory("forget source keys"))?;
    let mode = required_text(request, "mode")?;
    if !matches!(mode, "remove_influence" | "hard_delete" | "anonymize")
        || request
            .get("include_snapshots")
            .and_then(Value::as_bool)
            .is_none()
    {
        return Err(StagedStoreError::InvalidAdvisory("deletion mode"));
    }
    let verification_query = request
        .get("verification_query")
        .and_then(Value::as_str)
        .ok_or(StagedStoreError::InvalidAdvisory("verification query"))?;
    if let Some(targets) = targets {
        preflight_targeted_deletion(connection, call, targets)?;
    }
    let mut unique = BTreeSet::new();
    let mut removed = 0_u64;
    for source in sources {
        call.control
            .snapshot()
            .map_err(StagedStoreError::ControlEnded)?;
        let source = source
            .as_str()
            .filter(|source| !source.is_empty() && source.len() <= 1024)
            .ok_or(StagedStoreError::InvalidAdvisory("source key"))?;
        if !unique.insert(source) {
            return Err(StagedStoreError::InvalidAdvisory("duplicate source key"));
        }
        removed += if let Some(targets) = targets {
            let target = targets
                .iter()
                .find(|target| target.source_key == source)
                .ok_or(StagedStoreError::LifecycleConflict(
                    "deletion target coverage",
                ))?;
            fence_and_scrub_target(connection, &call.exact_scope, target, generation)?
        } else {
            fence_and_scrub_source(connection, &call.exact_scope, source, generation)?
        };
    }
    let remaining: u64 = if let Some(targets) = targets {
        targets.iter().map(|target| connection.query_row("SELECT COUNT(*) FROM tdmem_native_staged_observation_v1
            WHERE profile_id=?1 AND project_id=?2 AND repository_identity=?3 AND worktree_identity=?4 AND branch_identity=?5
            AND json_extract(original_source,'$.origin_scope.state')='recorded'
            AND json_extract(original_source,'$.origin_scope.exact_scope_identity.profile_id')=?1
            AND json_extract(original_source,'$.origin_scope.exact_scope_identity.project_id')=?2
            AND json_extract(original_source,'$.source.canonical_provider_id')=?6
            AND json_extract(original_source,'$.source.canonical_session_id')=?7
            AND json_extract(original_source,'$.source.source_key')=?8 AND tombstone=0",
            params![call.exact_scope.profile_id,call.exact_scope.project_id,call.exact_scope.repository_identity,
                call.exact_scope.worktree_identity,call.exact_scope.branch_identity,target.canonical_provider_id,
                target.canonical_session_id,target.source_key], |row| sqlite_u64(row,0)))
            .collect::<Result<Vec<_>,_>>()?.iter().sum()
    } else {
        unique.iter().map(|source| connection.query_row("SELECT COUNT(*) FROM tdmem_native_staged_observation_v1
        WHERE profile_id = ?1 AND project_id = ?2 AND repository_identity = ?3 AND worktree_identity = ?4 AND branch_identity = ?5
        AND (json_extract(original_source, '$.source.source_key') = ?6 OR (original_source IS NULL AND source_event_id = ?6)) AND tombstone = 0",
        params![call.exact_scope.profile_id, call.exact_scope.project_id, call.exact_scope.repository_identity,
            call.exact_scope.worktree_identity, call.exact_scope.branch_identity, source], |row| sqlite_u64(row, 0)))
        .collect::<Result<Vec<_>,_>>()?.iter().sum()
    };
    if remaining != 0 {
        return Err(StagedStoreError::LifecycleConflict("deletion verification"));
    }
    Ok((
        serde_json::json!({"postcondition":{"matched_effects":removed,"removed_effects":removed,"anonymized_effects":0,
        "retained_under_lock":0,"remaining_influence_count":remaining,"snapshots_examined":0,"snapshots_rewritten":0,
        "verification_query_digest":sha256_hex(verification_query.as_bytes()),
        "verification_state":"verified_absent"}, "warnings":[]}),
        true,
    ))
}

fn inspect_advisory(
    connection: &Connection,
    call: &tracedecay_memory_provider_registry::ProviderCall,
    request: &Value,
    generation: u64,
) -> Result<Value, StagedStoreError> {
    let scope = call.exact_scope.exact_scope_sha256();
    let view = required_text(request, "view")?;
    if view == "maintenance_receipt" {
        return inspect_maintenance_receipt(connection, call, request, generation);
    }
    if view == "capability_status" {
        // The application port supplies descriptor evidence after this exact
        // store-generation probe and an accepted-readiness check.
        return Ok(serde_json::json!({"view":view,"state_generation":generation}));
    }
    if !matches!(
        view,
        "state_summary" | "source_influence" | "trace" | "delivery_receipt"
    ) {
        return Err(StagedStoreError::InvalidAdvisory("inspection view"));
    }
    let maximum = bounded_integer(request, "maximum_items", 64)?;
    let byte_limit = bounded_integer(request, "maximum_bytes", 65536)? as usize;
    bounded_integer(request, "redaction_policy_revision", u64::MAX)?;
    let after = match request.get("cursor") {
        Some(Value::Null) | None => 0,
        Some(Value::String(cursor)) => cursor
            .strip_prefix(&format!("{scope}:"))
            .and_then(|text| text.parse::<u64>().ok())
            .ok_or(StagedStoreError::InvalidAdvisory("inspection cursor"))?,
        _ => return Err(StagedStoreError::InvalidAdvisory("inspection cursor")),
    };
    let mut statement = connection.prepare("SELECT provider_reference, admitted_sequence, actual_revision, feedback, tombstone, original_source,
        receipt,feedback_suppressed,validity_override,operation_id,idempotency_key,sanitized_payload,payload_sha256
        FROM tdmem_native_staged_observation_v1 WHERE exact_scope_sha256 = ?1 AND admitted_sequence > ?2
        AND (?4 IS NULL OR json_extract(original_source,'$.source.source_key')=?4)
        AND (?5 IS NULL OR idempotency_key=?5) AND (?6 IS NULL OR provider_reference=?6)
        ORDER BY admitted_sequence LIMIT ?3")?;
    let mut rows = statement.query(params![
        scope,
        sqlite_i64(after, "inspection cursor")?,
        sqlite_i64(maximum + 1, "inspection maximum_items")?,
        request
            .pointer("/selector/source_key")
            .and_then(Value::as_str),
        if view == "delivery_receipt" {
            Some(required_text(&request["selector"], "idempotency_key")?)
        } else {
            None
        },
        if view == "trace" {
            Some(required_text(&request["selector"], "stable_memory_ref")?)
        } else if view == "source_influence" {
            request
                .pointer("/selector/stable_memory_ref")
                .and_then(Value::as_str)
        } else {
            None
        },
    ])?;
    let mut items = Vec::new();
    let mut bytes = 2;
    let mut cursor = after;
    let mut partial = false;
    while let Some(row) = rows.next()? {
        let attribution = row
            .get::<_, Option<String>>(5)?
            .map(|text| {
                serde_json::from_str::<Value>(&text)
                    .map_err(|_| StagedStoreError::InvalidAdvisory("inspection attribution"))
            })
            .transpose()?;
        let mut item = if view == "state_summary" {
            serde_json::json!({"stable_memory_ref":row.get::<_,String>(0)?,"sequence":sqlite_u64(row, 1)?,
                "source_revision":row.get::<_,Option<String>>(2)?,"ranking_bias":row.get::<_,f64>(3)?,"privacy_or_retention_tombstone":row.get::<_,bool>(4)?,
                "validity":attribution.as_ref().and_then(|source|source.get("validity")),"receipt":row.get::<_,String>(6)?})
        } else {
            Value::Null
        };
        if view == "delivery_receipt" {
            item = serde_json::json!({"operation_id":row.get::<_,String>(9)?,"idempotency_key":row.get::<_,String>(10)?,
                "provider_receipt_digest":row.get::<_,String>(6)?,"stable_memory_ref":row.get::<_,String>(0)?});
        } else if view == "trace" {
            let payload: Option<Vec<u8>> = row.get(11)?;
            let mut content = if let Some(payload) = payload {
                if sha256_hex(&payload) != row.get::<_, String>(12)? {
                    return Err(StagedStoreError::PayloadDigestMismatch {
                        idempotency_key: row.get(10)?,
                        stored_payload_sha256: row.get(12)?,
                    });
                }
                extract_message_text(&payload)
            } else {
                None
            };
            if let Some(text) = &mut content {
                let mut end = text.len().min(byte_limit.saturating_sub(2048).max(1));
                while !text.is_char_boundary(end) {
                    end -= 1;
                }
                text.truncate(end);
            }
            let content_sha256 = content.as_ref().map(|text| sha256_hex(text.as_bytes()));
            item = serde_json::json!({"stable_memory_ref":row.get::<_,String>(0)?,"content":content,
                "content_sha256":content_sha256,"original_source":if content.is_some() {attribution.clone()} else {None}});
        } else if view == "source_influence" {
            let Some(attribution) = &attribution else {
                partial = true;
                continue;
            };
            let reference: String = row.get(0)?;
            // Original receipts predate the explicit association and retain
            // SHA(reference). Read them without rewriting their evidence.
            let original_reference_digest = sha256_hex(reference.as_bytes());
            let mut feedback = serde_json::json!({"helpful":0,"harmful":0,"ignored":0,"corrected":0,"superseded":0});
            let mut counts = connection.prepare(
                "SELECT json_extract(response,'$.signal'),COUNT(*) FROM tdmem_native_operation_v2 \
                 WHERE (json_extract(response,'$.applied_effect.stable_memory_ref')=?1 \
                    OR (json_extract(response,'$.applied_effect.stable_memory_ref') IS NULL \
                        AND json_extract(response,'$.target_digest')=?2)) \
                   AND json_extract(response,'$.signal') IS NOT NULL \
                 GROUP BY json_extract(response,'$.signal')",
            )?;
            let entries = counts
                .query_map(params![reference, original_reference_digest], |row| {
                    Ok((row.get::<_, String>(0)?, sqlite_u64(row, 1)?))
                })?;
            for entry in entries {
                let (signal, count) = entry?;
                feedback[signal] = count.into();
            }
            let last: Option<String> = connection
                .query_row(
                    "SELECT receipt FROM tdmem_native_operation_v2 \
                 WHERE (json_extract(response,'$.applied_effect.stable_memory_ref')=?1 \
                    OR (json_extract(response,'$.applied_effect.stable_memory_ref') IS NULL \
                        AND json_extract(response,'$.target_digest')=?2)) \
                   AND json_extract(response,'$.signal') IS NOT NULL \
                 ORDER BY generation_after DESC LIMIT 1",
                    params![reference, original_reference_digest],
                    |row| row.get(0),
                )
                .optional()?;
            let tombstone: bool = row.get(4)?;
            let suppressed: bool = row.get(7)?;
            let validity = match row.get::<_, Option<String>>(8)? {
                Some(text) => serde_json::from_str::<Value>(&text)
                    .map_err(|_| StagedStoreError::InvalidAdvisory("inspection validity"))?,
                None => attribution["validity"].clone(),
            };
            let now = super::native_provider::unix_millis_now().saturating_mul(1_000_000);
            let passed = |field: &str| {
                validity
                    .get(field)
                    .and_then(Value::as_str)
                    .and_then(super::native_provider::parse_rfc3339_nanos)
                    .is_some_and(|at| at <= now)
            };
            let disposition = if tombstone {
                "deleted"
            } else if passed("revoked_at") {
                "revoked"
            } else if passed("superseded_at") {
                "superseded"
            } else if passed("valid_until") {
                "expired"
            } else {
                "available"
            };
            let effect_summary = serde_json::json!({
                "ranking_bias": row.get::<_, f64>(3)?,
                "feedback_suppressed": suppressed,
                "validity": validity,
            })
            .to_string();
            if effect_summary.len() > 8192 {
                return Err(StagedStoreError::ValueOutOfRange {
                    field: "provider_local_effect_summary",
                });
            }
            item = serde_json::json!({"target":{"provider_id":call.provider_id.as_str(),"registration_revision":call.registration_revision,
                "original_scope":attribution["origin_scope"],"delivery_scope":scope_json(&call.exact_scope),"source":attribution["source"],
                "reference":{"kind":"stable_memory_ref","reference":reference}},"source":attribution["source"],
                "active":!tombstone&&!suppressed&&disposition=="available","disposition":disposition,"settled_feedback":feedback,"last_feedback_receipt":last,
                "provider_local_effect_summary":effect_summary});
        }
        let size = item.to_string().len() + 1;
        if items.len() >= maximum as usize || bytes + size > byte_limit {
            partial = true;
            break;
        }
        bytes += size;
        cursor = sqlite_u64(row, 1)?;
        items.push(item);
    }
    let missing = matches!(view, "trace" | "delivery_receipt") && items.is_empty();
    partial |= missing;
    Ok(
        serde_json::json!({"view":view,"items":items,"coverage":if partial {"partial"} else {"complete"},
        "next_cursor":(partial && !missing).then(||format!("{scope}:{cursor}")),"redactions":["source_identifiers"],
        "state_generation":generation,"warnings":if missing {vec!["native.inspection_evidence_unavailable"]} else {Vec::<&str>::new()}}),
    )
}

fn inspect_maintenance_receipt(
    connection: &Connection,
    call: &tracedecay_memory_provider_registry::ProviderCall,
    request: &Value,
    generation: u64,
) -> Result<Value, StagedStoreError> {
    let scope = call.exact_scope.exact_scope_sha256();
    let selector = &request["selector"];
    let key = required_text(selector, "idempotency_key")?;
    let requested_operation = selector
        .get("operation_id")
        .map(|_| required_text(selector, "operation_id"))
        .transpose()?;
    let stored: Option<(String, String, u64, u64, String, String)> = connection
        .query_row(
            "SELECT request_digest,operation_id,generation_before,generation_after,response,receipt \
             FROM tdmem_native_operation_v2 WHERE scope=?1 AND idempotency_key=?2 \
               AND (?3 IS NULL OR operation_id=?3)",
            params![scope, key, requested_operation],
            |row| {
                Ok((
                    row.get(0)?,
                    row.get(1)?,
                    sqlite_u64(row, 2)?,
                    sqlite_u64(row, 3)?,
                    row.get(4)?,
                    row.get(5)?,
                ))
            },
        )
        .optional()?;
    let mut response = serde_json::json!({"view":"maintenance_receipt","items":[],
        "coverage":"complete","next_cursor":null,"redactions":[],
        "state_generation":generation,"warnings":[]});
    let Some((digest, operation_id, before, after, stored_response, receipt)) = stored else {
        // Dry runs have no journal entry, just like an unknown exact key.
        return Ok(response);
    };
    let stored: Value = serde_json::from_str(&stored_response)
        .map_err(|_| StagedStoreError::InvalidAdvisory("maintenance receipt response"))?;
    if operation_id.is_empty()
        || after < before
        || digest.len() != 64
        || !digest
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
        || receipt != sha256_hex(format!("{scope}:{key}:{digest}:{before}:{after}").as_bytes())
        || stored["provider_receipt_digest"] != receipt
        || stored["state_generation_before"] != before
        || stored["state_generation_after"] != after
    {
        return Err(StagedStoreError::LifecycleConflict(
            "maintenance receipt integrity",
        ));
    }
    let task = required_text(&stored, "task")?;
    if !matches!(
        task,
        "consolidate" | "decay" | "prune_expired" | "validate_state" | "repair" | "compact"
    ) || stored.get("dry_run").and_then(Value::as_bool) != Some(false)
    {
        return Err(StagedStoreError::InvalidAdvisory(
            "maintenance receipt task",
        ));
    }
    let counter = |field: &str| {
        stored
            .get(field)
            .and_then(Value::as_u64)
            .ok_or(StagedStoreError::InvalidAdvisory(
                "maintenance receipt counters",
            ))
    };
    let scanned = counter("scanned_items")?;
    let changed = counter("changed_items")?;
    let removed = counter("removed_items")?;
    if removed > changed || changed > scanned {
        return Err(StagedStoreError::InvalidAdvisory(
            "maintenance receipt counters",
        ));
    }
    if stored
        .get("proposed_changes")
        .is_some_and(|value| value.as_u64().is_none())
    {
        return Err(StagedStoreError::InvalidAdvisory(
            "maintenance proposed changes",
        ));
    }
    let partial =
        stored
            .get("partial")
            .and_then(Value::as_bool)
            .ok_or(StagedStoreError::InvalidAdvisory(
                "maintenance receipt partial",
            ))?;
    match stored.get("resume_cursor") {
        Some(Value::Null) if !partial => {}
        Some(Value::String(cursor)) if partial && !cursor.is_empty() => {}
        _ => {
            return Err(StagedStoreError::InvalidAdvisory(
                "maintenance receipt cursor",
            ));
        }
    }
    let warnings = stored
        .get("warnings")
        .and_then(Value::as_array)
        .filter(|warnings| warnings.iter().all(Value::is_string))
        .ok_or(StagedStoreError::InvalidAdvisory(
            "maintenance receipt warnings",
        ))?;
    let mut outcome = serde_json::json!({"task":task,"dry_run":false,
        "scanned_items":scanned,"changed_items":changed,
        "removed_items":removed,"partial":partial,"resume_cursor":stored["resume_cursor"],
        "receipt":{"state_generation_before":before,"state_generation_after":after,
            "provider_receipt_digest":receipt}});
    if let Some(proposed) = stored.get("proposed_changes") {
        outcome["proposed_changes"] = proposed.clone();
    }
    response["items"] = serde_json::json!([{"operation_id":operation_id,
        "idempotency_key":key,"outcome":outcome}]);
    response["warnings"] = warnings.clone().into();
    Ok(response)
}

/// Pages finite descriptor or journal evidence after the actual read receipt
/// and generation fields have been attached, checking complete reply framing
/// against the response limit. No evidence is synthesized here.
pub(super) fn paginate_inspection_evidence(
    outcome: &mut StagedControlOutcome,
    call: &tracedecay_memory_provider_registry::ProviderCall,
    request: &Value,
    maximum_items: u64,
    maximum_response_bytes: u64,
) -> Result<(), StagedStoreError> {
    let maximum = bounded_integer(request, "maximum_items", 64)?.min(maximum_items) as usize;
    let byte_limit =
        bounded_integer(request, "maximum_bytes", 65536)?.min(maximum_response_bytes) as usize;
    bounded_integer(request, "redaction_policy_revision", u64::MAX)?;
    let identity = serde_json::json!({"scope":call.exact_scope.exact_scope_sha256(),
        "view":request["view"],"generation":outcome.response["state_generation"],"selector":request["selector"],
        "registration_revision":call.registration_revision,"ready_receipt":call.ready_receipt_sha256});
    let prefix = format!(
        "native-evidence:{}:",
        sha256_hex(identity.to_string().as_bytes())
    );
    let after = match request.get("cursor") {
        None | Some(Value::Null) => 0,
        Some(Value::String(cursor)) => cursor
            .strip_prefix(&prefix)
            .and_then(|offset| offset.parse::<usize>().ok())
            .ok_or(StagedStoreError::InvalidAdvisory("inspection cursor"))?,
        _ => return Err(StagedStoreError::InvalidAdvisory("inspection cursor")),
    };
    let items = std::mem::take(
        outcome
            .response
            .get_mut("items")
            .and_then(Value::as_array_mut)
            .ok_or(StagedStoreError::InvalidAdvisory("inspection items"))?,
    );
    if after > items.len() {
        return Err(StagedStoreError::InvalidAdvisory("inspection cursor"));
    }
    let set_cursor = |response: &mut Value, end: usize| {
        let partial = end < items.len();
        response["coverage"] = if partial { "partial" } else { "complete" }.into();
        response["next_cursor"] = if partial {
            format!("{prefix}{end}").into()
        } else {
            Value::Null
        };
    };
    let fits_response = |outcome: &StagedControlOutcome| {
        outcome.response.to_string().len() <= byte_limit
            && super::native_provider::native_control_reply(
                call,
                outcome.clone(),
                outcome.generation_after,
                maximum_response_bytes,
            )
            .terminal
            .terminal_code()
                == tracedecay_memory_provider_registry::TerminalCode::Success
    };
    set_cursor(&mut outcome.response, after);
    for (index, item) in items.iter().enumerate().skip(after) {
        call.control
            .snapshot()
            .map_err(StagedStoreError::ControlEnded)?;
        outcome.response["items"]
            .as_array_mut()
            .ok_or(StagedStoreError::InvalidAdvisory("inspection items"))?
            .push(item.clone());
        set_cursor(&mut outcome.response, index + 1);
        if index + 1 - after > maximum || !fits_response(outcome) {
            outcome.response["items"]
                .as_array_mut()
                .ok_or(StagedStoreError::InvalidAdvisory("inspection items"))?
                .pop();
            set_cursor(&mut outcome.response, index);
            if index == after {
                return Err(StagedStoreError::ValueOutOfRange {
                    field: "inspection evidence bytes",
                });
            }
            break;
        }
    }
    if !fits_response(outcome) {
        return Err(StagedStoreError::ValueOutOfRange {
            field: "inspection evidence bytes",
        });
    }
    Ok(())
}

fn maintain_advisory(
    connection: &Connection,
    call: &tracedecay_memory_provider_registry::ProviderCall,
    request: &Value,
) -> Result<(Value, bool), StagedStoreError> {
    let task = required_text(request, "task")?;
    if !matches!(
        task,
        "consolidate" | "decay" | "prune_expired" | "validate_state" | "repair" | "compact"
    ) {
        return Err(StagedStoreError::InvalidAdvisory("maintenance task"));
    }
    let limit = bounded_integer(request, "maximum_items", 1_000_000)?.min(512);
    let byte_limit = bounded_integer(request, "maximum_bytes", 1_073_741_824)?;
    let duration = bounded_integer(request, "maximum_duration_millis", 3_600_000)?.min(1000);
    let dry_run = request
        .get("dry_run")
        .and_then(Value::as_bool)
        .ok_or(StagedStoreError::InvalidAdvisory("dry_run"))?;
    let scope = call.exact_scope.exact_scope_sha256();
    let prefix = format!("{scope}:{task}:");
    let after = match request.get("resume_cursor") {
        None | Some(Value::Null) => 0,
        Some(Value::String(cursor)) => cursor
            .strip_prefix(&prefix)
            .and_then(|text| text.parse::<u64>().ok())
            .ok_or(StagedStoreError::InvalidAdvisory("maintenance cursor"))?,
        _ => return Err(StagedStoreError::InvalidAdvisory("maintenance cursor")),
    };
    let mut cursor = after;
    let started = std::time::Instant::now();
    let mut statement=connection.prepare("SELECT provider_reference,sanitized_payload,payload_sha256,feedback,valid_until_nanos,admitted_sequence
        FROM tdmem_native_staged_observation_v1 WHERE exact_scope_sha256 = ?1 AND tombstone = 0 AND admitted_sequence > ?3 ORDER BY admitted_sequence LIMIT ?2")?;
    let mut rows = statement.query(params![
        scope,
        sqlite_i64(limit + 1, "maintenance maximum_items")?,
        sqlite_i64(after, "maintenance cursor")?
    ])?;
    let mut scanned = 0_u64;
    let mut changed = 0_u64;
    let mut removed = 0_u64;
    let mut bytes = 0_u64;
    let mut partial = false;
    let mut updates = Vec::new();
    while let Some(row) = rows.next()? {
        call.control
            .snapshot()
            .map_err(StagedStoreError::ControlEnded)?;
        let payload: Vec<u8> = row.get(1)?;
        if scanned == limit
            || bytes + payload.len() as u64 > byte_limit
            || started.elapsed().as_millis() >= u128::from(duration)
        {
            partial = true;
            break;
        }
        scanned += 1;
        bytes += payload.len() as u64;
        let reference: String = row.get(0)?;
        let digest: String = row.get(2)?;
        let bias: f64 = row.get(3)?;
        let corrupt = sha256_hex(&payload) != digest;
        cursor = sqlite_u64(row, 5)?;
        let expired = row.get::<_, Option<i64>>(4)?.is_some_and(|until| {
            until <= super::native_provider::unix_millis_now().saturating_mul(1_000_000)
        });
        let remove = (task == "repair" && corrupt) || (task == "prune_expired" && expired);
        if corrupt && !remove {
            return Err(StagedStoreError::PayloadDigestMismatch {
                idempotency_key: reference,
                stored_payload_sha256: digest,
            });
        }
        let decay = task == "decay" && bias != 0.0;
        if remove || decay {
            changed += 1;
            removed += u64::from(remove);
            updates.push((reference, remove, bias * 0.9));
        }
    }
    drop(rows);
    drop(statement);
    if !dry_run {
        for (reference, remove, bias) in updates {
            if remove {
                connection.execute("UPDATE tdmem_native_staged_observation_v1 SET sanitized_payload=NULL,tombstone=1,feedback=0,feedback_suppressed=0,original_source=NULL,validity_override=NULL,valid_from_nanos=NULL,valid_until_nanos=NULL,superseded_nanos=NULL,revoked_nanos=NULL,projected_content_sha256=NULL WHERE provider_reference=?1",params![reference])?;
            } else {
                connection.execute("UPDATE tdmem_native_staged_observation_v1 SET feedback=?1 WHERE provider_reference=?2",params![bias,reference])?;
            }
        }
    }
    Ok((
        serde_json::json!({"task":task,"dry_run":dry_run,"scanned_items":scanned,"changed_items":if dry_run {0}else{changed},
        "removed_items":if dry_run {0}else{removed},"proposed_changes":changed,"partial":partial,"resume_cursor":partial.then(||format!("{prefix}{cursor}")),"warnings":[]}),
        !dry_run,
    ))
}

fn sync_row_validity(
    connection: &Connection,
    reference: &str,
    attribution: Option<&Value>,
) -> Result<(), StagedStoreError> {
    let validity = recorded_validity(attribution)?;
    let payload:Option<Vec<u8>>=connection.query_row("SELECT sanitized_payload FROM tdmem_native_staged_observation_v1 WHERE provider_reference=?1",params![reference],|row|row.get(0))?;
    let content_digest = payload
        .as_deref()
        .and_then(extract_message_text)
        .map(|text| sha256_hex(text.as_bytes()));
    connection.execute("UPDATE tdmem_native_staged_observation_v1 SET projected_content_sha256=?1 WHERE provider_reference=?2",params![content_digest,reference])?;
    connection.execute("UPDATE tdmem_native_staged_observation_v1 SET valid_from_nanos=?1,valid_until_nanos=?2,superseded_nanos=?3,revoked_nanos=?4 WHERE provider_reference=?5",
        params![validity.valid_from_utc_nanos,validity.valid_until_utc_nanos,validity.superseded_at_utc_nanos,validity.revoked_at_utc_nanos,reference])?;
    Ok(())
}

fn stage_in_transaction(
    transaction: &rusqlite::Transaction<'_>,
    record: StagedObservationRecord,
    retention: StagedRetentionPolicyV1,
) -> Result<StagedOutcome, StagedStoreError> {
    record.validate()?;
    let exact_scope_sha256 = record.scope.exact_scope_sha256();
    let payload_sha256 = sha256_hex(&record.sanitized_payload);
    let semantic_sha256 = semantic_observation_digest(&record.sanitized_payload)?;
    let source_revision = record.source_revision.as_deref();
    let envelope: Value = serde_json::from_slice(&record.sanitized_payload)
        .map_err(|_| StagedStoreError::InvalidAdvisory("observation envelope"))?;
    let original_source = envelope
        .pointer("/source_identity/original_source")
        .cloned();
    if let Some(attribution) = &original_source {
        validate_attribution(attribution)?;
        if extract_message_text(&record.sanitized_payload).is_none() {
            return Err(StagedStoreError::InvalidAdvisory("observation evidence"));
        }
        if attribution
            .pointer("/source/source_revision")
            .and_then(Value::as_str)
            != source_revision
        {
            return Err(StagedStoreError::InvalidAdvisory("source revision"));
        }
    }

    let source_key = original_source
        .as_ref()
        .and_then(|source| source.pointer("/source/source_key"))
        .and_then(Value::as_str)
        .unwrap_or(&record.source_event_id);
    if source_deleted(
        &transaction,
        &record.scope,
        source_key,
        original_source.as_ref(),
    )? {
        return Err(StagedStoreError::PrivacyDeleted);
    }
    let existing = transaction
        .query_row(
            "SELECT observation_kind, payload_sha256, provider_reference, receipt, \
                        effect_digest, admitted_sequence, operation_id, semantic_sha256, sanitized_payload \
                 FROM tdmem_native_staged_observation_v1 \
                 WHERE exact_scope_sha256 = ?1 AND idempotency_key = ?2",
            params![exact_scope_sha256, record.idempotency_key],
            |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, String>(3)?,
                    row.get::<_, String>(4)?,
                    row.get::<_, i64>(5)?,
                    row.get::<_, String>(6)?,
                    row.get::<_, Option<String>>(7)?,
                    row.get::<_, Option<Vec<u8>>>(8)?,
                ))
            },
        )
        .optional()?;
    if let Some((
        stored_kind,
        stored_payload_sha256,
        provider_reference,
        receipt,
        effect_digest,
        stored_sequence,
        stored_operation_id,
        stored_semantic,
        stored_payload,
    )) = existing
    {
        if stored_kind != record.observation_kind {
            return Ok(StagedOutcome::Conflict {
                reason: StagedConflictReason::KindDiverged {
                    stored_observation_kind: stored_kind,
                    offered_observation_kind: record.observation_kind,
                },
            });
        }
        let stored_semantic = stored_semantic.or_else(|| {
            stored_payload
                .as_deref()
                .and_then(|payload| semantic_observation_digest(payload).ok())
        });
        if stored_payload_sha256 != payload_sha256
            && stored_semantic.as_deref() != Some(semantic_sha256.as_str())
        {
            return Ok(StagedOutcome::Conflict {
                reason: StagedConflictReason::PayloadDiverged {
                    stored_payload_sha256,
                    offered_payload_sha256: payload_sha256,
                },
            });
        }
        let admitted_sequence =
            u64::try_from(stored_sequence).map_err(|_| StagedStoreError::ValueOutOfRange {
                field: "admitted_sequence",
            })?;
        // The stored evidence, not a freshly minted one. Every field the
        // duplicate wire contract admits — the receipt and the committing
        // operation identity — is answered from these columns, and the
        // provider reference and effect digest stay readable here for
        // audit even after the row's content has been evicted to a
        // tombstone. See `staged_duplicate_reply` for the fields the
        // `duplicate` committed-effect state does *not* carry.
        return Ok(StagedOutcome::Duplicate(StagedEffectEvidence {
            provider_reference,
            receipt,
            effect_digest,
            admitted_sequence,
            idempotency_key: record.idempotency_key,
            operation_id: stored_operation_id,
        }));
    }

    let source_conflict: Option<String> = transaction
            .query_row(
                "SELECT idempotency_key FROM tdmem_native_staged_observation_v1 \
                 WHERE exact_scope_sha256 = ?1 AND source_authority = ?2 \
                   AND source_event_id = ?3 AND actual_revision IS ?4 AND (payload_sha256 = ?5 OR semantic_sha256 = ?7 OR (?6 IS NOT NULL AND json_extract(original_source,'$.source.content_sha256') = ?6))",
                params![
                    exact_scope_sha256,
                    record.source_authority,
                    record.source_event_id,
                    source_revision,
                    payload_sha256,
                    original_source.as_ref().and_then(|source|source.pointer("/source/content_sha256")).and_then(Value::as_str),
                    semantic_sha256,
                ],
                |row| row.get(0),
            )
            .optional()?;
    if let Some(stored_idempotency_key) = source_conflict {
        return Ok(StagedOutcome::Conflict {
            reason: StagedConflictReason::SourceIdentityReused {
                stored_idempotency_key,
            },
        });
    }

    let previous = u64::try_from(generation_in(&transaction)?).map_err(|_| {
        StagedStoreError::ValueOutOfRange {
            field: "generation",
        }
    })?;
    let admitted_sequence = previous
        .checked_add(1)
        .ok_or(StagedStoreError::ValueOutOfRange {
            field: "admitted_sequence",
        })?;
    let stored_sequence = sqlite_i64(admitted_sequence, "admitted_sequence")?;
    let evidence = derive_effect_evidence(
        &exact_scope_sha256,
        &record.idempotency_key,
        &payload_sha256,
        admitted_sequence,
        &record.operation_id,
    );

    transaction.execute(
        "INSERT INTO tdmem_native_staged_observation_v1 (
                 exact_scope_sha256, idempotency_key, profile_id, project_id,
                 repository_identity, worktree_identity, branch_identity, agent_session_id,
                 resolved_scope_digest, source_authority, source_event_id, source_revision,
                 observation_kind, payload_contract, sanitized_payload, payload_sha256,
                 operation_id, request_identity, provider_reference, receipt, effect_digest,
                 admitted_sequence, admitted_at_unix_ms, tombstone, actual_revision, original_source, semantic_sha256
             ) VALUES (
                 ?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16,
                 ?17, ?18, ?19, ?20, ?21, ?22, ?23, 0, ?24, ?25, ?26
             )",
        params![
            exact_scope_sha256,
            record.idempotency_key,
            record.scope.profile_id,
            record.scope.project_id,
            record.scope.repository_identity,
            record.scope.worktree_identity,
            record.scope.branch_identity,
            record.scope.agent_session_id,
            record.scope.resolved_scope_digest,
            record.source_authority,
            record.source_event_id,
            0_i64,
            record.observation_kind,
            record.payload_contract,
            record.sanitized_payload,
            payload_sha256,
            record.operation_id,
            record.request_identity,
            evidence.provider_reference,
            evidence.receipt,
            evidence.effect_digest,
            stored_sequence,
            record.admitted_at_unix_ms,
            source_revision,
            original_source.as_ref().map(Value::to_string),
            semantic_sha256,
        ],
    )?;

    sync_row_validity(
        &transaction,
        &evidence.provider_reference,
        original_source.as_ref(),
    )?;
    evict_scope_overflow(
        &transaction,
        &exact_scope_sha256,
        retention.maximum_content_rows_per_scope,
    )?;

    transaction.execute(
        "UPDATE tdmem_native_state_v2 SET generation = ?1 WHERE singleton = 1",
        params![stored_sequence],
    )?;
    Ok(StagedOutcome::Committed(evidence))
}

const SNAPSHOT_SCHEMA: &str = super::native_provider::STATE_SCHEMA_VERSION;
// Old decoders compare this internal field before reading deletion fences. Keep
// the public provider/database schema stable while refusing unsafe old decoding.
const SOURCE_FENCE_SNAPSHOT_SCHEMA: &str = "native-staged-snapshot-source-fences-v1";
const SOURCE_FENCE_KIND: &str = "canonical_source_v1";
const SNAPSHOT_COLUMNS: &[&str] = &[
    "exact_scope_sha256",
    "idempotency_key",
    "profile_id",
    "project_id",
    "repository_identity",
    "worktree_identity",
    "branch_identity",
    "agent_session_id",
    "resolved_scope_digest",
    "source_authority",
    "source_event_id",
    "source_revision",
    "observation_kind",
    "payload_contract",
    "sanitized_payload",
    "payload_sha256",
    "operation_id",
    "request_identity",
    "provider_reference",
    "receipt",
    "effect_digest",
    "admitted_sequence",
    "admitted_at_unix_ms",
    "tombstone",
    "actual_revision",
    "original_source",
    "feedback",
    "feedback_suppressed",
    "validity_override",
    "valid_from_nanos",
    "valid_until_nanos",
    "superseded_nanos",
    "revoked_nanos",
    "semantic_sha256",
    "projected_content_sha256",
    "deleted_source_key",
];

fn export_snapshot(
    connection: &Connection,
    call: &tracedecay_memory_provider_registry::ProviderCall,
    generation: u64,
) -> Result<Value, StagedStoreError> {
    let scope = call.exact_scope.exact_scope_sha256();
    let mut statement=connection.prepare(&format!("SELECT {} FROM tdmem_native_staged_observation_v1 WHERE exact_scope_sha256 = ?1 ORDER BY admitted_sequence LIMIT 1025",SNAPSHOT_COLUMNS.join(",")))?;
    let mut rows = statement.query(params![scope])?;
    let mut values = Vec::new();
    let mut size = 0;
    while let Some(row) = rows.next()? {
        call.control
            .snapshot()
            .map_err(StagedStoreError::ControlEnded)?;
        let mut value = serde_json::Map::new();
        for (index, name) in SNAPSHOT_COLUMNS.iter().enumerate() {
            use rusqlite::types::ValueRef;
            let field = match row.get_ref(index)? {
                ValueRef::Null => Value::Null,
                ValueRef::Integer(integer) => integer.into(),
                ValueRef::Real(real) => serde_json::json!(real),
                ValueRef::Text(text) => std::str::from_utf8(text)
                    .map_err(|_| StagedStoreError::InvalidAdvisory("snapshot text"))?
                    .into(),
                ValueRef::Blob(bytes) => serde_json::json!({"blob":bytes}),
            };
            value.insert((*name).to_owned(), field);
        }
        let value = Value::Object(value);
        size += value.to_string().len();
        if size > 65536 || values.len() >= 1024 {
            return Err(StagedStoreError::ValueOutOfRange {
                field: "snapshot bytes",
            });
        }
        values.push(value);
    }
    let sources = snapshot_sources(&values)?;
    let acknowledged: u64 = connection
        .query_row(
            "SELECT acknowledged_sequence FROM tdmem_native_replay_v2 WHERE scope=?1",
            params![scope],
            |row| sqlite_u64(row, 0),
        )
        .optional()?
        .unwrap_or(0);
    let observation_sequence = values
        .iter()
        .filter_map(|row| row["admitted_sequence"].as_u64())
        .max()
        .unwrap_or(0);
    let mut operations = Vec::new();
    let mut statement = connection.prepare("SELECT idempotency_key,request_digest,operation_id,generation_before,generation_after,response,receipt FROM tdmem_native_operation_v2 WHERE scope=?1 ORDER BY generation_after,idempotency_key LIMIT 1025")?;
    let mut rows = statement.query(params![scope])?;
    while let Some(row) = rows.next()? {
        call.control
            .snapshot()
            .map_err(StagedStoreError::ControlEnded)?;
        if operations.len() >= 1024 {
            return Err(StagedStoreError::ValueOutOfRange {
                field: "snapshot operations",
            });
        }
        operations.push(serde_json::json!({"idempotency_key":row.get::<_,String>(0)?,"request_digest":row.get::<_,String>(1)?,
            "operation_id":row.get::<_,String>(2)?,"generation_before":sqlite_u64(row, 3)?,"generation_after":sqlite_u64(row, 4)?,
            "response":row.get::<_,String>(5)?,"receipt":row.get::<_,String>(6)?}));
    }
    let legacy_checkout = checkout_digest(&call.exact_scope);
    let typed_checkout = source_fence_checkout_digest(&call.exact_scope);
    let mut statement = connection.prepare("SELECT checkout,source_key,generation FROM tdmem_native_deleted_source_v2 WHERE checkout IN (?1,?2) ORDER BY checkout,source_key LIMIT 1025")?;
    let fences = statement.query_map(params![legacy_checkout,typed_checkout],|row| {
        let checkout: String = row.get(0)?;
        let mut fence = serde_json::json!({"source_key":row.get::<_,String>(1)?,"generation":sqlite_u64(row, 2)?});
        if checkout == typed_checkout {
            fence["kind"] = SOURCE_FENCE_KIND.into();
        }
        Ok(fence)
    })?.collect::<Result<Vec<_>,_>>()?;
    if fences.len() > 1024 {
        return Err(StagedStoreError::ValueOutOfRange {
            field: "snapshot deletion fences",
        });
    }
    let body_schema = if fences.iter().any(|fence| fence.get("kind").is_some()) {
        SOURCE_FENCE_SNAPSHOT_SCHEMA
    } else {
        SNAPSHOT_SCHEMA
    };
    let body = serde_json::json!({"schema":body_schema,"scope":scope,"generation":generation,"observation_sequence":observation_sequence,
        "rows":values,"operations":operations,"deletion_fences":fences,"acknowledged_sequence":acknowledged});
    let bytes = serde_json::to_vec(&body)
        .map_err(|_| StagedStoreError::InvalidAdvisory("snapshot encoding"))?;
    if bytes.len() > 65536 {
        return Err(StagedStoreError::ValueOutOfRange {
            field: "snapshot bytes",
        });
    }
    let digest = sha256_hex(&bytes);
    Ok(
        serde_json::json!({"snapshot":{"identity":{"snapshot_id":format!("native-snapshot:{digest}"),"provider_id":call.provider_id.as_str(),
        "implementation_identity_digest":super::native_provider::IMPLEMENTATION_IDENTITY_SHA256,"state_schema_version":SNAPSHOT_SCHEMA,
        "exact_scope_digest":scope,"state_generation":generation,"observation_sequence":observation_sequence,"parent_snapshot_id":Value::Null,
        "content_sha256":digest,"byte_length":bytes.len(),"created_at":super::native_provider::format_rfc3339_nanos(super::native_provider::unix_millis_now().saturating_mul(1_000_000))},
        "bytes":bytes,"sources":sources},"warnings":[]}),
    )
}

fn snapshot_sources(rows: &[Value]) -> Result<Vec<Value>, StagedStoreError> {
    let mut sources = std::collections::BTreeMap::new();
    for row in rows {
        if let Some(text) = row.get("original_source").and_then(Value::as_str) {
            let attribution: Value = serde_json::from_str(text)
                .map_err(|_| StagedStoreError::InvalidAdvisory("snapshot source"))?;
            validate_attribution(&attribution)?;
            let source = attribution["source"].clone();
            sources.insert(source.to_string(), source);
        } else if row.get("tombstone").and_then(Value::as_i64) != Some(1) {
            return Err(StagedStoreError::InvalidAdvisory(
                "snapshot source inventory unavailable",
            ));
        }
    }
    Ok(sources.into_values().collect())
}

fn restore_snapshot(
    connection: &Connection,
    call: &tracedecay_memory_provider_registry::ProviderCall,
    request: &Value,
    before: u64,
    admission: Option<&tracedecay_memory_provider_registry::CurrentAdvisoryAdmission>,
) -> Result<(Value, bool), StagedStoreError> {
    let snapshot = request
        .get("snapshot")
        .ok_or(StagedStoreError::InvalidAdvisory("snapshot"))?;
    let identity = &snapshot["identity"];
    let scope = call.exact_scope.exact_scope_sha256();
    if identity["provider_id"] != call.provider_id.as_str()
        || identity["state_schema_version"] != SNAPSHOT_SCHEMA
        || identity["implementation_identity_digest"]
            != super::native_provider::IMPLEMENTATION_IDENTITY_SHA256
        || identity["exact_scope_digest"] != scope
    {
        return Err(StagedStoreError::LifecycleConflict("snapshot incompatible"));
    }
    let bytes = snapshot
        .get("bytes")
        .and_then(Value::as_array)
        .filter(|bytes| bytes.len() <= 65536)
        .ok_or(StagedStoreError::InvalidAdvisory("snapshot bytes"))?
        .iter()
        .map(|byte| {
            byte.as_u64()
                .and_then(|value| u8::try_from(value).ok())
                .ok_or(StagedStoreError::InvalidAdvisory("snapshot byte"))
        })
        .collect::<Result<Vec<_>, _>>()?;
    if identity["byte_length"].as_u64() != Some(bytes.len() as u64)
        || identity["content_sha256"] != sha256_hex(&bytes)
    {
        return Err(StagedStoreError::LifecycleConflict("snapshot digest"));
    }
    let body: Value = serde_json::from_slice(&bytes)
        .map_err(|_| StagedStoreError::InvalidAdvisory("snapshot contents"))?;
    let typed_fence_schema = body["schema"] == SOURCE_FENCE_SNAPSHOT_SCHEMA;
    if (!typed_fence_schema && body["schema"] != SNAPSHOT_SCHEMA)
        || body["scope"] != scope
        || body["generation"] != identity["state_generation"]
        || body["observation_sequence"] != identity["observation_sequence"]
    {
        return Err(StagedStoreError::LifecycleConflict("snapshot identity"));
    }
    let rows = body
        .get("rows")
        .and_then(Value::as_array)
        .filter(|rows| rows.len() <= 1024)
        .ok_or(StagedStoreError::InvalidAdvisory("snapshot rows"))?;
    let sources = snapshot_sources(rows)?;
    let declared = snapshot
        .get("sources")
        .and_then(Value::as_array)
        .ok_or(StagedStoreError::InvalidAdvisory("snapshot inventory"))?;
    let canonical_set =
        |values: &[Value]| values.iter().map(Value::to_string).collect::<BTreeSet<_>>();
    if canonical_set(&sources) != canonical_set(declared)
        || canonical_set(declared).len() != declared.len()
    {
        return Err(StagedStoreError::LifecycleConflict(
            "snapshot source inventory",
        ));
    }
    let checkpoint = &request["disposition_checkpoint"];
    if exact_scope_from_value(&checkpoint["exact_scope"])? != call.exact_scope
        || required_text(checkpoint, "authority_ref").is_err()
        || super::native_provider::parse_rfc3339_nanos(required_text(checkpoint, "checked_at")?)
            .is_none()
    {
        return Err(StagedStoreError::LifecycleConflict("restore checkpoint"));
    }
    let dispositions = request
        .get("source_dispositions")
        .and_then(Value::as_array)
        .ok_or(StagedStoreError::InvalidAdvisory("source dispositions"))?;
    let mut states = std::collections::BTreeMap::new();
    for binding in dispositions {
        let source = binding
            .get("source")
            .ok_or(StagedStoreError::InvalidAdvisory(
                "source disposition binding",
            ))?;
        let disposition = &binding["current_disposition"];
        let state = required_text(disposition, "state")?;
        if !matches!(
            state,
            "available" | "superseded" | "revoked" | "deleted" | "redacted" | "expired"
        ) || required_text(disposition, "authority_ref").is_err()
            || super::native_provider::parse_rfc3339_nanos(required_text(
                disposition,
                "checked_at",
            )?)
            .is_none()
            || states.insert(source.to_string(), state).is_some()
        {
            return Err(StagedStoreError::LifecycleConflict(
                "restore disposition coverage",
            ));
        }
    }
    if states.keys().cloned().collect::<BTreeSet<_>>() != canonical_set(&sources) {
        return Err(StagedStoreError::LifecycleConflict(
            "restore disposition inventory",
        ));
    }
    let observation_sequence = rows
        .iter()
        .filter_map(|row| row["admitted_sequence"].as_u64())
        .max()
        .unwrap_or(0);
    if body["observation_sequence"].as_u64() != Some(observation_sequence) {
        return Err(StagedStoreError::LifecycleConflict(
            "snapshot observation sequence",
        ));
    }
    let fences = body["deletion_fences"]
        .as_array()
        .filter(|items| items.len() <= 1024)
        .ok_or(StagedStoreError::InvalidAdvisory(
            "snapshot deletion fences",
        ))?;
    // Validate all fence kinds before calling either mutation helper, including
    // when a malformed typed entry follows a valid legacy entry.
    let parsed_fences = fences
        .iter()
        .map(|fence| {
            let source_key = required_text(fence, "source_key")?;
            let generation =
                fence["generation"]
                    .as_u64()
                    .ok_or(StagedStoreError::InvalidAdvisory(
                        "snapshot fence generation",
                    ))?;
            let typed = match fence.get("kind") {
                None => false,
                Some(Value::String(kind)) if typed_fence_schema && kind == SOURCE_FENCE_KIND => {
                    true
                }
                _ => return Err(StagedStoreError::LifecycleConflict("snapshot fence schema")),
            };
            if typed
                && (source_key.len() != 64
                    || !source_key
                        .bytes()
                        .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte)))
            {
                return Err(StagedStoreError::InvalidAdvisory(
                    "snapshot typed fence digest",
                ));
            }
            sqlite_i64(generation, "deletion generation")?;
            Ok((typed, source_key, generation))
        })
        .collect::<Result<Vec<_>, StagedStoreError>>()?;
    if parsed_fences.iter().any(|(typed, _, _)| *typed) {
        preflight_imported_source_fences(connection, call)?;
    }
    for (typed, source_key, generation) in parsed_fences {
        if typed {
            fence_and_scrub_target_digest(connection, call, source_key, generation)?;
        } else {
            fence_and_scrub_source(connection, &call.exact_scope, source_key, generation)?;
        }
    }
    let mut keep = BTreeSet::new();
    let mut source_targets = BTreeSet::new();
    let mut restored = 0_u64;
    for row in rows {
        call.control
            .snapshot()
            .map_err(StagedStoreError::ControlEnded)?;
        if exact_scope_from_value(row)? != call.exact_scope
            || row["exact_scope_sha256"] != scope
            || row
                .as_object()
                .is_none_or(|object| object.len() != SNAPSHOT_COLUMNS.len())
        {
            return Err(StagedStoreError::LifecycleConflict("snapshot row scope"));
        }
        let reference = required_text(row, "provider_reference")?;
        if !is_staged_provider_reference(reference) || !keep.insert(reference.to_owned()) {
            return Err(StagedStoreError::LifecycleConflict("snapshot target alias"));
        }
        let sequence = row["admitted_sequence"]
            .as_u64()
            .filter(|value| *value > 0)
            .ok_or(StagedStoreError::InvalidAdvisory(
                "snapshot admitted sequence",
            ))?;
        let expected = derive_effect_evidence(
            &scope,
            required_text(row, "idempotency_key")?,
            required_text(row, "payload_sha256")?,
            sequence,
            required_text(row, "operation_id")?,
        );
        if expected.provider_reference != reference
            || row["receipt"] != expected.receipt
            || row["effect_digest"] != expected.effect_digest
        {
            return Err(StagedStoreError::LifecycleConflict(
                "snapshot effect identity",
            ));
        }
        let mut restored_row = row.clone();
        if let Some(source_key) = row["deleted_source_key"].as_str() {
            // Fence inventory was applied by explicit kind above. A scrubbed row
            // marker carries no origin proof and must never create a raw fence.
            if row["tombstone"] != 1
                || !fences.iter().any(|fence| fence["source_key"] == source_key)
            {
                return Err(StagedStoreError::LifecycleConflict(
                    "snapshot deletion marker",
                ));
            }
        }

        if let Some(text) = row["original_source"].as_str() {
            let attribution: Value = serde_json::from_str(text)
                .map_err(|_| StagedStoreError::InvalidAdvisory("snapshot source"))?;
            let source = &attribution["source"];
            if !source_targets.insert(source.to_string()) {
                return Err(StagedStoreError::LifecycleConflict(
                    "snapshot duplicate source target",
                ));
            }
            let source_key = required_text(source, "source_key")?;
            let state = states
                .get(&source.to_string())
                .copied()
                .ok_or(StagedStoreError::LifecycleConflict("source coverage"))?;
            let fresh_privacy = matches!(state, "deleted" | "redacted" | "expired");
            let legacy_privacy = legacy_source_deleted(connection, &call.exact_scope, source_key)?;
            let privacy = fresh_privacy
                || source_deleted(
                    connection,
                    &call.exact_scope,
                    source_key,
                    Some(&attribution),
                )?;
            if privacy {
                let marker = if fresh_privacy || !legacy_privacy {
                    if !recorded_origin_matches_checkout(&attribution, &call.exact_scope) {
                        return Err(StagedStoreError::LifecycleConflict(
                            "restore deletion origin unavailable",
                        ));
                    }
                    let target = NativeSourceTarget::from_source(source)?;
                    let fresh_privacy_admitted = admission
                        .and_then(|admission| admission.restore.as_ref())
                        .is_some_and(|restore| {
                            restore.sources.iter().any(|(identity, disposition)| {
                                super::native_provider::source_identity_matches(source, identity)
                                    && matches!(
                                        disposition.state,
                                        tracedecay_memory_provider_registry::SourceDisposition::Deleted
                                            | tracedecay_memory_provider_registry::SourceDisposition::Redacted
                                            | tracedecay_memory_provider_registry::SourceDisposition::Expired
                                    )
                            })
                        });
                    if fresh_privacy && !fresh_privacy_admitted {
                        return Err(StagedStoreError::LifecycleConflict(
                            "restore deletion authority unavailable",
                        ));
                    }
                    preflight_targeted_deletion(connection, call, std::slice::from_ref(&target))?;
                    fence_and_scrub_target(connection, &call.exact_scope, &target, before)?;
                    target.digest()
                } else {
                    // An imported or resident legacy raw fence keeps its original
                    // broad semantics; typed evidence never enters this branch.
                    fence_and_scrub_source(connection, &call.exact_scope, source_key, before)?;
                    source_key.to_owned()
                };
                restored_row["sanitized_payload"] = Value::Null;
                restored_row["tombstone"] = 1.into();
                restored_row["feedback"] = 0.into();
                restored_row["deleted_source_key"] = marker.into();
                for field in [
                    "original_source",
                    "validity_override",
                    "valid_from_nanos",
                    "valid_until_nanos",
                    "superseded_nanos",
                    "revoked_nanos",
                    "projected_content_sha256",
                ] {
                    restored_row[field] = Value::Null;
                }
                restored_row["feedback_suppressed"] = 0.into();
            } else {
                if !admission.is_some_and(|admission| {
                    admission.history_sources.iter().any(|trusted| {
                        super::native_provider::attribution_matches(
                            &attribution,
                            &trusted.attribution,
                        )
                    })
                }) {
                    return Err(StagedStoreError::LifecycleConflict(
                        "restore attribution authority unavailable",
                    ));
                }
                let mut effective = attribution.clone();
                if let Some(overlay) = restored_row["validity_override"].as_str() {
                    effective["validity"] = serde_json::from_str(overlay).map_err(|_| {
                        StagedStoreError::InvalidAdvisory("snapshot validity overlay")
                    })?;
                }
                let validity = recorded_validity(Some(&effective))?;
                if (state == "revoked" && validity.revoked_at_utc_nanos.is_none())
                    || (state == "superseded" && validity.superseded_at_utc_nanos.is_none())
                {
                    return Err(StagedStoreError::LifecycleConflict(
                        "restore lifecycle evidence",
                    ));
                }
            }
        }
        if !restored_row["sanitized_payload"].is_null() {
            let payload = restored_row["sanitized_payload"]["blob"]
                .as_array()
                .ok_or(StagedStoreError::InvalidAdvisory("snapshot payload"))?
                .iter()
                .map(|byte| {
                    byte.as_u64()
                        .and_then(|value| u8::try_from(value).ok())
                        .ok_or(StagedStoreError::InvalidAdvisory("snapshot payload byte"))
                })
                .collect::<Result<Vec<_>, _>>()?;
            if restored_row["payload_sha256"] != sha256_hex(&payload)
                || extract_message_text(&payload).is_none()
            {
                return Err(StagedStoreError::LifecycleConflict(
                    "snapshot payload integrity",
                ));
            }
            let envelope: Value = serde_json::from_slice(&payload)
                .map_err(|_| StagedStoreError::InvalidAdvisory("snapshot envelope"))?;
            let attribution: Option<Value> = restored_row["original_source"]
                .as_str()
                .map(serde_json::from_str)
                .transpose()
                .map_err(|_| StagedStoreError::InvalidAdvisory("snapshot attribution"))?;
            if envelope.pointer("/source_identity/original_source") != attribution.as_ref()
                || envelope["observation_kind"] != restored_row["observation_kind"]
                || envelope["payload_contract"] != restored_row["payload_contract"]
                || attribution.as_ref().is_some_and(|source| {
                    source["source"]["source_revision"] != restored_row["actual_revision"]
                        || source["source"]["observation_id"] != restored_row["source_event_id"]
                })
            {
                return Err(StagedStoreError::LifecycleConflict(
                    "snapshot source binding",
                ));
            }
            if let Some(expected) = envelope["payload_sha256"].as_str() {
                let canonical = tracedecay_memory_hygiene::canonical_payload_bytes(
                    &envelope["canonical_payload"],
                )
                .map_err(|_| StagedStoreError::InvalidAdvisory("snapshot canonical payload"))?;
                if expected != sha256_hex(&canonical) {
                    return Err(StagedStoreError::LifecycleConflict(
                        "snapshot canonical digest",
                    ));
                }
            }
            let mut effective = attribution
                .clone()
                .unwrap_or_else(|| serde_json::json!({"validity":{}}));
            if let Some(overlay) = restored_row["validity_override"].as_str() {
                effective["validity"] = serde_json::from_str(overlay)
                    .map_err(|_| StagedStoreError::InvalidAdvisory("snapshot validity overlay"))?;
            }
            let validity = recorded_validity(Some(&effective))?;
            restored_row["valid_from_nanos"] = serde_json::json!(validity.valid_from_utc_nanos);
            restored_row["valid_until_nanos"] = serde_json::json!(validity.valid_until_utc_nanos);
            restored_row["superseded_nanos"] = serde_json::json!(validity.superseded_at_utc_nanos);
            restored_row["revoked_nanos"] = serde_json::json!(validity.revoked_at_utc_nanos);
            restored_row["projected_content_sha256"] = serde_json::json!(
                extract_message_text(&payload).map(|text| sha256_hex(text.as_bytes()))
            );
        }
        let existing:Option<(String,String)>=connection.query_row("SELECT idempotency_key,payload_sha256 FROM tdmem_native_staged_observation_v1 WHERE provider_reference=?1",params![reference],|row|Ok((row.get(0)?,row.get(1)?))).optional()?;
        if let Some((key, digest)) = existing {
            if restored_row["idempotency_key"] != key || restored_row["payload_sha256"] != digest {
                return Err(StagedStoreError::LifecycleConflict("snapshot target alias"));
            }
            if restored_row["tombstone"] == 1 {
                connection.execute("UPDATE tdmem_native_staged_observation_v1 SET sanitized_payload=NULL,tombstone=1,feedback=0,feedback_suppressed=0,original_source=NULL,validity_override=NULL,valid_from_nanos=NULL,valid_until_nanos=NULL,superseded_nanos=NULL,revoked_nanos=NULL,projected_content_sha256=NULL WHERE provider_reference=?1",params![reference])?;
            }
            // Existing deletion/retention/correction fences and newer receipts survive rollback.
            continue;
        }
        let sql_values = SNAPSHOT_COLUMNS
            .iter()
            .map(|column| snapshot_sql_value(&restored_row[*column]))
            .collect::<Result<Vec<_>, _>>()?;
        let slots = (1..=SNAPSHOT_COLUMNS.len())
            .map(|index| format!("?{index}"))
            .collect::<Vec<_>>()
            .join(",");
        connection.execute(
            &format!(
                "INSERT INTO tdmem_native_staged_observation_v1 ({}) VALUES ({slots})",
                SNAPSHOT_COLUMNS.join(",")
            ),
            rusqlite::params_from_iter(sql_values),
        )?;
        restored += 1;
    }
    let keep_json = serde_json::to_string(&keep)
        .map_err(|_| StagedStoreError::InvalidAdvisory("snapshot targets"))?;
    connection.execute("UPDATE tdmem_native_staged_observation_v1 SET sanitized_payload=NULL,tombstone=1,feedback=0 WHERE exact_scope_sha256=?1 AND provider_reference NOT IN(SELECT value FROM json_each(?2))",params![scope,keep_json])?;
    let operations = body["operations"]
        .as_array()
        .filter(|items| items.len() <= 1024)
        .ok_or(StagedStoreError::InvalidAdvisory("snapshot operations"))?;
    let mut operation_keys = BTreeSet::new();
    for operation in operations {
        call.control
            .snapshot()
            .map_err(StagedStoreError::ControlEnded)?;
        let key = required_text(operation, "idempotency_key")?;
        let digest = required_text(operation, "request_digest")?;
        let operation_id = required_text(operation, "operation_id")?;
        let from =
            operation["generation_before"]
                .as_u64()
                .ok_or(StagedStoreError::InvalidAdvisory(
                    "snapshot receipt generation",
                ))?;
        let to =
            operation["generation_after"]
                .as_u64()
                .ok_or(StagedStoreError::InvalidAdvisory(
                    "snapshot receipt generation",
                ))?;
        let response = required_text(operation, "response")?;
        let receipt = required_text(operation, "receipt")?;
        if !operation_keys.insert(key)
            || from > to
            || Some(to) > body["generation"].as_u64()
            || receipt != sha256_hex(format!("{scope}:{key}:{digest}:{from}:{to}").as_bytes())
        {
            return Err(StagedStoreError::LifecycleConflict(
                "snapshot receipt integrity",
            ));
        }
        let value: Value = serde_json::from_str(response)
            .map_err(|_| StagedStoreError::InvalidAdvisory("snapshot receipt response"))?;
        if value["provider_receipt_digest"] != receipt
            || value["state_generation_before"] != from
            || value["state_generation_after"] != to
        {
            return Err(StagedStoreError::LifecycleConflict(
                "snapshot receipt response",
            ));
        }
        let existing:Option<String> = connection.query_row("SELECT receipt FROM tdmem_native_operation_v2 WHERE scope=?1 AND idempotency_key=?2",params![scope,key],|row|row.get(0)).optional()?;
        if existing
            .as_deref()
            .is_some_and(|existing| existing != receipt)
        {
            return Err(StagedStoreError::LifecycleConflict(
                "snapshot operation alias",
            ));
        }
        connection.execute(
            "INSERT OR IGNORE INTO tdmem_native_operation_v2 VALUES(?1,?2,?3,?4,?5,?6,?7,?8)",
            params![
                scope,
                key,
                digest,
                operation_id,
                sqlite_i64(from, "snapshot generation_before")?,
                sqlite_i64(to, "snapshot generation_after")?,
                response,
                receipt
            ],
        )?;
    }
    let snapshot_generation = body["generation"]
        .as_u64()
        .ok_or(StagedStoreError::InvalidAdvisory("snapshot generation"))?;
    connection.execute(
        "UPDATE tdmem_native_state_v2 SET generation=?1 WHERE singleton=1",
        params![sqlite_i64(
            before.max(snapshot_generation),
            "snapshot generation"
        )?],
    )?;
    let acknowledged =
        body["acknowledged_sequence"]
            .as_u64()
            .ok_or(StagedStoreError::InvalidAdvisory(
                "snapshot replay sequence",
            ))?;
    connection.execute("INSERT INTO tdmem_native_replay_v2 VALUES(?1,?2) ON CONFLICT(scope) DO UPDATE SET acknowledged_sequence=MAX(acknowledged_sequence,excluded.acknowledged_sequence)",params![scope,sqlite_i64(acknowledged, "snapshot replay sequence")?])?;
    Ok((
        serde_json::json!({"snapshot_id":identity["snapshot_id"],"restored_observation_sequence":body["observation_sequence"],"restored_rows":restored,"warnings":[]}),
        true,
    ))
}

fn snapshot_sql_value(value: &Value) -> Result<rusqlite::types::Value, StagedStoreError> {
    use rusqlite::types::Value as SqlValue;
    Ok(match value {
        Value::Null => SqlValue::Null,
        Value::String(text) => SqlValue::Text(text.clone()),
        Value::Number(number) => {
            if let Some(value) = number.as_i64() {
                SqlValue::Integer(value)
            } else {
                SqlValue::Real(
                    number
                        .as_f64()
                        .ok_or(StagedStoreError::InvalidAdvisory("snapshot number"))?,
                )
            }
        }
        Value::Object(object) => SqlValue::Blob(
            object
                .get("blob")
                .and_then(Value::as_array)
                .ok_or(StagedStoreError::InvalidAdvisory("snapshot blob"))?
                .iter()
                .map(|byte| {
                    byte.as_u64()
                        .and_then(|value| u8::try_from(value).ok())
                        .ok_or(StagedStoreError::InvalidAdvisory("snapshot byte"))
                })
                .collect::<Result<Vec<_>, _>>()?,
        ),
        _ => return Err(StagedStoreError::InvalidAdvisory("snapshot value")),
    })
}

fn replay_advisory(
    transaction: &rusqlite::Transaction<'_>,
    call: &tracedecay_memory_provider_registry::ProviderCall,
    request: &Value,
    retention: StagedRetentionPolicyV1,
) -> Result<(Value, bool), StagedStoreError> {
    let scope = call.exact_scope.exact_scope_sha256();
    let grant = &request["history_grant"];
    if exact_scope_from_value(&grant["destination_scope"])? != call.exact_scope
        || required_text(grant, "authorization_ref").is_err()
    {
        return Err(StagedStoreError::LifecycleConflict("replay history grant"));
    }
    let sources = grant["sources"]
        .as_array()
        .ok_or(StagedStoreError::InvalidAdvisory("replay grant sources"))?;
    let items = request["resolved_observations"]
        .as_array()
        .filter(|items| !items.is_empty() && items.len() <= 4096)
        .ok_or(StagedStoreError::InvalidAdvisory("replay observations"))?;
    let refs = request["observation_batch_refs"]
        .as_array()
        .ok_or(StagedStoreError::InvalidAdvisory("replay receipts"))?;
    let first = request["first_source_sequence"]
        .as_u64()
        .ok_or(StagedStoreError::InvalidAdvisory("replay first"))?;
    let last = request["last_source_sequence"]
        .as_u64()
        .ok_or(StagedStoreError::InvalidAdvisory("replay last"))?;
    let previous: u64 = transaction
        .query_row(
            "SELECT acknowledged_sequence FROM tdmem_native_replay_v2 WHERE scope=?1",
            params![scope],
            |row| sqlite_u64(row, 0),
        )
        .optional()?
        .unwrap_or(0);
    if request["expected_previous_acknowledged_sequence"].as_u64() != Some(previous)
        || last
            .checked_sub(first)
            .and_then(|value| value.checked_add(1))
            != Some(items.len() as u64)
        || (first > previous.saturating_add(1))
    {
        return Err(StagedStoreError::LifecycleConflict("replay sequence gap"));
    }
    let mut applied = 0_u64;
    let mut duplicates = 0_u64;
    let mut already = 0_u64;
    let mut rejected = 0_u64;
    let mut seen = BTreeSet::new();
    for (index, item) in items.iter().enumerate() {
        call.control
            .snapshot()
            .map_err(StagedStoreError::ControlEnded)?;
        let receipt = item
            .get("receipt_ref")
            .ok_or(StagedStoreError::InvalidAdvisory("replay receipt"))?;
        if !refs.contains(receipt) || !seen.insert(receipt.to_string()) {
            return Err(StagedStoreError::LifecycleConflict(
                "replay receipt binding",
            ));
        }
        let envelope = &item["observation"];
        let attribution = envelope
            .pointer("/source_identity/original_source")
            .ok_or(StagedStoreError::InvalidAdvisory("replay attribution"))?;
        validate_attribution(attribution)?;
        if attribution["source_sequence"].as_u64() != Some(first + index as u64)
            || envelope["source_sequence"].as_u64() != Some(first + index as u64)
        {
            return Err(StagedStoreError::LifecycleConflict("replay source order"));
        }
        let admitted = sources
            .iter()
            .find(|source| source.get("attribution") == Some(attribution))
            .ok_or(StagedStoreError::LifecycleConflict(
                "ungranted replay source",
            ))?;
        if !matches!(
            admitted
                .pointer("/current_disposition/state")
                .and_then(Value::as_str),
            Some("available" | "superseded" | "revoked")
        ) {
            rejected += 1;
            continue;
        }
        let original = &attribution["source"];
        if source_deleted(
            transaction,
            &call.exact_scope,
            required_text(original, "source_key")?,
            Some(attribution),
        )? {
            rejected += 1;
            continue;
        }
        let bytes = serde_json::to_vec(envelope)
            .map_err(|_| StagedStoreError::InvalidAdvisory("replay envelope"))?;
        if extract_message_text(&bytes).is_none() {
            return Err(StagedStoreError::InvalidAdvisory("replay evidence"));
        }
        let record = StagedObservationRecord {
            scope: call.exact_scope.clone(),
            idempotency_key: required_text(envelope, "idempotency_key")?.to_owned(),
            source_authority: "host_session".to_owned(),
            source_event_id: required_text(original, "observation_id")?.to_owned(),
            source_revision: original["source_revision"].as_str().map(str::to_owned),
            observation_kind: required_text(envelope, "observation_kind")?.to_owned(),
            payload_contract: required_text(envelope, "payload_contract")?.to_owned(),
            sanitized_payload: bytes,
            operation_id: call.operation_id.clone(),
            request_identity: required_text(envelope, "request_identity")?.to_owned(),
            admitted_at_unix_ms: super::native_provider::unix_millis_now(),
        };
        match stage_in_transaction(transaction, record, retention)? {
            StagedOutcome::Committed(_) => applied += 1,
            StagedOutcome::Duplicate(_) => duplicates += 1,
            StagedOutcome::Conflict {
                reason: StagedConflictReason::SourceIdentityReused { .. },
            } => already += 1,
            StagedOutcome::Conflict { .. } => rejected += 1,
        }
    }
    if last > previous {
        transaction.execute("INSERT INTO tdmem_native_replay_v2 VALUES(?1,?2) ON CONFLICT(scope) DO UPDATE SET acknowledged_sequence=MAX(acknowledged_sequence,excluded.acknowledged_sequence)",params![scope,sqlite_i64(last, "replay last sequence")?])?;
    }
    Ok((
        serde_json::json!({"first_source_sequence":first,"last_source_sequence":last,"acknowledged_sequence":previous.max(last),
        "applied_observations":applied,"duplicate_observations":duplicates,"sources_already_applied":already,"rejected_observations":rejected,
        "effect_unknown_observations":0,"partial":false,"warnings":[]}),
        applied > 0 || last > previous,
    ))
}

pub(super) fn scope_json(scope: &ExactScopeFields) -> Value {
    serde_json::json!({"profile_id":scope.profile_id,"project_id":scope.project_id,"repository_identity":scope.repository_identity,
        "worktree_identity":scope.worktree_identity,"branch_identity":scope.branch_identity,"agent_session_id":scope.agent_session_id,
        "resolved_scope_digest":scope.resolved_scope_digest})
}

impl StagedObservationStore {
    pub(crate) fn has_unknown_validity(
        &self,
        scope: &ExactScopeFields,
    ) -> Result<bool, StagedStoreError> {
        let guard = self.connection()?;
        Ok(guard.query_row("SELECT EXISTS(SELECT 1 FROM tdmem_native_staged_observation_v1 WHERE exact_scope_sha256=?1 AND tombstone=0 AND feedback_suppressed=0 AND valid_from_nanos IS NULL)",params![scope.exact_scope_sha256()],|row|row.get(0))?)
    }
}

fn semantic_observation_digest(bytes: &[u8]) -> Result<String, StagedStoreError> {
    let envelope: Value = serde_json::from_slice(bytes)
        .map_err(|_| StagedStoreError::InvalidAdvisory("observation envelope"))?;
    let value = serde_json::json!({"observation_kind":envelope["observation_kind"],"payload_contract":envelope["payload_contract"],
        "canonical_payload":envelope["canonical_payload"],"original_source":envelope.pointer("/source_identity/original_source")});
    Ok(sha256_hex(value.to_string().as_bytes()))
}
