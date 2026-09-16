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
use tracedecay_memory_provider_registry::{
    ApiError, OwnedExactScope, PayloadSanitizationReceipt, PayloadSanitizationReceiptParts,
};

use tracedecay_memory_observation::{
    AdmittedObservationV1, ObservationIdempotencyKeyV1, SqliteObservationJournal,
};

/// Directory the Native provider owns inside the host-granted provider-state
/// root. Placement only: scope identity is never derived from a path.
const NATIVE_PROVIDER_STATE_DIR_NAME: &str = "native";

/// File name of the staged-observation store.
const STAGED_STORE_FILE_NAME: &str = "staged-observations-v1.sqlite3";

/// The host journal is the authority for the bytes Native receives. Its
/// envelope digest covers the source settlement, transformed provider view,
/// extensions, and sanitization binding. Native keeps the journal outside its
/// private database and re-reads it when a provider-local row is used.
const HOST_OBSERVATION_JOURNAL_FILE_NAME: &str = "memory-observation-journal-v1.sqlite3";

/// Schema version this build writes and understands.
const SCHEMA_VERSION: i64 = 3;

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

/// The hygiene/projection evidence that authenticated one provider view.
///
/// `source_payload_sha256` inside the receipt names the bytes the host's
/// provider projection handed to hygiene. It is a different trust domain from
/// `OriginalSourceIdentity.content_sha256`, which names the complete canonical
/// source observation. Keeping the receipt verbatim lets a reopened Native
/// store revalidate the exact transformed bytes without pretending that a
/// narrowed or redacted provider payload is the original source.
#[derive(Clone, Debug, Eq, PartialEq)]
struct ProviderViewSanitization {
    receipt_json: String,
    extensions_digest: String,
}

/// The immutable host identity that selected one admitted observation.
///
/// A provider-local row cannot reconstruct this from its payload: stream
/// coordinates, settlement proof and provider registration are deliberately
/// outside the transformed provider view. Keep the complete identity attached
/// to the projection result while it is being checked so a source-event scan
/// can never silently choose between two otherwise identical views.
#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd)]
struct HostProjectionIdentity {
    source_authority: String,
    source_event_id: String,
    source_event_revision: u64,
    source_event_sha256: String,
    source_stream: String,
    source_sequence: u64,
    commit_point_id: String,
    settled_at_unix_micros: i64,
    settlement_proof_sha256: String,
    provider_id: String,
    provider_instance_id: String,
    registration_revision: u64,
    ready_receipt_digest: String,
}

/// One host-authenticated projection and the exact canonical key that named
/// it. Correction/replay paths may start with a lifecycle key; those paths are
/// allowed to translate it only after a fresh source admission identifies one
/// unambiguous host envelope.
#[derive(Clone, Debug, Eq, PartialEq)]
struct HostProjection {
    idempotency_key: String,
    identity: HostProjectionIdentity,
    sanitization: ProviderViewSanitization,
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
        self.stage_with_control(record, None, None)
    }

    pub(crate) fn stage_controlled(
        &self,
        record: StagedObservationRecord,
        call: &tracedecay_memory_provider_registry::ProviderCall,
        admission: Option<&tracedecay_memory_provider_registry::CurrentAdvisoryAdmission>,
    ) -> Result<StagedOutcome, StagedStoreError> {
        self.stage_with_control(record, Some(call), admission)
    }

    fn stage_with_control(
        &self,
        record: StagedObservationRecord,
        call: Option<&tracedecay_memory_provider_registry::ProviderCall>,
        admission: Option<&tracedecay_memory_provider_registry::CurrentAdvisoryAdmission>,
    ) -> Result<StagedOutcome, StagedStoreError> {
        let provider_view = match call {
            Some(call) => provider_view_sanitization_for_call(&record, call)?,
            None => direct_provider_view_sanitization(&record.sanitized_payload)?,
        };
        if let Some(call) = call {
            if let Some(host_projection) =
                host_projection_for_record(&self.path, &record, Some(call), admission)?
            {
                if host_projection.sanitization != provider_view {
                    return Err(StagedStoreError::LifecycleConflict(
                        "host projection lineage",
                    ));
                }
            }
        }
        let mut guard = self.connection()?;
        let transaction = guard.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let common_source = serde_json::from_slice::<Value>(&record.sanitized_payload)
            .ok()
            .is_some_and(|envelope| {
                envelope
                    .pointer("/source_identity/original_source")
                    .is_some()
            });
        let outcome = stage_in_transaction(
            &transaction,
            record,
            self.retention,
            &provider_view,
            call,
            admission,
        )?;
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
        self.recall_filtered(scope, query, limit, None, None, None, None, None, "")
    }

    /// Recalls rows for a live Native provider call. The call is retained all
    /// the way down to row validation so a controlled recall cannot use the
    /// direct/legacy journal fallback when host projection lineage is absent.
    pub(crate) fn recall_controlled(
        &self,
        scope: &ExactScopeFields,
        query: &str,
        limit: usize,
        history_grant: Option<&Value>,
        admission: Option<&tracedecay_memory_provider_registry::CurrentAdvisoryAdmission>,
        call: &tracedecay_memory_provider_registry::ProviderCall,
    ) -> Result<Vec<StagedRow>, StagedStoreError> {
        self.recall_filtered(
            scope,
            query,
            limit,
            None,
            history_grant,
            admission,
            None,
            Some(call),
            &call.request_id,
        )
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
        self.recall_temporal_with_admission(
            scope,
            query,
            temporal,
            history_grant,
            None,
            exclusions,
            request_id,
            None,
        )
    }

    pub(crate) fn recall_temporal_with_admission(
        &self,
        scope: &ExactScopeFields,
        query: &str,
        temporal: &tracedecay_memory_provider_registry::OwnedTemporalQuery,
        history_grant: Option<&Value>,
        admission: Option<&tracedecay_memory_provider_registry::CurrentAdvisoryAdmission>,
        exclusions: &tracedecay_memory_provider_registry::OwnedRecallExclusions,
        request_id: &str,
        call: Option<&tracedecay_memory_provider_registry::ProviderCall>,
    ) -> Result<Vec<StagedRow>, StagedStoreError> {
        self.recall_filtered(
            scope,
            query,
            self.retention.maximum_content_rows_per_scope,
            Some(temporal),
            history_grant,
            admission,
            Some(exclusions),
            call,
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
        admission: Option<&tracedecay_memory_provider_registry::CurrentAdvisoryAdmission>,
        exclusions: Option<&tracedecay_memory_provider_registry::OwnedRecallExclusions>,
        call: Option<&tracedecay_memory_provider_registry::ProviderCall>,
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

        // A request grant is only a claim. When the actor has a fresh host
        // admission, rebuild the source list from that admission and use it
        // for both the SQLite allowlist and the final candidate check. This
        // keeps a previously accepted grant from surviving a disposition
        // change between recalls.
        let fresh_sources: Vec<Value> = if let Some(admission) = admission {
            admission
                .history_sources
                .iter()
                .map(|trusted| {
                    let attribution =
                        super::provider_history::source_attribution_json(&trusted.attribution)
                            .map_err(|_| {
                                StagedStoreError::LifecycleConflict("history source attribution")
                            })?;
                    Ok(serde_json::json!({
                        "attribution": attribution,
                        "current_disposition": {
                            "state": trusted.current_disposition.state.as_wire()
                        }
                    }))
                })
                .collect::<Result<_, StagedStoreError>>()?
        } else {
            history_grant
                .and_then(|grant| grant.get("sources"))
                .and_then(Value::as_array)
                .cloned()
                .unwrap_or_default()
        };
        let allowed: Vec<String> = fresh_sources
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
            .collect();
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
                    admitted_sequence, admitted_at_unix_ms, exact_scope_sha256, actual_revision, original_source, feedback, validity_override, \
                    sanitization_receipt_json, sanitization_extensions_digest \
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
            Value::Array(fresh_sources).to_string(),
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
            validate_payload_source_identity(
                &payload,
                original_source.as_ref(),
                &row.get::<_, String>(9)?,
                source_revision.as_deref(),
            )?;
            if let Some(admission) = admission {
                let Some(original_source) = original_source.as_ref() else {
                    // A controlled recall cannot authenticate a source-less
                    // resident row against the fresh host source inventory.
                    continue;
                };
                let mut matching = admission.history_sources.iter().filter(|trusted| {
                    super::native_provider::attribution_matches(
                        original_source,
                        &trusted.attribution,
                    )
                });
                let Some(trusted) = matching.next() else {
                    // Apply the same fresh source allowlist to rows from the
                    // current checkout. Scope equality is not a substitute
                    // for current host attribution.
                    continue;
                };
                if matching.next().is_some() {
                    return Err(StagedStoreError::LifecycleConflict(
                        "history source ambiguity",
                    ));
                }
                if !matches!(
                    trusted.current_disposition.state,
                    tracedecay_memory_provider_registry::SourceDisposition::Available
                        | tracedecay_memory_provider_registry::SourceDisposition::Superseded
                        | tracedecay_memory_provider_registry::SourceDisposition::Revoked
                ) {
                    continue;
                }
            }
            let sanitization_receipt_json: Option<String> = row.get(27)?;
            let sanitization_extensions_digest: Option<String> = row.get(28)?;
            match (
                sanitization_receipt_json.as_deref(),
                sanitization_extensions_digest.as_deref(),
            ) {
                (Some(receipt_json), Some(extensions_digest)) => {
                    validate_host_projection_for_row(
                        &self.path,
                        &stored_scope,
                        &idempotency_key,
                        &row.get::<_, String>(8)?,
                        &row.get::<_, String>(9)?,
                        row.get::<_, Option<String>>(10)?.as_deref(),
                        &row.get::<_, String>(11)?,
                        &row.get::<_, String>(12)?,
                        &payload,
                        &payload_sha256,
                        receipt_json,
                        extensions_digest,
                        call,
                        admission,
                    )?;
                }
                (None, None) if call.is_none() && !has_host_projection_journal(&self.path) => {}
                (None, None) => {
                    return Err(StagedStoreError::LifecycleConflict(
                        "host projection lineage unavailable",
                    ));
                }
                _ => {
                    return Err(StagedStoreError::LifecycleConflict(
                        "host projection lineage",
                    ));
                }
            }
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
    if version < 3 {
        transaction.execute_batch(
            "ALTER TABLE tdmem_native_staged_observation_v1 ADD COLUMN sanitization_receipt_json TEXT;
             ALTER TABLE tdmem_native_staged_observation_v1 ADD COLUMN sanitization_extensions_digest TEXT;",
        )?;
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
         SET sanitized_payload = NULL, tombstone = 1, sanitization_receipt_json = NULL, \
             sanitization_extensions_digest = NULL \
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

    fn attributed_source(
        observation: &Value,
    ) -> tracedecay_memory_provider_registry::SourceAttribution {
        use tracedecay_memory_provider_registry::{
            OriginScopeEvidence, OriginalSourceIdentity, SourceAttribution,
        };
        let original = &observation["source_identity"]["original_source"];
        let source = &original["source"];
        SourceAttribution {
            source: OriginalSourceIdentity {
                canonical_provider_id: tracedecay_memory_provider_registry::OwnedProviderId::new(
                    source["canonical_provider_id"].as_str().expect("provider"),
                )
                .expect("provider id"),
                canonical_session_id: source["canonical_session_id"]
                    .as_str()
                    .expect("session")
                    .to_owned(),
                source_key: source["source_key"]
                    .as_str()
                    .expect("source key")
                    .to_owned(),
                stable_record_id: source["stable_record_id"].as_str().map(str::to_owned),
                observation_id: source["observation_id"]
                    .as_str()
                    .expect("observation")
                    .to_owned(),
                source_revision: source["source_revision"].as_str().map(str::to_owned),
                content_sha256: source["content_sha256"]
                    .as_str()
                    .expect("content digest")
                    .to_owned(),
            },
            origin_scope: OriginScopeEvidence::Recorded {
                scope: exact_scope_from_value(&original["origin_scope"]["exact_scope_identity"])
                    .expect("origin scope"),
                authority_ref: original["origin_scope"]["authority_ref"]
                    .as_str()
                    .expect("origin authority")
                    .to_owned(),
            },
            source_sequence: original["source_sequence"].as_u64().expect("sequence"),
            occurred_at_utc_nanos: original["occurred_at"]
                .as_str()
                .and_then(super::super::native_provider::parse_rfc3339_nanos),
            ingested_at_utc_nanos: super::super::native_provider::parse_rfc3339_nanos(
                original["ingested_at"].as_str().expect("ingested at"),
            )
            .expect("ingested timestamp"),
            validity: recorded_validity(Some(original)).expect("validity"),
        }
    }

    fn locator_target(
        delivery_scope: &ExactScopeFields,
        observation: &Value,
        locator: &str,
    ) -> Value {
        let original = &observation["source_identity"]["original_source"];
        json!({
            "provider_id":"tracedecay.native",
            "registration_revision":1,
            "original_scope":original["origin_scope"],
            "delivery_scope":scope_json(delivery_scope),
            "source":original["source"],
            "reference":{"kind":"retained_source_locator","reference":locator}
        })
    }

    fn admission_for(
        call: &tracedecay_memory_provider_registry::ProviderCall,
        sources: &[tracedecay_memory_provider_registry::SourceAttribution],
    ) -> tracedecay_memory_provider_registry::CurrentAdvisoryAdmission {
        use tracedecay_memory_provider_registry::{
            CurrentAdvisoryAdmission, CurrentSourceDisposition, GrantedHistorySource,
            SourceDisposition,
        };
        let disposition = CurrentSourceDisposition {
            state: SourceDisposition::Available,
            authority_ref: "fixture.current-source".to_owned(),
            authority_revision: Some(7),
            checked_at_utc_nanos: 1_750_000_000_000_000_000,
        };
        CurrentAdvisoryAdmission::new(
            call,
            sources
                .iter()
                .cloned()
                .map(|attribution| GrantedHistorySource {
                    attribution,
                    current_disposition: disposition.clone(),
                })
                .collect(),
            None,
        )
        .expect("fixture admission")
    }

    fn attributed_record(
        delivery_scope: &ExactScopeFields,
        observation: &Value,
        key: &str,
    ) -> StagedObservationRecord {
        let source = &observation["source_identity"]["original_source"]["source"];
        let mut record = record(
            delivery_scope,
            key,
            source["observation_id"].as_str().expect("observation id"),
            1,
            "ignored fixture text",
        );
        record.source_revision = source["source_revision"].as_str().map(str::to_owned);
        record.sanitized_payload = serde_json::to_vec(observation).expect("observation bytes");
        record
    }

    /// Builds a source envelope whose canonical payload contains a message and
    /// a second fact, then builds the provider view after eligibility narrows
    /// it to the message fact. The source attribution keeps the digest of the
    /// complete source canonical payload, while the delivered envelope carries
    /// the independent provider-view digest.
    fn mixed_fact_source_and_provider_view(origin: &ExactScopeFields) -> (Value, Value) {
        use tracedecay_memory_conformance::compatibility::{T1, common_observation};

        let mut source = common_observation(
            origin,
            1,
            Some("r1"),
            "mixed-fact provider view",
            Some(T1),
            None,
        );
        let message = json!({
            "kind":"message",
            "role":"user",
            "content":{"text":"eligible mixed-fact message"}
        });
        let source_canonical = json!({
            "facts":[
                message.clone(),
                {"kind":"tool_result","tool":"private.lookup","content":{"secret":"source-only"}}
            ]
        });
        let source_canonical_bytes =
            tracedecay_memory_hygiene::canonical_payload_bytes(&source_canonical)
                .expect("source canonical bytes");
        let source_digest = sha256_hex(&source_canonical_bytes);
        source["canonical_payload"] = source_canonical.into();
        source["payload_sha256"] = source_digest.clone().into();
        source["source_identity"]["original_source"]["source"]["content_sha256"] =
            source_digest.into();

        let mut provider_view = source.clone();
        let provider_canonical = json!({"facts":[message]});
        let provider_canonical_bytes =
            tracedecay_memory_hygiene::canonical_payload_bytes(&provider_canonical)
                .expect("provider canonical bytes");
        provider_view["canonical_payload"] = provider_canonical.into();
        provider_view["payload_sha256"] = sha256_hex(&provider_canonical_bytes).into();
        (source, provider_view)
    }

    /// Rewrites a staged row's provider-view proof to an authenticated
    /// redacted receipt. The receipt's source digest names the projected bytes
    /// the sanitizer read; it is deliberately different from the source
    /// attribution's complete canonical digest.
    fn install_redacted_provider_view_receipt(
        staged: &StagedObservationStore,
        idempotency_key: &str,
        projected_source: &Value,
    ) -> tracedecay_memory_provider_registry::PayloadSanitizationReceipt {
        use tracedecay_memory_provider_registry::{
            PayloadSanitizationReceipt, PayloadSanitizationReceiptParts, SanitizationDisposition,
        };

        let connection = staged.connection().expect("redaction receipt connection");
        let (payload_sha256, extensions_digest): (String, String) = connection
            .query_row(
                "SELECT payload_sha256, sanitization_extensions_digest
                 FROM tdmem_native_staged_observation_v1 WHERE idempotency_key=?1",
                params![idempotency_key],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .expect("staged provider-view evidence");
        let source_bytes = tracedecay_memory_hygiene::canonical_payload_bytes(projected_source)
            .expect("projected source bytes");
        let receipt = PayloadSanitizationReceipt::new(PayloadSanitizationReceiptParts {
            sanitizer_revision: "fixture.provider-view-redaction.v1".to_owned(),
            source_payload_sha256: sha256_hex(&source_bytes),
            sanitized_payload_sha256: payload_sha256,
            extensions_digest,
            disposition: SanitizationDisposition::Redacted,
            finding_count: 1,
            findings_digest: sha256_hex(b"fixture.provider-view-redaction.finding"),
        })
        .expect("redacted provider-view receipt");
        connection
            .execute(
                "UPDATE tdmem_native_staged_observation_v1
                 SET sanitization_receipt_json=?1, sanitization_extensions_digest=?2
                 WHERE idempotency_key=?3",
                params![
                    receipt.to_json(),
                    receipt.extensions_digest(),
                    idempotency_key
                ],
            )
            .expect("store redacted provider-view receipt");
        receipt
    }

    /// Installs the immutable host journal envelope that authenticated a
    /// direct fixture row. The provider-local row can copy this receipt, but
    /// it cannot mint a second host envelope for rewritten payload bytes.
    fn install_host_projection(root: &TempDir, record: &StagedObservationRecord) {
        use tracedecay_memory_provider_registry::{
            PayloadSanitizationReceipt, PayloadSanitizationReceiptParts,
        };

        let receipt =
            PayloadSanitizationReceipt::new(PayloadSanitizationReceiptParts::accepted_unmodified(
                "native-direct-stage.v1",
                sha256_hex(&record.sanitized_payload),
            ))
            .expect("host fixture sanitization receipt");
        install_host_projection_with_receipt(root, record, &receipt);
    }

    fn install_host_projection_with_receipt(
        root: &TempDir,
        record: &StagedObservationRecord,
        receipt: &tracedecay_memory_provider_registry::PayloadSanitizationReceipt,
    ) {
        use tracedecay_memory_observation::{
            AdmittedObservationV1, CanonicalSettlementReceiptV1, ForgetSourceKeyV1,
            ObservationIdV1, ObservationPrivacyV1, PrivacyClassificationV1, ProvenanceOriginV1,
            ProviderTargetV1, RetentionClassV1, SanitizationBindingV1, SourceAuthorityV1,
            SourceSequenceV1, SourceStreamIdV1, extensions_digest,
        };
        use tracedecay_memory_provider_registry::{
            CanonicalPayload, OwnedProviderId, OwnedVersionedId,
        };

        let payload_sha256 = sha256_hex(&record.sanitized_payload);
        let payload = CanonicalPayload::new(
            OwnedVersionedId::new(record.payload_contract.clone()).expect("payload contract"),
            record.sanitized_payload.clone(),
            payload_sha256,
        )
        .expect("host fixture payload");
        let extensions = Vec::new();
        let extensions_digest = extensions_digest(&extensions).expect("host fixture extensions");
        let mut admitted = AdmittedObservationV1 {
            observation_id: ObservationIdV1::from_v7_parts(1_750_000_000_000, [7; 10])
                .expect("host fixture observation id"),
            idempotency_key: ObservationIdempotencyKeyV1::parse(&"0".repeat(64))
                .expect("temporary host fixture key"),
            target: ProviderTargetV1 {
                provider_id: OwnedProviderId::new("tracedecay.native")
                    .expect("host fixture provider"),
                provider_instance_id: "tracedecay.native.project".to_owned(),
                registration_revision: 1,
                ready_receipt_digest: "a".repeat(64),
            },
            exact_scope: record.scope.clone(),
            source: CanonicalSettlementReceiptV1 {
                source_authority: SourceAuthorityV1::HostSession,
                commit_point_id: "fixture.host.commit".to_owned(),
                source_event_id: record.source_event_id.clone(),
                source_event_revision: 1,
                source_event_sha256: sha256_hex(&record.sanitized_payload),
                source_stream: SourceStreamIdV1::new("fixture.host.stream")
                    .expect("host fixture stream"),
                source_sequence: SourceSequenceV1(1),
                settled_at_unix_micros: 1_750_000_000_000_000,
                settlement_proof_sha256: "b".repeat(64),
            },
            observation_kind: OwnedVersionedId::new(record.observation_kind.clone())
                .expect("observation kind"),
            payload,
            extensions,
            extensions_digest: extensions_digest.clone(),
            provenance_origin: ProvenanceOriginV1::Agent,
            provenance_sha256: "c".repeat(64),
            privacy: ObservationPrivacyV1 {
                classification: PrivacyClassificationV1::Sensitive,
                retention_class: RetentionClassV1::Session,
                redaction_revision: 1,
                content_policy_revision: 1,
                forget_source_key: ForgetSourceKeyV1::new(format!(
                    "fixture:{}",
                    record.source_event_id
                ))
                .expect("host fixture forget key"),
                expires_at_unix_micros: 1_750_000_002_000_000,
            },
            sanitization: SanitizationBindingV1 {
                receipt_id: receipt.receipt_id().to_owned(),
                sanitizer_revision: receipt.sanitizer_revision().to_owned(),
                source_payload_sha256: receipt.source_payload_sha256().to_owned(),
                receipt_json: receipt.to_json(),
            },
            occurred_at_unix_micros: 1_750_000_000_000_000,
            admitted_at_unix_micros: 1_750_000_000_000_100,
            deadline_unix_micros: 1_750_000_002_000_000,
            request_id: "fixture.host.request".to_owned(),
            envelope_sha256: String::new(),
        };
        admitted.idempotency_key = admitted.derive_idempotency_key();
        admitted.envelope_sha256 = admitted.expected_envelope_sha256();
        admitted.validate().expect("valid host fixture envelope");

        let journal_path = root.path().join(HOST_OBSERVATION_JOURNAL_FILE_NAME);
        let journal = SqliteObservationJournal::open(
            &journal_path,
            super::super::observation_journey::ObservationJourneyPolicyV1::project_default()
                .retention,
        )
        .expect("host fixture journal");
        journal
            .append_admitted_at(&admitted, 1_750_000_000_000_200)
            .expect("append host fixture envelope");
    }

    #[test]
    fn retained_locator_rejects_fully_recomputed_provider_view_against_host_lineage() {
        use tracedecay_memory_conformance::compatibility::{T1, common_observation};
        use tracedecay_memory_provider_registry::{
            PayloadSanitizationReceipt, PayloadSanitizationReceiptParts, ProviderOperation,
        };

        let root = TempDir::new().expect("host-lineage root");
        let origin = scope("session.host-lineage-origin");
        let delivery = scope("session.host-lineage-delivery");
        let observation = common_observation(
            &origin,
            1,
            Some("r1"),
            "host-authenticated provider view",
            Some(T1),
            None,
        );
        let source = attributed_source(&observation);
        let staged = store(&root, 8);
        let record = attributed_record(&origin, &observation, "locator.host-lineage");
        staged
            .stage_or_duplicate(record.clone())
            .expect("stage provider-local row");
        install_host_projection(&root, &record);

        // Rewrite the canonical provider view and recompute every digest a
        // malicious provider-local writer can reach, including a fresh public
        // sanitization receipt. The immutable host envelope still names the
        // original bytes and must reject the retained locator resolution.
        let connection = staged.connection().expect("tamper connection");
        let original_payload: Vec<u8> = connection
            .query_row(
                "SELECT sanitized_payload FROM tdmem_native_staged_observation_v1 WHERE idempotency_key=?1",
                params!["locator.host-lineage"],
                |row| row.get(0),
            )
            .expect("stored provider view");
        let mut tampered: Value = serde_json::from_slice(&original_payload).expect("envelope");
        tampered["canonical_payload"]["content"] = "forged provider-local content".into();
        let canonical =
            tracedecay_memory_hygiene::canonical_payload_bytes(&tampered["canonical_payload"])
                .expect("forged canonical bytes");
        tampered["payload_sha256"] = sha256_hex(&canonical).into();
        let sanitized = serde_json::to_vec(&tampered).expect("forged envelope bytes");
        let payload_sha256 = sha256_hex(&sanitized);
        let semantic_sha256 = semantic_observation_digest(&sanitized).expect("semantic digest");
        let projected_content_sha256 =
            extract_message_text(&sanitized).map(|text| sha256_hex(text.as_bytes()));
        let receipt =
            PayloadSanitizationReceipt::new(PayloadSanitizationReceiptParts::accepted_unmodified(
                "attacker-recomputed-receipt.v1",
                payload_sha256.clone(),
            ))
            .expect("forged public receipt");
        let (sequence, operation_id): (i64, String) = connection
            .query_row(
                "SELECT admitted_sequence, operation_id FROM tdmem_native_staged_observation_v1 WHERE idempotency_key=?1",
                params!["locator.host-lineage"],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .expect("row evidence");
        let evidence = derive_effect_evidence(
            &origin.exact_scope_sha256(),
            "locator.host-lineage",
            &payload_sha256,
            u64::try_from(sequence).expect("sequence"),
            &operation_id,
        );
        connection
            .execute(
                "UPDATE tdmem_native_staged_observation_v1
                 SET sanitized_payload=?1, payload_sha256=?2, semantic_sha256=?3,
                     projected_content_sha256=?4, provider_reference=?5,
                     receipt=?6, effect_digest=?7, sanitization_receipt_json=?8,
                     sanitization_extensions_digest=?9
                 WHERE idempotency_key=?10",
                params![
                    sanitized,
                    payload_sha256,
                    semantic_sha256,
                    projected_content_sha256,
                    evidence.provider_reference,
                    evidence.receipt,
                    evidence.effect_digest,
                    receipt.to_json(),
                    receipt.extensions_digest(),
                    "locator.host-lineage",
                ],
            )
            .expect("recompute provider-local evidence");
        drop(connection);

        let target = locator_target(&delivery, &observation, "recall-memory-ref-v1:host-lineage");
        let request = feedback_request_for(target);
        let call = lifecycle_call(
            &delivery,
            1,
            ProviderOperation::Feedback,
            "locator.host-lineage-feedback",
            staged.generation().expect("generation"),
            &request,
        );
        let admission = admission_for(&call, std::slice::from_ref(&source));
        assert!(matches!(
            staged.control(&call, Some(&admission)),
            Err(StagedStoreError::LifecycleConflict(
                "host projection lineage"
            ))
        ));
        assert_eq!(
            staged
                .connection()
                .expect("post-rejection connection")
                .query_row(
                    "SELECT feedback FROM tdmem_native_staged_observation_v1 WHERE idempotency_key=?1",
                    params!["locator.host-lineage"],
                    |row| row.get::<_, f64>(0),
                )
                .expect("unchanged feedback"),
            0.0
        );
    }

    #[test]
    fn retained_locator_rejects_fully_recomputed_provider_view_without_host_lineage() {
        use tracedecay_memory_conformance::compatibility::{T1, common_observation};
        use tracedecay_memory_provider_registry::{
            PayloadSanitizationReceipt, PayloadSanitizationReceiptParts, ProviderOperation,
        };

        let root = TempDir::new().expect("host-lineage absence root");
        let origin = scope("session.host-lineage-absence-origin");
        let delivery = scope("session.host-lineage-absence-delivery");
        let observation = common_observation(
            &origin,
            1,
            Some("r1"),
            "host lineage must be present",
            Some(T1),
            None,
        );
        let source = attributed_source(&observation);
        let staged = store(&root, 8);
        let record = attributed_record(&origin, &observation, "locator.host-lineage-absence");
        staged
            .stage_or_duplicate(record.clone())
            .expect("stage provider-local row");

        // Recompute every provider-local value after changing the transformed
        // view. With no sibling host journal, even a self-consistent forged
        // receipt has no authenticated source-to-projection edge.
        let connection = staged.connection().expect("tamper connection");
        let original_payload: Vec<u8> = connection
            .query_row(
                "SELECT sanitized_payload FROM tdmem_native_staged_observation_v1 WHERE idempotency_key=?1",
                params!["locator.host-lineage-absence"],
                |row| row.get(0),
            )
            .expect("stored provider view");
        let mut tampered: Value = serde_json::from_slice(&original_payload).expect("envelope");
        tampered["canonical_payload"]["content"] = "forged without host lineage".into();
        let canonical =
            tracedecay_memory_hygiene::canonical_payload_bytes(&tampered["canonical_payload"])
                .expect("forged canonical bytes");
        tampered["payload_sha256"] = sha256_hex(&canonical).into();
        let sanitized = serde_json::to_vec(&tampered).expect("forged envelope bytes");
        let payload_sha256 = sha256_hex(&sanitized);
        let semantic_sha256 = semantic_observation_digest(&sanitized).expect("semantic digest");
        let projected_content_sha256 =
            extract_message_text(&sanitized).map(|text| sha256_hex(text.as_bytes()));
        let receipt =
            PayloadSanitizationReceipt::new(PayloadSanitizationReceiptParts::accepted_unmodified(
                "attacker-recomputed-receipt-without-host.v1",
                payload_sha256.clone(),
            ))
            .expect("forged public receipt");
        let (sequence, operation_id): (i64, String) = connection
            .query_row(
                "SELECT admitted_sequence, operation_id FROM tdmem_native_staged_observation_v1 WHERE idempotency_key=?1",
                params!["locator.host-lineage-absence"],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .expect("row evidence");
        let evidence = derive_effect_evidence(
            &origin.exact_scope_sha256(),
            "locator.host-lineage-absence",
            &payload_sha256,
            u64::try_from(sequence).expect("sequence"),
            &operation_id,
        );
        connection
            .execute(
                "UPDATE tdmem_native_staged_observation_v1
                 SET sanitized_payload=?1, payload_sha256=?2, semantic_sha256=?3,
                     projected_content_sha256=?4, provider_reference=?5,
                     receipt=?6, effect_digest=?7, sanitization_receipt_json=?8,
                     sanitization_extensions_digest=?9
                 WHERE idempotency_key=?10",
                params![
                    sanitized,
                    payload_sha256,
                    semantic_sha256,
                    projected_content_sha256,
                    evidence.provider_reference,
                    evidence.receipt,
                    evidence.effect_digest,
                    receipt.to_json(),
                    receipt.extensions_digest(),
                    "locator.host-lineage-absence",
                ],
            )
            .expect("recompute provider-local evidence");
        drop(connection);

        let target = locator_target(
            &delivery,
            &observation,
            "recall-memory-ref-v1:host-lineage-absence",
        );
        let request = feedback_request_for(target);
        let call = lifecycle_call(
            &delivery,
            1,
            ProviderOperation::Feedback,
            "locator.host-lineage-absence-feedback",
            staged.generation().expect("generation"),
            &request,
        );
        let admission = admission_for(&call, std::slice::from_ref(&source));
        assert!(matches!(
            staged.control(&call, Some(&admission)),
            Err(StagedStoreError::LifecycleConflict(
                "host projection lineage unavailable"
            ))
        ));
        assert_eq!(
            staged
                .connection()
                .expect("post-rejection connection")
                .query_row(
                    "SELECT feedback FROM tdmem_native_staged_observation_v1 WHERE idempotency_key=?1",
                    params!["locator.host-lineage-absence"],
                    |row| row.get::<_, f64>(0),
                )
                .expect("unchanged feedback"),
            0.0
        );
        assert!(!has_host_projection_journal(staged.path()));
        assert_eq!(record.source_event_id, source.source.observation_id);
    }

    #[test]
    fn retained_locator_survives_reopen_with_changed_ready_receipt_and_registration() {
        use tracedecay_memory_conformance::compatibility::{T1, common_observation};
        use tracedecay_memory_provider_registry::ProviderOperation;

        let root = TempDir::new().expect("re-handshake root");
        let origin = scope("session.re-handshake-origin");
        let delivery = scope("session.re-handshake-delivery");
        let observation = common_observation(
            &origin,
            1,
            Some("r1"),
            "retained locator survives re-handshake",
            Some(T1),
            None,
        );
        let source = attributed_source(&observation);
        let staged = store(&root, 8);
        let record = attributed_record(&origin, &observation, "locator.re-handshake-source");
        staged
            .stage_or_duplicate(record.clone())
            .expect("stage source");
        install_host_projection(&root, &record);
        let locator = "recall-memory-ref-v1:re-handshake";
        let target = locator_target(&delivery, &observation, locator);
        let request = feedback_request_for(target.clone());

        let mut first_call = lifecycle_call(
            &delivery,
            1,
            ProviderOperation::Feedback,
            "locator.re-handshake-first",
            staged.generation().expect("first generation"),
            &request,
        );
        first_call.ready_receipt_sha256 = "b".repeat(64);
        let first_admission = admission_for(&first_call, std::slice::from_ref(&source));
        let first = staged
            .control(&first_call, Some(&first_admission))
            .expect("first re-handshake feedback");
        assert_eq!(
            first.response["applied_effect"]["retained_source_locator"],
            locator
        );

        drop(staged);
        let reopened = store(&root, 8);
        let mut second_target = target;
        // The opaque locator names the settled source; its old delivery
        // registration remains in the request while the current call has
        // already re-registered with a new revision.
        second_target["registration_revision"] = 1.into();
        let second_request = feedback_request_for(second_target);
        let mut second_call = lifecycle_call(
            &delivery,
            2,
            ProviderOperation::Feedback,
            "locator.re-handshake-second",
            reopened.generation().expect("reopened generation"),
            &second_request,
        );
        second_call.ready_receipt_sha256 = "c".repeat(64);
        let second_admission = admission_for(&second_call, std::slice::from_ref(&source));
        let second = reopened
            .control(&second_call, Some(&second_admission))
            .expect("re-registered locator feedback");
        assert_eq!(
            second.response["applied_effect"]["retained_source_locator"],
            locator
        );
        assert!(
            !second
                .response
                .to_string()
                .contains(&record.idempotency_key)
        );
        assert_eq!(
            reopened
                .connection()
                .expect("feedback connection")
                .query_row(
                    "SELECT feedback FROM tdmem_native_staged_observation_v1 WHERE idempotency_key=?1",
                    params!["locator.re-handshake-source"],
                    |row| row.get::<_, f64>(0),
                )
                .expect("feedback persisted"),
            1.0
        );

        let next_generation = reopened.generation().expect("post-feedback generation");
        let rejected = |name: &str, request: Value| {
            let call = lifecycle_call(
                &delivery,
                2,
                ProviderOperation::Feedback,
                name,
                next_generation,
                &request,
            );
            let admission = admission_for(&call, std::slice::from_ref(&source));
            reopened.control(&call, Some(&admission))
        };
        let mut wrong_provider = second_request.clone();
        wrong_provider["target"]["provider_id"] = "tracedecay.other".into();
        assert!(matches!(
            rejected("locator.re-handshake-wrong-provider", wrong_provider),
            Err(StagedStoreError::LifecycleConflict("target attribution"))
        ));
        let mut wrong_scope = second_request.clone();
        wrong_scope["target"]["delivery_scope"] = scope_json(&scope("session.wrong-scope"));
        assert!(matches!(
            rejected("locator.re-handshake-wrong-scope", wrong_scope),
            Err(StagedStoreError::LifecycleConflict("target attribution"))
        ));
        let mut wrong_source = second_request;
        wrong_source["target"]["source"]["observation_id"] = "observation.wrong".into();
        assert!(matches!(
            rejected("locator.re-handshake-wrong-source", wrong_source),
            Err(StagedStoreError::LifecycleConflict("target source unknown"))
        ));
    }

    #[test]
    fn resident_source_tamper_blocks_deletion_and_recall() {
        use tracedecay_memory_conformance::compatibility::{T1, common_observation};
        use tracedecay_memory_provider_registry::ProviderOperation;

        let root = TempDir::new().expect("resident source tamper root");
        let origin = scope("session.resident-source-tamper");
        let observation_a = common_observation(
            &origin,
            1,
            Some("r1"),
            "source A must remain attributable",
            Some(T1),
            None,
        );
        let observation_b = common_observation(
            &origin,
            2,
            Some("r1"),
            "source B must remain attributable",
            Some(T1),
            None,
        );
        let source_a = attributed_source(&observation_a);
        let record_a = attributed_record(&origin, &observation_a, "resident.source-a");
        let record_b = attributed_record(&origin, &observation_b, "resident.source-b");
        let staged = store(&root, 8);
        staged
            .stage_or_duplicate(record_a.clone())
            .expect("stage source A");
        staged
            .stage_or_duplicate(record_b.clone())
            .expect("stage source B");
        install_host_projection(&root, &record_a);
        install_host_projection(&root, &record_b);

        // Change only the provider-local attribution. The payload and its
        // digest still describe A, while the row now claims B.
        staged
            .connection()
            .expect("tamper connection")
            .execute(
                "UPDATE tdmem_native_staged_observation_v1 SET original_source=?1 WHERE idempotency_key=?2",
                params![observation_b["source_identity"]["original_source"].to_string(), "resident.source-a"],
            )
            .expect("tamper resident attribution");

        let delete_request = json!({
            "forget_source_keys":[source_a.source.source_key],
            "mode":"hard_delete",
            "include_snapshots":true,
            "retention_lock_policy_revision":1,
            "verification_query":"resident source tamper",
        });
        let delete_call = lifecycle_call(
            &origin,
            1,
            ProviderOperation::DeleteBySource,
            "resident.source-tamper-delete",
            staged.generation().expect("delete generation"),
            &delete_request,
        );
        assert!(matches!(
            staged.control(&delete_call, None),
            Err(StagedStoreError::LifecycleConflict(
                "stored source attribution"
            ))
        ));
        assert_eq!(
            staged
                .connection()
                .expect("fence connection")
                .query_row(
                    "SELECT COUNT(*) FROM tdmem_native_deleted_source_v2",
                    [],
                    |row| row.get::<_, i64>(0),
                )
                .expect("deletion fence count"),
            0
        );

        // Recall performs the same payload-to-row attribution check before a
        // candidate can be emitted, including when the row is in this exact
        // checkout and no history grant is needed.
        assert!(matches!(
            staged.recall(&origin, "attributable", 8),
            Err(StagedStoreError::LifecycleConflict(
                "stored source attribution"
            ))
        ));
    }

    fn feedback_request_for(target: Value) -> Value {
        json!({
            "target":target,
            "signal":"helpful",
            "weight":"0.5",
            "canonical_outcome_receipt":"fixture.provider-view.feedback",
            "evidence_refs":[],
            "occurred_at":"2025-01-01T00:00:01Z"
        })
    }

    fn source_influence_request(target: Value, source_key: &str, locator: &str) -> Value {
        json!({
            "view":"source_influence",
            "selector":{"source_key":source_key,"stable_memory_ref":locator},
            "maximum_items":8,
            "maximum_bytes":16384,
            "redaction_policy_revision":1,
            "cursor":null,
            "target":target
        })
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
        let StagedOutcome::Committed(original) = staged
            .stage_or_duplicate(offered.clone())
            .expect("stage original")
        else {
            panic!("expected original commit");
        };
        let source = attributed_source(&observation);
        install_host_projection(&root, &offered);
        let locator = "recall-memory-ref-v1:original";
        let target = json!({
            "provider_id":"tracedecay.native", "registration_revision":1,
            "original_scope":observation["source_identity"]["original_source"]["origin_scope"],
            "delivery_scope":scope_json(&origin),
            "source":observation["source_identity"]["original_source"]["source"],
            "reference":{"kind":"retained_source_locator","reference":locator},
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
        let admission = admission_for(&call, std::slice::from_ref(&source));
        let first = staged
            .control(&call, Some(&admission))
            .expect("first feedback");
        let full_digest =
            sha256_hex(&serde_json::to_vec(&target).expect("full admitted target bytes"));
        assert_eq!(first.response["target_digest"], full_digest);
        assert_ne!(
            first.response["target_digest"],
            sha256_hex(original.provider_reference.as_bytes())
        );
        assert_eq!(
            first.response["applied_effect"]["stable_memory_ref"],
            locator
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
        let duplicate = staged
            .control(&call, Some(&admission))
            .expect("original redelivery");
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
        let later_admission = admission_for(&later_call, std::slice::from_ref(&source));
        let second = staged
            .control(&later_call, Some(&later_admission))
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
        let foreign_admission = admission_for(&foreign_call, std::slice::from_ref(&source));
        assert!(matches!(
            staged.control(&foreign_call, Some(&foreign_admission)),
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
        let validity_admission = admission_for(&validity_call, std::slice::from_ref(&source));
        let validity = staged
            .control(&validity_call, Some(&validity_admission))
            .expect("validity correction");
        assert_eq!(validity.response["target_digest"], full_digest);
        correction["correction_kind"] = "replace_content".into();
        correction["replacement"] =
            common_observation(&origin, 1, Some("r2"), "corrected beacon", Some(T2), None);
        let replacement_observation = correction["replacement"].clone();
        let replacement_source = attributed_source(&replacement_observation);
        let replacement_record =
            attributed_record(&origin, &replacement_observation, "key.replacement");
        install_host_projection(&root, &replacement_record);
        let replacement_call = lifecycle_call(
            &origin,
            1,
            ProviderOperation::Correction,
            "correction.content",
            staged.generation().expect("generation"),
            &correction,
        );
        let replacement_admission =
            admission_for(&replacement_call, &[source.clone(), replacement_source]);
        let replacement = staged
            .control(&replacement_call, Some(&replacement_admission))
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
            .control(&later_call, Some(&later_admission))
            .expect("new receipt redelivery");
        assert!(repeated.duplicate);
        assert_eq!(repeated.response, second.response);
        assert_eq!(repeated.receipt, second.receipt);
        assert_eq!(
            reopened
                .control(&call, Some(&admission))
                .expect("old receipt remains original")
                .response
                .to_string(),
            original_bytes
        );
    }

    #[test]
    fn retained_locator_feedback_correction_and_inspection_survive_reopen() {
        use tracedecay_memory_conformance::compatibility::{T1, T2, common_observation};
        use tracedecay_memory_provider_registry::ProviderOperation;

        let root = TempDir::new().expect("root");
        let origin = scope("session.origin");
        let delivery = scope("session.delivery");
        let observation = common_observation(
            &origin,
            1,
            Some("r1"),
            "retained locator beacon",
            Some(T1),
            None,
        );
        let source = attributed_source(&observation);
        let staged = store(&root, 8);
        let source_record = attributed_record(&origin, &observation, "locator.source");
        let StagedOutcome::Committed(private_effect) = staged
            .stage_or_duplicate(source_record.clone())
            .expect("stage source")
        else {
            panic!("expected source commit");
        };
        install_host_projection(&root, &source_record);
        let locator = "recall-memory-ref-v1:opaque-locator-1";
        let target = locator_target(&delivery, &observation, locator);
        let feedback = json!({
            "target":target,
            "signal":"helpful",
            "weight":"0.5",
            "canonical_outcome_receipt":"fixture.feedback",
            "evidence_refs":[],
            "occurred_at":T1
        });
        let feedback_call = lifecycle_call(
            &delivery,
            1,
            ProviderOperation::Feedback,
            "locator.feedback",
            staged.generation().expect("generation"),
            &feedback,
        );
        let feedback_admission = admission_for(&feedback_call, std::slice::from_ref(&source));
        let first = staged
            .control(&feedback_call, Some(&feedback_admission))
            .expect("locator feedback");
        assert_eq!(
            first.response["applied_effect"]["stable_memory_ref"],
            locator
        );
        assert_eq!(
            first.response["applied_effect"]["retained_source_locator"],
            locator
        );
        assert!(
            !first
                .response
                .to_string()
                .contains(&private_effect.provider_reference)
        );

        drop(staged);
        let reopened = store(&root, 8);
        let mut correction = json!({
            "target":target,
            "correction_kind":"change_validity",
            "replacement":{"valid_from":T1,"valid_until":T2},
            "expected_target_revision":"r1",
            "reason":"reopened correction",
            "evidence_refs":[]
        });
        let correction_call = lifecycle_call(
            &delivery,
            1,
            ProviderOperation::Correction,
            "locator.correction",
            reopened.generation().expect("reopened generation"),
            &correction,
        );
        let correction_admission = admission_for(&correction_call, std::slice::from_ref(&source));
        let corrected = reopened
            .control(&correction_call, Some(&correction_admission))
            .expect("reopened locator correction");
        assert_eq!(
            corrected.response["affected_provider_effects"],
            json!([locator])
        );
        assert!(
            !corrected
                .response
                .to_string()
                .contains(&private_effect.provider_reference)
        );

        correction["correction_kind"] = "replace_content".into();
        let replacement_observation = common_observation(
            &origin,
            1,
            Some("r2"),
            "retained locator correction",
            Some(T2),
            None,
        );
        let replacement_source = attributed_source(&replacement_observation);
        let replacement_record =
            attributed_record(&origin, &replacement_observation, "locator.replacement");
        install_host_projection(&root, &replacement_record);
        correction["replacement"] = replacement_observation;
        let replacement_call = lifecycle_call(
            &delivery,
            1,
            ProviderOperation::Correction,
            "locator.replacement",
            reopened
                .generation()
                .expect("validity correction generation"),
            &correction,
        );
        let replacement_admission =
            admission_for(&replacement_call, &[source.clone(), replacement_source]);
        let replaced = reopened
            .control(&replacement_call, Some(&replacement_admission))
            .expect("reopened locator replacement");
        let replacement_reference: String = reopened
            .connection()
            .expect("replacement connection")
            .query_row(
                "SELECT provider_reference FROM tdmem_native_staged_observation_v1 WHERE actual_revision=?1",
                params!["r2"],
                |row| row.get(0),
            )
            .expect("replacement private reference");
        assert_eq!(replaced.response["affected_provider_effects"], json!(2_u64));
        assert!(
            !replaced
                .response
                .to_string()
                .contains(&private_effect.provider_reference)
        );
        assert!(
            !replaced
                .response
                .to_string()
                .contains(&replacement_reference)
        );

        // Even if a private replacement reference is tampered into the
        // provider-local validity overlay, retained inspection must rebuild a
        // typed summary and redact that private value before it reaches the
        // host response.
        let tampered_replacement_reference =
            format!("{PROVIDER_REFERENCE_PREFIX}{}", "d".repeat(64));
        {
            let connection = reopened.connection().expect("tampered overlay connection");
            let overlay: String = connection
                .query_row(
                    "SELECT validity_override FROM tdmem_native_staged_observation_v1 WHERE idempotency_key=?1",
                    params!["locator.source"],
                    |row| row.get(0),
                )
                .expect("stored correction overlay");
            let mut overlay: Value = serde_json::from_str(&overlay).expect("overlay json");
            overlay["superseded_by"] = tampered_replacement_reference.clone().into();
            connection
                .execute(
                    "UPDATE tdmem_native_staged_observation_v1 SET validity_override=?1 WHERE idempotency_key=?2",
                    params![overlay.to_string(), "locator.source"],
                )
                .expect("tamper replacement reference");
        }

        let inspection = json!({
            "view":"source_influence",
            "selector":{"source_key":source.source.source_key,"stable_memory_ref":locator},
            "maximum_items":8,
            "maximum_bytes":16384,
            "redaction_policy_revision":1,
            "cursor":null,
            "target":target
        });
        let inspection_call = lifecycle_call(
            &delivery,
            1,
            ProviderOperation::Inspection,
            "locator.inspection",
            reopened.generation().expect("post-replacement generation"),
            &inspection,
        );
        let inspection_admission = admission_for(&inspection_call, std::slice::from_ref(&source));
        let inspected = reopened
            .control(&inspection_call, Some(&inspection_admission))
            .expect("reopened locator inspection");
        let item = inspected.response["items"]
            .as_array()
            .expect("inspection items")
            .first()
            .expect("retained influence");
        assert_eq!(
            item["target"]["reference"],
            json!({"kind":"retained_source_locator","reference":locator})
        );
        assert_eq!(item["settled_feedback"]["helpful"], 1);
        assert!(
            !inspected
                .response
                .to_string()
                .contains(&private_effect.provider_reference)
        );
        assert!(
            !inspected
                .response
                .to_string()
                .contains(&replacement_reference)
        );
        assert!(
            !inspected
                .response
                .to_string()
                .contains(&tampered_replacement_reference)
        );
    }

    #[test]
    fn retained_locator_accepts_mixed_fact_provider_projection_for_lifecycle_reads_and_writes() {
        use tracedecay_memory_provider_registry::ProviderOperation;

        let root = TempDir::new().expect("mixed-fact root");
        let origin = scope("session.mixed-origin");
        let delivery = scope("session.mixed-delivery");
        let (source_observation, provider_view) = mixed_fact_source_and_provider_view(&origin);
        let source = attributed_source(&source_observation);
        let staged = store(&root, 8);
        let provider_view_record =
            attributed_record(&origin, &provider_view, "locator.mixed-facts");
        let StagedOutcome::Committed(private_effect) = staged
            .stage_or_duplicate(provider_view_record.clone())
            .expect("stage narrowed provider view")
        else {
            panic!("expected mixed-fact source commit");
        };
        install_host_projection(&root, &provider_view_record);
        assert_ne!(
            provider_view["payload_sha256"],
            source_observation["source_identity"]["original_source"]["source"]["content_sha256"],
            "the provider view must carry a digest independent of the full source"
        );
        let locator = "recall-memory-ref-v1:mixed-facts";
        let target = locator_target(&delivery, &source_observation, locator);

        let feedback_call = lifecycle_call(
            &delivery,
            1,
            ProviderOperation::Feedback,
            "locator.mixed-feedback",
            staged.generation().expect("mixed generation"),
            &feedback_request_for(target.clone()),
        );
        let feedback_admission = admission_for(&feedback_call, std::slice::from_ref(&source));
        let feedback = staged
            .control(&feedback_call, Some(&feedback_admission))
            .expect("mixed-fact feedback");
        assert_eq!(
            feedback.response["applied_effect"]["retained_source_locator"],
            locator
        );
        assert!(
            !feedback
                .response
                .to_string()
                .contains(&private_effect.provider_reference)
        );

        let correction = json!({
            "target":target.clone(),
            "correction_kind":"change_validity",
            "replacement":{"valid_from":"2025-01-01T00:00:01Z","valid_until":"2025-01-01T00:00:02Z"},
            "expected_target_revision":"r1",
            "reason":"mixed-fact validity correction",
            "evidence_refs":[]
        });
        let correction_call = lifecycle_call(
            &delivery,
            1,
            ProviderOperation::Correction,
            "locator.mixed-correction",
            staged.generation().expect("mixed feedback generation"),
            &correction,
        );
        let correction_admission = admission_for(&correction_call, std::slice::from_ref(&source));
        let corrected = staged
            .control(&correction_call, Some(&correction_admission))
            .expect("mixed-fact correction");
        assert_eq!(
            corrected.response["affected_provider_effects"],
            json!([locator])
        );
        assert!(
            !corrected
                .response
                .to_string()
                .contains(&private_effect.provider_reference)
        );

        let inspection =
            source_influence_request(target, source.source.source_key.as_str(), locator);
        let inspection_call = lifecycle_call(
            &delivery,
            1,
            ProviderOperation::Inspection,
            "locator.mixed-inspection",
            staged.generation().expect("mixed correction generation"),
            &inspection,
        );
        let inspection_admission = admission_for(&inspection_call, std::slice::from_ref(&source));
        let inspected = staged
            .control(&inspection_call, Some(&inspection_admission))
            .expect("mixed-fact inspection");
        let item = inspected.response["items"]
            .as_array()
            .and_then(|items| items.first())
            .expect("mixed-fact influence item");
        assert_eq!(
            item["target"]["reference"],
            json!({"kind":"retained_source_locator","reference":locator})
        );
        assert!(
            !inspected
                .response
                .to_string()
                .contains(&private_effect.provider_reference)
        );
    }

    #[test]
    fn retained_locator_accepts_redacted_provider_view_only_with_intact_receipt() {
        use tracedecay_memory_conformance::compatibility::{T1, common_observation};
        use tracedecay_memory_provider_registry::ProviderOperation;

        let root = TempDir::new().expect("redacted root");
        let origin = scope("session.redacted-origin");
        let delivery = scope("session.redacted-delivery");
        let source_observation = common_observation(
            &origin,
            1,
            Some("r1"),
            "credential-bearing source text",
            Some(T1),
            None,
        );
        let mut provider_view = source_observation.clone();
        provider_view["canonical_payload"]["content"] = "[redacted by provider hygiene]".into();
        let provider_canonical =
            tracedecay_memory_hygiene::canonical_payload_bytes(&provider_view["canonical_payload"])
                .expect("redacted provider canonical bytes");
        provider_view["payload_sha256"] = sha256_hex(&provider_canonical).into();
        let source = attributed_source(&source_observation);
        let staged = store(&root, 8);
        let provider_view_record =
            attributed_record(&origin, &provider_view, "locator.redacted-view");
        let StagedOutcome::Committed(private_effect) = staged
            .stage_or_duplicate(provider_view_record.clone())
            .expect("stage redacted provider view")
        else {
            panic!("expected redacted source commit");
        };
        let host_receipt = install_redacted_provider_view_receipt(
            &staged,
            "locator.redacted-view",
            &source_observation,
        );
        install_host_projection_with_receipt(&root, &provider_view_record, &host_receipt);
        assert_ne!(
            provider_view["payload_sha256"],
            source_observation["source_identity"]["original_source"]["source"]["content_sha256"],
            "redaction must keep the transformed digest separate from source attribution"
        );
        let locator = "recall-memory-ref-v1:redacted-view";
        let target = locator_target(&delivery, &source_observation, locator);

        let feedback_call = lifecycle_call(
            &delivery,
            1,
            ProviderOperation::Feedback,
            "locator.redacted-feedback",
            staged.generation().expect("redacted generation"),
            &feedback_request_for(target.clone()),
        );
        let feedback_admission = admission_for(&feedback_call, std::slice::from_ref(&source));
        let feedback = staged
            .control(&feedback_call, Some(&feedback_admission))
            .expect("redacted feedback");
        assert_eq!(
            feedback.response["applied_effect"]["retained_source_locator"],
            locator
        );
        assert!(
            !feedback
                .response
                .to_string()
                .contains(&private_effect.provider_reference)
        );

        let correction = json!({
            "target":target.clone(),
            "correction_kind":"change_validity",
            "replacement":{"valid_from":T1,"valid_until":"2025-01-01T00:00:02Z"},
            "expected_target_revision":"r1",
            "reason":"redacted provider-view correction",
            "evidence_refs":[]
        });
        let correction_call = lifecycle_call(
            &delivery,
            1,
            ProviderOperation::Correction,
            "locator.redacted-correction",
            staged.generation().expect("redacted feedback generation"),
            &correction,
        );
        let correction_admission = admission_for(&correction_call, std::slice::from_ref(&source));
        let corrected = staged
            .control(&correction_call, Some(&correction_admission))
            .expect("redacted correction");
        assert_eq!(
            corrected.response["affected_provider_effects"],
            json!([locator])
        );
        assert!(
            !corrected
                .response
                .to_string()
                .contains(&private_effect.provider_reference)
        );

        let inspection =
            source_influence_request(target.clone(), source.source.source_key.as_str(), locator);
        let inspection_call = lifecycle_call(
            &delivery,
            1,
            ProviderOperation::Inspection,
            "locator.redacted-inspection",
            staged.generation().expect("redacted correction generation"),
            &inspection,
        );
        let inspection_admission = admission_for(&inspection_call, std::slice::from_ref(&source));
        let inspected = staged
            .control(&inspection_call, Some(&inspection_admission))
            .expect("redacted inspection");
        assert_eq!(
            inspected.response["items"][0]["target"]["reference"],
            json!({"kind":"retained_source_locator","reference":locator})
        );
        assert!(
            !inspected
                .response
                .to_string()
                .contains(&private_effect.provider_reference)
        );

        let original_payload: Vec<u8> = staged
            .connection()
            .expect("redacted payload connection")
            .query_row(
                "SELECT sanitized_payload FROM tdmem_native_staged_observation_v1 WHERE idempotency_key=?1",
                params!["locator.redacted-view"],
                |row| row.get(0),
            )
            .expect("redacted payload");
        let mut tampered_payload = original_payload.clone();
        tampered_payload.push(b' ');
        staged
            .connection()
            .expect("tampered payload connection")
            .execute(
                "UPDATE tdmem_native_staged_observation_v1 SET sanitized_payload=?1 WHERE idempotency_key=?2",
                params![tampered_payload, "locator.redacted-view"],
            )
            .expect("tamper transformed bytes");
        let tampered_bytes_call = lifecycle_call(
            &delivery,
            1,
            ProviderOperation::Feedback,
            "locator.redacted-tampered-bytes",
            staged.generation().expect("tampered bytes generation"),
            &feedback_request_for(target.clone()),
        );
        let tampered_bytes_admission =
            admission_for(&tampered_bytes_call, std::slice::from_ref(&source));
        assert!(matches!(
            staged.control(&tampered_bytes_call, Some(&tampered_bytes_admission)),
            Err(StagedStoreError::PayloadDigestMismatch { .. })
        ));

        staged
            .connection()
            .expect("restore payload connection")
            .execute(
                "UPDATE tdmem_native_staged_observation_v1 SET sanitized_payload=?1 WHERE idempotency_key=?2",
                params![original_payload, "locator.redacted-view"],
            )
            .expect("restore transformed bytes");
        staged
            .connection()
            .expect("tampered receipt connection")
            .execute(
                "UPDATE tdmem_native_staged_observation_v1
                 SET sanitization_receipt_json=replace(
                     sanitization_receipt_json,
                     'fixture.provider-view-redaction.v1',
                     'fixture.provider-view-redaction.v2'
                 )
                 WHERE idempotency_key=?1",
                params!["locator.redacted-view"],
            )
            .expect("tamper transformed receipt");
        let tampered_receipt_call = lifecycle_call(
            &delivery,
            1,
            ProviderOperation::Inspection,
            "locator.redacted-tampered-receipt",
            staged.generation().expect("tampered receipt generation"),
            &inspection,
        );
        let tampered_receipt_admission =
            admission_for(&tampered_receipt_call, std::slice::from_ref(&source));
        let tampered_receipt =
            staged.control(&tampered_receipt_call, Some(&tampered_receipt_admission));
        assert!(
            matches!(
                &tampered_receipt,
                Err(StagedStoreError::LifecycleConflict(
                    "target sanitization receipt"
                ))
            ),
            "tampered receipt result: {tampered_receipt:?}"
        );
    }

    #[test]
    fn retained_locator_requires_fresh_admission_and_rejects_mismatch_unknown_and_ambiguity() {
        use tracedecay_memory_conformance::compatibility::{T1, common_observation};
        use tracedecay_memory_provider_registry::ProviderOperation;

        let root = TempDir::new().expect("root");
        let origin = scope("session.origin");
        let delivery = scope("session.delivery");
        let observation = common_observation(
            &origin,
            1,
            Some("r1"),
            "locator authority beacon",
            Some(T1),
            None,
        );
        let source = attributed_source(&observation);
        let staged = store(&root, 8);
        let source_record = attributed_record(&origin, &observation, "locator.source");
        staged
            .stage_or_duplicate(source_record.clone())
            .expect("stage source");
        install_host_projection(&root, &source_record);
        let locator = "recall-memory-ref-v1:opaque-locator-2";
        let target = locator_target(&delivery, &observation, locator);
        let request_for = |target: Value| {
            json!({
                "target":target,
                "signal":"helpful",
                "weight":"0.5",
                "canonical_outcome_receipt":"fixture.feedback",
                "evidence_refs":[],
                "occurred_at":T1
            })
        };

        let no_grant_request = request_for(target.clone());
        let no_grant_call = lifecycle_call(
            &delivery,
            1,
            ProviderOperation::Feedback,
            "locator.no-grant",
            staged.generation().expect("generation"),
            &no_grant_request,
        );
        assert!(matches!(
            staged.control(&no_grant_call, None),
            Err(StagedStoreError::LifecycleConflict(
                "target authority unavailable"
            ))
        ));

        let mut mismatched_target = target.clone();
        mismatched_target["source"]["content_sha256"] = "f".repeat(64).into();
        let mismatch_request = request_for(mismatched_target);
        let mismatch_call = lifecycle_call(
            &delivery,
            1,
            ProviderOperation::Feedback,
            "locator.mismatch",
            staged.generation().expect("generation"),
            &mismatch_request,
        );
        let mismatch_admission = admission_for(&mismatch_call, std::slice::from_ref(&source));
        assert!(matches!(
            staged.control(&mismatch_call, Some(&mismatch_admission)),
            Err(StagedStoreError::LifecycleConflict(
                "target source mismatch"
            ))
        ));

        let mut unknown_target = target.clone();
        unknown_target["source"]["observation_id"] = "observation.unknown".into();
        let unknown_request = request_for(unknown_target);
        let unknown_call = lifecycle_call(
            &delivery,
            1,
            ProviderOperation::Feedback,
            "locator.unknown",
            staged.generation().expect("generation"),
            &unknown_request,
        );
        let unknown_admission = admission_for(&unknown_call, std::slice::from_ref(&source));
        assert!(matches!(
            staged.control(&unknown_call, Some(&unknown_admission)),
            Err(StagedStoreError::LifecycleConflict("target source unknown"))
        ));

        let mut malformed_target = target.clone();
        malformed_target["reference"]["reference"] = " locator".into();
        let malformed_request = request_for(malformed_target);
        let malformed_call = lifecycle_call(
            &delivery,
            1,
            ProviderOperation::Feedback,
            "locator.malformed",
            staged.generation().expect("generation"),
            &malformed_request,
        );
        assert!(matches!(
            staged.control(&malformed_call, None),
            Err(StagedStoreError::InvalidAdvisory("target reference"))
        ));

        let mut ambiguous_source = source.clone();
        ambiguous_source.source.source_key = "source.other".to_owned();
        let ambiguity_call = lifecycle_call(
            &delivery,
            1,
            ProviderOperation::Feedback,
            "locator.ambiguous",
            staged.generation().expect("generation"),
            &request_for(target.clone()),
        );
        let ambiguity_admission =
            admission_for(&ambiguity_call, &[source.clone(), ambiguous_source]);
        assert!(matches!(
            staged.control(&ambiguity_call, Some(&ambiguity_admission)),
            Err(StagedStoreError::LifecycleConflict(
                "target source ambiguous"
            ))
        ));

        // Two delivery rows on the same checkout can carry the same admitted
        // source identity after a restart/session change. A locator must not
        // guess which private Native row to mutate.
        staged
            .stage_or_duplicate(attributed_record(
                &scope("session.other"),
                &observation,
                "locator.source.other",
            ))
            .expect("stage second checkout row");
        let row_ambiguity_call = lifecycle_call(
            &delivery,
            1,
            ProviderOperation::Feedback,
            "locator.row-ambiguous",
            staged.generation().expect("generation after second row"),
            &request_for(target.clone()),
        );
        let row_ambiguity_admission =
            admission_for(&row_ambiguity_call, std::slice::from_ref(&source));
        assert!(matches!(
            staged.control(&row_ambiguity_call, Some(&row_ambiguity_admission)),
            Err(StagedStoreError::LifecycleConflict("target row ambiguous"))
        ));
        let connection = staged.connection().expect("connection");
        assert_eq!(
            connection
                .query_row(
                    "SELECT feedback FROM tdmem_native_staged_observation_v1",
                    [],
                    |row| row.get::<_, f64>(0),
                )
                .expect("unchanged feedback"),
            0.0
        );
    }

    #[test]
    fn retained_locator_duplicate_replay_rejects_missing_or_revoked_grant() {
        use tracedecay_memory_conformance::compatibility::{T1, common_observation};
        use tracedecay_memory_provider_registry::{ProviderOperation, SourceDisposition};

        let root = TempDir::new().expect("root");
        let origin = scope("session.origin");
        let delivery = scope("session.delivery");
        let observation = common_observation(
            &origin,
            1,
            Some("r1"),
            "locator duplicate beacon",
            Some(T1),
            None,
        );
        let source = attributed_source(&observation);
        let staged = store(&root, 8);
        let source_record = attributed_record(&origin, &observation, "locator.duplicate");
        staged
            .stage_or_duplicate(source_record.clone())
            .expect("stage source");
        install_host_projection(&root, &source_record);
        let target = locator_target(
            &delivery,
            &observation,
            "recall-memory-ref-v1:opaque-locator-duplicate",
        );
        let request = json!({
            "target":target,
            "signal":"helpful",
            "weight":"0.5",
            "canonical_outcome_receipt":"fixture.feedback",
            "evidence_refs":[],
            "occurred_at":T1
        });
        let generation = staged.generation().expect("generation");
        let first_call = lifecycle_call(
            &delivery,
            1,
            ProviderOperation::Feedback,
            "locator.duplicate-replay",
            generation,
            &request,
        );
        let first_admission = admission_for(&first_call, std::slice::from_ref(&source));
        staged
            .control(&first_call, Some(&first_admission))
            .expect("initial feedback");

        // The durable response journal is consulted only after a retained
        // locator has passed a fresh authority check. A replay without any
        // grant cannot use the old success as a capability.
        let no_grant_call = lifecycle_call(
            &delivery,
            1,
            ProviderOperation::Feedback,
            "locator.duplicate-replay",
            generation,
            &request,
        );
        assert!(matches!(
            staged.control(&no_grant_call, None),
            Err(StagedStoreError::LifecycleConflict(
                "target authority unavailable"
            ))
        ));

        // A fresh admission whose current source grant has been revoked is
        // equally unable to replay the prior operation. The old journal
        // response remains behind the authority boundary.
        let revoked_call = lifecycle_call(
            &delivery,
            1,
            ProviderOperation::Feedback,
            "locator.duplicate-replay",
            generation,
            &request,
        );
        let mut revoked_admission = admission_for(&revoked_call, std::slice::from_ref(&source));
        revoked_admission.history_sources[0]
            .current_disposition
            .state = SourceDisposition::Deleted;
        assert!(matches!(
            staged.control(&revoked_call, Some(&revoked_admission)),
            Err(StagedStoreError::LifecycleConflict(
                "target source unavailable"
            ))
        ));
        assert_eq!(
            staged
                .connection()
                .expect("feedback connection")
                .query_row(
                    "SELECT feedback FROM tdmem_native_staged_observation_v1",
                    [],
                    |row| row.get::<_, f64>(0),
                )
                .expect("feedback remains committed"),
            0.5
        );
    }

    #[test]
    fn general_replay_duplicate_rechecks_current_source_disposition() {
        use tracedecay_memory_conformance::compatibility::{T1, common_observation};
        use tracedecay_memory_provider_registry::{ProviderOperation, SourceDisposition};

        let root = TempDir::new().expect("root");
        let origin = scope("session.replay-origin");
        let delivery = scope("session.replay-delivery");
        let mut observation = common_observation(
            &origin,
            1,
            Some("r1"),
            "general replay duplicate beacon",
            Some(T1),
            None,
        );
        // The common conformance observation is the host envelope body. Replay
        // additionally needs the admitted observation delivery identity.
        observation["idempotency_key"] = "b".repeat(64).into();
        observation["request_identity"] = "fixture.replay.request".into();
        let source = attributed_source(&observation);
        let staged = store(&root, 8);
        let replay_record = attributed_record(
            &delivery,
            &observation,
            observation["idempotency_key"].as_str().expect("replay key"),
        );
        install_host_projection(&root, &replay_record);
        let attribution = observation["source_identity"]["original_source"].clone();
        let grant = |state: &str| {
            json!({
                "authorization_ref":"fixture.replay",
                "policy_revision":1,
                "destination_scope":scope_json(&delivery),
                "relation":"exact_scope",
                "sources":[{"attribution":attribution.clone(),"current_disposition":{
                    "state":state,"authority_ref":"fixture.current","authority_revision":1,
                    "checked_at":T1
                }}],
                "disposition_checkpoint":{"exact_scope":scope_json(&delivery),
                    "authority_ref":"fixture.checkpoint","authority_revision":1,"checked_at":T1}
            })
        };
        let request = |state: &str| {
            json!({
                "observation_batch_refs":["fixture.replay.receipt.1"],
                "resolved_observations":[{"receipt_ref":"fixture.replay.receipt.1",
                    "observation":observation.clone()}],
                "first_source_sequence":1,
                "last_source_sequence":1,
                "expected_previous_acknowledged_sequence":0,
                "history_grant":grant(state)
            })
        };

        let first_request = request("available");
        let generation = staged.generation().expect("initial generation");
        let first_call = lifecycle_call(
            &delivery,
            1,
            ProviderOperation::Replay,
            "replay.general-duplicate",
            generation,
            &first_request,
        );
        let first_admission = admission_for(&first_call, std::slice::from_ref(&source));
        let first = staged
            .control(&first_call, Some(&first_admission))
            .expect("initial replay");
        assert_eq!(first.response["applied_observations"], 1);

        // Keep the operation key and request body stable. The only change is
        // the fresh host disposition, which must be checked before the old
        // operation-journal success can be returned.
        for state in ["revoked", "deleted", "redacted", "expired"] {
            let retry_request = request(state);
            let retry_call = lifecycle_call(
                &delivery,
                1,
                ProviderOperation::Replay,
                "replay.general-duplicate",
                generation,
                &retry_request,
            );
            let mut retry_admission = admission_for(&retry_call, std::slice::from_ref(&source));
            retry_admission.history_sources[0].current_disposition.state =
                SourceDisposition::from_wire(state).expect("fixture disposition");
            assert!(
                matches!(
                    staged.control(&retry_call, Some(&retry_admission)),
                    Err(StagedStoreError::LifecycleConflict(
                        "replay source unavailable"
                    ))
                ),
                "duplicate replay unexpectedly bypassed {state} disposition"
            );
        }
        assert_eq!(
            staged
                .connection()
                .expect("replay connection")
                .query_row(
                    "SELECT COUNT(*) FROM tdmem_native_staged_observation_v1",
                    [],
                    |row| row.get::<_, i64>(0),
                )
                .expect("replay row count"),
            1
        );
    }

    #[test]
    fn retained_locator_validates_provenance_and_allows_tombstone_inspection_only() {
        use tracedecay_memory_conformance::compatibility::{T1, T2, common_observation};
        use tracedecay_memory_provider_registry::ProviderOperation;

        let tamper_root = TempDir::new().expect("tamper root");
        let origin = scope("session.origin");
        let delivery = scope("session.delivery");
        let observation = common_observation(
            &origin,
            1,
            Some("r1"),
            "locator provenance beacon",
            Some(T1),
            None,
        );
        let source = attributed_source(&observation);
        let tampered = store(&tamper_root, 8);
        let StagedOutcome::Committed(private_effect) = tampered
            .stage_or_duplicate(attributed_record(&origin, &observation, "locator.source"))
            .expect("stage source")
        else {
            panic!("expected source commit");
        };
        {
            let connection = tampered.connection().expect("connection");
            let original: String = connection
                .query_row(
                    "SELECT original_source FROM tdmem_native_staged_observation_v1",
                    [],
                    |row| row.get(0),
                )
                .expect("stored attribution");
            let mut value: Value = serde_json::from_str(&original).expect("attribution json");
            value["source"]["content_sha256"] = "e".repeat(64).into();
            connection
                .execute(
                    "UPDATE tdmem_native_staged_observation_v1 SET original_source=?1",
                    params![value.to_string()],
                )
                .expect("tamper attribution");
        }
        let target = locator_target(
            &delivery,
            &observation,
            "recall-memory-ref-v1:opaque-locator-3",
        );
        let request = json!({
            "target":target,
            "signal":"helpful",
            "weight":"0.5",
            "canonical_outcome_receipt":"fixture.feedback",
            "evidence_refs":[],
            "occurred_at":T1
        });
        let call = lifecycle_call(
            &delivery,
            1,
            ProviderOperation::Feedback,
            "locator.tampered",
            tampered.generation().expect("generation"),
            &request,
        );
        let admission = admission_for(&call, std::slice::from_ref(&source));
        assert!(tampered.control(&call, Some(&admission)).is_err());
        assert_eq!(
            private_effect.provider_reference.len(),
            PROVIDER_REFERENCE_PREFIX.len() + 64
        );

        let malformed_root = TempDir::new().expect("malformed root");
        let malformed = store(&malformed_root, 8);
        let malformed_record = attributed_record(&origin, &observation, "locator.source");
        malformed
            .stage_or_duplicate(malformed_record.clone())
            .expect("stage malformed source");
        install_host_projection(&malformed_root, &malformed_record);
        malformed
            .connection()
            .expect("malformed connection")
            .execute(
                "UPDATE tdmem_native_staged_observation_v1 SET original_source=?1",
                params!["not-json"],
            )
            .expect("tamper malformed attribution");
        let malformed_target = locator_target(
            &delivery,
            &observation,
            "recall-memory-ref-v1:opaque-locator-3-malformed",
        );
        let malformed_request = json!({
            "target":malformed_target,
            "signal":"helpful",
            "weight":"0.5",
            "canonical_outcome_receipt":"fixture.feedback",
            "evidence_refs":[],
            "occurred_at":T1
        });
        let malformed_call = lifecycle_call(
            &delivery,
            1,
            ProviderOperation::Feedback,
            "locator.malformed-stored-source",
            malformed.generation().expect("malformed generation"),
            &malformed_request,
        );
        let malformed_admission = admission_for(&malformed_call, std::slice::from_ref(&source));
        assert!(matches!(
            malformed.control(&malformed_call, Some(&malformed_admission)),
            Err(StagedStoreError::InvalidAdvisory("target source"))
        ));

        let tombstone_root = TempDir::new().expect("tombstone root");
        let staged = store(&tombstone_root, 1);
        let source_record = attributed_record(&origin, &observation, "locator.source");
        let StagedOutcome::Committed(private_effect) = staged
            .stage_or_duplicate(source_record.clone())
            .expect("stage source")
        else {
            panic!("expected tombstone source commit");
        };
        install_host_projection(&tombstone_root, &source_record);
        let replacement_observation = common_observation(
            &origin,
            1,
            Some("r2"),
            "tombstone replacement",
            Some(T2),
            None,
        );
        let replacement_record =
            attributed_record(&origin, &replacement_observation, "locator.replacement");
        let StagedOutcome::Committed(replacement_effect) = staged
            .stage_or_duplicate(replacement_record.clone())
            .expect("stage replacement")
        else {
            panic!("expected tombstone replacement commit");
        };
        install_host_projection(&tombstone_root, &replacement_record);
        staged
            .stage_or_duplicate(record(
                &origin,
                "locator.evict",
                "event.evict",
                1,
                "eviction beacon",
            ))
            .expect("stage eviction row");
        let locator = "recall-memory-ref-v1:opaque-locator-4";
        let target = locator_target(&delivery, &observation, locator);
        let inspection = json!({
            "view":"source_influence",
            "selector":{"source_key":source.source.source_key,"stable_memory_ref":locator},
            "maximum_items":8,
            "maximum_bytes":16384,
            "redaction_policy_revision":1,
            "cursor":null,
            "target":target
        });
        let inspection_call = lifecycle_call(
            &delivery,
            1,
            ProviderOperation::Inspection,
            "locator.tombstone-inspection",
            staged.generation().expect("generation"),
            &inspection,
        );
        let inspection_admission = admission_for(&inspection_call, std::slice::from_ref(&source));
        let inspected = staged
            .control(&inspection_call, Some(&inspection_admission))
            .expect("tombstone inspection");
        let item = inspected.response["items"]
            .as_array()
            .expect("inspection items")
            .first()
            .expect("tombstone item");
        assert_eq!(item["disposition"], "deleted");
        assert_eq!(
            item["target"]["reference"],
            json!({"kind":"retained_source_locator","reference":locator})
        );
        assert!(
            !inspected
                .response
                .to_string()
                .contains(&private_effect.provider_reference)
        );
        assert!(
            !inspected
                .response
                .to_string()
                .contains(&replacement_effect.provider_reference)
        );

        let feedback = json!({
            "target":target,
            "signal":"helpful",
            "weight":"0.5",
            "canonical_outcome_receipt":"fixture.feedback",
            "evidence_refs":[],
            "occurred_at":T1
        });
        let feedback_call = lifecycle_call(
            &delivery,
            1,
            ProviderOperation::Feedback,
            "locator.tombstone-feedback",
            staged.generation().expect("generation"),
            &feedback,
        );
        let feedback_admission = admission_for(&feedback_call, std::slice::from_ref(&source));
        assert!(matches!(
            staged.control(&feedback_call, Some(&feedback_admission)),
            Err(StagedStoreError::LifecycleConflict("target tombstone"))
        ));

        let correction = json!({
            "target":target,
            "correction_kind":"change_validity",
            "replacement":{"valid_from":T1,"valid_until":T2},
            "expected_target_revision":"r1",
            "reason":"tombstone correction",
            "evidence_refs":[]
        });
        let correction_call = lifecycle_call(
            &delivery,
            1,
            ProviderOperation::Correction,
            "locator.tombstone-correction",
            staged.generation().expect("generation"),
            &correction,
        );
        let correction_admission = admission_for(&correction_call, std::slice::from_ref(&source));
        assert!(matches!(
            staged.control(&correction_call, Some(&correction_admission)),
            Err(StagedStoreError::LifecycleConflict("target tombstone"))
        ));

        // Privacy deletion scrubs the attribution itself. The retained
        // locator may still inspect the tombstone through the preserved
        // source event/revision, while the fresh admission supplies the
        // caller-visible attribution and no provider-local handle escapes.
        let scrubbed_root = TempDir::new().expect("scrubbed tombstone root");
        let scrubbed = store(&scrubbed_root, 8);
        let scrubbed_record = attributed_record(&origin, &observation, "locator.scrubbed");
        let StagedOutcome::Committed(scrubbed_effect) = scrubbed
            .stage_or_duplicate(scrubbed_record.clone())
            .expect("stage scrubbed source")
        else {
            panic!("expected scrubbed source commit");
        };
        install_host_projection(&scrubbed_root, &scrubbed_record);
        let scrubbed_generation = scrubbed.generation().expect("scrubbed generation");
        {
            let connection = scrubbed.connection().expect("scrubbed connection");
            super::fence_and_scrub_source(
                &connection,
                &origin,
                &source.source.source_key,
                scrubbed_generation + 1,
            )
            .expect("scrub source");
            assert_eq!(
                connection
                    .query_row(
                        "SELECT original_source FROM tdmem_native_staged_observation_v1",
                        [],
                        |row| row.get::<_, Option<String>>(0),
                    )
                    .expect("scrubbed attribution"),
                None
            );
        }
        let scrubbed_target = locator_target(
            &delivery,
            &observation,
            "recall-memory-ref-v1:opaque-locator-4-scrubbed",
        );
        let scrubbed_inspection = json!({
            "view":"source_influence",
            "selector":{"source_key":source.source.source_key,"stable_memory_ref":"ignored"},
            "maximum_items":8,
            "maximum_bytes":16384,
            "redaction_policy_revision":1,
            "cursor":null,
            "target":scrubbed_target
        });
        let scrubbed_call = lifecycle_call(
            &delivery,
            1,
            ProviderOperation::Inspection,
            "locator.scrubbed-inspection",
            scrubbed_generation,
            &scrubbed_inspection,
        );
        let scrubbed_admission = admission_for(&scrubbed_call, std::slice::from_ref(&source));
        let inspected = scrubbed
            .control(&scrubbed_call, Some(&scrubbed_admission))
            .expect("scrubbed tombstone inspection");
        let item = inspected.response["items"]
            .as_array()
            .expect("scrubbed inspection items")
            .first()
            .expect("scrubbed tombstone item");
        assert_eq!(item["disposition"], "deleted");
        assert_eq!(
            item["target"]["reference"],
            json!({
                "kind":"retained_source_locator",
                "reference":"recall-memory-ref-v1:opaque-locator-4-scrubbed"
            })
        );
        assert!(
            !inspected
                .response
                .to_string()
                .contains(&scrubbed_effect.provider_reference)
        );
    }

    #[test]
    fn retained_locator_rejects_tampered_payload_identity_evidence_and_zero_row_update() {
        use tracedecay_memory_conformance::compatibility::{T1, common_observation};
        use tracedecay_memory_provider_registry::ProviderOperation;

        let feedback_request = |target: Value| {
            json!({
                "target":target,
                "signal":"helpful",
                "weight":"0.5",
                "canonical_outcome_receipt":"fixture.feedback",
                "evidence_refs":[],
                "occurred_at":T1
            })
        };

        // A syntactically valid attribution from another source cannot be
        // swapped into the matched row. The immutable source_event_id and
        // payload source identity still have to agree with it.
        let identity_root = TempDir::new().expect("identity root");
        let origin = scope("session.origin");
        let delivery = scope("session.delivery");
        let observation =
            common_observation(&origin, 1, Some("r1"), "identity target", Some(T1), None);
        let swapped_observation = common_observation(
            &scope("session.other"),
            2,
            Some("r1"),
            "valid swapped attribution",
            Some(T1),
            None,
        );
        let source = attributed_source(&observation);
        let identity_store = store(&identity_root, 8);
        let identity_record = attributed_record(&origin, &observation, "locator.identity");
        let StagedOutcome::Committed(identity_effect) = identity_store
            .stage_or_duplicate(identity_record.clone())
            .expect("stage identity row")
        else {
            panic!("expected identity commit");
        };
        install_host_projection(&identity_root, &identity_record);
        let target = locator_target(
            &delivery,
            &observation,
            "recall-memory-ref-v1:tampered-identity",
        );
        {
            let identity_connection = identity_store.connection().expect("identity connection");
            identity_connection
                .execute(
                    "UPDATE tdmem_native_staged_observation_v1 SET original_source=?1",
                    params![swapped_observation["source_identity"]["original_source"].to_string()],
                )
                .expect("swap valid attribution");
        }
        let identity_call = lifecycle_call(
            &delivery,
            1,
            ProviderOperation::Feedback,
            "locator.valid-attribution-swap",
            identity_store.generation().expect("identity generation"),
            &feedback_request(target.clone()),
        );
        let identity_admission = admission_for(&identity_call, std::slice::from_ref(&source));
        assert!(
            identity_store
                .control(&identity_call, Some(&identity_admission))
                .is_err()
        );
        assert_eq!(
            identity_store
                .connection()
                .expect("identity connection")
                .query_row(
                    "SELECT feedback FROM tdmem_native_staged_observation_v1 WHERE provider_reference=?1",
                    params![identity_effect.provider_reference],
                    |row| row.get::<_, f64>(0),
                )
                .expect("identity feedback"),
            0.0
        );

        // A payload swap is rejected from its stored byte digest before the
        // envelope can be used to derive text or source identity.
        let payload_root = TempDir::new().expect("payload root");
        let payload_store = store(&payload_root, 8);
        let payload_record = attributed_record(&origin, &observation, "locator.payload");
        let StagedOutcome::Committed(payload_effect) = payload_store
            .stage_or_duplicate(payload_record.clone())
            .expect("stage payload row")
        else {
            panic!("expected payload commit");
        };
        install_host_projection(&payload_root, &payload_record);
        {
            let payload_connection = payload_store.connection().expect("payload connection");
            payload_connection
                .execute(
                    "UPDATE tdmem_native_staged_observation_v1 SET sanitized_payload=?1",
                    params![serde_json::to_vec(&swapped_observation).expect("swapped payload")],
                )
                .expect("swap payload");
        }
        let payload_target = locator_target(
            &delivery,
            &observation,
            "recall-memory-ref-v1:tampered-payload",
        );
        let payload_call = lifecycle_call(
            &delivery,
            1,
            ProviderOperation::Feedback,
            "locator.payload-swap",
            payload_store.generation().expect("payload generation"),
            &feedback_request(payload_target),
        );
        let payload_admission = admission_for(&payload_call, std::slice::from_ref(&source));
        assert!(
            payload_store
                .control(&payload_call, Some(&payload_admission))
                .is_err()
        );
        assert_eq!(
            payload_store
                .connection()
                .expect("payload connection")
                .query_row(
                    "SELECT feedback FROM tdmem_native_staged_observation_v1 WHERE provider_reference=?1",
                    params![payload_effect.provider_reference],
                    |row| row.get::<_, f64>(0),
                )
                .expect("payload feedback"),
            0.0
        );

        // Recomputing every Native-local digest and effect field must not make
        // a rewritten canonical payload authoritative. The source's canonical
        // content digest comes from the fresh host attribution and remains the
        // binding that the provider cannot rewrite locally.
        let recomputed_root = TempDir::new().expect("recomputed root");
        let recomputed = store(&recomputed_root, 8);
        let recomputed_record = attributed_record(&origin, &observation, "locator.recomputed");
        recomputed
            .stage_or_duplicate(recomputed_record.clone())
            .expect("stage recomputed row");
        install_host_projection(&recomputed_root, &recomputed_record);
        {
            let connection = recomputed.connection().expect("recomputed connection");
            let original_payload: Vec<u8> = connection
                .query_row(
                    "SELECT sanitized_payload FROM tdmem_native_staged_observation_v1 WHERE idempotency_key=?1",
                    params!["locator.recomputed"],
                    |row| row.get(0),
                )
                .expect("stored recomputed payload");
            let mut tampered: Value =
                serde_json::from_slice(&original_payload).expect("stored recomputed envelope");
            tampered["canonical_payload"]["content"] = "coherent local rewrite".into();
            let canonical =
                tracedecay_memory_hygiene::canonical_payload_bytes(&tampered["canonical_payload"])
                    .expect("tampered canonical payload");
            tampered["payload_sha256"] = sha256_hex(&canonical).into();
            let sanitized = serde_json::to_vec(&tampered).expect("tampered envelope bytes");
            let payload_sha256 = sha256_hex(&sanitized);
            let semantic_sha256 =
                semantic_observation_digest(&sanitized).expect("tampered semantic digest");
            let projected_content_sha256 =
                extract_message_text(&sanitized).map(|text| sha256_hex(text.as_bytes()));
            let evidence = super::derive_effect_evidence(
                &origin.exact_scope_sha256(),
                "locator.recomputed",
                &payload_sha256,
                1,
                "operation.locator.recomputed",
            );
            connection
                .execute(
                    "UPDATE tdmem_native_staged_observation_v1
                     SET sanitized_payload=?1, payload_sha256=?2, semantic_sha256=?3,
                         projected_content_sha256=?4, provider_reference=?5,
                         receipt=?6, effect_digest=?7
                     WHERE idempotency_key=?8",
                    params![
                        sanitized,
                        payload_sha256,
                        semantic_sha256,
                        projected_content_sha256,
                        evidence.provider_reference,
                        evidence.receipt,
                        evidence.effect_digest,
                        "locator.recomputed",
                    ],
                )
                .expect("recompute Native-local evidence");
        }
        let recomputed_target = locator_target(
            &delivery,
            &observation,
            "recall-memory-ref-v1:tampered-coherent-local-digests",
        );
        let recomputed_call = lifecycle_call(
            &delivery,
            1,
            ProviderOperation::Feedback,
            "locator.recomputed-feedback",
            recomputed.generation().expect("recomputed generation"),
            &feedback_request(recomputed_target),
        );
        let recomputed_admission = admission_for(&recomputed_call, std::slice::from_ref(&source));
        assert!(matches!(
            recomputed.control(&recomputed_call, Some(&recomputed_admission)),
            Err(StagedStoreError::LifecycleConflict(
                "target sanitization receipt"
            ))
        ));

        // A valid Native reference copied from another row still fails the
        // deterministic evidence check; the locator never follows it.
        let evidence_root = TempDir::new().expect("evidence root");
        let evidence_store = store(&evidence_root, 8);
        let first_evidence_record = attributed_record(&origin, &observation, "locator.evidence.1");
        let StagedOutcome::Committed(_first_effect) = evidence_store
            .stage_or_duplicate(first_evidence_record.clone())
            .expect("stage first evidence row")
        else {
            panic!("expected first evidence commit");
        };
        install_host_projection(&evidence_root, &first_evidence_record);
        let second_origin = scope("session.other");
        let second_observation = common_observation(
            &second_origin,
            2,
            Some("r1"),
            "second evidence row",
            Some(T1),
            None,
        );
        let second_evidence_record =
            attributed_record(&second_origin, &second_observation, "locator.evidence.2");
        let StagedOutcome::Committed(second_effect) = evidence_store
            .stage_or_duplicate(second_evidence_record.clone())
            .expect("stage second evidence row")
        else {
            panic!("expected second evidence commit");
        };
        install_host_projection(&evidence_root, &second_evidence_record);
        {
            let evidence_connection = evidence_store.connection().expect("evidence connection");
            evidence_connection
                .execute(
                    "UPDATE tdmem_native_staged_observation_v1 SET provider_reference=?1 WHERE idempotency_key=?2",
                    params![second_effect.provider_reference, "locator.evidence.1"],
                )
                .expect("swap private reference");
        }
        let evidence_target = locator_target(
            &delivery,
            &observation,
            "recall-memory-ref-v1:tampered-evidence",
        );
        let evidence_call = lifecycle_call(
            &delivery,
            1,
            ProviderOperation::Feedback,
            "locator.private-reference-swap",
            evidence_store.generation().expect("evidence generation"),
            &feedback_request(evidence_target),
        );
        let evidence_admission = admission_for(&evidence_call, std::slice::from_ref(&source));
        assert!(
            evidence_store
                .control(&evidence_call, Some(&evidence_admission))
                .is_err()
        );
        assert_eq!(
            evidence_store
                .connection()
                .expect("evidence connection")
                .query_row(
                    "SELECT feedback FROM tdmem_native_staged_observation_v1 WHERE idempotency_key=?1",
                    params!["locator.evidence.1"],
                    |row| row.get::<_, f64>(0),
                )
                .expect("evidence feedback"),
            0.0
        );

        // validity_override is an untrusted JSON overlay and must remain
        // structurally tied to the recorded validity projection.
        let validity_root = TempDir::new().expect("validity root");
        let validity_store = store(&validity_root, 8);
        let validity_record = attributed_record(&origin, &observation, "locator.validity");
        let StagedOutcome::Committed(validity_effect) = validity_store
            .stage_or_duplicate(validity_record.clone())
            .expect("stage validity row")
        else {
            panic!("expected validity commit");
        };
        install_host_projection(&validity_root, &validity_record);
        {
            let validity_connection = validity_store.connection().expect("validity connection");
            let original: String = validity_connection
                .query_row(
                    "SELECT original_source FROM tdmem_native_staged_observation_v1 WHERE provider_reference=?1",
                    params![validity_effect.provider_reference],
                    |row| row.get(0),
                )
                .expect("validity attribution");
            let original: Value =
                serde_json::from_str(&original).expect("validity attribution json");
            let mut overlay = original["validity"].clone();
            overlay["unexpected_private_field"] = "must be rejected".into();
            validity_connection
                .execute(
                    "UPDATE tdmem_native_staged_observation_v1 SET validity_override=?1 WHERE provider_reference=?2",
                    params![overlay.to_string(), validity_effect.provider_reference],
                )
                .expect("tamper validity overlay");
        }
        let validity_target = locator_target(
            &delivery,
            &observation,
            "recall-memory-ref-v1:tampered-validity",
        );
        let validity_call = lifecycle_call(
            &delivery,
            1,
            ProviderOperation::Feedback,
            "locator.validity-swap",
            validity_store.generation().expect("validity generation"),
            &feedback_request(validity_target),
        );
        let validity_admission = admission_for(&validity_call, std::slice::from_ref(&source));
        assert!(
            validity_store
                .control(&validity_call, Some(&validity_admission))
                .is_err()
        );

        // A trigger that suppresses the write exercises the exactly-one
        // affected-row postcondition. The operation must refuse rather than
        // acknowledge a feedback effect that did not land.
        let update_root = TempDir::new().expect("update root");
        let update_store = store(&update_root, 8);
        let update_record = attributed_record(&origin, &observation, "locator.update");
        update_store
            .stage_or_duplicate(update_record.clone())
            .expect("stage update row");
        install_host_projection(&update_root, &update_record);
        {
            let update_connection = update_store.connection().expect("update connection");
            update_connection
                .execute_batch(
                    "CREATE TRIGGER ignore_native_feedback BEFORE UPDATE OF feedback
                     ON tdmem_native_staged_observation_v1 BEGIN SELECT RAISE(IGNORE); END;",
                )
                .expect("install zero-row trigger");
        }
        let update_target = locator_target(
            &delivery,
            &observation,
            "recall-memory-ref-v1:zero-row-update",
        );
        let update_call = lifecycle_call(
            &delivery,
            1,
            ProviderOperation::Feedback,
            "locator.zero-row-update",
            update_store.generation().expect("update generation"),
            &feedback_request(update_target),
        );
        let update_admission = admission_for(&update_call, std::slice::from_ref(&source));
        assert!(matches!(
            update_store.control(&update_call, Some(&update_admission)),
            Err(StagedStoreError::LifecycleConflict("target row update"))
        ));
        assert_eq!(
            update_store
                .connection()
                .expect("update connection")
                .query_row(
                    "SELECT feedback FROM tdmem_native_staged_observation_v1",
                    [],
                    |row| row.get::<_, f64>(0),
                )
                .expect("unchanged feedback"),
            0.0
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
                    delete_advisory(
                        &connection,
                        &call,
                        &invalid,
                        generation + 1,
                        None,
                        None,
                        staged.path(),
                    ),
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
        // A DeleteBySource call without the in-process history claim is a
        // legacy/source-less control attempt. It must be refused before the
        // durable operation journal or deletion fence can make it effective.
        assert!(matches!(
            staged.control(&call, None),
            Err(StagedStoreError::LifecycleConflict(
                "deletion authority unavailable"
            ))
        ));
        assert_eq!(
            staged
                .recall(&scope, "deletion", 8)
                .expect("source remains recallable"),
            original_rows
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
    if call.operation != ProviderOperation::DeleteBySource {
        return Ok(None);
    }
    // DeleteBySource is a controlled V2 operation. A broad source-key delete
    // without the in-process grant would let a caller fall back to Native's
    // legacy source namespace, so fail before the duplicate operation journal
    // can answer an earlier success.
    if call.history_grant().is_none() {
        return Err(StagedStoreError::LifecycleConflict(
            "deletion authority unavailable",
        ));
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

/// The validity overlay is provider-local state, but it is still interpreted
/// as canonical lifecycle metadata when a retained locator is resolved. Keep
/// its schema closed so an injected JSON key cannot become an unreviewed
/// inspection field or alter a later validity decision.
fn validate_validity_overlay(value: &Value) -> Result<(), StagedStoreError> {
    let object = value
        .as_object()
        .ok_or(StagedStoreError::LifecycleConflict(
            "target validity overlay",
        ))?;
    const ALLOWED: &[&str] = &[
        "valid_from",
        "valid_until",
        "superseded_at",
        "superseded_by",
        "revoked_at",
    ];
    if object
        .keys()
        .any(|field| !ALLOWED.iter().any(|allowed| *allowed == field))
    {
        return Err(StagedStoreError::LifecycleConflict(
            "target validity overlay",
        ));
    }
    Ok(())
}

fn validity_json(validity: &tracedecay_memory_provider_registry::RecordedValidity) -> Value {
    serde_json::json!({
        "valid_from": validity.valid_from_utc_nanos.and_then(super::native_provider::format_rfc3339_nanos),
        "valid_until": validity.valid_until_utc_nanos.and_then(super::native_provider::format_rfc3339_nanos),
        "superseded_at": validity.superseded_at_utc_nanos.and_then(super::native_provider::format_rfc3339_nanos),
        "superseded_by": validity.superseded_by,
        "revoked_at": validity.revoked_at_utc_nanos.and_then(super::native_provider::format_rfc3339_nanos),
    })
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
        // The host admission is part of the current semantic request. Apply
        // it before deriving the idempotency digest or consulting the journal
        // so duplicate lifecycle calls cannot reuse an old success after a
        // source disposition changes.
        if let Some(admission) = admission {
            super::native_provider::apply_current_admission(&mut request, admission);
        }
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
        // Every V2 mutation that names canonical history must prove its live
        // authority before the operation journal is consulted. In particular,
        // a duplicate feedback/correction/delete/replay must not turn an old
        // success into a capability after revocation or alias replacement.
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
        // A lifecycle target is an authority-bearing host claim even when the
        // semantic operation is an idempotent redelivery. Resolve every target
        // form against the fresh admission before consulting the durable
        // response journal. This deliberately rejects legacy stable refs and
        // source-less claims on the controlled V2 path.
        if matches!(
            call.operation,
            ProviderOperation::Feedback | ProviderOperation::Correction
        ) {
            let _ = resolve_target(&transaction, call, &request, admission, false, &self.path)?;
        }
        // Replay is also authority-bearing. Preflight the fresh source
        // dispositions before consulting the general operation journal so a
        // duplicate response cannot bypass a later revoke, privacy deletion,
        // redaction, or expiry decision.
        if call.operation == ProviderOperation::Replay {
            preflight_replay(&transaction, call, &request, admission, &self.path)?;
        }
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
        let inspection_target = if call.operation == ProviderOperation::Inspection
            && request
                .pointer("/target/reference/kind")
                .and_then(Value::as_str)
                == Some("retained_source_locator")
        {
            Some(resolve_target(
                &transaction,
                call,
                &request,
                admission,
                true,
                &self.path,
            )?)
        } else {
            None
        };
        let (mut response, changed) = match call.operation {
            ProviderOperation::Health => {
                let count: u64 = transaction.query_row("SELECT COUNT(*) FROM tdmem_native_staged_observation_v1 WHERE exact_scope_sha256 = ?1", params![scope], |row| sqlite_u64(row, 0))?;
                (
                    serde_json::json!({"readiness":"ready", "state_generation":before, "staged_rows":count, "recovery_state":"complete", "warnings":[]}),
                    false,
                )
            }
            ProviderOperation::Inspection => (
                inspect_advisory(
                    &transaction,
                    call,
                    &request,
                    before,
                    inspection_target.as_ref(),
                )?,
                false,
            ),
            ProviderOperation::Feedback => {
                feedback_advisory(&transaction, call, &request, admission, &self.path)?
            }
            ProviderOperation::Correction => correct_advisory(
                &transaction,
                call,
                &request,
                self.retention,
                admission,
                &self.path,
            )?,
            ProviderOperation::DeleteBySource => delete_advisory(
                &transaction,
                call,
                &request,
                before.saturating_add(1),
                deletion_targets.as_deref(),
                admission,
                &self.path,
            )?,
            ProviderOperation::Maintenance => maintain_advisory(&transaction, call, &request)?,
            ProviderOperation::SnapshotExport => {
                (export_snapshot(&transaction, call, before)?, false)
            }
            ProviderOperation::SnapshotRestore => {
                restore_snapshot(&transaction, call, &request, before, admission, &self.path)?
            }
            ProviderOperation::Replay => replay_advisory(
                &transaction,
                call,
                &request,
                self.retention,
                admission,
                &self.path,
            )?,
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

/// The provider-local row resolved from one lifecycle target.
///
/// `provider_reference` never crosses the Native/provider boundary. It is the
/// SQLite lookup key only. When the host supplied a retained source locator,
/// `outward_reference` is that opaque locator and is the only reference this
/// module may put in a response.
#[derive(Clone, Debug)]
struct ResolvedTarget {
    /// SQLite's immutable row identity for the lifetime of this connection.
    /// Lifecycle mutations must use this identity after resolution; a mutable
    /// provider reference is only evidence that is checked against the row.
    row_id: i64,
    provider_reference: String,
    outward_reference: String,
    retained_source_locator: Option<String>,
    attribution: Option<Value>,
    feedback: f64,
    tombstone: bool,
    target_digest: String,
}

#[derive(Debug)]
struct RetainedLocatorRow {
    row_id: i64,
    exact_scope_sha256: String,
    idempotency_key: String,
    profile_id: String,
    project_id: String,
    repository_identity: String,
    worktree_identity: String,
    branch_identity: String,
    agent_session_id: String,
    resolved_scope_digest: String,
    source_authority: String,
    source_event_id: String,
    actual_revision: Option<String>,
    observation_kind: String,
    payload_contract: String,
    sanitized_payload: Option<Vec<u8>>,
    payload_sha256: String,
    operation_id: String,
    request_identity: String,
    provider_reference: String,
    receipt: String,
    effect_digest: String,
    admitted_sequence: i64,
    admitted_at_unix_ms: i64,
    tombstone: bool,
    original_source: Option<Value>,
    feedback: f64,
    feedback_suppressed: bool,
    validity_override: Option<Value>,
    valid_from_nanos: Option<i64>,
    valid_until_nanos: Option<i64>,
    superseded_nanos: Option<i64>,
    revoked_nanos: Option<i64>,
    semantic_sha256: Option<String>,
    projected_content_sha256: Option<String>,
    deleted_source_key: Option<String>,
    sanitization_receipt_json: Option<String>,
    sanitization_extensions_digest: Option<String>,
}

fn lower_hex_digest(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

fn stored_text(value: &str, limit: usize) -> bool {
    !value.is_empty() && value.len() <= limit && !value.chars().any(char::is_control)
}

fn expected_payload_contract(observation_kind: &str) -> Option<&'static str> {
    match observation_kind {
        STAGED_SESSION_OBSERVATION_KIND => Some(STAGED_SESSION_PAYLOAD_CONTRACT),
        "source.edit_settled.v1" => Some("tracedecay.memory.observation.source-edit.v1"),
        "test.execution_settled.v1" => Some("tracedecay.memory.observation.test-execution.v1"),
        "feedback.outcome_settled.v1" => Some("tracedecay.memory.observation.feedback-outcome.v1"),
        _ => None,
    }
}

fn expected_source_identity_authority(observation_kind: &str) -> Option<&'static str> {
    match observation_kind {
        STAGED_SESSION_OBSERVATION_KIND => Some("host_session"),
        "source.edit_settled.v1" => Some("source_edit"),
        "test.execution_settled.v1" => Some("test_execution"),
        "feedback.outcome_settled.v1" => Some("feedback_outcome"),
        _ => None,
    }
}

fn validate_source_revision_text(source_revision: Option<&str>) -> Result<(), StagedStoreError> {
    match source_revision {
        None => Ok(()),
        Some(value)
            if !value.is_empty()
                && value.trim() == value
                && value.len() <= 1024
                && !value.chars().any(char::is_control) =>
        {
            Ok(())
        }
        Some(_) => Err(StagedStoreError::InvalidAdvisory("target source revision")),
    }
}

fn parse_stored_json(
    text: Option<String>,
    field: &'static str,
) -> Result<Option<Value>, StagedStoreError> {
    text.map(|text| {
        serde_json::from_str(&text).map_err(|_| StagedStoreError::InvalidAdvisory(field))
    })
    .transpose()
}

fn read_retained_locator_row(
    row: &rusqlite::Row<'_>,
) -> Result<RetainedLocatorRow, StagedStoreError> {
    Ok(RetainedLocatorRow {
        row_id: row.get(0)?,
        exact_scope_sha256: row.get(1)?,
        idempotency_key: row.get(2)?,
        profile_id: row.get(3)?,
        project_id: row.get(4)?,
        repository_identity: row.get(5)?,
        worktree_identity: row.get(6)?,
        branch_identity: row.get(7)?,
        agent_session_id: row.get(8)?,
        resolved_scope_digest: row.get(9)?,
        source_authority: row.get(10)?,
        source_event_id: row.get(11)?,
        actual_revision: row.get(12)?,
        observation_kind: row.get(13)?,
        payload_contract: row.get(14)?,
        sanitized_payload: row.get(15)?,
        payload_sha256: row.get(16)?,
        operation_id: row.get(17)?,
        request_identity: row.get(18)?,
        provider_reference: row.get(19)?,
        receipt: row.get(20)?,
        effect_digest: row.get(21)?,
        admitted_sequence: row.get(22)?,
        admitted_at_unix_ms: row.get(23)?,
        tombstone: row.get(24)?,
        original_source: parse_stored_json(row.get(25)?, "target source")?,
        feedback: row.get(26)?,
        feedback_suppressed: row.get(27)?,
        validity_override: parse_stored_json(row.get(28)?, "target validity override")?,
        valid_from_nanos: row.get(29)?,
        valid_until_nanos: row.get(30)?,
        superseded_nanos: row.get(31)?,
        revoked_nanos: row.get(32)?,
        semantic_sha256: row.get(33)?,
        projected_content_sha256: row.get(34)?,
        deleted_source_key: row.get(35)?,
        sanitization_receipt_json: row.get(36)?,
        sanitization_extensions_digest: row.get(37)?,
    })
}

/// Verifies the provider-view bytes against the hygiene receipt that admitted
/// them. The receipt's source digest names the already projected provider view
/// before hygiene; it must never be compared with the complete canonical
/// source digest carried by `original_source`.
fn validate_provider_view_sanitization(row: &RetainedLocatorRow) -> Result<(), StagedStoreError> {
    let Some(receipt_json) = row.sanitization_receipt_json.as_deref() else {
        return Err(StagedStoreError::LifecycleConflict(
            "target sanitization receipt",
        ));
    };
    let extensions_digest = row
        .sanitization_extensions_digest
        .as_deref()
        .filter(|digest| lower_hex_digest(digest))
        .ok_or(StagedStoreError::LifecycleConflict(
            "target sanitization extensions",
        ))?;
    let receipt = PayloadSanitizationReceipt::from_json(receipt_json)
        .map_err(|_| StagedStoreError::LifecycleConflict("target sanitization receipt"))?;
    receipt
        .verify_binding(&row.payload_sha256, extensions_digest)
        .map_err(|_| StagedStoreError::LifecycleConflict("target sanitization receipt"))?;
    Ok(())
}

/// Revalidates all mutable columns that can influence a retained-source
/// locator before it is allowed to name a Native row. The provider database is
/// private, but a locator is an authority-bearing host claim, so a matching
/// checkout/revision alone is not enough: every stored identity, envelope,
/// content digest, validity projection, and deterministic effect value must
/// still agree.
fn validate_retained_locator_row(
    row: &RetainedLocatorRow,
    delivery_scope: &ExactScopeFields,
    requested_revision: Option<&str>,
    trusted_attribution: &Value,
    staged_path: Option<&Path>,
    call: Option<&tracedecay_memory_provider_registry::ProviderCall>,
    admission: Option<&tracedecay_memory_provider_registry::CurrentAdvisoryAdmission>,
) -> Result<Option<Value>, StagedStoreError> {
    // The source digest is authoritative only as part of this freshly admitted
    // attribution. Validate its shape here; compare the complete attribution
    // with the fresh authority after row matching, below. The provider-view
    // payload is a separate transformed byte domain.
    validate_attribution(trusted_attribution)?;
    if row.row_id <= 0
        || !stored_text(&row.exact_scope_sha256, 64)
        || !stored_text(&row.idempotency_key, 32768)
        || !stored_text(&row.source_authority, 32768)
        || !stored_text(&row.source_event_id, 32768)
        || !stored_text(&row.observation_kind, 32768)
        || !stored_text(&row.payload_contract, 32768)
        || !stored_text(&row.operation_id, 32768)
        || !stored_text(&row.request_identity, 32768)
        || row.admitted_sequence <= 0
        || row.admitted_at_unix_ms < 0
        || !row.feedback.is_finite()
        || !(-1.0..=1.0).contains(&row.feedback)
        || !lower_hex_digest(&row.payload_sha256)
        || !lower_hex_digest(&row.receipt)
        || !lower_hex_digest(&row.effect_digest)
    {
        return Err(StagedStoreError::LifecycleConflict("target row integrity"));
    }
    validate_source_revision_text(row.actual_revision.as_deref())?;
    if row.source_authority != "host_session" {
        return Err(StagedStoreError::LifecycleConflict(
            "target source authority",
        ));
    }
    if row.actual_revision.as_deref() != requested_revision {
        return Err(StagedStoreError::LifecycleConflict(
            "target source revision",
        ));
    }
    let stored_scope = ExactScopeFields {
        profile_id: row.profile_id.clone(),
        project_id: row.project_id.clone(),
        repository_identity: row.repository_identity.clone(),
        worktree_identity: row.worktree_identity.clone(),
        branch_identity: row.branch_identity.clone(),
        agent_session_id: row.agent_session_id.clone(),
        resolved_scope_digest: row.resolved_scope_digest.clone(),
    };
    stored_scope
        .validate()
        .map_err(StagedStoreError::InvalidScope)?;
    if stored_scope.exact_scope_sha256() != row.exact_scope_sha256
        || stored_scope.profile_id != delivery_scope.profile_id
        || stored_scope.project_id != delivery_scope.project_id
        || stored_scope.repository_identity != delivery_scope.repository_identity
        || stored_scope.worktree_identity != delivery_scope.worktree_identity
        || stored_scope.branch_identity != delivery_scope.branch_identity
    {
        return Err(StagedStoreError::LifecycleConflict("target row scope"));
    }
    let expected_contract = expected_payload_contract(&row.observation_kind)
        .ok_or(StagedStoreError::LifecycleConflict("target row kind"))?;
    if row.payload_contract != expected_contract {
        return Err(StagedStoreError::LifecycleConflict("target row contract"));
    }
    let admitted_sequence = u64::try_from(row.admitted_sequence)
        .map_err(|_| StagedStoreError::LifecycleConflict("target row sequence"))?;
    let expected = derive_effect_evidence(
        &row.exact_scope_sha256,
        &row.idempotency_key,
        &row.payload_sha256,
        admitted_sequence,
        &row.operation_id,
    );
    if row.provider_reference != expected.provider_reference
        || row.receipt != expected.receipt
        || row.effect_digest != expected.effect_digest
    {
        return Err(StagedStoreError::LifecycleConflict("target row evidence"));
    }
    if (row.sanitized_payload.is_some()) == row.tombstone {
        return Err(StagedStoreError::LifecycleConflict("target row tombstone"));
    }
    if row
        .semantic_sha256
        .as_deref()
        .is_none_or(|digest| !lower_hex_digest(digest))
    {
        return Err(StagedStoreError::LifecycleConflict(
            "target semantic digest",
        ));
    }
    if row
        .deleted_source_key
        .as_deref()
        .is_some_and(|key| !stored_text(key, 1024))
    {
        return Err(StagedStoreError::LifecycleConflict(
            "target deletion marker",
        ));
    }

    let attribution = row.original_source.as_ref();
    if let Some(attribution) = attribution {
        validate_attribution(attribution)?;
        let source = attribution
            .get("source")
            .ok_or(StagedStoreError::InvalidAdvisory("target source"))?;
        if source.get("observation_id").and_then(Value::as_str)
            != Some(row.source_event_id.as_str())
            || target_source_revision(source)? != row.actual_revision.as_deref()
        {
            return Err(StagedStoreError::LifecycleConflict(
                "target source identity",
            ));
        }
    }
    if row.tombstone {
        if row.original_source.is_some() {
            return Err(StagedStoreError::LifecycleConflict(
                "target tombstone source",
            ));
        }
        if row.sanitization_receipt_json.is_some() || row.sanitization_extensions_digest.is_some() {
            return Err(StagedStoreError::LifecycleConflict(
                "target tombstone evidence",
            ));
        }
    } else {
        if row.sanitization_receipt_json.is_none() || row.sanitization_extensions_digest.is_none() {
            return Err(StagedStoreError::LifecycleConflict(
                "target sanitization receipt",
            ));
        }
        validate_provider_view_sanitization(row)?;
    }
    if let Some(overlay) = &row.validity_override {
        if attribution.is_none() {
            return Err(StagedStoreError::LifecycleConflict(
                "target validity overlay",
            ));
        }
        validate_validity_overlay(overlay)?;
        let effective = serde_json::json!({"validity": overlay});
        let validity = recorded_validity(Some(&effective))?;
        if row.valid_from_nanos != validity.valid_from_utc_nanos
            || row.valid_until_nanos != validity.valid_until_utc_nanos
            || row.superseded_nanos != validity.superseded_at_utc_nanos
            || row.revoked_nanos != validity.revoked_at_utc_nanos
        {
            return Err(StagedStoreError::LifecycleConflict(
                "target validity projection",
            ));
        }
    } else if let Some(attribution) = attribution {
        let validity = recorded_validity(Some(attribution))?;
        if row.valid_from_nanos != validity.valid_from_utc_nanos
            || row.valid_until_nanos != validity.valid_until_utc_nanos
            || row.superseded_nanos != validity.superseded_at_utc_nanos
            || row.revoked_nanos != validity.revoked_at_utc_nanos
        {
            return Err(StagedStoreError::LifecycleConflict(
                "target validity projection",
            ));
        }
    } else if row.valid_from_nanos.is_some()
        || row.valid_until_nanos.is_some()
        || row.superseded_nanos.is_some()
        || row.revoked_nanos.is_some()
    {
        return Err(StagedStoreError::LifecycleConflict(
            "target validity projection",
        ));
    }

    if let Some(content_digest) = row.projected_content_sha256.as_deref() {
        if !lower_hex_digest(content_digest) {
            return Err(StagedStoreError::LifecycleConflict("target content digest"));
        }
    } else if attribution.is_some() {
        return Err(StagedStoreError::LifecycleConflict("target content digest"));
    }
    let Some(payload) = row.sanitized_payload.as_deref() else {
        return Ok(row.original_source.clone());
    };
    if sha256_hex(payload) != row.payload_sha256 {
        return Err(StagedStoreError::PayloadDigestMismatch {
            idempotency_key: row.idempotency_key.clone(),
            stored_payload_sha256: row.payload_sha256.clone(),
        });
    }
    if let Some(staged_path) = staged_path {
        validate_host_projection_for_row(
            staged_path,
            &stored_scope,
            &row.idempotency_key,
            &row.source_authority,
            &row.source_event_id,
            row.actual_revision.as_deref(),
            &row.observation_kind,
            &row.payload_contract,
            payload,
            &row.payload_sha256,
            row.sanitization_receipt_json
                .as_deref()
                .ok_or(StagedStoreError::LifecycleConflict(
                    "target sanitization receipt",
                ))?,
            row.sanitization_extensions_digest.as_deref().ok_or(
                StagedStoreError::LifecycleConflict("target sanitization extensions"),
            )?,
            call,
            admission,
        )?;
    }
    let envelope: Value = serde_json::from_slice(payload)
        .map_err(|_| StagedStoreError::InvalidAdvisory("target observation envelope"))?;
    if envelope.get("observation_kind").and_then(Value::as_str)
        != Some(row.observation_kind.as_str())
        || envelope.get("payload_contract").and_then(Value::as_str)
            != Some(row.payload_contract.as_str())
        || envelope.pointer("/source_identity/original_source") != attribution
    {
        return Err(StagedStoreError::LifecycleConflict("target payload source"));
    }
    if let Some(identity) = envelope.get("source_identity") {
        if let Some(value) = identity.get("source_authority") {
            if value.as_str() != expected_source_identity_authority(&row.observation_kind) {
                return Err(StagedStoreError::LifecycleConflict(
                    "target payload identity",
                ));
            }
        }
        if let Some(value) = identity.get("source_event_id")
            && value.as_str() != Some(row.source_event_id.as_str())
        {
            return Err(StagedStoreError::LifecycleConflict(
                "target payload identity",
            ));
        }
    }
    let expected_payload_digest = envelope
        .get("payload_sha256")
        .and_then(Value::as_str)
        .filter(|value| lower_hex_digest(value))
        .ok_or(StagedStoreError::InvalidAdvisory("target canonical digest"))?;
    let canonical =
        tracedecay_memory_hygiene::canonical_payload_bytes(&envelope["canonical_payload"])
            .map_err(|_| StagedStoreError::InvalidAdvisory("target canonical payload"))?;
    let canonical_digest = sha256_hex(&canonical);
    if expected_payload_digest != canonical_digest {
        return Err(StagedStoreError::LifecycleConflict(
            "target canonical digest",
        ));
    }
    // `canonical_digest` is the transformed provider-view payload's own
    // digest. The complete original canonical source digest belongs to the
    // freshly trusted attribution above and is validated by exact attribution
    // equality below; these two values are intentionally independent because
    // eligibility narrowing and hygiene redaction can rewrite the provider
    // view before Native sees it.
    if extract_message_text(payload).is_none() {
        return Err(StagedStoreError::LifecycleConflict(
            "target observation evidence",
        ));
    }
    let semantic = semantic_observation_digest(payload)?;
    if row.semantic_sha256.as_deref() != Some(semantic.as_str()) {
        return Err(StagedStoreError::LifecycleConflict(
            "target semantic digest",
        ));
    }
    let content_digest = extract_message_text(payload).map(|text| sha256_hex(text.as_bytes()));
    if row.projected_content_sha256.as_deref() != content_digest.as_deref() {
        return Err(StagedStoreError::LifecycleConflict("target content digest"));
    }
    Ok(row.original_source.clone())
}

fn target_source_revision(source: &Value) -> Result<Option<&str>, StagedStoreError> {
    match source.get("source_revision") {
        Some(Value::Null) => Ok(None),
        Some(Value::String(value))
            if !value.is_empty()
                && value.trim() == value
                && value.len() <= 1024
                && !value.chars().any(char::is_control) =>
        {
            Ok(Some(value.as_str()))
        }
        _ => Err(StagedStoreError::InvalidAdvisory("target source revision")),
    }
}

fn validate_payload_original_source(
    payload: &[u8],
    stored_original_source: Option<&Value>,
) -> Result<(), StagedStoreError> {
    let envelope: Value = serde_json::from_slice(payload)
        .map_err(|_| StagedStoreError::InvalidAdvisory("stored observation envelope"))?;
    if envelope.pointer("/source_identity/original_source") != stored_original_source {
        return Err(StagedStoreError::LifecycleConflict(
            "stored source attribution",
        ));
    }
    Ok(())
}

fn validate_payload_source_identity(
    payload: &[u8],
    stored_original_source: Option<&Value>,
    stored_source_event_id: &str,
    stored_source_revision: Option<&str>,
) -> Result<(), StagedStoreError> {
    validate_payload_original_source(payload, stored_original_source)?;
    let Some(original_source) = stored_original_source else {
        return Ok(());
    };
    validate_attribution(original_source)?;
    let source = original_source
        .get("source")
        .ok_or(StagedStoreError::InvalidAdvisory(
            "stored source attribution",
        ))?;
    if source.get("observation_id").and_then(Value::as_str) != Some(stored_source_event_id)
        || target_source_revision(source)? != stored_source_revision
    {
        return Err(StagedStoreError::LifecycleConflict(
            "stored source identity",
        ));
    }
    Ok(())
}

/// Resolves a lifecycle target against Native's private row identity.
///
/// Controlled V2 lifecycle operations accept retained source locators only.
/// Stable references are a legacy provider-local alias and are refused before
/// any row lookup. A retained source locator is accepted only with the fresh
/// call-bound admission, where exactly one trusted source and exactly one
/// Native row agree on observation identity, actual revision, and checkout.
/// The row's full stored attribution is then checked against the trusted
/// source before any lifecycle effect is allowed.
fn resolve_target(
    connection: &Connection,
    call: &tracedecay_memory_provider_registry::ProviderCall,
    request: &Value,
    admission: Option<&tracedecay_memory_provider_registry::CurrentAdvisoryAdmission>,
    allow_tombstone: bool,
    staged_path: &Path,
) -> Result<ResolvedTarget, StagedStoreError> {
    let target = request
        .get("target")
        .ok_or(StagedStoreError::InvalidAdvisory("target"))?;
    if required_text(target, "provider_id")? != call.provider_id.as_str()
        || exact_scope_from_value(&target["delivery_scope"])? != call.exact_scope
    {
        return Err(StagedStoreError::LifecycleConflict("target attribution"));
    }
    let target_registration_revision = target
        .get("registration_revision")
        .and_then(Value::as_u64)
        .filter(|revision| *revision > 0)
        .ok_or(StagedStoreError::InvalidAdvisory(
            "target registration revision",
        ))?;
    // A provider restart may leave an older retained locator valid, but a
    // target from a future registration is never a valid alias for the
    // current call. The target's registration is part of its host authority;
    // accepting a higher value would permit an alias swap across instances.
    if target_registration_revision > call.registration_revision {
        return Err(StagedStoreError::LifecycleConflict(
            "target registration revision",
        ));
    }
    let reference = target
        .get("reference")
        .ok_or(StagedStoreError::InvalidAdvisory("target reference"))?;
    let kind = required_text(reference, "kind")?;
    let requested_reference = required_text(reference, "reference")?;

    if kind == "stable_memory_ref" {
        // Stable refs were minted from Native's private row identity. They
        // carry no fresh ProviderHistory authority and can be rebound after a
        // restart or an alias swap, so controlled feedback/correction must
        // never resolve them. The inspection surface has its own legacy path;
        // this resolver is called only by controlled lifecycle mutations.
        return Err(StagedStoreError::LifecycleConflict(
            "legacy target reference",
        ));
    }

    if kind != "retained_source_locator" {
        return Err(StagedStoreError::LifecycleConflict("target unknown"));
    }
    if requested_reference.trim() != requested_reference
        || requested_reference.len() > 1024
        || requested_reference.chars().any(char::is_control)
    {
        return Err(StagedStoreError::InvalidAdvisory("target reference"));
    }
    // Retained locators are host authority claims. Even a tombstone has to
    // run while the sibling host journal is available; its scrubbed payload
    // cannot carry the projection edge that a live row revalidates below.
    require_host_projection_journal(staged_path)?;
    let admission = admission.ok_or(StagedStoreError::LifecycleConflict(
        "target authority unavailable",
    ))?;
    admission
        .verify_for(call)
        .map_err(|_| StagedStoreError::LifecycleConflict("target authority binding"))?;
    let target_source = target
        .get("source")
        .ok_or(StagedStoreError::InvalidAdvisory("target source"))?;
    let observation_id = required_text(target_source, "observation_id")?;
    let source_revision = target_source_revision(target_source)?;
    let trusted: Vec<_> = admission
        .history_sources
        .iter()
        .filter(|source| {
            source.attribution.source.observation_id == observation_id
                && source.attribution.source.source_revision.as_deref() == source_revision
        })
        .collect();
    let trusted = match trusted.as_slice() {
        [] => {
            return Err(StagedStoreError::LifecycleConflict("target source unknown"));
        }
        [trusted] => trusted,
        _ => {
            return Err(StagedStoreError::LifecycleConflict(
                "target source ambiguous",
            ));
        }
    };
    let trusted_attribution =
        super::provider_history::source_attribution_json(&trusted.attribution)
            .map_err(|_| StagedStoreError::InvalidAdvisory("target authority source"))?;
    if !allow_tombstone
        && !super::provider_history::retained_history_source(trusted.current_disposition.state)
    {
        return Err(StagedStoreError::LifecycleConflict(
            "target source unavailable",
        ));
    }
    if target_source != trusted_attribution.get("source").unwrap_or(&Value::Null)
        || target.get("original_scope") != trusted_attribution.get("origin_scope")
    {
        return Err(StagedStoreError::LifecycleConflict(
            "target source mismatch",
        ));
    }

    let mut statement = connection.prepare(
        "SELECT rowid, exact_scope_sha256, idempotency_key, profile_id, project_id,
                repository_identity, worktree_identity, branch_identity, agent_session_id,
                resolved_scope_digest, source_authority, source_event_id, actual_revision,
                observation_kind, payload_contract, sanitized_payload, payload_sha256,
                operation_id, request_identity, provider_reference, receipt, effect_digest,
                admitted_sequence, admitted_at_unix_ms, tombstone, original_source,
                feedback, feedback_suppressed, validity_override, valid_from_nanos,
                valid_until_nanos, superseded_nanos, revoked_nanos, semantic_sha256,
                projected_content_sha256, deleted_source_key, sanitization_receipt_json,
                sanitization_extensions_digest
         FROM tdmem_native_staged_observation_v1
         WHERE profile_id = ?1 AND project_id = ?2 AND repository_identity = ?3
           AND worktree_identity = ?4 AND branch_identity = ?5
           AND actual_revision IS ?6 AND source_event_id = ?7
           ORDER BY admitted_sequence",
    )?;
    let mut rows = statement.query(params![
        call.exact_scope.profile_id,
        call.exact_scope.project_id,
        call.exact_scope.repository_identity,
        call.exact_scope.worktree_identity,
        call.exact_scope.branch_identity,
        source_revision,
        observation_id,
    ])?;
    let mut matches = Vec::new();
    while let Some(row) = rows.next()? {
        let candidate = read_retained_locator_row(row)?;
        let attribution = validate_retained_locator_row(
            &candidate,
            &call.exact_scope,
            source_revision,
            &trusted_attribution,
            Some(staged_path),
            Some(call),
            Some(admission),
        )?;
        let matches_source = if candidate.tombstone && attribution.is_none() {
            // Privacy scrubbing deliberately removes original_source. The
            // preserved source event/revision, already constrained by the
            // trusted-admission query above, is sufficient to identify a
            // tombstone for read-only inspection.
            candidate.source_event_id == observation_id
                && candidate.actual_revision.as_deref() == source_revision
        } else {
            attribution.as_ref().is_some_and(|attribution| {
                let Some(source) = attribution.get("source") else {
                    return false;
                };
                source.get("observation_id").and_then(Value::as_str) == Some(observation_id)
                    && target_source_revision(source).ok().flatten() == source_revision
            })
        };
        if !matches_source {
            continue;
        }
        matches.push((
            candidate.row_id,
            candidate.provider_reference,
            attribution,
            candidate.feedback,
            candidate.tombstone,
            candidate.deleted_source_key,
        ));
        if matches.len() > 1 {
            return Err(StagedStoreError::LifecycleConflict("target row ambiguous"));
        }
    }
    let Some((
        row_id,
        provider_reference,
        stored_attribution,
        feedback,
        tombstone,
        deleted_source_key,
    )) = matches.pop()
    else {
        return Err(StagedStoreError::LifecycleConflict("target row unknown"));
    };
    let attribution = match stored_attribution {
        Some(attribution) => {
            validate_attribution(&attribution)?;
            if attribution != trusted_attribution {
                return Err(StagedStoreError::LifecycleConflict(
                    "stored target source mismatch",
                ));
            }
            if source_deleted(
                connection,
                &call.exact_scope,
                trusted.attribution.source.source_key.as_str(),
                Some(&attribution),
            )? {
                return Err(StagedStoreError::PrivacyDeleted);
            }
            attribution
        }
        None if tombstone => {
            let target = NativeSourceTarget::from_source(&trusted_attribution["source"])?;
            let digest = target.digest();
            if deleted_source_key.as_deref() != Some(target.source_key.as_str())
                && deleted_source_key.as_deref() != Some(digest.as_str())
            {
                return Err(StagedStoreError::LifecycleConflict(
                    "target deletion marker",
                ));
            }
            trusted_attribution
        }
        None => return Err(StagedStoreError::InvalidAdvisory("target source")),
    };
    if tombstone && !allow_tombstone {
        return Err(StagedStoreError::LifecycleConflict("target tombstone"));
    }
    let target_bytes = serde_json::to_vec(target)
        .map_err(|_| StagedStoreError::InvalidAdvisory("target serialization"))?;
    Ok(ResolvedTarget {
        row_id,
        provider_reference,
        outward_reference: requested_reference.to_owned(),
        retained_source_locator: Some(requested_reference.to_owned()),
        attribution: Some(attribution),
        feedback,
        tombstone,
        target_digest: sha256_hex(&target_bytes),
    })
}

fn feedback_advisory(
    connection: &Connection,
    call: &tracedecay_memory_provider_registry::ProviderCall,
    request: &Value,
    admission: Option<&tracedecay_memory_provider_registry::CurrentAdvisoryAdmission>,
    staged_path: &Path,
) -> Result<(Value, bool), StagedStoreError> {
    let resolved = resolve_target(connection, call, request, admission, false, staged_path)?;
    let previous = resolved.feedback;
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
    let was_suppressed: Option<bool> = connection
        .query_row(
            "SELECT feedback_suppressed FROM tdmem_native_staged_observation_v1 WHERE rowid=?1",
            params![resolved.row_id],
            |row| row.get(0),
        )
        .optional()?;
    let Some(was_suppressed) = was_suppressed else {
        return Err(StagedStoreError::LifecycleConflict("target row missing"));
    };
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
    let changed = connection.execute(
        "UPDATE tdmem_native_staged_observation_v1
         SET feedback = ?1, feedback_suppressed = ?2 WHERE rowid = ?3",
        params![next, suppressed, resolved.row_id],
    )?;
    if changed != 1 {
        return Err(StagedStoreError::LifecycleConflict("target row update"));
    }
    // A settled event is a durable change even when its explicit neutral effect is zero.
    let mut applied_effect = serde_json::json!({
        "stable_memory_ref":resolved.outward_reference,
        "ranking_bias_before":previous,
        "ranking_bias_after":next,
        "ranking_bias_delta":next-previous,
        "recall_score_weight":"0.5",
        "feedback_suppressed_before":was_suppressed,
        "feedback_suppressed_after":suppressed,
        "neutral":delta == 0.0
    });
    if let Some(locator) = &resolved.retained_source_locator {
        applied_effect["retained_source_locator"] = locator.clone().into();
    }
    Ok((
        serde_json::json!({"target_digest":resolved.target_digest, "signal":signal,
            "applied_effect":applied_effect, "warnings":[]}),
        true,
    ))
}

fn correct_advisory(
    connection: &rusqlite::Transaction<'_>,
    call: &tracedecay_memory_provider_registry::ProviderCall,
    request: &Value,
    retention: StagedRetentionPolicyV1,
    admission: Option<&tracedecay_memory_provider_registry::CurrentAdvisoryAdmission>,
    staged_path: &Path,
) -> Result<(Value, bool), StagedStoreError> {
    let resolved = resolve_target(connection, call, request, admission, false, staged_path)?;
    let reference = &resolved.provider_reference;
    let mut original = resolved
        .attribution
        .clone()
        .ok_or(StagedStoreError::LifecycleConflict(
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
    if let Some(text) = connection.query_row(
        "SELECT validity_override FROM tdmem_native_staged_observation_v1 WHERE rowid=?1",
        params![resolved.row_id],
        |row| row.get::<_, Option<String>>(0),
    )? {
        original["validity"] = serde_json::from_str(&text)
            .map_err(|_| StagedStoreError::InvalidAdvisory("correction validity overlay"))?;
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
        let changed = connection.execute(
            "UPDATE tdmem_native_staged_observation_v1 SET validity_override=?1 WHERE rowid=?2",
            params![original["validity"].to_string(), resolved.row_id],
        )?;
        if changed != 1 {
            return Err(StagedStoreError::LifecycleConflict("target row update"));
        }
        sync_row_validity_by_rowid(connection, resolved.row_id, Some(&original))?;
        let affected = resolved.outward_reference.clone();
        return Ok((
            serde_json::json!({"target_digest":resolved.target_digest,"correction_kind":kind,"affected_provider_effects":[affected],"warnings":[]}),
            true,
        ));
    }
    let replacement_source = replacement
        .pointer("/source_identity/original_source")
        .ok_or(StagedStoreError::InvalidAdvisory("replacement attribution"))?;
    validate_attribution(replacement_source)?;
    let replacement_identity = replacement_source
        .get("source")
        .ok_or(StagedStoreError::InvalidAdvisory("replacement attribution"))?;
    let replacement_id = required_text(replacement_identity, "observation_id")?;
    let replacement_revision = target_source_revision(replacement_identity)?;
    if let Some(admission) = admission {
        let admitted_replacements: Vec<_> = admission
            .history_sources
            .iter()
            .filter(|source| {
                source.attribution.source.observation_id == replacement_id
                    && source.attribution.source.source_revision.as_deref() == replacement_revision
            })
            .collect();
        if !admitted_replacements.is_empty() {
            let [trusted_replacement] = admitted_replacements.as_slice() else {
                return Err(StagedStoreError::LifecycleConflict(
                    "replacement source ambiguous",
                ));
            };
            let trusted_replacement =
                super::provider_history::source_attribution_json(&trusted_replacement.attribution)
                    .map_err(|_| {
                        StagedStoreError::InvalidAdvisory("replacement authority source")
                    })?;
            if replacement_source != &trusted_replacement {
                return Err(StagedStoreError::LifecycleConflict(
                    "replacement source mismatch",
                ));
            }
        } else if resolved.retained_source_locator.is_some() {
            return Err(StagedStoreError::LifecycleConflict(
                "replacement source unknown",
            ));
        }
    }
    let revision = replacement_revision
        .filter(|revision| *revision != expected)
        .ok_or(StagedStoreError::LifecycleConflict("replacement revision"))?;
    validate_source_revision_text(Some(revision))?;
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
    let mut record = StagedObservationRecord {
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
    // The command idempotency key identifies this correction operation, not
    // the replacement observation. Translate it to the exact canonical
    // observation key only through the fresh source admission and host
    // journal. A self-minted provider receipt is never a replacement proof.
    let projection = host_projection_for_record(staged_path, &record, Some(call), admission)?
        .ok_or(StagedStoreError::LifecycleConflict(
            "host projection lineage unavailable",
        ))?;
    record.idempotency_key = projection.idempotency_key;
    let provider_view = projection.sanitization;
    let evidence = match stage_in_transaction(
        connection,
        record,
        retention,
        &provider_view,
        Some(call),
        admission,
    )? {
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
    let changed = connection.execute(
        "UPDATE tdmem_native_staged_observation_v1 SET validity_override = ?1 WHERE rowid = ?2",
        params![original["validity"].to_string(), resolved.row_id],
    )?;
    if changed != 1 {
        return Err(StagedStoreError::LifecycleConflict("target row update"));
    }
    sync_row_validity_by_rowid(connection, resolved.row_id, Some(&original))?;
    let mut replacement_rows = connection.prepare(
        "SELECT rowid FROM tdmem_native_staged_observation_v1 WHERE provider_reference=?1",
    )?;
    let replacement_row_ids = replacement_rows
        .query_map(params![evidence.provider_reference], |row| {
            row.get::<_, i64>(0)
        })?
        .collect::<Result<Vec<_>, _>>()?;
    let [replacement_row_id] = replacement_row_ids.as_slice() else {
        return Err(StagedStoreError::LifecycleConflict(
            "replacement row identity",
        ));
    };
    sync_row_validity_by_rowid(connection, *replacement_row_id, Some(replacement_source))?;
    // The replacement row has a new private Native reference. Expose only the
    // count on the retained-locator path; the original opaque locator remains
    // the caller's stable public handle, while both raw refs stay in SQLite.
    let affected = if resolved.retained_source_locator.is_some() {
        serde_json::json!(2_u64)
    } else {
        serde_json::json!([reference, evidence.provider_reference])
    };
    Ok((
        serde_json::json!({"target_digest":resolved.target_digest,"correction_kind":kind,
        "affected_provider_effects":affected, "warnings":[]}),
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
        deleted_source_key=?6,original_source=NULL,validity_override=NULL,valid_from_nanos=NULL,valid_until_nanos=NULL,superseded_nanos=NULL,revoked_nanos=NULL,projected_content_sha256=NULL,sanitization_receipt_json=NULL,sanitization_extensions_digest=NULL
        WHERE profile_id=?1 AND project_id=?2 AND repository_identity=?3 AND worktree_identity=?4 AND branch_identity=?5
        AND (json_extract(original_source,'$.source.source_key')=?6 OR deleted_source_key=?6 OR (original_source IS NULL AND source_event_id=?6))
        AND (sanitized_payload IS NOT NULL OR original_source IS NOT NULL OR validity_override IS NOT NULL OR feedback!=0 OR feedback_suppressed!=0)",
        params![scope.profile_id,scope.project_id,scope.repository_identity,scope.worktree_identity,scope.branch_identity,source])? as u64)
}

/// Before a privacy mutation uses the row's source index, authenticate every
/// resident payload/source pair against the immutable host journal. A mutable
/// `original_source` column must never be able to move a row out of (or into)
/// the deletion set. Tombstones have already scrubbed that pair and retain
/// only their deletion marker, so there is no source attribution left to
/// redirect.
fn validate_resident_original_sources(
    connection: &Connection,
    scope: &ExactScopeFields,
    staged_path: &Path,
    call: &tracedecay_memory_provider_registry::ProviderCall,
    admission: Option<&tracedecay_memory_provider_registry::CurrentAdvisoryAdmission>,
) -> Result<(), StagedStoreError> {
    require_host_projection_journal(staged_path)?;
    let mut statement = connection.prepare(
        "SELECT idempotency_key, source_authority, source_event_id, actual_revision, observation_kind,
                payload_contract, sanitized_payload, payload_sha256, original_source,
                sanitization_receipt_json, sanitization_extensions_digest, tombstone
         FROM tdmem_native_staged_observation_v1
         WHERE profile_id=?1 AND project_id=?2 AND repository_identity=?3
           AND worktree_identity=?4 AND branch_identity=?5",
    )?;
    let mut rows = statement.query(params![
        scope.profile_id,
        scope.project_id,
        scope.repository_identity,
        scope.worktree_identity,
        scope.branch_identity,
    ])?;
    while let Some(row) = rows.next()? {
        if row.get::<_, bool>(11)? {
            if row.get::<_, Option<String>>(8)?.is_some()
                || row.get::<_, Option<Vec<u8>>>(6)?.is_some()
            {
                return Err(StagedStoreError::LifecycleConflict(
                    "resident tombstone source",
                ));
            }
            continue;
        }
        let idempotency_key: String = row.get(0)?;
        let source_authority: String = row.get(1)?;
        let source_event_id: String = row.get(2)?;
        let source_revision: Option<String> = row.get(3)?;
        let observation_kind: String = row.get(4)?;
        let payload_contract: String = row.get(5)?;
        let payload: Vec<u8> =
            row.get::<_, Option<Vec<u8>>>(6)?
                .ok_or(StagedStoreError::LifecycleConflict(
                    "resident payload unavailable",
                ))?;
        let payload_sha256: String = row.get(7)?;
        let original_source: Value = row
            .get::<_, Option<String>>(8)?
            .ok_or(StagedStoreError::LifecycleConflict(
                "resident source unavailable",
            ))
            .and_then(|text| {
                serde_json::from_str(&text)
                    .map_err(|_| StagedStoreError::LifecycleConflict("resident source unavailable"))
            })?;
        validate_payload_source_identity(
            &payload,
            Some(&original_source),
            &source_event_id,
            source_revision.as_deref(),
        )?;
        let receipt_json: String =
            row.get::<_, Option<String>>(9)?
                .ok_or(StagedStoreError::LifecycleConflict(
                    "resident sanitization receipt",
                ))?;
        let extensions_digest: String =
            row.get::<_, Option<String>>(10)?
                .ok_or(StagedStoreError::LifecycleConflict(
                    "resident sanitization extensions",
                ))?;
        validate_host_projection_for_row(
            staged_path,
            scope,
            // Privacy preflight uses the row's canonical observation key. It
            // must never infer a host envelope from provider-view bytes.
            &idempotency_key,
            &source_authority,
            &source_event_id,
            source_revision.as_deref(),
            &observation_kind,
            &payload_contract,
            &payload,
            &payload_sha256,
            &receipt_json,
            &extensions_digest,
            Some(call),
            admission,
        )?;
    }
    Ok(())
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
        deleted_source_key=?6,original_source=NULL,validity_override=NULL,valid_from_nanos=NULL,valid_until_nanos=NULL,superseded_nanos=NULL,revoked_nanos=NULL,projected_content_sha256=NULL,sanitization_receipt_json=NULL,sanitization_extensions_digest=NULL
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
    admission: Option<&tracedecay_memory_provider_registry::CurrentAdvisoryAdmission>,
    staged_path: &Path,
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
    validate_resident_original_sources(
        connection,
        &call.exact_scope,
        staged_path,
        call,
        admission,
    )?;
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
    retained_target: Option<&ResolvedTarget>,
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
    // A locator is resolved to the private Native reference before this query.
    // The five checkout columns deliberately omit the delivery session so a
    // retained source can be inspected after reopening from another session.
    // Stable-reference inspection keeps the existing exact-scope query.
    let retained_locator =
        retained_target.and_then(|target| target.retained_source_locator.as_ref());
    let selected_reference = if retained_target.is_some() {
        None
    } else if view == "trace" {
        Some(required_text(&request["selector"], "stable_memory_ref")?)
    } else if view == "source_influence" {
        retained_target
            .map(|target| target.provider_reference.as_str())
            .or_else(|| {
                request
                    .pointer("/selector/stable_memory_ref")
                    .and_then(Value::as_str)
            })
    } else if view == "delivery_receipt" {
        retained_target.map(|target| target.provider_reference.as_str())
    } else {
        None
    };
    let mut statement = connection.prepare("SELECT provider_reference, admitted_sequence, actual_revision, feedback, tombstone, original_source,
        receipt,feedback_suppressed,validity_override,operation_id,idempotency_key,sanitized_payload,payload_sha256
        FROM tdmem_native_staged_observation_v1 WHERE
        ((?7 = 0 AND exact_scope_sha256 = ?1)
         OR (?7 = 1 AND profile_id = ?2 AND project_id = ?3 AND repository_identity = ?4
             AND worktree_identity = ?5 AND branch_identity = ?6
             AND rowid = ?12))
        AND admitted_sequence > ?8
        AND (?7 = 1 OR ?10 IS NULL OR json_extract(original_source,'$.source.source_key')=?10)
        AND (?7 = 1 OR ?11 IS NULL OR idempotency_key=?11)
        AND (?13 IS NULL OR provider_reference=?13)
        ORDER BY admitted_sequence LIMIT ?9")?;
    let mut rows = statement.query(params![
        scope,
        call.exact_scope.profile_id,
        call.exact_scope.project_id,
        call.exact_scope.repository_identity,
        call.exact_scope.worktree_identity,
        call.exact_scope.branch_identity,
        retained_locator.is_some(),
        sqlite_i64(after, "inspection cursor")?,
        sqlite_i64(maximum + 1, "inspection maximum_items")?,
        if retained_target.is_some() {
            None
        } else {
            request
                .pointer("/selector/source_key")
                .and_then(Value::as_str)
        },
        if view == "delivery_receipt" && retained_target.is_none() {
            Some(required_text(&request["selector"], "idempotency_key")?)
        } else {
            None
        },
        retained_target.map(|target| target.row_id),
        selected_reference,
    ])?;
    let mut items = Vec::new();
    let mut bytes = 2;
    let mut cursor = after;
    let mut partial = false;
    while let Some(row) = rows.next()? {
        let mut attribution = row
            .get::<_, Option<String>>(5)?
            .map(|text| {
                serde_json::from_str::<Value>(&text)
                    .map_err(|_| StagedStoreError::InvalidAdvisory("inspection attribution"))
            })
            .transpose()?;
        if attribution.is_none() && retained_target.is_some_and(|target| target.tombstone) {
            attribution = retained_target.and_then(|target| target.attribution.clone());
        }
        let mut item = if view == "state_summary" {
            let raw_reference: String = row.get(0)?;
            let reference = retained_target
                .map(|target| target.outward_reference.as_str())
                .unwrap_or(raw_reference.as_str());
            serde_json::json!({"stable_memory_ref":reference,"sequence":sqlite_u64(row, 1)?,
                "source_revision":row.get::<_,Option<String>>(2)?,"ranking_bias":row.get::<_,f64>(3)?,"privacy_or_retention_tombstone":row.get::<_,bool>(4)?,
                "validity":attribution.as_ref().and_then(|source|source.get("validity")),"receipt":row.get::<_,String>(6)?})
        } else {
            Value::Null
        };
        if view == "delivery_receipt" {
            let raw_reference: String = row.get(0)?;
            let reference = retained_target
                .map(|target| target.outward_reference.as_str())
                .unwrap_or(raw_reference.as_str());
            item = serde_json::json!({"operation_id":row.get::<_,String>(9)?,"idempotency_key":row.get::<_,String>(10)?,
                "provider_receipt_digest":row.get::<_,String>(6)?,"stable_memory_ref":reference});
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
            let raw_reference: String = row.get(0)?;
            let reference = retained_target
                .map(|target| target.outward_reference.as_str())
                .unwrap_or(raw_reference.as_str());
            item = serde_json::json!({"stable_memory_ref":reference,"content":content,
                "content_sha256":content_sha256,"original_source":if content.is_some() {attribution.clone()} else {None}});
        } else if view == "source_influence" {
            let Some(attribution) = &attribution else {
                partial = true;
                continue;
            };
            let reference: String = row.get(0)?;
            let outward_reference = retained_target
                .map(|target| target.outward_reference.as_str())
                .unwrap_or(reference.as_str());
            // Original receipts predate the explicit association and retain
            // SHA(reference). Read them without rewriting their evidence.
            let original_reference_digest = sha256_hex(reference.as_bytes());
            let mut feedback = serde_json::json!({"helpful":0,"harmful":0,"ignored":0,"corrected":0,"superseded":0});
            let mut counts = connection.prepare(
                "SELECT json_extract(response,'$.signal'),COUNT(*) FROM tdmem_native_operation_v2 \
                 WHERE (json_extract(response,'$.applied_effect.stable_memory_ref')=?1 \
                    OR json_extract(response,'$.applied_effect.retained_source_locator')=?1 \
                    OR (json_extract(response,'$.applied_effect.stable_memory_ref') IS NULL \
                        AND json_extract(response,'$.target_digest')=?2)) \
                   AND json_extract(response,'$.signal') IS NOT NULL \
                 GROUP BY json_extract(response,'$.signal')",
            )?;
            let entries = counts.query_map(
                params![outward_reference, original_reference_digest],
                |row| Ok((row.get::<_, String>(0)?, sqlite_u64(row, 1)?)),
            )?;
            for entry in entries {
                let (signal, count) = entry?;
                feedback[signal] = count.into();
            }
            let last: Option<String> = connection
                .query_row(
                    "SELECT receipt FROM tdmem_native_operation_v2 \
                 WHERE (json_extract(response,'$.applied_effect.stable_memory_ref')=?1 \
                    OR json_extract(response,'$.applied_effect.retained_source_locator')=?1 \
                    OR (json_extract(response,'$.applied_effect.stable_memory_ref') IS NULL \
                        AND json_extract(response,'$.target_digest')=?2)) \
                 AND json_extract(response,'$.signal') IS NOT NULL \
                 ORDER BY generation_after DESC LIMIT 1",
                    params![outward_reference, original_reference_digest],
                    |row| row.get(0),
                )
                .optional()?;
            let tombstone: bool = row.get(4)?;
            let suppressed: bool = row.get(7)?;
            let validity = match row.get::<_, Option<String>>(8)? {
                Some(text) => {
                    let overlay: Value = serde_json::from_str(&text)
                        .map_err(|_| StagedStoreError::InvalidAdvisory("inspection validity"))?;
                    validate_validity_overlay(&overlay)?;
                    let effective = serde_json::json!({"validity":overlay});
                    validity_json(&recorded_validity(Some(&effective))?)
                }
                None => validity_json(&recorded_validity(Some(attribution))?),
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
                // A replacement correction stores its private Native row
                // reference in the old row's validity overlay. Retained
                // locators must never echo that provider-local handle.
                "validity": if retained_target.is_some() {
                    let mut validity = validity;
                    if validity
                        .get("superseded_by")
                        .and_then(Value::as_str)
                        .is_some()
                    {
                        validity["superseded_by"] = Value::Null;
                    }
                    validity
                } else {
                    validity
                },
            })
            .to_string();
            if effect_summary.len() > 8192 {
                return Err(StagedStoreError::ValueOutOfRange {
                    field: "provider_local_effect_summary",
                });
            }
            let reference_kind = retained_target
                .and_then(|target| target.retained_source_locator.as_ref())
                .map(|_| "retained_source_locator")
                .unwrap_or("stable_memory_ref");
            item = serde_json::json!({"target":{"provider_id":call.provider_id.as_str(),"registration_revision":call.registration_revision,
                "original_scope":attribution["origin_scope"],"delivery_scope":scope_json(&call.exact_scope),"source":attribution["source"],
                "reference":{"kind":reference_kind,"reference":outward_reference}},"source":attribution["source"],
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
                connection.execute("UPDATE tdmem_native_staged_observation_v1 SET sanitized_payload=NULL,tombstone=1,feedback=0,feedback_suppressed=0,original_source=NULL,validity_override=NULL,valid_from_nanos=NULL,valid_until_nanos=NULL,superseded_nanos=NULL,revoked_nanos=NULL,projected_content_sha256=NULL,sanitization_receipt_json=NULL,sanitization_extensions_digest=NULL WHERE provider_reference=?1",params![reference])?;
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
    let mut statement = connection.prepare(
        "SELECT rowid FROM tdmem_native_staged_observation_v1 WHERE provider_reference=?1",
    )?;
    let row_ids = statement
        .query_map(params![reference], |row| row.get::<_, i64>(0))?
        .collect::<Result<Vec<_>, _>>()?;
    let [row_id] = row_ids.as_slice() else {
        return Err(StagedStoreError::LifecycleConflict("target row identity"));
    };
    sync_row_validity_by_rowid(connection, *row_id, attribution)
}

fn sync_row_validity_by_rowid(
    connection: &Connection,
    row_id: i64,
    attribution: Option<&Value>,
) -> Result<(), StagedStoreError> {
    if row_id <= 0 {
        return Err(StagedStoreError::LifecycleConflict("target row identity"));
    }
    let validity = recorded_validity(attribution)?;
    let payload: Option<Vec<u8>> = connection
        .query_row(
            "SELECT sanitized_payload FROM tdmem_native_staged_observation_v1 WHERE rowid=?1",
            params![row_id],
            |row| row.get(0),
        )
        .optional()?
        .ok_or(StagedStoreError::LifecycleConflict("target row missing"))?;
    let content_digest = payload
        .as_deref()
        .and_then(extract_message_text)
        .map(|text| sha256_hex(text.as_bytes()));
    let changed = connection.execute(
        "UPDATE tdmem_native_staged_observation_v1 SET projected_content_sha256=?1 WHERE rowid=?2",
        params![content_digest, row_id],
    )?;
    if changed != 1 {
        return Err(StagedStoreError::LifecycleConflict("target row update"));
    }
    let changed = connection.execute(
        "UPDATE tdmem_native_staged_observation_v1
         SET valid_from_nanos=?1,valid_until_nanos=?2,superseded_nanos=?3,revoked_nanos=?4
         WHERE rowid=?5",
        params![
            validity.valid_from_utc_nanos,
            validity.valid_until_utc_nanos,
            validity.superseded_at_utc_nanos,
            validity.revoked_at_utc_nanos,
            row_id
        ],
    )?;
    if changed != 1 {
        return Err(StagedStoreError::LifecycleConflict("target row update"));
    }
    Ok(())
}

/// Captures the host-authenticated hygiene receipt for the exact bytes being
/// staged. The observation boundary has already checked this receipt against
/// the journal, but Native repeats the binding against its own record before
/// persisting it so a mismatched record cannot inherit another call's proof.
fn provider_view_sanitization_for_call(
    record: &StagedObservationRecord,
    call: &tracedecay_memory_provider_registry::ProviderCall,
) -> Result<ProviderViewSanitization, StagedStoreError> {
    let payload_sha256 = sha256_hex(&record.sanitized_payload);
    if call.payload.bytes != record.sanitized_payload || call.payload.sha256 != payload_sha256 {
        return Err(StagedStoreError::LifecycleConflict(
            "provider view payload binding",
        ));
    }
    call.validate()
        .map_err(|_| StagedStoreError::LifecycleConflict("sanitization receipt"))?;
    let receipt = call
        .sanitization()
        .ok_or(StagedStoreError::InvalidAdvisory("sanitization receipt"))?;
    let extensions_digest = receipt.extensions_digest().to_owned();
    receipt
        .verify_binding(&payload_sha256, &extensions_digest)
        .map_err(|_| StagedStoreError::LifecycleConflict("sanitization receipt"))?;
    Ok(ProviderViewSanitization {
        receipt_json: receipt.to_json(),
        extensions_digest,
    })
}

/// Direct store/replay callers predate the observation-call seam and do not
/// carry a host receipt. Give those provider-local paths an explicit,
/// byte-bound accepted receipt so their rows remain structurally complete;
/// production observation delivery always uses
/// [`provider_view_sanitization_for_call`] above.
fn direct_provider_view_sanitization(
    payload: &[u8],
) -> Result<ProviderViewSanitization, StagedStoreError> {
    let payload_sha256 = sha256_hex(payload);
    let receipt =
        PayloadSanitizationReceipt::new(PayloadSanitizationReceiptParts::accepted_unmodified(
            "native-direct-stage.v1",
            payload_sha256,
        ))
        .map_err(|_| StagedStoreError::InvalidAdvisory("sanitization receipt"))?;
    Ok(ProviderViewSanitization {
        extensions_digest: receipt.extensions_digest().to_owned(),
        receipt_json: receipt.to_json(),
    })
}

/// Returns the host observation journal next to a Native staged store.
///
/// The Native database is provider-local and therefore cannot authenticate its
/// own projection evidence. The sibling host journal is the durable authority
/// that admitted the exact transformed bytes. A missing journal is tolerated
/// only for the legacy/direct test surface, which has no host admission call;
/// once a host journal is present, a controlled row must match one of its
/// validated admitted envelopes.
fn host_observation_journal_path(staged_path: &Path) -> Option<PathBuf> {
    staged_path
        .parent()
        .and_then(Path::parent)
        .map(|provider_state_root| provider_state_root.join(HOST_OBSERVATION_JOURNAL_FILE_NAME))
}

fn has_host_projection_journal(staged_path: &Path) -> bool {
    host_observation_journal_path(staged_path).is_some_and(|path| path.is_file())
}

fn require_host_projection_journal(staged_path: &Path) -> Result<(), StagedStoreError> {
    if has_host_projection_journal(staged_path) {
        Ok(())
    } else {
        Err(StagedStoreError::LifecycleConflict(
            "host projection lineage unavailable",
        ))
    }
}

/// Converts the immutable journal facts to the identity carried alongside a
/// host projection. Every field is retained for ambiguity checks; reducing the
/// match to a receipt or provider-view digest would let two settlements share
/// one provider alias.
fn host_projection_identity(admitted: &AdmittedObservationV1) -> HostProjectionIdentity {
    HostProjectionIdentity {
        source_authority: admitted.source.source_authority.as_wire().to_owned(),
        source_event_id: admitted.source.source_event_id.clone(),
        source_event_revision: admitted.source.source_event_revision,
        source_event_sha256: admitted.source.source_event_sha256.clone(),
        source_stream: admitted.source.source_stream.as_str().to_owned(),
        source_sequence: admitted.source.source_sequence.0,
        commit_point_id: admitted.source.commit_point_id.clone(),
        settled_at_unix_micros: admitted.source.settled_at_unix_micros,
        settlement_proof_sha256: admitted.source.settlement_proof_sha256.clone(),
        provider_id: admitted.target.provider_id.as_str().to_owned(),
        provider_instance_id: admitted.target.provider_instance_id.clone(),
        registration_revision: admitted.target.registration_revision,
        ready_receipt_digest: admitted.target.ready_receipt_digest.clone(),
    }
}

fn host_projection_from_admitted(admitted: &AdmittedObservationV1) -> HostProjection {
    HostProjection {
        idempotency_key: admitted.idempotency_key.as_str().to_owned(),
        identity: host_projection_identity(admitted),
        sanitization: ProviderViewSanitization {
            receipt_json: admitted.sanitization.receipt_json.clone(),
            extensions_digest: admitted.extensions_digest.clone(),
        },
    }
}

/// Checks optional settlement fields copied into a provider envelope. Most
/// Native observations intentionally carry only source attribution here; the
/// complete settlement remains in the host journal. When an envelope carries
/// the typed settlement object, every field is compared without conflating the
/// original-source digest with the sanitized provider-view digest.
fn payload_settlement_matches_host(admitted: &AdmittedObservationV1, payload: &[u8]) -> bool {
    let Ok(envelope) = serde_json::from_slice::<Value>(payload) else {
        return false;
    };
    let Some(identity) = envelope.get("source_identity") else {
        return true;
    };
    if let Some(value) = identity.get("source_authority") {
        if value.as_str() != Some(admitted.source.source_authority.as_wire()) {
            return false;
        }
    }
    if let Some(value) = identity.get("source_event_id") {
        if value.as_str() != Some(admitted.source.source_event_id.as_str()) {
            return false;
        }
    }
    for value in [
        envelope.get("source_sequence"),
        identity.get("source_sequence"),
    ]
    .into_iter()
    .flatten()
    {
        if value.as_u64() != Some(admitted.source.source_sequence.0) {
            return false;
        }
    }
    if let Some(value) = identity.get("source_stream") {
        if value.as_str() != Some(admitted.source.source_stream.as_str()) {
            return false;
        }
    }
    let Some(settlement) = identity
        .get("canonical_settlement_receipt")
        .filter(|value| value.is_object())
    else {
        // A string receipt in the provider envelope is an application-level
        // reference. It is intentionally not compared to the journal's
        // settlement proof or to the sanitized payload digest.
        return true;
    };
    if settlement
        .get("source_authority")
        .is_some_and(|value| value.as_str() != Some(admitted.source.source_authority.as_wire()))
        || settlement
            .get("commit_point_id")
            .is_some_and(|value| value.as_str() != Some(admitted.source.commit_point_id.as_str()))
        || settlement
            .get("source_event_id")
            .is_some_and(|value| value.as_str() != Some(admitted.source.source_event_id.as_str()))
        || settlement
            .get("source_event_revision")
            .is_some_and(|value| value.as_u64() != Some(admitted.source.source_event_revision))
        || settlement.get("source_event_sha256").is_some_and(|value| {
            value.as_str() != Some(admitted.source.source_event_sha256.as_str())
        })
        || settlement
            .get("source_stream")
            .is_some_and(|value| value.as_str() != Some(admitted.source.source_stream.as_str()))
        || settlement
            .get("source_sequence")
            .is_some_and(|value| value.as_u64() != Some(admitted.source.source_sequence.0))
        || settlement
            .get("settled_at_unix_micros")
            .is_some_and(|value| value.as_i64() != Some(admitted.source.settled_at_unix_micros))
        || settlement
            .get("settlement_proof_sha256")
            .is_some_and(|value| {
                value.as_str() != Some(admitted.source.settlement_proof_sha256.as_str())
            })
    {
        return false;
    }
    true
}

/// A lifecycle call may use a retained row from an older registration after a
/// provider restart. A future registration is never compatible, and a row
/// admitted under the current registration must carry this call's ready
/// receipt. Observe/replay are delivery operations and stay pinned exactly to
/// the call's registration; only lifecycle inspection/mutation can use the
/// monotonic older-registration policy.
fn host_target_matches_call(
    admitted: &AdmittedObservationV1,
    call: Option<&tracedecay_memory_provider_registry::ProviderCall>,
) -> bool {
    if admitted.target.provider_instance_id != super::native_provider::PROVIDER_INSTANCE_ID {
        return false;
    }
    let Some(call) = call else {
        return true;
    };
    if admitted.target.provider_id != call.provider_id {
        return false;
    }
    use tracedecay_memory_provider_registry::ProviderOperation;
    match call.operation {
        ProviderOperation::Observe | ProviderOperation::Replay => {
            admitted.target.registration_revision == call.registration_revision
                && admitted.target.ready_receipt_digest == call.ready_receipt_sha256
        }
        _ => {
            admitted.target.registration_revision <= call.registration_revision
                && (admitted.target.registration_revision != call.registration_revision
                    || admitted.target.ready_receipt_digest == call.ready_receipt_sha256)
        }
    }
}

fn host_admission_matches_record(
    admitted: &AdmittedObservationV1,
    record: &StagedObservationRecord,
    call: Option<&tracedecay_memory_provider_registry::ProviderCall>,
    expected_key: Option<&str>,
) -> bool {
    if expected_key.is_some_and(|key| admitted.idempotency_key.as_str() != key) {
        return false;
    }
    let scope = &admitted.exact_scope;
    let scope_matches = scope.exact_scope_sha256() == record.scope.exact_scope_sha256()
        && scope.profile_id == record.scope.profile_id
        && scope.project_id == record.scope.project_id
        && scope.repository_identity == record.scope.repository_identity
        && scope.worktree_identity == record.scope.worktree_identity
        && scope.branch_identity == record.scope.branch_identity
        && scope.agent_session_id == record.scope.agent_session_id
        && scope.resolved_scope_digest == record.scope.resolved_scope_digest;
    let payload_matches = admitted.payload.bytes == record.sanitized_payload
        && admitted.payload.sha256 == sha256_hex(&record.sanitized_payload)
        && payload_record_identity_matches_record(record)
        && payload_settlement_matches_host(admitted, &record.sanitized_payload);
    let target_matches = host_target_matches_call(admitted, call);
    let observe_binding_matches = call.is_none_or(|call| {
        call.operation != tracedecay_memory_provider_registry::ProviderOperation::Observe
            || (call
                .sanitization()
                .is_some_and(|receipt| receipt.to_json() == admitted.sanitization.receipt_json)
                && call.extensions == admitted.extensions)
    });
    scope_matches
        && admitted.source.source_authority.as_wire() == record.source_authority
        && admitted.source.source_event_id == record.source_event_id
        && admitted.observation_kind.as_str() == record.observation_kind
        && admitted.payload.contract_id.as_str() == record.payload_contract
        && payload_matches
        && target_matches
        && observe_binding_matches
}

/// The provider envelope carries the original source revision as a textual
/// attribution value (`"r1"`, for example), while the host settlement carries
/// its independent numeric event revision. Bind the former to the Native row
/// here, and leave the latter to the canonical key/settlement object so the two
/// digest and revision domains cannot be accidentally conflated.
fn payload_record_identity_matches_record(record: &StagedObservationRecord) -> bool {
    let Ok(envelope) = serde_json::from_slice::<Value>(&record.sanitized_payload) else {
        return false;
    };
    let Some(original) = envelope.pointer("/source_identity/original_source") else {
        return record.source_revision.is_none();
    };
    let Some(source) = original.get("source") else {
        return false;
    };
    let payload_revision = match source.get("source_revision") {
        None | Some(Value::Null) => None,
        Some(Value::String(value)) => Some(value.as_str()),
        Some(_) => return false,
    };
    source.get("observation_id").and_then(Value::as_str) == Some(record.source_event_id.as_str())
        && payload_revision == record.source_revision.as_deref()
}

/// A lifecycle key is not a projection key. Translation is therefore allowed
/// only when the current host authority supplied a complete source admission;
/// querying by provider-view bytes or by a private stable ref is never enough.
fn validate_projection_translation_authority(
    record: &StagedObservationRecord,
    call: &tracedecay_memory_provider_registry::ProviderCall,
    admission: Option<&tracedecay_memory_provider_registry::CurrentAdvisoryAdmission>,
) -> Result<(), StagedStoreError> {
    let admission = admission.ok_or(StagedStoreError::LifecycleConflict(
        "host projection authority unavailable",
    ))?;
    admission
        .verify_for(call)
        .map_err(|_| StagedStoreError::LifecycleConflict("host projection authority binding"))?;
    let envelope: Value = serde_json::from_slice(&record.sanitized_payload)
        .map_err(|_| StagedStoreError::InvalidAdvisory("host projection envelope"))?;
    let original = envelope.pointer("/source_identity/original_source").ok_or(
        StagedStoreError::LifecycleConflict("host projection source authority"),
    )?;
    let matches = admission
        .history_sources
        .iter()
        .filter(|source| {
            super::provider_history::source_attribution_json(&source.attribution)
                .ok()
                .is_some_and(|candidate| candidate == *original)
        })
        .count();
    match matches {
        1 => Ok(()),
        0 => Err(StagedStoreError::LifecycleConflict(
            "host projection source authority",
        )),
        _ => Err(StagedStoreError::LifecycleConflict(
            "host projection source authority ambiguous",
        )),
    }
}

fn admitted_source_authority(envelope: &Value) -> String {
    envelope
        .pointer("/source_identity/source_authority")
        .and_then(Value::as_str)
        .unwrap_or("host_session")
        .to_owned()
}

/// Reads the host-authenticated projection for one staged record.
///
/// A canonical observation key is the primary lookup and the only lookup for
/// ordinary delivery. A lifecycle operation may begin with its own command
/// key, but it can translate that key only through a fresh ProviderHistory
/// admission and a unique, fully matching host settlement. The old
/// `(scope, source_event_id, provider-view bytes)` fallback is intentionally
/// gone: two settlements can share those bytes.
fn host_projection_for_record(
    staged_path: &Path,
    record: &StagedObservationRecord,
    call: Option<&tracedecay_memory_provider_registry::ProviderCall>,
    admission: Option<&tracedecay_memory_provider_registry::CurrentAdvisoryAdmission>,
) -> Result<Option<HostProjection>, StagedStoreError> {
    let Some(journal_path) = host_observation_journal_path(staged_path) else {
        return if call.is_some() {
            Err(StagedStoreError::LifecycleConflict(
                "host projection lineage unavailable",
            ))
        } else {
            Ok(None)
        };
    };
    if !journal_path.is_file() {
        return if call.is_some() {
            Err(StagedStoreError::LifecycleConflict(
                "host projection lineage unavailable",
            ))
        } else {
            Ok(None)
        };
    }

    let journal = SqliteObservationJournal::open_existing(
        &journal_path,
        super::observation_journey::ObservationJourneyPolicyV1::project_default().retention,
    )
    .map_err(|_| StagedStoreError::LifecycleConflict("host projection journal unavailable"))?;

    // First try the exact canonical key. A valid but missing key may only be
    // translated with fresh source authority; a valid key that names another
    // envelope is an alias attempt and is refused without a second lookup.
    let parsed_key = ObservationIdempotencyKeyV1::parse(&record.idempotency_key).ok();
    if let Some(key) = parsed_key.as_ref() {
        if let Some(admitted) = journal
            .read_admitted_observation_by_idempotency(key)
            .map_err(|_| {
                StagedStoreError::LifecycleConflict("host projection journal unavailable")
            })?
        {
            if host_admission_matches_record(
                &admitted,
                record,
                call,
                Some(record.idempotency_key.as_str()),
            ) {
                return Ok(Some(host_projection_from_admitted(&admitted)));
            }
            return Err(StagedStoreError::LifecycleConflict(
                "host projection identity mismatch",
            ));
        }
    }

    let Some(call) = call else {
        // Direct pre-V2 callers have no authority with which to translate a
        // command key. Their legacy store path remains structurally readable,
        // but never receives a host projection from an inferred match.
        return Ok(None);
    };
    validate_projection_translation_authority(record, call, admission)?;

    let host_connection = Connection::open_with_flags(
        &journal_path,
        OpenFlags::SQLITE_OPEN_READ_ONLY
            | OpenFlags::SQLITE_OPEN_NO_MUTEX
            | OpenFlags::SQLITE_OPEN_URI,
    )
    .map_err(|_| StagedStoreError::LifecycleConflict("host projection journal unavailable"))?;
    let mut statement = host_connection
        .prepare(
            "SELECT idempotency_key FROM tdmem_observation_journal_v1
             WHERE exact_scope_sha256=?1 AND source_event_id=?2 AND provider_id=?3
               AND payload_bytes IS NOT NULL
             ORDER BY idempotency_key LIMIT 17",
        )
        .map_err(|_| StagedStoreError::LifecycleConflict("host projection journal unavailable"))?;
    let candidate_keys = statement
        .query_map(
            params![
                record.scope.exact_scope_sha256(),
                record.source_event_id,
                call.provider_id.as_str(),
            ],
            |row| row.get::<_, String>(0),
        )?
        .collect::<Result<Vec<_>, _>>()?;
    let mut matches = Vec::new();
    for candidate in candidate_keys {
        let key = ObservationIdempotencyKeyV1::parse(&candidate)
            .map_err(|_| StagedStoreError::LifecycleConflict("host projection journal key"))?;
        let Some(admitted) = journal
            .read_admitted_observation_by_idempotency(&key)
            .map_err(|_| {
                StagedStoreError::LifecycleConflict("host projection journal unavailable")
            })?
        else {
            continue;
        };
        if host_admission_matches_record(&admitted, record, Some(call), None) {
            matches.push(host_projection_from_admitted(&admitted));
        }
    }
    match matches.as_slice() {
        [] => Err(StagedStoreError::LifecycleConflict(
            "host projection lineage",
        )),
        [projection] => Ok(Some(projection.clone())),
        matches => {
            // If the current registration has one exact ready receipt, it is
            // the only unambiguous restart translation. Any remaining pair —
            // especially differing stream/sequence/settlement proof — fails
            // closed even when provider-view bytes are identical.
            let current: Vec<_> = matches
                .iter()
                .filter(|projection| {
                    projection.identity.registration_revision == call.registration_revision
                        && projection.identity.ready_receipt_digest == call.ready_receipt_sha256
                })
                .collect();
            if let [projection] = current.as_slice() {
                Ok(Some((*projection).clone()))
            } else {
                Err(StagedStoreError::LifecycleConflict(
                    "host projection lineage ambiguous",
                ))
            }
        }
    }
}

/// Binds a provider-view envelope's complete original-source attribution to
/// the fresh host admission before a public sanitization receipt is accepted.
///
/// The receipt proves only the transformed bytes. The trusted source list is
/// the host-owned lineage edge that prevents a provider-local payload from
/// relabeling itself with an arbitrary source (or swapping to another source
/// that happens to share a checkout and revision). Direct legacy staging has
/// no host call and keeps its historical structural-only behavior.
fn validate_stage_source_admission(
    original_source: Option<&Value>,
    call: Option<&tracedecay_memory_provider_registry::ProviderCall>,
    admission: Option<&tracedecay_memory_provider_registry::CurrentAdvisoryAdmission>,
) -> Result<(), StagedStoreError> {
    let Some(original_source) = original_source else {
        return Ok(());
    };
    let Some(admission) = admission else {
        if call.is_some() {
            return Err(StagedStoreError::LifecycleConflict(
                "source authority unavailable",
            ));
        }
        return Ok(());
    };
    if let Some(call) = call {
        admission
            .verify_for(call)
            .map_err(|_| StagedStoreError::LifecycleConflict("source authority binding"))?;
    }
    let matches: Vec<_> = admission
        .history_sources
        .iter()
        .filter(|source| {
            super::provider_history::source_attribution_json(&source.attribution)
                .ok()
                .is_some_and(|candidate| candidate == *original_source)
        })
        .collect();
    match matches.as_slice() {
        [_] => Ok(()),
        [] => Err(StagedStoreError::LifecycleConflict(
            "source projection lineage",
        )),
        _ => Err(StagedStoreError::LifecycleConflict(
            "source projection lineage ambiguous",
        )),
    }
}

/// Checks a retained row against the host admission that produced its
/// transformed provider view. Tombstones intentionally skip this check: their
/// content and hygiene binding have been purged, and inspection remains
/// possible from the durable identity/effect evidence alone.
fn validate_host_projection_for_row(
    staged_path: &Path,
    scope: &ExactScopeFields,
    idempotency_key: &str,
    source_authority: &str,
    source_event_id: &str,
    source_revision: Option<&str>,
    observation_kind: &str,
    payload_contract: &str,
    payload: &[u8],
    payload_sha256: &str,
    receipt_json: &str,
    extensions_digest: &str,
    call: Option<&tracedecay_memory_provider_registry::ProviderCall>,
    admission: Option<&tracedecay_memory_provider_registry::CurrentAdvisoryAdmission>,
) -> Result<(), StagedStoreError> {
    let record = StagedObservationRecord {
        scope: scope.clone(),
        idempotency_key: idempotency_key.to_owned(),
        source_authority: source_authority.to_owned(),
        source_event_id: source_event_id.to_owned(),
        source_revision: source_revision.map(str::to_owned),
        observation_kind: observation_kind.to_owned(),
        payload_contract: payload_contract.to_owned(),
        sanitized_payload: payload.to_vec(),
        operation_id: "host-lineage-validation".to_owned(),
        request_identity: "host-lineage-validation".to_owned(),
        admitted_at_unix_ms: 0,
    };
    let Some(host_projection) = host_projection_for_record(staged_path, &record, call, admission)?
    else {
        return Ok(());
    };
    if host_projection.sanitization.receipt_json != receipt_json
        || host_projection.sanitization.extensions_digest != extensions_digest
    {
        return Err(StagedStoreError::LifecycleConflict(
            "host projection lineage",
        ));
    }
    if payload_sha256 != sha256_hex(payload) {
        return Err(StagedStoreError::PayloadDigestMismatch {
            idempotency_key: idempotency_key.to_owned(),
            stored_payload_sha256: payload_sha256.to_owned(),
        });
    }
    Ok(())
}

fn stage_in_transaction(
    transaction: &rusqlite::Transaction<'_>,
    record: StagedObservationRecord,
    retention: StagedRetentionPolicyV1,
    provider_view: &ProviderViewSanitization,
    call: Option<&tracedecay_memory_provider_registry::ProviderCall>,
    admission: Option<&tracedecay_memory_provider_registry::CurrentAdvisoryAdmission>,
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
    validate_stage_source_admission(original_source.as_ref(), call, admission)?;
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
                 admitted_sequence, admitted_at_unix_ms, tombstone, actual_revision, original_source,
                 semantic_sha256, sanitization_receipt_json, sanitization_extensions_digest
             ) VALUES (
                 ?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16,
                 ?17, ?18, ?19, ?20, ?21, ?22, ?23, 0, ?24, ?25, ?26, ?27, ?28
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
            provider_view.receipt_json.as_str(),
            provider_view.extensions_digest.as_str(),
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
    "sanitization_receipt_json",
    "sanitization_extensions_digest",
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
    staged_path: &Path,
) -> Result<(Value, bool), StagedStoreError> {
    // Restores can introduce provider-view rows in bulk. Require the durable
    // host journal before reading any snapshot bytes so a provider-local
    // snapshot cannot mint its own projection lineage.
    require_host_projection_journal(staged_path)?;
    validate_resident_original_sources(
        connection,
        &call.exact_scope,
        staged_path,
        call,
        admission,
    )?;
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
        if exact_scope_from_value(row)? != call.exact_scope || row["exact_scope_sha256"] != scope {
            return Err(StagedStoreError::LifecycleConflict("snapshot row scope"));
        }
        let mut restored_row = row.clone();
        let row_field_count = row.as_object().map_or(0, serde_json::Map::len);
        let legacy_provider_view_evidence =
            row_field_count == SNAPSHOT_COLUMNS.len().saturating_sub(2);
        if legacy_provider_view_evidence {
            // Older snapshots predate the provider-view hygiene evidence. They
            // remain importable for ordinary state recovery, but their rows
            // cannot later be named by a retained locator until a fresh,
            // receipt-bearing observation is staged.
            restored_row["sanitization_receipt_json"] = Value::Null;
            restored_row["sanitization_extensions_digest"] = Value::Null;
        } else if row_field_count != SNAPSHOT_COLUMNS.len() {
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
                    "sanitization_receipt_json",
                    "sanitization_extensions_digest",
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
            if !legacy_provider_view_evidence {
                let receipt_json = restored_row["sanitization_receipt_json"].as_str().ok_or(
                    StagedStoreError::LifecycleConflict("snapshot sanitization receipt"),
                )?;
                let extensions_digest = restored_row["sanitization_extensions_digest"]
                    .as_str()
                    .filter(|digest| lower_hex_digest(digest))
                    .ok_or(StagedStoreError::LifecycleConflict(
                        "snapshot sanitization extensions",
                    ))?;
                let receipt =
                    PayloadSanitizationReceipt::from_json(receipt_json).map_err(|_| {
                        StagedStoreError::LifecycleConflict("snapshot sanitization receipt")
                    })?;
                receipt
                    .verify_binding(
                        restored_row["payload_sha256"]
                            .as_str()
                            .ok_or(StagedStoreError::InvalidAdvisory("snapshot payload digest"))?,
                        extensions_digest,
                    )
                    .map_err(|_| {
                        StagedStoreError::LifecycleConflict("snapshot sanitization receipt")
                    })?;
                validate_host_projection_for_row(
                    staged_path,
                    &call.exact_scope,
                    required_text(&restored_row, "idempotency_key")?,
                    required_text(&restored_row, "source_authority")?,
                    required_text(&restored_row, "source_event_id")?,
                    restored_row["actual_revision"].as_str(),
                    required_text(&restored_row, "observation_kind")?,
                    required_text(&restored_row, "payload_contract")?,
                    &payload,
                    required_text(&restored_row, "payload_sha256")?,
                    receipt_json,
                    extensions_digest,
                    Some(call),
                    admission,
                )?;
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
                connection.execute("UPDATE tdmem_native_staged_observation_v1 SET sanitized_payload=NULL,tombstone=1,feedback=0,feedback_suppressed=0,original_source=NULL,validity_override=NULL,valid_from_nanos=NULL,valid_until_nanos=NULL,superseded_nanos=NULL,revoked_nanos=NULL,projected_content_sha256=NULL,sanitization_receipt_json=NULL,sanitization_extensions_digest=NULL WHERE provider_reference=?1",params![reference])?;
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
    connection.execute("UPDATE tdmem_native_staged_observation_v1 SET sanitized_payload=NULL,tombstone=1,feedback=0,sanitization_receipt_json=NULL,sanitization_extensions_digest=NULL WHERE exact_scope_sha256=?1 AND provider_reference NOT IN(SELECT value FROM json_each(?2))",params![scope,keep_json])?;
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

/// Revalidates every replay source before a duplicate operation journal lookup.
///
/// The operation journal is an idempotency record, not a capability cache. A
/// successful replay may be retried with the same key, but the current host
/// admission and privacy/deletion fence still have to authorize that retry.
fn preflight_replay(
    transaction: &rusqlite::Transaction<'_>,
    call: &tracedecay_memory_provider_registry::ProviderCall,
    request: &Value,
    admission: Option<&tracedecay_memory_provider_registry::CurrentAdvisoryAdmission>,
    staged_path: &Path,
) -> Result<(), StagedStoreError> {
    use tracedecay_memory_provider_registry::SourceDisposition;

    let admission = admission.ok_or(StagedStoreError::LifecycleConflict(
        "replay authority unavailable",
    ))?;
    admission
        .verify_for(call)
        .map_err(|_| StagedStoreError::LifecycleConflict("replay authority binding"))?;
    let grant = request
        .get("history_grant")
        .ok_or(StagedStoreError::InvalidAdvisory("replay history grant"))?;
    let grant_sources = grant
        .get("sources")
        .and_then(Value::as_array)
        .ok_or(StagedStoreError::InvalidAdvisory("replay grant sources"))?;
    let items = request
        .get("resolved_observations")
        .and_then(Value::as_array)
        .filter(|items| !items.is_empty() && items.len() <= 4096)
        .ok_or(StagedStoreError::InvalidAdvisory("replay observations"))?;
    let refs = request
        .get("observation_batch_refs")
        .and_then(Value::as_array)
        .ok_or(StagedStoreError::InvalidAdvisory(
            "replay receipt inventory",
        ))?;
    if grant_sources.len() != items.len() {
        return Err(StagedStoreError::LifecycleConflict(
            "replay source coverage",
        ));
    }
    if refs.len() != items.len() {
        return Err(StagedStoreError::LifecycleConflict(
            "replay receipt coverage",
        ));
    }
    let mut seen_receipts = BTreeSet::new();
    let mut seen_sources = BTreeSet::new();
    for item in items {
        let envelope = item
            .get("observation")
            .ok_or(StagedStoreError::InvalidAdvisory("replay observation"))?;
        let receipt = required_text(item, "receipt_ref")?;
        if !refs.iter().any(|value| value.as_str() == Some(receipt))
            || !seen_receipts.insert(receipt.to_owned())
        {
            return Err(StagedStoreError::LifecycleConflict(
                "replay receipt binding",
            ));
        }
        let attribution = envelope
            .pointer("/source_identity/original_source")
            .ok_or(StagedStoreError::InvalidAdvisory("replay attribution"))?;
        validate_attribution(attribution)?;
        if !grant_sources
            .iter()
            .any(|source| source.get("attribution") == Some(attribution))
        {
            return Err(StagedStoreError::LifecycleConflict(
                "ungranted replay source",
            ));
        }
        let trusted: Vec<_> = admission
            .history_sources
            .iter()
            .filter(|source| {
                super::provider_history::source_attribution_json(&source.attribution)
                    .ok()
                    .is_some_and(|candidate| candidate == *attribution)
            })
            .collect();
        let trusted = match trusted.as_slice() {
            [trusted] => trusted,
            [] => {
                return Err(StagedStoreError::LifecycleConflict(
                    "ungranted replay source",
                ));
            }
            _ => {
                return Err(StagedStoreError::LifecycleConflict(
                    "ambiguous replay source",
                ));
            }
        };
        if !matches!(
            trusted.current_disposition.state,
            SourceDisposition::Available | SourceDisposition::Superseded
        ) {
            return Err(StagedStoreError::LifecycleConflict(
                "replay source unavailable",
            ));
        }
        let source_key = required_text(&attribution["source"], "source_key")?;
        let source_sequence = item
            .get("source_sequence")
            .and_then(Value::as_u64)
            .ok_or(StagedStoreError::InvalidAdvisory("replay source sequence"))?;
        if attribution["source_sequence"].as_u64() != Some(source_sequence)
            || envelope["source_sequence"].as_u64() != Some(source_sequence)
            || !seen_sources
                .insert(required_text(&attribution["source"], "observation_id")?.to_owned())
        {
            return Err(StagedStoreError::LifecycleConflict(
                "replay source sequence",
            ));
        }
        if source_deleted(
            transaction,
            &call.exact_scope,
            source_key,
            Some(attribution),
        )? {
            return Err(StagedStoreError::PrivacyDeleted);
        }
        // Replay is a delivery of an already admitted provider view. Before a
        // duplicate operation receipt can be reused, bind the item's exact
        // canonical key and all host settlement facts to the sibling journal.
        // This also rejects source-only replay envelopes that lack the key.
        let idempotency_key = required_text(item, "idempotency_key")?;
        ObservationIdempotencyKeyV1::parse(idempotency_key)
            .map_err(|_| StagedStoreError::LifecycleConflict("replay canonical key"))?;
        let request_identity = item
            .get("request_identity")
            .and_then(Value::as_str)
            .unwrap_or(call.request_id.as_str());
        let observation_kind = required_text(envelope, "observation_kind")?;
        let payload_contract = required_text(envelope, "payload_contract")?;
        let payload = serde_json::to_vec(envelope)
            .map_err(|_| StagedStoreError::InvalidAdvisory("replay observation"))?;
        let record = StagedObservationRecord {
            scope: call.exact_scope.clone(),
            idempotency_key: idempotency_key.to_owned(),
            source_authority: admitted_source_authority(envelope),
            source_event_id: required_text(&attribution["source"], "observation_id")?.to_owned(),
            source_revision: attribution["source"]["source_revision"]
                .as_str()
                .map(str::to_owned),
            observation_kind: observation_kind.to_owned(),
            payload_contract: payload_contract.to_owned(),
            sanitized_payload: payload,
            operation_id: call.operation_id.clone(),
            request_identity: request_identity.to_owned(),
            admitted_at_unix_ms: super::native_provider::unix_millis_now(),
        };
        let _ = host_projection_for_record(staged_path, &record, Some(call), Some(admission))?
            .ok_or(StagedStoreError::LifecycleConflict(
                "host projection lineage unavailable",
            ))?;
    }
    Ok(())
}

fn replay_advisory(
    transaction: &rusqlite::Transaction<'_>,
    call: &tracedecay_memory_provider_registry::ProviderCall,
    request: &Value,
    retention: StagedRetentionPolicyV1,
    admission: Option<&tracedecay_memory_provider_registry::CurrentAdvisoryAdmission>,
    staged_path: &Path,
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
        let mut record = StagedObservationRecord {
            scope: call.exact_scope.clone(),
            // Replay metadata is carried beside the retained observation by
            // ProviderHistory. The inner observation is intentionally kept
            // byte-for-byte canonical and does not mint a second key.
            idempotency_key: required_text(item, "idempotency_key")?.to_owned(),
            source_authority: admitted_source_authority(envelope),
            source_event_id: required_text(original, "observation_id")?.to_owned(),
            source_revision: original["source_revision"].as_str().map(str::to_owned),
            observation_kind: required_text(envelope, "observation_kind")?.to_owned(),
            payload_contract: required_text(envelope, "payload_contract")?.to_owned(),
            sanitized_payload: bytes,
            operation_id: call.operation_id.clone(),
            request_identity: item
                .get("request_identity")
                .and_then(Value::as_str)
                .unwrap_or(call.request_id.as_str())
                .to_owned(),
            admitted_at_unix_ms: super::native_provider::unix_millis_now(),
        };
        let projection = host_projection_for_record(staged_path, &record, Some(call), admission)?
            .ok_or(StagedStoreError::LifecycleConflict(
            "host projection lineage unavailable",
        ))?;
        record.idempotency_key = projection.idempotency_key;
        let provider_view = projection.sanitization;
        match stage_in_transaction(
            transaction,
            record,
            retention,
            &provider_view,
            Some(call),
            admission,
        )? {
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
