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
//! * **Same-session recall only, in this slice.** Rows are addressed by the full
//!   seven-field exact coding scope, and `exact_coding_scope` admission requires
//!   byte-equality on `agent_session_id` and `resolved_scope_digest`. A row
//!   staged in session A therefore cannot be recalled in session B even in the
//!   identical repository, worktree, and branch. That is a deliberate, honest
//!   limitation of this slice (see ADR-0002); the durable checkout-level binding
//!   is a separate bead. Because all seven scope fields are stored as explicit
//!   columns rather than only their digest, that later binding needs a new
//!   index, not a data migration.
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
const SCHEMA_VERSION: i64 = 1;

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
CREATE UNIQUE INDEX IF NOT EXISTS tdmem_native_staged_observation_source_v1
    ON tdmem_native_staged_observation_v1 (
        exact_scope_sha256, source_authority, source_event_id, source_revision,
        payload_sha256);

CREATE UNIQUE INDEX IF NOT EXISTS tdmem_native_staged_observation_sequence_v1
    ON tdmem_native_staged_observation_v1 (admitted_sequence);

CREATE INDEX IF NOT EXISTS tdmem_native_staged_observation_recall_v1
    ON tdmem_native_staged_observation_v1 (exact_scope_sha256, tombstone, admitted_sequence);
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
    pub(crate) source_revision: u64,
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
    pub(crate) source_revision: u64,
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
}

impl StagedObservationStore {
    /// Opens (creating if absent) the store under the host-granted
    /// provider-state root, applying schema v1.
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
        record.validate()?;
        let exact_scope_sha256 = record.scope.exact_scope_sha256();
        let payload_sha256 = sha256_hex(&record.sanitized_payload);
        let source_revision = i64::try_from(record.source_revision).map_err(|_| {
            StagedStoreError::ValueOutOfRange {
                field: "source_revision",
            }
        })?;

        let mut guard = self.connection()?;
        let transaction = guard.transaction_with_behavior(TransactionBehavior::Immediate)?;

        let existing = transaction
            .query_row(
                "SELECT observation_kind, payload_sha256, provider_reference, receipt, \
                        effect_digest, admitted_sequence, operation_id \
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
            if stored_payload_sha256 != payload_sha256 {
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
                   AND source_event_id = ?3 AND source_revision = ?4 AND payload_sha256 = ?5",
                params![
                    exact_scope_sha256,
                    record.source_authority,
                    record.source_event_id,
                    source_revision,
                    payload_sha256
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

        let previous: i64 = transaction.query_row(
            "SELECT COALESCE(MAX(admitted_sequence), 0) FROM tdmem_native_staged_observation_v1",
            [],
            |row| row.get(0),
        )?;
        let admitted_sequence = u64::try_from(previous.saturating_add(1)).map_err(|_| {
            StagedStoreError::ValueOutOfRange {
                field: "admitted_sequence",
            }
        })?;
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
                 admitted_sequence, admitted_at_unix_ms, tombstone
             ) VALUES (
                 ?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16,
                 ?17, ?18, ?19, ?20, ?21, ?22, ?23, 0
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
                source_revision,
                record.observation_kind,
                record.payload_contract,
                record.sanitized_payload,
                payload_sha256,
                record.operation_id,
                record.request_identity,
                evidence.provider_reference,
                evidence.receipt,
                evidence.effect_digest,
                i64::try_from(admitted_sequence).unwrap_or(i64::MAX),
                record.admitted_at_unix_ms,
            ],
        )?;

        evict_scope_overflow(
            &transaction,
            &exact_scope_sha256,
            self.retention.maximum_content_rows_per_scope,
        )?;

        #[cfg(test)]
        {
            if self
                .fail_next_commit
                .swap(false, std::sync::atomic::Ordering::SeqCst)
            {
                // Dropping the transaction rolls it back: no row, no evidence.
                return Err(StagedStoreError::InjectedCommitFault);
            }
        }

        transaction.commit()?;
        Ok(StagedOutcome::Committed(evidence))
    }

    /// Returns the staged rows of exactly this exact scope, best first.
    ///
    /// Only rows whose stored `exact_scope_sha256` equals this scope's digest
    /// and whose content survives (`tombstone = 0`) are returned; a row whose
    /// message text cannot be extracted contract-aware is skipped rather than
    /// answered with envelope JSON. Ordering is `(score desc, admitted_sequence
    /// desc, idempotency_key asc)` and is therefore total and reproducible.
    ///
    /// # Errors
    ///
    /// Returns [`StagedStoreError`] on `SQLite` failure, or
    /// [`StagedStoreError::ScopeDigestMismatch`] when a stored row's scope
    /// columns do not re-derive its stored digest.
    pub(crate) fn recall(
        &self,
        scope: &ExactScopeFields,
        query: &str,
        limit: usize,
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
        let exact_scope_sha256 = scope.exact_scope_sha256();
        let query_tokens = normalized_tokens(query);

        let guard = self.connection()?;
        let mut statement = guard.prepare(
            "SELECT idempotency_key, profile_id, project_id, repository_identity, \
                    worktree_identity, branch_identity, agent_session_id, resolved_scope_digest, \
                    source_authority, source_event_id, source_revision, observation_kind, \
                    payload_contract, sanitized_payload, payload_sha256, operation_id, \
                    request_identity, provider_reference, receipt, effect_digest, \
                    admitted_sequence, admitted_at_unix_ms \
             FROM tdmem_native_staged_observation_v1 \
             WHERE exact_scope_sha256 = ?1 AND tombstone = 0 \
             ORDER BY admitted_sequence ASC",
        )?;
        let mut raw = Vec::new();
        let mut rows = statement.query(params![exact_scope_sha256])?;
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
            if stored_scope.exact_scope_sha256() != exact_scope_sha256 {
                return Err(StagedStoreError::ScopeDigestMismatch {
                    idempotency_key,
                    stored_exact_scope_sha256: exact_scope_sha256,
                });
            }
            let payload: Vec<u8> = row.get(13)?;
            let Some(message_text) = extract_message_text(&payload) else {
                continue;
            };
            let source_revision = u64::try_from(row.get::<_, i64>(10)?).map_err(|_| {
                StagedStoreError::ValueOutOfRange {
                    field: "source_revision",
                }
            })?;
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
                payload_sha256: row.get(14)?,
                message_text,
                operation_id: row.get(15)?,
                request_identity: row.get(16)?,
                provider_reference: row.get(17)?,
                receipt: row.get(18)?,
                effect_digest: row.get(19)?,
                admitted_sequence,
                admitted_at_unix_ms: row.get(21)?,
                score: 0.0,
            });
        }
        drop(rows);
        drop(statement);
        drop(guard);

        // Recency is rank over the rows of this scope, oldest first, so it is a
        // function of the stored order and never of a wall clock.
        let total = raw.len();
        for (position, candidate) in raw.iter_mut().enumerate() {
            #[allow(clippy::cast_precision_loss)]
            let recency = (position as f64 + 1.0) / (total as f64);
            let lexical = lexical_overlap(&query_tokens, &candidate.message_text);
            candidate.score = RECENCY_WEIGHT.mul_add(recency, LEXICAL_WEIGHT * lexical);
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
    let facts = object.get("canonical_payload")?.get("facts")?.as_array()?;
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
            source_revision: revision,
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
        let beta = scope("session.beta");

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
        // a1 was evicted by the cap of two, and b1 belongs to another session.
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
                    "UPDATE tdmem_native_staged_observation_v1 SET branch_identity = ?1",
                    params!["refs/heads/tampered"],
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
}
