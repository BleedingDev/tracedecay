//! Provider-owned durable storage for one NCM namespace.
//!
//! A namespace is an explicitly-created SQLite database.  Opening never
//! manufactures an empty state: the database schema, metadata, identities,
//! and integrity check must all pass before an existing state is returned.
//! Mutations use one `BEGIN IMMEDIATE` transaction and one `COMMIT`; dropping
//! a mutation lets rusqlite roll the transaction back.

use crate::ports::StateRoot;
use rusqlite::{
    Connection, OpenFlags, OptionalExtension, Row, Transaction, TransactionBehavior, params,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::fmt;
use std::fs;
use std::path::{Path, PathBuf};
use std::time::Duration;
use tracedecay_memory_ncm_core::types::{
    AFFECT_DIM, AffectVector, AlgorithmIdentity, EMBEDDING_DIM, LTM_KEY_DIM, RecordId, SourceId,
};

const SCHEMA_VERSION: &str = "1";
const JOURNAL_SIZE_LIMIT: u64 = 16 * 1024 * 1024;
const MAX_RECORD_BYTES: usize = 16 * 1024;
const NORMAL_WRITE_HEADROOM: u64 = 64 * 1024;
const META_SCHEMA_VERSION: &str = "schema_version";
const META_ALGORITHM_PROFILE: &str = "algorithm_profile";
const META_ALGORITHM_CONFIG_SHA256: &str = "algorithm_config_sha256";
const META_PROJECTION_SHA256: &str = "projection_sha256";
const META_PROJECTION: &str = "projection";
const META_ENCODER_MODEL: &str = "encoder_model";
const META_ENCODER_ARTIFACT_SHA256: &str = "encoder_artifact_sha256";
const META_SEED: &str = "seed";
const META_CONFIG_JSON: &str = "config_json";
const META_EPOCH: &str = "epoch";
const META_COMMIT_SEQ: &str = "commit_seq";
const META_TICK: &str = "tick";
const META_FATIGUE: &str = "fatigue";
const META_STEPS_SINCE_CONSOLIDATION: &str = "steps_since_consolidation";
const META_LAST_MAINTENANCE: &str = "last_maintenance";
const META_NEXT_RECORD_ID: &str = "next_record_id";

const TABLE_NAMES: [&str; 6] = [
    "capsules",
    "checkpoints",
    "events",
    "fence",
    "meta",
    "revocations",
];

/// A committed mutation sequence number.
///
/// This is an alias rather than a second identity type so callers can compare
/// it directly with SQLite event and checkpoint sequence values.
pub type CommitSeq = u64;

/// Compatibility identity persisted with a namespace.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct StoreIdentity {
    /// Algorithm profile and canonical configuration digest.
    pub algorithm: AlgorithmIdentity,
    /// Digest of the persisted projection bytes.
    pub projection_sha256: String,
    /// Encoder model name that produced the embeddings.
    pub encoder_model: String,
    /// Digest of the encoder artifact.
    pub encoder_artifact_sha256: String,
    /// Projection matrices and biases used by the kernel.
    pub projection_bytes: Vec<u8>,
    /// Seed used to initialize deterministic state.
    pub seed: u64,
    /// Canonical NCM configuration JSON.
    pub config_json: String,
}

/// Status of a source capsule.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum CapsuleStatus {
    /// A record that participates in ordinary replay and recall.
    Valid,
    /// A record superseded by a correction.
    Superseded,
    /// A privacy tombstone whose retained content has been cleared.
    Revoked,
}

impl CapsuleStatus {
    fn as_str(self) -> &'static str {
        match self {
            Self::Valid => "valid",
            Self::Superseded => "superseded",
            Self::Revoked => "revoked",
        }
    }

    fn parse(value: String) -> Result<Self, StoreError> {
        match value.as_str() {
            "valid" => Ok(Self::Valid),
            "superseded" => Ok(Self::Superseded),
            "revoked" => Ok(Self::Revoked),
            other => Err(StoreError::Corrupt(format!(
                "unknown capsule status {other:?}"
            ))),
        }
    }
}

/// A source capsule ready for insertion.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Capsule {
    /// Opaque host-admitted source identity.
    pub source_id: SourceId,
    /// Canonical key text.
    pub key_text: String,
    /// Canonical value text.
    pub value_text: String,
    /// Four-channel affect signal.
    pub affect: AffectVector,
    /// Surprise signal.
    pub surprise: f32,
    /// Initial write intensity.
    pub intensity: f32,
    /// Normalized 384-dimensional key embedding.
    pub key_embedding: Vec<f32>,
    /// Normalized 384-dimensional value embedding.
    pub value_embedding: Vec<f32>,
    /// Canonical 64-dimensional LTM key computed at observation time.
    pub ltm_key: Vec<f32>,
    /// JSON provenance retained with the capsule.
    pub provenance: String,
}

impl Capsule {
    /// Builds a capsule with the supplied durable fields.
    #[must_use]
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        source_id: SourceId,
        key_text: String,
        value_text: String,
        affect: AffectVector,
        surprise: f32,
        intensity: f32,
        key_embedding: Vec<f32>,
        value_embedding: Vec<f32>,
        ltm_key: Vec<f32>,
        provenance: String,
    ) -> Self {
        Self {
            source_id,
            key_text,
            value_text,
            affect,
            surprise,
            intensity,
            key_embedding,
            value_embedding,
            ltm_key,
            provenance,
        }
    }

    fn validate(&self) -> Result<(), StoreError> {
        let text_bytes = self
            .key_text
            .len()
            .checked_add(self.value_text.len())
            .ok_or_else(|| StoreError::InvalidInput("record text length overflow".to_owned()))?;
        if text_bytes > MAX_RECORD_BYTES {
            return Err(StoreError::BudgetExceeded);
        }
        if self.key_embedding.len() != EMBEDDING_DIM {
            return Err(StoreError::InvalidInput(format!(
                "key embedding dimension {} != {EMBEDDING_DIM}",
                self.key_embedding.len()
            )));
        }
        if self.value_embedding.len() != EMBEDDING_DIM {
            return Err(StoreError::InvalidInput(format!(
                "value embedding dimension {} != {EMBEDDING_DIM}",
                self.value_embedding.len()
            )));
        }
        if self.ltm_key.len() != LTM_KEY_DIM {
            return Err(StoreError::InvalidInput(format!(
                "LTM key dimension {} != {LTM_KEY_DIM}",
                self.ltm_key.len()
            )));
        }
        if !self.key_embedding.iter().all(|value| value.is_finite()) {
            return Err(StoreError::InvalidInput(
                "key embedding contains a non-finite value".to_owned(),
            ));
        }
        if !self.value_embedding.iter().all(|value| value.is_finite()) {
            return Err(StoreError::InvalidInput(
                "value embedding contains a non-finite value".to_owned(),
            ));
        }
        if !self.ltm_key.iter().all(|value| value.is_finite()) {
            return Err(StoreError::InvalidInput(
                "LTM key contains a non-finite value".to_owned(),
            ));
        }
        if !self.affect.0.iter().all(|value| value.is_finite()) {
            return Err(StoreError::InvalidInput(
                "affect contains a non-finite value".to_owned(),
            ));
        }
        if !self.surprise.is_finite() || !self.intensity.is_finite() {
            return Err(StoreError::InvalidInput(
                "capsule scalar contains a non-finite value".to_owned(),
            ));
        }
        validate_json(&self.provenance, "provenance")
    }
}

/// A capsule read back from durable storage.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct StoredCapsule {
    /// Monotonic namespace-local record identity.
    pub record_id: RecordId,
    /// Opaque host-admitted source identity.
    pub source_id: SourceId,
    /// Canonical key text, empty after revocation.
    pub key_text: String,
    /// Canonical value text, empty after revocation.
    pub value_text: String,
    /// Four-channel affect signal.
    pub affect: AffectVector,
    /// Surprise signal.
    pub surprise: f32,
    /// Initial write intensity.
    pub intensity: f32,
    /// Key embedding, empty after revocation.
    pub key_embedding: Vec<f32>,
    /// Value embedding, empty after revocation.
    pub value_embedding: Vec<f32>,
    /// Canonical LTM key, empty after revocation.
    pub ltm_key: Vec<f32>,
    /// JSON provenance retained with the capsule.
    pub provenance: String,
    /// Durable lifecycle status.
    pub status: CapsuleStatus,
    /// Mutation sequence that inserted the capsule.
    pub commit_seq: CommitSeq,
}

/// A committed event journal row.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Event {
    /// Monotonic event sequence.
    pub seq: u64,
    /// Event kind.
    pub kind: String,
    /// Optional idempotency key.
    pub idempotency_key: Option<String>,
    /// Digest of the canonical operation payload.
    pub payload_sha256: String,
    /// JSON receipt returned for the operation.
    pub receipt: String,
    /// Logical tick at which the event was created.
    pub created_tick: u64,
}

/// A serialized kernel checkpoint.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Checkpoint {
    /// Commit sequence represented by the checkpoint.
    pub seq: CommitSeq,
    /// Privacy/state epoch represented by the checkpoint.
    pub epoch: u64,
    /// Opaque serialized kernel state.
    pub state: Vec<u8>,
}

/// A source revocation authority row.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Revocation {
    /// Revoked source identity.
    pub source_id: SourceId,
    /// Privacy epoch at revocation.
    pub epoch: u64,
    /// Commit sequence that recorded the revocation.
    pub seq: CommitSeq,
}

/// Persisted scheduler and generation metadata.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct StoreMeta {
    /// Privacy/state epoch.
    pub epoch: u64,
    /// Last committed mutation sequence.
    pub commit_seq: CommitSeq,
    /// Logical learning tick.
    pub tick: u64,
    /// Consolidation fatigue.
    pub fatigue: f32,
    /// Ticks since the last consolidation.
    pub steps_since_consolidation: u64,
    /// Identity of the last maintenance operation, if any.
    pub last_maintenance: Option<String>,
}

impl Default for StoreMeta {
    fn default() -> Self {
        Self {
            epoch: 1,
            commit_seq: 0,
            tick: 0,
            fatigue: 0.0,
            steps_since_consolidation: 0,
            last_maintenance: None,
        }
    }
}

/// Per-namespace storage limits.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Quota {
    /// Maximum source capsule and event basis bytes.
    pub source_basis_bytes: u64,
    /// Maximum physical controlled bytes.
    pub controlled_bytes: u64,
    /// Capacity reserved for privacy and recovery operations.
    pub reserve_bytes: u64,
}

impl Default for Quota {
    fn default() -> Self {
        Self {
            source_basis_bytes: 64 * 1024 * 1024,
            controlled_bytes: 256 * 1024 * 1024,
            reserve_bytes: 16 * 1024 * 1024,
        }
    }
}

/// Storage usage measured from SQLite files and stored payload lengths.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct StorageUsage {
    /// Main SQLite database file bytes.
    pub db_bytes: u64,
    /// SQLite WAL file bytes.
    pub wal_bytes: u64,
    /// Sum of durable capsule text and blob bytes.
    pub capsule_bytes: u64,
    /// Sum of checkpoint state bytes.
    pub checkpoint_bytes: u64,
    /// Sum of durable event text and receipt bytes.
    pub event_bytes: u64,
}

impl StorageUsage {
    /// Source-basis bytes subject to the ingestion quota.
    #[must_use]
    pub fn source_basis_bytes(self) -> u64 {
        self.capsule_bytes.saturating_add(self.event_bytes)
    }

    /// Physical bytes represented by the main database and WAL.
    #[must_use]
    pub fn physical_bytes(self) -> u64 {
        self.db_bytes.saturating_add(self.wal_bytes)
    }
}

/// Errors returned by the provider-owned store.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum StoreError {
    /// The namespace is not a lowercase 64-character hexadecimal digest.
    InvalidNamespace(String),
    /// The namespace database does not exist.
    Missing,
    /// Explicit creation was requested for an existing namespace.
    AlreadyExists,
    /// Another connection owns the namespace writer lock.
    Busy,
    /// The namespace exists but cannot be trusted as a durable store.
    Corrupt(String),
    /// The state exists but differs from the expected compatibility identity.
    Incompatible {
        /// Name of the incompatible identity field.
        field: String,
    },
    /// A mutation would exceed a configured storage budget.
    BudgetExceeded,
    /// An idempotency key is already present in the event journal.
    IdempotencyConflict,
    /// The caller supplied malformed or dimensionally invalid data.
    InvalidInput(String),
    /// The requested record does not exist.
    UnknownRecord(RecordId),
    /// An operating-system filesystem operation failed.
    Io(String),
    /// SQLite returned an operational error.
    Sqlite(String),
}

impl fmt::Display for StoreError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidNamespace(reason) => write!(formatter, "invalid namespace: {reason}"),
            Self::Missing => formatter.write_str("namespace store is missing"),
            Self::AlreadyExists => formatter.write_str("namespace store already exists"),
            Self::Busy => formatter.write_str("namespace store is busy"),
            Self::Corrupt(reason) => write!(formatter, "corrupt namespace store: {reason}"),
            Self::Incompatible { field } => write!(formatter, "incompatible store field: {field}"),
            Self::BudgetExceeded => formatter.write_str("namespace storage budget exceeded"),
            Self::IdempotencyConflict => formatter.write_str("idempotency key already exists"),
            Self::InvalidInput(reason) => write!(formatter, "invalid store input: {reason}"),
            Self::UnknownRecord(record_id) => write!(formatter, "unknown record {}", record_id.0),
            Self::Io(reason) => write!(formatter, "store filesystem error: {reason}"),
            Self::Sqlite(reason) => write!(formatter, "store SQLite error: {reason}"),
        }
    }
}

impl std::error::Error for StoreError {}

/// An explicitly-owned SQLite namespace store.
pub struct NamespaceStore {
    conn: Connection,
    db_path: PathBuf,
    namespace: String,
    identity: StoreIdentity,
    quota: Quota,
}

impl fmt::Debug for NamespaceStore {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("NamespaceStore")
            .field("db_path", &self.db_path)
            .field("namespace", &self.namespace)
            .field("identity", &self.identity)
            .field("quota", &self.quota)
            .finish()
    }
}

impl NamespaceStore {
    /// Creates a new namespace store and fails if its namespace directory exists.
    pub fn create(
        root: &StateRoot,
        namespace: &str,
        identity: StoreIdentity,
    ) -> Result<Self, StoreError> {
        let (namespace_dir, db_path) = namespace_paths(root, namespace)?;
        if namespace_dir.exists() {
            return Err(StoreError::AlreadyExists);
        }
        let namespaces_dir = root.path().join("namespaces");
        fs::create_dir_all(&namespaces_dir).map_err(io_error)?;
        if let Err(error) = fs::create_dir(&namespace_dir) {
            return if error.kind() == std::io::ErrorKind::AlreadyExists {
                Err(StoreError::AlreadyExists)
            } else {
                Err(io_error(error))
            };
        }

        let result =
            Self::create_in_directory(namespace, namespace_dir.clone(), db_path.clone(), identity);
        if result.is_err() {
            remove_created_namespace(&namespace_dir, &db_path);
        }
        result
    }

    fn create_in_directory(
        namespace: &str,
        _namespace_dir: PathBuf,
        db_path: PathBuf,
        identity: StoreIdentity,
    ) -> Result<Self, StoreError> {
        validate_identity(&identity)?;
        let mut conn = Connection::open_with_flags(
            &db_path,
            OpenFlags::SQLITE_OPEN_READ_WRITE | OpenFlags::SQLITE_OPEN_CREATE,
        )
        .map_err(map_sqlite_error)?;
        configure_connection(&conn)?;
        create_schema(&conn)?;
        let tx = conn
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(map_sqlite_error)?;
        insert_initial_metadata(&tx, &identity)?;
        tx.commit().map_err(map_sqlite_error)?;
        Ok(Self {
            conn,
            db_path,
            namespace: namespace.to_owned(),
            identity,
            quota: Quota::default(),
        })
    }

    /// Opens an existing namespace after checking locking, integrity, schema,
    /// and every compatibility identity field.
    pub fn open(
        root: &StateRoot,
        namespace: &str,
        expected: &StoreIdentity,
    ) -> Result<Self, StoreError> {
        let (namespace_dir, db_path) = namespace_paths(root, namespace)?;
        if !namespace_dir.is_dir() || !db_path.is_file() {
            return Err(StoreError::Missing);
        }
        let conn = Connection::open_with_flags(&db_path, OpenFlags::SQLITE_OPEN_READ_WRITE)
            .map_err(map_open_sqlite_error)?;
        configure_connection(&conn).map_err(map_open_error)?;
        integrity_check(&conn).map_err(map_open_error)?;
        validate_schema(&conn).map_err(map_open_error)?;
        let stored_identity = read_identity(&conn).map_err(map_open_error)?;
        compare_identity(&stored_identity, expected)?;
        read_store_meta(&conn).map_err(map_open_error)?;
        required_u64(&conn, META_NEXT_RECORD_ID).map_err(map_open_error)?;
        validate_durable_rows(&conn).map_err(map_open_error)?;
        Ok(Self {
            conn,
            db_path,
            namespace: namespace.to_owned(),
            identity: stored_identity,
            quota: Quota::default(),
        })
    }

    /// Returns whether a namespace database exists without creating anything.
    #[must_use]
    pub fn exists(root: &StateRoot, namespace: &str) -> bool {
        namespace_paths(root, namespace)
            .map(|(namespace_dir, db_path)| namespace_dir.is_dir() && db_path.is_file())
            .unwrap_or(false)
    }

    /// Namespace digest used to address this store.
    #[must_use]
    pub fn namespace(&self) -> &str {
        &self.namespace
    }

    /// Compatibility identity read from this store.
    #[must_use]
    pub fn identity(&self) -> &StoreIdentity {
        &self.identity
    }

    /// Main SQLite file path, useful for diagnostics and accounting tests.
    #[must_use]
    pub fn path(&self) -> &Path {
        &self.db_path
    }

    /// Quota applied to ordinary and privacy writes.
    #[must_use]
    pub fn quota(&self) -> Quota {
        self.quota
    }

    /// Starts one immediate transaction for a mutation.
    pub fn begin_mutation(&mut self) -> Result<Mutation<'_>, StoreError> {
        let tx = self
            .conn
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(map_sqlite_error)?;
        let current = read_u64(&tx, META_COMMIT_SEQ)?;
        let pending_commit_seq = current
            .checked_add(1)
            .ok_or_else(|| StoreError::Corrupt("commit sequence overflow".to_owned()))?;
        Ok(Mutation {
            tx,
            db_path: self.db_path.clone(),
            quota: self.quota,
            pending_commit_seq,
        })
    }

    /// Returns the newest checkpoint, if one has been committed.
    pub fn latest_checkpoint(&self) -> Result<Option<Checkpoint>, StoreError> {
        self.conn
            .query_row(
                "SELECT seq, epoch, state FROM checkpoints ORDER BY seq DESC LIMIT 1",
                [],
                checkpoint_from_row,
            )
            .optional()
            .map_err(map_sqlite_error)
    }

    /// Returns events with a sequence strictly greater than `seq`.
    pub fn events_after(&self, seq: u64) -> Result<Vec<Event>, StoreError> {
        let seq = sqlite_i64(seq, "event sequence")?;
        let mut statement = self
            .conn
            .prepare(
                "SELECT seq, kind, idempotency_key, payload_sha256, receipt, created_tick
                 FROM events WHERE seq > ?1 ORDER BY seq ASC",
            )
            .map_err(map_sqlite_error)?;
        let rows = statement
            .query_map(params![seq], event_from_row)
            .map_err(map_sqlite_error)?;
        rows.collect::<Result<Vec<_>, _>>()
            .map_err(map_sqlite_error)
    }

    /// Returns one capsule by its stable record identity.
    pub fn capsule(&self, record_id: RecordId) -> Result<Option<StoredCapsule>, StoreError> {
        let record_id = sqlite_i64(record_id.0, "record id")?;
        self.conn
            .query_row(
                "SELECT record_id, source_id, key_text, value_text, affect, surprise,
                        intensity, key_embedding, value_embedding, ltm_key, provenance,
                        status, commit_seq
                 FROM capsules WHERE record_id = ?1",
                params![record_id],
                capsule_from_row,
            )
            .optional()
            .map_err(map_sqlite_error)
    }

    /// Returns capsules ordered by insertion commit sequence and record ID.
    pub fn capsules_in_commit_order(
        &self,
        include_revoked: bool,
    ) -> Result<Vec<StoredCapsule>, StoreError> {
        let sql = if include_revoked {
            "SELECT record_id, source_id, key_text, value_text, affect, surprise,
                    intensity, key_embedding, value_embedding, ltm_key, provenance,
                    status, commit_seq
             FROM capsules ORDER BY commit_seq ASC, record_id ASC"
        } else {
            "SELECT record_id, source_id, key_text, value_text, affect, surprise,
                    intensity, key_embedding, value_embedding, ltm_key, provenance,
                    status, commit_seq
             FROM capsules WHERE status <> 'revoked'
             ORDER BY commit_seq ASC, record_id ASC"
        };
        let mut statement = self.conn.prepare(sql).map_err(map_sqlite_error)?;
        let rows = statement
            .query_map([], capsule_from_row)
            .map_err(map_sqlite_error)?;
        rows.collect::<Result<Vec<_>, _>>()
            .map_err(map_sqlite_error)
    }

    /// Returns all source revocations in deterministic source order.
    pub fn revocations(&self) -> Result<Vec<Revocation>, StoreError> {
        let mut statement = self
            .conn
            .prepare("SELECT source_id, epoch, seq FROM revocations ORDER BY source_id ASC")
            .map_err(map_sqlite_error)?;
        let rows = statement
            .query_map([], revocation_from_row)
            .map_err(map_sqlite_error)?;
        rows.collect::<Result<Vec<_>, _>>()
            .map_err(map_sqlite_error)
    }

    /// Returns the fence reason, if the namespace is fenced for maintenance.
    pub fn fenced(&self) -> Result<Option<String>, StoreError> {
        self.conn
            .query_row("SELECT reason FROM fence WHERE id = 1", [], |row| {
                row.get::<_, Option<String>>(0)
            })
            .optional()
            .map(|reason| reason.flatten())
            .map_err(map_sqlite_error)
    }

    /// Returns persisted scheduler and generation metadata.
    pub fn meta(&self) -> Result<StoreMeta, StoreError> {
        read_store_meta(&self.conn)
    }

    /// Returns file and payload accounting for this namespace.
    pub fn usage(&self) -> Result<StorageUsage, StoreError> {
        query_usage(&self.conn, &self.db_path)
    }
}

/// A mutation transaction. Dropping it rolls the SQLite transaction back.
pub struct Mutation<'a> {
    tx: Transaction<'a>,
    db_path: PathBuf,
    quota: Quota,
    pending_commit_seq: CommitSeq,
}

impl<'a> Mutation<'a> {
    /// Looks up a committed idempotency event by key.
    pub fn lookup_idempotency(
        &self,
        key: &str,
    ) -> Result<Option<(String, String, u64)>, StoreError> {
        self.tx
            .query_row(
                "SELECT payload_sha256, receipt, seq FROM events
                 WHERE idempotency_key = ?1",
                params![key],
                |row| {
                    let payload_sha256 = row.get::<_, String>(0)?;
                    let receipt = row.get::<_, String>(1)?;
                    let seq = row.get::<_, i64>(2)?;
                    Ok((payload_sha256, receipt, seq))
                },
            )
            .optional()
            .map_err(map_sqlite_error)
            .and_then(|value| {
                value
                    .map(|(payload_sha256, receipt, seq)| {
                        sqlite_u64(seq, "idempotency event sequence")
                            .map(|seq| (payload_sha256, receipt, seq))
                    })
                    .transpose()
            })
    }

    /// Appends one journal event and returns its event sequence.
    pub fn append_event(
        &mut self,
        kind: &str,
        key: Option<&str>,
        payload_sha256: &str,
        receipt_json: &str,
        tick: u64,
    ) -> Result<u64, StoreError> {
        validate_json(receipt_json, "receipt")?;
        if let Some(key) = key
            && self.lookup_idempotency(key)?.is_some()
        {
            return Err(StoreError::IdempotencyConflict);
        }
        let basis_bytes = sum_lengths(&[
            kind.len(),
            key.map_or(0, str::len),
            payload_sha256.len(),
            receipt_json.len(),
        ])?;
        self.ensure_budget(basis_bytes, false)?;
        let seq = next_event_seq(&self.tx)?;
        let result = self.tx.execute(
            "INSERT INTO events(seq, kind, idempotency_key, payload_sha256, receipt, created_tick)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
            params![
                sqlite_i64(seq, "event sequence")?,
                kind,
                key,
                payload_sha256,
                receipt_json,
                sqlite_i64(tick, "logical tick")?
            ],
        );
        match result {
            Ok(_) => Ok(seq),
            Err(error) if is_unique_constraint(&error) => Err(StoreError::IdempotencyConflict),
            Err(error) => Err(map_sqlite_error(error)),
        }
    }

    /// Inserts a valid source capsule and allocates its never-reused record ID.
    pub fn insert_capsule(&mut self, capsule: Capsule) -> Result<RecordId, StoreError> {
        capsule.validate()?;
        let basis_bytes = capsule_byte_lengths(&capsule)?;
        self.ensure_budget(basis_bytes, false)?;
        let next_record_id = read_u64(&self.tx, META_NEXT_RECORD_ID)?;
        let record_id = RecordId(next_record_id);
        let next = next_record_id
            .checked_add(1)
            .ok_or_else(|| StoreError::Corrupt("record ID counter overflow".to_owned()))?;
        let affect = f32_array_to_bytes(&capsule.affect.0);
        let key_embedding = f32_slice_to_bytes(&capsule.key_embedding);
        let value_embedding = f32_slice_to_bytes(&capsule.value_embedding);
        let ltm_key = f32_slice_to_bytes(&capsule.ltm_key);
        self.tx
            .execute(
                "INSERT INTO capsules(
                    record_id, source_id, key_text, value_text, affect, surprise, intensity,
                    key_embedding, value_embedding, ltm_key, provenance, status, commit_seq
                 ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, 'valid', ?12)",
                params![
                    sqlite_i64(record_id.0, "record ID")?,
                    capsule.source_id.0,
                    capsule.key_text,
                    capsule.value_text,
                    affect,
                    capsule.surprise,
                    capsule.intensity,
                    key_embedding,
                    value_embedding,
                    ltm_key,
                    capsule.provenance,
                    sqlite_i64(self.pending_commit_seq, "commit sequence")?
                ],
            )
            .map_err(map_sqlite_error)?;
        let encoded_next = encode_u64(next);
        set_meta_value(&self.tx, META_NEXT_RECORD_ID, &encoded_next)?;
        Ok(record_id)
    }

    /// Changes a capsule status. Revocation irreversibly clears retained text,
    /// embeddings, and the canonical LTM key in the same transaction.
    pub fn mark_capsule_status(
        &mut self,
        record_id: RecordId,
        status: CapsuleStatus,
    ) -> Result<(), StoreError> {
        let sqlite_record_id = sqlite_i64(record_id.0, "record ID")?;
        let current = self
            .tx
            .query_row(
                "SELECT status FROM capsules WHERE record_id = ?1",
                params![sqlite_record_id],
                |row| row.get::<_, String>(0),
            )
            .optional()
            .map_err(map_sqlite_error)?;
        let current = current.ok_or(StoreError::UnknownRecord(record_id))?;
        let current = CapsuleStatus::parse(current)?;
        if current == CapsuleStatus::Revoked && status != CapsuleStatus::Revoked {
            return Err(StoreError::InvalidInput(
                "revoked capsules cannot be restored".to_owned(),
            ));
        }
        if status == CapsuleStatus::Revoked {
            self.tx
                .execute(
                    "UPDATE capsules SET key_text = '', value_text = '',
                            key_embedding = X'', value_embedding = X'', ltm_key = X'',
                            status = 'revoked' WHERE record_id = ?1",
                    params![sqlite_record_id],
                )
                .map_err(map_sqlite_error)?;
        } else {
            self.tx
                .execute(
                    "UPDATE capsules SET status = ?1 WHERE record_id = ?2",
                    params![status.as_str(), sqlite_record_id],
                )
                .map_err(map_sqlite_error)?;
        }
        Ok(())
    }

    /// Stores or replaces one serialized kernel checkpoint.
    pub fn put_checkpoint(
        &mut self,
        seq: CommitSeq,
        epoch: u64,
        bytes: &[u8],
    ) -> Result<(), StoreError> {
        let old_len = self
            .tx
            .query_row(
                "SELECT length(state) FROM checkpoints WHERE seq = ?1",
                params![sqlite_i64(seq, "checkpoint sequence")?],
                |row| row.get::<_, Option<i64>>(0),
            )
            .optional()
            .map_err(map_sqlite_error)?
            .flatten()
            .map(|value| sqlite_u64(value, "checkpoint byte length"))
            .transpose()?
            .unwrap_or(0);
        let new_len = u64::try_from(bytes.len())
            .map_err(|_| StoreError::InvalidInput("checkpoint is too large".to_owned()))?;
        let additional = new_len.saturating_sub(old_len);
        self.ensure_budget(additional, true)?;
        self.tx
            .execute(
                "INSERT OR REPLACE INTO checkpoints(seq, epoch, state)
                 VALUES (?1, ?2, ?3)",
                params![
                    sqlite_i64(seq, "checkpoint sequence")?,
                    sqlite_i64(epoch, "checkpoint epoch")?,
                    bytes
                ],
            )
            .map_err(map_sqlite_error)?;
        Ok(())
    }

    /// Prunes checkpoints whose sequence is strictly before `seq`.
    pub fn prune_checkpoints_before(&mut self, seq: CommitSeq) -> Result<(), StoreError> {
        self.tx
            .execute(
                "DELETE FROM checkpoints WHERE seq < ?1",
                params![sqlite_i64(seq, "checkpoint sequence")?],
            )
            .map_err(map_sqlite_error)?;
        Ok(())
    }

    /// Adds or updates a source revocation authority row.
    pub fn add_revocation(
        &mut self,
        source_id: &str,
        epoch: u64,
        seq: CommitSeq,
    ) -> Result<(), StoreError> {
        if source_id.is_empty() {
            return Err(StoreError::InvalidInput(
                "source identity cannot be empty".to_owned(),
            ));
        }
        let already_present = self
            .tx
            .query_row(
                "SELECT 1 FROM revocations WHERE source_id = ?1",
                params![source_id],
                |row| row.get::<_, i64>(0),
            )
            .optional()
            .map_err(map_sqlite_error)?
            .is_some();
        if !already_present {
            let additional = sum_lengths(&[source_id.len(), 8, 8])?;
            self.ensure_budget(additional, true)?;
        }
        self.tx
            .execute(
                "INSERT INTO revocations(source_id, epoch, seq) VALUES (?1, ?2, ?3)
                 ON CONFLICT(source_id) DO UPDATE SET epoch = excluded.epoch, seq = excluded.seq",
                params![
                    source_id,
                    sqlite_i64(epoch, "revocation epoch")?,
                    sqlite_i64(seq, "revocation sequence")?
                ],
            )
            .map_err(map_sqlite_error)?;
        Ok(())
    }

    /// Sets the privacy/rebuild fence reason.
    pub fn set_fence(&mut self, reason: &str) -> Result<(), StoreError> {
        let additional = u64::try_from(reason.len())
            .map_err(|_| StoreError::InvalidInput("fence reason is too large".to_owned()))?;
        self.ensure_budget(additional, true)?;
        self.tx
            .execute(
                "INSERT INTO fence(id, reason) VALUES (1, ?1)
                 ON CONFLICT(id) DO UPDATE SET reason = excluded.reason",
                params![reason],
            )
            .map_err(map_sqlite_error)?;
        Ok(())
    }

    /// Clears the privacy/rebuild fence.
    pub fn clear_fence(&mut self) -> Result<(), StoreError> {
        self.tx
            .execute("DELETE FROM fence WHERE id = 1", [])
            .map_err(map_sqlite_error)?;
        Ok(())
    }

    /// Replaces persisted scheduler and generation metadata.
    pub fn set_meta(&mut self, meta: &StoreMeta) -> Result<(), StoreError> {
        if !meta.fatigue.is_finite() {
            return Err(StoreError::InvalidInput(
                "fatigue must be finite".to_owned(),
            ));
        }
        let epoch = encode_u64(meta.epoch);
        let commit_seq = encode_u64(meta.commit_seq);
        let tick = encode_u64(meta.tick);
        let fatigue = encode_f32(meta.fatigue);
        let steps = encode_u64(meta.steps_since_consolidation);
        set_meta_value(&self.tx, META_EPOCH, &epoch)?;
        set_meta_value(&self.tx, META_COMMIT_SEQ, &commit_seq)?;
        set_meta_value(&self.tx, META_TICK, &tick)?;
        set_meta_value(&self.tx, META_FATIGUE, &fatigue)?;
        set_meta_value(&self.tx, META_STEPS_SINCE_CONSOLIDATION, &steps)?;
        set_meta_value(
            &self.tx,
            META_LAST_MAINTENANCE,
            meta.last_maintenance.as_deref().unwrap_or("").as_bytes(),
        )?;
        Ok(())
    }

    /// Reads scheduler and generation metadata inside this transaction.
    pub fn get_meta(&self) -> Result<StoreMeta, StoreError> {
        read_store_meta(&self.tx)
    }

    /// Sets one raw non-identity metadata value for forward-compatible callers.
    pub fn set_meta_value(&mut self, key: &str, value: &[u8]) -> Result<(), StoreError> {
        if is_identity_meta_key(key) {
            return Err(StoreError::InvalidInput(
                "identity metadata is immutable".to_owned(),
            ));
        }
        set_meta_value(&self.tx, key, value)
    }

    /// Reads one raw metadata value inside this transaction.
    pub fn get_meta_value(&self, key: &str) -> Result<Option<Vec<u8>>, StoreError> {
        get_meta_value(&self.tx, key)
    }

    /// Commits this mutation exactly once and returns its commit sequence.
    pub fn commit(self) -> Result<CommitSeq, StoreError> {
        let pending_commit_seq = self.pending_commit_seq;
        let encoded = encode_u64(pending_commit_seq);
        set_meta_value(&self.tx, META_COMMIT_SEQ, &encoded)?;
        self.tx.commit().map_err(map_sqlite_error)?;
        Ok(pending_commit_seq)
    }

    fn ensure_budget(&self, additional_basis: u64, allow_reserve: bool) -> Result<(), StoreError> {
        let usage = query_usage(&self.tx, &self.db_path)?;
        if !allow_reserve
            && usage
                .source_basis_bytes()
                .checked_add(additional_basis)
                .is_none_or(|total| total > self.quota.source_basis_bytes)
        {
            return Err(StoreError::BudgetExceeded);
        }
        let current_physical = physical_bytes(&self.db_path)?;
        let limit = if allow_reserve {
            self.quota.controlled_bytes
        } else {
            self.quota
                .controlled_bytes
                .saturating_sub(self.quota.reserve_bytes)
        };
        let additional_physical = additional_basis.saturating_add(NORMAL_WRITE_HEADROOM);
        if current_physical
            .checked_add(additional_physical)
            .is_none_or(|total| total > limit)
        {
            return Err(StoreError::BudgetExceeded);
        }
        Ok(())
    }
}

fn namespace_paths(root: &StateRoot, namespace: &str) -> Result<(PathBuf, PathBuf), StoreError> {
    if namespace.len() != 64
        || !namespace
            .bytes()
            .all(|byte| matches!(byte, b'0'..=b'9' | b'a'..=b'f'))
    {
        return Err(StoreError::InvalidNamespace(
            "namespace must be lowercase sha256 hex".to_owned(),
        ));
    }
    let namespace_dir = root
        .namespace_dir(namespace)
        .map_err(StoreError::InvalidNamespace)?;
    let db_path = namespace_dir.join("ncm.sqlite");
    Ok((namespace_dir, db_path))
}

fn remove_created_namespace(namespace_dir: &Path, db_path: &Path) {
    let _ = fs::remove_file(db_path);
    let _ = fs::remove_file(namespace_dir.join("ncm.sqlite-wal"));
    let _ = fs::remove_file(namespace_dir.join("ncm.sqlite-shm"));
    let _ = fs::remove_dir(namespace_dir);
}

fn validate_identity(identity: &StoreIdentity) -> Result<(), StoreError> {
    if identity.algorithm.profile.is_empty() {
        return Err(StoreError::InvalidInput(
            "algorithm profile cannot be empty".to_owned(),
        ));
    }
    validate_json(&identity.config_json, "config")?;
    if is_sha256_hex(&identity.projection_sha256)
        && sha256_hex(&identity.projection_bytes) != identity.projection_sha256
    {
        return Err(StoreError::Incompatible {
            field: "projection_sha256".to_owned(),
        });
    }
    Ok(())
}

fn configure_connection(conn: &Connection) -> Result<(), StoreError> {
    conn.busy_timeout(Duration::from_millis(0))
        .map_err(map_sqlite_error)?;
    let locking_mode: String = conn
        .query_row("PRAGMA locking_mode = EXCLUSIVE", [], |row| row.get(0))
        .map_err(map_sqlite_error)?;
    if !locking_mode.eq_ignore_ascii_case("exclusive") {
        return Err(StoreError::Corrupt(format!(
            "locking mode is {locking_mode:?}, not exclusive"
        )));
    }
    let journal_mode: String = conn
        .query_row("PRAGMA journal_mode = WAL", [], |row| row.get(0))
        .map_err(map_sqlite_error)?;
    if !journal_mode.eq_ignore_ascii_case("wal") {
        return Err(StoreError::Corrupt(format!(
            "journal mode is {journal_mode:?}, not WAL"
        )));
    }
    conn.execute_batch("PRAGMA synchronous = FULL; PRAGMA wal_autocheckpoint = 1000;")
        .map_err(map_sqlite_error)?;
    let journal_limit_sql = format!("PRAGMA journal_size_limit = {JOURNAL_SIZE_LIMIT}");
    let _journal_limit: i64 = conn
        .query_row(&journal_limit_sql, [], |row| row.get(0))
        .map_err(map_sqlite_error)?;
    conn.execute_batch("BEGIN IMMEDIATE; ROLLBACK;")
        .map_err(map_sqlite_error)?;
    let probe: i64 = conn
        .query_row("SELECT 1", [], |row| row.get(0))
        .map_err(map_sqlite_error)?;
    if probe != 1 {
        return Err(StoreError::Corrupt(
            "SQLite probe returned an invalid value".to_owned(),
        ));
    }
    Ok(())
}

fn create_schema(conn: &Connection) -> Result<(), StoreError> {
    conn.execute_batch(
        "CREATE TABLE meta(
            key TEXT PRIMARY KEY,
            value BLOB
        );
        CREATE TABLE capsules(
            record_id INTEGER PRIMARY KEY,
            source_id TEXT,
            key_text TEXT,
            value_text TEXT,
            affect BLOB(16),
            surprise REAL,
            intensity REAL,
            key_embedding BLOB(1536),
            value_embedding BLOB(1536),
            ltm_key BLOB(256),
            provenance TEXT,
            status TEXT CHECK(status IN ('valid', 'superseded', 'revoked')),
            commit_seq INTEGER
        );
        CREATE TABLE events(
            seq INTEGER PRIMARY KEY,
            kind TEXT,
            idempotency_key TEXT UNIQUE,
            payload_sha256 TEXT,
            receipt TEXT,
            created_tick INTEGER
        );
        CREATE TABLE checkpoints(
            seq INTEGER PRIMARY KEY,
            epoch INTEGER,
            state BLOB
        );
        CREATE TABLE revocations(
            source_id TEXT PRIMARY KEY,
            epoch INTEGER,
            seq INTEGER
        );
        CREATE TABLE fence(
            id INTEGER PRIMARY KEY CHECK(id = 1),
            reason TEXT
        );",
    )
    .map_err(map_sqlite_error)
}

fn insert_initial_metadata(
    tx: &Transaction<'_>,
    identity: &StoreIdentity,
) -> Result<(), StoreError> {
    let entries: [(&str, Vec<u8>); 17] = [
        (META_SCHEMA_VERSION, SCHEMA_VERSION.as_bytes().to_vec()),
        (
            META_ALGORITHM_PROFILE,
            identity.algorithm.profile.as_bytes().to_vec(),
        ),
        (
            META_ALGORITHM_CONFIG_SHA256,
            identity.algorithm.config_sha256.as_bytes().to_vec(),
        ),
        (
            META_PROJECTION_SHA256,
            identity.projection_sha256.as_bytes().to_vec(),
        ),
        (META_PROJECTION, identity.projection_bytes.clone()),
        (
            META_ENCODER_MODEL,
            identity.encoder_model.as_bytes().to_vec(),
        ),
        (
            META_ENCODER_ARTIFACT_SHA256,
            identity.encoder_artifact_sha256.as_bytes().to_vec(),
        ),
        (META_SEED, encode_u64(identity.seed)),
        (META_CONFIG_JSON, identity.config_json.as_bytes().to_vec()),
        (META_EPOCH, encode_u64(1)),
        (META_COMMIT_SEQ, encode_u64(0)),
        (META_TICK, encode_u64(0)),
        (META_FATIGUE, encode_f32(0.0)),
        (META_STEPS_SINCE_CONSOLIDATION, encode_u64(0)),
        (META_LAST_MAINTENANCE, Vec::new()),
        (META_NEXT_RECORD_ID, encode_u64(1)),
        ("format", SCHEMA_VERSION.as_bytes().to_vec()),
    ];
    for (key, value) in entries {
        tx.execute(
            "INSERT INTO meta(key, value) VALUES (?1, ?2)",
            params![key, value],
        )
        .map_err(map_sqlite_error)?;
    }
    Ok(())
}

fn integrity_check(conn: &Connection) -> Result<(), StoreError> {
    let mut statement = conn
        .prepare("PRAGMA integrity_check")
        .map_err(map_sqlite_error)?;
    let rows = statement
        .query_map([], |row| row.get::<_, String>(0))
        .map_err(map_sqlite_error)?;
    for result in rows {
        let value = result.map_err(map_sqlite_error)?;
        if value != "ok" {
            return Err(StoreError::Corrupt(value));
        }
    }
    Ok(())
}

fn validate_durable_rows(conn: &Connection) -> Result<(), StoreError> {
    let mut events = conn
        .prepare(
            "SELECT seq, kind, idempotency_key, payload_sha256, receipt, created_tick
             FROM events ORDER BY seq ASC",
        )
        .map_err(map_sqlite_error)?;
    let event_rows = events
        .query_map([], event_from_row)
        .map_err(map_sqlite_error)?;
    for row in event_rows {
        row.map_err(map_sqlite_error)?;
    }

    let mut capsules = conn
        .prepare(
            "SELECT record_id, source_id, key_text, value_text, affect, surprise,
                    intensity, key_embedding, value_embedding, ltm_key, provenance,
                    status, commit_seq
             FROM capsules ORDER BY record_id ASC",
        )
        .map_err(map_sqlite_error)?;
    let capsule_rows = capsules
        .query_map([], capsule_from_row)
        .map_err(map_sqlite_error)?;
    for row in capsule_rows {
        row.map_err(map_sqlite_error)?;
    }

    let mut checkpoints = conn
        .prepare("SELECT seq, epoch, state FROM checkpoints ORDER BY seq ASC")
        .map_err(map_sqlite_error)?;
    let checkpoint_rows = checkpoints
        .query_map([], checkpoint_from_row)
        .map_err(map_sqlite_error)?;
    for row in checkpoint_rows {
        row.map_err(map_sqlite_error)?;
    }

    let mut revocations = conn
        .prepare("SELECT source_id, epoch, seq FROM revocations ORDER BY source_id ASC")
        .map_err(map_sqlite_error)?;
    let revocation_rows = revocations
        .query_map([], revocation_from_row)
        .map_err(map_sqlite_error)?;
    for row in revocation_rows {
        row.map_err(map_sqlite_error)?;
    }
    Ok(())
}

fn validate_schema(conn: &Connection) -> Result<(), StoreError> {
    let mut statement = conn
        .prepare(
            "SELECT name FROM sqlite_master
             WHERE type = 'table' AND name NOT LIKE 'sqlite_%' ORDER BY name",
        )
        .map_err(map_sqlite_error)?;
    let rows = statement
        .query_map([], |row| row.get::<_, String>(0))
        .map_err(map_sqlite_error)?;
    let actual = rows
        .collect::<Result<Vec<_>, _>>()
        .map_err(map_sqlite_error)?;
    let mut expected = TABLE_NAMES.map(str::to_owned).to_vec();
    expected.sort();
    if actual != expected {
        return Err(StoreError::Corrupt(format!(
            "schema tables differ: expected {expected:?}, found {actual:?}"
        )));
    }

    let columns: [(&str, &[&str]); 6] = [
        ("meta", &["key", "value"]),
        (
            "capsules",
            &[
                "record_id",
                "source_id",
                "key_text",
                "value_text",
                "affect",
                "surprise",
                "intensity",
                "key_embedding",
                "value_embedding",
                "ltm_key",
                "provenance",
                "status",
                "commit_seq",
            ],
        ),
        (
            "events",
            &[
                "seq",
                "kind",
                "idempotency_key",
                "payload_sha256",
                "receipt",
                "created_tick",
            ],
        ),
        ("checkpoints", &["seq", "epoch", "state"]),
        ("revocations", &["source_id", "epoch", "seq"]),
        ("fence", &["id", "reason"]),
    ];
    for (table, expected_columns) in columns {
        let sql = match table {
            "meta" => "PRAGMA table_info(meta)",
            "capsules" => "PRAGMA table_info(capsules)",
            "events" => "PRAGMA table_info(events)",
            "checkpoints" => "PRAGMA table_info(checkpoints)",
            "revocations" => "PRAGMA table_info(revocations)",
            "fence" => "PRAGMA table_info(fence)",
            _ => return Err(StoreError::Corrupt("unknown schema table".to_owned())),
        };
        let mut statement = conn.prepare(sql).map_err(map_sqlite_error)?;
        let rows = statement
            .query_map([], |row| row.get::<_, String>(1))
            .map_err(map_sqlite_error)?;
        let actual_columns = rows
            .collect::<Result<Vec<_>, _>>()
            .map_err(map_sqlite_error)?;
        let expected_columns = expected_columns
            .iter()
            .map(|value| (*value).to_owned())
            .collect::<Vec<_>>();
        if actual_columns != expected_columns {
            return Err(StoreError::Corrupt(format!(
                "columns for {table} differ: expected {expected_columns:?}, found {actual_columns:?}"
            )));
        }
    }
    Ok(())
}

fn read_identity(conn: &Connection) -> Result<StoreIdentity, StoreError> {
    let schema_version = required_text(conn, META_SCHEMA_VERSION)?;
    if schema_version != SCHEMA_VERSION {
        return Err(StoreError::Corrupt(format!(
            "unsupported schema version {schema_version:?}"
        )));
    }
    let identity = StoreIdentity {
        algorithm: AlgorithmIdentity {
            profile: required_text(conn, META_ALGORITHM_PROFILE)?,
            config_sha256: required_text(conn, META_ALGORITHM_CONFIG_SHA256)?,
        },
        projection_sha256: required_text(conn, META_PROJECTION_SHA256)?,
        encoder_model: required_text(conn, META_ENCODER_MODEL)?,
        encoder_artifact_sha256: required_text(conn, META_ENCODER_ARTIFACT_SHA256)?,
        projection_bytes: required_blob(conn, META_PROJECTION)?,
        seed: required_u64(conn, META_SEED)?,
        config_json: required_text(conn, META_CONFIG_JSON)?,
    };
    validate_json(&identity.config_json, "config")?;
    let projection_digest = sha256_hex(&identity.projection_bytes);
    if is_sha256_hex(&identity.projection_sha256) && projection_digest != identity.projection_sha256
    {
        return Err(StoreError::Corrupt(
            "projection digest does not match persisted projection".to_owned(),
        ));
    }
    Ok(identity)
}

fn compare_identity(stored: &StoreIdentity, expected: &StoreIdentity) -> Result<(), StoreError> {
    if stored.algorithm != expected.algorithm {
        return Err(StoreError::Incompatible {
            field: "algorithm".to_owned(),
        });
    }
    if stored.projection_sha256 != expected.projection_sha256 {
        return Err(StoreError::Incompatible {
            field: "projection_sha256".to_owned(),
        });
    }
    if stored.projection_bytes != expected.projection_bytes {
        return Err(StoreError::Incompatible {
            field: "projection_bytes".to_owned(),
        });
    }
    if stored.encoder_model != expected.encoder_model {
        return Err(StoreError::Incompatible {
            field: "encoder_model".to_owned(),
        });
    }
    if stored.encoder_artifact_sha256 != expected.encoder_artifact_sha256 {
        return Err(StoreError::Incompatible {
            field: "encoder_artifact_sha256".to_owned(),
        });
    }
    if stored.seed != expected.seed {
        return Err(StoreError::Incompatible {
            field: "seed".to_owned(),
        });
    }
    if stored.config_json != expected.config_json {
        return Err(StoreError::Incompatible {
            field: "config_json".to_owned(),
        });
    }
    Ok(())
}

fn read_store_meta(conn: &Connection) -> Result<StoreMeta, StoreError> {
    let last_maintenance = required_text(conn, META_LAST_MAINTENANCE)?;
    Ok(StoreMeta {
        epoch: required_u64(conn, META_EPOCH)?,
        commit_seq: required_u64(conn, META_COMMIT_SEQ)?,
        tick: required_u64(conn, META_TICK)?,
        fatigue: required_f32(conn, META_FATIGUE)?,
        steps_since_consolidation: required_u64(conn, META_STEPS_SINCE_CONSOLIDATION)?,
        last_maintenance: if last_maintenance.is_empty() {
            None
        } else {
            Some(last_maintenance)
        },
    })
}

fn get_meta_value(conn: &Connection, key: &str) -> Result<Option<Vec<u8>>, StoreError> {
    conn.query_row(
        "SELECT value FROM meta WHERE key = ?1",
        params![key],
        |row| row.get::<_, Option<Vec<u8>>>(0),
    )
    .optional()
    .map(|value| value.flatten())
    .map_err(map_sqlite_error)
}

fn set_meta_value(conn: &Connection, key: &str, value: &[u8]) -> Result<(), StoreError> {
    let changed = conn
        .execute(
            "UPDATE meta SET value = ?1 WHERE key = ?2",
            params![value, key],
        )
        .map_err(map_sqlite_error)?;
    if changed != 1 {
        return Err(StoreError::Corrupt(format!(
            "required metadata key {key:?} is missing"
        )));
    }
    Ok(())
}

fn required_blob(conn: &Connection, key: &str) -> Result<Vec<u8>, StoreError> {
    get_meta_value(conn, key)?
        .ok_or_else(|| StoreError::Corrupt(format!("required metadata key {key:?} is missing")))
}

fn required_text(conn: &Connection, key: &str) -> Result<String, StoreError> {
    let value = required_blob(conn, key)?;
    String::from_utf8(value)
        .map_err(|_| StoreError::Corrupt(format!("metadata key {key:?} is not UTF-8")))
}

fn required_u64(conn: &Connection, key: &str) -> Result<u64, StoreError> {
    let value = required_text(conn, key)?;
    value
        .parse::<u64>()
        .map_err(|_| StoreError::Corrupt(format!("metadata key {key:?} is not a u64")))
}

fn required_f32(conn: &Connection, key: &str) -> Result<f32, StoreError> {
    let value = required_text(conn, key)?;
    let value = value
        .parse::<f32>()
        .map_err(|_| StoreError::Corrupt(format!("metadata key {key:?} is not an f32")))?;
    if !value.is_finite() {
        return Err(StoreError::Corrupt(format!(
            "metadata key {key:?} is non-finite"
        )));
    }
    Ok(value)
}

fn read_u64(conn: &Connection, key: &str) -> Result<u64, StoreError> {
    required_u64(conn, key)
}

fn query_usage(conn: &Connection, db_path: &Path) -> Result<StorageUsage, StoreError> {
    let capsule_bytes = conn
        .query_row(
            "SELECT COALESCE(SUM(
                COALESCE(length(source_id), 0) + COALESCE(length(key_text), 0) +
                COALESCE(length(value_text), 0) + COALESCE(length(affect), 0) +
                COALESCE(length(key_embedding), 0) + COALESCE(length(value_embedding), 0) +
                COALESCE(length(ltm_key), 0) + COALESCE(length(provenance), 0) +
                COALESCE(length(status), 0)
             ), 0) FROM capsules",
            [],
            |row| row.get::<_, i64>(0),
        )
        .map_err(map_sqlite_error)
        .and_then(|value| sqlite_u64(value, "capsule usage"))?;
    let checkpoint_bytes = conn
        .query_row(
            "SELECT COALESCE(SUM(COALESCE(length(state), 0)), 0) FROM checkpoints",
            [],
            |row| row.get::<_, i64>(0),
        )
        .map_err(map_sqlite_error)
        .and_then(|value| sqlite_u64(value, "checkpoint usage"))?;
    let event_bytes = conn
        .query_row(
            "SELECT COALESCE(SUM(
                COALESCE(length(kind), 0) + COALESCE(length(idempotency_key), 0) +
                COALESCE(length(payload_sha256), 0) + COALESCE(length(receipt), 0)
             ), 0) FROM events",
            [],
            |row| row.get::<_, i64>(0),
        )
        .map_err(map_sqlite_error)
        .and_then(|value| sqlite_u64(value, "event usage"))?;
    Ok(StorageUsage {
        db_bytes: file_len(db_path)?,
        wal_bytes: file_len(&wal_path(db_path))?,
        capsule_bytes,
        checkpoint_bytes,
        event_bytes,
    })
}

fn physical_bytes(db_path: &Path) -> Result<u64, StoreError> {
    let db = file_len(db_path)?;
    let wal = file_len(&wal_path(db_path))?;
    let shm = file_len(&shm_path(db_path))?;
    db.checked_add(wal)
        .and_then(|value| value.checked_add(shm))
        .ok_or_else(|| StoreError::Io("physical file size overflow".to_owned()))
}

fn file_len(path: &Path) -> Result<u64, StoreError> {
    match fs::metadata(path) {
        Ok(metadata) => Ok(metadata.len()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(0),
        Err(error) => Err(io_error(error)),
    }
}

fn wal_path(db_path: &Path) -> PathBuf {
    db_path.with_file_name("ncm.sqlite-wal")
}

fn shm_path(db_path: &Path) -> PathBuf {
    db_path.with_file_name("ncm.sqlite-shm")
}

fn capsule_byte_lengths(capsule: &Capsule) -> Result<u64, StoreError> {
    sum_lengths(&[
        capsule.source_id.0.len(),
        capsule.key_text.len(),
        capsule.value_text.len(),
        AFFECT_DIM * std::mem::size_of::<f32>(),
        capsule.key_embedding.len() * std::mem::size_of::<f32>(),
        capsule.value_embedding.len() * std::mem::size_of::<f32>(),
        capsule.ltm_key.len() * std::mem::size_of::<f32>(),
        capsule.provenance.len(),
        CapsuleStatus::Valid.as_str().len(),
    ])
}

fn sum_lengths(lengths: &[usize]) -> Result<u64, StoreError> {
    lengths.iter().try_fold(0_u64, |total, length| {
        let length = u64::try_from(*length)
            .map_err(|_| StoreError::InvalidInput("payload length overflow".to_owned()))?;
        total
            .checked_add(length)
            .ok_or_else(|| StoreError::InvalidInput("payload length overflow".to_owned()))
    })
}

fn next_event_seq(conn: &Connection) -> Result<u64, StoreError> {
    let value = conn
        .query_row("SELECT COALESCE(MAX(seq), 0) + 1 FROM events", [], |row| {
            row.get::<_, i64>(0)
        })
        .map_err(map_sqlite_error)?;
    sqlite_u64(value, "event sequence")
}

fn checkpoint_from_row(row: &Row<'_>) -> rusqlite::Result<Checkpoint> {
    let seq = row.get::<_, i64>(0)?;
    let epoch = row.get::<_, i64>(1)?;
    Ok(Checkpoint {
        seq: sqlite_u64_row(seq, "checkpoint sequence")?,
        epoch: sqlite_u64_row(epoch, "checkpoint epoch")?,
        state: row.get(2)?,
    })
}

fn event_from_row(row: &Row<'_>) -> rusqlite::Result<Event> {
    let seq = row.get::<_, i64>(0)?;
    let created_tick = row.get::<_, i64>(5)?;
    let receipt = row.get::<_, String>(4)?;
    if serde_json::from_str::<serde_json::Value>(&receipt).is_err() {
        return Err(rusqlite::Error::InvalidQuery);
    }
    Ok(Event {
        seq: sqlite_u64_row(seq, "event sequence")?,
        kind: row.get(1)?,
        idempotency_key: row.get(2)?,
        payload_sha256: row.get(3)?,
        receipt,
        created_tick: sqlite_u64_row(created_tick, "event tick")?,
    })
}

fn capsule_from_row(row: &Row<'_>) -> rusqlite::Result<StoredCapsule> {
    let record_id = row.get::<_, i64>(0)?;
    let source_id = row.get::<_, String>(1)?;
    let key_text = row.get::<_, String>(2)?;
    let value_text = row.get::<_, String>(3)?;
    let affect = decode_affect(row.get::<_, Vec<u8>>(4)?)?;
    let surprise = row.get::<_, f32>(5)?;
    let intensity = row.get::<_, f32>(6)?;
    let key_embedding = row.get::<_, Vec<u8>>(7)?;
    let value_embedding = row.get::<_, Vec<u8>>(8)?;
    let ltm_key = row.get::<_, Vec<u8>>(9)?;
    let provenance = row.get::<_, String>(10)?;
    let status = CapsuleStatus::parse(row.get::<_, String>(11)?)
        .map_err(|_| rusqlite::Error::InvalidQuery)?;
    let commit_seq = row.get::<_, i64>(12)?;
    let (key_embedding, value_embedding, ltm_key) = if status == CapsuleStatus::Revoked {
        if !key_text.is_empty()
            || !value_text.is_empty()
            || !key_embedding.is_empty()
            || !value_embedding.is_empty()
            || !ltm_key.is_empty()
        {
            return Err(rusqlite::Error::InvalidQuery);
        }
        (Vec::new(), Vec::new(), Vec::new())
    } else {
        (
            decode_f32_blob(key_embedding, EMBEDDING_DIM)?,
            decode_f32_blob(value_embedding, EMBEDDING_DIM)?,
            decode_f32_blob(ltm_key, LTM_KEY_DIM)?,
        )
    };
    if !surprise.is_finite() || !intensity.is_finite() {
        return Err(rusqlite::Error::InvalidQuery);
    }
    if serde_json::from_str::<serde_json::Value>(&provenance).is_err() {
        return Err(rusqlite::Error::InvalidQuery);
    }
    Ok(StoredCapsule {
        record_id: RecordId(sqlite_u64_row(record_id, "record ID")?),
        source_id: SourceId(source_id),
        key_text,
        value_text,
        affect,
        surprise,
        intensity,
        key_embedding,
        value_embedding,
        ltm_key,
        provenance,
        status,
        commit_seq: sqlite_u64_row(commit_seq, "capsule commit sequence")?,
    })
}

fn revocation_from_row(row: &Row<'_>) -> rusqlite::Result<Revocation> {
    Ok(Revocation {
        source_id: SourceId(row.get(0)?),
        epoch: sqlite_u64_row(row.get::<_, i64>(1)?, "revocation epoch")?,
        seq: sqlite_u64_row(row.get::<_, i64>(2)?, "revocation sequence")?,
    })
}

fn decode_affect(bytes: Vec<u8>) -> rusqlite::Result<AffectVector> {
    let values = decode_f32_values(bytes, AFFECT_DIM)?;
    let values: [f32; AFFECT_DIM] = values
        .try_into()
        .map_err(|_| rusqlite::Error::InvalidQuery)?;
    Ok(AffectVector(values))
}

fn decode_f32_blob(bytes: Vec<u8>, expected: usize) -> rusqlite::Result<Vec<f32>> {
    decode_f32_values(bytes, expected)
}

fn decode_f32_values(bytes: Vec<u8>, expected: usize) -> rusqlite::Result<Vec<f32>> {
    let expected_bytes = expected
        .checked_mul(std::mem::size_of::<f32>())
        .ok_or(rusqlite::Error::InvalidQuery)?;
    if bytes.len() != expected_bytes {
        return Err(rusqlite::Error::InvalidQuery);
    }
    let mut values = Vec::with_capacity(expected);
    for chunk in bytes.chunks_exact(std::mem::size_of::<f32>()) {
        let value = f32::from_le_bytes(
            chunk
                .try_into()
                .map_err(|_| rusqlite::Error::InvalidQuery)?,
        );
        if !value.is_finite() {
            return Err(rusqlite::Error::InvalidQuery);
        }
        values.push(value);
    }
    Ok(values)
}

fn f32_array_to_bytes<const N: usize>(values: &[f32; N]) -> Vec<u8> {
    f32_slice_to_bytes(values)
}

fn f32_slice_to_bytes(values: &[f32]) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(std::mem::size_of_val(values));
    for value in values {
        bytes.extend_from_slice(&value.to_le_bytes());
    }
    bytes
}

fn encode_u64(value: u64) -> Vec<u8> {
    value.to_string().into_bytes()
}

fn encode_f32(value: f32) -> Vec<u8> {
    value.to_string().into_bytes()
}

fn sqlite_i64(value: u64, what: &str) -> Result<i64, StoreError> {
    i64::try_from(value)
        .map_err(|_| StoreError::InvalidInput(format!("{what} does not fit SQLite INTEGER")))
}

fn sqlite_u64(value: i64, what: &str) -> Result<u64, StoreError> {
    u64::try_from(value).map_err(|_| StoreError::Corrupt(format!("{what} is negative")))
}

fn sqlite_u64_row(value: i64, _what: &str) -> rusqlite::Result<u64> {
    u64::try_from(value).map_err(|_| rusqlite::Error::InvalidQuery)
}

fn validate_json(value: &str, what: &str) -> Result<(), StoreError> {
    serde_json::from_str::<serde_json::Value>(value)
        .map_err(|error| StoreError::InvalidInput(format!("{what} is not valid JSON: {error}")))?;
    Ok(())
}

fn is_identity_meta_key(key: &str) -> bool {
    matches!(
        key,
        META_SCHEMA_VERSION
            | META_ALGORITHM_PROFILE
            | META_ALGORITHM_CONFIG_SHA256
            | META_PROJECTION_SHA256
            | META_PROJECTION
            | META_ENCODER_MODEL
            | META_ENCODER_ARTIFACT_SHA256
            | META_SEED
            | META_CONFIG_JSON
            | "format"
    )
}

fn is_sha256_hex(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| matches!(byte, b'0'..=b'9' | b'a'..=b'f'))
}

fn sha256_hex(bytes: &[u8]) -> String {
    let digest = Sha256::digest(bytes);
    let mut output = String::with_capacity(digest.len() * 2);
    for byte in digest {
        use std::fmt::Write as _;
        let _ = write!(output, "{byte:02x}");
    }
    output
}

fn map_open_error(error: StoreError) -> StoreError {
    match error {
        StoreError::Busy | StoreError::Corrupt(_) => error,
        other => StoreError::Corrupt(other.to_string()),
    }
}

fn map_open_sqlite_error(error: rusqlite::Error) -> StoreError {
    match error.sqlite_error_code() {
        Some(rusqlite::ErrorCode::DatabaseBusy | rusqlite::ErrorCode::DatabaseLocked) => {
            StoreError::Busy
        }
        Some(rusqlite::ErrorCode::DatabaseCorrupt | rusqlite::ErrorCode::NotADatabase) => {
            StoreError::Corrupt(error.to_string())
        }
        _ => StoreError::Corrupt(error.to_string()),
    }
}

fn map_sqlite_error(error: rusqlite::Error) -> StoreError {
    match error.sqlite_error_code() {
        Some(rusqlite::ErrorCode::DatabaseBusy | rusqlite::ErrorCode::DatabaseLocked) => {
            StoreError::Busy
        }
        Some(rusqlite::ErrorCode::DatabaseCorrupt | rusqlite::ErrorCode::NotADatabase) => {
            StoreError::Corrupt(error.to_string())
        }
        _ => StoreError::Sqlite(error.to_string()),
    }
}

fn is_unique_constraint(error: &rusqlite::Error) -> bool {
    if error.sqlite_error_code() != Some(rusqlite::ErrorCode::ConstraintViolation) {
        return false;
    }
    let text = error.to_string().to_ascii_lowercase();
    text.contains("unique") || text.contains("events.idempotency_key")
}

fn io_error(error: std::io::Error) -> StoreError {
    StoreError::Io(error.to_string())
}
