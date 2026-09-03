//! Physical schema for the observation journal store.
//!
//! Seven tables with seven different jobs:
//!
//! * `tdmem_observation_journal_v1` — immutable admitted content.
//! * `tdmem_observation_delivery_v1` — the only mutable delivery authority.
//! * `tdmem_observation_receipt_v1` — append-only attempt audit.
//! * `tdmem_observation_attempt_refusal_v1` — append-only audit of answered
//!   attempts whose provider terminal the host refused as delivery evidence.
//! * `tdmem_observation_replay_cursor_v1` — the ingress replay position.
//! * `tdmem_observation_target_cursor_v1` — per-registration admitted position.
//! * `tdmem_observation_withheld_v2` — digests-only audit for refused events.
//!
//! Two addressing rules are load-bearing and are enforced by the keys here:
//!
//! * **Delivery is addressed by provider registration, never by instance.** The
//!   idempotency key is derived over `(provider_id, registration_revision)`, so
//!   anything keyed on `provider_instance_id` would strand queued work the
//!   moment a provider restarts and re-handshakes under a new instance id. The
//!   instance is recorded as per-attempt evidence
//!   (`last_provider_instance_id`, and the receipt's own column) and nothing
//!   addresses rows by it.
//! * **Source-sequence position is per registration.** The ingress cursor
//!   tracks replay for the stream as a whole; `tdmem_observation_target_cursor_v1`
//!   tracks how far each registration has come, so one settled event fans out to
//!   a lagging target after another target has already moved on.
//!
//! Nothing here is queryable by content: no FTS table, no payload index, no
//! kind-keyed content lookup. That absence is the structural guarantee that the
//! outbox never becomes a second authority for Native facts.

use rusqlite::{Connection, OptionalExtension, Transaction, TransactionBehavior};

use crate::error::ObservationJournalError;

use super::row::{WithheldAuditCursorV1, validate_withheld_page};

/// Schema version this build writes and understands.
pub const SCHEMA_VERSION: i64 = 6;

const LEGACY_SCHEMA_VERSION: i64 = 1;

/// The version that predates the refused-terminal audit table. Upgrading from
/// it adds a table and nothing else, so the DDL below is the whole migration.
const WITHHELD_V2_SCHEMA_VERSION: i64 = 2;

/// The version that predates the restart-recovery record. Upgrading from it
/// adds one table and nothing else: an existing store simply has no
/// acknowledged watermark yet, which is exactly the "first contact" shape the
/// recovery runtime already handles, so no acknowledged history is invented.
const PRE_RECOVERY_SCHEMA_VERSION: i64 = 3;

/// The version whose recovery record counted repair attempts without an
/// assessment identity to count them *per*. Upgrading from it adds the identity
/// column and the accepted replay-position policy; both start `NULL`, which is
/// the same "nothing accepted yet" shape a fresh target has.
const PRE_ASSESSMENT_IDENTITY_SCHEMA_VERSION: i64 = 4;

/// The version that predates the orphaned-attempt audit table. Upgrading from
/// it adds one table and nothing else: attempts spent by leases that lapsed
/// before this build has no orphan record, which is honest — the store never
/// held that evidence and nothing invents it retroactively.
const PRE_ATTEMPT_ORPHAN_SCHEMA_VERSION: i64 = 5;

/// Columns added to the recovery record after version 4.
const RECOVERY_V5_COLUMNS: [(&str, &str); 2] = [
    ("last_assessment_id", "TEXT"),
    (
        "replay_position_retained",
        "INTEGER CHECK (replay_position_retained IN (0, 1))",
    ),
];

/// `synchronous = FULL` is what makes "survives restart" mean "survives power
/// loss". `secure_delete = ON` is what makes privacy deletion zero freed pages
/// rather than merely unlink them.
const CONNECTION_PRAGMAS: &str = "\
PRAGMA synchronous = FULL;
PRAGMA foreign_keys = ON;
PRAGMA secure_delete = ON;
PRAGMA temp_store = MEMORY;
";

const SCHEMA_DDL: &str = r#"
CREATE TABLE IF NOT EXISTS tdmem_observation_journal_v1 (
    idempotency_key             TEXT    NOT NULL,
    observation_id              TEXT    NOT NULL,
    exact_scope_sha256          TEXT    NOT NULL,
    provider_id                 TEXT    NOT NULL,
    provider_instance_id        TEXT    NOT NULL,
    registration_revision       INTEGER NOT NULL CHECK (registration_revision > 0),
    ready_receipt_digest        TEXT    NOT NULL,
    source_authority            TEXT    NOT NULL CHECK (source_authority IN (
        'host_session', 'tool_execution', 'source_edit', 'test_execution',
        'diagnostic_broker', 'git_evidence', 'native_fact_promotion',
        'feedback_outcome', 'automation_outcome')),
    source_stream               TEXT    NOT NULL,
    source_event_id             TEXT    NOT NULL,
    source_event_revision       INTEGER NOT NULL CHECK (source_event_revision >= 0),
    source_event_sha256         TEXT    NOT NULL,
    source_sequence             INTEGER NOT NULL CHECK (source_sequence >= 0),
    settlement_receipt_json     TEXT    NOT NULL,
    exact_scope_json            TEXT    NOT NULL,
    observation_kind            TEXT    NOT NULL,
    payload_contract            TEXT    NOT NULL,
    payload_sha256              TEXT    NOT NULL,
    payload_bytes               BLOB,
    payload_byte_len            INTEGER NOT NULL CHECK (payload_byte_len > 0),
    extensions_digest           TEXT    NOT NULL,
    extensions_json             TEXT,
    provenance_origin           TEXT    NOT NULL CHECK (provenance_origin IN (
        'user', 'agent', 'tool', 'repository', 'tracedecay_native', 'automation')),
    provenance_sha256           TEXT    NOT NULL,
    privacy_classification      TEXT    NOT NULL CHECK (privacy_classification IN (
        'public', 'internal', 'sensitive', 'restricted')),
    retention_class             TEXT    NOT NULL CHECK (retention_class IN (
        'ephemeral', 'session', 'project', 'profile')),
    redaction_revision          INTEGER NOT NULL CHECK (redaction_revision >= 0),
    content_policy_revision     INTEGER NOT NULL CHECK (content_policy_revision >= 0),
    forget_source_key           TEXT    NOT NULL,
    expires_at_micros           INTEGER NOT NULL,
    occurred_at_micros          INTEGER NOT NULL,
    admitted_at_micros          INTEGER NOT NULL,
    deadline_micros             INTEGER NOT NULL,
    request_id                  TEXT    NOT NULL,
    envelope_sha256             TEXT    NOT NULL,
    sanitization_receipt_id     TEXT,
    sanitizer_revision          TEXT,
    source_payload_sha256       TEXT,
    sanitization_receipt_json   TEXT,
    content_forgotten_at_micros INTEGER,
    -- The hygiene binding is all-or-nothing. It is content-derived evidence,
    -- so privacy deletion clears all four columns together; any other
    -- combination is a row no reader can decode and is refused at write time.
    CHECK ((sanitization_receipt_id IS NULL) = (sanitizer_revision IS NULL)
       AND (sanitization_receipt_id IS NULL) = (source_payload_sha256 IS NULL)
       AND (sanitization_receipt_id IS NULL) = (sanitization_receipt_json IS NULL)),
    -- Content may only be present while its hygiene evidence is present.
    CHECK (payload_bytes IS NULL OR sanitization_receipt_id IS NOT NULL),
    PRIMARY KEY (idempotency_key)
) WITHOUT ROWID;

CREATE UNIQUE INDEX IF NOT EXISTS tdmem_observation_journal_observation_v1
    ON tdmem_observation_journal_v1 (observation_id);

-- One journal row per settled source event per provider *registration*. Two
-- admissions of the same event that derive different idempotency keys - which
-- happens when the sanitizer rule corpus changed between admission and crash
-- replay - collide here instead of creating a second row. The registration, not
-- the instance, is part of the key: one settled event legitimately fans out to
-- several registrations, while a restarted provider is the same registration
-- under a new instance id and must not get a second row.
CREATE UNIQUE INDEX IF NOT EXISTS tdmem_observation_journal_source_v1
    ON tdmem_observation_journal_v1 (
        provider_id, registration_revision, source_authority,
        exact_scope_sha256, source_stream, source_sequence);

CREATE INDEX IF NOT EXISTS tdmem_observation_journal_forget_v1
    ON tdmem_observation_journal_v1 (forget_source_key, content_forgotten_at_micros);

CREATE INDEX IF NOT EXISTS tdmem_observation_journal_retention_v1
    ON tdmem_observation_journal_v1 (retention_class, expires_at_micros);

CREATE TABLE IF NOT EXISTS tdmem_observation_delivery_v1 (
    idempotency_key         TEXT    NOT NULL
        REFERENCES tdmem_observation_journal_v1 (idempotency_key) ON DELETE CASCADE,
    observation_id          TEXT    NOT NULL,
    provider_id             TEXT    NOT NULL,
    registration_revision   INTEGER NOT NULL CHECK (registration_revision > 0),
    -- Per-attempt evidence: the instance that most recently claimed a lease.
    -- Nothing addresses a delivery row by it.
    last_provider_instance_id TEXT,
    state                   TEXT    NOT NULL CHECK (state IN (
        'pending', 'leased', 'acknowledged', 'duplicate_acknowledged', 'rejected',
        'effect_unknown', 'cancelled', 'expired', 'exhausted', 'forgotten')),
    -- Attempts consumed, incremented by the lease claim itself. A reaped lease
    -- never returns its number, so no two attempts of one row can share a
    -- receipt slot and a retry loop always converges on max_attempts.
    attempt_number          INTEGER NOT NULL CHECK (attempt_number >= 0),
    next_attempt_at_micros  INTEGER NOT NULL,
    lease_owner             TEXT,
    lease_id                TEXT,
    lease_expires_at_micros INTEGER,
    last_outcome            TEXT,
    last_committed_effect   TEXT,
    last_receipt_id         TEXT,
    source_sequence         INTEGER NOT NULL CHECK (source_sequence >= 0),
    exact_scope_sha256      TEXT    NOT NULL,
    queue_bytes             INTEGER NOT NULL CHECK (queue_bytes > 0),
    updated_at_micros       INTEGER NOT NULL,
    CHECK ((state = 'leased') = (lease_id IS NOT NULL)),
    PRIMARY KEY (idempotency_key)
) WITHOUT ROWID;

CREATE UNIQUE INDEX IF NOT EXISTS tdmem_observation_delivery_observation_v1
    ON tdmem_observation_delivery_v1 (observation_id);

CREATE INDEX IF NOT EXISTS tdmem_observation_delivery_ready_v1
    ON tdmem_observation_delivery_v1 (
        provider_id, registration_revision, state, next_attempt_at_micros, source_sequence);

CREATE INDEX IF NOT EXISTS tdmem_observation_delivery_lease_v1
    ON tdmem_observation_delivery_v1 (state, lease_expires_at_micros);

CREATE TABLE IF NOT EXISTS tdmem_observation_receipt_v1 (
    observation_id               TEXT    NOT NULL,
    attempt_number               INTEGER NOT NULL CHECK (attempt_number >= 1),
    receipt_id                   TEXT    NOT NULL,
    idempotency_key              TEXT    NOT NULL,
    payload_sha256               TEXT    NOT NULL,
    extensions_digest            TEXT    NOT NULL,
    provider_id                  TEXT    NOT NULL,
    -- Per-attempt evidence. NULL when the journal itself terminated the
    -- delivery (deadline, retention, privacy deletion) and no provider
    -- instance made the attempt.
    provider_instance_id         TEXT,
    registration_revision        INTEGER NOT NULL CHECK (registration_revision > 0),
    state_generation_before      INTEGER,
    state_generation_after       INTEGER,
    outcome                      TEXT    NOT NULL,
    committed_effect             TEXT    NOT NULL CHECK (committed_effect IN (
        'none', 'applied', 'duplicate', 'partial', 'unknown')),
    provider_effect_summary_json TEXT    NOT NULL,
    provider_receipt_digest      TEXT,
    started_at_micros            INTEGER NOT NULL,
    finished_at_micros           INTEGER NOT NULL,
    warnings_json                TEXT    NOT NULL,
    PRIMARY KEY (observation_id, attempt_number)
) WITHOUT ROWID;

CREATE UNIQUE INDEX IF NOT EXISTS tdmem_observation_receipt_id_v1
    ON tdmem_observation_receipt_v1 (receipt_id);

CREATE INDEX IF NOT EXISTS tdmem_observation_receipt_key_v1
    ON tdmem_observation_receipt_v1 (idempotency_key, attempt_number);

-- Answered attempts whose terminal the host refused. Same key shape as the
-- receipt table and the same append-only discipline, but deliberately a
-- different table: a row here is evidence that an attempt number was spent on
-- an answer the host would not accept, never evidence that a provider effect
-- committed. Nothing addresses a delivery row by it and nothing reads content
-- from it.
CREATE TABLE IF NOT EXISTS tdmem_observation_attempt_refusal_v1 (
    observation_id          TEXT    NOT NULL,
    attempt_number          INTEGER NOT NULL CHECK (attempt_number >= 1),
    idempotency_key         TEXT    NOT NULL,
    provider_id             TEXT    NOT NULL,
    provider_instance_id    TEXT    NOT NULL,
    registration_revision   INTEGER NOT NULL CHECK (registration_revision > 0),
    exact_scope_sha256      TEXT    NOT NULL,
    category                TEXT    NOT NULL CHECK (category IN (
        'terminal_identity_mismatch', 'receipt_not_admissible')),
    refused_field           TEXT    NOT NULL,
    expected_value          TEXT,
    provided_value          TEXT,
    detail                  TEXT    NOT NULL,
    terminal_operation      TEXT    NOT NULL,
    terminal_code           TEXT    NOT NULL,
    terminal_operation_id   TEXT    NOT NULL,
    provider_receipt_digest TEXT,
    started_at_micros       INTEGER NOT NULL,
    finished_at_micros      INTEGER NOT NULL,
    recorded_at_micros      INTEGER NOT NULL,
    PRIMARY KEY (observation_id, attempt_number)
) WITHOUT ROWID;

CREATE INDEX IF NOT EXISTS tdmem_observation_attempt_refusal_key_v1
    ON tdmem_observation_attempt_refusal_v1 (idempotency_key, attempt_number);

-- Attempt numbers spent by a lease that lapsed before any answer became
-- durable. Same key shape and same append-only discipline as the receipt and
-- refusal tables, and deliberately a third table: a row here says an attempt
-- was consumed and the host does not know what became of it, which is the one
-- thing neither of the other two may ever say. Nothing addresses a delivery row
-- by it and it carries no committed-effect claim.
CREATE TABLE IF NOT EXISTS tdmem_observation_attempt_orphan_v1 (
    observation_id            TEXT    NOT NULL,
    attempt_number            INTEGER NOT NULL CHECK (attempt_number >= 1),
    idempotency_key           TEXT    NOT NULL,
    provider_id               TEXT    NOT NULL,
    provider_instance_id      TEXT,
    registration_revision     INTEGER NOT NULL CHECK (registration_revision > 0),
    exact_scope_sha256        TEXT    NOT NULL,
    lease_id                  TEXT    NOT NULL,
    lease_owner               TEXT    NOT NULL,
    payload_sha256            TEXT    NOT NULL,
    cause                     TEXT    NOT NULL CHECK (cause IN (
        'lease_expired_without_answer')),
    recovery                  TEXT    NOT NULL CHECK (recovery IN (
        'redelivery_scheduled', 'attempts_exhausted')),
    lease_expired_at_micros   INTEGER NOT NULL,
    recorded_at_micros        INTEGER NOT NULL,
    PRIMARY KEY (observation_id, attempt_number)
) WITHOUT ROWID;

CREATE INDEX IF NOT EXISTS tdmem_observation_attempt_orphan_key_v1
    ON tdmem_observation_attempt_orphan_v1 (idempotency_key, attempt_number);

CREATE TABLE IF NOT EXISTS tdmem_observation_replay_cursor_v1 (
    source_authority             TEXT    NOT NULL,
    exact_scope_sha256           TEXT    NOT NULL,
    source_stream                TEXT    NOT NULL,
    last_admitted_sequence       INTEGER NOT NULL CHECK (last_admitted_sequence >= 0),
    last_source_event_id         TEXT    NOT NULL,
    last_source_event_revision   TEXT    NOT NULL,
    last_settlement_proof_sha256 TEXT,
    last_disposition             TEXT    NOT NULL CHECK (last_disposition IN (
        'admitted', 'withheld')),
    updated_at_micros            INTEGER NOT NULL,
    PRIMARY KEY (source_authority, exact_scope_sha256, source_stream)
) WITHOUT ROWID;

-- How far one provider registration has been admitted on one stream. The
-- ingress cursor above answers "where does replay resume"; this answers "is
-- this a regression for *this* target", which is the only question fan-out can
-- answer correctly: event n may reach registration B long after registration A
-- has taken n + 1.
CREATE TABLE IF NOT EXISTS tdmem_observation_target_cursor_v1 (
    provider_id                TEXT    NOT NULL,
    registration_revision      INTEGER NOT NULL CHECK (registration_revision > 0),
    source_authority           TEXT    NOT NULL,
    exact_scope_sha256         TEXT    NOT NULL,
    source_stream              TEXT    NOT NULL,
    last_admitted_sequence     INTEGER NOT NULL CHECK (last_admitted_sequence >= 0),
    last_source_event_id       TEXT    NOT NULL,
    last_source_event_revision INTEGER NOT NULL CHECK (last_source_event_revision >= 0),
    updated_at_micros          INTEGER NOT NULL,
    PRIMARY KEY (provider_id, registration_revision, source_authority,
                 exact_scope_sha256, source_stream)
) WITHOUT ROWID;

-- Restart recovery for one provider registration on one stream: the durable
-- acknowledged watermark, the provider state identity the last converged
-- assessment accepted, and the bounded automatic repair counter.
--
-- The watermark is the reason this is a row rather than a query. A retention
-- sweep or a privacy deletion legitimately removes an acknowledged delivery,
-- and a watermark derived from surviving rows would then move backwards and
-- re-propose work the provider already applied. The only statement that writes
-- `acknowledged_sequence` refuses to lower it, so the sequence is monotonic by
-- construction rather than by discipline.
CREATE TABLE IF NOT EXISTS tdmem_observation_recovery_v1 (
    provider_id                    TEXT    NOT NULL,
    registration_revision          INTEGER NOT NULL CHECK (registration_revision > 0),
    source_authority               TEXT    NOT NULL,
    exact_scope_sha256             TEXT    NOT NULL,
    source_stream                  TEXT    NOT NULL,
    acknowledged_sequence          INTEGER CHECK (acknowledged_sequence >= 0),
    acknowledged_observation_id    TEXT,
    acknowledged_at_micros         INTEGER,
    implementation_identity_sha256 TEXT,
    state_schema_version           TEXT,
    state_generation               INTEGER CHECK (state_generation >= 0),
    replay_position_retained       INTEGER CHECK (replay_position_retained IN (0, 1)),
    automatic_repair_attempts      INTEGER NOT NULL CHECK (automatic_repair_attempts >= 0),
    last_defect                    TEXT,
    last_assessment_id             TEXT,
    updated_at_micros              INTEGER NOT NULL,
    CHECK ((acknowledged_sequence IS NULL) = (acknowledged_at_micros IS NULL)),
    CHECK ((acknowledged_sequence IS NULL) = (acknowledged_observation_id IS NULL)),
    PRIMARY KEY (provider_id, registration_revision, source_authority,
                 exact_scope_sha256, source_stream)
) WITHOUT ROWID;

CREATE TABLE IF NOT EXISTS tdmem_observation_withheld_v2 (
    source_authority      TEXT    NOT NULL,
    exact_scope_sha256    TEXT    NOT NULL,
    source_stream         TEXT    NOT NULL,
    source_sequence       INTEGER NOT NULL CHECK (source_sequence >= 0),
    receipt_id            TEXT    NOT NULL,
    source_event_id       TEXT    NOT NULL,
    source_event_revision TEXT    NOT NULL,
    reason                TEXT    NOT NULL,
    source_payload_sha256 TEXT    NOT NULL,
    extensions_digest     TEXT    NOT NULL,
    sanitizer_revision    TEXT    NOT NULL,
    finding_count         INTEGER NOT NULL CHECK (finding_count >= 0),
    findings_digest       TEXT    NOT NULL,
    forget_source_key     TEXT    NOT NULL,
    withheld_at_micros    INTEGER NOT NULL,
    PRIMARY KEY (source_authority, exact_scope_sha256, source_stream, source_sequence, receipt_id)
) WITHOUT ROWID;

-- Privacy deletion reaches the withheld audit by key, and the retention sweep
-- ages it out by instant. Neither is possible without these.
CREATE INDEX IF NOT EXISTS tdmem_observation_withheld_forget_v2
    ON tdmem_observation_withheld_v2 (forget_source_key);

CREATE INDEX IF NOT EXISTS tdmem_observation_withheld_age_v2
    ON tdmem_observation_withheld_v2 (withheld_at_micros);
"#;

/// How long a normal statement waits behind another writer.
pub(crate) const BUSY_TIMEOUT_MILLIS: u64 = 5_000;

/// Rows of the withheld audit one *open* revalidates before it returns.
///
/// Open cost has to be flat in the size of that table, so the pass is bounded
/// here and the remainder is walked by
/// [`SqliteObservationJournal::validate_withheld_backlog`] on the owner's own
/// loop. The bound is a page, not a sample: the whole table is still
/// revalidated, just not all of it on the caller's critical path.
///
/// [`SqliteObservationJournal::validate_withheld_backlog`]: super::SqliteObservationJournal::validate_withheld_backlog
pub const OPEN_WITHHELD_AUDIT_ROWS: u32 = 256;

/// Applies pragmas and the versioned schema, failing closed on a newer store.
///
/// Returns where the bounded open-time withheld audit stopped, or `None` when
/// it already reached the end of the table.
pub(crate) fn initialize(
    connection: &mut Connection,
) -> Result<Option<WithheldAuditCursorV1>, ObservationJournalError> {
    // `journal_mode` returns a row, so it cannot go through `execute_batch`.
    let _mode: String = connection.query_row("PRAGMA journal_mode = WAL", [], |row| row.get(0))?;
    connection.execute_batch(CONNECTION_PRAGMAS)?;
    connection.busy_timeout(std::time::Duration::from_millis(BUSY_TIMEOUT_MILLIS))?;

    let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
    let version: i64 = transaction.query_row("PRAGMA user_version", [], |row| row.get(0))?;
    if version > SCHEMA_VERSION {
        return Err(ObservationJournalError::SchemaAhead {
            found: version,
            supported: SCHEMA_VERSION,
        });
    }

    match version {
        0 => transaction.execute_batch(SCHEMA_DDL)?,
        LEGACY_SCHEMA_VERSION => migrate_v1_to_v2(&transaction)?,
        // Version 2 gained the refused-terminal audit; every statement in the
        // DDL is `IF NOT EXISTS`, so applying it adds the new table and its
        // index without touching a single existing row.
        WITHHELD_V2_SCHEMA_VERSION | PRE_RECOVERY_SCHEMA_VERSION => {
            transaction.execute_batch(SCHEMA_DDL)?;
        }
        // Version 4 already has the recovery table, so `CREATE TABLE IF NOT
        // EXISTS` cannot widen it: the two columns are added explicitly.
        PRE_ASSESSMENT_IDENTITY_SCHEMA_VERSION
        | PRE_ATTEMPT_ORPHAN_SCHEMA_VERSION
        | SCHEMA_VERSION => {
            transaction.execute_batch(SCHEMA_DDL)?;
            add_recovery_v5_columns(&transaction)?;
        }
        _ => {
            return Err(ObservationJournalError::Corrupt {
                table: "sqlite_schema",
                field: "user_version",
            });
        }
    }
    let audit = validate_withheld_page(&transaction, None, OPEN_WITHHELD_AUDIT_ROWS)?;
    transaction.execute_batch(&format!("PRAGMA user_version = {SCHEMA_VERSION}"))?;
    transaction.commit()?;
    Ok(audit.resume_after)
}

/// Adds the version-5 recovery columns to a table that predates them.
///
/// `ALTER TABLE ... ADD COLUMN` is not idempotent, so each column is added only
/// when the table does not already carry it; a store created fresh at version 5
/// therefore passes through untouched.
fn add_recovery_v5_columns(transaction: &Transaction<'_>) -> Result<(), ObservationJournalError> {
    for (column, declaration) in RECOVERY_V5_COLUMNS {
        let present: i64 = transaction.query_row(
            "SELECT COUNT(*) FROM pragma_table_info('tdmem_observation_recovery_v1') \
             WHERE name = ?1",
            [column],
            |row| row.get(0),
        )?;
        if present == 0 {
            transaction.execute_batch(&format!(
                "ALTER TABLE tdmem_observation_recovery_v1 ADD COLUMN {column} {declaration}"
            ))?;
        }
    }
    Ok(())
}

/// Upgrades the original store without fabricating legacy hygiene evidence.
///
/// The stricter audit has its own physical table name. A version-1 table may be
/// left inert when empty, but rows in it cannot be migrated: they predate the
/// sanitizer revision and canonical findings evidence required to rederive the
/// receipt identity.
fn migrate_v1_to_v2(transaction: &Transaction<'_>) -> Result<(), ObservationJournalError> {
    let legacy_table_exists = transaction
        .query_row(
            "SELECT 1 FROM sqlite_schema WHERE type = 'table' \
             AND name = 'tdmem_observation_withheld_v1'",
            [],
            |row| row.get::<_, i64>(0),
        )
        .optional()?
        .is_some();
    if legacy_table_exists {
        let rows: i64 = transaction.query_row(
            "SELECT COUNT(*) FROM tdmem_observation_withheld_v1",
            [],
            |row| row.get(0),
        )?;
        if rows != 0 {
            return Err(
                ObservationJournalError::LegacyWithheldEvidenceUnmigratable {
                    rows: u64::try_from(rows).unwrap_or(u64::MAX),
                },
            );
        }
    }
    transaction.execute_batch(SCHEMA_DDL)?;
    Ok(())
}
