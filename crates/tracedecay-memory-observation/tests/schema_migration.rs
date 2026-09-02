//! Schema migration and persisted withheld-evidence integrity.

mod support;

use rusqlite::{Connection, params};
use support::{TestResult, digest_hex, journal, policy, withheld_at};
use tracedecay_memory_observation::{
    ObservationJournalError, SCHEMA_VERSION, SqliteObservationJournal,
};

const LEGACY_WITHHELD_DDL: &str = r#"
CREATE TABLE tdmem_observation_withheld_v1 (
    source_authority      TEXT    NOT NULL,
    exact_scope_sha256    TEXT    NOT NULL,
    source_stream         TEXT    NOT NULL,
    source_sequence       INTEGER NOT NULL CHECK (source_sequence >= 0),
    receipt_id            TEXT    NOT NULL,
    source_event_id       TEXT    NOT NULL,
    source_event_revision TEXT    NOT NULL,
    reason                TEXT    NOT NULL,
    source_payload_sha256 TEXT    NOT NULL,
    forget_source_key     TEXT    NOT NULL,
    withheld_at_micros    INTEGER NOT NULL,
    PRIMARY KEY (source_authority, exact_scope_sha256, source_stream, source_sequence, receipt_id)
) WITHOUT ROWID;
PRAGMA user_version = 1;
"#;

#[test]
fn a_fresh_store_records_the_current_schema_version() -> TestResult {
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("journal.sqlite3");
    drop(journal(&path)?);

    let connection = Connection::open(path)?;
    let version: i64 = connection.query_row("PRAGMA user_version", [], |row| row.get(0))?;
    assert_eq!(version, SCHEMA_VERSION);
    let table: String = connection.query_row(
        "SELECT name FROM sqlite_schema WHERE type = 'table' \
         AND name = 'tdmem_observation_withheld_v2'",
        [],
        |row| row.get(0),
    )?;
    assert_eq!(table, "tdmem_observation_withheld_v2");
    Ok(())
}

#[test]
fn an_empty_legacy_audit_is_left_inert_and_migrated_without_loss() -> TestResult {
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("journal.sqlite3");
    {
        let connection = Connection::open(&path)?;
        connection.execute_batch(LEGACY_WITHHELD_DDL)?;
    }

    drop(journal(&path)?);
    let connection = Connection::open(path)?;
    let version: i64 = connection.query_row("PRAGMA user_version", [], |row| row.get(0))?;
    assert_eq!(version, SCHEMA_VERSION);
    let evidence_columns: i64 = connection.query_row(
        "SELECT COUNT(*) FROM pragma_table_info('tdmem_observation_withheld_v2') \
         WHERE name IN ('extensions_digest', 'sanitizer_revision', 'finding_count', 'findings_digest')",
        [],
        |row| row.get(0),
    )?;
    assert_eq!(evidence_columns, 4);
    let indexes: i64 = connection.query_row(
        "SELECT COUNT(*) FROM sqlite_schema WHERE type = 'index' \
         AND name IN ('tdmem_observation_withheld_forget_v2', \
                      'tdmem_observation_withheld_age_v2')",
        [],
        |row| row.get(0),
    )?;
    assert_eq!(indexes, 2);
    let legacy_rows: i64 = connection.query_row(
        "SELECT COUNT(*) FROM tdmem_observation_withheld_v1",
        [],
        |row| row.get(0),
    )?;
    assert_eq!(legacy_rows, 0);
    Ok(())
}

#[test]
fn populated_legacy_audit_refuses_migration_without_inventing_evidence() -> TestResult {
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("journal.sqlite3");
    {
        let connection = Connection::open(&path)?;
        connection.execute_batch(LEGACY_WITHHELD_DDL)?;
        connection.execute(
            "INSERT INTO tdmem_observation_withheld_v1 (
                 source_authority, exact_scope_sha256, source_stream, source_sequence,
                 receipt_id, source_event_id, source_event_revision, reason,
                 source_payload_sha256, forget_source_key, withheld_at_micros
             ) VALUES ('host_session', ?1, 'session-1', 1, 'legacy-receipt',
                       'event-1', '0', 'secret_rejected', ?2, 'forget:session-1', 1)",
            params![digest_hex(b"scope"), digest_hex(b"source")],
        )?;
    }

    assert!(matches!(
        SqliteObservationJournal::open(&path, policy()),
        Err(ObservationJournalError::LegacyWithheldEvidenceUnmigratable { rows: 1 })
    ));
    let connection = Connection::open(path)?;
    let version: i64 = connection.query_row("PRAGMA user_version", [], |row| row.get(0))?;
    assert_eq!(version, 1, "a blocked migration advanced the schema marker");
    let rows: i64 = connection.query_row(
        "SELECT COUNT(*) FROM tdmem_observation_withheld_v1",
        [],
        |row| row.get(0),
    )?;
    assert_eq!(rows, 1, "a blocked migration deleted legacy audit evidence");
    Ok(())
}

#[test]
fn withheld_evidence_round_trips_exactly_through_sqlite() -> TestResult {
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("journal.sqlite3");
    let withheld = withheld_at(7, "forget:session-1")?;
    {
        let store = journal(&path)?;
        store.record_withheld_at(&withheld, 123_456)?;
    }

    let connection = Connection::open(path)?;
    let stored: (String, String, String, String, i64, String, String, i64) = connection.query_row(
        "SELECT receipt_id, reason, source_payload_sha256, extensions_digest, finding_count,
                sanitizer_revision, findings_digest, withheld_at_micros
         FROM tdmem_observation_withheld_v2 WHERE source_sequence = 7",
        [],
        |row| {
            Ok((
                row.get(0)?,
                row.get(1)?,
                row.get(2)?,
                row.get(3)?,
                row.get(4)?,
                row.get(5)?,
                row.get(6)?,
                row.get(7)?,
            ))
        },
    )?;
    assert_eq!(stored.0, withheld.receipt_id);
    assert_eq!(stored.1, withheld.reason);
    assert_eq!(stored.2, withheld.source_payload_sha256);
    assert_eq!(stored.3, withheld.extensions_digest);
    assert_eq!(stored.4, i64::from(withheld.finding_count));
    assert_eq!(stored.5, withheld.sanitizer_revision);
    assert_eq!(stored.6, withheld.findings_digest);
    assert_eq!(stored.7, 123_456);
    Ok(())
}

#[test]
fn restart_rejects_every_persisted_withheld_receipt_perturbation() -> TestResult {
    let perturbations = [
        "UPDATE tdmem_observation_withheld_v2 SET sanitizer_revision = sanitizer_revision || '.other'",
        "UPDATE tdmem_observation_withheld_v2 SET source_payload_sha256 = 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa'",
        "UPDATE tdmem_observation_withheld_v2 SET extensions_digest = 'cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc'",
        "UPDATE tdmem_observation_withheld_v2 SET reason = 'quarantined'",
        "UPDATE tdmem_observation_withheld_v2 SET finding_count = finding_count + 1",
        "UPDATE tdmem_observation_withheld_v2 SET findings_digest = 'bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb'",
        "UPDATE tdmem_observation_withheld_v2 SET receipt_id = receipt_id || '0'",
    ];

    for (index, perturbation) in perturbations.into_iter().enumerate() {
        let directory = tempfile::tempdir()?;
        let path = directory.path().join(format!("journal-{index}.sqlite3"));
        {
            let store = journal(&path)?;
            store.record_withheld_at(&withheld_at(7, "forget:session-1")?, 123_456)?;
        }
        {
            let connection = Connection::open(&path)?;
            assert_eq!(connection.execute(perturbation, [])?, 1);
        }
        assert!(matches!(
            SqliteObservationJournal::open(&path, policy()),
            Err(ObservationJournalError::Corrupt {
                table: "tdmem_observation_withheld_v2",
                field: "receipt_id"
            })
        ));
    }
    Ok(())
}
