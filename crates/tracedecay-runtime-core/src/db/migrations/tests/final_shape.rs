//! Persisted-schema admission regressions for the one accepted runtime shape.

use std::{
    fs,
    path::{Path, PathBuf},
};

use tempfile::TempDir;

use crate::db::engine::TestConnection;

use super::super::{
    SCHEMA_VERSION, create_schema_connection, ensure_schema_current_connection,
    verify_final_schema_connection,
};

#[derive(Debug, PartialEq, Eq)]
struct StoreSnapshot {
    user_version: i64,
    schema_bytes: Vec<u8>,
    file_bytes: Vec<u8>,
}

async fn fresh_current_store() -> (TempDir, PathBuf) {
    let directory = tempfile::tempdir().expect("create final-shape fixture directory");
    let path = directory.path().join("final-shape.db");
    let connection = TestConnection::open(&path);
    create_schema_connection(&connection)
        .await
        .expect("create final-shape fixture");
    drop(connection);
    (directory, path)
}

fn object_sql(path: &Path, object_type: &str, name: &str) -> Option<String> {
    let connection =
        rusqlite::Connection::open_with_flags(path, rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY)
            .expect("open final-shape fixture read-only");
    connection
        .query_row(
            "SELECT sql FROM sqlite_master WHERE type = ?1 AND name = ?2",
            [object_type, name],
            |row| row.get(0),
        )
        .ok()
        .flatten()
}

fn table_has_column(path: &Path, table: &str, column: &str) -> bool {
    let connection =
        rusqlite::Connection::open_with_flags(path, rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY)
            .expect("open final-shape fixture read-only");
    let mut statement = connection
        .prepare("SELECT 1 FROM pragma_table_xinfo(?1) WHERE name = ?2 COLLATE NOCASE")
        .expect("prepare final-shape column probe");
    statement.query_row([table, column], |_| Ok(())).is_ok()
}

fn store_snapshot(path: &Path) -> StoreSnapshot {
    let connection =
        rusqlite::Connection::open_with_flags(path, rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY)
            .expect("open final-shape snapshot read-only");
    let user_version = connection
        .query_row("PRAGMA user_version", (), |row| row.get(0))
        .expect("read final-shape snapshot version");
    let schema_bytes = connection
        .query_row(
            "SELECT CAST(COALESCE(group_concat(entry, char(0)), '') AS BLOB)
             FROM (
                 SELECT type || ':' || name || ':' || COALESCE(sql, '') AS entry
                 FROM sqlite_master
                 WHERE name NOT LIKE 'sqlite_%'
                 ORDER BY type, name
             )",
            (),
            |row| row.get(0),
        )
        .expect("read final-shape snapshot schema");
    drop(connection);
    StoreSnapshot {
        user_version,
        schema_bytes,
        file_bytes: fs::read(path).expect("read final-shape fixture bytes"),
    }
}

fn tamper(path: &Path, sql: &str) {
    let connection = rusqlite::Connection::open(path).expect("open final-shape fixture to tamper");
    connection
        .execute_batch(sql)
        .expect("apply literal final-shape tamper");
}

/// Opens the store through the runtime-core writer path, requires the typed
/// reset-required refusal, and returns its reason. The direct test connection
/// keeps publication outside this admission fixture so a refusal can be
/// proven byte-for-byte.
async fn assert_reset_required_without_repair(path: &Path, mutation: &str) -> String {
    let before = store_snapshot(path);
    let connection = TestConnection::open(path);
    let error = ensure_schema_current_connection(&connection)
        .await
        .expect_err("a stamped store with a structural tamper must be refused");
    let (authority, reason) = error
        .reset_required_context()
        .expect("final-shape refusal must remain typed reset-required");
    assert_eq!(authority, "SQLite store", "{mutation} refusal authority");
    drop(connection);
    assert_eq!(
        store_snapshot(path),
        before,
        "{mutation} refusal must not repair or otherwise rewrite the store"
    );
    reason.to_owned()
}

/// The canonical project store exactly as every release from v0.1.0-beta.25
/// through v0.1.0-beta.37 wrote it. The fixture header carries the
/// tag-to-inventory table; it is assembled from the tagged DDL rather than
/// from the current contract, because a released shape derived from the
/// current contract agrees with whatever this binary expects and so cannot
/// detect an admission that refuses what shipped.
const RELEASED_PROJECT_STORE_SQL: &str =
    include_str!("../../../../tests/fixtures/project-store-released-v34.sql");

/// Writes the released project store into an empty file, in the WAL mode
/// every shipped binary ran, and applies the requested shipped stamp.
fn released_project_store(directory: &TempDir, stamp: u32) -> PathBuf {
    let path = directory.path().join(format!("released-v{stamp}.db"));
    let connection = rusqlite::Connection::open(&path).expect("create released store fixture");
    connection
        .pragma_update(None, "journal_mode", "WAL")
        .expect("run the released store in WAL mode");
    connection
        .execute_batch(RELEASED_PROJECT_STORE_SQL)
        .expect("install the released project schema");
    connection
        .execute_batch(&format!("PRAGMA user_version = {stamp};"))
        .expect("stamp the released schema version");
    path
}

/// Every release from v0.1.0-beta.25 through v0.1.0-beta.37 wrote one
/// byte-identical project store, and every one of them carries the
/// `semantic_vector_*` staging family v36 retired with dense code retrieval.
/// That family has no forward path, so opening a released store must refuse
/// with the typed reset remedy, naming the retired object, before any legacy
/// row decode, repair, or publication.
#[tokio::test]
async fn released_project_store_is_refused_without_mutation() {
    for stamp in [34_u32, 35_u32] {
        let directory = tempfile::tempdir().expect("create released fixture directory");
        let path = released_project_store(&directory, stamp);
        assert!(
            object_sql(&path, "table", "semantic_vector_stages").is_some(),
            "the released fixture must carry the retired staging family"
        );

        let before = store_snapshot(&path);
        let read_only_connection = TestConnection::open(&path);
        let read_only = verify_final_schema_connection(&read_only_connection)
            .await
            .expect_err("a read-only mount must refuse a released dense store");
        let (authority, reason) = read_only
            .reset_required_context()
            .expect("read-only refusal of a released dense store is typed reset-required");
        assert_eq!(authority, "SQLite store");
        assert!(
            reason.contains("semantic_vector_"),
            "v{stamp} read-only refusal must name the retired family: {reason}"
        );
        drop(read_only_connection);
        assert_eq!(store_snapshot(&path), before);

        let reason = assert_reset_required_without_repair(&path, "released dense store").await;
        assert!(
            reason.contains("semantic_vector_"),
            "v{stamp} writer refusal must name the retired family: {reason}"
        );
        assert_eq!(store_snapshot(&path), before);
    }
}

/// Even a store whose objects happen to match the current inventory cannot be
/// admitted under a released stamp. This closes the old payload-digest and
/// trigger-repair escape hatches while checking the physical file and schema
/// inventory remain untouched.
#[tokio::test]
async fn released_v34_and_v35_stamps_are_reset_required_without_mutation() {
    for stamp in [34_u32, 35_u32] {
        let (_directory, path) = fresh_current_store().await;
        tamper(&path, &format!("PRAGMA user_version = {stamp};"));
        let before = store_snapshot(&path);
        let connection = TestConnection::open(&path);
        let error = ensure_schema_current_connection(&connection)
            .await
            .expect_err("released schema stamps must never migrate in place");
        let (authority, reason) = error
            .reset_required_context()
            .expect("released stamp refusal must be typed reset-required");
        assert_eq!(authority, "SQLite store");
        assert!(
            reason.contains(&format!("schema v{stamp}")),
            "refusal must identify the released stamp: {reason}"
        );
        drop(connection);
        assert_eq!(
            store_snapshot(&path),
            before,
            "v{stamp} refusal must leave the exact current-shape file untouched"
        );
    }
}

#[tokio::test]
async fn current_final_store_is_admitted_without_mutation() {
    let (_directory, path) = fresh_current_store().await;
    // Replaying canonical DDL is idempotent and leaves the exact inventory
    // unchanged; admission itself performs no install or repair.
    tamper(&path, tracedecay_store::GENERATION_DIAGNOSTICS_SCHEMA_DDL);
    tamper(
        &path,
        tracedecay_rusqlite_runtime::runtime_ledger::RUNTIME_LEDGER_SCHEMA,
    );
    let before = store_snapshot(&path);
    assert_eq!(before.user_version, i64::from(SCHEMA_VERSION));

    let connection = TestConnection::open(&path);
    ensure_schema_current_connection(&connection)
        .await
        .expect("the exact current store shape must remain admissible");
    drop(connection);

    assert_eq!(
        store_snapshot(&path),
        before,
        "current-shape admission must remain a query-only identity check"
    );
}

#[tokio::test]
async fn automation_run_receipt_indexes_are_required_final_shape() {
    let (_directory, path) = fresh_current_store().await;
    for name in [
        "idx_memory_v2_operation_receipts_automation_run",
        "idx_memory_v2_automatic_fact_receipts_automation_run",
    ] {
        let sql = object_sql(&path, "index", name).expect("automation-run index exists");
        assert!(
            sql.contains("json_extract"),
            "{name} must index the run identity"
        );
    }

    tamper(
        &path,
        "DROP INDEX idx_memory_v2_automatic_fact_receipts_automation_run;",
    );
    assert_reset_required_without_repair(&path, "missing automatic-run lookup index").await;
}

#[tokio::test]
async fn stamped_final_store_with_missing_or_tampered_required_shape_is_reset_required() {
    let (_directory, path) = fresh_current_store().await;
    tamper(&path, "DROP TABLE metadata;");
    assert!(object_sql(&path, "table", "metadata").is_none());
    assert_reset_required_without_repair(&path, "missing required table").await;

    let (_directory, path) = fresh_current_store().await;
    tamper(&path, "DROP INDEX idx_read_cache_session;");
    assert!(object_sql(&path, "index", "idx_read_cache_session").is_none());
    assert_reset_required_without_repair(&path, "missing required index").await;

    let (_directory, path) = fresh_current_store().await;
    tamper(
        &path,
        "ALTER TABLE metadata ADD COLUMN final_shape_tamper TEXT;",
    );
    assert!(table_has_column(&path, "metadata", "final_shape_tamper"));
    assert_reset_required_without_repair(&path, "unexpected final-shape column").await;

    let (_directory, path) = fresh_current_store().await;
    tamper(
        &path,
        "DROP TRIGGER memory_v2_automatic_fact_receipts_require_keys;",
    );
    assert!(
        object_sql(
            &path,
            "trigger",
            "memory_v2_automatic_fact_receipts_require_keys"
        )
        .is_none()
    );
    assert_reset_required_without_repair(&path, "missing required trigger").await;

    let (_directory, path) = fresh_current_store().await;
    tamper(
        &path,
        "CREATE TABLE unexpected_final_shape_object (id INTEGER PRIMARY KEY);",
    );
    assert!(object_sql(&path, "table", "unexpected_final_shape_object").is_some());
    assert_reset_required_without_repair(&path, "unexpected final-shape object").await;
}
