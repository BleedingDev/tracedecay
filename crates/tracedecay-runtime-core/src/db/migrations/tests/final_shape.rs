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

/// The v35 release added the payload-digest objects while retaining the rest of
/// the released inventory. The migration accepts both exact released
/// inventories because a v35 writer could be interrupted before its first
/// digest object was created.
const RELEASED_V35_PAYLOAD_DIGESTS_SCHEMA: &str =
    "CREATE TABLE IF NOT EXISTS memory_v2_assertion_payload_digests (
            payload_rowid INTEGER PRIMARY KEY,
            assertion_id TEXT NOT NULL,
            fact_id TEXT NOT NULL,
            owner_kind TEXT NOT NULL,
            project_id TEXT NOT NULL,
            content_digest TEXT NOT NULL CHECK(
                length(content_digest) = 71 AND content_digest LIKE 'sha256:%'
            ),
            UNIQUE(assertion_id, fact_id, owner_kind, project_id),
            FOREIGN KEY(payload_rowid)
                REFERENCES memory_v2_assertion_payloads(rowid)
        );

        CREATE INDEX IF NOT EXISTS memory_v2_assertion_payload_digests_lookup
            ON memory_v2_assertion_payload_digests(
                owner_kind, project_id, content_digest, fact_id
            );

        CREATE TRIGGER IF NOT EXISTS memory_v2_assertion_payload_digests_no_update
        BEFORE UPDATE ON memory_v2_assertion_payload_digests BEGIN
            SELECT RAISE(ABORT, 'memory_v2 assertion payload digests are immutable');
        END;
        CREATE TRIGGER IF NOT EXISTS memory_v2_payloads_digest_delete
        AFTER DELETE ON memory_v2_assertion_payloads BEGIN
            DELETE FROM memory_v2_assertion_payload_digests
            WHERE payload_rowid = OLD.rowid;
        END;";

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
    if stamp == 35 {
        connection
            .execute_batch(RELEASED_V35_PAYLOAD_DIGESTS_SCHEMA)
            .expect("install the released v35 payload-digest objects");
    }
    connection
        .execute_batch(&format!("PRAGMA user_version = {stamp};"))
        .expect("stamp the released schema version");
    path
}

/// Seed representative durable content in the released shape. The migration
/// must carry these rows into the same logical records in v36 while dropping
/// only the retired semantic-vector projection and normalized copy tables.
fn seed_released_project_store(path: &Path, stamp: u32) {
    let connection = rusqlite::Connection::open(path).expect("open released fixture to seed");
    connection
        .execute_batch(
            "INSERT INTO metadata(key, value) VALUES('migration.seed', 'kept');
             INSERT INTO retrieval_anchors(
                 anchor_id, anchor_json, owner_json, projection_generation
             ) VALUES(
                 'anchor-migration',
                 '{\"kind\":\"file\",\"path\":\"src/lib.rs\"}',
                 '{\"kind\":\"project\",\"project_id\":\"project-test\"}',
                 'generation-1'
             );
             INSERT INTO memory_v2_facts(
                 fact_id, owner_kind, project_id, owner_json, identity_json, created_at
             ) VALUES(
                 'fact-migration', 'project', 'project-test',
                 '{\"kind\":\"project\",\"project_id\":\"project-test\"}',
                 '{\"kind\":\"fact\",\"id\":\"fact-migration\"}', 1
             );
             INSERT INTO memory_v2_assertions(
                 assertion_id, fact_id, owner_kind, project_id, owner_json,
                 assertion_header_json, kind_json, payload_reference_json,
                 receipt_json, asserted_at, actor_id
             ) VALUES(
                 'assertion-migration', 'fact-migration', 'project', 'project-test',
                 '{\"kind\":\"project\",\"project_id\":\"project-test\"}',
                 '{}', '{}', '{\"kind\":\"payload\"}', '{}', 2, 'test'
             );
             INSERT INTO memory_v2_assertion_payloads(
                 assertion_id, fact_id, owner_kind, project_id, payload_json, content
             ) VALUES(
                 'assertion-migration', 'fact-migration', 'project', 'project-test',
                 '{\"text\":\"preserve me\"}', 'preserve me'
             );
             INSERT INTO memory_v2_lineage_events(
                 event_id, fact_id, owner_kind, project_id, event_json,
                 occurred_at, recorded_at
             ) VALUES(
                 'event-migration', 'fact-migration', 'project', 'project-test', '{}', 3, 3
             );
             INSERT INTO memory_v2_current_facts(
                 fact_id, owner_kind, project_id, payload_access, trust_score,
                 active_assertion_id, last_event_id, updated_at,
                 retrieval_count, access_count, helpful_count, unhelpful_count,
                 last_retrieved_at, last_recalled_at, last_feedback_at
             ) VALUES(
                 'fact-migration', 'project', 'project-test', 'eligible', 0.8,
                 'assertion-migration', 'event-migration', 4,
                 1, 2, 3, 4, 5, 6, 7
             );
             INSERT INTO graph_publication_replay_v1(
                 shard_id, namespace, projection, generation, idempotency_key,
                 input_digest, dependency_generation_closure_digest,
                 direct_dependency_bytes, expected_prior_head,
                 expected_recovered_digest, canonical_replay_source_digest,
                 canonical_replay_source
             ) VALUES(
                 'shard-migration', 'namespace', 'projection', 'generation-1', 'idempotency-1',
                 'input-1', 'closure-1', 2, NULL, 'recovered-1', 'source-1', X'01'
             );
             INSERT INTO diagnostic_generation_publications(
                 generation_id, record_state, state_generation, published_at
             ) VALUES('generation-1', 'current', NULL, 10);
             INSERT INTO generation_diagnostics(
                 diagnostic_anchor, generation_id, repository, worktree, reference,
                 source_revision, file_occurrence_id, content_digest,
                 symbol_occurrence_id, span_start, span_end, code, severity, message,
                 message_digest, producer_kind, producer, analyzer_revision,
                 configuration_revision, sanitization_receipt, evidence_class,
                 collected_at, record_state, state_generation, persisted_at
             ) VALUES(
                 'diagnostic-1', 'generation-1', 'repo', NULL, 'ref', 'revision-1',
                 'file-1', 'sha256:content', NULL, 0, 1, 'CODE', 'warning', 'message',
                 'sha256:message', 'test', 'test', 'analyzer-1', 'configuration-1',
                 NULL, 'direct', 10, 'current', NULL, 10
             );
             INSERT INTO external_source_objects_v1(
                 binding_id, native_object_digest, partition_digest,
                 mutation_digest, mutation_json
             ) VALUES(
                 'binding-1', 'sha256:object', 'sha256:partition',
                 'sha256:mutation', '{\"mutation_digest\":\"sha256:mutation\"}'
             );
             INSERT INTO external_source_projected_objects_v1(
                 binding_id, native_object_digest, mutation_json
             ) VALUES(
                 'binding-1', 'sha256:object', '{\"mutation_digest\":\"sha256:mutation\"}'
             );
             INSERT INTO external_source_projection_effects_v1(
                 binding_id, projection_digest, effect_index, native_object_digest,
                 effect_json, mutation_json
             ) VALUES(
                 'binding-1', 'sha256:projection', 0, 'sha256:object',
                 '{\"kind\":\"effect\"}', '{\"mutation_digest\":\"sha256:mutation\"}'
             );
             INSERT INTO external_source_commit_receipts_v1(
                 binding_id, idempotency_key, request_digest, definition_revision,
                 binding_revision, predecessor_frontier_digest,
                 successor_frontier_digest, receipt_digest, receipt_json
             ) VALUES(
                 'binding-1', 'commit-1', 'sha256:request', 1, 1,
                 'root', 'sha256:source', 'sha256:receipt',
                 '{\"idempotency_key\":\"commit-1\",\"request_digest\":\"sha256:request\",\"definition_revision\":1,\"definition_digest\":\"sha256:definition\",\"binding_revision\":1,\"binding_digest\":\"sha256:binding\",\"source_frontier\":{\"digest\":\"sha256:source\",\"n\":1},\"prior_source_frontier\":null,\"mutations\":[{\"mutation_digest\":\"sha256:mutation\"}],\"lineage\":[],\"snapshot_completion\":null,\"receipt_digest\":\"sha256:receipt\"}'
             );
             INSERT INTO external_source_mutations_v1(
                 binding_id, mutation_digest, native_object_digest,
                 revision_digest, source_receipt_digest, mutation_json
             ) VALUES(
                 'binding-1', 'sha256:mutation', 'sha256:object',
                 'sha256:revision', 'sha256:receipt',
                 '{\"mutation_digest\":\"sha256:mutation\"}'
             );
             INSERT INTO external_source_projection_publications_v1(
                 binding_id, projection_digest, source_receipt_digest,
                 predecessor_frontier_digest, successor_frontier_digest, receipt_json
             ) VALUES(
                 'binding-1', 'sha256:projection', 'sha256:receipt',
                 'sha256:source', 'sha256:projection',
                 '{\"projector\":{\"name\":\"test\"},\"definition_revision\":1,\"definition_digest\":\"sha256:definition\",\"binding_revision\":1,\"binding_digest\":\"sha256:binding\",\"expected_projection_frontier\":{\"digest\":\"sha256:source\"},\"source_frontier\":{\"digest\":\"sha256:projection\",\"n\":1},\"source_receipt_digest\":\"sha256:receipt\",\"mutations\":[{\"mutation_digest\":\"sha256:mutation\"}],\"effects\":[{\"kind\":\"effect\"}],\"lineage\":[],\"receipt_digest\":\"sha256:projection\"}'
             );",
        )
        .expect("seed released project content");
    // v35 stores already have the companion row; v34 stores get it as part of
    // the migration. The value is byte-equivalent to the production digest.
    if stamp == 35 {
        connection
            .execute_batch(
                "INSERT INTO memory_v2_assertion_payload_digests(
                     payload_rowid, assertion_id, fact_id, owner_kind, project_id, content_digest
                 )
                 SELECT rowid, assertion_id, fact_id, owner_kind, project_id,
                        'sha256:b48ed1895bff942135dda568a78b320ec96206710a83c3f2b9ce654d4ca45f4d'
                 FROM memory_v2_assertion_payloads;",
            )
            .expect("seed released v35 payload digest");
    }
}

/// A released store carrying memory, graph, diagnostics, and external-source
/// data migrates as one unit, while a read-only admission leaves it untouched.
#[tokio::test]
async fn released_project_store_migrates_without_data_loss() {
    for stamp in [34_u32, 35_u32] {
        let directory = tempfile::tempdir().expect("create released fixture directory");
        let path = released_project_store(&directory, stamp);
        seed_released_project_store(&path, stamp);
        assert!(
            object_sql(&path, "table", "semantic_vector_stages").is_some(),
            "the released fixture must carry the retired staging family"
        );

        let before = store_snapshot(&path);
        let read_only_connection = TestConnection::open(&path);
        let read_only = verify_final_schema_connection(&read_only_connection)
            .await
            .expect_err("a read-only mount must report pending writer migration");
        assert!(
            matches!(
                &read_only,
                tracedecay_domain::errors::TraceDecayError::Database { .. }
            ),
            "read-only released admission must report a pending migration: {read_only}"
        );
        let reason = read_only.to_string();
        assert!(
            reason.contains("writer migration") && reason.contains(&format!("schema v{stamp}")),
            "v{stamp} read-only admission must identify the writer migration: {reason}"
        );
        drop(read_only_connection);
        assert_eq!(store_snapshot(&path), before);

        let connection = TestConnection::open(&path);
        ensure_schema_current_connection(&connection)
            .await
            .expect("released project store migrates to v36");
        drop(connection);
        let migrated = rusqlite::Connection::open(&path).expect("open migrated project store");
        assert_eq!(
            migrated
                .query_row("PRAGMA user_version", (), |row| row.get::<_, i64>(0))
                .expect("read migrated schema stamp"),
            i64::from(SCHEMA_VERSION)
        );
        assert_eq!(
            migrated
                .query_row(
                    "SELECT value FROM metadata WHERE key = 'migration.seed'",
                    (),
                    |row| row.get::<_, String>(0)
                )
                .expect("metadata survives migration"),
            "kept"
        );
        assert_eq!(
            migrated
                .query_row(
                    "SELECT content FROM memory_v2_assertion_payloads WHERE rowid = 1",
                    (),
                    |row| row.get::<_, String>(0)
                )
                .expect("memory payload survives migration"),
            "preserve me"
        );
        assert_eq!(
            migrated
                .query_row("SELECT content_digest FROM memory_v2_assertion_payload_digests WHERE payload_rowid = 1", (), |row| row.get::<_, String>(0))
                .expect("payload digest survives migration"),
            "sha256:b48ed1895bff942135dda568a78b320ec96206710a83c3f2b9ce654d4ca45f4d"
        );
        assert_eq!(
            migrated
                .query_row("SELECT count(*) FROM graph_publication_replay_v1 WHERE generation = 'generation-1'", (), |row| row.get::<_, i64>(0))
                .expect("graph publication survives migration"),
            1
        );
        assert_eq!(
            migrated
                .query_row(
                    "SELECT count(*) FROM external_source_projection_publications_v2",
                    (),
                    |row| row.get::<_, i64>(0)
                )
                .expect("external publication survives migration"),
            1
        );
        assert_eq!(
            migrated
                .query_row("SELECT publication_revision FROM generation_diagnostics WHERE diagnostic_anchor = 'diagnostic-1'", (), |row| row.get::<_, i64>(0))
                .expect("diagnostic row survives migration"),
            1
        );
        assert!(
            object_sql(&path, "table", "semantic_vector_stages").is_none(),
            "retired semantic projection is removed after migration"
        );
        assert!(
            object_sql(&path, "table", "external_source_objects_v1").is_none(),
            "retired external mutation copy is removed after migration"
        );

        let second_before = store_snapshot(&path);
        let second_connection = TestConnection::open(&path);
        ensure_schema_current_connection(&second_connection)
            .await
            .expect("the migrated store is admitted on a second writer open");
        drop(second_connection);
        assert_eq!(
            store_snapshot(&path),
            second_before,
            "second admission of the migrated store is query-only"
        );
    }
}

/// Released source drift is rejected before any writer statement and remains
/// byte-identical. This covers both an unexpected object and altered DDL.
#[tokio::test]
async fn released_source_drift_is_reset_required_without_mutation() {
    for stamp in [34_u32, 35_u32] {
        let directory = tempfile::tempdir().expect("create released fixture directory");
        let path = released_project_store(&directory, stamp);
        tamper(
            &path,
            "CREATE TABLE released_shape_unknown (id INTEGER PRIMARY KEY);",
        );
        let reason =
            assert_reset_required_without_repair(&path, "unexpected released source object").await;
        assert!(
            reason.contains("released") || reason.contains("source schema"),
            "released source refusal must identify the source shape: {reason}"
        );
        // The first helper used an unmodified fixture. Add a separate DDL
        // mutation case to ensure a changed required object is also rejected.
        let directory = tempfile::tempdir().expect("create altered released fixture directory");
        let path = released_project_store(&directory, stamp);
        tamper(
            &path,
            "ALTER TABLE metadata ADD COLUMN released_shape_tamper TEXT;",
        );
        let reason =
            assert_reset_required_without_repair(&path, "altered released source DDL").await;
        assert!(
            reason.contains("incompatible")
                || reason.contains("missing")
                || reason.contains("unexpected"),
            "altered released source refusal must identify shape drift: {reason}"
        );
    }
}

/// A source row that cannot be represented in the normalized successor is a
/// database failure, and the writer transaction rolls back the schema changes
/// already issued before that failure.
#[tokio::test]
async fn released_migration_rolls_back_after_post_write_validation_failure() {
    let directory = tempfile::tempdir().expect("create rollback fixture directory");
    let path = released_project_store(&directory, 35);
    seed_released_project_store(&path, 35);
    tamper(
        &path,
        "UPDATE memory_v2_assertion_payload_digests
         SET content_digest = 'sha256:0000000000000000000000000000000000000000000000000000000000000000';",
    );
    let before = store_snapshot(&path);
    let connection = TestConnection::open(&path);
    let error = ensure_schema_current_connection(&connection)
        .await
        .expect_err("invalid v35 payload digest must fail migration");
    assert!(
        matches!(
            error,
            tracedecay_domain::errors::TraceDecayError::Database { .. }
        ),
        "payload digest validation is a database failure: {error}"
    );
    drop(connection);
    assert_eq!(
        store_snapshot(&path),
        before,
        "post-write migration failure must roll back to the exact released store"
    );
    assert!(object_sql(&path, "table", "semantic_vector_stages").is_some());
    assert!(object_sql(&path, "table", "external_source_objects_v1").is_some());
}

/// Legacy external-source rows are copied into several normalized tables, so
/// each digest-bearing column must agree with the JSON and its retained
/// history before the transaction changes the schema. Every contradiction is
/// rejected atomically and leaves the tampered released store untouched.
#[tokio::test]
async fn released_external_source_contradictions_roll_back_atomically() {
    for (label, mutation) in [
        (
            "object mutation digest",
            "UPDATE external_source_objects_v1
             SET mutation_digest = 'sha256:other';",
        ),
        (
            "commit successor frontier",
            "UPDATE external_source_commit_receipts_v1
             SET successor_frontier_digest = 'sha256:other';",
        ),
        (
            "projection source receipt",
            "UPDATE external_source_projection_publications_v1
             SET source_receipt_digest = 'sha256:other';",
        ),
        (
            "missing mutation history",
            "DELETE FROM external_source_mutations_v1;",
        ),
    ] {
        let directory = tempfile::tempdir().expect("create contradiction fixture directory");
        let path = released_project_store(&directory, 35);
        seed_released_project_store(&path, 35);
        tamper(&path, mutation);
        let before = store_snapshot(&path);

        let connection = TestConnection::open(&path);
        let error = ensure_schema_current_connection(&connection)
            .await
            .expect_err("contradictory external-source data must fail migration");
        assert!(
            matches!(
                &error,
                tracedecay_domain::errors::TraceDecayError::Database { .. }
            ),
            "{label} must be a database failure: {error}"
        );
        drop(connection);
        assert_eq!(
            store_snapshot(&path),
            before,
            "{label} failure must roll back every schema and data write"
        );
        assert_eq!(
            object_sql(&path, "table", "external_source_objects_v1").is_some(),
            true,
            "{label} failure must leave the released source tables in place"
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
