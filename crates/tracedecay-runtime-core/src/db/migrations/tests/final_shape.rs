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

const RELEASED_V35_PROJECT_STORE_SQL: &str =
    include_str!("../../../../tests/fixtures/project-store-released-v35-semantic.sql");

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

/// Writes the immutable v35 snapshot used by the source-shape admission path.
/// This is the store shape after the payload-digest step and before v36 retires
/// semantic-vector staging; it deliberately does not reuse the current schema
/// installers.
fn released_v35_current_project_store(directory: &TempDir) -> PathBuf {
    let path = directory.path().join("released-v35-current.db");
    let connection =
        rusqlite::Connection::open(&path).expect("create released v35 current fixture");
    connection
        .pragma_update(None, "journal_mode", "WAL")
        .expect("run the released v35 current store in WAL mode");
    connection
        .execute_batch(RELEASED_V35_PROJECT_STORE_SQL)
        .expect("install the immutable released v35 current project schema");
    connection
        .execute_batch("PRAGMA user_version = 35;")
        .expect("stamp the released v35 current schema");
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
                 1, 2, 3, 4, 5, NULL, 7
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
             INSERT INTO external_source_objects_v1(
                 binding_id, native_object_digest, partition_digest,
                 mutation_digest, mutation_json
             ) VALUES(
                 'binding-1', 'sha256:object-2', 'sha256:partition-2',
                 'sha256:mutation-2', '{\"mutation_digest\":\"sha256:mutation-2\"}'
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
                 '{\"idempotency_key\":\"commit-1\",\"request_digest\":\"sha256:request\",\"definition_revision\":1,\"definition_digest\":\"sha256:definition\",\"binding_revision\":1,\"binding_digest\":\"sha256:binding\",\"source_frontier\":{\"digest\":\"sha256:source\",\"n\":1},\"prior_source_frontier\":null,\"mutations\":[{\"mutation_digest\":\"sha256:mutation\"}],\"lineage\":[{\"lineage_digest\":\"sha256:lineage\",\"kind\":\"lineage\"}],\"snapshot_completion\":null,\"receipt_digest\":\"sha256:receipt\"}'
             );
             INSERT INTO external_source_commit_receipts_v1(
                 binding_id, idempotency_key, request_digest, definition_revision,
                 binding_revision, predecessor_frontier_digest,
                 successor_frontier_digest, receipt_digest, receipt_json
             ) VALUES(
                 'binding-1', 'commit-2', 'sha256:request-2', 1, 1,
                 'sha256:source', 'sha256:projection', 'sha256:receipt-2',
                 '{\"idempotency_key\":\"commit-2\",\"request_digest\":\"sha256:request-2\",\"definition_revision\":1,\"definition_digest\":\"sha256:definition\",\"binding_revision\":1,\"binding_digest\":\"sha256:binding\",\"source_frontier\":{\"digest\":\"sha256:projection\",\"n\":2},\"prior_source_frontier\":{\"digest\":\"sha256:source\",\"n\":1},\"mutations\":[{\"mutation_digest\":\"sha256:mutation\"},{\"mutation_digest\":\"sha256:mutation-2\"}],\"lineage\":[],\"snapshot_completion\":null,\"receipt_digest\":\"sha256:receipt-2\"}'
             );
             INSERT INTO external_source_mutations_v1(
                 binding_id, mutation_digest, native_object_digest,
                 revision_digest, source_receipt_digest, mutation_json
             ) VALUES(
                 'binding-1', 'sha256:mutation', 'sha256:object',
                 'sha256:revision', 'sha256:receipt',
                 '{\"mutation_digest\":\"sha256:mutation\"}'
             );
             INSERT INTO external_source_mutations_v1(
                 binding_id, mutation_digest, native_object_digest,
                 revision_digest, source_receipt_digest, mutation_json
             ) VALUES(
                 'binding-1', 'sha256:mutation-2', 'sha256:object-2',
                 'sha256:revision-2', 'sha256:receipt-2',
                 '{\"mutation_digest\":\"sha256:mutation-2\"}'
             );
             INSERT INTO external_source_projection_publications_v1(
                 binding_id, projection_digest, source_receipt_digest,
                 predecessor_frontier_digest, successor_frontier_digest, receipt_json
             ) VALUES(
                 'binding-1', 'sha256:projection', 'sha256:receipt',
                 'root', 'sha256:source',
                 '{\"projector\":{\"name\":\"test\"},\"definition_revision\":1,\"definition_digest\":\"sha256:definition\",\"binding_revision\":1,\"binding_digest\":\"sha256:binding\",\"expected_projection_frontier\":null,\"source_frontier\":{\"digest\":\"sha256:source\",\"n\":1},\"source_receipt_digest\":\"sha256:receipt\",\"mutations\":[{\"mutation_digest\":\"sha256:mutation\"}],\"effects\":[{\"kind\":\"effect\"}],\"lineage\":[{\"lineage_digest\":\"sha256:projection-lineage\",\"kind\":\"projection-lineage\"}],\"receipt_digest\":\"sha256:projection\"}'
             );",
        )
        .expect("seed released project content");
    connection
        .execute_batch(
            r#"
            INSERT INTO retrieval_anchor_dispositions(
                disposition_id, anchor_id, owner_json, state, superseded_by,
                reason_class, effective_at, record_json
            ) VALUES(
                'disposition-migration', 'anchor-migration',
                '{"kind":"project","project_id":"project-test"}',
                'redacted', NULL, 'retention', 12, '{"state":"redacted"}'
            );
            INSERT INTO retrieval_anchor_reverse_lineage(
                source_anchor_id, owner_json, derivative_kind, derivative_id,
                direct_evidence
            ) VALUES
                (
                    'anchor-migration',
                    '{"kind":"project","project_id":"project-test"}',
                    'span', 'span-migration', 1
                ),
                (
                    'anchor-migration',
                    '{"kind":"project","project_id":"project-test"}',
                    'contribution', 'contribution-migration', 1
                );
            INSERT INTO retrieval_anchor_derivative_tombstones(
                source_anchor_id, owner_json, derivative_kind, derivative_id,
                disposition_id, effective_at
            ) VALUES(
                'anchor-migration',
                '{"kind":"project","project_id":"project-test"}',
                'span', 'span-migration', 'disposition-migration', 13
            );

            INSERT INTO memory_v2_assertions(
                assertion_id, fact_id, owner_kind, project_id, owner_json,
                assertion_header_json, kind_json, payload_reference_json,
                receipt_json, asserted_at, actor_id
            ) VALUES(
                'assertion-replacement', 'fact-migration', 'project',
                'project-test',
                '{"kind":"project","project_id":"project-test"}',
                '{"revision":2}', '{"kind":"correction"}',
                '{"kind":"payload"}', '{}', 5, NULL
            );
            INSERT INTO memory_v2_assertion_supersession(
                assertion_id, fact_id, owner_kind, project_id,
                superseded_assertion_id, ordinal
            ) VALUES(
                'assertion-replacement', 'fact-migration', 'project',
                'project-test', 'assertion-migration', 0
            );
            INSERT INTO memory_v2_assertion_payload_purges(
                assertion_id, fact_id, owner_kind, project_id,
                payload_reference_json, detector_revision, purge_reason
            ) VALUES(
                'assertion-migration', 'fact-migration', 'project',
                'project-test', '{"redacted":true}', 'detector-1',
                'detector_flagged'
            );
            INSERT INTO memory_v2_lineage_events(
                event_id, fact_id, owner_kind, project_id, event_json,
                occurred_at, recorded_at
            ) VALUES
                (
                    'event-feedback', 'fact-migration', 'project',
                    'project-test', '{"kind":"feedback"}', 6, 6
                ),
                (
                    'event-automatic', 'fact-migration', 'project',
                    'project-test', '{"kind":"automatic"}', 7, 7
                );
            INSERT INTO memory_v2_evidence(
                evidence_id, fact_id, owner_kind, project_id, owner_json,
                anchor_id, evidence_json
            ) VALUES(
                'evidence-migration', 'fact-migration', 'project',
                'project-test',
                '{"kind":"project","project_id":"project-test"}',
                'anchor-migration', '{"kind":"source"}'
            );
            INSERT INTO memory_v2_assertion_evidence(
                assertion_id, evidence_id, fact_id, owner_kind, project_id,
                ordinal
            ) VALUES(
                'assertion-migration', 'evidence-migration', 'fact-migration',
                'project', 'project-test', 0
            );
            INSERT INTO memory_v2_operation_receipts(
                owner_kind, project_id, operation_id, operation_kind,
                request_digest, fact_id, event_id, receipt_json, recorded_at
            ) VALUES(
                'project', 'project-test', 'operation-migration', 'retrieval',
                'sha256:operation', 'fact-migration', 'event-migration',
                '{"operation_id":"operation-migration","automation_run_id":"run-migration"}',
                8
            );
            INSERT INTO memory_v2_feedback_history(
                owner_kind, project_id, fact_id, event_id, action, old_trust,
                new_trust, occurred_at, source, note, details_availability
            ) VALUES(
                'project', 'project-test', 'fact-migration', 'event-feedback',
                'helpful', 0.7, 0.8, 6, 'test', 'kept', 'available'
            );
            INSERT INTO memory_v2_automatic_fact_receipts(
                apply_id, owner_kind, project_id, owner_json, idempotency_key,
                request_digest, request_json, evidence_json, state,
                quarantine_reason, applied_fact_id, applied_assertion_id,
                applied_event_id, recorded_at
            ) VALUES(
                'apply-migration', 'project', 'project-test',
                '{"kind":"project","project_id":"project-test"}',
                'apply-key', 'sha256:apply',
                '{"automation_run_id":"run-migration"}',
                '{"source":"test"}', 'applied', NULL, 'fact-migration',
                'assertion-replacement', 'event-automatic', 9
            );

            INSERT INTO evidence_source_occurrences(
                occurrence_id, owner_digest, timeline_digest, source_anchor_id,
                source_order, record_digest, record_json
            ) VALUES(
                'occurrence-migration', 'sha256:owner', 'sha256:timeline',
                'anchor-migration', 0, 'sha256:occurrence', '{}'
            );
            INSERT INTO evidence_occurrence_sets(
                occurrence_set_id, owner_digest, record_digest, record_json
            ) VALUES('set-migration', 'sha256:owner', 'sha256:set', '{}');
            INSERT INTO evidence_occurrence_set_members(
                occurrence_set_id, canonical_ordinal, occurrence_id
            ) VALUES('set-migration', 0, 'occurrence-migration');
            INSERT INTO evidence_spans(
                span_id, owner_digest, occurrence_set_id, anchor_id,
                producer_kind, record_digest, record_json
            ) VALUES(
                'span-migration', 'sha256:owner', 'set-migration',
                'evidence-span-anchor', 'test', 'sha256:span', '{}'
            );
            INSERT INTO evidence_span_members(
                span_id, assembly_ordinal, run_ordinal, run_member_ordinal,
                occurrence_id
            ) VALUES('span-migration', 0, 0, 0, 'occurrence-migration');
            INSERT INTO evidence_span_projection_receipts(
                projection_receipt_id, span_id, record_digest, record_json
            ) VALUES(
                'projection-receipt-migration', 'span-migration',
                'sha256:projection-receipt', '{}'
            );
            INSERT INTO evidence_retriever_contributions(
                contribution_id, owner_digest, span_id, anchor_id,
                record_digest, record_json
            ) VALUES(
                'contribution-migration', 'sha256:owner', 'span-migration',
                'contribution-anchor', 'sha256:contribution', '{}'
            );
            INSERT INTO evidence_derived_anchors(
                anchor_id, owner_digest, target_kind, target_id, anchor_json
            ) VALUES(
                'derived-anchor-migration', 'sha256:owner', 'evidence_span',
                'span-migration', '{}'
            );
            INSERT INTO evidence_assembly_receipts(
                publication_receipt_id, owner_digest, privacy_domain_id,
                key_epoch, idempotency_key, assembly_digest, occurrence_set_id,
                span_id, contribution_id, projection_receipt_id, receipt_json
            ) VALUES(
                'publication-migration', 'sha256:owner', 'privacy', 1,
                'assembly-key', 'sha256:assembly', 'set-migration',
                'span-migration', 'contribution-migration',
                'projection-receipt-migration', '{}'
            );

            INSERT INTO graph_publication_replay_v1(
                shard_id, namespace, projection, generation, idempotency_key,
                input_digest, dependency_generation_closure_digest,
                direct_dependency_bytes, expected_prior_head,
                expected_recovered_digest, canonical_replay_source_digest,
                canonical_replay_source
            ) VALUES(
                'shard-migration', 'namespace', 'projection', 'generation-2',
                'idempotency-2', 'input-2', 'closure-2', 2, 'recovered-1',
                'recovered-2', 'source-2', X'0203'
            );
            INSERT INTO graph_publication_replay_dependencies_v1(
                owner_replay_sequence, ordinal, dependency_replay_sequence,
                shard_id, namespace, projection, generation
            ) VALUES(
                2, 0, 1, 'shard-migration', 'namespace', 'projection',
                'generation-1'
            );
            INSERT INTO graph_publication_replay_tombstones_v1(
                replay_sequence, shard_id, namespace, projection, generation,
                idempotency_key, input_digest,
                dependency_generation_closure_digest, direct_dependency_bytes,
                expected_prior_head, expected_recovered_digest,
                canonical_replay_source_digest
            ) VALUES(
                90, 'shard-migration', 'namespace', 'projection',
                'generation-tombstone', 'idempotency-tombstone',
                'input-tombstone', 'closure-tombstone', 2, 'recovered-2',
                'recovered-tombstone', 'source-tombstone'
            );
            INSERT INTO graph_publication_replay_tombstone_dependencies_v1(
                tombstone_replay_sequence, ordinal, shard_id, namespace,
                projection, generation
            ) VALUES(
                90, 0, 'shard-migration', 'namespace', 'projection',
                'generation-2'
            );
            INSERT INTO graph_verified_heads_v1(
                shard_id, namespace, projection, replay_sequence,
                recovered_digest
            ) VALUES(
                'shard-migration', 'namespace', 'projection', 2, 'recovered-2'
            );

            INSERT INTO external_source_definition_revisions_v1(
                source_id, definition_revision, definition_digest, definition_json
            ) VALUES(
                'source-1', 1, 'sha256:definition',
                '{"kind":"definition","revision":1}'
            );
            INSERT INTO external_source_binding_revisions_v1(
                binding_id, binding_revision, definition_revision, binding_digest,
                binding_json
            ) VALUES(
                'binding-1', 1, 1, 'sha256:binding',
                '{"kind":"binding","revision":1}'
            );
            INSERT INTO external_source_authority_receipts_v1(
                binding_id, idempotency_key, request_digest, definition_digest,
                binding_digest, receipt_json
            ) VALUES(
                'binding-1', 'authority-1', 'sha256:authority-request',
                'sha256:definition', 'sha256:binding',
                '{"idempotency_key":"authority-1","request_digest":"sha256:authority-request","prior_definition_digest":"sha256:prior-definition","prior_binding_digest":"sha256:prior-binding","definition_digest":"sha256:definition","binding_digest":"sha256:binding"}'
            );
            INSERT INTO external_source_lineage_v1(
                binding_id, lineage_digest, source_receipt_digest, lineage_json
            ) VALUES(
                'binding-1', 'sha256:lineage', 'sha256:receipt',
                '{"lineage_digest":"sha256:lineage","kind":"lineage"}'
            );
            INSERT INTO external_source_pending_projections_v1(
                binding_id, predecessor_frontier_digest,
                successor_frontier_digest, successor_sequence,
                source_receipt_digest
             ) VALUES(
                 'binding-1', 'sha256:source', 'sha256:projection', 2,
                 'sha256:receipt-2'
             );
            INSERT INTO external_source_projection_lineage_v1(
                binding_id, projection_digest, lineage_index, lineage_digest,
                lineage_json
            ) VALUES(
                'binding-1', 'sha256:projection', 0,
                'sha256:projection-lineage',
                '{"lineage_digest":"sha256:projection-lineage","kind":"projection-lineage"}'
            );
            INSERT INTO external_source_acquisition_queue_v1(
                binding_id, state_digest, not_before_micros, state_json
            ) VALUES(
                'binding-1', 'sha256:acquisition', NULL, '{"state":"ready"}'
            );
            INSERT INTO external_source_states_v1(
                binding_id, source_id, owner_kind, owner_id, definition_revision,
                definition_digest, binding_revision, binding_digest,
                source_frontier_digest, source_frontier_json,
                projection_frontier_digest, latest_source_receipt_digest,
                latest_projection_receipt_digest
            ) VALUES(
                'binding-1', 'source-1', 'project', 'project-test', 1,
                'sha256:definition', 1, 'sha256:binding', 'sha256:projection',
                '{"digest":"sha256:projection","n":2}', 'sha256:source',
                'sha256:receipt-2', 'sha256:projection'
            );

            INSERT INTO td_runtime_writer_checkpoint_v1(
                shard_json, incarnation, authority_epoch, commit_sequence,
                watermark_json, transaction_scope_json, original_receipt_json,
                operation_id, durability_json, committed_at_micros
            ) VALUES(
                '{"shard":"migration"}', 1, 2, 3,
                '{"watermark":null}', '{"scope":"project"}',
                '{"receipt":"checkpoint"}', 'checkpoint-migration',
                '{"durability":"sync"}', 10
            );
            INSERT INTO td_runtime_writer_idempotency_v1(
                shard_json, incarnation, authority_epoch, idempotency_key,
                request_digest, original_receipt_json, transaction_scope_json,
                operation_id, durability_json, committed_at_micros
            ) VALUES(
                '{"shard":"migration"}', 1, 2, 'idempotency-migration',
                'sha256:runtime-request', '{"receipt":"idempotency"}',
                '{"scope":"project"}', 'idempotency-migration',
                '{"durability":"sync"}', 11
            );
            INSERT INTO td_runtime_writer_outbox_v1(
                source_shard_json, source_incarnation, source_authority_epoch,
                effect_id, ordering_key, source_sequence, state, entry_json,
                source_receipt_json, transaction_scope_json, operation_id,
                durability_json, updated_at_micros
            ) VALUES(
                '{"shard":"migration"}', 1, 2, 'effect-migration', 'order-1',
                0, 'effect_unknown', '{"effect":true}',
                '{"receipt":"source"}', '{"scope":"project"}',
                'outbox-migration', '{"durability":"sync"}', 12
            );
            INSERT INTO td_runtime_writer_inbox_v1(
                target_shard_json, target_incarnation, target_authority_epoch,
                effect_id, ordering_key, source_sequence, target_sequence,
                identity_json, receipt_json, committed_at_micros
            ) VALUES(
                '{"shard":"migration"}', 1, 2, 'effect-inbox-migration',
                'order-1', 0, 1, '{"identity":"inbox"}',
                '{"receipt":"target"}', 13
            );
            INSERT INTO handoff_open_grants_v1(
                token_digest, issued_request_id, grant_payload, issued_at,
                expires_at, consumed_request_id, consumed_input_digest,
                consumption_payload
            ) VALUES(
                'sha256:handoff-token', 'handoff-request', '{"kind":"grant"}',
                1, 2, NULL, NULL, NULL
            );
            "#,
        )
        .expect("seed retained families in released project store");
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
        assert_eq!(
            migrated
                .query_row(
                    "SELECT state FROM retrieval_anchor_dispositions
                     WHERE disposition_id = 'disposition-migration'",
                    (),
                    |row| row.get::<_, String>(0),
                )
                .expect("retrieval disposition survives migration"),
            "redacted"
        );
        assert_eq!(
            migrated
                .query_row(
                    "SELECT effective_at FROM retrieval_anchor_derivative_tombstones
                     WHERE derivative_id = 'span-migration'",
                    (),
                    |row| row.get::<_, i64>(0),
                )
                .expect("retrieval tombstone survives migration"),
            13
        );
        assert_eq!(
            migrated
                .query_row(
                    "SELECT count(*) FROM retrieval_anchor_reverse_lineage
                     WHERE source_anchor_id = 'anchor-migration'",
                    (),
                    |row| row.get::<_, i64>(0),
                )
                .expect("retrieval lineage survives migration"),
            3
        );
        assert_eq!(
            migrated
                .query_row(
                    "SELECT count(*) FROM memory_v2_assertions
                     WHERE fact_id = 'fact-migration'",
                    (),
                    |row| row.get::<_, i64>(0),
                )
                .expect("all memory assertions survive migration"),
            2
        );
        assert_eq!(
            migrated
                .query_row(
                    "SELECT last_recalled_at FROM memory_v2_current_facts
                     WHERE fact_id = 'fact-migration'",
                    (),
                    |row| row.get::<_, Option<i64>>(0),
                )
                .expect("nullable current-fact timestamp survives migration"),
            None
        );
        assert_eq!(
            migrated
                .query_row(
                    "SELECT retrieval_count, access_count, helpful_count, unhelpful_count,
                            last_retrieved_at, last_feedback_at
                     FROM memory_v2_current_facts
                     WHERE fact_id = 'fact-migration'",
                    (),
                    |row| {
                        Ok((
                            row.get::<_, i64>(0)?,
                            row.get::<_, i64>(1)?,
                            row.get::<_, i64>(2)?,
                            row.get::<_, i64>(3)?,
                            row.get::<_, Option<i64>>(4)?,
                            row.get::<_, Option<i64>>(5)?,
                        ))
                    },
                )
                .expect("current-fact counters survive migration"),
            (1, 2, 3, 4, Some(5), Some(7))
        );
        assert_eq!(
            migrated
                .query_row(
                    "SELECT superseded_assertion_id FROM memory_v2_assertion_supersession
                     WHERE assertion_id = 'assertion-replacement'",
                    (),
                    |row| row.get::<_, String>(0),
                )
                .expect("assertion supersession survives migration"),
            "assertion-migration"
        );
        assert_eq!(
            migrated
                .query_row(
                    "SELECT detector_revision FROM memory_v2_assertion_payload_purges
                     WHERE assertion_id = 'assertion-migration'",
                    (),
                    |row| row.get::<_, String>(0),
                )
                .expect("payload purge receipt survives migration"),
            "detector-1"
        );
        assert_eq!(
            migrated
                .query_row(
                    "SELECT count(*) FROM memory_v2_evidence
                     WHERE evidence_id = 'evidence-migration'",
                    (),
                    |row| row.get::<_, i64>(0),
                )
                .expect("memory evidence survives migration"),
            1
        );
        assert_eq!(
            migrated
                .query_row(
                    "SELECT count(*) FROM memory_v2_assertion_evidence
                     WHERE assertion_id = 'assertion-migration'",
                    (),
                    |row| row.get::<_, i64>(0),
                )
                .expect("assertion evidence survives migration"),
            1
        );
        assert_eq!(
            migrated
                .query_row(
                    "SELECT receipt_json FROM memory_v2_operation_receipts
                     WHERE operation_id = 'operation-migration'",
                    (),
                    |row| row.get::<_, String>(0),
                )
                .expect("memory operation receipt survives migration"),
            "{\"operation_id\":\"operation-migration\",\"automation_run_id\":\"run-migration\"}"
        );
        assert_eq!(
            migrated
                .query_row(
                    "SELECT new_trust FROM memory_v2_feedback_history
                     WHERE event_id = 'event-feedback'",
                    (),
                    |row| row.get::<_, f64>(0),
                )
                .expect("feedback history survives migration"),
            0.8
        );
        assert_eq!(
            migrated
                .query_row(
                    "SELECT applied_assertion_id FROM memory_v2_automatic_fact_receipts
                     WHERE apply_id = 'apply-migration'",
                    (),
                    |row| row.get::<_, String>(0),
                )
                .expect("automatic fact receipt survives migration"),
            "assertion-replacement"
        );
        assert_eq!(
            migrated
                .query_row(
                    "SELECT count(*) FROM evidence_assembly_receipts
                     WHERE publication_receipt_id = 'publication-migration'",
                    (),
                    |row| row.get::<_, i64>(0),
                )
                .expect("evidence assembly receipt survives migration"),
            1
        );
        assert_eq!(
            migrated
                .query_row(
                    "SELECT count(*) FROM evidence_span_members
                     WHERE span_id = 'span-migration'",
                    (),
                    |row| row.get::<_, i64>(0),
                )
                .expect("evidence span membership survives migration"),
            1
        );
        assert_eq!(
            migrated
                .query_row(
                    "SELECT sequence FROM graph_publication_replay_v1
                     WHERE generation = 'generation-2'",
                    (),
                    |row| row.get::<_, i64>(0),
                )
                .expect("graph replay sequence survives migration"),
            2
        );
        assert_eq!(
            migrated
                .query_row(
                    "SELECT canonical_replay_source FROM graph_publication_replay_v1
                     WHERE generation = 'generation-2'",
                    (),
                    |row| row.get::<_, Vec<u8>>(0),
                )
                .expect("graph replay bytes survive migration"),
            vec![2, 3]
        );
        assert_eq!(
            migrated
                .query_row(
                    "SELECT dependency_replay_sequence
                     FROM graph_publication_replay_dependencies_v1
                     WHERE owner_replay_sequence = 2",
                    (),
                    |row| row.get::<_, i64>(0),
                )
                .expect("graph replay dependency survives migration"),
            1
        );
        assert_eq!(
            migrated
                .query_row(
                    "SELECT count(*) FROM graph_publication_replay_tombstones_v1
                     WHERE replay_sequence = 90",
                    (),
                    |row| row.get::<_, i64>(0),
                )
                .expect("graph replay tombstone survives migration"),
            1
        );
        assert_eq!(
            migrated
                .query_row(
                    "SELECT count(*) FROM graph_publication_replay_tombstone_dependencies_v1
                     WHERE tombstone_replay_sequence = 90",
                    (),
                    |row| row.get::<_, i64>(0),
                )
                .expect("graph tombstone dependency survives migration"),
            1
        );
        assert_eq!(
            migrated
                .query_row(
                    "SELECT recovered_digest FROM graph_verified_heads_v1
                     WHERE shard_id = 'shard-migration'",
                    (),
                    |row| row.get::<_, String>(0),
                )
                .expect("graph verified head survives migration"),
            "recovered-2"
        );
        assert_eq!(
            migrated
                .query_row(
                    "SELECT count(*) FROM external_source_frontiers_v1
                     WHERE binding_id = 'binding-1'",
                    (),
                    |row| row.get::<_, i64>(0),
                )
                .expect("external frontier history is materialized"),
            2
        );
        assert_eq!(
            migrated
                .query_row(
                    "SELECT json_extract(frontier_json, '$.n')
                     FROM external_source_frontiers_v1
                     WHERE binding_id = 'binding-1' AND frontier_digest = 'sha256:source'",
                    (),
                    |row| row.get::<_, i64>(0),
                )
                .expect("source frontier payload survives conversion"),
            1
        );
        assert_eq!(
            migrated
                .query_row(
                    "SELECT json_extract(receipt_json, '$.mutations[0]')
                     FROM external_source_commit_receipts_v2
                     WHERE binding_id = 'binding-1'
                       AND receipt_digest = 'sha256:receipt'",
                    (),
                    |row| row.get::<_, String>(0),
                )
                .expect("commit mutation digest is retained by reference"),
            "sha256:mutation"
        );
        assert_eq!(
            migrated
                .query_row(
                    "SELECT json_type(receipt_json, '$.prior_source_frontier')
                     FROM external_source_commit_receipts_v2
                     WHERE binding_id = 'binding-1'
                       AND receipt_digest = 'sha256:receipt'",
                    (),
                    |row| row.get::<_, String>(0),
                )
                .expect("null prior frontier survives conversion"),
            "null"
        );
        assert_eq!(
            migrated
                .query_row(
                    "SELECT mutation_digest FROM external_source_projected_objects_v2
                     WHERE binding_id = 'binding-1' AND native_object_digest = 'sha256:object'",
                    (),
                    |row| row.get::<_, String>(0),
                )
                .expect("projected mutation digest is retained by reference"),
            "sha256:mutation"
        );
        assert_eq!(
            migrated
                .query_row(
                    "SELECT mutation_digest FROM external_source_projection_effects_v2
                     WHERE binding_id = 'binding-1'
                       AND projection_digest = 'sha256:projection'
                       AND effect_index = 0",
                    (),
                    |row| row.get::<_, String>(0),
                )
                .expect("projection effect digest is retained by reference"),
            "sha256:mutation"
        );
        assert_eq!(
            migrated
                .query_row(
                    "SELECT json_array_length(receipt_json, '$.effects')
                     FROM external_source_projection_publications_v2
                     WHERE binding_id = 'binding-1'",
                    (),
                    |row| row.get::<_, i64>(0),
                )
                .expect("projection effects are represented by the effect table"),
            0
        );
        assert_eq!(
            migrated
                .query_row(
                    "SELECT count(*) FROM external_source_definition_revisions_v1
                     WHERE source_id = 'source-1'",
                    (),
                    |row| row.get::<_, i64>(0),
                )
                .expect("source definition revision survives migration"),
            1
        );
        assert_eq!(
            migrated
                .query_row(
                    "SELECT count(*) FROM external_source_binding_revisions_v1
                     WHERE binding_id = 'binding-1'",
                    (),
                    |row| row.get::<_, i64>(0),
                )
                .expect("source binding revision survives migration"),
            1
        );
        assert_eq!(
            migrated
                .query_row(
                    "SELECT count(*) FROM external_source_authority_receipts_v1
                     WHERE binding_id = 'binding-1'",
                    (),
                    |row| row.get::<_, i64>(0),
                )
                .expect("source authority receipt survives migration"),
            1
        );
        assert_eq!(
            migrated
                .query_row(
                    "SELECT count(*) FROM external_source_lineage_v1
                     WHERE binding_id = 'binding-1'",
                    (),
                    |row| row.get::<_, i64>(0),
                )
                .expect("source lineage survives migration"),
            1
        );
        assert_eq!(
            migrated
                .query_row(
                    "SELECT successor_sequence FROM external_source_pending_projections_v1
                     WHERE binding_id = 'binding-1'",
                    (),
                    |row| row.get::<_, i64>(0),
                )
                .expect("pending projection survives migration"),
            2
        );
        assert_eq!(
            migrated
                .query_row(
                    "SELECT count(*) FROM external_source_projection_lineage_v1
                     WHERE binding_id = 'binding-1'",
                    (),
                    |row| row.get::<_, i64>(0),
                )
                .expect("projection lineage survives migration"),
            1
        );
        assert_eq!(
            migrated
                .query_row(
                    "SELECT not_before_micros FROM external_source_acquisition_queue_v1
                     WHERE binding_id = 'binding-1'",
                    (),
                    |row| row.get::<_, Option<i64>>(0),
                )
                .expect("null acquisition schedule survives migration"),
            None
        );
        assert_eq!(
            migrated
                .query_row(
                    "SELECT projection_frontier_digest FROM external_source_states_v1
                     WHERE binding_id = 'binding-1'",
                    (),
                    |row| row.get::<_, Option<String>>(0),
                )
                .expect("source state survives migration"),
            Some("sha256:source".to_owned())
        );
        assert_eq!(
            migrated
                .query_row(
                    "SELECT commit_sequence FROM td_runtime_writer_checkpoint_v1
                     WHERE shard_json = '{\"shard\":\"migration\"}'",
                    (),
                    |row| row.get::<_, i64>(0),
                )
                .expect("runtime checkpoint survives migration"),
            3
        );
        assert_eq!(
            migrated
                .query_row(
                    "SELECT request_digest FROM td_runtime_writer_idempotency_v2
                     WHERE operation_id = 'idempotency-migration'",
                    (),
                    |row| row.get::<_, String>(0),
                )
                .expect("runtime idempotency row is migrated"),
            "sha256:runtime-request"
        );
        assert_eq!(
            migrated
                .query_row(
                    "SELECT state FROM td_runtime_writer_outbox_v1
                     WHERE effect_id = 'effect-migration'",
                    (),
                    |row| row.get::<_, String>(0),
                )
                .expect("runtime outbox row survives migration"),
            "effect_unknown"
        );
        assert_eq!(
            migrated
                .query_row(
                    "SELECT target_sequence FROM td_runtime_writer_inbox_v1
                     WHERE effect_id = 'effect-inbox-migration'",
                    (),
                    |row| row.get::<_, i64>(0),
                )
                .expect("runtime inbox row survives migration"),
            1
        );
        assert_eq!(
            migrated
                .query_row(
                    "SELECT consumed_request_id FROM handoff_open_grants_v1
                     WHERE token_digest = 'sha256:handoff-token'",
                    (),
                    |row| row.get::<_, Option<String>>(0),
                )
                .expect("null handoff consumption survives migration"),
            None
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

/// The immutable v35 snapshot is admitted independently of the current
/// installers, and its empty retired staging family is removed atomically.
#[tokio::test]
async fn released_v35_current_snapshot_migrates_to_the_final_shape() {
    let directory = tempfile::tempdir().expect("create released v35 current directory");
    let path = released_v35_current_project_store(&directory);
    assert!(object_sql(&path, "table", "semantic_vector_stages").is_some());
    let connection = TestConnection::open(&path);
    ensure_schema_current_connection(&connection)
        .await
        .expect("immutable released v35 current snapshot migrates");
    drop(connection);

    let migrated = rusqlite::Connection::open(&path).expect("open migrated v35 current store");
    assert_eq!(
        migrated
            .query_row("PRAGMA user_version", (), |row| row.get::<_, i64>(0))
            .expect("read migrated v35 current stamp"),
        i64::from(SCHEMA_VERSION)
    );
    assert!(object_sql(&path, "table", "semantic_vector_stages").is_none());
    let before = store_snapshot(&path);
    let connection = TestConnection::open(&path);
    ensure_schema_current_connection(&connection)
        .await
        .expect("migrated v35 current snapshot is idempotently admitted");
    drop(connection);
    assert_eq!(store_snapshot(&path), before);
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

/// A source payload digest that cannot be represented in the normalized
/// successor is a database failure, and the writer leaves the released store
/// untouched. The fixture temporarily replaces the immutable-digest trigger
/// only to construct this otherwise-unwriteable corrupted input.
#[tokio::test]
async fn released_invalid_payload_digest_is_rejected_without_mutation() {
    let directory = tempfile::tempdir().expect("create rollback fixture directory");
    let path = released_project_store(&directory, 35);
    seed_released_project_store(&path, 35);
    tamper(
        &path,
        "DROP TRIGGER memory_v2_assertion_payload_digests_no_update;
         UPDATE memory_v2_assertion_payload_digests
         SET content_digest = 'sha256:0000000000000000000000000000000000000000000000000000000000000000';",
    );
    // Reinstall the exact released trigger text so source-shape admission
    // reaches the corrupted row instead of reporting a fixture DDL drift.
    tamper(&path, RELEASED_V35_PAYLOAD_DIGESTS_SCHEMA);
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
        "invalid source data must leave the exact released store untouched"
    );
    assert!(object_sql(&path, "table", "semantic_vector_stages").is_some());
    assert!(object_sql(&path, "table", "external_source_objects_v1").is_some());
}

#[tokio::test]
async fn released_semantic_rows_are_reset_required_without_mutation() {
    let directory = tempfile::tempdir().expect("create semantic-row fixture directory");
    let path = released_project_store(&directory, 35);
    tamper(
        &path,
        "INSERT INTO semantic_vector_stages(
             shard_id, namespace, projection, build_id, plan_digest,
             semantic_generation_id, base_generation, publication_generation,
             publication_idempotency_key, source_scope, source_generation,
             source_dependency, source_manifest_digest, embedding_projection_digest,
             embedding_dimension, model_artifact_digest, projection_manifest_digest,
             privacy_domain_digest, privacy_key_epoch, expected_chunk_manifest_digest,
             expected_chunk_count, expected_prior_verified_head, writer_binding,
             code_scope_hash, plan_json, state, next_ordinal, checkpoint_digest,
             recorded_chunk_count, applied_ordinal, applied_receipt_digest,
             applied_checkpoint_digest, applied_graph_batch_digest,
             expected_recovered_digest, publication_intent_digest
         ) VALUES(
             'shard', 'namespace', 'projection', 'build', 'plan',
             'semantic-generation', NULL, 'publication', 'idempotency',
             'scope', 'source', '{}', 'source-manifest', 'embedding', 1,
             'model', 'projection-manifest', 'privacy', 1, 'chunk-manifest',
             0, NULL, '{}',
             '0000000000000000000000000000000000000000000000000000000000000000',
             '{}', 'pending', 0, 'checkpoint', 0, NULL, NULL, NULL, NULL, NULL, NULL
         );",
    );
    let reason = assert_reset_required_without_repair(&path, "retired semantic row").await;
    assert!(
        reason.contains("retired semantic") || reason.contains("semantic_vector"),
        "retired semantic data refusal must identify the unsupported table: {reason}"
    );
    assert_eq!(
        object_sql(&path, "table", "semantic_vector_stages").is_some(),
        true,
        "retired semantic data refusal must leave the source table in place"
    );
    tamper(
        &path,
        "DELETE FROM semantic_vector_stages;
         DELETE FROM semantic_vector_stage_census_authority;
         DELETE FROM semantic_vector_stage_adoption_authority;",
    );
    let reason = assert_reset_required_without_repair(&path, "retired semantic sequence").await;
    assert!(
        reason.contains("AUTOINCREMENT") || reason.contains("sequence"),
        "retired semantic sequence refusal must identify the unrecoverable high-water mark: {reason}"
    );
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
            "object nested mutation digest",
            "UPDATE external_source_objects_v1
             SET mutation_json = replace(
                 mutation_json,
                 'sha256:mutation',
                 'sha256:other'
             )
             WHERE native_object_digest = 'sha256:object';",
        ),
        (
            "authority receipt digest",
            "UPDATE external_source_authority_receipts_v1
             SET binding_digest = 'sha256:other';",
        ),
        (
            "commit successor frontier",
            "UPDATE external_source_commit_receipts_v1
             SET successor_frontier_digest = 'sha256:other'
             WHERE receipt_digest = 'sha256:receipt';",
        ),
        (
            "commit predecessor frontier",
            "UPDATE external_source_commit_receipts_v1
             SET predecessor_frontier_digest = 'sha256:other'
             WHERE receipt_digest = 'sha256:receipt';",
        ),
        (
            "commit receipt digest",
            "UPDATE external_source_commit_receipts_v1
             SET receipt_digest = 'sha256:other'
             WHERE receipt_digest = 'sha256:receipt';",
        ),
        (
            "projection source receipt",
            "UPDATE external_source_projection_publications_v1
             SET source_receipt_digest = 'sha256:other';",
        ),
        (
            "projection successor frontier",
            "UPDATE external_source_projection_publications_v1
             SET successor_frontier_digest = 'sha256:other';",
        ),
        (
            "projection receipt digest",
            "UPDATE external_source_projection_publications_v1
             SET projection_digest = 'sha256:other';",
        ),
        (
            "nested mutation digest",
            "UPDATE external_source_commit_receipts_v1
             SET receipt_json = replace(
                 receipt_json,
                 '\"mutation_digest\":\"sha256:mutation\"',
                 '\"mutation_digest\":null'
             )
             WHERE idempotency_key = 'commit-1';",
        ),
        (
            "receipt mutation payload",
            "UPDATE external_source_commit_receipts_v1
             SET receipt_json = replace(
                 receipt_json,
                 '{\"mutation_digest\":\"sha256:mutation\"}',
                 '{\"mutation_digest\":\"sha256:mutation\",\"extra\":\"lost\"}'
             )
             WHERE idempotency_key = 'commit-1';",
        ),
        (
            "projection effect history",
            "UPDATE external_source_projection_effects_v1
             SET effect_json = '{\"kind\":\"changed\"}';",
        ),
        (
            "missing projection effect history",
            "DELETE FROM external_source_projection_effects_v1;",
        ),
        (
            "missing projection history",
            "DELETE FROM external_source_projection_publications_v1;",
        ),
        (
            "missing frontier history",
            "UPDATE external_source_pending_projections_v1
             SET successor_frontier_digest = 'sha256:other';",
        ),
        (
            "source-state frontier payload",
            "UPDATE external_source_states_v1
             SET source_frontier_json = '{\"digest\":\"sha256:projection\",\"n\":3}';",
        ),
        (
            "frontier payload conflict",
            "UPDATE external_source_commit_receipts_v1
             SET receipt_json = replace(
                 receipt_json,
                 '{\"digest\":\"sha256:source\",\"n\":1}',
                 '{\"digest\":\"sha256:source\",\"n\":99}'
             )
             WHERE receipt_digest = 'sha256:receipt-2';",
        ),
        (
            "missing mutation history",
            "DELETE FROM external_source_mutations_v1;",
        ),
        (
            "mutation history not in receipt",
            "UPDATE external_source_commit_receipts_v1
             SET receipt_json = replace(
                 receipt_json,
                 '\"mutations\":[{\"mutation_digest\":\"sha256:mutation\"}]',
                 '\"mutations\":[]'
             )
             WHERE idempotency_key = 'commit-1';",
        ),
        (
            "source lineage not in receipt",
            "UPDATE external_source_commit_receipts_v1
             SET receipt_json = replace(
                 receipt_json,
                 '\"lineage\":[{\"lineage_digest\":\"sha256:lineage\",\"kind\":\"lineage\"}]',
                 '\"lineage\":[]'
             )
             WHERE receipt_digest = 'sha256:receipt';",
        ),
        (
            "projection lineage not in receipt",
            "UPDATE external_source_projection_publications_v1
             SET receipt_json = replace(
                 receipt_json,
                 '\"lineage\":[{\"lineage_digest\":\"sha256:projection-lineage\",\"kind\":\"projection-lineage\"}]',
                 '\"lineage\":[]'
             );",
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
