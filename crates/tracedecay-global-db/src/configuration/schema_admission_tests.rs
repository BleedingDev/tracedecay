use tracedecay_runtime_core::db::engine::{Executor, QueryExecutor, TransactionBehavior};

use super::{
    ConfigurationSchemaError, ensure_configuration_schema, fresh_configuration_store_evidence,
};

async fn final_connection() -> (
    tempfile::TempDir,
    tracedecay_runtime_core::db::engine::TestConnection,
) {
    let directory = tempfile::tempdir().unwrap();
    let connection = tracedecay_runtime_core::db::engine::TestConnection::open(
        &directory.path().join("configuration-admission.db"),
    );
    let fresh = fresh_configuration_store_evidence(&*connection)
        .await
        .unwrap()
        .expect("new database has fresh-store evidence");
    ensure_configuration_schema(&*connection, Some(&fresh))
        .await
        .unwrap();
    (directory, connection)
}

fn assert_reset_required(result: Result<(), ConfigurationSchemaError>) {
    assert!(matches!(
        result,
        Err(ConfigurationSchemaError::ResetRequired { .. })
    ));
}

#[tokio::test]
async fn same_named_table_with_extra_column_requires_reset() {
    let (_directory, connection) = final_connection().await;
    connection
        .execute_batch("ALTER TABLE configuration_entries ADD COLUMN malformed TEXT;")
        .await
        .unwrap();

    assert_reset_required(ensure_configuration_schema(&*connection, None).await);
}

#[tokio::test]
async fn same_named_index_with_wrong_columns_requires_reset() {
    let (_directory, connection) = final_connection().await;
    connection
        .execute_batch(
            "DROP INDEX idx_configuration_entry_key;
             CREATE INDEX idx_configuration_entry_key
                 ON configuration_entries(revision_id);",
        )
        .await
        .unwrap();

    assert_reset_required(ensure_configuration_schema(&*connection, None).await);
}

#[tokio::test]
async fn trigger_string_literal_case_is_part_of_the_exact_definition() {
    let (_directory, connection) = final_connection().await;
    connection
        .execute_batch(
            "DROP TRIGGER configuration_entries_immutable_update;
             CREATE TRIGGER configuration_entries_immutable_update
             BEFORE UPDATE ON configuration_entries
             BEGIN
                 SELECT RAISE(ABORT, 'Configuration entries are immutable');
             END;",
        )
        .await
        .unwrap();

    assert_reset_required(ensure_configuration_schema(&*connection, None).await);
}

#[tokio::test]
async fn arbitrary_named_index_attached_to_configuration_table_requires_reset() {
    let (_directory, connection) = final_connection().await;
    connection
        .execute_batch(
            "CREATE INDEX extra_entry_revision
                 ON configuration_entries(revision_id);",
        )
        .await
        .unwrap();

    assert_reset_required(ensure_configuration_schema(&*connection, None).await);
}

#[tokio::test]
async fn stale_fresh_evidence_cannot_create_configuration_schema() {
    let directory = tempfile::tempdir().unwrap();
    let connection = tracedecay_runtime_core::db::engine::TestConnection::open(
        &directory.path().join("stale-fresh.db"),
    );
    let fresh = fresh_configuration_store_evidence(&*connection)
        .await
        .unwrap()
        .expect("initially fresh");
    connection
        .execute_batch(
            "CREATE TABLE registered_identity (value TEXT NOT NULL);
             INSERT INTO registered_identity VALUES ('preserve-after-race');",
        )
        .await
        .unwrap();

    assert_reset_required(ensure_configuration_schema(&*connection, Some(&fresh)).await);
    let mut rows = connection
        .query("SELECT value FROM registered_identity", ())
        .await
        .unwrap();
    assert_eq!(
        rows.next()
            .await
            .unwrap()
            .unwrap()
            .get::<String>(0)
            .unwrap(),
        "preserve-after-race"
    );
}

#[tokio::test]
async fn non_fresh_store_missing_configuration_schema_is_unchanged() {
    let directory = tempfile::tempdir().unwrap();
    let connection = tracedecay_runtime_core::db::engine::TestConnection::open(
        &directory.path().join("registered.db"),
    );
    connection
        .execute_batch(
            "CREATE TABLE registered_identity (value TEXT NOT NULL);
             INSERT INTO registered_identity VALUES ('preserve-byte-state');",
        )
        .await
        .unwrap();
    assert!(
        fresh_configuration_store_evidence(&*connection)
            .await
            .unwrap()
            .is_none()
    );

    let before = sqlite_objects(&connection).await;
    assert_reset_required(ensure_configuration_schema(&*connection, None).await);
    assert_eq!(sqlite_objects(&connection).await, before);

    let mut rows = connection
        .query("SELECT value FROM registered_identity", ())
        .await
        .unwrap();
    assert_eq!(
        rows.next()
            .await
            .unwrap()
            .unwrap()
            .get::<String>(0)
            .unwrap(),
        "preserve-byte-state"
    );
}

async fn sqlite_objects(
    connection: &tracedecay_runtime_core::db::engine::TestConnection,
) -> Vec<(String, String, String)> {
    let mut rows = connection
        .query(
            "SELECT type, name, COALESCE(sql, '')
             FROM sqlite_master
             ORDER BY type, name",
            (),
        )
        .await
        .unwrap();
    let mut objects = Vec::new();
    while let Some(row) = rows.next().await.unwrap() {
        objects.push((
            row.get(0).unwrap(),
            row.get(1).unwrap(),
            row.get(2).unwrap(),
        ));
    }
    objects
}

/// Every release from beta.25 through beta.37 published the fixture's exact
/// shape. Read-only admission accepts an empty instance, then the writer
/// converges the retired empty tables while preserving supported rows.
const RELEASED_BETA37_CONFIGURATION_SQL: &str =
    include_str!("../../tests/fixtures/configuration-released-beta37.sql");
const RELEASED_BETA37_FULL_SNAPSHOT_SQL: &str =
    include_str!("../../tests/fixtures/configuration-released-beta37-full-snapshot.sql");

async fn released_schema_connection() -> (
    tempfile::TempDir,
    tracedecay_runtime_core::db::engine::TestConnection,
) {
    let directory = tempfile::tempdir().unwrap();
    let connection = tracedecay_runtime_core::db::engine::TestConnection::open(
        &directory.path().join("configuration-released.db"),
    );
    connection
        .execute_batch(RELEASED_BETA37_CONFIGURATION_SQL)
        .await
        .unwrap();
    (directory, connection)
}

async fn released_connection() -> (
    tempfile::TempDir,
    tracedecay_runtime_core::db::engine::TestConnection,
) {
    let (directory, connection) = released_schema_connection().await;
    connection
        .execute_batch(
            "INSERT INTO configuration_revisions VALUES
                ('revision.1', NULL, 'snapshot.1',
                 'sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa',
                 'sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb',
                 'actor.1', 'canonical_initialization', 1);
            INSERT INTO configuration_entries VALUES
                ('revision.1', 'analyzer.settings.v1', 'project', 'project.1', 1,
                 '{\"schema_version\":1,\"value\":{\"kind\":\"analyzer_settings\",\"value\":{\"schema_version\":1,\"selections\":[]}},\"provenance\":[{\"layer\":{\"kind\":\"default\"},\"revision_id\":\"configuration.registry.default.v1\",\"disposition\":\"defaulted\",\"safe_reason\":\"registry_default\"}]}');",
        )
        .await
        .unwrap();
    (directory, connection)
}

async fn released_full_snapshot_connection() -> (
    tempfile::TempDir,
    tracedecay_runtime_core::db::engine::TestConnection,
) {
    let (directory, connection) = released_schema_connection().await;
    connection
        .execute_batch(RELEASED_BETA37_FULL_SNAPSHOT_SQL)
        .await
        .unwrap();
    (directory, connection)
}

async fn prior_final_connection() -> (
    tempfile::TempDir,
    tracedecay_runtime_core::db::engine::TestConnection,
) {
    let (directory, connection) = released_connection().await;
    connection
        .execute_batch(
            "DROP TABLE configuration_credential_references;
             DROP TABLE configuration_semantic_retrieval_state_v1;
             DROP TABLE configuration_semantic_retrieval_pending_v1;
             DROP TABLE configuration_semantic_retrieval_inventory_v1;
             INSERT INTO configuration_semantic_accepted_profiles_v1 VALUES
                ('sha256:accepted', '{\"retired\":true}');",
        )
        .await
        .unwrap();
    (directory, connection)
}

async fn pre_residue_final_connection() -> (
    tempfile::TempDir,
    tracedecay_runtime_core::db::engine::TestConnection,
) {
    let (directory, connection) = released_connection().await;
    connection
        .execute_batch("DROP TABLE configuration_credential_references;")
        .await
        .unwrap();
    (directory, connection)
}

async fn count(connection: &impl QueryExecutor, sql: &str) -> i64 {
    let mut rows = connection.query(sql, ()).await.unwrap();
    rows.next().await.unwrap().unwrap().get::<i64>(0).unwrap()
}

const RELEASED_CONFIGURATION_RETIRED_TABLE_ROWS: &[(&str, &str)] = &[
    (
        "configuration_credential_references",
        r#"
            INSERT INTO configuration_credential_references VALUES
                ('credential.retired', 'api_token', 'sha256:ac', 'sha256:ad',
                 1, 'sha256:ae', 1, 1, 2, 0);
        "#,
    ),
    (
        "configuration_semantic_retrieval_state_v1",
        r#"
            INSERT INTO configuration_semantic_retrieval_state_v1 (
                project_id, scope_digest, scope_json, epoch, configuration_revision,
                transition_digest, activation_receipt_digest, active_vector_generation,
                rollback_vector_generation, state_json, activation_receipt_json
            ) VALUES (
                'project.state', 'scope.state',
                '{"project_id":"project.state","scope_digest":"scope.state"}',
                1, 'revision.1', NULL, NULL, NULL, NULL, '{}', NULL
            );
        "#,
    ),
    (
        "configuration_semantic_retrieval_pending_v1",
        r#"
            INSERT INTO configuration_semantic_retrieval_pending_v1 (
                project_id, scope_digest, scope_json, transition_digest, base_epoch,
                base_configuration_revision, transition_json, resulting_state_json, staged_at
            ) VALUES (
                'project.pending', 'scope.pending',
                '{"project_id":"project.pending","scope_digest":"scope.pending"}',
                'transition.pending', 0, 'revision.1', '{}', '{}', 1
            );
        "#,
    ),
    (
        "configuration_semantic_retrieval_inventory_v1",
        r#"
            INSERT INTO configuration_semantic_retrieval_inventory_v1
                (project_id, revision)
            VALUES ('project.inventory', 4);
        "#,
    ),
    (
        "configuration_semantic_accepted_profiles_v1",
        r#"
            INSERT INTO configuration_semantic_accepted_profiles_v1 VALUES
                ('sha256:accepted-profile', '{"profile":"retired"}');
        "#,
    ),
    (
        "configuration_semantic_accepted_profile_receipt_key_v1",
        r#"
            INSERT INTO configuration_semantic_accepted_profile_receipt_key_v1
                (singleton, key_material)
            VALUES (1, zeroblob(32));
        "#,
    ),
];

#[tokio::test]
async fn released_configuration_shape_is_admitted_and_converged_with_rows_intact() {
    let (_directory, connection) = released_connection().await;
    assert_eq!(
        count(
            &*connection,
            "SELECT COUNT(*) FROM sqlite_master WHERE name = 'configuration_credential_references'"
        )
        .await,
        1,
        "fixture carries the shipped table"
    );

    super::admit_configuration_schema(&*connection, None)
        .await
        .expect("the shipped beta.25-beta.37 shape is admissible read-only");
    ensure_configuration_schema(&*connection, None)
        .await
        .expect("the shipped shape converges instead of demanding a reset");

    assert_eq!(
        count(
            &*connection,
            "SELECT COUNT(*) FROM sqlite_master
             WHERE name LIKE 'configuration_credential_references%'
                OR name LIKE 'configuration_semantic_%'"
        )
        .await,
        0,
        "retired schema objects are gone"
    );

    let mut rows = connection
        .query(
            "SELECT typed_value FROM configuration_entries WHERE revision_id = 'revision.1'",
            (),
        )
        .await
        .unwrap();
    assert_eq!(
        rows.next()
            .await
            .unwrap()
            .unwrap()
            .get::<String>(0)
            .unwrap(),
        "{\"schema_version\":1,\"value\":{\"kind\":\"analyzer_settings\",\"value\":{\"schema_version\":1,\"selections\":[]}},\"provenance\":[{\"layer\":{\"kind\":\"default\"},\"revision_id\":\"configuration.registry.default.v1\",\"disposition\":\"defaulted\",\"safe_reason\":\"registry_default\"}]}"
    );
    drop(rows);
    ensure_configuration_schema(&*connection, None)
        .await
        .expect("the converged store is the exact current shape");
}

#[tokio::test]
async fn released_configuration_convergence_can_be_rolled_back_atomically() {
    let (_directory, connection) = released_connection().await;
    let transaction = connection
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .await
        .unwrap();
    ensure_configuration_schema(&transaction, None)
        .await
        .expect("the known released shape converges inside the transaction");
    transaction.rollback().await.unwrap();

    assert_eq!(
        count(
            &*connection,
            "SELECT COUNT(*) FROM sqlite_master
             WHERE name LIKE 'configuration_credential_references%'
                OR name LIKE 'configuration_semantic_%'"
        )
        .await,
        21,
        "rolling back retains every retired table and trigger"
    );
    assert_eq!(
        count(
            &*connection,
            "SELECT COUNT(*) FROM configuration_entries WHERE revision_id = 'revision.1'"
        )
        .await,
        1,
        "rolling back retains supported settings"
    );
}

#[tokio::test]
async fn prior_final_configuration_shape_requires_reset_without_mutation() {
    let (_directory, connection) = prior_final_connection().await;
    let before = sqlite_objects(&connection).await;
    assert_reset_required(super::admit_configuration_schema(&*connection, None).await);
    assert_reset_required(ensure_configuration_schema(&*connection, None).await);
    assert_eq!(sqlite_objects(&connection).await, before);

    assert_eq!(
        count(
            &*connection,
            "SELECT COUNT(*) FROM configuration_semantic_accepted_profiles_v1"
        )
        .await,
        1,
        "retired rows remain available for an explicit reset"
    );
    assert_eq!(
        count(
            &*connection,
            "SELECT COUNT(*) FROM configuration_entries WHERE revision_id = 'revision.1'"
        )
        .await,
        1,
        "supported configuration rows remain"
    );
}

#[tokio::test]
async fn pre_residue_final_configuration_shape_requires_reset_without_mutation() {
    let (_directory, connection) = pre_residue_final_connection().await;
    let before = sqlite_objects(&connection).await;
    assert_reset_required(super::admit_configuration_schema(&*connection, None).await);
    assert_reset_required(ensure_configuration_schema(&*connection, None).await);
    assert_eq!(sqlite_objects(&connection).await, before);

    assert_eq!(
        count(
            &*connection,
            "SELECT COUNT(*) FROM sqlite_master WHERE name = 'configuration_credential_references'"
        )
        .await,
        1,
        "released credential table remains available for an explicit reset"
    );
    assert_eq!(
        count(
            &*connection,
            "SELECT COUNT(*) FROM configuration_entries WHERE revision_id = 'revision.1'"
        )
        .await,
        1,
        "supported configuration rows remain"
    );
}

#[tokio::test]
async fn released_configuration_shape_with_credential_rows_stays_reset_required() {
    let (_directory, connection) = released_connection().await;
    connection
        .execute_batch(
            "INSERT INTO configuration_credential_references VALUES
                ('credential.1', 'api_token', 'sha256:ac', 'sha256:ad', 1, 'sha256:ae', 1, 1, 2, 0);",
        )
        .await
        .unwrap();
    let before = sqlite_objects(&connection).await;
    assert_reset_required(ensure_configuration_schema(&*connection, None).await);
    assert_eq!(sqlite_objects(&connection).await, before);
    assert_eq!(
        count(
            &*connection,
            "SELECT COUNT(*) FROM configuration_credential_references"
        )
        .await,
        1,
        "refusal must not discard the unknown row"
    );
}

#[tokio::test]
async fn released_configuration_shape_with_current_key_schema_revision_stays_reset_required() {
    let (_directory, connection) = released_connection().await;
    connection
        .execute_batch(
            r#"
            INSERT INTO configuration_entries VALUES
                ('revision.1', 'diagnostics.prewarm.v1', 'default', NULL, 2,
                 '{"schema_version":1,"value":{"kind":"boolean","value":false},"provenance":[{"layer":{"kind":"default"},"revision_id":"configuration.registry.default.v1","disposition":"defaulted","safe_reason":"registry_default"}]}');
            "#,
        )
        .await
        .unwrap();
    let before = sqlite_objects(&connection).await;

    assert_reset_required(super::admit_configuration_schema(&*connection, None).await);
    assert_eq!(sqlite_objects(&connection).await, before);
    assert_reset_required(ensure_configuration_schema(&*connection, None).await);
    assert_eq!(sqlite_objects(&connection).await, before);
    assert_eq!(
        count(
            &*connection,
            "SELECT COUNT(*) FROM configuration_entries
             WHERE key = 'diagnostics.prewarm.v1' AND schema_revision = 2",
        )
        .await,
        1,
        "unsupported entry schema revision must remain available for reset"
    );
}

#[tokio::test]
async fn released_configuration_shape_with_current_key_payload_version_stays_reset_required() {
    let (_directory, connection) = released_connection().await;
    connection
        .execute_batch(
            r#"
            INSERT INTO configuration_entries VALUES
                ('revision.1', 'diagnostics.prewarm.v1', 'default', NULL, 1,
                 '{"schema_version":2,"value":{"kind":"boolean","value":false},"provenance":[{"layer":{"kind":"default"},"revision_id":"configuration.registry.default.v1","disposition":"defaulted","safe_reason":"registry_default"}]}');
            "#,
        )
        .await
        .unwrap();
    let before = sqlite_objects(&connection).await;

    assert_reset_required(super::admit_configuration_schema(&*connection, None).await);
    assert_eq!(sqlite_objects(&connection).await, before);
    assert_reset_required(ensure_configuration_schema(&*connection, None).await);
    assert_eq!(sqlite_objects(&connection).await, before);
    assert_eq!(
        count(
            &*connection,
            "SELECT COUNT(*) FROM configuration_entries
             WHERE key = 'diagnostics.prewarm.v1' AND json_extract(typed_value, '$.schema_version') = 2",
        )
        .await,
        1,
        "unsupported encoded payload version must remain available for reset"
    );
}

#[tokio::test]
async fn every_retired_released_table_row_stays_reset_required_without_mutation() {
    for &(table, insert) in RELEASED_CONFIGURATION_RETIRED_TABLE_ROWS {
        let (_directory, connection) = released_connection().await;
        connection.execute_batch(insert).await.unwrap();
        let before = sqlite_objects(&connection).await;

        assert_reset_required(super::admit_configuration_schema(&*connection, None).await);
        assert_eq!(sqlite_objects(&connection).await, before);
        assert_reset_required(ensure_configuration_schema(&*connection, None).await);
        assert_eq!(sqlite_objects(&connection).await, before);

        assert_eq!(
            count(&*connection, &format!("SELECT COUNT(*) FROM {table}")).await,
            1,
            "the representative row in {table} must survive refusal"
        );
    }
}

#[tokio::test]
async fn full_beta37_snapshot_with_retired_entries_stays_reset_required() {
    let (_directory, connection) = released_full_snapshot_connection().await;
    let before = sqlite_objects(&connection).await;

    assert_reset_required(super::admit_configuration_schema(&*connection, None).await);
    assert_eq!(sqlite_objects(&connection).await, before);
    assert_reset_required(ensure_configuration_schema(&*connection, None).await);
    assert_eq!(sqlite_objects(&connection).await, before);

    assert_eq!(
        count(
            &*connection,
            "SELECT COUNT(*) FROM configuration_entries
             WHERE revision_id = 'revision.beta37.registry4'
               AND key = 'semantic.runtime.v1'",
        )
        .await,
        1,
        "the retired semantic runtime entry must remain available for reset"
    );
    assert_eq!(
        count(
            &*connection,
            "SELECT COUNT(*) FROM configuration_entries
             WHERE revision_id = 'revision.beta37.registry4'
               AND key = 'query.default_collection.v1'",
        )
        .await,
        1,
        "the retired collection entry must remain available for reset"
    );
}

#[tokio::test]
async fn full_beta37_snapshot_refusal_rolls_back_without_mutation() {
    let (_directory, connection) = released_full_snapshot_connection().await;
    let before = sqlite_objects(&connection).await;
    let transaction = connection
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .await
        .unwrap();

    assert_reset_required(ensure_configuration_schema(&transaction, None).await);
    transaction.rollback().await.unwrap();

    assert_eq!(sqlite_objects(&connection).await, before);
    assert_eq!(
        count(
            &*connection,
            "SELECT COUNT(*) FROM configuration_entries
             WHERE key = 'semantic.runtime.v1'",
        )
        .await,
        1,
        "rollback must preserve the beta37 retired setting"
    );
}
