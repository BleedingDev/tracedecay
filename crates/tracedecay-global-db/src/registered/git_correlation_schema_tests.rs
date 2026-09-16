use std::fs;

use tempfile::TempDir;
use tracedecay_domain::errors::TraceDecayError;
use tracedecay_runtime_core::db::TestDatabaseRuntimeScope;
use tracedecay_runtime_core::db::engine::{Executor, QueryExecutor, TestConnection};

use crate::tests::harness::open_registered_test_database_fixture;

async fn assert_git_schema_reset_without_mutation(malformed_schema: &str) {
    crate::register_registered_schema_installer();
    let directory = TempDir::new().unwrap();
    let database_path = directory.path().join("sessions.db");
    drop(
        open_registered_test_database_fixture(
            &database_path,
            TestDatabaseRuntimeScope::ProfileSessions,
        )
        .await
        .unwrap(),
    );

    let database = TestConnection::open(&database_path);
    let connection = (*database).clone();
    connection
        .execute_batch("DROP TABLE git_history_index_pending;")
        .await
        .unwrap();
    connection.execute_batch(malformed_schema).await.unwrap();
    drop(connection);
    drop(database);

    let before_bytes = fs::read(&database_path).unwrap();
    let before_schema = {
        let database = TestConnection::open(&database_path);
        let connection = (*database).clone();
        let mut rows = connection
            .query(
                "SELECT type, name, tbl_name, sql
                 FROM sqlite_master
                 WHERE name NOT LIKE 'sqlite_%'
                 ORDER BY type, name",
                (),
            )
            .await
            .unwrap();
        let mut schema = Vec::new();
        while let Some(row) = rows.next().await.unwrap() {
            schema.push((
                row.get::<String>(0).unwrap(),
                row.get::<String>(1).unwrap(),
                row.get::<String>(2).unwrap(),
                row.get::<Option<String>>(3).unwrap(),
            ));
        }
        schema
    };

    let error = match open_registered_test_database_fixture(
        &database_path,
        TestDatabaseRuntimeScope::ProfileSessions,
    )
    .await
    {
        Ok(_) => panic!("Git schema drift must refuse global-db admission"),
        Err(error) => error,
    };
    assert!(matches!(
        error,
        TraceDecayError::ResetRequired {
            ref authority,
            ..
        } if authority == "git correlation"
    ));
    assert_eq!(
        fs::read(&database_path).unwrap(),
        before_bytes,
        "typed Git-schema refusal must preserve the exact database bytes"
    );

    let database = TestConnection::open(&database_path);
    let connection = (*database).clone();
    let mut rows = connection
        .query(
            "SELECT type, name, tbl_name, sql
             FROM sqlite_master
             WHERE name NOT LIKE 'sqlite_%'
             ORDER BY type, name",
            (),
        )
        .await
        .unwrap();
    let mut schema = Vec::new();
    while let Some(row) = rows.next().await.unwrap() {
        schema.push((
            row.get::<String>(0).unwrap(),
            row.get::<String>(1).unwrap(),
            row.get::<String>(2).unwrap(),
            row.get::<Option<String>>(3).unwrap(),
        ));
    }
    assert_eq!(schema, before_schema);
}

#[tokio::test]
async fn global_schema_stage_maps_git_reset_required_and_preserves_the_store() {
    assert_git_schema_reset_without_mutation(
        "CREATE TABLE git_history_index_pending (
             source_rowid INTEGER NOT NULL,
             segment_ordinal INTEGER NOT NULL CHECK(segment_ordinal >= 0),
             oid TEXT NOT NULL,
             PRIMARY KEY(source_rowid, segment_ordinal, oid)
         );",
    )
    .await;
}
