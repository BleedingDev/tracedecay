use super::*;

fn test_conn() -> (
    tempfile::TempDir,
    tracedecay_runtime_core::db::engine::TestConnection,
) {
    let directory = tempfile::tempdir().unwrap();
    let connection = tracedecay_runtime_core::db::engine::TestConnection::open(
        &directory.path().join("sessions.db"),
    );
    (directory, connection)
}

struct FailingQueryExecutor;

impl QueryExecutor for FailingQueryExecutor {
    async fn query<P>(
        &self,
        _sql: &str,
        _params: P,
    ) -> tracedecay_runtime_core::db::engine::Result<tracedecay_runtime_core::db::engine::Rows>
    where
        P: tracedecay_runtime_core::db::engine::IntoParams,
    {
        Err(tracedecay_runtime_core::db::engine::Error::Runtime(
            "injected workflow read failure".to_string(),
        ))
    }
}

fn sample_run(run_id: &str, parent: &str) -> WorkflowRun {
    WorkflowRun {
        run_id: run_id.to_string(),
        parent_session_id: parent.to_string(),
        name: Some("triggering-evals".to_string()),
        description: Some("mine + run + score".to_string()),
        phase_json: Some(r#"[{"title":"Mine"},{"title":"Run"}]"#.to_string()),
        status: WorkflowStatus::Completed,
        started_ts: Some(1_700_000_000),
        ended_ts: Some(1_700_000_900),
        result_summary: Some("36 scenarios, 45 runs".to_string()),
        agent_count: 11,
    }
}

async fn schema_snapshot(
    conn: &tracedecay_runtime_core::db::engine::TestConnection,
) -> Vec<(String, Option<String>)> {
    let mut rows = conn
        .query(
            "SELECT name, sql FROM sqlite_master
             WHERE name NOT LIKE 'sqlite_%'
             ORDER BY name",
            (),
        )
        .await
        .unwrap();
    let mut snapshot = Vec::new();
    while let Some(row) = rows.next().await.unwrap() {
        snapshot.push((
            row.get::<String>(0).unwrap(),
            row.get::<Option<String>>(1).unwrap(),
        ));
    }
    snapshot
}

async fn assert_reset_required_without_mutation(
    conn: &tracedecay_runtime_core::db::engine::TestConnection,
) {
    let before = schema_snapshot(conn).await;
    let error = ensure_workflow_index_schema(conn)
        .await
        .expect_err("drifted workflow schema must require reset");
    assert!(matches!(
        error,
        WorkflowIndexError::ResetRequired {
            found_version: Some(WORKFLOW_INDEX_SCHEMA_VERSION),
            required_version: WORKFLOW_INDEX_SCHEMA_VERSION,
        }
    ));
    assert_eq!(schema_snapshot(conn).await, before);
}

async fn recreate_workflow_runs_with_status_check(
    conn: &tracedecay_runtime_core::db::engine::TestConnection,
    status_constraint: &str,
) {
    conn.execute_batch(
        "DROP INDEX idx_workflow_runs_parent;
         DROP TABLE workflow_runs;",
    )
    .await
    .unwrap();
    let sql = format!(
        "CREATE TABLE workflow_runs (
             run_id TEXT PRIMARY KEY,
             parent_session_id TEXT NOT NULL DEFAULT '',
             name TEXT,
             description TEXT,
             phase_json TEXT,
             status TEXT NOT NULL DEFAULT 'unknown' {status_constraint},
             started_ts INTEGER,
             ended_ts INTEGER,
             result_summary TEXT,
             agent_count INTEGER NOT NULL DEFAULT 0,
             created_at INTEGER NOT NULL DEFAULT (unixepoch()),
             updated_at INTEGER NOT NULL DEFAULT (unixepoch())
         );
         CREATE INDEX idx_workflow_runs_parent
             ON workflow_runs(parent_session_id, started_ts);"
    );
    conn.execute_batch(&sql).await.unwrap();
}

#[test]
fn status_from_disk_folds_known_and_unknown() {
    assert_eq!(
        WorkflowStatus::from_disk("completed"),
        WorkflowStatus::Completed
    );
    assert_eq!(WorkflowStatus::from_disk("done"), WorkflowStatus::Completed);
    assert_eq!(
        WorkflowStatus::from_disk("in_progress"),
        WorkflowStatus::Running
    );
    assert_eq!(WorkflowStatus::from_disk("blocked"), WorkflowStatus::Failed);
    assert_eq!(
        WorkflowStatus::from_disk("timed_out"),
        WorkflowStatus::Failed
    );
    assert_eq!(WorkflowStatus::from_disk("banana"), WorkflowStatus::Unknown);
}

#[tokio::test]
async fn queries_are_empty_before_schema_exists() {
    let (_directory, conn) = test_conn();
    // No tables yet: readers must fail-open to empty/None.
    assert!(!tables_present(&conn).await.unwrap());
    assert!(
        runs_for_session(&conn, "sess", 10)
            .await
            .unwrap()
            .is_empty()
    );
    assert!(run_for_id(&conn, "wf_x").await.unwrap().is_none());
    assert!(agents_for_run(&conn, "wf_x", 10).await.unwrap().is_empty());
}

#[tokio::test]
async fn schema_read_failures_are_not_reported_as_empty_or_absent() {
    let conn = FailingQueryExecutor;

    assert!(runs_for_session(&conn, "sess", 10).await.is_err());
    assert!(run_for_id(&conn, "wf_x").await.is_err());
    assert!(agents_for_run(&conn, "wf_x", 10).await.is_err());
    let scope = vec![("claude".to_owned(), "sess".to_owned())];
    assert!(runs_for_git_scope(&conn, Some(&scope), 10).await.is_err());
}

#[tokio::test]
async fn drifted_workflow_index_requires_reset_without_mutation() {
    let (_directory, conn) = test_conn();
    ensure_workflow_index_schema(&conn).await.unwrap();
    conn.execute_batch(
        "CREATE INDEX idx_workflow_runs_unexpected
         ON workflow_runs(run_id);",
    )
    .await
    .unwrap();
    assert_reset_required_without_mutation(&conn).await;
}

#[tokio::test]
async fn missing_workflow_check_constraint_requires_reset_without_mutation() {
    let (_directory, conn) = test_conn();
    ensure_workflow_index_schema(&conn).await.unwrap();
    recreate_workflow_runs_with_status_check(&conn, "").await;

    assert_reset_required_without_mutation(&conn).await;
}

#[tokio::test]
async fn altered_workflow_check_constraint_requires_reset_without_mutation() {
    let (_directory, conn) = test_conn();
    ensure_workflow_index_schema(&conn).await.unwrap();
    recreate_workflow_runs_with_status_check(&conn, "CHECK(status IN ('running', 'completed'))")
        .await;

    assert_reset_required_without_mutation(&conn).await;
}

#[tokio::test]
async fn altered_workflow_strict_and_foreign_key_constraints_require_reset() {
    let (_directory, conn) = test_conn();
    ensure_workflow_index_schema(&conn).await.unwrap();
    conn.execute_batch(
        "DROP INDEX idx_workflow_agents_run;
         DROP TABLE workflow_agents;
         CREATE TABLE workflow_agents (
             run_id TEXT NOT NULL,
             agent_label TEXT NOT NULL,
             agent_id TEXT NOT NULL DEFAULT '',
             phase TEXT,
             transcript_path TEXT,
             agent_session_id TEXT,
             status TEXT NOT NULL DEFAULT 'unknown'
                 CHECK(status IN ('running', 'completed', 'failed', 'unknown')),
             model TEXT,
             tokens INTEGER NOT NULL DEFAULT 0,
             started_ts INTEGER,
             ended_ts INTEGER,
             created_at INTEGER NOT NULL DEFAULT (unixepoch()),
             updated_at INTEGER NOT NULL DEFAULT (unixepoch()),
             PRIMARY KEY(run_id, agent_label, agent_id),
             FOREIGN KEY(run_id) REFERENCES workflow_runs(run_id)
         ) STRICT;
         CREATE INDEX idx_workflow_agents_run
             ON workflow_agents(run_id, phase);",
    )
    .await
    .unwrap();

    assert_reset_required_without_mutation(&conn).await;
}

#[tokio::test]
async fn arbitrary_workflow_objects_and_inert_table_objects_require_reset() {
    let (_directory, conn) = test_conn();
    ensure_workflow_index_schema(&conn).await.unwrap();
    conn.execute_batch(
        "CREATE TABLE workflow_auxiliary (id INTEGER);
         CREATE INDEX inert_workflow_index ON workflow_runs(run_id);
         CREATE TRIGGER workflow_audit_trigger
         AFTER INSERT ON workflow_runs
         BEGIN
             SELECT 1;
         END;",
    )
    .await
    .unwrap();

    assert_reset_required_without_mutation(&conn).await;
}

#[tokio::test]
async fn ensure_workflow_index_schema_is_idempotent_for_current_schema() {
    let (_directory, conn) = test_conn();
    ensure_workflow_index_schema(&conn).await.unwrap();
    let before = schema_snapshot(&conn).await;

    assert_eq!(
        require_admissible_workflow_index_schema(&conn)
            .await
            .unwrap(),
        WorkflowIndexSchemaAdmission::Current
    );
    ensure_workflow_index_schema(&conn).await.unwrap();

    assert_eq!(schema_snapshot(&conn).await, before);
}

#[tokio::test]
async fn failed_workflow_schema_install_can_be_rolled_back_without_partial_objects() {
    let (_directory, conn) = test_conn();
    conn.execute_batch(
        "CREATE TABLE session_schema_migrations (
             name TEXT PRIMARY KEY,
             version INTEGER NOT NULL,
             applied_at INTEGER NOT NULL DEFAULT (unixepoch()),
             CHECK(name <> 'workflow_indexing')
         );",
    )
    .await
    .unwrap();
    let before = schema_snapshot(&conn).await;

    let transaction = conn.transaction().await.unwrap();
    assert!(ensure_workflow_index_schema(&transaction).await.is_err());
    transaction.rollback().await.unwrap();

    assert_eq!(schema_snapshot(&conn).await, before);
}

#[tokio::test]
async fn upsert_is_idempotent_and_updates_mutable_columns() {
    let (_directory, conn) = test_conn();
    ensure_workflow_index_schema(&conn).await.unwrap();
    assert!(tables_present(&conn).await.unwrap());

    let mut run = sample_run("wf_alpha", "sess-1");
    run.status = WorkflowStatus::Running;
    run.result_summary = None;
    upsert_run(&conn, &run).await.unwrap();

    // Re-ingest the same run once it finished: overwrite, don't duplicate.
    let finished = sample_run("wf_alpha", "sess-1");
    upsert_run(&conn, &finished).await.unwrap();

    let all = runs_for_session(&conn, "sess-1", 10).await.unwrap();
    assert_eq!(all.len(), 1);
    assert_eq!(all[0], finished);
    assert_eq!(all[0].status, WorkflowStatus::Completed);
    assert_eq!(
        all[0].result_summary.as_deref(),
        Some("36 scenarios, 45 runs")
    );

    assert_eq!(run_for_id(&conn, "wf_alpha").await.unwrap(), Some(finished));
    assert!(run_for_id(&conn, "wf_missing").await.unwrap().is_none());
}

#[tokio::test]
async fn empty_run_id_is_rejected() {
    let (_directory, conn) = test_conn();
    ensure_workflow_index_schema(&conn).await.unwrap();
    let mut run = sample_run("   ", "sess");
    run.run_id = "   ".to_string();
    let err = upsert_run(&conn, &run).await.unwrap_err();
    assert!(matches!(err, WorkflowIndexError::InvalidArgument(_)));
}

#[tokio::test]
async fn agents_upsert_and_order_within_run() {
    let (_directory, conn) = test_conn();
    ensure_workflow_index_schema(&conn).await.unwrap();
    upsert_run(&conn, &sample_run("wf_a", "sess"))
        .await
        .unwrap();

    let second = WorkflowAgent {
        run_id: "wf_a".to_string(),
        agent_label: "run:batch2".to_string(),
        agent_id: "a222".to_string(),
        phase: Some("Run".to_string()),
        transcript_path: Some("/tmp/agent-a222.jsonl".to_string()),
        agent_session_id: None,
        status: WorkflowStatus::Completed,
        model: Some("claude-fable-5".to_string()),
        tokens: 4200,
        started_ts: Some(2_000),
        ended_ts: Some(2_500),
    };
    let first = WorkflowAgent {
        agent_label: "mine:claude".to_string(),
        agent_id: "a111".to_string(),
        phase: Some("Mine".to_string()),
        started_ts: Some(1_000),
        ..second.clone()
    };
    upsert_agent(&conn, &second).await.unwrap();
    upsert_agent(&conn, &first).await.unwrap();
    // Idempotent re-ingest of the first agent.
    upsert_agent(&conn, &first).await.unwrap();

    let agents = agents_for_run(&conn, "wf_a", 10).await.unwrap();
    let labels: Vec<&str> = agents.iter().map(|a| a.agent_label.as_str()).collect();
    assert_eq!(labels, vec!["mine:claude", "run:batch2"]);
    assert_eq!(agents[0].tokens, 4200);
    assert_eq!(agents[1].model.as_deref(), Some("claude-fable-5"));
}

#[tokio::test]
async fn runs_for_git_scope_filters_by_resolved_parent_sessions() {
    let (_directory, conn) = test_conn();
    ensure_workflow_index_schema(&conn).await.unwrap();

    // A run owned by sess-branch, another owned by sess-other.
    upsert_run(&conn, &sample_run("wf_on_branch", "sess-branch"))
        .await
        .unwrap();
    upsert_run(&conn, &sample_run("wf_elsewhere", "sess-other"))
        .await
        .unwrap();
    // An orphan run with no resolvable parent must never leak into a
    // git-scoped result.
    upsert_run(&conn, &sample_run("wf_orphan", ""))
        .await
        .unwrap();

    // The Git evidence graph resolved the scope to sess-branch.
    let scope = vec![("claude".to_owned(), "sess-branch".to_owned())];
    let hits = runs_for_git_scope(&conn, Some(&scope), 10).await.unwrap();
    let ids: Vec<&str> = hits.iter().map(|r| r.run_id.as_str()).collect();
    assert_eq!(ids, vec!["wf_on_branch"]);

    // An authoritative empty scope yields nothing.
    assert!(
        runs_for_git_scope(&conn, Some(&[]), 10)
            .await
            .unwrap()
            .is_empty()
    );

    // A resolved session with an empty id must not match the orphan run.
    let empty_parent = vec![("claude".to_owned(), String::new())];
    assert!(
        runs_for_git_scope(&conn, Some(&empty_parent), 10)
            .await
            .unwrap()
            .is_empty()
    );

    // An unavailable graph authority is an error, not an empty result.
    assert!(matches!(
        runs_for_git_scope(&conn, None, 10).await,
        Err(WorkflowIndexError::AuthorityUnavailable { .. })
    ));
}

#[tokio::test]
async fn registered_snapshot_preserves_workflow_query_semantics() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("sessions.db");
    let conn = tracedecay_runtime_core::db::engine::TestConnection::open(&path);
    ensure_workflow_index_schema(&conn).await.unwrap();

    let mut older = sample_run("wf_old", "sess-branch");
    older.started_ts = Some(1_000);
    let mut newer = sample_run("wf_new", "sess-branch");
    newer.started_ts = Some(2_000);
    upsert_run(&conn, &older).await.unwrap();
    upsert_run(&conn, &newer).await.unwrap();
    upsert_agent(
        &conn,
        &WorkflowAgent {
            run_id: "wf_new".to_string(),
            agent_label: "implement".to_string(),
            agent_id: "agent-1".to_string(),
            phase: Some("Build".to_string()),
            transcript_path: None,
            agent_session_id: Some("agent-session".to_string()),
            status: WorkflowStatus::Running,
            model: Some("test-model".to_string()),
            tokens: 41,
            started_ts: Some(2_001),
            ended_ts: None,
        },
    )
    .await
    .unwrap();
    let reader = RegisteredWorkflowIndexSnapshot::from_engine_test_snapshot(
        conn.read_snapshot().await.unwrap(),
    );

    let runs = reader.runs_for_session("sess-branch", 10).await.unwrap();
    assert_eq!(
        runs.iter()
            .map(|run| run.run_id.as_str())
            .collect::<Vec<_>>(),
        vec!["wf_new", "wf_old"]
    );
    assert_eq!(
        reader.run_for_id("wf_new").await.unwrap(),
        Some(newer.clone())
    );
    let agents = reader.agents_for_run("wf_new", 10).await.unwrap();
    assert_eq!(agents.len(), 1);
    assert_eq!(agents[0].agent_label, "implement");

    let scope = vec![("claude".to_owned(), "sess-branch".to_owned())];
    let scoped = reader.runs_for_git_scope(Some(&scope), 10).await.unwrap();
    assert_eq!(
        scoped
            .iter()
            .map(|run| run.run_id.as_str())
            .collect::<Vec<_>>(),
        vec!["wf_new", "wf_old"]
    );
}

#[tokio::test]
async fn registered_snapshot_isolated_from_later_workflow_writes() {
    let (_directory, conn) = test_conn();
    ensure_workflow_index_schema(&conn).await.unwrap();
    upsert_run(&conn, &sample_run("wf_before_snapshot", "sess-1"))
        .await
        .unwrap();

    let snapshot = RegisteredWorkflowIndexSnapshot::from_engine_test_snapshot(
        conn.read_snapshot().await.unwrap(),
    );
    // A later start keeps the fresh-snapshot ordering assertion meaningful:
    // with equal timestamps the query tie-breaks on `run_id DESC`, which would
    // invert the recency order this test observes.
    let mut later_run = sample_run("wf_after_snapshot", "sess-1");
    later_run.started_ts = Some(1_700_000_001);
    upsert_run(&conn, &later_run).await.unwrap();

    assert_eq!(
        snapshot
            .runs_for_session("sess-1", MAX_WORKFLOW_LIMIT)
            .await
            .unwrap()
            .iter()
            .map(|run| run.run_id.as_str())
            .collect::<Vec<_>>(),
        vec!["wf_before_snapshot"]
    );
    let fresh = RegisteredWorkflowIndexSnapshot::from_engine_test_snapshot(
        conn.read_snapshot().await.unwrap(),
    );
    assert_eq!(
        fresh
            .runs_for_session("sess-1", MAX_WORKFLOW_LIMIT)
            .await
            .unwrap()
            .iter()
            .map(|run| run.run_id.as_str())
            .collect::<Vec<_>>(),
        vec!["wf_after_snapshot", "wf_before_snapshot"]
    );
}
