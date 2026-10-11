use std::sync::Arc;

use tempfile::tempdir;
use tracedecay_domain::{RetrievalAnchorId, SessionId};
use tracedecay_global_db::RegisteredGlobalDb;
use tracedecay_global_db::tests::harness::{HostAdmissionScope, HostAdmissionTestRuntimeV1};
use tracedecay_graph_db::NeverCancelled;
use tracedecay_runtime_core::db::engine::params;
use tracedecay_store::SessionStoreResult;

use crate::SessionTemporalAccess;
use crate::handle::SessionTemporalRegisteredDb;
use crate::relations::{
    SessionRelationProjection, SessionRelationScope, SummaryRelationNode, SummarySourceRef,
};

async fn registered_runtime(directory: &std::path::Path) -> HostAdmissionTestRuntimeV1 {
    HostAdmissionTestRuntimeV1::profile(directory)
        .await
        .expect("registered profile runtime")
}

fn database(runtime: &HostAdmissionTestRuntimeV1) -> &RegisteredGlobalDb {
    runtime
        .registered_database(HostAdmissionScope::Profile)
        .expect("registered session database")
}

fn projection(
    scope: &SessionRelationScope,
    session_id: &SessionId,
    generation: u64,
) -> SessionRelationProjection {
    SessionRelationProjection {
        scope: scope.clone(),
        session_id: session_id.clone(),
        generation,
        summaries: vec![SummaryRelationNode {
            summary_id: format!("summary-{session_id}"),
            sources: vec![SummarySourceRef::Anchor {
                anchor_id: RetrievalAnchorId::new(format!("anchor-{session_id}"))
                    .expect("anchor id"),
            }],
            predecessor_summary_id: None,
        }],
        logical_copies: Vec::new(),
        thread_hierarchy: Vec::new(),
        agent_hierarchy: Vec::new(),
        parent_session_id: None,
        workflow_agents: Vec::new(),
    }
}

async fn activate_generation(
    database: &RegisteredGlobalDb,
    session_id: &SessionId,
    generation: i64,
) {
    let writer = database
        .writer_connection()
        .expect("registered writer connection");
    writer
        .execute(
            "INSERT INTO session_temporal_generations
                 (session_id, generation, state, frozen_watermarks_json, created_at)
             VALUES (?1, ?2, 'building', '{}', 1)",
            params![session_id.as_str(), generation],
        )
        .await
        .expect("begin generation");
    writer
        .execute(
            "UPDATE session_temporal_generations
             SET state = 'ready', ready_at = 2
             WHERE session_id = ?1 AND generation = ?2",
            params![session_id.as_str(), generation],
        )
        .await
        .expect("ready generation");
    writer
        .execute(
            "UPDATE session_temporal_generations
             SET state = 'active', activated_at = 3
             WHERE session_id = ?1 AND generation = ?2",
            params![session_id.as_str(), generation],
        )
        .await
        .expect("activate generation");
}

async fn seed_pending_receipt(
    database: &RegisteredGlobalDb,
    projection: &SessionRelationProjection,
    with_journal: bool,
) {
    let watermark =
        crate::relations::projection_watermark(projection).expect("projection watermark");
    let generation = i64::try_from(projection.generation).expect("generation");
    let writer = database
        .writer_connection()
        .expect("registered writer connection");
    writer
        .execute(
            "INSERT INTO session_relation_receipts
                 (session_id, generation, scope_kind, scope_id, expected_graph_watermark,
                  state, graph_watermark, created_at, applied_at)
             VALUES (?1, ?2, ?3, ?4, ?5, 'pending', NULL, 1, NULL)",
            params![
                projection.session_id.as_str(),
                generation,
                match &projection.scope {
                    SessionRelationScope::ProjectSessions { .. } => "project_sessions",
                    SessionRelationScope::ProfileSessions { .. } => "profile_sessions",
                },
                projection.scope.identity(),
                watermark.as_str(),
            ],
        )
        .await
        .expect("seed pending receipt");
    if with_journal {
        let projection_json = serde_json::to_string(projection).expect("projection json");
        writer
            .execute(
                "INSERT INTO session_relation_effect_journal
                     (session_id, generation, projection_json, created_at)
                 VALUES (?1, ?2, ?3, 1)",
                params![projection.session_id.as_str(), generation, projection_json],
            )
            .await
            .expect("seed journal");
    }
}

async fn receipt_state(
    database: &RegisteredGlobalDb,
    session_id: &SessionId,
    generation: i64,
) -> Option<String> {
    let snapshot = database.read_snapshot().await.expect("read snapshot");
    let mut rows = snapshot
        .query(
            "SELECT state FROM session_relation_receipts
             WHERE session_id = ?1 AND generation = ?2",
            params![session_id.as_str(), generation],
        )
        .await
        .expect("receipt state");
    rows.next()
        .await
        .expect("receipt row")
        .map(|row| row.get::<String>(0).expect("state"))
}

async fn journal_projection(
    database: &RegisteredGlobalDb,
    session_id: &SessionId,
    generation: i64,
) -> Option<String> {
    let snapshot = database.read_snapshot().await.expect("read snapshot");
    let mut rows = snapshot
        .query(
            "SELECT projection_json FROM session_relation_effect_journal
             WHERE session_id = ?1 AND generation = ?2",
            params![session_id.as_str(), generation],
        )
        .await
        .expect("journal projection");
    rows.next()
        .await
        .expect("journal row")
        .map(|row| row.get::<String>(0).expect("projection json"))
}

fn outcome_for<'a>(
    projections: &'a [SessionRelationProjection],
    outcomes: &'a [SessionStoreResult<()>],
    session_id: &SessionId,
) -> &'a SessionStoreResult<()> {
    projections
        .iter()
        .zip(outcomes)
        .find(|(projection, _)| projection.session_id == *session_id)
        .map(|(_, outcome)| outcome)
        .unwrap_or_else(|| panic!("missing per-item outcome for {session_id}"))
}

struct AlwaysCancelled;

impl tracedecay_graph_db::GraphCancellation for AlwaysCancelled {
    fn is_cancelled(&self) -> bool {
        true
    }
}

#[tokio::test]
async fn failed_acknowledgement_rolls_back_only_its_own_mutation() {
    let directory = tempdir().expect("temporary session store");
    let runtime = registered_runtime(directory.path()).await;
    let database = database(&runtime);
    let (scope, _store) =
        <RegisteredGlobalDb as SessionTemporalRegisteredDb>::session_relation_store(database)
            .expect("session relation store");

    let ok_session = SessionId::new("session-ok").expect("session id");
    let bad_session = SessionId::new("session-bad").expect("session id");
    activate_generation(database, &ok_session, 1).await;
    activate_generation(database, &bad_session, 1).await;

    let ok_projection = projection(&scope, &ok_session, 1);
    let bad_projection = projection(&scope, &bad_session, 1);
    seed_pending_receipt(database, &ok_projection, true).await;
    seed_pending_receipt(database, &bad_projection, false).await;

    let projections = [bad_projection, ok_projection];
    let outcomes = SessionTemporalAccess::new(database)
        .apply_session_relation_projection_items(&projections, Arc::new(NeverCancelled))
        .await
        .expect("batch apply");

    assert_eq!(
        outcome_for(&projections, &outcomes, &bad_session)
            .as_ref()
            .expect_err("missing-journal ack must refuse that item")
            .to_string(),
        "session-temporal storage operation publish native session relation receipt failed: \
         relation effect journal changed during native graph acknowledgement"
    );
    outcome_for(&projections, &outcomes, &ok_session)
        .as_ref()
        .expect("a peer item's failure must not poison the healthy ack");
    assert_eq!(
        receipt_state(database, &bad_session, 1).await.as_deref(),
        Some("pending"),
        "failed acknowledgement must roll back its applied update"
    );
    assert_eq!(
        journal_projection(database, &bad_session, 1).await,
        None,
        "a missing-journal failure must not invent a journal row"
    );
    assert_eq!(
        receipt_state(database, &ok_session, 1).await.as_deref(),
        Some("applied")
    );
    assert_eq!(
        journal_projection(database, &ok_session, 1).await,
        None,
        "the healthy acknowledgement must drain its journal"
    );
}

#[tokio::test]
async fn cancelled_batch_leaves_every_receipt_pending() {
    let directory = tempdir().expect("temporary session store");
    let runtime = registered_runtime(directory.path()).await;
    let database = database(&runtime);
    let (scope, _store) =
        <RegisteredGlobalDb as SessionTemporalRegisteredDb>::session_relation_store(database)
            .expect("session relation store");

    let session = SessionId::new("session-cancelled").expect("session id");
    activate_generation(database, &session, 1).await;
    let projection = projection(&scope, &session, 1);
    seed_pending_receipt(database, &projection, true).await;
    let journal_before = journal_projection(database, &session, 1).await;
    assert!(
        journal_before.is_some(),
        "the seeded journal must exist before the cancelled apply"
    );

    let projections = [projection];
    let outcomes = SessionTemporalAccess::new(database)
        .apply_session_relation_projection_items(&projections, Arc::new(AlwaysCancelled))
        .await
        .expect("cancelled batch apply");

    assert_eq!(
        outcome_for(&projections, &outcomes, &session)
            .as_ref()
            .expect_err("cancellation is a typed per-item refusal")
            .to_string(),
        "session temporal operation was cancelled"
    );
    assert_eq!(
        receipt_state(database, &session, 1).await.as_deref(),
        Some("pending")
    );
    assert_eq!(
        journal_projection(database, &session, 1).await,
        journal_before,
        "cancellation must leave the seeded journal in place"
    );
}

#[tokio::test]
async fn missing_receipt_fails_only_that_item() {
    let directory = tempdir().expect("temporary session store");
    let runtime = registered_runtime(directory.path()).await;
    let database = database(&runtime);
    let (scope, _store) =
        <RegisteredGlobalDb as SessionTemporalRegisteredDb>::session_relation_store(database)
            .expect("session relation store");

    let ok_session = SessionId::new("session-ok").expect("session id");
    let stale_session = SessionId::new("session-stale").expect("session id");
    activate_generation(database, &ok_session, 1).await;
    activate_generation(database, &stale_session, 1).await;

    let ok_projection = projection(&scope, &ok_session, 1);
    let stale_projection = projection(&scope, &stale_session, 1);
    seed_pending_receipt(database, &ok_projection, true).await;
    let projections = [stale_projection, ok_projection];
    let outcomes = SessionTemporalAccess::new(database)
        .apply_session_relation_projection_items(&projections, Arc::new(NeverCancelled))
        .await
        .expect("batch apply");

    assert_eq!(
        outcome_for(&projections, &outcomes, &stale_session)
            .as_ref()
            .expect_err("absent receipt must refuse that item")
            .to_string(),
        "session-temporal storage operation publish native session relation receipt failed: \
         relation receipt is unavailable"
    );
    outcome_for(&projections, &outcomes, &ok_session)
        .as_ref()
        .expect("an absent peer receipt must not poison the healthy ack");
    assert_eq!(
        receipt_state(database, &ok_session, 1).await.as_deref(),
        Some("applied")
    );
    assert_eq!(
        journal_projection(database, &ok_session, 1).await,
        None,
        "the healthy acknowledgement must drain its journal"
    );
    assert_eq!(
        receipt_state(database, &stale_session, 1).await,
        None,
        "an absent receipt must stay absent"
    );
    assert_eq!(
        journal_projection(database, &stale_session, 1).await,
        None,
        "an absent receipt must not invent a journal row"
    );
}
