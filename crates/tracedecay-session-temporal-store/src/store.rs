#[cfg(any(test, feature = "test-helpers"))]
use std::collections::BTreeSet;
use std::{
    future::Future,
    sync::{Arc, LazyLock},
};

#[cfg(any(test, feature = "test-helpers"))]
use futures_util::{StreamExt as _, TryStreamExt as _};
use tracedecay_graph_db::GraphCancellation;
use tracedecay_temporal_query::execution::ExecutionControl;

use tracedecay_store::{
    SessionRefreshBeginOrJoinPermit, SessionRefreshBeginOrJoinReceiptV1,
    SessionRefreshBeginOrJoinRequestV1, SessionRefreshCancelPermit,
    SessionRefreshCancellationRequestV1, SessionRefreshCompletePermit,
    SessionRefreshCompletionRequestV1, SessionRefreshFailPermit, SessionRefreshFailureRequestV1,
    SessionRefreshProgressPersistPermit, SessionRefreshProgressReadPermit,
    SessionRefreshProgressRequestV1, SessionRefreshProgressV1, SessionRefreshReceiptReadPermit,
    SessionRefreshReceiptRequestV1, SessionRefreshReceiptV1, SessionRefreshStore,
    SessionRetrievalPageV1, SessionRetrievalStore, SessionSnapshotFreezePermit, SessionStoreResult,
    SessionTemporalCapabilitiesV1, SessionTemporalCapabilityProvider, SessionTemporalCapabilityV1,
    SessionTemporalPageRetrievePermit, SessionTemporalProjectionBatchReceiptV1,
    SessionTemporalProjectionBatchV1, SessionTemporalRetrievalRequestV1,
    SessionTemporalSnapshotRequestV1, SessionTemporalSnapshotV1,
};

use super::refresh::SessionRefreshRecoveryV1;
#[cfg(any(test, feature = "test-helpers"))]
use super::refresh::{
    SessionRefreshBeginBatchItemV1, SessionRefreshBeginBatchOutcomeV1, SessionRefreshRestartStateV1,
};
use crate::handle::{SessionTemporalAccess, SessionTemporalRegisteredDb};
#[cfg(any(test, feature = "test-helpers"))]
use tracedecay_store::SessionStoreError;

/// Session-temporal projection adapter over an already-open authoritative database.
pub struct SessionTemporalStore<'a, D: SessionTemporalRegisteredDb> {
    db: &'a D,
}

/// One pending refresh after the read-only staging half of the materialize
/// pipeline: either a begin ready to fold into the grouped commit, or a
/// session whose durable state the completion loop resumes from.
#[cfg(any(test, feature = "test-helpers"))]
enum StagedSessionRefreshBegin {
    Grouped {
        request: SessionRefreshBeginOrJoinRequestV1,
        recovery: Box<SessionRefreshRecoveryV1>,
        progress: SessionRefreshProgressV1,
        batch: SessionTemporalProjectionBatchV1,
    },
    Resume(tracedecay_domain::SessionId),
}

/// How a staged session finishes once the grouped commit returns.
#[cfg(any(test, feature = "test-helpers"))]
enum StagedSessionRefreshPost {
    /// Index into the grouped commit's items/recoveries vectors.
    Grouped(usize),
    Resume(tracedecay_domain::SessionId),
}

/// How one staged begin completes after the grouped begin commit: a
/// completion request with its rebuilt relation projection ready for the
/// grouped complete commit, a session that resumes through the running
/// loop, or nothing left to do.
#[cfg(any(test, feature = "test-helpers"))]
enum MaterializeCompletionStage {
    Ready {
        session_id: tracedecay_domain::SessionId,
        request: SessionRefreshCompletionRequestV1,
        relation_projection: crate::relations::SessionRelationProjection,
        watermarks: tracedecay_store::SessionFrozenWatermarksV1,
    },
    Resume(tracedecay_domain::SessionId),
    Done,
}

#[derive(Clone, Debug)]
struct ExecutionControlGraphCancellation(ExecutionControl);

impl GraphCancellation for ExecutionControlGraphCancellation {
    fn is_cancelled(&self) -> bool {
        self.0.checkpoint().is_err()
    }
}

/// Adapts the request's live cancellation signal for bounded Grafeo calls.
///
/// `GraphCancellation` has no error channel, so it treats cancellation,
/// deadline expiry, and work-budget exhaustion as an interruption. Every
/// caller must checkpoint the same [`ExecutionControl`] immediately before and
/// after its graph operation to restore the original typed reason. This
/// adapter preserves all three interruption paths during traversal without
/// manufacturing a default control.
pub fn execution_control_graph_cancellation(
    control: &ExecutionControl,
) -> Arc<dyn GraphCancellation> {
    Arc::new(ExecutionControlGraphCancellation(control.clone()))
}

impl<'a, D: SessionTemporalRegisteredDb + Sync> SessionTemporalStore<'a, D> {
    pub const fn new(db: &'a D) -> Self {
        Self { db }
    }

    fn access(&self) -> SessionTemporalAccess<'_, D> {
        SessionTemporalAccess::new(self.db)
    }

    pub async fn persist_session_refresh_projection_batch(
        &self,
        progress: SessionRefreshProgressV1,
        batch: SessionTemporalProjectionBatchV1,
    ) -> SessionStoreResult<(
        SessionRefreshProgressV1,
        SessionTemporalProjectionBatchReceiptV1,
    )> {
        self.access()
            .persist_session_refresh_projection_batch_result(progress, batch)
            .await
    }

    pub async fn persist_session_refresh_projection_batch_controlled(
        &self,
        progress: SessionRefreshProgressV1,
        batch: SessionTemporalProjectionBatchV1,
        execution_control: ExecutionControl,
    ) -> SessionStoreResult<(
        SessionRefreshProgressV1,
        SessionTemporalProjectionBatchReceiptV1,
    )> {
        self.access()
            .persist_session_refresh_projection_batch_controlled_result(
                progress,
                batch,
                execution_control,
            )
            .await
    }

    /// Plans a refresh begin without committing it so the worker can fold
    /// the begin into the first projected batch's commit.
    pub async fn plan_session_refresh_begin(
        &self,
        request: SessionRefreshBeginOrJoinRequestV1,
    ) -> SessionStoreResult<crate::SessionRefreshBeginPlanV1> {
        self.access()
            .plan_session_refresh_begin_result(request)
            .await
    }

    /// Commits a refresh begin and its first projected batch in one write
    /// transaction; see
    /// [`SessionTemporalAccess::commit_session_refresh_begin_batch_result`].
    pub async fn commit_session_refresh_begin_batch(
        &self,
        request: SessionRefreshBeginOrJoinRequestV1,
        accepted_at: tracedecay_domain::UtcMicros,
        progress: SessionRefreshProgressV1,
        batch: SessionTemporalProjectionBatchV1,
        execution_control: ExecutionControl,
    ) -> SessionStoreResult<crate::SessionRefreshBeginBatchOutcomeV1> {
        self.access()
            .commit_session_refresh_begin_batch_result(
                request,
                accepted_at,
                progress,
                batch,
                execution_control,
            )
            .await
    }

    /// Commits several sessions' begin+first-batch folds in one physical
    /// transaction; see
    /// [`SessionTemporalAccess::commit_session_refresh_begin_batch_group`].
    pub async fn commit_session_refresh_begin_batch_group(
        &self,
        items: Vec<crate::SessionRefreshBeginBatchItemV1>,
        execution_control: ExecutionControl,
    ) -> SessionStoreResult<Vec<SessionStoreResult<crate::SessionRefreshBeginBatchOutcomeV1>>> {
        self.access()
            .commit_session_refresh_begin_batch_group(items, execution_control)
            .await
    }

    /// Stages one refresh completion for the grouped commit; see
    /// [`SessionTemporalAccess::stage_session_refresh_completion`].
    #[cfg(any(test, feature = "test-helpers"))]
    pub async fn stage_session_refresh_completion_for_test(
        &self,
        request: SessionRefreshCompletionRequestV1,
    ) -> SessionStoreResult<crate::SessionRefreshCompletionStage> {
        self.access()
            .stage_session_refresh_completion(request, &ExecutionControl::default())
            .await
    }

    /// Commits several sessions' refresh completions in one physical
    /// transaction; see
    /// [`SessionTemporalAccess::complete_session_refresh_group`].
    #[cfg(any(test, feature = "test-helpers"))]
    pub async fn complete_session_refresh_group_for_test(
        &self,
        items: Vec<(
            SessionRefreshCompletionRequestV1,
            crate::relations::SessionRelationProjection,
        )>,
    ) -> SessionStoreResult<Vec<SessionStoreResult<tracedecay_store::SessionRefreshReceiptV1>>>
    {
        self.access()
            .complete_session_refresh_group(items, ExecutionControl::default())
            .await
    }

    /// Records several sessions' pending relation receipts in one
    /// physical commit; see
    /// [`SessionTemporalAccess::record_relation_receipts_group`].
    #[cfg(any(test, feature = "test-helpers"))]
    pub async fn record_relation_receipts_group_for_test(
        &self,
        projections: &[crate::relations::SessionRelationProjection],
    ) -> SessionStoreResult<Vec<SessionStoreResult<tracedecay_graph_db::GraphWatermark>>> {
        self.access()
            .record_relation_receipts_group(projections, &ExecutionControl::default())
            .await
    }

    /// Applies one staged completion's projection to the native relation
    /// graph and verifies the loaded graph; see
    /// [`SessionTemporalAccess::apply_candidate_relation_projection`].
    #[cfg(any(test, feature = "test-helpers"))]
    pub async fn apply_candidate_relation_projection_for_test(
        &self,
        snapshot: &tracedecay_runtime_core::db::DatabaseEngineReadSnapshot,
        projection: &crate::relations::SessionRelationProjection,
    ) -> SessionStoreResult<crate::relations::SessionRelationProjection> {
        self.access()
            .apply_candidate_relation_projection(snapshot, projection, &ExecutionControl::default())
            .await
    }

    /// Runs the post-apply final-receipt scan for one staged completion;
    /// see [`SessionTemporalAccess::validate_staged_session_refresh_completion`].
    #[cfg(any(test, feature = "test-helpers"))]
    pub async fn validate_staged_session_refresh_completion_for_test(
        &self,
        session_id: &tracedecay_domain::SessionId,
        generation: tracedecay_domain::SessionProjectionGenerationV1,
        watermarks: &tracedecay_store::SessionFrozenWatermarksV1,
        projection: &crate::relations::SessionRelationProjection,
    ) -> SessionStoreResult<()> {
        self.access()
            .validate_staged_session_refresh_completion(
                session_id,
                generation,
                watermarks,
                projection,
                &ExecutionControl::default(),
            )
            .await
    }

    pub async fn session_refresh_recovery(
        &self,
        session_id: &tracedecay_domain::SessionId,
    ) -> SessionStoreResult<Option<SessionRefreshRecoveryV1>> {
        self.access()
            .session_refresh_recovery_result(session_id)
            .await
    }

    pub async fn running_session_refreshes(
        &self,
    ) -> SessionStoreResult<Vec<SessionRefreshRecoveryV1>> {
        self.access().running_session_refreshes_result().await
    }

    #[cfg(any(test, feature = "test-helpers"))]
    pub async fn materialize_session_temporal_refresh_batch_for_test(
        &self,
        recovery: &SessionRefreshRecoveryV1,
    ) -> SessionStoreResult<Option<(SessionRefreshProgressV1, SessionTemporalProjectionBatchV1)>>
    {
        self.access()
            .materialize_session_temporal_refresh_batch_result(recovery)
            .await
    }

    #[cfg(any(test, feature = "test-helpers"))]
    pub async fn materialize_pending_session_refresh_for_test(
        &self,
        session_id: &tracedecay_domain::SessionId,
    ) -> SessionStoreResult<()> {
        self.materialize_pending_session_refreshes_for_test(std::slice::from_ref(session_id))
            .await
    }

    #[cfg(any(test, feature = "test-helpers"))]
    pub async fn materialize_pending_session_refreshes_for_test(
        &self,
        session_ids: &[tracedecay_domain::SessionId],
    ) -> SessionStoreResult<()> {
        let mut pending: BTreeSet<_> = session_ids.iter().cloned().collect();
        let mut cursor = crate::SessionTemporalRefreshDiscoveryCursor::default();
        while !pending.is_empty() {
            let (requests, next_cursor, has_more) = SessionTemporalAccess::new(self.db)
                .pending_session_temporal_refresh_page_result(256, 0, &cursor)
                .await?
                .into_parts();
            let matched: Vec<_> = requests
                .into_iter()
                .filter(|request| pending.remove(request.session_id()))
                .collect();
            // Refresh pipelines are independent per session: plan each
            // begin and project its first batch off the writer lane
            // concurrently, then fold every staged begin into one grouped
            // physical commit (each item isolated by its own savepoint and
            // acknowledged only after the commit returns), then drive the
            // per-session completions concurrently again.
            let staged =
                futures_util::stream::iter(matched)
                    .map(|request| async move {
                        self.stage_session_refresh_begin_for_test(request).await
                    })
                    .buffered(8)
                    .try_collect::<Vec<_>>()
                    .await?;
            let mut items = Vec::new();
            let mut recoveries = Vec::new();
            let mut post = Vec::with_capacity(staged.len());
            for staged_begin in staged {
                match staged_begin {
                    StagedSessionRefreshBegin::Grouped {
                        request,
                        recovery,
                        progress,
                        batch,
                    } => {
                        let accepted_at = recovery.accepted_at();
                        post.push(StagedSessionRefreshPost::Grouped(recoveries.len()));
                        recoveries.push(recovery);
                        items.push(SessionRefreshBeginBatchItemV1 {
                            request,
                            accepted_at,
                            progress,
                            batch,
                        });
                    }
                    StagedSessionRefreshBegin::Resume(session_id) => {
                        post.push(StagedSessionRefreshPost::Resume(session_id));
                    }
                }
            }
            let outcomes = if items.is_empty() {
                Vec::new()
            } else {
                self.commit_session_refresh_begin_batch_group(items, ExecutionControl::default())
                    .await?
            };
            // Stage each completion — snapshot preconditions and the
            // candidate relation rebuild — off the writer lane, then fold
            // every staged completion into one grouped physical commit.
            // Items the commit refuses resume through the durable
            // running-refresh loop, the same state an independent refused
            // commit left behind.
            let mut staging: Vec<
                std::pin::Pin<
                    Box<
                        dyn Future<Output = SessionStoreResult<MaterializeCompletionStage>>
                            + Send
                            + '_,
                    >,
                >,
            > = Vec::with_capacity(post.len());
            let mut outcome_iter = outcomes.into_iter();
            for post_item in post {
                match post_item {
                    StagedSessionRefreshPost::Grouped(item_index) => {
                        let outcome = outcome_iter.next().ok_or(
                            SessionStoreError::InvalidStateTransition {
                                context: "test temporal fixture begin batch outcome",
                            },
                        )??;
                        let recovery = recoveries[item_index].as_ref();
                        staging.push(Box::pin(async move {
                            self.stage_materialize_completion_for_test(recovery, outcome)
                                .await
                        }));
                    }
                    StagedSessionRefreshPost::Resume(session_id) => {
                        staging.push(Box::pin(async move {
                            Ok::<_, SessionStoreError>(MaterializeCompletionStage::Resume(
                                session_id,
                            ))
                        }));
                    }
                }
            }
            let staged_finishes = futures_util::stream::iter(staging)
                .buffered(8)
                .try_collect::<Vec<_>>()
                .await?;
            // Fold every staged reconstruction into one grouped receipt
            // commit (per-item savepoints; the durable states sequential
            // receipt commits would leave), apply each committed
            // projection to the native relation graph concurrently off the
            // writer lane, re-run the post-apply receipt scan per session,
            // then fold every surviving completion into one grouped
            // commit. Items refused at any point resume through the
            // durable running-refresh loop, the same state an independent
            // refused or crashed commit left behind.
            let mut ready: Vec<
                Option<(
                    tracedecay_domain::SessionId,
                    SessionRefreshCompletionRequestV1,
                    tracedecay_store::SessionFrozenWatermarksV1,
                )>,
            > = Vec::new();
            let mut projections: Vec<crate::relations::SessionRelationProjection> = Vec::new();
            let mut resumes = Vec::new();
            for staged_finish in staged_finishes {
                match staged_finish {
                    MaterializeCompletionStage::Ready {
                        session_id,
                        request,
                        relation_projection,
                        watermarks,
                    } => {
                        ready.push(Some((session_id, request, watermarks)));
                        projections.push(relation_projection);
                    }
                    MaterializeCompletionStage::Resume(session_id) => {
                        resumes.push(session_id);
                    }
                    MaterializeCompletionStage::Done => {}
                }
            }
            let receipt_outcomes = if projections.is_empty() {
                Vec::new()
            } else {
                self.record_relation_receipts_group_for_test(&projections)
                    .await?
            };
            let mut complete_items = Vec::new();
            let mut complete_sessions = Vec::new();
            if !projections.is_empty() {
                let apply_snapshot = &self.access().read_snapshot().await.map_err(|_| {
                    SessionStoreError::InvalidStateTransition {
                        context: "test temporal fixture apply snapshot",
                    }
                })?;
                for (index, outcome) in receipt_outcomes.iter().enumerate() {
                    if outcome.is_err() {
                        resumes.push(ready[index].as_ref().unwrap().0.clone());
                    }
                }
                let apply_indices: Vec<usize> = receipt_outcomes
                    .iter()
                    .enumerate()
                    .filter_map(|(index, outcome)| outcome.is_ok().then_some(index))
                    .collect();
                let applied = futures_util::stream::iter(apply_indices.iter().map(|&index| {
                    let projection = &projections[index];
                    async move {
                        self.apply_candidate_relation_projection_for_test(
                            apply_snapshot,
                            projection,
                        )
                        .await
                    }
                }))
                .buffered(8)
                .collect::<Vec<_>>()
                .await;
                let mut validated = Vec::new();
                for (&index, result) in apply_indices.iter().zip(applied) {
                    match result {
                        Ok(verified) => validated.push((index, verified)),
                        Err(_) => {
                            resumes.push(ready[index].as_ref().unwrap().0.clone());
                        }
                    }
                }
                let validations =
                    futures_util::stream::iter(validated.iter().map(|(index, verified)| {
                        let (session_id, _, watermarks) = ready[*index].as_ref().unwrap();
                        async move {
                            // The final receipt scan names the candidate
                            // generation the refresh builds, not the base
                            // generation the watermarks froze as active.
                            let generation = tracedecay_domain::SessionProjectionGenerationV1::new(
                                verified.generation,
                            )
                            .map_err(|_| {
                                SessionStoreError::InvalidStateTransition {
                                    context: "test temporal fixture candidate generation",
                                }
                            })?;
                            self.validate_staged_session_refresh_completion_for_test(
                                session_id, generation, watermarks, verified,
                            )
                            .await
                        }
                    }))
                    .buffered(8)
                    .collect::<Vec<_>>()
                    .await;
                for ((index, verified), validation) in validated.into_iter().zip(validations) {
                    match validation {
                        Ok(()) => {
                            let (session_id, request, _) = ready[index].take().unwrap();
                            complete_sessions.push(session_id);
                            complete_items.push((request, verified));
                        }
                        Err(_) => {
                            resumes.push(ready[index].as_ref().unwrap().0.clone());
                        }
                    }
                }
            }
            let receipts = if complete_items.is_empty() {
                Vec::new()
            } else {
                self.complete_session_refresh_group_for_test(complete_items)
                    .await?
            };
            for (session_id, receipt) in complete_sessions.into_iter().zip(receipts) {
                match receipt {
                    Ok(_) => {}
                    Err(_) => resumes.push(session_id),
                }
            }
            futures_util::stream::iter(resumes)
                .map(|session_id| async move {
                    self.complete_running_session_refresh_for_test(&session_id)
                        .await
                })
                .buffered(8)
                .try_collect::<Vec<_>>()
                .await?;
            if !has_more && !pending.is_empty() {
                return Err(SessionStoreError::InvalidStateTransition {
                    context: "test temporal fixture pending refresh",
                });
            }
            cursor = next_cursor;
        }
        Ok(())
    }

    /// Plans one pending refresh begin and projects its first batch — the
    /// read-only half of the materialize pipeline, so it can run
    /// concurrently for a whole discovery page before the grouped commit.
    #[cfg(any(test, feature = "test-helpers"))]
    async fn stage_session_refresh_begin_for_test(
        &self,
        request: tracedecay_store::SessionRefreshBeginOrJoinRequestV1,
    ) -> SessionStoreResult<StagedSessionRefreshBegin> {
        if let crate::SessionRefreshBeginPlanV1::Prepared(recovery) =
            self.plan_session_refresh_begin(request.clone()).await?
        {
            if let Some((progress, batch)) = self
                .materialize_session_temporal_refresh_batch_for_test(&recovery)
                .await?
            {
                return Ok(StagedSessionRefreshBegin::Grouped {
                    request,
                    recovery,
                    progress,
                    batch,
                });
            }
        }
        Ok(StagedSessionRefreshBegin::Resume(
            request.session_id().clone(),
        ))
    }

    /// Stages one completion after the grouped begin commit: the fused
    /// commit already wrote this batch's projection receipt, so
    /// progress.is_complete() here is exactly the state the recovery reader
    /// would return ReadyToComplete for — build the completion request
    /// from memory instead of paying a recovery snapshot plus its reload
    /// queries, then prove the snapshot preconditions and rebuild the
    /// candidate relations off the writer lane. Anything else resumes
    /// from durable state the way the sequential path did.
    #[cfg(any(test, feature = "test-helpers"))]
    async fn stage_materialize_completion_for_test(
        &self,
        recovery: &SessionRefreshRecoveryV1,
        outcome: SessionRefreshBeginBatchOutcomeV1,
    ) -> SessionStoreResult<MaterializeCompletionStage> {
        if let SessionRefreshBeginBatchOutcomeV1::Persisted { progress, .. } = outcome {
            if progress.frontier().is_complete() && progress.committed_batches() > 0 {
                let mut completion = SessionRefreshCompletionRequestV1::new(
                    recovery.operation_id().clone(),
                    recovery.session_id().clone(),
                    progress.frontier(),
                    *progress.coverage(),
                )?;
                if let Some(source_coverage) = progress.source_coverage().cloned().or_else(|| {
                    recovery
                        .source_coverage(progress.frontier().committed_through())
                        .ok()
                }) {
                    completion = completion.with_source_coverage(source_coverage);
                }
                return match self
                    .stage_session_refresh_completion_for_test(completion)
                    .await?
                {
                    crate::SessionRefreshCompletionStage::Durable(_) => {
                        Ok(MaterializeCompletionStage::Done)
                    }
                    crate::SessionRefreshCompletionStage::Ready {
                        request,
                        relation_projection,
                        watermarks,
                    } => Ok(MaterializeCompletionStage::Ready {
                        session_id: recovery.session_id().clone(),
                        request,
                        relation_projection,
                        watermarks,
                    }),
                };
            }
        }
        Ok(MaterializeCompletionStage::Resume(
            recovery.session_id().clone(),
        ))
    }

    /// Projects and completes the refresh already running for `session_id`.
    #[cfg(any(test, feature = "test-helpers"))]
    pub async fn complete_running_session_refresh_for_test(
        &self,
        session_id: &tracedecay_domain::SessionId,
    ) -> SessionStoreResult<()> {
        loop {
            let recovery = self.session_refresh_recovery(session_id).await?.ok_or(
                SessionStoreError::InvalidStateTransition {
                    context: "test temporal fixture refresh recovery",
                },
            )?;
            match recovery.restart_state() {
                SessionRefreshRestartStateV1::ReadyToComplete => {
                    let progress =
                        recovery
                            .progress()
                            .ok_or(SessionStoreError::InvalidStateTransition {
                                context: "test temporal fixture refresh progress",
                            })?;
                    let mut request = SessionRefreshCompletionRequestV1::new(
                        recovery.operation_id().clone(),
                        recovery.session_id().clone(),
                        progress.frontier(),
                        *progress.coverage(),
                    )?;
                    if let Some(source_coverage) =
                        progress.source_coverage().cloned().or_else(|| {
                            recovery
                                .source_coverage(progress.frontier().committed_through())
                                .ok()
                        })
                    {
                        request = request.with_source_coverage(source_coverage);
                    }
                    self.complete_session_refresh(request, ExecutionControl::default())
                        .await?;
                    return Ok(());
                }
                SessionRefreshRestartStateV1::BeginProjection
                | SessionRefreshRestartStateV1::ResumeProjection { .. } => {
                    let (progress, batch) = self
                        .access()
                        .materialize_session_temporal_refresh_batch_result(&recovery)
                        .await?
                        .ok_or(SessionStoreError::InvalidStateTransition {
                            context: "test temporal fixture projection batch",
                        })?;
                    self.persist_session_refresh_projection_batch(progress, batch)
                        .await?;
                }
            }
        }
    }
}

impl<D: SessionTemporalRegisteredDb + Sync> SessionTemporalCapabilityProvider
    for SessionTemporalStore<'_, D>
{
    fn session_temporal_capabilities(&self) -> &SessionTemporalCapabilitiesV1 {
        static CAPABILITIES: LazyLock<SessionTemporalCapabilitiesV1> = LazyLock::new(|| {
            SessionTemporalCapabilitiesV1::new([
                SessionTemporalCapabilityV1::FrozenWatermarks,
                SessionTemporalCapabilityV1::RefreshJoin,
                SessionTemporalCapabilityV1::RefreshProgressPersistence,
                SessionTemporalCapabilityV1::RefreshCancellation,
            ])
        });
        &CAPABILITIES
    }
}

impl<D: SessionTemporalRegisteredDb + Sync> SessionRetrievalStore for SessionTemporalStore<'_, D> {
    fn freeze_session_temporal_snapshot_supported(
        &self,
        _permit: SessionSnapshotFreezePermit,
        request: SessionTemporalSnapshotRequestV1,
    ) -> impl Future<Output = SessionStoreResult<SessionTemporalSnapshotV1>> + Send {
        let db = self.db;
        async move {
            SessionTemporalAccess::new(db)
                .freeze_session_temporal_snapshot_result(request)
                .await
        }
    }

    fn retrieve_session_temporal_page_supported(
        &self,
        _permit: SessionTemporalPageRetrievePermit,
        request: SessionTemporalRetrievalRequestV1,
    ) -> impl Future<Output = SessionStoreResult<SessionRetrievalPageV1>> + Send {
        let db = self.db;
        async move {
            SessionTemporalAccess::new(db)
                .retrieve_session_temporal_page_result(request)
                .await
        }
    }
}

impl<D: SessionTemporalRegisteredDb + Sync> SessionRefreshStore for SessionTemporalStore<'_, D> {
    fn begin_or_join_session_refresh_supported(
        &self,
        _permit: SessionRefreshBeginOrJoinPermit,
        request: SessionRefreshBeginOrJoinRequestV1,
    ) -> impl Future<Output = SessionStoreResult<SessionRefreshBeginOrJoinReceiptV1>> + Send {
        let db = self.db;
        async move {
            SessionTemporalAccess::new(db)
                .begin_or_join_session_refresh_result(request)
                .await
        }
    }

    fn persist_session_refresh_progress_supported(
        &self,
        _permit: SessionRefreshProgressPersistPermit,
        progress: SessionRefreshProgressV1,
    ) -> impl Future<Output = SessionStoreResult<SessionRefreshProgressV1>> + Send {
        let db = self.db;
        async move {
            SessionTemporalAccess::new(db)
                .persist_session_refresh_progress_result(progress)
                .await
        }
    }

    fn session_refresh_progress_supported(
        &self,
        _permit: SessionRefreshProgressReadPermit,
        request: SessionRefreshProgressRequestV1,
    ) -> impl Future<Output = SessionStoreResult<Option<SessionRefreshProgressV1>>> + Send {
        let db = self.db;
        async move {
            SessionTemporalAccess::new(db)
                .session_refresh_progress_result(request)
                .await
        }
    }

    fn complete_session_refresh_supported(
        &self,
        _permit: SessionRefreshCompletePermit,
        request: SessionRefreshCompletionRequestV1,
        execution_control: ExecutionControl,
    ) -> impl Future<Output = SessionStoreResult<SessionRefreshReceiptV1>> + Send {
        let db = self.db;
        async move {
            SessionTemporalAccess::new(db)
                .complete_session_refresh_result(request, execution_control)
                .await
        }
    }

    fn fail_session_refresh_supported(
        &self,
        _permit: SessionRefreshFailPermit,
        request: SessionRefreshFailureRequestV1,
    ) -> impl Future<Output = SessionStoreResult<SessionRefreshReceiptV1>> + Send {
        let db = self.db;
        async move {
            SessionTemporalAccess::new(db)
                .fail_session_refresh_result(request)
                .await
        }
    }

    fn cancel_session_refresh_supported(
        &self,
        _permit: SessionRefreshCancelPermit,
        request: SessionRefreshCancellationRequestV1,
    ) -> impl Future<Output = SessionStoreResult<SessionRefreshReceiptV1>> + Send {
        let db = self.db;
        async move {
            SessionTemporalAccess::new(db)
                .cancel_session_refresh_result(request)
                .await
        }
    }

    fn session_refresh_receipt_supported(
        &self,
        _permit: SessionRefreshReceiptReadPermit,
        request: SessionRefreshReceiptRequestV1,
    ) -> impl Future<Output = SessionStoreResult<Option<SessionRefreshReceiptV1>>> + Send {
        let db = self.db;
        async move {
            SessionTemporalAccess::new(db)
                .session_refresh_receipt_result(request)
                .await
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn graph_cancellation_observes_the_callers_execution_control() {
        let control = tracedecay_temporal_query::execution::ExecutionControl::default();
        let cancellation = execution_control_graph_cancellation(&control);

        assert!(!cancellation.is_cancelled());
        control.cancel();
        assert!(cancellation.is_cancelled());
    }

    #[test]
    fn graph_cancellation_observes_deadlines_and_work_budgets() {
        let expired = tracedecay_temporal_query::execution::ExecutionControl::new(Some(
            std::time::Instant::now(),
        ));
        assert!(execution_control_graph_cancellation(&expired).is_cancelled());

        let budgeted =
            tracedecay_temporal_query::execution::ExecutionControl::default().with_work_limit(1);
        let cancellation = execution_control_graph_cancellation(&budgeted);
        assert!(!cancellation.is_cancelled());
        assert!(cancellation.is_cancelled());
    }
}
