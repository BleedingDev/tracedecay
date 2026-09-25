//! Canonical projection inputs for the per-project semantic runtime.

use std::collections::BTreeSet;
use std::sync::{Arc, Mutex};

use tracedecay_code_index::embedding_document::{
    EmbeddingDocumentComposerV1, EmbeddingSymbolContextIndexV1,
};
use tracedecay_code_index::production::CodeIndexPublishedGenerationV1;
use tracedecay_code_index::projection::expected_request_digest;
use tracedecay_domain::{
    ChangedCodeChunkSetV1, ChangedCodeChunkV1, CodeSearchChunkV1, ProjectionBatchRequestV1,
    ProjectionReplayReasonV1,
};
use tracedecay_semantic::{
    FastEmbedSemanticGenerationRequestV1, LoadedSemanticArtifactV1,
    PreparedSemanticRuntimeCommitV1, SemanticProjectionResumeOutcomeV1,
};
use tracedecay_semantic_contracts::{
    SemanticGenerationPointerV1, SemanticRuntimeScheduleFailureV1,
};
use tracedecay_vector_authority::{
    VectorGenerationBuildIdV1, VectorGenerationIdV1, VectorGenerationPlanV1,
    VectorProjectionCheckpointV1,
};

use super::runtime::{
    ProjectSemanticQuerySnapshotV1, ProjectSemanticRuntimeErrorV1, ProjectSemanticRuntimeV1,
    prepare_query_snapshot,
};

#[derive(Debug, Default)]
struct ProjectionCommitStateV1 {
    build: Option<VectorGenerationBuildIdV1>,
    checkpoint: Option<VectorProjectionCheckpointV1>,
    expected_active: Option<tracedecay_domain::VectorGenerationIdV1>,
    published: Option<tracedecay_domain::VectorGenerationIdV1>,
}

#[derive(Clone)]
struct ProjectionInstallCompensationV1 {
    expected_new: VectorGenerationIdV1,
    previous_active: Option<VectorGenerationIdV1>,
    vector_changed: bool,
    previous_query: Option<Arc<ProjectSemanticQuerySnapshotV1>>,
}

/// Convert the durable stage head into the scheduler's resume position and
/// next batch CAS token. A zero-batch stage must pass no checkpoint to its
/// first commit; a nonzero stage must preserve the exact durable checkpoint.
pub(super) fn resume_projection_checkpoint(
    checkpoint: Option<VectorProjectionCheckpointV1>,
) -> Result<
    (
        Option<VectorProjectionCheckpointV1>,
        SemanticProjectionResumeOutcomeV1,
    ),
    SemanticRuntimeScheduleFailureV1,
> {
    let checkpoint = checkpoint.ok_or(SemanticRuntimeScheduleFailureV1::Publication)?;
    let completed_batches = checkpoint.completed_batches;
    if completed_batches == 0 {
        Ok((None, SemanticProjectionResumeOutcomeV1::ReplayFromStart))
    } else {
        Ok((
            Some(checkpoint),
            SemanticProjectionResumeOutcomeV1::CompletedBatches(completed_batches),
        ))
    }
}

impl ProjectSemanticRuntimeV1 {
    /// Queue projection through the semantic handle. The call performs no
    /// embedding or model loading on the caller's task.
    pub async fn schedule_generation(
        &self,
        generation: Arc<CodeIndexPublishedGenerationV1>,
    ) -> Result<bool, ProjectSemanticRuntimeErrorV1> {
        let runtime = self.clone();
        let request =
            tokio::task::spawn_blocking(move || runtime.prepare_generation_request(generation))
                .await
                .map_err(|_| ProjectSemanticRuntimeErrorV1::TaskJoin)??;
        Ok(self.handle.schedule_generation(request))
    }

    fn prepare_generation_request(
        &self,
        generation: Arc<CodeIndexPublishedGenerationV1>,
    ) -> Result<FastEmbedSemanticGenerationRequestV1, ProjectSemanticRuntimeErrorV1> {
        if !self.config.enabled {
            return Err(ProjectSemanticRuntimeErrorV1::Runtime(
                SemanticRuntimeScheduleFailureV1::Artifact,
            ));
        }
        if self.cancellation.is_cancelled() {
            return Err(ProjectSemanticRuntimeErrorV1::Runtime(
                SemanticRuntimeScheduleFailureV1::Cancelled,
            ));
        }
        let projection = LoadedSemanticArtifactV1::lifecycle_projection(
            &self.lifecycle,
            generation.manifest(),
            self.config.resources,
            self.config.document_composition,
        )?;
        let lifecycle_target = self
            .lifecycle
            .lifecycle_mutation_target_for_projection(&projection)
            .ok_or(SemanticRuntimeScheduleFailureV1::Artifact)?;
        let current = self.handle.current();
        let request = projection_request(&generation, &projection, current.as_ref())?;
        let canonical_chunks = canonical_projection_chunks(&generation, &request);
        let expected_chunk_ids = generation
            .chunks()
            .chunks()
            .iter()
            .map(|chunk| chunk.id.clone())
            .collect::<Vec<_>>();
        let base_generation = request
            .previous_projection_key
            .as_ref()
            .and_then(|_| current.as_ref().map(|pointer| pointer.generation.clone()));
        let plan = VectorGenerationPlanV1::new(
            &projection,
            request.changes.to_generation.clone(),
            request.changes.manifest_digest.clone(),
            expected_chunk_ids,
            base_generation,
        )?;
        let target_vector_generation = plan.generation_id()?;
        let target_source_generation = request.changes.to_generation.clone();
        let target_projection_key = request.target_projection_key.clone();
        let documents = embedding_documents(&generation);
        let resources = self.config.resources;
        let max_embeds_per_batch =
            usize::try_from(self.config.resources.max_batch_size).map_err(|_| {
                ProjectSemanticRuntimeErrorV1::Configuration(
                    "semantic max_batch_size does not fit this target".to_owned(),
                )
            })?;
        let state = Arc::new(Mutex::new(ProjectionCommitStateV1::default()));
        let compensation = Arc::new(Mutex::new(None));
        let resume_state = Arc::clone(&state);
        let commit_state = Arc::clone(&state);
        let publish_state = state;
        let resume_store = self.vector_store.clone();
        let commit_store = self.vector_store.clone();
        let publish_store = self.vector_store.clone();
        let publish_compensation = compensation;
        let publish_query_snapshot = Arc::clone(&self.query_snapshot);
        let publish_code_generation = Arc::clone(&generation);
        let resume_cancellation = self.cancellation.clone();
        let commit_cancellation = self.cancellation.clone();
        let publish_cancellation = self.cancellation.clone();
        let load_lifecycle = Arc::clone(&self.lifecycle);
        let bound_lifecycle = Arc::clone(&self.lifecycle);
        FastEmbedSemanticGenerationRequestV1::new(
            target_source_generation.clone(),
            request,
            canonical_chunks,
            documents,
            max_embeds_per_batch,
            move || {
                LoadedSemanticArtifactV1::from_lifecycle_projection(
                    &load_lifecycle,
                    &projection,
                    resources,
                )
            },
            move || async move {
                tokio::task::spawn_blocking(move || {
                    if resume_cancellation.is_cancelled() {
                        return Err(SemanticRuntimeScheduleFailureV1::Cancelled);
                    }
                    let snapshot = resume_store
                        .snapshot()
                        .map_err(SemanticRuntimeScheduleFailureV1::projection)?;
                    let expected_active = snapshot.active_generation().cloned();
                    if snapshot.generation(&target_vector_generation).is_some() {
                        let mut state = resume_state
                            .lock()
                            .map_err(|_| SemanticRuntimeScheduleFailureV1::Runtime)?;
                        state.expected_active = expected_active;
                        state.published = Some(target_vector_generation);
                        return Ok(SemanticProjectionResumeOutcomeV1::AlreadyPublished);
                    }
                    drop(snapshot);
                    let build = resume_store
                        .begin_generation(plan)
                        .map_err(SemanticRuntimeScheduleFailureV1::projection)?;
                    let checkpoint = resume_store
                        .snapshot()
                        .map_err(SemanticRuntimeScheduleFailureV1::projection)?
                        .staged_checkpoint(&build)
                        .cloned();
                    let (checkpoint, resume_outcome) = resume_projection_checkpoint(checkpoint)?;
                    let mut state = resume_state
                        .lock()
                        .map_err(|_| SemanticRuntimeScheduleFailureV1::Runtime)?;
                    state.build = Some(build);
                    state.checkpoint = checkpoint;
                    state.expected_active = expected_active;
                    Ok(resume_outcome)
                })
                .await
                .map_err(|_| SemanticRuntimeScheduleFailureV1::Runtime)?
            },
            move |prepared| {
                let state = Arc::clone(&commit_state);
                let store = commit_store.clone();
                let cancellation = commit_cancellation.clone();
                async move {
                    tokio::task::spawn_blocking(move || {
                        if cancellation.is_cancelled() {
                            return Err(SemanticRuntimeScheduleFailureV1::Cancelled);
                        }
                        let mut state = state
                            .lock()
                            .map_err(|_| SemanticRuntimeScheduleFailureV1::Runtime)?;
                        let build = state
                            .build
                            .clone()
                            .ok_or(SemanticRuntimeScheduleFailureV1::Publication)?;
                        let checkpoint = store
                            .commit_batch(&build, state.checkpoint.as_ref(), prepared)
                            .map_err(SemanticRuntimeScheduleFailureV1::projection)?;
                        state.checkpoint = Some(checkpoint);
                        Ok(())
                    })
                    .await
                    .map_err(|_| SemanticRuntimeScheduleFailureV1::Runtime)?
                }
            },
            move || async move {
                let (build, expected_active, published) = {
                    let state = publish_state
                        .lock()
                        .map_err(|_| SemanticRuntimeScheduleFailureV1::Runtime)?;
                    (
                        state.build.clone(),
                        state.expected_active.clone(),
                        state.published.clone(),
                    )
                };
                let commit_store = publish_store.clone();
                let compensation_store = publish_store;
                let commit_compensation = Arc::clone(&publish_compensation);
                let commit_query_snapshot = Arc::clone(&publish_query_snapshot);
                Ok(PreparedSemanticRuntimeCommitV1::new(move || async move {
                    tokio::task::spawn_blocking(move || {
                        if publish_cancellation.is_cancelled() {
                            return Err(SemanticRuntimeScheduleFailureV1::Cancelled);
                        }
                        let (generation, previous_active, vector_changed) =
                            if let Some(generation) = published {
                                let changed = expected_active.as_ref() != Some(&generation);
                                commit_store
                                    .activate_generation_if_current(
                                        &generation,
                                        expected_active.as_ref(),
                                    )
                                    .map_err(SemanticRuntimeScheduleFailureV1::publication)?;
                                (generation, expected_active, changed)
                            } else {
                                let build = build
                                    .as_ref()
                                    .ok_or(SemanticRuntimeScheduleFailureV1::Publication)?;
                                let publication = commit_store
                                    .publish_generation_if_current(build, expected_active.as_ref())
                                    .map_err(SemanticRuntimeScheduleFailureV1::publication)?;
                                (
                                    publication.generation_id.clone(),
                                    publication.previous_active_generation,
                                    true,
                                )
                            };
                        let pointer = SemanticGenerationPointerV1 {
                            generation,
                            source_generation: target_source_generation,
                            projection_key: target_projection_key,
                        };
                        let authority = commit_store
                            .snapshot()
                            .map_err(SemanticRuntimeScheduleFailureV1::publication)?;
                        let prepared_query = match prepare_query_snapshot(
                            &authority,
                            &pointer,
                            publish_code_generation,
                        ) {
                            Ok(prepared) => prepared,
                            Err(error) => {
                                if vector_changed {
                                    commit_store
                                        .restore_active_generation_if_current(
                                            &pointer.generation,
                                            previous_active.as_ref(),
                                        )
                                        .map_err(SemanticRuntimeScheduleFailureV1::publication)?;
                                }
                                return Err(error);
                            }
                        };
                        let mut query_snapshot = match commit_query_snapshot.lock() {
                            Ok(snapshot) => snapshot,
                            Err(_) => {
                                if vector_changed {
                                    commit_store
                                        .restore_active_generation_if_current(
                                            &pointer.generation,
                                            previous_active.as_ref(),
                                        )
                                        .map_err(SemanticRuntimeScheduleFailureV1::publication)?;
                                }
                                return Err(SemanticRuntimeScheduleFailureV1::Runtime);
                            }
                        };
                        let mut compensation = match commit_compensation.lock() {
                            Ok(compensation) => compensation,
                            Err(_) => {
                                drop(query_snapshot);
                                if vector_changed {
                                    commit_store
                                        .restore_active_generation_if_current(
                                            &pointer.generation,
                                            previous_active.as_ref(),
                                        )
                                        .map_err(SemanticRuntimeScheduleFailureV1::publication)?;
                                }
                                return Err(SemanticRuntimeScheduleFailureV1::Runtime);
                            }
                        };
                        let previous_query = query_snapshot.replace(prepared_query);
                        *compensation = Some(ProjectionInstallCompensationV1 {
                            expected_new: pointer.generation.clone(),
                            previous_active,
                            vector_changed,
                            previous_query,
                        });
                        Ok(pointer)
                    })
                    .await
                    .map_err(|_| SemanticRuntimeScheduleFailureV1::Runtime)?
                })
                .on_install_failure(move |pointer, _reason| {
                    let Some(compensation) = publish_compensation
                        .lock()
                        .map_err(|_| SemanticRuntimeScheduleFailureV1::Runtime)?
                        .take()
                    else {
                        return Ok(());
                    };
                    if pointer.generation != compensation.expected_new {
                        return Err(SemanticRuntimeScheduleFailureV1::Publication);
                    }
                    if compensation.vector_changed {
                        compensation_store
                            .restore_active_generation_if_current(
                                &compensation.expected_new,
                                compensation.previous_active.as_ref(),
                            )
                            .map_err(SemanticRuntimeScheduleFailureV1::publication)?;
                    }
                    *publish_query_snapshot
                        .lock()
                        .map_err(|_| SemanticRuntimeScheduleFailureV1::Runtime)? =
                        compensation.previous_query;
                    Ok(())
                }))
            },
        )
        .map(|request| request.with_lifecycle_target(bound_lifecycle, lifecycle_target))
        .map_err(Into::into)
    }
}

pub(super) fn projection_request(
    generation: &CodeIndexPublishedGenerationV1,
    projection: &tracedecay_domain::AdmittedEmbeddingProjectionKeyV1,
    current: Option<&SemanticGenerationPointerV1>,
) -> Result<ProjectionBatchRequestV1, SemanticRuntimeScheduleFailureV1> {
    let source = generation.projection().request();
    let incremental = current.is_some_and(|pointer| {
        source.changes.from_generation.as_ref() == Some(&pointer.source_generation)
            && projection.projection_key() == &pointer.projection_key
    });
    let mut changes = if incremental {
        source.changes.clone()
    } else {
        let (reused_count, reused_digest) = ChangedCodeChunkSetV1::seal_reused_partition(&[])
            .map_err(SemanticRuntimeScheduleFailureV1::projection)?;
        ChangedCodeChunkSetV1 {
            from_generation: None,
            to_generation: generation.manifest().generation_id.clone(),
            manifest_digest: source.changes.manifest_digest.clone(),
            added_or_changed: generation
                .chunks()
                .chunks()
                .iter()
                .map(|chunk| ChangedCodeChunkV1 {
                    chunk_id: chunk.id.clone(),
                    prior_digest: None,
                    current_digest: Some(chunk.content_digest.clone()),
                })
                .collect(),
            deleted: Vec::new(),
            reused_count,
            reused_digest,
        }
    };
    changes.manifest_digest = changes
        .compute_digest()
        .map_err(SemanticRuntimeScheduleFailureV1::projection)?;
    changes
        .validate()
        .map_err(SemanticRuntimeScheduleFailureV1::projection)?;
    let mut request = ProjectionBatchRequestV1 {
        request_digest: changes.manifest_digest.clone(),
        changes,
        previous_projection_key: incremental.then(|| projection.projection_key().clone()),
        target_projection_key: projection.projection_key().clone(),
        replay_reason: if incremental {
            ProjectionReplayReasonV1::SourceEdit
        } else {
            ProjectionReplayReasonV1::FullRebuildIncompatible
        },
    };
    request.request_digest =
        expected_request_digest(&request).map_err(SemanticRuntimeScheduleFailureV1::projection)?;
    Ok(request)
}

pub(super) fn canonical_projection_chunks(
    generation: &CodeIndexPublishedGenerationV1,
    request: &ProjectionBatchRequestV1,
) -> Vec<Arc<CodeSearchChunkV1>> {
    let changed_ids = request
        .changes
        .added_or_changed
        .iter()
        .map(|change| &change.chunk_id)
        .collect::<BTreeSet<_>>();
    generation
        .chunks()
        .chunks()
        .iter()
        .filter(|chunk| changed_ids.contains(&chunk.id))
        .cloned()
        .collect()
}

pub(super) fn embedding_documents(
    generation: &CodeIndexPublishedGenerationV1,
) -> Arc<EmbeddingDocumentComposerV1> {
    Arc::new(EmbeddingDocumentComposerV1::new(
        EmbeddingSymbolContextIndexV1::from_generation_symbols(generation.symbols()),
    ))
}
