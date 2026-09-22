//! Production semantic search-quality orchestration.
//!
//! The evaluator owns the workload, answer key, lifecycle matrix, and evidence
//! checks. It does not own a model, projection, vector index, or ranking
//! source. Every ranking in a report must be returned by an application
//! [`SemanticAblationProductionAuthorityV1`]. The compatibility entry point
//! [`generate_semantic_ablation_runs`] intentionally fails closed: an artifact
//! is an identity pin and cannot be used as a ranking replay file.

use std::fs;
use std::path::{Path, PathBuf};
use std::time::Instant;

use serde::de::DeserializeOwned;
use tracedecay_query::search_quality::candidate_output::canonical_sha256;
use tracedecay_query::search_quality::semantic_ablation::{
    SEMANTIC_ABLATION_ARTIFACT_RELATIVE, SEMANTIC_ABLATION_CORPUS_RELATIVE,
    SEMANTIC_ABLATION_LABELS_RELATIVE, SEMANTIC_ABLATION_MODEL_MANIFEST_RELATIVE,
    SEMANTIC_ABLATION_RANKING_DIGEST_DOMAIN, SEMANTIC_ABLATION_REPETITIONS,
    SEMANTIC_ABLATION_SCHEMA_VERSION, SEMANTIC_ABLATION_WORKLOAD_RELATIVE,
    SemanticAblationArtifactV1, SemanticAblationErrorV1, SemanticAblationExecutionRequestV1,
    SemanticAblationInputDigestsV1, SemanticAblationLifecycleRequestV1, SemanticAblationModeV1,
    SemanticAblationProductionAuthorityV1, SemanticAblationRepetitionKindV1,
    SemanticAblationReportV1, SemanticAblationRunV1, SemanticAblationWorkloadV1,
    evaluate_semantic_ablation, semantic_ablation_authority_identity_digest,
    semantic_ablation_production_receipt_digest, validate_semantic_ablation_inputs,
    validate_semantic_model_manifest,
};

use crate::SearchEvalError;

/// The old signature is retained as a hard failure for downstream callers that
/// have not yet supplied the production adapter. Keeping this function rather
/// than silently changing its meaning prevents an artifact ranking map from
/// becoming an accidental test oracle.
pub fn generate_semantic_ablation_runs(
    _workload: &SemanticAblationWorkloadV1,
    _artifact: &SemanticAblationArtifactV1,
    _digests: &SemanticAblationInputDigestsV1,
) -> Result<Vec<SemanticAblationRunV1>, SearchEvalError> {
    Err(contract(
        "production semantic authority is required; semantic artifacts contain no rankings",
    ))
}

/// Execute the complete fixed semantic matrix through the production
/// authority. Labels are intentionally absent from this API and never reach
/// the authority.
pub fn generate_semantic_ablation_runs_with_authority(
    workload: &SemanticAblationWorkloadV1,
    artifact: &SemanticAblationArtifactV1,
    digests: &SemanticAblationInputDigestsV1,
    authority: &mut dyn SemanticAblationProductionAuthorityV1,
) -> Result<Vec<SemanticAblationRunV1>, SearchEvalError> {
    if workload.schema_version != SEMANTIC_ABLATION_SCHEMA_VERSION
        || artifact.schema_version != SEMANTIC_ABLATION_SCHEMA_VERSION
        || artifact.workload_id != workload.workload_id
        || artifact.workload_digest != digests.workload_digest
        || artifact.identity != workload.expected_artifact_identity
    {
        return Err(contract(
            "semantic generator inputs are not pinned together",
        ));
    }
    if SemanticAblationRepetitionKindV1::ALL
        .into_iter()
        .any(|kind| workload.repetition_contract.count(kind) != SEMANTIC_ABLATION_REPETITIONS)
    {
        return Err(contract(
            "semantic generator requires exactly ten cold, warm, and restart repetitions",
        ));
    }
    if authority.authority_identity() != &artifact.identity {
        return Err(contract(
            "production semantic authority identity does not match the pinned artifact",
        ));
    }
    let expected_authority_digest = semantic_ablation_authority_identity_digest(&artifact.identity)
        .map_err(map_semantic_error)?;
    let mut runs = Vec::with_capacity(
        SemanticAblationModeV1::ALL.len()
            * workload.queries.len()
            * SemanticAblationRepetitionKindV1::ALL.len()
            * SEMANTIC_ABLATION_REPETITIONS as usize,
    );

    for mode in SemanticAblationModeV1::ALL {
        for query in &workload.queries {
            for kind in SemanticAblationRepetitionKindV1::ALL {
                for repetition_index in 0..workload.repetition_contract.count(kind) {
                    let lifecycle_request = SemanticAblationLifecycleRequestV1 {
                        workload_digest: &digests.workload_digest,
                        corpus_digest: &digests.corpus_digest,
                        query,
                        mode,
                        repetition_kind: kind,
                        repetition_index,
                        artifact: &artifact.identity,
                    };
                    let lifecycle = match kind {
                        SemanticAblationRepetitionKindV1::Cold => {
                            authority.reset_for_cold(&lifecycle_request)
                        }
                        SemanticAblationRepetitionKindV1::Warm => {
                            authority.prepare_warm(&lifecycle_request)
                        }
                        SemanticAblationRepetitionKindV1::Restart => {
                            authority.restart_runtime(&lifecycle_request)
                        }
                    }
                    .map_err(map_semantic_error)?;
                    let request = SemanticAblationExecutionRequestV1 {
                        workload_digest: &digests.workload_digest,
                        corpus_digest: &digests.corpus_digest,
                        query,
                        mode,
                        repetition_kind: kind,
                        repetition_index,
                        lifecycle: &lifecycle,
                        artifact: &artifact.identity,
                    };
                    let started = Instant::now();
                    let result = authority.execute(request).map_err(map_semantic_error)?;
                    let latency_micros = started.elapsed().as_micros().max(1);
                    let latency_micros = u64::try_from(latency_micros).unwrap_or(u64::MAX);
                    let ranking_digest = canonical_sha256(&(
                        SEMANTIC_ABLATION_RANKING_DIGEST_DOMAIN,
                        &result.ranked_candidates,
                    ))
                    .map_err(|error| contract(format!("hash semantic ranking: {error}")))?;
                    if result.authority_identity_digest != expected_authority_digest {
                        return Err(contract(format!(
                            "{} returned a foreign semantic authority identity",
                            query.query_id
                        )));
                    }
                    let expected_receipt = semantic_ablation_production_receipt_digest(
                        &artifact.identity,
                        &request,
                        &result,
                        &ranking_digest,
                    )
                    .map_err(map_semantic_error)?;
                    if result.production_receipt_digest != expected_receipt {
                        return Err(contract(format!(
                            "{} returned a production receipt that does not bind its output",
                            query.query_id
                        )));
                    }
                    runs.push(SemanticAblationRunV1 {
                        schema_version: SEMANTIC_ABLATION_SCHEMA_VERSION,
                        run_id: format!(
                            "{}/{}/{}/{}/{:02}",
                            mode.as_str(),
                            query.partition,
                            query.query_id,
                            kind.as_str(),
                            repetition_index
                        ),
                        query_id: query.query_id.clone(),
                        partition: query.partition.clone(),
                        stratum: query.stratum.clone(),
                        mode,
                        repetition_kind: kind,
                        repetition_index,
                        workload_digest: digests.workload_digest.clone(),
                        corpus_digest: digests.corpus_digest.clone(),
                        labels_digest: digests.labels_digest.clone(),
                        model_digest: digests.model_digest.clone(),
                        projection_digest: digests.projection_digest.clone(),
                        vector_generation_digest: digests.vector_generation_digest.clone(),
                        index_digest: digests.index_digest.clone(),
                        source_digest: digests.source_digest.clone(),
                        artifact_manifest_digest: digests.artifact_manifest_digest.clone(),
                        artifact_status: result.artifact_status,
                        artifact_mismatch: result.artifact_mismatch,
                        fallback_used: result.fallback_used,
                        disabled_lanes: result.disabled_lanes,
                        invocation_counters: result.invocation_counters,
                        authority_identity_digest: result.authority_identity_digest,
                        lifecycle,
                        latency_micros,
                        failure: result.failure,
                        production_receipt_digest: result.production_receipt_digest,
                        ranking_digest,
                        ranked_candidates: result.ranked_candidates,
                        quality: None,
                        status: None,
                    });
                }
            }
        }
    }
    Ok(runs)
}

/// Generate through an injected production authority and score only after all
/// rankings have been retained. The hidden labels are passed to the evaluator,
/// never to [`generate_semantic_ablation_runs_with_authority`].
pub fn run_semantic_ablation_with_authority(
    workload: &SemanticAblationWorkloadV1,
    labels: &tracedecay_query::search_quality::semantic_ablation::SemanticAblationLabelsV1,
    artifact: &SemanticAblationArtifactV1,
    corpus_digest: &str,
    digests: &SemanticAblationInputDigestsV1,
    authority: &mut dyn SemanticAblationProductionAuthorityV1,
) -> Result<SemanticAblationReportV1, SearchEvalError> {
    let validated = validate_semantic_ablation_inputs(workload, labels, artifact, corpus_digest)
        .map_err(map_semantic_error)?;
    if &validated != digests {
        return Err(contract("semantic generator digest set is stale"));
    }
    let runs =
        generate_semantic_ablation_runs_with_authority(workload, artifact, digests, authority)?;
    evaluate_semantic_ablation(workload, labels, artifact, corpus_digest, &runs)
        .map_err(map_semantic_error)
}

/// Run the packaged inputs through an injected application authority.
pub fn run_default_semantic_ablation_with_authority(
    authority: &mut dyn SemanticAblationProductionAuthorityV1,
) -> Result<SemanticAblationReportV1, SearchEvalError> {
    let (workload, labels, artifact, digests) =
        tracedecay_query::search_quality::semantic_ablation::packaged::inputs()
            .map_err(map_semantic_error)?;
    run_semantic_ablation_with_authority(
        &workload,
        &labels,
        &artifact,
        &digests.corpus_digest,
        &digests,
        authority,
    )
}

/// The packaged command cannot invent an authority. Product composition must
/// call [`run_default_semantic_ablation_with_authority`] after constructing the
/// real semantic runtime adapter.
pub fn run_default_semantic_ablation() -> Result<SemanticAblationReportV1, SearchEvalError> {
    let _ = tracedecay_query::search_quality::semantic_ablation::packaged::inputs()
        .map_err(map_semantic_error)?;
    Err(contract(
        "production semantic authority is required; no replay or fallback path is available",
    ))
}

/// Run explicit files through an injected application authority. The product
/// model manifest is resolved from the repository root containing the artifact,
/// and its bytes are validated before any authority call is made.
pub fn run_semantic_ablation_from_files_with_authority(
    workload_path: &Path,
    labels_path: &Path,
    artifact_path: &Path,
    corpus_path: &Path,
    authority: &mut dyn SemanticAblationProductionAuthorityV1,
) -> Result<SemanticAblationReportV1, SearchEvalError> {
    let (workload, labels, artifact, corpus_digest, digests) =
        load_file_inputs(workload_path, labels_path, artifact_path, corpus_path)?;
    run_semantic_ablation_with_authority(
        &workload,
        &labels,
        &artifact,
        &corpus_digest,
        &digests,
        authority,
    )
}

/// Explicit-file compatibility entry point. It validates all pins, including
/// the product model manifest, and then fails closed until product composition
/// supplies a production authority.
pub fn run_semantic_ablation_from_files(
    workload_path: &Path,
    labels_path: &Path,
    artifact_path: &Path,
    corpus_path: &Path,
) -> Result<SemanticAblationReportV1, SearchEvalError> {
    let _ = load_file_inputs(workload_path, labels_path, artifact_path, corpus_path)?;
    Err(contract(
        "production semantic authority is required; explicit artifacts cannot replay rankings",
    ))
}

/// Resolve the four evaluator assets below one repository root.
pub fn semantic_ablation_paths(repo_root: &Path) -> (PathBuf, PathBuf, PathBuf, PathBuf) {
    (
        repo_root.join(SEMANTIC_ABLATION_WORKLOAD_RELATIVE),
        repo_root.join(SEMANTIC_ABLATION_LABELS_RELATIVE),
        repo_root.join(SEMANTIC_ABLATION_ARTIFACT_RELATIVE),
        repo_root.join(SEMANTIC_ABLATION_CORPUS_RELATIVE),
    )
}

pub fn semantic_model_manifest_path(repo_root: &Path) -> PathBuf {
    repo_root.join(SEMANTIC_ABLATION_MODEL_MANIFEST_RELATIVE)
}

fn load_file_inputs(
    workload_path: &Path,
    labels_path: &Path,
    artifact_path: &Path,
    corpus_path: &Path,
) -> Result<
    (
        SemanticAblationWorkloadV1,
        tracedecay_query::search_quality::semantic_ablation::SemanticAblationLabelsV1,
        SemanticAblationArtifactV1,
        String,
        SemanticAblationInputDigestsV1,
    ),
    SearchEvalError,
> {
    let workload: SemanticAblationWorkloadV1 = read_json(workload_path)?;
    let labels = read_json(labels_path)?;
    let artifact: SemanticAblationArtifactV1 = read_json(artifact_path)?;
    let corpus = fs::read(corpus_path).map_err(|error| {
        SearchEvalError::Contract(format!(
            "read semantic corpus {}: {error}",
            corpus_path.display()
        ))
    })?;
    let model_manifest_path = find_model_manifest(artifact_path).ok_or_else(|| {
        contract(format!(
            "cannot locate product model manifest {} from {}",
            SEMANTIC_ABLATION_MODEL_MANIFEST_RELATIVE,
            artifact_path.display()
        ))
    })?;
    let model_manifest = fs::read(&model_manifest_path).map_err(|error| {
        SearchEvalError::Contract(format!(
            "read semantic model manifest {}: {error}",
            model_manifest_path.display()
        ))
    })?;
    validate_semantic_model_manifest(&artifact, &model_manifest).map_err(map_semantic_error)?;
    let corpus_files = workload
        .corpus
        .iter()
        .map(|document| (document.path.as_str(), corpus.as_slice()))
        .collect::<Vec<_>>();
    let corpus_digest =
        tracedecay_query::search_quality::semantic_ablation::compute_semantic_corpus_digest(
            &workload,
            &corpus_files,
        )
        .map_err(map_semantic_error)?;
    let digests = validate_semantic_ablation_inputs(&workload, &labels, &artifact, &corpus_digest)
        .map_err(map_semantic_error)?;
    Ok((workload, labels, artifact, corpus_digest, digests))
}

fn find_model_manifest(asset_path: &Path) -> Option<PathBuf> {
    let mut current = asset_path.parent()?;
    loop {
        let candidate = current.join(SEMANTIC_ABLATION_MODEL_MANIFEST_RELATIVE);
        if candidate.is_file() {
            return Some(candidate);
        }
        current = current.parent()?;
    }
}

fn read_json<T: DeserializeOwned>(path: &Path) -> Result<T, SearchEvalError> {
    let bytes = fs::read(path).map_err(|error| {
        SearchEvalError::Contract(format!("read semantic asset {}: {error}", path.display()))
    })?;
    serde_json::from_slice(&bytes).map_err(|error| {
        SearchEvalError::Contract(format!("parse semantic asset {}: {error}", path.display()))
    })
}

fn map_semantic_error(error: SemanticAblationErrorV1) -> SearchEvalError {
    SearchEvalError::Contract(error.to_string())
}

fn contract(message: impl Into<String>) -> SearchEvalError {
    SearchEvalError::Contract(message.into())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tracedecay_query::search_quality::semantic_ablation::{
        SemanticAblationArtifactStatusV1, SemanticAblationCandidateV1,
        SemanticAblationExecutionResultV1, SemanticAblationInvocationCountersV1,
        SemanticAblationLifecycleEvidenceV1, SemanticAblationRankedCandidateV1, packaged,
        provenance, semantic_ablation_authority_identity_digest,
        semantic_ablation_lifecycle_receipt_digest, semantic_ablation_production_receipt_digest,
    };

    struct RecordingAuthority {
        identity:
            tracedecay_query::search_quality::semantic_ablation::SemanticAblationArtifactIdentityV1,
        candidates: Vec<SemanticAblationCandidateV1>,
    }

    impl RecordingAuthority {
        fn new(
            identity: tracedecay_query::search_quality::semantic_ablation::SemanticAblationArtifactIdentityV1,
            candidates: Vec<SemanticAblationCandidateV1>,
        ) -> Self {
            Self {
                identity,
                candidates,
            }
        }

        fn lifecycle(
            &self,
            request: &SemanticAblationLifecycleRequestV1<'_>,
            phase: &str,
            cache_reset: bool,
            runtime_restart: bool,
        ) -> Result<SemanticAblationLifecycleEvidenceV1, SemanticAblationErrorV1> {
            let runtime_identity = canonical_sha256(&(
                "test-production-runtime",
                request.mode,
                request.query.query_id.as_str(),
                if runtime_restart {
                    request.repetition_index
                } else {
                    0
                },
            ))
            .map_err(|error| SemanticAblationErrorV1::Contract(error.to_string()))?;
            let mut lifecycle = SemanticAblationLifecycleEvidenceV1 {
                phase: phase.to_owned(),
                runtime_identity,
                cache_reset,
                runtime_restart,
                lifecycle_receipt_digest: String::new(),
            };
            lifecycle.lifecycle_receipt_digest =
                semantic_ablation_lifecycle_receipt_digest(&lifecycle)?;
            Ok(lifecycle)
        }
    }

    impl SemanticAblationProductionAuthorityV1 for RecordingAuthority {
        fn authority_identity(
            &self,
        ) -> &tracedecay_query::search_quality::semantic_ablation::SemanticAblationArtifactIdentityV1
        {
            &self.identity
        }

        fn reset_for_cold(
            &mut self,
            request: &SemanticAblationLifecycleRequestV1<'_>,
        ) -> Result<SemanticAblationLifecycleEvidenceV1, SemanticAblationErrorV1> {
            self.lifecycle(request, "cold", true, false)
        }

        fn prepare_warm(
            &mut self,
            request: &SemanticAblationLifecycleRequestV1<'_>,
        ) -> Result<SemanticAblationLifecycleEvidenceV1, SemanticAblationErrorV1> {
            self.lifecycle(request, "warm", false, false)
        }

        fn restart_runtime(
            &mut self,
            request: &SemanticAblationLifecycleRequestV1<'_>,
        ) -> Result<SemanticAblationLifecycleEvidenceV1, SemanticAblationErrorV1> {
            self.lifecycle(request, "restart", true, true)
        }

        fn execute(
            &mut self,
            request: SemanticAblationExecutionRequestV1<'_>,
        ) -> Result<SemanticAblationExecutionResultV1, SemanticAblationErrorV1> {
            let (channel, semantic) = match request.mode {
                SemanticAblationModeV1::LexicalBaseline => ("lexical", false),
                SemanticAblationModeV1::SemanticOnly => ("semantic", true),
                SemanticAblationModeV1::Hybrid => ("hybrid", true),
            };
            let ranked_candidates = self
                .candidates
                .iter()
                .take(10)
                .enumerate()
                .map(|(index, candidate)| SemanticAblationRankedCandidateV1 {
                    candidate_id: candidate.candidate_id.clone(),
                    document_id: candidate.document_id.clone(),
                    source_path: candidate.source_path.clone(),
                    symbol: candidate.symbol.clone(),
                    record_digest: candidate.record_digest.clone(),
                    rank: index as u32 + 1,
                    score_ppm: 1_000_000 - index as u32 * 1_000,
                    channel: channel.to_owned(),
                    semantic_distance_micros: semantic.then_some(index as i64),
                    provenance: semantic.then(|| provenance(&self.identity)),
                })
                .collect::<Vec<_>>();
            let invocation_counters = match request.mode {
                SemanticAblationModeV1::LexicalBaseline => SemanticAblationInvocationCountersV1 {
                    exact: 1,
                    lexical: 1,
                    graph: 1,
                    semantic: 0,
                },
                SemanticAblationModeV1::SemanticOnly => SemanticAblationInvocationCountersV1 {
                    exact: 0,
                    lexical: 0,
                    graph: 0,
                    semantic: 1,
                },
                SemanticAblationModeV1::Hybrid => SemanticAblationInvocationCountersV1 {
                    exact: 1,
                    lexical: 1,
                    graph: 1,
                    semantic: 1,
                },
            };
            let disabled_lanes = match request.mode {
                SemanticAblationModeV1::LexicalBaseline => vec!["semantic".to_owned()],
                SemanticAblationModeV1::SemanticOnly => {
                    vec!["exact".to_owned(), "lexical".to_owned(), "graph".to_owned()]
                }
                SemanticAblationModeV1::Hybrid => Vec::new(),
            };
            let authority_identity_digest =
                semantic_ablation_authority_identity_digest(&self.identity)?;
            let mut result = SemanticAblationExecutionResultV1 {
                artifact_status: SemanticAblationArtifactStatusV1::Verified,
                artifact_mismatch: false,
                fallback_used: false,
                disabled_lanes,
                invocation_counters,
                ranked_candidates,
                authority_identity_digest,
                production_receipt_digest: String::new(),
                failure: None,
            };
            let ranking_digest = canonical_sha256(&(
                SEMANTIC_ABLATION_RANKING_DIGEST_DOMAIN,
                &result.ranked_candidates,
            ))
            .map_err(|error| SemanticAblationErrorV1::Contract(error.to_string()))?;
            result.production_receipt_digest = semantic_ablation_production_receipt_digest(
                &self.identity,
                &request,
                &result,
                &ranking_digest,
            )?;
            Ok(result)
        }
    }

    #[test]
    fn replay_entry_point_fails_closed() {
        let (workload, _labels, artifact, digests) = packaged::inputs().expect("semantic fixture");
        let error = generate_semantic_ablation_runs(&workload, &artifact, &digests)
            .expect_err("artifact replay must be impossible");
        assert!(error.to_string().contains("authority"));
    }

    #[test]
    fn injected_authority_executes_exact_lifecycle_matrix_without_labels() {
        let (workload, _labels, artifact, digests) = packaged::inputs().expect("semantic fixture");
        let mut authority =
            RecordingAuthority::new(artifact.identity.clone(), workload.candidates.clone());
        let runs = generate_semantic_ablation_runs_with_authority(
            &workload,
            &artifact,
            &digests,
            &mut authority,
        )
        .expect("production adapter matrix");
        assert_eq!(runs.len(), 3 * workload.queries.len() * 3 * 10);
        assert!(
            runs.iter()
                .all(|run| run.quality.is_none() && run.status.is_none())
        );
        assert!(
            runs.iter()
                .filter(|run| run.mode == SemanticAblationModeV1::SemanticOnly)
                .all(|run| run.invocation_counters.exact == 0
                    && run.invocation_counters.lexical == 0
                    && run.invocation_counters.graph == 0)
        );
    }

    #[test]
    fn injected_authority_rejects_extra_or_missing_repetitions() {
        let (mut workload, _labels, artifact, digests) =
            packaged::inputs().expect("semantic fixture");
        workload.repetition_contract.restart -= 1;
        let mut authority =
            RecordingAuthority::new(artifact.identity.clone(), workload.candidates.clone());
        let error = generate_semantic_ablation_runs_with_authority(
            &workload,
            &artifact,
            &digests,
            &mut authority,
        )
        .expect_err("the production matrix must have exactly ten repetitions per lifecycle");
        assert!(error.to_string().contains("exactly ten"));
    }

    #[test]
    fn explicit_file_entry_point_validates_manifest_before_refusing_replay() {
        let root =
            Path::new(env!("CARGO_MANIFEST_DIR")).join("../tracedecay-query/assets/runtime-root");
        let (workload_path, labels_path, artifact_path, corpus_path) =
            semantic_ablation_paths(&root);
        let error = run_semantic_ablation_from_files(
            &workload_path,
            &labels_path,
            &artifact_path,
            &corpus_path,
        )
        .expect_err("no authority must fail closed");
        assert!(error.to_string().contains("authority"));
    }
}
