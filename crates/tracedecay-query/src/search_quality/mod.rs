//! Production search-quality kernel: candidate types, packaged workload
//! inputs, and direct-report scoring for the exact/lexical/graph lanes.
//!
//! The evaluator that publishes a fixture corpus and compares live candidates
//! lives in `tracedecay-search-eval` and depends on this module.

pub mod candidate_output;
pub mod evaluate;
pub mod packaged;
pub mod report;
#[cfg(feature = "search-eval")]
pub mod semantic_ablation;

pub use candidate_output::{
    CandidateOutputError, CandidateWorkloadV1, CorpusDocumentV1, EvaluationConcurrencyContractV1,
    EvaluationExecutionContractV1, GenerateCandidateOutputsResultV1, NeedProvenanceKindV1,
    NeedProvenanceV1, ProductionCandidateOutputV1, ResourceMeasurementStatusV1, WorkloadQueryV1,
    compute_corpus_digest, compute_profile_material_digest, compute_workload_digest,
    load_candidate_workload, validate_workload_for_tuning,
};
pub use evaluate::{
    DirectEvaluationStatusV1, QUERY_BASELINE_PROFILE, SearchEvalError, evaluate_generated_outputs,
};
pub use report::{
    DirectEvaluationReportV1, DirectProfileEvaluationV1, DirectQualityMetricsV1,
    DirectQueryEvaluationV1, DirectQueryQualityV1, DirectRatioMetricV1, DirectStratumQualityV1,
    DirectWorstStratumV1,
};
#[cfg(feature = "search-eval")]
pub use semantic_ablation::{
    SEMANTIC_ABLATION_ARTIFACT_RELATIVE, SEMANTIC_ABLATION_AUTHORITY_DIGEST_DOMAIN,
    SEMANTIC_ABLATION_CORPUS_RELATIVE, SEMANTIC_ABLATION_CORPUS_SHA256,
    SEMANTIC_ABLATION_INDEX_SHA256, SEMANTIC_ABLATION_LABELS_RELATIVE,
    SEMANTIC_ABLATION_LABELS_SHA256, SEMANTIC_ABLATION_MANIFEST_SHA256,
    SEMANTIC_ABLATION_MIN_CANDIDATES, SEMANTIC_ABLATION_MIN_DECOYS,
    SEMANTIC_ABLATION_MIN_REPETITIONS,
    SEMANTIC_ABLATION_MODEL_MANIFEST_RELATIVE, SEMANTIC_ABLATION_MODEL_MANIFEST_SHA256,
    SEMANTIC_ABLATION_MODEL_SHA256, SEMANTIC_ABLATION_PRECISION_THRESHOLD_PPM,
    SEMANTIC_ABLATION_PROJECTION_SHA256, SEMANTIC_ABLATION_RANKING_DIGEST_DOMAIN,
    SEMANTIC_ABLATION_RECALL_THRESHOLD_PPM, SEMANTIC_ABLATION_RECEIPT_DIGEST_DOMAIN,
    SEMANTIC_ABLATION_REPETITIONS, SEMANTIC_ABLATION_SCHEMA_VERSION,
    SEMANTIC_ABLATION_SOURCE_DIGEST_DOMAIN, SEMANTIC_ABLATION_SOURCE_SHA256,
    SEMANTIC_ABLATION_TOP_K, SEMANTIC_ABLATION_VECTOR_SHA256, SEMANTIC_ABLATION_WORKLOAD_RELATIVE,
    SEMANTIC_ABLATION_WORKLOAD_SHA256, SemanticAblationArtifactIdentityV1,
    SemanticAblationArtifactStatusV1, SemanticAblationArtifactV1, SemanticAblationCandidateV1,
    SemanticAblationCorpusDocumentV1, SemanticAblationErrorV1, SemanticAblationExecutionRequestV1,
    SemanticAblationExecutionResultV1, SemanticAblationInputDigestsV1,
    SemanticAblationInvocationCountersV1, SemanticAblationLabelV1, SemanticAblationLabelsV1,
    SemanticAblationLifecycleEvidenceV1, SemanticAblationLifecycleRequestV1, SemanticAblationMode,
    SemanticAblationModeSummaryV1, SemanticAblationModeV1, SemanticAblationProductionAuthorityV1,
    SemanticAblationProvenanceV1, SemanticAblationQualityContractV1,
    SemanticAblationQueryQualityV1, SemanticAblationQueryV1, SemanticAblationRankedCandidateV1,
    SemanticAblationRatioV1, SemanticAblationRepetitionContractV1, SemanticAblationRepetitionKind,
    SemanticAblationRepetitionKindV1, SemanticAblationReportV1, SemanticAblationRunV1,
    SemanticAblationStatusV1, SemanticAblationStratumMetricsV1, SemanticAblationWorkloadV1,
    compute_semantic_corpus_digest, evaluate_semantic_ablation, provenance,
    semantic_ablation_authority_identity_digest, semantic_ablation_lifecycle_receipt_digest,
    semantic_ablation_production_receipt_digest, semantic_candidate_record_digest,
    validate_semantic_ablation_inputs, validate_semantic_ablation_report,
    validate_semantic_model_manifest,
};
