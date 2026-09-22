//! Evaluator-owned semantic ablation contracts.
//!
//! This module owns the evidence boundary for the semantic quality slice. It
//! does not contain a model, projection, vector store, or query router. A
//! caller must provide an authority backed by the production semantic query
//! path. In particular, an artifact is an identity/manifest pin only: it never
//! contains a ranking and it is never consulted to choose a candidate.

use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use thiserror::Error;
use tracedecay_domain::canonical_text::encode_tagged_lowercase_hex;
use tracedecay_domain::split_subtokens;

use super::candidate_output::canonical_sha256;

pub const SEMANTIC_ABLATION_SCHEMA_VERSION: u32 = 1;
pub const SEMANTIC_ABLATION_WORKLOAD_RELATIVE: &str =
    "tests/fixtures/search_quality/semantic-ablation/query-semantic-ablation-workload-v1.json";
pub const SEMANTIC_ABLATION_LABELS_RELATIVE: &str =
    "tests/fixtures/search_quality/semantic-ablation/labels-v1.json";
pub const SEMANTIC_ABLATION_ARTIFACT_RELATIVE: &str =
    "tests/fixtures/search_quality/semantic-ablation/artifact-v1.json";
pub const SEMANTIC_ABLATION_CORPUS_RELATIVE: &str =
    "tests/fixtures/search_quality/semantic-ablation/corpus/semantic_catalog.rs";
pub const SEMANTIC_ABLATION_MODEL_MANIFEST_RELATIVE: &str = "product/semantic/model-manifest.json";
pub const SEMANTIC_ABLATION_TOP_K: usize = 10;
pub const SEMANTIC_ABLATION_MIN_CANDIDATES: usize = 10;
/// Every query must have ten eligible decoys in addition to its hidden
/// relevant set, so a quality pass cannot be explained by a tiny candidate
/// pool.
pub const SEMANTIC_ABLATION_MIN_DECOYS: usize = 10;
/// Every query/mode has exactly this many runs for every lifecycle kind.
pub const SEMANTIC_ABLATION_REPETITIONS: u32 = 10;
/// Compatibility alias retained for callers that used the first contract name.
pub const SEMANTIC_ABLATION_MIN_REPETITIONS: u32 = SEMANTIC_ABLATION_REPETITIONS;
pub const SEMANTIC_ABLATION_RECALL_THRESHOLD_PPM: u32 = 800_000;
pub const SEMANTIC_ABLATION_PRECISION_THRESHOLD_PPM: u32 = 500_000;
pub const SEMANTIC_ABLATION_SOURCE_DIGEST_DOMAIN: &str =
    "tracedecay.search-eval.semantic-ablation.source.v1";
pub const SEMANTIC_ABLATION_RANKING_DIGEST_DOMAIN: &str =
    "tracedecay.search-eval.semantic-ablation.ranking.v1";
pub const SEMANTIC_ABLATION_RECEIPT_DIGEST_DOMAIN: &str =
    "tracedecay.search-eval.semantic-ablation.production-receipt.v1";
pub const SEMANTIC_ABLATION_LIFECYCLE_DIGEST_DOMAIN: &str =
    "tracedecay.search-eval.semantic-ablation.lifecycle.v1";
pub const SEMANTIC_ABLATION_AUTHORITY_DIGEST_DOMAIN: &str =
    "tracedecay.search-eval.semantic-ablation.authority.v1";

// The workload/corpus/labels are byte-frozen evaluator inputs. The model pin
// is the digest published by product/semantic/model-manifest.json; the other
// material digests are recomputed from the artifact's immutable identities.
pub const SEMANTIC_ABLATION_WORKLOAD_SHA256: &str =
    "sha256:080be3856041722c34fd6ee40b76006d3513af2d34e40e13b15f6eb8db163627";
pub const SEMANTIC_ABLATION_CORPUS_SHA256: &str =
    "sha256:caf2666f212326415d77d657faf5acd38ce8fcb90a140b73ed0480bd8eaa89ed";
pub const SEMANTIC_ABLATION_LABELS_SHA256: &str =
    "sha256:71976cceef507d6ab8dc1c66a207ce34400ef145534a7f2f5af5444b356532fc";
pub const SEMANTIC_ABLATION_MODEL_SHA256: &str =
    "sha256:70be81163e9740d742b7857e132713b323b5042d661485354d781cb8313c15af";
pub const SEMANTIC_ABLATION_PROJECTION_SHA256: &str =
    "sha256:02993e5b6c6cfab94f0383c7e2365e72fda07b8c7217d5f0bf54ac7ae4902eae";
pub const SEMANTIC_ABLATION_VECTOR_SHA256: &str =
    "sha256:8448e2a0d726d1b82be899155f3e139d9f80e5f2d32dca54cbe063471ed18cfa";
pub const SEMANTIC_ABLATION_INDEX_SHA256: &str =
    "sha256:2d5377d189e2d2f5231710a27aaa18dc12774ccaa996ad40e95e74277814779f";
pub const SEMANTIC_ABLATION_SOURCE_SHA256: &str =
    "sha256:5a4dada128ff30e646e513bc0047f35a9a4b22631ee5271140caa8233ba5a14a";
pub const SEMANTIC_ABLATION_MANIFEST_SHA256: &str =
    "sha256:c18d3e5eda3994bd4192c604dbf682fdab26aa9d86b3b340ac54cca1a5e4c825";
pub const SEMANTIC_ABLATION_MODEL_MANIFEST_SHA256: &str =
    "sha256:002759bbdc40f06fe8f67401fff11e5fe0235a6466c8bcf395749131f9721499";

const CORPUS_DIGEST_DOMAIN: &str = "tracedecay.search-eval.semantic-ablation.corpus.v1";
const WORKLOAD_DIGEST_DOMAIN: &str = "tracedecay.search-eval.semantic-ablation.workload.v1";
const LABEL_DIGEST_DOMAIN: &str = "tracedecay.search-eval.semantic-ablation.labels.v1";
const ARTIFACT_DIGEST_DOMAIN: &str = "tracedecay.search-eval.semantic-ablation.artifact.v1";
const PROJECTION_DIGEST_DOMAIN: &str =
    "tracedecay.search-eval.semantic-ablation.projection-material.v1";
const VECTOR_DIGEST_DOMAIN: &str = "tracedecay.search-eval.semantic-ablation.vector-material.v1";
const OUTPUT_DIGEST_DOMAIN: &str = "tracedecay.search-eval.semantic-ablation.output.v1";
const MATRIX_DIGEST_DOMAIN: &str = "tracedecay.search-eval.semantic-ablation.matrix.v1";

#[derive(Serialize)]
struct SemanticAblationProductionReceiptMaterial<'a> {
    identity: &'a SemanticAblationArtifactIdentityV1,
    workload_digest: &'a str,
    corpus_digest: &'a str,
    query_id: &'a str,
    partition: &'a str,
    stratum: &'a str,
    query: &'a str,
    allowed_scopes: &'a [String],
    mode: SemanticAblationModeV1,
    repetition_kind: SemanticAblationRepetitionKindV1,
    repetition_index: u32,
    lifecycle: &'a SemanticAblationLifecycleEvidenceV1,
    artifact_status: SemanticAblationArtifactStatusV1,
    artifact_mismatch: bool,
    fallback_used: bool,
    disabled_lanes: &'a [String],
    invocation_counters: &'a SemanticAblationInvocationCountersV1,
    ranked_candidates: &'a [SemanticAblationRankedCandidateV1],
    ranking_digest: &'a str,
}

#[derive(Serialize)]
struct SemanticAblationArtifactMaterial<'a> {
    workload_id: &'a str,
    model_manifest_path: &'a str,
    model_manifest_digest: &'a str,
    authority_id: &'a str,
    search_index_kind: &'a str,
    index_manifest_digest: &'a str,
    model_id: &'a str,
    model_revision: &'a str,
    model_artifact_digest: &'a str,
    projection_id: &'a str,
    projection_revision: &'a str,
    projection_artifact_digest: &'a str,
    vector_generation_id: &'a str,
    vector_generation_revision: &'a str,
    vector_artifact_digest: &'a str,
    index_id: &'a str,
    index_revision: &'a str,
    index_artifact_digest: &'a str,
}

/// The three routes that must remain distinguishable in the evaluator.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum SemanticAblationModeV1 {
    LexicalBaseline,
    SemanticOnly,
    Hybrid,
}

pub type SemanticAblationMode = SemanticAblationModeV1;

impl SemanticAblationModeV1 {
    pub const ALL: [Self; 3] = [Self::LexicalBaseline, Self::SemanticOnly, Self::Hybrid];

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::LexicalBaseline => "lexical_baseline",
            Self::SemanticOnly => "semantic_only",
            Self::Hybrid => "hybrid",
        }
    }
}

/// The lifecycle populations required by the semantic evaluation contract.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum SemanticAblationRepetitionKindV1 {
    Cold,
    Warm,
    Restart,
}

pub type SemanticAblationRepetitionKind = SemanticAblationRepetitionKindV1;

impl SemanticAblationRepetitionKindV1 {
    pub const ALL: [Self; 3] = [Self::Cold, Self::Warm, Self::Restart];

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Cold => "cold",
            Self::Warm => "warm",
            Self::Restart => "restart",
        }
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum SemanticAblationStatusV1 {
    Pass,
    ControlPass,
    Fail,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum SemanticAblationArtifactStatusV1 {
    Verified,
    Fallback,
    Mismatch,
}

#[derive(Debug, Error)]
pub enum SemanticAblationErrorV1 {
    #[error("semantic ablation contract violation: {0}")]
    Contract(String),
    #[error("semantic ablation input read failed for {path}: {source}")]
    Read {
        path: String,
        source: std::io::Error,
    },
    #[error("semantic ablation input parse failed for {path}: {source}")]
    Parse {
        path: String,
        source: serde_json::Error,
    },
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct SemanticAblationQualityContractV1 {
    pub top_k: usize,
    pub minimum_candidates_per_query: usize,
    pub minimum_relevant_candidates_per_query: usize,
    pub recall_at_10_threshold_ppm: u32,
    pub precision_at_10_threshold_ppm: u32,
    pub fixed_precision_denominator: bool,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct SemanticAblationRepetitionContractV1 {
    pub cold: u32,
    pub warm: u32,
    pub restart: u32,
}

impl SemanticAblationRepetitionContractV1 {
    pub const fn count(&self, kind: SemanticAblationRepetitionKindV1) -> u32 {
        match kind {
            SemanticAblationRepetitionKindV1::Cold => self.cold,
            SemanticAblationRepetitionKindV1::Warm => self.warm,
            SemanticAblationRepetitionKindV1::Restart => self.restart,
        }
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct SemanticAblationCorpusDocumentV1 {
    pub document_id: String,
    pub path: String,
    pub scope: String,
    pub language: String,
    pub eligibility: String,
    /// SHA-256 of the exact bytes read from path for this document.
    pub content_digest: String,
}

/// Candidate metadata is intentionally redundant. Its record digest binds the
/// opaque candidate identity to the corpus document and every source anchor.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct SemanticAblationCandidateV1 {
    pub candidate_id: String,
    pub document_id: String,
    pub scope: String,
    pub source_path: String,
    pub symbol: String,
    pub aliases: Vec<String>,
    pub terms: Vec<String>,
    pub subtokens: Vec<String>,
    pub record_digest: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct SemanticAblationQueryV1 {
    pub query_id: String,
    pub partition: String,
    pub stratum: String,
    pub query: String,
    pub allowed_scopes: Vec<String>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct SemanticAblationWorkloadV1 {
    pub schema_version: u32,
    pub workload_id: String,
    pub source_repository_commit: String,
    pub source_repository_tree: String,
    pub corpus: Vec<SemanticAblationCorpusDocumentV1>,
    pub candidates: Vec<SemanticAblationCandidateV1>,
    pub queries: Vec<SemanticAblationQueryV1>,
    pub repetition_contract: SemanticAblationRepetitionContractV1,
    pub quality_contract: SemanticAblationQualityContractV1,
    pub expected_artifact_identity: SemanticAblationArtifactIdentityV1,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct SemanticAblationLabelV1 {
    pub query_id: String,
    pub target_candidate_ids: Vec<String>,
    pub forbidden_candidate_ids: Vec<String>,
}

/// Labels are evaluator-only input. The production authority never receives
/// this type or any of its candidate IDs.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct SemanticAblationLabelsV1 {
    pub schema_version: u32,
    pub workload_id: String,
    pub label_set_id: String,
    pub labels: Vec<SemanticAblationLabelV1>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct SemanticAblationArtifactIdentityV1 {
    pub model_id: String,
    pub model_revision: String,
    pub model_artifact_digest: String,
    pub projection_id: String,
    pub projection_revision: String,
    pub projection_artifact_digest: String,
    pub vector_generation_id: String,
    pub vector_generation_revision: String,
    pub vector_artifact_digest: String,
    pub index_id: String,
    pub index_revision: String,
    pub index_artifact_digest: String,
    /// Digest of the evaluator artifact manifest, distinct from the product
    /// model manifest digest carried by SemanticAblationArtifactV1.
    pub manifest_digest: String,
}

/// An artifact is a pin to production identities and manifests. It carries no
/// query-specific ordering, scores, labels, or candidate IDs.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct SemanticAblationArtifactV1 {
    pub schema_version: u32,
    pub workload_id: String,
    pub workload_digest: String,
    pub model_manifest_path: String,
    pub model_manifest_digest: String,
    pub authority_id: String,
    pub search_index_kind: String,
    pub index_manifest_digest: String,
    pub identity: SemanticAblationArtifactIdentityV1,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct SemanticAblationInputDigestsV1 {
    pub workload_digest: String,
    pub corpus_digest: String,
    pub labels_digest: String,
    pub model_digest: String,
    pub projection_digest: String,
    pub vector_generation_digest: String,
    pub index_digest: String,
    pub source_digest: String,
    pub artifact_manifest_digest: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct SemanticAblationInvocationCountersV1 {
    pub exact: u64,
    pub lexical: u64,
    pub graph: u64,
    pub semantic: u64,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct SemanticAblationProvenanceV1 {
    pub model_id: String,
    pub model_revision: String,
    pub model_artifact_digest: String,
    pub projection_id: String,
    pub projection_revision: String,
    pub projection_artifact_digest: String,
    pub vector_generation_id: String,
    pub vector_generation_revision: String,
    pub vector_artifact_digest: String,
    pub index_id: String,
    pub index_revision: String,
    pub index_artifact_digest: String,
}

/// Candidate identity and metadata are retained with every production rank.
/// Semantic distance is present when the production semantic lane supplied the
/// row; lexical baseline rows must leave it absent.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct SemanticAblationRankedCandidateV1 {
    pub candidate_id: String,
    pub document_id: String,
    pub source_path: String,
    pub symbol: String,
    pub record_digest: String,
    pub rank: u32,
    pub score_ppm: u32,
    pub channel: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub semantic_distance_micros: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub provenance: Option<SemanticAblationProvenanceV1>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct SemanticAblationLifecycleEvidenceV1 {
    pub phase: String,
    /// Identity of the runtime/session that actually served this run. It is a
    /// digest rather than an OS process ID so reports remain reproducible.
    pub runtime_identity: String,
    pub cache_reset: bool,
    pub runtime_restart: bool,
    pub lifecycle_receipt_digest: String,
}

/// The identity and query inputs supplied while the production authority
/// performs a lifecycle transition. It has no lifecycle evidence yet because
/// the transition is what mints that evidence.
pub struct SemanticAblationLifecycleRequestV1<'a> {
    pub workload_digest: &'a str,
    pub corpus_digest: &'a str,
    pub query: &'a SemanticAblationQueryV1,
    pub mode: SemanticAblationModeV1,
    pub repetition_kind: SemanticAblationRepetitionKindV1,
    pub repetition_index: u32,
    pub artifact: &'a SemanticAblationArtifactIdentityV1,
}

/// Request passed to the production semantic authority. There is deliberately
/// no labels field and no expected ranking field.
#[derive(Clone, Copy)]
pub struct SemanticAblationExecutionRequestV1<'a> {
    pub workload_digest: &'a str,
    pub corpus_digest: &'a str,
    pub query: &'a SemanticAblationQueryV1,
    pub mode: SemanticAblationModeV1,
    pub repetition_kind: SemanticAblationRepetitionKindV1,
    pub repetition_index: u32,
    pub lifecycle: &'a SemanticAblationLifecycleEvidenceV1,
    pub artifact: &'a SemanticAblationArtifactIdentityV1,
}

/// Output returned by an authority backed by the production semantic query,
/// model, projection, vector, and immutable-index path.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct SemanticAblationExecutionResultV1 {
    pub artifact_status: SemanticAblationArtifactStatusV1,
    pub artifact_mismatch: bool,
    pub fallback_used: bool,
    pub disabled_lanes: Vec<String>,
    pub invocation_counters: SemanticAblationInvocationCountersV1,
    pub ranked_candidates: Vec<SemanticAblationRankedCandidateV1>,
    pub authority_identity_digest: String,
    pub production_receipt_digest: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub failure: Option<String>,
}

/// Adapter contract for the application semantic runtime. Implementations must
/// call the production query authority; a test double may be used only by
/// contract tests and cannot be used by the packaged runner.
pub trait SemanticAblationProductionAuthorityV1 {
    fn authority_identity(&self) -> &SemanticAblationArtifactIdentityV1;

    /// Drop request/model/vector caches while retaining the immutable
    /// generation. The returned evidence must prove the cold phase.
    fn reset_for_cold(
        &mut self,
        request: &SemanticAblationLifecycleRequestV1<'_>,
    ) -> Result<SemanticAblationLifecycleEvidenceV1, SemanticAblationErrorV1>;

    /// Reuse the same process/runtime/session. The returned evidence must prove
    /// that no cache reset or process restart occurred.
    fn prepare_warm(
        &mut self,
        request: &SemanticAblationLifecycleRequestV1<'_>,
    ) -> Result<SemanticAblationLifecycleEvidenceV1, SemanticAblationErrorV1>;

    /// Recreate the runtime, and where the caller supports it the process. A
    /// restart run is invalid unless the returned evidence marks it explicitly.
    fn restart_runtime(
        &mut self,
        request: &SemanticAblationLifecycleRequestV1<'_>,
    ) -> Result<SemanticAblationLifecycleEvidenceV1, SemanticAblationErrorV1>;

    fn execute(
        &mut self,
        request: SemanticAblationExecutionRequestV1<'_>,
    ) -> Result<SemanticAblationExecutionResultV1, SemanticAblationErrorV1>;
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct SemanticAblationRatioV1 {
    pub numerator: u64,
    pub denominator: u64,
    pub ppm: u32,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct SemanticAblationQueryQualityV1 {
    pub recall_at_10: SemanticAblationRatioV1,
    pub precision_at_10: SemanticAblationRatioV1,
    pub target_count: u64,
    pub returned_candidates: usize,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct SemanticAblationRunV1 {
    pub schema_version: u32,
    pub run_id: String,
    pub query_id: String,
    pub partition: String,
    pub stratum: String,
    pub mode: SemanticAblationModeV1,
    pub repetition_kind: SemanticAblationRepetitionKindV1,
    pub repetition_index: u32,
    pub workload_digest: String,
    pub corpus_digest: String,
    pub labels_digest: String,
    pub model_digest: String,
    pub projection_digest: String,
    pub vector_generation_digest: String,
    pub index_digest: String,
    pub source_digest: String,
    pub artifact_manifest_digest: String,
    pub artifact_status: SemanticAblationArtifactStatusV1,
    pub artifact_mismatch: bool,
    pub fallback_used: bool,
    pub disabled_lanes: Vec<String>,
    pub invocation_counters: SemanticAblationInvocationCountersV1,
    pub authority_identity_digest: String,
    pub lifecycle: SemanticAblationLifecycleEvidenceV1,
    pub latency_micros: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub failure: Option<String>,
    pub production_receipt_digest: String,
    pub ranking_digest: String,
    pub ranked_candidates: Vec<SemanticAblationRankedCandidateV1>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub quality: Option<SemanticAblationQueryQualityV1>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub status: Option<SemanticAblationStatusV1>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct SemanticAblationStratumMetricsV1 {
    pub mode: SemanticAblationModeV1,
    pub partition: String,
    pub stratum: String,
    pub query_count: u64,
    pub recall_at_10: SemanticAblationRatioV1,
    pub precision_at_10: SemanticAblationRatioV1,
    pub status: SemanticAblationStatusV1,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct SemanticAblationModeSummaryV1 {
    pub mode: SemanticAblationModeV1,
    pub query_count: u64,
    pub run_count: u64,
    pub status: SemanticAblationStatusV1,
    pub strata: Vec<SemanticAblationStratumMetricsV1>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct SemanticAblationReportV1 {
    pub schema_version: u32,
    pub command: String,
    pub status: SemanticAblationStatusV1,
    pub workload_digest: String,
    pub corpus_digest: String,
    pub labels_digest: String,
    pub source_digest: String,
    pub model_digest: String,
    pub projection_digest: String,
    pub vector_generation_digest: String,
    pub index_digest: String,
    pub artifact_manifest_digest: String,
    pub artifact_identity: SemanticAblationArtifactIdentityV1,
    pub output_digest: String,
    pub matrix_digest: String,
    pub runs: Vec<SemanticAblationRunV1>,
    pub modes: Vec<SemanticAblationModeSummaryV1>,
}

/// Compute the digest of the exact bytes used by every declared corpus record.
/// The declared per-document digest, path metadata, and candidate source
/// anchors are all checked before the aggregate digest is returned.
pub fn compute_semantic_corpus_digest(
    workload: &SemanticAblationWorkloadV1,
    files: &[(&str, &[u8])],
) -> Result<String, SemanticAblationErrorV1> {
    let mut bindings = Vec::with_capacity(workload.corpus.len());
    for document in &workload.corpus {
        let bytes = files
            .iter()
            .find_map(|(path, bytes)| (*path == document.path).then_some(*bytes))
            .ok_or_else(|| contract(format!("semantic corpus is missing {}", document.path)))?;
        let actual = tagged_digest(bytes);
        if document.content_digest != actual {
            return Err(contract(format!(
                "{} content digest does not match corpus bytes",
                document.document_id
            )));
        }
        bindings.push((document, actual));
    }
    validate_candidate_source_bindings(workload, files)?;
    digest(CORPUS_DIGEST_DOMAIN, &bindings)
}

/// Validate the product model manifest bytes against the artifact's pinned
/// path, bytes, and package digest. The packaged loader calls this before any
/// authority is allowed to run.
pub fn validate_semantic_model_manifest(
    artifact: &SemanticAblationArtifactV1,
    bytes: &[u8],
) -> Result<(), SemanticAblationErrorV1> {
    if artifact.model_manifest_path != SEMANTIC_ABLATION_MODEL_MANIFEST_RELATIVE {
        return Err(contract(
            "semantic artifact names a non-product model manifest",
        ));
    }
    if tagged_digest(bytes) != artifact.model_manifest_digest
        || artifact.model_manifest_digest != SEMANTIC_ABLATION_MODEL_MANIFEST_SHA256
    {
        return Err(contract(
            "semantic model manifest bytes do not match the pinned digest",
        ));
    }
    let manifest: serde_json::Value = serde_json::from_slice(bytes)
        .map_err(|error| contract(format!("parse product semantic model manifest: {error}")))?;
    if manifest.get("schema").and_then(serde_json::Value::as_str)
        != Some("tracedecay.distribution.fastembed-fixture.v1")
        || manifest.get("model").and_then(serde_json::Value::as_str)
            != Some(artifact.identity.model_id.as_str())
        || manifest
            .get("source")
            .and_then(|source| source.get("revision"))
            .and_then(serde_json::Value::as_str)
            != Some(artifact.identity.model_revision.as_str())
        || manifest
            .get("artifact_digest")
            .and_then(serde_json::Value::as_str)
            .map(|value| format!("sha256:{value}"))
            != Some(artifact.identity.model_artifact_digest.clone())
        || manifest
            .get("expected_dimensions")
            .and_then(serde_json::Value::as_u64)
            != Some(768)
        || manifest
            .get("max_length")
            .and_then(serde_json::Value::as_u64)
            != Some(8192)
    {
        return Err(contract(
            "semantic model manifest identity does not match artifact",
        ));
    }
    Ok(())
}

/// Validate workload, hidden labels, artifact manifest, and corpus identity
/// before candidate output is accepted.
pub fn validate_semantic_ablation_inputs(
    workload: &SemanticAblationWorkloadV1,
    labels: &SemanticAblationLabelsV1,
    artifact: &SemanticAblationArtifactV1,
    corpus_digest: &str,
) -> Result<SemanticAblationInputDigestsV1, SemanticAblationErrorV1> {
    validate_workload_shape(workload)?;
    validate_labels(workload, labels)?;
    validate_artifact_shape(workload, artifact)?;
    validate_zero_overlap(workload)?;
    if !is_digest(corpus_digest) {
        return Err(contract("corpus digest is not a SHA-256 identity"));
    }

    let (model_digest, projection_digest, vector_generation_digest, index_digest, manifest_digest) =
        artifact_material_digests(workload, artifact)?;
    let expected = &workload.expected_artifact_identity;
    if artifact.identity != *expected {
        return Err(contract(
            "semantic artifact identity does not match the workload pin",
        ));
    }
    if artifact.identity.model_artifact_digest != model_digest
        || artifact.identity.projection_artifact_digest != projection_digest
        || artifact.identity.vector_artifact_digest != vector_generation_digest
        || artifact.identity.index_artifact_digest != index_digest
        || artifact.identity.manifest_digest != manifest_digest
    {
        return Err(contract(
            "semantic artifact member or manifest digest does not match its material",
        ));
    }
    let workload_digest = digest(WORKLOAD_DIGEST_DOMAIN, workload)?;
    if workload_digest != SEMANTIC_ABLATION_WORKLOAD_SHA256 {
        return Err(contract(
            "semantic workload digest differs from the evaluator freeze",
        ));
    }
    if artifact.workload_digest != workload_digest {
        return Err(contract(
            "semantic artifact does not bind the workload digest",
        ));
    }
    let labels_digest = digest(LABEL_DIGEST_DOMAIN, labels)?;
    let source_digest = digest(
        SEMANTIC_ABLATION_SOURCE_DIGEST_DOMAIN,
        &(
            workload.source_repository_commit.as_str(),
            workload.source_repository_tree.as_str(),
            workload_digest.as_str(),
            corpus_digest,
        ),
    )?;
    if corpus_digest != SEMANTIC_ABLATION_CORPUS_SHA256
        || labels_digest != SEMANTIC_ABLATION_LABELS_SHA256
        || model_digest != SEMANTIC_ABLATION_MODEL_SHA256
        || projection_digest != SEMANTIC_ABLATION_PROJECTION_SHA256
        || vector_generation_digest != SEMANTIC_ABLATION_VECTOR_SHA256
        || index_digest != SEMANTIC_ABLATION_INDEX_SHA256
        || source_digest != SEMANTIC_ABLATION_SOURCE_SHA256
        || manifest_digest != SEMANTIC_ABLATION_MANIFEST_SHA256
    {
        return Err(contract(
            "semantic input digest differs from the evaluator freeze",
        ));
    }
    Ok(SemanticAblationInputDigestsV1 {
        workload_digest,
        corpus_digest: corpus_digest.to_owned(),
        labels_digest,
        model_digest,
        projection_digest,
        vector_generation_digest,
        index_digest,
        source_digest,
        artifact_manifest_digest: manifest_digest,
    })
}

/// Digest used by an authority to prove the identity it actually served.
pub fn semantic_ablation_authority_identity_digest(
    identity: &SemanticAblationArtifactIdentityV1,
) -> Result<String, SemanticAblationErrorV1> {
    digest(SEMANTIC_ABLATION_AUTHORITY_DIGEST_DOMAIN, identity)
}

/// Digest that binds a lifecycle transition to the runtime identity returned by
/// the production authority.
pub fn semantic_ablation_lifecycle_receipt_digest(
    lifecycle: &SemanticAblationLifecycleEvidenceV1,
) -> Result<String, SemanticAblationErrorV1> {
    digest(
        SEMANTIC_ABLATION_LIFECYCLE_DIGEST_DOMAIN,
        &(
            lifecycle.phase.as_str(),
            lifecycle.runtime_identity.as_str(),
            lifecycle.cache_reset,
            lifecycle.runtime_restart,
        ),
    )
}

/// Digest that production adapters must return with each query result. It
/// includes the request, lifecycle, lane counters, and ranking bytes but never
/// labels. A retained ranking mutation therefore invalidates the receipt even
/// if a caller forgets to update ranking_digest.
pub fn semantic_ablation_production_receipt_digest(
    identity: &SemanticAblationArtifactIdentityV1,
    request: &SemanticAblationExecutionRequestV1<'_>,
    result: &SemanticAblationExecutionResultV1,
    ranking_digest: &str,
) -> Result<String, SemanticAblationErrorV1> {
    let material = SemanticAblationProductionReceiptMaterial {
        identity,
        workload_digest: request.workload_digest,
        corpus_digest: request.corpus_digest,
        query_id: request.query.query_id.as_str(),
        partition: request.query.partition.as_str(),
        stratum: request.query.stratum.as_str(),
        query: request.query.query.as_str(),
        allowed_scopes: request.query.allowed_scopes.as_slice(),
        mode: request.mode,
        repetition_kind: request.repetition_kind,
        repetition_index: request.repetition_index,
        lifecycle: request.lifecycle,
        artifact_status: result.artifact_status,
        artifact_mismatch: result.artifact_mismatch,
        fallback_used: result.fallback_used,
        disabled_lanes: result.disabled_lanes.as_slice(),
        invocation_counters: &result.invocation_counters,
        ranked_candidates: result.ranked_candidates.as_slice(),
        ranking_digest,
    };
    digest(SEMANTIC_ABLATION_RECEIPT_DIGEST_DOMAIN, &material)
}

/// Score and validate a complete matrix. Labels enter only here, after the
/// production authority has returned its retained rankings.
pub fn evaluate_semantic_ablation(
    workload: &SemanticAblationWorkloadV1,
    labels: &SemanticAblationLabelsV1,
    artifact: &SemanticAblationArtifactV1,
    corpus_digest: &str,
    runs: &[SemanticAblationRunV1],
) -> Result<SemanticAblationReportV1, SemanticAblationErrorV1> {
    let digests = validate_semantic_ablation_inputs(workload, labels, artifact, corpus_digest)?;
    if runs.is_empty() {
        return Err(contract("semantic ablation run matrix is empty"));
    }
    let queries = workload
        .queries
        .iter()
        .map(|query| (query.query_id.as_str(), query))
        .collect::<BTreeMap<_, _>>();
    let labels_by_query = labels
        .labels
        .iter()
        .map(|label| (label.query_id.as_str(), label))
        .collect::<BTreeMap<_, _>>();
    let candidates = workload
        .candidates
        .iter()
        .map(|candidate| (candidate.candidate_id.as_str(), candidate))
        .collect::<BTreeMap<_, _>>();
    let expected_authority_digest =
        semantic_ablation_authority_identity_digest(&artifact.identity)?;

    let mut evaluated = Vec::with_capacity(runs.len());
    let mut seen_repetitions = BTreeSet::new();
    let mut stable_rankings = BTreeMap::<(SemanticAblationModeV1, String), String>::new();
    for input_run in runs {
        validate_run_identity(input_run, &digests)?;
        let query = queries.get(input_run.query_id.as_str()).ok_or_else(|| {
            contract(format!(
                "unknown semantic ablation query {}",
                input_run.query_id
            ))
        })?;
        let label = labels_by_query
            .get(input_run.query_id.as_str())
            .ok_or_else(|| contract(format!("missing label for {}", input_run.query_id)))?;
        if input_run.partition != query.partition || input_run.stratum != query.stratum {
            return Err(contract(format!(
                "{} does not bind query partition/stratum",
                input_run.run_id
            )));
        }
        let expected_run_id = format!(
            "{}/{}/{}/{}/{:02}",
            input_run.mode.as_str(),
            input_run.partition,
            input_run.query_id,
            input_run.repetition_kind.as_str(),
            input_run.repetition_index
        );
        if input_run.run_id != expected_run_id {
            return Err(contract(format!(
                "{} does not use the deterministic repetition identity",
                input_run.run_id
            )));
        }
        if !seen_repetitions.insert((
            input_run.mode,
            input_run.query_id.clone(),
            input_run.repetition_kind,
            input_run.repetition_index,
        )) {
            return Err(contract(format!(
                "duplicate semantic ablation repetition {}",
                input_run.run_id
            )));
        }
        if input_run.authority_identity_digest != expected_authority_digest {
            return Err(contract(format!(
                "{} was served by a foreign semantic authority",
                input_run.run_id
            )));
        }
        validate_lifecycle(input_run)?;
        validate_mode_controls(input_run)?;
        validate_ranked_candidates(input_run, query, &candidates, &artifact.identity)?;
        let expected_ranking_digest = digest(
            SEMANTIC_ABLATION_RANKING_DIGEST_DOMAIN,
            &input_run.ranked_candidates,
        )?;
        if input_run.ranking_digest != expected_ranking_digest {
            return Err(contract(format!(
                "{} ranking digest does not bind ranking bytes",
                input_run.run_id
            )));
        }
        let request = SemanticAblationExecutionRequestV1 {
            workload_digest: &input_run.workload_digest,
            corpus_digest: &input_run.corpus_digest,
            query,
            mode: input_run.mode,
            repetition_kind: input_run.repetition_kind,
            repetition_index: input_run.repetition_index,
            lifecycle: &input_run.lifecycle,
            artifact: &artifact.identity,
        };
        let receipt_result = SemanticAblationExecutionResultV1 {
            artifact_status: input_run.artifact_status,
            artifact_mismatch: input_run.artifact_mismatch,
            fallback_used: input_run.fallback_used,
            disabled_lanes: input_run.disabled_lanes.clone(),
            invocation_counters: input_run.invocation_counters.clone(),
            ranked_candidates: input_run.ranked_candidates.clone(),
            authority_identity_digest: input_run.authority_identity_digest.clone(),
            production_receipt_digest: input_run.production_receipt_digest.clone(),
            failure: input_run.failure.clone(),
        };
        let expected_receipt = semantic_ablation_production_receipt_digest(
            &artifact.identity,
            &request,
            &receipt_result,
            &input_run.ranking_digest,
        )?;
        if input_run.production_receipt_digest != expected_receipt {
            return Err(contract(format!(
                "{} production receipt does not bind authority output",
                input_run.run_id
            )));
        }
        if let Some(previous) = stable_rankings.insert(
            (input_run.mode, input_run.query_id.clone()),
            input_run.ranking_digest.clone(),
        ) && previous != input_run.ranking_digest
        {
            return Err(contract(format!(
                "{} ranking changed across cold/warm/restart repetitions",
                input_run.query_id
            )));
        }
        let quality = score_query(input_run, label, workload.quality_contract.top_k)?;
        let status = query_status(input_run.mode, &quality, &workload.quality_contract);
        let mut output_run = input_run.clone();
        if let Some(previous) = &input_run.quality {
            if previous != &quality {
                return Err(contract(format!(
                    "{} retained quality does not match labels",
                    input_run.run_id
                )));
            }
        }
        if let Some(previous) = input_run.status {
            if previous != status {
                return Err(contract(format!(
                    "{} retained status does not match labels",
                    input_run.run_id
                )));
            }
        }
        output_run.quality = Some(quality);
        output_run.status = Some(status);
        evaluated.push(output_run);
    }

    validate_repetition_matrix(workload, &evaluated)?;
    validate_lifecycle_matrix(&evaluated)?;
    evaluated.sort_by(|left, right| run_sort_key(left).cmp(&run_sort_key(right)));
    let modes = summarize_modes(workload, &evaluated)?;
    let all_pass = modes.iter().all(|summary| {
        matches!(
            summary.status,
            SemanticAblationStatusV1::Pass | SemanticAblationStatusV1::ControlPass
        )
    });
    let output_digest = digest(OUTPUT_DIGEST_DOMAIN, &evaluated)?;
    let matrix_material = evaluated
        .iter()
        .map(|run| {
            (
                run.mode,
                run.query_id.as_str(),
                run.partition.as_str(),
                run.stratum.as_str(),
                run.repetition_kind,
                run.repetition_index,
                run.ranking_digest.as_str(),
                run.production_receipt_digest.as_str(),
                &run.lifecycle,
            )
        })
        .collect::<Vec<_>>();
    let matrix_digest = digest(MATRIX_DIGEST_DOMAIN, &matrix_material)?;
    Ok(SemanticAblationReportV1 {
        schema_version: SEMANTIC_ABLATION_SCHEMA_VERSION,
        command: "semantic_ablation".to_owned(),
        status: if all_pass {
            SemanticAblationStatusV1::Pass
        } else {
            SemanticAblationStatusV1::Fail
        },
        workload_digest: digests.workload_digest,
        corpus_digest: digests.corpus_digest,
        labels_digest: digests.labels_digest,
        source_digest: digests.source_digest,
        model_digest: digests.model_digest,
        projection_digest: digests.projection_digest,
        vector_generation_digest: digests.vector_generation_digest,
        index_digest: digests.index_digest,
        artifact_manifest_digest: digests.artifact_manifest_digest,
        artifact_identity: artifact.identity.clone(),
        output_digest,
        matrix_digest,
        runs: evaluated,
        modes,
    })
}

pub fn validate_semantic_ablation_report(
    report: &SemanticAblationReportV1,
    workload: &SemanticAblationWorkloadV1,
    labels: &SemanticAblationLabelsV1,
    artifact: &SemanticAblationArtifactV1,
    corpus_digest: &str,
) -> Result<(), SemanticAblationErrorV1> {
    let reconstructed =
        evaluate_semantic_ablation(workload, labels, artifact, corpus_digest, &report.runs)?;
    if report != &reconstructed {
        return Err(contract(
            "semantic ablation report digest, aggregates, or retained runs changed",
        ));
    }
    Ok(())
}

fn validate_workload_shape(
    workload: &SemanticAblationWorkloadV1,
) -> Result<(), SemanticAblationErrorV1> {
    if workload.schema_version != SEMANTIC_ABLATION_SCHEMA_VERSION {
        return Err(contract("unsupported semantic ablation workload schema"));
    }
    if workload.workload_id.trim().is_empty()
        || workload.corpus.is_empty()
        || workload.candidates.is_empty()
        || workload.queries.is_empty()
    {
        return Err(contract(
            "semantic ablation workload has an empty required section",
        ));
    }
    if !is_git_identity(&workload.source_repository_commit)
        || !is_git_identity(&workload.source_repository_tree)
    {
        return Err(contract(
            "semantic ablation source commit/tree is not a Git identity",
        ));
    }
    let quality = &workload.quality_contract;
    if quality.top_k != SEMANTIC_ABLATION_TOP_K
        || quality.minimum_candidates_per_query < SEMANTIC_ABLATION_MIN_CANDIDATES
        || quality.minimum_relevant_candidates_per_query == 0
        || quality.recall_at_10_threshold_ppm != SEMANTIC_ABLATION_RECALL_THRESHOLD_PPM
        || quality.precision_at_10_threshold_ppm != SEMANTIC_ABLATION_PRECISION_THRESHOLD_PPM
        || !quality.fixed_precision_denominator
    {
        return Err(contract(
            "semantic ablation quality contract is not the fixed gate",
        ));
    }
    for kind in SemanticAblationRepetitionKindV1::ALL {
        if workload.repetition_contract.count(kind) != SEMANTIC_ABLATION_REPETITIONS {
            return Err(contract(format!(
                "semantic ablation {} repetitions must be exactly {}",
                kind.as_str(),
                SEMANTIC_ABLATION_REPETITIONS
            )));
        }
    }
    ensure_unique(
        workload
            .corpus
            .iter()
            .map(|document| document.document_id.as_str()),
        "corpus document",
    )?;
    let mut corpus_paths = BTreeSet::new();
    for document in &workload.corpus {
        if [
            document.document_id.as_str(),
            document.path.as_str(),
            document.scope.as_str(),
            document.language.as_str(),
            document.eligibility.as_str(),
        ]
        .iter()
        .any(|value| value.trim().is_empty())
            || !is_digest(&document.content_digest)
        {
            return Err(contract(format!(
                "corpus document {} is missing identity or content digest",
                document.document_id
            )));
        }
        if !corpus_paths.insert(document.path.as_str()) {
            return Err(contract(format!("duplicate corpus path {}", document.path)));
        }
    }
    ensure_unique(
        workload
            .candidates
            .iter()
            .map(|candidate| candidate.candidate_id.as_str()),
        "candidate",
    )?;
    ensure_unique(
        workload.queries.iter().map(|query| query.query_id.as_str()),
        "query",
    )?;
    let corpus_by_id = workload
        .corpus
        .iter()
        .map(|document| (document.document_id.as_str(), document))
        .collect::<BTreeMap<_, _>>();
    for candidate in &workload.candidates {
        let Some(document) = corpus_by_id.get(candidate.document_id.as_str()) else {
            return Err(contract(format!(
                "candidate {} cites a document outside the corpus",
                candidate.candidate_id
            )));
        };
        if candidate.candidate_id.trim().is_empty()
            || candidate.scope != document.scope
            || candidate.scope.trim().is_empty()
            || candidate.source_path.trim().is_empty()
            || candidate.symbol.trim().is_empty()
            || !is_digest(&candidate.record_digest)
            || candidate.aliases.is_empty()
            || candidate.terms.is_empty()
            || candidate.subtokens.is_empty()
            || candidate
                .aliases
                .iter()
                .chain(candidate.terms.iter())
                .chain(candidate.subtokens.iter())
                .any(|value| value.trim().is_empty())
        {
            return Err(contract(format!(
                "candidate {} is missing bound record metadata",
                candidate.candidate_id
            )));
        }
        let expected_record = semantic_candidate_record_digest(candidate, document)?;
        if candidate.record_digest != expected_record {
            return Err(contract(format!(
                "candidate {} record digest is forged",
                candidate.candidate_id
            )));
        }
    }
    for query in &workload.queries {
        if query.query_id.trim().is_empty()
            || (query.partition != "train" && query.partition != "validation")
            || query.stratum.trim().is_empty()
            || query.query.trim().is_empty()
            || query.allowed_scopes.is_empty()
            || query
                .allowed_scopes
                .iter()
                .any(|scope| scope.trim().is_empty())
        {
            return Err(contract(format!(
                "query {} is missing required fields",
                query.query_id
            )));
        }
        let eligible = workload
            .candidates
            .iter()
            .filter(|candidate| query.allowed_scopes.contains(&candidate.scope))
            .count();
        if eligible < quality.minimum_candidates_per_query {
            return Err(contract(format!(
                "query {} has only {} eligible candidates",
                query.query_id, eligible
            )));
        }
    }
    Ok(())
}

fn validate_labels(
    workload: &SemanticAblationWorkloadV1,
    labels: &SemanticAblationLabelsV1,
) -> Result<(), SemanticAblationErrorV1> {
    if labels.schema_version != SEMANTIC_ABLATION_SCHEMA_VERSION
        || labels.workload_id != workload.workload_id
        || labels.label_set_id.trim().is_empty()
    {
        return Err(contract("semantic labels do not bind the workload"));
    }
    ensure_unique(
        labels.labels.iter().map(|label| label.query_id.as_str()),
        "label",
    )?;
    let query_ids = workload
        .queries
        .iter()
        .map(|query| query.query_id.as_str())
        .collect::<BTreeSet<_>>();
    let candidate_ids = workload
        .candidates
        .iter()
        .map(|candidate| candidate.candidate_id.as_str())
        .collect::<BTreeSet<_>>();
    if labels.labels.len() != workload.queries.len()
        || labels
            .labels
            .iter()
            .any(|label| !query_ids.contains(label.query_id.as_str()))
    {
        return Err(contract(
            "semantic labels do not cover exactly the workload queries",
        ));
    }
    for label in &labels.labels {
        let query = workload
            .queries
            .iter()
            .find(|query| query.query_id == label.query_id)
            .ok_or_else(|| contract(format!("missing query for {}", label.query_id)))?;
        if label.target_candidate_ids.len()
            < workload
                .quality_contract
                .minimum_relevant_candidates_per_query
        {
            return Err(contract(format!(
                "{} has too few evaluator-owned relevant candidates",
                label.query_id
            )));
        }
        let eligible = workload
            .candidates
            .iter()
            .filter(|candidate| query.allowed_scopes.contains(&candidate.scope))
            .count();
        if eligible
            < label
                .target_candidate_ids
                .len()
                .saturating_add(SEMANTIC_ABLATION_MIN_DECOYS)
        {
            return Err(contract(format!(
                "{} has fewer than {} eligible decoys",
                label.query_id, SEMANTIC_ABLATION_MIN_DECOYS
            )));
        }
        ensure_unique(
            label.target_candidate_ids.iter().map(String::as_str),
            "target label",
        )?;
        ensure_unique(
            label.forbidden_candidate_ids.iter().map(String::as_str),
            "forbidden label",
        )?;
        if label
            .target_candidate_ids
            .iter()
            .chain(label.forbidden_candidate_ids.iter())
            .any(|candidate| !candidate_ids.contains(candidate.as_str()))
            || label
                .target_candidate_ids
                .iter()
                .any(|target| label.forbidden_candidate_ids.contains(target))
            || label.target_candidate_ids.iter().any(|target| {
                workload
                    .candidates
                    .iter()
                    .find(|candidate| candidate.candidate_id == *target)
                    .is_none_or(|candidate| !query.allowed_scopes.contains(&candidate.scope))
            })
        {
            return Err(contract(format!(
                "{} labels an unknown or contradictory candidate",
                label.query_id
            )));
        }
    }
    Ok(())
}

fn validate_artifact_shape(
    workload: &SemanticAblationWorkloadV1,
    artifact: &SemanticAblationArtifactV1,
) -> Result<(), SemanticAblationErrorV1> {
    if artifact.schema_version != SEMANTIC_ABLATION_SCHEMA_VERSION
        || artifact.workload_id != workload.workload_id
        || artifact.model_manifest_path != SEMANTIC_ABLATION_MODEL_MANIFEST_RELATIVE
        || artifact.authority_id != "tracedecay.application.production-semantic-runtime.v1"
        || artifact.search_index_kind != "exact_flat"
        || artifact.index_manifest_digest != artifact.identity.index_artifact_digest
        || !is_digest(&artifact.model_manifest_digest)
    {
        return Err(contract(
            "semantic artifact manifest is not a pinned production manifest",
        ));
    }
    let fields = [
        ("model_id", artifact.identity.model_id.as_str()),
        ("model_revision", artifact.identity.model_revision.as_str()),
        ("projection_id", artifact.identity.projection_id.as_str()),
        (
            "projection_revision",
            artifact.identity.projection_revision.as_str(),
        ),
        (
            "vector_generation_id",
            artifact.identity.vector_generation_id.as_str(),
        ),
        (
            "vector_generation_revision",
            artifact.identity.vector_generation_revision.as_str(),
        ),
        ("index_id", artifact.identity.index_id.as_str()),
        ("index_revision", artifact.identity.index_revision.as_str()),
    ];
    if fields.iter().any(|(_, value)| value.trim().is_empty()) {
        return Err(contract("semantic artifact identity has an empty field"));
    }
    for (name, value) in [
        (
            "model_artifact_digest",
            artifact.identity.model_artifact_digest.as_str(),
        ),
        (
            "projection_artifact_digest",
            artifact.identity.projection_artifact_digest.as_str(),
        ),
        (
            "vector_artifact_digest",
            artifact.identity.vector_artifact_digest.as_str(),
        ),
        (
            "index_artifact_digest",
            artifact.identity.index_artifact_digest.as_str(),
        ),
        (
            "manifest_digest",
            artifact.identity.manifest_digest.as_str(),
        ),
    ] {
        if !is_digest(value) {
            return Err(contract(format!("{name} is not a SHA-256 identity")));
        }
    }
    Ok(())
}

fn validate_zero_overlap(
    workload: &SemanticAblationWorkloadV1,
) -> Result<(), SemanticAblationErrorV1> {
    for query in &workload.queries {
        let query_tokens = production_tokens(&query.query);
        for candidate in &workload.candidates {
            let fields = [
                ("candidate_id", production_tokens(&candidate.candidate_id)),
                ("terms", production_tokens(&candidate.terms.join(" "))),
                (
                    "subtokens",
                    production_tokens(&candidate.subtokens.join(" ")),
                ),
                ("path", production_tokens(&candidate.source_path)),
                ("symbol", production_tokens(&candidate.symbol)),
                ("aliases", production_tokens(&candidate.aliases.join(" "))),
            ];
            for (field, candidate_tokens) in fields {
                let overlap = query_tokens
                    .intersection(&candidate_tokens)
                    .cloned()
                    .collect::<Vec<_>>();
                if !overlap.is_empty() {
                    return Err(contract(format!(
                        "{} has lexical overlap with {} {}: {}",
                        query.query_id,
                        candidate.candidate_id,
                        field,
                        overlap.join(", ")
                    )));
                }
            }
        }
    }
    Ok(())
}

fn validate_run_identity(
    run: &SemanticAblationRunV1,
    digests: &SemanticAblationInputDigestsV1,
) -> Result<(), SemanticAblationErrorV1> {
    if run.schema_version != SEMANTIC_ABLATION_SCHEMA_VERSION
        || run.workload_digest != digests.workload_digest
        || run.corpus_digest != digests.corpus_digest
        || run.labels_digest != digests.labels_digest
        || run.model_digest != digests.model_digest
        || run.projection_digest != digests.projection_digest
        || run.vector_generation_digest != digests.vector_generation_digest
        || run.index_digest != digests.index_digest
        || run.source_digest != digests.source_digest
        || run.artifact_manifest_digest != digests.artifact_manifest_digest
    {
        return Err(contract(format!(
            "{} does not bind every semantic input digest",
            run.run_id
        )));
    }
    if run.artifact_status != SemanticAblationArtifactStatusV1::Verified
        || run.artifact_mismatch
        || run.fallback_used
        || run.failure.is_some()
        || run.latency_micros == 0
    {
        return Err(contract(format!(
            "{} used fallback, mismatched artifacts, failed, or has no measured latency",
            run.run_id
        )));
    }
    Ok(())
}

fn validate_lifecycle(run: &SemanticAblationRunV1) -> Result<(), SemanticAblationErrorV1> {
    let lifecycle = &run.lifecycle;
    if lifecycle.runtime_identity.trim().is_empty()
        || !is_digest(&lifecycle.runtime_identity)
        || lifecycle.lifecycle_receipt_digest
            != semantic_ablation_lifecycle_receipt_digest(lifecycle)?
    {
        return Err(contract(format!(
            "{} lifecycle evidence is malformed",
            run.run_id
        )));
    }
    let expected_phase = run.repetition_kind.as_str();
    if lifecycle.phase != expected_phase {
        return Err(contract(format!(
            "{} lifecycle phase does not match repetition kind",
            run.run_id
        )));
    }
    match run.repetition_kind {
        SemanticAblationRepetitionKindV1::Cold => {
            if !lifecycle.cache_reset || lifecycle.runtime_restart {
                return Err(contract(format!(
                    "{} is not a cold cache reset",
                    run.run_id
                )));
            }
        }
        SemanticAblationRepetitionKindV1::Warm => {
            if lifecycle.cache_reset || lifecycle.runtime_restart {
                return Err(contract(format!("{} is not a warm reuse", run.run_id)));
            }
        }
        SemanticAblationRepetitionKindV1::Restart => {
            if !lifecycle.cache_reset || !lifecycle.runtime_restart {
                return Err(contract(format!("{} is not a runtime restart", run.run_id)));
            }
        }
    }
    Ok(())
}

fn validate_mode_controls(run: &SemanticAblationRunV1) -> Result<(), SemanticAblationErrorV1> {
    let counters = &run.invocation_counters;
    let expected_disabled: Vec<&str> = match run.mode {
        SemanticAblationModeV1::LexicalBaseline => {
            if counters.semantic != 0
                || counters.exact == 0
                || counters.lexical == 0
                || counters.graph == 0
            {
                return Err(contract(format!(
                    "{} lexical baseline invocation counters are not isolated",
                    run.run_id
                )));
            }
            vec!["semantic"]
        }
        SemanticAblationModeV1::SemanticOnly => {
            if counters.exact != 0
                || counters.lexical != 0
                || counters.graph != 0
                || counters.semantic == 0
            {
                return Err(contract(format!(
                    "{} semantic-only did not physically disable exact/lexical/graph",
                    run.run_id
                )));
            }
            vec!["exact", "lexical", "graph"]
        }
        SemanticAblationModeV1::Hybrid => {
            if counters.exact == 0
                || counters.lexical == 0
                || counters.graph == 0
                || counters.semantic == 0
            {
                return Err(contract(format!(
                    "{} hybrid invocation counters do not include every lane",
                    run.run_id
                )));
            }
            Vec::new()
        }
    };
    let observed = run
        .disabled_lanes
        .iter()
        .map(String::as_str)
        .collect::<Vec<_>>();
    if observed != expected_disabled {
        return Err(contract(format!(
            "{} disabled lane set does not match its mode",
            run.run_id
        )));
    }
    Ok(())
}

fn validate_ranked_candidates(
    run: &SemanticAblationRunV1,
    query: &SemanticAblationQueryV1,
    candidates: &BTreeMap<&str, &SemanticAblationCandidateV1>,
    identity: &SemanticAblationArtifactIdentityV1,
) -> Result<(), SemanticAblationErrorV1> {
    if run.ranked_candidates.len() < SEMANTIC_ABLATION_MIN_CANDIDATES {
        return Err(contract(format!(
            "{} does not retain at least ten production candidates",
            run.run_id
        )));
    }
    let expected_provenance = provenance(identity);
    let mut seen = BTreeSet::new();
    let mut prior_score = u32::MAX;
    for (index, ranked) in run.ranked_candidates.iter().enumerate() {
        let Some(candidate) = candidates.get(ranked.candidate_id.as_str()) else {
            return Err(contract(format!(
                "{} ranking cites an unknown candidate",
                run.run_id
            )));
        };
        if !seen.insert(ranked.candidate_id.as_str())
            || ranked.rank != index as u32 + 1
            || ranked.score_ppm > 1_000_000
            || ranked.score_ppm > prior_score
            || ranked.document_id != candidate.document_id
            || ranked.source_path != candidate.source_path
            || ranked.symbol != candidate.symbol
            || ranked.record_digest != candidate.record_digest
            || !query.allowed_scopes.contains(&candidate.scope)
        {
            return Err(contract(format!(
                "{} ranking has forged, duplicate, out-of-scope, or non-deterministic candidate metadata",
                run.run_id
            )));
        }
        prior_score = ranked.score_ppm;
        match run.mode {
            SemanticAblationModeV1::LexicalBaseline => {
                if ranked.channel != "lexical"
                    || ranked.provenance.is_some()
                    || ranked.semantic_distance_micros.is_some()
                {
                    return Err(contract(format!(
                        "{} lexical baseline carries semantic evidence",
                        run.run_id
                    )));
                }
            }
            SemanticAblationModeV1::SemanticOnly | SemanticAblationModeV1::Hybrid => {
                let expected_channel = if run.mode == SemanticAblationModeV1::SemanticOnly {
                    "semantic"
                } else {
                    "hybrid"
                };
                if ranked.channel != expected_channel
                    || ranked.semantic_distance_micros.is_none()
                    || ranked.provenance.as_ref() != Some(&expected_provenance)
                {
                    return Err(contract(format!(
                        "{} semantic candidate provenance is incomplete or foreign",
                        run.run_id
                    )));
                }
            }
        }
    }
    Ok(())
}

fn score_query(
    run: &SemanticAblationRunV1,
    label: &SemanticAblationLabelV1,
    top_k: usize,
) -> Result<SemanticAblationQueryQualityV1, SemanticAblationErrorV1> {
    let targets = label
        .target_candidate_ids
        .iter()
        .map(String::as_str)
        .collect::<BTreeSet<_>>();
    let forbidden = label
        .forbidden_candidate_ids
        .iter()
        .map(String::as_str)
        .collect::<BTreeSet<_>>();
    let top = run
        .ranked_candidates
        .iter()
        .take(top_k)
        .map(|candidate| candidate.candidate_id.as_str())
        .collect::<Vec<_>>();
    let top_unique = top.iter().copied().collect::<BTreeSet<_>>();
    let relevant_hits = top_unique.intersection(&targets).count() as u64;
    if top_unique.intersection(&forbidden).next().is_some() {
        return Err(contract(format!(
            "{} returned an evaluator-forbidden candidate",
            run.run_id
        )));
    }
    Ok(SemanticAblationQueryQualityV1 {
        recall_at_10: ratio(relevant_hits, targets.len() as u64),
        precision_at_10: ratio(relevant_hits, top_k as u64),
        target_count: targets.len() as u64,
        returned_candidates: run.ranked_candidates.len(),
    })
}

fn query_status(
    mode: SemanticAblationModeV1,
    quality: &SemanticAblationQueryQualityV1,
    quality_contract: &SemanticAblationQualityContractV1,
) -> SemanticAblationStatusV1 {
    if mode == SemanticAblationModeV1::LexicalBaseline {
        if quality.recall_at_10.numerator == 0 {
            SemanticAblationStatusV1::ControlPass
        } else {
            SemanticAblationStatusV1::Fail
        }
    } else if at_least(
        &quality.recall_at_10,
        quality_contract.recall_at_10_threshold_ppm,
    ) && at_least(
        &quality.precision_at_10,
        quality_contract.precision_at_10_threshold_ppm,
    ) {
        SemanticAblationStatusV1::Pass
    } else {
        SemanticAblationStatusV1::Fail
    }
}

fn validate_repetition_matrix(
    workload: &SemanticAblationWorkloadV1,
    runs: &[SemanticAblationRunV1],
) -> Result<(), SemanticAblationErrorV1> {
    let expected_total = SemanticAblationModeV1::ALL.len()
        * workload.queries.len()
        * SemanticAblationRepetitionKindV1::ALL.len()
        * SEMANTIC_ABLATION_REPETITIONS as usize;
    if runs.len() != expected_total {
        return Err(contract(format!(
            "semantic ablation matrix has {} runs; expected exactly {}",
            runs.len(),
            expected_total
        )));
    }
    for mode in SemanticAblationModeV1::ALL {
        for query in &workload.queries {
            for kind in SemanticAblationRepetitionKindV1::ALL {
                let mut indices = runs
                    .iter()
                    .filter(|run| {
                        run.mode == mode
                            && run.query_id == query.query_id
                            && run.repetition_kind == kind
                    })
                    .map(|run| run.repetition_index)
                    .collect::<Vec<_>>();
                indices.sort_unstable();
                let expected = (0..SEMANTIC_ABLATION_REPETITIONS).collect::<Vec<_>>();
                if indices != expected {
                    return Err(contract(format!(
                        "{} {} {} repetitions are incomplete or contain extras",
                        mode.as_str(),
                        query.query_id,
                        kind.as_str()
                    )));
                }
            }
        }
    }
    Ok(())
}

/// Lifecycle flags alone are easy to forge, so the matrix also proves the
/// runtime relationship between phases. Cold and warm runs for one query use
/// one runtime identity (cache reset versus reuse); every restart run receives
/// a fresh identity. A digest identity lets this remain portable across a
/// native process adapter and a separately spawned evaluator process.
fn validate_lifecycle_matrix(
    runs: &[SemanticAblationRunV1],
) -> Result<(), SemanticAblationErrorV1> {
    let mut groups = BTreeMap::<
        (SemanticAblationModeV1, String),
        BTreeMap<SemanticAblationRepetitionKindV1, Vec<&SemanticAblationRunV1>>,
    >::new();
    for run in runs {
        groups
            .entry((run.mode, run.query_id.clone()))
            .or_default()
            .entry(run.repetition_kind)
            .or_default()
            .push(run);
    }
    for ((mode, query_id), by_kind) in groups {
        let cold = by_kind
            .get(&SemanticAblationRepetitionKindV1::Cold)
            .ok_or_else(|| contract(format!("{mode:?} {query_id} has no cold lifecycle")))?;
        let warm = by_kind
            .get(&SemanticAblationRepetitionKindV1::Warm)
            .ok_or_else(|| contract(format!("{mode:?} {query_id} has no warm lifecycle")))?;
        let restart = by_kind
            .get(&SemanticAblationRepetitionKindV1::Restart)
            .ok_or_else(|| contract(format!("{mode:?} {query_id} has no restart lifecycle")))?;
        let cold_identities = cold
            .iter()
            .map(|run| run.lifecycle.runtime_identity.as_str())
            .collect::<BTreeSet<_>>();
        let warm_identities = warm
            .iter()
            .map(|run| run.lifecycle.runtime_identity.as_str())
            .collect::<BTreeSet<_>>();
        let restart_identities = restart
            .iter()
            .map(|run| run.lifecycle.runtime_identity.as_str())
            .collect::<BTreeSet<_>>();
        if cold_identities.len() != 1
            || warm_identities.len() != 1
            || cold_identities != warm_identities
            || restart_identities.len() != restart.len()
            || !restart_identities.is_disjoint(&cold_identities)
            || !restart_identities.is_disjoint(&warm_identities)
        {
            return Err(contract(format!(
                "{mode:?} {query_id} lifecycle identities do not prove cold/warm reuse and restart",
            )));
        }
    }
    Ok(())
}

fn summarize_modes(
    workload: &SemanticAblationWorkloadV1,
    runs: &[SemanticAblationRunV1],
) -> Result<Vec<SemanticAblationModeSummaryV1>, SemanticAblationErrorV1> {
    let mut summaries = Vec::new();
    for mode in SemanticAblationModeV1::ALL {
        let mode_runs = runs
            .iter()
            .filter(|run| run.mode == mode)
            .collect::<Vec<_>>();
        if mode_runs.is_empty() {
            return Err(contract(format!("{} has no runs", mode.as_str())));
        }
        let mut strata = Vec::new();
        let mut groups = BTreeSet::new();
        for run in &mode_runs {
            groups.insert((run.partition.clone(), run.stratum.clone()));
        }
        for (partition, stratum) in groups {
            let group = mode_runs
                .iter()
                .copied()
                .filter(|run| run.partition == partition && run.stratum == stratum)
                .collect::<Vec<_>>();
            let mut recall_num = 0_u64;
            let mut recall_den = 0_u64;
            let mut precision_num = 0_u64;
            let mut precision_den = 0_u64;
            for run in &group {
                let quality = run
                    .quality
                    .as_ref()
                    .ok_or_else(|| contract(format!("{} has no retained quality", run.run_id)))?;
                recall_num = recall_num.saturating_add(quality.recall_at_10.numerator);
                recall_den = recall_den.saturating_add(quality.recall_at_10.denominator);
                precision_num = precision_num.saturating_add(quality.precision_at_10.numerator);
                precision_den = precision_den.saturating_add(quality.precision_at_10.denominator);
            }
            let recall = ratio(recall_num, recall_den);
            let precision = ratio(precision_num, precision_den);
            let status = if mode == SemanticAblationModeV1::LexicalBaseline {
                if recall_num == 0 {
                    SemanticAblationStatusV1::ControlPass
                } else {
                    SemanticAblationStatusV1::Fail
                }
            } else if at_least(
                &recall,
                workload.quality_contract.recall_at_10_threshold_ppm,
            ) && at_least(
                &precision,
                workload.quality_contract.precision_at_10_threshold_ppm,
            ) {
                SemanticAblationStatusV1::Pass
            } else {
                SemanticAblationStatusV1::Fail
            };
            strata.push(SemanticAblationStratumMetricsV1 {
                mode,
                partition,
                stratum,
                query_count: group
                    .iter()
                    .map(|run| run.query_id.as_str())
                    .collect::<BTreeSet<_>>()
                    .len() as u64,
                recall_at_10: recall,
                precision_at_10: precision,
                status,
            });
        }
        let status = if strata.iter().all(|stratum| {
            matches!(
                stratum.status,
                SemanticAblationStatusV1::Pass | SemanticAblationStatusV1::ControlPass
            )
        }) {
            if mode == SemanticAblationModeV1::LexicalBaseline {
                SemanticAblationStatusV1::ControlPass
            } else {
                SemanticAblationStatusV1::Pass
            }
        } else {
            SemanticAblationStatusV1::Fail
        };
        summaries.push(SemanticAblationModeSummaryV1 {
            mode,
            query_count: mode_runs
                .iter()
                .map(|run| run.query_id.as_str())
                .collect::<BTreeSet<_>>()
                .len() as u64,
            run_count: mode_runs.len() as u64,
            status,
            strata,
        });
    }
    Ok(summaries)
}

fn artifact_material_digests(
    workload: &SemanticAblationWorkloadV1,
    artifact: &SemanticAblationArtifactV1,
) -> Result<(String, String, String, String, String), SemanticAblationErrorV1> {
    let model_digest = artifact.identity.model_artifact_digest.clone();
    let projection_digest = digest(
        PROJECTION_DIGEST_DOMAIN,
        &(
            artifact.identity.projection_id.as_str(),
            artifact.identity.projection_revision.as_str(),
            model_digest.as_str(),
            artifact.model_manifest_digest.as_str(),
        ),
    )?;
    let corpus_material = workload
        .corpus
        .iter()
        .map(|document| {
            (
                document.document_id.as_str(),
                document.path.as_str(),
                document.content_digest.as_str(),
            )
        })
        .collect::<Vec<_>>();
    let vector_generation_digest = digest(
        VECTOR_DIGEST_DOMAIN,
        &(
            artifact.identity.vector_generation_id.as_str(),
            artifact.identity.vector_generation_revision.as_str(),
            projection_digest.as_str(),
            corpus_material,
        ),
    )?;
    // The immutable index manifest is itself the index artifact identity. Do
    // not derive a second digest from that identity: doing so would make the
    // pinned manifest self-referential and would no longer identify the bytes
    // the production vector reader opened.
    let index_digest = artifact.identity.index_artifact_digest.clone();
    let material = SemanticAblationArtifactMaterial {
        workload_id: workload.workload_id.as_str(),
        model_manifest_path: artifact.model_manifest_path.as_str(),
        model_manifest_digest: artifact.model_manifest_digest.as_str(),
        authority_id: artifact.authority_id.as_str(),
        search_index_kind: artifact.search_index_kind.as_str(),
        index_manifest_digest: artifact.index_manifest_digest.as_str(),
        model_id: artifact.identity.model_id.as_str(),
        model_revision: artifact.identity.model_revision.as_str(),
        model_artifact_digest: model_digest.as_str(),
        projection_id: artifact.identity.projection_id.as_str(),
        projection_revision: artifact.identity.projection_revision.as_str(),
        projection_artifact_digest: projection_digest.as_str(),
        vector_generation_id: artifact.identity.vector_generation_id.as_str(),
        vector_generation_revision: artifact.identity.vector_generation_revision.as_str(),
        vector_artifact_digest: vector_generation_digest.as_str(),
        index_id: artifact.identity.index_id.as_str(),
        index_revision: artifact.identity.index_revision.as_str(),
        index_artifact_digest: index_digest.as_str(),
    };
    let manifest_digest = digest(ARTIFACT_DIGEST_DOMAIN, &material)?;
    Ok((
        model_digest,
        projection_digest,
        vector_generation_digest,
        index_digest,
        manifest_digest,
    ))
}

pub fn provenance(identity: &SemanticAblationArtifactIdentityV1) -> SemanticAblationProvenanceV1 {
    SemanticAblationProvenanceV1 {
        model_id: identity.model_id.clone(),
        model_revision: identity.model_revision.clone(),
        model_artifact_digest: identity.model_artifact_digest.clone(),
        projection_id: identity.projection_id.clone(),
        projection_revision: identity.projection_revision.clone(),
        projection_artifact_digest: identity.projection_artifact_digest.clone(),
        vector_generation_id: identity.vector_generation_id.clone(),
        vector_generation_revision: identity.vector_generation_revision.clone(),
        vector_artifact_digest: identity.vector_artifact_digest.clone(),
        index_id: identity.index_id.clone(),
        index_revision: identity.index_revision.clone(),
        index_artifact_digest: identity.index_artifact_digest.clone(),
    }
}

pub fn semantic_candidate_record_digest(
    candidate: &SemanticAblationCandidateV1,
    document: &SemanticAblationCorpusDocumentV1,
) -> Result<String, SemanticAblationErrorV1> {
    digest(
        "tracedecay.search-eval.semantic-ablation.candidate-record.v1",
        &(
            candidate.candidate_id.as_str(),
            candidate.document_id.as_str(),
            document.path.as_str(),
            document.content_digest.as_str(),
            candidate.scope.as_str(),
            candidate.source_path.as_str(),
            candidate.symbol.as_str(),
            &candidate.aliases,
            &candidate.terms,
            &candidate.subtokens,
        ),
    )
}

fn validate_candidate_source_bindings(
    workload: &SemanticAblationWorkloadV1,
    files: &[(&str, &[u8])],
) -> Result<(), SemanticAblationErrorV1> {
    for candidate in &workload.candidates {
        let document = workload
            .corpus
            .iter()
            .find(|document| document.document_id == candidate.document_id)
            .ok_or_else(|| {
                contract(format!(
                    "candidate {} has no document",
                    candidate.candidate_id
                ))
            })?;
        let bytes = files
            .iter()
            .find_map(|(path, bytes)| (*path == document.path).then_some(*bytes))
            .ok_or_else(|| {
                contract(format!(
                    "candidate {} has no source bytes",
                    candidate.candidate_id
                ))
            })?;
        for (field, value) in [
            ("source path", &candidate.source_path),
            ("symbol", &candidate.symbol),
        ] {
            if !bytes
                .windows(value.len())
                .any(|window| window == value.as_bytes())
            {
                return Err(contract(format!(
                    "candidate {} {} is not bound to corpus bytes",
                    candidate.candidate_id, field
                )));
            }
        }
    }
    Ok(())
}

fn production_tokens(value: &str) -> BTreeSet<String> {
    let mut tokens = BTreeSet::new();
    let mut current = String::new();
    let flush = |current: &mut String, tokens: &mut BTreeSet<String>| {
        if !current.is_empty() {
            for token in split_subtokens(current) {
                if !token.is_empty() {
                    tokens.insert(token);
                }
            }
            current.clear();
        }
    };
    for character in value.chars() {
        if character.is_ascii_alphanumeric() || character == '_' {
            current.push(character);
        } else {
            flush(&mut current, &mut tokens);
        }
    }
    flush(&mut current, &mut tokens);
    tokens
}

fn digest<T: Serialize>(domain: &str, value: &T) -> Result<String, SemanticAblationErrorV1> {
    canonical_sha256(&(domain, value))
        .map_err(|error| contract(format!("hash semantic evidence: {error}")))
}

fn tagged_digest(bytes: &[u8]) -> String {
    encode_tagged_lowercase_hex("sha256:", &Sha256::digest(bytes))
}

fn ratio(numerator: u64, denominator: u64) -> SemanticAblationRatioV1 {
    let ppm = if denominator == 0 {
        0
    } else {
        u32::try_from(
            u128::from(numerator)
                .saturating_mul(1_000_000)
                .checked_div(u128::from(denominator))
                .unwrap_or(0)
                .min(1_000_000),
        )
        .unwrap_or(1_000_000)
    };
    SemanticAblationRatioV1 {
        numerator,
        denominator,
        ppm,
    }
}

fn at_least(metric: &SemanticAblationRatioV1, threshold_ppm: u32) -> bool {
    u128::from(metric.numerator).saturating_mul(1_000_000)
        >= u128::from(metric.denominator).saturating_mul(u128::from(threshold_ppm))
}

fn run_sort_key(
    run: &SemanticAblationRunV1,
) -> (
    SemanticAblationModeV1,
    &str,
    &str,
    &str,
    SemanticAblationRepetitionKindV1,
    u32,
) {
    (
        run.mode,
        run.partition.as_str(),
        run.query_id.as_str(),
        run.stratum.as_str(),
        run.repetition_kind,
        run.repetition_index,
    )
}

fn ensure_unique<'a>(
    values: impl IntoIterator<Item = &'a str>,
    label: &str,
) -> Result<(), SemanticAblationErrorV1> {
    let mut seen = BTreeSet::new();
    for value in values {
        if !seen.insert(value) {
            return Err(contract(format!("duplicate {label} {value}")));
        }
    }
    Ok(())
}

fn is_digest(value: &str) -> bool {
    value.strip_prefix("sha256:").is_some_and(|hex| {
        hex.len() == 64
            && hex
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    })
}

fn is_git_identity(value: &str) -> bool {
    value.len() == 40
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

fn contract(message: impl Into<String>) -> SemanticAblationErrorV1 {
    SemanticAblationErrorV1::Contract(message.into())
}

/// Byte-pinned evaluator assets. Labels stay a separate object and are never
/// passed to a production authority.
pub mod packaged {
    use super::*;

    const WORKLOAD_BYTES: &[u8] = include_bytes!(
        "../../assets/runtime-root/tests/fixtures/search_quality/semantic-ablation/query-semantic-ablation-workload-v1.json"
    );
    const LABELS_BYTES: &[u8] = include_bytes!(
        "../../assets/runtime-root/tests/fixtures/search_quality/semantic-ablation/labels-v1.json"
    );
    const ARTIFACT_BYTES: &[u8] = include_bytes!(
        "../../assets/runtime-root/tests/fixtures/search_quality/semantic-ablation/artifact-v1.json"
    );
    const CORPUS_BYTES: &[u8] = include_bytes!(
        "../../assets/runtime-root/tests/fixtures/search_quality/semantic-ablation/corpus/semantic_catalog.rs"
    );
    const MODEL_MANIFEST_BYTES: &[u8] =
        include_bytes!("../../../../product/semantic/model-manifest.json");

    pub fn workload() -> Result<SemanticAblationWorkloadV1, SemanticAblationErrorV1> {
        parse(WORKLOAD_BYTES, SEMANTIC_ABLATION_WORKLOAD_RELATIVE)
    }

    pub fn labels() -> Result<SemanticAblationLabelsV1, SemanticAblationErrorV1> {
        parse(LABELS_BYTES, SEMANTIC_ABLATION_LABELS_RELATIVE)
    }

    pub fn artifact() -> Result<SemanticAblationArtifactV1, SemanticAblationErrorV1> {
        parse(ARTIFACT_BYTES, SEMANTIC_ABLATION_ARTIFACT_RELATIVE)
    }

    pub fn corpus_files() -> &'static [(&'static str, &'static [u8])] {
        &[(SEMANTIC_ABLATION_CORPUS_RELATIVE, CORPUS_BYTES)]
    }

    pub fn model_manifest_bytes() -> &'static [u8] {
        MODEL_MANIFEST_BYTES
    }

    pub fn inputs() -> Result<
        (
            SemanticAblationWorkloadV1,
            SemanticAblationLabelsV1,
            SemanticAblationArtifactV1,
            SemanticAblationInputDigestsV1,
        ),
        SemanticAblationErrorV1,
    > {
        let workload = workload()?;
        let labels = labels()?;
        let artifact = artifact()?;
        validate_semantic_model_manifest(&artifact, MODEL_MANIFEST_BYTES)?;
        let corpus_digest = compute_semantic_corpus_digest(&workload, corpus_files())?;
        let digests =
            validate_semantic_ablation_inputs(&workload, &labels, &artifact, &corpus_digest)?;
        Ok((workload, labels, artifact, digests))
    }

    fn parse<T>(bytes: &[u8], path: &str) -> Result<T, SemanticAblationErrorV1>
    where
        T: for<'de> Deserialize<'de>,
    {
        serde_json::from_slice(bytes).map_err(|source| SemanticAblationErrorV1::Parse {
            path: path.to_owned(),
            source,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn packaged_inputs_have_no_ranking_or_label_generation_seam() {
        let (workload, labels, artifact, digests) = packaged::inputs().expect("semantic fixture");
        assert_eq!(labels.labels.len(), workload.queries.len());
        assert!(is_digest(&digests.workload_digest));
        assert!(is_digest(&digests.labels_digest));
        let bytes = serde_json::to_vec(&artifact).expect("artifact JSON");
        assert!(
            !String::from_utf8(bytes)
                .expect("JSON text")
                .contains("rankings")
        );
    }

    #[test]
    fn exact_repetition_contract_rejects_other_counts() {
        let (mut workload, labels, artifact, digests) =
            packaged::inputs().expect("semantic fixture");
        workload.repetition_contract.warm = SEMANTIC_ABLATION_REPETITIONS + 1;
        let error = validate_semantic_ablation_inputs(
            &workload,
            &labels,
            &artifact,
            &digests.corpus_digest,
        )
        .expect_err("extra repetitions must fail input validation");
        assert!(error.to_string().contains("exactly"));
    }

    #[test]
    fn metadata_and_candidate_record_mutations_fail() {
        let (mut workload, labels, artifact, digests) =
            packaged::inputs().expect("semantic fixture");
        workload.candidates[0].source_path.push_str("-forged");
        let error = validate_semantic_ablation_inputs(
            &workload,
            &labels,
            &artifact,
            &digests.corpus_digest,
        )
        .expect_err("forged candidate metadata must fail");
        assert!(error.to_string().contains("record digest"));
    }

    #[test]
    fn corpus_byte_mutation_fails_before_digest_acceptance() {
        let (workload, _labels, _artifact, _digests) =
            packaged::inputs().expect("semantic fixture");
        let tampered = b"tampered semantic corpus";
        let files = [(SEMANTIC_ABLATION_CORPUS_RELATIVE, tampered.as_slice())];
        let error = compute_semantic_corpus_digest(&workload, &files)
            .expect_err("corpus bytes must remain bound to the declared record digest");
        assert!(error.to_string().contains("content digest"));
    }
}
