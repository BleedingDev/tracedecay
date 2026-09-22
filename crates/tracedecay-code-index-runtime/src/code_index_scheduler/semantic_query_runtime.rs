//! Exact-scope activation and execution for optional semantic augmentation.
//!
//! Query fallback remains an exact three-lane profile. Semantic influence is
//! separately activated with exact calibration and vector compatibility pins.

use std::collections::BTreeSet;
use std::path::Path;
use std::sync::Arc;

use thiserror::Error;
use tokio::task;
use tracedecay_contracts::ResolvedScope;
use tracedecay_domain::{
    EphemeralSanitizedQueryViewV1, OptionalStagePublicStatus, RetrievalRequest, RetrieverKind,
    SemanticRetrievalContinuationV1, SemanticSourceScopeV1, canonical_sha256,
};
use tracedecay_semantic::SemanticModelLifecycleOwnerV1;
use tracedecay_semantic_contracts::RerankCompatibilityPinsV1;

use super::CodeIndexSchedulerRegistryV1;
use super::query_runtime::{
    ExecutedQuerySearchV1, QuerySearchExecutionErrorV1, QuerySearchExecutionRequestV1,
};
use super::registry::unique_mounted_for_scope;
use crate::code_index::production::CodeIndexPublishedGenerationV1;
use crate::config::retrieval::SemanticCompatibilityPinsV1;
use tracedecay_application::semantic_runtime::{
    AuthorizedProjectSemanticSearchParametersV1, CommittedRetrievalProfileStateV1,
    ProductionProjectSemanticSearchBridgeV1, ProductionSemanticRetrievalConfigurationStoreV1,
    SemanticConfigurationPinV1, SemanticCurrentLinkedActivationV1,
};
use tracedecay_query::retrieval::AuthorizedQueryFallbackV1;
use tracedecay_query::retrieval::QueryAuthorityV1;
use tracedecay_query::retrieval::fusion::{CompositionOutputV1, digest_candidate_set};
use tracedecay_query::retrieval::ports::RetrievalExecutionControl;
use tracedecay_query::retrieval::semantic::{
    SemanticAbstentionDispositionV1, SemanticAbstentionV1, SemanticCompositionExecutionAuthorityV1,
    SemanticCompositionExecutionOutcomeV1, SemanticQueryModeV1, SemanticQueryServiceError,
    SemanticRetrievalRequestV1, apply_bounded_rerank_outcome,
};
use tracedecay_semantic::rerank_adapter::ProductionCodeRerankAuthorityV1;

#[derive(Clone)]
pub struct SemanticQueryAuthorityV1 {
    activation: SemanticCurrentLinkedActivationV1,
    query_profile_id: tracedecay_domain::FusionProfileId,
    profile_digest: tracedecay_domain::ManifestDigest,
    execution: SemanticCompositionExecutionAuthorityV1,
    rerank: Option<ConfiguredRerankAuthorityV1>,
}

#[derive(Clone)]
struct ConfiguredRerankAuthorityV1 {
    pins: RerankCompatibilityPinsV1,
    mounted: Option<ProductionCodeRerankAuthorityV1>,
}

/// All identities that a semantic cursor must carry in addition to the
/// profile, projection, vector, index, and ranking identities shared by the
/// historical scheduler route. Keeping these values together prevents a
/// continuation from silently crossing a model, capability, privacy, or
/// source-scope boundary when the active route changes.
#[derive(Clone)]
struct SemanticRouteIdentityV1 {
    model_artifact_digest: tracedecay_domain::ManifestDigest,
    execution_provider: tracedecay_domain::EmbeddingExecutionProviderV1,
    capability_manifest_digest: tracedecay_domain::ManifestDigest,
    privacy_domain: tracedecay_domain::PrivacyDomainId,
    privacy_key_epoch: u64,
    source_scope: SemanticSourceScopeV1,
}

impl SemanticRouteIdentityV1 {
    fn from_active(
        pins: &SemanticCompatibilityPinsV1,
        scope: &ResolvedScope,
        source_reference: Option<tracedecay_domain::RefId>,
    ) -> Self {
        let embedding_key = pins.projection.embedding_key();
        Self {
            model_artifact_digest: embedding_key.model_artifact_digest.clone(),
            execution_provider: embedding_key.execution_provider,
            capability_manifest_digest: pins.calibration.capability_manifest_digest.clone(),
            privacy_domain: pins.projection.privacy_domain().clone(),
            privacy_key_epoch: pins.projection.privacy_key_epoch(),
            source_scope: SemanticSourceScopeV1 {
                project_id: scope.project_id.clone(),
                repository_id: scope.repository_id.clone(),
                worktree_id: scope.worktree_id.clone(),
                // The branch label is attribution on the immutable source
                // generation. It may move on the same checkout after the
                // route was mounted, so bind the generation's sealed label
                // rather than the caller's current label.
                reference: source_reference,
            },
        }
    }

    /// The pagination unit tests exercise cursor mechanics without mounting a
    /// production semantic activation. Give that legacy helper a complete,
    /// structurally valid identity so it still drives the same authenticated
    /// continuation contract as production callers.
    fn for_pagination_test(
        request: &RetrievalRequest,
        authorized_query: &AuthorizedQueryFallbackV1,
        projection_key: &tracedecay_domain::ProjectionKeyV1,
    ) -> Self {
        Self {
            model_artifact_digest: projection_key.profile_digest.clone(),
            execution_provider: tracedecay_domain::EmbeddingExecutionProviderV1::Cpu,
            capability_manifest_digest: projection_key.profile_digest.clone(),
            privacy_domain: request.scope.privacy_domain.clone(),
            privacy_key_epoch: authorized_query.query_digest.key_epoch,
            source_scope: SemanticSourceScopeV1 {
                project_id: tracedecay_domain::ProjectId::new(
                    "project.semantic-pagination-test.v1",
                )
                .expect("pagination test project identity"),
                repository_id: request.scope.root.repository.clone(),
                worktree_id: request.scope.root.worktree.clone().unwrap_or_else(|| {
                    tracedecay_domain::WorktreeId::new("worktree.semantic-pagination-test.v1")
                        .expect("pagination test worktree identity")
                }),
                reference: request.scope.root.reference.clone(),
            },
        }
    }
}

impl SemanticQueryAuthorityV1 {
    /// Bind a committed semantic activation to the exact query profile the
    /// scope's fallback lanes execute.
    ///
    /// The serving query profile is supplied by the query authority that owns
    /// it. It cannot be re-derived from the committed state: activation moves
    /// the profile it displaced into the rollback slot, so the evaluated query
    /// profile occupies neither slot once a second activation commits.
    pub fn from_committed(
        committed: CommittedRetrievalProfileStateV1,
        query_profile_id: tracedecay_domain::FusionProfileId,
        _lifecycle_owner: Option<Arc<SemanticModelLifecycleOwnerV1>>,
    ) -> Result<Self, SemanticQueryAuthorityErrorV1> {
        let activation = committed
            .current_activation
            .ok_or(SemanticQueryAuthorityErrorV1::SemanticNotActivated)?;
        if activation.authority_receipt.is_none() {
            // A legacy runtime receipt proves only the pointer swap. Serving
            // semantic queries requires the durable authority proof for the
            // complete model/projection/index/capability binding.
            return Err(SemanticQueryAuthorityErrorV1::IncompatibleActivation);
        }
        // Configuration reads are durable authority, but keep the serving
        // mount defensive when a test/recovery caller supplies a manually
        // assembled committed state. Re-run the linked activation validator so
        // model artifact, runtime, projection, vector, index, calibration,
        // capability, and privacy identities are all admitted together.
        if SemanticCurrentLinkedActivationV1::new_with_authority(
            activation.receipt.clone(),
            activation.compatibility.clone(),
            activation.authority_receipt.clone(),
        )
        .is_err()
        {
            return Err(SemanticQueryAuthorityErrorV1::IncompatibleActivation);
        }
        let pins = &activation.compatibility;
        let accepted = committed.state.active();
        let lanes = accepted
            .profile()
            .calibrations
            .keys()
            .copied()
            .collect::<BTreeSet<_>>();
        let expected_lanes = BTreeSet::from([
            RetrieverKind::ExactLiteral,
            RetrieverKind::Lexical,
            RetrieverKind::Graph,
            RetrieverKind::Semantic,
        ]);
        let rerank_policy = accepted.rerank().cloned();
        let rerank_pins = accepted.compatibility().rerank.clone();
        let profile_digest = accepted.profile_digest().clone();
        let calibration_digest = pins.calibration.canonical_digest().ok();
        let resource_digest = canonical_sha256((
            "tracedecay.semantic-resource-requirement.v1",
            &pins.resources,
        ))
        .ok();
        let authority_binding_is_consistent =
            activation
                .authority_receipt
                .as_ref()
                .is_some_and(|authority_receipt| {
                    authority_receipt.validate().is_ok()
                        && authority_receipt.binding.model_artifact_digest
                            == pins.artifact_manifest_digest
                        && authority_receipt.binding.projection_key_digest
                            == pins.projection.projection_key().profile_digest
                        && authority_receipt.binding.search_index_key_digest
                            == pins.search_index_key.profile_digest
                        && authority_receipt.binding.capability_digest
                            == pins.calibration.capability_manifest_digest
                        && calibration_digest.as_ref()
                            == Some(&authority_receipt.binding.calibration_digest)
                        && resource_digest.as_ref()
                            == Some(&authority_receipt.binding.resource_digest)
                        && authority_receipt.binding.runtime_digest
                            == pins.runtime_compatibility_digest
                        && authority_receipt.binding.privacy_epoch
                            == pins.projection.privacy_key_epoch()
                });
        if let Some(authority_receipt) = activation.authority_receipt.as_ref()
            && (authority_receipt.binding.profile_digest != profile_digest
                || authority_receipt.configuration_revision
                    != activation.receipt.configuration.revision_id)
        {
            return Err(SemanticQueryAuthorityErrorV1::IncompatibleActivation);
        }
        if activation.receipt.activated_generation != pins.vector_generation_id
            || pins.calibration.projection_key != *pins.projection.projection_key()
            || pins.calibration.vector_generation != pins.vector_generation_id
            || pins.calibration.canonical_digest().is_err()
            || pins.search_index_key.validate().is_err()
            || pins.projection.embedding_key().model_artifact_digest
                != pins.artifact_manifest_digest
            || !authority_binding_is_consistent
            || lanes != expected_lanes
            || accepted
                .profile()
                .weights_micros
                .keys()
                .copied()
                .collect::<BTreeSet<_>>()
                != expected_lanes
            || accepted.profile().rerank_policy_id.as_ref()
                != rerank_policy.as_ref().map(|policy| &policy.policy_id)
            || rerank_policy.is_some() != rerank_pins.is_some()
            || rerank_policy.as_ref().is_some_and(|policy| {
                policy.evaluation_result_anchor != accepted.profile().evaluation_result_anchor
            })
            || accepted.compatibility().semantic.as_ref() != Some(pins)
        {
            return Err(SemanticQueryAuthorityErrorV1::IncompatibleActivation);
        }
        // V2's semantic composition authority keeps reranking as an admitted
        // identity but does not open a model on the query path. A lifecycle
        // owner may still be supplied by project-open callers for future
        // warming, but serving only uses an already mounted authority and
        // therefore records this stage as unavailable when no such mount is
        // injected. In particular, mounting the query route must never start
        // model acquisition or reconstruction.
        let rerank = rerank_pins.map(|pins| ConfiguredRerankAuthorityV1 {
            pins,
            mounted: None,
        });
        let execution = SemanticCompositionExecutionAuthorityV1::new(
            accepted.profile().clone(),
            accepted.diversity().clone(),
            accepted.rerank().cloned(),
            pins.fusion_revision.clone(),
        )
        .map_err(|error| SemanticQueryAuthorityErrorV1::Mount(error.to_string()))?;
        Ok(Self {
            activation,
            query_profile_id,
            profile_digest,
            execution,
            rerank,
        })
    }

    fn pins(&self) -> &SemanticCompatibilityPinsV1 {
        &self.activation.compatibility
    }
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum SemanticQueryAuthorityErrorV1 {
    #[error("semantic configuration authority is unavailable")]
    Unavailable,
    #[error("no active semantic PASS profile exists for the exact scope")]
    SemanticNotActivated,
    #[error("semantic configuration scope does not match the admitted scope")]
    ScopeMismatch,
    #[error("semantic activation pins are incompatible")]
    IncompatibleActivation,
    #[error("semantic authority mount failed: {0}")]
    Mount(String),
}

pub struct ExecutedQuerySemanticSearchV1 {
    pub query: ExecutedQuerySearchV1,
    pub semantic: SemanticAugmentationOutcomeV1,
}

/// Augmented payload carried behind a `Box` so the augmented arm does not
/// dominate the size of every `SemanticAugmentationOutcomeV1`, whose abstaining
/// arm holds only a reason and the shared query fallback.
pub struct SemanticAugmentedCompositionV1 {
    pub composition: CompositionOutputV1,
    pub cursor: Option<tracedecay_domain::RetrievalCursor>,
    pub hydration_budget: tracedecay_domain::RetrievalBudget,
    /// Shared query fallback carried for test identity assertions only.
    pub fallback: Arc<tracedecay_domain::QueryFallbackSubpayload>,
}

pub enum SemanticAugmentationOutcomeV1 {
    Augmented(Box<SemanticAugmentedCompositionV1>),
    Fallback {
        abstention: SemanticAbstentionV1,
        fallback: Arc<tracedecay_domain::QueryFallbackSubpayload>,
    },
}

#[cfg(test)]
impl SemanticAugmentationOutcomeV1 {
    fn fallback(&self) -> &Arc<tracedecay_domain::QueryFallbackSubpayload> {
        match self {
            Self::Augmented(augmented) => &augmented.fallback,
            Self::Fallback { fallback, .. } => fallback,
        }
    }
}

#[derive(Debug, Error)]
pub enum QuerySemanticSearchExecutionErrorV1 {
    #[error(transparent)]
    Query(#[from] QuerySearchExecutionErrorV1),
    #[error(
        "strict semantic retrieval is unavailable for code generation {generation}: {abstention:?}"
    )]
    StrictSemanticUnavailable {
        generation: tracedecay_domain::CodeGenerationId,
        abstention: SemanticAbstentionV1,
    },
    #[error(transparent)]
    Semantic(#[from] SemanticQueryServiceError),
}

fn bind_semantic_execution_error(
    generation: &tracedecay_domain::CodeGenerationId,
    error: SemanticQueryServiceError,
) -> QuerySemanticSearchExecutionErrorV1 {
    match error {
        SemanticQueryServiceError::StrictUnavailable(abstention) => {
            QuerySemanticSearchExecutionErrorV1::StrictSemanticUnavailable {
                generation: generation.clone(),
                abstention,
            }
        }
        error => QuerySemanticSearchExecutionErrorV1::Semantic(error),
    }
}

pub const fn semantic_abstention_reason(abstention: &SemanticAbstentionV1) -> &'static str {
    match abstention {
        SemanticAbstentionV1::IndexUnavailable => "semantic_index_unavailable",
        SemanticAbstentionV1::Indexing => "semantic_indexing",
        SemanticAbstentionV1::IndexDegraded => "semantic_degraded",
        SemanticAbstentionV1::IndexFailed => "semantic_failed",
        SemanticAbstentionV1::IndexStale => "semantic_generation_stale",
        SemanticAbstentionV1::IndexIncompatible => "semantic_generation_incompatible",
        SemanticAbstentionV1::CalibrationUnavailable => "calibration_unavailable",
        SemanticAbstentionV1::CalibrationInvalid => "calibration_invalid",
        SemanticAbstentionV1::CalibrationShifted => "calibration_shifted",
        SemanticAbstentionV1::NoCandidates => "semantic_no_candidates",
        SemanticAbstentionV1::BelowAcceptanceThreshold => "semantic_below_threshold",
        SemanticAbstentionV1::AmbiguousTopCandidates => "semantic_ambiguous",
        SemanticAbstentionV1::PartialCoverage => "semantic_partial",
        SemanticAbstentionV1::SemanticUnavailable => "semantic_unavailable",
        SemanticAbstentionV1::Cancelled => "semantic_cancelled",
        SemanticAbstentionV1::TimedOut => "semantic_timed_out",
        SemanticAbstentionV1::BudgetExceeded => "semantic_budget_exceeded",
        SemanticAbstentionV1::Denied => "semantic_denied",
        SemanticAbstentionV1::Stale => "semantic_stale",
        SemanticAbstentionV1::LaneFailure => "semantic_lane_failed",
    }
}

pub async fn mount_current_semantic_query_authority_on_project_open(
    registry: &CodeIndexSchedulerRegistryV1,
    project_root: &Path,
    scope: &ResolvedScope,
    configuration: &ProductionSemanticRetrievalConfigurationStoreV1,
    configuration_pin: &SemanticConfigurationPinV1,
) -> Result<(), SemanticQueryAuthorityErrorV1> {
    scope
        .validate()
        .map_err(|_| SemanticQueryAuthorityErrorV1::ScopeMismatch)?;
    let committed = configuration
        .current_committed_profile_state(configuration_pin)
        .await
        .map_err(|_| SemanticQueryAuthorityErrorV1::Unavailable)?;
    if committed.scope != *scope {
        return Err(SemanticQueryAuthorityErrorV1::ScopeMismatch);
    }
    registry
        .mount_semantic_query_authority_from_committed(project_root, scope, committed)
        .await
}

impl CodeIndexSchedulerRegistryV1 {
    pub async fn mount_semantic_query_authority_from_committed(
        &self,
        project_root: &Path,
        scope: &ResolvedScope,
        committed: CommittedRetrievalProfileStateV1,
    ) -> Result<(), SemanticQueryAuthorityErrorV1> {
        if committed.scope != *scope {
            return Err(SemanticQueryAuthorityErrorV1::ScopeMismatch);
        }
        // The semantic authority binds to the query profile this scope is
        // actually serving, which only the mounted query authority knows.
        let query_profile_id = self
            .query_authority_for_scope(scope)
            .await
            .ok_or(SemanticQueryAuthorityErrorV1::Unavailable)?
            .profile()
            .profile_id
            .clone();
        let lifecycle_owner = self.semantic_lifecycle_owner_for_scope(scope).await;
        let authority = task::spawn_blocking(move || {
            SemanticQueryAuthorityV1::from_committed(committed, query_profile_id, lifecycle_owner)
        })
        .await
        .map_err(|error| SemanticQueryAuthorityErrorV1::Mount(error.to_string()))??;
        let authority = Arc::new(authority);
        self.mount_semantic_query_authority(project_root, scope, authority)
            .await
    }

    pub async fn mount_semantic_query_authority(
        &self,
        project_root: &Path,
        scope: &ResolvedScope,
        authority: Arc<SemanticQueryAuthorityV1>,
    ) -> Result<(), SemanticQueryAuthorityErrorV1> {
        scope
            .validate()
            .map_err(|_| SemanticQueryAuthorityErrorV1::ScopeMismatch)?;
        let project_root = project_root
            .canonicalize()
            .map_err(|error| SemanticQueryAuthorityErrorV1::Mount(error.to_string()))?;
        let mut mounted = self.mounted.lock().await;
        let worktree = mounted
            .get_mut(&project_root)
            .ok_or(SemanticQueryAuthorityErrorV1::Unavailable)?;
        if worktree.repository_id != scope.repository_id
            || worktree.worktree_id != scope.worktree_id
        {
            return Err(SemanticQueryAuthorityErrorV1::ScopeMismatch);
        }
        if worktree.query_activation_revision.is_some() {
            return Err(SemanticQueryAuthorityErrorV1::Mount(
                "standalone semantic authority cannot replace a committed authority pair"
                    .to_owned(),
            ));
        }
        worktree.semantic_query_authority = Some((scope.scope_digest.clone(), authority));
        Ok(())
    }

    pub async fn semantic_lifecycle_owner_for_scope(
        &self,
        scope: &ResolvedScope,
    ) -> Option<Arc<SemanticModelLifecycleOwnerV1>> {
        scope.validate().ok()?;
        let mounted = self.mounted.lock().await;
        let (_, worktree) = unique_mounted_for_scope(&mounted, scope).unique()?;
        worktree.semantic_lifecycle_owner.clone()
    }

    /// The installed semantic route for one exact admitted scope.
    ///
    /// Worktree isolation is `unique_mounted_for_scope`, exactly as in
    /// `query_authority_for_scope`, and for the reason
    /// `ResolvedScope::identifies_same_checkout` documents: the scope digest
    /// also binds `reference`, the branch label HEAD happened to carry when
    /// the activation was sealed, and that label moves under a fixed worktree
    /// on every ordinary commit, branch switch, or detached checkout.
    /// Comparing it here denied the committed semantic authority the moment
    /// HEAD moved -- an explicit profile rollback installs coherently against
    /// the restored source and then every strict query abstained
    /// `CalibrationUnavailable` with nothing to point at. Serving eligibility
    /// is checkout identity plus the per-query source-coherence gates; the
    /// stored digest stays on the entry as the label the route was sealed
    /// under.
    async fn semantic_query_authority_for_scope(
        &self,
        scope: &ResolvedScope,
    ) -> Option<Arc<SemanticQueryAuthorityV1>> {
        let mounted = self.mounted.lock().await;
        unique_mounted_for_scope(&mounted, scope)
            .unique()
            .and_then(|(_root, worktree)| {
                worktree
                    .semantic_query_authority
                    .as_ref()
                    .map(|(_scope_digest, authority)| Arc::clone(authority))
            })
    }

    /// The semantic compatibility pins a query on `scope` would serve, or
    /// `None` when no committed semantic route is reachable from it.
    #[cfg(any(test, feature = "test-helpers"))]
    pub async fn served_semantic_pins_for_scope(
        &self,
        scope: &ResolvedScope,
    ) -> Option<SemanticCompatibilityPinsV1> {
        Some(
            self.semantic_query_authority_for_scope(scope)
                .await?
                .pins()
                .clone(),
        )
    }

    /// Run canonical query first, then attempt semantic influence against the
    /// same authenticated query and immutable code generation.
    #[hotpath::measure(future = true)]
    pub async fn execute_query_with_semantic<C>(
        &self,
        project_root: &Path,
        scope: &ResolvedScope,
        input: QuerySearchExecutionRequestV1,
        control: Arc<C>,
        mode: SemanticQueryModeV1,
    ) -> Result<ExecutedQuerySemanticSearchV1, QuerySemanticSearchExecutionErrorV1>
    where
        C: RetrievalExecutionControl + 'static,
    {
        // The serving boundary consumes only an already-mounted semantic
        // route. Model loading and semantic vector/index preparation belong to
        // project-open/background lifecycle work and never start from this
        // request.
        let query = hotpath::future!(
            self.execute_controlled_query(scope, input, control.clone()),
            label = "daemon.query.semantic.canonical"
        )
        .await?;
        if hotpath::future!(
            self.semantic_query_authority_for_scope(scope),
            label = "daemon.query.semantic.activation_lookup"
        )
        .await
        .is_none()
        {
            let semantic = semantic_abstention(
                mode,
                SemanticAbstentionV1::CalibrationUnavailable,
                Arc::clone(&query.authorized.fallback),
            )
            .map_err(|error| bind_semantic_execution_error(&query.generation, error))?;
            return Ok(ExecutedQuerySemanticSearchV1 { query, semantic });
        }
        let latest = match hotpath::future!(
            self.generation_for(scope, &query.generation),
            label = "daemon.query.semantic.generation_lookup"
        )
        .await
        {
            Ok(Some(latest)) => latest,
            Ok(None) => {
                let semantic = semantic_abstention(
                    mode,
                    SemanticAbstentionV1::IndexStale,
                    Arc::clone(&query.authorized.fallback),
                )
                .map_err(|error| bind_semantic_execution_error(&query.generation, error))?;
                return Ok(ExecutedQuerySemanticSearchV1 { query, semantic });
            }
            Err(reason) => {
                return Err(QuerySemanticSearchExecutionErrorV1::Query(
                    super::query_runtime::QuerySearchExecutionErrorV1::ExactGenerationUnavailable(
                        reason,
                    ),
                ));
            }
        };
        let semantic = hotpath::future!(
            self.execute_semantic_after_query(
                project_root,
                scope,
                &latest.generation,
                query.sanitized.request(),
                query.sanitized.query_view(),
                &query.authorized,
                control.as_ref(),
                mode,
            ),
            label = "daemon.query.semantic.augment"
        )
        .await
        .map_err(|error| bind_semantic_execution_error(&query.generation, error))?;
        Ok(ExecutedQuerySemanticSearchV1 { query, semantic })
    }

    /// Execute optional semantic augmentation against the exact active config,
    /// code generation, vector generation, calibration, and authenticated QUERY
    /// query. Every abstention returns the original canonical query `Arc`.
    #[allow(clippy::too_many_arguments)]
    #[hotpath::measure(future = true)]
    pub async fn execute_semantic_after_query<C>(
        &self,
        project_root: &Path,
        scope: &ResolvedScope,
        code_generation: &CodeIndexPublishedGenerationV1,
        base: &RetrievalRequest,
        query_view: &EphemeralSanitizedQueryViewV1,
        authorized_query: &AuthorizedQueryFallbackV1,
        control: &C,
        mode: SemanticQueryModeV1,
    ) -> Result<SemanticAugmentationOutcomeV1, SemanticQueryServiceError>
    where
        C: RetrievalExecutionControl + Sync,
    {
        let Some(authority) = hotpath::future!(
            self.semantic_query_authority_for_scope(scope),
            label = "daemon.query.semantic.activation_revalidate"
        )
        .await
        else {
            return semantic_abstention(
                mode,
                SemanticAbstentionV1::CalibrationUnavailable,
                Arc::clone(&authorized_query.fallback),
            );
        };
        if authorized_query.composition.profile_id != authority.query_profile_id {
            return semantic_abstention(
                mode,
                SemanticAbstentionV1::Stale,
                Arc::clone(&authorized_query.fallback),
            );
        }
        let pins = authority.pins();
        let manifest = code_generation.manifest();
        let snapshot = code_generation.snapshot();
        let route = SemanticRouteIdentityV1::from_active(pins, scope, snapshot.reference.clone());
        let source_generation_matches_activation = authority
            .activation
            .authority_receipt
            .as_ref()
            .is_some_and(|receipt| receipt.binding.source_generation == manifest.generation_id);
        let source_scope_matches_generation = source_generation_matches_activation
            && manifest.project_id == scope.project_id
            && snapshot.repository == scope.repository_id
            && snapshot.worktree.as_ref() == Some(&scope.worktree_id)
            && snapshot.reference == route.source_scope.reference
            && manifest.privacy_domain == route.privacy_domain
            && manifest.privacy_key_epoch == route.privacy_key_epoch;
        let request_scope_matches_resolved_scope = base.scope.root.repository
            == scope.repository_id
            && base.scope.root.worktree.as_ref() == Some(&scope.worktree_id)
            && base.scope.root.reference == route.source_scope.reference
            && base.scope.privacy_domain == route.privacy_domain;
        if !source_scope_matches_generation || !request_scope_matches_resolved_scope {
            return semantic_abstention(
                mode,
                SemanticAbstentionV1::IndexIncompatible,
                Arc::clone(&authorized_query.fallback),
            );
        }
        if !semantic_cursor_matches_route(
            authorized_query.request_cursor.as_ref(),
            &authority.execution.profile().profile_id,
            &authority.profile_digest,
            &code_generation.manifest().generation_id,
            &pins.vector_generation_id,
            pins.projection.projection_key(),
            &pins.search_index_key,
            &pins.fusion_revision,
            &route,
        ) {
            return semantic_abstention(
                mode,
                SemanticAbstentionV1::Stale,
                Arc::clone(&authorized_query.fallback),
            );
        }
        let request = SemanticRetrievalRequestV1 {
            base: base.clone(),
            source_scope: route.source_scope.clone(),
            query_digest: authorized_query.query_digest.clone(),
            query_view,
            projection: &pins.projection,
            search_index_key: &pins.search_index_key,
            capability_manifest_digest: pins.calibration.capability_manifest_digest.clone(),
            vector_generation: pins.vector_generation_id.clone(),
            code_generation: code_generation.manifest().generation_id.clone(),
            budget: authority.execution.profile().retrieval_budget,
        };
        if let Err(refusal) = request.validate() {
            // Every predicate collapses into one public abstention, so the
            // named predicate and its non-secret privacy tuple are the only
            // way an operator can tell a budget bug from a privacy split.
            let manifest = code_generation.manifest();
            tracing::warn!(
                event = "semantic_query_request_refused",
                predicate = refusal.predicate.as_str(),
                error = %refusal.error,
                scope_privacy_domain = %request.base.scope.privacy_domain,
                serving_privacy_domain = %manifest.privacy_domain,
                serving_privacy_key_epoch = manifest.privacy_key_epoch,
                projection_privacy_domain = %pins.projection.privacy_domain(),
                projection_privacy_key_epoch = pins.projection.privacy_key_epoch(),
                query_digest_privacy_domain = %authorized_query.query_digest.privacy_domain,
                query_digest_key_epoch = authorized_query.query_digest.key_epoch,
                cursor_key_id = ?authorized_query
                    .request_cursor
                    .as_ref()
                    .map(|cursor| cursor.key_id.as_str()),
                cursor_key_epoch = ?authorized_query
                    .request_cursor
                    .as_ref()
                    .map(|cursor| cursor.key_epoch),
                code_generation = %manifest.generation_id,
                vector_generation = ?pins.vector_generation_id,
                "the semantic lane request failed its own contract, so the query abstained \
                 as generation-incompatible"
            );
            return semantic_abstention(
                mode,
                SemanticAbstentionV1::IndexIncompatible,
                Arc::clone(&authorized_query.fallback),
            );
        }
        let outcome = hotpath::future!(
            ProductionProjectSemanticSearchBridgeV1.execute(
                AuthorizedProjectSemanticSearchParametersV1 {
                    project_root,
                    code_generation,
                    request: &request,
                    calibration: Some(&pins.calibration),
                    control,
                    mode,
                    authorized_query,
                }
            ),
            label = "daemon.query.semantic.vector_and_lane"
        )
        .await?;
        let outcome = hotpath::measure_block!("daemon.query.semantic.compose", {
            authority.execution.execute(
                authorized_query,
                outcome,
                semantic_abstention_disposition(mode),
            )
        })?;
        match outcome {
            SemanticCompositionExecutionOutcomeV1::Fallback {
                abstention,
                fallback,
            } => Ok(SemanticAugmentationOutcomeV1::Fallback {
                abstention,
                fallback,
            }),
            SemanticCompositionExecutionOutcomeV1::Augmented(mut executed) => {
                if authorized_query
                    .request_cursor
                    .as_ref()
                    .and_then(|cursor| cursor.semantic.as_ref())
                    .is_none()
                {
                    executed.rerank = apply_configured_semantic_rerank(
                        &authority,
                        code_generation,
                        query_view,
                        base,
                        &mut executed.composition,
                        control,
                    );
                }
                let mut composition = executed.composition;
                let Some(query_authority) = hotpath::future!(
                    self.query_authority_for_scope(scope),
                    label = "daemon.query.semantic.pagination_authority"
                )
                .await
                else {
                    return Err(SemanticQueryServiceError::InvalidCursor);
                };
                let cursor = hotpath::measure_block!("daemon.query.semantic.paginate", {
                    paginate_semantic_composition_with_route(
                        query_authority.as_ref(),
                        base,
                        query_view,
                        authorized_query,
                        &route,
                        &authority.profile_digest,
                        &code_generation.manifest().generation_id,
                        &pins.vector_generation_id,
                        pins.projection.projection_key(),
                        &pins.search_index_key,
                        &pins.fusion_revision,
                        &authority.execution.profile().retrieval_budget,
                        &executed.rerank,
                        &mut composition,
                    )
                })?;
                Ok(SemanticAugmentationOutcomeV1::Augmented(Box::new(
                    SemanticAugmentedCompositionV1 {
                        composition,
                        cursor,
                        hydration_budget: authority.execution.profile().retrieval_budget,
                        fallback: executed.fallback,
                    },
                )))
            }
        }
    }
}

fn semantic_cursor_matches_activation(
    cursor: Option<&tracedecay_domain::RetrievalCursor>,
    profile_id: &tracedecay_domain::FusionProfileId,
    profile_digest: &tracedecay_domain::ManifestDigest,
    code_generation: &tracedecay_domain::CodeGenerationId,
    vector_generation: &tracedecay_domain::VectorGenerationIdV1,
    projection_key: &tracedecay_domain::ProjectionKeyV1,
    search_index_key: &tracedecay_domain::SemanticSearchIndexKeyV1,
    semantic_ranking_revision: &tracedecay_domain::ComponentRevision,
) -> bool {
    let Some(cursor) = cursor else {
        return true;
    };
    let Some(semantic) = cursor.semantic.as_ref() else {
        return cursor.next_ordinal == 0;
    };
    semantic.profile_id == *profile_id
        && semantic.profile_digest == *profile_digest
        && semantic.code_generation == *code_generation
        && semantic.vector_generation == *vector_generation
        && semantic.projection_key == *projection_key
        && semantic.search_index_key == *search_index_key
        && semantic.ranking_revision.as_str() == semantic_ranking_revision.as_str()
}

fn semantic_cursor_matches_route(
    cursor: Option<&tracedecay_domain::RetrievalCursor>,
    profile_id: &tracedecay_domain::FusionProfileId,
    profile_digest: &tracedecay_domain::ManifestDigest,
    code_generation: &tracedecay_domain::CodeGenerationId,
    vector_generation: &tracedecay_domain::VectorGenerationIdV1,
    projection_key: &tracedecay_domain::ProjectionKeyV1,
    search_index_key: &tracedecay_domain::SemanticSearchIndexKeyV1,
    semantic_ranking_revision: &tracedecay_domain::ComponentRevision,
    route: &SemanticRouteIdentityV1,
) -> bool {
    if !semantic_cursor_matches_activation(
        cursor,
        profile_id,
        profile_digest,
        code_generation,
        vector_generation,
        projection_key,
        search_index_key,
        semantic_ranking_revision,
    ) {
        return false;
    }
    let Some(cursor) = cursor else {
        return true;
    };
    let Some(semantic) = cursor.semantic.as_ref() else {
        return cursor.next_ordinal == 0;
    };
    semantic.model_artifact_digest == route.model_artifact_digest
        && semantic.execution_provider == route.execution_provider
        && semantic.capability_manifest_digest == route.capability_manifest_digest
        && semantic.privacy_domain == route.privacy_domain
        && semantic.privacy_key_epoch == route.privacy_key_epoch
        && semantic.source_scope == route.source_scope
}

struct SemanticRerankControlV1<'a, C: ?Sized>(&'a C);

impl<C> RetrievalExecutionControl for SemanticRerankControlV1<'_, C>
where
    C: RetrievalExecutionControl + ?Sized,
{
    fn elapsed_micros(&self) -> u64 {
        self.0.elapsed_micros()
    }

    fn is_cancelled(&self) -> bool {
        self.0.is_cancelled()
    }
}

fn mounted_compatible_rerank(
    configured: Option<&ConfiguredRerankAuthorityV1>,
) -> Option<&ProductionCodeRerankAuthorityV1> {
    configured.and_then(|configured| {
        configured
            .mounted
            .as_ref()
            .filter(|rerank| rerank.compatibility() == &configured.pins)
    })
}

fn apply_configured_semantic_rerank<C>(
    authority: &SemanticQueryAuthorityV1,
    code_generation: &CodeIndexPublishedGenerationV1,
    query_view: &EphemeralSanitizedQueryViewV1,
    request: &RetrievalRequest,
    composition: &mut CompositionOutputV1,
    control: &C,
) -> OptionalStagePublicStatus
where
    C: RetrievalExecutionControl + ?Sized,
{
    let Some(policy) = authority.execution.rerank_policy() else {
        return OptionalStagePublicStatus::NotRequested;
    };
    let Some(rerank) = mounted_compatible_rerank(authority.rerank.as_ref()) else {
        return OptionalStagePublicStatus::Unavailable(
            tracedecay_domain::SanitizedStageFailure::AuthorityUnavailable,
        );
    };
    let outcome = rerank.execute(
        code_generation,
        query_view,
        request,
        policy,
        &composition.ranked_candidates,
        &SemanticRerankControlV1(control),
    );
    apply_bounded_rerank_outcome(composition, outcome)
}

fn paginate_semantic_composition(
    query_authority: &QueryAuthorityV1,
    request: &RetrievalRequest,
    query_view: &EphemeralSanitizedQueryViewV1,
    authorized_query: &AuthorizedQueryFallbackV1,
    profile_digest: &tracedecay_domain::ManifestDigest,
    code_generation: &tracedecay_domain::CodeGenerationId,
    vector_generation: &tracedecay_domain::VectorGenerationIdV1,
    projection_key: &tracedecay_domain::ProjectionKeyV1,
    search_index_key: &tracedecay_domain::SemanticSearchIndexKeyV1,
    semantic_ranking_revision: &tracedecay_domain::ComponentRevision,
    semantic_budget: &tracedecay_domain::RetrievalBudget,
    rerank: &OptionalStagePublicStatus,
    composition: &mut CompositionOutputV1,
) -> Result<Option<tracedecay_domain::RetrievalCursor>, SemanticQueryServiceError> {
    let route =
        SemanticRouteIdentityV1::for_pagination_test(request, authorized_query, projection_key);
    paginate_semantic_composition_with_route(
        query_authority,
        request,
        query_view,
        authorized_query,
        &route,
        profile_digest,
        code_generation,
        vector_generation,
        projection_key,
        search_index_key,
        semantic_ranking_revision,
        semantic_budget,
        rerank,
        composition,
    )
}

fn paginate_semantic_composition_with_route(
    query_authority: &QueryAuthorityV1,
    request: &RetrievalRequest,
    query_view: &EphemeralSanitizedQueryViewV1,
    authorized_query: &AuthorizedQueryFallbackV1,
    route: &SemanticRouteIdentityV1,
    profile_digest: &tracedecay_domain::ManifestDigest,
    code_generation: &tracedecay_domain::CodeGenerationId,
    vector_generation: &tracedecay_domain::VectorGenerationIdV1,
    projection_key: &tracedecay_domain::ProjectionKeyV1,
    search_index_key: &tracedecay_domain::SemanticSearchIndexKeyV1,
    semantic_ranking_revision: &tracedecay_domain::ComponentRevision,
    semantic_budget: &tracedecay_domain::RetrievalBudget,
    rerank: &OptionalStagePublicStatus,
    composition: &mut CompositionOutputV1,
) -> Result<Option<tracedecay_domain::RetrievalCursor>, SemanticQueryServiceError> {
    let candidate_set_digest = digest_candidate_set(&composition.ranked_candidates)
        .map_err(|_| SemanticQueryServiceError::InvalidCursor)?;
    let ranking_revision =
        tracedecay_domain::RankingRevision::new(semantic_ranking_revision.as_str().to_owned())
            .map_err(|_| SemanticQueryServiceError::InvalidCursor)?;
    let supplied_semantic = authorized_query
        .request_cursor
        .as_ref()
        .and_then(|cursor| cursor.semantic.as_ref());
    if supplied_semantic.is_some_and(|semantic| {
        semantic.model_artifact_digest != route.model_artifact_digest
            || semantic.execution_provider != route.execution_provider
            || semantic.capability_manifest_digest != route.capability_manifest_digest
            || semantic.privacy_domain != route.privacy_domain
            || semantic.privacy_key_epoch != route.privacy_key_epoch
            || semantic.source_scope != route.source_scope
    }) {
        return Err(SemanticQueryServiceError::InvalidCursor);
    }
    if supplied_semantic.is_some_and(|cursor| {
        cursor.profile_id != composition.profile_id || cursor.code_generation != *code_generation
    }) {
        return Err(SemanticQueryServiceError::InvalidCursor);
    }
    if composition.ranked_candidates.is_empty() {
        return if authorized_query.request_cursor.is_none() {
            Ok(None)
        } else {
            Err(SemanticQueryServiceError::InvalidCursor)
        };
    }
    let semantic_start = match supplied_semantic {
        Some(cursor)
            if cursor.profile_id == composition.profile_id
                && cursor.profile_digest == *profile_digest
                && cursor.code_generation == *code_generation
                && cursor.vector_generation == *vector_generation
                && cursor.projection_key == *projection_key
                && cursor.search_index_key == *search_index_key
                && cursor.candidate_set_digest == candidate_set_digest
                && cursor.public_lane_statuses == composition.public_lane_statuses
                && cursor.lane_checkpoints == composition.lane_checkpoints
                && cursor.ranking_revision == ranking_revision =>
        {
            cursor.next_ordinal as usize
        }
        Some(_) => return Err(SemanticQueryServiceError::InvalidCursor),
        None => 0,
    };
    if semantic_start >= composition.ranked_candidates.len() {
        return Err(SemanticQueryServiceError::InvalidCursor);
    }
    let semantic_page_size = usize::try_from(semantic_budget.max_hydrated_results)
        .map_err(|_| SemanticQueryServiceError::InvalidCursor)?;
    let semantic_page_size = semantic_page_size.min(authorized_query.page_size);
    if semantic_page_size == 0 {
        return Err(SemanticQueryServiceError::InvalidCursor);
    }
    let semantic_end = semantic_start
        .saturating_add(semantic_page_size)
        .min(composition.ranked_candidates.len());
    let page_len = semantic_end.saturating_sub(semantic_start);

    let query_start = authorized_query
        .request_cursor
        .as_ref()
        .map_or(0, |cursor| cursor.next_ordinal as usize);
    if query_start > authorized_query.composition.ranked_candidates.len() {
        return Err(SemanticQueryServiceError::InvalidCursor);
    }
    let query_end = query_start
        .saturating_add(page_len)
        .min(authorized_query.composition.ranked_candidates.len());
    let has_more = semantic_end < composition.ranked_candidates.len();
    let cursor = if has_more {
        let mut cursor = query_authority
            .continuation_cursor_at(
                request,
                query_view,
                &authorized_query.composition,
                query_end,
            )
            .map_err(|_| SemanticQueryServiceError::InvalidCursor)?;
        query_authority
            .bind_semantic_continuation(
                &mut cursor,
                SemanticRetrievalContinuationV1 {
                    profile_id: composition.profile_id.clone(),
                    profile_digest: profile_digest.clone(),
                    code_generation: code_generation.clone(),
                    vector_generation: vector_generation.clone(),
                    model_artifact_digest: route.model_artifact_digest.clone(),
                    execution_provider: route.execution_provider,
                    projection_key: projection_key.clone(),
                    search_index_key: search_index_key.clone(),
                    capability_manifest_digest: route.capability_manifest_digest.clone(),
                    privacy_domain: route.privacy_domain.clone(),
                    privacy_key_epoch: route.privacy_key_epoch,
                    source_scope: route.source_scope.clone(),
                    candidate_set_digest,
                    public_lane_statuses: composition.public_lane_statuses.clone(),
                    lane_checkpoints: composition.lane_checkpoints.clone(),
                    ranking_revision,
                    rerank: rerank.clone(),
                    ordered_candidate_anchors: composition
                        .ranked_candidates
                        .iter()
                        .map(|candidate| candidate.candidate.anchor_id.clone())
                        .collect(),
                    next_ordinal: u32::try_from(semantic_end)
                        .map_err(|_| SemanticQueryServiceError::InvalidCursor)?,
                },
            )
            .map_err(|_| SemanticQueryServiceError::InvalidCursor)?;
        Some(cursor)
    } else {
        None
    };
    let mut ranked = std::mem::take(&mut composition.ranked_candidates);
    composition.ranked_candidates = if semantic_start == 0 {
        ranked.truncate(semantic_end);
        ranked
    } else {
        ranked.drain(semantic_start..semantic_end).collect()
    };
    Ok(cursor)
}

fn semantic_abstention(
    mode: SemanticQueryModeV1,
    abstention: SemanticAbstentionV1,
    fallback: Arc<tracedecay_domain::QueryFallbackSubpayload>,
) -> Result<SemanticAugmentationOutcomeV1, SemanticQueryServiceError> {
    match mode {
        SemanticQueryModeV1::FallbackAllowed => Ok(SemanticAugmentationOutcomeV1::Fallback {
            abstention,
            fallback,
        }),
        SemanticQueryModeV1::StrictSemantic => {
            Err(SemanticQueryServiceError::StrictUnavailable(abstention))
        }
    }
}

fn semantic_abstention_disposition(mode: SemanticQueryModeV1) -> SemanticAbstentionDispositionV1 {
    match mode {
        SemanticQueryModeV1::FallbackAllowed => SemanticAbstentionDispositionV1::UseFallback,
        SemanticQueryModeV1::StrictSemantic => SemanticAbstentionDispositionV1::RejectUnavailable,
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use tracedecay_domain::{
        AuthorizationRevision, CalibrationProfileId, CodeGenerationId, CompactCandidate,
        ComponentRevision, DiversityPolicy, EvidenceRole, ExactAdmissionProof,
        ExactAdmissionRuleRevision, ExactClass, ExactFieldV1, FixedPointScore,
        FreshnessCompatibilityV1, FreshnessVectorDigest, FusedCandidate, FusionProfile,
        LogicalEvidenceId, ManifestDigest, OccurrenceProvenance, PrincipalId, ProjectionKeyV1,
        ProjectionKindV1, PublicRetrieverStatus, QueryDigest, QueryFallbackSubpayload, QueryMac,
        QueryNormalizationRevision, RankedCandidate, RankingDecision, RankingDecisionKind,
        RetrievalAnchorId, RetrievalBudget, RetrievalCursorKeyId, RetrievalRequest, RetrievalScope,
        RetrievalSnapshot, RetrieverBatch, RetrieverCoverage, RetrieverOutcome, SanitizerRevision,
        ScoreDomainCalibrationV1, SemanticSearchIndexKeyV1, SemanticSearchIndexProfileV1,
        SingleRootScopeV1, SourceFreshness, TemporalModeV1, UtcMicros, VectorGenerationIdV1,
        VectorWatermark,
    };

    use super::*;
    use tracedecay_query::retrieval::fusion::{CompositionLaneInput, RetrievalCursorKeyringV1};
    use tracedecay_query::retrieval::rerank::{
        AdmittedNativeRerankExecutorV1, DeterministicLocalRerankExecutorV1, LocalRerankFailureV1,
        LocalRerankInputV1, LocalRerankPermitV1,
    };
    use tracedecay_query::retrieval::semantic::{
        SemanticCalibrationEvidenceV1, SemanticQueryServiceOutcomeV1,
    };
    use tracedecay_semantic_contracts::RerankCompatibilityPinsV1;

    fn id<T>(value: &str) -> T
    where
        T: TryFrom<String>,
        T::Error: std::fmt::Debug,
    {
        T::try_from(value.to_owned()).expect("fixture identity")
    }

    fn digest<T>(byte: char) -> T
    where
        T: TryFrom<String>,
        T::Error: std::fmt::Debug,
    {
        id(&format!("sha256:{}", byte.to_string().repeat(64)))
    }

    fn search_index_key() -> SemanticSearchIndexKeyV1 {
        SemanticSearchIndexProfileV1::exact_flat_v1()
            .and_then(|profile| profile.index_key())
            .expect("exact-flat search index key")
    }

    fn fallback() -> Arc<QueryFallbackSubpayload> {
        Arc::new(
            QueryFallbackSubpayload::new(
                "profile.query.semantic-bridge.v1"
                    .to_owned()
                    .try_into()
                    .expect("profile id"),
                Vec::new(),
                BTreeMap::from([
                    (RetrieverKind::ExactLiteral, PublicRetrieverStatus::Complete),
                    (RetrieverKind::Lexical, PublicRetrieverStatus::Complete),
                    (RetrieverKind::Graph, PublicRetrieverStatus::Complete),
                ]),
                Vec::new(),
                None,
            )
            .expect("canonical query fallback"),
        )
    }

    fn budget() -> RetrievalBudget {
        RetrievalBudget {
            max_candidates_per_lane: 16,
            max_fused_candidates: 16,
            max_hydrated_results: 8,
            max_hydration_bytes: 65_536,
            deadline_micros: None,
        }
    }

    fn semantic_budget(page_size: u32) -> RetrievalBudget {
        RetrievalBudget {
            max_hydrated_results: page_size,
            ..budget()
        }
    }

    fn query_profile() -> FusionProfile {
        let lanes = RetrieverKind::QUERY_FALLBACK_LANES;
        FusionProfile {
            profile_id: id("profile.query.pagination.v1"),
            evaluation_result_anchor: id("evaluation.query.pagination.v1"),
            calibrations: lanes
                .into_iter()
                .map(|lane| {
                    (
                        lane,
                        id::<CalibrationProfileId>(&format!(
                            "calibration.{}.pagination.v1",
                            lane.as_str()
                        )),
                    )
                })
                .collect(),
            score_domain_calibrations: BTreeMap::new(),
            minimum_calibrated_feature_micros: BTreeMap::new(),
            weights_micros: [
                (RetrieverKind::ExactLiteral, 1_000_000),
                (RetrieverKind::Lexical, 500_000),
                (RetrieverKind::Graph, 250_000),
            ]
            .into_iter()
            .collect(),
            diversity_policy_id: id("diversity.query.pagination.v1"),
            rerank_policy_id: None,
            retrieval_budget: budget(),
        }
    }

    fn query_authority(request: &RetrievalRequest) -> QueryAuthorityV1 {
        let profile = query_profile();
        QueryAuthorityV1::new(
            profile.clone(),
            DiversityPolicy {
                policy_id: profile.diversity_policy_id,
                evaluation_result_anchor: Some(profile.evaluation_result_anchor),
                per_source_namespace: None,
                per_source_instance: None,
                per_repository: None,
                per_file: None,
                per_session_or_thread: None,
                per_copy_cluster: None,
                per_evidence_role: None,
            },
            id("ranking.query.pagination.v1"),
            RetrievalCursorKeyringV1::new(
                request.scope.privacy_domain.clone(),
                id::<RetrievalCursorKeyId>("cursor-key.query.pagination.v1"),
                7,
                vec![7_u8; 32],
                1_000_000,
            )
            .expect("cursor keyring"),
        )
        .expect("query authority")
    }

    fn request() -> RetrievalRequest {
        RetrievalRequest {
            principal: id::<PrincipalId>("principal.pagination"),
            scope: RetrievalScope {
                privacy_domain: id("privacy.pagination"),
                root: SingleRootScopeV1 {
                    repository: id("repository.pagination"),
                    worktree: None,
                    reference: None,
                },
            },
            temporal_mode: TemporalModeV1::Current,
            snapshot: RetrievalSnapshot {
                watermarks: VectorWatermark::default(),
                freshness_digest: digest::<FreshnessVectorDigest>('f'),
                authorization_revision: id::<AuthorizationRevision>("authorization.pagination.v1"),
                captured_at: UtcMicros(7),
            },
            profile_id: query_profile().profile_id,
            budget: budget(),
        }
    }

    fn ranked(ordinal: u32) -> RankedCandidate {
        RankedCandidate {
            candidate: FusedCandidate {
                anchor_id: id::<RetrievalAnchorId>(&format!("anchor.pagination.{ordinal}")),
                logical_evidence_id: id::<LogicalEvidenceId>(&format!(
                    "logical.pagination.{ordinal}"
                )),
                occurrences: Vec::new(),
                exact_class: ExactClass::Approximate,
                utility_micros: u64::from(100 - ordinal),
                contributions: Vec::new(),
                freshness: Vec::new(),
                decisions: Vec::new(),
            },
            final_ordinal: ordinal,
        }
    }

    fn composition(
        profile_id: tracedecay_domain::FusionProfileId,
        lanes: &[RetrieverKind],
    ) -> CompositionOutputV1 {
        CompositionOutputV1 {
            profile_id,
            ranked_candidates: (0..6).map(ranked).collect(),
            comparator_records: Vec::new(),
            internal_lane_outcomes: BTreeMap::new(),
            public_lane_statuses: lanes
                .iter()
                .copied()
                .map(|lane| (lane, PublicRetrieverStatus::Complete))
                .collect(),
            freshness: Vec::new(),
            lane_checkpoints: Vec::new(),
            dedupe_decisions: Vec::new(),
            diversity_decisions: Vec::new(),
        }
    }

    fn fallback_page(ordinals: &[u32]) -> Arc<QueryFallbackSubpayload> {
        let candidates = ordinals
            .iter()
            .copied()
            .enumerate()
            .map(|(page_ordinal, source_ordinal)| {
                let mut candidate = ranked(source_ordinal);
                candidate.final_ordinal = page_ordinal as u32;
                candidate
            })
            .collect();
        Arc::new(
            QueryFallbackSubpayload::new(
                query_profile().profile_id,
                candidates,
                BTreeMap::from([
                    (RetrieverKind::ExactLiteral, PublicRetrieverStatus::Complete),
                    (RetrieverKind::Lexical, PublicRetrieverStatus::Complete),
                    (RetrieverKind::Graph, PublicRetrieverStatus::Complete),
                ]),
                Vec::new(),
                None,
            )
            .expect("canonical query fallback page"),
        )
    }

    /// Test-only semantic port used to drive the serving decision without a
    /// model, vector store, or projection worker. The production bridge has
    /// the same contract: it receives the authenticated fallback and may
    /// either return semantic influence or a typed abstention.
    struct MockSemanticPortV1 {
        abstention: SemanticAbstentionV1,
    }

    impl MockSemanticPortV1 {
        fn execute(
            &self,
            mode: SemanticQueryModeV1,
            fallback: Arc<QueryFallbackSubpayload>,
        ) -> Result<SemanticAugmentationOutcomeV1, SemanticQueryServiceError> {
            semantic_abstention(mode, self.abstention.clone(), fallback)
        }
    }

    fn semantic_port_fallback_test(
        mode: SemanticQueryModeV1,
        abstention: SemanticAbstentionV1,
    ) -> Result<SemanticAugmentationOutcomeV1, SemanticQueryServiceError> {
        let fallback = fallback_page(&[0, 1]);
        let identity = Arc::as_ptr(&fallback);
        let outcome = MockSemanticPortV1 { abstention }.execute(mode, fallback)?;
        assert_eq!(Arc::as_ptr(outcome.fallback()), identity);
        Ok(outcome)
    }

    fn mock_freshness(file: &str) -> SourceFreshness {
        SourceFreshness {
            source_namespace: id("namespace.mock-semantic"),
            source_instance: id(&format!("instance.{file}")),
            source_watermark: Some(1),
            projection_watermark: Some(1),
            observed_at: UtcMicros(1),
            source_generation: Some(1),
            generation_lag: Some(0),
            compatibility: FreshnessCompatibilityV1::Current,
            policy_revision: id("policy.mock-semantic.v1"),
        }
    }

    fn mock_compact_candidate(
        lane: RetrieverKind,
        name: &str,
        raw_score_micros: u64,
        ordinal_rank: u32,
        file: &str,
    ) -> CompactCandidate {
        CompactCandidate {
            anchor_id: id(&format!("anchor.mock-semantic.{name}")),
            logical_evidence_id: id(&format!("logical.mock-semantic.{name}")),
            source_occurrence_id: id(&format!("occurrence.mock-semantic.{name}")),
            file_occurrence_id: Some(id(&format!("file.mock-semantic.{file}"))),
            source_namespace: id("namespace.mock-semantic"),
            repository_id: Some(id("repository.mock-semantic")),
            session_or_thread_id: None,
            logical_copy_cluster_id: None,
            logical_copy_evidence_anchor: None,
            evidence_role: EvidenceRole::Primary,
            retriever: lane,
            retriever_revision: id(&format!("retriever.mock-semantic.{}.v1", lane.as_str())),
            score_domain: id(&format!("score.mock-semantic.{}.v1", lane.as_str())),
            raw_score: FixedPointScore(raw_score_micros),
            ordinal_rank,
            exact_admission_proof: None,
            retriever_evidence_anchor: id(&format!("evidence.mock-semantic.{name}")),
            freshness: mock_freshness(file),
        }
    }

    fn mock_exact_candidate(name: &str, file: &str) -> CompactCandidate {
        let mut candidate =
            mock_compact_candidate(RetrieverKind::ExactLiteral, name, 1_000_000, 0, file);
        candidate.exact_admission_proof = Some(ExactAdmissionProof {
            rule_revision: id::<ExactAdmissionRuleRevision>("exact-rules.mock-semantic.v1"),
            field: ExactFieldV1::Identifier,
            original_bytes: name.as_bytes().to_vec(),
            canonical_bytes: name.as_bytes().to_vec(),
            normalization_steps: Vec::new(),
            scope_digest: digest('1'),
            authorization_revision: id("authorization.mock-semantic.v1"),
            snapshot_digest: digest('2'),
        });
        candidate
    }

    fn mock_lane(lane: RetrieverKind, candidates: Vec<CompactCandidate>) -> CompositionLaneInput {
        let evidence_by_occurrence = candidates
            .iter()
            .map(|candidate| (candidate.source_occurrence_id.clone(), ()))
            .collect();
        CompositionLaneInput::new(
            lane,
            RetrieverOutcome::Complete(RetrieverBatch {
                candidates,
                evidence_by_occurrence,
                coverage: RetrieverCoverage::default(),
                continuation: None,
            }),
        )
        .expect("mock semantic lane is valid")
    }

    fn mock_protected_exact_ranked(candidate: &CompactCandidate) -> RankedCandidate {
        let freshness = candidate.freshness.clone();
        let occurrence = OccurrenceProvenance {
            source_occurrence_id: candidate.source_occurrence_id.clone(),
            file_occurrence_id: candidate.file_occurrence_id.clone(),
            retriever_evidence_anchor: candidate.retriever_evidence_anchor.clone(),
            source_namespace: candidate.source_namespace.clone(),
            repository_id: candidate.repository_id.clone(),
            session_or_thread_id: candidate.session_or_thread_id.clone(),
            logical_copy_cluster_id: candidate.logical_copy_cluster_id.clone(),
            logical_copy_evidence_anchor: candidate.logical_copy_evidence_anchor.clone(),
            evidence_role: candidate.evidence_role,
            freshness: freshness.clone(),
        };
        RankedCandidate {
            candidate: FusedCandidate {
                anchor_id: candidate.anchor_id.clone(),
                logical_evidence_id: candidate.logical_evidence_id.clone(),
                occurrences: vec![occurrence.clone()],
                exact_class: ExactClass::ExactMessage,
                utility_micros: 1_000_000,
                contributions: Vec::new(),
                freshness: vec![freshness],
                decisions: vec![RankingDecision {
                    kind: RankingDecisionKind::ExactTierAdmission,
                    retriever: Some(RetrieverKind::ExactLiteral),
                    policy_anchor: Some(id("policy-anchor.mock-semantic.v1")),
                    evidence_anchor: Some(occurrence.retriever_evidence_anchor),
                    detail: "mock protected exact admission".to_owned(),
                }],
            },
            final_ordinal: 0,
        }
    }

    fn mock_semantic_profile() -> FusionProfile {
        let lanes = [
            RetrieverKind::ExactLiteral,
            RetrieverKind::Lexical,
            RetrieverKind::Graph,
            RetrieverKind::Semantic,
        ];
        let score_domain_calibrations = lanes
            .into_iter()
            .map(|lane| {
                let score_domain = id(&format!("score.mock-semantic.{}.v1", lane.as_str()));
                (
                    score_domain.clone(),
                    ScoreDomainCalibrationV1 {
                        calibration_profile_id: id(&format!(
                            "calibration.mock-semantic.{}.v1",
                            lane.as_str()
                        )),
                        score_domain,
                        raw_min_micros: 0,
                        raw_max_micros: 1_000_000,
                    },
                )
            })
            .collect();
        FusionProfile {
            profile_id: id("profile.semantic.mock-semantic.v1"),
            evaluation_result_anchor: id("evaluation.mock-semantic.v1"),
            calibrations: lanes
                .into_iter()
                .map(|lane| {
                    (
                        lane,
                        id(&format!("calibration.mock-semantic.{}.v1", lane.as_str())),
                    )
                })
                .collect(),
            score_domain_calibrations,
            minimum_calibrated_feature_micros: BTreeMap::new(),
            weights_micros: lanes.into_iter().map(|lane| (lane, 1_000_000)).collect(),
            diversity_policy_id: id("diversity.mock-semantic.v1"),
            rerank_policy_id: None,
            retrieval_budget: budget(),
        }
    }

    #[test]
    fn semantic_composition_resumes_across_three_authenticated_pages() {
        let request = request();
        let query_view = EphemeralSanitizedQueryViewV1::sanitize(
            "pagination",
            id::<SanitizerRevision>("sanitizer.pagination.v1"),
            id::<QueryNormalizationRevision>("normalization.pagination.v1"),
        )
        .expect("query view");
        let authority = query_authority(&request);
        let query_composition = composition(
            query_profile().profile_id,
            &RetrieverKind::QUERY_FALLBACK_LANES,
        );
        let semantic_profile =
            id::<tracedecay_domain::FusionProfileId>("profile.semantic.pagination.v1");
        let code_generation = id::<CodeGenerationId>("code-generation.pagination.v1");
        let vector_generation = VectorGenerationIdV1::new(digest::<ManifestDigest>('a'));
        let projection = ProjectionKeyV1 {
            kind: ProjectionKindV1::Embedding,
            schema_revision: "projection.pagination.v1".to_owned(),
            profile_digest: digest('b'),
        };
        let ranking_revision = id::<ComponentRevision>("ranking.semantic.pagination.v1");
        let fallback = fallback();
        let fallback_identity = Arc::as_ptr(&fallback);
        let mut request_cursor = None;
        let mut seen = Vec::new();

        for page_number in 0..3 {
            let mut semantic_composition = composition(
                semantic_profile.clone(),
                &[
                    RetrieverKind::ExactLiteral,
                    RetrieverKind::Lexical,
                    RetrieverKind::Graph,
                    RetrieverKind::Semantic,
                ],
            );
            let authorized = AuthorizedQueryFallbackV1 {
                query_digest: authority
                    .authenticate_query(&request, &query_view)
                    .expect("query digest"),
                fallback: Arc::clone(&fallback),
                composition: query_composition.clone(),
                fallback_lanes: Vec::new(),
                page_size: 5,
                request_cursor,
            };
            request_cursor = paginate_semantic_composition(
                &authority,
                &request,
                &query_view,
                &authorized,
                &digest::<ManifestDigest>('c'),
                &code_generation,
                &vector_generation,
                &projection,
                &search_index_key(),
                &ranking_revision,
                &semantic_budget(2),
                &OptionalStagePublicStatus::NotRequested,
                &mut semantic_composition,
            )
            .expect("authenticated semantic page");
            seen.extend(
                semantic_composition
                    .ranked_candidates
                    .iter()
                    .map(|candidate| candidate.final_ordinal),
            );
            assert_eq!(
                request_cursor.is_some(),
                page_number < 2,
                "only nonterminal pages continue"
            );
            if let Some(cursor) = request_cursor.as_ref() {
                assert_eq!(cursor.next_ordinal, (page_number + 1) * 2);
            }
            assert_eq!(Arc::as_ptr(&authorized.fallback), fallback_identity);
        }

        assert_eq!(seen, vec![0, 1, 2, 3, 4, 5]);
    }

    #[test]
    fn empty_first_semantic_page_completes_without_a_cursor() {
        let request = request();
        let query_view = EphemeralSanitizedQueryViewV1::sanitize(
            "empty semantic page",
            id::<SanitizerRevision>("sanitizer.pagination.v1"),
            id::<QueryNormalizationRevision>("normalization.pagination.v1"),
        )
        .expect("query view");
        let authority = query_authority(&request);
        let mut semantic_composition = composition(
            id("profile.semantic.pagination.v1"),
            &[
                RetrieverKind::ExactLiteral,
                RetrieverKind::Lexical,
                RetrieverKind::Graph,
                RetrieverKind::Semantic,
            ],
        );
        semantic_composition.ranked_candidates.clear();
        let authorized = AuthorizedQueryFallbackV1 {
            query_digest: authority
                .authenticate_query(&request, &query_view)
                .expect("query digest"),
            fallback: fallback(),
            composition: composition(
                query_profile().profile_id,
                &RetrieverKind::QUERY_FALLBACK_LANES,
            ),
            fallback_lanes: Vec::new(),
            page_size: 10,
            request_cursor: None,
        };

        let cursor = paginate_semantic_composition(
            &authority,
            &request,
            &query_view,
            &authorized,
            &digest::<ManifestDigest>('c'),
            &id::<CodeGenerationId>("code-generation.pagination.v1"),
            &VectorGenerationIdV1::new(digest::<ManifestDigest>('a')),
            &ProjectionKeyV1 {
                kind: ProjectionKindV1::Embedding,
                schema_revision: "projection.pagination.v1".to_owned(),
                profile_digest: digest('b'),
            },
            &search_index_key(),
            &id::<ComponentRevision>("ranking.semantic.pagination.v1"),
            &semantic_budget(2),
            &OptionalStagePublicStatus::NotRequested,
            &mut semantic_composition,
        )
        .expect("an empty first page is a completed result");

        assert!(cursor.is_none());
        assert!(semantic_composition.ranked_candidates.is_empty());
    }

    #[test]
    fn requested_limit_caps_the_active_profile_hydration_page() {
        let request = request();
        let query_view = EphemeralSanitizedQueryViewV1::sanitize(
            "pagination",
            id::<SanitizerRevision>("sanitizer.pagination.v1"),
            id::<QueryNormalizationRevision>("normalization.pagination.v1"),
        )
        .expect("query view");
        let authority = query_authority(&request);
        let mut semantic_composition = composition(
            id("profile.semantic.pagination.v1"),
            &[
                RetrieverKind::ExactLiteral,
                RetrieverKind::Lexical,
                RetrieverKind::Graph,
                RetrieverKind::Semantic,
            ],
        );
        let authorized = AuthorizedQueryFallbackV1 {
            query_digest: authority
                .authenticate_query(&request, &query_view)
                .expect("query digest"),
            fallback: fallback(),
            composition: composition(
                query_profile().profile_id,
                &RetrieverKind::QUERY_FALLBACK_LANES,
            ),
            fallback_lanes: Vec::new(),
            page_size: 1,
            request_cursor: None,
        };

        let cursor = paginate_semantic_composition(
            &authority,
            &request,
            &query_view,
            &authorized,
            &digest::<ManifestDigest>('c'),
            &id::<CodeGenerationId>("code-generation.pagination.v1"),
            &VectorGenerationIdV1::new(digest::<ManifestDigest>('a')),
            &ProjectionKeyV1 {
                kind: ProjectionKindV1::Embedding,
                schema_revision: "projection.pagination.v1".to_owned(),
                profile_digest: digest('b'),
            },
            &search_index_key(),
            &id::<ComponentRevision>("ranking.semantic.pagination.v1"),
            &semantic_budget(2),
            &OptionalStagePublicStatus::NotRequested,
            &mut semantic_composition,
        )
        .expect("bounded semantic page")
        .expect("continuation");

        assert_eq!(semantic_composition.ranked_candidates.len(), 1);
        assert_eq!(cursor.next_ordinal, 1);
        assert_eq!(cursor.semantic.expect("semantic cursor").next_ordinal, 1);
    }

    #[test]
    fn frozen_rerank_order_is_restored_without_reexecuting_the_optional_stage() {
        let request = request();
        let query_view = EphemeralSanitizedQueryViewV1::sanitize(
            "pagination",
            id::<SanitizerRevision>("sanitizer.pagination.v1"),
            id::<QueryNormalizationRevision>("normalization.pagination.v1"),
        )
        .expect("query view");
        let authority = query_authority(&request);
        let query_composition = composition(
            query_profile().profile_id,
            &RetrieverKind::QUERY_FALLBACK_LANES,
        );
        let profile_digest = digest::<ManifestDigest>('c');
        let code_generation = id::<CodeGenerationId>("code-generation.pagination.v1");
        let vector_generation = VectorGenerationIdV1::new(digest::<ManifestDigest>('a'));
        let projection = ProjectionKeyV1 {
            kind: ProjectionKindV1::Embedding,
            schema_revision: "projection.pagination.v1".to_owned(),
            profile_digest: digest('b'),
        };
        let ranking_revision = id::<ComponentRevision>("ranking.semantic.pagination.v1");
        let mut first = composition(
            id("profile.semantic.pagination.v1"),
            &[
                RetrieverKind::ExactLiteral,
                RetrieverKind::Lexical,
                RetrieverKind::Graph,
                RetrieverKind::Semantic,
            ],
        );
        first.ranked_candidates.reverse();
        for (ordinal, candidate) in first.ranked_candidates.iter_mut().enumerate() {
            candidate.final_ordinal = ordinal as u32;
        }
        let authorized = AuthorizedQueryFallbackV1 {
            query_digest: authority
                .authenticate_query(&request, &query_view)
                .expect("query digest"),
            fallback: fallback(),
            composition: query_composition.clone(),
            fallback_lanes: Vec::new(),
            page_size: 2,
            request_cursor: None,
        };
        let cursor = paginate_semantic_composition(
            &authority,
            &request,
            &query_view,
            &authorized,
            &profile_digest,
            &code_generation,
            &vector_generation,
            &projection,
            &search_index_key(),
            &ranking_revision,
            &semantic_budget(2),
            &OptionalStagePublicStatus::Complete,
            &mut first,
        )
        .expect("first reranked page")
        .expect("continuation");
        let continuation = cursor.semantic.as_ref().expect("semantic continuation");
        let continuation_rerank = continuation.rerank.clone();

        let mut resumed = composition(
            id("profile.semantic.pagination.v1"),
            &[
                RetrieverKind::ExactLiteral,
                RetrieverKind::Lexical,
                RetrieverKind::Graph,
                RetrieverKind::Semantic,
            ],
        );
        tracedecay_query::retrieval::semantic::restore_frozen_semantic_order(
            continuation,
            &mut resumed,
        )
        .expect("authenticated order restores");
        assert_eq!(
            resumed
                .ranked_candidates
                .iter()
                .map(|candidate| candidate.candidate.anchor_id.as_str())
                .collect::<Vec<_>>(),
            [
                "anchor.pagination.5",
                "anchor.pagination.4",
                "anchor.pagination.3",
                "anchor.pagination.2",
                "anchor.pagination.1",
                "anchor.pagination.0",
            ]
        );
        let authorized = AuthorizedQueryFallbackV1 {
            query_digest: authority
                .authenticate_query(&request, &query_view)
                .expect("query digest"),
            fallback: fallback(),
            composition: query_composition,
            fallback_lanes: Vec::new(),
            page_size: 2,
            request_cursor: Some(cursor),
        };
        let next = paginate_semantic_composition(
            &authority,
            &request,
            &query_view,
            &authorized,
            &profile_digest,
            &code_generation,
            &vector_generation,
            &projection,
            &search_index_key(),
            &ranking_revision,
            &semantic_budget(2),
            &continuation_rerank,
            &mut resumed,
        )
        .expect("second frozen page");

        assert!(next.is_some());
        assert_eq!(
            resumed
                .ranked_candidates
                .iter()
                .map(|candidate| candidate.candidate.anchor_id.as_str())
                .collect::<Vec<_>>(),
            ["anchor.pagination.3", "anchor.pagination.2"]
        );
    }

    #[test]
    fn augmented_pagination_stops_when_semantic_candidates_end_before_query() {
        let request = request();
        let query_view = EphemeralSanitizedQueryViewV1::sanitize(
            "pagination",
            id::<SanitizerRevision>("sanitizer.pagination.v1"),
            id::<QueryNormalizationRevision>("normalization.pagination.v1"),
        )
        .expect("query view");
        let authority = query_authority(&request);
        let query_composition = composition(
            query_profile().profile_id,
            &RetrieverKind::QUERY_FALLBACK_LANES,
        );
        let mut semantic_composition = composition(
            id("profile.semantic.pagination.v1"),
            &[
                RetrieverKind::ExactLiteral,
                RetrieverKind::Lexical,
                RetrieverKind::Graph,
                RetrieverKind::Semantic,
            ],
        );
        semantic_composition.ranked_candidates.truncate(2);
        let authorized = AuthorizedQueryFallbackV1 {
            query_digest: authority
                .authenticate_query(&request, &query_view)
                .expect("query digest"),
            fallback: fallback(),
            composition: query_composition,
            fallback_lanes: Vec::new(),
            page_size: 5,
            request_cursor: None,
        };

        let cursor = paginate_semantic_composition(
            &authority,
            &request,
            &query_view,
            &authorized,
            &digest::<ManifestDigest>('c'),
            &id::<CodeGenerationId>("code-generation.pagination.v1"),
            &VectorGenerationIdV1::new(digest::<ManifestDigest>('a')),
            &ProjectionKeyV1 {
                kind: ProjectionKindV1::Embedding,
                schema_revision: "projection.pagination.v1".to_owned(),
                profile_digest: digest('b'),
            },
            &search_index_key(),
            &id::<ComponentRevision>("ranking.semantic.pagination.v1"),
            &semantic_budget(2),
            &OptionalStagePublicStatus::NotRequested,
            &mut semantic_composition,
        )
        .expect("terminal semantic page");

        assert!(
            cursor.is_none(),
            "query remainder cannot extend semantic paging"
        );
        assert_eq!(
            semantic_composition
                .ranked_candidates
                .iter()
                .map(|candidate| candidate.final_ordinal)
                .collect::<Vec<_>>(),
            vec![0, 1]
        );
    }

    #[test]
    fn semantic_activation_mid_query_pagination_preserves_the_existing_page() {
        let request = request();
        let query_view = EphemeralSanitizedQueryViewV1::sanitize(
            "pagination",
            id::<SanitizerRevision>("sanitizer.pagination.v1"),
            id::<QueryNormalizationRevision>("normalization.pagination.v1"),
        )
        .expect("query view");
        let authority = query_authority(&request);
        let query_composition = composition(
            query_profile().profile_id,
            &RetrieverKind::QUERY_FALLBACK_LANES,
        );
        let legacy_cursor = authority
            .continuation_cursor_at(&request, &query_view, &query_composition, 2)
            .expect("authenticated query continuation");
        let semantic_profile =
            id::<tracedecay_domain::FusionProfileId>("profile.semantic.pagination.v1");
        let code_generation = id::<CodeGenerationId>("code-generation.pagination.v1");
        let vector_generation = VectorGenerationIdV1::new(digest::<ManifestDigest>('a'));
        let projection = ProjectionKeyV1 {
            kind: ProjectionKindV1::Embedding,
            schema_revision: "projection.pagination.v1".to_owned(),
            profile_digest: digest('b'),
        };
        let ranking_revision = id::<ComponentRevision>("ranking.semantic.pagination.v1");

        assert!(!semantic_cursor_matches_activation(
            Some(&legacy_cursor),
            &semantic_profile,
            &digest::<ManifestDigest>('c'),
            &code_generation,
            &vector_generation,
            &projection,
            &search_index_key(),
            &ranking_revision,
        ));
        let fallback = fallback_page(&[2, 3]);
        let identity = Arc::as_ptr(&fallback);
        let outcome = semantic_abstention(
            SemanticQueryModeV1::FallbackAllowed,
            SemanticAbstentionV1::Stale,
            fallback,
        )
        .expect("legacy continuation falls back");

        assert_eq!(Arc::as_ptr(outcome.fallback()), identity);
        let mut seen = vec![
            "anchor.pagination.0".to_owned(),
            "anchor.pagination.1".to_owned(),
        ];
        seen.extend(
            outcome
                .fallback()
                .ordered_candidates
                .iter()
                .map(|candidate| candidate.candidate.anchor_id.as_str().to_owned()),
        );
        assert_eq!(
            seen,
            [
                "anchor.pagination.0",
                "anchor.pagination.1",
                "anchor.pagination.2",
                "anchor.pagination.3",
            ]
        );
    }

    #[test]
    fn semantic_profile_change_mid_pagination_preserves_the_query_page() {
        let request = request();
        let query_view = EphemeralSanitizedQueryViewV1::sanitize(
            "pagination",
            id::<SanitizerRevision>("sanitizer.pagination.v1"),
            id::<QueryNormalizationRevision>("normalization.pagination.v1"),
        )
        .expect("query view");
        let authority = query_authority(&request);
        let query_composition = composition(
            query_profile().profile_id,
            &RetrieverKind::QUERY_FALLBACK_LANES,
        );
        let original_profile =
            id::<tracedecay_domain::FusionProfileId>("profile.semantic.pagination.v1");
        let changed_profile =
            id::<tracedecay_domain::FusionProfileId>("profile.semantic.pagination.v2");
        let code_generation = id::<CodeGenerationId>("code-generation.pagination.v1");
        let vector_generation = VectorGenerationIdV1::new(digest::<ManifestDigest>('a'));
        let projection = ProjectionKeyV1 {
            kind: ProjectionKindV1::Embedding,
            schema_revision: "projection.pagination.v1".to_owned(),
            profile_digest: digest('b'),
        };
        let ranking_revision = id::<ComponentRevision>("ranking.semantic.pagination.v1");
        let mut semantic_composition = composition(
            original_profile.clone(),
            &[
                RetrieverKind::ExactLiteral,
                RetrieverKind::Lexical,
                RetrieverKind::Graph,
                RetrieverKind::Semantic,
            ],
        );
        let authorized = AuthorizedQueryFallbackV1 {
            query_digest: authority
                .authenticate_query(&request, &query_view)
                .expect("query digest"),
            fallback: fallback_page(&[0, 1]),
            composition: query_composition,
            fallback_lanes: Vec::new(),
            page_size: 2,
            request_cursor: None,
        };
        let cursor = paginate_semantic_composition(
            &authority,
            &request,
            &query_view,
            &authorized,
            &digest::<ManifestDigest>('c'),
            &code_generation,
            &vector_generation,
            &projection,
            &search_index_key(),
            &ranking_revision,
            &semantic_budget(2),
            &OptionalStagePublicStatus::NotRequested,
            &mut semantic_composition,
        )
        .expect("first semantic page")
        .expect("semantic continuation");

        assert!(semantic_cursor_matches_activation(
            Some(&cursor),
            &original_profile,
            &digest::<ManifestDigest>('c'),
            &code_generation,
            &vector_generation,
            &projection,
            &search_index_key(),
            &ranking_revision,
        ));
        assert!(!semantic_cursor_matches_activation(
            Some(&cursor),
            &original_profile,
            &digest::<ManifestDigest>('d'),
            &code_generation,
            &vector_generation,
            &projection,
            &search_index_key(),
            &ranking_revision,
        ));
        assert!(!semantic_cursor_matches_activation(
            Some(&cursor),
            &changed_profile,
            &digest::<ManifestDigest>('c'),
            &code_generation,
            &vector_generation,
            &projection,
            &search_index_key(),
            &ranking_revision,
        ));
        let mut changed_search_index = search_index_key();
        changed_search_index.profile_digest = digest('e');
        assert!(!semantic_cursor_matches_activation(
            Some(&cursor),
            &original_profile,
            &digest::<ManifestDigest>('c'),
            &code_generation,
            &vector_generation,
            &projection,
            &changed_search_index,
            &ranking_revision,
        ));
        let fallback = fallback_page(&[2, 3]);
        let identity = Arc::as_ptr(&fallback);
        let outcome = semantic_abstention(
            SemanticQueryModeV1::FallbackAllowed,
            SemanticAbstentionV1::Stale,
            fallback,
        )
        .expect("profile drift falls back");
        assert_eq!(Arc::as_ptr(outcome.fallback()), identity);
        let mut seen = vec![
            "anchor.pagination.0".to_owned(),
            "anchor.pagination.1".to_owned(),
        ];
        seen.extend(
            outcome
                .fallback()
                .ordered_candidates
                .iter()
                .map(|candidate| candidate.candidate.anchor_id.as_str().to_owned()),
        );
        assert_eq!(
            seen,
            [
                "anchor.pagination.0",
                "anchor.pagination.1",
                "anchor.pagination.2",
                "anchor.pagination.3",
            ]
        );
    }

    #[test]
    fn strict_semantic_reports_typed_unavailable_without_a_fallback_result() {
        assert!(matches!(
            semantic_abstention(
                SemanticQueryModeV1::StrictSemantic,
                SemanticAbstentionV1::CalibrationUnavailable,
                fallback(),
            ),
            Err(SemanticQueryServiceError::StrictUnavailable(
                SemanticAbstentionV1::CalibrationUnavailable
            ))
        ));
    }

    #[test]
    fn strict_semantic_execution_error_preserves_the_query_generation() {
        let generation = id::<CodeGenerationId>("code-generation.strict-semantic-selected.v1");
        let error = bind_semantic_execution_error(
            &generation,
            SemanticQueryServiceError::StrictUnavailable(
                SemanticAbstentionV1::CalibrationUnavailable,
            ),
        );

        assert!(matches!(
            error,
            QuerySemanticSearchExecutionErrorV1::StrictSemanticUnavailable {
                generation: selected,
                abstention: SemanticAbstentionV1::CalibrationUnavailable,
            } if selected == generation
        ));
    }

    #[test]
    fn mock_semantic_port_preserves_fallback_bytes_for_every_abstention() {
        for abstention in [
            SemanticAbstentionV1::IndexStale,
            SemanticAbstentionV1::IndexIncompatible,
            SemanticAbstentionV1::CalibrationShifted,
            SemanticAbstentionV1::SemanticUnavailable,
        ] {
            let outcome = semantic_port_fallback_test(
                SemanticQueryModeV1::FallbackAllowed,
                abstention.clone(),
            )
            .expect("fallback mode accepts the typed semantic abstention");
            assert!(matches!(
                outcome,
                SemanticAugmentationOutcomeV1::Fallback {
                    abstention: actual,
                    ..
                } if actual == abstention
            ));
        }
    }

    #[test]
    fn mock_semantic_port_strict_mode_returns_typed_unavailable() {
        let result = MockSemanticPortV1 {
            abstention: SemanticAbstentionV1::IndexStale,
        }
        .execute(SemanticQueryModeV1::StrictSemantic, fallback_page(&[0]));
        let Err(error) = result else {
            panic!("strict mode must not manufacture semantic candidates");
        };

        assert!(matches!(
            error,
            SemanticQueryServiceError::StrictUnavailable(SemanticAbstentionV1::IndexStale)
        ));
    }

    #[test]
    fn semantic_cursor_rejects_restart_and_every_bound_identity_drift() {
        let request = request();
        let query_view = EphemeralSanitizedQueryViewV1::sanitize(
            "cursor identity",
            id::<SanitizerRevision>("sanitizer.pagination.v1"),
            id::<QueryNormalizationRevision>("normalization.pagination.v1"),
        )
        .expect("query view");
        let authority = query_authority(&request);
        let query_composition = composition(
            query_profile().profile_id,
            &RetrieverKind::QUERY_FALLBACK_LANES,
        );
        let authorized = AuthorizedQueryFallbackV1 {
            query_digest: authority
                .authenticate_query(&request, &query_view)
                .expect("query digest"),
            fallback: fallback(),
            composition: query_composition,
            fallback_lanes: Vec::new(),
            page_size: 2,
            request_cursor: None,
        };
        let mut semantic_composition = composition(
            id("profile.semantic.cursor-identity.v1"),
            &[
                RetrieverKind::ExactLiteral,
                RetrieverKind::Lexical,
                RetrieverKind::Graph,
                RetrieverKind::Semantic,
            ],
        );
        let code_generation = id::<CodeGenerationId>("code-generation.cursor-identity.v1");
        let vector_generation = VectorGenerationIdV1::new(digest::<ManifestDigest>('a'));
        let projection = ProjectionKeyV1 {
            kind: ProjectionKindV1::Embedding,
            schema_revision: "projection.cursor-identity.v1".to_owned(),
            profile_digest: digest('b'),
        };
        let search_index = search_index_key();
        let ranking_revision = id::<ComponentRevision>("ranking.cursor-identity.v1");
        let profile_digest = digest::<ManifestDigest>('c');
        let route =
            SemanticRouteIdentityV1::for_pagination_test(&request, &authorized, &projection);
        let cursor = paginate_semantic_composition(
            &authority,
            &request,
            &query_view,
            &authorized,
            &profile_digest,
            &code_generation,
            &vector_generation,
            &projection,
            &search_index,
            &ranking_revision,
            &semantic_budget(2),
            &OptionalStagePublicStatus::NotRequested,
            &mut semantic_composition,
        )
        .expect("first semantic page")
        .expect("continuation");

        let same = (
            profile_digest.clone(),
            code_generation.clone(),
            vector_generation.clone(),
            projection.clone(),
            search_index.clone(),
            ranking_revision.clone(),
        );
        assert!(semantic_cursor_matches_route(
            Some(&cursor),
            &semantic_composition.profile_id,
            &same.0,
            &same.1,
            &same.2,
            &same.3,
            &same.4,
            &same.5,
            &route,
        ));

        let mut changed_route = route.clone();
        changed_route.model_artifact_digest = digest('d');
        assert!(!semantic_cursor_matches_route(
            Some(&cursor),
            &semantic_composition.profile_id,
            &same.0,
            &same.1,
            &same.2,
            &same.3,
            &same.4,
            &same.5,
            &changed_route,
        ));
        let mut changed_route = route.clone();
        changed_route.capability_manifest_digest = digest('e');
        assert!(!semantic_cursor_matches_route(
            Some(&cursor),
            &semantic_composition.profile_id,
            &same.0,
            &same.1,
            &same.2,
            &same.3,
            &same.4,
            &same.5,
            &changed_route,
        ));
        let mut changed_route = route.clone();
        changed_route.execution_provider = tracedecay_domain::EmbeddingExecutionProviderV1::Cuda;
        assert!(!semantic_cursor_matches_route(
            Some(&cursor),
            &semantic_composition.profile_id,
            &same.0,
            &same.1,
            &same.2,
            &same.3,
            &same.4,
            &same.5,
            &changed_route,
        ));
        let mut changed_route = route.clone();
        changed_route.privacy_domain = id("privacy.cursor-identity.v2");
        assert!(!semantic_cursor_matches_route(
            Some(&cursor),
            &semantic_composition.profile_id,
            &same.0,
            &same.1,
            &same.2,
            &same.3,
            &same.4,
            &same.5,
            &changed_route,
        ));
        let mut changed_route = route.clone();
        changed_route.privacy_key_epoch += 1;
        assert!(!semantic_cursor_matches_route(
            Some(&cursor),
            &semantic_composition.profile_id,
            &same.0,
            &same.1,
            &same.2,
            &same.3,
            &same.4,
            &same.5,
            &changed_route,
        ));
        let mut changed_route = route.clone();
        changed_route.source_scope.reference = Some(id("refs/heads/restarted"));
        assert!(!semantic_cursor_matches_route(
            Some(&cursor),
            &semantic_composition.profile_id,
            &same.0,
            &same.1,
            &same.2,
            &same.3,
            &same.4,
            &same.5,
            &changed_route,
        ));

        let mut changed_search_index = search_index.clone();
        changed_search_index.profile_digest = digest('d');
        assert!(!semantic_cursor_matches_activation(
            Some(&cursor),
            &id("profile.semantic.cursor-identity.v2"),
            &profile_digest,
            &code_generation,
            &vector_generation,
            &projection,
            &search_index,
            &ranking_revision,
        ));
        assert!(!semantic_cursor_matches_activation(
            Some(&cursor),
            &semantic_composition.profile_id,
            &digest('e'),
            &code_generation,
            &vector_generation,
            &projection,
            &search_index,
            &ranking_revision,
        ));
        assert!(!semantic_cursor_matches_activation(
            Some(&cursor),
            &semantic_composition.profile_id,
            &profile_digest,
            &id("code-generation.cursor-identity.v2"),
            &vector_generation,
            &projection,
            &search_index,
            &ranking_revision,
        ));
        assert!(!semantic_cursor_matches_activation(
            Some(&cursor),
            &semantic_composition.profile_id,
            &profile_digest,
            &code_generation,
            &VectorGenerationIdV1::new(digest('f')),
            &projection,
            &search_index,
            &ranking_revision,
        ));
        assert!(!semantic_cursor_matches_activation(
            Some(&cursor),
            &semantic_composition.profile_id,
            &profile_digest,
            &code_generation,
            &vector_generation,
            &ProjectionKeyV1 {
                kind: ProjectionKindV1::Embedding,
                schema_revision: "projection.cursor-identity.v2".to_owned(),
                profile_digest: digest('b'),
            },
            &search_index,
            &ranking_revision,
        ));
        assert!(!semantic_cursor_matches_activation(
            Some(&cursor),
            &semantic_composition.profile_id,
            &profile_digest,
            &code_generation,
            &vector_generation,
            &projection,
            &changed_search_index,
            &ranking_revision,
        ));
        assert!(!semantic_cursor_matches_activation(
            Some(&cursor),
            &semantic_composition.profile_id,
            &profile_digest,
            &code_generation,
            &vector_generation,
            &projection,
            &search_index,
            &id("ranking.cursor-identity.v2"),
        ));
    }

    #[test]
    fn mock_semantic_recomposition_keeps_protected_exact_incumbents() {
        let exact = mock_exact_candidate("protected", "shared");
        let protected = mock_protected_exact_ranked(&exact);
        let semantic = mock_compact_candidate(
            RetrieverKind::Semantic,
            "semantic-challenger",
            1_000_000,
            0,
            "shared",
        );
        let fallback = Arc::new(
            QueryFallbackSubpayload::new(
                query_profile().profile_id,
                vec![protected.clone()],
                BTreeMap::from([
                    (RetrieverKind::ExactLiteral, PublicRetrieverStatus::Complete),
                    (RetrieverKind::Lexical, PublicRetrieverStatus::Complete),
                    (RetrieverKind::Graph, PublicRetrieverStatus::Complete),
                ]),
                Vec::new(),
                None,
            )
            .expect("protected exact fallback"),
        );
        let mut fallback_composition = composition(
            query_profile().profile_id,
            &RetrieverKind::QUERY_FALLBACK_LANES,
        );
        fallback_composition.ranked_candidates = vec![protected.clone()];
        let authorized = AuthorizedQueryFallbackV1 {
            query_digest: QueryDigest::new(
                id("privacy.mock-semantic"),
                1,
                QueryMac::new(format!("hmac-sha256:{}", "1".repeat(64))).expect("query MAC"),
            ),
            fallback: Arc::clone(&fallback),
            composition: fallback_composition,
            fallback_lanes: vec![
                mock_lane(RetrieverKind::ExactLiteral, vec![exact]),
                mock_lane(RetrieverKind::Lexical, Vec::new()),
                mock_lane(RetrieverKind::Graph, Vec::new()),
            ],
            page_size: 4,
            request_cursor: None,
        };
        let profile = mock_semantic_profile();
        let diversity = DiversityPolicy {
            policy_id: profile.diversity_policy_id.clone(),
            evaluation_result_anchor: Some(profile.evaluation_result_anchor.clone()),
            per_source_namespace: None,
            per_source_instance: None,
            per_repository: None,
            per_file: Some(1),
            per_session_or_thread: None,
            per_copy_cluster: None,
            per_evidence_role: None,
        };
        let authority = SemanticCompositionExecutionAuthorityV1::new(
            profile,
            diversity,
            None,
            id("ranking.mock-semantic.v1"),
        )
        .expect("semantic composition authority");
        let outcome = authority
            .execute(
                &authorized,
                SemanticQueryServiceOutcomeV1::Augmented {
                    semantic_lane: mock_lane(RetrieverKind::Semantic, vec![semantic]),
                    calibration: SemanticCalibrationEvidenceV1 {
                        calibration_profile_id: id("calibration.mock-semantic.semantic.v1"),
                        cohort_digest: digest('3'),
                        best_distance: serde_json::from_str("0").expect("distance"),
                        next_best_margin_micros: u64::MAX,
                    },
                    fallback: Arc::clone(&fallback),
                },
                SemanticAbstentionDispositionV1::UseFallback,
            )
            .expect("mock semantic lane composes");
        let SemanticCompositionExecutionOutcomeV1::Augmented(executed) = outcome else {
            panic!("an admitted mock semantic lane must augment");
        };

        assert_eq!(Arc::as_ptr(&executed.fallback), Arc::as_ptr(&fallback));
        assert_eq!(
            executed.composition.ranked_candidates[0]
                .candidate
                .anchor_id
                .as_str(),
            "anchor.mock-semantic.protected"
        );
        assert!(
            executed
                .composition
                .ranked_candidates
                .iter()
                .any(|candidate| candidate.candidate.exact_class != ExactClass::Approximate)
        );
    }

    struct IdentityRerankExecutorV1 {
        digest: ManifestDigest,
    }

    impl DeterministicLocalRerankExecutorV1 for IdentityRerankExecutorV1 {
        fn planned_model_invocations(
            &self,
            _candidate_count: u32,
        ) -> Result<u32, LocalRerankFailureV1> {
            Ok(1)
        }

        fn rerank(
            &self,
            _policy: &tracedecay_domain::RerankPolicy,
            inputs: &[LocalRerankInputV1<'_>],
            _permit: LocalRerankPermitV1,
        ) -> Result<Vec<RetrievalAnchorId>, LocalRerankFailureV1> {
            Ok(inputs
                .iter()
                .map(|input| input.candidate.candidate.anchor_id.clone())
                .collect())
        }
    }

    impl AdmittedNativeRerankExecutorV1 for IdentityRerankExecutorV1 {
        fn artifact_manifest_digest(&self) -> &ManifestDigest {
            &self.digest
        }
    }

    fn rerank_pins(byte: char) -> RerankCompatibilityPinsV1 {
        RerankCompatibilityPinsV1 {
            implementation_revision: id("rerank.fastembed.production.v1"),
            artifact_manifest_digest: digest(byte),
            runtime_compatibility_digest: digest(byte),
        }
    }

    #[test]
    fn configured_rerank_is_unavailable_when_unmounted_or_pins_diverge() {
        let pins = rerank_pins('a');
        let unmounted = ConfiguredRerankAuthorityV1 {
            pins: pins.clone(),
            mounted: None,
        };
        assert!(mounted_compatible_rerank(Some(&unmounted)).is_none());
        assert!(mounted_compatible_rerank(None).is_none());

        let mounted = ProductionCodeRerankAuthorityV1::from_executor_for_test(
            rerank_pins('b'),
            Arc::new(IdentityRerankExecutorV1 {
                digest: digest('b'),
            }),
        );
        let mismatched = ConfiguredRerankAuthorityV1 {
            pins,
            mounted: Some(mounted),
        };
        assert!(mounted_compatible_rerank(Some(&mismatched)).is_none());
    }

    #[test]
    fn configured_rerank_selects_the_mounted_authority_with_exact_pins() {
        let pins = rerank_pins('c');
        let mounted = ProductionCodeRerankAuthorityV1::from_executor_for_test(
            pins.clone(),
            Arc::new(IdentityRerankExecutorV1 {
                digest: digest('c'),
            }),
        );
        let configured = ConfiguredRerankAuthorityV1 {
            pins: pins.clone(),
            mounted: Some(mounted),
        };
        let selected = mounted_compatible_rerank(Some(&configured)).expect("compatible mount");
        assert_eq!(selected.compatibility(), &pins);
    }
}
