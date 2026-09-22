//! Deferred bounded rerank compatibility contracts.
//!
//! V2 does not execute a reranker. These borrowed view and executor contracts
//! remain available to adapters that compile against the historical surface;
//! [`BoundedRerankRuntimeV1`] always returns the unchanged input with a typed
//! unavailable status until a later serving contract admits the stage.

use tracedecay_domain::{
    AuthorizedRerankView, CandidateSetDigest, ManifestDigest, OptionalStagePublicStatus,
    PrivacyDomainId, RankedCandidate, RerankPolicy, RetrievalAnchorId, RetrievalRequest,
    SanitizedStageFailure,
};

use super::ports::RetrievalExecutionControl;

/// A strict permit for producing one ephemeral authorized view.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RerankViewPermitV1 {
    pub expected_snapshot_digest: CandidateSetDigest,
    pub expected_privacy_domain: PrivacyDomainId,
    pub remaining_input_bytes: u64,
    pub remaining_input_tokens: u64,
    pub remaining_work_units: u64,
    pub remaining_deadline_micros: Option<u64>,
}

/// Internal authorization result. Missing and denied deliberately coalesce to
/// the same public authority-unavailable status.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RerankViewOutcomeV1 {
    Authorized {
        view: AuthorizedRerankView,
        input_tokens: u64,
        work_units: u64,
    },
    Missing,
    Denied,
    Unavailable(SanitizedStageFailure),
    Cancelled,
}

/// Produces source-local, authorized views for this invocation only.
///
/// Implementations must not cache or persist either returned views or their
/// approved feature bytes.
pub trait EphemeralRerankViewSourceV1 {
    fn authorize_ephemeral_view(
        &mut self,
        request: &RetrievalRequest,
        candidate: &RankedCandidate,
        permit: &RerankViewPermitV1,
    ) -> RerankViewOutcomeV1;
}

/// One borrowed, authorized executor input. It cannot outlive this invocation.
#[derive(Clone, Copy, Debug)]
pub struct LocalRerankInputV1<'a> {
    pub candidate: &'a RankedCandidate,
    pub view: &'a AuthorizedRerankView,
}

/// Strict resource permit passed to the deterministic local executor.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct LocalRerankPermitV1 {
    pub input_bytes: u64,
    pub input_tokens: u64,
    pub work_units: u64,
    pub model_invocations: u32,
    pub remaining_deadline_micros: Option<u64>,
}

/// Sanitized local executor failure.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LocalRerankFailureV1 {
    Unavailable(SanitizedStageFailure),
    Rejected(SanitizedStageFailure),
    TimedOut,
    Cancelled,
}

/// Deterministic, local-only rerank execution port.
///
/// `planned_model_invocations` is a payload-free preflight and must not run
/// the model. `rerank` returns only a permutation of admitted anchors; it
/// cannot inject model-specific scores into the retrieval contract.
pub trait DeterministicLocalRerankExecutorV1 {
    fn planned_model_invocations(&self, candidate_count: u32) -> Result<u32, LocalRerankFailureV1>;

    fn rerank(
        &self,
        policy: &RerankPolicy,
        inputs: &[LocalRerankInputV1<'_>],
        permit: LocalRerankPermitV1,
    ) -> Result<Vec<RetrievalAnchorId>, LocalRerankFailureV1>;
}

/// Deterministic local executor admitted from one verified artifact.
pub trait AdmittedNativeRerankExecutorV1: DeterministicLocalRerankExecutorV1 {
    fn artifact_manifest_digest(&self) -> &ManifestDigest;
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct RerankUsageV1 {
    pub candidates: u32,
    pub input_bytes: u64,
    pub input_tokens: u64,
    pub work_units: u64,
    pub model_invocations: u32,
    pub elapsed_micros: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BoundedRerankOutcomeV1 {
    pub ordered_candidates: Vec<RankedCandidate>,
    pub public_status: OptionalStagePublicStatus,
    pub usage: RerankUsageV1,
}

/// Compatibility shell for the deferred rerank stage.
///
/// V2 does not execute a reranker. Keeping this shell lets older adapters
/// compile while ensuring that a direct caller receives the same unchanged
/// post-composition candidates and a typed unavailable status as the semantic
/// composition authority.
pub struct BoundedRerankRuntimeV1<'a, S: ?Sized, E: ?Sized> {
    views: &'a mut S,
    executor: &'a E,
}

impl<'a, S, E> BoundedRerankRuntimeV1<'a, S, E>
where
    S: EphemeralRerankViewSourceV1 + ?Sized,
    E: DeterministicLocalRerankExecutorV1 + ?Sized,
{
    pub fn new(views: &'a mut S, executor: &'a E) -> Self {
        Self { views, executor }
    }

    #[hotpath::measure(label = "query.rerank")]
    pub fn rerank(
        &mut self,
        request: &RetrievalRequest,
        policy: &RerankPolicy,
        pre_rerank: &[RankedCandidate],
        control: &dyn RetrievalExecutionControl,
    ) -> BoundedRerankOutcomeV1 {
        let _ = (&mut self.views, self.executor, request, policy);
        BoundedRerankOutcomeV1 {
            ordered_candidates: pre_rerank.to_vec(),
            public_status: OptionalStagePublicStatus::Unavailable(
                SanitizedStageFailure::AuthorityUnavailable,
            ),
            usage: RerankUsageV1 {
                candidates: u32::try_from(pre_rerank.len()).unwrap_or(u32::MAX),
                elapsed_micros: control.elapsed_micros(),
                ..RerankUsageV1::default()
            },
        }
    }
}
