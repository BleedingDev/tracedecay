use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use tracedecay_contracts::SemanticActivationCoordinationPort;
use tracedecay_domain::configuration::ConfigurationRevisionId;
use tracedecay_domain::{ManifestDigest, UtcMicros};

use super::{
    ConfigurationLinkedSemanticRuntimeBackendV1, DurableSemanticActivationJournalV1,
    ProductionSemanticRetrievalConfigurationStoreV1, ProductionSemanticRuntimeV1,
    RetrievalProfileActivationObserverV1, SemanticActivationAuthorityV1,
    SemanticActivationCandidateV1, SemanticActivationReceiptV1, SemanticActivationRequestV1,
    SemanticConfigurationBackendErrorV1, SemanticConfigurationPinV1, SemanticRollbackReceiptV1,
    SemanticRollbackRequestV1, SemanticRuntimeControlErrorV1, SemanticRuntimeFuture,
    SemanticRuntimeIntegrationPortV1, SemanticRuntimeOwnerV1, SemanticRuntimeStatusV1,
};
use crate::config::retrieval::{
    AcceptedRetrievalProfileV1, RetrievalProfileCasV1, RetrievalProfileMutationCapabilityV1,
    RetrievalProfileStateSnapshotV1, RetrievalProfileStateV1, RetrievalRuntimeCompatibilityV1,
};
use tracedecay_contracts::{
    SemanticActivationAuthorityReceiptV1, SemanticActivationAvailabilityV1,
};
use tracedecay_global_db::configuration::OwnedGlobalDbConfigurationControlStore;
use tracedecay_global_db::configuration::contracts::{
    ConfigurationCurrentStateV1, ConfigurationMutationAuthority, DirectConfigurationMutation,
};
use tracedecay_global_db::configuration::store::ConfigurationDirectCommitOutcomeV1;

pub use tracedecay_contracts::SemanticActivationCoordinationErrorV1;

type ProductionOwner = SemanticRuntimeOwnerV1<
    OwnedGlobalDbConfigurationControlStore,
    ConfigurationLinkedSemanticRuntimeBackendV1<
        ProductionSemanticRetrievalConfigurationStoreV1,
        ProductionSemanticRuntimeV1,
    >,
>;

impl From<SemanticRuntimeControlErrorV1> for SemanticActivationCoordinationErrorV1 {
    fn from(error: SemanticRuntimeControlErrorV1) -> Self {
        Self::Runtime(error.to_string())
    }
}

/// Application coordinator for an already-authorized configuration mutation and its
/// linked semantic profile transition. It never selects profiles, fabricates
/// grants, or exposes a transport endpoint.
pub struct ProductionSemanticActivationCoordinatorV1 {
    configuration: ProductionSemanticRetrievalConfigurationStoreV1,
    owner: Arc<ProductionOwner>,
    authority: Option<Arc<SemanticActivationAuthorityV1<DurableSemanticActivationJournalV1>>>,
}

impl ProductionSemanticActivationCoordinatorV1 {
    pub fn new(
        configuration: ProductionSemanticRetrievalConfigurationStoreV1,
        central_configuration: OwnedGlobalDbConfigurationControlStore,
        runtime: ProductionSemanticRuntimeV1,
        observer: Arc<dyn RetrievalProfileActivationObserverV1>,
    ) -> Self {
        let authority = configuration.activation_authority().ok();
        Self::new_with_authority(
            configuration,
            central_configuration,
            runtime,
            observer,
            authority,
        )
    }

    /// Fallible production constructor used by composition roots that must
    /// refuse startup when the semantic authority journal cannot be opened or
    /// replayed. [`Self::new`] remains an infallible compatibility wrapper and
    /// leaves the coordinator unavailable when this admission fails.
    pub fn try_new(
        configuration: ProductionSemanticRetrievalConfigurationStoreV1,
        central_configuration: OwnedGlobalDbConfigurationControlStore,
        runtime: ProductionSemanticRuntimeV1,
        observer: Arc<dyn RetrievalProfileActivationObserverV1>,
    ) -> Result<Self, SemanticActivationCoordinationErrorV1> {
        let authority = configuration
            .activation_authority()
            .map_err(|_| SemanticActivationCoordinationErrorV1::Unavailable)?;
        Ok(Self::new_with_authority(
            configuration,
            central_configuration,
            runtime,
            observer,
            Some(authority),
        ))
    }

    fn new_with_authority(
        configuration: ProductionSemanticRetrievalConfigurationStoreV1,
        central_configuration: OwnedGlobalDbConfigurationControlStore,
        runtime: ProductionSemanticRuntimeV1,
        observer: Arc<dyn RetrievalProfileActivationObserverV1>,
        authority: Option<Arc<SemanticActivationAuthorityV1<DurableSemanticActivationJournalV1>>>,
    ) -> Self {
        let backend = ConfigurationLinkedSemanticRuntimeBackendV1::new_with_activation_observer(
            configuration.clone(),
            runtime,
            observer,
        );
        Self {
            configuration,
            owner: Arc::new(SemanticRuntimeOwnerV1::new(central_configuration, backend)),
            authority,
        }
    }

    pub(crate) fn configuration_inventory_authority(
        &self,
    ) -> ProductionSemanticRetrievalConfigurationStoreV1 {
        self.configuration.clone()
    }

    pub fn semantic_activation_authority(
        &self,
    ) -> Option<Arc<SemanticActivationAuthorityV1<DurableSemanticActivationJournalV1>>> {
        self.authority.clone()
    }

    /// Replay the durable authority and finish one pending linked transition
    /// left by an interrupted process. The pending central mutation envelope
    /// is read before any retry; successful recovery runs through the same
    /// owner commit path, which deletes the pending row and sidecar only after
    /// the central transaction is durable.
    pub async fn recover_after_restart(
        &self,
        now: UtcMicros,
    ) -> Result<Option<SemanticActivationAuthorityReceiptV1>, SemanticActivationCoordinationErrorV1>
    {
        let authority = self
            .authority
            .clone()
            .ok_or(SemanticActivationCoordinationErrorV1::Unavailable)?;
        let pending = self
            .configuration
            .pending_transition_for_recovery()
            .await
            .map_err(configuration_error_at("recover.pending"))?;
        if let Some(pending) = pending {
            let transition = pending.transition;
            let authority_receipt = match self
                .configuration
                .recover_pending_authority_receipt(&transition)
                .map_err(configuration_error_at("recover.authority_receipt"))?
            {
                Some(receipt) => receipt,
                None => {
                    let expected = expected_authority_digest(
                        authority.current_receipt().as_ref(),
                        &transition,
                        matches!(
                            transition.operation,
                            crate::config::retrieval::RetrievalProfileAuditOperationV1::Rollback { .. }
                        ),
                    )?;
                    match transition.operation {
                        crate::config::retrieval::RetrievalProfileAuditOperationV1::Activate => {
                            let candidate = self
                                .authority_candidate_from_transition(&transition, expected)
                                .await?;
                            authority
                                .activate(candidate, now)
                                .map_err(authority_error_at)?
                        }
                        crate::config::retrieval::RetrievalProfileAuditOperationV1::Rollback {
                            ..
                        } => authority
                            .rollback(expected, now)
                            .map_err(authority_error_at)?,
                    }
                }
            };
            if authority.current_receipt().as_ref() != Some(&authority_receipt) {
                return Err(SemanticActivationCoordinationErrorV1::Conflict);
            }
            self.configuration
                .remember_authority_receipt(
                    &transition.transition_digest,
                    authority_receipt.clone(),
                )
                .map_err(configuration_error_at("recover.remember_authority_receipt"))?;
            match transition.operation {
                crate::config::retrieval::RetrievalProfileAuditOperationV1::Activate => {
                    let request = activation_request_from_transition(&transition)?;
                    self.owner
                        .activate(request)
                        .await
                        .map_err(SemanticActivationCoordinationErrorV1::from)?;
                }
                crate::config::retrieval::RetrievalProfileAuditOperationV1::Rollback { .. } => {
                    let request = rollback_request_from_transition(&transition)?;
                    self.owner
                        .rollback(request)
                        .await
                        .map_err(SemanticActivationCoordinationErrorV1::from)?;
                }
            }
            return Ok(Some(authority_receipt));
        }

        // A clean restart still records a restart receipt, giving the next
        // process a durable proof that this exact component was remounted.
        self.configuration
            .cleanup_terminal_pending()
            .await
            .map_err(configuration_error_at("recover.cleanup_terminal_pending"))?;
        let restarted = if let Some(current) = authority.current_receipt() {
            // Never append a restart marker until the configuration owner has
            // proved that the same authority receipt is the committed serving
            // state. This keeps an authority journal from advancing after a
            // crash that left central state behind.
            let committed = self
                .configuration
                .current_committed_state()
                .await
                .map_err(configuration_error_at("recover.current_committed_state"))?
                .ok_or(SemanticActivationCoordinationErrorV1::Unavailable)?;
            let observed = committed
                .current_activation
                .as_ref()
                .and_then(|activation| activation.authority_receipt.as_ref())
                .ok_or(SemanticActivationCoordinationErrorV1::Unavailable)?;
            if observed.receipt_digest != current.receipt_digest {
                return Err(SemanticActivationCoordinationErrorV1::Conflict);
            }
            Some(
                authority
                    .restart_with_expected(current.receipt_digest, now)
                    .map_err(authority_error_at)?,
            )
        } else {
            None
        };
        self.reobserve_current_activation().await?;
        Ok(restarted)
    }

    /// Expose journal replay status for production Doctor/startup callers.
    /// Opening the coordinator has already replayed the journal; this method
    /// returns the current fail-closed availability without mutating it.
    pub fn semantic_activation_status(&self) -> SemanticActivationAvailabilityV1 {
        self.authority.as_ref().map_or(
            SemanticActivationAvailabilityV1::Unavailable {
                reason: tracedecay_contracts::SemanticActivationUnavailableReasonV1::Corrupt,
            },
            |authority| authority.status(),
        )
    }

    /// Re-observe the exact latest durable transition after a verified model
    /// lifecycle recovery. The registrar remains the sole live-route publisher;
    /// this method neither changes configuration nor publishes graph state.
    #[hotpath::measure(label = "usecases.semantic.reobserve", future = true)]
    pub async fn reobserve_current_activation(
        &self,
    ) -> Result<
        Option<(
            i64,
            tracedecay_domain::ConfigurationRevisionId,
            ManifestDigest,
        )>,
        SemanticActivationCoordinationErrorV1,
    > {
        let Some(committed) = self
            .configuration
            .current_committed_state()
            .await
            .map_err(configuration_error_at("current_committed_state"))?
        else {
            return Ok(None);
        };
        let identity = (
            committed.epoch,
            committed.state.configuration_revision().clone(),
            committed.transition_digest.clone(),
        );
        self.owner
            .runtime()
            .reconcile_committed_activation(committed)
            .await
            .map_err(|error| match error {
                super::RetrievalProfileActivationObserverErrorV1::Unavailable => {
                    SemanticActivationCoordinationErrorV1::Unavailable
                }
                super::RetrievalProfileActivationObserverErrorV1::Rejected => {
                    SemanticActivationCoordinationErrorV1::Rejected
                }
                super::RetrievalProfileActivationObserverErrorV1::Conflict => {
                    SemanticActivationCoordinationErrorV1::Conflict
                }
            })?;
        Ok(Some(identity))
    }

    #[hotpath::measure(label = "usecases.semantic.bootstrap", future = true)]
    pub async fn bootstrap_query_profile(
        &self,
        configuration: ConfigurationCurrentStateV1,
        accepted_query: AcceptedRetrievalProfileV1,
        runtime: &RetrievalRuntimeCompatibilityV1,
    ) -> Result<(), SemanticActivationCoordinationErrorV1> {
        if !accepted_query.is_exact_query_fallback() {
            return Err(SemanticActivationCoordinationErrorV1::Rejected);
        }
        let pin = SemanticConfigurationPinV1::from_current(&configuration)
            .map_err(|_| SemanticActivationCoordinationErrorV1::Rejected)?;
        let state =
            RetrievalProfileStateV1::new(configuration.revision_id, accepted_query, runtime)
                .map_err(|_| SemanticActivationCoordinationErrorV1::Rejected)?;
        self.configuration
            .install_initial_state(&pin, &state)
            .await
            .map_err(configuration_error_at("bootstrap_query_profile.install"))
    }

    pub async fn current_profile_state(
        &self,
    ) -> Result<
        crate::config::retrieval::RetrievalProfileStateSnapshotV1,
        SemanticActivationCoordinationErrorV1,
    > {
        self.configuration
            .current_profile_state()
            .await
            .map_err(configuration_error_at("current_profile_state"))
    }

    pub async fn preview_central_mutation(
        &self,
        authority: &ConfigurationMutationAuthority,
        mutation: &DirectConfigurationMutation,
        expected_revision: &tracedecay_domain::ConfigurationRevisionId,
    ) -> Result<
        tracedecay_global_db::configuration::store::ConfigurationDirectCommitOutcomeV1,
        SemanticActivationCoordinationErrorV1,
    > {
        self.configuration
            .preview_central_mutation(authority, mutation, expected_revision)
            .await
            .map_err(configuration_error_at("preview_central_mutation"))
    }

    #[allow(clippy::too_many_arguments)]
    #[hotpath::measure(label = "usecases.semantic.activate", future = true)]
    pub async fn stage_and_activate(
        &self,
        base_configuration: SemanticConfigurationPinV1,
        result_configuration: ConfigurationCurrentStateV1,
        capability: &RetrievalProfileMutationCapabilityV1,
        expected: RetrievalProfileCasV1,
        candidate: AcceptedRetrievalProfileV1,
        current_runtime: &RetrievalRuntimeCompatibilityV1,
        candidate_runtime: &RetrievalRuntimeCompatibilityV1,
        central_mutation: DirectConfigurationMutation,
        freshness_vector_digest: ManifestDigest,
        now: UtcMicros,
    ) -> Result<SemanticActivationReceiptV1, SemanticActivationCoordinationErrorV1> {
        let candidate_for_authority = candidate.clone();
        let result_configuration = SemanticConfigurationPinV1::from_current(&result_configuration)
            .map_err(|_| SemanticActivationCoordinationErrorV1::Rejected)?;
        let transition = self
            .configuration
            .stage_activation(
                base_configuration,
                result_configuration,
                capability,
                expected,
                candidate,
                current_runtime,
                candidate_runtime,
                central_mutation,
                freshness_vector_digest,
                now,
            )
            .await
            .map_err(configuration_error_at(
                "stage_and_activate.stage_activation",
            ))?;
        let authority = self
            .authority
            .as_ref()
            .ok_or(SemanticActivationCoordinationErrorV1::Unavailable)?;
        let expected_authority =
            expected_authority_digest(authority.current_receipt().as_ref(), &transition, false)?;
        let authority_candidate = self
            .authority_candidate(&transition, &candidate_for_authority, expected_authority)
            .await?;
        let authority_receipt = authority
            .activate(authority_candidate, now)
            .map_err(authority_error_at)?;
        self.configuration
            .remember_authority_receipt(&transition.transition_digest, authority_receipt)
            .map_err(configuration_error_at(
                "stage_and_activate.authority_receipt",
            ))?;
        let target = transition
            .result_active_semantic
            .as_ref()
            .ok_or(SemanticActivationCoordinationErrorV1::Rejected)?
            .vector_generation_id
            .clone();
        let request = SemanticActivationRequestV1::new(
            target,
            transition
                .prior_active_semantic
                .as_ref()
                .map(|pins| pins.vector_generation_id.clone()),
            transition
                .prior_rollback_semantic
                .as_ref()
                .map(|pins| pins.vector_generation_id.clone()),
        )
        .map_err(|_| SemanticActivationCoordinationErrorV1::Rejected)?;
        self.owner
            .activate(request)
            .await
            .map_err(SemanticActivationCoordinationErrorV1::from)
            .inspect_err(crate::hotpath_observe::semantic_coordination_error)
    }

    #[allow(clippy::too_many_arguments)]
    #[hotpath::measure(label = "usecases.semantic.rollback", future = true)]
    pub async fn stage_and_rollback(
        &self,
        base_configuration: SemanticConfigurationPinV1,
        result_configuration: ConfigurationCurrentStateV1,
        capability: &RetrievalProfileMutationCapabilityV1,
        expected: RetrievalProfileCasV1,
        restored_runtime: &RetrievalRuntimeCompatibilityV1,
        central_mutation: DirectConfigurationMutation,
        trigger: String,
        freshness_vector_digest: ManifestDigest,
        now: UtcMicros,
    ) -> Result<SemanticRollbackReceiptV1, SemanticActivationCoordinationErrorV1> {
        let result_configuration = SemanticConfigurationPinV1::from_current(&result_configuration)
            .map_err(|_| {
                SemanticActivationCoordinationErrorV1::RejectedDetail(
                    "stage_and_rollback: result configuration is not pinnable".to_owned(),
                )
            })?;
        let transition = self
            .configuration
            .stage_rollback(
                base_configuration,
                result_configuration,
                capability,
                expected,
                restored_runtime,
                central_mutation,
                trigger,
                freshness_vector_digest,
                now,
            )
            .await
            .map_err(configuration_error_at("stage_and_rollback.stage_rollback"))?;
        let authority = self
            .authority
            .as_ref()
            .ok_or(SemanticActivationCoordinationErrorV1::Unavailable)?;
        let expected_authority =
            expected_authority_digest(authority.current_receipt().as_ref(), &transition, true)?;
        let authority_candidate = self
            .authority_candidate_from_transition(&transition, expected_authority)
            .await?;
        let authority_receipt = authority
            .rollback(authority_candidate.expected_current_receipt_digest, now)
            .map_err(authority_error_at)?;
        self.configuration
            .remember_authority_receipt(&transition.transition_digest, authority_receipt)
            .map_err(configuration_error_at(
                "stage_and_rollback.authority_receipt",
            ))?;
        let expected_active = transition
            .prior_active_semantic
            .as_ref()
            .ok_or_else(|| {
                SemanticActivationCoordinationErrorV1::RejectedDetail(
                    "stage_and_rollback: staged transition has no prior active semantic pin"
                        .to_owned(),
                )
            })?
            .vector_generation_id
            .clone();
        let request = match transition.result_active_semantic.as_ref() {
            Some(target) => SemanticRollbackRequestV1::new(
                target.vector_generation_id.clone(),
                expected_active,
                transition
                    .prior_rollback_semantic
                    .as_ref()
                    .ok_or_else(|| {
                        SemanticActivationCoordinationErrorV1::RejectedDetail(
                            "stage_and_rollback: staged transition has no prior rollback \
                             semantic pin"
                                .to_owned(),
                        )
                    })?
                    .vector_generation_id
                    .clone(),
            ),
            None => SemanticRollbackRequestV1::disable(expected_active),
        }
        .map_err(|_| {
            SemanticActivationCoordinationErrorV1::RejectedDetail(
                "stage_and_rollback: rollback request is invalid".to_owned(),
            )
        })?;
        self.owner
            .rollback(request)
            .await
            .map_err(SemanticActivationCoordinationErrorV1::from)
            .inspect_err(crate::hotpath_observe::semantic_coordination_error)
    }

    async fn authority_candidate(
        &self,
        transition: &super::SemanticConfigurationTransitionV1,
        accepted: &AcceptedRetrievalProfileV1,
        expected_current_receipt_digest: Option<ManifestDigest>,
    ) -> Result<SemanticActivationCandidateV1, SemanticActivationCoordinationErrorV1> {
        let semantic = transition
            .result_active_semantic
            .as_ref()
            .ok_or(SemanticActivationCoordinationErrorV1::Rejected)?;
        let source_generation = self
            .owner
            .runtime()
            .generations()
            .active_vector_generation(semantic)
            .await
            .ok_or(SemanticActivationCoordinationErrorV1::Unavailable)?
            .source_generation()
            .clone();
        let epochs =
            self.next_authority_epochs(semantic.projection.embedding_key().privacy_key_epoch)?;
        let binding = accepted
            .semantic_activation_binding(
                source_generation,
                epochs[0],
                epochs[1],
                epochs[2],
                epochs[3],
                epochs[4],
                epochs[5],
                epochs[6],
            )
            .map_err(|_| SemanticActivationCoordinationErrorV1::Rejected)?;
        SemanticActivationCandidateV1::new(
            transition.result_configuration.revision_id.clone(),
            binding,
            expected_current_receipt_digest,
        )
        .map_err(|_| SemanticActivationCoordinationErrorV1::Rejected)
    }

    async fn authority_candidate_from_transition(
        &self,
        transition: &super::SemanticConfigurationTransitionV1,
        expected_current_receipt_digest: Option<ManifestDigest>,
    ) -> Result<SemanticActivationCandidateV1, SemanticActivationCoordinationErrorV1> {
        let semantic = transition
            .result_active_semantic
            .as_ref()
            .ok_or(SemanticActivationCoordinationErrorV1::Rejected)?;
        let source_generation = self
            .owner
            .runtime()
            .generations()
            .active_vector_generation(semantic)
            .await
            .ok_or(SemanticActivationCoordinationErrorV1::Unavailable)?
            .source_generation()
            .clone();
        let epochs =
            self.next_authority_epochs(semantic.projection.embedding_key().privacy_key_epoch)?;
        let binding = semantic
            .activation_binding(
                source_generation,
                transition.result_active_profile_digest.clone(),
                epochs[0],
                epochs[1],
                epochs[2],
                epochs[3],
                epochs[4],
                epochs[5],
                epochs[6],
            )
            .map_err(|_| SemanticActivationCoordinationErrorV1::Rejected)?;
        SemanticActivationCandidateV1::new(
            transition.result_configuration.revision_id.clone(),
            binding,
            expected_current_receipt_digest,
        )
        .map_err(|_| SemanticActivationCoordinationErrorV1::Rejected)
    }

    fn next_authority_epochs(
        &self,
        privacy_epoch: u64,
    ) -> Result<[u64; 7], SemanticActivationCoordinationErrorV1> {
        let Some(authority) = self.authority.as_ref() else {
            return Err(SemanticActivationCoordinationErrorV1::Unavailable);
        };
        let Some(current) = authority.current_receipt() else {
            return Ok([1, 1, 1, 1, 1, 1, privacy_epoch]);
        };
        let next = |epoch: u64| {
            epoch
                .checked_add(1)
                .ok_or(SemanticActivationCoordinationErrorV1::Rejected)
        };
        Ok([
            next(current.binding.capability_epoch)?,
            next(current.binding.profile_epoch)?,
            next(current.binding.calibration_epoch)?,
            next(current.binding.resource_epoch)?,
            next(current.binding.runtime_epoch)?,
            next(current.binding.authorization_epoch)?,
            privacy_epoch,
        ])
    }
}

impl SemanticActivationCoordinationPort for ProductionSemanticActivationCoordinatorV1 {
    type ConfigurationState = ConfigurationCurrentStateV1;
    type AcceptedProfile = AcceptedRetrievalProfileV1;
    type RuntimeCompatibility = RetrievalRuntimeCompatibilityV1;
    type ConfigurationPin = SemanticConfigurationPinV1;
    type MutationCapability = RetrievalProfileMutationCapabilityV1;
    type ProfileCas = RetrievalProfileCasV1;
    type CentralMutation = DirectConfigurationMutation;
    type ActivationReceipt = SemanticActivationReceiptV1;
    type RollbackReceipt = SemanticRollbackReceiptV1;
    type ProfileState = RetrievalProfileStateSnapshotV1;
    type MutationAuthority = ConfigurationMutationAuthority;
    type PreviewOutcome = ConfigurationDirectCommitOutcomeV1;

    fn bootstrap_query_profile<'a>(
        &'a self,
        configuration: Self::ConfigurationState,
        accepted_query: Self::AcceptedProfile,
        runtime: &'a Self::RuntimeCompatibility,
    ) -> Pin<Box<dyn Future<Output = Result<(), SemanticActivationCoordinationErrorV1>> + Send + 'a>>
    {
        Box::pin(async move {
            ProductionSemanticActivationCoordinatorV1::bootstrap_query_profile(
                self,
                configuration,
                accepted_query,
                runtime,
            )
            .await
        })
    }

    fn current_profile_state<'a>(
        &'a self,
    ) -> Pin<
        Box<
            dyn Future<Output = Result<Self::ProfileState, SemanticActivationCoordinationErrorV1>>
                + Send
                + 'a,
        >,
    > {
        Box::pin(async move {
            ProductionSemanticActivationCoordinatorV1::current_profile_state(self).await
        })
    }

    fn preview_central_mutation<'a>(
        &'a self,
        authority: &'a Self::MutationAuthority,
        mutation: &'a Self::CentralMutation,
        expected_revision: &'a ConfigurationRevisionId,
    ) -> Pin<
        Box<
            dyn Future<Output = Result<Self::PreviewOutcome, SemanticActivationCoordinationErrorV1>>
                + Send
                + 'a,
        >,
    > {
        Box::pin(async move {
            ProductionSemanticActivationCoordinatorV1::preview_central_mutation(
                self,
                authority,
                mutation,
                expected_revision,
            )
            .await
        })
    }

    fn stage_and_activate<'a>(
        &'a self,
        base_configuration: Self::ConfigurationPin,
        result_configuration: Self::ConfigurationState,
        capability: &'a Self::MutationCapability,
        expected: Self::ProfileCas,
        candidate: Self::AcceptedProfile,
        current_runtime: &'a Self::RuntimeCompatibility,
        candidate_runtime: &'a Self::RuntimeCompatibility,
        central_mutation: Self::CentralMutation,
        freshness_vector_digest: ManifestDigest,
        now: UtcMicros,
    ) -> Pin<
        Box<
            dyn Future<
                    Output = Result<Self::ActivationReceipt, SemanticActivationCoordinationErrorV1>,
                > + Send
                + 'a,
        >,
    > {
        Box::pin(async move {
            ProductionSemanticActivationCoordinatorV1::stage_and_activate(
                self,
                base_configuration,
                result_configuration,
                capability,
                expected,
                candidate,
                current_runtime,
                candidate_runtime,
                central_mutation,
                freshness_vector_digest,
                now,
            )
            .await
        })
    }

    fn stage_and_rollback<'a>(
        &'a self,
        base_configuration: Self::ConfigurationPin,
        result_configuration: Self::ConfigurationState,
        capability: &'a Self::MutationCapability,
        expected: Self::ProfileCas,
        restored_runtime: &'a Self::RuntimeCompatibility,
        central_mutation: Self::CentralMutation,
        trigger: String,
        freshness_vector_digest: ManifestDigest,
        now: UtcMicros,
    ) -> Pin<
        Box<
            dyn Future<
                    Output = Result<Self::RollbackReceipt, SemanticActivationCoordinationErrorV1>,
                > + Send
                + 'a,
        >,
    > {
        Box::pin(async move {
            ProductionSemanticActivationCoordinatorV1::stage_and_rollback(
                self,
                base_configuration,
                result_configuration,
                capability,
                expected,
                restored_runtime,
                central_mutation,
                trigger,
                freshness_vector_digest,
                now,
            )
            .await
        })
    }
}

impl SemanticRuntimeIntegrationPortV1 for ProductionSemanticActivationCoordinatorV1 {
    fn status(&self) -> SemanticRuntimeFuture<'_, SemanticRuntimeStatusV1> {
        SemanticRuntimeIntegrationPortV1::status(self.owner.as_ref())
    }

    fn activate(
        &self,
        request: SemanticActivationRequestV1,
    ) -> SemanticRuntimeFuture<'_, Result<SemanticActivationReceiptV1, SemanticRuntimeControlErrorV1>>
    {
        SemanticRuntimeIntegrationPortV1::activate(self.owner.as_ref(), request)
    }

    fn rollback(
        &self,
        request: SemanticRollbackRequestV1,
    ) -> SemanticRuntimeFuture<'_, Result<SemanticRollbackReceiptV1, SemanticRuntimeControlErrorV1>>
    {
        SemanticRuntimeIntegrationPortV1::rollback(self.owner.as_ref(), request)
    }
}

/// Every stage of a linked transition answers a configuration refusal with the
/// same `Rejected` category, so the refusal has to carry the stage that
/// produced it or an operator cannot reach it from the public problem.
fn configuration_error_at(
    stage: &'static str,
) -> impl Fn(SemanticConfigurationBackendErrorV1) -> SemanticActivationCoordinationErrorV1 {
    move |error| {
        let mapped = match error {
            SemanticConfigurationBackendErrorV1::Unavailable => {
                SemanticActivationCoordinationErrorV1::Unavailable
            }
            SemanticConfigurationBackendErrorV1::Rejected => {
                SemanticActivationCoordinationErrorV1::RejectedDetail(format!(
                    "{stage}: retrieval configuration transition was rejected"
                ))
            }
            SemanticConfigurationBackendErrorV1::RejectedAt(inner) => {
                SemanticActivationCoordinationErrorV1::RejectedDetail(format!("{stage}: {inner}"))
            }
            SemanticConfigurationBackendErrorV1::Conflict => {
                SemanticActivationCoordinationErrorV1::Conflict
            }
        };
        crate::hotpath_observe::semantic_coordination_error(&mapped);
        mapped
    }
}

fn activation_request_from_transition(
    transition: &super::SemanticConfigurationTransitionV1,
) -> Result<SemanticActivationRequestV1, SemanticActivationCoordinationErrorV1> {
    let target = transition
        .result_active_semantic
        .as_ref()
        .ok_or(SemanticActivationCoordinationErrorV1::Rejected)?
        .vector_generation_id
        .clone();
    SemanticActivationRequestV1::new(
        target,
        transition
            .prior_active_semantic
            .as_ref()
            .map(|pins| pins.vector_generation_id.clone()),
        transition
            .prior_rollback_semantic
            .as_ref()
            .map(|pins| pins.vector_generation_id.clone()),
    )
    .map_err(|_| SemanticActivationCoordinationErrorV1::Rejected)
}

fn rollback_request_from_transition(
    transition: &super::SemanticConfigurationTransitionV1,
) -> Result<SemanticRollbackRequestV1, SemanticActivationCoordinationErrorV1> {
    let expected_active = transition
        .prior_active_semantic
        .as_ref()
        .ok_or(SemanticActivationCoordinationErrorV1::Rejected)?
        .vector_generation_id
        .clone();
    match transition.result_active_semantic.as_ref() {
        Some(target) => SemanticRollbackRequestV1::new(
            target.vector_generation_id.clone(),
            expected_active,
            transition
                .prior_rollback_semantic
                .as_ref()
                .ok_or(SemanticActivationCoordinationErrorV1::Rejected)?
                .vector_generation_id
                .clone(),
        ),
        None => SemanticRollbackRequestV1::disable(expected_active),
    }
    .map_err(|_| SemanticActivationCoordinationErrorV1::Rejected)
}

fn authority_error_at(
    error: super::SemanticActivationAuthorityErrorV1,
) -> SemanticActivationCoordinationErrorV1 {
    let mapped = match error {
        super::SemanticActivationAuthorityErrorV1::Conflict => {
            SemanticActivationCoordinationErrorV1::Conflict
        }
        super::SemanticActivationAuthorityErrorV1::Unavailable { .. }
        | super::SemanticActivationAuthorityErrorV1::Journal(_)
        | super::SemanticActivationAuthorityErrorV1::JournalRecovery { .. }
        | super::SemanticActivationAuthorityErrorV1::NoRollback => {
            SemanticActivationCoordinationErrorV1::Unavailable
        }
        super::SemanticActivationAuthorityErrorV1::InvalidCandidate(_)
        | super::SemanticActivationAuthorityErrorV1::InvalidState => {
            SemanticActivationCoordinationErrorV1::Rejected
        }
    };
    crate::hotpath_observe::semantic_coordination_error(&mapped);
    mapped
}

fn expected_authority_digest(
    current: Option<&SemanticActivationAuthorityReceiptV1>,
    transition: &super::SemanticConfigurationTransitionV1,
    require_current: bool,
) -> Result<Option<ManifestDigest>, SemanticActivationCoordinationErrorV1> {
    let Some(current) = current else {
        if require_current || transition.prior_active_semantic.is_some() {
            return Err(SemanticActivationCoordinationErrorV1::Unavailable);
        }
        return Ok(None);
    };
    if current.configuration_revision != transition.base_configuration.revision_id
        || current.binding.profile_digest != transition.prior_active_profile_digest
        || transition
            .prior_active_semantic
            .as_ref()
            .is_none_or(|semantic| {
                current.binding.vector_generation_digest
                    != *semantic.vector_generation_id.as_digest()
            })
    {
        return Err(SemanticActivationCoordinationErrorV1::Conflict);
    }
    Ok(Some(current.receipt_digest.clone()))
}
