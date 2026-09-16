//! Host-owned canonical input for the Native provider boundary.
//!
//! This seam deliberately contains no provider store, database path, schema
//! migration, provider generation, or provider receipt.  It joins the
//! host-owned exact destination scope to one canonical source observation and
//! the already-retained delivery/history evidence.  The selected provider can
//! therefore consume the same proof regardless of whether the destination is
//! Native or another provider.

use std::sync::Arc;

use tracedecay_domain::CanonicalObservationIdV1;
use tracedecay_global_db::GlobalDbObservationStore;
use tracedecay_memory_observation::{
    AdmittedObservationV1, DeliveryStateV1, ObservationDeliveryReceiptV1, ObservationJournalError,
    SourceDeliveryEvidenceV1,
};
use tracedecay_memory_provider_registry::{
    AdvisoryContractError, ApiError, HistoryGrant, OperationControl, OriginScopeEvidence,
    OriginalSourceIdentity, OwnedExactScope, OwnedProviderId, SourceAttribution, TerminalCode,
};
use tracedecay_session_runtime::session_retrieval::SessionApplicationRetrievalPortV1;
use tracedecay_store::{ObservationAdmissionPort, ObservationStoreError, StoredObservation};

use super::provider_history::{
    HistoryGrantRevalidationV1, OriginalObservationAuthorityV1, ProviderHistoryErrorV1,
    validate_history_record,
};

/// Host evidence needed to authorize one canonical source for a selected
/// provider operation.
///
/// All values in this input are host-owned or read through host-owned ports.
/// In particular, `delivery_evidence` is a retained journal projection and
/// `history_grant` is a host authorization claim that is revalidated before it
/// is used.  Neither value is accepted from a provider response.
pub(crate) struct NativeCanonicalAuthorityInputV1 {
    /// Full destination identity, including the requesting session.
    pub(crate) exact_scope: OwnedExactScope,
    /// Original canonical source identity, independent of the destination.
    pub(crate) source_identity: OriginalSourceIdentity,
    /// Destination provider identity used when revalidating the history grant.
    pub(crate) provider_id: OwnedProviderId,
    /// Existing registered canonical observation read authority.
    pub(crate) observations: Arc<GlobalDbObservationStore>,
    /// Retained host delivery evidence for this source.
    pub(crate) delivery_evidence: SourceDeliveryEvidenceV1,
    /// Host-issued original-source grant, including current disposition.
    pub(crate) history_grant: HistoryGrant,
    /// Host authority for live original-event and session authorization proof.
    pub(crate) original_authority: Arc<dyn OriginalObservationAuthorityV1>,
    /// Host authority that refreshes source and disposition history evidence.
    pub(crate) history_authority: Arc<dyn HistoryGrantRevalidationV1>,
    /// Canonical session retrieval port mounted for the destination.
    pub(crate) session_retrieval: Arc<dyn SessionApplicationRetrievalPortV1>,
}

/// Canonical proof returned after the input has been read and revalidated.
///
/// The proof carries immutable public observation and delivery values only.
/// Provider-local state remains behind the selected provider application port;
/// the session retrieval port is retained so callers cannot accidentally fall
/// back to a provider-owned session database.
pub(crate) struct NativeCanonicalAuthorityProofV1 {
    /// Canonical observation resolved by its host identity.
    pub(crate) observation: StoredObservation,
    /// Sanitized admission whose durable delivery was proven.
    pub(crate) admitted: Box<AdmittedObservationV1>,
    /// Host-retained receipt for the settled delivery attempt.
    pub(crate) delivery_receipt: ObservationDeliveryReceiptV1,
    /// Original source identity authorized by the host grant.
    pub(crate) source_identity: OriginalSourceIdentity,
    /// Canonical session retrieval port for destination reads.
    pub(crate) session_retrieval: Arc<dyn SessionApplicationRetrievalPortV1>,
}

/// Failure while assembling or revalidating a canonical Native authority
/// input.
#[derive(Debug, thiserror::Error)]
pub(crate) enum NativeCanonicalAuthorityErrorV1 {
    /// A public provider-boundary value failed validation.
    #[error("canonical Native authority boundary is invalid: {0}")]
    Api(#[source] ApiError),
    /// The source identity failed the common advisory contract.
    #[error("canonical Native source identity is invalid: {0}")]
    SourceIdentity(#[source] AdvisoryContractError),
    /// The host history claim failed common structural validation.
    #[error("canonical Native history grant is invalid: {0}")]
    HistoryGrant(#[source] AdvisoryContractError),
    /// The source identity cannot be looked up in the canonical store.
    #[error("canonical Native source observation identity is invalid")]
    InvalidObservationIdentity,
    /// The canonical observation row is absent from the registered store.
    #[error("canonical Native source observation is missing")]
    MissingObservation,
    /// The registered observation authority failed to read or validate a row.
    #[error("canonical Native observation authority failed: {0}")]
    ObservationStore(#[source] ObservationStoreError),
    /// A durable observation or settlement envelope failed journal validation.
    #[error("canonical Native admitted observation is invalid: {0}")]
    Observation(#[source] ObservationJournalError),
    /// The source row or delivery evidence disagreed with the host claim.
    #[error("canonical Native authority claim mismatch: {0}")]
    ClaimMismatch(&'static str),
    /// The host history or original-source authority rejected the proof.
    #[error("canonical Native history authority rejected the proof: {0}")]
    History(#[source] ProviderHistoryErrorV1),
    /// The operation was cancelled or exceeded its host budget.
    #[error("canonical Native authority operation stopped: {0:?}")]
    Control(TerminalCode),
}

type Result<T> = std::result::Result<T, NativeCanonicalAuthorityErrorV1>;

impl NativeCanonicalAuthorityInputV1 {
    /// Checks the host-owned shape before any authority or store read occurs.
    pub(crate) fn validate_shape(&self) -> Result<()> {
        validate_authority_shape(
            &self.exact_scope,
            &self.source_identity,
            &self.history_grant,
        )?;
        validate_delivery_shape(
            &self.delivery_evidence,
            &self.exact_scope,
            &self.provider_id,
            &self.source_identity,
            self.history_source()?.source_sequence,
        )?;
        Ok(())
    }

    /// Reads and revalidates the one canonical source represented by this
    /// input.  The call never opens a path or mints a provider-owned receipt.
    pub(crate) async fn validate(
        &self,
        control: &OperationControl,
    ) -> Result<NativeCanonicalAuthorityProofV1> {
        self.validate_shape()?;
        check_control(control)?;

        self.history_authority
            .revalidate_async(&self.provider_id, &self.history_grant, control)
            .await
            .map_err(NativeCanonicalAuthorityErrorV1::History)?;
        check_control(control)?;

        let observation_id =
            CanonicalObservationIdV1::new(self.source_identity.observation_id.clone())
                .map_err(|_| NativeCanonicalAuthorityErrorV1::InvalidObservationIdentity)?;
        let observation = self
            .observations
            .read_admitted_observation(&observation_id)
            .await
            .map_err(NativeCanonicalAuthorityErrorV1::ObservationStore)?
            .ok_or(NativeCanonicalAuthorityErrorV1::MissingObservation)?;
        check_control(control)?;

        observation
            .observation()
            .identity()
            .validate()
            .map_err(|_| NativeCanonicalAuthorityErrorV1::ClaimMismatch("source identity"))?;
        observation
            .observation()
            .scope()
            .validate()
            .map_err(|_| NativeCanonicalAuthorityErrorV1::ClaimMismatch("source scope"))?;
        observation
            .validated_repository_provenance_attachment()
            .map_err(NativeCanonicalAuthorityErrorV1::ObservationStore)?;

        let attribution = validate_history_record(&self.history_grant, &observation)
            .map_err(NativeCanonicalAuthorityErrorV1::History)?;
        if attribution.source != self.source_identity {
            return Err(NativeCanonicalAuthorityErrorV1::ClaimMismatch(
                "source identity differs from canonical observation",
            ));
        }

        let authority_ref = match &attribution.origin_scope {
            OriginScopeEvidence::Recorded { authority_ref, .. } => authority_ref,
            OriginScopeEvidence::IngestionOnly | OriginScopeEvidence::Unavailable => {
                return Err(NativeCanonicalAuthorityErrorV1::ClaimMismatch(
                    "original source scope proof unavailable",
                ));
            }
        };
        if !self
            .original_authority
            .validate_original_event(authority_ref, &observation)
            .map_err(NativeCanonicalAuthorityErrorV1::History)?
        {
            return Err(NativeCanonicalAuthorityErrorV1::ClaimMismatch(
                "original event proof",
            ));
        }
        if !self
            .original_authority
            .authorizes_session(
                &self.source_identity.canonical_session_id,
                &self.exact_scope,
            )
            .map_err(NativeCanonicalAuthorityErrorV1::History)?
        {
            return Err(NativeCanonicalAuthorityErrorV1::ClaimMismatch(
                "source session is not authorized for destination scope",
            ));
        }

        let (admitted, delivery_receipt) = validate_delivery_shape(
            &self.delivery_evidence,
            &self.exact_scope,
            &self.provider_id,
            &self.source_identity,
            attribution.source_sequence,
        )?;
        check_control(control)?;

        Ok(NativeCanonicalAuthorityProofV1 {
            observation,
            admitted,
            delivery_receipt,
            source_identity: self.source_identity.clone(),
            session_retrieval: Arc::clone(&self.session_retrieval),
        })
    }

    fn history_source(&self) -> Result<&SourceAttribution> {
        self.history_grant
            .sources
            .first()
            .map(|source| &source.attribution)
            .ok_or(NativeCanonicalAuthorityErrorV1::ClaimMismatch(
                "history source missing",
            ))
    }
}

/// Validates scope, source identity and the one-source history coverage that a
/// single canonical proof can actually represent.
fn validate_authority_shape(
    exact_scope: &OwnedExactScope,
    source_identity: &OriginalSourceIdentity,
    history_grant: &HistoryGrant,
) -> Result<()> {
    exact_scope
        .validate()
        .map_err(NativeCanonicalAuthorityErrorV1::Api)?;
    source_identity
        .validate()
        .map_err(NativeCanonicalAuthorityErrorV1::SourceIdentity)?;
    history_grant
        .validate_structure()
        .map_err(NativeCanonicalAuthorityErrorV1::HistoryGrant)?;
    if history_grant.destination_scope != *exact_scope {
        return Err(NativeCanonicalAuthorityErrorV1::ClaimMismatch(
            "history destination scope",
        ));
    }
    if history_grant.sources.len() != 1 {
        return Err(NativeCanonicalAuthorityErrorV1::ClaimMismatch(
            "single source proof required",
        ));
    }
    if history_grant.sources[0].attribution.source != *source_identity {
        return Err(NativeCanonicalAuthorityErrorV1::ClaimMismatch(
            "history source identity",
        ));
    }
    Ok(())
}

/// Revalidates the immutable sanitized admission and the host-retained
/// acknowledgement that proves the source reached the destination provider.
fn validate_delivery_shape(
    evidence: &SourceDeliveryEvidenceV1,
    exact_scope: &OwnedExactScope,
    provider_id: &OwnedProviderId,
    source_identity: &OriginalSourceIdentity,
    source_sequence: u64,
) -> Result<(Box<AdmittedObservationV1>, ObservationDeliveryReceiptV1)> {
    let SourceDeliveryEvidenceV1::Retained {
        admitted,
        state,
        receipt: Some(receipt),
    } = evidence
    else {
        return Err(match evidence {
            SourceDeliveryEvidenceV1::Missing => {
                NativeCanonicalAuthorityErrorV1::ClaimMismatch("delivery evidence missing")
            }
            SourceDeliveryEvidenceV1::Purged { .. } => {
                NativeCanonicalAuthorityErrorV1::ClaimMismatch("delivery content purged")
            }
            SourceDeliveryEvidenceV1::Retained { receipt: None, .. } => {
                NativeCanonicalAuthorityErrorV1::ClaimMismatch("settled delivery receipt missing")
            }
            SourceDeliveryEvidenceV1::Retained {
                receipt: Some(_), ..
            } => NativeCanonicalAuthorityErrorV1::ClaimMismatch("delivery evidence shape"),
        });
    };

    admitted
        .validate()
        .map_err(NativeCanonicalAuthorityErrorV1::Observation)?;
    admitted
        .source
        .validate()
        .map_err(NativeCanonicalAuthorityErrorV1::Observation)?;
    admitted
        .payload
        .validate()
        .map_err(NativeCanonicalAuthorityErrorV1::Api)?;
    admitted
        .sanitization
        .validate(&admitted.payload.sha256, &admitted.extensions_digest)
        .map_err(NativeCanonicalAuthorityErrorV1::Observation)?;
    receipt
        .validate()
        .map_err(NativeCanonicalAuthorityErrorV1::Observation)?;

    if admitted.target.provider_id != *provider_id {
        return Err(NativeCanonicalAuthorityErrorV1::ClaimMismatch(
            "delivery provider",
        ));
    }
    if admitted.exact_scope != *exact_scope {
        return Err(NativeCanonicalAuthorityErrorV1::ClaimMismatch(
            "delivery destination scope",
        ));
    }
    if admitted.source.source_event_id != source_identity.observation_id
        || admitted.source.source_sequence.0 != source_sequence
    {
        return Err(NativeCanonicalAuthorityErrorV1::ClaimMismatch(
            "settled source identity",
        ));
    }
    if receipt.observation_id != admitted.observation_id
        || receipt.idempotency_key != admitted.idempotency_key
        || receipt.provider_id != admitted.target.provider_id
        || receipt.provider_instance_id.as_deref()
            != Some(admitted.target.provider_instance_id.as_str())
        || receipt.registration_revision != admitted.target.registration_revision
        || receipt.payload_sha256 != admitted.payload.sha256
        || receipt.extensions_digest != admitted.extensions_digest
    {
        return Err(NativeCanonicalAuthorityErrorV1::ClaimMismatch(
            "delivery receipt binding",
        ));
    }
    if !matches!(
        state,
        DeliveryStateV1::Acknowledged | DeliveryStateV1::DuplicateAcknowledged
    ) {
        return Err(NativeCanonicalAuthorityErrorV1::ClaimMismatch(
            "delivery is not settled",
        ));
    }
    if receipt.implied_state() != *state
        || !matches!(
            (receipt.outcome, receipt.committed_effect),
            (
                tracedecay_memory_observation::ObservationOutcomeV1::Applied,
                tracedecay_memory_observation::ObservationCommittedEffectV1::Applied
            ) | (
                tracedecay_memory_observation::ObservationOutcomeV1::PartialEffect,
                tracedecay_memory_observation::ObservationCommittedEffectV1::Applied
            ) | (
                tracedecay_memory_observation::ObservationOutcomeV1::DuplicateAcknowledged,
                tracedecay_memory_observation::ObservationCommittedEffectV1::Duplicate
            )
        )
    {
        return Err(NativeCanonicalAuthorityErrorV1::ClaimMismatch(
            "delivery acknowledgement effect",
        ));
    }
    Ok((Box::new((**admitted).clone()), receipt.clone()))
}

fn check_control(control: &OperationControl) -> Result<()> {
    control
        .snapshot()
        .map(|_| ())
        .map_err(NativeCanonicalAuthorityErrorV1::Control)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tracedecay_memory_provider_registry::{
        CurrentSourceDisposition, HistoryRelation, RestoreDispositionCheckpoint, SourceDisposition,
    };

    fn scope(suffix: &str) -> OwnedExactScope {
        OwnedExactScope::new(
            "profile.fixture",
            "project.fixture",
            "repository.fixture",
            "worktree.fixture",
            "refs/heads/fixture",
            format!("session.{suffix}"),
            format!("sha256:{}", "a".repeat(64)),
        )
        .expect("valid exact scope")
    }

    fn source(suffix: &str) -> OriginalSourceIdentity {
        OriginalSourceIdentity {
            canonical_provider_id: OwnedProviderId::new("claude").expect("source provider"),
            canonical_session_id: format!("source-session.{suffix}"),
            source_key: format!("source-key.{suffix}"),
            stable_record_id: None,
            observation_id: format!("source-observation.{suffix}"),
            source_revision: Some("revision.1".to_owned()),
            content_sha256: "b".repeat(64),
        }
    }

    fn grant(scope: OwnedExactScope, source: OriginalSourceIdentity) -> HistoryGrant {
        HistoryGrant {
            authorization_ref: "host-history.fixture".to_owned(),
            policy_revision: 1,
            destination_scope: scope.clone(),
            relation: HistoryRelation::ExactScope,
            sources: vec![tracedecay_memory_provider_registry::GrantedHistorySource {
                attribution: SourceAttribution {
                    source,
                    origin_scope: OriginScopeEvidence::Recorded {
                        scope: scope.clone(),
                        authority_ref: "host-origin.fixture".to_owned(),
                    },
                    source_sequence: 1,
                    occurred_at_utc_nanos: None,
                    ingested_at_utc_nanos: 1,
                    validity: Default::default(),
                },
                current_disposition: CurrentSourceDisposition {
                    state: SourceDisposition::Available,
                    authority_ref: "host-disposition.fixture".to_owned(),
                    authority_revision: Some(1),
                    checked_at_utc_nanos: 1,
                },
            }],
            disposition_checkpoint: RestoreDispositionCheckpoint {
                exact_scope: scope,
                authority_ref: "host-disposition-checkpoint.fixture".to_owned(),
                authority_revision: Some(1),
                checked_at_utc_nanos: 1,
            },
        }
    }

    #[test]
    fn shape_rejects_destination_scope_drift() {
        let expected_scope = scope("destination");
        let source = source("one");
        let history = grant(scope("other"), source.clone());
        let error = validate_authority_shape(&expected_scope, &source, &history).unwrap_err();
        assert!(matches!(
            error,
            NativeCanonicalAuthorityErrorV1::ClaimMismatch("history destination scope")
        ));
    }

    #[test]
    fn shape_rejects_source_identity_drift() {
        let expected_scope = scope("destination");
        let history = grant(expected_scope.clone(), source("one"));
        let error =
            validate_authority_shape(&expected_scope, &source("two"), &history).unwrap_err();
        assert!(matches!(
            error,
            NativeCanonicalAuthorityErrorV1::ClaimMismatch("history source identity")
        ));
    }

    #[test]
    fn delivery_evidence_rejects_missing_or_purged_content() {
        let expected_scope = scope("destination");
        let expected_source = source("one");
        let provider = OwnedProviderId::new("tracedecay.native").expect("provider");
        for evidence in [
            SourceDeliveryEvidenceV1::Missing,
            SourceDeliveryEvidenceV1::Purged {
                state: DeliveryStateV1::Forgotten,
            },
        ] {
            let error =
                validate_delivery_shape(&evidence, &expected_scope, &provider, &expected_source, 1)
                    .unwrap_err();
            assert!(matches!(
                error,
                NativeCanonicalAuthorityErrorV1::ClaimMismatch(
                    "delivery evidence missing" | "delivery content purged"
                )
            ));
        }
    }
}
