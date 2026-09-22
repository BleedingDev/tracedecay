//! Semantic activation coordination port consumed by the configuration
//! control plane.
//!
//! This trait is only the methods the configuration runtime invokes on an
//! already-authorized semantic coordinator. It does not select profiles,
//! expose inventory stores, or mount a transport. Associated types keep
//! retrieval and store payloads in the crates that own them so this
//! ports-and-contracts crate does not take a `tracedecay-application` or
//! `tracedecay-search-eval` edge.

use std::future::Future;
use std::pin::Pin;

use serde::{Deserialize, Serialize};
use thiserror::Error;
use tracedecay_domain::configuration::ConfigurationRevisionId;
use tracedecay_domain::{CodeGenerationId, ManifestDigest, UtcMicros, canonical_sha256};

#[derive(Clone, Debug, Error, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "reason", rename_all = "snake_case")]
pub enum SemanticQualificationFailureV1 {
    #[error(
        "packaged PASS for profile {profile_id} is stale: packaged workload \
         {packaged_workload_digest}, current workload {current_workload_digest}, evidence \
         {evidence_digest}; {remedy}"
    )]
    StaleWorkload {
        profile_id: String,
        packaged_workload_digest: String,
        current_workload_digest: String,
        evidence_digest: String,
        remedy: String,
    },
    #[error(
        "native qualification evidence {evidence_digest} for profile {profile_id} and workload \
         {workload_digest} did not pass; {remedy}"
    )]
    FailedQualification {
        profile_id: String,
        workload_digest: String,
        evidence_digest: String,
        remedy: String,
    },
    /// The packaged bytes predate the current asset schema, so they cannot be
    /// read as evidence at all — distinct from evidence that was read and
    /// refused. The methodology version is unreadable here because it lives
    /// inside the shape that failed to decode.
    #[error(
        "qualification evidence for profile {profile_id} was written under packaged schema \
         {packaged_schema_version}, which this build superseded with schema \
         {current_schema_version}; {remedy}"
    )]
    SupersededSchema {
        profile_id: String,
        packaged_schema_version: u32,
        current_schema_version: u32,
        evidence_digest: Option<String>,
        remedy: String,
    },
    /// The packaged bytes decode, but they were scored under a decision rule
    /// this build no longer implements. Reinterpreting them under the current
    /// rule would assert a measurement nobody made.
    #[error(
        "qualification evidence for profile {profile_id} was scored under methodology \
         {packaged_methodology_version}, and this build decides under methodology \
         {current_methodology_version}; {remedy}"
    )]
    SupersededMethodology {
        profile_id: String,
        packaged_methodology_version: u32,
        current_methodology_version: u32,
        evidence_digest: Option<String>,
        remedy: String,
    },
    #[error(
        "no valid qualification evidence for profile {profile_id} and workload \
         {current_workload_digest}: {detail}; {remedy}"
    )]
    NoQualificationEvidence {
        profile_id: String,
        current_workload_digest: String,
        evidence_digest: Option<String>,
        detail: String,
        remedy: String,
    },
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "state", rename_all = "snake_case")]
pub enum SemanticQualificationStateV1 {
    Qualified {
        profile_id: String,
        workload_digest: String,
        evidence_digest: String,
    },
    Unqualified {
        failure: SemanticQualificationFailureV1,
    },
}

/// Typed failure for one configuration-linked semantic activation or rollback.
///
/// The `Runtime` payload is a display string so this crate does not name the
/// semantic-runtime control error. Implementors map that error at the
/// coordinator boundary.
#[derive(Clone, Debug, Error, PartialEq, Eq)]
pub enum SemanticActivationCoordinationErrorV1 {
    #[error("semantic activation configuration authority is unavailable")]
    Unavailable,
    #[error("semantic activation input was rejected")]
    Rejected,
    #[error("semantic activation input was rejected: {0}")]
    RejectedDetail(String),
    #[error("semantic activation qualification refused: {0}")]
    Qualification(Box<SemanticQualificationFailureV1>),
    #[error("semantic activation compare-and-swap conflicted")]
    Conflict,
    #[error("semantic runtime activation failed: {0}")]
    Runtime(String),
}

impl From<SemanticQualificationFailureV1> for SemanticActivationCoordinationErrorV1 {
    fn from(failure: SemanticQualificationFailureV1) -> Self {
        Self::Qualification(Box::new(failure))
    }
}

/// Coordination surface the configuration runtime actually calls.
///
/// Method list is the production call set from
/// `ProjectConfigurationRuntime` and the configuration operation that
/// reaches the installed coordinator through that runtime:
/// `bootstrap_query_profile`, `current_profile_state`,
/// `preview_central_mutation`, `stage_and_activate`, `stage_and_rollback`.
pub trait SemanticActivationCoordinationPort: Send + Sync {
    type ConfigurationState: Send + 'static;
    type AcceptedProfile: Send + 'static;
    type RuntimeCompatibility: Send + Sync + 'static;
    type ConfigurationPin: Send + 'static;
    type MutationCapability: Send + Sync + 'static;
    type ProfileCas: Send + 'static;
    type CentralMutation: Send + 'static;
    type ActivationReceipt: Send + 'static;
    type RollbackReceipt: Send + 'static;
    type ProfileState: Send + 'static;
    type MutationAuthority: Send + Sync + 'static;
    type PreviewOutcome: Send + 'static;

    fn bootstrap_query_profile<'a>(
        &'a self,
        configuration: Self::ConfigurationState,
        accepted_query: Self::AcceptedProfile,
        runtime: &'a Self::RuntimeCompatibility,
    ) -> Pin<Box<dyn Future<Output = Result<(), SemanticActivationCoordinationErrorV1>> + Send + 'a>>;

    fn current_profile_state<'a>(
        &'a self,
    ) -> Pin<
        Box<
            dyn Future<Output = Result<Self::ProfileState, SemanticActivationCoordinationErrorV1>>
                + Send
                + 'a,
        >,
    >;

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
    >;

    #[allow(clippy::too_many_arguments)]
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
    >;

    #[allow(clippy::too_many_arguments)]
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
    >;
}

/// The complete identity a semantic authority must verify before it can be
/// mounted.  The individual digests are deliberately opaque: this contract
/// records the identity of an already verified component and never acquires,
/// builds, or selects one.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct SemanticActivationBindingV1 {
    /// Digest of the exact model artifact and tokenizer package.
    pub model_artifact_digest: ManifestDigest,
    /// Code/source generation used to produce the projected vectors.
    pub source_generation: CodeGenerationId,
    /// Immutable vector generation served by the projection.
    pub vector_generation_digest: ManifestDigest,
    /// Admitted embedding projection identity.
    pub projection_key_digest: ManifestDigest,
    /// Search index schema/profile identity.
    pub search_index_key_digest: ManifestDigest,
    /// Code-index capability manifest identity.
    pub capability_digest: ManifestDigest,
    /// Evaluated retrieval profile identity.
    pub profile_digest: ManifestDigest,
    /// Calibration evidence/profile identity.
    pub calibration_digest: ManifestDigest,
    /// Resource measurement/ceiling identity.
    pub resource_digest: ManifestDigest,
    /// Runtime implementation/compatibility identity.
    pub runtime_digest: ManifestDigest,
    /// Monotone epochs from each authority participating in admission.
    pub capability_epoch: u64,
    pub profile_epoch: u64,
    pub calibration_epoch: u64,
    pub resource_epoch: u64,
    pub runtime_epoch: u64,
    pub authorization_epoch: u64,
    pub privacy_epoch: u64,
}

impl SemanticActivationBindingV1 {
    /// Validate the complete binding before it is journaled or served.
    pub fn validate(&self) -> Result<(), SemanticActivationContractErrorV1> {
        self.model_artifact_digest.validate().map_err(|_| {
            SemanticActivationContractErrorV1::InvalidBinding("model_artifact_digest")
        })?;
        self.source_generation
            .validate()
            .map_err(|_| SemanticActivationContractErrorV1::InvalidBinding("source_generation"))?;
        for (name, digest) in [
            ("vector_generation_digest", &self.vector_generation_digest),
            ("projection_key_digest", &self.projection_key_digest),
            ("search_index_key_digest", &self.search_index_key_digest),
            ("capability_digest", &self.capability_digest),
            ("profile_digest", &self.profile_digest),
            ("calibration_digest", &self.calibration_digest),
            ("resource_digest", &self.resource_digest),
            ("runtime_digest", &self.runtime_digest),
        ] {
            digest
                .validate()
                .map_err(|_| SemanticActivationContractErrorV1::InvalidBinding(name))?;
        }
        for (name, epoch) in [
            ("capability_epoch", self.capability_epoch),
            ("profile_epoch", self.profile_epoch),
            ("calibration_epoch", self.calibration_epoch),
            ("resource_epoch", self.resource_epoch),
            ("runtime_epoch", self.runtime_epoch),
            ("authorization_epoch", self.authorization_epoch),
            ("privacy_epoch", self.privacy_epoch),
        ] {
            if epoch == 0 {
                return Err(SemanticActivationContractErrorV1::ZeroEpoch(name));
            }
        }
        Ok(())
    }
}

/// Why a semantic authority cannot be mounted.  These reasons are safe to
/// expose at the product boundary and distinguish absence from stale or
/// damaged persisted state.
#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum SemanticActivationUnavailableReasonV1 {
    Missing,
    Stale,
    Corrupt,
    Incompatible,
}

/// The transition represented by an authority receipt.
#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum SemanticActivationOperationV1 {
    Activate,
    Rollback,
    Restart,
}

/// Durable proof that a complete semantic component set was admitted and
/// committed.  The digest covers the full binding and the compare-and-swap
/// lineage, so a receipt from another source, epoch, profile, or restart is
/// rejected as a different authority.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct SemanticActivationAuthorityReceiptV1 {
    pub operation: SemanticActivationOperationV1,
    pub configuration_revision: ConfigurationRevisionId,
    pub binding: SemanticActivationBindingV1,
    pub previous_receipt_digest: Option<ManifestDigest>,
    pub sequence: u64,
    pub committed_at: UtcMicros,
    pub receipt_digest: ManifestDigest,
}

impl SemanticActivationAuthorityReceiptV1 {
    pub fn issue(
        operation: SemanticActivationOperationV1,
        configuration_revision: ConfigurationRevisionId,
        binding: SemanticActivationBindingV1,
        previous_receipt_digest: Option<ManifestDigest>,
        sequence: u64,
        committed_at: UtcMicros,
    ) -> Result<Self, SemanticActivationContractErrorV1> {
        configuration_revision.validate().map_err(|_| {
            SemanticActivationContractErrorV1::InvalidReceipt("configuration_revision")
        })?;
        binding.validate()?;
        if let Some(digest) = &previous_receipt_digest {
            digest.validate().map_err(|_| {
                SemanticActivationContractErrorV1::InvalidReceipt("previous_receipt_digest")
            })?;
        }
        if sequence == 0 {
            return Err(SemanticActivationContractErrorV1::InvalidReceipt(
                "sequence",
            ));
        }
        let receipt_digest = authority_receipt_digest(
            operation,
            &configuration_revision,
            &binding,
            previous_receipt_digest.as_ref(),
            sequence,
            committed_at,
        )?;
        Ok(Self {
            operation,
            configuration_revision,
            binding,
            previous_receipt_digest,
            sequence,
            committed_at,
            receipt_digest,
        })
    }

    pub fn validate(&self) -> Result<(), SemanticActivationContractErrorV1> {
        self.configuration_revision.validate().map_err(|_| {
            SemanticActivationContractErrorV1::InvalidReceipt("configuration_revision")
        })?;
        self.binding.validate()?;
        if let Some(digest) = &self.previous_receipt_digest {
            digest.validate().map_err(|_| {
                SemanticActivationContractErrorV1::InvalidReceipt("previous_receipt_digest")
            })?;
        }
        if self.sequence == 0 {
            return Err(SemanticActivationContractErrorV1::InvalidReceipt(
                "sequence",
            ));
        }
        let expected = authority_receipt_digest(
            self.operation,
            &self.configuration_revision,
            &self.binding,
            self.previous_receipt_digest.as_ref(),
            self.sequence,
            self.committed_at,
        )?;
        if expected != self.receipt_digest {
            return Err(SemanticActivationContractErrorV1::ReceiptIdentityMismatch);
        }
        Ok(())
    }
}

/// Journal phase used to recover an atomic activation/rollback across process
/// restart.  A committed phase is the only phase that can become current.
#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum SemanticActivationJournalPhaseV1 {
    Prepared,
    Committed,
    RolledBack,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct SemanticActivationJournalEntryV1 {
    pub sequence: u64,
    pub phase: SemanticActivationJournalPhaseV1,
    pub receipt: SemanticActivationAuthorityReceiptV1,
}

impl SemanticActivationJournalEntryV1 {
    pub fn validate(&self) -> Result<(), SemanticActivationContractErrorV1> {
        if self.sequence == 0 || self.sequence != self.receipt.sequence {
            return Err(SemanticActivationContractErrorV1::InvalidJournal);
        }
        self.receipt.validate()?;
        if !matches!(self.phase, SemanticActivationJournalPhaseV1::Committed)
            && !matches!(
                self.receipt.operation,
                SemanticActivationOperationV1::Activate | SemanticActivationOperationV1::Rollback
            )
        {
            return Err(SemanticActivationContractErrorV1::InvalidJournal);
        }
        Ok(())
    }
}

/// Current serving state returned by an activation authority.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "state", rename_all = "snake_case")]
pub enum SemanticActivationAvailabilityV1 {
    Ready {
        receipt: SemanticActivationAuthorityReceiptV1,
    },
    Unavailable {
        reason: SemanticActivationUnavailableReasonV1,
    },
}

impl SemanticActivationAvailabilityV1 {
    pub fn validate(&self) -> Result<(), SemanticActivationContractErrorV1> {
        if let Self::Ready { receipt } = self {
            receipt.validate()?
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, Error, PartialEq, Eq)]
pub enum SemanticActivationContractErrorV1 {
    #[error("semantic activation binding is invalid: {0}")]
    InvalidBinding(&'static str),
    #[error("semantic activation binding epoch is zero: {0}")]
    ZeroEpoch(&'static str),
    #[error("semantic activation receipt is invalid: {0}")]
    InvalidReceipt(&'static str),
    #[error("semantic activation receipt identity mismatch")]
    ReceiptIdentityMismatch,
    #[error("semantic activation journal is invalid")]
    InvalidJournal,
}

/// Durable journal surface used by the semantic activation authority.
///
/// Implementations own persistence and transaction boundaries.  The
/// authority only appends validated entries and replays entries that are
/// already present; opening the authority never downloads or builds an
/// artifact.
#[derive(Clone, Copy, Debug, Error, PartialEq, Eq)]
pub enum SemanticActivationJournalErrorV1 {
    #[error("semantic activation journal is unavailable")]
    Unavailable,
    #[error("semantic activation journal is corrupt")]
    Corrupt,
    #[error("semantic activation journal compare-and-swap conflicted")]
    Conflict,
}

pub trait SemanticActivationJournalPortV1: Send + Sync {
    fn entries(
        &self,
    ) -> Result<Vec<SemanticActivationJournalEntryV1>, SemanticActivationJournalErrorV1>;

    /// Append one journal phase only when the caller still owns the expected
    /// committed sequence.  Implementations must perform the check and write
    /// as one atomic operation so separate coordinator instances cannot mint
    /// the same sequence or interleave a prepared pair.
    fn append_if_sequence(
        &self,
        expected_sequence: u64,
        entry: SemanticActivationJournalEntryV1,
    ) -> Result<(), SemanticActivationJournalErrorV1>;
}

fn authority_receipt_digest(
    operation: SemanticActivationOperationV1,
    configuration_revision: &ConfigurationRevisionId,
    binding: &SemanticActivationBindingV1,
    previous_receipt_digest: Option<&ManifestDigest>,
    sequence: u64,
    committed_at: UtcMicros,
) -> Result<ManifestDigest, SemanticActivationContractErrorV1> {
    canonical_sha256(&(
        "tracedecay.semantic-activation-authority-receipt.v1",
        operation,
        configuration_revision,
        binding,
        previous_receipt_digest,
        sequence,
        committed_at,
    ))
    .map_err(|_| SemanticActivationContractErrorV1::ReceiptIdentityMismatch)
}

#[cfg(test)]
mod authority_contract_tests {
    use super::*;

    fn digest(byte: char) -> ManifestDigest {
        ManifestDigest::new(format!("sha256:{}", byte.to_string().repeat(64))).unwrap()
    }

    fn binding() -> SemanticActivationBindingV1 {
        SemanticActivationBindingV1 {
            model_artifact_digest: digest('a'),
            source_generation: CodeGenerationId::new("source-generation.fixture").unwrap(),
            vector_generation_digest: digest('b'),
            projection_key_digest: digest('c'),
            search_index_key_digest: digest('d'),
            capability_digest: digest('e'),
            profile_digest: digest('f'),
            calibration_digest: digest('1'),
            resource_digest: digest('2'),
            runtime_digest: digest('3'),
            capability_epoch: 1,
            profile_epoch: 1,
            calibration_epoch: 1,
            resource_epoch: 1,
            runtime_epoch: 1,
            authorization_epoch: 1,
            privacy_epoch: 1,
        }
    }

    fn revision() -> ConfigurationRevisionId {
        ConfigurationRevisionId::try_from("configuration.revision.fixture".to_owned()).unwrap()
    }

    #[test]
    fn receipt_binds_every_component_and_epoch() {
        let receipt = SemanticActivationAuthorityReceiptV1::issue(
            SemanticActivationOperationV1::Activate,
            revision(),
            binding(),
            None,
            1,
            UtcMicros(1),
        )
        .unwrap();
        receipt.validate().unwrap();
        let mut stale = receipt.clone();
        stale.binding.privacy_epoch = 2;
        assert_eq!(
            stale.validate(),
            Err(SemanticActivationContractErrorV1::ReceiptIdentityMismatch)
        );
    }

    #[test]
    fn incomplete_binding_is_rejected_before_journal() {
        let mut value = binding();
        value.runtime_epoch = 0;
        assert_eq!(
            value.validate(),
            Err(SemanticActivationContractErrorV1::ZeroEpoch(
                "runtime_epoch"
            ))
        );
    }

    #[test]
    fn prepared_restart_entry_cannot_claim_restart_receipt() {
        let receipt = SemanticActivationAuthorityReceiptV1::issue(
            SemanticActivationOperationV1::Restart,
            revision(),
            binding(),
            None,
            1,
            UtcMicros(1),
        )
        .unwrap();
        let entry = SemanticActivationJournalEntryV1 {
            sequence: 1,
            phase: SemanticActivationJournalPhaseV1::Prepared,
            receipt,
        };
        assert_eq!(
            entry.validate(),
            Err(SemanticActivationContractErrorV1::InvalidJournal)
        );
    }
}
