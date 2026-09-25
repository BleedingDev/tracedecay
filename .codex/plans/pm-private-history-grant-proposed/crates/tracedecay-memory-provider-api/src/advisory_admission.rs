//! Ephemeral results from a host authority installed during provider composition.
//! These values bind a checked call; their construction does not grant authority.

use std::collections::{BTreeMap, BTreeSet};
use std::error::Error;
use std::fmt;
use std::sync::Arc;

use sha2::{Digest, Sha256};

use crate::contract::{SourceDisposition, TerminalCode};
use crate::{
    AdvisoryContractError, ApiError, CurrentSourceDisposition, GrantedHistorySource, HistoryGrant,
    MAX_COMMITTED_EFFECT_ITEM_REFS, OperationControl, OriginScopeEvidence, OriginalSourceIdentity,
    OwnedExactScope, ProviderCall, ProviderOperation, RecordedValidity,
    RestoreDispositionCheckpoint, SourceAttribution, lowercase_sha256_hex,
    opaque_extensions_digest,
};

/// Maximum distinct source inventory in one current admission result.
pub const MAX_ADVISORY_ADMISSION_SOURCES: usize = MAX_COMMITTED_EFFECT_ITEM_REFS;

/// A host authority refusal or invalid result at the advisory admission boundary.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum AdvisoryAdmissionError {
    /// The provider call violates an existing envelope or payload invariant.
    Boundary(ApiError),
    /// A returned source, scope, or disposition violates the common contract.
    Contract(AdvisoryContractError),
    /// The result describes a different call from the one being dispatched.
    BindingMismatch,
    /// A returned collection or operation combination is inconsistent.
    Invalid(&'static str),
    /// The live call was cancelled or its deadline expired.
    Control(TerminalCode),
    /// An existing host authority could not be read.
    Unavailable(&'static str),
    /// The installed host authority refused the requested source access.
    Denied(&'static str),
}

impl fmt::Display for AdvisoryAdmissionError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Boundary(error) => error.fmt(formatter),
            Self::Contract(error) => error.fmt(formatter),
            Self::BindingMismatch => formatter.write_str("advisory admission call binding differs"),
            Self::Invalid(field) => write!(formatter, "invalid advisory admission {field}"),
            Self::Control(code) => {
                write!(formatter, "advisory admission control: {}", code.as_wire())
            }
            Self::Unavailable(authority) => {
                write!(formatter, "advisory authority unavailable: {authority}")
            }
            Self::Denied(reason) => write!(formatter, "advisory admission denied: {reason}"),
        }
    }
}

impl Error for AdvisoryAdmissionError {}

impl From<ApiError> for AdvisoryAdmissionError {
    fn from(value: ApiError) -> Self {
        Self::Boundary(value)
    }
}

impl From<AdvisoryContractError> for AdvisoryAdmissionError {
    fn from(value: AdvisoryContractError) -> Self {
        Self::Contract(value)
    }
}

/// Existing host authority supplied by provider composition, never by call JSON.
///
/// Implementations resolve original event receipts, canonical source identity,
/// session policy, repository markers and current canonical/provider privacy
/// disposition using the host's existing ports immediately before use. Payload
/// grants and snapshot inventories are claims to check against those ports.
/// A digest, matching label, or constructed admission value is insufficient.
///
/// Providers trust only the fresh result of the authority installed in their
/// composition, then run [`CurrentAdvisoryAdmission::verify_for`] before mutation.
/// They must check exact source/inventory coverage and enforce each returned
/// disposition. Results must not be cached, persisted, or accepted from callers.
pub trait AdvisoryAdmissionAuthority: Send + Sync {
    /// Revalidates this exact call against current host authority and live control.
    fn admit(
        &self,
        call: &ProviderCall,
    ) -> Result<CurrentAdvisoryAdmission, AdvisoryAdmissionError>;
}

/// Framed digest binding an ephemeral authority result to one exact provider call.
///
/// Constructing this value proves only binding, never source authorization.
#[derive(Clone, Debug)]
pub struct AdvisoryCallBinding {
    sha256: String,
    control: OperationControl,
}

impl PartialEq for AdvisoryCallBinding {
    fn eq(&self, other: &Self) -> bool {
        self.sha256 == other.sha256
            && self.control.budget_started_at == other.control.budget_started_at
            && Arc::ptr_eq(&self.control.cancellation.0, &other.control.cancellation.0)
    }
}

impl Eq for AdvisoryCallBinding {}

impl AdvisoryCallBinding {
    /// Validates the call and binds its identities, operation, scope, readiness,
    /// generation, idempotency, canonical payload, extensions, capabilities and
    /// original finite control budget. The payload digest covers any raw grant.
    /// A separate typed digest covers the optional in-process history claim.
    /// Retained runtime control identity also prevents replacement of the live
    /// cancellation token or restarting an otherwise identical monotonic budget.
    pub fn from_call(call: &ProviderCall) -> Result<Self, AdvisoryAdmissionError> {
        call.control
            .snapshot()
            .map_err(AdvisoryAdmissionError::Control)?;
        call.validate()?;
        let extensions = opaque_extensions_digest(&call.extensions)?;
        let scope = call.exact_scope.exact_scope_sha256();
        let mut digest = Sha256::new();
        digest.update(b"tracedecay.memory-provider.advisory-call-binding.v1\0");
        for value in [
            call.provider_id.as_str(),
            call.operation.as_wire(),
            call.ready_receipt_sha256.as_str(),
            call.request_id.as_str(),
            call.operation_id.as_str(),
            call.payload.contract_id.as_str(),
            call.payload.sha256.as_str(),
            extensions.as_str(),
            scope.as_str(),
        ] {
            bind_field(&mut digest, value.as_bytes());
        }
        for value in [
            call.registration_revision,
            call.expected_state_generation,
            call.control.remaining_millis(),
        ] {
            bind_field(&mut digest, &value.to_be_bytes());
        }
        bind_field(
            &mut digest,
            &call.control.deadline_utc_micros().to_be_bytes(),
        );
        bind_field(&mut digest, &[u8::from(call.idempotency_key.is_some())]);
        if let Some(key) = &call.idempotency_key {
            bind_field(&mut digest, key.as_bytes());
        }
        bind_field(
            &mut digest,
            &u64::try_from(call.required_capabilities.len())
                .unwrap_or(u64::MAX)
                .to_be_bytes(),
        );
        for capability in &call.required_capabilities {
            bind_field(&mut digest, capability.as_str().as_bytes());
        }
        if let Some(grant) = call.history_grant() {
            bind_field(&mut digest, b"in-process-history-grant.v1");
            bind_field(&mut digest, &history_grant_sha256(grant));
        }
        call.control
            .snapshot()
            .map_err(AdvisoryAdmissionError::Control)?;
        Ok(Self {
            sha256: lowercase_sha256_hex(digest.finalize().into()),
            control: call.control.clone(),
        })
    }

    /// Rechecks the live call and refuses any change to its bound values.
    pub fn verify_for(&self, call: &ProviderCall) -> Result<(), AdvisoryAdmissionError> {
        self.control
            .snapshot()
            .map_err(AdvisoryAdmissionError::Control)?;
        if self != &Self::from_call(call)? {
            return Err(AdvisoryAdmissionError::BindingMismatch);
        }
        Ok(())
    }

    /// Returns the non-authorizing digest for runtime comparison and diagnostics.
    #[must_use]
    pub fn sha256(&self) -> &str {
        &self.sha256
    }
}

fn bind_field(digest: &mut Sha256, value: &[u8]) {
    digest.update(u64::try_from(value.len()).unwrap_or(u64::MAX).to_be_bytes());
    digest.update(value);
}

// Canonical typed encoding for an untrusted claim: every field is length framed,
// optional values and origin variants carry explicit tags, and source order is
// preserved. Exhaustive destructuring makes new fields require a binding update.
// This fingerprint performs no authority check and is never a wire contract.
fn history_grant_sha256(grant: &HistoryGrant) -> [u8; 32] {
    let HistoryGrant {
        authorization_ref,
        policy_revision,
        destination_scope,
        relation,
        sources,
        disposition_checkpoint,
    } = grant;
    let mut digest = Sha256::new();
    digest.update(b"tracedecay.memory-provider.history-grant-claim.v1\0");
    bind_field(&mut digest, authorization_ref.as_bytes());
    bind_field(&mut digest, &policy_revision.to_be_bytes());
    bind_field(
        &mut digest,
        destination_scope.exact_scope_sha256().as_bytes(),
    );
    bind_field(&mut digest, relation.as_wire().as_bytes());
    bind_field(
        &mut digest,
        &u64::try_from(sources.len())
            .unwrap_or(u64::MAX)
            .to_be_bytes(),
    );
    for GrantedHistorySource {
        attribution,
        current_disposition,
    } in sources
    {
        bind_source_attribution(&mut digest, attribution);
        let CurrentSourceDisposition {
            state,
            authority_ref,
            authority_revision,
            checked_at_utc_nanos,
        } = current_disposition;
        bind_field(&mut digest, state.as_wire().as_bytes());
        bind_field(&mut digest, authority_ref.as_bytes());
        bind_optional_field(&mut digest, authority_revision.map(u64::to_be_bytes));
        bind_field(&mut digest, &checked_at_utc_nanos.to_be_bytes());
    }
    let RestoreDispositionCheckpoint {
        exact_scope,
        authority_ref,
        authority_revision,
        checked_at_utc_nanos,
    } = disposition_checkpoint;
    bind_field(&mut digest, exact_scope.exact_scope_sha256().as_bytes());
    bind_field(&mut digest, authority_ref.as_bytes());
    bind_optional_field(&mut digest, authority_revision.map(u64::to_be_bytes));
    bind_field(&mut digest, &checked_at_utc_nanos.to_be_bytes());
    digest.finalize().into()
}

fn bind_source_attribution(digest: &mut Sha256, attribution: &SourceAttribution) {
    let SourceAttribution {
        source,
        origin_scope,
        source_sequence,
        occurred_at_utc_nanos,
        ingested_at_utc_nanos,
        validity,
    } = attribution;
    let OriginalSourceIdentity {
        canonical_provider_id,
        canonical_session_id,
        source_key,
        stable_record_id,
        observation_id,
        source_revision,
        content_sha256,
    } = source;
    bind_field(digest, canonical_provider_id.as_str().as_bytes());
    bind_field(digest, canonical_session_id.as_bytes());
    bind_field(digest, source_key.as_bytes());
    bind_optional_field(digest, stable_record_id.as_deref().map(str::as_bytes));
    bind_field(digest, observation_id.as_bytes());
    bind_optional_field(digest, source_revision.as_deref().map(str::as_bytes));
    bind_field(digest, content_sha256.as_bytes());
    match origin_scope {
        OriginScopeEvidence::Recorded {
            scope,
            authority_ref,
        } => {
            bind_field(digest, b"recorded");
            bind_field(digest, scope.exact_scope_sha256().as_bytes());
            bind_field(digest, authority_ref.as_bytes());
        }
        OriginScopeEvidence::IngestionOnly => bind_field(digest, b"ingestion_only"),
        OriginScopeEvidence::Unavailable => bind_field(digest, b"unavailable"),
    }
    bind_field(digest, &source_sequence.to_be_bytes());
    bind_optional_field(digest, occurred_at_utc_nanos.map(i64::to_be_bytes));
    bind_field(digest, &ingested_at_utc_nanos.to_be_bytes());
    let RecordedValidity {
        valid_from_utc_nanos,
        valid_until_utc_nanos,
        superseded_at_utc_nanos,
        superseded_by,
        revoked_at_utc_nanos,
    } = validity;
    bind_optional_field(digest, valid_from_utc_nanos.map(i64::to_be_bytes));
    bind_optional_field(digest, valid_until_utc_nanos.map(i64::to_be_bytes));
    bind_optional_field(digest, superseded_at_utc_nanos.map(i64::to_be_bytes));
    bind_optional_field(digest, superseded_by.as_deref().map(str::as_bytes));
    bind_optional_field(digest, revoked_at_utc_nanos.map(i64::to_be_bytes));
}

fn bind_optional_field<T: AsRef<[u8]>>(digest: &mut Sha256, value: Option<T>) {
    bind_field(digest, &[u8::from(value.is_some())]);
    if let Some(value) = value {
        bind_field(digest, value.as_ref());
    }
}

/// Current per-source restore decisions read independently of snapshot creation.
/// Explicit deleted/redacted/expired entries remain present so a provider can
/// fence their restored effects. Missing or unknown disposition cannot make a
/// restored namespace ready. Provider generations do not order this checkpoint.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CurrentRestoreAdmission {
    /// Existing host checkpoint from the current authority read.
    pub checkpoint: RestoreDispositionCheckpoint,
    /// Complete source inventory paired with fresh effective dispositions.
    pub sources: Vec<(OriginalSourceIdentity, CurrentSourceDisposition)>,
}

impl CurrentRestoreAdmission {
    /// Checks structure without promoting the supplied values to host authority.
    pub fn new(
        checkpoint: RestoreDispositionCheckpoint,
        sources: Vec<(OriginalSourceIdentity, CurrentSourceDisposition)>,
    ) -> Result<Self, AdvisoryAdmissionError> {
        let admission = Self {
            checkpoint,
            sources,
        };
        admission.validate_for(&admission.checkpoint.exact_scope)?;
        Ok(admission)
    }

    /// Checks the destination, bounded unique inventory and known dispositions.
    /// The installed authority remains responsible for current host evidence.
    pub fn validate_for(
        &self,
        destination: &OwnedExactScope,
    ) -> Result<(), AdvisoryAdmissionError> {
        self.checkpoint.validate_for(destination)?;
        if self.sources.len() > MAX_ADVISORY_ADMISSION_SOURCES {
            return Err(AdvisoryAdmissionError::Invalid("restore source bound"));
        }
        let mut seen = BTreeSet::new();
        for (source, disposition) in &self.sources {
            source.validate()?;
            disposition.validate()?;
            if disposition.state == SourceDisposition::Unknown {
                return Err(AdvisoryAdmissionError::Invalid(
                    "unknown restore disposition",
                ));
            }
            if !seen.insert(source_key(source)) {
                return Err(AdvisoryAdmissionError::Invalid("duplicate restore source"));
            }
        }
        Ok(())
    }

    /// Requires exact, duplicate-free coverage of the inventory actually decoded
    /// from snapshot bytes. Wire-declared inventory alone is insufficient.
    pub fn verify_inventory(
        &self,
        actual: &[OriginalSourceIdentity],
    ) -> Result<(), AdvisoryAdmissionError> {
        self.validate_for(&self.checkpoint.exact_scope)?;
        if actual.len() != self.sources.len() {
            return Err(AdvisoryAdmissionError::Invalid(
                "restore inventory coverage",
            ));
        }
        let admitted: BTreeMap<_, _> = self
            .sources
            .iter()
            .map(|(source, _)| (source_key(source), source))
            .collect();
        let mut seen = BTreeSet::new();
        for source in actual {
            source.validate()?;
            if !seen.insert(source_key(source)) {
                return Err(AdvisoryAdmissionError::Invalid("duplicate snapshot source"));
            }
            if admitted.get(&source_key(source)).copied() != Some(source) {
                return Err(AdvisoryAdmissionError::Invalid(
                    "restore inventory coverage",
                ));
            }
        }
        Ok(())
    }
}

/// Ephemeral checked source inputs returned only by an installed host authority.
///
/// Attribution is the immutable snapshot carried by the admitted payload. Fresh
/// canonical supersession/revocation/privacy status belongs in the separate
/// current disposition; it must not rewrite attribution or require equality of
/// the frozen validity snapshot with today's canonical lifecycle metadata.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CurrentAdvisoryAdmission {
    /// Binding to the exact call for which the installed authority ran.
    pub binding: AdvisoryCallBinding,
    /// Frozen source attribution and separately refreshed source dispositions.
    /// For restore, this optional subset must exactly match identities and
    /// current dispositions in the complete restore inventory.
    pub history_sources: Vec<GrantedHistorySource>,
    /// Complete current restore inventory, required for snapshot restoration.
    pub restore: Option<CurrentRestoreAdmission>,
}

impl CurrentAdvisoryAdmission {
    /// Constructs a structurally checked result for an authority implementation.
    /// Calling this constructor is not evidence that any host authority ran.
    pub fn new(
        call: &ProviderCall,
        history_sources: Vec<GrantedHistorySource>,
        restore: Option<CurrentRestoreAdmission>,
    ) -> Result<Self, AdvisoryAdmissionError> {
        let admission = Self {
            binding: AdvisoryCallBinding::from_call(call)?,
            history_sources,
            restore,
        };
        admission.verify_for(call)?;
        Ok(admission)
    }

    /// Checks live control, exact call binding and returned structure. Consumers
    /// must additionally match requested source coverage and enforce dispositions.
    pub fn verify_for(&self, call: &ProviderCall) -> Result<(), AdvisoryAdmissionError> {
        self.binding.verify_for(call)?;
        if self.history_sources.len() > MAX_ADVISORY_ADMISSION_SOURCES {
            return Err(AdvisoryAdmissionError::Invalid("history source bound"));
        }
        if self.restore.is_some() != (call.operation == ProviderOperation::SnapshotRestore) {
            return Err(AdvisoryAdmissionError::Invalid(
                "restore operation evidence",
            ));
        }
        let restore_sources = if let Some(restore) = &self.restore {
            restore.validate_for(&call.exact_scope)?;
            Some(
                restore
                    .sources
                    .iter()
                    .map(|(source, disposition)| (source_key(source), (source, disposition)))
                    .collect::<BTreeMap<_, _>>(),
            )
        } else {
            None
        };
        let mut seen = BTreeSet::new();
        for source in &self.history_sources {
            source.attribution.validate()?;
            source.attribution.origin_scope.recorded_scope()?;
            source.current_disposition.validate()?;
            if !seen.insert(source_key(&source.attribution.source)) {
                return Err(AdvisoryAdmissionError::Invalid(
                    "duplicate admitted history source",
                ));
            }
            if let Some(restore_sources) = &restore_sources {
                let matches = restore_sources
                    .get(&source_key(&source.attribution.source))
                    .is_some_and(|(original, disposition)| {
                        *original == &source.attribution.source
                            && *disposition == &source.current_disposition
                    });
                if !matches {
                    return Err(AdvisoryAdmissionError::Invalid(
                        "restore history source binding",
                    ));
                }
            }
        }
        call.control
            .snapshot()
            .map_err(AdvisoryAdmissionError::Control)?;
        Ok(())
    }
}

fn source_key(source: &OriginalSourceIdentity) -> (&str, &str, &str, &str, Option<&str>) {
    (
        source.canonical_provider_id.as_str(),
        source.canonical_session_id.as_str(),
        source.source_key.as_str(),
        source.observation_id.as_str(),
        source.source_revision.as_deref(),
    )
}
