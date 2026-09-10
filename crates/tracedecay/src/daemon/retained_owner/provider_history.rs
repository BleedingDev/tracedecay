//! Host-authorized history from the existing canonical observation, original
//! event, repository marker, disposition and provider journal authorities.
//! Values on the provider wire are claims; every use reads these authorities.

use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use sha2::{Digest, Sha256};
use tracedecay_contracts::ResolvedScope;
use tracedecay_domain::{
    BrainId, CanonicalObservationEnvelopeV1, CanonicalObservationIdV1, EvidenceAvailabilityV1,
    FactOwnerV1, ObservationScopeV1, ProjectId, RepositoryId, UserProfileId, WorktreeId,
};
use tracedecay_memory_observation::SqliteObservationJournal;
use tracedecay_memory_provider_registry::{
    CurrentSourceDisposition, GrantedHistorySource, HistoryGrant, OriginScopeEvidence,
    OriginalSourceIdentity, OwnedExactScope, OwnedProviderId, RecordedValidity,
    RestoreDispositionCheckpoint, SourceAttribution,
};
use tracedecay_memory_provider_registry::{HistoryRelation, SourceDisposition};
use tracedecay_sessions::repository_provenance::RepositoryProvenanceAdmissionContext;
use tracedecay_store::observation::ObservationOriginV1;
use tracedecay_store::{
    AnchorDispositionStateV1, ObservationAdmissionPort, ObservationRecentWindowRequest,
    ObservationReplayRequest, RetrievalAnchorDispositionStore, RetrievalAnchorOwnerV1,
    StoreShardIdV1, StoreShardScopeV1, StoredObservation,
};

use super::observation_journey::exact_scope_for_session;
use tracedecay_memory_provider_registry::recall_admission::{
    RecallOutcomeScopeV1, source_attribution::RecallSourceAttributionV1,
};

const MAX_HISTORY_PAGE: usize = 256;

/// One composition-time binding from a provider built during core admission to
/// its actual host authority once the existing project journal is mounted.
/// Decisions are never retained; every call reaches the installed authority.
#[derive(Default)]
pub(crate) struct ProviderHistoryAuthorityMountV1 {
    authority: std::sync::OnceLock<
        Arc<dyn tracedecay_memory_provider_registry::AdvisoryAdmissionAuthority>,
    >,
}

impl ProviderHistoryAuthorityMountV1 {
    pub(crate) fn bind(
        &self,
        authority: Arc<dyn tracedecay_memory_provider_registry::AdvisoryAdmissionAuthority>,
    ) -> HistoryResult<()> {
        self.authority.set(authority).map_err(|_| {
            ProviderHistoryErrorV1::ClaimMismatch("provider history authority already mounted")
        })
    }
}

impl tracedecay_memory_provider_registry::AdvisoryAdmissionAuthority
    for ProviderHistoryAuthorityMountV1
{
    fn admit(
        &self,
        call: &tracedecay_memory_provider_registry::ProviderCall,
    ) -> Result<
        tracedecay_memory_provider_registry::CurrentAdvisoryAdmission,
        tracedecay_memory_provider_registry::AdvisoryAdmissionError,
    > {
        self.authority
            .get()
            .ok_or(
                tracedecay_memory_provider_registry::AdvisoryAdmissionError::Unavailable(
                    "provider history authority not mounted",
                ),
            )?
            .admit(call)
    }
}

/// Bounded reader of the existing host admission ledger. This object can be
/// shared by live capture and background ingestion; neither path consults the
/// mutable checkout to invent an original event.
pub(crate) struct HookOriginReaderV1 {
    data_root: PathBuf,
    brain_id: BrainId,
    profile_id: UserProfileId,
}

impl HookOriginReaderV1 {
    pub(crate) fn new(data_root: PathBuf, brain_id: BrainId, profile_id: UserProfileId) -> Self {
        Self {
            data_root,
            brain_id,
            profile_id,
        }
    }

    fn host(provider: &str) -> Option<tracedecay_hooks::HookHostV1> {
        match provider {
            "claude" => Some(tracedecay_hooks::HookHostV1::ClaudeCode),
            "codex" => Some(tracedecay_hooks::HookHostV1::Codex),
            _ => None,
        }
    }

    fn ledger_root(&self, host: tracedecay_hooks::HookHostV1) -> PathBuf {
        self.data_root
            .join("hook-v2-admissions")
            .join(host.hook_key())
    }

    fn proofs(
        &self,
        host: tracedecay_hooks::HookHostV1,
    ) -> HistoryResult<Vec<tracedecay_hooks::admission_ledger::HookLiveOriginProofV1>> {
        tracedecay_hooks::admission_ledger::read_hook_live_origin_proofs(
            &self.ledger_root(host),
            host,
            tracedecay_contracts::now_micros(),
        )
        .map_err(|_| ProviderHistoryErrorV1::Unavailable("original live-event ledger"))
    }

    fn boundary_matches_profile(
        &self,
        boundary: &tracedecay_hooks::admission_ledger::HookLiveOriginBoundaryV1,
    ) -> bool {
        boundary.observation.scope.brain_id == self.brain_id
            && boundary.observation.scope.profile_id == self.profile_id
    }

    fn resolve_source(
        &self,
        identity: &tracedecay_domain::ObservationIdentityMaterialV1,
        resume_checkpoint: Option<(u64, u64)>,
        authority_ref: Option<&str>,
    ) -> HistoryResult<
        Option<tracedecay_sessions::repository_provenance::OriginalObservationEvidenceV1>,
    > {
        identity
            .validate()
            .map_err(|_| ProviderHistoryErrorV1::Ineligible("original source identity"))?;
        let Some(host) = Self::host(identity.source().provider().as_str()) else {
            return Ok(None);
        };
        let Some((file_identity, fingerprint)) = resume_checkpoint else {
            return Ok(None);
        };
        let mut matched = None;
        for proof in self.proofs(host)? {
            let boundary = &proof.baseline;
            let baseline = &boundary.observation;
            if !self.boundary_matches_profile(boundary)
                || authority_ref.is_some_and(|expected| expected != proof.proof_ref)
                || baseline.source != *identity.source()
                || baseline
                    .scope
                    .repository
                    .project_id()
                    .map(|id| ObservationScopeV1::Project {
                        project_id: id.clone(),
                    })
                    .as_ref()
                    != Some(identity.scope())
                || baseline.checkpoint.generation != identity.generation().generation_id()
                || baseline.checkpoint.file_identity != file_identity
                || !proof.frames.iter().any(|frame| {
                    frame.start == identity.position().start()
                        && frame.end == identity.position().end()
                        && frame.start >= boundary.start.physical_eof
                        && frame.resume_fingerprint == fingerprint
                })
            {
                continue;
            }
            let evidence =
                tracedecay_sessions::repository_provenance::OriginalObservationEvidenceV1 {
                    repository: baseline.scope.repository.clone(),
                    authority_ref: proof.proof_ref,
                };
            if matched.is_some() {
                return Err(ProviderHistoryErrorV1::Ineligible(
                    "ambiguous original event proof",
                ));
            }
            matched = Some(evidence);
        }
        Ok(matched)
    }

    fn live_boundaries(
        &self,
    ) -> HistoryResult<Vec<tracedecay_hooks::admission_ledger::HookLiveOriginBoundaryV1>> {
        let mut boundaries = Vec::new();
        for host in [
            tracedecay_hooks::HookHostV1::ClaudeCode,
            tracedecay_hooks::HookHostV1::Codex,
        ] {
            let current = tracedecay_hooks::admission_ledger::read_hook_live_origin_boundaries(
                &self.ledger_root(host),
                host,
                tracedecay_contracts::now_micros(),
            )
            .map_err(|_| ProviderHistoryErrorV1::Unavailable("live session boundary"))?;
            boundaries.extend(
                current
                    .into_iter()
                    .filter(|boundary| self.boundary_matches_profile(boundary)),
            );
            boundaries.extend(
                self.proofs(host)?
                    .into_iter()
                    .map(|proof| proof.baseline)
                    .filter(|boundary| self.boundary_matches_profile(boundary)),
            );
        }
        Ok(boundaries)
    }
}

impl tracedecay_sessions::repository_provenance::OriginalObservationProvenanceResolverV1
    for HookOriginReaderV1
{
    fn resolve(
        &self,
        identity: &tracedecay_domain::ObservationIdentityMaterialV1,
        resume_checkpoint: Option<(u64, u64)>,
    ) -> Option<tracedecay_sessions::repository_provenance::OriginalObservationEvidenceV1> {
        self.resolve_source(identity, resume_checkpoint, None)
            .ok()
            .flatten()
    }
}

/// Policy revision one permits history only between independently admitted
/// live sessions in the current exact checkout and profile.
pub(crate) struct MountedOriginalObservationAuthorityV1 {
    pub(crate) reader: Arc<HookOriginReaderV1>,
    pub(crate) bridge: Arc<HistoryIdentityBridgeV1>,
}

impl OriginalObservationAuthorityV1 for MountedOriginalObservationAuthorityV1 {
    fn validate_original_event(
        &self,
        authority_ref: &str,
        stored: &StoredObservation,
    ) -> HistoryResult<bool> {
        let cursor = stored.committed_cursor();
        let checkpoint = cursor.file_identity().zip(cursor.resume_fingerprint());
        let Some(evidence) = self.reader.resolve_source(
            stored.observation().identity(),
            checkpoint,
            Some(authority_ref),
        )?
        else {
            return Ok(false);
        };
        let attachment = stored
            .validated_repository_provenance_attachment()
            .map_err(|_| ProviderHistoryErrorV1::Ineligible("original provenance attachment"))?;
        Ok(
            matches!(attachment.availability(), EvidenceAvailabilityV1::Known(binding) if binding.capture() == &evidence.repository),
        )
    }

    fn authorizes_session(
        &self,
        source_session: &str,
        destination: &OwnedExactScope,
    ) -> HistoryResult<bool> {
        self.bridge.revalidate()?;
        self.bridge.validate_destination(destination)?;
        let mut source_admitted = false;
        let mut destination_admitted = false;
        for boundary in self.reader.live_boundaries()? {
            let repository = &boundary.observation.scope.repository;
            if repository.project_id() != Some(&self.bridge.canonical_project)
                || repository.repository_id() != &self.bridge.canonical_repository
                || repository.worktree_id() != Some(&self.bridge.canonical_worktree)
                || repository.evidence().attached_ref().value()
                    != self.bridge.scope.reference.as_ref()
            {
                continue;
            }
            let session = boundary.observation.source.session_id().as_str();
            source_admitted |= session == source_session;
            destination_admitted |= self
                .bridge
                .destination(session)
                .is_ok_and(|scope| &scope == destination);
        }
        Ok(source_admitted && destination_admitted)
    }
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum ProviderHistoryErrorV1 {
    #[error("history authority unavailable: {0}")]
    Unavailable(&'static str),
    #[error("history source is ineligible: {0}")]
    Ineligible(&'static str),
    #[error("history claim differs from its current authority: {0}")]
    ClaimMismatch(&'static str),
    #[error("history operation stopped: {0:?}")]
    Control(tracedecay_memory_provider_registry::TerminalCode),
}

type HistoryResult<T> = Result<T, ProviderHistoryErrorV1>;

/// Existing durable live-event authority supplied by the host. Implementations
/// must resolve the receipt and its exact source identity/range; a structurally
/// valid attachment or a matching session label alone cannot return true.
pub(crate) trait OriginalObservationAuthorityV1: Send + Sync {
    fn validate_original_event(
        &self,
        authority_ref: &str,
        observation: &StoredObservation,
    ) -> HistoryResult<bool>;

    /// Existing profile/session policy authorizes each original session for this
    /// exact destination. No missing grant means an implicit checkout-wide grant.
    fn authorizes_session(
        &self,
        canonical_session_id: &str,
        destination: &OwnedExactScope,
    ) -> HistoryResult<bool>;
}

/// Synchronous journal-dispatch adapter over the same canonical reader above.
/// The mounted host implements this using its retained runtime and registered
/// observation/disposition/event ports; no provider reply can supply it.
pub(crate) trait HistoryGrantRevalidationV1: Send + Sync {
    fn revalidate(
        &self,
        provider_id: &OwnedProviderId,
        grant: &HistoryGrant,
        control: &tracedecay_memory_provider_registry::OperationControl,
    ) -> HistoryResult<()>;

    fn revalidate_async<'a>(
        &'a self,
        provider_id: &'a OwnedProviderId,
        grant: &'a HistoryGrant,
        control: &'a tracedecay_memory_provider_registry::OperationControl,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = HistoryResult<()>> + Send + 'a>>;
}

/// One admitted bridge between the two existing identity schemes. Source IDs
/// are checked in the canonical namespace; daemon IDs are independently resolved
/// and checked against the mounted scope. Neither is compared to the other.
pub(crate) struct HistoryIdentityBridgeV1 {
    project_root: PathBuf,
    profile_id: UserProfileId,
    scope: ResolvedScope,
    canonical_project: ProjectId,
    canonical_repository: RepositoryId,
    canonical_worktree: WorktreeId,
}

impl HistoryIdentityBridgeV1 {
    pub(crate) fn admit(
        project_root: &Path,
        profile_id: &UserProfileId,
        mounted_scope: &ResolvedScope,
        registered_shard: &StoreShardIdV1,
    ) -> HistoryResult<Self> {
        if registered_shard.profile_id != *profile_id
            || registered_shard.scope
                != (StoreShardScopeV1::ProjectSessions {
                    project_id: mounted_scope.project_id.clone(),
                })
        {
            return Err(ProviderHistoryErrorV1::Ineligible(
                "registered project/profile",
            ));
        }
        let (canonical_project, canonical_repository, canonical_worktree) =
            Self::resolve_bridge(project_root, mounted_scope)?;
        Ok(Self {
            project_root: project_root.to_owned(),
            profile_id: profile_id.clone(),
            scope: mounted_scope.clone(),
            canonical_project,
            canonical_repository,
            canonical_worktree,
        })
    }

    fn resolve_bridge(
        project_root: &Path,
        mounted: &ResolvedScope,
    ) -> HistoryResult<(ProjectId, RepositoryId, WorktreeId)> {
        let resolved = tracedecay_code_index_runtime::resolved_scope_for_project(
            project_root,
            &mounted.project_id,
        )
        .map_err(|_| ProviderHistoryErrorV1::Unavailable("daemon scope"))?;
        if &resolved != mounted {
            return Err(ProviderHistoryErrorV1::Ineligible("mounted scope changed"));
        }
        let marker =
            tracedecay_runtime_core::storage::read_repository_identity_marker(project_root)
                .map_err(|_| ProviderHistoryErrorV1::Unavailable("repository marker"))?
                .ok_or(ProviderHistoryErrorV1::Unavailable("repository marker"))?;
        let context = RepositoryProvenanceAdmissionContext::from_authoritative_project_marker(
            project_root,
            &mounted.project_id,
            &marker,
        )
        .ok_or(ProviderHistoryErrorV1::Ineligible(
            "repository marker binding",
        ))?;
        // Probe validates the marker's common directory against the actual
        // repository before admitting its project-salted identity mapping.
        let capture = context.capture_snapshot(tracedecay_contracts::now_micros());
        let EvidenceAvailabilityV1::Known(capture) = capture.availability() else {
            return Err(ProviderHistoryErrorV1::Unavailable(
                "current repository capture",
            ));
        };
        if capture.evidence().attached_ref().value() != mounted.reference.as_ref() {
            return Err(ProviderHistoryErrorV1::Ineligible(
                "current reference binding",
            ));
        }
        context
            .admitted_identity()
            .ok_or(ProviderHistoryErrorV1::Unavailable(
                "canonical repository identity",
            ))
    }

    /// Fresh marker and daemon scope read. Cached construction is not proof that
    /// a later checkout, worktree replacement or branch still matches.
    pub(crate) fn revalidate(&self) -> HistoryResult<()> {
        let current = Self::resolve_bridge(&self.project_root, &self.scope)?;
        if current
            != (
                self.canonical_project.clone(),
                self.canonical_repository.clone(),
                self.canonical_worktree.clone(),
            )
        {
            return Err(ProviderHistoryErrorV1::Ineligible(
                "identity bridge changed",
            ));
        }
        Ok(())
    }

    pub(crate) fn destination(&self, canonical_session_id: &str) -> HistoryResult<OwnedExactScope> {
        exact_scope_for_session(&self.profile_id, &self.scope, canonical_session_id)
            .map_err(|_| ProviderHistoryErrorV1::Ineligible("destination scope"))
    }

    /// Freshly checks the retained namespace against actual mounted identities.
    /// The caller keeps the original agent-session identity carried by the trace.
    pub(crate) fn authorize_control_scope(
        &self,
        destination: &OwnedExactScope,
        control: &tracedecay_memory_provider_registry::OperationControl,
    ) -> HistoryResult<()> {
        control
            .snapshot()
            .map_err(ProviderHistoryErrorV1::Control)?;
        self.revalidate()?;
        control
            .snapshot()
            .map_err(ProviderHistoryErrorV1::Control)?;
        self.validate_destination(destination)?;
        control
            .snapshot()
            .map_err(ProviderHistoryErrorV1::Control)?;
        Ok(())
    }

    fn validate_destination(&self, destination: &OwnedExactScope) -> HistoryResult<()> {
        destination
            .validate()
            .map_err(|_| ProviderHistoryErrorV1::ClaimMismatch("destination"))?;
        let scope = &self.scope;
        if destination.profile_id != self.profile_id.as_str()
            || destination.project_id != scope.project_id.as_str()
            || destination.repository_identity != scope.repository_id.as_str()
            || destination.worktree_identity != scope.worktree_id.as_str()
            || Some(destination.branch_identity.as_str())
                != scope.reference.as_ref().map(|v| v.as_str())
            || destination.resolved_scope_digest != scope.scope_digest.as_str()
        {
            return Err(ProviderHistoryErrorV1::Ineligible("destination checkout"));
        }
        Ok(())
    }

    fn original_scope<O: OriginalObservationAuthorityV1 + ?Sized>(
        &self,
        stored: &StoredObservation,
        original_authority: &O,
    ) -> HistoryResult<OriginScopeEvidence> {
        let attachment = stored
            .validated_repository_provenance_attachment()
            .map_err(|_| ProviderHistoryErrorV1::Ineligible("original attachment binding"))?;
        let authority_ref = match attachment.origin() {
            ObservationOriginV1::Unavailable => return Ok(OriginScopeEvidence::Unavailable),
            ObservationOriginV1::IngestionOnly => return Ok(OriginScopeEvidence::IngestionOnly),
            ObservationOriginV1::Recorded { authority_ref, .. } => authority_ref,
        };
        if !original_authority.validate_original_event(authority_ref, stored)? {
            return Err(ProviderHistoryErrorV1::Ineligible(
                "original live-event receipt",
            ));
        }
        let EvidenceAvailabilityV1::Known(binding) = attachment.availability() else {
            return Err(ProviderHistoryErrorV1::Ineligible(
                "partial original repository capture",
            ));
        };
        let original = binding.capture();
        if original.project_id() != Some(&self.canonical_project)
            || original.repository_id() != &self.canonical_repository
            || original.worktree_id() != Some(&self.canonical_worktree)
            || original.evidence().attached_ref().value() != self.scope.reference.as_ref()
        {
            return Err(ProviderHistoryErrorV1::Ineligible("original checkout"));
        }
        Ok(OriginScopeEvidence::Recorded {
            scope: self.destination(stored.observation().source().session_id().as_str())?,
            authority_ref: authority_ref.clone(),
        })
    }
}

/// Bounded canonical page with truthful exclusions. Progress is later committed
/// by the destination's existing observation journey, never by this reader.
pub(crate) struct ProviderHistoryPageV1 {
    pub(crate) records: Vec<StoredObservation>,
    pub(crate) grant: Option<HistoryGrant>,
    pub(crate) last_scanned_sequence: u64,
    pub(crate) scanned: usize,
    pub(crate) withheld: usize,
    pub(crate) unknown_revision: usize,
    pub(crate) has_more: bool,
    pub(crate) has_older: bool,
}

pub(crate) struct ProviderHistoryReaderV1<'a, S: ?Sized, D: ?Sized, O: ?Sized> {
    pub(crate) bridge: &'a HistoryIdentityBridgeV1,
    pub(crate) observations: &'a S,
    pub(crate) dispositions: &'a D,
    pub(crate) original_authority: &'a O,
    pub(crate) journal: &'a SqliteObservationJournal,
    pub(crate) provider_id: &'a OwnedProviderId,
    pub(crate) policy_revision: u64,
}

/// Real installed provider authority over the mounted canonical ports and
/// existing journal. The runtime handle is borrowed from daemon composition.
pub(crate) struct ProviderHistoryAuthorityV1<S, D> {
    pub(crate) mounted_scope: ResolvedScope,
    pub(crate) profile_id: UserProfileId,
    pub(crate) registered_shard: StoreShardIdV1,
    pub(crate) observations: Arc<S>,
    pub(crate) dispositions: Arc<D>,
    pub(crate) original_authority: Option<Arc<MountedOriginalObservationAuthorityV1>>,
    pub(crate) journal: Arc<SqliteObservationJournal>,
    pub(crate) provider_id: OwnedProviderId,
    pub(crate) policy_revision: u64,
    pub(crate) runtime: tokio::runtime::Handle,
}

impl<S, D> ProviderHistoryAuthorityV1<S, D>
where
    S: ObservationAdmissionPort,
    D: RetrievalAnchorDispositionStore,
{
    pub(crate) fn reader(
        &self,
    ) -> HistoryResult<ProviderHistoryReaderV1<'_, S, D, MountedOriginalObservationAuthorityV1>>
    {
        self.validate_mount()?;
        let original_authority =
            self.original_authority
                .as_ref()
                .ok_or(ProviderHistoryErrorV1::Unavailable(
                    "original repository provenance",
                ))?;
        Ok(ProviderHistoryReaderV1 {
            bridge: original_authority.bridge.as_ref(),
            observations: self.observations.as_ref(),
            dispositions: self.dispositions.as_ref(),
            original_authority: original_authority.as_ref(),
            journal: self.journal.as_ref(),
            provider_id: &self.provider_id,
            policy_revision: self.policy_revision,
        })
    }

    pub(crate) fn validate_mount(&self) -> HistoryResult<()> {
        if self.registered_shard.profile_id != self.profile_id
            || self.registered_shard.scope
                != (StoreShardScopeV1::ProjectSessions {
                    project_id: self.mounted_scope.project_id.clone(),
                })
        {
            return Err(ProviderHistoryErrorV1::Ineligible(
                "registered project/profile",
            ));
        }
        if self.original_authority.as_ref().is_some_and(|original| {
            original.bridge.profile_id != self.profile_id
                || original.bridge.scope != self.mounted_scope
        }) {
            return Err(ProviderHistoryErrorV1::Ineligible("original bridge mount"));
        }
        Ok(())
    }

    fn validate_call_scope(&self, destination: &OwnedExactScope) -> HistoryResult<()> {
        self.validate_mount()?;
        destination
            .validate()
            .map_err(|_| ProviderHistoryErrorV1::ClaimMismatch("destination"))?;
        let scope = &self.mounted_scope;
        if destination.profile_id != self.profile_id.as_str()
            || destination.project_id != scope.project_id.as_str()
            || destination.repository_identity != scope.repository_id.as_str()
            || destination.worktree_identity != scope.worktree_id.as_str()
            || Some(destination.branch_identity.as_str())
                != scope.reference.as_ref().map(|v| v.as_str())
            || destination.resolved_scope_digest != scope.scope_digest.as_str()
        {
            return Err(ProviderHistoryErrorV1::Ineligible("destination checkout"));
        }
        Ok(())
    }

    fn validate_provider(&self, provider: &OwnedProviderId) -> HistoryResult<()> {
        if provider != &self.provider_id {
            return Err(ProviderHistoryErrorV1::ClaimMismatch("mounted provider"));
        }
        Ok(())
    }

    fn retained_admission(
        &self,
        key: &str,
        destination: &OwnedExactScope,
        registration_revision: u64,
    ) -> HistoryResult<tracedecay_memory_observation::AdmittedObservationV1> {
        let key = tracedecay_memory_observation::ObservationIdempotencyKeyV1::parse(key)
            .map_err(|_| ProviderHistoryErrorV1::ClaimMismatch("journal admission key"))?;
        let admitted = self
            .journal
            .read_admitted_observation_by_idempotency(&key)
            .map_err(|_| ProviderHistoryErrorV1::Unavailable("retained sanitized admission"))?
            .ok_or(ProviderHistoryErrorV1::Ineligible(
                "retained sanitized admission missing",
            ))?;
        if admitted.target.provider_id != self.provider_id
            || admitted.target.registration_revision != registration_revision
            || &admitted.exact_scope != destination
        {
            return Err(ProviderHistoryErrorV1::ClaimMismatch(
                "retained admission destination",
            ));
        }
        Ok(admitted)
    }

    async fn admit_current(
        &self,
        call: &tracedecay_memory_provider_registry::ProviderCall,
    ) -> Result<
        tracedecay_memory_provider_registry::CurrentAdvisoryAdmission,
        tracedecay_memory_provider_registry::AdvisoryAdmissionError,
    > {
        use tracedecay_memory_provider_registry::{
            AdvisoryAdmissionError, CurrentAdvisoryAdmission, CurrentRestoreAdmission,
            ProviderOperation,
        };
        call.validate()?;
        call.control
            .snapshot()
            .map_err(AdvisoryAdmissionError::Control)?;
        self.validate_provider(&call.provider_id)
            .map_err(advisory_error)?;
        self.validate_call_scope(&call.exact_scope)
            .map_err(advisory_error)?;
        let payload: Value = serde_json::from_slice(&call.payload.bytes)
            .map_err(|_| AdvisoryAdmissionError::Invalid("canonical advisory payload"))?;
        let payload_claimed = payload
            .get("history_grant")
            .filter(|value| !value.is_null())
            .map(history_grant_from_json)
            .transpose()
            .map_err(advisory_error)?;
        // Both carriers are claims. Every accepted source is resolved again by
        // the operation's existing canonical admission path below.
        let claimed = match (call.history_grant(), payload_claimed.as_ref()) {
            (Some(private), Some(payload)) if private != payload => {
                return Err(AdvisoryAdmissionError::Denied("conflicting history grants"));
            }
            (Some(private), _) => Some(private),
            (None, payload) => payload,
        };
        if claimed.is_some_and(|grant| grant.destination_scope != call.exact_scope) {
            return Err(AdvisoryAdmissionError::Denied("history destination"));
        }
        match call.operation {
            ProviderOperation::Observe => {
                let key = call
                    .idempotency_key
                    .as_deref()
                    .ok_or(AdvisoryAdmissionError::Invalid("observation key"))?;
                let admitted = self
                    .retained_admission(key, &call.exact_scope, call.registration_revision)
                    .map_err(advisory_error)?;
                if admitted.payload != call.payload
                    || admitted.extensions != call.extensions
                    || admitted.observation_id.as_str() != call.request_id
                    || call
                        .sanitization()
                        .map(|receipt| receipt.to_json())
                        .as_deref()
                        != Some(&admitted.sanitization.receipt_json)
                {
                    return Err(AdvisoryAdmissionError::Denied(
                        "observation differs from retained sanitized admission",
                    ));
                }
                if claimed.is_none() {
                    if payload
                        .pointer("/source_identity/original_source")
                        .is_some()
                    {
                        return Err(AdvisoryAdmissionError::Denied(
                            "original source without history grant",
                        ));
                    }
                    let id = CanonicalObservationIdV1::new(admitted.source.source_event_id.clone())
                        .map_err(|_| {
                            AdvisoryAdmissionError::Invalid("observation canonical source")
                        })?;
                    let stored = bounded_read(
                        &call.control,
                        self.observations.read_admitted_observation(&id),
                        "observation canonical source",
                    )
                    .await
                    .map_err(advisory_error)?
                    .ok_or(AdvisoryAdmissionError::Unavailable(
                        "observation canonical source",
                    ))?;
                    if stored.sequence() != admitted.source.source_sequence.0
                        || stored.observation().scope()
                            != &(ObservationScopeV1::Project {
                                project_id: self.mounted_scope.project_id.clone(),
                            })
                        || exact_scope_for_session(
                            &self.profile_id,
                            &self.mounted_scope,
                            stored.observation().source().session_id().as_str(),
                        )
                        .map_err(|_| {
                            AdvisoryAdmissionError::Denied(
                                "ordinary observation source delivery scope",
                            )
                        })? != call.exact_scope
                    {
                        return Err(AdvisoryAdmissionError::Denied(
                            "ordinary observation source delivery scope",
                        ));
                    }
                    return CurrentAdvisoryAdmission::new(call, Vec::new(), None);
                }
                let grant = claimed.ok_or(AdvisoryAdmissionError::Denied(
                    "observation history grant missing",
                ))?;
                if grant.sources.len() != 1 {
                    return Err(AdvisoryAdmissionError::Denied(
                        "observation history source coverage",
                    ));
                }
                let original = source_attribution_from_json(
                    payload.pointer("/source_identity/original_source").ok_or(
                        AdvisoryAdmissionError::Invalid("observation original source"),
                    )?,
                )
                .map_err(advisory_error)?;
                if grant.sources[0].attribution != original
                    || admitted.source.source_event_id != original.source.observation_id
                    || admitted.source.source_sequence.0 != original.source_sequence
                {
                    return Err(AdvisoryAdmissionError::Denied(
                        "observation source coverage",
                    ));
                }
                let current = self
                    .reader()
                    .map_err(advisory_error)?
                    .revalidate_grant(grant, &call.control)
                    .await
                    .map_err(advisory_error)?;
                CurrentAdvisoryAdmission::new(call, current.sources, None)
            }
            ProviderOperation::Replay => {
                let grant = claimed.ok_or(AdvisoryAdmissionError::Denied(
                    "replay history grant missing",
                ))?;
                let items = payload
                    .get("resolved_observations")
                    .and_then(Value::as_array)
                    .filter(|items| !items.is_empty() && items.len() <= MAX_HISTORY_PAGE)
                    .ok_or(AdvisoryAdmissionError::Invalid(
                        "resolved replay observations",
                    ))?;
                let refs = payload
                    .get("observation_batch_refs")
                    .and_then(Value::as_array)
                    .ok_or(AdvisoryAdmissionError::Invalid("replay receipt inventory"))?;
                let mut seen = std::collections::BTreeSet::new();
                let mut seen_sources = std::collections::BTreeSet::new();
                if items.len() != grant.sources.len() || refs.len() != items.len() {
                    return Err(AdvisoryAdmissionError::Denied("replay source coverage"));
                }
                for item in items {
                    call.control
                        .snapshot()
                        .map_err(AdvisoryAdmissionError::Control)?;
                    let observation =
                        item.get("observation")
                            .ok_or(AdvisoryAdmissionError::Invalid(
                                "resolved replay observation",
                            ))?;
                    let key = observation
                        .get("idempotency_key")
                        .and_then(Value::as_str)
                        .ok_or(AdvisoryAdmissionError::Invalid("resolved replay key"))?;
                    let admitted = self
                        .retained_admission(key, &call.exact_scope, call.registration_revision)
                        .map_err(advisory_error)?;
                    let expected =
                        resolved_replay_observation(&admitted).map_err(advisory_error)?;
                    let receipt = item
                        .get("receipt_ref")
                        .and_then(Value::as_str)
                        .ok_or(AdvisoryAdmissionError::Invalid("resolved replay receipt"))?;
                    if expected != *item
                        || !refs.iter().any(|value| value.as_str() == Some(receipt))
                        || !seen.insert(receipt)
                    {
                        return Err(AdvisoryAdmissionError::Denied(
                            "replay bytes or receipt differ from retained sanitized admission",
                        ));
                    }
                    let original = source_attribution_from_json(
                        observation
                            .pointer("/source_identity/original_source")
                            .ok_or(AdvisoryAdmissionError::Invalid("replay original source"))?,
                    )
                    .map_err(advisory_error)?;
                    if !seen_sources.insert(original.source.observation_id.clone())
                        || !grant
                            .sources
                            .iter()
                            .any(|source| source.attribution == original)
                        || admitted.source.source_event_id != original.source.observation_id
                        || admitted.source.source_sequence.0 != original.source_sequence
                    {
                        return Err(AdvisoryAdmissionError::Denied("replay source coverage"));
                    }
                }
                let current = self
                    .reader()
                    .map_err(advisory_error)?
                    .refresh_grant(grant, &call.control, true)
                    .await
                    .map_err(advisory_error)?;
                CurrentAdvisoryAdmission::new(call, current.sources, None)
            }
            ProviderOperation::SnapshotRestore => {
                let reader = self.reader().map_err(advisory_error)?;
                reader.bridge.revalidate().map_err(advisory_error)?;
                if claimed.is_some() {
                    return Err(AdvisoryAdmissionError::Invalid("restore history grant"));
                }
                let inventory = payload
                    .pointer("/snapshot/sources")
                    .and_then(Value::as_array)
                    .filter(|sources| {
                        sources.len()
                            <= tracedecay_memory_provider_registry::MAX_ADVISORY_ADMISSION_SOURCES
                    })
                    .ok_or(AdvisoryAdmissionError::Invalid("restore source inventory"))?;
                let declared = payload
                    .get("source_dispositions")
                    .and_then(Value::as_array)
                    .filter(|sources| sources.len() == inventory.len())
                    .ok_or(AdvisoryAdmissionError::Invalid(
                        "restore disposition inventory",
                    ))?;
                let mut current = Vec::with_capacity(inventory.len());
                let mut attributed = Vec::with_capacity(inventory.len());
                let mut seen = std::collections::BTreeSet::new();
                for wire in inventory {
                    call.control
                        .snapshot()
                        .map_err(AdvisoryAdmissionError::Control)?;
                    let source: tracedecay_memory_provider_registry::recall_admission::source_attribution::RecallOriginalSourceIdentityV1 =
                        serde_json::from_value(wire.clone()).map_err(|_| AdvisoryAdmissionError::Invalid("restore original source"))?;
                    let source = source
                        .to_owned_source()
                        .map_err(|_| AdvisoryAdmissionError::Invalid("restore original source"))?;
                    if !seen.insert(source.observation_id.clone())
                        || declared
                            .iter()
                            .filter(|item| item.get("source") == Some(wire))
                            .count()
                            != 1
                    {
                        return Err(AdvisoryAdmissionError::Denied(
                            "restore inventory duplicate or missing source",
                        ));
                    }
                    let id = CanonicalObservationIdV1::new(source.observation_id.clone()).map_err(
                        |_| AdvisoryAdmissionError::Invalid("restore canonical observation"),
                    )?;
                    let stored = bounded_read(
                        &call.control,
                        self.observations.read_admitted_observation(&id),
                        "restore canonical observation",
                    )
                    .await
                    .map_err(advisory_error)?;
                    match stored {
                        Some(stored) => {
                            let actual = reader
                                .project_source(&stored, &call.exact_scope, &call.control)
                                .await
                                .map_err(advisory_error)?;
                            if actual.attribution.source != source {
                                return Err(AdvisoryAdmissionError::Denied(
                                    "restore canonical source differs",
                                ));
                            }
                            current.push((source, actual.current_disposition.clone()));
                            attributed.push(actual);
                        }
                        None => {
                            // A retained host fence can keep purged source bytes
                            // blocked. It cannot supply missing original attribution
                            // or make any unavailable source eligible again.
                            let key = source_fence_digest(
                                self.profile_id.as_str(),
                                self.mounted_scope.project_id.as_str(),
                                &source,
                            );
                            let fence = self
                                .journal
                                .read_provider_source_fence(self.provider_id.as_str(), &key)
                                .map_err(|_| {
                                    AdvisoryAdmissionError::Unavailable("restore source fence")
                                })?
                                .filter(|fence| {
                                    fence.blocks_revision(source.source_revision.as_deref())
                                })
                                .ok_or(AdvisoryAdmissionError::Unavailable(
                                    "restore canonical source",
                                ))?;
                            current.push((
                                source,
                                CurrentSourceDisposition {
                                    state: SourceDisposition::Deleted,
                                    authority_ref: format!(
                                        "host-source:{}",
                                        digest_fields(&[&key, &fence.revision.to_string()])
                                    ),
                                    authority_revision: None,
                                    checked_at_utc_nanos: checked_nanos(
                                        tracedecay_contracts::now_micros().0,
                                    )
                                    .map_err(advisory_error)?,
                                },
                            ));
                        }
                    }
                }
                reader.bridge.revalidate().map_err(advisory_error)?;
                let references: Vec<_> = current
                    .iter()
                    .map(|(_, disposition)| disposition.authority_ref.as_str())
                    .collect();
                let checkpoint = RestoreDispositionCheckpoint {
                    exact_scope: call.exact_scope.clone(),
                    authority_ref: format!("host-disposition:{}", digest_fields(&references)),
                    authority_revision: None,
                    checked_at_utc_nanos: checked_nanos(tracedecay_contracts::now_micros().0)
                        .map_err(advisory_error)?,
                };
                CurrentAdvisoryAdmission::new(
                    call,
                    attributed,
                    Some(CurrentRestoreAdmission::new(checkpoint, current)?),
                )
            }
            _ => {
                let sources = match claimed {
                    Some(grant) => {
                        self.reader()
                            .map_err(advisory_error)?
                            .refresh_grant(grant, &call.control, true)
                            .await
                            .map_err(advisory_error)?
                            .sources
                    }
                    None => Vec::new(),
                };
                CurrentAdvisoryAdmission::new(call, sources, None)
            }
        }
    }
}

impl<S, D> HistoryGrantRevalidationV1 for ProviderHistoryAuthorityV1<S, D>
where
    S: ObservationAdmissionPort,
    D: RetrievalAnchorDispositionStore,
{
    fn revalidate(
        &self,
        provider_id: &OwnedProviderId,
        grant: &HistoryGrant,
        control: &tracedecay_memory_provider_registry::OperationControl,
    ) -> HistoryResult<()> {
        self.validate_provider(provider_id)?;
        if tokio::runtime::Handle::try_current().is_ok() {
            return Err(ProviderHistoryErrorV1::Unavailable(
                "synchronous history authority requires dispatch thread",
            ));
        }
        self.runtime
            .block_on(self.reader()?.revalidate_grant(grant, control))
            .map(|_| ())
    }

    fn revalidate_async<'a>(
        &'a self,
        provider_id: &'a OwnedProviderId,
        grant: &'a HistoryGrant,
        control: &'a tracedecay_memory_provider_registry::OperationControl,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = HistoryResult<()>> + Send + 'a>> {
        Box::pin(async move {
            self.validate_provider(provider_id)?;
            self.reader()?
                .revalidate_grant(grant, control)
                .await
                .map(|_| ())
        })
    }
}

impl<S, D> tracedecay_memory_provider_registry::AdvisoryAdmissionAuthority
    for ProviderHistoryAuthorityV1<S, D>
where
    S: ObservationAdmissionPort,
    D: RetrievalAnchorDispositionStore,
{
    fn admit(
        &self,
        call: &tracedecay_memory_provider_registry::ProviderCall,
    ) -> Result<
        tracedecay_memory_provider_registry::CurrentAdvisoryAdmission,
        tracedecay_memory_provider_registry::AdvisoryAdmissionError,
    > {
        if tokio::runtime::Handle::try_current().is_ok() {
            return Err(
                tracedecay_memory_provider_registry::AdvisoryAdmissionError::Unavailable(
                    "synchronous advisory authority requires dispatch thread",
                ),
            );
        }
        self.runtime.block_on(self.admit_current(call))
    }
}

fn advisory_error(
    error: ProviderHistoryErrorV1,
) -> tracedecay_memory_provider_registry::AdvisoryAdmissionError {
    use tracedecay_memory_provider_registry::AdvisoryAdmissionError;
    match error {
        ProviderHistoryErrorV1::Unavailable(reason) => AdvisoryAdmissionError::Unavailable(reason),
        ProviderHistoryErrorV1::Ineligible(reason)
        | ProviderHistoryErrorV1::ClaimMismatch(reason) => AdvisoryAdmissionError::Denied(reason),
        ProviderHistoryErrorV1::Control(terminal) => AdvisoryAdmissionError::Control(terminal),
    }
}

fn source_attribution_from_json(value: &Value) -> HistoryResult<SourceAttribution> {
    serde_json::from_value::<RecallSourceAttributionV1>(value.clone())
        .map_err(|_| ProviderHistoryErrorV1::ClaimMismatch("source attribution"))?
        .to_owned_attribution()
        .map_err(|_| ProviderHistoryErrorV1::ClaimMismatch("source attribution"))
}

/// Exact replay projection of a retained sanitized admission. The outer replay
/// metadata is added without rewriting the journal's immutable payload bytes.
pub(crate) fn resolved_replay_observation(
    admitted: &tracedecay_memory_observation::AdmittedObservationV1,
) -> HistoryResult<Value> {
    admitted
        .validate()
        .map_err(|_| ProviderHistoryErrorV1::Unavailable("retained replay admission"))?;
    let mut observation: Value = serde_json::from_slice(&admitted.payload.bytes)
        .map_err(|_| ProviderHistoryErrorV1::Unavailable("retained replay payload"))?;
    if observation
        .pointer("/source_identity/original_source")
        .is_none()
    {
        return Err(ProviderHistoryErrorV1::Ineligible(
            "retained replay original source",
        ));
    }
    observation["idempotency_key"] = json!(admitted.idempotency_key.as_str());
    observation["source_sequence"] = json!(admitted.source.source_sequence.0);
    Ok(json!({"receipt_ref":admitted.sanitization.receipt_id,"observation":observation}))
}

impl<S, D, O> ProviderHistoryReaderV1<'_, S, D, O>
where
    S: ObservationAdmissionPort + ?Sized,
    D: RetrievalAnchorDispositionStore + ?Sized,
    O: OriginalObservationAuthorityV1 + ?Sized,
{
    /// Fresh recent coverage comes from canonical sequence authority, never
    /// from the destination's durable enqueue cursor.
    pub(crate) async fn select_recent_page(
        &self,
        destination: &OwnedExactScope,
        limit: usize,
        control: &tracedecay_memory_provider_registry::OperationControl,
    ) -> HistoryResult<ProviderHistoryPageV1> {
        self.check_control(control)?;
        self.bridge.revalidate()?;
        self.bridge.validate_destination(destination)?;
        if self.policy_revision == 0 || limit == 0 || limit > MAX_HISTORY_PAGE {
            return Err(ProviderHistoryErrorV1::Ineligible(
                "history page bound or policy",
            ));
        }
        let request = ObservationRecentWindowRequest::new(limit)
            .map_err(|_| ProviderHistoryErrorV1::Ineligible("canonical recent window bound"))?;
        let window = bounded_read(
            control,
            self.observations
                .recent_admitted_observation_window(request),
            "canonical recent window",
        )
        .await?;
        let (after_sequence, through_sequence, has_older) = match window {
            Some(window) => {
                window.validate().map_err(|_| {
                    ProviderHistoryErrorV1::Unavailable("canonical recent window bounds")
                })?;
                (
                    window.first_sequence - 1,
                    window.last_sequence,
                    window.has_older,
                )
            }
            None => (0, 0, false),
        };
        let mut page = self
            .select_window_page(
                destination,
                after_sequence,
                through_sequence,
                limit,
                control,
            )
            .await?;
        page.has_older = has_older;
        Ok(page)
    }

    pub(crate) async fn select_page(
        &self,
        destination: &OwnedExactScope,
        after_sequence: u64,
        limit: usize,
        control: &tracedecay_memory_provider_registry::OperationControl,
    ) -> HistoryResult<ProviderHistoryPageV1> {
        self.select_page_with_upper_bound(destination, after_sequence, None, limit, control)
            .await
    }

    /// Upper bound is frozen by the preceding sequence-only window read. Rows
    /// committed later must not enter coverage, grants or downstream enqueue.
    pub(crate) async fn select_window_page(
        &self,
        destination: &OwnedExactScope,
        after_sequence: u64,
        through_sequence: u64,
        limit: usize,
        control: &tracedecay_memory_provider_registry::OperationControl,
    ) -> HistoryResult<ProviderHistoryPageV1> {
        if through_sequence < after_sequence {
            return Err(ProviderHistoryErrorV1::ClaimMismatch(
                "canonical recent window bounds",
            ));
        }
        self.select_page_with_upper_bound(
            destination,
            after_sequence,
            Some(through_sequence),
            limit,
            control,
        )
        .await
    }

    async fn select_page_with_upper_bound(
        &self,
        destination: &OwnedExactScope,
        after_sequence: u64,
        through_sequence: Option<u64>,
        limit: usize,
        control: &tracedecay_memory_provider_registry::OperationControl,
    ) -> HistoryResult<ProviderHistoryPageV1> {
        self.check_control(control)?;
        self.bridge.revalidate()?;
        self.bridge.validate_destination(destination)?;
        if self.policy_revision == 0 || limit == 0 || limit > MAX_HISTORY_PAGE {
            return Err(ProviderHistoryErrorV1::Ineligible(
                "history page bound or policy",
            ));
        }
        let request = ObservationReplayRequest::new(after_sequence, limit)
            .map_err(|_| ProviderHistoryErrorV1::Ineligible("canonical page bound"))?;
        let page = bounded_read(
            control,
            self.observations.replay_admitted_observations(request),
            "canonical replay",
        )
        .await?;
        // This is deliberately before counting, origin probing, projection,
        // grant construction, or retaining any record for journal enqueue.
        let page: Vec<_> = page
            .into_iter()
            .filter(|stored| through_sequence.is_none_or(|through| stored.sequence() <= through))
            .collect();
        let has_more = page.len() == limit
            && through_sequence.is_none_or(|through| {
                page.last()
                    .is_some_and(|stored| stored.sequence() < through)
            });
        let mut result = ProviderHistoryPageV1 {
            records: Vec::new(),
            grant: None,
            last_scanned_sequence: after_sequence,
            scanned: page.len(),
            withheld: 0,
            unknown_revision: 0,
            has_more,
            has_older: false,
        };
        let mut sources = Vec::new();
        for stored in page {
            self.check_control(control)?;
            result.last_scanned_sequence = stored.sequence();
            match self.project_source(&stored, destination, control).await {
                Ok(source) if retained_history_source(source.current_disposition.state) => {
                    result.unknown_revision +=
                        usize::from(source.attribution.source.source_revision.is_none());
                    sources.push(source);
                }
                Ok(_) | Err(ProviderHistoryErrorV1::Ineligible(_)) => result.withheld += 1,
                Err(error) => return Err(error),
            }
            result.records.push(stored);
        }
        self.check_control(control)?;
        self.bridge.revalidate()?;
        if !sources.is_empty() {
            result.grant = Some(self.grant(destination, sources)?);
        }
        Ok(result)
    }

    /// Directly authorizes one retained source by its canonical key. No claimed
    /// grant is constructed from ledger data to bootstrap this authority.
    pub(crate) async fn authorize_retained_source(
        &self,
        destination: &OwnedExactScope,
        expected: &SourceAttribution,
        control: &tracedecay_memory_provider_registry::OperationControl,
        include_unavailable: bool,
    ) -> HistoryResult<HistoryGrant> {
        self.check_control(control)?;
        if self.policy_revision == 0 {
            return Err(ProviderHistoryErrorV1::ClaimMismatch(
                "history policy revision",
            ));
        }
        expected
            .validate()
            .map_err(|_| ProviderHistoryErrorV1::ClaimMismatch("retained source attribution"))?;
        self.bridge.authorize_control_scope(destination, control)?;
        let id = CanonicalObservationIdV1::new(expected.source.observation_id.clone())
            .map_err(|_| ProviderHistoryErrorV1::ClaimMismatch("canonical observation id"))?;
        let stored = bounded_read(
            control,
            self.observations.read_admitted_observation(&id),
            "canonical observation",
        )
        .await?
        .ok_or(ProviderHistoryErrorV1::Ineligible(
            "canonical observation missing",
        ))?;
        let actual = self.project_source(&stored, destination, control).await?;
        if actual.attribution != *expected {
            return Err(ProviderHistoryErrorV1::ClaimMismatch(
                "original source attribution",
            ));
        }
        if !include_unavailable && !retained_history_source(actual.current_disposition.state) {
            return Err(ProviderHistoryErrorV1::Ineligible(
                "current source disposition",
            ));
        }
        self.bridge.authorize_control_scope(destination, control)?;
        self.grant(destination, vec![actual])
    }

    /// Reads every actual source again at dispatch/recall/restore. Provider
    /// generation and disposition revision are never compared here.
    pub(crate) async fn revalidate_grant(
        &self,
        claimed: &HistoryGrant,
        control: &tracedecay_memory_provider_registry::OperationControl,
    ) -> HistoryResult<HistoryGrant> {
        self.refresh_grant(claimed, control, false).await
    }

    async fn refresh_grant(
        &self,
        claimed: &HistoryGrant,
        control: &tracedecay_memory_provider_registry::OperationControl,
        include_unavailable: bool,
    ) -> HistoryResult<HistoryGrant> {
        self.check_control(control)?;
        claimed
            .validate_structure()
            .map_err(|_| ProviderHistoryErrorV1::ClaimMismatch("history grant"))?;
        if claimed.policy_revision != self.policy_revision
            || claimed.sources.len() > MAX_HISTORY_PAGE
        {
            return Err(ProviderHistoryErrorV1::ClaimMismatch(
                "policy or source bound",
            ));
        }
        self.bridge.revalidate()?;
        self.bridge
            .validate_destination(&claimed.destination_scope)?;
        let mut current = Vec::with_capacity(claimed.sources.len());
        for source in &claimed.sources {
            self.check_control(control)?;
            let id =
                CanonicalObservationIdV1::new(source.attribution.source.observation_id.clone())
                    .map_err(|_| {
                        ProviderHistoryErrorV1::ClaimMismatch("canonical observation id")
                    })?;
            let stored = bounded_read(
                control,
                self.observations.read_admitted_observation(&id),
                "canonical observation",
            )
            .await?
            .ok_or(ProviderHistoryErrorV1::Ineligible(
                "canonical observation missing",
            ))?;
            let actual = self
                .project_source(&stored, &claimed.destination_scope, control)
                .await?;
            if actual.attribution != source.attribution {
                return Err(ProviderHistoryErrorV1::ClaimMismatch(
                    "original source attribution",
                ));
            }
            if !include_unavailable && !retained_history_source(actual.current_disposition.state) {
                return Err(ProviderHistoryErrorV1::Ineligible(
                    "current source disposition",
                ));
            }
            current.push(actual);
        }
        self.check_control(control)?;
        self.bridge.revalidate()?;
        self.grant(&claimed.destination_scope, current)
    }

    async fn project_source(
        &self,
        stored: &StoredObservation,
        destination: &OwnedExactScope,
        control: &tracedecay_memory_provider_registry::OperationControl,
    ) -> HistoryResult<GrantedHistorySource> {
        let observation = stored.observation();
        if observation.scope()
            != &(ObservationScopeV1::Project {
                project_id: self.bridge.canonical_project.clone(),
            })
        {
            return Err(ProviderHistoryErrorV1::Ineligible("canonical project"));
        }
        let origin = self
            .bridge
            .original_scope(stored, self.original_authority)?;
        origin
            .recorded_scope()
            .map_err(|_| ProviderHistoryErrorV1::Ineligible("original scope unavailable"))?;
        if !self
            .original_authority
            .authorizes_session(observation.source().session_id().as_str(), destination)?
        {
            return Err(ProviderHistoryErrorV1::Ineligible(
                "ungranted source session",
            ));
        }
        let envelope: CanonicalObservationEnvelopeV1 =
            serde_json::from_value(observation.payload().clone())
                .map_err(|_| ProviderHistoryErrorV1::Ineligible("canonical envelope"))?;
        envelope
            .validate()
            .map_err(|_| ProviderHistoryErrorV1::Ineligible("canonical envelope"))?;
        if envelope.provider() != observation.source().provider()
            || envelope.relations().session_id() != observation.source().session_id()
            || envelope.evidence().ordering_domain() != observation.identity().ordering_domain()
            || envelope.evidence().range() != observation.identity().position()
            || observation
                .identity()
                .native_record_id()
                .is_some_and(|id| id != envelope.stable_record_id())
        {
            return Err(ProviderHistoryErrorV1::Ineligible(
                "canonical envelope source binding",
            ));
        }
        let canonical_bytes =
            tracedecay_memory_hygiene::canonical_payload_bytes(observation.payload())
                .map_err(|_| ProviderHistoryErrorV1::Ineligible("canonical source bytes"))?;
        let source = OriginalSourceIdentity {
            canonical_provider_id: OwnedProviderId::new(observation.source().provider().as_str())
                .map_err(|_| {
                ProviderHistoryErrorV1::Ineligible("canonical provider")
            })?,
            canonical_session_id: observation.source().session_id().as_str().to_owned(),
            source_key: observation.source().source_key().as_str().to_owned(),
            stable_record_id: observation
                .identity()
                .native_record_id()
                .map(|v| v.as_str().to_owned()),
            observation_id: observation.observation_id().as_str().to_owned(),
            source_revision: envelope.evidence().revision().map(str::to_owned),
            content_sha256: hex::encode(Sha256::digest(canonical_bytes)),
        };
        let attribution = SourceAttribution {
            source,
            origin_scope: origin,
            source_sequence: stored.sequence(),
            occurred_at_utc_nanos: stored
                .retrieval_anchor()
                .occurred_at()
                .map(|time| checked_nanos(time.start.0))
                .transpose()?,
            ingested_at_utc_nanos: checked_nanos(stored.retrieval_anchor().ingested_at().0)?,
            validity: RecordedValidity::default(),
        };
        let owner = RetrievalAnchorOwnerV1::V2(FactOwnerV1::Project {
            project_id: self.bridge.canonical_project.clone(),
        });
        let current = bounded_read(
            control,
            self.dispositions
                .current_disposition(stored.retrieval_anchor_id(), &owner),
            "current source disposition",
        )
        .await?;
        let (mut state, mut authority_ref) = match current {
            None => (
                SourceDisposition::Available,
                format!("anchor:{}:initial", stored.retrieval_anchor_id().as_str()),
            ),
            Some(record) => {
                if record.owner() != &owner || record.anchor_id() != stored.retrieval_anchor_id() {
                    return Err(ProviderHistoryErrorV1::Unavailable(
                        "disposition owner binding",
                    ));
                }
                let state = match record.state() {
                    AnchorDispositionStateV1::Active => SourceDisposition::Available,
                    AnchorDispositionStateV1::Superseded => SourceDisposition::Superseded,
                    AnchorDispositionStateV1::Redacted => SourceDisposition::Redacted,
                    AnchorDispositionStateV1::Expired => SourceDisposition::Expired,
                    AnchorDispositionStateV1::Deleted => SourceDisposition::Deleted,
                    AnchorDispositionStateV1::Quarantined
                    | AnchorDispositionStateV1::Unavailable => SourceDisposition::Unknown,
                };
                (
                    state,
                    format!("anchor-disposition:{}", record.disposition_id()),
                )
            }
        };
        let key = original_source_fence_digest(&attribution)?;
        let fence_control = control
            .snapshot()
            .map_err(ProviderHistoryErrorV1::Control)?;
        if let Some(fence) = self
            .journal
            .read_provider_source_fence_bounded(
                self.provider_id.as_str(),
                &key,
                tracedecay_memory_observation::RecoveryTimeBudgetV1 {
                    remaining_micros: i64::try_from(fence_control.remaining_millis)
                        .unwrap_or(i64::MAX)
                        .saturating_mul(1_000),
                },
                &control.cancellation(),
            )
            .map_err(|error| match error {
                tracedecay_memory_observation::ObservationJournalError::OperationCancelled {
                    ..
                } => ProviderHistoryErrorV1::Control(
                    tracedecay_memory_provider_registry::TerminalCode::Cancelled,
                ),
                tracedecay_memory_observation::ObservationJournalError::BudgetExhausted {
                    ..
                } => ProviderHistoryErrorV1::Control(
                    tracedecay_memory_provider_registry::TerminalCode::DeadlineExceeded,
                ),
                _ => ProviderHistoryErrorV1::Unavailable("provider source fence"),
            })?
        {
            if fence.blocks_revision(attribution.source.source_revision.as_deref()) {
                state = SourceDisposition::Deleted;
            }
            authority_ref = format!(
                "host-source:{}",
                digest_fields(&[&authority_ref, &key, &fence.revision.to_string()])
            );
        }
        attribution
            .validate()
            .map_err(|_| ProviderHistoryErrorV1::Ineligible("source attribution"))?;
        Ok(GrantedHistorySource {
            attribution,
            current_disposition: CurrentSourceDisposition {
                state,
                authority_ref,
                authority_revision: None,
                checked_at_utc_nanos: checked_nanos(tracedecay_contracts::now_micros().0)?,
            },
        })
    }

    /// Resolves one provider-carried identity through the actual canonical row.
    /// The returned full attribution is read from canonical origin proof; it is
    /// never reconstructed from the smaller snapshot identity inventory.
    pub(crate) async fn authorize_original_source_identity(
        &self,
        destination: &OwnedExactScope,
        expected: &OriginalSourceIdentity,
        control: &tracedecay_memory_provider_registry::OperationControl,
    ) -> HistoryResult<GrantedHistorySource> {
        self.check_control(control)?;
        expected
            .validate()
            .map_err(|_| ProviderHistoryErrorV1::ClaimMismatch("original source identity"))?;
        self.bridge.authorize_control_scope(destination, control)?;
        let id = CanonicalObservationIdV1::new(expected.observation_id.clone())
            .map_err(|_| ProviderHistoryErrorV1::ClaimMismatch("canonical observation id"))?;
        let stored = bounded_read(
            control,
            self.observations.read_admitted_observation(&id),
            "control canonical source identity",
        )
        .await?
        .ok_or(ProviderHistoryErrorV1::Ineligible(
            "control canonical source missing",
        ))?;
        let actual = self.project_source(&stored, destination, control).await?;
        if &actual.attribution.source != expected {
            return Err(ProviderHistoryErrorV1::ClaimMismatch(
                "canonical original source changed",
            ));
        }
        self.bridge.authorize_control_scope(destination, control)?;
        Ok(actual)
    }

    pub(crate) fn disposition_checkpoint(
        &self,
        destination: &OwnedExactScope,
        sources: &[GrantedHistorySource],
    ) -> HistoryResult<RestoreDispositionCheckpoint> {
        let references: Vec<_> = sources
            .iter()
            .map(|source| source.current_disposition.authority_ref.as_str())
            .collect();
        Ok(RestoreDispositionCheckpoint {
            exact_scope: destination.clone(),
            authority_ref: format!("host-disposition:{}", digest_fields(&references)),
            authority_revision: None,
            checked_at_utc_nanos: checked_nanos(tracedecay_contracts::now_micros().0)?,
        })
    }

    pub(crate) fn grant(
        &self,
        destination: &OwnedExactScope,
        sources: Vec<GrantedHistorySource>,
    ) -> HistoryResult<HistoryGrant> {
        let checkpoint = self.disposition_checkpoint(destination, &sources)?;
        let relation = if sources
            .iter()
            .all(|source| source.attribution.origin_scope.recorded_scope() == Ok(destination))
        {
            HistoryRelation::ExactScope
        } else {
            HistoryRelation::SameCheckout
        };
        let grant = HistoryGrant {
            authorization_ref: format!(
                "host-history:{}",
                digest_fields(&[
                    &destination.exact_scope_sha256(),
                    &checkpoint.authority_ref,
                    &self.policy_revision.to_string()
                ])
            ),
            policy_revision: self.policy_revision,
            destination_scope: destination.clone(),
            relation,
            sources,
            disposition_checkpoint: checkpoint,
        };
        grant
            .validate_structure()
            .map_err(|_| ProviderHistoryErrorV1::ClaimMismatch("assembled history grant"))?;
        Ok(grant)
    }

    fn check_control(
        &self,
        control: &tracedecay_memory_provider_registry::OperationControl,
    ) -> HistoryResult<()> {
        control
            .snapshot()
            .map(|_| ())
            .map_err(ProviderHistoryErrorV1::Control)
    }
}

/// Retained nonprivacy states can be admitted to a provider for temporal
/// evaluation. This does not invent event times or make a current answer eligible.
pub(crate) fn retained_history_source(state: SourceDisposition) -> bool {
    matches!(
        state,
        SourceDisposition::Available | SourceDisposition::Superseded | SourceDisposition::Revoked
    )
}

/// Stable source key for host deletion fanout. The destination scope, provider
/// registration, source revision, source bytes and delivery id never enter it.
pub(crate) fn original_source_fence_digest(source: &SourceAttribution) -> HistoryResult<String> {
    source
        .validate()
        .map_err(|_| ProviderHistoryErrorV1::ClaimMismatch("fence source"))?;
    let origin = source
        .origin_scope
        .recorded_scope()
        .map_err(|_| ProviderHistoryErrorV1::Ineligible("fence original scope"))?;
    Ok(source_fence_digest(
        &origin.profile_id,
        &origin.project_id,
        &source.source,
    ))
}

fn source_fence_digest(profile: &str, project: &str, source: &OriginalSourceIdentity) -> String {
    digest_fields(&[
        profile,
        project,
        source.canonical_provider_id.as_str(),
        &source.canonical_session_id,
        &source.source_key,
    ])
}

fn digest_fields(values: &[&str]) -> String {
    let mut digest = Sha256::new();
    digest.update(b"tracedecay.host-provider-history.v1\0");
    for value in values {
        digest.update((value.len() as u64).to_be_bytes());
        digest.update(value.as_bytes());
    }
    hex::encode(digest.finalize())
}

fn checked_nanos(micros: i64) -> HistoryResult<i64> {
    micros
        .checked_mul(1000)
        .ok_or(ProviderHistoryErrorV1::Ineligible(
            "timestamp outside nanosecond range",
        ))
}

fn timestamp(nanos: i64) -> String {
    chrono::DateTime::<chrono::Utc>::from_timestamp_nanos(nanos)
        .to_rfc3339_opts(chrono::SecondsFormat::AutoSi, true)
}

fn read_timestamp(value: &str) -> HistoryResult<i64> {
    if !value.ends_with('Z') {
        return Err(ProviderHistoryErrorV1::ClaimMismatch("UTC timestamp"));
    }
    chrono::DateTime::parse_from_rfc3339(value)
        .ok()
        .and_then(|v| v.timestamp_nanos_opt())
        .ok_or(ProviderHistoryErrorV1::ClaimMismatch(
            "nanosecond timestamp",
        ))
}

fn scope_wire(scope: &OwnedExactScope) -> RecallOutcomeScopeV1 {
    RecallOutcomeScopeV1 {
        profile_id: scope.profile_id.clone(),
        project_id: scope.project_id.clone(),
        repository_identity: scope.repository_identity.clone(),
        worktree_identity: scope.worktree_identity.clone(),
        branch_identity: scope.branch_identity.clone(),
        agent_session_id: scope.agent_session_id.clone(),
        resolved_scope_digest: scope.resolved_scope_digest.clone(),
    }
}

fn scope_owned(scope: RecallOutcomeScopeV1) -> HistoryResult<OwnedExactScope> {
    OwnedExactScope::new(
        scope.profile_id,
        scope.project_id,
        scope.repository_identity,
        scope.worktree_identity,
        scope.branch_identity,
        scope.agent_session_id,
        scope.resolved_scope_digest,
    )
    .map_err(|_| ProviderHistoryErrorV1::ClaimMismatch("exact scope"))
}

pub(crate) fn source_attribution_json(attribution: &SourceAttribution) -> HistoryResult<Value> {
    attribution
        .validate()
        .map_err(|_| ProviderHistoryErrorV1::ClaimMismatch("source attribution"))?;
    let source = &attribution.source;
    let validity = &attribution.validity;
    let origin = match &attribution.origin_scope {
        OriginScopeEvidence::Recorded {
            scope,
            authority_ref,
        } => {
            json!({"state":"recorded", "exact_scope_identity":scope_wire(scope), "authority_ref":authority_ref})
        }
        OriginScopeEvidence::IngestionOnly => json!({"state":"ingestion_only"}),
        OriginScopeEvidence::Unavailable => json!({"state":"unavailable"}),
    };
    Ok(json!({
        "source": {"canonical_provider_id":source.canonical_provider_id.as_str(),
            "canonical_session_id":source.canonical_session_id,"source_key":source.source_key,
            "stable_record_id":source.stable_record_id,"observation_id":source.observation_id,
            "source_revision":source.source_revision,"content_sha256":source.content_sha256},
        "origin_scope":origin,"source_sequence":attribution.source_sequence,
        "occurred_at":attribution.occurred_at_utc_nanos.map(timestamp),
        "ingested_at":timestamp(attribution.ingested_at_utc_nanos),
        "validity":{"valid_from":validity.valid_from_utc_nanos.map(timestamp),
            "valid_until":validity.valid_until_utc_nanos.map(timestamp),
            "superseded_at":validity.superseded_at_utc_nanos.map(timestamp),
            "superseded_by":validity.superseded_by,"revoked_at":validity.revoked_at_utc_nanos.map(timestamp)}
    }))
}

fn required_nullable<'de, D, T>(deserializer: D) -> Result<Option<T>, D::Error>
where
    D: serde::Deserializer<'de>,
    T: Deserialize<'de>,
{
    Option::deserialize(deserializer)
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct WireDisposition {
    state: String,
    authority_ref: String,
    #[serde(deserialize_with = "required_nullable")]
    authority_revision: Option<u64>,
    checked_at: String,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct WireCheckpoint {
    exact_scope: RecallOutcomeScopeV1,
    authority_ref: String,
    #[serde(deserialize_with = "required_nullable")]
    authority_revision: Option<u64>,
    checked_at: String,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct WireGrantedSource {
    attribution: RecallSourceAttributionV1,
    current_disposition: WireDisposition,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct WireHistoryGrant {
    authorization_ref: String,
    policy_revision: u64,
    destination_scope: RecallOutcomeScopeV1,
    relation: String,
    sources: Vec<WireGrantedSource>,
    disposition_checkpoint: WireCheckpoint,
}

pub(crate) fn history_grant_json(grant: &HistoryGrant) -> HistoryResult<Value> {
    grant
        .validate_structure()
        .map_err(|_| ProviderHistoryErrorV1::ClaimMismatch("history grant"))?;
    let sources = grant
        .sources
        .iter()
        .map(|source| {
            Ok(WireGrantedSource {
                attribution: serde_json::from_value(source_attribution_json(&source.attribution)?)
                    .map_err(|_| ProviderHistoryErrorV1::ClaimMismatch("source wire"))?,
                current_disposition: WireDisposition {
                    state: source.current_disposition.state.as_wire().to_owned(),
                    authority_ref: source.current_disposition.authority_ref.clone(),
                    authority_revision: source.current_disposition.authority_revision,
                    checked_at: timestamp(source.current_disposition.checked_at_utc_nanos),
                },
            })
        })
        .collect::<HistoryResult<Vec<_>>>()?;
    let checkpoint = &grant.disposition_checkpoint;
    serde_json::to_value(WireHistoryGrant {
        authorization_ref: grant.authorization_ref.clone(),
        policy_revision: grant.policy_revision,
        destination_scope: scope_wire(&grant.destination_scope),
        relation: grant.relation.as_wire().to_owned(),
        sources,
        disposition_checkpoint: WireCheckpoint {
            exact_scope: scope_wire(&checkpoint.exact_scope),
            authority_ref: checkpoint.authority_ref.clone(),
            authority_revision: checkpoint.authority_revision,
            checked_at: timestamp(checkpoint.checked_at_utc_nanos),
        },
    })
    .map_err(|_| ProviderHistoryErrorV1::ClaimMismatch("history wire"))
}

pub(crate) fn history_grant_from_json(value: &Value) -> HistoryResult<HistoryGrant> {
    let wire: WireHistoryGrant = serde_json::from_value(value.clone())
        .map_err(|_| ProviderHistoryErrorV1::ClaimMismatch("history wire"))?;
    if wire.sources.is_empty() || wire.sources.len() > MAX_HISTORY_PAGE {
        return Err(ProviderHistoryErrorV1::ClaimMismatch("source count"));
    }
    let sources = wire
        .sources
        .into_iter()
        .map(|source| {
            let disposition = source.current_disposition;
            Ok(GrantedHistorySource {
                attribution: source
                    .attribution
                    .to_owned_attribution()
                    .map_err(|_| ProviderHistoryErrorV1::ClaimMismatch("source wire"))?,
                current_disposition: CurrentSourceDisposition {
                    state: SourceDisposition::from_wire(&disposition.state)
                        .ok_or(ProviderHistoryErrorV1::ClaimMismatch("source state"))?,
                    authority_ref: disposition.authority_ref,
                    authority_revision: disposition.authority_revision,
                    checked_at_utc_nanos: read_timestamp(&disposition.checked_at)?,
                },
            })
        })
        .collect::<HistoryResult<Vec<_>>>()?;
    let checkpoint = wire.disposition_checkpoint;
    let grant = HistoryGrant {
        authorization_ref: wire.authorization_ref,
        policy_revision: wire.policy_revision,
        destination_scope: scope_owned(wire.destination_scope)?,
        relation: HistoryRelation::from_wire(&wire.relation)
            .ok_or(ProviderHistoryErrorV1::ClaimMismatch("history relation"))?,
        sources,
        disposition_checkpoint: RestoreDispositionCheckpoint {
            exact_scope: scope_owned(checkpoint.exact_scope)?,
            authority_ref: checkpoint.authority_ref,
            authority_revision: checkpoint.authority_revision,
            checked_at_utc_nanos: read_timestamp(&checkpoint.checked_at)?,
        },
    };
    grant
        .validate_structure()
        .map_err(|_| ProviderHistoryErrorV1::ClaimMismatch("history grant"))?;
    Ok(grant)
}

pub(crate) fn validate_history_record<'a>(
    grant: &'a HistoryGrant,
    stored: &StoredObservation,
) -> HistoryResult<&'a SourceAttribution> {
    grant
        .validate_structure()
        .map_err(|_| ProviderHistoryErrorV1::ClaimMismatch("history grant"))?;
    let observation = stored.observation();
    let attribution = grant
        .sources
        .iter()
        .find(|source| {
            source.attribution.source.observation_id == observation.observation_id().as_str()
        })
        .map(|source| &source.attribution)
        .ok_or(ProviderHistoryErrorV1::Ineligible(
            "record outside history grant",
        ))?;
    let bytes = tracedecay_memory_hygiene::canonical_payload_bytes(observation.payload())
        .map_err(|_| ProviderHistoryErrorV1::ClaimMismatch("canonical record encoding"))?;
    if attribution.source_sequence != stored.sequence()
        || attribution.source.canonical_provider_id.as_str()
            != observation.source().provider().as_str()
        || attribution.source.canonical_session_id != observation.source().session_id().as_str()
        || attribution.source.source_key != observation.source().source_key().as_str()
        || attribution.source.content_sha256 != hex::encode(Sha256::digest(bytes))
    {
        return Err(ProviderHistoryErrorV1::ClaimMismatch(
            "history canonical record",
        ));
    }
    Ok(attribution)
}

pub(crate) async fn bounded_read<T, E>(
    control: &tracedecay_memory_provider_registry::OperationControl,
    future: impl std::future::Future<Output = Result<T, E>>,
    authority: &'static str,
) -> HistoryResult<T> {
    let snapshot = control
        .snapshot()
        .map_err(ProviderHistoryErrorV1::Control)?;
    let deadline =
        tokio::time::Instant::now() + std::time::Duration::from_millis(snapshot.remaining_millis);
    tokio::pin!(future);
    loop {
        control
            .snapshot()
            .map_err(ProviderHistoryErrorV1::Control)?;
        tokio::select! {
            biased;
            result = &mut future => {
                control.snapshot().map_err(ProviderHistoryErrorV1::Control)?;
                return result.map_err(|_| ProviderHistoryErrorV1::Unavailable(authority));
            }
            () = tokio::time::sleep_until(deadline) => {
                return Err(ProviderHistoryErrorV1::Control(control.snapshot().err().unwrap_or(
                    tracedecay_memory_provider_registry::TerminalCode::DeadlineExceeded,
                )));
            }
            () = tokio::time::sleep(std::time::Duration::from_millis(10)) => {}
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tracedecay_memory_provider_registry::{CancellationToken, OperationControl, TerminalCode};

    #[test]
    fn retained_history_gate_keeps_revocation_distinct_from_privacy_deletion() {
        for state in [
            SourceDisposition::Available,
            SourceDisposition::Superseded,
            SourceDisposition::Revoked,
        ] {
            assert!(retained_history_source(state), "{state:?}");
        }
        for state in [
            SourceDisposition::Deleted,
            SourceDisposition::Redacted,
            SourceDisposition::Expired,
            SourceDisposition::Unknown,
        ] {
            assert!(!retained_history_source(state), "{state:?}");
        }
    }

    #[tokio::test]
    async fn pending_authority_read_preserves_cancellation_and_deadline_terminals() {
        let cancellation = CancellationToken::new();
        let control = OperationControl::new(
            tracedecay_contracts::now_micros().0 + 30_000_000,
            30_000,
            cancellation.clone(),
        );
        let read = bounded_read(
            &control,
            std::future::pending::<Result<(), ()>>(),
            "pending",
        );
        let cancel = async {
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
            cancellation.cancel();
        };
        let (result, ()) = tokio::join!(
            tokio::time::timeout(std::time::Duration::from_millis(500), read),
            cancel,
        );
        assert!(matches!(
            result,
            Ok(Err(ProviderHistoryErrorV1::Control(
                TerminalCode::Cancelled
            )))
        ));
        let deadline = OperationControl::new(
            tracedecay_contracts::now_micros().0 + 25_000,
            25,
            CancellationToken::new(),
        );
        assert!(matches!(
            bounded_read(
                &deadline,
                std::future::pending::<Result<(), ()>>(),
                "pending"
            )
            .await,
            Err(ProviderHistoryErrorV1::Control(
                TerminalCode::DeadlineExceeded
            ))
        ));
    }
}

impl MountedOriginalObservationAuthorityV1 {
    /// Point session-row admission uses this exact provider/session boundary.
    /// Existing scope encoding remains unchanged and does not add a provider salt.
    pub(crate) fn authorize_live_canonical_session(
        &self,
        canonical_provider_id: &str,
        session_id: &str,
        project_path: &str,
        destination: &OwnedExactScope,
        control: &tracedecay_memory_provider_registry::OperationControl,
    ) -> HistoryResult<()> {
        self.bridge.authorize_control_scope(destination, control)?;
        if HookOriginReaderV1::host(canonical_provider_id).is_none() {
            return Err(ProviderHistoryErrorV1::Ineligible(
                "canonical hook provider",
            ));
        }
        let canonical_project_path = std::fs::canonicalize(project_path)
            .map_err(|_| ProviderHistoryErrorV1::Unavailable("canonical session project path"))?;
        if canonical_project_path != self.bridge.project_root {
            return Err(ProviderHistoryErrorV1::Ineligible(
                "canonical session project",
            ));
        }
        let mut admitted = false;
        for boundary in self.reader.live_boundaries()? {
            control
                .snapshot()
                .map_err(ProviderHistoryErrorV1::Control)?;
            let repository = &boundary.observation.scope.repository;
            let source = &boundary.observation.source;
            if source.provider().as_str() == canonical_provider_id
                && source.session_id().as_str() == session_id
                && repository.project_id() == Some(&self.bridge.canonical_project)
                && repository.repository_id() == &self.bridge.canonical_repository
                && repository.worktree_id() == Some(&self.bridge.canonical_worktree)
                && repository.evidence().attached_ref().value()
                    == self.bridge.scope.reference.as_ref()
                && self
                    .bridge
                    .destination(session_id)
                    .is_ok_and(|scope| &scope == destination)
            {
                admitted = true;
            }
        }
        self.bridge.authorize_control_scope(destination, control)?;
        if admitted {
            Ok(())
        } else {
            Err(ProviderHistoryErrorV1::Ineligible(
                "current canonical session boundary",
            ))
        }
    }
}
