//! The mounted production observation journey.
//!
//! This module is the one place where three authorities meet, and it owns the
//! seam between them rather than letting any of them learn about the others:
//!
//! * the **canonical observation store** (`tracedecay_store::ObservationStore`,
//!   reached through the project's registered session database) settles host
//!   session messages;
//! * the **durable journal** (`tracedecay_memory_observation`) owns delivery
//!   status, attempts, acknowledgements, and replay position;
//! * the **provider registry** (`tracedecay_memory_provider_registry`) owns the
//!   readiness handshake and the only dispatch route.
//!
//! # Why the mount order is what it is
//!
//! The exact coding scope is copied verbatim from the authoritative
//! [`ResolvedScope`] that project-open code-index authorities already
//! published. Nothing here re-derives repository, worktree, or branch identity
//! from a path, a CWD, or the journal's own storage location — the host-event
//! observation policy names exactly those as forbidden inference inputs. That
//! is why the journey mounts *after* `project_code_index_authorities` and
//! *after* the project session database is open: before both, there is no
//! truthful scope and no settled source to replay.
//!
//! # What the journal path is, and is not
//!
//! The journal file lives beside the project's other store-owned databases,
//! under the canonical store layout's data root. That location is diagnostic
//! and storage placement only. It is never an identity input: two checkouts
//! that resolve to the same exact scope would share an identity regardless of
//! where their journals sit, and a journal that is moved keeps every identity
//! it holds.
//!
//! # Crash safety
//!
//! Startup replay is authoritative. The journal's per-stream replay cursor says
//! where to resume, so a crash between the canonical commit and the journal
//! append is recovered by re-presenting the canonical record — safe because the
//! idempotency key is content-derived. A bounded live replay worker scans the
//! same durable watermark while the project server is mounted; it only makes
//! convergence faster and is never the thing that makes it correct.
//!
//! # What Native does with these observations today
//!
//! Native declares `observation.accept.v1`, and its adapter recognises
//! `session.message_committed.v1` as a known kind paired with a known payload
//! contract — but it currently answers `capability_unsupported` with the
//! diagnostic `native.observation_staged` for every kind except its own fact
//! promotion. That is a real, typed, non-retryable terminal, and this journey
//! records it as one: the row settles `Rejected` after a single attempt rather
//! than retrying forever, and nothing here rounds a staged kind up to a
//! success. The delivery path is genuinely mounted and genuinely exercised;
//! what is not yet true is that Native *ingests* session messages.

use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};
use std::time::Duration;

use serde_json::{Map, Value};
use sha2::{Digest, Sha256};
use tracedecay_application::ResolvedScope;
use tracedecay_domain::{DurableObservationV1, ObservationScopeV1, ProjectId, UserProfileId};
use tracedecay_memory_hygiene::{
    HygieneError, ObservationAdmission, ObservationSanitizer, canonical_payload_bytes,
};
use tracedecay_memory_observation::{
    AdmissionDecisionV1, AdmittedObservationV1, CanonicalSettlementReceiptV1, DeliveryAttemptV1,
    DeliveryControlV1, DeliveryRuntimeV1, DeliveryWakeV1, DispatchPolicyV1, DispatchRequestV1,
    ForgetSourceKeyV1, IdempotencyInputV1, IngressBatchReportV1, IngressHaltV1, IngressRuntimeV1,
    LeaseRequestV1, LeasedObservationV1, OBSERVATION_CONTRACT_ID, ObservationAdmissionAdapterV1,
    ObservationIdV1, ObservationIdempotencyKeyV1, ObservationJournalError, ObservationPrivacyV1,
    ObservationRuntimeError, PrivacyClassificationV1, ProvenanceOriginV1,
    ProviderDeliveryAdapterV1, ProviderTargetV1, RetentionClassV1, RetentionPolicyV1,
    RetentionSweepScheduleV1, RetentionSweeperV1, RetentionTickV1, SanitizationBindingV1,
    ShutdownRequestV1, SourceAuthorityV1, SourceRecordV1, SourceSequenceV1, SourceStreamIdV1,
    SourceStreamKeyV1, SqliteObservationJournal, WakeOutcomeV1, WithheldAdmissionV1,
    extensions_digest,
};
use tracedecay_memory_provider_registry::{
    ApiError, CancellationToken, CanonicalPayload, FabricError, HandshakeRequest,
    HandshakeRequestParts, NATIVE_PROVIDER_ID, OperationControl, OwnedExactScope, OwnedProviderId,
    OwnedVersionedId, PayloadSanitizationReceipt, ProjectMemoryProviderComposition,
    ProjectMemoryProviderRegistry, ProviderCall, ProviderCallParts, ProviderLimits,
    ProviderOperation, ReadinessTargetError,
};
use tracedecay_runtime_core::cancellation::CancellationToken as HostCancellationToken;
use tracedecay_store::{
    ObservationAdmissionPort, ObservationReplayRequest, ObservationStoreError, StoredObservation,
};

/// File name of the project-owned observation journal inside the canonical
/// store layout. Placement only; never an identity input.
const JOURNAL_FILE_NAME: &str = "memory-observation-journal-v1.sqlite3";

/// Domain separator for the product-owned binding from a canonical source
/// session identity to a provider-qualified `agent_session_id`.
const AGENT_SESSION_BINDING_DOMAIN: &[u8] =
    b"tracedecay.memory-provider.agent-session-binding.v1\0";

/// Human-readable prefix so an operator reading a provider log can tell a bound
/// identity apart from a raw host session id.
const AGENT_SESSION_BINDING_PREFIX: &str = "tdmem-agent-session.v1.";

/// The observation kind and inner payload contract this journey admits, taken
/// from `product/observations/host-event-observation-policy.json` event class
/// `host.session_message_committed.v1` and matched by the Native adapter's own
/// kind/contract table.
const SESSION_MESSAGE_OBSERVATION_KIND: &str = "session.message_committed.v1";
const SESSION_MESSAGE_PAYLOAD_CONTRACT: &str = "tracedecay.memory.observation.session-message.v1";

/// `canonical_commit_point.point_id` for that event class.
const SESSION_COMMIT_POINT_ID: &str = "session_observation_store.commit";

/// The single source stream this journey replays: one canonical observation
/// sequence per project observation store.
const SESSION_SOURCE_STREAM: &str = "session_observation_store";

/// Bounded page size for canonical replay. The store caps replay at 1_000.
const REPLAY_PAGE_ITEMS: usize = 128;

/// Bounded startup replay budget. Convergence continues on the live replay
/// worker, so a large backlog never holds project open.
const REPLAY_STARTUP_PAGES: usize = 64;

/// Bounded live replay budget per pass. A larger backlog remains canonical and
/// is picked up by the next pass without monopolizing the async runtime.
const REPLAY_LIVE_PAGES: usize = 8;
/// Wall-clock budget for the inline startup replay pass in project open.
const STARTUP_REPLAY_BUDGET: Duration = Duration::from_secs(10);
/// Pause after a failed or halted live replay pass before the next attempt.
const LIVE_REPLAY_ERROR_BACKOFF: Duration = Duration::from_secs(5);
/// Wall-clock deadline one live replay pass runs under. A pass that reaches it
/// stops between records and the next pass resumes from the durable watermark
/// after the park: a yield, not a fault.
const LIVE_REPLAY_PASS_BUDGET: Duration = Duration::from_secs(5);

/// Maximum time between canonical-store replay passes while the project server
/// is mounted. Startup replay remains the crash authority; this bounded poll
/// closes the live post-commit edge for every producer that writes the shared
/// registered store, including producers that do not run through the MCP server.
const LIVE_REPLAY_PARK: Duration = Duration::from_millis(250);

/// Deadline one readiness handshake is given. A handshake is proven per exact
/// scope and cached by the registry, so this bounds a rare call, not a batch.
const READINESS_DEADLINE_MICROS: i64 = 5_000_000;

/// Delivery deadline stamped on the journal envelope. Longer than one attempt's
/// deadline because it bounds the whole at-least-once lifetime of the row.
const ADMISSION_DEADLINE_MICROS: i64 = 86_400_000_000;

/// Every bound the mounted journey runs under, supplied by the composition
/// root through [`ObservationJourneyMountInputsV1`] and validated at mount.
///
/// Nothing in here is a library default: the retention policy bounds the
/// journal, the dispatch policy bounds one delivery round and must fit inside
/// the retention policy, and the cadences bound the worker's own loop.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct ObservationJourneyPolicyV1 {
    /// Queue size, attempt ceiling, backoff, ages, and sweep batch of the
    /// project-owned journal.
    pub(crate) retention: RetentionPolicyV1,
    /// Lease length, batch size, per-attempt budget, and reap budget of the
    /// delivery worker.
    pub(crate) dispatch: DispatchPolicyV1,
    /// How long the delivery worker parks between wakes. A missed wake still
    /// converges within this bound; it never replaces the wake.
    pub(crate) delivery_park: Duration,
    /// Cadence of the bounded retention sweep the delivery worker drives over
    /// the project journal. Each pass is bounded by the policy's
    /// `sweep_batch_rows`; a backlog is due again on the next worker turn.
    pub(crate) retention_sweep_interval_micros: i64,
    /// How long a failed or non-actionable sweep pass waits before retrying.
    pub(crate) retention_sweep_error_backoff_micros: i64,
}

impl ObservationJourneyPolicyV1 {
    /// The product's bounds for a project journey. Explicit, not defaulted:
    /// every value is a product decision the composition root passes through,
    /// and the composition root may pass a different validated policy instead.
    pub(crate) const fn project_default() -> Self {
        Self {
            retention: RetentionPolicyV1 {
                ephemeral_max_age_micros: 3_600_000_000,
                session_max_age_micros: 86_400_000_000,
                project_max_age_micros: 2_592_000_000_000,
                profile_max_age_micros: 2_592_000_000_000,
                receipt_retention_micros: 604_800_000_000,
                max_queue_items: 10_000,
                max_queue_bytes: 64 * 1_048_576,
                max_attempts: 8,
                backoff_base_micros: 1_000_000,
                backoff_max_micros: 300_000_000,
                sweep_batch_rows: 512,
            },
            dispatch: DispatchPolicyV1 {
                lease_duration_micros: 30_000_000,
                batch_max_items: 16,
                batch_max_bytes: 1_048_576,
                attempt_budget_micros: 5_000_000,
                reap_budget: 256,
            },
            delivery_park: Duration::from_millis(250),
            retention_sweep_interval_micros: 60_000_000,
            retention_sweep_error_backoff_micros: 300_000_000,
        }
    }

    /// Refuses a policy that cannot bound the worker. The retention policy is
    /// validated again by the journal at open; the dispatch policy must fit
    /// inside it; the park must be finite and non-zero so a missed wake still
    /// converges.
    fn validate(&self) -> Result<(), ObservationJourneyError> {
        self.retention
            .validate()
            .map_err(ObservationJourneyError::Journal)?;
        self.dispatch
            .validate_against(&self.retention)
            .map_err(ObservationJourneyError::Journal)?;
        if self.delivery_park.is_zero() {
            return Err(ObservationJourneyError::Journal(
                ObservationJournalError::InvalidDispatchPolicy {
                    field: "delivery_park",
                },
            ));
        }
        Ok(())
    }
}

/// Every way the mounted journey can refuse, typed.
#[derive(Debug, thiserror::Error)]
pub(crate) enum ObservationJourneyError {
    /// Composition is disabled, so there is no registry to mount against.
    #[error("provider composition is disabled, so no observation journey can mount")]
    CompositionDisabled,
    /// The authoritative resolved scope carries no branch or detached
    /// reference, so no exact coding scope exists. The journey does not mount
    /// rather than inventing a branch identity from a path.
    #[error(
        "authoritative resolved scope for project {project_id} carries no reference, so no exact \
         coding scope exists for provider observation"
    )]
    ScopeReferenceUnavailable {
        /// Project whose scope could not be completed.
        project_id: String,
    },
    /// The mount inputs disagree with the authoritative scope. Nothing is
    /// re-derived to make them agree.
    #[error(
        "observation journey inputs disagree with the authoritative scope on {field}: expected \
         {expected}, received {received}"
    )]
    ScopeDisagreement {
        /// Which identity disagreed.
        field: &'static str,
        /// The authoritative value.
        expected: String,
        /// The value the caller supplied.
        received: String,
    },
    /// A provider-contract value was rejected.
    #[error("provider contract rejected an observation journey value: {0}")]
    Contract(#[source] ApiError),
    /// The readiness handshake did not produce a readiness target.
    #[error("provider readiness handshake did not complete: {0}")]
    Readiness(#[source] ReadinessTargetError),
    /// A journal-contract value was rejected.
    #[error("observation journal rejected a mount value: {0}")]
    Journal(#[source] ObservationJournalError),
    /// The journal file could not be opened.
    #[error("observation journal at {path} could not be opened: {source}")]
    JournalOpen {
        /// Storage placement of the journal, for diagnostics only.
        path: PathBuf,
        /// Underlying journal failure.
        #[source]
        source: ObservationJournalError,
    },
    /// The canonical hygiene policy could not be loaded.
    #[error("observation hygiene policy is unavailable: {0}")]
    Hygiene(#[source] HygieneError),
    /// System entropy was unavailable, so no unforgeable challenge nonce could
    /// be minted. A constant nonce would make the handshake replayable, so the
    /// mount fails instead of substituting one.
    #[error("system entropy is unavailable, so no readiness challenge could be minted")]
    EntropyUnavailable,
    /// The delivery worker thread could not start.
    #[error("observation delivery worker could not start: {0}")]
    Worker(#[source] std::io::Error),
    /// Canonical replay failed.
    #[error("canonical observation replay failed: {0}")]
    Replay(#[source] ObservationStoreError),
    /// Ingress refused a batch.
    #[error("observation ingress refused a canonical batch: {0}")]
    Ingress(#[source] ObservationRuntimeError),
    /// The blocking-pool task that recovers and ingests one record did not
    /// complete. The record's own transaction either committed or did not; the
    /// next pass re-presents it from the watermark either way.
    #[error("observation ingest task did not complete: {0}")]
    IngestTask(#[source] tokio::task::JoinError),
    /// The caller's cancellation token was cancelled between records. Every
    /// record is its own journal transaction, so the durable watermark holds
    /// exactly the records admitted before the cancellation was observed.
    #[error(
        "canonical observation replay was cancelled after admitting {admitted} records; the \
         durable watermark is preserved"
    )]
    Cancelled {
        /// Records this pass admitted before it observed the cancellation.
        admitted: u64,
    },
    /// The caller's deadline elapsed between records. Same durability as
    /// [`Self::Cancelled`]: nothing past the watermark is lost.
    #[error(
        "canonical observation replay stopped at its deadline after admitting {admitted} \
         records; the durable watermark is preserved"
    )]
    DeadlineExceeded {
        /// Records this pass admitted before it reached the deadline.
        admitted: u64,
    },
}

/// One way the mounted journey failed to stop cleanly.
///
/// Returned to the daemon's shutdown status rather than only logged, so an
/// unclean stop surfaces as `Failed` instead of hiding behind `Clean`.
#[derive(Debug, thiserror::Error)]
pub(crate) enum ObservationShutdownFailureV1 {
    /// The live replay task ended with something other than a cancellation.
    #[error("the canonical observation replay task did not exit cleanly: {0}")]
    LiveReplayJoin(#[source] tokio::task::JoinError),
    /// The live replay task did not stop inside the daemon deadline.
    #[error(
        "the canonical observation replay task did not stop within the daemon shutdown deadline"
    )]
    LiveReplayDeadline,
    /// The delivery worker thread panicked.
    #[error("the observation delivery worker did not exit cleanly")]
    WorkerPanicked,
    /// The blocking join of the delivery worker failed.
    #[error("the observation delivery worker join task failed: {0}")]
    WorkerJoin(#[source] tokio::task::JoinError),
    /// The delivery worker did not stop inside the daemon deadline.
    #[error("the observation delivery worker did not stop within the daemon shutdown deadline")]
    WorkerDeadline,
    /// Leases were still held after the bounded reap.
    #[error(
        "{leases_outstanding} observation delivery leases remain outstanding after shutdown \
         ({leases_reaped} reaped)"
    )]
    LeasesOutstanding {
        /// Leases the bounded reap released.
        leases_reaped: u32,
        /// Leases still held when the reap budget ran out.
        leases_outstanding: u64,
    },
    /// The journal's own shutdown pass failed.
    #[error("observation delivery shutdown pass failed: {0}")]
    ShutdownPass(#[source] ObservationRuntimeError),
}

/// The narrow product-owned binding from a canonical source session identity to
/// the provider-qualified `agent_session_id`.
///
/// The provider is given a derived identity rather than the host's own session
/// id, for two reasons that both matter to correctness and not only to privacy:
///
/// * the binding is **domain separated and deterministic**, so the same session
///   in the same exact checkout always yields the same provider-visible
///   identity across restarts, replays, and provider re-registration — which is
///   what keeps the content-derived idempotency key stable;
/// * the binding **absorbs the whole checkout identity**, so the same host
///   session observed from a different profile, project, repository, worktree,
///   or reference is a *different* provider identity. A provider therefore
///   cannot correlate one agent session across checkouts it was never scoped
///   to.
///
/// Every input is length-framed before it is absorbed, so no two different
/// tuples can produce the same preimage by shifting a separator.
pub(super) fn provider_agent_session_id(
    profile_id: &UserProfileId,
    scope: &ResolvedScope,
    canonical_session_id: &str,
) -> String {
    let mut digest = Sha256::new();
    digest.update(AGENT_SESSION_BINDING_DOMAIN);
    for value in [
        profile_id.as_str().as_bytes(),
        scope.project_id.as_str().as_bytes(),
        scope.repository_id.as_str().as_bytes(),
        scope.worktree_id.as_str().as_bytes(),
        scope
            .reference
            .as_ref()
            .map_or(&b""[..], |reference| reference.as_str().as_bytes()),
        scope.scope_digest.as_str().as_bytes(),
        canonical_session_id.as_bytes(),
    ] {
        absorb(&mut digest, value);
    }
    format!(
        "{AGENT_SESSION_BINDING_PREFIX}{}",
        hex::encode(digest.finalize())
    )
}

/// Builds the exact coding scope for one canonical session, copying the
/// authoritative resolved scope verbatim.
///
/// `scope_digest` is already an algorithm-tagged `sha256:` digest, which is
/// exactly the shape `OwnedExactScope` requires of `resolved_scope_digest`; it
/// is passed through untouched rather than re-tagged or re-hashed.
fn exact_scope_for_session(
    profile_id: &UserProfileId,
    scope: &ResolvedScope,
    canonical_session_id: &str,
) -> Result<OwnedExactScope, ObservationJourneyError> {
    let reference = scope.reference.as_ref().ok_or_else(|| {
        ObservationJourneyError::ScopeReferenceUnavailable {
            project_id: scope.project_id.as_str().to_owned(),
        }
    })?;
    OwnedExactScope::new(
        profile_id.as_str(),
        scope.project_id.as_str(),
        scope.repository_id.as_str(),
        scope.worktree_id.as_str(),
        reference.as_str(),
        provider_agent_session_id(profile_id, scope, canonical_session_id),
        scope.scope_digest.as_str(),
    )
    .map_err(ObservationJourneyError::Contract)
}

/// Everything the admission adapter needs that does not change per record.
struct AdmissionContextV1 {
    profile_id: UserProfileId,
    scope: ResolvedScope,
    composition: Arc<ProjectMemoryProviderComposition>,
    registration_revision: u64,
    limits: ProviderLimits,
    observe_capability: OwnedVersionedId,
    sanitizer: ObservationSanitizer,
    observation_kind: OwnedVersionedId,
    provider_payload_contract: OwnedVersionedId,
}

/// Turns one canonical `StoredObservation` into an admission decision.
///
/// The order is fixed by the observation contract and enforced here, not
/// documented and hoped for: hygiene runs *before* any journal digest or
/// idempotency key is derived, and the key is derived over the sanitized bytes
/// that will actually be delivered. Nothing in this adapter writes to, mutates,
/// or deletes canonical evidence — a secret-bearing record produces a withheld
/// audit row in the journal and leaves the canonical observation exactly as the
/// host settled it.
struct CanonicalObservationAdmissionAdapterV1 {
    context: AdmissionContextV1,
}

/// Typed admission refusals. None of them is a silent drop: each one stops
/// ingress at the offending record with that record's own identity attached.
#[derive(Debug, thiserror::Error)]
enum AdmissionAdapterError {
    #[error(
        "canonical observation {source_event_id} is scoped outside the mounted project, so it \
         cannot be admitted under this journey's exact scope"
    )]
    ScopeMismatch {
        /// Settled event identity that could not be admitted.
        source_event_id: String,
    },
    #[error("canonical observation {source_event_id} has no usable exact coding scope: {source}")]
    ExactScope {
        /// Settled event identity that could not be admitted.
        source_event_id: String,
        /// Underlying refusal.
        #[source]
        source: ObservationJourneyError,
    },
    #[error("hygiene could not decide canonical observation {source_event_id}: {source}")]
    Hygiene {
        /// Settled event identity that could not be decided.
        source_event_id: String,
        /// Underlying refusal.
        #[source]
        source: HygieneError,
    },
    #[error("provider envelope for {source_event_id} could not be canonically encoded: {source}")]
    CanonicalEncoding {
        /// Settled event identity that could not be encoded.
        source_event_id: String,
        /// Underlying refusal.
        #[source]
        source: HygieneError,
    },
    #[error("hygiene rewrote the provider envelope shape for {source_event_id}")]
    EnvelopeShapeRewritten {
        /// Settled event identity whose envelope no longer parses.
        source_event_id: String,
    },
    #[error("journal contract refused the decision for {source_event_id}: {source}")]
    Journal {
        /// Settled event identity the journal refused.
        source_event_id: String,
        /// Underlying refusal.
        #[source]
        source: ObservationJournalError,
    },
    #[error("provider contract refused the admitted payload for {source_event_id}: {source}")]
    Payload {
        /// Settled event identity whose payload was refused.
        source_event_id: String,
        /// Underlying refusal.
        #[source]
        source: ApiError,
    },
    #[error("provider readiness could not be proven for {source_event_id}: {source}")]
    Readiness {
        /// Settled event identity whose exact scope was not accepted.
        source_event_id: String,
        /// Underlying refusal.
        #[source]
        source: ObservationJourneyError,
    },
}

impl ObservationAdmissionAdapterV1 for CanonicalObservationAdmissionAdapterV1 {
    type Record = StoredObservation;
    type Error = AdmissionAdapterError;

    fn decide(
        &self,
        record: &SourceRecordV1<Self::Record>,
    ) -> Result<AdmissionDecisionV1, Self::Error> {
        let context = &self.context;
        let stored = &record.record;
        let observation = stored.observation();
        let source_event_id = record.source_event_id.clone();

        // The canonical record must already belong to the mounted project. One
        // that does not is refused, never re-scoped: re-scoping would deliver
        // another project's content under this project's exact scope.
        let scoped_here = matches!(
            observation.scope(),
            ObservationScopeV1::Project { project_id } if project_id == &context.scope.project_id
        );
        if !scoped_here {
            return Err(AdmissionAdapterError::ScopeMismatch { source_event_id });
        }
        let exact_scope = exact_scope_for_session(
            &context.profile_id,
            &context.scope,
            observation.source().session_id().as_str(),
        )
        .map_err(|source| AdmissionAdapterError::ExactScope {
            source_event_id: source_event_id.clone(),
            source,
        })?;

        // The provider sees the observation envelope, not the bare canonical
        // payload, so hygiene has to run over the envelope: the sanitization
        // receipt binds the exact bytes that will be delivered, and a receipt
        // minted over the inner payload alone would not describe them. The
        // sanitizer walks the whole structure, so a secret nested anywhere in
        // the canonical payload is still found.
        let envelope = provider_observation_envelope(
            context.observation_kind.as_str(),
            SESSION_MESSAGE_PAYLOAD_CONTRACT,
            observation.payload(),
        );
        // A settled record whose *shape* hygiene will not walk — nested or
        // sized beyond the ceilings the store itself never lets a record reach
        // — has been classified as nothing, so it is withheld under a typed
        // reason rather than refused: a refusal here would stall the replay
        // cursor on evidence the host already settled and repeat on every
        // open. Every other hygiene error stays a refusal, because a detector
        // fault must keep failing closed and a caller bug must stay visible.
        let admission = context
            .sanitizer
            .admit_observation(&envelope, &[])
            .or_else(|error| {
                let terminal = context
                    .sanitizer
                    .withhold_unclassifiable(&envelope, &[], error)?;
                tracing::warn!(
                    event = "memory_observation_unclassifiable_record_withheld",
                    source_event_id = %source_event_id,
                    "settled canonical record lies beyond the hygiene ceilings; withheld \
                     without classification, canonical evidence untouched"
                );
                Ok(terminal)
            })
            .map_err(|source| AdmissionAdapterError::Hygiene {
                source_event_id: source_event_id.clone(),
                source,
            })?;

        let settlement = canonical_settlement_receipt(record, stored);
        let forget_source_key =
            forget_source_key_for(&exact_scope, observation).map_err(|source| {
                AdmissionAdapterError::Journal {
                    source_event_id: source_event_id.clone(),
                    source,
                }
            })?;

        match admission {
            ObservationAdmission::Withheld {
                reason,
                receipt_id,
                source_payload_sha256,
                extensions_digest,
                sanitizer_revision,
                finding_count,
                findings_digest,
            } => {
                // Digests and a typed reason only. The canonical evidence the
                // host settled is not touched; the withheld row advances the
                // replay cursor so a refused event is not re-emitted forever,
                // and `source_event_id` still points at the untouched record.
                let withheld = WithheldAdmissionV1 {
                    source_authority: record.stream.source_authority.as_wire().to_owned(),
                    exact_scope_sha256: exact_scope.exact_scope_sha256(),
                    source_stream: record.stream.source_stream.as_str().to_owned(),
                    source_sequence: record.source_sequence.0,
                    source_event_id: source_event_id.clone(),
                    source_event_revision: record.source_event_revision.to_string(),
                    receipt_id,
                    reason: reason.as_str().to_owned(),
                    source_payload_sha256,
                    extensions_digest,
                    sanitizer_revision,
                    finding_count,
                    findings_digest,
                    forget_source_key,
                };
                withheld
                    .validate()
                    .map_err(|source| AdmissionAdapterError::Journal {
                        source_event_id,
                        source,
                    })?;
                Ok(AdmissionDecisionV1::Withhold(Box::new(withheld)))
            }
            ObservationAdmission::Admitted { sanitized, receipt } => {
                // Hygiene may redact spans inside the payload; it must not have
                // turned the envelope into something the provider cannot parse.
                // Checking is one map lookup, and the alternative is a dispatch
                // that fails at the provider with a contract violation nobody
                // can attribute back to redaction.
                if !envelope_shape_survived(&sanitized, context.observation_kind.as_str()) {
                    return Err(AdmissionAdapterError::EnvelopeShapeRewritten { source_event_id });
                }
                let bytes = canonical_payload_bytes(&sanitized).map_err(|source| {
                    AdmissionAdapterError::CanonicalEncoding {
                        source_event_id: source_event_id.clone(),
                        source,
                    }
                })?;
                let payload_sha256 = receipt.sanitized_payload_sha256().to_owned();
                let payload = CanonicalPayload::new(
                    context.provider_payload_contract.clone(),
                    bytes,
                    payload_sha256.clone(),
                )
                .map_err(|source| AdmissionAdapterError::Payload {
                    source_event_id: source_event_id.clone(),
                    source,
                })?;
                let extensions = Vec::new();
                let extensions_digest = extensions_digest(&extensions).map_err(|source| {
                    AdmissionAdapterError::Journal {
                        source_event_id: source_event_id.clone(),
                        source,
                    }
                })?;
                let sanitization = SanitizationBindingV1 {
                    receipt_id: receipt.receipt_id().to_owned(),
                    sanitizer_revision: receipt.sanitizer_revision().to_owned(),
                    source_payload_sha256: receipt.source_payload_sha256().to_owned(),
                    receipt_json: receipt.to_json(),
                };

                let target = readiness_target_for_scope(
                    &context.composition,
                    &exact_scope,
                    context.registration_revision,
                    context.limits,
                    context.observe_capability.clone(),
                    readiness_control(),
                )
                .map_err(|source| AdmissionAdapterError::Readiness {
                    source_event_id: source_event_id.clone(),
                    source,
                })?;
                let admitted_at_unix_micros = tracedecay_application::now_micros().0;
                let occurred_at_unix_micros = settlement.settled_at_unix_micros;
                let privacy = ObservationPrivacyV1 {
                    classification: PrivacyClassificationV1::Sensitive,
                    retention_class: RetentionClassV1::Session,
                    redaction_revision: 1,
                    content_policy_revision: 1,
                    forget_source_key,
                    expires_at_unix_micros: admitted_at_unix_micros
                        .saturating_add(ADMISSION_DEADLINE_MICROS),
                };
                let idempotency_key = ObservationIdempotencyKeyV1::derive(&IdempotencyInputV1 {
                    contract_id: OBSERVATION_CONTRACT_ID,
                    provider_id: target.provider_id.as_str(),
                    registration_revision: target.registration_revision,
                    exact_scope_sha256: &exact_scope.exact_scope_sha256(),
                    source_authority: settlement.source_authority,
                    source_event_id: &settlement.source_event_id,
                    source_event_revision: settlement.source_event_revision,
                    observation_kind: context.observation_kind.as_str(),
                    payload_contract: payload.contract_id.as_str(),
                    payload_sha256: &payload_sha256,
                    extensions_digest: &extensions_digest,
                });
                let observation_id =
                    mint_observation_id(admitted_at_unix_micros).map_err(|source| {
                        AdmissionAdapterError::Journal {
                            source_event_id: source_event_id.clone(),
                            source,
                        }
                    })?;
                let mut admitted = AdmittedObservationV1 {
                    observation_id,
                    idempotency_key,
                    target,
                    exact_scope,
                    source: settlement,
                    observation_kind: context.observation_kind.clone(),
                    payload,
                    extensions,
                    extensions_digest,
                    provenance_origin: ProvenanceOriginV1::Agent,
                    provenance_sha256: canonical_provenance_digest(stored),
                    privacy,
                    sanitization,
                    occurred_at_unix_micros,
                    admitted_at_unix_micros,
                    deadline_unix_micros: admitted_at_unix_micros
                        .saturating_add(ADMISSION_DEADLINE_MICROS),
                    request_id: format!("observe.{}", record.source_sequence.0),
                    envelope_sha256: String::new(),
                };
                admitted.envelope_sha256 = admitted.expected_envelope_sha256();
                admitted
                    .validate()
                    .map_err(|source| AdmissionAdapterError::Journal {
                        source_event_id,
                        source,
                    })?;
                Ok(AdmissionDecisionV1::Admit(Box::new(admitted)))
            }
        }
    }
}

/// Builds the provider observation envelope the Native adapter parses.
fn provider_observation_envelope(
    observation_kind: &str,
    payload_contract: &str,
    canonical_payload: &Value,
) -> Value {
    let mut envelope = Map::new();
    envelope.insert("canonical_payload".to_owned(), canonical_payload.clone());
    envelope.insert(
        "observation_kind".to_owned(),
        Value::String(observation_kind.to_owned()),
    );
    envelope.insert(
        "payload_contract".to_owned(),
        Value::String(payload_contract.to_owned()),
    );
    Value::Object(envelope)
}

/// Whether the sanitized envelope still carries the three fields the provider
/// adapter requires, with the kind and contract unrewritten.
fn envelope_shape_survived(sanitized: &Value, observation_kind: &str) -> bool {
    let Some(object) = sanitized.as_object() else {
        return false;
    };
    object.get("observation_kind").and_then(Value::as_str) == Some(observation_kind)
        && object.get("payload_contract").and_then(Value::as_str)
            == Some(SESSION_MESSAGE_PAYLOAD_CONTRACT)
        && object
            .get("canonical_payload")
            .is_some_and(|payload| !payload.is_null())
}

/// Mints a UUIDv7 observation identity from real entropy.
///
/// The stamp is the admission instant; the ten trailing bytes come from the
/// operating system. Feeding a counter here would collide on the journal's
/// unique index, so the entropy is not decorative — and an entropy failure is
/// reported rather than papered over with a constant.
fn mint_observation_id(
    admitted_at_unix_micros: i64,
) -> Result<ObservationIdV1, ObservationJournalError> {
    let unix_millis = u64::try_from(admitted_at_unix_micros.max(0) / 1_000).unwrap_or(0);
    let mut entropy = [0u8; 10];
    if getrandom::getrandom(&mut entropy).is_err() {
        return Err(ObservationJournalError::InvalidObservationId {
            detail: "system entropy is unavailable for observation identity".to_owned(),
        });
    }
    ObservationIdV1::from_v7_parts(unix_millis, entropy)
}

/// The forget-source key a privacy deletion targets: the exact scope plus the
/// canonical session the content came from. Deleting one agent session's
/// provider copies must not reach another session in the same project.
fn forget_source_key_for(
    exact_scope: &OwnedExactScope,
    observation: &DurableObservationV1,
) -> Result<ForgetSourceKeyV1, ObservationJournalError> {
    ForgetSourceKeyV1::new(format!(
        "session:{}:{}",
        exact_scope.exact_scope_sha256(),
        observation.source().session_id().as_str()
    ))
}

/// Copies the canonical commit receipt into the journal's settlement proof.
///
/// Every field is carried over from what the host authority already settled.
/// Nothing here mints a settlement the store did not report — that is exactly
/// the observation contract's `reject_not_canonically_settled`.
fn canonical_settlement_receipt(
    record: &SourceRecordV1<StoredObservation>,
    stored: &StoredObservation,
) -> CanonicalSettlementReceiptV1 {
    CanonicalSettlementReceiptV1 {
        source_authority: record.stream.source_authority,
        commit_point_id: SESSION_COMMIT_POINT_ID.to_owned(),
        source_event_id: record.source_event_id.clone(),
        source_event_revision: record.source_event_revision,
        source_event_sha256: canonical_source_event_digest(stored),
        source_stream: record.stream.source_stream.clone(),
        source_sequence: record.source_sequence,
        settled_at_unix_micros: stored.retrieval_anchor().ingested_at().0,
        settlement_proof_sha256: canonical_settlement_proof_digest(stored),
    }
}

/// Digest of the settled source event: the canonical observation identity, the
/// payload the store durably holds, and the position it settled at.
fn canonical_source_event_digest(stored: &StoredObservation) -> String {
    let observation = stored.observation();
    let mut digest = Sha256::new();
    digest.update(b"tracedecay.memory-provider.canonical-source-event.v1\0");
    absorb(
        &mut digest,
        observation.observation_id().as_str().as_bytes(),
    );
    absorb(
        &mut digest,
        observation.payload_reference().digest().as_str().as_bytes(),
    );
    absorb(&mut digest, &stored.sequence().to_be_bytes());
    hex::encode(digest.finalize())
}

/// Digest over exactly the `receipt_fields` the host-event observation policy
/// names for the `session_observation_store.commit` point.
fn canonical_settlement_proof_digest(stored: &StoredObservation) -> String {
    let observation = stored.observation();
    let identity = observation.identity();
    let mut digest = Sha256::new();
    digest.update(b"tracedecay.memory-provider.canonical-settlement-proof.v1\0");
    // durable_observation_id
    absorb(
        &mut digest,
        observation.observation_id().as_str().as_bytes(),
    );
    // source_cursor
    absorb(
        &mut digest,
        &stored.committed_cursor().position().to_be_bytes(),
    );
    // source_generation
    absorb(
        &mut digest,
        &identity.generation().generation_id().to_be_bytes(),
    );
    // source_range
    absorb(&mut digest, &identity.position().start().to_be_bytes());
    absorb(&mut digest, &identity.position().end().to_be_bytes());
    // settlement_digest: the retained sanitization receipt the store settled
    // under. `resolved_scope_digest` is bound separately, through the exact
    // scope the envelope digest already absorbs.
    absorb(
        &mut digest,
        stored
            .sanitization_receipt()
            .receipt()
            .receipt_id()
            .as_str()
            .as_bytes(),
    );
    hex::encode(digest.finalize())
}

/// Digest over the provenance the admitting authority holds. The provider gets
/// the digest, never the retained provenance record itself.
fn canonical_provenance_digest(stored: &StoredObservation) -> String {
    let mut digest = Sha256::new();
    digest.update(b"tracedecay.memory-provider.canonical-provenance.v1\0");
    absorb(
        &mut digest,
        stored.retrieval_anchor_id().as_str().as_bytes(),
    );
    absorb(
        &mut digest,
        stored.projection_generation().as_str().as_bytes(),
    );
    absorb(
        &mut digest,
        stored.observation().source().provider().as_str().as_bytes(),
    );
    hex::encode(digest.finalize())
}

/// Length-frames one field so no two field boundaries can collide.
fn absorb(digest: &mut Sha256, value: &[u8]) {
    digest.update(u64::try_from(value.len()).unwrap_or(u64::MAX).to_be_bytes());
    digest.update(value);
}

/// Delivers one leased observation through the registry, and only through the
/// registry.
///
/// The call is built from the leased row's own bytes, extensions, stored
/// sanitization binding, exact scope, idempotency key, and deadline, plus the
/// readiness evidence currently held. Nothing is re-read from the canonical
/// store at delivery time: the provider must see exactly the bytes the journal
/// committed, or its `payload_sha256` comparison would not match the receipt
/// the journal stores.
struct RegistryObservationDeliveryAdapterV1 {
    composition: Arc<ProjectMemoryProviderComposition>,
    registration_revision: u64,
    limits: ProviderLimits,
    observe_capability: OwnedVersionedId,
}

/// Typed delivery refusals. Every one of them produces no receipt, which is
/// what makes the attempt redeliverable rather than settled.
#[derive(Debug, thiserror::Error)]
enum DeliveryAdapterError {
    #[error("provider composition is disabled, so no observation can be delivered")]
    Disabled,
    #[error("provider readiness could not be proven for the leased exact scope: {0}")]
    Readiness(#[source] ObservationJourneyError),
    #[error("stored sanitization receipt could not be reattached: {0}")]
    Sanitization(#[source] ApiError),
    #[error("observation call could not be built: {0}")]
    Call(#[source] ApiError),
    #[error("registry refused the observation: {0}")]
    Fabric(#[source] FabricError),
}

impl RegistryObservationDeliveryAdapterV1 {
    fn registry(&self) -> Result<&ProjectMemoryProviderRegistry, DeliveryAdapterError> {
        self.composition
            .registry()
            .ok_or(DeliveryAdapterError::Disabled)
    }
}

impl ProviderDeliveryAdapterV1 for RegistryObservationDeliveryAdapterV1 {
    type Error = DeliveryAdapterError;

    fn deliver(
        &self,
        leased: &LeasedObservationV1,
        control: &DeliveryControlV1,
    ) -> Result<DeliveryAttemptV1, Self::Error> {
        // The attempt's bound is the runtime's, never minted here: its
        // deadline is already the tightest of the dispatch budget, the lease
        // expiry, and the row's own delivery deadline, and its token is the
        // wake edge's, cancelled at shutdown. Readiness and the observation
        // call both run under it, so a shutdown reaches a provider that is
        // inside either.
        let started_at_unix_micros = tracedecay_application::now_micros().0;
        let operation_control = |now: i64| {
            OperationControl::new(
                control.deadline_unix_micros(),
                u64::try_from(control.remaining_micros(now) / 1_000).unwrap_or(u64::MAX),
                control.cancellation(),
            )
        };
        let readiness = readiness_target_for_scope(
            &self.composition,
            &leased.exact_scope,
            self.registration_revision,
            self.limits,
            self.observe_capability.clone(),
            operation_control(started_at_unix_micros),
        )
        .map_err(DeliveryAdapterError::Readiness)?;
        let registry = self.registry()?;
        let control = operation_control(tracedecay_application::now_micros().0);
        // The persisted receipt is reattached verbatim so the boundary check
        // runs against the exact hygiene evidence that admitted these bytes.
        let sanitization = PayloadSanitizationReceipt::from_json(&leased.sanitization.receipt_json)
            .map_err(DeliveryAdapterError::Sanitization)?;
        let call = ProviderCall::new(ProviderCallParts {
            operation: ProviderOperation::Observe,
            provider_id: readiness.provider_id.clone(),
            registration_revision: readiness.registration_revision,
            ready_receipt_sha256: readiness.ready_receipt_digest.clone(),
            exact_scope: leased.exact_scope.clone(),
            request_id: leased.observation_id.as_str().to_owned(),
            operation_id: format!(
                "{}.{}",
                leased.observation_id.as_str(),
                leased.attempt_number
            ),
            expected_state_generation: 0,
            idempotency_key: Some(leased.idempotency_key.as_str().to_owned()),
            control,
            payload: CanonicalPayload::new(
                leased.payload.contract_id.clone(),
                leased.payload.bytes.clone(),
                leased.payload.sha256.clone(),
            )
            .map_err(DeliveryAdapterError::Call)?,
            required_capabilities: vec![self.observe_capability.clone()],
            extensions: leased.extensions.clone(),
        })
        .map_err(DeliveryAdapterError::Call)?
        .with_sanitization(sanitization);
        call.validate_request_bytes(self.limits.request_bytes)
            .map_err(DeliveryAdapterError::Call)?;

        match registry.deliver_observation(&call) {
            Ok(receipt) => Ok(DeliveryAttemptV1::Answered {
                terminal: Box::new(receipt.terminal),
                started_at_unix_micros,
                finished_at_unix_micros: tracedecay_application::now_micros().0,
            }),
            Err(error) => Err(DeliveryAdapterError::Fabric(error)),
        }
    }
}

/// The bound an admission-time readiness proof runs under. Admission is
/// already bounded between records by the replay pass; this bounds the one
/// handshake call inside a record.
fn readiness_control() -> OperationControl {
    let now = tracedecay_application::now_micros().0;
    OperationControl::new(
        now.saturating_add(READINESS_DEADLINE_MICROS),
        u64::try_from(READINESS_DEADLINE_MICROS / 1_000).unwrap_or(u64::MAX),
        CancellationToken::new(),
    )
}

fn readiness_target_for_scope(
    composition: &ProjectMemoryProviderComposition,
    exact_scope: &OwnedExactScope,
    registration_revision: u64,
    host_limits: ProviderLimits,
    observe_capability: OwnedVersionedId,
    control: OperationControl,
) -> Result<ProviderTargetV1, ObservationJourneyError> {
    let registry = composition
        .registry()
        .ok_or(ObservationJourneyError::CompositionDisabled)?;
    let request = readiness_handshake_request(
        exact_scope,
        registration_revision,
        host_limits,
        observe_capability,
        control,
    )?;
    let readiness = registry
        .readiness_target(&request)
        .map_err(ObservationJourneyError::Readiness)?;
    let target = ProviderTargetV1 {
        provider_id: readiness.provider_id().clone(),
        provider_instance_id: readiness.provider_instance_id().to_owned(),
        registration_revision: readiness.registration_revision(),
        ready_receipt_digest: readiness.ready_receipt_sha256().to_owned(),
    };
    target
        .validate()
        .map_err(ObservationJourneyError::Journal)?;
    Ok(target)
}

/// Builds the readiness handshake request for one exact scope.
fn readiness_handshake_request(
    exact_scope: &OwnedExactScope,
    registration_revision: u64,
    host_limits: ProviderLimits,
    observe_capability: OwnedVersionedId,
    control: OperationControl,
) -> Result<HandshakeRequest, ObservationJourneyError> {
    let mut challenge_nonce = [0u8; 32];
    if getrandom::getrandom(&mut challenge_nonce).is_err() {
        return Err(ObservationJourneyError::EntropyUnavailable);
    }
    HandshakeRequest::new(HandshakeRequestParts {
        provider_id: OwnedProviderId::new(NATIVE_PROVIDER_ID)
            .map_err(ObservationJourneyError::Contract)?,
        registration_revision,
        exact_scope: exact_scope.clone(),
        request_id: format!("observation-readiness.{}", exact_scope.exact_scope_sha256()),
        required_capabilities: vec![observe_capability],
        host_limits,
        control,
        challenge_nonce,
    })
    .map_err(ObservationJourneyError::Contract)
}

/// Caller-owned bounds one replay pass runs under.
///
/// Both are propagated, never minted here: the composition root passes the
/// project-open cancellation and the startup budget, the live replay task
/// passes the journey's own stop token and its per-pass budget. Replay checks
/// both before every page and before every record; a record already handed to
/// the journal completes its own transaction.
#[derive(Clone, Copy, Debug)]
pub(crate) struct ReplayBoundsV1<'a> {
    /// Cancels the pass between records with a typed terminal.
    pub(crate) cancellation: &'a HostCancellationToken,
    /// Absolute deadline the pass stops at between records.
    pub(crate) deadline: tokio::time::Instant,
}

/// What one bounded replay pass did.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ReplayPassV1 {
    /// Canonical records this pass newly appended to the journal.
    pub(crate) admitted: u64,
    /// The typed journal refusal that stopped the pass, when one did. The
    /// per-stream watermark stays at the refused position: the journal is
    /// delivery-status authority only and cannot decide which of two settled
    /// events at one source position is the truthful one, so it never steps
    /// over the refusal.
    pub(crate) halted: Option<IngressHaltV1>,
}

/// The retained owner for one project's observation journey.
///
/// It holds the registry handle, the durable journal, the delivery wake edge,
/// and the worker thread. Dropping it without [`Self::shutdown`] still strands
/// nothing: every lease carries its own expiry and any process can reap it.
pub(crate) struct ProjectObservationJourneyV1 {
    journal: Arc<SqliteObservationJournal>,
    wake: Arc<DeliveryWakeV1>,
    source_stream: SourceStreamIdV1,
    adapter: Arc<CanonicalObservationAdmissionAdapterV1>,
    delivery: Arc<RegistryObservationDeliveryAdapterV1>,
    provider_id: String,
    provider_instance_id: String,
    registration_revision: u64,
    lease_owner: String,
    retention_sweep_schedule: RetentionSweepScheduleV1,
    dispatch_policy: DispatchPolicyV1,
    delivery_park: Duration,
    stopping: HostCancellationToken,
    worker: Mutex<Option<JoinHandle<()>>>,
    live_replay_task: Mutex<Option<tokio::task::JoinHandle<()>>>,
    /// The typed refusal the most recent replay pass stopped on, so a
    /// permanent halt is reported once rather than at the backoff rate.
    live_halt: Mutex<Option<IngressHaltV1>>,
    journal_path: PathBuf,
}

impl ProjectObservationJourneyV1 {
    /// Storage placement of the journal. Diagnostics only — never identity.
    pub(crate) fn journal_path(&self) -> &Path {
        &self.journal_path
    }

    /// Runs bounded canonical replay, admitting or withholding every settled
    /// observation after the journal watermark.
    ///
    /// This is the authoritative path. It is what runs at mount, and a live
    /// wakeup only calls it earlier. `bounds` are checked before every page and
    /// before every record. A journal refusal is returned typed *in* the pass
    /// rather than as an error: the pass did what it could, the watermark
    /// holds at the refused position, and the caller decides how to treat it.
    pub(crate) async fn replay_canonical_observations<S>(
        &self,
        store: &S,
        max_pages: usize,
        bounds: ReplayBoundsV1<'_>,
    ) -> Result<ReplayPassV1, ObservationJourneyError>
    where
        S: ObservationAdmissionPort + ?Sized,
    {
        let mut admitted = 0_u64;
        for _ in 0..max_pages {
            // Journey shutdown is not a caller terminal: every record is
            // journaled in its own transaction, so stopping between pages
            // loses nothing and the next pass resumes from the watermark.
            if self.stopping.is_cancelled() {
                break;
            }
            check_replay_bounds(bounds, admitted)?;
            let after_sequence = self.replay_watermark().await?;
            let request = ObservationReplayRequest::new(after_sequence, REPLAY_PAGE_ITEMS)
                .map_err(ObservationJourneyError::Replay)?;
            let page = match tokio::time::timeout_at(
                bounds.deadline,
                store.replay_admitted_observations(request),
            )
            .await
            {
                Ok(page) => page.map_err(ObservationJourneyError::Replay)?,
                Err(_elapsed) => {
                    return Err(ObservationJourneyError::DeadlineExceeded { admitted });
                }
            };
            if page.is_empty() {
                break;
            }

            for stored in page {
                if self.stopping.is_cancelled() {
                    return Ok(ReplayPassV1 {
                        admitted,
                        halted: None,
                    });
                }
                check_replay_bounds(bounds, admitted)?;
                let record = self.source_record(stored)?;
                let report = self.ingest_record(record).await?;
                admitted = admitted.saturating_add(u64::from(report.appended));
                if let Some(halt) = report.halted_on {
                    return Ok(ReplayPassV1 {
                        admitted,
                        halted: Some(halt),
                    });
                }
            }
        }
        Ok(ReplayPassV1 {
            admitted,
            halted: None,
        })
    }

    /// Reads the journey-wide replay watermark on the blocking pool: the
    /// journal connection sits behind a mutex the delivery worker also holds
    /// across fsync'd writes, so even a read may wait on disk.
    async fn replay_watermark(&self) -> Result<u64, ObservationJourneyError> {
        let journal = Arc::clone(&self.journal);
        let source_stream = self.source_stream.clone();
        let sequence = tokio::task::spawn_blocking(move || {
            journal.maximum_replay_sequence(SourceAuthorityV1::HostSession, &source_stream)
        })
        .await
        .map_err(ObservationJourneyError::IngestTask)?
        .map_err(ObservationJourneyError::Journal)?;
        Ok(sequence.map_or(0, |sequence| sequence.0))
    }

    /// Binds one settled canonical record to its exact per-session stream.
    fn source_record(
        &self,
        stored: StoredObservation,
    ) -> Result<SourceRecordV1<StoredObservation>, ObservationJourneyError> {
        let exact_scope = exact_scope_for_session(
            &self.adapter.context.profile_id,
            &self.adapter.context.scope,
            stored.observation().source().session_id().as_str(),
        )?;
        let stream = SourceStreamKeyV1 {
            source_authority: SourceAuthorityV1::HostSession,
            exact_scope_sha256: exact_scope.exact_scope_sha256(),
            source_stream: self.source_stream.clone(),
        };
        stream
            .validate()
            .map_err(ObservationJourneyError::Journal)?;
        Ok(SourceRecordV1 {
            stream,
            source_sequence: SourceSequenceV1(stored.sequence()),
            source_event_id: stored.observation().observation_id().as_str().to_owned(),
            source_event_revision: 1,
            record: stored,
        })
    }

    /// Recovers the record's stream position and ingests it on the blocking
    /// pool. Recovery and ingest are synchronous SQLite work under a
    /// `synchronous = FULL` journal behind a mutex, and admission walks the
    /// whole envelope through hygiene; none of it may park a runtime worker
    /// that serves the project, whether the caller is project open or the
    /// live replay task.
    async fn ingest_record(
        &self,
        record: SourceRecordV1<StoredObservation>,
    ) -> Result<IngressBatchReportV1, ObservationJourneyError> {
        let journal = Arc::clone(&self.journal);
        let adapter = Arc::clone(&self.adapter);
        let wake = Arc::clone(&self.wake);
        tokio::task::spawn_blocking(move || {
            let ingress = IngressRuntimeV1::new(journal.as_ref(), adapter.as_ref(), wake.as_ref());
            let resume = ingress.recover(&record.stream)?;
            ingress.ingest(&resume, std::slice::from_ref(&record))
        })
        .await
        .map_err(ObservationJourneyError::IngestTask)?
        .map_err(ObservationJourneyError::Ingress)
    }

    /// Records the typed refusal a replay pass stopped on.
    ///
    /// Logged at error level only when it is a new halt, so a permanent
    /// refusal is reported once and then retried at the backoff rate without
    /// repeating itself at log rate.
    fn record_halt(&self, halt: IngressHaltV1) {
        let mut slot = match self.live_halt.lock() {
            Ok(slot) => slot,
            Err(poisoned) => poisoned.into_inner(),
        };
        if slot.as_ref() == Some(&halt) {
            tracing::trace!(
                event = "memory_observation_replay_still_halted",
                source_sequence = halt.source_sequence.0,
                "canonical replay is still halted at the same refused position"
            );
            return;
        }
        tracing::error!(
            event = "memory_observation_replay_halted",
            source_sequence = halt.source_sequence.0,
            source_event_id = %halt.source_event_id,
            outcome = ?halt.outcome,
            journal = %self.journal_path.display(),
            "canonical replay halted on a typed journal refusal; the watermark holds at the \
             refused position and replay backs off"
        );
        *slot = Some(halt);
    }

    /// Clears a recorded halt once a pass got past the refused position.
    fn clear_halt(&self) {
        let mut slot = match self.live_halt.lock() {
            Ok(slot) => slot,
            Err(poisoned) => poisoned.into_inner(),
        };
        if slot.take().is_some() {
            tracing::info!(
                event = "memory_observation_replay_halt_cleared",
                journal = %self.journal_path.display(),
                "canonical replay advanced past a previously refused position"
            );
        }
    }

    /// The typed refusal the most recent replay pass stopped on, if any.
    pub(crate) fn halted_on(&self) -> Option<IngressHaltV1> {
        match self.live_halt.lock() {
            Ok(slot) => slot.clone(),
            Err(poisoned) => poisoned.into_inner().clone(),
        }
    }

    /// Signals the delivery worker that new work landed.
    pub(crate) fn wake_delivery(&self) {
        self.wake.signal();
    }

    /// Starts the bounded live replay edge over the canonical registered store.
    ///
    /// The store is the same durable authority used by every session producer.
    /// Polling its journal watermark instead of decorating individual producers
    /// means commits made by daemon sync, historical refresh, hooks, or MCP
    /// replay all converge through one path. At most one task may be installed.
    pub(crate) fn start_live_replay<S>(
        self: &Arc<Self>,
        store: S,
    ) -> Result<(), ObservationJourneyError>
    where
        S: ObservationAdmissionPort + 'static,
    {
        let mut slot = self.live_replay_task.lock().map_err(|_| {
            ObservationJourneyError::Worker(std::io::Error::other("live replay task lock poisoned"))
        })?;
        if slot.is_some() {
            return Err(ObservationJourneyError::Worker(std::io::Error::other(
                "live replay task already started",
            )));
        }
        let weak = Arc::downgrade(self);
        let task = tokio::spawn(async move {
            loop {
                tokio::time::sleep(LIVE_REPLAY_PARK).await;
                let Some(journey) = weak.upgrade() else {
                    break;
                };
                if journey.stopping.is_cancelled() {
                    break;
                }
                let bounds = ReplayBoundsV1 {
                    cancellation: &journey.stopping,
                    deadline: tokio::time::Instant::now() + LIVE_REPLAY_PASS_BUDGET,
                };
                match journey
                    .replay_canonical_observations(&store, REPLAY_LIVE_PAGES, bounds)
                    .await
                {
                    Ok(pass) => {
                        if pass.admitted > 0 {
                            journey.wake_delivery();
                        }
                        match pass.halted {
                            Some(halt) => {
                                // The watermark stays at the refused position,
                                // so retrying at the park interval would only
                                // repeat the same refusal at log rate. Record
                                // it typed, once per distinct halt, and back
                                // off; a queue that drains clears on a later
                                // pass, a permanent conflict stays visible.
                                journey.record_halt(halt);
                                tokio::time::sleep(LIVE_REPLAY_ERROR_BACKOFF).await;
                            }
                            None => journey.clear_halt(),
                        }
                    }
                    Err(ObservationJourneyError::Cancelled { .. }) => break,
                    Err(ObservationJourneyError::DeadlineExceeded { admitted }) => {
                        if admitted > 0 {
                            journey.wake_delivery();
                        }
                        tracing::debug!(
                            event = "memory_observation_live_replay_pass_budget",
                            admitted,
                            "live replay pass stopped at its budget; the next pass resumes from \
                             the watermark"
                        );
                    }
                    Err(error) => {
                        tracing::warn!(
                            event = "memory_observation_live_replay_failed",
                            error = %error,
                            "bounded canonical observation replay failed"
                        );
                        // A transient store or journal fault clears on its
                        // own; back off so it is not retried at park rate.
                        tokio::time::sleep(LIVE_REPLAY_ERROR_BACKOFF).await;
                    }
                }
            }
        });
        *slot = Some(task);
        Ok(())
    }

    /// Stops the worker and reports truthfully whether anything is still held.
    ///
    /// The deadline is the daemon's own shared shutdown deadline. A worker that
    /// does not stop inside it, and a lease still outstanding after the bounded
    /// reap, are both returned as typed failures for the daemon's shutdown
    /// status rather than reported as a clean stop. Every stage still runs:
    /// one failure never skips the teardown after it.
    pub(crate) async fn shutdown(
        &self,
        deadline: tokio::time::Instant,
    ) -> Vec<ObservationShutdownFailureV1> {
        let mut failures = Vec::new();
        self.stopping.cancel();
        self.wake.request_shutdown();
        if let Some(halt) = self.halted_on() {
            // Not a shutdown failure: a standing replay condition the next
            // life meets again at the same durable watermark.
            tracing::warn!(
                event = "memory_observation_shutdown_with_halted_replay",
                source_sequence = halt.source_sequence.0,
                source_event_id = %halt.source_event_id,
                outcome = ?halt.outcome,
                journal = %self.journal_path.display(),
                "canonical replay is still halted at a refused position at shutdown"
            );
        }
        let live_replay_task = match self.live_replay_task.lock() {
            Ok(mut task) => task.take(),
            Err(poisoned) => poisoned.into_inner().take(),
        };
        if let Some(task) = live_replay_task {
            task.abort();
            match tokio::time::timeout_at(deadline, task).await {
                Ok(Ok(())) => {}
                Ok(Err(error)) if error.is_cancelled() => {}
                Ok(Err(error)) => {
                    failures.push(ObservationShutdownFailureV1::LiveReplayJoin(error));
                }
                Err(_) => failures.push(ObservationShutdownFailureV1::LiveReplayDeadline),
            }
        }
        let worker = match self.worker.lock() {
            Ok(mut worker) => worker.take(),
            Err(poisoned) => poisoned.into_inner().take(),
        };
        if let Some(worker) = worker {
            let join = tokio::task::spawn_blocking(move || worker.join());
            match tokio::time::timeout_at(deadline, join).await {
                Ok(Ok(Ok(()))) => {}
                Ok(Ok(Err(_))) => failures.push(ObservationShutdownFailureV1::WorkerPanicked),
                Ok(Err(error)) => failures.push(ObservationShutdownFailureV1::WorkerJoin(error)),
                Err(_) => failures.push(ObservationShutdownFailureV1::WorkerDeadline),
            }
        }
        let runtime = DeliveryRuntimeV1::new(
            self.journal.as_ref(),
            self.delivery.as_ref(),
            self.wake.as_ref(),
        );
        match runtime.shutdown(&ShutdownRequestV1 {
            provider_id: self.provider_id.clone(),
            now_unix_micros: tracedecay_application::now_micros().0,
            reap_budget: self.dispatch_policy.reap_budget,
        }) {
            Ok(report) if report.quiesced => {}
            Ok(report) => failures.push(ObservationShutdownFailureV1::LeasesOutstanding {
                leases_reaped: report.leases_reaped,
                leases_outstanding: report.leases_outstanding,
            }),
            Err(error) => failures.push(ObservationShutdownFailureV1::ShutdownPass(error)),
        }
        failures
    }

    fn spawn_worker(&self) -> Result<JoinHandle<()>, std::io::Error> {
        let journal = Arc::clone(&self.journal);
        let delivery = Arc::clone(&self.delivery);
        let wake = Arc::clone(&self.wake);
        let stopping = self.stopping.clone();
        let provider_id = self.provider_id.clone();
        let provider_instance_id = self.provider_instance_id.clone();
        let registration_revision = self.registration_revision;
        let lease_owner = self.lease_owner.clone();
        let retention_sweep_schedule = self.retention_sweep_schedule;
        let dispatch_policy = self.dispatch_policy;
        let delivery_park = self.delivery_park;
        // A dedicated OS thread rather than a tokio task: both the journal and
        // `deliver_observation` are synchronous and hold a mutex across the
        // whole call, so parking them on a runtime worker would block it.
        thread::Builder::new()
            .name("td-memory-observation".to_owned())
            .spawn(move || {
                let runtime =
                    DeliveryRuntimeV1::new(journal.as_ref(), delivery.as_ref(), wake.as_ref());
                // Due at once: a journal a restart found full of aged rows is
                // swept on the first turn, not after the first interval.
                let mut sweeper = RetentionSweeperV1::new(
                    journal.as_ref(),
                    retention_sweep_schedule,
                    tracedecay_application::now_micros().0,
                );
                while !stopping.is_cancelled() {
                    if runtime.wait_for_work(delivery_park) == WakeOutcomeV1::ShutdownRequested {
                        break;
                    }
                    let now = tracedecay_application::now_micros().0;
                    let request = DispatchRequestV1 {
                        lease: LeaseRequestV1 {
                            provider_id: provider_id.clone(),
                            registration_revision,
                            provider_instance_id: provider_instance_id.clone(),
                            exact_scope_sha256: None,
                            lease_owner: lease_owner.clone(),
                            now_unix_micros: now,
                            lease_duration_micros: dispatch_policy.lease_duration_micros,
                            max_items: dispatch_policy.batch_max_items,
                            max_bytes: dispatch_policy.batch_max_bytes,
                        },
                        // An adapter failure produced no provider answer, so
                        // the row comes back after the policy's first backoff
                        // step. The journal's own attempt ceiling still bounds
                        // it; nothing here retries a typed terminal.
                        retry_after_unix_micros: now
                            .saturating_add(journal.policy().backoff_base_micros),
                        attempt_budget_micros: dispatch_policy.attempt_budget_micros,
                    };
                    match runtime.dispatch_batch(&request) {
                        Ok(report) => {
                            if report.cancelled_before_dispatch > 0 || report.cancelled_in_flight > 0
                            {
                                tracing::info!(
                                    event = "memory_observation_dispatch_cancelled",
                                    leased = report.leased,
                                    cancelled_in_flight = report.cancelled_in_flight,
                                    cancelled_before_dispatch = report.cancelled_before_dispatch,
                                    "shutdown stopped an observation dispatch round; released rows stay pending"
                                );
                            }
                            for failure in &report.failures {
                                tracing::warn!(
                                    event = "memory_observation_delivery_failed",
                                    observation_id = %failure.observation_id.as_str(),
                                    attempt = failure.attempt_number,
                                    lease_released = failure.lease_released,
                                    error = %failure.cause,
                                    "one observation delivery produced no receipt"
                                );
                            }
                        }
                        Err(error) => {
                            // A journal-level failure is neither swallowed nor
                            // retried in a tight loop: the worker parks and the
                            // next wake tries again.
                            tracing::warn!(
                                event = "memory_observation_dispatch_failed",
                                error = %error,
                                "one observation dispatch round failed"
                            );
                        }
                    }
                    if let Err(error) = runtime.reap(
                        tracedecay_application::now_micros().0,
                        dispatch_policy.reap_budget,
                    ) {
                        tracing::warn!(
                            event = "memory_observation_reap_failed",
                            error = %error,
                            "lapsed observation leases could not be reaped"
                        );
                    }
                    // Retention is driven by the same loop that delivers, so
                    // an expired row is terminalized with a receipt and then
                    // purged by a mounted path — never left to a sweep nobody
                    // calls. The sweeper owns the cadence; a failure here is
                    // logged and waits out its backoff rather than looping.
                    match sweeper.tick(tracedecay_application::now_micros().0) {
                        Ok(RetentionTickV1::NotDue { .. }) => {}
                        Ok(RetentionTickV1::Swept {
                            receipt,
                            next_due_unix_micros,
                        }) => {
                            if receipt.remaining_candidates > 0
                                || receipt.payloads_purged > 0
                                || receipt.deliveries_expired > 0
                                || receipt.deliveries_forgotten > 0
                                || receipt.journal_rows_deleted > 0
                                || receipt.withheld_rows_deleted > 0
                            {
                                tracing::info!(
                                    event = "memory_observation_retention_swept",
                                    payloads_purged = receipt.payloads_purged,
                                    deliveries_expired = receipt.deliveries_expired,
                                    deliveries_forgotten = receipt.deliveries_forgotten,
                                    journal_rows_deleted = receipt.journal_rows_deleted,
                                    receipts_deleted = receipt.receipts_deleted,
                                    withheld_rows_deleted = receipt.withheld_rows_deleted,
                                    wal_truncated = receipt.wal_truncated,
                                    remaining_candidates = receipt.remaining_candidates,
                                    next_due_unix_micros,
                                    "observation journal retention sweep ran"
                                );
                            }
                        }
                        Err(error) => {
                            tracing::warn!(
                                event = "memory_observation_retention_sweep_failed",
                                error = %error,
                                next_due_unix_micros = sweeper.next_due_unix_micros(),
                                "observation journal retention sweep failed"
                            );
                        }
                    }
                }
            })
    }
}

impl Drop for ProjectObservationJourneyV1 {
    fn drop(&mut self) {
        self.stopping.cancel();
        self.wake.request_shutdown();
        let live_replay_task = match self.live_replay_task.get_mut() {
            Ok(task) => task.take(),
            Err(poisoned) => poisoned.into_inner().take(),
        };
        if let Some(task) = live_replay_task {
            task.abort();
        }
        let worker = match self.worker.get_mut() {
            Ok(worker) => worker.take(),
            Err(poisoned) => poisoned.into_inner().take(),
        };
        if let Some(worker) = worker
            && worker.join().is_err()
        {
            tracing::warn!(
                event = "memory_observation_worker_panicked",
                "the observation delivery worker panicked during owner drop"
            );
        }
    }
}

/// Inputs the composition root supplies to mount one project's journey.
pub(crate) struct ObservationJourneyMountInputsV1 {
    /// Enabled provider composition. A disabled composition is refused.
    pub(crate) composition: Arc<ProjectMemoryProviderComposition>,
    /// Authoritative profile identity.
    pub(crate) profile_id: UserProfileId,
    /// Authoritative resolved scope, used verbatim.
    pub(crate) scope: ResolvedScope,
    /// The authoritative project identity the composition root resolved
    /// independently of the scope. Checked against the scope rather than
    /// trusted, so one mount can never straddle two projects.
    pub(crate) authoritative_project_id: ProjectId,
    /// Canonical store-owned data root. Storage placement only.
    pub(crate) store_data_root: PathBuf,
    /// Product-owned registration revision the fabric registered under.
    pub(crate) registration_revision: u64,
    /// Host limits the handshake negotiates against.
    pub(crate) host_limits: ProviderLimits,
    /// Every bound the journey runs under. Validated at mount; a policy that
    /// cannot bound the worker refuses the mount.
    pub(crate) policy: ObservationJourneyPolicyV1,
}

/// Mounts one project's observation journey.
///
/// Order is enforced by the argument list: the caller cannot reach this
/// function without an authoritative resolved scope and an enabled composition.
/// Readiness is proved separately for each canonical record's own source-session
/// scope before admission and again immediately before delivery.
pub(crate) fn mount_project_observation_journey(
    inputs: ObservationJourneyMountInputsV1,
) -> Result<Arc<ProjectObservationJourneyV1>, ObservationJourneyError> {
    inputs
        .composition
        .registry()
        .ok_or(ObservationJourneyError::CompositionDisabled)?;
    if inputs.scope.project_id != inputs.authoritative_project_id {
        return Err(ObservationJourneyError::ScopeDisagreement {
            field: "project_id",
            expected: inputs.authoritative_project_id.as_str().to_owned(),
            received: inputs.scope.project_id.as_str().to_owned(),
        });
    }
    inputs.policy.validate()?;
    let observe_capability = OwnedVersionedId::new("observation.accept.v1")
        .map_err(ObservationJourneyError::Contract)?;
    let source_stream =
        SourceStreamIdV1::new(SESSION_SOURCE_STREAM).map_err(ObservationJourneyError::Journal)?;
    let lease_owner = format!(
        "tracedecay.daemon.observation.{}",
        inputs.scope.scope_digest.as_str()
    );

    let retention_sweep_schedule = RetentionSweepScheduleV1::bounded(
        inputs.policy.retention_sweep_interval_micros,
        inputs.policy.retention_sweep_error_backoff_micros,
    )
    .map_err(ObservationJourneyError::Journal)?;

    let journal_path = inputs.store_data_root.join(JOURNAL_FILE_NAME);
    let journal = SqliteObservationJournal::open(&journal_path, inputs.policy.retention).map_err(
        |source| ObservationJourneyError::JournalOpen {
            path: journal_path.clone(),
            source,
        },
    )?;
    let sanitizer = ObservationSanitizer::new().map_err(ObservationJourneyError::Hygiene)?;

    let adapter = Arc::new(CanonicalObservationAdmissionAdapterV1 {
        context: AdmissionContextV1 {
            profile_id: inputs.profile_id,
            scope: inputs.scope,
            composition: Arc::clone(&inputs.composition),
            registration_revision: inputs.registration_revision,
            limits: inputs.host_limits,
            observe_capability: observe_capability.clone(),
            sanitizer,
            observation_kind: OwnedVersionedId::new(SESSION_MESSAGE_OBSERVATION_KIND)
                .map_err(ObservationJourneyError::Contract)?,
            provider_payload_contract: OwnedVersionedId::new(OBSERVATION_CONTRACT_ID)
                .map_err(ObservationJourneyError::Contract)?,
        },
    });
    let delivery = Arc::new(RegistryObservationDeliveryAdapterV1 {
        composition: Arc::clone(&inputs.composition),
        registration_revision: inputs.registration_revision,
        limits: inputs.host_limits,
        observe_capability,
    });

    let journey = Arc::new(ProjectObservationJourneyV1 {
        journal: Arc::new(journal),
        wake: Arc::new(DeliveryWakeV1::new()),
        source_stream,
        adapter,
        provider_id: NATIVE_PROVIDER_ID.to_owned(),
        provider_instance_id: super::native_provider::PROVIDER_INSTANCE_ID.to_owned(),
        registration_revision: inputs.registration_revision,
        lease_owner,
        retention_sweep_schedule,
        dispatch_policy: inputs.policy.dispatch,
        delivery_park: inputs.policy.delivery_park,
        delivery,
        stopping: HostCancellationToken::new(),
        worker: Mutex::new(None),
        live_replay_task: Mutex::new(None),
        live_halt: Mutex::new(None),
        journal_path,
    });
    let worker = journey
        .spawn_worker()
        .map_err(ObservationJourneyError::Worker)?;
    match journey.worker.lock() {
        Ok(mut slot) => *slot = Some(worker),
        Err(poisoned) => *poisoned.into_inner() = Some(worker),
    }
    Ok(journey)
}

/// Refuses to start another record once the caller's bounds are spent.
fn check_replay_bounds(
    bounds: ReplayBoundsV1<'_>,
    admitted: u64,
) -> Result<(), ObservationJourneyError> {
    if bounds.cancellation.is_cancelled() {
        return Err(ObservationJourneyError::Cancelled { admitted });
    }
    if tokio::time::Instant::now() >= bounds.deadline {
        return Err(ObservationJourneyError::DeadlineExceeded { admitted });
    }
    Ok(())
}

/// The bounded startup replay the composition root runs once the journey is
/// mounted and the canonical observation store is reachable.
///
/// This is the authoritative convergence pass; the bounded live replay task
/// only makes later convergence faster. It runs inline in project open under
/// the project-open `cancellation` and its own wall-clock budget: the page
/// bound caps the work, the budget keeps a slow store from holding the open
/// hostage (past it the pass stops between records and the live replay task
/// continues from the watermark), and cancellation is the caller's terminal,
/// returned typed so the open reports it as the drain it is.
pub(crate) async fn run_startup_replay<S>(
    journey: &ProjectObservationJourneyV1,
    store: &S,
    cancellation: &HostCancellationToken,
) -> Result<ReplayPassV1, ObservationJourneyError>
where
    S: ObservationAdmissionPort + ?Sized,
{
    let bounds = ReplayBoundsV1 {
        cancellation,
        deadline: tokio::time::Instant::now() + STARTUP_REPLAY_BUDGET,
    };
    let pass = match journey
        .replay_canonical_observations(store, REPLAY_STARTUP_PAGES, bounds)
        .await
    {
        Ok(pass) => pass,
        Err(ObservationJourneyError::DeadlineExceeded { admitted }) => {
            tracing::warn!(
                event = "memory_observation_startup_replay_budget_exhausted",
                budget_millis = STARTUP_REPLAY_BUDGET.as_millis() as u64,
                admitted,
                journal = %journey.journal_path().display(),
                "startup canonical replay stopped at its time budget; live replay continues"
            );
            ReplayPassV1 {
                admitted,
                halted: None,
            }
        }
        Err(error) => return Err(error),
    };
    if pass.admitted > 0 {
        journey.wake_delivery();
    }
    if let Some(halt) = pass.halted.clone() {
        journey.record_halt(halt);
    }
    Ok(pass)
}

/// The whole product-owned mount sequence behind one call, so the composition
/// root holds a single seam: mount the journey, run the authoritative startup
/// replay under the project-open cancellation, then start the bounded live
/// replay edge over the same store.
///
/// Cancellation during startup replay is returned typed as
/// [`ObservationJourneyError::Cancelled`]; the journey is dropped with the
/// refused open and nothing past the durable watermark is lost. Any other
/// startup replay refusal is an observer-side edge over evidence the host
/// already settled, so it does not take the project server down: the journal
/// watermark does not advance past the refused record, the live replay task
/// retries it with backoff, and the failure is logged at error level with the
/// journal path. Mount and live-replay-start failures are returned typed.
pub(crate) async fn mount_and_replay<S>(
    inputs: ObservationJourneyMountInputsV1,
    observation_store: S,
    cancellation: &HostCancellationToken,
) -> Result<Arc<ProjectObservationJourneyV1>, ObservationJourneyError>
where
    S: ObservationAdmissionPort + 'static,
{
    let journey = mount_project_observation_journey(inputs)?;
    let admitted =
        match run_startup_replay(journey.as_ref(), &observation_store, cancellation).await {
            Ok(pass) => pass.admitted,
            Err(error @ ObservationJourneyError::Cancelled { .. }) => return Err(error),
            Err(error) => {
                tracing::error!(
                    event = "memory_observation_startup_replay_failed",
                    error = %error,
                    journal = %journey.journal_path().display(),
                    "project observation startup replay failed; the project server stays up and \
                     live replay will retry from the durable watermark"
                );
                0
            }
        };
    journey.start_live_replay(observation_store)?;
    tracing::info!(
        event = "memory_observation_startup_replay",
        admitted,
        journal = %journey.journal_path().display(),
        "project observation journey mounted"
    );
    Ok(journey)
}

#[cfg(test)]
mod tests {
    #![allow(clippy::expect_used, clippy::panic, clippy::unwrap_used)]

    use std::collections::BTreeSet;
    use std::sync::LazyLock;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use serde_json::json;
    use tempfile::TempDir;
    use tracedecay_application::ResolvedScope;
    use tracedecay_domain::{
        CanonicalMessageRoleV1, CanonicalObservationEnvelopeV1, CanonicalObservationEvidenceV1,
        CanonicalObservationFactV1, CanonicalObservationIdV1, CanonicalObservationRelationsV1,
        ComponentVersion, ObservationId, ObservationIdentityMaterialV1,
        ObservationOrderingDomainV1, ObservationSourceCursorV1, ObservationSourceGenerationV1,
        ObservationSourceIdentityV1, ObservationSourceRangeV1, PayloadReferenceV1,
        ProjectionGenerationId, ProviderId, RefId, RepositoryId, RetentionClass,
        SanitizationReceiptId, SanitizationReceiptRefV1, SanitizationReceiptV1,
        SanitizerDispositionV1, SensitivityV1, SessionId, UtcMicros, WorktreeId,
    };
    use tracedecay_memory_observation::AppendOutcomeV1;
    use tracedecay_memory_provider_registry::{
        CommittedEffectEvidence, EnabledProviderMode, FabricConfig, FallbackDirective,
        HandshakeResponse, NativeMemoryApplicationPort, NativeObservation,
        NativeProviderActivation, ProviderDescriptor, ProviderReply, TerminalCode, TerminalRecord,
    };
    use tracedecay_sessions::admission::HostAdmissionScope;
    use tracedecay_store::{
        AnchoredObservationWrite, ObservationStore, ObservationWrite,
        build_observation_resolution_authorization_v1, build_observation_retrieval_anchor_v2,
    };

    use super::*;
    use crate::host_admission::HostAdmissionTestRuntimeV1;
    use tracedecay_global_db::GlobalDbObservationStore;

    const READY_RECEIPT: &str = "1111111111111111111111111111111111111111111111111111111111111111";
    const PROVIDER_RECEIPT: &str =
        "2222222222222222222222222222222222222222222222222222222222222222";
    const EFFECT_DIGEST: &str = "3333333333333333333333333333333333333333333333333333333333333333";

    #[derive(Clone)]
    struct DeliveredObservation {
        bytes: Vec<u8>,
        exact_scope: OwnedExactScope,
    }

    struct JourneyNativePort {
        descriptor: ProviderDescriptor,
        observe_calls: AtomicUsize,
        delivered: Mutex<Vec<DeliveredObservation>>,
        handshake_hook: Mutex<Option<Box<dyn Fn() + Send + Sync>>>,
    }

    impl JourneyNativePort {
        fn new() -> Self {
            let capabilities = BTreeSet::from([
                OwnedVersionedId::new("provider.health.v1").expect("health capability"),
                OwnedVersionedId::new("observation.accept.v1").expect("observe capability"),
                OwnedVersionedId::new("recall.query.v1").expect("recall capability"),
            ]);
            let descriptor = ProviderDescriptor::new(
                OwnedProviderId::new(NATIVE_PROVIDER_ID).expect("native provider"),
                "0".repeat(64),
                "journey-test-v1",
                0,
                capabilities,
                super::super::native_provider::native_provider_limits(),
            )
            .expect("provider descriptor");
            Self {
                descriptor,
                observe_calls: AtomicUsize::new(0),
                delivered: Mutex::new(Vec::new()),
                handshake_hook: Mutex::new(None),
            }
        }

        /// Runs `hook` on every readiness handshake. Admission proves
        /// readiness for each record's own session scope, so this is the one
        /// point inside record admission a test can act from.
        fn on_handshake(&self, hook: impl Fn() + Send + Sync + 'static) {
            *self.handshake_hook.lock().unwrap() = Some(Box::new(hook));
        }

        fn unexpected<T>() -> T {
            panic!("journey test reached an unrelated provider operation")
        }
    }

    impl NativeMemoryApplicationPort for JourneyNativePort {
        fn descriptor(&self) -> ProviderDescriptor {
            self.descriptor.clone()
        }

        fn handshake(&self, request: &HandshakeRequest) -> HandshakeResponse {
            if let Some(hook) = self.handshake_hook.lock().unwrap().as_ref() {
                hook();
            }
            HandshakeResponse {
                terminal: TerminalRecord::new(
                    ProviderOperation::Handshake,
                    request.provider_id.clone(),
                    TerminalCode::Success,
                    CommittedEffectEvidence::none(Some(self.descriptor.state_generation)),
                    FallbackDirective::forbidden(),
                    request.request_id.clone(),
                    request.exact_scope.exact_scope_sha256(),
                    None,
                )
                .expect("handshake terminal"),
                descriptor: Some(self.descriptor.clone()),
                provider_instance_id: Some(
                    super::super::native_provider::PROVIDER_INSTANCE_ID.to_owned(),
                ),
                state_namespace: Some("journey-test".to_owned()),
                accepted_scope: Some(request.exact_scope.clone()),
                effective_limits: Some(request.host_limits.minimum(self.descriptor.limits)),
                ready_receipt_sha256: Some(READY_RECEIPT.to_owned()),
                warnings: Vec::new(),
            }
        }

        fn health(&self, _call: &ProviderCall) -> ProviderReply {
            Self::unexpected()
        }

        fn observe(&self, observation: NativeObservation<'_>) -> ProviderReply {
            self.observe_calls.fetch_add(1, Ordering::Relaxed);
            self.delivered.lock().unwrap().push(DeliveredObservation {
                bytes: observation.call.payload.bytes.clone(),
                exact_scope: observation.call.exact_scope.clone(),
            });
            let generation_after = observation.call.expected_state_generation.saturating_add(1);
            ProviderReply {
                terminal: TerminalRecord::new(
                    ProviderOperation::Observe,
                    observation.call.provider_id.clone(),
                    TerminalCode::Success,
                    CommittedEffectEvidence::committed(
                        observation.call.expected_state_generation,
                        generation_after,
                        vec!["observation:journey-test".to_owned()],
                        PROVIDER_RECEIPT,
                        EFFECT_DIGEST,
                    )
                    .expect("committed effect"),
                    FallbackDirective::forbidden(),
                    observation.call.operation_id.clone(),
                    observation.call.exact_scope.exact_scope_sha256(),
                    None,
                )
                .expect("observation terminal"),
                payload: Some(observation.call.payload.clone()),
                warnings: Vec::new(),
                extensions: observation.call.extensions.clone(),
                state_generation: generation_after,
            }
        }

        fn recall(&self, _call: &ProviderCall) -> ProviderReply {
            Self::unexpected()
        }

        fn feedback(&self, _call: &ProviderCall) -> ProviderReply {
            Self::unexpected()
        }

        fn maintenance(&self, _call: &ProviderCall) -> ProviderReply {
            Self::unexpected()
        }

        fn inspection(&self, _call: &ProviderCall) -> ProviderReply {
            Self::unexpected()
        }

        fn correction(&self, _call: &ProviderCall) -> ProviderReply {
            Self::unexpected()
        }

        fn delete_by_source(&self, _call: &ProviderCall) -> ProviderReply {
            Self::unexpected()
        }

        fn snapshot_export(&self, _call: &ProviderCall) -> ProviderReply {
            Self::unexpected()
        }

        fn snapshot_restore(&self, _call: &ProviderCall) -> ProviderReply {
            Self::unexpected()
        }

        fn replay(&self, _call: &ProviderCall) -> ProviderReply {
            Self::unexpected()
        }
    }

    fn scope(project_id: ProjectId) -> ResolvedScope {
        ResolvedScope::new(
            project_id,
            RepositoryId::new("repository.observation-journey").expect("repository"),
            WorktreeId::new("worktree.observation-journey").expect("worktree"),
            Some(RefId::new("refs/heads/observation-journey").expect("reference")),
        )
        .expect("resolved scope")
    }

    fn canonical_observation(
        project_id: &ProjectId,
        session_id: &SessionId,
        text: &str,
    ) -> DurableObservationV1 {
        canonical_observation_at(project_id, session_id, text, 0)
    }

    /// The native record identity of the `position`-th settled record of one
    /// session. Position zero keeps the plain name so existing rows and
    /// snapshots read the same.
    fn record_id_at(session_id: &SessionId, position: u64) -> ObservationId {
        let name = if position == 0 {
            format!("record.{}", session_id.as_str())
        } else {
            format!("record.{}.{position}", session_id.as_str())
        };
        ObservationId::new(name).expect("record id")
    }

    /// The receipt name for the `position`-th settled record of one session,
    /// with the same position-zero shape as [`record_id_at`].
    fn receipt_name_at(session_id: &SessionId, position: u64) -> String {
        if position == 0 {
            format!("receipt.{}", session_id.as_str())
        } else {
            format!("receipt.{}.{position}", session_id.as_str())
        }
    }

    /// Like [`canonical_observation`], but as the `position`-th record of the
    /// session: a *different* settled event under the same exact session
    /// scope, which is what a source position conflict needs.
    fn canonical_observation_at(
        project_id: &ProjectId,
        session_id: &SessionId,
        text: &str,
        position: u64,
    ) -> DurableObservationV1 {
        let provider = ProviderId::new("claude").expect("provider");
        let range = ObservationSourceRangeV1::new(position, position + 1).expect("range");
        let record_id = record_id_at(session_id, position);
        let envelope = CanonicalObservationEnvelopeV1::new(
            provider,
            "message",
            record_id,
            CanonicalObservationRelationsV1::new(session_id.clone()),
            vec![CanonicalObservationFactV1::Message {
                role: CanonicalMessageRoleV1::Assistant,
                content: json!({"text": text}),
                model: Some("model.fixture".to_owned()),
                timestamp: Some(1_750_000_000),
            }],
            CanonicalObservationEvidenceV1::new(ObservationOrderingDomainV1::SnapshotOrder, range),
        )
        .expect("canonical envelope");
        let payload = serde_json::to_value(envelope).expect("canonical payload");
        canonical_observation_with_payload_at(project_id, session_id, payload, position)
    }

    /// Settles `payload` as the durable record for `session_id` the way the
    /// store API does: identity, receipt, and payload reference are bound to
    /// exactly these bytes, whatever their shape. The canonical envelope
    /// validator is not consulted here, which is how a row persisted under a
    /// different contract revision looks to the journey.
    fn canonical_observation_with_payload(
        project_id: &ProjectId,
        session_id: &SessionId,
        payload: Value,
    ) -> DurableObservationV1 {
        canonical_observation_with_payload_at(project_id, session_id, payload, 0)
    }

    fn canonical_observation_with_payload_at(
        project_id: &ProjectId,
        session_id: &SessionId,
        payload: Value,
        position: u64,
    ) -> DurableObservationV1 {
        let provider = ProviderId::new("claude").expect("provider");
        let source = ObservationSourceIdentityV1::for_provider(provider, session_id.clone())
            .expect("source");
        let generation = ObservationSourceGenerationV1::new(1).expect("generation");
        let range = ObservationSourceRangeV1::new(position, position + 1).expect("range");
        let record_id = record_id_at(session_id, position);
        let identity = ObservationIdentityMaterialV1::for_native_record(
            source,
            ObservationScopeV1::Project {
                project_id: project_id.clone(),
            },
            generation,
            range,
            ObservationOrderingDomainV1::SnapshotOrder,
            record_id,
        )
        .expect("observation identity");
        let receipt = SanitizationReceiptV1::new(
            SanitizationReceiptRefV1::new(
                SanitizationReceiptId::new(receipt_name_at(session_id, position))
                    .expect("receipt id"),
                ComponentVersion::new("sanitizer.observation-journey-test.v1")
                    .expect("sanitizer version"),
            )
            .expect("receipt reference"),
            SanitizerDispositionV1::Accepted,
            SensitivityV1::NonSensitive,
            Some(PayloadReferenceV1::for_payload(&payload).expect("payload reference")),
        )
        .expect("sanitization receipt");
        DurableObservationV1::new(
            identity,
            receipt,
            RetentionClass::new("retention.observation-journey-test").expect("retention"),
            payload,
        )
        .expect("durable observation")
    }

    fn anchored_write(observation: DurableObservationV1) -> AnchoredObservationWrite {
        let identity = observation.identity();
        let next_cursor = ObservationSourceCursorV1::for_ordering(
            observation.source().clone(),
            observation.scope().clone(),
            identity.generation(),
            identity.ordering_domain(),
            identity.position().end(),
        )
        .expect("next cursor");
        let write = ObservationWrite::new(observation, None, next_cursor).expect("write");
        let projection_generation =
            ProjectionGenerationId::new("projection.observation-journey-test.v1")
                .expect("projection generation");
        let authorization = build_observation_resolution_authorization_v1(
            write.observation(),
            "observation-journey-test",
        )
        .expect("resolution authorization");
        let anchor = build_observation_retrieval_anchor_v2(
            write.observation(),
            projection_generation.clone(),
            UtcMicros(1_750_000_000_000_000),
            authorization,
        )
        .expect("retrieval anchor");
        AnchoredObservationWrite::new(write, anchor, projection_generation).expect("anchored write")
    }

    /// Builds the committed shape of `observation` at `sequence` the way the
    /// canonical store commits it, without going through the store's write
    /// boundary.
    fn settled_record(sequence: u64, observation: DurableObservationV1) -> StoredObservation {
        let identity = observation.identity();
        let committed_cursor = ObservationSourceCursorV1::for_ordering(
            observation.source().clone(),
            observation.scope().clone(),
            identity.generation(),
            identity.ordering_domain(),
            identity.position().end(),
        )
        .expect("committed cursor");
        let projection_generation =
            ProjectionGenerationId::new("projection.observation-journey-test.v1")
                .expect("projection generation");
        let authorization =
            build_observation_resolution_authorization_v1(&observation, "observation-journey-test")
                .expect("resolution authorization");
        let anchor = build_observation_retrieval_anchor_v2(
            &observation,
            projection_generation.clone(),
            UtcMicros(1_750_000_000_000_000),
            authorization,
        )
        .expect("retrieval anchor");
        StoredObservation::new(
            sequence,
            observation,
            committed_cursor,
            anchor,
            projection_generation,
            tracedecay_store::ObservationProjectionStatus::NotQueued,
        )
        .expect("settled record")
    }

    /// Replay port over records already settled elsewhere. It stands in for
    /// the canonical store only where the store's own write boundary cannot
    /// produce the row under test; the journey reads it through the same
    /// trait it reads the store through.
    struct SettledRecordsPort {
        records: Vec<StoredObservation>,
    }

    impl SettledRecordsPort {
        fn single(record: StoredObservation) -> Self {
            Self {
                records: vec![record],
            }
        }
    }

    impl ObservationAdmissionPort for SettledRecordsPort {
        async fn read_admitted_observation(
            &self,
            observation_id: &CanonicalObservationIdV1,
        ) -> Result<Option<StoredObservation>, ObservationStoreError> {
            Ok(self
                .records
                .iter()
                .find(|record| record.observation().observation_id() == observation_id)
                .cloned())
        }

        async fn replay_admitted_observations(
            &self,
            request: ObservationReplayRequest,
        ) -> Result<Vec<StoredObservation>, ObservationStoreError> {
            Ok(self
                .records
                .iter()
                .filter(|record| record.sequence() > request.after_sequence())
                .take(request.limit())
                .cloned()
                .collect())
        }
    }

    static NEVER_CANCELLED: LazyLock<HostCancellationToken> =
        LazyLock::new(HostCancellationToken::new);

    /// Bounds no test pass reaches: replay here is bounded by its page count.
    fn open_bounds() -> ReplayBoundsV1<'static> {
        ReplayBoundsV1 {
            cancellation: LazyLock::force(&NEVER_CANCELLED),
            deadline: tokio::time::Instant::now() + Duration::from_secs(30),
        }
    }

    /// Replay port that re-presents the same settled records on every pass
    /// and counts the passes, so a test can see whether the live replay task
    /// backs off or spins.
    struct CountingReplayPort {
        inner: SettledRecordsPort,
        passes: Arc<AtomicUsize>,
    }

    impl ObservationAdmissionPort for CountingReplayPort {
        async fn read_admitted_observation(
            &self,
            observation_id: &CanonicalObservationIdV1,
        ) -> Result<Option<StoredObservation>, ObservationStoreError> {
            self.inner.read_admitted_observation(observation_id).await
        }

        async fn replay_admitted_observations(
            &self,
            request: ObservationReplayRequest,
        ) -> Result<Vec<StoredObservation>, ObservationStoreError> {
            self.passes.fetch_add(1, Ordering::Relaxed);
            self.inner.replay_admitted_observations(request).await
        }
    }

    fn composition(port: Arc<JourneyNativePort>) -> Arc<ProjectMemoryProviderComposition> {
        Arc::new(
            ProjectMemoryProviderComposition::compose(NativeProviderActivation::Enabled {
                fabric_config: FabricConfig {
                    max_registered_providers: 1,
                    max_in_flight: 1,
                },
                port,
                registration_revision: 1,
                mode: EnabledProviderMode::Observer,
            })
            .expect("provider composition"),
        )
    }

    /// Diagnostic snapshot of the journal so a failed wait explains itself
    /// instead of reporting only a deadline.
    fn journal_snapshot(path: &Path) -> String {
        let connection = rusqlite::Connection::open(path).expect("journal connection");
        let count = |table: &str| -> i64 {
            connection
                .query_row(&format!("SELECT COUNT(*) FROM {table}"), [], |row| {
                    row.get(0)
                })
                .unwrap_or(-1)
        };
        let mut deliveries = String::new();
        if let Ok(mut statement) = connection.prepare(
            "SELECT observation_id, state, attempt_number, last_outcome FROM tdmem_observation_delivery_v1",
        ) {
            let rows = statement
                .query_map([], |row| {
                    Ok(format!(
                        "{}:{}:{}:{}",
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, i64>(2).unwrap_or(-1),
                        row.get::<_, Option<String>>(3)?.unwrap_or_default()
                    ))
                })
                .map(|rows| rows.flatten().collect::<Vec<_>>())
                .unwrap_or_default();
            deliveries = rows.join(" | ");
        }
        format!(
            "journal={} delivery={} withheld={} receipts={} cursors={} deliveries=[{}]",
            count("tdmem_observation_journal_v1"),
            count("tdmem_observation_delivery_v1"),
            count("tdmem_observation_withheld_v2"),
            count("tdmem_observation_receipt_v1"),
            count("tdmem_observation_replay_cursor_v1"),
            deliveries
        )
    }

    /// Waits until the single delivery row leaves `pending`/`leased` and
    /// returns its settled state and attempt count.
    async fn wait_for_settlement(journal_path: &Path) -> (String, i64) {
        let settled = tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                let connection = rusqlite::Connection::open(journal_path).expect("journal");
                let row = connection
                    .query_row(
                        "SELECT state, attempt_number FROM tdmem_observation_delivery_v1",
                        [],
                        |row| Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)?)),
                    )
                    .ok();
                match row {
                    Some((state, attempts)) if state != "pending" && state != "leased" => {
                        return (state, attempts);
                    }
                    _ => tokio::time::sleep(Duration::from_millis(20)).await,
                }
            }
        })
        .await;
        match settled {
            Ok(settled) => settled,
            Err(_) => panic!(
                "observation delivery never settled; {}",
                journal_snapshot(journal_path)
            ),
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn live_canonical_commit_settles_typed_native_rejection_with_receipt() {
        let _ = tracing_subscriber::fmt()
            .with_test_writer()
            .with_max_level(tracing::Level::DEBUG)
            .try_init();
        let temp = TempDir::new().expect("temporary journey root");
        let project_id = ProjectId::new("project.observation-journey").expect("project id");
        let profile_root = temp.path().join("profile");
        let project_root = temp.path().join("project");
        let runtime =
            HostAdmissionTestRuntimeV1::project(&profile_root, &project_root, project_id.clone())
                .await
                .expect("registered project database");
        let database = runtime
            .registered_database_arc(HostAdmissionScope::Project)
            .expect("project database");
        let store = database.observation_store();
        let resolved_scope = scope(project_id.clone());
        let port = Arc::new(JourneyNativePort::new());
        let journal_root = temp.path().join("journey");
        std::fs::create_dir_all(&journal_root).expect("journal root");
        let journey = mount_project_observation_journey(ObservationJourneyMountInputsV1 {
            composition: composition(Arc::clone(&port)),
            profile_id: UserProfileId::new("profile.observation-journey").expect("profile id"),
            scope: resolved_scope.clone(),
            authoritative_project_id: project_id.clone(),
            store_data_root: journal_root,
            registration_revision: 1,
            host_limits: super::super::native_provider::native_provider_limits(),
            policy: ObservationJourneyPolicyV1::project_default(),
        })
        .expect("mounted journey");

        assert_eq!(
            run_startup_replay(journey.as_ref(), &store, &HostCancellationToken::new())
                .await
                .unwrap()
                .admitted,
            0
        );
        journey
            .start_live_replay(store.clone())
            .expect("live replay task");

        let session_id = SessionId::new("session.observation-journey").expect("session id");
        let observation = canonical_observation(&project_id, &session_id, "persisted journey text");
        let expected_bytes = canonical_payload_bytes(&provider_observation_envelope(
            SESSION_MESSAGE_OBSERVATION_KIND,
            SESSION_MESSAGE_PAYLOAD_CONTRACT,
            observation.payload(),
        ))
        .expect("provider payload bytes");
        store
            .persist_observation(anchored_write(observation))
            .await
            .expect("canonical observation commit");

        // Direct replay surfaces a typed admission failure immediately; the
        // live task may already have consumed the record, so either 0 or 1.
        let admitted = journey
            .replay_canonical_observations(&store, REPLAY_LIVE_PAGES, open_bounds())
            .await
            .expect("direct canonical replay")
            .admitted;
        assert!(admitted <= 1, "unexpected admitted count {admitted}");
        let snapshot = journal_snapshot(journey.journal_path());
        assert!(
            snapshot.starts_with("journal=1 "),
            "canonical record was not journaled: {snapshot}"
        );

        // The journal row carries exactly the sanitized bytes and the exact
        // per-session scope that the provider is later called with.
        let expected_scope = exact_scope_for_session(
            &UserProfileId::new("profile.observation-journey").unwrap(),
            &resolved_scope,
            session_id.as_str(),
        )
        .unwrap();
        {
            let connection = rusqlite::Connection::open(journey.journal_path()).unwrap();
            let (kind, scope_sha256, bytes): (String, String, Vec<u8>) = connection
                .query_row(
                    "SELECT observation_kind, exact_scope_sha256, payload_bytes \
                     FROM tdmem_observation_journal_v1",
                    [],
                    |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
                )
                .unwrap();
            assert_eq!(kind, SESSION_MESSAGE_OBSERVATION_KIND);
            assert_eq!(scope_sha256, expected_scope.exact_scope_sha256());
            assert_eq!(bytes, expected_bytes);
        }

        // Native currently answers `capability_unsupported` /
        // `native.observation_staged` for session messages before its port is
        // reached. The journey must record that as one typed, non-retried
        // rejection with an immutable attempt receipt, not loop on it.
        let (state, attempts) = wait_for_settlement(journey.journal_path()).await;
        assert_eq!(
            (state.as_str(), attempts),
            ("rejected", 1),
            "{}",
            journal_snapshot(journey.journal_path())
        );
        assert_eq!(port.observe_calls.load(Ordering::Relaxed), 0);
        {
            let connection = rusqlite::Connection::open(journey.journal_path()).unwrap();
            let (receipts, outcome, effect): (i64, String, String) = connection
                .query_row(
                    "SELECT COUNT(*), MIN(outcome), MIN(committed_effect) \
                     FROM tdmem_observation_receipt_v1",
                    [],
                    |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
                )
                .unwrap();
            assert_eq!(receipts, 1);
            assert_eq!(effect, "none");
            // The journal maps a `capability_unsupported` terminal onto its
            // unsupported-rejection outcome class.
            assert_eq!(outcome, "rejected_extension_unsupported");
        }

        journey
            .shutdown(tokio::time::Instant::now() + Duration::from_secs(2))
            .await;
    }

    /// Waits until exactly `expected` delivery rows exist and none is still
    /// `pending`/`leased`, returning `(canonical source_event_id, state,
    /// attempts)` per row. Delivery rows are keyed by the journal's own
    /// observation id, so the canonical id comes from the joined journal row.
    async fn wait_for_deliveries(
        journal_path: &Path,
        expected: usize,
    ) -> Vec<(String, String, i64)> {
        let settled = tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                let connection = rusqlite::Connection::open(journal_path).expect("journal");
                let rows = connection
                    .prepare(
                        "SELECT journal.source_event_id, delivery.state, \
                                delivery.attempt_number \
                         FROM tdmem_observation_delivery_v1 AS delivery \
                         JOIN tdmem_observation_journal_v1 AS journal \
                           ON journal.observation_id = delivery.observation_id \
                         ORDER BY journal.source_event_id",
                    )
                    .and_then(|mut statement| {
                        statement
                            .query_map([], |row| {
                                Ok((
                                    row.get::<_, String>(0)?,
                                    row.get::<_, String>(1)?,
                                    row.get::<_, i64>(2)?,
                                ))
                            })
                            .map(|rows| rows.flatten().collect::<Vec<_>>())
                    })
                    .unwrap_or_default();
                let settled = rows.len() == expected
                    && rows
                        .iter()
                        .all(|(_, state, _)| state != "pending" && state != "leased");
                if settled {
                    return rows;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await;
        match settled {
            Ok(rows) => rows,
            Err(_) => panic!(
                "expected {expected} settled deliveries; {}",
                journal_snapshot(journal_path)
            ),
        }
    }

    /// Two sessions committed back to back must land as two journal rows,
    /// each bound to its own exact per-session scope, and a remount over the
    /// same journal must resume from the durable watermark: nothing already
    /// journaled is re-admitted, while the next canonical commit after the
    /// remount still flows through.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn interleaved_sessions_bind_exact_scopes_and_remount_resumes_from_watermark() {
        let _ = tracing_subscriber::fmt()
            .with_test_writer()
            .with_max_level(tracing::Level::DEBUG)
            .try_init();
        let temp = TempDir::new().expect("temporary journey root");
        let project_id = ProjectId::new("project.observation-journey-scopes").expect("project id");
        let profile_id = UserProfileId::new("profile.observation-journey").expect("profile id");
        let profile_root = temp.path().join("profile");
        let project_root = temp.path().join("project");
        let runtime =
            HostAdmissionTestRuntimeV1::project(&profile_root, &project_root, project_id.clone())
                .await
                .expect("registered project database");
        let database = runtime
            .registered_database_arc(HostAdmissionScope::Project)
            .expect("project database");
        let store = database.observation_store();
        let resolved_scope = scope(project_id.clone());
        let journal_root = temp.path().join("journey");
        std::fs::create_dir_all(&journal_root).expect("journal root");
        let mount = |port: Arc<JourneyNativePort>| {
            mount_project_observation_journey(ObservationJourneyMountInputsV1 {
                composition: composition(port),
                profile_id: profile_id.clone(),
                scope: resolved_scope.clone(),
                authoritative_project_id: project_id.clone(),
                store_data_root: journal_root.clone(),
                registration_revision: 1,
                host_limits: super::super::native_provider::native_provider_limits(),
                policy: ObservationJourneyPolicyV1::project_default(),
            })
            .expect("mounted journey")
        };
        let expected_scope_sha = |session: &str| {
            exact_scope_for_session(&profile_id, &resolved_scope, session)
                .unwrap()
                .exact_scope_sha256()
        };

        // First mount: two sessions interleave on the canonical stream.
        let first_port = Arc::new(JourneyNativePort::new());
        let journey = mount(Arc::clone(&first_port));
        assert_eq!(
            run_startup_replay(journey.as_ref(), &store, &HostCancellationToken::new())
                .await
                .unwrap()
                .admitted,
            0
        );
        journey
            .start_live_replay(store.clone())
            .expect("live replay task");

        let alpha = SessionId::new("session.alpha").expect("session id");
        let beta = SessionId::new("session.beta").expect("session id");
        let alpha_observation = canonical_observation(&project_id, &alpha, "alpha says hello");
        let beta_observation = canonical_observation(&project_id, &beta, "beta says hello");
        let alpha_id = alpha_observation.observation_id().as_str().to_owned();
        let beta_id = beta_observation.observation_id().as_str().to_owned();
        assert_ne!(
            alpha_id, beta_id,
            "distinct sessions must not share an observation id"
        );
        store
            .persist_observation(anchored_write(alpha_observation))
            .await
            .expect("alpha commit");
        store
            .persist_observation(anchored_write(beta_observation))
            .await
            .expect("beta commit");

        let deliveries = wait_for_deliveries(journey.journal_path(), 2).await;
        for (_, state, attempts) in &deliveries {
            assert_eq!((state.as_str(), *attempts), ("rejected", 1));
        }
        assert_eq!(first_port.observe_calls.load(Ordering::Relaxed), 0);
        {
            let connection = rusqlite::Connection::open(journey.journal_path()).unwrap();
            let mut statement = connection
                .prepare(
                    "SELECT source_event_id, exact_scope_sha256 \
                     FROM tdmem_observation_journal_v1 ORDER BY source_sequence",
                )
                .unwrap();
            let rows: Vec<(String, String)> = statement
                .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))
                .unwrap()
                .flatten()
                .collect();
            // Each journal row binds the scope of *its own* session; the two
            // scopes differ even though project, repository, and worktree
            // are shared.
            assert_eq!(
                rows,
                vec![
                    (alpha_id.clone(), expected_scope_sha("session.alpha")),
                    (beta_id.clone(), expected_scope_sha("session.beta")),
                ]
            );
            assert_ne!(rows[0].1, rows[1].1);

            let (cursors, watermark): (i64, i64) = connection
                .query_row(
                    "SELECT COUNT(*), MAX(last_admitted_sequence) \
                     FROM tdmem_observation_replay_cursor_v1",
                    [],
                    |row| Ok((row.get(0)?, row.get(1)?)),
                )
                .unwrap();
            assert_eq!(cursors, 2, "one replay cursor per exact scope");
            assert_eq!(
                watermark, 2,
                "watermark sits on the last committed sequence"
            );
        }
        let before_remount = journal_snapshot(journey.journal_path());
        let journal_path = journey.journal_path().to_path_buf();
        journey
            .shutdown(tokio::time::Instant::now() + Duration::from_secs(2))
            .await;
        drop(journey);

        // Second mount over the same journal: startup replay must read the
        // durable watermark and admit nothing that is already journaled.
        let second_port = Arc::new(JourneyNativePort::new());
        let journey = mount(Arc::clone(&second_port));
        assert_eq!(journey.journal_path(), journal_path.as_path());
        assert_eq!(
            run_startup_replay(journey.as_ref(), &store, &HostCancellationToken::new())
                .await
                .unwrap()
                .admitted,
            0,
            "remount re-admitted journaled observations: {}",
            journal_snapshot(&journal_path)
        );
        assert_eq!(journal_snapshot(&journal_path), before_remount);
        journey
            .start_live_replay(store.clone())
            .expect("live replay task");

        // A commit after the remount still flows: the watermark advances
        // instead of pinning the stream.
        let gamma = SessionId::new("session.gamma").expect("session id");
        let gamma_observation = canonical_observation(&project_id, &gamma, "gamma says hello");
        let gamma_id = gamma_observation.observation_id().as_str().to_owned();
        store
            .persist_observation(anchored_write(gamma_observation))
            .await
            .expect("gamma commit");
        let deliveries = wait_for_deliveries(&journal_path, 3).await;
        let gamma_row = deliveries
            .iter()
            .find(|(id, _, _)| *id == gamma_id)
            .expect("gamma delivery row");
        assert_eq!((gamma_row.1.as_str(), gamma_row.2), ("rejected", 1));
        assert_eq!(second_port.observe_calls.load(Ordering::Relaxed), 0);
        {
            let connection = rusqlite::Connection::open(&journal_path).unwrap();
            let (journal, receipts, watermark): (i64, i64, i64) = connection
                .query_row(
                    "SELECT (SELECT COUNT(*) FROM tdmem_observation_journal_v1), \
                            (SELECT COUNT(*) FROM tdmem_observation_receipt_v1), \
                            (SELECT MAX(last_admitted_sequence) \
                               FROM tdmem_observation_replay_cursor_v1)",
                    [],
                    |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
                )
                .unwrap();
            assert_eq!((journal, receipts, watermark), (3, 3, 3));
        }

        journey
            .shutdown(tokio::time::Instant::now() + Duration::from_secs(2))
            .await;
    }

    /// Reads every on-disk byte of the journal, including a WAL segment that
    /// has not been checkpointed, so a secret cannot hide in the write-ahead
    /// log while the main file looks clean.
    fn journal_files_contain(journal_path: &Path, needle: &[u8]) -> bool {
        assert!(!needle.is_empty(), "an empty needle proves nothing");
        // The main file must exist, or the scan is vacuous; the WAL and the
        // shared-memory index are optional side files.
        let main = std::fs::read(journal_path).expect("journal main file readable");
        let side = ["-wal", "-shm"].into_iter().filter_map(|suffix| {
            let mut path = journal_path.as_os_str().to_owned();
            path.push(suffix);
            std::fs::read(PathBuf::from(path)).ok()
        });
        std::iter::once(main)
            .chain(side)
            .any(|bytes| bytes.windows(needle.len()).any(|window| window == needle))
    }

    struct HygieneJourneyFixture {
        _runtime: HostAdmissionTestRuntimeV1,
        store: GlobalDbObservationStore,
        port: Arc<JourneyNativePort>,
        journey: Arc<ProjectObservationJourneyV1>,
        project_id: ProjectId,
    }

    /// Mounts the production journey against a real registered project store
    /// and a real journal, exactly as the composition root does, without the
    /// bounded live replay task so admission counts are deterministic: direct
    /// canonical replay is the authoritative path either way.
    async fn mount_hygiene_fixture(temp: &TempDir, project: &str) -> HygieneJourneyFixture {
        let project_id = ProjectId::new(project).expect("project id");
        let runtime = HostAdmissionTestRuntimeV1::project(
            &temp.path().join("profile"),
            &temp.path().join("project"),
            project_id.clone(),
        )
        .await
        .expect("registered project database");
        let store = runtime
            .registered_database_arc(HostAdmissionScope::Project)
            .expect("project database")
            .observation_store();
        let port = Arc::new(JourneyNativePort::new());
        let journal_root = temp.path().join("journey");
        std::fs::create_dir_all(&journal_root).expect("journal root");
        let journey = mount_project_observation_journey(ObservationJourneyMountInputsV1 {
            composition: composition(Arc::clone(&port)),
            profile_id: UserProfileId::new("profile.observation-hygiene").expect("profile id"),
            scope: scope(project_id.clone()),
            authoritative_project_id: project_id.clone(),
            store_data_root: journal_root,
            registration_revision: 1,
            host_limits: super::super::native_provider::native_provider_limits(),
            policy: ObservationJourneyPolicyV1::project_default(),
        })
        .expect("mounted journey");
        assert_eq!(
            run_startup_replay(journey.as_ref(), &store, &HostCancellationToken::new())
                .await
                .unwrap()
                .admitted,
            0
        );
        HygieneJourneyFixture {
            _runtime: runtime,
            store,
            port,
            journey,
            project_id,
        }
    }

    /// Acceptance: startup replay honours the project-open cancellation
    /// between records with a typed terminal, and the durable watermark holds
    /// exactly the records that completed before it. A later open resumes
    /// from that watermark under its own token and admits only what was
    /// never started.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn startup_replay_cancelled_between_records_returns_typed_terminal_and_holds_watermark() {
        let temp = TempDir::new().expect("temporary journey root");
        let fixture = mount_hygiene_fixture(&temp, "project.observation-cancel").await;
        let first = canonical_observation(
            &fixture.project_id,
            &SessionId::new("session.observation-cancel-first").expect("session id"),
            "first settled record",
        );
        let second = canonical_observation(
            &fixture.project_id,
            &SessionId::new("session.observation-cancel-second").expect("session id"),
            "second settled record",
        );
        let records = SettledRecordsPort {
            records: vec![settled_record(1, first), settled_record(2, second)],
        };

        // Admission proves readiness for each record's own session scope, so
        // the provider handshake is the one point inside record admission the
        // test can act from: the first record's admission drains the open.
        let cancellation = HostCancellationToken::new();
        let cancel = cancellation.clone();
        fixture.port.on_handshake(move || cancel.cancel());

        let error = run_startup_replay(fixture.journey.as_ref(), &records, &cancellation)
            .await
            .expect_err("a cancelled open must not report a completed replay");
        assert!(
            matches!(error, ObservationJourneyError::Cancelled { admitted: 1 }),
            "unexpected terminal: {error}"
        );
        let watermark = fixture
            .journey
            .journal
            .maximum_replay_sequence(
                SourceAuthorityV1::HostSession,
                &fixture.journey.source_stream,
            )
            .expect("watermark");
        assert_eq!(watermark, Some(SourceSequenceV1(1)));
        let snapshot = journal_snapshot(fixture.journey.journal_path());
        assert!(
            snapshot.starts_with("journal=1 delivery=1 "),
            "only the completed record may be journaled: {snapshot}"
        );

        // The next open resumes from the durable watermark under its own
        // token: the record that never started is admitted, nothing repeats.
        let pass = run_startup_replay(
            fixture.journey.as_ref(),
            &records,
            &HostCancellationToken::new(),
        )
        .await
        .expect("resumed replay");
        assert_eq!(
            pass,
            ReplayPassV1 {
                admitted: 1,
                halted: None,
            }
        );
        let snapshot = journal_snapshot(fixture.journey.journal_path());
        assert!(snapshot.starts_with("journal=2 delivery=2 "), "{snapshot}");
        let failures = fixture
            .journey
            .shutdown(tokio::time::Instant::now() + Duration::from_secs(2))
            .await;
        assert!(failures.is_empty(), "{failures:?}");
    }

    /// Acceptance: a permanent journal refusal is a typed halt, not an error
    /// and not a hole. The watermark holds at the refused position, the halt
    /// is reported typed from the pass and from the journey, and the live
    /// replay task backs off instead of repeating the refusal at park rate.
    ///
    /// The refusal is seeded the way a partial restore leaves a journal: the
    /// journaled row survives, its replay cursor does not, and the canonical
    /// stream then presents a different settled event at the same position.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn live_replay_backs_off_on_a_permanent_source_sequence_conflict_and_holds_watermark() {
        let temp = TempDir::new().expect("temporary journey root");
        let fixture = mount_hygiene_fixture(&temp, "project.observation-conflict").await;
        let session_id = SessionId::new("session.observation-conflict").expect("session id");
        let original =
            canonical_observation(&fixture.project_id, &session_id, "original settled event");
        let original_event_id = original.observation_id().as_str().to_owned();
        let pass = run_startup_replay(
            fixture.journey.as_ref(),
            &SettledRecordsPort::single(settled_record(1, original)),
            &HostCancellationToken::new(),
        )
        .await
        .expect("original replay");
        assert_eq!(
            pass,
            ReplayPassV1 {
                admitted: 1,
                halted: None,
            }
        );
        let (state, _) = wait_for_settlement(fixture.journey.journal_path()).await;
        assert_eq!(state, "rejected");

        {
            let connection =
                rusqlite::Connection::open(fixture.journey.journal_path()).expect("journal");
            connection
                .busy_timeout(Duration::from_secs(5))
                .expect("busy timeout");
            connection
                .execute("DELETE FROM tdmem_observation_replay_cursor_v1", [])
                .expect("drop replay cursor");
        }
        let conflicting = canonical_observation_at(
            &fixture.project_id,
            &session_id,
            "a different settled event at the same position",
            1,
        );
        assert_ne!(conflicting.observation_id().as_str(), original_event_id);
        let passes = Arc::new(AtomicUsize::new(0));
        let rewritten = CountingReplayPort {
            inner: SettledRecordsPort::single(settled_record(1, conflicting)),
            passes: Arc::clone(&passes),
        };

        // The authoritative pass returns the refusal typed inside the pass:
        // nothing admitted, nothing stepped over, the row that was there stays.
        let pass = fixture
            .journey
            .replay_canonical_observations(&rewritten, REPLAY_LIVE_PAGES, open_bounds())
            .await
            .expect("a typed journal refusal is a halt, not a replay error");
        assert_eq!(pass.admitted, 0);
        let halt = pass.halted.expect("halted pass");
        assert_eq!(halt.source_sequence, SourceSequenceV1(1));
        assert!(
            matches!(
                &halt.outcome,
                AppendOutcomeV1::SourceSequenceConflict {
                    stored_source_event_id,
                    ..
                } if stored_source_event_id == &original_event_id
            ),
            "unexpected refusal: {:?}",
            halt.outcome
        );
        let snapshot = journal_snapshot(fixture.journey.journal_path());
        assert!(snapshot.starts_with("journal=1 delivery=1 "), "{snapshot}");

        // The live replay task meets the same refusal, reports it typed on the
        // journey, and then waits out the backoff instead of polling the
        // refused position at the park interval.
        fixture
            .journey
            .start_live_replay(rewritten)
            .expect("live replay task");
        let halted = tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                if let Some(halt) = fixture.journey.halted_on() {
                    return halt;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .expect("live replay must surface the halt");
        assert_eq!(halted, halt);
        let passes_at_halt = passes.load(Ordering::Relaxed);
        tokio::time::sleep(Duration::from_millis(1_500)).await;
        assert_eq!(
            passes.load(Ordering::Relaxed),
            passes_at_halt,
            "live replay retried a halted position inside its backoff"
        );
        let snapshot = journal_snapshot(fixture.journey.journal_path());
        assert!(snapshot.starts_with("journal=1 delivery=1 "), "{snapshot}");
        let failures = fixture
            .journey
            .shutdown(tokio::time::Instant::now() + Duration::from_secs(2))
            .await;
        assert!(failures.is_empty(), "{failures:?}");
    }

    /// Acceptance: a reject-floor secret inside a settled canonical session
    /// message is withheld by the mounted production journey before any
    /// journal payload, delivery row, or provider call exists — and the
    /// canonical evidence the host settled is still there, unchanged.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn secret_bearing_canonical_commit_is_withheld_before_dispatch_and_evidence_survives() {
        let temp = TempDir::new().expect("temporary journey root");
        let fixture = mount_hygiene_fixture(&temp, "project.observation-hygiene-withheld").await;
        let session_id =
            SessionId::new("session.observation-hygiene-withheld").expect("session id");
        // Assembled from fragments so this file is not itself a secret corpus.
        let secret = concat!("ghp_", "KsY7QwT2mZ4bV9nR6cX1jH8pL3dG5fA0eUwQ");
        let observation = canonical_observation(
            &fixture.project_id,
            &session_id,
            &format!("rotate the token {secret} before the next deploy"),
        );
        let canonical_id = observation.observation_id().clone();
        let canonical_payload = observation.payload().clone();
        let source_envelope_bytes = canonical_payload_bytes(&provider_observation_envelope(
            SESSION_MESSAGE_OBSERVATION_KIND,
            SESSION_MESSAGE_PAYLOAD_CONTRACT,
            &canonical_payload,
        ))
        .expect("provider envelope bytes");
        fixture
            .store
            .persist_observation(anchored_write(observation))
            .await
            .expect("canonical observation commit");

        let admitted = fixture
            .journey
            .replay_canonical_observations(&fixture.store, REPLAY_LIVE_PAGES, open_bounds())
            .await
            .expect("direct canonical replay")
            .admitted;
        assert_eq!(admitted, 0, "a withheld event must not count as admitted");
        let snapshot = journal_snapshot(fixture.journey.journal_path());
        assert!(
            snapshot.starts_with("journal=0 delivery=0 withheld=1 receipts=0 cursors=1 "),
            "expected one withheld row and no delivery work: {snapshot}"
        );
        assert_eq!(fixture.port.observe_calls.load(Ordering::Relaxed), 0);

        // The withheld row is a typed reason plus digests that point back at
        // the untouched canonical record, and the replay cursor moved past it.
        {
            let connection = rusqlite::Connection::open(fixture.journey.journal_path()).unwrap();
            let (reason, source_event_id, source_payload_sha256, receipt_id, finding_count): (
                String,
                String,
                String,
                String,
                i64,
            ) = connection
                .query_row(
                    "SELECT reason, source_event_id, source_payload_sha256, receipt_id, \
                     finding_count FROM tdmem_observation_withheld_v2",
                    [],
                    |row| {
                        Ok((
                            row.get(0)?,
                            row.get(1)?,
                            row.get(2)?,
                            row.get(3)?,
                            row.get(4)?,
                        ))
                    },
                )
                .unwrap();
            assert_eq!(reason, "secret_rejected");
            assert_eq!(source_event_id, canonical_id.as_str());
            assert_eq!(
                source_payload_sha256,
                tracedecay_domain::canonical_text::sha256_hex(&source_envelope_bytes)
            );
            assert!(
                receipt_id.starts_with("obs-hygiene-withheld.v1."),
                "{receipt_id}"
            );
            assert!(finding_count >= 1);
            let (disposition, last_event_id): (String, String) = connection
                .query_row(
                    "SELECT last_disposition, last_source_event_id \
                     FROM tdmem_observation_replay_cursor_v1",
                    [],
                    |row| Ok((row.get(0)?, row.get(1)?)),
                )
                .unwrap();
            assert_eq!(disposition, "withheld");
            assert_eq!(last_event_id, canonical_id.as_str());
        }
        assert!(
            !journal_files_contain(fixture.journey.journal_path(), secret.as_bytes()),
            "the secret reached the journal file or its WAL"
        );

        // Transient or secret, hygiene never deletes or rewrites canonical
        // evidence: the host's record is still readable and byte-identical.
        let stored = fixture
            .store
            .get_observation(&canonical_id)
            .await
            .expect("canonical store read")
            .expect("canonical record still present");
        assert_eq!(stored.observation().payload(), &canonical_payload);

        // Replaying again is idempotent: the cursor already covers the event,
        // so no second withheld row appears and still nothing is dispatched.
        let admitted_again = fixture
            .journey
            .replay_canonical_observations(&fixture.store, REPLAY_LIVE_PAGES, open_bounds())
            .await
            .expect("second canonical replay")
            .admitted;
        assert_eq!(admitted_again, 0);
        let snapshot = journal_snapshot(fixture.journey.journal_path());
        assert!(
            snapshot.starts_with("journal=0 delivery=0 withheld=1 receipts=0 cursors=1 "),
            "replay was not idempotent: {snapshot}"
        );
        assert_eq!(fixture.port.observe_calls.load(Ordering::Relaxed), 0);

        fixture
            .journey
            .shutdown(tokio::time::Instant::now() + Duration::from_secs(2))
            .await;
    }

    /// Acceptance: a redact-class credential assignment inside a settled
    /// canonical session message is rewritten before the journal append; the
    /// journalled bytes — the only bytes delivery can ever send — carry the
    /// redaction marker and a receipt bound to exactly those bytes, while the
    /// canonical record keeps its original content.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn credential_assignment_is_redacted_and_the_journal_binds_the_delivered_bytes() {
        let temp = TempDir::new().expect("temporary journey root");
        let fixture = mount_hygiene_fixture(&temp, "project.observation-hygiene-redacted").await;
        let session_id =
            SessionId::new("session.observation-hygiene-redacted").expect("session id");
        let secret = concat!("api_", "key=", "0000000000000000");
        let observation = canonical_observation(
            &fixture.project_id,
            &session_id,
            &format!("the config still sets {secret} for the sandbox"),
        );
        let canonical_id = observation.observation_id().clone();
        let canonical_payload = observation.payload().clone();
        let source_envelope_bytes = canonical_payload_bytes(&provider_observation_envelope(
            SESSION_MESSAGE_OBSERVATION_KIND,
            SESSION_MESSAGE_PAYLOAD_CONTRACT,
            &canonical_payload,
        ))
        .expect("provider envelope bytes");
        fixture
            .store
            .persist_observation(anchored_write(observation))
            .await
            .expect("canonical observation commit");

        let admitted = fixture
            .journey
            .replay_canonical_observations(&fixture.store, REPLAY_LIVE_PAGES, open_bounds())
            .await
            .expect("direct canonical replay")
            .admitted;
        assert_eq!(admitted, 1);
        let snapshot = journal_snapshot(fixture.journey.journal_path());
        assert!(
            snapshot.starts_with("journal=1 delivery=1 withheld=0 "),
            "expected one admitted row: {snapshot}"
        );

        let (
            payload_bytes,
            payload_sha256,
            receipt_id,
            sanitizer_revision,
            source_sha256,
            receipt_json,
        ): (Vec<u8>, String, String, String, String, String) = {
            let connection = rusqlite::Connection::open(fixture.journey.journal_path()).unwrap();
            connection
                .query_row(
                    "SELECT payload_bytes, payload_sha256, sanitization_receipt_id, \
                     sanitizer_revision, source_payload_sha256, sanitization_receipt_json \
                     FROM tdmem_observation_journal_v1",
                    [],
                    |row| {
                        Ok((
                            row.get(0)?,
                            row.get(1)?,
                            row.get(2)?,
                            row.get(3)?,
                            row.get(4)?,
                            row.get(5)?,
                        ))
                    },
                )
                .unwrap()
        };
        let journalled = String::from_utf8(payload_bytes.clone()).expect("utf-8 payload");
        assert_ne!(
            payload_bytes, source_envelope_bytes,
            "redaction left the bytes untouched"
        );
        assert!(
            !journalled.contains(secret),
            "the assignment survived: {journalled}"
        );
        assert!(
            journalled.contains("[TraceDecay redacted: credential assignment]"),
            "no redaction marker in the delivered bytes: {journalled}"
        );
        assert!(
            journalled.contains("for the sandbox"),
            "durable text around the assignment was lost: {journalled}"
        );
        assert_eq!(
            payload_sha256,
            tracedecay_domain::canonical_text::sha256_hex(&payload_bytes)
        );
        assert_eq!(
            source_sha256,
            tracedecay_domain::canonical_text::sha256_hex(&source_envelope_bytes)
        );

        // The persisted receipt is decodable, names the redaction, and binds
        // the journalled bytes rather than the source bytes.
        let receipt = PayloadSanitizationReceipt::from_json(&receipt_json).expect("receipt json");
        assert_eq!(receipt.receipt_id(), receipt_id);
        assert_eq!(receipt.sanitizer_revision(), sanitizer_revision);
        assert_eq!(
            receipt.disposition(),
            tracedecay_memory_hygiene::SanitizationDisposition::Redacted
        );
        assert_eq!(receipt.sanitized_payload_sha256(), payload_sha256);
        assert_eq!(receipt.source_payload_sha256(), source_sha256);
        assert!(
            !journal_files_contain(fixture.journey.journal_path(), secret.as_bytes()),
            "the assignment reached the journal file or its WAL"
        );

        // Canonical evidence is untouched by redaction of the provider copy.
        let stored = fixture
            .store
            .get_observation(&canonical_id)
            .await
            .expect("canonical store read")
            .expect("canonical record still present");
        assert_eq!(stored.observation().payload(), &canonical_payload);

        // Delivery runs over the journalled bytes and settles as Native's typed
        // staged rejection; the sanitized row is what was offered, not the
        // source, and a second replay does not re-admit the event.
        fixture.journey.wake_delivery();
        let (state, attempts) = wait_for_settlement(fixture.journey.journal_path()).await;
        assert_eq!(
            (state.as_str(), attempts),
            ("rejected", 1),
            "{}",
            journal_snapshot(fixture.journey.journal_path())
        );
        assert_eq!(fixture.port.observe_calls.load(Ordering::Relaxed), 0);
        let admitted_again = fixture
            .journey
            .replay_canonical_observations(&fixture.store, REPLAY_LIVE_PAGES, open_bounds())
            .await
            .expect("second canonical replay")
            .admitted;
        assert_eq!(admitted_again, 0);
        assert!(
            journal_snapshot(fixture.journey.journal_path())
                .starts_with("journal=1 delivery=1 withheld=0 ")
        );

        fixture
            .journey
            .shutdown(tokio::time::Instant::now() + Duration::from_secs(2))
            .await;
    }

    /// Acceptance: a settled canonical record whose *shape* lies beyond the
    /// hygiene ceilings is not an admission fault. The mounted journey records
    /// a typed `unclassifiable_payload` withheld row carrying digests only,
    /// advances the replay cursor, creates no delivery work, leaves the
    /// canonical record untouched, and replays idempotently — so the startup
    /// pass returns `Ok` and the project server opens.
    ///
    /// The hygiene ceilings are derived from the canonical store contract, so
    /// no record that passes the canonical envelope validator can reach this
    /// path any more; the fixture persists the record through the durable
    /// store API, which is exactly how a row settled under another contract
    /// revision would look here.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn unclassifiable_canonical_record_is_withheld_with_a_typed_reason_and_replay_stays_idempotent()
     {
        let temp = TempDir::new().expect("temporary journey root");
        let fixture =
            mount_hygiene_fixture(&temp, "project.observation-hygiene-unclassifiable").await;
        let session_id =
            SessionId::new("session.observation-hygiene-unclassifiable").expect("session id");
        let ceiling = fixture
            .journey
            .adapter
            .context
            .sanitizer
            .policy()
            .max_canonical_bytes();

        // Grow the message text until the provider envelope's canonical bytes
        // sit one past the hygiene ceiling. The prose has no separator or long
        // token, so size is the only thing hygiene could object to.
        let placeholder = "placeholder";
        let mut payload = canonical_observation(&fixture.project_id, &session_id, placeholder)
            .payload()
            .clone();
        let envelope_for = |payload: &Value| {
            canonical_payload_bytes(&provider_observation_envelope(
                SESSION_MESSAGE_OBSERVATION_KIND,
                SESSION_MESSAGE_PAYLOAD_CONTRACT,
                payload,
            ))
            .expect("provider envelope bytes")
        };
        let overhead = envelope_for(&payload).len() - placeholder.len();
        let text_len = ceiling + 1 - overhead;
        let mut text = "fact ".repeat(text_len / 5);
        text.push_str(&"f".repeat(text_len % 5));
        payload["facts"][0]["content"]["text"] = Value::String(text);
        let source_envelope_bytes = envelope_for(&payload);
        assert_eq!(source_envelope_bytes.len(), ceiling + 1);
        let observation =
            canonical_observation_with_payload(&fixture.project_id, &session_id, payload.clone());
        let canonical_id = observation.observation_id().clone();
        // The current canonical store refuses a record this large at its own
        // write boundary, so the row is presented through the replay port the
        // journey reads, exactly as a row settled under another contract
        // revision would arrive: sequence 1, cursor and anchor built the way
        // the store builds them.
        let records = SettledRecordsPort::single(settled_record(1, observation));

        // The authoritative startup pass is Ok: the record is a typed terminal,
        // not a refusal that stalls the cursor and fails every open.
        let admitted = run_startup_replay(
            fixture.journey.as_ref(),
            &records,
            &HostCancellationToken::new(),
        )
        .await
        .expect("an unclassifiable record must not be an admission fault")
        .admitted;
        assert_eq!(admitted, 0, "a withheld event must not count as admitted");
        let snapshot = journal_snapshot(fixture.journey.journal_path());
        assert!(
            snapshot.starts_with("journal=0 delivery=0 withheld=1 receipts=0 cursors=1 "),
            "expected one withheld row and no delivery work: {snapshot}"
        );
        assert_eq!(fixture.port.observe_calls.load(Ordering::Relaxed), 0);

        {
            let connection = rusqlite::Connection::open(fixture.journey.journal_path()).unwrap();
            let (reason, source_event_id, source_payload_sha256, receipt_id, finding_count): (
                String,
                String,
                String,
                String,
                i64,
            ) = connection
                .query_row(
                    "SELECT reason, source_event_id, source_payload_sha256, receipt_id, \
                     finding_count FROM tdmem_observation_withheld_v2",
                    [],
                    |row| {
                        Ok((
                            row.get(0)?,
                            row.get(1)?,
                            row.get(2)?,
                            row.get(3)?,
                            row.get(4)?,
                        ))
                    },
                )
                .unwrap();
            assert_eq!(reason, "unclassifiable_payload");
            assert_eq!(source_event_id, canonical_id.as_str());
            assert_eq!(
                source_payload_sha256,
                tracedecay_domain::canonical_text::sha256_hex(&source_envelope_bytes)
            );
            assert!(
                receipt_id.starts_with("obs-hygiene-withheld.v1."),
                "{receipt_id}"
            );
            assert_eq!(finding_count, 0, "nothing was classified");
            let (disposition, last_event_id): (String, String) = connection
                .query_row(
                    "SELECT last_disposition, last_source_event_id \
                     FROM tdmem_observation_replay_cursor_v1",
                    [],
                    |row| Ok((row.get(0)?, row.get(1)?)),
                )
                .unwrap();
            assert_eq!(disposition, "withheld");
            assert_eq!(last_event_id, canonical_id.as_str());
        }

        // Withholding never touches canonical evidence: the oversized record is
        // still readable through the port and byte-identical.
        let stored = records
            .read_admitted_observation(&canonical_id)
            .await
            .expect("settled record read")
            .expect("settled record still present");
        assert_eq!(stored.observation().payload(), &payload);

        // A second startup pass is idempotent: the cursor already covers the
        // event, so no second withheld row appears and still nothing is sent.
        let admitted_again = run_startup_replay(
            fixture.journey.as_ref(),
            &records,
            &HostCancellationToken::new(),
        )
        .await
        .expect("second startup replay")
        .admitted;
        assert_eq!(admitted_again, 0);
        let snapshot = journal_snapshot(fixture.journey.journal_path());
        assert!(
            snapshot.starts_with("journal=0 delivery=0 withheld=1 receipts=0 cursors=1 "),
            "replay was not idempotent: {snapshot}"
        );
        assert_eq!(fixture.port.observe_calls.load(Ordering::Relaxed), 0);

        fixture
            .journey
            .shutdown(tokio::time::Instant::now() + Duration::from_secs(2))
            .await;
    }

    /// Waits until the single journal row's content has been purged by the
    /// mounted worker's retention sweep, and returns its delivery state.
    async fn wait_for_content_purge(journal_path: &Path) -> String {
        let purged = tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                let connection = rusqlite::Connection::open(journal_path).expect("journal");
                let row = connection
                    .query_row(
                        "SELECT d.state, \
                         j.payload_bytes IS NULL AND j.content_forgotten_at_micros IS NOT NULL \
                         FROM tdmem_observation_journal_v1 j \
                         JOIN tdmem_observation_delivery_v1 d \
                         ON d.idempotency_key = j.idempotency_key",
                        [],
                        |row| Ok((row.get::<_, String>(0)?, row.get::<_, bool>(1)?)),
                    )
                    .ok();
                match row {
                    Some((state, true)) => return state,
                    _ => tokio::time::sleep(Duration::from_millis(20)).await,
                }
            }
        })
        .await;
        match purged {
            Ok(state) => state,
            Err(_) => panic!(
                "the mounted retention sweep never purged the expired row; {}",
                journal_snapshot(journal_path)
            ),
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn remount_sweeps_rows_that_expired_while_the_journey_was_down() {
        let _ = tracing_subscriber::fmt()
            .with_test_writer()
            .with_max_level(tracing::Level::DEBUG)
            .try_init();
        let temp = TempDir::new().expect("temporary journey root");
        let project_id = ProjectId::new("project.observation-retention").expect("project id");
        let profile_root = temp.path().join("profile");
        let project_root = temp.path().join("project");
        let runtime =
            HostAdmissionTestRuntimeV1::project(&profile_root, &project_root, project_id.clone())
                .await
                .expect("registered project database");
        let database = runtime
            .registered_database_arc(HostAdmissionScope::Project)
            .expect("project database");
        let store = database.observation_store();
        let port = Arc::new(JourneyNativePort::new());
        let journal_root = temp.path().join("journey");
        std::fs::create_dir_all(&journal_root).expect("journal root");
        let inputs = || ObservationJourneyMountInputsV1 {
            composition: composition(Arc::clone(&port)),
            profile_id: UserProfileId::new("profile.observation-retention").expect("profile id"),
            scope: scope(project_id.clone()),
            authoritative_project_id: project_id.clone(),
            store_data_root: journal_root.clone(),
            registration_revision: 1,
            host_limits: super::super::native_provider::native_provider_limits(),
            policy: ObservationJourneyPolicyV1::project_default(),
        };

        // First life: one canonical commit settles through the real path, via
        // the same helper the composition root calls.
        let journey = mount_and_replay(inputs(), store.clone(), &HostCancellationToken::new())
            .await
            .expect("mounted journey");
        let journal_path = journey.journal_path().to_path_buf();
        let session_id = SessionId::new("session.observation-retention").expect("session id");
        store
            .persist_observation(anchored_write(canonical_observation(
                &project_id,
                &session_id,
                "retained journey text",
            )))
            .await
            .expect("canonical observation commit");
        let (state, attempts) = wait_for_settlement(&journal_path).await;
        assert_eq!(
            (state.as_str(), attempts),
            ("rejected", 1),
            "{}",
            journal_snapshot(&journal_path)
        );
        journey
            .shutdown(tokio::time::Instant::now() + Duration::from_secs(5))
            .await;
        drop(journey);

        // A settled row keeps its bytes until its effective expiry, and the
        // first life's sweeps found nothing expired. Age it past its admitted
        // privacy expiry while the journey is down, the way a clock does
        // between two daemon lives.
        {
            let connection = rusqlite::Connection::open(&journal_path).unwrap();
            let present: bool = connection
                .query_row(
                    "SELECT payload_bytes IS NOT NULL FROM tdmem_observation_journal_v1",
                    [],
                    |row| row.get(0),
                )
                .unwrap();
            assert!(present, "content was purged before expiry");
            let aged = tracedecay_application::now_micros().0 - 3_600_000_000;
            connection
                .execute(
                    "UPDATE tdmem_observation_journal_v1 SET expires_at_micros = ?1",
                    [aged],
                )
                .unwrap();
        }

        // Second life: the mounted worker's first retention turn purges the
        // expired content through the production path — the row is already
        // terminal, so nothing is re-terminalized — while startup replay
        // resumes at the durable watermark instead of re-admitting the record.
        let journey = mount_and_replay(inputs(), store.clone(), &HostCancellationToken::new())
            .await
            .expect("remounted journey");
        assert_eq!(journey.journal_path(), journal_path.as_path());
        let state = wait_for_content_purge(&journal_path).await;
        assert_eq!(
            state, "rejected",
            "a settled row is purged, not re-terminalized"
        );
        let snapshot = journal_snapshot(&journal_path);
        assert!(
            snapshot.starts_with("journal=1 ") && snapshot.contains("receipts=1 "),
            "replay must not re-admit and audit must survive purge: {snapshot}"
        );
        assert_eq!(port.observe_calls.load(Ordering::Relaxed), 0);
        journey
            .shutdown(tokio::time::Instant::now() + Duration::from_secs(5))
            .await;
    }
}
