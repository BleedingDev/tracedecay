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
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::mpsc::{self, RecvTimeoutError};
use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};
use std::time::Duration;

use serde_json::{Map, Value};
use sha2::{Digest, Sha256};
use tracedecay_application::ResolvedScope;
use tracedecay_domain::{DurableObservationV1, ObservationScopeV1, ProjectId, UserProfileId};
use tracedecay_memory_hygiene::{
    AdvisoryMetadataAdmissionV1, AdvisoryMetadataFieldV1, AdvisoryTextAdmissionV1,
    AdvisoryTextHardener, AdvisoryTextWithheldReasonV1, AdvisoryTrustTierV1, HygieneError,
    ObservationAdmission, ObservationSanitizer, UNTRUSTED_BOUNDARY_LABEL, canonical_payload_bytes,
};
use tracedecay_memory_observation::{
    AdmissionDecisionV1, AdmittedObservationV1, BackpressureGateV1, BackpressureHaltV1,
    BackpressurePolicyV1, BackpressureReasonV1, BackpressureStateV1, CanonicalSettlementReceiptV1,
    DeliveryAttemptV1, DeliveryControlV1, DeliveryRuntimeV1, DeliveryWakeV1, DispatchPolicyV1,
    DispatchRequestV1, DrainStopV1, ForgetSourceKeyV1, IdempotencyInputV1, IngressBatchReportV1,
    IngressControlV1, IngressHaltV1, IngressRuntimeV1, IngressStopReasonV1, LeaseRequestV1,
    LeasedObservationV1, OBSERVATION_CONTRACT_ID, ObservationAdmissionAdapterV1,
    ObservationDispatchPortV1, ObservationIdV1, ObservationIdempotencyKeyV1,
    ObservationJournalError, ObservationLaneKeyV1, ObservationLoadClassV1, ObservationPrivacyV1,
    ObservationRuntimeError, PrivacyClassificationV1, ProvenanceOriginV1, ProviderCheckpointV1,
    ProviderDeliveryAdapterV1, ProviderReplayPositionV1, ProviderTargetV1, QueueBacklogV1,
    RecoveryBudgetV1, RecoveryControlV1, RecoveryPlanV1, RecoveryRuntimeV1, RecoveryTargetKeyV1,
    RetentionClassV1, RetentionPolicyV1, RetentionSweepScheduleV1, RetentionSweeperV1,
    RetentionTickV1, RetryBackoffV1, SanitizationBindingV1, ShutdownRequestV1, SourceAuthorityV1,
    SourceRecordV1, SourceSequenceV1, SourceStreamIdV1, SourceStreamKeyV1,
    SqliteObservationJournal, WakeOutcomeV1, WithheldAdmissionV1, extensions_digest,
};
use tracedecay_memory_provider_registry::{
    ApiError, BoundedCallRefusalV1, BoundedProviderCallV1, CancellationToken, CanonicalPayload,
    CompositionLifecycleError, FabricError, HandshakeRequest, HandshakeRequestParts,
    HandshakeResponse, NATIVE_PROVIDER_ID, OperationControl, OwnedExactScope, OwnedProviderId,
    OwnedVersionedId, PayloadSanitizationReceipt, ProjectMemoryProviderComposition,
    ProjectMemoryProviderRegistry, ProviderCall, ProviderCallParts, ProviderHandshakeWorkV1,
    ProviderLimits, ProviderOperation, ReadinessEvidenceV1, RestartBudgetV1, ShutdownBudgetV1,
    SupervisedProviderReadinessV1, SupervisedReadinessConfigV1, SupervisedReadinessError,
};
use tracedecay_runtime_core::cancellation::CancellationToken as HostCancellationToken;
use tracedecay_store::{
    ObservationAdmissionPort, ObservationReplayRequest, ObservationStoreError, StoredObservation,
};

/// File name of the project-owned observation journal inside the canonical
/// store layout. Placement only; never an identity input.
const JOURNAL_FILE_NAME: &str = "memory-observation-journal-v1.sqlite3";

/// Directory name of the host-owned root every supervised provider's state is
/// contained under, inside the canonical store layout. The host creates it and
/// grants each admitted namespace a capability rooted beneath it; a provider
/// never names a path outside it (`tdmem-1107`).
const PROVIDER_STATE_DIR_NAME: &str = "provider-state";

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

/// Retention class every canonical session observation is admitted under.
///
/// One constant serves both the pre-admission classification the backpressure
/// gate refuses on and the envelope the adapter then builds, so the cheap gate
/// and the envelope can never disagree about what this stream is.
const ADMITTED_RETENTION_CLASS: RetentionClassV1 = RetentionClassV1::Session;

/// How long the caller waits past its own deadline for an in-flight blocking
/// admission to observe its cancellation and return.
///
/// This is not how a record is bounded — the record's own deadline and
/// cancellation are checked inside the blocking work, before hygiene and
/// before the append. This is the last resort that keeps the *caller* bounded
/// when the blocking pool itself is saturated by other work, so a replay pass
/// returns a typed deadline terminal instead of parking indefinitely. The
/// record stays canonical either way and the watermark does not move.
const FOREGROUND_ABORT_GRACE: Duration = Duration::from_secs(2);

/// Spawn attempts one supervised exact scope may make inside
/// [`SUPERVISOR_RESTART_WINDOW_MICROS`]. Re-proving the readiness of a healthy
/// incarnation spends none of these, so this bounds crash loops only.
const SUPERVISOR_RESTART_ATTEMPTS_PER_WINDOW: u32 = 5;
/// Rolling window those spawn attempts are counted in.
const SUPERVISOR_RESTART_WINDOW_MICROS: i64 = 60_000_000;
/// First enforced delay between spawn attempts.
const SUPERVISOR_BACKOFF_BASE_MICROS: i64 = 50_000;
/// Ceiling the enforced doubling saturates at.
const SUPERVISOR_BACKOFF_MAX_MICROS: i64 = 5_000_000;
/// Graceful-stop budget before a supervised instance is forcibly terminated.
const SUPERVISOR_GRACE_MICROS: i64 = 2_000_000;
/// Forced-termination budget after grace elapses.
const SUPERVISOR_KILL_MICROS: i64 = 1_000_000;
/// Finite ceiling on concurrently supervised exact scopes for one project.
/// Beyond it the coldest scope is retired after its instance's death is
/// confirmed, so the owner set never grows without bound and never wedges.
const SUPERVISED_SCOPE_CEILING: usize = 64;

/// Consecutive automatic recovery assessments one incompatible provider state
/// may consume before the journey stops proposing automatic recovery and the
/// refusal names the repair an operator has to perform. The counter is durable
/// and is cleared by an actual convergence, so a provider that comes back
/// healthy is not held against its history.
const RECOVERY_MAX_AUTOMATIC_ATTEMPTS: u32 = 3;

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
    /// Thresholds the ingress gate sheds and refuses on, and the foreground
    /// admission budget a coding agent is entitled to.
    pub(crate) backpressure: BackpressurePolicyV1,
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
                // A restart backlog is durable and nothing signals about it
                // twice, so one turn drains up to sixteen batches — bounded by
                // a wall budget well inside the daemon's shutdown deadline so
                // reaping, retention, and stopping are never starved.
                max_rounds_per_drain: 16,
                drain_budget_micros: 10_000_000,
            },
            backpressure: BackpressurePolicyV1 {
                // Session-lifetime observation traffic — everything this
                // journey admits today — stops at three quarters of the
                // journal's own ceiling, leaving the last quarter for
                // project- and profile-lifetime work that must not be
                // refused early. Nothing is discarded either way: a shed
                // holds the canonical watermark and the record is
                // re-presented by the next replay pass.
                shed_optional_at_ppm: 750_000,
                refuse_at_ppm: 950_000,
                // A lane whose oldest queued row has waited five minutes is
                // not draining, and adding to it helps nobody.
                max_backlog_age_micros: 300_000_000,
                // One canonical record's sanitize-derive-append path. Beyond
                // it the journal itself is what is slow.
                foreground_budget_micros: 250_000,
                // Three consecutive overruns, not one: a single slow fsync is
                // a disk hiccup, and refusing observation traffic over it
                // would make the product jumpy for nothing. A run of three is
                // an admission path that is genuinely not keeping up, and
                // there the remedy — optional traffic stops competing for the
                // journal so the delivery worker can drain — is real.
                foreground_breach_streak: 3,
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
        self.backpressure
            .validate()
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
    /// The supervised provider lifecycle refused readiness for this exact
    /// scope. The host keeps running against this typed degradation; nothing
    /// downstream is admitted without a validated readiness target.
    #[error("supervised provider lifecycle refused readiness: {0}")]
    SupervisedReadiness(#[source] SupervisedReadinessError),
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
    /// The authoritative startup replay failed in a way no later pass can
    /// clear, so the mount is refused instead of reporting a healthy journey
    /// over a committed observation that will never be delivered.
    ///
    /// The journal watermark still holds in front of the refused record and
    /// nothing was lost; what is refused is the *claim* that the journey is
    /// converging, because it is not.
    #[error(
        "project observation startup replay failed permanently, so the journey did not mount: \
         {source}"
    )]
    StartupReplayPermanent {
        /// The refusal that cannot be retried away.
        #[source]
        source: Box<ObservationJourneyError>,
    },
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

// ---------------------------------------------------------------------------
// Untrusted-memory gate for provider recall (tdmem-1105)
//
// Hygiene has two directions. Outbound, it keeps a credential from leaving the
// host inside an observation. Inbound, a recall candidate is text a provider
// wrote that ends up inside the context an agent reads as instructions, so it
// is untrusted advisory data and must be contained, de-marked-up, secret-
// scanned, and trust-labelled before context assembly.
//
// This module is the one root file that owns the hygiene pipeline, so the gate
// is composed here and handed to the advisory recall lane as a root-local
// value. The recall lane therefore never names the hygiene crate itself, and
// the pipeline keeps exactly one owner inside the composition root.
// ---------------------------------------------------------------------------

/// The provider-controlled label one metadata hardening decided about.
///
/// Re-exported under a root-local name so the advisory recall lane can name a
/// metadata field without naming the hygiene pipeline: this file is the one
/// root owner of that crate.
pub(super) use tracedecay_memory_hygiene::AdvisoryMetadataFieldV1 as UntrustedRecallMetadataFieldV1;
/// Why the untrusted-memory gate refused one provider string, re-exported
/// under a root-local name for the same reason.
pub(super) use tracedecay_memory_hygiene::AdvisoryTextWithheldReasonV1 as UntrustedRecallWithheldReasonV1;

/// The fault the untrusted-memory gate raises when the admitted secret
/// pipeline itself cannot decide, re-exported under a root-local name so the
/// recall lane can propagate it as a typed value rather than as prose.
pub(super) type UntrustedRecallGateFaultV1 = HygieneError;

/// Trust the host places in one recall candidate's text.
///
/// It is derived from the host's *own* provenance verdict, never from the
/// provider's claim: only a host authority's confirmation is
/// [`Self::HostConfirmed`], and a claim the host could not confirm is worth no
/// more than no claim at all.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum UntrustedRecallTrustV1 {
    /// No provenance was established, or the claim did not resolve.
    Unattributed,
    /// The provider named a source or gave a redaction reason; unconfirmed.
    ProviderAttested,
    /// A host authority confirmed the claimed source.
    HostConfirmed,
}

impl UntrustedRecallTrustV1 {
    /// The provider-neutral tier the hygiene gate reads.
    const fn tier(self) -> AdvisoryTrustTierV1 {
        match self {
            Self::Unattributed => AdvisoryTrustTierV1::Unattributed,
            Self::ProviderAttested => AdvisoryTrustTierV1::ProviderAttested,
            Self::HostConfirmed => AdvisoryTrustTierV1::HostConfirmed,
        }
    }
}

/// The untrusted-memory gate every provider recall candidate passes before it
/// can reach context assembly.
#[derive(Clone, Debug)]
pub(super) struct UntrustedRecallGateV1 {
    hardener: AdvisoryTextHardener,
}

impl UntrustedRecallGateV1 {
    /// The instruction boundary an admitted advisory item carries. The host
    /// writes it; a provider copy of it inside candidate text is neutralized.
    pub(super) const BOUNDARY_LABEL: &'static str = UNTRUSTED_BOUNDARY_LABEL;

    /// Composes the gate from the canonical hygiene policy.
    ///
    /// # Errors
    ///
    /// Returns the hygiene fault itself, not a rendered string, when the
    /// canonical class-to-action table cannot be assembled. The caller reports
    /// its lane unavailable: provider text is never delivered unclassified,
    /// and the caller can still say *which* fault stopped it.
    pub(super) fn open() -> Result<Self, HygieneError> {
        AdvisoryTextHardener::new().map(|hardener| Self { hardener })
    }

    /// Hardens one candidate's content and optional explanation.
    ///
    /// A refused item is not dropped: the typed outcome keeps the withheld
    /// reason, the trust tier, and the source digest, so a refusal stays a
    /// structural fact rather than a sentence a caller would have to parse.
    /// The in-band notice the agent reads is derived from that same typed
    /// reason by [`UntrustedRecallItemV1::rendered_content`].
    ///
    /// # Errors
    ///
    /// Returns [`HygieneError`] when the admitted secret pipeline itself
    /// faults. A detector fault says nothing about whether the text was safe,
    /// so it is never flattened into a withholding: the caller must fail its
    /// lane.
    pub(super) fn harden(
        &self,
        content: &str,
        explanation: Option<&str>,
        trust: UntrustedRecallTrustV1,
    ) -> Result<UntrustedRecallItemV1, HygieneError> {
        Ok(
            match self.hardener.harden(content, explanation, trust.tier())? {
                AdvisoryTextAdmissionV1::Admitted(hardened) => UntrustedRecallItemV1::Admitted {
                    content: hardened.content().to_owned(),
                    explanation: hardened.explanation().map(str::to_owned),
                    source_content_sha256: hardened.source_content_sha256().to_owned(),
                    hardened_content_sha256: hardened.hardened_content_sha256().to_owned(),
                },
                AdvisoryTextAdmissionV1::Withheld {
                    reason,
                    source_content_sha256,
                    ..
                } => UntrustedRecallItemV1::Withheld {
                    reason,
                    source_content_sha256,
                },
            },
        )
    }

    /// Hardens one provider-controlled metadata label — a candidate identity,
    /// a claimed provenance source, or a provider-authored reason.
    ///
    /// These are agent-visible for exactly the same reason content is: they
    /// are interpolated into the same rendered line. They therefore pass the
    /// same gate rather than being copied through as opaque keys.
    ///
    /// # Errors
    ///
    /// Returns [`HygieneError`] when the admitted secret pipeline faults.
    pub(super) fn harden_metadata(
        &self,
        field: AdvisoryMetadataFieldV1,
        value: &str,
    ) -> Result<UntrustedRecallMetadataV1, HygieneError> {
        Ok(match self.hardener.harden_metadata(field, value)? {
            AdvisoryMetadataAdmissionV1::Admitted { value, .. } => {
                UntrustedRecallMetadataV1::Admitted(value)
            }
            AdvisoryMetadataAdmissionV1::Withheld {
                reason,
                source_sha256,
                ..
            } => UntrustedRecallMetadataV1::Withheld {
                reason,
                source_sha256,
            },
        })
    }

    /// The in-band notice that stands in for one withheld item's text.
    pub(super) fn withheld_text(code: &str) -> String {
        format!("{} withheld: {code}", Self::BOUNDARY_LABEL)
    }
}

/// What the untrusted-memory gate decided about one candidate's text.
///
/// Both arms carry the source digest, so a refusal is auditable without
/// keeping a copy of the refused bytes, and a caller never has to read prose
/// to learn which happened.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) enum UntrustedRecallItemV1 {
    /// The text may be compiled into a context pack.
    Admitted {
        /// Agent-visible content, host boundary label included.
        content: String,
        /// Retained explanation, if the gate admitted one.
        explanation: Option<String>,
        /// Digest of the provider's original content.
        source_content_sha256: String,
        /// Digest of the delivered content.
        hardened_content_sha256: String,
    },
    /// The text must not be delivered.
    Withheld {
        /// Which rule fired.
        reason: AdvisoryTextWithheldReasonV1,
        /// Digest of the provider's original content.
        source_content_sha256: String,
    },
}

impl UntrustedRecallItemV1 {
    /// The content the agent reads for this item.
    ///
    /// A withheld item is never silently dropped: it keeps its place in the
    /// list and its text is replaced by a typed in-band notice, so a refusal
    /// looks like a refusal rather than like a provider with less to say.
    pub(super) fn rendered_content(&self) -> String {
        match self {
            Self::Admitted { content, .. } => content.clone(),
            Self::Withheld { reason, .. } => UntrustedRecallGateV1::withheld_text(reason.code()),
        }
    }

    /// The retained explanation, if any.
    pub(super) fn rendered_explanation(&self) -> Option<String> {
        match self {
            Self::Admitted { explanation, .. } => explanation.clone(),
            Self::Withheld { .. } => None,
        }
    }

    /// Digest of the provider's original content, admitted or not.
    pub(super) fn source_content_sha256(&self) -> &str {
        match self {
            Self::Admitted {
                source_content_sha256,
                ..
            }
            | Self::Withheld {
                source_content_sha256,
                ..
            } => source_content_sha256,
        }
    }

    /// Digest of the delivered content, when there is delivered content.
    pub(super) fn hardened_content_sha256(&self) -> Option<&str> {
        match self {
            Self::Admitted {
                hardened_content_sha256,
                ..
            } => Some(hardened_content_sha256),
            Self::Withheld { .. } => None,
        }
    }

    /// The typed withholding reason, when the gate refused the text.
    pub(super) const fn withheld_reason(&self) -> Option<AdvisoryTextWithheldReasonV1> {
        match self {
            Self::Admitted { .. } => None,
            Self::Withheld { reason, .. } => Some(*reason),
        }
    }
}

/// What the untrusted-memory gate decided about one provider-controlled
/// metadata label.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) enum UntrustedRecallMetadataV1 {
    /// The label may be rendered, in this contained form.
    Admitted(String),
    /// The label must not be rendered. The caller substitutes a host-minted
    /// stand-in rather than a repaired copy of the provider's bytes.
    Withheld {
        /// Which rule fired.
        reason: AdvisoryTextWithheldReasonV1,
        /// Digest of the provider's original label.
        source_sha256: String,
    },
}

impl UntrustedRecallMetadataV1 {
    /// The contained label, when the gate admitted one.
    pub(super) fn admitted(&self) -> Option<&str> {
        match self {
            Self::Admitted(value) => Some(value),
            Self::Withheld { .. } => None,
        }
    }

    /// The typed refusal, when the gate refused the label.
    pub(super) const fn withheld_reason(&self) -> Option<AdvisoryTextWithheldReasonV1> {
        match self {
            Self::Admitted(_) => None,
            Self::Withheld { reason, .. } => Some(*reason),
        }
    }

    /// Digest of the provider's original label, admitted or not.
    pub(super) fn source_sha256(&self) -> Option<&str> {
        match self {
            Self::Admitted(_) => None,
            Self::Withheld { source_sha256, .. } => Some(source_sha256),
        }
    }
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
    readiness: Arc<SupervisedProviderReadinessV1>,
    /// The provider lane this journey's records queue in. Named from the
    /// registration alone, so ingress can measure the lane's pressure before
    /// it pays for the readiness handshake that would name an instance.
    provider_lane: ObservationLaneKeyV1,
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
    type Control = ReplayIngestControlV1;

    fn lane(&self, _record: &SourceRecordV1<Self::Record>) -> ObservationLaneKeyV1 {
        self.context.provider_lane.clone()
    }

    fn classify(&self, _record: &SourceRecordV1<Self::Record>) -> ObservationLoadClassV1 {
        // The same constant the envelope below carries. Answering here costs
        // nothing, and it is what lets the gate refuse a lane that is already
        // shedding this stream *before* hygiene, digest derivation, and a
        // readiness proof are paid for on a record that cannot be admitted.
        ObservationLoadClassV1::of(ADMITTED_RETENTION_CLASS)
    }

    fn decide(
        &self,
        record: &SourceRecordV1<Self::Record>,
        control: &Self::Control,
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
                    &context.readiness,
                    &exact_scope,
                    context.registration_revision,
                    context.limits,
                    context.observe_capability.clone(),
                    // The caller's own bound, narrowed to the admission-time
                    // handshake budget. Minting a fresh token here is what
                    // made a five-second readiness call outlive a project that
                    // had already closed.
                    control.operation_control(READINESS_DEADLINE_MICROS),
                )
                .map_err(|source| AdmissionAdapterError::Readiness {
                    source_event_id: source_event_id.clone(),
                    source,
                })?;
                let admitted_at_unix_micros = tracedecay_application::now_micros().0;
                let occurred_at_unix_micros = settlement.settled_at_unix_micros;
                let privacy = ObservationPrivacyV1 {
                    classification: PrivacyClassificationV1::Sensitive,
                    retention_class: ADMITTED_RETENTION_CLASS,
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
    readiness: Arc<SupervisedProviderReadinessV1>,
    registration_revision: u64,
    limits: ProviderLimits,
    observe_capability: OwnedVersionedId,
    /// Restart recovery. Every attempt passes through it, so a provider whose
    /// state moved under the journal is refused before it is written to.
    recovery: ObservationRecoveryGateV1,
}

/// Typed delivery refusals. Every one of them produces no receipt, which is
/// what makes the attempt redeliverable rather than settled.
#[derive(Debug, thiserror::Error)]
enum DeliveryAdapterError {
    #[error("provider composition is disabled, so no observation can be delivered")]
    Disabled,
    #[error("provider readiness could not be proven for the leased exact scope: {0}")]
    Readiness(#[source] ObservationJourneyError),
    #[error("restart recovery refused this provider incarnation: {0}")]
    Recovery(#[source] RecoveryRefusalV1),
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

/// Why restart recovery refused to deliver into the incarnation that answered
/// the handshake.
///
/// Every variant produces no provider call, no receipt, and no state change:
/// the row stays exactly as deliverable as it was, and the failure reaches the
/// dispatch report with its cause intact.
#[derive(Debug, thiserror::Error)]
enum RecoveryRefusalV1 {
    #[error("recovery could not be assessed against the journal: {0}")]
    Assessment(#[source] ObservationRuntimeError),
    #[error("restart recovery stopped because the delivery attempt was cancelled: {0}")]
    Cancelled(#[source] ObservationRuntimeError),
    #[error("restart recovery stopped because the attempt's budget expired: {0}")]
    DeadlineExceeded(#[source] ObservationRuntimeError),
    #[error("provider state is incompatible ({defect}); {repair} before delivery resumes")]
    StateIncompatible {
        /// Canonical wire value of the typed defect.
        defect: &'static str,
        /// Canonical wire value of the repair the defect requires.
        repair: &'static str,
    },
}

impl RecoveryRefusalV1 {
    /// Classifies one runtime failure without flattening a bound that expired
    /// into a journal failure: only the first is a reason to hand the row
    /// straight back.
    fn from_runtime(error: ObservationRuntimeError) -> Self {
        match error {
            cancelled @ ObservationRuntimeError::RecoveryCancelled { .. } => {
                Self::Cancelled(cancelled)
            }
            expired @ ObservationRuntimeError::RecoveryDeadlineExceeded { .. } => {
                Self::DeadlineExceeded(expired)
            }
            other => Self::Assessment(other),
        }
    }
}

/// The restart-recovery gate every delivery attempt passes through.
///
/// This is what makes `tdmem-0506` production code rather than a reusable
/// type. The provider's own state schema and generation come from the *same*
/// validated handshake that produced the delivery address, so the journal never
/// pairs an address from one incarnation with state evidence from another. A
/// provider whose state schema moved, or whose generation went backwards
/// because it was restored or wiped, is refused here — before any observation
/// reaches it — instead of being silently reinitialized by replaying history it
/// no longer holds.
///
/// The admitted answer is the expected state generation the provider call must
/// declare. Nothing else may supply it: a hardcoded expectation would make the
/// fabric's own `ready.state_generation == call.expected_state_generation`
/// check vacuous.
struct ObservationRecoveryGateV1 {
    journal: Arc<SqliteObservationJournal>,
    provider_id: String,
    registration_revision: u64,
    source_authority: SourceAuthorityV1,
    source_stream: SourceStreamIdV1,
    budget: RecoveryBudgetV1,
}

impl ObservationRecoveryGateV1 {
    /// The replay position the validated readiness evidence proves for this
    /// incarnation.
    ///
    /// Absence is a *decision* here, not a default. The handshake contract's
    /// only sanctioned channel for a provider-local acknowledged position is
    /// the replay capability, so an incarnation that does not declare it keeps
    /// no position, and exact-effect verification rests on the host's
    /// content-derived idempotency key and its own durable receipts. An
    /// incarnation that *does* declare it and whose position the host cannot
    /// read is refused rather than treated as the first case, because that is
    /// precisely the shape in which lost provider effects would go unnoticed.
    fn replay_position(evidence: &ReadinessEvidenceV1) -> ProviderReplayPositionV1 {
        if evidence.retains_replay_position() {
            ProviderReplayPositionV1::Unreadable
        } else {
            ProviderReplayPositionV1::NotRetained
        }
    }

    /// Assesses the incarnation that just proved readiness for one exact scope
    /// and returns the generation a delivery call may declare.
    ///
    /// The attempt's own bound travels into the assessment: the journal read,
    /// the frontier read, and the single durable write all run under the
    /// delivery deadline and the delivery cancellation token, so a shutdown or
    /// an expired budget stops recovery itself rather than only the provider
    /// call after it.
    fn admit_delivery(
        &self,
        exact_scope_sha256: &str,
        evidence: &ReadinessEvidenceV1,
        control: &DeliveryControlV1,
        now_unix_micros: i64,
    ) -> Result<u64, RecoveryRefusalV1> {
        let checkpoint = ProviderCheckpointV1 {
            target: RecoveryTargetKeyV1 {
                provider_id: self.provider_id.clone(),
                registration_revision: self.registration_revision,
                stream: SourceStreamKeyV1 {
                    source_authority: self.source_authority,
                    exact_scope_sha256: exact_scope_sha256.to_owned(),
                    source_stream: self.source_stream.clone(),
                },
            },
            implementation_identity_sha256: evidence.implementation_identity_sha256().to_owned(),
            state_schema_version: evidence.state_schema_version().to_owned(),
            state_generation: evidence.state_generation(),
            replay_position: Self::replay_position(evidence),
        };
        let recovery_control =
            RecoveryControlV1::new(control.deadline_unix_micros(), control.cancellation());
        let runtime = RecoveryRuntimeV1::new(self.journal.as_ref(), self.budget)
            .map_err(RecoveryRefusalV1::Assessment)?;
        let plan = runtime
            .assess(&checkpoint, &recovery_control, now_unix_micros)
            .map_err(RecoveryRefusalV1::from_runtime)?;
        let operator_repair_required =
            matches!(plan, RecoveryPlanV1::OperatorRepairRequired { .. });
        match &plan {
            // Replay is not a separate code path: the outbox already holds the
            // unacknowledged rows, and this dispatcher is the loop that drains
            // them. What the plan adds is the verified generation and the
            // proof that draining them is safe.
            RecoveryPlanV1::Converged {
                expected_state_generation,
                ..
            }
            | RecoveryPlanV1::ReplayUnacknowledged {
                expected_state_generation,
                ..
            } => Ok(*expected_state_generation),
            RecoveryPlanV1::StateIncompatible { defect, repair, .. }
            | RecoveryPlanV1::OperatorRepairRequired { defect, repair, .. } => {
                tracing::warn!(
                    event = "memory_observation_recovery_refused",
                    exact_scope_sha256,
                    defect = defect.as_wire(),
                    repair = repair.as_wire(),
                    operator_repair_required,
                    "restart recovery refused delivery into this provider state"
                );
                Err(RecoveryRefusalV1::StateIncompatible {
                    defect: defect.as_wire(),
                    repair: repair.as_wire(),
                })
            }
        }
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
        let (readiness, evidence) = readiness_target_and_evidence_for_scope(
            &self.readiness,
            &leased.exact_scope,
            self.registration_revision,
            self.limits,
            self.observe_capability.clone(),
            operation_control(started_at_unix_micros),
        )
        .map_err(DeliveryAdapterError::Readiness)?;
        // The recovery gate runs on the evidence of the very handshake above,
        // before one byte reaches the provider. A refusal is typed, produces no
        // receipt, and leaves the row exactly as deliverable as it was.
        let expected_state_generation = match self.recovery.admit_delivery(
            &leased.exact_scope_sha256,
            &evidence,
            control,
            tracedecay_application::now_micros().0,
        ) {
            Ok(generation) => generation,
            // A recovery pass stopped by the shutdown that owns this attempt is
            // not a provider refusal: no byte reached the provider, so the row
            // is handed straight back to the next life of the dispatcher
            // instead of serving a backoff for a shutdown it did not cause.
            Err(RecoveryRefusalV1::Cancelled(_)) if control.is_cancelled() => {
                return Ok(DeliveryAttemptV1::CancelledByShutdown);
            }
            Err(refusal) => return Err(DeliveryAdapterError::Recovery(refusal)),
        };
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
            expected_state_generation,
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

/// One record's bound, in the shape both the ingress runtime and the admission
/// adapter take.
///
/// The deadline is the replay pass's own remaining wall budget, so a record can
/// never outlive the pass that started it. The cancellation is not a fresh
/// identity: it is the caller's own project-open token and the journey's own
/// stop token, read synchronously at every checkpoint, plus a relay that
/// carries the same signal into a sub-operation that is already in flight.
/// Minting a token here instead is what let an admission keep working — for up
/// to a five-second readiness budget — for a project that had already closed.
#[derive(Debug)]
pub(crate) struct ReplayIngestControlV1 {
    deadline_unix_micros: i64,
    /// The caller's own token — the project-open cancellation at startup, the
    /// journey's stop token on the live task. Read synchronously at every
    /// checkpoint, so a checkpoint can never race an asynchronous relay.
    caller: HostCancellationToken,
    /// The journey's stop token, so shutdown reaches inside a record too.
    stopping: HostCancellationToken,
    /// The very same signal in the shape a provider operation takes. It is
    /// flipped from the two tokens above — never minted as a new identity —
    /// both before a sub-operation starts and, by the relay the caller spawns,
    /// while one is already in flight.
    cancellation: CancellationToken,
}

impl ReplayIngestControlV1 {
    /// The bound a sub-operation inside this record runs under: never wider
    /// than the caller's remaining budget, and carrying the caller's own
    /// cancellation rather than a new one.
    fn operation_control(&self, budget_micros: i64) -> OperationControl {
        if self.caller.is_cancelled() || self.stopping.is_cancelled() {
            self.cancellation.cancel();
        }
        let now = tracedecay_application::now_micros().0;
        let deadline = now
            .saturating_add(budget_micros)
            .min(self.deadline_unix_micros);
        let remaining = deadline.saturating_sub(now).max(0);
        OperationControl::new(
            deadline,
            u64::try_from(remaining / 1_000).unwrap_or(0),
            self.cancellation.clone(),
        )
    }
}

impl IngressControlV1 for ReplayIngestControlV1 {
    fn now_unix_micros(&self) -> i64 {
        tracedecay_application::now_micros().0
    }

    fn deadline_unix_micros(&self) -> i64 {
        self.deadline_unix_micros
    }

    fn is_cancelled(&self) -> bool {
        self.caller.is_cancelled()
            || self.stopping.is_cancelled()
            || self.cancellation.is_cancelled()
    }
}

/// The wall-clock instant a monotonic pass deadline corresponds to.
///
/// The pass budget is a `tokio::time::Instant` and every bound inside a record
/// is UTC micros, so the remaining budget is what carries across — never a
/// fresh budget, which would silently widen the caller's deadline.
fn wall_deadline_micros(deadline: tokio::time::Instant) -> i64 {
    let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
    let remaining = i64::try_from(remaining.as_micros()).unwrap_or(i64::MAX);
    tracedecay_application::now_micros()
        .0
        .saturating_add(remaining)
}

/// Finite ceiling on provider calls this host will hold abandoned to a
/// non-returning provider before it refuses to start another one. An
/// abandoned call costs one parked thread; bounding them is what keeps "the
/// host stays usable" from becoming "the host grows a thread per hung call".
const MAX_ABANDONED_PROVIDER_CALLS: usize = 8;

/// How often the bounded wait re-checks the caller's cancellation.
const ISOLATION_POLL_MILLIS: u64 = 5;

/// The host's bounded-execution boundary for provider calls.
///
/// The supervisor's unwind boundary contains a panicking provider; it cannot
/// contain one that never returns, because containing that needs a thread the
/// host can walk away from. This is that thread. The provider's handshake runs
/// on a worker, the calling thread waits only for the operation's own live
/// budget, and a provider that does not answer inside it is **abandoned** —
/// the host returns a typed refusal and keeps running while the worker stays
/// parked in the provider. Abandoned workers are counted and finite: past the
/// ceiling the boundary refuses to start another call at all.
///
/// This lives at the composition root because the provider registry is
/// source-contracted to name no OS capability
/// (`product/architecture/memory-dependency-policy.json`): the registry
/// declares the boundary, the root supplies it (`tdmem-1107`).
#[derive(Debug)]
struct ThreadBoundedProviderCallV1 {
    abandoned: AtomicUsize,
    max_abandoned: usize,
}

impl ThreadBoundedProviderCallV1 {
    const fn new(max_abandoned: usize) -> Self {
        Self {
            abandoned: AtomicUsize::new(0),
            max_abandoned,
        }
    }
}

impl BoundedProviderCallV1 for ThreadBoundedProviderCallV1 {
    fn handshake_within(
        &self,
        budget_millis: u64,
        cancellation: &CancellationToken,
        work: ProviderHandshakeWorkV1,
    ) -> Result<Result<HandshakeResponse, CompositionLifecycleError>, BoundedCallRefusalV1> {
        let abandoned = self.abandoned.load(Ordering::Acquire);
        if abandoned >= self.max_abandoned {
            return Err(BoundedCallRefusalV1::Exhausted {
                abandoned,
                maximum: self.max_abandoned,
            });
        }
        let (answers, inbox) = mpsc::sync_channel(1);
        thread::Builder::new()
            .name("tdmem-provider-handshake".to_owned())
            .spawn(move || {
                // A send failure means the caller already abandoned this call.
                let _ = answers.send(work());
            })
            .map_err(|source| BoundedCallRefusalV1::Unavailable(source.to_string()))?;

        let mut remaining = Duration::from_millis(budget_millis);
        let slice = Duration::from_millis(ISOLATION_POLL_MILLIS);
        loop {
            let wait = remaining.min(slice);
            match inbox.recv_timeout(wait) {
                Ok(answer) => return Ok(answer),
                Err(RecvTimeoutError::Disconnected) => {
                    return Err(BoundedCallRefusalV1::Unavailable(
                        "bounded provider call ended without answering".to_owned(),
                    ));
                }
                Err(RecvTimeoutError::Timeout) => {
                    remaining = remaining.saturating_sub(wait);
                    if cancellation.is_cancelled() {
                        self.abandoned.fetch_add(1, Ordering::AcqRel);
                        return Err(BoundedCallRefusalV1::Cancelled);
                    }
                    if remaining.is_zero() {
                        self.abandoned.fetch_add(1, Ordering::AcqRel);
                        return Err(BoundedCallRefusalV1::Abandoned {
                            waited_millis: budget_millis,
                        });
                    }
                }
            }
        }
    }
}

/// Mounts the project's provider lifecycle supervisor over the composed
/// provider set.
///
/// This is where `tdmem-0504`'s supervisor becomes production code rather
/// than a reusable type: the journey obtains **every** readiness target from
/// it, so restart bounding, exact-scope ownership, predecessor-death
/// confirmation, adapter-panic containment, and fail-closed readiness
/// validation are on the only path a provider observation can take.
fn mount_supervised_provider_readiness(
    composition: Arc<ProjectMemoryProviderComposition>,
    registration_revision: u64,
    host_limits: ProviderLimits,
    provider_state_root: PathBuf,
) -> Result<SupervisedProviderReadinessV1, ObservationJourneyError> {
    let provider_id =
        OwnedProviderId::new(NATIVE_PROVIDER_ID).map_err(ObservationJourneyError::Contract)?;
    SupervisedProviderReadinessV1::new(
        composition,
        Arc::new(ThreadBoundedProviderCallV1::new(
            MAX_ABANDONED_PROVIDER_CALLS,
        )),
        provider_id,
        registration_revision,
        host_limits,
        SupervisedReadinessConfigV1 {
            restart_budget: RestartBudgetV1 {
                max_attempts_per_window: SUPERVISOR_RESTART_ATTEMPTS_PER_WINDOW,
                window_micros: SUPERVISOR_RESTART_WINDOW_MICROS,
                backoff_base_micros: SUPERVISOR_BACKOFF_BASE_MICROS,
                backoff_max_micros: SUPERVISOR_BACKOFF_MAX_MICROS,
            },
            shutdown_budget: ShutdownBudgetV1 {
                grace_micros: SUPERVISOR_GRACE_MICROS,
                kill_micros: SUPERVISOR_KILL_MICROS,
            },
            start_budget_micros: READINESS_DEADLINE_MICROS,
            handshake_budget_micros: READINESS_DEADLINE_MICROS,
            max_supervised_scopes: SUPERVISED_SCOPE_CEILING,
        },
    )
    .map_err(ObservationJourneyError::SupervisedReadiness)?
    // The Native provider is admitted to own exactly the state namespaces
    // under its own provider identity. A handshake that reports any other
    // namespace — a traversal out of the host-owned root, or another
    // authority's name — is a fail-closed readiness refusal here rather than
    // a namespace the host would then treat as this provider's state
    // (`tdmem-1107`).
    .with_admitted_state_namespace_prefix(NATIVE_PROVIDER_ID)
    .map_err(ObservationJourneyError::SupervisedReadiness)?
    // Containment, not merely validation: the host owns the state root, and a
    // validated readiness is granted a capability rooted at its admitted
    // namespace underneath it. That capability is the only provider state path
    // the host produces (`tdmem-1107`).
    .with_state_root(provider_state_root)
    .map_err(ObservationJourneyError::SupervisedReadiness)
}

/// Obtains a readiness target for one exact scope **through the mounted
/// provider lifecycle supervisor**, never straight from the registry.
///
/// The supervisor is what makes this path bounded: it owns exactly this exact
/// scope, refuses a request for any other, enforces a finite restart budget
/// and capped backoff instead of re-handshaking on every record, confirms a
/// predecessor incarnation is dead before it starts a replacement, contains an
/// adapter panic, and validates every readiness invariant before a target
/// exists. A refusal is typed degradation: the record is not admitted or
/// delivered, and the host keeps running.
fn readiness_target_for_scope(
    supervised: &SupervisedProviderReadinessV1,
    exact_scope: &OwnedExactScope,
    registration_revision: u64,
    host_limits: ProviderLimits,
    observe_capability: OwnedVersionedId,
    control: OperationControl,
) -> Result<ProviderTargetV1, ObservationJourneyError> {
    readiness_target_and_evidence_for_scope(
        supervised,
        exact_scope,
        registration_revision,
        host_limits,
        observe_capability,
        control,
    )
    .map(|(target, _)| target)
}

/// The same supervised pass, also returning the readiness evidence it proved.
///
/// One handshake, two answers. Delivery needs both — the address to send to and
/// the provider state identity restart recovery compares against its durable
/// expectation — and taking them from separate handshakes would let the journal
/// deliver to one incarnation while it verified another.
fn readiness_target_and_evidence_for_scope(
    supervised: &SupervisedProviderReadinessV1,
    exact_scope: &OwnedExactScope,
    registration_revision: u64,
    host_limits: ProviderLimits,
    observe_capability: OwnedVersionedId,
    control: OperationControl,
) -> Result<(ProviderTargetV1, ReadinessEvidenceV1), ObservationJourneyError> {
    let request = readiness_handshake_request(
        exact_scope,
        registration_revision,
        host_limits,
        observe_capability,
        control,
    )?;
    let (readiness, evidence) = supervised
        .ready_target_with_evidence(&request, tracedecay_application::now_micros().0)
        .map_err(ObservationJourneyError::SupervisedReadiness)?;
    let target = ProviderTargetV1 {
        provider_id: readiness.provider_id().clone(),
        provider_instance_id: readiness.provider_instance_id().to_owned(),
        registration_revision: readiness.registration_revision(),
        ready_receipt_digest: readiness.ready_receipt_sha256().to_owned(),
    };
    target
        .validate()
        .map_err(ObservationJourneyError::Journal)?;
    Ok((target, evidence))
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

/// What one record's bounded admission did.
///
/// The deadline case is separate from every other outcome because it is the
/// one where the runtime has *no* report: the blocking admission did not come
/// back inside the caller's budget. Nothing is lost — the record is canonical
/// and the journal owns its own watermark — but the pass has to say so with a
/// typed terminal rather than invent an empty report.
#[derive(Debug)]
pub(crate) enum RecordOutcomeV1 {
    /// Admission returned, with whatever it decided.
    Reported(IngressBatchReportV1),
    /// Admission did not return inside the caller's budget.
    DeadlineExceeded,
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
    /// The typed backpressure refusal that stopped the pass, when one did.
    /// Like a halt, the watermark stays where it was and the canonical store
    /// still holds the record — this is a lane that stopped taking work, never
    /// a record that was thrown away.
    pub(crate) shed: Option<BackpressureHaltV1>,
}

/// The retained owner for one project's observation journey.
///
/// It holds the registry handle, the durable journal, the delivery wake edge,
/// and the worker thread. Dropping it without [`Self::shutdown`] still strands
/// nothing: every lease carries its own expiry and any process can reap it.
pub(crate) struct ProjectObservationJourneyV1 {
    journal: Arc<SqliteObservationJournal>,
    wake: Arc<DeliveryWakeV1>,
    /// The one gate every admission on this journey is measured against, and
    /// the owner of this lane's backlog metrics.
    backpressure: Arc<BackpressureGateV1>,
    /// The provider lane this journey's queue pressure is accounted under.
    /// Naming it without a readiness handshake is what lets the journey
    /// republish real backlog on a pass that admitted nothing.
    provider_lane: ObservationLaneKeyV1,
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
                        shed: None,
                    });
                }
                check_replay_bounds(bounds, admitted)?;
                let record = self.source_record(stored)?;
                let report = match self.ingest_record(record, bounds).await? {
                    RecordOutcomeV1::Reported(report) => report,
                    RecordOutcomeV1::DeadlineExceeded => {
                        // The record did not return inside the caller's own
                        // budget. Nothing was committed for it that the
                        // watermark does not already describe, so the pass
                        // reports its typed terminal and the next one resumes
                        // from the journal.
                        return Err(ObservationJourneyError::DeadlineExceeded { admitted });
                    }
                };
                admitted = admitted.saturating_add(u64::from(report.appended));
                if let Some(stop) = report.stopped_on {
                    // The caller's own bound reached inside the record. The
                    // watermark holds at the named position and the canonical
                    // store still owns it. Journey shutdown is not a caller
                    // terminal — every record is journaled in its own
                    // transaction, so stopping loses nothing.
                    if self.stopping.is_cancelled() {
                        return Ok(ReplayPassV1 {
                            admitted,
                            halted: None,
                            shed: None,
                        });
                    }
                    return Err(match stop.reason {
                        IngressStopReasonV1::Cancelled => {
                            ObservationJourneyError::Cancelled { admitted }
                        }
                        IngressStopReasonV1::DeadlineExceeded => {
                            ObservationJourneyError::DeadlineExceeded { admitted }
                        }
                    });
                }
                if let Some(shed) = report.shed_on {
                    // The lane refused the record, so the pass stops here.
                    // Continuing would re-pay sanitization, digest derivation,
                    // and a readiness proof per record for a lane that is
                    // already refusing, which is exactly the foreground cost
                    // the gate exists to avoid. The watermark holds and the
                    // canonical store still owns the record.
                    self.report_backpressure(&shed);
                    return Ok(ReplayPassV1 {
                        admitted,
                        halted: None,
                        shed: Some(shed),
                    });
                }
                if let Some(halt) = report.halted_on {
                    return Ok(ReplayPassV1 {
                        admitted,
                        halted: Some(halt),
                        shed: None,
                    });
                }
            }
        }
        Ok(ReplayPassV1 {
            admitted,
            halted: None,
            shed: None,
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
        bounds: ReplayBoundsV1<'_>,
    ) -> Result<RecordOutcomeV1, ObservationJourneyError> {
        let journal = Arc::clone(&self.journal);
        let adapter = Arc::clone(&self.adapter);
        let wake = Arc::clone(&self.wake);
        let backpressure = Arc::clone(&self.backpressure);
        // The caller's bound, carried into the record rather than checked
        // around it. The deadline is what is left of the pass; the tokens are
        // the caller's own, read synchronously at every checkpoint.
        let control = Arc::new(ReplayIngestControlV1 {
            deadline_unix_micros: wall_deadline_micros(bounds.deadline),
            caller: bounds.cancellation.clone(),
            stopping: self.stopping.clone(),
            cancellation: CancellationToken::new(),
        });
        // Ingress reads the caller's tokens directly at every checkpoint, so
        // the stop before the append is synchronous and cannot race. This
        // relay covers the other half: a sub-operation that is *already
        // running* under an `OperationControl` — a readiness handshake with a
        // five-second budget — sees the same give-up while it is in flight
        // rather than only at the next record.
        let relay = {
            let caller = bounds.cancellation.clone();
            let stopping = self.stopping.clone();
            let record_cancellation = control.cancellation.clone();
            tokio::spawn(async move {
                tokio::select! {
                    () = caller.cancelled() => {}
                    () = stopping.cancelled() => {}
                }
                record_cancellation.cancel();
            })
        };
        // The admission path is what a coding agent's committed message waits
        // behind, so it is measured rather than assumed: the gate takes the
        // sample and sheds optional traffic while the journal itself is what
        // is slow. `Instant` is monotonic, so a clock step cannot fabricate a
        // budget breach.
        let started = std::time::Instant::now();
        let ingest_control = Arc::clone(&control);
        let task = tokio::task::spawn_blocking(move || {
            let ingress = IngressRuntimeV1::new(
                journal.as_ref(),
                adapter.as_ref(),
                wake.as_ref(),
                backpressure.as_ref(),
                ingest_control.as_ref(),
            );
            let resume = ingress.recover(&record.stream)?;
            ingress.ingest(&resume, std::slice::from_ref(&record))
        });
        // The caller is bounded even when the record is not: the blocking pool
        // is shared, and a pass that has spent its budget has to return rather
        // than wait for a turn on it.
        let joined = tokio::time::timeout_at(bounds.deadline + FOREGROUND_ABORT_GRACE, task).await;
        relay.abort();
        // Sampled on every path, including the ones that failed. A latency
        // sample taken only after a successful report leaves the lane blind to
        // exactly the admissions that hurt most — the slow ones that then
        // failed — and the breach run is what sheds optional traffic before
        // the next record pays the same cost.
        let elapsed = i64::try_from(started.elapsed().as_micros()).unwrap_or(i64::MAX);
        let foreground = self.backpressure.observe_foreground(elapsed);
        if !foreground.within_budget() {
            tracing::warn!(
                event = "memory_observation_foreground_budget_exceeded",
                elapsed_micros = elapsed,
                budget_micros = self.backpressure.policy().foreground_budget_micros,
                consecutive_breaches = self.backpressure.foreground_breaches(),
                journal = %self.journal_path.display(),
                "one canonical observation admission overran its foreground budget; a run of \
                 them sheds optional observation traffic before the next record is admitted, \
                 until an admission comes back inside it"
            );
        }
        match joined {
            Ok(joined) => Ok(RecordOutcomeV1::Reported(
                joined
                    .map_err(ObservationJourneyError::IngestTask)?
                    .map_err(ObservationJourneyError::Ingress)?,
            )),
            Err(_elapsed) => {
                // Tell the abandoned work to stop at its next checkpoint. It
                // holds nothing the next pass cannot recover: every decision
                // it could still commit carries its own watermark.
                control.cancellation.cancel();
                Ok(RecordOutcomeV1::DeadlineExceeded)
            }
        }
    }

    /// Reports one backpressure refusal with the measurements it was taken on.
    ///
    /// This is the lane's operational signal: queue size, queue bytes, the age
    /// of the oldest row still waiting, and which of them refused the record.
    fn report_backpressure(&self, shed: &BackpressureHaltV1) {
        let backlog = &shed.refusal.backlog;
        tracing::warn!(
            event = "memory_observation_backpressure_shed",
            source_sequence = shed.source_sequence.0,
            source_event_id = %shed.source_event_id,
            load_class = shed.refusal.load_class.as_wire(),
            reason = shed.refusal.reason.as_wire(),
            state = shed.refusal.state.as_wire(),
            queue_items = backlog.queue_items,
            queue_bytes = backlog.queue_bytes,
            max_queue_items = backlog.max_queue_items,
            max_queue_bytes = backlog.max_queue_bytes,
            utilization_ppm = backlog.utilization_ppm,
            projected_utilization_ppm = shed.refusal.projected_utilization_ppm,
            additional_bytes = shed.refusal.additional_bytes,
            oldest_backlog_age_micros = backlog.oldest_backlog_age_micros,
            foreground_latency_micros = backlog.foreground_latency_micros,
            journal = %self.journal_path.display(),
            "the observation lane refused a canonical record; the watermark holds and the record \
             is re-presented once the lane drains"
        );
    }

    /// The most recent backlog measurement this lane took, for telemetry and
    /// operational inspection. `None` until the first gated admission.
    pub(crate) fn backlog_metrics(&self) -> Option<QueueBacklogV1> {
        self.backpressure.metrics()
    }

    /// Re-reads the lane and republishes its backlog on the current instant.
    ///
    /// This is the only thing that keeps the published metric current. Ingress
    /// measures around the records it admits; delivery drains rows without
    /// ever passing through ingress; and a pass that admitted nothing measures
    /// nothing at all. So the lane is read again — from the journal, not from
    /// anything remembered — after every pass and at shutdown, which is what
    /// makes an idle lane's reported size, age, and state describe the journal
    /// as it is rather than as it was at the last admission.
    async fn refresh_backlog(&self) -> Result<QueueBacklogV1, ObservationJourneyError> {
        let journal = Arc::clone(&self.journal);
        let backpressure = Arc::clone(&self.backpressure);
        let lane = self.provider_lane.clone();
        let now_unix_micros = tracedecay_application::now_micros().0;
        tokio::task::spawn_blocking(move || {
            journal
                .lane_pressure(&lane)
                .map(|pressure| backpressure.observe(&pressure, now_unix_micros))
        })
        .await
        .map_err(ObservationJourneyError::IngestTask)?
        .map_err(ObservationJourneyError::Journal)
    }

    /// Refreshes the lane and publishes it whenever it is not nominal.
    ///
    /// Called on every replay pass, so a lane that is filling up is visible
    /// *before* it starts refusing work — a metric that only appeared on a
    /// refusal would tell an operator about the problem exactly one step too
    /// late. A nominal lane emits nothing, so this is not a log-rate loop.
    async fn report_backlog(&self) {
        let backlog = match self.refresh_backlog().await {
            Ok(backlog) => backlog,
            Err(error) => {
                tracing::debug!(
                    event = "memory_observation_backlog_refresh_failed",
                    error = %error,
                    "the observation lane's backlog could not be re-read"
                );
                return;
            }
        };
        if backlog.state == BackpressureStateV1::Nominal {
            return;
        }
        tracing::info!(
            event = "memory_observation_backlog_pressure",
            state = backlog.state.as_wire(),
            trigger = backlog.trigger.map(BackpressureReasonV1::as_wire),
            queue_items = backlog.queue_items,
            queue_bytes = backlog.queue_bytes,
            max_queue_items = backlog.max_queue_items,
            max_queue_bytes = backlog.max_queue_bytes,
            utilization_ppm = backlog.utilization_ppm,
            oldest_backlog_age_micros = backlog.oldest_backlog_age_micros,
            foreground_latency_micros = backlog.foreground_latency_micros,
            foreground_breaches = backlog.foreground_breaches,
            journal = %self.journal_path.display(),
            "the observation lane is under backpressure"
        );
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
                        journey.report_backlog().await;
                        if pass.shed.is_some() {
                            // The lane refused work. A drain is the only thing
                            // that can clear it, and ingress already woke the
                            // worker, so back off rather than re-presenting the
                            // same record at park rate against the same full
                            // lane. Nothing is lost: the watermark holds and
                            // the refusal was already reported typed.
                            tokio::time::sleep(LIVE_REPLAY_ERROR_BACKOFF).await;
                            continue;
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
        // Read the lane one last time rather than replaying whatever the last
        // admission happened to see: delivery may have drained rows since, and
        // a final admission may have crossed a threshold the remembered
        // measurement was taken one row before. A handover that reported the
        // stale figure would tell the next life the wrong thing.
        let backlog = match self.refresh_backlog().await {
            Ok(backlog) => Some(backlog),
            Err(error) => {
                // Reported, never swallowed: the handover then falls back to
                // the last measurement ingress took, which is stale by
                // construction and is labelled as such.
                tracing::warn!(
                    event = "memory_observation_backlog_refresh_failed_at_shutdown",
                    error = %error,
                    journal = %self.journal_path.display(),
                    "the observation lane could not be re-read at shutdown; the handover below \
                     is the last measurement admission took, not the journal as it stands"
                );
                self.backlog_metrics()
            }
        };
        if let Some(backlog) = backlog {
            // What the lane was still holding when it stopped. The rows are
            // durable and the next life resumes on them, so this is the
            // operational handover, not a loss report.
            tracing::info!(
                event = "memory_observation_backlog_at_shutdown",
                state = backlog.state.as_wire(),
                queue_items = backlog.queue_items,
                queue_bytes = backlog.queue_bytes,
                max_queue_items = backlog.max_queue_items,
                max_queue_bytes = backlog.max_queue_bytes,
                utilization_ppm = backlog.utilization_ppm,
                oldest_backlog_age_micros = backlog.oldest_backlog_age_micros,
                foreground_latency_micros = backlog.foreground_latency_micros,
                journal = %self.journal_path.display(),
                "observation lane backlog at shutdown"
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
                        // the row comes back on the journal's own capped
                        // exponential for the attempt the claim consumed — the
                        // same curve a recorded `provider_unavailable` rides,
                        // rather than a flat interval that would hammer an
                        // unreachable provider until its ceiling is gone. The
                        // journal's attempt ceiling still bounds it; nothing
                        // here retries a typed terminal.
                        retry_backoff: RetryBackoffV1::of(journal.policy()),
                        attempt_budget_micros: dispatch_policy.attempt_budget_micros,
                    };
                    // A drain, not a single batch: the wake edge is one
                    // collapsed signal, so a backlog that is already journalled
                    // would otherwise move `batch_max_items` per park interval
                    // with nothing to signal about it again. The bounds are
                    // derived from the dispatch policy revalidated against the
                    // journal's own retention policy, which is the only way to
                    // obtain them — this loop cannot widen them.
                    match dispatch_policy
                        .drain_bounds(journal.policy(), now)
                        .map_err(ObservationRuntimeError::from)
                        .and_then(|bounds| {
                            runtime.drain(&request, &bounds, || {
                                tracedecay_application::now_micros().0
                            })
                        }) {
                        Ok(report) => {
                            if report.totals.cancelled_before_dispatch > 0
                                || report.totals.cancelled_in_flight > 0
                            {
                                tracing::info!(
                                    event = "memory_observation_dispatch_cancelled",
                                    rounds = report.rounds,
                                    leased = report.totals.leased,
                                    cancelled_in_flight = report.totals.cancelled_in_flight,
                                    cancelled_before_dispatch =
                                        report.totals.cancelled_before_dispatch,
                                    "shutdown stopped an observation dispatch round; released rows stay pending"
                                );
                            }
                            for failure in &report.totals.failures {
                                tracing::warn!(
                                    event = "memory_observation_delivery_failed",
                                    observation_id = %failure.observation_id.as_str(),
                                    attempt = failure.attempt_number,
                                    lease_released = failure.lease_released,
                                    error = %failure.cause,
                                    "one observation delivery produced no receipt"
                                );
                            }
                            // Work the bounds cut short is real, durable, and
                            // eligible now. Re-arming the wake makes the next
                            // turn start at once instead of parking on a
                            // backlog nothing will signal about again; a
                            // shutdown stop is not re-armed, because the next
                            // wait must return `ShutdownRequested`.
                            if report.more_work_pending()
                                && report.stop != DrainStopV1::ShutdownRequested
                            {
                                tracing::debug!(
                                    event = "memory_observation_dispatch_yielded",
                                    rounds = report.rounds,
                                    leased = report.totals.leased,
                                    stop = ?report.stop,
                                    "observation dispatch reached its drain bound with work still queued"
                                );
                                wake.signal();
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
    // Shared before the adapters are built: the delivery adapter's recovery
    // gate reads the same durable journal the worker delivers from, so the
    // acknowledged watermark it compares against is the one this dispatcher
    // actually advances.
    let journal = Arc::new(
        SqliteObservationJournal::open(&journal_path, inputs.policy.retention).map_err(
            |source| ObservationJourneyError::JournalOpen {
                path: journal_path.clone(),
                source,
            },
        )?,
    );
    let sanitizer = ObservationSanitizer::new().map_err(ObservationJourneyError::Hygiene)?;

    // One supervised lifecycle owner for this project's journey, shared by
    // admission and delivery so both observe the same incarnation, the same
    // restart budget, and the same typed degradation.
    let supervised_readiness = Arc::new(mount_supervised_provider_readiness(
        Arc::clone(&inputs.composition),
        inputs.registration_revision,
        inputs.host_limits,
        inputs.store_data_root.join(PROVIDER_STATE_DIR_NAME),
    )?);

    // The lane this journey queues in, named from the registration alone so
    // pressure can be measured without a readiness handshake.
    let provider_lane = ObservationLaneKeyV1 {
        provider_id: OwnedProviderId::new(NATIVE_PROVIDER_ID)
            .map_err(ObservationJourneyError::Contract)?,
        registration_revision: inputs.registration_revision,
    };
    provider_lane
        .validate()
        .map_err(ObservationJourneyError::Journal)?;

    let adapter = Arc::new(CanonicalObservationAdmissionAdapterV1 {
        context: AdmissionContextV1 {
            profile_id: inputs.profile_id,
            scope: inputs.scope,
            readiness: Arc::clone(&supervised_readiness),
            provider_lane: provider_lane.clone(),
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
        readiness: supervised_readiness,
        registration_revision: inputs.registration_revision,
        limits: inputs.host_limits,
        observe_capability,
        recovery: ObservationRecoveryGateV1 {
            journal: Arc::clone(&journal),
            provider_id: NATIVE_PROVIDER_ID.to_owned(),
            registration_revision: inputs.registration_revision,
            source_authority: SourceAuthorityV1::HostSession,
            source_stream: source_stream.clone(),
            budget: RecoveryBudgetV1 {
                max_automatic_attempts: RECOVERY_MAX_AUTOMATIC_ATTEMPTS,
            },
        },
    });

    // Validated at mount, exactly like the retention and dispatch policies:
    // an ingress whose bounds were never checked would be an ingress with no
    // bounds at all until the first saturation.
    let backpressure = Arc::new(
        BackpressureGateV1::new(inputs.policy.backpressure)
            .map_err(ObservationJourneyError::Journal)?,
    );
    let journey = Arc::new(ProjectObservationJourneyV1 {
        journal,
        wake: Arc::new(DeliveryWakeV1::new()),
        backpressure,
        provider_lane,
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
                shed: None,
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

/// Whether a startup replay refusal is one a later pass can clear.
///
/// The rule is fail-closed: a refusal counts as retryable only when the store
/// itself named a transport-level failure, or when the blocking ingest task was
/// cancelled rather than lost. Everything else — a canonical record the
/// contract refuses, an identity or cursor disagreement, a journal or ingress
/// refusal, a panicked ingest task, a scope or hygiene failure — describes the
/// evidence, not the attempt, and replaying the same bytes produces the same
/// answer forever.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum StartupReplayRecoverabilityV1 {
    /// A later pass over the same watermark can succeed.
    Retryable,
    /// No retry can clear this. The commit stays undelivered until the
    /// underlying evidence or code is repaired.
    Permanent,
}

/// Classifies one startup replay refusal.
pub(crate) fn startup_replay_recoverability(
    error: &ObservationJourneyError,
) -> StartupReplayRecoverabilityV1 {
    match error {
        // The canonical store reported a storage-layer failure: a busy
        // database, a transient I/O error, a lock it could not take. The rows
        // are intact and the next pass reads them.
        ObservationJourneyError::Replay(ObservationStoreError::Storage { .. }) => {
            StartupReplayRecoverabilityV1::Retryable
        }
        // The blocking-pool task was cancelled, not lost: its transaction
        // either committed or did not, and the watermark re-presents the
        // record either way. A panicked task is a different thing entirely and
        // falls through to permanent.
        ObservationJourneyError::IngestTask(join) if join.is_cancelled() => {
            StartupReplayRecoverabilityV1::Retryable
        }
        _ => StartupReplayRecoverabilityV1::Permanent,
    }
}

/// The whole product-owned mount sequence behind one call, so the composition
/// root holds a single seam: mount the journey, run the authoritative startup
/// replay under the project-open cancellation, then start the bounded live
/// replay edge over the same store.
///
/// Cancellation during startup replay is returned typed as
/// [`ObservationJourneyError::Cancelled`]; the journey is dropped with the
/// refused open and nothing past the durable watermark is lost.
///
/// Every other startup replay refusal is **classified** rather than swallowed,
/// because "live replay will retry it" is only true of a failure a retry can
/// clear. A retryable refusal — a canonical store that was busy, a blocking
/// ingest task the runtime cancelled — leaves the watermark where it was and
/// the bounded live replay task genuinely converges on it, so the mount
/// succeeds and the refusal is logged with its retry path. A permanent one —
/// an unreadable or contract-violating canonical record, a journal or ingress
/// refusal, a panicked ingest task — cannot be cleared by replaying the same
/// bytes again, and a mount that reported success over it would leave a
/// committed observation undelivered for as long as the project stayed open
/// while every readiness surface said the journey was healthy. That is
/// returned typed as [`ObservationJourneyError::StartupReplayPermanent`], so
/// project open fails with the reason instead of starting degraded in silence.
///
/// Mount and live-replay-start failures are returned typed.
pub(crate) async fn mount_and_replay<S>(
    inputs: ObservationJourneyMountInputsV1,
    observation_store: S,
    cancellation: &HostCancellationToken,
) -> Result<Arc<ProjectObservationJourneyV1>, ObservationJourneyError>
where
    S: ObservationAdmissionPort + 'static,
{
    let journey = mount_project_observation_journey(inputs)?;
    let admitted = match run_startup_replay(journey.as_ref(), &observation_store, cancellation)
        .await
    {
        Ok(pass) => pass.admitted,
        Err(error @ ObservationJourneyError::Cancelled { .. }) => return Err(error),
        Err(error) => {
            let recoverability = startup_replay_recoverability(&error);
            match recoverability {
                StartupReplayRecoverabilityV1::Retryable => {
                    tracing::error!(
                        event = "memory_observation_startup_replay_failed",
                        error = %error,
                        recoverability = "retryable",
                        journal = %journey.journal_path().display(),
                        "project observation startup replay failed on a retryable condition; the \
                         project server stays up and live replay retries from the durable \
                         watermark"
                    );
                    0
                }
                StartupReplayRecoverabilityV1::Permanent => {
                    tracing::error!(
                        event = "memory_observation_startup_replay_failed",
                        error = %error,
                        recoverability = "permanent",
                        journal = %journey.journal_path().display(),
                        "project observation startup replay failed permanently; no retry can \
                         clear it, so project open is refused rather than reporting a healthy \
                         journey over an undelivered commit"
                    );
                    return Err(ObservationJourneyError::StartupReplayPermanent {
                        source: Box::new(error),
                    });
                }
            }
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
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

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
    use crate::store::GlobalDbObservationStore;

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
            Self::with_state_generation(0)
        }

        /// An incarnation reporting `state_generation`.
        ///
        /// A non-zero generation is what makes "the verified generation
        /// reached the provider call" checkable: the fabric refuses a call
        /// whose `expected_state_generation` is not this incarnation's own, so
        /// a hardcoded expectation cannot reach a settlement.
        fn with_state_generation(state_generation: u64) -> Self {
            let capabilities = BTreeSet::from([
                OwnedVersionedId::new("provider.health.v1").expect("health capability"),
                OwnedVersionedId::new("observation.accept.v1").expect("observe capability"),
                OwnedVersionedId::new("recall.query.v1").expect("recall capability"),
            ]);
            let descriptor = ProviderDescriptor::new(
                OwnedProviderId::new(NATIVE_PROVIDER_ID).expect("native provider"),
                "0".repeat(64),
                "journey-test-v1",
                state_generation,
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
                // Inside the namespace the mount admits for the Native
                // provider; a namespace outside it is a readiness refusal.
                state_namespace: Some("tracedecay.native.journey-test".to_owned()),
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

    /// Mounts the production journey over one caller-supplied incarnation,
    /// exactly as the composition root does.
    ///
    /// Restart-recovery tests need to move the provider state identity the
    /// journal compares against, which means owning the port the mount runs
    /// over.
    struct RecoveryJourneyFixture {
        _runtime: HostAdmissionTestRuntimeV1,
        store: GlobalDbObservationStore,
        journey: Arc<ProjectObservationJourneyV1>,
        project_id: ProjectId,
        profile_id: UserProfileId,
        resolved_scope: ResolvedScope,
    }

    async fn mount_journey_over_port(
        temp: &TempDir,
        project: &str,
        profile: &str,
        port: Arc<JourneyNativePort>,
    ) -> RecoveryJourneyFixture {
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
        let profile_id = UserProfileId::new(profile).expect("profile id");
        let resolved_scope = scope(project_id.clone());
        let journal_root = temp.path().join("journey");
        std::fs::create_dir_all(&journal_root).expect("journal root");
        let journey = mount_project_observation_journey(ObservationJourneyMountInputsV1 {
            composition: composition(port),
            profile_id: profile_id.clone(),
            scope: resolved_scope.clone(),
            authoritative_project_id: project_id.clone(),
            store_data_root: journal_root,
            registration_revision: 1,
            host_limits: super::super::native_provider::native_provider_limits(),
            policy: ObservationJourneyPolicyV1::project_default(),
        })
        .expect("mounted journey");
        RecoveryJourneyFixture {
            _runtime: runtime,
            store,
            journey,
            project_id,
            profile_id,
            resolved_scope,
        }
    }

    /// One row of the mounted journey's durable recovery record.
    fn recovery_row(journal_path: &Path) -> Option<(String, i64, i64, Option<String>)> {
        let connection = rusqlite::Connection::open(journal_path).expect("journal");
        connection
            .query_row(
                "SELECT implementation_identity_sha256, state_generation, \
                 automatic_repair_attempts, last_defect \
                 FROM tdmem_observation_recovery_v1",
                [],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, i64>(1)?,
                        row.get::<_, i64>(2)?,
                        row.get::<_, Option<String>>(3)?,
                    ))
                },
            )
            .ok()
    }

    fn journal_counts(journal_path: &Path) -> (i64, String) {
        let connection = rusqlite::Connection::open(journal_path).expect("journal");
        let receipts: i64 = connection
            .query_row(
                "SELECT COUNT(*) FROM tdmem_observation_receipt_v1",
                [],
                |row| row.get(0),
            )
            .expect("receipt count");
        let state: String = connection
            .query_row(
                "SELECT state FROM tdmem_observation_delivery_v1",
                [],
                |row| row.get(0),
            )
            .expect("delivery state");
        (receipts, state)
    }

    /// tdmem-0506, mounted. The generation a delivery call declares is the one
    /// restart recovery verified against this incarnation's own readiness
    /// evidence, and the gate's decision is written through the journey's real
    /// journal.
    ///
    /// The fabric refuses any call whose `expected_state_generation` is not the
    /// ready incarnation's, so a hardcoded expectation — the defect this bead
    /// exists to remove — never reaches a settlement at all.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn mounted_delivery_declares_the_state_generation_recovery_verified() {
        let temp = TempDir::new().expect("temporary journey root");
        let port = Arc::new(JourneyNativePort::with_state_generation(5));
        let fixture = mount_journey_over_port(
            &temp,
            "project.observation-recovery-generation",
            "profile.observation-recovery",
            Arc::clone(&port),
        )
        .await;
        let store = &fixture.store;
        let journey = &fixture.journey;
        let project_id = &fixture.project_id;

        let session_id = SessionId::new("session.observation-recovery").expect("session id");
        store
            .persist_observation(anchored_write(canonical_observation(
                project_id,
                &session_id,
                "recovery journey text",
            )))
            .await
            .expect("canonical observation commit");
        let admitted = journey
            .replay_canonical_observations(store, REPLAY_LIVE_PAGES, open_bounds())
            .await
            .expect("canonical replay")
            .admitted;
        assert!(admitted <= 1, "unexpected admitted count {admitted}");

        journey.wake_delivery();
        let (state, attempts) = wait_for_settlement(journey.journal_path()).await;
        assert_eq!(
            (state.as_str(), attempts),
            ("rejected", 1),
            "delivery never reached the provider under the verified generation: {}",
            journal_snapshot(journey.journal_path())
        );

        let (identity, generation, repair_attempts, defect) =
            recovery_row(journey.journal_path()).expect("recovery record written by the gate");
        assert_eq!(identity, "0".repeat(64));
        assert_eq!(
            generation, 5,
            "the gate accepted a generation that is not this incarnation's"
        );
        assert_eq!(repair_attempts, 0);
        assert_eq!(defect, None);

        let failures = journey
            .shutdown(tokio::time::Instant::now() + Duration::from_secs(2))
            .await;
        assert!(failures.is_empty(), "{failures:?}");
    }

    /// tdmem-0506, mounted. A durable recovery record naming a different
    /// implementation identity refuses delivery *before* the provider is
    /// called: no receipt, no settlement, the row still deliverable, and the
    /// refusal recorded once however many times the dispatcher retries it.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_moved_provider_identity_refuses_mounted_delivery_before_the_provider_call() {
        let temp = TempDir::new().expect("temporary journey root");
        let port = Arc::new(JourneyNativePort::with_state_generation(5));
        let fixture = mount_journey_over_port(
            &temp,
            "project.observation-recovery-identity",
            "profile.observation-recovery",
            Arc::clone(&port),
        )
        .await;
        let store = &fixture.store;
        let journey = &fixture.journey;
        let project_id = &fixture.project_id;
        let profile_id = &fixture.profile_id;
        let resolved_scope = &fixture.resolved_scope;

        // A previous life of this host converged with a *different*
        // implementation under the same pinned registration. Nothing about the
        // schema or the generation moved, so only the identity comparison can
        // catch it.
        let session_id = SessionId::new("session.observation-recovery").expect("session id");
        let exact_scope = exact_scope_for_session(profile_id, resolved_scope, session_id.as_str())
            .expect("exact scope");
        {
            let connection = rusqlite::Connection::open(journey.journal_path()).expect("journal");
            connection
                .execute(
                    "INSERT INTO tdmem_observation_recovery_v1 (\
                         provider_id, registration_revision, source_authority, \
                         exact_scope_sha256, source_stream, implementation_identity_sha256, \
                         state_schema_version, state_generation, replay_position_retained, \
                         automatic_repair_attempts, updated_at_micros) \
                     VALUES (?1, 1, 'host_session', ?2, ?3, ?4, 'journey-test-v1', 5, 0, 0, 1)",
                    rusqlite::params![
                        NATIVE_PROVIDER_ID,
                        exact_scope.exact_scope_sha256(),
                        SESSION_SOURCE_STREAM,
                        "1".repeat(64),
                    ],
                )
                .expect("seeded recovery record");
        }

        store
            .persist_observation(anchored_write(canonical_observation(
                project_id,
                &session_id,
                "recovery journey text",
            )))
            .await
            .expect("canonical observation commit");
        let admitted = journey
            .replay_canonical_observations(store, REPLAY_LIVE_PAGES, open_bounds())
            .await
            .expect("canonical replay")
            .admitted;
        assert!(admitted <= 1, "unexpected admitted count {admitted}");

        journey.wake_delivery();
        let refused = tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                if let Some((_, _, attempts, Some(defect))) = recovery_row(journey.journal_path()) {
                    return (attempts, defect);
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await;
        let (attempts, defect) = match refused {
            Ok(refused) => refused,
            Err(_) => panic!(
                "the mounted recovery gate never refused: {}",
                journal_snapshot(journey.journal_path())
            ),
        };
        assert_eq!(defect, "implementation_identity_changed");
        assert_eq!(
            attempts, 1,
            "one incarnation is one assessment however often the dispatcher retries it"
        );

        // The provider was never asked: no receipt exists, and the row is
        // still deliverable rather than settled.
        let (receipts, state) = journal_counts(journey.journal_path());
        assert_eq!(receipts, 0, "a refused recovery must produce no receipt");
        assert!(
            state == "pending" || state == "leased",
            "a refused recovery must leave the row deliverable, found {state}"
        );
        assert_eq!(port.observe_calls.load(Ordering::Relaxed), 0);

        let failures = journey
            .shutdown(tokio::time::Instant::now() + Duration::from_secs(2))
            .await;
        assert!(failures.is_empty(), "{failures:?}");
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
    /// *inside* a record with a typed terminal, and the durable watermark
    /// holds exactly the records that were committed before it. A later open
    /// resumes from that watermark under its own token and admits everything
    /// the cancelled open did not commit.
    ///
    /// The cancellation fires during the first record's readiness handshake —
    /// the expensive step inside admission — so this is the case a
    /// between-records check cannot catch: the record was fully admitted and
    /// is then *not* committed, because the caller stopped wanting it before
    /// the append. Committing it anyway would charge a closing project for
    /// work it gave up on and move a watermark on its behalf.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn startup_replay_cancelled_inside_a_record_returns_typed_terminal_and_holds_watermark() {
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
            matches!(error, ObservationJourneyError::Cancelled { admitted: 0 }),
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
        assert_eq!(
            watermark, None,
            "the record the caller cancelled during must not be committed"
        );
        let snapshot = journal_snapshot(fixture.journey.journal_path());
        assert!(
            snapshot.starts_with("journal=0 delivery=0 "),
            "nothing may be journaled after the caller gave up: {snapshot}"
        );

        // Nothing was lost either: both records are still the canonical
        // store's, and the next open under its own token admits both exactly
        // once.
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
                admitted: 2,
                halted: None,
                shed: None,
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

    /// Acceptance: the mounted lane's published backlog is re-read from the
    /// journal rather than replayed from whatever the last admission measured.
    ///
    /// Ingress only measures around records it admits, and delivery moves rows
    /// to terminal without ever passing through ingress. So a lane that is
    /// quiet — the normal case — would otherwise keep reporting the pressure
    /// of the last append forever, which is the one reading guaranteed to be
    /// out of date.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn the_mounted_lane_republishes_backlog_read_from_the_journal() {
        let temp = TempDir::new().expect("temporary journey root");
        let fixture = mount_hygiene_fixture(&temp, "project.observation-backlog").await;
        let settled = canonical_observation(
            &fixture.project_id,
            &SessionId::new("session.observation-backlog").expect("session id"),
            "a settled record for the lane",
        );
        let records = SettledRecordsPort::single(settled_record(1, settled));
        let pass = run_startup_replay(
            fixture.journey.as_ref(),
            &records,
            &HostCancellationToken::new(),
        )
        .await
        .expect("startup replay");
        assert_eq!(pass.admitted, 1);

        // Ingress republished the lane after the append it committed, so a
        // measurement exists at all.
        let after_append = fixture
            .journey
            .backlog_metrics()
            .expect("an admitted record must publish the lane it landed in");

        // Re-reading is what keeps it current. Whatever the delivery worker
        // has done since, the published reading has to agree with the journal.
        fixture.journey.report_backlog().await;
        let published = fixture
            .journey
            .backlog_metrics()
            .expect("a refreshed lane must still publish");
        let pressure = fixture
            .journey
            .journal
            .lane_pressure(&fixture.journey.provider_lane)
            .expect("lane pressure");
        assert_eq!(published.queue_items, pressure.queue_items);
        assert_eq!(published.queue_bytes, pressure.queue_bytes);
        assert_eq!(published.max_queue_items, pressure.max_queue_items);
        assert_eq!(published.max_queue_bytes, pressure.max_queue_bytes);
        assert!(
            published.observed_at_unix_micros >= after_append.observed_at_unix_micros,
            "a refresh must publish a later instant than the append that preceded it"
        );

        let failures = fixture
            .journey
            .shutdown(tokio::time::Instant::now() + Duration::from_secs(5))
            .await;
        assert!(failures.is_empty(), "{failures:?}");
    }

    /// Acceptance: a mounted admission that ignores its own budget still
    /// returns inside the *caller's* bound, with a typed terminal, an
    /// unmoved watermark, and a foreground latency sample — and the record it
    /// abandoned is still the canonical store's to re-present.
    ///
    /// This is the production path, not a hand-driven gate. The readiness
    /// handshake is the expensive step inside admission; here it does not come
    /// back, which is exactly what a wedged or overloaded provider looks like.
    /// Without a bound on the caller, one such record parks the replay pass for
    /// as long as the provider feels like taking — orders of magnitude past the
    /// declared 250 ms foreground budget — and the pass deadline, checked only
    /// between records, never gets a turn.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_mounted_admission_that_ignores_its_budget_returns_at_the_caller_bound() {
        let temp = TempDir::new().expect("temporary journey root");
        let fixture = mount_hygiene_fixture(&temp, "project.observation-slow").await;
        let settled = canonical_observation(
            &fixture.project_id,
            &SessionId::new("session.observation-slow").expect("session id"),
            "a settled record behind a wedged provider",
        );
        let records = SettledRecordsPort::single(settled_record(1, settled));

        // A provider that returns only when this test lets it. Nothing but the
        // caller's own bound can end the wait, which is the point.
        let release = Arc::new(AtomicBool::new(false));
        let waiter = Arc::clone(&release);
        fixture.port.on_handshake(move || {
            let give_up = std::time::Instant::now() + Duration::from_secs(10);
            while !waiter.load(Ordering::Relaxed) && std::time::Instant::now() < give_up {
                std::thread::sleep(Duration::from_millis(10));
            }
        });

        let cancellation = HostCancellationToken::new();
        let bounds = ReplayBoundsV1 {
            cancellation: &cancellation,
            deadline: tokio::time::Instant::now() + Duration::from_millis(150),
        };
        let started = std::time::Instant::now();
        let error = fixture
            .journey
            .replay_canonical_observations(&records, 1, bounds)
            .await
            .expect_err("a pass that ran out of budget must not report a completed replay");
        let elapsed = started.elapsed();

        assert!(
            matches!(
                error,
                ObservationJourneyError::DeadlineExceeded { admitted: 0 }
            ),
            "unexpected terminal: {error}"
        );
        assert!(
            elapsed < Duration::from_secs(5),
            "the pass must return on its own bound rather than on the provider's, took {elapsed:?}"
        );

        // Nothing was committed for the record the pass gave up on, so the
        // watermark is exactly where it was and the canonical store still owns
        // it.
        let watermark = fixture
            .journey
            .journal
            .maximum_replay_sequence(
                SourceAuthorityV1::HostSession,
                &fixture.journey.source_stream,
            )
            .expect("watermark");
        assert_eq!(watermark, None);
        let snapshot = journal_snapshot(fixture.journey.journal_path());
        assert!(snapshot.starts_with("journal=0 delivery=0 "), "{snapshot}");

        // The admission was measured even though it never produced a report.
        // A sample taken only on success would leave the lane blind to exactly
        // the admissions that hurt, and the breach run is what sheds optional
        // traffic before the next record pays the same cost.
        let budget = fixture
            .journey
            .backpressure
            .policy()
            .foreground_budget_micros;
        let sample = fixture
            .journey
            .backpressure
            .foreground_sample()
            .expect("a foreground sample must be taken on every path");
        assert!(
            sample > budget,
            "a {sample}us admission must be recorded as over the {budget}us budget"
        );
        assert_eq!(fixture.journey.backpressure.foreground_breaches(), 1);

        // Let the provider go and prove the abandoned record is still
        // admittable: nothing was dropped, only refused.
        release.store(true, Ordering::Relaxed);
        let pass = run_startup_replay(
            fixture.journey.as_ref(),
            &records,
            &HostCancellationToken::new(),
        )
        .await
        .expect("resumed replay");
        assert_eq!(pass.admitted, 1);
        let failures = fixture
            .journey
            .shutdown(tokio::time::Instant::now() + Duration::from_secs(5))
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
                shed: None,
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

    // ---------------------------------------------------------------- //
    // Startup replay classification: a permanent refusal never mounts. //
    // ---------------------------------------------------------------- //

    /// A canonical store whose replay refuses with caller-chosen failures
    /// before it answers.
    ///
    /// The refusal comes back through the same trait method the real store
    /// answers on, so the classification runs over a real replay refusal
    /// rather than over a constructed error value.
    struct RefusingReplayPort {
        /// Refusals still to hand out; popped from the end.
        refusals: Mutex<Vec<ObservationStoreError>>,
        /// What replay returns once the refusals are spent.
        then: Vec<StoredObservation>,
    }

    impl ObservationAdmissionPort for RefusingReplayPort {
        async fn read_admitted_observation(
            &self,
            _observation_id: &CanonicalObservationIdV1,
        ) -> Result<Option<StoredObservation>, ObservationStoreError> {
            Ok(None)
        }

        async fn replay_admitted_observations(
            &self,
            request: ObservationReplayRequest,
        ) -> Result<Vec<StoredObservation>, ObservationStoreError> {
            let next = match self.refusals.lock() {
                Ok(mut slot) => slot.pop(),
                Err(poisoned) => poisoned.into_inner().pop(),
            };
            if let Some(error) = next {
                return Err(error);
            }
            Ok(self
                .then
                .iter()
                .filter(|record| record.sequence() > request.after_sequence())
                .take(request.limit())
                .cloned()
                .collect())
        }
    }

    fn busy_store_failure() -> ObservationStoreError {
        ObservationStoreError::Storage {
            operation: "replay_admitted_observations",
            source: Box::new(std::io::Error::other("canonical store was busy")),
        }
    }

    /// Mount inputs for a journey whose only collaborator under test is the
    /// canonical replay port.
    fn classification_mount_inputs(
        temp: &TempDir,
        project: &str,
    ) -> ObservationJourneyMountInputsV1 {
        let project_id = ProjectId::new(project).expect("project id");
        let journal_root = temp.path().join("journey");
        std::fs::create_dir_all(&journal_root).expect("journal root");
        ObservationJourneyMountInputsV1 {
            composition: composition(Arc::new(JourneyNativePort::new())),
            profile_id: UserProfileId::new("profile.observation-startup-classification")
                .expect("profile id"),
            scope: scope(project_id.clone()),
            authoritative_project_id: project_id,
            store_data_root: journal_root,
            registration_revision: 1,
            host_limits: super::super::native_provider::native_provider_limits(),
            policy: ObservationJourneyPolicyV1::project_default(),
        }
    }

    /// Acceptance: the classification is fail-closed. Only a storage-layer
    /// refusal and a cancelled ingest task are retryable; everything that
    /// describes the *evidence* is permanent, because replaying the same bytes
    /// produces the same answer forever.
    #[test]
    fn startup_replay_recoverability_is_fail_closed() {
        assert_eq!(
            startup_replay_recoverability(&ObservationJourneyError::Replay(busy_store_failure())),
            StartupReplayRecoverabilityV1::Retryable
        );
        assert_eq!(
            startup_replay_recoverability(&ObservationJourneyError::Replay(
                ObservationStoreError::CursorObservationMismatch
            )),
            StartupReplayRecoverabilityV1::Permanent
        );
        assert_eq!(
            startup_replay_recoverability(&ObservationJourneyError::Ingress(
                ObservationRuntimeError::InvalidDispatchRequest {
                    field: "attempt_budget_micros"
                }
            )),
            StartupReplayRecoverabilityV1::Permanent
        );
        assert_eq!(
            startup_replay_recoverability(&ObservationJourneyError::Journal(
                ObservationJournalError::EnvelopeDigestMismatch
            )),
            StartupReplayRecoverabilityV1::Permanent
        );
        assert_eq!(
            startup_replay_recoverability(&ObservationJourneyError::EntropyUnavailable),
            StartupReplayRecoverabilityV1::Permanent
        );
    }

    /// Acceptance: a permanent startup replay refusal refuses the mount.
    ///
    /// This is the defect the catch-all used to hide. A canonical record the
    /// contract cannot read is not something a retry clears, so a mount that
    /// substituted `admitted = 0` and returned `Ok` left a committed
    /// observation undelivered for as long as the project stayed open while
    /// project open reported success.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_permanent_startup_replay_failure_refuses_the_mount() {
        let temp = TempDir::new().expect("temporary journey root");
        let store = RefusingReplayPort {
            refusals: Mutex::new(vec![ObservationStoreError::CursorObservationMismatch]),
            then: Vec::new(),
        };
        let outcome = mount_and_replay(
            classification_mount_inputs(&temp, "project.observation-permanent"),
            store,
            &HostCancellationToken::new(),
        )
        .await;
        let Err(error) = outcome else {
            panic!("a permanent startup replay refusal must not mount a healthy journey");
        };
        assert!(
            matches!(
                error,
                ObservationJourneyError::StartupReplayPermanent { .. }
            ),
            "unexpected terminal: {error}"
        );
    }

    /// The same is true of an ingress refusal and of a journal refusal that
    /// reaches the pass as an error: neither is an attempt that can be retried.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_permanent_replay_limit_refusal_refuses_the_mount() {
        let temp = TempDir::new().expect("temporary journey root");
        let store = RefusingReplayPort {
            refusals: Mutex::new(vec![ObservationStoreError::InvalidReplayLimit {
                limit: 0,
                max: 512,
            }]),
            then: Vec::new(),
        };
        let outcome = mount_and_replay(
            classification_mount_inputs(&temp, "project.observation-permanent-limit"),
            store,
            &HostCancellationToken::new(),
        )
        .await;
        let Err(error) = outcome else {
            panic!("a permanent startup replay refusal must not mount a healthy journey");
        };
        assert!(
            matches!(
                error,
                ObservationJourneyError::StartupReplayPermanent { .. }
            ),
            "unexpected terminal: {error}"
        );
    }

    /// Acceptance: a retryable refusal keeps the project open *and* the retry
    /// it promises actually happens.
    ///
    /// The mount is only allowed to survive a storage-layer refusal because
    /// live replay converges on the record afterwards, so the test holds the
    /// mount to that claim rather than to the `Ok` alone.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_retryable_startup_replay_failure_mounts_and_live_replay_converges() {
        let temp = TempDir::new().expect("temporary journey root");
        let project_id =
            ProjectId::new("project.observation-startup-classification").expect("project id");
        let session_id =
            SessionId::new("session.observation-startup-retryable").expect("session id");
        let record = settled_record(
            1,
            canonical_observation(&project_id, &session_id, "settled behind a busy store"),
        );
        let store = RefusingReplayPort {
            refusals: Mutex::new(vec![busy_store_failure()]),
            then: vec![record],
        };
        let journey = mount_and_replay(
            classification_mount_inputs(&temp, "project.observation-startup-classification"),
            store,
            &HostCancellationToken::new(),
        )
        .await
        .expect("a retryable startup replay refusal keeps the project open");
        let (state, attempts) = wait_for_settlement(journey.journal_path()).await;
        assert_eq!(
            (state.as_str(), attempts),
            ("rejected", 1),
            "live replay never converged on the record the busy store withheld: {}",
            journal_snapshot(journey.journal_path())
        );
        journey
            .shutdown(tokio::time::Instant::now() + Duration::from_secs(5))
            .await;
    }
}
