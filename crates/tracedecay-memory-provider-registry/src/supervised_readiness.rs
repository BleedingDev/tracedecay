//! The mountable side of provider lifecycle supervision.
//!
//! [`supervisor`](crate::supervisor) owns the transport-agnostic state
//! machine. This module owns the two things a composition root actually needs
//! to mount it: a concrete [`ProviderLifecycleAdapterV1`] over a composed
//! provider set, and a per-exact-scope owner that turns one supervised
//! readiness pass into the same [`ProviderReadinessTargetV1`] the root already
//! consumes.
//!
//! # Why the composed provider set is a real lifecycle adapter
//!
//! The first mounted provider is TraceDecay Native, and it is constructed
//! in-process by [`ProjectMemoryProviderComposition`]. Its instance therefore
//! has no lifetime distinct from the composition: there is no child to spawn
//! and none to reap, so [`CompositionLifecycleAdapterV1::start`] performs no
//! spawn and [`CompositionLifecycleAdapterV1::request_stop`] confirms death
//! immediately. That is stated plainly rather than dressed up — for this
//! topology supervision contributes **readiness validation, typed
//! degradation, enforced restart pacing, and exact-scope ownership**, not
//! process control. What it does *not* do is claim readiness from
//! construction: `start` returning `Ok` proves nothing, and the supervisor
//! still requires a fully validated handshake through the fabric before this
//! adapter's provider is `Ready`.
//!
//! A process topology (ADR-0009 for NCM, bead `tdmem-0703`) implements the
//! same trait with a real spawn, a real wait, and a real kill; nothing in the
//! supervisor or in this owner changes when it does.
//!
//! # One owner per exact scope, bounded
//!
//! [`SupervisedScopeReadinessV1`] binds one supervisor to one exact
//! profile/project/repository/worktree/reference/session scope.
//! [`SupervisedProviderReadinessV1`] holds those owners keyed by the
//! canonical exact-scope digest under a finite ceiling: a host that opens
//! more scopes than the ceiling gets
//! [`SupervisedReadinessError::ScopeCapacityExceeded`], never an unbounded
//! map of supervisors.

use std::collections::BTreeMap;
use std::fmt;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use crate::state_capability::ProviderStateAuthorityV1;
use crate::supervisor::{
    DegradationKindV1, ProviderLifecycleAdapterV1, ProviderSupervisorV1, QuarantinePolicyV1,
    QuarantineRecordV1, QuarantineReleaseError, ReadinessEvidenceV1, ReproveOutcomeV1,
    RestartBudgetV1, ShutdownBudgetV1, SupervisedScopeV1, SupervisorConfigError,
    SupervisorOutcomeV1, validate_admitted_state_namespace_prefix,
};
use crate::{
    CancellationToken, FabricError, HandshakeRequest, HandshakeResponse, OperationControl,
    OwnedProviderId, ProjectMemoryProviderComposition, ProviderLimits, ProviderReadinessTargetV1,
};

/// A concrete [`ProviderLifecycleAdapterV1`] over one composed provider set.
///
/// The adapter owns no process. Its readiness path is the real fabric
/// handshake against the registered provider, so a supervisor driving it
/// observes real terminals, real descriptors, and real negotiated limits.
pub struct CompositionLifecycleAdapterV1 {
    composition: Arc<ProjectMemoryProviderComposition>,
    isolation: Arc<dyn BoundedProviderCallV1>,
}

impl CompositionLifecycleAdapterV1 {
    /// Binds one composed provider set to the bounded-execution boundary its
    /// handshakes run inside.
    ///
    /// The boundary is not optional. An in-process provider that never returns
    /// cannot be contained by an unwind boundary, a deadline value, or the
    /// provider's own cooperation; it can only be contained by something that
    /// keeps the calling thread free. Requiring it at construction is what
    /// stops a mount from silently getting the unbounded behaviour back.
    #[must_use]
    pub fn new(
        composition: Arc<ProjectMemoryProviderComposition>,
        isolation: Arc<dyn BoundedProviderCallV1>,
    ) -> Self {
        Self {
            composition,
            isolation,
        }
    }
}

/// One provider handshake, ready to run inside a bounded-execution boundary.
pub type ProviderHandshakeWorkV1 =
    Box<dyn FnOnce() -> Result<HandshakeResponse, CompositionLifecycleError> + Send + 'static>;

/// Why a bounded-execution boundary produced no provider answer.
#[derive(Clone, Debug, Eq, PartialEq, thiserror::Error)]
pub enum BoundedCallRefusalV1 {
    /// The provider did not answer inside the budget and the call was
    /// abandoned. The calling thread was released; the provider was not
    /// waited for.
    #[error("provider did not answer within {waited_millis}ms and the call was abandoned")]
    Abandoned {
        /// Budget the boundary actually waited.
        waited_millis: u64,
    },
    /// The caller cancelled while the call was in flight.
    #[error("bounded provider call was cancelled by the caller")]
    Cancelled,
    /// More calls are already abandoned to a non-returning provider than the
    /// boundary will hold, so no further call was started.
    #[error("provider has {abandoned} abandoned call(s) at the ceiling of {maximum}")]
    Exhausted {
        /// Calls already abandoned.
        abandoned: usize,
        /// Finite ceiling.
        maximum: usize,
    },
    /// The boundary itself could not run the call.
    #[error("bounded provider call could not be started: {0}")]
    Unavailable(String),
}

/// The host's bounded-execution boundary for one provider call.
///
/// # Why this is a port
///
/// Enforcing a deadline against a call that may never return needs an OS
/// capability — a worker the host can abandon, or a child process it can kill.
/// This crate is the host's *authority* layer and is source-contracted to name
/// no thread, process, filesystem, or network capability
/// (`product/architecture/memory-dependency-policy.json`), so it declares the
/// boundary and the composition root — or, for a process topology, the
/// supervised local-process adapter of ADR-0009 — supplies it.
///
/// An implementation must:
///
/// * return within `budget_millis` whether or not the provider does;
/// * abandon rather than join a call that outlives the budget, so the calling
///   thread is never blocked on a hung provider;
/// * bound the number of calls it holds abandoned, refusing
///   [`BoundedCallRefusalV1::Exhausted`] instead of accumulating one worker per
///   hung call forever;
/// * observe `cancellation` while it waits and refuse
///   [`BoundedCallRefusalV1::Cancelled`] when the caller withdraws.
pub trait BoundedProviderCallV1: Send + Sync + fmt::Debug {
    /// Runs one provider handshake under a hard bound.
    ///
    /// `Ok` carries whatever the provider's own call produced; `Err` means no
    /// answer exists and why.
    fn handshake_within(
        &self,
        budget_millis: u64,
        cancellation: &CancellationToken,
        work: ProviderHandshakeWorkV1,
    ) -> Result<Result<HandshakeResponse, CompositionLifecycleError>, BoundedCallRefusalV1>;
}

/// Failure of one [`CompositionLifecycleAdapterV1`] call.
#[derive(Clone, Debug, Eq, PartialEq, thiserror::Error)]
pub enum CompositionLifecycleError {
    /// Composition is disabled, so no provider instance exists to supervise.
    /// This is the typed unavailability a disabled configuration produces; it
    /// is never a fabricated readiness.
    #[error("provider composition is disabled, so no provider instance exists")]
    CompositionDisabled,
    /// The supervisor's own bound deadline for this call had already elapsed
    /// before the call could run.
    #[error("provider lifecycle deadline elapsed before the {operation} call could run")]
    DeadlineElapsed {
        /// Adapter call whose deadline had elapsed.
        operation: &'static str,
    },
    /// The bounded fabric refused the readiness handshake.
    #[error("memory fabric refused the readiness handshake: {0}")]
    Fabric(#[source] FabricError),
    /// The caller cancelled the operation before the handshake was started.
    #[error("readiness handshake was cancelled by the caller")]
    Cancelled,
    /// The host's bounded-execution boundary produced no provider answer. The
    /// host was never blocked on the provider.
    #[error("bounded provider handshake produced no answer: {0}")]
    Isolation(#[source] BoundedCallRefusalV1),
}

impl ProviderLifecycleAdapterV1 for CompositionLifecycleAdapterV1 {
    type Error = CompositionLifecycleError;

    fn start(&self, deadline_unix_micros: i64) -> Result<(), Self::Error> {
        // No spawn: the in-process instance is the composed provider set. The
        // only real precondition is that a provider set exists at all, and
        // this is emphatically not a readiness claim — the supervisor still
        // requires a validated handshake next.
        if self.composition.registry().is_none() {
            return Err(CompositionLifecycleError::CompositionDisabled);
        }
        if deadline_unix_micros <= 0 {
            return Err(CompositionLifecycleError::DeadlineElapsed { operation: "start" });
        }
        Ok(())
    }

    fn handshake(
        &self,
        request: &HandshakeRequest,
        deadline_unix_micros: i64,
    ) -> Result<HandshakeResponse, Self::Error> {
        if self.composition.registry().is_none() {
            return Err(CompositionLifecycleError::CompositionDisabled);
        }
        if deadline_unix_micros <= 0 {
            return Err(CompositionLifecycleError::DeadlineElapsed {
                operation: "handshake",
            });
        }
        // The request's own `OperationControl` carries the live budget and
        // cancellation the fabric enforces per operation, and the supervisor's
        // tighter handshake budget is already folded into it by the caller
        // (`SupervisedScopeReadinessV1::bounded_request`). That budget is what
        // this call is actually bounded by — enforced by the host's isolation
        // boundary rather than trusted to the provider, because an untrusted
        // provider that simply never returns would otherwise wedge the host.
        let budget_millis = request.control.remaining_millis();
        if budget_millis == 0 {
            return Err(CompositionLifecycleError::DeadlineElapsed {
                operation: "handshake",
            });
        }
        let cancellation = request.control.cancellation();
        if cancellation.is_cancelled() {
            return Err(CompositionLifecycleError::Cancelled);
        }

        let composition = Arc::clone(&self.composition);
        let isolated = request.clone();
        let work: ProviderHandshakeWorkV1 = Box::new(move || {
            composition.registry().map_or(
                Err(CompositionLifecycleError::CompositionDisabled),
                |registry| {
                    registry
                        .handshake(&isolated)
                        .map_err(CompositionLifecycleError::Fabric)
                },
            )
        });

        self.isolation
            .handshake_within(budget_millis, &cancellation, work)
            .map_err(CompositionLifecycleError::Isolation)?
    }

    fn request_stop(&self, _deadline_unix_micros: i64) -> Result<bool, Self::Error> {
        // An in-process incarnation has nothing that outlives this call, so
        // death is confirmed here rather than escalated to `kill`.
        Ok(true)
    }

    fn kill(&self, _deadline_unix_micros: i64) -> Result<(), Self::Error> {
        Ok(())
    }
}

/// The finite budgets one supervised readiness owner runs under.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SupervisedReadinessConfigV1 {
    /// Bounded restart ceiling and enforced backoff.
    pub restart_budget: RestartBudgetV1,
    /// Bounded graceful-stop and forced-kill budget.
    pub shutdown_budget: ShutdownBudgetV1,
    /// Bound handed to [`ProviderLifecycleAdapterV1::start`].
    pub start_budget_micros: i64,
    /// Bound handed to [`ProviderLifecycleAdapterV1::handshake`].
    pub handshake_budget_micros: i64,
    /// Finite ceiling on concurrently supervised exact scopes.
    pub max_supervised_scopes: usize,
}

impl SupervisedReadinessConfigV1 {
    /// Rejects a configuration that cannot bound anything.
    pub fn validate(&self) -> Result<(), SupervisorConfigError> {
        self.restart_budget.validate()?;
        self.shutdown_budget.validate()?;
        if self.start_budget_micros <= 0 {
            return Err(SupervisorConfigError::InvalidField {
                field: "start_budget_micros",
            });
        }
        if self.handshake_budget_micros <= 0 {
            return Err(SupervisorConfigError::InvalidField {
                field: "handshake_budget_micros",
            });
        }
        if self.max_supervised_scopes == 0 {
            return Err(SupervisorConfigError::InvalidField {
                field: "max_supervised_scopes",
            });
        }
        Ok(())
    }
}

/// Typed failure of one supervised readiness pass.
///
/// Every variant is a refusal to claim readiness. None of them is a silent
/// fallback, and none of them is fatal to the host: a caller that receives
/// one continues without provider readiness for that scope.
#[derive(Clone, Debug, Eq, PartialEq, thiserror::Error)]
pub enum SupervisedReadinessError {
    /// The configuration or the bound scope could not bound supervision.
    #[error("supervised readiness configuration is invalid: {0}")]
    Config(#[source] SupervisorConfigError),
    /// More exact scopes were supervised than the finite ceiling allows.
    #[error(
        "supervised readiness refuses scope {exact_scope_sha256}: {supervised} scope(s) already \
         supervised at the ceiling of {maximum}"
    )]
    ScopeCapacityExceeded {
        /// Digest of the refused exact scope.
        exact_scope_sha256: String,
        /// Scopes already supervised.
        supervised: usize,
        /// Finite ceiling.
        maximum: usize,
    },
    /// The supervisor for that exact scope reported typed degradation. The
    /// kind is the supervisor's own persisted classification.
    #[error(
        "supervised provider is unavailable for scope {exact_scope_sha256} ({kind}): {detail}; \
         next pass eligible in {retry_in_micros}us"
    )]
    Unavailable {
        /// Digest of the exact scope whose provider is unavailable.
        exact_scope_sha256: String,
        /// Typed degradation kind.
        kind: DegradationKindV1,
        /// Degradation detail captured at the pass.
        detail: String,
        /// Micros until the enforced backoff admits another pass, or
        /// [`i64::MAX`] when the restart budget is spent for this window.
        retry_in_micros: i64,
    },
    /// A supervisor owner was left poisoned by a panic in another thread. The
    /// host keeps running; this scope's readiness is refused until the owner
    /// is rebuilt.
    #[error("supervised readiness owner for scope {exact_scope_sha256} is poisoned")]
    OwnerPoisoned {
        /// Digest of the affected exact scope.
        exact_scope_sha256: String,
    },
    /// The caller's own deadline had already elapsed at the instant of the
    /// pass, so no adapter was contacted and no readiness was claimed.
    #[error(
        "supervised readiness for scope {exact_scope_sha256} refused: the caller's deadline \
         {deadline_utc_micros} had already elapsed"
    )]
    DeadlineElapsed {
        /// Digest of the refused exact scope.
        exact_scope_sha256: String,
        /// Absolute deadline the caller carried.
        deadline_utc_micros: i64,
    },
    /// The host-owned provider state root could not be opened, so no
    /// contained state authority exists to grant.
    #[error("supervised readiness could not open the provider state root: {0}")]
    StateRoot(String),
    /// An explicit quarantine release was refused. The provider stays
    /// quarantined.
    #[error(
        "supervised provider quarantine for scope {exact_scope_sha256} was not released: {reason}"
    )]
    QuarantineRelease {
        /// Digest of the affected exact scope.
        exact_scope_sha256: String,
        /// Why the release was refused.
        reason: QuarantineReleaseError,
    },
}

/// One exact scope's supervised readiness owner.
///
/// Holds exactly one [`ProviderSupervisorV1`] bound to exactly one exact
/// scope, behind a mutex so a shared host can drive it. There is no second
/// path to that supervisor: readiness for this scope is whatever the
/// supervisor last validated.
pub struct SupervisedScopeReadinessV1 {
    exact_scope_sha256: String,
    config: SupervisedReadinessConfigV1,
    supervisor: Mutex<ProviderSupervisorV1<CompositionLifecycleAdapterV1>>,
}

/// One quarantined exact scope, as an inspection surface reports it.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct QuarantinedScopeV1 {
    /// Canonical digest of the quarantined exact scope.
    pub exact_scope_sha256: String,
    /// The persisted quarantine evidence for that scope.
    pub record: QuarantineRecordV1,
}

impl SupervisedScopeReadinessV1 {
    /// Binds one supervisor to one exact scope over one composed provider
    /// set.
    pub fn new(
        composition: Arc<ProjectMemoryProviderComposition>,
        isolation: Arc<dyn BoundedProviderCallV1>,
        scope: SupervisedScopeV1,
        config: SupervisedReadinessConfigV1,
    ) -> Result<Self, SupervisedReadinessError> {
        Self::with_quarantine_policy(
            composition,
            isolation,
            scope,
            config,
            QuarantinePolicyV1::DEFAULT,
        )
    }

    /// Binds one supervisor to one exact scope under an explicit quarantine
    /// ceiling.
    pub fn with_quarantine_policy(
        composition: Arc<ProjectMemoryProviderComposition>,
        isolation: Arc<dyn BoundedProviderCallV1>,
        scope: SupervisedScopeV1,
        config: SupervisedReadinessConfigV1,
        quarantine_policy: QuarantinePolicyV1,
    ) -> Result<Self, SupervisedReadinessError> {
        config
            .validate()
            .map_err(SupervisedReadinessError::Config)?;
        let exact_scope_sha256 = scope.exact_scope_sha256().to_owned();
        let supervisor = ProviderSupervisorV1::new(
            CompositionLifecycleAdapterV1::new(composition, isolation),
            scope,
            config.restart_budget,
            config.shutdown_budget,
        )
        .map_err(SupervisedReadinessError::Config)?
        .with_quarantine_policy(quarantine_policy)
        .map_err(SupervisedReadinessError::Config)?;
        Ok(Self {
            exact_scope_sha256,
            config,
            supervisor: Mutex::new(supervisor),
        })
    }

    /// Binds one supervisor to one exact scope that is **already**
    /// quarantined, restoring the evidence a previous owner for the same
    /// scope earned before it was retired.
    pub fn with_restored_quarantine(
        composition: Arc<ProjectMemoryProviderComposition>,
        isolation: Arc<dyn BoundedProviderCallV1>,
        scope: SupervisedScopeV1,
        config: SupervisedReadinessConfigV1,
        quarantine_policy: QuarantinePolicyV1,
        record: QuarantineRecordV1,
    ) -> Result<Self, SupervisedReadinessError> {
        config
            .validate()
            .map_err(SupervisedReadinessError::Config)?;
        let exact_scope_sha256 = scope.exact_scope_sha256().to_owned();
        let supervisor = ProviderSupervisorV1::new(
            CompositionLifecycleAdapterV1::new(composition, isolation),
            scope,
            config.restart_budget,
            config.shutdown_budget,
        )
        .map_err(SupervisedReadinessError::Config)?
        .with_quarantine_policy(quarantine_policy)
        .map_err(SupervisedReadinessError::Config)?
        .with_restored_quarantine(record);
        Ok(Self {
            exact_scope_sha256,
            config,
            supervisor: Mutex::new(supervisor),
        })
    }

    /// Returns the canonical digest of the exact scope this owner supervises.
    #[must_use]
    pub fn exact_scope_sha256(&self) -> &str {
        &self.exact_scope_sha256
    }

    /// Drives one bounded supervised readiness pass and, only on a fully
    /// validated handshake, returns the readiness target.
    ///
    /// Deadlines are the tighter of the request's own live control deadline
    /// and this owner's finite start/handshake budgets, so the caller's
    /// deadline is propagated and never widened.
    pub fn ready_target(
        &self,
        request: &HandshakeRequest,
        now_unix_micros: i64,
    ) -> Result<ProviderReadinessTargetV1, SupervisedReadinessError> {
        self.ready_target_with_evidence(request, now_unix_micros)
            .map(|(target, _)| target)
    }

    /// Drives the same bounded readiness pass and also returns the validated
    /// readiness evidence it proved.
    ///
    /// The delivery address and the provider's own state identity come from
    /// **one** handshake, which is what lets a caller compare the provider's
    /// state schema and generation against its durable expectations without
    /// spending a second handshake — and without ever pairing an address from
    /// one incarnation with state evidence from another.
    pub fn ready_target_with_evidence(
        &self,
        request: &HandshakeRequest,
        now_unix_micros: i64,
    ) -> Result<(ProviderReadinessTargetV1, ReadinessEvidenceV1), SupervisedReadinessError> {
        let request_deadline = request.control.deadline_utc_micros();
        if request_deadline <= now_unix_micros {
            return Err(SupervisedReadinessError::DeadlineElapsed {
                exact_scope_sha256: self.exact_scope_sha256.clone(),
                deadline_utc_micros: request_deadline,
            });
        }
        let start_deadline = now_unix_micros
            .saturating_add(self.config.start_budget_micros)
            .min(request_deadline);
        let handshake_deadline = now_unix_micros
            .saturating_add(self.config.handshake_budget_micros)
            .min(request_deadline);
        // The supervisor's own handshake budget is folded into the live
        // operation control the adapter enforces, so the tighter of the two
        // bounds is the one the provider call actually runs under. Only the
        // budget is tightened: the absolute deadline stays the caller's, since
        // the supervisor's instants are the caller's clock domain and the
        // control's remaining budget is what is monotonic.
        let bounded = self.bounded_request(request, request_deadline);
        let request = &bounded;

        let mut supervisor = self
            .supervisor
            .lock()
            .unwrap_or_else(PoisonError::into_inner);

        // Steady state first: a readiness re-proof of an incarnation that is
        // already `Ready` is one handshake, and it must not spend the
        // crash-loop budget. Only a provider that is not currently ready goes
        // through the bounded restart path.
        if let ReproveOutcomeV1::Ready(evidence) =
            supervisor.reprove_readiness(request, handshake_deadline)
        {
            return Ok((
                ProviderReadinessTargetV1 {
                    provider_id: supervisor.scope().provider_id().clone(),
                    provider_instance_id: evidence.provider_instance_id().to_owned(),
                    registration_revision: supervisor.scope().registration_revision(),
                    ready_receipt_sha256: evidence.ready_receipt_sha256().to_owned(),
                },
                evidence,
            ));
        }

        let outcome = supervisor.start_or_restart(
            request,
            now_unix_micros,
            start_deadline,
            handshake_deadline,
        );
        match outcome {
            SupervisorOutcomeV1::Ready(evidence) => Ok((
                ProviderReadinessTargetV1 {
                    provider_id: supervisor.scope().provider_id().clone(),
                    provider_instance_id: evidence.provider_instance_id().to_owned(),
                    registration_revision: supervisor.scope().registration_revision(),
                    ready_receipt_sha256: evidence.ready_receipt_sha256().to_owned(),
                },
                evidence,
            )),
            SupervisorOutcomeV1::Unavailable(cause) => {
                // `next_restart_delay_micros` is what a caller schedules its
                // next pass from; carrying it on the typed error is what makes
                // the enforced backoff observable instead of advisory.
                let retry_in_micros = supervisor
                    .next_restart_delay_micros(now_unix_micros)
                    .unwrap_or(i64::MAX);
                Err(SupervisedReadinessError::Unavailable {
                    exact_scope_sha256: self.exact_scope_sha256.clone(),
                    kind: cause.kind(),
                    detail: cause.to_string(),
                    retry_in_micros,
                })
            }
        }
    }

    /// Returns `request` with its live operation budget tightened to this
    /// owner's finite handshake budget.
    ///
    /// This is the propagation the adapter's bounded execution enforces: a
    /// provider call may never run longer than the smaller of the caller's own
    /// remaining budget and the supervisor's handshake budget.
    fn bounded_request(
        &self,
        request: &HandshakeRequest,
        request_deadline: i64,
    ) -> HandshakeRequest {
        let budget_millis =
            u64::try_from(self.config.handshake_budget_micros.saturating_div(1_000))
                .unwrap_or(u64::MAX);
        let tightened = request.control.remaining_millis().min(budget_millis);
        let mut bounded = request.clone();
        bounded.control =
            OperationControl::new(request_deadline, tightened, request.control.cancellation());
        bounded
    }

    /// Reports a crash observed outside a handshake, invalidating readiness
    /// and persisting [`DegradationKindV1::Crashed`].
    pub fn report_crash(&self) -> Result<DegradationKindV1, SupervisedReadinessError> {
        let mut supervisor = self
            .supervisor
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        match supervisor.report_crash() {
            SupervisorOutcomeV1::Unavailable(cause) => Ok(cause.kind()),
            // `report_crash` cannot produce readiness; the arm exists because
            // the outcome type is shared and this crate refuses `unreachable!`.
            SupervisorOutcomeV1::Ready(_) => Err(SupervisedReadinessError::OwnerPoisoned {
                exact_scope_sha256: self.exact_scope_sha256.clone(),
            }),
        }
    }

    /// Runs one bounded shutdown pass for this scope's instance.
    pub fn shutdown(&self, now_unix_micros: i64) -> Result<bool, SupervisedReadinessError> {
        let mut supervisor = self
            .supervisor
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        match supervisor.shutdown(now_unix_micros) {
            Ok(report) => Ok(report.escalated_to_kill),
            Err(cause) => Err(SupervisedReadinessError::Unavailable {
                exact_scope_sha256: self.exact_scope_sha256.clone(),
                kind: cause.kind(),
                detail: cause.to_string(),
                retry_in_micros: i64::MAX,
            }),
        }
    }

    /// The persisted quarantine record for this scope, or `None` when the
    /// provider is not quarantined. Makes no adapter call.
    #[must_use]
    pub fn quarantine(&self) -> Option<QuarantineRecordV1> {
        let supervisor = self
            .supervisor
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        supervisor.quarantine().cloned()
    }

    /// Releases this scope's quarantine explicitly, after a bounded shutdown
    /// has confirmed the quarantined instance is dead.
    ///
    /// The shutdown is part of the release rather than a separate courtesy:
    /// a quarantined instance that is still alive must not be joined by a
    /// replacement, so a release that cannot confirm death is refused and the
    /// quarantine stands.
    pub fn release_quarantine(
        &self,
        now_unix_micros: i64,
    ) -> Result<QuarantineRecordV1, SupervisedReadinessError> {
        let mut supervisor = self
            .supervisor
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        if let Err(cause) = supervisor.shutdown(now_unix_micros) {
            return Err(SupervisedReadinessError::Unavailable {
                exact_scope_sha256: self.exact_scope_sha256.clone(),
                kind: cause.kind(),
                detail: cause.to_string(),
                retry_in_micros: i64::MAX,
            });
        }
        supervisor.release_quarantine().map_err(|reason| {
            SupervisedReadinessError::QuarantineRelease {
                exact_scope_sha256: self.exact_scope_sha256.clone(),
                reason,
            }
        })
    }

    /// The typed degradation currently persisted for this scope, or `None`
    /// when the provider is not degraded. Makes no adapter call.
    #[must_use]
    pub fn current_degradation(&self) -> Option<DegradationKindV1> {
        let supervisor = self
            .supervisor
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        supervisor.current_degradation().map(|record| record.kind())
    }
}

impl fmt::Debug for SupervisedScopeReadinessV1 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SupervisedScopeReadinessV1")
            .field("exact_scope_sha256", &self.exact_scope_sha256)
            .field("degradation", &self.current_degradation())
            .finish()
    }
}

/// The host's bounded set of per-exact-scope supervised readiness owners.
///
/// This is what a composition root mounts: one value, shared, from which
/// every exact scope's readiness is obtained. Owners are created lazily on
/// first readiness request for a scope and never exceed
/// [`SupervisedReadinessConfigV1::max_supervised_scopes`].
pub struct SupervisedProviderReadinessV1 {
    composition: Arc<ProjectMemoryProviderComposition>,
    isolation: Arc<dyn BoundedProviderCallV1>,
    /// Serializes a readiness proof with the provider operation that consumes
    /// it. The fabric intentionally retains only the latest ready receipt for a
    /// registration, so another scope's handshake must not rotate that receipt
    /// between proof and dispatch.
    dispatch_gate: Mutex<()>,
    config: SupervisedReadinessConfigV1,
    host_limits: ProviderLimits,
    provider_id: OwnedProviderId,
    registration_revision: u64,
    pinned_implementation_identity_sha256: Option<String>,
    pinned_state_schema_version: Option<String>,
    admitted_state_namespace_prefix: Option<String>,
    state_authority: Option<ProviderStateAuthorityV1>,
    quarantine_policy: QuarantinePolicyV1,
    owners: Mutex<OwnerRegistryV1>,
}

/// One supervised scope owner plus when it was last used, so the finite
/// ceiling can retire the coldest scope instead of refusing every new one
/// forever.
struct OwnerSlotV1 {
    owner: Arc<SupervisedScopeReadinessV1>,
    last_used_unix_micros: i64,
}

/// The mount's own state: the live per-scope owners, plus the quarantine
/// evidence of scopes whose owners were retired.
///
/// The ledger is what makes quarantine survive the finite owner ceiling. An
/// owner is a live object the ceiling may retire; a quarantine is a decision
/// about a *provider scope* that only an explicit release may undo. Keeping
/// the record here means a hostile provider cannot launder its quarantine by
/// churning scopes until its own owner is evicted.
struct OwnerRegistryV1 {
    live: BTreeMap<String, OwnerSlotV1>,
    retired_quarantines: BTreeMap<String, QuarantineRecordV1>,
}

/// One readiness proof held exclusively until its associated provider use
/// finishes.
///
/// The guard is registration-wide rather than scope-local because the fabric's
/// current ready receipt is registration-wide. Keeping it alive prevents an
/// admission or another scope from replacing that receipt after the handshake
/// but before the caller reaches the registry.
pub struct SupervisedReadinessDispatchV1<'a> {
    _dispatch: MutexGuard<'a, ()>,
    target: ProviderReadinessTargetV1,
    evidence: ReadinessEvidenceV1,
}

impl SupervisedReadinessDispatchV1<'_> {
    /// Readiness target produced by the guarded handshake.
    #[must_use]
    pub const fn target(&self) -> &ProviderReadinessTargetV1 {
        &self.target
    }

    /// State evidence produced by the same guarded handshake.
    #[must_use]
    pub const fn evidence(&self) -> &ReadinessEvidenceV1 {
        &self.evidence
    }
}

impl OwnerRegistryV1 {
    const fn new() -> Self {
        Self {
            live: BTreeMap::new(),
            retired_quarantines: BTreeMap::new(),
        }
    }
}

impl SupervisedProviderReadinessV1 {
    /// Mounts supervised readiness over one composed provider set for one
    /// selected provider identity and registration revision.
    pub fn new(
        composition: Arc<ProjectMemoryProviderComposition>,
        isolation: Arc<dyn BoundedProviderCallV1>,
        provider_id: OwnedProviderId,
        registration_revision: u64,
        host_limits: ProviderLimits,
        config: SupervisedReadinessConfigV1,
    ) -> Result<Self, SupervisedReadinessError> {
        config
            .validate()
            .map_err(SupervisedReadinessError::Config)?;
        if registration_revision == 0 {
            return Err(SupervisedReadinessError::Config(
                SupervisorConfigError::InvalidField {
                    field: "registration_revision",
                },
            ));
        }
        Ok(Self {
            composition,
            isolation,
            dispatch_gate: Mutex::new(()),
            config,
            host_limits,
            provider_id,
            registration_revision,
            pinned_implementation_identity_sha256: None,
            pinned_state_schema_version: None,
            admitted_state_namespace_prefix: None,
            state_authority: None,
            quarantine_policy: QuarantinePolicyV1::DEFAULT,
            owners: Mutex::new(OwnerRegistryV1::new()),
        })
    }

    /// Admits exactly one state-namespace prefix every supervised scope's
    /// provider may own.
    ///
    /// This is the host authority behind
    /// [`ReadinessDefectV1`](crate::ReadinessDefectV1)`::StateNamespaceNotAdmitted`:
    /// the provider reports the namespace its incarnation loaded, and a
    /// namespace outside the admitted prefix is refused fail-closed instead of
    /// becoming this provider's state. Owners already created keep the prefix
    /// they were built with, so this is set once at mount.
    pub fn with_admitted_state_namespace_prefix(
        mut self,
        prefix: &str,
    ) -> Result<Self, SupervisedReadinessError> {
        validate_admitted_state_namespace_prefix(prefix)
            .map_err(SupervisedReadinessError::Config)?;
        self.admitted_state_namespace_prefix = Some(prefix.to_owned());
        Ok(self)
    }

    /// Opens the host-owned root every supervised provider's state is
    /// contained by, and binds it as the state authority of every scope this
    /// mount supervises.
    ///
    /// Validating the reported namespace proves what a provider claims; this
    /// is what the host grants. With a root bound, a validated readiness also
    /// mints a
    /// [`ProviderStateCapabilityV1`](crate::state_capability::ProviderStateCapabilityV1)
    /// under it, and that capability is the only provider state path the host
    /// produces. Owners already created keep the authority they were built
    /// with, so this is set once at mount.
    pub fn with_state_root(
        mut self,
        root: impl Into<std::path::PathBuf>,
    ) -> Result<Self, SupervisedReadinessError> {
        let authority = ProviderStateAuthorityV1::new(root)
            .map_err(|source| SupervisedReadinessError::StateRoot(source.to_string()))?;
        self.state_authority = Some(authority);
        Ok(self)
    }

    /// The host-owned state authority this mount grants capabilities from, or
    /// `None` when the host bound no state root.
    #[must_use]
    pub const fn state_authority(&self) -> Option<&ProviderStateAuthorityV1> {
        self.state_authority.as_ref()
    }

    /// Pins the quarantine ceiling every supervised scope enforces, replacing
    /// [`QuarantinePolicyV1::DEFAULT`].
    pub fn with_quarantine_policy(
        mut self,
        policy: QuarantinePolicyV1,
    ) -> Result<Self, SupervisedReadinessError> {
        policy
            .validate()
            .map_err(SupervisedReadinessError::Config)?;
        self.quarantine_policy = policy;
        Ok(self)
    }

    /// Pins the immutable build and state-schema identity every supervised
    /// scope must observe at readiness.
    ///
    /// ADR-0009 requires the provider-reported build and state identity to be
    /// compared with the supervisor's own pinned values; a host that knows
    /// which executable and schema it admitted pins them here, and a
    /// mismatch becomes a fail-closed contract violation instead of a
    /// silently accepted foreign build. Owners already created keep the
    /// pinning they were built with, so this is set once at mount.
    #[must_use]
    pub fn with_pinned_identity(
        mut self,
        implementation_identity_sha256: Option<String>,
        state_schema_version: Option<String>,
    ) -> Self {
        self.pinned_implementation_identity_sha256 = implementation_identity_sha256;
        self.pinned_state_schema_version = state_schema_version;
        self
    }

    /// Returns the owner supervising `request`'s exact scope, creating it on
    /// first use inside the finite ceiling.
    ///
    /// A scope whose quarantine evidence is in the ledger comes back
    /// **quarantined**: retiring an owner is a capacity decision, never a
    /// forgiveness decision.
    pub fn owner_for(
        &self,
        request: &HandshakeRequest,
        now_unix_micros: i64,
    ) -> Result<Arc<SupervisedScopeReadinessV1>, SupervisedReadinessError> {
        let exact_scope_sha256 = request.exact_scope.exact_scope_sha256();
        let mut owners = self.owners.lock().unwrap_or_else(PoisonError::into_inner);
        if let Some(existing) = owners.live.get_mut(&exact_scope_sha256) {
            existing.last_used_unix_micros = now_unix_micros;
            return Ok(Arc::clone(&existing.owner));
        }
        if owners.live.len() >= self.config.max_supervised_scopes {
            self.retire_coldest_owner(&mut owners, now_unix_micros, &exact_scope_sha256)?;
        }
        let scope = SupervisedScopeV1::new(
            self.provider_id.clone(),
            self.registration_revision,
            request.exact_scope.clone(),
            self.host_limits,
        )
        .map_err(SupervisedReadinessError::Config)?
        .with_pinned_identity(
            self.pinned_implementation_identity_sha256.clone(),
            self.pinned_state_schema_version.clone(),
        );
        let scope = match &self.admitted_state_namespace_prefix {
            Some(prefix) => scope
                .with_admitted_state_namespace_prefix(prefix)
                .map_err(SupervisedReadinessError::Config)?,
            None => scope,
        };
        let scope = match &self.state_authority {
            Some(authority) => scope.with_state_authority(authority.clone()),
            None => scope,
        };
        // Quarantine evidence outlives the owner that earned it: a scope that
        // was retired while quarantined is rebuilt quarantined, so it makes no
        // adapter call and still needs an explicit release.
        let restored = owners.retired_quarantines.remove(&exact_scope_sha256);
        let owner = Arc::new(match restored {
            Some(record) => SupervisedScopeReadinessV1::with_restored_quarantine(
                Arc::clone(&self.composition),
                Arc::clone(&self.isolation),
                scope,
                self.config,
                self.quarantine_policy,
                record,
            )?,
            None => SupervisedScopeReadinessV1::with_quarantine_policy(
                Arc::clone(&self.composition),
                Arc::clone(&self.isolation),
                scope,
                self.config,
                self.quarantine_policy,
            )?,
        });
        owners.live.insert(
            exact_scope_sha256,
            OwnerSlotV1 {
                owner: Arc::clone(&owner),
                last_used_unix_micros: now_unix_micros,
            },
        );
        Ok(owner)
    }

    /// Retires one owner to make room, preserving any quarantine it holds.
    ///
    /// A non-quarantined owner is always preferred, because retiring a
    /// quarantined one costs a live supervisor that is currently refusing a
    /// hostile provider. When every owner is quarantined the coldest one is
    /// retired anyway and its record is moved to the ledger, which is bounded
    /// by the same finite scope ceiling; a full ledger refuses the new scope
    /// rather than dropping evidence.
    fn retire_coldest_owner(
        &self,
        owners: &mut OwnerRegistryV1,
        now_unix_micros: i64,
        requested_scope_sha256: &str,
    ) -> Result<(), SupervisedReadinessError> {
        let capacity_exceeded = || SupervisedReadinessError::ScopeCapacityExceeded {
            exact_scope_sha256: requested_scope_sha256.to_owned(),
            supervised: owners.live.len(),
            maximum: self.config.max_supervised_scopes,
        };
        let coldest = owners
            .live
            .iter()
            .filter(|(_, slot)| slot.owner.quarantine().is_none())
            .min_by_key(|(_, slot)| slot.last_used_unix_micros)
            .map(|(digest, _)| digest.clone())
            .or_else(|| {
                // Every owner is quarantined. Retiring one is admissible only
                // while the ledger can still hold its evidence.
                if owners.retired_quarantines.len() >= self.config.max_supervised_scopes {
                    return None;
                }
                owners
                    .live
                    .iter()
                    .min_by_key(|(_, slot)| slot.last_used_unix_micros)
                    .map(|(digest, _)| digest.clone())
            });
        let Some(digest) = coldest else {
            return Err(capacity_exceeded());
        };
        let Some(slot) = owners.live.get(&digest) else {
            return Err(capacity_exceeded());
        };
        // Confirm the retiring instance's death first: two owners for one
        // provider namespace is the thing supervision exists to prevent. A
        // retirement that cannot confirm death refuses the new scope.
        if slot.owner.shutdown(now_unix_micros).is_err() {
            return Err(capacity_exceeded());
        }
        if let Some(record) = slot.owner.quarantine() {
            owners.retired_quarantines.insert(digest.clone(), record);
        }
        owners.live.remove(&digest);
        Ok(())
    }

    /// Drives one bounded supervised readiness pass for `request`'s exact
    /// scope.
    pub fn ready_target(
        &self,
        request: &HandshakeRequest,
        now_unix_micros: i64,
    ) -> Result<ProviderReadinessTargetV1, SupervisedReadinessError> {
        self.ready_dispatch_with_evidence(request, now_unix_micros)
            .map(|dispatch| dispatch.target.clone())
    }

    /// Drives one bounded supervised readiness pass and also returns the
    /// validated readiness evidence, so an address and the provider state
    /// identity a caller compares against always come from one handshake.
    pub fn ready_target_with_evidence(
        &self,
        request: &HandshakeRequest,
        now_unix_micros: i64,
    ) -> Result<(ProviderReadinessTargetV1, ReadinessEvidenceV1), SupervisedReadinessError> {
        self.ready_dispatch_with_evidence(request, now_unix_micros)
            .map(|dispatch| (dispatch.target.clone(), dispatch.evidence.clone()))
    }

    /// Proves readiness and retains exclusive dispatch ownership until the
    /// returned guard is dropped.
    ///
    /// Callers that immediately contact the provider must keep this guard alive
    /// through that contact. Ordinary readiness-only callers can use
    /// [`Self::ready_target`] or [`Self::ready_target_with_evidence`], which
    /// release ownership as soon as their evidence is copied out.
    pub fn ready_dispatch_with_evidence(
        &self,
        request: &HandshakeRequest,
        now_unix_micros: i64,
    ) -> Result<SupervisedReadinessDispatchV1<'_>, SupervisedReadinessError> {
        let dispatch = self
            .dispatch_gate
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        let (target, evidence) = self
            .owner_for(request, now_unix_micros)?
            .ready_target_with_evidence(request, now_unix_micros)?;
        Ok(SupervisedReadinessDispatchV1 {
            _dispatch: dispatch,
            target,
            evidence,
        })
    }

    /// Every exact scope whose provider is currently quarantined, with the
    /// evidence that quarantined it.
    ///
    /// This is the operational-visibility surface for quarantine: a host that
    /// sees repeated typed unavailability can name which scopes stopped
    /// calling their provider entirely and why, without touching an adapter.
    #[must_use]
    pub fn quarantined_scopes(&self) -> Vec<QuarantinedScopeV1> {
        let owners = self.owners.lock().unwrap_or_else(PoisonError::into_inner);
        let live = owners.live.iter().filter_map(|(exact_scope_sha256, slot)| {
            slot.owner.quarantine().map(|record| QuarantinedScopeV1 {
                exact_scope_sha256: exact_scope_sha256.clone(),
                record,
            })
        });
        // Retired owners are gone; the quarantines they earned are not.
        let retired = owners
            .retired_quarantines
            .iter()
            .map(|(exact_scope_sha256, record)| QuarantinedScopeV1 {
                exact_scope_sha256: exact_scope_sha256.clone(),
                record: record.clone(),
            });
        live.chain(retired).collect()
    }

    /// Releases the quarantine of one supervised exact scope explicitly.
    pub fn release_quarantine(
        &self,
        exact_scope_sha256: &str,
        now_unix_micros: i64,
    ) -> Result<QuarantineRecordV1, SupervisedReadinessError> {
        let owner = {
            let mut owners = self.owners.lock().unwrap_or_else(PoisonError::into_inner);
            match owners.live.get(exact_scope_sha256) {
                Some(slot) => Some(Arc::clone(&slot.owner)),
                None => {
                    // A retired-but-quarantined scope releases from the
                    // ledger: its instance's death was already confirmed when
                    // the owner was retired, and the release is still
                    // explicit and auditable.
                    return match owners.retired_quarantines.remove(exact_scope_sha256) {
                        Some(record) => Ok(record),
                        None => Err(SupervisedReadinessError::QuarantineRelease {
                            exact_scope_sha256: exact_scope_sha256.to_owned(),
                            reason: QuarantineReleaseError::NotQuarantined,
                        }),
                    };
                }
            }
        };
        match owner {
            Some(owner) => owner.release_quarantine(now_unix_micros),
            None => Err(SupervisedReadinessError::QuarantineRelease {
                exact_scope_sha256: exact_scope_sha256.to_owned(),
                reason: QuarantineReleaseError::NotQuarantined,
            }),
        }
    }

    /// Exact scopes whose quarantine evidence outlived their retired owner.
    #[must_use]
    pub fn retired_quarantined_scopes(&self) -> usize {
        self.owners
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .retired_quarantines
            .len()
    }

    /// Number of exact scopes currently supervised.
    #[must_use]
    pub fn supervised_scopes(&self) -> usize {
        self.owners
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .live
            .len()
    }
}

impl fmt::Debug for SupervisedProviderReadinessV1 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SupervisedProviderReadinessV1")
            .field("provider_id", &self.provider_id.as_str())
            .field("registration_revision", &self.registration_revision)
            .field("supervised_scopes", &self.supervised_scopes())
            .finish()
    }
}

impl From<SupervisorConfigError> for SupervisedReadinessError {
    fn from(value: SupervisorConfigError) -> Self {
        Self::Config(value)
    }
}

/// Convenience so a host can treat a readiness refusal as a source error.
impl SupervisedReadinessError {
    /// Returns the typed degradation kind when the refusal came from the
    /// supervisor rather than from configuration or capacity.
    #[must_use]
    pub const fn degradation_kind(&self) -> Option<DegradationKindV1> {
        match self {
            Self::Unavailable { kind, .. } => Some(*kind),
            Self::Config(_)
            | Self::ScopeCapacityExceeded { .. }
            | Self::OwnerPoisoned { .. }
            | Self::DeadlineElapsed { .. }
            | Self::StateRoot(_)
            | Self::QuarantineRelease { .. } => None,
        }
    }
}

/// Asserts at compile time that a mounted owner is shareable across the
/// host's own threads, which is what lets one composition-root value serve
/// every scope.
const fn _assert_shareable<T: Send + Sync>() {}
const _: () = _assert_shareable::<SupervisedProviderReadinessV1>();
const _: () = _assert_shareable::<SupervisedScopeReadinessV1>();
