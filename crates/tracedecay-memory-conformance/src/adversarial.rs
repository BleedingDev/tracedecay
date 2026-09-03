//! A provider double that misbehaves on demand, for driving a host's real
//! mounted dispatch and recall paths adversarially (`tdmem-sz9`).
//!
//! Everything here is provider-neutral: the double implements the same
//! [`MemoryProvider`] contract a shipped adapter implements, so a host can
//! register it wherever it registers a real provider and the misbehaviour
//! travels through the *production* dispatch path rather than a test-only
//! seam. Nothing in this module knows what a candidate, an observation, or a
//! recall payload looks like; payload shaping is delegated to the host-side
//! [`AdversarialPayloadSourceV1`] the caller injects, so the crate that owns a
//! payload contract keeps owning it.
//!
//! Two properties make this a harness rather than a stub:
//!
//! * every misbehaviour is *selected*, per contact, from a script, so one
//!   provider identity can be compliant on its handshake and hostile on its
//!   call, or hostile once and compliant on the retry;
//! * every contact is *recorded* in an [`AdversarialLedgerV1`], including
//!   whether the cancellation token was already cancelled when the double
//!   chose to answer anyway. A test asserts the misbehaviour was really
//!   exhibited instead of inferring it from the host's answer, which is the
//!   difference between proving containment and proving nothing.
//!
//! The double never fabricates a terminal the canonical
//! [`TerminalRecord`] constructor refuses: the contract's own validator is the
//! floor for what a provider can even emit. What it does exhibit is the class
//! of violation that is only detectable by *cross-checking a reply against the
//! call it answers* — the wrong operation kind, a foreign operation id, a
//! payload on a failing terminal, a state generation that moved backwards, a
//! duplicate acknowledgement naming somebody else's mutation, a reply past the
//! effective response ceiling, a payload whose digest does not describe its
//! bytes — plus the failure modes that are not replies at all: a panic
//! mid-dispatch, a call that blocks past its own deadline, and a call that
//! never returns at all until the test holding its release latch lets it go.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex, MutexGuard};
use std::time::{Duration, Instant};

use sha2::{Digest, Sha256};
use tracedecay_memory_provider_api::contract::TerminalCode;
use tracedecay_memory_provider_api::{
    ApiError, CanonicalPayload, CommittedEffectEvidence, FallbackDirective, HandshakeRequest,
    HandshakeResponse, MemoryProvider, OwnedExactScope, OwnedVersionedId, ProviderCall,
    ProviderDescriptor, ProviderOperation, ProviderReply, TerminalRecord,
};

/// How often a blocking misbehaviour re-reads the cancellation token.
const CANCELLATION_POLL: Duration = Duration::from_millis(2);

/// Hard ceiling on how long a *timed* blocking misbehaviour may occupy the
/// calling thread, whatever the script asks for.
///
/// A timed block is a stand-in for a slow provider, and one that could be
/// scripted into an unbounded wait would hang the suite instead of proving
/// anything, so the ceiling is enforced here rather than trusted to each
/// caller. The genuinely non-returning provider is a different behaviour with
/// a different exit condition — [`MisbehaviourV1::NeverRepliesUntilReleased`]
/// returns only when the test releases it, never on a timer — so this ceiling
/// does not apply to it and cannot turn its containment test into a test of a
/// provider that eventually answered on its own.
const MAXIMUM_BLOCK: Duration = Duration::from_millis(5_000);

/// Operation id a foreign-operation reply is attributed to. It is deliberately
/// well-formed bounded canonical text: the violation must be *which* operation
/// the reply names, not a malformed identifier the constructor would refuse on
/// its own.
const FOREIGN_OPERATION_ID: &str = "adversarial.foreign-operation.v1";

/// Idempotency key a duplicate acknowledgement claims to have matched. It
/// names a mutation the host never delivered on this call.
const FOREIGN_IDEMPOTENCY_KEY: &str =
    "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";

/// A syntactically valid digest that describes nothing this provider produced.
const FOREIGN_DIGEST: &str = "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc";

/// Diagnostic id the double answers with when the *harness itself* could not
/// build the reply it was scripted to send. Seeing it in a test means the
/// double is broken, not that the host contained anything.
const HARNESS_DEFECT_DIAGNOSTIC_ID: &str = "adversarial.harness.defect.v1";

/// A test-held latch a non-returning provider call waits on.
///
/// This is what makes "never replies" a *deterministic* behaviour rather than
/// a long timer: a call that reaches the latch is inside the provider until
/// the test that owns the latch releases it, and nothing else can end it. The
/// host therefore has to contain a call that is still running when it answers
/// its own caller, which is the only shape in which "the host does not follow
/// its provider down" is provable.
///
/// Releasing is for suite cleanup: after a test has asserted containment it
/// releases the latch so every borrowed worker leaves the provider and the
/// host's own worker census can be checked back to its baseline. A latch that
/// is never released leaves the parked calls parked, which is precisely what
/// a real non-returning provider does.
#[derive(Clone, Default)]
pub struct ReleaseLatchV1 {
    inner: Arc<ReleaseLatchInner>,
}

#[derive(Default)]
struct ReleaseLatchInner {
    released: Mutex<bool>,
    changed: Condvar,
    /// Calls currently waiting on this latch. Observable so a test can wait
    /// for the provider to really be parked instead of sleeping on a guess.
    parked: AtomicU64,
    /// Calls that entered the latch at any point, so a released latch still
    /// says how many calls it held.
    admitted: AtomicU64,
}

impl ReleaseLatchV1 {
    /// A latch nothing has released yet.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Releases every call parked on the latch, and every later one.
    ///
    /// Idempotent: releasing an already released latch is a no-op, so a test
    /// may release in its own cleanup path without knowing whether it already
    /// did.
    pub fn release(&self) {
        let mut released = self.guard();
        *released = true;
        drop(released);
        self.inner.changed.notify_all();
    }

    /// Whether the latch has been released.
    #[must_use]
    pub fn is_released(&self) -> bool {
        *self.guard()
    }

    /// Calls parked on the latch right now.
    #[must_use]
    pub fn parked(&self) -> u64 {
        self.inner.parked.load(Ordering::Acquire)
    }

    /// Calls that have ever entered the latch.
    #[must_use]
    pub fn admitted(&self) -> u64 {
        self.inner.admitted.load(Ordering::Acquire)
    }

    /// Blocks the calling thread until the latch is released, and reports how
    /// long it was held there.
    ///
    /// There is no timeout by construction: a call that could time out here
    /// would be a slow provider, not a non-returning one.
    pub fn wait(&self) -> Duration {
        let started = Instant::now();
        self.inner.admitted.fetch_add(1, Ordering::AcqRel);
        self.inner.parked.fetch_add(1, Ordering::AcqRel);
        let mut released = self.guard();
        while !*released {
            released = self
                .inner
                .changed
                .wait(released)
                .unwrap_or_else(std::sync::PoisonError::into_inner);
        }
        drop(released);
        self.inner.parked.fetch_sub(1, Ordering::AcqRel);
        started.elapsed()
    }

    /// The released flag, recovered if a scripted panic poisoned the lock.
    fn guard(&self) -> MutexGuard<'_, bool> {
        self.inner
            .released
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

impl std::fmt::Debug for ReleaseLatchV1 {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ReleaseLatchV1")
            .field("released", &self.is_released())
            .field("parked", &self.parked())
            .field("admitted", &self.admitted())
            .finish()
    }
}

/// Two handles are equal when they are the same latch: a script is compared
/// by which latch it waits on, never by whether two latches happen to be in
/// the same state at the moment of comparison.
impl PartialEq for ReleaseLatchV1 {
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.inner, &other.inner)
    }
}

impl Eq for ReleaseLatchV1 {}

/// What the double does when the host contacts it for a readiness handshake.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum HandshakeMisbehaviourV1 {
    /// Answer a well-formed, self-consistent readiness response.
    Compliant,
    /// Answer readiness while the returned descriptor declares a capability
    /// the provider was never registered with — the provider lying about what
    /// it can do, after registration, when the host is least able to re-check.
    DeclaresUnregisteredCapability(String),
    /// Answer readiness for a checkout other than the one the host asked
    /// about, so a later call would be settled against a foreign scope.
    AcceptsForeignScope,
    /// Answer a successful readiness terminal while withholding the ready
    /// receipt every subsequent call must quote.
    WithholdsReadyReceipt,
    /// Refuse readiness with a typed non-success terminal.
    Refuses(TerminalCode),
}

/// What the double does when the host dispatches one operation to it.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum MisbehaviourV1 {
    /// Answer a well-formed reply bound to the call, with the payload the
    /// injected source supplies.
    Compliant,
    /// Occupy the calling thread for `block_millis` and then answer success
    /// anyway, reading neither the deadline nor the cancellation token. This
    /// is both "hangs past the deadline" and "ignores cancellation": which one
    /// it proves depends on what the host does while the thread is held.
    BlocksPastDeadline {
        /// Milliseconds to hold the calling thread, capped by the harness.
        block_millis: u64,
    },
    /// Block until the cancellation token fires, then answer `cancelled`; if
    /// the token never fires, give up after `ceiling_millis` and answer
    /// `deadline_exceeded` instead. This is the *well-behaved* blocking
    /// provider, and the two terminals are what let a test tell "the host's
    /// cancellation reached the provider" apart from "the provider gave up on
    /// its own".
    BlocksUntilCancelled {
        /// Milliseconds after which the double stops waiting.
        ceiling_millis: u64,
    },
    /// Enter the call and never leave it until the test releases the latch.
    ///
    /// This is the provider that genuinely does not return: no timer ends it,
    /// no cancellation ends it, and the host has to answer its own caller with
    /// the call still inside the provider or not at all. The latch is the
    /// test's, and releasing it is a cleanup step, not part of the behaviour.
    NeverRepliesUntilReleased(ReleaseLatchV1),
    /// Panic after receiving the call and before producing any reply.
    PanicsMidDispatch,
    /// Answer a terminal attributed to another operation kind.
    TerminalForAnotherOperation(ProviderOperation),
    /// Answer a failing terminal that nevertheless carries a result payload.
    PayloadOnFailureTerminal(TerminalCode),
    /// Answer a terminal that names an operation the host never dispatched.
    ReplyForForeignOperation,
    /// Answer a terminal bound to a scope the host never asked about.
    TerminalForForeignScope,
    /// Report a provider-local state generation older than the one the call
    /// declared, as a restored or rolled-back provider would.
    StateGenerationBackwards,
    /// Answer success with a payload padded past the effective response
    /// ceiling the host admitted at handshake.
    OversizedReply {
        /// Padding bytes appended to the compliant payload.
        padding_bytes: usize,
    },
    /// Acknowledge the call as a duplicate of a mutation the host never
    /// delivered on it — a settlement claimed for somebody else's work.
    DuplicateAcknowledgingAnotherMutation,
    /// Answer success with a payload whose declared digest does not describe
    /// its bytes: committed evidence that cannot be verified.
    CorruptedPayloadDigest,
}

impl MisbehaviourV1 {
    /// Stable snake_case label, so a ledger row and a test name can agree.
    #[must_use]
    pub const fn label(&self) -> &'static str {
        match self {
            Self::Compliant => "compliant",
            Self::BlocksPastDeadline { .. } => "blocks_past_deadline",
            Self::BlocksUntilCancelled { .. } => "blocks_until_cancelled",
            Self::NeverRepliesUntilReleased(_) => "never_replies_until_released",
            Self::PanicsMidDispatch => "panics_mid_dispatch",
            Self::TerminalForAnotherOperation(_) => "terminal_for_another_operation",
            Self::PayloadOnFailureTerminal(_) => "payload_on_failure_terminal",
            Self::ReplyForForeignOperation => "reply_for_foreign_operation",
            Self::TerminalForForeignScope => "terminal_for_foreign_scope",
            Self::StateGenerationBackwards => "state_generation_backwards",
            Self::OversizedReply { .. } => "oversized_reply",
            Self::DuplicateAcknowledgingAnotherMutation => {
                "duplicate_acknowledging_another_mutation"
            }
            Self::CorruptedPayloadDigest => "corrupted_payload_digest",
        }
    }
}

/// The misbehaviours one double exhibits, in dispatch order.
///
/// `steps` are consumed one per contact; once they run out every further
/// contact gets `tail`. A script is therefore explicit about how a provider
/// behaves on the retry, which is where crash-loop and quarantine behaviour is
/// actually decided.
#[derive(Clone, Debug)]
pub struct AdversarialScriptV1<T> {
    steps: Vec<T>,
    tail: T,
}

impl<T: Clone> AdversarialScriptV1<T> {
    /// A script that exhibits `tail` on every contact.
    #[must_use]
    pub const fn always(tail: T) -> Self {
        Self {
            steps: Vec::new(),
            tail,
        }
    }

    /// A script that exhibits `steps` in order and then `tail` forever.
    #[must_use]
    pub fn then(steps: Vec<T>, tail: T) -> Self {
        Self { steps, tail }
    }

    /// The behaviour for contact number `contact` (zero-based).
    fn at(&self, contact: usize) -> T {
        self.steps.get(contact).cloned().unwrap_or_else(|| {
            let tail = &self.tail;
            tail.clone()
        })
    }
}

/// One recorded contact with the double.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ExhibitedV1 {
    /// Operation the host dispatched (`Handshake` for a readiness contact).
    pub operation: ProviderOperation,
    /// Label of the misbehaviour the double selected for this contact.
    pub misbehaviour: &'static str,
    /// Operation id the host dispatched under.
    pub operation_id: String,
    /// Whether the caller's cancellation token was already cancelled at the
    /// moment the double produced its reply. `true` next to a compliant-looking
    /// success is the proof that the provider ignored cancellation.
    pub cancelled_when_answered: bool,
    /// Wall time the double held the calling thread, in milliseconds.
    pub held_millis: u64,
}

/// What the double actually did, as opposed to what it was asked to do.
#[derive(Debug, Default)]
struct LedgerInner {
    contacts: Vec<ExhibitedV1>,
}

/// Observable record of every contact one double received.
///
/// Cloning shares the record, so a host can own the provider while the test
/// keeps reading what it did.
#[derive(Clone, Debug, Default)]
pub struct AdversarialLedgerV1 {
    inner: Arc<Mutex<LedgerInner>>,
}

impl AdversarialLedgerV1 {
    /// An empty ledger.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Reads the record, recovering it if a scripted panic poisoned the lock.
    ///
    /// A poisoned lock is expected here — panicking mid-dispatch is one of the
    /// behaviours under test — and losing the whole record to it would destroy
    /// exactly the evidence the panic test needs.
    fn guard(&self) -> MutexGuard<'_, LedgerInner> {
        self.inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    fn record(&self, contact: ExhibitedV1) {
        self.guard().contacts.push(contact);
    }

    /// Every contact, in the order it arrived.
    #[must_use]
    pub fn contacts(&self) -> Vec<ExhibitedV1> {
        self.guard().contacts.clone()
    }

    /// Contacts for one operation kind.
    #[must_use]
    pub fn contacts_for(&self, operation: ProviderOperation) -> Vec<ExhibitedV1> {
        self.guard()
            .contacts
            .iter()
            .filter(|contact| contact.operation == operation)
            .cloned()
            .collect()
    }

    /// Number of contacts for one operation kind.
    #[must_use]
    pub fn count_for(&self, operation: ProviderOperation) -> usize {
        self.contacts_for(operation).len()
    }

    /// Whether the double ever answered a contact while the caller's
    /// cancellation token was already cancelled.
    #[must_use]
    pub fn answered_after_cancellation(&self) -> bool {
        self.guard()
            .contacts
            .iter()
            .any(|contact| contact.cancelled_when_answered)
    }
}

/// Host-side payload shaping for the double.
///
/// The double is provider-neutral and therefore cannot know what a compliant
/// reply payload for a given operation looks like. The crate that owns the
/// payload contract implements this and injects it, which is also what lets a
/// test forge *candidate-level* misbehaviour — replayed, flooded, forged-scope
/// or malformed candidates — without teaching this module a recall schema.
pub trait AdversarialPayloadSourceV1: Send + Sync + 'static {
    /// The payload a compliant reply to `call` carries, or `None` for an
    /// operation whose success terminal carries no payload.
    ///
    /// An `Err` is a defect in the injected source, not a provider
    /// misbehaviour: the double surfaces it as an `internal_failure` terminal
    /// carrying the supplied diagnostic id rather than silently answering
    /// success with no payload.
    fn payload_for(&self, call: &ProviderCall) -> Result<Option<CanonicalPayload>, String>;
}

/// A payload source that answers every operation with no payload.
#[derive(Clone, Copy, Debug, Default)]
pub struct NoPayloadSourceV1;

impl AdversarialPayloadSourceV1 for NoPayloadSourceV1 {
    fn payload_for(&self, _call: &ProviderCall) -> Result<Option<CanonicalPayload>, String> {
        Ok(None)
    }
}

/// Immutable identity the double answers under.
pub struct AdversarialProviderInputsV1 {
    /// Descriptor the host registers this double with. Its `provider_id` is
    /// the identity every terminal is bound to.
    pub descriptor: ProviderDescriptor,
    /// Runtime instance identity reported at handshake.
    pub provider_instance_id: String,
    /// Provider-local state namespace reported at handshake.
    pub state_namespace: String,
    /// Ready receipt digest reported at handshake.
    pub ready_receipt_sha256: String,
    /// Handshake behaviour, per readiness contact.
    pub handshake_script: AdversarialScriptV1<HandshakeMisbehaviourV1>,
    /// Call behaviour, per dispatched operation.
    pub invoke_script: AdversarialScriptV1<MisbehaviourV1>,
    /// Host-side payload shaping.
    pub payloads: Arc<dyn AdversarialPayloadSourceV1>,
}

/// Decrements the in-flight counter however the call leaves the provider:
/// a normal return, an error, or the scripted panic unwinding through it.
///
/// This is what makes "no provider work outlived the host's call" an
/// assertion rather than a hope: a host that walks away from a blocked
/// provider leaves the counter above zero, and the test sees it.
struct InFlightGuard(Arc<AtomicU64>);

impl Drop for InFlightGuard {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::AcqRel);
    }
}

/// A provider that misbehaves on demand, on the real provider contract.
pub struct AdversarialProviderV1 {
    descriptor: ProviderDescriptor,
    provider_instance_id: String,
    state_namespace: String,
    ready_receipt_sha256: String,
    handshake_script: AdversarialScriptV1<HandshakeMisbehaviourV1>,
    invoke_script: AdversarialScriptV1<MisbehaviourV1>,
    payloads: Arc<dyn AdversarialPayloadSourceV1>,
    handshakes: AtomicU64,
    invocations: AtomicU64,
    in_flight: Arc<AtomicU64>,
    ledger: AdversarialLedgerV1,
}

impl std::fmt::Debug for AdversarialProviderV1 {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("AdversarialProviderV1")
            .field("provider_id", &self.descriptor.provider_id.as_str())
            .field("handshakes", &self.handshakes.load(Ordering::Acquire))
            .field("invocations", &self.invocations.load(Ordering::Acquire))
            .field("in_flight", &self.in_flight.load(Ordering::Acquire))
            .finish_non_exhaustive()
    }
}

impl AdversarialProviderV1 {
    /// Builds a double from an explicit identity and script pair.
    #[must_use]
    pub fn new(inputs: AdversarialProviderInputsV1) -> Self {
        Self {
            descriptor: inputs.descriptor,
            provider_instance_id: inputs.provider_instance_id,
            state_namespace: inputs.state_namespace,
            ready_receipt_sha256: inputs.ready_receipt_sha256,
            handshake_script: inputs.handshake_script,
            invoke_script: inputs.invoke_script,
            payloads: inputs.payloads,
            handshakes: AtomicU64::new(0),
            invocations: AtomicU64::new(0),
            in_flight: Arc::new(AtomicU64::new(0)),
            ledger: AdversarialLedgerV1::new(),
        }
    }

    /// Calls that entered the provider and have not left it.
    ///
    /// A host that answered its caller while this is above zero abandoned a
    /// worker to a provider that is still running.
    #[must_use]
    pub fn in_flight(&self) -> u64 {
        self.in_flight.load(Ordering::Acquire)
    }

    /// The shared record of everything this double was asked to do.
    #[must_use]
    pub fn ledger(&self) -> AdversarialLedgerV1 {
        self.ledger.clone()
    }

    /// Readiness contacts received so far.
    #[must_use]
    pub fn handshake_count(&self) -> u64 {
        self.handshakes.load(Ordering::Acquire)
    }

    /// Operation dispatches received so far.
    #[must_use]
    pub fn invocation_count(&self) -> u64 {
        self.invocations.load(Ordering::Acquire)
    }

    /// Blocks the calling thread, optionally watching the cancellation token,
    /// and reports how long it actually held it.
    fn hold(call: &ProviderCall, limit: Duration, watch_cancellation: bool) -> (Duration, bool) {
        let started = Instant::now();
        let limit = limit.min(MAXIMUM_BLOCK);
        let cancellation = call.control.cancellation();
        loop {
            let elapsed = started.elapsed();
            if elapsed >= limit {
                return (elapsed, false);
            }
            if watch_cancellation && cancellation.is_cancelled() {
                return (elapsed, true);
            }
            std::thread::sleep(CANCELLATION_POLL.min(limit.saturating_sub(elapsed)));
        }
    }

    /// A terminal bound to this call, with the supplied code and effect.
    fn terminal(
        &self,
        call: &ProviderCall,
        operation: ProviderOperation,
        terminal_code: TerminalCode,
        effect: CommittedEffectEvidence,
        operation_id: &str,
        exact_scope_sha256: String,
    ) -> Result<TerminalRecord, ApiError> {
        TerminalRecord::new(
            operation,
            self.descriptor.provider_id.clone(),
            terminal_code,
            effect,
            FallbackDirective::forbidden(),
            operation_id,
            exact_scope_sha256,
            (!matches!(
                terminal_code,
                TerminalCode::Success | TerminalCode::SuccessZeroResults | TerminalCode::Partial
            ))
            .then(|| format!("{}.diagnostic.v1", call.operation.as_wire())),
        )
    }

    /// The reply the double falls back to when it cannot even build the reply
    /// it was scripted to send. This is never silent: the diagnostic names the
    /// harness itself, so a test that sees it knows the *double* failed rather
    /// than the host.
    fn harness_failure(&self, call: &ProviderCall) -> ProviderReply {
        ProviderReply {
            terminal: TerminalRecord::internal_failure_before_dispatch_for_call(
                call,
                HARNESS_DEFECT_DIAGNOSTIC_ID,
            ),
            payload: None,
            warnings: vec!["adversarial harness could not build its scripted reply".to_owned()],
            extensions: Vec::new(),
            state_generation: call.expected_state_generation,
        }
    }

    fn compliant_reply(&self, call: &ProviderCall) -> Result<ProviderReply, ApiError> {
        let payload = match self.payloads.payload_for(call) {
            Ok(payload) => payload,
            Err(detail) => {
                let terminal = self.terminal(
                    call,
                    call.operation,
                    TerminalCode::InternalFailure,
                    CommittedEffectEvidence::none(Some(call.expected_state_generation)),
                    &call.operation_id,
                    call.exact_scope.exact_scope_sha256(),
                )?;
                return Ok(ProviderReply {
                    terminal,
                    payload: None,
                    warnings: vec![detail],
                    extensions: Vec::new(),
                    state_generation: call.expected_state_generation,
                });
            }
        };
        let effect = self.success_effect(call)?;
        let state_generation = settled_generation(&effect, call);
        let terminal = self.terminal(
            call,
            call.operation,
            TerminalCode::Success,
            effect,
            &call.operation_id,
            call.exact_scope.exact_scope_sha256(),
        )?;
        Ok(ProviderReply {
            terminal,
            payload,
            warnings: Vec::new(),
            extensions: Vec::new(),
            state_generation,
        })
    }

    /// The effect a compliant success carries: a mutating operation commits
    /// one item and advances the generation, a read-only operation commits
    /// nothing. Both shapes are what the contract's own validator requires,
    /// so a compliant reply really is compliant.
    fn success_effect(&self, call: &ProviderCall) -> Result<CommittedEffectEvidence, ApiError> {
        if call.operation.mutates_provider_state() {
            CommittedEffectEvidence::committed(
                call.expected_state_generation,
                call.expected_state_generation.saturating_add(1),
                vec![call.operation_id.clone()],
                digest_of(call.operation_id.as_bytes()),
                digest_of(&call.payload.bytes),
            )
        } else {
            Ok(CommittedEffectEvidence::none(Some(
                call.expected_state_generation,
            )))
        }
    }

    fn misbehaving_reply(
        &self,
        call: &ProviderCall,
        misbehaviour: &MisbehaviourV1,
        observed_cancellation: bool,
    ) -> Result<ProviderReply, ApiError> {
        let scope_sha256 = call.exact_scope.exact_scope_sha256();
        match misbehaviour {
            MisbehaviourV1::Compliant
            | MisbehaviourV1::BlocksPastDeadline { .. }
            | MisbehaviourV1::PanicsMidDispatch => self.compliant_reply(call),
            MisbehaviourV1::BlocksUntilCancelled { .. }
            | MisbehaviourV1::NeverRepliesUntilReleased(_) => {
                // The terminal states what actually happened: `cancelled` only
                // when the token really fired, `deadline_exceeded` when the
                // double gave up on its own ceiling. A double that always
                // claimed cancellation would let a host that never cancels
                // anything pass a cancellation test.
                //
                // A released never-replying call reports the same pair for the
                // same reason: by the time the test lets it go the host has
                // long since answered somebody else, so the only honest thing
                // it can say is whether the token had fired by then.
                let terminal = self.terminal(
                    call,
                    call.operation,
                    if observed_cancellation {
                        TerminalCode::Cancelled
                    } else {
                        TerminalCode::DeadlineExceeded
                    },
                    CommittedEffectEvidence::none(Some(call.expected_state_generation)),
                    &call.operation_id,
                    scope_sha256,
                )?;
                Ok(ProviderReply {
                    terminal,
                    payload: None,
                    warnings: Vec::new(),
                    extensions: Vec::new(),
                    state_generation: call.expected_state_generation,
                })
            }
            MisbehaviourV1::TerminalForAnotherOperation(operation) => {
                let terminal = self.terminal(
                    call,
                    *operation,
                    TerminalCode::Success,
                    CommittedEffectEvidence::none(Some(call.expected_state_generation)),
                    &call.operation_id,
                    scope_sha256,
                )?;
                Ok(ProviderReply {
                    terminal,
                    payload: None,
                    warnings: Vec::new(),
                    extensions: Vec::new(),
                    state_generation: call.expected_state_generation,
                })
            }
            MisbehaviourV1::PayloadOnFailureTerminal(terminal_code) => {
                let terminal = self.terminal(
                    call,
                    call.operation,
                    *terminal_code,
                    CommittedEffectEvidence::none(Some(call.expected_state_generation)),
                    &call.operation_id,
                    scope_sha256,
                )?;
                Ok(ProviderReply {
                    terminal,
                    payload: self.payloads.payload_for(call).ok().flatten(),
                    warnings: Vec::new(),
                    extensions: Vec::new(),
                    state_generation: call.expected_state_generation,
                })
            }
            MisbehaviourV1::ReplyForForeignOperation => {
                let effect = self.success_effect(call)?;
                let state_generation = settled_generation(&effect, call);
                let terminal = self.terminal(
                    call,
                    call.operation,
                    TerminalCode::Success,
                    effect,
                    FOREIGN_OPERATION_ID,
                    scope_sha256,
                )?;
                Ok(ProviderReply {
                    terminal,
                    payload: self.payloads.payload_for(call).ok().flatten(),
                    warnings: Vec::new(),
                    extensions: Vec::new(),
                    state_generation,
                })
            }
            MisbehaviourV1::TerminalForForeignScope => {
                let effect = self.success_effect(call)?;
                let state_generation = settled_generation(&effect, call);
                let terminal = self.terminal(
                    call,
                    call.operation,
                    TerminalCode::Success,
                    effect,
                    &call.operation_id,
                    foreign_scope(&call.exact_scope)?.exact_scope_sha256(),
                )?;
                Ok(ProviderReply {
                    terminal,
                    payload: self.payloads.payload_for(call).ok().flatten(),
                    warnings: Vec::new(),
                    extensions: Vec::new(),
                    state_generation,
                })
            }
            MisbehaviourV1::StateGenerationBackwards => {
                let regressed = call.expected_state_generation.saturating_sub(1);
                let terminal = self.terminal(
                    call,
                    call.operation,
                    TerminalCode::Success,
                    CommittedEffectEvidence::none(Some(regressed)),
                    &call.operation_id,
                    scope_sha256,
                )?;
                Ok(ProviderReply {
                    terminal,
                    payload: None,
                    warnings: Vec::new(),
                    extensions: Vec::new(),
                    state_generation: regressed,
                })
            }
            MisbehaviourV1::OversizedReply { padding_bytes } => {
                let mut reply = self.compliant_reply(call)?;
                let mut bytes = reply
                    .payload
                    .as_ref()
                    .map_or_else(|| b"{}".to_vec(), |payload| payload.bytes.clone());
                bytes.extend(std::iter::repeat_n(b' ', *padding_bytes));
                let contract_id = reply.payload.as_ref().map_or_else(
                    || call.payload.contract_id.clone(),
                    |p| p.contract_id.clone(),
                );
                reply.payload = Some(CanonicalPayload::new(
                    contract_id,
                    bytes.clone(),
                    digest_of(&bytes),
                )?);
                Ok(reply)
            }
            MisbehaviourV1::DuplicateAcknowledgingAnotherMutation => {
                let effect = CommittedEffectEvidence::duplicate(
                    call.expected_state_generation,
                    FOREIGN_IDEMPOTENCY_KEY,
                    FOREIGN_OPERATION_ID,
                    FOREIGN_DIGEST,
                )?;
                let terminal = self.terminal(
                    call,
                    call.operation,
                    TerminalCode::Success,
                    effect,
                    &call.operation_id,
                    scope_sha256,
                )?;
                Ok(ProviderReply {
                    terminal,
                    payload: None,
                    warnings: Vec::new(),
                    extensions: Vec::new(),
                    state_generation: call.expected_state_generation,
                })
            }
            MisbehaviourV1::CorruptedPayloadDigest => {
                let mut reply = self.compliant_reply(call)?;
                let contract_id = reply.payload.as_ref().map_or_else(
                    || call.payload.contract_id.clone(),
                    |p| p.contract_id.clone(),
                );
                let bytes = reply
                    .payload
                    .as_ref()
                    .map_or_else(|| b"{}".to_vec(), |payload| payload.bytes.clone());
                // Built field-by-field on purpose: the validating constructor
                // would refuse this, and a provider that lies about its own
                // digest is exactly the case the host must catch itself.
                reply.payload = Some(CanonicalPayload {
                    contract_id,
                    bytes,
                    sha256: FOREIGN_DIGEST.to_owned(),
                });
                Ok(reply)
            }
        }
    }
}

/// The generation a reply must report so that it agrees with its own effect
/// evidence. A compliant reply is compliant on every field, including this
/// one; making it disagree here would turn every mutating-operation test into
/// an accidental state-generation test.
fn settled_generation(effect: &CommittedEffectEvidence, call: &ProviderCall) -> u64 {
    effect
        .state_generation_after()
        .unwrap_or(call.expected_state_generation)
}

/// A scope that differs from `scope` in exactly one identity field, so a
/// terminal bound to it is provably about another checkout.
fn foreign_scope(scope: &OwnedExactScope) -> Result<OwnedExactScope, ApiError> {
    OwnedExactScope::new(
        &scope.profile_id,
        &scope.project_id,
        &scope.repository_identity,
        format!("{}.foreign", scope.worktree_identity),
        &scope.branch_identity,
        &scope.agent_session_id,
        &scope.resolved_scope_digest,
    )
}

/// Lowercase hex SHA-256 of `bytes`.
fn digest_of(bytes: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    hex_lower(&hasher.finalize())
}

fn hex_lower(bytes: &[u8]) -> String {
    let mut encoded = String::with_capacity(bytes.len().saturating_mul(2));
    for byte in bytes {
        encoded.push(char::from_digit(u32::from(byte >> 4), 16).unwrap_or('0'));
        encoded.push(char::from_digit(u32::from(byte & 0x0f), 16).unwrap_or('0'));
    }
    encoded
}

impl MemoryProvider for AdversarialProviderV1 {
    fn descriptor(&self) -> ProviderDescriptor {
        self.descriptor.clone()
    }

    fn handshake(&self, request: &HandshakeRequest) -> HandshakeResponse {
        let contact = self.handshakes.fetch_add(1, Ordering::AcqRel);
        let misbehaviour = self
            .handshake_script
            .at(usize::try_from(contact).unwrap_or(usize::MAX));
        let started = Instant::now();
        let response = self.handshake_response(request, &misbehaviour);
        self.ledger.record(ExhibitedV1 {
            operation: ProviderOperation::Handshake,
            misbehaviour: handshake_label(&misbehaviour),
            operation_id: request.request_id.clone(),
            cancelled_when_answered: request.control.cancellation().is_cancelled(),
            held_millis: u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX),
        });
        response
    }

    fn invoke(&self, call: &ProviderCall) -> ProviderReply {
        self.in_flight.fetch_add(1, Ordering::AcqRel);
        let _in_flight = InFlightGuard(Arc::clone(&self.in_flight));
        let contact = self.invocations.fetch_add(1, Ordering::AcqRel);
        let misbehaviour = self
            .invoke_script
            .at(usize::try_from(contact).unwrap_or(usize::MAX));
        let (held, observed_cancellation) = match &misbehaviour {
            MisbehaviourV1::BlocksPastDeadline { block_millis } => {
                Self::hold(call, Duration::from_millis(*block_millis), false)
            }
            MisbehaviourV1::BlocksUntilCancelled { ceiling_millis } => {
                Self::hold(call, Duration::from_millis(*ceiling_millis), true)
            }
            // No ceiling and no cancellation check: this call leaves the
            // provider when the test says so and at no other moment.
            MisbehaviourV1::NeverRepliesUntilReleased(latch) => {
                let held = latch.wait();
                (held, call.control.cancellation().is_cancelled())
            }
            _ => (Duration::ZERO, false),
        };
        if matches!(misbehaviour, MisbehaviourV1::PanicsMidDispatch) {
            // Recorded before unwinding so the ledger still shows the contact
            // that killed the call.
            self.ledger.record(ExhibitedV1 {
                operation: call.operation,
                misbehaviour: misbehaviour.label(),
                operation_id: call.operation_id.clone(),
                cancelled_when_answered: call.control.cancellation().is_cancelled(),
                held_millis: 0,
            });
            return panic_mid_dispatch(&call.operation_id);
        }
        let reply = self
            .misbehaving_reply(call, &misbehaviour, observed_cancellation)
            .unwrap_or_else(|_| self.harness_failure(call));
        self.ledger.record(ExhibitedV1 {
            operation: call.operation,
            misbehaviour: misbehaviour.label(),
            operation_id: call.operation_id.clone(),
            cancelled_when_answered: call.control.cancellation().is_cancelled(),
            held_millis: u64::try_from(held.as_millis()).unwrap_or(u64::MAX),
        });
        reply
    }
}

/// The scripted crash. Isolated in its own function so the `panic` lint is
/// suppressed for exactly one expression instead of the whole module.
#[allow(clippy::panic)]
fn panic_mid_dispatch(operation_id: &str) -> ProviderReply {
    panic!("adversarial provider crashed mid-dispatch on {operation_id}");
}

const fn handshake_label(misbehaviour: &HandshakeMisbehaviourV1) -> &'static str {
    match misbehaviour {
        HandshakeMisbehaviourV1::Compliant => "compliant",
        HandshakeMisbehaviourV1::DeclaresUnregisteredCapability(_) => {
            "declares_unregistered_capability"
        }
        HandshakeMisbehaviourV1::AcceptsForeignScope => "accepts_foreign_scope",
        HandshakeMisbehaviourV1::WithholdsReadyReceipt => "withholds_ready_receipt",
        HandshakeMisbehaviourV1::Refuses(_) => "refuses_readiness",
    }
}

impl AdversarialProviderV1 {
    fn handshake_response(
        &self,
        request: &HandshakeRequest,
        misbehaviour: &HandshakeMisbehaviourV1,
    ) -> HandshakeResponse {
        let terminal_code = match misbehaviour {
            HandshakeMisbehaviourV1::Refuses(code) => *code,
            _ => TerminalCode::Success,
        };
        let mut descriptor = self.descriptor.clone();
        if let HandshakeMisbehaviourV1::DeclaresUnregisteredCapability(capability) = misbehaviour
            && let Ok(capability) = OwnedVersionedId::new(capability)
        {
            descriptor.capabilities.insert(capability);
        }
        let accepted_scope = match misbehaviour {
            HandshakeMisbehaviourV1::AcceptsForeignScope => {
                foreign_scope(&request.exact_scope).unwrap_or_else(|_| request.exact_scope.clone())
            }
            _ => request.exact_scope.clone(),
        };
        let terminal = TerminalRecord::new(
            ProviderOperation::Handshake,
            self.descriptor.provider_id.clone(),
            terminal_code,
            CommittedEffectEvidence::none(Some(self.descriptor.state_generation)),
            FallbackDirective::forbidden(),
            request.request_id.clone(),
            request.exact_scope.exact_scope_sha256(),
            (terminal_code != TerminalCode::Success)
                .then(|| "adversarial.handshake.refused.v1".to_owned()),
        );
        let Ok(terminal) = terminal else {
            return HandshakeResponse {
                terminal: TerminalRecord::failure_before_dispatch(
                    ProviderOperation::Handshake,
                    self.descriptor.provider_id.clone(),
                    TerminalCode::InternalFailure,
                    &request.request_id,
                    request.exact_scope.exact_scope_sha256(),
                    Some(self.descriptor.state_generation),
                    HARNESS_DEFECT_DIAGNOSTIC_ID,
                ),
                descriptor: None,
                provider_instance_id: None,
                state_namespace: None,
                accepted_scope: None,
                effective_limits: None,
                ready_receipt_sha256: None,
                warnings: Vec::new(),
            };
        };
        let successful = terminal_code == TerminalCode::Success;
        HandshakeResponse {
            terminal,
            descriptor: successful.then(|| descriptor.clone()),
            provider_instance_id: successful.then(|| self.provider_instance_id.clone()),
            state_namespace: successful.then(|| self.state_namespace.clone()),
            accepted_scope: successful.then_some(accepted_scope),
            effective_limits: successful.then(|| request.host_limits.minimum(descriptor.limits)),
            ready_receipt_sha256: match misbehaviour {
                HandshakeMisbehaviourV1::WithholdsReadyReceipt => None,
                _ => successful.then(|| self.ready_receipt_sha256.clone()),
            },
            warnings: Vec::new(),
        }
    }
}
