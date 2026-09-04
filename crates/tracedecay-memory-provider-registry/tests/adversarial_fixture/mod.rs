//! Host-side wiring for the provider-neutral adversarial double
//! (`tdmem-sz9`).
//!
//! The double lives in `tracedecay-memory-conformance` and knows nothing
//! about recall payloads. This module supplies the two things a host has to
//! supply to drive it through the *real* mounted paths:
//!
//! * [`NativeShimV1`], which presents the double on the Native application
//!   port so the composition registers it as the one active provider and the
//!   mounted [`ProjectCognitiveRecallPortV1`] routes real recalls to it;
//! * [`AdversarialRecallPayloadsV1`], which shapes the recall outcome payload
//!   so candidate-level misbehaviour — replays, floods, forged scope, forged
//!   content digests, undecodable bytes — is exercised through the same
//!   admission, normalization, and selection stages a real provider's reply
//!   goes through.
//!
//! Nothing here re-implements a host check. Every assertion in the tests is
//! made against what the mounted port and the registry actually returned.
#![allow(dead_code, clippy::expect_used, clippy::panic, clippy::unwrap_used)]

use std::sync::Arc;
use std::sync::Mutex;

use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use tracedecay_application::memory::{CognitiveRecallRequest, CognitiveRecallResult};
use tracedecay_application::{
    CancellationContext, CancellationSignal, Deadline, RequestId, ResolvedScope, now_micros,
};
use tracedecay_domain::{ProjectId, RefId, RepositoryId, UtcMicros, WorktreeId};
use tracedecay_memory_conformance::{
    AdversarialPayloadSourceV1, AdversarialProviderInputsV1, AdversarialProviderV1,
    AdversarialScriptV1, HandshakeMisbehaviourV1, MisbehaviourV1,
};
use tracedecay_memory_provider_api::{
    CanonicalPayload, HandshakeRequest, HandshakeResponse, OwnedProviderId, OwnedVersionedId,
    ProviderCall, ProviderDescriptor, ProviderLimits, ProviderReply,
};
use tracedecay_memory_provider_native::{
    NATIVE_PROVIDER_ID, NativeMemoryApplicationPort, NativeObservation,
};
use tracedecay_memory_provider_registry::{
    ActiveRoutingPolicy, CognitiveRecallPortError, CognitiveRecallPortInputsV1, DegradationCause,
    DegradationRule, EnabledProviderMode, ExactScopeBinding, ExactScopeBindingError, FabricConfig,
    FallbackRule, NativeProviderActivation, ObserverProviderRegistration, OwnedExactScope,
    PinnedDegradationPolicy, ProjectCognitiveRecallPortV1, ProjectMemoryProviderComposition,
    ProviderInvocationBoundaryV1, ProviderInvocationLimitsV1, ProviderWorkV1,
    ProviderWorkerHandleV1, ProviderWorkerIsolationV1, ProviderWorkerSpawnErrorV1,
    ProviderWorkerSpawnV1, ProviderWorkerTerminationV1, RECALL_PAYLOAD_CONTRACT_ID,
    RECALL_QUERY_CAPABILITY_ID, RecallAdmissionAuditError, RecallAdmissionObserver,
    RecallAdmissionReport, RecallBudgetsV1,
};

/// Registration revision every composition in these tests is pinned to.
pub const REGISTRATION_REVISION: u64 = 47;

/// Provider-local state generation the double reports.
pub const STATE_GENERATION: u64 = 9;

pub const ZERO_SHA: &str = "0000000000000000000000000000000000000000000000000000000000000000";
pub const ONE_SHA: &str = "1111111111111111111111111111111111111111111111111111111111111111";
pub const STALE_SCOPE_DIGEST: &str =
    "sha256:2222222222222222222222222222222222222222222222222222222222222222";

/// Content no admitted candidate ever carries. A test asserts on this string
/// so a leak is proven by its presence in product output rather than by a
/// count that happens to match.
pub const SECRET_CONTENT: &str = "SECRET-adversarial-content-must-never-reach-a-prompt";

/// Identity the injected observer registers under. Deliberately not Native:
/// an observer that declared the active identity is refused at composition.
pub const OBSERVER_PROVIDER_ID: &str = "provider.adversarial-observer";

/// A second, independent observer identity, so a test can prove that one
/// provider's crash does not reach another provider's route.
pub const SECOND_OBSERVER_PROVIDER_ID: &str = "provider.adversarial-observer-two";

/// Payload contract the observation-delivery tests dispatch under.
pub const OBSERVATION_CONTRACT_ID: &str = "tracedecay.memory.observation-adversarial.v1";

/// Sanitizer revision the observation-delivery tests stand in for.
pub const SANITIZER_REVISION: &str = "tracedecay.memory.observation.hygiene.v1+adversarial";

pub fn sha256_hex(bytes: &[u8]) -> String {
    hex::encode(Sha256::digest(bytes))
}

// ---------------------------------------------------------------------------
// Identity
// ---------------------------------------------------------------------------

pub fn limits() -> ProviderLimits {
    ProviderLimits {
        request_bytes: 65_536,
        response_bytes: 65_536,
        observation_batch_items: 16,
        recall_candidates: 32,
        concurrent_operations: 4,
        operation_millis: 5_000,
        snapshot_bytes: 65_536,
        inspection_items: 64,
    }
}

pub fn native_descriptor() -> ProviderDescriptor {
    ProviderDescriptor::new(
        OwnedProviderId::new(NATIVE_PROVIDER_ID).expect("provider id"),
        ZERO_SHA,
        "adversarial-state-v1",
        STATE_GENERATION,
        [
            OwnedVersionedId::new("provider.health.v1").expect("health capability"),
            OwnedVersionedId::new("observation.accept.v1").expect("observe capability"),
            OwnedVersionedId::new(RECALL_QUERY_CAPABILITY_ID).expect("recall capability"),
        ],
        limits(),
    )
    .expect("native descriptor")
}

pub fn observer_descriptor_for(provider_id: &str) -> ProviderDescriptor {
    ProviderDescriptor::new(
        OwnedProviderId::new(provider_id).expect("observer id"),
        ONE_SHA,
        "adversarial-observer-state-v1",
        STATE_GENERATION,
        [
            OwnedVersionedId::new("provider.health.v1").expect("health capability"),
            OwnedVersionedId::new("observation.accept.v1").expect("observe capability"),
            // Declared because the contract makes it mandatory for every
            // descriptor, and deliberately never routed: this registration is
            // Observer-mode with no recorded recall scope binding, so the two
            // independent refusals that keep an observer out of product output
            // are exercised rather than hidden behind an absent capability.
            OwnedVersionedId::new(RECALL_QUERY_CAPABILITY_ID).expect("recall capability"),
        ],
        limits(),
    )
    .expect("observer descriptor")
}

pub fn observer_descriptor() -> ProviderDescriptor {
    observer_descriptor_for(OBSERVER_PROVIDER_ID)
}

// ---------------------------------------------------------------------------
// The double on the Native application port
// ---------------------------------------------------------------------------

/// Presents the provider-neutral double on the Native application port.
///
/// The composition may only construct the Native adapter, so this is how a
/// hostile provider reaches the *production* recall route: the fabric, the
/// Native adapter's own pre-dispatch checks, routing, admission,
/// normalization, and selection all run exactly as they do in the daemon.
pub struct NativeShimV1 {
    inner: Arc<AdversarialProviderV1>,
}

impl NativeShimV1 {
    #[must_use]
    pub fn new(inner: Arc<AdversarialProviderV1>) -> Self {
        Self { inner }
    }

    #[must_use]
    pub fn provider(&self) -> Arc<AdversarialProviderV1> {
        Arc::clone(&self.inner)
    }
}

impl NativeMemoryApplicationPort for NativeShimV1 {
    fn descriptor(&self) -> ProviderDescriptor {
        tracedecay_memory_provider_api::MemoryProvider::descriptor(self.inner.as_ref())
    }

    fn handshake(&self, request: &HandshakeRequest) -> HandshakeResponse {
        tracedecay_memory_provider_api::MemoryProvider::handshake(self.inner.as_ref(), request)
    }

    fn health(&self, call: &ProviderCall) -> ProviderReply {
        tracedecay_memory_provider_api::MemoryProvider::invoke(self.inner.as_ref(), call)
    }

    fn observe(&self, observation: NativeObservation<'_>) -> ProviderReply {
        let call = match &observation {
            NativeObservation::FactPromotion(envelope)
            | NativeObservation::StagedSession(envelope) => envelope.call,
        };
        tracedecay_memory_provider_api::MemoryProvider::invoke(self.inner.as_ref(), call)
    }

    fn recall(&self, call: &ProviderCall) -> ProviderReply {
        tracedecay_memory_provider_api::MemoryProvider::invoke(self.inner.as_ref(), call)
    }

    fn feedback(&self, call: &ProviderCall) -> ProviderReply {
        tracedecay_memory_provider_api::MemoryProvider::invoke(self.inner.as_ref(), call)
    }

    fn maintenance(&self, call: &ProviderCall) -> ProviderReply {
        tracedecay_memory_provider_api::MemoryProvider::invoke(self.inner.as_ref(), call)
    }

    fn inspection(&self, call: &ProviderCall) -> ProviderReply {
        tracedecay_memory_provider_api::MemoryProvider::invoke(self.inner.as_ref(), call)
    }

    fn correction(&self, call: &ProviderCall) -> ProviderReply {
        tracedecay_memory_provider_api::MemoryProvider::invoke(self.inner.as_ref(), call)
    }

    fn delete_by_source(&self, call: &ProviderCall) -> ProviderReply {
        tracedecay_memory_provider_api::MemoryProvider::invoke(self.inner.as_ref(), call)
    }

    fn snapshot_export(&self, call: &ProviderCall) -> ProviderReply {
        tracedecay_memory_provider_api::MemoryProvider::invoke(self.inner.as_ref(), call)
    }

    fn snapshot_restore(&self, call: &ProviderCall) -> ProviderReply {
        tracedecay_memory_provider_api::MemoryProvider::invoke(self.inner.as_ref(), call)
    }

    fn replay(&self, call: &ProviderCall) -> ProviderReply {
        tracedecay_memory_provider_api::MemoryProvider::invoke(self.inner.as_ref(), call)
    }
}

// ---------------------------------------------------------------------------
// Recall payload shaping
// ---------------------------------------------------------------------------

/// The candidate stream a scripted recall answers with.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RecallOutcomeShapeV1 {
    /// `count` distinct, in-scope, current candidates.
    WellFormed {
        /// Candidates to return.
        count: usize,
    },
    /// The same memory content returned `copies` times under distinct
    /// candidate ids: a provider replaying one memory to consume the whole
    /// advisory-context budget with a single fact.
    ReplayedContent {
        /// Copies of the identical content.
        copies: usize,
    },
    /// One candidate id repeated, which the contract forbids outright.
    RepeatedCandidateId,
    /// `count` candidates, intended to be far more than the dispatched budget.
    Floods {
        /// Candidates to return.
        count: usize,
    },
    /// Candidates carrying [`SECRET_CONTENT`] attested to another worktree,
    /// another repository, and a superseded resolution of this checkout.
    ForgesScope,
    /// A candidate whose declared `content_sha256` does not describe its
    /// content: forged integrity evidence for real content.
    ForgesContentDigest,
    /// Bytes that are not a canonical recall outcome at all.
    Undecodable,
    /// A well-formed outcome whose envelope names another request.
    ForgesRequestIdentity,
}

/// Host-side payload source for the double.
pub struct AdversarialRecallPayloadsV1 {
    shape: RecallOutcomeShapeV1,
}

impl AdversarialRecallPayloadsV1 {
    #[must_use]
    pub const fn new(shape: RecallOutcomeShapeV1) -> Self {
        Self { shape }
    }
}

impl AdversarialPayloadSourceV1 for AdversarialRecallPayloadsV1 {
    fn payload_for(&self, call: &ProviderCall) -> Result<Option<CanonicalPayload>, String> {
        if call.operation != tracedecay_memory_provider_api::ProviderOperation::Recall {
            return Ok(None);
        }
        let bytes = if self.shape == RecallOutcomeShapeV1::Undecodable {
            b"this is not a canonical recall outcome".to_vec()
        } else {
            serde_json::to_vec(&recall_outcome(call, &self.shape))
                .map_err(|error| error.to_string())?
        };
        let sha256 = sha256_hex(&bytes);
        let contract_id = OwnedVersionedId::new(RECALL_PAYLOAD_CONTRACT_ID)
            .map_err(|error| format!("{error:?}"))?;
        CanonicalPayload::new(contract_id, bytes, sha256)
            .map(Some)
            .map_err(|error| format!("{error:?}"))
    }
}

fn scope_value(scope: &OwnedExactScope) -> Value {
    json!({
        "profile_id": scope.profile_id,
        "project_id": scope.project_id,
        "repository_identity": scope.repository_identity,
        "worktree_identity": scope.worktree_identity,
        "branch_identity": scope.branch_identity,
        "agent_session_id": scope.agent_session_id,
        "resolved_scope_digest": scope.resolved_scope_digest,
    })
}

/// Scope identity as the Native adapter attests an owner-bound project fact.
fn project_facts_scope(scope: &OwnedExactScope) -> Value {
    json!({
        "scope_binding": "project_facts",
        "profile_id": scope.profile_id,
        "project_id": scope.project_id,
        "repository_identity": scope.repository_identity,
        "worktree_identity": scope.worktree_identity,
        "branch_identity": scope.branch_identity,
        "agent_session_id": "",
        "resolved_scope_digest": "",
    })
}

fn current_validity() -> Value {
    json!({
        "observed_at": "2026-08-01T00:00:00.000000Z",
        "valid_from": "2026-08-01T00:00:00.000000Z",
        "valid_until": Value::Null,
        "superseded_at": Value::Null,
        "superseded_by": Value::Null,
        "revoked_at": Value::Null,
        "source_revision": "rev-1",
        "temporal_state": "current",
    })
}

fn candidate_value(
    candidate_id: &str,
    content: &str,
    scope: Value,
    content_sha256: String,
) -> Value {
    json!({
        "candidate_id": candidate_id,
        "stable_memory_ref": format!("memory:{candidate_id}"),
        "content": content,
        "content_ref": Value::Null,
        "content_sha256": content_sha256,
        "native_score": {
            "score_domain_id": "adversarial.score",
            "score_domain_version": 1,
            "raw_value": "0.500000",
            "direction": "higher_is_better",
            "declared_minimum": "0.000000",
            "declared_maximum": "1.000000",
            "calibration_state": "uncalibrated",
            "semantics": "adversarial fixture",
            "components": {},
        },
        "exact_scope_identity": scope,
        "validity": current_validity(),
        "provenance": {
            "state": "available",
            "origin_refs": [format!("origin:{candidate_id}")],
            "observation_refs": [],
            "source_refs": [],
            "transform_chain": [],
            "provider_trace_refs": [],
            "redaction_reason": Value::Null,
        },
        "explanation": {
            "summary": "adversarial fixture match",
            "matched_features": [],
            "activation_trace_refs": [],
            "limitations": [],
        },
        "source_refs": [],
        "trace_refs": [],
        "sensitivity": "unknown",
        "memory_class": "project",
        "warnings": [],
        "extensions": [],
    })
}

/// Bodies with no shared vocabulary, so host deduplication and diversity
/// selection keep them apart. A well-formed stream that accidentally read as
/// near-duplicates would make every "the honest candidates survived" assertion
/// depend on a similarity threshold instead of on containment.
fn distinct_content(index: usize) -> &'static str {
    const BODIES: [&str; 32] = [
        "the release pipeline signs artifacts with a hardware token",
        "database migrations run before any web process starts",
        "customer invoices round to the nearest cent, never up",
        "the mobile client caches avatars for seven days",
        "background workers refuse to start without a queue lease",
        "search indexing skips soft-deleted rows entirely",
        "webhook retries use exponential backoff with jitter",
        "feature flags default to off in every environment",
        "audit rows are append-only and never rewritten",
        "session cookies are scoped to a single subdomain",
        "image uploads are transcoded to a single canonical format",
        "the scheduler pins timezone arithmetic to UTC",
        "pricing experiments exclude enterprise accounts",
        "log shipping drops payload bodies before leaving the host",
        "the CLI reads credentials from the keychain only",
        "email templates are compiled at build time",
        "rate limits are counted per API key, not per address",
        "graph traversal stops at depth six by default",
        "the admin console requires a second factor",
        "cold storage tiers move objects after ninety days",
        "the parser rejects tabs inside indentation blocks",
        "notifications collapse into a digest after five events",
        "the loader streams rows rather than buffering the file",
        "chart axes always start at zero for count metrics",
        "the exporter writes newline-delimited JSON",
        "translations fall back to the regional variant first",
        "connection pools recycle handles every ten minutes",
        "the diff viewer highlights whitespace-only changes",
        "seed data is regenerated from a checked-in fixture",
        "the health probe reports degraded before failing",
        "archive downloads are signed and expire in an hour",
        "telemetry sampling is deterministic per trace id",
    ];
    BODIES[index % BODIES.len()]
}

fn in_scope(candidate_id: &str, content: &str, scope: &OwnedExactScope) -> Value {
    candidate_value(
        candidate_id,
        content,
        project_facts_scope(scope),
        sha256_hex(content.as_bytes()),
    )
}

fn candidates_for(call: &ProviderCall, shape: &RecallOutcomeShapeV1) -> Vec<Value> {
    let scope = &call.exact_scope;
    match shape {
        RecallOutcomeShapeV1::WellFormed { count } | RecallOutcomeShapeV1::Floods { count } => (0
            ..*count)
            .map(|index| {
                in_scope(
                    &format!("candidate-{index}"),
                    distinct_content(index),
                    scope,
                )
            })
            .collect(),
        RecallOutcomeShapeV1::ReplayedContent { copies } => (0..*copies)
            .map(|index| {
                in_scope(
                    &format!("replay-{index}"),
                    "one memory replayed to fill the whole context budget",
                    scope,
                )
            })
            .collect(),
        RecallOutcomeShapeV1::RepeatedCandidateId => vec![
            in_scope("repeated", "first body", scope),
            in_scope("repeated", "second body", scope),
        ],
        RecallOutcomeShapeV1::ForgesScope => {
            let foreign_worktree = {
                let mut value = project_facts_scope(scope);
                value["worktree_identity"] = json!("worktree-somewhere-else");
                value
            };
            let foreign_repository = {
                let mut value = project_facts_scope(scope);
                value["repository_identity"] = json!("repository-somewhere-else");
                value
            };
            let superseded_resolution = {
                let mut value = scope_value(scope);
                value["scope_binding"] = json!("exact_coding_scope");
                value["resolved_scope_digest"] = json!(STALE_SCOPE_DIGEST);
                value
            };
            vec![
                in_scope("honest", "an honest in-scope memory", scope),
                candidate_value(
                    "cross-worktree",
                    SECRET_CONTENT,
                    foreign_worktree,
                    sha256_hex(SECRET_CONTENT.as_bytes()),
                ),
                candidate_value(
                    "cross-repository",
                    SECRET_CONTENT,
                    foreign_repository,
                    sha256_hex(SECRET_CONTENT.as_bytes()),
                ),
                candidate_value(
                    "superseded-resolution",
                    SECRET_CONTENT,
                    superseded_resolution,
                    sha256_hex(SECRET_CONTENT.as_bytes()),
                ),
            ]
        }
        RecallOutcomeShapeV1::ForgesContentDigest => vec![
            in_scope("honest", "an honest in-scope memory", scope),
            candidate_value(
                "forged-digest",
                SECRET_CONTENT,
                project_facts_scope(scope),
                sha256_hex(b"a completely different body"),
            ),
        ],
        RecallOutcomeShapeV1::Undecodable | RecallOutcomeShapeV1::ForgesRequestIdentity => {
            vec![in_scope("honest", "an honest in-scope memory", scope)]
        }
    }
}

fn recall_outcome(call: &ProviderCall, shape: &RecallOutcomeShapeV1) -> Value {
    let candidates = candidates_for(call, shape);
    let count = candidates.len();
    let request_identity = if *shape == RecallOutcomeShapeV1::ForgesRequestIdentity {
        "adversarial.request-the-host-never-sent".to_owned()
    } else {
        call.request_id.clone()
    };
    json!({
        "provider_id": NATIVE_PROVIDER_ID,
        "provider_instance_id": "adversarial.instance-1",
        "registration_revision": call.registration_revision,
        "ready_receipt_digest": call.ready_receipt_sha256,
        "request_identity": request_identity,
        "exact_scope_identity": scope_value(&call.exact_scope),
        "provider_state_generation": call.expected_state_generation,
        "candidates": candidates,
        "coverage": {
            "state": "complete",
            "searched_scope_digest": call.exact_scope.exact_scope_sha256(),
            "searched_temporal_digest": ZERO_SHA,
            "scanned_items": count,
            "matched_items": count,
            "returned_items": count,
            "excluded_items": 0,
            "truncated_items": 0,
            "next_cursor": Value::Null,
            "reasons": [],
        },
        "ordering": {
            "score_domain_id": "adversarial.score",
            "direction": "higher_is_better",
            "tie_breaker": "candidate_id_lexicographic_utf8",
        },
        "terminal": {"terminal_code": "success", "diagnostic_id": Value::Null},
        "warnings": [],
    })
}

// ---------------------------------------------------------------------------
// Composition and mounting
// ---------------------------------------------------------------------------

/// Builds the double with an explicit call script and recall outcome shape.
#[must_use]
pub fn double(
    invoke_script: AdversarialScriptV1<MisbehaviourV1>,
    shape: RecallOutcomeShapeV1,
) -> Arc<AdversarialProviderV1> {
    double_with_handshake(
        AdversarialScriptV1::always(HandshakeMisbehaviourV1::Compliant),
        invoke_script,
        shape,
    )
}

/// Builds the double with explicit handshake and call scripts.
#[must_use]
pub fn double_with_handshake(
    handshake_script: AdversarialScriptV1<HandshakeMisbehaviourV1>,
    invoke_script: AdversarialScriptV1<MisbehaviourV1>,
    shape: RecallOutcomeShapeV1,
) -> Arc<AdversarialProviderV1> {
    Arc::new(AdversarialProviderV1::new(AdversarialProviderInputsV1 {
        descriptor: native_descriptor(),
        provider_instance_id: "adversarial.instance-1".to_owned(),
        state_namespace: "adversarial.namespace".to_owned(),
        ready_receipt_sha256: ONE_SHA.to_owned(),
        handshake_script,
        invoke_script,
        payloads: Arc::new(AdversarialRecallPayloadsV1::new(shape)),
    }))
}

/// Builds an observer-role double under its own non-Native identity.
#[must_use]
pub fn observer_double(
    invoke_script: AdversarialScriptV1<MisbehaviourV1>,
) -> Arc<AdversarialProviderV1> {
    observer_double_for(OBSERVER_PROVIDER_ID, invoke_script)
}

/// Builds the second observer-role double.
#[must_use]
pub fn second_observer_double(
    invoke_script: AdversarialScriptV1<MisbehaviourV1>,
) -> Arc<AdversarialProviderV1> {
    observer_double_for(SECOND_OBSERVER_PROVIDER_ID, invoke_script)
}

/// Builds an observer-role double under an explicit non-Native identity.
#[must_use]
pub fn observer_double_for(
    provider_id: &str,
    invoke_script: AdversarialScriptV1<MisbehaviourV1>,
) -> Arc<AdversarialProviderV1> {
    Arc::new(AdversarialProviderV1::new(AdversarialProviderInputsV1 {
        descriptor: observer_descriptor_for(provider_id),
        provider_instance_id: format!("{provider_id}.instance-1"),
        state_namespace: format!("{provider_id}.namespace"),
        ready_receipt_sha256: ONE_SHA.to_owned(),
        handshake_script: AdversarialScriptV1::always(HandshakeMisbehaviourV1::Compliant),
        invoke_script,
        payloads: Arc::new(tracedecay_memory_conformance::NoPayloadSourceV1),
    }))
}

/// Composes the registry with the double as the one active provider.
#[must_use]
pub fn compose_active(double: Arc<AdversarialProviderV1>) -> Arc<ProjectMemoryProviderComposition> {
    Arc::new(
        ProjectMemoryProviderComposition::compose(NativeProviderActivation::Enabled {
            fabric_config: FabricConfig {
                max_registered_providers: 1,
                max_in_flight: 2,
            },
            port: Arc::new(NativeShimV1::new(double)),
            registration_revision: REGISTRATION_REVISION,
            mode: EnabledProviderMode::Active,
        })
        .expect("enabled composition"),
    )
}

/// Composes a compliant active provider plus the supplied observer set, which
/// is how the mounted observation-delivery route is reached.
#[must_use]
pub fn compose_with_observers(
    observers: Vec<Arc<AdversarialProviderV1>>,
) -> Arc<ProjectMemoryProviderComposition> {
    let registrations = observers
        .into_iter()
        .map(|observer| ObserverProviderRegistration {
            provider: observer,
            registration_revision: REGISTRATION_REVISION,
        })
        .collect::<Vec<_>>();
    let providers = registrations.len().saturating_add(1);
    Arc::new(
        ProjectMemoryProviderComposition::compose_with_observers(
            NativeProviderActivation::Enabled {
                fabric_config: FabricConfig {
                    max_registered_providers: providers,
                    max_in_flight: 4,
                },
                port: Arc::new(NativeShimV1::new(double(
                    AdversarialScriptV1::always(MisbehaviourV1::Compliant),
                    RecallOutcomeShapeV1::WellFormed { count: 1 },
                ))),
                registration_revision: REGISTRATION_REVISION,
                mode: EnabledProviderMode::Active,
            },
            registrations,
        )
        .expect("enabled composition with observers"),
    )
}

/// Host-side scope binding standing in for the composition root.
pub struct TestScopeBinding;

impl ExactScopeBinding for TestScopeBinding {
    fn bind_exact_scope(
        &self,
        scope: &ResolvedScope,
    ) -> Result<OwnedExactScope, ExactScopeBindingError> {
        let reference = scope.reference.as_ref().ok_or_else(|| {
            ExactScopeBindingError::ReferenceUnavailable {
                project_id: scope.project_id.as_str().to_owned(),
            }
        })?;
        Ok(OwnedExactScope::new(
            "profile-adversarial",
            scope.project_id.as_str(),
            scope.repository_id.as_str(),
            scope.worktree_id.as_str(),
            reference.as_str(),
            "session-adversarial",
            scope.scope_digest.as_str(),
        )?)
    }
}

/// Records every admission report the mounted port produced.
#[derive(Default)]
pub struct LedgerObserver(pub Mutex<Vec<RecallAdmissionReport>>);

impl LedgerObserver {
    #[must_use]
    pub fn reports(&self) -> Vec<RecallAdmissionReport> {
        self.0.lock().expect("ledger lock").clone()
    }
}

impl RecallAdmissionObserver for LedgerObserver {
    fn observe_admission(
        &self,
        report: &RecallAdmissionReport,
    ) -> Result<(), RecallAdmissionAuditError> {
        self.0.lock().expect("ledger lock").push(report.clone());
        Ok(())
    }
}

pub fn budgets() -> RecallBudgetsV1 {
    RecallBudgetsV1 {
        maximum_candidates: 8,
        maximum_candidate_content_bytes: 4_096,
        maximum_total_content_bytes: 65_536,
        maximum_source_refs_per_candidate: 8,
        maximum_trace_refs_per_candidate: 8,
        maximum_warnings: 8,
        maximum_extensions_per_candidate: 8,
    }
}

/// Mounts the production cognitive-recall port over the composition.
pub fn mount(
    composition: Arc<ProjectMemoryProviderComposition>,
    observer: Arc<LedgerObserver>,
) -> Result<ProjectCognitiveRecallPortV1, CognitiveRecallPortError> {
    mount_with_boundary(composition, observer, test_invocation_boundary())
}

/// Mounts the production cognitive-recall port over the composition with a
/// caller-supplied host execution boundary, so a test can ask the host what
/// its provider workers are doing.
pub fn mount_with_boundary(
    composition: Arc<ProjectMemoryProviderComposition>,
    observer: Arc<LedgerObserver>,
    invocation_boundary: Arc<ProviderInvocationBoundaryV1>,
) -> Result<ProjectCognitiveRecallPortV1, CognitiveRecallPortError> {
    ProjectCognitiveRecallPortV1::mount(CognitiveRecallPortInputsV1 {
        invocation_boundary,
        composition,
        scope_binding: Arc::new(TestScopeBinding),
        admission_observer: observer,
        routing: ActiveRoutingPolicy::new_with_degradation(
            OwnedProviderId::new(NATIVE_PROVIDER_ID).expect("provider id"),
            REGISTRATION_REVISION,
            FallbackRule::Forbidden,
            DegradationRule::ExplicitPinned(
                PinnedDegradationPolicy::new(
                    "policy.adversarial.degradation",
                    1,
                    DegradationCause::ALL.iter().copied(),
                )
                .expect("degradation policy"),
            ),
        )
        .expect("routing policy"),
        host_limits: limits(),
        policy_revision: 1,
        budgets: budgets(),
    })
}

pub fn resolved_scope() -> ResolvedScope {
    ResolvedScope::new(
        ProjectId::new("project.adversarial").expect("project id"),
        RepositoryId::new("repository.adversarial").expect("repository id"),
        WorktreeId::new("worktree.adversarial").expect("worktree id"),
        Some(RefId::new("refs/heads/adversarial").expect("reference id")),
    )
    .expect("resolved scope")
}

pub const CANCELLATION_TOKEN_ID: &str = "token.adversarial";

pub fn live_signal() -> CancellationSignal {
    CancellationSignal::active(CANCELLATION_TOKEN_ID).expect("live cancellation signal")
}

/// One recall request with an explicit deadline offset and candidate budget.
pub fn request(deadline_offset_micros: i64, maximum_candidates: usize) -> CognitiveRecallRequest {
    let now = now_micros();
    CognitiveRecallRequest::new(
        resolved_scope(),
        RequestId::new("request.adversarial").expect("request id"),
        Deadline::new(UtcMicros(now.0.saturating_add(deadline_offset_micros))).expect("deadline"),
        CancellationContext::active(CANCELLATION_TOKEN_ID).expect("active context"),
        "adversarial recall",
        maximum_candidates,
    )
    .expect("recall request")
}

/// Concatenates every content string the application result carries, so a
/// leak assertion looks at what a prompt would actually receive.
#[must_use]
pub fn delivered_content(result: &CognitiveRecallResult) -> String {
    result
        .candidates()
        .iter()
        .map(|candidate| candidate.content())
        .collect::<Vec<_>>()
        .join("\n")
}

// ---------------------------------------------------------------------------
// Mounted observation delivery
// ---------------------------------------------------------------------------

/// The exact coding scope observation delivery runs under. It is the scope the
/// host's own binding derives, so a delivery and a recall in these tests are
/// about the same checkout.
#[must_use]
pub fn observation_scope() -> OwnedExactScope {
    TestScopeBinding
        .bind_exact_scope(&resolved_scope())
        .expect("bound exact scope")
}

/// The readiness handshake the host issues for one registered observer.
#[must_use]
pub fn observer_handshake(provider_id: &str) -> HandshakeRequest {
    tracedecay_memory_provider_api::HandshakeRequest::new(
        tracedecay_memory_provider_api::HandshakeRequestParts {
            provider_id: OwnedProviderId::new(provider_id).expect("provider id"),
            registration_revision: REGISTRATION_REVISION,
            exact_scope: observation_scope(),
            request_id: format!("observer-handshake.{provider_id}"),
            required_capabilities: vec![
                OwnedVersionedId::new("observation.accept.v1").expect("observe capability"),
            ],
            host_limits: limits(),
            control: tracedecay_memory_provider_api::OperationControl::new(
                i64::MAX,
                1_000,
                tracedecay_memory_provider_api::CancellationToken::new(),
            ),
            challenge_nonce: [7; 32],
        },
    )
    .expect("observer handshake request")
}

/// One admitted observation delivery, complete with the sanitization receipt
/// the mounted journey attaches before dispatch.
#[must_use]
pub fn observation_call(
    provider_id: &str,
    operation_id: &str,
    ready_receipt_sha256: String,
) -> ProviderCall {
    let bytes = format!(r#"{{"observation":"{operation_id}"}}"#).into_bytes();
    let payload = CanonicalPayload::new(
        OwnedVersionedId::new(OBSERVATION_CONTRACT_ID).expect("payload contract"),
        bytes.clone(),
        sha256_hex(&bytes),
    )
    .expect("observation payload");
    let receipt = tracedecay_memory_provider_api::PayloadSanitizationReceipt::new(
        tracedecay_memory_provider_api::PayloadSanitizationReceiptParts::accepted_unmodified(
            SANITIZER_REVISION,
            payload.sha256.clone(),
        ),
    )
    .expect("sanitization receipt");
    ProviderCall::new(tracedecay_memory_provider_api::ProviderCallParts {
        operation: tracedecay_memory_provider_api::ProviderOperation::Observe,
        provider_id: OwnedProviderId::new(provider_id).expect("provider id"),
        registration_revision: REGISTRATION_REVISION,
        ready_receipt_sha256,
        exact_scope: observation_scope(),
        request_id: operation_id.to_owned(),
        operation_id: operation_id.to_owned(),
        expected_state_generation: STATE_GENERATION,
        idempotency_key: Some(sha256_hex(operation_id.as_bytes())),
        control: tracedecay_memory_provider_api::OperationControl::new(
            i64::MAX,
            1_000,
            tracedecay_memory_provider_api::CancellationToken::new(),
        ),
        payload,
        required_capabilities: vec![
            OwnedVersionedId::new("observation.accept.v1").expect("observe capability"),
        ],
        extensions: Vec::new(),
    })
    .expect("observation call")
    .with_sanitization(receipt)
}

/// Runs `body` with the default panic hook silenced, so a test that
/// deliberately provokes a provider crash does not print a backtrace that
/// looks like a failure.
pub fn without_panic_noise<T>(body: impl FnOnce() -> T) -> T {
    let previous = std::panic::take_hook();
    std::panic::set_hook(Box::new(|_| {}));
    let outcome = body();
    std::panic::set_hook(previous);
    outcome
}

/// A test host worker: the execution capability the composition root supplies
/// in production. Detached, exactly like the production one, so a wedged
/// provider cannot hold the test runtime open either.
pub struct TestWorkerSpawn;

/// The in-process worker this test host starts and, exactly like the daemon,
/// cannot stop.
struct TestThreadWorkerHandle;

impl ProviderWorkerHandleV1 for TestThreadWorkerHandle {
    fn terminate(&self) -> ProviderWorkerTerminationV1 {
        ProviderWorkerTerminationV1::NotTerminable
    }
}

impl ProviderWorkerSpawnV1 for TestWorkerSpawn {
    fn isolation(&self) -> ProviderWorkerIsolationV1 {
        ProviderWorkerIsolationV1::CooperativeOnly
    }

    fn spawn_detached(
        &self,
        name: &str,
        work: ProviderWorkV1,
    ) -> Result<Box<dyn ProviderWorkerHandleV1>, ProviderWorkerSpawnErrorV1> {
        std::thread::Builder::new()
            .name(name.to_owned())
            .spawn(work)
            .map(|_joinable| -> Box<dyn ProviderWorkerHandleV1> {
                Box::new(TestThreadWorkerHandle)
            })
            .map_err(|error| ProviderWorkerSpawnErrorV1::new(error.to_string()))
    }
}

/// One host execution boundary for a mounted test port.
#[must_use]
pub fn test_invocation_boundary() -> Arc<ProviderInvocationBoundaryV1> {
    Arc::new(ProviderInvocationBoundaryV1::new(
        ProviderInvocationLimitsV1::for_in_flight(2),
        Arc::new(TestWorkerSpawn),
    ))
}
