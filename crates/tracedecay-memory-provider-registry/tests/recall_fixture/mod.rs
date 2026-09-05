//! Shared fixture for recall admission and recall port tests: a real fabric,
//! the real Native adapter, and a fixture Native application port that
//! returns a canonical recall outcome mixing in-scope, cross-scope, stale,
//! and revoked candidates.
#![allow(dead_code, clippy::expect_used, clippy::panic, clippy::unwrap_used)]

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use tracedecay_memory_provider_api::contract::TerminalCode;
use tracedecay_memory_provider_api::{
    CancellationToken, CanonicalPayload, CommittedEffectEvidence, FallbackDirective,
    HandshakeRequest, HandshakeRequestParts, HandshakeResponse, OperationControl, OwnedExactScope,
    OwnedProviderId, OwnedVersionedId, ProviderCall, ProviderCallParts, ProviderDescriptor,
    ProviderLimits, ProviderOperation, ProviderReply, TerminalRecord,
};
use tracedecay_memory_provider_native::{
    NATIVE_PROVIDER_ID, NativeMemoryApplicationPort, NativeObservation,
};
use tracedecay_memory_provider_registry::{
    AdmittedTemporalQuery, EnabledProviderMode, FabricConfig, NativeProviderActivation,
    ProjectMemoryProviderComposition, ProviderInvocationBoundaryV1, ProviderInvocationLimitsV1,
    ProviderWorkV1, ProviderWorkerHandleV1, ProviderWorkerIsolationV1, ProviderWorkerSpawnErrorV1,
    ProviderWorkerSpawnV1, ProviderWorkerTerminationV1, RECALL_PAYLOAD_CONTRACT_ID,
    RECALL_QUERY_CAPABILITY_ID, RecallBudgetsV1, RecallCandidateV1, RecallRequestParts,
    RecallScopeBindingsV1, ScopeBinding, build_recall_request_payload,
};

pub const ZERO_SHA: &str = "0000000000000000000000000000000000000000000000000000000000000000";
pub const ONE_SHA: &str = "1111111111111111111111111111111111111111111111111111111111111111";
pub const SCOPE_DIGEST: &str =
    "sha256:1111111111111111111111111111111111111111111111111111111111111111";
pub const STALE_SCOPE_DIGEST: &str =
    "sha256:2222222222222222222222222222222222222222222222222222222222222222";
pub const EVALUATION_TIME: &str = "2026-09-01T12:00:00.000000Z";
pub const SECRET_CONTENT: &str = "SECRET-TOKEN-must-never-leak-into-prompts";

pub fn sha256_hex(bytes: &[u8]) -> String {
    hex::encode(Sha256::digest(bytes))
}

pub fn admitted_scope() -> OwnedExactScope {
    OwnedExactScope::new(
        "profile-recall",
        "project-recall",
        "repository-recall",
        "worktree-recall",
        "refs/heads/recall",
        "session-recall",
        SCOPE_DIGEST,
    )
    .expect("admitted scope")
}

pub fn scope_value(scope: &OwnedExactScope) -> Value {
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

/// Bindings a unit test grants when it exercises the exact-scope rules
/// directly, standing in for a registry record of an exact-scope provider.
pub fn authorized_exact() -> RecallScopeBindingsV1 {
    RecallScopeBindingsV1::new([ScopeBinding::ExactCodingScope])
}

/// The bindings the host records for the Native provider at registration,
/// mirroring `NATIVE_RECALL_SCOPE_BINDINGS`.
///
/// Native attests owner-bound facts as `project_facts`/`profile_facts` and
/// its provider-local staged session observations as `exact_coding_scope`.
pub fn authorized_native() -> RecallScopeBindingsV1 {
    RecallScopeBindingsV1::new([
        ScopeBinding::ExactCodingScope,
        ScopeBinding::ProjectFacts,
        ScopeBinding::ProfileFacts,
    ])
}

/// A facts-only authorization set: a provider the host authorized for owner
/// facts and nothing else.
///
/// This is the set that proves binding refusal precedes field comparison; it
/// is deliberately not Native's own registration record.
pub fn authorized_facts_only() -> RecallScopeBindingsV1 {
    RecallScopeBindingsV1::new([ScopeBinding::ProjectFacts, ScopeBinding::ProfileFacts])
}

/// Candidate scope identity as an exact-scope provider attests it: every
/// field of the admitted scope plus the explicit binding.
pub fn exact_scope_candidate_value(scope: &OwnedExactScope) -> Value {
    let mut value = scope_value(scope);
    value["scope_binding"] = json!("exact_coding_scope");
    value
}

/// Candidate scope identity as the Native adapter attests a project-owned
/// fact: profile and project bound, checkout identity carried, session and
/// resolved-scope digest left empty because the binding forbids them.
pub fn project_facts_candidate_value(scope: &OwnedExactScope) -> Value {
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

/// Builds one canonical candidate. A scope value without an explicit
/// `scope_binding` is completed as `exact_coding_scope`, so unit tests that
/// mutate one identity field keep exercising the exact-scope rules.
pub fn candidate_value(candidate_id: &str, content: &str, scope: Value, validity: Value) -> Value {
    let mut scope = scope;
    if let Value::Object(fields) = &mut scope {
        fields
            .entry("scope_binding")
            .or_insert_with(|| json!("exact_coding_scope"));
    }
    json!({
        "candidate_id": candidate_id,
        "stable_memory_ref": format!("memory:{candidate_id}"),
        "content": content,
        "content_ref": Value::Null,
        "content_sha256": sha256_hex(content.as_bytes()),
        "native_score": {
            "score_domain_id": "test.score",
            "score_domain_version": 1,
            "raw_value": "0.500000",
            "direction": "higher_is_better",
            "declared_minimum": "0.000000",
            "declared_maximum": "1.000000",
            "calibration_state": "uncalibrated",
            "semantics": "fixture",
            "components": {},
        },
        "confidence": Value::Null,
        "exact_scope_identity": scope,
        "validity": validity,
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
            "summary": "fixture match",
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

pub fn current_validity() -> Value {
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

pub fn validity_with(state: &str, overrides: &[(&str, Value)]) -> Value {
    let mut validity = current_validity();
    validity["temporal_state"] = json!(state);
    for (key, value) in overrides {
        validity[*key] = value.clone();
    }
    validity
}

pub fn decode(value: Value) -> RecallCandidateV1 {
    serde_json::from_value(value).expect("fixture candidate decodes")
}

pub fn candidate(id: &str, scope: Value, validity: Value) -> RecallCandidateV1 {
    decode(candidate_value(
        id,
        &format!("content of {id}"),
        scope,
        validity,
    ))
}

pub fn with_scope_field(field: &str, value: &str) -> Value {
    let mut scope = scope_value(&admitted_scope());
    scope[field] = json!(value);
    scope
}

pub fn current_query() -> AdmittedTemporalQuery {
    AdmittedTemporalQuery::current(EVALUATION_TIME).expect("current query")
}

// --- Real fabric + Native adapter path -----------------------------------

pub struct RecallFixturePort {
    pub descriptor: ProviderDescriptor,
    pub handshake_calls: AtomicUsize,
    pub recall_calls: AtomicUsize,
    pub outcome_request_identity: Option<String>,
    /// Terminal returned by the readiness handshake.
    pub handshake_terminal_code: TerminalCode,
    /// `Success` returns the mixed candidate outcome; `SuccessZeroResults`
    /// returns a complete outcome with an empty candidate list; every other
    /// code returns a payload-less failure terminal.
    pub terminal_code: TerminalCode,
    /// Replaces the `native_score` of the named candidates in the mixed
    /// outcome, so a test can exercise host normalization over the real
    /// fabric, adapter, and port path.
    pub native_score_overrides: std::collections::BTreeMap<String, Value>,
    /// Replaces the stable provider reference of named candidates in the
    /// mixed outcome, including with `null` to exercise explicit absence.
    pub stable_memory_ref_overrides: std::collections::BTreeMap<String, Value>,
    /// Replaces the validity object of named candidates.
    pub validity_overrides: std::collections::BTreeMap<String, Value>,
    /// Replaces the whole candidate list with `(candidate_id, content)` pairs,
    /// every one of them in-scope and current, so a test can drive the real
    /// fabric, adapter, and port path with a candidate stream of its own
    /// shape — duplicate content included.
    pub candidate_contents: Option<Vec<(String, String)>>,
}

impl RecallFixturePort {
    pub fn new() -> Self {
        Self {
            descriptor: descriptor(),
            handshake_calls: AtomicUsize::new(0),
            recall_calls: AtomicUsize::new(0),
            outcome_request_identity: None,
            handshake_terminal_code: TerminalCode::Success,
            terminal_code: TerminalCode::Success,
            native_score_overrides: std::collections::BTreeMap::new(),
            stable_memory_ref_overrides: std::collections::BTreeMap::new(),
            validity_overrides: std::collections::BTreeMap::new(),
            candidate_contents: None,
        }
    }

    pub fn outcome_value(&self, call: &ProviderCall) -> Value {
        let mut outcome = match &self.candidate_contents {
            Some(contents) => self.explicit_outcome_value(call, contents),
            None => self.mixed_outcome_value(call),
        };
        if let Value::Array(candidates) = &mut outcome["candidates"] {
            for candidate in candidates.iter_mut() {
                let id = candidate["candidate_id"]
                    .as_str()
                    .unwrap_or_default()
                    .to_owned();
                if let Some(score) = self.native_score_overrides.get(&id) {
                    candidate["native_score"] = score.clone();
                }
                if let Some(stable_memory_ref) = self.stable_memory_ref_overrides.get(&id) {
                    candidate["stable_memory_ref"] = stable_memory_ref.clone();
                }
                if let Some(validity) = self.validity_overrides.get(&id) {
                    candidate["validity"] = validity.clone();
                }
            }
        }
        if self.terminal_code == TerminalCode::SuccessZeroResults {
            outcome["candidates"] = json!([]);
            outcome["coverage"]["state"] = json!("zero_results");
            for counter in ["scanned_items", "matched_items", "returned_items"] {
                outcome["coverage"][counter] = json!(0);
            }
            outcome["terminal"]["terminal_code"] = json!("success_zero_results");
        } else if self.terminal_code == TerminalCode::Partial {
            outcome["terminal"]["terminal_code"] = json!("partial");
        }
        outcome
    }

    /// A complete, successful outcome whose candidates are exactly the
    /// supplied `(candidate_id, content)` pairs, all attested as in-scope
    /// current project facts.
    fn explicit_outcome_value(&self, call: &ProviderCall, contents: &[(String, String)]) -> Value {
        let candidate_scope = project_facts_candidate_value(&call.exact_scope);
        let candidates: Vec<Value> = contents
            .iter()
            .map(|(candidate_id, content)| {
                candidate_value(
                    candidate_id,
                    content,
                    candidate_scope.clone(),
                    current_validity(),
                )
            })
            .collect();
        let count = candidates.len();
        let mut outcome = self.mixed_outcome_value(call);
        outcome["candidates"] = Value::Array(candidates);
        for counter in ["scanned_items", "matched_items", "returned_items"] {
            outcome["coverage"][counter] = json!(count);
        }
        outcome
    }

    fn mixed_outcome_value(&self, call: &ProviderCall) -> Value {
        let scope = scope_value(&call.exact_scope);
        // Candidates attest what the Native adapter attests for project-owned
        // facts. The host also records Native as authorized for
        // `exact_coding_scope`, which its staged session observations use, so
        // an exact-scope candidate here is judged on its fields rather than
        // refused on the binding.
        let candidate_scope = project_facts_candidate_value(&call.exact_scope);
        let foreign_worktree = {
            let mut foreign = candidate_scope.clone();
            foreign["worktree_identity"] = json!("worktree-other");
            foreign
        };
        let foreign_repository = {
            let mut foreign = candidate_scope.clone();
            foreign["repository_identity"] = json!("repository-other");
            foreign
        };
        // A full exact-scope attestation whose every identity field matches
        // except the resolved-scope digest: it belongs to an earlier
        // resolution of this checkout, so it is stale rather than in scope.
        // This is the shape a staged observation carries after the scope is
        // re-resolved, and it must not be admitted.
        let stale_exact_scope = {
            let mut exact = exact_scope_candidate_value(&call.exact_scope);
            exact["resolved_scope_digest"] = json!(STALE_SCOPE_DIGEST);
            exact
        };
        let candidates = vec![
            candidate_value(
                "in-scope-1",
                "first in-scope memory",
                candidate_scope.clone(),
                current_validity(),
            ),
            candidate_value(
                "cross-worktree",
                SECRET_CONTENT,
                foreign_worktree,
                current_validity(),
            ),
            candidate_value(
                "revoked",
                "revoked memory",
                candidate_scope.clone(),
                validity_with(
                    "revoked",
                    &[("revoked_at", json!("2026-08-15T00:00:00.000000Z"))],
                ),
            ),
            candidate_value(
                "cross-repository",
                SECRET_CONTENT,
                foreign_repository,
                current_validity(),
            ),
            candidate_value(
                "stale-exact-scope",
                "exact-scope memory from a superseded scope resolution",
                stale_exact_scope,
                current_validity(),
            ),
            candidate_value(
                "in-scope-2",
                "second in-scope memory",
                candidate_scope,
                current_validity(),
            ),
        ];
        json!({
            "provider_id": NATIVE_PROVIDER_ID,
            "provider_instance_id": "native.recall-fixture",
            "registration_revision": call.registration_revision,
            "ready_receipt_digest": call.ready_receipt_sha256,
            "request_identity": self
                .outcome_request_identity
                .clone()
                .unwrap_or_else(|| call.request_id.clone()),
            "exact_scope_identity": scope,
            "provider_state_generation": call.expected_state_generation,
            "candidates": candidates,
            "coverage": {
                "state": "complete",
                "searched_scope_digest": call.exact_scope.exact_scope_sha256(),
                "searched_temporal_digest": ZERO_SHA,
                "scanned_items": 6,
                "matched_items": 6,
                "returned_items": 6,
                "excluded_items": 0,
                "truncated_items": 0,
                "next_cursor": Value::Null,
                "reasons": [],
            },
            "ordering": {
                "score_domain_id": "test.score",
                "direction": "higher_is_better",
                "tie_breaker": "candidate_id_lexicographic_utf8",
            },
            "terminal": {"terminal_code": "success", "diagnostic_id": Value::Null},
            "warnings": [],
        })
    }
}

pub fn unexpected<T>() -> T {
    panic!("recall admission tests must not execute this provider operation")
}

impl NativeMemoryApplicationPort for RecallFixturePort {
    fn descriptor(&self) -> ProviderDescriptor {
        self.descriptor.clone()
    }

    fn handshake(&self, request: &HandshakeRequest) -> HandshakeResponse {
        self.handshake_calls.fetch_add(1, Ordering::Relaxed);
        let ready = self.handshake_terminal_code == TerminalCode::Success;
        HandshakeResponse {
            terminal: TerminalRecord::new(
                ProviderOperation::Handshake,
                OwnedProviderId::new(NATIVE_PROVIDER_ID).expect("provider id"),
                self.handshake_terminal_code,
                CommittedEffectEvidence::none(Some(self.descriptor.state_generation)),
                FallbackDirective::forbidden(),
                request.request_id.clone(),
                request.exact_scope.exact_scope_sha256(),
                (self.handshake_terminal_code != TerminalCode::Success)
                    .then(|| "native.recall-fixture.handshake".to_owned()),
            )
            .expect("handshake terminal"),
            descriptor: ready.then(|| self.descriptor.clone()),
            provider_instance_id: ready.then(|| "native.recall-fixture".to_owned()),
            state_namespace: ready.then(|| "native.recall-scope".to_owned()),
            accepted_scope: ready.then(|| request.exact_scope.clone()),
            effective_limits: ready.then(|| request.host_limits.minimum(self.descriptor.limits)),
            ready_receipt_sha256: ready.then(|| ONE_SHA.to_owned()),
            warnings: Vec::new(),
        }
    }

    fn health(&self, _call: &ProviderCall) -> ProviderReply {
        unexpected()
    }

    fn observe(&self, _observation: NativeObservation<'_>) -> ProviderReply {
        unexpected()
    }

    fn recall(&self, call: &ProviderCall) -> ProviderReply {
        self.recall_calls.fetch_add(1, Ordering::Relaxed);
        let payload = if matches!(
            self.terminal_code,
            TerminalCode::Success | TerminalCode::SuccessZeroResults | TerminalCode::Partial
        ) {
            let bytes = serde_json::to_vec(&self.outcome_value(call)).expect("outcome bytes");
            let sha256 = sha256_hex(&bytes);
            Some(
                CanonicalPayload::new(
                    OwnedVersionedId::new(RECALL_PAYLOAD_CONTRACT_ID).expect("contract"),
                    bytes,
                    sha256,
                )
                .expect("payload"),
            )
        } else {
            None
        };
        ProviderReply {
            terminal: TerminalRecord::new(
                call.operation,
                call.provider_id.clone(),
                self.terminal_code,
                CommittedEffectEvidence::none(Some(call.expected_state_generation)),
                FallbackDirective::forbidden(),
                call.operation_id.clone(),
                call.exact_scope.exact_scope_sha256(),
                (!matches!(
                    self.terminal_code,
                    TerminalCode::Success | TerminalCode::SuccessZeroResults
                ))
                .then(|| "native.recall-fixture.failure".to_owned()),
            )
            .expect("recall terminal"),
            payload,
            warnings: Vec::new(),
            extensions: Vec::new(),
            state_generation: call.expected_state_generation,
        }
    }

    fn feedback(&self, _call: &ProviderCall) -> ProviderReply {
        unexpected()
    }

    fn maintenance(&self, _call: &ProviderCall) -> ProviderReply {
        unexpected()
    }

    fn inspection(&self, _call: &ProviderCall) -> ProviderReply {
        unexpected()
    }

    fn correction(&self, _call: &ProviderCall) -> ProviderReply {
        unexpected()
    }

    fn delete_by_source(&self, _call: &ProviderCall) -> ProviderReply {
        unexpected()
    }

    fn snapshot_export(&self, _call: &ProviderCall) -> ProviderReply {
        unexpected()
    }

    fn snapshot_restore(&self, _call: &ProviderCall) -> ProviderReply {
        unexpected()
    }

    fn replay(&self, _call: &ProviderCall) -> ProviderReply {
        unexpected()
    }
}

pub fn limits() -> ProviderLimits {
    ProviderLimits {
        request_bytes: 8_192,
        response_bytes: 65_536,
        observation_batch_items: 16,
        recall_candidates: 32,
        concurrent_operations: 4,
        operation_millis: 1_000,
        snapshot_bytes: 65_536,
        inspection_items: 64,
    }
}

pub fn descriptor() -> ProviderDescriptor {
    ProviderDescriptor::new(
        OwnedProviderId::new(NATIVE_PROVIDER_ID).expect("provider id"),
        ZERO_SHA,
        "recall-admission-test-v1",
        5,
        [
            OwnedVersionedId::new("provider.health.v1").expect("health capability"),
            OwnedVersionedId::new("observation.accept.v1").expect("observe capability"),
            OwnedVersionedId::new(RECALL_QUERY_CAPABILITY_ID).expect("recall capability"),
        ],
        limits(),
    )
    .expect("descriptor")
}

pub fn budgets() -> RecallBudgetsV1 {
    RecallBudgetsV1 {
        maximum_candidates: 8,
        maximum_candidate_content_bytes: 4_096,
        maximum_total_content_bytes: 16_384,
        maximum_source_refs_per_candidate: 8,
        maximum_trace_refs_per_candidate: 8,
        maximum_warnings: 8,
        maximum_extensions_per_candidate: 8,
    }
}

pub fn compose(port: Arc<RecallFixturePort>) -> ProjectMemoryProviderComposition {
    ProjectMemoryProviderComposition::compose(NativeProviderActivation::Enabled {
        fabric_config: FabricConfig {
            max_registered_providers: 1,
            max_in_flight: 2,
        },
        port,
        registration_revision: 31,
        mode: EnabledProviderMode::Active,
    })
    .expect("enabled composition")
}

pub fn recall_call(response: &HandshakeResponse, temporal: &AdmittedTemporalQuery) -> ProviderCall {
    let ready_receipt_sha256 = response
        .ready_receipt_sha256
        .clone()
        .expect("ready receipt");
    let payload = build_recall_request_payload(&RecallRequestParts {
        provider_id: OwnedProviderId::new(NATIVE_PROVIDER_ID).expect("provider id"),
        registration_revision: 31,
        ready_receipt_sha256: ready_receipt_sha256.clone(),
        exact_scope: admitted_scope(),
        request_id: "recall-request-1".to_owned(),
        objective: "search project memory".to_owned(),
        query: "recall admission".to_owned(),
        temporal: temporal.clone(),
        budgets: budgets(),
        policy_revision: 1,
        deadline_utc_micros: i64::MAX,
        remaining_millis: 1_000,
    })
    .expect("request payload");
    ProviderCall::new(ProviderCallParts {
        operation: ProviderOperation::Recall,
        provider_id: OwnedProviderId::new(NATIVE_PROVIDER_ID).expect("provider id"),
        registration_revision: 31,
        ready_receipt_sha256,
        exact_scope: admitted_scope(),
        request_id: "recall-request-1".to_owned(),
        operation_id: "recall-operation-1".to_owned(),
        expected_state_generation: 5,
        idempotency_key: None,
        control: OperationControl::new(i64::MAX, 1_000, CancellationToken::new()),
        payload,
        required_capabilities: vec![
            OwnedVersionedId::new(RECALL_QUERY_CAPABILITY_ID).expect("recall capability"),
        ],
        extensions: Vec::new(),
    })
    .expect("recall call")
}

pub fn handshake() -> HandshakeRequest {
    HandshakeRequest::new(HandshakeRequestParts {
        provider_id: OwnedProviderId::new(NATIVE_PROVIDER_ID).expect("provider id"),
        registration_revision: 31,
        exact_scope: admitted_scope(),
        request_id: "recall-handshake".to_owned(),
        required_capabilities: vec![
            OwnedVersionedId::new(RECALL_QUERY_CAPABILITY_ID).expect("recall capability"),
        ],
        host_limits: limits(),
        control: OperationControl::new(i64::MAX, 1_000, CancellationToken::new()),
        challenge_nonce: [7; 32],
    })
    .expect("handshake request")
}

// ---------------------------------------------------------------------------
// tdmem-0903: an injected evaluation observer for the composed provider set.
// ---------------------------------------------------------------------------

/// Stable identity of the injected evaluation observer used by provider-set
/// tests. It is deliberately not the Native identity: the composition refuses
/// an observer that declares the separately selected active provider.
pub const EVALUATION_OBSERVER_PROVIDER_ID: &str = "provider.evaluation-observer";

/// Contract id of the observation payload these tests dispatch.
pub const OBSERVER_PAYLOAD_CONTRACT_ID: &str = "tracedecay.memory.observation-test.v1";

/// Sanitizer revision the fixture stands in for. The real revision comes from
/// `tracedecay-memory-hygiene`; dispatch only requires a self-consistent
/// receipt that binds the dispatched payload.
pub const OBSERVER_SANITIZER_REVISION: &str =
    "tracedecay.memory.observation.hygiene.v1+recall-fixture";

/// What an injected observer does when the host delivers an observation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ObserverBehaviour {
    /// Accept the observation and return a well-formed committed terminal.
    Accepts,
    /// Return a terminal misattributed to another operation kind, which the
    /// fabric must refuse. Stands in for any provider-side observer defect.
    FailsDelivery,
}

/// A concrete evaluation observer adapter, injected by the composition root.
///
/// It counts every handshake and invocation so a test can prove the observer
/// really ran (or really was refused) rather than inferring it from a return
/// value.
pub struct EvaluationObserverProvider {
    behaviour: ObserverBehaviour,
    pub handshakes: AtomicUsize,
    pub invocations: AtomicUsize,
}

impl EvaluationObserverProvider {
    #[must_use]
    pub fn new(behaviour: ObserverBehaviour) -> Self {
        Self {
            behaviour,
            handshakes: AtomicUsize::new(0),
            invocations: AtomicUsize::new(0),
        }
    }

    #[must_use]
    pub fn observer_descriptor() -> ProviderDescriptor {
        ProviderDescriptor::new(
            OwnedProviderId::new(EVALUATION_OBSERVER_PROVIDER_ID).expect("observer id"),
            ONE_SHA,
            "observer-state-v1",
            0,
            [
                OwnedVersionedId::new("provider.health.v1").expect("health capability"),
                OwnedVersionedId::new("observation.accept.v1").expect("observe capability"),
                // Declared deliberately: the observer is *capable* of recall
                // and is refused anyway, so the refusal is proven to come
                // from the mode gate and the absent recall authorization, not
                // from a missing capability.
                OwnedVersionedId::new(RECALL_QUERY_CAPABILITY_ID).expect("recall capability"),
            ],
            limits(),
        )
        .expect("observer descriptor")
    }

    #[must_use]
    pub fn handshake_count(&self) -> usize {
        self.handshakes.load(Ordering::Acquire)
    }

    #[must_use]
    pub fn invocation_count(&self) -> usize {
        self.invocations.load(Ordering::Acquire)
    }
}

impl tracedecay_memory_provider_api::MemoryProvider for EvaluationObserverProvider {
    fn descriptor(&self) -> ProviderDescriptor {
        Self::observer_descriptor()
    }

    fn handshake(&self, request: &HandshakeRequest) -> HandshakeResponse {
        self.handshakes.fetch_add(1, Ordering::AcqRel);
        let descriptor = Self::observer_descriptor();
        HandshakeResponse {
            terminal: TerminalRecord::new(
                ProviderOperation::Handshake,
                descriptor.provider_id.clone(),
                TerminalCode::Success,
                CommittedEffectEvidence::none(Some(descriptor.state_generation)),
                FallbackDirective::forbidden(),
                &request.request_id,
                request.exact_scope.exact_scope_sha256(),
                None,
            )
            .expect("observer handshake terminal"),
            descriptor: Some(descriptor.clone()),
            provider_instance_id: Some("observer.instance-1".to_owned()),
            state_namespace: Some("observer-namespace-1".to_owned()),
            accepted_scope: Some(request.exact_scope.clone()),
            effective_limits: Some(request.host_limits.minimum(descriptor.limits)),
            ready_receipt_sha256: Some(ONE_SHA.to_owned()),
            warnings: Vec::new(),
        }
    }

    fn invoke(&self, call: &ProviderCall) -> ProviderReply {
        self.invocations.fetch_add(1, Ordering::AcqRel);
        // The failing variant misattributes its terminal to another operation
        // kind: a provider defect the fabric must refuse before it ever
        // retains an observer terminal.
        let (operation, effect, state_generation) = match self.behaviour {
            ObserverBehaviour::Accepts => (
                call.operation,
                CommittedEffectEvidence::committed(
                    0,
                    1,
                    vec![call.operation_id.clone()],
                    ONE_SHA,
                    ONE_SHA,
                )
                .expect("observer committed effect"),
                1,
            ),
            ObserverBehaviour::FailsDelivery => (
                ProviderOperation::Recall,
                CommittedEffectEvidence::none(Some(0)),
                0,
            ),
        };
        ProviderReply {
            terminal: TerminalRecord::new(
                operation,
                OwnedProviderId::new(EVALUATION_OBSERVER_PROVIDER_ID).expect("observer id"),
                TerminalCode::Success,
                effect,
                FallbackDirective::forbidden(),
                &call.operation_id,
                call.exact_scope.exact_scope_sha256(),
                None,
            )
            .expect("observer terminal"),
            payload: None,
            warnings: Vec::new(),
            extensions: Vec::new(),
            state_generation,
        }
    }
}

/// The readiness handshake request the host issues for an injected observer.
#[must_use]
pub fn observer_handshake_request(
    exact_scope: OwnedExactScope,
    registration_revision: u64,
) -> HandshakeRequest {
    HandshakeRequest::new(HandshakeRequestParts {
        provider_id: OwnedProviderId::new(EVALUATION_OBSERVER_PROVIDER_ID).expect("observer id"),
        registration_revision,
        exact_scope,
        request_id: "observer-handshake-1".to_owned(),
        required_capabilities: vec![
            OwnedVersionedId::new("provider.health.v1").expect("health capability"),
            OwnedVersionedId::new("observation.accept.v1").expect("observe capability"),
        ],
        host_limits: limits(),
        control: OperationControl::new(i64::MAX, 1_000, CancellationToken::new()),
        challenge_nonce: [7; 32],
    })
    .expect("observer handshake request")
}

/// One admitted observation delivery for an injected observer, complete with
/// the sanitization receipt observation dispatch fails closed without.
#[must_use]
pub fn observer_observation_call(
    exact_scope: OwnedExactScope,
    registration_revision: u64,
    ready_receipt_sha256: String,
) -> ProviderCall {
    let bytes = br#"{"observation":"fixture"}"#.to_vec();
    let payload = CanonicalPayload::new(
        OwnedVersionedId::new(OBSERVER_PAYLOAD_CONTRACT_ID).expect("payload contract"),
        bytes.clone(),
        sha256_hex(&bytes),
    )
    .expect("observation payload");
    let receipt = tracedecay_memory_provider_api::PayloadSanitizationReceipt::new(
        tracedecay_memory_provider_api::PayloadSanitizationReceiptParts::accepted_unmodified(
            OBSERVER_SANITIZER_REVISION,
            payload.sha256.clone(),
        ),
    )
    .expect("sanitization receipt");
    ProviderCall::new(ProviderCallParts {
        operation: ProviderOperation::Observe,
        provider_id: OwnedProviderId::new(EVALUATION_OBSERVER_PROVIDER_ID).expect("observer id"),
        registration_revision,
        ready_receipt_sha256,
        exact_scope,
        request_id: "observer-observation-1".to_owned(),
        operation_id: "observer-observation-operation-1".to_owned(),
        expected_state_generation: 0,
        idempotency_key: Some("observer-idempotency-1".to_owned()),
        control: OperationControl::new(i64::MAX, 1_000, CancellationToken::new()),
        payload,
        required_capabilities: vec![
            OwnedVersionedId::new("observation.accept.v1").expect("observe capability"),
        ],
        extensions: Vec::new(),
    })
    .expect("observation call")
    .with_sanitization(receipt)
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
