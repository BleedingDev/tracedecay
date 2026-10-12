//! Adapter-boundary reproduction for the historical NCM recall loss.
//!
//! The surface is deterministic and contains only opaque worker rows. This is
//! a passing adapter contract diagnostic: the adapter prefix-clips a high
//! ranked row under the caller byte budget and reports a partial result. The
//! failing backfill reproduction lives at the pure core seam.

#![allow(
    clippy::expect_used,
    clippy::field_reassign_with_default,
    clippy::panic,
    clippy::unwrap_used
)]
#![doc = "Controlled NCM adapter recall clipping contract diagnostic."]

use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::sync::Arc;
use tracedecay_memory_provider_api::contract::TerminalCode;
use tracedecay_memory_provider_api::{
    CancellationToken, CanonicalPayload, CommittedEffectEvidence, FallbackDirective,
    HandshakeRequest, HandshakeRequestParts, MemoryProvider, OperationControl, OwnedExactScope,
    OwnedProviderId, OwnedVersionedId, ProviderCall, ProviderCallParts, ProviderDescriptor,
    ProviderLimits, ProviderOperation, ProviderReply, TerminalRecord,
};
use tracedecay_memory_provider_ncm::{
    NCM_PROVIDER_ID, NcmCognitiveSurface, NcmNamespace, NcmProviderAdapter, NcmSurfaceCall,
    NcmSurfaceHandshakeRequest, NcmSurfaceHandshakeResponse,
};
use tracedecay_memory_provider_registry::recall_admission::{
    AdmittedTemporalQuery, RecallBudgetsV1, RecallRequestParts, build_recall_request_payload,
};

const ZERO_SHA: &str = "0000000000000000000000000000000000000000000000000000000000000000";
const ONE_SHA: &str = "1111111111111111111111111111111111111111111111111111111111111111";
const RESOLVED_SCOPE_DIGEST: &str =
    "sha256:1111111111111111111111111111111111111111111111111111111111111111";

/// Deterministic worker surface whose recall reply has one oversized high
/// score row followed by a smaller row that fits the same request.
struct BudgetSurface {
    descriptor: ProviderDescriptor,
}

impl BudgetSurface {
    fn new() -> Self {
        let capabilities = [
            "provider.health.v1",
            "observation.accept.v1",
            "recall.query.v1",
        ]
        .into_iter()
        .map(|capability| OwnedVersionedId::new(capability).expect("capability"))
        .collect::<Vec<_>>();
        Self {
            descriptor: ProviderDescriptor::new(
                OwnedProviderId::new(NCM_PROVIDER_ID).expect("NCM provider id"),
                ZERO_SHA,
                "ncm-state-v1",
                4,
                capabilities,
                limits(),
            )
            .expect("descriptor"),
        }
    }

    fn terminal(
        &self,
        operation: ProviderOperation,
        operation_id: &str,
        namespace: &NcmNamespace,
        generation: Option<u64>,
    ) -> TerminalRecord {
        TerminalRecord::new(
            operation,
            self.descriptor.provider_id.clone(),
            TerminalCode::Success,
            CommittedEffectEvidence::none(generation),
            FallbackDirective::forbidden(),
            operation_id.to_owned(),
            namespace.as_str(),
            None,
        )
        .expect("surface terminal")
    }
}

impl NcmCognitiveSurface for BudgetSurface {
    fn descriptor(&self) -> ProviderDescriptor {
        self.descriptor.clone()
    }

    fn handshake(&self, request: &NcmSurfaceHandshakeRequest) -> NcmSurfaceHandshakeResponse {
        let receipt = ONE_SHA.to_owned();
        NcmSurfaceHandshakeResponse {
            terminal: self.terminal(
                ProviderOperation::Handshake,
                &request.request_id,
                &request.namespace,
                None,
            ),
            descriptor: Some(self.descriptor.clone()),
            provider_instance_id: Some("ncm.instance.reproduction".to_owned()),
            namespace: Some(request.namespace.clone()),
            effective_limits: Some(request.host_limits.minimum(self.descriptor.limits)),
            ready_receipt_sha256: Some(receipt.clone()),
            challenge_response_sha256: Some(request.expected_challenge_response_sha256(
                &self.descriptor,
                "ncm.instance.reproduction",
                &receipt,
            )),
            warnings: Vec::new(),
        }
    }

    fn invoke(&self, call: &NcmSurfaceCall) -> ProviderReply {
        let payload_bytes = if call.operation == ProviderOperation::Recall {
            serde_json::to_vec(&json!({
                "common_recall": {
                    "candidates": [
                        row(call, 1, &"oversized-ranked-candidate-".repeat(32), 1.0),
                        row(call, 2, "fits-sequence-3", 0.5)
                    ],
                    "truncated": false,
                    "unknown_items": 0,
                    "excluded_items": 0,
                    "scanned_items": 2,
                    "score_upper_bound": 1.0
                }
            }))
            .expect("worker payload")
        } else {
            call.payload.bytes.clone()
        };
        let payload = CanonicalPayload::new(
            call.payload.contract_id.clone(),
            payload_bytes.clone(),
            hex_digest(&Sha256::digest(&payload_bytes)),
        )
        .expect("surface payload");
        ProviderReply {
            terminal: self.terminal(
                call.operation,
                &call.operation_id,
                &call.namespace,
                Some(call.expected_state_generation),
            ),
            payload: Some(payload),
            warnings: Vec::new(),
            extensions: Vec::new(),
            state_generation: call.expected_state_generation,
        }
    }
}

fn limits() -> ProviderLimits {
    ProviderLimits {
        request_bytes: 1_048_576,
        response_bytes: 1_048_576,
        observation_batch_items: 16,
        recall_candidates: 16,
        concurrent_operations: 1,
        operation_millis: 1_000,
        snapshot_bytes: 1_048_576,
        inspection_items: 64,
    }
}

fn scope() -> OwnedExactScope {
    OwnedExactScope::new(
        "profile-reproduction",
        "project-reproduction",
        "repository-reproduction",
        "worktree-reproduction",
        "refs/heads/reproduction",
        "session-reproduction",
        RESOLVED_SCOPE_DIGEST,
    )
    .expect("scope")
}

fn scope_value(exact: &OwnedExactScope) -> Value {
    json!({
        "profile_id": exact.profile_id,
        "project_id": exact.project_id,
        "repository_identity": exact.repository_identity,
        "worktree_identity": exact.worktree_identity,
        "branch_identity": exact.branch_identity,
        "agent_session_id": exact.agent_session_id,
        "resolved_scope_digest": exact.resolved_scope_digest
    })
}

fn opaque(namespace: &NcmNamespace, kind: &[u8], value: &str) -> String {
    let mut digest = Sha256::new();
    digest.update(b"tracedecay.ncm.opaque-id.v1");
    digest.update([0]);
    for field in [namespace.as_str().as_bytes(), kind, value.as_bytes()] {
        digest.update((field.len() as u64).to_be_bytes());
        digest.update(field);
    }
    hex_digest(&digest.finalize())
}

fn stable_reference(namespace: &NcmNamespace, record_id: u64, capsule: &str) -> String {
    let mut digest = Sha256::new();
    for field in [
        b"tracedecay.ncm.memory-reference.v1".as_slice(),
        namespace.as_str().as_bytes(),
        &record_id.to_be_bytes(),
        capsule.as_bytes(),
    ] {
        digest.update((field.len() as u64).to_be_bytes());
        digest.update(field);
    }
    format!("ncm-memory:{}", hex_digest(&digest.finalize()))
}

fn source_id(
    namespace: &NcmNamespace,
    exact: &OwnedExactScope,
    source_key: &str,
) -> (String, String) {
    let identity = serde_json::to_string(&json!([
        exact.profile_id,
        exact.project_id,
        "codex",
        "codex-session",
        source_key
    ]))
    .expect("source identity");
    (
        opaque(namespace, b"common-source-key-v1", &identity),
        opaque(namespace, b"forget-source-key", source_key),
    )
}

fn row(call: &NcmSurfaceCall, record_id: u64, content: &str, activation: f64) -> Value {
    let exact = scope();
    let namespace = call.namespace.clone();
    let source_key = format!("source-{record_id}");
    let (full_source_id, legacy_source_id) = source_id(&namespace, &exact, &source_key);
    let original = json!({
        "source": {
            "canonical_provider_id": "codex",
            "canonical_session_id": "codex-session",
            "source_key": source_key,
            "stable_record_id": format!("record-{record_id}"),
            "observation_id": format!("observation-{record_id}"),
            "source_revision": format!("revision-{record_id}"),
            "content_sha256": "ab".repeat(32)
        },
        "origin_scope": {
            "state": "recorded",
            "exact_scope_identity": scope_value(&exact),
            "authority_ref": "reproduction-authority"
        },
        "source_sequence": record_id + 1,
        "occurred_at": "2026-01-01T00:00:00Z",
        "ingested_at": "2026-01-01T00:00:00Z",
        "validity": {
            "valid_from": "2026-01-01T00:00:00Z",
            "valid_until": null,
            "superseded_at": null,
            "superseded_by": null,
            "revoked_at": null
        }
    });
    let retained = json!({
        "original_source": original,
        "canonical_payload": {"role": "assistant", "content": content},
        "source_refs": [format!("record:{record_id}")],
        "delivery_scope": scope_value(&exact),
        "projection": {
            "key_text": content,
            "value_text": format!("assistant: {content}"),
            "observation_kind": "session.message_committed.v1"
        }
    });
    let retained_bytes = serde_json::to_vec(&retained).expect("retained capsule");
    let capsule_digest = hex_digest(&Sha256::digest(&retained_bytes));
    let stable = stable_reference(&namespace, record_id, &capsule_digest);
    let surface_payload: Value =
        serde_json::from_slice(&call.payload.bytes).expect("projected recall payload");
    let request_token = surface_payload["selection"]["request_token"]
        .as_str()
        .expect("opaque request token");
    let mut candidate_digest = Sha256::new();
    for field in [request_token.as_bytes(), stable.as_bytes()] {
        candidate_digest.update((field.len() as u64).to_be_bytes());
        candidate_digest.update(field);
    }
    json!({
        "record_id": record_id,
        "source": full_source_id,
        "stable_memory_ref": stable,
        "candidate_id": format!("ncm-candidate:{}", hex_digest(&candidate_digest.finalize())),
        "key_text": content,
        "value_text": format!("assistant: {content}"),
        "activation": activation,
        "provenance": {
            "source_binding": {
                "version": 1,
                "source_id": full_source_id,
                "legacy_source_id": legacy_source_id
            },
            "common_capsule": {
                "version": 1,
                "bytes": retained_bytes,
                "sha256": capsule_digest
            }
        }
    })
}

fn handshake_request() -> HandshakeRequest {
    HandshakeRequest::new(HandshakeRequestParts {
        provider_id: OwnedProviderId::new(NCM_PROVIDER_ID).expect("provider id"),
        registration_revision: 1,
        exact_scope: scope(),
        request_id: "ncm-reproduction-handshake".to_owned(),
        required_capabilities: vec![
            OwnedVersionedId::new("provider.health.v1").expect("health capability"),
            OwnedVersionedId::new("observation.accept.v1").expect("observe capability"),
            OwnedVersionedId::new("recall.query.v1").expect("recall capability"),
        ],
        host_limits: limits(),
        control: OperationControl::new(i64::MAX, 500, CancellationToken::new()),
        challenge_nonce: [7; 32],
    })
    .expect("handshake request")
}

fn recall_call(ready_receipt: String) -> ProviderCall {
    let exact = scope();
    let request_id = "ncm-reproduction-recall-request".to_owned();
    let payload = build_recall_request_payload(&RecallRequestParts {
        provider_id: OwnedProviderId::new(NCM_PROVIDER_ID).expect("provider id"),
        registration_revision: 1,
        ready_receipt_sha256: ready_receipt.clone(),
        exact_scope: exact.clone(),
        request_id: request_id.clone(),
        objective: "reproduce historical recall".to_owned(),
        query: "what did the quicksilver retry budget change record, down to the obsidian-ledger-tail note?"
            .to_owned(),
        temporal: AdmittedTemporalQuery::current("2026-01-02T00:00:00Z")
            .expect("valid current temporal query"),
        budgets: RecallBudgetsV1 {
            maximum_candidates: 2,
            maximum_candidate_content_bytes: 128,
            maximum_total_content_bytes: 32,
            maximum_source_refs_per_candidate: 1,
            maximum_trace_refs_per_candidate: 1,
            maximum_warnings: 4,
            maximum_extensions_per_candidate: 1,
        },
        policy_revision: 1,
        deadline_utc_micros: i64::MAX,
        remaining_millis: 500,
    })
    .expect("canonical recall payload");
    ProviderCall::new(ProviderCallParts {
        operation: ProviderOperation::Recall,
        provider_id: OwnedProviderId::new(NCM_PROVIDER_ID).expect("provider id"),
        registration_revision: 1,
        ready_receipt_sha256: ready_receipt,
        exact_scope: exact,
        request_id,
        operation_id: "ncm-reproduction-recall-operation".to_owned(),
        expected_state_generation: 4,
        idempotency_key: None,
        control: OperationControl::new(i64::MAX, 500, CancellationToken::new()),
        payload,
        required_capabilities: vec![
            OwnedVersionedId::new("recall.query.v1").expect("recall capability"),
        ],
        extensions: Vec::new(),
    })
    .expect("recall call")
}

fn hex_digest(value: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut result = String::with_capacity(value.len() * 2);
    for byte in value {
        result.push(char::from(HEX[usize::from(byte >> 4)]));
        result.push(char::from(HEX[usize::from(byte & 0x0f)]));
    }
    result
}

/// Negative control for the byte-budget hypothesis: adapter reconstruction is
/// expected to prefix-clip the high-ranked row and report partial coverage.
/// The separate historical hypothesis is a transient instance-proof
/// OnceLock<Option<String>> that caches None and suppresses replay until
/// daemon recreation. The failing candidate backfill assertion belongs to the
/// runtime integration test, before this adapter boundary.
#[test]
fn reconstruction_prefix_clips_a_budgeted_candidate_without_claiming_core_backfill() {
    let surface = Arc::new(BudgetSurface::new());
    let adapter = NcmProviderAdapter::new(surface).expect("adapter");
    let handshake = handshake_request();
    let ready = adapter.handshake(&handshake);
    assert_eq!(
        ready.terminal.terminal_code(),
        TerminalCode::Success,
        "{ready:?}"
    );
    let call = recall_call(
        ready
            .ready_receipt_sha256
            .expect("successful handshake receipt"),
    );
    let reply = adapter.invoke(&call);
    assert!(
        matches!(
            reply.terminal.terminal_code(),
            TerminalCode::Success | TerminalCode::Partial
        ),
        "budgeted recall may be complete or partial, but must remain a valid reply: {reply:?}"
    );
    let payload = reply.payload.expect("reconstructed recall payload");
    let value: Value = serde_json::from_slice(&payload.bytes).expect("reconstructed JSON");
    let candidates = value["candidates"].as_array().expect("candidate array");
    assert_eq!(
        candidates.len(),
        1,
        "the total byte budget admits one prefix"
    );
    assert_eq!(
        candidates[0]["provenance"]["original_sources"][0]["source_sequence"], 2,
        "the adapter keeps the ranked row it clips; core backfill is tested separately"
    );
    // The high row is clipped to the remaining 32 bytes, and the following
    // row is scanned with zero bytes left. Both are reported as truncation.
    assert_eq!(value["coverage"]["truncated_items"], 2);
    assert_eq!(value["terminal"]["code"], "partial");
}
