//! Lossless bounded DTOs for an owned real-provider test child.
//!
//! Values originate in the actual provider API. No field is inferred from a
//! host scope, implementation name or expected scenario result. The process
//! owner must bound its receive wait by the ORIGINAL caller control and forward
//! live cancellation to the token passed to restore, including while a child
//! call is executing. A serialized cancellation snapshot alone is insufficient.

use super::{array, err, optional_string, parse_scope, string};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tracedecay_memory_provider_api::contract::{CommittedEffectState, TerminalCode};
use tracedecay_memory_provider_api::{
    CancellationToken, CanonicalPayload, CommittedEffectEvidence, CommittedEffectEvidenceParts,
    FallbackDirective, HandshakeRequest, HandshakeRequestParts, HandshakeResponse,
    OperationControl, OwnedOpaqueExtension, OwnedProviderId, OwnedVersionedId,
    PayloadSanitizationReceipt, PinnedFallbackPolicy, ProviderCall, ProviderCallParts,
    ProviderDescriptor, ProviderLimits, ProviderOperation, ProviderReply, TerminalRecord,
};

/// Finite JSON frame ceiling, including byte-array encoding expansion.
pub const MAX_TRANSPORT_JSON_BYTES: usize = 192 * 1024 * 1024;

macro_rules! dto {
    ($name:ident, $doc:literal) => {
        #[doc = $doc]
        #[derive(Clone, Debug, Serialize, Deserialize)]
        #[serde(transparent)]
        pub struct $name(Value);
        impl $name {
            /// Encodes one bounded owned-child frame without changing its data.
            pub fn encode(&self) -> Result<Vec<u8>, String> {
                encode(&self.0)
            }
            /// Decodes a bounded child frame; restore checks all required types.
            pub fn decode(bytes: &[u8]) -> Result<Self, String> {
                decode(bytes).map(Self)
            }
        }
    };
}
dto!(
    DescriptorDto,
    "Actual provider descriptor, including its full capability and limit set."
);
dto!(
    HandshakeRequestDto,
    "Original handshake request with its exact nonce and finite live control snapshot."
);
dto!(
    HandshakeResponseDto,
    "Actual provider handshake response, including terminal, receipt and accepted scope."
);
dto!(
    ProviderCallDto,
    "Actual call bytes and identities, opaque extensions and original sanitization receipt."
);
dto!(
    ProviderReplyDto,
    "Actual provider reply bytes and complete typed terminal/effect/fallback evidence."
);

impl DescriptorDto {
    /// Captures the actual provider descriptor.
    pub fn capture(value: &ProviderDescriptor) -> Result<Self, String> {
        checked(descriptor(value)).map(Self)
    }
    /// Restores every descriptor field from this frame alone.
    pub fn restore(self) -> Result<ProviderDescriptor, String> {
        checked(self.0).and_then(|value| read_descriptor(&value))
    }
}
impl HandshakeRequestDto {
    /// Captures remaining time now, never the original unspent duration.
    pub fn capture(value: &HandshakeRequest) -> Result<Self, String> {
        checked(json!({"provider_id": value.provider_id.as_str(), "registration_revision": value.registration_revision,
            "exact_scope": crate::compatibility::scope_json(&value.exact_scope), "request_id": value.request_id,
            "required_capabilities": capabilities(&value.required_capabilities), "host_limits": limits(value.host_limits),
            "control": control(&value.control)?, "challenge_nonce": value.challenge_nonce})).map(Self)
    }
    /// Restores with a live caller-cancellation bridge. `transport_elapsed` must
    /// include known queue/transit delay; wall transit is also subtracted. The
    /// original parent control must still bound the transport's entire wait.
    pub fn restore(
        self,
        cancellation: CancellationToken,
        transport_elapsed: Duration,
    ) -> Result<HandshakeRequest, String> {
        let v = checked(self.0)?;
        let nonce = bytes(&v["challenge_nonce"])?;
        HandshakeRequest::new(HandshakeRequestParts {
            provider_id: provider(&v["provider_id"])?,
            registration_revision: number(&v["registration_revision"])?,
            exact_scope: parse_scope(&v["exact_scope"])?,
            request_id: string(&v["request_id"])?.into(),
            required_capabilities: read_capabilities(&v["required_capabilities"])?,
            host_limits: read_limits(&v["host_limits"])?,
            control: read_control(&v["control"], cancellation, transport_elapsed)?,
            challenge_nonce: nonce
                .try_into()
                .map_err(|_| "child handshake nonce length differs")?,
        })
        .map_err(err)
    }
}
impl HandshakeResponseDto {
    /// Captures the real response, including failure responses without readiness.
    pub fn capture(value: &HandshakeResponse) -> Result<Self, String> {
        checked(json!({"terminal": terminal(&value.terminal), "descriptor": value.descriptor.as_ref().map(descriptor),
            "provider_instance_id": value.provider_instance_id, "state_namespace": value.state_namespace,
            "accepted_scope": value.accepted_scope.as_ref().map(crate::compatibility::scope_json),
            "effective_limits": value.effective_limits.map(limits), "ready_receipt_sha256": value.ready_receipt_sha256,
            "warnings": value.warnings})).map(Self)
    }
    /// Restores only the actual transmitted provider fields.
    pub fn restore(self) -> Result<HandshakeResponse, String> {
        let v = checked(self.0)?;
        Ok(HandshakeResponse {
            terminal: read_terminal(&v["terminal"])?,
            descriptor: optional(&v["descriptor"], read_descriptor)?,
            provider_instance_id: optional_string(&v["provider_instance_id"])?,
            state_namespace: optional_string(&v["state_namespace"])?,
            accepted_scope: optional(&v["accepted_scope"], parse_scope)?,
            effective_limits: optional(&v["effective_limits"], read_limits)?,
            ready_receipt_sha256: optional_string(&v["ready_receipt_sha256"])?,
            warnings: strings(&v["warnings"])?,
        })
    }
}
impl ProviderCallDto {
    /// Copies original canonical bytes and the actual receipt; it never mints a
    /// replacement receipt or rebuilds a payload from projected host context.
    pub fn capture(value: &ProviderCall) -> Result<Self, String> {
        checked(json!({"operation": value.operation.as_wire(), "provider_id": value.provider_id.as_str(),
            "registration_revision": value.registration_revision, "ready_receipt_sha256": value.ready_receipt_sha256,
            "exact_scope": crate::compatibility::scope_json(&value.exact_scope), "request_id": value.request_id,
            "operation_id": value.operation_id, "expected_state_generation": value.expected_state_generation,
            "idempotency_key": value.idempotency_key, "control": control(&value.control)?, "payload": payload(&value.payload),
            "required_capabilities": capabilities(&value.required_capabilities), "extensions": extensions(&value.extensions),
            "sanitization_receipt": value.sanitization().map(PayloadSanitizationReceipt::to_json)})).map(Self)
    }
    /// Recreates the original call with a live cancellation bridge and a reduced
    /// budget. The owner must keep polling the original control during the RPC.
    pub fn restore(
        self,
        cancellation: CancellationToken,
        transport_elapsed: Duration,
    ) -> Result<ProviderCall, String> {
        let v = checked(self.0)?;
        let mut call = ProviderCall::new(ProviderCallParts {
            operation: operation(&v["operation"])?,
            provider_id: provider(&v["provider_id"])?,
            registration_revision: number(&v["registration_revision"])?,
            ready_receipt_sha256: string(&v["ready_receipt_sha256"])?.into(),
            exact_scope: parse_scope(&v["exact_scope"])?,
            request_id: string(&v["request_id"])?.into(),
            operation_id: string(&v["operation_id"])?.into(),
            expected_state_generation: number(&v["expected_state_generation"])?,
            idempotency_key: optional_string(&v["idempotency_key"])?,
            control: read_control(&v["control"], cancellation, transport_elapsed)?,
            payload: read_payload(&v["payload"])?,
            required_capabilities: read_capabilities(&v["required_capabilities"])?,
            extensions: read_extensions(&v["extensions"])?,
        })
        .map_err(err)?;
        if let Some(receipt) = optional_string(&v["sanitization_receipt"])? {
            call = call
                .with_sanitization(PayloadSanitizationReceipt::from_json(&receipt).map_err(err)?);
        }
        call.validate().map_err(err)?;
        Ok(call)
    }
}
impl ProviderReplyDto {
    /// Copies all real reply fields, preserving invalid payload digests too so
    /// the common runner, rather than the transport, assesses provider behavior.
    pub fn capture(value: &ProviderReply) -> Result<Self, String> {
        checked(json!({"terminal": terminal(&value.terminal), "payload": value.payload.as_ref().map(payload),
            "warnings": value.warnings, "extensions": extensions(&value.extensions), "state_generation": value.state_generation})).map(Self)
    }
    /// Restores the transmitted evidence without inferring success or content.
    pub fn restore(self) -> Result<ProviderReply, String> {
        let v = checked(self.0)?;
        Ok(ProviderReply {
            terminal: read_terminal(&v["terminal"])?,
            payload: optional(&v["payload"], read_payload)?,
            warnings: strings(&v["warnings"])?,
            extensions: read_extensions(&v["extensions"])?,
            state_generation: number(&v["state_generation"])?,
        })
    }
}

fn encode(value: &Value) -> Result<Vec<u8>, String> {
    let bytes = serde_json::to_vec(value).map_err(err)?;
    if bytes.len() > MAX_TRANSPORT_JSON_BYTES {
        return Err("child transport frame byte bound exceeded".into());
    }
    Ok(bytes)
}
fn checked(value: Value) -> Result<Value, String> {
    encode(&value)?;
    if !value.is_object() {
        return Err("child transport object required".into());
    }
    Ok(value)
}
fn decode(bytes: &[u8]) -> Result<Value, String> {
    if bytes.len() > MAX_TRANSPORT_JSON_BYTES {
        return Err("child transport frame byte bound exceeded".into());
    }
    checked(serde_json::from_slice(bytes).map_err(err)?)
}
fn optional<T>(
    value: &Value,
    f: impl FnOnce(&Value) -> Result<T, String>,
) -> Result<Option<T>, String> {
    if value.is_null() {
        Ok(None)
    } else {
        f(value).map(Some)
    }
}
fn number(value: &Value) -> Result<u64, String> {
    value
        .as_u64()
        .ok_or_else(|| "child unsigned integer required".into())
}
fn signed(value: &Value) -> Result<i64, String> {
    value
        .as_i64()
        .ok_or_else(|| "child signed integer required".into())
}
fn boolean(value: &Value) -> Result<bool, String> {
    value
        .as_bool()
        .ok_or_else(|| "child boolean required".into())
}
fn strings(value: &Value) -> Result<Vec<String>, String> {
    array(value)?
        .iter()
        .map(|v| string(v).map(str::to_owned))
        .collect()
}
fn bytes(value: &Value) -> Result<Vec<u8>, String> {
    array(value)?
        .iter()
        .map(|v| u8::try_from(number(v)?).map_err(err))
        .collect()
}
fn provider(value: &Value) -> Result<OwnedProviderId, String> {
    OwnedProviderId::new(string(value)?).map_err(err)
}
fn version(value: &Value) -> Result<OwnedVersionedId, String> {
    OwnedVersionedId::new(string(value)?).map_err(err)
}
fn operation(value: &Value) -> Result<ProviderOperation, String> {
    [
        ProviderOperation::Handshake,
        ProviderOperation::Health,
        ProviderOperation::Observe,
        ProviderOperation::Recall,
        ProviderOperation::Feedback,
        ProviderOperation::Maintenance,
        ProviderOperation::Inspection,
        ProviderOperation::Correction,
        ProviderOperation::DeleteBySource,
        ProviderOperation::SnapshotExport,
        ProviderOperation::SnapshotRestore,
        ProviderOperation::Replay,
    ]
    .into_iter()
    .find(|operation| Some(operation.as_wire()) == value.as_str())
    .ok_or_else(|| "child operation invalid".into())
}
fn capabilities(value: &std::collections::BTreeSet<OwnedVersionedId>) -> Vec<&str> {
    value.iter().map(OwnedVersionedId::as_str).collect()
}
fn read_capabilities(value: &Value) -> Result<Vec<OwnedVersionedId>, String> {
    array(value)?.iter().map(version).collect()
}
fn payload(value: &CanonicalPayload) -> Value {
    json!({"contract_id": value.contract_id.as_str(), "bytes": value.bytes, "sha256": value.sha256})
}
fn read_payload(value: &Value) -> Result<CanonicalPayload, String> {
    Ok(CanonicalPayload {
        contract_id: version(&value["contract_id"])?,
        bytes: bytes(&value["bytes"])?,
        sha256: string(&value["sha256"])?.into(),
    })
}
fn extensions(values: &[OwnedOpaqueExtension]) -> Vec<Value> {
    values.iter().map(|v| json!({"extension_id": v.extension_id.as_str(), "extension_version": v.extension_version, "required": v.required,
        "payload_sha256": v.payload_sha256, "canonical_payload": v.canonical_payload})).collect()
}
fn read_extensions(value: &Value) -> Result<Vec<OwnedOpaqueExtension>, String> {
    array(value)?
        .iter()
        .map(|v| {
            Ok(OwnedOpaqueExtension {
                extension_id: version(&v["extension_id"])?,
                extension_version: u32::try_from(number(&v["extension_version"])?).map_err(err)?,
                required: boolean(&v["required"])?,
                payload_sha256: string(&v["payload_sha256"])?.into(),
                canonical_payload: bytes(&v["canonical_payload"])?,
            })
        })
        .collect()
}
fn limits(v: ProviderLimits) -> Value {
    json!({"request_bytes":v.request_bytes,"response_bytes":v.response_bytes,"observation_batch_items":v.observation_batch_items,
    "recall_candidates":v.recall_candidates,"concurrent_operations":v.concurrent_operations,"operation_millis":v.operation_millis,"snapshot_bytes":v.snapshot_bytes,"inspection_items":v.inspection_items})
}
fn read_limits(v: &Value) -> Result<ProviderLimits, String> {
    Ok(ProviderLimits {
        request_bytes: number(&v["request_bytes"])?,
        response_bytes: number(&v["response_bytes"])?,
        observation_batch_items: number(&v["observation_batch_items"])?,
        recall_candidates: number(&v["recall_candidates"])?,
        concurrent_operations: number(&v["concurrent_operations"])?,
        operation_millis: number(&v["operation_millis"])?,
        snapshot_bytes: number(&v["snapshot_bytes"])?,
        inspection_items: number(&v["inspection_items"])?,
    })
}
fn descriptor(v: &ProviderDescriptor) -> Value {
    json!({"provider_id":v.provider_id.as_str(),"implementation_identity_sha256":v.implementation_identity_sha256,
    "state_schema_version":v.state_schema_version,"state_generation":v.state_generation,"protocol_major":v.protocol_major,"protocol_minor":v.protocol_minor,
    "capabilities":capabilities(&v.capabilities),"limits":limits(v.limits)})
}
fn read_descriptor(v: &Value) -> Result<ProviderDescriptor, String> {
    Ok(ProviderDescriptor {
        provider_id: provider(&v["provider_id"])?,
        implementation_identity_sha256: string(&v["implementation_identity_sha256"])?.into(),
        state_schema_version: string(&v["state_schema_version"])?.into(),
        state_generation: number(&v["state_generation"])?,
        protocol_major: u16::try_from(number(&v["protocol_major"])?).map_err(err)?,
        protocol_minor: u16::try_from(number(&v["protocol_minor"])?).map_err(err)?,
        capabilities: read_capabilities(&v["capabilities"])?.into_iter().collect(),
        limits: read_limits(&v["limits"])?,
    })
}
fn now_micros() -> Result<i64, String> {
    i64::try_from(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(err)?
            .as_micros(),
    )
    .map_err(err)
}
fn control(v: &OperationControl) -> Result<Value, String> {
    // Capture wall time before the live snapshot. Any clock sampling/coding delay
    // is charged against transit, and a backward wall jump expires the child.
    let captured_at = now_micros()?;
    let remaining = v.snapshot().map(|s| s.remaining_millis).unwrap_or(0);
    Ok(
        json!({"deadline_utc_micros":v.deadline_utc_micros(),"remaining_millis":remaining,
        "captured_at_utc_micros":captured_at,"cancelled":v.cancellation().is_cancelled()}),
    )
}
fn read_control(
    v: &Value,
    cancellation: CancellationToken,
    transport_elapsed: Duration,
) -> Result<OperationControl, String> {
    if boolean(&v["cancelled"])? {
        cancellation.cancel();
    }
    let captured = signed(&v["captured_at_utc_micros"])?;
    let now = now_micros()?;
    let wall_elapsed = now
        .checked_sub(captured)
        .filter(|delta| *delta >= 0)
        .map(|delta| Duration::from_micros(delta as u64));
    let elapsed = wall_elapsed.map(|duration| duration.max(transport_elapsed));
    let spent = elapsed
        .map(|duration| {
            u64::try_from(duration.as_nanos().saturating_add(999_999) / 1_000_000)
                .unwrap_or(u64::MAX)
        })
        .unwrap_or(u64::MAX);
    Ok(OperationControl::new(
        signed(&v["deadline_utc_micros"])?,
        number(&v["remaining_millis"])?.saturating_sub(spent),
        cancellation,
    ))
}
fn terminal(v: &TerminalRecord) -> Value {
    let e = v.committed_effect();
    let f = v.fallback();
    json!({"operation":v.operation().as_wire(),"provider_id":v.provider_id().as_str(),"terminal_code":v.terminal_code().as_wire(),
        "operation_id":v.operation_id(),"exact_scope_sha256":v.exact_scope_sha256(),"diagnostic_id":v.diagnostic_id(),
        "effect":{"state":e.state().as_wire(),"committed_boundary":e.committed_boundary(),"state_generation_before":e.state_generation_before(),
            "state_generation_after":e.state_generation_after(),"committed_item_refs":e.committed_item_refs(),"uncommitted_item_refs":e.uncommitted_item_refs(),
            "provider_receipt_sha256":e.provider_receipt_sha256(),"reconciliation_action":e.reconciliation_action(),"verification_sha256":e.verification_sha256(),
            "duplicate_of_idempotency_key":e.duplicate_of_idempotency_key(),"duplicate_of_operation_id":e.duplicate_of_operation_id()},
        "fallback":{"eligibility":f.eligibility().as_wire(),"source_provider_id":f.source_provider_id().map(OwnedProviderId::as_str),
            "policy":f.policy().map(|p|json!({"policy_id":p.policy_id(),"policy_revision":p.policy_revision(),"target_provider_id":p.target_provider_id().as_str()})),"reason":f.reason()}})
}
fn read_terminal(v: &Value) -> Result<TerminalRecord, String> {
    let e = &v["effect"];
    let f = &v["fallback"];
    let effect = CommittedEffectEvidence::from_parts(CommittedEffectEvidenceParts {
        state: CommittedEffectState::from_wire(string(&e["state"])?)
            .ok_or("child effect invalid")?,
        committed_boundary: optional_string(&e["committed_boundary"])?,
        state_generation_before: optional(&e["state_generation_before"], number)?,
        state_generation_after: optional(&e["state_generation_after"], number)?,
        committed_item_refs: strings(&e["committed_item_refs"])?,
        uncommitted_item_refs: strings(&e["uncommitted_item_refs"])?,
        provider_receipt_sha256: optional_string(&e["provider_receipt_sha256"])?,
        reconciliation_action: optional_string(&e["reconciliation_action"])?,
        verification_sha256: optional_string(&e["verification_sha256"])?,
        duplicate_of_idempotency_key: optional_string(&e["duplicate_of_idempotency_key"])?,
        duplicate_of_operation_id: optional_string(&e["duplicate_of_operation_id"])?,
    })
    .map_err(err)?;
    let fallback = match string(&f["eligibility"])? {
        "forbidden"
            if f["source_provider_id"].is_null()
                && f["policy"].is_null()
                && f["reason"].is_null() =>
        {
            FallbackDirective::forbidden()
        }
        "explicit_policy_only" => FallbackDirective::explicit_policy_only(
            &provider(&f["source_provider_id"])?,
            PinnedFallbackPolicy::new(
                string(&f["policy"]["policy_id"])?,
                number(&f["policy"]["policy_revision"])?,
                provider(&f["policy"]["target_provider_id"])?,
            )
            .map_err(err)?,
            string(&f["reason"])?,
        )
        .map_err(err)?,
        _ => return Err("child fallback fields invalid".into()),
    };
    TerminalRecord::new(
        operation(&v["operation"])?,
        provider(&v["provider_id"])?,
        TerminalCode::from_wire(string(&v["terminal_code"])?).ok_or("child terminal invalid")?,
        effect,
        fallback,
        string(&v["operation_id"])?,
        string(&v["exact_scope_sha256"])?,
        optional_string(&v["diagnostic_id"])?,
    )
    .map_err(err)
}
