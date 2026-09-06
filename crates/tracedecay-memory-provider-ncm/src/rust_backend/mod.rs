//! Supervised Rust worker implementation of the topology-neutral NCM surface.

use std::collections::BTreeSet;
use std::error::Error;
use std::fmt;
use std::path::PathBuf;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use serde_json::{Map, Value, json};
use sha2::{Digest, Sha256};
pub use tracedecay_memory_ncm_runtime::client::WorkerOptions;
use tracedecay_memory_ncm_runtime::client::{ClientError, WorkerClient};
use tracedecay_memory_ncm_runtime::engine::{Outcome, RejectReason};
pub use tracedecay_memory_ncm_runtime::ports::StateRoot;
use tracedecay_memory_ncm_runtime::wire::{Operation, Reply as WorkerReply, Request};
use tracedecay_memory_provider_api::contract::TerminalCode;
use tracedecay_memory_provider_api::{
    CanonicalPayload, CommittedEffectEvidence, FallbackDirective, OwnedProviderId,
    OwnedVersionedId, ProviderDescriptor, ProviderLimits, ProviderOperation, ProviderReply,
    TerminalRecord,
};

use crate::{
    NCM_PROVIDER_ID, NcmCognitiveSurface, NcmNamespace, NcmSurfaceCall, NcmSurfaceHandshakeRequest,
    NcmSurfaceHandshakeResponse,
};

const ALGORITHM_PROFILE: &str = "ncm-biomem-rs.v1";
const PROVISIONAL_CONFIG_SHA256: &str =
    "0000000000000000000000000000000000000000000000000000000000000000";
const PREFLIGHT_NAMESPACE: &str =
    "0000000000000000000000000000000000000000000000000000000000000000";
const RECEIPT_DOMAIN: &[u8] = b"tracedecay.ncm.rust-worker-receipt.v1\0";
const READY_DOMAIN: &[u8] = b"tracedecay.ncm.rust-worker-ready.v1\0";
const IMPLEMENTATION_DOMAIN: &[u8] = b"tracedecay.ncm.rust-worker-implementation.v1\0";
const DEFAULT_PREFLIGHT_MILLIS: u64 = 5_000;

/// Configuration for the supervised Rust NCM surface.
#[derive(Clone, Debug)]
pub struct RustNcmConfig {
    /// Absolute worker executable path.
    pub worker_binary: PathBuf,
    /// Admitted absolute state root.
    pub state_root: StateRoot,
    /// Worker launch, restart, and test-double controls.
    pub worker_options: WorkerOptions,
}

/// Failure while constructing the Rust-backed surface.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RustNcmError {
    /// The worker owner or executable could not be started.
    WorkerSpawn(String),
    /// The supplied state root did not remain an admitted absolute root.
    StateRoot(String),
    /// A successful preflight returned malformed or contradictory identity.
    HandshakeIdentity(String),
}

impl fmt::Display for RustNcmError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::WorkerSpawn(detail) => write!(formatter, "NCM worker spawn failed: {detail}"),
            Self::StateRoot(detail) => write!(formatter, "NCM state root rejected: {detail}"),
            Self::HandshakeIdentity(detail) => {
                write!(formatter, "NCM handshake identity rejected: {detail}")
            }
        }
    }
}

impl Error for RustNcmError {}

#[derive(Clone, Debug, Eq, PartialEq)]
struct RuntimeIdentity {
    config_sha256: String,
    projection_sha256: String,
    encoder_model: String,
    encoder_artifact_sha256: String,
    epoch: u64,
}

struct SurfaceState {
    descriptor: ProviderDescriptor,
    identity: Option<RuntimeIdentity>,
}

/// Real NCM surface backed by one supervised `tracedecay-ncm-worker` process.
pub struct RustNcmSurface {
    client: WorkerClient,
    state: Mutex<SurfaceState>,
    fallback_descriptor: ProviderDescriptor,
    next_request_id: AtomicU64,
}

impl RustNcmSurface {
    /// Constructs a worker client and performs a read-only identity preflight.
    ///
    /// An unavailable production encoder is not a construction error: the
    /// surface remains constructible and handshakes return a typed not-ready
    /// terminal until a new surface is created after the model is installed.
    pub fn new(config: RustNcmConfig) -> Result<Self, RustNcmError> {
        let checked_root = StateRoot::new(config.state_root.path().to_path_buf())
            .map_err(RustNcmError::StateRoot)?;
        if !config.worker_binary.is_absolute() {
            return Err(RustNcmError::WorkerSpawn(
                "worker binary path must be absolute".to_owned(),
            ));
        }
        let client = WorkerClient::spawn(
            &config.worker_binary,
            checked_root.path(),
            config.worker_options,
        )
        .map_err(|error| RustNcmError::WorkerSpawn(error.to_string()))?;
        let fallback_descriptor =
            descriptor_for(PROVISIONAL_CONFIG_SHA256, "not-ready", "not-ready", 0)?;
        let preflight = Request::new(
            1,
            DEFAULT_PREFLIGHT_MILLIS,
            Operation::Handshake,
            PREFLIGHT_NAMESPACE,
            json!({"algorithm_profile": ALGORITHM_PROFILE}),
        );
        let (descriptor, identity) =
            match client.call(preflight, Duration::from_millis(DEFAULT_PREFLIGHT_MILLIS)) {
                Ok(reply) if reply.outcome == Outcome::Success => {
                    let identity = parse_runtime_identity(&reply)?;
                    let descriptor = descriptor_from_identity(&identity, reply.state_generation)?;
                    (descriptor, Some(identity))
                }
                Ok(reply) if matches!(reply.outcome, Outcome::Unavailable(_)) => {
                    (fallback_descriptor.clone(), None)
                }
                Ok(reply) => {
                    return Err(RustNcmError::HandshakeIdentity(format!(
                        "preflight outcome {:?}",
                        reply.outcome
                    )));
                }
                Err(ClientError::Spawn(detail)) => return Err(RustNcmError::WorkerSpawn(detail)),
                Err(ClientError::RestartExhausted | ClientError::WorkerExited) => {
                    return Err(RustNcmError::WorkerSpawn(
                        "worker exited during identity preflight".to_owned(),
                    ));
                }
                Err(error) => {
                    return Err(RustNcmError::HandshakeIdentity(error.to_string()));
                }
            };
        Ok(Self {
            client,
            state: Mutex::new(SurfaceState {
                descriptor,
                identity,
            }),
            fallback_descriptor,
            next_request_id: AtomicU64::new(2),
        })
    }

    /// Returns the current worker process identifier, when the lazy worker is alive.
    #[must_use]
    pub fn worker_pid(&self) -> Option<u32> {
        self.client.pid()
    }

    fn request_id(&self) -> u64 {
        self.next_request_id.fetch_add(1, Ordering::Relaxed)
    }

    fn descriptor_snapshot(&self) -> ProviderDescriptor {
        self.state
            .lock()
            .map(|state| state.descriptor.clone())
            .unwrap_or_else(|_| self.fallback_descriptor.clone())
    }

    fn update_generation(&self, generation: u64) {
        if let Ok(mut state) = self.state.lock() {
            state.descriptor.state_generation = generation;
        }
    }

    fn install_identity(
        &self,
        identity: RuntimeIdentity,
        generation: u64,
    ) -> Result<(), RustNcmError> {
        let descriptor = descriptor_from_identity(&identity, generation)?;
        let mut state = self.state.lock().map_err(|_| {
            RustNcmError::HandshakeIdentity("surface state lock poisoned".to_owned())
        })?;
        state.descriptor = descriptor;
        state.identity = Some(identity);
        Ok(())
    }

    fn worker_call(
        &self,
        operation: Operation,
        namespace: &NcmNamespace,
        payload: Value,
        millis: u64,
    ) -> Result<WorkerReply, ClientError> {
        let request = Request::new(
            self.request_id(),
            millis,
            operation,
            namespace.as_str(),
            payload,
        );
        self.client.call(request, Duration::from_millis(millis))
    }
}

impl NcmCognitiveSurface for RustNcmSurface {
    fn descriptor(&self) -> ProviderDescriptor {
        self.descriptor_snapshot()
    }

    fn handshake(&self, request: &NcmSurfaceHandshakeRequest) -> NcmSurfaceHandshakeResponse {
        let control = match request.control.snapshot() {
            Ok(control) => control,
            Err(code) => return handshake_failure(request, code, "ncm.rust.control_terminal"),
        };
        let expected_model = self.state.lock().ok().and_then(|state| {
            state
                .identity
                .as_ref()
                .map(|identity| identity.encoder_model.clone())
        });
        let mut payload = json!({
            "protocol_version": 1,
            "algorithm_profile": ALGORITHM_PROFILE,
        });
        if let Some(model) = expected_model
            && let Some(object) = payload.as_object_mut()
        {
            object.insert("model".to_owned(), Value::String(model));
        }
        let reply = match self.worker_call(
            Operation::Handshake,
            &request.namespace,
            payload,
            control.remaining_millis,
        ) {
            Ok(reply) => reply,
            Err(error) => {
                return handshake_failure(
                    request,
                    client_terminal_code(&error),
                    client_diagnostic(&error),
                );
            }
        };
        if reply.outcome != Outcome::Success {
            return handshake_failure(
                request,
                outcome_terminal_code(&reply.outcome),
                outcome_diagnostic(&reply.outcome),
            );
        }
        let identity = match parse_runtime_identity(&reply) {
            Ok(identity) => identity,
            Err(error) => {
                return handshake_failure(
                    request,
                    TerminalCode::StateIncompatible,
                    error_diagnostic(&error),
                );
            }
        };
        let candidate = match descriptor_from_identity(&identity, reply.state_generation) {
            Ok(descriptor) => descriptor,
            Err(error) => {
                return handshake_failure(
                    request,
                    TerminalCode::StateIncompatible,
                    error_diagnostic(&error),
                );
            }
        };
        let current = self.descriptor_snapshot();
        if !same_immutable_descriptor(&current, &candidate)
            || current.state_generation != candidate.state_generation
        {
            let _ = self.install_identity(identity, reply.state_generation);
            return handshake_failure(
                request,
                TerminalCode::StaleIdentity,
                "ncm.rust.handshake_identity_refresh_required",
            );
        }
        if self
            .install_identity(identity.clone(), reply.state_generation)
            .is_err()
        {
            return handshake_failure(
                request,
                TerminalCode::ProviderUnavailable,
                "ncm.rust.surface_state_unavailable",
            );
        }
        let descriptor = self.descriptor_snapshot();
        let ready_receipt = ready_receipt(request.namespace.as_str(), &identity);
        let instance_id = implementation_version(&identity.config_sha256);
        let challenge =
            request.expected_challenge_response_sha256(&descriptor, &instance_id, &ready_receipt);
        let terminal = surface_terminal(
            ProviderOperation::Handshake,
            &request.request_id,
            request.namespace.as_str(),
            TerminalCode::Success,
            // A handshake commits nothing, but its evidence anchors to the
            // descriptor generation the host is about to bind readiness to.
            CommittedEffectEvidence::none(Some(descriptor.state_generation)),
            None,
        );
        NcmSurfaceHandshakeResponse {
            terminal,
            descriptor: Some(descriptor.clone()),
            provider_instance_id: Some(instance_id),
            namespace: Some(request.namespace.clone()),
            effective_limits: Some(request.host_limits.minimum(descriptor.limits)),
            ready_receipt_sha256: Some(ready_receipt),
            challenge_response_sha256: Some(challenge),
            warnings: Vec::new(),
        }
    }

    fn invoke(&self, call: &NcmSurfaceCall) -> ProviderReply {
        let control = match call.control.snapshot() {
            Ok(control) => control,
            Err(code) => return pre_dispatch_reply(call, code, "ncm.rust.control_terminal"),
        };
        let payload = match translate_payload(call) {
            Ok(payload) => payload,
            Err(diagnostic) => {
                return pre_dispatch_reply(call, TerminalCode::InvalidRequest, diagnostic);
            }
        };
        let operation = wire_operation(call.operation);
        let reply = match self.worker_call(
            operation,
            &call.namespace,
            payload,
            control.remaining_millis,
        ) {
            Ok(reply) => reply,
            Err(error) => return client_error_reply(call, &error),
        };
        if call.operation.mutates_provider_state()
            && reply.state_generation >= call.expected_state_generation
        {
            self.update_generation(reply.state_generation);
        }
        worker_reply(call, reply)
    }
}

fn descriptor_for(
    config_sha256: &str,
    encoder_model: &str,
    encoder_artifact_sha256: &str,
    generation: u64,
) -> Result<ProviderDescriptor, RustNcmError> {
    let version = implementation_version(config_sha256);
    let mut implementation = Sha256::new();
    implementation.update(IMPLEMENTATION_DOMAIN);
    digest_field(&mut implementation, version.as_bytes());
    digest_field(&mut implementation, encoder_model.as_bytes());
    digest_field(&mut implementation, encoder_artifact_sha256.as_bytes());
    let identity_sha256 = hex_digest(&implementation.finalize());
    let provider_id = OwnedProviderId::new(NCM_PROVIDER_ID)
        .map_err(|error| RustNcmError::HandshakeIdentity(error.to_string()))?;
    let capabilities = capability_ids()?;
    ProviderDescriptor::new(
        provider_id,
        identity_sha256,
        version,
        generation,
        capabilities,
        provider_limits(),
    )
    .map_err(|error| RustNcmError::HandshakeIdentity(error.to_string()))
}

fn descriptor_from_identity(
    identity: &RuntimeIdentity,
    generation: u64,
) -> Result<ProviderDescriptor, RustNcmError> {
    descriptor_for(
        &identity.config_sha256,
        &identity.encoder_model,
        &identity.encoder_artifact_sha256,
        generation,
    )
}

fn implementation_version(config_sha256: &str) -> String {
    let prefix = config_sha256.get(..12).unwrap_or(config_sha256);
    format!("{ALGORITHM_PROFILE}+{prefix}")
}

fn capability_ids() -> Result<BTreeSet<OwnedVersionedId>, RustNcmError> {
    [
        "provider.health.v1",
        "observation.accept.v1",
        "recall.query.v1",
        "feedback.record.v1",
        "maintenance.run.v1",
        "inspection.read.v1",
        "correction.apply.v1",
        "deletion.by_source.v1",
        "snapshot.export.v1",
        "snapshot.restore.v1",
    ]
    .into_iter()
    .map(|value| {
        OwnedVersionedId::new(value)
            .map_err(|error| RustNcmError::HandshakeIdentity(error.to_string()))
    })
    .collect()
}

const fn provider_limits() -> ProviderLimits {
    ProviderLimits {
        request_bytes: 256 * 1024,
        response_bytes: 1024 * 1024,
        observation_batch_items: 16,
        recall_candidates: 16,
        concurrent_operations: 1,
        operation_millis: 30_000,
        snapshot_bytes: 256 * 1024 * 1024,
        inspection_items: 1_000,
    }
}

fn same_immutable_descriptor(left: &ProviderDescriptor, right: &ProviderDescriptor) -> bool {
    left.provider_id == right.provider_id
        && left.implementation_identity_sha256 == right.implementation_identity_sha256
        && left.state_schema_version == right.state_schema_version
        && left.protocol_major == right.protocol_major
        && left.protocol_minor == right.protocol_minor
        && left.capabilities == right.capabilities
        && left.limits == right.limits
}

fn parse_runtime_identity(reply: &WorkerReply) -> Result<RuntimeIdentity, RustNcmError> {
    let payload = reply.payload.as_ref().ok_or_else(|| {
        RustNcmError::HandshakeIdentity("successful handshake omitted payload".to_owned())
    })?;
    let profile = required_str(payload.pointer("/algorithm/profile"), "algorithm.profile")?;
    if profile != ALGORITHM_PROFILE {
        return Err(RustNcmError::HandshakeIdentity(format!(
            "algorithm profile {profile}"
        )));
    }
    let config_sha256 = required_sha256(
        payload.pointer("/algorithm/config_sha256"),
        "algorithm.config_sha256",
    )?;
    let projection_sha256 = required_sha256(payload.get("projection_sha256"), "projection_sha256")?;
    let encoder_model = required_str(payload.pointer("/encoder/model"), "encoder.model")?;
    let encoder_artifact_sha256 = required_str(
        payload.pointer("/encoder/artifact_sha256"),
        "encoder.artifact_sha256",
    )?;
    let epoch = payload
        .get("epoch")
        .and_then(Value::as_u64)
        .ok_or_else(|| RustNcmError::HandshakeIdentity("missing epoch".to_owned()))?;
    Ok(RuntimeIdentity {
        config_sha256,
        projection_sha256,
        encoder_model,
        encoder_artifact_sha256,
        epoch,
    })
}

fn required_str(value: Option<&Value>, field: &str) -> Result<String, RustNcmError> {
    value
        .and_then(Value::as_str)
        .filter(|value| !value.is_empty())
        .map(str::to_owned)
        .ok_or_else(|| RustNcmError::HandshakeIdentity(format!("missing {field}")))
}

fn required_sha256(value: Option<&Value>, field: &str) -> Result<String, RustNcmError> {
    let value = required_str(value, field)?;
    if valid_sha256(&value) {
        Ok(value)
    } else {
        Err(RustNcmError::HandshakeIdentity(format!("invalid {field}")))
    }
}

fn valid_sha256(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

fn ready_receipt(namespace: &str, identity: &RuntimeIdentity) -> String {
    let mut digest = Sha256::new();
    digest.update(READY_DOMAIN);
    digest_field(&mut digest, namespace.as_bytes());
    digest_field(&mut digest, ALGORITHM_PROFILE.as_bytes());
    digest_field(&mut digest, identity.config_sha256.as_bytes());
    digest_field(&mut digest, identity.projection_sha256.as_bytes());
    digest_field(&mut digest, identity.encoder_model.as_bytes());
    digest_field(&mut digest, identity.encoder_artifact_sha256.as_bytes());
    digest.update(identity.epoch.to_be_bytes());
    hex_digest(&digest.finalize())
}

fn handshake_failure(
    request: &NcmSurfaceHandshakeRequest,
    code: TerminalCode,
    diagnostic: &'static str,
) -> NcmSurfaceHandshakeResponse {
    NcmSurfaceHandshakeResponse {
        terminal: surface_terminal(
            ProviderOperation::Handshake,
            &request.request_id,
            request.namespace.as_str(),
            code,
            CommittedEffectEvidence::none(None),
            Some(diagnostic),
        ),
        descriptor: None,
        provider_instance_id: None,
        namespace: None,
        effective_limits: None,
        ready_receipt_sha256: None,
        challenge_response_sha256: None,
        warnings: Vec::new(),
    }
}

fn surface_terminal(
    operation: ProviderOperation,
    operation_id: &str,
    namespace: &str,
    code: TerminalCode,
    effect: CommittedEffectEvidence,
    diagnostic: Option<&str>,
) -> TerminalRecord {
    let provider = match OwnedProviderId::new(NCM_PROVIDER_ID) {
        Ok(provider) => provider,
        Err(_) => {
            return TerminalRecord::failure_before_dispatch(
                operation,
                OwnedProviderId::new("ncm").unwrap_or_else(|_| unreachable_provider_id()),
                TerminalCode::InternalFailure,
                operation_id,
                namespace,
                effect.state_generation_before(),
                "ncm.rust.provider_identity_invalid",
            );
        }
    };
    match TerminalRecord::new(
        operation,
        provider.clone(),
        code,
        effect,
        FallbackDirective::forbidden(),
        operation_id,
        namespace,
        diagnostic.map(str::to_owned),
    ) {
        Ok(terminal) => terminal,
        Err(_) => TerminalRecord::failure_before_dispatch(
            operation,
            provider,
            TerminalCode::InternalFailure,
            operation_id,
            namespace,
            None,
            "ncm.rust.terminal_construction_failed",
        ),
    }
}

fn unreachable_provider_id() -> OwnedProviderId {
    // This branch is unreachable because the same literal is validated during
    // descriptor construction. Keep the trait total without panicking.
    match OwnedProviderId::new("ncm") {
        Ok(provider) => provider,
        Err(_) => std::process::abort(),
    }
}

fn pre_dispatch_reply(
    call: &NcmSurfaceCall,
    code: TerminalCode,
    diagnostic: &'static str,
) -> ProviderReply {
    ProviderReply {
        terminal: surface_terminal(
            call.operation,
            &call.operation_id,
            call.namespace.as_str(),
            code,
            CommittedEffectEvidence::none(Some(call.expected_state_generation)),
            Some(diagnostic),
        ),
        payload: None,
        warnings: Vec::new(),
        extensions: Vec::new(),
        state_generation: call.expected_state_generation,
    }
}

fn client_error_reply(call: &NcmSurfaceCall, error: &ClientError) -> ProviderReply {
    if matches!(error, ClientError::EffectUnknown { .. }) && call.operation.mutates_provider_state()
    {
        let receipt = unknown_receipt(call, error);
        let action = format!("ncm.worker.reconcile-idempotency.v1:{}", &receipt[..16]);
        let effect = CommittedEffectEvidence::unknown(receipt, action).unwrap_or_else(|_| {
            CommittedEffectEvidence::unknown_from_reconciliation_digest([0; 32])
        });
        return ProviderReply {
            terminal: surface_terminal(
                call.operation,
                &call.operation_id,
                call.namespace.as_str(),
                TerminalCode::EffectUnknown,
                effect,
                Some("ncm.rust.worker_effect_unknown"),
            ),
            payload: None,
            warnings: Vec::new(),
            extensions: Vec::new(),
            state_generation: call.expected_state_generation,
        };
    }
    pre_dispatch_reply(call, client_terminal_code(error), client_diagnostic(error))
}

fn worker_reply(call: &NcmSurfaceCall, reply: WorkerReply) -> ProviderReply {
    let terminal_code = outcome_terminal_code(&reply.outcome);
    let success = matches!(reply.outcome, Outcome::Success | Outcome::Empty);
    let replayed = reply
        .payload
        .as_ref()
        .and_then(|payload| payload.get("replayed"))
        .and_then(Value::as_bool)
        .unwrap_or(false);
    let receipt = worker_receipt(call.operation, &reply);
    let effect = if call.operation.mutates_provider_state() {
        if reply.outcome == Outcome::Success && replayed {
            CommittedEffectEvidence::duplicate(
                reply.state_generation,
                call.idempotency_key.as_deref().unwrap_or("ncm-no-key"),
                format!("ncm.engine.operation.{}", &receipt[..16]),
                &receipt,
            )
            .unwrap_or_else(|_| {
                CommittedEffectEvidence::unknown_from_reconciliation_digest([0; 32])
            })
        } else if reply.outcome == Outcome::Success {
            CommittedEffectEvidence::committed(
                call.expected_state_generation,
                reply.state_generation,
                vec![format!("ncm.{}.committed", call.operation.as_wire())],
                &receipt,
                &receipt,
            )
            .unwrap_or_else(|_| {
                CommittedEffectEvidence::unknown_from_reconciliation_digest([0; 32])
            })
        } else if reply.outcome == Outcome::EffectUnknown {
            CommittedEffectEvidence::unknown(
                &receipt,
                format!("ncm.worker.reconcile-idempotency.v1:{}", &receipt[..16]),
            )
            .unwrap_or_else(|_| {
                CommittedEffectEvidence::unknown_from_reconciliation_digest([0; 32])
            })
        } else {
            CommittedEffectEvidence::none(Some(call.expected_state_generation))
        }
    } else {
        CommittedEffectEvidence::none(Some(call.expected_state_generation))
    };
    let payload = if success {
        reply
            .payload
            .as_ref()
            .and_then(|payload| canonical_response_payload(call, payload).ok())
    } else {
        None
    };
    let diagnostic = (!success).then(|| worker_diagnostic(&reply));
    ProviderReply {
        terminal: surface_terminal(
            call.operation,
            &call.operation_id,
            call.namespace.as_str(),
            terminal_code,
            effect,
            diagnostic,
        ),
        payload,
        warnings: Vec::new(),
        extensions: Vec::new(),
        state_generation: reply
            .state_generation
            .max(if call.operation.mutates_provider_state() {
                0
            } else {
                call.expected_state_generation
            }),
    }
}

fn canonical_response_payload(
    call: &NcmSurfaceCall,
    value: &Value,
) -> Result<CanonicalPayload, ()> {
    let bytes = serde_json::to_vec(value).map_err(|_| ())?;
    let sha256 = hex_digest(&Sha256::digest(&bytes));
    CanonicalPayload::new(call.payload.contract_id.clone(), bytes, sha256).map_err(|_| ())
}

fn wire_operation(operation: ProviderOperation) -> Operation {
    match operation {
        ProviderOperation::Handshake => Operation::Handshake,
        ProviderOperation::Health => Operation::Health,
        ProviderOperation::Observe => Operation::Observe,
        ProviderOperation::Recall => Operation::Recall,
        ProviderOperation::Feedback => Operation::Feedback,
        ProviderOperation::Maintenance => Operation::Maintenance,
        ProviderOperation::Inspection => Operation::Inspection,
        ProviderOperation::Correction => Operation::Correction,
        ProviderOperation::DeleteBySource => Operation::DeleteBySource,
        ProviderOperation::SnapshotExport => Operation::SnapshotExport,
        ProviderOperation::SnapshotRestore => Operation::SnapshotRestore,
        ProviderOperation::Replay => Operation::Replay,
    }
}

fn translate_payload(call: &NcmSurfaceCall) -> Result<Value, &'static str> {
    let value: Value =
        serde_json::from_slice(&call.payload.bytes).map_err(|_| "ncm.rust.payload_invalid_json")?;
    let object = value.as_object().ok_or("ncm.rust.payload_not_object")?;
    match call.operation {
        ProviderOperation::Handshake => Err("ncm.rust.handshake_wrong_port"),
        ProviderOperation::Health
        | ProviderOperation::Inspection
        | ProviderOperation::SnapshotExport => Ok(Value::Object(Map::new())),
        ProviderOperation::Observe => translate_observe(call, object),
        ProviderOperation::Recall => {
            let query = string_at(object, &["query_text", "query"])
                .ok_or("ncm.rust.recall_query_missing")?;
            let top_k = u64_at(object, &["top_k", "maximum_candidates"])
                .unwrap_or(5)
                .min(16);
            Ok(json!({"query_text": query, "top_k": top_k}))
        }
        ProviderOperation::Feedback => {
            let records = object
                .get("record_ids")
                .and_then(Value::as_array)
                .ok_or("ncm.rust.feedback_records_missing")?;
            Ok(json!({
                "idempotency_key": required_key(call)?,
                "record_ids": records
            }))
        }
        ProviderOperation::Correction => Ok(json!({
            "idempotency_key": required_key(call)?,
            "superseded": u64_at(object, &["superseded", "superseded_record_id"])
                .ok_or("ncm.rust.correction_superseded_missing")?,
            "superseding": u64_at(object, &["superseding", "superseding_record_id"])
                .ok_or("ncm.rust.correction_superseding_missing")?,
            "evidence": string_at(object, &["evidence", "evidence_sha256"])
                .ok_or("ncm.rust.correction_evidence_missing")?
        })),
        ProviderOperation::Maintenance => {
            let kind = maintenance_kind(object)?;
            Ok(json!({"idempotency_key": required_key(call)?, "kind": kind}))
        }
        ProviderOperation::DeleteBySource => Ok(json!({
            "idempotency_key": required_key(call)?,
            "source": string_at(object, &["source", "source_id", "forget_source_key"])
                .ok_or("ncm.rust.delete_source_missing")?
        })),
        ProviderOperation::SnapshotRestore => {
            let snapshot = object
                .get("snapshot")
                .or_else(|| object.get("bytes"))
                .and_then(Value::as_array)
                .ok_or("ncm.rust.snapshot_bytes_missing")?;
            Ok(json!({"idempotency_key": required_key(call)?, "snapshot": snapshot}))
        }
        ProviderOperation::Replay => Ok(Value::Object(Map::new())),
    }
}

fn translate_observe(
    call: &NcmSurfaceCall,
    object: &Map<String, Value>,
) -> Result<Value, &'static str> {
    let kind =
        string_at(object, &["observation_kind"]).ok_or("ncm.rust.observation_kind_missing")?;
    let payload_contract =
        string_at(object, &["payload_contract"]).ok_or("ncm.rust.observation_contract_missing")?;
    if expected_observation_contract(&kind) != Some(payload_contract.as_str()) {
        return Err("ncm.rust.observation_contract_mismatch");
    }
    let canonical = object
        .get("canonical_payload")
        .and_then(Value::as_object)
        .ok_or("ncm.rust.observation_payload_missing")?;
    let source =
        observation_source(object, canonical).ok_or("ncm.rust.observation_source_missing")?;
    let (key_text, value_text) =
        observation_text(&kind, canonical).ok_or("ncm.rust.observation_text_missing")?;
    let affect = object
        .get("affect")
        .or_else(|| canonical.get("affect"))
        .cloned()
        .unwrap_or(Value::Null);
    let surprise = f32_at(object, canonical, "surprise").unwrap_or(0.0);
    let intensity = f32_at(object, canonical, "intensity").unwrap_or(1.0);
    let provenance = object
        .get("provenance")
        .cloned()
        .unwrap_or_else(|| json!({"observation_kind": kind, "payload_contract": payload_contract}));
    let payload_sha256 = observe_digest(
        &source,
        &key_text,
        &value_text,
        &affect,
        surprise,
        intensity,
        &provenance,
    )
    .map_err(|_| "ncm.rust.observation_digest_failed")?;
    let mut translated = json!({
        "idempotency_key": required_key(call)?,
        "payload_sha256": payload_sha256,
        "source": source,
        "key_text": key_text,
        "value_text": value_text,
        "affect": affect,
        "surprise": surprise,
        "intensity": intensity,
        "provenance": provenance
    });
    for field in ["test_sleep_before_ms", "test_sleep_after_commit_ms"] {
        if let Some(value) = object.get(field)
            && let Some(target) = translated.as_object_mut()
        {
            target.insert(field.to_owned(), value.clone());
        }
    }
    Ok(translated)
}

fn expected_observation_contract(kind: &str) -> Option<&'static str> {
    match kind {
        "session.message_committed.v1" => Some("tracedecay.memory.observation.session-message.v1"),
        "tool.execution_settled.v1" => Some("tracedecay.memory.observation.tool-execution.v1"),
        "source.edit_settled.v1" => Some("tracedecay.memory.observation.source-edit.v1"),
        "test.execution_settled.v1" => Some("tracedecay.memory.observation.test-execution.v1"),
        "diagnostic.observed.v1" => Some("tracedecay.memory.observation.diagnostic.v1"),
        "git.evidence_observed.v1" => Some("tracedecay.memory.observation.git-evidence.v1"),
        "native.fact_promoted.v1" => Some("tracedecay.memory.observation.native-fact-promotion.v1"),
        "feedback.outcome_settled.v1" => Some("tracedecay.memory.observation.feedback-outcome.v1"),
        "automation.outcome_settled.v1" => {
            Some("tracedecay.memory.observation.automation-outcome.v1")
        }
        _ => None,
    }
}

fn observation_source(
    envelope: &Map<String, Value>,
    canonical: &Map<String, Value>,
) -> Option<String> {
    string_at(canonical, &["forget_source_key"])
        .or_else(|| string_at(envelope, &["source_identity"]))
        .or_else(|| {
            envelope
                .get("source_identity")
                .and_then(Value::as_object)
                .and_then(|source| {
                    string_at(
                        source,
                        &[
                            "forget_source_key",
                            "source_event_sha256",
                            "source_event_id",
                        ],
                    )
                })
        })
}

fn observation_text(kind: &str, payload: &Map<String, Value>) -> Option<(String, String)> {
    let nested = payload
        .get("payload")
        .and_then(Value::as_object)
        .unwrap_or(payload);
    let pair = match kind {
        "session.message_committed.v1" => (
            string_at(nested, &["role", "message_kind", "summary"]),
            string_at(nested, &["content", "message", "text", "summary"]),
        ),
        "tool.execution_settled.v1" => (
            string_at(nested, &["command", "tool_name", "tool", "summary"]),
            string_at(
                nested,
                &["outcome_summary", "result", "output", "outcome", "summary"],
            ),
        ),
        "source.edit_settled.v1" => (
            string_at(nested, &["change_summary", "path_summary", "summary"]),
            string_at(
                nested,
                &["result_summary", "diff_summary", "content", "summary"],
            ),
        ),
        "test.execution_settled.v1" => (
            string_at(nested, &["test_name", "command", "summary"]),
            string_at(nested, &["outcome_summary", "result", "outcome", "summary"]),
        ),
        "diagnostic.observed.v1" => (
            string_at(nested, &["code", "diagnostic", "summary"]),
            string_at(nested, &["message", "detail", "summary"]),
        ),
        "git.evidence_observed.v1" => (
            string_at(nested, &["commit", "ref", "summary"]),
            string_at(nested, &["message", "evidence", "summary"]),
        ),
        "native.fact_promoted.v1" => (
            string_at(nested, &["subject", "key", "summary"]),
            string_at(nested, &["fact", "value", "content", "summary"]),
        ),
        "feedback.outcome_settled.v1" | "automation.outcome_settled.v1" => (
            string_at(nested, &["action", "job", "summary"]),
            string_at(nested, &["outcome_summary", "result", "outcome", "summary"]),
        ),
        _ => (None, None),
    };
    match pair {
        (Some(key), Some(value)) if !key.trim().is_empty() && !value.trim().is_empty() => {
            Some((key, value))
        }
        _ => None,
    }
}

fn observe_digest(
    source: &str,
    key_text: &str,
    value_text: &str,
    affect: &Value,
    surprise: f32,
    intensity: f32,
    provenance: &Value,
) -> Result<String, serde_json::Error> {
    let source = serde_json::to_string(source)?;
    let key_text = serde_json::to_string(key_text)?;
    let value_text = serde_json::to_string(value_text)?;
    let affect = serde_json::to_string(affect)?;
    let surprise = serde_json::to_string(&surprise)?;
    let intensity = serde_json::to_string(&intensity)?;
    let provenance = serde_json::to_string(provenance)?;
    let bytes = format!(
        "{{\"source\":{source},\"key_text\":{key_text},\"value_text\":{value_text},\"affect\":{affect},\"surprise\":{surprise},\"intensity\":{intensity},\"provenance\":{provenance}}}"
    );
    Ok(hex_digest(&Sha256::digest(bytes.as_bytes())))
}

fn maintenance_kind(object: &Map<String, Value>) -> Result<Value, &'static str> {
    let kind = object
        .get("kind")
        .ok_or("ncm.rust.maintenance_kind_missing")?;
    if kind.is_object() {
        return Ok(kind.clone());
    }
    let name = kind.as_str().ok_or("ncm.rust.maintenance_kind_invalid")?;
    match name {
        "advance" => Ok(json!({"advance": {"ticks": u64_at(object, &["ticks"]).unwrap_or(1)}})),
        "consolidate" | "merge_prune" | "checkpoint" | "compact" => {
            Ok(Value::String(name.to_owned()))
        }
        _ => Err("ncm.rust.maintenance_kind_unsupported"),
    }
}

fn required_key(call: &NcmSurfaceCall) -> Result<&str, &'static str> {
    call.idempotency_key
        .as_deref()
        .filter(|value| !value.is_empty())
        .ok_or("ncm.rust.idempotency_key_missing")
}

fn string_at(object: &Map<String, Value>, names: &[&str]) -> Option<String> {
    names.iter().find_map(|name| {
        object
            .get(*name)
            .and_then(Value::as_str)
            .filter(|value| !value.is_empty())
            .map(str::to_owned)
    })
}

fn u64_at(object: &Map<String, Value>, names: &[&str]) -> Option<u64> {
    names
        .iter()
        .find_map(|name| object.get(*name).and_then(Value::as_u64))
}

fn f32_at(envelope: &Map<String, Value>, payload: &Map<String, Value>, field: &str) -> Option<f32> {
    envelope
        .get(field)
        .or_else(|| payload.get(field))
        .and_then(Value::as_f64)
        .map(|value| value as f32)
        .filter(|value| value.is_finite())
}

fn outcome_terminal_code(outcome: &Outcome) -> TerminalCode {
    match outcome {
        Outcome::Success => TerminalCode::Success,
        Outcome::Empty => TerminalCode::SuccessZeroResults,
        Outcome::Rejected(RejectReason::IdempotencyConflict) => TerminalCode::Conflict,
        Outcome::Rejected(RejectReason::InvalidRequest(_) | RejectReason::UnknownRecord(_)) => {
            TerminalCode::InvalidRequest
        }
        Outcome::Busy | Outcome::BudgetExceeded => TerminalCode::CapacityExceeded,
        Outcome::Cancelled => TerminalCode::Cancelled,
        Outcome::EffectUnknown => TerminalCode::EffectUnknown,
        Outcome::Incompatible => TerminalCode::StateIncompatible,
        Outcome::Corrupt => TerminalCode::ResetRequired,
        Outcome::Unavailable(_) => TerminalCode::ProviderUnavailable,
        Outcome::Unsupported => TerminalCode::CapabilityUnsupported,
    }
}

fn worker_diagnostic(reply: &WorkerReply) -> &'static str {
    match reply.error.as_ref().map(|error| error.kind.as_str()) {
        Some("oversized_reply" | "oversized_frame") => "ncm.rust.worker_reply_oversized",
        Some("malformed_json") => "ncm.rust.worker_reply_malformed",
        _ => outcome_diagnostic(&reply.outcome),
    }
}

fn outcome_diagnostic(outcome: &Outcome) -> &'static str {
    match outcome {
        Outcome::Success | Outcome::Empty => "ncm.rust.success",
        Outcome::Rejected(RejectReason::IdempotencyConflict) => "ncm.rust.idempotency_conflict",
        Outcome::Rejected(_) => "ncm.rust.request_rejected",
        Outcome::Busy => "ncm.rust.worker_busy",
        Outcome::Cancelled => "ncm.rust.worker_cancelled",
        Outcome::EffectUnknown => "ncm.rust.worker_effect_unknown",
        Outcome::Incompatible => "ncm.rust.state_incompatible",
        Outcome::Corrupt => "ncm.rust.state_corrupt",
        Outcome::Unavailable(_) => "ncm.rust.worker_unavailable",
        Outcome::Unsupported => "ncm.rust.operation_unsupported",
        Outcome::BudgetExceeded => "ncm.rust.budget_exceeded",
    }
}

fn client_terminal_code(error: &ClientError) -> TerminalCode {
    match error {
        ClientError::Busy => TerminalCode::CapacityExceeded,
        ClientError::Cancelled => TerminalCode::Cancelled,
        ClientError::EffectUnknown { .. } => TerminalCode::EffectUnknown,
        ClientError::RequestTooLarge => TerminalCode::InvalidRequest,
        ClientError::Disabled
        | ClientError::Spawn(_)
        | ClientError::RestartExhausted
        | ClientError::Transport(_)
        | ClientError::MalformedReply(_)
        | ClientError::WorkerExited
        | ClientError::UnknownIdempotencyKey
        | ClientError::OwnerStopped => TerminalCode::ProviderUnavailable,
    }
}

fn client_diagnostic(error: &ClientError) -> &'static str {
    match error {
        ClientError::Disabled => "ncm.rust.worker_disabled",
        ClientError::Busy => "ncm.rust.worker_busy",
        ClientError::Cancelled => "ncm.rust.worker_cancelled",
        ClientError::EffectUnknown { .. } => "ncm.rust.worker_effect_unknown",
        ClientError::RequestTooLarge => "ncm.rust.request_too_large",
        ClientError::Spawn(_) => "ncm.rust.worker_spawn_failed",
        ClientError::RestartExhausted => "ncm.rust.worker_restart_exhausted",
        ClientError::Transport(_) => "ncm.rust.worker_transport_failed",
        ClientError::MalformedReply(_) => "ncm.rust.worker_reply_malformed",
        ClientError::WorkerExited => "ncm.rust.worker_exited",
        ClientError::UnknownIdempotencyKey => "ncm.rust.reconciliation_key_unknown",
        ClientError::OwnerStopped => "ncm.rust.worker_owner_stopped",
    }
}

fn error_diagnostic(error: &RustNcmError) -> &'static str {
    match error {
        RustNcmError::WorkerSpawn(_) => "ncm.rust.worker_spawn_failed",
        RustNcmError::StateRoot(_) => "ncm.rust.state_root_invalid",
        RustNcmError::HandshakeIdentity(_) => "ncm.rust.handshake_identity_invalid",
    }
}

fn worker_receipt(operation: ProviderOperation, reply: &WorkerReply) -> String {
    let mut payload = reply.payload.clone().unwrap_or(Value::Null);
    if let Some(object) = payload.as_object_mut() {
        object.remove("replayed");
    }
    let payload_bytes = serde_json::to_vec(&payload).unwrap_or_default();
    let mut digest = Sha256::new();
    digest.update(RECEIPT_DOMAIN);
    digest_field(&mut digest, operation.as_wire().as_bytes());
    digest.update(reply.state_generation.to_be_bytes());
    digest_field(&mut digest, &payload_bytes);
    hex_digest(&digest.finalize())
}

fn unknown_receipt(call: &NcmSurfaceCall, error: &ClientError) -> String {
    let mut digest = Sha256::new();
    digest.update(RECEIPT_DOMAIN);
    digest_field(&mut digest, call.operation.as_wire().as_bytes());
    digest.update(call.expected_state_generation.to_be_bytes());
    digest_field(&mut digest, error.to_string().as_bytes());
    hex_digest(&digest.finalize())
}

fn digest_field(digest: &mut Sha256, bytes: &[u8]) {
    digest.update(u64::try_from(bytes.len()).unwrap_or(u64::MAX).to_be_bytes());
    digest.update(bytes);
}

fn hex_digest(value: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut output = String::with_capacity(value.len().saturating_mul(2));
    for byte in value {
        output.push(char::from(HEX[usize::from(byte >> 4)]));
        output.push(char::from(HEX[usize::from(byte & 0x0f)]));
    }
    output
}
