#![forbid(unsafe_code)]
#![deny(missing_docs)]
#![deny(warnings)]
#![deny(clippy::dbg_macro)]
#![deny(clippy::expect_used)]
#![deny(clippy::panic)]
#![deny(clippy::print_stderr)]
#![deny(clippy::print_stdout)]
#![deny(clippy::todo)]
#![deny(clippy::unimplemented)]
#![deny(clippy::unwrap_used)]
//! Deterministic, dependency-light provider used to prove Memory Provider V1 conformance.
//!
//! The dummy provider is deliberately capability-poor. It implements compatible
//! handshake, mandatory health, idempotent observation acceptance, deterministic
//! recall, and deterministic snapshot/restore. Every unsupported optional lifecycle
//! operation returns a typed terminal outcome. It has no TraceDecay storage, code-index,
//! host, dashboard, transport, or concrete NCM dependency.

use std::collections::BTreeMap;
use std::collections::btree_map::Entry;
use std::str;

use sha2::{Digest, Sha256};

#[rustfmt::skip]
#[path = "../../../contracts/memory-provider-v1/generated/rust/memory_provider_v1.rs"]
/// Generated provider-neutral Memory Provider V1 bindings.
pub mod contract;

use contract::{
    CancellationState, CommittedEffectState, FallbackEligibility, RequestControl, TerminalCode,
};

const SNAPSHOT_MAGIC: &[u8] = b"TRACEDECAY-DUMMY-SNAPSHOT-V1\n";
const PROVIDER_HEALTH: &str = "provider.health.v1";
const OBSERVATION_ACCEPT: &str = "observation.accept.v1";
const RECALL_QUERY: &str = "recall.query.v1";
const SNAPSHOT_EXPORT: &str = "snapshot.export.v1";
const SNAPSHOT_RESTORE: &str = "snapshot.restore.v1";

/// Capabilities implemented by the deterministic dummy provider.
pub const DECLARED_CAPABILITIES: &[&str] = &[
    PROVIDER_HEALTH,
    OBSERVATION_ACCEPT,
    RECALL_QUERY,
    SNAPSHOT_EXPORT,
    SNAPSHOT_RESTORE,
];

/// Immutable operation context admitted by the host.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OperationContext {
    /// Exact TraceDecay scope digest.
    pub exact_scope_digest: String,
    /// Stable UUIDv7-like operation identity used by test fixtures.
    pub operation_id: String,
    /// Deterministic idempotency key for effect-capable calls.
    pub idempotency_key: String,
    /// Provider state generation expected by a new effect.
    pub expected_state_generation: u64,
    /// Deadline and cancellation state supplied by the host.
    pub request_control: RequestControl,
}

/// Provider-neutral terminal result returned by every dummy-provider operation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Terminal<T> {
    /// Closed typed terminal code.
    pub terminal_code: TerminalCode,
    /// Truthful provider-local committed-effect state.
    pub committed_effect: CommittedEffectState,
    /// Explicit fallback eligibility; the dummy provider always forbids fallback.
    pub fallback: FallbackEligibility,
    /// Optional operation-specific successful payload.
    pub payload: Option<T>,
    /// Stable diagnostic identity for failed calls.
    pub diagnostic_id: Option<String>,
    /// Provider state generation observed after the call.
    pub state_generation: u64,
    /// Request idempotency key a duplicate acknowledgement deduplicated.
    pub duplicate_of_idempotency_key: Option<String>,
    /// Operation whose earlier delivery actually committed the effect.
    pub duplicate_of_operation_id: Option<String>,
}

impl<T> Terminal<T> {
    fn success(payload: T, state_generation: u64, effect: CommittedEffectState) -> Self {
        Self {
            terminal_code: TerminalCode::Success,
            committed_effect: effect,
            fallback: FallbackEligibility::Forbidden,
            payload: Some(payload),
            diagnostic_id: None,
            state_generation,
            duplicate_of_idempotency_key: None,
            duplicate_of_operation_id: None,
        }
    }

    /// Reports a redelivery of a mutation this provider already committed.
    ///
    /// The generation is unchanged because nothing new was written, and the
    /// evidence names both the key that matched and the operation that
    /// originally committed, so the host can tell this is the same mutation
    /// rather than a second effect.
    fn duplicate(
        payload: T,
        state_generation: u64,
        duplicate_of_idempotency_key: String,
        duplicate_of_operation_id: String,
    ) -> Self {
        Self {
            terminal_code: TerminalCode::Success,
            committed_effect: CommittedEffectState::Duplicate,
            fallback: FallbackEligibility::Forbidden,
            payload: Some(payload),
            diagnostic_id: None,
            state_generation,
            duplicate_of_idempotency_key: Some(duplicate_of_idempotency_key),
            duplicate_of_operation_id: Some(duplicate_of_operation_id),
        }
    }

    fn zero_results(payload: T, state_generation: u64) -> Self {
        Self {
            terminal_code: TerminalCode::SuccessZeroResults,
            committed_effect: CommittedEffectState::None,
            fallback: FallbackEligibility::Forbidden,
            payload: Some(payload),
            diagnostic_id: None,
            state_generation,
            duplicate_of_idempotency_key: None,
            duplicate_of_operation_id: None,
        }
    }

    fn failure(code: TerminalCode, state_generation: u64) -> Self {
        Self {
            terminal_code: code,
            committed_effect: CommittedEffectState::None,
            fallback: FallbackEligibility::Forbidden,
            payload: None,
            diagnostic_id: Some(format!("dummy.{}", code.as_wire())),
            state_generation,
            duplicate_of_idempotency_key: None,
            duplicate_of_operation_id: None,
        }
    }
}

/// Compatible read-only handshake result.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HandshakeResult {
    /// Stable logical provider identity.
    pub provider_id: String,
    /// Runtime instance identity, separate from provider identity.
    pub provider_instance_id: String,
    /// Exact admitted scope digest.
    pub exact_scope_digest: String,
    /// Selected provider protocol.
    pub selected_protocol: String,
    /// Declared capabilities in deterministic registry order.
    pub declared_capabilities: Vec<String>,
    /// Current provider state generation.
    pub state_generation: u64,
}

/// Mandatory read-only provider health result.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HealthResult {
    /// Stable logical provider identity.
    pub provider_id: String,
    /// Runtime instance identity.
    pub provider_instance_id: String,
    /// Exact admitted scope digest.
    pub exact_scope_digest: String,
    /// Current provider state generation.
    pub state_generation: u64,
    /// Highest monotonically acknowledged source sequence.
    pub acknowledged_sequence: u64,
    /// Number of stored provider-local observations.
    pub stored_observations: usize,
    /// Real declared capabilities.
    pub declared_capabilities: Vec<String>,
}

/// Opaque optional extension preserved without activating behavior.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OwnedOpaqueExtension {
    /// Stable extension identity.
    pub extension_id: String,
    /// Positive extension version.
    pub extension_version: u32,
    /// Whether the extension is required rather than optional.
    pub required: bool,
    /// Canonical opaque payload bytes.
    pub canonical_payload: Vec<u8>,
    /// SHA-256 of canonical payload bytes.
    pub payload_sha256: String,
}

/// Canonically admitted provider observation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Observation {
    /// Stable source observation identity.
    pub observation_id: String,
    /// Deterministic source sequence.
    pub source_sequence: u64,
    /// Canonical observation content used by the dummy recall implementation.
    pub canonical_content: String,
    /// SHA-256 of canonical content bytes.
    pub payload_sha256: String,
    /// Unknown optional extensions to preserve inertly.
    pub extensions: Vec<OwnedOpaqueExtension>,
}

/// Observation acceptance classification.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ObservationAcceptance {
    /// A new provider-local effect was committed.
    Applied,
    /// An identical prior effect was acknowledged without duplication.
    DuplicateAcknowledged,
}

/// Successful observation result.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ObservationResult {
    /// Applied or idempotent-duplicate classification.
    pub acceptance: ObservationAcceptance,
    /// Highest acknowledged source sequence after the call.
    pub acknowledged_sequence: u64,
    /// Digest of the stored observation fingerprint.
    pub stored_fingerprint_sha256: String,
}

/// Bounded deterministic recall request.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RecallRequest {
    /// Exact operation context.
    pub context: OperationContext,
    /// Non-empty case-sensitive query used by the minimal deterministic provider.
    pub query: String,
    /// Positive maximum number of candidates.
    pub maximum_candidates: usize,
}

/// One deterministic advisory recall candidate.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RecallCandidate {
    /// Request-scoped candidate identity.
    pub candidate_id: String,
    /// Stable provider-local reference for this explicit dummy item.
    pub stable_memory_ref: String,
    /// Canonical candidate content.
    pub canonical_content: String,
    /// SHA-256 of canonical candidate content.
    pub content_sha256: String,
    /// Original monotonically admitted source sequence.
    pub source_sequence: u64,
    /// Opaque optional extensions preserved from the source observation.
    pub extensions: Vec<OwnedOpaqueExtension>,
}

/// Successful deterministic recall result.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RecallResult {
    /// Deterministically ordered advisory candidates.
    pub candidates: Vec<RecallCandidate>,
    /// Whether the complete provider-local state was searched.
    pub coverage_complete: bool,
}

/// Deterministic provider-local snapshot.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Snapshot {
    /// Canonical binary snapshot bytes.
    pub bytes: Vec<u8>,
    /// SHA-256 of canonical binary snapshot bytes.
    pub content_sha256: String,
    /// Provider state generation captured by the snapshot.
    pub state_generation: u64,
    /// Highest admitted source sequence captured by the snapshot.
    pub acknowledged_sequence: u64,
}

/// Successful snapshot restore result.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RestoreResult {
    /// Restored provider state generation.
    pub state_generation: u64,
    /// Restored highest source sequence.
    pub acknowledged_sequence: u64,
    /// Whether the restore changed provider-local state.
    pub changed: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct StoredObservation {
    observation_id: String,
    /// Operation whose delivery actually committed this observation. Retained
    /// so a later redelivery under the same key can name what already ran
    /// instead of claiming a fresh effect.
    committed_by_operation_id: String,
    source_sequence: u64,
    canonical_content: String,
    payload_sha256: String,
    fingerprint_sha256: String,
    extensions: Vec<OwnedOpaqueExtension>,
}

/// Deterministic provider that implements mandatory Memory Provider V1 conformance.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DummyProvider {
    provider_id: String,
    provider_instance_id: String,
    exact_scope_digest: String,
    state_generation: u64,
    acknowledged_sequence: u64,
    observations: BTreeMap<String, StoredObservation>,
}

impl DummyProvider {
    /// Constructs an empty deterministic provider for one exact scope.
    pub fn new(provider_id: &str, exact_scope_digest: &str) -> Result<Self, String> {
        contract::ProviderId::new(provider_id).map_err(|error| error.to_string())?;
        if exact_scope_digest.len() != 64
            || !exact_scope_digest
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
        {
            return Err("scope digest must be lowercase SHA-256 hex".to_owned());
        }
        Ok(Self {
            provider_id: provider_id.to_owned(),
            provider_instance_id: format!("{provider_id}.dummy-instance-1"),
            exact_scope_digest: exact_scope_digest.to_owned(),
            state_generation: 0,
            acknowledged_sequence: 0,
            observations: BTreeMap::new(),
        })
    }

    /// Returns current provider-local state generation.
    #[must_use]
    pub const fn state_generation(&self) -> u64 {
        self.state_generation
    }

    /// Returns highest acknowledged source sequence.
    #[must_use]
    pub const fn acknowledged_sequence(&self) -> u64 {
        self.acknowledged_sequence
    }

    /// Returns the exact admitted scope digest.
    #[must_use]
    pub fn exact_scope_digest(&self) -> &str {
        &self.exact_scope_digest
    }

    /// Performs a read-only compatible handshake.
    #[must_use]
    pub fn handshake(
        &self,
        requested_provider_id: &str,
        exact_scope_digest: &str,
        request_control: RequestControl,
    ) -> Terminal<HandshakeResult> {
        if let Some(code) = preflight_control(request_control) {
            return Terminal::failure(code, self.state_generation);
        }
        if requested_provider_id != self.provider_id {
            return Terminal::failure(TerminalCode::InvalidRequest, self.state_generation);
        }
        if exact_scope_digest != self.exact_scope_digest {
            return Terminal::failure(TerminalCode::ScopeMismatch, self.state_generation);
        }
        Terminal::success(
            HandshakeResult {
                provider_id: self.provider_id.clone(),
                provider_instance_id: self.provider_instance_id.clone(),
                exact_scope_digest: self.exact_scope_digest.clone(),
                selected_protocol: "1.0".to_owned(),
                declared_capabilities: declared_capabilities(),
                state_generation: self.state_generation,
            },
            self.state_generation,
            CommittedEffectState::None,
        )
    }

    /// Returns mandatory read-only health for the exact scope.
    #[must_use]
    pub fn health(&self, context: &OperationContext) -> Terminal<HealthResult> {
        if let Some(code) = self.preflight_scope_control(context) {
            return Terminal::failure(code, self.state_generation);
        }
        Terminal::success(
            HealthResult {
                provider_id: self.provider_id.clone(),
                provider_instance_id: self.provider_instance_id.clone(),
                exact_scope_digest: self.exact_scope_digest.clone(),
                state_generation: self.state_generation,
                acknowledged_sequence: self.acknowledged_sequence,
                stored_observations: self.observations.len(),
                declared_capabilities: declared_capabilities(),
            },
            self.state_generation,
            CommittedEffectState::None,
        )
    }

    /// Applies one observation exactly once under its deterministic idempotency key.
    #[must_use]
    pub fn observe(
        &mut self,
        context: &OperationContext,
        observation: Observation,
    ) -> Terminal<ObservationResult> {
        if let Some(code) = self.preflight_scope_control(context) {
            return Terminal::failure(code, self.state_generation);
        }
        if context.idempotency_key.is_empty()
            || observation.observation_id.is_empty()
            || observation.source_sequence == 0
        {
            return Terminal::failure(TerminalCode::InvalidRequest, self.state_generation);
        }
        if observation.payload_sha256 != sha256_hex(observation.canonical_content.as_bytes()) {
            return Terminal::failure(TerminalCode::ContractViolation, self.state_generation);
        }
        if observation
            .extensions
            .iter()
            .any(|extension| extension.required)
        {
            return Terminal::failure(TerminalCode::CapabilityUnsupported, self.state_generation);
        }
        if observation
            .extensions
            .iter()
            .any(|extension| extension.payload_sha256 != sha256_hex(&extension.canonical_payload))
        {
            return Terminal::failure(TerminalCode::ContractViolation, self.state_generation);
        }
        let fingerprint_sha256 = observation_fingerprint(&observation);
        match self.observations.entry(context.idempotency_key.clone()) {
            Entry::Occupied(existing) => {
                if existing.get().fingerprint_sha256 != fingerprint_sha256 {
                    return Terminal::failure(TerminalCode::Conflict, self.state_generation);
                }
                Terminal::duplicate(
                    ObservationResult {
                        acceptance: ObservationAcceptance::DuplicateAcknowledged,
                        acknowledged_sequence: self.acknowledged_sequence,
                        stored_fingerprint_sha256: fingerprint_sha256,
                    },
                    self.state_generation,
                    context.idempotency_key.clone(),
                    existing.get().committed_by_operation_id.clone(),
                )
            }
            Entry::Vacant(vacant) => {
                if context.expected_state_generation != self.state_generation {
                    return Terminal::failure(TerminalCode::StaleIdentity, self.state_generation);
                }
                let expected_sequence = self.acknowledged_sequence.saturating_add(1);
                if observation.source_sequence != expected_sequence {
                    return Terminal::failure(TerminalCode::Conflict, self.state_generation);
                }
                vacant.insert(StoredObservation {
                    observation_id: observation.observation_id,
                    committed_by_operation_id: context.operation_id.clone(),
                    source_sequence: observation.source_sequence,
                    canonical_content: observation.canonical_content,
                    payload_sha256: observation.payload_sha256,
                    fingerprint_sha256: fingerprint_sha256.clone(),
                    extensions: observation.extensions,
                });
                self.acknowledged_sequence = expected_sequence;
                self.state_generation = self.state_generation.saturating_add(1);
                Terminal::success(
                    ObservationResult {
                        acceptance: ObservationAcceptance::Applied,
                        acknowledged_sequence: self.acknowledged_sequence,
                        stored_fingerprint_sha256: fingerprint_sha256,
                    },
                    self.state_generation,
                    CommittedEffectState::Committed,
                )
            }
        }
    }

    /// Performs deterministic read-only substring recall for the exact scope.
    #[must_use]
    pub fn recall(&self, request: &RecallRequest) -> Terminal<RecallResult> {
        if let Some(code) = self.preflight_scope_control(&request.context) {
            return Terminal::failure(code, self.state_generation);
        }
        if request.context.expected_state_generation != self.state_generation {
            return Terminal::failure(TerminalCode::StaleIdentity, self.state_generation);
        }
        if request.query.is_empty() || request.maximum_candidates == 0 {
            return Terminal::failure(TerminalCode::InvalidRequest, self.state_generation);
        }
        let candidates = self
            .observations
            .iter()
            .filter(|(_, observation)| observation.canonical_content.contains(&request.query))
            .take(request.maximum_candidates)
            .enumerate()
            .map(|(index, (stable_ref, observation))| RecallCandidate {
                candidate_id: format!(
                    "dummy-candidate-{}-{}",
                    observation.source_sequence,
                    index.saturating_add(1)
                ),
                stable_memory_ref: stable_ref.clone(),
                canonical_content: observation.canonical_content.clone(),
                content_sha256: observation.payload_sha256.clone(),
                source_sequence: observation.source_sequence,
                extensions: observation.extensions.clone(),
            })
            .collect::<Vec<_>>();
        let payload = RecallResult {
            candidates,
            coverage_complete: true,
        };
        if payload.candidates.is_empty() {
            Terminal::zero_results(payload, self.state_generation)
        } else {
            Terminal::success(payload, self.state_generation, CommittedEffectState::None)
        }
    }

    /// Exports a deterministic generation-consistent snapshot.
    #[must_use]
    pub fn snapshot(&self, context: &OperationContext) -> Terminal<Snapshot> {
        if let Some(code) = self.preflight_scope_control(context) {
            return Terminal::failure(code, self.state_generation);
        }
        if context.expected_state_generation != self.state_generation {
            return Terminal::failure(TerminalCode::StaleIdentity, self.state_generation);
        }
        match self.snapshot_internal() {
            Ok(snapshot) => {
                Terminal::success(snapshot, self.state_generation, CommittedEffectState::None)
            }
            Err(_) => Terminal::failure(TerminalCode::InternalFailure, self.state_generation),
        }
    }

    /// Restores a compatible deterministic snapshot without implicit overwrite.
    #[must_use]
    pub fn restore(
        &mut self,
        context: &OperationContext,
        snapshot: &Snapshot,
    ) -> Terminal<RestoreResult> {
        if let Some(code) = self.preflight_scope_control(context) {
            return Terminal::failure(code, self.state_generation);
        }
        if context.expected_state_generation != self.state_generation {
            return Terminal::failure(TerminalCode::StaleIdentity, self.state_generation);
        }
        if snapshot.content_sha256 != sha256_hex(&snapshot.bytes) {
            return Terminal::failure(TerminalCode::ContractViolation, self.state_generation);
        }
        let decoded = match decode_snapshot(&snapshot.bytes) {
            Ok(decoded) => decoded,
            Err(_) => {
                return Terminal::failure(TerminalCode::StateIncompatible, self.state_generation);
            }
        };
        if decoded.provider_id != self.provider_id
            || decoded.exact_scope_digest != self.exact_scope_digest
            || decoded.state_generation != snapshot.state_generation
            || decoded.acknowledged_sequence != snapshot.acknowledged_sequence
        {
            return Terminal::failure(TerminalCode::StateIncompatible, self.state_generation);
        }
        if let Ok(current) = self.snapshot_internal()
            && current.content_sha256 == snapshot.content_sha256
        {
            return Terminal::success(
                RestoreResult {
                    state_generation: self.state_generation,
                    acknowledged_sequence: self.acknowledged_sequence,
                    changed: false,
                },
                self.state_generation,
                CommittedEffectState::None,
            );
        }
        if !self.observations.is_empty() || self.state_generation != 0 {
            return Terminal::failure(TerminalCode::Conflict, self.state_generation);
        }
        self.state_generation = decoded.state_generation;
        self.acknowledged_sequence = decoded.acknowledged_sequence;
        self.observations = decoded.observations;
        Terminal::success(
            RestoreResult {
                state_generation: self.state_generation,
                acknowledged_sequence: self.acknowledged_sequence,
                changed: true,
            },
            self.state_generation,
            CommittedEffectState::Committed,
        )
    }

    /// Returns a typed unsupported result for an undeclared optional capability.
    #[must_use]
    pub fn unsupported_optional(
        &self,
        context: &OperationContext,
        capability_id: &str,
    ) -> Terminal<()> {
        if let Some(code) = self.preflight_scope_control(context) {
            return Terminal::failure(code, self.state_generation);
        }
        if DECLARED_CAPABILITIES.contains(&capability_id) {
            return Terminal::failure(TerminalCode::InvalidRequest, self.state_generation);
        }
        Terminal::failure(TerminalCode::CapabilityUnsupported, self.state_generation)
    }

    fn preflight_scope_control(&self, context: &OperationContext) -> Option<TerminalCode> {
        if let Some(code) = preflight_control(context.request_control) {
            return Some(code);
        }
        if context.exact_scope_digest != self.exact_scope_digest {
            return Some(TerminalCode::ScopeMismatch);
        }
        None
    }

    fn snapshot_internal(&self) -> Result<Snapshot, SnapshotCodecError> {
        let bytes = encode_snapshot(self)?;
        Ok(Snapshot {
            content_sha256: sha256_hex(&bytes),
            bytes,
            state_generation: self.state_generation,
            acknowledged_sequence: self.acknowledged_sequence,
        })
    }
}

fn declared_capabilities() -> Vec<String> {
    DECLARED_CAPABILITIES
        .iter()
        .map(|capability| (*capability).to_owned())
        .collect()
}

fn preflight_control(control: RequestControl) -> Option<TerminalCode> {
    if control.cancellation == CancellationState::Cancelled {
        Some(TerminalCode::Cancelled)
    } else if control.remaining_millis == 0 {
        Some(TerminalCode::DeadlineExceeded)
    } else {
        None
    }
}

fn observation_fingerprint(observation: &Observation) -> String {
    let mut digest = Sha256::new();
    digest.update(observation.observation_id.as_bytes());
    digest.update(observation.source_sequence.to_be_bytes());
    digest.update(observation.payload_sha256.as_bytes());
    digest.update(observation.canonical_content.as_bytes());
    for extension in &observation.extensions {
        digest.update(extension.extension_id.as_bytes());
        digest.update(extension.extension_version.to_be_bytes());
        digest.update([u8::from(extension.required)]);
        digest.update(extension.payload_sha256.as_bytes());
        digest.update(&extension.canonical_payload);
    }
    hex_digest(digest.finalize().as_slice())
}

fn sha256_hex(value: &[u8]) -> String {
    hex_digest(Sha256::digest(value).as_slice())
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

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum SnapshotCodecError {
    LengthOverflow,
    Truncated,
    InvalidUtf8,
    InvalidMagic,
    TrailingBytes,
    InvalidBoolean,
    DuplicateKey,
}

fn write_u64(output: &mut Vec<u8>, value: u64) {
    output.extend_from_slice(&value.to_be_bytes());
}

fn write_u32(output: &mut Vec<u8>, value: u32) {
    output.extend_from_slice(&value.to_be_bytes());
}

fn write_bytes(output: &mut Vec<u8>, value: &[u8]) -> Result<(), SnapshotCodecError> {
    let length = u64::try_from(value.len()).map_err(|_| SnapshotCodecError::LengthOverflow)?;
    write_u64(output, length);
    output.extend_from_slice(value);
    Ok(())
}

fn write_string(output: &mut Vec<u8>, value: &str) -> Result<(), SnapshotCodecError> {
    write_bytes(output, value.as_bytes())
}

fn encode_snapshot(provider: &DummyProvider) -> Result<Vec<u8>, SnapshotCodecError> {
    let mut output = Vec::new();
    output.extend_from_slice(SNAPSHOT_MAGIC);
    write_string(&mut output, &provider.provider_id)?;
    write_string(&mut output, &provider.exact_scope_digest)?;
    write_u64(&mut output, provider.state_generation);
    write_u64(&mut output, provider.acknowledged_sequence);
    write_u64(
        &mut output,
        u64::try_from(provider.observations.len())
            .map_err(|_| SnapshotCodecError::LengthOverflow)?,
    );
    for (idempotency_key, observation) in &provider.observations {
        write_string(&mut output, idempotency_key)?;
        write_string(&mut output, &observation.observation_id)?;
        write_string(&mut output, &observation.committed_by_operation_id)?;
        write_u64(&mut output, observation.source_sequence);
        write_string(&mut output, &observation.canonical_content)?;
        write_string(&mut output, &observation.payload_sha256)?;
        write_string(&mut output, &observation.fingerprint_sha256)?;
        write_u64(
            &mut output,
            u64::try_from(observation.extensions.len())
                .map_err(|_| SnapshotCodecError::LengthOverflow)?,
        );
        for extension in &observation.extensions {
            write_string(&mut output, &extension.extension_id)?;
            write_u32(&mut output, extension.extension_version);
            output.push(u8::from(extension.required));
            write_bytes(&mut output, &extension.canonical_payload)?;
            write_string(&mut output, &extension.payload_sha256)?;
        }
    }
    Ok(output)
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct DecodedSnapshot {
    provider_id: String,
    exact_scope_digest: String,
    state_generation: u64,
    acknowledged_sequence: u64,
    observations: BTreeMap<String, StoredObservation>,
}

struct SnapshotCursor<'a> {
    bytes: &'a [u8],
    offset: usize,
}

impl<'a> SnapshotCursor<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, offset: 0 }
    }

    fn read_exact(&mut self, length: usize) -> Result<&'a [u8], SnapshotCodecError> {
        let end = self
            .offset
            .checked_add(length)
            .ok_or(SnapshotCodecError::LengthOverflow)?;
        let value = self
            .bytes
            .get(self.offset..end)
            .ok_or(SnapshotCodecError::Truncated)?;
        self.offset = end;
        Ok(value)
    }

    fn read_u64(&mut self) -> Result<u64, SnapshotCodecError> {
        let bytes = self.read_exact(8)?;
        let mut value = [0_u8; 8];
        value.copy_from_slice(bytes);
        Ok(u64::from_be_bytes(value))
    }

    fn read_u32(&mut self) -> Result<u32, SnapshotCodecError> {
        let bytes = self.read_exact(4)?;
        let mut value = [0_u8; 4];
        value.copy_from_slice(bytes);
        Ok(u32::from_be_bytes(value))
    }

    fn read_bytes(&mut self) -> Result<Vec<u8>, SnapshotCodecError> {
        let length =
            usize::try_from(self.read_u64()?).map_err(|_| SnapshotCodecError::LengthOverflow)?;
        Ok(self.read_exact(length)?.to_vec())
    }

    fn read_string(&mut self) -> Result<String, SnapshotCodecError> {
        String::from_utf8(self.read_bytes()?).map_err(|_| SnapshotCodecError::InvalidUtf8)
    }

    fn finished(&self) -> bool {
        self.offset == self.bytes.len()
    }
}

fn decode_snapshot(bytes: &[u8]) -> Result<DecodedSnapshot, SnapshotCodecError> {
    if !bytes.starts_with(SNAPSHOT_MAGIC) {
        return Err(SnapshotCodecError::InvalidMagic);
    }
    let mut cursor = SnapshotCursor::new(&bytes[SNAPSHOT_MAGIC.len()..]);
    let provider_id = cursor.read_string()?;
    let exact_scope_digest = cursor.read_string()?;
    let state_generation = cursor.read_u64()?;
    let acknowledged_sequence = cursor.read_u64()?;
    let observation_count =
        usize::try_from(cursor.read_u64()?).map_err(|_| SnapshotCodecError::LengthOverflow)?;
    let mut observations = BTreeMap::new();
    for _ in 0..observation_count {
        let idempotency_key = cursor.read_string()?;
        let observation_id = cursor.read_string()?;
        let committed_by_operation_id = cursor.read_string()?;
        let source_sequence = cursor.read_u64()?;
        let canonical_content = cursor.read_string()?;
        let payload_sha256 = cursor.read_string()?;
        let fingerprint_sha256 = cursor.read_string()?;
        let extension_count =
            usize::try_from(cursor.read_u64()?).map_err(|_| SnapshotCodecError::LengthOverflow)?;
        let mut extensions = Vec::with_capacity(extension_count);
        for _ in 0..extension_count {
            let extension_id = cursor.read_string()?;
            let extension_version = cursor.read_u32()?;
            let required = match cursor.read_exact(1)?[0] {
                0 => false,
                1 => true,
                _ => return Err(SnapshotCodecError::InvalidBoolean),
            };
            let canonical_payload = cursor.read_bytes()?;
            let payload_sha256 = cursor.read_string()?;
            extensions.push(OwnedOpaqueExtension {
                extension_id,
                extension_version,
                required,
                canonical_payload,
                payload_sha256,
            });
        }
        let previous = observations.insert(
            idempotency_key,
            StoredObservation {
                observation_id,
                committed_by_operation_id,
                source_sequence,
                canonical_content,
                payload_sha256,
                fingerprint_sha256,
                extensions,
            },
        );
        if previous.is_some() {
            return Err(SnapshotCodecError::DuplicateKey);
        }
    }
    if !cursor.finished() {
        return Err(SnapshotCodecError::TrailingBytes);
    }
    Ok(DecodedSnapshot {
        provider_id,
        exact_scope_digest,
        state_generation,
        acknowledged_sequence,
        observations,
    })
}
