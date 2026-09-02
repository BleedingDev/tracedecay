//! Mandatory and restart conformance journeys for the deterministic dummy provider.

use std::error::Error;
use std::io;

use sha2::{Digest, Sha256};
use tracedecay_memory_dummy_provider::contract::{
    CancellationState, CommittedEffectState, FallbackEligibility, RequestControl, TerminalCode,
};
use tracedecay_memory_dummy_provider::{
    DECLARED_CAPABILITIES, DummyProvider, Observation, ObservationAcceptance, OperationContext,
    OwnedOpaqueExtension, RecallRequest, Snapshot, Terminal,
};

const PROVIDER_ID: &str = "test.dummy";
const SCOPE_A: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
const SCOPE_B: &str = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";

fn sha256_hex(bytes: &[u8]) -> String {
    let digest = Sha256::digest(bytes);
    let mut output = String::with_capacity(64);
    for byte in digest {
        use std::fmt::Write as _;
        let _ = write!(output, "{byte:02x}");
    }
    output
}

fn live_control() -> RequestControl {
    RequestControl {
        deadline_utc_micros: i64::MAX,
        remaining_millis: 10_000,
        cancellation: CancellationState::Live,
    }
}

fn context(scope: &str, generation: u64, key: &str) -> OperationContext {
    OperationContext {
        exact_scope_digest: scope.to_owned(),
        operation_id: format!("operation-{key}"),
        idempotency_key: key.to_owned(),
        expected_state_generation: generation,
        request_control: live_control(),
    }
}

fn observation(sequence: u64, content: &str) -> Observation {
    Observation {
        observation_id: format!("observation-{sequence}"),
        source_sequence: sequence,
        canonical_content: content.to_owned(),
        payload_sha256: sha256_hex(content.as_bytes()),
        extensions: Vec::new(),
    }
}

fn extension(required: bool, payload: &[u8]) -> OwnedOpaqueExtension {
    OwnedOpaqueExtension {
        extension_id: "vendor.example.v1".to_owned(),
        extension_version: 1,
        required,
        canonical_payload: payload.to_vec(),
        payload_sha256: sha256_hex(payload),
    }
}

fn provider(scope: &str) -> Result<DummyProvider, Box<dyn Error>> {
    DummyProvider::new(PROVIDER_ID, scope)
        .map_err(|message| io::Error::new(io::ErrorKind::InvalidInput, message).into())
}

fn payload<T>(terminal: Terminal<T>, label: &str) -> Result<T, Box<dyn Error>> {
    match terminal.payload {
        Some(value) => Ok(value),
        None => Err(io::Error::other(format!(
            "{label} returned no payload ({:?})",
            terminal.terminal_code
        ))
        .into()),
    }
}

fn apply(
    provider: &mut DummyProvider,
    key: &str,
    sequence: u64,
    content: &str,
) -> Result<(), Box<dyn Error>> {
    let terminal = provider.observe(
        &context(SCOPE_A, provider.state_generation(), key),
        observation(sequence, content),
    );
    if terminal.terminal_code != TerminalCode::Success {
        return Err(io::Error::other(format!(
            "observation was not applied: {:?}",
            terminal.terminal_code
        ))
        .into());
    }
    Ok(())
}

#[test]
fn compatible_handshake_is_read_only() -> Result<(), Box<dyn Error>> {
    let provider = provider(SCOPE_A)?;
    let before = provider.clone();
    let terminal = provider.handshake(PROVIDER_ID, SCOPE_A, live_control());
    assert_eq!(terminal.terminal_code, TerminalCode::Success);
    assert_eq!(terminal.committed_effect, CommittedEffectState::None);
    let result = payload(terminal, "handshake")?;
    assert_eq!(result.provider_id, PROVIDER_ID);
    assert_eq!(result.selected_protocol, "1.0");
    assert_eq!(result.declared_capabilities, DECLARED_CAPABILITIES);
    assert_eq!(provider, before);
    Ok(())
}

#[test]
fn provider_identity_mismatch_fails_closed() -> Result<(), Box<dyn Error>> {
    let provider = provider(SCOPE_A)?;
    let terminal = provider.handshake("test.other", SCOPE_A, live_control());
    assert_eq!(terminal.terminal_code, TerminalCode::InvalidRequest);
    assert_eq!(terminal.committed_effect, CommittedEffectState::None);
    assert!(terminal.payload.is_none());
    assert_eq!(provider.state_generation(), 0);
    Ok(())
}

#[test]
fn health_reports_real_capabilities_without_mutation() -> Result<(), Box<dyn Error>> {
    let provider = provider(SCOPE_A)?;
    let before = provider.clone();
    let terminal = provider.health(&context(SCOPE_A, 0, "health"));
    assert_eq!(terminal.terminal_code, TerminalCode::Success);
    let result = payload(terminal, "health")?;
    assert_eq!(result.declared_capabilities, DECLARED_CAPABILITIES);
    assert_eq!(result.stored_observations, 0);
    assert_eq!(provider, before);
    Ok(())
}

#[test]
fn cancelled_call_stops_before_effect() -> Result<(), Box<dyn Error>> {
    let mut provider = provider(SCOPE_A)?;
    let mut request = context(SCOPE_A, 0, "cancelled");
    request.request_control.cancellation = CancellationState::Cancelled;
    let terminal = provider.observe(&request, observation(1, "cancelled content"));
    assert_eq!(terminal.terminal_code, TerminalCode::Cancelled);
    assert_eq!(terminal.committed_effect, CommittedEffectState::None);
    assert_eq!(provider.state_generation(), 0);
    assert_eq!(provider.acknowledged_sequence(), 0);
    Ok(())
}

#[test]
fn expired_deadline_stops_before_effect() -> Result<(), Box<dyn Error>> {
    let mut provider = provider(SCOPE_A)?;
    let mut request = context(SCOPE_A, 0, "expired");
    request.request_control.remaining_millis = 0;
    let terminal = provider.observe(&request, observation(1, "expired content"));
    assert_eq!(terminal.terminal_code, TerminalCode::DeadlineExceeded);
    assert_eq!(terminal.committed_effect, CommittedEffectState::None);
    assert_eq!(provider.state_generation(), 0);
    Ok(())
}

#[test]
fn scope_mismatch_fails_closed() -> Result<(), Box<dyn Error>> {
    let mut provider = provider(SCOPE_A)?;
    let terminal = provider.observe(
        &context(SCOPE_B, 0, "wrong-scope"),
        observation(1, "wrong scope"),
    );
    assert_eq!(terminal.terminal_code, TerminalCode::ScopeMismatch);
    assert_eq!(terminal.committed_effect, CommittedEffectState::None);
    assert_eq!(provider.state_generation(), 0);
    Ok(())
}

#[test]
fn observation_applies_once() -> Result<(), Box<dyn Error>> {
    let mut provider = provider(SCOPE_A)?;
    let terminal = provider.observe(
        &context(SCOPE_A, 0, "first"),
        observation(1, "first observation"),
    );
    assert_eq!(terminal.terminal_code, TerminalCode::Success);
    assert_eq!(terminal.committed_effect, CommittedEffectState::Committed);
    let result = payload(terminal, "observe")?;
    assert_eq!(result.acceptance, ObservationAcceptance::Applied);
    assert_eq!(result.acknowledged_sequence, 1);
    assert_eq!(provider.state_generation(), 1);
    Ok(())
}

#[test]
fn duplicate_observation_is_idempotent() -> Result<(), Box<dyn Error>> {
    let mut provider = provider(SCOPE_A)?;
    let request = context(SCOPE_A, 0, "duplicate");
    let item = observation(1, "same observation");
    let first = provider.observe(&request, item.clone());
    assert_eq!(first.terminal_code, TerminalCode::Success);
    assert_eq!(first.committed_effect, CommittedEffectState::Committed);
    let generation = provider.state_generation();
    // An at-least-once redelivery reaches the provider as a *new* operation that
    // carries the same idempotency key, so the acknowledgement has to name the
    // earlier operation that actually committed the effect.
    let redelivery = OperationContext {
        operation_id: "operation-duplicate-retry".to_owned(),
        ..context(SCOPE_A, generation, "duplicate")
    };
    let duplicate = provider.observe(&redelivery, item);
    assert_eq!(duplicate.terminal_code, TerminalCode::Success);
    assert_eq!(duplicate.committed_effect, CommittedEffectState::Duplicate);
    assert_eq!(
        duplicate.duplicate_of_idempotency_key.as_deref(),
        Some("duplicate")
    );
    assert_eq!(
        duplicate.duplicate_of_operation_id.as_deref(),
        Some("operation-duplicate")
    );
    assert_eq!(duplicate.state_generation, generation);
    let result = payload(duplicate, "duplicate observe")?;
    assert_eq!(
        result.acceptance,
        ObservationAcceptance::DuplicateAcknowledged
    );
    assert_eq!(provider.state_generation(), generation);
    assert_eq!(provider.acknowledged_sequence(), 1);
    Ok(())
}

#[test]
fn same_key_different_observation_conflicts() -> Result<(), Box<dyn Error>> {
    let mut provider = provider(SCOPE_A)?;
    let request = context(SCOPE_A, 0, "conflict");
    let first = provider.observe(&request, observation(1, "first value"));
    assert_eq!(first.terminal_code, TerminalCode::Success);
    let generation = provider.state_generation();
    let conflict = provider.observe(&request, observation(1, "different value"));
    assert_eq!(conflict.terminal_code, TerminalCode::Conflict);
    assert_eq!(conflict.committed_effect, CommittedEffectState::None);
    assert_eq!(provider.state_generation(), generation);
    Ok(())
}

#[test]
fn source_sequence_gap_conflicts() -> Result<(), Box<dyn Error>> {
    let mut provider = provider(SCOPE_A)?;
    let terminal = provider.observe(&context(SCOPE_A, 0, "gap"), observation(2, "gap"));
    assert_eq!(terminal.terminal_code, TerminalCode::Conflict);
    assert_eq!(provider.state_generation(), 0);
    assert_eq!(provider.acknowledged_sequence(), 0);
    Ok(())
}

#[test]
fn stale_state_generation_fails() -> Result<(), Box<dyn Error>> {
    let mut provider = provider(SCOPE_A)?;
    apply(&mut provider, "first", 1, "first")?;
    let terminal = provider.observe(&context(SCOPE_A, 0, "stale"), observation(2, "second"));
    assert_eq!(terminal.terminal_code, TerminalCode::StaleIdentity);
    assert_eq!(terminal.committed_effect, CommittedEffectState::None);
    assert_eq!(provider.state_generation(), 1);
    Ok(())
}

#[test]
fn required_unknown_extension_is_unsupported() -> Result<(), Box<dyn Error>> {
    let mut provider = provider(SCOPE_A)?;
    let mut item = observation(1, "required extension");
    item.extensions.push(extension(true, b"required"));
    let terminal = provider.observe(&context(SCOPE_A, 0, "required-extension"), item);
    assert_eq!(terminal.terminal_code, TerminalCode::CapabilityUnsupported);
    assert_eq!(terminal.committed_effect, CommittedEffectState::None);
    assert_eq!(provider.state_generation(), 0);
    Ok(())
}

#[test]
fn optional_unknown_extension_round_trips_inertly() -> Result<(), Box<dyn Error>> {
    let mut provider = provider(SCOPE_A)?;
    let opaque = extension(false, b"opaque-payload");
    let mut item = observation(1, "extension content");
    item.extensions.push(opaque.clone());
    let applied = provider.observe(&context(SCOPE_A, 0, "optional-extension"), item);
    assert_eq!(applied.terminal_code, TerminalCode::Success);
    let request = RecallRequest {
        context: context(SCOPE_A, 1, "recall-extension"),
        query: "extension".to_owned(),
        maximum_candidates: 5,
    };
    let result = payload(provider.recall(&request), "recall")?;
    assert_eq!(result.candidates.len(), 1);
    assert_eq!(result.candidates[0].extensions, vec![opaque]);
    Ok(())
}

#[test]
fn recall_is_deterministic_and_advisory() -> Result<(), Box<dyn Error>> {
    let mut provider = provider(SCOPE_A)?;
    apply(&mut provider, "z-key", 1, "shared first")?;
    apply(&mut provider, "a-key", 2, "shared second")?;
    let request = RecallRequest {
        context: context(SCOPE_A, 2, "recall"),
        query: "shared".to_owned(),
        maximum_candidates: 10,
    };
    let first = provider.recall(&request);
    let second = provider.recall(&request);
    assert_eq!(first, second);
    assert_eq!(first.terminal_code, TerminalCode::Success);
    assert_eq!(first.committed_effect, CommittedEffectState::None);
    assert_eq!(first.fallback, FallbackEligibility::Forbidden);
    let result = payload(first, "recall")?;
    assert_eq!(result.candidates.len(), 2);
    assert_eq!(result.candidates[0].stable_memory_ref, "a-key");
    assert_eq!(result.candidates[1].stable_memory_ref, "z-key");
    assert_eq!(provider.state_generation(), 2);
    Ok(())
}

#[test]
fn zero_results_is_typed_success() -> Result<(), Box<dyn Error>> {
    let provider = provider(SCOPE_A)?;
    let request = RecallRequest {
        context: context(SCOPE_A, 0, "zero-results"),
        query: "missing".to_owned(),
        maximum_candidates: 10,
    };
    let terminal = provider.recall(&request);
    assert_eq!(terminal.terminal_code, TerminalCode::SuccessZeroResults);
    assert_eq!(terminal.committed_effect, CommittedEffectState::None);
    assert_eq!(payload(terminal, "zero-result recall")?.candidates.len(), 0);
    Ok(())
}

#[test]
fn snapshot_bytes_are_deterministic() -> Result<(), Box<dyn Error>> {
    let mut provider = provider(SCOPE_A)?;
    apply(&mut provider, "first", 1, "snapshot content")?;
    let request = context(SCOPE_A, 1, "snapshot");
    let first = payload(provider.snapshot(&request), "snapshot")?;
    let second = payload(provider.snapshot(&request), "snapshot")?;
    assert_eq!(first, second);
    assert_eq!(first.content_sha256, sha256_hex(&first.bytes));
    Ok(())
}

#[test]
fn restart_restore_preserves_recall() -> Result<(), Box<dyn Error>> {
    let mut original = provider(SCOPE_A)?;
    apply(&mut original, "first", 1, "restart memory")?;
    let snapshot = payload(
        original.snapshot(&context(SCOPE_A, 1, "snapshot")),
        "snapshot",
    )?;

    let mut restarted = provider(SCOPE_A)?;
    let restored = restarted.restore(&context(SCOPE_A, 0, "restore"), &snapshot);
    assert_eq!(restored.terminal_code, TerminalCode::Success);
    assert_eq!(restored.committed_effect, CommittedEffectState::Committed);
    assert!(payload(restored, "restore")?.changed);

    let recall = RecallRequest {
        context: context(SCOPE_A, 1, "recall-restored"),
        query: "restart".to_owned(),
        maximum_candidates: 5,
    };
    let result = payload(restarted.recall(&recall), "restored recall")?;
    assert_eq!(result.candidates.len(), 1);
    assert_eq!(result.candidates[0].canonical_content, "restart memory");
    Ok(())
}

#[test]
fn identical_restore_is_no_effect() -> Result<(), Box<dyn Error>> {
    let mut provider = provider(SCOPE_A)?;
    apply(&mut provider, "first", 1, "same state")?;
    let snapshot = payload(
        provider.snapshot(&context(SCOPE_A, 1, "snapshot")),
        "snapshot",
    )?;
    let restored = provider.restore(&context(SCOPE_A, 1, "restore-identical"), &snapshot);
    assert_eq!(restored.terminal_code, TerminalCode::Success);
    assert_eq!(restored.committed_effect, CommittedEffectState::None);
    assert!(!payload(restored, "identical restore")?.changed);
    assert_eq!(provider.state_generation(), 1);
    Ok(())
}

#[test]
fn nonempty_different_restore_conflicts() -> Result<(), Box<dyn Error>> {
    let mut source = provider(SCOPE_A)?;
    apply(&mut source, "source", 1, "source state")?;
    let snapshot = payload(
        source.snapshot(&context(SCOPE_A, 1, "source-snapshot")),
        "snapshot",
    )?;

    let mut destination = provider(SCOPE_A)?;
    apply(&mut destination, "destination", 1, "different state")?;
    let terminal = destination.restore(&context(SCOPE_A, 1, "different-restore"), &snapshot);
    assert_eq!(terminal.terminal_code, TerminalCode::Conflict);
    assert_eq!(terminal.committed_effect, CommittedEffectState::None);
    assert_eq!(destination.state_generation(), 1);
    Ok(())
}

#[test]
fn corrupt_snapshot_fails_closed() -> Result<(), Box<dyn Error>> {
    let mut source = provider(SCOPE_A)?;
    apply(&mut source, "source", 1, "source state")?;
    let mut snapshot = payload(
        source.snapshot(&context(SCOPE_A, 1, "snapshot")),
        "snapshot",
    )?;
    let byte = snapshot
        .bytes
        .get_mut(0)
        .ok_or_else(|| io::Error::other("snapshot unexpectedly empty"))?;
    *byte ^= 0xff;
    snapshot.content_sha256 = sha256_hex(&snapshot.bytes);

    let mut destination = provider(SCOPE_A)?;
    let terminal = destination.restore(&context(SCOPE_A, 0, "corrupt"), &snapshot);
    assert_eq!(terminal.terminal_code, TerminalCode::StateIncompatible);
    assert_eq!(terminal.committed_effect, CommittedEffectState::None);
    assert_eq!(destination.state_generation(), 0);
    Ok(())
}

#[test]
fn cross_scope_snapshot_is_incompatible() -> Result<(), Box<dyn Error>> {
    let mut source = provider(SCOPE_A)?;
    apply(&mut source, "source", 1, "source state")?;
    let snapshot: Snapshot = payload(
        source.snapshot(&context(SCOPE_A, 1, "snapshot")),
        "snapshot",
    )?;
    let mut destination = provider(SCOPE_B)?;
    let terminal = destination.restore(&context(SCOPE_B, 0, "cross-scope"), &snapshot);
    assert_eq!(terminal.terminal_code, TerminalCode::StateIncompatible);
    assert_eq!(destination.state_generation(), 0);
    Ok(())
}

#[test]
fn unsupported_feedback_is_explicit() -> Result<(), Box<dyn Error>> {
    let provider = provider(SCOPE_A)?;
    let terminal =
        provider.unsupported_optional(&context(SCOPE_A, 0, "feedback"), "feedback.record.v1");
    assert_eq!(terminal.terminal_code, TerminalCode::CapabilityUnsupported);
    assert_eq!(terminal.fallback, FallbackEligibility::Forbidden);
    assert_eq!(terminal.committed_effect, CommittedEffectState::None);
    Ok(())
}

#[test]
fn unsupported_maintenance_is_explicit() -> Result<(), Box<dyn Error>> {
    let provider = provider(SCOPE_A)?;
    let terminal =
        provider.unsupported_optional(&context(SCOPE_A, 0, "maintenance"), "maintenance.run.v1");
    assert_eq!(terminal.terminal_code, TerminalCode::CapabilityUnsupported);
    assert_eq!(terminal.fallback, FallbackEligibility::Forbidden);
    assert_eq!(terminal.committed_effect, CommittedEffectState::None);
    Ok(())
}
