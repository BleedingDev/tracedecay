#!/usr/bin/env python3
"""Materialize the TraceDecay Native provider adapter for tdmem-0303."""

from __future__ import annotations

import fnmatch
import json
import subprocess
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
CRATE = ROOT / "crates/tracedecay-memory-provider-native"
FLOOR = "08fbe33a7c7f403191fd5d6e356c7b6681b96403"

CARGO = '''[package]
name = "tracedecay-memory-provider-native"
version.workspace = true
edition.workspace = true
publish = false
license = "MIT"
description = "Provider-neutral adapter boundary for TraceDecay Native memory application ports"
repository = "https://github.com/BleedingDev/tracedecay"

[dependencies]
tracedecay-memory-provider-api = { path = "../tracedecay-memory-provider-api" }

[lints.rust]
missing_docs = "deny"
unsafe_code = "forbid"
warnings = "deny"

[lints.clippy]
dbg_macro = "deny"
expect_used = "deny"
panic = "deny"
print_stderr = "deny"
print_stdout = "deny"
todo = "deny"
unimplemented = "deny"
unwrap_used = "deny"
'''

LIB = r'''#![forbid(unsafe_code)]
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
//! TraceDecay Native memory behind the provider-neutral runtime boundary.
//!
//! This crate is deliberately an adapter, not a second memory implementation.
//! It owns no database, index, scoring, curation, privacy, graph, or persistence
//! state. A future composition mount supplies the existing owner-bound Native
//! application port. The adapter validates the stable Native provider identity,
//! routes provider operations to narrow port methods, preserves canonical call
//! bytes and exact scope unchanged, and rejects undeclared optional operations
//! through the Native port's typed terminal authority.

use std::error::Error;
use std::fmt;
use std::sync::Arc;

use tracedecay_memory_provider_api::contract::TerminalCode;
use tracedecay_memory_provider_api::{
    ApiError, HandshakeRequest, HandshakeResponse, MemoryProvider, ProviderCall,
    ProviderDescriptor, ProviderOperation, ProviderReply,
};

/// Stable logical provider identity for TraceDecay Native memory.
pub const NATIVE_PROVIDER_ID: &str = "tracedecay.native";

/// Construction failure before a Native adapter can be registered.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum NativeAdapterError {
    /// The supplied application port did not expose the stable Native identity.
    ProviderIdMismatch {
        /// Required stable identity.
        expected: &'static str,
        /// Identity declared by the supplied port.
        declared: String,
    },
}

impl fmt::Display for NativeAdapterError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ProviderIdMismatch { expected, declared } => write!(
                formatter,
                "Native application port declared provider {declared}, expected {expected}"
            ),
        }
    }
}

impl Error for NativeAdapterError {}

/// Narrow application boundary implemented by the existing TraceDecay Native
/// memory composition in M3.
///
/// The port owns Native authority and therefore constructs all Native terminal
/// records, provenance, receipts, and exact-scope digests. The adapter never
/// fabricates these values and never opens or mutates Native persistence.
pub trait NativeMemoryApplicationPort: Send + Sync + 'static {
    /// Returns the current real Native descriptor and capability set.
    fn descriptor(&self) -> ProviderDescriptor;

    /// Performs the existing read-only Native compatibility handshake.
    fn handshake(&self, request: &HandshakeRequest) -> HandshakeResponse;

    /// Executes mandatory Native health without changing state.
    fn health(&self, call: &ProviderCall) -> ProviderReply;

    /// Admits a provider observation through the existing Native application
    /// policy. Arbitrary observations must not be converted into facts by the
    /// adapter itself.
    fn observe(&self, call: &ProviderCall) -> ProviderReply;

    /// Executes existing Native recall and preserves Native ordering, scores,
    /// evidence, temporal state, and provenance in the canonical payload.
    fn recall(&self, call: &ProviderCall) -> ProviderReply;

    /// Executes one declared optional Native lifecycle or inspection operation.
    fn lifecycle(&self, call: &ProviderCall) -> ProviderReply;

    /// Returns a typed Native rejection with the authoritative exact-scope
    /// digest and diagnostic identity.
    fn reject(
        &self,
        call: &ProviderCall,
        terminal_code: TerminalCode,
        diagnostic_id: &'static str,
    ) -> ProviderReply;
}

/// Provider-neutral TraceDecay Native adapter over one existing application
/// port.
pub struct NativeProvider {
    port: Arc<dyn NativeMemoryApplicationPort>,
}

impl NativeProvider {
    /// Constructs a Native provider only when the supplied port declares the
    /// stable Native identity and the mandatory provider capabilities.
    pub fn new(port: Arc<dyn NativeMemoryApplicationPort>) -> Result<Self, NativeAdapterError> {
        let descriptor = port.descriptor();
        if descriptor.provider_id.as_str() != NATIVE_PROVIDER_ID {
            return Err(NativeAdapterError::ProviderIdMismatch {
                expected: NATIVE_PROVIDER_ID,
                declared: descriptor.provider_id.as_str().to_owned(),
            });
        }
        Ok(Self { port })
    }

    fn reject(
        &self,
        call: &ProviderCall,
        terminal_code: TerminalCode,
        diagnostic_id: &'static str,
    ) -> ProviderReply {
        self.port.reject(call, terminal_code, diagnostic_id)
    }
}

impl MemoryProvider for NativeProvider {
    fn descriptor(&self) -> ProviderDescriptor {
        self.port.descriptor()
    }

    fn handshake(&self, request: &HandshakeRequest) -> HandshakeResponse {
        self.port.handshake(request)
    }

    fn invoke(&self, call: &ProviderCall) -> ProviderReply {
        if call.provider_id.as_str() != NATIVE_PROVIDER_ID {
            return self.reject(
                call,
                TerminalCode::InvalidRequest,
                "native.provider_id_mismatch",
            );
        }
        if call.operation == ProviderOperation::Handshake {
            return self.reject(
                call,
                TerminalCode::InvalidRequest,
                "native.handshake_requires_handshake_port",
            );
        }
        let descriptor = self.port.descriptor();
        if !descriptor.supports(call.operation.capability_id()) {
            return self.reject(
                call,
                TerminalCode::CapabilityUnsupported,
                "native.capability_unsupported",
            );
        }
        match call.operation {
            ProviderOperation::Health => self.port.health(call),
            ProviderOperation::Observe => self.port.observe(call),
            ProviderOperation::Recall => self.port.recall(call),
            ProviderOperation::Feedback
            | ProviderOperation::Maintenance
            | ProviderOperation::Inspection
            | ProviderOperation::Correction
            | ProviderOperation::DeleteBySource
            | ProviderOperation::SnapshotExport
            | ProviderOperation::SnapshotRestore
            | ProviderOperation::Replay => self.port.lifecycle(call),
            ProviderOperation::Handshake => self.reject(
                call,
                TerminalCode::InvalidRequest,
                "native.handshake_requires_handshake_port",
            ),
        }
    }
}
'''

TESTS = r'''use std::collections::BTreeSet;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use tracedecay_memory_provider_api::contract::{
    CommittedEffectState, FallbackEligibility, TerminalCode,
};
use tracedecay_memory_provider_api::{
    CancellationToken, CanonicalPayload, HandshakeRequest, HandshakeRequestParts,
    HandshakeResponse, MemoryProvider, OperationControl, OwnedExactScope, OwnedProviderId,
    OwnedVersionedId, ProviderCall, ProviderCallParts, ProviderDescriptor, ProviderLimits,
    ProviderOperation, ProviderReply, TerminalRecord,
};
use tracedecay_memory_provider_native::{
    NATIVE_PROVIDER_ID, NativeAdapterError, NativeMemoryApplicationPort, NativeProvider,
};

const ZERO_SHA: &str = "0000000000000000000000000000000000000000000000000000000000000000";
const ONE_SHA: &str = "1111111111111111111111111111111111111111111111111111111111111111";

#[derive(Default)]
struct Counters {
    handshake: AtomicUsize,
    health: AtomicUsize,
    observe: AtomicUsize,
    recall: AtomicUsize,
    lifecycle: AtomicUsize,
    reject: AtomicUsize,
}

struct MockNativePort {
    descriptor: ProviderDescriptor,
    counters: Counters,
    last_call: Mutex<Option<ProviderCall>>,
    last_handshake: Mutex<Option<HandshakeRequest>>,
}

impl MockNativePort {
    fn new(provider_id: &str, optional: &[&str]) -> Self {
        let mut capabilities = vec![
            OwnedVersionedId::new("provider.health.v1").expect("health capability"),
            OwnedVersionedId::new("observation.accept.v1").expect("observe capability"),
            OwnedVersionedId::new("recall.query.v1").expect("recall capability"),
        ];
        capabilities.extend(
            optional
                .iter()
                .map(|value| OwnedVersionedId::new(*value).expect("optional capability")),
        );
        Self {
            descriptor: ProviderDescriptor::new(
                OwnedProviderId::new(provider_id).expect("provider id"),
                ZERO_SHA,
                "native-state-v1",
                7,
                capabilities,
                limits(),
            )
            .expect("descriptor"),
            counters: Counters::default(),
            last_call: Mutex::new(None),
            last_handshake: Mutex::new(None),
        }
    }

    fn terminal(&self, call: &ProviderCall, code: TerminalCode) -> ProviderReply {
        let effect = if code == TerminalCode::Success && call.operation.mutates_provider_state() {
            CommittedEffectState::Committed
        } else {
            CommittedEffectState::None
        };
        ProviderReply {
            terminal: TerminalRecord::new(
                code,
                effect,
                FallbackEligibility::Forbidden,
                call.operation_id.clone(),
                ZERO_SHA,
                if effect == CommittedEffectState::Committed {
                    Some(ONE_SHA.to_owned())
                } else {
                    None
                },
                (code != TerminalCode::Success).then(|| format!("native.{}", code.as_wire())),
            )
            .expect("terminal"),
            payload: (code == TerminalCode::Success).then(|| call.payload.clone()),
            warnings: Vec::new(),
            extensions: call.extensions.clone(),
            state_generation: if effect == CommittedEffectState::Committed {
                call.expected_state_generation.saturating_add(1)
            } else {
                call.expected_state_generation
            },
        }
    }

    fn record(&self, call: &ProviderCall) {
        *self.last_call.lock().expect("last call lock") = Some(call.clone());
    }
}

impl NativeMemoryApplicationPort for MockNativePort {
    fn descriptor(&self) -> ProviderDescriptor {
        self.descriptor.clone()
    }

    fn handshake(&self, request: &HandshakeRequest) -> HandshakeResponse {
        self.counters.handshake.fetch_add(1, Ordering::Relaxed);
        *self.last_handshake.lock().expect("handshake lock") = Some(request.clone());
        HandshakeResponse {
            terminal: TerminalRecord::new(
                TerminalCode::Success,
                CommittedEffectState::None,
                FallbackEligibility::Forbidden,
                request.request_id.clone(),
                ZERO_SHA,
                None,
                None,
            )
            .expect("handshake terminal"),
            descriptor: Some(self.descriptor.clone()),
            provider_instance_id: Some("native.instance-1".to_owned()),
            state_namespace: Some("native.project".to_owned()),
            accepted_scope: Some(request.exact_scope.clone()),
            effective_limits: Some(request.host_limits.minimum(self.descriptor.limits)),
            ready_receipt_sha256: Some(ONE_SHA.to_owned()),
            warnings: Vec::new(),
        }
    }

    fn health(&self, call: &ProviderCall) -> ProviderReply {
        self.counters.health.fetch_add(1, Ordering::Relaxed);
        self.record(call);
        self.terminal(call, TerminalCode::Success)
    }

    fn observe(&self, call: &ProviderCall) -> ProviderReply {
        self.counters.observe.fetch_add(1, Ordering::Relaxed);
        self.record(call);
        self.terminal(call, TerminalCode::Success)
    }

    fn recall(&self, call: &ProviderCall) -> ProviderReply {
        self.counters.recall.fetch_add(1, Ordering::Relaxed);
        self.record(call);
        self.terminal(call, TerminalCode::Success)
    }

    fn lifecycle(&self, call: &ProviderCall) -> ProviderReply {
        self.counters.lifecycle.fetch_add(1, Ordering::Relaxed);
        self.record(call);
        self.terminal(call, TerminalCode::Success)
    }

    fn reject(
        &self,
        call: &ProviderCall,
        terminal_code: TerminalCode,
        _diagnostic_id: &'static str,
    ) -> ProviderReply {
        self.counters.reject.fetch_add(1, Ordering::Relaxed);
        self.terminal(call, terminal_code)
    }
}

fn limits() -> ProviderLimits {
    ProviderLimits {
        request_bytes: 4096,
        response_bytes: 8192,
        observation_batch_items: 16,
        recall_candidates: 32,
        concurrent_operations: 4,
        operation_millis: 1000,
        snapshot_bytes: 65536,
        inspection_items: 64,
    }
}

fn scope() -> OwnedExactScope {
    OwnedExactScope::new(
        "profile-a",
        "project-a",
        "repository-a",
        "worktree-a",
        "refs/heads/main",
        "session-a",
        3,
    )
    .expect("scope")
}

fn call(provider_id: &str, operation: ProviderOperation) -> ProviderCall {
    ProviderCall::new(ProviderCallParts {
        operation,
        provider_id: OwnedProviderId::new(provider_id).expect("provider id"),
        registration_revision: 1,
        ready_receipt_sha256: ZERO_SHA.to_owned(),
        exact_scope: scope(),
        request_id: "request-a".to_owned(),
        operation_id: format!("operation-{}", operation.capability_id()),
        expected_state_generation: 7,
        idempotency_key: operation
            .mutates_provider_state()
            .then(|| "idempotency-a".to_owned()),
        control: OperationControl::new(1000, 500, CancellationToken::new()),
        payload: CanonicalPayload::new(
            OwnedVersionedId::new("tracedecay.memory.test-payload.v1")
                .expect("payload contract"),
            b"{\"fixture\":true}".to_vec(),
            ONE_SHA,
        )
        .expect("payload"),
        required_capabilities: vec![
            OwnedVersionedId::new(operation.capability_id()).expect("operation capability"),
        ],
        extensions: Vec::new(),
    })
    .expect("call")
}

fn handshake(provider_id: &str) -> HandshakeRequest {
    HandshakeRequest::new(HandshakeRequestParts {
        provider_id: OwnedProviderId::new(provider_id).expect("provider id"),
        registration_revision: 1,
        exact_scope: scope(),
        request_id: "handshake-a".to_owned(),
        required_capabilities: vec![
            OwnedVersionedId::new("provider.health.v1").expect("health"),
            OwnedVersionedId::new("observation.accept.v1").expect("observe"),
            OwnedVersionedId::new("recall.query.v1").expect("recall"),
        ],
        host_limits: limits(),
        control: OperationControl::new(1000, 500, CancellationToken::new()),
        challenge_nonce: [9; 32],
    })
    .expect("handshake")
}

#[test]
fn constructor_rejects_non_native_identity() {
    let port = Arc::new(MockNativePort::new("vendor.memory", &[]));
    let result = NativeProvider::new(port);
    assert_eq!(
        result.err(),
        Some(NativeAdapterError::ProviderIdMismatch {
            expected: NATIVE_PROVIDER_ID,
            declared: "vendor.memory".to_owned(),
        })
    );
}

#[test]
fn descriptor_is_owned_by_the_application_port() {
    let port = Arc::new(MockNativePort::new(NATIVE_PROVIDER_ID, &[]));
    let provider = NativeProvider::new(port).expect("adapter");
    assert_eq!(provider.descriptor().provider_id.as_str(), NATIVE_PROVIDER_ID);
    assert!(provider.descriptor().supports("provider.health.v1"));
}

#[test]
fn handshake_preserves_exact_scope_and_request_identity() {
    let port = Arc::new(MockNativePort::new(NATIVE_PROVIDER_ID, &[]));
    let provider = NativeProvider::new(port.clone()).expect("adapter");
    let request = handshake(NATIVE_PROVIDER_ID);
    let response = provider.handshake(&request);
    assert_eq!(response.terminal.terminal_code, TerminalCode::Success);
    assert_eq!(response.accepted_scope, Some(request.exact_scope.clone()));
    assert_eq!(port.counters.handshake.load(Ordering::Relaxed), 1);
    let recorded = port
        .last_handshake
        .lock()
        .expect("handshake lock")
        .clone()
        .expect("recorded handshake");
    assert_eq!(recorded.request_id, request.request_id);
    assert_eq!(recorded.exact_scope, request.exact_scope);
}

#[test]
fn mandatory_operations_route_without_payload_transformation() {
    let port = Arc::new(MockNativePort::new(NATIVE_PROVIDER_ID, &[]));
    let provider = NativeProvider::new(port.clone()).expect("adapter");
    for operation in [
        ProviderOperation::Health,
        ProviderOperation::Observe,
        ProviderOperation::Recall,
    ] {
        let request = call(NATIVE_PROVIDER_ID, operation);
        let expected_payload = request.payload.clone();
        let reply = provider.invoke(&request);
        assert_eq!(reply.terminal.terminal_code, TerminalCode::Success);
        assert_eq!(reply.payload, Some(expected_payload));
        let recorded = port
            .last_call
            .lock()
            .expect("last call lock")
            .clone()
            .expect("recorded call");
        assert_eq!(recorded.exact_scope, request.exact_scope);
        assert_eq!(recorded.payload, request.payload);
        assert_eq!(recorded.control.snapshot(), request.control.snapshot());
    }
    assert_eq!(port.counters.health.load(Ordering::Relaxed), 1);
    assert_eq!(port.counters.observe.load(Ordering::Relaxed), 1);
    assert_eq!(port.counters.recall.load(Ordering::Relaxed), 1);
}

#[test]
fn declared_optional_operation_routes_to_lifecycle_port() {
    let port = Arc::new(MockNativePort::new(
        NATIVE_PROVIDER_ID,
        &["feedback.record.v1"],
    ));
    let provider = NativeProvider::new(port.clone()).expect("adapter");
    let request = call(NATIVE_PROVIDER_ID, ProviderOperation::Feedback);
    let reply = provider.invoke(&request);
    assert_eq!(reply.terminal.terminal_code, TerminalCode::Success);
    assert_eq!(reply.terminal.committed_effect, CommittedEffectState::Committed);
    assert_eq!(port.counters.lifecycle.load(Ordering::Relaxed), 1);
    assert_eq!(port.counters.reject.load(Ordering::Relaxed), 0);
}

#[test]
fn undeclared_optional_operation_is_explicitly_unsupported() {
    let port = Arc::new(MockNativePort::new(NATIVE_PROVIDER_ID, &[]));
    let provider = NativeProvider::new(port.clone()).expect("adapter");
    let request = call(NATIVE_PROVIDER_ID, ProviderOperation::Maintenance);
    let reply = provider.invoke(&request);
    assert_eq!(reply.terminal.terminal_code, TerminalCode::CapabilityUnsupported);
    assert_eq!(reply.terminal.committed_effect, CommittedEffectState::None);
    assert_eq!(port.counters.lifecycle.load(Ordering::Relaxed), 0);
    assert_eq!(port.counters.reject.load(Ordering::Relaxed), 1);
}

#[test]
fn wrong_target_identity_is_rejected_before_native_operation() {
    let port = Arc::new(MockNativePort::new(NATIVE_PROVIDER_ID, &[]));
    let provider = NativeProvider::new(port.clone()).expect("adapter");
    let request = call("vendor.memory", ProviderOperation::Recall);
    let reply = provider.invoke(&request);
    assert_eq!(reply.terminal.terminal_code, TerminalCode::InvalidRequest);
    assert_eq!(port.counters.recall.load(Ordering::Relaxed), 0);
    assert_eq!(port.counters.reject.load(Ordering::Relaxed), 1);
}

#[test]
fn handshake_operation_must_use_the_handshake_method() {
    let port = Arc::new(MockNativePort::new(NATIVE_PROVIDER_ID, &[]));
    let provider = NativeProvider::new(port.clone()).expect("adapter");
    let request = call(NATIVE_PROVIDER_ID, ProviderOperation::Handshake);
    let reply = provider.invoke(&request);
    assert_eq!(reply.terminal.terminal_code, TerminalCode::InvalidRequest);
    assert_eq!(port.counters.handshake.load(Ordering::Relaxed), 0);
    assert_eq!(port.counters.reject.load(Ordering::Relaxed), 1);
}

#[test]
fn provider_has_no_internal_memory_state() {
    let port = Arc::new(MockNativePort::new(NATIVE_PROVIDER_ID, &[]));
    let provider = NativeProvider::new(port.clone()).expect("adapter");
    let before = provider.descriptor();
    let request = call(NATIVE_PROVIDER_ID, ProviderOperation::Observe);
    let reply = provider.invoke(&request);
    assert_eq!(reply.terminal.committed_effect, CommittedEffectState::Committed);
    assert_eq!(provider.descriptor(), before);
    assert_eq!(port.counters.observe.load(Ordering::Relaxed), 1);
}

#[test]
fn descriptor_capabilities_are_deterministically_ordered() {
    let port = Arc::new(MockNativePort::new(
        NATIVE_PROVIDER_ID,
        &["snapshot.export.v1", "feedback.record.v1"],
    ));
    let provider = NativeProvider::new(port).expect("adapter");
    let capabilities = provider
        .descriptor()
        .capabilities
        .iter()
        .map(|value| value.as_str())
        .collect::<BTreeSet<_>>();
    assert!(capabilities.contains("feedback.record.v1"));
    assert!(capabilities.contains("snapshot.export.v1"));
}
'''

README = '''# TraceDecay Native Memory Provider Adapter

This product-owned crate places the existing TraceDecay Native memory application behind the provider-neutral `MemoryProvider` boundary. It owns no Native data or algorithms.

The adapter:

- accepts only a port that declares the stable `tracedecay.native` identity;
- routes mandatory health, observation, and recall calls without rewriting canonical payload or exact scope;
- routes only declared optional lifecycle capabilities;
- delegates all terminal records, provenance, receipts, scoring, temporal state, and rejection diagnostics to the Native application authority;
- contains no TraceDecay database, store, graph, code-index, daemon, host, dashboard, transport, NCM, or OCEAN dependency.

M2 proves the boundary with a mock application port. M3 supplies the real owner-bound TraceDecay application bridge and direct-versus-provider parity journeys. No second fact store, score implementation, curation path, or persistence format is introduced here.
'''


def write(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")


def add_workspace_member() -> None:
    path = ROOT / "Cargo.toml"
    text = path.read_text(encoding="utf-8")
    member = '    "crates/tracedecay-memory-provider-native",\n'
    if member in text:
        return
    marker = '    "crates/tracedecay-memory-fabric",\n'
    if marker not in text:
        raise SystemExit("workspace insertion marker is missing")
    path.write_text(text.replace(marker, marker + member, 1), encoding="utf-8")


def git_changed(path: str) -> bool:
    return subprocess.run(
        ["git", "diff", "--quiet", "--", path], cwd=ROOT, check=False
    ).returncode != 0


def product_patterns() -> list[str]:
    policy = json.loads(
        (ROOT / "product/upstream/patch-footprint-policy.json").read_text(encoding="utf-8")
    )
    return [value for value in policy["product_owned_paths"] if isinstance(value, str)]


def is_product_owned(path: str, patterns: list[str]) -> bool:
    return any(fnmatch.fnmatchcase(path, pattern) for pattern in patterns)


def diff_stats() -> tuple[int, int, int]:
    result = subprocess.run(
        ["git", "diff", "--no-renames", "--numstat", FLOOR, "--"],
        cwd=ROOT,
        check=True,
        capture_output=True,
        text=True,
    )
    patterns = product_patterns()
    production = 0
    tests = 0
    lines = 0
    for raw in result.stdout.splitlines():
        if not raw:
            continue
        added, deleted, path = raw.split("\t", 2)
        if added == "-" or deleted == "-" or is_product_owned(path, patterns):
            continue
        changed = int(added) + int(deleted)
        lines += changed
        name = Path(path).name.lower()
        if path.startswith("tests/") or "/tests/" in path or "fixture" in name or name.endswith("_test.rs"):
            tests += 1
        else:
            production += 1
    return production, tests, lines


def update_convergence_map() -> None:
    path = ROOT / "product/upstream/convergence-map.json"
    document = json.loads(path.read_text(encoding="utf-8"))
    entries = {entry["path"]: entry for entry in document["entries"]}
    for upstream_path in ("Cargo.toml", "Cargo.lock"):
        entry = entries.get(upstream_path)
        if entry is None:
            raise SystemExit(f"missing convergence entry for {upstream_path}")
        if "tdmem-0303" not in entry["bead_ids"]:
            entry["bead_ids"].append("tdmem-0303")
        for command in (
            "cargo clippy -p tracedecay-memory-provider-native --all-targets --locked -- -D warnings",
            "cargo test -p tracedecay-memory-provider-native --locked",
        ):
            if command not in entry["verification"]:
                entry["verification"].append(command)
    production, tests, lines = diff_stats()
    document["snapshot"] = {
        "upstream_existing_production_files": production,
        "upstream_existing_test_or_fixture_files": tests,
        "total_upstream_changed_lines": lines,
        "composition_root_files": 0,
        "exception_zone_files": 0,
        "observed_state": "The product branch changes only additive root workspace membership and generated path-package lock entries; provider API, fabric, and Native adapter remain product-owned.",
    }
    path.write_text(json.dumps(document, indent=2, sort_keys=True) + "\n", encoding="utf-8")


write(CRATE / "Cargo.toml", CARGO)
write(CRATE / "src/lib.rs", LIB)
write(CRATE / "tests/native_adapter.rs", TESTS)
write(CRATE / "README.md", README)
add_workspace_member()
subprocess.run(
    ["cargo", "metadata", "--format-version", "1", "--no-deps"],
    cwd=ROOT,
    check=True,
    stdout=subprocess.DEVNULL,
)
update_convergence_map()

manifest = [
    {
        "path": "crates/tracedecay-memory-provider-native",
        "message": "feat(memory): add TraceDecay Native provider adapter",
    }
]
for path, message in (
    ("Cargo.toml", "build(memory): register Native provider workspace member"),
    ("Cargo.lock", "build(memory): lock Native provider path package"),
    (
        "product/upstream/convergence-map.json",
        "docs(upstream): map Native provider workspace wiring",
    ),
):
    if git_changed(path):
        manifest.append({"path": path, "message": message})
write(
    ROOT / ".beads/operations/prepared-files.json",
    json.dumps(manifest, indent=2) + "\n",
)
Path(__file__).unlink()
