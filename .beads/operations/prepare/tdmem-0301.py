#!/usr/bin/env python3
"""Retry tdmem-0301 with canonical formatting and bounded constructors."""

from __future__ import annotations

import subprocess
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
BODY = Path(__file__).with_name("_tdmem-0301-body.py")
SOURCE_COMMIT = "fab219ad8b29956d21489779a111f00902b60032"
SOURCE_PATH = ".beads/operations/prepare/tdmem-0301.py"

source = subprocess.check_output(
    ["git", "show", f"{SOURCE_COMMIT}:{SOURCE_PATH}"],
    cwd=ROOT,
    text=True,
)
BODY.write_text(source, encoding="utf-8")
subprocess.run(["python3", str(BODY)], cwd=ROOT, check=True)

crate_root = ROOT / "crates/tracedecay-memory-provider-api"
api_test = crate_root / "tests/api.rs"
test_text = api_test.read_text(encoding="utf-8")
old_assertion = '''    assert_eq!(
        ProviderCall::new(missing_capability),
        Err(ApiError::MissingOperationCapability("recall.query.v1"))
    );
'''
new_assertion = '''    assert!(matches!(
        ProviderCall::new(missing_capability),
        Err(ApiError::MissingOperationCapability("recall.query.v1"))
    ));
'''
if old_assertion not in test_text:
    raise SystemExit("provider API assertion patch marker is missing")
test_text = test_text.replace(old_assertion, new_assertion, 1)

test_text = test_text.replace(
    "    ApiError, CancellationToken, CanonicalPayload, HandshakeRequest, HandshakeResponse,\n",
    "    ApiError, CancellationToken, CanonicalPayload, HandshakeRequest, HandshakeRequestParts,\n    HandshakeResponse,\n",
    1,
)
old_handshake_call = '''    let handshake = HandshakeRequest::new(
        provider_id(),
        1,
        scope(),
        "handshake-request",
        [
            capability("provider.health.v1"),
            capability("observation.accept.v1"),
            capability("recall.query.v1"),
        ],
        limits(),
        OperationControl::new(123, 10, CancellationToken::new()),
        [7; 32],
    )
    .expect("test handshake is valid");
'''
new_handshake_call = '''    let handshake = HandshakeRequest::new(HandshakeRequestParts {
        provider_id: provider_id(),
        registration_revision: 1,
        exact_scope: scope(),
        request_id: "handshake-request".to_owned(),
        required_capabilities: vec![
            capability("provider.health.v1"),
            capability("observation.accept.v1"),
            capability("recall.query.v1"),
        ],
        host_limits: limits(),
        control: OperationControl::new(123, 10, CancellationToken::new()),
        challenge_nonce: [7; 32],
    })
    .expect("test handshake is valid");
'''
if old_handshake_call not in test_text:
    raise SystemExit("provider API handshake test patch marker is missing")
api_test.write_text(
    test_text.replace(old_handshake_call, new_handshake_call, 1),
    encoding="utf-8",
)

lib_path = crate_root / "src/lib.rs"
lib_text = lib_path.read_text(encoding="utf-8")
impl_start = lib_text.find("impl HandshakeRequest {\n")
impl_end_marker = "\n}\n\n/// Successful or failed handshake response."
impl_end = lib_text.find(impl_end_marker, impl_start)
if impl_start < 0 or impl_end < 0:
    raise SystemExit("provider API handshake implementation markers are missing")
replacement = '''/// Builder payload for one provider handshake request.
#[derive(Clone, Debug)]
pub struct HandshakeRequestParts {
    /// Selected provider identity.
    pub provider_id: OwnedProviderId,
    /// Accepted registration revision.
    pub registration_revision: u64,
    /// Exact TraceDecay-owned scope.
    pub exact_scope: OwnedExactScope,
    /// Stable request identity.
    pub request_id: String,
    /// Mandatory requested capabilities.
    pub required_capabilities: Vec<OwnedVersionedId>,
    /// Finite host ceilings.
    pub host_limits: ProviderLimits,
    /// Live request control.
    pub control: OperationControl,
    /// Canonical 32-byte challenge nonce.
    pub challenge_nonce: [u8; 32],
}

impl HandshakeRequest {
    /// Validates one handshake request assembled from explicit parts.
    pub fn new(parts: HandshakeRequestParts) -> Result<Self, ApiError> {
        require_non_empty(&parts.request_id, "request_id")?;
        let mut capability_set = BTreeSet::new();
        for capability in parts.required_capabilities {
            let capability_name = capability.as_str().to_owned();
            if !capability_set.insert(capability) {
                return Err(ApiError::DuplicateCapability(capability_name));
            }
        }
        Ok(Self {
            provider_id: parts.provider_id,
            registration_revision: parts.registration_revision,
            exact_scope: parts.exact_scope,
            request_id: parts.request_id,
            required_capabilities: capability_set,
            host_limits: parts.host_limits.validate()?,
            control: parts.control,
            challenge_nonce: parts.challenge_nonce,
        })
    }
}'''
lib_path.write_text(
    lib_text[:impl_start] + replacement + lib_text[impl_end + 2 :],
    encoding="utf-8",
)

subprocess.run(
    ["cargo", "metadata", "--format-version", "1"],
    cwd=ROOT,
    check=True,
    stdout=subprocess.DEVNULL,
)
subprocess.run(
    ["cargo", "fmt", "--package", "tracedecay-memory-provider-api"],
    cwd=ROOT,
    check=True,
)

Path(__file__).unlink()
