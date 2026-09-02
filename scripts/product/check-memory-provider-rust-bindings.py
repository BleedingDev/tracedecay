#!/usr/bin/env python3
"""Validate deterministic generated Rust bindings and prohibit duplicate wire types."""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import subprocess
import sys
import tempfile
from pathlib import Path
from typing import Any

DEFAULT_GENERATED = Path(
    "product/contracts/memory-provider-v1/generated/rust/memory_provider_v1.rs"
)
DEFAULT_MANIFEST = Path(
    "product/contracts/memory-provider-v1/generated/rust/manifest.json"
)
DEFAULT_SCAN_ROOTS = [
    Path("crates/tracedecay-memory-provider-api"),
    Path("crates/tracedecay-memory-provider-registry"),
    Path("crates/tracedecay-memory-provider-native"),
    Path("crates/tracedecay-memory-provider-ncm"),
    Path("crates/tracedecay-memory-observation"),
    Path("crates/tracedecay-memory-hygiene"),
    Path("crates/tracedecay-memory-context"),
    Path("crates/tracedecay-memory-conformance"),
]

EXPECTED_MANIFEST_FIELDS = {
    "schema_version",
    "manifest_id",
    "contract_set_id",
    "canonical_encoding",
    "generator_path",
    "generator_sha256",
    "contract_set_path",
    "contract_set_sha256",
    "output_path",
    "output_sha256",
    "output_bytes",
    "source_contracts",
    "contract_count",
    "capability_count",
    "mandatory_capability_count",
    "optional_capability_count",
    "enum_counts",
    "required_field_set_counts",
}

REQUIRED_TYPE_NAMES = {
    "CapabilityId",
    "CapabilityRequirement",
    "CapabilitySpec",
    "CancellationState",
    "CommittedEffectState",
    "CommittedEffectEvidence",
    "CommittedEffectExpectation",
    "ContractSpec",
    "ExactScopeIdentity",
    "FallbackEligibility",
    "FallbackDirective",
    "HandshakeReadinessState",
    "IdentifierError",
    "OpaqueExtension",
    "ProvenanceState",
    "ProviderId",
    "ProviderLimitSpec",
    "ProviderResolutionState",
    "PinnedFallbackPolicy",
    "RequestControl",
    "RetryClass",
    "TemporalMode",
    "TerminalCode",
    "TerminalCodePolicy",
    "TerminalSummary",
    "TerminalTextLimitSpec",
}

REQUIRED_CONSTANTS = {
    "CAPABILITIES",
    "CONTRACTS",
    "CONTRACT_SET_ID",
    "CONTRACT_SET_SHA256",
    "EXACT_SCOPE_DIGEST_ALGORITHM",
    "EXACT_SCOPE_DIGEST_DOMAIN",
    "EXACT_SCOPE_DIGEST_GOLDEN_SHA256",
    "EXACT_SCOPE_DIGEST_GOLDEN_STRINGS",
    "EXACT_SCOPE_DIGEST_OUTPUT_ENCODING",
    "EXACT_SCOPE_DIGEST_STRING_FIELD_ENCODING",
    "EXACT_SCOPE_DIGEST_STRING_FIELDS",
    "EXACT_SCOPE_REQUIRED_FIELDS",
    "GENERATOR_SHA256",
    "HANDSHAKE_REQUEST_REQUIRED_FIELDS",
    "HANDSHAKE_RESPONSE_REQUIRED_FIELDS",
    "LIFECYCLE_COMMON_REQUEST_REQUIRED_FIELDS",
    "OBSERVATION_REQUIRED_FIELDS",
    "PROVIDER_LIMITS",
    "PROVIDER_LIMIT_MAXIMA",
    "RECALL_CANDIDATE_REQUIRED_FIELDS",
    "RECALL_REQUEST_REQUIRED_FIELDS",
    "RECALL_RESPONSE_REQUIRED_FIELDS",
    "TERMINAL_ENVELOPE_REQUIRED_FIELDS",
    "TERMINAL_CODE_POLICIES",
    "TERMINAL_COMMITTED_BOUNDARY_MAX_BYTES",
    "TERMINAL_DIAGNOSTIC_ID_MAX_BYTES",
    "TERMINAL_EFFECT_ITEM_REF_MAX_BYTES",
    "TERMINAL_FALLBACK_POLICY_ID_MAX_BYTES",
    "TERMINAL_FALLBACK_REASON_MAX_BYTES",
    "TERMINAL_OPERATION_ID_MAX_BYTES",
    "TERMINAL_RECONCILIATION_ACTION_MAX_BYTES",
    "TERMINAL_TEXT_LIMITS",
}

FORBIDDEN_PATTERNS = {
    "unsafe block": re.compile(r"\bunsafe\s*\{"),
    "unsafe function": re.compile(r"\bunsafe\s+fn\b"),
    "unsafe impl": re.compile(r"\bunsafe\s+impl\b"),
    "unwrap": re.compile(r"\.unwrap\s*\("),
    "expect": re.compile(r"\.expect\s*\("),
    "panic macro": re.compile(r"\bpanic!\s*\("),
    "todo macro": re.compile(r"\btodo!\s*\("),
    "unimplemented macro": re.compile(r"\bunimplemented!\s*\("),
    "dbg macro": re.compile(r"\bdbg!\s*\("),
    "stdout print": re.compile(r"\bprintln!\s*\("),
    "stderr print": re.compile(r"\beprintln!\s*\("),
}

TYPE_DECLARATION_RE = re.compile(
    r"(?m)^\s*pub\s+(?:struct|enum|type|union)\s+([A-Za-z_][A-Za-z0-9_]*)"
)
CONSTANT_DECLARATION_RE = re.compile(
    r"(?m)^\s*pub\s+const\s+([A-Z][A-Z0-9_]*)\s*:"
)
SHA_RE = re.compile(r"^[0-9a-f]{64}$")

ALLOWED_OWNED_RUNTIME_TYPES = {
    "crates/tracedecay-memory-provider-api/src/lib.rs": {
        "CommittedEffectEvidence",
        "FallbackDirective",
        "PinnedFallbackPolicy",
    }
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--repo", type=Path, default=Path("."))
    parser.add_argument("--generated", type=Path, default=DEFAULT_GENERATED)
    parser.add_argument("--manifest", type=Path, default=DEFAULT_MANIFEST)
    parser.add_argument(
        "--scan-root",
        type=Path,
        action="append",
        default=None,
        help="Additional or replacement product Rust root to scan for duplicate wire types.",
    )
    return parser.parse_args()


def resolve(repo: Path, path: Path) -> Path:
    return path if path.is_absolute() else repo / path


def load_object(path: Path, label: str, errors: list[str]) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except OSError as exc:
        errors.append(f"could not read {label}: {exc}")
        return {}
    except json.JSONDecodeError as exc:
        errors.append(f"could not parse {label}: {exc}")
        return {}
    if not isinstance(value, dict):
        errors.append(f"{label} root must be an object")
        return {}
    return value


def sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def require_repo_file(repo: Path, raw: Any, label: str, errors: list[str]) -> Path | None:
    if not isinstance(raw, str) or not raw:
        errors.append(f"{label} must be a non-empty path")
        return None
    path = Path(raw)
    if path.is_absolute() or ".." in path.parts:
        errors.append(f"{label} must be repository-relative: {raw}")
        return None
    full = repo / path
    if not full.is_file():
        errors.append(f"{label} does not exist: {raw}")
        return None
    return full


def run(
    argv: list[str], cwd: Path, label: str, errors: list[str]
) -> subprocess.CompletedProcess[str] | None:
    try:
        result = subprocess.run(
            argv,
            cwd=cwd,
            check=False,
            capture_output=True,
            text=True,
        )
    except OSError as exc:
        errors.append(f"could not execute {label}: {exc}")
        return None
    if result.returncode != 0:
        output = result.stdout.strip() or result.stderr.strip()
        errors.append(f"{label} failed: {output}")
    return result


def validate_manifest(
    repo: Path,
    generated_path: Path,
    manifest: dict[str, Any],
    generated_bytes: bytes,
    errors: list[str],
) -> None:
    if set(manifest) != EXPECTED_MANIFEST_FIELDS:
        errors.append(
            "generated Rust manifest fields drifted; "
            f"missing={sorted(EXPECTED_MANIFEST_FIELDS - set(manifest))}, "
            f"extra={sorted(set(manifest) - EXPECTED_MANIFEST_FIELDS)}"
        )
    if manifest.get("schema_version") != 1:
        errors.append("generated Rust manifest schema_version must be 1")
    if manifest.get("manifest_id") != (
        "tracedecay.memory.provider.generated-rust.manifest.v1"
    ):
        errors.append("generated Rust manifest ID drifted")
    if manifest.get("contract_set_id") != (
        "tracedecay.memory.provider.contract-set.v1"
    ):
        errors.append("generated Rust manifest contract-set ID drifted")
    if manifest.get("canonical_encoding") != (
        "utf8_rust_source_without_bom_with_lf"
    ):
        errors.append("generated Rust canonical encoding drifted")
    if manifest.get("output_path") != str(generated_path.relative_to(repo)):
        errors.append("generated Rust manifest output path drifted")
    if manifest.get("output_sha256") != sha256_bytes(generated_bytes):
        errors.append("generated Rust output SHA-256 drifted")
    if manifest.get("output_bytes") != len(generated_bytes):
        errors.append("generated Rust output byte count drifted")

    generator_path = require_repo_file(
        repo, manifest.get("generator_path"), "generator_path", errors
    )
    contract_set_path = require_repo_file(
        repo, manifest.get("contract_set_path"), "contract_set_path", errors
    )
    if generator_path is not None:
        if manifest.get("generator_sha256") != sha256_bytes(
            generator_path.read_bytes()
        ):
            errors.append("generated Rust generator SHA-256 drifted")
    if contract_set_path is not None:
        contract_set = load_object(contract_set_path, "contract set", errors)
        canonical = json.dumps(
            contract_set,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        ).encode("utf-8")
        if manifest.get("contract_set_sha256") != sha256_bytes(canonical):
            errors.append("generated Rust contract-set SHA-256 drifted")

    for field in (
        "generator_sha256",
        "contract_set_sha256",
        "output_sha256",
    ):
        value = manifest.get(field)
        if not isinstance(value, str) or SHA_RE.fullmatch(value) is None:
            errors.append(f"generated Rust manifest {field} must be lowercase SHA-256")
    source_contracts = manifest.get("source_contracts")
    if not isinstance(source_contracts, list) or len(source_contracts) != 6:
        errors.append("generated Rust manifest must contain six source contracts")
    else:
        for index, row in enumerate(source_contracts):
            if not isinstance(row, dict):
                errors.append(f"source_contracts[{index}] must be an object")
                continue
            for digest_field in ("contract_sha256", "schema_sha256"):
                value = row.get(digest_field)
                if not isinstance(value, str) or SHA_RE.fullmatch(value) is None:
                    errors.append(
                        f"source_contracts[{index}].{digest_field} must be SHA-256"
                    )
    if manifest.get("contract_count") != 6:
        errors.append("generated Rust manifest contract count must be six")
    mandatory = manifest.get("mandatory_capability_count")
    optional = manifest.get("optional_capability_count")
    total = manifest.get("capability_count")
    if not all(isinstance(value, int) for value in (mandatory, optional, total)):
        errors.append("generated Rust capability counts must be integers")
    elif mandatory + optional != total or mandatory != 3 or optional < 1:
        errors.append("generated Rust capability counts are inconsistent")


def validate_source(generated_bytes: bytes, errors: list[str]) -> str:
    if generated_bytes.startswith(b"\xef\xbb\xbf"):
        errors.append("generated Rust source must not contain UTF-8 BOM")
    if not generated_bytes.endswith(b"\n"):
        errors.append("generated Rust source must end with LF")
    try:
        source = generated_bytes.decode("utf-8")
    except UnicodeDecodeError as exc:
        errors.append(f"generated Rust source is not UTF-8: {exc}")
        return ""
    required_header = (
        "// @generated by scripts/product/generate-memory-provider-rust.py; DO NOT EDIT."
    )
    if not source.startswith(required_header):
        errors.append("generated Rust source lacks canonical generated header")
    for lint in (
        "#![forbid(unsafe_code)]",
        "#![deny(warnings)]",
        "#![deny(missing_docs)]",
        "#![deny(clippy::unwrap_used)]",
        "#![deny(clippy::expect_used)]",
        "#![deny(clippy::panic)]",
    ):
        if lint not in source:
            errors.append(f"generated Rust source is missing lint {lint}")
    for label, pattern in FORBIDDEN_PATTERNS.items():
        if pattern.search(source):
            errors.append(f"generated Rust source contains forbidden {label}")
    type_names = set(TYPE_DECLARATION_RE.findall(source))
    missing_types = REQUIRED_TYPE_NAMES - type_names
    if missing_types:
        errors.append(f"generated Rust source is missing types {sorted(missing_types)}")
    constant_names = set(CONSTANT_DECLARATION_RE.findall(source))
    missing_constants = REQUIRED_CONSTANTS - constant_names
    if missing_constants:
        errors.append(
            f"generated Rust source is missing constants {sorted(missing_constants)}"
        )
    return source


def validate_generator_check(repo: Path, errors: list[str]) -> None:
    run(
        [
            "python3",
            "scripts/product/generate-memory-provider-rust.py",
            "--repo",
            ".",
            "--check",
        ],
        repo,
        "generated Rust zero-drift check",
        errors,
    )


def validate_rust_compilation(
    repo: Path, generated_path: Path, errors: list[str]
) -> None:
    with tempfile.TemporaryDirectory() as temporary:
        root = Path(temporary)
        library_path = root / "libtracedecay_memory_provider_v1.rlib"
        run(
            [
                "rustc",
                "--edition=2024",
                "--crate-name",
                "tracedecay_memory_provider_v1",
                "--crate-type=lib",
                "-Dwarnings",
                str(generated_path),
                "-o",
                str(library_path),
            ],
            repo,
            "generated Rust library compilation",
            errors,
        )
        probe_path = root / "probe.rs"
        generated_literal = json.dumps(str(generated_path))
        probe_path.write_text(
            "\n".join(
                [
                    f"#[path = {generated_literal}]",
                    "pub mod contract;",
                    "",
                    "fn main() -> Result<(), String> {",
                    "    let provider = contract::ProviderId::new(\"tracedecay.native\")",
                    "        .map_err(|error| error.to_string())?;",
                    "    if provider.as_str() != \"tracedecay.native\" {",
                    "        return Err(\"provider round-trip failed\".to_owned());",
                    "    }",
                    "    if contract::ProviderId::new(\"NCM\").is_ok() {",
                    "        return Err(\"non-canonical provider ID was accepted\".to_owned());",
                    "    }",
                    "    let capability = contract::CapabilityId::new(\"recall.query.v1\")",
                    "        .map_err(|error| error.to_string())?;",
                    "    if capability.as_str() != \"recall.query.v1\" {",
                    "        return Err(\"capability round-trip failed\".to_owned());",
                    "    }",
                    "    if contract::CapabilityId::new(\"recall.query\").is_ok() {",
                    "        return Err(\"unversioned capability was accepted\".to_owned());",
                    "    }",
                    "    let terminal = contract::TerminalCode::from_wire(\"effect_unknown\")",
                    "        .ok_or_else(|| \"terminal decode failed\".to_owned())?;",
                    "    if terminal.as_wire() != \"effect_unknown\" {",
                    "        return Err(\"terminal round-trip failed\".to_owned());",
                    "    }",
                    "    use contract::{CommittedEffectExpectation as E, FallbackEligibility as F, TerminalCode as T, TerminalCodePolicy as P};",
                    "    if contract::TERMINAL_CODE_POLICIES != &[",
                    "        P { terminal_code: T::Success, effect_expectation: E::OperationSpecific, maximum_fallback_eligibility: F::Forbidden },",
                    "        P { terminal_code: T::SuccessZeroResults, effect_expectation: E::None, maximum_fallback_eligibility: F::Forbidden },",
                    "        P { terminal_code: T::Partial, effect_expectation: E::NoneOrOperationSpecific, maximum_fallback_eligibility: F::Forbidden },",
                    "        P { terminal_code: T::InvalidRequest, effect_expectation: E::None, maximum_fallback_eligibility: F::Forbidden },",
                    "        P { terminal_code: T::Unauthorized, effect_expectation: E::None, maximum_fallback_eligibility: F::Forbidden },",
                    "        P { terminal_code: T::CapabilityUnsupported, effect_expectation: E::None, maximum_fallback_eligibility: F::ExplicitPolicyOnly },",
                    "        P { terminal_code: T::ScopeUnavailable, effect_expectation: E::None, maximum_fallback_eligibility: F::Forbidden },",
                    "        P { terminal_code: T::ScopeMismatch, effect_expectation: E::None, maximum_fallback_eligibility: F::Forbidden },",
                    "        P { terminal_code: T::StaleIdentity, effect_expectation: E::None, maximum_fallback_eligibility: F::Forbidden },",
                    "        P { terminal_code: T::Conflict, effect_expectation: E::None, maximum_fallback_eligibility: F::Forbidden },",
                    "        P { terminal_code: T::CapacityExceeded, effect_expectation: E::None, maximum_fallback_eligibility: F::Forbidden },",
                    "        P { terminal_code: T::DeadlineExceeded, effect_expectation: E::NonePartialOrUnknown, maximum_fallback_eligibility: F::Forbidden },",
                    "        P { terminal_code: T::Cancelled, effect_expectation: E::NonePartialOrUnknown, maximum_fallback_eligibility: F::Forbidden },",
                    "        P { terminal_code: T::ProviderUnavailable, effect_expectation: E::NoneOrUnknown, maximum_fallback_eligibility: F::ExplicitPolicyOnly },",
                    "        P { terminal_code: T::ResetRequired, effect_expectation: E::None, maximum_fallback_eligibility: F::Forbidden },",
                    "        P { terminal_code: T::StateIncompatible, effect_expectation: E::None, maximum_fallback_eligibility: F::Forbidden },",
                    "        P { terminal_code: T::PartialEffect, effect_expectation: E::Partial, maximum_fallback_eligibility: F::Forbidden },",
                    "        P { terminal_code: T::EffectUnknown, effect_expectation: E::Unknown, maximum_fallback_eligibility: F::Forbidden },",
                    "        P { terminal_code: T::ContractViolation, effect_expectation: E::NonePartialOrUnknown, maximum_fallback_eligibility: F::Forbidden },",
                    "        P { terminal_code: T::InternalFailure, effect_expectation: E::NonePartialOrUnknown, maximum_fallback_eligibility: F::Forbidden },",
                    "    ] {",
                    "        return Err(\"terminal code policy table drifted\".to_owned());",
                    "    }",
                    "    let committed_refs = [\"item-1\".to_owned()];",
                    "    let effect = contract::CommittedEffectEvidence {",
                    "        state: contract::CommittedEffectState::Committed,",
                    "        committed_boundary: None,",
                    "        state_generation_before: Some(1),",
                    "        state_generation_after: Some(2),",
                    "        committed_item_refs: &committed_refs,",
                    "        uncommitted_item_refs: &[],",
                    "        provider_receipt_digest: Some(\"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\"),",
                    "        reconciliation_action: None,",
                    "        verification_digest: Some(\"bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb\"),",
                    "        duplicate_of_idempotency_key: None,",
                    "        duplicate_of_operation_id: None,",
                    "    };",
                    "    let duplicate = contract::CommittedEffectEvidence {",
                    "        state: contract::CommittedEffectState::Duplicate,",
                    "        committed_boundary: None,",
                    "        state_generation_before: Some(2),",
                    "        state_generation_after: Some(2),",
                    "        committed_item_refs: &[],",
                    "        uncommitted_item_refs: &[],",
                    "        provider_receipt_digest: Some(\"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\"),",
                    "        reconciliation_action: None,",
                    "        verification_digest: None,",
                    "        duplicate_of_idempotency_key: Some(\"dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd\"),",
                    "        duplicate_of_operation_id: Some(\"observe-operation-1\"),",
                    "    };",
                    "    if contract::CommittedEffectState::from_wire(\"duplicate\") != Some(contract::CommittedEffectState::Duplicate)",
                    "        || contract::CommittedEffectState::Duplicate.as_wire() != \"duplicate\"",
                    "        || duplicate.duplicate_of_idempotency_key.is_none()",
                    "        || duplicate.duplicate_of_operation_id.is_none()",
                    "        || duplicate.state_generation_before != duplicate.state_generation_after",
                    "    {",
                    "        return Err(\"duplicate committed-effect binding drifted\".to_owned());",
                    "    }",
                    "    let fallback = contract::FallbackDirective { eligibility: F::Forbidden, policy: None, reason: None };",
                    "    let summary = contract::TerminalSummary { operation_kind: \"recall\", provider_id: \"tracedecay.native\", terminal_code: T::Success, committed_effect: effect, fallback, operation_id: \"operation-1\", exact_scope_digest: \"cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc\", diagnostic_id: None };",
                    "    if summary.committed_effect.committed_item_refs != &[\"item-1\"]",
                    "        || summary.fallback.eligibility != F::Forbidden",
                    "        || summary.operation_kind != \"recall\"",
                    "        || summary.provider_id != \"tracedecay.native\"",
                    "    {",
                    "        return Err(\"terminal structured binding probe failed\".to_owned());",
                    "    }",
                    "    if contract::TERMINAL_TEXT_LIMITS != &[",
                    "        contract::TerminalTextLimitSpec { field: \"operation_id\", maximum_bytes: 256 },",
                    "        contract::TerminalTextLimitSpec { field: \"committed_boundary\", maximum_bytes: 256 },",
                    "        contract::TerminalTextLimitSpec { field: \"effect_item_ref\", maximum_bytes: 256 },",
                    "        contract::TerminalTextLimitSpec { field: \"reconciliation_action\", maximum_bytes: 512 },",
                    "        contract::TerminalTextLimitSpec { field: \"fallback_policy_id\", maximum_bytes: 128 },",
                    "        contract::TerminalTextLimitSpec { field: \"fallback_reason\", maximum_bytes: 512 },",
                    "        contract::TerminalTextLimitSpec { field: \"diagnostic_id\", maximum_bytes: 128 },",
                    "    ] {",
                    "        return Err(\"terminal text limit catalog drifted\".to_owned());",
                    "    }",
                    "    if contract::CAPABILITIES.len() < 4 || contract::CONTRACTS.len() != 6 {",
                    "        return Err(\"generated authority counts are invalid\".to_owned());",
                    "    }",
                    "    if contract::PROVIDER_LIMITS != &[",
                    "        contract::ProviderLimitSpec { limit_id: \"request_bytes\", minimum: 1, maximum: 16_777_216, unit: \"bytes\" },",
                    "        contract::ProviderLimitSpec { limit_id: \"response_bytes\", minimum: 1, maximum: 33_554_432, unit: \"bytes\" },",
                    "        contract::ProviderLimitSpec { limit_id: \"observation_batch_items\", minimum: 1, maximum: 4_096, unit: \"items\" },",
                    "        contract::ProviderLimitSpec { limit_id: \"recall_candidates\", minimum: 1, maximum: 10_000, unit: \"items\" },",
                    "        contract::ProviderLimitSpec { limit_id: \"concurrent_operations\", minimum: 1, maximum: 1_024, unit: \"operations\" },",
                    "        contract::ProviderLimitSpec { limit_id: \"operation_millis\", minimum: 1, maximum: 3_600_000, unit: \"milliseconds\" },",
                    "        contract::ProviderLimitSpec { limit_id: \"snapshot_bytes\", minimum: 1, maximum: 1_073_741_824, unit: \"bytes\" },",
                    "        contract::ProviderLimitSpec { limit_id: \"inspection_items\", minimum: 1, maximum: 100_000, unit: \"items\" },",
                    "    ] {",
                    "        return Err(\"provider limit catalog drifted\".to_owned());",
                    "    }",
                    "    if contract::EXACT_SCOPE_DIGEST_ALGORITHM != \"sha256\"",
                    "        || contract::EXACT_SCOPE_DIGEST_DOMAIN != b\"tracedecay.memory-provider.exact-scope.v1\\0\"",
                    "        || contract::EXACT_SCOPE_DIGEST_STRING_FIELDS != &[",
                    "            \"profile_id\",",
                    "            \"project_id\",",
                    "            \"repository_identity\",",
                    "            \"worktree_identity\",",
                    "            \"branch_identity\",",
                    "            \"agent_session_id\",",
                    "            \"resolved_scope_digest\",",
                    "        ]",
                    "        || contract::EXACT_SCOPE_DIGEST_STRING_FIELD_ENCODING",
                    "            != \"u64_big_endian_byte_length_then_utf8_bytes\"",
                    "        || contract::EXACT_SCOPE_DIGEST_OUTPUT_ENCODING != \"lowercase_hex_64\"",
                    "        || contract::EXACT_SCOPE_DIGEST_GOLDEN_STRINGS != &[",
                    "            \"profile-1\",",
                    "            \"project-1\",",
                    "            \"repo-1\",",
                    "            \"worktree-1\",",
                    "            \"refs/heads/main\",",
                    "            \"session-1\",",
                    "            \"sha256:1111111111111111111111111111111111111111111111111111111111111111\",",
                    "        ]",
                    "        || contract::EXACT_SCOPE_DIGEST_GOLDEN_SHA256",
                    "            != \"2f525c8c3d59bfa3d9729405c4f3f1307fade77494b6ddf251c89abc490f0a52\"",
                    "    {",
                    "        return Err(\"exact-scope digest contract drifted\".to_owned());",
                    "    }",
                    "    Ok(())",
                    "}",
                    "",
                ]
            ),
            encoding="utf-8",
        )
        binary_path = root / "probe"
        compile_result = run(
            [
                "rustc",
                "--edition=2024",
                "-Dwarnings",
                str(probe_path),
                "-o",
                str(binary_path),
            ],
            repo,
            "generated Rust probe compilation",
            errors,
        )
        if compile_result is not None and compile_result.returncode == 0:
            run([str(binary_path)], repo, "generated Rust probe execution", errors)


def validate_duplicate_types(
    repo: Path, generated_path: Path, scan_roots: list[Path], errors: list[str]
) -> int:
    declarations: dict[str, list[str]] = {name: [] for name in REQUIRED_TYPE_NAMES}
    scanned = 0
    generated_resolved = generated_path.resolve()
    for raw_root in scan_roots:
        root = resolve(repo, raw_root)
        if not root.exists():
            continue
        paths = [root] if root.is_file() else sorted(root.rglob("*.rs"))
        for path in paths:
            if not path.is_file() or path.resolve() == generated_resolved:
                continue
            scanned += 1
            try:
                source = path.read_text(encoding="utf-8")
            except OSError as exc:
                errors.append(f"could not read duplicate-scan path {path}: {exc}")
                continue
            for type_name in TYPE_DECLARATION_RE.findall(source):
                if type_name in declarations:
                    try:
                        display = str(path.relative_to(repo))
                    except ValueError:
                        display = str(path)
                    if type_name in ALLOWED_OWNED_RUNTIME_TYPES.get(display, set()):
                        continue
                    declarations[type_name].append(display)
    duplicates = {
        type_name: paths for type_name, paths in declarations.items() if paths
    }
    if duplicates:
        errors.append(
            "duplicate hand-maintained wire/domain type declarations found: "
            + json.dumps(duplicates, sort_keys=True)
        )
    return scanned


def validate(
    repo: Path,
    generated_path: Path,
    manifest_path: Path,
    scan_roots: list[Path],
) -> tuple[list[str], int]:
    errors: list[str] = []
    try:
        generated_bytes = generated_path.read_bytes()
    except OSError as exc:
        errors.append(f"could not read generated Rust source: {exc}")
        generated_bytes = b""
    manifest = load_object(manifest_path, "generated Rust manifest", errors)
    if generated_bytes:
        validate_source(generated_bytes, errors)
        validate_manifest(repo, generated_path, manifest, generated_bytes, errors)
    validate_generator_check(repo, errors)
    if generated_bytes:
        validate_rust_compilation(repo, generated_path, errors)
    scanned = validate_duplicate_types(repo, generated_path, scan_roots, errors)
    return errors, scanned


def main() -> int:
    args = parse_args()
    repo = args.repo.resolve()
    generated_path = resolve(repo, args.generated)
    manifest_path = resolve(repo, args.manifest)
    scan_roots = args.scan_root if args.scan_root is not None else DEFAULT_SCAN_ROOTS
    errors, scanned = validate(
        repo,
        generated_path,
        manifest_path,
        scan_roots,
    )
    if errors:
        print(json.dumps({"ok": False, "errors": errors}, indent=2, sort_keys=True))
        return 1
    manifest = load_object(manifest_path, "generated Rust manifest", [])
    print(
        json.dumps(
            {
                "ok": True,
                "manifest_id": manifest.get("manifest_id"),
                "contract_set_id": manifest.get("contract_set_id"),
                "output_sha256": manifest.get("output_sha256"),
                "contract_count": manifest.get("contract_count"),
                "capability_count": manifest.get("capability_count"),
                "mandatory_capability_count": manifest.get(
                    "mandatory_capability_count"
                ),
                "optional_capability_count": manifest.get(
                    "optional_capability_count"
                ),
                "duplicate_scan_files": scanned,
                "rustc_compile": "passed",
                "probe_execution": "passed",
            },
            indent=2,
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
