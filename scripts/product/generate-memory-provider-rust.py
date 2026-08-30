#!/usr/bin/env python3
"""Generate or verify dependency-free Rust bindings from the canonical M1 contracts."""

from __future__ import annotations

import argparse
import difflib
import hashlib
import json
import keyword
import re
import sys
from pathlib import Path
from typing import Any, Iterable

DEFAULT_CONTRACT_SET = Path(
    "product/contracts/memory-provider-v1/contract-set.json"
)
DEFAULT_OUTPUT_DIR = Path(
    "product/contracts/memory-provider-v1/generated/rust"
)

EXPECTED_CONTRACT_IDS = [
    "tracedecay.memory.provider.registry.v1",
    "tracedecay.memory.provider.handshake.v1",
    "tracedecay.memory.provider.observation.v1",
    "tracedecay.memory.provider.recall.v1",
    "tracedecay.memory.provider.lifecycle.v1",
    "tracedecay.memory.provider.terminal.v1",
]

EXPECTED_PROVIDER_LIMIT_IDENTITIES = [
    ("request_bytes", 1, "bytes"),
    ("response_bytes", 1, "bytes"),
    ("observation_batch_items", 1, "items"),
    ("recall_candidates", 1, "items"),
    ("concurrent_operations", 1, "operations"),
    ("operation_millis", 1, "milliseconds"),
    ("snapshot_bytes", 1, "bytes"),
    ("inspection_items", 1, "items"),
]

EXPECTED_TERMINAL_TEXT_LIMITS = [
    ("operation_id", "TERMINAL_OPERATION_ID_MAX_BYTES", 256),
    ("committed_boundary", "TERMINAL_COMMITTED_BOUNDARY_MAX_BYTES", 256),
    ("effect_item_ref", "TERMINAL_EFFECT_ITEM_REF_MAX_BYTES", 256),
    ("reconciliation_action", "TERMINAL_RECONCILIATION_ACTION_MAX_BYTES", 512),
    ("fallback_policy_id", "TERMINAL_FALLBACK_POLICY_ID_MAX_BYTES", 128),
    ("fallback_reason", "TERMINAL_FALLBACK_REASON_MAX_BYTES", 512),
    ("diagnostic_id", "TERMINAL_DIAGNOSTIC_ID_MAX_BYTES", 128),
]


class GenerationError(RuntimeError):
    """Raised when a source contract cannot produce deterministic Rust bindings."""


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--repo", type=Path, default=Path("."))
    parser.add_argument("--contract-set", type=Path, default=DEFAULT_CONTRACT_SET)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--write", action="store_true")
    mode.add_argument("--check", action="store_true")
    return parser.parse_args()


def resolve(repo: Path, path: Path) -> Path:
    return path if path.is_absolute() else repo / path


def load_json(path: Path, label: str) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except OSError as exc:
        raise GenerationError(f"could not read {label} {path}: {exc}") from exc
    except json.JSONDecodeError as exc:
        raise GenerationError(f"could not parse {label} {path}: {exc}") from exc
    if not isinstance(value, dict):
        raise GenerationError(f"{label} root must be an object")
    return value


def canonical_bytes(value: Any) -> bytes:
    try:
        encoded = json.dumps(
            value,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        )
    except (TypeError, ValueError) as exc:
        raise GenerationError(f"value is not canonical JSON: {exc}") from exc
    return encoded.encode("utf-8")


def sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def canonical_sha(value: Any) -> str:
    return sha256_bytes(canonical_bytes(value))


def require_relative_file(repo: Path, raw: Any, label: str) -> Path:
    if not isinstance(raw, str) or not raw:
        raise GenerationError(f"{label} must be a non-empty repository-relative path")
    path = Path(raw)
    if path.is_absolute() or ".." in path.parts:
        raise GenerationError(f"{label} must be repository-relative: {raw}")
    full = repo / path
    if not full.is_file():
        raise GenerationError(f"{label} does not exist: {raw}")
    return full


def rust_string(value: str) -> str:
    return json.dumps(value, ensure_ascii=False)


def rust_identifier(value: str) -> str:
    pieces = re.split(r"[^A-Za-z0-9]+", value)
    name = "".join(piece[:1].upper() + piece[1:] for piece in pieces if piece)
    if not name:
        raise GenerationError(f"cannot derive Rust identifier from {value!r}")
    if name[0].isdigit():
        name = f"Value{name}"
    if keyword.iskeyword(name.lower()):
        name = f"Value{name}"
    return name


def ensure_unique_variants(values: Iterable[str], label: str) -> list[tuple[str, str]]:
    result: list[tuple[str, str]] = []
    seen: dict[str, str] = {}
    for value in values:
        if not isinstance(value, str) or not value:
            raise GenerationError(f"{label} contains an empty non-string value")
        variant = rust_identifier(value)
        previous = seen.get(variant)
        if previous is not None:
            raise GenerationError(
                f"{label} values {previous!r} and {value!r} collide as {variant}"
            )
        seen[variant] = value
        result.append((variant, value))
    return result


def load_contracts(
    repo: Path, contract_set_path: Path
) -> tuple[dict[str, Any], list[dict[str, Any]], dict[str, dict[str, Any]]]:
    contract_set = load_json(contract_set_path, "contract set")
    if contract_set.get("schema_version") != 1:
        raise GenerationError("contract-set schema_version must be 1")
    if contract_set.get("contract_set_id") != (
        "tracedecay.memory.provider.contract-set.v1"
    ):
        raise GenerationError("contract-set ID drifted")
    if contract_set.get("status") != "accepted":
        raise GenerationError("contract-set must be accepted")
    entries = contract_set.get("contracts")
    if not isinstance(entries, list) or len(entries) != 6:
        raise GenerationError("contract-set must contain exactly six contracts")
    actual_ids = [entry.get("contract_id") for entry in entries if isinstance(entry, dict)]
    if actual_ids != EXPECTED_CONTRACT_IDS:
        raise GenerationError("contract-set order or IDs drifted")

    contracts: dict[str, dict[str, Any]] = {}
    digests: list[dict[str, Any]] = []
    for expected_order, entry in enumerate(entries, start=1):
        if not isinstance(entry, dict):
            raise GenerationError("contract-set entry must be an object")
        if entry.get("order") != expected_order:
            raise GenerationError("contract-set order must be contiguous")
        contract_id = entry.get("contract_id")
        if not isinstance(contract_id, str):
            raise GenerationError("contract-set entry has no contract ID")
        contract_path = require_relative_file(
            repo, entry.get("contract_path"), f"{contract_id}.contract_path"
        )
        schema_path = require_relative_file(
            repo, entry.get("schema_path"), f"{contract_id}.schema_path"
        )
        contract = load_json(contract_path, contract_id)
        schema = load_json(schema_path, f"{contract_id} schema")
        if contract.get("contract_id") != contract_id:
            raise GenerationError(f"{contract_id} identity mismatch")
        if contract.get("bead_id") != entry.get("bead_id"):
            raise GenerationError(f"{contract_id} bead mismatch")
        if contract.get("status") != "accepted":
            raise GenerationError(f"{contract_id} is not accepted")
        if schema.get("additionalProperties") is not False:
            raise GenerationError(f"{contract_id} schema root is not strict")
        properties = schema.get("properties")
        if not isinstance(properties, dict):
            raise GenerationError(f"{contract_id} schema has no properties")
        if properties.get("contract_id", {}).get("const") != contract_id:
            raise GenerationError(f"{contract_id} schema does not pin contract ID")
        contracts[contract_id] = contract
        digests.append(
            {
                "order": expected_order,
                "contract_id": contract_id,
                "bead_id": entry["bead_id"],
                "contract_path": entry["contract_path"],
                "schema_path": entry["schema_path"],
                "contract_sha256": canonical_sha(contract),
                "schema_sha256": canonical_sha(schema),
            }
        )
    return contract_set, digests, contracts


def registry_capabilities(registry: dict[str, Any]) -> list[dict[str, Any]]:
    authority = registry.get("capability_registry")
    if not isinstance(authority, dict):
        raise GenerationError("registry contract has no capability_registry authority")
    result: list[dict[str, Any]] = []
    for requirement in ("mandatory", "optional"):
        rows = authority.get(requirement)
        if not isinstance(rows, list):
            raise GenerationError(f"capability_registry.{requirement} must be an array")
        for row in rows:
            if not isinstance(row, dict):
                raise GenerationError("capability row must be an object")
            capability_id = row.get("id")
            if not isinstance(capability_id, str) or not capability_id:
                raise GenerationError("capability row has no ID")
            if row.get("requirement") != requirement:
                raise GenerationError(
                    f"capability {capability_id} requirement mismatch"
                )
            if row.get("advisory_only") is not True:
                raise GenerationError(f"capability {capability_id} must be advisory")
            if row.get("may_mutate_tracedecay_authority") is not False:
                raise GenerationError(
                    f"capability {capability_id} may mutate TraceDecay authority"
                )
            result.append(
                {
                    "id": capability_id,
                    "requirement": requirement,
                    "class": row.get("class"),
                    "detailed_contract_bead": row.get("detailed_contract_bead"),
                    "advisory_only": True,
                    "may_mutate_tracedecay_authority": False,
                }
            )
    ids = [row["id"] for row in result]
    if len(ids) != len(set(ids)):
        raise GenerationError("capability IDs are not unique")
    return result


def list_strings(value: Any, label: str) -> list[str]:
    if not isinstance(value, list) or any(not isinstance(item, str) for item in value):
        raise GenerationError(f"{label} must be an array of strings")
    if len(value) != len(set(value)):
        raise GenerationError(f"{label} contains duplicates")
    return list(value)


def required_fields(contracts: dict[str, dict[str, Any]]) -> dict[str, list[str]]:
    handshake = contracts["tracedecay.memory.provider.handshake.v1"]
    observation = contracts["tracedecay.memory.provider.observation.v1"]
    recall = contracts["tracedecay.memory.provider.recall.v1"]
    lifecycle = contracts["tracedecay.memory.provider.lifecycle.v1"]
    terminal = contracts["tracedecay.memory.provider.terminal.v1"]
    return {
        "HANDSHAKE_REQUEST_REQUIRED_FIELDS": list_strings(
            handshake.get("handshake_request", {}).get("required_fields"),
            "handshake request fields",
        ),
        "HANDSHAKE_RESPONSE_REQUIRED_FIELDS": list_strings(
            handshake.get("handshake_response", {}).get("required_fields"),
            "handshake response fields",
        ),
        "EXACT_SCOPE_REQUIRED_FIELDS": list_strings(
            handshake.get("exact_scope_identity", {}).get("required_fields"),
            "exact scope fields",
        ),
        "OBSERVATION_REQUIRED_FIELDS": list_strings(
            observation.get("observation_envelope", {}).get("required_fields"),
            "observation fields",
        ),
        "RECALL_REQUEST_REQUIRED_FIELDS": list_strings(
            recall.get("recall_request", {}).get("required_fields"),
            "recall request fields",
        ),
        "RECALL_CANDIDATE_REQUIRED_FIELDS": list_strings(
            recall.get("provider_candidate", {}).get("required_fields"),
            "recall candidate fields",
        ),
        "RECALL_RESPONSE_REQUIRED_FIELDS": list_strings(
            recall.get("recall_response", {}).get("required_fields"),
            "recall response fields",
        ),
        "LIFECYCLE_COMMON_REQUEST_REQUIRED_FIELDS": list_strings(
            lifecycle.get("common_request", {}).get("required_fields"),
            "lifecycle common request fields",
        ),
        "TERMINAL_ENVELOPE_REQUIRED_FIELDS": list_strings(
            terminal.get("terminal_envelope", {}).get("required_fields"),
            "terminal envelope fields",
        ),
    }


def enum_sources(contracts: dict[str, dict[str, Any]]) -> dict[str, list[str]]:
    registry = contracts["tracedecay.memory.provider.registry.v1"]
    handshake = contracts["tracedecay.memory.provider.handshake.v1"]
    recall = contracts["tracedecay.memory.provider.recall.v1"]
    terminal = contracts["tracedecay.memory.provider.terminal.v1"]
    terminal_rows = terminal.get("terminal_codes")
    if not isinstance(terminal_rows, list) or not terminal_rows:
        raise GenerationError("terminal code table must be a non-empty array")
    effect_expectations: list[str] = []
    for index, row in enumerate(terminal_rows):
        if not isinstance(row, dict):
            raise GenerationError(f"terminal code row {index} must be an object")
        expectation = row.get("effect_expectation")
        if not isinstance(expectation, str) or not expectation:
            raise GenerationError(
                f"terminal code row {index} has no effect expectation"
            )
        if expectation not in effect_expectations:
            effect_expectations.append(expectation)
    return {
        "ProviderResolutionState": list_strings(
            registry.get("selection_contract", {}).get("resolution_states"),
            "provider resolution states",
        ),
        "HandshakeReadinessState": list_strings(
            handshake.get("readiness_states"), "handshake readiness states"
        ),
        "TemporalMode": list_strings(
            recall.get("temporal_query", {}).get("modes"), "temporal modes"
        ),
        "ProvenanceState": list_strings(
            recall.get("provenance", {}).get("states"), "provenance states"
        ),
        "TerminalCode": [
            row["code"]
            for row in terminal_rows
            if isinstance(row, dict) and isinstance(row.get("code"), str)
        ],
        "CommittedEffectExpectation": effect_expectations,
        "RetryClass": list_strings(
            terminal.get("retry", {}).get("classes"), "retry classes"
        ),
        "FallbackEligibility": list_strings(
            terminal.get("fallback", {}).get("eligibility_values"),
            "fallback eligibility",
        ),
        "CommittedEffectState": list_strings(
            terminal.get("committed_effect", {}).get("states"),
            "committed effect states",
        ),
    }


def terminal_code_policies(
    terminal: dict[str, Any],
    terminal_codes: list[str],
    effect_expectations: list[str],
    fallback_values: list[str],
) -> list[dict[str, str]]:
    """Validate and retain the semantic cross-field policy for every terminal code."""
    rows = terminal.get("terminal_codes")
    if not isinstance(rows, list) or len(rows) != len(terminal_codes):
        raise GenerationError("terminal code policy table shape drifted")
    expected_fields = {
        "code",
        "class",
        "effect_expectation",
        "retry_class",
        "fallback_eligibility",
    }
    policies: list[dict[str, str]] = []
    for index, (row, expected_code) in enumerate(zip(rows, terminal_codes)):
        if not isinstance(row, dict) or set(row) != expected_fields:
            raise GenerationError(
                f"terminal code row {index} fields must be exactly {sorted(expected_fields)}"
            )
        code = row.get("code")
        expectation = row.get("effect_expectation")
        fallback = row.get("fallback_eligibility")
        if code != expected_code:
            raise GenerationError("terminal code policy order drifted")
        if expectation not in effect_expectations:
            raise GenerationError(f"terminal code {code} has unknown effect expectation")
        if fallback not in fallback_values:
            raise GenerationError(f"terminal code {code} has unknown fallback eligibility")
        policies.append(
            {
                "code": str(code),
                "effect_expectation": str(expectation),
                "fallback_eligibility": str(fallback),
            }
        )
    return policies


def render_string_slice(name: str, values: list[str], doc: str) -> list[str]:
    lines = [f"/// {doc}", f"pub const {name}: &[&str] = &["]
    lines.extend(f"    {rust_string(value)}," for value in values)
    lines.append("];\n")
    return lines


def render_wire_enum(name: str, values: list[str]) -> list[str]:
    variants = ensure_unique_variants(values, name)
    lines = [
        f"/// Closed wire values for `{name}`.",
        "#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]",
        f"pub enum {name} {{",
    ]
    for variant, wire in variants:
        lines.append(f"    /// Wire value `{wire}`.")
        lines.append(f"    {variant},")
    lines.extend(
        [
            "}",
            "",
            f"impl {name} {{",
            "    /// Returns the canonical wire value.",
            "    #[must_use]",
            "    pub const fn as_wire(self) -> &'static str {",
            "        match self {",
        ]
    )
    for variant, wire in variants:
        lines.append(f"            Self::{variant} => {rust_string(wire)},")
    lines.extend(
        [
            "        }",
            "    }",
            "",
            "    /// Decodes one canonical wire value.",
            "    #[must_use]",
            "    pub fn from_wire(value: &str) -> Option<Self> {",
            "        match value {",
        ]
    )
    for variant, wire in variants:
        lines.append(f"            {rust_string(wire)} => Some(Self::{variant}),")
    lines.extend(
        [
            "            _ => None,",
            "        }",
            "    }",
            "}",
            "",
        ]
    )
    return lines


def render_rust(
    contract_set: dict[str, Any],
    contract_digests: list[dict[str, Any]],
    contracts: dict[str, dict[str, Any]],
    generator_sha256: str,
) -> tuple[bytes, dict[str, Any]]:
    handshake = contracts["tracedecay.memory.provider.handshake.v1"]
    exact_scope = handshake.get("exact_scope_identity")
    if not isinstance(exact_scope, dict):
        raise GenerationError("handshake contract has no exact-scope identity")
    digest_spec = exact_scope.get("digest")
    if not isinstance(digest_spec, dict):
        raise GenerationError("handshake contract has no exact-scope digest contract")
    expected_digest_fields = {
        "algorithm",
        "domain_ascii",
        "domain_suffix_byte_hex",
        "string_field_order",
        "string_field_encoding",
        "scope_revision_encoding",
        "output_encoding",
        "golden_vector",
    }
    if set(digest_spec) != expected_digest_fields:
        raise GenerationError("exact-scope digest contract fields drifted")
    digest_string_fields = list_strings(
        digest_spec.get("string_field_order"),
        "exact-scope digest string fields",
    )
    if digest_string_fields != [
        "profile_id",
        "project_id",
        "repository_identity",
        "worktree_identity",
        "branch_identity",
        "agent_session_id",
    ]:
        raise GenerationError("exact-scope digest string field order drifted")
    if digest_spec.get("algorithm") != "sha256":
        raise GenerationError("exact-scope digest algorithm must be SHA-256")
    digest_domain = digest_spec.get("domain_ascii")
    if digest_domain != "tracedecay.memory-provider.exact-scope.v1":
        raise GenerationError("exact-scope digest ASCII domain drifted")
    if digest_spec.get("domain_suffix_byte_hex") != "00":
        raise GenerationError("exact-scope digest domain must end with NUL")
    if digest_spec.get("string_field_encoding") != (
        "u64_big_endian_byte_length_then_utf8_bytes"
    ):
        raise GenerationError("exact-scope digest string boundary encoding drifted")
    if digest_spec.get("scope_revision_encoding") != "u64_big_endian":
        raise GenerationError("exact-scope digest scope revision encoding drifted")
    if digest_spec.get("output_encoding") != "lowercase_hex_64":
        raise GenerationError("exact-scope digest output encoding drifted")

    golden = digest_spec.get("golden_vector")
    if not isinstance(golden, dict) or set(golden) != set(digest_string_fields) | {
        "scope_revision",
        "digest",
    }:
        raise GenerationError("exact-scope digest golden vector fields drifted")
    golden_strings: list[str] = []
    golden_bytes = bytearray(digest_domain.encode("ascii"))
    golden_bytes.extend(bytes.fromhex(str(digest_spec["domain_suffix_byte_hex"])))
    for field in digest_string_fields:
        value = golden.get(field)
        if not isinstance(value, str):
            raise GenerationError(f"exact-scope digest golden {field} must be a string")
        golden_strings.append(value)
        encoded = value.encode("utf-8")
        golden_bytes.extend(len(encoded).to_bytes(8, "big"))
        golden_bytes.extend(encoded)
    golden_revision = golden.get("scope_revision")
    if (
        not isinstance(golden_revision, int)
        or isinstance(golden_revision, bool)
        or not 0 <= golden_revision <= (2**64 - 1)
    ):
        raise GenerationError("exact-scope digest golden revision must be a u64")
    golden_bytes.extend(golden_revision.to_bytes(8, "big"))
    golden_digest = golden.get("digest")
    if (
        not isinstance(golden_digest, str)
        or re.fullmatch(r"[0-9a-f]{64}", golden_digest) is None
        or sha256_bytes(golden_bytes) != golden_digest
    ):
        raise GenerationError("exact-scope digest golden output is invalid")
    rust_digest_domain = rust_string(digest_domain + "\0").replace("\\u0000", "\\0")

    limit_catalog = handshake.get("limit_catalog")
    if not isinstance(limit_catalog, list):
        raise GenerationError("handshake contract has no provider limit catalog")
    provider_limits: list[tuple[str, int, int, str]] = []
    for index, row in enumerate(limit_catalog):
        if not isinstance(row, dict):
            raise GenerationError(f"provider limit {index} must be an object")
        if set(row) != {"id", "minimum", "maximum", "unit"}:
            raise GenerationError(
                f"provider limit {index} fields must be id, minimum, maximum, and unit"
            )
        limit_id = row.get("id")
        minimum = row.get("minimum")
        maximum = row.get("maximum")
        unit = row.get("unit")
        if not isinstance(limit_id, str) or not limit_id:
            raise GenerationError(f"provider limit {index} has no ID")
        if (
            not isinstance(minimum, int)
            or isinstance(minimum, bool)
            or not 1 <= minimum <= (2**64 - 1)
        ):
            raise GenerationError(
                f"provider limit {limit_id} minimum must be a positive u64"
            )
        if (
            not isinstance(maximum, int)
            or isinstance(maximum, bool)
            or not 1 <= maximum <= (2**64 - 1)
        ):
            raise GenerationError(
                f"provider limit {limit_id} maximum must be a positive u64"
            )
        if minimum > maximum:
            raise GenerationError(
                f"provider limit {limit_id} minimum exceeds maximum"
            )
        if not isinstance(unit, str) or not unit:
            raise GenerationError(f"provider limit {limit_id} unit must be a string")
        provider_limits.append((limit_id, minimum, maximum, unit))
    if [
        (limit_id, minimum, unit)
        for limit_id, minimum, _maximum, unit in provider_limits
    ] != EXPECTED_PROVIDER_LIMIT_IDENTITIES:
        raise GenerationError("provider limit catalog order, minimum, or unit drifted")

    capabilities = registry_capabilities(
        contracts["tracedecay.memory.provider.registry.v1"]
    )
    fields = required_fields(contracts)
    enums = enum_sources(contracts)
    for enum_name, values in enums.items():
        if not values or len(values) != len(set(values)):
            raise GenerationError(f"{enum_name} values are empty or duplicate")
    terminal_policies = terminal_code_policies(
        contracts["tracedecay.memory.provider.terminal.v1"],
        enums["TerminalCode"],
        enums["CommittedEffectExpectation"],
        enums["FallbackEligibility"],
    )

    contract_set_sha256 = canonical_sha(contract_set)
    lines: list[str] = [
        "// @generated by scripts/product/generate-memory-provider-rust.py; DO NOT EDIT.",
        f"// generator-sha256: {generator_sha256}",
        f"// contract-set-sha256: {contract_set_sha256}",
        "#![forbid(unsafe_code)]",
        "#![deny(warnings)]",
        "#![deny(missing_docs)]",
        "#![deny(clippy::dbg_macro)]",
        "#![deny(clippy::expect_used)]",
        "#![deny(clippy::panic)]",
        "#![deny(clippy::todo)]",
        "#![deny(clippy::unimplemented)]",
        "#![deny(clippy::unwrap_used)]",
        "//! Generated dependency-free Rust domain bindings for the canonical Memory Provider V1 contracts.",
        "//!",
        "//! The JSON contracts and schemas remain the sole wire authority. These bindings contain",
        "//! closed values, stable identifiers, field-name constants, and provider-neutral domain",
        "//! wrappers. They intentionally contain no transport decoder and no concrete provider logic.",
        "",
        "use core::fmt;",
        "",
        "/// Canonical Memory Provider V1 contract-set identity.",
        f"pub const CONTRACT_SET_ID: &str = {rust_string(contract_set['contract_set_id'])};",
        "/// SHA-256 of the canonical contract-set source.",
        f"pub const CONTRACT_SET_SHA256: &str = {rust_string(contract_set_sha256)};",
        "/// SHA-256 of the generator that emitted this file.",
        f"pub const GENERATOR_SHA256: &str = {rust_string(generator_sha256)};",
        "",
        "/// Canonical exact-scope digest algorithm.",
        f"pub const EXACT_SCOPE_DIGEST_ALGORITHM: &str = {rust_string(str(digest_spec['algorithm']))};",
        "/// Canonical exact-scope digest domain, including its trailing NUL.",
        f"pub const EXACT_SCOPE_DIGEST_DOMAIN: &[u8] = b{rust_digest_domain};",
        "/// Canonical exact-scope string field order.",
        "pub const EXACT_SCOPE_DIGEST_STRING_FIELDS: &[&str] = &[",
        *[f"    {rust_string(value)}," for value in digest_string_fields],
        "];",
        "/// Canonical framing for every exact-scope string field.",
        f"pub const EXACT_SCOPE_DIGEST_STRING_FIELD_ENCODING: &str = {rust_string(str(digest_spec['string_field_encoding']))};",
        "/// Canonical exact-scope revision encoding.",
        f"pub const EXACT_SCOPE_DIGEST_SCOPE_REVISION_ENCODING: &str = {rust_string(str(digest_spec['scope_revision_encoding']))};",
        "/// Canonical exact-scope digest output encoding.",
        f"pub const EXACT_SCOPE_DIGEST_OUTPUT_ENCODING: &str = {rust_string(str(digest_spec['output_encoding']))};",
        "/// Canonical string values for the fixed exact-scope digest golden vector.",
        "pub const EXACT_SCOPE_DIGEST_GOLDEN_STRINGS: &[&str] = &[",
        *[f"    {rust_string(value)}," for value in golden_strings],
        "];",
        "/// Scope revision for the fixed exact-scope digest golden vector.",
        f"pub const EXACT_SCOPE_DIGEST_GOLDEN_SCOPE_REVISION: u64 = {golden_revision};",
        "/// Expected lowercase SHA-256 for the fixed exact-scope digest golden vector.",
        f"pub const EXACT_SCOPE_DIGEST_GOLDEN_SHA256: &str = {rust_string(golden_digest)};",
        "",
        "/// One canonical finite provider limit used during handshake negotiation.",
        "#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]",
        "pub struct ProviderLimitSpec {",
        "    /// Stable limit identity.",
        "    pub limit_id: &'static str,",
        "    /// Canonical inclusive minimum.",
        "    pub minimum: u64,",
        "    /// Canonical inclusive maximum.",
        "    pub maximum: u64,",
        "    /// Canonical measurement unit.",
        "    pub unit: &'static str,",
        "}",
        "",
        "/// Canonical provider-limit catalog in handshake negotiation order.",
        "pub const PROVIDER_LIMITS: &[ProviderLimitSpec] = &[",
        *[
            "    ProviderLimitSpec {\n"
            f"        limit_id: {rust_string(limit_id)},\n"
            f"        minimum: {minimum},\n"
            f"        maximum: {maximum},\n"
            f"        unit: {rust_string(unit)},\n"
            "    },"
            for limit_id, minimum, maximum, unit in provider_limits
        ],
        "];",
        "",
        "/// Canonical provider-limit maxima retained for source compatibility.",
        "pub const PROVIDER_LIMIT_MAXIMA: &[(&str, u64)] = &[",
        *[
            f"    ({rust_string(limit_id)}, {maximum}),"
            for limit_id, _minimum, maximum, _unit in provider_limits
        ],
        "];",
        "",
        "/// One conservative API-only terminal text bound.",
        "#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]",
        "pub struct TerminalTextLimitSpec {",
        "    /// Terminal field identity.",
        "    pub field: &'static str,",
        "    /// Maximum UTF-8 bytes retained by the owned runtime API.",
        "    pub maximum_bytes: usize,",
        "}",
        "",
        *[
            f"/// Conservative API-only UTF-8 byte bound for `{field}`.\n"
            f"pub const {constant}: usize = {maximum};"
            for field, constant, maximum in EXPECTED_TERMINAL_TEXT_LIMITS
        ],
        "",
        "/// Conservative API-only terminal text bounds in field order.",
        "pub const TERMINAL_TEXT_LIMITS: &[TerminalTextLimitSpec] = &[",
        *[
            "    TerminalTextLimitSpec { "
            f"field: {rust_string(field)}, maximum_bytes: {constant} "
            "},"
            for field, constant, _maximum in EXPECTED_TERMINAL_TEXT_LIMITS
        ],
        "];",
        "",
        "/// One canonical contract in the M1 authority set.",
        "#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]",
        "pub struct ContractSpec {",
        "    /// Canonical contract identity.",
        "    pub contract_id: &'static str,",
        "    /// Owning Beads issue.",
        "    pub bead_id: &'static str,",
        "    /// SHA-256 of canonical contract JSON.",
        "    pub contract_sha256: &'static str,",
        "    /// SHA-256 of canonical schema JSON.",
        "    pub schema_sha256: &'static str,",
        "}",
        "",
        "/// Canonical contracts in dependency order.",
        "pub const CONTRACTS: &[ContractSpec] = &[",
    ]
    for row in contract_digests:
        lines.extend(
            [
                "    ContractSpec {",
                f"        contract_id: {rust_string(row['contract_id'])},",
                f"        bead_id: {rust_string(row['bead_id'])},",
                f"        contract_sha256: {rust_string(row['contract_sha256'])},",
                f"        schema_sha256: {rust_string(row['schema_sha256'])},",
                "    },",
            ]
        )
    lines.extend(
        [
            "];",
            "",
            "/// Whether a provider capability is required by every implementation.",
            "#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]",
            "pub enum CapabilityRequirement {",
            "    /// Required for every compatible provider.",
            "    Mandatory,",
            "    /// Optional and capability-gated.",
            "    Optional,",
            "}",
            "",
            "/// One provider-neutral capability specification.",
            "#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]",
            "pub struct CapabilitySpec {",
            "    /// Canonical versioned capability identity.",
            "    pub capability_id: &'static str,",
            "    /// Mandatory or optional classification.",
            "    pub requirement: CapabilityRequirement,",
            "    /// Provider-local behavior class.",
            "    pub behavior_class: &'static str,",
            "    /// Bead that owns detailed semantics.",
            "    pub detailed_contract_bead: &'static str,",
            "    /// Whether output is advisory relative to TraceDecay authorities.",
            "    pub advisory_only: bool,",
            "    /// Whether the capability may mutate TraceDecay canonical authority.",
            "    pub may_mutate_tracedecay_authority: bool,",
            "}",
            "",
            "/// Canonical provider capabilities in registry order.",
            "pub const CAPABILITIES: &[CapabilitySpec] = &[",
        ]
    )
    for row in capabilities:
        requirement = (
            "CapabilityRequirement::Mandatory"
            if row["requirement"] == "mandatory"
            else "CapabilityRequirement::Optional"
        )
        lines.extend(
            [
                "    CapabilitySpec {",
                f"        capability_id: {rust_string(row['id'])},",
                f"        requirement: {requirement},",
                f"        behavior_class: {rust_string(str(row['class']))},",
                f"        detailed_contract_bead: {rust_string(str(row['detailed_contract_bead']))},",
                "        advisory_only: true,",
                "        may_mutate_tracedecay_authority: false,",
                "    },",
            ]
        )
    lines.extend(["];", ""])

    for name, values in fields.items():
        lines.extend(
            render_string_slice(
                name,
                values,
                f"Canonical required field names for `{name.lower()}`.",
            )
        )

    for name, values in enums.items():
        lines.extend(render_wire_enum(name, values))

    lines.extend(
        [
            "/// Canonical semantic policy for one closed terminal code.",
            "#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]",
            "pub struct TerminalCodePolicy {",
            "    /// Closed terminal code.",
            "    pub terminal_code: TerminalCode,",
            "    /// Maximum committed-effect shapes admitted by this code.",
            "    pub effect_expectation: CommittedEffectExpectation,",
            "    /// Maximum fallback eligibility; hosts may safely narrow it to forbidden.",
            "    pub maximum_fallback_eligibility: FallbackEligibility,",
            "}",
            "",
            "/// Canonical terminal-code semantic table in contract order.",
            "pub const TERMINAL_CODE_POLICIES: &[TerminalCodePolicy] = &[",
        ]
    )
    for policy in terminal_policies:
        lines.extend(
            [
                "    TerminalCodePolicy {",
                "        terminal_code: "
                f"TerminalCode::{rust_identifier(policy['code'])},",
                "        effect_expectation: "
                "CommittedEffectExpectation::"
                f"{rust_identifier(policy['effect_expectation'])},",
                "        maximum_fallback_eligibility: "
                f"FallbackEligibility::{rust_identifier(policy['fallback_eligibility'])},",
                "    },",
            ]
        )
    lines.extend(["];", ""])

    lines.extend(
        [
            "/// Stable provider-identifier validation error.",
            "#[derive(Clone, Copy, Debug, Eq, PartialEq)]",
            "pub enum IdentifierError {",
            "    /// Identifier was empty.",
            "    Empty,",
            "    /// Identifier exceeded its contract byte limit.",
            "    TooLong,",
            "    /// Identifier did not match canonical ASCII syntax.",
            "    InvalidSyntax,",
            "}",
            "",
            "impl fmt::Display for IdentifierError {",
            "    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {",
            "        formatter.write_str(match self {",
            "            Self::Empty => \"identifier is empty\",",
            "            Self::TooLong => \"identifier exceeds its byte limit\",",
            "            Self::InvalidSyntax => \"identifier has non-canonical syntax\",",
            "        })",
            "    }",
            "}",
            "",
            "fn valid_segmented_ascii(value: &str, maximum: usize) -> Result<(), IdentifierError> {",
            "    let bytes = value.as_bytes();",
            "    if bytes.is_empty() {",
            "        return Err(IdentifierError::Empty);",
            "    }",
            "    if bytes.len() > maximum {",
            "        return Err(IdentifierError::TooLong);",
            "    }",
            "    if !bytes[0].is_ascii_lowercase() {",
            "        return Err(IdentifierError::InvalidSyntax);",
            "    }",
            "    let mut previous_was_separator = false;",
            "    for byte in bytes {",
            "        if byte.is_ascii_lowercase() || byte.is_ascii_digit() {",
            "            previous_was_separator = false;",
            "        } else if matches!(byte, b'.' | b'_' | b'-') && !previous_was_separator {",
            "            previous_was_separator = true;",
            "        } else {",
            "            return Err(IdentifierError::InvalidSyntax);",
            "        }",
            "    }",
            "    if previous_was_separator {",
            "        return Err(IdentifierError::InvalidSyntax);",
            "    }",
            "    Ok(())",
            "}",
            "",
            "/// Stable logical memory-provider identity.",
            "#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]",
            "pub struct ProviderId<'a>(&'a str);",
            "",
            "impl<'a> ProviderId<'a> {",
            "    /// Validates and constructs a provider identity.",
            "    pub fn new(value: &'a str) -> Result<Self, IdentifierError> {",
            "        valid_segmented_ascii(value, 64)?;",
            "        Ok(Self(value))",
            "    }",
            "",
            "    /// Returns canonical provider identity bytes as UTF-8 text.",
            "    #[must_use]",
            "    pub const fn as_str(self) -> &'a str {",
            "        self.0",
            "    }",
            "}",
            "",
            "/// Stable versioned provider-capability identity.",
            "#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]",
            "pub struct CapabilityId<'a>(&'a str);",
            "",
            "impl<'a> CapabilityId<'a> {",
            "    /// Validates and constructs a versioned capability identity.",
            "    pub fn new(value: &'a str) -> Result<Self, IdentifierError> {",
            "        if value.len() > 96 {",
            "            return Err(IdentifierError::TooLong);",
            "        }",
            "        let Some((prefix, version)) = value.rsplit_once(\".v\") else {",
            "            return Err(IdentifierError::InvalidSyntax);",
            "        };",
            "        valid_segmented_ascii(prefix, 90)?;",
            "        if version.is_empty()",
            "            || !version.bytes().all(|byte| byte.is_ascii_digit())",
            "            || version.as_bytes().first() == Some(&b'0')",
            "        {",
            "            return Err(IdentifierError::InvalidSyntax);",
            "        }",
            "        Ok(Self(value))",
            "    }",
            "",
            "    /// Returns the canonical capability identity.",
            "    #[must_use]",
            "    pub const fn as_str(self) -> &'a str {",
            "        self.0",
            "    }",
            "}",
            "",
            "/// Exact coding scope admitted by TraceDecay.",
            "#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]",
            "pub struct ExactScopeIdentity<'a> {",
            "    /// Profile authority identity.",
            "    pub profile_id: &'a str,",
            "    /// Project authority identity.",
            "    pub project_id: &'a str,",
            "    /// Repository authority identity.",
            "    pub repository_identity: &'a str,",
            "    /// Exact linked-worktree identity.",
            "    pub worktree_identity: &'a str,",
            "    /// Exact branch or detached-reference identity.",
            "    pub branch_identity: &'a str,",
            "    /// Exact coding-agent session identity.",
            "    pub agent_session_id: &'a str,",
            "    /// Monotonic TraceDecay scope revision.",
            "    pub scope_revision: u64,",
            "}",
            "",
            "/// Request cancellation state at dispatch.",
            "#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]",
            "pub enum CancellationState {",
            "    /// Request is still live.",
            "    Live,",
            "    /// Request was already cancelled.",
            "    Cancelled,",
            "}",
            "",
            "/// Immutable request-control budget passed to a concrete provider operation.",
            "#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]",
            "pub struct RequestControl {",
            "    /// Absolute UTC deadline in microseconds since Unix epoch.",
            "    pub deadline_utc_micros: i64,",
            "    /// Monotonic remaining budget at provider dispatch.",
            "    pub remaining_millis: u64,",
            "    /// Live cancellation state.",
            "    pub cancellation: CancellationState,",
            "}",
            "",
            "/// Opaque versioned extension that cannot silently activate behavior.",
            "#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]",
            "pub struct OpaqueExtension<'a> {",
            "    /// Stable extension identity.",
            "    pub extension_id: &'a str,",
            "    /// Positive extension version.",
            "    pub extension_version: u32,",
            "    /// Whether an unknown extension is required rather than optional.",
            "    pub required: bool,",
            "    /// SHA-256 of canonical opaque payload bytes.",
            "    pub payload_sha256: &'a str,",
            "    /// Canonical opaque payload bytes.",
            "    pub canonical_payload: &'a [u8],",
            "}",
            "",
            "/// Borrowed committed-effect semantic projection; not a complete wire envelope.",
            "#[derive(Clone, Copy, Debug, Eq, PartialEq)]",
            "pub struct CommittedEffectEvidence<'a> {",
            "    /// Truthful committed-effect state.",
            "    pub state: CommittedEffectState,",
            "    /// Exact committed boundary for a partial effect.",
            "    pub committed_boundary: Option<&'a str>,",
            "    /// Provider-local state generation before the operation.",
            "    pub state_generation_before: Option<u64>,",
            "    /// Provider-local state generation after settlement or reconciliation.",
            "    pub state_generation_after: Option<u64>,",
            "    /// Provider-local item references known to have committed.",
            "    pub committed_item_refs: &'a [String],",
            "    /// Provider-local item references known not to have committed.",
            "    pub uncommitted_item_refs: &'a [String],",
            "    /// Provider receipt proving or anchoring effect reconciliation.",
            "    pub provider_receipt_digest: Option<&'a str>,",
            "    /// Explicit reconciliation or resume action.",
            "    pub reconciliation_action: Option<&'a str>,",
            "    /// Digest that verifies the known committed partition.",
            "    pub verification_digest: Option<&'a str>,",
            "}",
            "",
            "/// Borrowed host policy pin required before any fallback is eligible.",
            "#[derive(Clone, Copy, Debug, Eq, PartialEq)]",
            "pub struct PinnedFallbackPolicy<'a> {",
            "    /// Stable host policy identity.",
            "    pub policy_id: &'a str,",
            "    /// Positive pinned host policy revision.",
            "    pub policy_revision: u64,",
            "    /// Explicit alternate provider selected by the policy.",
            "    pub target_provider_id: &'a str,",
            "}",
            "",
            "/// Borrowed fallback semantic projection; not the flat V1 wire shape.",
            "#[derive(Clone, Copy, Debug, Eq, PartialEq)]",
            "pub struct FallbackDirective<'a> {",
            "    /// Explicit fallback eligibility.",
            "    pub eligibility: FallbackEligibility,",
            "    /// Complete host policy pin when eligibility is explicit-policy-only.",
            "    pub policy: Option<PinnedFallbackPolicy<'a>>,",
            "    /// Non-empty policy decision reason when eligibility is explicit-policy-only.",
            "    pub reason: Option<&'a str>,",
            "}",
            "",
            "/// Provider-neutral terminal summary retained by TraceDecay.",
            "#[derive(Clone, Copy, Debug, Eq, PartialEq)]",
            "pub struct TerminalSummary<'a> {",
            "    /// Canonical provider operation kind.",
            "    pub operation_kind: &'a str,",
            "    /// Provider that produced this terminal.",
            "    pub provider_id: &'a str,",
            "    /// Typed terminal outcome.",
            "    pub terminal_code: TerminalCode,",
            "    /// Structured committed-effect evidence.",
            "    pub committed_effect: CommittedEffectEvidence<'a>,",
            "    /// Structured fallback policy decision.",
            "    pub fallback: FallbackDirective<'a>,",
            "    /// Stable operation identity.",
            "    pub operation_id: &'a str,",
            "    /// Exact scope digest.",
            "    pub exact_scope_digest: &'a str,",
            "    /// Optional stable diagnostic identity.",
            "    pub diagnostic_id: Option<&'a str>,",
            "}",
        ]
    )

    source = ("\n".join(lines).rstrip() + "\n").encode("utf-8")
    metadata = {
        "contract_count": len(contract_digests),
        "capability_count": len(capabilities),
        "mandatory_capability_count": sum(
            1 for row in capabilities if row["requirement"] == "mandatory"
        ),
        "optional_capability_count": sum(
            1 for row in capabilities if row["requirement"] == "optional"
        ),
        "enum_counts": {name: len(values) for name, values in enums.items()},
        "required_field_set_counts": {
            name: len(values) for name, values in fields.items()
        },
    }
    return source, metadata


def render_outputs(
    repo: Path, contract_set_path: Path, output_dir: Path
) -> tuple[bytes, bytes]:
    contract_set, contract_digests, contracts = load_contracts(
        repo, contract_set_path
    )
    generator_path = Path(__file__).resolve()
    generator_bytes = generator_path.read_bytes()
    generator_sha256 = sha256_bytes(generator_bytes)
    rust_source, metadata = render_rust(
        contract_set,
        contract_digests,
        contracts,
        generator_sha256,
    )
    output_path = output_dir / "memory_provider_v1.rs"
    manifest = {
        "schema_version": 1,
        "manifest_id": "tracedecay.memory.provider.generated-rust.manifest.v1",
        "contract_set_id": contract_set["contract_set_id"],
        "canonical_encoding": "utf8_rust_source_without_bom_with_lf",
        "generator_path": str(generator_path.relative_to(repo)),
        "generator_sha256": generator_sha256,
        "contract_set_path": str(contract_set_path.relative_to(repo)),
        "contract_set_sha256": canonical_sha(contract_set),
        "output_path": str(output_path.relative_to(repo)),
        "output_sha256": sha256_bytes(rust_source),
        "output_bytes": len(rust_source),
        "source_contracts": contract_digests,
        **metadata,
    }
    return rust_source, canonical_bytes(manifest) + b"\n"


def diff_bytes(expected: bytes, actual: bytes, label: str) -> str:
    expected_lines = expected.decode("utf-8").splitlines(keepends=True)
    actual_lines = actual.decode("utf-8").splitlines(keepends=True)
    return "".join(
        difflib.unified_diff(
            actual_lines,
            expected_lines,
            fromfile=f"checked-in/{label}",
            tofile=f"generated/{label}",
        )
    )


def write_if_changed(path: Path, content: bytes) -> bool:
    current = path.read_bytes() if path.exists() else None
    if current == content:
        return False
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(content)
    return True


def main() -> int:
    args = parse_args()
    repo = args.repo.resolve()
    contract_set_path = resolve(repo, args.contract_set)
    output_dir = resolve(repo, args.output_dir)
    try:
        rust_source, manifest_bytes = render_outputs(
            repo, contract_set_path, output_dir
        )
        outputs = {
            output_dir / "memory_provider_v1.rs": rust_source,
            output_dir / "manifest.json": manifest_bytes,
        }
        if args.write:
            changed = [
                str(path.relative_to(repo))
                for path, content in outputs.items()
                if write_if_changed(path, content)
            ]
            print(
                json.dumps(
                    {
                        "ok": True,
                        "mode": "write",
                        "changed": changed,
                        "output_sha256": sha256_bytes(rust_source),
                    },
                    indent=2,
                    sort_keys=True,
                )
            )
            return 0

        drift: list[str] = []
        for path, expected in outputs.items():
            if not path.is_file():
                drift.append(f"missing generated file: {path.relative_to(repo)}")
                continue
            actual = path.read_bytes()
            if actual != expected:
                drift.append(
                    diff_bytes(expected, actual, str(path.relative_to(repo)))
                )
        if drift:
            print(
                json.dumps(
                    {"ok": False, "mode": "check", "drift": drift},
                    indent=2,
                    sort_keys=True,
                )
            )
            return 1
        print(
            json.dumps(
                {
                    "ok": True,
                    "mode": "check",
                    "output_sha256": sha256_bytes(rust_source),
                    "manifest_sha256": sha256_bytes(manifest_bytes),
                },
                indent=2,
                sort_keys=True,
            )
        )
        return 0
    except GenerationError as exc:
        print(
            json.dumps(
                {
                    "ok": False,
                    "mode": "write" if args.write else "check",
                    "error": str(exc),
                },
                indent=2,
                sort_keys=True,
            )
        )
        return 1


if __name__ == "__main__":
    sys.exit(main())
