#!/usr/bin/env python3
"""Validate provider handshake compatibility, scope, limits, and request control."""

from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path
from typing import Any, Iterable

TOP_LEVEL = {
    "schema_version",
    "contract_id",
    "bead_id",
    "title",
    "status",
    "authority",
    "scope",
    "depends_on_contracts",
    "protocol_identity",
    "handshake_request",
    "handshake_response",
    "implementation_identity",
    "state_identity",
    "exact_scope_identity",
    "limit_catalog",
    "limit_negotiation",
    "request_control",
    "compatibility_algorithm",
    "readiness_states",
    "ready_receipt",
    "side_effect_contract",
    "invariants",
    "verification_beads",
}

REQUEST_FIELDS = [
    "provider_id",
    "registration_revision",
    "adapter_contract_version",
    "host_implementation_identity",
    "supported_protocol_ranges",
    "required_capabilities",
    "host_limit_ceiling",
    "exact_scope_identity",
    "request_identity",
    "deadline",
    "cancellation",
    "challenge_nonce",
]
RESPONSE_FIELDS = [
    "provider_id",
    "provider_instance_id",
    "implementation_identity",
    "selected_protocol_version",
    "state_identity",
    "declared_capabilities",
    "provider_limit_ceiling",
    "accepted_scope_identity",
    "challenge_response",
    "readiness_state",
    "warnings",
]
IMPLEMENTATION_FIELDS = [
    "implementation_name",
    "implementation_version",
    "build_identity",
    "artifact_sha256",
    "license_identity",
    "source_provenance",
    "adapter_contract_version",
    "state_schema_version",
]
STATE_FIELDS = [
    "provider_id",
    "state_namespace",
    "state_schema_version",
    "scope_digest",
    "state_generation",
]
SCOPE_FIELDS = [
    "profile_id",
    "project_id",
    "repository_identity",
    "worktree_identity",
    "branch_identity",
    "agent_session_id",
    "scope_revision",
]
LIMITS = {
    "request_bytes": (1, 16777216, "bytes"),
    "response_bytes": (1, 33554432, "bytes"),
    "observation_batch_items": (1, 4096, "items"),
    "recall_candidates": (1, 10000, "items"),
    "concurrent_operations": (1, 1024, "operations"),
    "operation_millis": (1, 3600000, "milliseconds"),
    "snapshot_bytes": (1, 1073741824, "bytes"),
    "inspection_items": (1, 100000, "items"),
}
READINESS_STATES = {
    "ready",
    "provider_unknown",
    "provider_disabled",
    "provider_reserved",
    "provider_retiring",
    "adapter_unavailable",
    "provider_unavailable",
    "provider_id_mismatch",
    "registration_revision_conflict",
    "adapter_contract_incompatible",
    "protocol_incompatible",
    "implementation_identity_invalid",
    "challenge_failed",
    "state_schema_incompatible",
    "state_owner_mismatch",
    "scope_unavailable",
    "scope_mismatch",
    "required_capability_missing",
    "limit_negotiation_failed",
    "deadline_exceeded",
    "cancelled",
    "contract_violation",
}
RECEIPT_FIELDS = [
    "provider_id",
    "provider_instance_id",
    "registration_revision",
    "implementation_identity_digest",
    "selected_protocol_version",
    "state_identity_digest",
    "scope_digest",
    "declared_capabilities_digest",
    "effective_limits_digest",
    "handshake_transcript_digest",
    "issued_at",
    "expires_at",
]
SIDE_EFFECT_FIELDS = {
    "handshake_is_read_only",
    "provider_state_mutation_allowed",
    "tracedecay_state_mutation_allowed",
    "canonical_fact_mutation_allowed",
    "context_injection_allowed",
    "ready_from_process_existence_allowed",
    "ready_from_open_socket_allowed",
    "ready_from_nonempty_state_allowed",
}
REQUIRED_INVARIANTS = [
    "compatible handshake is required",
    "independently verified",
    "never proves readiness",
    "runtime location is diagnostic metadata",
    "no wildcards or CWD inference",
    "state owner, schema, namespace, and scope must match",
    "exact intersection",
    "finite, positive, known",
    "never call the provider",
    "reach the actual provider operation",
    "Handshake is read-only",
    "expiring scoped receipt",
    "no failure silently falls back",
]
REQUIRED_DOC_PHRASES = [
    "never proves provider readiness",
    "expired deadline or already-cancelled token terminates before provider contact",
    "Runtime location is diagnostic only",
    "Wildcards, CWD inference",
    "`min(host_ceiling, provider_ceiling)`",
    "There is no implicit cross-major downgrade",
    "cancellation reach the concrete provider operation",
    "Handshake cannot mutate provider state",
    "failure never silently falls back",
    "does not select an NCM transport",
]
BEAD_RE = re.compile(r"^tdmem-[0-9]{4}$")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--repo", type=Path, default=Path("."))
    parser.add_argument(
        "--contract",
        type=Path,
        default=Path("product/contracts/memory-provider-v1/provider-handshake-contract.json"),
    )
    parser.add_argument(
        "--schema",
        type=Path,
        default=Path("product/contracts/memory-provider-v1/provider-handshake-contract.schema.json"),
    )
    parser.add_argument(
        "--doc",
        type=Path,
        default=Path("product/contracts/memory-provider-v1/provider-handshake-contract.md"),
    )
    parser.add_argument("--issues", type=Path, default=Path(".beads/issues.jsonl"))
    return parser.parse_args()


def resolve(repo: Path, path: Path) -> Path:
    return path if path.is_absolute() else repo / path


def load_object(path: Path, label: str, errors: list[str]) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        errors.append(f"could not load {label}: {exc}")
        return {}
    if not isinstance(value, dict):
        errors.append(f"{label} root must be an object")
        return {}
    return value


def load_issue_ids(path: Path, errors: list[str]) -> set[str]:
    ids: set[str] = set()
    try:
        lines = path.read_text(encoding="utf-8").splitlines()
    except OSError as exc:
        errors.append(f"could not load Beads authority: {exc}")
        return ids
    for number, line in enumerate(lines, start=1):
        if not line.strip():
            continue
        try:
            row = json.loads(line)
        except json.JSONDecodeError as exc:
            errors.append(f"invalid Beads JSONL at line {number}: {exc}")
            continue
        issue_id = row.get("id") if isinstance(row, dict) else None
        if not isinstance(issue_id, str):
            errors.append(f"Beads line {number} has no string id")
            continue
        ids.add(issue_id)
    return ids


def obj(value: Any, label: str, errors: list[str]) -> dict[str, Any]:
    if not isinstance(value, dict):
        errors.append(f"{label} must be an object")
        return {}
    return value


def arr(value: Any, label: str, errors: list[str]) -> list[Any]:
    if not isinstance(value, list):
        errors.append(f"{label} must be an array")
        return []
    return value


def exact_keys(row: dict[str, Any], expected: set[str], label: str, errors: list[str]) -> None:
    if set(row) != expected:
        errors.append(
            f"{label} fields drifted; missing={sorted(expected-set(row))}, extra={sorted(set(row)-expected)}"
        )


def nonempty(row: dict[str, Any], field: str, label: str, errors: list[str]) -> str:
    value = row.get(field)
    if not isinstance(value, str) or not value.strip():
        errors.append(f"{label}.{field} must be a non-empty string")
        return ""
    return value.strip()


def bead(value: Any, label: str, ids: set[str], errors: list[str]) -> None:
    if not isinstance(value, str) or not BEAD_RE.fullmatch(value):
        errors.append(f"{label} must match tdmem-NNNN")
    elif value not in ids:
        errors.append(f"{label} references unknown Beads issue {value}")


def unique_by(rows: Iterable[Any], field: str, label: str, errors: list[str]) -> dict[str, dict[str, Any]]:
    result: dict[str, dict[str, Any]] = {}
    for index, raw in enumerate(rows):
        if not isinstance(raw, dict):
            errors.append(f"{label}[{index}] must be an object")
            continue
        value = raw.get(field)
        if not isinstance(value, str) or not value:
            errors.append(f"{label}[{index}].{field} must be a non-empty string")
            continue
        if value in result:
            errors.append(f"duplicate {label} {field} {value}")
            continue
        result[value] = raw
    return result


def validate_header(contract: dict[str, Any], errors: list[str]) -> None:
    exact_keys(contract, TOP_LEVEL, "contract", errors)
    if contract.get("schema_version") != 1:
        errors.append("schema_version must be 1")
    if contract.get("contract_id") != "tracedecay.memory.provider.handshake.v1":
        errors.append("contract_id must be tracedecay.memory.provider.handshake.v1")
    if contract.get("bead_id") != "tdmem-0202":
        errors.append("bead_id must be tdmem-0202")
    if contract.get("status") != "accepted":
        errors.append("contract status must be accepted")
    if contract.get("authority") != "TraceDecay provider registry composition root":
        errors.append("handshake authority must remain the TraceDecay registry composition root")
    if contract.get("scope") != "coding_agents_only":
        errors.append("handshake scope must remain coding_agents_only")
    if contract.get("depends_on_contracts") != ["tracedecay.memory.provider.registry.v1"]:
        errors.append("handshake must depend only on provider registry V1 at this stage")
    nonempty(contract, "title", "contract", errors)


def validate_protocol(contract: dict[str, Any], errors: list[str]) -> None:
    row = obj(contract.get("protocol_identity"), "protocol_identity", errors)
    expected = {
        "type_name",
        "current_major",
        "current_minor",
        "major_compatibility",
        "minor_selection",
        "implicit_downgrade",
        "unknown_major_policy",
        "empty_intersection_policy",
    }
    exact_keys(row, expected, "protocol_identity", errors)
    if row.get("type_name") != "MemoryProviderProtocolVersionV1":
        errors.append("protocol type must be MemoryProviderProtocolVersionV1")
    if row.get("current_major") != 1 or row.get("current_minor") != 0:
        errors.append("current provider protocol must be 1.0")
    if row.get("major_compatibility") != "exact_intersection_required":
        errors.append("protocol major compatibility must require exact intersection")
    if row.get("minor_selection") != "highest_mutually_supported_minor_within_selected_major":
        errors.append("protocol minor selection must be deterministic highest mutual minor")
    if row.get("implicit_downgrade") is not False:
        errors.append("implicit protocol downgrade must be false")
    if row.get("unknown_major_policy") != "reject_protocol_incompatible":
        errors.append("unknown protocol major must reject as incompatible")
    if row.get("empty_intersection_policy") != "reject_protocol_incompatible":
        errors.append("empty protocol intersection must reject as incompatible")


def validate_request_response(contract: dict[str, Any], errors: list[str]) -> None:
    request = obj(contract.get("handshake_request"), "handshake_request", errors)
    request_keys = {
        "type_name",
        "required_fields",
        "provider_id_source",
        "registration_revision_source",
        "exact_scope_source",
        "challenge_nonce_bytes",
        "maximum_supported_protocol_ranges",
        "maximum_required_capabilities",
        "duplicate_protocol_range_policy",
        "duplicate_required_capability_policy",
    }
    exact_keys(request, request_keys, "handshake_request", errors)
    if request.get("type_name") != "MemoryProviderHandshakeRequestV1":
        errors.append("handshake request type drifted")
    if request.get("required_fields") != REQUEST_FIELDS:
        errors.append("handshake request required fields must remain canonical and ordered")
    if request.get("provider_id_source") != "accepted_registry_selection":
        errors.append("provider ID must come from accepted registry selection")
    if request.get("registration_revision_source") != "accepted_registry_selection":
        errors.append("registration revision must come from accepted registry selection")
    if request.get("exact_scope_source") != "TraceDecay scope authority":
        errors.append("exact scope must come from TraceDecay scope authority")
    if request.get("challenge_nonce_bytes") != 32:
        errors.append("handshake challenge nonce must be 32 bytes")
    if not isinstance(request.get("maximum_supported_protocol_ranges"), int) or not 1 <= request["maximum_supported_protocol_ranges"] <= 8:
        errors.append("maximum supported protocol ranges must be between 1 and 8")
    if not isinstance(request.get("maximum_required_capabilities"), int) or not 1 <= request["maximum_required_capabilities"] <= 32:
        errors.append("maximum required capabilities must be between 1 and 32")
    for field in ("duplicate_protocol_range_policy", "duplicate_required_capability_policy"):
        if request.get(field) != "reject_non_canonical_request":
            errors.append(f"handshake_request.{field} must reject non-canonical requests")

    response = obj(contract.get("handshake_response"), "handshake_response", errors)
    response_keys = {
        "type_name",
        "required_fields",
        "provider_instance_id_semantics",
        "challenge_response",
        "maximum_warnings",
        "unknown_response_field_policy",
    }
    exact_keys(response, response_keys, "handshake_response", errors)
    if response.get("type_name") != "MemoryProviderHandshakeResponseV1":
        errors.append("handshake response type drifted")
    if response.get("required_fields") != RESPONSE_FIELDS:
        errors.append("handshake response required fields must remain canonical and ordered")
    if response.get("provider_instance_id_semantics") != "opaque_runtime_instance_identity_not_provider_identity":
        errors.append("provider instance ID must remain opaque runtime identity")
    if response.get("challenge_response") != "sha256_over_canonical_request_challenge_and_response_identity":
        errors.append("challenge response digest contract drifted")
    if not isinstance(response.get("maximum_warnings"), int) or not 0 <= response["maximum_warnings"] <= 32:
        errors.append("maximum handshake warnings must be bounded at 32")
    if response.get("unknown_response_field_policy") != "reject_contract_violation":
        errors.append("unknown handshake response fields must reject contract violation")


def validate_identities(contract: dict[str, Any], errors: list[str]) -> None:
    implementation = obj(contract.get("implementation_identity"), "implementation_identity", errors)
    implementation_keys = {
        "type_name",
        "required_fields",
        "artifact_sha256_encoding",
        "build_identity_maximum_bytes",
        "source_provenance_maximum_bytes",
        "license_identity_maximum_bytes",
        "runtime_location_is_identity",
        "process_id_is_identity",
        "socket_path_is_identity",
        "database_path_is_identity",
    }
    exact_keys(implementation, implementation_keys, "implementation_identity", errors)
    if implementation.get("type_name") != "MemoryProviderImplementationIdentityV1":
        errors.append("implementation identity type drifted")
    if implementation.get("required_fields") != IMPLEMENTATION_FIELDS:
        errors.append("implementation identity required fields drifted")
    if implementation.get("artifact_sha256_encoding") != "lowercase_hex_64":
        errors.append("implementation artifact digest must be lowercase SHA-256 hex")
    for field, maximum in (
        ("build_identity_maximum_bytes", 256),
        ("source_provenance_maximum_bytes", 1024),
        ("license_identity_maximum_bytes", 128),
    ):
        value = implementation.get(field)
        if not isinstance(value, int) or not 1 <= value <= maximum:
            errors.append(f"implementation_identity.{field} must be bounded by {maximum}")
    for field in (
        "runtime_location_is_identity",
        "process_id_is_identity",
        "socket_path_is_identity",
        "database_path_is_identity",
    ):
        if implementation.get(field) is not False:
            errors.append(f"implementation_identity.{field} must be false")

    state = obj(contract.get("state_identity"), "state_identity", errors)
    state_keys = {
        "type_name",
        "required_fields",
        "state_namespace_maximum_bytes",
        "scope_digest_encoding",
        "state_generation_minimum",
        "path_is_authority",
        "owner_match_required",
        "scope_match_required",
        "schema_compatibility_required",
    }
    exact_keys(state, state_keys, "state_identity", errors)
    if state.get("type_name") != "MemoryProviderStateIdentityV1":
        errors.append("state identity type drifted")
    if state.get("required_fields") != STATE_FIELDS:
        errors.append("state identity required fields drifted")
    if not isinstance(state.get("state_namespace_maximum_bytes"), int) or not 1 <= state["state_namespace_maximum_bytes"] <= 128:
        errors.append("state namespace maximum must be bounded at 128 bytes")
    if state.get("scope_digest_encoding") != "lowercase_hex_64":
        errors.append("state scope digest must be lowercase SHA-256 hex")
    if state.get("state_generation_minimum") != 0:
        errors.append("state generation minimum must be zero")
    if state.get("path_is_authority") is not False:
        errors.append("provider state path must never be authority")
    for field in ("owner_match_required", "scope_match_required", "schema_compatibility_required"):
        if state.get(field) is not True:
            errors.append(f"state_identity.{field} must be true")

    scope = obj(contract.get("exact_scope_identity"), "exact_scope_identity", errors)
    scope_keys = {
        "type_name",
        "required_fields",
        "wildcards_allowed",
        "missing_identity_policy",
        "mismatch_policy",
        "provider_path_inference_allowed",
        "cwd_inference_allowed",
    }
    exact_keys(scope, scope_keys, "exact_scope_identity", errors)
    if scope.get("type_name") != "MemoryProviderExactScopeIdentityV1":
        errors.append("exact scope identity type drifted")
    if scope.get("required_fields") != SCOPE_FIELDS:
        errors.append("exact scope required fields must include profile/project/repository/worktree/branch/session/revision")
    for field in ("wildcards_allowed", "provider_path_inference_allowed", "cwd_inference_allowed"):
        if scope.get(field) is not False:
            errors.append(f"exact_scope_identity.{field} must be false")
    if scope.get("missing_identity_policy") != "reject_scope_unavailable":
        errors.append("missing exact scope must reject scope_unavailable")
    if scope.get("mismatch_policy") != "reject_scope_mismatch":
        errors.append("mismatched exact scope must reject scope_mismatch")


def validate_limits(contract: dict[str, Any], errors: list[str]) -> None:
    catalog = unique_by(arr(contract.get("limit_catalog"), "limit_catalog", errors), "id", "limit_catalog", errors)
    if set(catalog) != set(LIMITS):
        errors.append("limit catalog must exactly contain the eight V1 bounded limits")
    for limit_id, expected in LIMITS.items():
        row = catalog.get(limit_id, {})
        exact_keys(row, {"id", "minimum", "maximum", "unit"}, f"limit[{limit_id}]", errors)
        minimum, maximum, unit = expected
        if row.get("minimum") != minimum or row.get("maximum") != maximum or row.get("unit") != unit:
            errors.append(f"limit {limit_id} bounds or unit drifted")
        if isinstance(row.get("minimum"), int) and isinstance(row.get("maximum"), int) and row["minimum"] > row["maximum"]:
            errors.append(f"limit {limit_id} minimum exceeds maximum")

    negotiation = obj(contract.get("limit_negotiation"), "limit_negotiation", errors)
    negotiation_keys = {
        "type_name",
        "required_limit_ids",
        "algorithm",
        "host_may_clamp_further_per_request",
        "provider_may_exceed_effective_limit",
        "unbounded_value_allowed",
        "zero_value_allowed",
        "missing_limit_policy",
        "unknown_limit_policy",
        "overflow_policy",
    }
    exact_keys(negotiation, negotiation_keys, "limit_negotiation", errors)
    if negotiation.get("type_name") != "MemoryProviderEffectiveLimitsV1":
        errors.append("effective limits type drifted")
    if negotiation.get("required_limit_ids") != list(LIMITS):
        errors.append("required limit IDs must remain canonical and ordered")
    if negotiation.get("algorithm") != "effective_limit_is_minimum_of_host_and_provider_ceiling_for_every_limit_id":
        errors.append("effective limit algorithm must take the host/provider minimum")
    if negotiation.get("host_may_clamp_further_per_request") is not True:
        errors.append("host must be allowed to clamp limits further per request")
    for field in ("provider_may_exceed_effective_limit", "unbounded_value_allowed", "zero_value_allowed"):
        if negotiation.get(field) is not False:
            errors.append(f"limit_negotiation.{field} must be false")
    for field, expected in (
        ("missing_limit_policy", "reject_limit_negotiation_failed"),
        ("unknown_limit_policy", "reject_contract_violation"),
        ("overflow_policy", "reject_contract_violation"),
    ):
        if negotiation.get(field) != expected:
            errors.append(f"limit_negotiation.{field} must be {expected}")


def validate_control(contract: dict[str, Any], errors: list[str]) -> None:
    control = obj(contract.get("request_control"), "request_control", errors)
    exact_keys(control, {"deadline", "cancellation"}, "request_control", errors)
    deadline = obj(control.get("deadline"), "request_control.deadline", errors)
    deadline_keys = {
        "type_name",
        "representation",
        "deadline_required",
        "expired_before_dispatch_policy",
        "provider_must_receive_remaining_budget",
        "provider_operation_must_stop_after_deadline",
        "deadline_extension_allowed",
    }
    exact_keys(deadline, deadline_keys, "request_control.deadline", errors)
    if deadline.get("type_name") != "MemoryProviderDeadlineV1":
        errors.append("deadline type drifted")
    if deadline.get("representation") != "absolute_utc_micros_plus_monotonic_remaining_budget_at_dispatch":
        errors.append("deadline representation must preserve absolute and monotonic budget")
    if deadline.get("expired_before_dispatch_policy") != "deadline_exceeded_without_provider_call":
        errors.append("expired deadline must terminate without provider call")
    for field in ("deadline_required", "provider_must_receive_remaining_budget", "provider_operation_must_stop_after_deadline"):
        if deadline.get(field) is not True:
            errors.append(f"deadline.{field} must be true")
    if deadline.get("deadline_extension_allowed") is not False:
        errors.append("deadline extension must be false")

    cancellation = obj(control.get("cancellation"), "request_control.cancellation", errors)
    cancellation_keys = {
        "type_name",
        "live_token_required",
        "already_cancelled_policy",
        "provider_must_observe_token",
        "bounded_stop_required",
        "cancellation_as_success_allowed",
    }
    exact_keys(cancellation, cancellation_keys, "request_control.cancellation", errors)
    if cancellation.get("type_name") != "MemoryProviderCancellationV1":
        errors.append("cancellation type drifted")
    if cancellation.get("already_cancelled_policy") != "cancelled_without_provider_call":
        errors.append("already-cancelled request must terminate without provider call")
    for field in ("live_token_required", "provider_must_observe_token", "bounded_stop_required"):
        if cancellation.get(field) is not True:
            errors.append(f"cancellation.{field} must be true")
    if cancellation.get("cancellation_as_success_allowed") is not False:
        errors.append("cancellation as success must be false")


def validate_algorithm_states_receipt(contract: dict[str, Any], errors: list[str]) -> None:
    algorithm = arr(contract.get("compatibility_algorithm"), "compatibility_algorithm", errors)
    if len(algorithm) != 10 or len(set(algorithm)) != 10:
        errors.append("compatibility algorithm must contain ten unique ordered steps")
    serialized = " ".join(str(step) for step in algorithm).casefold()
    for phrase in (
        "registration revision",
        "deadline is expired",
        "exact provider_id echo",
        "mutually supported provider protocol",
        "state owner",
        "every requested capability",
        "lower host/provider ceiling",
        "ready only after all checks",
        "without fallback or fake readiness",
    ):
        if phrase not in serialized:
            errors.append(f"compatibility algorithm is missing {phrase!r}")

    states = arr(contract.get("readiness_states"), "readiness_states", errors)
    if set(states) != READINESS_STATES or len(states) != len(READINESS_STATES):
        errors.append("readiness states must exactly cover the V1 typed outcomes")

    receipt = obj(contract.get("ready_receipt"), "ready_receipt", errors)
    receipt_keys = {
        "type_name",
        "required_fields",
        "digest_algorithm",
        "portable_across_provider_restart",
        "portable_across_registration_revision",
        "portable_across_scope_revision",
        "required_before_provider_mutation",
        "required_before_provider_recall",
    }
    exact_keys(receipt, receipt_keys, "ready_receipt", errors)
    if receipt.get("type_name") != "MemoryProviderReadyReceiptV1":
        errors.append("ready receipt type drifted")
    if receipt.get("required_fields") != RECEIPT_FIELDS:
        errors.append("ready receipt required fields drifted")
    if receipt.get("digest_algorithm") != "sha256_over_canonical_json":
        errors.append("ready receipt digest algorithm must be canonical JSON SHA-256")
    for field in (
        "portable_across_provider_restart",
        "portable_across_registration_revision",
        "portable_across_scope_revision",
    ):
        if receipt.get(field) is not False:
            errors.append(f"ready_receipt.{field} must be false")
    for field in ("required_before_provider_mutation", "required_before_provider_recall"):
        if receipt.get(field) is not True:
            errors.append(f"ready_receipt.{field} must be true")


def validate_side_effects_invariants(contract: dict[str, Any], ids: set[str], errors: list[str]) -> None:
    effects = obj(contract.get("side_effect_contract"), "side_effect_contract", errors)
    exact_keys(effects, SIDE_EFFECT_FIELDS, "side_effect_contract", errors)
    if effects.get("handshake_is_read_only") is not True:
        errors.append("handshake must be read-only")
    for field in SIDE_EFFECT_FIELDS - {"handshake_is_read_only"}:
        if effects.get(field) is not False:
            errors.append(f"side_effect_contract.{field} must be false")

    invariants = arr(contract.get("invariants"), "invariants", errors)
    if len(invariants) < 13 or len(set(invariants)) != len(invariants):
        errors.append("handshake contract must state at least thirteen unique invariants")
    serialized = " ".join(str(value) for value in invariants).casefold()
    for phrase in REQUIRED_INVARIANTS:
        if phrase.casefold() not in serialized:
            errors.append(f"handshake invariants are missing {phrase!r}")

    beads = arr(contract.get("verification_beads"), "verification_beads", errors)
    if len(beads) < 8 or len(set(beads)) != len(beads):
        errors.append("verification_beads must contain at least eight unique issues")
    for value in beads:
        bead(value, "verification_beads", ids, errors)
    for required in ("tdmem-0206", "tdmem-0209", "tdmem-0304", "tdmem-0504", "tdmem-0506", "tdmem-0701", "tdmem-0702"):
        if required not in beads:
            errors.append(f"verification_beads is missing {required}")


def validate_schema(schema: dict[str, Any], errors: list[str]) -> None:
    if schema.get("$schema") != "https://json-schema.org/draft/2020-12/schema":
        errors.append("handshake schema must use JSON Schema 2020-12")
    if schema.get("type") != "object" or schema.get("additionalProperties") is not False:
        errors.append("handshake schema root must be a strict object")
    if set(schema.get("required", [])) != TOP_LEVEL:
        errors.append("handshake schema required fields must match the contract")
    properties = obj(schema.get("properties"), "schema.properties", errors)
    if properties.get("schema_version", {}).get("const") != 1:
        errors.append("handshake schema must pin schema_version 1")
    if properties.get("contract_id", {}).get("const") != "tracedecay.memory.provider.handshake.v1":
        errors.append("handshake schema must pin contract_id")
    if properties.get("bead_id", {}).get("const") != "tdmem-0202":
        errors.append("handshake schema must pin bead_id tdmem-0202")
    definitions = obj(schema.get("$defs"), "schema.$defs", errors)
    for name in ("beadId", "strictObject", "limit"):
        if name not in definitions:
            errors.append(f"handshake schema is missing $defs.{name}")
    if definitions.get("limit", {}).get("additionalProperties") is not False:
        errors.append("handshake limit schema must deny additional properties")


def validate_doc(path: Path, errors: list[str]) -> None:
    try:
        text = path.read_text(encoding="utf-8")
    except OSError as exc:
        errors.append(f"could not load handshake documentation: {exc}")
        return
    for phrase in REQUIRED_DOC_PHRASES:
        if phrase.casefold() not in text.casefold():
            errors.append(f"handshake documentation is missing {phrase!r}")
    if "TBD" in text or "TODO" in text:
        errors.append("handshake documentation contains unresolved TBD/TODO text")


def validate_dependencies(repo: Path, errors: list[str]) -> None:
    registry = load_object(
        repo / "product/contracts/memory-provider-v1/provider-registry-contract.json",
        "provider registry contract",
        errors,
    )
    if registry.get("status") != "accepted" or registry.get("contract_id") != "tracedecay.memory.provider.registry.v1":
        errors.append("handshake requires accepted provider registry V1")
    slots = {row.get("provider_id"): row for row in registry.get("bootstrap_slots", []) if isinstance(row, dict)}
    if slots.get("ncm", {}).get("implementation_gate_beads", [])[:2] != ["tdmem-0701", "tdmem-0702"]:
        errors.append("registry must keep NCM surface audit before topology selection")
    if slots.get("ocean", {}).get("counts_as_implemented") is not False:
        errors.append("registry must keep OCEAN unimplemented")


def validate(repo: Path, contract: dict[str, Any], schema: dict[str, Any], doc: Path, ids: set[str]) -> list[str]:
    errors: list[str] = []
    validate_header(contract, errors)
    validate_protocol(contract, errors)
    validate_request_response(contract, errors)
    validate_identities(contract, errors)
    validate_limits(contract, errors)
    validate_control(contract, errors)
    validate_algorithm_states_receipt(contract, errors)
    validate_side_effects_invariants(contract, ids, errors)
    validate_schema(schema, errors)
    validate_doc(doc, errors)
    validate_dependencies(repo, errors)
    return errors


def main() -> int:
    args = parse_args()
    repo = args.repo.resolve()
    contract_path = resolve(repo, args.contract)
    schema_path = resolve(repo, args.schema)
    doc_path = resolve(repo, args.doc)
    issues_path = resolve(repo, args.issues)
    bootstrap: list[str] = []
    contract = load_object(contract_path, "handshake contract", bootstrap)
    schema = load_object(schema_path, "handshake schema", bootstrap)
    ids = load_issue_ids(issues_path, bootstrap)
    if bootstrap:
        print(json.dumps({"ok": False, "errors": bootstrap}, indent=2, sort_keys=True))
        return 1
    errors = validate(repo, contract, schema, doc_path, ids)
    if errors:
        print(json.dumps({"ok": False, "errors": errors}, indent=2, sort_keys=True))
        return 1
    print(
        json.dumps(
            {
                "ok": True,
                "schema_version": contract["schema_version"],
                "contract_id": contract["contract_id"],
                "bead_id": contract["bead_id"],
                "status": contract["status"],
                "protocol": f"{contract['protocol_identity']['current_major']}.{contract['protocol_identity']['current_minor']}",
                "limit_count": len(contract["limit_catalog"]),
                "readiness_state_count": len(contract["readiness_states"]),
                "handshake_read_only": contract["side_effect_contract"]["handshake_is_read_only"],
                "silent_fallback": False,
            },
            indent=2,
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
