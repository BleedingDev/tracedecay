#!/usr/bin/env python3
"""Validate typed provider terminal, retry, fallback, and effect semantics."""

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
    "terminal_envelope",
    "terminal_codes",
    "domain_detail",
    "result_payload",
    "coverage",
    "retry",
    "fallback",
    "committed_effect",
    "request_control_precedence",
    "mandatory_operation_mapping",
    "mandatory_operation_rules",
    "terminal_validation_order",
    "invariants",
    "verification_beads",
}

ENVELOPE_FIELDS = [
    "operation_kind",
    "operation_id",
    "request_identity",
    "provider_id",
    "provider_instance_id",
    "registration_revision",
    "ready_receipt_digest",
    "exact_scope_digest",
    "started_at",
    "finished_at",
    "terminal_code",
    "domain_detail",
    "result_contract_id",
    "result_payload",
    "result_sha256",
    "coverage",
    "retry",
    "fallback",
    "committed_effect",
    "diagnostic_id",
    "warnings",
]

OPERATION_KINDS = [
    "handshake",
    "health",
    "observe",
    "recall",
    "feedback",
    "maintenance",
    "inspection",
    "correction",
    "deletion_by_source",
    "snapshot_export",
    "snapshot_restore",
    "replay",
    "explicit_fact_projection",
    "explain_trace",
]

EXPECTED_TERMINALS = {
    "success": ("success", "operation_specific", "never", "forbidden"),
    "success_zero_results": ("success", "none", "never", "forbidden"),
    "partial": (
        "degraded_success",
        "none_or_operation_specific",
        "resume_or_new_request",
        "forbidden",
    ),
    "invalid_request": (
        "caller_failure",
        "none",
        "after_request_change",
        "forbidden",
    ),
    "unauthorized": (
        "policy_failure",
        "none",
        "after_authorization_change",
        "forbidden",
    ),
    "capability_unsupported": (
        "compatibility_failure",
        "none",
        "after_provider_or_configuration_change",
        "explicit_policy_only",
    ),
    "scope_unavailable": (
        "identity_failure",
        "none",
        "after_scope_admission",
        "forbidden",
    ),
    "scope_mismatch": (
        "identity_failure",
        "none",
        "after_request_change",
        "forbidden",
    ),
    "stale_identity": (
        "identity_failure",
        "none",
        "after_identity_refresh",
        "forbidden",
    ),
    "conflict": (
        "state_failure",
        "none",
        "after_state_refresh_or_request_change",
        "forbidden",
    ),
    "capacity_exceeded": (
        "resource_failure",
        "none",
        "after_backoff_or_capacity_change",
        "forbidden",
    ),
    "deadline_exceeded": (
        "request_control_failure",
        "none_partial_or_unknown",
        "new_request_only_after_effect_reconciliation",
        "forbidden",
    ),
    "cancelled": (
        "request_control_failure",
        "none_partial_or_unknown",
        "new_request_only_after_effect_reconciliation",
        "forbidden",
    ),
    "provider_unavailable": (
        "availability_failure",
        "none_or_unknown",
        "after_bounded_backoff_and_health",
        "explicit_policy_only",
    ),
    "reset_required": (
        "operator_failure",
        "none",
        "after_operator_reset_or_migration",
        "forbidden",
    ),
    "state_incompatible": (
        "compatibility_failure",
        "none",
        "after_migration_or_compatible_state",
        "forbidden",
    ),
    "partial_effect": (
        "effect_failure",
        "partial",
        "resume_or_reconcile_before_retry",
        "forbidden",
    ),
    "effect_unknown": (
        "effect_failure",
        "unknown",
        "reconcile_before_any_retry",
        "forbidden",
    ),
    "contract_violation": (
        "protocol_failure",
        "none_partial_or_unknown",
        "after_implementation_fix_and_effect_reconciliation",
        "forbidden",
    ),
    "internal_failure": (
        "provider_failure",
        "none_partial_or_unknown",
        "after_bounded_backoff_or_fix_and_effect_reconciliation",
        "forbidden",
    ),
}

RETRY_CLASSES = {
    value[2] for value in EXPECTED_TERMINALS.values()
}

MANDATORY_OPERATIONS = {
    "provider.health.v1": ("health", "tracedecay.memory.provider.health.v1"),
    "observation.accept.v1": (
        "observe",
        "tracedecay.memory.provider.observation.v1",
    ),
    "recall.query.v1": (
        "recall",
        "tracedecay.memory.recall.query.outcome.v1",
    ),
}

REQUIRED_DOC_PHRASES = [
    "Every mandatory provider operation returns exactly one",
    "Missing, empty, malformed",
    "`cancelled` and `deadline_exceeded` are distinct",
    "Partial effects identify the exact committed boundary",
    "Unknown effects require reconciliation before any retry",
    "Automatic retry defaults to disabled",
    "Fallback eligibility is `forbidden` or `explicit_policy_only`",
    "current product policy is **no automatic fallback**",
    "An empty candidate list without typed terminal and coverage is a contract violation",
    "`partial` means degraded read/inspection coverage",
    "distinct from `partial_effect`",
    "## Duplicate acknowledgement",
    "`duplicate_of_idempotency_key` is the deterministic idempotency key the provider matched",
    "A duplicate is never inferred from an absent effect",
]

REQUIRED_INVARIANTS = [
    "Every mandatory provider operation returns one",
    "terminal-code table is closed",
    "Cancellation and deadline_exceeded are distinct",
    "Read-only operations report committed-effect state none",
    "Partial effects identify the exact committed boundary",
    "Unknown effects require reconciliation",
    "Retryability is an explicit bounded directive",
    "Automatic retry is disabled by default",
    "Fallback eligibility is explicit",
    "future fallback requires a pinned policy",
    "Successful zero results",
    "Partial read coverage and partial committed mutation effects are distinct",
    "Result payloads are canonical",
    "exact-scope identities are retained",
    "Contract violations cannot be silently repaired",
    "A duplicate committed effect states that a prior delivery of this exact mutation already committed",
]

BEAD_RE = re.compile(r"^tdmem-[0-9]{4}$")
DETAIL_RE = re.compile(r"^[a-z][a-z0-9]*(?:[._-][a-z0-9]+)*$")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--repo", type=Path, default=Path("."))
    parser.add_argument(
        "--contract",
        type=Path,
        default=Path(
            "product/contracts/memory-provider-v1/provider-terminal-contract.json"
        ),
    )
    parser.add_argument(
        "--schema",
        type=Path,
        default=Path(
            "product/contracts/memory-provider-v1/provider-terminal-contract.schema.json"
        ),
    )
    parser.add_argument(
        "--doc",
        type=Path,
        default=Path(
            "product/contracts/memory-provider-v1/provider-terminal-contract.md"
        ),
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
    result: set[str] = set()
    try:
        lines = path.read_text(encoding="utf-8").splitlines()
    except OSError as exc:
        errors.append(f"could not load Beads authority: {exc}")
        return result
    for number, line in enumerate(lines, start=1):
        if not line.strip():
            continue
        try:
            value = json.loads(line)
        except json.JSONDecodeError as exc:
            errors.append(f"invalid Beads JSONL at line {number}: {exc}")
            continue
        issue_id = value.get("id") if isinstance(value, dict) else None
        if not isinstance(issue_id, str):
            errors.append(f"Beads line {number} has no string id")
            continue
        if issue_id in result:
            errors.append(f"duplicate Beads issue id {issue_id}")
        result.add(issue_id)
    return result


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


def exact_keys(
    row: dict[str, Any], expected: set[str], label: str, errors: list[str]
) -> None:
    actual = set(row)
    if actual != expected:
        errors.append(
            f"{label} fields drifted; missing={sorted(expected - actual)}, "
            f"extra={sorted(actual - expected)}"
        )


def nonempty(
    row: dict[str, Any], field: str, label: str, errors: list[str]
) -> str:
    value = row.get(field)
    if not isinstance(value, str) or not value.strip():
        errors.append(f"{label}.{field} must be a non-empty string")
        return ""
    return value.strip()


def check_bead(value: Any, label: str, issue_ids: set[str], errors: list[str]) -> None:
    if not isinstance(value, str) or not BEAD_RE.fullmatch(value):
        errors.append(f"{label} must match tdmem-NNNN")
    elif value not in issue_ids:
        errors.append(f"{label} references unknown Beads issue {value}")


def unique_by(
    rows: Iterable[Any], field: str, label: str, errors: list[str]
) -> dict[str, dict[str, Any]]:
    result: dict[str, dict[str, Any]] = {}
    for index, raw in enumerate(rows):
        if not isinstance(raw, dict):
            errors.append(f"{label}[{index}] must be an object")
            continue
        identity = raw.get(field)
        if not isinstance(identity, str) or not identity:
            errors.append(f"{label}[{index}].{field} must be a non-empty string")
            continue
        if identity in result:
            errors.append(f"duplicate {label} {field} {identity}")
            continue
        result[identity] = raw
    return result


def validate_header(contract: dict[str, Any], errors: list[str]) -> None:
    exact_keys(contract, TOP_LEVEL, "contract", errors)
    if contract.get("schema_version") != 1:
        errors.append("schema_version must be 1")
    if contract.get("contract_id") != "tracedecay.memory.provider.terminal.v1":
        errors.append("contract_id must be tracedecay.memory.provider.terminal.v1")
    if contract.get("bead_id") != "tdmem-0206":
        errors.append("bead_id must be tdmem-0206")
    if contract.get("status") != "accepted":
        errors.append("contract status must be accepted")
    if contract.get("authority") != "TraceDecay provider protocol terminal envelope":
        errors.append("terminal authority must remain TraceDecay provider protocol")
    if contract.get("scope") != "coding_agents_only":
        errors.append("terminal scope must remain coding_agents_only")
    if contract.get("depends_on_contracts") != [
        "tracedecay.memory.provider.registry.v1",
        "tracedecay.memory.provider.handshake.v1",
        "tracedecay.memory.provider.observation.v1",
        "tracedecay.memory.provider.recall.v1",
        "tracedecay.memory.provider.lifecycle.v1",
    ]:
        errors.append("terminal dependencies must include all accepted M1 contracts")
    nonempty(contract, "title", "contract", errors)


def validate_envelope(contract: dict[str, Any], errors: list[str]) -> None:
    envelope = obj(contract.get("terminal_envelope"), "terminal_envelope", errors)
    keys = {
        "type_name",
        "contract_id",
        "required_fields",
        "operation_kinds",
        "unknown_field_policy",
        "maximum_warnings",
        "diagnostic_id_required_for_failure",
        "provider_may_omit_terminal_envelope",
        "empty_response_allowed",
    }
    exact_keys(envelope, keys, "terminal_envelope", errors)
    if envelope.get("type_name") != "MemoryProviderTerminalEnvelopeV1":
        errors.append("terminal envelope type drifted")
    if envelope.get("contract_id") != (
        "tracedecay.memory.provider.terminal-envelope.v1"
    ):
        errors.append("terminal envelope wire contract ID drifted")
    if envelope.get("required_fields") != ENVELOPE_FIELDS:
        errors.append("terminal envelope required fields drifted")
    if envelope.get("operation_kinds") != OPERATION_KINDS:
        errors.append("terminal operation kinds drifted")
    if envelope.get("unknown_field_policy") != "reject_contract_violation":
        errors.append("unknown terminal-envelope fields must fail closed")
    maximum = envelope.get("maximum_warnings")
    if not isinstance(maximum, int) or not 0 <= maximum <= 32:
        errors.append("terminal warnings must be bounded at 32")
    if envelope.get("diagnostic_id_required_for_failure") is not True:
        errors.append("failure terminal requires diagnostic ID")
    if envelope.get("provider_may_omit_terminal_envelope") is not False:
        errors.append("provider may not omit terminal envelope")
    if envelope.get("empty_response_allowed") is not False:
        errors.append("empty provider response must be forbidden")


def validate_terminal_table(contract: dict[str, Any], errors: list[str]) -> None:
    rows = unique_by(
        arr(contract.get("terminal_codes"), "terminal_codes", errors),
        "code",
        "terminal_codes",
        errors,
    )
    if set(rows) != set(EXPECTED_TERMINALS):
        errors.append("terminal-code table must exactly contain the twenty V1 codes")
    expected_fields = {
        "code",
        "class",
        "effect_expectation",
        "retry_class",
        "fallback_eligibility",
    }
    for code, expected in EXPECTED_TERMINALS.items():
        row = rows.get(code, {})
        exact_keys(row, expected_fields, f"terminal[{code}]", errors)
        actual = (
            row.get("class"),
            row.get("effect_expectation"),
            row.get("retry_class"),
            row.get("fallback_eligibility"),
        )
        if actual != expected:
            errors.append(f"terminal {code} semantics drifted")
    if rows.get("cancelled", {}).get("class") != rows.get(
        "deadline_exceeded", {}
    ).get("class"):
        errors.append("cancelled and deadline must share request-control class")
    if rows.get("cancelled", {}).get("code") == rows.get(
        "deadline_exceeded", {}
    ).get("code"):
        errors.append("cancelled and deadline must remain distinct codes")
    if rows.get("partial_effect", {}).get("effect_expectation") != "partial":
        errors.append("partial_effect must require partial committed effect")
    if rows.get("effect_unknown", {}).get("effect_expectation") != "unknown":
        errors.append("effect_unknown must require unknown committed effect")


def validate_detail_result_coverage(contract: dict[str, Any], errors: list[str]) -> None:
    detail = obj(contract.get("domain_detail"), "domain_detail", errors)
    keys = {
        "type_name",
        "required_fields",
        "detail_id_pattern",
        "detail_version_minimum",
        "maximum_payload_bytes",
        "detail_may_change_terminal_semantics",
        "detail_may_change_retry_or_fallback",
        "unknown_optional_detail_policy",
        "unknown_required_detail_policy",
    }
    exact_keys(detail, keys, "domain_detail", errors)
    if detail.get("type_name") != "MemoryProviderDomainDetailV1":
        errors.append("domain detail type drifted")
    if detail.get("required_fields") != [
        "detail_id",
        "detail_version",
        "canonical_payload",
        "payload_sha256",
    ]:
        errors.append("domain detail fields drifted")
    pattern_raw = detail.get("detail_id_pattern")
    try:
        pattern = re.compile(pattern_raw) if isinstance(pattern_raw, str) else None
    except re.error as exc:
        errors.append(f"domain detail pattern invalid: {exc}")
        pattern = None
    if pattern is None or pattern.fullmatch("scope.stale") is None:
        errors.append("domain detail pattern must accept canonical IDs")
    if pattern is not None and pattern.fullmatch("Bad/Detail") is not None:
        errors.append("domain detail pattern accepts invalid ID")
    if detail.get("detail_version_minimum") != 1:
        errors.append("domain detail version minimum must be one")
    maximum = detail.get("maximum_payload_bytes")
    if not isinstance(maximum, int) or not 1 <= maximum <= 131072:
        errors.append("domain detail payload must be bounded at 131072 bytes")
    for field in (
        "detail_may_change_terminal_semantics",
        "detail_may_change_retry_or_fallback",
    ):
        if detail.get(field) is not False:
            errors.append(f"domain_detail.{field} must be false")
    if detail.get("unknown_optional_detail_policy") != (
        "preserve_opaque_inert_round_trip"
    ):
        errors.append("unknown optional detail must round-trip inertly")
    if detail.get("unknown_required_detail_policy") != (
        "reject_contract_violation"
    ):
        errors.append("unknown required detail must fail contract validation")

    result = obj(contract.get("result_payload"), "result_payload", errors)
    keys = {
        "type_name",
        "success_requires_result_contract_unless_operation_defines_empty_success",
        "failure_result_payload_allowed",
        "result_contract_id_required_when_payload_present",
        "result_sha256_required_when_payload_present",
        "canonical_encoding",
        "maximum_payload_bytes_source",
        "unknown_result_contract_policy",
    }
    exact_keys(result, keys, "result_payload", errors)
    if result.get("type_name") != "MemoryProviderResultPayloadV1":
        errors.append("result payload type drifted")
    if result.get(
        "success_requires_result_contract_unless_operation_defines_empty_success"
    ) is not True:
        errors.append("successful result requires typed payload unless explicitly empty")
    if result.get("failure_result_payload_allowed") is not False:
        errors.append("failure result payload must be forbidden in V1")
    for field in (
        "result_contract_id_required_when_payload_present",
        "result_sha256_required_when_payload_present",
    ):
        if result.get(field) is not True:
            errors.append(f"result_payload.{field} must be true")
    if result.get("canonical_encoding") != "rfc8785_json":
        errors.append("result payload must use RFC8785 canonical JSON")
    if result.get("maximum_payload_bytes_source") != "effective_limit.response_bytes":
        errors.append("result payload limit must come from handshake")
    if result.get("unknown_result_contract_policy") != "reject_contract_violation":
        errors.append("unknown result contract must fail closed")

    coverage = obj(contract.get("coverage"), "coverage", errors)
    keys = {
        "type_name",
        "required_fields",
        "states",
        "partial_requires_reason",
        "success_zero_results_requires_zero_results_coverage",
        "partial_terminal_requires_partial_coverage",
        "failure_cannot_claim_complete_coverage",
        "resume_cursor_is_scope_request_and_state_bound",
    }
    exact_keys(coverage, keys, "coverage", errors)
    if coverage.get("type_name") != "MemoryProviderTerminalCoverageV1":
        errors.append("terminal coverage type drifted")
    if coverage.get("required_fields") != [
        "state",
        "completed_units",
        "total_units",
        "excluded_units",
        "truncated_units",
        "resume_cursor",
        "reasons",
    ]:
        errors.append("terminal coverage fields drifted")
    if coverage.get("states") != [
        "not_applicable",
        "complete",
        "partial",
        "zero_results",
    ]:
        errors.append("terminal coverage states drifted")
    for field in (
        "partial_requires_reason",
        "success_zero_results_requires_zero_results_coverage",
        "partial_terminal_requires_partial_coverage",
        "failure_cannot_claim_complete_coverage",
        "resume_cursor_is_scope_request_and_state_bound",
    ):
        if coverage.get(field) is not True:
            errors.append(f"coverage.{field} must be true")


def validate_retry_fallback(contract: dict[str, Any], errors: list[str]) -> None:
    retry = obj(contract.get("retry"), "retry", errors)
    keys = {
        "type_name",
        "required_fields",
        "classes",
        "automatic_retry_default",
        "automatic_retry_requires_explicit_policy_and_positive_budget",
        "minimum_backoff_millis_minimum",
        "maximum_attempts_remaining_minimum",
        "unbounded_attempts_allowed",
        "retry_may_reuse_new_idempotency_key_for_same_mutation",
        "retry_may_begin_before_unknown_or_partial_effect_reconciliation",
    }
    exact_keys(retry, keys, "retry", errors)
    if retry.get("type_name") != "MemoryProviderRetryDirectiveV1":
        errors.append("retry directive type drifted")
    if retry.get("required_fields") != [
        "class",
        "automatic_retry_allowed",
        "minimum_backoff_millis",
        "maximum_attempts_remaining",
        "requires_identity_refresh",
        "requires_state_reconciliation",
        "requires_operator_action",
        "resume_cursor",
    ]:
        errors.append("retry directive fields drifted")
    if set(retry.get("classes", [])) != RETRY_CLASSES:
        errors.append("retry classes must exactly match terminal-code table")
    if retry.get("automatic_retry_default") is not False:
        errors.append("automatic retry must default to false")
    if retry.get(
        "automatic_retry_requires_explicit_policy_and_positive_budget"
    ) is not True:
        errors.append("automatic retry requires explicit policy and positive budget")
    if retry.get("minimum_backoff_millis_minimum") != 0:
        errors.append("minimum backoff lower bound must be zero")
    if retry.get("maximum_attempts_remaining_minimum") != 0:
        errors.append("attempt budget lower bound must be zero")
    for field in (
        "unbounded_attempts_allowed",
        "retry_may_reuse_new_idempotency_key_for_same_mutation",
        "retry_may_begin_before_unknown_or_partial_effect_reconciliation",
    ):
        if retry.get(field) is not False:
            errors.append(f"retry.{field} must be false")

    fallback = obj(contract.get("fallback"), "fallback", errors)
    keys = {
        "type_name",
        "required_fields",
        "eligibility_values",
        "default_eligibility",
        "fallback_may_be_inferred_from_empty_result",
        "fallback_may_be_inferred_from_provider_unavailable",
        "fallback_requires_pinned_policy",
        "fallback_requires_explicit_target_provider",
        "fallback_requires_new_handshake_and_scope_admission",
        "fallback_may_reuse_provider_specific_state_identity",
        "current_product_policy",
    }
    exact_keys(fallback, keys, "fallback", errors)
    if fallback.get("type_name") != "MemoryProviderFallbackDirectiveV1":
        errors.append("fallback directive type drifted")
    if fallback.get("required_fields") != [
        "eligibility",
        "policy_id",
        "policy_revision",
        "target_provider_id",
        "reason",
    ]:
        errors.append("fallback directive fields drifted")
    if fallback.get("eligibility_values") != [
        "forbidden",
        "explicit_policy_only",
    ]:
        errors.append("fallback eligibility values drifted")
    if fallback.get("default_eligibility") != "forbidden":
        errors.append("fallback must default to forbidden")
    for field in (
        "fallback_may_be_inferred_from_empty_result",
        "fallback_may_be_inferred_from_provider_unavailable",
        "fallback_may_reuse_provider_specific_state_identity",
    ):
        if fallback.get(field) is not False:
            errors.append(f"fallback.{field} must be false")
    for field in (
        "fallback_requires_pinned_policy",
        "fallback_requires_explicit_target_provider",
        "fallback_requires_new_handshake_and_scope_admission",
    ):
        if fallback.get(field) is not True:
            errors.append(f"fallback.{field} must be true")
    if fallback.get("current_product_policy") != "no_automatic_fallback":
        errors.append("current product policy must forbid automatic fallback")


def validate_effect_control(contract: dict[str, Any], errors: list[str]) -> None:
    effect = obj(contract.get("committed_effect"), "committed_effect", errors)
    effect_true_fields = (
        "read_only_operation_requires_none",
        "success_mutation_requires_committed_duplicate_or_none_if_no_effect",
        "partial_effect_terminal_requires_partial",
        "effect_unknown_terminal_requires_unknown",
        "deadline_or_cancelled_may_report_none_partial_or_unknown",
        "partial_requires_committed_boundary",
        "partial_requires_committed_and_uncommitted_item_sets",
        "unknown_requires_reconciliation_action",
        "effect_receipt_required_when_state_not_none",
        "same_mutation_retry_requires_same_idempotency_key",
        "duplicate_means_prior_delivery_of_this_mutation_already_committed",
        "duplicate_requires_matching_request_idempotency_key",
        "duplicate_requires_original_operation_identity",
        "duplicate_requires_unchanged_state_generation",
        "duplicate_identity_forbidden_unless_duplicate",
    )
    keys = {
        "type_name",
        "required_fields",
        "states",
        "duplicate_may_be_inferred_from_absent_effect",
        *effect_true_fields,
    }
    exact_keys(effect, keys, "committed_effect", errors)
    if effect.get("type_name") != "MemoryProviderCommittedEffectV1":
        errors.append("committed-effect type drifted")
    if effect.get("required_fields") != [
        "state",
        "committed_boundary",
        "state_generation_before",
        "state_generation_after",
        "committed_item_refs",
        "uncommitted_item_refs",
        "provider_receipt_digest",
        "reconciliation_action",
        "verification_digest",
        "duplicate_of_idempotency_key",
        "duplicate_of_operation_id",
    ]:
        errors.append("committed-effect fields drifted")
    if effect.get("states") != [
        "none",
        "committed",
        "duplicate",
        "partial",
        "unknown",
    ]:
        errors.append("committed-effect states drifted")
    for field in effect_true_fields:
        if effect.get(field) is not True:
            errors.append(f"committed_effect.{field} must be true")
    if effect.get("duplicate_may_be_inferred_from_absent_effect") is not False:
        errors.append(
            "committed_effect.duplicate_may_be_inferred_from_absent_effect must be false"
        )

    control = obj(
        contract.get("request_control_precedence"),
        "request_control_precedence",
        errors,
    )
    keys = {
        "already_cancelled_before_dispatch",
        "expired_deadline_before_dispatch",
        "both_terminal_before_dispatch",
        "during_provider_operation",
        "cancellation_may_be_reported_as_timeout",
        "timeout_may_be_reported_as_cancellation",
        "request_control_may_be_reported_as_success",
    }
    exact_keys(control, keys, "request_control_precedence", errors)
    if control.get("already_cancelled_before_dispatch") != (
        "cancelled_without_provider_call"
    ):
        errors.append("already-cancelled request must not call provider")
    if control.get("expired_deadline_before_dispatch") != (
        "deadline_exceeded_without_provider_call"
    ):
        errors.append("expired deadline must not call provider")
    if control.get("both_terminal_before_dispatch") != (
        "earliest_monotonic_terminal_event_wins"
    ):
        errors.append("request-control precedence must use earliest monotonic event")
    nonempty(control, "during_provider_operation", "request_control_precedence", errors)
    for field in (
        "cancellation_may_be_reported_as_timeout",
        "timeout_may_be_reported_as_cancellation",
        "request_control_may_be_reported_as_success",
    ):
        if control.get(field) is not False:
            errors.append(f"request_control_precedence.{field} must be false")


def validate_mandatory_operations(
    contract: dict[str, Any], registry: dict[str, Any], errors: list[str]
) -> None:
    rows = unique_by(
        arr(
            contract.get("mandatory_operation_mapping"),
            "mandatory_operation_mapping",
            errors,
        ),
        "capability_id",
        "mandatory_operation_mapping",
        errors,
    )
    if set(rows) != set(MANDATORY_OPERATIONS):
        errors.append("mandatory operation map must exactly cover health, observe, recall")
    for capability_id, expected in MANDATORY_OPERATIONS.items():
        row = rows.get(capability_id, {})
        exact_keys(
            row,
            {"capability_id", "operation_kind", "result_contract_id"},
            f"mandatory_operation[{capability_id}]",
            errors,
        )
        if (row.get("operation_kind"), row.get("result_contract_id")) != expected:
            errors.append(f"mandatory operation {capability_id} mapping drifted")

    capability_registry = registry.get("capability_registry")
    mandatory = (
        capability_registry.get("mandatory", [])
        if isinstance(capability_registry, dict)
        else []
    )
    registry_ids = {
        row.get("id")
        for row in mandatory
        if isinstance(row, dict) and row.get("requirement") == "mandatory"
    }
    if set(MANDATORY_OPERATIONS) - registry_ids:
        errors.append("registry mandatory capabilities do not match terminal map")

    rules = obj(
        contract.get("mandatory_operation_rules"),
        "mandatory_operation_rules",
        errors,
    )
    keys = {
        "every_call_returns_terminal_envelope",
        "missing_envelope_is_contract_violation",
        "empty_transport_response_is_contract_violation",
        "operation_specific_payload_is_nested_under_terminal_envelope",
        "operation_specific_failure_may_bypass_terminal_envelope",
    }
    exact_keys(rules, keys, "mandatory_operation_rules", errors)
    for field in (
        "every_call_returns_terminal_envelope",
        "missing_envelope_is_contract_violation",
        "empty_transport_response_is_contract_violation",
        "operation_specific_payload_is_nested_under_terminal_envelope",
    ):
        if rules.get(field) is not True:
            errors.append(f"mandatory_operation_rules.{field} must be true")
    if rules.get("operation_specific_failure_may_bypass_terminal_envelope") is not False:
        errors.append("operation-specific failure cannot bypass terminal envelope")


def validate_order_invariants_beads(
    contract: dict[str, Any], issue_ids: set[str], errors: list[str]
) -> None:
    order = arr(
        contract.get("terminal_validation_order"),
        "terminal_validation_order",
        errors,
    )
    if len(order) != 10 or len(set(order)) != 10:
        errors.append("terminal validation order must contain ten unique steps")
    text = " ".join(str(value) for value in order).casefold()
    for phrase in (
        "terminal envelope contract identity",
        "provider, registration, ready receipt, exact scope",
        "closed terminal-code table",
        "result payload presence",
        "coverage state",
        "retry directive",
        "fallback directive",
        "committed-effect state",
        "cancellation and timeout",
        "contract_violation",
    ):
        if phrase not in text:
            errors.append(f"terminal validation order is missing {phrase!r}")

    invariants = arr(contract.get("invariants"), "invariants", errors)
    if len(invariants) < 15 or len(set(invariants)) != len(invariants):
        errors.append("terminal contract must state at least fifteen unique invariants")
    text = " ".join(str(value) for value in invariants).casefold()
    for phrase in REQUIRED_INVARIANTS:
        if phrase.casefold() not in text:
            errors.append(f"terminal invariants are missing {phrase!r}")

    beads = arr(contract.get("verification_beads"), "verification_beads", errors)
    if len(beads) < 10 or len(set(beads)) != len(beads):
        errors.append("verification_beads must contain at least ten unique issues")
    for value in beads:
        check_bead(value, "verification_beads", issue_ids, errors)
    for required in (
        "tdmem-0207",
        "tdmem-0208",
        "tdmem-0209",
        "tdmem-0503",
        "tdmem-0504",
        "tdmem-0506",
        "tdmem-0601",
        "tdmem-0703",
        "tdmem-0903",
    ):
        if required not in beads:
            errors.append(f"verification_beads is missing {required}")


def validate_schema(schema: dict[str, Any], errors: list[str]) -> None:
    if schema.get("$schema") != "https://json-schema.org/draft/2020-12/schema":
        errors.append("terminal schema must use JSON Schema 2020-12")
    if schema.get("type") != "object" or schema.get("additionalProperties") is not False:
        errors.append("terminal schema root must be a strict object")
    if set(schema.get("required", [])) != TOP_LEVEL:
        errors.append("terminal schema required fields must match contract")
    properties = obj(schema.get("properties"), "schema.properties", errors)
    if properties.get("schema_version", {}).get("const") != 1:
        errors.append("terminal schema must pin schema_version 1")
    if properties.get("contract_id", {}).get("const") != (
        "tracedecay.memory.provider.terminal.v1"
    ):
        errors.append("terminal schema must pin contract_id")
    if properties.get("bead_id", {}).get("const") != "tdmem-0206":
        errors.append("terminal schema must pin bead_id tdmem-0206")
    if properties.get("terminal_codes", {}).get("minItems") != 20:
        errors.append("terminal schema must require twenty terminal codes")
    definitions = obj(schema.get("$defs"), "schema.$defs", errors)
    for name in ("beadId", "object", "terminalCode", "mandatoryOperation"):
        if name not in definitions:
            errors.append(f"terminal schema is missing $defs.{name}")
    if definitions.get("terminalCode", {}).get("additionalProperties") is not False:
        errors.append("terminal-code schema must be strict")


def validate_doc(path: Path, errors: list[str]) -> None:
    try:
        text = path.read_text(encoding="utf-8")
    except OSError as exc:
        errors.append(f"could not load terminal documentation: {exc}")
        return
    for phrase in REQUIRED_DOC_PHRASES:
        if phrase.casefold() not in text.casefold():
            errors.append(f"terminal documentation is missing {phrase!r}")
    if "TBD" in text or "TODO" in text:
        errors.append("terminal documentation contains unresolved TBD/TODO text")


def validate_dependencies(repo: Path, errors: list[str]) -> dict[str, Any]:
    filenames = [
        "provider-registry-contract.json",
        "provider-handshake-contract.json",
        "provider-observation-contract.json",
        "provider-recall-contract.json",
        "provider-lifecycle-contract.json",
    ]
    contracts: dict[str, Any] = {}
    for filename in filenames:
        contract = load_object(
            repo / "product/contracts/memory-provider-v1" / filename,
            filename,
            errors,
        )
        contracts[filename] = contract
        if contract.get("status") != "accepted":
            errors.append(f"terminal contract requires accepted {filename}")
    lifecycle_states = set(
        contracts.get("provider-lifecycle-contract.json", {}).get(
            "lifecycle_specific_terminal_states", []
        )
    )
    expected_mappable = {
        "success",
        "partial_effect",
        "effect_unknown",
        "invalid_request",
        "unauthorized",
        "capability_unsupported",
        "scope_unavailable",
        "scope_mismatch",
        "stale_identity",
        "deadline_exceeded",
        "cancelled",
        "provider_unavailable",
        "state_incompatible",
        "reset_required",
        "contract_violation",
        "internal_failure",
    }
    if not expected_mappable.issubset(lifecycle_states):
        errors.append("lifecycle terminal states cannot map to canonical terminal table")
    return contracts


def validate(
    repo: Path,
    contract: dict[str, Any],
    schema: dict[str, Any],
    doc_path: Path,
    issue_ids: set[str],
) -> list[str]:
    errors: list[str] = []
    dependencies = validate_dependencies(repo, errors)
    validate_header(contract, errors)
    validate_envelope(contract, errors)
    validate_terminal_table(contract, errors)
    validate_detail_result_coverage(contract, errors)
    validate_retry_fallback(contract, errors)
    validate_effect_control(contract, errors)
    validate_mandatory_operations(
        contract,
        dependencies.get("provider-registry-contract.json", {}),
        errors,
    )
    validate_order_invariants_beads(contract, issue_ids, errors)
    validate_schema(schema, errors)
    validate_doc(doc_path, errors)
    return errors


def main() -> int:
    args = parse_args()
    repo = args.repo.resolve()
    bootstrap: list[str] = []
    contract = load_object(resolve(repo, args.contract), "terminal contract", bootstrap)
    schema = load_object(resolve(repo, args.schema), "terminal schema", bootstrap)
    issue_ids = load_issue_ids(resolve(repo, args.issues), bootstrap)
    if bootstrap:
        print(json.dumps({"ok": False, "errors": bootstrap}, indent=2, sort_keys=True))
        return 1
    errors = validate(repo, contract, schema, resolve(repo, args.doc), issue_ids)
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
                "terminal_code_count": len(contract["terminal_codes"]),
                "mandatory_operation_count": len(
                    contract["mandatory_operation_mapping"]
                ),
                "automatic_retry_default": contract["retry"][
                    "automatic_retry_default"
                ],
                "fallback_default": contract["fallback"]["default_eligibility"],
                "current_fallback_policy": contract["fallback"][
                    "current_product_policy"
                ],
                "effect_states": contract["committed_effect"]["states"],
                "cancelled_distinct_from_timeout": True,
            },
            indent=2,
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
