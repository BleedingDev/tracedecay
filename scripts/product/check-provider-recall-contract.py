#!/usr/bin/env python3
"""Validate provider-neutral recall scope, temporal, score, and provenance semantics."""

from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path
from typing import Any

TOP_LEVEL = {
    "schema_version",
    "contract_id",
    "bead_id",
    "title",
    "status",
    "authority",
    "scope",
    "depends_on_contracts",
    "recall_request",
    "exact_scope_semantics",
    "temporal_query",
    "budgets",
    "exclusions",
    "extension_contract",
    "provider_candidate",
    "candidate_scope_binding",
    "content_reference",
    "native_score",
    "host_normalized_score",
    "validity",
    "provenance",
    "explanation",
    "recall_response",
    "coverage",
    "ordering",
    "recall_specific_terminal_states",
    "invariants",
    "verification_beads",
}

REQUEST_FIELDS = [
    "provider_id",
    "registration_revision",
    "ready_receipt_digest",
    "exact_scope_identity",
    "request_identity",
    "objective",
    "query",
    "temporal_query",
    "budgets",
    "exclusions",
    "required_capabilities",
    "policy_revision",
    "extensions",
    "deadline",
    "cancellation",
]

SCOPE_FIELDS = [
    "profile_id",
    "project_id",
    "repository_identity",
    "worktree_identity",
    "branch_identity",
    "agent_session_id",
    "resolved_scope_digest",
]

TEMPORAL_FIELDS = [
    "mode",
    "evaluation_time",
    "as_of",
    "interval_start",
    "interval_end",
    "include_superseded",
    "include_revoked",
    "unknown_validity_policy",
]

BUDGET_FIELDS = [
    "maximum_candidates",
    "maximum_candidate_content_bytes",
    "maximum_total_content_bytes",
    "maximum_source_refs_per_candidate",
    "maximum_trace_refs_per_candidate",
    "maximum_warnings",
    "maximum_extensions_per_candidate",
]

EXCLUSION_FIELDS = [
    "stable_memory_refs",
    "candidate_ids",
    "source_refs",
    "trace_refs",
    "observation_ids",
    "content_sha256",
]

CANDIDATE_FIELDS = [
    "candidate_id",
    "stable_memory_ref",
    "content",
    "content_ref",
    "content_sha256",
    "native_score",
    "confidence",
    "exact_scope_identity",
    "validity",
    "provenance",
    "explanation",
    "source_refs",
    "trace_refs",
    "sensitivity",
    "memory_class",
    "warnings",
    "extensions",
]

SCOPE_BINDINGS = [
    "exact_coding_scope",
    "project_facts",
    "profile_facts",
]

CANDIDATE_SCOPE_FIELDS = ["scope_binding", *SCOPE_FIELDS]

BINDING_RULES = {
    "exact_coding_scope": {
        "required_equal": SCOPE_FIELDS,
        "optional_empty_or_equal": [],
        "forbidden": [],
    },
    "project_facts": {
        "required_equal": ["profile_id", "project_id"],
        "optional_empty_or_equal": [
            "repository_identity",
            "worktree_identity",
            "branch_identity",
        ],
        "forbidden": ["agent_session_id", "resolved_scope_digest"],
    },
    "profile_facts": {
        "required_equal": ["profile_id"],
        "optional_empty_or_equal": [],
        "forbidden": [
            "project_id",
            "repository_identity",
            "worktree_identity",
            "branch_identity",
            "agent_session_id",
            "resolved_scope_digest",
        ],
    },
}

NATIVE_SCORE_FIELDS = [
    "score_domain_id",
    "score_domain_version",
    "raw_value",
    "direction",
    "declared_minimum",
    "declared_maximum",
    "calibration_state",
    "semantics",
    "components",
]

VALIDITY_FIELDS = [
    "observed_at",
    "valid_from",
    "valid_until",
    "superseded_at",
    "superseded_by",
    "revoked_at",
    "source_revision",
    "temporal_state",
]

PROVENANCE_FIELDS = [
    "state",
    "origin_refs",
    "observation_refs",
    "source_refs",
    "transform_chain",
    "provider_trace_refs",
    "redaction_reason",
]

RESPONSE_FIELDS = [
    "provider_id",
    "provider_instance_id",
    "registration_revision",
    "ready_receipt_digest",
    "request_identity",
    "exact_scope_identity",
    "provider_state_generation",
    "candidates",
    "coverage",
    "ordering",
    "terminal",
    "warnings",
]

COVERAGE_FIELDS = [
    "state",
    "searched_scope_digest",
    "searched_temporal_digest",
    "scanned_items",
    "matched_items",
    "returned_items",
    "excluded_items",
    "truncated_items",
    "next_cursor",
    "reasons",
]

TERMINAL_STATES = {
    "success",
    "success_zero_results",
    "partial",
    "invalid_request",
    "capability_unsupported",
    "scope_unavailable",
    "scope_mismatch",
    "stale_scope",
    "extension_unsupported",
    "provenance_policy_rejected_all",
    "budget_exhausted",
    "deadline_exceeded",
    "cancelled",
    "provider_unavailable",
    "state_incompatible",
    "contract_violation",
    "internal_failure",
}

REQUIRED_DOC_PHRASES = [
    "Provider recall is a bounded advisory read",
    "exact profile, project, repository, worktree, branch, agent-session, and scope-revision identity",
    "Stable provider memory references are optional",
    "Provider-native scores are **not cross-provider comparable**",
    "Only the TraceDecay context compiler may create",
    "Missing provenance is never represented by an empty successful object",
    "unknown optional extensions round-trip as inert opaque data",
    "An empty candidate list is never a failure or fallback signal",
    "TraceDecay alone validates, normalizes, deduplicates, budgets, formats, explains, and assembles candidates",
    "The host, not the provider, decides which bindings a provider may attest",
]

REQUIRED_INVARIANT_PHRASES = [
    "bounded advisory read",
    "exact profile/project/repository/worktree/branch/session scope",
    "Stable provider memory references are optional",
    "exactly one of inline content or a content reference",
    "cannot be compared across providers",
    "cannot supply the host-normalized score",
    "Temporal mode",
    "cross-worktree",
    "Missing provenance is an explicit",
    "not proof",
    "Unknown optional extensions round-trip",
    "positive, finite",
    "Exclusions are mandatory",
    "Successful zero results",
    "deterministic provider ordering",
    "TraceDecay alone admits",
    "explicit scope binding",
]

BEAD_RE = re.compile(r"^tdmem-[0-9]{4}$")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--repo", type=Path, default=Path("."))
    parser.add_argument(
        "--contract",
        type=Path,
        default=Path(
            "product/contracts/memory-provider-v1/provider-recall-contract.json"
        ),
    )
    parser.add_argument(
        "--schema",
        type=Path,
        default=Path(
            "product/contracts/memory-provider-v1/provider-recall-contract.schema.json"
        ),
    )
    parser.add_argument(
        "--doc",
        type=Path,
        default=Path(
            "product/contracts/memory-provider-v1/provider-recall-contract.md"
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
        if issue_id in ids:
            errors.append(f"duplicate Beads issue id {issue_id}")
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


def validate_bead(value: Any, label: str, ids: set[str], errors: list[str]) -> None:
    if not isinstance(value, str) or not BEAD_RE.fullmatch(value):
        errors.append(f"{label} must match tdmem-NNNN")
    elif value not in ids:
        errors.append(f"{label} references unknown Beads issue {value}")


def validate_header(contract: dict[str, Any], errors: list[str]) -> None:
    exact_keys(contract, TOP_LEVEL, "contract", errors)
    if contract.get("schema_version") != 1:
        errors.append("schema_version must be 1")
    if contract.get("contract_id") != "tracedecay.memory.provider.recall.v1":
        errors.append("contract_id must be tracedecay.memory.provider.recall.v1")
    if contract.get("bead_id") != "tdmem-0204":
        errors.append("bead_id must be tdmem-0204")
    if contract.get("status") != "accepted":
        errors.append("contract status must be accepted")
    if contract.get("authority") != "TraceDecay recall admission and context compiler":
        errors.append("recall authority must remain TraceDecay admission/context compiler")
    if contract.get("scope") != "coding_agents_only":
        errors.append("recall scope must remain coding_agents_only")
    if contract.get("depends_on_contracts") != [
        "tracedecay.memory.provider.registry.v1",
        "tracedecay.memory.provider.handshake.v1",
    ]:
        errors.append("recall dependencies must be registry then handshake V1")
    nonempty(contract, "title", "contract", errors)


def validate_request_scope_temporal(
    contract: dict[str, Any], errors: list[str]
) -> None:
    request = obj(contract.get("recall_request"), "recall_request", errors)
    request_keys = {
        "type_name",
        "contract_id",
        "required_fields",
        "objective_minimum_bytes",
        "objective_maximum_bytes",
        "query_minimum_bytes",
        "query_maximum_bytes",
        "required_capabilities",
        "unknown_field_policy",
        "empty_query_allowed",
        "provider_may_widen_scope",
        "provider_may_extend_deadline",
    }
    exact_keys(request, request_keys, "recall_request", errors)
    if request.get("type_name") != "MemoryProviderRecallRequestV1":
        errors.append("recall request type drifted")
    if request.get("contract_id") != "tracedecay.memory.recall.query.request.v1":
        errors.append("recall request wire contract ID drifted")
    if request.get("required_fields") != REQUEST_FIELDS:
        errors.append("recall request required fields must remain canonical and ordered")
    for minimum_field, maximum_field, hard_max in (
        ("objective_minimum_bytes", "objective_maximum_bytes", 8192),
        ("query_minimum_bytes", "query_maximum_bytes", 32768),
    ):
        minimum = request.get(minimum_field)
        maximum = request.get(maximum_field)
        if not (
            isinstance(minimum, int)
            and isinstance(maximum, int)
            and 1 <= minimum <= maximum <= hard_max
        ):
            errors.append(
                f"{minimum_field}/{maximum_field} must be positive and bounded"
            )
    if request.get("required_capabilities") != ["recall.query.v1"]:
        errors.append("recall request must require recall.query.v1")
    if request.get("unknown_field_policy") != "reject_contract_violation":
        errors.append("unknown recall request fields must fail closed")
    for field in (
        "empty_query_allowed",
        "provider_may_widen_scope",
        "provider_may_extend_deadline",
    ):
        if request.get(field) is not False:
            errors.append(f"recall_request.{field} must be false")

    scope = obj(
        contract.get("exact_scope_semantics"), "exact_scope_semantics", errors
    )
    scope_keys = {
        "type_name",
        "required_fields",
        "match_semantics",
        "wildcards_allowed",
        "provider_path_inference_allowed",
        "cwd_inference_allowed",
        "repository_only_match_allowed",
        "cross_worktree_recall_allowed",
        "cross_branch_recall_allowed",
        "cross_session_recall_allowed",
        "cross_scope_candidate_policy",
    }
    exact_keys(scope, scope_keys, "exact_scope_semantics", errors)
    if scope.get("type_name") != "MemoryProviderRecallExactScopeV1":
        errors.append("recall exact-scope type drifted")
    if scope.get("required_fields") != SCOPE_FIELDS:
        errors.append("recall exact scope fields must remain canonical and ordered")
    if scope.get("match_semantics") != "all_required_identity_fields_must_match_exactly":
        errors.append("recall scope must require exact identity match")
    for field in (
        "wildcards_allowed",
        "provider_path_inference_allowed",
        "cwd_inference_allowed",
        "repository_only_match_allowed",
        "cross_worktree_recall_allowed",
        "cross_branch_recall_allowed",
        "cross_session_recall_allowed",
    ):
        if scope.get(field) is not False:
            errors.append(f"exact_scope_semantics.{field} must be false")
    if scope.get("cross_scope_candidate_policy") != "reject_scope_mismatch":
        errors.append("cross-scope candidates must reject as scope mismatch")

    temporal = obj(contract.get("temporal_query"), "temporal_query", errors)
    temporal_keys = {
        "type_name",
        "required_fields",
        "modes",
        "time_representation",
        "current_semantics",
        "as_of_semantics",
        "interval_semantics",
        "history_semantics",
        "as_of_required_for_mode",
        "interval_bounds_required_for_mode",
        "invalid_interval_policy",
        "future_evaluation_time_policy",
        "default_include_superseded",
        "default_include_revoked",
        "unknown_validity_policies",
        "default_unknown_validity_policy",
    }
    exact_keys(temporal, temporal_keys, "temporal_query", errors)
    if temporal.get("type_name") != "MemoryProviderRecallTemporalQueryV1":
        errors.append("recall temporal-query type drifted")
    if temporal.get("required_fields") != TEMPORAL_FIELDS:
        errors.append("temporal query required fields drifted")
    if temporal.get("modes") != ["current", "as_of", "interval", "history"]:
        errors.append("temporal query modes must remain canonical and ordered")
    if temporal.get("time_representation") != "utc_rfc3339_nanos":
        errors.append("temporal time representation must be UTC RFC3339 nanoseconds")
    for field in (
        "current_semantics",
        "as_of_semantics",
        "interval_semantics",
        "history_semantics",
    ):
        nonempty(temporal, field, "temporal_query", errors)
    if temporal.get("as_of_required_for_mode") != "as_of":
        errors.append("as_of timestamp must be required for as_of mode")
    if temporal.get("interval_bounds_required_for_mode") != "interval":
        errors.append("interval bounds must be required for interval mode")
    if temporal.get("invalid_interval_policy") != "reject_invalid_request":
        errors.append("invalid temporal interval must reject request")
    if temporal.get("future_evaluation_time_policy") != "reject_invalid_request":
        errors.append("future evaluation time must reject request")
    if temporal.get("default_include_superseded") is not False:
        errors.append("superseded candidates must be excluded by default")
    if temporal.get("default_include_revoked") is not False:
        errors.append("revoked candidates must be excluded by default")
    if temporal.get("unknown_validity_policies") != [
        "exclude",
        "degrade",
        "allow_with_warning",
    ]:
        errors.append("unknown-validity policies drifted")
    if temporal.get("default_unknown_validity_policy") != "exclude":
        errors.append("unknown validity must be excluded by default")


def validate_budgets_exclusions_extensions(
    contract: dict[str, Any], errors: list[str]
) -> None:
    budgets = obj(contract.get("budgets"), "budgets", errors)
    keys = {
        "type_name",
        "required_fields",
        "maximum_candidates_source",
        "maximum_total_content_bytes_source",
        "all_values_positive_and_finite",
        "provider_may_exceed_budget",
        "zero_budget_policy",
        "missing_budget_policy",
        "truncation_policy",
    }
    exact_keys(budgets, keys, "budgets", errors)
    if budgets.get("type_name") != "MemoryProviderRecallBudgetsV1":
        errors.append("recall budgets type drifted")
    if budgets.get("required_fields") != BUDGET_FIELDS:
        errors.append("recall budget fields drifted")
    if budgets.get("maximum_candidates_source") != (
        "min(request, effective_limit.recall_candidates)"
    ):
        errors.append("candidate budget must be handshake-clamped")
    if budgets.get("maximum_total_content_bytes_source") != (
        "min(request, effective_limit.response_bytes)"
    ):
        errors.append("response-byte budget must be handshake-clamped")
    if budgets.get("all_values_positive_and_finite") is not True:
        errors.append("all recall budgets must be positive and finite")
    if budgets.get("provider_may_exceed_budget") is not False:
        errors.append("provider cannot exceed recall budget")
    for field in ("zero_budget_policy", "missing_budget_policy"):
        if budgets.get(field) != "reject_invalid_request":
            errors.append(f"budgets.{field} must reject invalid request")
    if budgets.get("truncation_policy") != (
        "return_partial_coverage_with_explicit_truncation"
    ):
        errors.append("recall truncation must return explicit partial coverage")

    exclusions = obj(contract.get("exclusions"), "exclusions", errors)
    keys = {
        "type_name",
        "required_fields",
        "maximum_entries_per_class",
        "duplicate_entry_policy",
        "provider_must_honor_exclusions",
        "ignored_exclusion_policy",
    }
    exact_keys(exclusions, keys, "exclusions", errors)
    if exclusions.get("type_name") != "MemoryProviderRecallExclusionsV1":
        errors.append("recall exclusions type drifted")
    if exclusions.get("required_fields") != EXCLUSION_FIELDS:
        errors.append("recall exclusion fields drifted")
    maximum = exclusions.get("maximum_entries_per_class")
    if not isinstance(maximum, int) or not 1 <= maximum <= 1024:
        errors.append("exclusion classes must be bounded at 1024")
    if exclusions.get("duplicate_entry_policy") != "reject_non_canonical_request":
        errors.append("duplicate exclusions must reject non-canonical request")
    if exclusions.get("provider_must_honor_exclusions") is not True:
        errors.append("provider must honor recall exclusions")
    if exclusions.get("ignored_exclusion_policy") != "contract_violation":
        errors.append("ignored exclusion must be contract violation")

    extension = obj(contract.get("extension_contract"), "extension_contract", errors)
    keys = {
        "type_name",
        "required_fields",
        "maximum_request_extensions",
        "maximum_candidate_extensions",
        "maximum_extension_bytes",
        "unknown_optional_extension_policy",
        "unknown_required_extension_policy",
        "unknown_extension_may_change_scope",
        "unknown_extension_may_change_authority",
        "unknown_extension_may_activate_behavior",
    }
    exact_keys(extension, keys, "extension_contract", errors)
    if extension.get("type_name") != "MemoryProviderRecallExtensionV1":
        errors.append("recall extension type drifted")
    if extension.get("required_fields") != [
        "extension_id",
        "extension_version",
        "criticality",
        "canonical_payload",
        "payload_sha256",
    ]:
        errors.append("recall extension fields drifted")
    for field, hard_max in (
        ("maximum_request_extensions", 16),
        ("maximum_candidate_extensions", 16),
        ("maximum_extension_bytes", 131072),
    ):
        value = extension.get(field)
        if not isinstance(value, int) or not 1 <= value <= hard_max:
            errors.append(f"extension_contract.{field} must be bounded at {hard_max}")
    if extension.get("unknown_optional_extension_policy") != (
        "preserve_opaque_inert_round_trip"
    ):
        errors.append("unknown optional recall extensions must round-trip inertly")
    if extension.get("unknown_required_extension_policy") != (
        "reject_extension_unsupported"
    ):
        errors.append("unknown required recall extensions must fail explicitly")
    for field in (
        "unknown_extension_may_change_scope",
        "unknown_extension_may_change_authority",
        "unknown_extension_may_activate_behavior",
    ):
        if extension.get(field) is not False:
            errors.append(f"extension_contract.{field} must be false")


def validate_candidate_scores(contract: dict[str, Any], errors: list[str]) -> None:
    candidate = obj(contract.get("provider_candidate"), "provider_candidate", errors)
    keys = {
        "type_name",
        "required_fields",
        "candidate_id_semantics",
        "candidate_id_stable_across_requests",
        "stable_memory_ref_required",
        "stable_memory_ref_null_allowed",
        "confidence_required_nullable",
        "confidence_null_semantics",
        "confidence_number_semantics",
        "content_selection_rule",
        "content_digest_required",
        "maximum_candidate_bytes_source",
        "provider_candidate_is_advisory",
        "provider_candidate_may_mutate_context",
        "provider_candidate_may_mutate_tracedecay_authority",
    }
    exact_keys(candidate, keys, "provider_candidate", errors)
    if candidate.get("type_name") != "MemoryProviderRecallCandidateV1":
        errors.append("provider recall candidate type drifted")
    if candidate.get("required_fields") != CANDIDATE_FIELDS:
        errors.append("provider candidate fields drifted")
    if candidate.get("candidate_id_semantics") != (
        "request_scoped_provider_candidate_identity"
    ):
        errors.append("candidate ID must be request-scoped")
    if candidate.get("candidate_id_stable_across_requests") is not False:
        errors.append("candidate ID must not be stable across requests")
    if candidate.get("stable_memory_ref_required") is not False:
        errors.append("stable memory references must be optional")
    if candidate.get("stable_memory_ref_null_allowed") is not True:
        errors.append("stable memory reference null must be allowed")
    if candidate.get("confidence_required_nullable") is not True:
        errors.append("candidate confidence must be required-nullable")
    if candidate.get("confidence_null_semantics") != (
        "provider_did_not_supply_confidence"
    ):
        errors.append("candidate confidence null semantics drifted")
    if candidate.get("confidence_number_semantics") != (
        "finite_number_inclusive_0_0_to_1_0"
    ):
        errors.append("candidate confidence number semantics drifted")
    if candidate.get("content_selection_rule") != "exactly_one_of_content_or_content_ref":
        errors.append("candidate must contain exactly one content form")
    if candidate.get("content_digest_required") is not True:
        errors.append("candidate content digest must be required")
    if candidate.get("maximum_candidate_bytes_source") != (
        "request.budgets.maximum_candidate_content_bytes"
    ):
        errors.append("candidate bytes must be request-budgeted")
    if candidate.get("provider_candidate_is_advisory") is not True:
        errors.append("provider candidate must remain advisory")
    for field in (
        "provider_candidate_may_mutate_context",
        "provider_candidate_may_mutate_tracedecay_authority",
    ):
        if candidate.get(field) is not False:
            errors.append(f"provider_candidate.{field} must be false")

    content_ref = obj(contract.get("content_reference"), "content_reference", errors)
    keys = {
        "type_name",
        "required_fields",
        "reference_kinds",
        "provider_local_reference_is_authority",
        "hydration_requires_scope_revalidation",
        "hydration_failure_policy",
    }
    exact_keys(content_ref, keys, "content_reference", errors)
    if content_ref.get("type_name") != "MemoryProviderRecallContentReferenceV1":
        errors.append("content-reference type drifted")
    if content_ref.get("required_fields") != [
        "reference_kind",
        "reference_identity",
        "reference_revision",
        "content_sha256",
        "hydration_authority",
    ]:
        errors.append("content-reference fields drifted")
    if content_ref.get("reference_kinds") != [
        "provider_local",
        "tracedecay_source",
        "tracedecay_session",
        "tracedecay_native_fact",
    ]:
        errors.append("content reference kinds drifted")
    if content_ref.get("provider_local_reference_is_authority") is not False:
        errors.append("provider-local content reference cannot be authority")
    if content_ref.get("hydration_requires_scope_revalidation") is not True:
        errors.append("content hydration must revalidate scope")
    nonempty(content_ref, "hydration_failure_policy", "content_reference", errors)

    native = obj(contract.get("native_score"), "native_score", errors)
    keys = {
        "type_name",
        "required_fields",
        "raw_value_representation",
        "directions",
        "calibration_states",
        "provider_native_scores_cross_provider_comparable",
        "provider_native_scores_cross_domain_comparable",
        "missing_score_policy",
        "non_finite_score_allowed",
        "score_components_maximum",
    }
    exact_keys(native, keys, "native_score", errors)
    if native.get("type_name") != "MemoryProviderNativeScoreV1":
        errors.append("native score type drifted")
    if native.get("required_fields") != NATIVE_SCORE_FIELDS:
        errors.append("native score fields drifted")
    if native.get("raw_value_representation") != "canonical_decimal_string":
        errors.append("native score raw value must be canonical decimal string")
    if native.get("directions") != ["higher_is_better", "lower_is_better"]:
        errors.append("native score directions drifted")
    if native.get("calibration_states") != [
        "uncalibrated",
        "provider_calibrated",
        "externally_calibrated",
    ]:
        errors.append("native score calibration states drifted")
    if native.get("provider_native_scores_cross_provider_comparable") is not False:
        errors.append("native scores must not be cross-provider comparable")
    if native.get("provider_native_scores_cross_domain_comparable") is not False:
        errors.append("native scores must not be cross-domain comparable")
    if native.get("missing_score_policy") != "reject_contract_violation":
        errors.append("missing native score must reject contract violation")
    if native.get("non_finite_score_allowed") is not False:
        errors.append("non-finite native score must be forbidden")
    components = native.get("score_components_maximum")
    if not isinstance(components, int) or not 1 <= components <= 32:
        errors.append("native score components must be bounded at 32")

    normalized = obj(
        contract.get("host_normalized_score"), "host_normalized_score", errors
    )
    keys = {
        "type_name",
        "owner",
        "required_fields",
        "value_range",
        "provider_may_supply_normalized_score",
        "provider_response_field",
        "normalization_required_before_cross_provider_comparison",
        "normalization_may_change_native_score",
        "unavailable_normalization_policy",
    }
    exact_keys(normalized, keys, "host_normalized_score", errors)
    if normalized.get("type_name") != "MemoryProviderHostNormalizedScoreV1":
        errors.append("host-normalized score type drifted")
    if normalized.get("owner") != "TraceDecay context compiler":
        errors.append("TraceDecay context compiler must own normalized score")
    if normalized.get("required_fields") != [
        "normalization_policy_id",
        "normalization_policy_revision",
        "normalized_value",
        "input_native_score_digest",
        "calibration_evidence",
        "warnings",
    ]:
        errors.append("host-normalized score fields drifted")
    if normalized.get("value_range") != "closed_0_to_1_canonical_decimal_string":
        errors.append("normalized score must use closed [0,1] canonical decimal range")
    if normalized.get("provider_may_supply_normalized_score") is not False:
        errors.append("provider cannot supply host-normalized score")
    if normalized.get("provider_response_field") is not None:
        errors.append("provider response cannot contain host-normalized score field")
    if normalized.get("normalization_required_before_cross_provider_comparison") is not True:
        errors.append("normalization must precede cross-provider comparison")
    if normalized.get("normalization_may_change_native_score") is not False:
        errors.append("normalization cannot alter native score")
    nonempty(
        normalized,
        "unavailable_normalization_policy",
        "host_normalized_score",
        errors,
    )


def validate_candidate_scope_binding(contract: dict[str, Any], errors: list[str]) -> None:
    binding = obj(
        contract.get("candidate_scope_binding"), "candidate_scope_binding", errors
    )
    keys = {
        "type_name",
        "carried_in",
        "wire_field",
        "required_fields",
        "bindings",
        "binding_source",
        "authorization_source",
        "authorization_carried_by",
        "missing_binding_policy",
        "unknown_binding_policy",
        "unauthorized_binding_policy",
        "malformed_identity_policy",
        "forbidden_identity_policy",
        "provider_may_widen_binding",
        "binding_rules",
    }
    exact_keys(binding, keys, "candidate_scope_binding", errors)
    if binding.get("type_name") != "MemoryProviderRecallCandidateScopeIdentityV1":
        errors.append("candidate scope binding type drifted")
    if binding.get("carried_in") != "provider_candidate.exact_scope_identity":
        errors.append("candidate scope binding must live in the candidate exact scope")
    if binding.get("wire_field") != "scope_binding":
        errors.append("candidate scope binding wire field must be scope_binding")
    if binding.get("required_fields") != CANDIDATE_SCOPE_FIELDS:
        errors.append("candidate scope fields must be scope_binding plus the exact scope")
    if binding.get("bindings") != SCOPE_BINDINGS:
        errors.append("candidate scope bindings must mirror the authority-matrix namespaces")
    if "coding-memory-authority-matrix.json" not in str(binding.get("binding_source")):
        errors.append("candidate scope bindings must cite the authority matrix")
    if "registration_contract.recall_scope_bindings" not in str(
        binding.get("authorization_source")
    ):
        errors.append("candidate scope binding authorization must come from registration")
    if binding.get("authorization_carried_by") != (
        "host_admitted_call_never_provider_reply"
    ):
        errors.append("scope binding authorization must travel with the admitted call")
    for field, expected in (
        ("missing_binding_policy", "reject_contract_violation"),
        ("unknown_binding_policy", "reject_contract_violation"),
        ("unauthorized_binding_policy", "reject_scope_binding_unauthorized"),
        ("malformed_identity_policy", "reject_unknown_identity"),
        ("forbidden_identity_policy", "reject_forbidden_identity"),
    ):
        if binding.get(field) != expected:
            errors.append(f"candidate_scope_binding.{field} must be {expected}")
    if binding.get("provider_may_widen_binding") is not False:
        errors.append("candidate_scope_binding.provider_may_widen_binding must be false")
    rules = arr(binding.get("binding_rules"), "candidate_scope_binding.binding_rules", errors)
    seen: list[str] = []
    for index, rule in enumerate(rules):
        row = obj(rule, f"candidate_scope_binding.binding_rules[{index}]", errors)
        name = row.get("binding")
        if name not in BINDING_RULES or name in seen:
            errors.append(f"binding rule {index} names an unknown or duplicate binding")
            continue
        seen.append(name)
        expected = BINDING_RULES[name]
        rule_keys = {"binding", "required_equal", "optional_empty_or_equal", "forbidden"}
        if name == "exact_coding_scope":
            rule_keys.add("resolved_scope_digest_mismatch_policy")
            if row.get("resolved_scope_digest_mismatch_policy") != "reject_stale_identity":
                errors.append("exact_coding_scope digest mismatch must be stale identity")
        exact_keys(row, rule_keys, f"binding_rules[{name}]", errors)
        for key in ("required_equal", "optional_empty_or_equal", "forbidden"):
            if row.get(key) != expected[key]:
                errors.append(f"binding rule {name}.{key} drifted from the authority matrix")
        covered = (
            list(row.get("required_equal") or [])
            + list(row.get("optional_empty_or_equal") or [])
            + list(row.get("forbidden") or [])
        )
        if sorted(covered) != sorted(SCOPE_FIELDS):
            errors.append(f"binding rule {name} must classify every exact scope field once")
    if seen != SCOPE_BINDINGS:
        errors.append("binding rules must cover every binding in contract order")


def validate_validity_provenance_explanation(
    contract: dict[str, Any], errors: list[str]
) -> None:
    validity = obj(contract.get("validity"), "validity", errors)
    keys = {
        "type_name",
        "required_fields",
        "temporal_states",
        "time_representation",
        "valid_until_semantics",
        "revoked_candidate_default_policy",
        "superseded_candidate_default_policy",
        "unknown_candidate_default_policy",
        "provider_may_omit_source_revision",
    }
    exact_keys(validity, keys, "validity", errors)
    if validity.get("type_name") != "MemoryProviderRecallValidityV1":
        errors.append("recall validity type drifted")
    if validity.get("required_fields") != VALIDITY_FIELDS:
        errors.append("recall validity fields drifted")
    if validity.get("temporal_states") != [
        "current",
        "future",
        "expired",
        "superseded",
        "revoked",
        "unknown",
    ]:
        errors.append("recall temporal states drifted")
    if validity.get("time_representation") != "utc_rfc3339_nanos":
        errors.append("validity time representation must be UTC RFC3339 nanoseconds")
    if validity.get("valid_until_semantics") != "exclusive":
        errors.append("valid_until must be exclusive")
    for field in (
        "revoked_candidate_default_policy",
        "superseded_candidate_default_policy",
        "unknown_candidate_default_policy",
    ):
        if validity.get(field) != "exclude":
            errors.append(f"validity.{field} must be exclude")
    if validity.get("provider_may_omit_source_revision") is not False:
        errors.append("provider must not omit source revision")

    provenance = obj(contract.get("provenance"), "provenance", errors)
    keys = {
        "type_name",
        "required_fields",
        "states",
        "maximum_refs_per_class",
        "maximum_transform_steps",
        "missing_provenance_is_explicit",
        "provider_may_fabricate_provenance",
        "provider_may_drop_known_provenance",
        "policy_actions",
        "default_unavailable_policy_action",
        "default_redacted_policy_action",
    }
    exact_keys(provenance, keys, "provenance", errors)
    if provenance.get("type_name") != "MemoryProviderRecallProvenanceV1":
        errors.append("recall provenance type drifted")
    if provenance.get("required_fields") != PROVENANCE_FIELDS:
        errors.append("recall provenance fields drifted")
    if provenance.get("states") != ["available", "redacted", "unavailable"]:
        errors.append("provenance states drifted")
    for field, hard_max in (
        ("maximum_refs_per_class", 64),
        ("maximum_transform_steps", 32),
    ):
        value = provenance.get(field)
        if not isinstance(value, int) or not 1 <= value <= hard_max:
            errors.append(f"provenance.{field} must be bounded at {hard_max}")
    if provenance.get("missing_provenance_is_explicit") is not True:
        errors.append("missing provenance must be explicit")
    if provenance.get("provider_may_fabricate_provenance") is not False:
        errors.append("provider cannot fabricate provenance")
    if provenance.get("provider_may_drop_known_provenance") is not False:
        errors.append("provider cannot drop known provenance")
    if provenance.get("policy_actions") != ["exclude", "degrade_allow", "audit_only"]:
        errors.append("provenance policy actions drifted")
    if provenance.get("default_unavailable_policy_action") != "exclude":
        errors.append("unavailable provenance must default to exclude")
    if provenance.get("default_redacted_policy_action") != "degrade_allow":
        errors.append("redacted provenance must default to degrade_allow")

    explanation = obj(contract.get("explanation"), "explanation", errors)
    keys = {
        "type_name",
        "required_fields",
        "summary_maximum_bytes",
        "maximum_matched_features",
        "maximum_activation_trace_refs",
        "maximum_limitations",
        "explanation_is_proof",
        "missing_explanation_policy",
    }
    exact_keys(explanation, keys, "explanation", errors)
    if explanation.get("type_name") != "MemoryProviderRecallExplanationV1":
        errors.append("recall explanation type drifted")
    if explanation.get("required_fields") != [
        "summary",
        "matched_features",
        "activation_trace_refs",
        "limitations",
    ]:
        errors.append("recall explanation fields drifted")
    for field, hard_max in (
        ("summary_maximum_bytes", 8192),
        ("maximum_matched_features", 64),
        ("maximum_activation_trace_refs", 64),
        ("maximum_limitations", 32),
    ):
        value = explanation.get(field)
        if not isinstance(value, int) or not 1 <= value <= hard_max:
            errors.append(f"explanation.{field} must be bounded at {hard_max}")
    if explanation.get("explanation_is_proof") is not False:
        errors.append("provider explanation must not be proof")
    nonempty(explanation, "missing_explanation_policy", "explanation", errors)


def validate_response_coverage_ordering(
    contract: dict[str, Any], errors: list[str]
) -> None:
    response = obj(contract.get("recall_response"), "recall_response", errors)
    keys = {
        "type_name",
        "contract_id",
        "required_fields",
        "candidate_count_source",
        "successful_zero_results_is_explicit",
        "empty_candidate_list_is_not_failure_or_fallback_signal",
        "provider_may_inject_context",
        "provider_may_select_final_context",
    }
    exact_keys(response, keys, "recall_response", errors)
    if response.get("type_name") != "MemoryProviderRecallOutcomeV1":
        errors.append("recall response type drifted")
    if response.get("contract_id") != "tracedecay.memory.recall.query.outcome.v1":
        errors.append("recall response wire contract ID drifted")
    if response.get("required_fields") != RESPONSE_FIELDS:
        errors.append("recall response fields drifted")
    if response.get("candidate_count_source") != (
        "request.budgets.maximum_candidates"
    ):
        errors.append("response candidate count must come from request budget")
    if response.get("successful_zero_results_is_explicit") is not True:
        errors.append("successful zero results must be explicit")
    if response.get("empty_candidate_list_is_not_failure_or_fallback_signal") is not True:
        errors.append("empty candidate list must not imply failure or fallback")
    for field in ("provider_may_inject_context", "provider_may_select_final_context"):
        if response.get(field) is not False:
            errors.append(f"recall_response.{field} must be false")

    coverage = obj(contract.get("coverage"), "coverage", errors)
    keys = {
        "type_name",
        "required_fields",
        "states",
        "zero_results_requires_successful_complete_search",
        "partial_requires_reason",
        "cursor_is_provider_opaque",
        "cursor_scope_bound",
        "cursor_request_contract_bound",
    }
    exact_keys(coverage, keys, "coverage", errors)
    if coverage.get("type_name") != "MemoryProviderRecallCoverageV1":
        errors.append("recall coverage type drifted")
    if coverage.get("required_fields") != COVERAGE_FIELDS:
        errors.append("recall coverage fields drifted")
    if coverage.get("states") != ["complete", "partial", "zero_results"]:
        errors.append("recall coverage states drifted")
    for field in (
        "zero_results_requires_successful_complete_search",
        "partial_requires_reason",
        "cursor_is_provider_opaque",
        "cursor_scope_bound",
        "cursor_request_contract_bound",
    ):
        if coverage.get(field) is not True:
            errors.append(f"coverage.{field} must be true")

    ordering = obj(contract.get("ordering"), "ordering", errors)
    keys = {
        "provider_order",
        "tie_breaker",
        "host_may_reorder_after_admission",
        "host_reorder_requires_normalization_and_explain_trace",
        "provider_order_cross_provider_authority",
        "fixed_request_state_must_produce_deterministic_order",
    }
    exact_keys(ordering, keys, "ordering", errors)
    nonempty(ordering, "provider_order", "ordering", errors)
    if ordering.get("tie_breaker") != "candidate_id_lexicographic_utf8":
        errors.append("candidate ID must be deterministic provider tie-breaker")
    if ordering.get("host_may_reorder_after_admission") is not True:
        errors.append("host may reorder only after admission")
    if ordering.get("host_reorder_requires_normalization_and_explain_trace") is not True:
        errors.append("host reorder requires normalization and explain trace")
    if ordering.get("provider_order_cross_provider_authority") is not False:
        errors.append("provider order has no cross-provider authority")
    if ordering.get("fixed_request_state_must_produce_deterministic_order") is not True:
        errors.append("fixed recall inputs must produce deterministic provider order")

    states = arr(
        contract.get("recall_specific_terminal_states"),
        "recall_specific_terminal_states",
        errors,
    )
    if set(states) != TERMINAL_STATES or len(states) != len(TERMINAL_STATES):
        errors.append("recall terminal states must exactly cover V1 outcomes")


def validate_invariants_beads(
    contract: dict[str, Any], ids: set[str], errors: list[str]
) -> None:
    invariants = arr(contract.get("invariants"), "invariants", errors)
    if len(invariants) < 17 or len(set(invariants)) != len(invariants):
        errors.append("recall contract must state at least seventeen unique invariants")
    serialized = " ".join(str(value) for value in invariants).casefold()
    for phrase in REQUIRED_INVARIANT_PHRASES:
        if phrase.casefold() not in serialized:
            errors.append(f"recall invariants are missing {phrase!r}")

    beads = arr(contract.get("verification_beads"), "verification_beads", errors)
    if len(beads) < 10 or len(set(beads)) != len(beads):
        errors.append("verification_beads must contain at least ten unique issues")
    for value in beads:
        validate_bead(value, "verification_beads", ids, errors)
    for required in (
        "tdmem-0206",
        "tdmem-0207",
        "tdmem-0209",
        "tdmem-0402",
        "tdmem-0601",
        "tdmem-0602",
        "tdmem-0603",
        "tdmem-0604",
        "tdmem-0608",
        "tdmem-0609",
    ):
        if required not in beads:
            errors.append(f"verification_beads is missing {required}")


def validate_schema(schema: dict[str, Any], errors: list[str]) -> None:
    if schema.get("$schema") != "https://json-schema.org/draft/2020-12/schema":
        errors.append("recall schema must use JSON Schema 2020-12")
    if schema.get("type") != "object" or schema.get("additionalProperties") is not False:
        errors.append("recall schema root must be a strict object")
    if set(schema.get("required", [])) != TOP_LEVEL:
        errors.append("recall schema required fields must match the contract")
    properties = obj(schema.get("properties"), "schema.properties", errors)
    if properties.get("schema_version", {}).get("const") != 1:
        errors.append("recall schema must pin schema_version 1")
    if properties.get("contract_id", {}).get("const") != (
        "tracedecay.memory.provider.recall.v1"
    ):
        errors.append("recall schema must pin contract_id")
    if properties.get("bead_id", {}).get("const") != "tdmem-0204":
        errors.append("recall schema must pin bead_id tdmem-0204")
    if properties.get("recall_specific_terminal_states", {}).get("minItems") != 17:
        errors.append("recall schema must require seventeen terminal states")
    if properties.get("invariants", {}).get("minItems") != 17:
        errors.append("recall schema must require seventeen invariants")
    definitions = obj(schema.get("$defs"), "schema.$defs", errors)
    for name in ("beadId", "object"):
        if name not in definitions:
            errors.append(f"recall schema is missing $defs.{name}")


def validate_doc(path: Path, errors: list[str]) -> None:
    try:
        text = path.read_text(encoding="utf-8")
    except OSError as exc:
        errors.append(f"could not load recall documentation: {exc}")
        return
    for phrase in REQUIRED_DOC_PHRASES:
        if phrase.casefold() not in text.casefold():
            errors.append(f"recall documentation is missing {phrase!r}")
    if "TBD" in text or "TODO" in text:
        errors.append("recall documentation contains unresolved TBD/TODO text")


def validate_dependencies(repo: Path, errors: list[str]) -> None:
    registry = load_object(
        repo
        / "product/contracts/memory-provider-v1/provider-registry-contract.json",
        "provider registry contract",
        errors,
    )
    handshake = load_object(
        repo
        / "product/contracts/memory-provider-v1/provider-handshake-contract.json",
        "provider handshake contract",
        errors,
    )
    if registry.get("status") != "accepted" or registry.get("contract_id") != (
        "tracedecay.memory.provider.registry.v1"
    ):
        errors.append("recall requires accepted provider registry V1")
    if handshake.get("status") != "accepted" or handshake.get("contract_id") != (
        "tracedecay.memory.provider.handshake.v1"
    ):
        errors.append("recall requires accepted provider handshake V1")

    capability_registry = registry.get("capability_registry")
    mandatory = (
        capability_registry.get("mandatory", [])
        if isinstance(capability_registry, dict)
        else []
    )
    recall_rows = [
        row
        for row in mandatory
        if isinstance(row, dict) and row.get("id") == "recall.query.v1"
    ]
    if len(recall_rows) != 1:
        errors.append("provider registry must retain one mandatory recall.query.v1")
    elif recall_rows[0].get("detailed_contract_bead") != "tdmem-0204":
        errors.append("recall.query.v1 must point to tdmem-0204")

    handshake_scope = handshake.get("exact_scope_identity", {}).get(
        "required_fields", []
    )
    if handshake_scope != SCOPE_FIELDS:
        errors.append("recall exact scope must match the accepted handshake scope")
    handshake_limits = {
        row.get("id")
        for row in handshake.get("limit_catalog", [])
        if isinstance(row, dict)
    }
    for limit_id in ("recall_candidates", "response_bytes"):
        if limit_id not in handshake_limits:
            errors.append(f"handshake limit catalog is missing {limit_id}")


def validate(
    repo: Path,
    contract: dict[str, Any],
    schema: dict[str, Any],
    doc: Path,
    ids: set[str],
) -> list[str]:
    errors: list[str] = []
    validate_header(contract, errors)
    validate_request_scope_temporal(contract, errors)
    validate_budgets_exclusions_extensions(contract, errors)
    validate_candidate_scores(contract, errors)
    validate_candidate_scope_binding(contract, errors)
    validate_validity_provenance_explanation(contract, errors)
    validate_response_coverage_ordering(contract, errors)
    validate_invariants_beads(contract, ids, errors)
    validate_schema(schema, errors)
    validate_doc(doc, errors)
    validate_dependencies(repo, errors)
    return errors


def main() -> int:
    args = parse_args()
    repo = args.repo.resolve()
    bootstrap: list[str] = []
    contract = load_object(resolve(repo, args.contract), "recall contract", bootstrap)
    schema = load_object(resolve(repo, args.schema), "recall schema", bootstrap)
    ids = load_issue_ids(resolve(repo, args.issues), bootstrap)
    if bootstrap:
        print(json.dumps({"ok": False, "errors": bootstrap}, indent=2, sort_keys=True))
        return 1

    errors = validate(repo, contract, schema, resolve(repo, args.doc), ids)
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
                "stable_memory_ref_required": contract["provider_candidate"][
                    "stable_memory_ref_required"
                ],
                "native_scores_cross_provider_comparable": contract[
                    "native_score"
                ]["provider_native_scores_cross_provider_comparable"],
                "normalized_score_owner": contract["host_normalized_score"]["owner"],
                "provenance_states": contract["provenance"]["states"],
                "temporal_modes": contract["temporal_query"]["modes"],
                "terminal_state_count": len(
                    contract["recall_specific_terminal_states"]
                ),
                "provider_may_inject_context": contract["recall_response"][
                    "provider_may_inject_context"
                ],
                "scope_bindings": contract["candidate_scope_binding"]["bindings"],
            },
            indent=2,
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
