#!/usr/bin/env python3
"""Validate capability-gated provider lifecycle semantics."""

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
    "common_request",
    "capability_gating",
    "health",
    "feedback",
    "maintenance",
    "inspection",
    "correction",
    "deletion_by_source",
    "snapshot",
    "replay",
    "provider_local_projection",
    "lifecycle_specific_terminal_states",
    "invariants",
    "verification_beads",
}

COMMON_FIELDS = [
    "provider_id",
    "registration_revision",
    "ready_receipt_digest",
    "exact_scope_identity",
    "operation_id",
    "idempotency_key",
    "expected_state_generation",
    "request_identity",
    "policy_revision",
    "deadline",
    "cancellation",
    "extensions",
]

CAPABILITIES = {
    "provider.health.v1": ("tracedecay.memory.provider.health.v1", "mandatory"),
    "feedback.record.v1": ("tracedecay.memory.provider.feedback.v1", "optional"),
    "maintenance.run.v1": (
        "tracedecay.memory.provider.maintenance.v1",
        "optional",
    ),
    "inspection.read.v1": (
        "tracedecay.memory.provider.inspection.v1",
        "optional",
    ),
    "correction.apply.v1": (
        "tracedecay.memory.provider.correction.v1",
        "optional",
    ),
    "deletion.by_source.v1": (
        "tracedecay.memory.provider.deletion-by-source.v1",
        "optional",
    ),
    "snapshot.export.v1": (
        "tracedecay.memory.provider.snapshot-export.v1",
        "optional",
    ),
    "snapshot.restore.v1": (
        "tracedecay.memory.provider.snapshot-restore.v1",
        "optional",
    ),
    "replay.apply.v1": ("tracedecay.memory.provider.replay.v1", "optional"),
    "facts.explicit.v1": (
        "tracedecay.memory.provider.explicit-fact-projection.v1",
        "optional",
    ),
    "explain.trace.v1": (
        "tracedecay.memory.provider.explain-trace.v1",
        "optional",
    ),
}

LIFECYCLE_TERMINALS = {
    "success",
    "success_no_effect",
    "partial_effect",
    "effect_unknown",
    "invalid_request",
    "unauthorized",
    "capability_unsupported",
    "scope_unavailable",
    "scope_mismatch",
    "stale_identity",
    "target_unknown",
    "source_unknown",
    "revision_conflict",
    "idempotency_conflict",
    "settlement_unverified",
    "maintenance_busy",
    "retention_lock",
    "deadline_exceeded",
    "cancelled",
    "provider_unavailable",
    "state_incompatible",
    "reset_required",
    "extension_unsupported",
    "contract_violation",
    "internal_failure",
}

REQUIRED_DOC_PHRASES = [
    "Missing optional behavior returns the typed `capability_unsupported` result",
    "targets exactly one of",
    "a stable provider memory reference",
    "a recall trace reference",
    "a context-pack item reference",
    "Maintenance tasks are consolidate, decay, prune-expired, validate-state, repair, and compact",
    "Cancellation and timeout never become success",
    "cannot expose raw credentials",
    "expected target revision is mandatory",
    "successful operation has a verifiable postcondition",
    "zero remaining provider influence",
    "Snapshot identity binds",
    "Implicit reset and implicit overwrite are forbidden",
    "Replay consumes canonical observation receipts",
]

REQUIRED_INVARIANTS = [
    "capability-gated",
    "typed capability_unsupported",
    "stable operation identity",
    "Health is mandatory",
    "Feedback targets exactly one",
    "never TraceDecay Native trust",
    "Maintenance is finite",
    "Inspection is bounded",
    "Correction is idempotent",
    "verifiable postcondition",
    "may not reappear",
    "Snapshots bind",
    "Replay consumes canonical observation receipts",
    "remain advisory",
    "typed and receipt-backed",
]

BEAD_RE = re.compile(r"^tdmem-[0-9]{4}$")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--repo", type=Path, default=Path("."))
    parser.add_argument(
        "--contract",
        type=Path,
        default=Path(
            "product/contracts/memory-provider-v1/provider-lifecycle-contract.json"
        ),
    )
    parser.add_argument(
        "--schema",
        type=Path,
        default=Path(
            "product/contracts/memory-provider-v1/provider-lifecycle-contract.schema.json"
        ),
    )
    parser.add_argument(
        "--doc",
        type=Path,
        default=Path(
            "product/contracts/memory-provider-v1/provider-lifecycle-contract.md"
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
    if contract.get("contract_id") != "tracedecay.memory.provider.lifecycle.v1":
        errors.append("contract_id must be tracedecay.memory.provider.lifecycle.v1")
    if contract.get("bead_id") != "tdmem-0205":
        errors.append("bead_id must be tdmem-0205")
    if contract.get("status") != "accepted":
        errors.append("contract status must be accepted")
    if contract.get("authority") != (
        "TraceDecay lifecycle admission and provider-local capability fabric"
    ):
        errors.append("lifecycle authority must remain TraceDecay lifecycle admission")
    if contract.get("scope") != "coding_agents_only":
        errors.append("lifecycle scope must remain coding_agents_only")
    if contract.get("depends_on_contracts") != [
        "tracedecay.memory.provider.registry.v1",
        "tracedecay.memory.provider.handshake.v1",
        "tracedecay.memory.provider.observation.v1",
        "tracedecay.memory.provider.recall.v1",
    ]:
        errors.append(
            "lifecycle dependencies must be registry, handshake, observation, recall V1"
        )
    nonempty(contract, "title", "contract", errors)


def validate_common(contract: dict[str, Any], errors: list[str]) -> None:
    common = obj(contract.get("common_request"), "common_request", errors)
    keys = {
        "type_name",
        "required_fields",
        "operation_id_type",
        "idempotency_key_encoding",
        "exact_scope_match_required",
        "live_ready_receipt_required",
        "deadline_required",
        "cancellation_required",
        "unknown_optional_extension_policy",
        "unknown_required_extension_policy",
        "provider_may_widen_scope",
        "provider_may_extend_deadline",
        "provider_may_mutate_tracedecay_authority",
    }
    exact_keys(common, keys, "common_request", errors)
    if common.get("type_name") != "MemoryProviderLifecycleRequestContextV1":
        errors.append("lifecycle common request type drifted")
    if common.get("required_fields") != COMMON_FIELDS:
        errors.append("lifecycle common request fields drifted")
    if common.get("operation_id_type") != "uuid_v7_lowercase":
        errors.append("lifecycle operation ID must be lowercase UUIDv7")
    if common.get("idempotency_key_encoding") != "lowercase_hex_64":
        errors.append("lifecycle idempotency key must be lowercase SHA-256 hex")
    for field in (
        "exact_scope_match_required",
        "live_ready_receipt_required",
        "deadline_required",
        "cancellation_required",
    ):
        if common.get(field) is not True:
            errors.append(f"common_request.{field} must be true")
    if common.get("unknown_optional_extension_policy") != (
        "preserve_opaque_inert_round_trip"
    ):
        errors.append("unknown optional lifecycle extensions must round-trip inertly")
    if common.get("unknown_required_extension_policy") != (
        "reject_extension_unsupported"
    ):
        errors.append("unknown required lifecycle extensions must fail explicitly")
    for field in (
        "provider_may_widen_scope",
        "provider_may_extend_deadline",
        "provider_may_mutate_tracedecay_authority",
    ):
        if common.get(field) is not False:
            errors.append(f"common_request.{field} must be false")


def validate_capability_gating(
    contract: dict[str, Any], registry: dict[str, Any], errors: list[str]
) -> None:
    gating = obj(contract.get("capability_gating"), "capability_gating", errors)
    keys = {
        "type_name",
        "capability_to_operation",
        "unsupported_operation_outcome",
        "unsupported_operation_may_fallback",
        "provider_name_implies_capability",
        "capability_requires_registration_and_handshake_declaration",
    }
    exact_keys(gating, keys, "capability_gating", errors)
    if gating.get("type_name") != "MemoryProviderLifecycleCapabilityGateV1":
        errors.append("lifecycle capability gate type drifted")
    rows = unique_by(
        arr(
            gating.get("capability_to_operation"),
            "capability_gating.capability_to_operation",
            errors,
        ),
        "capability_id",
        "capability_gating.capability_to_operation",
        errors,
    )
    if set(rows) != set(CAPABILITIES):
        errors.append("lifecycle capability map must exactly cover V1 lifecycle capabilities")
    for capability_id, (contract_id, requirement) in CAPABILITIES.items():
        row = rows.get(capability_id, {})
        exact_keys(
            row,
            {"capability_id", "operation_contract_id", "requirement"},
            f"capability_map[{capability_id}]",
            errors,
        )
        if row.get("operation_contract_id") != contract_id:
            errors.append(f"capability {capability_id} operation contract drifted")
        if row.get("requirement") != requirement:
            errors.append(f"capability {capability_id} requirement must be {requirement}")
    if gating.get("unsupported_operation_outcome") != "capability_unsupported":
        errors.append("unsupported lifecycle operation must be capability_unsupported")
    for field in (
        "unsupported_operation_may_fallback",
        "provider_name_implies_capability",
    ):
        if gating.get(field) is not False:
            errors.append(f"capability_gating.{field} must be false")
    if gating.get("capability_requires_registration_and_handshake_declaration") is not True:
        errors.append("lifecycle capability must require registry and handshake declaration")

    catalog = registry.get("capability_registry")
    known: dict[str, dict[str, Any]] = {}
    if isinstance(catalog, dict):
        for class_name in ("mandatory", "optional"):
            for row in catalog.get(class_name, []):
                if isinstance(row, dict) and isinstance(row.get("id"), str):
                    known[row["id"]] = row
    for capability_id, (_, requirement) in CAPABILITIES.items():
        row = known.get(capability_id)
        if row is None:
            errors.append(f"registry is missing lifecycle capability {capability_id}")
            continue
        if row.get("requirement") != requirement:
            errors.append(
                f"registry lifecycle capability {capability_id} requirement mismatch"
            )
        if row.get("detailed_contract_bead") != "tdmem-0205":
            errors.append(
                f"registry lifecycle capability {capability_id} must point to tdmem-0205"
            )
        if row.get("may_mutate_tracedecay_authority") is not False:
            errors.append(
                f"registry lifecycle capability {capability_id} must not mutate TraceDecay authority"
            )


def validate_health(contract: dict[str, Any], errors: list[str]) -> None:
    health = obj(contract.get("health"), "health", errors)
    keys = {
        "type_name",
        "contract_id",
        "capability_id",
        "mutation",
        "required_request_fields",
        "requested_checks",
        "required_response_fields",
        "readiness_states",
        "process_existence_proves_ready",
        "socket_existence_proves_ready",
        "nonempty_state_proves_ready",
        "health_may_mutate_state",
    }
    exact_keys(health, keys, "health", errors)
    if health.get("type_name") != "MemoryProviderHealthV1":
        errors.append("health type drifted")
    if health.get("contract_id") != "tracedecay.memory.provider.health.v1":
        errors.append("health contract ID drifted")
    if health.get("capability_id") != "provider.health.v1":
        errors.append("health capability ID drifted")
    if health.get("mutation") is not False:
        errors.append("health mutation must be false")
    if health.get("required_request_fields") != ["common_request", "requested_checks"]:
        errors.append("health request fields drifted")
    if health.get("requested_checks") != [
        "protocol",
        "state",
        "scope",
        "capacity",
        "persistence",
        "recovery",
        "privacy",
    ]:
        errors.append("health checks drifted")
    if health.get("readiness_states") != [
        "ready",
        "degraded",
        "not_ready",
        "unavailable",
    ]:
        errors.append("health readiness states drifted")
    for field in (
        "process_existence_proves_ready",
        "socket_existence_proves_ready",
        "nonempty_state_proves_ready",
        "health_may_mutate_state",
    ):
        if health.get(field) is not False:
            errors.append(f"health.{field} must be false")


def validate_feedback(contract: dict[str, Any], errors: list[str]) -> None:
    feedback = obj(contract.get("feedback"), "feedback", errors)
    keys = {
        "type_name",
        "contract_id",
        "capability_id",
        "mutation",
        "required_request_fields",
        "target",
        "signals",
        "weight_representation",
        "canonical_outcome_receipt_required",
        "unsettled_outcome_policy",
        "idempotent",
        "same_key_different_feedback_policy",
        "provider_may_change_native_trust",
        "required_response_fields",
    }
    exact_keys(feedback, keys, "feedback", errors)
    if feedback.get("type_name") != "MemoryProviderFeedbackV1":
        errors.append("feedback type drifted")
    if feedback.get("contract_id") != "tracedecay.memory.provider.feedback.v1":
        errors.append("feedback contract ID drifted")
    if feedback.get("capability_id") != "feedback.record.v1":
        errors.append("feedback capability ID drifted")
    if feedback.get("mutation") is not True:
        errors.append("feedback must be a provider-local mutation")
    target = obj(feedback.get("target"), "feedback.target", errors)
    target_keys = {
        "type_name",
        "selection_rule",
        "target_kinds",
        "stable_memory_ref_required",
        "recall_trace_ref_scope_bound",
        "context_pack_item_ref_request_bound",
        "unknown_target_policy",
    }
    exact_keys(target, target_keys, "feedback.target", errors)
    if target.get("selection_rule") != "exactly_one":
        errors.append("feedback target must select exactly one kind")
    if target.get("target_kinds") != [
        "stable_memory_ref",
        "recall_trace_ref",
        "context_pack_item_ref",
    ]:
        errors.append("feedback target kinds drifted")
    if target.get("stable_memory_ref_required") is not False:
        errors.append("feedback stable memory ref must remain optional")
    if target.get("recall_trace_ref_scope_bound") is not True:
        errors.append("feedback recall trace must be scope-bound")
    if target.get("context_pack_item_ref_request_bound") is not True:
        errors.append("feedback context-pack target must be request-bound")
    if target.get("unknown_target_policy") != "target_unknown":
        errors.append("feedback unknown target must be target_unknown")
    if feedback.get("signals") != [
        "helpful",
        "harmful",
        "ignored",
        "corrected",
        "superseded",
    ]:
        errors.append("feedback signals drifted")
    if feedback.get("weight_representation") != (
        "canonical_decimal_string_closed_0_to_1"
    ):
        errors.append("feedback weight representation drifted")
    if feedback.get("canonical_outcome_receipt_required") is not True:
        errors.append("feedback requires canonically settled outcome receipt")
    if feedback.get("unsettled_outcome_policy") != "settlement_unverified":
        errors.append("unsettled feedback outcome must be settlement_unverified")
    if feedback.get("idempotent") is not True:
        errors.append("feedback must be idempotent")
    if feedback.get("same_key_different_feedback_policy") != "idempotency_conflict":
        errors.append("different feedback under same key must conflict")
    if feedback.get("provider_may_change_native_trust") is not False:
        errors.append("provider feedback cannot change Native trust")


def validate_maintenance_inspection(
    contract: dict[str, Any], errors: list[str]
) -> None:
    maintenance = obj(contract.get("maintenance"), "maintenance", errors)
    keys = {
        "type_name",
        "contract_id",
        "capability_id",
        "mutation",
        "required_request_fields",
        "tasks",
        "all_limits_positive_and_finite",
        "maximum_items_hard_limit",
        "maximum_bytes_hard_limit",
        "maximum_duration_millis_hard_limit",
        "effective_duration_is_minimum_of_request_handshake_and_deadline",
        "one_mutating_maintenance_operation_per_provider_scope",
        "concurrent_mutation_policy",
        "deadline_and_cancellation_reach_provider_loop",
        "partial_progress_must_be_reported",
        "unbounded_scan_allowed",
        "required_response_fields",
    }
    exact_keys(maintenance, keys, "maintenance", errors)
    if maintenance.get("type_name") != "MemoryProviderMaintenanceV1":
        errors.append("maintenance type drifted")
    if maintenance.get("capability_id") != "maintenance.run.v1":
        errors.append("maintenance capability ID drifted")
    if maintenance.get("mutation") is not True:
        errors.append("maintenance must be a provider-local mutation")
    if maintenance.get("tasks") != [
        "consolidate",
        "decay",
        "prune_expired",
        "validate_state",
        "repair",
        "compact",
    ]:
        errors.append("maintenance tasks drifted")
    for field in (
        "all_limits_positive_and_finite",
        "effective_duration_is_minimum_of_request_handshake_and_deadline",
        "one_mutating_maintenance_operation_per_provider_scope",
        "deadline_and_cancellation_reach_provider_loop",
        "partial_progress_must_be_reported",
    ):
        if maintenance.get(field) is not True:
            errors.append(f"maintenance.{field} must be true")
    for field, hard_max in (
        ("maximum_items_hard_limit", 1_000_000),
        ("maximum_bytes_hard_limit", 1_073_741_824),
        ("maximum_duration_millis_hard_limit", 3_600_000),
    ):
        value = maintenance.get(field)
        if not isinstance(value, int) or not 1 <= value <= hard_max:
            errors.append(f"maintenance.{field} must be positive and bounded")
    if maintenance.get("concurrent_mutation_policy") != "maintenance_busy":
        errors.append("concurrent maintenance mutation must be maintenance_busy")
    if maintenance.get("unbounded_scan_allowed") is not False:
        errors.append("unbounded maintenance scan must be false")

    inspection = obj(contract.get("inspection"), "inspection", errors)
    keys = {
        "type_name",
        "contract_id",
        "capability_id",
        "mutation",
        "required_request_fields",
        "views",
        "all_limits_positive_and_finite",
        "provider_internal_secret_material_allowed",
        "raw_credentials_allowed",
        "hidden_canonical_authority_allowed",
        "redaction_required",
        "cursor_scope_bound",
        "required_response_fields",
    }
    exact_keys(inspection, keys, "inspection", errors)
    if inspection.get("type_name") != "MemoryProviderInspectionV1":
        errors.append("inspection type drifted")
    if inspection.get("capability_id") != "inspection.read.v1":
        errors.append("inspection capability ID drifted")
    if inspection.get("mutation") is not False:
        errors.append("inspection mutation must be false")
    if inspection.get("all_limits_positive_and_finite") is not True:
        errors.append("inspection limits must be positive and finite")
    for field in (
        "provider_internal_secret_material_allowed",
        "raw_credentials_allowed",
        "hidden_canonical_authority_allowed",
    ):
        if inspection.get(field) is not False:
            errors.append(f"inspection.{field} must be false")
    if inspection.get("redaction_required") is not True:
        errors.append("inspection redaction must be required")
    if inspection.get("cursor_scope_bound") is not True:
        errors.append("inspection cursor must be scope-bound")


def validate_correction_deletion(
    contract: dict[str, Any], errors: list[str]
) -> None:
    correction = obj(contract.get("correction"), "correction", errors)
    keys = {
        "type_name",
        "contract_id",
        "capability_id",
        "mutation",
        "required_request_fields",
        "target_selection_rule",
        "target_kinds",
        "correction_kinds",
        "expected_target_revision_required",
        "revision_mismatch_policy",
        "idempotent",
        "same_key_different_correction_policy",
        "native_fact_mutation_allowed",
        "source_code_mutation_allowed",
        "required_response_fields",
    }
    exact_keys(correction, keys, "correction", errors)
    if correction.get("capability_id") != "correction.apply.v1":
        errors.append("correction capability ID drifted")
    if correction.get("target_selection_rule") != "exactly_one":
        errors.append("correction target must select exactly one kind")
    if correction.get("target_kinds") != [
        "stable_memory_ref",
        "recall_trace_ref",
        "source_ref",
    ]:
        errors.append("correction target kinds drifted")
    if correction.get("expected_target_revision_required") is not True:
        errors.append("correction expected target revision must be required")
    if correction.get("revision_mismatch_policy") != "revision_conflict":
        errors.append("correction revision mismatch must be conflict")
    if correction.get("idempotent") is not True:
        errors.append("correction must be idempotent")
    if correction.get("same_key_different_correction_policy") != (
        "idempotency_conflict"
    ):
        errors.append("different correction under same key must conflict")
    for field in ("native_fact_mutation_allowed", "source_code_mutation_allowed"):
        if correction.get(field) is not False:
            errors.append(f"correction.{field} must be false")

    deletion = obj(
        contract.get("deletion_by_source"), "deletion_by_source", errors
    )
    keys = {
        "type_name",
        "contract_id",
        "capability_id",
        "mutation",
        "required_request_fields",
        "minimum_source_keys",
        "maximum_source_keys",
        "duplicate_source_key_policy",
        "modes",
        "default_mode",
        "include_snapshots_required",
        "provider_may_report_success_without_verification",
        "verifiable_postcondition",
        "idempotent",
        "same_key_different_source_set_policy",
        "native_fact_deletion_allowed",
        "session_evidence_deletion_allowed",
        "required_response_fields",
    }
    exact_keys(deletion, keys, "deletion_by_source", errors)
    if deletion.get("capability_id") != "deletion.by_source.v1":
        errors.append("deletion-by-source capability ID drifted")
    minimum = deletion.get("minimum_source_keys")
    maximum = deletion.get("maximum_source_keys")
    if not (
        isinstance(minimum, int)
        and isinstance(maximum, int)
        and 1 <= minimum <= maximum <= 1024
    ):
        errors.append("deletion source-key count must be positive and bounded")
    if deletion.get("duplicate_source_key_policy") != "reject_non_canonical_request":
        errors.append("duplicate deletion source keys must be rejected")
    if deletion.get("modes") != [
        "remove_influence",
        "hard_delete",
        "anonymize",
    ]:
        errors.append("deletion modes drifted")
    if deletion.get("default_mode") != "remove_influence":
        errors.append("default deletion mode must remove influence")
    if deletion.get("include_snapshots_required") is not True:
        errors.append("deletion request must explicitly address snapshots")
    if deletion.get("provider_may_report_success_without_verification") is not False:
        errors.append("deletion cannot report success without verification")
    postcondition = obj(
        deletion.get("verifiable_postcondition"),
        "deletion_by_source.verifiable_postcondition",
        errors,
    )
    post_keys = {
        "required_fields",
        "verification_states",
        "successful_remove_requires_remaining_influence_zero",
        "retention_lock_requires_explicit_receipt",
        "snapshot_omission_may_be_silent",
        "provider_recall_may_return_deleted_source",
    }
    exact_keys(
        postcondition,
        post_keys,
        "deletion_by_source.verifiable_postcondition",
        errors,
    )
    required_post_fields = [
        "matched_effects",
        "removed_effects",
        "anonymized_effects",
        "retained_under_lock",
        "remaining_influence_count",
        "snapshots_examined",
        "snapshots_rewritten",
        "verification_query_digest",
        "verification_state",
        "state_generation_before",
        "state_generation_after",
    ]
    if postcondition.get("required_fields") != required_post_fields:
        errors.append("deletion postcondition fields drifted")
    if postcondition.get("verification_states") != [
        "verified_absent",
        "verified_anonymized",
        "retained_under_explicit_lock",
        "verification_failed",
        "partial",
    ]:
        errors.append("deletion verification states drifted")
    for field in (
        "successful_remove_requires_remaining_influence_zero",
        "retention_lock_requires_explicit_receipt",
    ):
        if postcondition.get(field) is not True:
            errors.append(f"deletion postcondition {field} must be true")
    for field in (
        "snapshot_omission_may_be_silent",
        "provider_recall_may_return_deleted_source",
    ):
        if postcondition.get(field) is not False:
            errors.append(f"deletion postcondition {field} must be false")
    if deletion.get("idempotent") is not True:
        errors.append("deletion by source must be idempotent")
    if deletion.get("same_key_different_source_set_policy") != (
        "idempotency_conflict"
    ):
        errors.append("different deletion source set under same key must conflict")
    for field in (
        "native_fact_deletion_allowed",
        "session_evidence_deletion_allowed",
    ):
        if deletion.get(field) is not False:
            errors.append(f"deletion_by_source.{field} must be false")


def validate_snapshot_replay_projection(
    contract: dict[str, Any], errors: list[str]
) -> None:
    snapshot = obj(contract.get("snapshot"), "snapshot", errors)
    keys = {
        "type_name",
        "export_contract_id",
        "restore_contract_id",
        "export_capability_id",
        "restore_capability_id",
        "snapshot_identity_required_fields",
        "snapshot_bytes_limit_source",
        "export_is_read_only",
        "export_consistent_generation_required",
        "restore_mutation",
        "restore_idempotent",
        "restore_requires_exact_provider",
        "restore_requires_compatible_implementation",
        "restore_requires_compatible_state_schema",
        "restore_requires_exact_scope",
        "restore_requires_digest_match",
        "restore_requires_expected_generation",
        "implicit_reset_allowed",
        "implicit_overwrite_allowed",
        "incompatible_restore_outcome",
        "restore_required_response_fields",
    }
    exact_keys(snapshot, keys, "snapshot", errors)
    if snapshot.get("export_capability_id") != "snapshot.export.v1":
        errors.append("snapshot export capability ID drifted")
    if snapshot.get("restore_capability_id") != "snapshot.restore.v1":
        errors.append("snapshot restore capability ID drifted")
    if snapshot.get("snapshot_identity_required_fields") != [
        "snapshot_id",
        "provider_id",
        "implementation_identity_digest",
        "state_schema_version",
        "exact_scope_digest",
        "state_generation",
        "observation_sequence",
        "parent_snapshot_id",
        "content_sha256",
        "byte_length",
        "created_at",
    ]:
        errors.append("snapshot identity fields drifted")
    if snapshot.get("snapshot_bytes_limit_source") != "effective_limit.snapshot_bytes":
        errors.append("snapshot byte limit must come from handshake")
    for field in (
        "export_is_read_only",
        "export_consistent_generation_required",
        "restore_mutation",
        "restore_idempotent",
        "restore_requires_exact_provider",
        "restore_requires_compatible_implementation",
        "restore_requires_compatible_state_schema",
        "restore_requires_exact_scope",
        "restore_requires_digest_match",
        "restore_requires_expected_generation",
    ):
        if snapshot.get(field) is not True:
            errors.append(f"snapshot.{field} must be true")
    for field in ("implicit_reset_allowed", "implicit_overwrite_allowed"):
        if snapshot.get(field) is not False:
            errors.append(f"snapshot.{field} must be false")
    if snapshot.get("incompatible_restore_outcome") != "state_incompatible":
        errors.append("incompatible snapshot restore must be state_incompatible")

    replay = obj(contract.get("replay"), "replay", errors)
    keys = {
        "type_name",
        "contract_id",
        "capability_id",
        "mutation",
        "required_request_fields",
        "batch_refs_are_canonical_observation_receipts",
        "sequence_monotonic_required",
        "sequence_gap_policy",
        "idempotent",
        "duplicate_sequence_same_digest_policy",
        "duplicate_sequence_different_digest_policy",
        "deadline_and_cancellation_reach_provider_loop",
        "partial_progress_must_be_reported",
        "required_response_fields",
    }
    exact_keys(replay, keys, "replay", errors)
    if replay.get("capability_id") != "replay.apply.v1":
        errors.append("replay capability ID drifted")
    for field in (
        "batch_refs_are_canonical_observation_receipts",
        "sequence_monotonic_required",
        "idempotent",
        "deadline_and_cancellation_reach_provider_loop",
        "partial_progress_must_be_reported",
    ):
        if replay.get(field) is not True:
            errors.append(f"replay.{field} must be true")
    if replay.get("sequence_gap_policy") != "sequence_gap":
        errors.append("replay sequence gap must be explicit")
    if replay.get("duplicate_sequence_same_digest_policy") != (
        "duplicate_acknowledged"
    ):
        errors.append("replay duplicate sequence/same digest must acknowledge")
    if replay.get("duplicate_sequence_different_digest_policy") != (
        "idempotency_conflict"
    ):
        errors.append("replay duplicate sequence/different digest must conflict")

    projection = obj(
        contract.get("provider_local_projection"),
        "provider_local_projection",
        errors,
    )
    keys = {
        "explicit_fact_capability_id",
        "explain_trace_capability_id",
        "explicit_projection_is_native_fact_authority",
        "provider_explanation_is_proof",
        "promotion_to_native_requires_separate_authorized_operation",
        "missing_projection_or_explanation_outcome",
    }
    exact_keys(projection, keys, "provider_local_projection", errors)
    if projection.get("explicit_fact_capability_id") != "facts.explicit.v1":
        errors.append("explicit fact projection capability ID drifted")
    if projection.get("explain_trace_capability_id") != "explain.trace.v1":
        errors.append("explain trace capability ID drifted")
    for field in (
        "explicit_projection_is_native_fact_authority",
        "provider_explanation_is_proof",
    ):
        if projection.get(field) is not False:
            errors.append(f"provider_local_projection.{field} must be false")
    if projection.get("promotion_to_native_requires_separate_authorized_operation") is not True:
        errors.append("provider projection promotion must be separately authorized")


def validate_terminal_invariants_beads(
    contract: dict[str, Any], issue_ids: set[str], errors: list[str]
) -> None:
    terminal = arr(
        contract.get("lifecycle_specific_terminal_states"),
        "lifecycle_specific_terminal_states",
        errors,
    )
    if set(terminal) != LIFECYCLE_TERMINALS or len(terminal) != len(
        LIFECYCLE_TERMINALS
    ):
        errors.append("lifecycle terminal states must exactly cover V1 outcomes")
    if "cancelled" not in terminal or "deadline_exceeded" not in terminal:
        errors.append("cancellation and deadline must remain distinct")
    if "capability_unsupported" not in terminal:
        errors.append("lifecycle terminal states must include capability_unsupported")
    if "partial_effect" not in terminal or "effect_unknown" not in terminal:
        errors.append("lifecycle terminals must expose partial and unknown effects")

    invariants = arr(contract.get("invariants"), "invariants", errors)
    if len(invariants) < 15 or len(set(invariants)) != len(invariants):
        errors.append("lifecycle contract must state at least fifteen unique invariants")
    serialized = " ".join(str(value) for value in invariants).casefold()
    for phrase in REQUIRED_INVARIANTS:
        if phrase.casefold() not in serialized:
            errors.append(f"lifecycle invariants are missing {phrase!r}")

    beads = arr(contract.get("verification_beads"), "verification_beads", errors)
    if len(beads) < 10 or len(set(beads)) != len(beads):
        errors.append("verification_beads must contain at least ten unique issues")
    for value in beads:
        check_bead(value, "verification_beads", issue_ids, errors)
    for required in (
        "tdmem-0206",
        "tdmem-0207",
        "tdmem-0209",
        "tdmem-0402",
        "tdmem-0503",
        "tdmem-0504",
        "tdmem-0506",
        "tdmem-0802",
        "tdmem-0803",
        "tdmem-0805",
        "tdmem-0806",
    ):
        if required not in beads:
            errors.append(f"verification_beads is missing {required}")


def validate_schema(schema: dict[str, Any], errors: list[str]) -> None:
    if schema.get("$schema") != "https://json-schema.org/draft/2020-12/schema":
        errors.append("lifecycle schema must use JSON Schema 2020-12")
    if schema.get("type") != "object" or schema.get("additionalProperties") is not False:
        errors.append("lifecycle schema root must be a strict object")
    if set(schema.get("required", [])) != TOP_LEVEL:
        errors.append("lifecycle schema required fields must match contract")
    properties = obj(schema.get("properties"), "schema.properties", errors)
    if properties.get("schema_version", {}).get("const") != 1:
        errors.append("lifecycle schema must pin schema_version 1")
    if properties.get("contract_id", {}).get("const") != (
        "tracedecay.memory.provider.lifecycle.v1"
    ):
        errors.append("lifecycle schema must pin contract_id")
    if properties.get("bead_id", {}).get("const") != "tdmem-0205":
        errors.append("lifecycle schema must pin bead_id tdmem-0205")
    if properties.get("lifecycle_specific_terminal_states", {}).get("minItems") != 25:
        errors.append("lifecycle schema must require twenty-five terminal states")
    if properties.get("invariants", {}).get("minItems") != 15:
        errors.append("lifecycle schema must require fifteen invariants")


def validate_doc(path: Path, errors: list[str]) -> None:
    try:
        text = path.read_text(encoding="utf-8")
    except OSError as exc:
        errors.append(f"could not load lifecycle documentation: {exc}")
        return
    for phrase in REQUIRED_DOC_PHRASES:
        if phrase.casefold() not in text.casefold():
            errors.append(f"lifecycle documentation is missing {phrase!r}")
    if "TBD" in text or "TODO" in text:
        errors.append("lifecycle documentation contains unresolved TBD/TODO text")


def validate_dependencies(repo: Path, errors: list[str]) -> dict[str, Any]:
    paths = {
        "registry": "provider-registry-contract.json",
        "handshake": "provider-handshake-contract.json",
        "observation": "provider-observation-contract.json",
        "recall": "provider-recall-contract.json",
    }
    loaded: dict[str, Any] = {}
    for key, filename in paths.items():
        loaded[key] = load_object(
            repo / "product/contracts/memory-provider-v1" / filename,
            f"{key} contract",
            errors,
        )
        if loaded[key].get("status") != "accepted":
            errors.append(f"lifecycle requires accepted {key} contract")
    if loaded["handshake"].get("side_effect_contract", {}).get(
        "handshake_is_read_only"
    ) is not True:
        errors.append("lifecycle requires read-only handshake readiness")
    if loaded["observation"].get("idempotency", {}).get(
        "provider_must_persist_deduplication"
    ) is not True:
        errors.append("lifecycle requires persistent provider deduplication")
    if loaded["recall"].get("provider_candidate", {}).get(
        "stable_memory_ref_required"
    ) is not False:
        errors.append("lifecycle feedback requires optional stable memory references")
    return loaded


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
    validate_common(contract, errors)
    validate_capability_gating(contract, dependencies.get("registry", {}), errors)
    validate_health(contract, errors)
    validate_feedback(contract, errors)
    validate_maintenance_inspection(contract, errors)
    validate_correction_deletion(contract, errors)
    validate_snapshot_replay_projection(contract, errors)
    validate_terminal_invariants_beads(contract, issue_ids, errors)
    validate_schema(schema, errors)
    validate_doc(doc_path, errors)
    return errors


def main() -> int:
    args = parse_args()
    repo = args.repo.resolve()
    bootstrap: list[str] = []
    contract = load_object(resolve(repo, args.contract), "lifecycle contract", bootstrap)
    schema = load_object(resolve(repo, args.schema), "lifecycle schema", bootstrap)
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
                "lifecycle_capability_count": len(
                    contract["capability_gating"]["capability_to_operation"]
                ),
                "feedback_target_kinds": contract["feedback"]["target"][
                    "target_kinds"
                ],
                "maintenance_task_count": len(contract["maintenance"]["tasks"]),
                "forget_postcondition_required": contract["deletion_by_source"][
                    "provider_may_report_success_without_verification"
                ]
                is False,
                "terminal_state_count": len(
                    contract["lifecycle_specific_terminal_states"]
                ),
                "unsupported_outcome": contract["capability_gating"][
                    "unsupported_operation_outcome"
                ],
            },
            indent=2,
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
