#!/usr/bin/env python3
"""Validate normalized, idempotent, post-settlement provider observations."""

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
    "observation_envelope",
    "source_identity",
    "observation_kinds",
    "extension_contract",
    "memory_effect_semantics",
    "normalization",
    "idempotency",
    "provenance",
    "privacy",
    "ordering",
    "batch_contract",
    "admission_order",
    "provider_acceptance_outcomes",
    "delivery_receipt",
    "observer_non_interference",
    "invariants",
    "verification_beads",
}

ENVELOPE_FIELDS = [
    "observation_id",
    "idempotency_key",
    "provider_id",
    "registration_revision",
    "ready_receipt_digest",
    "exact_scope_identity",
    "source_identity",
    "observation_kind",
    "payload_contract",
    "canonical_payload",
    "payload_sha256",
    "extensions",
    "provenance",
    "privacy",
    "occurred_at",
    "admitted_at",
    "source_sequence",
    "request_identity",
    "deadline",
    "cancellation",
]

SOURCE_FIELDS = [
    "source_authority",
    "source_event_id",
    "source_event_revision",
    "source_event_sha256",
    "canonical_settlement_receipt",
]

SOURCE_AUTHORITIES = {
    "host_session",
    "tool_execution",
    "source_edit",
    "test_execution",
    "diagnostic_broker",
    "git_evidence",
    "native_fact_promotion",
    "feedback_outcome",
    "automation_outcome",
}

KINDS = {
    "session.message_committed.v1": (
        "host_session",
        "tracedecay.memory.observation.session-message.v1",
    ),
    "tool.execution_settled.v1": (
        "tool_execution",
        "tracedecay.memory.observation.tool-execution.v1",
    ),
    "source.edit_settled.v1": (
        "source_edit",
        "tracedecay.memory.observation.source-edit.v1",
    ),
    "test.execution_settled.v1": (
        "test_execution",
        "tracedecay.memory.observation.test-execution.v1",
    ),
    "diagnostic.observed.v1": (
        "diagnostic_broker",
        "tracedecay.memory.observation.diagnostic.v1",
    ),
    "git.evidence_observed.v1": (
        "git_evidence",
        "tracedecay.memory.observation.git-evidence.v1",
    ),
    "native.fact_promoted.v1": (
        "native_fact_promotion",
        "tracedecay.memory.observation.native-fact-promotion.v1",
    ),
    "feedback.outcome_settled.v1": (
        "feedback_outcome",
        "tracedecay.memory.observation.feedback-outcome.v1",
    ),
    "automation.outcome_settled.v1": (
        "automation_outcome",
        "tracedecay.memory.observation.automation-outcome.v1",
    ),
}

EXTENSION_FIELDS = [
    "extension_id",
    "extension_version",
    "criticality",
    "canonical_payload",
    "payload_sha256",
]

PROVENANCE_FIELDS = [
    "origin_kind",
    "origin_identity",
    "actor_identity",
    "host_identity",
    "evidence_anchors",
    "transform_chain",
]

PRIVACY_FIELDS = [
    "classification",
    "retention_class",
    "redaction_revision",
    "content_policy_revision",
    "forget_source_key",
    "expires_at",
]

BATCH_FIELDS = [
    "batch_id",
    "provider_id",
    "registration_revision",
    "ready_receipt_digest",
    "exact_scope_identity",
    "observations",
    "request_identity",
    "deadline",
    "cancellation",
]

OUTCOMES = {
    "applied",
    "duplicate_acknowledged",
    "rejected_contract_violation",
    "rejected_scope_mismatch",
    "rejected_provenance_unavailable",
    "rejected_privacy_policy",
    "rejected_payload_too_large",
    "rejected_extension_unsupported",
    "idempotency_conflict",
    "provider_unavailable",
    "deadline_exceeded",
    "cancelled",
    "partial_effect",
    "effect_unknown",
}

RECEIPT_FIELDS = [
    "receipt_id",
    "observation_id",
    "idempotency_key",
    "payload_sha256",
    "extensions_digest",
    "provider_id",
    "provider_instance_id",
    "registration_revision",
    "state_generation_before",
    "state_generation_after",
    "attempt_number",
    "outcome",
    "committed_effect",
    "provider_effect_summary",
    "provider_receipt_digest",
    "started_at",
    "finished_at",
    "warnings",
]

EFFECT_SUMMARY_FIELDS = [
    "effect_count",
    "stable_memory_refs",
    "provider_trace_refs",
    "no_effect_reason",
]

REQUIRED_DOC = [
    "after the canonical TraceDecay source event settles",
    "exact profile, project, repository, worktree, branch, and agent-session scope",
    "unknown **optional** extensions are preserved byte-for-byte",
    "unknown **required** extensions fail explicitly",
    "An observation is an admitted input event. It is **not** a memory record",
    "same key + same canonical payload/extensions",
    "same key + different canonical payload/extensions",
    "crash window after provider commit but before acknowledgement",
    "providers must tolerate duplicate and out-of-order delivery",
    "partial commits must report per-item effects",
    "Success without provider acknowledgement is forbidden",
    "Provider latency/failure cannot delay or change it",
]

REQUIRED_INVARIANT_PHRASES = [
    "canonically settled TraceDecay source event",
    "exact profile, project, repository, worktree, branch, and agent-session scope",
    "Unknown optional extensions round-trip",
    "unknown required extensions fail explicitly",
    "not a provider memory record",
    "zero, one, or many opaque provider-local effects",
    "deterministic across retries and crashes",
    "same idempotency key with the same payload and extensions",
    "survive acknowledgement loss",
    "at least once",
    "fail closed",
    "never mutates current code",
    "cannot alter the canonical source outcome",
    "success is impossible without provider acknowledgement",
    "partial commits",
    "may not extend retention",
]

BEAD_RE = re.compile(r"^tdmem-[0-9]{4}$")
VERSIONED_ID_RE = re.compile(
    r"^[a-z][a-z0-9]*(?:[._-][a-z0-9]+)*\.v[1-9][0-9]*$"
)
EXTENSION_ID_RE = re.compile(r"^[a-z][a-z0-9]*(?:[._-][a-z0-9]+)*$")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--repo", type=Path, default=Path("."))
    parser.add_argument(
        "--contract",
        type=Path,
        default=Path(
            "product/contracts/memory-provider-v1/provider-observation-contract.json"
        ),
    )
    parser.add_argument(
        "--schema",
        type=Path,
        default=Path(
            "product/contracts/memory-provider-v1/provider-observation-contract.schema.json"
        ),
    )
    parser.add_argument(
        "--doc",
        type=Path,
        default=Path(
            "product/contracts/memory-provider-v1/provider-observation-contract.md"
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
    if set(row) != expected:
        errors.append(
            f"{label} fields drifted; "
            f"missing={sorted(expected - set(row))}, extra={sorted(set(row) - expected)}"
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


def unique_by(
    rows: Iterable[Any], field: str, label: str, errors: list[str]
) -> dict[str, dict[str, Any]]:
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
    if contract.get("contract_id") != "tracedecay.memory.provider.observation.v1":
        errors.append(
            "contract_id must be tracedecay.memory.provider.observation.v1"
        )
    if contract.get("bead_id") != "tdmem-0203":
        errors.append("bead_id must be tdmem-0203")
    if contract.get("status") != "accepted":
        errors.append("contract status must be accepted")
    if contract.get("authority") != "TraceDecay observation admission and dispatch fabric":
        errors.append("observation authority must remain TraceDecay admission/dispatch")
    if contract.get("scope") != "coding_agents_only":
        errors.append("observation scope must remain coding_agents_only")
    if contract.get("depends_on_contracts") != [
        "tracedecay.memory.provider.registry.v1",
        "tracedecay.memory.provider.handshake.v1",
    ]:
        errors.append(
            "observation contract dependencies must be registry then handshake V1"
        )
    nonempty(contract, "title", "contract", errors)


def validate_envelope(contract: dict[str, Any], errors: list[str]) -> None:
    envelope = obj(contract.get("observation_envelope"), "observation_envelope", errors)
    keys = {
        "type_name",
        "required_fields",
        "observation_id_type",
        "maximum_envelope_bytes",
        "maximum_payload_bytes",
        "unknown_field_policy",
        "empty_payload_allowed",
    }
    exact_keys(envelope, keys, "observation_envelope", errors)
    if envelope.get("type_name") != "MemoryProviderObservationEnvelopeV1":
        errors.append("observation envelope type drifted")
    if envelope.get("required_fields") != ENVELOPE_FIELDS:
        errors.append(
            "observation envelope required fields must remain canonical and ordered"
        )
    if envelope.get("observation_id_type") != "uuid_v7_lowercase":
        errors.append("observation ID must be lowercase UUIDv7")
    maximum_envelope = envelope.get("maximum_envelope_bytes")
    maximum_payload = envelope.get("maximum_payload_bytes")
    if not (
        isinstance(maximum_envelope, int)
        and isinstance(maximum_payload, int)
        and 1 <= maximum_payload < maximum_envelope <= 4_194_304
    ):
        errors.append("observation payload/envelope byte limits must be finite and nested")
    if envelope.get("unknown_field_policy") != "reject_contract_violation":
        errors.append("unknown observation envelope fields must reject")
    if envelope.get("empty_payload_allowed") is not False:
        errors.append("empty observation payload must be false")


def validate_source_and_kinds(contract: dict[str, Any], errors: list[str]) -> None:
    source = obj(contract.get("source_identity"), "source_identity", errors)
    keys = {
        "type_name",
        "required_fields",
        "source_authorities",
        "source_event_id_maximum_bytes",
        "source_event_revision_minimum",
        "canonical_settlement_receipt_required",
        "unsettled_source_policy",
        "path_is_source_identity",
    }
    exact_keys(source, keys, "source_identity", errors)
    if source.get("type_name") != "MemoryProviderObservationSourceIdentityV1":
        errors.append("source identity type drifted")
    if source.get("required_fields") != SOURCE_FIELDS:
        errors.append("source identity required fields drifted")
    if set(source.get("source_authorities", [])) != SOURCE_AUTHORITIES:
        errors.append("source authorities must exactly cover the nine V1 admitted sources")
    maximum_id = source.get("source_event_id_maximum_bytes")
    if not isinstance(maximum_id, int) or not 1 <= maximum_id <= 256:
        errors.append("source event ID must be bounded by 256 bytes")
    if source.get("source_event_revision_minimum") != 0:
        errors.append("source event revision minimum must be zero")
    if source.get("canonical_settlement_receipt_required") is not True:
        errors.append("canonical settlement receipt must be required")
    if source.get("unsettled_source_policy") != "reject_not_canonically_settled":
        errors.append("unsettled source events must be rejected")
    if source.get("path_is_source_identity") is not False:
        errors.append("path must not be source identity")

    kinds = unique_by(
        arr(contract.get("observation_kinds"), "observation_kinds", errors),
        "id",
        "observation_kinds",
        errors,
    )
    if set(kinds) != set(KINDS):
        errors.append(
            "observation kinds must exactly contain the nine V1 coding-agent events"
        )
    for kind_id, (authority, payload) in KINDS.items():
        row = kinds.get(kind_id, {})
        exact_keys(
            row,
            {"id", "source_authority", "payload_contract"},
            f"observation_kind[{kind_id}]",
            errors,
        )
        if VERSIONED_ID_RE.fullmatch(kind_id) is None:
            errors.append(f"observation kind ID is non-canonical: {kind_id}")
        if row.get("source_authority") != authority or row.get("payload_contract") != payload:
            errors.append(
                f"observation kind {kind_id} authority or payload contract drifted"
            )


def validate_extensions(contract: dict[str, Any], errors: list[str]) -> None:
    extension = obj(contract.get("extension_contract"), "extension_contract", errors)
    keys = {
        "type_name",
        "required_fields",
        "extension_id_pattern",
        "extension_version_minimum",
        "maximum_extensions",
        "maximum_extension_bytes",
        "maximum_total_extension_bytes",
        "criticality_values",
        "known_extension_policy",
        "unknown_optional_extension_policy",
        "unknown_required_extension_policy",
        "unknown_extension_may_activate_behavior",
        "unknown_extension_may_mutate_authority",
        "provider_may_drop_preserved_extension",
        "provider_specific_top_level_fields_allowed",
    }
    exact_keys(extension, keys, "extension_contract", errors)
    if extension.get("type_name") != "MemoryProviderObservationExtensionV1":
        errors.append("observation extension type drifted")
    if extension.get("required_fields") != EXTENSION_FIELDS:
        errors.append("observation extension required fields drifted")
    pattern_raw = extension.get("extension_id_pattern")
    try:
        pattern = re.compile(pattern_raw) if isinstance(pattern_raw, str) else None
    except re.error as exc:
        errors.append(f"extension ID pattern is invalid: {exc}")
        pattern = None
    if pattern is None:
        errors.append("extension ID pattern must be a string regex")
    else:
        for accepted in ("coding.git", "vendor_hint", "x-foo", "abc.v2"):
            if pattern.fullmatch(accepted) is None:
                errors.append(f"extension ID pattern rejects canonical example {accepted!r}")
        for rejected in ("", "X", "a..b", "/foo", " foo"):
            if pattern.fullmatch(rejected) is not None:
                errors.append(f"extension ID pattern accepts non-canonical example {rejected!r}")
    if extension.get("extension_version_minimum") != 1:
        errors.append("extension version minimum must be one")
    maximum_extensions = extension.get("maximum_extensions")
    maximum_one = extension.get("maximum_extension_bytes")
    maximum_total = extension.get("maximum_total_extension_bytes")
    if not (
        isinstance(maximum_extensions, int)
        and 1 <= maximum_extensions <= 32
        and isinstance(maximum_one, int)
        and 1 <= maximum_one <= 262_144
        and isinstance(maximum_total, int)
        and maximum_one <= maximum_total <= 524_288
    ):
        errors.append("extension count and byte limits must be finite and nested")
    if extension.get("criticality_values") != ["optional", "required"]:
        errors.append("extension criticality values must remain optional then required")
    if extension.get("known_extension_policy") != "validate_against_versioned_extension_contract":
        errors.append("known extensions must use versioned validation")
    if extension.get("unknown_optional_extension_policy") != "preserve_opaque_inert_round_trip":
        errors.append("unknown optional extensions must round-trip inertly")
    if extension.get("unknown_required_extension_policy") != "reject_extension_unsupported":
        errors.append("unknown required extensions must fail explicitly")
    for field in (
        "unknown_extension_may_activate_behavior",
        "unknown_extension_may_mutate_authority",
        "provider_may_drop_preserved_extension",
        "provider_specific_top_level_fields_allowed",
    ):
        if extension.get(field) is not False:
            errors.append(f"extension_contract.{field} must be false")


def validate_memory_effect_semantics(
    contract: dict[str, Any], errors: list[str]
) -> None:
    effect = obj(
        contract.get("memory_effect_semantics"),
        "memory_effect_semantics",
        errors,
    )
    keys = {
        "observation_is_memory_record",
        "observation_id_is_provider_memory_id",
        "stable_provider_memory_id_required",
        "provider_effect_cardinality",
        "provider_internal_representation_is_opaque",
        "provider_may_consolidate_multiple_observations",
        "provider_may_split_one_observation_into_multiple_traces",
        "provider_may_reject_without_effect",
        "provider_effect_summary_is_not_canonical_memory_state",
        "provider_effect_summary_required_in_delivery_receipt",
        "traceability_requirement",
        "native_fact_promotion_is_separate_authorized_operation",
    }
    exact_keys(effect, keys, "memory_effect_semantics", errors)
    for field in (
        "observation_is_memory_record",
        "observation_id_is_provider_memory_id",
        "stable_provider_memory_id_required",
    ):
        if effect.get(field) is not False:
            errors.append(f"memory_effect_semantics.{field} must be false")
    if effect.get("provider_effect_cardinality") != "zero_one_or_many_provider_internal_effects":
        errors.append("provider effect cardinality must allow zero, one, or many")
    for field in (
        "provider_internal_representation_is_opaque",
        "provider_may_consolidate_multiple_observations",
        "provider_may_split_one_observation_into_multiple_traces",
        "provider_may_reject_without_effect",
        "provider_effect_summary_is_not_canonical_memory_state",
        "provider_effect_summary_required_in_delivery_receipt",
        "native_fact_promotion_is_separate_authorized_operation",
    ):
        if effect.get(field) is not True:
            errors.append(f"memory_effect_semantics.{field} must be true")
    traceability = nonempty(
        effect, "traceability_requirement", "memory_effect_semantics", errors
    ).casefold()
    for phrase in ("observation_id", "idempotency_key", "delivery receipt"):
        if phrase not in traceability:
            errors.append(
                f"memory effect traceability requirement is missing {phrase!r}"
            )


def validate_normalization_and_idempotency(
    contract: dict[str, Any], errors: list[str]
) -> None:
    normalization = obj(contract.get("normalization"), "normalization", errors)
    keys = {
        "canonical_encoding",
        "unicode_normalization",
        "object_key_order",
        "floating_point_values_allowed",
        "non_finite_numbers_allowed",
        "duplicate_object_keys_allowed",
        "unknown_payload_contract_policy",
        "payload_digest",
        "extension_payload_digest",
        "envelope_digest",
        "transport_metadata_in_digest",
        "provider_specific_payload_shape_allowed",
    }
    exact_keys(normalization, keys, "normalization", errors)
    expected_strings = {
        "canonical_encoding": "rfc8785_json",
        "unicode_normalization": "nfc",
        "object_key_order": "lexicographic_utf8",
        "unknown_payload_contract_policy": "reject_contract_violation",
        "payload_digest": "sha256_over_canonical_payload",
        "extension_payload_digest": "sha256_over_canonical_extension_payload",
        "envelope_digest": "sha256_over_canonical_envelope_without_transport_metadata",
    }
    for field, value in expected_strings.items():
        if normalization.get(field) != value:
            errors.append(f"normalization.{field} must be {value}")
    for field in (
        "floating_point_values_allowed",
        "non_finite_numbers_allowed",
        "duplicate_object_keys_allowed",
        "transport_metadata_in_digest",
        "provider_specific_payload_shape_allowed",
    ):
        if normalization.get(field) is not False:
            errors.append(f"normalization.{field} must be false")

    idem = obj(contract.get("idempotency"), "idempotency", errors)
    keys = {
        "type_name",
        "encoding",
        "derivation",
        "stable_across_delivery_retries",
        "stable_across_dispatch_process_restart",
        "stable_across_provider_restart",
        "stable_across_transport_topology",
        "provider_must_persist_deduplication",
        "same_key_same_payload_outcome",
        "same_key_different_payload_outcome",
        "duplicate_acknowledgement_evidence",
        "duplicate_acknowledgement_may_be_inferred",
        "same_source_new_revision_requires_new_key",
        "random_retry_key_allowed",
        "timestamp_only_key_allowed",
    }
    exact_keys(idem, keys, "idempotency", errors)
    if idem.get("type_name") != "MemoryProviderObservationIdempotencyKeyV1":
        errors.append("observation idempotency type drifted")
    if idem.get("encoding") != "lowercase_hex_64":
        errors.append("observation idempotency encoding must be lowercase_hex_64")
    derivation = str(idem.get("derivation", "")).casefold()
    for phrase in (
        "provider",
        "registration",
        "scope",
        "source_authority",
        "source_event_id",
        "source_event_revision",
        "observation_kind",
        "payload_contract",
        "payload_sha256",
        "extensions_digest",
    ):
        if phrase not in derivation:
            errors.append(f"idempotency derivation is missing {phrase}")
    for field in (
        "stable_across_delivery_retries",
        "stable_across_dispatch_process_restart",
        "stable_across_provider_restart",
        "stable_across_transport_topology",
        "provider_must_persist_deduplication",
        "same_source_new_revision_requires_new_key",
    ):
        if idem.get(field) is not True:
            errors.append(f"idempotency.{field} must be true")
    if idem.get("same_key_same_payload_outcome") != "duplicate_acknowledged":
        errors.append("same key/same payload must acknowledge duplicate")
    if idem.get("same_key_different_payload_outcome") != "idempotency_conflict":
        errors.append("same key/different payload must be idempotency conflict")
    if idem.get("duplicate_acknowledgement_evidence") != (
        "terminal_committed_effect_state_duplicate_bound_to_request_idempotency_key"
    ):
        errors.append(
            "duplicate acknowledgement must be proven by bound duplicate committed-effect evidence"
        )
    for field in (
        "random_retry_key_allowed",
        "timestamp_only_key_allowed",
        "duplicate_acknowledgement_may_be_inferred",
    ):
        if idem.get(field) is not False:
            errors.append(f"idempotency.{field} must be false")


def validate_provenance_privacy_ordering(
    contract: dict[str, Any], errors: list[str]
) -> None:
    provenance = obj(contract.get("provenance"), "provenance", errors)
    keys = {
        "type_name",
        "required_fields",
        "origin_kinds",
        "maximum_evidence_anchors",
        "maximum_transform_steps",
        "missing_provenance_policy",
        "provider_may_rewrite_origin",
        "provider_may_drop_transform_chain",
    }
    exact_keys(provenance, keys, "provenance", errors)
    if provenance.get("type_name") != "MemoryProviderObservationProvenanceV1":
        errors.append("observation provenance type drifted")
    if provenance.get("required_fields") != PROVENANCE_FIELDS:
        errors.append("observation provenance required fields drifted")
    if set(provenance.get("origin_kinds", [])) != {
        "user",
        "agent",
        "tool",
        "repository",
        "tracedecay_native",
        "automation",
    }:
        errors.append("observation origin kinds drifted")
    if not isinstance(provenance.get("maximum_evidence_anchors"), int) or not (
        1 <= provenance["maximum_evidence_anchors"] <= 64
    ):
        errors.append("evidence anchors must be bounded at 64")
    if not isinstance(provenance.get("maximum_transform_steps"), int) or not (
        1 <= provenance["maximum_transform_steps"] <= 32
    ):
        errors.append("transform chain must be bounded at 32")
    if provenance.get("missing_provenance_policy") != "reject_provenance_unavailable":
        errors.append("missing provenance must be rejected")
    if provenance.get("provider_may_rewrite_origin") is not False:
        errors.append("provider cannot rewrite provenance origin")
    if provenance.get("provider_may_drop_transform_chain") is not False:
        errors.append("provider cannot drop provenance transform chain")

    privacy = obj(contract.get("privacy"), "privacy", errors)
    keys = {
        "type_name",
        "required_fields",
        "classifications",
        "retention_classes",
        "raw_secret_material_allowed",
        "unadmitted_personal_data_allowed",
        "forget_source_key_required",
        "provider_may_extend_expiry",
        "missing_privacy_metadata_policy",
    }
    exact_keys(privacy, keys, "privacy", errors)
    if privacy.get("type_name") != "MemoryProviderObservationPrivacyV1":
        errors.append("observation privacy type drifted")
    if privacy.get("required_fields") != PRIVACY_FIELDS:
        errors.append("observation privacy required fields drifted")
    if privacy.get("classifications") != [
        "public",
        "internal",
        "sensitive",
        "restricted",
    ]:
        errors.append("privacy classifications must remain ordered")
    if privacy.get("retention_classes") != [
        "ephemeral",
        "session",
        "project",
        "profile",
    ]:
        errors.append("retention classes must remain ordered")
    for field in (
        "raw_secret_material_allowed",
        "unadmitted_personal_data_allowed",
        "provider_may_extend_expiry",
    ):
        if privacy.get(field) is not False:
            errors.append(f"privacy.{field} must be false")
    if privacy.get("forget_source_key_required") is not True:
        errors.append("forget source key must be required")
    if privacy.get("missing_privacy_metadata_policy") != "reject_privacy_metadata_unavailable":
        errors.append("missing privacy metadata must be rejected")

    ordering = obj(contract.get("ordering"), "ordering", errors)
    keys = {
        "source_sequence_type",
        "source_sequence_scope",
        "source_sequence_monotonic_required",
        "delivery_order_guaranteed",
        "provider_must_tolerate_out_of_order_delivery",
        "provider_must_tolerate_duplicate_delivery",
        "occurred_at_is_order_authority",
        "admitted_at_is_order_authority",
        "missing_predecessor_policy",
    }
    exact_keys(ordering, keys, "ordering", errors)
    if ordering.get("source_sequence_type") != "unsigned_64":
        errors.append("source sequence type must be unsigned_64")
    if ordering.get("source_sequence_scope") != "source_authority_plus_exact_scope_plus_source_stream":
        errors.append("source sequence scope drifted")
    for field in (
        "source_sequence_monotonic_required",
        "provider_must_tolerate_out_of_order_delivery",
        "provider_must_tolerate_duplicate_delivery",
    ):
        if ordering.get(field) is not True:
            errors.append(f"ordering.{field} must be true")
    for field in (
        "delivery_order_guaranteed",
        "occurred_at_is_order_authority",
        "admitted_at_is_order_authority",
    ):
        if ordering.get(field) is not False:
            errors.append(f"ordering.{field} must be false")
    if ordering.get("missing_predecessor_policy") != (
        "accept_with_gap_warning_unless_payload_contract_requires_predecessor"
    ):
        errors.append("missing predecessor policy drifted")


def validate_batch_and_receipt(contract: dict[str, Any], errors: list[str]) -> None:
    batch = obj(contract.get("batch_contract"), "batch_contract", errors)
    keys = {
        "type_name",
        "required_fields",
        "homogeneous_fields",
        "minimum_items",
        "maximum_items_source",
        "maximum_bytes_source",
        "duplicate_idempotency_keys_in_batch_policy",
        "partial_batch_commit_must_be_reported",
        "atomic_batch_required",
    }
    exact_keys(batch, keys, "batch_contract", errors)
    if batch.get("type_name") != "MemoryProviderObservationBatchV1":
        errors.append("observation batch type drifted")
    if batch.get("required_fields") != BATCH_FIELDS:
        errors.append("observation batch required fields drifted")
    if batch.get("homogeneous_fields") != [
        "provider_id",
        "registration_revision",
        "ready_receipt_digest",
        "exact_scope_identity",
    ]:
        errors.append("batch homogeneous fields drifted")
    if batch.get("minimum_items") != 1:
        errors.append("observation batch minimum must be one")
    if batch.get("maximum_items_source") != "effective_limit.observation_batch_items":
        errors.append("batch item limit must come from compatible readiness limits")
    if batch.get("maximum_bytes_source") != "effective_limit.request_bytes":
        errors.append("batch byte limit must come from compatible readiness limits")
    if batch.get("duplicate_idempotency_keys_in_batch_policy") != "reject_non_canonical_batch":
        errors.append("duplicate batch idempotency keys must be rejected")
    if batch.get("partial_batch_commit_must_be_reported") is not True:
        errors.append("partial batch commit must be reported")
    if batch.get("atomic_batch_required") is not False:
        errors.append("batch contract must not assume atomicity")

    outcomes = arr(
        contract.get("provider_acceptance_outcomes"),
        "provider_acceptance_outcomes",
        errors,
    )
    if set(outcomes) != OUTCOMES or len(outcomes) != len(OUTCOMES):
        errors.append("provider acceptance outcomes must exactly cover V1 receipt states")

    receipt = obj(contract.get("delivery_receipt"), "delivery_receipt", errors)
    keys = {
        "type_name",
        "required_fields",
        "attempt_number_minimum",
        "committed_effect_states",
        "provider_effect_summary_fields",
        "stable_memory_refs_optional",
        "provider_trace_refs_optional",
        "provider_receipt_digest_required_for",
        "receipt_is_immutable",
        "success_without_provider_acknowledgement_allowed",
    }
    exact_keys(receipt, keys, "delivery_receipt", errors)
    if receipt.get("type_name") != "MemoryProviderObservationDeliveryReceiptV1":
        errors.append("delivery receipt type drifted")
    if receipt.get("required_fields") != RECEIPT_FIELDS:
        errors.append("delivery receipt required fields drifted")
    if receipt.get("attempt_number_minimum") != 1:
        errors.append("delivery attempt numbers must start at one")
    if receipt.get("committed_effect_states") != [
        "none",
        "applied",
        "duplicate",
        "partial",
        "unknown",
    ]:
        errors.append("committed effect states must remain ordered")
    if receipt.get("provider_effect_summary_fields") != EFFECT_SUMMARY_FIELDS:
        errors.append("provider effect summary fields drifted")
    if receipt.get("stable_memory_refs_optional") is not True:
        errors.append("stable memory references must remain optional")
    if receipt.get("provider_trace_refs_optional") is not True:
        errors.append("provider trace references must remain optional")
    if set(receipt.get("provider_receipt_digest_required_for", [])) != {
        "applied",
        "duplicate_acknowledged",
        "partial_effect",
    }:
        errors.append("provider receipt digest requirements drifted")
    if receipt.get("receipt_is_immutable") is not True:
        errors.append("delivery receipt must be immutable")
    if receipt.get("success_without_provider_acknowledgement_allowed") is not False:
        errors.append("success without provider acknowledgement must be false")


def validate_admission_and_observer(
    contract: dict[str, Any], errors: list[str]
) -> None:
    steps = arr(contract.get("admission_order"), "admission_order", errors)
    if len(steps) != 10 or len(set(steps)) != 10:
        errors.append("observation admission order must contain ten unique steps")
    serialized = " ".join(str(step) for step in steps).casefold()
    for phrase in (
        "canonically settled source receipt",
        "same exact scope",
        "before journal append",
        "payload contract, extensions",
        "canonicalize payload and extensions",
        "durable bounded observation journal",
        "dispatch at least once",
        "deduplicate by idempotency key",
        "delivery receipt for every",
        "never alter the already-settled canonical source outcome",
    ):
        if phrase not in serialized:
            errors.append(f"observation admission order is missing {phrase!r}")

    observer = obj(
        contract.get("observer_non_interference"),
        "observer_non_interference",
        errors,
    )
    keys = {
        "canonical_source_settlement_precedes_observation",
        "provider_failure_may_change_source_outcome",
        "provider_latency_may_delay_source_settlement",
        "provider_output_may_enter_context_in_observer_mode",
        "provider_output_may_trigger_tools_or_external_actions_in_observer_mode",
        "provider_output_may_mutate_native_facts_in_observer_mode",
    }
    exact_keys(observer, keys, "observer_non_interference", errors)
    if observer.get("canonical_source_settlement_precedes_observation") is not True:
        errors.append("canonical source settlement must precede observation")
    for field in keys - {"canonical_source_settlement_precedes_observation"}:
        if observer.get(field) is not False:
            errors.append(f"observer_non_interference.{field} must be false")


def validate_invariants_and_beads(
    contract: dict[str, Any], ids: set[str], errors: list[str]
) -> None:
    invariants = arr(contract.get("invariants"), "invariants", errors)
    if len(invariants) < 16 or len(set(invariants)) != len(invariants):
        errors.append("observation contract must state at least sixteen unique invariants")
    serialized = " ".join(str(value) for value in invariants).casefold()
    for phrase in REQUIRED_INVARIANT_PHRASES:
        if phrase.casefold() not in serialized:
            errors.append(f"observation invariants are missing {phrase!r}")

    beads = arr(contract.get("verification_beads"), "verification_beads", errors)
    if len(beads) < 8 or len(set(beads)) != len(beads):
        errors.append("verification_beads must contain at least eight unique issues")
    for value in beads:
        validate_bead(value, "verification_beads", ids, errors)
    for required in (
        "tdmem-0205",
        "tdmem-0206",
        "tdmem-0209",
        "tdmem-0502",
        "tdmem-0503",
        "tdmem-0505",
        "tdmem-0506",
        "tdmem-0903",
    ):
        if required not in beads:
            errors.append(f"verification_beads is missing {required}")


def validate_schema(schema: dict[str, Any], errors: list[str]) -> None:
    if schema.get("$schema") != "https://json-schema.org/draft/2020-12/schema":
        errors.append("observation schema must use JSON Schema 2020-12")
    if schema.get("type") != "object" or schema.get("additionalProperties") is not False:
        errors.append("observation schema root must be a strict object")
    if set(schema.get("required", [])) != TOP_LEVEL:
        errors.append("observation schema required fields must match the contract")
    properties = obj(schema.get("properties"), "schema.properties", errors)
    if properties.get("schema_version", {}).get("const") != 1:
        errors.append("observation schema must pin schema_version 1")
    if properties.get("contract_id", {}).get("const") != (
        "tracedecay.memory.provider.observation.v1"
    ):
        errors.append("observation schema must pin contract_id")
    if properties.get("bead_id", {}).get("const") != "tdmem-0203":
        errors.append("observation schema must pin bead_id tdmem-0203")
    if properties.get("provider_acceptance_outcomes", {}).get("minItems") != 14:
        errors.append("observation schema must require fourteen acceptance outcomes")
    if properties.get("invariants", {}).get("minItems") != 16:
        errors.append("observation schema must require sixteen invariants")
    definitions = obj(schema.get("$defs"), "schema.$defs", errors)
    for name in ("beadId", "object", "observationKind"):
        if name not in definitions:
            errors.append(f"observation schema is missing $defs.{name}")
    if definitions.get("observationKind", {}).get("additionalProperties") is not False:
        errors.append("observation kind schema must deny additional properties")


def validate_doc(path: Path, errors: list[str]) -> None:
    try:
        text = path.read_text(encoding="utf-8")
    except OSError as exc:
        errors.append(f"could not load observation documentation: {exc}")
        return
    for phrase in REQUIRED_DOC:
        if phrase.casefold() not in text.casefold():
            errors.append(f"observation documentation is missing {phrase!r}")
    if "TBD" in text or "TODO" in text:
        errors.append("observation documentation contains unresolved TBD/TODO text")


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
        errors.append("observation requires accepted provider registry V1")
    if handshake.get("status") != "accepted" or handshake.get("contract_id") != (
        "tracedecay.memory.provider.handshake.v1"
    ):
        errors.append("observation requires accepted provider handshake V1")

    capability_registry = registry.get("capability_registry")
    mandatory = (
        capability_registry.get("mandatory", [])
        if isinstance(capability_registry, dict)
        else []
    )
    mandatory_ids = {
        row.get("id") for row in mandatory if isinstance(row, dict)
    }
    if "observation.accept.v1" not in mandatory_ids:
        errors.append("provider registry must retain mandatory observation.accept.v1")

    if handshake.get("side_effect_contract", {}).get("handshake_is_read_only") is not True:
        errors.append("observation depends on read-only handshake readiness")
    scope_fields = handshake.get("exact_scope_identity", {}).get("required_fields", [])
    for required in (
        "profile_id",
        "project_id",
        "repository_identity",
        "worktree_identity",
        "branch_identity",
        "agent_session_id",
        "resolved_scope_digest",
    ):
        if required not in scope_fields:
            errors.append(
                f"handshake exact scope is missing observation-required field {required}"
            )


def validate(
    repo: Path,
    contract: dict[str, Any],
    schema: dict[str, Any],
    doc: Path,
    ids: set[str],
) -> list[str]:
    errors: list[str] = []
    validate_header(contract, errors)
    validate_envelope(contract, errors)
    validate_source_and_kinds(contract, errors)
    validate_extensions(contract, errors)
    validate_memory_effect_semantics(contract, errors)
    validate_normalization_and_idempotency(contract, errors)
    validate_provenance_privacy_ordering(contract, errors)
    validate_batch_and_receipt(contract, errors)
    validate_admission_and_observer(contract, errors)
    validate_invariants_and_beads(contract, ids, errors)
    validate_schema(schema, errors)
    validate_doc(doc, errors)
    validate_dependencies(repo, errors)
    return errors


def main() -> int:
    args = parse_args()
    repo = args.repo.resolve()
    bootstrap: list[str] = []
    contract = load_object(resolve(repo, args.contract), "observation contract", bootstrap)
    schema = load_object(resolve(repo, args.schema), "observation schema", bootstrap)
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
                "observation_kind_count": len(contract["observation_kinds"]),
                "acceptance_outcome_count": len(
                    contract["provider_acceptance_outcomes"]
                ),
                "extension_policy": contract["extension_contract"][
                    "unknown_optional_extension_policy"
                ],
                "observation_is_memory_record": contract[
                    "memory_effect_semantics"
                ]["observation_is_memory_record"],
                "stable_memory_ref_required": contract["memory_effect_semantics"][
                    "stable_provider_memory_id_required"
                ],
                "delivery_semantics": "at_least_once_idempotent",
                "canonical_source_settlement_precedes_observation": contract[
                    "observer_non_interference"
                ]["canonical_source_settlement_precedes_observation"],
                "silent_success_without_acknowledgement": contract[
                    "delivery_receipt"
                ]["success_without_provider_acknowledgement_allowed"],
            },
            indent=2,
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
