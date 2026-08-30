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
    "session.message_committed.v1": ("host_session", "tracedecay.memory.observation.session-message.v1"),
    "tool.execution_settled.v1": ("tool_execution", "tracedecay.memory.observation.tool-execution.v1"),
    "source.edit_settled.v1": ("source_edit", "tracedecay.memory.observation.source-edit.v1"),
    "test.execution_settled.v1": ("test_execution", "tracedecay.memory.observation.test-execution.v1"),
    "diagnostic.observed.v1": ("diagnostic_broker", "tracedecay.memory.observation.diagnostic.v1"),
    "git.evidence_observed.v1": ("git_evidence", "tracedecay.memory.observation.git-evidence.v1"),
    "native.fact_promoted.v1": ("native_fact_promotion", "tracedecay.memory.observation.native-fact-promotion.v1"),
    "feedback.outcome_settled.v1": ("feedback_outcome", "tracedecay.memory.observation.feedback-outcome.v1"),
    "automation.outcome_settled.v1": ("automation_outcome", "tracedecay.memory.observation.automation-outcome.v1"),
}
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
    "provider_id",
    "provider_instance_id",
    "registration_revision",
    "state_generation_before",
    "state_generation_after",
    "attempt_number",
    "outcome",
    "committed_effect",
    "provider_receipt_digest",
    "started_at",
    "finished_at",
    "warnings",
]
REQUIRED_DOC = [
    "after the canonical TraceDecay source event settles",
    "never participate in the source transaction",
    "same key + same payload digest",
    "same key + different payload digest",
    "crash window after provider commit but before acknowledgement",
    "providers must tolerate duplicate and out-of-order delivery",
    "partial commits must report per-item effects",
    "Success without provider acknowledgement is forbidden",
    "Provider latency/failure cannot delay or change it",
]
BEAD_RE = re.compile(r"^tdmem-[0-9]{4}$")
VERSIONED_ID_RE = re.compile(r"^[a-z][a-z0-9]*(?:[._-][a-z0-9]+)*\.v[1-9][0-9]*$")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--repo", type=Path, default=Path("."))
    parser.add_argument("--contract", type=Path, default=Path("product/contracts/memory-provider-v1/provider-observation-contract.json"))
    parser.add_argument("--schema", type=Path, default=Path("product/contracts/memory-provider-v1/provider-observation-contract.schema.json"))
    parser.add_argument("--doc", type=Path, default=Path("product/contracts/memory-provider-v1/provider-observation-contract.md"))
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
        errors.append(f"{label} fields drifted; missing={sorted(expected-set(row))}, extra={sorted(set(row)-expected)}")


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
    if contract.get("contract_id") != "tracedecay.memory.provider.observation.v1":
        errors.append("contract_id must be tracedecay.memory.provider.observation.v1")
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
        errors.append("observation contract dependencies must be registry then handshake V1")
    nonempty(contract, "title", "contract", errors)


def validate_envelope_source_kinds(contract: dict[str, Any], errors: list[str]) -> None:
    envelope = obj(contract.get("observation_envelope"), "observation_envelope", errors)
    envelope_keys = {
        "type_name", "required_fields", "observation_id_type", "maximum_envelope_bytes",
        "maximum_payload_bytes", "unknown_field_policy", "empty_payload_allowed"
    }
    exact_keys(envelope, envelope_keys, "observation_envelope", errors)
    if envelope.get("type_name") != "MemoryProviderObservationEnvelopeV1":
        errors.append("observation envelope type drifted")
    if envelope.get("required_fields") != ENVELOPE_FIELDS:
        errors.append("observation envelope required fields must remain canonical and ordered")
    if envelope.get("observation_id_type") != "uuid_v7_lowercase":
        errors.append("observation ID must be lowercase UUIDv7")
    max_envelope = envelope.get("maximum_envelope_bytes")
    max_payload = envelope.get("maximum_payload_bytes")
    if not isinstance(max_envelope, int) or not isinstance(max_payload, int) or not 1 <= max_payload < max_envelope <= 4194304:
        errors.append("observation payload/envelope byte limits must be finite and nested")
    if envelope.get("unknown_field_policy") != "reject_contract_violation":
        errors.append("unknown observation envelope fields must reject")
    if envelope.get("empty_payload_allowed") is not False:
        errors.append("empty observation payload must be false")

    source = obj(contract.get("source_identity"), "source_identity", errors)
    source_keys = {
        "type_name", "required_fields", "source_authorities", "source_event_id_maximum_bytes",
        "source_event_revision_minimum", "canonical_settlement_receipt_required",
        "unsettled_source_policy", "path_is_source_identity"
    }
    exact_keys(source, source_keys, "source_identity", errors)
    if source.get("type_name") != "MemoryProviderObservationSourceIdentityV1":
        errors.append("source identity type drifted")
    if source.get("required_fields") != SOURCE_FIELDS:
        errors.append("source identity required fields drifted")
    if set(source.get("source_authorities", [])) != SOURCE_AUTHORITIES:
        errors.append("source authorities must exactly cover the nine V1 admitted sources")
    if not isinstance(source.get("source_event_id_maximum_bytes"), int) or not 1 <= source["source_event_id_maximum_bytes"] <= 256:
        errors.append("source event ID must be bounded by 256 bytes")
    if source.get("source_event_revision_minimum") != 0:
        errors.append("source event revision minimum must be zero")
    if source.get("canonical_settlement_receipt_required") is not True:
        errors.append("canonical settlement receipt must be required")
    if source.get("unsettled_source_policy") != "reject_not_canonically_settled":
        errors.append("unsettled source events must be rejected")
    if source.get("path_is_source_identity") is not False:
        errors.append("path must not be source identity")

    kinds = unique_by(arr(contract.get("observation_kinds"), "observation_kinds", errors), "id", "observation_kinds", errors)
    if set(kinds) != set(KINDS):
        errors.append("observation kinds must exactly contain the nine V1 coding-agent events")
    for kind_id, expected in KINDS.items():
        row = kinds.get(kind_id, {})
        exact_keys(row, {"id", "source_authority", "payload_contract"}, f"observation_kind[{kind_id}]", errors)
        if VERSIONED_ID_RE.fullmatch(kind_id) is None:
            errors.append(f"observation kind ID is non-canonical: {kind_id}")
        authority, payload = expected
        if row.get("source_authority") != authority or row.get("payload_contract") != payload:
            errors.append(f"observation kind {kind_id} authority or payload contract drifted")
        if authority not in SOURCE_AUTHORITIES:
            errors.append(f"observation kind {kind_id} uses unknown source authority")


def validate_normalization_idempotency(contract: dict[str, Any], errors: list[str]) -> None:
    normalization = obj(contract.get("normalization"), "normalization", errors)
    normalization_keys = {
        "canonical_encoding", "unicode_normalization", "object_key_order", "floating_point_values_allowed",
        "non_finite_numbers_allowed", "duplicate_object_keys_allowed", "unknown_payload_contract_policy",
        "payload_digest", "envelope_digest", "transport_metadata_in_digest", "provider_specific_payload_shape_allowed"
    }
    exact_keys(normalization, normalization_keys, "normalization", errors)
    expected = {
        "canonical_encoding": "rfc8785_json",
        "unicode_normalization": "nfc",
        "object_key_order": "lexicographic_utf8",
        "unknown_payload_contract_policy": "reject_contract_violation",
        "payload_digest": "sha256_over_canonical_payload",
        "envelope_digest": "sha256_over_canonical_envelope_without_transport_metadata",
    }
    for field, value in expected.items():
        if normalization.get(field) != value:
            errors.append(f"normalization.{field} must be {value}")
    for field in (
        "floating_point_values_allowed", "non_finite_numbers_allowed", "duplicate_object_keys_allowed",
        "transport_metadata_in_digest", "provider_specific_payload_shape_allowed"
    ):
        if normalization.get(field) is not False:
            errors.append(f"normalization.{field} must be false")

    idem = obj(contract.get("idempotency"), "idempotency", errors)
    idem_keys = {
        "type_name", "encoding", "derivation", "stable_across_delivery_retries",
        "stable_across_dispatch_process_restart", "stable_across_provider_restart",
        "stable_across_transport_topology", "provider_must_persist_deduplication",
        "same_key_same_payload_outcome", "same_key_different_payload_outcome",
        "same_source_new_revision_requires_new_key", "random_retry_key_allowed", "timestamp_only_key_allowed"
    }
    exact_keys(idem, idem_keys, "idempotency", errors)
    if idem.get("type_name") != "MemoryProviderObservationIdempotencyKeyV1" or idem.get("encoding") != "lowercase_hex_64":
        errors.append("observation idempotency identity or encoding drifted")
    derivation = str(idem.get("derivation", "")).casefold()
    for phrase in ("provider", "registration", "scope", "source_authority", "source_event_id", "source_event_revision", "observation_kind", "payload_contract"):
        if phrase not in derivation:
            errors.append(f"idempotency derivation is missing {phrase}")
    for field in (
        "stable_across_delivery_retries", "stable_across_dispatch_process_restart",
        "stable_across_provider_restart", "stable_across_transport_topology",
        "provider_must_persist_deduplication", "same_source_new_revision_requires_new_key"
    ):
        if idem.get(field) is not True:
            errors.append(f"idempotency.{field} must be true")
    if idem.get("same_key_same_payload_outcome") != "duplicate_acknowledged":
        errors.append("same key/same payload must acknowledge duplicate")
    if idem.get("same_key_different_payload_outcome") != "idempotency_conflict":
        errors.append("same key/different payload must be idempotency conflict")
    for field in ("random_retry_key_allowed", "timestamp_only_key_allowed"):
        if idem.get(field) is not False:
            errors.append(f"idempotency.{field} must be false")


def validate_provenance_privacy_ordering(contract: dict[str, Any], errors: list[str]) -> None:
    provenance = obj(contract.get("provenance"), "provenance", errors)
    provenance_keys = {
        "type_name", "required_fields", "origin_kinds", "maximum_evidence_anchors",
        "maximum_transform_steps", "missing_provenance_policy", "provider_may_rewrite_origin",
        "provider_may_drop_transform_chain"
    }
    exact_keys(provenance, provenance_keys, "provenance", errors)
    if provenance.get("type_name") != "MemoryProviderObservationProvenanceV1" or provenance.get("required_fields") != PROVENANCE_FIELDS:
        errors.append("observation provenance type or fields drifted")
    if set(provenance.get("origin_kinds", [])) != {"user", "agent", "tool", "repository", "tracedecay_native", "automation"}:
        errors.append("observation origin kinds drifted")
    if not isinstance(provenance.get("maximum_evidence_anchors"), int) or not 1 <= provenance["maximum_evidence_anchors"] <= 64:
        errors.append("evidence anchors must be bounded at 64")
    if not isinstance(provenance.get("maximum_transform_steps"), int) or not 1 <= provenance["maximum_transform_steps"] <= 32:
        errors.append("transform chain must be bounded at 32")
    if provenance.get("missing_provenance_policy") != "reject_provenance_unavailable":
        errors.append("missing provenance must be rejected")
    if provenance.get("provider_may_rewrite_origin") is not False or provenance.get("provider_may_drop_transform_chain") is not False:
        errors.append("provider cannot rewrite origin or drop transform chain")

    privacy = obj(contract.get("privacy"), "privacy", errors)
    privacy_keys = {
        "type_name", "required_fields", "classifications", "retention_classes", "raw_secret_material_allowed",
        "unadmitted_personal_data_allowed", "forget_source_key_required", "provider_may_extend_expiry",
        "missing_privacy_metadata_policy"
    }
    exact_keys(privacy, privacy_keys, "privacy", errors)
    if privacy.get("type_name") != "MemoryProviderObservationPrivacyV1" or privacy.get("required_fields") != PRIVACY_FIELDS:
        errors.append("observation privacy type or fields drifted")
    if privacy.get("classifications") != ["public", "internal", "sensitive", "restricted"]:
        errors.append("privacy classifications must remain ordered")
    if privacy.get("retention_classes") != ["ephemeral", "session", "project", "profile"]:
        errors.append("retention classes must remain ordered")
    for field in ("raw_secret_material_allowed", "unadmitted_personal_data_allowed", "provider_may_extend_expiry"):
        if privacy.get(field) is not False:
            errors.append(f"privacy.{field} must be false")
    if privacy.get("forget_source_key_required") is not True:
        errors.append("forget source key must be required")
    if privacy.get("missing_privacy_metadata_policy") != "reject_privacy_metadata_unavailable":
        errors.append("missing privacy metadata must be rejected")

    ordering = obj(contract.get("ordering"), "ordering", errors)
    ordering_keys = {
        "source_sequence_type", "source_sequence_scope", "source_sequence_monotonic_required",
        "delivery_order_guaranteed", "provider_must_tolerate_out_of_order_delivery",
        "provider_must_tolerate_duplicate_delivery", "occurred_at_is_order_authority",
        "admitted_at_is_order_authority", "missing_predecessor_policy"
    }
    exact_keys(ordering, ordering_keys, "ordering", errors)
    if ordering.get("source_sequence_type") != "unsigned_64":
        errors.append("source sequence type must be unsigned_64")
    if ordering.get("source_sequence_scope") != "source_authority_plus_exact_scope_plus_source_stream":
        errors.append("source sequence scope drifted")
    for field in ("source_sequence_monotonic_required", "provider_must_tolerate_out_of_order_delivery", "provider_must_tolerate_duplicate_delivery"):
        if ordering.get(field) is not True:
            errors.append(f"ordering.{field} must be true")
    for field in ("delivery_order_guaranteed", "occurred_at_is_order_authority", "admitted_at_is_order_authority"):
        if ordering.get(field) is not False:
            errors.append(f"ordering.{field} must be false")
    if ordering.get("missing_predecessor_policy") != "accept_with_gap_warning_unless_payload_contract_requires_predecessor":
        errors.append("missing predecessor policy drifted")


def validate_batch_outcomes_receipt(contract: dict[str, Any], errors: list[str]) -> None:
    batch = obj(contract.get("batch_contract"), "batch_contract", errors)
    batch_keys = {
        "type_name", "required_fields", "homogeneous_fields", "minimum_items", "maximum_items_source",
        "maximum_bytes_source", "duplicate_idempotency_keys_in_batch_policy",
        "partial_batch_commit_must_be_reported", "atomic_batch_required"
    }
    exact_keys(batch, batch_keys, "batch_contract", errors)
    if batch.get("type_name") != "MemoryProviderObservationBatchV1" or batch.get("required_fields") != BATCH_FIELDS:
        errors.append("observation batch type or fields drifted")
    if batch.get("homogeneous_fields") != ["provider_id", "registration_revision", "ready_receipt_digest", "exact_scope_identity"]:
        errors.append("batch homogeneous fields drifted")
    if batch.get("minimum_items") != 1:
        errors.append("observation batch minimum must be one")
    if batch.get("maximum_items_source") != "effective_limit.observation_batch_items" or batch.get("maximum_bytes_source") != "effective_limit.request_bytes":
        errors.append("batch limits must come from compatible effective limits")
    if batch.get("duplicate_idempotency_keys_in_batch_policy") != "reject_non_canonical_batch":
        errors.append("duplicate batch idempotency keys must be rejected")
    if batch.get("partial_batch_commit_must_be_reported") is not True or batch.get("atomic_batch_required") is not False:
        errors.append("batch contract must report partial commit and not assume atomicity")

    outcomes = arr(contract.get("provider_acceptance_outcomes"), "provider_acceptance_outcomes", errors)
    if set(outcomes) != OUTCOMES or len(outcomes) != len(OUTCOMES):
        errors.append("provider acceptance outcomes must exactly cover V1 receipt states")

    receipt = obj(contract.get("delivery_receipt"), "delivery_receipt", errors)
    receipt_keys = {
        "type_name", "required_fields", "attempt_number_minimum", "committed_effect_states",
        "provider_receipt_digest_required_for", "receipt_is_immutable", "success_without_provider_acknowledgement_allowed"
    }
    exact_keys(receipt, receipt_keys, "delivery_receipt", errors)
    if receipt.get("type_name") != "MemoryProviderObservationDeliveryReceiptV1" or receipt.get("required_fields") != RECEIPT_FIELDS:
        errors.append("delivery receipt type or fields drifted")
    if receipt.get("attempt_number_minimum") != 1:
        errors.append("delivery attempt numbers must start at one")
    if receipt.get("committed_effect_states") != ["none", "applied", "duplicate", "partial", "unknown"]:
        errors.append("committed effect states must remain ordered")
    if set(receipt.get("provider_receipt_digest_required_for", [])) != {"applied", "duplicate_acknowledged", "partial_effect"}:
        errors.append("provider receipt digest requirements drifted")
    if receipt.get("receipt_is_immutable") is not True:
        errors.append("delivery receipt must be immutable")
    if receipt.get("success_without_provider_acknowledgement_allowed") is not False:
        errors.append("success without provider acknowledgement must be false")


def validate_admission_noninterference(contract: dict[str, Any], errors: list[str]) -> None:
    steps = arr(contract.get("admission_order"), "admission_order", errors)
    if len(steps) != 10 or len(set(steps)) != 10:
        errors.append("observation admission order must contain ten unique steps")
    serialized = " ".join(str(step) for step in steps).casefold()
    for phrase in (
        "canonically settled source receipt", "same exact scope", "before journal append", "canonicalize payload",
        "durable bounded observation journal", "dispatch at least once", "deduplicate by idempotency key",
        "delivery receipt for every", "never alter the already-settled canonical source outcome"
    ):
        if phrase not in serialized:
            errors.append(f"observation admission order is missing {phrase!r}")

    observer = obj(contract.get("observer_non_interference"), "observer_non_interference", errors)
    observer_keys = {
        "canonical_source_settlement_precedes_observation", "provider_failure_may_change_source_outcome",
        "provider_latency_may_delay_source_settlement", "provider_output_may_enter_context_in_observer_mode",
        "provider_output_may_trigger_tools_or_external_actions_in_observer_mode",
        "provider_output_may_mutate_native_facts_in_observer_mode"
    }
    exact_keys(observer, observer_keys, "observer_non_interference", errors)
    if observer.get("canonical_source_settlement_precedes_observation") is not True:
        errors.append("canonical source settlement must precede observation")
    for field in observer_keys - {"canonical_source_settlement_precedes_observation"}:
        if observer.get(field) is not False:
            errors.append(f"observer_non_interference.{field} must be false")


def validate_invariants_beads(contract: dict[str, Any], ids: set[str], errors: list[str]) -> None:
    invariants = arr(contract.get("invariants"), "invariants", errors)
    if len(invariants) < 13 or len(set(invariants)) != len(invariants):
        errors.append("observation contract must state at least thirteen unique invariants")
    serialized = " ".join(str(value) for value in invariants).casefold()
    for phrase in (
        "canonically settled", "immutable, exact-scope", "deterministic across process restart",
        "never depend only on timestamps", "same idempotency key with the same payload",
        "survive acknowledgement loss", "at least once", "fail closed", "never mutates current code",
        "cannot alter the canonical source outcome", "success is impossible without provider acknowledgement",
        "partial commits", "may not extend retention"
    ):
        if phrase not in serialized:
            errors.append(f"observation invariants are missing {phrase!r}")
    beads = arr(contract.get("verification_beads"), "verification_beads", errors)
    if len(beads) < 8 or len(set(beads)) != len(beads):
        errors.append("verification_beads must contain at least eight unique issues")
    for value in beads:
        bead(value, "verification_beads", ids, errors)
    for required in ("tdmem-0205", "tdmem-0206", "tdmem-0209", "tdmem-0502", "tdmem-0503", "tdmem-0505", "tdmem-0506", "tdmem-0903"):
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
    if properties.get("contract_id", {}).get("const") != "tracedecay.memory.provider.observation.v1":
        errors.append("observation schema must pin contract_id")
    if properties.get("bead_id", {}).get("const") != "tdmem-0203":
        errors.append("observation schema must pin bead_id tdmem-0203")
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
    registry = load_object(repo / "product/contracts/memory-provider-v1/provider-registry-contract.json", "provider registry contract", errors)
    handshake = load_object(repo / "product/contracts/memory-provider-v1/provider-handshake-contract.json", "provider handshake contract", errors)
    if registry.get("status") != "accepted" or registry.get("contract_id") != "tracedecay.memory.provider.registry.v1":
        errors.append("observation requires accepted provider registry V1")
    if handshake.get("status") != "accepted" or handshake.get("contract_id") != "tracedecay.memory.provider.handshake.v1":
        errors.append("observation requires accepted provider handshake V1")
    capabilities = {row.get("id") for row in registry.get("capability_catalog", []) if isinstance(row, dict)}
    if "observation.accept.v1" not in capabilities:
        errors.append("provider registry must retain observation.accept.v1")
    if handshake.get("side_effect_contract", {}).get("handshake_is_read_only") is not True:
        errors.append("observation depends on read-only handshake readiness")


def validate(repo: Path, contract: dict[str, Any], schema: dict[str, Any], doc: Path, ids: set[str]) -> list[str]:
    errors: list[str] = []
    validate_header(contract, errors)
    validate_envelope_source_kinds(contract, errors)
    validate_normalization_idempotency(contract, errors)
    validate_provenance_privacy_ordering(contract, errors)
    validate_batch_outcomes_receipt(contract, errors)
    validate_admission_noninterference(contract, errors)
    validate_invariants_beads(contract, ids, errors)
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
    print(json.dumps({
        "ok": True,
        "schema_version": contract["schema_version"],
        "contract_id": contract["contract_id"],
        "bead_id": contract["bead_id"],
        "status": contract["status"],
        "observation_kind_count": len(contract["observation_kinds"]),
        "acceptance_outcome_count": len(contract["provider_acceptance_outcomes"]),
        "delivery_semantics": "at_least_once_idempotent",
        "canonical_source_settlement_precedes_observation": contract["observer_non_interference"]["canonical_source_settlement_precedes_observation"],
        "silent_success_without_acknowledgement": contract["delivery_receipt"]["success_without_provider_acknowledgement_allowed"],
    }, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    sys.exit(main())
