#!/usr/bin/env python3
"""Validate the V1 memory-provider identity and capability registry contract."""

from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path
from typing import Any, Iterable

EXPECTED_TOP_LEVEL = {
    "schema_version",
    "contract_id",
    "bead_id",
    "title",
    "status",
    "authority",
    "scope",
    "provider_identity",
    "capability_identity",
    "capability_catalog",
    "registration_contract",
    "selection_contract",
    "bootstrap_slots",
    "invariants",
    "verification_beads",
}

EXPECTED_CAPABILITIES = {
    "observation.accept.v1": ("provider_local_mutation", "tdmem-0203"),
    "recall.query.v1": ("advisory_read", "tdmem-0204"),
    "feedback.record.v1": ("provider_local_mutation", "tdmem-0205"),
    "maintenance.run.v1": ("provider_local_mutation", "tdmem-0205"),
    "inspection.read.v1": ("provider_local_read", "tdmem-0205"),
    "correction.apply.v1": ("provider_local_mutation", "tdmem-0205"),
    "forget.by_source.v1": ("provider_local_mutation", "tdmem-0205"),
    "snapshot.export.v1": ("provider_local_read", "tdmem-0205"),
    "snapshot.restore.v1": ("provider_local_mutation", "tdmem-0205"),
}

EXPECTED_REGISTRATION_FIELDS = {
    "provider_id",
    "adapter_contract_version",
    "registration_state",
    "capability_declaration_state",
    "declared_capabilities",
    "implementation_identity",
    "registration_revision",
}

EXPECTED_REGISTRATION_STATES = ["registered", "disabled", "reserved", "retiring"]
EXPECTED_DECLARATION_STATES = [
    "declared",
    "deferred_until_compatible_handshake",
    "unavailable_without_versioned_specification",
]
EXPECTED_SELECTION_FIELDS = {
    "provider_id",
    "required_capabilities",
    "exact_scope_identity",
    "configuration_revision",
    "request_identity",
    "deadline",
    "cancellation",
}
EXPECTED_RESOLUTION_STATES = {
    "resolved",
    "provider_unknown",
    "provider_disabled",
    "provider_reserved",
    "provider_retiring",
    "adapter_unavailable",
    "capability_declaration_deferred",
    "capability_unsupported",
    "protocol_incompatible",
    "scope_unavailable",
    "configuration_revision_conflict",
    "deadline_exceeded",
    "cancelled",
    "ambiguous_registration",
}
EXPECTED_RESOLVED_REQUIRES = {
    "exact_provider_id_match",
    "accepted_registration_revision",
    "compatible_adapter_contract_version",
    "all_required_capabilities_declared",
    "exact_scope_admission",
    "live_deadline",
    "live_cancellation",
}
EXPECTED_SLOT_FIELDS = {
    "provider_id",
    "display_name",
    "slot_state",
    "specification_state",
    "capability_declaration_state",
    "implementation_gate_beads",
    "counts_as_implemented",
}
EXPECTED_BOOTSTRAP = {
    "tracedecay.native": {
        "slot_state": "declared",
        "specification_state": "versioned_native_application_ports",
        "capability_declaration_state": "deferred_until_compatible_handshake",
        "implementation_gate_beads": [
            "tdmem-0401",
            "tdmem-0402",
            "tdmem-0403",
            "tdmem-0404",
        ],
    },
    "ncm": {
        "slot_state": "reserved",
        "specification_state": "licensed_surface_audit_required",
        "capability_declaration_state": "deferred_until_compatible_handshake",
        "implementation_gate_beads": ["tdmem-0701", "tdmem-0702", "tdmem-0703"],
    },
    "ocean": {
        "slot_state": "reserved",
        "specification_state": "versioned_specification_unavailable",
        "capability_declaration_state": "unavailable_without_versioned_specification",
        "implementation_gate_beads": [],
    },
}

REQUIRED_INVARIANT_PHRASES = [
    "stable identity",
    "never inferred from provider names",
    "only the registry/composition boundary may branch",
    "remain provider-neutral",
    "all requested capabilities",
    "distinct typed states",
    "never silently falls back",
    "advisory with respect to TraceDecay canonical authorities",
    "does not select an execution topology",
    "cannot count as implemented",
]

REQUIRED_README_PHRASES = [
    "stable `MemoryProviderIdV1` identity",
    "versioned capability identity",
    "fail-closed provider resolution",
    "prohibition on provider-name branching",
    "There is no implicit fallback",
    "`tracedecay.native`",
    "`ncm`",
    "`ocean`",
    "None of the bootstrap slots counts as implemented",
    "Concrete Native/NCM adapters remain out of scope",
]

BEAD_RE = re.compile(r"^tdmem-[0-9]{4}$")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--repo", type=Path, default=Path("."))
    parser.add_argument(
        "--contract",
        type=Path,
        default=Path(
            "product/contracts/memory-provider-v1/provider-registry-contract.json"
        ),
    )
    parser.add_argument(
        "--schema",
        type=Path,
        default=Path(
            "product/contracts/memory-provider-v1/provider-registry-contract.schema.json"
        ),
    )
    parser.add_argument(
        "--readme",
        type=Path,
        default=Path("product/contracts/memory-provider-v1/README.md"),
    )
    parser.add_argument(
        "--issues", type=Path, default=Path(".beads/issues.jsonl")
    )
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
    issue_ids: set[str] = set()
    try:
        lines = path.read_text(encoding="utf-8").splitlines()
    except OSError as exc:
        errors.append(f"could not load Beads authority: {exc}")
        return issue_ids
    for line_number, line in enumerate(lines, start=1):
        if not line.strip():
            continue
        try:
            row = json.loads(line)
        except json.JSONDecodeError as exc:
            errors.append(f"invalid Beads JSONL at line {line_number}: {exc}")
            continue
        issue_id = row.get("id") if isinstance(row, dict) else None
        if not isinstance(issue_id, str):
            errors.append(f"Beads line {line_number} has no string id")
            continue
        if issue_id in issue_ids:
            errors.append(f"duplicate Beads issue id {issue_id}")
        issue_ids.add(issue_id)
    return issue_ids


def require_object(value: Any, field: str, errors: list[str]) -> dict[str, Any]:
    if not isinstance(value, dict):
        errors.append(f"{field} must be an object")
        return {}
    return value


def require_list(value: Any, field: str, errors: list[str]) -> list[Any]:
    if not isinstance(value, list):
        errors.append(f"{field} must be an array")
        return []
    return value


def require_exact_keys(
    value: dict[str, Any], expected: set[str], label: str, errors: list[str]
) -> None:
    actual = set(value)
    if actual != expected:
        missing = sorted(expected - actual)
        extra = sorted(actual - expected)
        errors.append(f"{label} fields drifted; missing={missing}, extra={extra}")


def non_empty_string(
    row: dict[str, Any], field: str, label: str, errors: list[str]
) -> str:
    value = row.get(field)
    if not isinstance(value, str) or not value.strip():
        errors.append(f"{label}.{field} must be a non-empty string")
        return ""
    return value.strip()


def validate_bead(value: Any, label: str, issue_ids: set[str], errors: list[str]) -> None:
    if not isinstance(value, str) or not BEAD_RE.fullmatch(value):
        errors.append(f"{label} must match tdmem-NNNN")
    elif value not in issue_ids:
        errors.append(f"{label} references unknown Beads issue {value}")


def index_unique(
    rows: Iterable[Any], field: str, label: str, errors: list[str]
) -> dict[str, dict[str, Any]]:
    indexed: dict[str, dict[str, Any]] = {}
    for offset, raw in enumerate(rows):
        if not isinstance(raw, dict):
            errors.append(f"{label}[{offset}] must be an object")
            continue
        value = raw.get(field)
        if not isinstance(value, str) or not value:
            errors.append(f"{label}[{offset}].{field} must be a non-empty string")
            continue
        if value in indexed:
            errors.append(f"duplicate {label} {field} {value}")
            continue
        indexed[value] = raw
    return indexed


def compile_declared_pattern(
    raw: Any, label: str, maximum_bytes: int, errors: list[str]
) -> re.Pattern[str] | None:
    if not isinstance(raw, str) or not raw:
        errors.append(f"{label} must be a non-empty regular expression")
        return None
    try:
        compiled = re.compile(raw)
    except re.error as exc:
        errors.append(f"{label} is not a valid regular expression: {exc}")
        return None
    if maximum_bytes <= 0:
        errors.append(f"{label} maximum byte bound must be positive")
    return compiled


def validate_header(contract: dict[str, Any], errors: list[str]) -> None:
    require_exact_keys(contract, EXPECTED_TOP_LEVEL, "contract", errors)
    if contract.get("schema_version") != 1:
        errors.append("schema_version must be 1")
    if contract.get("contract_id") != "tracedecay.memory.provider.registry.v1":
        errors.append("contract_id must be tracedecay.memory.provider.registry.v1")
    if contract.get("bead_id") != "tdmem-0201":
        errors.append("bead_id must be tdmem-0201")
    if contract.get("status") != "accepted":
        errors.append("contract status must be accepted")
    if contract.get("authority") != "TraceDecay provider registry composition root":
        errors.append("registry authority must remain the TraceDecay composition root")
    if contract.get("scope") != "coding_agents_only":
        errors.append("provider registry scope must remain coding_agents_only")
    non_empty_string(contract, "title", "contract", errors)


def validate_provider_identity(contract: dict[str, Any], errors: list[str]) -> None:
    identity = require_object(contract.get("provider_identity"), "provider_identity", errors)
    expected_fields = {
        "type_name",
        "encoding",
        "canonical_pattern",
        "minimum_bytes",
        "maximum_bytes",
        "case_sensitive",
        "stable_across_restarts",
        "stable_across_adapter_upgrades",
        "display_name_is_not_identity",
        "forbidden_sources",
    }
    require_exact_keys(identity, expected_fields, "provider_identity", errors)
    if identity.get("type_name") != "MemoryProviderIdV1":
        errors.append("provider identity type must be MemoryProviderIdV1")
    if identity.get("encoding") != "utf-8":
        errors.append("provider identity encoding must be utf-8")
    if identity.get("minimum_bytes") != 1:
        errors.append("provider identity minimum_bytes must be 1")
    maximum = identity.get("maximum_bytes")
    if not isinstance(maximum, int) or not 32 <= maximum <= 128:
        errors.append("provider identity maximum_bytes must be between 32 and 128")
        maximum = 64
    for field in (
        "case_sensitive",
        "stable_across_restarts",
        "stable_across_adapter_upgrades",
        "display_name_is_not_identity",
    ):
        if identity.get(field) is not True:
            errors.append(f"provider_identity.{field} must be true")
    forbidden = require_list(
        identity.get("forbidden_sources"), "provider_identity.forbidden_sources", errors
    )
    required_forbidden = {
        "display_name",
        "process_id",
        "socket_path",
        "database_path",
        "provider_state_digest",
        "runtime_order",
        "configuration_position",
    }
    if set(forbidden) != required_forbidden:
        errors.append("provider identity forbidden_sources must reject every unstable source")

    pattern = compile_declared_pattern(
        identity.get("canonical_pattern"),
        "provider_identity.canonical_pattern",
        maximum,
        errors,
    )
    if pattern is not None:
        for accepted in ("tracedecay.native", "ncm", "ocean", "vendor-provider.v2"):
            if pattern.fullmatch(accepted) is None:
                errors.append(f"provider identity pattern rejects canonical example {accepted!r}")
        for rejected in ("Native", " ncm", "ncm/worker", "ncm..v1", "", "_ncm"):
            if pattern.fullmatch(rejected) is not None:
                errors.append(f"provider identity pattern accepts non-canonical example {rejected!r}")


def validate_capabilities(
    contract: dict[str, Any], issue_ids: set[str], errors: list[str]
) -> None:
    identity = require_object(
        contract.get("capability_identity"), "capability_identity", errors
    )
    expected_identity_fields = {
        "type_name",
        "canonical_pattern",
        "maximum_bytes",
        "version_is_part_of_identity",
        "unknown_capability_policy",
        "duplicate_capability_policy",
    }
    require_exact_keys(
        identity, expected_identity_fields, "capability_identity", errors
    )
    if identity.get("type_name") != "MemoryProviderCapabilityIdV1":
        errors.append("capability identity type must be MemoryProviderCapabilityIdV1")
    maximum = identity.get("maximum_bytes")
    if not isinstance(maximum, int) or not 32 <= maximum <= 192:
        errors.append("capability identity maximum_bytes must be between 32 and 192")
        maximum = 96
    if identity.get("version_is_part_of_identity") is not True:
        errors.append("capability version must be part of identity")
    if identity.get("unknown_capability_policy") != "reject":
        errors.append("unknown capability policy must reject")
    if identity.get("duplicate_capability_policy") != "reject":
        errors.append("duplicate capability policy must reject")
    pattern = compile_declared_pattern(
        identity.get("canonical_pattern"),
        "capability_identity.canonical_pattern",
        maximum,
        errors,
    )

    rows = require_list(contract.get("capability_catalog"), "capability_catalog", errors)
    catalog = index_unique(rows, "id", "capability_catalog", errors)
    if set(catalog) != set(EXPECTED_CAPABILITIES):
        errors.append("capability catalog must exactly contain the nine V1 behavior identities")
    expected_fields = {
        "id",
        "class",
        "advisory_only",
        "detailed_contract_bead",
        "may_mutate_tracedecay_authority",
    }
    for capability_id, row in catalog.items():
        require_exact_keys(row, expected_fields, f"capability[{capability_id}]", errors)
        if pattern is not None and pattern.fullmatch(capability_id) is None:
            errors.append(f"capability ID is non-canonical: {capability_id}")
        expected = EXPECTED_CAPABILITIES.get(capability_id)
        if expected is not None:
            expected_class, expected_bead = expected
            if row.get("class") != expected_class:
                errors.append(f"capability {capability_id}.class must be {expected_class}")
            if row.get("detailed_contract_bead") != expected_bead:
                errors.append(
                    f"capability {capability_id}.detailed_contract_bead must be {expected_bead}"
                )
        if row.get("advisory_only") is not True:
            errors.append(f"capability {capability_id} must remain advisory_only")
        if row.get("may_mutate_tracedecay_authority") is not False:
            errors.append(
                f"capability {capability_id} must not mutate TraceDecay authority"
            )
        validate_bead(
            row.get("detailed_contract_bead"),
            f"capability[{capability_id}].detailed_contract_bead",
            issue_ids,
            errors,
        )


def validate_registration(contract: dict[str, Any], errors: list[str]) -> None:
    registration = require_object(
        contract.get("registration_contract"), "registration_contract", errors
    )
    expected_fields = {
        "type_name",
        "registry_writer",
        "provider_self_registration",
        "required_fields",
        "registration_states",
        "capability_declaration_states",
        "immutable_after_registration",
        "revision_rule",
        "duplicate_provider_id_policy",
        "unknown_provider_policy",
        "adapter_construction_boundary",
        "public_surface_provider_branching",
    }
    require_exact_keys(registration, expected_fields, "registration_contract", errors)
    if registration.get("type_name") != "MemoryProviderRegistrationV1":
        errors.append("registration type must be MemoryProviderRegistrationV1")
    if registration.get("registry_writer") != "TraceDecay provider registry composition root":
        errors.append("registration writer must remain the TraceDecay composition root")
    if registration.get("provider_self_registration") is not False:
        errors.append("provider self-registration must be false")
    if set(require_list(registration.get("required_fields"), "registration required_fields", errors)) != EXPECTED_REGISTRATION_FIELDS:
        errors.append("registration required_fields drifted")
    if registration.get("registration_states") != EXPECTED_REGISTRATION_STATES:
        errors.append("registration states must remain canonical and ordered")
    if registration.get("capability_declaration_states") != EXPECTED_DECLARATION_STATES:
        errors.append("capability declaration states must remain canonical and ordered")
    if registration.get("immutable_after_registration") != [
        "provider_id",
        "implementation_identity",
    ]:
        errors.append("provider_id and implementation_identity must be immutable")
    if registration.get("duplicate_provider_id_policy") != "reject_ambiguous_registration":
        errors.append("duplicate provider IDs must reject ambiguous registration")
    if registration.get("unknown_provider_policy") != "reject_unknown_provider":
        errors.append("unknown provider IDs must be rejected")
    if registration.get("public_surface_provider_branching") is not False:
        errors.append("public-surface provider branching must be false")
    boundary = non_empty_string(
        registration, "adapter_construction_boundary", "registration_contract", errors
    ).casefold()
    if "only the provider registry/composition layer" not in boundary:
        errors.append("adapter construction must be confined to registry/composition")
    revision = non_empty_string(
        registration, "revision_rule", "registration_contract", errors
    ).casefold()
    if "increments registration_revision exactly once" not in revision:
        errors.append("registration revision rule must increment exactly once")


def validate_selection(contract: dict[str, Any], errors: list[str]) -> None:
    selection = require_object(
        contract.get("selection_contract"), "selection_contract", errors
    )
    expected_fields = {
        "type_name",
        "required_request_fields",
        "required_capability_semantics",
        "maximum_required_capabilities",
        "duplicate_required_capability_policy",
        "active_provider_cardinality_per_exact_scope",
        "resolution_states",
        "resolved_requires",
        "silent_fallback",
        "fallback_provider",
        "successful_empty_resolution",
    }
    require_exact_keys(selection, expected_fields, "selection_contract", errors)
    if selection.get("type_name") != "MemoryProviderSelectionRequestV1":
        errors.append("selection type must be MemoryProviderSelectionRequestV1")
    if set(require_list(selection.get("required_request_fields"), "selection required_request_fields", errors)) != EXPECTED_SELECTION_FIELDS:
        errors.append("selection request must carry provider, capabilities, exact scope, revision, identity, deadline, and cancellation")
    if selection.get("required_capability_semantics") != "all":
        errors.append("selection must require all requested capabilities")
    maximum = selection.get("maximum_required_capabilities")
    if not isinstance(maximum, int) or not 1 <= maximum <= 32:
        errors.append("maximum_required_capabilities must be between 1 and 32")
    if selection.get("duplicate_required_capability_policy") != "reject_non_canonical_request":
        errors.append("duplicate requested capabilities must be rejected")
    if selection.get("active_provider_cardinality_per_exact_scope") != "zero_or_one":
        errors.append("active provider cardinality must be zero_or_one per exact scope")
    if set(require_list(selection.get("resolution_states"), "selection resolution_states", errors)) != EXPECTED_RESOLUTION_STATES:
        errors.append("selection resolution states must exactly cover the V1 fail-closed outcomes")
    if set(require_list(selection.get("resolved_requires"), "selection resolved_requires", errors)) != EXPECTED_RESOLVED_REQUIRES:
        errors.append("resolved provider requirements drifted")
    if selection.get("silent_fallback") is not False:
        errors.append("silent provider fallback must be false")
    if selection.get("fallback_provider") is not None:
        errors.append("fallback_provider must be null")
    if selection.get("successful_empty_resolution") is not False:
        errors.append("successful empty provider resolution must be false")


def validate_bootstrap_slots(
    contract: dict[str, Any], issue_ids: set[str], errors: list[str]
) -> None:
    slots = index_unique(
        require_list(contract.get("bootstrap_slots"), "bootstrap_slots", errors),
        "provider_id",
        "bootstrap_slots",
        errors,
    )
    if set(slots) != set(EXPECTED_BOOTSTRAP):
        errors.append("bootstrap slots must exactly reserve tracedecay.native, ncm, and ocean")
    for provider_id, expected in EXPECTED_BOOTSTRAP.items():
        row = slots.get(provider_id, {})
        require_exact_keys(row, EXPECTED_SLOT_FIELDS, f"bootstrap_slot[{provider_id}]", errors)
        non_empty_string(row, "display_name", f"bootstrap_slot[{provider_id}]", errors)
        for field in (
            "slot_state",
            "specification_state",
            "capability_declaration_state",
        ):
            if row.get(field) != expected[field]:
                errors.append(
                    f"bootstrap slot {provider_id}.{field} must be {expected[field]}"
                )
        gates = require_list(
            row.get("implementation_gate_beads"),
            f"bootstrap_slot[{provider_id}].implementation_gate_beads",
            errors,
        )
        if gates != expected["implementation_gate_beads"]:
            errors.append(f"bootstrap slot {provider_id} implementation gates drifted")
        for bead in gates:
            validate_bead(
                bead,
                f"bootstrap_slot[{provider_id}].implementation_gate_beads",
                issue_ids,
                errors,
            )
        if row.get("counts_as_implemented") is not False:
            errors.append(f"bootstrap slot {provider_id} must not count as implemented")

    ncm = slots.get("ncm", {})
    for forbidden_field in (
        "execution_topology",
        "transport",
        "process_model",
        "selected_adapter",
        "declared_capabilities",
    ):
        if forbidden_field in ncm:
            errors.append(f"NCM bootstrap slot must not preselect {forbidden_field}")
    ocean = slots.get("ocean", {})
    if ocean.get("implementation_gate_beads") != []:
        errors.append("OCEAN bootstrap slot must have no speculative implementation gates")


def validate_invariants_and_beads(
    contract: dict[str, Any], issue_ids: set[str], errors: list[str]
) -> None:
    invariants = require_list(contract.get("invariants"), "invariants", errors)
    if len(invariants) < 10:
        errors.append("provider registry contract must state at least ten invariants")
    serialized = " ".join(str(value) for value in invariants).casefold()
    for phrase in REQUIRED_INVARIANT_PHRASES:
        if phrase.casefold() not in serialized:
            errors.append(f"provider registry invariants are missing {phrase!r}")
    if len(set(invariants)) != len(invariants):
        errors.append("provider registry invariants must be unique")

    beads = require_list(contract.get("verification_beads"), "verification_beads", errors)
    if len(beads) < 8 or len(set(beads)) != len(beads):
        errors.append("verification_beads must contain at least eight unique issues")
    for bead in beads:
        validate_bead(bead, "verification_beads", issue_ids, errors)
    for required in (
        "tdmem-0202",
        "tdmem-0203",
        "tdmem-0204",
        "tdmem-0205",
        "tdmem-0206",
        "tdmem-0209",
        "tdmem-0303",
        "tdmem-0306",
    ):
        if required not in beads:
            errors.append(f"verification_beads is missing required follow-on {required}")


def validate_schema(schema: dict[str, Any], errors: list[str]) -> None:
    if schema.get("$schema") != "https://json-schema.org/draft/2020-12/schema":
        errors.append("provider registry schema must use JSON Schema 2020-12")
    if schema.get("type") != "object":
        errors.append("provider registry schema root must be object")
    if schema.get("additionalProperties") is not False:
        errors.append("provider registry schema root must deny additional properties")
    required = schema.get("required")
    if not isinstance(required, list) or set(required) != EXPECTED_TOP_LEVEL:
        errors.append("provider registry schema required fields must match the contract")
    properties = require_object(schema.get("properties"), "schema.properties", errors)
    for field in EXPECTED_TOP_LEVEL:
        if field not in properties:
            errors.append(f"provider registry schema is missing property {field}")
    if properties.get("schema_version", {}).get("const") != 1:
        errors.append("provider registry schema must pin schema_version 1")
    if properties.get("contract_id", {}).get("const") != "tracedecay.memory.provider.registry.v1":
        errors.append("provider registry schema must pin contract_id")
    if properties.get("bead_id", {}).get("const") != "tdmem-0201":
        errors.append("provider registry schema must pin bead_id tdmem-0201")
    definitions = require_object(schema.get("$defs"), "schema.$defs", errors)
    for name in ("beadId", "providerId", "capabilityId", "capability", "bootstrapSlot"):
        if name not in definitions:
            errors.append(f"provider registry schema is missing $defs.{name}")
    for object_property in (
        "provider_identity",
        "capability_identity",
        "registration_contract",
        "selection_contract",
    ):
        if properties.get(object_property, {}).get("additionalProperties") is not False:
            errors.append(f"schema property {object_property} must deny additional properties")
    for definition in ("capability", "bootstrapSlot"):
        if definitions.get(definition, {}).get("additionalProperties") is not False:
            errors.append(f"schema definition {definition} must deny additional properties")


def validate_readme(path: Path, errors: list[str]) -> None:
    try:
        text = path.read_text(encoding="utf-8")
    except OSError as exc:
        errors.append(f"could not load provider contract README: {exc}")
        return
    for phrase in REQUIRED_README_PHRASES:
        if phrase.casefold() not in text.casefold():
            errors.append(f"provider contract README is missing {phrase!r}")
    if "TBD" in text or "TODO" in text:
        errors.append("provider contract README contains unresolved TBD/TODO text")


def validate_architecture_dependencies(repo: Path, errors: list[str]) -> None:
    go = load_object(repo / "product/architecture/m0-go-no-go.json", "M0 GO decision", errors)
    if go.get("verdict") != "go" or go.get("next_executable_bead") != "tdmem-0201":
        errors.append("tdmem-0201 requires the accepted M0 GO decision and next-bead lock")

    manifest = load_object(
        repo / "product/architecture/adr/manifest.json",
        "foundational ADR manifest",
        errors,
    )
    if manifest.get("status") != "accepted":
        errors.append("provider registry contract requires accepted foundational ADRs")
    decisions = {
        row.get("id"): row
        for row in manifest.get("decisions", [])
        if isinstance(row, dict)
    }
    adr1 = decisions.get("ADR-0001", {})
    adr4 = decisions.get("ADR-0004", {})
    if "capability" not in str(adr1.get("decision_summary", "")).casefold():
        errors.append("ADR-0001 must retain the capability-based provider boundary")
    topology = adr4.get("ncm_topology")
    if not isinstance(topology, dict) or topology.get("state") != "deferred":
        errors.append("ADR-0004 must keep NCM topology deferred")


def validate_document(
    repo: Path,
    contract: dict[str, Any],
    schema: dict[str, Any],
    readme_path: Path,
    issue_ids: set[str],
) -> list[str]:
    errors: list[str] = []
    validate_header(contract, errors)
    validate_provider_identity(contract, errors)
    validate_capabilities(contract, issue_ids, errors)
    validate_registration(contract, errors)
    validate_selection(contract, errors)
    validate_bootstrap_slots(contract, issue_ids, errors)
    validate_invariants_and_beads(contract, issue_ids, errors)
    validate_schema(schema, errors)
    validate_readme(readme_path, errors)
    validate_architecture_dependencies(repo, errors)
    return errors


def main() -> int:
    args = parse_args()
    repo = args.repo.resolve()
    contract_path = resolve(repo, args.contract)
    schema_path = resolve(repo, args.schema)
    readme_path = resolve(repo, args.readme)
    issues_path = resolve(repo, args.issues)
    bootstrap_errors: list[str] = []
    contract = load_object(contract_path, "provider registry contract", bootstrap_errors)
    schema = load_object(schema_path, "provider registry schema", bootstrap_errors)
    issue_ids = load_issue_ids(issues_path, bootstrap_errors)
    if bootstrap_errors:
        print(json.dumps({"ok": False, "errors": bootstrap_errors}, indent=2, sort_keys=True))
        return 1

    errors = validate_document(repo, contract, schema, readme_path, issue_ids)
    if errors:
        print(json.dumps({"ok": False, "errors": errors}, indent=2, sort_keys=True))
        return 1

    receipt = {
        "ok": True,
        "schema_version": contract["schema_version"],
        "contract_id": contract["contract_id"],
        "bead_id": contract["bead_id"],
        "status": contract["status"],
        "capability_count": len(contract["capability_catalog"]),
        "bootstrap_provider_ids": sorted(
            slot["provider_id"] for slot in contract["bootstrap_slots"]
        ),
        "resolution_state_count": len(
            contract["selection_contract"]["resolution_states"]
        ),
        "silent_fallback": contract["selection_contract"]["silent_fallback"],
        "ncm_topology": "deferred",
        "ocean_counts_as_implemented": next(
            slot["counts_as_implemented"]
            for slot in contract["bootstrap_slots"]
            if slot["provider_id"] == "ocean"
        ),
    }
    print(json.dumps(receipt, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    sys.exit(main())
