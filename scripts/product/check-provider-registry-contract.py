#!/usr/bin/env python3
"""Validate the V1 provider identity and mandatory/optional capability registry."""
from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path
from typing import Any

TOP_LEVEL = {
    'schema_version', 'contract_id', 'bead_id', 'title', 'status', 'authority',
    'scope', 'provider_identity', 'capability_identity', 'capability_registry',
    'capability_catalog', 'unknown_capability_contract', 'registration_contract', 'selection_contract',
    'bootstrap_slots', 'invariants', 'verification_beads',
}
MANDATORY = {
    'provider.health.v1': 'tdmem-0205',
    'observation.accept.v1': 'tdmem-0203',
    'recall.query.v1': 'tdmem-0204',
}
OPTIONAL = {
    'feedback.record.v1': 'tdmem-0205',
    'maintenance.run.v1': 'tdmem-0205',
    'recall.temporal.v1': 'tdmem-0204',
    'recall.associative.v1': 'tdmem-0204',
    'facts.explicit.v1': 'tdmem-0205',
    'explain.trace.v1': 'tdmem-0205',
    'correction.apply.v1': 'tdmem-0205',
    'deletion.by_source.v1': 'tdmem-0205',
    'snapshot.export.v1': 'tdmem-0205',
    'snapshot.restore.v1': 'tdmem-0205',
    'replay.apply.v1': 'tdmem-0205',
    'inspection.read.v1': 'tdmem-0205',
}
CAPABILITY_FIELDS = {
    'id', 'requirement', 'purpose', 'operation_class', 'inputs', 'outputs',
    'failure_modes', 'compatibility_rules', 'detailed_contract_bead',
    'advisory_only', 'may_mutate_tracedecay_authority',
}
IO_FIELDS = {'name', 'contract_id', 'required', 'description'}
COMPATIBILITY_FIELDS = {
    'capability_major', 'major_version_rule', 'minor_extension_rule',
    'unknown_extension_rule', 'activation_rule', 'downgrade_rule',
}
STANDARD_FAILURES = {
    'invalid_request', 'scope_unavailable', 'capability_unsupported',
    'deadline_exceeded', 'cancelled', 'provider_unavailable',
    'contract_violation',
}
PROVIDER_IDS = {'tracedecay.native', 'ncm', 'ocean'}
BEAD_RE = re.compile(r'^tdmem-[0-9]{4}$')
NAME_RE = re.compile(r'^[a-z][a-z0-9_]*$')
CONTRACT_ID_RE = re.compile(
    r'^tracedecay\.memory\.[a-z][a-z0-9]*(?:[._-][a-z0-9]+)*\.(?:request|outcome)\.v[1-9][0-9]*$'
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument('--repo', type=Path, default=Path('.'))
    parser.add_argument('--contract', type=Path, default=Path('product/contracts/memory-provider-v1/provider-registry-contract.json'))
    parser.add_argument('--schema', type=Path, default=Path('product/contracts/memory-provider-v1/provider-registry-contract.schema.json'))
    parser.add_argument('--readme', type=Path, default=Path('product/contracts/memory-provider-v1/README.md'))
    parser.add_argument('--issues', type=Path, default=Path('.beads/issues.jsonl'))
    return parser.parse_args()


def resolve(repo: Path, path: Path) -> Path:
    return path if path.is_absolute() else repo / path


def load_object(path: Path, label: str, errors: list[str]) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding='utf-8'))
    except (OSError, json.JSONDecodeError) as exc:
        errors.append(f'could not load {label}: {exc}')
        return {}
    if not isinstance(value, dict):
        errors.append(f'{label} root must be an object')
        return {}
    return value


def load_issue_ids(path: Path, errors: list[str]) -> set[str]:
    ids: set[str] = set()
    try:
        lines = path.read_text(encoding='utf-8').splitlines()
    except OSError as exc:
        errors.append(f'could not load Beads authority: {exc}')
        return ids
    for number, line in enumerate(lines, 1):
        if not line.strip():
            continue
        try:
            row = json.loads(line)
        except json.JSONDecodeError as exc:
            errors.append(f'invalid Beads JSONL line {number}: {exc}')
            continue
        issue_id = row.get('id') if isinstance(row, dict) else None
        if not isinstance(issue_id, str):
            errors.append(f'Beads line {number} lacks string id')
        elif issue_id in ids:
            errors.append(f'duplicate Beads issue id {issue_id}')
        else:
            ids.add(issue_id)
    return ids


def exact_keys(row: Any, expected: set[str], label: str, errors: list[str]) -> dict[str, Any]:
    if not isinstance(row, dict):
        errors.append(f'{label} must be an object')
        return {}
    actual = set(row)
    if actual != expected:
        errors.append(f'{label} fields drifted; missing={sorted(expected-actual)}, extra={sorted(actual-expected)}')
    return row


def array(value: Any, label: str, errors: list[str]) -> list[Any]:
    if not isinstance(value, list):
        errors.append(f'{label} must be an array')
        return []
    return value


def nonempty(value: Any, label: str, errors: list[str], minimum: int = 1) -> str:
    if not isinstance(value, str) or len(value.strip()) < minimum:
        errors.append(f'{label} must be a non-empty string of at least {minimum} characters')
        return ''
    return value.strip()


def require_bead(value: Any, label: str, issue_ids: set[str], errors: list[str]) -> None:
    if not isinstance(value, str) or not BEAD_RE.fullmatch(value):
        errors.append(f'{label} must match tdmem-NNNN')
    elif value not in issue_ids:
        errors.append(f'{label} references unknown Beads issue {value}')


def validate_header(contract: dict[str, Any], errors: list[str]) -> None:
    exact_keys(contract, TOP_LEVEL, 'contract', errors)
    expected = {
        'schema_version': 1,
        'contract_id': 'tracedecay.memory.provider.registry.v1',
        'bead_id': 'tdmem-0201',
        'status': 'accepted',
        'authority': 'TraceDecay provider registry composition root',
        'scope': 'coding_agents_only',
    }
    for field, value in expected.items():
        if contract.get(field) != value:
            errors.append(f'{field} must be {value!r}')
    nonempty(contract.get('title'), 'title', errors, 10)


def validate_provider_identity(contract: dict[str, Any], errors: list[str]) -> re.Pattern[str] | None:
    expected_fields = {
        'type_name', 'encoding', 'canonical_pattern', 'minimum_bytes', 'maximum_bytes',
        'case_sensitive', 'stable_across_restarts', 'stable_across_adapter_upgrades',
        'display_name_is_not_identity', 'forbidden_sources',
    }
    row = exact_keys(contract.get('provider_identity'), expected_fields, 'provider_identity', errors)
    if row.get('type_name') != 'MemoryProviderIdV1' or row.get('encoding') != 'utf-8':
        errors.append('provider identity type/encoding drifted')
    if row.get('minimum_bytes') != 1 or row.get('maximum_bytes') != 64:
        errors.append('provider identity bounds must remain 1..64 bytes')
    for field in ('case_sensitive', 'stable_across_restarts', 'stable_across_adapter_upgrades', 'display_name_is_not_identity'):
        if row.get(field) is not True:
            errors.append(f'provider_identity.{field} must be true')
    required_forbidden = {
        'display_name', 'process_id', 'socket_path', 'database_path',
        'provider_state_digest', 'runtime_order', 'configuration_position',
    }
    forbidden = array(row.get('forbidden_sources'), 'provider_identity.forbidden_sources', errors)
    if set(forbidden) != required_forbidden or len(forbidden) != len(required_forbidden):
        errors.append('provider identity must reject every unstable identity source exactly once')
    try:
        pattern = re.compile(row.get('canonical_pattern', ''))
    except re.error as exc:
        errors.append(f'provider identity pattern invalid: {exc}')
        return None
    for value in ('tracedecay.native', 'ncm', 'ocean', 'vendor-provider.v2'):
        if pattern.fullmatch(value) is None:
            errors.append(f'provider identity pattern rejects {value}')
    for value in ('Native', ' ncm', 'ncm/worker', 'ncm..v1', '_ncm', ''):
        if pattern.fullmatch(value) is not None:
            errors.append(f'provider identity pattern accepts non-canonical {value!r}')
    return pattern


def validate_capability_identity(contract: dict[str, Any], errors: list[str]) -> re.Pattern[str] | None:
    expected_fields = {
        'type_name', 'canonical_pattern', 'maximum_bytes', 'version_is_part_of_identity',
        'provider_name_is_not_capability_identity', 'duplicate_capability_policy',
        'unknown_declaration_policy', 'unknown_selection_policy',
    }
    row = exact_keys(contract.get('capability_identity'), expected_fields, 'capability_identity', errors)
    if row.get('type_name') != 'MemoryProviderCapabilityIdV1':
        errors.append('capability identity type drifted')
    if row.get('maximum_bytes') != 96:
        errors.append('capability identity maximum_bytes must be 96')
    if row.get('version_is_part_of_identity') is not True or row.get('provider_name_is_not_capability_identity') is not True:
        errors.append('capability identity must be versioned and provider-name independent')
    if row.get('duplicate_capability_policy') != 'reject_non_canonical_declaration':
        errors.append('duplicate capabilities must reject non-canonical declaration')
    if row.get('unknown_declaration_policy') != 'preserve_opaque_inert':
        errors.append('unknown capability declarations must preserve opaque inert payloads')
    if row.get('unknown_selection_policy') != 'typed_capability_unsupported':
        errors.append('unknown capability selection must return typed unsupported')
    try:
        pattern = re.compile(row.get('canonical_pattern', ''))
    except re.error as exc:
        errors.append(f'capability identity pattern invalid: {exc}')
        return None
    for value in ('provider.health.v1', 'recall.temporal.v2', 'vendor.experimental.v9'):
        if pattern.fullmatch(value) is None:
            errors.append(f'capability identity pattern rejects {value}')
    for value in ('recall.query', 'Recall.query.v1', 'ncm/recall.v1', '.v1', 'x.v0'):
        if pattern.fullmatch(value) is not None:
            errors.append(f'capability identity pattern accepts non-canonical {value!r}')
    return pattern


def validate_io(rows: Any, label: str, errors: list[str]) -> None:
    values = array(rows, label, errors)
    if not values:
        errors.append(f'{label} must define at least one field')
        return
    names: set[str] = set()
    contracts: set[str] = set()
    for index, raw in enumerate(values):
        row = exact_keys(raw, IO_FIELDS, f'{label}[{index}]', errors)
        name = nonempty(row.get('name'), f'{label}[{index}].name', errors)
        contract_id = nonempty(row.get('contract_id'), f'{label}[{index}].contract_id', errors)
        nonempty(row.get('description'), f'{label}[{index}].description', errors, 20)
        if not NAME_RE.fullmatch(name):
            errors.append(f'{label}[{index}].name is not canonical snake_case')
        if not CONTRACT_ID_RE.fullmatch(contract_id):
            errors.append(f'{label}[{index}].contract_id is not a versioned request/outcome identity')
        if row.get('required') is not True:
            errors.append(f'{label}[{index}].required must be true in V1')
        if name in names or contract_id in contracts:
            errors.append(f'{label} contains duplicate name or contract identity')
        names.add(name); contracts.add(contract_id)


def validate_capability(
    raw: Any,
    requirement: str,
    index: int,
    expected_bead: str,
    capability_pattern: re.Pattern[str] | None,
    issue_ids: set[str],
    errors: list[str],
) -> str:
    label = f'capability_registry.{requirement}[{index}]'
    row = exact_keys(raw, CAPABILITY_FIELDS, label, errors)
    cid = nonempty(row.get('id'), f'{label}.id', errors)
    if capability_pattern is not None and capability_pattern.fullmatch(cid) is None:
        errors.append(f'{label}.id is not canonical versioned capability identity')
    if row.get('requirement') != requirement:
        errors.append(f'{label}.requirement must be {requirement}')
    if any(cid == provider or cid.startswith(provider + '.') for provider in PROVIDER_IDS):
        errors.append(f'{label}.id branches on a concrete provider name')
    nonempty(row.get('purpose'), f'{label}.purpose', errors, 30)
    if row.get('operation_class') not in {'advisory_read', 'provider_local_read', 'provider_local_mutation'}:
        errors.append(f'{label}.operation_class is unsupported')
    validate_io(row.get('inputs'), f'{label}.inputs', errors)
    validate_io(row.get('outputs'), f'{label}.outputs', errors)
    failures = array(row.get('failure_modes'), f'{label}.failure_modes', errors)
    if len(failures) != len(set(failures)):
        errors.append(f'{label}.failure_modes contains duplicates')
    if not STANDARD_FAILURES.issubset(set(failures)):
        errors.append(f'{label}.failure_modes must include all common typed terminal modes')
    for failure in failures:
        if not isinstance(failure, str) or not NAME_RE.fullmatch(failure):
            errors.append(f'{label}.failure_modes contains non-canonical value {failure!r}')
    compatibility = exact_keys(row.get('compatibility_rules'), COMPATIBILITY_FIELDS, f'{label}.compatibility_rules', errors)
    expected_compatibility = {
        'capability_major': 1,
        'major_version_rule': 'exact_match',
        'minor_extension_rule': 'preserve_unknown_optional_fields',
        'unknown_extension_rule': 'preserve_opaque_or_reject_explicitly',
        'activation_rule': 'known_catalog_entry_and_explicit_registration_revision_and_explicit_selection',
        'downgrade_rule': 'no_implicit_downgrade',
    }
    for field, value in expected_compatibility.items():
        if compatibility.get(field) != value:
            errors.append(f'{label}.compatibility_rules.{field} must be {value!r}')
    if row.get('detailed_contract_bead') != expected_bead:
        errors.append(f'{label}.detailed_contract_bead must be {expected_bead}')
    require_bead(row.get('detailed_contract_bead'), f'{label}.detailed_contract_bead', issue_ids, errors)
    if row.get('advisory_only') is not True or row.get('may_mutate_tracedecay_authority') is not False:
        errors.append(f'{label} must remain advisory and unable to mutate TraceDecay authority')
    return cid


def validate_capability_registry(
    contract: dict[str, Any], capability_pattern: re.Pattern[str] | None,
    issue_ids: set[str], errors: list[str]
) -> None:
    registry = exact_keys(contract.get('capability_registry'), {'mandatory', 'optional'}, 'capability_registry', errors)
    mandatory_rows = array(registry.get('mandatory'), 'capability_registry.mandatory', errors)
    optional_rows = array(registry.get('optional'), 'capability_registry.optional', errors)
    mandatory_ids: list[str] = []
    optional_ids: list[str] = []
    for index, raw in enumerate(mandatory_rows):
        cid = raw.get('id') if isinstance(raw, dict) else ''
        mandatory_ids.append(validate_capability(raw, 'mandatory', index, MANDATORY.get(cid, ''), capability_pattern, issue_ids, errors))
    for index, raw in enumerate(optional_rows):
        cid = raw.get('id') if isinstance(raw, dict) else ''
        optional_ids.append(validate_capability(raw, 'optional', index, OPTIONAL.get(cid, ''), capability_pattern, issue_ids, errors))
    if set(mandatory_ids) != set(MANDATORY) or len(mandatory_ids) != len(MANDATORY):
        errors.append(f'mandatory capability set drifted; expected={sorted(MANDATORY)}, actual={sorted(mandatory_ids)}')
    if set(optional_ids) != set(OPTIONAL) or len(optional_ids) != len(OPTIONAL):
        errors.append(f'optional capability set drifted; expected={sorted(OPTIONAL)}, actual={sorted(optional_ids)}')
    overlap = set(mandatory_ids) & set(optional_ids)
    if overlap:
        errors.append(f'mandatory and optional capability sets overlap: {sorted(overlap)}')
    all_ids = mandatory_ids + optional_ids
    if len(all_ids) != len(set(all_ids)):
        errors.append('capability registry contains duplicate identities')

    catalog_rows = array(contract.get('capability_catalog'), 'capability_catalog', errors)
    catalog_ids: list[str] = []
    for index, raw in enumerate(catalog_rows):
        row = exact_keys(raw, {'id'}, f'capability_catalog[{index}]', errors)
        capability_id = row.get('id')
        if not isinstance(capability_id, str):
            errors.append(f'capability_catalog[{index}].id must be a string')
        else:
            catalog_ids.append(capability_id)
    if len(catalog_ids) != len(set(catalog_ids)):
        errors.append('capability_catalog compatibility projection contains duplicates')
    if set(catalog_ids) != set(all_ids):
        errors.append('capability_catalog must exactly project every authoritative mandatory and optional capability')


def validate_unknown_capabilities(contract: dict[str, Any], capability_pattern: re.Pattern[str] | None, errors: list[str]) -> None:
    expected_fields = {
        'type_name', 'accepted_identity_requirement', 'wire_fields', 'decode_policy',
        'encode_policy', 'registration_policy', 'activation_policy', 'selection_policy',
        'may_count_as_mandatory', 'may_satisfy_required_capability',
        'may_infer_behavior_from_name', 'may_activate_from_presence',
    }
    row = exact_keys(contract.get('unknown_capability_contract'), expected_fields, 'unknown_capability_contract', errors)
    expected = {
        'type_name': 'OpaqueMemoryProviderCapabilityV1',
        'accepted_identity_requirement': 'syntactically_valid_versioned_capability_id',
        'wire_fields': ['id', 'canonical_payload'],
        'decode_policy': 'preserve_canonical_payload_opaque',
        'encode_policy': 'round_trip_canonical_payload_without_semantic_rewrite',
        'registration_policy': 'retain_as_opaque_not_supported',
        'activation_policy': 'inert_until_catalog_acceptance_and_new_registration_revision_and_explicit_selection',
        'selection_policy': 'return_typed_capability_unsupported',
        'may_count_as_mandatory': False,
        'may_satisfy_required_capability': False,
        'may_infer_behavior_from_name': False,
        'may_activate_from_presence': False,
    }
    for field, value in expected.items():
        if row.get(field) != value:
            errors.append(f'unknown_capability_contract.{field} must be {value!r}')
    sample = {
        'id': 'vendor.experimental.v9',
        'canonical_payload': {'alpha': [1, 2], 'future_flag': True, 'nested': {'x': 'y'}},
    }
    if capability_pattern is not None and capability_pattern.fullmatch(sample['id']) is None:
        errors.append('unknown capability sample must be syntactically valid')
    encoded = json.dumps(sample, sort_keys=True, separators=(',', ':'), ensure_ascii=False)
    decoded = json.loads(encoded)
    reencoded = json.dumps(decoded, sort_keys=True, separators=(',', ':'), ensure_ascii=False)
    if encoded != reencoded:
        errors.append('unknown capability canonical payload does not round-trip')


def validate_registration(contract: dict[str, Any], errors: list[str]) -> None:
    expected_fields = {
        'type_name', 'registry_writer', 'provider_self_registration', 'required_fields',
        'registration_states', 'immutable_after_registration', 'revision_rule',
        'mandatory_capability_rule', 'unknown_capability_rule', 'duplicate_provider_id_policy',
        'unknown_provider_policy', 'adapter_construction_boundary', 'public_surface_provider_branching',
        'recall_scope_bindings',
    }
    row = exact_keys(contract.get('registration_contract'), expected_fields, 'registration_contract', errors)
    if row.get('type_name') != 'MemoryProviderRegistrationV1' or row.get('registry_writer') != contract.get('authority'):
        errors.append('registration type/writer drifted')
    if row.get('provider_self_registration') is not False or row.get('public_surface_provider_branching') is not False:
        errors.append('providers cannot self-register and public surfaces cannot branch by provider')
    required = {
        'provider_id', 'adapter_contract_version', 'registration_state', 'known_capabilities',
        'opaque_unknown_capabilities', 'implementation_identity', 'registration_revision',
        'recall_scope_bindings',
    }
    fields = array(row.get('required_fields'), 'registration_contract.required_fields', errors)
    if set(fields) != required or len(fields) != len(required):
        errors.append('registration required_fields drifted')
    if row.get('registration_states') != ['registered', 'disabled', 'reserved', 'retiring']:
        errors.append('registration states drifted')
    if row.get('immutable_after_registration') != ['provider_id', 'implementation_identity']:
        errors.append('registration immutable identity drifted')
    if row.get('mandatory_capability_rule') != 'registered providers must declare every mandatory capability before readiness':
        errors.append('mandatory registration capability rule drifted')
    if row.get('unknown_capability_rule') != 'opaque unknown declarations are preserved but never become supported or active':
        errors.append('unknown registration capability rule drifted')
    if row.get('duplicate_provider_id_policy') != 'reject_ambiguous_registration' or row.get('unknown_provider_policy') != 'reject_unknown_provider':
        errors.append('registration provider failure policies drifted')
    if 'Only the provider registry/composition layer may branch on provider_id' not in str(row.get('adapter_construction_boundary')):
        errors.append('adapter construction boundary must be registry-only')
    bindings = exact_keys(
        row.get('recall_scope_bindings'),
        {
            'declared_by', 'provider_may_self_declare', 'values', 'value_source',
            'provider_declarations', 'admission_input', 'unauthorized_binding_policy',
        },
        'registration_contract.recall_scope_bindings',
        errors,
    )
    scope_binding_values = ['exact_coding_scope', 'project_facts', 'profile_facts']
    if bindings.get('provider_may_self_declare') is not False:
        errors.append('providers cannot self-declare recall scope bindings')
    if bindings.get('values') != scope_binding_values:
        errors.append('recall scope binding values drifted from the recall contract')
    if bindings.get('admission_input') != 'recorded_registration_passed_with_the_admitted_call':
        errors.append('recall scope bindings must reach admission through the admitted call')
    if bindings.get('unauthorized_binding_policy') != 'reject_scope_binding_unauthorized':
        errors.append('unauthorized recall scope bindings must be rejected explicitly')
    declarations = bindings.get('provider_declarations')
    if not isinstance(declarations, dict) or not declarations:
        errors.append('recall scope binding provider declarations must be a non-empty object')
        declarations = {}
    for provider_id, declared in declarations.items():
        if not isinstance(declared, list) or not declared or len(set(declared)) != len(declared):
            errors.append(f'recall scope bindings for {provider_id} must be a unique non-empty list')
        elif any(value not in scope_binding_values for value in declared):
            errors.append(f'recall scope bindings for {provider_id} name an unknown binding')
    if declarations.get('tracedecay.native') != ['project_facts', 'profile_facts']:
        errors.append('tracedecay.native must be authorized for project_facts and profile_facts only')
    if declarations.get('ncm') != ['exact_coding_scope']:
        errors.append('ncm must be authorized for exact_coding_scope only')


def validate_selection(contract: dict[str, Any], errors: list[str]) -> None:
    expected_fields = {
        'type_name', 'required_request_fields', 'required_capability_semantics',
        'maximum_required_capabilities', 'duplicate_required_capability_policy',
        'unknown_required_capability_policy', 'active_provider_cardinality_per_exact_scope',
        'resolution_states', 'resolved_requires', 'silent_fallback', 'fallback_provider',
        'successful_empty_resolution',
    }
    row = exact_keys(contract.get('selection_contract'), expected_fields, 'selection_contract', errors)
    required_fields = {
        'provider_id', 'required_capabilities', 'exact_scope_identity', 'configuration_revision',
        'request_identity', 'deadline', 'cancellation',
    }
    actual_fields = array(row.get('required_request_fields'), 'selection_contract.required_request_fields', errors)
    if set(actual_fields) != required_fields or len(actual_fields) != len(required_fields):
        errors.append('selection required_request_fields drifted')
    if row.get('type_name') != 'MemoryProviderSelectionRequestV1':
        errors.append('selection type drifted')
    expected_scalar = {
        'required_capability_semantics': 'all',
        'maximum_required_capabilities': 32,
        'duplicate_required_capability_policy': 'reject_non_canonical_request',
        'unknown_required_capability_policy': 'return_typed_capability_unsupported',
        'active_provider_cardinality_per_exact_scope': 'zero_or_one',
        'silent_fallback': False,
        'fallback_provider': None,
        'successful_empty_resolution': False,
    }
    for field, value in expected_scalar.items():
        if row.get(field) != value:
            errors.append(f'selection_contract.{field} must be {value!r}')
    states = set(array(row.get('resolution_states'), 'selection_contract.resolution_states', errors))
    required_states = {
        'resolved', 'provider_unknown', 'provider_disabled', 'provider_reserved', 'provider_retiring',
        'adapter_unavailable', 'mandatory_capability_missing', 'capability_unsupported',
        'protocol_incompatible', 'scope_unavailable', 'configuration_revision_conflict',
        'deadline_exceeded', 'cancelled', 'ambiguous_registration',
    }
    if states != required_states:
        errors.append('selection resolution states drifted')
    resolved = set(array(row.get('resolved_requires'), 'selection_contract.resolved_requires', errors))
    required_resolved = {
        'exact_provider_id_match', 'accepted_registration_revision', 'compatible_adapter_contract_version',
        'all_mandatory_capabilities_declared', 'all_required_capabilities_known_and_declared',
        'exact_scope_admission', 'live_deadline', 'live_cancellation',
    }
    if resolved != required_resolved:
        errors.append('selection resolved requirements drifted')


def validate_bootstrap(contract: dict[str, Any], issue_ids: set[str], errors: list[str]) -> None:
    rows = array(contract.get('bootstrap_slots'), 'bootstrap_slots', errors)
    indexed: dict[str, dict[str, Any]] = {}
    fields = {'provider_id', 'display_name', 'slot_state', 'specification_state', 'capability_declaration_state', 'implementation_gate_beads', 'counts_as_implemented'}
    for index, raw in enumerate(rows):
        row = exact_keys(raw, fields, f'bootstrap_slots[{index}]', errors)
        pid = row.get('provider_id')
        if not isinstance(pid, str) or pid in indexed:
            errors.append(f'bootstrap_slots[{index}] has invalid or duplicate provider_id')
            continue
        indexed[pid] = row
        for offset, bead_id in enumerate(array(row.get('implementation_gate_beads'), f'bootstrap_slots[{index}].implementation_gate_beads', errors)):
            require_bead(bead_id, f'bootstrap_slots[{index}].implementation_gate_beads[{offset}]', issue_ids, errors)
        if row.get('counts_as_implemented') is not False:
            errors.append(f'bootstrap slot {pid} must not count as implemented')
    if set(indexed) != PROVIDER_IDS:
        errors.append('bootstrap slots must contain exactly tracedecay.native, ncm, and ocean')
        return
    native, ncm, ocean = indexed['tracedecay.native'], indexed['ncm'], indexed['ocean']
    if native.get('slot_state') != 'declared' or native.get('implementation_gate_beads') != ['tdmem-0401', 'tdmem-0402', 'tdmem-0403', 'tdmem-0404']:
        errors.append('Native bootstrap slot/parity gates drifted')
    if ncm.get('slot_state') != 'reserved' or ncm.get('implementation_gate_beads') != ['tdmem-0701', 'tdmem-0702', 'tdmem-0703']:
        errors.append('NCM must remain reserved with surface audit before topology')
    if ocean.get('slot_state') != 'reserved' or ocean.get('specification_state') != 'versioned_specification_unavailable' or ocean.get('implementation_gate_beads') != []:
        errors.append('OCEAN must remain an identity-only reservation without implementation gates')


def validate_invariants(contract: dict[str, Any], errors: list[str]) -> None:
    invariants = array(contract.get('invariants'), 'invariants', errors)
    if len(invariants) != len(set(invariants)) or len(invariants) < 12:
        errors.append('invariants must contain at least 12 unique entries')
    text = '\n'.join(str(item) for item in invariants)
    for phrase in (
        'Mandatory and optional capabilities are separate',
        'canonical inputs, outputs, typed failure modes, and explicit compatibility rules',
        'capability_catalog is a derived compatibility projection only',
        'round-trips as opaque canonical payload and remains inert',
        'never counts as support',
        'registry/composition boundary may branch',
        'never silently falls back',
        'NCM slot does not select an execution topology',
        'OCEAN slot reserves identity only',
    ):
        if phrase not in text:
            errors.append(f'invariants missing phrase {phrase!r}')


def validate_schema(schema: dict[str, Any], contract: dict[str, Any], errors: list[str]) -> None:
    if schema.get('$schema') != 'https://json-schema.org/draft/2020-12/schema':
        errors.append('schema must use draft 2020-12')
    if schema.get('type') != 'object' or schema.get('additionalProperties') is not False:
        errors.append('schema root must be strict object')
    if set(schema.get('required', [])) != TOP_LEVEL:
        errors.append('schema required top-level fields drifted')
    properties = schema.get('properties')
    if not isinstance(properties, dict) or set(properties) != TOP_LEVEL:
        errors.append('schema top-level properties drifted')
        return
    registry = properties.get('capability_registry', {})
    if registry.get('additionalProperties') is not False or set(registry.get('required', [])) != {'mandatory', 'optional'}:
        errors.append('schema must strictly separate mandatory and optional capability arrays')
    catalog = properties.get('capability_catalog', {})
    if catalog.get('minItems') != 15 or catalog.get('maxItems') != 15:
        errors.append('schema capability_catalog must remain the bounded 15-entry compatibility projection')
    catalog_item = catalog.get('items', {}) if isinstance(catalog, dict) else {}
    if catalog_item.get('additionalProperties') is not False or set(catalog_item.get('required', [])) != {'id'}:
        errors.append('schema capability_catalog item must contain only id')
    defs = schema.get('$defs')
    if not isinstance(defs, dict):
        errors.append('schema lacks $defs')
        return
    capability = defs.get('capability', {})
    if capability.get('additionalProperties') is not False or set(capability.get('required', [])) != CAPABILITY_FIELDS:
        errors.append('schema capability shape is not strict')
    unknown = defs.get('unknownCapability', {})
    if unknown.get('additionalProperties') is not False:
        errors.append('schema unknown capability shape must be strict')
    props = unknown.get('properties', {}) if isinstance(unknown, dict) else {}
    if props.get('may_activate_from_presence', {}).get('const') is not False:
        errors.append('schema must forbid activation from unknown capability presence')
    if props.get('encode_policy', {}).get('const') != 'round_trip_canonical_payload_without_semantic_rewrite':
        errors.append('schema must lock unknown capability round-trip policy')
    selection = defs.get('selection', {}).get('properties', {})
    if selection.get('silent_fallback', {}).get('const') is not False:
        errors.append('schema must forbid silent fallback')


def validate_readme(path: Path, errors: list[str]) -> None:
    try:
        text = path.read_text(encoding='utf-8')
    except OSError as exc:
        errors.append(f'could not read contract README: {exc}')
        return
    phrases = [
        'stable `MemoryProviderIdV1` identity',
        'Mandatory versus optional',
        '`provider.health.v1`',
        '`observation.accept.v1`',
        '`recall.query.v1`',
        'canonical input and output contract identities',
        'typed failure modes',
        'explicit compatibility rules',
        'Unknown capability round-trip',
        '`OpaqueMemoryProviderCapabilityV1`',
        'cannot activate anything',
        'typed `capability_unsupported`',
        'There is no implicit fallback',
        '`tracedecay.native`', '`ncm`', '`ocean`',
        'None of the bootstrap slots counts as implemented',
    ]
    for phrase in phrases:
        if phrase not in text:
            errors.append(f'README missing required phrase {phrase!r}')
    if 'TODO' in text or 'TBD' in text:
        errors.append('README contains unresolved TODO/TBD')


def validate_verification_beads(contract: dict[str, Any], issue_ids: set[str], errors: list[str]) -> None:
    rows = array(contract.get('verification_beads'), 'verification_beads', errors)
    if len(rows) != len(set(rows)) or len(rows) < 10:
        errors.append('verification_beads must contain at least ten unique IDs')
    for index, value in enumerate(rows):
        require_bead(value, f'verification_beads[{index}]', issue_ids, errors)
    for required in ('tdmem-0202', 'tdmem-0203', 'tdmem-0204', 'tdmem-0205', 'tdmem-0206', 'tdmem-0207', 'tdmem-0208', 'tdmem-0209'):
        if required not in rows:
            errors.append(f'verification_beads missing {required}')


def validate(repo: Path, contract_path: Path, schema_path: Path, readme_path: Path, issues_path: Path) -> tuple[list[str], dict[str, Any]]:
    errors: list[str] = []
    contract = load_object(contract_path, 'provider registry contract', errors)
    schema = load_object(schema_path, 'provider registry schema', errors)
    issue_ids = load_issue_ids(issues_path, errors)
    validate_header(contract, errors)
    validate_provider_identity(contract, errors)
    capability_pattern = validate_capability_identity(contract, errors)
    validate_capability_registry(contract, capability_pattern, issue_ids, errors)
    validate_unknown_capabilities(contract, capability_pattern, errors)
    validate_registration(contract, errors)
    validate_selection(contract, errors)
    validate_bootstrap(contract, issue_ids, errors)
    validate_invariants(contract, errors)
    validate_verification_beads(contract, issue_ids, errors)
    validate_schema(schema, contract, errors)
    validate_readme(readme_path, errors)
    registry = contract.get('capability_registry', {}) if isinstance(contract, dict) else {}
    summary = {
        'ok': not errors,
        'schema_version': contract.get('schema_version'),
        'bead_id': contract.get('bead_id'),
        'mandatory_capabilities': len(registry.get('mandatory', [])) if isinstance(registry, dict) else 0,
        'optional_capabilities': len(registry.get('optional', [])) if isinstance(registry, dict) else 0,
        'unknown_capability_policy': contract.get('unknown_capability_contract', {}).get('activation_policy') if isinstance(contract.get('unknown_capability_contract'), dict) else None,
        'bootstrap_slots': len(contract.get('bootstrap_slots', [])) if isinstance(contract.get('bootstrap_slots'), list) else 0,
    }
    return errors, summary


def main() -> int:
    args = parse_args()
    repo = args.repo.resolve()
    errors, summary = validate(
        repo,
        resolve(repo, args.contract),
        resolve(repo, args.schema),
        resolve(repo, args.readme),
        resolve(repo, args.issues),
    )
    if errors:
        for error in errors:
            print(f'ERROR: {error}', file=sys.stderr)
        return 1
    print(json.dumps(summary, indent=2, sort_keys=True))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
