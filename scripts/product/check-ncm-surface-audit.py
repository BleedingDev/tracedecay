#!/usr/bin/env python3
"""Validate the substantive NCM/Biomem provider-surface audit."""

from __future__ import annotations

import argparse
import json
import re
import shlex
import sys
from pathlib import Path
from typing import Any, Iterable


DEFAULT_AUDIT = Path(
    "crates/tracedecay-memory-provider-ncm/audits/tdmem-0701-capability-matrix.json"
)
DEFAULT_REGISTRY = Path(
    "product/contracts/memory-provider-v1/provider-registry-contract.json"
)
CLASSIFICATIONS = frozenset({"supported", "adaptable", "blocking", "unsupported"})
MANDATORY_OPERATIONS = {
    "provider.health.v1": "health",
    "observation.accept.v1": "observe",
    "recall.query.v1": "recall",
}
EVIDENCE_KINDS = frozenset({"source_symbol", "measured_probe"})
PROBE_AVAILABILITIES = frozenset({"measured", "blocked", "unsupported"})
FORBIDDEN_NCM_AUTHORITY_TERMS = (
    "git",
    "source control",
    "revision control",
    "checkout discovery",
    "repository",
    "worktree",
    "file navigation",
    "file lookup",
    "code navigation",
    "code graph",
    "symbol lookup",
    "source lookup",
    "current code",
    "prompt composition",
    "tool execution",
    "native db",
    "native database",
    "native facts",
    "tracedecay storage",
)
REQUIRED_AUTHORITY_EXCLUSIONS = (
    "git_and_repository_resolution",
    "codebase_navigation",
    "tracedecay_storage",
    "canonical_tracedecay_authority",
)
AUDIT_SCHEMA_VERSION = 1
AUDIT_BEAD_ID = "tdmem-0701"
PINNED_BIOMEM_REPOSITORY = "https://github.com/bleedingDev/biomem"
PINNED_BIOMEM_REVISION = "500847ff65b5d9548b3826fa29bf3ccf8d221147"
PINNED_BIOMEM_EVIDENCE_REPOSITORY = f"bleedingDev/biomem@{PINNED_BIOMEM_REVISION}"
REVISION_PROBE_ID = "tdmem-0701.revision-platform.v1"
SURFACE_PROBE_ID = "tracedecay.ncm.surface-probe.v2"
CONCURRENCY_LEVELS = (1, 2, 4, 8)
SURFACE_MEASUREMENT_IDS = (
    "python_syntax",
    "callable_surface_inventory",
    "http_health_identity",
    "http_parallel_requests",
    "client_disconnect",
)
CORE_MEASUREMENT_IDS = (
    "health_load_state_identity",
    "observation_retry_effects",
    "bounded_recall",
    "core_parallel_operations",
    "cancellation_deadline_observation",
    "cross_scope_leakage",
    "restart_equivalence",
    "interrupted_save_restore_incompatibility",
)
ALL_MEASUREMENT_IDS = SURFACE_MEASUREMENT_IDS + CORE_MEASUREMENT_IDS
SURFACE_CLAIM_SCOPES = {
    "python_syntax": "immutable_biomem_python_source",
    "callable_surface_inventory": "immutable_biomem_source_signatures",
    "http_health_identity": (
        "actual_biomem_http_transport_with_synthetic_status_handler"
    ),
    "http_parallel_requests": (
        "actual_biomem_http_transport_with_bounded_synthetic_handler"
    ),
    "client_disconnect": (
        "actual_biomem_http_transport_with_bounded_synthetic_handler"
    ),
}
PRODUCTION_BLOCKERS = {
    "state-readiness": "biomem",
    "exact-scope-isolation": "adapter",
    "server-cancellation-effect-reconciliation": "biomem",
    "crash-safe-persistence": "biomem",
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--repo", type=Path, default=Path("."))
    parser.add_argument("--audit", type=Path, default=DEFAULT_AUDIT)
    parser.add_argument("--registry", type=Path, default=DEFAULT_REGISTRY)
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


def non_empty_string(value: Any, field: str, errors: list[str]) -> str:
    if not isinstance(value, str) or not value.strip():
        errors.append(f"{field} must be a non-empty string")
        return ""
    return value.strip()


def non_negative_integer(value: Any, field: str, errors: list[str]) -> int | None:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        errors.append(f"{field} must be a non-negative integer")
        return None
    return value


def boolean(value: Any, field: str, errors: list[str]) -> bool | None:
    if not isinstance(value, bool):
        errors.append(f"{field} must be a boolean")
        return None
    return value


def exact_string_list(
    value: Any,
    field: str,
    expected: tuple[str, ...],
    errors: list[str],
) -> list[Any]:
    rows = require_list(value, field, errors)
    if rows != list(expected):
        errors.append(f"{field} must be exactly {list(expected)!r}")
    return rows


def index_rows(
    rows: Iterable[Any], field: str, id_field: str, errors: list[str]
) -> dict[str, dict[str, Any]]:
    indexed: dict[str, dict[str, Any]] = {}
    for offset, value in enumerate(rows):
        label = f"{field}[{offset}]"
        if not isinstance(value, dict):
            errors.append(f"{label} must be an object")
            continue
        row_id = non_empty_string(value.get(id_field), f"{label}.{id_field}", errors)
        if not row_id:
            continue
        if row_id in indexed:
            errors.append(f"{field} classifies {row_id!r} more than once")
            continue
        indexed[row_id] = value
    return indexed


def canonical_capabilities(
    registry: dict[str, Any], errors: list[str]
) -> dict[str, str]:
    capability_registry = require_object(
        registry.get("capability_registry"), "registry.capability_registry", errors
    )
    result: dict[str, str] = {}
    for requirement in ("mandatory", "optional"):
        rows = require_list(
            capability_registry.get(requirement),
            f"registry.capability_registry.{requirement}",
            errors,
        )
        for offset, row in enumerate(rows):
            label = f"registry.capability_registry.{requirement}[{offset}]"
            if not isinstance(row, dict):
                errors.append(f"{label} must be an object")
                continue
            capability_id = non_empty_string(row.get("id"), f"{label}.id", errors)
            if not capability_id:
                continue
            if capability_id in result:
                errors.append(f"registry repeats capability {capability_id!r}")
            result[capability_id] = requirement
    return result


def evidence_index(
    audit: dict[str, Any], errors: list[str]
) -> dict[str, dict[str, Any]]:
    rows = require_list(audit.get("evidence"), "audit.evidence", errors)
    indexed = index_rows(rows, "audit.evidence", "id", errors)
    probe_ids: set[str] = set()
    for evidence_id, row in indexed.items():
        label = f"audit.evidence[{evidence_id!r}]"
        kind = non_empty_string(row.get("kind"), f"{label}.kind", errors)
        if kind not in EVIDENCE_KINDS:
            errors.append(
                f"{label}.kind must be source_symbol or measured_probe, got {kind!r}"
            )
        elif kind == "source_symbol":
            non_empty_string(row.get("path"), f"{label}.path", errors)
            non_empty_string(row.get("symbol"), f"{label}.symbol", errors)
        else:
            probe_id = non_empty_string(
                row.get("probe_id"), f"{label}.probe_id", errors
            )
            if probe_id in probe_ids:
                errors.append(f"audit.evidence repeats measured probe_id {probe_id!r}")
            probe_ids.add(probe_id)
            observed = row.get("observed")
            if not isinstance(observed, dict) or not observed:
                errors.append(f"{label}.observed must be a non-empty measured object")
    return indexed


def validate_pinned_identity(
    audit: dict[str, Any],
    evidence: dict[str, dict[str, Any]],
    errors: list[str],
) -> None:
    if audit.get("schema_version") != AUDIT_SCHEMA_VERSION:
        errors.append(f"audit.schema_version must be {AUDIT_SCHEMA_VERSION}")
    if audit.get("bead_id") != AUDIT_BEAD_ID:
        errors.append(f"audit.bead_id must be {AUDIT_BEAD_ID!r}")

    subject = require_object(audit.get("audit_subject"), "audit.audit_subject", errors)
    if subject.get("repository") != PINNED_BIOMEM_REPOSITORY:
        errors.append(
            "audit.audit_subject.repository must identify the pinned Biomem repository"
        )
    if subject.get("revision") != PINNED_BIOMEM_REVISION:
        errors.append(
            "audit.audit_subject.revision must equal the immutable Biomem revision"
        )

    measured_probe_mapping = {
        evidence_id: row.get("probe_id")
        for evidence_id, row in evidence.items()
        if row.get("kind") == "measured_probe"
    }
    expected_probe_mapping = {
        "probe-revision-platform": REVISION_PROBE_ID,
        "probe-surface": SURFACE_PROBE_ID,
    }
    if measured_probe_mapping != expected_probe_mapping:
        errors.append(
            "audit measured-probe evidence must contain exactly the pinned revision "
            f"and surface probes: {expected_probe_mapping!r}"
        )

    for evidence_id, row in evidence.items():
        if row.get("kind") != "source_symbol":
            continue
        path = str(row.get("path", ""))
        repository = row.get("repository")
        if repository == PINNED_BIOMEM_EVIDENCE_REPOSITORY or path.startswith("src/"):
            if repository != PINNED_BIOMEM_EVIDENCE_REPOSITORY:
                errors.append(
                    f"audit.evidence[{evidence_id!r}].repository must equal "
                    f"{PINNED_BIOMEM_EVIDENCE_REPOSITORY!r}"
                )
            if not path.startswith("src/") or ".." in Path(path).parts:
                errors.append(
                    f"audit.evidence[{evidence_id!r}].path must be a normalized src/ path"
                )

    revision_probe = evidence.get("probe-revision-platform")
    if revision_probe is None:
        errors.append("audit.evidence must include probe-revision-platform")
    else:
        if revision_probe.get("probe_id") != REVISION_PROBE_ID:
            errors.append(
                f"audit.evidence['probe-revision-platform'].probe_id must be "
                f"{REVISION_PROBE_ID!r}"
            )
        revision_observed = require_object(
            revision_probe.get("observed"),
            "audit.evidence['probe-revision-platform'].observed",
            errors,
        )
        if revision_observed.get("revision") != PINNED_BIOMEM_REVISION:
            errors.append(
                "audit.evidence['probe-revision-platform'].observed.revision must "
                "equal the immutable Biomem revision"
            )


def validate_measurement_envelope(
    row: Any,
    probe_id: str,
    expected_availability: str | None,
    expected_claim_scope: str,
    errors: list[str],
) -> dict[str, Any]:
    label = f"audit.evidence['probe-surface'].observed.measurements[{probe_id!r}]"
    envelope = require_object(row, label, errors)
    required_fields = {
        "probe_id",
        "availability",
        "claim_scope",
        "expectation",
        "observed",
        "diagnostic",
        "elapsed_ms",
    }
    if set(envelope) != required_fields:
        errors.append(
            f"{label} must contain exactly the probe measurement envelope fields"
        )
    if envelope.get("probe_id") != probe_id:
        errors.append(f"{label}.probe_id must be {probe_id!r}")
    availability = envelope.get("availability")
    if availability not in PROBE_AVAILABILITIES:
        errors.append(f"{label}.availability is invalid: {availability!r}")
    if expected_availability is not None and availability != expected_availability:
        errors.append(f"{label}.availability must be {expected_availability!r}")
    if envelope.get("claim_scope") != expected_claim_scope:
        errors.append(f"{label}.claim_scope must be {expected_claim_scope!r}")
    non_empty_string(envelope.get("expectation"), f"{label}.expectation", errors)
    elapsed = envelope.get("elapsed_ms")
    if elapsed is not None:
        non_negative_integer(elapsed, f"{label}.elapsed_ms", errors)
    if availability == "measured":
        if not isinstance(envelope.get("observed"), dict) or not envelope.get(
            "observed"
        ):
            errors.append(f"{label}.observed must be a non-empty object")
        if envelope.get("diagnostic") is not None:
            errors.append(f"{label}.diagnostic must be null for measured evidence")
    elif availability in {"blocked", "unsupported"}:
        if envelope.get("observed") is not None:
            errors.append(f"{label}.observed must be null for blocked evidence")
        non_empty_string(envelope.get("diagnostic"), f"{label}.diagnostic", errors)
        if elapsed is not None:
            errors.append(f"{label}.elapsed_ms must be null for skipped core evidence")
    return envelope


def validate_surface_probe_projection(
    evidence: dict[str, dict[str, Any]], errors: list[str]
) -> dict[str, dict[str, Any]]:
    row = evidence.get("probe-surface")
    if row is None:
        errors.append("audit.evidence must include probe-surface")
        return {}
    if row.get("probe_id") != SURFACE_PROBE_ID:
        errors.append(
            f"audit.evidence['probe-surface'].probe_id must be {SURFACE_PROBE_ID!r}"
        )
    exact_string_list(
        row.get("measurement_ids"),
        "audit.evidence['probe-surface'].measurement_ids",
        ALL_MEASUREMENT_IDS,
        errors,
    )

    try:
        command = shlex.split(str(row.get("command", "")))
    except ValueError as exc:
        errors.append(f"audit.evidence['probe-surface'].command is invalid: {exc}")
        command = []
    if not any(token.endswith("probe-ncm-surface.py") for token in command):
        errors.append("probe-surface command must invoke probe-ncm-surface.py")
    revision_flags = [
        offset for offset, token in enumerate(command) if token == "--expected-revision"
    ]
    command_revision = (
        command[revision_flags[0] + 1]
        if len(revision_flags) == 1 and revision_flags[0] + 1 < len(command)
        else None
    )
    if len(revision_flags) != 1 or command_revision != PINNED_BIOMEM_REVISION:
        errors.append(
            "probe-surface command must pass the exact immutable Biomem revision once"
        )
    core_mode_flags = [
        offset for offset, token in enumerate(command) if token == "--core-mode"
    ]
    command_core_mode = (
        command[core_mode_flags[0] + 1]
        if len(core_mode_flags) == 1 and core_mode_flags[0] + 1 < len(command)
        else None
    )
    observed = require_object(
        row.get("observed"), "audit.evidence['probe-surface'].observed", errors
    )
    expected_observed_keys = {
        "schema_version",
        "probe_id",
        "input",
        "measurements",
        "summary",
    }
    if set(observed) != expected_observed_keys:
        errors.append(
            "probe-surface observed projection must contain the exact v2 top-level fields"
        )
    if observed.get("schema_version") != 2:
        errors.append("probe-surface observed.schema_version must be 2")
    if observed.get("probe_id") != SURFACE_PROBE_ID:
        errors.append(f"probe-surface observed.probe_id must be {SURFACE_PROBE_ID!r}")
    input_row = require_object(
        observed.get("input"), "probe-surface observed.input", errors
    )
    if input_row.get("expected_revision") != PINNED_BIOMEM_REVISION:
        errors.append("probe-surface input must retain the pinned Biomem revision")
    core_mode = input_row.get("core_mode")
    if core_mode not in {"auto", "skip"}:
        errors.append("probe-surface input.core_mode must be 'auto' or 'skip'")
    if len(core_mode_flags) != 1 or command_core_mode != core_mode:
        errors.append(
            "probe-surface command must explicitly match observed input.core_mode"
        )
    if input_row.get("concurrency_levels") != list(CONCURRENCY_LEVELS):
        errors.append("probe-surface input.concurrency_levels must be [1, 2, 4, 8]")

    raw_measurements = require_list(
        observed.get("measurements"), "probe-surface observed.measurements", errors
    )
    indexed = index_rows(
        raw_measurements,
        "audit.evidence['probe-surface'].observed.measurements",
        "probe_id",
        errors,
    )
    emitted_ids = tuple(
        item.get("probe_id") if isinstance(item, dict) else None
        for item in raw_measurements
    )
    if emitted_ids != ALL_MEASUREMENT_IDS:
        errors.append(
            "probe-surface observed.measurements must preserve the exact probe sequence"
        )
    result: dict[str, dict[str, Any]] = {}
    for probe_id in SURFACE_MEASUREMENT_IDS:
        result[probe_id] = validate_measurement_envelope(
            indexed.get(probe_id),
            probe_id,
            "measured",
            SURFACE_CLAIM_SCOPES[probe_id],
            errors,
        )
    for probe_id in CORE_MEASUREMENT_IDS:
        envelope = validate_measurement_envelope(
            indexed.get(probe_id),
            probe_id,
            "blocked" if core_mode == "skip" else None,
            "actual_biomem_text_memory",
            errors,
        )
        result[probe_id] = envelope
        if (
            core_mode == "skip"
            and envelope.get("diagnostic") != "caller selected --core-mode skip"
        ):
            errors.append(
                f"blocked core measurement {probe_id!r} must record the skip reason"
            )
        if (
            core_mode == "auto"
            and envelope.get("availability") in {"blocked", "unsupported"}
            and envelope.get("diagnostic") == "caller selected --core-mode skip"
        ):
            errors.append(
                f"auto-mode core measurement {probe_id!r} cannot claim caller-selected skip"
            )

    summary = require_object(
        observed.get("summary"), "probe-surface observed.summary", errors
    )
    expected_summary = {
        availability: sum(
            isinstance(item, dict) and item.get("availability") == availability
            for item in raw_measurements
        )
        for availability in sorted(PROBE_AVAILABILITIES)
    }
    expected_summary["total"] = len(raw_measurements)
    for key in ("blocked", "measured", "unsupported", "total"):
        non_negative_integer(summary.get(key), f"probe-surface summary.{key}", errors)
    if summary != expected_summary:
        errors.append(f"probe-surface summary must be exactly {expected_summary!r}")
    if all(
        isinstance(summary.get(key), int) and not isinstance(summary.get(key), bool)
        for key in ("blocked", "measured", "unsupported", "total")
    ) and (
        summary["blocked"] + summary["measured"] + summary["unsupported"]
        != summary["total"]
    ):
        errors.append("probe-surface summary counts must conserve total")
    return result


def validate_surface_measurements(
    audit: dict[str, Any],
    measurements: dict[str, dict[str, Any]],
    errors: list[str],
) -> None:
    syntax = require_object(
        measurements.get("python_syntax", {}).get("observed"),
        "python_syntax observed",
        errors,
    )
    files_checked = non_negative_integer(
        syntax.get("files_checked"), "python_syntax files_checked", errors
    )
    error_count = non_negative_integer(
        syntax.get("error_count"), "python_syntax error_count", errors
    )
    syntax_errors = require_list(syntax.get("errors"), "python_syntax errors", errors)
    if error_count is not None and error_count != len(syntax_errors):
        errors.append("python_syntax error_count must equal len(errors)")
    if files_checked is not None and files_checked == 0:
        errors.append("python_syntax must check at least one source file")

    inventory = require_object(
        measurements.get("callable_surface_inventory", {}).get("observed"),
        "callable_surface_inventory observed",
        errors,
    )
    if inventory.get("inventory_kind") != "declared_methods_only":
        errors.append("callable inventory must declare declared_methods_only scope")
    for diagnostic in ("text_memory_parse_diagnostic", "http_parse_diagnostic"):
        if inventory.get(diagnostic) is not None:
            errors.append(f"callable inventory {diagnostic} must be null")
    for polarity in (
        "cancellation_parameter_present",
        "deadline_parameter_present",
    ):
        if inventory.get(polarity) is not False:
            errors.append(f"callable inventory {polarity} must be false")
    expected_method_ids = {
        "get_stats",
        "list_memories",
        "load",
        "restore",
        "save",
        "search",
        "store_record",
    }
    for field, expected_ids in (
        ("text_memory_methods", expected_method_ids),
        (
            "http_methods",
            {"_handle_quick_status", "_submit_command", "do_GET", "do_POST"},
        ),
    ):
        methods = require_object(inventory.get(field), f"inventory {field}", errors)
        if set(methods) != expected_ids:
            errors.append(f"inventory {field} must contain the exact probed methods")
        for method, raw in methods.items():
            method_row = require_object(raw, f"inventory {field}.{method}", errors)
            if method_row.get("present") is not True:
                errors.append(f"inventory {field}.{method}.present must be true")
            parameters = require_list(
                method_row.get("parameters"),
                f"inventory {field}.{method}.parameters",
                errors,
            )
            if not parameters or any(
                not isinstance(parameter, str) or not parameter
                for parameter in parameters
            ):
                errors.append(
                    f"inventory {field}.{method}.parameters must be non-empty strings"
                )

    health = require_object(
        measurements.get("http_health_identity", {}).get("observed"),
        "http_health_identity observed",
        errors,
    )
    health_polarities = {
        "handler_status_was_synthetic": True,
        "loaded_state_identity_complete": False,
        "ready": True,
        "ready_without_loaded_state_identity": True,
    }
    for field, expected in health_polarities.items():
        if health.get(field) is not expected:
            errors.append(f"http_health_identity {field} must be {expected!r}")
    if health.get("loaded_state_identity_fields_present") != []:
        errors.append(
            "http_health_identity loaded_state_identity_fields_present must be empty"
        )
    if health.get("http_status") != 200:
        errors.append("http_health_identity http_status must be 200")

    parallel = require_object(
        measurements.get("http_parallel_requests", {}).get("observed"),
        "http_parallel_requests observed",
        errors,
    )
    if parallel.get("concurrency_levels") != list(CONCURRENCY_LEVELS):
        errors.append("http_parallel_requests concurrency_levels must be [1, 2, 4, 8]")
    if parallel.get("real_memory_backend_used") is not False:
        errors.append("http_parallel_requests real_memory_backend_used must be false")
    matrix = require_list(parallel.get("matrix"), "http parallel matrix", errors)
    if len(matrix) != len(CONCURRENCY_LEVELS):
        errors.append("http parallel matrix must contain exactly four rows")
    for expected_level, raw in zip(CONCURRENCY_LEVELS, matrix):
        label = f"http parallel matrix[{expected_level}]"
        row = require_object(raw, label, errors)
        values = {
            field: non_negative_integer(row.get(field), f"{label}.{field}", errors)
            for field in (
                "parallel_requests",
                "attempted",
                "completed",
                "errors",
                "elapsed_ms",
                "max_active",
            )
        }
        if values["parallel_requests"] != expected_level:
            errors.append(f"{label}.parallel_requests must be {expected_level}")
        if values["attempted"] != expected_level:
            errors.append(f"{label}.attempted must be {expected_level}")
        if values["completed"] != expected_level:
            errors.append(f"{label}.completed must be {expected_level}")
        if values["errors"] != 0:
            errors.append(f"{label}.errors must be 0")
        if values["max_active"] != expected_level:
            errors.append(f"{label}.max_active must be {expected_level}")
        if (
            values["attempted"] is not None
            and values["completed"] is not None
            and values["errors"] is not None
            and values["completed"] + values["errors"] != values["attempted"]
        ):
            errors.append(f"{label} completed + errors must equal attempted")
        if (
            values["max_active"] is not None
            and values["attempted"] is not None
            and not (1 <= values["max_active"] <= values["attempted"])
        ):
            errors.append(f"{label}.max_active must be within 1..attempted")

    disconnect = require_object(
        measurements.get("client_disconnect", {}).get("observed"),
        "client_disconnect observed",
        errors,
    )
    for field in ("elapsed_ms", "server_completion_wait_ms"):
        non_negative_integer(
            disconnect.get(field), f"client_disconnect {field}", errors
        )
    counters = {
        field: non_negative_integer(
            disconnect.get(field), f"client_disconnect {field}", errors
        )
        for field in ("handler_started", "handler_completed", "handler_cancelled")
    }
    expected_disconnect = {
        "timeout_seen": True,
        "server_completed_after_disconnect": True,
        "server_observed_cancellation": False,
        "handler_started": 1,
        "handler_completed": 1,
        "handler_cancelled": 0,
    }
    for field, expected in expected_disconnect.items():
        if disconnect.get(field) != expected or isinstance(
            disconnect.get(field), bool
        ) != isinstance(expected, bool):
            errors.append(f"client_disconnect {field} must be {expected!r}")
    if (
        counters["handler_started"] is not None
        and all(
            counters[field] is not None
            for field in ("handler_completed", "handler_cancelled")
        )
        and counters["handler_started"]
        != (counters["handler_completed"] + counters["handler_cancelled"])
    ):
        errors.append(
            "client_disconnect handler_completed + handler_cancelled must equal handler_started"
        )
    if disconnect.get("server_observed_cancellation") is not (
        counters["handler_cancelled"] is not None and counters["handler_cancelled"] > 0
    ):
        errors.append(
            "client_disconnect cancellation polarity must match handler_cancelled"
        )

    section = require_object(
        audit.get("threading_and_cancellation"),
        "audit.threading_and_cancellation",
        errors,
    )
    if section.get("classification") != "blocking_for_mutations_and_bounded_stop":
        errors.append(
            "audit.threading_and_cancellation.classification must remain "
            "'blocking_for_mutations_and_bounded_stop'"
        )
    for field, probe_id in (
        ("core_threading_evidence", "core_parallel_operations"),
        ("core_cancellation_evidence", "cancellation_deadline_observation"),
    ):
        availability = measurements.get(probe_id, {}).get("availability")
        if section.get(field) != availability:
            errors.append(
                f"audit.threading_and_cancellation.{field} must match "
                f"the {probe_id!r} envelope availability"
            )
    projected = require_object(
        section.get("observed_results"),
        "audit.threading_and_cancellation.observed_results",
        errors,
    )
    if projected.get("transport_concurrency") != parallel:
        errors.append(
            "threading_and_cancellation transport projection is stale or incomplete"
        )
    if projected.get("client_disconnect") != disconnect:
        errors.append(
            "threading_and_cancellation disconnect projection is stale or incomplete"
        )

    validate_measured_core_measurements(measurements, errors)


def validate_measured_core_measurements(
    measurements: dict[str, dict[str, Any]], errors: list[str]
) -> None:
    measured = {
        probe_id: require_object(
            measurements.get(probe_id, {}).get("observed"),
            f"{probe_id} observed",
            errors,
        )
        for probe_id in CORE_MEASUREMENT_IDS
        if measurements.get(probe_id, {}).get("availability") == "measured"
    }

    health = measured.get("health_load_state_identity")
    if health is not None:
        require_list(health.get("stats_keys"), "core health stats_keys", errors)
        require_list(
            health.get("identity_fields_present"),
            "core health identity_fields_present",
            errors,
        )
        boolean(
            health.get("loaded_state_identity_complete"),
            "core health loaded_state_identity_complete",
            errors,
        )
        boolean(health.get("load_invoked"), "core health load_invoked", errors)
        boolean(
            health.get("state_existed_before_load"),
            "core health state_existed_before_load",
            errors,
        )
        non_empty_string(
            health.get("load_result_type"), "core health load_result_type", errors
        )

    retry = measured.get("observation_retry_effects")
    if retry is not None:
        attempted = non_negative_integer(
            retry.get("attempted"), "core retry attempted", errors
        )
        completed = non_negative_integer(
            retry.get("completed"), "core retry completed", errors
        )
        if attempted != 2 or completed != attempted:
            errors.append("core retry must record two attempted and completed calls")
        non_negative_integer(
            retry.get("matching_memory_count"),
            "core retry matching_memory_count",
            errors,
        )
        require_list(
            retry.get("provider_receipt_or_idempotency_fields_present"),
            "core retry provider_receipt_or_idempotency_fields_present",
            errors,
        )

    recall = measured.get("bounded_recall")
    if recall is not None:
        requested = non_negative_integer(
            recall.get("requested_top_k"), "core recall requested_top_k", errors
        )
        returned = non_negative_integer(
            recall.get("returned"), "core recall returned", errors
        )
        bounded = boolean(recall.get("bounded"), "core recall bounded", errors)
        if (
            requested is not None
            and returned is not None
            and (bounded is not (returned <= requested))
        ):
            errors.append(
                "core recall bounded polarity must match returned <= requested"
            )
        for field in (
            "memory_ids_present",
            "provenance_present",
            "native_scores_present",
        ):
            count = non_negative_integer(
                recall.get(field), f"core recall {field}", errors
            )
            if count is not None and returned is not None and count > returned:
                errors.append(f"core recall {field} cannot exceed returned")

    parallel = measured.get("core_parallel_operations")
    if parallel is not None:
        if parallel.get("concurrency_levels") != list(CONCURRENCY_LEVELS):
            errors.append("core parallel concurrency_levels must be [1, 2, 4, 8]")
        for matrix_name in ("read_matrix", "write_matrix"):
            matrix = require_list(
                parallel.get(matrix_name), f"core parallel {matrix_name}", errors
            )
            if len(matrix) != len(CONCURRENCY_LEVELS):
                errors.append(f"core parallel {matrix_name} must contain four rows")
            for level, raw in zip(CONCURRENCY_LEVELS, matrix):
                label = f"core parallel {matrix_name}[{level}]"
                row = require_object(raw, label, errors)
                values = {
                    field: non_negative_integer(
                        row.get(field), f"{label}.{field}", errors
                    )
                    for field in (
                        "parallel_callers",
                        "attempted",
                        "completed",
                        "errors",
                        "max_callers_inflight",
                        "elapsed_ms",
                    )
                }
                if values["parallel_callers"] != level or values["attempted"] != level:
                    errors.append(f"{label} must represent concurrency level {level}")
                if (
                    all(
                        values[field] is not None
                        for field in ("attempted", "completed", "errors")
                    )
                    and values["completed"] + values["errors"] != values["attempted"]
                ):
                    errors.append(f"{label} completed + errors must equal attempted")
                if (
                    values["max_callers_inflight"] is not None
                    and values["attempted"] is not None
                    and not (1 <= values["max_callers_inflight"] <= values["attempted"])
                ):
                    errors.append(f"{label}.max_callers_inflight is implausible")
                require_list(row.get("error_types"), f"{label}.error_types", errors)

    cancellation = measured.get("cancellation_deadline_observation")
    if cancellation is not None:
        for field in (
            "cancellation_parameter_present",
            "deadline_parameter_present",
            "caller_wait_timeout_seen",
            "operation_settled_after_caller_timeout",
            "normal_return_observed",
            "error_observed",
            "operation_still_running_after_followup_wait",
        ):
            boolean(cancellation.get(field), f"core cancellation {field}", errors)
        non_negative_integer(
            cancellation.get("elapsed_ms"), "core cancellation elapsed_ms", errors
        )
        if cancellation.get("provider_cancellation_observed") not in {
            None,
            True,
            False,
        }:
            errors.append(
                "core cancellation provider_cancellation_observed must be boolean or null"
            )
        if cancellation.get("committed_effect") is not None:
            errors.append("read-only core cancellation committed_effect must be null")
        if cancellation.get("operation_kind") != "read_only":
            errors.append("core cancellation operation_kind must be 'read_only'")
        if (
            cancellation.get("operation_settled_after_caller_timeout") is True
            and cancellation.get("caller_wait_timeout_seen") is not True
        ):
            errors.append(
                "core cancellation settlement-after-timeout requires a timeout"
            )
        if (
            cancellation.get("operation_still_running_after_followup_wait") is True
            and cancellation.get("operation_settled_after_caller_timeout") is True
        ):
            errors.append("core cancellation cannot be settled and still running")
        if (
            cancellation.get("normal_return_observed") is True
            and cancellation.get("error_observed") is True
        ):
            errors.append(
                "core cancellation cannot report both normal return and error"
            )
        call_outcome = cancellation.get("call_outcome")
        if call_outcome is not None and not isinstance(call_outcome, dict):
            errors.append("core cancellation call_outcome must be an object or null")

    scope = measured.get("cross_scope_leakage")
    if scope is not None:
        for field in ("scope_a_count", "scope_b_count", "leaked_identity_count"):
            non_negative_integer(scope.get(field), f"core scope {field}", errors)
        boolean(
            scope.get("scope_a_identity_present"),
            "core scope scope_a_identity_present",
            errors,
        )
        boolean(
            scope.get("isolated_state_paths_used"),
            "core scope isolated_state_paths_used",
            errors,
        )

    restart = measured.get("restart_equivalence")
    if restart is not None:
        for field in ("before_count", "after_count"):
            non_negative_integer(restart.get(field), f"core restart {field}", errors)
        for field in ("same_memory_ids", "same_bounded_recall_product"):
            boolean(restart.get(field), f"core restart {field}", errors)
        for field in (
            "missing_after_restart",
            "unexpected_after_restart",
            "bounded_recall_before",
            "bounded_recall_after",
        ):
            require_list(restart.get(field), f"core restart {field}", errors)

    restore = measured.get("interrupted_save_restore_incompatibility")
    if restore is not None:
        interrupted = require_object(
            restore.get("interrupted_save"), "core interrupted_save", errors
        )
        if interrupted.get("availability") != "blocked":
            errors.append("core interrupted_save availability must be 'blocked'")
        non_empty_string(
            interrupted.get("diagnostic"), "core interrupted_save diagnostic", errors
        )
        incompatible = require_object(
            restore.get("incompatible_restore"), "core incompatible_restore", errors
        )
        if incompatible.get("availability") != "measured":
            errors.append("core incompatible_restore availability must be 'measured'")
        for field in ("raised", "state_unchanged"):
            boolean(
                incompatible.get(field), f"core incompatible_restore {field}", errors
            )
        for field in ("before_count", "after_count"):
            non_negative_integer(
                incompatible.get(field), f"core incompatible_restore {field}", errors
            )


def licensed_surface_evidence(row: dict[str, Any]) -> bool:
    if row.get("kind") == "measured_probe":
        return True
    repository = str(row.get("repository", "")).lower()
    path = str(row.get("path", "")).lower()
    return "biomem" in repository or path.startswith("src/")


def evidence_text(row: dict[str, Any]) -> str:
    return " ".join(
        str(row.get(field, "")).lower()
        for field in ("id", "repository", "path", "symbol", "probe_id", "command")
    )


def document_evidence_references(audit: dict[str, Any]) -> set[str]:
    references: set[str] = set()

    def walk(value: Any) -> None:
        if isinstance(value, dict):
            for key, nested in value.items():
                if key == "evidence_ids" and isinstance(nested, list):
                    references.update(item for item in nested if isinstance(item, str))
                else:
                    walk(nested)
        elif isinstance(value, list):
            for nested in value:
                walk(nested)

    walk({key: value for key, value in audit.items() if key != "evidence"})
    return references


def measured_key_text(row: dict[str, Any]) -> str:
    keys: list[str] = []

    def walk(value: Any) -> None:
        if isinstance(value, dict):
            for key, nested in value.items():
                keys.append(str(key).lower())
                walk(nested)
        elif isinstance(value, list):
            for nested in value:
                walk(nested)

    walk(row.get("observed"))
    return " ".join(keys)


def evidence_references(
    row: dict[str, Any],
    label: str,
    evidence: dict[str, dict[str, Any]],
    errors: list[str],
    *,
    required: bool,
) -> list[str]:
    raw = row.get("evidence_ids")
    if raw is None and not required:
        return []
    references = require_list(raw, f"{label}.evidence_ids", errors)
    result: list[str] = []
    for offset, value in enumerate(references):
        evidence_id = non_empty_string(value, f"{label}.evidence_ids[{offset}]", errors)
        if not evidence_id:
            continue
        if evidence_id not in evidence:
            errors.append(f"{label} cites unknown evidence {evidence_id!r}")
        result.append(evidence_id)
    if required and not result:
        errors.append(f"{label} must cite source-symbol or measured-probe evidence")
    return result


def validate_capabilities(
    audit: dict[str, Any],
    canonical: dict[str, str],
    evidence: dict[str, dict[str, Any]],
    errors: list[str],
) -> dict[str, dict[str, Any]]:
    rows = require_list(
        audit.get("capability_matrix"), "audit.capability_matrix", errors
    )
    matrix = index_rows(rows, "audit.capability_matrix", "capability_id", errors)
    missing = sorted(set(canonical) - set(matrix))
    unknown = sorted(set(matrix) - set(canonical))
    if missing:
        errors.append(
            f"capability_matrix is missing canonical capabilities: {missing!r}"
        )
    if unknown:
        errors.append(
            f"capability_matrix contains non-canonical capabilities: {unknown!r}"
        )

    for capability_id in sorted(set(canonical) & set(matrix)):
        row = matrix[capability_id]
        label = f"audit.capability_matrix[{capability_id!r}]"
        requirement = non_empty_string(
            row.get("requirement"), f"{label}.requirement", errors
        )
        if requirement != canonical[capability_id]:
            errors.append(
                f"{label}.requirement must be {canonical[capability_id]!r}, "
                f"got {requirement!r}"
            )
        classification = non_empty_string(
            row.get("classification"), f"{label}.classification", errors
        )
        if classification not in CLASSIFICATIONS:
            errors.append(f"{label}.classification is invalid: {classification!r}")
        if canonical[capability_id] == "mandatory" and classification == "unsupported":
            errors.append(
                f"mandatory capability {capability_id!r} cannot be unsupported"
            )
        references = evidence_references(
            row,
            label,
            evidence,
            errors,
            required=classification in {"supported", "adaptable"},
        )
        if classification in {"supported", "adaptable"}:
            referenced_rows = [
                evidence[reference] for reference in references if reference in evidence
            ]
            if not any(licensed_surface_evidence(row) for row in referenced_rows):
                errors.append(
                    f"{label} must cite the licensed surface or a measured probe"
                )
        adapter_requirements = require_list(
            row.get("adapter_requirements"), f"{label}.adapter_requirements", errors
        )
        for offset, value in enumerate(adapter_requirements):
            non_empty_string(value, f"{label}.adapter_requirements[{offset}]", errors)
        if not isinstance(row.get("ncm_change_required"), bool):
            errors.append(f"{label}.ncm_change_required must be a boolean")
        if classification == "supported" and (
            adapter_requirements or row.get("ncm_change_required") is not False
        ):
            errors.append(
                f"{label} cannot be supported while adapter work or NCM changes remain"
            )
        if classification == "adaptable" and not adapter_requirements:
            errors.append(f"{label} adaptable classification requires adapter work")
        if classification == "blocking":
            blockers = require_list(row.get("blockers"), f"{label}.blockers", errors)
            if not blockers:
                errors.append(f"{label} blocking classification requires blockers")
        if classification == "unsupported" and adapter_requirements:
            errors.append(
                f"{label} unsupported classification cannot claim an adapter path"
            )
    return matrix


def validate_mandatory_operations(
    audit: dict[str, Any],
    matrix: dict[str, dict[str, Any]],
    evidence: dict[str, dict[str, Any]],
    errors: list[str],
) -> None:
    rows = require_list(
        audit.get("mandatory_operations"), "audit.mandatory_operations", errors
    )
    operations = index_rows(rows, "audit.mandatory_operations", "capability_id", errors)
    if set(operations) != set(MANDATORY_OPERATIONS):
        errors.append(
            "mandatory_operations must classify exactly "
            f"{sorted(MANDATORY_OPERATIONS)!r}"
        )
    for capability_id, operation in MANDATORY_OPERATIONS.items():
        row = operations.get(capability_id)
        if row is None:
            continue
        label = f"audit.mandatory_operations[{capability_id!r}]"
        if row.get("mandatory") is not True:
            errors.append(f"{label}.mandatory must be true")
        if row.get("operation") != operation:
            errors.append(f"{label}.operation must be {operation!r}")
        classification = row.get("classification")
        if classification not in {"supported", "adaptable", "blocking"}:
            errors.append(
                f"{label}.classification must be supported, adaptable, or blocking"
            )
        matrix_row = matrix.get(capability_id)
        if matrix_row is not None and classification != matrix_row.get(
            "classification"
        ):
            errors.append(f"{label}.classification disagrees with capability_matrix")
        non_empty_string(
            row.get("licensed_primitive"), f"{label}.licensed_primitive", errors
        )
        references = evidence_references(row, label, evidence, errors, required=True)
        relevant_terms = {
            "provider.health.v1": ("health", "status", "load"),
            "observation.accept.v1": ("store", "write", "observation"),
            "recall.query.v1": ("search", "recall"),
        }[capability_id]
        if references and not any(
            any(
                term in evidence_text(evidence.get(reference, {}))
                for term in relevant_terms
            )
            for reference in references
        ):
            errors.append(
                f"{label} evidence does not support the {operation} licensed primitive"
            )
        for list_field in ("conformance_gaps", "adapter_requirements"):
            values = require_list(row.get(list_field), f"{label}.{list_field}", errors)
            for offset, value in enumerate(values):
                non_empty_string(value, f"{label}.{list_field}[{offset}]", errors)
        if not isinstance(row.get("ncm_change_required"), bool):
            errors.append(f"{label}.ncm_change_required must be a boolean")
        conformance_gaps = row.get("conformance_gaps")
        adapter_requirements = row.get("adapter_requirements")
        if classification == "supported" and (
            conformance_gaps
            or adapter_requirements
            or row.get("ncm_change_required") is not False
        ):
            errors.append(
                f"{label} cannot be supported while conformance gaps or changes remain"
            )
        if classification == "adaptable" and not adapter_requirements:
            errors.append(f"{label} adaptable classification requires adapter work")
        if matrix_row is not None and row.get("ncm_change_required") != matrix_row.get(
            "ncm_change_required"
        ):
            errors.append(
                f"{label}.ncm_change_required disagrees with capability_matrix"
            )

    recall = operations.get("recall.query.v1")
    if recall is not None:
        primitive = str(recall.get("licensed_primitive", "")).lower()
        if "search" not in primitive or (
            "retrieve" in primitive and "not retrieve" not in primitive
        ):
            errors.append(
                "recall.query.v1 must map to side-effect-free search, not mutating retrieve"
            )


def observation_rows(
    section: dict[str, Any],
    field: str,
    evidence: dict[str, dict[str, Any]],
    errors: list[str],
    *,
    require_measurement: bool,
    measurement_terms: tuple[str, ...] = (),
) -> None:
    key = field.rsplit(".", 1)[-1]
    rows = require_list(section.get(key), field, errors)
    if not rows:
        errors.append(f"{field} must contain observed outcomes")
        return
    section_references = evidence_references(
        section,
        field.rsplit(".", 1)[0],
        evidence,
        errors,
        required=False,
    )
    measured_references = {
        reference
        for reference in section_references
        if evidence.get(reference, {}).get("kind") == "measured_probe"
    }
    for offset, row in enumerate(rows):
        label = f"{field}[{offset}]"
        if isinstance(row, str):
            non_empty_string(row, label, errors)
            continue
        if not isinstance(row, dict):
            errors.append(f"{label} must be a string or object")
            continue
        non_empty_string(row.get("outcome"), f"{label}.outcome", errors)
        references = evidence_references(
            row,
            label,
            evidence,
            errors,
            required=not section_references,
        )
        measured_references.update(
            reference
            for reference in references
            if evidence.get(reference, {}).get("kind") == "measured_probe"
        )
    measured = bool(measured_references)
    relevant_measurement = any(
        any(
            term in measured_key_text(evidence.get(reference, {}))
            for term in measurement_terms
        )
        for reference in measured_references
    )
    if require_measurement and (not measured or not relevant_measurement):
        errors.append(f"{field} must cite at least one measured probe")


def validate_persistence(
    audit: dict[str, Any], evidence: dict[str, dict[str, Any]], errors: list[str]
) -> None:
    section = require_object(audit.get("persistence"), "audit.persistence", errors)
    state_identity = require_object(
        section.get("state_identity"), "audit.persistence.state_identity", errors
    )
    non_empty_string(
        state_identity.get("observed"),
        "audit.persistence.state_identity.observed",
        errors,
    )
    if "production_compatible" in state_identity:
        if not isinstance(state_identity.get("production_compatible"), bool):
            errors.append(
                "audit.persistence.state_identity.production_compatible must be a boolean"
            )
        observed_identity = str(state_identity.get("observed", "")).lower()
        if state_identity.get("production_compatible") is True and any(
            marker in observed_identity
            for marker in ("no ", "absent", "missing", "unavailable", "not exposed")
        ):
            errors.append(
                "audit.persistence.state_identity cannot claim production compatibility "
                "when identity is absent"
            )
    else:
        missing = require_list(
            state_identity.get("missing"),
            "audit.persistence.state_identity.missing",
            errors,
        )
        if not missing:
            errors.append(
                "audit.persistence.state_identity must identify missing production identity"
            )
        for offset, value in enumerate(missing):
            non_empty_string(
                value,
                f"audit.persistence.state_identity.missing[{offset}]",
                errors,
            )
    compatibility = require_list(
        section.get("compatibility"), "audit.persistence.compatibility", errors
    )
    if not compatibility:
        errors.append(
            "audit.persistence.compatibility must document state compatibility"
        )
    for offset, row in enumerate(compatibility):
        label = f"audit.persistence.compatibility[{offset}]"
        if isinstance(row, str):
            non_empty_string(row, label, errors)
            continue
        if not isinstance(row, dict):
            errors.append(f"{label} must be a string or object")
            continue
        non_empty_string(row.get("dimension"), f"{label}.dimension", errors)
        non_empty_string(row.get("observed"), f"{label}.observed", errors)
        non_empty_string(row.get("required"), f"{label}.required", errors)
    non_empty_string(
        section.get("load_failure_policy"),
        "audit.persistence.load_failure_policy",
        errors,
    )
    load_policy = str(section.get("load_failure_policy", "")).lower().replace("_", " ")
    if "fail closed" not in load_policy or not any(
        marker in load_policy for marker in ("required", "reject", "block", "never")
    ):
        errors.append(
            "audit.persistence.load_failure_policy must require fail-closed rejection"
        )
    if section.get("implicit_reset_allowed") is not False:
        errors.append("audit.persistence.implicit_reset_allowed must be false")
    if section.get("required_load_failure_behavior") != "reject_readiness":
        errors.append(
            "audit.persistence.required_load_failure_behavior must be 'reject_readiness'"
        )
    raw_outcomes = section.get("observed_outcomes")
    if isinstance(raw_outcomes, list) and any(
        not isinstance(row, dict) for row in raw_outcomes
    ):
        errors.append(
            "audit.persistence.observed_outcomes must use evidence-bound objects"
        )
    observation_rows(
        section,
        "audit.persistence.observed_outcomes",
        evidence,
        errors,
        require_measurement=False,
    )

    lifecycle = require_object(audit.get("lifecycle"), "audit.lifecycle", errors)
    readiness = non_empty_string(
        lifecycle.get("readiness"), "audit.lifecycle.readiness", errors
    ).lower()
    if lifecycle.get("readiness_verification") != "unverified_loaded_state_identity":
        errors.append(
            "audit.lifecycle.readiness_verification must be "
            "'unverified_loaded_state_identity'"
        )
    if not any(
        marker in readiness
        for marker in ("independent", "without", "rather than", "no verified", "merely")
    ):
        errors.append(
            "audit.lifecycle.readiness must identify the current unverified readiness signal"
        )
    evidence_references(lifecycle, "audit.lifecycle", evidence, errors, required=True)


def validate_threading_and_cancellation(
    audit: dict[str, Any], evidence: dict[str, dict[str, Any]], errors: list[str]
) -> None:
    section = require_object(
        audit.get("threading_and_cancellation"),
        "audit.threading_and_cancellation",
        errors,
    )
    observation_rows(
        section,
        "audit.threading_and_cancellation.threading_observations",
        evidence,
        errors,
        require_measurement=True,
        measurement_terms=("parallel", "concurrent", "thread", "max_active"),
    )
    observation_rows(
        section,
        "audit.threading_and_cancellation.cancellation_observations",
        evidence,
        errors,
        require_measurement=True,
        measurement_terms=("cancel", "disconnect", "timeout", "effect"),
    )
    for field, measured_scope in (
        (
            "threading_observations",
            "actual_biomem_http_transport_with_bounded_synthetic_handler",
        ),
        (
            "cancellation_observations",
            "actual_biomem_http_transport_with_bounded_synthetic_handler",
        ),
    ):
        rows = require_list(
            section.get(field), f"audit.threading_and_cancellation.{field}", errors
        )
        if len(rows) != 5:
            errors.append(
                f"audit.threading_and_cancellation.{field} must contain five typed observations"
            )
        for offset, raw in enumerate(rows):
            label = f"audit.threading_and_cancellation.{field}[{offset}]"
            row = require_object(raw, label, errors)
            expected_kind = (
                "measured_probe" if offset == 0 else "pinned_source_inference"
            )
            if row.get("evidence_kind") != expected_kind:
                errors.append(f"{label}.evidence_kind must be {expected_kind!r}")
            claim_scope = non_empty_string(
                row.get("claim_scope"), f"{label}.claim_scope", errors
            )
            references = require_list(
                row.get("evidence_ids"), f"{label}.evidence_ids", errors
            )
            if offset == 0:
                if claim_scope != measured_scope:
                    errors.append(f"{label}.claim_scope must be {measured_scope!r}")
                if references != ["probe-surface"]:
                    errors.append(f"{label} must cite only probe-surface")
            elif any(
                evidence.get(reference, {}).get("kind") != "source_symbol"
                for reference in references
            ):
                errors.append(f"{label} must cite pinned source-symbol evidence only")

    cancellation_rows = section.get("cancellation_observations")
    if isinstance(cancellation_rows, list) and cancellation_rows:
        measured_outcome = str(
            cancellation_rows[0].get("outcome", "")
            if isinstance(cancellation_rows[0], dict)
            else cancellation_rows[0]
        ).lower()
        if "finally" in measured_outcome or "did not distinguish" in measured_outcome:
            errors.append(
                "measured disconnect outcome must not use the stale finally-only projection"
            )
        if not all(
            marker in measured_outcome
            for marker in ("normal-return", "no cancellederror", "synthetic")
        ):
            errors.append(
                "measured disconnect outcome must precisely report the instrumented synthetic result"
            )
        source_text = " ".join(
            str(row.get("outcome", "")).lower()
            for row in cancellation_rows[1:]
            if isinstance(row, dict)
        )
        if (
            "future.cancel" not in source_text
            or "provider cancellation signal" not in source_text
        ):
            errors.append(
                "source-inferred cancellation observations must ground the absent server cancellation path"
            )


def validate_production_gate(
    audit: dict[str, Any], evidence: dict[str, dict[str, Any]], errors: list[str]
) -> None:
    gate = require_object(audit.get("production_gate"), "audit.production_gate", errors)
    if gate.get("status") != "blocked":
        errors.append("audit.production_gate.status must remain 'blocked'")
    if gate.get("fake_readiness_allowed") is not False:
        errors.append("audit.production_gate.fake_readiness_allowed must be false")
    if gate.get("state_identity_required") is not True:
        errors.append("audit.production_gate.state_identity_required must be true")
    blockers = require_list(
        gate.get("blockers"), "audit.production_gate.blockers", errors
    )
    owners: set[str] = set()
    blocker_ids: set[str] = set()
    for offset, row in enumerate(blockers):
        label = f"audit.production_gate.blockers[{offset}]"
        if not isinstance(row, dict):
            errors.append(f"{label} must be an object")
            continue
        blocker_id = non_empty_string(row.get("id"), f"{label}.id", errors)
        if blocker_id in blocker_ids:
            errors.append(f"audit.production_gate.blockers repeats id {blocker_id!r}")
        blocker_ids.add(blocker_id)
        title = non_empty_string(row.get("title"), f"{label}.title", errors)
        owner = non_empty_string(
            row.get("owner_boundary"), f"{label}.owner_boundary", errors
        )
        if owner not in {"adapter", "biomem"}:
            errors.append(f"{label}.owner_boundary must be adapter or biomem")
        else:
            owners.add(owner)
        owner_text = f"{blocker_id} {title}".lower()
        expected_owner = None
        if any(
            marker in owner_text
            for marker in ("exact-scope", "exact scope", "envelope", "mapping")
        ):
            expected_owner = "adapter"
        elif any(
            marker in owner_text
            for marker in (
                "state-readiness",
                "loaded-state",
                "server-cancellation",
                "server-side",
                "crash-safe",
                "idempotency",
            )
        ):
            expected_owner = "biomem"
        if expected_owner is not None and owner != expected_owner:
            errors.append(
                f"{label}.owner_boundary must be {expected_owner} for {blocker_id!r}"
            )
        evidence_references(row, label, evidence, errors, required=True)
    missing_owners = sorted({"adapter", "biomem"} - owners)
    if missing_owners:
        errors.append(
            "production blockers must be split between adapter and biomem; "
            f"missing {missing_owners!r}"
        )
    if blocker_ids != set(PRODUCTION_BLOCKERS):
        missing = sorted(set(PRODUCTION_BLOCKERS) - blocker_ids)
        unexpected = sorted(blocker_ids - set(PRODUCTION_BLOCKERS))
        errors.append(
            "production blockers must contain exactly the declared four IDs; "
            f"missing={missing!r}, unexpected={unexpected!r}"
        )
    for blocker in blockers:
        if not isinstance(blocker, dict):
            continue
        blocker_id = blocker.get("id")
        expected_owner = PRODUCTION_BLOCKERS.get(str(blocker_id))
        if (
            expected_owner is not None
            and blocker.get("owner_boundary") != expected_owner
        ):
            errors.append(
                f"production blocker {blocker_id!r} must be owned by {expected_owner}"
            )


def validate_authority(audit: dict[str, Any], errors: list[str]) -> None:
    boundary = require_object(
        audit.get("authority_boundary"), "audit.authority_boundary", errors
    )
    exclusions = require_object(
        boundary.get("exclusions"), "audit.authority_boundary.exclusions", errors
    )
    for field in REQUIRED_AUTHORITY_EXCLUSIONS:
        if exclusions.get(field) is not False:
            errors.append(f"audit.authority_boundary.exclusions.{field} must be false")

    raw_assigned = boundary.get("ncm_assigned_authorities")
    if raw_assigned is None:
        assigned = [
            non_empty_string(
                boundary.get("ncm_role"), "audit.authority_boundary.ncm_role", errors
            )
        ]
    else:
        assigned = require_list(
            raw_assigned,
            "audit.authority_boundary.ncm_assigned_authorities",
            errors,
        )
    for offset, value in enumerate(assigned):
        authority = non_empty_string(
            value,
            f"audit.authority_boundary.ncm_assigned_authorities[{offset}]",
            errors,
        ).lower()
        normalized = " ".join(re.findall(r"[a-z0-9]+", authority))
        for forbidden in FORBIDDEN_NCM_AUTHORITY_TERMS:
            normalized_forbidden = " ".join(re.findall(r"[a-z0-9]+", forbidden))
            pattern = rf"(?:^| ){re.escape(normalized_forbidden)}(?: |$)"
            if re.search(pattern, normalized):
                errors.append(
                    "NCM must not own Git, code-navigation, or TraceDecay-storage "
                    f"authority: {authority!r} contains {forbidden!r}"
                )

    raw_retained = boundary.get("tracedecay_retained_authorities")
    if raw_retained is None:
        retained = [
            non_empty_string(
                boundary.get("tracedecay_role"),
                "audit.authority_boundary.tracedecay_role",
                errors,
            )
        ]
    else:
        retained = require_list(
            raw_retained,
            "audit.authority_boundary.tracedecay_retained_authorities",
            errors,
        )
    retained_values: list[str] = []
    for offset, value in enumerate(retained):
        retained_values.append(
            non_empty_string(
                value,
                f"audit.authority_boundary.tracedecay_retained_authorities[{offset}]",
                errors,
            ).lower()
        )
    retained_text = " ".join(retained_values)
    if any(
        negation in retained_text
        for negation in (
            "does not retain",
            "not retained",
            "without authority",
            "no authority",
        )
    ):
        errors.append("TraceDecay retained-authority statement must not be negated")
    retained_markers = {
        "Git/repository/worktree authority": ("git", "repository", "worktree"),
        "code-navigation authority": ("code navigation", "current code", "code graph"),
        "TraceDecay storage authority": (
            "tracedecay storage",
            "native",
            "canonical facts",
        ),
    }
    for authority, markers in retained_markers.items():
        if not any(marker in retained_text for marker in markers):
            errors.append(
                "audit.authority_boundary must explicitly retain "
                f"{authority} in TraceDecay"
            )


def validate_document(
    audit: dict[str, Any], registry: dict[str, Any]
) -> tuple[list[str], dict[str, int]]:
    errors: list[str] = []
    canonical = canonical_capabilities(registry, errors)
    registry_mandatory = {
        capability_id
        for capability_id, requirement in canonical.items()
        if requirement == "mandatory"
    }
    if registry_mandatory != set(MANDATORY_OPERATIONS):
        errors.append(
            "mandatory operation mapping is out of sync with the canonical registry: "
            f"registry={sorted(registry_mandatory)!r}, "
            f"checker={sorted(MANDATORY_OPERATIONS)!r}"
        )
    evidence = evidence_index(audit, errors)
    validate_pinned_identity(audit, evidence, errors)
    surface_measurements = validate_surface_probe_projection(evidence, errors)
    matrix = validate_capabilities(audit, canonical, evidence, errors)
    validate_mandatory_operations(audit, matrix, evidence, errors)
    validate_persistence(audit, evidence, errors)
    validate_surface_measurements(audit, surface_measurements, errors)
    validate_threading_and_cancellation(audit, evidence, errors)
    validate_production_gate(audit, evidence, errors)
    validate_authority(audit, errors)
    orphaned_evidence = sorted(set(evidence) - document_evidence_references(audit))
    if orphaned_evidence:
        errors.append(f"audit contains unreferenced evidence: {orphaned_evidence!r}")
    return errors, {
        "canonical_capabilities": len(canonical),
        "classified_capabilities": len(matrix),
        "evidence_items": len(evidence),
    }


def main() -> int:
    args = parse_args()
    errors: list[str] = []
    audit = load_object(resolve(args.repo, args.audit), "audit", errors)
    registry = load_object(resolve(args.repo, args.registry), "registry", errors)
    if not errors:
        document_errors, counts = validate_document(audit, registry)
        errors.extend(document_errors)
    else:
        counts = {}
    if errors:
        for error in errors:
            print(error, file=sys.stderr)
        return 1
    print(
        "NCM surface audit valid: "
        f"{counts['classified_capabilities']} capabilities, "
        f"{counts['evidence_items']} evidence items"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
