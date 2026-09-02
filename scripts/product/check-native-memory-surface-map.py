#!/usr/bin/env python3
"""Validate the versioned TraceDecay Native memory production surface map."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any, Iterable

EXPECTED_OPERATIONS = {
    "fact_store_curate",
    "fact_store_add",
    "fact_store_search",
    "fact_store_probe",
    "fact_store_related",
    "fact_store_reason",
    "fact_store_contradict",
    "fact_store_get",
    "fact_store_update",
    "fact_store_remove",
    "fact_store_list",
    "fact_feedback",
    "memory_status",
}

EXPECTED_PAGINATED = {
    "fact_store_search",
    "fact_store_probe",
    "fact_store_related",
    "fact_store_reason",
    "fact_store_list",
}

EXPECTED_DIRECTIONS = {
    "fact_store_curate": "write",
    "fact_store_add": "write",
    "fact_store_search": "read",
    "fact_store_probe": "read",
    "fact_store_related": "read",
    "fact_store_reason": "read",
    "fact_store_contradict": "read",
    "fact_store_get": "read",
    "fact_store_update": "write",
    "fact_store_remove": "write",
    "fact_store_list": "read",
    "fact_feedback": "write",
    "memory_status": "read",
}

EXPECTED_READ_AUTHORITIES = {
    "fact_store_curate": "native_explicit_fact_log",
    "fact_store_add": "native_explicit_fact_log",
    "fact_store_search": "native_search_projection",
    "fact_store_probe": "native_search_projection",
    "fact_store_related": "native_relation_graph",
    "fact_store_reason": "native_search_projection",
    "fact_store_contradict": "native_search_projection",
    "fact_store_get": "native_explicit_fact_log",
    "fact_store_update": "native_explicit_fact_log",
    "fact_store_remove": "native_explicit_fact_log",
    "fact_store_list": "native_explicit_fact_log",
    "fact_feedback": "native_feedback_log",
    "memory_status": "native_status_projection",
}

EXPECTED_AUTHORITIES = {
    "native_explicit_fact_log",
    "native_feedback_log",
    "automatic_fact_receipts",
    "session_observation_store",
    "native_relation_graph",
    "native_search_projection",
    "native_status_projection",
}

EXPECTED_INTERNAL_ENTRIES = {
    "daemon_retained_memory_owner",
    "dashboard_holographic_inspection",
    "memory_curator_automation",
    "automatic_fact_promotion",
    "host_observation_ingest",
    "session_context_retrieval",
    "native_fact_retrieval_and_tracking",
    "privacy_remediation",
    "store_maintenance",
    "memory_graph_reconciliation",
    "sdk_operation_client",
}

EXPECTED_DERIVED_SURFACES = {
    "grafeo_memory_relation_graph",
    "fts_and_ranking_scores",
    "fhrr_vectors",
    "dashboard_memory_payloads",
    "trust_and_feedback_summary",
    "automatic_fact_and_automation_views",
}

SOURCE_MARKERS = {
    "crates/tracedecay-session-memory/src/memory/mod.rs": [
        "memory_application_for_db",
        "pub struct MemoryApplication",
    ],
    "crates/tracedecay-runtime-core/src/store/memory/mod.rs": [
        "pub struct DatabaseFactStore",
        "impl ProjectMemoryFactStore",
        "schedule_project_memory_graph_reconciliation",
    ],
    "crates/tracedecay/src/tracedecay/facts.rs": [
        "project_memory_owner",
        "project_memory_application",
    ],
    "crates/tracedecay/src/daemon/retained_owner/memory.rs": [
        "DirectRetainedMemoryPortV1",
        "RetainedMemoryExecutionPortV1",
    ],
    "crates/tracedecay-application/src/retained_surfaces.rs": [
        "FactStoreAdd",
        "FactStoreSearch",
        "FactFeedback",
        "MemoryStatus",
    ],
    "crates/tracedecay-cli/src/tool_command.rs": [
        "tracedecay_fact_store_add",
        "tracedecay_fact_feedback",
        "tracedecay_memory_status",
    ],
    "crates/tracedecay-sdk/src/operations.rs": [
        "operation.application.fact_store_add",
        "operation.application.fact_feedback",
        "operation.application.memory_status",
    ],
    "crates/tracedecay/src/daemon/dashboard_automation/retained_curator.rs": [
        "execute_retained_memory_curator",
        "run_memory_curator_with_backend_for_retained_settlement",
    ],
    "crates/tracedecay/src/mcp/tools/handlers/hook_runtime/ingest.rs": [
        "HostAdmissionFacade",
        "transcript_capture_kernel",
    ],
    "crates/tracedecay-session-runtime/src/session_retrieval/admitted.rs": [
        "SessionApplicationRetrievalPortV1",
        "retrieve_admitted_with_cancellation",
    ],
    "crates/tracedecay-dashboard-api/src/lib.rs": [
        "memory_owner",
        "mem_db",
    ],
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--repo", type=Path, default=Path("."))
    parser.add_argument(
        "--map",
        dest="map_path",
        type=Path,
        default=Path("product/architecture/native-memory-surface-map.json"),
    )
    return parser.parse_args()


def as_list(value: Any, field: str, errors: list[str]) -> list[Any]:
    if not isinstance(value, list):
        errors.append(f"{field} must be an array")
        return []
    return value


def index_by_id(rows: Iterable[Any], field: str, errors: list[str]) -> dict[str, dict[str, Any]]:
    indexed: dict[str, dict[str, Any]] = {}
    for offset, raw in enumerate(rows):
        if not isinstance(raw, dict):
            errors.append(f"{field}[{offset}] must be an object")
            continue
        item_id = raw.get("id")
        if not isinstance(item_id, str) or not item_id:
            errors.append(f"{field}[{offset}].id must be a non-empty string")
            continue
        if item_id in indexed:
            errors.append(f"{field} contains duplicate id {item_id!r}")
            continue
        indexed[item_id] = raw
    return indexed


def path_values(document: dict[str, Any]) -> set[str]:
    values: set[str] = set()

    def visit(value: Any, key: str | None = None) -> None:
        if isinstance(value, dict):
            for child_key, child in value.items():
                visit(child, child_key)
        elif isinstance(value, list):
            if key in {"source_paths", "mount_paths", "tests"}:
                for child in value:
                    if isinstance(child, str):
                        values.add(child)
            else:
                for child in value:
                    visit(child, key)

    visit(document)
    verification = document.get("verification")
    if isinstance(verification, dict):
        for key in ("checker", "baseline_receipt"):
            value = verification.get(key)
            if isinstance(value, str):
                values.add(value)
    return values


def resolve_document_path(repo: Path, configured: Path) -> Path:
    return configured if configured.is_absolute() else repo / configured


def validate_paths(repo: Path, document: dict[str, Any], errors: list[str]) -> None:
    for raw in sorted(path_values(document)):
        candidate = Path(raw)
        if candidate.is_absolute() or ".." in candidate.parts:
            errors.append(f"source path must be repository-relative: {raw}")
            continue
        if not (repo / candidate).exists():
            errors.append(f"referenced source path does not exist: {raw}")

    for raw, markers in SOURCE_MARKERS.items():
        path = repo / raw
        if not path.is_file():
            errors.append(f"marker source is unavailable: {raw}")
            continue
        try:
            text = path.read_text(encoding="utf-8")
        except OSError as exc:
            errors.append(f"could not read marker source {raw}: {exc}")
            continue
        for marker in markers:
            if marker not in text:
                errors.append(f"{raw} is missing expected production marker {marker!r}")

    retained = repo / "crates/tracedecay-application/src/retained_surfaces.rs"
    cli = repo / "crates/tracedecay-cli/src/tool_command.rs"
    sdk = repo / "crates/tracedecay-sdk/src/operations.rs"
    if retained.is_file() and cli.is_file() and sdk.is_file():
        retained_text = retained.read_text(encoding="utf-8")
        cli_text = cli.read_text(encoding="utf-8")
        sdk_text = sdk.read_text(encoding="utf-8")
        for operation in sorted(EXPECTED_OPERATIONS):
            if f'"{operation}"' not in retained_text:
                errors.append(f"retained catalog does not expose {operation}")
            if f"tracedecay_{operation}" not in cli_text:
                errors.append(f"dynamic CLI does not own tracedecay_{operation}")
            if f"operation.application.{operation}" not in sdk_text:
                errors.append(f"SDK descriptor is missing operation.application.{operation}")


def validate_authorities(document: dict[str, Any], errors: list[str]) -> dict[str, dict[str, Any]]:
    rows = as_list(document.get("authorities"), "authorities", errors)
    indexed = index_by_id(rows, "authorities", errors)
    missing = EXPECTED_AUTHORITIES - indexed.keys()
    extra = indexed.keys() - EXPECTED_AUTHORITIES
    if missing:
        errors.append(f"authorities missing: {sorted(missing)}")
    if extra:
        errors.append(f"unexpected authorities: {sorted(extra)}")

    for authority_id in ("native_explicit_fact_log", "native_feedback_log"):
        row = indexed.get(authority_id, {})
        if row.get("class") != "canonical":
            errors.append(f"{authority_id} must be canonical")
        if not isinstance(row.get("write_owner"), str) or not row.get("write_owner"):
            errors.append(f"{authority_id} must name one write owner")
        if not isinstance(row.get("read_authority"), str) or not row.get("read_authority"):
            errors.append(f"{authority_id} must name its read authority")

    for authority_id in (
        "native_relation_graph",
        "native_search_projection",
        "native_status_projection",
    ):
        row = indexed.get(authority_id, {})
        if row.get("class") != "derived":
            errors.append(f"{authority_id} must be derived")
        if row.get("rebuildable") is not True:
            errors.append(f"{authority_id} must be explicitly rebuildable")

    if indexed.get("session_observation_store", {}).get("class") != "canonical_separate_domain":
        errors.append("session_observation_store must remain a separate canonical domain")
    if indexed.get("automatic_fact_receipts", {}).get("class") != "canonical_receipt":
        errors.append("automatic_fact_receipts must remain a receipt authority")
    return indexed


def validate_operations(
    document: dict[str, Any],
    authorities: dict[str, dict[str, Any]],
    errors: list[str],
) -> None:
    rows = as_list(document.get("public_operations"), "public_operations", errors)
    indexed = index_by_id(rows, "public_operations", errors)
    missing = EXPECTED_OPERATIONS - indexed.keys()
    extra = indexed.keys() - EXPECTED_OPERATIONS
    if missing:
        errors.append(f"public operations missing: {sorted(missing)}")
    if extra:
        errors.append(f"unexpected public operations: {sorted(extra)}")

    for operation_id in sorted(EXPECTED_OPERATIONS):
        row = indexed.get(operation_id)
        if row is None:
            continue
        direction = EXPECTED_DIRECTIONS[operation_id]
        if row.get("direction") != direction:
            errors.append(f"{operation_id}.direction must be {direction}")
        if row.get("canonical_mutation") is not (direction == "write"):
            errors.append(f"{operation_id}.canonical_mutation disagrees with direction")
        if row.get("paginated") is not (operation_id in EXPECTED_PAGINATED):
            errors.append(f"{operation_id}.paginated has the wrong contract")
        expected_effect = "administrative" if direction == "write" else "read"
        if row.get("effect_class") != expected_effect:
            errors.append(f"{operation_id}.effect_class must be {expected_effect}")

        surfaces = row.get("surfaces")
        if not isinstance(surfaces, dict):
            errors.append(f"{operation_id}.surfaces must be an object")
            continue
        expected_surfaces = {
            "http": f"/application/retained/{operation_id}",
            "mcp": f"tracedecay_{operation_id}",
            "cli": f"tracedecay tool {operation_id}",
            "sdk": f"operation.application.{operation_id}",
        }
        if surfaces != expected_surfaces:
            errors.append(f"{operation_id}.surfaces do not match the retained catalog")

        read_authority = row.get("read_authority")
        if read_authority != EXPECTED_READ_AUTHORITIES[operation_id]:
            errors.append(
                f"{operation_id}.read_authority must be "
                f"{EXPECTED_READ_AUTHORITIES[operation_id]}"
            )
        if read_authority not in authorities:
            errors.append(f"{operation_id} references unknown read authority {read_authority!r}")
        write_owner = row.get("write_owner")
        if write_owner not in authorities:
            errors.append(f"{operation_id} references unknown write owner {write_owner!r}")

        if operation_id == "fact_feedback" and write_owner != "native_feedback_log":
            errors.append("fact_feedback must be written only by native_feedback_log")
        elif operation_id != "fact_feedback" and write_owner != "native_explicit_fact_log":
            errors.append(f"{operation_id} must retain native_explicit_fact_log as write owner")


def validate_internal_entries(document: dict[str, Any], errors: list[str]) -> None:
    rows = as_list(document.get("internal_entry_points"), "internal_entry_points", errors)
    indexed = index_by_id(rows, "internal_entry_points", errors)
    missing = EXPECTED_INTERNAL_ENTRIES - indexed.keys()
    extra = indexed.keys() - EXPECTED_INTERNAL_ENTRIES
    if missing:
        errors.append(f"internal entry points missing: {sorted(missing)}")
    if extra:
        errors.append(f"unexpected internal entry points: {sorted(extra)}")
    for entry_id, row in indexed.items():
        for field in ("category", "effect", "entry", "write_owner", "read_authority", "notes"):
            if not isinstance(row.get(field), str) or not row.get(field):
                errors.append(f"{entry_id}.{field} must be a non-empty string")
        if not isinstance(row.get("canonical_mutation"), bool):
            errors.append(f"{entry_id}.canonical_mutation must be boolean")

    if indexed.get("host_observation_ingest", {}).get("canonical_mutation") is not False:
        errors.append("host_observation_ingest must not mutate canonical explicit facts")
    if indexed.get("memory_graph_reconciliation", {}).get("canonical_mutation") is not False:
        errors.append("memory_graph_reconciliation must remain derived")
    if indexed.get("privacy_remediation", {}).get("canonical_mutation") is not True:
        errors.append("privacy_remediation must name its bounded canonical mutation")


def validate_derived_surfaces(
    document: dict[str, Any],
    authorities: dict[str, dict[str, Any]],
    errors: list[str],
) -> None:
    rows = as_list(document.get("derived_surfaces"), "derived_surfaces", errors)
    indexed = index_by_id(rows, "derived_surfaces", errors)
    missing = EXPECTED_DERIVED_SURFACES - indexed.keys()
    extra = indexed.keys() - EXPECTED_DERIVED_SURFACES
    if missing:
        errors.append(f"derived surfaces missing: {sorted(missing)}")
    if extra:
        errors.append(f"unexpected derived surfaces: {sorted(extra)}")
    for surface_id, row in indexed.items():
        if row.get("rebuildable") is not True:
            errors.append(f"{surface_id} must be rebuildable")
        if row.get("canonical") is not False:
            errors.append(f"{surface_id} must be marked non-canonical")
        authority = row.get("authority")
        if authority not in authorities:
            errors.append(f"{surface_id} references unknown authority {authority!r}")
        elif authorities[authority].get("class") not in {"derived", "canonical_receipt"}:
            errors.append(
                f"{surface_id} must derive from a derived or receipt authority, not {authority}"
            )


def validate_seams(document: dict[str, Any], errors: list[str]) -> None:
    rows = as_list(document.get("provider_seams"), "provider_seams", errors)
    indexed = index_by_id(rows, "provider_seams", errors)
    ranks = sorted(row.get("rank") for row in indexed.values() if isinstance(row.get("rank"), int))
    if ranks != [1, 2, 3, 4, 5, 6]:
        errors.append("provider seam ranks must be exactly 1 through 6")

    by_rank = {
        row.get("rank"): row
        for row in indexed.values()
        if isinstance(row.get("rank"), int)
    }
    expected_ids = {
        1: "normalized_observation_fanout",
        2: "advisory_recall_contributor",
        3: "outcome_and_feedback_fanout",
        4: "capability_registry_and_daemon_composition",
        5: "project_memory_fact_store_replacement",
        6: "transport_or_projection_provider_branching",
    }
    for rank, seam_id in expected_ids.items():
        if by_rank.get(rank, {}).get("id") != seam_id:
            errors.append(f"provider seam rank {rank} must be {seam_id}")

    if by_rank.get(1, {}).get("recommendation") != "preferred_observer_mount":
        errors.append("rank 1 must be the preferred observer mount")
    if by_rank.get(2, {}).get("recommendation") != "preferred_active_read_mount":
        errors.append("rank 2 must be the preferred active-read mount")
    if by_rank.get(5, {}).get("recommendation") != "rejected":
        errors.append("ProjectMemoryFactStore replacement must be rejected")
    if by_rank.get(6, {}).get("recommendation") != "forbidden":
        errors.append("transport/provider-name branching must be forbidden")
    for rank, row in by_rank.items():
        if not isinstance(row.get("rationale"), str) or not row.get("rationale"):
            errors.append(f"provider seam rank {rank} must explain its rationale")
        prohibited = row.get("prohibited_side_effects")
        if not isinstance(prohibited, list) or not prohibited:
            errors.append(f"provider seam rank {rank} must list prohibited side effects")


def validate_document(repo: Path, document: dict[str, Any]) -> list[str]:
    errors: list[str] = []
    if document.get("schema_version") != 1:
        errors.append("schema_version must be 1")
    if document.get("bead_id") != "tdmem-0103":
        errors.append("bead_id must be tdmem-0103")

    invariants = as_list(document.get("invariants"), "invariants", errors)
    joined_invariants = "\n".join(value for value in invariants if isinstance(value, str))
    for marker in (
        "one canonical writer",
        "advisory",
        "separate authorities",
        "derived and rebuildable",
        "deadline/cancellation",
    ):
        if marker not in joined_invariants:
            errors.append(f"invariants are missing required statement containing {marker!r}")

    authorities = validate_authorities(document, errors)
    validate_operations(document, authorities, errors)
    validate_internal_entries(document, errors)
    validate_derived_surfaces(document, authorities, errors)
    validate_seams(document, errors)
    validate_paths(repo, document, errors)
    return errors


def main() -> int:
    args = parse_args()
    repo = args.repo.resolve()
    map_path = resolve_document_path(repo, args.map_path)
    try:
        document = json.loads(map_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        print(json.dumps({"ok": False, "errors": [f"could not load map: {exc}"]}))
        return 1
    if not isinstance(document, dict):
        print(json.dumps({"ok": False, "errors": ["map root must be an object"]}))
        return 1

    errors = validate_document(repo, document)
    if errors:
        print(json.dumps({"ok": False, "errors": errors}, indent=2, sort_keys=True))
        return 1

    receipt = {
        "ok": True,
        "schema_version": document["schema_version"],
        "bead_id": document["bead_id"],
        "authorities": len(document["authorities"]),
        "public_operations": len(document["public_operations"]),
        "internal_entry_points": len(document["internal_entry_points"]),
        "derived_surfaces": len(document["derived_surfaces"]),
        "provider_seams": len(document["provider_seams"]),
        "map": str(map_path.relative_to(repo)),
    }
    print(json.dumps(receipt, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    sys.exit(main())
