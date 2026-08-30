#!/usr/bin/env python3
"""Validate the upstream patch budget, convergence map, and dependency directions."""

from __future__ import annotations

import argparse
import fnmatch
import json
import subprocess
import sys
import tomllib
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any, Iterable

EXPECTED_FLOOR = "08fbe33a7c7f403191fd5d6e356c7b6681b96403"
EXPECTED_POLICY_REVISION = "patch-footprint.v1"
EXPECTED_BUDGET = {
    "max_upstream_existing_production_files": 12,
    "max_upstream_existing_test_or_fixture_files": 6,
    "max_total_upstream_changed_lines": 900,
    "max_changed_lines_per_upstream_file": 180,
    "max_composition_root_files": 6,
    "max_allowed_touch_point_files_per_category": 3,
    "default_max_exception_zone_files": 0,
    "max_exception_files_per_adr": 2,
    "max_workspace_manifest_files": 2,
    "manual_generated_file_edits": 0,
}
EXPECTED_PRODUCT_PATTERNS = {
    ".beads/**",
    "product/**",
    "scripts/product/**",
    "scripts/check-product-upstream-floor.py",
    "tests/product_*",
    ".github/workflows/apply-beads-operation.yml",
    ".github/workflows/materialize-beads.yml",
    ".github/workflows/product-*.yml",
    "crates/tracedecay-memory-provider-api/**",
    "crates/tracedecay-memory-provider-registry/**",
    "crates/tracedecay-memory-provider-native/**",
    "crates/tracedecay-memory-provider-ncm/**",
    "crates/tracedecay-memory-observation/**",
    "crates/tracedecay-memory-context/**",
    "crates/tracedecay-memory-conformance/**",
    "crates/tracedecay/tests/product_memory_provider/**",
    "crates/tracedecay/tests/product_memory_provider_*.rs",
}
EXPECTED_TOUCH_POINTS = {
    "workspace_wiring",
    "application_contract_mount",
    "daemon_composition_mount",
    "normalized_observation_mount",
    "recall_context_mount",
    "post_settlement_feedback_mount",
    "configuration_registry_mount",
}
EXPECTED_EXCEPTION_ZONES = {
    "native_database_internals",
    "code_index_internals",
    "generated_contracts",
    "host_specific_adapters",
    "toolchain_build_and_ci_policy",
}
EXPECTED_DEPENDENCY_RULES = {
    "provider_api_is_inward",
    "context_compiler_is_provider_neutral",
    "adapters_do_not_depend_on_each_other",
    "ncm_adapter_does_not_reach_native_store",
    "transports_are_adapter_blind",
    "upstream_crates_do_not_import_concrete_adapters",
}
EXPECTED_MAP_FIELDS = {
    "path",
    "touch_point",
    "rationale",
    "semantic_invariants",
    "verification",
    "bead_ids",
    "line_budget",
    "rebase_or_removal_plan",
    "status",
}
EXPECTED_EXCEPTION_FIELDS = {
    "zone",
    "adr",
    "why_unavoidable",
    "alternatives_rejected",
    "policy_revision",
    "rollback_plan",
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--repo", type=Path, default=Path("."))
    parser.add_argument(
        "--policy",
        type=Path,
        default=Path("product/upstream/patch-footprint-policy.json"),
    )
    parser.add_argument(
        "--map",
        dest="map_path",
        type=Path,
        default=Path("product/upstream/convergence-map.json"),
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


def require_list(value: Any, field: str, errors: list[str]) -> list[Any]:
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
        row_id = raw.get("id")
        if not isinstance(row_id, str) or not row_id:
            errors.append(f"{field}[{offset}].id must be a non-empty string")
            continue
        if row_id in indexed:
            errors.append(f"{field} contains duplicate id {row_id!r}")
            continue
        indexed[row_id] = raw
    return indexed


def non_empty_string(row: dict[str, Any], field: str, label: str, errors: list[str]) -> str:
    value = row.get(field)
    if not isinstance(value, str) or not value.strip():
        errors.append(f"{label}.{field} must be a non-empty string")
        return ""
    return value.strip()


def pattern_matches(path: str, pattern: str) -> bool:
    return fnmatch.fnmatchcase(path, pattern)


def matches_any(path: str, patterns: Iterable[str]) -> bool:
    return any(pattern_matches(path, pattern) for pattern in patterns)


def git(repo: Path, *args: str, check: bool = True) -> subprocess.CompletedProcess[str]:
    result = subprocess.run(
        ["git", *args],
        cwd=repo,
        check=False,
        capture_output=True,
        text=True,
    )
    if check and result.returncode != 0:
        raise RuntimeError(
            f"git {' '.join(args)} failed ({result.returncode}): {result.stderr.strip()}"
        )
    return result


def validate_floor(repo: Path, policy: dict[str, Any], convergence: dict[str, Any], errors: list[str]) -> str:
    upstream = policy.get("upstream_floor")
    if not isinstance(upstream, dict):
        errors.append("upstream_floor must be an object")
        return EXPECTED_FLOOR
    floor = upstream.get("sha")
    if floor != EXPECTED_FLOOR:
        errors.append(f"upstream floor must remain {EXPECTED_FLOOR}")
        floor = EXPECTED_FLOOR
    if upstream.get("repository") != "ScriptedAlchemy/tracedecay":
        errors.append("upstream repository must be ScriptedAlchemy/tracedecay")
    if upstream.get("pull_request") != 707:
        errors.append("upstream pull request must be 707")
    metadata_raw = upstream.get("metadata")
    if not isinstance(metadata_raw, str):
        errors.append("upstream_floor.metadata must be a path")
    else:
        metadata = load_object(repo / metadata_raw, "upstream metadata", errors)
        pinned = metadata.get("pinned_floor") if isinstance(metadata, dict) else None
        if not isinstance(pinned, dict) or pinned.get("sha") != EXPECTED_FLOOR:
            errors.append("upstream metadata pinned_floor does not match patch policy")

    if convergence.get("upstream_floor_sha") != EXPECTED_FLOOR:
        errors.append("convergence map floor does not match patch policy")
    if convergence.get("policy_revision") != EXPECTED_POLICY_REVISION:
        errors.append("convergence map policy revision does not match patch policy")

    try:
        result = git(repo, "merge-base", "--is-ancestor", EXPECTED_FLOOR, "HEAD", check=False)
    except OSError as exc:
        errors.append(f"could not execute git ancestry check: {exc}")
    else:
        if result.returncode != 0:
            errors.append("pinned upstream floor is not an ancestor of HEAD")
    return str(floor)


def validate_policy_structure(policy: dict[str, Any], errors: list[str]) -> tuple[
    dict[str, dict[str, Any]], dict[str, dict[str, Any]], dict[str, dict[str, Any]]
]:
    if policy.get("schema_version") != 1:
        errors.append("policy schema_version must be 1")
    if policy.get("bead_id") != "tdmem-0105":
        errors.append("policy bead_id must be tdmem-0105")
    if policy.get("policy_revision") != EXPECTED_POLICY_REVISION:
        errors.append(f"policy_revision must be {EXPECTED_POLICY_REVISION}")
    for field in ("title", "scope"):
        non_empty_string(policy, field, "policy", errors)

    principles = require_list(policy.get("principles"), "principles", errors)
    principle_text = "\n".join(value for value in principles if isinstance(value, str))
    for marker in (
        "Add product-owned crates",
        "Every intentional edit",
        "Provider names",
        "Database internals",
        "ADR",
    ):
        if marker not in principle_text:
            errors.append(f"policy principles are missing {marker!r}")

    product_patterns = require_list(
        policy.get("product_owned_paths"), "product_owned_paths", errors
    )
    if any(not isinstance(value, str) for value in product_patterns):
        errors.append("product_owned_paths entries must be strings")
    pattern_set = {value for value in product_patterns if isinstance(value, str)}
    missing_patterns = EXPECTED_PRODUCT_PATTERNS - pattern_set
    extra_patterns = pattern_set - EXPECTED_PRODUCT_PATTERNS
    if missing_patterns:
        errors.append(f"product-owned path patterns missing: {sorted(missing_patterns)}")
    if extra_patterns:
        errors.append(f"unexpected/broad product-owned path patterns: {sorted(extra_patterns)}")
    for forbidden_broad in ("crates/**", "crates/tracedecay/**", "tests/**", ".github/**"):
        if forbidden_broad in pattern_set:
            errors.append(f"product-owned paths must not hide upstream tree {forbidden_broad!r}")

    budget = policy.get("initial_budget")
    if not isinstance(budget, dict):
        errors.append("initial_budget must be an object")
    else:
        for key, expected in EXPECTED_BUDGET.items():
            if budget.get(key) != expected:
                errors.append(f"initial_budget.{key} must be {expected}")
        notes = require_list(budget.get("notes"), "initial_budget.notes", errors)
        note_text = "\n".join(value for value in notes if isinstance(value, str))
        for marker in ("product_owned_paths", "Renaming", "additions plus deletions", "Cargo.lock"):
            if marker not in note_text:
                errors.append(f"initial budget notes are missing {marker!r}")

    touch_rows = require_list(policy.get("allowed_touch_points"), "allowed_touch_points", errors)
    touches = index_by_id(touch_rows, "allowed_touch_points", errors)
    missing_touches = EXPECTED_TOUCH_POINTS - touches.keys()
    extra_touches = touches.keys() - EXPECTED_TOUCH_POINTS
    if missing_touches:
        errors.append(f"allowed touch points missing: {sorted(missing_touches)}")
    if extra_touches:
        errors.append(f"unexpected allowed touch points: {sorted(extra_touches)}")
    for touch_id, row in touches.items():
        non_empty_string(row, "category", touch_id, errors)
        paths = require_list(row.get("paths"), f"{touch_id}.paths", errors)
        if not paths or any(not isinstance(value, str) for value in paths):
            errors.append(f"{touch_id}.paths must contain strings")
        for cap in ("max_files", "max_changed_lines"):
            value = row.get(cap)
            if not isinstance(value, int) or isinstance(value, bool) or value <= 0:
                errors.append(f"{touch_id}.{cap} must be a positive integer")
        for field in ("allowed_changes", "forbidden_changes", "required_verification"):
            values = require_list(row.get(field), f"{touch_id}.{field}", errors)
            if not values:
                errors.append(f"{touch_id}.{field} must not be empty")

    zone_rows = require_list(policy.get("exception_zones"), "exception_zones", errors)
    zones = index_by_id(zone_rows, "exception_zones", errors)
    missing_zones = EXPECTED_EXCEPTION_ZONES - zones.keys()
    extra_zones = zones.keys() - EXPECTED_EXCEPTION_ZONES
    if missing_zones:
        errors.append(f"exception zones missing: {sorted(missing_zones)}")
    if extra_zones:
        errors.append(f"unexpected exception zones: {sorted(extra_zones)}")
    for zone_id, row in zones.items():
        paths = require_list(row.get("paths"), f"{zone_id}.paths", errors)
        if not paths or any(not isinstance(value, str) for value in paths):
            errors.append(f"{zone_id}.paths must contain strings")
        if row.get("default_policy") not in {"forbidden", "generated_only"}:
            errors.append(f"{zone_id}.default_policy must be forbidden or generated_only")
        non_empty_string(row, "reason", zone_id, errors)
        evidence = require_list(
            row.get("required_exception_evidence"),
            f"{zone_id}.required_exception_evidence",
            errors,
        )
        if not evidence or not any("ADR" in str(value) for value in evidence):
            errors.append(f"{zone_id} must require ADR evidence")

    dependency_rows = require_list(
        policy.get("dependency_direction_rules"),
        "dependency_direction_rules",
        errors,
    )
    dependencies = index_by_id(dependency_rows, "dependency_direction_rules", errors)
    missing_rules = EXPECTED_DEPENDENCY_RULES - dependencies.keys()
    extra_rules = dependencies.keys() - EXPECTED_DEPENDENCY_RULES
    if missing_rules:
        errors.append(f"dependency direction rules missing: {sorted(missing_rules)}")
    if extra_rules:
        errors.append(f"unexpected dependency direction rules: {sorted(extra_rules)}")
    for rule_id, row in dependencies.items():
        from_packages = require_list(row.get("from_packages"), f"{rule_id}.from_packages", errors)
        forbidden = require_list(
            row.get("forbidden_dependencies"),
            f"{rule_id}.forbidden_dependencies",
            errors,
        )
        if not from_packages or not forbidden:
            errors.append(f"{rule_id} must define source and forbidden package patterns")
        non_empty_string(row, "reason", rule_id, errors)

    convergence_contract = policy.get("convergence_map")
    if not isinstance(convergence_contract, dict):
        errors.append("convergence_map policy contract must be an object")
    else:
        if convergence_contract.get("path") != "product/upstream/convergence-map.json":
            errors.append("convergence_map.path is not canonical")
        if convergence_contract.get("entry_required_for_every_upstream_existing_file") is not True:
            errors.append("every upstream existing-file edit must require a convergence entry")
        required = set(
            value
            for value in require_list(
                convergence_contract.get("required_entry_fields"),
                "convergence_map.required_entry_fields",
                errors,
            )
            if isinstance(value, str)
        )
        if required != EXPECTED_MAP_FIELDS:
            errors.append("convergence-map required fields do not match the entry contract")
        exception_required = set(
            value
            for value in require_list(
                convergence_contract.get("exception_required_fields"),
                "convergence_map.exception_required_fields",
                errors,
            )
            if isinstance(value, str)
        )
        if exception_required != EXPECTED_EXCEPTION_FIELDS:
            errors.append("convergence-map exception fields do not match the exception contract")

    return touches, zones, dependencies


def validate_declared_paths(repo: Path, policy: dict[str, Any], errors: list[str]) -> None:
    for row in policy.get("allowed_touch_points", []):
        if not isinstance(row, dict):
            continue
        for raw in row.get("paths", []):
            if not isinstance(raw, str) or any(character in raw for character in "*?["):
                continue
            if not (repo / raw).exists():
                errors.append(f"allowed touch point references missing path: {raw}")

    for raw in (
        "product/upstream/patch-footprint-policy.md",
        "product/upstream/convergence-map.json",
        "scripts/product/check-patch-footprint-policy.py",
        "tests/product_patch_footprint_policy_test.py",
    ):
        if not (repo / raw).exists():
            errors.append(f"patch-footprint deliverable is missing: {raw}")


def validate_convergence_structure(convergence: dict[str, Any], errors: list[str]) -> dict[str, dict[str, Any]]:
    if convergence.get("schema_version") != 1:
        errors.append("convergence-map schema_version must be 1")
    if convergence.get("bead_id") != "tdmem-0105":
        errors.append("convergence-map bead_id must be tdmem-0105")
    non_empty_string(convergence, "purpose", "convergence map", errors)
    snapshot = convergence.get("snapshot")
    if not isinstance(snapshot, dict):
        errors.append("convergence-map snapshot must be an object")
    contract = convergence.get("entry_contract")
    if not isinstance(contract, dict):
        errors.append("convergence-map entry_contract must be an object")
    else:
        if set(contract.get("status_values", [])) != {"active", "retired"}:
            errors.append("convergence-map status values must be active and retired")
        expected_touch_values = EXPECTED_TOUCH_POINTS | {"exception"}
        if set(contract.get("touch_point_values", [])) != expected_touch_values:
            errors.append("convergence-map touch-point values are incomplete")

    entries = require_list(convergence.get("entries"), "convergence-map entries", errors)
    indexed: dict[str, dict[str, Any]] = {}
    for offset, raw in enumerate(entries):
        if not isinstance(raw, dict):
            errors.append(f"convergence-map entries[{offset}] must be an object")
            continue
        path = raw.get("path")
        if not isinstance(path, str) or not path:
            errors.append(f"convergence-map entries[{offset}].path must be non-empty")
            continue
        if path in indexed:
            errors.append(f"duplicate convergence-map path {path!r}")
            continue
        indexed[path] = raw
        missing = EXPECTED_MAP_FIELDS - raw.keys()
        if missing:
            errors.append(f"convergence entry {path} missing fields: {sorted(missing)}")
        if raw.get("status") not in {"active", "retired"}:
            errors.append(f"convergence entry {path} has invalid status")
        if raw.get("touch_point") not in EXPECTED_TOUCH_POINTS | {"exception"}:
            errors.append(f"convergence entry {path} has invalid touch_point")
        for field in ("rationale", "rebase_or_removal_plan"):
            non_empty_string(raw, field, f"convergence entry {path}", errors)
        for field in ("semantic_invariants", "verification", "bead_ids"):
            values = require_list(raw.get(field), f"convergence entry {path}.{field}", errors)
            if not values:
                errors.append(f"convergence entry {path}.{field} must not be empty")
        line_budget = raw.get("line_budget")
        if not isinstance(line_budget, int) or isinstance(line_budget, bool) or line_budget <= 0:
            errors.append(f"convergence entry {path}.line_budget must be positive")
        if raw.get("touch_point") == "exception":
            exception = raw.get("exception")
            if not isinstance(exception, dict):
                errors.append(f"exception entry {path} must include exception evidence")
            else:
                missing_exception = EXPECTED_EXCEPTION_FIELDS - exception.keys()
                if missing_exception:
                    errors.append(
                        f"exception entry {path} missing evidence: {sorted(missing_exception)}"
                    )
    return indexed


def diff_numstat(repo: Path, floor: str, errors: list[str]) -> dict[str, tuple[int, int]]:
    try:
        result = git(repo, "diff", "--no-renames", "--numstat", f"{floor}...HEAD", "--")
    except (OSError, RuntimeError) as exc:
        errors.append(f"could not read branch diff: {exc}")
        return {}
    rows: dict[str, tuple[int, int]] = {}
    for line in result.stdout.splitlines():
        if not line:
            continue
        parts = line.split("\t", 2)
        if len(parts) != 3:
            errors.append(f"unparseable git numstat line: {line!r}")
            continue
        added_raw, deleted_raw, path = parts
        if added_raw == "-" or deleted_raw == "-":
            errors.append(f"binary upstream/product diff is unsupported: {path}")
            continue
        try:
            rows[path] = (int(added_raw), int(deleted_raw))
        except ValueError:
            errors.append(f"invalid git numstat values for {path}")
    return rows


def is_test_or_fixture(path: str) -> bool:
    name = Path(path).name.lower()
    return (
        path.startswith("tests/")
        or "/tests/" in path
        or "/test/" in path
        or "fixture" in name
        or name.endswith("_test.rs")
        or name.endswith("_tests.rs")
        or name.startswith("test_")
    )


def matching_touch_points(path: str, touches: dict[str, dict[str, Any]]) -> list[str]:
    matches: list[str] = []
    for touch_id, row in touches.items():
        patterns = [value for value in row.get("paths", []) if isinstance(value, str)]
        if matches_any(path, patterns):
            matches.append(touch_id)
    return matches


def matching_exception_zones(path: str, zones: dict[str, dict[str, Any]]) -> list[str]:
    matches: list[str] = []
    for zone_id, row in zones.items():
        patterns = [value for value in row.get("paths", []) if isinstance(value, str)]
        if matches_any(path, patterns):
            matches.append(zone_id)
    return matches


def validate_actual_footprint(
    repo: Path,
    floor: str,
    policy: dict[str, Any],
    convergence: dict[str, Any],
    touches: dict[str, dict[str, Any]],
    zones: dict[str, dict[str, Any]],
    entries: dict[str, dict[str, Any]],
    errors: list[str],
) -> dict[str, int]:
    stats = diff_numstat(repo, floor, errors)
    product_patterns = [
        value for value in policy.get("product_owned_paths", []) if isinstance(value, str)
    ]
    upstream = {
        path: counts
        for path, counts in stats.items()
        if not matches_any(path, product_patterns)
    }
    active_entries = {
        path: row for path, row in entries.items() if row.get("status") == "active"
    }
    retired_entries = {
        path: row for path, row in entries.items() if row.get("status") == "retired"
    }

    for path in sorted(upstream.keys() - active_entries.keys()):
        errors.append(f"upstream-owned changed file lacks active convergence entry: {path}")
    for path in sorted(active_entries.keys() - upstream.keys()):
        errors.append(f"active convergence entry has no current upstream diff: {path}")
    for path in sorted(retired_entries.keys() & upstream.keys()):
        errors.append(f"retired convergence entry cannot authorize current diff: {path}")

    production_files = 0
    test_files = 0
    total_lines = 0
    composition_files = 0
    forbidden_exception_files = 0
    workspace_files = 0
    category_files: Counter[str] = Counter()
    category_lines: Counter[str] = Counter()
    adr_exception_counts: Counter[str] = Counter()

    for path, (added, deleted) in upstream.items():
        changed = added + deleted
        total_lines += changed
        if is_test_or_fixture(path):
            test_files += 1
        else:
            production_files += 1
        entry = active_entries.get(path)
        touch_matches = matching_touch_points(path, touches)
        zone_matches = matching_exception_zones(path, zones)
        if entry is None:
            continue

        line_budget = entry.get("line_budget")
        per_file_cap = EXPECTED_BUDGET["max_changed_lines_per_upstream_file"]
        if isinstance(line_budget, int):
            if line_budget > per_file_cap:
                errors.append(
                    f"convergence entry {path} line budget {line_budget} exceeds global cap {per_file_cap}"
                )
            if changed > line_budget:
                errors.append(
                    f"upstream file {path} changed {changed} lines, exceeding entry budget {line_budget}"
                )
        if changed > per_file_cap:
            errors.append(
                f"upstream file {path} changed {changed} lines, exceeding per-file cap {per_file_cap}"
            )

        touch_point = entry.get("touch_point")
        if touch_point == "exception":
            exception = entry.get("exception")
            if not isinstance(exception, dict):
                continue
            zone = exception.get("zone")
            if zone not in zone_matches:
                errors.append(
                    f"exception entry {path} names zone {zone!r} but path matches {zone_matches}"
                )
            if zone not in zones:
                errors.append(f"exception entry {path} names unknown zone {zone!r}")
            adr = exception.get("adr")
            if isinstance(adr, str):
                adr_exception_counts[adr] += 1
                if not adr.startswith("product/architecture/adr/") or not (repo / adr).is_file():
                    errors.append(f"exception entry {path} ADR is missing or outside product ADRs: {adr}")
            if exception.get("policy_revision") != policy.get("policy_revision"):
                errors.append(f"exception entry {path} uses a different policy revision")
            forbidden_exception_files += 1
        else:
            if touch_point not in touch_matches:
                errors.append(
                    f"convergence entry {path} selects {touch_point!r}; matching touch points are {touch_matches}"
                )
            if not touch_matches:
                errors.append(f"upstream changed file is outside allowed touch points: {path}")
            if zone_matches:
                generated_only = all(
                    zones[zone].get("default_policy") == "generated_only" for zone in zone_matches
                )
                if not generated_only:
                    errors.append(
                        f"upstream changed file {path} is in exception zone(s) {zone_matches} without exception evidence"
                    )
                else:
                    generated = entry.get("generated")
                    if not isinstance(generated, dict):
                        errors.append(f"generated output {path} must record generator/reproduction evidence")
                    else:
                        for field in ("generator_path", "reproduction", "zero_drift_check"):
                            non_empty_string(generated, field, f"generated entry {path}", errors)
            if isinstance(touch_point, str):
                category_files[touch_point] += 1
                category_lines[touch_point] += changed
                if touch_point == "daemon_composition_mount":
                    composition_files += 1
                if touch_point == "workspace_wiring":
                    workspace_files += 1

        required_fields = set(policy.get("convergence_map", {}).get("required_entry_fields", []))
        if required_fields - entry.keys():
            errors.append(f"convergence entry {path} no longer satisfies policy fields")

    budget = policy.get("initial_budget", {})
    cap_checks = [
        (
            "upstream production files",
            production_files,
            budget.get("max_upstream_existing_production_files"),
        ),
        (
            "upstream test/fixture files",
            test_files,
            budget.get("max_upstream_existing_test_or_fixture_files"),
        ),
        (
            "total upstream changed lines",
            total_lines,
            budget.get("max_total_upstream_changed_lines"),
        ),
        (
            "composition-root files",
            composition_files,
            budget.get("max_composition_root_files"),
        ),
        (
            "exception-zone files",
            forbidden_exception_files,
            budget.get("default_max_exception_zone_files"),
        ),
        (
            "workspace manifest files",
            workspace_files,
            budget.get("max_workspace_manifest_files"),
        ),
    ]
    for label, actual, cap in cap_checks:
        if isinstance(cap, int) and actual > cap:
            errors.append(f"{label} {actual} exceeds budget {cap}")

    for touch_id, count in category_files.items():
        row = touches.get(touch_id, {})
        local_cap = row.get("max_files")
        global_cap = budget.get("max_allowed_touch_point_files_per_category")
        effective_caps = [value for value in (local_cap, global_cap) if isinstance(value, int)]
        if effective_caps and count > min(effective_caps):
            errors.append(
                f"touch-point category {touch_id} uses {count} files, exceeding cap {min(effective_caps)}"
            )
        line_cap = row.get("max_changed_lines")
        if isinstance(line_cap, int) and category_lines[touch_id] > line_cap:
            errors.append(
                f"touch-point category {touch_id} changes {category_lines[touch_id]} lines, exceeding cap {line_cap}"
            )

    adr_cap = budget.get("max_exception_files_per_adr")
    if isinstance(adr_cap, int):
        for adr, count in adr_exception_counts.items():
            if count > adr_cap:
                errors.append(f"ADR {adr} authorizes {count} exception files, exceeding cap {adr_cap}")

    snapshot = convergence.get("snapshot")
    computed = {
        "upstream_existing_production_files": production_files,
        "upstream_existing_test_or_fixture_files": test_files,
        "total_upstream_changed_lines": total_lines,
        "composition_root_files": composition_files,
        "exception_zone_files": forbidden_exception_files,
    }
    if isinstance(snapshot, dict):
        for key, value in computed.items():
            if snapshot.get(key) != value:
                errors.append(
                    f"convergence snapshot {key}={snapshot.get(key)!r} does not match actual {value}"
                )
        non_empty_string(snapshot, "observed_state", "convergence snapshot", errors)
    return computed


def dependency_names(manifest: dict[str, Any]) -> set[str]:
    names: set[str] = set()

    def collect(section: Any) -> None:
        if not isinstance(section, dict):
            return
        for key, value in section.items():
            if isinstance(value, dict) and isinstance(value.get("package"), str):
                names.add(value["package"])
            else:
                names.add(key)

    for key in ("dependencies", "dev-dependencies", "build-dependencies"):
        collect(manifest.get(key))
    targets = manifest.get("target")
    if isinstance(targets, dict):
        for target in targets.values():
            if isinstance(target, dict):
                for key in ("dependencies", "dev-dependencies", "build-dependencies"):
                    collect(target.get(key))
    return names


def package_matches(package: str, patterns: Iterable[str]) -> bool:
    return any(fnmatch.fnmatchcase(package, pattern) for pattern in patterns)


def validate_dependency_directions(
    repo: Path,
    rules: dict[str, dict[str, Any]],
    errors: list[str],
) -> int:
    manifests: dict[str, tuple[Path, set[str]]] = {}
    for path in sorted((repo / "crates").glob("*/Cargo.toml")):
        try:
            document = tomllib.loads(path.read_text(encoding="utf-8"))
        except (OSError, tomllib.TOMLDecodeError) as exc:
            errors.append(f"could not parse {path.relative_to(repo)}: {exc}")
            continue
        package = document.get("package")
        name = package.get("name") if isinstance(package, dict) else None
        if isinstance(name, str):
            manifests[name] = (path, dependency_names(document))

    for rule_id, rule in rules.items():
        from_patterns = [
            value for value in rule.get("from_packages", []) if isinstance(value, str)
        ]
        except_patterns = [
            value for value in rule.get("except_packages", []) if isinstance(value, str)
        ]
        forbidden_patterns = [
            value
            for value in rule.get("forbidden_dependencies", [])
            if isinstance(value, str)
        ]
        for package, (path, dependencies) in manifests.items():
            if not package_matches(package, from_patterns):
                continue
            if package_matches(package, except_patterns):
                continue
            for dependency in sorted(dependencies):
                if package_matches(dependency, forbidden_patterns):
                    errors.append(
                        f"dependency direction {rule_id} violated: {package} -> {dependency} "
                        f"in {path.relative_to(repo)}"
                    )
    return len(manifests)


def validate_document(
    repo: Path,
    policy: dict[str, Any],
    convergence: dict[str, Any],
) -> tuple[list[str], dict[str, int], int]:
    errors: list[str] = []
    touches, zones, dependency_rules = validate_policy_structure(policy, errors)
    validate_declared_paths(repo, policy, errors)
    entries = validate_convergence_structure(convergence, errors)
    floor = validate_floor(repo, policy, convergence, errors)
    footprint = validate_actual_footprint(
        repo,
        floor,
        policy,
        convergence,
        touches,
        zones,
        entries,
        errors,
    )
    manifest_count = validate_dependency_directions(repo, dependency_rules, errors)
    return errors, footprint, manifest_count


def relative_or_absolute(path: Path, repo: Path) -> str:
    try:
        return str(path.relative_to(repo))
    except ValueError:
        return str(path)


def main() -> int:
    args = parse_args()
    repo = args.repo.resolve()
    policy_path = resolve(repo, args.policy)
    map_path = resolve(repo, args.map_path)
    bootstrap_errors: list[str] = []
    policy = load_object(policy_path, "patch-footprint policy", bootstrap_errors)
    convergence = load_object(map_path, "convergence map", bootstrap_errors)
    if bootstrap_errors:
        print(json.dumps({"ok": False, "errors": bootstrap_errors}, indent=2, sort_keys=True))
        return 1

    errors, footprint, manifest_count = validate_document(repo, policy, convergence)
    if errors:
        print(json.dumps({"ok": False, "errors": errors}, indent=2, sort_keys=True))
        return 1

    receipt = {
        "ok": True,
        "schema_version": policy["schema_version"],
        "bead_id": policy["bead_id"],
        "policy_revision": policy["policy_revision"],
        "upstream_floor_sha": EXPECTED_FLOOR,
        "allowed_touch_points": len(policy["allowed_touch_points"]),
        "exception_zones": len(policy["exception_zones"]),
        "dependency_direction_rules": len(policy["dependency_direction_rules"]),
        "workspace_manifests_checked": manifest_count,
        "footprint": footprint,
        "policy": relative_or_absolute(policy_path, repo),
        "convergence_map": relative_or_absolute(map_path, repo),
    }
    print(json.dumps(receipt, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    sys.exit(main())
