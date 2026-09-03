#!/usr/bin/env python3
"""Validate the upstream patch budget, convergence map, and dependency directions."""

from __future__ import annotations

import argparse
import fnmatch
import json
import re
import subprocess
import sys
import tomllib
from collections import Counter
from pathlib import Path
from typing import Any, Iterable

EXPECTED_FLOOR = "5749e4fcfe268e17bd19a0e6ef90c646f7b37289"
EXPECTED_POLICY_REVISION = "patch-footprint.v3"
EXPECTED_CONVERGENCE_SCHEMA = "product/upstream/convergence-map.schema.json"
EXPECTED_CONVERGENCE_SCHEMA_VERSION = 2
EXPECTED_CLASSIFICATION_PRECEDENCE = [
    "active_upstream_entry_exact_path",
    "product_area_path_pattern",
    "policy_touch_point_path",
]
EXPECTED_ENTRY_RULES = [
    "Product paths resolve through exactly one active ownership area.",
    "Upstream paths require one exact active entry before authorization.",
    "Retired rows preserve history without granting current execution authority.",
]
BEAD_ID_RE = re.compile(r"^tdmem-[0-9]{4}$")
# Revision patch-footprint.v3 (ADR-0014) carries the v2 rule forward: each cap
# is the footprint measured at the revision tree plus at most ~15% headroom.
# v2 (ADR-0011) sized the M4 journey mount, M5 recall port, Native
# configuration, and session-sync scope; v3 adds the Claude Code host hook
# ingest journey, which measures 37 upstream production files and 3393 total
# upstream changed lines. Per-entry line budgets in the convergence map remain
# the binding per-file limit.
EXPECTED_BUDGET = {
    "max_upstream_existing_production_files": 37,
    "max_upstream_existing_test_or_fixture_files": 9,
    "max_total_upstream_changed_lines": 3500,
    "max_changed_lines_per_upstream_file": 560,
    "max_composition_root_files": 15,
    # 15 covers the daemon composition mount once the observation journey,
    # cognitive recall, and Native configuration seams are live; every other
    # category stays tighter through its local max_files, which binds via min().
    "max_allowed_touch_point_files_per_category": 15,
    # Revision v2 raised this from zero to exactly the two additive
    # configuration-registry files approved by ADR-0012; revision v3 raises it
    # to four for the two host-adapter hook files approved by ADR-0014. The
    # per-ADR cap of 2 keeps either ADR from being stretched to a third file.
    "default_max_exception_zone_files": 4,
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
    "crates/tracedecay-memory-fabric/**",
    "crates/tracedecay-memory-provider-registry/**",
    "crates/tracedecay-memory-provider-native/**",
    "crates/tracedecay-memory-provider-ncm/**",
    "crates/tracedecay-memory-observation/**",
    "crates/tracedecay-memory-hygiene/**",
    "crates/tracedecay-memory-context/**",
    "crates/tracedecay-memory-conformance/**",
    "crates/tracedecay-memory-evaluation/**",
    "crates/tracedecay/tests/product_memory_provider/**",
    "crates/tracedecay/tests/product_memory_provider_*.rs",
    "crates/tracedecay-cli/tests/product_memory_provider_*.rs",
    "crates/tracedecay/src/daemon/retained_owner/native_provider.rs",
    "crates/tracedecay/src/daemon/retained_owner/native_provider_tests.rs",
    "crates/tracedecay/src/daemon/retained_owner/native_provider_parity_tests.rs",
    "crates/tracedecay/src/daemon/retained_owner/native_baseline_tests.rs",
    "crates/tracedecay/src/daemon/retained_owner/native_staged_observations.rs",
    "crates/tracedecay/src/daemon/retained_owner/observation_journey.rs",
    "crates/tracedecay/src/daemon/retained_owner/claude_host_journey_tests.rs",
    "crates/tracedecay/src/daemon/retained_owner/observation_journey/tests/crash_restart_fuzz.rs",
    "crates/tracedecay/src/daemon/retained_owner/cognitive_recall.rs",
}
EXPECTED_ADMINISTRATIVE_EXCLUSIONS = {".codex/**"}
EXPECTED_TOUCH_POINTS = {
    "workspace_wiring",
    "application_contract_mount",
    "cognitive_recall_contract",
    "daemon_composition_mount",
    "daemon_shutdown_deadline",
    "production_harness_shutdown",
    "integration_test_runtime_isolation",
    "normalized_observation_mount",
    "recall_context_mount",
    "post_settlement_feedback_mount",
    "configuration_registry_mount",
    "host_hook_ingest",
}
# Touch-point-local caps are the tight per-seam limit behind the ADR-0011
# aggregate caps, so they are pinned here too: a category cannot widen its own
# reach by editing the policy alone. ADR-0011 invariant 2 ("a cap increase is
# approved by ADR before the change that needs it, and is never bundled into
# the change that exceeds the previous cap") binds these caps as well.
EXPECTED_TOUCH_POINT_CAPS = {
    "workspace_wiring": (2, 140),
    "application_contract_mount": (3, 220),
    "cognitive_recall_contract": (4, 940),
    "daemon_composition_mount": (15, 810),
    "daemon_shutdown_deadline": (8, 420),
    "production_harness_shutdown": (1, 62),
    "integration_test_runtime_isolation": (3, 240),
    "normalized_observation_mount": (2, 160),
    "recall_context_mount": (5, 340),
    "post_settlement_feedback_mount": (2, 160),
    "configuration_registry_mount": (5, 540),
    "host_hook_ingest": (3, 200),
}
# Categories whose local caps were revised above the value this policy revision
# shipped with, and the ADR that approved the exact numbers pinned above. A
# revised category must carry a matching `cap_revision` block; a category with
# no approved revision must not carry one.
REVISED_TOUCH_POINT_CAPS = {
    # ADR-0016 supersedes only ADR-0015 as the approving decision for this
    # category. The file cap remains 8; only the 360-line cap is replaced.
    "daemon_shutdown_deadline": {
        "adr": (
            "product/architecture/adr/"
            "ADR-0016-daemon-shutdown-receipt-ordering-headroom.md"
        ),
        "previous_max_files": 8,
        "previous_max_changed_lines": 360,
    },
}
EXPECTED_CAP_REVISION_FIELDS = {
    "adr",
    "measured_changed_lines",
    "measured_files",
    "policy_revision",
    "previous_max_changed_lines",
    "previous_max_files",
}
# ADR-0011 invariant 1: a cap is the measurement it was derived from plus at
# most roughly fifteen percent headroom, expressed as integers so the check
# never depends on float rounding.
CAP_HEADROOM_NUMERATOR = 115
CAP_HEADROOM_DENOMINATOR = 100
EXPECTED_EXCEPTION_ZONES = {
    "native_database_internals",
    "code_index_internals",
    "generated_contracts",
    "host_specific_adapters",
    "toolchain_build_and_ci_policy",
}
TRACEDECAY_INTERNAL_DEPENDENCY_PATTERNS = frozenset(
    {
        "tracedecay-runtime-core",
        "tracedecay-runtime*",
        "tracedecay-automation*",
        "tracedecay-*store*",
        "tracedecay-storage*",
        "tracedecay-*-db",
        "tracedecay-rusqlite-runtime",
        "tracedecay-code-*",
        "tracedecay-*query*",
        "tracedecay-semantic*",
        "rusqlite",
        "grafeo*",
        "libsql*",
        "private-fs*",
    }
)
CONCRETE_PROVIDER_DEPENDENCY_PATTERNS = frozenset(
    {
        "tracedecay-memory-provider-*",
        "biomem*",
        "ncm*",
        "ocean*",
    }
)
PROVIDER_API_DEPENDENCY_ALLOWANCE = frozenset({"tracedecay-memory-provider-api"})
EXPECTED_DEPENDENCY_RULES = {
    "provider_api_is_inward": {
        "from_packages": frozenset({"tracedecay-memory-provider-api"}),
        "except_packages": frozenset(),
        "allowed_dependencies": frozenset(),
        "forbidden_dependencies": frozenset({"tracedecay", "tracedecay-*"})
        | TRACEDECAY_INTERNAL_DEPENDENCY_PATTERNS,
    },
    "memory_fabric_is_provider_neutral": {
        "from_packages": frozenset({"tracedecay-memory-fabric"}),
        "except_packages": frozenset(),
        "allowed_dependencies": PROVIDER_API_DEPENDENCY_ALLOWANCE,
        "forbidden_dependencies": CONCRETE_PROVIDER_DEPENDENCY_PATTERNS,
    },
    "context_compiler_is_provider_neutral": {
        "from_packages": frozenset({"tracedecay-memory-context"}),
        "except_packages": frozenset(),
        "allowed_dependencies": PROVIDER_API_DEPENDENCY_ALLOWANCE,
        "forbidden_dependencies": CONCRETE_PROVIDER_DEPENDENCY_PATTERNS,
    },
    "adapters_do_not_depend_on_each_other": {
        "from_packages": frozenset({"tracedecay-memory-provider-native"}),
        "except_packages": frozenset(),
        "allowed_dependencies": PROVIDER_API_DEPENDENCY_ALLOWANCE,
        "forbidden_dependencies": frozenset(
            {
                "tracedecay-memory-provider-*",
                "tracedecay-memory-fabric",
                "tracedecay",
                "biomem*",
                "ncm*",
                "ocean*",
            }
        ),
    },
    "concrete_adapters_do_not_reach_tracedecay_internals": {
        "from_packages": frozenset({"tracedecay-memory-provider-*"}),
        "except_packages": frozenset(
            {
                "tracedecay-memory-provider-api",
                "tracedecay-memory-provider-registry",
            }
        ),
        "allowed_dependencies": frozenset(),
        "forbidden_dependencies": frozenset({"tracedecay", "tracedecay-memory-fabric"})
        | TRACEDECAY_INTERNAL_DEPENDENCY_PATTERNS,
    },
    "ncm_adapter_does_not_reach_native_store": {
        "from_packages": frozenset({"tracedecay-memory-provider-ncm"}),
        "except_packages": frozenset(),
        "allowed_dependencies": PROVIDER_API_DEPENDENCY_ALLOWANCE,
        "forbidden_dependencies": frozenset(
            {
                "tracedecay-memory-provider-*",
                "tracedecay-memory-fabric",
                "tracedecay",
                "ncm*",
                "ocean*",
            }
        )
        | TRACEDECAY_INTERNAL_DEPENDENCY_PATTERNS,
    },
    "transports_are_adapter_blind": {
        "from_packages": frozenset(
            {
                "tracedecay-cli",
                "tracedecay-mcp",
                "tracedecay-dashboard-api",
                "tracedecay-sdk",
            }
        ),
        "except_packages": frozenset(),
        "allowed_dependencies": PROVIDER_API_DEPENDENCY_ALLOWANCE,
        "forbidden_dependencies": CONCRETE_PROVIDER_DEPENDENCY_PATTERNS,
    },
    "upstream_crates_do_not_import_concrete_adapters": {
        "from_packages": frozenset({"tracedecay-*"}),
        "except_packages": frozenset(
            {
                "tracedecay",
                "tracedecay-memory-provider-registry",
                "tracedecay-memory-provider-native",
                "tracedecay-memory-provider-ncm",
                "tracedecay-memory-conformance",
            }
        ),
        "allowed_dependencies": PROVIDER_API_DEPENDENCY_ALLOWANCE,
        "forbidden_dependencies": CONCRETE_PROVIDER_DEPENDENCY_PATTERNS,
    },
}
EXPECTED_DEPENDENCY_EXCEPTION_FIELDS = {
    "rule",
    "source",
    "dependency",
    "adr",
    "rationale",
}
EXPECTED_PROTECTED_PACKAGE_IDENTITIES = {
    Path(
        "crates/tracedecay-memory-provider-api/Cargo.toml"
    ): "tracedecay-memory-provider-api",
    Path("crates/tracedecay-memory-fabric/Cargo.toml"): "tracedecay-memory-fabric",
    Path(
        "crates/tracedecay-memory-provider-native/Cargo.toml"
    ): "tracedecay-memory-provider-native",
    Path(
        "crates/tracedecay-memory-provider-ncm/Cargo.toml"
    ): "tracedecay-memory-provider-ncm",
    Path(
        "crates/tracedecay-memory-provider-registry/Cargo.toml"
    ): "tracedecay-memory-provider-registry",
    Path("crates/tracedecay-memory-context/Cargo.toml"): "tracedecay-memory-context",
    Path(
        "crates/tracedecay-memory-observation/Cargo.toml"
    ): "tracedecay-memory-observation",
    Path(
        "crates/tracedecay-memory-conformance/Cargo.toml"
    ): "tracedecay-memory-conformance",
    Path(
        "crates/tracedecay-memory-evaluation/Cargo.toml"
    ): "tracedecay-memory-evaluation",
}
EXPECTED_POLICY_MAP_FIELDS = {
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
EXPECTED_V2_MAP_FIELDS = EXPECTED_POLICY_MAP_FIELDS | {
    "area_id",
    "owner",
    "upstream_owner",
    "tests",
    "last_verified_upstream_sha",
    "upstreamability",
}
EXPECTED_V2_AREA_FIELDS = {
    "id",
    "status",
    "owner",
    "ownership_class",
    "feature",
    "path_patterns",
    "touch_points",
    "bead_ids",
    "rationale",
    "semantic_invariants",
    "tests",
    "last_verified_upstream_sha",
    "upstreamability",
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


def index_by_id(
    rows: Iterable[Any], field: str, errors: list[str]
) -> dict[str, dict[str, Any]]:
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


def non_empty_string(
    row: dict[str, Any], field: str, label: str, errors: list[str]
) -> str:
    value = row.get(field)
    if not isinstance(value, str) or not value.strip():
        errors.append(f"{label}.{field} must be a non-empty string")
        return ""
    return value.strip()


def pattern_matches(path: str, pattern: str) -> bool:
    """Match registry globs without allowing a single star to cross directories."""
    pieces = ["^"]
    index = 0
    while index < len(pattern):
        character = pattern[index]
        if character == "*":
            if index + 1 < len(pattern) and pattern[index + 1] == "*":
                pieces.append(".*")
                index += 2
            else:
                pieces.append("[^/]*")
                index += 1
        elif character == "?":
            pieces.append("[^/]")
            index += 1
        elif character == "[":
            end = pattern.find("]", index + 1)
            if end == -1:
                pieces.append(re.escape(character))
                index += 1
            else:
                body = pattern[index + 1 : end]
                if body.startswith("!"):
                    body = "^" + body[1:]
                elif body.startswith("^"):
                    body = "\\" + body
                pieces.append("[" + body.replace("/", "") + "]")
                index = end + 1
        else:
            pieces.append(re.escape(character))
            index += 1
    pieces.append("$")
    try:
        return re.fullmatch("".join(pieces), path) is not None
    except re.error:
        return False


def matches_any(path: str, patterns: Iterable[str]) -> bool:
    return any(pattern_matches(path, pattern) for pattern in patterns)


def contains_glob(value: str) -> bool:
    return any(character in value for character in "*?[")


def literal_pattern_prefix(pattern: str) -> str:
    positions = [pattern.find(character) for character in "*?["]
    positions = [position for position in positions if position >= 0]
    return pattern if not positions else pattern[: min(positions)]


def representative_pattern_path(pattern: str) -> str:
    value = re.sub(r"\[[^\]]+\]", "x", pattern)
    value = value.replace("**", "sample/path")
    value = value.replace("*", "sample")
    return value.replace("?", "x")


def pattern_is_covered(candidate: str, allowed: str) -> bool:
    """Conservatively prove that a registry pattern stays inside policy scope."""
    if candidate == allowed:
        return True
    if not contains_glob(candidate):
        return pattern_matches(candidate, allowed)
    allowed_prefix = literal_pattern_prefix(allowed)
    candidate_prefix = literal_pattern_prefix(candidate)
    if not candidate_prefix.startswith(allowed_prefix):
        return False
    if allowed.endswith("/**"):
        return True
    sample = representative_pattern_path(candidate)
    return "/" not in candidate[len(candidate_prefix) :] and pattern_matches(
        sample, allowed
    )


def is_substantive_prose(value: str) -> bool:
    normalized = " ".join(value.split())
    words = re.findall(r"[A-Za-z0-9][A-Za-z0-9_-]*", normalized)
    return (
        len(normalized) >= 20
        and len(words) >= 4
        and normalized.casefold() not in {"tbd", "todo", "n/a", "none", "placeholder"}
    )


def is_affirmative_dependency_decision(value: str) -> bool:
    """Require an exception ADR to grant, rather than merely discuss, an edge."""
    normalized = " ".join(value.split()).casefold()
    active_grant = re.search(
        r"(?:^|[.!?;:]\s+)(?:we\s+)?(?:hereby\s+)?"
        r"(?:permit|allow|approve|authorize|accept)\b",
        normalized,
    )
    passive_grant = re.search(
        r"\b(?:edge|dependency)\b.{0,60}\b(?:is|are)\s+"
        r"(?:explicitly\s+|hereby\s+)?"
        r"(?:permitted|allowed|approved|authorized|accepted)\b",
        normalized,
    )
    return active_grant is not None or passive_grant is not None


def strip_html_comments(document: str) -> str:
    """Remove complete and unterminated HTML comments before Markdown parsing."""
    visible: list[str] = []
    cursor = 0
    while cursor < len(document):
        opening = document.find("<!--", cursor)
        if opening < 0:
            visible.append(document[cursor:])
            break
        visible.append(document[cursor:opening])
        closing = document.find("-->", opening + 4)
        if closing < 0:
            break
        cursor = closing + 3
    return "".join(visible)


def markdown_level_two_sections(document: str) -> dict[str, list[list[str]]]:
    sections: dict[str, list[list[str]]] = {}
    active: list[str] | None = None
    fenced_by: tuple[str, int] | None = None
    visible_document = strip_html_comments(document)
    for line in visible_document.splitlines():
        indentation = len(line) - len(line.lstrip(" "))
        if line.startswith("\t") or indentation >= 4:
            continue
        stripped = line[indentation:]
        fence = re.match(r"^(`{3,}|~{3,})(.*)$", stripped)
        if fenced_by is None and fence is not None:
            marker = fence.group(1)
            info = fence.group(2)
            if marker[0] == "`" and "`" in info:
                pass
            else:
                fenced_by = (marker[0], len(marker))
                continue
        elif fenced_by is not None:
            closing = re.match(r"^([`~]+)\s*$", stripped)
            if (
                closing is not None
                and closing.group(1)[0] == fenced_by[0]
                and len(closing.group(1)) >= fenced_by[1]
                and len(set(closing.group(1))) == 1
            ):
                fenced_by = None
            continue
        heading = re.match(r"^(#{1,6})\s+(.+?)\s*#*\s*$", stripped)
        if heading is not None:
            if len(heading.group(1)) <= 2:
                active = None
                if len(heading.group(1)) == 2:
                    title = heading.group(2).strip()
                    active = []
                    sections.setdefault(title, []).append(active)
            continue
        if active is not None:
            active.append(line)
    return sections


def validate_dependency_exception_structure(
    policy: dict[str, Any],
    rules: dict[str, dict[str, Any]],
    errors: list[str],
) -> list[dict[str, Any]]:
    rows = require_list(
        policy.get("dependency_direction_exceptions"),
        "dependency_direction_exceptions",
        errors,
    )
    validated: list[dict[str, Any]] = []
    seen: set[tuple[str, str, str]] = set()
    for offset, raw in enumerate(rows):
        label = f"dependency_direction_exceptions[{offset}]"
        if not isinstance(raw, dict):
            errors.append(f"{label} must be an object")
            continue
        fields = set(raw)
        missing = EXPECTED_DEPENDENCY_EXCEPTION_FIELDS - fields
        extra = fields - EXPECTED_DEPENDENCY_EXCEPTION_FIELDS
        if missing:
            errors.append(f"{label} missing fields: {sorted(missing)}")
        if extra:
            errors.append(f"{label} has unsupported fields: {sorted(extra)}")

        normalized: dict[str, str] = {}
        for field in EXPECTED_DEPENDENCY_EXCEPTION_FIELDS:
            value = non_empty_string(raw, field, label, errors)
            if value:
                normalized[field] = value
                if raw.get(field) != value:
                    errors.append(
                        f"{label}.{field} must not have surrounding whitespace"
                    )

        rationale = normalized.get("rationale")
        if rationale and not is_substantive_prose(rationale):
            errors.append(f"{label}.rationale must be substantive prose")

        for field in ("rule", "source", "dependency"):
            value = normalized.get(field)
            if value and contains_glob(value):
                errors.append(f"{label}.{field} must be literal; globs are forbidden")

        adr = normalized.get("adr")
        if adr and contains_glob(adr):
            errors.append(f"{label}.adr must be an exact in-repo ADR path")

        rule_id = normalized.get("rule")
        if rule_id and rule_id not in rules:
            errors.append(f"{label} names unknown dependency rule {rule_id!r}")

        key_fields = (
            normalized.get("rule"),
            normalized.get("source"),
            normalized.get("dependency"),
        )
        if all(key_fields):
            key = (key_fields[0], key_fields[1], key_fields[2])
            if key in seen:
                errors.append(
                    f"duplicate dependency direction exception for "
                    f"{key[0]}: {key[1]} -> {key[2]}"
                )
            else:
                seen.add(key)

        if fields == EXPECTED_DEPENDENCY_EXCEPTION_FIELDS and len(normalized) == len(
            EXPECTED_DEPENDENCY_EXCEPTION_FIELDS
        ):
            validated.append(raw)
    return validated


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


def validate_floor(
    repo: Path, policy: dict[str, Any], convergence: dict[str, Any], errors: list[str]
) -> str:
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
        result = git(
            repo, "merge-base", "--is-ancestor", EXPECTED_FLOOR, "HEAD", check=False
        )
    except OSError as exc:
        errors.append(f"could not execute git ancestry check: {exc}")
    else:
        if result.returncode != 0:
            errors.append("pinned upstream floor is not an ancestor of HEAD")
    return str(floor)


def is_affirmative_cap_decision(value: str, touch_id: str) -> bool:
    """Require a cap ADR to grant, rather than merely discuss, the exact category."""
    normalized = " ".join(value.split())
    if touch_id not in normalized:
        return False
    return (
        re.search(
            r"(?:^|[.!?;:]\s+)(?:we\s+)?(?:hereby\s+)?"
            r"(?:approve|authorize|permit|allow)\b",
            normalized.casefold(),
        )
        is not None
    )


def validate_cap_revision_adr(
    repo: Path,
    adr: str,
    label: str,
    touch_id: str,
    approval: dict[str, Any],
    approved: tuple[int, int],
    measurements: dict[str, int],
    errors: list[str],
) -> None:
    """Require the approving ADR to bind this category's exact numbers."""
    raw_path = Path(adr)
    repo_root = repo.resolve()
    adr_root = (repo / "product/architecture/adr").resolve()
    if (
        raw_path.is_absolute()
        or ".." in raw_path.parts
        or raw_path.suffix != ".md"
        or not adr.startswith("product/architecture/adr/")
    ):
        errors.append(
            f"{label} ADR must be an exact path under product/architecture/adr: {adr}"
        )
        return
    try:
        adr_root.relative_to(repo_root)
    except ValueError:
        errors.append(f"{label} ADR directory resolves outside the repository: {adr}")
        return
    resolved = (repo / raw_path).resolve()
    try:
        resolved.relative_to(adr_root)
    except ValueError:
        errors.append(f"{label} ADR resolves outside product/architecture/adr: {adr}")
        return
    if not resolved.is_file():
        errors.append(f"{label} ADR is missing: {adr}")
        return
    try:
        document = resolved.read_text(encoding="utf-8")
    except (OSError, UnicodeDecodeError) as exc:
        errors.append(f"{label} ADR could not be read as UTF-8: {adr}: {exc}")
        return

    sections = markdown_level_two_sections(document)
    binding_sections = sections.get("Touch-point cap revision", [])
    if len(binding_sections) != 1:
        errors.append(
            f"{label} ADR must contain exactly one "
            "'## Touch-point cap revision' section"
        )
    else:
        fields: dict[str, list[str]] = {}
        for line in binding_sections[0]:
            match = re.match(
                r"^\s*[-*]\s+([A-Z][A-Za-z][A-Za-z -]*):\s+`([^`]+)`\s*$",
                line,
            )
            if match is not None:
                fields.setdefault(match.group(1), []).append(match.group(2))
        expected_fields = {
            "Touch point": touch_id,
            "Previous max files": str(approval["previous_max_files"]),
            "Previous max changed lines": str(approval["previous_max_changed_lines"]),
            "Approved max files": str(approved[0]),
            "Approved max changed lines": str(approved[1]),
            "Policy revision": EXPECTED_POLICY_REVISION,
        }
        for field, measured_key in (
            ("Measured files", "measured_files"),
            ("Measured changed lines", "measured_changed_lines"),
        ):
            if measured_key in measurements:
                expected_fields[field] = str(measurements[measured_key])
        for field, expected in expected_fields.items():
            if fields.get(field, []) != [expected]:
                errors.append(
                    f"{label} ADR {field.lower()} binding must be exactly {expected!r}"
                )

    decision_sections = sections.get("Decision", [])
    if len(decision_sections) != 1:
        errors.append(f"{label} ADR must contain exactly one '## Decision' section")
        return
    prose = " ".join(line.strip() for line in decision_sections[0] if line.strip())
    if not is_substantive_prose(prose):
        errors.append(f"{label} ADR decision must be substantive prose")
    elif not is_affirmative_cap_decision(prose, touch_id):
        errors.append(
            f"{label} ADR decision must explicitly and affirmatively approve "
            f"the {touch_id} cap increase"
        )


def validate_touch_point_cap_revision(
    repo: Path,
    touch_id: str,
    row: dict[str, Any],
    errors: list[str],
) -> None:
    """Bind a revised touch-point cap to the ADR that approved its exact numbers."""
    label = f"{touch_id}.cap_revision"
    approval = REVISED_TOUCH_POINT_CAPS.get(touch_id)
    revision = row.get("cap_revision")
    if approval is None:
        if revision is not None:
            errors.append(
                f"{touch_id} declares a cap_revision without an "
                "ADR-approved cap increase"
            )
        return
    approved = EXPECTED_TOUCH_POINT_CAPS[touch_id]
    if (
        approved[0] <= approval["previous_max_files"]
        and approved[1] <= approval["previous_max_changed_lines"]
    ):
        errors.append(f"{label} records no cap increase over the previous caps")
    if not isinstance(revision, dict):
        errors.append(f"{label} must be an object naming the approving ADR")
        return
    if set(revision) != EXPECTED_CAP_REVISION_FIELDS:
        errors.append(
            f"{label} fields must be exactly "
            f"{sorted(EXPECTED_CAP_REVISION_FIELDS)}"
        )
        return
    if revision.get("policy_revision") != EXPECTED_POLICY_REVISION:
        errors.append(f"{label}.policy_revision must be {EXPECTED_POLICY_REVISION}")
    for field in ("adr", "previous_max_files", "previous_max_changed_lines"):
        if revision.get(field) != approval[field]:
            errors.append(f"{label}.{field} must be {approval[field]!r}")
    measurements: dict[str, int] = {}
    for field, cap in (
        ("measured_files", approved[0]),
        ("measured_changed_lines", approved[1]),
    ):
        value = revision.get(field)
        if not isinstance(value, int) or isinstance(value, bool) or value <= 0:
            errors.append(f"{label}.{field} must be a positive integer")
            continue
        measurements[field] = value
        if value > cap:
            errors.append(
                f"{label}.{field} {value} exceeds the approved cap {cap}"
            )
        elif cap * CAP_HEADROOM_DENOMINATOR > value * CAP_HEADROOM_NUMERATOR:
            errors.append(
                f"{touch_id} cap {cap} exceeds its measurement {value} by more "
                "than the ~15% headroom ADR-0011 allows"
            )
    adr = revision.get("adr")
    if isinstance(adr, str):
        validate_cap_revision_adr(
            repo,
            adr,
            label,
            touch_id,
            approval,
            approved,
            measurements,
            errors,
        )


def validate_policy_structure(
    repo: Path, policy: dict[str, Any], errors: list[str]
) -> tuple[
    dict[str, dict[str, Any]],
    dict[str, dict[str, Any]],
    dict[str, dict[str, Any]],
    list[dict[str, Any]],
]:
    if policy.get("schema_version") != 1:
        errors.append("policy schema_version must be 1")
    policy_bead = non_empty_string(policy, "bead_id", "policy", errors)
    if policy_bead and BEAD_ID_RE.fullmatch(policy_bead) is None:
        errors.append("policy.bead_id must be a canonical tdmem bead id")
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
        errors.append(
            f"product-owned path patterns missing: {sorted(missing_patterns)}"
        )
    if extra_patterns:
        errors.append(
            f"unexpected/broad product-owned path patterns: {sorted(extra_patterns)}"
        )
    for forbidden_broad in (
        "crates/**",
        "crates/tracedecay/**",
        "tests/**",
        ".github/**",
    ):
        if forbidden_broad in pattern_set:
            errors.append(
                f"product-owned paths must not hide upstream tree {forbidden_broad!r}"
            )

    administrative_patterns = require_list(
        policy.get("administrative_paths_excluded_from_footprint"),
        "administrative_paths_excluded_from_footprint",
        errors,
    )
    if any(not isinstance(value, str) for value in administrative_patterns):
        errors.append(
            "administrative_paths_excluded_from_footprint entries must be strings"
        )
    administrative_pattern_set = {
        value for value in administrative_patterns if isinstance(value, str)
    }
    missing_administrative = (
        EXPECTED_ADMINISTRATIVE_EXCLUSIONS - administrative_pattern_set
    )
    extra_administrative = (
        administrative_pattern_set - EXPECTED_ADMINISTRATIVE_EXCLUSIONS
    )
    if missing_administrative:
        errors.append(
            "administrative footprint exclusions missing: "
            f"{sorted(missing_administrative)}"
        )
    if extra_administrative:
        errors.append(
            "unexpected/broad administrative footprint exclusions: "
            f"{sorted(extra_administrative)}"
        )

    budget = policy.get("initial_budget")
    if not isinstance(budget, dict):
        errors.append("initial_budget must be an object")
    else:
        for key, expected in EXPECTED_BUDGET.items():
            if budget.get(key) != expected:
                errors.append(f"initial_budget.{key} must be {expected}")
        notes = require_list(budget.get("notes"), "initial_budget.notes", errors)
        note_text = "\n".join(value for value in notes if isinstance(value, str))
        for marker in (
            "product_owned_paths",
            "Renaming",
            "additions plus deletions",
            "Cargo.lock",
        ):
            if marker not in note_text:
                errors.append(f"initial budget notes are missing {marker!r}")

    touch_rows = require_list(
        policy.get("allowed_touch_points"), "allowed_touch_points", errors
    )
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
        expected_caps = EXPECTED_TOUCH_POINT_CAPS.get(touch_id)
        for index, cap in enumerate(("max_files", "max_changed_lines")):
            value = row.get(cap)
            if not isinstance(value, int) or isinstance(value, bool) or value <= 0:
                errors.append(f"{touch_id}.{cap} must be a positive integer")
            elif expected_caps is not None and value != expected_caps[index]:
                errors.append(f"{touch_id}.{cap} must be {expected_caps[index]}")
        validate_touch_point_cap_revision(repo, touch_id, row, errors)
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
            errors.append(
                f"{zone_id}.default_policy must be forbidden or generated_only"
            )
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
    expected_rule_ids = set(EXPECTED_DEPENDENCY_RULES)
    missing_rules = expected_rule_ids - dependencies.keys()
    extra_rules = dependencies.keys() - expected_rule_ids
    if missing_rules:
        errors.append(f"dependency direction rules missing: {sorted(missing_rules)}")
    if extra_rules:
        errors.append(f"unexpected dependency direction rules: {sorted(extra_rules)}")
    for rule_id, row in dependencies.items():
        from_packages = require_list(
            row.get("from_packages"), f"{rule_id}.from_packages", errors
        )
        except_packages = require_list(
            row.get("except_packages", []), f"{rule_id}.except_packages", errors
        )
        allowed_dependencies = require_list(
            row.get("allowed_dependencies", []),
            f"{rule_id}.allowed_dependencies",
            errors,
        )
        forbidden = require_list(
            row.get("forbidden_dependencies"),
            f"{rule_id}.forbidden_dependencies",
            errors,
        )
        if not from_packages or not forbidden:
            errors.append(
                f"{rule_id} must define source and forbidden package patterns"
            )
        for field, values in (
            ("from_packages", from_packages),
            ("except_packages", except_packages),
            ("allowed_dependencies", allowed_dependencies),
            ("forbidden_dependencies", forbidden),
        ):
            if any(not isinstance(value, str) or not value.strip() for value in values):
                errors.append(f"{rule_id}.{field} entries must be non-empty strings")
            string_values = [value for value in values if isinstance(value, str)]
            if len(string_values) != len(set(string_values)):
                errors.append(f"{rule_id}.{field} contains duplicate patterns")
            if field == "allowed_dependencies" and any(
                contains_glob(value) for value in string_values
            ):
                errors.append(
                    f"{rule_id}.allowed_dependencies must contain literal package names"
                )

        expected = EXPECTED_DEPENDENCY_RULES.get(rule_id)
        if expected is not None:
            for field, values in (
                ("from_packages", from_packages),
                ("except_packages", except_packages),
                ("allowed_dependencies", allowed_dependencies),
                ("forbidden_dependencies", forbidden),
            ):
                actual_values = {
                    value
                    for value in values
                    if isinstance(value, str) and value.strip()
                }
                expected_values = set(expected[field])
                missing_values = expected_values - actual_values
                extra_values = actual_values - expected_values
                if missing_values or extra_values:
                    errors.append(
                        f"{rule_id}.{field} must match the canonical dependency contract; "
                        f"missing={sorted(missing_values)}, extra={sorted(extra_values)}"
                    )
        non_empty_string(row, "reason", rule_id, errors)

    dependency_exceptions = validate_dependency_exception_structure(
        policy, dependencies, errors
    )

    convergence_contract = policy.get("convergence_map")
    if not isinstance(convergence_contract, dict):
        errors.append("convergence_map policy contract must be an object")
    else:
        if convergence_contract.get("path") != "product/upstream/convergence-map.json":
            errors.append("convergence_map.path is not canonical")
        if (
            convergence_contract.get("entry_required_for_every_upstream_existing_file")
            is not True
        ):
            errors.append(
                "every upstream existing-file edit must require a convergence entry"
            )
        required = set(
            value
            for value in require_list(
                convergence_contract.get("required_entry_fields"),
                "convergence_map.required_entry_fields",
                errors,
            )
            if isinstance(value, str)
        )
        if required != EXPECTED_POLICY_MAP_FIELDS:
            errors.append(
                "convergence-map required fields do not match the entry contract"
            )
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
            errors.append(
                "convergence-map exception fields do not match the exception contract"
            )

    return touches, zones, dependencies, dependency_exceptions


def validate_repo_relative_path(
    value: Any,
    label: str,
    errors: list[str],
    *,
    allow_glob: bool,
) -> str:
    if not isinstance(value, str) or not value:
        errors.append(f"{label} must be a non-empty string")
        return ""
    invalid = (
        value.startswith("/")
        or re.match(r"^[A-Za-z]:[/\\]", value) is not None
        or value.startswith("./")
        or "\\" in value
        or "//" in value
        or value.endswith("/")
        or any(part in {"", ".", ".."} for part in value.split("/"))
    )
    if not allow_glob and contains_glob(value):
        invalid = True
    if invalid:
        errors.append(f"{label} must be a normalized repo-relative POSIX path")
    return value


def validate_convergence_structure(
    convergence: dict[str, Any], errors: list[str]
) -> tuple[dict[str, dict[str, Any]], dict[str, dict[str, Any]]]:
    if convergence.get("schema_version") != EXPECTED_CONVERGENCE_SCHEMA_VERSION:
        errors.append("convergence-map schema_version must be integer 2")
    if convergence.get("$schema") != EXPECTED_CONVERGENCE_SCHEMA:
        errors.append(
            f"convergence-map $schema must be {EXPECTED_CONVERGENCE_SCHEMA!r}"
        )
    bead_id = non_empty_string(convergence, "bead_id", "convergence map", errors)
    if bead_id and BEAD_ID_RE.fullmatch(bead_id) is None:
        errors.append("convergence map.bead_id must be a canonical tdmem bead id")

    owners = convergence.get("owners")
    owner_ids: dict[str, str] = {}
    if not isinstance(owners, dict):
        errors.append("convergence-map owners must be an object")
    else:
        for ownership in ("product", "upstream"):
            owner = owners.get(ownership)
            if not isinstance(owner, dict):
                errors.append(f"convergence-map owners.{ownership} must be an object")
                continue
            owner_ids[ownership] = non_empty_string(
                owner, "id", f"convergence-map owners.{ownership}", errors
            )
            non_empty_string(
                owner, "repository", f"convergence-map owners.{ownership}", errors
            )

    classification = convergence.get("classification_contract")
    if not isinstance(classification, dict):
        errors.append("convergence-map classification_contract must be an object")
    else:
        if classification.get("path_format") != "repo-relative-posix":
            errors.append(
                "convergence-map classification path format must be repo-relative-posix"
            )
        if classification.get("precedence") != EXPECTED_CLASSIFICATION_PRECEDENCE:
            errors.append("convergence-map classification precedence has drifted")
        if classification.get("ambiguous_match") != "error":
            errors.append("convergence-map ambiguous ownership matches must be errors")
        if classification.get("unclassified_path") != "error":
            errors.append("convergence-map unclassified paths must be errors")

    contract = convergence.get("entry_contract")
    if not isinstance(contract, dict):
        errors.append("convergence-map entry_contract must be an object")
    else:
        if contract.get("rules") != EXPECTED_ENTRY_RULES:
            errors.append("convergence-map executable ownership rules have drifted")
        if contract.get("area_status_values") != ["active", "planned", "retired"]:
            errors.append("convergence-map area status values have drifted")
        if contract.get("entry_status_values") != ["active", "retired"]:
            errors.append("convergence-map entry status values have drifted")

    area_rows = require_list(convergence.get("areas"), "convergence-map areas", errors)
    areas: dict[str, dict[str, Any]] = {}
    for offset, raw in enumerate(area_rows):
        label = f"convergence-map areas[{offset}]"
        if not isinstance(raw, dict):
            errors.append(f"{label} must be an object")
            continue
        area_id = non_empty_string(raw, "id", label, errors)
        if not area_id:
            continue
        if area_id in areas:
            errors.append(f"duplicate convergence-map area id {area_id!r}")
            continue
        areas[area_id] = raw
        missing = EXPECTED_V2_AREA_FIELDS - raw.keys()
        if missing:
            errors.append(
                f"convergence area {area_id} missing fields: {sorted(missing)}"
            )
        status = raw.get("status")
        if status not in {"active", "planned", "retired"}:
            errors.append(f"convergence area {area_id} has invalid status")
        ownership_class = raw.get("ownership_class")
        if ownership_class not in {"product_owned", "upstream_owned"}:
            errors.append(f"convergence area {area_id} has invalid ownership_class")
        expected_owner = owner_ids.get(
            "product" if ownership_class == "product_owned" else "upstream"
        )
        if expected_owner and raw.get("owner") != expected_owner:
            errors.append(f"convergence area {area_id} names the wrong canonical owner")
        patterns = require_list(
            raw.get("path_patterns"),
            f"convergence area {area_id}.path_patterns",
            errors,
        )
        if not patterns:
            errors.append(f"convergence area {area_id}.path_patterns must not be empty")
        for pattern_offset, pattern in enumerate(patterns):
            validate_repo_relative_path(
                pattern,
                f"convergence area {area_id}.path_patterns[{pattern_offset}]",
                errors,
                allow_glob=True,
            )
        touch_points = require_list(
            raw.get("touch_points"), f"convergence area {area_id}.touch_points", errors
        )
        if not touch_points:
            errors.append(f"convergence area {area_id}.touch_points must not be empty")

    entry_rows = require_list(
        convergence.get("entries"), "convergence-map entries", errors
    )
    entries: dict[str, dict[str, Any]] = {}
    for offset, raw in enumerate(entry_rows):
        label = f"convergence-map entries[{offset}]"
        if not isinstance(raw, dict):
            errors.append(f"{label} must be an object")
            continue
        path = validate_repo_relative_path(
            raw.get("path"), f"{label}.path", errors, allow_glob=False
        )
        if not path:
            continue
        if path in entries:
            errors.append(f"duplicate convergence-map path {path!r}")
            continue
        entries[path] = raw
        missing = EXPECTED_V2_MAP_FIELDS - raw.keys()
        if missing:
            errors.append(f"convergence entry {path} missing fields: {sorted(missing)}")
        status = raw.get("status")
        if status not in {"active", "retired"}:
            errors.append(f"convergence entry {path} has invalid status")
        touch_point = raw.get("touch_point")
        if touch_point not in EXPECTED_TOUCH_POINTS | {"exception"}:
            errors.append(f"convergence entry {path} has invalid touch_point")
        for field in (
            "area_id",
            "owner",
            "upstream_owner",
            "rationale",
            "rebase_or_removal_plan",
            "last_verified_upstream_sha",
        ):
            non_empty_string(raw, field, f"convergence entry {path}", errors)
        for field in ("semantic_invariants", "verification", "tests", "bead_ids"):
            values = require_list(
                raw.get(field), f"convergence entry {path}.{field}", errors
            )
            if not values:
                errors.append(f"convergence entry {path}.{field} must not be empty")
        line_budget = raw.get("line_budget")
        if (
            not isinstance(line_budget, int)
            or isinstance(line_budget, bool)
            or line_budget <= 0
        ):
            errors.append(f"convergence entry {path}.line_budget must be positive")
        area_id = raw.get("area_id")
        area = areas.get(area_id) if isinstance(area_id, str) else None
        if area is None:
            errors.append(f"convergence entry {path} names unknown area {area_id!r}")
        elif status == "active":
            if area.get("status") != "active":
                errors.append(
                    f"active convergence entry {path} must use an active area"
                )
            if area.get("ownership_class") != "upstream_owned":
                errors.append(
                    f"active convergence entry {path} must use an upstream-owned area"
                )
            matching_areas = sorted(
                candidate_id
                for candidate_id, candidate in areas.items()
                if candidate.get("status") == "active"
                and candidate.get("ownership_class") == "upstream_owned"
                and any(
                    isinstance(pattern, str) and pattern_matches(path, pattern)
                    for pattern in candidate.get("path_patterns", [])
                )
            )
            if matching_areas != [area_id]:
                errors.append(
                    f"active convergence entry {path} must resolve to exactly its "
                    f"upstream area; matched {matching_areas}"
                )
        if owner_ids.get("product") and raw.get("owner") != owner_ids["product"]:
            errors.append(f"convergence entry {path} names the wrong product owner")
        if (
            owner_ids.get("upstream")
            and raw.get("upstream_owner") != owner_ids["upstream"]
        ):
            errors.append(f"convergence entry {path} names the wrong upstream owner")
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
    return entries, areas


def parse_numstat_records(output: str, errors: list[str]) -> dict[str, tuple[int, int]]:
    rows: dict[str, tuple[int, int]] = {}
    for record in output.split("\0"):
        if not record:
            continue
        parts = record.split("\t", 2)
        if len(parts) != 3:
            errors.append(f"unparseable git numstat record: {record!r}")
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


def diff_numstat(
    repo: Path, floor: str, errors: list[str]
) -> dict[str, tuple[int, int]]:
    """Measure the checkout against its floor, including every dirty-state layer."""
    commands = (
        ("floor-to-worktree", ("diff", "--no-renames", "--numstat", "-z", floor, "--")),
        (
            "staged",
            ("diff", "--cached", "--no-renames", "--numstat", "-z", "HEAD", "--"),
        ),
        ("unstaged", ("diff", "--no-renames", "--numstat", "-z", "--")),
    )
    measured: dict[str, tuple[int, int]] = {}
    for label, command in commands:
        try:
            result = git(repo, *command)
        except (OSError, RuntimeError, UnicodeError) as exc:
            errors.append(f"could not read {label} diff: {exc}")
            continue
        layer = parse_numstat_records(result.stdout, errors)
        for path, counts in layer.items():
            current = measured.get(path)
            if current is None or sum(counts) > sum(current):
                measured[path] = counts

    try:
        untracked = git(repo, "ls-files", "--others", "--exclude-standard", "-z")
    except (OSError, RuntimeError, UnicodeError) as exc:
        errors.append(f"could not enumerate untracked paths: {exc}")
        return measured
    for path in sorted(value for value in untracked.stdout.split("\0") if value):
        if path in measured:
            continue
        absolute = repo / path
        if absolute.is_symlink():
            errors.append(f"untracked symbolic link diff is unsupported: {path}")
            continue
        try:
            contents = absolute.read_bytes()
        except OSError as exc:
            errors.append(f"could not read untracked path {path}: {exc}")
            continue
        if b"\0" in contents:
            errors.append(f"binary upstream/product diff is unsupported: {path}")
            continue
        measured[path] = (len(contents.splitlines()), 0)
    return measured


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


def matching_active_area_ids(
    path: str,
    areas: dict[str, dict[str, Any]],
    ownership_class: str,
) -> list[str]:
    return sorted(
        area_id
        for area_id, area in areas.items()
        if area.get("status") == "active"
        and area.get("ownership_class") == ownership_class
        and any(
            isinstance(pattern, str) and pattern_matches(path, pattern)
            for pattern in area.get("path_patterns", [])
        )
    )


def validate_actual_footprint(
    repo: Path,
    floor: str,
    policy: dict[str, Any],
    touches: dict[str, dict[str, Any]],
    zones: dict[str, dict[str, Any]],
    entries: dict[str, dict[str, Any]],
    areas: dict[str, dict[str, Any]],
    errors: list[str],
) -> dict[str, int]:
    stats = diff_numstat(repo, floor, errors)
    product_patterns = [
        value
        for value in policy.get("product_owned_paths", [])
        if isinstance(value, str)
    ]
    administrative_patterns = [
        value
        for value in policy.get("administrative_paths_excluded_from_footprint", [])
        if isinstance(value, str)
    ]
    active_entries = {
        path: row for path, row in entries.items() if row.get("status") == "active"
    }
    retired_entries = {
        path: row for path, row in entries.items() if row.get("status") == "retired"
    }
    upstream: dict[str, tuple[int, int]] = {}
    for path, counts in stats.items():
        if matches_any(path, administrative_patterns):
            continue
        if path in active_entries:
            upstream[path] = counts
            continue
        product_area_ids = matching_active_area_ids(path, areas, "product_owned")
        if len(product_area_ids) > 1:
            errors.append(
                f"changed path {path!r} ambiguously matches active product areas "
                f"{product_area_ids}"
            )
            continue
        if len(product_area_ids) == 1:
            continue
        if matches_any(path, product_patterns):
            errors.append(
                f"product-owned changed path lacks an active ownership area: {path}"
            )
            continue
        upstream[path] = counts

    for path in sorted(upstream.keys() - active_entries.keys()):
        errors.append(
            f"upstream-owned changed file lacks active convergence entry: {path}"
        )
    for path in sorted(active_entries.keys() - upstream.keys()):
        errors.append(f"active convergence entry has no current upstream diff: {path}")
    for path in sorted(retired_entries.keys() & upstream.keys()):
        errors.append(
            f"retired convergence entry cannot authorize current diff: {path}"
        )

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
                if (
                    not adr.startswith("product/architecture/adr/")
                    or not (repo / adr).is_file()
                ):
                    errors.append(
                        f"exception entry {path} ADR is missing or outside product ADRs: {adr}"
                    )
            if exception.get("policy_revision") != policy.get("policy_revision"):
                errors.append(
                    f"exception entry {path} uses a different policy revision"
                )
            forbidden_exception_files += 1
        else:
            if touch_point not in touch_matches:
                errors.append(
                    f"convergence entry {path} selects {touch_point!r}; matching touch points are {touch_matches}"
                )
            if not touch_matches:
                errors.append(
                    f"upstream changed file is outside allowed touch points: {path}"
                )
            if zone_matches:
                generated_only = all(
                    zones[zone].get("default_policy") == "generated_only"
                    for zone in zone_matches
                )
                if not generated_only:
                    errors.append(
                        f"upstream changed file {path} is in exception zone(s) {zone_matches} without exception evidence"
                    )
                else:
                    generated = entry.get("generated")
                    if not isinstance(generated, dict):
                        errors.append(
                            f"generated output {path} must record generator/reproduction evidence"
                        )
                    else:
                        for field in (
                            "generator_path",
                            "reproduction",
                            "zero_drift_check",
                        ):
                            non_empty_string(
                                generated, field, f"generated entry {path}", errors
                            )
            if isinstance(touch_point, str):
                category_files[touch_point] += 1
                category_lines[touch_point] += changed
                if touch_point == "daemon_composition_mount":
                    composition_files += 1
                if touch_point == "workspace_wiring":
                    workspace_files += 1

        required_fields = set(
            policy.get("convergence_map", {}).get("required_entry_fields", [])
        )
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
        effective_caps = [
            value for value in (local_cap, global_cap) if isinstance(value, int)
        ]
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
                errors.append(
                    f"ADR {adr} authorizes {count} exception files, exceeding cap {adr_cap}"
                )

    computed = {
        "upstream_existing_production_files": production_files,
        "upstream_existing_test_or_fixture_files": test_files,
        "total_upstream_changed_lines": total_lines,
        "composition_root_files": composition_files,
        "exception_zone_files": forbidden_exception_files,
    }
    return computed


DEPENDENCY_SECTIONS = ("dependencies", "dev-dependencies", "build-dependencies")
LEGACY_DEPENDENCY_SECTIONS = ("dev_dependencies", "build_dependencies")


def dependency_tables(manifest: dict[str, Any]) -> list[tuple[str, Any]]:
    tables: list[tuple[str, Any]] = []
    for section_name in DEPENDENCY_SECTIONS + LEGACY_DEPENDENCY_SECTIONS:
        tables.append((section_name, manifest.get(section_name)))
    targets = manifest.get("target")
    if isinstance(targets, dict):
        for target_name, target in targets.items():
            if not isinstance(target, dict):
                continue
            for section_name in DEPENDENCY_SECTIONS + LEGACY_DEPENDENCY_SECTIONS:
                tables.append(
                    (f"target.{target_name}.{section_name}", target.get(section_name))
                )
    return tables


def validate_legacy_manifest_tables(
    manifest: dict[str, Any], label: str, errors: list[str]
) -> None:
    if "project" in manifest:
        errors.append(f"{label} uses unsupported legacy [project]; use [package]")
    if "workspace_dependencies" in manifest:
        errors.append(
            f"{label} uses unsupported legacy [workspace_dependencies]; "
            "use [workspace.dependencies]"
        )
    for section_name, table in dependency_tables(manifest):
        leaf = section_name.rsplit(".", 1)[-1]
        if leaf in LEGACY_DEPENDENCY_SECTIONS and table is not None:
            canonical = leaf.replace("_", "-")
            errors.append(
                f"{label} uses unsupported legacy [{section_name}]; "
                f"use [{section_name.removesuffix(leaf)}{canonical}]"
            )


def effective_dependency_declaration(
    alias: str,
    declaration: Any,
    workspace_dependencies: dict[str, Any],
) -> tuple[Any, bool]:
    if isinstance(declaration, dict) and declaration.get("workspace") is True:
        inherited = workspace_dependencies.get(alias)
        if inherited is not None:
            return inherited, True
    return declaration, False


def canonical_dependency_name(
    alias: str,
    declaration: Any,
    workspace_dependencies: dict[str, Any],
    *,
    manifest_path: Path | None = None,
    repo: Path | None = None,
) -> str:
    effective, inherited = effective_dependency_declaration(
        alias, declaration, workspace_dependencies
    )
    package_name = alias
    if isinstance(effective, dict):
        package = effective.get("package")
        if isinstance(package, str) and package:
            package_name = package
        dependency_path = effective.get("path")
        if (
            isinstance(dependency_path, str)
            and dependency_path
            and manifest_path is not None
            and repo is not None
        ):
            base = repo if inherited else manifest_path.parent
            target = Path(dependency_path)
            target_manifest = (
                target if target.name == "Cargo.toml" else target / "Cargo.toml"
            )
            if not target_manifest.is_absolute():
                target_manifest = base / target_manifest
            expected = protected_package_identity(repo, target_manifest)
            if expected is not None:
                package_name = expected
    return package_name


def dependency_declarations(
    manifest: dict[str, Any],
    workspace_dependencies: dict[str, Any] | None = None,
    *,
    manifest_path: Path | None = None,
    repo: Path | None = None,
) -> list[tuple[str, str, str]]:
    inherited = workspace_dependencies or {}
    declarations: list[tuple[str, str, str]] = []

    def collect(section: Any, section_name: str) -> None:
        if not isinstance(section, dict):
            return
        for alias, value in section.items():
            declarations.append(
                (
                    canonical_dependency_name(
                        alias,
                        value,
                        inherited,
                        manifest_path=manifest_path,
                        repo=repo,
                    ),
                    alias,
                    section_name,
                )
            )

    for section_name, table in dependency_tables(manifest):
        collect(table, section_name)
    return declarations


def dependency_names(
    manifest: dict[str, Any],
    workspace_dependencies: dict[str, Any] | None = None,
) -> set[str]:
    return {
        dependency
        for dependency, _alias, _section in dependency_declarations(
            manifest, workspace_dependencies
        )
    }


def package_matches(package: str, patterns: Iterable[str]) -> bool:
    return any(fnmatch.fnmatchcase(package, pattern) for pattern in patterns)


def load_workspace_manifest(
    repo: Path, errors: list[str]
) -> tuple[dict[str, Any], dict[str, Any]]:
    root_manifest = repo / "Cargo.toml"
    if not root_manifest.is_file():
        return {}, {}
    try:
        document = tomllib.loads(root_manifest.read_text(encoding="utf-8"))
    except (OSError, tomllib.TOMLDecodeError) as exc:
        errors.append(f"could not parse Cargo.toml: {exc}")
        return {}, {}
    workspace = document.get("workspace")
    dependencies: dict[str, Any] = {}
    legacy_dependencies = document.get("workspace_dependencies")
    if isinstance(legacy_dependencies, dict):
        errors.append(
            "Cargo.toml uses unsupported legacy [workspace_dependencies]; "
            "use [workspace.dependencies]"
        )
        dependencies.update(legacy_dependencies)
    if isinstance(workspace, dict):
        canonical_dependencies = workspace.get("dependencies")
        if isinstance(canonical_dependencies, dict):
            dependencies.update(canonical_dependencies)
    return document, dependencies


def workspace_manifest_paths(
    repo: Path, root_manifest: dict[str, Any], errors: list[str]
) -> list[Path]:
    repo_root = repo.resolve()
    workspace = root_manifest.get("workspace")
    exclude_patterns: list[str] = []
    if isinstance(workspace, dict):
        raw_excludes = workspace.get("exclude", [])
        if isinstance(raw_excludes, list):
            for raw_exclude in raw_excludes:
                if not isinstance(raw_exclude, str):
                    errors.append("workspace.exclude entries must be strings")
                    continue
                exclude_path = Path(raw_exclude)
                if exclude_path.is_absolute() or ".." in exclude_path.parts:
                    errors.append(
                        f"workspace.exclude entry must be a relative in-repo pattern "
                        f"without '..': {raw_exclude!r}"
                    )
                    continue
                exclude_patterns.append(raw_exclude.rstrip("/"))
        else:
            errors.append("workspace.exclude must be an array of relative patterns")

    def inside_repo(candidate: Path, origin: str) -> Path | None:
        try:
            resolved = candidate.resolve()
            resolved.relative_to(repo_root)
        except (OSError, ValueError):
            errors.append(
                f"{origin} resolves outside the repository and was not scanned"
            )
            return None
        return resolved

    def excluded(candidate: Path) -> bool:
        try:
            relative_manifest = candidate.relative_to(repo_root).as_posix()
        except ValueError:
            return False
        relative_package = Path(relative_manifest).parent.as_posix()
        return any(
            fnmatch.fnmatchcase(relative_package, pattern)
            or fnmatch.fnmatchcase(relative_manifest, pattern)
            for pattern in exclude_patterns
        )

    paths: set[Path] = set()
    pending: list[Path] = []
    checked_excluded_identities: set[Path] = set()

    def offer(candidate: Path, origin: str) -> None:
        manifest = (
            candidate if candidate.name == "Cargo.toml" else candidate / "Cargo.toml"
        )
        resolved = inside_repo(manifest, origin)
        if resolved is None or not resolved.is_file():
            return
        if excluded(resolved):
            expected_name = protected_package_identity(repo_root, resolved)
            if (
                expected_name is not None
                and resolved not in checked_excluded_identities
            ):
                checked_excluded_identities.add(resolved)
                try:
                    excluded_document = tomllib.loads(
                        resolved.read_text(encoding="utf-8")
                    )
                    package = excluded_document.get("package")
                    declared_name = (
                        package.get("name") if isinstance(package, dict) else None
                    )
                    if declared_name != expected_name:
                        errors.append(
                            f"protected package identity mismatch: "
                            f"{resolved.relative_to(repo_root).as_posix()} must declare "
                            f"[package].name = {expected_name!r}; found {declared_name!r}"
                        )
                except (OSError, tomllib.TOMLDecodeError):
                    pass
            return
        if resolved not in paths:
            paths.add(resolved)
            pending.append(resolved)

    if isinstance(root_manifest.get("package"), dict) or isinstance(
        root_manifest.get("project"), dict
    ):
        offer(repo / "Cargo.toml", "root package")

    if isinstance(workspace, dict):
        members = workspace.get("members", [])
        if isinstance(members, list):
            for raw_member in members:
                if not isinstance(raw_member, str):
                    errors.append("workspace.members entries must be strings")
                    continue
                member_path = Path(raw_member)
                unsafe = member_path.is_absolute() or ".." in member_path.parts
                if unsafe:
                    errors.append(
                        f"workspace member must be a relative in-repo pattern without "
                        f"'..': {raw_member!r}"
                    )
                    if contains_glob(raw_member):
                        continue
                    offer(
                        member_path
                        if member_path.is_absolute()
                        else repo / member_path,
                        f"workspace member {raw_member!r}",
                    )
                    continue
                try:
                    matches = repo.glob(raw_member)
                    for matched in matches:
                        offer(matched, f"workspace member {raw_member!r}")
                except (OSError, ValueError) as exc:
                    errors.append(
                        f"could not expand workspace member {raw_member!r}: {exc}"
                    )
        else:
            errors.append("workspace.members must be an array of relative patterns")

    workspace_dependencies = (
        workspace.get("dependencies", {}) if isinstance(workspace, dict) else {}
    )
    if isinstance(workspace_dependencies, dict):
        pseudo_manifest = {"dependencies": workspace_dependencies}
        pending_roots = [(repo_root / "Cargo.toml", pseudo_manifest, {})]
    else:
        pending_roots = []

    visited_for_paths: set[Path] = set()
    while pending or pending_roots:
        synthetic_workspace_dependencies = False
        if pending_roots:
            manifest_path, document, inherited = pending_roots.pop()
            synthetic_workspace_dependencies = True
        else:
            manifest_path = pending.pop()
            if manifest_path in visited_for_paths:
                continue
            try:
                document = tomllib.loads(manifest_path.read_text(encoding="utf-8"))
            except (OSError, tomllib.TOMLDecodeError):
                continue
            inherited = (
                workspace.get("dependencies", {}) if isinstance(workspace, dict) else {}
            )
            if not isinstance(inherited, dict):
                inherited = {}
        if not synthetic_workspace_dependencies:
            visited_for_paths.add(manifest_path)
        for section_name, table in dependency_tables(document):
            if not isinstance(table, dict):
                continue
            for alias, declaration in table.items():
                effective, inherited_declaration = effective_dependency_declaration(
                    alias, declaration, inherited
                )
                if not isinstance(effective, dict):
                    continue
                raw_path = effective.get("path")
                if not isinstance(raw_path, str) or not raw_path:
                    continue
                base = repo_root if inherited_declaration else manifest_path.parent
                dependency_path = Path(raw_path)
                offer(
                    dependency_path
                    if dependency_path.is_absolute()
                    else base / dependency_path,
                    f"path dependency {alias!r} in {manifest_path.relative_to(repo_root)} "
                    f"({section_name})",
                )
    return sorted(paths)


def protected_package_identity(repo: Path, manifest_path: Path) -> str | None:
    try:
        relative = manifest_path.resolve().relative_to(repo.resolve())
    except (OSError, ValueError):
        return None
    expected = EXPECTED_PROTECTED_PACKAGE_IDENTITIES.get(relative)
    if expected is not None:
        return expected
    if (
        len(relative.parts) == 3
        and relative.parts[0] == "crates"
        and relative.parts[2] == "Cargo.toml"
        and relative.parts[1].startswith("tracedecay-memory-provider-")
    ):
        return relative.parts[1]
    return None


def validate_dependency_exception_adr(
    repo: Path,
    adr: str,
    label: str,
    rule_id: str,
    source: str,
    dependency: str,
    errors: list[str],
) -> bool:
    raw_path = Path(adr)
    repo_root = repo.resolve()
    adr_root = (repo / "product/architecture/adr").resolve()
    if (
        raw_path.is_absolute()
        or ".." in raw_path.parts
        or raw_path.suffix != ".md"
        or not adr.startswith("product/architecture/adr/")
    ):
        errors.append(
            f"{label} ADR must be an exact path under product/architecture/adr: {adr}"
        )
        return False
    try:
        adr_root.relative_to(repo_root)
    except ValueError:
        errors.append(f"{label} ADR directory resolves outside the repository: {adr}")
        return False
    resolved = (repo / raw_path).resolve()
    try:
        resolved.relative_to(adr_root)
    except ValueError:
        errors.append(f"{label} ADR resolves outside product/architecture/adr: {adr}")
        return False
    if not resolved.is_file():
        errors.append(f"{label} ADR is missing: {adr}")
        return False
    try:
        document = resolved.read_text(encoding="utf-8")
    except (OSError, UnicodeDecodeError) as exc:
        errors.append(f"{label} ADR could not be read as UTF-8: {adr}: {exc}")
        return False

    sections = markdown_level_two_sections(document)
    valid = True
    binding_sections = sections.get("Dependency-direction exception", [])
    if len(binding_sections) != 1:
        errors.append(
            f"{label} ADR must contain exactly one "
            "'## Dependency-direction exception' section"
        )
        valid = False
    else:
        fields: dict[str, list[str]] = {}
        for line in binding_sections[0]:
            match = re.match(
                r"^\s*[-*]\s+(Rule|Source|Dependency):\s+`([^`]+)`\s*$",
                line,
            )
            if match is not None:
                fields.setdefault(match.group(1), []).append(match.group(2))
        expected_fields = {
            "Rule": rule_id,
            "Source": source,
            "Dependency": dependency,
        }
        for field, expected in expected_fields.items():
            values = fields.get(field, [])
            if values != [expected]:
                errors.append(
                    f"{label} ADR {field.lower()} binding must be exactly {expected!r}"
                )
                valid = False

    for title in ("Decision", "Rationale"):
        prose_sections = sections.get(title, [])
        if len(prose_sections) != 1:
            errors.append(f"{label} ADR must contain exactly one '## {title}' section")
            valid = False
            continue
        prose = " ".join(line.strip() for line in prose_sections[0] if line.strip())
        if not is_substantive_prose(prose):
            errors.append(f"{label} ADR {title.lower()} must be substantive prose")
            valid = False
        elif title == "Decision" and not is_affirmative_dependency_decision(prose):
            errors.append(
                f"{label} ADR decision must explicitly and affirmatively authorize "
                "the exact dependency edge"
            )
            valid = False
    return valid


def validate_dependency_directions(
    repo: Path,
    rules: dict[str, dict[str, Any]],
    errors: list[str],
    exceptions: list[dict[str, Any]] | None = None,
) -> int:
    exceptions = exceptions or []
    repo_root = repo.resolve()
    root_manifest, workspace_dependencies = load_workspace_manifest(repo, errors)
    manifests: dict[str, tuple[Path, list[tuple[str, str, str]]]] = {}
    protected_dependency_names: dict[str, str] = {}
    for path in workspace_manifest_paths(repo_root, root_manifest, errors):
        try:
            document = tomllib.loads(path.read_text(encoding="utf-8"))
        except (OSError, tomllib.TOMLDecodeError) as exc:
            errors.append(f"could not parse {path.relative_to(repo_root)}: {exc}")
            continue
        relative_path = path.relative_to(repo_root).as_posix()
        validate_legacy_manifest_tables(document, relative_path, errors)
        package = document.get("package")
        if not isinstance(package, dict):
            package = document.get("project")
        name = package.get("name") if isinstance(package, dict) else None
        expected_name = protected_package_identity(repo_root, path)
        effective_name = name
        if expected_name is not None:
            effective_name = expected_name
            if name != expected_name:
                errors.append(
                    f"protected package identity mismatch: "
                    f"{relative_path} must declare "
                    f"[package].name = {expected_name!r}; found {name!r}"
                )
                if isinstance(name, str) and name:
                    protected_dependency_names[name] = expected_name
        if isinstance(effective_name, str):
            if effective_name in manifests:
                errors.append(f"duplicate workspace package name {effective_name!r}")
                continue
            manifests[effective_name] = (
                path,
                dependency_declarations(
                    document,
                    workspace_dependencies,
                    manifest_path=path,
                    repo=repo_root,
                ),
            )

    if protected_dependency_names:
        for source, (path, declarations) in list(manifests.items()):
            manifests[source] = (
                path,
                [
                    (
                        protected_dependency_names.get(dependency, dependency),
                        alias,
                        section,
                    )
                    for dependency, alias, section in declarations
                ],
            )

    declared_edges = {
        (source, dependency)
        for source, (_path, declarations) in manifests.items()
        for dependency, _alias, _section in declarations
    }
    usable_exceptions: set[tuple[str, str, str]] = set()
    for offset, exception in enumerate(exceptions):
        label = f"dependency_direction_exceptions[{offset}]"
        rule_id = exception.get("rule")
        source = exception.get("source")
        dependency = exception.get("dependency")
        adr = exception.get("adr")
        if not all(
            isinstance(value, str) and value.strip()
            for value in (
                rule_id,
                source,
                dependency,
                adr,
                exception.get("rationale"),
            )
        ):
            continue
        rule = rules.get(rule_id)
        if rule is None:
            continue
        adr_valid = validate_dependency_exception_adr(
            repo,
            adr,
            label,
            rule_id,
            source,
            dependency,
            errors,
        )
        manifest = manifests.get(source)
        if manifest is None:
            errors.append(f"{label} names unknown source package {source!r}")
            continue
        from_patterns = [
            value for value in rule.get("from_packages", []) if isinstance(value, str)
        ]
        except_patterns = [
            value for value in rule.get("except_packages", []) if isinstance(value, str)
        ]
        allowed_dependencies = {
            value
            for value in rule.get("allowed_dependencies", [])
            if isinstance(value, str)
        }
        forbidden_patterns = [
            value
            for value in rule.get("forbidden_dependencies", [])
            if isinstance(value, str)
        ]
        if not package_matches(source, from_patterns) or package_matches(
            source, except_patterns
        ):
            errors.append(
                f"{label} source {source!r} does not match dependency rule {rule_id}"
            )
            continue
        if dependency in allowed_dependencies or not package_matches(
            dependency, forbidden_patterns
        ):
            errors.append(
                f"{label} dependency {dependency!r} is not a forbidden edge "
                f"for rule {rule_id} after structural allowances"
            )
            continue
        if (source, dependency) not in declared_edges:
            errors.append(
                f"{label} is stale/unused: {source} -> {dependency} is not declared"
            )
            continue
        if adr_valid:
            usable_exceptions.add((rule_id, source, dependency))

    for rule_id, rule in rules.items():
        from_patterns = [
            value for value in rule.get("from_packages", []) if isinstance(value, str)
        ]
        except_patterns = [
            value for value in rule.get("except_packages", []) if isinstance(value, str)
        ]
        allowed_dependencies = {
            value
            for value in rule.get("allowed_dependencies", [])
            if isinstance(value, str)
        }
        forbidden_patterns = [
            value
            for value in rule.get("forbidden_dependencies", [])
            if isinstance(value, str)
        ]
        for package, (path, declarations) in manifests.items():
            if not package_matches(package, from_patterns):
                continue
            if package_matches(package, except_patterns):
                continue
            for dependency, alias, section in sorted(declarations):
                if dependency in allowed_dependencies:
                    continue
                if package_matches(dependency, forbidden_patterns):
                    if (rule_id, package, dependency) in usable_exceptions:
                        continue
                    declaration = (
                        f"{section} key {alias!r}" if alias != dependency else section
                    )
                    errors.append(
                        f"dependency direction {rule_id} violated: {package} -> {dependency} "
                        f"in {path.relative_to(repo_root)} ({declaration})"
                    )
    return len(manifests)


def validate_document(
    repo: Path,
    policy: dict[str, Any],
    convergence: dict[str, Any],
) -> tuple[list[str], dict[str, int], int]:
    errors: list[str] = []
    touches, zones, dependency_rules, dependency_exceptions = validate_policy_structure(
        repo, policy, errors
    )
    entries, areas = validate_convergence_structure(convergence, errors)
    floor = validate_floor(repo, policy, convergence, errors)
    footprint = validate_actual_footprint(
        repo,
        floor,
        policy,
        touches,
        zones,
        entries,
        areas,
        errors,
    )
    manifest_count = validate_dependency_directions(
        repo, dependency_rules, errors, dependency_exceptions
    )
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
        print(
            json.dumps(
                {"ok": False, "errors": bootstrap_errors}, indent=2, sort_keys=True
            )
        )
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
