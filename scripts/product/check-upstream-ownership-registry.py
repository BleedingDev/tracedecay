#!/usr/bin/env python3
"""Validate the schema-v2 upstream ownership registry and classify paths."""

from __future__ import annotations

import argparse
import json
import re
import shlex
import subprocess
import sys
from collections import Counter
from pathlib import Path
from typing import Any, Iterable

SCHEMA_REFERENCE = "product/upstream/convergence-map.schema.json"
SCHEMA_DIALECT = "https://json-schema.org/draft/2020-12/schema"
EXPECTED_SCHEMA_VERSION = 2
EXPECTED_BEAD_ID = "tdmem-0308"
CANONICAL_PRODUCT_PATTERNS = {
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
    "crates/tracedecay-memory-context/**",
    "crates/tracedecay-memory-conformance/**",
    "crates/tracedecay/tests/product_memory_provider/**",
    "crates/tracedecay/tests/product_memory_provider_*.rs",
    "crates/tracedecay/src/daemon/retained_owner/native_provider.rs",
    "crates/tracedecay/src/daemon/retained_owner/native_provider_tests.rs",
    "crates/tracedecay/src/daemon/retained_owner/native_provider_parity_tests.rs",
}
EXPECTED_ENTRY_RULES = [
    "Product paths resolve through exactly one active ownership area.",
    "Upstream paths require one exact active entry before authorization.",
    "Retired rows preserve history without granting current execution authority.",
]
EXPECTED_CLASSIFICATION_CONTRACT = {
    "path_format": "repo-relative-posix",
    "precedence": [
        "active_upstream_entry_exact_path",
        "product_area_path_pattern",
        "policy_touch_point_path",
    ],
    "ambiguous_match": "error",
    "unclassified_path": "error",
}
AREA_STATUSES = ("active", "planned", "retired")
ENTRY_STATUSES = ("active", "retired")
OWNERSHIP_CLASSES = {"product_owned", "upstream_owned"}
UPSTREAMABILITY_KINDS = {
    "product_only",
    "upstream_candidate",
    "minimal_mount",
    "generated_resolution",
}
SHA_RE = re.compile(r"^[0-9a-f]{40}$")
BEAD_RE = re.compile(r"^tdmem-[0-9]{4}$")
AREA_ID_RE = re.compile(r"^[a-z][a-z0-9]*(?:_[a-z0-9]+)*$")
FEATURE_RE = re.compile(r"^[a-z][a-z0-9_.-]*$")
REPOSITORY_RE = re.compile(r"^[^/\s]+/[^/\s]+$")
GLOB_CHARS = frozenset("*?[")
MAX_DIAGNOSTIC_ERRORS = 100

ROOT_FIELDS = {
    "$schema",
    "schema_version",
    "bead_id",
    "policy_revision",
    "upstream_floor_sha",
    "owners",
    "classification_contract",
    "areas",
    "entries",
    "entry_contract",
}
OWNER_FIELDS = {"id", "repository"}
CLASSIFICATION_FIELDS = {
    "path_format",
    "precedence",
    "ambiguous_match",
    "unclassified_path",
}
AREA_FIELDS = {
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
UPSTREAMABILITY_FIELDS = {"kind", "rationale"}
ENTRY_REQUIRED_FIELDS = {
    "path",
    "area_id",
    "owner",
    "upstream_owner",
    "touch_point",
    "rationale",
    "semantic_invariants",
    "verification",
    "tests",
    "bead_ids",
    "line_budget",
    "rebase_or_removal_plan",
    "status",
    "last_verified_upstream_sha",
    "upstreamability",
}
ENTRY_OPTIONAL_FIELDS = {"generated"}
GENERATED_FIELDS = {"generator_path", "reproduction", "zero_drift_check"}
ENTRY_CONTRACT_FIELDS = {
    "rules",
    "area_status_values",
    "entry_status_values",
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--repo", type=Path, default=Path("."))
    parser.add_argument(
        "--schema", type=Path, default=Path(SCHEMA_REFERENCE)
    )
    parser.add_argument(
        "--map",
        dest="map_path",
        type=Path,
        default=Path("product/upstream/convergence-map.json"),
    )
    parser.add_argument(
        "--policy",
        type=Path,
        default=Path("product/upstream/patch-footprint-policy.json"),
    )
    parser.add_argument(
        "--sync-policy",
        type=Path,
        default=Path("product/upstream/sync-policy.json"),
    )
    parser.add_argument(
        "--floor-metadata",
        type=Path,
        default=Path("product/upstream/tracedecay-v2-pr707.json"),
    )
    parser.add_argument(
        "--beads", type=Path, default=Path(".beads/issues.jsonl")
    )
    parser.add_argument(
        "--classify-path",
        action="append",
        default=[],
        help="Repo-relative path that must resolve under the registry contract",
    )
    parser.add_argument(
        "--skip-changed-path-classification",
        action="store_true",
        help="Skip git diff classification only for isolated synthetic fixtures",
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
    if type(value) is not dict:
        errors.append(f"{label} root must be an object")
        return {}
    return value


def load_bead_ids(path: Path, errors: list[str]) -> set[str]:
    try:
        lines = path.read_text(encoding="utf-8").splitlines()
    except OSError as exc:
        errors.append(f"could not load beads authority: {exc}")
        return set()
    bead_ids: set[str] = set()
    for line_number, line in enumerate(lines, start=1):
        if not line.strip():
            continue
        try:
            row = json.loads(line)
        except json.JSONDecodeError as exc:
            errors.append(f"beads authority line {line_number} is invalid JSON: {exc}")
            continue
        if type(row) is not dict or type(row.get("id")) is not str:
            errors.append(f"beads authority line {line_number} lacks a string id")
            continue
        bead_id = row["id"]
        if bead_id in bead_ids:
            errors.append(f"beads authority contains duplicate id {bead_id!r}")
            continue
        bead_ids.add(bead_id)
    if not bead_ids:
        errors.append("beads authority contains no issue ids")
    return bead_ids


def validate_closed_object(
    value: Any,
    label: str,
    required: set[str],
    errors: list[str],
    optional: set[str] | None = None,
) -> dict[str, Any] | None:
    if type(value) is not dict:
        errors.append(f"{label} must be an object")
        return None
    allowed = required | (optional or set())
    missing = sorted(required - value.keys())
    unknown = sorted(value.keys() - allowed)
    if missing:
        errors.append(f"{label} missing required fields: {missing}")
    if unknown:
        errors.append(f"{label} contains unknown fields: {unknown}")
    return value


def canonical_string(value: Any, label: str, errors: list[str]) -> str:
    if type(value) is not str or not value:
        errors.append(f"{label} must be a non-empty string")
        return ""
    if value != value.strip():
        errors.append(f"{label} must not contain surrounding whitespace")
    if any(ord(character) < 32 or ord(character) == 127 for character in value):
        errors.append(f"{label} must not contain control characters")
    return value


def substantive_prose(value: Any, label: str, errors: list[str]) -> str:
    text = canonical_string(value, label, errors)
    normalized = " ".join(text.split())
    words = re.findall(r"[A-Za-z0-9][A-Za-z0-9_-]*", normalized)
    placeholders = {"tbd", "todo", "placeholder", "n/a", "none"}
    if (
        len(normalized) < 20
        or len(words) < 4
        or normalized.casefold() in placeholders
    ):
        errors.append(f"{label} must contain substantive prose")
    return text


def string_list(
    value: Any,
    label: str,
    errors: list[str],
    *,
    non_empty: bool = True,
) -> list[str]:
    if type(value) is not list:
        errors.append(f"{label} must be an array")
        return []
    if non_empty and not value:
        errors.append(f"{label} must not be empty")
    result: list[str] = []
    seen: set[str] = set()
    for offset, raw in enumerate(value):
        item = canonical_string(raw, f"{label}[{offset}]", errors)
        if not item:
            continue
        if item in seen:
            errors.append(f"{label} contains duplicate value {item!r}")
        else:
            seen.add(item)
            result.append(item)
    return result


def contains_glob(value: str) -> bool:
    return any(character in value for character in GLOB_CHARS)


def validate_repo_path(
    value: Any,
    label: str,
    errors: list[str],
    *,
    allow_glob: bool,
) -> str:
    path = canonical_string(value, label, errors)
    if not path:
        return ""
    invalid = any(
        ord(character) < 32 or ord(character) == 127 for character in path
    )
    if path.startswith("/") or re.match(r"^[A-Za-z]:[/\\]", path):
        invalid = True
    if "\\" in path or "//" in path or path.endswith("/"):
        invalid = True
    parts = path.split("/")
    if path.startswith("./") or any(part in {"", ".", ".."} for part in parts):
        invalid = True
    if not allow_glob and contains_glob(path):
        errors.append(f"{label} must be an exact path without glob syntax")
        invalid = True
    if path.count("[") != path.count("]"):
        invalid = True
    if invalid:
        errors.append(f"{label} must be a normalized repo-relative POSIX path")
    return path


def glob_regex(pattern: str) -> re.Pattern[str]:
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
    return re.compile("".join(pieces))


def path_matches(path: str, pattern: str) -> bool:
    try:
        return glob_regex(pattern).fullmatch(path) is not None
    except re.error:
        return False


def literal_prefix(pattern: str) -> str:
    positions = [pattern.find(character) for character in GLOB_CHARS]
    positions = [position for position in positions if position >= 0]
    return pattern if not positions else pattern[: min(positions)]


def representative_path(pattern: str) -> str:
    value = re.sub(r"\[[^\]]+\]", "x", pattern)
    value = value.replace("**", "sample/path")
    value = value.replace("*", "sample")
    value = value.replace("?", "x")
    return value


def pattern_is_covered(candidate: str, allowed: str) -> bool:
    if candidate == allowed:
        return True
    if not contains_glob(candidate):
        return path_matches(candidate, allowed)
    allowed_prefix = literal_prefix(allowed)
    candidate_prefix = literal_prefix(candidate)
    if not candidate_prefix.startswith(allowed_prefix):
        return False
    if allowed.endswith("/**"):
        return True
    sample = representative_path(candidate)
    return "/" not in candidate[len(candidate_prefix) :] and path_matches(sample, allowed)


def command_test_path_is_executable(repo: Path, raw_path: str, suffix: str) -> bool:
    path_errors: list[str] = []
    normalized = validate_repo_path(
        raw_path, "test command path", path_errors, allow_glob=False
    )
    return (
        not path_errors
        and normalized.startswith("tests/")
        and normalized.endswith(suffix)
        and (repo / normalized).is_file()
    )


def valid_behavioral_test_command(command: str, repo: Path) -> bool:
    if any(token in command for token in ("\n", ";", "&&", "||", "`", "$(")):
        return False
    try:
        parts = shlex.split(command)
    except ValueError:
        return False
    if not parts:
        return False
    if any(part in {"--no-run", "--list", "--help", "-h"} for part in parts):
        return False
    executable = Path(parts[0]).name
    if executable == "cargo":
        return len(parts) >= 2 and (
            parts[1] == "test"
            or (parts[1] == "nextest" and len(parts) >= 3 and parts[2] == "run")
        )
    if executable in {"python", "python3"}:
        if len(parts) >= 3 and parts[1] == "-m":
            return parts[2] in {"pytest", "unittest"}
        return len(parts) >= 2 and command_test_path_is_executable(
            repo, parts[1], ".py"
        )
    if executable in {"pytest", "py.test"}:
        return True
    if executable in {"bash", "sh"}:
        return len(parts) >= 2 and command_test_path_is_executable(
            repo, parts[1], ".sh"
        )
    return False


def validate_test_commands(
    value: Any, label: str, repo: Path, errors: list[str]
) -> list[str]:
    commands = string_list(value, label, errors)
    for command in commands:
        if not valid_behavioral_test_command(command, repo):
            errors.append(
                f"{label} command {command!r} is not an executable behavioral test"
            )
    return commands


def validate_bead_ids(
    value: Any,
    label: str,
    known_beads: set[str],
    errors: list[str],
) -> list[str]:
    bead_ids = string_list(value, label, errors)
    for bead_id in bead_ids:
        if BEAD_RE.fullmatch(bead_id) is None:
            errors.append(f"{label} contains malformed bead id {bead_id!r}")
        elif bead_id not in known_beads:
            errors.append(f"{label} references unknown bead id {bead_id!r}")
    return bead_ids


def validate_sha(value: Any, label: str, errors: list[str]) -> str:
    sha = canonical_string(value, label, errors)
    if SHA_RE.fullmatch(sha) is None:
        errors.append(f"{label} must be a lowercase 40-character SHA")
    return sha


def nested_object(
    row: dict[str, Any], key: str, label: str, errors: list[str]
) -> dict[str, Any]:
    value = row.get(key)
    if type(value) is not dict:
        errors.append(f"{label}.{key} must be an object")
        return {}
    return value


def validate_schema_definition(schema: dict[str, Any], errors: list[str]) -> None:
    if schema.get("$schema") != SCHEMA_DIALECT:
        errors.append("ownership registry schema must use JSON Schema draft 2020-12")
    if schema.get("type") != "object" or schema.get("additionalProperties") is not False:
        errors.append("ownership registry schema root must be a closed object")
    required = schema.get("required")
    if type(required) is not list or required != [
        "$schema",
        "schema_version",
        "bead_id",
        "policy_revision",
        "upstream_floor_sha",
        "owners",
        "classification_contract",
        "areas",
        "entries",
        "entry_contract",
    ]:
        errors.append("ownership registry schema root required fields drifted")
    properties = schema.get("properties")
    if type(properties) is not dict or set(properties) != ROOT_FIELDS:
        errors.append("ownership registry schema root properties drifted")
        return
    version = properties.get("schema_version")
    if type(version) is not dict or type(version.get("const")) is not int or version.get("const") != 2:
        errors.append("ownership registry schema must require integer schema_version 2")
    definitions = schema.get("$defs")
    expected_definitions = {
        "sha",
        "bead_id",
        "relative_path",
        "exact_path",
        "non_empty_unique_strings",
        "owner",
        "upstreamability",
        "area",
        "generated",
        "entry",
    }
    if type(definitions) is not dict or set(definitions) != expected_definitions:
        errors.append("ownership registry schema definitions drifted")
        return

    def require_closed_schema_object(
        name: str,
        required_fields: set[str],
        property_fields: set[str] | None = None,
    ) -> None:
        definition = definitions.get(name)
        expected_properties = property_fields or required_fields
        if type(definition) is not dict:
            errors.append(f"ownership registry schema {name} definition must be an object")
            return
        required_value = definition.get("required")
        schema_properties = definition.get("properties")
        if definition.get("type") != "object" or definition.get("additionalProperties") is not False:
            errors.append(f"ownership registry schema {name} definition must be closed")
        if type(required_value) is not list or set(required_value) != required_fields:
            errors.append(f"ownership registry schema {name} required fields drifted")
        if type(schema_properties) is not dict or set(schema_properties) != expected_properties:
            errors.append(f"ownership registry schema {name} properties drifted")

    require_closed_schema_object("owner", OWNER_FIELDS)
    require_closed_schema_object("upstreamability", UPSTREAMABILITY_FIELDS)
    require_closed_schema_object("area", AREA_FIELDS)
    require_closed_schema_object("generated", GENERATED_FIELDS)
    require_closed_schema_object(
        "entry", ENTRY_REQUIRED_FIELDS, ENTRY_REQUIRED_FIELDS | ENTRY_OPTIONAL_FIELDS
    )
    area_definition = definitions["area"]
    entry_definition = definitions["entry"]
    if area_definition.get("properties", {}).get("status", {}).get("enum") != list(AREA_STATUSES):
        errors.append("ownership registry schema area statuses drifted")
    if entry_definition.get("properties", {}).get("status", {}).get("enum") != list(ENTRY_STATUSES):
        errors.append("ownership registry schema entry statuses drifted")
    if entry_definition.get("properties", {}).get("path", {}).get("$ref") != "#/$defs/exact_path":
        errors.append("ownership registry schema entry path must use exact_path")
    for field, expected_fields in (
        ("owners", {"product", "upstream"}),
        ("classification_contract", CLASSIFICATION_FIELDS),
        ("entry_contract", ENTRY_CONTRACT_FIELDS),
    ):
        definition = properties.get(field)
        if (
            type(definition) is not dict
            or definition.get("type") != "object"
            or definition.get("additionalProperties") is not False
            or type(definition.get("required")) is not list
            or set(definition["required"]) != expected_fields
            or type(definition.get("properties")) is not dict
            or set(definition["properties"]) != expected_fields
        ):
            errors.append(f"ownership registry schema {field} shape drifted")
    schema_rules = properties.get("entry_contract", {}).get("properties", {}).get("rules", {})
    if type(schema_rules) is not dict or schema_rules.get("const") != EXPECTED_ENTRY_RULES:
        errors.append("ownership registry schema executable entry rules drifted")


def validate_header(
    registry: dict[str, Any], known_beads: set[str], errors: list[str]
) -> None:
    validate_closed_object(registry, "convergence map", ROOT_FIELDS, errors)
    if type(registry.get("schema_version")) is not int or registry.get("schema_version") != 2:
        errors.append(
            "convergence map schema_version must be integer 2; schema v1 must be migrated before validation"
        )
    if registry.get("$schema") != SCHEMA_REFERENCE:
        errors.append(f"convergence map $schema must be {SCHEMA_REFERENCE!r}")
    bead_id = canonical_string(registry.get("bead_id"), "convergence map.bead_id", errors)
    if bead_id != EXPECTED_BEAD_ID:
        errors.append(f"convergence map.bead_id must be {EXPECTED_BEAD_ID}")
    if bead_id and bead_id not in known_beads:
        errors.append(f"convergence map.bead_id references unknown bead id {bead_id!r}")
    canonical_string(
        registry.get("policy_revision"), "convergence map.policy_revision", errors
    )
    validate_sha(
        registry.get("upstream_floor_sha"),
        "convergence map.upstream_floor_sha",
        errors,
    )

    contract = validate_closed_object(
        registry.get("classification_contract"),
        "convergence map.classification_contract",
        CLASSIFICATION_FIELDS,
        errors,
    )
    if contract is not None:
        for key, expected in EXPECTED_CLASSIFICATION_CONTRACT.items():
            value = contract.get(key)
            if type(expected) is list:
                if type(value) is not list or value != expected:
                    errors.append(
                        f"classification_contract.{key} must equal {expected!r}"
                    )
            elif value != expected:
                errors.append(
                    f"classification_contract.{key} must equal {expected!r}"
                )

    entry_contract = validate_closed_object(
        registry.get("entry_contract"),
        "convergence map.entry_contract",
        ENTRY_CONTRACT_FIELDS,
        errors,
    )
    if entry_contract is not None:
        rules = string_list(entry_contract.get("rules"), "entry_contract.rules", errors)
        for offset, rule in enumerate(rules):
            substantive_prose(rule, f"entry_contract.rules[{offset}]", errors)
        if rules != EXPECTED_ENTRY_RULES:
            errors.append(
                f"entry_contract.rules must equal the executable rules {EXPECTED_ENTRY_RULES!r}"
            )
        for field, expected in (
            ("area_status_values", list(AREA_STATUSES)),
            ("entry_status_values", list(ENTRY_STATUSES)),
        ):
            value = entry_contract.get(field)
            if type(value) is not list or value != expected:
                errors.append(f"entry_contract.{field} must equal {expected!r}")


def validate_authorities(
    registry: dict[str, Any],
    sync_policy: dict[str, Any],
    policy: dict[str, Any],
    metadata: dict[str, Any],
    errors: list[str],
) -> tuple[str, dict[str, str]]:
    pinned = nested_object(metadata, "pinned_floor", "floor metadata", errors)
    source = nested_object(metadata, "source", "floor metadata", errors)
    product_metadata = nested_object(metadata, "product", "floor metadata", errors)
    accepted_floor = validate_sha(
        pinned.get("sha"), "floor metadata.pinned_floor.sha", errors
    )
    if pinned.get("must_be_ancestor_of_product_head") is not True:
        errors.append(
            "floor metadata.pinned_floor.must_be_ancestor_of_product_head must be true"
        )
    source_repository = canonical_string(
        source.get("repository"), "floor metadata.source.repository", errors
    )
    product_repository = canonical_string(
        product_metadata.get("repository"),
        "floor metadata.product.repository",
        errors,
    )
    pull_request = source.get("pull_request")
    if type(pull_request) is not int or pull_request <= 0:
        errors.append("floor metadata.source.pull_request must be a positive integer")

    sync_ownership = nested_object(sync_policy, "ownership", "sync policy", errors)
    sync_remotes = nested_object(sync_policy, "remotes", "sync policy", errors)
    sync_product = nested_object(sync_remotes, "product", "sync policy.remotes", errors)
    sync_upstream = nested_object(sync_remotes, "upstream", "sync policy.remotes", errors)
    sync_floor = nested_object(sync_policy, "floor", "sync policy", errors)
    policy_floor = nested_object(policy, "upstream_floor", "patch policy", errors)

    for label, actual in (
        ("sync policy.floor.sha", sync_floor.get("sha")),
        ("patch policy.upstream_floor.sha", policy_floor.get("sha")),
        ("convergence map.upstream_floor_sha", registry.get("upstream_floor_sha")),
    ):
        if actual != accepted_floor:
            errors.append(f"{label} must equal the canonical pinned floor")
    for label, actual in (
        ("sync policy.floor.pull_request", sync_floor.get("pull_request")),
        ("patch policy.upstream_floor.pull_request", policy_floor.get("pull_request")),
    ):
        if type(actual) is not int or actual != pull_request:
            errors.append(f"{label} must equal the canonical pull request")
    for label, actual in (
        ("sync policy.floor.metadata", sync_floor.get("metadata")),
        ("patch policy.upstream_floor.metadata", policy_floor.get("metadata")),
    ):
        if actual != "product/upstream/tracedecay-v2-pr707.json":
            errors.append(f"{label} must reference canonical upstream metadata")
    if policy_floor.get("repository") != source_repository:
        errors.append(
            "patch policy.upstream_floor.repository must equal canonical upstream repository"
        )
    if registry.get("policy_revision") != policy.get("policy_revision"):
        errors.append("convergence map.policy_revision must equal patch policy revision")

    owners = validate_closed_object(
        registry.get("owners"), "convergence map.owners", {"product", "upstream"}, errors
    )
    owner_values: dict[str, str] = {"product": "", "upstream": ""}
    if owners is None:
        return accepted_floor, owner_values
    product_owner = validate_closed_object(
        owners.get("product"), "owners.product", OWNER_FIELDS, errors
    )
    upstream_owner = validate_closed_object(
        owners.get("upstream"), "owners.upstream", OWNER_FIELDS, errors
    )
    product_id = (
        canonical_string(product_owner.get("id"), "owners.product.id", errors)
        if product_owner is not None
        else ""
    )
    product_repo = (
        canonical_string(
            product_owner.get("repository"), "owners.product.repository", errors
        )
        if product_owner is not None
        else ""
    )
    upstream_id = (
        canonical_string(upstream_owner.get("id"), "owners.upstream.id", errors)
        if upstream_owner is not None
        else ""
    )
    upstream_repo = (
        canonical_string(
            upstream_owner.get("repository"), "owners.upstream.repository", errors
        )
        if upstream_owner is not None
        else ""
    )
    owner_values = {"product": product_id, "upstream": upstream_id}
    for label, repository, expected in (
        ("owners.product.repository", product_repo, product_repository),
        ("owners.upstream.repository", upstream_repo, source_repository),
    ):
        if REPOSITORY_RE.fullmatch(repository) is None:
            errors.append(f"{label} must use owner/repository syntax")
        if repository != expected:
            errors.append(f"{label} must equal canonical repository {expected!r}")
    if product_id != sync_ownership.get("sync_owner"):
        errors.append("owners.product.id must equal sync policy sync_owner")
    patch_owners = sync_ownership.get("product_patch_owners")
    if type(patch_owners) is not list or product_id not in patch_owners:
        errors.append("owners.product.id must be a declared product patch owner")
    if upstream_id != sync_ownership.get("review_owner"):
        errors.append("owners.upstream.id must equal sync policy review_owner")
    if product_repo != sync_product.get("repository"):
        errors.append("owners.product.repository must equal sync policy product repository")
    if upstream_repo != sync_upstream.get("repository"):
        errors.append("owners.upstream.repository must equal sync policy upstream repository")
    if product_repo and product_repo.split("/", 1)[0] != product_id:
        errors.append("owners.product.id must equal its repository owner segment")
    if upstream_repo and upstream_repo.split("/", 1)[0] != upstream_id:
        errors.append("owners.upstream.id must equal its repository owner segment")
    return accepted_floor, owner_values


def index_touch_points(
    policy: dict[str, Any], errors: list[str]
) -> dict[str, list[str]]:
    rows = policy.get("allowed_touch_points")
    if type(rows) is not list:
        errors.append("patch policy.allowed_touch_points must be an array")
        return {}
    touches: dict[str, list[str]] = {}
    for offset, raw in enumerate(rows):
        if type(raw) is not dict:
            errors.append(f"patch policy.allowed_touch_points[{offset}] must be an object")
            continue
        touch_id = canonical_string(
            raw.get("id"), f"patch policy.allowed_touch_points[{offset}].id", errors
        )
        paths = string_list(
            raw.get("paths"),
            f"patch policy.allowed_touch_points[{offset}].paths",
            errors,
        )
        for path_offset, path in enumerate(paths):
            validate_repo_path(
                path,
                f"patch policy.allowed_touch_points[{offset}].paths[{path_offset}]",
                errors,
                allow_glob=True,
            )
        if touch_id in touches:
            errors.append(f"patch policy contains duplicate touch point {touch_id!r}")
        elif touch_id:
            touches[touch_id] = paths
    return touches


def index_administrative_exclusions(
    policy: dict[str, Any], errors: list[str]
) -> list[str]:
    """Load the policy-owned path exclusions used by footprint accounting.

    Exclusions are intentionally sourced from the patch policy rather than
    duplicated in this checker.  If the policy field or any of its patterns
    is malformed, return no usable exclusions so a malformed policy cannot
    silently authorize a path.
    """
    label = "patch policy.administrative_paths_excluded_from_footprint"
    before = len(errors)
    patterns = string_list(
        policy.get("administrative_paths_excluded_from_footprint"),
        label,
        errors,
    )
    for offset, pattern in enumerate(patterns):
        validate_repo_path(
            pattern,
            f"{label}[{offset}]",
            errors,
            allow_glob=True,
        )
    return patterns if len(errors) == before else []


def validate_areas(
    repo: Path,
    registry: dict[str, Any],
    accepted_floor: str,
    owners: dict[str, str],
    touches: dict[str, list[str]],
    product_patterns: list[str],
    known_beads: set[str],
    errors: list[str],
) -> dict[str, dict[str, Any]]:
    rows = registry.get("areas")
    if type(rows) is not list:
        errors.append("convergence map.areas must be an array")
        return {}
    if not rows:
        errors.append("convergence map.areas must not be empty")
    indexed: dict[str, dict[str, Any]] = {}
    pattern_owners: dict[str, str] = {}
    for offset, raw in enumerate(rows):
        label = f"areas[{offset}]"
        area = validate_closed_object(raw, label, AREA_FIELDS, errors)
        if area is None:
            continue
        area_id = canonical_string(area.get("id"), f"{label}.id", errors)
        if AREA_ID_RE.fullmatch(area_id) is None:
            errors.append(f"{label}.id must be normalized snake_case")
        if area_id in indexed:
            errors.append(f"convergence map.areas contains duplicate id {area_id!r}")
        elif area_id:
            indexed[area_id] = area
        status = area.get("status")
        if type(status) is not str or status not in AREA_STATUSES:
            errors.append(f"{label}.status must be one of {list(AREA_STATUSES)!r}")
        ownership_class = area.get("ownership_class")
        if type(ownership_class) is not str or ownership_class not in OWNERSHIP_CLASSES:
            errors.append(f"{label}.ownership_class is invalid")
        owner = canonical_string(area.get("owner"), f"{label}.owner", errors)
        expected_owner = (
            owners.get("product")
            if ownership_class == "product_owned"
            else owners.get("upstream")
        )
        if expected_owner and owner != expected_owner:
            errors.append(
                f"{label}.owner must equal the canonical {ownership_class} owner"
            )
        feature = canonical_string(area.get("feature"), f"{label}.feature", errors)
        if FEATURE_RE.fullmatch(feature) is None:
            errors.append(f"{label}.feature must be a normalized feature name")
        path_patterns = string_list(
            area.get("path_patterns"), f"{label}.path_patterns", errors
        )
        touch_ids = string_list(area.get("touch_points"), f"{label}.touch_points", errors)
        for touch_id in touch_ids:
            if touch_id not in touches:
                errors.append(f"{label}.touch_points references unknown {touch_id!r}")
        for pattern_offset, pattern in enumerate(path_patterns):
            validate_repo_path(
                pattern,
                f"{label}.path_patterns[{pattern_offset}]",
                errors,
                allow_glob=True,
            )
            prior = pattern_owners.get(pattern)
            if prior is not None:
                errors.append(
                    f"area path pattern {pattern!r} is duplicated by {prior!r} and {area_id!r}"
                )
            else:
                pattern_owners[pattern] = area_id
            if ownership_class == "product_owned":
                if not any(
                    pattern_is_covered(pattern, allowed)
                    for allowed in product_patterns
                ):
                    errors.append(
                        f"{label}.path_patterns pattern {pattern!r} is outside canonical product-owned paths"
                    )
            elif ownership_class == "upstream_owned":
                allowed_paths = [
                    allowed
                    for touch_id in touch_ids
                    for allowed in touches.get(touch_id, [])
                ]
                if not any(
                    pattern_is_covered(pattern, allowed) for allowed in allowed_paths
                ):
                    errors.append(
                        f"{label}.path_patterns pattern {pattern!r} is not bounded by its policy touch points"
                    )
        validate_bead_ids(
            area.get("bead_ids"), f"{label}.bead_ids", known_beads, errors
        )
        substantive_prose(area.get("rationale"), f"{label}.rationale", errors)
        invariants = string_list(
            area.get("semantic_invariants"), f"{label}.semantic_invariants", errors
        )
        for invariant_offset, invariant in enumerate(invariants):
            substantive_prose(
                invariant,
                f"{label}.semantic_invariants[{invariant_offset}]",
                errors,
            )
        validate_test_commands(area.get("tests"), f"{label}.tests", repo, errors)
        sha = validate_sha(
            area.get("last_verified_upstream_sha"),
            f"{label}.last_verified_upstream_sha",
            errors,
        )
        if status in {"active", "planned"} and sha != accepted_floor:
            errors.append(
                f"{label}.last_verified_upstream_sha must equal the canonical pinned floor"
            )
        upstreamability = validate_closed_object(
            area.get("upstreamability"),
            f"{label}.upstreamability",
            UPSTREAMABILITY_FIELDS,
            errors,
        )
        if upstreamability is not None:
            kind = upstreamability.get("kind")
            if type(kind) is not str or kind not in UPSTREAMABILITY_KINDS:
                errors.append(f"{label}.upstreamability.kind is invalid")
            substantive_prose(
                upstreamability.get("rationale"),
                f"{label}.upstreamability.rationale",
                errors,
            )
    return indexed


def validate_entries(
    repo: Path,
    registry: dict[str, Any],
    accepted_floor: str,
    owners: dict[str, str],
    areas: dict[str, dict[str, Any]],
    touches: dict[str, list[str]],
    product_patterns: list[str],
    known_beads: set[str],
    errors: list[str],
) -> dict[str, dict[str, Any]]:
    rows = registry.get("entries")
    if type(rows) is not list:
        errors.append("convergence map.entries must be an array")
        return {}
    indexed: dict[str, dict[str, Any]] = {}
    for offset, raw in enumerate(rows):
        label = f"entries[{offset}]"
        entry = validate_closed_object(
            raw,
            label,
            ENTRY_REQUIRED_FIELDS,
            errors,
            optional=ENTRY_OPTIONAL_FIELDS,
        )
        if entry is None:
            continue
        path = validate_repo_path(
            entry.get("path"), f"{label}.path", errors, allow_glob=False
        )
        if path in indexed:
            errors.append(f"convergence map.entries contains duplicate path {path!r}")
        elif path:
            indexed[path] = entry
        status = entry.get("status")
        if type(status) is not str or status not in ENTRY_STATUSES:
            errors.append(f"{label}.status must be one of {list(ENTRY_STATUSES)!r}")
        area_id = canonical_string(entry.get("area_id"), f"{label}.area_id", errors)
        area = areas.get(area_id)
        matching_upstream_areas = sorted(
            candidate_id
            for candidate_id, candidate in areas.items()
            if candidate.get("status") == "active"
            and candidate.get("ownership_class") == "upstream_owned"
            and any(
                type(pattern) is str and path_matches(path, pattern)
                for pattern in candidate.get("path_patterns", [])
            )
        )
        if status == "active" and matching_upstream_areas != [area_id]:
            errors.append(
                f"{label}.path must resolve to exactly its active upstream area; matched {matching_upstream_areas!r}"
            )
        if area is None:
            errors.append(f"{label}.area_id references unknown area {area_id!r}")
        else:
            if status == "active" and area.get("status") != "active":
                errors.append(f"{label} active entry must reference an active area")
            if area.get("ownership_class") != "upstream_owned":
                errors.append(f"{label} upstream entry must reference an upstream-owned area")
            patterns = area.get("path_patterns", [])
            if path and not any(
                type(pattern) is str and path_matches(path, pattern)
                for pattern in patterns
            ):
                errors.append(f"{label}.path is outside its referenced area")
        owner = canonical_string(entry.get("owner"), f"{label}.owner", errors)
        if owners.get("product") and owner != owners["product"]:
            errors.append(f"{label}.owner must equal the canonical product owner")
        upstream_owner = canonical_string(
            entry.get("upstream_owner"), f"{label}.upstream_owner", errors
        )
        if owners.get("upstream") and upstream_owner != owners["upstream"]:
            errors.append(
                f"{label}.upstream_owner must equal the canonical upstream owner"
            )
        touch_point = canonical_string(
            entry.get("touch_point"), f"{label}.touch_point", errors
        )
        if touch_point not in touches:
            errors.append(f"{label}.touch_point references unknown policy touch point")
        if area is not None and touch_point not in area.get("touch_points", []):
            errors.append(f"{label}.touch_point is not declared by its area")
        matching_touches = [
            touch_id
            for touch_id, patterns in touches.items()
            if path and any(path_matches(path, pattern) for pattern in patterns)
        ]
        if len(matching_touches) != 1:
            errors.append(
                f"{label}.path must resolve to exactly one policy touch point; matched {matching_touches!r}"
            )
        elif matching_touches[0] != touch_point:
            errors.append(
                f"{label}.touch_point does not authorize exact path {path!r}"
            )
        if path and any(path_matches(path, pattern) for pattern in product_patterns):
            errors.append(f"{label}.path is product-owned and cannot be an upstream entry")
        substantive_prose(entry.get("rationale"), f"{label}.rationale", errors)
        substantive_prose(
            entry.get("rebase_or_removal_plan"),
            f"{label}.rebase_or_removal_plan",
            errors,
        )
        invariants = string_list(
            entry.get("semantic_invariants"), f"{label}.semantic_invariants", errors
        )
        for invariant_offset, invariant in enumerate(invariants):
            substantive_prose(
                invariant,
                f"{label}.semantic_invariants[{invariant_offset}]",
                errors,
            )
        string_list(entry.get("verification"), f"{label}.verification", errors)
        validate_test_commands(entry.get("tests"), f"{label}.tests", repo, errors)
        validate_bead_ids(
            entry.get("bead_ids"), f"{label}.bead_ids", known_beads, errors
        )
        budget = entry.get("line_budget")
        if type(budget) is not int or budget <= 0:
            errors.append(f"{label}.line_budget must be a positive integer")
        sha = validate_sha(
            entry.get("last_verified_upstream_sha"),
            f"{label}.last_verified_upstream_sha",
            errors,
        )
        if status == "active" and sha != accepted_floor:
            errors.append(
                f"{label}.last_verified_upstream_sha must equal the canonical pinned floor"
            )
        upstreamability = validate_closed_object(
            entry.get("upstreamability"),
            f"{label}.upstreamability",
            UPSTREAMABILITY_FIELDS,
            errors,
        )
        if upstreamability is not None:
            kind = upstreamability.get("kind")
            if type(kind) is not str or kind not in UPSTREAMABILITY_KINDS:
                errors.append(f"{label}.upstreamability.kind is invalid")
            substantive_prose(
                upstreamability.get("rationale"),
                f"{label}.upstreamability.rationale",
                errors,
            )
        if "generated" in entry:
            generated = validate_closed_object(
                entry.get("generated"),
                f"{label}.generated",
                GENERATED_FIELDS,
                errors,
            )
            if generated is not None:
                validate_repo_path(
                    generated.get("generator_path"),
                    f"{label}.generated.generator_path",
                    errors,
                    allow_glob=False,
                )
                canonical_string(
                    generated.get("reproduction"),
                    f"{label}.generated.reproduction",
                    errors,
                )
                canonical_string(
                    generated.get("zero_drift_check"),
                    f"{label}.generated.zero_drift_check",
                    errors,
                )
    return indexed


def matching_area_ids(
    path: str,
    areas: dict[str, dict[str, Any]],
    *,
    ownership_class: str,
) -> list[str]:
    return sorted(
        area_id
        for area_id, area in areas.items()
        if area.get("status") == "active"
        and area.get("ownership_class") == ownership_class
        and any(
            type(pattern) is str and path_matches(path, pattern)
            for pattern in area.get("path_patterns", [])
        )
    )


def classify_paths(
    requested_paths: Iterable[str],
    areas: dict[str, dict[str, Any]],
    entries: dict[str, dict[str, Any]],
    touches: dict[str, list[str]],
    product_patterns: list[str],
    errors: list[str],
    *,
    administrative_patterns: Iterable[str] = (),
) -> list[dict[str, str]]:
    results: list[dict[str, str]] = []
    seen: set[str] = set()
    active_entries = {
        path: entry
        for path, entry in entries.items()
        if entry.get("status") == "active"
    }
    retired_entries = {
        path: entry
        for path, entry in entries.items()
        if entry.get("status") == "retired"
    }
    for offset, raw_path in enumerate(requested_paths):
        path = validate_repo_path(
            raw_path, f"classify_path[{offset}]", errors, allow_glob=False
        )
        if not path:
            continue
        if path in seen:
            errors.append(f"classify_path contains duplicate path {path!r}")
            continue
        seen.add(path)
        if any(
            type(pattern) is str and path_matches(path, pattern)
            for pattern in administrative_patterns
        ):
            continue
        active_entry = active_entries.get(path)
        if active_entry is not None:
            results.append(
                {
                    "path": path,
                    "kind": "upstream_entry",
                    "area_id": str(active_entry.get("area_id", "")),
                    "touch_point": str(active_entry.get("touch_point", "")),
                }
            )
            continue
        if path in retired_entries:
            errors.append(
                f"path {path!r} has only a retired entry and is not authorized"
            )
            continue
        product_area_ids = matching_area_ids(
            path, areas, ownership_class="product_owned"
        )
        if len(product_area_ids) > 1:
            errors.append(
                f"path {path!r} ambiguously matches active product areas {product_area_ids!r}"
            )
            continue
        if len(product_area_ids) == 1:
            results.append(
                {
                    "path": path,
                    "kind": "product_area",
                    "area_id": product_area_ids[0],
                }
            )
            continue
        upstream_area_ids = matching_area_ids(
            path, areas, ownership_class="upstream_owned"
        )
        touch_ids = sorted(
            touch_id
            for touch_id, patterns in touches.items()
            if any(path_matches(path, pattern) for pattern in patterns)
        )
        if len(touch_ids) > 1:
            errors.append(
                f"path {path!r} ambiguously matches policy touch points {touch_ids!r}"
            )
        elif upstream_area_ids or touch_ids:
            errors.append(
                f"upstream path {path!r} lacks an active exact convergence entry"
            )
        elif any(path_matches(path, pattern) for pattern in product_patterns):
            errors.append(
                f"product path {path!r} is unclassified by active ownership areas"
            )
        else:
            errors.append(f"path {path!r} is unclassified by the M2 ownership registry")
    return results


def changed_repository_paths(
    repo: Path, accepted_floor: str, errors: list[str]
) -> list[str]:
    commands = [
        [
            "git",
            "-C",
            str(repo),
            "diff",
            "--name-only",
            "--diff-filter=ACDMRTUXB",
            "-z",
            accepted_floor,
            "--",
        ],
        [
            "git",
            "-C",
            str(repo),
            "ls-files",
            "--others",
            "--exclude-standard",
            "-z",
        ],
    ]
    paths: set[str] = set()
    for command in commands:
        try:
            result = subprocess.run(command, check=False, capture_output=True)
        except OSError as exc:
            errors.append(f"could not enumerate changed repository paths: {exc}")
            continue
        if result.returncode != 0:
            detail = result.stderr.decode("utf-8", errors="replace").strip()
            errors.append(f"could not enumerate changed repository paths: {detail}")
            continue
        for raw in result.stdout.split(b"\0"):
            if not raw:
                continue
            try:
                paths.add(raw.decode("utf-8"))
            except UnicodeDecodeError:
                errors.append("changed repository path is not valid UTF-8")
    return sorted(paths)


def validate_document(
    repo: Path,
    schema: dict[str, Any],
    registry: dict[str, Any],
    sync_policy: dict[str, Any],
    policy: dict[str, Any],
    metadata: dict[str, Any],
    known_beads: set[str],
    requested_paths: Iterable[str],
    *,
    classify_changed_paths: bool = True,
) -> tuple[list[str], dict[str, Any], list[dict[str, str]]]:
    errors: list[str] = []
    validate_schema_definition(schema, errors)
    validate_header(registry, known_beads, errors)
    accepted_floor, owners = validate_authorities(
        registry, sync_policy, policy, metadata, errors
    )
    touches = index_touch_points(policy, errors)
    administrative_patterns = index_administrative_exclusions(policy, errors)
    product_patterns = string_list(
        policy.get("product_owned_paths"),
        "patch policy.product_owned_paths",
        errors,
    )
    if len(product_patterns) != len(CANONICAL_PRODUCT_PATTERNS) or set(product_patterns) != CANONICAL_PRODUCT_PATTERNS:
        errors.append("patch policy.product_owned_paths must equal the canonical product pattern set")
    for offset, pattern in enumerate(product_patterns):
        validate_repo_path(
            pattern,
            f"patch policy.product_owned_paths[{offset}]",
            errors,
            allow_glob=True,
        )
    areas = validate_areas(
        repo,
        registry,
        accepted_floor,
        owners,
        touches,
        product_patterns,
        known_beads,
        errors,
    )
    entries = validate_entries(
        repo,
        registry,
        accepted_floor,
        owners,
        areas,
        touches,
        product_patterns,
        known_beads,
        errors,
    )
    classifications = classify_paths(
        requested_paths,
        areas,
        entries,
        touches,
        product_patterns,
        errors,
        administrative_patterns=administrative_patterns,
    )
    changed_paths: list[str] = []
    changed_classifications: list[dict[str, str]] = []
    if (
        classify_changed_paths
        and type(registry.get("schema_version")) is int
        and registry.get("schema_version") == 2
    ):
        changed_paths = changed_repository_paths(repo, accepted_floor, errors)
        changed_classifications = classify_paths(
            changed_paths,
            areas,
            entries,
            touches,
            product_patterns,
            errors,
            administrative_patterns=administrative_patterns,
        )
    area_counts = Counter(
        area.get("status")
        for area in areas.values()
        if area.get("status") in AREA_STATUSES
    )
    entry_counts = Counter(
        entry.get("status")
        for entry in entries.values()
        if entry.get("status") in ENTRY_STATUSES
    )
    classification_counts = Counter(row["kind"] for row in classifications)
    counts = {
        "areas": {
            "active": area_counts["active"],
            "planned": area_counts["planned"],
            "retired": area_counts["retired"],
            "total": len(areas),
        },
        "entries": {
            "active": entry_counts["active"],
            "retired": entry_counts["retired"],
            "total": len(entries),
        },
        "classifications": {
            "product_area": classification_counts["product_area"],
            "upstream_entry": classification_counts["upstream_entry"],
            "total": len(classifications),
        },
        "policy_touch_points": len(touches),
        "changed_paths": {
            "classified": len(changed_classifications),
            "total": len(changed_paths),
        },
    }
    unique_errors = sorted(dict.fromkeys(errors))
    counts["validation_errors"] = len(unique_errors)
    if len(unique_errors) > MAX_DIAGNOSTIC_ERRORS:
        omitted = len(unique_errors) - MAX_DIAGNOSTIC_ERRORS
        unique_errors = unique_errors[:MAX_DIAGNOSTIC_ERRORS] + [
            f"{omitted} additional validation errors omitted"
        ]
    return unique_errors, counts, classifications


def main() -> int:
    args = parse_args()
    repo = args.repo.resolve()
    bootstrap_errors: list[str] = []
    schema = load_object(resolve(repo, args.schema), "ownership registry schema", bootstrap_errors)
    registry = load_object(resolve(repo, args.map_path), "convergence map", bootstrap_errors)
    policy = load_object(resolve(repo, args.policy), "patch policy", bootstrap_errors)
    sync_policy = load_object(resolve(repo, args.sync_policy), "sync policy", bootstrap_errors)
    metadata = load_object(resolve(repo, args.floor_metadata), "floor metadata", bootstrap_errors)
    known_beads = load_bead_ids(resolve(repo, args.beads), bootstrap_errors)
    if bootstrap_errors:
        print(
            json.dumps(
                {"ok": False, "errors": sorted(dict.fromkeys(bootstrap_errors))},
                indent=2,
                sort_keys=True,
            )
        )
        return 1

    errors, counts, classifications = validate_document(
        repo,
        schema,
        registry,
        sync_policy,
        policy,
        metadata,
        known_beads,
        args.classify_path,
        classify_changed_paths=not args.skip_changed_path_classification,
    )
    if errors:
        print(
            json.dumps(
                {"ok": False, "errors": errors, "counts": counts},
                indent=2,
                sort_keys=True,
            )
        )
        return 1
    print(
        json.dumps(
            {
                "ok": True,
                "schema_version": EXPECTED_SCHEMA_VERSION,
                "counts": counts,
                "classifications": classifications,
            },
            indent=2,
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
