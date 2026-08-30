#!/usr/bin/env python3
"""Enforce Memory Fabric crate dependency direction and exact ADR-bound exceptions."""

from __future__ import annotations

import argparse
import fnmatch
import json
import sys
import tomllib
from pathlib import Path
from typing import Any, Iterable

REQUIRED_EXCEPTION_FIELDS = {
    "id",
    "rule_id",
    "from_package",
    "to_package",
    "adr",
    "rationale",
    "reviewed_by",
    "status",
}
EXPECTED_STATUS_VALUES = {"active", "retired"}
EXPECTED_ADR_PREFIX = "product/architecture/adr/"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--repo", type=Path, default=Path("."))
    parser.add_argument(
        "--policy",
        type=Path,
        default=Path("product/upstream/patch-footprint-policy.json"),
    )
    return parser.parse_args()


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


def package_matches(package: str, patterns: Iterable[str]) -> bool:
    return any(fnmatch.fnmatchcase(package, pattern) for pattern in patterns)


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
            if not isinstance(target, dict):
                continue
            for key in ("dependencies", "dev-dependencies", "build-dependencies"):
                collect(target.get(key))
    return names


def scan_manifests(
    repo: Path, errors: list[str]
) -> dict[str, tuple[Path, set[str]]]:
    manifests: dict[str, tuple[Path, set[str]]] = {}
    for path in sorted((repo / "crates").glob("*/Cargo.toml")):
        try:
            document = tomllib.loads(path.read_text(encoding="utf-8"))
        except (OSError, tomllib.TOMLDecodeError) as exc:
            errors.append(f"could not parse {path.relative_to(repo)}: {exc}")
            continue
        package = document.get("package")
        name = package.get("name") if isinstance(package, dict) else None
        if not isinstance(name, str) or not name:
            errors.append(f"{path.relative_to(repo)} has no package.name")
            continue
        if name in manifests:
            errors.append(f"duplicate workspace package name {name!r}")
            continue
        manifests[name] = (path, dependency_names(document))
    return manifests


def index_rules(policy: dict[str, Any], errors: list[str]) -> dict[str, dict[str, Any]]:
    rows = policy.get("dependency_direction_rules")
    if not isinstance(rows, list):
        errors.append("dependency_direction_rules must be an array")
        return {}
    rules: dict[str, dict[str, Any]] = {}
    for offset, raw in enumerate(rows):
        if not isinstance(raw, dict):
            errors.append(f"dependency_direction_rules[{offset}] must be an object")
            continue
        rule_id = raw.get("id")
        if not isinstance(rule_id, str) or not rule_id:
            errors.append(f"dependency_direction_rules[{offset}].id must be non-empty")
            continue
        if rule_id in rules:
            errors.append(f"duplicate dependency direction rule {rule_id!r}")
            continue
        from_packages = raw.get("from_packages")
        forbidden = raw.get("forbidden_dependencies")
        if not isinstance(from_packages, list) or not all(
            isinstance(value, str) and value for value in from_packages
        ):
            errors.append(f"dependency rule {rule_id} has invalid from_packages")
        if not isinstance(forbidden, list) or not all(
            isinstance(value, str) and value for value in forbidden
        ):
            errors.append(f"dependency rule {rule_id} has invalid forbidden_dependencies")
        rules[rule_id] = raw
    return rules


def contains_glob(value: str) -> bool:
    return any(character in value for character in "*?[]")


def validate_exception_contract(policy: dict[str, Any], errors: list[str]) -> None:
    contract = policy.get("dependency_direction_exception_contract")
    if not isinstance(contract, dict):
        errors.append("dependency_direction_exception_contract must be an object")
        return
    fields = contract.get("required_fields")
    if not isinstance(fields, list) or set(fields) != REQUIRED_EXCEPTION_FIELDS:
        errors.append("dependency exception required_fields do not match the exact-edge contract")
    statuses = contract.get("status_values")
    if not isinstance(statuses, list) or set(statuses) != EXPECTED_STATUS_VALUES:
        errors.append("dependency exception status_values must be active and retired")
    if contract.get("exact_edge_only") is not True:
        errors.append("dependency exceptions must be exact-edge-only")
    if contract.get("adr_prefix") != EXPECTED_ADR_PREFIX:
        errors.append(f"dependency exception ADR prefix must be {EXPECTED_ADR_PREFIX}")


def validate_exceptions(
    repo: Path,
    policy: dict[str, Any],
    rules: dict[str, dict[str, Any]],
    errors: list[str],
) -> tuple[dict[tuple[str, str, str], dict[str, Any]], int]:
    validate_exception_contract(policy, errors)
    rows = policy.get("dependency_direction_exceptions")
    if not isinstance(rows, list):
        errors.append("dependency_direction_exceptions must be an array")
        return {}, 0
    active: dict[tuple[str, str, str], dict[str, Any]] = {}
    seen_ids: set[str] = set()
    seen_keys: set[tuple[str, str, str]] = set()
    for offset, raw in enumerate(rows):
        label = f"dependency_direction_exceptions[{offset}]"
        if not isinstance(raw, dict):
            errors.append(f"{label} must be an object")
            continue
        missing = REQUIRED_EXCEPTION_FIELDS - raw.keys()
        if missing:
            errors.append(f"{label} missing fields: {sorted(missing)}")
        exception_id = raw.get("id")
        if not isinstance(exception_id, str) or not exception_id:
            errors.append(f"{label}.id must be non-empty")
        elif exception_id in seen_ids:
            errors.append(f"duplicate dependency exception id {exception_id!r}")
        else:
            seen_ids.add(exception_id)
        rule_id = raw.get("rule_id")
        source = raw.get("from_package")
        dependency = raw.get("to_package")
        status = raw.get("status")
        if status not in EXPECTED_STATUS_VALUES:
            errors.append(f"{label}.status must be active or retired")
        for field, value in (("from_package", source), ("to_package", dependency)):
            if not isinstance(value, str) or not value:
                errors.append(f"{label}.{field} must be non-empty")
            elif contains_glob(value):
                errors.append(f"{label}.{field} must name one exact package, not a glob")
        if not isinstance(rule_id, str) or rule_id not in rules:
            errors.append(f"{label}.rule_id must name an existing dependency rule")
            continue
        rule = rules[rule_id]
        if isinstance(source, str) and not contains_glob(source):
            from_patterns = [
                value for value in rule.get("from_packages", []) if isinstance(value, str)
            ]
            except_patterns = [
                value for value in rule.get("except_packages", []) if isinstance(value, str)
            ]
            if not package_matches(source, from_patterns) or package_matches(
                source, except_patterns
            ):
                errors.append(f"{label}.from_package is outside rule {rule_id}")
        if isinstance(dependency, str) and not contains_glob(dependency):
            forbidden = [
                value
                for value in rule.get("forbidden_dependencies", [])
                if isinstance(value, str)
            ]
            if not package_matches(dependency, forbidden):
                errors.append(f"{label}.to_package is not forbidden by rule {rule_id}")
        adr = raw.get("adr")
        if not isinstance(adr, str) or not adr.startswith(EXPECTED_ADR_PREFIX):
            errors.append(f"{label}.adr must be a product architecture ADR path")
        elif not (repo / adr).is_file():
            errors.append(f"{label}.adr does not exist: {adr}")
        for field in ("rationale", "reviewed_by"):
            value = raw.get(field)
            if not isinstance(value, str) or not value.strip():
                errors.append(f"{label}.{field} must be non-empty")
        if not (
            isinstance(rule_id, str)
            and isinstance(source, str)
            and isinstance(dependency, str)
            and not contains_glob(source)
            and not contains_glob(dependency)
        ):
            continue
        key = (rule_id, source, dependency)
        if key in seen_keys:
            errors.append(f"duplicate dependency exception edge {key!r}")
            continue
        seen_keys.add(key)
        if status == "active":
            active[key] = raw
    return active, len(rows)


def validate_repository(
    repo: Path, policy: dict[str, Any]
) -> tuple[list[str], dict[str, int]]:
    errors: list[str] = []
    rules = index_rules(policy, errors)
    active_exceptions, exception_count = validate_exceptions(repo, policy, rules, errors)
    manifests = scan_manifests(repo, errors)
    used_exceptions: set[tuple[str, str, str]] = set()
    violations = 0
    for rule_id, rule in rules.items():
        from_patterns = [
            value for value in rule.get("from_packages", []) if isinstance(value, str)
        ]
        except_patterns = [
            value for value in rule.get("except_packages", []) if isinstance(value, str)
        ]
        forbidden = [
            value
            for value in rule.get("forbidden_dependencies", [])
            if isinstance(value, str)
        ]
        allowed = [
            value
            for value in rule.get("allowed_dependencies", [])
            if isinstance(value, str)
        ]
        for package, (path, dependencies) in manifests.items():
            if not package_matches(package, from_patterns) or package_matches(
                package, except_patterns
            ):
                continue
            for dependency in sorted(dependencies):
                if not package_matches(dependency, forbidden):
                    continue
                if package_matches(dependency, allowed):
                    continue
                violations += 1
                key = (rule_id, package, dependency)
                if key in active_exceptions:
                    used_exceptions.add(key)
                else:
                    errors.append(
                        f"dependency direction {rule_id} violated: {package} -> {dependency} "
                        f"in {path.relative_to(repo)}"
                    )
    for key in sorted(active_exceptions.keys() - used_exceptions):
        errors.append(f"active dependency exception is stale or does not match an edge: {key!r}")
    return errors, {
        "rules": len(rules),
        "manifests_checked": len(manifests),
        "exceptions": exception_count,
        "active_exceptions": len(active_exceptions),
        "used_exceptions": len(used_exceptions),
        "forbidden_edges_observed": violations,
    }


def main() -> int:
    args = parse_args()
    repo = args.repo.resolve()
    policy_path = args.policy if args.policy.is_absolute() else repo / args.policy
    bootstrap_errors: list[str] = []
    policy = load_object(policy_path, "memory dependency policy", bootstrap_errors)
    if bootstrap_errors:
        print(json.dumps({"ok": False, "errors": bootstrap_errors}, indent=2, sort_keys=True))
        return 1
    errors, stats = validate_repository(repo, policy)
    if errors:
        print(json.dumps({"ok": False, "errors": errors, **stats}, indent=2, sort_keys=True))
        return 1
    print(
        json.dumps(
            {
                "ok": True,
                "policy": str(policy_path.relative_to(repo)),
                **stats,
            },
            indent=2,
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
