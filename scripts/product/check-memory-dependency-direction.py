#!/usr/bin/env python3
"""Enforce the product memory dependency graph from Cargo metadata."""

from __future__ import annotations

import argparse
import fnmatch
import json
import re
import subprocess
import sys
from pathlib import Path
from typing import Any

DATE_RE = re.compile(r"^[0-9]{4}-[0-9]{2}-[0-9]{2}$")
REQUIRED_EXCEPTION_FIELDS = (
    "id",
    "rule_id",
    "from_package",
    "to_package",
    "adr",
    "rationale",
    "owner",
    "verification",
    "review_after",
)


def load_json(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        raise ValueError(f"cannot read JSON {path}: {error}") from error
    if not isinstance(value, dict):
        raise ValueError(f"JSON root must be an object: {path}")
    return value


def dependency_names(package: dict[str, Any]) -> set[str]:
    names: set[str] = set()
    for dependency in package.get("dependencies", []):
        if isinstance(dependency, str):
            names.add(dependency)
        elif isinstance(dependency, dict) and isinstance(dependency.get("name"), str):
            names.add(dependency["name"])
        else:
            raise ValueError(
                f"package {package.get('name', '<unknown>')} has malformed dependency metadata"
            )
    return names


def matches_any(value: str, patterns: list[str]) -> bool:
    return any(fnmatch.fnmatchcase(value, pattern) for pattern in patterns)


def validate_exceptions(
    repo: Path, policy: dict[str, Any]
) -> tuple[dict[tuple[str, str, str], dict[str, Any]], list[str]]:
    errors: list[str] = []
    index: dict[tuple[str, str, str], dict[str, Any]] = {}
    exceptions = policy.get("exceptions", [])
    if not isinstance(exceptions, list):
        return index, ["policy exceptions must be an array"]
    for offset, exception in enumerate(exceptions):
        label = f"exception[{offset}]"
        if not isinstance(exception, dict):
            errors.append(f"{label} must be an object")
            continue
        for field in REQUIRED_EXCEPTION_FIELDS:
            value = exception.get(field)
            if field == "verification":
                if not isinstance(value, list) or not value or not all(
                    isinstance(item, str) and item.strip() for item in value
                ):
                    errors.append(f"{label}.{field} must be a non-empty string array")
            elif not isinstance(value, str) or not value.strip():
                errors.append(f"{label}.{field} must be a non-empty string")
        review_after = exception.get("review_after")
        if isinstance(review_after, str) and not DATE_RE.fullmatch(review_after):
            errors.append(f"{label}.review_after must use YYYY-MM-DD")
        adr = exception.get("adr")
        if isinstance(adr, str) and adr:
            adr_path = Path(adr)
            if adr_path.is_absolute() or ".." in adr_path.parts:
                errors.append(f"{label}.adr must be a repository-relative path")
            elif not adr.startswith("product/architecture/adr/"):
                errors.append(f"{label}.adr must live under product/architecture/adr")
            elif not (repo / adr_path).is_file():
                errors.append(f"{label}.adr does not exist: {adr}")
        key_values = (
            exception.get("rule_id"),
            exception.get("from_package"),
            exception.get("to_package"),
        )
        if all(isinstance(value, str) and value for value in key_values):
            key = (key_values[0], key_values[1], key_values[2])
            if key in index:
                errors.append(
                    f"duplicate exception for rule {key[0]} edge {key[1]} -> {key[2]}"
                )
            else:
                index[key] = exception
    return index, errors


def load_metadata(
    repo: Path, policy: dict[str, Any], fixture: Path | None
) -> dict[str, Any]:
    if fixture is not None:
        return load_json(fixture)
    command = policy.get("metadata_command")
    if not isinstance(command, list) or not command or not all(
        isinstance(item, str) and item for item in command
    ):
        raise ValueError("policy metadata_command must be a non-empty string array")
    result = subprocess.run(
        command,
        cwd=repo,
        check=False,
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        detail = result.stderr.strip() or result.stdout.strip()
        raise ValueError(f"cargo metadata failed ({result.returncode}): {detail}")
    try:
        value = json.loads(result.stdout)
    except json.JSONDecodeError as error:
        raise ValueError(f"cargo metadata returned invalid JSON: {error}") from error
    if not isinstance(value, dict):
        raise ValueError("cargo metadata root must be an object")
    return value


def check_policy(
    repo: Path, policy: dict[str, Any], metadata: dict[str, Any]
) -> list[str]:
    errors: list[str] = []
    if policy.get("schema_version") != 1:
        errors.append("policy schema_version must be 1")
    raw_packages = metadata.get("packages")
    if not isinstance(raw_packages, list):
        return errors + ["cargo metadata packages must be an array"]
    packages: dict[str, dict[str, Any]] = {}
    for package in raw_packages:
        if not isinstance(package, dict) or not isinstance(package.get("name"), str):
            errors.append("cargo metadata contains a malformed package")
            continue
        name = package["name"]
        if name in packages:
            errors.append(f"cargo metadata contains duplicate package {name}")
        packages[name] = package

    exceptions, exception_errors = validate_exceptions(repo, policy)
    errors.extend(exception_errors)
    used_exceptions: set[tuple[str, str, str]] = set()

    def reject_or_except(rule_id: str, source: str, target: str, reason: str) -> None:
        key = (rule_id, source, target)
        if key in exceptions:
            used_exceptions.add(key)
        else:
            errors.append(
                f"forbidden dependency {source} -> {target} ({rule_id}): {reason}"
            )

    contracts = policy.get("package_contracts", [])
    if not isinstance(contracts, list):
        errors.append("policy package_contracts must be an array")
        contracts = []
    for contract in contracts:
        if not isinstance(contract, dict):
            errors.append("package contract must be an object")
            continue
        package_name = contract.get("package")
        if not isinstance(package_name, str) or not package_name:
            errors.append("package contract package must be a non-empty string")
            continue
        package = packages.get(package_name)
        if package is None:
            errors.append(f"managed package is missing from cargo metadata: {package_name}")
            continue
        allowed = contract.get("allowed_direct_dependencies", [])
        required = contract.get("required_direct_dependencies", [])
        if not isinstance(allowed, list) or not all(
            isinstance(item, str) for item in allowed
        ):
            errors.append(f"{package_name} allowed_direct_dependencies must be strings")
            continue
        if not isinstance(required, list) or not all(
            isinstance(item, str) for item in required
        ):
            errors.append(f"{package_name} required_direct_dependencies must be strings")
            continue
        allowed_set = set(allowed)
        required_set = set(required)
        if not required_set.issubset(allowed_set):
            errors.append(f"{package_name} required dependencies must also be allowed")
        actual = dependency_names(package)
        for missing in sorted(required_set - actual):
            errors.append(f"required dependency is missing: {package_name} -> {missing}")
        for target in sorted(actual - allowed_set):
            reject_or_except(
                f"package-contract:{package_name}",
                package_name,
                target,
                "dependency is outside the package allowlist",
            )

    rules = policy.get("rules", [])
    if not isinstance(rules, list):
        errors.append("policy rules must be an array")
        rules = []
    for rule in rules:
        if not isinstance(rule, dict):
            errors.append("dependency rule must be an object")
            continue
        rule_id = rule.get("id")
        from_patterns = rule.get("from_packages", [])
        forbidden_patterns = rule.get("forbidden_dependencies", [])
        allowed_patterns = rule.get("allowed_dependencies", [])
        reason = rule.get("reason")
        if not isinstance(rule_id, str) or not rule_id:
            errors.append("dependency rule id must be a non-empty string")
            continue
        if not isinstance(reason, str) or not reason:
            errors.append(f"dependency rule {rule_id} requires a reason")
        if not all(
            isinstance(values, list)
            and all(isinstance(item, str) for item in values)
            for values in (from_patterns, forbidden_patterns, allowed_patterns)
        ):
            errors.append(f"dependency rule {rule_id} patterns must be string arrays")
            continue
        for source, package in packages.items():
            if not matches_any(source, from_patterns):
                continue
            for target in sorted(dependency_names(package)):
                if matches_any(target, forbidden_patterns) and not matches_any(
                    target, allowed_patterns
                ):
                    reject_or_except(
                        rule_id,
                        source,
                        target,
                        reason or "forbidden by policy",
                    )

    for key in sorted(set(exceptions) - used_exceptions):
        errors.append(
            f"unused dependency exception {key[0]} for edge {key[1]} -> {key[2]}"
        )
    return errors


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo", default=".", help="repository root")
    parser.add_argument(
        "--policy",
        default="product/architecture/memory-dependency-policy.json",
        help="policy path relative to the repository",
    )
    parser.add_argument(
        "--metadata-fixture",
        help="read Cargo metadata JSON from this path instead of invoking Cargo",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(sys.argv[1:] if argv is None else argv)
    repo = Path(args.repo).resolve()
    policy_path = Path(args.policy)
    if not policy_path.is_absolute():
        policy_path = repo / policy_path
    fixture = Path(args.metadata_fixture).resolve() if args.metadata_fixture else None
    try:
        policy = load_json(policy_path)
        metadata = load_metadata(repo, policy, fixture)
        errors = check_policy(repo, policy, metadata)
    except ValueError as error:
        print(f"memory dependency policy error: {error}", file=sys.stderr)
        return 2
    if errors:
        for error in errors:
            print(f"memory dependency policy violation: {error}", file=sys.stderr)
        return 1
    print("memory dependency direction verified")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
