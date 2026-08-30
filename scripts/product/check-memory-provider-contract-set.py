#!/usr/bin/env python3
"""Validate the canonical M1 contract set and deterministic golden fixtures."""

from __future__ import annotations

import argparse
import hashlib
import json
import subprocess
import sys
from pathlib import Path
from typing import Any, Iterable

EXPECTED_CONTRACTS = [
    (1, "tracedecay.memory.provider.registry.v1", "tdmem-0201"),
    (2, "tracedecay.memory.provider.handshake.v1", "tdmem-0202"),
    (3, "tracedecay.memory.provider.observation.v1", "tdmem-0203"),
    (4, "tracedecay.memory.provider.recall.v1", "tdmem-0204"),
    (5, "tracedecay.memory.provider.lifecycle.v1", "tdmem-0205"),
    (6, "tracedecay.memory.provider.terminal.v1", "tdmem-0206"),
]

EXPECTED_RULES = {
    "contract-major-exact",
    "required-fields-closed",
    "unknown-enum-closed",
    "unknown-optional-extension-roundtrip",
    "unknown-required-extension-reject",
    "same-major-addition-only-at-extension-points",
    "terminal-envelope-mandatory",
    "canonical-roundtrip",
}

EXPECTED_CATEGORIES = {
    "success",
    "zero_results",
    "degradation",
    "cancellation",
    "timeout",
    "incompatibility",
    "stale_scope",
    "malformed",
    "unknown_optional_roundtrip",
    "unknown_required_rejection",
    "idempotent_duplicate",
    "idempotency_conflict",
    "partial_effect",
    "effect_unknown",
}

REQUIRED_FIXTURES = {
    "handshake.ready.success",
    "handshake.protocol.incompatible",
    "handshake.cancelled.pre-dispatch",
    "handshake.timeout.pre-dispatch",
    "observation.apply.success",
    "observation.duplicate.idempotent",
    "observation.idempotency.conflict",
    "observation.unknown-optional-extension.roundtrip",
    "observation.unknown-required-extension.reject",
    "recall.success.current",
    "recall.zero-results",
    "recall.partial.degraded",
    "recall.stale-scope",
    "recall.provenance.unavailable",
    "recall.malformed.native-score",
    "lifecycle.feedback.unsupported",
    "lifecycle.forget.verified-absent",
    "lifecycle.maintenance.cancelled",
    "lifecycle.snapshot.incompatible",
    "terminal.partial-effect",
    "terminal.effect-unknown",
    "malformed.unknown-top-level-field",
    "registry.resolve.native.success",
    "registry.unknown-optional-capability.roundtrip",
    "registry.unknown-required-capability.reject",
}

CONTRACT_SET_FIELDS = {
    "schema_version",
    "contract_set_id",
    "bead_id",
    "title",
    "status",
    "canonical_encoding",
    "digest_algorithm",
    "contract_order_is_authoritative",
    "contracts",
    "compatibility_rules",
    "goldens",
    "verification",
}

SCENARIO_FIELDS = {
    "schema_version",
    "scenario_set_id",
    "bead_id",
    "canonical_encoding",
    "fixtures",
}

FIXTURE_FIELDS = {
    "fixture_id",
    "category",
    "contract_id",
    "operation",
    "input",
    "expected",
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--repo", type=Path, default=Path("."))
    parser.add_argument(
        "--contract-set",
        type=Path,
        default=Path("product/contracts/memory-provider-v1/contract-set.json"),
    )
    parser.add_argument(
        "--contract-set-schema",
        type=Path,
        default=Path("product/contracts/memory-provider-v1/contract-set.schema.json"),
    )
    parser.add_argument(
        "--scenarios",
        type=Path,
        default=Path("product/contracts/memory-provider-v1/golden-scenarios.json"),
    )
    parser.add_argument(
        "--scenario-schema",
        type=Path,
        default=Path("product/contracts/memory-provider-v1/golden-scenarios.schema.json"),
    )
    parser.add_argument(
        "--goldens-dir",
        type=Path,
        default=Path("product/contracts/memory-provider-v1/goldens"),
    )
    return parser.parse_args()


def resolve(repo: Path, path: Path) -> Path:
    return path if path.is_absolute() else repo / path


def load_object(path: Path, label: str, errors: list[str]) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except OSError as exc:
        errors.append(f"could not read {label}: {exc}")
        return {}
    except json.JSONDecodeError as exc:
        errors.append(f"could not parse {label}: {exc}")
        return {}
    if not isinstance(value, dict):
        errors.append(f"{label} root must be an object")
        return {}
    return value


def canonical_bytes(value: Any) -> bytes:
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")


def canonical_sha(value: Any) -> str:
    return hashlib.sha256(canonical_bytes(value)).hexdigest()


def sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def exact_keys(
    value: dict[str, Any], expected: set[str], label: str, errors: list[str]
) -> None:
    actual = set(value)
    if actual != expected:
        errors.append(
            f"{label} fields drifted; missing={sorted(expected - actual)}, "
            f"extra={sorted(actual - expected)}"
        )


def require_file(repo: Path, raw: Any, label: str, errors: list[str]) -> Path | None:
    if not isinstance(raw, str) or not raw:
        errors.append(f"{label} must be a non-empty path")
        return None
    path = Path(raw)
    if path.is_absolute() or ".." in path.parts:
        errors.append(f"{label} must be repository-relative: {raw}")
        return None
    full = repo / path
    if not full.is_file():
        errors.append(f"{label} does not exist: {raw}")
        return None
    return full


def unique_by(
    rows: Iterable[Any], field: str, label: str, errors: list[str]
) -> dict[str, dict[str, Any]]:
    result: dict[str, dict[str, Any]] = {}
    for index, row in enumerate(rows):
        if not isinstance(row, dict):
            errors.append(f"{label}[{index}] must be an object")
            continue
        identity = row.get(field)
        if not isinstance(identity, str) or not identity:
            errors.append(f"{label}[{index}].{field} must be non-empty")
            continue
        if identity in result:
            errors.append(f"duplicate {label} {field} {identity}")
            continue
        result[identity] = row
    return result


def validate_contract_set(
    repo: Path,
    contract_set: dict[str, Any],
    schema: dict[str, Any],
    errors: list[str],
) -> dict[str, dict[str, Any]]:
    exact_keys(contract_set, CONTRACT_SET_FIELDS, "contract-set", errors)
    if contract_set.get("schema_version") != 1:
        errors.append("contract-set schema_version must be 1")
    if contract_set.get("contract_set_id") != (
        "tracedecay.memory.provider.contract-set.v1"
    ):
        errors.append("contract-set ID drifted")
    if contract_set.get("bead_id") != "tdmem-0207":
        errors.append("contract-set bead_id must be tdmem-0207")
    if contract_set.get("status") != "accepted":
        errors.append("contract-set status must be accepted")
    if contract_set.get("canonical_encoding") != (
        "utf8_rfc8785_json_without_bom_with_lf"
    ):
        errors.append("contract-set canonical encoding drifted")
    if contract_set.get("digest_algorithm") != "sha256":
        errors.append("contract-set digest algorithm must be sha256")
    if contract_set.get("contract_order_is_authoritative") is not True:
        errors.append("contract-set order must be authoritative")

    rows = contract_set.get("contracts")
    if not isinstance(rows, list) or len(rows) != 6:
        errors.append("contract-set must contain exactly six contracts")
        rows = []
    indexed = unique_by(rows, "contract_id", "contracts", errors)
    expected_ids = {row[1] for row in EXPECTED_CONTRACTS}
    if set(indexed) != expected_ids:
        errors.append("contract-set IDs do not match the six accepted M1 contracts")
    actual_order = [
        (row.get("order"), row.get("contract_id"), row.get("bead_id"))
        for row in rows
        if isinstance(row, dict)
    ]
    if actual_order != EXPECTED_CONTRACTS:
        errors.append("contract-set order, IDs, or Beads drifted")

    contract_documents: dict[str, dict[str, Any]] = {}
    for contract_id, row in indexed.items():
        expected_fields = {
            "order",
            "contract_id",
            "bead_id",
            "contract_path",
            "schema_path",
            "documentation_path",
            "checker_path",
            "test_path",
            "required_status",
        }
        exact_keys(row, expected_fields, f"contract[{contract_id}]", errors)
        loaded_paths: dict[str, Path | None] = {}
        for field in (
            "contract_path",
            "schema_path",
            "documentation_path",
            "checker_path",
            "test_path",
        ):
            loaded_paths[field] = require_file(
                repo, row.get(field), f"contract[{contract_id}].{field}", errors
            )
        contract_path = loaded_paths["contract_path"]
        schema_path = loaded_paths["schema_path"]
        if contract_path is None or schema_path is None:
            continue
        contract = load_object(contract_path, contract_id, errors)
        contract_schema = load_object(schema_path, f"{contract_id} schema", errors)
        contract_documents[contract_id] = contract
        if contract.get("contract_id") != contract_id:
            errors.append(f"contract {contract_id} identity mismatch")
        if contract.get("bead_id") != row.get("bead_id"):
            errors.append(f"contract {contract_id} bead mismatch")
        if contract.get("status") != row.get("required_status"):
            errors.append(f"contract {contract_id} is not accepted")
        if contract_schema.get("additionalProperties") is not False:
            errors.append(f"contract schema {contract_id} root must be strict")
        properties = contract_schema.get("properties")
        if not isinstance(properties, dict):
            errors.append(f"contract schema {contract_id} has no properties")
        else:
            if properties.get("contract_id", {}).get("const") != contract_id:
                errors.append(f"contract schema {contract_id} does not pin ID")
            if properties.get("bead_id", {}).get("const") != row.get("bead_id"):
                errors.append(f"contract schema {contract_id} does not pin bead")

    rules = contract_set.get("compatibility_rules")
    if not isinstance(rules, list):
        errors.append("compatibility_rules must be an array")
        rules = []
    rule_map = unique_by(rules, "id", "compatibility_rules", errors)
    if set(rule_map) != EXPECTED_RULES:
        errors.append("compatibility rules must exactly contain the eight V1 rules")
    for rule_id, row in rule_map.items():
        exact_keys(row, {"id", "rule"}, f"compatibility_rule[{rule_id}]", errors)
        if not isinstance(row.get("rule"), str) or len(row["rule"]) < 20:
            errors.append(f"compatibility rule {rule_id} is not substantive")

    if schema.get("$schema") != "https://json-schema.org/draft/2020-12/schema":
        errors.append("contract-set schema must use JSON Schema 2020-12")
    if schema.get("type") != "object" or schema.get("additionalProperties") is not False:
        errors.append("contract-set schema root must be strict")
    if set(schema.get("required", [])) != CONTRACT_SET_FIELDS:
        errors.append("contract-set schema required fields drifted")
    schema_properties = schema.get("properties")
    if not isinstance(schema_properties, dict):
        errors.append("contract-set schema has no properties")
    else:
        if schema_properties.get("contract_set_id", {}).get("const") != (
            "tracedecay.memory.provider.contract-set.v1"
        ):
            errors.append("contract-set schema does not pin ID")
        if schema_properties.get("contracts", {}).get("minItems") != 6:
            errors.append("contract-set schema must require six contracts")
    return contract_documents


def validate_scenarios(
    scenarios: dict[str, Any], schema: dict[str, Any], contracts: set[str], errors: list[str]
) -> dict[str, dict[str, Any]]:
    exact_keys(scenarios, SCENARIO_FIELDS, "scenarios", errors)
    if scenarios.get("schema_version") != 1:
        errors.append("scenario schema_version must be 1")
    if scenarios.get("scenario_set_id") != (
        "tracedecay.memory.provider.golden-scenarios.v1"
    ):
        errors.append("scenario-set ID drifted")
    if scenarios.get("bead_id") != "tdmem-0207":
        errors.append("scenario bead_id must be tdmem-0207")
    if scenarios.get("canonical_encoding") != (
        "utf8_rfc8785_json_without_bom_with_lf"
    ):
        errors.append("scenario canonical encoding drifted")
    rows = scenarios.get("fixtures")
    if not isinstance(rows, list) or len(rows) < 24:
        errors.append("scenario-set must contain at least twenty-four fixtures")
        rows = []
    indexed = unique_by(rows, "fixture_id", "fixtures", errors)
    if not REQUIRED_FIXTURES.issubset(set(indexed)):
        errors.append(
            f"scenario-set missing required fixtures: {sorted(REQUIRED_FIXTURES - set(indexed))}"
        )
    categories: set[str] = set()
    for fixture_id, row in indexed.items():
        exact_keys(row, FIXTURE_FIELDS, f"fixture[{fixture_id}]", errors)
        category = row.get("category")
        if category not in EXPECTED_CATEGORIES:
            errors.append(f"fixture {fixture_id} has unknown category {category!r}")
        else:
            categories.add(category)
        if row.get("contract_id") not in contracts:
            errors.append(f"fixture {fixture_id} references unknown contract")
        if not isinstance(row.get("operation"), str) or not row["operation"]:
            errors.append(f"fixture {fixture_id} has no operation")
        if not isinstance(row.get("input"), dict) or not isinstance(
            row.get("expected"), dict
        ):
            errors.append(f"fixture {fixture_id} input/expected must be objects")
        try:
            canonical_bytes(row)
        except (TypeError, ValueError) as exc:
            errors.append(f"fixture {fixture_id} is not canonical JSON: {exc}")
    if categories != EXPECTED_CATEGORIES:
        errors.append(
            f"scenario categories drifted; missing={sorted(EXPECTED_CATEGORIES - categories)}"
        )

    if schema.get("$schema") != "https://json-schema.org/draft/2020-12/schema":
        errors.append("scenario schema must use JSON Schema 2020-12")
    if schema.get("type") != "object" or schema.get("additionalProperties") is not False:
        errors.append("scenario schema root must be strict")
    if set(schema.get("required", [])) != SCENARIO_FIELDS:
        errors.append("scenario schema required fields drifted")
    properties = schema.get("properties")
    if not isinstance(properties, dict):
        errors.append("scenario schema has no properties")
    elif properties.get("fixtures", {}).get("minItems") != 24:
        errors.append("scenario schema must require at least twenty-four fixtures")
    return indexed


def validate_generated(
    repo: Path,
    contract_set_path: Path,
    scenarios_path: Path,
    goldens_dir: Path,
    scenario_map: dict[str, dict[str, Any]],
    errors: list[str],
) -> None:
    command = [
        "python3",
        "scripts/product/generate-memory-provider-goldens.py",
        "--repo",
        ".",
        "--contract-set",
        str(contract_set_path),
        "--scenarios",
        str(scenarios_path),
        "--output-dir",
        str(goldens_dir),
        "--check",
    ]
    result = subprocess.run(
        command,
        cwd=repo,
        check=False,
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        errors.append(
            "golden generator check failed: "
            + (result.stdout.strip() or result.stderr.strip())
        )
        return

    fixtures_path = goldens_dir / "fixtures.jsonl"
    manifest_path = goldens_dir / "manifest.json"
    if not fixtures_path.is_file() or not manifest_path.is_file():
        errors.append("generated golden files are missing")
        return
    raw_fixture_bytes = fixtures_path.read_bytes()
    if raw_fixture_bytes.startswith(b"\xef\xbb\xbf"):
        errors.append("golden fixtures must not contain a UTF-8 BOM")
    if raw_fixture_bytes and not raw_fixture_bytes.endswith(b"\n"):
        errors.append("golden fixture file must end with LF")
    lines = raw_fixture_bytes.splitlines()
    fixture_rows: list[dict[str, Any]] = []
    for number, line in enumerate(lines, start=1):
        try:
            value = json.loads(line)
        except json.JSONDecodeError as exc:
            errors.append(f"generated fixture line {number} is invalid JSON: {exc}")
            continue
        if canonical_bytes(value) != line:
            errors.append(f"generated fixture line {number} is not canonical JSON")
        if not isinstance(value, dict):
            errors.append(f"generated fixture line {number} must be an object")
            continue
        fixture_rows.append(value)
    generated_ids = [row.get("fixture_id") for row in fixture_rows]
    if generated_ids != sorted(generated_ids, key=lambda value: value.encode("utf-8")):
        errors.append("generated fixtures are not ordered by UTF-8 fixture ID")
    if set(generated_ids) != set(scenario_map):
        errors.append("generated fixture IDs do not match scenario authority")

    for row in fixture_rows:
        fixture_id = row.get("fixture_id")
        if fixture_id not in scenario_map:
            continue
        source = scenario_map[fixture_id]
        if row.get("input") != source.get("input") or row.get("expected") != source.get(
            "expected"
        ):
            errors.append(f"generated fixture {fixture_id} changed source semantics")
        category = row.get("category")
        if category == "unknown_optional_roundtrip":
            digest = row.get("opaque_roundtrip_payload_sha256")
            if not isinstance(digest, str) or len(digest) != 64:
                errors.append(
                    f"unknown optional fixture {fixture_id} lacks round-trip payload digest"
                )
        elif row.get("opaque_roundtrip_payload_sha256") is not None:
            errors.append(
                f"non-roundtrip fixture {fixture_id} has opaque round-trip digest"
            )

    manifest = load_object(manifest_path, "generated golden manifest", errors)
    if manifest.get("schema_version") != 1:
        errors.append("generated manifest schema_version must be 1")
    if manifest.get("manifest_id") != (
        "tracedecay.memory.provider.goldens.manifest.v1"
    ):
        errors.append("generated manifest ID drifted")
    if manifest.get("fixture_count") != len(lines):
        errors.append("generated manifest fixture count drifted")
    if manifest.get("fixtures_sha256") != sha256_bytes(raw_fixture_bytes):
        errors.append("generated manifest fixture digest drifted")
    manifest_rows = manifest.get("fixtures")
    if not isinstance(manifest_rows, list) or len(manifest_rows) != len(lines):
        errors.append("generated manifest fixture index drifted")
    else:
        for number, (manifest_row, line) in enumerate(
            zip(manifest_rows, lines, strict=True), start=1
        ):
            if not isinstance(manifest_row, dict):
                errors.append(f"generated manifest row {number} must be object")
                continue
            if manifest_row.get("line_number") != number:
                errors.append(f"generated manifest line number {number} drifted")
            if manifest_row.get("line_sha256") != sha256_bytes(line):
                errors.append(f"generated manifest line digest {number} drifted")


def validate(
    repo: Path,
    contract_set_path: Path,
    contract_set_schema_path: Path,
    scenarios_path: Path,
    scenario_schema_path: Path,
    goldens_dir: Path,
) -> list[str]:
    errors: list[str] = []
    contract_set = load_object(contract_set_path, "contract set", errors)
    contract_set_schema = load_object(
        contract_set_schema_path, "contract-set schema", errors
    )
    scenarios = load_object(scenarios_path, "golden scenarios", errors)
    scenario_schema = load_object(scenario_schema_path, "scenario schema", errors)
    contracts = validate_contract_set(
        repo, contract_set, contract_set_schema, errors
    )
    scenario_map = validate_scenarios(
        scenarios, scenario_schema, set(contracts), errors
    )
    validate_generated(
        repo,
        contract_set_path,
        scenarios_path,
        goldens_dir,
        scenario_map,
        errors,
    )
    return errors


def main() -> int:
    args = parse_args()
    repo = args.repo.resolve()
    contract_set_path = resolve(repo, args.contract_set)
    contract_set_schema_path = resolve(repo, args.contract_set_schema)
    scenarios_path = resolve(repo, args.scenarios)
    scenario_schema_path = resolve(repo, args.scenario_schema)
    goldens_dir = resolve(repo, args.goldens_dir)
    errors = validate(
        repo,
        contract_set_path,
        contract_set_schema_path,
        scenarios_path,
        scenario_schema_path,
        goldens_dir,
    )
    if errors:
        print(json.dumps({"ok": False, "errors": errors}, indent=2, sort_keys=True))
        return 1
    manifest = load_object(goldens_dir / "manifest.json", "golden manifest", [])
    print(
        json.dumps(
            {
                "ok": True,
                "contract_set_id": "tracedecay.memory.provider.contract-set.v1",
                "bead_id": "tdmem-0207",
                "contract_count": 6,
                "fixture_count": manifest.get("fixture_count"),
                "category_count": len(EXPECTED_CATEGORIES),
                "compatibility_rule_count": len(EXPECTED_RULES),
                "fixtures_sha256": manifest.get("fixtures_sha256"),
                "generator_sha256": manifest.get("generator_sha256"),
            },
            indent=2,
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
