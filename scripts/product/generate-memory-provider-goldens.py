#!/usr/bin/env python3
"""Generate or verify deterministic Memory Provider V1 golden fixtures."""

from __future__ import annotations

import argparse
import difflib
import hashlib
import json
import sys
from pathlib import Path
from typing import Any

DEFAULT_CONTRACT_SET = Path(
    "product/contracts/memory-provider-v1/contract-set.json"
)
DEFAULT_SCENARIOS = Path(
    "product/contracts/memory-provider-v1/golden-scenarios.json"
)
DEFAULT_OUTPUT_DIR = Path(
    "product/contracts/memory-provider-v1/goldens"
)

CATEGORY_RULES = {
    "success": ["canonical-roundtrip", "terminal-envelope-mandatory"],
    "zero_results": ["canonical-roundtrip", "terminal-envelope-mandatory"],
    "degradation": ["terminal-envelope-mandatory"],
    "cancellation": ["terminal-envelope-mandatory"],
    "timeout": ["terminal-envelope-mandatory"],
    "incompatibility": ["contract-major-exact", "terminal-envelope-mandatory"],
    "stale_scope": ["terminal-envelope-mandatory"],
    "malformed": ["required-fields-closed", "unknown-enum-closed"],
    "unknown_optional_roundtrip": [
        "unknown-optional-extension-roundtrip",
        "same-major-addition-only-at-extension-points",
        "canonical-roundtrip",
    ],
    "unknown_required_rejection": [
        "unknown-required-extension-reject",
        "same-major-addition-only-at-extension-points",
    ],
    "idempotent_duplicate": ["canonical-roundtrip", "terminal-envelope-mandatory"],
    "idempotency_conflict": ["terminal-envelope-mandatory"],
    "partial_effect": ["terminal-envelope-mandatory"],
    "effect_unknown": ["terminal-envelope-mandatory"],
}


class GenerationError(RuntimeError):
    """Raised when source authorities are invalid or output drifts."""


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--repo", type=Path, default=Path("."))
    parser.add_argument("--contract-set", type=Path, default=DEFAULT_CONTRACT_SET)
    parser.add_argument("--scenarios", type=Path, default=DEFAULT_SCENARIOS)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--write", action="store_true")
    mode.add_argument("--check", action="store_true")
    return parser.parse_args()


def resolve(repo: Path, path: Path) -> Path:
    return path if path.is_absolute() else repo / path


def load_json(path: Path, label: str) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except OSError as exc:
        raise GenerationError(f"could not read {label} {path}: {exc}") from exc
    except json.JSONDecodeError as exc:
        raise GenerationError(f"could not parse {label} {path}: {exc}") from exc
    if not isinstance(value, dict):
        raise GenerationError(f"{label} root must be an object")
    return value


def canonical_bytes(value: Any) -> bytes:
    try:
        encoded = json.dumps(
            value,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        )
    except (TypeError, ValueError) as exc:
        raise GenerationError(f"value is not canonical JSON: {exc}") from exc
    return encoded.encode("utf-8")


def sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def canonical_sha(value: Any) -> str:
    return sha256_bytes(canonical_bytes(value))


def require_relative_file(repo: Path, raw: Any, label: str) -> Path:
    if not isinstance(raw, str) or not raw:
        raise GenerationError(f"{label} must be a non-empty repository-relative path")
    path = Path(raw)
    if path.is_absolute() or ".." in path.parts:
        raise GenerationError(f"{label} must be repository-relative: {raw}")
    full = repo / path
    if not full.is_file():
        raise GenerationError(f"{label} does not exist: {raw}")
    return full


def validate_contract_set(
    repo: Path, contract_set: dict[str, Any]
) -> tuple[dict[str, dict[str, Any]], list[dict[str, Any]]]:
    if contract_set.get("schema_version") != 1:
        raise GenerationError("contract-set schema_version must be 1")
    if contract_set.get("contract_set_id") != (
        "tracedecay.memory.provider.contract-set.v1"
    ):
        raise GenerationError("contract-set ID drifted")
    if contract_set.get("bead_id") != "tdmem-0207":
        raise GenerationError("contract-set bead_id must be tdmem-0207")
    if contract_set.get("status") != "accepted":
        raise GenerationError("contract-set status must be accepted")
    if contract_set.get("canonical_encoding") != (
        "utf8_rfc8785_json_without_bom_with_lf"
    ):
        raise GenerationError("contract-set canonical encoding drifted")
    if contract_set.get("digest_algorithm") != "sha256":
        raise GenerationError("contract-set digest algorithm must be sha256")
    if contract_set.get("contract_order_is_authoritative") is not True:
        raise GenerationError("contract order must be authoritative")

    entries = contract_set.get("contracts")
    if not isinstance(entries, list) or len(entries) != 6:
        raise GenerationError("contract-set must contain exactly six contracts")
    orders = [entry.get("order") for entry in entries if isinstance(entry, dict)]
    if orders != [1, 2, 3, 4, 5, 6]:
        raise GenerationError("contract-set order must be contiguous 1..6")

    contracts: dict[str, dict[str, Any]] = {}
    digests: list[dict[str, Any]] = []
    for entry in entries:
        if not isinstance(entry, dict):
            raise GenerationError("contract-set entry must be an object")
        contract_id = entry.get("contract_id")
        if not isinstance(contract_id, str) or not contract_id:
            raise GenerationError("contract-set entry has no contract_id")
        if contract_id in contracts:
            raise GenerationError(f"duplicate contract ID {contract_id}")
        contract_path = require_relative_file(
            repo, entry.get("contract_path"), f"{contract_id}.contract_path"
        )
        schema_path = require_relative_file(
            repo, entry.get("schema_path"), f"{contract_id}.schema_path"
        )
        for field in ("documentation_path", "checker_path", "test_path"):
            require_relative_file(repo, entry.get(field), f"{contract_id}.{field}")
        contract = load_json(contract_path, f"contract {contract_id}")
        schema = load_json(schema_path, f"schema {contract_id}")
        if contract.get("contract_id") != contract_id:
            raise GenerationError(f"{contract_id} contract identity mismatch")
        if contract.get("bead_id") != entry.get("bead_id"):
            raise GenerationError(f"{contract_id} bead identity mismatch")
        if contract.get("status") != entry.get("required_status"):
            raise GenerationError(f"{contract_id} status is not accepted")
        schema_properties = schema.get("properties")
        if not isinstance(schema_properties, dict):
            raise GenerationError(f"{contract_id} schema has no properties")
        if schema_properties.get("contract_id", {}).get("const") != contract_id:
            raise GenerationError(f"{contract_id} schema does not pin contract ID")
        if schema_properties.get("bead_id", {}).get("const") != entry.get("bead_id"):
            raise GenerationError(f"{contract_id} schema does not pin bead ID")
        if schema.get("additionalProperties") is not False:
            raise GenerationError(f"{contract_id} schema root is not strict")
        contracts[contract_id] = contract
        digests.append(
            {
                "order": entry["order"],
                "contract_id": contract_id,
                "bead_id": entry["bead_id"],
                "contract_path": entry["contract_path"],
                "schema_path": entry["schema_path"],
                "contract_sha256": canonical_sha(contract),
                "schema_sha256": canonical_sha(schema),
            }
        )

    rules = contract_set.get("compatibility_rules")
    if not isinstance(rules, list) or len(rules) != 8:
        raise GenerationError("contract-set must contain eight compatibility rules")
    rule_ids = [rule.get("id") for rule in rules if isinstance(rule, dict)]
    if len(rule_ids) != len(set(rule_ids)):
        raise GenerationError("compatibility rule IDs must be unique")
    required_rules = {
        rule for category_rules in CATEGORY_RULES.values() for rule in category_rules
    }
    if not required_rules.issubset(set(rule_ids)):
        missing = sorted(required_rules - set(rule_ids))
        raise GenerationError(f"contract-set is missing compatibility rules {missing}")
    return contracts, digests


def validate_scenarios(
    scenarios: dict[str, Any],
    contracts: dict[str, dict[str, Any]],
    minimum_fixture_count: int,
    required_categories: list[str],
) -> list[dict[str, Any]]:
    if scenarios.get("schema_version") != 1:
        raise GenerationError("scenario-set schema_version must be 1")
    if scenarios.get("scenario_set_id") != (
        "tracedecay.memory.provider.golden-scenarios.v1"
    ):
        raise GenerationError("scenario-set ID drifted")
    if scenarios.get("bead_id") != "tdmem-0207":
        raise GenerationError("scenario-set bead_id must be tdmem-0207")
    if scenarios.get("canonical_encoding") != (
        "utf8_rfc8785_json_without_bom_with_lf"
    ):
        raise GenerationError("scenario canonical encoding drifted")
    fixtures = scenarios.get("fixtures")
    if not isinstance(fixtures, list) or len(fixtures) < minimum_fixture_count:
        raise GenerationError(
            f"scenario-set must contain at least {minimum_fixture_count} fixtures"
        )
    result: list[dict[str, Any]] = []
    fixture_ids: set[str] = set()
    categories: set[str] = set()
    for index, raw in enumerate(fixtures):
        if not isinstance(raw, dict):
            raise GenerationError(f"fixture {index} must be an object")
        expected_fields = {
            "fixture_id",
            "category",
            "contract_id",
            "operation",
            "input",
            "expected",
        }
        if set(raw) != expected_fields:
            raise GenerationError(
                f"fixture {index} fields drifted; "
                f"missing={sorted(expected_fields - set(raw))}, "
                f"extra={sorted(set(raw) - expected_fields)}"
            )
        fixture_id = raw.get("fixture_id")
        if not isinstance(fixture_id, str) or not fixture_id:
            raise GenerationError(f"fixture {index} has no fixture_id")
        if fixture_id in fixture_ids:
            raise GenerationError(f"duplicate fixture ID {fixture_id}")
        fixture_ids.add(fixture_id)
        category = raw.get("category")
        if category not in CATEGORY_RULES:
            raise GenerationError(
                f"fixture {fixture_id} has unknown category {category!r}"
            )
        categories.add(category)
        contract_id = raw.get("contract_id")
        if contract_id not in contracts:
            raise GenerationError(
                f"fixture {fixture_id} references unknown contract {contract_id!r}"
            )
        operation = raw.get("operation")
        if not isinstance(operation, str) or not operation:
            raise GenerationError(f"fixture {fixture_id} has no operation")
        if not isinstance(raw.get("input"), dict) or not isinstance(
            raw.get("expected"), dict
        ):
            raise GenerationError(
                f"fixture {fixture_id} input and expected must be objects"
            )
        canonical_bytes(raw)
        result.append(raw)

    missing_categories = set(required_categories) - categories
    if missing_categories:
        raise GenerationError(
            f"scenario-set is missing required categories {sorted(missing_categories)}"
        )
    return sorted(result, key=lambda fixture: fixture["fixture_id"].encode("utf-8"))


def opaque_roundtrip_payload(fixture: dict[str, Any]) -> Any | None:
    if fixture["category"] != "unknown_optional_roundtrip":
        return None
    value = fixture["input"]
    for key in ("extension", "unknown_capability", "unknown_detail"):
        if key in value:
            return value[key]
    raise GenerationError(
        f"unknown optional fixture {fixture['fixture_id']} has no opaque payload"
    )


def render_outputs(
    repo: Path,
    contract_set_path: Path,
    scenario_path: Path,
    output_dir: Path,
) -> tuple[bytes, bytes]:
    contract_set = load_json(contract_set_path, "contract set")
    contracts, contract_digests = validate_contract_set(repo, contract_set)
    goldens = contract_set.get("goldens")
    if not isinstance(goldens, dict):
        raise GenerationError("contract-set goldens must be an object")
    minimum_count = goldens.get("minimum_fixture_count")
    if not isinstance(minimum_count, int) or minimum_count < 1:
        raise GenerationError("minimum fixture count must be positive")
    required_categories = goldens.get("required_categories")
    if not isinstance(required_categories, list) or any(
        not isinstance(value, str) for value in required_categories
    ):
        raise GenerationError("required categories must be string array")
    if set(required_categories) != set(CATEGORY_RULES):
        raise GenerationError("required category authority does not match generator")
    if goldens.get("fixture_order") != "fixture_id_lexicographic_utf8":
        raise GenerationError("fixture order authority drifted")
    if goldens.get("line_encoding") != (
        "one_rfc8785_json_object_per_lf_terminated_line"
    ):
        raise GenerationError("fixture line encoding authority drifted")
    if goldens.get("generated_files_are_hand_editable") is not False:
        raise GenerationError("generated golden files must not be hand-editable")

    scenarios = load_json(scenario_path, "golden scenarios")
    fixtures = validate_scenarios(
        scenarios, contracts, minimum_count, required_categories
    )
    digest_by_contract = {
        row["contract_id"]: row for row in contract_digests
    }
    lines: list[bytes] = []
    manifest_rows: list[dict[str, Any]] = []
    for line_number, fixture in enumerate(fixtures, start=1):
        contract_digest = digest_by_contract[fixture["contract_id"]]
        roundtrip = opaque_roundtrip_payload(fixture)
        output = {
            "fixture_schema_version": 1,
            "fixture_id": fixture["fixture_id"],
            "category": fixture["category"],
            "contract_id": fixture["contract_id"],
            "contract_sha256": contract_digest["contract_sha256"],
            "schema_sha256": contract_digest["schema_sha256"],
            "operation": fixture["operation"],
            "compatibility_rule_ids": CATEGORY_RULES[fixture["category"]],
            "input": fixture["input"],
            "expected": fixture["expected"],
            "opaque_roundtrip_payload_sha256": (
                canonical_sha(roundtrip) if roundtrip is not None else None
            ),
        }
        line = canonical_bytes(output)
        lines.append(line + b"\n")
        manifest_rows.append(
            {
                "fixture_id": fixture["fixture_id"],
                "category": fixture["category"],
                "contract_id": fixture["contract_id"],
                "line_number": line_number,
                "line_sha256": sha256_bytes(line),
            }
        )
    fixture_bytes = b"".join(lines)
    generator_bytes = Path(__file__).read_bytes()
    manifest = {
        "schema_version": 1,
        "manifest_id": "tracedecay.memory.provider.goldens.manifest.v1",
        "contract_set_id": contract_set["contract_set_id"],
        "scenario_set_id": scenarios["scenario_set_id"],
        "canonical_encoding": contract_set["canonical_encoding"],
        "digest_algorithm": "sha256",
        "generator_path": str(Path(__file__).resolve().relative_to(repo)),
        "generator_sha256": sha256_bytes(generator_bytes),
        "contract_set_path": str(contract_set_path.relative_to(repo)),
        "contract_set_sha256": canonical_sha(contract_set),
        "scenario_source_path": str(scenario_path.relative_to(repo)),
        "scenario_source_sha256": canonical_sha(scenarios),
        "fixtures_path": str(
            (output_dir / "fixtures.jsonl").relative_to(repo)
        ),
        "fixtures_sha256": sha256_bytes(fixture_bytes),
        "fixture_count": len(fixtures),
        "contract_digests": contract_digests,
        "fixtures": manifest_rows,
    }
    manifest_bytes = canonical_bytes(manifest) + b"\n"
    return fixture_bytes, manifest_bytes


def diff_bytes(expected: bytes, actual: bytes, label: str) -> str:
    expected_lines = expected.decode("utf-8").splitlines(keepends=True)
    actual_lines = actual.decode("utf-8").splitlines(keepends=True)
    return "".join(
        difflib.unified_diff(
            actual_lines,
            expected_lines,
            fromfile=f"checked-in/{label}",
            tofile=f"generated/{label}",
        )
    )


def write_if_changed(path: Path, content: bytes) -> bool:
    current = path.read_bytes() if path.exists() else None
    if current == content:
        return False
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(content)
    return True


def main() -> int:
    args = parse_args()
    repo = args.repo.resolve()
    contract_set_path = resolve(repo, args.contract_set)
    scenario_path = resolve(repo, args.scenarios)
    output_dir = resolve(repo, args.output_dir)
    try:
        fixture_bytes, manifest_bytes = render_outputs(
            repo, contract_set_path, scenario_path, output_dir
        )
        outputs = {
            output_dir / "fixtures.jsonl": fixture_bytes,
            output_dir / "manifest.json": manifest_bytes,
        }
        if args.write:
            changed = [
                str(path.relative_to(repo))
                for path, content in outputs.items()
                if write_if_changed(path, content)
            ]
            print(
                json.dumps(
                    {
                        "ok": True,
                        "mode": "write",
                        "changed": changed,
                        "fixture_count": fixture_bytes.count(b"\n"),
                    },
                    indent=2,
                    sort_keys=True,
                )
            )
            return 0

        drift: list[str] = []
        for path, expected in outputs.items():
            if not path.is_file():
                drift.append(f"missing generated file: {path.relative_to(repo)}")
                continue
            actual = path.read_bytes()
            if actual != expected:
                drift.append(
                    diff_bytes(expected, actual, str(path.relative_to(repo)))
                )
        if drift:
            print(
                json.dumps(
                    {"ok": False, "mode": "check", "drift": drift},
                    indent=2,
                    sort_keys=True,
                )
            )
            return 1
        print(
            json.dumps(
                {
                    "ok": True,
                    "mode": "check",
                    "fixture_count": fixture_bytes.count(b"\n"),
                    "fixtures_sha256": sha256_bytes(fixture_bytes),
                    "manifest_sha256": sha256_bytes(manifest_bytes),
                },
                indent=2,
                sort_keys=True,
            )
        )
        return 0
    except GenerationError as exc:
        print(
            json.dumps(
                {"ok": False, "mode": "write" if args.write else "check", "error": str(exc)},
                indent=2,
                sort_keys=True,
            )
        )
        return 1


if __name__ == "__main__":
    sys.exit(main())
