#!/usr/bin/env python3
"""Validate source-linked external lesson intake records."""

from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit


DEFAULT_INTAKE = Path("product/upstream/external-lesson-intake.json")
DEFAULT_SCHEMA = Path("product/upstream/external-lesson-intake.schema.json")
DEFAULT_ISSUES = Path(".beads/issues.jsonl")

CONTRACT_ID = "tracedecay.external-lesson-intake.v1"
SCHEMA_VERSION = 1
CONTRACT_BEAD = "tdmem-1207"
STATUSES = ["accepted", "rejected"]
TARGET_KINDS = ["capability", "policy"]
CODE_MODES = ["clean_reimplementation", "copied_external_code", "none_rejected"]

BEAD_RE = re.compile(r"^tdmem-[0-9]{4}$")
COMMIT_RE = re.compile(r"^[0-9a-f]{40}$")
LESSON_ID_RE = re.compile(r"^[a-z0-9]+(?:[.-][a-z0-9]+)*$")
IDENTIFIER_RE = re.compile(r"^[a-z0-9][a-z0-9._-]{1,63}$")
TARGET_ID_RE = re.compile(r"^[a-z][a-z0-9]*(?:[._-][a-z0-9]+)+$")

EXPECTED_ROOT_FIELDS = {
    "schema_version",
    "contract_id",
    "bead_id",
    "title",
    "policy",
    "lessons",
}
EXPECTED_POLICY_FIELDS = {
    "statuses",
    "target_kinds",
    "accepted_requires_neutral_regression_tests",
    "source_specific_assumptions_boundary",
    "copied_external_code_policy",
}
EXPECTED_LESSON_FIELDS = {
    "lesson_id",
    "status",
    "source",
    "extracted_generic_invariant",
    "target",
    "source_assumptions",
    "neutral_regression_tests",
    "implementation_bead",
    "code_use",
    "decision",
}
EXPECTED_SOURCE_FIELDS = {
    "repository",
    "commit",
    "identifiers",
    "license",
    "evidence",
}
EXPECTED_LICENSE_FIELDS = {"identity", "provenance_statement", "evidence_path"}
EXPECTED_EVIDENCE_FIELDS = {
    "source_path",
    "source_url",
    "local_evidence_path",
    "claim",
}
EXPECTED_TARGET_FIELDS = {"kind", "id", "contract_path", "mapping"}
EXPECTED_ASSUMPTION_FIELDS = {"assumption", "adapter_boundary", "adapter_path"}
EXPECTED_TEST_FIELDS = {"path", "proves"}
EXPECTED_CODE_USE_FIELDS = {
    "mode",
    "external_code_copied",
    "copy_records",
    "note",
}
EXPECTED_COPY_FIELDS = {"source_path", "destination_path", "license_notice_path"}
EXPECTED_DECISION_FIELDS = {"rationale", "rejection_rationale"}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--repo", type=Path, default=Path("."))
    parser.add_argument("--intake", type=Path, default=DEFAULT_INTAKE)
    parser.add_argument("--schema", type=Path, default=DEFAULT_SCHEMA)
    parser.add_argument("--issues", type=Path, default=DEFAULT_ISSUES)
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


def load_issue_ids(path: Path, errors: list[str]) -> set[str]:
    ids: set[str] = set()
    try:
        lines = path.read_text(encoding="utf-8").splitlines()
    except OSError as exc:
        errors.append(f"could not load Beads issues: {exc}")
        return ids
    for line_number, line in enumerate(lines, 1):
        if not line.strip():
            continue
        try:
            value = json.loads(line)
        except json.JSONDecodeError as exc:
            errors.append(f"Beads issues line {line_number} is invalid JSON: {exc}")
            continue
        if isinstance(value, dict) and isinstance(value.get("id"), str):
            ids.add(value["id"])
    return ids


def exact_fields(
    value: Any, expected: set[str], label: str, errors: list[str]
) -> dict[str, Any]:
    if not isinstance(value, dict):
        errors.append(f"{label} must be an object")
        return {}
    actual = set(value)
    missing = sorted(expected - actual)
    unexpected = sorted(actual - expected)
    if missing:
        errors.append(f"{label} is missing fields: {missing}")
    if unexpected:
        errors.append(f"{label} has unexpected fields: {unexpected}")
    return value


def array(value: Any, label: str, errors: list[str]) -> list[Any]:
    if not isinstance(value, list):
        errors.append(f"{label} must be an array")
        return []
    return value


def nonempty(value: Any, label: str, errors: list[str]) -> str:
    if not isinstance(value, str) or not value.strip():
        errors.append(f"{label} must be a non-empty string")
        return ""
    return value.strip()


def substantive(value: Any, label: str, errors: list[str]) -> str:
    text = nonempty(value, label, errors)
    if text and (len(text) < 20 or len(re.findall(r"[A-Za-z0-9]+", text)) < 4):
        errors.append(f"{label} must be a substantive explanation")
    return text


def validate_relative_path(raw: Any, label: str, errors: list[str]) -> str:
    if isinstance(raw, str) and raw != raw.strip():
        errors.append(f"{label} must not contain leading or trailing whitespace")
    value = nonempty(raw, label, errors)
    if not value:
        return ""
    path = Path(value)
    if (
        path.is_absolute()
        or "." in path.parts
        or ".." in path.parts
        or "\\" in value
        or value != path.as_posix()
        or re.match(r"^[A-Za-z]:", value)
        or any(character in value for character in "*?[]")
        or any(ord(character) < 32 for character in value)
    ):
        errors.append(
            f"{label} must be a normalized literal repository-relative POSIX path"
        )
        return ""
    return value


def require_repo_file(
    repo: Path,
    raw: Any,
    label: str,
    errors: list[str],
    *,
    prefix: str | None = None,
) -> Path | None:
    relative = validate_relative_path(raw, label, errors)
    if not relative:
        return None
    if prefix is not None and not relative.startswith(prefix):
        errors.append(f"{label} must be under {prefix}")
    candidate = repo / relative
    try:
        candidate.resolve().relative_to(repo.resolve())
    except ValueError:
        errors.append(f"{label} resolves outside the repository")
        return None
    if not candidate.is_file():
        errors.append(f"{label} does not name a real file: {relative}")
        return None
    return candidate


def has_identifier(text: str, identifiers: list[str]) -> str | None:
    folded = text.casefold()
    for identifier in identifiers:
        pattern = rf"(?<![a-z0-9]){re.escape(identifier.casefold())}(?![a-z0-9])"
        if re.search(pattern, folded):
            return identifier
    return None


def contains_token(text: str, token: str) -> bool:
    pattern = rf"(?<![A-Za-z0-9]){re.escape(token)}(?![A-Za-z0-9])"
    return re.search(pattern, text, flags=re.IGNORECASE) is not None


def is_external_adapter_path(path: str) -> bool:
    parts = Path(path).parts
    if len(parts) < 3 or parts[0] != "crates":
        return False
    crate = parts[1]
    if not crate.startswith("tracedecay-memory-provider-"):
        return False
    return crate not in {
        "tracedecay-memory-provider-api",
        "tracedecay-memory-provider-native",
        "tracedecay-memory-provider-registry",
    }


def validate_schema(schema: dict[str, Any], errors: list[str]) -> None:
    expected_root = {
        "$schema",
        "$id",
        "title",
        "type",
        "additionalProperties",
        "required",
        "properties",
        "$defs",
    }
    exact_fields(schema, expected_root, "intake schema", errors)
    if schema.get("$schema") != "https://json-schema.org/draft/2020-12/schema":
        errors.append("intake schema must use JSON Schema draft 2020-12")
    if schema.get("type") != "object":
        errors.append("intake schema root type must be object")
    if schema.get("additionalProperties") is not False:
        errors.append("intake schema root must reject additional properties")
    if schema.get("required") != sorted(EXPECTED_ROOT_FIELDS):
        errors.append("intake schema root required fields drifted")
    properties = exact_fields(
        schema.get("properties"),
        EXPECTED_ROOT_FIELDS,
        "intake schema properties",
        errors,
    )
    if properties.get("schema_version", {}).get("const") != SCHEMA_VERSION:
        errors.append("intake schema must pin schema_version 1")
    if properties.get("contract_id", {}).get("const") != CONTRACT_ID:
        errors.append(f"intake schema must pin contract_id {CONTRACT_ID}")
    if properties.get("bead_id", {}).get("const") != CONTRACT_BEAD:
        errors.append(f"intake schema must pin bead_id {CONTRACT_BEAD}")
    lessons = properties.get("lessons", {})
    if not isinstance(lessons, dict) or lessons.get("type") != "array":
        errors.append("intake schema lessons must be an array")
    elif lessons.get("items") != {"$ref": "#/$defs/lesson"}:
        errors.append(
            "intake schema lessons must reference the canonical lesson definition"
        )
    definitions = schema.get("$defs")
    required_definitions = {
        "lesson",
        "source",
        "license",
        "evidence",
        "target",
        "assumption",
        "regression_test",
        "code_use",
        "copy_record",
        "decision",
    }
    if not isinstance(definitions, dict) or set(definitions) != required_definitions:
        errors.append("intake schema definitions drifted")
        return
    definition_fields = {
        "lesson": EXPECTED_LESSON_FIELDS,
        "source": EXPECTED_SOURCE_FIELDS,
        "license": EXPECTED_LICENSE_FIELDS,
        "evidence": EXPECTED_EVIDENCE_FIELDS,
        "target": EXPECTED_TARGET_FIELDS,
        "assumption": EXPECTED_ASSUMPTION_FIELDS,
        "regression_test": EXPECTED_TEST_FIELDS,
        "code_use": EXPECTED_CODE_USE_FIELDS,
        "copy_record": EXPECTED_COPY_FIELDS,
        "decision": EXPECTED_DECISION_FIELDS,
    }
    for name, fields in definition_fields.items():
        definition = definitions.get(name, {})
        if definition.get("type") != "object":
            errors.append(f"intake schema {name} definition must be an object")
        if definition.get("additionalProperties") is not False:
            errors.append(
                f"intake schema {name} definition must reject additional properties"
            )
        if definition.get("required") != sorted(fields):
            errors.append(f"intake schema {name} required fields drifted")
        exact_fields(
            definition.get("properties"),
            fields,
            f"intake schema {name} properties",
            errors,
        )
    policy = properties.get("policy", {})
    if policy.get("additionalProperties") is not False:
        errors.append("intake schema policy must reject additional properties")
    if policy.get("required") != sorted(EXPECTED_POLICY_FIELDS):
        errors.append("intake schema policy required fields drifted")
    exact_fields(
        policy.get("properties"),
        EXPECTED_POLICY_FIELDS,
        "intake schema policy properties",
        errors,
    )
    lesson = definitions.get("lesson", {})
    source = definitions.get("source", {})
    commit_schema = source.get("properties", {}).get("commit", {})
    if commit_schema.get("pattern") != "^[0-9a-f]{40}$":
        errors.append(
            "intake schema source commit must require an exact lowercase SHA-1"
        )
    accepted_rule = lesson.get("allOf")
    if not isinstance(accepted_rule, list) or len(accepted_rule) < 2:
        errors.append(
            "intake schema must encode accepted and rejected decision conditions"
        )


def validate_license(
    repo: Path,
    value: Any,
    label: str,
    commit: str,
    repository: str,
    errors: list[str],
) -> None:
    license_row = exact_fields(value, EXPECTED_LICENSE_FIELDS, label, errors)
    identity = nonempty(license_row.get("identity"), f"{label}.identity", errors)
    substantive(
        license_row.get("provenance_statement"),
        f"{label}.provenance_statement",
        errors,
    )
    evidence = require_repo_file(
        repo, license_row.get("evidence_path"), f"{label}.evidence_path", errors
    )
    if evidence is None:
        return
    try:
        contents = evidence.read_text(encoding="utf-8")
    except OSError as exc:
        errors.append(f"could not read {label}.evidence_path: {exc}")
        return
    if identity and not contains_token(contents, identity):
        errors.append(f"{label}.evidence_path does not record the license identity")
    if commit and commit not in contents:
        errors.append(f"{label}.evidence_path does not record the source commit")
    repository_name = repository.rstrip("/").rsplit("/", 1)[-1]
    if repository_name and repository_name.casefold() not in contents.casefold():
        errors.append(f"{label}.evidence_path does not record the source repository")


def validate_source(
    repo: Path, value: Any, label: str, errors: list[str]
) -> tuple[list[str], set[str], str]:
    source = exact_fields(value, EXPECTED_SOURCE_FIELDS, label, errors)
    repository = nonempty(source.get("repository"), f"{label}.repository", errors)
    if repository:
        parsed = urlsplit(repository)
        path_parts = [part for part in parsed.path.split("/") if part]
        if (
            parsed.scheme != "https"
            or not parsed.netloc
            or parsed.username is not None
            or parsed.password is not None
            or len(path_parts) < 2
            or parsed.query
            or parsed.fragment
            or repository.endswith("/")
        ):
            errors.append(f"{label}.repository must be a stable https repository URL")
    commit = nonempty(source.get("commit"), f"{label}.commit", errors)
    if commit and not COMMIT_RE.fullmatch(commit):
        errors.append(f"{label}.commit must be an exact 40-character lowercase commit")

    raw_identifiers = array(source.get("identifiers"), f"{label}.identifiers", errors)
    identifiers: list[str] = []
    for index, raw in enumerate(raw_identifiers):
        identifier = nonempty(raw, f"{label}.identifiers[{index}]", errors)
        if identifier and not IDENTIFIER_RE.fullmatch(identifier):
            errors.append(
                f"{label}.identifiers[{index}] has an invalid source identifier"
            )
        if identifier:
            identifiers.append(identifier)
    if not identifiers:
        errors.append(f"{label}.identifiers must contain at least one source name")
    if len(set(identifiers)) != len(identifiers):
        errors.append(f"{label}.identifiers must be unique")

    license_value = source.get("license")
    validate_license(
        repo, license_value, f"{label}.license", commit, repository, errors
    )
    license_identity = (
        license_value.get("identity", "") if isinstance(license_value, dict) else ""
    )

    raw_evidence = array(source.get("evidence"), f"{label}.evidence", errors)
    linked_source_paths: set[str] = set()
    if not raw_evidence:
        errors.append(f"{label}.evidence must contain at least one source link")
    evidence_keys: list[tuple[str, str]] = []
    for index, raw in enumerate(raw_evidence):
        evidence_label = f"{label}.evidence[{index}]"
        evidence = exact_fields(raw, EXPECTED_EVIDENCE_FIELDS, evidence_label, errors)
        source_path = validate_relative_path(
            evidence.get("source_path"), f"{evidence_label}.source_path", errors
        )
        source_url = nonempty(
            evidence.get("source_url"), f"{evidence_label}.source_url", errors
        )
        substantive(evidence.get("claim"), f"{evidence_label}.claim", errors)
        if source_path:
            linked_source_paths.add(source_path)
        if source_path and source_url:
            evidence_keys.append((source_path, source_url))
        if repository and commit and source_path and source_url:
            unfragmented = source_url.split("#", 1)[0].split("?", 1)[0]
            expected = f"{repository}/blob/{commit}/{source_path}"
            if unfragmented != expected:
                errors.append(
                    f"{evidence_label}.source_url must link source_path at the exact commit"
                )
        local = require_repo_file(
            repo,
            evidence.get("local_evidence_path"),
            f"{evidence_label}.local_evidence_path",
            errors,
        )
        if local is not None:
            try:
                contents = local.read_text(encoding="utf-8")
            except OSError as exc:
                errors.append(
                    f"could not read {evidence_label}.local_evidence_path: {exc}"
                )
            else:
                if commit and commit not in contents:
                    errors.append(
                        f"{evidence_label}.local_evidence_path does not record the exact commit"
                    )
                if source_path and source_path not in contents:
                    errors.append(
                        f"{evidence_label}.local_evidence_path does not record source_path"
                    )
    if len(set(evidence_keys)) != len(evidence_keys):
        errors.append(f"{label}.evidence must not contain duplicate source links")
    return identifiers, linked_source_paths, str(license_identity)


def validate_target(
    repo: Path,
    value: Any,
    label: str,
    identifiers: list[str],
    errors: list[str],
) -> None:
    target = exact_fields(value, EXPECTED_TARGET_FIELDS, label, errors)
    kind = nonempty(target.get("kind"), f"{label}.kind", errors)
    if kind and kind not in TARGET_KINDS:
        errors.append(f"{label}.kind must be one of {TARGET_KINDS}")
    target_id = nonempty(target.get("id"), f"{label}.id", errors)
    if target_id and not TARGET_ID_RE.fullmatch(target_id):
        errors.append(f"{label}.id must be a generic capability or policy identifier")
    contract_path = validate_relative_path(
        target.get("contract_path"), f"{label}.contract_path", errors
    )
    if contract_path:
        contract = require_repo_file(
            repo, contract_path, f"{label}.contract_path", errors, prefix="product/"
        )
        if contract is not None and target_id:
            try:
                contents = contract.read_text(encoding="utf-8")
            except OSError as exc:
                errors.append(f"could not read {label}.contract_path: {exc}")
            else:
                if target_id not in contents:
                    errors.append(f"{label}.contract_path does not record target id")
    mapping = substantive(target.get("mapping"), f"{label}.mapping", errors)
    for field, text in (
        ("id", target_id),
        ("contract_path", contract_path),
        ("mapping", mapping),
    ):
        source_name = has_identifier(text, identifiers)
        if source_name:
            errors.append(
                f"{label}.{field} is source-specific ({source_name}); targets must be generic"
            )


def validate_assumptions(repo: Path, value: Any, label: str, errors: list[str]) -> None:
    assumptions = array(value, label, errors)
    if not assumptions:
        errors.append(f"{label} must record at least one source-specific assumption")
    for index, raw in enumerate(assumptions):
        assumption_label = f"{label}[{index}]"
        assumption = exact_fields(
            raw, EXPECTED_ASSUMPTION_FIELDS, assumption_label, errors
        )
        substantive(
            assumption.get("assumption"), f"{assumption_label}.assumption", errors
        )
        nonempty(
            assumption.get("adapter_boundary"),
            f"{assumption_label}.adapter_boundary",
            errors,
        )
        adapter_path = validate_relative_path(
            assumption.get("adapter_path"),
            f"{assumption_label}.adapter_path",
            errors,
        )
        if adapter_path:
            if not is_external_adapter_path(adapter_path):
                errors.append(
                    f"{assumption_label}.adapter_path must stay inside a concrete external provider adapter"
                )
            require_repo_file(
                repo, adapter_path, f"{assumption_label}.adapter_path", errors
            )


def validate_regression_tests(
    repo: Path,
    value: Any,
    label: str,
    status: str,
    identifiers: list[str],
    errors: list[str],
) -> None:
    tests = array(value, label, errors)
    if status == "accepted" and not tests:
        errors.append(
            f"{label} must contain a real neutral test for an accepted lesson"
        )
    seen: set[str] = set()
    for index, raw in enumerate(tests):
        test_label = f"{label}[{index}]"
        test = exact_fields(raw, EXPECTED_TEST_FIELDS, test_label, errors)
        path = validate_relative_path(test.get("path"), f"{test_label}.path", errors)
        proves = substantive(test.get("proves"), f"{test_label}.proves", errors)
        if path:
            if path in seen:
                errors.append(f"{label} contains duplicate test path {path}")
            seen.add(path)
            require_repo_file(repo, path, f"{test_label}.path", errors, prefix="tests/")
        for field, text in (("path", path), ("proves", proves)):
            source_name = has_identifier(text, identifiers)
            if source_name:
                errors.append(
                    f"{test_label}.{field} is source-specific ({source_name}); regression tests must be provider-neutral"
                )


def validate_code_use(
    repo: Path,
    value: Any,
    label: str,
    status: str,
    linked_source_paths: set[str],
    license_identity: str,
    errors: list[str],
) -> None:
    code_use = exact_fields(value, EXPECTED_CODE_USE_FIELDS, label, errors)
    mode = nonempty(code_use.get("mode"), f"{label}.mode", errors)
    if mode and mode not in CODE_MODES:
        errors.append(f"{label}.mode must be one of {CODE_MODES}")
    copied = code_use.get("external_code_copied")
    if not isinstance(copied, bool):
        errors.append(f"{label}.external_code_copied must be a boolean")
    records = array(code_use.get("copy_records"), f"{label}.copy_records", errors)
    substantive(code_use.get("note"), f"{label}.note", errors)

    if status == "rejected":
        if mode != "none_rejected":
            errors.append(f"{label}.mode must be none_rejected for a rejected lesson")
        if copied is not False or records:
            errors.append(f"{label} cannot copy external code for a rejected lesson")
    elif copied is True:
        if mode != "copied_external_code":
            errors.append(f"{label}.mode must record copied_external_code")
        if not records:
            errors.append(
                f"{label}.copy_records must record provenance for copied external code"
            )
    elif copied is False:
        if mode != "clean_reimplementation":
            errors.append(f"{label}.mode must record clean_reimplementation")
        if records:
            errors.append(f"{label}.copy_records must be empty when no code was copied")

    record_keys: list[tuple[str, str, str]] = []
    for index, raw in enumerate(records):
        record_label = f"{label}.copy_records[{index}]"
        record = exact_fields(raw, EXPECTED_COPY_FIELDS, record_label, errors)
        source_path = validate_relative_path(
            record.get("source_path"), f"{record_label}.source_path", errors
        )
        if source_path and source_path not in linked_source_paths:
            errors.append(
                f"{record_label}.source_path must have an exact-commit source evidence link"
            )
        destination = require_repo_file(
            repo,
            record.get("destination_path"),
            f"{record_label}.destination_path",
            errors,
        )
        notice = require_repo_file(
            repo,
            record.get("license_notice_path"),
            f"{record_label}.license_notice_path",
            errors,
        )
        if destination is not None and notice is not None and source_path:
            record_keys.append(
                (
                    source_path,
                    str(record.get("destination_path")),
                    str(record.get("license_notice_path")),
                )
            )
        if notice is not None and license_identity:
            try:
                notice_contents = notice.read_text(encoding="utf-8")
            except OSError as exc:
                errors.append(
                    f"could not read {record_label}.license_notice_path: {exc}"
                )
            else:
                if not contains_token(notice_contents, license_identity):
                    errors.append(
                        f"{record_label}.license_notice_path does not record the source license identity"
                    )
    if len(set(record_keys)) != len(record_keys):
        errors.append(f"{label}.copy_records must be unique")


def validate_decision(value: Any, label: str, status: str, errors: list[str]) -> None:
    decision = exact_fields(value, EXPECTED_DECISION_FIELDS, label, errors)
    substantive(decision.get("rationale"), f"{label}.rationale", errors)
    rejection = decision.get("rejection_rationale")
    if status == "accepted" and rejection is not None:
        errors.append(
            f"{label}.rejection_rationale must be null for an accepted lesson"
        )
    if status == "rejected":
        substantive(rejection, f"{label}.rejection_rationale", errors)


def validate_lesson(
    repo: Path,
    value: Any,
    index: int,
    issue_ids: set[str],
    errors: list[str],
) -> str:
    label = f"lessons[{index}]"
    lesson = exact_fields(value, EXPECTED_LESSON_FIELDS, label, errors)
    lesson_id = nonempty(lesson.get("lesson_id"), f"{label}.lesson_id", errors)
    if lesson_id and not LESSON_ID_RE.fullmatch(lesson_id):
        errors.append(f"{label}.lesson_id must be a lowercase stable identifier")
    status = nonempty(lesson.get("status"), f"{label}.status", errors)
    if status and status not in STATUSES:
        errors.append(f"{label}.status must be one of {STATUSES}")

    identifiers, linked_source_paths, license_identity = validate_source(
        repo, lesson.get("source"), f"{label}.source", errors
    )
    invariant = substantive(
        lesson.get("extracted_generic_invariant"),
        f"{label}.extracted_generic_invariant",
        errors,
    )
    source_name = has_identifier(invariant, identifiers)
    if source_name:
        errors.append(
            f"{label}.extracted_generic_invariant is source-specific ({source_name})"
        )
    validate_target(repo, lesson.get("target"), f"{label}.target", identifiers, errors)
    validate_assumptions(
        repo, lesson.get("source_assumptions"), f"{label}.source_assumptions", errors
    )
    validate_regression_tests(
        repo,
        lesson.get("neutral_regression_tests"),
        f"{label}.neutral_regression_tests",
        status,
        identifiers,
        errors,
    )

    bead = nonempty(
        lesson.get("implementation_bead"), f"{label}.implementation_bead", errors
    )
    if bead and (not BEAD_RE.fullmatch(bead) or bead not in issue_ids):
        errors.append(
            f"{label}.implementation_bead references unknown Beads issue {bead}"
        )
    validate_code_use(
        repo,
        lesson.get("code_use"),
        f"{label}.code_use",
        status,
        linked_source_paths,
        license_identity,
        errors,
    )
    validate_decision(lesson.get("decision"), f"{label}.decision", status, errors)
    return lesson_id


def validate_document(
    repo: Path,
    document: dict[str, Any],
    issue_ids: set[str],
    errors: list[str],
) -> tuple[int, int]:
    exact_fields(document, EXPECTED_ROOT_FIELDS, "intake", errors)
    if document.get("schema_version") != SCHEMA_VERSION:
        errors.append("intake.schema_version must be 1")
    if document.get("contract_id") != CONTRACT_ID:
        errors.append(f"intake.contract_id must be {CONTRACT_ID}")
    if document.get("bead_id") != CONTRACT_BEAD:
        errors.append(f"intake.bead_id must be {CONTRACT_BEAD}")
    elif CONTRACT_BEAD not in issue_ids:
        errors.append(f"intake.bead_id references unknown Beads issue {CONTRACT_BEAD}")
    nonempty(document.get("title"), "intake.title", errors)

    policy = exact_fields(
        document.get("policy"), EXPECTED_POLICY_FIELDS, "intake.policy", errors
    )
    if policy.get("statuses") != STATUSES:
        errors.append(f"intake.policy.statuses must be exactly {STATUSES}")
    if policy.get("target_kinds") != TARGET_KINDS:
        errors.append(f"intake.policy.target_kinds must be exactly {TARGET_KINDS}")
    if policy.get("accepted_requires_neutral_regression_tests") is not True:
        errors.append("accepted lessons must require neutral regression tests")
    if policy.get("source_specific_assumptions_boundary") != "adapter_only":
        errors.append("source-specific assumptions must be confined to adapters")
    if policy.get("copied_external_code_policy") != (
        "recorded_license_and_provenance_required"
    ):
        errors.append(
            "copied external code must require recorded license and provenance"
        )

    lessons = array(document.get("lessons"), "intake.lessons", errors)
    if not lessons:
        errors.append("intake.lessons must contain at least one lesson")
    lesson_ids: list[str] = []
    accepted = 0
    for index, lesson in enumerate(lessons):
        lesson_id = validate_lesson(repo, lesson, index, issue_ids, errors)
        if lesson_id:
            lesson_ids.append(lesson_id)
        if isinstance(lesson, dict) and lesson.get("status") == "accepted":
            accepted += 1
    duplicates = sorted(
        lesson_id for lesson_id in set(lesson_ids) if lesson_ids.count(lesson_id) > 1
    )
    if duplicates:
        errors.append(f"intake lesson IDs must be unique: {duplicates}")
    return accepted, len(lessons) - accepted


def main() -> int:
    args = parse_args()
    repo = args.repo.resolve()
    errors: list[str] = []
    schema = load_object(
        resolve(repo, args.schema), "external lesson intake schema", errors
    )
    document = load_object(resolve(repo, args.intake), "external lesson intake", errors)
    issue_ids = load_issue_ids(resolve(repo, args.issues), errors)
    if schema:
        validate_schema(schema, errors)
    accepted = rejected = 0
    if document:
        accepted, rejected = validate_document(repo, document, issue_ids, errors)
    if errors:
        for error in errors:
            print(f"error: {error}", file=sys.stderr)
        return 1
    print(
        "external lesson intake valid: "
        f"{accepted + rejected} lesson(s), {accepted} accepted, {rejected} rejected"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
