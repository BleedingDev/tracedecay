#!/usr/bin/env python3
"""Validate that a captured TraceDecay V2 baseline is closure-grade evidence."""

from __future__ import annotations

import argparse
import json
import re
import subprocess
import sys
from pathlib import Path
from typing import Any, NoReturn


EXPECTED_ISSUE_ID = "tdmem-0102"
EXPECTED_BRANCH = "feat/pluggable-memory-providers-v2"
EXPECTED_FLOOR = "08fbe33a7c7f403191fd5d6e356c7b6681b96403"
SHA256 = re.compile(r"^[0-9a-f]{64}$")
TOOL_IDS = {
    "rustc_version",
    "cargo_version",
    "nextest_version",
    "node_version",
    "npm_version",
    "python_version",
    "git_version",
    "ast_grep_version",
}
SETUP_IDS = {
    "initial_git_status",
    "floor_ancestry",
    "cargo_metadata",
    "cargo_clean",
    "dashboard_install",
    "dashboard_contracts",
    "dashboard_bundle",
}
BUILD_IDS = {"rust_cli_build", "dashboard_build"}
FOCUS_IDS = {
    "memory_tests",
    "retrieval_tests",
    "host_tests",
    "daemon_tests",
    "dashboard_unit_tests",
    "dashboard_api_tests",
}


class ReceiptError(ValueError):
    """The baseline receipt is incomplete or internally inconsistent."""


def fail(message: str) -> NoReturn:
    raise ReceiptError(message)


def require_mapping(value: object, authority: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        fail(f"{authority} must be an object")
    return value


def require_string(mapping: dict[str, Any], key: str, authority: str) -> str:
    value = mapping.get(key)
    if not isinstance(value, str) or not value.strip():
        fail(f"{authority}.{key} must be a non-empty string")
    return value.strip()


def git(root: Path, *arguments: str, allowed: frozenset[int] = frozenset({0})) -> int:
    result = subprocess.run(
        ["git", "-C", str(root), *arguments],
        check=False,
        capture_output=True,
        text=True,
        timeout=60,
    )
    if result.returncode not in allowed:
        detail = result.stderr.strip() or result.stdout.strip() or "no diagnostic"
        fail(f"git {' '.join(arguments)} exited {result.returncode}: {detail}")
    return result.returncode


def validate_command(command: dict[str, Any], command_id: str) -> None:
    if command.get("id") != command_id:
        fail(f"command map key {command_id!r} does not match payload id")
    argv = command.get("argv")
    if not isinstance(argv, list) or not argv or any(
        not isinstance(item, str) or not item for item in argv
    ):
        fail(f"{command_id}: argv must be a non-empty string array")
    for digest_key in ("stdout_sha256", "stderr_sha256"):
        digest = command.get(digest_key)
        if not isinstance(digest, str) or not SHA256.fullmatch(digest):
            fail(f"{command_id}: {digest_key} must be a SHA-256 digest")
    duration = command.get("duration_ms")
    if not isinstance(duration, int) or duration < 0:
        fail(f"{command_id}: duration_ms must be a non-negative integer")
    if command.get("passed") is not True:
        if command.get("classification") != "upstream_failure":
            fail(f"{command_id}: failed command lacks upstream_failure classification")
        if not isinstance(command.get("failure_summary"), str) or not command[
            "failure_summary"
        ].strip():
            fail(f"{command_id}: failed command lacks an isolated failure summary")
        fingerprint = command.get("failure_fingerprint")
        if not isinstance(fingerprint, str) or not SHA256.fullmatch(fingerprint):
            fail(f"{command_id}: failed command lacks a failure fingerprint")


def validate(root: Path, receipt_path: Path) -> dict[str, Any]:
    document = require_mapping(
        json.loads(receipt_path.read_text(encoding="utf-8")), "receipt"
    )
    if document.get("schema_version") != 1:
        fail("receipt.schema_version must be 1")
    if document.get("issue_id") != EXPECTED_ISSUE_ID:
        fail(f"receipt.issue_id must be {EXPECTED_ISSUE_ID}")

    repository = require_mapping(document.get("repository"), "receipt.repository")
    branch = require_string(repository, "branch", "receipt.repository")
    head = require_string(repository, "head_sha", "receipt.repository")
    floor = require_string(repository, "pinned_floor_sha", "receipt.repository")
    if branch != EXPECTED_BRANCH:
        fail(f"receipt branch mismatch: {branch!r}")
    if floor != EXPECTED_FLOOR:
        fail(f"receipt floor mismatch: {floor!r}")
    if repository.get("initial_checkout_clean") is not True:
        fail("baseline did not start from a clean checkout")
    runtime_changes = repository.get("runtime_paths_changed_since_floor")
    if runtime_changes != []:
        fail(f"runtime paths changed before baseline capture: {runtime_changes!r}")

    git(root, "cat-file", "-e", f"{head}^{{commit}}")
    ancestry = git(
        root,
        "merge-base",
        "--is-ancestor",
        floor,
        head,
        allowed=frozenset({0, 1}),
    )
    if ancestry != 0:
        fail(f"receipt head {head} does not descend from pinned floor {floor}")

    commands_raw = document.get("commands")
    if not isinstance(commands_raw, list):
        fail("receipt.commands must be an array")
    commands: dict[str, dict[str, Any]] = {}
    for raw_command in commands_raw:
        command = require_mapping(raw_command, "receipt.commands[]")
        command_id = require_string(command, "id", "receipt.commands[]")
        if command_id in commands:
            fail(f"duplicate command id {command_id!r}")
        commands[command_id] = command

    required_ids = TOOL_IDS | SETUP_IDS | BUILD_IDS | FOCUS_IDS
    missing = sorted(required_ids - set(commands))
    if missing:
        fail(f"receipt is missing required commands: {missing}")

    for command_id, command in commands.items():
        if command_id in required_ids:
            validate_command(command, command_id)

    hard_failures = sorted(
        command_id
        for command_id in TOOL_IDS | SETUP_IDS | BUILD_IDS
        if commands[command_id].get("passed") is not True
    )
    if hard_failures:
        fail(f"clean baseline prerequisites/builds failed: {hard_failures}")

    focused_failures = sorted(
        command_id
        for command_id in FOCUS_IDS
        if commands[command_id].get("passed") is not True
    )
    for command_id in focused_failures:
        validate_command(commands[command_id], command_id)

    summary = require_mapping(document.get("summary"), "receipt.summary")
    if summary.get("closure_eligible") is not True:
        fail("receipt summary is not closure eligible")
    expected_status = "degraded" if focused_failures else "passed"
    if summary.get("overall_status") != expected_status:
        fail(
            "receipt summary status mismatch: "
            f"expected {expected_status!r}, got {summary.get('overall_status')!r}"
        )
    if sorted(summary.get("focused_failures", [])) != focused_failures:
        fail("receipt focused failure summary does not match command evidence")

    return {
        "schema_version": 1,
        "verified": True,
        "issue_id": EXPECTED_ISSUE_ID,
        "head_sha": head,
        "pinned_floor_sha": floor,
        "overall_status": expected_status,
        "focused_failures": focused_failures,
        "commands_verified": len(required_ids),
    }


def main() -> None:
    root = Path(__file__).resolve().parents[2]
    parser = argparse.ArgumentParser()
    parser.add_argument("--repo", type=Path, default=root)
    parser.add_argument(
        "--receipt",
        type=Path,
        default=root / "product/baseline/tracedecay-v2-pr707-linux.json",
    )
    args = parser.parse_args()
    try:
        result = validate(args.repo.resolve(), args.receipt.resolve())
    except (OSError, json.JSONDecodeError, ReceiptError) as error:
        print(f"check-v2-baseline: {error}", file=sys.stderr)
        raise SystemExit(1) from error
    print(json.dumps(result, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
