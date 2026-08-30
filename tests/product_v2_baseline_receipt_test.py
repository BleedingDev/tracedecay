#!/usr/bin/env python3
"""Focused contract tests for the versioned V2 baseline receipt verifier."""

from __future__ import annotations

import copy
import json
import subprocess
import tempfile
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
CHECKER = ROOT / "scripts/product/check-v2-baseline.py"
FLOOR = "08fbe33a7c7f403191fd5d6e356c7b6681b96403"
ZERO_DIGEST = "0" * 64
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


def git(*arguments: str) -> str:
    return subprocess.run(
        ["git", "-C", str(ROOT), *arguments],
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()


def command(command_id: str, *, passed: bool = True) -> dict[str, Any]:
    focused = command_id in FOCUS_IDS
    return {
        "id": command_id,
        "lane": "fixture",
        "argv": ["fixture", command_id],
        "cwd": ".",
        "timeout_seconds": 1,
        "started_at": "2026-08-30T00:00:00Z",
        "completed_at": "2026-08-30T00:00:01Z",
        "duration_ms": 1,
        "exit_code": 0 if passed else 1,
        "timed_out": False,
        "passed": passed,
        "classification": "passed" if passed else "upstream_failure",
        "required_to_build": command_id in SETUP_IDS | BUILD_IDS,
        "focused_test": focused,
        "stdout_bytes": 0,
        "stderr_bytes": 0,
        "stdout_sha256": ZERO_DIGEST,
        "stderr_sha256": ZERO_DIGEST,
        "stdout_tail": "",
        "stderr_tail": "fixture failure" if not passed else "",
        "failure_summary": None if passed else "fixture failure",
        "failure_fingerprint": None if passed else "1" * 64,
    }


def receipt(*, degraded: bool = False) -> dict[str, Any]:
    required = sorted(TOOL_IDS | SETUP_IDS | BUILD_IDS | FOCUS_IDS)
    commands = [
        command(command_id, passed=not (degraded and command_id == "memory_tests"))
        for command_id in required
    ]
    return {
        "schema_version": 1,
        "issue_id": "tdmem-0102",
        "captured_at": "2026-08-30T00:00:02Z",
        "platform": {"system": "fixture"},
        "repository": {
            "branch": "feat/pluggable-memory-providers-v2",
            "head_sha": git("rev-parse", "HEAD"),
            "pinned_floor_sha": FLOOR,
            "changed_paths_since_floor": [],
            "runtime_paths_changed_since_floor": [],
            "initial_checkout_clean": True,
        },
        "policy": {},
        "commands": commands,
        "summary": {
            "overall_status": "degraded" if degraded else "passed",
            "closure_eligible": True,
            "build_failures": [],
            "focused_failures": ["memory_tests"] if degraded else [],
            "missing_focus_lanes": [],
            "commands_total": len(commands),
            "commands_passed": len(commands) - int(degraded),
            "commands_failed": int(degraded),
            "duration_ms": len(commands),
        },
    }


def run_case(document: dict[str, Any], expected_status: int, needle: str | None = None) -> None:
    with tempfile.TemporaryDirectory(prefix="tracedecay-baseline-receipt-") as directory:
        path = Path(directory) / "receipt.json"
        path.write_text(json.dumps(document), encoding="utf-8")
        result = subprocess.run(
            [
                "python3",
                str(CHECKER),
                "--repo",
                str(ROOT),
                "--receipt",
                str(path),
            ],
            check=False,
            capture_output=True,
            text=True,
        )
    assert result.returncode == expected_status, result.stderr or result.stdout
    if needle is not None:
        assert needle in result.stderr + result.stdout, result.stderr or result.stdout


def main() -> None:
    run_case(receipt(), 0, '"overall_status": "passed"')
    run_case(receipt(degraded=True), 0, '"overall_status": "degraded"')

    missing = receipt()
    missing["commands"] = [
        item for item in missing["commands"] if item["id"] != "daemon_tests"
    ]
    run_case(missing, 1, "missing required commands")

    runtime_changed = receipt()
    runtime_changed["repository"]["runtime_paths_changed_since_floor"] = [
        "crates/tracedecay/src/lib.rs"
    ]
    run_case(runtime_changed, 1, "runtime paths changed")

    build_failed = receipt()
    for item in build_failed["commands"]:
        if item["id"] == "rust_cli_build":
            item.update(command("rust_cli_build", passed=False))
    build_failed["summary"]["overall_status"] = "failed"
    build_failed["summary"]["closure_eligible"] = False
    build_failed["summary"]["build_failures"] = ["rust_cli_build"]
    run_case(build_failed, 1, "clean baseline prerequisites/builds failed")

    unisolated = receipt(degraded=True)
    for item in unisolated["commands"]:
        if item["id"] == "memory_tests":
            item["failure_fingerprint"] = None
    run_case(unisolated, 1, "lacks a failure fingerprint")


if __name__ == "__main__":
    main()
