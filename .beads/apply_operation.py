#!/usr/bin/env python3
"""Apply exactly one reviewed Beads operation and repack its JSONL authority."""

from __future__ import annotations

import base64
import bz2
import hashlib
import json
import os
import re
import shutil
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, NoReturn, Sequence


BEADS_DIR = Path(__file__).resolve().parent
ROOT = BEADS_DIR.parent
PENDING_DIR = BEADS_DIR / "operations/pending"
APPLIED_DIR = BEADS_DIR / "operations/applied"
RECEIPTS_DIR = BEADS_DIR / "receipts"
JSONL_PATH = BEADS_DIR / "issues.jsonl"
PLAN_DIR = BEADS_DIR / "plan"
MATERIALIZER = BEADS_DIR / "materialize.py"
MATERIALIZE_WORKFLOW = ROOT / ".github/workflows/materialize-beads.yml"
PART_GLOB = "issues.jsonl.bz2.b64.part*"
PART_PREFIX = "issues.jsonl.bz2.b64.part"
PART_SIZE = 4_000
ISSUE_ID = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]*$")


class OperationError(ValueError):
    """A pending operation is malformed or failed its evidence gates."""


def fail(message: str) -> NoReturn:
    raise OperationError(message)


def sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def tail(value: str, limit: int = 4_000) -> str:
    return value if len(value) <= limit else value[-limit:]


def command_receipt(
    name: str,
    argv: Sequence[str],
    result: subprocess.CompletedProcess[str],
    duration_ms: int,
) -> dict[str, object]:
    stdout = result.stdout or ""
    stderr = result.stderr or ""
    return {
        "name": name,
        "argv": list(argv),
        "exit_code": result.returncode,
        "duration_ms": duration_ms,
        "stdout_bytes": len(stdout.encode("utf-8")),
        "stderr_bytes": len(stderr.encode("utf-8")),
        "stdout_sha256": sha256_bytes(stdout.encode("utf-8")),
        "stderr_sha256": sha256_bytes(stderr.encode("utf-8")),
        "stdout_tail": tail(stdout),
        "stderr_tail": tail(stderr),
    }


def run(
    name: str,
    argv: Sequence[str],
    *,
    timeout_seconds: int = 900,
    allowed_statuses: frozenset[int] = frozenset({0}),
) -> tuple[subprocess.CompletedProcess[str], dict[str, object]]:
    if not argv or any(not isinstance(item, str) or not item for item in argv):
        fail(f"{name}: argv must contain non-empty strings")
    if not 1 <= timeout_seconds <= 3_600:
        fail(f"{name}: timeout_seconds must be between 1 and 3600")
    started = time.monotonic()
    try:
        result = subprocess.run(
            list(argv),
            cwd=ROOT,
            check=False,
            capture_output=True,
            text=True,
            timeout=timeout_seconds,
            env={**os.environ, "NO_COLOR": "1", "CLICOLOR": "0"},
        )
    except (OSError, subprocess.TimeoutExpired) as error:
        fail(f"{name}: command failed to run: {error}")
    duration_ms = round((time.monotonic() - started) * 1_000)
    receipt = command_receipt(name, argv, result, duration_ms)
    if result.returncode not in allowed_statuses:
        detail = tail(result.stderr or result.stdout or "no diagnostic")
        fail(f"{name}: exited {result.returncode}: {detail}")
    return result, receipt


def require_mapping(value: object, authority: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        fail(f"{authority} must be an object")
    return value


def require_string(mapping: dict[str, Any], key: str, authority: str) -> str:
    value = mapping.get(key)
    if not isinstance(value, str) or not value.strip():
        fail(f"{authority}.{key} must be a non-empty string")
    return value.strip()


def load_operation() -> tuple[Path, dict[str, Any], bytes]:
    pending = sorted(PENDING_DIR.glob("*.json"))
    if len(pending) != 1:
        fail(f"expected exactly one pending operation, found {len(pending)}")
    path = pending[0]
    raw = path.read_bytes()
    operation = require_mapping(json.loads(raw), "operation")
    if operation.get("schema_version") != 1:
        fail("operation.schema_version must be 1")
    issue_id = require_string(operation, "issue_id", "operation")
    if not ISSUE_ID.fullmatch(issue_id):
        fail("operation.issue_id contains unsupported characters")
    if path.stem != issue_id:
        fail(f"pending filename {path.name!r} must match issue id {issue_id!r}")
    mode = require_string(operation, "mode", "operation")
    if mode not in {"claim", "complete"}:
        fail("operation.mode must be 'claim' or 'complete'")
    require_string(operation, "actor", "operation")
    require_string(operation, "claim_comment", "operation")
    if mode == "complete":
        require_string(operation, "close_reason", "operation")
        require_string(operation, "close_comment", "operation")
        criteria = operation.get("acceptance_criteria")
        if not isinstance(criteria, list) or not criteria:
            fail("operation.acceptance_criteria must be a non-empty array")
        if any(not isinstance(item, str) or not item.strip() for item in criteria):
            fail("operation.acceptance_criteria entries must be non-empty strings")
        checks = operation.get("checks")
        if not isinstance(checks, list) or not checks:
            fail("operation.checks must be a non-empty array")
    return path, operation, raw


def unwrap_issue(document: object) -> dict[str, Any]:
    payload = document
    if isinstance(payload, dict) and "data" in payload:
        payload = payload["data"]
    if isinstance(payload, list):
        if len(payload) != 1 or not isinstance(payload[0], dict):
            fail("br show returned an unexpected issue list")
        payload = payload[0]
    if not isinstance(payload, dict):
        fail("br show returned an unexpected payload")
    return payload


def show_issue(issue_id: str) -> tuple[dict[str, Any], dict[str, object]]:
    result, receipt = run("show issue", ["br", "show", issue_id, "--json"])
    try:
        issue = unwrap_issue(json.loads(result.stdout))
    except json.JSONDecodeError as error:
        fail(f"br show returned invalid JSON: {error}")
    if issue.get("id") != issue_id:
        fail(f"br show returned issue {issue.get('id')!r}, expected {issue_id!r}")
    return issue, receipt


def parse_checks(operation: dict[str, Any]) -> list[tuple[str, list[str], int]]:
    parsed: list[tuple[str, list[str], int]] = []
    for index, raw_check in enumerate(operation.get("checks", [])):
        check = require_mapping(raw_check, f"operation.checks[{index}]")
        name = require_string(check, "name", f"operation.checks[{index}]")
        argv = check.get("argv")
        if not isinstance(argv, list) or not argv:
            fail(f"operation.checks[{index}].argv must be a non-empty array")
        if any(not isinstance(item, str) or not item for item in argv):
            fail(f"operation.checks[{index}].argv entries must be non-empty strings")
        timeout = check.get("timeout_seconds", 900)
        if not isinstance(timeout, int):
            fail(f"operation.checks[{index}].timeout_seconds must be an integer")
        parsed.append((name, list(argv), timeout))
    return parsed


def validate_cycles(stdout: str) -> None:
    try:
        document = json.loads(stdout)
    except json.JSONDecodeError as error:
        fail(f"br dep cycles returned invalid JSON: {error}")
    payload: object = document
    if isinstance(payload, dict) and "data" in payload:
        payload = payload["data"]
    if isinstance(payload, dict):
        count = payload.get("count", payload.get("total_count"))
        cycles = payload.get("cycles", [])
        if count not in (None, 0) or cycles not in (None, []):
            fail(f"dependency cycles remain: {payload!r}")
    elif payload not in (None, []):
        fail(f"dependency cycles remain: {payload!r}")


def validate_jsonl(raw: bytes) -> tuple[list[dict[str, Any]], str]:
    try:
        text = raw.decode("utf-8")
    except UnicodeDecodeError as error:
        fail(f"issues.jsonl is not UTF-8: {error}")
    lines = [line for line in text.splitlines() if line.strip()]
    issues: list[dict[str, Any]] = []
    for line_number, line in enumerate(lines, start=1):
        try:
            issue = json.loads(line)
        except json.JSONDecodeError as error:
            fail(f"issues.jsonl line {line_number} is invalid: {error}")
        if not isinstance(issue, dict):
            fail(f"issues.jsonl line {line_number} is not an object")
        issues.append(issue)
    ids = [issue.get("id") for issue in issues]
    if any(not isinstance(issue_id, str) or not issue_id for issue_id in ids):
        fail("every JSONL issue must have a non-empty string id")
    if len(ids) != len(set(ids)):
        fail("issues.jsonl contains duplicate ids")
    return issues, sha256_bytes(raw)


def replace_exact(pattern: str, replacement: str, path: Path, authority: str) -> None:
    content = path.read_text(encoding="utf-8")
    updated, count = re.subn(pattern, replacement, content, count=1, flags=re.MULTILINE)
    if count != 1:
        fail(f"could not update {authority} in {path}")
    path.write_text(updated, encoding="utf-8")


def pack_plan() -> dict[str, object]:
    raw = JSONL_PATH.read_bytes()
    issues, digest = validate_jsonl(raw)
    encoded = base64.b64encode(bz2.compress(raw, compresslevel=9)).decode("ascii")
    parts = [encoded[index : index + PART_SIZE] for index in range(0, len(encoded), PART_SIZE)]
    if not parts:
        fail("refusing to pack an empty JSONL payload")

    PLAN_DIR.mkdir(parents=True, exist_ok=True)
    for existing in PLAN_DIR.glob(PART_GLOB):
        existing.unlink()
    for index, part in enumerate(parts):
        (PLAN_DIR / f"{PART_PREFIX}{index:02d}").write_text(part + "\n", encoding="ascii")

    replace_exact(
        r"^EXPECTED_PARTS = \d+$",
        f"EXPECTED_PARTS = {len(parts)}",
        MATERIALIZER,
        "EXPECTED_PARTS",
    )
    replace_exact(
        r'^EXPECTED_SHA256 = "[0-9a-f]{64}"$',
        f'EXPECTED_SHA256 = "{digest}"',
        MATERIALIZER,
        "EXPECTED_SHA256",
    )
    replace_exact(
        r"^\s*BEADS_JSONL_SHA256: [0-9a-f]{64}$",
        f"      BEADS_JSONL_SHA256: {digest}",
        MATERIALIZE_WORKFLOW,
        "BEADS_JSONL_SHA256",
    )
    run("verify packed plan", ["python3", str(MATERIALIZER.relative_to(ROOT))])
    return {
        "issues": len(issues),
        "jsonl_sha256": digest,
        "encoded_bytes": len(encoded),
        "parts": len(parts),
        "part_size": PART_SIZE,
    }


def git_head() -> str:
    result, _ = run("read implementation head", ["git", "rev-parse", "HEAD"])
    return result.stdout.strip()


def apply() -> dict[str, object]:
    pending_path, operation, operation_raw = load_operation()
    issue_id = require_string(operation, "issue_id", "operation")
    actor = require_string(operation, "actor", "operation")
    mode = require_string(operation, "mode", "operation")
    started_at = utc_now()
    commands: list[dict[str, object]] = []

    _, receipt = run("rebuild Beads database", ["br", "sync", "--import-only", "--rebuild"])
    commands.append(receipt)
    issue, receipt = show_issue(issue_id)
    commands.append(receipt)

    status = issue.get("status")
    if status in {"open", "deferred", "draft"}:
        _, receipt = run(
            "claim issue",
            [
                "br",
                "update",
                "--actor",
                actor,
                issue_id,
                "--status",
                "in_progress",
                "--transition-comment",
                require_string(operation, "claim_comment", "operation"),
            ],
        )
        commands.append(receipt)
    elif status != "in_progress":
        fail(f"issue {issue_id} cannot be processed from status {status!r}")

    check_receipts: list[dict[str, object]] = []
    if mode == "complete":
        for name, argv, timeout in parse_checks(operation):
            _, receipt = run(name, argv, timeout_seconds=timeout)
            check_receipts.append(receipt)

        criteria = [item.strip() for item in operation["acceptance_criteria"]]
        criteria_text = "\n".join(f"- [x] {criterion}" for criterion in criteria)
        _, receipt = run(
            "complete acceptance criteria",
            [
                "br",
                "update",
                "--actor",
                actor,
                issue_id,
                "--acceptance-criteria",
                criteria_text,
            ],
        )
        commands.append(receipt)

        _, receipt = run(
            "close issue",
            [
                "br",
                "close",
                "--actor",
                actor,
                issue_id,
                "--reason",
                require_string(operation, "close_reason", "operation"),
                "--transition-comment",
                require_string(operation, "close_comment", "operation"),
            ],
        )
        commands.append(receipt)

    _, receipt = run("flush Beads JSONL", ["br", "sync", "--flush-only"])
    commands.append(receipt)
    cycles_result, receipt = run("check dependency cycles", ["br", "dep", "cycles", "--json"])
    commands.append(receipt)
    validate_cycles(cycles_result.stdout)

    doctor_result, receipt = run(
        "run Beads doctor",
        ["br", "doctor"],
        allowed_statuses=frozenset({0, 1}),
    )
    commands.append(receipt)
    if any(line.startswith("ERROR ") for line in doctor_result.stdout.splitlines()):
        fail("br doctor reported an ERROR line")
    if "HEALTH workspace: healthy" not in doctor_result.stdout:
        fail("br doctor did not report a healthy workspace")

    final_issue, receipt = show_issue(issue_id)
    commands.append(receipt)
    expected_status = "closed" if mode == "complete" else "in_progress"
    if final_issue.get("status") != expected_status:
        fail(
            f"issue {issue_id} ended in {final_issue.get('status')!r}, "
            f"expected {expected_status!r}"
        )

    pack_receipt = pack_plan()
    APPLIED_DIR.mkdir(parents=True, exist_ok=True)
    applied_path = APPLIED_DIR / pending_path.name
    if applied_path.exists():
        fail(f"applied operation already exists: {applied_path}")
    shutil.move(str(pending_path), applied_path)

    receipt_document = {
        "schema_version": 1,
        "issue_id": issue_id,
        "mode": mode,
        "actor": actor,
        "implementation_head": git_head(),
        "operation_sha256": sha256_bytes(operation_raw),
        "started_at": started_at,
        "completed_at": utc_now(),
        "final_status": expected_status,
        "checks": check_receipts,
        "beads_commands": commands,
        "plan": pack_receipt,
    }
    RECEIPTS_DIR.mkdir(parents=True, exist_ok=True)
    receipt_path = RECEIPTS_DIR / f"{issue_id}.json"
    receipt_path.write_text(
        json.dumps(receipt_document, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    return receipt_document


def main() -> None:
    try:
        receipt = apply()
    except (OSError, json.JSONDecodeError, OperationError) as error:
        print(f"apply-beads-operation: {error}", file=sys.stderr)
        raise SystemExit(1) from error
    print(json.dumps(receipt, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
