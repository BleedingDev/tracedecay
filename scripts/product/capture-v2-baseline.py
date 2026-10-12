#!/usr/bin/env python3
"""Capture a clean, focused TraceDecay V2 baseline as a versioned receipt."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import platform
import subprocess
import sys
import tempfile
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, NoReturn, Sequence


SCHEMA_VERSION = 1
ISSUE_ID = "tdmem-0102"
DEFAULT_OUTPUT = Path("product/baseline/tracedecay-v2-pr707-linux.json")
PROVENANCE = Path("product/upstream/tracedecay-v2-pr707.json")
RUNTIME_PREFIXES = (
    "crates/",
    "dashboard/src/",
    "dashboard/codegen/",
    "plugin/",
    "build.rs",
)
SETUP_IDS = {
    "initial_git_status",
    "floor_ancestry",
    "cargo_metadata",
    "cargo_clean",
    "dashboard_install",
    "dashboard_contracts",
    "dashboard_build",
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
TAIL_LIMIT = 12_000


class BaselineError(ValueError):
    """The baseline harness cannot produce a trustworthy receipt."""


def fail(message: str) -> NoReturn:
    raise BaselineError(message)


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def text_tail(value: str, limit: int = TAIL_LIMIT) -> str:
    if len(value) <= limit:
        return value
    return value[-limit:]


def failure_summary(stdout: str, stderr: str) -> str | None:
    for source in (stderr, stdout):
        lines = [line.strip() for line in source.splitlines() if line.strip()]
        if lines:
            return lines[-1][:500]
    return None


def command_fingerprint(
    argv: Sequence[str], exit_code: int | None, timed_out: bool, stdout: str, stderr: str
) -> str:
    material = json.dumps(
        {
            "argv": list(argv),
            "exit_code": exit_code,
            "timed_out": timed_out,
            "stdout_sha256": sha256_bytes(stdout.encode("utf-8")),
            "stderr_sha256": sha256_bytes(stderr.encode("utf-8")),
        },
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return sha256_bytes(material)


def run_command(
    root: Path,
    *,
    command_id: str,
    lane: str,
    argv: Sequence[str],
    cwd: Path | None = None,
    timeout_seconds: int,
    required_to_build: bool = False,
    focused_test: bool = False,
    expect_stdout_empty: bool = False,
) -> dict[str, Any]:
    if not argv or any(not isinstance(item, str) or not item for item in argv):
        fail(f"{command_id}: argv must contain non-empty strings")
    working_directory = (root / cwd).resolve() if cwd is not None else root
    started_at = utc_now()
    started = time.monotonic()
    timed_out = False
    exit_code: int | None
    stdout = ""
    stderr = ""
    try:
        result = subprocess.run(
            list(argv),
            cwd=working_directory,
            check=False,
            capture_output=True,
            text=True,
            timeout=timeout_seconds,
            env={
                **os.environ,
                "NO_COLOR": "1",
                "CLICOLOR": "0",
                "CARGO_TERM_COLOR": "never",
            },
        )
        exit_code = result.returncode
        stdout = result.stdout or ""
        stderr = result.stderr or ""
    except subprocess.TimeoutExpired as error:
        timed_out = True
        exit_code = None
        stdout = (error.stdout or "") if isinstance(error.stdout, str) else ""
        stderr = (error.stderr or "") if isinstance(error.stderr, str) else ""
        stderr = f"{stderr}\ncommand timed out after {timeout_seconds}s".strip()
    except OSError as error:
        exit_code = None
        stderr = f"command failed to start: {error}"

    duration_ms = round((time.monotonic() - started) * 1_000)
    passed = exit_code == 0 and not timed_out
    if expect_stdout_empty and stdout.strip():
        passed = False
        stderr = (
            f"{stderr}\nexpected empty stdout but observed tracked/untracked changes"
        ).strip()

    classification = "passed"
    if not passed:
        classification = "upstream_failure" if focused_test else "baseline_failure"

    return {
        "id": command_id,
        "lane": lane,
        "argv": list(argv),
        "cwd": str(working_directory.relative_to(root)),
        "timeout_seconds": timeout_seconds,
        "started_at": started_at,
        "completed_at": utc_now(),
        "duration_ms": duration_ms,
        "exit_code": exit_code,
        "timed_out": timed_out,
        "passed": passed,
        "classification": classification,
        "required_to_build": required_to_build,
        "focused_test": focused_test,
        "stdout_bytes": len(stdout.encode("utf-8")),
        "stderr_bytes": len(stderr.encode("utf-8")),
        "stdout_sha256": sha256_bytes(stdout.encode("utf-8")),
        "stderr_sha256": sha256_bytes(stderr.encode("utf-8")),
        "stdout_tail": text_tail(stdout),
        "stderr_tail": text_tail(stderr),
        "failure_summary": None if passed else failure_summary(stdout, stderr),
        "failure_fingerprint": None
        if passed
        else command_fingerprint(argv, exit_code, timed_out, stdout, stderr),
    }


def git(root: Path, *arguments: str) -> str:
    result = subprocess.run(
        ["git", "-C", str(root), *arguments],
        check=False,
        capture_output=True,
        text=True,
        timeout=60,
    )
    if result.returncode != 0:
        detail = result.stderr.strip() or result.stdout.strip() or "no diagnostic"
        fail(f"git {' '.join(arguments)} failed: {detail}")
    return result.stdout.strip()


def load_floor(root: Path) -> str:
    document = json.loads((root / PROVENANCE).read_text(encoding="utf-8"))
    floor = document.get("pinned_floor", {}).get("sha")
    if not isinstance(floor, str) or len(floor) != 40:
        fail("upstream provenance does not contain a valid pinned floor")
    return floor


def runtime_changes(root: Path, floor: str, head: str) -> tuple[list[str], list[str]]:
    changed = [
        line
        for line in git(root, "diff", "--name-only", f"{floor}..{head}").splitlines()
        if line
    ]
    runtime = [
        path
        for path in changed
        if any(path == prefix or path.startswith(prefix) for prefix in RUNTIME_PREFIXES)
    ]
    return changed, runtime


def os_release() -> dict[str, str]:
    path = Path("/etc/os-release")
    if not path.is_file():
        return {}
    values: dict[str, str] = {}
    for line in path.read_text(encoding="utf-8").splitlines():
        if "=" not in line:
            continue
        key, value = line.split("=", 1)
        values[key] = value.strip().strip('"')
    return values


def atomic_json_write(path: Path, document: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = (json.dumps(document, indent=2, sort_keys=True) + "\n").encode("utf-8")
    fd, temporary = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
    try:
        with os.fdopen(fd, "wb") as handle:
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
    finally:
        try:
            os.unlink(temporary)
        except FileNotFoundError:
            pass


def capture(root: Path, output: Path) -> dict[str, Any]:
    root = root.resolve()
    output = output if output.is_absolute() else root / output
    floor = load_floor(root)
    head = git(root, "rev-parse", "HEAD")
    branch = git(root, "symbolic-ref", "--quiet", "--short", "HEAD")
    changed_paths, runtime_paths = runtime_changes(root, floor, head)

    commands: list[dict[str, Any]] = []

    def execute(**kwargs: Any) -> None:
        receipt = run_command(root, **kwargs)
        print(
            f"baseline: {receipt['id']}: "
            f"{'PASS' if receipt['passed'] else 'FAIL'} "
            f"({receipt['duration_ms']} ms)",
            flush=True,
        )
        commands.append(receipt)

    execute(
        command_id="initial_git_status",
        lane="precondition",
        argv=["git", "status", "--porcelain=v1", "--untracked-files=all"],
        timeout_seconds=60,
        required_to_build=True,
        expect_stdout_empty=True,
    )
    execute(
        command_id="floor_ancestry",
        lane="precondition",
        argv=["git", "merge-base", "--is-ancestor", floor, head],
        timeout_seconds=60,
        required_to_build=True,
    )

    for command_id, argv in (
        ("rustc_version", ["rustc", "--version", "--verbose"]),
        ("cargo_version", ["cargo", "--version", "--verbose"]),
        ("nextest_version", ["cargo", "nextest", "--version"]),
        ("node_version", ["node", "--version"]),
        ("npm_version", ["npm", "--version"]),
        ("python_version", ["python3", "--version"]),
        ("git_version", ["git", "--version"]),
        ("ast_grep_version", ["ast-grep", "--version"]),
    ):
        execute(
            command_id=command_id,
            lane="toolchain",
            argv=argv,
            timeout_seconds=60,
            required_to_build=True,
        )

    execute(
        command_id="cargo_metadata",
        lane="precondition",
        argv=["cargo", "metadata", "--locked", "--format-version", "1", "--no-deps"],
        timeout_seconds=300,
        required_to_build=True,
    )
    execute(
        command_id="cargo_clean",
        lane="clean-build",
        argv=["cargo", "clean"],
        timeout_seconds=600,
        required_to_build=True,
    )
    execute(
        command_id="rust_cli_build",
        lane="clean-build",
        argv=[
            "cargo",
            "build",
            "-p",
            "tracedecay-cli",
            "--bin",
            "tracedecay",
            "--locked",
        ],
        timeout_seconds=3_600,
        required_to_build=True,
    )

    common_nextest = ["--profile", "ci", "--locked", "--features", "test-helpers"]
    execute(
        command_id="memory_tests",
        lane="memory",
        argv=[
            "cargo",
            "nextest",
            "run",
            "-p",
            "tracedecay",
            "--test",
            "memory_suite",
            *common_nextest,
        ],
        timeout_seconds=2_400,
        focused_test=True,
    )
    execute(
        command_id="retrieval_tests",
        lane="retrieval",
        argv=[
            "cargo",
            "nextest",
            "run",
            "-p",
            "tracedecay",
            "--test",
            "search_quality_suite",
            *common_nextest,
        ],
        timeout_seconds=2_400,
        focused_test=True,
    )
    execute(
        command_id="host_tests",
        lane="host",
        argv=[
            "cargo",
            "nextest",
            "run",
            "-p",
            "tracedecay",
            "--test",
            "agent_suite",
            *common_nextest,
        ],
        timeout_seconds=2_400,
        focused_test=True,
    )
    execute(
        command_id="daemon_tests",
        lane="daemon",
        argv=[
            "cargo",
            "nextest",
            "run",
            "-p",
            "tracedecay",
            "--test",
            "daemon_runtime_acceptance",
            *common_nextest,
        ],
        timeout_seconds=2_400,
        focused_test=True,
    )

    execute(
        command_id="dashboard_install",
        lane="dashboard",
        argv=["npm", "ci"],
        cwd=Path("dashboard"),
        timeout_seconds=900,
        required_to_build=True,
    )
    execute(
        command_id="dashboard_contracts",
        lane="dashboard",
        argv=["npm", "run", "contracts:check"],
        cwd=Path("dashboard"),
        timeout_seconds=600,
        required_to_build=True,
    )
    execute(
        command_id="dashboard_build",
        lane="dashboard",
        argv=["npm", "run", "build"],
        cwd=Path("dashboard"),
        timeout_seconds=900,
        required_to_build=True,
    )
    execute(
        command_id="dashboard_bundle",
        lane="dashboard",
        argv=["python3", "scripts/check-dashboard-bundle.py", "dashboard/app-dist"],
        timeout_seconds=300,
        required_to_build=True,
    )
    execute(
        command_id="dashboard_unit_tests",
        lane="dashboard",
        argv=["npm", "test"],
        cwd=Path("dashboard"),
        timeout_seconds=900,
        focused_test=True,
    )
    execute(
        command_id="dashboard_api_tests",
        lane="dashboard-api",
        argv=[
            "cargo",
            "nextest",
            "run",
            "-p",
            "tracedecay",
            "--test",
            "dashboard_api_test",
            "--profile",
            "ci",
            "--locked",
            "--features",
            "test-transport",
        ],
        timeout_seconds=2_400,
        focused_test=True,
    )

    by_id = {command["id"]: command for command in commands}
    build_failures = [
        command_id
        for command_id in SETUP_IDS | BUILD_IDS
        if command_id not in by_id or not by_id[command_id]["passed"]
    ]
    focused_failures = [
        command_id
        for command_id in FOCUS_IDS
        if command_id not in by_id or not by_id[command_id]["passed"]
    ]
    missing_focus = sorted(FOCUS_IDS - set(by_id))
    closure_eligible = not build_failures and not runtime_paths and not missing_focus
    overall_status = (
        "failed"
        if not closure_eligible
        else "degraded"
        if focused_failures
        else "passed"
    )

    receipt: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "issue_id": ISSUE_ID,
        "captured_at": utc_now(),
        "platform": {
            "system": platform.system(),
            "release": platform.release(),
            "machine": platform.machine(),
            "python_platform": platform.platform(),
            "os_release": os_release(),
        },
        "repository": {
            "branch": branch,
            "head_sha": head,
            "pinned_floor_sha": floor,
            "changed_paths_since_floor": changed_paths,
            "runtime_paths_changed_since_floor": runtime_paths,
            "initial_checkout_clean": by_id["initial_git_status"]["passed"],
        },
        "policy": {
            "builds_must_pass": sorted(BUILD_IDS),
            "setup_must_pass": sorted(SETUP_IDS),
            "focused_lanes_may_be_degraded_only_with_receipts": sorted(FOCUS_IDS),
        },
        "commands": commands,
        "summary": {
            "overall_status": overall_status,
            "closure_eligible": closure_eligible,
            "build_failures": sorted(build_failures),
            "focused_failures": sorted(focused_failures),
            "missing_focus_lanes": missing_focus,
            "commands_total": len(commands),
            "commands_passed": sum(bool(command["passed"]) for command in commands),
            "commands_failed": sum(not bool(command["passed"]) for command in commands),
            "duration_ms": sum(int(command["duration_ms"]) for command in commands),
        },
    }
    atomic_json_write(output, receipt)
    print(json.dumps(receipt["summary"], indent=2, sort_keys=True))
    return receipt


def main() -> None:
    root = Path(__file__).resolve().parents[2]
    parser = argparse.ArgumentParser()
    parser.add_argument("--repo", type=Path, default=root)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    args = parser.parse_args()
    try:
        capture(args.repo, args.output)
    except (BaselineError, OSError, json.JSONDecodeError) as error:
        print(f"capture-v2-baseline: {error}", file=sys.stderr)
        raise SystemExit(1) from error


if __name__ == "__main__":
    main()
