#!/usr/bin/env python3
"""Run and independently verify the semantic ablation evidence matrix.

The build is always admitted through cargo-hauler so an evaluation cannot
silently bypass the shared V2 build/session policy.
"""

from __future__ import annotations

import argparse
import re
import subprocess
import sys
import time
from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]
DEFAULT_OUTPUT = ROOT / "product/evaluation/search-quality/semantic-ablation-report-v1.json"
VERIFY = Path(__file__).with_name("verify-semantic-ablation.py")


def run_hauler(session: str, output: Path, timeout: int) -> int:
    output.parent.mkdir(parents=True, exist_ok=True)
    status = subprocess.run(
        ["hauler", "status", "--session", session],
        cwd=ROOT,
        text=True,
        capture_output=True,
        check=False,
    )
    sys.stdout.write(status.stdout)
    sys.stderr.write(status.stderr)
    if status.returncode != 0:
        print(f"cargo-hauler session {session!r} is not available", file=sys.stderr)
        return status.returncode
    command = [
        "hauler",
        "exec",
        "--session",
        session,
        "--",
        "cargo",
        "run",
        "--locked",
        "-p",
        "tracedecay-search-eval",
        "--bin",
        "tracedecay-search-eval",
        "--",
        "semantic-ablation",
        "--output",
        str(output),
    ]
    try:
        result = subprocess.run(
            command,
            cwd=ROOT,
            text=True,
            capture_output=True,
            check=False,
            timeout=timeout,
        )
    except subprocess.TimeoutExpired as error:
        print(f"semantic-ablation hauler run timed out: {error}", file=sys.stderr)
        return 124
    sys.stdout.write(result.stdout)
    sys.stderr.write(result.stderr)
    if result.returncode == 75:
        ticket = re.search(r"ticket\s+(cc-\d+)", result.stdout + result.stderr)
        if ticket is None:
            print("hauler queued the run but did not return a ticket", file=sys.stderr)
            return result.returncode
        deadline = time.monotonic() + timeout
        while True:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                print(f"semantic-ablation hauler await timed out: {ticket.group(1)}", file=sys.stderr)
                return 124
            try:
                awaited = subprocess.run(
                    [
                        "hauler",
                        "await",
                        "--max-wait-ms",
                        str(min(55_000, max(1_000, int(remaining * 1_000)))),
                        ticket.group(1),
                    ],
                    cwd=ROOT,
                    text=True,
                    capture_output=True,
                    check=False,
                    timeout=min(timeout, max(1, int(remaining))),
                )
            except subprocess.TimeoutExpired as error:
                print(f"semantic-ablation hauler await timed out: {error}", file=sys.stderr)
                return 124
            sys.stdout.write(awaited.stdout)
            sys.stderr.write(awaited.stderr)
            rendered = awaited.stdout + awaited.stderr
            still_pending = re.search(
                r"(?:still pending|queued|wait expired|call hauler await again)",
                rendered,
                flags=re.IGNORECASE,
            )
            if still_pending:
                continue
            return awaited.returncode
    return result.returncode


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--session", default="v2-replacement")
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--timeout", type=int, default=1800)
    args = parser.parse_args(argv)
    output = args.output if args.output.is_absolute() else ROOT / args.output
    status = run_hauler(args.session, output, args.timeout)
    if status != 0:
        print(f"semantic-ablation Cargo run failed with exit {status}", file=sys.stderr)
        return status
    if not output.is_file():
        print(f"semantic-ablation did not write {output}", file=sys.stderr)
        return 1
    verified = subprocess.run(
        [sys.executable, str(VERIFY), "--report", str(output)],
        cwd=ROOT,
        check=False,
    )
    return verified.returncode


if __name__ == "__main__":
    raise SystemExit(main())
