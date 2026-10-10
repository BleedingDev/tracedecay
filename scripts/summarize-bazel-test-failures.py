#!/usr/bin/env python3
"""Print failed Bazel rustc names and nearby panic text.

rustc writes the trailing ``failures:`` section only when the process
exits. A TIMEOUT kills the suite first, and the uploaded ``test.log`` then
stops mid-run. This walks ``bazel-testlogs`` and prints every FAILED /
TIMEOUT target plus assertion fragments that already flushed, so a
truncated artifact still names the red tests.
"""

from __future__ import annotations

import pathlib
import re
import sys
import xml.etree.ElementTree as ET

FAILED_LINE = re.compile(r"^(test |.*::).*(FAILED|TIMEOUT|panicked at|assertion)", re.I)
LOG_TAIL_LINES = 80


def repo_root() -> pathlib.Path:
    return pathlib.Path(__file__).resolve().parents[1]


def testlogs_root() -> pathlib.Path:
    root = repo_root() / "bazel-testlogs"
    if root.is_dir():
        return root
    return pathlib.Path("bazel-testlogs")


def xml_status(path: pathlib.Path) -> str | None:
    try:
        tree = ET.parse(path)
    except ET.ParseError:
        return None
    failures = 0
    errors = 0
    tests = 0
    for suite in tree.iter("testsuite"):
        tests += int(suite.attrib.get("tests") or 0)
        failures += int(suite.attrib.get("failures") or 0)
        errors += int(suite.attrib.get("errors") or 0)
    if failures or errors:
        return f"xml tests={tests} failures={failures} errors={errors}"
    return None


def summarize_log(path: pathlib.Path) -> list[str]:
    try:
        text = path.read_text(encoding="utf-8", errors="replace")
    except OSError as error:
        return [f"  could not read {path}: {error}"]
    lines = [line.rstrip("\n") for line in text.splitlines()]
    hits = [line for line in lines if FAILED_LINE.search(line)]
    out = [f"  {line}" for line in hits[-40:]]
    if not hits:
        tail = lines[-LOG_TAIL_LINES:]
        out.append(f"  (no FAILED/panic line; last {len(tail)} lines)")
        out.extend(f"  {line}" for line in tail)
    return out


def main() -> int:
    root = testlogs_root()
    if not root.is_dir():
        print(f"no bazel-testlogs directory at {root}")
        return 0
    logs = sorted(root.rglob("test.log"))
    if not logs:
        print(f"no test.log files under {root}")
        return 0
    printed = 0
    for log in logs:
        xml = log.with_name("test.xml")
        status = xml_status(xml) if xml.is_file() else None
        body = summarize_log(log)
        interesting = status is not None or any(
            "FAILED" in line or "TIMEOUT" in line or "panicked" in line or "assertion" in line
            for line in body
        )
        if not interesting and log.stat().st_size == 0:
            continue
        if not interesting:
            continue
        printed += 1
        relative = log.relative_to(root)
        print(f"=== {relative} ===")
        if status:
            print(f"  {status}")
        print("\n".join(body))
        print()
    if printed == 0:
        print(f"no failed test.log summaries under {root}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
