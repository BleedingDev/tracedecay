#!/usr/bin/env python3
"""Materialize and verify the versioned Beads Rust backlog.

The authored plan payload is committed in chunked bzip2+base64 form. This
script expands it into the canonical ``.beads/issues.jsonl`` collaboration
surface, materializing the relation defaults persisted by Beads Rust 0.5.7.
"""

from __future__ import annotations

import base64
import bz2
import hashlib
import json
import os
from pathlib import Path
import tempfile
from typing import Any, NoReturn

BEADS_DIR = Path(__file__).resolve().parent
PLAN_DIR = BEADS_DIR / "plan"
OUTPUT = BEADS_DIR / "issues.jsonl"

EXPECTED_PARTS = 11
EXPECTED_ISSUES = 131
EXPECTED_SOURCE_SHA256 = "d366442412d68710465317bc361db2c864dcde1edefe321a560f610345927c0a"
EXPECTED_OUTPUT_SHA256 = "d366442412d68710465317bc361db2c864dcde1edefe321a560f610345927c0a"
EXPECTED_ROOT_ID = "tdmem-0000"


def fail(message: str) -> NoReturn:
    raise SystemExit(f"materialize-beads: {message}")


def load_payload() -> bytes:
    parts = sorted(PLAN_DIR.glob("issues.jsonl.bz2.b64.part*"))
    if len(parts) != EXPECTED_PARTS:
        fail(f"expected {EXPECTED_PARTS} payload parts, found {len(parts)}")

    expected_names = [
        f"issues.jsonl.bz2.b64.part{index:02d}" for index in range(EXPECTED_PARTS)
    ]
    names = [part.name for part in parts]
    if names != expected_names:
        fail(f"unexpected payload part set: {names!r}")

    encoded = "".join(part.read_text(encoding="ascii").strip() for part in parts)
    try:
        compressed = base64.b64decode(encoded, validate=True)
        raw = bz2.decompress(compressed)
    except (ValueError, OSError) as error:
        fail(f"payload decode failed: {error}")

    digest = hashlib.sha256(raw).hexdigest()
    if digest != EXPECTED_SOURCE_SHA256:
        fail(
            "source payload SHA-256 mismatch: "
            f"expected {EXPECTED_SOURCE_SHA256}, got {digest}"
        )
    return raw


def parse_issues(raw: bytes) -> list[dict[str, Any]]:
    try:
        text = raw.decode("utf-8")
    except UnicodeDecodeError as error:
        fail(f"payload is not UTF-8: {error}")

    lines = [line for line in text.splitlines() if line.strip()]
    if len(lines) != EXPECTED_ISSUES:
        fail(f"expected {EXPECTED_ISSUES} issues, found {len(lines)}")

    issues: list[dict[str, Any]] = []
    for line_number, line in enumerate(lines, start=1):
        try:
            issue = json.loads(line)
        except json.JSONDecodeError as error:
            fail(f"invalid JSON on line {line_number}: {error}")
        if not isinstance(issue, dict):
            fail(f"line {line_number} is not a JSON object")
        issues.append(issue)
    return issues


def materialize_beads_defaults(issues: list[dict[str, Any]]) -> None:
    """Apply defaults that Beads Rust persists for imported relationships.

    Beads Rust 0.5.7 verifies an import by comparing the rehydrated issue with
    the normalized JSONL payload. Relationship rows persist explicit defaults
    for ``created_by``, ``metadata``, and ``thread_id``. Writing those defaults
    here makes the versioned JSONL round-trip exactly through ``br sync``.
    """

    for issue in issues:
        dependencies = issue.get("dependencies", [])
        if not isinstance(dependencies, list):
            fail(f"{issue.get('id', '<unknown>')}: dependencies must be a list")
        for dependency in dependencies:
            if not isinstance(dependency, dict):
                fail(f"{issue.get('id', '<unknown>')}: dependency must be an object")
            dependency.setdefault("created_by", "import")
            dependency.setdefault("metadata", "{}")
            dependency.setdefault("thread_id", "")


def serialize_issues(issues: list[dict[str, Any]]) -> bytes:
    text = "".join(
        json.dumps(issue, ensure_ascii=False, separators=(",", ":")) + "\n"
        for issue in issues
    )
    raw = text.encode("utf-8")
    digest = hashlib.sha256(raw).hexdigest()
    if digest != EXPECTED_OUTPUT_SHA256:
        fail(
            "canonical output SHA-256 mismatch: "
            f"expected {EXPECTED_OUTPUT_SHA256}, got {digest}"
        )
    return raw


def validate_graph(issues: list[dict[str, Any]]) -> None:
    ids = [issue.get("id") for issue in issues]
    if any(not isinstance(issue_id, str) or not issue_id for issue_id in ids):
        fail("every issue must have a non-empty string id")
    if len(set(ids)) != len(ids):
        fail("duplicate issue ids detected")
    if EXPECTED_ROOT_ID not in ids:
        fail(f"root epic {EXPECTED_ROOT_ID} is missing")

    known_ids = set(ids)
    adjacency: dict[str, list[str]] = {issue_id: [] for issue_id in ids}
    for issue in issues:
        issue_id = issue["id"]
        for dependency in issue.get("dependencies", []):
            source = dependency.get("issue_id")
            target = dependency.get("depends_on_id")
            if source != issue_id:
                fail(
                    f"{issue_id}: dependency source mismatch: "
                    f"expected {issue_id}, got {source!r}"
                )
            if target not in known_ids:
                fail(f"{issue_id}: unresolved dependency {target!r}")
            adjacency[issue_id].append(target)

    visiting: set[str] = set()
    visited: set[str] = set()

    def visit(node: str, path: list[str]) -> None:
        if node in visiting:
            cycle_start = path.index(node) if node in path else 0
            fail("dependency cycle: " + " -> ".join(path[cycle_start:] + [node]))
        if node in visited:
            return
        visiting.add(node)
        path.append(node)
        for dependency in adjacency[node]:
            visit(dependency, path)
        path.pop()
        visiting.remove(node)
        visited.add(node)

    for issue_id in ids:
        visit(issue_id, [])


def atomic_write(raw: bytes) -> None:
    OUTPUT.parent.mkdir(parents=True, exist_ok=True)
    fd, temp_name = tempfile.mkstemp(
        prefix=f".{OUTPUT.name}.", suffix=".tmp", dir=OUTPUT.parent
    )
    try:
        with os.fdopen(fd, "wb") as handle:
            handle.write(raw)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temp_name, OUTPUT)
    finally:
        try:
            os.unlink(temp_name)
        except FileNotFoundError:
            pass


def main() -> None:
    issues = parse_issues(load_payload())
    materialize_beads_defaults(issues)
    validate_graph(issues)
    raw = serialize_issues(issues)
    atomic_write(raw)

    epic_count = sum(issue.get("issue_type") == "epic" for issue in issues)
    deferred_count = sum(issue.get("status") == "deferred" for issue in issues)
    print(
        "materialize-beads: wrote "
        f"{OUTPUT} ({len(issues)} issues, {epic_count} epics, "
        f"{deferred_count} deferred, sha256={EXPECTED_OUTPUT_SHA256})"
    )


if __name__ == "__main__":
    main()
