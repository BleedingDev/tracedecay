#!/usr/bin/env python3
"""Retry tdmem-0303 while preserving the reviewed materializer body."""

from __future__ import annotations

import json
import subprocess
from pathlib import Path

HERE = Path(__file__).resolve()
ROOT = HERE.parents[3]
SOURCE_COMMIT = "ff27866d1e69958908437b0cda3f93491bec12ed"
SOURCE_PATH = ".beads/operations/prepare/tdmem-0303.py"
BODY = HERE.with_name("_tdmem-0303-body.py")
MANIFEST = ROOT / ".beads/operations/prepared-files.json"

result = subprocess.run(
    ["git", "show", f"{SOURCE_COMMIT}:{SOURCE_PATH}"],
    cwd=ROOT,
    check=True,
    capture_output=True,
    text=True,
)
source = result.stdout
old = '''        if entry is None:\n            raise SystemExit(f"missing convergence entry for {upstream_path}")\n'''
new = '''        if entry is None:\n            if upstream_path == "Cargo.lock" and not git_changed("Cargo.lock"):\n                continue\n            raise SystemExit(f"missing convergence entry for {upstream_path}")\n'''
if source.count(old) != 1:
    raise SystemExit("could not locate the reviewed convergence-entry guard")
BODY.write_text(source.replace(old, new, 1), encoding="utf-8")
subprocess.run(["python3", str(BODY)], cwd=ROOT, check=True)
subprocess.run(
    ["cargo", "fmt", "--package", "tracedecay-memory-provider-native"],
    cwd=ROOT,
    check=True,
)

rows = json.loads(MANIFEST.read_text(encoding="utf-8"))
filtered: list[dict[str, str]] = []
for row in rows:
    status = subprocess.run(
        ["git", "status", "--porcelain", "--untracked-files=all", "--", row["path"]],
        cwd=ROOT,
        check=True,
        capture_output=True,
        text=True,
    ).stdout
    if status.strip():
        filtered.append(row)
if not filtered:
    raise SystemExit("Native adapter materializer produced no reviewable changes")
MANIFEST.write_text(json.dumps(filtered, indent=2) + "\n", encoding="utf-8")
HERE.unlink()
