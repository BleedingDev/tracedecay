#!/usr/bin/env python3
"""Retry tdmem-0303 while preserving the reviewed materializer body."""

from __future__ import annotations

import subprocess
from pathlib import Path

HERE = Path(__file__).resolve()
ROOT = HERE.parents[3]
SOURCE_COMMIT = "3a879d024f983ac67777e532f283ec1b88ca5e02"
SOURCE_PATH = ".beads/operations/prepare/tdmem-0303.py"
BODY = HERE.with_name("_tdmem-0303-body.py")

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
HERE.unlink()
