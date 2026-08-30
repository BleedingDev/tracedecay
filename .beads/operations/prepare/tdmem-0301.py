#!/usr/bin/env python3
"""Retry tdmem-0301 and derive convergence counters from the actual floor diff."""

from __future__ import annotations

import json
import subprocess
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
BODY = Path(__file__).with_name("_tdmem-0301-retry-body.py")
SOURCE_COMMIT = "887b6a8d945468acc139aa1375d3d6508c405b5b"
SOURCE_PATH = ".beads/operations/prepare/tdmem-0301.py"
FLOOR = "08fbe33a7c7f403191fd5d6e356c7b6681b96403"

source = subprocess.check_output(
    ["git", "show", f"{SOURCE_COMMIT}:{SOURCE_PATH}"],
    cwd=ROOT,
    text=True,
)
BODY.write_text(source, encoding="utf-8")
subprocess.run(["python3", str(BODY)], cwd=ROOT, check=True)

result = subprocess.run(
    [
        "git",
        "diff",
        "--no-renames",
        "--numstat",
        FLOOR,
        "--",
        "Cargo.toml",
        "Cargo.lock",
    ],
    cwd=ROOT,
    check=True,
    capture_output=True,
    text=True,
)
files = 0
changed_lines = 0
for raw in result.stdout.splitlines():
    if not raw.strip():
        continue
    added, deleted, _path = raw.split("\t", 2)
    files += 1
    changed_lines += int(added) + int(deleted)

map_path = ROOT / "product/upstream/convergence-map.json"
document = json.loads(map_path.read_text(encoding="utf-8"))
document["snapshot"] = {
    "upstream_existing_production_files": files,
    "upstream_existing_test_or_fixture_files": 0,
    "total_upstream_changed_lines": changed_lines,
    "composition_root_files": 0,
    "exception_zone_files": 0,
    "observed_state": (
        "The product branch changes only additive root workspace membership and its "
        "generated path-package lock entry; provider implementation remains additive."
    ),
}
map_path.write_text(
    json.dumps(document, indent=2, sort_keys=True) + "\n",
    encoding="utf-8",
)

Path(__file__).unlink()
