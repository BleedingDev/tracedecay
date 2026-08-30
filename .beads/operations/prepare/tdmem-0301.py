#!/usr/bin/env python3
"""Retry tdmem-0301 without treating an unchanged Cargo.lock as prepared output."""

from __future__ import annotations

import json
import subprocess
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
BODY = Path(__file__).with_name("_tdmem-0301-body.py")
SOURCE_COMMIT = "fab219ad8b29956d21489779a111f00902b60032"
SOURCE_PATH = ".beads/operations/prepare/tdmem-0301.py"

source = subprocess.check_output(
    ["git", "show", f"{SOURCE_COMMIT}:{SOURCE_PATH}"],
    cwd=ROOT,
    text=True,
)
BODY.write_text(source, encoding="utf-8")
subprocess.run(["python3", str(BODY)], cwd=ROOT, check=True)

manifest_path = ROOT / ".beads/operations/prepared-files.json"
manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
manifest = [row for row in manifest if row.get("path") != "Cargo.lock"]
manifest_path.write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")

map_path = ROOT / "product/upstream/convergence-map.json"
document = json.loads(map_path.read_text(encoding="utf-8"))
document["entries"] = [
    row for row in document["entries"] if row.get("path") != "Cargo.lock"
]
document["snapshot"]["upstream_existing_production_files"] = 1
document["snapshot"]["observed_state"] = (
    "The product branch changes only additive root workspace membership; "
    "Cargo.lock is unchanged because the dependency-free provider API adds no resolved package edge."
)
map_path.write_text(json.dumps(document, indent=2, sort_keys=True) + "\n", encoding="utf-8")

Path(__file__).unlink()
