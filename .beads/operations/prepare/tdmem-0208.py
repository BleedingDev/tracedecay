#!/usr/bin/env python3
"""Materialize deterministic Rust bindings before tdmem-0208 evidence checks."""

from __future__ import annotations

import json
import subprocess
from pathlib import Path

REPO = Path(__file__).resolve().parents[3]

subprocess.run(
    [
        "python3",
        "scripts/product/generate-memory-provider-rust.py",
        "--repo",
        ".",
        "--write",
    ],
    cwd=REPO,
    check=True,
)

marker = {
    "schema_version": 1,
    "paths": ["product/contracts/memory-provider-v1/generated/rust"],
    "commit_message": "feat(contract): materialize generated Rust bindings (tdmem-0208)",
}
marker_path = REPO / ".beads/operations/prepared-files.json"
marker_path.write_text(
    json.dumps(marker, indent=2, sort_keys=True) + "\n",
    encoding="utf-8",
)
