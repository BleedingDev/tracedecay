#!/usr/bin/env python3
"""Materialize deterministic M1 contract goldens before tdmem-0207 checks."""

from __future__ import annotations

import json
import subprocess
from pathlib import Path

REPO = Path(__file__).resolve().parents[3]

subprocess.run(
    [
        "python3",
        "scripts/product/generate-memory-provider-goldens.py",
        "--repo",
        ".",
        "--write",
    ],
    cwd=REPO,
    check=True,
)

manifest = [
    {
        "path": "product/contracts/memory-provider-v1/goldens",
        "message": "test(contract): materialize canonical M1 golden fixtures (tdmem-0207)",
    }
]
marker_path = REPO / ".beads/operations/prepared-files.json"
marker_path.write_text(
    json.dumps(manifest, indent=2, sort_keys=True) + "\n",
    encoding="utf-8",
)
