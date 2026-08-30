#!/usr/bin/env python3
"""Materialize tdmem-0304 with strict, documented integration tests."""

from __future__ import annotations

import subprocess
from pathlib import Path

HERE = Path(__file__).resolve()
ROOT = HERE.parents[3]
SOURCE_COMMIT = "c3d9e1e6fd3d807828a4b373be14b6f0da3db848"
SOURCE_PATH = ".beads/operations/prepare/tdmem-0304.py"
BODY = HERE.with_name("_tdmem-0304-body.py")

result = subprocess.run(
    ["git", "show", f"{SOURCE_COMMIT}:{SOURCE_PATH}"],
    cwd=ROOT,
    check=True,
    capture_output=True,
    text=True,
)
source = result.stdout
undocumented_tests = "TESTS = r'''use std::collections::BTreeSet;\n"
documented_tests = (
    "TESTS = r'''//! Focused integration tests for the topology-neutral NCM adapter boundary.\n"
    "#![allow(clippy::expect_used)]\n\n"
    "use std::collections::BTreeSet;\n"
)
if source.count(undocumented_tests) != 1:
    raise SystemExit("could not locate the undocumented NCM integration test crate")
source = source.replace(undocumented_tests, documented_tests, 1)
BODY.write_text(source, encoding="utf-8")
subprocess.run(["python3", str(BODY)], cwd=ROOT, check=True)
HERE.unlink()
