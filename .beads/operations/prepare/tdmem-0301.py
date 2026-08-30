#!/usr/bin/env python3
"""Retry tdmem-0301 with canonical formatting and a generated lock entry."""

from __future__ import annotations

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

api_test = ROOT / "crates/tracedecay-memory-provider-api/tests/api.rs"
test_text = api_test.read_text(encoding="utf-8")
old = '''    assert_eq!(
        ProviderCall::new(missing_capability),
        Err(ApiError::MissingOperationCapability("recall.query.v1"))
    );
'''
new = '''    assert!(matches!(
        ProviderCall::new(missing_capability),
        Err(ApiError::MissingOperationCapability("recall.query.v1"))
    ));
'''
if old not in test_text:
    raise SystemExit("provider API assertion patch marker is missing")
api_test.write_text(test_text.replace(old, new, 1), encoding="utf-8")

subprocess.run(
    ["cargo", "metadata", "--format-version", "1"],
    cwd=ROOT,
    check=True,
    stdout=subprocess.DEVNULL,
)
subprocess.run(
    ["cargo", "fmt", "--package", "tracedecay-memory-provider-api"],
    cwd=ROOT,
    check=True,
)

Path(__file__).unlink()
