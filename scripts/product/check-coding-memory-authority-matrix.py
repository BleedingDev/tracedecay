#!/usr/bin/env python3
"""Stable entrypoint for the coding-memory authority-matrix validator."""

from __future__ import annotations

import runpy
from pathlib import Path


core = Path(__file__).with_name("check-coding-memory-authority-matrix-core.py")
namespace = runpy.run_path(str(core))
namespace["SOURCE_MARKERS"][
    "crates/tracedecay-usecases/src/configuration/runtime.rs"
] = [
    "pub struct ProjectConfigurationRuntime",
    "transactional store handle",
    "retained store remains the sole",
    "runtime configuration authority",
]

raise SystemExit(namespace["main"]())
