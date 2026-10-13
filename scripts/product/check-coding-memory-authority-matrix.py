#!/usr/bin/env python3
"""Stable entrypoint for the coding-memory authority-matrix validator."""

from __future__ import annotations

import runpy
from pathlib import Path


# This entrypoint executes the core checker UNMODIFIED. It used to overwrite
# SOURCE_MARKERS at runpy time to work around the multi-line rustdoc
# assertion in configuration/runtime.rs; the core now matches that assertion
# properly (see `DocAssertion` there), so there is exactly one authoritative
# marker definition and no runtime patching. Do not reintroduce an override
# here: a wrapper-only patch lets the core drift into a state where the gate
# passes through this file but fails when run directly.
core = Path(__file__).with_name("check-coding-memory-authority-matrix-core.py")
namespace = runpy.run_path(str(core))

raise SystemExit(namespace["main"]())
