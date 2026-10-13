#!/usr/bin/env python3
"""Run the Beads operation engine against the current two-hash materializer."""

from __future__ import annotations

import base64
import bz2
import importlib.util
from pathlib import Path


ENGINE_PATH = Path(__file__).resolve().with_name("apply_operation.py")
spec = importlib.util.spec_from_file_location("beads_apply_operation", ENGINE_PATH)
if spec is None or spec.loader is None:
    raise SystemExit(f"cannot load operation engine from {ENGINE_PATH}")
engine = importlib.util.module_from_spec(spec)
spec.loader.exec_module(engine)


def pack_plan() -> dict[str, object]:
    raw = engine.JSONL_PATH.read_bytes()
    issues, digest = engine.validate_jsonl(raw)
    encoded = base64.b64encode(bz2.compress(raw, compresslevel=9)).decode("ascii")
    parts = [
        encoded[index : index + engine.PART_SIZE]
        for index in range(0, len(encoded), engine.PART_SIZE)
    ]
    if not parts:
        engine.fail("refusing to pack an empty JSONL payload")

    engine.PLAN_DIR.mkdir(parents=True, exist_ok=True)
    for existing in engine.PLAN_DIR.glob(engine.PART_GLOB):
        existing.unlink()
    for index, part in enumerate(parts):
        (engine.PLAN_DIR / f"{engine.PART_PREFIX}{index:02d}").write_text(
            part + "\n", encoding="ascii"
        )

    engine.replace_exact(
        r"^EXPECTED_PARTS = \d+$",
        f"EXPECTED_PARTS = {len(parts)}",
        engine.MATERIALIZER,
        "EXPECTED_PARTS",
    )
    for constant in ("EXPECTED_SOURCE_SHA256", "EXPECTED_OUTPUT_SHA256"):
        engine.replace_exact(
            rf'^{constant} = "[0-9a-f]{{64}}"$',
            f'{constant} = "{digest}"',
            engine.MATERIALIZER,
            constant,
        )
    engine.run(
        "verify packed plan",
        ["python3", str(engine.MATERIALIZER.relative_to(engine.ROOT))],
    )
    return {
        "issues": len(issues),
        "jsonl_sha256": digest,
        "encoded_bytes": len(encoded),
        "parts": len(parts),
        "part_size": engine.PART_SIZE,
        "materializer_contract": "source-and-output-sha256",
    }


engine.pack_plan = pack_plan
engine.main()
