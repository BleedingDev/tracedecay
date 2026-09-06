#!/usr/bin/env python3
"""Matched pre-embedded read benchmark against pinned Biomem Python source."""

from __future__ import annotations

import argparse
import importlib.util
import json
import math
import sys
import time
from pathlib import Path
from typing import Any

REFERENCE_COMMIT = "500847ff65b5d9548b3826fa29bf3ccf8d221147"
DEFAULT_ROOT = Path.home() / "workspace/bleedingdev/projects/biomem/code/biomem"


def percentile(values: list[int], percent: int) -> float:
    values.sort()
    rank = max(0, min(len(values) - 1, math.ceil(percent * len(values) / 100) - 1))
    return values[rank] / 1000.0


def blocked(detail: str) -> dict[str, Any]:
    return {
        "schema_version": 1,
        "population": "python_reference_kernel",
        "reference_commit": REFERENCE_COMMIT,
        "data": {"status": "blocked_environment", "detail": detail},
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--biomem-root", type=Path, default=DEFAULT_ROOT)
    parser.add_argument("--ops", type=int, default=10_000)
    args = parser.parse_args()
    module_path = args.biomem_root / "src/memory_module/memory_centers.py"
    if not module_path.is_file():
        print(json.dumps(blocked(f"missing pinned source: {module_path}"), indent=2))
        return 0
    try:
        import torch
    except ModuleNotFoundError as error:
        print(json.dumps(blocked(f"PyTorch unavailable: {error}"), indent=2))
        return 0
    spec = importlib.util.spec_from_file_location("biomem_reference_memory_centers", module_path)
    if spec is None or spec.loader is None:
        print(json.dumps(blocked("cannot load memory_centers.py"), indent=2))
        return 0
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    torch.manual_seed(0x5EED020)
    populations = []
    for name, active in (("empty", 0), ("sparse", 512), ("full", 4096)):
        centers = module.MemoryCenters(
            n_centers=4096,
            d_key=64,
            d_value=128,
            sigma_read=0.5,
            sigma_write=0.15,
            use_hybrid_metric=True,
            minkowski_p=0.5,
            weight_cosine=0.7,
            weight_minkowski=0.3,
            hybrid_candidates=64,
            device="cpu",
        )
        centers.active[:active] = True
        centers.h[:active] = 1.0
        query = torch.randn(1, 1, 64)
        query = torch.nn.functional.normalize(query, dim=-1)
        for _ in range(10):
            centers.read(query, top_k=16, increment_stats=False)
        timings = []
        started = time.perf_counter_ns()
        for _ in range(args.ops):
            one = time.perf_counter_ns()
            centers.read(query, top_k=16, increment_stats=False)
            timings.append(time.perf_counter_ns() - one)
        elapsed = time.perf_counter_ns() - started
        populations.append({
            "population": name,
            "active_ltm_centers": active,
            "measurement": {
                "name": "biomem_memory_centers_read",
                "distribution": {
                    "samples": args.ops,
                    "p50_us": percentile(timings.copy(), 50),
                    "p95_us": percentile(timings.copy(), 95),
                    "p99_us": percentile(timings.copy(), 99),
                    "elapsed_ms": elapsed / 1_000_000.0,
                },
                "semantics": "Biomem MemoryCenters.read, same 64-D pre-embedded LTM capacity and top_k=16, increment_stats=False",
            },
        })
    print(json.dumps({
        "schema_version": 1,
        "population": "python_reference_kernel",
        "reference_commit": REFERENCE_COMMIT,
        "data": {"status": "measured", "populations": populations},
    }, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
