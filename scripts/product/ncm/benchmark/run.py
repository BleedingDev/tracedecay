#!/usr/bin/env python3
"""Run bounded NCM benchmark populations and write one raw host result."""

from __future__ import annotations

import argparse
import json
import os
import platform
import re
import subprocess
import sys
import time
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[4]
CRATE = "tracedecay-memory-ncm-runtime"
RESULTS = ROOT / "product" / "ncm" / "performance"
WORKER = ROOT / "target" / "release" / "tracedecay-ncm-worker"


def build_bench(default_features: bool) -> Path:
    cmd = ["cargo", "bench", "-p", CRATE, "--bench", "ncm_scale", "--no-run", "--message-format=json"]
    if not default_features:
        cmd.append("--no-default-features")
    completed = subprocess.run(
        cmd,
        cwd=ROOT,
        env={**os.environ, "RUSTC_WRAPPER": ""},
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        check=False,
    )
    if completed.returncode != 0:
        sys.stderr.write(completed.stderr[-4000:])
        raise RuntimeError("failed to build ncm_scale benchmark")
    executable = None
    for line in completed.stdout.splitlines():
        try:
            message = json.loads(line)
        except json.JSONDecodeError:
            continue
        target = message.get("target", {})
        if target.get("name") == "ncm_scale" and message.get("executable"):
            executable = Path(message["executable"])
    if executable is None or not executable.is_file():
        raise RuntimeError("cargo did not report the ncm_scale executable")
    return executable


def command(executable: Path, population: str, *args: str) -> list[str]:
    return [str(executable), population, *args]


def timed_run(cmd: list[str], timeout: int = 590) -> dict[str, Any]:
    env = os.environ.copy()
    env["RUSTC_WRAPPER"] = ""
    wrapped = ["/usr/bin/time", "-l", *cmd] if Path("/usr/bin/time").exists() else cmd
    started = time.monotonic()
    completed = subprocess.run(
        wrapped,
        cwd=ROOT,
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        timeout=timeout,
        check=False,
    )
    elapsed = time.monotonic() - started
    if completed.returncode != 0:
        sys.stderr.write(completed.stderr[-4000:])
        raise RuntimeError(f"command exited {completed.returncode}: {' '.join(cmd)}")
    try:
        payload = json.loads(completed.stdout)
    except json.JSONDecodeError as error:
        raise RuntimeError(f"benchmark did not emit JSON: {completed.stdout[-1000:]}") from error
    peak = None
    match = re.search(r"^\s*(\d+)\s+maximum resident set size\s*$", completed.stderr, re.MULTILINE)
    if match:
        peak = int(match.group(1))
    return {
        "command": cmd,
        "wall_seconds": elapsed,
        "peak_rss_bytes": peak,
        "result": payload,
    }


def hardware() -> dict[str, Any]:
    def sysctl(name: str) -> str | None:
        result = subprocess.run(
            ["sysctl", "-n", name], capture_output=True, text=True, check=False
        )
        return result.stdout.strip() if result.returncode == 0 else None

    return {
        "hostname": platform.node(),
        "platform": platform.platform(),
        "machine": platform.machine(),
        "processor": platform.processor(),
        "cpu_brand": sysctl("machdep.cpu.brand_string"),
        "physical_memory_bytes": int(sysctl("hw.memsize") or 0) or None,
        "logical_cpus": os.cpu_count(),
        "python": platform.python_version(),
        "rustc": subprocess.run(
            ["rustc", "--version"], capture_output=True, text=True, check=True
        ).stdout.strip(),
    }


def git(*args: str) -> str:
    return subprocess.run(
        ["git", *args], cwd=ROOT, capture_output=True, text=True, check=True
    ).stdout.strip()


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--quick", action="store_true", help="use smoke-scale operation counts")
    parser.add_argument("--model-root", type=Path, help="state root containing models/")
    parser.add_argument("--biomem-root", type=Path, default=Path.home() / "workspace/bleedingdev/projects/biomem/code/biomem")
    parser.add_argument("--output", type=Path)
    parser.add_argument(
        "--only",
        action="append",
        choices=("self-test", "kernel", "reference", "store", "ipc", "engine", "mixed", "maintenance", "encoder"),
        help="run only the named population group; may be repeated",
    )
    parsed = parser.parse_args()
    selected = set(parsed.only or ())
    run_all = not selected
    def wants(name: str) -> bool:
        return run_all or name in selected
    ops = 100 if parsed.quick else 10_000
    kernel_ops = 1_000 if parsed.quick else 100_000
    maintenance_samples = 2 if parsed.quick else 10

    subprocess.run(
        [
            "cargo", "build", "-p", CRATE, "--bin", "tracedecay-ncm-worker",
            "--release", "--no-default-features",
        ],
        cwd=ROOT,
        env={**os.environ, "RUSTC_WRAPPER": ""},
        check=True,
    )

    bench_no_default = build_bench(default_features=False)
    bench_default = build_bench(default_features=True)

    runs: list[dict[str, Any]] = []
    if wants("self-test"):
        runs.append(timed_run(command(bench_no_default, "self-test", "--worker", str(WORKER))))
    if wants("kernel"):
        runs.append(timed_run(command(bench_no_default, "kernel", "--ops", str(kernel_ops), "--population", "all")))
    if wants("reference"):
        reference_script = ROOT / "scripts/product/ncm/benchmark/reference_kernel.py"
        runs.append(timed_run([
            sys.executable, str(reference_script),
            "--biomem-root", str(parsed.biomem_root.resolve()),
            "--ops", str(100 if parsed.quick else 10_000),
        ]))
    if wants("store"):
        for record_bytes in (64, 4096, 16384):
            runs.append(timed_run(command(bench_no_default, "store", "--ops", str(ops), "--record-bytes", str(record_bytes))))
    if wants("ipc"):
        runs.append(timed_run(command(bench_no_default, "ipc", "--ops", str(ops), "--worker", str(WORKER))))
    if wants("engine"):
        for namespaces in (1, 4):
            for record_bytes in (64, 4096, 16384):
                runs.append(timed_run(command(bench_no_default, "engine", "--ops", str(ops), "--namespaces", str(namespaces), "--record-bytes", str(record_bytes))))
    if wants("mixed"):
        for namespaces in (1, 4):
            runs.append(timed_run(command(bench_no_default, "mixed", "--ops", str(ops), "--namespaces", str(namespaces))))
    if wants("maintenance"):
        runs.append(timed_run(command(bench_no_default, "maintenance", "--seed-records", "128", "--samples", str(maintenance_samples))))
    if wants("encoder"):
        encoder_root = parsed.model_root.resolve() if parsed.model_root else ROOT
        encoder_args = [
            "--ops", "10" if parsed.quick else "100",
            "--model-root", str(encoder_root),
        ]
        runs.append(timed_run(command(bench_default, "encoder", *encoder_args)))

    blocked = [
        run for run in runs
        if run["result"].get("data", {}).get("status") == "blocked_environment"
    ]
    host_slug = re.sub(r"[^a-z0-9]+", "-", platform.node().lower()).strip("-") or "host"
    output = parsed.output or RESULTS / f"results-{host_slug}.json"
    output.parent.mkdir(parents=True, exist_ok=True)
    document = {
        "schema_version": 1,
        "profile": "ncm-biomem-rs.v1",
        "task": "ncm-rs-020",
        "commit": git("rev-parse", "HEAD"),
        "branch": git("branch", "--show-current"),
        "working_tree_clean_before_results": not bool(git("status", "--short")),
        "captured_unix_seconds": int(time.time()),
        "hardware": hardware(),
        "feature_sets": {
            "non_encoder": "--no-default-features, HashEncoder where text is required",
            "real_encoder": "default features, pinned MiniLmEncoder, offline-only",
        },
        "operation_counts": {"default": ops, "kernel_only": kernel_ops},
        "status": "blocked_environment" if blocked else ("complete" if run_all else "partial"),
        "selected_groups": sorted(selected) if selected else ["all"],
        "blocked_populations": [run["result"]["data"] for run in blocked],
        "runs": runs,
        "interpretation": {
            "kernel_only_warning": "Kernel-only latencies are pre-embedded and are not full-text recall latencies.",
            "flat_baseline_scope": "The flat baseline scans the same normalized active LTM center keys and returns distance-only top-k; it excludes projection, record support, and hydration.",
            "idempotency_limit": "Unique idempotency events remain durable until the 64 MiB source-basis quota rejects further ingestion. VACUUM/checkpoint compaction does not prune them.",
            "allocated_bytes": "Not measured: adding an allocator instrumentation dependency was not justified and no suitable dependency was already locked.",
        },
    }
    temporary = output.with_suffix(output.suffix + ".tmp")
    temporary.write_text(json.dumps(document, indent=2, sort_keys=True) + "\n")
    temporary.replace(output)
    print(output)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
