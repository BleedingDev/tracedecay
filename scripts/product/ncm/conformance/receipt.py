#!/usr/bin/env python3
"""Run the real-worker conformance suite and write its receipt.

The receipt names the exact source tree, the worker binary the suite drove,
the algorithm profile, the pinned model identity, the state schema version,
and every executed test with its outcome. It is evidence for task ncm-rs-019
and a prerequisite of the standalone backend gate (``check-backend.py``).

Only the standard library is used. The suite is always run ``--locked`` so the
receipt cannot describe a dependency set other than the committed one.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import subprocess
import sys
from pathlib import Path
from typing import Any

CRATE = "tracedecay-memory-provider-ncm"
SUITE = "rust_backend_conformance"
REAL_ENCODER_TEST = "enabled::real_encoder_process_population"
WORKER_CRATE = "tracedecay-memory-ncm-runtime"
WORKER_BIN = "tracedecay-ncm-worker"


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()


def run(command: list[str], *, cwd: Path, env: dict[str, str], timeout: int) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        command,
        cwd=cwd,
        env=env,
        text=True,
        capture_output=True,
        timeout=timeout,
        check=False,
    )


def git(repo: Path, *args: str) -> str:
    return subprocess.run(["git", "-C", str(repo), *args], text=True, capture_output=True, check=True).stdout.strip()


def source_constant(path: Path, name: str) -> str:
    match = re.search(rf'const {name}: &str = "([^"]+)";', path.read_text(encoding="utf-8"))
    if match is None:
        raise SystemExit(f"constant {name} not found in {path}")
    return match.group(1)


def parse_list(output: str) -> list[str]:
    return sorted(line[: -len(": test")] for line in output.splitlines() if line.endswith(": test"))


def parse_outcomes(output: str) -> dict[str, str]:
    outcomes: dict[str, str] = {}
    for line in output.splitlines():
        match = re.match(r"^test (\S+) \.\.\. (ok|FAILED|ignored)(?:,.*)?$", line.strip())
        if match:
            outcomes[match.group(1)] = {"ok": "pass", "FAILED": "fail", "ignored": "skip"}[match.group(2)]
    return outcomes


def build_worker(repo: Path, env: dict[str, str], *, real_encoder: bool) -> Path:
    command = ["cargo", "build", "--locked", "-p", WORKER_CRATE, "--bin", WORKER_BIN]
    if not real_encoder:
        command.append("--no-default-features")
    result = run(command, cwd=repo, env=env, timeout=3600)
    if result.returncode != 0:
        sys.stderr.write(result.stderr[-4000:])
        raise SystemExit("worker build failed")
    target = Path(env.get("CARGO_TARGET_DIR", repo / "target"))
    return (target / "debug" / WORKER_BIN).resolve()


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo", type=Path, default=Path(__file__).resolve().parents[4])
    parser.add_argument("--worker", type=Path, help="absolute worker binary; built when omitted")
    parser.add_argument(
        "--model-root",
        type=Path,
        help="installed pinned model root; enables the real-encoder population "
        "(defaults to $TRACEDECAY_NCM_REAL_MODEL_ROOT)",
    )
    parser.add_argument("--output", type=Path, help="receipt path (default product/ncm/conformance/receipt.json)")
    args = parser.parse_args()

    repo = args.repo.resolve()
    env = os.environ.copy()
    env["RUSTC_WRAPPER"] = ""
    model_root = args.model_root or (
        Path(env["TRACEDECAY_NCM_REAL_MODEL_ROOT"]) if env.get("TRACEDECAY_NCM_REAL_MODEL_ROOT") else None
    )
    if model_root is not None:
        model_root = model_root.resolve()
        if not (model_root / "models" / "ncm-encoder-manifest.json").is_file():
            raise SystemExit(f"model root has no installed manifest: {model_root}")

    worker = args.worker.resolve() if args.worker else build_worker(repo, env, real_encoder=model_root is not None)
    if not worker.is_file():
        raise SystemExit(f"worker binary missing: {worker}")
    env["TRACEDECAY_NCM_WORKER"] = str(worker)
    if model_root is not None:
        env["TRACEDECAY_NCM_REAL_MODEL_ROOT"] = str(model_root)

    base = ["cargo", "test", "--locked", "-p", CRATE, "--features", "rust-backend", "--test", SUITE]
    commands: list[list[str]] = []

    listing = run([*base, "--", "--list"], cwd=repo, env=env, timeout=3600)
    commands.append([*base, "--", "--list"])
    if listing.returncode != 0:
        sys.stderr.write(listing.stderr[-4000:])
        raise SystemExit("test listing failed")
    test_ids = parse_list(listing.stdout)
    if not test_ids:
        raise SystemExit("no tests listed")

    suite = run([*base], cwd=repo, env=env, timeout=3600)
    commands.append(list(base))
    outcomes = parse_outcomes(suite.stdout)

    if model_root is not None:
        real = [*base, "--", "--ignored", "--exact", REAL_ENCODER_TEST]
        commands.append(real)
        result = run(real, cwd=repo, env=env, timeout=3600)
        outcomes.update(parse_outcomes(result.stdout))

    tests = []
    for test_id in test_ids:
        outcome = outcomes.get(test_id, "not-run")
        if outcome == "skip" and test_id == REAL_ENCODER_TEST:
            outcome = "skip (no model root)"
        tests.append({"id": test_id, "outcome": outcome})
    counts = {
        "pass": sum(1 for t in tests if t["outcome"] == "pass"),
        "fail": sum(1 for t in tests if t["outcome"] == "fail"),
        "skip": sum(1 for t in tests if t["outcome"].startswith("skip")),
        "not_run": sum(1 for t in tests if t["outcome"] == "not-run"),
    }
    manifest_path = repo / "product/ncm/reference/embedding-manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    status = "pass" if counts["fail"] == 0 and counts["not_run"] == 0 and counts["pass"] > 0 else "fail"
    if model_root is None:
        status = "pass-without-real-encoder" if status == "pass" else status

    receipt: dict[str, Any] = {
        "task": "ncm-rs-019",
        "status": status,
        "source": {
            "commit": git(repo, "rev-parse", "HEAD"),
            "tree": git(repo, "rev-parse", "HEAD^{tree}"),
            "branch": git(repo, "branch", "--show-current"),
            "dirty": bool(git(repo, "status", "--porcelain", "--untracked-files=no")),
        },
        "worker": {
            "path": str(worker),
            "sha256": sha256_file(worker),
            "real_encoder": model_root is not None,
        },
        "identity": {
            "algorithm_profile": source_constant(repo / "crates/tracedecay-memory-ncm-core/src/types.rs", "ALGORITHM_PROFILE"),
            "state_schema_version": source_constant(repo / "crates/tracedecay-memory-ncm-runtime/src/store/mod.rs", "SCHEMA_VERSION"),
            "model": manifest["model"],
            "model_artifact_sha256": manifest["files"][0]["sha256"],
            "embedding_manifest_sha256": sha256_file(manifest_path),
            "model_root": str(model_root) if model_root else None,
        },
        "suite": {"crate": CRATE, "test_target": SUITE, "counts": counts, "tests": tests},
        "commands": [" ".join(c) for c in commands],
    }
    output = (args.output or repo / "product/ncm/conformance/receipt.json").resolve()
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(receipt, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(f"conformance receipt: {output} ({status}; {counts})")
    return 0 if status.startswith("pass") else 1


if __name__ == "__main__":
    raise SystemExit(main())
