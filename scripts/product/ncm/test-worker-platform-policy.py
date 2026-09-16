#!/usr/bin/env python3
"""Tests for the NCM worker platform policy checker."""

from __future__ import annotations

import importlib.util
import json
from pathlib import Path
import subprocess
import sys
import tempfile


ROOT = Path(__file__).resolve().parents[3]
POLICY = ROOT / "product/ncm/reference/worker-platforms.json"
WORKER_MANIFEST = ROOT / "product/ncm/reference/worker-manifest.json"
RELEASE_TARGETS = ROOT / ".github/release-targets.json"
CHECKER = Path(__file__).with_name("check-worker-platform.py")


def load_policy_module():
    path = Path(__file__).with_name("worker_platform_policy.py")
    spec = importlib.util.spec_from_file_location("worker_platform_policy", path)
    if spec is None or spec.loader is None:
        raise AssertionError("cannot load policy module")
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def run_checker(*arguments: str, expect: int = 0) -> str:
    completed = subprocess.run(
        [sys.executable, str(CHECKER), *arguments],
        cwd=ROOT,
        capture_output=True,
        text=True,
    )
    if completed.returncode != expect:
        raise AssertionError(completed.stdout + completed.stderr)
    return completed.stdout


def main() -> int:
    module = load_policy_module()
    policy = module.validate_worker_platform_policy(
        POLICY,
        worker_manifest_path=WORKER_MANIFEST,
        release_target_manifest_path=RELEASE_TARGETS,
    )
    assert module.worker_platform_capability(policy, "aarch64-apple-darwin") == {
        "target": "aarch64-apple-darwin",
        "status": "supported",
        "fallback": "ncm-worker",
    }
    assert module.worker_platform_capability(policy, "x86_64-unknown-linux-gnu") == {
        "target": "x86_64-unknown-linux-gnu",
        "status": "unsupported",
        "fallback": "native-only",
    }

    output = run_checker(
        "--policy",
        str(POLICY),
        "--worker-manifest",
        str(WORKER_MANIFEST),
        "--release-targets",
        str(RELEASE_TARGETS),
    )
    assert "consistent" in output
    capability = json.loads(
        run_checker(
            "--policy",
            str(POLICY),
            "--worker-manifest",
            str(WORKER_MANIFEST),
            "--release-targets",
            str(RELEASE_TARGETS),
            "--target",
            "x86_64-pc-windows-msvc",
        )
    )
    assert capability["status"] == "unsupported"
    run_checker(
        "--policy",
        str(POLICY),
        "--worker-manifest",
        str(WORKER_MANIFEST),
        "--release-targets",
        str(RELEASE_TARGETS),
        "--target",
        "x86_64-pc-windows-msvc",
        "--require-supported",
        expect=2,
    )

    with tempfile.TemporaryDirectory() as directory:
        root = Path(directory)
        policy_path = root / "worker-platforms.json"
        worker_path = root / "worker-manifest.json"
        release_path = root / "release-targets.json"
        policy_value = json.loads(POLICY.read_text(encoding="utf-8"))
        worker_value = json.loads(WORKER_MANIFEST.read_text(encoding="utf-8"))
        release_value = json.loads(RELEASE_TARGETS.read_text(encoding="utf-8"))
        worker_value["targets"].append(
            {
                "triple": "x86_64-unknown-linux-gnu",
                "os": "linux",
                "arch": "x86_64",
                "family": "unix",
                "bytes": 1,
                "sha256": "0" * 64,
            }
        )
        worker_path.write_text(json.dumps(worker_value), encoding="utf-8")
        policy_path.write_text(json.dumps(policy_value), encoding="utf-8")
        release_path.write_text(json.dumps(release_value), encoding="utf-8")
        run_checker(
            "--policy",
            str(policy_path),
            "--worker-manifest",
            str(worker_path),
            "--release-targets",
            str(release_path),
            expect=2,
        )

        worker_value["targets"] = worker_value["targets"][:1]
        worker_value["targets"][0]["bytes"] = 0
        worker_path.write_text(json.dumps(worker_value), encoding="utf-8")
        run_checker(
            "--policy",
            str(policy_path),
            "--worker-manifest",
            str(worker_path),
            "--release-targets",
            str(release_path),
            expect=2,
        )

        worker_path.write_text(
            WORKER_MANIFEST.read_text(encoding="utf-8"), encoding="utf-8"
        )
        policy_value["packaging"]["standard_cli_archive_includes_worker"] = True
        policy_path.write_text(json.dumps(policy_value), encoding="utf-8")
        run_checker(
            "--policy",
            str(policy_path),
            "--worker-manifest",
            str(worker_path),
            "--release-targets",
            str(release_path),
            expect=2,
        )
    print("NCM worker platform policy tests passed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
