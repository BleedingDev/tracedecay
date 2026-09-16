#!/usr/bin/env python3
"""Focused tests for exact release artifact coverage."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
import subprocess
import sys
import tempfile


SCRIPT = Path(__file__).with_name("check-release-artifacts.py")


LEGACY_TARGETS = {
    "include": [
        {
            "name": "linux",
            "runner": "linux",
            "target": "linux",
            "archive": "tar.gz",
        },
        {
            "name": "windows",
            "runner": "windows",
            "target": "windows",
            "archive": "zip",
        },
    ]
}

NCM_TARGETS = {
    "include": [
        {
            "name": "aarch64-macos",
            "runner": "macos-14",
            "target": "aarch64-apple-darwin",
            "archive": "tar.gz",
            "ncm": "supported",
            "sidecar": {
                "worker": "tracedecay-ncm-worker",
                "archive": "tar.gz",
                "manifest": "worker-manifest.json",
                "checksum": "sha256",
            },
        },
        {
            "name": "linux",
            "runner": "linux",
            "target": "x86_64-unknown-linux-gnu",
            "archive": "tar.gz",
            "ncm": "native-only",
        },
    ]
}

NCM_POLICY = {
    "schema_version": 1,
    "provider_id": "ncm",
    "worker": "tracedecay-ncm-worker",
    "packaging": {
        "worker_distribution": "separate-sidecar",
        "standard_cli_archive_includes_worker": False,
        "manifest_sidecar_required": True,
    },
    "release_targets": [
        {
            "name": "aarch64-macos",
            "target": "aarch64-apple-darwin",
            "ncm": "supported",
        },
        {
            "name": "linux",
            "target": "x86_64-unknown-linux-gnu",
            "ncm": "native-only",
        },
    ],
}


def invoke(
    root: Path,
    *,
    targets: dict[str, object],
    profile: str = "stable",
    sidecars: bool = False,
) -> subprocess.CompletedProcess[str]:
    manifest = root / "targets.json"
    manifest.write_text(json.dumps(targets), encoding="utf-8")
    binaries = root / "binaries"
    mcpbs = root / "mcpbs"
    binaries.mkdir(exist_ok=True)
    mcpbs.mkdir(exist_ok=True)
    command = [
        sys.executable,
        str(SCRIPT),
        "--manifest",
        str(manifest),
        "--tag",
        "v1.2.3",
        "--profile",
        profile,
        "--binaries",
        str(binaries),
        "--mcpbs",
        str(mcpbs),
    ]
    if sidecars:
        command.extend(["--sidecars", str(root / "sidecars")])
        command.extend(["--worker-platforms", str(root / "worker-platforms.json")])
    return subprocess.run(command, capture_output=True, text=True)


def write_cli_assets(root: Path, targets: dict[str, object], profile: str = "stable") -> None:
    (root / "binaries").mkdir(exist_ok=True)
    (root / "mcpbs").mkdir(exist_ok=True)
    binary_prefix = "tracedecay-beta" if profile == "beta" else "tracedecay"
    for target in targets["include"]:
        binary = root / "binaries" / (
            f"{binary_prefix}-v1.2.3-{target['name']}.{target['archive']}"
        )
        mcpb = root / "mcpbs" / f"{binary_prefix}-v1.2.3-{target['name']}.mcpb"
        binary.write_bytes(b"archive")
        mcpb.write_bytes(b"mcpb")


def write_sidecar(root: Path, profile: str = "stable") -> str:
    sidecars = root / "sidecars"
    sidecars.mkdir(exist_ok=True)
    prefix = "tracedecay-ncm-worker-beta" if profile == "beta" else "tracedecay-ncm-worker"
    archive = f"{prefix}-v1.2.3-aarch64-macos.tar.gz"
    (sidecars / archive).write_bytes(b"worker sidecar")
    digest = hashlib.sha256((sidecars / archive).read_bytes()).hexdigest()
    (sidecars / f"{archive}.sha256").write_text(
        f"{digest}  {archive}\n", encoding="utf-8"
    )
    return archive


def main() -> int:
    with tempfile.TemporaryDirectory() as directory:
        root = Path(directory)

        # The original CLI archive/MCPB contract remains valid without NCM
        # metadata or a sidecar directory.
        write_cli_assets(root, LEGACY_TARGETS)
        completed = invoke(root, targets=LEGACY_TARGETS)
        assert completed.returncode == 0, completed.stderr
        (root / "mcpbs" / "tracedecay-v1.2.3-linux.mcpb").unlink()
        assert invoke(root, targets=LEGACY_TARGETS).returncode != 0
        (root / "mcpbs" / "tracedecay-v1.2.3-linux.mcpb").write_bytes(b"mcpb")
        (root / "binaries" / "unexpected.zip").write_bytes(b"artifact")
        assert invoke(root, targets=LEGACY_TARGETS).returncode != 0

        for child in (root / "binaries", root / "mcpbs"):
            for item in child.iterdir():
                item.unlink()
        write_cli_assets(root, LEGACY_TARGETS, profile="beta")
        assert invoke(root, targets=LEGACY_TARGETS, profile="beta").returncode == 0

        (root / "worker-platforms.json").write_text(
            json.dumps(NCM_POLICY), encoding="utf-8"
        )
        for child in (root / "binaries", root / "mcpbs"):
            for item in child.iterdir():
                item.unlink()
        write_cli_assets(root, NCM_TARGETS)
        write_sidecar(root)
        completed = invoke(root, targets=NCM_TARGETS, sidecars=True)
        assert completed.returncode == 0, completed.stderr

        sidecar_archive = write_sidecar(root)
        (root / "sidecars" / f"{sidecar_archive}.sha256").unlink()
        assert invoke(root, targets=NCM_TARGETS, sidecars=True).returncode != 0
        write_sidecar(root)
        (root / "sidecars" / f"{sidecar_archive}.sha256").write_text(
            f"{'0' * 64}  {sidecar_archive}\n", encoding="utf-8"
        )
        assert invoke(root, targets=NCM_TARGETS, sidecars=True).returncode != 0
        write_sidecar(root)
        assert invoke(root, targets=NCM_TARGETS).returncode != 0

        # A sidecar is allowed only for the policy-supported arm64 macOS row.
        (root / "sidecars" / "tracedecay-ncm-worker-v1.2.3-linux.tar.gz").write_bytes(
            b"unexpected"
        )
        assert invoke(root, targets=NCM_TARGETS, sidecars=True).returncode != 0

        for child in (root / "binaries", root / "mcpbs", root / "sidecars"):
            for item in child.iterdir():
                item.unlink()
        write_cli_assets(root, NCM_TARGETS, profile="beta")
        write_sidecar(root, profile="beta")
        assert (
            invoke(root, targets=NCM_TARGETS, profile="beta", sidecars=True).returncode
            == 0
        )

    print("release artifact validator tests passed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
