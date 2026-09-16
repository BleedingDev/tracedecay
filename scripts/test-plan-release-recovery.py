#!/usr/bin/env python3
"""Behavioral tests for immutable release recovery planning."""

from __future__ import annotations

import json
import subprocess
import tempfile
from pathlib import Path


SCRIPT = Path(__file__).with_name("plan-release-recovery.py")
TARGETS = {
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
                "model_manifest": "model-acquisition-manifest.json",
                "checksum": "sha256",
            },
        },
        {
            "name": "linux",
            "runner": "ubuntu",
            "target": "x86_64-linux",
            "archive": "tar.gz",
            "ncm": "native-only",
        },
    ]
}
POLICY = {
    "schema_version": 1,
    "provider_id": "ncm",
    "worker": "tracedecay-ncm-worker",
    "model_acquisition_manifest": "product/ncm/release/model-acquisition-manifest.json",
    "packaging": {
        "worker_distribution": "separate-sidecar",
        "standard_cli_archive_includes_worker": False,
        "manifest_sidecar_required": True,
        "model_acquisition_manifest_sidecar_required": True,
    },
    "release_targets": [
        {
            "name": "aarch64-macos",
            "target": "aarch64-apple-darwin",
            "ncm": "supported",
        },
        {"name": "linux", "target": "x86_64-linux", "ncm": "native-only"},
    ],
}

ARM_BINARY = "tracedecay-v1.2.3-aarch64-macos.tar.gz"
ARM_MCPB = "tracedecay-v1.2.3-aarch64-macos.mcpb"
ARM_SIDECAR = "tracedecay-ncm-worker-v1.2.3-aarch64-macos.tar.gz"
ARM_SIDECAR_CHECKSUM = f"{ARM_SIDECAR}.sha256"
LINUX_BINARY = "tracedecay-v1.2.3-linux.tar.gz"
LINUX_MCPB = "tracedecay-v1.2.3-linux.mcpb"


def run(
    root: Path,
    assets: tuple[str, ...],
    profile: str = "stable",
    success: bool = True,
) -> tuple[dict[str, object], list[str]]:
    (root / "assets").write_text("\n".join(assets), encoding="utf-8")
    github_output = root / "github-output"
    retained = root / "retained"
    completed = subprocess.run(
        [
            "python3",
            str(SCRIPT),
            "--manifest",
            str(root / "targets.json"),
            "--worker-platforms",
            str(root / "worker-platforms.json"),
            "--tag",
            "v1.2.3",
            "--profile",
            profile,
            "--asset-names",
            str(root / "assets"),
            "--retained-output",
            str(retained),
            "--github-output",
            str(github_output),
        ],
        capture_output=True,
        text=True,
    )
    if (completed.returncode == 0) != success:
        raise AssertionError(completed.stdout + completed.stderr)
    if not success:
        return {}, []
    outputs = dict(
        line.split("=", 1)
        for line in github_output.read_text(encoding="utf-8").splitlines()
    )
    matrix = json.loads(outputs["matrix"])
    expected_build = "true" if matrix["include"] else "false"
    assert outputs["build_required"] == expected_build
    return matrix, retained.read_text().splitlines()


def main() -> None:
    with tempfile.TemporaryDirectory() as directory:
        root = Path(directory)
        (root / "targets.json").write_text(json.dumps(TARGETS), encoding="utf-8")
        (root / "worker-platforms.json").write_text(json.dumps(POLICY), encoding="utf-8")

        matrix, retained = run(root, ())
        assert matrix == TARGETS
        assert retained == []

        matrix, retained = run(root, (ARM_BINARY,))
        assert matrix == TARGETS
        assert retained == [ARM_BINARY]

        matrix, retained = run(root, (ARM_BINARY, ARM_MCPB, ARM_SIDECAR))
        assert matrix == {"include": [TARGETS["include"][0], TARGETS["include"][1]]}
        assert retained == sorted((ARM_BINARY, ARM_MCPB, ARM_SIDECAR))

        complete_assets = (
            ARM_BINARY,
            ARM_MCPB,
            ARM_SIDECAR,
            ARM_SIDECAR_CHECKSUM,
            LINUX_BINARY,
            LINUX_MCPB,
            "SHA256SUMS",
            "install.sh",
        )
        matrix, retained = run(root, complete_assets)
        assert matrix == {"include": []}
        assert retained == sorted(complete_assets[:6])

        run(root, (ARM_BINARY, "SHA256SUMS"), success=False)
        run(root, (ARM_BINARY, "install.sh"), success=False)
        run(root, ("tracedecay-ncm-worker-v1.2.3-linux.tar.gz",), success=False)

        beta_arm = "tracedecay-beta-v1.2.3-aarch64-macos.tar.gz"
        beta_sidecar = "tracedecay-ncm-worker-beta-v1.2.3-aarch64-macos.tar.gz"
        beta_sidecar_checksum = f"{beta_sidecar}.sha256"
        matrix, retained = run(
            root,
            (beta_arm, beta_sidecar, beta_sidecar_checksum),
            profile="beta",
        )
        assert matrix == {"include": [TARGETS["include"][0], TARGETS["include"][1]]}
        assert retained == sorted((beta_arm, beta_sidecar, beta_sidecar_checksum))

    print("release recovery planner tests passed")


if __name__ == "__main__":
    main()
