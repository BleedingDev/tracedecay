#!/usr/bin/env python3
"""Behavioral tests for immutable release recovery planning."""

from __future__ import annotations

import json
import io
import subprocess
import tempfile
from pathlib import Path
import tarfile
import zipfile


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


def macho_fixture(*, cpu_type: int = 0x0100000C) -> bytes:
    return b"\xcf\xfa\xed\xfe" + cpu_type.to_bytes(4, "little")


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


def write_retained_sidecar(root: Path, *, legacy: bool = False) -> None:
    directory = root / "retained-assets"
    directory.mkdir(exist_ok=True)
    archive_path = directory / ARM_SIDECAR
    with tarfile.open(archive_path, mode="w:gz") as archive:
        entries = [
            ("tracedecay-ncm-worker", macho_fixture(), 0o755),
            ("worker-manifest.json", b"{}", 0o644),
        ]
        if not legacy:
            model_manifest = {
                "schema_version": 1,
                "manifest_type": "ncm-model-acquisition",
                "provider_id": "ncm",
                "worker": "tracedecay-ncm-worker",
                "target": "aarch64-apple-darwin",
                "release_name": "aarch64-macos",
                "revision_provenance_sha256": "a" * 64,
                "transaction": {"journal": "ncm-model-lifecycle-v1.json"},
                "receipt": {
                    "relative_path": "receipts/ncm-model-acquisition-v1.json",
                    "required_fields": ["revision_provenance_sha256"],
                },
            }
            entries.append(
                (
                    "model-acquisition-manifest.json",
                    json.dumps(model_manifest).encode("utf-8"),
                    0o644,
                )
            )
        for name, payload, mode in entries:
            info = tarfile.TarInfo(name)
            info.mode = mode
            info.size = len(payload)
            archive.addfile(info, io.BytesIO(payload))


def write_retained_binary(root: Path, *, wrong_arch: bool = False) -> None:
    directory = root / "retained-assets"
    directory.mkdir(exist_ok=True)
    payload = macho_fixture(cpu_type=0x01000007 if wrong_arch else 0x0100000C)
    with tarfile.open(directory / ARM_BINARY, mode="w:gz") as archive:
        info = tarfile.TarInfo("tracedecay")
        info.mode = 0o755
        info.size = len(payload)
        archive.addfile(info, io.BytesIO(payload))


def write_retained_mcpb(root: Path, *, wrong_arch: bool = False) -> None:
    directory = root / "retained-assets"
    directory.mkdir(exist_ok=True)
    payload = macho_fixture(cpu_type=0x01000007 if wrong_arch else 0x0100000C)
    with zipfile.ZipFile(directory / ARM_MCPB, mode="w") as archive:
        archive.writestr("manifest.json", b"{}")
        entry = zipfile.ZipInfo("server/tracedecay")
        entry.external_attr = (0o100755) << 16
        archive.writestr(entry, payload)


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

        write_retained_sidecar(root)
        write_retained_binary(root)
        write_retained_mcpb(root)
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
                "--asset-directory",
                str(root / "retained-assets"),
            ],
            capture_output=True,
            text=True,
        )
        assert completed.returncode == 0, completed.stdout + completed.stderr
        write_retained_binary(root, wrong_arch=True)
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
                "--asset-directory",
                str(root / "retained-assets"),
            ],
            capture_output=True,
            text=True,
        )
        assert completed.returncode != 0
        assert "Mach-O CPU type" in completed.stderr
        write_retained_binary(root)
        write_retained_sidecar(root, legacy=True)
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
                "--asset-directory",
                str(root / "retained-assets"),
            ],
            capture_output=True,
            text=True,
        )
        assert completed.returncode != 0
        assert "model-acquisition-manifest.json" in completed.stderr

    print("release recovery planner tests passed")


if __name__ == "__main__":
    main()
