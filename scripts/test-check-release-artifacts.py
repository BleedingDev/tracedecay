#!/usr/bin/env python3
"""Focused tests for exact release artifact coverage."""

from __future__ import annotations

import hashlib
import io
import json
from pathlib import Path
import subprocess
import sys
import tarfile
import tempfile


SCRIPT = Path(__file__).with_name("check-release-artifacts.py")
WORKER_BYTES = b"worker sidecar"


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
                "model_manifest": "model-acquisition-manifest.json",
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
        command.extend(["--worker-manifest", str(root / "worker-manifest.json")])
        command.extend(
            ["--model-acquisition-manifest", str(root / "model-acquisition-manifest.json")]
        )
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
    manifest = {
        "schema_version": 1,
        "worker": "tracedecay-ncm-worker",
        "protocol_version": 1,
        "protocol_identity": "tracedecay.ncm.worker.v1",
        "targets": [
            {
                "triple": "aarch64-apple-darwin",
                "os": "macos",
                "arch": "aarch64",
                "family": "unix",
                "bytes": len(WORKER_BYTES),
                "sha256": hashlib.sha256(WORKER_BYTES).hexdigest(),
            }
        ],
    }
    manifest_path = root / "worker-manifest.json"
    manifest_path.write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")
    model_manifest = {
        "schema_version": 1,
        "manifest_type": "ncm-model-acquisition",
        "provider_id": "ncm",
        "worker": "tracedecay-ncm-worker",
        "target": "aarch64-apple-darwin",
        "release_name": "aarch64-macos",
        "embedding_manifest": "product/ncm/reference/embedding-manifest.json",
        "embedding_manifest_sha256": "40084ced45c8bc429e525f65ffbfec6dd4e9ded4267f1be8d092c499f2dcb328",
        "model_root": "models",
        "cache_repository": "models--Xenova--paraphrase-multilingual-MiniLM-L12-v2",
        "model": "paraphrase-multilingual-MiniLM-L12-v2",
        "repository": "Xenova/paraphrase-multilingual-MiniLM-L12-v2",
        "revision": "2c4055b12046f11709e9df2c122e59ffbdc2f900",
        "revision_provenance": (
            "product/ncm/receipts/backend/2fc72f1d81f543224d8e7d8ef19195b026ba855f.json"
            "#/identities/model/revision"
        ),
        "max_length": 128,
        "pooling": "mean",
        "normalize": True,
        "transport": "https",
        "base_url": (
            "https://huggingface.co/Xenova/paraphrase-multilingual-MiniLM-L12-v2/resolve/"
            "2c4055b12046f11709e9df2c122e59ffbdc2f900/"
        ),
        "files": [
            {
                "path": "onnx/model.onnx",
                "url": (
                    "https://huggingface.co/Xenova/paraphrase-multilingual-MiniLM-L12-v2/resolve/"
                    "2c4055b12046f11709e9df2c122e59ffbdc2f900/onnx/model.onnx"
                ),
                "bytes": 470268510,
                "sha256": "185ae63f47e17a7e8d30d0e6a3cde6a6e4b79bc5b81666ecffc279a6856ca113",
            },
            {
                "path": "tokenizer.json",
                "url": (
                    "https://huggingface.co/Xenova/paraphrase-multilingual-MiniLM-L12-v2/resolve/"
                    "2c4055b12046f11709e9df2c122e59ffbdc2f900/tokenizer.json"
                ),
                "bytes": 17082913,
                "sha256": "b60b6b43406a48bf3638526314f3d232d97058bc93472ff2de930d43686fa441",
            },
            {
                "path": "config.json",
                "url": (
                    "https://huggingface.co/Xenova/paraphrase-multilingual-MiniLM-L12-v2/resolve/"
                    "2c4055b12046f11709e9df2c122e59ffbdc2f900/config.json"
                ),
                "bytes": 673,
                "sha256": "05b570bff786faa5c4604152aa16f19f77ed6dfc31e47dd0f3dd987078693ac7",
            },
            {
                "path": "special_tokens_map.json",
                "url": (
                    "https://huggingface.co/Xenova/paraphrase-multilingual-MiniLM-L12-v2/resolve/"
                    "2c4055b12046f11709e9df2c122e59ffbdc2f900/special_tokens_map.json"
                ),
                "bytes": 280,
                "sha256": "06e405a36dfe4b9604f484f6a1e619af1a7f7d09e34a8555eb0b77b66318067f",
            },
            {
                "path": "tokenizer_config.json",
                "url": (
                    "https://huggingface.co/Xenova/paraphrase-multilingual-MiniLM-L12-v2/resolve/"
                    "2c4055b12046f11709e9df2c122e59ffbdc2f900/tokenizer_config.json"
                ),
                "bytes": 496,
                "sha256": "3f5961b9ac86288cccdb97f32fb848d6187c78e1603958c53f3ea1f296b7d8a2",
            },
        ],
        "transaction": {
            "version": 1,
            "publication": "atomic-directory-swap",
            "journal": "ncm-model-acquisition-v1.json",
            "staging_prefix": ".ncm-model-staging-",
            "backup_prefix": ".ncm-model-backup-",
        },
        "receipt": {
            "schema_version": 1,
            "relative_path": "receipts/ncm-model-acquisition-v1.json",
            "required_fields": [
                "schema_version",
                "operation_id",
                "operation",
                "outcome",
                "target",
                "model",
                "repository",
                "revision",
                "manifest_sha256",
                "files",
                "created_at_unix",
            ],
        },
    }
    model_manifest_path = root / "model-acquisition-manifest.json"
    model_manifest_path.write_text(
        json.dumps(model_manifest, indent=2) + "\n", encoding="utf-8"
    )
    prefix = "tracedecay-ncm-worker-beta" if profile == "beta" else "tracedecay-ncm-worker"
    archive = f"{prefix}-v1.2.3-aarch64-macos.tar.gz"
    archive_path = sidecars / archive
    with tarfile.open(archive_path, mode="w:gz") as bundle:
        worker_info = tarfile.TarInfo("tracedecay-ncm-worker")
        worker_info.mode = 0o755
        worker_info.uid = worker_info.gid = 0
        worker_info.mtime = 0
        worker_info.size = len(WORKER_BYTES)
        bundle.addfile(worker_info, io.BytesIO(WORKER_BYTES))
        manifest_bytes = manifest_path.read_bytes()
        manifest_info = tarfile.TarInfo("worker-manifest.json")
        manifest_info.mode = 0o644
        manifest_info.uid = manifest_info.gid = 0
        manifest_info.mtime = 0
        manifest_info.size = len(manifest_bytes)
        bundle.addfile(manifest_info, io.BytesIO(manifest_bytes))
        model_bytes = model_manifest_path.read_bytes()
        model_info = tarfile.TarInfo("model-acquisition-manifest.json")
        model_info.mode = 0o644
        model_info.uid = model_info.gid = 0
        model_info.mtime = 0
        model_info.size = len(model_bytes)
        bundle.addfile(model_info, io.BytesIO(model_bytes))
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
