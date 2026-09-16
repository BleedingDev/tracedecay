#!/usr/bin/env python3
"""Regression gates for the installed NCM release verifier."""

from __future__ import annotations

import copy
import hashlib
import importlib.util
import io
import json
from pathlib import Path
import stat
import sys
import tarfile
import tempfile
import unittest


SCRIPT = Path(__file__).with_name("verify-installed.py")
REPO = SCRIPT.parents[3]
spec = importlib.util.spec_from_file_location("ncm_verify_installed", SCRIPT)
if spec is None or spec.loader is None:
    raise RuntimeError(f"cannot load {SCRIPT}")
MODULE = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = MODULE
spec.loader.exec_module(MODULE)


def fixture_manifest() -> dict[str, object]:
    value = json.loads(
        (REPO / "product/ncm/release/model-acquisition-manifest.json").read_text(
            encoding="utf-8"
        )
    )
    for entry in value["files"]:
        payload = (entry["path"] + " fixture bytes").encode("utf-8")
        entry["bytes"] = len(payload)
        entry["sha256"] = hashlib.sha256(payload).hexdigest()
        entry["url"] = (
            "https://huggingface.co/Xenova/paraphrase-multilingual-MiniLM-L12-v2/resolve/"
            f"{MODULE.MODEL_REVISION}/{entry['path']}"
        )
    return value


def write_source(root: Path, manifest: dict[str, object]) -> None:
    for entry in manifest["files"]:
        payload = (entry["path"] + " fixture bytes").encode("utf-8")
        destination = root / entry["path"]
        destination.parent.mkdir(parents=True, exist_ok=True)
        destination.write_bytes(payload)


def write_tar_entry(archive: tarfile.TarFile, name: str, payload: bytes, mode: int) -> None:
    info = tarfile.TarInfo(name)
    info.size = len(payload)
    info.mode = mode
    info.uid = info.gid = 0
    info.mtime = 0
    archive.addfile(info, io.BytesIO(payload))


class VerifyInstalledTest(unittest.TestCase):
    def test_checked_in_release_contract_is_target_bound(self) -> None:
        result = MODULE.verify_release_contract(REPO)
        self.assertEqual(result["target"], MODULE.SUPPORTED_TARGET)
        self.assertEqual(
            result["embedding_manifest_sha256"],
            MODULE.sha256_bytes(
                (REPO / "product/ncm/reference/embedding-manifest.json").read_bytes()
            ),
        )

    def test_target_drift_is_rejected(self) -> None:
        manifest = fixture_manifest()
        manifest["target"] = "x86_64-unknown-linux-gnu"
        with self.assertRaisesRegex(MODULE.VerificationFailure, "target"):
            MODULE.validate_acquisition_manifest(manifest)

    def test_model_install_update_and_failed_update_are_transactional(self) -> None:
        manifest = fixture_manifest()
        with tempfile.TemporaryDirectory(prefix="ncm-installed-model-") as directory:
            root = Path(directory) / "state"
            source = Path(directory) / "source"
            source.mkdir()
            write_source(source, manifest)

            first = MODULE.acquire_model(root, manifest, source_dir=source)
            self.assertEqual(first["outcome"], "committed")
            first_tree = MODULE._tree_digest(root / "models")
            receipt = root / "receipts" / MODULE.RECEIPT_FILENAME
            self.assertTrue(receipt.is_file())
            receipt_value = json.loads(receipt.read_text(encoding="utf-8"))
            self.assertEqual(receipt_value["revision"], MODULE.MODEL_REVISION)
            self.assertEqual(receipt_value["target"], MODULE.SUPPORTED_TARGET)

            existing = MODULE.acquire_model(root, manifest, source_dir=source)
            self.assertEqual(existing["outcome"], "already_present")
            self.assertEqual(MODULE._tree_digest(root / "models"), first_tree)

            with self.assertRaisesRegex(MODULE.VerificationFailure, "injected"):
                MODULE.acquire_model(
                    root,
                    manifest,
                    operation="update",
                    source_dir=source,
                    failure_after="staged",
                )
            self.assertEqual(MODULE._tree_digest(root / "models"), first_tree)
            self.assertFalse((root / MODULE.JOURNAL_FILENAME).exists())

            updated = copy.deepcopy(manifest)
            changed = b"updated fixture bytes"
            first_entry = updated["files"][0]
            first_entry["bytes"] = len(changed)
            first_entry["sha256"] = hashlib.sha256(changed).hexdigest()
            source_file = source / first_entry["path"]
            source_file.write_bytes(changed)
            second = MODULE.acquire_model(root, updated, operation="update", source_dir=source)
            self.assertEqual(second["outcome"], "committed")
            self.assertNotEqual(MODULE._tree_digest(root / "models"), first_tree)
            self.assertEqual(MODULE.verify_model_tree(root, updated)["revision"], MODULE.MODEL_REVISION)

    def test_worker_sidecar_carries_both_target_bound_manifests(self) -> None:
        model = fixture_manifest()
        worker_payload = b"worker fixture"
        worker_manifest = {
            "schema_version": 1,
            "worker": MODULE.WORKER_NAME,
            "protocol_version": 1,
            "protocol_identity": "tracedecay.ncm.worker.v1",
            "targets": [
                {
                    "triple": MODULE.SUPPORTED_TARGET,
                    "os": "macos",
                    "arch": "aarch64",
                    "family": "unix",
                    "bytes": len(worker_payload),
                    "sha256": hashlib.sha256(worker_payload).hexdigest(),
                }
            ],
        }
        with tempfile.TemporaryDirectory(prefix="ncm-installed-sidecar-") as directory:
            root = Path(directory)
            trusted_worker = root / MODULE.WORKER_MANIFEST_NAME
            trusted_model = root / MODULE.MODEL_ACQUISITION_MANIFEST_NAME
            trusted_worker.write_text(json.dumps(worker_manifest), encoding="utf-8")
            trusted_model.write_text(json.dumps(model, indent=2) + "\n", encoding="utf-8")
            archive_path = root / "worker.tar.gz"
            with tarfile.open(archive_path, "w:gz") as archive:
                write_tar_entry(archive, MODULE.WORKER_NAME, worker_payload, 0o755)
                write_tar_entry(archive, MODULE.WORKER_MANIFEST_NAME, trusted_worker.read_bytes(), 0o644)
                write_tar_entry(archive, MODULE.MODEL_ACQUISITION_MANIFEST_NAME, trusted_model.read_bytes(), 0o644)
            checksum = root / "worker.tar.gz.sha256"
            checksum.write_text(
                f"{hashlib.sha256(archive_path.read_bytes()).hexdigest()}  {archive_path.name}\n",
                encoding="utf-8",
            )
            result = MODULE.verify_worker_archive(
                archive_path,
                worker_manifest_path=trusted_worker,
                model_manifest_path=trusted_model,
                checksum_path=checksum,
            )
            self.assertEqual(result["target"], MODULE.SUPPORTED_TARGET)
            self.assertEqual(result["worker"]["bytes"], len(worker_payload))

            with self.assertRaisesRegex(MODULE.VerificationFailure, "trusted worker manifest"):
                MODULE.verify_worker_archive(
                    archive_path,
                    model_manifest_path=trusted_model,
                    checksum_path=checksum,
                )
            with self.assertRaisesRegex(MODULE.VerificationFailure, "checksum is required"):
                MODULE.verify_worker_archive(
                    archive_path,
                    worker_manifest_path=trusted_worker,
                    model_manifest_path=trusted_model,
                )

            tampered = json.loads(trusted_model.read_text(encoding="utf-8"))
            tampered["target"] = "x86_64-unknown-linux-gnu"
            trusted_model.write_text(json.dumps(tampered), encoding="utf-8")
            with self.assertRaises(MODULE.VerificationFailure):
                MODULE.verify_worker_archive(
                    archive_path,
                    worker_manifest_path=trusted_worker,
                    model_manifest_path=trusted_model,
                    checksum_path=checksum,
                )

    def test_cli_archive_is_smoked_after_safe_extraction(self) -> None:
        with tempfile.TemporaryDirectory(prefix="ncm-installed-cli-") as directory:
            root = Path(directory)
            binary = b"#!/bin/sh\ncase \"$1\" in --version) echo fixture ;; --help) echo help ;; esac\n"
            archive_path = root / "tracedecay.tar.gz"
            with tarfile.open(archive_path, "w:gz") as archive:
                write_tar_entry(archive, "tracedecay", binary, 0o755)
            result = MODULE.verify_binary_archive(archive_path, target=MODULE.SUPPORTED_TARGET)
            self.assertEqual(result["entry"], "tracedecay")
            self.assertEqual(result["bytes"], len(binary))


if __name__ == "__main__":
    suite = unittest.defaultTestLoader.loadTestsFromTestCase(VerifyInstalledTest)
    result = unittest.TextTestRunner(verbosity=1).run(suite)
    raise SystemExit(0 if result.wasSuccessful() else 1)
