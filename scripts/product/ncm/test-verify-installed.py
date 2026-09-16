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
from unittest import mock


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


def write_revision_receipt(root: Path, manifest: dict[str, object]) -> Path:
    """Create a fixture receipt whose digest is bound into the manifest."""
    path = root / Path(MODULE.MODEL_REVISION_RECEIPT_PATH).name
    receipt = {
        "schema_version": 1,
        "identities": {
            "model": {
                "model": manifest["model"],
                "revision": manifest["revision"],
                "artifact_sha256": next(
                    entry["sha256"]
                    for entry in manifest["files"]
                    if entry["path"] == "onnx/model.onnx"
                ),
                "manifest_sha256": manifest["embedding_manifest_sha256"],
                "files": [
                    {
                        "path": entry["path"],
                        "bytes": entry["bytes"],
                        "sha256": entry["sha256"],
                    }
                    for entry in manifest["files"]
                ],
            }
        },
    }
    raw = json.dumps(receipt, indent=2).encode("utf-8") + b"\n"
    path.write_bytes(raw)
    manifest["revision_provenance_sha256"] = hashlib.sha256(raw).hexdigest()
    return path


def write_tar_entry(archive: tarfile.TarFile, name: str, payload: bytes, mode: int) -> None:
    info = tarfile.TarInfo(name)
    info.size = len(payload)
    info.mode = mode
    info.uid = info.gid = 0
    info.mtime = 0
    archive.addfile(info, io.BytesIO(payload))


def elf_fixture(*, machine: int, elf_class: int = 2, byte_order: int = 1) -> bytes:
    """Build the smallest header accepted by the release format gate."""
    payload = bytearray(64)
    payload[:4] = b"\x7fELF"
    payload[4] = elf_class
    payload[5] = byte_order
    payload[6] = 1
    payload[18:20] = machine.to_bytes(2, "little" if byte_order == 1 else "big")
    return bytes(payload)


def pe_fixture(*, machine: int, pe_offset: int = 0x40) -> bytes:
    """Build a minimal DOS/PE header with a selected COFF machine."""
    payload = bytearray(pe_offset + 24)
    payload[:2] = b"MZ"
    payload[0x3C:0x40] = pe_offset.to_bytes(4, "little")
    payload[pe_offset : pe_offset + 4] = b"PE\0\0"
    payload[pe_offset + 4 : pe_offset + 6] = machine.to_bytes(2, "little")
    return bytes(payload)


def macho_fixture() -> bytes:
    """Build the minimal arm64 Mach-O header used by sidecar fixtures."""
    return b"\xcf\xfa\xed\xfe" + (0x0100000C).to_bytes(4, "little")


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
            verified = MODULE.verify_model_tree(root, manifest)
            valid_receipt = receipt.read_bytes()

            duplicate_receipt = copy.deepcopy(receipt_value)
            duplicate_receipt["files"].append(copy.deepcopy(duplicate_receipt["files"][0]))
            receipt.write_text(json.dumps(duplicate_receipt), encoding="utf-8")
            with self.assertRaisesRegex(
                MODULE.VerificationFailure,
                "installed model acquisition receipt repeats",
            ):
                MODULE._validate_installed_receipt(root, manifest, verified)

            garbage_receipt = copy.deepcopy(receipt_value)
            garbage_receipt["files"].append("garbage")
            receipt.write_text(json.dumps(garbage_receipt), encoding="utf-8")
            with self.assertRaisesRegex(
                MODULE.VerificationFailure,
                r"installed model acquisition receipt files\[5\] must be an object",
            ):
                MODULE._validate_installed_receipt(root, manifest, verified)
            receipt.write_bytes(valid_receipt)

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
            MODULE._validate_installed_receipt(
                root,
                updated,
                MODULE.verify_model_tree(root, updated),
            )

    def test_published_recovery_keeps_journal_until_receipt_matches(self) -> None:
        manifest = fixture_manifest()
        with tempfile.TemporaryDirectory(prefix="ncm-published-recovery-") as directory:
            root = Path(directory) / "state"
            source = Path(directory) / "source"
            source.mkdir()
            revision_receipt = write_revision_receipt(Path(directory), manifest)
            write_source(source, manifest)
            MODULE.acquire_model(
                root,
                manifest,
                source_dir=source,
                revision_receipt_path=revision_receipt,
            )
            acquisition_receipt = root / "receipts" / MODULE.RECEIPT_FILENAME
            acquisition_receipt.unlink()
            operation_id = MODULE._operation_id("install")
            journal = {
                "schema_version": 1,
                "operation_id": operation_id,
                "operation": "install",
                "phase": "published",
                "target": MODULE.SUPPORTED_TARGET,
                "revision": MODULE.MODEL_REVISION,
                "staging_name": None,
                "backup_name": None,
                "before_digest": None,
                "after_digest": MODULE._tree_digest(root / "models"),
            }
            (root / MODULE.JOURNAL_FILENAME).write_text(
                json.dumps(journal), encoding="utf-8"
            )

            with self.assertRaisesRegex(
                MODULE.VerificationFailure, "acquisition receipt"
            ):
                MODULE.recover_model(
                    root, manifest, revision_receipt=revision_receipt
                )
            self.assertTrue((root / MODULE.JOURNAL_FILENAME).is_file())

            verified = MODULE.verify_model_tree(
                root, manifest, revision_receipt=revision_receipt
            )
            stale = MODULE._receipt(
                manifest,
                "install",
                "committed",
                root,
                verified,
                MODULE._operation_id("install"),
            )
            MODULE._write_receipt(root, manifest, stale, None)
            with self.assertRaisesRegex(MODULE.VerificationFailure, "operation identity"):
                MODULE.recover_model(
                    root, manifest, revision_receipt=revision_receipt
                )
            self.assertTrue((root / MODULE.JOURNAL_FILENAME).is_file())

            matching = MODULE._receipt(
                manifest, "install", "committed", root, verified, operation_id
            )
            MODULE._write_receipt(root, manifest, matching, None)
            result = MODULE.recover_model(
                root, manifest, revision_receipt=revision_receipt
            )
            self.assertEqual(result["outcome"], "committed")
            self.assertFalse((root / MODULE.JOURNAL_FILENAME).exists())

    def test_worker_sidecar_carries_both_target_bound_manifests(self) -> None:
        model = fixture_manifest()
        worker_payload = macho_fixture()
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
            version = "1.2.3"
            source_sha = "a" * 40
            binary = (
                "#!/bin/sh\n"
                f'case "$1" in --version) echo "tracedecay {version}+{source_sha}" ;; '
                "--help) echo help ;; esac\n"
            ).encode()
            archive_path = root / "tracedecay.tar.gz"
            with tarfile.open(archive_path, "w:gz") as archive:
                write_tar_entry(archive, "tracedecay", binary, 0o755)
            archive_digest = hashlib.sha256(archive_path.read_bytes()).hexdigest()
            with self.assertRaisesRegex(MODULE.VerificationFailure, "Mach-O"):
                MODULE.verify_binary_archive(
                    archive_path,
                    target=MODULE.SUPPORTED_TARGET,
                    expected_version=version,
                    expected_source_sha=source_sha,
                    expected_archive_sha256=archive_digest,
                )
            script_fixture = root / "tracedecay-script-fixture"
            script_fixture.write_bytes(binary)
            script_fixture.chmod(0o755)
            MODULE._probe_cli_identity(
                script_fixture,
                expected_version=version,
                expected_source_sha=source_sha,
            )

            with self.assertRaisesRegex(MODULE.VerificationFailure, "Mach-O"):
                MODULE.verify_binary_archive(
                    archive_path,
                    target=MODULE.SUPPORTED_TARGET,
                    expected_version=version,
                    expected_source_sha="b" * 40,
                    expected_archive_sha256=archive_digest,
                )

            with mock.patch.object(MODULE, "_verify_executable_format"):
                verified_archive = MODULE.verify_binary_archive(
                    archive_path,
                    target=MODULE.SUPPORTED_TARGET,
                    expected_version=version,
                    expected_source_sha=source_sha,
                    expected_archive_sha256=archive_digest,
                    expected_binary_sha256=hashlib.sha256(binary).hexdigest(),
                )
                self.assertEqual(
                    verified_archive["sha256"], hashlib.sha256(binary).hexdigest()
                )
                with self.assertRaisesRegex(
                    MODULE.VerificationFailure,
                    "separately verified installed binary",
                ):
                    MODULE.verify_binary_archive(
                        archive_path,
                        target=MODULE.SUPPORTED_TARGET,
                        expected_version=version,
                        expected_source_sha=source_sha,
                        expected_archive_sha256=archive_digest,
                        expected_binary_sha256="b" * 64,
                    )

            installed = root / "tracedecay"
            installed.write_bytes(binary)
            installed.chmod(0o755)
            with self.assertRaisesRegex(MODULE.VerificationFailure, "Mach-O"):
                MODULE.verify_installed_binary(
                    installed,
                    target=MODULE.SUPPORTED_TARGET,
                    expected_version=version,
                    expected_source_sha=source_sha,
                    expected_binary_sha256=hashlib.sha256(binary).hexdigest(),
                )

            with self.assertRaisesRegex(MODULE.VerificationFailure, "expected installed CLI SHA-256"):
                MODULE.verify_installed_binary(
                    installed,
                    target=MODULE.SUPPORTED_TARGET,
                    expected_version=version,
                    expected_source_sha=source_sha,
                )

    def test_elf_and_pe_headers_bind_release_targets(self) -> None:
        with tempfile.TemporaryDirectory(prefix="ncm-executable-headers-") as directory:
            root = Path(directory)
            arm_elf = root / "arm64-elf"
            arm_elf.write_bytes(elf_fixture(machine=0x00B7))
            MODULE._verify_executable_format(
                arm_elf,
                target="aarch64-unknown-linux-gnu",
            )

            x86_elf = root / "x86-elf"
            # Real release binaries exceed the header prefix bound; format
            # inspection must not treat that bound as a full-file limit.
            x86_elf.write_bytes(elf_fixture(machine=0x003E) + b"\0" * 8192)
            MODULE._verify_executable_format(
                x86_elf,
                target="x86_64-unknown-linux-gnu",
            )
            with self.assertRaisesRegex(MODULE.VerificationFailure, "ELF machine"):
                MODULE._verify_executable_format(
                    x86_elf,
                    target="aarch64-unknown-linux-gnu",
                )
            narrow_elf = root / "narrow-elf"
            narrow_elf.write_bytes(elf_fixture(machine=0x0003, elf_class=1))
            with self.assertRaisesRegex(MODULE.VerificationFailure, "ELF class"):
                MODULE._verify_executable_format(
                    narrow_elf,
                    target="x86_64-unknown-linux-gnu",
                )

            truncated_elf = root / "truncated-elf"
            truncated_elf.write_bytes(b"\x7fELF\x02\x01")
            with self.assertRaisesRegex(MODULE.VerificationFailure, "ELF"):
                MODULE._verify_executable_format(
                    truncated_elf,
                    target="x86_64-unknown-linux-gnu",
                )

            x86_pe = root / "x86-pe"
            x86_pe.write_bytes(pe_fixture(machine=0x8664) + b"\0" * 8192)
            MODULE._verify_executable_format(
                x86_pe,
                target="x86_64-pc-windows-msvc",
            )
            arm_pe = root / "arm-pe"
            arm_pe.write_bytes(pe_fixture(machine=0xAA64))
            with self.assertRaisesRegex(MODULE.VerificationFailure, "COFF machine"):
                MODULE._verify_executable_format(
                    arm_pe,
                    target="x86_64-pc-windows-msvc",
                )

            truncated_pe = root / "truncated-pe"
            truncated_pe.write_bytes(b"MZ" + b"\0" * 58 + (0x40).to_bytes(4, "little"))
            with self.assertRaisesRegex(MODULE.VerificationFailure, "PE"):
                MODULE._verify_executable_format(
                    truncated_pe,
                    target="x86_64-pc-windows-msvc",
                )


if __name__ == "__main__":
    suite = unittest.defaultTestLoader.loadTestsFromTestCase(VerifyInstalledTest)
    result = unittest.TextTestRunner(verbosity=1).run(suite)
    raise SystemExit(0 if result.wasSuccessful() else 1)
