#!/usr/bin/env python3
"""Focused tests for the NCM worker artifact trust boundary."""

from __future__ import annotations

import importlib.util
import json
import tempfile
import unittest
from pathlib import Path
from types import ModuleType


REPO = Path(__file__).resolve().parents[1]
CHECKER = REPO / "scripts/product/ncm/check-backend.py"


def load_checker() -> ModuleType:
    spec = importlib.util.spec_from_file_location("ncm_backend_checker", CHECKER)
    if spec is None or spec.loader is None:
        raise RuntimeError("cannot load NCM backend checker")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


CHECKER_MODULE = load_checker()


class NcmWorkerArtifactTest(unittest.TestCase):
    def fixture(self, *, installed: bool = False) -> tuple[Path, Path, bytes, dict]:
        root = Path(tempfile.mkdtemp(prefix="ncm-worker-trust-"))
        target = CHECKER_MODULE.current_worker_target()
        data = b"fixture-worker\x00fastembed\x00onnxruntime"
        manifest = {
            "schema_version": CHECKER_MODULE.WORKER_MANIFEST_SCHEMA_VERSION,
            "worker": CHECKER_MODULE.WORKER_NAME,
            "protocol_version": CHECKER_MODULE.WORKER_PROTOCOL_VERSION,
            "protocol_identity": CHECKER_MODULE.WORKER_PROTOCOL_IDENTITY,
            "targets": [
                {
                    "triple": target[0],
                    "os": target[1],
                    "arch": target[2],
                    "family": target[3],
                    "bytes": len(data),
                    "sha256": CHECKER_MODULE.sha256_bytes(data),
                }
            ],
        }
        trusted = root / "product" / "ncm" / "reference" / "worker-manifest.json"
        trusted.parent.mkdir(parents=True)
        trusted.write_text(json.dumps(manifest, indent=2), encoding="utf-8")
        binary = root / ("bundle/bin" if installed else "target/debug") / CHECKER_MODULE.WORKER_NAME
        binary.parent.mkdir(parents=True)
        binary.write_bytes(data)
        binary.chmod(binary.stat().st_mode | 0o111)
        (binary.parent / "worker-manifest.json").write_text(
            json.dumps(manifest, indent=2), encoding="utf-8"
        )
        return root, binary, data, manifest

    def test_source_tree_artifact_uses_checked_in_pin(self) -> None:
        root, binary, _, _ = self.fixture()
        receipt = CHECKER_MODULE.verify_worker_artifact(binary, repo=root)
        self.assertEqual(receipt["manifest_path"], str(binary.parent / "worker-manifest.json"))
        self.assertEqual(receipt["manifest_sha256"], CHECKER_MODULE.sha256_bytes(
            CHECKER_MODULE.canonical_json(json.loads((binary.parent / "worker-manifest.json").read_text()))
        ))

    def test_installed_artifact_requires_matching_sibling_manifest(self) -> None:
        root, binary, _, manifest = self.fixture(installed=True)
        sibling = binary.parent / "worker-manifest.json"
        manifest["targets"][0]["sha256"] = "0" * 64
        sibling.write_text(json.dumps(manifest, indent=2), encoding="utf-8")
        with self.assertRaises(CHECKER_MODULE.GateFailure):
            CHECKER_MODULE.verify_worker_artifact(binary, repo=root)

    def test_ancestor_manifest_cannot_satisfy_installed_binding(self) -> None:
        root, binary, _, manifest = self.fixture(installed=True)
        sibling = binary.parent / "worker-manifest.json"
        sibling.unlink()
        ancestor = binary.parents[1] / "product" / "ncm" / "reference" / "worker-manifest.json"
        ancestor.parent.mkdir(parents=True)
        ancestor.write_text(json.dumps(manifest, indent=2), encoding="utf-8")
        with self.assertRaisesRegex(CHECKER_MODULE.GateFailure, "beside the worker"):
            CHECKER_MODULE.verify_worker_artifact(binary, repo=root)

    def test_artifact_digest_mismatch_is_rejected_after_manifest_trust(self) -> None:
        root, binary, data, _ = self.fixture(installed=True)
        binary.write_bytes(data[:-1] + b"X")
        binary.chmod(binary.stat().st_mode | 0o111)
        with self.assertRaisesRegex(CHECKER_MODULE.GateFailure, "digest mismatch"):
            CHECKER_MODULE.verify_worker_artifact(binary, repo=root)

    def test_symlink_worker_is_rejected_before_manifest_or_artifact_use(self) -> None:
        root, binary, data, _ = self.fixture(installed=True)
        target = binary.with_name("real-worker")
        target.write_bytes(data)
        target.chmod(target.stat().st_mode | 0o111)
        binary.unlink()
        binary.symlink_to(target)
        with self.assertRaisesRegex(CHECKER_MODULE.GateFailure, "must not be a symlink"):
            CHECKER_MODULE.verify_worker_artifact(binary, repo=root)


if __name__ == "__main__":
    unittest.main()
