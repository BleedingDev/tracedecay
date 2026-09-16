"""Focused tests for the NCM model snapshot trust boundary."""

from __future__ import annotations

import importlib.util
import json
import os
import shutil
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


class NcmModelSnapshotTest(unittest.TestCase):
    def fixture(self) -> tuple[Path, Path, dict]:
        root = Path(tempfile.mkdtemp(prefix="ncm-model-snapshot-"))
        self.addCleanup(shutil.rmtree, root, ignore_errors=True)
        models = root / "models"
        repository = models / CHECKER_MODULE.MODEL_CACHE_REPOSITORY
        snapshot = repository / "snapshots" / CHECKER_MODULE.MODEL_REVISION
        (repository / "refs").mkdir(parents=True)
        snapshot.mkdir(parents=True)
        (repository / "refs" / "main").write_text(
            CHECKER_MODULE.MODEL_REVISION + "\n", encoding="utf-8"
        )
        artifacts = {
            "onnx/model.onnx": b"fixture-onnx",
            "tokenizer.json": b"fixture-tokenizer",
            "config.json": b"{}",
            "special_tokens_map.json": b"{}",
            "tokenizer_config.json": b"{}",
        }
        files = []
        for relative, payload in artifacts.items():
            path = snapshot / relative
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(payload)
            files.append(
                {
                    "path": relative,
                    "sha256": CHECKER_MODULE.sha256_bytes(payload),
                    "bytes": len(payload),
                }
            )
        manifest = {
            "model": CHECKER_MODULE.MODEL_NAME,
            "repository": CHECKER_MODULE.MODEL_REPOSITORY,
            "revision": CHECKER_MODULE.MODEL_REVISION,
            "revision_provenance": CHECKER_MODULE.MODEL_REVISION_PROVENANCE,
            "files": files,
            "max_length": CHECKER_MODULE.MODEL_MAX_LENGTH,
            "pooling": CHECKER_MODULE.MODEL_POOLING,
            "normalize": CHECKER_MODULE.MODEL_NORMALIZE,
        }
        manifest_path = root / "embedding-manifest.json"
        manifest_path.write_text(json.dumps(manifest, indent=2), encoding="utf-8")
        (models / "ncm-encoder-manifest.json").write_text(
            json.dumps(manifest, indent=2), encoding="utf-8"
        )
        return root, manifest_path, manifest

    def test_regular_pinned_snapshot_is_accepted(self) -> None:
        root, manifest_path, manifest = self.fixture()
        result = CHECKER_MODULE.verify_model(root, manifest_path)
        self.assertEqual(result["revision"], CHECKER_MODULE.MODEL_REVISION)
        self.assertEqual(result["artifact_sha256"], manifest["files"][0]["sha256"])

    def test_wrong_main_ref_is_rejected(self) -> None:
        root, manifest_path, _ = self.fixture()
        ref = root / "models" / CHECKER_MODULE.MODEL_CACHE_REPOSITORY / "refs" / "main"
        ref.write_text("0" * 40, encoding="utf-8")
        with self.assertRaisesRegex(CHECKER_MODULE.GateFailure, "expected"):
            CHECKER_MODULE.verify_model(root, manifest_path)

    @unittest.skipUnless(os.name == "posix", "model cache symlink checks require POSIX")
    def test_symlinked_model_components_are_rejected(self) -> None:
        cases = ("models", "repository", "refs", "main", "snapshots", "snapshot", "onnx", "artifact")
        for case in cases:
            with self.subTest(component=case):
                root, manifest_path, _ = self.fixture()
                models = root / "models"
                repository = models / CHECKER_MODULE.MODEL_CACHE_REPOSITORY
                refs = repository / "refs"
                snapshots = repository / "snapshots"
                snapshot = snapshots / CHECKER_MODULE.MODEL_REVISION
                if case == "models":
                    outside = root / "outside-models"
                    models.rename(outside)
                    models.symlink_to(outside, target_is_directory=True)
                elif case == "repository":
                    outside = root / "outside-repository"
                    repository.rename(outside)
                    repository.symlink_to(outside, target_is_directory=True)
                elif case == "refs":
                    outside = root / "outside-refs"
                    refs.rename(outside)
                    refs.symlink_to(outside, target_is_directory=True)
                elif case == "main":
                    outside = root / "outside-main"
                    outside.write_text(CHECKER_MODULE.MODEL_REVISION, encoding="utf-8")
                    (refs / "main").unlink()
                    (refs / "main").symlink_to(outside)
                elif case == "snapshots":
                    outside = root / "outside-snapshots"
                    snapshots.rename(outside)
                    snapshots.symlink_to(outside, target_is_directory=True)
                elif case == "snapshot":
                    outside = root / "outside-snapshot"
                    snapshot.rename(outside)
                    snapshot.symlink_to(outside, target_is_directory=True)
                elif case == "onnx":
                    outside = root / "outside-onnx"
                    onnx = snapshot / "onnx"
                    onnx.rename(outside)
                    onnx.symlink_to(outside, target_is_directory=True)
                else:
                    artifact = snapshot / "tokenizer.json"
                    outside = root / "outside-tokenizer.json"
                    outside.write_bytes(artifact.read_bytes())
                    artifact.unlink()
                    artifact.symlink_to(outside)
                with self.assertRaises(CHECKER_MODULE.GateFailure):
                    CHECKER_MODULE.verify_model(root, manifest_path)


if __name__ == "__main__":
    unittest.main()
