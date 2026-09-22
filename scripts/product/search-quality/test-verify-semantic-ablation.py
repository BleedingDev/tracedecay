#!/usr/bin/env python3
"""Mutation tests for the semantic-ablation evidence verifier.

These tests exercise the verifier with tampered evaluator inputs and lifecycle
evidence. They intentionally do not construct a ranking oracle or provide
labels to any production execution path.
"""

from __future__ import annotations

import argparse
import copy
import importlib.util
from pathlib import Path
import sys
import unittest


SCRIPT = Path(__file__).with_name("verify-semantic-ablation.py")
REPO = SCRIPT.parents[3]
spec = importlib.util.spec_from_file_location("verify_semantic_ablation", SCRIPT)
if spec is None or spec.loader is None:
    raise RuntimeError(f"cannot load {SCRIPT}")
MODULE = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = MODULE
spec.loader.exec_module(MODULE)


def fixture_inputs() -> tuple[dict[str, object], dict[str, object], dict[str, object], bytes, bytes]:
    paths = MODULE.default_paths(REPO)
    return MODULE.load_inputs(argparse.Namespace(**paths))


class SemanticAblationVerifierMutationTest(unittest.TestCase):
    def setUp(self) -> None:
        self.workload, self.labels, self.artifact, self.corpus, self.manifest = fixture_inputs()

    def assert_inputs_rejected(self, *, workload=None, artifact=None, corpus=None, manifest=None) -> None:
        with self.assertRaises(ValueError):
            MODULE.validate_inputs(
                workload if workload is not None else self.workload,
                self.labels,
                artifact if artifact is not None else self.artifact,
                corpus if corpus is not None else self.corpus,
                manifest if manifest is not None else self.manifest,
            )

    def test_artifact_cannot_smuggle_fake_or_hardcoded_rankings(self) -> None:
        artifact = copy.deepcopy(self.artifact)
        artifact["rankings"] = {"train-authorization-001": ["r9v2k7"]}
        self.assert_inputs_rejected(artifact=artifact)

    def test_wrong_model_manifest_and_identity_are_rejected(self) -> None:
        self.assert_inputs_rejected(manifest=self.manifest.replace(b"JinaEmbeddingsV2BaseCode", b"OtherModel"))
        artifact = copy.deepcopy(self.artifact)
        artifact["identity"]["model_id"] = "OtherModel"
        self.assert_inputs_rejected(artifact=artifact)

    def test_corpus_bytes_and_candidate_metadata_are_bound(self) -> None:
        self.assert_inputs_rejected(corpus=self.corpus + b"\nforged")
        workload = copy.deepcopy(self.workload)
        workload["candidates"][0]["source_path"] += ".forged"
        self.assert_inputs_rejected(workload=workload)

    def test_duplicate_candidate_identity_is_rejected(self) -> None:
        workload = copy.deepcopy(self.workload)
        workload["candidates"][1]["candidate_id"] = workload["candidates"][0]["candidate_id"]
        self.assert_inputs_rejected(workload=workload)

    def test_missing_or_extra_repetition_is_rejected(self) -> None:
        for count in (MODULE.REPETITIONS - 1, MODULE.REPETITIONS + 1):
            workload = copy.deepcopy(self.workload)
            workload["repetition_contract"]["cold"] = count
            self.assert_inputs_rejected(workload=workload)

    @staticmethod
    def lifecycle(kind: str, runtime_identity: str) -> dict[str, object]:
        return {
            "phase": kind,
            "runtime_identity": runtime_identity,
            "cache_reset": kind != "warm",
            "runtime_restart": kind == "restart",
            "lifecycle_receipt_digest": "sha256:" + "0" * 64,
        }

    def test_missing_lifecycle_population_is_rejected(self) -> None:
        runs = [
            {
                "mode": "semantic_only",
                "query_id": "validation-authorization-001",
                "repetition_kind": "cold",
                "lifecycle": self.lifecycle("cold", "sha256:" + "1" * 64),
            },
            {
                "mode": "semantic_only",
                "query_id": "validation-authorization-001",
                "repetition_kind": "restart",
                "lifecycle": self.lifecycle("restart", "sha256:" + "2" * 64),
            },
        ]
        with self.assertRaisesRegex(ValueError, "warm lifecycle"):
            MODULE.validate_lifecycle_matrix(runs)

    def test_restart_reuse_is_rejected(self) -> None:
        runs = [
            {
                "mode": "semantic_only",
                "query_id": "validation-authorization-001",
                "repetition_kind": "cold",
                "lifecycle": self.lifecycle("cold", "sha256:" + "1" * 64),
            },
            {
                "mode": "semantic_only",
                "query_id": "validation-authorization-001",
                "repetition_kind": "warm",
                "lifecycle": self.lifecycle("warm", "sha256:" + "1" * 64),
            },
            {
                "mode": "semantic_only",
                "query_id": "validation-authorization-001",
                "repetition_kind": "restart",
                "lifecycle": self.lifecycle("restart", "sha256:" + "1" * 64),
            },
        ]
        with self.assertRaisesRegex(ValueError, "restart identity was reused"):
            MODULE.validate_lifecycle_matrix(runs)


if __name__ == "__main__":
    raise SystemExit(unittest.main())
