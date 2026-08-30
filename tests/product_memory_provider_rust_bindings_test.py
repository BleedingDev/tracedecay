#!/usr/bin/env python3
"""Contract tests for generated Memory Provider V1 Rust bindings."""

from __future__ import annotations

import json
import shutil
import subprocess
import tempfile
import unittest
from pathlib import Path
from typing import Callable

REPO = Path(__file__).resolve().parents[1]
GENERATED = (
    REPO
    / "product/contracts/memory-provider-v1/generated/rust/memory_provider_v1.rs"
)
MANIFEST = REPO / "product/contracts/memory-provider-v1/generated/rust/manifest.json"
GENERATOR = REPO / "scripts/product/generate-memory-provider-rust.py"
CHECKER = REPO / "scripts/product/check-memory-provider-rust-bindings.py"
TEMP_ROOT = REPO / ".beads" / "rust-bindings-test-tmp"


class MemoryProviderRustBindingsTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        TEMP_ROOT.mkdir(parents=True, exist_ok=True)

    @classmethod
    def tearDownClass(cls) -> None:
        if TEMP_ROOT.exists():
            shutil.rmtree(TEMP_ROOT)

    def run_checker(
        self,
        *,
        mutate_source: Callable[[str], str] | None = None,
        mutate_manifest: Callable[[dict[str, object]], None] | None = None,
        duplicate_source: str | None = None,
    ) -> subprocess.CompletedProcess[str]:
        if mutate_source is None and mutate_manifest is None and duplicate_source is None:
            return subprocess.run(
                [
                    "python3",
                    str(CHECKER),
                    "--repo",
                    str(REPO),
                    "--generated",
                    str(GENERATED),
                    "--manifest",
                    str(MANIFEST),
                ],
                check=False,
                capture_output=True,
                text=True,
            )

        with tempfile.TemporaryDirectory(dir=TEMP_ROOT) as temp_dir:
            root = Path(temp_dir)
            generated = root / "memory_provider_v1.rs"
            manifest_path = root / "manifest.json"
            source = GENERATED.read_text(encoding="utf-8")
            if mutate_source is not None:
                source = mutate_source(source)
            generated.write_text(source, encoding="utf-8")
            manifest = json.loads(MANIFEST.read_text(encoding="utf-8"))
            manifest["output_path"] = str(generated.relative_to(REPO))
            if mutate_manifest is not None:
                mutate_manifest(manifest)
            manifest_path.write_text(
                json.dumps(manifest, separators=(",", ":"), sort_keys=True) + "\n",
                encoding="utf-8",
            )
            argv = [
                "python3",
                str(CHECKER),
                "--repo",
                str(REPO),
                "--generated",
                str(generated),
                "--manifest",
                str(manifest_path),
            ]
            if duplicate_source is not None:
                scan_root = root / "scan"
                scan_root.mkdir()
                (scan_root / "duplicate.rs").write_text(
                    duplicate_source,
                    encoding="utf-8",
                )
                argv.extend(["--scan-root", str(scan_root)])
            return subprocess.run(
                argv,
                check=False,
                capture_output=True,
                text=True,
            )

    def assert_rejected(
        self,
        marker: str,
        *,
        mutate_source: Callable[[str], str] | None = None,
        mutate_manifest: Callable[[dict[str, object]], None] | None = None,
        duplicate_source: str | None = None,
    ) -> None:
        result = self.run_checker(
            mutate_source=mutate_source,
            mutate_manifest=mutate_manifest,
            duplicate_source=duplicate_source,
        )
        self.assertNotEqual(result.returncode, 0, result.stdout + result.stderr)
        receipt = json.loads(result.stdout)
        self.assertFalse(receipt["ok"])
        self.assertIn(marker, "\n".join(receipt["errors"]))

    def test_real_generated_bindings_compile_and_validate(self) -> None:
        result = self.run_checker()
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        receipt = json.loads(result.stdout)
        self.assertTrue(receipt["ok"])
        self.assertEqual(
            receipt["manifest_id"],
            "tracedecay.memory.provider.generated-rust.manifest.v1",
        )
        self.assertEqual(
            receipt["contract_set_id"],
            "tracedecay.memory.provider.contract-set.v1",
        )
        self.assertEqual(receipt["contract_count"], 6)
        self.assertGreaterEqual(receipt["capability_count"], 4)
        self.assertEqual(receipt["mandatory_capability_count"], 3)
        self.assertGreaterEqual(receipt["optional_capability_count"], 1)
        self.assertEqual(receipt["rustc_compile"], "passed")
        self.assertEqual(receipt["probe_execution"], "passed")
        self.assertRegex(receipt["output_sha256"], r"^[0-9a-f]{64}$")

    def test_generator_check_is_deterministic_and_non_mutating(self) -> None:
        before = {
            path.name: path.read_bytes()
            for path in sorted(GENERATED.parent.iterdir())
            if path.is_file()
        }
        result = subprocess.run(
            [
                "python3",
                str(GENERATOR),
                "--repo",
                str(REPO),
                "--check",
            ],
            check=False,
            capture_output=True,
            text=True,
        )
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        after = {
            path.name: path.read_bytes()
            for path in sorted(GENERATED.parent.iterdir())
            if path.is_file()
        }
        self.assertEqual(before, after)

    def test_generated_header_is_required(self) -> None:
        self.assert_rejected(
            "generated Rust source lacks canonical generated header",
            mutate_source=lambda source: source.replace(
                "// @generated by scripts/product/generate-memory-provider-rust.py; DO NOT EDIT.\n",
                "",
                1,
            ),
        )

    def test_unsafe_code_is_rejected(self) -> None:
        self.assert_rejected(
            "generated Rust source contains forbidden unsafe block",
            mutate_source=lambda source: source + "\npub fn bad() { unsafe {} }\n",
        )

    def test_unwrap_is_rejected(self) -> None:
        self.assert_rejected(
            "generated Rust source contains forbidden unwrap",
            mutate_source=lambda source: source
            + "\npub fn bad(value: Option<u8>) -> u8 { value.unwrap() }\n",
        )

    def test_required_wire_type_cannot_disappear(self) -> None:
        self.assert_rejected(
            "generated Rust source is missing types",
            mutate_source=lambda source: source.replace(
                "pub struct TerminalSummary<'a>",
                "struct TerminalSummary<'a>",
                1,
            ),
        )

    def test_required_field_constant_cannot_disappear(self) -> None:
        self.assert_rejected(
            "generated Rust source is missing constants",
            mutate_source=lambda source: source.replace(
                "pub const OBSERVATION_REQUIRED_FIELDS",
                "const OBSERVATION_REQUIRED_FIELDS",
                1,
            ),
        )

    def test_manifest_output_digest_is_verified(self) -> None:
        def mutate(manifest: dict[str, object]) -> None:
            manifest["output_sha256"] = "0" * 64

        self.assert_rejected(
            "generated Rust output SHA-256 drifted",
            mutate_manifest=mutate,
        )

    def test_manifest_generator_digest_is_verified(self) -> None:
        def mutate(manifest: dict[str, object]) -> None:
            manifest["generator_sha256"] = "0" * 64

        self.assert_rejected(
            "generated Rust generator SHA-256 drifted",
            mutate_manifest=mutate,
        )

    def test_manifest_contract_set_digest_is_verified(self) -> None:
        def mutate(manifest: dict[str, object]) -> None:
            manifest["contract_set_sha256"] = "0" * 64

        self.assert_rejected(
            "generated Rust contract-set SHA-256 drifted",
            mutate_manifest=mutate,
        )

    def test_duplicate_hand_maintained_wire_type_is_rejected(self) -> None:
        self.assert_rejected(
            "duplicate hand-maintained wire/domain type declarations found",
            duplicate_source=(
                "pub struct MemoryProviderTerminalEnvelopeV1;\n"
                "pub enum TerminalCode { Success }\n"
            ),
        )

    def test_duplicate_scan_ignores_unrelated_types(self) -> None:
        result = self.run_checker(
            duplicate_source="pub struct UnrelatedProductType;\n"
        )
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)

    def test_manifest_fields_are_closed(self) -> None:
        def mutate(manifest: dict[str, object]) -> None:
            manifest["surprise"] = True

        self.assert_rejected(
            "generated Rust manifest fields drifted",
            mutate_manifest=mutate,
        )

    def test_generated_file_drift_is_rejected_by_generator(self) -> None:
        self.assert_rejected(
            "generated Rust output SHA-256 drifted",
            mutate_source=lambda source: source + "// manual edit\n",
        )


if __name__ == "__main__":
    unittest.main()
