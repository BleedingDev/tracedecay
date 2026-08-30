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
HANDSHAKE_CONTRACT = (
    REPO / "product/contracts/memory-provider-v1/provider-handshake-contract.json"
)
CONTRACT_SET = REPO / "product/contracts/memory-provider-v1/contract-set.json"
TEMP_ROOT = REPO / ".beads" / "rust-bindings-test-tmp"

LIMIT_ROWS = [
    ("request_bytes", 16_777_216, "bytes", "items"),
    ("response_bytes", 33_554_432, "bytes", "items"),
    ("observation_batch_items", 4_096, "items", "bytes"),
    ("recall_candidates", 10_000, "items", "bytes"),
    ("concurrent_operations", 1_024, "operations", "items"),
    ("operation_millis", 3_600_000, "milliseconds", "operations"),
    ("snapshot_bytes", 1_073_741_824, "bytes", "items"),
    ("inspection_items", 100_000, "items", "bytes"),
]


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

    def mutate_generated_limit(
        self,
        source: str,
        limit_id: str,
        field: str,
        old_value: str,
        new_value: str,
    ) -> str:
        start = source.index(f'        limit_id: "{limit_id}",')
        end = source.index("    },", start)
        row = source[start:end]
        old = f"        {field}: {old_value},"
        self.assertEqual(row.count(old), 1)
        changed = row.replace(old, f"        {field}: {new_value},", 1)
        return source[:start] + changed + source[end:]

    def run_generator_with_limit_mutation(
        self, limit_id: str, field: str, value: object
    ) -> subprocess.CompletedProcess[str]:
        with tempfile.TemporaryDirectory(dir=TEMP_ROOT) as temp_dir:
            root = Path(temp_dir)
            handshake = json.loads(HANDSHAKE_CONTRACT.read_text(encoding="utf-8"))
            rows = handshake["limit_catalog"]
            row = next(item for item in rows if item["id"] == limit_id)
            row[field] = value
            handshake_path = root / "provider-handshake-contract.json"
            handshake_path.write_text(
                json.dumps(handshake, separators=(",", ":"), sort_keys=True) + "\n",
                encoding="utf-8",
            )
            contract_set = json.loads(CONTRACT_SET.read_text(encoding="utf-8"))
            contract_set["contracts"][1]["contract_path"] = str(
                handshake_path.relative_to(REPO)
            )
            contract_set_path = root / "contract-set.json"
            contract_set_path.write_text(
                json.dumps(contract_set, separators=(",", ":"), sort_keys=True) + "\n",
                encoding="utf-8",
            )
            return subprocess.run(
                [
                    "python3",
                    str(GENERATOR),
                    "--repo",
                    str(REPO),
                    "--contract-set",
                    str(contract_set_path.relative_to(REPO)),
                    "--output-dir",
                    str((root / "generated").relative_to(REPO)),
                    "--check",
                ],
                check=False,
                capture_output=True,
                text=True,
            )

    def assert_generator_rejected_limit_mutation(
        self, limit_id: str, field: str, value: object, marker: str
    ) -> None:
        result = self.run_generator_with_limit_mutation(limit_id, field, value)
        self.assertNotEqual(result.returncode, 0, result.stdout + result.stderr)
        receipt = json.loads(result.stdout)
        self.assertFalse(receipt["ok"])
        self.assertIn(marker, receipt["error"])

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

    def test_structured_terminal_wire_type_cannot_disappear(self) -> None:
        self.assert_rejected(
            "generated Rust source is missing types",
            mutate_source=lambda source: source.replace(
                "pub struct CommittedEffectEvidence<'a>",
                "struct CommittedEffectEvidence<'a>",
                1,
            ),
        )

    def test_terminal_policy_constant_cannot_disappear(self) -> None:
        self.assert_rejected(
            "generated Rust source is missing constants",
            mutate_source=lambda source: source.replace(
                "pub const TERMINAL_CODE_POLICIES",
                "const TERMINAL_CODE_POLICIES",
                1,
            ),
        )

    def test_terminal_policy_semantic_drift_fails_compile_probe(self) -> None:
        anchor = """        terminal_code: TerminalCode::Success,
        effect_expectation: CommittedEffectExpectation::OperationSpecific,
        maximum_fallback_eligibility: FallbackEligibility::Forbidden,"""
        replacement = anchor.replace(
            "FallbackEligibility::Forbidden",
            "FallbackEligibility::ExplicitPolicyOnly",
        )
        self.assert_rejected(
            "terminal code policy table drifted",
            mutate_source=lambda source: source.replace(anchor, replacement, 1),
        )

    def test_terminal_text_limit_semantic_drift_fails_compile_probe(self) -> None:
        anchor = "pub const TERMINAL_DIAGNOSTIC_ID_MAX_BYTES: usize = 128;"
        self.assert_rejected(
            "terminal text limit catalog drifted",
            mutate_source=lambda source: source.replace(
                anchor,
                "pub const TERMINAL_DIAGNOSTIC_ID_MAX_BYTES: usize = 129;",
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

    def test_exact_scope_digest_constant_cannot_disappear(self) -> None:
        self.assert_rejected(
            "generated Rust source is missing constants",
            mutate_source=lambda source: source.replace(
                "pub const EXACT_SCOPE_DIGEST_DOMAIN",
                "const EXACT_SCOPE_DIGEST_DOMAIN",
                1,
            ),
        )

    def test_each_provider_limit_minimum_drift_fails_compile_probe(self) -> None:
        for limit_id, _maximum, _unit, _alternate_unit in LIMIT_ROWS:
            with self.subTest(limit_id=limit_id):
                self.assert_rejected(
                    "provider limit catalog drifted",
                    mutate_source=lambda source, current=limit_id: (
                        self.mutate_generated_limit(
                            source, current, "minimum", "1", "2"
                        )
                    ),
                )

    def test_each_provider_limit_unit_drift_fails_compile_probe(self) -> None:
        for limit_id, _maximum, unit, alternate_unit in LIMIT_ROWS:
            with self.subTest(limit_id=limit_id):
                self.assert_rejected(
                    "provider limit catalog drifted",
                    mutate_source=lambda source, current=limit_id, old=unit, new=alternate_unit: (
                        self.mutate_generated_limit(
                            source,
                            current,
                            "unit",
                            json.dumps(old),
                            json.dumps(new),
                        )
                    ),
                )

    def test_each_provider_limit_maximum_drift_fails_compile_probe(self) -> None:
        for limit_id, maximum, _unit, _alternate_unit in LIMIT_ROWS:
            with self.subTest(limit_id=limit_id):
                self.assert_rejected(
                    "provider limit catalog drifted",
                    mutate_source=lambda source, current=limit_id, old=maximum: (
                        self.mutate_generated_limit(
                            source,
                            current,
                            "maximum",
                            str(old),
                            str(old - 1),
                        )
                    ),
                )

    def test_generator_rejects_each_schema_valid_limit_identity_drift(self) -> None:
        for limit_id, _maximum, _unit, alternate_unit in LIMIT_ROWS:
            with self.subTest(limit_id=limit_id, field="minimum"):
                self.assert_generator_rejected_limit_mutation(
                    limit_id,
                    "minimum",
                    2,
                    "provider limit catalog order, minimum, or unit drifted",
                )
            with self.subTest(limit_id=limit_id, field="unit"):
                self.assert_generator_rejected_limit_mutation(
                    limit_id,
                    "unit",
                    alternate_unit,
                    "provider limit catalog order, minimum, or unit drifted",
                )

    def test_generator_rejects_zero_for_each_limit_bound(self) -> None:
        for limit_id, _maximum, _unit, _alternate_unit in LIMIT_ROWS:
            with self.subTest(limit_id=limit_id, field="minimum"):
                self.assert_generator_rejected_limit_mutation(
                    limit_id,
                    "minimum",
                    0,
                    f"provider limit {limit_id} minimum must be a positive u64",
                )
            with self.subTest(limit_id=limit_id, field="maximum"):
                self.assert_generator_rejected_limit_mutation(
                    limit_id,
                    "maximum",
                    0,
                    f"provider limit {limit_id} maximum must be a positive u64",
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
