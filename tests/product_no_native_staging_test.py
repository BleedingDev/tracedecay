#!/usr/bin/env python3
"""Fixture tests for the production Native staging guard."""

from __future__ import annotations

import importlib.util
import json
import subprocess
import tempfile
import unittest
from pathlib import Path
from types import ModuleType


REPO = Path(__file__).resolve().parents[1]
CHECKER = REPO / "scripts/product/check-no-native-staging.py"
FIXTURE_PATH = (
    "crates/tracedecay-daemon-service/src/retained_owner/native_provider.rs"
)


def load_checker() -> ModuleType:
    spec = importlib.util.spec_from_file_location("no_native_staging_checker", CHECKER)
    if spec is None or spec.loader is None:
        raise RuntimeError("cannot load Native staging checker")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


CHECKER_MODULE = load_checker()

VALID_SOURCE = r'''
pub(crate) fn native_recall() -> Result<()> {
    canonical_memory_authority::recall()
}

#[cfg(test)]
mod tests {
    use super::*;

    // Test fixtures may document and exercise the retired path without
    // shipping it in production.
    let _ = StagedObservationStore::open(root);
    let _ = "staged-observations-v1.sqlite3";
    let _ = SqliteObservationJournal::open_existing(path);
    fn backfill_test_fixture() {}
}
'''


class NoNativeStagingTest(unittest.TestCase):
    def run_checker(self, files: dict[str, str]) -> subprocess.CompletedProcess[str]:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            for relative, contents in files.items():
                path = root / relative
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_text(contents, encoding="utf-8")
            return subprocess.run(
                ["python3", str(CHECKER), "--repo", str(root)],
                check=False,
                capture_output=True,
                text=True,
            )

    def assert_clean(self, source: str) -> None:
        result = self.run_checker({FIXTURE_PATH: source})
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        receipt = json.loads(result.stdout)
        self.assertTrue(receipt["ok"])
        self.assertEqual(receipt["checked_files"], 1)

    def assert_rejected(self, source: str, marker: str) -> None:
        result = self.run_checker({FIXTURE_PATH: source})
        self.assertNotEqual(result.returncode, 0, result.stdout + result.stderr)
        receipt = json.loads(result.stdout)
        self.assertFalse(receipt["ok"])
        self.assertIn(marker, "\n".join(receipt["errors"]))

    def test_real_checker_helpers_keep_scope_explicit(self) -> None:
        relative = Path(FIXTURE_PATH)
        self.assertTrue(CHECKER_MODULE.is_native_production_path(relative, Path(".")))
        self.assertFalse(
            CHECKER_MODULE.is_native_production_path(
                Path("crates/tracedecay-daemon-service/src/retained_owner/native_provider_tests.rs"),
                Path("."),
            )
        )

    def test_test_module_can_keep_retired_fixture_examples(self) -> None:
        self.assert_clean(VALID_SOURCE)

    def test_staged_store_type_is_rejected(self) -> None:
        self.assert_rejected(
            "pub fn production() { let _ = StagedObservationStore::open(root); }\n",
            "retired Native staging type reference",
        )

    def test_staged_database_filename_is_rejected(self) -> None:
        self.assert_rejected(
            'pub const FILE: &str = "staged-observations-v1.sqlite3";\n',
            "retired Native staging database filename",
        )

    def test_provider_schema_migration_is_rejected(self) -> None:
        self.assert_rejected(
            "pub fn migrate_provider_schema() {}\n",
            "provider-local schema migration/backfill",
        )

    def test_provider_backfill_is_rejected(self) -> None:
        self.assert_rejected(
            "pub fn backfill_provider_rows() {}\n",
            "provider-local schema migration/backfill",
        )

    def test_provider_schema_ddl_is_rejected(self) -> None:
        self.assert_rejected(
            'pub const DDL: &str = "CREATE TABLE provider_rows(id INTEGER)";\n',
            "provider-local schema migration/backfill",
        )

    def test_direct_observation_journal_reopen_is_rejected(self) -> None:
        self.assert_rejected(
            "pub fn reopen() { let _ = SqliteObservationJournal::open_existing(path); }\n",
            "direct observation journal open_existing",
        )

    def test_test_file_is_not_a_production_native_path(self) -> None:
        result = self.run_checker(
            {
                FIXTURE_PATH: "pub fn production() {}\n",
                FIXTURE_PATH.replace("native_provider.rs", "native_provider_tests.rs"): (
                    "pub fn fixture() { let _ = StagedObservationStore::open(root); }\n"
                )
            }
        )
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        receipt = json.loads(result.stdout)
        self.assertTrue(receipt["ok"])
        self.assertEqual(receipt["checked_files"], 1)


if __name__ == "__main__":
    unittest.main()
