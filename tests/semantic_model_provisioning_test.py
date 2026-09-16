"""Focused offline contract tests for the V2 code-semantic model authority."""

from __future__ import annotations

import hashlib
import json
import os
import socket
import stat
import sys
import tempfile
import unittest
from pathlib import Path

# Match the product scripts when this file is executed directly from another
# working directory (or with ``python -S``).
ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import scripts.product.semantic.provisioning as provisioning  # noqa: E402

from scripts.product.semantic.provisioning import (  # noqa: E402
    JOURNAL_SCHEMA,
    MANIFEST_SCHEMA,
    RECEIPT_SCHEMA,
    UNINSTALL_JOURNAL_SCHEMA,
    JournalValidationError,
    ManifestValidationError,
    SemanticProvisioningError,
    SimulatedInterruption,
    UnsafePathError,
    VerificationError,
    artifact_digest,
    install_offline,
    _install_offline_core,
    _recover_install_core,
    _rollback_install_core,
    _uninstall_offline_core,
    _validate_receipt_core,
    _verify_directory_core,
    _verify_installation_core,
    _atomic_write_json,
    journal_path_for,
    parse_manifest,
    receipt_path_for,
    rollback_path_for,
    rollback_receipt_path_for,
    uninstall_journal_path_for,
    staging_path_for,
    validate_receipt,
    verify_directory,
    load_receipt,
)


def _tiny_manifest() -> dict[str, object]:
    payloads = {
        "model": b"tiny model bytes\n",
        "tokenizer": b"tiny tokenizer\n",
        "config": b"{}\n",
        "special_tokens_map": b"{}\n",
        "tokenizer_config": b"{}\n",
    }
    members = {
        role: {
            "path": f"{role}.bin",
            "upstream_path": f"upstream/{role}.bin",
            "length": len(data),
            "sha256": hashlib.sha256(data).hexdigest(),
        }
        for role, data in payloads.items()
    }
    document: dict[str, object] = {
        "schema": MANIFEST_SCHEMA,
        "model": "TestSemanticModel",
        "source": {
            "upstream": "https://example.invalid/test-semantic-model",
            "revision": "a" * 40,
            "license": "Apache-2.0",
            "license_url": "https://www.apache.org/licenses/LICENSE-2.0",
            "provenance": "https://example.invalid/test-semantic-model/tree/" + "a" * 40,
        },
        "expected_dimensions": 3,
        "max_length": 16,
        "members": members,
    }
    document["artifact_digest"] = artifact_digest(
        {**document, "artifact_digest": "0" * 64}
    )
    return document


def _write_fixture(root: Path, document: dict[str, object] | None = None) -> dict[str, object]:
    document = document or _tiny_manifest()
    root.mkdir(parents=True)
    (root / "fixture.json").write_text(
        json.dumps(document, indent=2, sort_keys=False) + "\n", encoding="utf-8"
    )
    members = document["members"]
    assert isinstance(members, dict)
    for role, member in members.items():
        assert isinstance(member, dict)
        path = root / member["path"]
        path.write_bytes((f"{role} payload\n").encode("utf-8"))
        # Rewrite declarations to match the intentionally small test payload.
        data = path.read_bytes()
        member["length"] = len(data)
        member["sha256"] = hashlib.sha256(data).hexdigest()
    document["artifact_digest"] = artifact_digest(document)
    (root / "fixture.json").write_text(
        json.dumps(document, indent=2, sort_keys=False) + "\n", encoding="utf-8"
    )
    return document


class SemanticModelProvisioningTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory(prefix="semantic-provisioning-")
        self.root = Path(self.temp.name)
        self.source = self.root / "source"
        self.target = self.root / "published"
        self.document = _write_fixture(self.source)
        self.manifest = parse_manifest(self.document, require_pinned=False)

    def tearDown(self) -> None:
        self.temp.cleanup()

    def test_complete_receipt_and_idempotent_reinstall(self) -> None:
        first = _install_offline_core(self.source, self.target, manifest=self.manifest)
        self.assertEqual(first.status, "installed")
        self.assertEqual(first.receipt, receipt_path_for(self.target))
        evidence = _verify_installation_core(self.target, self.manifest)
        self.assertEqual(evidence.member_count, 5)
        self.assertEqual(evidence.artifact_digest, artifact_digest(self.manifest))
        receipt = load_receipt(first.receipt)
        self.assertEqual(receipt["schema"], RECEIPT_SCHEMA)
        self.assertEqual(receipt["target"], str(self.target.resolve()))
        _validate_receipt_core(receipt, self.target, self.manifest)
        self.assertFalse(journal_path_for(self.target).exists())
        self.assertFalse(staging_path_for(self.target).exists())

        second = _install_offline_core(self.source, self.target, manifest=self.manifest)
        self.assertEqual(second.status, "already_installed")
        self.assertEqual(second.artifact_digest, first.artifact_digest)

    def test_corrupt_published_bytes_are_rejected_and_left_in_place(self) -> None:
        _install_offline_core(self.source, self.target, manifest=self.manifest)
        corrupted = self.target / self.manifest.members["model"]["path"]
        corrupted.write_bytes(b"corrupt")
        with self.assertRaises(VerificationError):
            _verify_installation_core(self.target, self.manifest)
        with self.assertRaises(VerificationError):
            _install_offline_core(self.source, self.target, manifest=self.manifest)
        self.assertEqual(corrupted.read_bytes(), b"corrupt")

    def test_interrupted_complete_stage_recovers_without_source_or_network(self) -> None:
        with self.assertRaises(SimulatedInterruption):
            _install_offline_core(
                self.source,
                self.target,
                manifest=self.manifest,
                interrupt_after="staged",
            )
        self.assertFalse(self.target.exists())
        self.assertTrue(staging_path_for(self.target).is_dir())
        journal = json.loads(journal_path_for(self.target).read_text(encoding="utf-8"))
        self.assertEqual(journal["schema"], JOURNAL_SCHEMA)
        recovered = _recover_install_core(self.target, manifest=self.manifest)
        self.assertEqual(recovered.status, "recovered")
        _verify_installation_core(self.target, self.manifest)

    def test_stage_directory_is_fsynced_before_staged_journal(self) -> None:
        events: list[tuple[str, object]] = []
        original_fsync = provisioning._fsync_directory
        original_write = provisioning._write_journal

        def record_fsync(path: Path, **kwargs: object) -> None:
            events.append(("fsync", path))
            original_fsync(path, **kwargs)  # type: ignore[arg-type]

        def record_journal(path: Path, journal: object) -> None:
            if isinstance(journal, dict) and journal.get("phase") == "staged":
                events.append(("staged", path))
            original_write(path, journal)  # type: ignore[arg-type]

        provisioning._fsync_directory = record_fsync  # type: ignore[assignment]
        provisioning._write_journal = record_journal  # type: ignore[assignment]
        try:
            with self.assertRaises(SimulatedInterruption):
                _install_offline_core(
                    self.source,
                    self.target,
                    manifest=self.manifest,
                    interrupt_after="staged",
                )
        finally:
            provisioning._fsync_directory = original_fsync
            provisioning._write_journal = original_write
        staged_index = next(index for index, event in enumerate(events) if event[0] == "staged")
        self.assertTrue(
            any(
                index < staged_index and event == ("fsync", staging_path_for(self.target))
                for index, event in enumerate(events)
            )
        )

    def test_interrupted_member_copy_resumes_from_explicit_source(self) -> None:
        with self.assertRaises(SimulatedInterruption):
            _install_offline_core(
                self.source,
                self.target,
                manifest=self.manifest,
                interrupt_after="member",
            )
        self.assertFalse(self.target.exists())
        resumed = _install_offline_core(self.source, self.target, manifest=self.manifest)
        self.assertEqual(resumed.status, "installed")
        _verify_installation_core(self.target, self.manifest)

    def test_interrupted_after_publish_recovers_receipt(self) -> None:
        with self.assertRaises(SimulatedInterruption):
            _install_offline_core(
                self.source,
                self.target,
                manifest=self.manifest,
                interrupt_after="published",
            )
        self.assertTrue(self.target.is_dir())
        self.assertFalse(receipt_path_for(self.target).exists())
        self.assertTrue(journal_path_for(self.target).is_file())
        recovered = _recover_install_core(self.target, manifest=self.manifest)
        self.assertEqual(recovered.status, "recovered")
        _verify_installation_core(self.target, self.manifest)

    def test_manifest_rejects_traversal_and_duplicate_inventory(self) -> None:
        traversal = json.loads(json.dumps(self.document))
        traversal["members"]["model"]["path"] = "../outside.bin"
        with self.assertRaises(ManifestValidationError):
            parse_manifest(traversal, require_pinned=False)

        duplicate = json.loads(json.dumps(self.document))
        duplicate["members"]["tokenizer"]["path"] = duplicate["members"]["model"]["path"]
        with self.assertRaises(ManifestValidationError):
            parse_manifest(duplicate, require_pinned=False)

    def test_source_hard_link_is_rejected(self) -> None:
        alias = self.source / "model-alias.bin"
        os.link(self.source / self.manifest.members["model"]["path"], alias)
        with self.assertRaises(UnsafePathError):
            _verify_directory_core(self.source, self.manifest)

    @unittest.skipUnless(hasattr(os, "symlink"), "symlinks unavailable")
    def test_source_symlink_is_rejected(self) -> None:
        outside = self.root / "outside.bin"
        outside.write_bytes(b"outside")
        model_path = self.source / self.manifest.members["model"]["path"]
        model_path.unlink()
        os.symlink(outside, model_path)
        with self.assertRaises(UnsafePathError):
            _verify_directory_core(self.source, self.manifest)

    @unittest.skipUnless(hasattr(os, "symlink"), "symlinks unavailable")
    def test_trusted_os_alias_is_resolved_but_internal_symlink_is_rejected(self) -> None:
        os_alias = Path("/var")
        if not os_alias.is_symlink():
            self.skipTest("this host does not expose /var as an OS alias")
        try:
            relative_root = self.root.relative_to(os_alias)
        except ValueError:
            self.skipTest("temporary directory is not under the /var alias")
        aliased_source = os_alias / relative_root / "source"
        aliased_target = os_alias / relative_root / "published-alias"

        result = _install_offline_core(aliased_source, aliased_target, manifest=self.manifest)
        self.assertEqual(result.status, "installed")
        _verify_installation_core(aliased_target, self.manifest)

        internal = aliased_source / "internal"
        internal.mkdir()
        os.symlink(self.root / "outside.bin", internal / "link")
        with self.assertRaises(UnsafePathError):
            _verify_directory_core(aliased_source, self.manifest)

    def test_contract_has_no_query_network_path(self) -> None:
        original_socket = socket.socket

        def fail_socket(*args: object, **kwargs: object) -> None:
            raise AssertionError("semantic model provisioning attempted network access")

        socket.socket = fail_socket  # type: ignore[assignment]
        try:
            result = _install_offline_core(self.source, self.target, manifest=self.manifest)
            self.assertEqual(result.status, "installed")
        finally:
            socket.socket = original_socket

    def test_public_install_rejects_unpinned_or_mutable_manifest_objects(self) -> None:
        with self.assertRaises(ManifestValidationError):
            install_offline(self.source, self.target, manifest=self.manifest)

        constructed = type(self.manifest)(json.loads(json.dumps(self.document)))
        with self.assertRaises(ManifestValidationError):
            install_offline(self.source, self.target, manifest=constructed)

    def test_public_verification_apis_reject_unpinned_manifests(self) -> None:
        _install_offline_core(self.source, self.target, manifest=self.manifest)
        receipt = load_receipt(receipt_path_for(self.target))
        with self.assertRaises(ManifestValidationError):
            verify_directory(self.target, self.manifest)
        with self.assertRaises(ManifestValidationError):
            validate_receipt(receipt, self.target, self.manifest)

    def test_parent_fsync_failure_is_propagated_for_json_journals(self) -> None:
        journal = journal_path_for(self.target)
        original_fsync = provisioning.os.fsync

        def fail_directory_fsync(fd: int) -> None:
            metadata = provisioning.os.fstat(fd)
            if stat.S_ISDIR(metadata.st_mode):
                raise OSError("simulated parent directory fsync failure")
            original_fsync(fd)

        provisioning.os.fsync = fail_directory_fsync  # type: ignore[assignment]
        try:
            with self.assertRaises(SemanticProvisioningError):
                _atomic_write_json(journal, {"phase": "created"})
        finally:
            provisioning.os.fsync = original_fsync

    def test_malformed_journal_phase_is_a_typed_rejection(self) -> None:
        with self.assertRaises(SimulatedInterruption):
            _install_offline_core(
                self.source,
                self.target,
                manifest=self.manifest,
                interrupt_after="staged",
            )
        journal_path = journal_path_for(self.target)
        journal = json.loads(journal_path.read_text(encoding="utf-8"))
        journal["phase"] = []
        journal_path.write_text(json.dumps(journal) + "\n", encoding="utf-8")
        with self.assertRaises(JournalValidationError):
            _recover_install_core(self.target, manifest=self.manifest)

    def test_validated_manifest_is_deeply_immutable(self) -> None:
        with self.assertRaises(TypeError):
            self.manifest.document["model"] = "substituted"  # type: ignore[index]
        with self.assertRaises(TypeError):
            self.manifest.document["members"]["model"]["path"] = "substituted"  # type: ignore[index]

    def test_uninstall_retains_snapshot_and_rollback_restores_it(self) -> None:
        _install_offline_core(self.source, self.target, manifest=self.manifest)
        removed = _uninstall_offline_core(self.target, manifest=self.manifest)
        self.assertEqual(removed.status, "uninstalled")
        self.assertFalse(self.target.exists())
        self.assertFalse(receipt_path_for(self.target).exists())
        self.assertTrue(rollback_path_for(self.target).is_dir())
        self.assertTrue(rollback_receipt_path_for(self.target).is_file())
        journal = json.loads(uninstall_journal_path_for(self.target).read_text(encoding="utf-8"))
        self.assertEqual(journal["schema"], UNINSTALL_JOURNAL_SCHEMA)
        restored = _rollback_install_core(self.target, manifest=self.manifest)
        self.assertEqual(restored.status, "rolled_back")
        _verify_installation_core(self.target, self.manifest)
        self.assertFalse(uninstall_journal_path_for(self.target).exists())
        self.assertFalse(rollback_path_for(self.target).exists())
        self.assertFalse(rollback_receipt_path_for(self.target).exists())

    def test_uninstall_and_rollback_journals_recover_after_each_cutover(self) -> None:
        _install_offline_core(self.source, self.target, manifest=self.manifest)
        with self.assertRaises(SimulatedInterruption):
            _uninstall_offline_core(
                self.target,
                manifest=self.manifest,
                interrupt_after="uninstall-target",
            )
        self.assertFalse(self.target.exists())
        recovered = _uninstall_offline_core(self.target, manifest=self.manifest)
        self.assertEqual(recovered.status, "already_uninstalled")

        with self.assertRaises(SimulatedInterruption):
            _rollback_install_core(
                self.target,
                manifest=self.manifest,
                interrupt_after="rollback-target",
            )
        self.assertTrue(self.target.is_dir())
        self.assertFalse(receipt_path_for(self.target).exists())
        restored = _rollback_install_core(self.target, manifest=self.manifest)
        self.assertEqual(restored.status, "rolled_back")
        _verify_installation_core(self.target, self.manifest)

    @unittest.skipUnless(hasattr(os, "symlink"), "symlinks unavailable")
    def test_source_parent_swap_after_admission_fails_closed(self) -> None:
        source_parent = self.root / "admitted-parent"
        nested_source = source_parent / "source"
        source_parent.mkdir()
        self.source.rename(nested_source)
        replacement = self.root / "replacement-parent"
        replacement.mkdir()
        original_validate = provisioning._validate_source
        moved_parent = self.root / "admitted-parent-moved"

        def validate_then_swap(source: Path, manifest: object) -> object:
            result = original_validate(source, manifest)  # type: ignore[arg-type]
            source_parent.rename(moved_parent)
            os.symlink(replacement, source_parent)
            return result

        provisioning._validate_source = validate_then_swap  # type: ignore[assignment]
        try:
            with self.assertRaises(UnsafePathError):
                _install_offline_core(nested_source, self.target, manifest=self.manifest)
        finally:
            provisioning._validate_source = original_validate
            if source_parent.is_symlink():
                source_parent.unlink()
            if moved_parent.exists():
                moved_parent.rename(source_parent)

    @unittest.skipUnless(hasattr(os, "symlink"), "symlinks unavailable")
    def test_publication_parent_swap_fails_without_touching_foreign_tree(self) -> None:
        publication_parent = self.root / "publication-parent"
        publication_parent.mkdir()
        target = Path(os.path.realpath(publication_parent / "published"))
        moved_parent = self.root / "publication-parent-moved"
        foreign_marker = self.root / "foreign-marker"
        original_atomic = provisioning._atomic_replace
        swapped = False

        def swap_parent(source: Path, destination: Path, **kwargs: object) -> None:
            nonlocal swapped
            if destination == target and not swapped:
                publication_parent.rename(moved_parent)
                publication_parent.mkdir()
                foreign_marker.write_text("foreign", encoding="utf-8")
                swapped = True
            original_atomic(source, destination, **kwargs)  # type: ignore[arg-type]

        provisioning._atomic_replace = swap_parent  # type: ignore[assignment]
        try:
            with self.assertRaises(UnsafePathError):
                _install_offline_core(self.source, target, manifest=self.manifest)
        finally:
            provisioning._atomic_replace = original_atomic
            if foreign_marker.exists():
                foreign_marker.unlink()
            if publication_parent.exists():
                publication_parent.rmdir()
            if moved_parent.exists():
                moved_parent.rename(publication_parent)
        self.assertFalse(target.exists())
        self.assertTrue(swapped)

    @unittest.skipUnless(hasattr(os, "symlink"), "symlinks unavailable")
    def test_staging_symlink_swap_fails_without_publishing_foreign_tree(self) -> None:
        target = Path(os.path.realpath(self.target))
        stage = staging_path_for(target)
        saved_stage = self.root / "saved-stage"
        foreign_stage = self.root / "foreign-stage"
        foreign_stage.mkdir()
        original_atomic = provisioning._atomic_replace
        swapped = False

        def swap_stage(source: Path, destination: Path, **kwargs: object) -> None:
            nonlocal swapped
            if destination == target and not swapped:
                stage.rename(saved_stage)
                os.symlink(foreign_stage, stage)
                swapped = True
            try:
                original_atomic(source, destination, **kwargs)  # type: ignore[arg-type]
            finally:
                if stage.is_symlink():
                    stage.unlink()
                if saved_stage.exists():
                    saved_stage.rename(stage)

        provisioning._atomic_replace = swap_stage  # type: ignore[assignment]
        try:
            with self.assertRaises(UnsafePathError):
                _install_offline_core(self.source, target, manifest=self.manifest)
        finally:
            provisioning._atomic_replace = original_atomic
            if stage.is_symlink():
                stage.unlink()
            if saved_stage.exists():
                saved_stage.rename(stage)
        self.assertFalse(target.exists())
        self.assertTrue(foreign_stage.is_dir())
        self.assertTrue(swapped)

    @unittest.skipUnless(hasattr(os, "symlink"), "symlinks unavailable")
    def test_staging_symlink_swap_during_rename_is_rejected(self) -> None:
        target = Path(os.path.realpath(self.target))
        stage = staging_path_for(target)
        saved_stage = self.root / "saved-stage-during-rename"
        foreign_stage = self.root / "foreign-stage-during-rename"
        foreign_stage.mkdir()
        original_rename = provisioning.os.rename
        original_supports_dir_fd = provisioning.os.supports_dir_fd
        swapped = False

        def swap_during_rename(
            source_name: str,
            destination_name: str,
            *,
            src_dir_fd: int | None = None,
            dst_dir_fd: int | None = None,
        ) -> None:
            nonlocal swapped
            is_publication = source_name == stage.name and destination_name == target.name
            if is_publication and not swapped:
                stage.rename(saved_stage)
                os.symlink(foreign_stage, stage)
                swapped = True
            try:
                original_rename(
                    source_name,
                    destination_name,
                    src_dir_fd=src_dir_fd,
                    dst_dir_fd=dst_dir_fd,
                )
            finally:
                if is_publication:
                    if stage.is_symlink():
                        stage.unlink()
                    if saved_stage.exists():
                        saved_stage.rename(stage)

        provisioning.os.rename = swap_during_rename  # type: ignore[assignment]
        provisioning.os.supports_dir_fd = set(original_supports_dir_fd) | {swap_during_rename}
        try:
            with self.assertRaises(UnsafePathError):
                _install_offline_core(self.source, target, manifest=self.manifest)
        finally:
            provisioning.os.rename = original_rename
            provisioning.os.supports_dir_fd = original_supports_dir_fd
            if stage.is_symlink():
                stage.unlink()
            if saved_stage.exists():
                saved_stage.rename(stage)
        self.assertFalse(target.exists())
        self.assertTrue(stage.is_dir())
        self.assertTrue(foreign_stage.is_dir())
        self.assertTrue(swapped)


if __name__ == "__main__":
    unittest.main()
