#!/usr/bin/env python3
"""Finalize the standalone dummy-provider crate before tdmem-0209 checks."""

from __future__ import annotations

import json
import subprocess
from pathlib import Path

SCRIPT = Path(__file__).resolve()
REPO = SCRIPT.parents[3]
ROOT = REPO / "product/conformance/dummy-provider"


def replace_once(path: Path, old: str, new: str) -> None:
    text = path.read_text(encoding="utf-8")
    if old not in text:
        if new in text:
            return
        raise RuntimeError(f"expected patch anchor missing from {path}: {old!r}")
    path.write_text(text.replace(old, new, 1), encoding="utf-8")


replace_once(
    ROOT / "Cargo.toml",
    'rust-version = "1.85"',
    'rust-version = "1.97.1"',
)
replace_once(
    ROOT / "conformance-manifest.json",
    '"status": "accepted_candidate"',
    '"status": "accepted"',
)
replace_once(
    ROOT / "src/lib.rs",
    "TerminalCode::ProviderIdMismatch",
    "TerminalCode::InvalidRequest",
)
replace_once(
    ROOT / "tests/conformance.rs",
    "TerminalCode::ProviderIdMismatch",
    "TerminalCode::InvalidRequest",
)
replace_once(
    ROOT / "src/lib.rs",
    '''        if let Ok(current) = self.snapshot_internal() {
            if current.content_sha256 == snapshot.content_sha256 {
                return Terminal::success(
                    RestoreResult {
                        state_generation: self.state_generation,
                        acknowledged_sequence: self.acknowledged_sequence,
                        changed: false,
                    },
                    self.state_generation,
                    CommittedEffectState::None,
                );
            }
        }
''',
    '''        if let Ok(current) = self.snapshot_internal()
            && current.content_sha256 == snapshot.content_sha256
        {
            return Terminal::success(
                RestoreResult {
                    state_generation: self.state_generation,
                    acknowledged_sequence: self.acknowledged_sequence,
                    changed: false,
                },
                self.state_generation,
                CommittedEffectState::None,
            );
        }
''',
)

checker = REPO / "scripts/product/check-dummy-provider-conformance.py"
replace_once(
    checker,
    'TEST_RE = re.compile(r"(?m)^fn\\s+([a-z][a-z0-9_]*)\\s*\\(")',
    'TEST_RE = re.compile(r"(?m)^#\\[test\\]\\nfn\\s+([a-z][a-z0-9_]*)\\s*\\(")',
)
old_registry = '''    rows = registry.get("capability_registry")
    if not isinstance(rows, list):
        errors.append("provider registry capability authority is missing")
    else:
        registry_mandatory: set[str] = set()
        registry_optional: set[str] = set()
        for row in rows:
            if not isinstance(row, dict):
                continue
            capability_id = row.get("capability_id")
            requirement = row.get("requirement")
            if isinstance(capability_id, str):
                if requirement == "mandatory":
                    registry_mandatory.add(capability_id)
                elif requirement == "optional":
                    registry_optional.add(capability_id)
        if mandatory != registry_mandatory:
            errors.append("dummy mandatory capabilities do not match registry authority")
        if implemented | unsupported != registry_optional:
            errors.append("dummy optional capability partition does not match registry authority")
'''
new_registry = '''    registry_authority = registry.get("capability_registry")
    if not isinstance(registry_authority, dict):
        errors.append("provider registry capability authority is missing")
    else:
        registry_mandatory: set[str] = set()
        registry_optional: set[str] = set()
        for requirement, target in (
            ("mandatory", registry_mandatory),
            ("optional", registry_optional),
        ):
            rows = registry_authority.get(requirement)
            if not isinstance(rows, list):
                errors.append(
                    f"provider registry capability_registry.{requirement} is missing"
                )
                continue
            for row in rows:
                if not isinstance(row, dict):
                    continue
                capability_id = row.get("id")
                row_requirement = row.get("requirement")
                if isinstance(capability_id, str) and row_requirement == requirement:
                    target.add(capability_id)
        if mandatory != registry_mandatory:
            errors.append("dummy mandatory capabilities do not match registry authority")
        if implemented | unsupported != registry_optional:
            errors.append("dummy optional capability partition does not match registry authority")
'''
replace_once(checker, old_registry, new_registry)

checker_anchor = '''    sources = require_list(manifest.get("source_paths"), "source_paths", errors)
'''
checker_insert = '''    state_model = require_object(manifest.get("state_model"), "state_model", errors)
    expected_state_model = {
        "authority": "provider_local_only",
        "representation": "BTreeMap_idempotency_key_to_canonical_observation",
        "source_sequence_monotonic": True,
        "state_generation_monotonic": True,
        "duplicate_same_key_same_fingerprint": "duplicate_acknowledged_without_new_effect",
        "duplicate_same_key_different_fingerprint": "conflict_without_new_effect",
        "snapshot_encoding": "canonical_length_prefixed_binary_v1",
        "snapshot_digest": "sha256",
        "snapshot_deterministic": True,
        "restore_idempotent": True,
        "implicit_reset": False,
        "implicit_overwrite": False,
    }
    for field, expected in expected_state_model.items():
        if state_model.get(field) != expected:
            errors.append(f"state_model.{field} must be {expected!r}")

    isolation = require_object(
        manifest.get("authority_and_isolation"),
        "authority_and_isolation",
        errors,
    )
    expected_isolation = {
        "exact_scope_required": True,
        "provider_may_widen_scope": False,
        "provider_may_mutate_tracedecay_authority": False,
        "provider_may_inject_context": False,
        "provider_may_trigger_tools_or_external_actions": False,
        "provider_may_change_native_trust": False,
        "provider_may_access_tracedecay_database": False,
        "provider_may_access_code_index": False,
        "unknown_optional_extensions_are_inert": True,
        "unknown_required_extensions_fail_before_effect": True,
    }
    for field, expected in expected_isolation.items():
        if isolation.get(field) != expected:
            errors.append(
                f"authority_and_isolation.{field} must be {expected!r}"
            )

    request_control = require_object(
        manifest.get("request_control"), "request_control", errors
    )
    expected_control = {
        "already_cancelled_outcome": "cancelled_without_effect",
        "expired_deadline_outcome": "deadline_exceeded_without_effect",
        "cancellation_distinct_from_timeout": True,
        "health_read_only": True,
        "recall_read_only": True,
        "snapshot_export_read_only": True,
    }
    for field, expected in expected_control.items():
        if request_control.get(field) != expected:
            errors.append(f"request_control.{field} must be {expected!r}")

    dependency_policy = require_object(
        manifest.get("dependencies"), "dependencies", errors
    )
    if dependency_policy.get("allowed_direct") != ["sha2"]:
        errors.append("dependencies.allowed_direct must be exactly ['sha2']")
    if dependency_policy.get("forbidden_prefixes") != [
        "tracedecay",
        "ncm",
        "ocean",
    ]:
        errors.append("dependencies.forbidden_prefixes drifted")
    if dependency_policy.get("path_dependencies_allowed") is not False:
        errors.append("dependencies.path_dependencies_allowed must be false")
    if dependency_policy.get("workspace_dependencies_allowed") is not False:
        errors.append("dependencies.workspace_dependencies_allowed must be false")

    sources = require_list(manifest.get("source_paths"), "source_paths", errors)
'''
replace_once(checker, checker_anchor, checker_insert)

old_lock = '''    else:
        lock_text = lock_path.read_text(encoding="utf-8")
        if 'name = "tracedecay-memory-dummy-provider"' not in lock_text:
            errors.append("dummy Cargo.lock lacks the root package")
        if 'name = "sha2"' not in lock_text:
            errors.append("dummy Cargo.lock lacks sha2")
        for prefix in FORBIDDEN_DEP_PREFIXES:
            if re.search(rf'(?m)^name = "{re.escape(prefix)}[^\\"]*"$', lock_text):
                errors.append(f"dummy Cargo.lock contains forbidden package prefix {prefix!r}")
'''
new_lock = '''    else:
        try:
            lock = tomllib.loads(lock_path.read_text(encoding="utf-8"))
        except (OSError, tomllib.TOMLDecodeError) as exc:
            errors.append(f"could not parse dummy Cargo.lock: {exc}")
        else:
            packages = lock.get("package")
            if not isinstance(packages, list):
                errors.append("dummy Cargo.lock package list is missing")
                packages = []
            names = {
                row.get("name")
                for row in packages
                if isinstance(row, dict) and isinstance(row.get("name"), str)
            }
            root_name = "tracedecay-memory-dummy-provider"
            if root_name not in names:
                errors.append("dummy Cargo.lock lacks the root package")
            if "sha2" not in names:
                errors.append("dummy Cargo.lock lacks sha2")
            for name in sorted(value for value in names if isinstance(value, str)):
                if name == root_name:
                    continue
                if name.startswith(FORBIDDEN_DEP_PREFIXES):
                    errors.append(f"dummy Cargo.lock contains forbidden package {name!r}")
'''
replace_once(checker, old_lock, new_lock)

mutation_tests = REPO / "tests/product_dummy_provider_conformance_test.py"
replace_once(
    mutation_tests,
    '"dummy Rust source imports forbidden implementation",',
    '"authority_and_isolation.provider_may_mutate_tracedecay_authority must be False",',
)
replace_once(
    mutation_tests,
    '"provider_may_access_tracedecay_database",',
    '"authority_and_isolation.provider_may_access_tracedecay_database must be False",',
)
replace_once(
    mutation_tests,
    'self.assert_rejected(manifest, "snapshot_deterministic")',
    'self.assert_rejected(manifest, "state_model.snapshot_deterministic must be True")',
)
replace_once(
    mutation_tests,
    'self.assert_rejected(manifest, "implicit_reset")',
    'self.assert_rejected(manifest, "state_model.implicit_reset must be False")',
)

subprocess.run(
    [
        "cargo",
        "generate-lockfile",
        "--manifest-path",
        str(ROOT / "Cargo.toml"),
    ],
    cwd=REPO,
    check=True,
)
subprocess.run(
    [
        "cargo",
        "fmt",
        "--manifest-path",
        str(ROOT / "Cargo.toml"),
        "--all",
    ],
    cwd=REPO,
    check=True,
)

marker = [
    {
        "path": "product/conformance/dummy-provider",
        "message": "test(conformance): finalize standalone dummy provider (tdmem-0209)",
    },
    {
        "path": "scripts/product/check-dummy-provider-conformance.py",
        "message": "test(conformance): enforce dummy-provider evidence (tdmem-0209)",
    },
    {
        "path": "tests/product_dummy_provider_conformance_test.py",
        "message": "test(conformance): finalize dummy-provider mutation tests (tdmem-0209)",
    },
]
(REPO / ".beads/operations/prepared-files.json").write_text(
    json.dumps(marker, indent=2, sort_keys=True) + "\n",
    encoding="utf-8",
)
SCRIPT.unlink()
