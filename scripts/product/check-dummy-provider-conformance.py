#!/usr/bin/env python3
"""Validate the standalone deterministic M1 dummy provider and its evidence."""

from __future__ import annotations

import argparse
import json
import re
import sys
import tomllib
from pathlib import Path
from typing import Any

ROOT = Path("product/conformance/dummy-provider")
EXPECTED_TESTS = {
    "compatible_handshake_is_read_only",
    "provider_identity_mismatch_fails_closed",
    "health_reports_real_capabilities_without_mutation",
    "cancelled_call_stops_before_effect",
    "expired_deadline_stops_before_effect",
    "scope_mismatch_fails_closed",
    "observation_applies_once",
    "duplicate_observation_is_idempotent",
    "same_key_different_observation_conflicts",
    "source_sequence_gap_conflicts",
    "stale_state_generation_fails",
    "required_unknown_extension_is_unsupported",
    "optional_unknown_extension_round_trips_inertly",
    "recall_is_deterministic_and_advisory",
    "zero_results_is_typed_success",
    "snapshot_bytes_are_deterministic",
    "restart_restore_preserves_recall",
    "identical_restore_is_no_effect",
    "nonempty_different_restore_conflicts",
    "corrupt_snapshot_fails_closed",
    "cross_scope_snapshot_is_incompatible",
    "unsupported_feedback_is_explicit",
    "unsupported_maintenance_is_explicit",
}
EXPECTED_MANDATORY = {
    "provider.health.v1",
    "observation.accept.v1",
    "recall.query.v1",
}
EXPECTED_IMPLEMENTED_OPTIONAL = {
    "snapshot.export.v1",
    "snapshot.restore.v1",
}
FORBIDDEN_DEP_PREFIXES = ("tracedecay", "ncm", "ocean")
FORBIDDEN_SOURCE_PATTERNS = {
    "unsafe block": re.compile(r"\bunsafe\s*\{"),
    "unwrap": re.compile(r"\.unwrap\s*\("),
    "expect": re.compile(r"\.expect\s*\("),
    "panic": re.compile(r"\bpanic!\s*\("),
    "todo": re.compile(r"\btodo!\s*\("),
    "unimplemented": re.compile(r"\bunimplemented!\s*\("),
    "dbg": re.compile(r"\bdbg!\s*\("),
    "stdout print": re.compile(r"\bprintln!\s*\("),
    "stderr print": re.compile(r"\beprintln!\s*\("),
}
REQUIRED_SOURCE_MARKERS = {
    "#![forbid(unsafe_code)]",
    "#![deny(warnings)]",
    "#![deny(missing_docs)]",
    "BTreeMap<String, StoredObservation>",
    "pub fn handshake(",
    "pub fn health(",
    "pub fn observe(",
    "pub fn recall(",
    "pub fn snapshot(",
    "pub fn restore(",
    "pub fn unsupported_optional(",
    "Entry::Occupied",
    "DuplicateAcknowledged",
    "TerminalCode::CapabilityUnsupported",
    "TerminalCode::DeadlineExceeded",
    "TerminalCode::Cancelled",
    "TerminalCode::ScopeMismatch",
    "TerminalCode::StaleIdentity",
    "TerminalCode::StateIncompatible",
    "CommittedEffectState::None",
    "CommittedEffectState::Duplicate",
    "committed_by_operation_id",
    "FallbackEligibility::Forbidden",
    "SNAPSHOT_MAGIC",
    "sha256_hex",
}
REQUIRED_DOC_MARKERS = {
    "intentionally small, deterministic, capability-poor",
    "same key and identical canonical fingerprint",
    "committed-effect state `duplicate`",
    "SuccessZeroResults",
    "refuses implicit overwrite",
    "Unknown optional observation extensions are preserved byte-for-byte",
    "Unknown required extensions return typed unsupported before any provider effect",
    "forbids unsafe code",
    "no path dependency on TraceDecay internals",
}
BEAD_RE = re.compile(r"^tdmem-[0-9]{4}$")
TEST_RE = re.compile(r"(?m)^#\[test\]\nfn\s+([a-z][a-z0-9_]*)\s*\(")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--repo", type=Path, default=Path("."))
    parser.add_argument(
        "--manifest",
        type=Path,
        default=ROOT / "conformance-manifest.json",
    )
    parser.add_argument("--issues", type=Path, default=Path(".beads/issues.jsonl"))
    return parser.parse_args()


def resolve(repo: Path, path: Path) -> Path:
    return path if path.is_absolute() else repo / path


def load_json(path: Path, label: str, errors: list[str]) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        errors.append(f"could not load {label}: {exc}")
        return {}
    if not isinstance(value, dict):
        errors.append(f"{label} root must be an object")
        return {}
    return value


def load_issues(path: Path, errors: list[str]) -> set[str]:
    result: set[str] = set()
    try:
        lines = path.read_text(encoding="utf-8").splitlines()
    except OSError as exc:
        errors.append(f"could not load Beads authority: {exc}")
        return result
    for line_number, line in enumerate(lines, start=1):
        if not line.strip():
            continue
        try:
            value = json.loads(line)
        except json.JSONDecodeError as exc:
            errors.append(f"invalid Beads JSONL at line {line_number}: {exc}")
            continue
        issue_id = value.get("id") if isinstance(value, dict) else None
        if not isinstance(issue_id, str):
            errors.append(f"Beads line {line_number} has no string id")
            continue
        result.add(issue_id)
    return result


def require_object(value: Any, label: str, errors: list[str]) -> dict[str, Any]:
    if not isinstance(value, dict):
        errors.append(f"{label} must be an object")
        return {}
    return value


def require_list(value: Any, label: str, errors: list[str]) -> list[Any]:
    if not isinstance(value, list):
        errors.append(f"{label} must be an array")
        return []
    return value


def validate_manifest(
    repo: Path,
    manifest: dict[str, Any],
    issue_ids: set[str],
    errors: list[str],
) -> None:
    if manifest.get("schema_version") != 1:
        errors.append("dummy conformance schema_version must be 1")
    if manifest.get("manifest_id") != "tracedecay.memory.provider.dummy-conformance.v1":
        errors.append("dummy conformance manifest_id drifted")
    if manifest.get("bead_id") != "tdmem-0209":
        errors.append("dummy conformance bead_id must be tdmem-0209")
    if manifest.get("status") != "accepted":
        errors.append("dummy conformance status must be accepted")
    if manifest.get("provider_id") != "test.dummy":
        errors.append("dummy provider_id must remain test.dummy")
    if manifest.get("provider_protocol") != "1.0":
        errors.append("dummy provider protocol must remain 1.0")

    implementation = require_object(manifest.get("implementation"), "implementation", errors)
    expected_impl = {
        "package_name": "tracedecay-memory-dummy-provider",
        "package_version": "0.0.0",
        "edition": "2024",
        "minimum_rust_version": "1.97.1",
        "publish": False,
        "license": "MIT",
        "state_schema_version": "dummy.state.v1",
        "execution_topology": "in_process_test_crate",
        "transport": "direct_rust_api_for_conformance_only",
    }
    for field, expected in expected_impl.items():
        if implementation.get(field) != expected:
            errors.append(f"implementation.{field} must be {expected!r}")

    authorities = require_object(
        manifest.get("contract_authorities"), "contract_authorities", errors
    )
    for field, raw in authorities.items():
        if not isinstance(raw, str) or not raw:
            errors.append(f"contract_authorities.{field} must be a path")
            continue
        path = Path(raw)
        if path.is_absolute() or ".." in path.parts or not (repo / path).is_file():
            errors.append(f"contract authority is missing or unsafe: {raw}")

    capabilities = require_object(manifest.get("capabilities"), "capabilities", errors)
    mandatory = set(require_list(capabilities.get("mandatory"), "capabilities.mandatory", errors))
    implemented = set(
        require_list(
            capabilities.get("implemented_optional"),
            "capabilities.implemented_optional",
            errors,
        )
    )
    unsupported = set(
        require_list(
            capabilities.get("explicitly_unsupported_optional"),
            "capabilities.explicitly_unsupported_optional",
            errors,
        )
    )
    if mandatory != EXPECTED_MANDATORY:
        errors.append("dummy mandatory capabilities must be health, observation, and recall")
    if implemented != EXPECTED_IMPLEMENTED_OPTIONAL:
        errors.append("dummy implemented optional capabilities must be snapshot export/restore")
    if mandatory & implemented or mandatory & unsupported or implemented & unsupported:
        errors.append("dummy capability classes must be disjoint")
    if capabilities.get("provider_name_implies_capability") is not False:
        errors.append("provider name must not imply capabilities")
    if capabilities.get("unsupported_outcome") != "capability_unsupported":
        errors.append("unsupported optional capability outcome drifted")
    if capabilities.get("silent_fallback") is not False:
        errors.append("dummy provider silent fallback must be false")

    registry = load_json(
        repo / "product/contracts/memory-provider-v1/provider-registry-contract.json",
        "provider registry contract",
        errors,
    )
    registry_authority = registry.get("capability_registry")
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

    state_model = require_object(manifest.get("state_model"), "state_model", errors)
    expected_state_model = {
        "authority": "provider_local_only",
        "representation": "BTreeMap_idempotency_key_to_canonical_observation",
        "source_sequence_monotonic": True,
        "state_generation_monotonic": True,
        "duplicate_same_key_same_fingerprint": (
            "duplicate_committed_effect_bound_to_request_key_and_committing_operation"
        ),
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
    for raw in sources:
        if not isinstance(raw, str):
            errors.append("source_paths entries must be strings")
            continue
        path = Path(raw)
        if path.is_absolute() or ".." in path.parts or not (repo / path).is_file():
            errors.append(f"dummy source path is missing or unsafe: {raw}")

    tests = require_list(manifest.get("required_test_cases"), "required_test_cases", errors)
    if set(tests) != EXPECTED_TESTS or len(tests) != len(EXPECTED_TESTS):
        errors.append("required_test_cases must exactly cover the 23 mandatory journeys")

    verification = require_object(manifest.get("verification"), "verification", errors)
    for field in ("checker", "python_tests", "rust_tests"):
        raw = verification.get(field)
        if not isinstance(raw, str) or not (repo / raw).is_file():
            errors.append(f"verification.{field} is missing: {raw!r}")
    commands = require_list(verification.get("commands"), "verification.commands", errors)
    serialized_commands = "\n".join(str(value) for value in commands)
    for marker in ("cargo fmt", "cargo clippy", "cargo test", "check-dummy-provider-conformance.py"):
        if marker not in serialized_commands:
            errors.append(f"verification commands are missing {marker!r}")

    if "tdmem-0209" not in issue_ids:
        errors.append("Beads authority does not contain tdmem-0209")


def validate_cargo(repo: Path, errors: list[str]) -> None:
    cargo_path = repo / ROOT / "Cargo.toml"
    try:
        cargo = tomllib.loads(cargo_path.read_text(encoding="utf-8"))
    except (OSError, tomllib.TOMLDecodeError) as exc:
        errors.append(f"could not parse dummy Cargo.toml: {exc}")
        return
    package = require_object(cargo.get("package"), "Cargo.toml package", errors)
    expected = {
        "name": "tracedecay-memory-dummy-provider",
        "version": "0.0.0",
        "edition": "2024",
        "rust-version": "1.97.1",
        "publish": False,
        "license": "MIT",
    }
    for field, value in expected.items():
        if package.get(field) != value:
            errors.append(f"Cargo.toml package.{field} must be {value!r}")
    workspace = cargo.get("workspace")
    if workspace != {}:
        errors.append("dummy provider must be an isolated empty Cargo workspace")
    dependencies = require_object(cargo.get("dependencies"), "Cargo.toml dependencies", errors)
    if set(dependencies) != {"sha2"}:
        errors.append("dummy provider direct dependencies must be exactly sha2")
    for name, spec in dependencies.items():
        if name.startswith(FORBIDDEN_DEP_PREFIXES):
            errors.append(f"forbidden dummy provider dependency: {name}")
        if isinstance(spec, dict):
            if "path" in spec or spec.get("workspace") is True:
                errors.append(f"dummy provider dependency {name} cannot be path/workspace based")
    rust_lints = require_object(
        require_object(cargo.get("lints"), "Cargo.toml lints", errors).get("rust"),
        "Cargo.toml lints.rust",
        errors,
    )
    if rust_lints.get("unsafe_code") != "forbid":
        errors.append("dummy provider must forbid unsafe_code")
    if rust_lints.get("warnings") != "deny" or rust_lints.get("missing_docs") != "deny":
        errors.append("dummy provider must deny warnings and missing docs")
    clippy = require_object(
        require_object(cargo.get("lints"), "Cargo.toml lints", errors).get("clippy"),
        "Cargo.toml lints.clippy",
        errors,
    )
    for lint in (
        "dbg_macro",
        "expect_used",
        "panic",
        "print_stderr",
        "print_stdout",
        "todo",
        "unimplemented",
        "unwrap_used",
    ):
        if clippy.get(lint) != "deny":
            errors.append(f"dummy provider must deny clippy::{lint}")

    lock_path = repo / ROOT / "Cargo.lock"
    if not lock_path.is_file():
        errors.append("dummy provider Cargo.lock is missing")
    else:
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


def validate_source(repo: Path, errors: list[str]) -> None:
    source_path = repo / ROOT / "src/lib.rs"
    test_path = repo / ROOT / "tests/conformance.rs"
    try:
        source = source_path.read_text(encoding="utf-8")
        tests = test_path.read_text(encoding="utf-8")
    except OSError as exc:
        errors.append(f"could not read dummy Rust source/tests: {exc}")
        return
    for marker in REQUIRED_SOURCE_MARKERS:
        if marker not in source:
            errors.append(f"dummy Rust source is missing {marker!r}")
    for label, pattern in FORBIDDEN_SOURCE_PATTERNS.items():
        if pattern.search(source) or pattern.search(tests):
            errors.append(f"dummy Rust code contains forbidden {label}")
    if "generated/rust/memory_provider_v1.rs" not in source:
        errors.append("dummy provider must include the generated provider-neutral bindings")
    for forbidden in ("tracedecay_runtime", "tracedecay_store", "tracedecay_code", "ncm", "ocean"):
        if re.search(rf"(?m)^use\s+{re.escape(forbidden)}", source):
            errors.append(f"dummy provider source imports forbidden implementation {forbidden}")
    actual_tests = set(TEST_RE.findall(tests))
    missing = EXPECTED_TESTS - actual_tests
    extra = actual_tests - EXPECTED_TESTS
    if missing:
        errors.append(f"dummy Rust conformance tests are missing: {sorted(missing)}")
    if extra:
        errors.append(f"dummy Rust conformance tests contain undeclared cases: {sorted(extra)}")


def validate_documentation(repo: Path, errors: list[str]) -> None:
    path = repo / ROOT / "README.md"
    try:
        text = path.read_text(encoding="utf-8")
    except OSError as exc:
        errors.append(f"could not read dummy provider README: {exc}")
        return
    for marker in REQUIRED_DOC_MARKERS:
        if marker.casefold() not in text.casefold():
            errors.append(f"dummy provider README is missing {marker!r}")
    if "TBD" in text or "TODO" in text:
        errors.append("dummy provider README contains unresolved TBD/TODO text")


def validate_generated_authority(repo: Path, errors: list[str]) -> None:
    generated = repo / "product/contracts/memory-provider-v1/generated/rust/memory_provider_v1.rs"
    if not generated.is_file():
        errors.append("generated Rust provider authority is missing")
        return
    text = generated.read_text(encoding="utf-8")
    for marker in (
        "pub const CAPABILITIES",
        "pub enum TerminalCode",
        "pub enum CommittedEffectState",
        "pub enum FallbackEligibility",
        "pub struct RequestControl",
    ):
        if marker not in text:
            errors.append(f"generated Rust authority is missing {marker!r}")


def validate(repo: Path, manifest: dict[str, Any], issue_ids: set[str]) -> list[str]:
    errors: list[str] = []
    validate_manifest(repo, manifest, issue_ids, errors)
    validate_cargo(repo, errors)
    validate_source(repo, errors)
    validate_documentation(repo, errors)
    validate_generated_authority(repo, errors)
    return errors


def main() -> int:
    args = parse_args()
    repo = args.repo.resolve()
    bootstrap: list[str] = []
    manifest = load_json(resolve(repo, args.manifest), "dummy conformance manifest", bootstrap)
    issue_ids = load_issues(resolve(repo, args.issues), bootstrap)
    if bootstrap:
        print(json.dumps({"ok": False, "errors": bootstrap}, indent=2, sort_keys=True))
        return 1
    errors = validate(repo, manifest, issue_ids)
    if errors:
        print(json.dumps({"ok": False, "errors": errors}, indent=2, sort_keys=True))
        return 1
    print(
        json.dumps(
            {
                "ok": True,
                "schema_version": manifest["schema_version"],
                "manifest_id": manifest["manifest_id"],
                "bead_id": manifest["bead_id"],
                "status": manifest["status"],
                "provider_id": manifest["provider_id"],
                "mandatory_capabilities": len(manifest["capabilities"]["mandatory"]),
                "implemented_optional_capabilities": len(
                    manifest["capabilities"]["implemented_optional"]
                ),
                "explicitly_unsupported_optional_capabilities": len(
                    manifest["capabilities"]["explicitly_unsupported_optional"]
                ),
                "required_test_cases": len(manifest["required_test_cases"]),
                "rust_version": manifest["implementation"]["minimum_rust_version"],
            },
            indent=2,
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
