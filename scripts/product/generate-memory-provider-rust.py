#!/usr/bin/env python3
"""Project the retained canonical provider registry into its Rust catalog.

The surrounding Rust declarations own the provider's typed wire values. Only
capabilities and the common advisory profile come from the registry JSON.
"""
from __future__ import annotations

import argparse
import difflib
import hashlib
import json
import os
import re
import sys
from pathlib import Path
from typing import Any

DEFAULT_REGISTRY = Path("product/contracts/memory-provider-v1/provider-registry-contract.json")
DEFAULT_OUTPUT = Path("product/contracts/memory-provider-v1/generated/rust/memory_provider_v1.rs")
CATALOG_START = "/// Canonical provider capabilities in registry order.\n"
CATALOG_END = "/// Canonical required field names for `handshake_request_required_fields`.\n"
REGISTRY_ID = "tracedecay.memory.provider.registry.v1"


class GenerationError(RuntimeError):
    """A registry cannot produce an unambiguous Rust catalog."""


def canonical_sha(value: Any) -> str:
    encoded = json.dumps(value, ensure_ascii=False, sort_keys=True,
                         separators=(",", ":"), allow_nan=False).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def rust_string(value: str) -> str:
    return json.dumps(value, ensure_ascii=False)


def registry_capabilities(registry: dict[str, Any]) -> list[dict[str, Any]]:
    authority = registry.get("capability_registry")
    if not isinstance(authority, dict):
        raise GenerationError("registry has no capability_registry authority")
    capabilities: list[dict[str, Any]] = []
    identities: set[str] = set()
    for requirement in ("mandatory", "optional"):
        rows = authority.get(requirement)
        if not isinstance(rows, list):
            raise GenerationError(f"capability_registry.{requirement} must be an array")
        for row in rows:
            if not isinstance(row, dict):
                raise GenerationError("capability must be an object")
            identity = row.get("id")
            if not isinstance(identity, str) or not identity or identity in identities:
                raise GenerationError("capability identity is missing or duplicated")
            if row.get("requirement") != requirement:
                raise GenerationError(f"capability {identity} requirement mismatch")
            if row.get("advisory_only") is not True:
                raise GenerationError(f"capability {identity} must be advisory")
            if row.get("may_mutate_tracedecay_authority") is not False:
                raise GenerationError(f"capability {identity} may mutate TraceDecay authority")
            if not isinstance(row.get("detailed_contract_bead"), str):
                raise GenerationError(f"capability {identity} has no semantic owner")
            identities.add(identity)
            capabilities.append(row)
    return capabilities


def render_string_slice(name: str, values: Any) -> list[str]:
    if not isinstance(values, list) or any(not isinstance(value, str) for value in values):
        raise GenerationError(f"{name} must contain strings")
    return ["/// Canonical common advisory profile declaration.",
            f"pub const {name}: &[&str] = &[",
            *(f"    {rust_string(value)}," for value in values), "];", ""]


def render_catalog(registry: dict[str, Any]) -> str:
    lines = [CATALOG_START.rstrip(), "pub const CAPABILITIES: &[CapabilitySpec] = &["]
    for row in registry_capabilities(registry):
        requirement = "Mandatory" if row["requirement"] == "mandatory" else "Optional"
        lines.extend([
            "    CapabilitySpec {",
            f"        capability_id: {rust_string(row['id'])},",
            f"        requirement: CapabilityRequirement::{requirement},",
            f"        behavior_class: {rust_string(str(row.get('class')))},",
            f"        detailed_contract_bead: {rust_string(row['detailed_contract_bead'])},",
            "        advisory_only: true,",
            "        may_mutate_tracedecay_authority: false,", "    },",
        ])
    profile = registry.get("common_advisory_profile")
    if not isinstance(profile, dict) or not isinstance(profile.get("profile_id"), str):
        raise GenerationError("registry has no common advisory profile")
    lines.extend([
        "];", "", "/// Explicit opt-in capability for the complete common advisory profile.",
        f"pub const COMMON_ADVISORY_PROFILE_ID: &str = {rust_string(profile['profile_id'])};", "",
    ])
    for name, key in (
        ("COMMON_ADVISORY_REQUIRED_CAPABILITIES", "required_capabilities"),
        ("COMMON_ADVISORY_OBSERVATION_KINDS", "required_observation_kinds"),
        ("COMMON_ADVISORY_OPTIONAL_CAPABILITIES", "optional_capabilities"),
        ("COMMON_ADVISORY_OPTIONAL_OBSERVATION_KINDS", "optional_observation_kinds"),
    ):
        lines.extend(render_string_slice(name, profile.get(key)))
    return "\n".join(lines) + "\n"


def project_registry(source: str, registry: dict[str, Any]) -> str:
    if registry.get("contract_id") != REGISTRY_ID:
        raise GenerationError("registry contract identity mismatch")
    if source.count(CATALOG_START) != 1 or source.count(CATALOG_END) != 1:
        raise GenerationError("Rust catalog boundaries are missing or ambiguous")
    start = source.index(CATALOG_START)
    end = source.index(CATALOG_END, start)
    source = source[:start] + render_catalog(registry) + source[end:]
    digest_pattern = re.compile(
        rf'(contract_id: "{re.escape(REGISTRY_ID)}",\n'
        r'        bead_id: "[^"\n]+",\n        contract_sha256: ")[a-f0-9]{64}(",)')
    source, count = digest_pattern.subn(
        lambda match: match[1] + canonical_sha(registry) + match[2], source)
    if count != 1:
        raise GenerationError("Rust registry digest declaration is missing or ambiguous")
    source = re.sub(
        r"\A// @generated by scripts/product/generate-memory-provider-rust.py; DO NOT EDIT.\n"
        r"// generator-sha256: [a-f0-9]{64}\n// contract-set-sha256: [a-f0-9]{64}\n",
        "// Capability catalog generated by scripts/product/generate-memory-provider-rust.py.\n", source)
    old_docs = (
        "//! Generated dependency-free Rust domain bindings for the canonical Memory Provider V1 contracts.\n"
        "//!\n//! The JSON contracts and schemas remain the sole wire authority. These bindings contain\n"
        "//! closed values, stable identifiers, field-name constants, and provider-neutral domain\n"
        "//! wrappers. They intentionally contain no transport decoder and no concrete provider logic.\n")
    source = source.replace(old_docs,
        "//! Canonical dependency-free provider wire values and identifiers.\n"
        "//!\n//! The capability catalog and advisory profile are generated from the retained registry JSON.\n"
        "//! The remaining typed declarations own the wire values inherited from the contract set.\n", 1)
    source = source.replace("/// SHA-256 of the canonical contract-set source.\n",
        "/// Retained conformance identity of the inherited contract-set encoding.\n", 1)
    source = source.replace("/// SHA-256 of the generator that emitted this file.\n",
        "/// SHA-256 of the original generator that emitted the inherited wire declarations.\n", 1)
    return source


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo", type=Path,
                        default=Path(os.environ.get("BUILD_WORKSPACE_DIRECTORY", ".")))
    parser.add_argument("--registry", type=Path, default=DEFAULT_REGISTRY)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--write", action="store_true")
    mode.add_argument("--check", action="store_true")
    args = parser.parse_args()
    repo = args.repo.resolve()
    try:
        registry = json.loads((repo / args.registry).read_text(encoding="utf-8"))
        if not isinstance(registry, dict):
            raise GenerationError("registry root must be an object")
        output = repo / args.output
        current = output.read_text(encoding="utf-8")
        expected = project_registry(current, registry)
        if project_registry(expected, registry) != expected:
            raise GenerationError("registry projection is not stable")
        if args.write:
            if expected != current:
                output.write_text(expected, encoding="utf-8")
        elif expected != current:
            sys.stderr.write("".join(difflib.unified_diff(
                current.splitlines(keepends=True), expected.splitlines(keepends=True),
                fromfile=str(output), tofile="canonical registry projection")))
            return 1
        print(json.dumps({"ok": True, "registry_sha256": canonical_sha(registry)}))
        return 0
    except (GenerationError, OSError, ValueError) as error:
        print(json.dumps({"ok": False, "error": str(error)}))
        return 1


if __name__ == "__main__":
    sys.exit(main())
