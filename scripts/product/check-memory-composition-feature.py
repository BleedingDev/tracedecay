#!/usr/bin/env python3
"""Verify the narrow, dormant-by-default TraceDecay memory-provider host mount."""

from __future__ import annotations

import argparse
import re
import sys
import tomllib
from pathlib import Path
from typing import Any

FEATURE = "memory-provider-host"
REGISTRY_PACKAGE = "tracedecay-memory-provider-registry"
REGISTRY_DEP_FEATURE = f"dep:{REGISTRY_PACKAGE}"
REGISTRY_CRATE_IDENT = "tracedecay_memory_provider_registry"
ROOT_MANIFEST = Path("crates/tracedecay/Cargo.toml")
COMPOSITION_MOUNT = Path("crates/tracedecay/src/daemon/project_composition.rs")
# Files that may retain the composed host for the project-server lifetime.
# They may name the registry crate's composition type but must never compose
# providers themselves.
RETENTION_MOUNTS = (
    Path("crates/tracedecay/src/mcp/server.rs"),
    Path("crates/tracedecay/src/mcp/server/construction.rs"),
)
ROOT_SOURCE = Path("crates/tracedecay/src")
# The concrete Native adapter type; a word boundary keeps the registry's
# NativeProviderActivation seam from matching.
CONCRETE_NATIVE_ADAPTER = re.compile(r"\bNativeProvider\b")


def read_toml(path: Path) -> dict[str, Any]:
    try:
        with path.open("rb") as handle:
            value = tomllib.load(handle)
    except (OSError, tomllib.TOMLDecodeError) as error:
        raise ValueError(f"cannot read TOML {path}: {error}") from error
    if not isinstance(value, dict):
        raise ValueError(f"TOML root must be a table: {path}")
    return value


def check_repository(repo: Path) -> list[str]:
    errors: list[str] = []
    manifest_path = repo / ROOT_MANIFEST
    mount_path = repo / COMPOSITION_MOUNT
    manifest = read_toml(manifest_path)
    features = manifest.get("features")
    dependencies = manifest.get("dependencies")
    if not isinstance(features, dict):
        return ["root manifest [features] table is missing"]
    if not isinstance(dependencies, dict):
        return ["root manifest [dependencies] table is missing"]

    feature_values = features.get(FEATURE)
    if feature_values != [REGISTRY_DEP_FEATURE]:
        errors.append(
            f"feature {FEATURE} must contain only {REGISTRY_DEP_FEATURE}, found {feature_values!r}"
        )
    if features.get("default") != ["production"]:
        errors.append(
            f"default features must remain exactly ['production'], found {features.get('default')!r}"
        )
    production = features.get("production")
    if not isinstance(production, list):
        errors.append("feature production must be an array")
    elif REGISTRY_DEP_FEATURE in production or REGISTRY_PACKAGE in production:
        errors.append(
            f"feature production must reach {REGISTRY_PACKAGE} only through {FEATURE}"
        )

    dependency = dependencies.get(REGISTRY_PACKAGE)
    if not isinstance(dependency, dict):
        errors.append(f"optional dependency {REGISTRY_PACKAGE} is missing")
    else:
        if dependency.get("optional") is not True:
            errors.append(f"dependency {REGISTRY_PACKAGE} must be optional")
        expected_path = "../tracedecay-memory-provider-registry"
        if dependency.get("path") != expected_path:
            errors.append(
                f"dependency {REGISTRY_PACKAGE} path must be {expected_path}"
            )
        forbidden_keys = sorted(set(dependency) & {"default-features", "features"})
        if forbidden_keys:
            errors.append(
                f"dependency {REGISTRY_PACKAGE} must not silently enable features: {forbidden_keys}"
            )

    try:
        mount = mount_path.read_text(encoding="utf-8")
    except OSError as error:
        return errors + [f"cannot read composition mount {mount_path}: {error}"]
    required_fragments = (
        f'#[cfg(feature = "{FEATURE}")]\nfn mount_project_memory_provider_host(',
        f"{REGISTRY_CRATE_IDENT}::ProjectMemoryProviderComposition::compose(activation)",
        f"{REGISTRY_CRATE_IDENT}::NativeProviderActivation::Disabled,",
    )
    for fragment in required_fragments:
        if fragment not in mount:
            errors.append(f"composition mount is missing exact fragment: {fragment}")
    for forbidden in (
        "tracedecay_memory_provider_native",
        "NcmProviderAdapter",
    ):
        if forbidden in mount:
            errors.append(
                f"composition mount must delegate through the registry, not name {forbidden}"
            )
    if CONCRETE_NATIVE_ADAPTER.search(mount):
        errors.append(
            "composition mount must delegate through the registry, not name NativeProvider"
        )

    source_root = repo / ROOT_SOURCE
    if source_root.is_dir():
        for path in sorted(source_root.rglob("*.rs")):
            relative = path.relative_to(repo)
            if relative == COMPOSITION_MOUNT:
                continue
            text = path.read_text(encoding="utf-8")
            if relative in RETENTION_MOUNTS:
                if "::compose(" in text:
                    errors.append(
                        f"retention mount must not compose providers: {relative}"
                    )
            elif REGISTRY_CRATE_IDENT in text:
                errors.append(
                    f"registry dependency leaked outside the composition mount: {relative}"
                )
            if "NativeProviderActivation::Enabled" in text:
                errors.append(
                    f"production sources must keep the provider host dormant: {relative}"
                )
            if "tracedecay_memory_provider_native" in text or CONCRETE_NATIVE_ADAPTER.search(
                text
            ):
                errors.append(f"concrete Native adapter leaked into root source: {relative}")
    return errors


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo", default=".", help="repository root")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(sys.argv[1:] if argv is None else argv)
    repo = Path(args.repo).resolve()
    try:
        errors = check_repository(repo)
    except ValueError as error:
        print(f"memory composition feature error: {error}", file=sys.stderr)
        return 2
    if errors:
        for error in errors:
            print(f"memory composition feature violation: {error}", file=sys.stderr)
        return 1
    print("memory composition feature verified")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
