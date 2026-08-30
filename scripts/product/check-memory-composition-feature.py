#!/usr/bin/env python3
"""Verify the narrow, default-off TraceDecay Memory Fabric feature."""

from __future__ import annotations

import argparse
import sys
import tomllib
from pathlib import Path
from typing import Any

FEATURE = "memory-fabric"
REGISTRY_PACKAGE = "tracedecay-memory-provider-registry"
REGISTRY_DEP_FEATURE = f"dep:{REGISTRY_PACKAGE}"
ROOT_MANIFEST = Path("crates/tracedecay/Cargo.toml")
COMPOSITION_MOUNT = Path("crates/tracedecay/src/runtime_ports.rs")
ROOT_SOURCE = Path("crates/tracedecay/src")


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
    for baseline in ("default", "production"):
        values = features.get(baseline)
        if not isinstance(values, list):
            errors.append(f"feature {baseline} must be an array")
            continue
        if FEATURE in values or REGISTRY_DEP_FEATURE in values or REGISTRY_PACKAGE in values:
            errors.append(f"feature {baseline} must not enable {FEATURE}")

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
        '#[cfg(feature = "memory-fabric")]\npub(crate) fn compose_native_memory_fabric(',
        "tracedecay_memory_provider_registry::compose_native_memory(port, config)",
    )
    for fragment in required_fragments:
        if fragment not in mount:
            errors.append(f"composition mount is missing exact fragment: {fragment}")
    for forbidden in (
        "tracedecay_memory_provider_native",
        "NativeProvider",
        "NcmProviderAdapter",
    ):
        if forbidden in mount:
            errors.append(
                f"composition mount must delegate through the registry, not name {forbidden}"
            )

    source_root = repo / ROOT_SOURCE
    if source_root.is_dir():
        for path in sorted(source_root.rglob("*.rs")):
            relative = path.relative_to(repo)
            if relative == COMPOSITION_MOUNT:
                continue
            text = path.read_text(encoding="utf-8")
            if "tracedecay_memory_provider_registry" in text:
                errors.append(
                    f"registry dependency leaked outside the composition mount: {relative}"
                )
            if "tracedecay_memory_provider_native" in text or "NativeProvider" in text:
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
