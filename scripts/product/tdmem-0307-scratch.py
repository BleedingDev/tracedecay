#!/usr/bin/env python3
"""Materialize and validate the tdmem-0307 composition branch."""

from __future__ import annotations

import fnmatch
import json
import subprocess
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
FLOOR = "08fbe33a7c7f403191fd5d6e356c7b6681b96403"
BRANCH = "agent/tdmem-0307-composition"
MANIFEST_PATHS = [
    "Cargo.lock",
    "crates/tracedecay/Cargo.toml",
    "crates/tracedecay/src/lib.rs",
    "crates/tracedecay/src/memory_provider_composition.rs",
    "crates/tracedecay/tests/product_memory_provider/composition.rs",
    "product/upstream/patch-footprint-policy.json",
    "product/upstream/convergence-map.json",
]


def run(*argv: str, capture: bool = False) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        list(argv),
        cwd=ROOT,
        check=True,
        capture_output=capture,
        text=True,
    )


def patch_root_manifest() -> None:
    path = ROOT / "crates/tracedecay/Cargo.toml"
    text = path.read_text(encoding="utf-8")
    feature = """# Product-owned Memory Fabric composition. Default-off: neither `default` nor
# `production` includes it, and feature enablement alone performs no runtime mount.
memory-provider-fabric = [
    "dep:tracedecay-memory-fabric",
    "dep:tracedecay-memory-provider-api",
    "dep:tracedecay-memory-provider-native",
]

"""
    if "memory-provider-fabric = [" not in text:
        marker = 'default = ["production"]\n\n'
        if marker not in text:
            raise SystemExit("default feature marker missing")
        text = text.replace(marker, marker + feature, 1)

    dependencies = """tracedecay-memory-fabric = { path = "../tracedecay-memory-fabric", version = "0.1.0", optional = true }
tracedecay-memory-provider-api = { path = "../tracedecay-memory-provider-api", version = "0.1.0", optional = true }
tracedecay-memory-provider-native = { path = "../tracedecay-memory-provider-native", version = "0.1.0", optional = true }
"""
    if "tracedecay-memory-fabric = {" not in text:
        marker = (
            'tracedecay-maintenance = { path = "../tracedecay-maintenance", '
            'version = "0.1.0" }\n'
        )
        if marker not in text:
            raise SystemExit("dependency marker missing")
        text = text.replace(marker, marker + dependencies, 1)

    target = """[[test]]
name = "memory_provider_composition"
path = "tests/product_memory_provider/composition.rs"
required-features = ["memory-provider-fabric"]

"""
    if 'name = "memory_provider_composition"' not in text:
        marker = '[lib]\nname = "tracedecay"\n'
        if marker not in text:
            raise SystemExit("library target marker missing")
        text = text.replace(marker, target + marker, 1)
    path.write_text(text, encoding="utf-8")


def patch_root_library() -> None:
    path = ROOT / "crates/tracedecay/src/lib.rs"
    text = path.read_text(encoding="utf-8")
    if "mod memory_provider_composition;" not in text:
        marker = "mod runtime_ports;\npub use runtime_ports::register_runtime_ports;"
        replacement = """#[cfg(feature = "memory-provider-fabric")]
mod memory_provider_composition;
#[cfg(feature = "memory-provider-fabric")]
pub use memory_provider_composition::{
    FabricError, MemoryFabric, NativeMemoryApplicationPort, NativeMemoryFabricConfig,
    NativeMemoryFabricMount, NativeMemoryMode, NativeMemoryMountError, ProviderMode,
    ProviderStatus, compose_native_memory_fabric,
};
mod runtime_ports;
pub use runtime_ports::register_runtime_ports;"""
        if marker not in text:
            raise SystemExit("runtime port marker missing")
        text = text.replace(marker, replacement, 1)
    path.write_text(text, encoding="utf-8")


def patch_footprint_policy() -> dict[str, object]:
    path = ROOT / "product/upstream/patch-footprint-policy.json"
    value = json.loads(path.read_text(encoding="utf-8"))
    touch = next(
        row
        for row in value["allowed_touch_points"]
        if row["id"] == "daemon_composition_mount"
    )
    for item in (
        "crates/tracedecay/Cargo.toml",
        "crates/tracedecay/src/lib.rs",
        "crates/tracedecay/src/memory_provider_composition.rs",
    ):
        if item not in touch["paths"]:
            touch["paths"].append(item)
    change = (
        "gate explicit product-owned Memory Fabric construction behind a "
        "default-off root feature"
    )
    if change not in touch["allowed_changes"]:
        touch["allowed_changes"].append(change)
    path.write_text(
        json.dumps(value, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    return value


def convergence_entries() -> list[dict[str, object]]:
    return [
        {
            "path": "crates/tracedecay/Cargo.toml",
            "touch_point": "daemon_composition_mount",
            "rationale": (
                "Declare one opt-in root feature and optional product-owned "
                "dependencies so ordinary and production builds cannot construct "
                "Memory Fabric infrastructure."
            ),
            "semantic_invariants": [
                "The default and production feature sets exclude memory-provider-fabric.",
                "All Memory Fabric dependencies are optional and activated only by the explicit feature.",
                "The root package remains the only composition layer permitted to know the concrete Native adapter.",
            ],
            "verification": [
                "python3 tests/product_memory_composition_feature_test.py",
                "cargo tree -p tracedecay --no-default-features -e normal --prefix none --locked",
                "cargo check -p tracedecay --no-default-features --features memory-provider-fabric --lib --locked",
            ],
            "bead_ids": ["tdmem-0307"],
            "line_budget": 48,
            "rebase_or_removal_plan": (
                "Remove the feature, optional dependency declarations, and "
                "dedicated integration-test target without changing an existing "
                "production feature."
            ),
            "status": "active",
        },
        {
            "path": "crates/tracedecay/src/lib.rs",
            "touch_point": "daemon_composition_mount",
            "rationale": (
                "Expose the explicit feature-gated mount while leaving the "
                "existing register_runtime_ports path independent of Memory Fabric."
            ),
            "semantic_invariants": [
                "No default runtime registration calls the Memory Fabric composition function.",
                "The module and its public seam do not exist when memory-provider-fabric is disabled.",
                "Existing public modules and runtime-port registration remain unchanged.",
            ],
            "verification": [
                "python3 tests/product_memory_composition_feature_test.py",
                "cargo check -p tracedecay --no-default-features --lib --locked",
                "cargo check -p tracedecay --no-default-features --features memory-provider-fabric --lib --locked",
            ],
            "bead_ids": ["tdmem-0307"],
            "line_budget": 24,
            "rebase_or_removal_plan": (
                "Delete only the cfg-gated module declaration and re-export block."
            ),
            "status": "active",
        },
        {
            "path": "crates/tracedecay/src/memory_provider_composition.rs",
            "touch_point": "daemon_composition_mount",
            "rationale": (
                "Construct one finite provider-neutral fabric and one Native "
                "adapter only after an explicit caller supplies the existing "
                "Native application authority."
            ),
            "semantic_invariants": [
                "Feature enablement alone starts no thread, queue, global registration, catalog mutation, context contribution, or state creation.",
                "Disabled behavior is represented by absence of the feature and mount call, so no disabled configuration can instantiate provider infrastructure.",
                "The concrete NativeProvider remains private to the composition module and host routes receive only provider-neutral fabric types.",
                "Invalid revision or resource limits fail before the Native authority is inspected.",
            ],
            "verification": [
                "cargo clippy -p tracedecay --no-default-features --features memory-provider-fabric --lib --test memory_provider_composition --locked -- -D warnings",
                "cargo test -p tracedecay --no-default-features --features memory-provider-fabric --test memory_provider_composition --locked",
                "python3 tests/product_memory_composition_feature_test.py",
            ],
            "bead_ids": ["tdmem-0307"],
            "line_budget": 180,
            "rebase_or_removal_plan": (
                "Delete the isolated module; no provider state, migration, host "
                "route, or public transport schema remains."
            ),
            "status": "active",
        },
    ]


def patch_convergence_map() -> dict[str, object]:
    path = ROOT / "product/upstream/convergence-map.json"
    value = json.loads(path.read_text(encoding="utf-8"))
    entries = {row["path"]: row for row in value["entries"]}
    lock_entry = entries["Cargo.lock"]
    if "tdmem-0307" not in lock_entry["bead_ids"]:
        lock_entry["bead_ids"].append("tdmem-0307")
    for command in (
        "cargo check -p tracedecay --no-default-features --lib --locked",
        "cargo check -p tracedecay --no-default-features --features memory-provider-fabric --lib --locked",
        "cargo test -p tracedecay --no-default-features --features memory-provider-fabric --test memory_provider_composition --locked",
    ):
        if command not in lock_entry["verification"]:
            lock_entry["verification"].append(command)
    for entry in convergence_entries():
        if entry["path"] not in entries:
            value["entries"].append(entry)
            entries[entry["path"]] = entry
    path.write_text(
        json.dumps(value, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    return value


def product_owned(path: str, patterns: list[str]) -> bool:
    return any(fnmatch.fnmatchcase(path, pattern) for pattern in patterns)


def is_test_or_fixture(path: str) -> bool:
    name = Path(path).name.lower()
    return (
        path.startswith("tests/")
        or "/tests/" in path
        or "/test/" in path
        or "fixture" in name
        or name.endswith("_test.rs")
        or name.endswith("_tests.rs")
        or name.startswith("test_")
    )


def update_snapshot(
    policy: dict[str, object],
    convergence: dict[str, object],
) -> None:
    patterns = [
        value
        for value in policy["product_owned_paths"]
        if isinstance(value, str)
    ]
    result = run(
        "git",
        "diff",
        "--no-renames",
        "--numstat",
        FLOOR,
        "--",
        capture=True,
    )
    rows: dict[str, tuple[int, int]] = {}
    for line in result.stdout.splitlines():
        if not line:
            continue
        added, deleted, path = line.split("\t", 2)
        if added == "-" or deleted == "-":
            raise SystemExit(f"binary diff is unsupported: {path}")
        if not product_owned(path, patterns):
            rows[path] = (int(added), int(deleted))

    entries = {row["path"]: row for row in convergence["entries"]}
    convergence["snapshot"].update(
        {
            "upstream_existing_production_files": sum(
                not is_test_or_fixture(path) for path in rows
            ),
            "upstream_existing_test_or_fixture_files": sum(
                is_test_or_fixture(path) for path in rows
            ),
            "total_upstream_changed_lines": sum(
                added + deleted for added, deleted in rows.values()
            ),
            "composition_root_files": sum(
                entries.get(path, {}).get("touch_point")
                == "daemon_composition_mount"
                for path in rows
            ),
            "exception_zone_files": sum(
                entries.get(path, {}).get("touch_point") == "exception"
                for path in rows
            ),
            "observed_state": (
                "M2 adds one default-off, explicit root composition seam. "
                "Ordinary and production builds retain no Memory Fabric "
                "dependencies or runtime mount; the opt-in path constructs one "
                "bounded fabric and keeps the concrete Native adapter private."
            ),
        }
    )
    (ROOT / "product/upstream/convergence-map.json").write_text(
        json.dumps(convergence, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )


def has_worktree_changes() -> bool:
    result = subprocess.run(
        ["git", "diff", "--quiet"],
        cwd=ROOT,
        check=False,
    )
    return result.returncode != 0


def commit_materialized_files() -> bool:
    if not has_worktree_changes():
        return False
    run("git", "config", "user.name", "github-actions[bot]")
    run(
        "git",
        "config",
        "user.email",
        "41898282+github-actions[bot]@users.noreply.github.com",
    )
    run("git", "add", *MANIFEST_PATHS)
    run("git", "commit", "-m", "feat(memory): stage default-off composition mount")
    return True


def validate_committed_state() -> None:
    run(
        "python3",
        "scripts/product/check-patch-footprint-policy.py",
        "--repo",
        ".",
        "--policy",
        "product/upstream/patch-footprint-policy.json",
        "--map",
        "product/upstream/convergence-map.json",
    )
    run(
        "python3",
        "scripts/product/check-memory-dependency-direction.py",
        "--repo",
        ".",
        "--policy",
        "product/architecture/memory-dependency-policy.json",
    )
    run(
        "python3",
        "scripts/product/check-memory-dependency-policy.py",
        "--repo",
        ".",
        "--policy",
        "product/upstream/patch-footprint-policy.json",
    )


def main() -> None:
    patch_root_manifest()
    patch_root_library()
    policy = patch_footprint_policy()
    convergence = patch_convergence_map()
    run("cargo", "metadata", "--format-version", "1", "--no-deps")
    run(
        "rustfmt",
        "--edition",
        "2024",
        "crates/tracedecay/src/memory_provider_composition.rs",
        "crates/tracedecay/tests/product_memory_provider/composition.rs",
    )
    update_snapshot(policy, convergence)
    run("git", "diff", "--check")
    changed = commit_materialized_files()
    validate_committed_state()
    if changed:
        run("git", "push", "origin", f"HEAD:{BRANCH}")


if __name__ == "__main__":
    main()
