#!/usr/bin/env python3
"""Wire, verify, and convergence-map the tdmem-0307 composition feature."""

from __future__ import annotations

import fnmatch
import json
import subprocess
from pathlib import Path
from typing import Any

HERE = Path(__file__).resolve()
ROOT = HERE.parents[3]
FLOOR = "08fbe33a7c7f403191fd5d6e356c7b6681b96403"
REGISTRY = "tracedecay-memory-provider-registry"
FEATURE = "memory-fabric"


def run(argv: list[str]) -> None:
    subprocess.run(argv, cwd=ROOT, check=True)


def output(argv: list[str]) -> str:
    return subprocess.run(
        argv,
        cwd=ROOT,
        check=True,
        capture_output=True,
        text=True,
    ).stdout


def changed(path: str) -> bool:
    return bool(
        subprocess.run(
            ["git", "status", "--porcelain", "--untracked-files=all", "--", path],
            cwd=ROOT,
            check=True,
            capture_output=True,
            text=True,
        ).stdout.strip()
    )


def replace_once(text: str, old: str, new: str, label: str) -> str:
    if text.count(old) != 1:
        raise SystemExit(f"could not locate unique {label}")
    return text.replace(old, new, 1)


def add_workspace_member() -> None:
    path = ROOT / "Cargo.toml"
    text = path.read_text(encoding="utf-8")
    member = '    "crates/tracedecay-memory-provider-registry",\n'
    if member in text:
        return
    marker = '    "crates/tracedecay-memory-conformance",\n'
    path.write_text(
        replace_once(text, marker, marker + member, "conformance workspace member"),
        encoding="utf-8",
    )


def add_root_feature() -> None:
    path = ROOT / "crates/tracedecay/Cargo.toml"
    text = path.read_text(encoding="utf-8")
    feature = 'memory-fabric = ["dep:tracedecay-memory-provider-registry"]\n'
    if feature not in text:
        marker = 'default = ["production"]\n'
        block = '''default = ["production"]

# Default-off M2 composition mount. The normal production graph is unchanged;
# explicit activation resolves only the product-owned provider registry.
memory-fabric = ["dep:tracedecay-memory-provider-registry"]
'''
        text = replace_once(text, marker, block, "root default feature")
    dependency = (
        'tracedecay-memory-provider-registry = { path = '
        '"../tracedecay-memory-provider-registry", optional = true }\n'
    )
    if dependency not in text:
        marker = (
            'tracedecay-maintenance = { path = "../tracedecay-maintenance", '
            'version = "0.1.0" }\n'
        )
        text = replace_once(text, marker, marker + dependency, "root maintenance dependency")
    path.write_text(text, encoding="utf-8")


def add_root_composition_mount() -> None:
    path = ROOT / "crates/tracedecay/src/runtime_ports.rs"
    text = path.read_text(encoding="utf-8")
    signature = "pub(crate) fn compose_native_memory_fabric("
    if signature in text:
        return
    marker = "fn compose_application_catalog_snapshot() -> std::result::Result<\n"
    block = '''/// Constructs the bounded Native provider composition for the explicit
/// `memory-fabric` feature. M3 supplies the existing Native application port.
#[cfg(feature = "memory-fabric")]
#[allow(dead_code)]
pub(crate) fn compose_native_memory_fabric(
    port: std::sync::Arc<dyn tracedecay_memory_provider_registry::NativeMemoryApplicationPort>,
    config: tracedecay_memory_provider_registry::NativeCompositionConfig,
) -> Result<
    tracedecay_memory_provider_registry::NativeMemoryComposition,
    tracedecay_memory_provider_registry::CompositionError,
> {
    tracedecay_memory_provider_registry::compose_native_memory(port, config)
}

'''
    path.write_text(
        replace_once(text, marker, block + marker, "application catalog composer"),
        encoding="utf-8",
    )


def update_patch_policy() -> None:
    path = ROOT / "product/upstream/patch-footprint-policy.json"
    document = json.loads(path.read_text(encoding="utf-8"))
    rows = document.get("allowed_touch_points", [])
    row = next(
        (value for value in rows if value.get("id") == "daemon_composition_mount"),
        None,
    )
    if row is None:
        raise SystemExit("missing daemon_composition_mount policy row")
    manifest = "crates/tracedecay/Cargo.toml"
    if manifest not in row["paths"]:
        row["paths"].insert(0, manifest)
    allowed = (
        "declare a default-off root feature whose only optional dependency is "
        "the product-owned provider registry"
    )
    if allowed not in row["allowed_changes"]:
        row["allowed_changes"].append(allowed)
    for verification in (
        "default-off memory composition feature tests",
        "disabled and explicitly enabled root build receipts",
    ):
        if verification not in row["required_verification"]:
            row["required_verification"].append(verification)
    path.write_text(json.dumps(document, indent=2) + "\n", encoding="utf-8")


def update_architecture_workflow() -> None:
    path = ROOT / ".github/workflows/product-memory-architecture.yml"
    path.write_text(
        '''name: Product memory architecture

on:
  push:
    paths:
      - Cargo.toml
      - Cargo.lock
      - crates/tracedecay/Cargo.toml
      - crates/tracedecay/src/runtime_ports.rs
      - crates/tracedecay-memory-*/**
      - product/architecture/memory-dependency-policy.json
      - scripts/product/check-memory-dependency-direction.py
      - scripts/product/check-memory-composition-feature.py
      - tests/product_memory_dependency_direction_test.py
      - tests/product_memory_composition_feature_test.py
      - .github/workflows/product-memory-architecture.yml
  pull_request:
    paths:
      - Cargo.toml
      - Cargo.lock
      - crates/tracedecay/Cargo.toml
      - crates/tracedecay/src/runtime_ports.rs
      - crates/tracedecay-memory-*/**
      - product/architecture/memory-dependency-policy.json
      - scripts/product/check-memory-dependency-direction.py
      - scripts/product/check-memory-composition-feature.py
      - tests/product_memory_dependency_direction_test.py
      - tests/product_memory_composition_feature_test.py
      - .github/workflows/product-memory-architecture.yml

permissions:
  contents: read

jobs:
  dependency-direction:
    runs-on: ubuntu-latest
    timeout-minutes: 10
    steps:
      - name: Check out repository
        uses: actions/checkout@v4

      - name: Run focused dependency-policy tests
        run: python3 tests/product_memory_dependency_direction_test.py

      - name: Verify the real Cargo dependency graph
        run: |
          python3 scripts/product/check-memory-dependency-direction.py \\
            --repo . \\
            --policy product/architecture/memory-dependency-policy.json

  default-off-composition:
    runs-on: ubuntu-latest
    timeout-minutes: 30
    steps:
      - name: Check out repository
        uses: actions/checkout@v4

      - name: Run focused composition-feature tests
        run: python3 tests/product_memory_composition_feature_test.py

      - name: Verify the real default-off feature mount
        run: python3 scripts/product/check-memory-composition-feature.py --repo .

      - name: Test the product-owned composition registry
        run: cargo test -p tracedecay-memory-provider-registry --locked

      - name: Compile TraceDecay without the feature
        run: cargo check -p tracedecay --lib --no-default-features --locked

      - name: Compile TraceDecay with the feature
        run: cargo check -p tracedecay --lib --no-default-features --features memory-fabric --locked
''',
        encoding="utf-8",
    )


def product_patterns(policy: dict[str, Any]) -> list[str]:
    return [value for value in policy["product_owned_paths"] if isinstance(value, str)]


def is_product_owned(path: str, patterns: list[str]) -> bool:
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


def diff_rows() -> dict[str, tuple[int, int]]:
    rows: dict[str, tuple[int, int]] = {}
    for raw in output(["git", "diff", "--no-renames", "--numstat", FLOOR, "--"]).splitlines():
        if not raw:
            continue
        added, deleted, path = raw.split("\t", 2)
        if added == "-" or deleted == "-":
            raise SystemExit(f"binary change is unsupported: {path}")
        rows[path] = (int(added), int(deleted))
    return rows


def update_convergence_map() -> None:
    policy = json.loads(
        (ROOT / "product/upstream/patch-footprint-policy.json").read_text(encoding="utf-8")
    )
    path = ROOT / "product/upstream/convergence-map.json"
    document = json.loads(path.read_text(encoding="utf-8"))
    entries = {entry["path"]: entry for entry in document["entries"]}
    for upstream_path in ("Cargo.toml", "Cargo.lock"):
        entry = entries.get(upstream_path)
        if entry is None:
            if upstream_path == "Cargo.lock" and not changed("Cargo.lock"):
                continue
            raise SystemExit(f"missing convergence entry for {upstream_path}")
        if "tdmem-0307" not in entry["bead_ids"]:
            entry["bead_ids"].append("tdmem-0307")
        for command in (
            "cargo clippy -p tracedecay-memory-provider-registry --all-targets --locked -- -D warnings",
            "cargo test -p tracedecay-memory-provider-registry --locked",
            "python3 scripts/product/check-memory-composition-feature.py --repo .",
        ):
            if command not in entry["verification"]:
                entry["verification"].append(command)

    entries["crates/tracedecay/Cargo.toml"] = {
        "bead_ids": ["tdmem-0307"],
        "line_budget": 48,
        "path": "crates/tracedecay/Cargo.toml",
        "rationale": "Declare one explicit default-off Memory Fabric feature that resolves only the product-owned provider registry.",
        "rebase_or_removal_plan": "Remove the memory-fabric feature and its optional registry dependency together; preserve the upstream default and production feature lists.",
        "semantic_invariants": [
            "The default feature list remains exactly production.",
            "The production feature does not enable memory-fabric.",
            "The root crate depends on no concrete provider adapter directly."
        ],
        "status": "active",
        "touch_point": "daemon_composition_mount",
        "verification": [
            "python3 tests/product_memory_composition_feature_test.py",
            "python3 scripts/product/check-memory-composition-feature.py --repo .",
            "cargo check -p tracedecay --lib --no-default-features --locked",
            "cargo check -p tracedecay --lib --no-default-features --features memory-fabric --locked"
        ]
    }
    entries["crates/tracedecay/src/runtime_ports.rs"] = {
        "bead_ids": ["tdmem-0307"],
        "line_budget": 48,
        "path": "crates/tracedecay/src/runtime_ports.rs",
        "rationale": "Expose the single root-owned, feature-gated constructor that delegates all concrete provider composition to the product registry.",
        "rebase_or_removal_plan": "Delete only the gated compose_native_memory_fabric function when the feature is removed; leave all existing runtime-port registration unchanged.",
        "semantic_invariants": [
            "The mount is absent unless memory-fabric is explicitly enabled.",
            "The root names no concrete Native or NCM adapter type.",
            "Construction delegates without opening stores, starting workers, or changing existing runtime-port registration."
        ],
        "status": "active",
        "touch_point": "daemon_composition_mount",
        "verification": [
            "python3 scripts/product/check-memory-composition-feature.py --repo .",
            "cargo check -p tracedecay --lib --no-default-features --features memory-fabric --locked",
            "cargo test -p tracedecay-memory-provider-registry --locked"
        ]
    }
    document["entries"] = [entries[key] for key in sorted(entries)]

    patterns = product_patterns(policy)
    rows = diff_rows()
    upstream = {
        item: counts
        for item, counts in rows.items()
        if not is_product_owned(item, patterns)
    }
    production = sum(not is_test_or_fixture(item) for item in upstream)
    tests = sum(is_test_or_fixture(item) for item in upstream)
    total_lines = sum(added + deleted for added, deleted in upstream.values())
    active = {
        entry["path"]: entry
        for entry in document["entries"]
        if entry.get("status") == "active"
    }
    composition = sum(
        active.get(item, {}).get("touch_point") == "daemon_composition_mount"
        for item in upstream
    )
    exceptions = sum(
        active.get(item, {}).get("touch_point") == "exception" for item in upstream
    )
    document["snapshot"] = {
        "upstream_existing_production_files": production,
        "upstream_existing_test_or_fixture_files": tests,
        "total_upstream_changed_lines": total_lines,
        "composition_root_files": composition,
        "exception_zone_files": exceptions,
        "observed_state": "M2 remains additive: the default TraceDecay feature graph is unchanged, while explicit memory-fabric activation reaches one thin root mount and one product-owned Native registry composition."
    }
    path.write_text(json.dumps(document, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def verify_feature_graphs() -> None:
    disabled = output(
        [
            "cargo",
            "tree",
            "--locked",
            "-p",
            "tracedecay",
            "--no-default-features",
            "-e",
            "features",
        ]
    )
    if REGISTRY in disabled:
        raise SystemExit("registry resolved without explicit memory-fabric feature")
    enabled = output(
        [
            "cargo",
            "tree",
            "--locked",
            "-p",
            "tracedecay",
            "--no-default-features",
            "--features",
            FEATURE,
            "-e",
            "features",
        ]
    )
    for package in (
        REGISTRY,
        "tracedecay-memory-fabric",
        "tracedecay-memory-provider-native",
        "tracedecay-memory-provider-api",
    ):
        if package not in enabled:
            raise SystemExit(f"enabled feature graph is missing {package}")


add_workspace_member()
add_root_feature()
add_root_composition_mount()
update_patch_policy()
update_architecture_workflow()
run(["cargo", "metadata", "--format-version", "1", "--no-deps"])
run(["cargo", "fmt", "--package", "tracedecay-memory-provider-registry"])
run(["cargo", "fmt", "--package", "tracedecay"])
run(["python3", "tests/product_memory_composition_feature_test.py"])
run(["python3", "scripts/product/check-memory-composition-feature.py", "--repo", "."])
run(["python3", "tests/product_memory_dependency_direction_test.py"])
run(
    [
        "python3",
        "scripts/product/check-memory-dependency-direction.py",
        "--repo",
        ".",
        "--policy",
        "product/architecture/memory-dependency-policy.json",
    ]
)
run(
    [
        "cargo",
        "clippy",
        "-p",
        REGISTRY,
        "--all-targets",
        "--locked",
        "--",
        "-D",
        "warnings",
    ]
)
run(["cargo", "test", "-p", REGISTRY, "--locked"])
run(
    [
        "cargo",
        "check",
        "-p",
        "tracedecay",
        "--lib",
        "--no-default-features",
        "--locked",
    ]
)
run(
    [
        "cargo",
        "check",
        "-p",
        "tracedecay",
        "--lib",
        "--no-default-features",
        "--features",
        FEATURE,
        "--locked",
    ]
)
verify_feature_graphs()
update_convergence_map()
run(["git", "diff", "--check"])

manifest: list[dict[str, str]] = []
for path, message in (
    (
        "crates/tracedecay-memory-provider-registry",
        "style(memory): format provider composition registry",
    ),
    ("Cargo.toml", "build(memory): register provider composition workspace member"),
    ("Cargo.lock", "build(memory): lock provider composition package"),
    ("crates/tracedecay/Cargo.toml", "feat(memory): add default-off Memory Fabric feature"),
    (
        "crates/tracedecay/src/runtime_ports.rs",
        "feat(memory): mount narrow Native fabric composition",
    ),
    (
        "product/upstream/patch-footprint-policy.json",
        "docs(upstream): authorize default-off composition touchpoints",
    ),
    (
        "product/upstream/convergence-map.json",
        "docs(upstream): map default-off composition wiring",
    ),
    (
        ".github/workflows/product-memory-architecture.yml",
        "ci(memory): verify default-off composition",
    ),
):
    if changed(path):
        manifest.append({"path": path, "message": message})
if not manifest:
    raise SystemExit("composition materializer produced no reviewable changes")
(ROOT / ".beads/operations/prepared-files.json").write_text(
    json.dumps(manifest, indent=2) + "\n", encoding="utf-8"
)
HERE.unlink()
