#!/usr/bin/env python3
"""Materialize the validated provider-neutral memory conformance crate."""

from __future__ import annotations

import json
import subprocess
from pathlib import Path

HERE = Path(__file__).resolve()
ROOT = HERE.parents[3]
VALIDATED_COMMIT = "8f234a1faaba1d93b2a6754db32502bf3308a6a7"
FLOOR = "08fbe33a7c7f403191fd5d6e356c7b6681b96403"
CRATE_PATHS = [
    "crates/tracedecay-memory-conformance/Cargo.toml",
    "crates/tracedecay-memory-conformance/README.md",
    "crates/tracedecay-memory-conformance/src/lib.rs",
    "crates/tracedecay-memory-conformance/tests/dummy_provider.rs",
]


def run(*argv: str, capture: bool = False) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        list(argv),
        cwd=ROOT,
        check=True,
        capture_output=capture,
        text=True,
    )


def write_validated(path: str) -> None:
    result = run("git", "show", f"{VALIDATED_COMMIT}:{path}", capture=True)
    target = ROOT / path
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(result.stdout, encoding="utf-8")


def register_workspace_member() -> None:
    path = ROOT / "Cargo.toml"
    text = path.read_text(encoding="utf-8")
    marker = '    "crates/tracedecay-memory-provider-ncm",\n'
    member = '    "crates/tracedecay-memory-conformance",\n'
    if member in text:
        return
    if marker not in text:
        raise SystemExit("NCM workspace marker is missing")
    path.write_text(text.replace(marker, marker + member, 1), encoding="utf-8")


def update_convergence_map() -> None:
    path = ROOT / "product/upstream/convergence-map.json"
    value = json.loads(path.read_text(encoding="utf-8"))
    entries = {entry["path"]: entry for entry in value["entries"]}
    for upstream_path in ("Cargo.toml", "Cargo.lock"):
        entry = entries.get(upstream_path)
        if entry is None:
            raise SystemExit(f"missing convergence entry for {upstream_path}")
        if "tdmem-0305" not in entry["bead_ids"]:
            entry["bead_ids"].append("tdmem-0305")
        for command in (
            "cargo clippy -p tracedecay-memory-conformance --all-targets --locked -- -D warnings",
            "cargo test -p tracedecay-memory-conformance --locked",
        ):
            if command not in entry["verification"]:
                entry["verification"].append(command)

    result = run(
        "git",
        "diff",
        "--no-renames",
        "--numstat",
        FLOOR,
        "--",
        "Cargo.toml",
        "Cargo.lock",
        capture=True,
    )
    changed_lines = 0
    changed_files = 0
    for line in result.stdout.splitlines():
        if not line:
            continue
        added, deleted, _ = line.split("\t", 2)
        changed_lines += int(added) + int(deleted)
        changed_files += 1
    value["snapshot"].update(
        {
            "composition_root_files": 0,
            "exception_zone_files": 0,
            "observed_state": (
                "The product branch changes only additive workspace membership and generated "
                "path-package lock entries; provider API, fabric, Native adapter, topology-neutral "
                "NCM boundary, and provider-neutral conformance harness remain product-owned."
            ),
            "total_upstream_changed_lines": changed_lines,
            "upstream_existing_production_files": changed_files,
            "upstream_existing_test_or_fixture_files": 0,
        }
    )
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n", encoding="utf-8")


for crate_path in CRATE_PATHS:
    write_validated(crate_path)
register_workspace_member()
run("cargo", "check", "-p", "tracedecay-memory-conformance", "--all-targets")
run("cargo", "fmt", "--package", "tracedecay-memory-conformance")
update_convergence_map()

messages = {
    "crates/tracedecay-memory-conformance/Cargo.toml": "feat(memory): add conformance crate manifest",
    "crates/tracedecay-memory-conformance/README.md": "docs(memory): explain provider-neutral conformance boundary",
    "crates/tracedecay-memory-conformance/src/lib.rs": "feat(memory): add provider conformance and differential harness",
    "crates/tracedecay-memory-conformance/tests/dummy_provider.rs": "test(memory): prove mandatory conformance and observer isolation",
    "Cargo.toml": "build(memory): register conformance workspace crate",
    "Cargo.lock": "build(memory): lock conformance workspace crate",
    "product/upstream/convergence-map.json": "docs(upstream): map conformance workspace wiring",
}
manifest: list[dict[str, str]] = []
for path, message in messages.items():
    status = run(
        "git",
        "status",
        "--porcelain",
        "--untracked-files=all",
        "--",
        path,
        capture=True,
    ).stdout
    if status.strip():
        manifest.append({"path": path, "message": message})
if not manifest:
    raise SystemExit("conformance materializer produced no reviewable changes")
(ROOT / ".beads/operations/prepared-files.json").write_text(
    json.dumps(manifest, indent=2) + "\n",
    encoding="utf-8",
)
HERE.unlink()
