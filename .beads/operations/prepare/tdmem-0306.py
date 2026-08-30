#!/usr/bin/env python3
"""Materialize the validated exact-edge memory dependency policy."""

from __future__ import annotations

import json
import subprocess
from pathlib import Path

HERE = Path(__file__).resolve()
ROOT = HERE.parents[3]
VALIDATED_COMMIT = "7013ebc7f18a9ea024d722ff39633c57b7463054"
VALIDATED_PATHS = [
    "scripts/product/check-memory-dependency-policy.py",
    "tests/product_memory_dependency_policy_test.py",
    "product/architecture/memory-dependency-policy.md",
    ".github/workflows/product-memory-dependencies.yml",
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
    if path.endswith(".py"):
        target.chmod(0o755)


for validated_path in VALIDATED_PATHS:
    write_validated(validated_path)

policy_path = ROOT / "product/upstream/patch-footprint-policy.json"
policy = json.loads(policy_path.read_text(encoding="utf-8"))
policy["dependency_direction_exception_contract"] = {
    "required_fields": [
        "id",
        "rule_id",
        "from_package",
        "to_package",
        "adr",
        "rationale",
        "reviewed_by",
        "status",
    ],
    "status_values": ["active", "retired"],
    "exact_edge_only": True,
    "adr_prefix": "product/architecture/adr/",
}
policy["dependency_direction_exceptions"] = []
policy_path.write_text(json.dumps(policy, indent=2, sort_keys=True) + "\n", encoding="utf-8")

run(
    "python3",
    "-m",
    "py_compile",
    "scripts/product/check-memory-dependency-policy.py",
    "tests/product_memory_dependency_policy_test.py",
)
run("python3", "tests/product_memory_dependency_policy_test.py")
run(
    "python3",
    "scripts/product/check-memory-dependency-policy.py",
    "--repo",
    ".",
    "--policy",
    "product/upstream/patch-footprint-policy.json",
)
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

messages = {
    "scripts/product/check-memory-dependency-policy.py": "feat(memory): enforce exact dependency-direction edges",
    "tests/product_memory_dependency_policy_test.py": "test(memory): cover forbidden edges and ADR exceptions",
    "product/architecture/memory-dependency-policy.md": "docs(memory): define dependency exception governance",
    ".github/workflows/product-memory-dependencies.yml": "ci(memory): gate dependency direction and footprint",
    "product/upstream/patch-footprint-policy.json": "policy(memory): require exact ADR-bound dependency exceptions",
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
    raise SystemExit("dependency-policy materializer produced no reviewable changes")
(ROOT / ".beads/operations/prepared-files.json").write_text(
    json.dumps(manifest, indent=2) + "\n",
    encoding="utf-8",
)
HERE.unlink()
