#!/usr/bin/env python3
"""Verify that a checkout still contains its immutable upstream floor."""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Sequence


SHA1 = re.compile(r"^[0-9a-f]{40}$")
REPOSITORY = re.compile(r"^[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+$")


class VerificationError(ValueError):
    """The provenance document or checked-out Git history violates policy."""


@dataclass(frozen=True)
class Provenance:
    schema_version: int
    product_repository: str
    product_branch: str
    source_repository: str
    source_pull_request: int
    pinned_floor_sha: str


def require_mapping(value: object, authority: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise VerificationError(f"{authority} must be a JSON object")
    return value


def require_string(mapping: dict[str, Any], key: str, authority: str) -> str:
    value = mapping.get(key)
    if not isinstance(value, str) or not value.strip():
        raise VerificationError(f"{authority}.{key} must be a non-empty string")
    return value.strip()


def require_sha(value: str, authority: str) -> str:
    if not SHA1.fullmatch(value):
        raise VerificationError(f"{authority} must be a lowercase 40-character Git SHA")
    return value


def load_provenance(path: Path) -> tuple[Provenance, bytes]:
    raw = path.read_bytes()
    document = require_mapping(json.loads(raw), "metadata")
    schema_version = document.get("schema_version")
    if schema_version != 1:
        raise VerificationError(
            f"metadata.schema_version must be 1, got {schema_version!r}"
        )

    product = require_mapping(document.get("product"), "metadata.product")
    source = require_mapping(document.get("source"), "metadata.source")
    floor = require_mapping(document.get("pinned_floor"), "metadata.pinned_floor")
    observed = require_mapping(
        document.get("observed_pull_request"), "metadata.observed_pull_request"
    )
    update = require_mapping(
        document.get("update_procedure"), "metadata.update_procedure"
    )

    product_repository = require_string(product, "repository", "metadata.product")
    source_repository = require_string(source, "repository", "metadata.source")
    if not REPOSITORY.fullmatch(product_repository):
        raise VerificationError("metadata.product.repository must use owner/name form")
    if not REPOSITORY.fullmatch(source_repository):
        raise VerificationError("metadata.source.repository must use owner/name form")

    pull_request = source.get("pull_request")
    if not isinstance(pull_request, int) or pull_request <= 0:
        raise VerificationError("metadata.source.pull_request must be a positive integer")

    require_sha(
        require_string(observed, "base_sha", "metadata.observed_pull_request"),
        "metadata.observed_pull_request.base_sha",
    )
    require_sha(
        require_string(observed, "head_sha", "metadata.observed_pull_request"),
        "metadata.observed_pull_request.head_sha",
    )
    require_string(update, "observed_pull_request", "metadata.update_procedure")
    require_string(update, "pinned_floor", "metadata.update_procedure")

    must_be_ancestor = floor.get("must_be_ancestor_of_product_head")
    if must_be_ancestor is not True:
        raise VerificationError(
            "metadata.pinned_floor.must_be_ancestor_of_product_head must be true"
        )

    return (
        Provenance(
            schema_version=schema_version,
            product_repository=product_repository,
            product_branch=require_string(product, "branch", "metadata.product"),
            source_repository=source_repository,
            source_pull_request=pull_request,
            pinned_floor_sha=require_sha(
                require_string(floor, "sha", "metadata.pinned_floor"),
                "metadata.pinned_floor.sha",
            ),
        ),
        raw,
    )


def run_git(
    repo: Path,
    arguments: Sequence[str],
    *,
    allowed_statuses: frozenset[int] = frozenset({0}),
) -> subprocess.CompletedProcess[str]:
    try:
        result = subprocess.run(
            ["git", "-C", str(repo), *arguments],
            check=False,
            capture_output=True,
            text=True,
            timeout=30,
        )
    except (OSError, subprocess.TimeoutExpired) as error:
        raise VerificationError(f"git {' '.join(arguments)} failed to run: {error}") from error
    if result.returncode not in allowed_statuses:
        detail = result.stderr.strip() or result.stdout.strip() or "no diagnostic"
        raise VerificationError(
            f"git {' '.join(arguments)} exited {result.returncode}: {detail}"
        )
    return result


def verify(
    repo: Path,
    metadata_path: Path,
    *,
    require_product_branch: bool,
) -> dict[str, object]:
    provenance, metadata_raw = load_provenance(metadata_path)
    repo = repo.resolve()

    top_level = Path(
        run_git(repo, ["rev-parse", "--show-toplevel"]).stdout.strip()
    ).resolve()
    head = run_git(top_level, ["rev-parse", "HEAD"]).stdout.strip()
    floor = provenance.pinned_floor_sha

    run_git(top_level, ["cat-file", "-e", f"{floor}^{{commit}}"])
    ancestry = run_git(
        top_level,
        ["merge-base", "--is-ancestor", floor, head],
        allowed_statuses=frozenset({0, 1}),
    )
    if ancestry.returncode == 1:
        raise VerificationError(
            f"pinned floor {floor} is not an ancestor of checked-out head {head}"
        )

    merge_base = run_git(top_level, ["merge-base", floor, head]).stdout.strip()
    if merge_base != floor:
        raise VerificationError(
            f"merge base {merge_base} does not equal pinned floor {floor}"
        )

    branch_result = run_git(
        top_level,
        ["symbolic-ref", "--quiet", "--short", "HEAD"],
        allowed_statuses=frozenset({0, 1}),
    )
    current_branch = branch_result.stdout.strip() if branch_result.returncode == 0 else None
    if require_product_branch and current_branch != provenance.product_branch:
        raise VerificationError(
            "checked-out branch mismatch: "
            f"expected {provenance.product_branch!r}, got {current_branch or 'detached HEAD'!r}"
        )

    ahead_by = int(
        run_git(top_level, ["rev-list", "--count", f"{floor}..{head}"]).stdout.strip()
    )
    return {
        "schema_version": 1,
        "verified": True,
        "product_repository": provenance.product_repository,
        "product_branch": provenance.product_branch,
        "checked_out_branch": current_branch,
        "checked_out_head": head,
        "pinned_floor_sha": floor,
        "merge_base": merge_base,
        "ahead_by": ahead_by,
        "source_repository": provenance.source_repository,
        "source_pull_request": provenance.source_pull_request,
        "metadata_sha256": hashlib.sha256(metadata_raw).hexdigest(),
    }


def main() -> None:
    root = Path(__file__).resolve().parents[1]
    parser = argparse.ArgumentParser()
    parser.add_argument("--repo", type=Path, default=root)
    parser.add_argument(
        "--metadata",
        type=Path,
        default=root / "product/upstream/tracedecay-v2-pr707.json",
    )
    parser.add_argument("--require-product-branch", action="store_true")
    args = parser.parse_args()

    try:
        receipt = verify(
            args.repo,
            args.metadata,
            require_product_branch=args.require_product_branch,
        )
    except (OSError, json.JSONDecodeError, VerificationError) as error:
        print(f"product upstream floor verification failed: {error}", file=sys.stderr)
        raise SystemExit(1) from error

    print(json.dumps(receipt, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
