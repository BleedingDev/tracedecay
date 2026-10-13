#!/usr/bin/env python3
"""Validate the immutable vendor floor before an isolated upstream sync."""

from __future__ import annotations

import argparse
import json
import re
import subprocess
import sys
from pathlib import Path
from typing import Any, Sequence
from urllib.parse import urlparse


SHA1 = re.compile(r"^[0-9a-f]{40}$")
REPOSITORY = re.compile(r"^[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+$")


class PreflightError(ValueError):
    """The sync policy, provenance, or checkout is unsafe for a sync."""


def require_object(value: object, label: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise PreflightError(f"{label} must be a JSON object")
    return value


def require_string(mapping: dict[str, Any], key: str, label: str) -> str:
    value = mapping.get(key)
    if not isinstance(value, str) or not value.strip():
        raise PreflightError(f"{label}.{key} must be a non-empty string")
    return value.strip()


def require_string_list(mapping: dict[str, Any], key: str, label: str) -> list[str]:
    value = mapping.get(key)
    if not isinstance(value, list) or not value:
        raise PreflightError(f"{label}.{key} must be a non-empty array")
    if any(not isinstance(item, str) or not item.strip() for item in value):
        raise PreflightError(f"{label}.{key} must contain only non-empty strings")
    result = [item.strip() for item in value]
    if len(result) != len(set(result)):
        raise PreflightError(f"{label}.{key} must not contain duplicates")
    return result


def require_true(mapping: dict[str, Any], key: str, label: str) -> None:
    if mapping.get(key) is not True:
        raise PreflightError(f"{label}.{key} must be true")


def require_sha(value: str, label: str) -> str:
    if not SHA1.fullmatch(value):
        raise PreflightError(f"{label} must be a lowercase 40-character Git SHA")
    return value


def load_json(path: Path, label: str) -> dict[str, Any]:
    try:
        return require_object(json.loads(path.read_text(encoding="utf-8")), label)
    except (OSError, json.JSONDecodeError) as error:
        raise PreflightError(f"could not load {label} from {path}: {error}") from error


def resolve(repo: Path, path: str) -> Path:
    candidate = Path(path)
    return candidate if candidate.is_absolute() else repo / candidate


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
        raise PreflightError(f"git {' '.join(arguments)} failed to run: {error}") from error
    if result.returncode not in allowed_statuses:
        detail = result.stderr.strip() or result.stdout.strip() or "no diagnostic"
        raise PreflightError(
            f"git {' '.join(arguments)} exited {result.returncode}: {detail}"
        )
    return result


def canonical_github_repository(url: str, label: str) -> str:
    scp = re.fullmatch(r"git@github\.com:(?P<path>[^?#]+)", url)
    if scp:
        path = scp.group("path")
    else:
        parsed = urlparse(url)
        if parsed.scheme not in {"https", "ssh"}:
            raise PreflightError(f"{label} must use HTTPS or SSH")
        if (parsed.hostname or "").lower() != "github.com":
            raise PreflightError(f"{label} must use github.com")
        if parsed.scheme == "https" and (parsed.username or parsed.password):
            raise PreflightError(f"{label} must not contain embedded credentials")
        if parsed.scheme == "ssh" and (parsed.username != "git" or parsed.password):
            raise PreflightError(f"{label} SSH URL must use the git user without a password")
        path = parsed.path
    repository = path.strip("/")
    if repository.endswith(".git"):
        repository = repository[:-4]
    if not REPOSITORY.fullmatch(repository):
        raise PreflightError(f"{label} does not identify one owner/repository pair")
    return repository


def require_remote(repo: Path, name: str, repository: str, label: str) -> str:
    url = run_git(repo, ["remote", "get-url", name]).stdout.strip()
    actual = canonical_github_repository(url, f"{label} remote {name!r}")
    if actual.casefold() != repository.casefold():
        raise PreflightError(
            f"{label} remote {name!r} points to {actual!r}, expected {repository!r}"
        )
    return actual


def require_full_ref(repo: Path, ref: str, label: str) -> None:
    result = run_git(
        repo,
        ["check-ref-format", ref],
        allowed_statuses=frozenset({0, 1}),
    )
    if result.returncode == 1:
        raise PreflightError(f"{label} must be a valid full Git ref, got {ref!r}")


def resolve_direct_ref_commit(repo: Path, ref: str, label: str) -> str:
    symbolic = run_git(
        repo,
        ["symbolic-ref", "--quiet", ref],
        allowed_statuses=frozenset({0, 1}),
    )
    if symbolic.returncode == 0:
        raise PreflightError(f"{label} {ref!r} must not be a symbolic ref")
    result = run_git(
        repo,
        ["show-ref", "--verify", "--hash", ref],
        allowed_statuses=frozenset({0, 1}),
    )
    if result.returncode == 1:
        raise PreflightError(f"{label} {ref!r} does not resolve to a local ref")
    return resolve_commit(repo, result.stdout.strip(), label)


def resolve_commit(repo: Path, ref: str, label: str) -> str:
    result = run_git(
        repo,
        ["rev-parse", "--verify", "--quiet", f"{ref}^{{commit}}"],
        allowed_statuses=frozenset({0, 1}),
    )
    if result.returncode == 1:
        raise PreflightError(f"{label} {ref!r} does not resolve to a local commit")
    return require_sha(result.stdout.strip(), label)


def require_ancestor(repo: Path, ancestor: str, descendant: str, label: str) -> None:
    result = run_git(
        repo,
        ["merge-base", "--is-ancestor", ancestor, descendant],
        allowed_statuses=frozenset({0, 1}),
    )
    if result.returncode == 1:
        raise PreflightError(f"{label}: {ancestor} is not an ancestor of {descendant}")


def source_ancestry(repo: Path, floor: str, source: str) -> tuple[str, str]:
    merge_base = run_git(
        repo,
        ["merge-base", floor, source],
        allowed_statuses=frozenset({0, 1}),
    )
    if merge_base.returncode == 1:
        raise PreflightError(
            f"source candidate {source} has no common ancestry with pinned floor {floor}"
        )
    base = require_sha(merge_base.stdout.strip(), "source merge base")
    if base == floor:
        return "descendant_of_floor", base
    if base == source:
        return "behind_floor", base
    return "diverged_from_floor", base


def load_contract(
    repo: Path, policy_path: Path
) -> tuple[dict[str, Any], dict[str, Any]]:
    policy = load_json(policy_path, "sync policy")
    if policy.get("schema_version") != 1:
        raise PreflightError("sync policy.schema_version must be 1")
    if policy.get("authority") != "product-owned":
        raise PreflightError("sync policy.authority must be 'product-owned'")

    ownership = require_object(policy.get("ownership"), "sync policy.ownership")
    require_string(ownership, "sync_owner", "sync policy.ownership")
    require_string(ownership, "review_owner", "sync policy.ownership")
    require_string_list(ownership, "product_patch_owners", "sync policy.ownership")

    remotes = require_object(policy.get("remotes"), "sync policy.remotes")
    product_remote = require_object(remotes.get("product"), "sync policy.remotes.product")
    upstream_remote = require_object(
        remotes.get("upstream"), "sync policy.remotes.upstream"
    )
    for remote, label in (
        (product_remote, "sync policy.remotes.product"),
        (upstream_remote, "sync policy.remotes.upstream"),
    ):
        require_string(remote, "name", label)
        repository = require_string(remote, "repository", label)
        if not REPOSITORY.fullmatch(repository):
            raise PreflightError(f"{label}.repository must use owner/name form")
    if product_remote["name"] == upstream_remote["name"]:
        raise PreflightError("product and upstream remotes must use different names")

    refs = require_object(policy.get("refs"), "sync policy.refs")
    product_branch = require_string(refs, "product_branch", "sync policy.refs")
    sync_prefix = require_string(refs, "sync_branch_prefix", "sync policy.refs")
    discovery_refs = require_string_list(refs, "upstream_discovery", "sync policy.refs")
    if not product_branch.startswith("refs/heads/"):
        raise PreflightError("sync policy.refs.product_branch must be a full refs/heads ref")
    require_full_ref(repo, product_branch, "sync policy.refs.product_branch")
    if not sync_prefix.startswith("refs/heads/") or not sync_prefix.endswith("/"):
        raise PreflightError(
            "sync policy.refs.sync_branch_prefix must be a full refs/heads prefix ending in '/'"
        )
    require_full_ref(
        repo,
        f"{sync_prefix}validation-probe",
        "sync policy.refs.sync_branch_prefix",
    )
    upstream_prefix = f"refs/remotes/{upstream_remote['name']}/"
    if any(not ref.startswith(upstream_prefix) for ref in discovery_refs):
        raise PreflightError(
            "sync policy.refs.upstream_discovery entries must use the declared upstream remote"
        )
    for discovery_ref in discovery_refs:
        require_full_ref(repo, discovery_ref, "sync policy.refs.upstream_discovery entry")

    floor = require_object(policy.get("floor"), "sync policy.floor")
    require_string(floor, "metadata", "sync policy.floor")
    require_sha(require_string(floor, "sha", "sync policy.floor"), "sync policy.floor.sha")
    pull_request = floor.get("pull_request")
    if not isinstance(pull_request, int) or pull_request <= 0:
        raise PreflightError("sync policy.floor.pull_request must be a positive integer")

    preflight = require_object(policy.get("preflight"), "sync policy.preflight")
    require_true(preflight, "requires_clean_worktree", "sync policy.preflight")
    require_true(preflight, "requires_floor_ancestor", "sync policy.preflight")
    forbidden = set(
        require_string_list(
            preflight, "forbidden_direct_targets", "sync policy.preflight"
        )
    )
    required_forbidden = {
        "refs/heads/main",
        "refs/heads/master",
        f"refs/remotes/{product_remote['name']}/main",
        f"refs/remotes/{product_remote['name']}/master",
    }
    missing = sorted(required_forbidden - forbidden)
    if missing:
        raise PreflightError(
            "sync policy.preflight.forbidden_direct_targets is missing "
            + ", ".join(missing)
        )

    metadata = load_json(resolve(repo, floor["metadata"]), "floor metadata")
    return policy, metadata


def verify(repo: Path, policy_path: Path, source_ref: str | None) -> dict[str, Any]:
    repo = repo.resolve()
    top_level = Path(
        run_git(repo, ["rev-parse", "--show-toplevel"]).stdout.strip()
    ).resolve()
    policy, metadata = load_contract(top_level, policy_path)
    ownership = policy["ownership"]
    remotes = policy["remotes"]
    refs = policy["refs"]
    floor_policy = policy["floor"]
    preflight = policy["preflight"]

    if metadata.get("schema_version") != 1:
        raise PreflightError("floor metadata.schema_version must be 1")

    product = require_object(metadata.get("product"), "floor metadata.product")
    source = require_object(metadata.get("source"), "floor metadata.source")
    pinned_floor = require_object(
        metadata.get("pinned_floor"), "floor metadata.pinned_floor"
    )
    metadata_floor = require_sha(
        require_string(pinned_floor, "sha", "floor metadata.pinned_floor"),
        "floor metadata.pinned_floor.sha",
    )
    if pinned_floor.get("must_be_ancestor_of_product_head") is not True:
        raise PreflightError(
            "floor metadata.pinned_floor.must_be_ancestor_of_product_head must be true"
        )
    if metadata_floor != floor_policy["sha"]:
        raise PreflightError("sync policy floor SHA does not match canonical floor metadata")
    if source.get("pull_request") != floor_policy["pull_request"]:
        raise PreflightError("sync policy pull request does not match canonical floor metadata")
    if require_string(source, "repository", "floor metadata.source").casefold() != remotes[
        "upstream"
    ]["repository"].casefold():
        raise PreflightError("upstream repository does not match canonical floor metadata")
    if require_string(product, "repository", "floor metadata.product").casefold() != remotes[
        "product"
    ]["repository"].casefold():
        raise PreflightError("product repository does not match canonical floor metadata")
    expected_product_branch = refs["product_branch"].removeprefix("refs/heads/")
    if require_string(product, "branch", "floor metadata.product") != expected_product_branch:
        raise PreflightError("product branch does not match canonical floor metadata")

    product_repository = require_remote(
        top_level,
        remotes["product"]["name"],
        remotes["product"]["repository"],
        "product",
    )
    upstream_repository = require_remote(
        top_level,
        remotes["upstream"]["name"],
        remotes["upstream"]["repository"],
        "upstream",
    )

    source_ref = source_ref or refs["upstream_discovery"][0]
    if source_ref not in refs["upstream_discovery"]:
        raise PreflightError(
            f"source ref {source_ref!r} is not an approved upstream discovery ref"
        )
    source_sha = resolve_direct_ref_commit(top_level, source_ref, "source ref")
    product_sha = resolve_direct_ref_commit(
        top_level, refs["product_branch"], "product branch"
    )
    head_sha = resolve_commit(top_level, "HEAD", "checked-out head")
    resolve_commit(top_level, metadata_floor, "pinned floor")

    current = run_git(
        top_level,
        ["symbolic-ref", "--quiet", "HEAD"],
        allowed_statuses=frozenset({0, 1}),
    )
    if current.returncode == 1:
        raise PreflightError("sync preflight requires an attached isolated sync branch")
    current_branch = current.stdout.strip()
    forbidden = set(preflight["forbidden_direct_targets"])
    if current_branch in forbidden or current_branch.rsplit("/", 1)[-1] in {
        "main",
        "master",
    }:
        raise PreflightError(f"direct sync target {current_branch!r} is forbidden")
    if current_branch == refs["product_branch"]:
        raise PreflightError(
            "sync must not operate directly on the product branch; create an isolated sync branch"
        )
    if not current_branch.startswith(refs["sync_branch_prefix"]):
        raise PreflightError(
            f"sync branch {current_branch!r} must use prefix {refs['sync_branch_prefix']!r}"
        )
    if head_sha != product_sha:
        raise PreflightError(
            "isolated sync branch must start at the current product branch head before preflight"
        )

    tree = run_git(
        top_level, ["status", "--porcelain=v1", "--untracked-files=all"]
    ).stdout
    if tree:
        first_paths = [line[3:] for line in tree.splitlines()[:8]]
        suffix = "" if len(tree.splitlines()) <= 8 else ", ..."
        raise PreflightError(
            "working tree is not clean: " + ", ".join(first_paths) + suffix
        )

    require_ancestor(top_level, metadata_floor, product_sha, "floor ancestry failed")
    require_ancestor(top_level, metadata_floor, head_sha, "checked-out ancestry failed")
    source_relationship, source_merge_base = source_ancestry(
        top_level, metadata_floor, source_sha
    )

    return {
        "ok": True,
        "tree_state": "clean",
        "sync_owner": ownership["sync_owner"],
        "review_owner": ownership["review_owner"],
        "product_patch_owners": ownership["product_patch_owners"],
        "product_repository": product_repository,
        "upstream_repository": upstream_repository,
        "product_branch": refs["product_branch"],
        "product_head_sha": product_sha,
        "sync_branch": current_branch,
        "sync_head_sha": head_sha,
        "source_ref": source_ref,
        "source_sha": source_sha,
        "source_relationship": source_relationship,
        "source_merge_base": source_merge_base,
        "floor_pull_request": floor_policy["pull_request"],
        "floor_sha": metadata_floor,
    }


def main() -> None:
    root = Path(__file__).resolve().parents[2]
    parser = argparse.ArgumentParser()
    parser.add_argument("--repo", type=Path, default=root)
    parser.add_argument(
        "--policy",
        type=Path,
        default=root / "product/upstream/sync-policy.json",
    )
    parser.add_argument(
        "--source-ref",
        help="approved moving upstream ref to resolve to an immutable candidate SHA",
    )
    args = parser.parse_args()

    policy_path = args.policy if args.policy.is_absolute() else args.repo / args.policy
    try:
        evidence = verify(args.repo, policy_path, args.source_ref)
    except PreflightError as error:
        print(
            json.dumps({"ok": False, "errors": [str(error)]}, indent=2, sort_keys=True)
        )
        raise SystemExit(1) from error
    print(json.dumps(evidence, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
