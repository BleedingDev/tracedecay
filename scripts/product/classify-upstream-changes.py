#!/usr/bin/env python3
"""Classify an upstream floor transition against product convergence authority."""

from __future__ import annotations

import argparse
import importlib.util
import json
import subprocess
import sys
import tomllib
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any, Iterable

REPORT_KIND = "tracedecay.upstream-change-classification.v1"
SCHEMA_VERSION = 1
MAX_DIAGNOSTICS = 50
STATUS_NAMES = {
    "A": "added",
    "B": "broken_pairing",
    "C": "copied",
    "D": "deleted",
    "M": "modified",
    "R": "renamed",
    "T": "type_changed",
    "U": "unmerged",
    "X": "unknown",
}
RELATION_RANK = {"shared_touch_point": 1, "shared_area": 2, "direct_path": 3}
EXPECTED_CLASSIFICATION_CONTRACT = {
    "path_format": "repo-relative-posix",
    "precedence": [
        "active_upstream_entry_exact_path",
        "product_area_path_pattern",
        "policy_touch_point_path",
    ],
    "ambiguous_match": "error",
    "unclassified_path": "error",
}


class ClassificationError(RuntimeError):
    """An input or git failure that prevents a classification report."""


class Diagnostics:
    """Collect every failure count while bounding rendered diagnostic payloads."""

    def __init__(self, limit: int = MAX_DIAGNOSTICS) -> None:
        self.limit = limit
        self.total = 0
        self.items: list[dict[str, str]] = []

    def add(self, code: str, message: str, *, path: str | None = None) -> None:
        self.total += 1
        if len(self.items) >= self.limit:
            return
        item = {"code": code, "message": message}
        if path is not None:
            item["path"] = path
        self.items.append(item)

    def report(self) -> dict[str, Any]:
        return {
            "items": self.items,
            "shown": len(self.items),
            "total": self.total,
            "truncated": self.total - len(self.items),
        }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Classify files changed between two substantive git commits."
    )
    parser.add_argument("--repo", type=Path, default=Path("."))
    parser.add_argument("--old-floor", required=True, help="Old floor commit or ref")
    parser.add_argument(
        "--candidate-floor", required=True, help="Candidate floor commit or ref"
    )
    parser.add_argument(
        "--map",
        dest="map_path",
        type=Path,
        default=Path("product/upstream/convergence-map.json"),
    )
    parser.add_argument(
        "--policy",
        type=Path,
        default=Path("product/upstream/patch-footprint-policy.json"),
    )
    return parser.parse_args()


def resolve(repo: Path, path: Path) -> Path:
    return path if path.is_absolute() else repo / path


def load_registry_helpers() -> Any:
    """Reuse the ownership registry's canonical repo-path glob semantics."""
    helper_path = Path(__file__).with_name("check-upstream-ownership-registry.py")
    spec = importlib.util.spec_from_file_location(
        "product_upstream_ownership_registry_helpers", helper_path
    )
    if spec is None or spec.loader is None:
        raise ClassificationError(f"could not load path matcher from {helper_path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    if not callable(getattr(module, "path_matches", None)):
        raise ClassificationError(f"path matcher is missing from {helper_path}")
    if not callable(getattr(module, "validate_repo_path", None)):
        raise ClassificationError(f"path validator is missing from {helper_path}")
    return module


REGISTRY_HELPERS = load_registry_helpers()
PATH_MATCHES = REGISTRY_HELPERS.path_matches


def utf8_key(value: str) -> bytes:
    return value.encode("utf-8", errors="surrogateescape")


def sorted_strings(values: Iterable[str]) -> list[str]:
    return sorted(set(values), key=utf8_key)


def git(
    repo: Path,
    arguments: list[str],
    *,
    check: bool = True,
) -> subprocess.CompletedProcess[bytes]:
    result = subprocess.run(
        ["git", "-C", str(repo), *arguments],
        check=False,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    if check and result.returncode != 0:
        detail = result.stderr.decode("utf-8", errors="replace").strip()
        command = "git " + " ".join(arguments[:2])
        raise ClassificationError(f"{command} failed: {detail or 'unknown git error'}")
    return result


def git_text(repo: Path, arguments: list[str]) -> str:
    return git(repo, arguments).stdout.decode("utf-8", errors="replace").strip()


def resolve_commit(repo: Path, reference: str) -> dict[str, str]:
    commit = git_text(
        repo,
        ["rev-parse", "--verify", "--end-of-options", f"{reference}^{{commit}}"],
    )
    tree = git_text(repo, ["show", "-s", "--format=%T", commit])
    return {"requested": reference, "commit": commit, "tree": tree}


def commit_relationship(repo: Path, old: str, candidate: str) -> dict[str, Any]:
    merge_base_result = git(repo, ["merge-base", old, candidate], check=False)
    merge_base = (
        merge_base_result.stdout.decode("ascii", errors="replace").strip()
        if merge_base_result.returncode == 0
        else None
    )
    ancestor = git(repo, ["merge-base", "--is-ancestor", old, candidate], check=False)
    if ancestor.returncode not in (0, 1):
        detail = ancestor.stderr.decode("utf-8", errors="replace").strip()
        raise ClassificationError(
            f"git merge-base --is-ancestor failed: {detail or 'unknown git error'}"
        )
    return {"merge_base": merge_base, "old_is_ancestor": ancestor.returncode == 0}


def load_object(path: Path, label: str) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ClassificationError(f"could not load {label}: {exc}") from exc
    if type(value) is not dict:
        raise ClassificationError(f"{label} root must be an object")
    return value


def validate_authority_headers(
    registry: dict[str, Any],
    policy: dict[str, Any],
    old_commit: str,
    diagnostics: Diagnostics,
) -> None:
    if registry.get("schema_version") != 2:
        diagnostics.add(
            "invalid_authority", "convergence map schema_version must be 2"
        )
    if registry.get("classification_contract") != EXPECTED_CLASSIFICATION_CONTRACT:
        diagnostics.add(
            "invalid_authority",
            "convergence map classification_contract does not match the v2 precedence",
        )
    map_revision = registry.get("policy_revision")
    policy_revision = policy.get("policy_revision")
    if not isinstance(map_revision, str) or not map_revision:
        diagnostics.add(
            "invalid_authority", "convergence map policy_revision must be a string"
        )
    if map_revision != policy_revision:
        diagnostics.add(
            "stale_authority",
            "convergence map and patch policy revisions do not match",
        )
    accepted_floor = registry.get("upstream_floor_sha")
    if accepted_floor != old_commit:
        diagnostics.add(
            "stale_authority",
            "old floor commit does not equal convergence map upstream_floor_sha",
        )
    policy_floor = policy.get("upstream_floor")
    policy_floor_sha = (
        policy_floor.get("sha") if isinstance(policy_floor, dict) else None
    )
    if policy_floor_sha != old_commit:
        diagnostics.add(
            "stale_authority",
            "old floor commit does not equal patch policy upstream_floor.sha",
        )


def normalized_repo_path(value: str, label: str, *, allow_glob: bool) -> bool:
    try:
        value.encode("utf-8")
    except UnicodeEncodeError:
        return False
    errors: list[str] = []
    REGISTRY_HELPERS.validate_repo_path(
        value, label, errors, allow_glob=allow_glob
    )
    return not errors


def changed_paths(repo: Path, old: str, candidate: str) -> list[dict[str, str]]:
    raw = git(
        repo,
        [
            "diff",
            "--name-status",
            "-z",
            "--no-renames",
            "--no-ext-diff",
            "--no-textconv",
            "--diff-filter=ACDMRTUXB",
            old,
            candidate,
            "--",
        ],
    ).stdout
    tokens = raw.split(b"\0")
    if tokens and tokens[-1] == b"":
        tokens.pop()
    if len(tokens) % 2 != 0:
        raise ClassificationError("git diff returned an incomplete name-status record")
    rows: list[dict[str, str]] = []
    for offset in range(0, len(tokens), 2):
        raw_status = tokens[offset].decode("ascii", errors="replace")
        path = tokens[offset + 1].decode("utf-8", errors="surrogateescape")
        status_code = raw_status[:1]
        rows.append(
            {
                "path": path,
                "status": STATUS_NAMES.get(status_code, "unknown"),
                "status_code": raw_status,
            }
        )
    return sorted(rows, key=lambda row: utf8_key(row["path"]))


def committed_crates(repo: Path, commit: str) -> list[dict[str, str]]:
    raw = git(repo, ["ls-tree", "-r", "-z", "--name-only", commit, "--"]).stdout
    paths = [
        value.decode("utf-8", errors="surrogateescape")
        for value in raw.split(b"\0")
        if value
    ]
    manifests = [
        path for path in paths if path == "Cargo.toml" or path.endswith("/Cargo.toml")
    ]
    crates: list[dict[str, str]] = []
    for manifest in sorted(manifests, key=utf8_key):
        contents = git(repo, ["show", f"{commit}:{manifest}"]).stdout
        try:
            document = tomllib.loads(contents.decode("utf-8"))
        except (UnicodeDecodeError, tomllib.TOMLDecodeError):
            continue
        package = document.get("package")
        name = package.get("name") if isinstance(package, dict) else None
        if not isinstance(name, str) or not name:
            continue
        root = str(Path(manifest).parent)
        if root == ".":
            root = ""
        crates.append({"manifest": manifest, "name": name, "root": root})
    return crates


def crate_for_path(
    path: str,
    candidate_crates: list[dict[str, str]],
    old_crates: list[dict[str, str]],
) -> dict[str, str] | None:
    def choose(rows: list[dict[str, str]]) -> dict[str, str] | None:
        matches = [
            row
            for row in rows
            if (not row["root"])
            or path == row["manifest"]
            or path.startswith(row["root"] + "/")
        ]
        if not matches:
            return None
        selected = max(matches, key=lambda row: len(row["root"]))
        return {key: selected[key] for key in ("name", "root", "manifest")}

    return choose(candidate_crates) or choose(old_crates)


def crates_for_change(
    change: dict[str, str],
    old_crates: list[dict[str, str]],
    candidate_crates: list[dict[str, str]],
) -> tuple[dict[str, str] | None, dict[str, str] | None, dict[str, str] | None]:
    path = change["path"]
    before = crate_for_path(path, old_crates, [])
    after = crate_for_path(path, candidate_crates, [])
    if change["status"] == "deleted":
        selected = before
    elif change["status"] == "added":
        selected = after
    else:
        selected = after or before
    return before, after, selected


def index_active_rows(
    rows: Any,
    label: str,
    diagnostics: Diagnostics,
) -> dict[str, dict[str, Any]]:
    if type(rows) is not list:
        diagnostics.add("invalid_authority", f"{label} must be an array")
        return {}
    indexed: dict[str, dict[str, Any]] = {}
    for offset, raw in enumerate(rows):
        if type(raw) is not dict:
            diagnostics.add(
                "invalid_authority", f"{label}[{offset}] must be an object"
            )
            continue
        row_id = raw.get("id")
        if type(row_id) is not str or not row_id:
            diagnostics.add(
                "invalid_authority", f"{label}[{offset}].id must be a string"
            )
            continue
        if raw.get("status", "active") != "active":
            continue
        if row_id in indexed:
            diagnostics.add(
                "ambiguous_authority", f"{label} contains duplicate active id {row_id!r}"
            )
            continue
        patterns = raw.get("path_patterns")
        if type(patterns) is not list or not patterns:
            diagnostics.add(
                "invalid_authority",
                f"{label}[{offset}].path_patterns must be a non-empty array",
            )
        else:
            for pattern in patterns:
                if type(pattern) is not str or not normalized_repo_path(
                    pattern, f"{label}[{offset}].path_patterns", allow_glob=True
                ):
                    diagnostics.add(
                        "invalid_authority",
                        f"{label}[{offset}] contains a non-normalized path pattern",
                    )
                    break
        if raw.get("ownership_class") not in {"product_owned", "upstream_owned"}:
            diagnostics.add(
                "invalid_authority",
                f"{label}[{offset}].ownership_class is invalid",
            )
        indexed[row_id] = raw
    return indexed


def index_policy_rows(
    rows: Any,
    label: str,
    diagnostics: Diagnostics,
) -> dict[str, dict[str, Any]]:
    if type(rows) is not list:
        diagnostics.add("invalid_policy", f"{label} must be an array")
        return {}
    indexed: dict[str, dict[str, Any]] = {}
    for offset, raw in enumerate(rows):
        if type(raw) is not dict or type(raw.get("id")) is not str:
            diagnostics.add(
                "invalid_policy", f"{label}[{offset}] must contain a string id"
            )
            continue
        row_id = raw["id"]
        if row_id in indexed:
            diagnostics.add(
                "ambiguous_policy", f"{label} contains duplicate id {row_id!r}"
            )
            continue
        patterns = raw.get("paths")
        if type(patterns) is not list or not patterns:
            diagnostics.add(
                "invalid_policy", f"{label}[{offset}].paths must be a non-empty array"
            )
        else:
            for pattern in patterns:
                if type(pattern) is not str or not normalized_repo_path(
                    pattern, f"{label}[{offset}].paths", allow_glob=True
                ):
                    diagnostics.add(
                        "invalid_policy",
                        f"{label}[{offset}] contains a non-normalized path pattern",
                    )
                    break
        indexed[row_id] = raw
    return indexed


def row_patterns(row: dict[str, Any], field: str) -> list[str]:
    value = row.get(field, [])
    if type(value) is not list:
        return []
    return [pattern for pattern in value if type(pattern) is str and pattern]


def matching_ids(
    path: str,
    rows: dict[str, dict[str, Any]],
    pattern_field: str,
) -> list[str]:
    return sorted_strings(
        row_id
        for row_id, row in rows.items()
        if any(PATH_MATCHES(path, pattern) for pattern in row_patterns(row, pattern_field))
    )


def active_entries(
    rows: Any, diagnostics: Diagnostics
) -> dict[str, dict[str, Any]]:
    if type(rows) is not list:
        diagnostics.add("invalid_authority", "convergence map entries must be an array")
        return {}
    indexed: dict[str, dict[str, Any]] = {}
    for offset, raw in enumerate(rows):
        if type(raw) is not dict or raw.get("status", "active") != "active":
            continue
        path = raw.get("path")
        if type(path) is not str or not path:
            diagnostics.add(
                "invalid_authority", f"entries[{offset}].path must be a string"
            )
            continue
        if not normalized_repo_path(path, f"entries[{offset}].path", allow_glob=False):
            diagnostics.add(
                "invalid_authority",
                f"entries[{offset}].path must be normalized repo-relative POSIX",
                path=path,
            )
            continue
        if path in indexed:
            diagnostics.add(
                "ambiguous_authority",
                f"convergence map contains duplicate active path {path!r}",
                path=path,
            )
            continue
        indexed[path] = raw
    return indexed


def patch_relations(
    path: str,
    selected_area_ids: list[str],
    touch_point_ids: list[str],
    entries: dict[str, dict[str, Any]],
) -> dict[str, str]:
    relations: dict[str, str] = {}
    for patch_path, entry in entries.items():
        candidates: list[str] = []
        if patch_path == path:
            candidates.append("direct_path")
        if entry.get("area_id") in selected_area_ids:
            candidates.append("shared_area")
        if entry.get("touch_point") in touch_point_ids:
            candidates.append("shared_touch_point")
        if candidates:
            relations[patch_path] = max(candidates, key=RELATION_RANK.__getitem__)
    return relations


def strongest_conflict(
    relations: dict[str, str], classification_status: str
) -> str:
    if classification_status == "product_area":
        return "high"
    if classification_status in {"unmapped", "ambiguous"}:
        return "review_required"
    if classification_status == "unrelated_upstream":
        return "none"
    kinds = set(relations.values())
    if "direct_path" in kinds:
        return "high"
    if "shared_area" in kinds:
        return "medium"
    if "shared_touch_point" in kinds:
        return "low"
    return "none"


def classify(
    repo: Path,
    old_floor: dict[str, str],
    candidate_floor: dict[str, str],
    registry: dict[str, Any],
    policy: dict[str, Any],
) -> tuple[dict[str, Any], int]:
    diagnostics = Diagnostics()
    validate_authority_headers(
        registry, policy, old_floor["commit"], diagnostics
    )
    relationship = commit_relationship(
        repo, old_floor["commit"], candidate_floor["commit"]
    )
    if not relationship["old_is_ancestor"]:
        if relationship["merge_base"] is None:
            message = (
                "candidate floor has no merge base with the old floor and is not "
                "a descendant"
            )
        else:
            message = "candidate floor is not a descendant of the old floor"
        diagnostics.add("non_descendant_candidate", message)
    areas = index_active_rows(registry.get("areas"), "convergence map areas", diagnostics)
    entries = active_entries(registry.get("entries"), diagnostics)
    touch_points = index_policy_rows(
        policy.get("allowed_touch_points"), "allowed touch points", diagnostics
    )
    zones = index_policy_rows(policy.get("exception_zones", []), "exception zones", diagnostics)
    for area_id, area in areas.items():
        if area.get("last_verified_upstream_sha") != old_floor["commit"]:
            diagnostics.add(
                "stale_authority",
                f"active area {area_id!r} is not verified at the old floor",
            )
    for path, entry in entries.items():
        if entry.get("last_verified_upstream_sha") != old_floor["commit"]:
            diagnostics.add(
                "stale_authority",
                f"active product patch {path!r} is not verified at the old floor",
                path=path,
            )

    upstream_owner = ""
    owners = registry.get("owners")
    if isinstance(owners, dict):
        upstream = owners.get("upstream")
        if isinstance(upstream, dict) and isinstance(upstream.get("id"), str):
            upstream_owner = upstream["id"]

    changes = changed_paths(repo, old_floor["commit"], candidate_floor["commit"])
    changed_path_set = {row["path"] for row in changes}
    old_crates = committed_crates(repo, old_floor["commit"])
    candidate_crates = committed_crates(repo, candidate_floor["commit"])

    classified: list[dict[str, Any]] = []
    exact_patch_matches: dict[str, dict[str, str]] = defaultdict(dict)
    authority_counts: Counter[tuple[str, str, str, str]] = Counter()
    touch_counts: Counter[str] = Counter()
    zone_counts: Counter[str] = Counter()
    unmapped_count = 0

    for change in changes:
        path = change["path"]
        if not normalized_repo_path(path, "changed path", allow_glob=False):
            diagnostics.add(
                "invalid_changed_path",
                "git changed path is not normalized repo-relative POSIX",
                path=path,
            )
        direct_entry = entries.get(path)
        area_ids = matching_ids(path, areas, "path_patterns")
        product_area_ids = [
            area_id
            for area_id in area_ids
            if areas[area_id].get("ownership_class") == "product_owned"
        ]
        upstream_area_ids = [
            area_id
            for area_id in area_ids
            if areas[area_id].get("ownership_class") == "upstream_owned"
        ]
        explicit_touch_ids = matching_ids(path, touch_points, "paths")
        zone_ids = matching_ids(path, zones, "paths")
        selected_area_ids: list[str] = []
        classification_status = "unrelated_upstream"
        if direct_entry is not None:
            entry_area = direct_entry.get("area_id")
            if type(entry_area) is str and entry_area in areas:
                selected_area_ids = [entry_area]
                classification_status = "mapped"
            else:
                classification_status = "ambiguous"
                diagnostics.add(
                    "invalid_patch_authority",
                    f"active product patch {path!r} references a missing active area",
                    path=path,
                )
        elif len(product_area_ids) > 1 or len(upstream_area_ids) > 1:
            selected_area_ids = area_ids
            classification_status = "ambiguous"
            diagnostics.add(
                "ambiguous_area",
                f"changed path matches multiple active ownership areas: {area_ids!r}",
                path=path,
            )
        elif len(product_area_ids) == 1:
            selected_area_ids = product_area_ids
            classification_status = "product_area"
        elif upstream_area_ids or explicit_touch_ids or zone_ids:
            selected_area_ids = upstream_area_ids
            classification_status = "unmapped"
            unmapped_count += 1
            diagnostics.add(
                "unmapped_touched_area",
                "product-relevant upstream path lacks an active exact convergence entry",
                path=path,
            )

        declared_touch_ids = [
            touch_id
            for area_id in selected_area_ids
            for touch_id in row_patterns(areas[area_id], "touch_points")
            if touch_id in touch_points
        ]
        touch_ids = sorted_strings([*explicit_touch_ids, *declared_touch_ids])

        generated_by_entry = isinstance(
            direct_entry.get("generated") if direct_entry else None, dict
        )
        generated_by_zone = direct_entry is None and bool(zone_ids) and all(
            zones[zone_id].get("default_policy") == "generated_only"
            for zone_id in zone_ids
        )
        change_kind = "generated" if generated_by_entry or generated_by_zone else "semantic"
        generated_evidence: dict[str, Any] | None = None
        if generated_by_entry and direct_entry is not None:
            metadata = direct_entry["generated"]
            generator_path = metadata.get("generator_path")
            generated_evidence = {
                "generator_changed": generator_path in changed_path_set,
                "generator_path": str(generator_path or ""),
                "reproduction": str(metadata.get("reproduction", "")),
                "source": "convergence_entry",
                "zero_drift_check": str(metadata.get("zero_drift_check", "")),
            }
        elif generated_by_zone:
            generated_evidence = {
                "source": "policy_generated_only_zone",
                "zones": zone_ids,
            }

        relations = patch_relations(path, selected_area_ids, touch_ids, entries)
        if direct_entry is not None and classification_status == "mapped":
            exact_patch_matches[path][path] = "direct_path"

        authorities: list[dict[str, str]] = []
        for area_id in selected_area_ids:
            area = areas[area_id]
            authority = {
                "id": area_id,
                "kind": "ownership_area",
                "owner": str(area.get("owner", "")),
                "ownership_class": str(area.get("ownership_class", "")),
            }
            authorities.append(authority)
            authority_counts[
                (
                    authority["id"],
                    authority["kind"],
                    authority["owner"],
                    authority["ownership_class"],
                )
            ] += 1
        if not authorities and classification_status not in {"ambiguous"}:
            authority = {
                "id": "canonical_upstream",
                "kind": "repository_owner",
                "owner": upstream_owner,
                "ownership_class": "upstream_owned",
            }
            authorities.append(authority)
            authority_counts[
                (
                    authority["id"],
                    authority["kind"],
                    authority["owner"],
                    authority["ownership_class"],
                )
            ] += 1

        for touch_id in touch_ids:
            touch_counts[touch_id] += 1
        for zone_id in zone_ids:
            zone_counts[zone_id] += 1

        crate_before, crate_after, selected_crate = crates_for_change(
            change, old_crates, candidate_crates
        )
        classified.append(
            {
                **change,
                "authorities": authorities,
                "change_kind": change_kind,
                "classification_status": classification_status,
                "crate": selected_crate,
                "crate_after": crate_after,
                "crate_before": crate_before,
                "generated_evidence": generated_evidence,
                "likely_conflict": strongest_conflict(relations, classification_status),
                "mapped_product_patches": [
                    {"path": path, "relation": "direct_path"}
                ]
                if direct_entry is not None and classification_status == "mapped"
                else [],
                "related_product_patches": [
                    {"path": patch_path, "relation": relations[patch_path]}
                    for patch_path in sorted(relations, key=utf8_key)
                    if patch_path != path
                ],
                "touch_points": touch_ids,
                "zones": zone_ids,
            }
        )

    crate_files: dict[tuple[str, str, str], set[str]] = defaultdict(set)
    crate_sides: dict[tuple[str, str, str], set[str]] = defaultdict(set)
    for row in classified:
        for side in ("before", "after"):
            crate = row[f"crate_{side}"]
            if crate is None:
                continue
            key = (crate["name"], crate["root"], crate["manifest"])
            crate_files[key].add(row["path"])
            crate_sides[key].add(side)
    changed_crates = [
        {
            "changed_file_count": len(crate_files[key]),
            "manifest": key[2],
            "name": key[0],
            "present_after": "after" in crate_sides[key],
            "present_before": "before" in crate_sides[key],
            "root": key[1],
        }
        for key in sorted(crate_files, key=lambda item: (utf8_key(item[0]), utf8_key(item[1])))
    ]

    affected_authorities = [
        {
            "changed_file_count": count,
            "id": key[0],
            "kind": key[1],
            "owner": key[2],
            "ownership_class": key[3],
        }
        for key, count in sorted(authority_counts.items(), key=lambda item: item[0])
    ]
    affected_touch_points = [
        {"changed_file_count": touch_counts[touch_id], "id": touch_id}
        for touch_id in sorted(touch_counts, key=utf8_key)
    ]
    affected_zones = [
        {
            "changed_file_count": zone_counts[zone_id],
            "default_policy": str(zones[zone_id].get("default_policy", "")),
            "id": zone_id,
        }
        for zone_id in sorted(zone_counts, key=utf8_key)
    ]

    mapped_patches: list[dict[str, Any]] = []
    for patch_path in sorted(exact_patch_matches, key=utf8_key):
        entry = entries[patch_path]
        changed_files = exact_patch_matches[patch_path]
        generated = entry.get("generated")
        mapped_patches.append(
            {
                "area_id": str(entry.get("area_id", "")),
                "bead_ids": sorted_strings(
                    value
                    for value in entry.get("bead_ids", [])
                    if type(value) is str
                ),
                "changed_files": [
                    {"path": path, "relation": changed_files[path]}
                    for path in sorted(changed_files, key=utf8_key)
                ],
                "generated": generated if isinstance(generated, dict) else None,
                "path": patch_path,
                "touch_point": str(entry.get("touch_point", "")),
            }
        )

    kind_counts = Counter(row["change_kind"] for row in classified)
    status_counts = Counter(row["status"] for row in classified)
    conflict_counts = Counter(row["likely_conflict"] for row in classified)
    if not classified:
        change_set_kind = "no_changes"
    elif kind_counts["generated"] == len(classified):
        change_set_kind = "generated_only"
    elif kind_counts["semantic"] == len(classified):
        change_set_kind = "semantic"
    else:
        change_set_kind = "mixed"

    review_gate = "fail" if diagnostics.total else "pass"
    report = {
        "affected_authorities": affected_authorities,
        "affected_touch_points": affected_touch_points,
        "affected_zones": affected_zones,
        "changed_crates": changed_crates,
        "changed_files": classified,
        "diagnostics": diagnostics.report(),
        "inputs": {
            "candidate_floor": candidate_floor,
            "old_floor": old_floor,
        },
        "mapped_product_patches": mapped_patches,
        "relationship": relationship,
        "report_kind": REPORT_KIND,
        "schema_version": SCHEMA_VERSION,
        "summary": {
            "auto_accept": False,
            "change_set_kind": change_set_kind,
            "changed_crate_count": len(changed_crates),
            "changed_file_count": len(classified),
            "conflict_counts": dict(sorted(conflict_counts.items())),
            "generated_file_count": kind_counts["generated"],
            "mapped_product_patch_count": len(mapped_patches),
            "review_gate": review_gate,
            "semantic_file_count": kind_counts["semantic"],
            "status_counts": dict(sorted(status_counts.items())),
            "unmapped_file_count": unmapped_count,
        },
        "unmapped_paths": sorted_strings(
            row["path"]
            for row in classified
            if row["classification_status"] == "unmapped"
        ),
    }
    return report, (0 if review_gate == "pass" else 1)


def failure_report(message: str) -> dict[str, Any]:
    return {
        "diagnostics": {
            "items": [{"code": "classification_error", "message": message}],
            "shown": 1,
            "total": 1,
            "truncated": 0,
        },
        "report_kind": REPORT_KIND,
        "schema_version": SCHEMA_VERSION,
        "summary": {
            "auto_accept": False,
            "review_gate": "error",
        },
    }


def emit(report: dict[str, Any]) -> None:
    json.dump(report, sys.stdout, indent=2, sort_keys=True, ensure_ascii=True)
    sys.stdout.write("\n")


def main() -> int:
    args = parse_args()
    try:
        repo = args.repo.resolve()
        registry = load_object(resolve(repo, args.map_path), "convergence map")
        policy = load_object(resolve(repo, args.policy), "patch-footprint policy")
        old_floor = resolve_commit(repo, args.old_floor)
        candidate_floor = resolve_commit(repo, args.candidate_floor)
        report, status = classify(repo, old_floor, candidate_floor, registry, policy)
    except ClassificationError as exc:
        emit(failure_report(str(exc)))
        return 2
    emit(report)
    return status


if __name__ == "__main__":
    raise SystemExit(main())
