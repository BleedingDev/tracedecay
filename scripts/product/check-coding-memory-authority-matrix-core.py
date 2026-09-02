#!/usr/bin/env python3
"""Validate the versioned coding-memory authority matrix against the repository."""

from __future__ import annotations

import argparse
import json
import re
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable

EXPECTED_AXES = {
    "profile_id",
    "project_id",
    "repository_id",
    "worktree_id",
    "branch_ref",
    "agent_session_id",
    "provider_id",
    "provider_instance_id",
    "request_id",
    "operation_id",
}

EXPECTED_DOMAINS = {
    "current_code_truth",
    "repository_identity",
    "worktree_identity",
    "branch_identity",
    "session_evidence",
    "explicit_facts",
    "provider_observation_journal",
    "provider_cognitive_state",
    "provider_recall_candidates",
    "curated_rules",
    "final_compiled_context",
}

DURABLE_DOMAINS = {
    "current_code_truth",
    "repository_identity",
    "worktree_identity",
    "branch_identity",
    "session_evidence",
    "explicit_facts",
    "provider_observation_journal",
    "provider_cognitive_state",
    "curated_rules",
}

NON_CANONICAL_DOMAINS = {
    "provider_recall_candidates",
    "final_compiled_context",
}

EXPECTED_CLASSES = {
    "current_code_truth": "canonical",
    "repository_identity": "canonical_identity",
    "worktree_identity": "canonical_identity",
    "branch_identity": "canonical_identity",
    "session_evidence": "canonical",
    "explicit_facts": "canonical",
    "provider_observation_journal": "canonical",
    "provider_cognitive_state": "canonical_external",
    "provider_recall_candidates": "advisory_ephemeral",
    "curated_rules": "canonical",
    "final_compiled_context": "ephemeral_assembly",
}

EXPECTED_RULES = {
    "truth_precedence",
    "single_writer",
    "explicit_promotion",
    "observer_isolation",
    "scope_isolation",
    "provenance_and_freshness",
    "typed_degradation",
    "no_silent_fallback",
    "provider_name_isolation",
}

EXPECTED_LANE_ORDER = [
    "current_code_truth",
    "curated_rules",
    "explicit_facts",
    "session_evidence",
    "provider_recall_candidates",
]

DOC_LINE = re.compile(r"^\s*(?:///|//!)\s?(.*)$")
ATTRIBUTE_LINE = re.compile(r"^\s*#!?\[")


@dataclass(frozen=True)
class DocAssertion:
    """A rustdoc invariant that must survive as ONE attached assertion.

    Rust doc comments wrap across `///` lines, so a literal substring check
    cannot see a multi-line sentence. This marker instead normalizes each doc
    block (line prefixes stripped, all whitespace collapsed to single spaces)
    and requires two things at once:

    * ``phrase`` must appear CONTIGUOUSLY in the normalized block, so the
      clauses cannot be separated, reordered, or scattered into unrelated
      comments elsewhere in the file; and
    * the block must be the documentation attached to an item whose
      declaration starts with ``item_prefix``, so the assertion has to
      document the real production accessor it constrains rather than sit in
      a detached historical note beside a contradicting implementation.
    """

    phrase: str
    item_prefix: str

    def describe(self) -> str:
        return f"rustdoc assertion on `{self.item_prefix}`: {self.phrase!r}"


def collapse_whitespace(value: str) -> str:
    return " ".join(value.split())


def rust_doc_blocks(text: str) -> list[tuple[str, str]]:
    """Return ``(normalized doc text, attached item declaration)`` pairs.

    A doc block ends at the first line that is not a doc line. Attributes
    between the docs and the item are skipped (they still attach). Anything
    else — including a blank line, which detaches the comment in Rust — ends
    the block and becomes the "item", so a detached note yields an item that
    matches no ``item_prefix``.
    """
    blocks: list[tuple[str, str]] = []
    current: list[str] = []
    for line in text.splitlines():
        matched = DOC_LINE.match(line)
        if matched is not None:
            current.append(matched.group(1))
            continue
        if not current:
            continue
        if ATTRIBUTE_LINE.match(line):
            continue
        blocks.append((collapse_whitespace(" ".join(current)), collapse_whitespace(line)))
        current = []
    if current:
        blocks.append((collapse_whitespace(" ".join(current)), ""))
    return blocks


def doc_assertion_holds(text: str, assertion: DocAssertion) -> bool:
    phrase = collapse_whitespace(assertion.phrase)
    prefix = collapse_whitespace(assertion.item_prefix)
    for doc, item in rust_doc_blocks(text):
        if phrase in doc and item.startswith(prefix):
            return True
    return False


SOURCE_MARKERS = {
    "crates/tracedecay/src/tracedecay/edits/file_authority.rs": [
        "struct SourceEditFileAuthority",
        "fn publish",
        "expected_identity",
    ],
    "crates/tracedecay/src/daemon/project_open_owners/source_edit_owner.rs": [
        "source_edit_request_context",
        "current_authority",
        "SourceEditEffectControlV1",
    ],
    "crates/tracedecay-session-memory/src/context/registered_scope.rs": [
        "pub struct RegisteredScopeResolver",
        "read_repository_identity_marker",
        "current_branch",
        "UnauthorizedSiblingRoot",
    ],
    "crates/tracedecay-session-memory/src/context/mod.rs": [
        "pub struct ResolvedGitRoute",
        "pub struct ResolvedSessionIdentity",
        "RepositoryId",
        "WorktreeId",
        "BranchId",
        "application_scope",
    ],
    "crates/tracedecay/src/mcp/tools/handlers/hook_runtime/ingest.rs": [
        "HostAdmissionFacade",
        "transcript_capture_kernel",
    ],
    "crates/tracedecay-session-runtime/src/session_retrieval/admitted.rs": [
        "SessionApplicationRetrievalPortV1",
        "retrieve_admitted_with_cancellation",
    ],
    "crates/tracedecay-session-memory/src/memory/mod.rs": [
        "pub struct MemoryApplication",
        "memory_application_for_db",
        "DatabaseFactStore",
    ],
    "crates/tracedecay-runtime-core/src/store/memory/mod.rs": [
        "pub struct DatabaseFactStore",
        "impl ProjectMemoryFactStore",
        "schedule_project_memory_graph_reconciliation",
    ],
    "crates/tracedecay-configuration/src/configuration/runtime.rs": [
        "pub struct ProjectConfigurationRuntime",
        "transactional store handle",
        # Single-writer configuration authority. The assertion is one rustdoc
        # sentence that wraps across a `///` line break in the source, so a
        # plain substring check can never see it contiguously. `DocAssertion`
        # normalizes the doc block (strips `///` prefixes, collapses
        # whitespace) and then requires the WHOLE sentence contiguously, and
        # requires that doc block to be the documentation attached to the
        # public accessor it constrains. Splitting the sentence across
        # unrelated comments, or leaving it as a free-floating historical
        # note while the accessor documents a second authority, is rejected.
        DocAssertion(
            phrase=(
                "Effective values and revisions must be read from "
                "[`Self::client`] so the retained store remains the sole "
                "runtime configuration authority."
            ),
            item_prefix="pub fn configuration_target",
        ),
    ],
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--repo", type=Path, default=Path("."))
    parser.add_argument(
        "--matrix",
        dest="matrix_path",
        type=Path,
        default=Path("product/architecture/coding-memory-authority-matrix.json"),
    )
    return parser.parse_args()


def resolve(repo: Path, path: Path) -> Path:
    return path if path.is_absolute() else repo / path


def require_list(value: Any, field: str, errors: list[str]) -> list[Any]:
    if not isinstance(value, list):
        errors.append(f"{field} must be an array")
        return []
    return value


def index_by_id(rows: Iterable[Any], field: str, errors: list[str]) -> dict[str, dict[str, Any]]:
    indexed: dict[str, dict[str, Any]] = {}
    for offset, raw in enumerate(rows):
        if not isinstance(raw, dict):
            errors.append(f"{field}[{offset}] must be an object")
            continue
        row_id = raw.get("id")
        if not isinstance(row_id, str) or not row_id:
            errors.append(f"{field}[{offset}].id must be a non-empty string")
            continue
        if row_id in indexed:
            errors.append(f"{field} contains duplicate id {row_id!r}")
            continue
        indexed[row_id] = raw
    return indexed


def non_empty_string(row: dict[str, Any], field: str, authority: str, errors: list[str]) -> str:
    value = row.get(field)
    if not isinstance(value, str) or not value.strip():
        errors.append(f"{authority}.{field} must be a non-empty string")
        return ""
    return value.strip()


def validate_axes(document: dict[str, Any], errors: list[str]) -> dict[str, dict[str, Any]]:
    rows = require_list(document.get("namespace_axes"), "namespace_axes", errors)
    indexed = index_by_id(rows, "namespace_axes", errors)
    missing = EXPECTED_AXES - indexed.keys()
    extra = indexed.keys() - EXPECTED_AXES
    if missing:
        errors.append(f"namespace axes missing: {sorted(missing)}")
    if extra:
        errors.append(f"unexpected namespace axes: {sorted(extra)}")
    for axis_id, row in indexed.items():
        non_empty_string(row, "authority", axis_id, errors)
        non_empty_string(row, "purpose", axis_id, errors)
    return indexed


def validate_namespace_variants(
    domain_id: str,
    row: dict[str, Any],
    errors: list[str],
) -> set[str]:
    variants = require_list(row.get("namespace_variants"), f"{domain_id}.namespace_variants", errors)
    if not variants:
        errors.append(f"{domain_id} must define at least one namespace variant")
    names: set[str] = set()
    required_axes: set[str] = set()
    for offset, raw in enumerate(variants):
        if not isinstance(raw, dict):
            errors.append(f"{domain_id}.namespace_variants[{offset}] must be an object")
            continue
        name = raw.get("name")
        if not isinstance(name, str) or not name:
            errors.append(f"{domain_id}.namespace_variants[{offset}].name must be non-empty")
            continue
        if name in names:
            errors.append(f"{domain_id} contains duplicate namespace variant {name!r}")
        names.add(name)
        buckets: dict[str, set[str]] = {}
        for bucket in ("required", "optional", "forbidden"):
            values = require_list(
                raw.get(bucket),
                f"{domain_id}.{name}.{bucket}",
                errors,
            )
            if any(not isinstance(value, str) for value in values):
                errors.append(f"{domain_id}.{name}.{bucket} entries must be strings")
                values = [value for value in values if isinstance(value, str)]
            bucket_values = set(values)
            unknown = bucket_values - EXPECTED_AXES
            if unknown:
                errors.append(
                    f"{domain_id}.{name}.{bucket} contains unknown axes: {sorted(unknown)}"
                )
            if len(bucket_values) != len(values):
                errors.append(f"{domain_id}.{name}.{bucket} contains duplicate axes")
            buckets[bucket] = bucket_values
        for left, right in (("required", "optional"), ("required", "forbidden"), ("optional", "forbidden")):
            overlap = buckets[left] & buckets[right]
            if overlap:
                errors.append(
                    f"{domain_id}.{name} places axes in both {left} and {right}: {sorted(overlap)}"
                )
        required_axes.update(buckets["required"])
    return required_axes


def validate_domains(
    document: dict[str, Any],
    errors: list[str],
) -> tuple[dict[str, dict[str, Any]], set[str]]:
    rows = require_list(document.get("authority_domains"), "authority_domains", errors)
    indexed = index_by_id(rows, "authority_domains", errors)
    missing = EXPECTED_DOMAINS - indexed.keys()
    extra = indexed.keys() - EXPECTED_DOMAINS
    if missing:
        errors.append(f"authority domains missing: {sorted(missing)}")
    if extra:
        errors.append(f"unexpected authority domains: {sorted(extra)}")

    covered_required_axes: set[str] = set()
    owners: dict[str, str] = {}
    for domain_id, row in indexed.items():
        expected_class = EXPECTED_CLASSES.get(domain_id)
        if row.get("authority_class") != expected_class:
            errors.append(f"{domain_id}.authority_class must be {expected_class}")
        non_empty_string(row, "implementation_state", domain_id, errors)
        owner = non_empty_string(row, "owner", domain_id, errors)
        if owner:
            owners[domain_id] = owner
        for field in (
            "consistency_model",
            "failure_behavior",
            "provider_semantics",
        ):
            non_empty_string(row, field, domain_id, errors)
        for field in ("write_interfaces", "read_interfaces", "prohibited_side_effects"):
            values = require_list(row.get(field), f"{domain_id}.{field}", errors)
            if field != "write_interfaces" or domain_id in DURABLE_DOMAINS:
                if not values:
                    errors.append(f"{domain_id}.{field} must not be empty")
            if any(not isinstance(value, str) or not value.strip() for value in values):
                errors.append(f"{domain_id}.{field} entries must be non-empty strings")
        covered_required_axes.update(validate_namespace_variants(domain_id, row, errors))

        writer = row.get("canonical_writer")
        if domain_id in DURABLE_DOMAINS:
            if not isinstance(writer, str) or not writer.strip():
                errors.append(f"{domain_id} must name exactly one canonical writer")
        elif domain_id in NON_CANONICAL_DOMAINS and writer is not None:
            errors.append(f"{domain_id} is non-canonical and canonical_writer must be null")
        if "canonical_writers" in row or "alternate_writers" in row or "co_writers" in row:
            errors.append(f"{domain_id} must not define plural or alternate canonical writers")

    if len(owners) != len(set(owners.values())):
        collisions: dict[str, list[str]] = {}
        for domain_id, owner in owners.items():
            collisions.setdefault(owner, []).append(domain_id)
        duplicate_owners = {owner: ids for owner, ids in collisions.items() if len(ids) > 1}
        errors.append(f"authority domains share an owner identifier: {duplicate_owners}")

    missing_coverage = {
        "repository_id",
        "worktree_id",
        "branch_ref",
        "agent_session_id",
        "provider_id",
        "provider_instance_id",
    } - covered_required_axes
    if missing_coverage:
        errors.append(
            "required namespace coverage is missing for: " + str(sorted(missing_coverage))
        )
    return indexed, covered_required_axes


def contains_all(value: str, markers: Iterable[str]) -> bool:
    lowered = value.lower()
    return all(marker.lower() in lowered for marker in markers)


def validate_domain_decisions(domains: dict[str, dict[str, Any]], errors: list[str]) -> None:
    explicit = domains.get("explicit_facts", {})
    if explicit.get("native_surface_authority") != "native_explicit_fact_log":
        errors.append("explicit_facts must map to native_explicit_fact_log")
    if not contains_all(str(explicit.get("canonical_writer", "")), ["Native", "MemoryApplication"]):
        errors.append("explicit_facts must retain Native MemoryApplication as canonical writer")
    explicit_prohibited = "\n".join(explicit.get("prohibited_side_effects", []))
    if not contains_all(explicit_prohibited, ["provider", "multiple canonical fact writers"]):
        errors.append("explicit_facts must prohibit provider and multiple-writer mutation")

    code = domains.get("current_code_truth", {})
    code_prohibited = "\n".join(code.get("prohibited_side_effects", []))
    if not contains_all(code_prohibited, ["provider", "memory", "worktree"]):
        errors.append("current_code_truth must prohibit provider/memory override and scope drift")

    session = domains.get("session_evidence", {})
    if not contains_all(str(session.get("provider_semantics", "")), ["mirrored", "never replaces"]):
        errors.append("session_evidence must remain separate from provider state")

    observations = domains.get("provider_observation_journal", {})
    if observations.get("owner") != "product_observation_dispatcher":
        errors.append("provider_observation_journal must have one product dispatcher owner")
    observation_failure = str(observations.get("failure_behavior", ""))
    if not contains_all(observation_failure, ["cannot alter prompts", "cannot alter", "Capacity exhaustion"]):
        errors.append("provider observations must fail visibly without canonical influence")

    cognitive = domains.get("provider_cognitive_state", {})
    if cognitive.get("owner") != "selected_provider_instance":
        errors.append("provider_cognitive_state must be owned only by the selected provider instance")
    if not contains_all(str(cognitive.get("provider_semantics", "")), ["sole internal authority", "registry"]):
        errors.append("provider cognitive state must remain adapter/registry isolated")

    recall = domains.get("provider_recall_candidates", {})
    if recall.get("provider_semantics") != "advisory_only":
        errors.append("provider recall must be explicitly advisory_only")
    recall_prohibited = "\n".join(recall.get("prohibited_side_effects", []))
    for marker in ("direct source edit", "direct Native fact mutation", "direct configuration", "silent fallback"):
        if marker not in recall_prohibited:
            errors.append(f"provider recall must prohibit {marker!r}")

    rules = domains.get("curated_rules", {})
    if rules.get("owner") != "configuration_control_plane":
        errors.append("curated_rules must be owned by the configuration control plane")
    if not contains_all(str(rules.get("canonical_writer", "")), ["transactional", "configuration"]):
        errors.append("curated_rules must use the transactional configuration writer")

    compiled = domains.get("final_compiled_context", {})
    if compiled.get("owner") != "tracedecay_context_compiler":
        errors.append("TraceDecay context compiler must solely own final context assembly")
    if not contains_all(str(compiled.get("failure_behavior", "")), ["Code-truth", "partial", "never silently"]):
        errors.append("final context must fail code truth closed and expose typed partial provider lanes")
    compiled_prohibited = "\n".join(compiled.get("prohibited_side_effects", []))
    if not contains_all(compiled_prohibited, ["provider constructing", "replacing code truth", "provenance", "mutating"]):
        errors.append("final context must prohibit provider ownership, truth override, and mutation")


def validate_rules(document: dict[str, Any], errors: list[str]) -> None:
    rows = require_list(document.get("cross_domain_rules"), "cross_domain_rules", errors)
    indexed = index_by_id(rows, "cross_domain_rules", errors)
    missing = EXPECTED_RULES - indexed.keys()
    extra = indexed.keys() - EXPECTED_RULES
    if missing:
        errors.append(f"cross-domain rules missing: {sorted(missing)}")
    if extra:
        errors.append(f"unexpected cross-domain rules: {sorted(extra)}")
    for rule_id, row in indexed.items():
        non_empty_string(row, "rule", rule_id, errors)

    no_fallback = str(indexed.get("no_silent_fallback", {}).get("rule", ""))
    if not contains_all(no_fallback, ["never switches", "explicit", "observable"]):
        errors.append("no_silent_fallback rule must reject implicit provider switching")
    single_writer = str(indexed.get("single_writer", {}).get("rule", ""))
    if not contains_all(single_writer, ["one canonical writer", "never become co-writers"]):
        errors.append("single_writer rule must reject projection/adapter co-writers")
    precedence = str(indexed.get("truth_precedence", {}).get("rule", ""))
    if not contains_all(precedence, ["Current code truth", "Native facts", "provider candidates"]):
        errors.append("truth_precedence must name code, Native facts, and provider candidates")


def validate_lane_order(document: dict[str, Any], errors: list[str]) -> None:
    rows = require_list(document.get("context_lane_order"), "context_lane_order", errors)
    if len(rows) != len(EXPECTED_LANE_ORDER):
        errors.append(f"context_lane_order must have {len(EXPECTED_LANE_ORDER)} rows")
        return
    ordered: list[str] = []
    for expected_rank, raw in enumerate(rows, start=1):
        if not isinstance(raw, dict):
            errors.append(f"context_lane_order[{expected_rank - 1}] must be an object")
            continue
        if raw.get("rank") != expected_rank:
            errors.append(f"context lane rank {expected_rank} is malformed")
        domain = raw.get("domain")
        if not isinstance(domain, str):
            errors.append(f"context lane rank {expected_rank} must name a domain")
            continue
        ordered.append(domain)
        non_empty_string(raw, "role", f"context lane {expected_rank}", errors)
    if ordered != EXPECTED_LANE_ORDER:
        errors.append(
            "context lane precedence must be: " + " > ".join(EXPECTED_LANE_ORDER)
        )


def repository_paths(document: dict[str, Any]) -> list[tuple[str, str]]:
    values: list[tuple[str, str]] = []
    source = document.get("source_surface_map")
    if isinstance(source, str):
        values.append(("root", source))
    verification = document.get("verification")
    if isinstance(verification, dict):
        for key in ("checker", "native_surface_map"):
            value = verification.get(key)
            if isinstance(value, str):
                values.append(("verification", value))
        tests = verification.get("tests")
        if isinstance(tests, list):
            values.extend(("verification", value) for value in tests if isinstance(value, str))
    domains = document.get("authority_domains")
    if isinstance(domains, list):
        for raw in domains:
            if not isinstance(raw, dict):
                continue
            state = raw.get("implementation_state")
            domain_id = raw.get("id")
            paths = raw.get("source_paths")
            if not isinstance(paths, list):
                continue
            # Planned domains may reference the intended product seam that a later
            # implementation bead will create. Current/current-extended domains
            # must resolve now.
            if state == "planned":
                continue
            for value in paths:
                if isinstance(value, str):
                    values.append((str(domain_id), value))
    return values


def validate_paths(repo: Path, document: dict[str, Any], errors: list[str]) -> None:
    for authority, raw in repository_paths(document):
        path = Path(raw)
        if path.is_absolute() or ".." in path.parts:
            errors.append(f"{authority} path must be repository-relative: {raw}")
            continue
        if not (repo / path).exists():
            errors.append(f"{authority} references a missing repository path: {raw}")

    companion = repo / "product/architecture/coding-memory-authority-matrix.md"
    if not companion.is_file():
        errors.append("Markdown authority-matrix companion is missing")

    for raw, markers in SOURCE_MARKERS.items():
        path = repo / raw
        if not path.is_file():
            errors.append(f"source marker file is missing: {raw}")
            continue
        text = path.read_text(encoding="utf-8")
        for marker in markers:
            if isinstance(marker, DocAssertion):
                if not doc_assertion_holds(text, marker):
                    errors.append(f"{raw} is missing production {marker.describe()}")
                continue
            if marker not in text:
                errors.append(f"{raw} is missing production marker {marker!r}")


def load_native_surface_map(repo: Path, document: dict[str, Any], errors: list[str]) -> dict[str, Any]:
    raw = document.get("source_surface_map")
    if not isinstance(raw, str):
        errors.append("source_surface_map must be a repository-relative path")
        return {}
    path = repo / raw
    try:
        surface = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        errors.append(f"could not load native surface map: {exc}")
        return {}
    if not isinstance(surface, dict):
        errors.append("native surface map root must be an object")
        return {}
    return surface


def validate_native_surface_alignment(
    repo: Path,
    document: dict[str, Any],
    domains: dict[str, dict[str, Any]],
    errors: list[str],
) -> None:
    surface = load_native_surface_map(repo, document, errors)
    if not surface:
        return
    if surface.get("bead_id") != "tdmem-0103":
        errors.append("native surface map must be the completed tdmem-0103 authority")
    authorities = {
        row.get("id")
        for row in surface.get("authorities", [])
        if isinstance(row, dict) and isinstance(row.get("id"), str)
    }
    if "native_explicit_fact_log" not in authorities:
        errors.append("native surface map lacks native_explicit_fact_log")
    if domains.get("explicit_facts", {}).get("native_surface_authority") not in authorities:
        errors.append("explicit_facts references an authority absent from the Native surface map")

    seams = {
        row.get("rank"): row.get("id")
        for row in surface.get("provider_seams", [])
        if isinstance(row, dict) and isinstance(row.get("rank"), int)
    }
    expected_seams = {
        1: "normalized_observation_fanout",
        2: "advisory_recall_contributor",
        3: "outcome_and_feedback_fanout",
        4: "capability_registry_and_daemon_composition",
    }
    for rank, seam_id in expected_seams.items():
        if seams.get(rank) != seam_id:
            errors.append(f"Native surface seam rank {rank} must remain {seam_id}")


def validate_document(repo: Path, document: dict[str, Any]) -> list[str]:
    errors: list[str] = []
    if document.get("schema_version") != 1:
        errors.append("schema_version must be 1")
    if document.get("bead_id") != "tdmem-0104":
        errors.append("bead_id must be tdmem-0104")
    decision = document.get("decision")
    if not isinstance(decision, str) or not contains_all(
        decision,
        ["TraceDecay owns", "accepted Native facts", "advisory candidates"],
    ):
        errors.append("decision must preserve TraceDecay authority and advisory providers")

    invariants = require_list(document.get("global_invariants"), "global_invariants", errors)
    invariant_text = "\n".join(value for value in invariants if isinstance(value, str))
    for marker in (
        "exactly one canonical authority",
        "Current code",
        "TraceDecay Native",
        "Provider recall candidates are advisory",
        "context compiler",
        "Repository, worktree, branch, agent-session, and provider namespaces",
    ):
        if marker not in invariant_text:
            errors.append(f"global invariants are missing {marker!r}")

    validate_axes(document, errors)
    domains, _ = validate_domains(document, errors)
    validate_domain_decisions(domains, errors)
    validate_rules(document, errors)
    validate_lane_order(document, errors)
    validate_paths(repo, document, errors)
    validate_native_surface_alignment(repo, document, domains, errors)
    return errors


def main() -> int:
    args = parse_args()
    repo = args.repo.resolve()
    matrix_path = resolve(repo, args.matrix_path)
    try:
        document = json.loads(matrix_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        print(json.dumps({"ok": False, "errors": [f"could not load matrix: {exc}"]}))
        return 1
    if not isinstance(document, dict):
        print(json.dumps({"ok": False, "errors": ["matrix root must be an object"]}))
        return 1

    errors = validate_document(repo, document)
    if errors:
        print(json.dumps({"ok": False, "errors": errors}, indent=2, sort_keys=True))
        return 1

    receipt = {
        "ok": True,
        "schema_version": document["schema_version"],
        "bead_id": document["bead_id"],
        "namespace_axes": len(document["namespace_axes"]),
        "authority_domains": len(document["authority_domains"]),
        "durable_domains": len(DURABLE_DOMAINS),
        "cross_domain_rules": len(document["cross_domain_rules"]),
        "context_lanes": len(document["context_lane_order"]),
        "matrix": str(matrix_path.relative_to(repo)),
    }
    print(json.dumps(receipt, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    sys.exit(main())
