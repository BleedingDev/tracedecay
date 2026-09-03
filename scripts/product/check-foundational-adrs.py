#!/usr/bin/env python3
"""Validate the foundational pluggable-memory ADR set."""

from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path
from typing import Any, Iterable

EXPECTED_DECISIONS = {
    "ADR-0001": "provider_boundary",
    "ADR-0002": "authority_and_advisory_semantics",
    "ADR-0003": "monorepo_layout_and_dependency_direction",
    "ADR-0004": "provider_execution_topology_decision_gate",
    "ADR-0005": "persistence_idempotency_and_recovery",
    "ADR-0006": "context_compilation",
    "ADR-0007": "observer_isolation_and_activation",
    "ADR-0008": "upstream_convergence",
    "ADR-0009": "ncm_execution_topology",
    "ADR-0010": "native_provider_parity_projection",
    "ADR-0011": "patch_footprint_revision_v2",
    "ADR-0012": "configuration_registry_additive_settings",
    "ADR-0013": "daemon_shutdown_touch_point_expansion",
    "ADR-0014": "host_hook_ingest_footprint_revision_v3",
}

EXPECTED_PATHS = {
    "ADR-0001": "product/architecture/adr/ADR-0001-provider-boundary.md",
    "ADR-0002": "product/architecture/adr/ADR-0002-authority-and-advisory-semantics.md",
    "ADR-0003": "product/architecture/adr/ADR-0003-monorepo-layout.md",
    "ADR-0004": "product/architecture/adr/ADR-0004-provider-execution-topology-gate.md",
    "ADR-0005": "product/architecture/adr/ADR-0005-persistence-and-observation-delivery.md",
    "ADR-0006": "product/architecture/adr/ADR-0006-context-compilation.md",
    "ADR-0007": "product/architecture/adr/ADR-0007-observer-isolation-and-activation.md",
    "ADR-0008": "product/architecture/adr/ADR-0008-upstream-convergence.md",
    "ADR-0009": "product/architecture/adr/ADR-0009-ncm-isolated-local-process.md",
    "ADR-0010": "product/architecture/adr/ADR-0010-native-provider-parity-projection.md",
    "ADR-0011": "product/architecture/adr/ADR-0011-patch-footprint-revision-v2.md",
    "ADR-0012": "product/architecture/adr/ADR-0012-configuration-registry-additive-settings.md",
    "ADR-0013": "product/architecture/adr/ADR-0013-daemon-shutdown-touch-point-expansion.md",
    "ADR-0014": "product/architecture/adr/ADR-0014-host-hook-ingest-footprint-revision-v3.md",
}

EXPECTED_STATUSES = {
    "ADR-0001": "accepted",
    "ADR-0002": "accepted",
    "ADR-0003": "accepted",
    "ADR-0004": "accepted_decision_gate",
    "ADR-0005": "accepted",
    "ADR-0006": "accepted",
    "ADR-0007": "accepted",
    "ADR-0008": "accepted",
    "ADR-0009": "accepted_with_production_blockers",
    "ADR-0010": "accepted",
    "ADR-0011": "accepted",
    "ADR-0012": "accepted",
    "ADR-0013": "accepted",
    "ADR-0014": "accepted",
}

REQUIRED_SECTIONS = [
    "Context",
    "Decision",
    "Consequences",
    "Rejected alternatives",
    "Invariants",
    "Verification",
    "Review triggers",
]

REQUIRED_GLOBAL_PHRASES = {
    "ADR-0001": [
        "Direct `ProjectMemoryFactStore` implementation",
        "Provider-name branching",
        "TraceDecay Native remains the only canonical authority",
    ],
    "ADR-0002": [
        "provider recall as canonical truth",
        "implicit promotion",
        "Every durable domain has one named canonical writer",
    ],
    "ADR-0003": [
        "provider-name branching",
        "additive product-owned crates",
        "Concrete adapters do not depend on each other",
    ],
    "ADR-0004": [
        "deliberately **deferred**",
        "`tdmem-0701`",
        "`tdmem-0702`",
        "process existence as readiness",
    ],
    "ADR-0005": [
        "durable, bounded observation journal/outbox",
        "at-least-once",
        "Unbounded in-memory queue",
        "Provider state in Native fact tables",
    ],
    "ADR-0006": [
        "TraceDecay owns one request-scoped context compiler",
        "Provider-built final context",
        "Compare raw scores across providers",
        "Current code truth outranks every memory lane",
    ],
    "ADR-0007": [
        "No observer-produced value is reachable",
        "Best-effort isolation by convention",
        "Implicit provider activation",
        "no silent fallback exists",
    ],
    "ADR-0008": [
        "isolated sync train",
        "Unmapped upstream-owned edits",
        "Every current upstream existing-file diff",
        "never force-update",
    ],
    "ADR-0013": [
        "never bundled into the change that exceeds the previous cap.",
        "ADR-0011 invariant 2 is upheld rather than amended",
        "crates/tracedecay-daemon-service/src/invocation/types.rs",
        "## Touch-point cap revision",
        "Previous max changed lines: `287`",
        "Approved max changed lines: `320`",
    ],
    "ADR-0014": [
        "never bundled into the change that exceeds the previous cap",
        "| upstream existing production files | 34 | 37 | 37 | 0% |",
        "| total upstream changed lines | 3300 | 3500 | 3393 | 3.2% |",
        "| exception-zone files without ADR/policy revision | 2 | 4 | 4 | 0% |",
        "3393 total upstream changed lines",
        "`host_hook_ingest` touch point is capped at 3 files and 200 changed",
        "`host_specific_adapters` zone keeps its `forbidden` default",
        "crates/tracedecay-cli/tests/product_memory_provider_*.rs",
    ],
    "ADR-0009": [
        "supervised isolated local process",
        "`NcmCognitiveSurface`",
        "one provider-owned state directory",
        "no TraceDecay database",
        "restart invalidates the readiness result",
        "production admission stays blocked",
        "server-side deadline/cancellation",
        "atomic crash-safe persistence",
    ],
}

REQUIRED_SOURCE_AUTHORITIES = {
    "product/architecture/native-memory-surface-map.json",
    "product/architecture/coding-memory-authority-matrix.json",
    "product/upstream/patch-footprint-policy.json",
    "product/upstream/convergence-map.json",
    "product/baseline/tracedecay-v2-pr707-linux.json",
}

NCM_TOPOLOGY_EVIDENCE = {
    "crates/tracedecay-memory-provider-ncm/audits/tdmem-0701-licensed-surface-audit.md",
    "crates/tracedecay-memory-provider-ncm/audits/tdmem-0701-capability-matrix.json",
    "product/contracts/memory-provider-v1/provider-lifecycle-contract.json",
    "product/contracts/memory-provider-v1/provider-observation-contract.json",
}

NCM_TOPOLOGY_FIELDS = {
    "state",
    "selected_topology",
    "transport",
    "adapter_boundary",
    "instance_cardinality",
    "state_ownership",
    "restart_readiness",
    "restart_effect_evidence",
    "production_admission",
    "denied_authorities",
    "required_blockers",
}

NCM_DENIED_AUTHORITIES = {
    "tracedecay_databases",
    "repository_and_worktree_files",
    "host_credentials",
    "raw_scope_identity",
    "git_and_code_navigation",
    "session_truth",
    "canonical_facts",
    "tools_and_approvals",
    "prompt_and_final_context",
}

NCM_PRODUCTION_BLOCKERS = {
    "exact_scope_isolation",
    "loaded_state_identity",
    "server_cancellation_and_effect_reconciliation",
    "durable_deduplication",
    "atomic_persistence",
    "supervisor_capability",
}

NCM_REQUIRED_REJECTIONS = {
    "in-process Biomem integration",
    "MCP stdio as the provider transport",
}

NCM_REQUIRED_SECTION_PHRASES = {
    "Decision": [
        "It does not admit NCM to production.",
        "Mandatory observation remains blocking",
        "selected HTTP endpoint",
        "Durable operation identities, dedupe records",
        "Every request receives one monotonic remaining budget",
    ],
    "Migration path": [
        "exclusive supervisor migration fence",
        "source and destination provider/build/config/schema/generation/scope identity",
        "item/count conservation",
    ],
    "Consequences": [
        "Process isolation is not a source-protection or IP boundary",
    ],
    "Invariants": [
        "operation/effect evidence needed for dedupe and reconciliation",
    ],
}

BEAD_ID_RE = re.compile(r"^tdmem-[0-9]{4}$")
ADR_ID_RE = re.compile(r"^ADR-[0-9]{4}$")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--repo", type=Path, default=Path("."))
    parser.add_argument(
        "--manifest",
        type=Path,
        default=Path("product/architecture/adr/manifest.json"),
    )
    parser.add_argument(
        "--issues",
        type=Path,
        default=Path(".beads/issues.jsonl"),
    )
    return parser.parse_args()


def resolve(repo: Path, path: Path) -> Path:
    return path if path.is_absolute() else repo / path


def load_object(path: Path, label: str, errors: list[str]) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        errors.append(f"could not load {label}: {exc}")
        return {}
    if not isinstance(value, dict):
        errors.append(f"{label} root must be an object")
        return {}
    return value


def require_list(value: Any, field: str, errors: list[str]) -> list[Any]:
    if not isinstance(value, list):
        errors.append(f"{field} must be an array")
        return []
    return value


def non_empty_string(row: dict[str, Any], field: str, label: str, errors: list[str]) -> str:
    value = row.get(field)
    if not isinstance(value, str) or not value.strip():
        errors.append(f"{label}.{field} must be a non-empty string")
        return ""
    return value.strip()


def load_issue_ids(path: Path, errors: list[str]) -> set[str]:
    ids: set[str] = set()
    try:
        lines = path.read_text(encoding="utf-8").splitlines()
    except OSError as exc:
        errors.append(f"could not load Beads authority: {exc}")
        return ids
    for line_number, line in enumerate(lines, start=1):
        if not line.strip():
            continue
        try:
            issue = json.loads(line)
        except json.JSONDecodeError as exc:
            errors.append(f"invalid Beads JSONL at line {line_number}: {exc}")
            continue
        issue_id = issue.get("id") if isinstance(issue, dict) else None
        if not isinstance(issue_id, str):
            errors.append(f"Beads line {line_number} has no string id")
            continue
        if issue_id in ids:
            errors.append(f"duplicate Beads issue id {issue_id}")
        ids.add(issue_id)
    return ids


def index_decisions(rows: Iterable[Any], errors: list[str]) -> dict[str, dict[str, Any]]:
    indexed: dict[str, dict[str, Any]] = {}
    for offset, raw in enumerate(rows):
        if not isinstance(raw, dict):
            errors.append(f"decisions[{offset}] must be an object")
            continue
        decision_id = raw.get("id")
        if not isinstance(decision_id, str) or not ADR_ID_RE.fullmatch(decision_id):
            errors.append(f"decisions[{offset}].id must match ADR-NNNN")
            continue
        if decision_id in indexed:
            errors.append(f"duplicate ADR id {decision_id}")
            continue
        indexed[decision_id] = raw
    return indexed


def validate_manifest_structure(
    repo: Path,
    manifest: dict[str, Any],
    errors: list[str],
) -> dict[str, dict[str, Any]]:
    if manifest.get("schema_version") != 1:
        errors.append("manifest schema_version must be 1")
    if manifest.get("bead_id") != "tdmem-0106":
        errors.append("manifest bead_id must be tdmem-0106")
    if manifest.get("status") != "accepted":
        errors.append("manifest status must be accepted")
    for field in ("title", "date", "decision_scope"):
        non_empty_string(manifest, field, "manifest", errors)

    sections = require_list(manifest.get("required_sections"), "required_sections", errors)
    if sections != REQUIRED_SECTIONS:
        errors.append(f"required_sections must be exactly {REQUIRED_SECTIONS}")

    authorities = require_list(manifest.get("source_authorities"), "source_authorities", errors)
    authority_set = {value for value in authorities if isinstance(value, str)}
    if authority_set != REQUIRED_SOURCE_AUTHORITIES:
        errors.append(
            "source_authorities must exactly match the M0 evidence authorities"
        )
    for raw in sorted(authority_set):
        path = Path(raw)
        if path.is_absolute() or ".." in path.parts:
            errors.append(f"source authority must be repository-relative: {raw}")
        elif not (repo / path).is_file():
            errors.append(f"source authority is missing: {raw}")

    decisions = index_decisions(
        require_list(manifest.get("decisions"), "decisions", errors), errors
    )
    missing = EXPECTED_DECISIONS.keys() - decisions.keys()
    extra = decisions.keys() - EXPECTED_DECISIONS.keys()
    if missing:
        errors.append(f"foundational ADRs missing: {sorted(missing)}")
    if extra:
        errors.append(f"unexpected foundational ADRs: {sorted(extra)}")
    return decisions


def validate_decision_metadata(
    decisions: dict[str, dict[str, Any]],
    issue_ids: set[str],
    errors: list[str],
) -> None:
    order = list(EXPECTED_DECISIONS)
    positions = {decision_id: index for index, decision_id in enumerate(order)}
    graph: dict[str, set[str]] = {}

    for decision_id, row in decisions.items():
        expected_topic = EXPECTED_DECISIONS.get(decision_id)
        if expected_topic is None:
            errors.append(f"unexpected foundational ADR {decision_id}")
            continue
        if row.get("topic") != expected_topic:
            errors.append(f"{decision_id}.topic must be {expected_topic}")
        if row.get("path") != EXPECTED_PATHS[decision_id]:
            errors.append(f"{decision_id}.path must be {EXPECTED_PATHS[decision_id]}")
        if row.get("status") != EXPECTED_STATUSES[decision_id]:
            errors.append(
                f"{decision_id}.status must be {EXPECTED_STATUSES[decision_id]}"
            )
        for field in ("title", "decision_summary"):
            non_empty_string(row, field, decision_id, errors)

        rejections = require_list(
            row.get("required_rejections"),
            f"{decision_id}.required_rejections",
            errors,
        )
        if len(rejections) < 2 or any(
            not isinstance(value, str) or not value.strip() for value in rejections
        ):
            errors.append(f"{decision_id} must declare at least two required rejections")

        beads = require_list(
            row.get("verification_beads"),
            f"{decision_id}.verification_beads",
            errors,
        )
        if len(beads) < 2:
            errors.append(f"{decision_id} must point to at least two executable beads")
        for bead_id in beads:
            if not isinstance(bead_id, str) or not BEAD_ID_RE.fullmatch(bead_id):
                errors.append(f"{decision_id} has invalid verification bead {bead_id!r}")
            elif bead_id not in issue_ids:
                errors.append(f"{decision_id} references unknown verification bead {bead_id}")

        dependencies = require_list(
            row.get("depends_on_adrs"),
            f"{decision_id}.depends_on_adrs",
            errors,
        )
        graph[decision_id] = set()
        for dependency in dependencies:
            if not isinstance(dependency, str):
                errors.append(
                    f"{decision_id} depends_on_adrs entries must be ADR ids"
                )
                continue
            if dependency not in decisions:
                errors.append(f"{decision_id} depends on unknown ADR {dependency!r}")
                continue
            graph[decision_id].add(dependency)
            if positions.get(dependency, 10_000) >= positions[decision_id]:
                errors.append(
                    f"{decision_id} must depend only on an earlier foundational ADR"
                )

    visiting: set[str] = set()
    visited: set[str] = set()

    def visit(node: str) -> None:
        if node in visited:
            return
        if node in visiting:
            errors.append(f"foundational ADR dependency cycle includes {node}")
            return
        visiting.add(node)
        for dependency in graph.get(node, set()):
            visit(dependency)
        visiting.remove(node)
        visited.add(node)

    for node in graph:
        visit(node)


def section_body(text: str, section: str) -> str:
    marker = f"## {section}"
    start = text.find(marker)
    if start < 0:
        return ""
    body_start = start + len(marker)
    next_heading = text.find("\n## ", body_start)
    if next_heading < 0:
        next_heading = len(text)
    return text[body_start:next_heading].strip()


def validate_adr_files(
    repo: Path,
    decisions: dict[str, dict[str, Any]],
    errors: list[str],
) -> None:
    for decision_id, row in decisions.items():
        raw_path = row.get("path")
        if not isinstance(raw_path, str):
            continue
        path = Path(raw_path)
        if path.is_absolute() or ".." in path.parts:
            errors.append(f"{decision_id} path must be repository-relative")
            continue
        full_path = repo / path
        try:
            text = full_path.read_text(encoding="utf-8")
        except OSError as exc:
            errors.append(f"could not read {decision_id} document: {exc}")
            continue

        expected_title = row.get("title")
        if not text.startswith(f"# {decision_id}: {expected_title}\n"):
            errors.append(f"{decision_id} document title does not match manifest")
        if "Status:" not in text[:500] or "Date:" not in text[:500]:
            errors.append(f"{decision_id} document must declare Status and Date")
        if "TBD" in text or "TODO" in text:
            errors.append(f"{decision_id} contains unresolved TBD/TODO text")

        for section in REQUIRED_SECTIONS:
            body = section_body(text, section)
            if not body:
                errors.append(f"{decision_id} is missing non-empty section {section!r}")

        rejected = section_body(text, "Rejected alternatives")
        if rejected.count("**") < 4:
            errors.append(f"{decision_id} must explain at least two rejected alternatives")
        invariants = section_body(text, "Invariants")
        if len(re.findall(r"(?m)^\d+\. ", invariants)) < 4:
            errors.append(f"{decision_id} must state at least four numbered invariants")
        verification = section_body(text, "Verification")
        verification_beads = row.get("verification_beads")
        if not isinstance(verification_beads, list):
            verification_beads = []
        for bead_id in verification_beads:
            if isinstance(bead_id, str) and f"`{bead_id}`" not in verification:
                errors.append(
                    f"{decision_id} verification section does not cite {bead_id}"
                )

        for phrase in REQUIRED_GLOBAL_PHRASES.get(decision_id, []):
            if phrase.casefold() not in text.casefold():
                errors.append(f"{decision_id} is missing required phrase {phrase!r}")
        required_rejections = row.get("required_rejections")
        if not isinstance(required_rejections, list):
            required_rejections = []
        for rejection in required_rejections:
            if isinstance(rejection, str) and rejection.casefold() not in rejected.casefold():
                errors.append(
                    f"{decision_id} rejected-alternatives section is missing manifest rejection {rejection!r}"
                )

        if decision_id == "ADR-0009":
            for section, phrases in NCM_REQUIRED_SECTION_PHRASES.items():
                body = section_body(text, section)
                if not body:
                    errors.append(
                        f"ADR-0009 is missing non-empty topology section {section!r}"
                    )
                    continue
                for phrase in phrases:
                    if phrase.casefold() not in body.casefold():
                        errors.append(
                            f"ADR-0009 {section!r} section is missing semantic requirement {phrase!r}"
                        )


def validate_topology_gate(
    decisions: dict[str, dict[str, Any]], errors: list[str]
) -> None:
    row = decisions.get("ADR-0004", {})
    topology = row.get("ncm_topology")
    if not isinstance(topology, dict):
        errors.append("ADR-0004 must define ncm_topology decision gate")
        return
    if topology.get("state") != "deferred":
        errors.append("NCM execution topology must remain deferred until tdmem-0701/0702")
    gate_beads = topology.get("decision_gate_beads")
    if gate_beads != ["tdmem-0701", "tdmem-0702"]:
        errors.append("NCM topology decision gate must be tdmem-0701 then tdmem-0702")
    candidates = require_list(
        topology.get("allowed_candidates"),
        "ADR-0004.ncm_topology.allowed_candidates",
        errors,
    )
    if "in_process_crate" not in candidates or "isolated_local_process" not in candidates:
        errors.append("NCM topology gate must compare in-process and isolated-process candidates")
    for forbidden_field in (
        "selected",
        "selected_topology",
        "transport",
        "process_model",
        "decision",
    ):
        if forbidden_field in topology:
            errors.append(
                f"NCM topology gate must not preselect {forbidden_field!r}"
            )

    selection_row = decisions.get("ADR-0009", {})
    selection = selection_row.get("ncm_topology")
    if not isinstance(selection, dict):
        errors.append("ADR-0009 must define the selected NCM topology")
        return

    if set(selection) != NCM_TOPOLOGY_FIELDS:
        errors.append(
            "ADR-0009.ncm_topology fields must exactly encode the selected boundary"
        )

    exact_values = {
        "state": "selected",
        "selected_topology": "isolated_local_process",
        "transport": "private_loopback_http",
        "adapter_boundary": "NcmCognitiveSurface",
        "instance_cardinality": "one_instance_per_exact_scope",
        "state_ownership": "one_provider_owned_state_directory_per_exact_scope",
        "restart_readiness": "invalidated",
        "restart_effect_evidence": "durable_and_queryable",
        "production_admission": "blocked",
    }
    for field, expected in exact_values.items():
        if selection.get(field) != expected:
            errors.append(f"ADR-0009.ncm_topology.{field} must be {expected}")

    required_rejections = selection_row.get("required_rejections")
    rejection_strings = (
        {value for value in required_rejections if isinstance(value, str)}
        if isinstance(required_rejections, list)
        else set()
    )
    if (
        not isinstance(required_rejections, list)
        or len(rejection_strings) != len(required_rejections)
        or rejection_strings != NCM_REQUIRED_REJECTIONS
    ):
        errors.append(
            "ADR-0009 required_rejections must exactly reject in-process Biomem and MCP stdio"
        )

    denied = require_list(
        selection.get("denied_authorities"),
        "ADR-0009.ncm_topology.denied_authorities",
        errors,
    )
    denied_strings = {value for value in denied if isinstance(value, str)}
    if (
        len(denied_strings) != len(denied)
        or len(denied_strings) != len(NCM_DENIED_AUTHORITIES)
        or denied_strings != NCM_DENIED_AUTHORITIES
    ):
        errors.append(
            "ADR-0009 denied authorities must exactly forbid host and TraceDecay authority"
        )

    blockers = require_list(
        selection.get("required_blockers"),
        "ADR-0009.ncm_topology.required_blockers",
        errors,
    )
    blocker_strings = {value for value in blockers if isinstance(value, str)}
    if (
        len(blocker_strings) != len(blockers)
        or len(blocker_strings) != len(NCM_PRODUCTION_BLOCKERS)
        or blocker_strings != NCM_PRODUCTION_BLOCKERS
    ):
        errors.append(
            "ADR-0009 production blockers must preserve exact-scope, identity, cancellation/effect, dedupe, persistence, and supervisor gaps"
        )

    evidence = require_list(
        selection_row.get("evidence_sources"),
        "ADR-0009.evidence_sources",
        errors,
    )
    evidence_strings = {value for value in evidence if isinstance(value, str)}
    if (
        len(evidence_strings) != len(evidence)
        or len(evidence_strings) != len(NCM_TOPOLOGY_EVIDENCE)
        or evidence_strings != NCM_TOPOLOGY_EVIDENCE
    ):
        errors.append("ADR-0009 evidence_sources must exactly bind the audit and contracts")


def validate_document(
    repo: Path,
    manifest: dict[str, Any],
    issue_ids: set[str],
) -> list[str]:
    errors: list[str] = []
    decisions = validate_manifest_structure(repo, manifest, errors)
    validate_decision_metadata(decisions, issue_ids, errors)
    validate_adr_files(repo, decisions, errors)
    validate_topology_gate(decisions, errors)
    topology_evidence = decisions.get("ADR-0009", {}).get("evidence_sources", [])
    if isinstance(topology_evidence, list):
        for raw in topology_evidence:
            if not isinstance(raw, str):
                continue
            path = Path(raw)
            if path.is_absolute() or ".." in path.parts:
                errors.append(f"ADR-0009 evidence source must be repository-relative: {raw}")
            elif not (repo / path).is_file():
                errors.append(f"ADR-0009 evidence source is missing: {raw}")
    return errors


def relative(path: Path, repo: Path) -> str:
    try:
        return str(path.relative_to(repo))
    except ValueError:
        return str(path)


def main() -> int:
    args = parse_args()
    repo = args.repo.resolve()
    manifest_path = resolve(repo, args.manifest)
    issues_path = resolve(repo, args.issues)
    bootstrap_errors: list[str] = []
    manifest = load_object(manifest_path, "ADR manifest", bootstrap_errors)
    issue_ids = load_issue_ids(issues_path, bootstrap_errors)
    if bootstrap_errors:
        print(json.dumps({"ok": False, "errors": bootstrap_errors}, indent=2, sort_keys=True))
        return 1

    errors = validate_document(repo, manifest, issue_ids)
    if errors:
        print(json.dumps({"ok": False, "errors": errors}, indent=2, sort_keys=True))
        return 1

    topology_selection = next(
        row for row in manifest["decisions"] if row["id"] == "ADR-0009"
    )["ncm_topology"]
    receipt = {
        "ok": True,
        "schema_version": manifest["schema_version"],
        "bead_id": manifest["bead_id"],
        "status": manifest["status"],
        "decision_count": len(manifest["decisions"]),
        "verification_bead_count": len(
            {
                bead
                for decision in manifest["decisions"]
                for bead in decision["verification_beads"]
            }
        ),
        "ncm_topology_state": topology_selection["state"],
        "ncm_production_admission": topology_selection["production_admission"],
        "manifest": relative(manifest_path, repo),
    }
    print(json.dumps(receipt, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    sys.exit(main())
