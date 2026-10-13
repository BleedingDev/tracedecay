#!/usr/bin/env python3
"""Validate the M0 pluggable-memory GO/NO-GO decision and implementation train."""

from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path
from typing import Any, Iterable

EXPECTED_EVIDENCE = {
    "upstream_floor": "product/upstream/pr707-floor.json",
    "clean_baseline": "product/baseline/tracedecay-v2-pr707-linux.json",
    "native_memory_surface": "product/architecture/native-memory-surface-map.json",
    "authority_matrix": "product/architecture/coding-memory-authority-matrix.json",
    "patch_footprint": "product/upstream/patch-footprint-policy.json",
    "convergence_map": "product/upstream/convergence-map.json",
    "foundational_adrs": "product/architecture/adr/manifest.json",
}

REQUIRED_CONDITIONS = {
    "provider_boundary_viable_without_authority_replacement",
    "native_explicit_fact_authority_preserved",
    "current_code_truth_remains_tracedecay_owned",
    "repository_worktree_branch_session_scope_remains_tracedecay_owned",
    "provider_recall_is_advisory",
    "final_context_compilation_remains_tracedecay_owned",
    "observer_mode_is_mechanically_non_influential",
    "provider_writes_are_idempotent_and_crash_recoverable",
    "upstream_patch_budget_is_locked",
    "ncm_execution_topology_is_deferred",
    "ocean_implementation_is_deferred_until_a_versioned_specification",
}

REQUIRED_RISKS = {
    "contract_semantic_leakage",
    "native_parity_drift",
    "partial_provider_effects",
    "unsafe_recall_admission",
    "observer_influence",
    "ncm_surface_and_topology_unknown",
    "upstream_convergence_cost",
}

REQUIRED_DEFERRED = {
    "ncm_execution_topology": "deferred",
    "ocean_implementation": "deferred",
    "active_multi_provider_blending": "not_authorized",
}

# The nine decisions the M0 go/no-go was originally signed off against. Later
# ADRs may be added freely; none of these may disappear.
REQUIRED_FOUNDATIONAL_ADRS = {
    "ADR-0001",
    "ADR-0002",
    "ADR-0003",
    "ADR-0004",
    "ADR-0005",
    "ADR-0006",
    "ADR-0007",
    "ADR-0008",
    "ADR-0009",
}

REQUIRED_HARD_GATES = {
    "no_contract_bypass",
    "no_native_cutover_without_parity",
    "no_ncm_transport_before_audit",
    "no_observer_influence",
    "no_active_mode_before_safety_gates",
    "no_unmapped_upstream_edits",
}

REQUIRED_MARKDOWN_PHRASES = [
    "**Decision:** GO",
    "`tdmem-0201`",
    "`ProjectMemoryFactStore`",
    "Provider recall remains labelled advisory evidence",
    "No production NCM transport before `tdmem-0701` and `tdmem-0702`",
    "No observer-produced value reachable",
    "No existing upstream-owned file edit without a current convergence-map entry",
    "NO-GO triggers",
]

BEAD_RE = re.compile(r"^tdmem-[0-9]{4}$")
HEX40_RE = re.compile(r"^[0-9a-f]{40}$")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--repo", type=Path, default=Path("."))
    parser.add_argument(
        "--decision",
        type=Path,
        default=Path("product/architecture/m0-go-no-go.json"),
    )
    parser.add_argument(
        "--report",
        type=Path,
        default=Path("product/architecture/m0-go-no-go.md"),
    )
    parser.add_argument(
        "--issues", type=Path, default=Path(".beads/issues.jsonl")
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


def load_issue_ids(path: Path, errors: list[str]) -> set[str]:
    issue_ids: set[str] = set()
    try:
        lines = path.read_text(encoding="utf-8").splitlines()
    except OSError as exc:
        errors.append(f"could not load Beads authority: {exc}")
        return issue_ids
    for line_number, line in enumerate(lines, start=1):
        if not line.strip():
            continue
        try:
            row = json.loads(line)
        except json.JSONDecodeError as exc:
            errors.append(f"invalid Beads JSONL at line {line_number}: {exc}")
            continue
        issue_id = row.get("id") if isinstance(row, dict) else None
        if not isinstance(issue_id, str):
            errors.append(f"Beads line {line_number} has no string id")
            continue
        if issue_id in issue_ids:
            errors.append(f"duplicate Beads issue id {issue_id}")
        issue_ids.add(issue_id)
    return issue_ids


def require_list(value: Any, field: str, errors: list[str]) -> list[Any]:
    if not isinstance(value, list):
        errors.append(f"{field} must be an array")
        return []
    return value


def require_object(value: Any, field: str, errors: list[str]) -> dict[str, Any]:
    if not isinstance(value, dict):
        errors.append(f"{field} must be an object")
        return {}
    return value


def non_empty_string(
    row: dict[str, Any], field: str, label: str, errors: list[str]
) -> str:
    value = row.get(field)
    if not isinstance(value, str) or not value.strip():
        errors.append(f"{label}.{field} must be a non-empty string")
        return ""
    return value.strip()


def index_by_id(
    rows: Iterable[Any], field: str, errors: list[str]
) -> dict[str, dict[str, Any]]:
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
            errors.append(f"duplicate {field} id {row_id}")
            continue
        indexed[row_id] = raw
    return indexed


def validate_bead_id(
    value: Any, label: str, issue_ids: set[str], errors: list[str]
) -> None:
    if not isinstance(value, str) or not BEAD_RE.fullmatch(value):
        errors.append(f"{label} must match tdmem-NNNN")
    elif value not in issue_ids:
        errors.append(f"{label} references unknown Beads issue {value}")


def validate_header(document: dict[str, Any], errors: list[str]) -> None:
    if document.get("schema_version") != 1:
        errors.append("schema_version must be 1")
    if document.get("bead_id") != "tdmem-0107":
        errors.append("bead_id must be tdmem-0107")
    if document.get("verdict") != "go":
        errors.append("M0 verdict must be go")
    if document.get("next_executable_bead") != "tdmem-0201":
        errors.append("next_executable_bead must be tdmem-0201")
    for field in ("title", "date", "decision"):
        non_empty_string(document, field, "decision", errors)
    decision = str(document.get("decision", "")).casefold()
    for phrase in (
        "capability-based provider boundary",
        "do not replace tracedecay authorities",
        "final context ownership",
    ):
        if phrase not in decision:
            errors.append(f"decision text must state {phrase!r}")


def validate_conditions(document: dict[str, Any], errors: list[str]) -> None:
    conditions = require_object(document.get("conditions"), "conditions", errors)
    keys = set(conditions)
    if keys != REQUIRED_CONDITIONS:
        errors.append(
            "conditions must exactly cover authority, advisory, isolation, recovery, patch-budget, NCM, and OCEAN gates"
        )
    for key in sorted(REQUIRED_CONDITIONS):
        if conditions.get(key) is not True:
            errors.append(f"condition {key} must be true for a GO verdict")


def validate_evidence(repo: Path, document: dict[str, Any], errors: list[str]) -> None:
    evidence = index_by_id(
        require_list(document.get("evidence"), "evidence", errors),
        "evidence",
        errors,
    )
    if set(evidence) != set(EXPECTED_EVIDENCE):
        errors.append("evidence must exactly include the seven accepted M0 authorities")
    for evidence_id, expected_path in EXPECTED_EVIDENCE.items():
        row = evidence.get(evidence_id, {})
        if row.get("path") != expected_path:
            errors.append(f"evidence {evidence_id}.path must be {expected_path}")
        for field in ("status", "finding"):
            non_empty_string(row, field, f"evidence[{evidence_id}]", errors)
        path = Path(expected_path)
        if path.is_absolute() or ".." in path.parts:
            errors.append(f"evidence path must be repository-relative: {expected_path}")
        elif not (repo / path).is_file():
            errors.append(f"evidence file is missing: {expected_path}")

    floor = load_object(
        repo / "product/upstream/pr707-floor.json", "upstream floor", errors
    )
    pinned_sha = floor.get("pinned_floor_sha") or floor.get("floor_sha")
    if not isinstance(pinned_sha, str) or not HEX40_RE.fullmatch(pinned_sha):
        errors.append("upstream floor evidence must expose a 40-character pinned SHA")

    baseline = load_object(
        repo / "product/baseline/tracedecay-v2-pr707-linux.json",
        "baseline receipt",
        errors,
    )
    serialized_baseline = json.dumps(baseline, sort_keys=True).casefold()
    if "pass" not in serialized_baseline and "success" not in serialized_baseline:
        errors.append("baseline receipt must record a passed/successful result")

    authority = load_object(
        repo / "product/architecture/coding-memory-authority-matrix.json",
        "authority matrix",
        errors,
    )
    if authority.get("decision") not in (None, "accepted") and authority.get("status") not in (
        None,
        "accepted",
    ):
        errors.append("authority matrix must remain accepted")

    adrs = load_object(
        repo / "product/architecture/adr/manifest.json",
        "foundational ADR manifest",
        errors,
    )
    # The M0 gate cares that the foundational decisions are all accepted and
    # that none of the originally required ones has been dropped — not that the
    # count is frozen. The set grows as the program takes new decisions:
    # ADR-0009 selected the isolated NCM topology, ADR-0010 fixed the Native
    # parity projection, ADR-0011 revised the patch-footprint budget, and
    # ADR-0012 authorized the additive configuration-registry exception.
    # Pinning an exact count only forced an edit to this gate each time.
    decisions = adrs.get("decisions", [])
    if adrs.get("status") != "accepted":
        errors.append("foundational ADR manifest must be accepted")
    if not isinstance(decisions, list) or len(decisions) < 9:
        errors.append(
            "foundational ADR manifest must contain at least nine accepted decisions"
        )
    else:
        declared = {row.get("id") for row in decisions if isinstance(row, dict)}
        missing = sorted(REQUIRED_FOUNDATIONAL_ADRS - declared)
        if missing:
            errors.append(
                f"foundational ADR manifest is missing required decisions: {missing}"
            )
    try:
        topology = next(
            row for row in adrs["decisions"] if row.get("id") == "ADR-0004"
        )["ncm_topology"]
    except (KeyError, StopIteration, TypeError):
        errors.append("ADR-0004 NCM topology decision gate is missing")
    else:
        if topology.get("state") != "deferred":
            errors.append("M0 GO requires NCM topology to remain deferred")


def validate_reasons(document: dict[str, Any], errors: list[str]) -> None:
    reasons = require_list(document.get("go_reasons"), "go_reasons", errors)
    if len(reasons) < 5:
        errors.append("GO decision must provide at least five evidence-backed reasons")
    for offset, reason in enumerate(reasons):
        if not isinstance(reason, str) or len(reason.strip()) < 40:
            errors.append(f"go_reasons[{offset}] must be a substantive string")


def validate_risks(
    document: dict[str, Any], issue_ids: set[str], errors: list[str]
) -> None:
    risks = index_by_id(
        require_list(document.get("residual_risks"), "residual_risks", errors),
        "residual_risks",
        errors,
    )
    if set(risks) != REQUIRED_RISKS:
        errors.append("residual_risks must exactly cover the seven M0 risk classes")
    for risk_id, row in risks.items():
        if row.get("severity") not in {"high", "critical"}:
            errors.append(f"risk {risk_id} severity must be high or critical")
        for field in ("risk", "mitigation"):
            non_empty_string(row, field, f"risk[{risk_id}]", errors)
        beads = require_list(
            row.get("blocking_beads"), f"risk[{risk_id}].blocking_beads", errors
        )
        if not beads:
            errors.append(f"risk {risk_id} must name at least one blocking bead")
        for bead in beads:
            validate_bead_id(bead, f"risk[{risk_id}].blocking_beads", issue_ids, errors)


def validate_deferred(
    document: dict[str, Any], issue_ids: set[str], errors: list[str]
) -> None:
    deferred = index_by_id(
        require_list(document.get("deferred_decisions"), "deferred_decisions", errors),
        "deferred_decisions",
        errors,
    )
    if set(deferred) != set(REQUIRED_DEFERRED):
        errors.append("deferred_decisions must exactly cover NCM topology, OCEAN, and active blending")
    for decision_id, expected_state in REQUIRED_DEFERRED.items():
        row = deferred.get(decision_id, {})
        if row.get("state") != expected_state:
            errors.append(f"deferred decision {decision_id}.state must be {expected_state}")
        non_empty_string(row, "reason", f"deferred[{decision_id}]", errors)
        gate = require_list(
            row.get("decision_gate"), f"deferred[{decision_id}].decision_gate", errors
        )
        for bead in gate:
            validate_bead_id(bead, f"deferred[{decision_id}].decision_gate", issue_ids, errors)
    if deferred.get("ncm_execution_topology", {}).get("decision_gate") != [
        "tdmem-0701",
        "tdmem-0702",
    ]:
        errors.append("NCM topology decision gate must be tdmem-0701 then tdmem-0702")
    if deferred.get("ocean_implementation", {}).get("decision_gate") != []:
        errors.append("OCEAN must have no speculative implementation decision gate")


def validate_implementation_order(
    document: dict[str, Any], issue_ids: set[str], errors: list[str]
) -> None:
    rows = require_list(
        document.get("implementation_order"), "implementation_order", errors
    )
    if len(rows) < 6:
        errors.append("implementation_order must lock at least M1 through M6")
        return
    expected_orders = list(range(1, len(rows) + 1))
    actual_orders = [row.get("order") if isinstance(row, dict) else None for row in rows]
    if actual_orders != expected_orders:
        errors.append("implementation_order order fields must be contiguous and sorted")

    milestones: set[str] = set()
    previous_entry_number = -1
    for offset, raw in enumerate(rows):
        if not isinstance(raw, dict):
            continue
        label = f"implementation_order[{offset}]"
        milestone = non_empty_string(raw, "milestone", label, errors)
        if milestone in milestones:
            errors.append(f"duplicate implementation milestone {milestone}")
        milestones.add(milestone)
        non_empty_string(raw, "name", label, errors)
        entry = raw.get("entry_bead")
        exit_bead = raw.get("exit_bead")
        validate_bead_id(entry, f"{label}.entry_bead", issue_ids, errors)
        validate_bead_id(exit_bead, f"{label}.exit_bead", issue_ids, errors)
        required = require_list(
            raw.get("required_before_next"), f"{label}.required_before_next", errors
        )
        if not required:
            errors.append(f"{label} must name required_before_next beads")
        for bead in required:
            validate_bead_id(bead, f"{label}.required_before_next", issue_ids, errors)
        if entry not in required or exit_bead not in required:
            errors.append(f"{label} required_before_next must include entry and exit beads")
        if isinstance(entry, str) and BEAD_RE.fullmatch(entry):
            number = int(entry.split("-")[1])
            if number <= previous_entry_number:
                errors.append("implementation_order entry beads must advance monotonically")
            previous_entry_number = number

    first = rows[0] if isinstance(rows[0], dict) else {}
    if first.get("milestone") != "M1" or first.get("entry_bead") != "tdmem-0201":
        errors.append("implementation order must begin with M1 / tdmem-0201")
    first_required = set(first.get("required_before_next", []))
    for bead in ("tdmem-0201", "tdmem-0202", "tdmem-0203", "tdmem-0204", "tdmem-0205", "tdmem-0206", "tdmem-0209"):
        if bead not in first_required:
            errors.append(f"M1 gate is missing required contract bead {bead}")

    m6 = next(
        (row for row in rows if isinstance(row, dict) and row.get("milestone") == "M6"),
        None,
    )
    if not isinstance(m6, dict):
        errors.append("implementation_order must include M6 NCM decision gate")
    elif m6.get("required_before_next", [])[:2] != ["tdmem-0701", "tdmem-0702"]:
        errors.append("M6 must audit NCM before selecting its topology")


def validate_hard_gates(document: dict[str, Any], errors: list[str]) -> None:
    gates = index_by_id(
        require_list(document.get("hard_gates"), "hard_gates", errors),
        "hard_gates",
        errors,
    )
    if set(gates) != REQUIRED_HARD_GATES:
        errors.append("hard_gates must exactly include the six M0 implementation gates")
    for gate_id, row in gates.items():
        rule = non_empty_string(row, "rule", f"hard_gate[{gate_id}]", errors)
        if len(rule) < 40:
            errors.append(f"hard gate {gate_id} must be substantive")

    serialized = " ".join(str(row.get("rule", "")) for row in gates.values()).casefold()
    for phrase in (
        "no concrete provider implementation",
        "native remains on the direct path",
        "no production ncm transport",
        "observer mode cannot affect",
        "active ncm mode",
        "existing upstream-owned file edit",
    ):
        if phrase not in serialized:
            errors.append(f"hard gates must state {phrase!r}")


def validate_no_go_and_signoff(
    document: dict[str, Any], issue_ids: set[str], errors: list[str]
) -> None:
    triggers = require_list(document.get("no_go_triggers"), "no_go_triggers", errors)
    if len(triggers) < 7:
        errors.append("no_go_triggers must name at least seven stop conditions")
    serialized = " ".join(str(value) for value in triggers).casefold()
    for phrase in (
        "canonical ownership",
        "projectmemoryfactstore",
        "observer execution",
        "native parity",
        "ncm licensing",
        "patch budget",
    ):
        if phrase not in serialized:
            errors.append(f"NO-GO triggers must cover {phrase!r}")

    signoff = require_object(document.get("sign_off"), "sign_off", errors)
    for field in ("decision_owner", "evidence_owner", "state", "first_action"):
        non_empty_string(signoff, field, "sign_off", errors)
    if "tdmem-0201" not in str(signoff.get("first_action", "")):
        errors.append("sign_off.first_action must start with tdmem-0201")
    validate_bead_id(document.get("next_executable_bead"), "next_executable_bead", issue_ids, errors)


def validate_report(path: Path, errors: list[str]) -> None:
    try:
        text = path.read_text(encoding="utf-8")
    except OSError as exc:
        errors.append(f"could not load M0 report: {exc}")
        return
    for phrase in REQUIRED_MARKDOWN_PHRASES:
        if phrase.casefold() not in text.casefold():
            errors.append(f"M0 Markdown report is missing required phrase {phrase!r}")
    if text.count("## ") < 10:
        errors.append("M0 Markdown report must contain the complete decision structure")
    if "TBD" in text or "TODO" in text:
        errors.append("M0 Markdown report contains unresolved TBD/TODO text")


def validate_document(
    repo: Path,
    document: dict[str, Any],
    report_path: Path,
    issue_ids: set[str],
) -> list[str]:
    errors: list[str] = []
    validate_header(document, errors)
    validate_conditions(document, errors)
    validate_evidence(repo, document, errors)
    validate_reasons(document, errors)
    validate_risks(document, issue_ids, errors)
    validate_deferred(document, issue_ids, errors)
    validate_implementation_order(document, issue_ids, errors)
    validate_hard_gates(document, errors)
    validate_no_go_and_signoff(document, issue_ids, errors)
    validate_report(report_path, errors)
    return errors


def main() -> int:
    args = parse_args()
    repo = args.repo.resolve()
    decision_path = resolve(repo, args.decision)
    report_path = resolve(repo, args.report)
    issues_path = resolve(repo, args.issues)
    bootstrap_errors: list[str] = []
    document = load_object(decision_path, "M0 decision", bootstrap_errors)
    issue_ids = load_issue_ids(issues_path, bootstrap_errors)
    if bootstrap_errors:
        print(json.dumps({"ok": False, "errors": bootstrap_errors}, indent=2, sort_keys=True))
        return 1

    errors = validate_document(repo, document, report_path, issue_ids)
    if errors:
        print(json.dumps({"ok": False, "errors": errors}, indent=2, sort_keys=True))
        return 1

    receipt = {
        "ok": True,
        "schema_version": document["schema_version"],
        "bead_id": document["bead_id"],
        "verdict": document["verdict"],
        "next_executable_bead": document["next_executable_bead"],
        "evidence_count": len(document["evidence"]),
        "risk_count": len(document["residual_risks"]),
        "implementation_stage_count": len(document["implementation_order"]),
        "hard_gate_count": len(document["hard_gates"]),
        "ncm_topology_state": next(
            row["state"]
            for row in document["deferred_decisions"]
            if row["id"] == "ncm_execution_topology"
        ),
        "ocean_state": next(
            row["state"]
            for row in document["deferred_decisions"]
            if row["id"] == "ocean_implementation"
        ),
    }
    print(json.dumps(receipt, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    sys.exit(main())
