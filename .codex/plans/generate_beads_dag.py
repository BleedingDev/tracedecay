#!/usr/bin/env python3
"""Generate plan-graph inputs from the live Beads Rust work surface."""

from __future__ import annotations

import json
import subprocess
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[2]
PLANS_ROOT = ROOT / ".codex" / "plans"
RUNNABLE_ROOT = PLANS_ROOT / "beads-runnable"
ISOLATED_ROOT = PLANS_ROOT / "beads-isolated"
EPIC_ROOT = PLANS_ROOT / "beads-epics"
DEFERRED_ROOT = PLANS_ROOT / "beads-deferred"
GENERATED_ROOTS = (RUNNABLE_ROOT, ISOLATED_ROOT, EPIC_ROOT, DEFERRED_ROOT)


def br_json(*args: str) -> Any:
    command = [
        "br",
        "--no-auto-import",
        "--no-auto-flush",
        *args,
        "--json",
    ]
    completed = subprocess.run(
        command,
        cwd=ROOT,
        check=True,
        capture_output=True,
        text=True,
    )
    return json.loads(completed.stdout)


def markdown_text(value: str | None, fallback: str) -> str:
    text = (value or "").strip()
    return text if text else fallback


def render_plan(issue: dict[str, Any], dependencies: list[dict[str, Any]]) -> str:
    issue_id = issue["id"]
    title = issue["title"]
    status = "in_progress" if issue["status"] == "in_progress" else "pending"
    description = markdown_text(issue.get("description"), "No additional description is recorded in Beads.")
    design = markdown_text(issue.get("design"), "Follow the Beads acceptance contract and existing repository seams.")
    acceptance = markdown_text(
        issue.get("acceptance_criteria"),
        "- [ ] Deliver the described behavior with focused validation.",
    )
    blocking = sorted(
        dependency["id"]
        for dependency in dependencies
        if dependency.get("dependency_type") == "blocks"
        and dependency.get("status") not in {"closed", "tombstone"}
    )
    parents = sorted(
        dependency["id"]
        for dependency in dependencies
        if dependency.get("dependency_type") == "parent-child"
    )
    todo = (
        f"Deliver Bead {issue_id}: {title}; satisfy every recorded acceptance criterion, "
        "run the smallest behavioral dependency cone, then commit and push the green slice."
    )
    overview = " ".join(description.split())
    dependency_note = ", ".join(blocking) if blocking else "none"
    parent_note = ", ".join(parents) if parents else "none"
    return f"""---
name: {issue_id}
overview: {json.dumps(overview)}
todos:
  - id: {issue_id}-deliver
    content: {json.dumps(todo)}
    status: {status}
isProject: false
---

# {issue_id}: {title}

## Execution Notes

Beads issue: `{issue_id}`. Current Beads status at generation: `{issue['status']}`.

{description}

Design authority:

{design}

Acceptance authority:

{acceptance}

## Constraints

- Beads is the live source of truth. Re-read `br show {issue_id}` and require `br ready` before a new claim.
- Unsatisfied blocking Beads dependencies at generation: {dependency_note}.
- Beads parent/hierarchy references: {parent_note}. Parent readiness is enforced by `br`, not fabricated as a plan dependency.
- Build semantic producers/consumers and focused evidence before bookkeeping. Do not create hash-, receipt-, lock-, or presence-only gates.
- Root alone owns heavy Cargo execution, integration acceptance, commits, pushes, and Beads closure.
- Workers must stay inside their assigned write scope, preserve concurrent edits, and stop rather than widen scope.
- Do not contact upstream maintainers or mutate external work items.

## Operator Guidance

Intersect `plan-graph frontier` with live `br ready`; launch only Beads-ready or already-claimed nodes. Assign one coherent file/module owner, run focused checks, review the exact diff locally, then publish the green slice immediately. Close the Bead only after every acceptance item and real journey required by the issue are evidenced.
"""


def main() -> None:
    issue_envelope = br_json(
        "list",
        "--status",
        "open",
        "--status",
        "in_progress",
        "--status",
        "deferred",
        "--deferred",
        "--format",
        "json",
        "--limit",
        "0",
    )
    issues = {issue["id"]: issue for issue in issue_envelope["issues"]}
    details: dict[str, dict[str, Any]] = {}
    for issue_id in sorted(issues):
        detail = br_json("show", issue_id)
        details[issue_id] = detail[0]

    for directory in GENERATED_ROOTS:
        directory.mkdir(parents=True, exist_ok=True)
        for stale in directory.glob("*.plan.md"):
            stale.unlink()

    active_task_ids: set[str] = set()
    isolated_ids: set[str] = set()
    epic_ids: set[str] = set()
    deferred_ids: set[str] = set()
    for issue_id, issue in sorted(details.items()):
        if issue["status"] == "deferred":
            destination = DEFERRED_ROOT
            deferred_ids.add(issue_id)
        elif issue["issue_type"] == "epic":
            destination = EPIC_ROOT
            epic_ids.add(issue_id)
        elif issue_id == "tdmem-floor-daemon-test-env-kmw":
            destination = ISOLATED_ROOT
            isolated_ids.add(issue_id)
        else:
            destination = RUNNABLE_ROOT
            active_task_ids.add(issue_id)
        plan = render_plan(issue, issue.get("dependencies", []))
        (destination / f"{issue_id}.plan.md").write_text(plan)

    direct_edges: set[tuple[str, str]] = set()
    for issue_id in sorted(active_task_ids):
        for dependency in details[issue_id].get("dependencies", []):
            dependency_id = dependency["id"]
            if (
                dependency.get("dependency_type") == "blocks"
                and dependency_id in active_task_ids
            ):
                direct_edges.add((dependency_id, issue_id))

    def inherited_parent_blockers(issue_id: str, seen: set[str]) -> set[str]:
        if issue_id in seen:
            return set()
        seen = {*seen, issue_id}
        blockers: set[str] = set()
        for dependency in details[issue_id].get("dependencies", []):
            dependency_id = dependency["id"]
            dependency_type = dependency.get("dependency_type")
            if dependency_type == "blocks" and dependency_id in active_task_ids:
                blockers.add(dependency_id)
            elif dependency_type == "parent-child" and dependency_id in details:
                blockers.update(inherited_parent_blockers(dependency_id, seen))
        return blockers

    parent_gate_edges: set[tuple[str, str]] = set()
    for issue_id in sorted(active_task_ids):
        for blocker_id in inherited_parent_blockers(issue_id, set()):
            parent_gate_edges.add((blocker_id, issue_id))

    edges = direct_edges | parent_gate_edges

    degrees = {issue_id: 0 for issue_id in active_task_ids}
    for source, target in edges:
        degrees[source] += 1
        degrees[target] += 1
    orphan_ids = sorted(issue_id for issue_id, degree in degrees.items() if degree == 0)
    if orphan_ids:
        raise SystemExit(
            "runnable task plans without a true blocking edge must use an isolated graph: "
            + ", ".join(orphan_ids)
        )

    edge_lines = [f"{source}:{target}" for source, target in sorted(edges)]
    (RUNNABLE_ROOT / "edges.txt").write_text("\n".join(edge_lines) + "\n")
    manifest = {
        "schema": "tracedecay.beads-plan-selection.v1",
        "source": "br list/show with auto import/flush disabled",
        "runnable_plan_count": len(active_task_ids),
        "runnable_edge_count": len(edges),
        "direct_blocking_edge_count": len(direct_edges),
        "inherited_parent_gate_edge_count": len(parent_gate_edges - direct_edges),
        "isolated_plan_ids": sorted(isolated_ids),
        "excluded_epic_plan_ids": sorted(epic_ids),
        "excluded_deferred_plan_ids": sorted(deferred_ids),
        "claimed_plan_ids": sorted(
            issue_id
            for issue_id, issue in details.items()
            if issue["status"] == "in_progress"
        ),
    }
    (PLANS_ROOT / "beads-selection.json").write_text(
        json.dumps(manifest, indent=2, sort_keys=True) + "\n"
    )
    print(json.dumps(manifest, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
