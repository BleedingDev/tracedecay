#!/usr/bin/env python3
"""Run the installed plan-graph CLI with this plan's exact saved selection."""
import argparse
import json
import fnmatch
import hashlib
from pathlib import Path
import subprocess
import sys

parser = argparse.ArgumentParser()
parser.add_argument("command", choices=["validate", "summary", "dag", "frontier"])
parser.add_argument("--format", choices=["text", "json", "mermaid"], default="text")
parser.add_argument(
    "--state-dir",
    type=Path,
    help="Override the graph state root; useful for read-only temporary inspection.",
)
parser.add_argument(
    "--no-write-state",
    action="store_true",
    help="Do not refresh the managed snapshot while inspecting the graph.",
)
args = parser.parse_args()
selection = json.loads(Path(__file__).with_name("execution-selection.json").read_text())
cli = Path("/Users/satan/.codex/skills/plan-graph/scripts/plan_graph.py")
plans_root = Path(selection["plans_root"]).expanduser()
excluded = {
    entry
    for entry in selection.get("exclude", [])
    if isinstance(entry, str)
}
patterns = selection.get("glob", "*.plan.md")
if isinstance(patterns, str):
    patterns = [patterns]
selected_plans = [
    path.resolve()
    for path in sorted(plans_root.glob("*.plan.md"))
    if any(fnmatch.fnmatch(path.name, pattern) for pattern in patterns)
    and path.name not in excluded
    and path.name.removesuffix(".plan.md") not in excluded
]

selected_slugs = {path.name.removesuffix(".plan.md") for path in selected_plans}
active_edges = set(selection.get("depends", []))
replacement_artifacts = selection.get("replacement_artifacts", [])
selection_errors = []
seen_replacements = set()
if not isinstance(replacement_artifacts, list):
    selection_errors.append("replacement_artifacts must be a list")
    replacement_artifacts = []
for index, replacement in enumerate(replacement_artifacts):
    if not isinstance(replacement, dict):
        selection_errors.append(f"replacement_artifacts[{index}] must be an object")
        continue
    source = str(replacement.get("source_plan", "")).strip()
    todo = str(replacement.get("source_todo", "")).strip()
    target = str(replacement.get("replacement_plan", "")).strip()
    key = (source, todo, target)
    if key in seen_replacements:
        selection_errors.append(
            f"replacement_artifacts[{index}] duplicates {source}:{todo}->{target}"
        )
    seen_replacements.add(key)
    if source not in selected_slugs:
        selection_errors.append(
            f"replacement_artifacts[{index}] source plan '{source}' is not selected"
        )
    if target not in selected_slugs:
        selection_errors.append(
            f"replacement_artifacts[{index}] replacement plan '{target}' is not selected"
        )
    if not todo:
        selection_errors.append(f"replacement_artifacts[{index}] has an empty source_todo")
    if not isinstance(replacement.get("blocking"), bool):
        selection_errors.append(
            f"replacement_artifacts[{index}] blocking must be boolean"
        )
    if f"{source}:{target}" in active_edges and replacement.get("blocking") is False:
        selection_errors.append(
            f"replacement_artifacts[{index}] nonblocking replacement is also an active dependency"
        )
    superseded_todos = selection.get("superseded_todos", {}).get(source, [])
    if todo not in superseded_todos:
        selection_errors.append(
            f"replacement_artifacts[{index}] source todo '{source}:{todo}' is not listed in superseded_todos"
        )
for superseded_source, targets in selection.get("superseded", {}).items():
    source_file = f"{superseded_source}.plan.md"
    if source_file not in excluded and superseded_source not in excluded:
        selection_errors.append(
            f"superseded plan '{superseded_source}' must be excluded from the active selection"
        )
    if not isinstance(targets, list) or not targets:
        selection_errors.append(f"superseded plan '{superseded_source}' has no replacement plans")
    else:
        for target in targets:
            if target not in selected_slugs:
                selection_errors.append(
                    f"replacement plan '{target}' for '{superseded_source}' is not selected"
                )

selection_metadata = {
    "exclude": selection.get("exclude", []),
    "superseded": selection.get("superseded", {}),
    "superseded_todos": selection.get("superseded_todos", {}),
    "replacement_artifacts": replacement_artifacts,
}

selected_plan_paths = sorted(str(path) for path in selected_plans)
selection_edges = sorted(
    tuple(edge.split(":", 1))
    for edge in selection.get("depends", [])
    if isinstance(edge, str) and ":" in edge
)
plan_set_hash = hashlib.sha1("\n".join(selected_plan_paths).encode("utf-8")).hexdigest()[:10]
selection_hash = hashlib.sha1(
    json.dumps(
        {"selected_plan_paths": selected_plan_paths, "edges": selection_edges},
        sort_keys=True,
    ).encode("utf-8")
).hexdigest()[:10]

command = [
    sys.executable,
    "-S",
    str(cli),
    args.command,
    "--plans-root",
    str(plans_root),
    "--graph-id",
    selection["graph_id"],
    "--state-dir",
    str(args.state_dir.expanduser() if args.state_dir else selection["state_root"]),
    "--strict",
    "--format",
    args.format,
    "--lanes",
    "49",
    "--max-depth",
    "3",
]
for plan in selected_plans:
    command += ["--plan", str(plan)]
if not args.no_write_state:
    command.append("--write-state")
for edge in selection["depends"]:
    command += ["--depends", edge]

if selection_errors:
    if args.format == "json":
        print(json.dumps({"errors": selection_errors, "warnings": []}, indent=2))
    else:
        print("\n".join(selection_errors), file=sys.stderr)
    raise SystemExit(1)

result = subprocess.run(
    command,
    cwd=plans_root.parents[3],
    capture_output=True,
    text=True,
)

if result.stdout:
    if args.format == "json":
        try:
            output = json.loads(result.stdout)
        except json.JSONDecodeError:
            output = None
        if isinstance(output, dict):
            if args.command == "validate":
                output.update(
                    {
                        "graph_id": selection["graph_id"],
                        "plan_set_hash": plan_set_hash,
                        "selection_hash": selection_hash,
                        "plan_count": len(selected_plans),
                        "edge_count": len(selection_edges),
                    }
                )
            output["selection_metadata"] = selection_metadata
            print(json.dumps(output, indent=2))
        else:
            print(result.stdout, end="")
    else:
        print(result.stdout, end="")
if result.stderr:
    print(result.stderr, end="", file=sys.stderr)

if result.returncode == 0 and not args.no_write_state:
    state_root = (args.state_dir.expanduser() if args.state_dir else Path(selection["state_root"]).expanduser())
    snapshot_path = state_root / selection["graph_id"] / "snapshot.json"
    if snapshot_path.exists():
        try:
            snapshot = json.loads(snapshot_path.read_text())
        except json.JSONDecodeError:
            snapshot = None
        if isinstance(snapshot, dict):
            snapshot["selection_metadata"] = selection_metadata
            snapshot_path.write_text(json.dumps(snapshot, indent=2) + "\n")

raise SystemExit(result.returncode)
