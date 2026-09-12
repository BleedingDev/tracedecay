#!/usr/bin/env python3
"""Run the installed plan-graph CLI with this plan's exact saved selection."""
import argparse
import json
from pathlib import Path
import subprocess
import sys

parser = argparse.ArgumentParser()
parser.add_argument("command", choices=["validate", "summary", "dag", "frontier"])
parser.add_argument("--format", choices=["text", "json", "mermaid"], default="text")
args = parser.parse_args()
selection = json.loads(Path(__file__).with_name("execution-selection.json").read_text())
cli = Path("/Users/satan/.codex/skills/plan-graph/scripts/plan_graph.py")
command = [sys.executable, "-S", str(cli), args.command,
           "--plans-root", selection["plans_root"], "--glob", selection["glob"],
           "--graph-id", selection["graph_id"], "--state-dir", selection["state_root"],
           "--write-state", "--strict", "--format", args.format,
           "--lanes", "49", "--max-depth", "3"]
for edge in selection["depends"]:
    command += ["--depends", edge]
raise SystemExit(subprocess.run(command, cwd=Path(selection["plans_root"]).parents[3]).returncode)

