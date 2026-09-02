#!/usr/bin/env bash
set -euo pipefail

# shellcheck source=../scripts/lib/gate-test.sh
. "$(dirname "${BASH_SOURCE[0]}")/../scripts/lib/gate-test.sh"

SCRIPT="$GATE_REPO_ROOT/scripts/check-product-upstream-floor.py"
REAL_METADATA="$GATE_REPO_ROOT/product/upstream/tracedecay-v2-pr707.json"
FIXTURE="$GATE_SCRATCH/fixture"
METADATA="$GATE_SCRATCH/provenance.json"

mkdir -p "$FIXTURE"
git -C "$FIXTURE" init -q -b feat/pluggable-memory-providers-v2
git -C "$FIXTURE" config user.name test
git -C "$FIXTURE" config user.email test@example.invalid
printf 'floor\n' >"$FIXTURE/history.txt"
git -C "$FIXTURE" add history.txt
git -C "$FIXTURE" commit -q -m floor
floor_sha=$(git -C "$FIXTURE" rev-parse HEAD)
printf 'head\n' >>"$FIXTURE/history.txt"
git -C "$FIXTURE" commit -qam head

cat >"$METADATA" <<JSON
{
  "schema_version": 1,
  "product": {
    "repository": "BleedingDev/tracedecay",
    "branch": "feat/pluggable-memory-providers-v2"
  },
  "source": {
    "repository": "ScriptedAlchemy/tracedecay",
    "pull_request": 707
  },
  "pinned_floor": {
    "sha": "$floor_sha",
    "must_be_ancestor_of_product_head": true
  },
  "observed_pull_request": {
    "base_sha": "1111111111111111111111111111111111111111",
    "head_sha": "2222222222222222222222222222222222222222"
  },
  "update_procedure": {
    "observed_pull_request": "refresh the dated observation",
    "pinned_floor": "move only through a dedicated convergence change"
  }
}
JSON

gate_run "$SCRIPT" --repo "$FIXTURE" --metadata "$METADATA" --require-product-branch
gate_expect_success "descendant checkout"
gate_output_contains "descendant checkout" "\"pinned_floor_sha\": \"$floor_sha\""
gate_output_contains "descendant checkout" "\"ahead_by\": 1"

git -C "$FIXTURE" switch -q -c scratch
gate_run "$SCRIPT" --repo "$FIXTURE" --metadata "$METADATA" --require-product-branch
gate_expect_status "branch mismatch" 1
gate_output_contains "branch mismatch" "checked-out branch mismatch"

git -C "$FIXTURE" switch -q --orphan unrelated
git -C "$FIXTURE" rm -q -rf --ignore-unmatch .
git -C "$FIXTURE" clean -q -fdx
printf 'unrelated\n' >"$FIXTURE/unrelated.txt"
git -C "$FIXTURE" add unrelated.txt
git -C "$FIXTURE" commit -q -m unrelated
gate_run "$SCRIPT" --repo "$FIXTURE" --metadata "$METADATA"
gate_expect_status "unrelated history" 1
gate_output_contains "unrelated history" "is not an ancestor"

python3 - "$METADATA" <<'PY'
import json
import sys
from pathlib import Path

path = Path(sys.argv[1])
document = json.loads(path.read_text())
document["pinned_floor"]["sha"] = "not-a-sha"
path.write_text(json.dumps(document), encoding="utf-8")
PY
gate_run "$SCRIPT" --repo "$FIXTURE" --metadata "$METADATA"
gate_expect_status "invalid metadata" 1
gate_output_contains "invalid metadata" "must be a lowercase 40-character Git SHA"

gate_run "$SCRIPT" --repo "$GATE_REPO_ROOT" --metadata "$REAL_METADATA"
gate_expect_success "real product checkout"
gate_output_contains "real product checkout" \
  "\"pinned_floor_sha\": \"5749e4fcfe268e17bd19a0e6ef90c646f7b37289\""
