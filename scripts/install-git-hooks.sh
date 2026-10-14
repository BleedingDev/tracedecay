#!/usr/bin/env bash
set -euo pipefail

repo_root="$(git rev-parse --show-toplevel)"
cd "$repo_root"

pnpm install
# Written to the shared repository config, so every linked worktree inherits
# it; the relative path resolves against each worktree's own root.
git config core.hooksPath .githooks
chmod +x .githooks/commit-msg .githooks/pre-commit .githooks/pre-push scripts/local-gates.sh

echo "Installed repository Git hooks via core.hooksPath=.githooks"
echo "Commit messages will be checked with pnpm run lint:commit"
echo "Staged Rust is rustfmt-formatted on commit; scripts/local-gates.sh runs on push"
