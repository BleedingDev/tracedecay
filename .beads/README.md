# Pluggable Memory Providers — Beads plan

This directory is the versioned planning authority for branch
`feat/pluggable-memory-providers-v2` in `BleedingDev/tracedecay`.

## Fixed program scope

- Base: `ScriptedAlchemy/tracedecay` PR #707, head SHA
  `08fbe33a7c7f403191fd5d6e356c7b6681b96403`.
- Product scope: **coding agents only**.
- Repository shape: **one TraceDecay V2 monorepo**.
- Initial providers: **TraceDecay Native** and **NCM/Biomem**.
- Future provider: **OCEAN**, explicitly deferred until a versioned specification exists.
- Out of scope for this program: games, NPCs, OntOS-specific surfaces, and a standalone
  general-purpose distribution.

The root program bead is `tdmem-0000`. It contains inherited agent governance.
Each task is attached to a milestone epic through a `parent-child` relationship and
carries explicit blockers where order matters.

## Backlog

- 131 total beads
- 15 epics
- 116 executable tasks
- 6 deferred beads

| Epic | Scope | Priority | Status |
|---|---|---:|---|
| `tdmem-0100` | M0 — Pin upstream V2 and prove the integration seams | P0 | open |
| `tdmem-0200` | M1 — Define the provider contract and capability model | P0 | open |
| `tdmem-0300` | M2 — Create isolated monorepo crates and composition mounts | P0 | open |
| `tdmem-0400` | M3 — Adapt TraceDecay Native memory and prove parity | P0 | open |
| `tdmem-0500` | M4 — Build observation dispatch, provider lifecycle, and recovery | P1 | open |
| `tdmem-0600` | M5 — Integrate provider recall into TraceDecay context assembly | P1 | open |
| `tdmem-0700` | M6 — Integrate NCM/Biomem as the first cognitive provider | P1 | open |
| `tdmem-0800` | M7 — Add outcome-grounded learning, curation, and forgetting | P1 | open |
| `tdmem-0900` | M8 — Build differential evaluation and regression gates | P1 | open |
| `tdmem-1000` | M9 — Prove real coding-agent host journeys | P1 | open |
| `tdmem-1100` | M10 — Add inspection, security, deletion, and operational visibility | P2 | open |
| `tdmem-1200` | Continuous — Preserve convergence with Zack and ingest external lessons safely | P0 | open |
| `tdmem-1300` | Deferred — Add OCEAN after its versioned specification stabilizes | P3 | deferred |
| `tdmem-1400` | M11 — Harden and release the first alpha | P2 | open |

## Initialize a local Beads database

`issues.jsonl` is committed. `beads.db` and WAL/lock files are intentionally local.

```bash
# Install beads_rust (`br`) first.
br sync --import-only --rebuild
br doctor
br dep cycles --json
br ready --json
```

After pulling new planning changes:

```bash
br sync --import-only
```

After creating/updating/closing beads:

```bash
br sync --flush-only
git add .beads/
```

Never commit `beads.db`, WAL files, locks, or local history.

## Mandatory agent workflow

```bash
ACTOR="${BR_ACTOR:-assistant}"

br ready --json
br show <id> --json

br update --actor "$ACTOR" <id> \
  --status in_progress \
  --transition-comment "Claiming with plan and affected files"

# Implement and verify.

# Mark every acceptance checkbox complete before closure.
br update --actor "$ACTOR" <id> \
  --acceptance-criteria-file /path/to/completed-checklist.md

br close --actor "$ACTOR" <id> \
  --reason "Completed with commit/test evidence" \
  --transition-comment "Evidence: tests, journeys, receipts, commit"

br dep cycles --json
br sync --flush-only
```

## Governance

Inherited context is enabled. Claiming a child should surface the root program constraints
and the immediate epic's objective. The non-negotiable rules include:

- preserve TraceDecay's existing authorities;
- provider recall is advisory;
- no silent fallback or fake readiness;
- exact project/worktree/session scope;
- typed failures, deadlines, cancellation, and idempotency;
- observer providers cannot influence product output;
- product code stays in isolated crates and narrow composition mounts;
- every Zack-owned file edit is recorded in the upstream convergence map;
- real end-to-end coding-agent journeys are required where specified;
- no task is closed from implementation claims alone.

## Source references

- TraceDecay V2 PR #707: https://github.com/ScriptedAlchemy/tracedecay/pull/707
- Product fork: https://github.com/BleedingDev/tracedecay
- NCM/Biomem: https://github.com/bleedingDev/biomem
- Beads Rust: https://github.com/Dicklesworthstone/beads_rust
