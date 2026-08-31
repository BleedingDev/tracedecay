# Deterministic coding-memory scenarios

`coding-memory-scenarios.v1.json` is the provider-neutral corpus for
`tdmem-0901`. It is a versioned, secret-free fixture: the runner supplies the
provider under test, while the fixture, task, observations, code revisions,
and adjudication remain unchanged for every provider.

The corpus deliberately uses a temporary Git repository, synthetic source
bytes, fixed UTC timestamps, stable operation/observation IDs, and no network,
randomness, credentials, or external processes. A provider may keep its own
state, but its output is advisory. The observer lane is not an input to the
product decision.

## Scenario inventory

| ID | Behavior under test | Safety property |
| --- | --- | --- |
| `stale_project_change` | old cache claim followed by a code revision | current exact-worktree code wins and stale lineage remains visible |
| `failed_approach` | retry change that first fails, then is corrected | failed approach remains negative knowledge |
| `cross_agent_reuse` | one session leaves evidence for a later session | reuse is allowed at matching scope with author attribution |
| `project_worktree_scope` | linked worktree and unrelated project have different facts | no path-only, sibling-worktree, or cross-project leakage |
| `contradiction` | documentation conflicts with current code and a passing test | conflict is surfaced and current authority wins |
| `restart` | provider restarts and receives a replay | committed observation survives and replay is idempotent |
| `cancellation` | three-item write is cancelled after one commit | cancellation is distinct from success and reports its effect boundary |
| `provider_corruption` | state checksum does not match its bytes | corrupt state is rejected without implicit repair or fallback |
| `privacy_deletion` | one scoped note is forgotten and checked after restart | deletion is verified across provider state and snapshots |

## Scenario contract

Every scenario contains the same executable layers:

1. `observations` — settled source events with stable sequence, exact scope,
   revision, digest, and synthetic payload.
2. `code_evidence_revisions` — ordered code/evidence revisions, changed files,
   and pass/fail evidence with digests.
3. `steps` — a deterministic provider-neutral action sequence for a runner.
4. `expected_admissible_behavior` — what may enter final context, what must be
   rejected, and the permitted typed terminal outcomes.
5. `adjudication_rubric` — weighted checks whose weights sum to one; all
   safety-critical checks must pass for a scenario pass.

The JSON schema is
`coding-memory-scenarios.v1.schema.json`. The focused validator is
`tests/product_tdmem_0901_scenario_corpus_test.py` and can be run without
Cargo or model credentials:

```sh
python3 tests/product_tdmem_0901_scenario_corpus_test.py
```

The corpus intentionally does not name or require a concrete provider. A
future differential runner can run the same scenario against any provider or
baseline by injecting the run identity and mapping provider results to the
generic terminal/admission vocabulary.
