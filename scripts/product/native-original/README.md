# Direct original Native comparison

This directory contains the accepted runner and case contract for comparing
the untouched upstream PR707 Native runtime at
`57006f60cb45bcee8487e73a40d4fad1a12ee2b6` with the current candidate
checkout. The runner invokes each side through a published `tracedecay tool`,
MCP stdio, or an explicitly supplied external command boundary. It does not
implement a store, score a response, or create a baseline result. The older
b3/571 revisions remain historical audit metadata and are never used as the
execution oracle.

Validate a case before the build owner runs it:

```sh
python3 -S scripts/product/native-original/runner.py validate \
  --contract scripts/product/native-original/case-contract.json \
  --cases scripts/product/native-original/example-case.json
```

Create or verify the immutable detached test-input checkout at
`/Users/satan/workspace/bleedingdev/projects/tracedecay/.worktrees/native-original-reference-57006f60`
at the pinned revision. An existing checkout must be clean; the runner never
resets or deletes it. Reference verification is fixed to this 570 revision and
has no revision override. Build the reference and candidate independently with
`cargo build -p tracedecay-cli --bin tracedecay --features test-transport`.
The reference build uses `test-transport`; it does not use
`memory-provider-host`. Supply the candidate source root/revision so the
moving candidate identity is recorded on every run.

```sh
python3 -S scripts/product/native-original/runner.py run \
  --contract scripts/product/native-original/case-contract.json \
  --cases scripts/product/native-original/cases/facts/search.json \
  --reference-checkout /Users/satan/workspace/bleedingdev/projects/tracedecay/.worktrees/native-original-reference-57006f60 \
  --original-binary /Users/satan/workspace/bleedingdev/projects/tracedecay/.worktrees/native-original-reference-57006f60/target/release/tracedecay \
  --product-binary target/release/tracedecay \
  --product-source-root /Users/satan/workspace/bleedingdev/projects/tracedecay/.worktrees/pluggable-memory-providers-v2 \
  --product-source-revision "$(git rev-parse HEAD)" \
  --artifact-root target/task-scratch/native-original
```

Each case gives the original and product sides independent source, binary,
process, store, project, profile, state, environment, and artifact roots.
`side_setup` actions seed those roots through public routes.
`output_bindings` capture response JSON pointers per side and make generated
IDs available as `${bindings.name}` in later requests; an absent binding is
unknown. `composition_proof` is a required side-specific hook for parity cases
and must name response pointers that prove which production boundary was
selected. A proof may contain ordered `checks` when selection configuration and
runtime status are separate responses. `lifecycle` provides explicit
runner-owned daemon identity, close, and fresh-authority reopen observations.
A checkpoint named `reopened` without that hook is recorded as unknown, and a
close child response without daemon exit evidence is recorded as unknown.

The accepted session/LCM surface includes public `tracedecay_message_search`,
`tracedecay_session_lookup`, and `tracedecay_sessions_for` lookups alongside
the retained LCM read and refresh routes. Retired public operations are not
advertised by this contract. Each paired action declares whether its operation
requires mutation effect/receipt/state evidence, explicit no-effect evidence,
or retrieval evidence; missing declarations produce `effect_unknown`.

Use `--readiness-mode readiness` only with the complete accepted route set.
The runner rejects every `--operation` filtered suite in that mode and rejects
any case collection missing an accepted Native, session/LCM, or host-extension
route. Focused development runs use the default `comparison` mode and do not
claim readiness. Every case also emits an immutable row in `attempts.jsonl`
with binary, process, store, composition, effect, receipt, state, and reopen
identities. `cancelled`, `partial`, `effect_unknown`, and `blocked` remain
typed outcomes.

The runner preserves request bytes, stdout, stderr, process metadata, parsed
responses, bindings, proof assessments, cleanup receipts, root identities, and
comparison decisions under the run artifact directory. Comparison projections
are case-declared JSON pointers only. Score, order, provenance, omissions,
receipts, errors, and state observations remain exact unless a case declares a
reviewed identity mapping or nondeterministic pointer. `pass`, `fail`,
`unknown`, `unsupported`, `invalid`, `censored`, `cancelled`, `partial`,
`effect_unknown`, and `blocked` outcomes stay visible;
missing binaries, same binaries or binary roots, modified reference source,
zero relevant cases, and cleanup failure cannot pass or become an empty
success. Host-extension regression cases retain product evidence while their
unavailable original counterpart is classified separately.
