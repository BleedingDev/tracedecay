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

# Verify the normative executable wrapper and its argument/exit-status boundary.
python3 -S scripts/product/native-original/test_native_original_runner_wrapper.py

# Run the complete dependency-free runner suite; this also invokes the
# normative wrapper check above.
python3 -S scripts/product/native-original/test_runner.py
```

Create or verify the immutable detached test-input checkout at
`/Users/satan/workspace/bleedingdev/projects/tracedecay/.worktrees/native-original-reference-57006f60`
at the pinned revision. An existing checkout must be clean; the runner never
resets or deletes it. Reference verification is fixed to this 570 revision and
has no revision override. Build the two sides with their declared feature
sets: the immutable reference uses
`cargo build -p tracedecay-cli --bin tracedecay --features test-transport`,
while the moving candidate uses the default `production` feature plus
`memory-provider-host,semantic-fastembed`:
`cargo build -p tracedecay-cli --bin tracedecay --features memory-provider-host,semantic-fastembed`.
The reference build does not use `memory-provider-host`. The frozen NCM rows
use the `independent_real_worker_conformance` oracle because the 570 checkout
does not ship an encoder worker: the reference side records an explicit
unavailable worker boundary, while the candidate side must attest a real
worker independently. The worker attestation is a runner input with this
concrete shape (all hashes are lowercase SHA-256 values):

```json
{
  "format": "tracedecay.native-original.ncm-worker-attestation.v1",
  "kind": "ncm_encoder_worker",
  "worker_id": "ncm-worker-v1",
  "binary_path": "/opt/tracedecay/bin/ncm-worker",
  "binary_sha256": "<64-hex>",
  "source_root": "/opt/tracedecay-src",
  "source_revision": "<40-hex Git SHA>",
  "protocol_version": "1.0",
  "implementation_sha256": "<64-hex>",
  "model_artifact_sha256": "<64-hex>",
  "tokenizer_sha256": "<64-hex>",
  "vector_fixture_digest": "<64-hex>",
  "pin_sha256": "<SHA-256 of the canonical identity fields>"
}
```

Pass that manifest with `--ncm-worker-attestation <path>`. The runner requires
the pinned `worker_id` `ncm-worker-v1`, all implementation/model/tokenizer/
vector pins, a native image outside system executable roots, and a separately
observed live worker PID whose `/proc/<pid>/exe` path and digest match the
attestation on Linux. On macOS the runner uses the OS-owned
`libproc.proc_pidpath` plus the exact 136-byte
`proc_pidinfo(PROC_PIDTBSDINFO)` pid/parent/uid/start-time record, and
cross-checks one `txt` vnode with root-owned `/usr/sbin/lsof`; it does not
fall back to `ps` or depend on `/proc`.
Shells,
interpreters, echo utilities, scripts, symlinks, and response-only worker
claims are unavailable. The worker process must be a child of the runner-owned
daemon incarnation named by the authenticated action receipt and its runtime
identity. The manifest is read through a stable non-symlink file and is
re-read before use; it is never obtained from a child response. The runner
copies the worker into an owner-private immutable root before accepting the
evidence. Supply the
candidate source root/revision so the moving candidate identity is recorded
on every run.

```sh
python3 -S scripts/product/native-original/runner.py run \
  --contract scripts/product/native-original/case-contract.json \
  --cases scripts/product/native-original/cases/facts/search.json \
  --reference-checkout /Users/satan/workspace/bleedingdev/projects/tracedecay/.worktrees/native-original-reference-57006f60 \
  --original-binary /Users/satan/workspace/bleedingdev/projects/tracedecay/.worktrees/native-original-reference-57006f60/target/release/tracedecay \
  --product-binary target/release/tracedecay \
  --product-source-root /Users/satan/workspace/bleedingdev/projects/tracedecay/.worktrees/pluggable-memory-providers-v2 \
  --product-source-revision "$(git rev-parse HEAD)" \
  --original-build-attestation target/task-scratch/native-original/original-build-attestation.json \
  --product-build-attestation target/task-scratch/native-original/product-build-attestation.json \
  --ncm-worker-attestation target/task-scratch/native-original/trusted-ncm-worker.json \
  --artifact-root target/task-scratch/native-original
```

The build owner must write the product attestation with the selected binary
path and SHA-256, candidate source root, and full Git revision. A missing,
unknown, or mismatched attestation blocks the run before a process starts.

`case-contract.json` advertises Draft 2020-12 for its embedded `case_schema`;
the schema's `matrix_binding` fields include the complete nested
`identity_evidence` and `effect_receipt_state` shapes, plus the frozen
top-level and track-specific identity requirements. Runtime validation also
checks the frozen row binding digest for operation, oracle case, profile,
track, identity, and both evidence shapes.

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
Fact-store MCP actions use the daemon's action-specific mapping, for example
`fact_store_search` → `tracedecay_fact_store_search`; the generic
`tracedecay_fact_store` dispatcher is not a substitute for a published route.

Use `--readiness-mode readiness` only with the complete frozen matrix.
The runner loads and verifies the frozen 201-row matrix from
`.codex/plans/native-original/execution-results/readiness-matrix.json`,
including every row ID, oracle kind, and planned run count. It rejects every
`--operation` filtered suite in that mode and rejects any case collection
missing a matrix row. Focused development runs use the default
`comparison` mode and do not claim readiness. The frozen matrix's normative
compatibility name is `native-original-runner --case <row-id> --ledger
<attempts.jsonl>`; in this source checkout the equivalent invocation is
`python3 -S scripts/product/native-original/runner.py --case <row-id> --ledger
<attempts.jsonl>`, and the build/release wrapper must expose the normative
name. Without full run inputs it records a typed `blocked` attempt. Every case also
emits an immutable row in `attempts.jsonl` with binary, process, observed store,
operation reachability, composition, effect, receipt, state, checkpoint, and
reopen identities. `cancelled`, `partial`, `effect_unknown`, and `blocked`
remain typed outcomes. A readiness report is also `blocked` until the observed
ledger population reaches every row's frozen `planned` attempt count; planned
counts recorded as metadata never count as executed attempts.
This checkout intentionally does not bundle a complete 201-row case collection:
`example-case.json` is a focused comparison fixture, and readiness validation
rejects it until all frozen rows and their 1,381 observed attempts are supplied.
Every readiness case must carry `matrix_binding` that exactly repeats its
frozen row ID, track, profile, operation, oracle kind/case, identity profile,
and the complete nested identity/effect evidence shape (including required
field names). Every executable readiness action must also carry
`matrix_row_id`, `matrix_operation`, `matrix_oracle_case`, and `matrix_route`.
The runner translates public matrix rows to their published callable route and
rejects a route such as `fact_store_search` when it is being used to stand in
for a different frozen operation. Rows without a published route must repeat
their frozen `matrix_candidate_route` and `matrix_reference_route` until an
external adapter is supplied; they cannot claim readiness through a generic
route.
Unexpected or over-counted rows also block readiness. The final run manifest and each case result are replaced
atomically after execution so partial runs retain their observed ledger and
never claim the frozen planned population.
Each finalized `case-result.json` is newline-terminated canonical JSON. Its
`result_digest` is the SHA-256 of the same canonical payload with the digest
field (and nested ledger-row digest fields) set to `null`; the runner verifies
both that digest and the exact serialized bytes when reading the artifact.

Reachability and store identity are accepted only when a response carries the
runner-owned daemon's authenticated action receipt. For an owned MCP daemon,
the runner first sends `initialize` with `_meta.nativeOriginalActionPrepare`,
checks the public acknowledgement, and then sends the actual `tools/call`
with `_meta.nativeOriginalActionNonce` (plus the action digest, scope, and
entrypoint). The terminal response must carry
`_meta.nativeOriginalActionReceipt`, or the same receipt under JSON-RPC error
data. The runner verifies the length-prefixed action/result digests, daemon
generation, matching acknowledgement expiry, live store coordinates, and HMAC
using the proof key retained only in runner memory. Receipt SHA-256 uses the
same length-prefixed format/revision/field transcript as the daemon; the
cross-language vectors are checked independently by `test_runner.py`: action
`ba976a9affb57255509c3074812528652eba127bb43824083d42b512886b18dc`, nested
canonical JSON `845e97727c85776f2eb6586b00fa21b97feb290257eeec38f7e5a2957caa501c`,
receipt-stripped result `bddd60f823dbda9f775dde4bf2bd2a2431141f1e6752c43059a92029bf3193fa`,
coherent HMAC `3dfd3d15b4dc2428aaec6a8acaab13e570e92504dc1d7542731419c824667ad2`,
and receipt SHA `7a0a9eaffd0a3834cf2468752818b5cb17ca5f249c32b54d64cd810602d343c3`.
A response's own
store/provider label is retained as raw
evidence but cannot establish ownership; missing or mismatched receipts block
the action. Receipt-backed requests fail closed when they contain explicit
`session_id`, `thread_id`, `conversation_id`, provider/session aliases, or
other caller-selected scope selectors; the daemon-selected scope must be
obtained before prepare. Digest-bearing action and result JSON rejects all
floating-point values until both sides publish an exact number canonicalizer.
Native, NCM, and semantic matrix tracks record their track-specific
identity fields and block a pass when required fields are missing. The runner
records every retained production terminal status and maps cancelled, partial,
unavailable, effect-unknown, and blocked outcomes without collapsing them into
pass.

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
