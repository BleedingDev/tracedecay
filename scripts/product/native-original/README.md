# Direct original Native comparison

This directory contains the runner and the proposed case contract for
comparing an untouched b3 Native runtime with a selected product runtime. The
runner invokes each side through a published `tracedecay tool`, MCP stdio, or
explicit external command boundary. It does not implement a store or create a
baseline response.

Validate a case before the build owner runs it:

```sh
python3 -S scripts/product/native-original/runner.py validate \
  --contract scripts/product/native-original/case-contract.json \
  --cases scripts/product/native-original/example-case.json
```

Run the comparison only with an existing clean b3 checkout and separately
built executables. The candidate source revision is supplied by the build
owner; its binary digest is always captured.

```sh
python3 -S scripts/product/native-original/runner.py run \
  --contract scripts/product/native-original/case-contract.json \
  --cases scripts/product/native-original/cases/facts/search.json \
  --reference-checkout .worktrees/native-original-reference-b3 \
  --original-binary target/reference/release/tracedecay \
  --product-binary target/release/tracedecay \
  --product-source-revision 571daf3a9612e5247443e4da3a107b542686c1ef \
  --artifact-root target/task-scratch/native-original
```

Each case gives the original and product sides independent project, profile,
state, environment, and artifact roots. `side_setup` actions seed those roots
through public routes. `output_bindings` capture response JSON pointers per
side and make generated IDs available as `${bindings.name}` in later requests;
an absent binding is unknown. `composition_proof` is a required side-specific
hook for parity cases and must name response pointers that prove which
production boundary was selected. A proof may contain ordered `checks` when
selection configuration and runtime status are separate responses. `lifecycle` provides explicit runner-owned
daemon identity, close, and fresh-authority reopen observations. A checkpoint
named `reopened` without that hook is recorded as unknown, and a close child
response without daemon exit evidence is recorded as unknown.

The runner preserves request bytes, stdout, stderr, process metadata, parsed
responses, bindings, proof assessments, and comparison decisions under the
run artifact directory. Comparison projections are case-declared JSON
pointers only. Score, order, provenance, omissions, receipts, errors, and
state observations remain exact unless a case declares a reviewed identity
mapping or nondeterministic pointer. Unknown, unsupported, invalid, and
censored outcomes stay visible and are never converted into empty success.
