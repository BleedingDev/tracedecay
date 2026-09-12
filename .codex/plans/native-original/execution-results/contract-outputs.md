# Memory Provider V1 contract generator inventory

This inventory was captured before any generator invocation for `rn-contract`.
All listed generated outputs are inside the owned
`product/contracts/memory-provider-v1/**` root. The generator and checker
scripts are outside this node's ownership and remain read-only.

## Rust binding generator

Authoritative command:

```text
python3 scripts/product/generate-memory-provider-rust.py --repo . --check
```

Read inputs consumed by `scripts/product/generate-memory-provider-rust.py`:

| Path | Role |
|---|---|
| `product/contracts/memory-provider-v1/contract-set.json` | accepted six-contract order, compatibility and catalog authority |
| `product/contracts/memory-provider-v1/provider-registry-contract.json` | contract 1 values |
| `product/contracts/memory-provider-v1/provider-registry-contract.schema.json` | contract 1 schema |
| `product/contracts/memory-provider-v1/provider-handshake-contract.json` | contract 2 values |
| `product/contracts/memory-provider-v1/provider-handshake-contract.schema.json` | contract 2 schema |
| `product/contracts/memory-provider-v1/provider-observation-contract.json` | contract 3 values |
| `product/contracts/memory-provider-v1/provider-observation-contract.schema.json` | contract 3 schema |
| `product/contracts/memory-provider-v1/provider-recall-contract.json` | contract 4 values |
| `product/contracts/memory-provider-v1/provider-recall-contract.schema.json` | contract 4 schema |
| `product/contracts/memory-provider-v1/provider-lifecycle-contract.json` | contract 5 values |
| `product/contracts/memory-provider-v1/provider-lifecycle-contract.schema.json` | contract 5 schema |
| `product/contracts/memory-provider-v1/provider-terminal-contract.json` | contract 6 values |
| `product/contracts/memory-provider-v1/provider-terminal-contract.schema.json` | contract 6 schema |
| `scripts/product/generate-memory-provider-rust.py` | generator source, recorded by the generated manifest |

Existing outputs (generator-owned; never hand-edit):

| Path | Status before this node |
|---|---|
| `product/contracts/memory-provider-v1/generated/rust/memory_provider_v1.rs` | present |
| `product/contracts/memory-provider-v1/generated/rust/manifest.json` | present |

## Golden fixture generator

Authoritative command:

```text
python3 scripts/product/generate-memory-provider-goldens.py --repo . --check
```

Read inputs consumed or existence-validated by
`scripts/product/generate-memory-provider-goldens.py`:

| Path | Role |
|---|---|
| `product/contracts/memory-provider-v1/contract-set.json` | contract order, compatibility rules and golden authority |
| `product/contracts/memory-provider-v1/golden-scenarios.json` | fixture scenario source |
| `product/contracts/memory-provider-v1/golden-scenarios.schema.json` | declared scenario schema authority; validated by the contract-set checker |
| `product/contracts/memory-provider-v1/provider-{registry,handshake,observation,recall,lifecycle,terminal}-contract.json` | contract digests embedded in fixture lines |
| `product/contracts/memory-provider-v1/provider-{registry,handshake,observation,recall,lifecycle,terminal}-contract.schema.json` | schema digests embedded in the golden manifest |
| `product/contracts/memory-provider-v1/{README.md,provider-handshake-contract.md,provider-observation-contract.md,provider-recall-contract.md,provider-lifecycle-contract.md,provider-terminal-contract.md}` | documentation paths required by contract-set metadata/checks |
| `scripts/product/check-provider-{registry,handshake,observation,recall,lifecycle,terminal}-contract.py` | checker paths required by contract-set metadata/checks |
| `tests/product_provider_{registry,handshake,observation,recall,lifecycle,terminal}_contract_test.py` | test paths required by contract-set metadata/checks |
| `scripts/product/generate-memory-provider-goldens.py` | generator source, recorded by the golden manifest |

Existing outputs (generator-owned; never hand-edit):

| Path | Status before this node |
|---|---|
| `product/contracts/memory-provider-v1/goldens/fixtures.jsonl` | present |
| `product/contracts/memory-provider-v1/goldens/manifest.json` | present |

## Decision for this node

The once-delivered Native context marker is an owned runtime API identity and
documentation clarification. It does not add a wire field, capability, or
canonical JSON contract member. Therefore none of the four generated outputs
is regenerated or changed by `rn-contract`; the two `--check` commands above
remain the required zero-drift verification.
