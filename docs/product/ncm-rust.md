# NCM Rust release lifecycle

NCM is an independent Rust implementation of the Biomem based `ncm-biomem-rs.v1`
contract. It is not an official OpenTechLab product and it does not claim to
move memory into a decoder's hidden state. The capability is opt in, and
Native remains the fallback on every target that does not have a pinned NCM
worker artifact.

The release matrix currently supports NCM only on `aarch64-apple-darwin`
(`aarch64-macos`). The normal CLI archive contains the CLI alone. The matching
worker sidecar is named
`tracedecay-ncm-worker-<tag>-aarch64-macos.tar.gz` and contains exactly:

* `tracedecay-ncm-worker`, the executable worker;
* `worker-manifest.json`, which pins the worker protocol, target, size and
  SHA-256; and
* `model-acquisition-manifest.json`, which binds this target to the exact
  MiniLM revision and the five model file digests.

The sidecar archive and its `.sha256` file are target specific. A manifest from
another target, a worker copied from another archive, or a sidecar without the
model acquisition manifest is rejected by the release artifact gate. The
source of truth for the worker matrix is
[`product/ncm/reference/worker-platforms.json`](../../product/ncm/reference/worker-platforms.json),
and the release descriptor is
[`product/ncm/release/model-acquisition-manifest.json`](../../product/ncm/release/model-acquisition-manifest.json).

## Installed release verification

Run the portable regression gate from a checkout of the release source:

```bash
python3 scripts/product/ncm/verify-installed.py
```

The gate checks the target-bound release descriptor, the trusted worker and
embedding manifests, and the transaction and receipt schema without contacting
the network. Given release assets, it safely extracts and smokes the CLI, then
verifies the worker sidecar and its archive checksum:

```bash
python3 scripts/product/ncm/verify-installed.py \
  --binary-archive tracedecay-v<version>-aarch64-macos.tar.gz \
  --worker-archive tracedecay-ncm-worker-v<version>-aarch64-macos.tar.gz
```

The release binary is still installed through the normal archive path. NCM
model acquisition is an explicit operation under the chosen absolute state
root. The release verifier can exercise the same operation with an offline
fixture source (the regression tests use this mode; production uses the HTTPS
URLs pinned in the descriptor):

```bash
python3 scripts/product/ncm/verify-installed.py \
  --manifest product/ncm/release/model-acquisition-manifest.json \
  --model-root "$PWD/.local/ncm-state" \
  --operation install
```

The model transaction performs these steps:

1. refuse a pending lifecycle journal and validate the target, repository,
   immutable revision, URL and every expected size/digest;
2. download into a private same-filesystem staging directory, hashing each
   file as it is written;
3. materialize the exact runtime layout under
   `models/models--Xenova--paraphrase-multilingual-MiniLM-L12-v2/`, including
   `refs/main`, the immutable revision snapshot and
   `ncm-encoder-manifest.json`;
4. fsync the candidate, move the previous `models` directory to a private
   backup, and atomically publish the verified candidate; and
5. verify the published tree before writing
   `receipts/ncm-model-acquisition-v1.json`.

An install against an already verified tree records `already_present` without
redownloading. An update records `committed` only after the new tree and all
digests verify. A failed download or publication removes only its private
staging state and restores the previous model tree; the journal can be
replayed with `--operation recover`. Other state-root entries, including the
provider namespace, are not part of this transaction.

The receipt records the operation id, target and release name, model/repository
identity, immutable revision, embedding-manifest digest, acquisition-manifest
digest, complete file digest list, published tree digest and creation time.
It is the evidence required before an installed worker may be considered ready.
Missing or mismatched model artifacts remain unavailable; they are never
silently replaced from an ambient Hugging Face cache or environment override.

On `x86_64-linux`, `aarch64-linux`, and `x86_64-windows`, the release policy is
explicitly `native-only` until a separately built worker and target-bound
manifest are accepted. A successful portable Rust build alone does not widen
that claim.

## Release checks

The two release workflows package and compare both sidecar manifests after
extraction. Their portable validation list runs
`scripts/product/ncm/test-verify-installed.py`, which covers clean model
installation, an already-installed profile, transactional update failure and
success, sidecar target binding, archive checksums and CLI smoke. The broader
artifact checks remain in `scripts/check-release-artifacts.py`; the feature and
platform matrix checks remain in `scripts/check-distribution-feature-wiring.py`
and `scripts/product/ncm/check-worker-platform.py`.
