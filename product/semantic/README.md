# Code-semantic model authority

This directory is the product-side declaration for the opt-in V2 dense code
semantic authority. `model-manifest.json` restores the real CPU FastEmbed/Jina
model pin recorded by historical commits `1cbad2dda` and `dca4de9de`:

- model: `JinaEmbeddingsV2BaseCode`
- upstream revision: `516f4baf13dec4ddddda8631e019b5737c8bc250`
- dimensions: `768`
- maximum input length: `8192`
- catalog package digest:
  `70be81163e9740d742b7857e132713b323b5042d661485354d781cb8313c15af`

The five payload members and their byte lengths and SHA-256 values are
declared in the manifest. The package digest uses the historical Rust
`catalog_package_digest` encoding: model id, revision, and the sorted role,
upstream path, little-endian length, and member digest tuples.

This is a digest-only authenticity contract. The manifest and package digests
identify the pinned bytes, but they are not signatures or signed attestations;
signature verification is intentionally outside this offline provisioner.

Provisioning accepts a local directory containing `fixture.json` and exactly
the declared members. It validates the complete inventory before copying,
rejects traversal, absolute, platform-ambiguous, symlink, hard-link, and
special-file entries, and writes files with one link each. It then records a
target-bound journal, atomically publishes the complete staged directory, and
writes a schema-valid acquisition receipt beside the target.

The command never resolves a hub URL, searches an ambient model cache, starts
an inference runtime, or performs query work. An operator or a separate
release preparation job may obtain the pinned bytes; the provisioning command
only consumes the explicit local directory and remains usable with networking
disabled:

```sh
python3 -S -B scripts/product/semantic/provision_model.py --check
python3 -S -B scripts/product/semantic/provision_model.py \
  --manifest product/semantic/model-manifest.json \
  --source /path/to/jina-516f4baf13dec4ddddda8631e019b5737c8bc250 \
  --target /path/to/profile/semantic-model
python3 -S -B scripts/product/semantic/verify_model.py \
  --manifest product/semantic/model-manifest.json \
  --target /path/to/profile/semantic-model --json
```

The journal and staging directory are recoverable after interruption. A later
run resumes only from the recorded target and identity, verifies staged bytes,
and publishes once complete. A corrupt target, receipt, journal, or stage is
reported and left for explicit repair. A complete target plus matching receipt
is an idempotent success. `--uninstall` durably moves a verified publication to
a rollback snapshot, and `--rollback` restores that snapshot through its own
journal; either operation can be resumed after interruption.
