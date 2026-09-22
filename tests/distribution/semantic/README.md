# Offline semantic distribution fixture

`fixture.json` is the checked-in identity declaration for the real Jina code
embedding package. The model bytes are intentionally absent from Git. The
manifest restores the historical pin at revision
`516f4baf13dec4ddddda8631e019b5737c8bc250` and declares all five members,
including exact lengths and SHA-256 values.

Use an explicit local directory of those bytes as the provisioning source:

```sh
python3 -S -B tests/distribution/semantic/prepare_fixture.py \
  /path/to/jina-fixture \
  /path/to/profile/semantic-model
python3 -S -B tests/distribution/semantic/validate_fixture.py \
  /path/to/profile/semantic-model
```

Preparation is deliberately offline. There is no curl, hub lookup, ambient
cache discovery, query-time download, or model substitution in this contract.
The source directory must already contain `fixture.json` and every declared
member. Preparation stages and hashes each regular, single-link file, writes a
journal before publication, and atomically renames the completed directory.
The receipt sidecar binds the target, model, revision, manifest digest, and
historical artifact digest.

Authenticity is deliberately digest-only: these manifest and package digests
identify the pinned bytes, while signature verification and signed attestations
remain outside this offline provisioner.
