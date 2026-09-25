# Semantic code search

Semantic code search is an optional project-scoped lane layered onto
TraceDecay's exact, lexical, and graph retrieval. It runs the pinned
`JinaEmbeddingsV2BaseCode` model on CPU, produces 768-dimensional vectors, and
scans them with the exact-flat index. The embedding document defaults to
`SymbolContextHeader`, which places deterministic symbol identity and scope
before the sanitized source text.

The runtime is disabled by default. Automatic model acquisition is also off by
default, and a search request never downloads or loads a model.

## Keep V1 and V2 profiles separate

V2 accepts only the supported exact-final profile shape described in
[Profile Storage Support Boundary](PROFILE-STORAGE-SUPPORT.md). Do not point a
V2 binary at an existing V1 profile, including a profile created by the 0.0.74
release at schema version 18. Preserve that profile and create a new, empty V2
profile instead.

The CLI has no global `--profile` option. Pin the binary and every storage route
explicitly in the shell that will run V2 commands. Keep the profile outside the
project being indexed.

```sh
export TD_BIN=/absolute/path/to/the/v2/tracedecay
export TD_PROJECT=/absolute/path/to/the/repository
export TD_V2_PROFILE="$HOME/.tracedecay-v2"

mkdir -m 700 "$TD_V2_PROFILE"
export TD_V2_PROFILE="$(cd "$TD_V2_PROFILE" && pwd -P)"
export TRACEDECAY_DATA_DIR="$TD_V2_PROFILE"
export TRACEDECAY_GLOBAL_DB="$TD_V2_PROFILE/global.db"
export TRACEDECAY_DAEMON_SOCKET="$TD_V2_PROFILE/daemon.sock"

"$TD_BIN" --version
```

These settings isolate the profile registry and global database, the daemon
transport, and project shards. V2 project stores are created below
`$TD_V2_PROFILE/projects/<project-id>`. `HOME` does not need to change.
Normal host-session discovery can still read supported agent-host sources under
the user's home; these settings redirect TraceDecay storage and service state,
not those input sources.

On macOS, a Unix socket path can contain at most 103 bytes. The example above
fits most home paths. If the daemon reports that its socket path is too long,
choose a shorter absolute profile path. Profiles do not otherwise need short
paths.

When building this source checkout, include both optional capabilities used by
the integrated V2 binary:

```sh
hauler exec -- cargo build -p tracedecay-cli \
  --features memory-provider-host,semantic-fastembed
export TD_BIN="$PWD/target/debug/tracedecay"
```

Run the V2 daemon in the foreground in a dedicated terminal, with the same
environment exports:

```sh
"$TD_BIN" daemon run \
  --profile-root "$TD_V2_PROFILE" \
  --socket "$TRACEDECAY_DAEMON_SOCKET"
```

Do not replace or restart the installed V1 service. The
`daemon install-service`, `start`, and `restart` commands manage that installed
user service, so they are not the restart mechanism for this isolated
foreground daemon.

In a second terminal, repeat the same `TD_*` and `TRACEDECAY_*` exports. With
the isolated V2 daemon already listening, initialize the project into that
profile:

```sh
"$TD_BIN" init "$TD_PROJECT"
```

Run the remaining CLI commands from this second terminal.

## Configure memory recall

The memory-provider host is separate from semantic code search, but it uses the
same isolated profile and foreground-daemon restart procedure. Provider
participation and active recall routing are both disabled by default. Enabling
a provider does not select it for recall.

There is not yet a dedicated provider-selection command. `ncm install` enables
the NCM observer while deliberately preserving Native enablement and recall
routing. Until the CLI owns provider selection, set the two public project
settings through the typed configuration tools. The following helper uses
`jq`, reads the current configuration revision before every write, and applies
each value with revision compare-and-swap:

```sh
decode_tool() {
  jq 'if ((.content? | type) == "array") then first(.content[]? | select(.type == "text") | .text | fromjson) else . end'
}

TD_PROJECT_ID=$(
  "$TD_BIN" tool --project "$TD_PROJECT" tracedecay_project_context \
    --args '{"format":"json"}' --json |
    decode_tool |
    jq -r '.. | objects | .project_id? // empty' |
    sort -u | tail -1
)

configuration_revision() {
  "$TD_BIN" tool --project "$TD_PROJECT" tracedecay_configuration_observed_state \
    --args '{}' --json |
    decode_tool |
    jq -r '.. | objects | .desired_revision_id? // empty' |
    sort -u | tail -1
}

set_project_setting() {
  key=$1
  value=$2
  idempotency_key=$3
  payload=$(jq -cn \
    --arg project "$TD_PROJECT_ID" \
    --arg key "$key" \
    --arg revision "$(configuration_revision)" \
    --arg idempotency "$idempotency_key" \
    --argjson value "$value" \
    '{layer:{kind:"project",project_id:$project},key:$key,value:$value,expected_revision:$revision,idempotency_key:$idempotency}')
  "$TD_BIN" tool --project "$TD_PROJECT" tracedecay_configuration_set \
    --args "$payload" --json
}
```

Use a fresh idempotency key for each intended change. To enable Native and make
it the active recall provider:

```sh
set_project_setting memory.provider_native_enabled.v1 \
  '{"kind":"boolean","value":true}' native-enable-01
native_route=$(jq -cnS \
  '{active_provider:"tracedecay.native",degradation:null,fallback:null}')
set_project_setting memory.provider_recall_routing.v1 \
  "$(jq -cn --arg value "$native_route" '{kind:"text",value:$value}')" \
  native-route-01
```

Stop and restart the isolated foreground daemon after changing either setting.
This routing document is fail-closed: it does not allow degraded provider
results. Add a degradation policy only when the operator intends to accept its
specific typed causes.

### Prepare a local NCM sidecar

NCM is supported only on `aarch64-apple-darwin`; the other release targets are
Native-only. Build the production worker separately from the CLI:

```sh
hauler exec -- cargo build --locked \
  -p tracedecay-memory-ncm-runtime \
  --bin tracedecay-ncm-worker \
  --release \
  --target aarch64-apple-darwin \
  --no-default-features \
  --features real-encoder
```

The checked-in manifest pins one exact worker. A new build is not accepted
merely because it used this command; continue only when the archive verifier
below accepts its bytes. The locally verified artifact for this checkout uses
the `8e554dbf` paths below.

Package the worker with the two checked-in trust manifests, then verify the
archive before extracting it:

```sh
export TD_NCM_ARCHIVE="$PWD/target/ncm-sidecar-local/tracedecay-ncm-worker-v2-local-8e554dbf-aarch64-macos.tar.gz"
export TD_NCM_BUNDLE="$PWD/target/ncm-sidecar-local/source-8e554dbf"

mkdir -p "$(dirname "$TD_NCM_ARCHIVE")"
python3 scripts/package-release-archive.py \
  --binary target/aarch64-apple-darwin/release/tracedecay-ncm-worker \
  --output "$TD_NCM_ARCHIVE" \
  --format tar.gz \
  --entry-name tracedecay-ncm-worker \
  --epoch "$(git show -s --format=%ct HEAD)" \
  --companion product/ncm/reference/worker-manifest.json=worker-manifest.json \
  --companion product/ncm/release/model-acquisition-manifest.json=model-acquisition-manifest.json

python3 scripts/product/ncm/verify-installed.py \
  --worker-archive "$TD_NCM_ARCHIVE" \
  --target aarch64-apple-darwin \
  --release-name aarch64-macos
mkdir "$TD_NCM_BUNDLE"
tar -xzf "$TD_NCM_ARCHIVE" -C "$TD_NCM_BUNDLE"
```

The verifier fails if the worker bytes do not match the pinned manifest or if
either companion manifest is missing or changed. The complete artifact and
model contract is documented in
[NCM Rust release lifecycle](product/ncm-rust.md).

Acquire the pinned model into the isolated V2 profile, then ask the CLI to
install the verified worker bundle for this project:

```sh
export TD_NCM_STATE="$TD_V2_PROFILE/ncm-state"
python3 scripts/product/ncm/verify-installed.py \
  --manifest product/ncm/release/model-acquisition-manifest.json \
  --revision-receipt product/ncm/receipts/backend/2fc72f1d81f543224d8e7d8ef19195b026ba855f.json \
  --model-root "$TD_NCM_STATE" \
  --operation install

"$TD_BIN" ncm install \
  --path "$TD_PROJECT" \
  --worker "$TD_NCM_BUNDLE/tracedecay-ncm-worker" \
  --state-root "$TD_NCM_STATE" \
  --yes --json
```

Use the same absolute state-root spelling for model acquisition and
`ncm install`. The acquisition receipt binds that string exactly; on macOS,
for example, do not mix `/tmp/...` with its `/private/tmp/...` resolution.

`ncm install` enables the NCM observer but preserves Native and the current
route. Select NCM only after the install command returns a committed receipt:

```sh
ncm_route=$(jq -cnS \
  '{active_provider:"ncm",degradation:null,fallback:null}')
set_project_setting memory.provider_recall_routing.v1 \
  "$(jq -cn --arg value "$ncm_route" '{kind:"text",value:$value}')" \
  ncm-route-01
```

Restart the isolated foreground daemon, then inspect worker, model, and
recovery state:

```sh
"$TD_BIN" ncm status --path "$TD_PROJECT" --json
```

Switching back to the Native routing document above makes Native active again;
the installed NCM remains an observer. The final fresh-profile provider smoke
for this checkout is still in progress, so do not treat this section as a
readiness claim yet.

## Enable and prepare the semantic lane

Inspect the persisted configuration and the runtime mounted by the daemon:

```sh
"$TD_BIN" semantic status --path "$TD_PROJECT" --json
```

Enable semantic retrieval without automatic acquisition:

```sh
"$TD_BIN" semantic enable --path "$TD_PROJECT"
```

Enabling or disabling changes project configuration. Restart the daemon before
expecting the mounted runtime to change. For the isolated foreground daemon,
stop it and rerun the same `daemon run` command above.

After the restart, explicitly acquire the pinned catalog artifact:

```sh
"$TD_BIN" semantic acquire --path "$TD_PROJECT" --json
```

Alternatively, import a complete local artifact package. TraceDecay verifies
the canonical manifest and every declared package member before installing it:

```sh
"$TD_BIN" semantic import \
  --path "$TD_PROJECT" \
  --manifest /absolute/path/to/manifest.json \
  --source /absolute/path/to/artifact-directory \
  --json
```

Rerun `semantic status --json` while acquisition and indexing progress. The
lane is ready for strict queries when both of these observations are present:

- `observed_runtime.model_lifecycle.state.state` is `ready`.
- `observed_runtime.semantic_index.status.state` is `current`.

Other lifecycle states, including `selected_not_downloaded`, `downloading`,
`verifying`, `loading`, `indexing`, and `failed`, are truthful non-ready states.
The status response includes remediation flags and any runtime degradation or
failure reason.

To let project open queue acquisition in the background, enable with
`--auto-download` instead. This does not allow downloads on the query path.

```sh
"$TD_BIN" semantic enable --path "$TD_PROJECT" --auto-download
```

Restart the daemon after changing this setting.

## Search

Semantic retrieval is opt-in per request. Omitting `semantic_mode` keeps the
baseline exact, lexical, and graph search.

The examples use both JSON controls. `"format":"json"` requests structured
tool content, while the CLI's `--json` flag prints the raw MCP response.

Use `fallback` for ordinary work. It requests the semantic lane when ready and
preserves baseline results when semantics is unavailable:

```sh
"$TD_BIN" tool --project "$TD_PROJECT" search --args - --json <<'JSON'
{"query":"where is project identity resolved?","semantic_mode":"fallback","limit":10,"format":"json"}
JSON
```

Use `strict` when the result must come from a complete, compatible semantic
generation. A non-ready, stale, failed, or incompatible semantic lane returns a
typed unavailable error instead of silently serving baseline results:

```sh
"$TD_BIN" tool --project "$TD_PROJECT" search --args - --json <<'JSON'
{"query":"where is project identity resolved?","semantic_mode":"strict","limit":10,"format":"json"}
JSON
```

Strict mode checks semantic availability and generation compatibility. It does
not promise that every natural-language query has a relevant repository match.
The current exact-flat policy uses a maximum cosine distance of `2` and a
minimum margin of `0`, so it can return the nearest represented document even
when the repository does not contain the concept the query describes.

`tracedecay_context` accepts the same `semantic_mode` values for broader
relationship synthesis:

```sh
"$TD_BIN" tool --project "$TD_PROJECT" context --args - --json <<'JSON'
{"task":"explain how project identity reaches store selection","semantic_mode":"fallback","include_code":true,"format":"json"}
JSON
```

When a search response includes `next_cursor`, pass that exact opaque value to
the next request. Repeat the same query, semantic mode, and lexical controls;
only add the cursor. Changing modes between pages is rejected.

```sh
"$TD_BIN" tool --project "$TD_PROJECT" search --args - --json <<'JSON'
{"query":"where is project identity resolved?","semantic_mode":"strict","limit":10,"cursor":"<next_cursor from the previous response>","format":"json"}
JSON
```

Disable the lane when it is no longer wanted, then restart the daemon so the
mounted semantic runtime retires:

```sh
"$TD_BIN" semantic disable --path "$TD_PROJECT"
```
