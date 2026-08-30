# tdmem-0701 — licensed Biomem/NCM surface audit

Audit target: `bleedingDev/biomem` at exact revision
`500847ff65b5d9548b3826fa29bf3ccf8d221147`, package version `0.0.2`, MIT
license, evaluated against the TraceDecay memory-provider v1 contract on Darwin
arm64 with Python 3.12.8. The source revision, host platform, and Python version
were measured rather than inferred. [P1] [P4]

The audit is deliberately about callable behavior. It does not treat package,
endpoint, or artifact presence as capability proof. The probe exercised source
syntax, the real HTTP fallback implementation, and actual model-backed
`TextMemory` operations against isolated temporary state. It used a
caller-supplied populated model cache with offline flags and a non-loopback
network guard; it did not download a model, start the full daemon, touch
production state, run Cargo, or build the repository. [P2] [P3]

## Decision

The licensed surface is useful, but it is **not production-ready as a mandatory
TraceDecay provider**:

| Mandatory operation | Classification | Usable primitive | Decisive result | Evidence |
|---|---|---|---|---|
| `provider.health.v1` | **adaptable** | HTTP `GET /api/health` plus `status` | Product/version/protocol/process-state identity is real. Readiness must remain fail-closed until loaded-state and implementation/state identity digests bind immutable build, effective configuration, schema/generation, recovery, and exact scope. | [H1] [H2] [S2] [P3] |
| `observation.accept.v1` | **blocking** | `store_record` | A retry reinforces memory again; there is no durable idempotency ledger, payload-conflict check, server cancellation, effect reconciliation, or crash-safe commit. Mandatory observation conformance cannot be supplied by envelope translation alone. | [O1] [O2] [O3] [S1] [P3] |
| `recall.query.v1` | **adaptable** | `search`, explicitly **not** `retrieve` | `search` is bounded and returns stable IDs, native scores, content, and provenance without recall-stat mutation. The adapter must add exact scope, validity, provenance state, exclusions, coverage, deterministic ties, terminal/deadline mapping, and response budgets. | [R1] [R2] [R3] |

The overall production gate is therefore **blocked** on four irreducible
conditions: verified state readiness, exact-scope isolation, server-side
cancellation/effect reconciliation, and crash-safe persistence. Durable
observation deduplication is part of the effect-reconciliation blocker, not an
optional optimization. [H1] [O1] [S1] [S2] [P3]

Here, **adaptable** identifies a genuine primitive whose semantics can be kept:
it does not mean production admission is adapter-only. Health still requires a
Biomem surface addition for loaded-state and build/config/state identity
evidence before the adapter may report ready. [H1] [H2] [S2]

The machine-readable companion is
[`tdmem-0701-capability-matrix.json`](tdmem-0701-capability-matrix.json). It
classifies all 15 registry capabilities exactly once and separates adapter work
from changes required in Biomem.

## Mandatory operation analysis

### Health — adaptable, but readiness is not yet trustworthy

`HTTPFallbackServer._handle_quick_status` invokes `status` and sets `ready=true`
whenever the returned object has `status=success`. `LocalDaemonClient.health`
then validates `product=biomem`, non-empty package version, HTTP protocol v1,
`transport=http`, and `ready=true`. These are valid transport/product identity
checks. [H2] [H3]

They are not the provider health contract. `CommandHandler._handle_status`
reports security state, session count, and best-effort memory statistics; it
does not report a provider instance, immutable build/config identity, state
namespace/schema/generation, exact-scope digest, persistence/recovery state,
capacity state, or negotiated capability/limit state. [H1] [H4]

The model-backed probe independently invoked `load` against a fresh isolated
state path. It returned `None`, found no pre-existing state, and exposed none
of the five expected implementation/config/schema/generation/loaded-state
identity fields among its 12 statistics keys. This is measured core behavior,
not an inference from HTTP readiness. [P3]

The gap is safety-critical because `TextMemory._load_impl` catches every load
exception and only logs it. The already-created empty centers remain live.
`_handle_status` independently returns success and even sets
`memory_stored=true` if `get_stats` works, so a corrupt/incompatible state file
can look ready. Readiness must fail closed until the licensed boundary exposes
an explicit loaded-state result and the adapter can bind it to build,
configuration, schema, generation, recovery, and scope identity. [S2] [H4]
[H1]

### Observe — blocking for mandatory conformance

The positive primitive is concrete: `store_record` validates bounded key/value,
optional caller `memory_id`, limited provenance, intensity/surprise/age, then
serializes `TextMemory.store_record` plus `save` under the command lock. It
returns an authoritative stable `memory_id` and `created` or `reinforced`
outcome. [O2]

That stable ID is not idempotency. `MemoryCenters.write` treats the same
`memory_id` plus the same key as an instruction to reinforce the center: it
updates intensity, value/emotion vectors, canonical value text, provenance,
and returns `reinforced`. A same-key retry therefore creates another semantic
effect instead of `duplicate_acknowledged`. A reused ID with a different key is
rejected, but there is no same-id/same-canonical-payload ledger or
same-id/different-payload conflict test. [O3]

The actual core retry probe made two identical `store_record` calls. Both
completed, both returned index `0`, one matching stable ID remained, and the
write counter advanced `0 -> 1 -> 2`; neither result exposed a receipt or
idempotency field. The measured 51,193 ms run therefore confirms reinforcement
on retry rather than durable duplicate acknowledgement. [P3]

Durability also cannot be reconciled. The center is mutated before
`memory.save()`. If save fails, `store_record` returns `SAVE_FAILED`, but the
RAM effect still exists and may later be persisted by another save or shutdown.
The client collapses `SAVE_FAILED`, `WRITE_ERROR`, and `DUPLICATE_MEMORY_ID` to
`INTERNAL_ERROR` because they are outside its exposed error set. Neither side
returns committed `none/applied/partial/unknown`, a state generation, or a
queryable operation identity. [O2] [E1] [S1]

The transport adds client timeouts but no provider deadline or live
cancellation. In the focused disconnect probe, the client timed out after
31 ms; the bounded synthetic handler later took its normal-return branch, and
the probe observed zero `CancelledError`s. That measurement applies only to
this synthetic transport request, not real `TextMemory`, model, persistence,
or mutation-effect behavior. Independently, the pinned `_submit_command`
source waits on the submitted future and has no socket-disconnect path to
`future.cancel()` or a provider cancellation signal. A timed-out
`store_record` therefore has an unknown effect and an unsafe blind retry.
[P3] [T1]

Mandatory observe therefore requires Biomem changes for persisted dedupe,
same-key conflict semantics, server-side operation identity/cancellation,
effect reconciliation, and crash-safe commit. The TraceDecay adapter separately
owns canonical envelope validation, exact-scope admission, privacy/provenance,
source sequencing, and typed terminal mapping. [O1] [O2] [O3] [P3]

### Recall — adaptable through `search`, not `retrieve`

`TextMemory.search` is protected by the core RLock, wraps query embedding in
`torch.no_grad`, returns at most `top_k`, and supplies content, stable
`memory_id`, native similarity, layer, provenance, age, usage, and intensity.
`CommandHandler._handle_search` bounds `top_k` to 50, removes the internal
center index, and labels the method administrative search without recall side
effects. [R2]

The model-backed recall probe requested eight results and returned one. That
single result had a stable ID, native score, and provenance, and the bounded
polarity was true. The result establishes the primitive's measured shape; it
does not supply contract scope, validity, coverage, tie, or terminal semantics.
[P3]

`retrieve` is not acceptable for provider recall: it writes the query into the
session cache and calls compound reads with `increment_stats=true`, which
increments read/usage state. Selecting `search` avoids changing licensed recall
semantics merely to fit the provider contract. [R3]

The adapter must still construct the provider response: exact admitted scope;
request-scoped candidate IDs; canonical content digests; current/future/
expired/superseded/revoked/unknown validity; source revision; explicit
available/redacted/unavailable provenance; exclusions; scanned/matched/
returned/excluded/truncated coverage; deterministic score ties; total/candidate
byte budgets; typed zero-result/partial/error terminals; and deadline/
cancellation mapping. Unknown validity or scope must be excluded, never guessed.
[R1]

This mapping is only sound after topology supplies one isolated opaque namespace
per exact TraceDecay scope. The current daemon has one configured state file and
does not accept scope on `search`. [S3] [R2]
[R1]

## Callable operation inventory

The callable layers are distinct; capability claims must say which one is used.

| Layer | Callable operations | Concurrency/bounds | Provider relevance | Evidence |
|---|---|---|---|---|
| MCP stdio | `biomem_status`, `biomem_store`, `biomem_retrieve`, `biomem_search`, `biomem_list`, `biomem_graph` | Pydantic-bounded arguments; search `top_k<=50`, list `limit<=100`, graph `max_nodes<=250` | Useful local client surface. It is stateless, owns no state file, and explicitly marks store non-idempotent. | [A1] |
| Loopback HTTP v1 | `GET /api/health`, `GET /api/status`, `POST /api` | Loopback-only; server body <=1 MiB; client request <=1 MiB, response <=4 MiB; connect 2 s/read 30 s | Real topology candidate, but timeout is client-side and not end-to-end provider control. | [H2] [H3] [T1] |
| `CommandHandler` | `retrieve`, `store`, `store_record`, `search`, `list_memories`, `ollama_chat`, `backup`, `restore`, activation/suspension/clear/config/status, projections, center get/update/delete, `batch_import`, export/report/refactor | Selected request fields bounded; memory operations commonly serialized through one asyncio lock; no provider deadline/cancellation envelope | The complete daemon command catalog. Optional provider capabilities must not be inferred from similarly named commands. | [A2] |
| `TextMemory` | `store`, `store_record`, `recall`, `search`, `edit`, `forget`, `step`, consolidate, `save`, `load`, `reset`, migration/stats/list/backup/restore/refactor | Fixed configured capacities and a process-local RLock on most public read/write methods | Core cognitive primitives; direct use still lacks provider identity, scope, idempotency, terminals, and live control. | [O4] [R2] [M1] [S2] |

Optional capability conclusions follow from those actual operations:

- `recall.associative.v1` is adaptable from semantic `search`; it cannot be
  declared until the scoped recall envelope and native score domain are bound.
  [R1] [R2]
- Maintenance, correction, deletion, snapshot export/restore, replay, and
  inspection have related administrative/core primitives but are blocking as
  complete provider capabilities. Their current operations omit mandatory
  scope, generation, idempotency, live control, partial/resume, verified
  postcondition, or redaction/coverage semantics. [C1] [L1] [M1] [I1]
- Feedback, temporal recall, explicit facts, and explain trace are unsupported;
  neither the exhaustive command map nor the six MCP tools expose their
  contract semantics. They must not be declared based on product names or
  key/value/provenance fields. [A1] [A2]

## Persistence, compatibility, and corruption

New `.bdbm` files are `BDBMZIP01` plus an unencrypted ZIP containing
`vectors.pt` and `metadata.json`. The metadata contains key/value text, stable
IDs, provenance, statistics, and version. The format is portable; legacy
`BDBMENC01` input is decrypted with a machine fingerprint, and a raw ZIP is
accepted with a warning. [S1]

Legacy `.pt` loading runs `migrate_state`, attempts a best-effort pre-migration
backup, and overwrites the version marker with `1.0`. `.bdbm` loading does not
run that migration or reject a mismatched version. `_apply_state` applies
present sections selectively, so missing schema sections are not rejected as an
incompatible whole. [S1] [S2]

Corruption behavior is fail-open at the memory object: `BDBMContainer.load`
raises on unknown format, malformed ZIP, missing vectors, bad JSON, or failed
legacy decryption; `TextMemory._load_impl` catches the exception, logs it, and
continues with initialized state. Restore calls the same swallowed loader and
can return to the handler without proof that compatible state was applied.
Implicit reset/empty-state continuation is forbidden for provider readiness.
[H1] [S1] [S2] [L1]

The model-backed incompatible-restore probe supplied a truncated non-BDBM
payload. `restore` did not raise, and the five in-memory IDs remained unchanged
before and after. This confirms the swallowed failure at the callable boundary.
The interrupted-save sub-observation remains explicitly blocked because the
revision exposes no deterministic interruption hook; no process-kill result was
fabricated. [P3]

Writes are not crash-safe. `BDBMContainer.save` builds the complete blob in
memory and calls `Path(path).write_bytes(blob)` on the final path. There is no
temporary sibling, atomic rename, file `fsync`, or directory `fsync`. A process
crash can truncate or replace the target state file. [S1]

State identity is also incomplete: `TextMemory.STATE_VERSION` and a filesystem
path exist, but provider owner, state namespace, scope digest, immutable build/
configuration identity, and monotonic generation do not. One daemon resolves
one state file from CLI/config; the path is not valid scope authority. [S2]
[S3] [R1]

The portable format exposes text/provenance in unencrypted ZIP metadata. That
does not itself violate the local product, but a TraceDecay provider must reject
unadmitted secrets/personal data and enforce classification, retention,
redaction revision, forget-source key, and expiry before dispatch. [S1] [O1]

## Lifecycle, threading, deadlines, and cancellation

Startup constructs `TextMemory` and attempts auto-load before the server starts;
the application then forces lazy embedder model access. [S2] [S3] The server
starts an HTTP fallback thread and WebSocket listener around one
memory/handler/session cache. [L2] [T1] Shutdown closes connections/listener and
attempts a final save, but logs save failure and continues. [L2]

The actual HTTP transport overlapped bounded synthetic handler coroutines. On
the audited host, those handlers reached `max_active` 1, 2, 4, and 8 at
matching request levels with zero errors. That is transport evidence only: in
production, worker threads submit commands to one daemon event loop;
store/search/list and several maintenance operations then serialize under
`CommandHandler.lock`. [P3] [T1] [A2]

Separately, the model-backed core probe completed all actual `TextMemory`
read/write calls with zero errors at caller levels 1, 2, 4, and 8. Maximum
callers in flight were 1/2/3/8 for reads and 1/2/4/8 for writes. These are
measured caller-overlap values, not proof that locked model or persistence work
executes simultaneously. [P3]

Core `TextMemory` methods generally use `threading.RLock`, but the daemon's
`update_center` and `delete_center` handlers dispatch direct center access with
`asyncio.to_thread` outside `CommandHandler.lock` and `TextMemory._lock`.
`update_center` targets singular `key_text`/`value_text` attributes absent from
the center layout; `delete_center` changes `active`/`h` but its singular text
guards do not clear the actual plural text arrays. Thread safety and successful
mutation are therefore not blanket properties of the command surface. [C1]

`LocalDaemonClient` has bounded HTTP connect/read/write/pool timeouts and maps
HTTP timeouts to `DEADLINE_EXCEEDED`. It first performs a health request and
then a command request, each with its own timeout; no single monotonic remaining
budget reaches both steps or the provider loop. [H3]

Client task cancellation is not the missing guarantee. The daemon protocol has
no operation ID, absolute/remaining deadline, live cancellation token, cancel
command, or outcome query. The measured disconnect established a bounded client
timeout, later normal return by the synthetic handler, and zero observed
`CancelledError`s for that request. Separately, the pinned source shows that
the HTTP bridge never connects client disconnect to submitted-future
cancellation, so a bounded client wait does not bound provider mutation.
[P3] [T1] [A2]

The actual core cancellation observation was read-only. Its caller wait timed
out, the operation then settled normally and returned five results after 36 ms,
and it was not still running after the follow-up wait. The callable exposed no
cancellation or deadline parameter, and the probe recorded provider
cancellation and committed effect as unknown/not applicable rather than
inventing cancellation evidence. [P3]

## Errors and inspectability

Known daemon validation failures use stable string codes. Unexpected handler
exceptions are logged and reduced to `INTERNAL_ERROR`. HTTP command-level
errors normally remain HTTP 200 with `status=error`; the local client exposes a
small safe allow-list and maps other codes to generic `INTERNAL_ERROR`. This is
safe against detail leakage but insufficient for typed provider terminals and
mutation-effect truth. [E1] [E2] [T1]

Existing inspection primitives are useful but incomplete:

- `status` returns security state, active sessions, and best-effort stats. [H4]
- `search` returns bounded stable-ID results without internal center indices.
  [R2]
- `list_memories` returns deterministic stable-ID pages, but first asks the core
  for up to 1,000,000 records and has no byte budget or generation-bound cursor.
  [I1]
- `get_memory_graph` caps selected nodes at 250 and reports truncation. [I1]

There is no provider inspection view for delivery outcome, source influence,
state generation, recovery, snapshot identity, or capability status, and no
contract redaction/coverage envelope. `inspection.read.v1` must remain
undeclared. [I1] [A2]

## Platform evidence

The package declares Python `>=3.10`, macOS/Windows/Linux classifiers, and
Python 3.10–3.14 classifiers. Its Darwin arm64 dependency branch uses
`numpy>=1.26` without the Intel `<2` cap. [P4]

On Darwin 25.6.0 arm64 / Python 3.12.8:

- the source checkout resolved to the exact target revision; [P1]
- all 33 `.py` files under its `src/` tree compiled via `compile(..., "exec")`
  with zero syntax errors in 469 ms; [P2]
- the actual `HTTPFallbackServer` transport, driven by a bounded synthetic
  status handler, returned HTTP 200 with `product=biomem`, version `0.0.2`,
  protocol `1`, transport `http`, and `ready=true` while exposing no loaded-state
  identity fields; [P3]
- synthetic HTTP levels 1/2/4/8 completed 1/2/4/8 requests with zero errors,
  matching maximum handler overlap, in 81/79/81/83 ms; [P3]
- the same probe measured a 31 ms client timeout,
  followed by one normal handler return and zero observed `CancelledError`s;
  the server completed after disconnect during the 170 ms follow-up wait; [P3]
- all eight actual-`TextMemory` envelopes were measured: load identity remained
  incomplete, retry wrote twice, recall was bounded, read/write caller matrices
  completed without errors, the read-only operation outlived its caller wait,
  isolated state paths did not leak the admitted ID, save/restart preserved all
  five IDs and the bounded recall product, and incompatible restore returned
  without raising while leaving those five IDs unchanged. [P3]

This is source, transport, and isolated model-backed core evidence. It is not a
full-daemon conformance result, and the measured gaps keep the production gate
blocked.

## Authority boundary

NCM/Biomem owns provider-local advisory cognitive memory only. It owns **no**
codebase navigation, Git/repository/worktree resolution, file or symbol truth,
TraceDecay storage, session truth, canonical facts, tools, approvals, prompt
authority, or final context assembly. The six MCP operations are memory-local
status/store/retrieve/search/list/graph operations and supply no basis for a
broader authority claim. [A1]

TraceDecay remains the sole authority for exact profile/project/repository/
worktree/branch/session scope, current code, admitted observations, canonical
facts, provider admission, context compilation, and externally visible coding
actions. Provider output is advisory and cannot silently mutate those
authorities. [O1] [R1]

## Production blockers and ownership

| Blocker ID | Owner | Exit condition | Evidence |
|---|---|---|---|
| `state-readiness` | Biomem | Health exposes verified load result plus build/config, state schema/generation, recovery, and scope-compatible identity; corrupt/incompatible state fails closed. | [H1] [H2] [H4] [S2] |
| `exact-scope-isolation` | Adapter | One opaque provider namespace is selected from the complete admitted TraceDecay scope; scope, privacy, provenance, validity, coverage, limits, and terminals map without widening authority. | [H1] [O1] [R1] [S3] [A1] |
| `server-cancellation-effect-reconciliation` | Biomem | Durable dedupe survives restart; same-key/different-payload conflicts; remaining budget/live cancel reaches the operation; timed-out mutations return or can later resolve `none/applied/partial/unknown`. | [O1] [O2] [O3] [H3] [T1] [P3] |
| `crash-safe-persistence` | Biomem | Mutation state commits via durable atomic replace, and corrupt/incompatible load cannot become ready. | [S1] [S2] |

## Evidence index

TraceDecay contract evidence:

- **[H1]** `product/contracts/memory-provider-v1/provider-lifecycle-contract.json`
  `/health` and invariants; mandatory health requires state/scope/persistence/
  recovery checks and forbids socket/process/non-empty-state readiness.
- **[O1]** `product/contracts/memory-provider-v1/provider-observation-contract.json`
  `/observation_envelope`, `/idempotency`, `/delivery_receipt`, and invariants.
- **[R1]** `product/contracts/memory-provider-v1/provider-recall-contract.json`
  `/recall_request`, `/provider_candidate`, `/recall_response`, `/coverage`,
  `/validity`, `/provenance`, and invariants.

Exact Biomem revision source evidence:

- **[A1]** [`src/memory_module/mcp_server.py:create_server`](https://github.com/bleedingDev/biomem/blob/500847ff65b5d9548b3826fa29bf3ccf8d221147/src/memory_module/mcp_server.py#L1-L186).
- **[A2]** [`src/memory_module/protocol.py:CommandHandler.__init__/handle`](https://github.com/bleedingDev/biomem/blob/500847ff65b5d9548b3826fa29bf3ccf8d221147/src/memory_module/protocol.py#L160-L264) and [`store/search/list handlers`](https://github.com/bleedingDev/biomem/blob/500847ff65b5d9548b3826fa29bf3ccf8d221147/src/memory_module/protocol.py#L507-L779).
- **[H2]** [`src/memory_module/http_fallback.py:_status_payload/_handle_quick_status/do_GET`](https://github.com/bleedingDev/biomem/blob/500847ff65b5d9548b3826fa29bf3ccf8d221147/src/memory_module/http_fallback.py#L227-L271).
- **[H3]** [`src/memory_module/local_daemon_client.py:limits/LocalDaemonClient`](https://github.com/bleedingDev/biomem/blob/500847ff65b5d9548b3826fa29bf3ccf8d221147/src/memory_module/local_daemon_client.py#L18-L263).
- **[H4]** [`src/memory_module/protocol.py:_handle_status`](https://github.com/bleedingDev/biomem/blob/500847ff65b5d9548b3826fa29bf3ccf8d221147/src/memory_module/protocol.py#L1243-L1261).
- **[O2]** [`src/memory_module/protocol.py:_handle_store/_handle_store_record`](https://github.com/bleedingDev/biomem/blob/500847ff65b5d9548b3826fa29bf3ccf8d221147/src/memory_module/protocol.py#L507-L684).
- **[O3]** [`src/memory_module/memory_centers.py:MemoryCenters.write`](https://github.com/bleedingDev/biomem/blob/500847ff65b5d9548b3826fa29bf3ccf8d221147/src/memory_module/memory_centers.py#L445-L634).
- **[O4]** [`src/memory_module/text_memory.py:__init__ RLock`](https://github.com/bleedingDev/biomem/blob/500847ff65b5d9548b3826fa29bf3ccf8d221147/src/memory_module/text_memory.py#L186-L187) and [`store/store_record/_store_impl`](https://github.com/bleedingDev/biomem/blob/500847ff65b5d9548b3826fa29bf3ccf8d221147/src/memory_module/text_memory.py#L221-L396).
- **[R2]** [`src/memory_module/protocol.py:_normalise_record`](https://github.com/bleedingDev/biomem/blob/500847ff65b5d9548b3826fa29bf3ccf8d221147/src/memory_module/protocol.py#L428-L465), [`_handle_search/_handle_list_memories`](https://github.com/bleedingDev/biomem/blob/500847ff65b5d9548b3826fa29bf3ccf8d221147/src/memory_module/protocol.py#L686-L779), and [`src/memory_module/text_memory.py:search`](https://github.com/bleedingDev/biomem/blob/500847ff65b5d9548b3826fa29bf3ccf8d221147/src/memory_module/text_memory.py#L523-L573).
- **[R3]** [`src/memory_module/protocol.py:_handle_retrieve`](https://github.com/bleedingDev/biomem/blob/500847ff65b5d9548b3826fa29bf3ccf8d221147/src/memory_module/protocol.py#L471-L505), [`_retrieve_memories`](https://github.com/bleedingDev/biomem/blob/500847ff65b5d9548b3826fa29bf3ccf8d221147/src/memory_module/protocol.py#L1131-L1169), [`src/memory_module/text_memory.py:recall/_recall_impl`](https://github.com/bleedingDev/biomem/blob/500847ff65b5d9548b3826fa29bf3ccf8d221147/src/memory_module/text_memory.py#L437-L518), and [`src/memory_module/memory_centers.py:read_compound/read_compound_records`](https://github.com/bleedingDev/biomem/blob/500847ff65b5d9548b3826fa29bf3ccf8d221147/src/memory_module/memory_centers.py#L304-L440).
- **[S1]** [`src/memory_module/bdbm_container.py:BDBMContainer`](https://github.com/bleedingDev/biomem/blob/500847ff65b5d9548b3826fa29bf3ccf8d221147/src/memory_module/bdbm_container.py#L32-L254).
- **[S2]** [`src/memory_module/text_memory.py:STATE_VERSION/__init__/auto-load`](https://github.com/bleedingDev/biomem/blob/500847ff65b5d9548b3826fa29bf3ccf8d221147/src/memory_module/text_memory.py#L67-L196) and [`save/load/_load_impl/_apply_state/migrate_state/restore`](https://github.com/bleedingDev/biomem/blob/500847ff65b5d9548b3826fa29bf3ccf8d221147/src/memory_module/text_memory.py#L693-L999).
- **[S3]** [`src/memory_module/main.py:_resolve_state_file/_run_background_server/_run_headless`](https://github.com/bleedingDev/biomem/blob/500847ff65b5d9548b3826fa29bf3ccf8d221147/src/memory_module/main.py#L136-L466).
- **[T1]** [`src/memory_module/http_fallback.py:limits/loopback checks`](https://github.com/bleedingDev/biomem/blob/500847ff65b5d9548b3826fa29bf3ccf8d221147/src/memory_module/http_fallback.py#L33-L76) and [`HTTPFallbackServer.start/_submit_command/ThreadingHTTPServer`](https://github.com/bleedingDev/biomem/blob/500847ff65b5d9548b3826fa29bf3ccf8d221147/src/memory_module/http_fallback.py#L110-L395).
- **[C1]** [`src/memory_module/protocol.py:_handle_update_center/_handle_delete_center`](https://github.com/bleedingDev/biomem/blob/500847ff65b5d9548b3826fa29bf3ccf8d221147/src/memory_module/protocol.py#L1510-L1565) and [`src/memory_module/memory_centers.py:center layout`](https://github.com/bleedingDev/biomem/blob/500847ff65b5d9548b3826fa29bf3ccf8d221147/src/memory_module/memory_centers.py#L91-L117).
- **[E1]** [`src/memory_module/local_daemon_client.py:_EXPOSED_DAEMON_CODES`](https://github.com/bleedingDev/biomem/blob/500847ff65b5d9548b3826fa29bf3ccf8d221147/src/memory_module/local_daemon_client.py#L25-L32) and [`_raise_daemon_error`](https://github.com/bleedingDev/biomem/blob/500847ff65b5d9548b3826fa29bf3ccf8d221147/src/memory_module/local_daemon_client.py#L243-L263).
- **[E2]** [`src/memory_module/protocol.py:error-code constants`](https://github.com/bleedingDev/biomem/blob/500847ff65b5d9548b3826fa29bf3ccf8d221147/src/memory_module/protocol.py#L60-L82) and [`handle/_error_response`](https://github.com/bleedingDev/biomem/blob/500847ff65b5d9548b3826fa29bf3ccf8d221147/src/memory_module/protocol.py#L209-L278).
- **[L1]** [`src/memory_module/protocol.py:backup/restore/export/refactor handlers`](https://github.com/bleedingDev/biomem/blob/500847ff65b5d9548b3826fa29bf3ccf8d221147/src/memory_module/protocol.py#L1373-L1459).
- **[L2]** [`src/memory_module/ws_server.py:BDBMServer`](https://github.com/bleedingDev/biomem/blob/500847ff65b5d9548b3826fa29bf3ccf8d221147/src/memory_module/ws_server.py#L82-L229).
- **[M1]** [`src/memory_module/text_memory.py:step/consolidate`](https://github.com/bleedingDev/biomem/blob/500847ff65b5d9548b3826fa29bf3ccf8d221147/src/memory_module/text_memory.py#L651-L688) and [`refactor`](https://github.com/bleedingDev/biomem/blob/500847ff65b5d9548b3826fa29bf3ccf8d221147/src/memory_module/text_memory.py#L1033-L1089).
- **[I1]** [`src/memory_module/protocol.py:list`](https://github.com/bleedingDev/biomem/blob/500847ff65b5d9548b3826fa29bf3ccf8d221147/src/memory_module/protocol.py#L717-L779), [`status`](https://github.com/bleedingDev/biomem/blob/500847ff65b5d9548b3826fa29bf3ccf8d221147/src/memory_module/protocol.py#L1243-L1261), and [`graph`](https://github.com/bleedingDev/biomem/blob/500847ff65b5d9548b3826fa29bf3ccf8d221147/src/memory_module/protocol.py#L1696-L1789).
- **[P4]** [`src/pyproject.toml:project metadata/dependencies`](https://github.com/bleedingDev/biomem/blob/500847ff65b5d9548b3826fa29bf3ccf8d221147/src/pyproject.toml#L5-L57).

Measured probes:

- **[P1]** `git -C <exact-revision-biomem-checkout> rev-parse HEAD`;
  `uname -srm`; `python3 --version`. These commands observed the exact
  revision `500847ff65b5d9548b3826fa29bf3ccf8d221147`,
  `Darwin 25.6.0 arm64`, and `Python 3.12.8`.
- **[P2]** `<selected-model-venv-python> -s
  scripts/product/probe-ncm-surface.py --source
  <exact-revision-biomem-checkout> --expected-revision
  500847ff65b5d9548b3826fa29bf3ccf8d221147 --core-mode auto --model-cache
  <caller-model-cache> --json` compiled all 33 `.py` files under the immutable
  archived `src/` tree with `compile(source, path, "exec")`: 33 checked, 0
  syntax errors, 469 ms.
- **[P3]** The same reusable v2 probe loaded the exact-revision
  `HTTPFallbackServer` with bounded synthetic handlers and ran actual
  model-backed `TextMemory` operations in isolated temporary state. The core
  child used the explicitly selected environment, the caller's populated model
  cache, offline dependency flags, and a non-loopback socket guard. HTTP health
  returned 200 and `ready=true` with no loaded-state identity fields; transport
  levels 1/2/4/8 completed without error; and the 31 ms client timeout was
  followed by one normal handler return, zero `CancelledError`s, and completion
  during the 170 ms follow-up wait. Absence of a server cancellation path is
  grounded separately in [T1]. All eight core envelopes were measured, while
  the nested interrupted-save observation remains explicitly blocked because
  no deterministic interruption hook exists. The exact typed values and
  13-measured/0-blocked/0-unsupported conservation summary are recorded under
  `evidence[id=probe-surface]` in the companion JSON.
