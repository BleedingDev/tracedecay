# Semantic diagnosis: rn-semantic-diagnose

## Result

The observed result is blocked at setup and admission. It is not evidence of a
bad calibration formula.

The chain is:

1. The selected model is JinaEmbeddingsV2BaseCode, but the lifecycle is
   selected_not_downloaded with auto_download: false.
2. The verified artifact store has only .artifact-store.lock; its artifacts,
   receipts, and staging directories are empty.
3. LoadedSemanticArtifactV1::lifecycle_projection therefore returns Artifact,
   and production records semantic_projection_schedule
   outcome=artifact_unavailable. No successful semantic projection or vector
   generation exists.
4. Strict demand enters the model-demand hook, but the lifecycle owner refuses
   demand acquisition when auto_download is false. No acquisition worker
   reaches download or verification.
5. The scoped semantic query-authority lookup is empty. The strict request
   consequently returns typed unavailability with the inner
   CalibrationUnavailable/calibration_unavailable reason before the semantic
   lane can retrieve vectors or evaluate calibration.
6. There is no calibration measurement, activation receipt, mounted semantic
   authority, restart proof, or true semantic contribution in this run.

The first correction is to provision the exact real fixture in a fresh
isolated profile, select it with automatic acquisition enabled, wait for
Installed/Ready, run explicit semantic activation, then restart the same
isolated composition and prove a semantic contribution. A separate bounded
production cleanup seam remains after that setup correction.

Diagnostic source state:

- accepted start: f6d9bdf7073de9a25994b71e638fc1ff6013e4b0
- source inspected: 9c36fe6d62d25b47c39e4c78723e252e4cb1b597
- branch/worktree: feat/pluggable-memory-providers-v2 in the assigned
  .worktrees/pluggable-memory-providers-v2
- the later source-head movement is a peer commit; the scoped product source
  paths used below have no diff between the accepted start and inspected head
- no product implementation path was edited by this diagnosis
- no Cargo command, candidate binary, daemon, or service was run for this
  diagnosis

## Existing evidence and inventory

The archived smoke output at
target/test-profile/branch-smoke-hn1komyc is useful only as historical
pre-acquisition evidence:

- index-result.json reached a fresh exact result for branch_smoke, while
  semantic coverage remained unavailable (calibration_unavailable).
- index-daemon.log:27-28 records
  semantic_projection_schedule outcome=artifact_unavailable for the code
  generation. The earlier scheduler_unavailable line is a code-index
  scheduling condition, not a successful semantic projection.
- the lifecycle record at
  target/test-profile/branch-smoke-hn1komyc/profile/semantic-models/lifecycle.json
  has:

      selected_model: JinaEmbeddingsV2BaseCode
      auto_download: false
      state.state: selected_not_downloaded
      state.revision: 516f4baf13dec4ddddda8631e019b5737c8bc250
      state.artifact_digest: 70be81163e9740d742b7857e132713b323b5042d661485354d781cb8313c15af
      previous_ready: null

- result.json identifies an old binary at the same target/debug/tracedecay
  path and head 276ded8fa; it has no source provenance for the inspected
  candidate. Its archived tool project_list also reports that the source
  project was not enrolled. It is not a current acceptance run and was not
  executed or reused.

A read-only scan of the moving target/test-profile found hundreds of Jina
lifecycle records. The parent inventory counted 331; a repeat scan while peer
jobs were still writing isolated outputs counted 329. Every record in the
repeat scan was selected_not_downloaded. The count is timing-dependent; the
invariant is that no record reached a downloaded or installed state. Every
inspected semantic-models/verified-artifacts store contained only the lock
file plus empty subdirectories.

The checked-in manifest is metadata, not a payload. It pins:

- Jina revision 516f4baf13dec4ddddda8631e019b5737c8bc250
- 768 dimensions and an 8192-token maximum
- model.onnx length 641,517,466 and SHA-256
  63363fc178428b74620c6f3780cbc7191883fa5c7f84c0945c45eb5c4256733b
- tokenizer/config/special-token member lengths and SHA-256 values in
  tests/distribution/fastembed/fixture.json
- upstream https://huggingface.co/jinaai/jina-embeddings-v2-base-code,
  Apache-2.0

The five payload members are absent from the checkout and from the inspected
Hugging Face cache. The only ordinary user-cache model found was the
476 MB sentence-transformers/paraphrase-multilingual-MiniLM-L12-v2 tree.
The separately inventoried 465 MB NCM MiniLM tree is also ineligible: neither
model is the cataloged Jina FastEmbed artifact and neither can prove strict
Jina semantic acceptance. No cache was changed and no download was attempted.

## Transition trace

| Transition | Source guard and evidence | Result here |
| --- | --- | --- |
| Strict demand | crates/tracedecay-code-index-runtime/src/code_index_scheduler/semantic_query_runtime.rs:395-405 calls enqueue_demand_acquisition_if_needed before canonical generation lookup. owner.rs:831-852 requires auto_download and SelectedNotDownloaded/retryable Failed. | The recorded profile has auto_download: false, so the demand hook returns false; no worker is started. |
| Acquisition | owner.rs:1197-1285 single-flights a worker; acquisition.rs:16-71 runs and records acquisition. | Not reached. There is no worker receipt, Downloading progression, or source fetch in the observed profile. |
| Verification | acquisition.rs:161-198 verifies every staged member length and SHA-256 before install; acquisition.rs:204-258 imports and leases a shared verified artifact, or :260-306 atomically installs a private one. | Not reached because no five-member fixture exists. |
| Projection | production.rs:1841-1868 calls LoadedSemanticArtifactV1::lifecycle_projection. On Artifact, it logs artifact_unavailable and supplies a loader that returns the same error; :2182-2193 leaves lifecycle progress untouched when scheduling is refused. | The archived log proves refusal before a successful projection. No vector generation was published. |
| Calibration | acceptance_calibration.rs:118-166 measures deterministic background distances only from a vector map; fewer than 32 vectors or 256 valid pairs is uncalibrated. service.rs:317-325 and :466-478 preflight a committed profile against projection, vector-generation, and capability identities. | No vector map or committed calibration profile exists. The missing authority return occurs earlier than this formula/preflight path. |
| Activation receipt | coordinator.rs:170-226 stages configuration, requires a result active semantic vector generation, builds SemanticActivationRequestV1, and calls the owner. config_backend.rs:229-287 validates generations, issues SemanticActivationReceiptV1, and commits the linked transition. | Not reached; there is no active vector generation or receipt. |
| Authority mount | semantic_query_runtime.rs:243-293 reads committed scoped configuration and the serving query authority; :295-323 validates checkout identity before mounting; :351-364 returns only a unique mounted semantic authority. | No committed activation pair is mounted. execute_query_with_semantic returns CalibrationUnavailable at :411-424 when this lookup is empty. A code-index authority log line does not establish a semantic query authority. |
| Restart | Production open calls mount_current_semantic_query_authority_on_project_open; the complete journey test reopens the composition and checks the receipt generation. | Not run: there is no current binary/fixture and no isolated dynamic acceptance execution. |
| True semantic evidence | production.rs:2309-2400 requires exact code/projection/capability identities and a retained vector generation before calling the calibrated service. The shipped CLI test requires semantic.status == complete and a candidate contribution with retriever == semantic (semantic_activation_test.rs:424-452). | Not produced. The old smoke only answered from exact/lexical/graph lanes and never entered vector retrieval. |

The public setup has no standalone semantic select, semantic acquire, or
semantic verify command. The supported route is:

1. create a temporary home/profile and a temporary authenticated Git project;
2. run init;
3. read configuration_observed_state;
4. submit the CAS configuration_set request for
   SEMANTIC_RUNTIME_SETTING_KEY with SemanticConfig.selected_model =
   JinaEmbeddingsV2BaseCode, as implemented by
   semantic_activation_test.rs:131-167;
5. verify through tool ... runtime --json that lifecycle acquisition moves
   through selected_not_downloaded, downloading, verifying, installed,
   loading, indexing, and ready;
6. issue semantic activate --profile hybrid-conservative --project ... --json;
7. require the receipt digests and activated generation, then require runtime
   state == ready;
8. issue strict search and require semantic.status == complete plus a real
   semantic candidate contribution;
9. shut down and reopen the same isolated composition, require the same
   activated generation, and repeat the strict contribution assertion.

The test fixture helper first copies only the byte-pinned members into an
isolated Hugging Face cache, then uses the lifecycle owner to select with
auto_download: true and acquire through the verified store. It does not
substitute the NCM model or synthetic ONNX bytes.

## Reason names at the two response layers

semantic_query_runtime.rs:418-424 emits the typed
SemanticAbstentionV1::CalibrationUnavailable reason when the semantic
authority is absent. The production search payload therefore carries inner
semantic.reason = "calibration_unavailable" while the strict result remains
status = "unavailable". The current
semantic_index_fixture_check_test.rs:253-263 asserts that pair.

semantic_availability_journey_test.rs:421-431 still asserts the outer
reason = "semantic_unavailable" for the same typed strict refusal. These are
different serialization layers and existing tests must be interpreted
accordingly. Preserve both names in evidence; neither name proves that the
calibration statistic was computed and rejected.

## One bounded production seam

The responsible code seam is retained terminal acquisition-worker outcomes:

- AcquisitionWorkerStateV1::join_and_retain in
  crates/tracedecay-semantic/src/model_lifecycle/owner.rs:200-220 stores any
  join error in worker.outcome; reap_finished removes the handle and retains
  the error.
- spawn_acquire at owner.rs:1197-1204 returns false whenever worker.outcome
  is already set, and also returns false if reaping a finished worker returns
  an error.
- cancel_and_join_background_acquisition and
  cancel_and_join_background_acquisition_until at owner.rs:922-955
  repeatedly return the retained error.
- join_acquisition_worker at owner.rs:1333-1344 turns a panic into
  WorkerJoinFailed; cancellation cleanup quarantine/failure is also returned,
  while ordinary Cancelled maps to a clean cancellation.
- select_model at owner.rs:673-676 cancels and ignores a reap error, leaving a
  retained outcome available to block later demand.
- resolve_background_acquisition_outcome at owner.rs:910-918 is the only
  drain, and repository search finds its only caller in the test
  tests_first.rs:692-737. There is no production caller.

The adjacent test uses PanickingFixtureSource, observes two repeated
WorkerJoinFailed shutdown results, explicitly drains the outcome, and then
gets Ok(true). In production, a reaped panic can leave durable lifecycle
state at Downloading while the retained outcome makes every future
strict-demand spawn return false. The same permanent block applies to
CancellationCleanupQuarantined/CancellationCleanupFailed until their
terminal cleanup state is resolved.

The bounded fix case is therefore: add one production-owned recovery decision
at the lifecycle retry/demand boundary that observes and resolves a terminal
worker outcome after the handle is reaped, while preserving the typed error
until that decision and never clearing an outcome for a still-running worker.
The smoke failure itself predates this seam: its fixture is absent and its
selection disables automatic demand. The seam must be tested separately with
the worker fault; it should not be used to explain the current
ArtifactUnavailable/missing-authority observation.

## Isolated build and runtime ticket

No current candidate binary or verified Jina fixture is available in this
worktree. The following commands are the exact supported ticket for the
designated build/runtime owner. They have not been run here.

Use a clean candidate checkout at the accepted source revision (the
distribution gate rejects tracked or untracked source drift), and keep all
fixture/profile outputs in isolated scratch space:

    repo=/path/to/clean/tracedecay
    run="$repo/target/test-profile/readiness/semantic/diagnose/<run-id>"
    mkdir -p "$run"

Validate the manifest without downloading, then prepare the real immutable
fixture into the run directory. An optional
TRACEDECAY_DISTRIBUTION_FASTEMBED_CACHE may point only to a separately
provided cache whose members pass the same length/SHA checks; no such cache
was found here.

    python3 "$repo/tests/distribution/fastembed/prepare_fixture.py" \
      --check "$repo/tests/distribution/fastembed"
    python3 "$repo/tests/distribution/fastembed/prepare_fixture.py" \
      "$repo/tests/distribution/fastembed" "$run/fastembed"

Build the production CLI in the repo-local target directory:

    cargo build \
      --manifest-path "$repo/crates/tracedecay-cli/Cargo.toml" \
      --release --no-default-features --features production --bin tracedecay

Verify the built binary's embedded source identity before using it:

    "$repo/target/release/tracedecay" --version

The reported version must carry the clean candidate source SHA. Then run the
ignored shipped-CLI journey with the isolated fixture; the test itself creates
an isolated home/project, seeds the exact fixture, starts the binary with an
isolated socket, selects via configuration CAS, waits for lifecycle readiness,
activates, and proves strict semantic contribution:

    TRACEDECAY_TEST_BIN="$repo/target/release/tracedecay" \
    TRACEDECAY_DISTRIBUTION_FASTEMBED_FIXTURE="$run/fastembed" \
    TRACEDECAY_DISTRIBUTION_FASTEMBED_PROFILE_PARENT="$run/profile" \
    CARGO_NET_OFFLINE=true HF_HUB_OFFLINE=1 \
    cargo nextest run \
      --manifest-path "$repo/crates/tracedecay-cli/Cargo.toml" \
      --release --no-default-features --features production \
      --test core_cli_suite --run-ignored all \
      -E 'test(=semantic_activation_test::shipped_cli_activates_a_published_profile_for_strict_semantic_search)'

Run the production composition journey separately to cover shutdown/reopen,
authority restoration, and exact retry/rollback evidence:

    TRACEDECAY_DISTRIBUTION_FASTEMBED_FIXTURE="$run/fastembed" \
    TRACEDECAY_DISTRIBUTION_FASTEMBED_PROFILE_PARENT="$run/activation-profile" \
    CARGO_NET_OFFLINE=true HF_HUB_OFFLINE=1 \
    cargo nextest run \
      --manifest-path "$repo/crates/tracedecay/Cargo.toml" \
      --release --no-default-features --features production --lib \
      -E 'test(~semantic_activation_journey_test::public_semantic_activation_rollback_and_exact_retry_preserve_graph_authority)'

The repository's full supported gate,
scripts/check-distribution-acceptance.sh --repo "$repo", performs the same
fixture preparation, source-identity checks, packaging, semantic lifecycle
acceptance, activation/recovery journey, and shipped CLI journey. It is the
preferred ticket once the candidate checkout is clean. The accepted gate runs
the semantic legs with CARGO_NET_OFFLINE=true and HF_HUB_OFFLINE=1 after
fixture preparation, so runtime acquisition cannot silently download.

## Negative controls and acceptance boundaries

- neg.semantic_unavailable_strict: empty/unactivated authority must return
  typed strict unavailability, retain the canonical exact/lexical/graph result
  where policy allows, and perform no vector query.
- neg.semantic_artifact_fault: missing, corrupt, wrong-length, or mismatched
  member must stop at verification; it must not publish Installed/Ready,
  project vectors, issue a receipt, or mount authority.
- neg.semantic_foreign_generation: a vector/source/projection/capability
  identity mismatch must be rejected as incompatible and cannot be presented
  as a semantic hit.
- neg.installed_no_authority: even an installed/ready model without a
  committed mounted authority remains strict-unavailable with the inner
  calibration_unavailable reason; it must not call the lane.
- neg.cancel_no_success: ordinary cancellation must not publish a partial
  install; cleanup quarantine/failure remains a typed terminal outcome.
- neg.single_flight: repeated strict requests while acquisition is pending
  may queue demand but must not create more than one acquisition worker.
- neg.semantic_cursor: a continuation tied to a stale generation must be
  rejected rather than querying a foreign vector generation.
- neg.lexical_ablation: after true activation, a zero-token-overlap target
  must be found through strict semantic evidence and absent from the lexical
  ablation control. A ready state or fallback-only answer is insufficient.

## True-serving oracle from the active checkout

The smallest checked-in semantic candidate is
tests/fixtures/search_quality/query-semantic-candidate-workload-v1.json:735-751:

- query id: validation-015
- query: what stops an old login token from authorizing another request
- allowed scope: context_eval
- target anchor: context_eval::auth_session::validate_session
- target literal: pub fn validate_session

The target implementation is the session-expiry check in
tests/fixtures/context_eval_project/src/auth/session.rs. The frozen lexical
baseline is known to miss validation-015 at the target symbol/anchor level
(the search-evaluation regression expects validation-015 among the failed
queries at crates/tracedecay-search-eval/src/report_tests.rs:84-91). A likely
lexical distractor is the authentication/login path, including
auth::login::authenticate, with nearby session construction and token symbols
also sharing query words.

This is a semantic candidate oracle, not a zero-overlap lexical-ablation
case. The query's words such as login, token, and request occur elsewhere in
the fixture corpus, so whole-document lexical overlap is expected. The
claim to prove is target recovery through the semantic lane after lexical
baseline miss, with generation and evidence identity pinned.

Once the isolated provisioning ticket is unblocked, issue the strict MCP
search equivalent to the shipped CLI request:

    tracedecay_search({
      "query": "what stops an old login token from authorizing another request",
      "limit": 10,
      "format": "json",
      "semantic_mode": "strict_semantic"
    })

Require status unavailable before activation, then after activation require
semantic.status == complete, the target
context_eval::auth_session::validate_session in results, and a candidate
contribution whose retriever is semantic. Pair that public MCP result with the
retained exact-flat oracle row carrying CodeSemanticEvidenceV1 and the pinned
code-generation, projection, vector-generation, and capability identities.
Public MCP does not serialize that exact-flat evidence or all of those pinned
generation fields, so a semantic status and contribution alone are
insufficient evidence of the true serving route.

No dynamic run of validation-015 was possible in this diagnosis because the
current candidate binary and verified Jina fixture are absent. The
reproduce-semantic-states plan todo remains pending.

## Plan status

- reproduce-semantic-states: pending — no current candidate binary,
  materialized verified Jina fixture, isolated provisioned profile, restart,
  or true semantic result was available. The old smoke is pre-acquisition and
  has no current source identity.
- trace-semantic-activation: complete — the observed pre-acquisition chain
  and every downstream source guard through authority mount/restart/true
  evidence are recorded; the dynamic stages are explicitly blocked.
- freeze-semantic-fix-case: complete — the setup correction, one bounded
  retained-worker-outcome seam, exact runtime ticket, and negative controls are
  recorded before repair.
