//! End-to-end V1 replacement boundary for the project-owned Native provider.
//!
//! The fixture is deliberately assembled from the released beta.37 SQL
//! contracts. The acceptance lane drives the shipped replacement coordinator
//! against that fixture, then reopens the published profile through the same
//! host composition used by the daemon. The external project checkout stays
//! outside the profile shard so the coordinator's project inventory and the
//! complete backup can be checked independently.

#![allow(clippy::expect_used, clippy::panic, clippy::unwrap_used)]

use std::collections::BTreeMap;
#[cfg(unix)]
use std::fmt::Display;
use std::fs;
use std::path::{Path, PathBuf};
#[cfg(unix)]
use std::process::{Command, Stdio};
use std::sync::Arc;
#[cfg(unix)]
use std::sync::Mutex as StdMutex;
#[cfg(unix)]
use std::time::{Duration, Instant};

use rusqlite::{Connection, OpenFlags, params};
use tempfile::TempDir;
use tracedecay_domain::ProjectId;
use tracedecay_maintenance::profile_backup::{
    ProfileBackupError, create_complete_profile_backup, load_and_verify_backup,
    rehearse_complete_profile_backup, set_rehearsal_publication_fault_for_test,
};
use tracedecay_project::project::{TraceDecay, TraceDecayOpenOptions};
use tracedecay_project::test_support::host_admission::HostAdmissionTestRuntimeV1;
use tracedecay_runtime_core::db::DatabaseAuthority;
use tracedecay_runtime_core::storage::{
    PROFILE_IDENTITY_RECORD_NAME, STORE_MANIFEST_FILENAME, read_store_manifest,
};

#[cfg(unix)]
use axum::body::{Body, to_bytes};
#[cfg(unix)]
use axum::http::{Request, StatusCode};
#[cfg(unix)]
use serde_json::{Value, json};
#[cfg(unix)]
use tower::ServiceExt;
#[cfg(unix)]
use tracedecay_contracts::retained_surfaces::{RetainedSurfaceOperation, RetainedSurfacePortsV1};
#[cfg(unix)]
use tracedecay_contracts::{
    CancellationSignal, CapabilityGrantId, CapabilityGrantSnapshot, Deadline, DisclosureClass,
    RequestId, ResolvedScope,
};
#[cfg(unix)]
use tracedecay_domain::{
    ActorId, BrainId, CanonicalMessageRoleV1, CanonicalObservationEnvelopeV1,
    CanonicalObservationEvidenceV1, CanonicalObservationFactV1, CanonicalObservationRelationsV1,
    ComponentVersion, DurableObservationV1, ManifestDigest, ObservationId,
    ObservationIdentityMaterialV1, ObservationOrderingDomainV1, ObservationScopeV1,
    ObservationSourceCursorV1, ObservationSourceGenerationV1, ObservationSourceIdentityV1,
    ObservationSourceRangeV1, PayloadReferenceV1, ProjectionGenerationId, ProviderId,
    RetentionClass, SanitizationReceiptId, SanitizationReceiptRefV1, SanitizationReceiptV1,
    SanitizerDispositionV1, SensitivityV1, SessionId, UserProfileId, UtcMicros,
};
#[cfg(unix)]
use tracedecay_memory_provider_ncm::{
    NcmCognitiveSurface, NcmProviderAdapter, RustNcmConfig, RustNcmSurface, RustNcmWorkerOwner,
    StateRoot, WorkerOptions,
};
#[cfg(unix)]
use tracedecay_memory_provider_registry::{
    ObservationInstanceProofV1, ObservationProviderMountV1, ObservationStateNamespacePolicyV1,
    ProviderExecutionShapeV1, ProviderLifecycleOwnerErrorV1, ProviderLifecycleOwnerV1,
    ProviderLifecycleOwnershipV1, ProviderRegistrationV1, RecallScopeBindingsV1,
};
#[cfg(unix)]
use tracedecay_runtime_core::cancellation::CancellationToken as HostCancellationToken;
#[cfg(unix)]
use tracedecay_sessions::admission::HostAdmissionScope;
#[cfg(unix)]
use tracedecay_store::{
    AnchoredObservationWrite, ObservationStore, ObservationWrite,
    build_observation_resolution_authorization_v1, build_observation_retrieval_anchor_v2,
};

const BRAIN_ID: &str = "brain.beta37-replacement";
const PROFILE_ID: &str = "profile.beta37-replacement";
const PROJECT_ID: &str = "project.beta37-replacement";
const TRANSCRIPT_TEXT: &str =
    "The beta.37 canonical session remains served after the V1-to-V2 replacement.";
const BETA37_NATIVE_PROVIDER_STATE_PATH: &str = "native-provider-state/beta37-memory.json";
const BETA37_NATIVE_MEMORY_ID: &str = "native-beta37-memory";
const BETA37_NATIVE_MEMORY_CONTENT: &str = "V1 Native state is inspection-only";
const BETA37_NATIVE_MEMORY_SOURCE: &str = "beta.37";
const BETA37_PROFILE_MEMORY_FACT_ID: &str = "fact.beta37.native-user-memory";
const BETA37_PROFILE_MEMORY_EVENT_ID: &str = "event.beta37.native-user-memory";
const BETA37_PROFILE_MEMORY_ASSERTION_ID: &str = "assertion.beta37.native-user-memory";
const BETA37_PROFILE_MEMORY_ANCHOR_ID: &str = "anchor.beta37.native-user-memory";
const BETA37_PROFILE_MEMORY_EVIDENCE_ID: &str = "evidence.beta37.native-user-memory";
const BETA37_LCM_EXTERNAL_PAYLOAD_REF: &str = "payload.beta37.external";
const BETA37_LCM_EXTERNAL_CONTENT: &str =
    "The beta.37 LCM payload remains linked to its durable external object.";

// Released configuration entries carry a complete typed snapshot entry rather
// than the pre-release scalar strings. These payloads keep the old provider
// selections admissible while exercising the current provider registry.
const BETA37_CONFIGURATION_NATIVE_ENABLED: &str = r#"{"schema_version":1,"value":{"kind":"boolean","value":true},"provenance":[{"layer":{"kind":"default"},"revision_id":"configuration.registry.default.v1","disposition":"defaulted","safe_reason":"registry_default"}]}"#;
const BETA37_CONFIGURATION_NCM_OBSERVER: &str = r#"{"schema_version":1,"value":{"kind":"text","value":"{\"mode\":\"disabled\"}"},"provenance":[{"layer":{"kind":"default"},"revision_id":"configuration.registry.default.v1","disposition":"defaulted","safe_reason":"registry_default"}]}"#;
const BETA37_CONFIGURATION_RECALL_ROUTING: &str = r#"{"schema_version":1,"value":{"kind":"text","value":"{\"active_provider\":\"tracedecay.native\",\"fallback\":null,\"degradation\":null}"},"provenance":[{"layer":{"kind":"default"},"revision_id":"configuration.registry.default.v1","disposition":"defaulted","safe_reason":"registry_default"}]}"#;

const SESSION_TEMPORAL_PROJECTION_RECEIPTS_V3_DDL: &str = "
    CREATE TABLE session_temporal_projection_receipts (
        session_id TEXT NOT NULL,
        generation INTEGER NOT NULL,
        batch_ordinal INTEGER NOT NULL CHECK(batch_ordinal >= 0),
        batch_digest TEXT NOT NULL,
        frozen_watermarks_json TEXT NOT NULL CHECK(json_valid(frozen_watermarks_json)),
        source_through INTEGER NOT NULL CHECK(source_through >= 0),
        projection_through INTEGER NOT NULL CHECK(projection_through >= 0),
        occurrence_count INTEGER NOT NULL CHECK(occurrence_count >= 0),
        occurrence_digest TEXT NOT NULL,
        dimension_count INTEGER NOT NULL CHECK(dimension_count >= 0),
        dimension_digest TEXT NOT NULL,
        copy_count INTEGER NOT NULL CHECK(copy_count >= 0),
        copy_digest TEXT NOT NULL,
        assertion_count INTEGER NOT NULL CHECK(assertion_count >= 0),
        assertion_digest TEXT NOT NULL,
        supersession_count INTEGER NOT NULL CHECK(supersession_count >= 0),
        supersession_digest TEXT NOT NULL,
        current_count INTEGER NOT NULL CHECK(current_count >= 0),
        current_digest TEXT NOT NULL,
        fts_count INTEGER NOT NULL CHECK(fts_count >= 0),
        fts_digest TEXT NOT NULL,
        committed_at INTEGER NOT NULL,
        PRIMARY KEY(session_id, generation, batch_ordinal),
        UNIQUE(session_id, generation, batch_digest),
        FOREIGN KEY(session_id, generation)
            REFERENCES session_temporal_generations(session_id, generation) ON DELETE CASCADE
    );";

const BETA37_LCM_OBJECTS: &[&str] = &[
    "idx_lcm_external_payloads_owner",
    "idx_lcm_maintenance_debt_kind",
    "idx_lcm_raw_session_id",
    "idx_lcm_raw_session_order",
    "idx_lcm_summary_nodes_codex_pending_root_order",
    "idx_lcm_summary_nodes_codex_pending_session_order",
    "idx_lcm_summary_nodes_session_depth_time",
    "idx_lcm_summary_sources_source",
    "lcm_external_payloads",
    "lcm_gc_marks",
    "lcm_gc_meta",
    "lcm_lifecycle_state",
    "lcm_maintenance_debt",
    "lcm_raw_messages",
    "lcm_raw_messages_fts",
    "lcm_raw_messages_fts_config",
    "lcm_raw_messages_fts_data",
    "lcm_raw_messages_fts_docsize",
    "lcm_raw_messages_fts_idx",
    "lcm_raw_messages_fts_delete",
    "lcm_raw_messages_fts_insert",
    "lcm_raw_messages_fts_update",
    "lcm_summary_nodes",
    "lcm_summary_nodes_fts",
    "lcm_summary_nodes_fts_config",
    "lcm_summary_nodes_fts_data",
    "lcm_summary_nodes_fts_docsize",
    "lcm_summary_nodes_fts_idx",
    "lcm_summary_nodes_fts_delete",
    "lcm_summary_nodes_fts_insert",
    "lcm_summary_nodes_fts_update",
    "lcm_summary_sources",
];

#[derive(Debug)]
struct Beta37V1ProfileFixture {
    temp: TempDir,
    profile: PathBuf,
    project_root: PathBuf,
    project_store: PathBuf,
}

#[derive(Debug, PartialEq, Eq)]
struct V1Preflight {
    project_user_version: i64,
    project_facts: i64,
    project_graph_replays: i64,
    native_provider_state: Vec<u8>,
    session_temporal_version: i64,
    lcm_version: i64,
    credential_references: i64,
    configuration_entries: i64,
    retained_session_messages: i64,
    lcm_raw_messages: i64,
    lcm_external_payloads: i64,
    workflow_runs: i64,
    workflow_agents: i64,
    git_publications: i64,
}

impl Beta37V1ProfileFixture {
    async fn create() -> Self {
        let temp = tempfile::tempdir().expect("beta.37 replacement fixture root");
        let profile = temp.path().join("v1-profile");
        let project_root = temp.path().join("v1-project");
        fs::create_dir_all(&profile).expect("create V1 profile root");
        fs::create_dir_all(&project_root).expect("create V1 project root");
        write_profile_identity(&profile);

        // Materialize every registered session authority through the same
        // composition root a daemon uses. The fixture then replaces only the
        // released authority-local artifacts with their beta.37 shape.
        let runtime = HostAdmissionTestRuntimeV1::project(
            &profile,
            &project_root,
            ProjectId::new(PROJECT_ID).expect("project identity"),
        )
        .await
        .expect("materialize final host-admission authorities");
        // Force the distinct profile-memory authority to be created by the
        // registered session runtime before the released beta.37 rows are
        // seeded. Opening `user-memory.db` directly would create an empty
        // SQLite file and would let the fixture bypass the production memory
        // schema/admission path entirely.
        runtime
            .session_registry_for_test()
            .profile_memory()
            .await
            .expect("materialize profile-memory authority");
        drop(runtime);
        tokio::task::yield_now().await;
        seed_beta37_profile_memory(&profile.join("user-memory.db"));

        fs::write(
            profile.join("enrollment.json"),
            format!(
                "{{\"schema\":\"beta.37\",\"brain_id\":\"{BRAIN_ID}\",\"profile_id\":\"{PROFILE_ID}\",\"project_id\":\"{PROJECT_ID}\",\"git_authority\":\"session-store\"}}"
            ),
        )
        .expect("write V1 enrollment");
        fs::write(
            profile.join("config.toml"),
            format!(
                "[profile]\nrelease = \"v0.1.0-beta.37\"\nactive_provider = \"tracedecay.native\"\ncredential_ref = \"credential.beta37\"\n\n[settings.native]\nenabled = true\nrecall_scope = \"exact_coding_scope\"\n"
            ),
        )
        .expect("write V1 configuration");
        fs::create_dir_all(profile.join("migration-inventory"))
            .expect("create migration inventory");
        fs::write(
            profile.join("migration-inventory/beta37.json"),
            br#"{"release":"v0.1.0-beta.37","project_store":"v34","configuration":"beta37","session_temporal":3,"lcm":8,"workflow":1,"git_correlation":5,"credential_references":["credential.beta37"]}"#,
        )
        .expect("write V1 migration inventory");

        let store_root = profile.join("projects").join(PROJECT_ID);
        let manifest = read_store_manifest(&store_root.join(STORE_MANIFEST_FILENAME))
            .expect("read materialized project manifest");
        assert_eq!(manifest.project_id.as_deref(), Some(PROJECT_ID));
        assert_eq!(manifest.data_root, store_root);
        let project_store = manifest.data_root.join(manifest.graph_db_relpath);
        let project_sessions_store = manifest.data_root.join(&manifest.sessions_db_relpath);
        replace_project_store_with_released_v34(&project_store);
        fs::create_dir_all(store_root.join("sessions")).expect("create transcript directory");
        fs::write(
            store_root.join("sessions/codex-beta37.jsonl"),
            format!("{{\"role\":\"assistant\",\"content\":{TRANSCRIPT_TEXT:?}}}\n"),
        )
        .expect("write released transcript source");
        fs::write(
            store_root.join("branch-meta.json"),
            br#"{"default_branch":"main","branch":"main","git_authority":"session-store"}"#,
        )
        .expect("write released branch metadata");
        fs::create_dir_all(project_root.join("src")).expect("create V1 code graph source");
        fs::write(
            project_root.join("src/native_provider.rs"),
            "pub struct NativeProvider;\n// beta.37 code graph source\n",
        )
        .expect("write V1 code graph source");

        // The production project scope resolver obtains the branch from the
        // checkout itself.  Give the released fixture the same committed
        // `main` route that a beta.37 service would have served; a bare git
        // identity marker is not enough for canonical session retrieval.
        #[cfg(unix)]
        {
            let status = Command::new("git")
                .current_dir(&project_root)
                .args(["init", "-b", "main"])
                .status()
                .expect("initialize beta37 project fixture repository");
            assert!(
                status.success(),
                "initialize beta37 project fixture repository: {status}"
            );
            let status = Command::new("git")
                .current_dir(&project_root)
                .args(["-c", "core.hooksPath=.git/no-hooks", "add", "."])
                .status()
                .expect("stage beta37 project fixture");
            assert!(status.success(), "stage beta37 project fixture: {status}");
            let status = Command::new("git")
                .current_dir(&project_root)
                .args([
                    "-c",
                    "core.hooksPath=.git/no-hooks",
                    "user.name=TraceDecay beta37 fixture",
                    "-c",
                    "user.email=beta37-fixture@example.invalid",
                    "commit",
                    "--quiet",
                    "--allow-empty",
                    "-m",
                    "beta37 fixture",
                ])
                .status()
                .expect("commit beta37 project fixture");
            assert!(status.success(), "commit beta37 project fixture: {status}");
            let status = Command::new("git")
                .current_dir(&project_root)
                .args(["branch", "-M", "main"])
                .status()
                .expect("name beta37 fixture branch");
            assert!(status.success(), "name beta37 fixture branch: {status}");
        }

        let session_path = profile.join("user-sessions.db");
        seed_session_temporal_workflow_git_and_lcm(&session_path);
        convert_session_to_released_v3(&session_path);
        convert_lcm_to_released_beta37(&session_path);
        // The daemon's canonical project retrieval route serves the
        // registered ProjectSessions shard, while the profile authority is a
        // separate registered shard. Seed both from the same beta.37 release
        // rows so a production Native recall cannot accidentally fall back to
        // the profile database or fixture-only text.
        if project_sessions_store != session_path {
            seed_session_temporal_workflow_git_and_lcm(&project_sessions_store);
            convert_session_to_released_v3(&project_sessions_store);
            convert_lcm_to_released_beta37(&project_sessions_store);
        }
        fs::create_dir_all(profile.join("lcm-payloads")).expect("create LCM payload directory");
        fs::write(
            profile
                .join("lcm-payloads")
                .join(BETA37_LCM_EXTERNAL_PAYLOAD_REF),
            BETA37_LCM_EXTERNAL_CONTENT,
        )
        .expect("write released external LCM payload");
        replace_configuration_with_released_beta37(&profile.join("global.db"));

        // Native provider state is a host-owned opaque artifact in the
        // released profile. Keeping it outside the exact V2 memory schema is
        // deliberate: an invented SQLite table would make the shipped
        // migration refuse the profile for an unsupported shape. The
        // replacement must carry these bytes through its opaque authority and
        // prove the identity/content/source after every reopen.
        let native_state = profile.join(BETA37_NATIVE_PROVIDER_STATE_PATH);
        fs::create_dir_all(native_state.parent().expect("native state parent"))
            .expect("create Native provider state directory");
        fs::write(
            native_state,
            format!(
                "{{\"memory_id\":\"{BETA37_NATIVE_MEMORY_ID}\",\"content\":\"{BETA37_NATIVE_MEMORY_CONTENT}\",\"source\":\"{BETA37_NATIVE_MEMORY_SOURCE}\"}}"
            ),
        )
        .expect("seed released Native provider state");

        Self {
            temp,
            profile,
            project_root,
            project_store,
        }
    }

    fn inventory(&self) -> BTreeMap<String, Vec<u8>> {
        inventory(&self.profile)
    }
}

fn write_profile_identity(profile: &Path) {
    let path = profile.join("profile-identity.json");
    DatabaseAuthority::publish_record_atomically(
        &path.with_extension("json.tmp"),
        &path,
        &serde_json::to_vec_pretty(&serde_json::json!({
            "schema_version": 1,
            "brain_id": BRAIN_ID,
            "profile_id": PROFILE_ID,
        }))
        .expect("profile identity JSON"),
        PROFILE_IDENTITY_RECORD_NAME,
    )
    .expect("publish V1 profile identity");
}

fn replace_project_store_with_released_v34(path: &Path) {
    remove_database_and_sidecars(path);
    let conn = Connection::open(path).expect("create released v34 project store");
    conn.execute_batch(include_str!(
        "../../../tracedecay-runtime-core/tests/fixtures/project-store-released-v34.sql"
    ))
    .expect("load released v34 project schema");
    conn.execute_batch("PRAGMA user_version = 34;")
        .expect("mark project store v34");

    let owner = format!(r#"{{"kind":"project","project_id":"{PROJECT_ID}"}}"#);
    conn.execute(
        "INSERT INTO metadata(key, value) VALUES (?1, ?2)",
        params!["release_fixture", "beta37-project-v34"],
    )
    .expect("seed project metadata");
    conn.execute(
        "INSERT INTO memory_v2_facts(
            fact_id, owner_kind, project_id, owner_json, identity_json, created_at
         ) VALUES (?1, 'project', ?2, ?3, ?4, ?5)",
        params![
            "fact.beta37.code-graph",
            PROJECT_ID,
            owner,
            r#"{"source":"beta37-code-graph"}",
            1_740_000_000_i64,
        ],
    )
    .expect("seed released project fact");
    conn.execute(
        "INSERT INTO memory_v2_lineage_events(
            event_id, fact_id, owner_kind, project_id, event_json, occurred_at, recorded_at
         ) VALUES (?1, ?2, 'project', ?3, ?4, ?5, ?6)",
        params![
            "event.beta37.code-graph",
            "fact.beta37.code-graph",
            PROJECT_ID,
            r#"{"kind":"assertion_recorded","release":"beta37"}"#,
            1_740_000_000_i64,
            1_740_000_001_i64,
        ],
    )
    .expect("seed released fact lineage");
    conn.execute(
        "INSERT INTO memory_v2_assertions(
            assertion_id, fact_id, owner_kind, project_id, owner_json,
            assertion_header_json, kind_json, payload_reference_json, receipt_json,
            asserted_at, actor_id
         ) VALUES (?1, ?2, 'project', ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)",
        params![
            "assertion.beta37.code-graph",
            "fact.beta37.code-graph",
            PROJECT_ID,
            r#"{"kind":"project","project_id":"project.beta37-replacement"}"#,
            r#"{"assertion_id":"assertion.beta37.code-graph"}"#,
            r#"{"kind":"fact"}"#,
            r#"{"payload":"payload.beta37.code-graph"}"#,
            r#"{"receipt":"receipt.beta37.code-graph"}"#,
            1_740_000_000_i64,
            "fixture.beta37",
        ],
    )
    .expect("seed released fact assertion");
    conn.execute(
        "INSERT INTO memory_v2_assertion_payloads(
            assertion_id, fact_id, owner_kind, project_id, payload_json, content
         ) VALUES (?1, ?2, 'project', ?3, ?4, ?5)",
        params![
            "assertion.beta37.code-graph",
            "fact.beta37.code-graph",
            PROJECT_ID,
            r#"{"content":"The beta.37 code graph carries a signed symbol generation."}"#,
            "The beta.37 code graph carries a signed symbol generation.",
        ],
    )
    .expect("seed released fact payload");
    conn.execute(
        "INSERT INTO memory_v2_current_facts(
            fact_id, owner_kind, project_id, payload_access, trust_score,
            active_assertion_id, last_event_id, updated_at
         ) VALUES (?1, 'project', ?2, 'eligible', ?3, ?4, ?5, ?6)",
        params![
            "fact.beta37.code-graph",
            PROJECT_ID,
            0.91_f64,
            "assertion.beta37.code-graph",
            "event.beta37.code-graph",
            1_740_000_001_i64,
        ],
    )
    .expect("seed released current fact projection");
    conn.execute(
        "INSERT INTO retrieval_anchors(anchor_id, anchor_json, owner_json, projection_generation)
         VALUES (?1, ?2, ?3, ?4)",
        params![
            "anchor.beta37.code-graph",
            r#"{"kind":"code_symbol","symbol":"NativeProvider"}"#,
            r#"{"kind":"project","project_id":"project.beta37-replacement"}"#,
            "generation.beta37.code",
        ],
    )
    .expect("seed code graph retrieval anchor");
    conn.execute(
        "INSERT INTO memory_v2_evidence(
            evidence_id, fact_id, owner_kind, project_id, owner_json, anchor_id, evidence_json
         ) VALUES (?1, ?2, 'project', ?3, ?4, ?5, ?6)",
        params![
            "evidence.beta37.code-graph",
            "fact.beta37.code-graph",
            PROJECT_ID,
            r#"{"kind":"project","project_id":"project.beta37-replacement"}"#,
            "anchor.beta37.code-graph",
            r#"{"path":"src/native_provider.rs","symbol":"NativeProvider"}"#,
        ],
    )
    .expect("seed code graph evidence");
    conn.execute(
        "INSERT INTO memory_v2_assertion_evidence(
            assertion_id, evidence_id, fact_id, owner_kind, project_id, ordinal
         ) VALUES (?1, ?2, ?3, 'project', ?4, 0)",
        params![
            "assertion.beta37.code-graph",
            "evidence.beta37.code-graph",
            "fact.beta37.code-graph",
            PROJECT_ID,
        ],
    )
    .expect("seed code graph assertion evidence");
    conn.execute(
        "INSERT INTO graph_publication_replay_v1(
            shard_id, namespace, projection, generation, idempotency_key,
            input_digest, dependency_generation_closure_digest, direct_dependency_bytes,
            expected_prior_head, expected_recovered_digest, canonical_replay_source_digest,
            canonical_replay_source
         ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, NULL, ?9, ?10, ?11)",
        params![
            format!("project:{PROJECT_ID}"),
            "code-graph",
            "symbols",
            "generation.beta37.code",
            "replay.beta37.code",
            "sha256:input-beta37-code",
            "sha256:dependencies-beta37-code",
            2_i64,
            "sha256:recovered-beta37-code",
            "sha256:source-beta37-code",
            br#"{"symbols":["NativeProvider"]}"#.as_slice(),
        ],
    )
    .expect("seed code graph replay source");
    conn.execute(
        "INSERT INTO graph_verified_heads_v1(
            shard_id, namespace, projection, replay_sequence, recovered_digest
         ) VALUES (?1, ?2, ?3, 1, ?4)",
        params![
            format!("project:{PROJECT_ID}"),
            "code-graph",
            "symbols",
            "sha256:recovered-beta37-code",
        ],
    )
    .expect("seed verified code graph head");
    conn.execute(
        "INSERT INTO generation_diagnostics(
            diagnostic_anchor, generation_id, repository, worktree, reference,
            source_revision, file_occurrence_id, content_digest, symbol_occurrence_id,
            span_start, span_end, code, severity, message, message_digest,
            producer_kind, producer, analyzer_revision, configuration_revision,
            sanitization_receipt, evidence_class, collected_at, record_state,
            state_generation, persisted_at
         ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13,
                   ?14, ?15, ?16, ?17, ?18, ?19, ?20, ?21, ?22, ?23, ?24, ?25)",
        params![
            "diagnostic.beta37.code-graph",
            "generation.beta37.code",
            "repo.beta37",
            "worktree.beta37",
            "refs/heads/main",
            "commit.beta37",
            "file.beta37.native-provider",
            "sha256:file-beta37-native-provider",
            "symbol.beta37.NativeProvider",
            10_i64,
            28_i64,
            "native-provider",
            "info",
            "beta.37 Native provider symbol",
            "sha256:message-beta37-native-provider",
            "code-index",
            "fixture",
            "v1",
            "configuration.beta37",
            "sanitized.beta37",
            "code",
            1_740_000_000_i64,
            "current",
            "generation.beta37.code",
            1_740_000_001_i64,
        ],
    )
    .expect("seed code graph diagnostic");
    drop(conn);
}

fn seed_session_temporal_workflow_git_and_lcm(path: &Path) {
    let conn = Connection::open(path).expect("open materialized session store");
    conn.execute_batch("PRAGMA foreign_keys = ON;")
        .expect("enable session foreign keys");
    conn.execute(
        "INSERT INTO sessions(
            provider, session_id, project_key, project_path, title, started_at,
            ended_at, transcript_path, metadata_json, parent_session_id,
            is_subagent, agent_id, parent_tool_use_id
         ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, NULL, ?7, ?8, NULL, 0, NULL, NULL)",
        params![
            "codex",
            "session.beta37.replacement",
            PROJECT_ID,
            "/beta37/project",
            "beta.37 replacement transcript",
            1_740_000_000_i64,
            "projects/project.beta37-replacement/sessions/codex-beta37.jsonl",
            r#"{"release":"beta.37","authority":"lcm"}"#,
        ],
    )
    .expect("seed retained session");
    conn.execute(
        "INSERT INTO session_messages(
            provider, message_id, session_id, role, timestamp, ordinal, text,
            kind, model, tool_names, source_path, source_offset, metadata_json
         ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13)",
        params![
            "codex",
            "message.beta37.replacement",
            "session.beta37.replacement",
            "assistant",
            1_740_000_001_i64,
            0_i64,
            TRANSCRIPT_TEXT,
            "text",
            "codex-beta37",
            "tracedecay_context",
            "projects/project.beta37-replacement/sessions/codex-beta37.jsonl",
            0_i64,
            r#"{"release":"beta.37"}"#,
        ],
    )
    .expect("seed retained session message");
    conn.execute(
        "INSERT INTO session_temporal_generations(
            session_id, generation, state, frozen_watermarks_json, created_at
         ) VALUES (?1, 1, 'building', '{}', ?2)",
        params!["session.beta37.replacement", 1_740_000_002_i64],
    )
    .expect("seed session temporal generation");

    conn.execute(
        "INSERT INTO workflow_runs(
            run_id, parent_session_id, name, description, phase_json, status,
            started_ts, ended_ts, result_summary, agent_count, created_at, updated_at
         ) VALUES (?1, ?2, ?3, ?4, ?5, 'completed', ?6, ?7, ?8, 1, ?6, ?7)",
        params![
            "workflow.beta37.replacement",
            "session.beta37.replacement",
            "beta37-replacement",
            "released workflow authority",
            r#"{"phase":"migration"}"#,
            1_740_000_003_i64,
            1_740_000_004_i64,
            "replayed",
        ],
    )
    .expect("seed workflow run");
    conn.execute(
        "INSERT INTO workflow_agents(
            run_id, agent_label, agent_id, phase, transcript_path,
            agent_session_id, status, model, tokens, started_ts, ended_ts,
            created_at, updated_at
         ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, 'completed', ?7, 12, ?8, ?9, ?8, ?9)",
        params![
            "workflow.beta37.replacement",
            "primary",
            "agent.beta37",
            "migration",
            "projects/project.beta37-replacement/sessions/codex-beta37.jsonl",
            "session.beta37.replacement",
            "codex-beta37",
            1_740_000_003_i64,
            1_740_000_004_i64,
        ],
    )
    .expect("seed workflow agent");
    conn.execute(
        "INSERT INTO workflow_index_meta(key, value) VALUES ('latest_run_mtime', 1740000004)",
        [],
    )
    .expect("seed workflow index metadata");

    conn.execute(
        "INSERT INTO git_correlation_meta(key, value) VALUES ('history_frontier', 1)",
        [],
    )
    .expect("seed Git correlation metadata");
    conn.execute(
        "INSERT INTO git_evidence_publication_outbox(
            receipt_id, publication_prefix, evidence_json, created_at
         ) VALUES (?1, ?2, ?3, ?4)",
        params![
            "git-publication.beta37",
            "beta37/project",
            r#"{"commit":"commit.beta37","source":"code-graph"}"#,
            1_740_000_005_i64,
        ],
    )
    .expect("seed Git publication receipt");

    conn.execute(
        "INSERT INTO lcm_raw_messages(
            provider, message_id, session_id, role, ordinal, timestamp, content,
            content_hash, storage_kind, payload_ref, snippet_text, index_text,
            legacy_source, legacy_truncated, metadata_json
         ) VALUES (?1, ?2, ?3, ?4, 0, ?5, ?6, ?7, 'inline', NULL, ?6, ?6, 0, 0, ?8)",
        params![
            "codex",
            "message.beta37.replacement",
            "session.beta37.replacement",
            "assistant",
            1_740_000_001_i64,
            TRANSCRIPT_TEXT,
            "sha256:beta37-message",
            r#"{"source":"beta.37","lcm":"retained"}"#,
        ],
    )
    .expect("seed LCM raw message");
    let external_content_hash =
        tracedecay_domain::canonical_text::sha256_hex(BETA37_LCM_EXTERNAL_CONTENT.as_bytes());
    conn.execute(
        "INSERT INTO lcm_raw_messages(
            provider, message_id, session_id, role, ordinal, timestamp, content,
            content_hash, storage_kind, payload_ref, snippet_text, index_text,
            legacy_source, legacy_truncated, metadata_json
         ) VALUES (?1, ?2, ?3, ?4, 1, ?5, NULL, ?6, 'external', ?7, ?8, ?8, 0, 0, ?9)",
        params![
            "codex",
            "message.beta37.external",
            "session.beta37.replacement",
            "tool",
            1_740_000_002_i64,
            external_content_hash,
            BETA37_LCM_EXTERNAL_PAYLOAD_REF,
            BETA37_LCM_EXTERNAL_CONTENT,
            r#"{"source":"beta.37","lcm":"external"}"#,
        ],
    )
    .expect("seed external LCM raw message");
    conn.execute(
        "INSERT INTO lcm_external_payloads(
            payload_ref, provider, session_id, message_id, kind, content_hash,
            byte_count, char_count, created_at, metadata_json
         ) VALUES (?1, ?2, ?3, ?4, 'message', ?5, ?6, ?7, ?8, ?9)",
        params![
            BETA37_LCM_EXTERNAL_PAYLOAD_REF,
            "codex",
            "session.beta37.replacement",
            "message.beta37.external",
            external_content_hash,
            i64::try_from(BETA37_LCM_EXTERNAL_CONTENT.len()).expect("external payload byte count"),
            i64::try_from(BETA37_LCM_EXTERNAL_CONTENT.chars().count())
                .expect("external payload character count"),
            1_740_000_002_i64,
            r#"{"source":"beta.37","lcm":"external"}"#,
        ],
    )
    .expect("seed external LCM payload metadata");
    conn.execute(
        "INSERT INTO lcm_lifecycle_state(
            provider, conversation_id, current_session_id,
            last_finalized_session_id, current_frontier_store_id,
            last_finalized_frontier_store_id, updated_at
         ) VALUES ('codex', 'conversation.beta37', 'session.beta37.replacement',
                   'session.beta37.replacement', 1, 1, 1740000002)",
        [],
    )
    .expect("seed LCM lifecycle state");
    drop(conn);
}

fn convert_session_to_released_v3(path: &Path) {
    let conn = Connection::open(path).expect("open session store for beta.37 conversion");
    let trigger_sql = trigger_sql_for_table(&conn, "session_temporal_projection_receipts");
    conn.execute_batch("PRAGMA foreign_keys = OFF;")
        .expect("disable foreign keys for released temporal conversion");
    conn.execute_batch(
        "DROP TABLE IF EXISTS session_temporal_projection_receipts;
         DROP TABLE IF EXISTS session_relation_receipts;",
    )
    .expect("drop post-beta37 temporal tables");
    conn.execute_batch(SESSION_TEMPORAL_PROJECTION_RECEIPTS_V3_DDL)
        .expect("install beta37 projection receipt table");
    conn.execute_batch(include_str!(
        "../../../tracedecay-global-db/tests/fixtures/session-relation-receipts-before-recovery.sql"
    ))
    .expect("install beta37 relation receipt table");
    for sql in trigger_sql {
        conn.execute_batch(&sql)
            .expect("restore beta37 projection trigger");
    }
    conn.execute_batch(include_str!(
        "../../../tracedecay-global-db/tests/fixtures/session-temporal-released-v3-triggers.sql"
    ))
    .expect("install beta37 temporal triggers");
    let changed = conn
        .execute(
            "UPDATE session_temporal_schema_migrations
             SET version = 3, applied_at = 100
             WHERE name = 'session-temporal'",
            [],
        )
        .expect("mark released session temporal schema");
    assert_eq!(
        changed, 1,
        "host fixture must carry session temporal marker"
    );
    drop(conn);
}

fn convert_lcm_to_released_beta37(path: &Path) {
    let conn = Connection::open(path).expect("open session store for beta37 LCM conversion");
    let mut statement = conn
        .prepare(
            "SELECT type, name FROM sqlite_master
             WHERE name LIKE 'lcm\\_%' ESCAPE '\\'
                OR name LIKE 'idx_lcm\\_%' ESCAPE '\\'",
        )
        .expect("inspect LCM object inventory");
    let rows = statement
        .query_map([], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
        })
        .expect("query LCM object inventory")
        .collect::<Result<Vec<_>, _>>()
        .expect("collect LCM object inventory");
    drop(statement);
    for object_type in ["trigger", "index", "table"] {
        for (actual_type, name) in rows.iter().filter(|(kind, _)| kind == object_type) {
            if BETA37_LCM_OBJECTS.contains(&name.as_str()) {
                continue;
            }
            let quoted_name = quote_identifier(name);
            conn.execute_batch(&format!("DROP {actual_type} IF EXISTS {quoted_name};"))
                .expect("drop post-beta37 LCM object");
        }
    }
    drop(conn);
}

fn replace_configuration_with_released_beta37(path: &Path) {
    let conn = Connection::open(path).expect("open global store for beta37 configuration");
    conn.execute_batch("PRAGMA foreign_keys = OFF;")
        .expect("disable foreign keys for released configuration conversion");
    let mut statement = conn
        .prepare(
            "SELECT type, name FROM sqlite_master
             WHERE name LIKE 'configuration\\_%' ESCAPE '\\'",
        )
        .expect("inspect configuration object inventory");
    let objects = statement
        .query_map([], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
        })
        .expect("query configuration object inventory")
        .collect::<Result<Vec<_>, _>>()
        .expect("collect configuration object inventory");
    drop(statement);
    for object_type in ["trigger", "index", "table"] {
        for (actual_type, name) in objects.iter().filter(|(kind, _)| kind == object_type) {
            let quoted_name = quote_identifier(name);
            conn.execute_batch(&format!("DROP {actual_type} IF EXISTS {quoted_name};"))
                .expect("drop previous configuration object");
        }
    }
    conn.execute_batch(include_str!(
        "../../../tracedecay-global-db/tests/fixtures/configuration-released-beta37.sql"
    ))
    .expect("install beta37 configuration schema");
    conn.execute_batch(&format!(
        "INSERT INTO configuration_revisions(
            revision_id, parent_revision_id, snapshot_id, effective_behavior_digest,
            resolution_provenance_digest, actor_id, operation_kind, created_at
         ) VALUES ('revision.beta37', NULL, 'snapshot.beta37', 'sha256:behavior-beta37',
                   'sha256:provenance-beta37', 'fixture.beta37', 'bootstrap', 1740000000);
         INSERT INTO configuration_entries(
            revision_id, key, layer_kind, layer_id, schema_revision, typed_value
         ) VALUES
            ('revision.beta37', 'memory.provider_native_enabled.v1', 'default', NULL, 1, '{native_enabled}'),
            ('revision.beta37', 'memory.provider_ncm_observer.v1', 'default', NULL, 1, '{ncm_observer}'),
            ('revision.beta37', 'memory.provider_recall_routing.v1', 'default', NULL, 1, '{recall_routing}');
         INSERT INTO configuration_topology_policies(
            revision_id, schema_version, topology_policy_digest, placement_kind,
            default_cross_merge_mode, allow_cross_repository, cleanliness_kind,
            review_kind, require_fresh_preflight, maximum_preflight_age_seconds,
            history_rewrite_kind, escalation_kind, automatic_gc_kind,
            notification_level, sealed_policy_value
         ) VALUES ('revision.beta37', 1, 'sha256:topology-beta37', 'single_root',
                   'forbid', 0, 'clean', 'required', 1, 300,
                   'forbid_force_and_rebase', 'manual', 'disabled', 'normal',
                   X'00000000000000000000000000000000');
         INSERT INTO configuration_topology_roots(
            revision_id, root_ordinal, root_id, locator_digest,
            repository_scope_digest, maximum_active_worktrees
         ) VALUES ('revision.beta37', 0, 'root.beta37', 'sha256:locator-beta37',
                   'sha256:scope-beta37', 4);
         INSERT INTO configuration_topology_protected_refs(
            revision_id, rule_ordinal, selector_kind, selector_digest, disposition
         ) VALUES ('revision.beta37', 0, 'branch', 'sha256:main-beta37', 'protected');
         INSERT INTO configuration_source_bindings(
            revision_id, binding_id, source_kind, locator_digest, authority_kind,
            project_id, user_profile_id, provenance_digest
         ) VALUES ('revision.beta37', 'binding.beta37', 'git', 'sha256:git-beta37',
                   'project', 'project.beta37-replacement', NULL, 'sha256:provenance-git');
         INSERT INTO configuration_access_rules(
            revision_id, rule_id, subject_kind, subject_id, actor_kind, actor_id,
            operation_kind, source_kind, authority_kind, project_id, user_profile_id,
            capability_encoding, effect, expires_at
         ) VALUES ('revision.beta37', 'access.beta37', 'project', 'project.beta37-replacement',
                   'host', 'codex', 'read', 'git', 'project', 'project.beta37-replacement',
                   NULL, 'read,recall', 'allow', NULL);
         INSERT INTO configuration_component_activation_events(
            component, desired_revision_id, observed_revision_id,
            last_working_revision_id, restart_required, activation_error_code, occurred_at
         ) VALUES ('native-provider', 'revision.beta37', 'revision.beta37',
                   'revision.beta37', 0, NULL, 1740000001);",
        native_enabled = BETA37_CONFIGURATION_NATIVE_ENABLED,
        ncm_observer = BETA37_CONFIGURATION_NCM_OBSERVER,
        recall_routing = BETA37_CONFIGURATION_RECALL_ROUTING,
    ))
    .expect("seed beta37 configuration settings");
    drop(conn);
}

fn trigger_sql_for_table(conn: &Connection, table: &str) -> Vec<String> {
    let mut statement = conn
        .prepare(
            "SELECT sql FROM sqlite_master
             WHERE type = 'trigger' AND tbl_name = ?1 ORDER BY name",
        )
        .expect("prepare temporal trigger query");
    statement
        .query_map([table], |row| row.get::<_, String>(0))
        .expect("query temporal trigger SQL")
        .collect::<Result<Vec<_>, _>>()
        .expect("collect temporal trigger SQL")
}

fn quote_identifier(value: &str) -> String {
    format!("\"{}\"", value.replace('"', "\"\""))
}

fn remove_database_and_sidecars(path: &Path) {
    for suffix in ["", "-wal", "-shm", "-journal"] {
        let candidate = if suffix.is_empty() {
            path.to_path_buf()
        } else {
            PathBuf::from(format!("{}{}", path.display(), suffix))
        };
        if candidate.exists() {
            fs::remove_file(candidate).expect("remove database artifact");
        }
    }
}

fn inventory(root: &Path) -> BTreeMap<String, Vec<u8>> {
    let mut files = BTreeMap::new();
    inventory_directory(root, root, &mut files);
    files
}

/// Compare the V1 authority bytes without lifecycle metadata owned by the
/// replacement coordinator. A directory exchange keeps the held lifecycle
/// lock reachable from the preserved root, and the coordinator's journal /
/// rehearsal markers are likewise transaction metadata rather than V1
/// authorities. Every other path remains part of the equality check.
fn profile_authority_inventory(root: &Path) -> BTreeMap<String, Vec<u8>> {
    let mut files = inventory(root);
    for reserved in [
        "lifecycle.lock",
        ".tracedecay-v1-replacement.json",
        ".tracedecay-profile-rehearsal.json",
        ".tracedecay-v1-to-v2-manifest.json",
    ] {
        files.remove(reserved);
    }
    files
}

fn inventory_directory(root: &Path, directory: &Path, files: &mut BTreeMap<String, Vec<u8>>) {
    let mut entries = fs::read_dir(directory)
        .expect("read inventory directory")
        .map(|entry| entry.expect("read inventory entry"))
        .collect::<Vec<_>>();
    entries.sort_by_key(|entry| entry.file_name());
    for entry in entries {
        let path = entry.path();
        let metadata = fs::symlink_metadata(&path).expect("inspect inventory entry");
        if metadata.file_type().is_symlink() {
            panic!("fixture contains unexpected symlink: {}", path.display());
        }
        if metadata.is_dir() {
            inventory_directory(root, &path, files);
        } else if metadata.is_file() {
            let relative = path
                .strip_prefix(root)
                .expect("inventory relative path")
                .to_string_lossy()
                .replace('\\', "/");
            files.insert(relative, fs::read(path).expect("read inventory file"));
        }
    }
}

fn assert_external_lcm_payload_closure(profile: &Path) {
    assert_external_lcm_payload_closure_for(profile, &profile.join("user-sessions.db"));
}

fn assert_external_lcm_payload_closure_for(profile: &Path, sessions_path: &Path) {
    let sessions = open_read_only(sessions_path);
    let (payload_ref, provider, session_id, message_id, content_hash, byte_count, char_count): (
        String,
        String,
        String,
        String,
        String,
        i64,
        i64,
    ) = sessions
        .query_row(
            "SELECT payload_ref, provider, session_id, message_id, content_hash,
                    byte_count, char_count
             FROM lcm_external_payloads
             WHERE payload_ref = ?1",
            [BETA37_LCM_EXTERNAL_PAYLOAD_REF],
            |row| {
                Ok((
                    row.get(0)?,
                    row.get(1)?,
                    row.get(2)?,
                    row.get(3)?,
                    row.get(4)?,
                    row.get(5)?,
                    row.get(6)?,
                ))
            },
        )
        .expect("read external LCM payload metadata");
    assert_eq!(provider, "codex");
    assert_eq!(session_id, "session.beta37.replacement");
    assert_eq!(message_id, "message.beta37.external");
    let payload_path = profile.join("lcm-payloads").join(&payload_ref);
    let payload = fs::read(&payload_path).expect("read external LCM payload object");
    assert_eq!(payload, BETA37_LCM_EXTERNAL_CONTENT.as_bytes());
    assert_eq!(
        content_hash,
        tracedecay_domain::canonical_text::sha256_hex(&payload)
    );
    assert_eq!(
        byte_count,
        i64::try_from(payload.len()).expect("payload byte count")
    );
    assert_eq!(
        char_count,
        i64::try_from(String::from_utf8_lossy(&payload).chars().count())
            .expect("payload character count")
    );
    let (storage_kind, raw_payload_ref, raw_content): (String, String, Option<String>) = sessions
        .query_row(
            "SELECT storage_kind, payload_ref, content
             FROM lcm_raw_messages
             WHERE provider = ?1 AND message_id = ?2",
            params![provider, message_id],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .expect("read external LCM raw row");
    assert_eq!(storage_kind, "external");
    assert_eq!(raw_payload_ref, payload_ref);
    assert!(
        raw_content.is_none(),
        "external raw rows must close through payload_ref"
    );
}

/// Seed the released profile-memory authority with the Native user-memory
/// record that the replacement must carry forward. The opaque provider file
/// above exercises first-party provider bytes; this relational record proves
/// that the same identity, payload, lineage, receipt, and source anchor also
/// survive the registered profile-memory migration.
fn seed_beta37_profile_memory(path: &Path) {
    let mut conn = Connection::open(path).expect("open beta37 profile-memory authority");
    conn.execute_batch("PRAGMA foreign_keys = ON;")
        .expect("enable profile-memory foreign keys");
    let transaction = conn
        .transaction()
        .expect("begin beta37 profile-memory seed");
    let owner = r#"{"kind":"profile"}"#;
    transaction
        .execute(
            "INSERT INTO memory_v2_facts(
                fact_id, owner_kind, project_id, owner_json, identity_json, created_at
             ) VALUES (?1, 'profile', '', ?2, ?3, ?4)",
            params![
                BETA37_PROFILE_MEMORY_FACT_ID,
                owner,
                r#"{"source":"beta37-native-user-memory","memory_id":"native-beta37-memory"}"#,
                1_740_000_006_i64,
            ],
        )
        .expect("seed beta37 profile-memory fact");
    transaction
        .execute(
            "INSERT INTO memory_v2_lineage_events(
                event_id, fact_id, owner_kind, project_id, event_json,
                occurred_at, recorded_at
             ) VALUES (?1, ?2, 'profile', '', ?3, ?4, ?5)",
            params![
                BETA37_PROFILE_MEMORY_EVENT_ID,
                BETA37_PROFILE_MEMORY_FACT_ID,
                r#"{"kind":"assertion_recorded","release":"beta37","source":"native-provider"}"#,
                1_740_000_006_i64,
                1_740_000_007_i64,
            ],
        )
        .expect("seed beta37 profile-memory lineage");
    transaction
        .execute(
            "INSERT INTO retrieval_anchors(
                anchor_id, anchor_json, owner_json, projection_generation
             ) VALUES (?1, ?2, ?3, ?4)",
            params![
                BETA37_PROFILE_MEMORY_ANCHOR_ID,
                r#"{"kind":"user_memory","memory_id":"native-beta37-memory"}"#,
                owner,
                "generation.beta37.native",
            ],
        )
        .expect("seed beta37 profile-memory retrieval anchor");
    transaction
        .execute(
            "INSERT INTO memory_v2_assertions(
                assertion_id, fact_id, owner_kind, project_id, owner_json,
                assertion_header_json, kind_json, payload_reference_json,
                receipt_json, asserted_at, actor_id
             ) VALUES (?1, ?2, 'profile', '', ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
            params![
                BETA37_PROFILE_MEMORY_ASSERTION_ID,
                BETA37_PROFILE_MEMORY_FACT_ID,
                owner,
                format!(r#"{{"assertion_id":"{BETA37_PROFILE_MEMORY_ASSERTION_ID}"}}"#),
                r#"{"kind":"user_memory"}"#,
                r#"{"payload":"native-beta37-memory"}"#,
                r#"{"receipt":"receipt.beta37.native-user-memory"}"#,
                1_740_000_006_i64,
                "fixture.beta37",
            ],
        )
        .expect("seed beta37 profile-memory assertion");
    transaction
        .execute(
            "INSERT INTO memory_v2_assertion_payloads(
                assertion_id, fact_id, owner_kind, project_id, payload_json, content
             ) VALUES (?1, ?2, 'profile', '', ?3, ?4)",
            params![
                BETA37_PROFILE_MEMORY_ASSERTION_ID,
                BETA37_PROFILE_MEMORY_FACT_ID,
                r#"{"content":"V1 Native state is inspection-only","source":"beta.37"}"#,
                BETA37_NATIVE_MEMORY_CONTENT,
            ],
        )
        .expect("seed beta37 profile-memory payload");
    transaction
        .execute(
            "INSERT INTO memory_v2_current_facts(
                fact_id, owner_kind, project_id, payload_access, trust_score,
                active_assertion_id, last_event_id, updated_at
             ) VALUES (?1, 'profile', '', 'eligible', ?2, ?3, ?4, ?5)",
            params![
                BETA37_PROFILE_MEMORY_FACT_ID,
                0.88_f64,
                BETA37_PROFILE_MEMORY_ASSERTION_ID,
                BETA37_PROFILE_MEMORY_EVENT_ID,
                1_740_000_007_i64,
            ],
        )
        .expect("seed beta37 profile-memory current projection");
    transaction
        .execute(
            "INSERT INTO memory_v2_evidence(
                evidence_id, fact_id, owner_kind, project_id, owner_json,
                anchor_id, evidence_json
             ) VALUES (?1, ?2, 'profile', '', ?3, ?4, ?5)",
            params![
                BETA37_PROFILE_MEMORY_EVIDENCE_ID,
                BETA37_PROFILE_MEMORY_FACT_ID,
                owner,
                BETA37_PROFILE_MEMORY_ANCHOR_ID,
                r#"{"source":"beta.37","path":"native-provider-state/beta37-memory.json"}"#,
            ],
        )
        .expect("seed beta37 profile-memory evidence");
    transaction
        .execute(
            "INSERT INTO memory_v2_assertion_evidence(
                assertion_id, evidence_id, fact_id, owner_kind, project_id, ordinal
             ) VALUES (?1, ?2, ?3, 'profile', '', 0)",
            params![
                BETA37_PROFILE_MEMORY_ASSERTION_ID,
                BETA37_PROFILE_MEMORY_EVIDENCE_ID,
                BETA37_PROFILE_MEMORY_FACT_ID,
            ],
        )
        .expect("seed beta37 profile-memory assertion evidence");
    transaction
        .execute(
            "INSERT INTO memory_v2_operation_receipts(
                owner_kind, project_id, operation_id, operation_kind,
                request_digest, fact_id, event_id, receipt_json, recorded_at
             ) VALUES ('profile', '', ?1, 'add', ?2, ?3, ?4, ?5, ?6)",
            params![
                "operation.beta37.native-user-memory",
                "sha256:request-beta37-native-user-memory",
                BETA37_PROFILE_MEMORY_FACT_ID,
                BETA37_PROFILE_MEMORY_EVENT_ID,
                r#"{"receipt_id":"receipt.beta37.native-user-memory","source":"beta.37"}"#,
                1_740_000_008_i64,
            ],
        )
        .expect("seed beta37 profile-memory operation receipt");
    transaction
        .commit()
        .expect("commit beta37 profile-memory seed");
}

fn assert_beta37_profile_memory_authority(profile: &Path) {
    let memory = open_read_only(&profile.join("user-memory.db"));
    let fact: (String, String, String, String, i64) = memory
        .query_row(
            "SELECT fact_id, owner_kind, project_id, owner_json, created_at
             FROM memory_v2_facts WHERE fact_id = ?1",
            [BETA37_PROFILE_MEMORY_FACT_ID],
            |row| {
                Ok((
                    row.get(0)?,
                    row.get(1)?,
                    row.get(2)?,
                    row.get(3)?,
                    row.get(4)?,
                ))
            },
        )
        .expect("read beta37 profile-memory fact");
    assert_eq!(
        fact,
        (
            BETA37_PROFILE_MEMORY_FACT_ID.to_owned(),
            "profile".to_owned(),
            String::new(),
            r#"{"kind":"profile"}"#.to_owned(),
            1_740_000_006,
        )
    );
    let lineage: (String, String, String, i64, i64) = memory
        .query_row(
            "SELECT event_id, fact_id, event_json, occurred_at, recorded_at
             FROM memory_v2_lineage_events WHERE event_id = ?1",
            [BETA37_PROFILE_MEMORY_EVENT_ID],
            |row| {
                Ok((
                    row.get(0)?,
                    row.get(1)?,
                    row.get(2)?,
                    row.get(3)?,
                    row.get(4)?,
                ))
            },
        )
        .expect("read beta37 profile-memory lineage");
    assert_eq!(
        lineage,
        (
            BETA37_PROFILE_MEMORY_EVENT_ID.to_owned(),
            BETA37_PROFILE_MEMORY_FACT_ID.to_owned(),
            r#"{"kind":"assertion_recorded","release":"beta37","source":"native-provider"}"#
                .to_owned(),
            1_740_000_006,
            1_740_000_007,
        )
    );
    let payload: (String, String) = memory
        .query_row(
            "SELECT payload_json, content
             FROM memory_v2_assertion_payloads WHERE assertion_id = ?1",
            [BETA37_PROFILE_MEMORY_ASSERTION_ID],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .expect("read beta37 profile-memory payload");
    assert_eq!(
        payload,
        (
            r#"{"content":"V1 Native state is inspection-only","source":"beta.37"}"#.to_owned(),
            BETA37_NATIVE_MEMORY_CONTENT.to_owned(),
        )
    );
    let current: (String, String, String, String, f64, String, String, i64) = memory
        .query_row(
            "SELECT fact_id, owner_kind, project_id, payload_access, trust_score,
                    active_assertion_id, last_event_id, updated_at
             FROM memory_v2_current_facts WHERE fact_id = ?1",
            [BETA37_PROFILE_MEMORY_FACT_ID],
            |row| {
                Ok((
                    row.get(0)?,
                    row.get(1)?,
                    row.get(2)?,
                    row.get(3)?,
                    row.get(4)?,
                    row.get(5)?,
                    row.get(6)?,
                    row.get(7)?,
                ))
            },
        )
        .expect("read beta37 profile-memory current projection");
    assert_eq!(
        current,
        (
            BETA37_PROFILE_MEMORY_FACT_ID.to_owned(),
            "profile".to_owned(),
            String::new(),
            "eligible".to_owned(),
            0.88,
            BETA37_PROFILE_MEMORY_ASSERTION_ID.to_owned(),
            BETA37_PROFILE_MEMORY_EVENT_ID.to_owned(),
            1_740_000_007,
        )
    );
    let evidence: (String, String, String, String) = memory
        .query_row(
            "SELECT evidence_id, fact_id, anchor_id, evidence_json
             FROM memory_v2_evidence WHERE evidence_id = ?1",
            [BETA37_PROFILE_MEMORY_EVIDENCE_ID],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
        )
        .expect("read beta37 profile-memory source evidence");
    assert_eq!(
        evidence,
        (
            BETA37_PROFILE_MEMORY_EVIDENCE_ID.to_owned(),
            BETA37_PROFILE_MEMORY_FACT_ID.to_owned(),
            BETA37_PROFILE_MEMORY_ANCHOR_ID.to_owned(),
            r#"{"source":"beta.37","path":"native-provider-state/beta37-memory.json"}"#.to_owned(),
        )
    );
    let receipt: (String, String, String, String, i64) = memory
        .query_row(
            "SELECT operation_id, operation_kind, request_digest, receipt_json, recorded_at
             FROM memory_v2_operation_receipts WHERE operation_id = 'operation.beta37.native-user-memory'",
            [],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?, row.get(4)?)),
        )
        .expect("read beta37 profile-memory operation receipt");
    assert_eq!(
        receipt,
        (
            "operation.beta37.native-user-memory".to_owned(),
            "add".to_owned(),
            "sha256:request-beta37-native-user-memory".to_owned(),
            r#"{"receipt_id":"receipt.beta37.native-user-memory","source":"beta.37"}"#.to_owned(),
            1_740_000_008,
        )
    );
}

fn assert_beta37_native_provider_state(profile: &Path) {
    let path = profile.join(BETA37_NATIVE_PROVIDER_STATE_PATH);
    let bytes = fs::read(&path).expect("read released Native provider state");
    assert_eq!(
        bytes,
        format!(
            "{{\"memory_id\":\"{BETA37_NATIVE_MEMORY_ID}\",\"content\":\"{BETA37_NATIVE_MEMORY_CONTENT}\",\"source\":\"{BETA37_NATIVE_MEMORY_SOURCE}\"}}"
        )
        .as_bytes(),
        "Native provider identity/content/source must remain byte stable"
    );
}

/// Check the released logical rows directly at every replacement boundary.
/// The complete backup manifest proves byte closure; these assertions prove
/// that the coordinator's V1-to-V2 transformation retained the identities,
/// ownership, provenance, timestamps, receipts, and source anchors that are
/// allowed to change representation during schema migration.
fn assert_beta37_project_authority(project_store: &Path) {
    let project = open_read_only(project_store);
    let metadata: String = project
        .query_row(
            "SELECT value FROM metadata WHERE key = 'release_fixture'",
            [],
            |row| row.get(0),
        )
        .expect("read beta37 project metadata");
    assert_eq!(metadata, "beta37-project-v34");

    let fact: (String, String, String, String, i64) = project
        .query_row(
            "SELECT fact_id, owner_kind, owner_json, identity_json, created_at
             FROM memory_v2_facts WHERE fact_id = 'fact.beta37.code-graph'",
            [],
            |row| {
                Ok((
                    row.get(0)?,
                    row.get(1)?,
                    row.get(2)?,
                    row.get(3)?,
                    row.get(4)?,
                ))
            },
        )
        .expect("read beta37 project fact");
    assert_eq!(
        fact,
        (
            "fact.beta37.code-graph".to_owned(),
            "project".to_owned(),
            format!(r#"{{"kind":"project","project_id":"{PROJECT_ID}"}}"#),
            r#"{"source":"beta37-code-graph"}"#.to_owned(),
            1_740_000_000,
        )
    );

    let lineage: (String, String, String, i64, i64) = project
        .query_row(
            "SELECT event_id, fact_id, event_json, occurred_at, recorded_at
             FROM memory_v2_lineage_events WHERE event_id = 'event.beta37.code-graph'",
            [],
            |row| {
                Ok((
                    row.get(0)?,
                    row.get(1)?,
                    row.get(2)?,
                    row.get(3)?,
                    row.get(4)?,
                ))
            },
        )
        .expect("read beta37 project lineage");
    assert_eq!(
        lineage,
        (
            "event.beta37.code-graph".to_owned(),
            "fact.beta37.code-graph".to_owned(),
            r#"{"kind":"assertion_recorded","release":"beta37"}"#.to_owned(),
            1_740_000_000,
            1_740_000_001,
        )
    );

    let assertion: (String, String, String, String, String, i64, String) = project
        .query_row(
            "SELECT assertion_id, fact_id, owner_json, payload_reference_json,
                    receipt_json, asserted_at, actor_id
             FROM memory_v2_assertions WHERE assertion_id = 'assertion.beta37.code-graph'",
            [],
            |row| {
                Ok((
                    row.get(0)?,
                    row.get(1)?,
                    row.get(2)?,
                    row.get(3)?,
                    row.get(4)?,
                    row.get(5)?,
                    row.get(6)?,
                ))
            },
        )
        .expect("read beta37 project assertion");
    assert_eq!(
        assertion,
        (
            "assertion.beta37.code-graph".to_owned(),
            "fact.beta37.code-graph".to_owned(),
            format!(r#"{{"kind":"project","project_id":"{PROJECT_ID}"}}"#),
            r#"{"payload":"payload.beta37.code-graph"}"#.to_owned(),
            r#"{"receipt":"receipt.beta37.code-graph"}"#.to_owned(),
            1_740_000_000,
            "fixture.beta37".to_owned(),
        )
    );
    let payload: (String, String) = project
        .query_row(
            "SELECT payload_json, content FROM memory_v2_assertion_payloads
             WHERE assertion_id = 'assertion.beta37.code-graph'",
            [],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .expect("read beta37 project assertion payload");
    assert_eq!(
        payload,
        (
            r#"{"content":"The beta.37 code graph carries a signed symbol generation."}"#
                .to_owned(),
            "The beta.37 code graph carries a signed symbol generation.".to_owned(),
        )
    );

    let current: (String, String, f64, String, String, i64) = project
        .query_row(
            "SELECT fact_id, payload_access, trust_score, active_assertion_id,
                    last_event_id, updated_at
             FROM memory_v2_current_facts WHERE fact_id = 'fact.beta37.code-graph'",
            [],
            |row| {
                Ok((
                    row.get(0)?,
                    row.get(1)?,
                    row.get(2)?,
                    row.get(3)?,
                    row.get(4)?,
                    row.get(5)?,
                ))
            },
        )
        .expect("read beta37 current project fact");
    assert_eq!(
        current,
        (
            "fact.beta37.code-graph".to_owned(),
            "eligible".to_owned(),
            0.91,
            "assertion.beta37.code-graph".to_owned(),
            "event.beta37.code-graph".to_owned(),
            1_740_000_001,
        )
    );

    let anchor: (String, String, String, String) = project
        .query_row(
            "SELECT anchor_id, anchor_json, owner_json, projection_generation
             FROM retrieval_anchors WHERE anchor_id = 'anchor.beta37.code-graph'",
            [],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
        )
        .expect("read beta37 retrieval anchor");
    assert_eq!(
        anchor,
        (
            "anchor.beta37.code-graph".to_owned(),
            r#"{"kind":"code_symbol","symbol":"NativeProvider"}"#.to_owned(),
            format!(r#"{{"kind":"project","project_id":"{PROJECT_ID}"}}"#),
            "generation.beta37.code".to_owned(),
        )
    );
    let evidence: (String, String, String, String) = project
        .query_row(
            "SELECT evidence_id, fact_id, anchor_id, evidence_json
             FROM memory_v2_evidence WHERE evidence_id = 'evidence.beta37.code-graph'",
            [],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
        )
        .expect("read beta37 source evidence");
    assert_eq!(
        evidence,
        (
            "evidence.beta37.code-graph".to_owned(),
            "fact.beta37.code-graph".to_owned(),
            "anchor.beta37.code-graph".to_owned(),
            r#"{"path":"src/native_provider.rs","symbol":"NativeProvider"}"#.to_owned(),
        )
    );
    let assertion_evidence: (String, String, String, i64) = project
        .query_row(
            "SELECT assertion_id, evidence_id, fact_id, ordinal
             FROM memory_v2_assertion_evidence
             WHERE assertion_id = 'assertion.beta37.code-graph'
               AND evidence_id = 'evidence.beta37.code-graph'",
            [],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
        )
        .expect("read beta37 assertion source evidence binding");
    assert_eq!(
        assertion_evidence,
        (
            "assertion.beta37.code-graph".to_owned(),
            "evidence.beta37.code-graph".to_owned(),
            "fact.beta37.code-graph".to_owned(),
            0,
        )
    );

    let replay: (String, String, String, i64, String, String, Vec<u8>) = project
        .query_row(
            "SELECT shard_id, namespace, projection, direct_dependency_bytes,
                    expected_recovered_digest, canonical_replay_source_digest,
                    canonical_replay_source
             FROM graph_publication_replay_v1 WHERE idempotency_key = 'replay.beta37.code'",
            [],
            |row| {
                Ok((
                    row.get(0)?,
                    row.get(1)?,
                    row.get(2)?,
                    row.get(3)?,
                    row.get(4)?,
                    row.get(5)?,
                    row.get(6)?,
                ))
            },
        )
        .expect("read beta37 graph replay source");
    assert_eq!(
        replay,
        (
            format!("project:{PROJECT_ID}"),
            "code-graph".to_owned(),
            "symbols".to_owned(),
            2,
            "sha256:recovered-beta37-code".to_owned(),
            "sha256:source-beta37-code".to_owned(),
            br#"{"symbols":["NativeProvider"]}"#.to_vec(),
        )
    );

    let diagnostic: (
        String,
        String,
        String,
        String,
        String,
        String,
        String,
        String,
        i64,
        i64,
    ) = project
        .query_row(
            "SELECT diagnostic_anchor, generation_id, repository, worktree,
                        reference, source_revision, file_occurrence_id,
                        content_digest, collected_at, persisted_at
                 FROM generation_diagnostics
                 WHERE diagnostic_anchor = 'diagnostic.beta37.code-graph'",
            [],
            |row| {
                Ok((
                    row.get(0)?,
                    row.get(1)?,
                    row.get(2)?,
                    row.get(3)?,
                    row.get(4)?,
                    row.get(5)?,
                    row.get(6)?,
                    row.get(7)?,
                    row.get(8)?,
                    row.get(9)?,
                ))
            },
        )
        .expect("read beta37 source diagnostic");
    assert_eq!(
        diagnostic,
        (
            "diagnostic.beta37.code-graph".to_owned(),
            "generation.beta37.code".to_owned(),
            "repo.beta37".to_owned(),
            "worktree.beta37".to_owned(),
            "refs/heads/main".to_owned(),
            "commit.beta37".to_owned(),
            "file.beta37.native-provider".to_owned(),
            "sha256:file-beta37-native-provider".to_owned(),
            1_740_000_000,
            1_740_000_001,
        )
    );
}

fn assert_beta37_session_authority(profile: &Path) {
    assert_beta37_session_database(&profile.join("user-sessions.db"));
}

fn assert_beta37_session_database(path: &Path) {
    let sessions = open_read_only(path);
    let session: (String, String, String, i64, String, String) = sessions
        .query_row(
            "SELECT provider, session_id, project_key, started_at,
                    transcript_path, metadata_json
             FROM sessions WHERE session_id = 'session.beta37.replacement'",
            [],
            |row| {
                Ok((
                    row.get(0)?,
                    row.get(1)?,
                    row.get(2)?,
                    row.get(3)?,
                    row.get(4)?,
                    row.get(5)?,
                ))
            },
        )
        .expect("read beta37 retained session");
    assert_eq!(
        session,
        (
            "codex".to_owned(),
            "session.beta37.replacement".to_owned(),
            PROJECT_ID.to_owned(),
            1_740_000_000,
            "projects/project.beta37-replacement/sessions/codex-beta37.jsonl".to_owned(),
            r#"{"release":"beta.37","authority":"lcm"}"#.to_owned(),
        )
    );
    let message: (
        String,
        String,
        String,
        i64,
        i64,
        String,
        String,
        i64,
        String,
        String,
    ) = sessions
        .query_row(
            "SELECT provider, message_id, session_id, timestamp, ordinal, text,
                    source_path, source_offset, kind, metadata_json
             FROM session_messages WHERE message_id = 'message.beta37.replacement'",
            [],
            |row| {
                Ok((
                    row.get(0)?,
                    row.get(1)?,
                    row.get(2)?,
                    row.get(3)?,
                    row.get(4)?,
                    row.get(5)?,
                    row.get(6)?,
                    row.get(7)?,
                    row.get(8)?,
                    row.get(9)?,
                ))
            },
        )
        .expect("read beta37 session message");
    assert_eq!(
        message,
        (
            "codex".to_owned(),
            "message.beta37.replacement".to_owned(),
            "session.beta37.replacement".to_owned(),
            1_740_000_001,
            0,
            TRANSCRIPT_TEXT.to_owned(),
            "projects/project.beta37-replacement/sessions/codex-beta37.jsonl".to_owned(),
            0,
            "text".to_owned(),
            r#"{"release":"beta.37"}"#.to_owned(),
        )
    );
    let workflow: (String, String, i64, i64, String) = sessions
        .query_row(
            "SELECT run_id, parent_session_id, started_ts, ended_ts, result_summary
             FROM workflow_runs WHERE run_id = 'workflow.beta37.replacement'",
            [],
            |row| {
                Ok((
                    row.get(0)?,
                    row.get(1)?,
                    row.get(2)?,
                    row.get(3)?,
                    row.get(4)?,
                ))
            },
        )
        .expect("read beta37 workflow receipt");
    assert_eq!(
        workflow,
        (
            "workflow.beta37.replacement".to_owned(),
            "session.beta37.replacement".to_owned(),
            1_740_000_003,
            1_740_000_004,
            "replayed".to_owned(),
        )
    );
    let git_receipt: (String, String, String, i64) = sessions
        .query_row(
            "SELECT receipt_id, publication_prefix, evidence_json, created_at
             FROM git_evidence_publication_outbox
             WHERE receipt_id = 'git-publication.beta37'",
            [],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
        )
        .expect("read beta37 Git receipt");
    assert_eq!(
        git_receipt,
        (
            "git-publication.beta37".to_owned(),
            "beta37/project".to_owned(),
            r#"{"commit":"commit.beta37","source":"code-graph"}"#.to_owned(),
            1_740_000_005,
        )
    );
    let inline_raw: (String, String, i64, String, String) = sessions
        .query_row(
            "SELECT message_id, storage_kind, timestamp, content, metadata_json
             FROM lcm_raw_messages WHERE message_id = 'message.beta37.replacement'",
            [],
            |row| {
                Ok((
                    row.get(0)?,
                    row.get(1)?,
                    row.get(2)?,
                    row.get(3)?,
                    row.get(4)?,
                ))
            },
        )
        .expect("read beta37 inline LCM message");
    assert_eq!(
        inline_raw,
        (
            "message.beta37.replacement".to_owned(),
            "inline".to_owned(),
            1_740_000_001,
            TRANSCRIPT_TEXT.to_owned(),
            r#"{"source":"beta.37","lcm":"retained"}"#.to_owned(),
        )
    );
}

fn assert_beta37_configuration_authority(profile: &Path) {
    let global = open_read_only(&profile.join("global.db"));
    let entries = global
        .prepare(
            "SELECT key, typed_value FROM configuration_entries
             ORDER BY key",
        )
        .expect("prepare beta37 configuration entries")
        .query_map([], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
        })
        .expect("query beta37 configuration entries")
        .collect::<Result<Vec<_>, _>>()
        .expect("collect beta37 configuration entries");
    assert_eq!(
        entries,
        vec![
            (
                "memory.provider_native_enabled.v1".to_owned(),
                BETA37_CONFIGURATION_NATIVE_ENABLED.to_owned(),
            ),
            (
                "memory.provider_ncm_observer.v1".to_owned(),
                BETA37_CONFIGURATION_NCM_OBSERVER.to_owned(),
            ),
            (
                "memory.provider_recall_routing.v1".to_owned(),
                BETA37_CONFIGURATION_RECALL_ROUTING.to_owned(),
            ),
        ]
    );
    assert_eq!(
        fs::read_to_string(profile.join("config.toml")).expect("read beta37 configuration file"),
        "[profile]\nrelease = \"v0.1.0-beta.37\"\nactive_provider = \"tracedecay.native\"\ncredential_ref = \"credential.beta37\"\n\n[settings.native]\nenabled = true\nrecall_scope = \"exact_coding_scope\"\n"
    );
    let retired_credential_table: i64 = global
        .query_row(
            "SELECT COUNT(*) FROM sqlite_master
             WHERE type = 'table' AND name = 'configuration_credential_references'",
            [],
            |row| row.get(0),
        )
        .expect("inspect beta37 credential-reference retirement");
    assert!(
        retired_credential_table == 0
            || global
                .query_row(
                    "SELECT COUNT(*) FROM configuration_credential_references",
                    [],
                    |row| row.get::<_, i64>(0),
                )
                .expect("count empty beta37 credential-reference table")
                == 0,
        "successful replacement must not discard a non-empty credential authority"
    );
}

fn assert_beta37_authorities(profile: &Path, project_store: &Path) {
    assert_beta37_native_provider_state(profile);
    assert_beta37_profile_memory_authority(profile);
    assert_beta37_configuration_authority(profile);
    assert_beta37_session_authority(profile);
    let project_sessions = project_sessions_store_from_graph(project_store);
    assert_beta37_session_database(&project_sessions);
    assert_external_lcm_payload_closure_for(profile, &project_sessions);
    assert_external_lcm_payload_closure(profile);
    assert_beta37_project_authority(project_store);
}

fn project_sessions_store_from_graph(project_store: &Path) -> PathBuf {
    let project_store_root = project_store.parent().expect("project store parent");
    let project_manifest = read_store_manifest(&project_store_root.join(STORE_MANIFEST_FILENAME))
        .expect("read project store manifest for session authority");
    let direct_project_sessions = project_store_root.join("sessions.db");
    if direct_project_sessions.is_file() {
        direct_project_sessions
    } else if project_manifest.data_root == project_store_root
        || project_manifest.data_root.starts_with(project_store_root)
    {
        project_manifest
            .data_root
            .join(&project_manifest.sessions_db_relpath)
    } else {
        // A quarantined V1 tree keeps the source manifest's absolute
        // `data_root`, which still names the live profile path after the
        // coordinator exchanges the directories. Resolve that manifest
        // against the selected local tree so a rollback assertion cannot
        // accidentally reopen the live V2 session shard.
        project_store_root.join(&project_manifest.sessions_db_relpath)
    }
}

/// The complete backup API owns the relational authorities and the project
/// store. First-party opaque roots are copied by the replacement coordinator
/// after this manifest is verified, so a maintenance-only rehearsal must be
/// checked against exactly the entries it promises rather than silently
/// treating an absent opaque root as restored.
fn assert_beta37_backed_up_authorities(restore_root: &Path) {
    assert_beta37_configuration_authority(restore_root);
    assert_beta37_profile_memory_authority(restore_root);
    assert_beta37_session_database(&restore_root.join("user-sessions.db"));
    let restored_project_store = project_store_from_profile(restore_root);
    let restored_project_sessions = project_sessions_store_from_graph(&restored_project_store);
    assert_beta37_session_database(&restored_project_sessions);
    assert_beta37_project_authority(&restored_project_store);
}

/// A rehearsal restores every backup entry byte-for-byte except the project
/// store manifest, whose `data_root` must be rebound to the new profile root.
/// Compare the complete path set as well as each file so a missing opaque
/// payload cannot hide behind a successful SQLite reopen.
fn assert_rehearsed_tree_matches_backup(backup_root: &Path, restore_root: &Path) {
    let manifest = load_and_verify_backup(backup_root).expect("verify rehearsal backup manifest");
    let expected = manifest
        .entries
        .iter()
        .filter(|entry| entry.present)
        .map(|entry| entry.logical_path.clone())
        .collect::<std::collections::BTreeSet<_>>();
    let mut actual = inventory(restore_root)
        .into_keys()
        .collect::<std::collections::BTreeSet<_>>();
    assert!(
        !restore_root
            .join(".tracedecay-profile-rehearsal.json")
            .exists(),
        "rehearsal marker must be removed after publication"
    );
    actual.remove(".tracedecay-profile-rehearsal.json");
    let manifest_paths = expected
        .iter()
        .filter(|path| {
            Path::new(path).file_name().and_then(|name| name.to_str())
                == Some(STORE_MANIFEST_FILENAME)
        })
        .cloned()
        .collect::<Vec<_>>();
    for path in &manifest_paths {
        actual.remove(path);
    }
    let expected_non_manifests = expected
        .iter()
        .filter(|path| {
            !manifest_paths
                .iter()
                .any(|manifest_path| manifest_path == *path)
        })
        .cloned()
        .collect::<std::collections::BTreeSet<_>>();
    assert_eq!(
        actual, expected_non_manifests,
        "rehearsed profile must have the complete backup path closure"
    );
    for path in expected_non_manifests {
        assert_eq!(
            fs::read(backup_root.join(&path)).expect("read backup closure entry"),
            fs::read(restore_root.join(&path)).expect("read restored closure entry"),
            "rehearsed entry differs from the external backup: {path}"
        );
    }
    for path in manifest_paths {
        let backup_manifest = read_store_manifest(&backup_root.join(&path))
            .expect("read backup project store manifest");
        let restored_manifest = read_store_manifest(&restore_root.join(&path))
            .expect("read restored project store manifest");
        assert_eq!(restored_manifest.project_id, backup_manifest.project_id);
        assert_eq!(
            restored_manifest.graph_db_relpath,
            backup_manifest.graph_db_relpath
        );
        assert_eq!(
            restored_manifest.data_root,
            restore_root.join(Path::new(&path).parent().expect("store manifest parent"))
        );
    }
}

fn open_read_only(path: &Path) -> Connection {
    Connection::open_with_flags(
        path,
        OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
    )
    .expect("open read-only replacement preflight database")
}

fn v1_preflight(fixture: &Beta37V1ProfileFixture) -> V1Preflight {
    v1_preflight_paths(&fixture.profile, &fixture.project_store)
}

fn v1_preflight_paths(profile: &Path, project_store: &Path) -> V1Preflight {
    let project = open_read_only(project_store);
    let project_user_version = project
        .query_row("PRAGMA user_version", [], |row| row.get(0))
        .expect("read V1 project version");
    let admission =
        tracedecay_runtime_core::db::migrations::verify_admissible_final_shape_rusqlite(&project)
            .expect_err("released v34 project must require a fresh V2 store");
    assert!(
        matches!(
            &admission,
            tracedecay_domain::errors::TraceDecayError::ResetRequired { .. }
        ),
        "V1 preflight must return typed ResetRequired before interpretation: {admission:?}"
    );
    let global = open_read_only(&profile.join("global.db"));
    let configuration_entries = global
        .query_row("SELECT COUNT(*) FROM configuration_entries", [], |row| {
            row.get(0)
        })
        .expect("read V1 configuration entries");
    let credential_references = global
        .query_row(
            "SELECT COUNT(*) FROM configuration_credential_references",
            [],
            |row| row.get(0),
        )
        .expect("read V1 credential references");
    let native_provider_state = fs::read(profile.join(BETA37_NATIVE_PROVIDER_STATE_PATH))
        .expect("read V1 Native provider state bytes");
    assert_beta37_native_provider_state(profile);
    let sessions = open_read_only(&profile.join("user-sessions.db"));
    let project_facts = project
        .query_row(
            "SELECT COUNT(*) FROM memory_v2_facts WHERE project_id = ?1",
            [PROJECT_ID],
            |row| row.get(0),
        )
        .expect("read V1 project facts");
    let project_graph_replays = project
        .query_row(
            "SELECT COUNT(*) FROM graph_publication_replay_v1 WHERE shard_id = ?1",
            [format!("project:{PROJECT_ID}")],
            |row| row.get(0),
        )
        .expect("read V1 code graph replay rows");
    let session_temporal_version = sessions
        .query_row(
            "SELECT version FROM session_temporal_schema_migrations WHERE name = 'session-temporal'",
            [],
            |row| row.get(0),
        )
        .expect("read V1 session temporal version");
    let lcm_version = sessions
        .query_row(
            "SELECT version FROM session_schema_migrations WHERE name = 'lcm'",
            [],
            |row| row.get(0),
        )
        .expect("read V1 LCM version");
    assert_eq!(
        project_user_version, 34,
        "released beta.37 project fixture must carry the v34 marker"
    );
    assert_eq!(
        session_temporal_version, 3,
        "released beta.37 session fixture must carry temporal v3"
    );
    assert_eq!(
        lcm_version, 8,
        "released beta.37 session fixture must carry LCM v8"
    );
    let retained_session_messages = sessions
        .query_row("SELECT COUNT(*) FROM session_messages", [], |row| {
            row.get(0)
        })
        .expect("read retained V1 session messages");
    let lcm_raw_messages = sessions
        .query_row("SELECT COUNT(*) FROM lcm_raw_messages", [], |row| {
            row.get(0)
        })
        .expect("read V1 LCM raw messages");
    let lcm_external_payloads = sessions
        .query_row("SELECT COUNT(*) FROM lcm_external_payloads", [], |row| {
            row.get(0)
        })
        .expect("read V1 external LCM payloads");
    let workflow_runs = sessions
        .query_row("SELECT COUNT(*) FROM workflow_runs", [], |row| row.get(0))
        .expect("read V1 workflow runs");
    let workflow_agents = sessions
        .query_row("SELECT COUNT(*) FROM workflow_agents", [], |row| row.get(0))
        .expect("read V1 workflow agents");
    let git_publications = sessions
        .query_row(
            "SELECT COUNT(*) FROM git_evidence_publication_outbox",
            [],
            |row| row.get(0),
        )
        .expect("read V1 Git publications");
    V1Preflight {
        project_user_version,
        project_facts,
        project_graph_replays,
        native_provider_state,
        session_temporal_version,
        lcm_version,
        credential_references,
        configuration_entries,
        retained_session_messages,
        lcm_raw_messages,
        lcm_external_payloads,
        workflow_runs,
        workflow_agents,
        git_publications,
    }
}

fn create_backup(fixture: &Beta37V1ProfileFixture) -> PathBuf {
    let lease = tracedecay_runtime_core::lifecycle_lease::acquire_exclusive_for_profile(
        &fixture.profile,
        "beta37 replacement acceptance backup",
    )
    .expect("acquire V1 backup lease");
    create_complete_profile_backup(
        &fixture.profile,
        &fixture.temp.path().join("backups"),
        "backup.beta37.replacement",
        1_740_000_100,
        &lease,
    )
    .expect("create complete beta37 backup")
}

#[cfg(unix)]
fn replacement_ncm_paths(temp: &TempDir) -> (PathBuf, PathBuf) {
    let worker = PathBuf::from(
        std::env::var_os("TRACEDECAY_NCM_WORKER")
            .expect("TRACEDECAY_NCM_WORKER is required for the ignored beta37 replacement journey"),
    );
    let installed = PathBuf::from(std::env::var_os("TRACEDECAY_NCM_REAL_MODEL_ROOT").expect(
        "TRACEDECAY_NCM_REAL_MODEL_ROOT is required for the ignored beta37 replacement journey",
    ));
    assert!(
        worker.is_absolute() && worker.is_file(),
        "TRACEDECAY_NCM_WORKER must be an absolute regular-file path"
    );
    assert!(
        installed.is_absolute(),
        "TRACEDECAY_NCM_REAL_MODEL_ROOT must be an absolute path"
    );
    let model_dir = installed.join("models");
    assert!(
        model_dir.is_dir(),
        "TRACEDECAY_NCM_REAL_MODEL_ROOT must contain a models directory"
    );
    let worker = fs::canonicalize(worker).expect("canonical configured NCM worker");
    let model_dir = fs::canonicalize(model_dir).expect("canonical installed NCM models");
    let state_root = temp.path().join("ncm-state");
    fs::create_dir_all(&state_root).expect("create mutable NCM state root");
    // The production model lifecycle opens the model directory with
    // `O_NOFOLLOW` and rejects a symlink at the root. Copy the verified model
    // tree into the isolated state root so the acceptance uses the same
    // regular-directory admission while keeping mutable worker namespaces
    // physically separate from the installed artifacts.
    copy_ncm_model_tree(&model_dir, &state_root.join("models"));
    assert_ne!(
        fs::canonicalize(&state_root).expect("canonical mutable NCM state root"),
        model_dir,
        "NCM mutable state must remain separate from immutable model artifacts"
    );
    (worker, state_root)
}

#[cfg(unix)]
fn copy_ncm_model_tree(source: &Path, target: &Path) {
    let metadata = fs::symlink_metadata(source).unwrap_or_else(|error| {
        panic!(
            "inspect installed NCM model entry '{}': {error}",
            source.display()
        )
    });
    assert!(
        !metadata.file_type().is_symlink(),
        "installed NCM model tree contains a symlink: {}",
        source.display()
    );
    if metadata.is_dir() {
        fs::create_dir_all(target).unwrap_or_else(|error| {
            panic!(
                "create isolated NCM model directory '{}': {error}",
                target.display()
            )
        });
        let mut entries = fs::read_dir(source)
            .unwrap_or_else(|error| {
                panic!("read installed NCM models '{}': {error}", source.display())
            })
            .map(|entry| entry.expect("read installed NCM model entry"))
            .collect::<Vec<_>>();
        entries.sort_by_key(|entry| entry.file_name());
        for entry in entries {
            copy_ncm_model_tree(&entry.path(), &target.join(entry.file_name()));
        }
    } else {
        assert!(
            metadata.is_file(),
            "installed NCM model tree contains unsupported entry: {}",
            source.display()
        );
        fs::copy(source, target).unwrap_or_else(|error| {
            panic!(
                "copy installed NCM model '{}' to '{}': {error}",
                source.display(),
                target.display()
            )
        });
    }
}

#[cfg(unix)]
fn replacement_cli_path() -> PathBuf {
    if let Some(path) = std::env::var_os("TRACEDECAY_V1_TO_V2_CLI") {
        let path = PathBuf::from(path);
        assert!(
            path.is_file(),
            "TRACEDECAY_V1_TO_V2_CLI must point to the shipped tracedecay binary"
        );
        return path;
    }
    let path = std::env::var_os("CARGO_BIN_EXE_tracedecay").expect(
        "TRACEDECAY_V1_TO_V2_CLI or CARGO_BIN_EXE_tracedecay is required for the ignored beta37 replacement journey",
    );
    let path = PathBuf::from(path);
    assert!(
        path.is_file(),
        "CARGO_BIN_EXE_tracedecay must point to the shipped tracedecay binary"
    );
    path
}

#[cfg(unix)]
fn replacement_v1_harness_path() -> PathBuf {
    let path = std::env::var_os("TRACEDECAY_V1_BETA37_HARNESS").expect(
        "TRACEDECAY_V1_BETA37_HARNESS is required for the ignored beta37 replacement journey",
    );
    let path = PathBuf::from(path);
    assert!(
        path.is_file(),
        "TRACEDECAY_V1_BETA37_HARNESS must point to the released beta.37 service/binary harness"
    );
    path
}

#[cfg(unix)]
fn parse_command_json_object(stdout: &[u8], label: &str) -> Value {
    let stdout = String::from_utf8(stdout.to_vec()).expect("replacement command UTF-8 output");
    // The released service writes diagnostics before its JSON result and the
    // result itself may contain nested objects. Select the complete command
    // envelope, rather than returning the last parseable nested object.
    for (offset, _) in stdout.match_indices('{') {
        if let Ok(value) = serde_json::from_str::<Value>(&stdout[offset..]) {
            if value.get("status").is_some() || value.get("protocol").is_some() {
                return value;
            }
        }
    }
    panic!("{label} did not emit a JSON object: {stdout}");
}

#[cfg(unix)]
fn invoke_v1_release_harness(
    harness: &Path,
    profile_root: &Path,
    project_root: &Path,
    action: &str,
) -> Value {
    let output = Command::new(harness)
        .args([
            "--profile-root",
            profile_root.to_str().expect("V1 profile root UTF-8"),
            "--project-root",
            project_root.to_str().expect("V1 project root UTF-8"),
            "--action",
            action,
            "--json",
        ])
        .env("TRACEDECAY_DATA_DIR", profile_root)
        .env("TRACEDECAY_PROJECT_ROOT", project_root)
        .output()
        .unwrap_or_else(|error| panic!("run released beta.37 harness {action}: {error}"));
    assert!(
        output.status.success(),
        "released beta.37 harness {action} failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let value = parse_command_json_object(&output.stdout, &format!("released beta.37 {action}"));
    assert_eq!(value["status"], "ok", "released beta.37 harness {action}");
    assert_eq!(value["profile_id"], PROFILE_ID);
    assert_eq!(value["project_id"], PROJECT_ID);
    assert_eq!(
        value["action"], action,
        "released beta.37 harness must execute the requested {action} phase"
    );
    let release = value
        .get("release")
        .or_else(|| value.get("version"))
        .and_then(Value::as_str)
        .unwrap_or_else(|| {
            panic!("released beta.37 harness {action} must identify the released binary/service")
        });
    assert!(
        release.contains("beta.37"),
        "released beta.37 harness {action} identified the wrong release: {release}"
    );
    value
}

#[cfg(unix)]
fn invoke_v1_release_observation(
    harness: &Path,
    profile_root: &Path,
    project_root: &Path,
    action: &str,
) -> Value {
    let value = invoke_v1_release_harness(harness, profile_root, project_root, action);
    assert!(
        value.get("session_id").is_some(),
        "released beta.37 harness {action} must expose canonical session identity"
    );
    value
        .get("content")
        .or_else(|| value.get("text"))
        .and_then(Value::as_str)
        .expect("released beta.37 harness content/text");
    for key in ["source", "provenance", "timestamp", "receipt"] {
        assert!(
            value.get(key).is_some(),
            "released beta.37 harness {action} must expose {key} evidence"
        );
    }
    value
}

#[cfg(unix)]
fn v1_process_identity(value: &Value, label: &str) -> (u64, String) {
    let identity = value
        .get("process_identity")
        .or_else(|| value.get("daemon_identity"))
        .unwrap_or_else(|| panic!("{label} must expose the released service process identity"));
    let pid = identity
        .get("pid")
        .and_then(Value::as_u64)
        .unwrap_or_else(|| panic!("{label} process identity must expose a numeric pid"));
    let start = identity
        .get("start_identity")
        .or_else(|| identity.get("process_run_id"))
        .and_then(Value::as_str)
        .unwrap_or_else(|| panic!("{label} process identity must expose a start identity"));
    assert!(pid > 0, "{label} process identity pid must be live");
    (pid, start.to_owned())
}

#[cfg(unix)]
fn assert_v1_process_stopped(value: &Value, label: &str) {
    let exited = value
        .get("exited")
        .or_else(|| value.get("joined"))
        .and_then(Value::as_bool)
        .unwrap_or_else(|| panic!("{label} must expose a joined process exit result"));
    assert!(exited, "{label} released service process must be reaped");
    assert!(
        value.get("exit_error").is_some_and(Value::is_null),
        "{label} must expose a null process exit error after a confirmed reap"
    );
}

#[cfg(unix)]
fn assert_v1_namespace_has_no_v2_bytes(profile: &Path, label: &str) {
    for relative in inventory(profile).into_keys() {
        assert!(
            !relative.contains("tracedecay-v1-to-v2")
                && !relative.contains("memory-observation")
                && !relative.contains("ncm-state"),
            "{label} V1 namespace must not expose V2 provider bytes: {relative}"
        );
    }
}

#[cfg(unix)]
fn assert_v1_observation_matches(
    value: &Value,
    expected_session: &str,
    expected_content: &str,
    label: &str,
) {
    assert_eq!(value["status"], "ok", "{label} status");
    assert_eq!(value["profile_id"], PROFILE_ID, "{label} profile");
    assert_eq!(value["project_id"], PROJECT_ID, "{label} project");
    assert_eq!(value["session_id"], expected_session, "{label} session");
    let text = value
        .get("content")
        .or_else(|| value.get("text"))
        .and_then(Value::as_str)
        .expect("V1 harness content/text");
    assert_eq!(text, expected_content, "{label} content");
    for key in ["source", "provenance", "timestamp", "receipt"] {
        assert!(value.get(key).is_some(), "{label} missing {key} evidence");
    }
}

#[cfg(unix)]
fn invoke_storage_replace_v1(cli: &Path, fixture: &Beta37V1ProfileFixture) -> Value {
    invoke_storage_replace_v1_with_id(cli, fixture, "beta37-cli-replacement")
}

#[cfg(unix)]
fn invoke_storage_replace_v1_with_id(
    cli: &Path,
    fixture: &Beta37V1ProfileFixture,
    backup_id: &str,
) -> Value {
    let backup_parent = fixture.temp.path().join(format!("{backup_id}-backups"));
    fs::create_dir_all(&backup_parent).expect("create CLI backup parent");
    let output = Command::new(cli)
        .current_dir(&fixture.project_root)
        .args([
            "--yes",
            "storage",
            "replace-v1",
            "--profile-root",
            fixture.profile.to_str().expect("profile root UTF-8"),
            "--backup-to",
            backup_parent.to_str().expect("backup parent UTF-8"),
            "--backup-id",
            backup_id,
            "--provider",
            "native",
            "--json",
        ])
        .env("TRACEDECAY_DATA_DIR", &fixture.profile)
        .env("TRACEDECAY_PROJECT_ROOT", &fixture.project_root)
        .output()
        .expect("run shipped storage replace-v1 CLI");
    assert!(
        output.status.success(),
        "shipped storage replace-v1 failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let summary = parse_command_json_object(&output.stdout, "storage replace-v1");
    assert_eq!(summary["protocol"], "tracedecay-v1-to-v2");
    assert_eq!(summary["protocol_version"], 1);
    assert_eq!(summary["provider"], "native");
    assert!(
        summary["operation_id"].as_str().is_some(),
        "replacement summary must carry operation identity"
    );
    assert!(
        summary["backup_root"].as_str().is_some(),
        "replacement summary must carry external backup root"
    );
    assert!(
        summary["preserved_v1_root"].as_str().is_some(),
        "replacement summary must carry preserved V1 root"
    );
    summary
}

/// Kill the shipped replacement process after its atomic publication has
/// exchanged the V1 profile for the V2 target. The next invocation must use
/// the coordinator's durable journal/rehearsal rollback before it can accept
/// any new replacement arguments. This is the process-level crash boundary
/// that an in-process backup rehearsal cannot exercise.
#[cfg(unix)]
fn kill_storage_replace_v1_after_publication(
    cli: &Path,
    fixture: &Beta37V1ProfileFixture,
    backup_id: &str,
) -> Value {
    let backup_parent = fixture.temp.path().join(format!("{backup_id}-backups"));
    fs::create_dir_all(&backup_parent).expect("create faulted CLI backup parent");
    let mut child = Command::new(cli)
        .current_dir(&fixture.project_root)
        .args([
            "--yes",
            "storage",
            "replace-v1",
            "--profile-root",
            fixture.profile.to_str().expect("profile root UTF-8"),
            "--backup-to",
            backup_parent.to_str().expect("backup parent UTF-8"),
            "--backup-id",
            backup_id,
            "--provider",
            "native",
            "--json",
        ])
        .env("TRACEDECAY_DATA_DIR", &fixture.profile)
        .env("TRACEDECAY_PROJECT_ROOT", &fixture.project_root)
        // Keep the process output out of the acceptance pipe while it is
        // deliberately terminated. The durable journal is the evidence.
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("spawn shipped storage replace-v1 for crash boundary");
    let journal_path = fixture.profile.join(".tracedecay-v1-replacement.json");
    let deadline = Instant::now() + Duration::from_secs(180);
    loop {
        if journal_path.is_file() {
            let journal: Value = serde_json::from_slice(
                &fs::read(&journal_path).expect("read faulted replacement journal"),
            )
            .expect("decode faulted replacement journal");
            let phase = journal["phase"]
                .as_str()
                .expect("faulted replacement journal phase");
            if matches!(phase, "source_quarantined" | "published") {
                assert!(
                    fixture
                        .profile
                        .join(".tracedecay-v1-to-v2-manifest.json")
                        .is_file(),
                    "fault boundary must expose the published V2 manifest before restart"
                );
                child
                    .kill()
                    .expect("kill shipped replacement at publication boundary");
                let status = child.wait().expect("wait for killed replacement process");
                assert!(
                    !status.success(),
                    "faulted replacement process must exit unsuccessfully after kill"
                );
                return journal;
            }
        }
        if let Some(status) = child.try_wait().expect("poll shipped replacement process") {
            panic!("shipped replacement completed before the publication crash boundary: {status}");
        }
        if Instant::now() >= deadline {
            let _ = child.kill();
            let _ = child.wait();
            panic!("timed out waiting for the shipped replacement publication journal");
        }
        std::thread::sleep(Duration::from_millis(2));
    }
}

/// Recover an interrupted replacement in a fresh CLI process, then stop the
/// same invocation at argument validation. `handle_replace_v1` performs
/// journal rollback before validating the replacement destination, so the
/// invalid backup id leaves the restored V1 namespace available to the old
/// service for its real boot/observe/recall proof.
#[cfg(unix)]
fn recover_storage_replace_v1_to_v1_namespace(
    cli: &Path,
    fixture: &Beta37V1ProfileFixture,
) -> Value {
    let output = Command::new(cli)
        .current_dir(&fixture.project_root)
        .args([
            "--yes",
            "storage",
            "replace-v1",
            "--profile-root",
            fixture.profile.to_str().expect("profile root UTF-8"),
            "--backup-to",
            fixture
                .temp
                .path()
                .join("recovery-not-published")
                .to_str()
                .expect("recovery backup parent UTF-8"),
            "--backup-id",
            "invalid/after-recovery",
            "--provider",
            "native",
            "--json",
        ])
        .env("TRACEDECAY_DATA_DIR", &fixture.profile)
        .env("TRACEDECAY_PROJECT_ROOT", &fixture.project_root)
        .output()
        .expect("run shipped replacement recovery process");
    assert!(
        !output.status.success(),
        "recovery probe must stop after restoring V1, before a second migration"
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("backup-id") || stderr.contains("backup id"),
        "recovery probe must fail at typed backup-id validation: {stderr}"
    );
    assert!(
        !fixture
            .profile
            .join(".tracedecay-v1-replacement.json")
            .exists(),
        "recovery probe must clear the durable replacement journal"
    );
    json!({"status":"recovered_v1", "stderr":stderr.as_ref()})
}

#[cfg(unix)]
fn project_store_from_profile(profile_root: &Path) -> PathBuf {
    let store_root = profile_root.join("projects").join(PROJECT_ID);
    let manifest = read_store_manifest(&store_root.join(STORE_MANIFEST_FILENAME))
        .expect("read published project store manifest");
    if manifest.data_root == store_root || manifest.data_root.starts_with(&store_root) {
        manifest.data_root.join(manifest.graph_db_relpath)
    } else {
        // The preserved V1 quarantine retains its original absolute
        // `data_root`, so bind the graph path to the local quarantine tree.
        store_root.join(manifest.graph_db_relpath)
    }
}

#[cfg(unix)]
fn project_store_from_backup_root(profile_root: &Path) -> PathBuf {
    let store_root = profile_root.join("projects").join(PROJECT_ID);
    let manifest = read_store_manifest(&store_root.join(STORE_MANIFEST_FILENAME))
        .expect("read backup project store manifest");
    store_root.join(manifest.graph_db_relpath)
}

#[cfg(unix)]
fn project_sessions_store_from_profile_root(profile_root: &Path) -> PathBuf {
    let store_root = profile_root.join("projects").join(PROJECT_ID);
    let manifest = read_store_manifest(&store_root.join(STORE_MANIFEST_FILENAME))
        .expect("read profile project store manifest for session authority");
    let direct_project_sessions = store_root.join("sessions.db");
    if direct_project_sessions.is_file() {
        direct_project_sessions
    } else {
        store_root.join(&manifest.sessions_db_relpath)
    }
}

#[cfg(unix)]
fn assert_complete_backup_manifest(
    backup_root: &Path,
) -> tracedecay_maintenance::profile_backup::CompleteProfileBackupManifest {
    assert_complete_backup_manifest_with_id(backup_root, "beta37-cli-replacement")
}

#[cfg(unix)]
fn assert_complete_backup_manifest_with_id(
    backup_root: &Path,
    expected_backup_id: &str,
) -> tracedecay_maintenance::profile_backup::CompleteProfileBackupManifest {
    let manifest = load_and_verify_backup(backup_root).expect("verify complete replacement backup");
    assert_eq!(manifest.schema_version, 2);
    assert_eq!(manifest.backup_id, expected_backup_id);
    assert_eq!(manifest.source_brain_id, BRAIN_ID);
    assert_eq!(manifest.source_profile_id, PROFILE_ID);
    assert_eq!(manifest.projects.len(), 1);
    assert_eq!(manifest.projects[0].project_id, PROJECT_ID);
    for required in [
        "global.db",
        "user-sessions.db",
        "user-memory.db",
        "projects",
        "enrollment.json",
        "config.toml",
        "migration-inventory",
        "profile-identity.json",
    ] {
        if required == "projects" {
            assert!(
                manifest
                    .entries
                    .iter()
                    .any(|entry| entry.logical_path.starts_with("projects/") && entry.present),
                "projects authority must close at least one concrete project entry"
            );
        } else {
            let entry = manifest
                .entries
                .iter()
                .find(|entry| entry.logical_path == required)
                .unwrap_or_else(|| panic!("backup manifest omitted required authority {required}"));
            assert!(entry.present, "required backup authority {required} absent");
        }
    }
    assert!(
        manifest
            .entries
            .iter()
            .all(|entry| !entry.present || entry.byte_len.is_some() && entry.sha256.is_some()),
        "every present backup entry must carry length and digest"
    );
    manifest
}

#[cfg(unix)]
fn assert_cli_backup_tree_closure(source_root: &Path, backup_root: &Path, label: &str) {
    // The complete backup manifest owns SQLite snapshots and the replacement
    // coordinator extends that verified root with opaque provider/LCM bytes.
    // Compare the resulting path closure to the quarantined V1 tree while
    // accounting for the lifecycle/manifest metadata and database sidecars
    // that are intentionally not copied as serving authorities.
    let reserved = [
        "lifecycle.lock",
        ".tracedecay-v1-replacement.json",
        ".tracedecay-profile-rehearsal.json",
        ".tracedecay-v1-to-v2-manifest.json",
    ];
    let normalize = |root: &Path| {
        inventory(root)
            .into_iter()
            .filter(|(path, _)| {
                !reserved.iter().any(|reserved| path == reserved)
                    && path != "backup-manifest.json"
                    && !is_database_sidecar_path(path)
            })
            .collect::<BTreeMap<_, _>>()
    };
    let source = normalize(source_root);
    let backup = normalize(backup_root);
    assert_eq!(
        source.keys().collect::<Vec<_>>(),
        backup.keys().collect::<Vec<_>>(),
        "{label} backup must close every source authority path"
    );
    for (path, source_bytes) in source {
        if is_sqlite_authority_path(&path) {
            continue;
        }
        assert_eq!(
            source_bytes.as_slice(),
            backup
                .get(&path)
                .expect("normalized backup authority path")
                .as_slice(),
            "{label} backup changed opaque/non-SQLite authority bytes at {path}"
        );
    }
}

#[cfg(unix)]
fn is_database_sidecar_path(path: &str) -> bool {
    let Some(name) = Path::new(path).file_name().and_then(|name| name.to_str()) else {
        return false;
    };
    name.ends_with("-wal")
        || name.ends_with("-shm")
        || name.ends_with("-journal")
        || name.ends_with(".grafeo.wal")
}

#[cfg(unix)]
fn is_sqlite_authority_path(path: &str) -> bool {
    let Some(name) = Path::new(path).file_name().and_then(|name| name.to_str()) else {
        return false;
    };
    matches!(
        name,
        "global.db" | "user-sessions.db" | "user-memory.db" | "tracedecay.db"
    ) || (name == "sessions.db" && path.starts_with("projects/"))
}

#[cfg(unix)]
fn assert_first_party_replacement_manifest(profile: &Path, summary: &Value) {
    let path = profile.join(".tracedecay-v1-to-v2-manifest.json");
    let manifest: Value = serde_json::from_slice(
        &fs::read(&path).expect("read shipped first-party replacement manifest"),
    )
    .expect("decode shipped first-party replacement manifest");
    assert_eq!(manifest["schema_version"], 1);
    assert_eq!(manifest["protocol"], "tracedecay-v1-to-v2");
    assert_eq!(manifest["protocol_version"], 1);
    assert_eq!(manifest["operation_id"], summary["operation_id"]);
    assert_eq!(manifest["provider"], "native");

    let authorities = manifest["authorities"]
        .as_array()
        .expect("replacement manifest authority reports");
    assert_eq!(authorities.len(), 8);
    for authority in authorities {
        for field in [
            "authority",
            "source_digest",
            "target_digest",
            "source_semantic_digest",
            "target_semantic_digest",
            "source_row_digest",
            "target_row_digest",
            "disposition",
        ] {
            assert!(
                authority[field]
                    .as_str()
                    .is_some_and(|value| value.len() > 0),
                "replacement manifest authority must carry {field}"
            );
        }
        assert!(authority["source_entries"].as_u64().is_some());
        assert!(authority["target_entries"].as_u64().is_some());
        assert!(authority["source_rows"].as_u64().is_some());
        assert!(authority["target_rows"].as_u64().is_some());
    }

    // Opaque provider state and external LCM payload objects are outside the
    // eight SQLite authorities. The shipped coordinator's independent
    // manifest must still prove byte/directory closure across the cutover.
    let opaque = &manifest["opaque_manifest"];
    for field in ["source_digest", "target_digest"] {
        assert!(
            opaque[field].as_str().is_some_and(|value| value.len() > 0),
            "replacement opaque manifest must carry a non-empty {field}"
        );
    }
    for field in ["source_entries", "target_entries"] {
        assert!(
            opaque[field].as_u64().is_some(),
            "replacement opaque manifest must carry {field}"
        );
    }
    assert_eq!(opaque["source_digest"], opaque["target_digest"]);
    assert_eq!(opaque["source_entries"], opaque["target_entries"]);
    assert!(
        opaque["source_entries"]
            .as_u64()
            .is_some_and(|entries| entries > 0),
        "replacement opaque manifest must close at least one provider/source artifact"
    );
    let external = &manifest["external_projects"];
    assert_eq!(external["source"], external["target"]);
    assert_eq!(
        external["source"]
            .as_array()
            .expect("replacement external project reports")
            .len(),
        1
    );
}

#[cfg(unix)]
fn public_state_selector(provider_id: &str) -> Value {
    json!({
        "kind": "canonical_session",
        "provider_id": provider_id,
        "registration_revision": 1,
        "canonical_provider_id": "codex",
        "session_id": "session.beta37.replacement"
    })
}

#[cfg(unix)]
async fn invoke_public_provider_control(
    router: &axum::Router,
    operation: RetainedSurfaceOperation,
    body: Value,
    suffix: &str,
) -> (StatusCode, Value) {
    let now = tracedecay_contracts::now_micros().0;
    let request_id = RequestId::new(format!("replacement-control-{suffix}"))
        .expect("public provider-control request id");
    let controls = tracedecay_api::HttpApplicationControls {
        deadline: Deadline::new(UtcMicros(now.saturating_add(60_000_000)))
            .expect("public provider-control deadline"),
        cancellation: CancellationSignal::active(format!("replacement-http-{suffix}"))
            .expect("public provider-control cancellation"),
    };
    let request = Request::builder()
        .method("POST")
        .uri(tracedecay_api::retained_application_route_path(operation))
        .header("content-type", "application/json")
        .extension(request_id)
        .extension(controls)
        .body(Body::from(
            serde_json::to_vec(&body).expect("encode public provider-control body"),
        ))
        .expect("build public provider-control HTTP request");
    let response = router
        .clone()
        .oneshot(request)
        .await
        .expect("invoke public provider-control HTTP route");
    let status = response.status();
    let body = to_bytes(response.into_body(), 1_048_576)
        .await
        .expect("read public provider-control HTTP response");
    (
        status,
        serde_json::from_slice(&body).expect("decode public provider-control HTTP response"),
    )
}

#[cfg(unix)]
fn assert_public_control_success(status: StatusCode, body: &Value, operation: &str) {
    assert_eq!(status, StatusCode::OK, "public {operation} status");
    assert_eq!(body["kind"], "success", "public {operation} envelope");
    assert!(
        body["value"].is_object(),
        "public {operation} must carry a canonical result"
    );
}

#[cfg(unix)]
fn assert_public_provider_unavailable(status: StatusCode, body: &Value) {
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(body["kind"], "problem");
    assert_eq!(body["value"]["problem"]["kind"], "unavailable");
}

#[cfg(unix)]
struct ReplacementNcmInstanceProof(Arc<RustNcmSurface>);

#[cfg(unix)]
impl std::fmt::Debug for ReplacementNcmInstanceProof {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("ReplacementNcmInstanceProof")
    }
}

#[cfg(unix)]
impl ObservationInstanceProofV1 for ReplacementNcmInstanceProof {
    fn prove(
        &self,
        deadline: Instant,
        cancelled: Arc<dyn Fn() -> bool + Send + Sync>,
    ) -> Result<Option<String>, tracedecay_memory_provider_registry::TerminalCode> {
        self.0.prove_provider_instance(deadline, cancelled)
    }
}

#[cfg(unix)]
struct ReplacementNcmLifecycleOwner(Arc<RustNcmWorkerOwner>);

#[cfg(unix)]
fn replacement_lifecycle_deadline(
    deadline_unix_micros: i64,
) -> Result<Instant, ProviderLifecycleOwnerErrorV1> {
    let remaining = deadline_unix_micros
        .checked_sub(tracedecay_contracts::now_micros().0)
        .filter(|remaining| *remaining > 0)
        .ok_or(ProviderLifecycleOwnerErrorV1::DeadlineElapsed)?;
    Instant::now()
        .checked_add(Duration::from_micros(remaining as u64))
        .ok_or(ProviderLifecycleOwnerErrorV1::DeadlineElapsed)
}

#[cfg(unix)]
fn replacement_owner_result<T>(
    result: Result<T, impl Display>,
    deadline: Instant,
) -> Result<T, ProviderLifecycleOwnerErrorV1> {
    result.map_err(|error| {
        if Instant::now() >= deadline {
            ProviderLifecycleOwnerErrorV1::DeadlineElapsed
        } else {
            ProviderLifecycleOwnerErrorV1::Unavailable(error.to_string())
        }
    })
}

#[cfg(unix)]
impl ProviderLifecycleOwnerV1 for ReplacementNcmLifecycleOwner {
    fn start(&self, deadline_unix_micros: i64) -> Result<(), ProviderLifecycleOwnerErrorV1> {
        let deadline = replacement_lifecycle_deadline(deadline_unix_micros)?;
        replacement_owner_result(self.0.start(deadline), deadline)
    }

    fn request_stop(
        &self,
        deadline_unix_micros: i64,
    ) -> Result<bool, ProviderLifecycleOwnerErrorV1> {
        let deadline = replacement_lifecycle_deadline(deadline_unix_micros)?;
        replacement_owner_result(self.0.request_stop(deadline), deadline)
    }

    fn kill(&self, deadline_unix_micros: i64) -> Result<(), ProviderLifecycleOwnerErrorV1> {
        let deadline = replacement_lifecycle_deadline(deadline_unix_micros)?;
        replacement_owner_result(self.0.kill(deadline), deadline)
    }
}

#[cfg(unix)]
fn replacement_ncm_factory(
    owners: Arc<StdMutex<Vec<Arc<RustNcmWorkerOwner>>>>,
    reused_owner: Option<Arc<RustNcmWorkerOwner>>,
) -> crate::retained_owner::NcmRegistrationFactoryV1 {
    Arc::new(
        move |profile_id, worker_binary, state_root, registration_revision, mode, authority| {
            let owner = match reused_owner.as_ref() {
                Some(owner) => Arc::clone(owner),
                None => {
                    let admitted_state_root = StateRoot::new(state_root.clone())
                        .map_err(|error| format!("admit NCM state root: {error}"))?;
                    Arc::new(
                        RustNcmWorkerOwner::new(RustNcmConfig {
                            worker_binary,
                            state_root: admitted_state_root,
                            worker_options: WorkerOptions::default(),
                        })
                        .map_err(|error| format!("construct NCM worker owner: {error}"))?,
                    )
                }
            };
            let surface = Arc::new(
                RustNcmSurface::from_production_worker(Arc::clone(&owner))
                    .map_err(|error| format!("construct production NCM surface: {error}"))?,
            );
            let descriptor = surface.descriptor();
            let provider_instance_id = surface
                .provider_instance_id()
                .map_err(|error| format!("read NCM provider instance identity: {error}"))?;
            let proof = Some(Arc::new(ReplacementNcmInstanceProof(Arc::clone(&surface)))
                as Arc<dyn ObservationInstanceProofV1>);
            let provider =
                NcmProviderAdapter::new(Arc::clone(&surface) as Arc<dyn NcmCognitiveSurface>)
                    .map_err(|error| format!("construct NCM adapter: {error}"))?;
            let provider = match authority {
                Some(authority) => provider.with_admission_authority(authority),
                None => provider,
            };
            let provider: Arc<dyn tracedecay_memory_provider_registry::MemoryProviderV1> =
                Arc::new(provider);
            let registration = ProviderRegistrationV1 {
                provider_id: descriptor.provider_id.clone(),
                provider,
                registration_revision,
                mode,
                execution_shape: ProviderExecutionShapeV1::HostAuthoredInProcess,
                recall_scope_bindings: RecallScopeBindingsV1::from_wire(
                    tracedecay_memory_provider_ncm::NCM_RECALL_SCOPE_BINDINGS
                        .iter()
                        .copied(),
                )
                .map_err(|error| format!("admit NCM recall scope bindings: {error}"))?,
                lifecycle: ProviderLifecycleOwnershipV1::Owned(Arc::new(
                    ReplacementNcmLifecycleOwner(Arc::clone(&owner)),
                )),
            };
            let mount = ObservationProviderMountV1 {
                provider_id: descriptor.provider_id,
                registration_revision,
                provider_instance_id,
                instance_proof: proof,
                host_limits: descriptor.limits,
                state_root: state_root.join("namespaces"),
                journal_file_name: "memory-observation-ncm-journal-v1.sqlite3",
                state_namespace_policy:
                    ObservationStateNamespacePolicyV1::AdapterAttestedExactScope,
            };
            owners
                .lock()
                .map_err(|_| "NCM owner capture mutex was poisoned".to_owned())?
                .push(owner);
            let _ = profile_id;
            Ok((registration, mount))
        },
    )
}

#[cfg(unix)]
#[derive(Clone)]
struct ProductionRouteExecutor {
    service: crate::DaemonInvocationService,
    lsp_registry: Arc<tokio::sync::Mutex<tracedecay_lsp::LspSessionRegistry>>,
    profile_id: UserProfileId,
    project_root: PathBuf,
}

#[cfg(unix)]
impl tracedecay_contracts::ApplicationInvocationExecutor for ProductionRouteExecutor {
    fn invoke(
        &self,
        invocation: tracedecay_contracts::ApplicationInvocation,
    ) -> tracedecay_contracts::ApplicationInvocationFuture<
        '_,
        Result<tracedecay_contracts::ApplicationResponse, tracedecay_contracts::InvocationError>,
    > {
        // The replacement lane publishes retained operations through the
        // registered HTTP route below. Keep the required application-executor
        // bridge explicit: feedback observations still use the production
        // daemon service, while an unclassified application surface is refused
        // before it could bypass the retained route's catalog admission.
        let service = self.service.clone();
        let lsp_registry = Arc::clone(&self.lsp_registry);
        let profile_id = self.profile_id.clone();
        let project_root = self.project_root.clone();
        Box::pin(async move {
            let (context, request) = invocation.into_parts();
            let (request_id, _target, deadline, cancellation) = context.into_parts();
            match request {
                tracedecay_contracts::ApplicationRequest::FeedbackObservation {
                    configuration_digest,
                    observed_at,
                    event,
                } => {
                    let event = serde_json::from_value(event)
                        .map_err(|_| tracedecay_contracts::InvocationError::InvalidRequest)?;
                    service
                        .invoke_with_cancellation_for_profile(
                            &lsp_registry,
                            &profile_id,
                            Some(&project_root),
                            None,
                            None,
                            None,
                            tracedecay_daemon_protocol::DaemonInvocationRequest::feedback_observation(
                                request_id.as_str(),
                                configuration_digest,
                                observed_at,
                                event,
                            ),
                            None,
                        )
                        .await;
                    Ok(tracedecay_contracts::ApplicationResponse::ObservationAccepted)
                }
                tracedecay_contracts::ApplicationRequest::Surface { .. } => {
                    Err(tracedecay_contracts::InvocationError::InvalidRequest)
                }
                tracedecay_contracts::ApplicationRequest::OperationEvents { .. }
                | tracedecay_contracts::ApplicationRequest::OperationCancel { .. } => {
                    let _ = (deadline, cancellation);
                    Err(tracedecay_contracts::InvocationError::Unavailable)
                }
            }
        })
    }
}

#[cfg(unix)]
impl tracedecay_daemon_protocol::DaemonInvocationExecutor for ProductionRouteExecutor {
    fn invoke_controlled(
        &self,
        request: tracedecay_daemon_protocol::DaemonInvocationRequest,
        deadline: Deadline,
        cancellation: CancellationSignal,
        _policy: tracedecay_daemon_protocol::InvocationCancellationPolicy,
    ) -> tracedecay_daemon_protocol::DaemonInvocationExecutorFuture<
        '_,
        Result<
            tracedecay_daemon_protocol::DaemonInvocationResponse,
            tracedecay_daemon_protocol::DaemonInvocationError,
        >,
    > {
        let service = self.service.clone();
        let lsp_registry = Arc::clone(&self.lsp_registry);
        let profile_id = self.profile_id.clone();
        let project_root = self.project_root.clone();
        Box::pin(async move {
            if cancellation.is_cancelled() {
                return Err(
                    tracedecay_daemon_protocol::DaemonInvocationError::Cancelled {
                        stage: tracedecay_contracts::CancellationStage::BeforeAdmission,
                    },
                );
            }
            if tracedecay_daemon_protocol::deadline_remaining(&deadline).is_none() {
                return Err(
                    tracedecay_daemon_protocol::DaemonInvocationError::TimedOut {
                        stage: tracedecay_contracts::CancellationStage::BeforeAdmission,
                    },
                );
            }
            Ok(service
                .invoke_with_cancellation_for_profile(
                    &lsp_registry,
                    &profile_id,
                    Some(&project_root),
                    None,
                    None,
                    None,
                    request,
                    None,
                )
                .await)
        })
    }

    fn observe_feedback(
        &self,
        subject_digest: ManifestDigest,
        observed_at: UtcMicros,
        event: tracedecay_contracts::feedback::observations::FeedbackSourceEventV1,
    ) -> tracedecay_daemon_protocol::DaemonInvocationExecutorFuture<
        '_,
        tracedecay_domain::errors::Result<()>,
    > {
        let service = self.service.clone();
        let lsp_registry = Arc::clone(&self.lsp_registry);
        let profile_id = self.profile_id.clone();
        let project_root = self.project_root.clone();
        Box::pin(async move {
            let request_id = RequestId::new("replacement-feedback-observation")
                .expect("replacement feedback observation request id");
            let _response = service
                .invoke_with_cancellation_for_profile(
                    &lsp_registry,
                    &profile_id,
                    Some(&project_root),
                    None,
                    None,
                    None,
                    tracedecay_daemon_protocol::DaemonInvocationRequest::feedback_observation(
                        request_id.as_str(),
                        subject_digest,
                        observed_at,
                        event,
                    ),
                    None,
                )
                .await;
            Ok(())
        })
    }
}

#[cfg(unix)]
async fn bind_canonical_project_retrieval(
    host: &Arc<crate::retained_owner::ProjectMemoryProviderHostMountV1>,
    runtime: &HostAdmissionTestRuntimeV1,
    graph: &TraceDecay,
    project_root: &Path,
    project_id: &ProjectId,
    profile_id: &UserProfileId,
    session_db: &tracedecay_global_db::RegisteredGlobalDbLeaseV1,
) {
    let registry = runtime
        .registered_database(HostAdmissionScope::Project)
        .expect("registered canonical project session authority");
    let serving_db = graph.db().canonical_database_path();
    let serving = tracedecay_session_runtime::session_retrieval::SessionRetrievalServingIdentityV1::resolve_project(
        project_id.as_str(),
        &serving_db,
        graph.serving_branch(),
        project_root,
        profile_id,
        &session_db.binding().shard_id,
        registry,
    )
    .await
    .expect("resolve canonical project session retrieval identity");
    let root = tracedecay_session_runtime::session_retrieval::DaemonSessionRetrievalRoot::project(
        serving, registry,
    )
    .await
    .expect("mount canonical project session retrieval root");
    let retrieval = Arc::new(
        tracedecay_session_runtime::session_retrieval::DaemonSessionRetrievalService::new_without_refresh_worker(
            session_db.clone(), root,
        )
        .expect("construct canonical project session retrieval service"),
    );
    host.bind_session_retrieval(
        retrieval
            as Arc<dyn tracedecay_session_runtime::session_retrieval::SessionApplicationRetrievalPortV1>,
    )
    .expect("bind canonical project session retrieval to production Native");
}

#[cfg(unix)]
struct MountedReplacementProject {
    runtime: HostAdmissionTestRuntimeV1,
    graph: Arc<TraceDecay>,
    session_db: tracedecay_global_db::RegisteredGlobalDbLeaseV1,
    profile_id: UserProfileId,
    brain_id: BrainId,
    scope: ResolvedScope,
    host: Arc<crate::retained_owner::ProjectMemoryProviderHostMountV1>,
    /// The host composition remains inspectable when a required Native
    /// observation refuses an already-admitted `StagedSession` at the full
    /// publication fence. That refusal is the production settlement: the
    /// daemon must keep the recall binding while declining to publish a
    /// control/observation owner that cannot settle its startup backlog.
    full: Option<Arc<crate::retained_owner::ProjectMemoryProviderFullMountV1>>,
    full_mount_error: Option<String>,
}

#[cfg(unix)]
fn assert_live_replacement_authorities(profile_root: &Path, project: &MountedReplacementProject) {
    // Reopen every migrated authority while the fresh daemon composition is
    // serving recall. This binds the post-restart recall proof to the same
    // durable rows, external LCM object, opaque Native bytes, and project
    // graph that the replacement manifest claimed to preserve.
    assert_beta37_authorities(profile_root, project.graph.db().canonical_database_path());
    assert_eq!(
        project.profile_id.as_str(),
        PROFILE_ID,
        "reopened provider composition profile identity"
    );
    assert_eq!(
        project.brain_id.as_str(),
        BRAIN_ID,
        "reopened brain identity"
    );
    assert_eq!(project.scope.project_id.as_str(), PROJECT_ID);
    assert_eq!(
        &project.session_db.binding().shard_id.scope,
        &tracedecay_store::StoreShardScopeV1::ProjectSessions {
            project_id: ProjectId::new(PROJECT_ID).expect("project identity")
        },
        "reopened canonical project session shard"
    );
}

#[cfg(unix)]
async fn mount_production_replacement_project(
    profile_root: &Path,
    project_root: &Path,
    project_id: &ProjectId,
    selection: tracedecay_domain::configuration::MemoryProviderSelectionV1,
    ncm_observer: tracedecay_domain::configuration::MemoryProviderNcmObserverV1,
    routing: tracedecay_domain::configuration::MemoryProviderRecallRoutingV1,
    factory: Option<crate::retained_owner::NcmRegistrationFactoryV1>,
) -> MountedReplacementProject {
    let expected_active_provider = selection.active_provider();
    let runtime =
        HostAdmissionTestRuntimeV1::project(profile_root, project_root, project_id.clone())
            .await
            .expect("reopen published profile through host-admission runtime");
    let session_db = runtime
        .registered_database_arc(HostAdmissionScope::Project)
        .expect("registered project session database lease");
    let graph = Arc::new(
        runtime
            .open_project_graph_for_test(project_root, TraceDecayOpenOptions::default())
            .await
            .expect("reopen published project graph through production runtime"),
    );
    let profile_id = session_db.binding().shard_id.profile_id.clone();
    let identity = tracedecay_daemon_identity::profile_identity::load_existing(profile_root)
        .expect("load published profile identity");
    let brain_id = identity.brain_id().clone();
    let scope = tracedecay_code_index_runtime::resolved_scope_for_project(project_root, project_id)
        .expect("resolve production project scope from checkout");
    // The composition root constructs Native through its production
    // `NativeSessionRetrievalMountV1::for_project` binding. This acceptance
    // only supplies the admitted canonical retrieval service after that mount
    // has created the provider owner; it never constructs the dormant Native
    // application port directly.
    let host = crate::retained_owner::mount_project_memory_provider_host(
        crate::retained_owner::ProjectMemoryProviderHostInputsV1 {
            activation: selection,
            ncm_observer,
            graph: Arc::clone(&graph),
            canonical_project_path: project_root.to_path_buf(),
            profile_id: profile_id.clone(),
            scope: scope.clone(),
            authoritative_project_id: project_id.clone(),
            store_data_root: graph.store_layout().data_root.clone(),
            recall_routing: routing.clone(),
            ncm_registration_factory: factory,
            native_port_interposition: None,
        },
    )
    .await
    .expect("compose production project memory-provider host");
    let selected_registration = host
        .registry()
        .and_then(|registry| registry.selected_registration());
    match expected_active_provider {
        Some(tracedecay_domain::configuration::MemoryProviderKindV1::Native) => {
            let registration = selected_registration.expect("selected Native registration");
            assert_eq!(registration.provider_id.as_str(), "tracedecay.native");
            assert!(
                !registration.requires_common_advisory_profile,
                "canonical Native activation must not claim the provider-local common profile"
            );
        }
        Some(tracedecay_domain::configuration::MemoryProviderKindV1::Ncm) => {
            let registration = selected_registration.expect("selected NCM registration");
            assert_eq!(registration.provider_id.as_str(), "ncm");
            assert!(
                registration.requires_common_advisory_profile,
                "active injected NCM must retain the complete common profile requirement"
            );
        }
        None => assert!(
            selected_registration.is_none(),
            "observer-only composition must not publish a selected registration"
        ),
    }
    bind_canonical_project_retrieval(
        &host,
        &runtime,
        &graph,
        project_root,
        project_id,
        &profile_id,
        &session_db,
    )
    .await;
    let cancellation = HostCancellationToken::new();
    let full_result = crate::retained_owner::mount_project_memory_provider_full(
        &host,
        crate::retained_owner::ProjectMemoryProviderFullMountInputsV1 {
            graph: Arc::clone(&graph),
            canonical_project_path: project_root.to_path_buf(),
            profile_id: profile_id.clone(),
            brain_id: brain_id.clone(),
            scope: scope.clone(),
            authoritative_project_id: project_id.clone(),
            session_db: session_db.clone(),
            configuration_digest: ManifestDigest::new(format!("sha256:{}", "0".repeat(64)))
                .expect("replacement configuration digest"),
        },
        &cancellation,
    )
    .await;
    let (full, full_mount_error) = match full_result {
        Ok(full) => (Some(full), None),
        Err(error) => (None, Some(error)),
    };
    MountedReplacementProject {
        runtime,
        graph,
        session_db,
        profile_id,
        brain_id,
        scope,
        host,
        full,
        full_mount_error,
    }
}

#[cfg(unix)]
async fn activate_production_replacement_project(
    project: &MountedReplacementProject,
) -> Result<(), String> {
    let cancellation = HostCancellationToken::new();
    let full = project.full.as_ref().ok_or_else(|| {
        project
            .full_mount_error
            .clone()
            .unwrap_or_else(|| "production full provider mount unavailable".to_owned())
    })?;
    full.activate_after_publication(project.session_db.observation_store(), &cancellation)
        .await
}

#[cfg(unix)]
async fn shutdown_production_replacement_project(project: &MountedReplacementProject) {
    let Some(full) = project.full.as_ref() else {
        // A required Native startup refusal already shut down every partial
        // journey before returning from the production full mount. There is
        // no retained worker left for this acceptance helper to stop.
        return;
    };
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    let mut failures = Vec::new();
    for journey in full.observation_journeys() {
        failures.extend(journey.shutdown(deadline).await);
    }
    assert_eq!(
        failures,
        Vec::<String>::new(),
        "production journey shutdown failures"
    );
}

#[cfg(unix)]
async fn production_cognitive_recall(
    host: &Arc<crate::retained_owner::ProjectMemoryProviderHostMountV1>,
    scope: &ResolvedScope,
    query: &str,
    suffix: &str,
) -> tracedecay_contracts::memory::CognitiveRecallResult {
    let mount = host
        .cognitive_recall_mount()
        .expect("production cognitive recall mount");
    let port = mount
        .port_for_session("session.beta37.replacement")
        .expect("production canonical session recall port");
    let signal = CancellationSignal::active(format!("replacement-recall-{suffix}"))
        .expect("production recall cancellation signal");
    let request_id = RequestId::new(format!("replacement-recall-{suffix}"))
        .expect("production recall request id");
    let deadline = Deadline::new(UtcMicros(
        tracedecay_contracts::now_micros()
            .0
            .saturating_add(60_000_000),
    ))
    .expect("production recall deadline");
    let request = tracedecay_contracts::memory::CognitiveRecallRequest::new(
        scope.clone(),
        request_id,
        deadline,
        signal.context(),
        query,
        4,
    )
    .expect("production cognitive recall request");
    let outcome = port
        .recall_admitted(request, &signal)
        .await
        .expect("production cognitive recall");
    assert_eq!(outcome.result.scope(), scope);
    assert_eq!(outcome.result.degradation(), None);
    outcome.result
}

#[cfg(unix)]
#[derive(Clone, Debug, PartialEq, Eq)]
struct RecallSnapshot {
    provider_id: String,
    registration_revision: u64,
    candidate_id: String,
    stable_reference: String,
    content: String,
    provenance: Value,
}

#[cfg(unix)]
fn snapshot_recall(
    result: &tracedecay_contracts::memory::CognitiveRecallResult,
    expected_provider: &str,
    expected_text: &str,
    scope: &ResolvedScope,
    label: &str,
) -> RecallSnapshot {
    assert_eq!(
        result.provider().provider_id(),
        expected_provider,
        "{label} provider"
    );
    assert_eq!(
        result.provider().registration_revision(),
        1,
        "{label} registration"
    );
    assert_eq!(result.scope(), scope, "{label} scope");
    let candidate = result
        .candidates()
        .iter()
        .find(|candidate| candidate.content().contains(expected_text))
        .unwrap_or_else(|| panic!("{label} did not return the beta37 canonical content"));
    let stable_reference = candidate
        .stable_reference()
        .expect("production recall candidate stable reference")
        .to_owned();
    assert_ne!(stable_reference, "", "{label} stable identity");
    assert_ne!(candidate.candidate_id(), "", "{label} candidate identity");
    assert!(
        matches!(
            candidate.provenance(),
            tracedecay_contracts::memory::CognitiveRecallProvenance::Available { .. }
        ),
        "{label} candidate provenance must remain available"
    );
    RecallSnapshot {
        provider_id: result.provider().provider_id().to_owned(),
        registration_revision: result.provider().registration_revision(),
        candidate_id: candidate.candidate_id().to_owned(),
        stable_reference,
        content: candidate.content().to_owned(),
        provenance: serde_json::to_value(candidate.provenance())
            .expect("serialize recall provenance"),
    }
}

#[cfg(unix)]
async fn register_public_replacement_route(
    profile_id: UserProfileId,
    project_root: &Path,
    project_id: ProjectId,
    scope: ResolvedScope,
    full: Option<&Arc<crate::retained_owner::ProjectMemoryProviderFullMountV1>>,
) -> axum::Router {
    let service = crate::DaemonInvocationService::default();
    let actor = ActorId::new("codex").expect("replacement route actor");
    let application_operations = [
        RetainedSurfaceOperation::ProviderHealth,
        RetainedSurfaceOperation::ProviderInspection,
        RetainedSurfaceOperation::ProviderMaintenance,
    ]
    .into_iter()
    .map(|operation| {
        tracedecay_contracts::retained_surface_application_operation(operation)
            .expect("provider-control application operation")
    })
    .collect::<Vec<_>>();
    let grant = CapabilityGrantSnapshot::new(
        CapabilityGrantId::new("grant.beta37.replacement").expect("replacement grant id"),
        1,
        ManifestDigest::new(format!("sha256:{}", "b".repeat(64)))
            .expect("replacement grant digest"),
        actor.clone(),
        UtcMicros(1),
        UtcMicros(i64::MAX - 1),
        scope.clone(),
        application_operations
            .iter()
            .map(|operation| operation.capability_id().clone())
            .collect(),
        application_operations
            .iter()
            .map(|operation| operation.use_case_id().clone())
            .collect(),
        DisclosureClass::Sensitive,
    )
    .expect("replacement route capability grant");
    let ports = Arc::new(match full {
        Some(full) => {
            RetainedSurfacePortsV1::default().with_provider_control(full.provider_control_mount())
        }
        None => RetainedSurfacePortsV1::default(),
    });
    crate::DaemonRetainedRuntimeRegistrar::new(&service)
        .register(
            profile_id.clone(),
            project_root.to_path_buf(),
            scope,
            actor,
            grant,
            ports,
        )
        .await
        .expect("register production retained provider-control route");
    let executor = Arc::new(ProductionRouteExecutor {
        service: service.clone(),
        lsp_registry: Arc::new(tokio::sync::Mutex::new(
            tracedecay_lsp::LspSessionRegistry::default(),
        )),
        profile_id,
        project_root: project_root.to_path_buf(),
    });
    // The public assembly wires the retained `router_with_executor`; its
    // handlers reach the registered `invoke_registered_http` boundary before
    // calling this daemon invocation service.
    crate::application_surface::assemble_http_application_router(
        executor,
        service.operation_events(),
        project_id,
    )
    .expect("assemble production public HTTP application router")
}

#[cfg(unix)]
fn stop_ncm_owner(owner: &Arc<RustNcmWorkerOwner>, label: &str) {
    let stopped = owner
        .request_stop(Instant::now() + Duration::from_secs(30))
        .unwrap_or_else(|error| panic!("{label}: stop NCM owner: {error}"));
    assert!(
        stopped,
        "{label}: NCM owner stop must confirm child reaping"
    );
    assert_eq!(owner.worker_pid(), None, "{label}: NCM child must be gone");
}

#[cfg(unix)]
fn kill_ncm_owner(owner: &Arc<RustNcmWorkerOwner>, label: &str) {
    owner
        .kill(Instant::now() + Duration::from_secs(30))
        .unwrap_or_else(|error| panic!("{label}: kill NCM owner: {error}"));
    assert_eq!(owner.worker_pid(), None, "{label}: NCM child must be gone");
}

#[cfg(unix)]
fn assert_provider_journal(path: &Path, expected_receipts: i64, label: &str) {
    assert!(path.is_file(), "{label} journal must be durable");
    let connection = open_read_only(path);
    let receipts: i64 = connection
        .query_row(
            "SELECT COUNT(*) FROM tdmem_observation_receipt_v1",
            [],
            |row| row.get(0),
        )
        .unwrap_or_else(|error| panic!("{label} journal receipt query: {error}"));
    assert_eq!(
        receipts, expected_receipts,
        "{label} journal receipt count must match the selected settlement"
    );
    let terminal_rows: i64 = connection
        .query_row(
            "SELECT COUNT(*) FROM tdmem_observation_receipt_v1 WHERE committed_effect IS NOT NULL",
            [],
            |row| row.get(0),
        )
        .unwrap_or_else(|error| panic!("{label} journal terminal query: {error}"));
    assert_eq!(
        terminal_rows, receipts,
        "{label} every receipt must be terminal"
    );
}

#[cfg(unix)]
fn assert_native_staged_session_refusal(project: &MountedReplacementProject, label: &str) {
    assert!(
        project.full.is_some(),
        "{label} must retain the production observation/control mount"
    );
    let journal = project
        .graph
        .store_layout()
        .data_root
        .join("memory-observation-journal-v1.sqlite3");
    assert_native_staged_session_refusal_journal(&journal, label);
}

#[cfg(unix)]
fn assert_native_staged_session_refusal_journal(path: &Path, label: &str) {
    assert!(path.is_file(), "{label} Native journal must be durable");
    let connection = open_read_only(path);
    let (state, attempt_number, outcome, committed_effect, summary, provider_instance): (
        String,
        i64,
        String,
        String,
        String,
        Option<String>,
    ) = connection
        .query_row(
            "SELECT d.state, d.attempt_number, r.outcome, r.committed_effect, \
                    r.provider_effect_summary_json, r.provider_instance_id \
               FROM tdmem_observation_delivery_v1 d \
               JOIN tdmem_observation_receipt_v1 r \
                 ON r.observation_id = d.observation_id \
              ORDER BY r.attempt_number DESC LIMIT 1",
            [],
            |row| {
                Ok((
                    row.get(0)?,
                    row.get(1)?,
                    row.get(2)?,
                    row.get(3)?,
                    row.get(4)?,
                    row.get(5)?,
                ))
            },
        )
        .unwrap_or_else(|error| panic!("{label} Native refusal receipt query: {error}"));
    assert_eq!(state, "rejected", "{label} Native refusal state");
    assert!(
        attempt_number >= 1,
        "{label} Native refusal must retain the consumed attempt number"
    );
    assert_eq!(
        outcome, "rejected_extension_unsupported",
        "{label} Native refusal must settle as unsupported"
    );
    assert_eq!(
        committed_effect, "none",
        "{label} Native StagedSession refusal must prove no provider effect"
    );
    assert!(
        provider_instance
            .as_deref()
            .is_some_and(|value| !value.is_empty()),
        "{label} Native refusal must retain the answering provider instance"
    );
    let summary: Value = serde_json::from_str(&summary)
        .unwrap_or_else(|error| panic!("{label} Native refusal summary JSON: {error}"));
    assert_eq!(
        summary.get("no_effect_reason").and_then(Value::as_str),
        Some("native.staged_session_not_required"),
        "{label} Native refusal must retain the provider diagnostic"
    );
}

#[cfg(unix)]
async fn wait_for_native_staged_session_refusal(project: &MountedReplacementProject, label: &str) {
    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        let journal = project
            .graph
            .store_layout()
            .data_root
            .join("memory-observation-journal-v1.sqlite3");
        if journal.is_file()
            && open_read_only(&journal)
                .query_row(
                    "SELECT 1 FROM tdmem_observation_receipt_v1 \
                      WHERE outcome = 'rejected_extension_unsupported' \
                        AND provider_effect_summary_json LIKE '%native.staged_session_not_required%' \
                      LIMIT 1",
                    [],
                    |row| row.get::<_, i64>(0),
                )
                .is_ok()
        {
            assert_native_staged_session_refusal(project, label);
            return;
        }
        assert!(
            Instant::now() < deadline,
            "{label} timed out waiting for production Native StagedSession refusal"
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

#[cfg(unix)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn beta37_incompatible_project_refusal_is_typed_and_does_not_mutate_authorities() {
    let fixture = Beta37V1ProfileFixture::create().await;
    let before_profile = fixture.inventory();
    let before_project = inventory(&fixture.project_root);
    let before_authorities = v1_preflight(&fixture);
    assert_beta37_authorities(&fixture.profile, &fixture.project_store);

    let error = tracedecay_runtime_core::db::migrations::verify_admissible_final_shape_rusqlite(
        &open_read_only(&fixture.project_store),
    )
    .expect_err("beta37 project must be refused before interpretation");
    assert!(
        matches!(
            error,
            tracedecay_domain::errors::TraceDecayError::ResetRequired { .. }
        ),
        "incompatible V1 project refusal must remain typed: {error:?}"
    );
    let observed_preflight = v1_preflight(&fixture);
    assert_eq!(observed_preflight, before_authorities);
    assert_eq!(inventory(&fixture.profile), before_profile);
    assert_eq!(inventory(&fixture.project_root), before_project);
    assert_beta37_authorities(&fixture.profile, &fixture.project_store);
}

#[cfg(unix)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn beta37_complete_backup_faults_preserve_authorities_and_rehearse_exactly() {
    let fixture = Beta37V1ProfileFixture::create().await;
    let before_profile = fixture.inventory();
    let before_project = inventory(&fixture.project_root);
    assert_beta37_authorities(&fixture.profile, &fixture.project_store);
    let backup_root = create_backup(&fixture);
    let manifest = load_and_verify_backup(&backup_root).expect("verify beta37 complete backup");
    assert_eq!(manifest.source_profile_id, PROFILE_ID);
    assert_eq!(manifest.source_brain_id, BRAIN_ID);
    assert_eq!(manifest.projects.len(), 1);
    assert_eq!(manifest.projects[0].project_id, PROJECT_ID);

    for (fault, suffix) in [
        ("before_rename", "before-rename"),
        ("after_rename_before_parent_sync", "after-rename"),
        ("after_parent_sync_before_marker_removal", "after-marker"),
    ] {
        let restore_root = fixture.temp.path().join(format!("fault-restore-{suffix}"));
        set_rehearsal_publication_fault_for_test(fault);
        let error = rehearse_complete_profile_backup(&backup_root, &restore_root)
            .expect_err("faulted backup rehearsal must return typed unavailable");
        assert!(
            matches!(error, ProfileBackupError::Unavailable { .. }),
            "{fault} must be a typed unavailable publication failure: {error:?}"
        );
        set_rehearsal_publication_fault_for_test("");
        rehearse_complete_profile_backup(&backup_root, &restore_root)
            .expect("rehearsal retry after publication fault");
        assert_rehearsed_tree_matches_backup(&backup_root, &restore_root);
        assert_beta37_backed_up_authorities(&restore_root);
    }
    assert_eq!(inventory(&fixture.profile), before_profile);
    assert_eq!(inventory(&fixture.project_root), before_project);
    assert_beta37_authorities(&fixture.profile, &fixture.project_store);
}

#[cfg(unix)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires the shipped V2 CLI, released beta.37 harness, NCM worker, and pinned offline model"]
async fn beta37_released_native_ncm_native_rollback_journey() {
    let cli = replacement_cli_path();
    let harness = replacement_v1_harness_path();
    let fixture = Beta37V1ProfileFixture::create().await;
    let (worker_binary, ncm_state_root) = replacement_ncm_paths(&fixture.temp);

    // The old release is installed and started by its own service harness;
    // this keeps the pre-migration evidence tied to a real V1 process rather
    // than to a transcript-shaped fixture response.
    let baseline_install =
        invoke_v1_release_harness(&harness, &fixture.profile, &fixture.project_root, "install");
    let baseline_start =
        invoke_v1_release_harness(&harness, &fixture.profile, &fixture.project_root, "start");
    let baseline_start_identity = v1_process_identity(&baseline_start, "released V1 start");
    let baseline_observe =
        invoke_v1_release_observation(&harness, &fixture.profile, &fixture.project_root, "observe");
    let baseline_recall =
        invoke_v1_release_observation(&harness, &fixture.profile, &fixture.project_root, "recall");
    assert_v1_observation_matches(
        &baseline_recall,
        "session.beta37.replacement",
        TRANSCRIPT_TEXT,
        "released V1 baseline recall",
    );
    assert_eq!(baseline_install["project_id"], PROJECT_ID);
    assert_eq!(baseline_start["project_id"], PROJECT_ID);
    assert_eq!(baseline_observe["project_id"], PROJECT_ID);
    let baseline_stop =
        invoke_v1_release_harness(&harness, &fixture.profile, &fixture.project_root, "stop");
    assert_v1_process_stopped(&baseline_stop, "released V1 stop before replacement");

    // The released service may append an observation receipt while proving its
    // host route. Preserve the exact post-service V1 namespace the coordinator
    // is about to quarantine and compare that tree with the CLI's preserved root.
    let source_profile_inventory = profile_authority_inventory(&fixture.profile);
    let source_project_inventory = inventory(&fixture.project_root);
    let source_project_store = fixture.project_store.clone();
    let source_preflight = v1_preflight(&fixture);
    assert_beta37_authorities(&fixture.profile, &source_project_store);

    let summary = invoke_storage_replace_v1(&cli, &fixture);
    let backup_root = PathBuf::from(
        summary["backup_root"]
            .as_str()
            .expect("replacement backup root")
            .to_owned(),
    );
    let preserved_v1_root = PathBuf::from(
        summary["preserved_v1_root"]
            .as_str()
            .expect("preserved V1 root")
            .to_owned(),
    );
    assert!(backup_root.is_dir(), "CLI backup root must be durable");
    assert!(
        preserved_v1_root.is_dir(),
        "CLI preserved V1 root must be durable"
    );
    assert_complete_backup_manifest(&backup_root);
    assert_first_party_replacement_manifest(&fixture.profile, &summary);
    let backup_project_store = project_store_from_backup_root(&backup_root);
    assert_beta37_project_authority(&backup_project_store);
    let backup_project_sessions = project_sessions_store_from_profile_root(&backup_root);
    assert_beta37_session_database(&backup_project_sessions);
    assert_external_lcm_payload_closure_for(&backup_root, &backup_project_sessions);
    assert_beta37_native_provider_state(&backup_root);
    assert_external_lcm_payload_closure(&backup_root);
    assert_eq!(
        profile_authority_inventory(&preserved_v1_root),
        source_profile_inventory,
        "preserved V1 authorities must equal the pre-cutover bytes"
    );
    assert_cli_backup_tree_closure(&preserved_v1_root, &backup_root, "CLI replacement backup");
    assert_eq!(inventory(&fixture.project_root), source_project_inventory);
    let preserved_project_store = project_store_from_profile(&preserved_v1_root);
    assert_eq!(
        v1_preflight(&Beta37V1ProfileFixture {
            temp: tempfile::tempdir().expect("temporary preflight wrapper"),
            profile: preserved_v1_root.clone(),
            project_root: fixture.project_root.clone(),
            project_store: preserved_project_store.clone(),
        }),
        source_preflight
    );
    // The wrapper above is used only for the value-shaped preflight; the
    // authority assertions below read the actual preserved root and checkout.
    assert_beta37_authorities(&preserved_v1_root, &preserved_project_store);
    assert_external_lcm_payload_closure(&preserved_v1_root);

    let live_project_store = project_store_from_profile(&fixture.profile);
    assert_beta37_project_authority(&live_project_store);
    assert_beta37_session_authority(&fixture.profile);
    assert_beta37_configuration_authority(&fixture.profile);
    assert_beta37_native_provider_state(&fixture.profile);
    assert_eq!(
        inventory(&fixture.project_root),
        source_project_inventory,
        "migration must not mutate the external project checkout"
    );

    // Native is mounted through the daemon project composition and receives
    // the canonical session service resolved from the registered project shard.
    let native_routing = tracedecay_domain::configuration::MemoryProviderRecallRoutingV1 {
        active_provider: Some("tracedecay.native".to_owned()),
        ..Default::default()
    };
    let native_selection = tracedecay_domain::configuration::MemoryProviderSelectionV1::resolve(
        true,
        &tracedecay_domain::configuration::MemoryProviderNcmObserverV1::Disabled {},
        &native_routing,
    )
    .expect("resolve Native production selection");
    let native_project = mount_production_replacement_project(
        &fixture.profile,
        &fixture.project_root,
        &ProjectId::new(PROJECT_ID).expect("project id"),
        native_selection,
        tracedecay_domain::configuration::MemoryProviderNcmObserverV1::Disabled {},
        native_routing,
        None,
    )
    .await;
    activate_production_replacement_project(&native_project)
        .await
        .expect("activate production Native observation journey");
    let native_router = register_public_replacement_route(
        native_project.profile_id.clone(),
        &fixture.project_root,
        ProjectId::new(PROJECT_ID).expect("project id"),
        native_project.scope.clone(),
        native_project.full.as_ref(),
    )
    .await;
    let (native_health_status, native_health_body) = invoke_public_provider_control(
        &native_router,
        RetainedSurfaceOperation::ProviderHealth,
        json!({
            "state": public_state_selector("tracedecay.native"),
            "requested_checks": ["protocol", "state", "persistence"]
        }),
        "native-health",
    )
    .await;
    assert_public_control_success(native_health_status, &native_health_body, "Native health");
    let native_result = production_cognitive_recall(
        &native_project.host,
        &native_project.scope,
        TRANSCRIPT_TEXT,
        "native-before-ncm",
    )
    .await;
    let native_snapshot = snapshot_recall(
        &native_result,
        "tracedecay.native",
        TRANSCRIPT_TEXT,
        &native_project.scope,
        "Native before NCM",
    );
    assert_live_replacement_authorities(&fixture.profile, &native_project);
    let native_journal = native_project
        .graph
        .store_layout()
        .data_root
        .join("memory-observation-journal-v1.sqlite3");
    assert_provider_journal(&native_journal, 0, "Native production");
    shutdown_production_replacement_project(&native_project).await;
    drop(native_router);
    drop(native_project);

    // NCM receives the canonical session observation through the durable
    // project observation store. Native remains admitted as a production
    // observer during this cutover, so its typed StagedSession refusal is
    // recorded alongside the authoritative NCM receipt without fabricating a
    // Native-side effect for an unsupported observation.
    let ncm_routing = tracedecay_domain::configuration::MemoryProviderRecallRoutingV1 {
        active_provider: Some("ncm".to_owned()),
        ..Default::default()
    };
    let ncm_observer = tracedecay_domain::configuration::MemoryProviderNcmObserverV1::Enabled {
        worker_binary: worker_binary.clone(),
        state_root: ncm_state_root.clone(),
    };
    let ncm_selection = tracedecay_domain::configuration::MemoryProviderSelectionV1::resolve(
        true,
        &ncm_observer,
        &ncm_routing,
    )
    .expect("resolve NCM production selection");
    let first_owners = Arc::new(StdMutex::new(Vec::new()));
    let ncm_project = mount_production_replacement_project(
        &fixture.profile,
        &fixture.project_root,
        &ProjectId::new(PROJECT_ID).expect("project id"),
        ncm_selection,
        ncm_observer.clone(),
        ncm_routing.clone(),
        Some(replacement_ncm_factory(Arc::clone(&first_owners), None)),
    )
    .await;
    let session_store = ncm_project.session_db.observation_store();
    persist_replacement_observation(
        &session_store,
        &ProjectId::new(PROJECT_ID).expect("project id"),
        &SessionId::new("session.beta37.replacement").expect("canonical replacement session"),
        TRANSCRIPT_TEXT,
    )
    .await;
    activate_production_replacement_project(&ncm_project)
        .await
        .expect("activate production NCM observation journey");
    let first_owner = first_owners
        .lock()
        .expect("NCM owner capture mutex")
        .last()
        .cloned()
        .expect("production NCM factory owner");
    let ncm_router = register_public_replacement_route(
        ncm_project.profile_id.clone(),
        &fixture.project_root,
        ProjectId::new(PROJECT_ID).expect("project id"),
        ncm_project.scope.clone(),
        ncm_project.full.as_ref(),
    )
    .await;
    for (operation, body, name) in [
        (
            RetainedSurfaceOperation::ProviderHealth,
            json!({
                "state": public_state_selector("ncm"),
                "requested_checks": ["protocol", "state", "persistence"]
            }),
            "NCM health",
        ),
        (
            RetainedSurfaceOperation::ProviderInspection,
            json!({
                "state": public_state_selector("ncm"),
                "selection": {"view": "state_summary"},
                "maximum_items": 4,
                "maximum_bytes": 65536
            }),
            "NCM inspection",
        ),
        (
            RetainedSurfaceOperation::ProviderMaintenance,
            json!({
                "state": public_state_selector("ncm"),
                "task": "consolidate",
                "maximum_items": 4,
                "maximum_bytes": 65536,
                "maximum_duration_millis": 5000,
                "dry_run": false
            }),
            "NCM maintenance",
        ),
    ] {
        let (status, body) =
            invoke_public_provider_control(&ncm_router, operation, body, name).await;
        assert_public_control_success(status, &body, name);
    }
    let first_ncm_result = production_cognitive_recall(
        &ncm_project.host,
        &ncm_project.scope,
        TRANSCRIPT_TEXT,
        "ncm-first",
    )
    .await;
    let ncm_snapshot = snapshot_recall(
        &first_ncm_result,
        "ncm",
        TRANSCRIPT_TEXT,
        &ncm_project.scope,
        "NCM first",
    );
    assert_live_replacement_authorities(&fixture.profile, &ncm_project);
    wait_for_native_staged_session_refusal(&ncm_project, "Native observer during NCM cutover")
        .await;
    assert_provider_journal(&native_journal, 1, "Native observer cutover");
    let first_pid = first_owner
        .worker_pid()
        .expect("NCM worker process after recall");
    let first_incarnation = first_owner
        .worker_incarnation()
        .expect("NCM worker incarnation after recall");
    let ncm_journal = ncm_project
        .graph
        .store_layout()
        .data_root
        .join("memory-observation-ncm-journal-v1.sqlite3");
    assert_provider_journal(&ncm_journal, 1, "NCM production");
    shutdown_production_replacement_project(&ncm_project).await;
    drop(ncm_router);
    drop(ncm_project);
    stop_ncm_owner(&first_owner, "NCM first generation");

    // Reopen a fresh daemon/project composition over the same state root. The
    // worker owner, process ID, and incarnation must all prove a real restart;
    // the canonical recall identity/content/provenance comes from the durable
    // NCM namespace rather than the fixture input passed to recall.
    let second_owners = Arc::new(StdMutex::new(Vec::new()));
    let second_project = mount_production_replacement_project(
        &fixture.profile,
        &fixture.project_root,
        &ProjectId::new(PROJECT_ID).expect("project id"),
        ncm_selection,
        ncm_observer,
        ncm_routing,
        Some(replacement_ncm_factory(
            Arc::clone(&second_owners),
            Some(Arc::clone(&first_owner)),
        )),
    )
    .await;
    activate_production_replacement_project(&second_project)
        .await
        .expect("activate restarted production NCM observation journey");
    let second_owner = second_owners
        .lock()
        .expect("NCM restart owner capture mutex")
        .last()
        .cloned()
        .expect("NCM restart worker owner");
    let second_router = register_public_replacement_route(
        second_project.profile_id.clone(),
        &fixture.project_root,
        ProjectId::new(PROJECT_ID).expect("project id"),
        second_project.scope.clone(),
        second_project.full.as_ref(),
    )
    .await;
    let second_result = production_cognitive_recall(
        &second_project.host,
        &second_project.scope,
        TRANSCRIPT_TEXT,
        "ncm-restart",
    )
    .await;
    let second_snapshot = snapshot_recall(
        &second_result,
        "ncm",
        TRANSCRIPT_TEXT,
        &second_project.scope,
        "NCM restart",
    );
    assert_live_replacement_authorities(&fixture.profile, &second_project);
    assert_eq!(second_snapshot.content, ncm_snapshot.content);
    assert_eq!(second_snapshot.provenance, ncm_snapshot.provenance);
    assert_eq!(
        second_snapshot.stable_reference,
        ncm_snapshot.stable_reference
    );
    let second_pid = second_owner
        .worker_pid()
        .expect("NCM restarted worker process");
    let second_incarnation = second_owner
        .worker_incarnation()
        .expect("NCM restarted worker incarnation");
    assert_ne!(
        second_pid, first_pid,
        "NCM restart must prove a new process"
    );
    assert_eq!(
        second_incarnation,
        first_incarnation.saturating_add(1),
        "NCM restart must advance the existing owner's incarnation"
    );
    assert!(
        Arc::ptr_eq(&second_owner, &first_owner),
        "NCM restart must reuse the admitted lifecycle owner"
    );
    let (active_health_status, active_health_body) = invoke_public_provider_control(
        &second_router,
        RetainedSurfaceOperation::ProviderHealth,
        json!({
            "state": public_state_selector("ncm"),
            "requested_checks": ["protocol", "state", "persistence"]
        }),
        "ncm-active-health",
    )
    .await;
    assert_public_control_success(
        active_health_status,
        &active_health_body,
        "NCM active health",
    );
    assert_provider_journal(&ncm_journal, 1, "NCM restarted production");

    // Kill the actual NCM child and prove the public route exposes its typed
    // unavailable terminal before Native is admitted as the rollback owner.
    // This is the failure-triggered cutover boundary; a graceful stop would
    // exercise lifecycle shutdown but would not prove the failed publication
    // path that the rollback is required to recover from.
    kill_ncm_owner(&second_owner, "NCM failed generation");
    let (failed_health_status, failed_health_body) = invoke_public_provider_control(
        &second_router,
        RetainedSurfaceOperation::ProviderHealth,
        json!({
            "state": public_state_selector("ncm"),
            "requested_checks": ["protocol", "state", "persistence"]
        }),
        "ncm-failed-health",
    )
    .await;
    assert_public_provider_unavailable(failed_health_status, &failed_health_body);
    shutdown_production_replacement_project(&second_project).await;
    drop(second_router);
    drop(second_project);

    // Native rollback uses the same production composition and canonical
    // retrieval service. Its StagedSession observation disposition is typed;
    // the rollback proof depends on canonical history and the Native journal,
    // never on treating an unsupported observation as applied.
    let rollback_project = mount_production_replacement_project(
        &fixture.profile,
        &fixture.project_root,
        &ProjectId::new(PROJECT_ID).expect("project id"),
        native_selection,
        tracedecay_domain::configuration::MemoryProviderNcmObserverV1::Disabled {},
        tracedecay_domain::configuration::MemoryProviderRecallRoutingV1 {
            active_provider: Some("tracedecay.native".to_owned()),
            ..Default::default()
        },
        None,
    )
    .await;
    activate_production_replacement_project(&rollback_project)
        .await
        .expect("Native rollback must reopen after its prior refusal watermark is settled");
    assert_native_staged_session_refusal_journal(
        &rollback_project
            .graph
            .store_layout()
            .data_root
            .join("memory-observation-journal-v1.sqlite3"),
        "Native rollback",
    );
    let rollback_router = register_public_replacement_route(
        rollback_project.profile_id.clone(),
        &fixture.project_root,
        ProjectId::new(PROJECT_ID).expect("project id"),
        rollback_project.scope.clone(),
        rollback_project.full.as_ref(),
    )
    .await;
    let (rollback_health_status, rollback_health_body) = invoke_public_provider_control(
        &rollback_router,
        RetainedSurfaceOperation::ProviderHealth,
        json!({
            "state": public_state_selector("tracedecay.native"),
            "requested_checks": ["protocol", "state", "persistence"]
        }),
        "native-rollback-health",
    )
    .await;
    assert_public_control_success(
        rollback_health_status,
        &rollback_health_body,
        "Native rollback health",
    );
    let rollback_result = production_cognitive_recall(
        &rollback_project.host,
        &rollback_project.scope,
        TRANSCRIPT_TEXT,
        "native-rollback",
    )
    .await;
    let rollback_snapshot = snapshot_recall(
        &rollback_result,
        "tracedecay.native",
        TRANSCRIPT_TEXT,
        &rollback_project.scope,
        "Native rollback",
    );
    assert_eq!(rollback_snapshot, native_snapshot);
    assert_live_replacement_authorities(&fixture.profile, &rollback_project);
    let rollback_journal = rollback_project
        .graph
        .store_layout()
        .data_root
        .join("memory-observation-journal-v1.sqlite3");
    assert_provider_journal(&rollback_journal, 1, "Native rollback");
    shutdown_production_replacement_project(&rollback_project).await;
    drop(rollback_router);
    drop(rollback_project);

    // Fault the backup publication boundary, then boot the coordinator's
    // complete V1 quarantine with the actual released service. The old
    // service must never see the V2 semantic manifest or NCM namespace.
    let rollback_probe_root = fixture.temp.path().join("rollback-v1-publication-probe");
    set_rehearsal_publication_fault_for_test("before_rename");
    let error = rehearse_complete_profile_backup(&backup_root, &rollback_probe_root)
        .expect_err("faulted V1 rollback publication");
    assert!(
        matches!(error, ProfileBackupError::Unavailable { .. }),
        "faulted rollback must remain typed unavailable: {error:?}"
    );
    set_rehearsal_publication_fault_for_test("");
    // The coordinator's durable quarantine is the rollback profile. It keeps
    // every V1 opaque root and is already independently matched to the source
    // inventory above; the failed rehearsal must not replace it with a
    // partial backup tree.
    let old_profile_root = preserved_v1_root.clone();
    assert!(
        !old_profile_root
            .join(".tracedecay-v1-to-v2-manifest.json")
            .exists()
    );
    assert_eq!(
        profile_authority_inventory(&old_profile_root),
        source_profile_inventory,
        "V1 quarantine must contain no V2 bytes"
    );
    assert_v1_namespace_has_no_v2_bytes(&old_profile_root, "V1 rollback quarantine");
    let rollback_install = invoke_v1_release_harness(
        &harness,
        &old_profile_root,
        &fixture.project_root,
        "install",
    );
    let rollback_start =
        invoke_v1_release_harness(&harness, &old_profile_root, &fixture.project_root, "start");
    let rollback_start_identity =
        v1_process_identity(&rollback_start, "released V1 rollback start");
    assert_ne!(
        rollback_start_identity, baseline_start_identity,
        "V1 rollback must start a fresh released service process"
    );
    let rollback_observe = invoke_v1_release_observation(
        &harness,
        &old_profile_root,
        &fixture.project_root,
        "observe",
    );
    let rollback_recall =
        invoke_v1_release_observation(&harness, &old_profile_root, &fixture.project_root, "recall");
    assert_v1_observation_matches(
        &rollback_recall,
        "session.beta37.replacement",
        TRANSCRIPT_TEXT,
        "released V1 rollback recall",
    );
    assert_eq!(rollback_install["profile_id"], PROFILE_ID);
    assert_eq!(rollback_start["profile_id"], PROFILE_ID);
    assert_eq!(
        rollback_observe["project_id"],
        baseline_observe["project_id"]
    );
    assert_eq!(rollback_recall["source"], baseline_recall["source"]);
    assert_eq!(rollback_recall["provenance"], baseline_recall["provenance"]);
    assert_eq!(rollback_recall["timestamp"], baseline_recall["timestamp"]);
    assert_eq!(rollback_recall["receipt"], baseline_recall["receipt"]);
    assert_v1_namespace_has_no_v2_bytes(&old_profile_root, "released V1 rollback service");
    let rollback_stop =
        invoke_v1_release_harness(&harness, &old_profile_root, &fixture.project_root, "stop");
    assert_v1_process_stopped(&rollback_stop, "released V1 rollback stop");

    // Forward recovery reopens the live V2 profile and rebinds Native after
    // the old binary has been exercised on its own restored namespace.
    let recovered_project = mount_production_replacement_project(
        &fixture.profile,
        &fixture.project_root,
        &ProjectId::new(PROJECT_ID).expect("project id"),
        native_selection,
        tracedecay_domain::configuration::MemoryProviderNcmObserverV1::Disabled {},
        tracedecay_domain::configuration::MemoryProviderRecallRoutingV1 {
            active_provider: Some("tracedecay.native".to_owned()),
            ..Default::default()
        },
        None,
    )
    .await;
    activate_production_replacement_project(&recovered_project)
        .await
        .expect("forward Native recovery must reopen after the settled refusal watermark");
    assert_native_staged_session_refusal_journal(
        &recovered_project
            .graph
            .store_layout()
            .data_root
            .join("memory-observation-journal-v1.sqlite3"),
        "Native forward recovery",
    );
    let recovered_result = production_cognitive_recall(
        &recovered_project.host,
        &recovered_project.scope,
        TRANSCRIPT_TEXT,
        "native-forward-recovery",
    )
    .await;
    let recovered_snapshot = snapshot_recall(
        &recovered_result,
        "tracedecay.native",
        TRANSCRIPT_TEXT,
        &recovered_project.scope,
        "Native forward recovery",
    );
    assert_eq!(recovered_snapshot, native_snapshot);
    assert_live_replacement_authorities(&fixture.profile, &recovered_project);
    shutdown_production_replacement_project(&recovered_project).await;
    drop(recovered_project);

    // Exercise the coordinator's process-restart recovery boundary on a
    // second durable beta.37 profile. The successful journey above proves
    // the steady-state provider cutover; this profile is intentionally killed
    // after publication so a fresh CLI process must restore the old namespace
    // before the released V1 service is allowed to boot again.
    let crash_fixture = Beta37V1ProfileFixture::create().await;
    let crash_baseline_install = invoke_v1_release_harness(
        &harness,
        &crash_fixture.profile,
        &crash_fixture.project_root,
        "install",
    );
    let crash_baseline_start = invoke_v1_release_harness(
        &harness,
        &crash_fixture.profile,
        &crash_fixture.project_root,
        "start",
    );
    let crash_baseline_start_identity =
        v1_process_identity(&crash_baseline_start, "released V1 crash-profile start");
    let crash_baseline_observe = invoke_v1_release_observation(
        &harness,
        &crash_fixture.profile,
        &crash_fixture.project_root,
        "observe",
    );
    let crash_baseline_recall = invoke_v1_release_observation(
        &harness,
        &crash_fixture.profile,
        &crash_fixture.project_root,
        "recall",
    );
    assert_v1_observation_matches(
        &crash_baseline_recall,
        "session.beta37.replacement",
        TRANSCRIPT_TEXT,
        "released V1 crash-profile baseline recall",
    );
    assert_eq!(crash_baseline_install["project_id"], PROJECT_ID);
    let crash_baseline_stop = invoke_v1_release_harness(
        &harness,
        &crash_fixture.profile,
        &crash_fixture.project_root,
        "stop",
    );
    assert_v1_process_stopped(
        &crash_baseline_stop,
        "released V1 crash-profile baseline stop",
    );
    let crash_source_preflight =
        v1_preflight_paths(&crash_fixture.profile, &crash_fixture.project_store);
    let crash_source_project_inventory = inventory(&crash_fixture.project_root);
    let fault_journal =
        kill_storage_replace_v1_after_publication(&cli, &crash_fixture, "beta37-cli-crash");
    assert!(
        matches!(
            fault_journal["phase"].as_str(),
            Some("source_quarantined") | Some("published")
        ),
        "faulted replacement must die after profile publication: {fault_journal}"
    );
    assert!(
        crash_fixture
            .profile
            .join(".tracedecay-v1-to-v2-manifest.json")
            .is_file(),
        "faulted replacement must leave a durable V2 publication for recovery"
    );
    let recovery_probe = recover_storage_replace_v1_to_v1_namespace(&cli, &crash_fixture);
    assert_eq!(recovery_probe["status"], "recovered_v1");
    let crash_backup_root = crash_fixture
        .temp
        .path()
        .join("beta37-cli-crash-backups")
        .join("beta37-cli-crash");
    assert_complete_backup_manifest_with_id(&crash_backup_root, "beta37-cli-crash");
    let recovered_v1_project_store = project_store_from_profile(&crash_fixture.profile);
    assert!(
        !crash_fixture
            .profile
            .join(".tracedecay-v1-to-v2-manifest.json")
            .exists(),
        "coordinator restart must remove the failed V2 manifest before V1 boot"
    );
    assert_eq!(
        v1_preflight_paths(&crash_fixture.profile, &recovered_v1_project_store),
        crash_source_preflight,
        "coordinator restart must restore every V1 authority before old-service boot"
    );
    assert_beta37_authorities(&crash_fixture.profile, &recovered_v1_project_store);
    assert_eq!(
        inventory(&crash_fixture.project_root),
        crash_source_project_inventory,
        "coordinator restart must preserve the external checkout"
    );
    assert_cli_backup_tree_closure(
        &crash_fixture.profile,
        &crash_backup_root,
        "faulted CLI replacement backup",
    );
    assert_v1_namespace_has_no_v2_bytes(&crash_fixture.profile, "coordinator V1 rollback");
    let rollback_v1_install = invoke_v1_release_harness(
        &harness,
        &crash_fixture.profile,
        &crash_fixture.project_root,
        "install",
    );
    let rollback_v1_start = invoke_v1_release_harness(
        &harness,
        &crash_fixture.profile,
        &crash_fixture.project_root,
        "start",
    );
    let rollback_v1_start_identity = v1_process_identity(
        &rollback_v1_start,
        "released V1 crash-profile rollback start",
    );
    assert_ne!(
        rollback_v1_start_identity, crash_baseline_start_identity,
        "coordinator restart must launch a fresh released V1 service"
    );
    let rollback_v1_observe = invoke_v1_release_observation(
        &harness,
        &crash_fixture.profile,
        &crash_fixture.project_root,
        "observe",
    );
    let rollback_v1_recall = invoke_v1_release_observation(
        &harness,
        &crash_fixture.profile,
        &crash_fixture.project_root,
        "recall",
    );
    assert_v1_observation_matches(
        &rollback_v1_recall,
        "session.beta37.replacement",
        TRANSCRIPT_TEXT,
        "released V1 crash-profile rollback recall",
    );
    assert_eq!(rollback_v1_install["profile_id"], PROFILE_ID);
    assert_eq!(
        rollback_v1_observe["project_id"],
        crash_baseline_observe["project_id"]
    );
    for key in ["source", "provenance", "timestamp", "receipt"] {
        assert_eq!(
            rollback_v1_recall[key], crash_baseline_recall[key],
            "released V1 rollback must preserve {key} evidence"
        );
    }
    let rollback_v1_stop = invoke_v1_release_harness(
        &harness,
        &crash_fixture.profile,
        &crash_fixture.project_root,
        "stop",
    );
    assert_v1_process_stopped(&rollback_v1_stop, "released V1 crash-profile rollback stop");
    assert_v1_namespace_has_no_v2_bytes(
        &crash_fixture.profile,
        "released V1 crash-profile rollback service",
    );
    let crash_rollback_profile_inventory = profile_authority_inventory(&crash_fixture.profile);

    let forward_summary =
        invoke_storage_replace_v1_with_id(&cli, &crash_fixture, "beta37-cli-crash-forward");
    let forward_backup_root = PathBuf::from(
        forward_summary["backup_root"]
            .as_str()
            .expect("forward recovery backup root"),
    );
    let forward_preserved_v1_root = PathBuf::from(
        forward_summary["preserved_v1_root"]
            .as_str()
            .expect("forward recovery preserved V1 root"),
    );
    assert_complete_backup_manifest_with_id(&forward_backup_root, "beta37-cli-crash-forward");
    assert_first_party_replacement_manifest(&crash_fixture.profile, &forward_summary);
    assert_cli_backup_tree_closure(
        &forward_preserved_v1_root,
        &forward_backup_root,
        "CLI forward-recovery backup",
    );
    assert_beta37_authorities(
        &crash_fixture.profile,
        &project_store_from_profile(&crash_fixture.profile),
    );
    assert_eq!(
        profile_authority_inventory(&forward_preserved_v1_root),
        crash_rollback_profile_inventory,
        "forward recovery must retain the exact V1 quarantine for downgrade"
    );
    assert_eq!(
        inventory(&crash_fixture.project_root),
        crash_source_project_inventory,
        "forward recovery must keep the external checkout unchanged"
    );

    // The exact beta37 authorities remain available in the preserved V1 root,
    // while the live project and external checkout retain their own closure.
    assert_eq!(
        v1_preflight(&Beta37V1ProfileFixture {
            temp: tempfile::tempdir().expect("temporary final preflight wrapper"),
            profile: preserved_v1_root,
            project_root: fixture.project_root.clone(),
            project_store: preserved_project_store,
        }),
        source_preflight
    );
    assert_eq!(inventory(&fixture.project_root), source_project_inventory);
    assert_beta37_authorities(&fixture.profile, &live_project_store);
    assert_external_lcm_payload_closure(&fixture.profile);
}

#[cfg(unix)]
fn replacement_canonical_observation(
    project_id: &ProjectId,
    session_id: &SessionId,
    text: &str,
) -> DurableObservationV1 {
    // The seeded beta.37 session is a Codex canonical session. Keep the
    // observation's source provider aligned with that registered identity so
    // provider history attribution cannot silently accept a cross-provider
    // session alias during the NCM cutover.
    let provider = ProviderId::new("codex").expect("canonical provider");
    let range = ObservationSourceRangeV1::new(0, 1).expect("canonical source range");
    let record_id =
        ObservationId::new(format!("record.{}", session_id.as_str())).expect("canonical record id");
    let envelope = CanonicalObservationEnvelopeV1::new(
        provider.clone(),
        "message",
        record_id.clone(),
        CanonicalObservationRelationsV1::new(session_id.clone()),
        vec![CanonicalObservationFactV1::Message {
            role: CanonicalMessageRoleV1::Assistant,
            content: json!({"text": text}),
            model: Some("model.beta37-replacement".to_owned()),
            timestamp: Some(1_750_000_000),
        }],
        CanonicalObservationEvidenceV1::new(ObservationOrderingDomainV1::SnapshotOrder, range),
    )
    .expect("canonical observation envelope");
    let payload = serde_json::to_value(envelope).expect("canonical observation payload");
    let source = ObservationSourceIdentityV1::for_provider(provider, session_id.clone())
        .expect("canonical observation source");
    let generation = ObservationSourceGenerationV1::new(1).expect("canonical source generation");
    let identity = ObservationIdentityMaterialV1::for_native_record(
        source,
        ObservationScopeV1::Project {
            project_id: project_id.clone(),
        },
        generation,
        range,
        ObservationOrderingDomainV1::SnapshotOrder,
        record_id,
    )
    .expect("canonical observation identity");
    let receipt = SanitizationReceiptV1::new(
        SanitizationReceiptRefV1::new(
            SanitizationReceiptId::new(format!("receipt.{}", session_id.as_str()))
                .expect("canonical receipt id"),
            ComponentVersion::new("sanitizer.beta37-replacement.v1")
                .expect("canonical sanitizer version"),
        )
        .expect("canonical receipt reference"),
        SanitizerDispositionV1::Accepted,
        SensitivityV1::NonSensitive,
        Some(PayloadReferenceV1::for_payload(&payload).expect("canonical payload reference")),
    )
    .expect("canonical sanitization receipt");
    DurableObservationV1::new(
        identity,
        receipt,
        RetentionClass::new("retention.beta37-replacement.v1").expect("canonical retention"),
        payload,
    )
    .expect("durable canonical observation")
}

#[cfg(unix)]
fn replacement_anchored_write(observation: DurableObservationV1) -> AnchoredObservationWrite {
    let identity = observation.identity();
    let next_cursor = ObservationSourceCursorV1::for_ordering(
        observation.source().clone(),
        observation.scope().clone(),
        identity.generation(),
        identity.ordering_domain(),
        identity.position().end(),
    )
    .expect("canonical next cursor");
    let write = ObservationWrite::new(observation, None, next_cursor).expect("canonical write");
    let projection_generation = ProjectionGenerationId::new("projection.beta37-replacement.v1")
        .expect("canonical projection generation");
    let authorization = build_observation_resolution_authorization_v1(
        write.observation(),
        "beta37-replacement-acceptance",
    )
    .expect("canonical resolution authorization");
    let anchor = build_observation_retrieval_anchor_v2(
        write.observation(),
        projection_generation.clone(),
        UtcMicros(1_750_000_000_000_000),
        authorization,
    )
    .expect("canonical retrieval anchor");
    AnchoredObservationWrite::new(write, anchor, projection_generation)
        .expect("anchored canonical observation write")
}

#[cfg(unix)]
async fn persist_replacement_observation(
    store: &impl ObservationStore,
    project_id: &ProjectId,
    session_id: &SessionId,
    text: &str,
) {
    store
        .persist_observation(replacement_anchored_write(
            replacement_canonical_observation(project_id, session_id, text),
        ))
        .await
        .expect("persist canonical replacement observation");
}
