use std::collections::BTreeSet;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

use serde_json::json;
use tracedecay_code_index::graph_projection::HermeticCodeGraphProjectionStore;
use tracedecay_contracts::retrieval::SourceReadBodyPolicyV1;
use tracedecay_contracts::{
    CancellationSignal, CapabilityGrantId, CapabilityGrantSnapshot, Deadline, DisclosureClass,
    RequestContext, RequestId, ResolvedScope, now_micros,
};
use tracedecay_domain::{
    ActorId, CodeGenerationId, ManifestDigest, ProjectId, RefId, RepositoryId, UtcMicros,
    WorktreeId,
};
use tracedecay_graph_db::NeverCancelled;
use tracedecay_runtime_core::db::{Database, DatabaseAuthority, TestDatabaseRuntimeMode};
use tracedecay_tool_catalog::{CapabilityId, UseCaseId};

use super::verified_query_test_support::{
    ImmediateAdmission, ImmediateProjection, fixture_scope, fixture_store, graph_operation,
};
use super::{
    CodeGraphSourceAuthorityPort, CodeGraphSourceBindFuture, CodeGraphSourceBindRequest,
    VerifiedGraphQuery, VerifiedGraphQueryRequest, open_verified_graph_query,
};
use crate::SourceReadRuntimePort;
use crate::context::read_modes::{LineRange, ReadMode};
use crate::context::source_read::SourceReadRequest;
use tracedecay_session_memory::context::read_cache::{self, GLOBAL_SESSION};

/// Identity-only runtime: bind-time denial must refuse it before consulting
/// any other surface, so touching the database is a test failure.
struct IdentityOnlySource {
    project_root: PathBuf,
    project_id: String,
}

impl SourceReadRuntimePort for IdentityOnlySource {
    fn project_root(&self) -> &Path {
        &self.project_root
    }

    fn db(&self) -> &Database {
        unreachable!("identity-only fixture source")
    }

    fn is_read_only(&self) -> bool {
        true
    }

    fn project_id(&self) -> &str {
        &self.project_id
    }
}

struct CountingSource {
    project_root: PathBuf,
    project_id: String,
    db: Database,
    db_hits: Arc<AtomicUsize>,
    read_only: bool,
}

impl SourceReadRuntimePort for CountingSource {
    fn project_root(&self) -> &Path {
        &self.project_root
    }

    fn db(&self) -> &Database {
        self.db_hits.fetch_add(1, Ordering::SeqCst);
        &self.db
    }

    fn is_read_only(&self) -> bool {
        self.read_only
    }

    fn project_id(&self) -> &str {
        &self.project_id
    }
}

/// Same-identity, same-root facade that swaps its database answer after the
/// flip. With source authority frozen at admitted open, the flip must never
/// be observable.
struct SwappingSource {
    project_root: PathBuf,
    project_id: String,
    bound_db: Database,
    forged_db: Database,
    forged: AtomicBool,
    bound_hits: AtomicUsize,
    forged_hits: AtomicUsize,
}

impl SourceReadRuntimePort for SwappingSource {
    fn project_root(&self) -> &Path {
        &self.project_root
    }

    fn db(&self) -> &Database {
        if self.forged.load(Ordering::SeqCst) {
            self.forged_hits.fetch_add(1, Ordering::SeqCst);
            &self.forged_db
        } else {
            self.bound_hits.fetch_add(1, Ordering::SeqCst);
            &self.bound_db
        }
    }

    fn is_read_only(&self) -> bool {
        true
    }

    fn project_id(&self) -> &str {
        &self.project_id
    }
}

struct FixtureSourceBind {
    runtime: Arc<dyn SourceReadRuntimePort>,
}

impl CodeGraphSourceAuthorityPort for FixtureSourceBind {
    fn bind<'a>(
        &'a self,
        _request: CodeGraphSourceBindRequest<'a>,
    ) -> CodeGraphSourceBindFuture<'a> {
        let runtime = Arc::clone(&self.runtime);
        Box::pin(async move { Ok(runtime) })
    }
}

async fn test_database(path: &Path) -> Database {
    crate::register_test_schema_installer();
    let authority = DatabaseAuthority::acquire_test(path, "verified query source forge")
        .expect("database authority");
    Database::publish_test_runtime(path, &authority, TestDatabaseRuntimeMode::Initialize)
        .await
        .expect("database")
        .0
}

fn fixture_context(project_id: &str, cancellation: &CancellationSignal) -> RequestContext {
    let scope = ResolvedScope::new(
        ProjectId::new(project_id).expect("project"),
        RepositoryId::new("repository.verified-query-source").expect("repository"),
        WorktreeId::new("worktree.verified-query-source").expect("worktree"),
        Some(RefId::new("refs/heads/verified-query-source").expect("reference")),
    )
    .expect("scope");
    let grant = CapabilityGrantSnapshot::new(
        CapabilityGrantId::new("grant.verified-query-source").expect("grant"),
        1,
        ManifestDigest::new(format!("sha256:{}", "a".repeat(64))).expect("digest"),
        ActorId::new("actor.verified-query-source.issuer").expect("issuer"),
        UtcMicros(1),
        UtcMicros(i64::MAX),
        scope.clone(),
        BTreeSet::from(
            [CapabilityId::new("capability.verified-query-source").expect("capability")],
        ),
        BTreeSet::from([UseCaseId::new("use-case.verified-query-source").expect("use case")]),
        DisclosureClass::Evidence,
    )
    .expect("grant");
    RequestContext::new(
        ActorId::new("actor.verified-query-source.requester").expect("requester"),
        scope,
        grant,
        RequestId::new("request.verified-query-source").expect("request"),
        Deadline::new(UtcMicros(i64::MAX)).expect("deadline"),
        cancellation.context(),
    )
    .expect("context")
}

fn fixture_query(project_id: &str) -> VerifiedGraphQuery {
    let cancellation =
        CancellationSignal::active("cancel.verified-query-source").expect("cancellation");
    let projection = HermeticCodeGraphProjectionStore::memory(&cancellation).expect("projection");
    let generation =
        CodeGenerationId::new("generation.verified-query-source.1").expect("generation");
    projection
        .publish_with_cancellation(&generation, &[], &[], Arc::new(NeverCancelled))
        .expect("publish");
    let store = projection.verified_store(&generation).expect("store");
    let graph_cancellation = super::application_graph_cancellation(&cancellation);
    let reader = store
        .interactive_reader_with_cancellation(&generation, Arc::clone(&graph_cancellation))
        .expect("reader");
    VerifiedGraphQuery::from_fixture_reader(
        reader,
        graph_cancellation,
        fixture_context(project_id, &cancellation),
    )
}

fn assert_denied(error: tracedecay_domain::errors::TraceDecayError) {
    let (code, retryable, _) = error
        .project_route_context()
        .expect("typed denied source route");
    assert_eq!(code, "code-graph-denied");
    assert!(!retryable);
}

fn full_read_request(project_id: &str) -> SourceReadRequest<'_> {
    SourceReadRequest {
        file: "src/lib.rs",
        mode: ReadMode::Full,
        body_policy: SourceReadBodyPolicyV1::IfChanged,
        line_range: None,
        raw_lines: None,
        include_symbols: false,
        project_id,
    }
}

#[test]
fn unbound_query_refuses_source_reads() {
    let query = fixture_query("project.verified-query-source.a");
    let error = query
        .resolve_indexed_source_file("src/lib.rs")
        .expect_err("unbound source must fail closed");
    assert_denied(error);
}

#[tokio::test]
async fn resolve_rejects_absolute_path_under_another_project_root() {
    let home = tempfile::tempdir().expect("temp");
    let project_a = home.path().join("project-a");
    let project_b = home.path().join("project-b");
    std::fs::create_dir_all(project_a.join("src")).expect("project a");
    std::fs::create_dir_all(project_b.join("src")).expect("project b");
    std::fs::write(project_b.join("src/secret.rs"), "fn secret() {}\n").expect("foreign file");
    let db = test_database(&project_a.join("bound.db")).await;
    let query =
        fixture_query("project.verified-query-source.a").with_source(Arc::new(CountingSource {
            project_root: project_a,
            project_id: "project.verified-query-source.a".to_owned(),
            db,
            db_hits: Arc::new(AtomicUsize::new(0)),
            read_only: true,
        }));
    let error = query
        .resolve_indexed_source_file(project_b.join("src/secret.rs").to_str().expect("utf8"))
        .expect_err("foreign root must be denied");
    assert!(
        error.to_string().contains("escapes project root")
            || error
                .project_route_context()
                .is_some_and(|(code, _, _)| code == "code-graph-denied"),
        "foreign root must fail closed, got {error}"
    );
}

#[tokio::test]
async fn read_source_rejects_request_project_id_outside_bound_source() {
    let home = tempfile::tempdir().expect("temp");
    let project_a = home.path().join("project-a");
    std::fs::create_dir_all(&project_a).expect("project a");
    let db = test_database(&project_a.join("bound.db")).await;
    let query =
        fixture_query("project.verified-query-source.a").with_source(Arc::new(CountingSource {
            project_root: project_a,
            project_id: "project.verified-query-source.a".to_owned(),
            db,
            db_hits: Arc::new(AtomicUsize::new(0)),
            read_only: true,
        }));
    for body_policy in [SourceReadBodyPolicyV1::IfChanged, SourceReadBodyPolicyV1::Required] {
        let mut request = full_read_request("project.verified-query-source.b");
        request.body_policy = body_policy;
        let error = match query.read_source(request).await {
            Ok(_) => panic!("foreign request project id must be denied"),
            Err(error) => error,
        };
        assert_denied(error);
    }
}

#[tokio::test]
async fn open_denies_cross_project_source_at_bind() {
    let home = tempfile::tempdir().expect("temp");
    let admission = ImmediateAdmission {
        scope: fixture_scope("verified-query-source-deny"),
    };
    let projection = ImmediateProjection {
        scope: fixture_scope("verified-query-source-deny"),
        store: fixture_store("verified-query-source-deny"),
    };
    let bind = FixtureSourceBind {
        runtime: Arc::new(IdentityOnlySource {
            project_root: home.path().to_path_buf(),
            project_id: "project.verified-query-source-other".to_owned(),
        }),
    };
    let deadline = Deadline::new(UtcMicros(i64::MAX)).expect("deadline");
    let cancellation =
        CancellationSignal::active("cancel.verified-query-source.bind-deny").expect("signal");
    let operation = graph_operation();
    let error = match open_verified_graph_query(
        &admission,
        &projection,
        VerifiedGraphQueryRequest::new(
            &operation,
            RequestId::new("request.verified-query-source.bind-deny").expect("request"),
            deadline,
            &cancellation,
        ),
        Some(&bind),
    )
    .await
    {
        Ok(_) => panic!("cross-project source bind must be denied"),
        Err(error) => error,
    };
    assert_denied(error);
}

#[tokio::test]
async fn forged_runtime_cannot_redirect_reads_after_admitted_open() {
    let home = tempfile::tempdir().expect("temp");
    let project = home.path().join("project");
    std::fs::create_dir_all(project.join("src")).expect("project");
    let source_file = project.join("src/lib.rs");
    std::fs::write(&source_file, "fn bound() {}\n").expect("file");
    let project_id = "project.verified-query-source-swap";
    let bound_db = test_database(&project.join("bound.db")).await;
    let forged_db = test_database(&project.join("forged.db")).await;
    let mtime_ns = read_cache::file_mtime_ns(&source_file).expect("mtime");
    let args_hash = read_cache::args_hash(&json!({
        "lines": serde_json::Value::Null,
        "last_sync_at": serde_json::Value::Null,
    }))
    .expect("args hash");
    read_cache::put(
        &forged_db,
        project_id,
        GLOBAL_SESSION,
        "src/lib.rs",
        mtime_ns,
        "full",
        &args_hash,
        "forged-cache-digest",
        b"forged-body",
        1,
    )
    .await
    .expect("poison forged cache");
    let facade = Arc::new(SwappingSource {
        project_root: project,
        project_id: project_id.to_owned(),
        bound_db,
        forged_db,
        forged: AtomicBool::new(false),
        bound_hits: AtomicUsize::new(0),
        forged_hits: AtomicUsize::new(0),
    });
    let admission = ImmediateAdmission {
        scope: fixture_scope("verified-query-source-swap"),
    };
    let projection = ImmediateProjection {
        scope: fixture_scope("verified-query-source-swap"),
        store: fixture_store("verified-query-source-swap"),
    };
    let bind = FixtureSourceBind {
        runtime: Arc::clone(&facade) as Arc<dyn SourceReadRuntimePort>,
    };
    let deadline =
        Deadline::new(UtcMicros(now_micros().0.saturating_add(60_000_000))).expect("deadline");
    let cancellation =
        CancellationSignal::active("cancel.verified-query-source.swap").expect("signal");
    let operation = graph_operation();
    let query = open_verified_graph_query(
        &admission,
        &projection,
        VerifiedGraphQueryRequest::new(
            &operation,
            RequestId::new("request.verified-query-source.swap").expect("request"),
            deadline,
            &cancellation,
        ),
        Some(&bind),
    )
    .await
    .expect("admitted open with bound source");
    assert_eq!(
        facade.bound_hits.load(Ordering::SeqCst),
        1,
        "the database authority is captured exactly once at admitted open"
    );
    // Flip the facade after admission: a live runtime would now answer with
    // the forged database, but the frozen authority must never consult it.
    facade.forged.store(true, Ordering::SeqCst);
    for body_policy in [SourceReadBodyPolicyV1::IfChanged, SourceReadBodyPolicyV1::Required] {
        let mut request = full_read_request(project_id);
        request.body_policy = body_policy;
        let output = query.read_source(request).await.expect("bound source read");
        assert_ne!(output.digest, "forged-cache-digest");
        assert!(!output.unchanged);
        assert_eq!(output.body.as_deref(), Some("fn bound() {}\n"));
    }
    assert_eq!(
        facade.forged_hits.load(Ordering::SeqCst),
        0,
        "same-id/same-root forged runtime must not be readable"
    );
    assert_eq!(
        facade.bound_hits.load(Ordering::SeqCst),
        1,
        "reads use the frozen authority, never the runtime facade"
    );
}

#[tokio::test]
async fn required_source_read_bypasses_mtime_cache_and_returns_actual_body() {
    let root = tempfile::tempdir().expect("project");
    std::fs::create_dir(root.path().join("src")).expect("source directory");
    let file = root.path().join("src/lib.rs");
    let original = "fn first() {}\n";
    let replacement = "fn other() {}\n";
    assert_eq!(original.len(), replacement.len());
    std::fs::write(&file, original).expect("source");
    let original_mtime = std::fs::metadata(&file).unwrap().modified().unwrap();
    let project_id = "project.verified-query-source.required-cache";
    let query = fixture_query(project_id).with_source(Arc::new(CountingSource {
        project_root: root.path().to_path_buf(),
        project_id: project_id.to_owned(),
        db: test_database(&root.path().join("bound.db")).await,
        db_hits: Arc::new(AtomicUsize::new(0)),
        read_only: false,
    }));
    let first = query.read_source(full_read_request(project_id)).await.expect("first read");
    assert_eq!(first.body.as_deref(), Some(original));
    assert!(!first.unchanged);
    let cached = query.read_source(full_read_request(project_id)).await.expect("cached read");
    assert!(cached.unchanged);
    assert!(cached.body.is_none());
    assert_eq!(cached.digest, first.digest);
    let mut required = full_read_request(project_id);
    required.body_policy = SourceReadBodyPolicyV1::Required;
    let repeated = query.read_source(required).await.expect("required repeated read");
    assert_eq!(repeated.body.as_deref(), Some(original));
    assert!(!repeated.unchanged);
    assert_eq!(repeated.digest, read_cache::digest_bytes(original.as_bytes()));

    std::fs::write(&file, replacement).expect("replace same-length source");
    std::fs::File::options().write(true).open(&file).unwrap()
        .set_times(std::fs::FileTimes::new().set_modified(original_mtime)).expect("restore mtime");
    assert_eq!(read_cache::file_mtime_ns(&file).unwrap(), first.mtime_ns);
    let stale_receipt = query.read_source(full_read_request(project_id)).await.expect("compatible metadata cache");
    assert!(stale_receipt.unchanged);
    assert!(stale_receipt.body.is_none());
    assert_eq!(stale_receipt.digest, first.digest);
    for _ in 0..2 {
        let mut required = full_read_request(project_id);
        required.body_policy = SourceReadBodyPolicyV1::Required;
        let fresh = query.read_source(required).await.expect("required fresh read");
        assert_eq!(fresh.body.as_deref(), Some(replacement));
        assert!(!fresh.unchanged);
        assert_eq!(fresh.digest, read_cache::digest_bytes(replacement.as_bytes()));
        assert_ne!(fresh.digest, first.digest);
    }
}

#[tokio::test]
async fn required_source_read_preserves_decoding_ranges_and_empty_bodies() {
    let root = tempfile::tempdir().expect("project");
    std::fs::create_dir(root.path().join("src")).expect("source directory");
    let file = root.path().join("src/lib.rs");
    let decoded = "first\r\né second\r\nthird\r\n";
    let bytes: Vec<u8> = [0xff, 0xfe].into_iter()
        .chain(decoded.encode_utf16().flat_map(u16::to_le_bytes)).collect();
    std::fs::write(&file, bytes).expect("UTF-16 source");
    let project_id = "project.verified-query-source.required-lines";
    let query = fixture_query(project_id).with_source(Arc::new(CountingSource {
        project_root: root.path().to_path_buf(),
        project_id: project_id.to_owned(),
        db: test_database(&root.path().join("bound.db")).await,
        db_hits: Arc::new(AtomicUsize::new(0)),
        read_only: false,
    }));
    let mut required = full_read_request(project_id);
    required.body_policy = SourceReadBodyPolicyV1::Required;
    let full = query.read_source(required).await.expect("decoded full body");
    assert_eq!(full.body.as_deref(), Some(decoded));
    assert_eq!(full.digest, read_cache::digest_bytes(decoded.as_bytes()));
    for (raw, expected) in [("2-3", "é second\nthird"), ("2-3", "é second\nthird"), ("20-30", "")] {
        let output = query.read_source(SourceReadRequest {
            mode: ReadMode::Lines,
            body_policy: SourceReadBodyPolicyV1::Required,
            raw_lines: Some(raw),
            line_range: Some(LineRange::parse(raw).expect("range")),
            ..full_read_request(project_id)
        }).await.expect("required line body");
        assert_eq!(output.body.as_deref(), Some(expected));
        assert!(!output.unchanged);
        assert_eq!(output.digest, read_cache::digest_bytes(expected.as_bytes()));
        assert_eq!(output.token_count, crate::context::read_modes::estimate_tokens(expected));
    }
    std::fs::write(&file, "").expect("empty source");
    for _ in 0..2 {
        let mut required = full_read_request(project_id);
        required.body_policy = SourceReadBodyPolicyV1::Required;
        let empty = query.read_source(required).await.expect("required empty body");
        assert_eq!(empty.body.as_deref(), Some(""));
        assert!(!empty.unchanged);
        assert_eq!(empty.digest, read_cache::digest_bytes(b""));
        assert_eq!(empty.token_count, 0);
    }
}

#[tokio::test]
async fn required_source_read_uses_original_admitted_cancellation() {
    let root = tempfile::tempdir().expect("project");
    std::fs::create_dir(root.path().join("src")).expect("source directory");
    std::fs::write(root.path().join("src/lib.rs"), "fn admitted() {}\n").expect("source");
    let project_id = "project.verified-query-source-required-control";
    let admission = ImmediateAdmission { scope: fixture_scope("verified-query-source-required-control") };
    let projection = ImmediateProjection {
        scope: fixture_scope("verified-query-source-required-control"),
        store: fixture_store("verified-query-source-required-control"),
    };
    let bind = FixtureSourceBind {
        runtime: Arc::new(CountingSource {
            project_root: root.path().to_path_buf(),
            project_id: project_id.to_owned(),
            db: test_database(&root.path().join("bound.db")).await,
            db_hits: Arc::new(AtomicUsize::new(0)),
            read_only: false,
        }),
    };
    let deadline = Deadline::new(UtcMicros(now_micros().0.saturating_add(60_000_000))).expect("deadline");
    let cancellation = CancellationSignal::active("cancel.verified-query-source.required-control").expect("signal");
    let operation = graph_operation();
    let query = open_verified_graph_query(
        &admission, &projection,
        VerifiedGraphQueryRequest::new(&operation, RequestId::new("request.verified-query-source.required-control").expect("request"), deadline, &cancellation),
        Some(&bind),
    ).await.expect("admitted source read");
    assert_eq!(query.request_context().deadline(), deadline);
    assert!(cancellation.cancel(now_micros()));
    let mut required = full_read_request(project_id);
    required.body_policy = SourceReadBodyPolicyV1::Required;
    let error = match query.read_source(required).await {
        Ok(_) => panic!("required body must not revive a cancelled request"),
        Err(error) => error,
    };
    assert_eq!(error.project_route_context().expect("typed cancellation").0, "code-graph-cancelled");
    assert_eq!(query.request_context().deadline(), deadline);
}
