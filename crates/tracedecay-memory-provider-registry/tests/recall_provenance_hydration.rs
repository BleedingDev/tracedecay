//! Behavioral proof that provenance hydration is host-backed, not shape-shaped.
//!
//! The source store here is a real filesystem-backed resolver over a real
//! temporary checkout: it opens the file the provider claimed and counts the
//! lines the host actually holds. That is what makes these fixtures evidence
//! rather than restatements of the parser — a claim about a file that is not
//! on disk, or a range past the end of one that is, has to fail.
#![allow(clippy::expect_used, clippy::panic, clippy::unwrap_used)]

use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use tracedecay_application::{CancellationSignal, ResolvedScope};
use tracedecay_domain::{ProjectId, RefId, RepositoryId, WorktreeId};
use tracedecay_memory_provider_registry::{
    HostCanonicalRecordStore, HostEvidenceControlV1, HostEvidenceLookupErrorV1, HostEvidenceRefV1,
    HostEvidenceScopeV1, HostProvenanceAuthority, HostSessionEvidenceStore,
    HostSourceEvidenceStore, MountedHostProvenanceAuthorityV1, ProvenanceHydrationDegradationV1,
    ProvenanceHydrationError, ProvenanceHydrationPassV1, ProvenanceHydrationPolicyV1,
    ProviderItemProvenanceV1,
};

const SESSION: &str = "session.hydration.integration";
const MAX_SOURCE_BYTES: u64 = 4 * 1024 * 1024;

/// A real host source store: it resolves the claimed relative path inside the
/// authoritative worktree root, refuses anything that escapes it after
/// canonicalization, and counts the lines the file really has.
struct CheckoutSourceStore {
    reads: AtomicUsize,
}

impl CheckoutSourceStore {
    fn new() -> Self {
        Self {
            reads: AtomicUsize::new(0),
        }
    }

    fn reads(&self) -> usize {
        self.reads.load(Ordering::SeqCst)
    }
}

impl HostSourceEvidenceStore for CheckoutSourceStore {
    fn source_line_count(
        &self,
        scope: &HostEvidenceScopeV1,
        relative_path: &Path,
    ) -> Result<u64, HostEvidenceLookupErrorV1> {
        self.reads.fetch_add(1, Ordering::SeqCst);
        let root = fs::canonicalize(scope.worktree_root()).map_err(|error| {
            HostEvidenceLookupErrorV1::Unavailable {
                reason: format!("worktree root unreadable: {error}"),
            }
        })?;
        let resolved = fs::canonicalize(root.join(relative_path))
            .map_err(|_| HostEvidenceLookupErrorV1::NotFound)?;
        if !resolved.starts_with(&root) {
            return Err(HostEvidenceLookupErrorV1::ForeignScope {
                field: "worktree_root",
            });
        }
        let metadata = fs::metadata(&resolved).map_err(|_| HostEvidenceLookupErrorV1::NotFound)?;
        if !metadata.is_file() {
            return Err(HostEvidenceLookupErrorV1::NotFound);
        }
        if metadata.len() > MAX_SOURCE_BYTES {
            return Err(HostEvidenceLookupErrorV1::Unavailable {
                reason: "source file exceeds the host evidence read bound".to_owned(),
            });
        }
        let text = fs::read_to_string(&resolved).map_err(|error| {
            HostEvidenceLookupErrorV1::Unavailable {
                reason: format!("source file unreadable: {error}"),
            }
        })?;
        Ok(u64::try_from(text.lines().count()).unwrap_or(u64::MAX))
    }
}

struct SessionStore(BTreeMap<String, u64>);

impl HostSessionEvidenceStore for SessionStore {
    fn session_ordinal_ceiling(
        &self,
        _scope: &HostEvidenceScopeV1,
        session_id: &str,
    ) -> Result<u64, HostEvidenceLookupErrorV1> {
        self.0
            .get(session_id)
            .copied()
            .ok_or(HostEvidenceLookupErrorV1::NotFound)
    }
}

struct RecordStore(BTreeMap<String, String>);

impl HostCanonicalRecordStore for RecordStore {
    fn confirm_canonical_record(
        &self,
        scope: &HostEvidenceScopeV1,
        record_id: &str,
    ) -> Result<(), HostEvidenceLookupErrorV1> {
        match self.0.get(record_id) {
            None => Err(HostEvidenceLookupErrorV1::NotFound),
            Some(project) if project == scope.scope().project_id.as_str() => Ok(()),
            Some(_) => Err(HostEvidenceLookupErrorV1::ForeignScope {
                field: "project_id",
            }),
        }
    }
}

struct Fixture {
    root: PathBuf,
    outside: PathBuf,
    source: Arc<CheckoutSourceStore>,
    authority: MountedHostProvenanceAuthorityV1,
    scope: HostEvidenceScopeV1,
}

fn fixture(name: &str) -> Fixture {
    let base = PathBuf::from(env!("CARGO_TARGET_TMPDIR")).join(format!("hydration-{name}"));
    let _ = fs::remove_dir_all(&base);
    let root = base.join("worktree");
    let outside = base.join("other-worktree");
    fs::create_dir_all(root.join("crates/foo/src")).expect("checkout tree");
    fs::create_dir_all(&outside).expect("sibling tree");
    fs::write(
        root.join("crates/foo/src/lib.rs"),
        (1..=40)
            .map(|line| format!("// line {line}\n"))
            .collect::<String>(),
    )
    .expect("seed real source file");
    fs::write(outside.join("secret.rs"), "// not this checkout\n").expect("seed sibling file");

    let source = Arc::new(CheckoutSourceStore::new());
    let authority = MountedHostProvenanceAuthorityV1::new(
        Arc::clone(&source) as Arc<dyn HostSourceEvidenceStore>,
        Arc::new(SessionStore(BTreeMap::from([(SESSION.to_owned(), 12_u64)]))),
        Arc::new(RecordStore(BTreeMap::from([
            (
                "native.fact.real".to_owned(),
                "project.hydration".to_owned(),
            ),
            (
                "native.fact.foreign".to_owned(),
                "project.elsewhere".to_owned(),
            ),
        ]))),
    );
    let scope = HostEvidenceScopeV1::new(
        "profile.hydration",
        ResolvedScope::new(
            ProjectId::new("project.hydration").expect("project id"),
            RepositoryId::new("repository.hydration").expect("repository id"),
            WorktreeId::new("worktree.hydration").expect("worktree id"),
            Some(RefId::new("refs/heads/hydration").expect("reference")),
        )
        .expect("resolved scope"),
        SESSION,
        &root,
    )
    .expect("host evidence scope");
    Fixture {
        root,
        outside,
        source,
        authority,
        scope,
    }
}

fn live() -> CancellationSignal {
    CancellationSignal::active("token.hydration.integration").expect("live signal")
}

/// Real defect this catches: an authority that confirms a claim on shape
/// alone. Only the range that exists in the real checkout hydrates; a
/// well-formed claim about a file the host does not have, and a range past
/// the end of a file it does have, are refused.
#[test]
fn only_a_range_that_exists_in_the_real_checkout_hydrates() {
    let fixture = fixture("real-source");
    let signal = live();
    let control = HostEvidenceControlV1::new(0, 1_000, &signal);

    assert_eq!(
        fixture
            .authority
            .resolve(
                "source:crates/foo/src/lib.rs#L10-L20",
                &fixture.scope,
                &control
            )
            .expect("a real file range must hydrate"),
        HostEvidenceRefV1::SourceRange {
            path: "crates/foo/src/lib.rs".to_owned(),
            start_line: 10,
            end_line: 20,
        }
    );

    let absent = fixture
        .authority
        .resolve(
            "source:crates/foo/src/absent.rs#L1-L2",
            &fixture.scope,
            &control,
        )
        .expect_err("a claim about a file that is not on disk must be refused");
    assert!(matches!(
        absent,
        ProvenanceHydrationError::Unresolvable { .. }
    ));

    let past_end = fixture
        .authority
        .resolve(
            "source:crates/foo/src/lib.rs#L40-L41",
            &fixture.scope,
            &control,
        )
        .expect_err("a range past the end of the real file must be refused");
    match past_end {
        ProvenanceHydrationError::Unresolvable { reason, .. } => {
            assert!(reason.contains("holds 40"), "{reason}");
        }
        other => panic!("expected an out-of-range refusal: {other:?}"),
    }
}

/// Real defect this catches: a provider citing a file in a sibling worktree.
/// The traversal is refused before the store is ever asked, so the sibling
/// file is never opened even though it exists on disk.
#[test]
fn a_sibling_worktree_path_is_refused_without_touching_the_filesystem() {
    let fixture = fixture("traversal");
    assert!(
        fixture.outside.join("secret.rs").is_file(),
        "the sibling file must really exist for this to prove anything"
    );
    let signal = live();
    let control = HostEvidenceControlV1::new(0, 1_000, &signal);
    let before = fixture.source.reads();

    for claim in [
        "source:../other-worktree/secret.rs#L1-L1",
        "source:crates/../../other-worktree/secret.rs#L1-L1",
    ] {
        let error = fixture
            .authority
            .resolve(claim, &fixture.scope, &control)
            .expect_err("a traversing claim must be refused");
        assert!(
            matches!(error, ProvenanceHydrationError::Unresolvable { .. }),
            "{claim}: {error:?}"
        );
    }
    let absolute = format!(
        "source:{}#L1-L1",
        fixture.outside.join("secret.rs").display()
    );
    let error = fixture
        .authority
        .resolve(&absolute, &fixture.scope, &control)
        .expect_err("an absolute claim must be refused");
    assert!(matches!(
        error,
        ProvenanceHydrationError::Unresolvable { .. }
    ));
    assert_eq!(
        fixture.source.reads(),
        before,
        "an out-of-scope path must never reach the host source store"
    );
    assert!(fixture.root.is_dir());
}

/// Real defect this catches: `record:anything` hydrating without the host
/// confirming the canonical record exists and is owned by this project.
#[test]
fn a_canonical_record_hydrates_only_when_the_host_owns_it() {
    let fixture = fixture("records");
    let signal = live();
    let control = HostEvidenceControlV1::new(0, 1_000, &signal);
    assert_eq!(
        fixture
            .authority
            .resolve("record:native.fact.real", &fixture.scope, &control)
            .expect("a real owned record must hydrate"),
        HostEvidenceRefV1::CanonicalRecord {
            record_id: "native.fact.real".to_owned(),
        }
    );
    for claim in ["record:anything", "record:native.fact.foreign"] {
        let error = fixture
            .authority
            .resolve(claim, &fixture.scope, &control)
            .expect_err("an unowned or nonexistent record must be refused");
        assert!(
            matches!(error, ProvenanceHydrationError::Unresolvable { .. }),
            "{claim}: {error:?}"
        );
    }
}

/// Real defect this catches: a pass that runs out of attempts handing the
/// remaining provider claims back as `Available`, which renders as a cited
/// source. Every claim past the bound must become an explicit
/// `Unresolvable` and the pass must record the typed degradation.
#[test]
fn more_claims_than_the_budget_degrade_explicitly_rather_than_passing_through() {
    let fixture = fixture("budget");
    let signal = live();
    let control = HostEvidenceControlV1::new(0, 1_000, &signal);
    let policy = ProvenanceHydrationPolicyV1::new(false, 2).expect("policy");
    let mut pass = ProvenanceHydrationPassV1::new(policy);
    let claim = ProviderItemProvenanceV1::Available {
        source: "record:native.fact.real".to_owned(),
    };

    let mut hydrated = 0_usize;
    let mut unresolved = 0_usize;
    for _ in 0..5 {
        let decision = pass.hydrate(&fixture.authority, &fixture.scope, &control, &claim);
        assert!(
            !matches!(
                decision.provenance,
                ProviderItemProvenanceV1::Available { .. }
            ),
            "hydration must never return a raw provider claim: {decision:?}"
        );
        match decision.provenance {
            ProviderItemProvenanceV1::Hydrated { .. } => hydrated += 1,
            ProviderItemProvenanceV1::Unresolvable { .. } => unresolved += 1,
            other => panic!("unexpected provenance state: {other:?}"),
        }
    }
    assert_eq!(hydrated, 2, "only the budgeted attempts may be confirmed");
    assert_eq!(unresolved, 3);
    assert_eq!(pass.attempts_spent(), 2);
    assert_eq!(
        pass.degradation(),
        Some(&ProvenanceHydrationDegradationV1::BudgetExhausted {
            budget: 2,
            unattempted: 3,
        })
    );
}
