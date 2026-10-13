//! Behavioral proof that the host-granted provider state capability is
//! containment and not merely a validated string (`tdmem-1107`).
//!
//! The host is the only thing that touches the filesystem — the registry crate
//! is source-contracted to name no filesystem capability — so these tests play
//! the host: they write **only** at paths the capability resolved, exactly as
//! the composition root does. A refused path produces no path at all, and the
//! assertion that matters is the byte content of the file outside the granted
//! root after every attempt.
#![allow(clippy::expect_used, clippy::panic)]

use std::fs;
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};

use tracedecay_memory_provider_registry::{
    ProviderStateAccessError, ProviderStateAuthorityError, ProviderStateAuthorityV1,
};

static SCRATCH_SEQUENCE: AtomicUsize = AtomicUsize::new(0);

/// A private, absolute scratch directory for one test.
fn scratch(name: &str) -> PathBuf {
    let sequence = SCRATCH_SEQUENCE.fetch_add(1, Ordering::Relaxed);
    let path = PathBuf::from(env!("CARGO_TARGET_TMPDIR"))
        .join("state-capability")
        .join(format!("{name}-{}-{sequence}", std::process::id()));
    let _ = fs::remove_dir_all(&path);
    fs::create_dir_all(&path).expect("scratch directory");
    fs::canonicalize(&path).expect("absolute scratch directory")
}

/// The host's own write, performed only where the capability said it may be.
fn host_write(target: &PathBuf, bytes: &[u8]) {
    if let Some(parent) = target.parent() {
        fs::create_dir_all(parent).expect("state directory");
    }
    fs::write(target, bytes).expect("host write");
}

/// The paths an adversarial provider would name to reach beyond its own state.
const ESCAPES: [&str; 8] = [
    "../outside-secret.txt",
    "../../outside-secret.txt",
    "facts/../../outside-secret.txt",
    "/etc/hosts",
    "~/outside-secret.txt",
    "..",
    "./outside-secret.txt",
    "%2e%2e/outside-secret.txt",
];

/// An admitted path resolves inside the granted root and the host's write
/// lands there; every traversal or absolute form resolves to **nothing**, so
/// the file outside the root is byte-for-byte unchanged.
///
/// Catches a "containment" that only classifies strings while still handing
/// the caller a path: the assertion is the content of the outside file after
/// every attempt.
#[test]
fn an_admitted_path_resolves_inside_the_root_and_no_escape_resolves_at_all() {
    let base = scratch("admitted-and-escapes");
    let outside = base.join("outside-secret.txt");
    fs::write(&outside, b"host-owned").expect("outside file");
    let authority =
        ProviderStateAuthorityV1::new(base.join("provider-state")).expect("state authority");
    let capability = authority
        .grant("tracedecay.native.project")
        .expect("granted capability");

    let admitted = capability
        .resolve("facts/first.json")
        .expect("an admitted path resolves");
    assert!(
        admitted.starts_with(capability.root()),
        "an admitted path must resolve under the granted root"
    );
    host_write(&admitted, b"{\"fact\":1}");
    assert_eq!(
        fs::read(&admitted).expect("written bytes"),
        b"{\"fact\":1}".to_vec()
    );

    for escape in ESCAPES {
        let refusal = capability
            .resolve(escape)
            .expect_err("an escaping path must resolve to nothing");
        assert!(
            matches!(
                refusal,
                ProviderStateAccessError::EscapesRoot { .. }
                    | ProviderStateAccessError::UnusablePath { .. }
            ),
            "unexpected refusal for {escape}: {refusal}"
        );
    }

    assert_eq!(
        fs::read(&outside).expect("outside file survives"),
        b"host-owned".to_vec(),
        "no refused path may become a write outside the granted root"
    );
}

/// A namespace that could address storage outside the host-owned root is
/// refused at grant time, so no capability rooted outside it ever exists.
#[test]
fn a_namespace_that_escapes_the_host_root_is_never_granted() {
    let base = scratch("unusable-namespace");
    let authority =
        ProviderStateAuthorityV1::new(base.join("provider-state")).expect("state authority");

    for namespace in [
        "../elsewhere",
        "/absolute",
        "",
        ".hidden",
        "a//b",
        "a/../b",
        "name with space",
        "windows\\path",
    ] {
        let refusal = authority
            .grant(namespace)
            .expect_err("an unusable namespace must not be granted");
        assert!(
            matches!(
                refusal,
                ProviderStateAuthorityError::NamespaceUnusable { .. }
                    | ProviderStateAuthorityError::NamespaceEscapesRoot { .. }
            ),
            "unexpected refusal for {namespace}: {refusal}"
        );
    }

    let granted = authority
        .grant("tracedecay.native.project")
        .expect("an admitted namespace is granted");
    assert!(granted.root().starts_with(authority.root()));
    assert_eq!(granted.state_namespace(), "tracedecay.native.project");
}

/// A root a caller could re-interpret is refused: only an absolute, normalized
/// host-owned directory can bound anything.
#[test]
fn an_unusable_state_root_is_refused_at_configuration() {
    for root in ["relative/root", "../escaping-root", ""] {
        assert!(
            ProviderStateAuthorityV1::new(root).is_err(),
            "{root} must not be accepted as a host-owned state root"
        );
    }
    let base = scratch("root-with-dot-segment");
    assert!(
        ProviderStateAuthorityV1::new(base.join("..").join("elsewhere")).is_err(),
        "a root carrying a parent segment must be refused"
    );
    assert!(ProviderStateAuthorityV1::new(base.join("provider-state")).is_ok());
}

/// Two namespaces get two roots, and neither can name a path inside the
/// other's. The host writes one namespace's state and the sibling's attempt to
/// reach it resolves to nothing, so the bytes are untouched.
#[test]
fn one_namespace_cannot_reach_another_namespaces_state() {
    let base = scratch("namespace-isolation");
    let authority =
        ProviderStateAuthorityV1::new(base.join("provider-state")).expect("state authority");
    let mine = authority.grant("tracedecay.native").expect("own namespace");
    let theirs = authority.grant("other.authority").expect("other namespace");

    let their_state = theirs.resolve("secret.txt").expect("own state resolves");
    host_write(&their_state, b"other-owned");

    assert!(
        mine.resolve("../other.authority/secret.txt").is_err(),
        "a capability must not name a path inside a sibling namespace"
    );
    assert_ne!(mine.root(), theirs.root());
    assert_eq!(
        fs::read(&their_state).expect("sibling state survives"),
        b"other-owned".to_vec()
    );
}
