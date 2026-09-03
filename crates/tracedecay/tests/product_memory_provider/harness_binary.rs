//! Product-owned same-checkout guard for the integration-test CLI.
//!
//! `product/upstream/convergence-map.json` records, for
//! `crates/tracedecay/tests/common/mod.rs`, that integration tests run a
//! same-checkout CLI. The release-prefix `--version` check in that upstream
//! file cannot enforce it: a sibling worktree builds the same release from the
//! same sources and reports an indistinguishable version. The guard therefore
//! lives here, in product-owned code, and the upstream harness keeps a
//! two-line mount.

use std::path::{Path, PathBuf};

/// The Cargo target directory that produced the running test executable.
///
/// Integration tests run from `<target>/<profile>/deps/<test>`, so the build
/// tree is three levels up. It is canonicalized because the selected CLI is
/// canonicalized too, and containment is only meaningful between resolved
/// paths.
pub(crate) fn test_build_tree() -> PathBuf {
    let executable = std::env::current_exe().expect("test executable path should resolve");
    let build_tree = executable
        .ancestors()
        .nth(3)
        .expect("integration test should run from <target>/<profile>/deps");
    build_tree.canonicalize().unwrap_or_else(|error| {
        panic!(
            "cannot resolve the test build tree at {}: {error}",
            build_tree.display()
        )
    })
}

/// Refuse a CLI that this checkout's own build tree did not produce.
///
/// Only the artifact's location proves which checkout built it, so a binary
/// from another worktree, another target directory, or a system install is
/// refused with the two paths that disagree rather than silently driving the
/// suite against foreign behavior.
pub(crate) fn refuse_foreign_test_bin(binary: &Path, build_tree: &Path) -> Result<(), String> {
    if binary.starts_with(build_tree) {
        return Ok(());
    }
    Err(format!(
        "the tracedecay CLI at {} was not produced by this checkout's build tree {}; \
         build it here with `cargo build -p tracedecay-cli --bin tracedecay` and point \
         TRACEDECAY_TEST_BIN at that artifact",
        binary.display(),
        build_tree.display()
    ))
}
