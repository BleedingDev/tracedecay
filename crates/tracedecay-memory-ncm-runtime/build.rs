//! Selects the NCM worker trust root for this build.
//!
//! Bazel supplies `TRACEDECAY_NCM_BAZEL_WORKER_MANIFEST` as a declared action
//! input. An absolute `TRACEDECAY_NCM_WORKER_MANIFEST` can select a release
//! trust root instead. Without either selection, the checked-in manifest pins
//! no worker. Only the embedded copy belongs to this build action; the bundle
//! writer publishes the worker and its sibling manifest together.

use std::env;
use std::error::Error;
use std::fs;
use std::io::{self, Write};
use std::path::{Path, PathBuf};

const CHECKED_IN_WORKER_MANIFEST: &str = "../../product/ncm/reference/worker-manifest.json";
const WORKER_MANIFEST_ENV: &str = "TRACEDECAY_NCM_WORKER_MANIFEST";
const BAZEL_WORKER_MANIFEST_ENV: &str = "TRACEDECAY_NCM_BAZEL_WORKER_MANIFEST";
const EMBEDDED_WORKER_MANIFEST: &str = "trusted-worker-manifest.json";

fn main() -> Result<(), Box<dyn Error>> {
    let mut output = io::BufWriter::new(io::stdout().lock());
    writeln!(output, "cargo:rerun-if-env-changed={WORKER_MANIFEST_ENV}")?;
    writeln!(
        output,
        "cargo:rerun-if-env-changed={BAZEL_WORKER_MANIFEST_ENV}"
    )?;
    writeln!(output, "cargo:rerun-if-env-changed=TARGET")?;
    let source = trusted_manifest_source()?;
    writeln!(output, "cargo:rerun-if-changed={}", source.display())?;
    let target = env::var("TARGET")?;
    writeln!(
        output,
        "cargo:rustc-env=TRACEDECAY_NCM_TARGET_TRIPLE={target}"
    )?;
    output.flush()?;

    let out_dir = PathBuf::from(env::var_os("OUT_DIR").ok_or("OUT_DIR is not set")?);
    copy_manifest(&source, &out_dir.join(EMBEDDED_WORKER_MANIFEST))
}

fn trusted_manifest_source() -> Result<PathBuf, Box<dyn Error>> {
    if let Some(path) = env::var_os(BAZEL_WORKER_MANIFEST_ENV) {
        if env::var_os(WORKER_MANIFEST_ENV).is_some() {
            return Err(format!(
                "{WORKER_MANIFEST_ENV} and {BAZEL_WORKER_MANIFEST_ENV} cannot both be set"
            )
            .into());
        }
        // A Bazel execpath is relative to the action's working directory,
        // while the release override remains explicitly absolute.
        return Ok(env::current_dir()?.join(path));
    }
    match env::var_os(WORKER_MANIFEST_ENV) {
        Some(path) => {
            let path = PathBuf::from(path);
            if !path.is_absolute() {
                return Err(format!(
                    "{WORKER_MANIFEST_ENV} must be an absolute path, got {}",
                    path.display()
                )
                .into());
            }
            Ok(path)
        }
        None => Ok(PathBuf::from(
            env::var_os("CARGO_MANIFEST_DIR").ok_or("CARGO_MANIFEST_DIR is not set")?,
        )
        .join(CHECKED_IN_WORKER_MANIFEST)),
    }
}

fn copy_manifest(source: &Path, destination: &Path) -> Result<(), Box<dyn Error>> {
    // Copying a file onto itself truncates it, which would erase the trust
    // root when the override names the manifest Cargo already placed here.
    if let (Ok(source), Ok(destination)) = (source.canonicalize(), destination.canonicalize())
        && source == destination
    {
        return Ok(());
    }
    fs::copy(source, destination).map(drop).map_err(|error| {
        io::Error::other(format!(
            "copy {} to {}: {error}",
            source.display(),
            destination.display()
        ))
        .into()
    })
}
