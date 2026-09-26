//! Selects the NCM worker trust root for this build.
//!
//! The trusted worker manifest is `TRACEDECAY_NCM_WORKER_MANIFEST` when that
//! absolute path is set (a release bundle produced by
//! `scripts/product/ncm/build-worker-bundle.py`), otherwise the checked-in
//! `product/ncm/reference/worker-manifest.json`, which pins no worker. The
//! selection is copied into `OUT_DIR` for embedding and beside Cargo-built
//! binaries so a local worker finds its sibling manifest.

use std::env;
use std::error::Error;
use std::fs;
use std::io::{self, Write};
use std::path::{Path, PathBuf};

const CHECKED_IN_WORKER_MANIFEST: &str = "../../product/ncm/reference/worker-manifest.json";
const WORKER_MANIFEST_ENV: &str = "TRACEDECAY_NCM_WORKER_MANIFEST";
const EMBEDDED_WORKER_MANIFEST: &str = "trusted-worker-manifest.json";
const SIBLING_WORKER_MANIFEST: &str = "worker-manifest.json";

fn main() -> Result<(), Box<dyn Error>> {
    let mut output = io::BufWriter::new(io::stdout().lock());
    writeln!(output, "cargo:rerun-if-env-changed={WORKER_MANIFEST_ENV}")?;
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
    copy_manifest(&source, &out_dir.join(EMBEDDED_WORKER_MANIFEST))?;
    let profile_dir = out_dir
        .ancestors()
        .nth(3)
        .ok_or("Cargo build output has no profile directory")?;
    copy_manifest(&source, &profile_dir.join(SIBLING_WORKER_MANIFEST))
}

fn trusted_manifest_source() -> Result<PathBuf, Box<dyn Error>> {
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
