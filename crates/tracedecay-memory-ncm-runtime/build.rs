//! Copies the checked-in worker trust root beside Cargo-built worker binaries.

use std::env;
use std::error::Error;
use std::fs;
use std::io::{self, Write};
use std::path::PathBuf;

const WORKER_MANIFEST: &str = "../../product/ncm/reference/worker-manifest.json";

fn main() -> Result<(), Box<dyn Error>> {
    let mut output = io::BufWriter::new(io::stdout().lock());
    writeln!(output, "cargo:rerun-if-changed={WORKER_MANIFEST}")?;
    output.flush()?;

    let out_dir = PathBuf::from(env::var_os("OUT_DIR").ok_or("OUT_DIR is not set")?);
    let profile_dir = out_dir
        .ancestors()
        .nth(3)
        .ok_or("Cargo build output has no profile directory")?;
    let manifest =
        PathBuf::from(env::var_os("CARGO_MANIFEST_DIR").ok_or("CARGO_MANIFEST_DIR is not set")?)
            .join(WORKER_MANIFEST);
    let destination = profile_dir.join("worker-manifest.json");
    fs::copy(&manifest, &destination)
        .map(|_| ())
        .map_err(|error| {
            std::io::Error::other(format!(
                "copy {} to {}: {error}",
                manifest.display(),
                destination.display()
            ))
            .into()
        })
}
