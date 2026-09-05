//! Supervised NCM worker binary (task ncm-rs-017). Until that task lands this
//! binary refuses to serve rather than pretending to.

fn main() -> std::process::ExitCode {
    // No stdout/stderr output: the wire protocol owns stdio once implemented.
    std::process::ExitCode::from(2)
}
