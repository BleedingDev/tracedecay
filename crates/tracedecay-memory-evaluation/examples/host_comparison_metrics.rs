//! Evaluate one host/lane/trial with the existing metric catalog and delivery join.

use std::error::Error;
use std::io::Write;

use tracedecay_memory_evaluation::{HostRetrievalRun, MetricCatalog, evaluate_host_retrieval};

fn main() -> Result<(), Box<dyn Error>> {
    let args: Vec<_> = std::env::args_os().skip(1).collect();
    if args.len() != 2 {
        return Err("usage: host_comparison_metrics CATALOG_JSON HOST_RETRIEVAL_RUN_JSON".into());
    }
    let catalog = MetricCatalog::from_json_str(&std::fs::read_to_string(&args[0])?)?;
    let run: HostRetrievalRun = serde_json::from_slice(&std::fs::read(&args[1])?)?;
    let report = evaluate_host_retrieval(&catalog, &run)?;
    let mut output = std::io::stdout().lock();
    serde_json::to_writer_pretty(&mut output, &report)?;
    output.write_all(b"\n")?;
    Ok(())
}
