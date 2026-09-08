use std::{env, future::Future, pin::Pin};

use tracedecay::session_temporal_benchmark::{
    refresh_contract, run_measurement, validate_contract,
};

fn main() {
    let arguments = env::args()
        .skip(1)
        .filter(|argument| argument != "--bench")
        .collect::<Vec<_>>();
    let result = match arguments.as_slice() {
        [] => validate_contract(),
        [argument] if argument == "--validate-only" => validate_contract(),
        [argument] if argument == "--run" => {
            let runtime = tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()
                .expect("build tokio runtime");
            // Erase the instrumented future's layout at the runtime boundary;
            // block_on still polls and owns the complete benchmark operation.
            let measurement: Pin<Box<dyn Future<Output = Result<_, String>>>> =
                Box::pin(run_measurement());
            runtime.block_on(measurement).map(|value| {
                println!("{}", serde_json::to_string_pretty(&value).unwrap());
            })
        }
        [argument] if argument == "--refresh-contract" => {
            let runtime = tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()
                .expect("build tokio runtime");
            let refresh: Pin<Box<dyn Future<Output = Result<_, String>>>> =
                Box::pin(refresh_contract());
            runtime.block_on(refresh).map(|value| {
                println!("{}", serde_json::to_string_pretty(&value).unwrap());
            })
        }
        _ => Err(
            "usage: cargo bench -p tracedecay --bench session_temporal --features test-helpers [-- --run|--refresh-contract]"
                .to_owned(),
        ),
    };
    if let Err(error) = result {
        eprintln!("session-temporal benchmark: {error}");
        std::process::exit(1);
    }
}
