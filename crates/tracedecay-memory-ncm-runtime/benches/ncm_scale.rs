//! Resource, saturation, and latency evidence for `ncm-biomem-rs.v1`.
//!
//! This is a dependency-free `harness = false` benchmark so the same binary can
//! emit machine-readable evidence and execute discriminating capacity checks.

use serde::Serialize;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::error::Error;
use std::hint::black_box;
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::{Duration, Instant};
use tempfile::TempDir;
use tracedecay_memory_ncm_core::kernel::{NcmKernel, NewRecord};
use tracedecay_memory_ncm_core::recall::RecallPolicy;
use tracedecay_memory_ncm_core::records::RecordInput;
use tracedecay_memory_ncm_core::types::{
    AffectVector, AlgorithmIdentity, CenterSlot, CoreError, EMBEDDING_DIM, LTM_KEY_DIM, Layer,
    LogicalTick, NcmConfig, STM_KEY_DIM, SourceId, VALUE_DIM,
};
use tracedecay_memory_ncm_runtime::client::{
    ClientError, KILL_ESCALATION, MAX_QUEUED_BYTES, MAX_QUEUED_REQUESTS, WorkerClient,
    WorkerOptions,
};
use tracedecay_memory_ncm_runtime::embedding::doubles::HashEncoder;
use tracedecay_memory_ncm_runtime::embedding::{MiniLmEncoder, PinnedEncoder};
use tracedecay_memory_ncm_runtime::engine::{
    MaintenanceKind, MaintenanceRequest, NcmEngine, ObserveRequest, Outcome, RecallRequest,
    RejectReason,
};
use tracedecay_memory_ncm_runtime::ports::{Deadline, StateRoot, TextEncoder};
use tracedecay_memory_ncm_runtime::store::{Capsule, NamespaceStore, StoreError, StoreIdentity};
use tracedecay_memory_ncm_runtime::wire::{
    self, MAX_REPLY_BYTES, MAX_REQUEST_BYTES, Operation, Request,
};

type BenchResult<T> = Result<T, Box<dyn Error>>;
const DEFAULT_OPS: usize = 10_000;
const DEFAULT_DEADLINE_MS: u64 = 60_000;

#[derive(Clone, Debug, Serialize)]
struct Distribution {
    samples: usize,
    min_us: f64,
    p50_us: f64,
    p95_us: f64,
    p99_us: f64,
    max_us: f64,
    mean_us: f64,
    elapsed_ms: f64,
    operations_per_second: f64,
}

#[derive(Clone, Debug, Serialize)]
struct Measurement {
    name: String,
    distribution: Distribution,
    outcomes: Value,
}

fn main() -> BenchResult<()> {
    let args = std::env::args().skip(1).collect::<Vec<_>>();
    let command = args.first().map_or("help", String::as_str);
    let output = match command {
        "kernel" => benchmark_kernel(&args[1..])?,
        "store" => benchmark_store(&args[1..])?,
        "engine" => benchmark_engine(&args[1..])?,
        "maintenance" => benchmark_maintenance(&args[1..])?,
        "ipc" => benchmark_ipc(&args[1..])?,
        "encoder" => benchmark_encoder(&args[1..]),
        "mixed" => benchmark_mixed(&args[1..])?,
        "self-test" => self_test(&args[1..])?,
        "help" | "--help" | "-h" => help(),
        other => return Err(failure(format!("unknown benchmark population: {other}"))),
    };
    let encoded = serde_json::to_vec_pretty(&output)?;
    let mut stdout = io::stdout().lock();
    stdout.write_all(&encoded)?;
    stdout.write_all(b"\n")?;
    Ok(())
}

fn help() -> Value {
    json!({
        "benchmark": "ncm_scale",
        "commands": {
            "kernel": "--ops N --population empty|sparse|full|all",
            "store": "--ops N --record-bytes 64|4096|16384",
            "engine": "--ops N --namespaces 1|4 --record-bytes N",
            "maintenance": "--seed-records N --samples N",
            "ipc": "--ops N --worker /absolute/path",
            "encoder": "--ops N --model-root /absolute/path",
            "mixed": "--ops N --namespaces 1|4",
            "self-test": "--worker /absolute/path (optional IPC controls)"
        }
    })
}

fn benchmark_kernel(args: &[String]) -> BenchResult<Value> {
    let ops = usize_arg(args, "--ops", DEFAULT_OPS)?;
    let population = string_arg(args, "--population").unwrap_or_else(|| "all".to_owned());
    let names: Vec<&str> = if population == "all" {
        vec!["empty", "sparse", "full"]
    } else {
        vec![population.as_str()]
    };
    let mut populations = Vec::new();
    for name in names {
        let (stm_count, ltm_count) = match name {
            "empty" => (0, 0),
            "sparse" => (64, 512),
            "full" => (512, 4096),
            _ => return Err(failure("population must be empty, sparse, full, or all")),
        };
        let mut kernel = populated_kernel(stm_count, ltm_count)?;
        let query = deterministic_vector(EMBEDDING_DIM, 0x51f0_11aa);
        let mut non_empty = 0_usize;
        let recall = measure("kernel_recall", ops, |_| {
            let recalled = kernel.recall(&query, 16, RecallPolicy::default())?;
            if !matches!(
                recalled,
                tracedecay_memory_ncm_core::recall::RecallOutput::Empty
            ) {
                non_empty = non_empty.saturating_add(1);
            }
            black_box(recalled);
            Ok(())
        })?;
        let ltm_query = kernel.projections.project_to_ltm(&query)?;
        let flat = measure("flat_vector_ltm_top16", ops, |_| {
            black_box(flat_top_k(&kernel, &ltm_query, 16));
            Ok(())
        })?;
        let observe_samples = ops.min(128);
        let mut observed = 0_usize;
        let mut rejected_capacity = 0_usize;
        let observe = measure("kernel_observe", observe_samples, |index| {
            let embedding = deterministic_vector(EMBEDDING_DIM, 0x9000 + index as u64);
            match kernel.observe(
                &embedding,
                &embedding,
                NewRecord {
                    source: SourceId(format!("kernel-source-{index}")),
                    key_text: format!("kernel-key-{index}"),
                    value_text: "v".repeat(48),
                    affect: AffectVector::neutral(),
                    surprise: 0.4,
                    intensity: 1.0,
                },
            ) {
                Ok(report) => {
                    observed = observed.saturating_add(1);
                    black_box(report);
                }
                Err(CoreError::CapacityExhausted(_)) => {
                    rejected_capacity = rejected_capacity.saturating_add(1);
                }
                Err(error) => return Err(Box::new(error)),
            }
            Ok(())
        })?;
        let maintenance_samples = ops.min(16);
        let mut maintenance_kernel = populated_kernel(stm_count, ltm_count)?;
        let consolidate = measure("kernel_consolidate", maintenance_samples, |_| {
            match maintenance_kernel.consolidate() {
                Ok(report) => {
                    black_box(report);
                }
                Err(CoreError::CapacityExhausted(_)) => {}
                Err(error) => return Err(Box::new(error)),
            }
            Ok(())
        })?;
        populations.push(json!({
            "population": name,
            "stm_active_at_start": stm_count,
            "ltm_active_at_start": ltm_count,
            "capacity": {"stm": 512, "ltm": 4096},
            "measurements": [
                with_outcomes(recall, json!({"non_empty": non_empty})),
                with_outcomes(flat, json!({"representation": "same normalized LTM center keys", "semantics": "distance-only brute-force top-k; no hydration"})),
                with_outcomes(observe, json!({"success": observed, "capacity_rejected": rejected_capacity})),
                with_outcomes(consolidate, json!({"stateful_trace": true}))
            ]
        }));
    }
    Ok(envelope(
        "kernel",
        json!({
            "requested_ops": ops,
            "populations": populations,
            "warning": "Pre-embedded kernel timings exclude tokenization, ONNX inference, IPC, SQLite commit, and text hydration. They are not full-text recall latencies."
        }),
    ))
}

fn benchmark_store(args: &[String]) -> BenchResult<Value> {
    let ops = usize_arg(args, "--ops", DEFAULT_OPS)?;
    let record_bytes = usize_arg(args, "--record-bytes", 64)?;
    if record_bytes > 16 * 1024 {
        return Err(failure("record bytes exceeds frozen 16 KiB bound"));
    }
    let temp = TempDir::new()?;
    let root = StateRoot::new(temp.path())?;
    let namespace = namespace(1);
    let identity = store_identity();
    let mut store = NamespaceStore::create(&root, &namespace, identity.clone())?;
    let mut committed = 0_usize;
    let mut budget_rejected = 0_usize;
    let mut other_rejected = 0_usize;
    let commit = measure("durable_commit", ops, |index| {
        let mut mutation = store.begin_mutation()?;
        let key_len = record_bytes.min(32);
        let value_len = record_bytes.saturating_sub(key_len);
        let capsule = capsule(
            &format!("source-{index}"),
            &"k".repeat(key_len),
            &"v".repeat(value_len),
        );
        match mutation.insert_capsule(capsule) {
            Ok(_) => match mutation.append_event(
                "observe",
                Some(&format!("unique-{index}")),
                &"a".repeat(64),
                "{}",
                index as u64,
            ) {
                Ok(_) => {
                    mutation.commit()?;
                    committed = committed.saturating_add(1);
                }
                Err(StoreError::BudgetExceeded) => {
                    budget_rejected = budget_rejected.saturating_add(1);
                }
                Err(_) => {
                    other_rejected = other_rejected.saturating_add(1);
                }
            },
            Err(StoreError::BudgetExceeded) => {
                budget_rejected = budget_rejected.saturating_add(1);
            }
            Err(_) => {
                other_rejected = other_rejected.saturating_add(1);
            }
        }
        Ok(())
    })?;
    let before_compact = store.usage()?;
    let compact_started = Instant::now();
    let after_compact = store.compact(false)?;
    let compact_us = compact_started.elapsed().as_secs_f64() * 1_000_000.0;
    drop(store);
    let reopened = NamespaceStore::open(&root, &namespace, &identity)?;
    let reopened_capsules = reopened.capsules_in_commit_order(true)?.len();
    let reopened_events = reopened.events_after(0)?.len();
    let quota = reopened.quota();
    Ok(envelope(
        "store",
        json!({
            "requested_ops": ops,
            "record_bytes": record_bytes,
            "measurement": with_outcomes(commit, json!({
                "committed": committed,
                "budget_rejected_before_effect": budget_rejected,
                "other_rejected": other_rejected
            })),
            "quota": quota,
            "usage_before_compact": before_compact,
            "usage_after_compact": after_compact,
            "compact_us": compact_us,
            "reopen": {"capsules": reopened_capsules, "events": reopened_events},
            "idempotency_retention": "Unique idempotency rows are retained until the 64 MiB source-basis quota rejects new ingestion; checkpoint/VACUUM compaction does not prune event rows."
        }),
    ))
}

fn benchmark_engine(args: &[String]) -> BenchResult<Value> {
    let ops = usize_arg(args, "--ops", DEFAULT_OPS)?;
    let namespaces = usize_arg(args, "--namespaces", 1)?;
    let record_bytes = usize_arg(args, "--record-bytes", 64)?;
    if !matches!(namespaces, 1 | 4) || record_bytes > 16 * 1024 {
        return Err(failure(
            "engine requires 1 or 4 namespaces and record bytes <= 16384",
        ));
    }
    let temp = TempDir::new()?;
    let root = StateRoot::new(temp.path())?;
    let engine = NcmEngine::new(root, Arc::new(HashEncoder::new()), NcmConfig::default());
    let namespace_ids = (0..namespaces).map(namespace).collect::<Vec<_>>();
    let observe_ops = (ops / 10).max(1).min(512_usize.saturating_mul(namespaces));
    let recall_ops = ops.saturating_sub(observe_ops).max(1);
    let mut success = 0_usize;
    let mut rejected = 0_usize;
    let observe = measure("end_to_end_hash_observe", observe_ops, |index| {
        let ns = &namespace_ids[index % namespaces];
        let reply = engine.observe(ns, observe_request(index, record_bytes, index % 17)?);
        match reply.outcome {
            Outcome::Success => success = success.saturating_add(1),
            Outcome::BudgetExceeded | Outcome::Rejected(_) => rejected = rejected.saturating_add(1),
            Outcome::Cancelled => return Err(failure("unexpected observe cancellation")),
            _ => rejected = rejected.saturating_add(1),
        }
        black_box(reply);
        Ok(())
    })?;
    let mut recall_success = 0_usize;
    let recall = measure("end_to_end_hash_recall", recall_ops, |index| {
        let reply = engine.recall(
            &namespace_ids[index % namespaces],
            RecallRequest {
                query_text: format!("key-{}", index % ops.max(1)),
                top_k: 16,
                deadline: deadline(),
            },
        );
        if matches!(reply.outcome, Outcome::Success) {
            recall_success = recall_success.saturating_add(1);
        }
        black_box(reply);
        Ok(())
    })?;
    let health = engine.health();
    let inspections = namespace_ids
        .iter()
        .map(|ns| engine.inspection(ns).payload)
        .collect::<Vec<_>>();
    Ok(envelope(
        "engine",
        json!({
            "requested_trace_ops": ops,
            "observe_samples": observe_ops,
            "recall_samples": recall_ops,
            "resident_namespaces": namespaces,
            "record_bytes": record_bytes,
            "measurements": [
                with_outcomes(observe, json!({"success": success, "rejected": rejected})),
                with_outcomes(recall, json!({"success": recall_success}))
            ],
            "health": health.payload,
            "inspections": inspections,
            "encoder": "HashEncoder test double; ONNX inference measured separately"
        }),
    ))
}

fn benchmark_maintenance(args: &[String]) -> BenchResult<Value> {
    let seed_records = usize_arg(args, "--seed-records", 128)?;
    let samples = usize_arg(args, "--samples", 10)?;
    let temp = TempDir::new()?;
    let root = StateRoot::new(temp.path())?;
    let engine = NcmEngine::new(root, Arc::new(HashEncoder::new()), NcmConfig::default());
    let ns = namespace(7);
    for index in 0..seed_records {
        let reply = engine.observe(&ns, observe_request(index, 64, index % 8)?);
        if !matches!(reply.outcome, Outcome::Success) {
            break;
        }
    }
    let kinds = [
        MaintenanceKind::Consolidate,
        MaintenanceKind::MergePrune,
        MaintenanceKind::Compact,
    ];
    let mut results = Vec::new();
    for kind in kinds {
        let label = format!("maintenance_{}", maintenance_label(&kind));
        let mut success = 0_usize;
        let measurement = measure(&label, samples, |index| {
            let reply = engine.maintenance(
                &ns,
                MaintenanceRequest {
                    idempotency_key: format!("{label}-{index}"),
                    kind: kind.clone(),
                    deadline: deadline(),
                },
            );
            if matches!(reply.outcome, Outcome::Success) {
                success = success.saturating_add(1);
            }
            black_box(reply);
            Ok(())
        })?;
        results.push(with_outcomes(measurement, json!({"success": success})));
    }
    Ok(envelope(
        "maintenance",
        json!({"seed_records": seed_records, "samples": samples, "measurements": results, "inspection": engine.inspection(&ns).payload}),
    ))
}

fn benchmark_ipc(args: &[String]) -> BenchResult<Value> {
    let ops = usize_arg(args, "--ops", DEFAULT_OPS)?;
    let worker = worker_arg(args)?;
    let temp = TempDir::new()?;
    let options = WorkerOptions {
        test_double: true,
        reconciliation_deadline: Duration::from_secs(5),
        ..WorkerOptions::default()
    };
    let client = Arc::new(WorkerClient::spawn(&worker, temp.path(), options)?);
    let mut success = 0_usize;
    let round_trip = measure("ipc_health_round_trip", ops, |index| {
        let reply = client.call(
            Request::new(index as u64 + 1, 0, Operation::Health, "", json!({})),
            Duration::from_secs(5),
        )?;
        if matches!(reply.outcome, Outcome::Success) {
            success = success.saturating_add(1);
        }
        black_box(reply);
        Ok(())
    })?;
    let queue = queue_saturation(Arc::clone(&client))?;
    let cancellation_started = Instant::now();
    let cancelled = client.call(
        Request::new(
            900_000,
            0,
            Operation::Health,
            "",
            json!({"test_sleep_before_ms": 1_000}),
        ),
        Duration::from_millis(30),
    );
    let cancellation_ms = cancellation_started.elapsed().as_secs_f64() * 1_000.0;
    let cancellation_kind = match cancelled {
        Err(ClientError::Cancelled) => "cancelled",
        Err(ClientError::EffectUnknown { .. }) => "effect_unknown",
        Err(_) => "other_error",
        Ok(_) => "unexpected_success",
    };
    Ok(envelope(
        "ipc",
        json!({
            "worker": worker,
            "requested_ops": ops,
            "measurement": with_outcomes(round_trip, json!({"success": success})),
            "queue": queue,
            "cancellation": {
                "completion_ms": cancellation_ms,
                "outcome": cancellation_kind,
                "kill_escalation_limit_ms": KILL_ESCALATION.as_millis()
            },
            "mailbox_limits": {"requests": MAX_QUEUED_REQUESTS, "bytes": MAX_QUEUED_BYTES}
        }),
    ))
}

fn benchmark_encoder(args: &[String]) -> Value {
    match encoder_inner(args) {
        Ok(value) => value,
        Err(error) => envelope(
            "encoder",
            json!({
                "status": "blocked_environment",
                "detail": error.to_string(),
                "requirement": "Install the five pinned Xenova artifacts under <state-root>/models and rerun with default features. Missing model is not a waived population."
            }),
        ),
    }
}

fn encoder_inner(args: &[String]) -> BenchResult<Value> {
    let ops = usize_arg(args, "--ops", 100)?;
    let model_root = string_arg(args, "--model-root")
        .or_else(|| std::env::var("NCM_MODEL_ROOT").ok())
        .ok_or_else(|| failure("--model-root or NCM_MODEL_ROOT is required"))?;
    let root = StateRoot::new(PathBuf::from(model_root))?;
    let pinned = PinnedEncoder::reference()?;
    let opened = Instant::now();
    let encoder = MiniLmEncoder::open(&root, &pinned)?;
    let open_ms = opened.elapsed().as_secs_f64() * 1_000.0;
    let single = measure("real_encoder_single", ops, |_| {
        let encoded = encoder.encode(&["bounded multilingual memory benchmark"], deadline())?;
        black_box(encoded);
        Ok(())
    })?;
    let batch = vec!["bounded multilingual memory benchmark"; 16];
    let batch16 = measure("real_encoder_batch16", ops, |_| {
        let encoded = encoder.encode(&batch, deadline())?;
        black_box(encoded);
        Ok(())
    })?;
    Ok(envelope(
        "encoder",
        json!({
            "status": "measured",
            "open_ms": open_ms,
            "identity": {
                "model": encoder.identity().model,
                "artifact_sha256": encoder.identity().artifact_sha256,
                "max_length": encoder.identity().max_length
            },
            "measurements": [
                with_outcomes(single, json!({"batch": 1})),
                with_outcomes(batch16, json!({"batch": 16}))
            ]
        }),
    ))
}

fn benchmark_mixed(args: &[String]) -> BenchResult<Value> {
    let ops = usize_arg(args, "--ops", DEFAULT_OPS)?;
    let namespace_count = usize_arg(args, "--namespaces", 1)?;
    if !matches!(namespace_count, 1 | 4) {
        return Err(failure("mixed workload requires 1 or 4 namespaces"));
    }
    let temp = TempDir::new()?;
    let root = StateRoot::new(temp.path())?;
    let config = NcmConfig::default();
    let encoder: Arc<dyn TextEncoder> = Arc::new(HashEncoder::new());
    let mut engine = NcmEngine::new(root.clone(), Arc::clone(&encoder), config.clone());
    let namespaces = (0..namespace_count)
        .map(|index| namespace(index + 20))
        .collect::<Vec<_>>();
    for (index, ns) in namespaces.iter().enumerate() {
        let reply = engine.observe(ns, observe_request(index, 64, index)?);
        if !matches!(reply.outcome, Outcome::Success) {
            return Err(failure("mixed workload seed observe failed"));
        }
    }
    let mut counts = serde_json::Map::new();
    let trace = measure("mixed_trace", ops, |index| {
        let ns = &namespaces[index % namespace_count];
        let selector = index % 1_000;
        let (kind, outcome) = if selector < 970 {
            let reply = engine.recall(
                ns,
                RecallRequest {
                    query_text: format!("key-{}", index % 128),
                    top_k: 16,
                    deadline: deadline(),
                },
            );
            ("recall", reply.outcome)
        } else if selector < 990 {
            let reply = engine.observe(ns, observe_request(index + 10_000, 64, index % 12)?);
            ("observe", reply.outcome)
        } else if selector < 994 {
            let reply = engine.maintenance(
                ns,
                MaintenanceRequest {
                    idempotency_key: format!("mixed-consolidate-{index}"),
                    kind: MaintenanceKind::Consolidate,
                    deadline: deadline(),
                },
            );
            ("consolidate", reply.outcome)
        } else if selector == 994 {
            let reply = engine.maintenance(
                ns,
                MaintenanceRequest {
                    idempotency_key: format!("mixed-merge-{index}"),
                    kind: MaintenanceKind::MergePrune,
                    deadline: deadline(),
                },
            );
            ("merge_prune", reply.outcome)
        } else if selector == 995 {
            let source = SourceId(format!("source-{}", index % 12));
            let reply =
                engine.delete_by_source(ns, &source, &format!("mixed-delete-{index}"), deadline());
            ("delete", reply.outcome)
        } else if selector == 996 {
            let reply = engine.maintenance(
                ns,
                MaintenanceRequest {
                    idempotency_key: format!("mixed-compact-{index}"),
                    kind: MaintenanceKind::Compact,
                    deadline: deadline(),
                },
            );
            ("compact", reply.outcome)
        } else if selector == 999 {
            engine = NcmEngine::new(root.clone(), Arc::clone(&encoder), config.clone());
            let reply = engine.inspection(ns);
            ("restart", reply.outcome)
        } else {
            let reply = engine.recall(
                ns,
                RecallRequest {
                    query_text: format!("key-{}", index % 128),
                    top_k: 16,
                    deadline: deadline(),
                },
            );
            ("recall", reply.outcome)
        };
        increment_count(&mut counts, &format!("{kind}:{outcome:?}"));
        Ok(())
    })?;
    Ok(envelope(
        "mixed",
        json!({
            "requested_ops": ops,
            "namespaces": namespace_count,
            "measurement": with_outcomes(trace, Value::Object(counts)),
            "profile": "frozen NcmConfig::default with HashEncoder; real encoder is a separate population"
        }),
    ))
}

fn self_test(args: &[String]) -> BenchResult<Value> {
    let mut controls = vec![
        test_record_bound()?,
        test_kernel_capacity()?,
        test_wire_bound()?,
        test_source_quota()?,
        test_compaction_reopen_digest()?,
        test_engine_bounds()?,
    ];
    if let Some(worker) = string_arg(args, "--worker") {
        controls.push(test_ipc_bounds(Path::new(&worker))?);
    } else {
        controls.push(json!({
            "name": "ipc_mailbox_and_cancellation",
            "status": "blocked_environment",
            "detail": "pass --worker /absolute/path to execute process controls"
        }));
    }
    Ok(envelope(
        "self-test",
        json!({
            "status": "pass",
            "controls": controls,
            "negative_control_rule": "Each passing control names the assertion that a no-op or bound-removal mutant would fail."
        }),
    ))
}

fn test_record_bound() -> BenchResult<Value> {
    let mut kernel = NcmKernel::new(1, NcmConfig::default())?;
    let before = kernel.state_digest();
    let embedding = deterministic_vector(EMBEDDING_DIM, 4);
    let result = kernel.observe(
        &embedding,
        &embedding,
        NewRecord {
            source: SourceId("oversize-source".to_owned()),
            key_text: "k".repeat(16 * 1024),
            value_text: "v".to_owned(),
            affect: AffectVector::neutral(),
            surprise: 0.0,
            intensity: 1.0,
        },
    );
    require(
        matches!(result, Err(CoreError::BudgetExceeded("record text"))),
        "oversized record was not rejected",
    )?;
    require(
        before == kernel.state_digest(),
        "oversized record mutated kernel state",
    )?;
    Ok(json!({
        "name": "record_16k_pre_mutation",
        "status": "pass",
        "mutant_caught": "Removing the max_record_bytes check makes the expected BudgetExceeded assertion fail; mutating before validation changes the digest equality assertion."
    }))
}

fn test_kernel_capacity() -> BenchResult<Value> {
    let mut kernel = populated_kernel(512, 0)?;
    let before_records = kernel.records.len();
    let embedding = deterministic_vector(EMBEDDING_DIM, 0xfedc_ba98);
    let result = kernel.observe(
        &embedding,
        &embedding,
        NewRecord {
            source: SourceId("capacity-source".to_owned()),
            key_text: "capacity key".to_owned(),
            value_text: "capacity value".to_owned(),
            affect: AffectVector::neutral(),
            surprise: 1.0,
            intensity: 1.0,
        },
    );
    require(
        matches!(result, Err(CoreError::CapacityExhausted(Layer::Stm))),
        "full STM did not reject a novel write",
    )?;
    require(
        kernel.records.len() == before_records,
        "capacity rejection retained a new record",
    )?;
    Ok(json!({
        "name": "fixed_center_capacity",
        "status": "pass",
        "mutant_caught": "A dynamic-growth or silent-reinforcement mutant makes the CapacityExhausted assertion fail; a non-atomic mutant changes the record count."
    }))
}

fn test_wire_bound() -> BenchResult<Value> {
    let request = Request::new(
        1,
        1_000,
        Operation::Recall,
        namespace(1),
        json!({"query_text": "q".repeat(MAX_REQUEST_BYTES), "top_k": 1}),
    );
    let encoded = wire::encode_request(&request);
    require(
        matches!(encoded, Err(wire::FrameError::Oversized { .. })),
        "oversized request encoded",
    )?;
    Ok(json!({
        "name": "wire_request_bound",
        "status": "pass",
        "limits": {"request": MAX_REQUEST_BYTES, "reply": MAX_REPLY_BYTES},
        "mutant_caught": "Removing the encode-side frame check makes the expected Oversized assertion fail."
    }))
}

fn test_source_quota() -> BenchResult<Value> {
    let temp = TempDir::new()?;
    let root = StateRoot::new(temp.path())?;
    let ns = namespace(3);
    let mut store = NamespaceStore::create(&root, &ns, store_identity())?;
    let quota = store.quota();
    let first_bytes: usize = 63 * 1024 * 1024;
    let first_receipt = format!("\"{}\"", "x".repeat(first_bytes.saturating_sub(2)));
    let mut first = store.begin_mutation()?;
    first.append_event(
        "quota",
        Some("quota-first"),
        &"a".repeat(64),
        &first_receipt,
        0,
    )?;
    first.commit()?;
    let before = store.usage()?;
    let second_receipt = format!("\"{}\"", "y".repeat(2 * 1024 * 1024));
    let mut second = store.begin_mutation()?;
    let rejected = second.append_event(
        "quota",
        Some("quota-second"),
        &"b".repeat(64),
        &second_receipt,
        0,
    );
    require(
        matches!(rejected, Err(StoreError::BudgetExceeded)),
        "source quota did not reject before insertion",
    )?;
    drop(second);
    let after = store.usage()?;
    require(
        after.source_basis_bytes() == before.source_basis_bytes(),
        "rejected quota write changed basis usage",
    )?;
    require(
        after.source_basis_bytes() <= quota.source_basis_bytes,
        "source basis exceeded quota",
    )?;
    Ok(json!({
        "name": "source_basis_quota",
        "status": "pass",
        "quota": quota,
        "usage": after,
        "mutant_caught": "Removing the preflight makes the second append succeed; checking after INSERT changes source_basis_bytes despite the rejected transaction assertion."
    }))
}

fn test_compaction_reopen_digest() -> BenchResult<Value> {
    let temp = TempDir::new()?;
    let root = StateRoot::new(temp.path())?;
    let ns = namespace(4);
    let encoder: Arc<dyn TextEncoder> = Arc::new(HashEncoder::new());
    let config = NcmConfig::default();
    let engine = NcmEngine::new(root.clone(), Arc::clone(&encoder), config.clone());
    for index in 0..8 {
        let reply = engine.observe(&ns, observe_request(index, 64, index % 3)?);
        require(
            matches!(reply.outcome, Outcome::Success),
            "seed observe failed",
        )?;
    }
    let checkpoint = engine.maintenance(
        &ns,
        MaintenanceRequest {
            idempotency_key: "self-checkpoint".to_owned(),
            kind: MaintenanceKind::Checkpoint,
            deadline: deadline(),
        },
    );
    require(
        matches!(checkpoint.outcome, Outcome::Success),
        "checkpoint failed",
    )?;
    let before = engine.inspection(&ns);
    let before_digest = before.payload["state_digest"]
        .as_str()
        .ok_or_else(|| failure("missing before digest"))?
        .to_owned();
    let compact = engine.maintenance(
        &ns,
        MaintenanceRequest {
            idempotency_key: "self-compact".to_owned(),
            kind: MaintenanceKind::Compact,
            deadline: deadline(),
        },
    );
    require(
        matches!(compact.outcome, Outcome::Success),
        "compact failed",
    )?;
    let after_compact = engine.inspection(&ns);
    require(
        after_compact.payload["state_digest"] == before_digest,
        "compact changed kernel digest",
    )?;
    drop(engine);
    let reopened = NcmEngine::new(root, encoder, config);
    let after_reopen = reopened.inspection(&ns);
    require(
        matches!(after_reopen.outcome, Outcome::Success),
        "reopen failed",
    )?;
    require(
        after_reopen.payload["state_digest"] == before_digest,
        "reopen digest differs after compact",
    )?;
    Ok(json!({
        "name": "checkpoint_compact_reopen_retention",
        "status": "pass",
        "digest": before_digest,
        "mutant_caught": "Dropping events/checkpoint state during compaction or replay makes either post-compact or post-reopen digest equality fail."
    }))
}

fn test_engine_bounds() -> BenchResult<Value> {
    let temp = TempDir::new()?;
    let root = StateRoot::new(temp.path())?;
    let engine = NcmEngine::new(root, Arc::new(HashEncoder::new()), NcmConfig::default());
    let ns = namespace(5);
    let seed = engine.observe(&ns, observe_request(1, 64, 0)?);
    require(
        matches!(seed.outcome, Outcome::Success),
        "engine seed failed",
    )?;
    let top_k = engine.recall(
        &ns,
        RecallRequest {
            query_text: "key".to_owned(),
            top_k: 17,
            deadline: deadline(),
        },
    );
    require(
        matches!(
            top_k.outcome,
            Outcome::Rejected(RejectReason::InvalidRequest(_))
        ),
        "top_k 17 was not rejected",
    )?;
    let advance = engine.maintenance(
        &ns,
        MaintenanceRequest {
            idempotency_key: "advance-over-limit".to_owned(),
            kind: MaintenanceKind::Advance { ticks: 10_001 },
            deadline: deadline(),
        },
    );
    require(
        matches!(advance.outcome, Outcome::BudgetExceeded),
        "advance 10001 was not budget rejected",
    )?;
    for index in 0..5 {
        let reply = engine.observe(
            &namespace(index + 40),
            observe_request(index + 100, 64, index)?,
        );
        require(
            matches!(reply.outcome, Outcome::Success),
            "resident namespace seed failed",
        )?;
    }
    let health = engine.health();
    require(
        health.payload["resident_namespaces"] == 4,
        "resident LRU exceeded four",
    )?;
    require(
        health.payload["catalog_namespaces"]
            .as_u64()
            .is_some_and(|count| count >= 6),
        "catalog count missing created namespaces",
    )?;
    Ok(json!({
        "name": "engine_topk_advance_resident_bounds",
        "status": "pass",
        "health": health.payload,
        "mutant_caught": "Removing top_k, advance, or LRU bounds respectively changes the rejection outcomes or resident_namespaces == 4 assertion."
    }))
}

fn test_ipc_bounds(worker: &Path) -> BenchResult<Value> {
    require(worker.is_absolute(), "worker path must be absolute")?;
    let temp = TempDir::new()?;
    let client = Arc::new(WorkerClient::spawn(
        worker,
        temp.path(),
        WorkerOptions {
            test_double: true,
            ..WorkerOptions::default()
        },
    )?);
    let queue = queue_saturation(Arc::clone(&client))?;
    require(
        queue["busy"].as_u64().is_some_and(|count| count > 0),
        "mailbox saturation produced no Busy result",
    )?;
    let started = Instant::now();
    let result = client.call(
        Request::new(
            77,
            0,
            Operation::Health,
            "",
            json!({"test_sleep_before_ms": 1_000}),
        ),
        Duration::from_millis(30),
    );
    let elapsed = started.elapsed();
    require(
        matches!(result, Err(ClientError::Cancelled)),
        "non-mutating deadline was not cancelled",
    )?;
    require(
        elapsed <= Duration::from_millis(400),
        "cancellation exceeded kill escalation envelope",
    )?;
    Ok(json!({
        "name": "ipc_mailbox_and_cancellation",
        "status": "pass",
        "queue": queue,
        "cancellation_ms": elapsed.as_secs_f64() * 1000.0,
        "mutant_caught": "An unbounded mailbox yields zero Busy results; removing kill escalation makes the <=400 ms cancellation assertion fail."
    }))
}

fn queue_saturation(client: Arc<WorkerClient>) -> BenchResult<Value> {
    const CALLS: usize = 41;
    let barrier = Arc::new(Barrier::new(CALLS + 1));
    let mut handles = Vec::with_capacity(CALLS);
    for index in 0..CALLS {
        let client = Arc::clone(&client);
        let barrier = Arc::clone(&barrier);
        handles.push(thread::spawn(move || {
            barrier.wait();
            let payload = if index == 0 {
                json!({"test_sleep_before_ms": 500})
            } else {
                json!({})
            };
            client.call(
                Request::new(800_000 + index as u64, 0, Operation::Health, "", payload),
                Duration::from_secs(3),
            )
        }));
    }
    barrier.wait();
    let mut busy = 0_usize;
    let mut success = 0_usize;
    let mut other = 0_usize;
    for handle in handles {
        match handle
            .join()
            .map_err(|_| failure("queue worker thread panicked"))?
        {
            Err(ClientError::Busy) => busy = busy.saturating_add(1),
            Ok(_) => success = success.saturating_add(1),
            Err(_) => other = other.saturating_add(1),
        }
    }
    Ok(
        json!({"submitted": CALLS, "capacity": MAX_QUEUED_REQUESTS, "busy": busy, "success": success, "other": other}),
    )
}

fn populated_kernel(stm_count: usize, ltm_count: usize) -> BenchResult<NcmKernel> {
    let mut kernel = NcmKernel::new(0x05ee_d020, NcmConfig::default())?;
    fill_layer(&mut kernel, Layer::Stm, stm_count)?;
    fill_layer(&mut kernel, Layer::Ltm, ltm_count)?;
    Ok(kernel)
}

fn fill_layer(kernel: &mut NcmKernel, layer: Layer, count: usize) -> BenchResult<()> {
    let capacity = match layer {
        Layer::Stm => kernel.stm.config.n_centers,
        Layer::Ltm => kernel.ltm.config.n_centers,
    };
    if count > capacity {
        return Err(failure("fixture population exceeds center capacity"));
    }
    for index in 0..count {
        let stm_key = deterministic_vector(STM_KEY_DIM, 0x1000 + index as u64);
        let ltm_key = deterministic_vector(LTM_KEY_DIM, 0x2000 + index as u64);
        let record_id = kernel.records.insert(RecordInput {
            source: SourceId(format!("fixture-{layer:?}-{index}")),
            key_text: format!("fixture-key-{layer:?}-{index}"),
            value_text: "fixture-value".to_owned(),
            created_tick: LogicalTick(0),
            ltm_key: ltm_key.clone(),
            stm_key: stm_key.clone(),
            value_vec: deterministic_vector(VALUE_DIM, 0x3000 + index as u64),
            affect: AffectVector::neutral(),
        })?;
        let slot = CenterSlot {
            layer,
            index: u32::try_from(index)?,
            incarnation: 1,
        };
        {
            let centers = match layer {
                Layer::Stm => &mut kernel.stm,
                Layer::Ltm => &mut kernel.ltm,
            };
            let key = if layer == Layer::Stm {
                &stm_key
            } else {
                &ltm_key
            };
            let d_key = centers.config.d_key;
            centers.keys[index * d_key..(index + 1) * d_key].copy_from_slice(key);
            centers.ltm_keys[index * LTM_KEY_DIM..(index + 1) * LTM_KEY_DIM]
                .copy_from_slice(&ltm_key);
            centers.values[index * VALUE_DIM..(index + 1) * VALUE_DIM]
                .copy_from_slice(&deterministic_vector(VALUE_DIM, 0x4000 + index as u64));
            centers.intensity[index] = 1.0 + (index % 17) as f32 * 0.01;
            centers.active[index] = true;
            centers.incarnation[index] = 1;
            centers.record[index] = Some(record_id);
            centers.support[index] = vec![record_id];
        }
        kernel.support.set_support(slot, &[record_id])?;
    }
    Ok(())
}

fn flat_top_k(kernel: &NcmKernel, query: &[f32], top_k: usize) -> Vec<(usize, f32)> {
    let mut distances = kernel
        .ltm
        .active
        .iter()
        .enumerate()
        .filter(|(_, active)| **active)
        .map(|(index, _)| {
            let dot = kernel
                .ltm
                .key(index)
                .iter()
                .zip(query.iter())
                .map(|(left, right)| left * right)
                .sum::<f32>();
            (index, 2.0 - 2.0 * dot.clamp(-1.0, 1.0))
        })
        .collect::<Vec<_>>();
    distances.sort_by(|left, right| {
        left.1
            .total_cmp(&right.1)
            .then_with(|| left.0.cmp(&right.0))
    });
    distances.truncate(top_k.min(distances.len()));
    distances
}

fn deterministic_vector(dimension: usize, seed: u64) -> Vec<f32> {
    let mut state = seed | 1;
    let mut values = Vec::with_capacity(dimension);
    for _ in 0..dimension {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        let unit = (state as f64 / u64::MAX as f64) as f32;
        values.push(unit * 2.0 - 1.0);
    }
    let norm = values.iter().map(|value| value * value).sum::<f32>().sqrt();
    if norm > 0.0 {
        for value in &mut values {
            *value /= norm;
        }
    }
    values
}

fn capsule(source: &str, key: &str, value: &str) -> Capsule {
    Capsule::new(
        SourceId(source.to_owned()),
        key.to_owned(),
        value.to_owned(),
        AffectVector::neutral(),
        0.4,
        1.0,
        deterministic_vector(EMBEDDING_DIM, 11),
        deterministic_vector(EMBEDDING_DIM, 12),
        deterministic_vector(LTM_KEY_DIM, 13),
        "{}".to_owned(),
    )
}

fn store_identity() -> StoreIdentity {
    let projection_bytes = (0_u8..=31).collect::<Vec<_>>();
    let projection_sha256 = hex(&Sha256::digest(&projection_bytes));
    StoreIdentity {
        algorithm: AlgorithmIdentity {
            profile: "ncm-biomem-rs.v1".to_owned(),
            config_sha256: "benchmark-config".to_owned(),
        },
        projection_sha256,
        encoder_model: "test-double/hash".to_owned(),
        encoder_artifact_sha256: "test-double/hash-v1".to_owned(),
        projection_bytes,
        seed: 7,
        config_json: "{}".to_owned(),
    }
}

fn observe_request(
    index: usize,
    record_bytes: usize,
    source_index: usize,
) -> BenchResult<ObserveRequest> {
    let key_len = record_bytes.min(32);
    let value_len = record_bytes.saturating_sub(key_len);
    let mut request = ObserveRequest {
        idempotency_key: format!("observe-{index}"),
        payload_sha256: String::new(),
        source: SourceId(format!("source-{source_index}")),
        key_text: if key_len == 0 {
            String::new()
        } else {
            format!("{:k<width$}", format!("key-{index}"), width = key_len)
        },
        value_text: "v".repeat(value_len),
        affect: None,
        surprise: 0.4,
        intensity: 1.0,
        provenance: json!({"benchmark": "ncm_scale"}),
        deadline: deadline(),
    };
    request.payload_sha256 = request.canonical_payload_sha256().map_err(failure)?;
    Ok(request)
}

fn deadline() -> Deadline {
    Deadline {
        remaining_ms: DEFAULT_DEADLINE_MS,
    }
}

fn namespace(index: usize) -> String {
    format!("{index:02x}{}", "0".repeat(62))
}

fn worker_arg(args: &[String]) -> BenchResult<PathBuf> {
    let explicit = string_arg(args, "--worker").map(PathBuf::from);
    let environment = std::env::var_os("NCM_WORKER_BINARY").map(PathBuf::from);
    let inferred = std::env::current_exe()
        .ok()
        .and_then(|path| path.parent().and_then(Path::parent).map(Path::to_path_buf))
        .map(|path| path.join("tracedecay-ncm-worker"));
    let worker = explicit
        .or(environment)
        .or(inferred)
        .ok_or_else(|| failure("worker binary path unavailable"))?;
    if !worker.is_absolute() || !worker.is_file() {
        return Err(failure(format!(
            "worker binary is not an absolute file: {}",
            worker.display()
        )));
    }
    Ok(worker)
}

fn measure<F>(name: &str, operations: usize, mut operation: F) -> BenchResult<Measurement>
where
    F: FnMut(usize) -> BenchResult<()>,
{
    if operations == 0 {
        return Err(failure("measurement operations must be positive"));
    }
    let mut samples = Vec::with_capacity(operations);
    let trace_started = Instant::now();
    for index in 0..operations {
        let started = Instant::now();
        operation(index)?;
        samples.push(started.elapsed().as_nanos() as u64);
    }
    let elapsed = trace_started.elapsed();
    Ok(Measurement {
        name: name.to_owned(),
        distribution: distribution(samples, elapsed),
        outcomes: Value::Null,
    })
}

fn distribution(mut samples: Vec<u64>, elapsed: Duration) -> Distribution {
    samples.sort_unstable();
    let count = samples.len();
    let sum = samples.iter().map(|value| *value as f64).sum::<f64>();
    let elapsed_seconds = elapsed.as_secs_f64();
    Distribution {
        samples: count,
        min_us: samples[0] as f64 / 1_000.0,
        p50_us: percentile(&samples, 50) as f64 / 1_000.0,
        p95_us: percentile(&samples, 95) as f64 / 1_000.0,
        p99_us: percentile(&samples, 99) as f64 / 1_000.0,
        max_us: samples[count - 1] as f64 / 1_000.0,
        mean_us: sum / count as f64 / 1_000.0,
        elapsed_ms: elapsed_seconds * 1_000.0,
        operations_per_second: count as f64 / elapsed_seconds.max(f64::EPSILON),
    }
}

fn percentile(sorted: &[u64], percentile: usize) -> u64 {
    let numerator = percentile.saturating_mul(sorted.len());
    let rank = numerator
        .div_ceil(100)
        .saturating_sub(1)
        .min(sorted.len() - 1);
    sorted[rank]
}

fn with_outcomes(mut measurement: Measurement, outcomes: Value) -> Measurement {
    measurement.outcomes = outcomes;
    measurement
}

fn envelope(population: &str, data: Value) -> Value {
    json!({
        "schema_version": 1,
        "profile": "ncm-biomem-rs.v1",
        "population": population,
        "commit": git_commit(),
        "process_rss_kib_snapshot": process_rss_kib(),
        "allocated_bytes": null,
        "allocated_bytes_note": "No allocator instrumentation dependency is present; /usr/bin/time -l peak RSS is attached by scripts/product/ncm/benchmark/run.sh.",
        "data": data
    })
}

fn git_commit() -> Option<String> {
    Command::new("git")
        .args(["rev-parse", "HEAD"])
        .output()
        .ok()
        .filter(|output| output.status.success())
        .and_then(|output| String::from_utf8(output.stdout).ok())
        .map(|value| value.trim().to_owned())
}

fn process_rss_kib() -> Option<u64> {
    let pid = std::process::id().to_string();
    Command::new("ps")
        .args(["-o", "rss=", "-p", &pid])
        .output()
        .ok()
        .filter(|output| output.status.success())
        .and_then(|output| String::from_utf8(output.stdout).ok())
        .and_then(|value| value.trim().parse::<u64>().ok())
}

fn usize_arg(args: &[String], name: &str, default: usize) -> BenchResult<usize> {
    match value_after(args, name) {
        Some(value) => value
            .parse::<usize>()
            .map_err(|error| failure(format!("invalid {name}: {error}"))),
        None => Ok(default),
    }
}

fn string_arg(args: &[String], name: &str) -> Option<String> {
    value_after(args, name).map(ToOwned::to_owned)
}

fn value_after<'a>(args: &'a [String], name: &str) -> Option<&'a str> {
    args.iter()
        .position(|arg| arg == name)
        .and_then(|index| args.get(index + 1))
        .map(String::as_str)
}

fn maintenance_label(kind: &MaintenanceKind) -> &'static str {
    match kind {
        MaintenanceKind::Advance { .. } => "advance",
        MaintenanceKind::Consolidate => "consolidate",
        MaintenanceKind::MergePrune => "merge_prune",
        MaintenanceKind::Checkpoint => "checkpoint",
        MaintenanceKind::Compact => "compact",
    }
}

fn increment_count(counts: &mut serde_json::Map<String, Value>, key: &str) {
    let current = counts.get(key).and_then(Value::as_u64).unwrap_or(0);
    counts.insert(key.to_owned(), json!(current.saturating_add(1)));
}

fn hex(bytes: &[u8]) -> String {
    let mut result = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        use std::fmt::Write as _;
        let _ = write!(result, "{byte:02x}");
    }
    result
}

fn require(condition: bool, message: &str) -> BenchResult<()> {
    if condition {
        Ok(())
    } else {
        Err(failure(message))
    }
}

fn failure(message: impl Into<String>) -> Box<dyn Error> {
    Box::new(io::Error::other(message.into()))
}
