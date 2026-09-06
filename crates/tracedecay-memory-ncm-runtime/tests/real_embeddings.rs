//! Integration coverage for the pinned native multilingual encoder.

#![cfg(feature = "real-encoder")]
#![allow(
    clippy::expect_used,
    clippy::panic,
    clippy::print_stderr,
    clippy::unwrap_used
)]

use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::Instant;

use tracedecay_memory_ncm_runtime::embedding::doubles::HashEncoder;
use tracedecay_memory_ncm_runtime::embedding::{
    KEYWORD_AFFECT_SIGNAL_SOURCE, MANIFEST_FILENAME, MAX_INPUT_BYTES, MODEL_NAME, MiniLmEncoder,
    PinnedEncoder, extract_keyword_affect, install, is_model_cached, offline_probe,
};
use tracedecay_memory_ncm_runtime::ports::{Deadline, EncoderError, StateRoot, TextEncoder};

const CACHE_REPOSITORY_DIR: &str = "models--Xenova--paraphrase-multilingual-MiniLM-L12-v2";
const REQUIRED_FILES: [&str; 5] = [
    "onnx/model.onnx",
    "tokenizer.json",
    "config.json",
    "special_tokens_map.json",
    "tokenizer_config.json",
];
const DEADLINE: Deadline = Deadline {
    remaining_ms: u64::MAX,
};

#[test]
fn keyword_affect_heuristic_matches_the_versioned_reference_signal() {
    assert_eq!(KEYWORD_AFFECT_SIGNAL_SOURCE, "keyword-heuristic.v1");
    assert_eq!(extract_keyword_affect("neutral text").0, [1.0; 4]);
    let affect = extract_keyword_affect("RADOST klid fear chyba pomoc").0;
    for (actual, expected) in affect.into_iter().zip([1.3, 1.05, 1.6, 1.25]) {
        assert!((actual - expected).abs() < 1e-6);
    }
    assert_eq!(
        extract_keyword_affect(
            "radost úspěch skvělé výborně super hurá joy success great excellent amazing win"
        )
        .0[0],
        2.0,
        "the reference clamps each channel to 2.0"
    );
}

#[test]
fn hash_encoder_is_rejected_by_real_model_identity_gate() {
    let encoder = HashEncoder::new();
    assert!(
        !encoder
            .identity()
            .model
            .starts_with("paraphrase-multilingual")
    );
    let embeddings = encoder
        .encode(&["test double"], DEADLINE)
        .expect("hash test double should encode");
    assert_eq!(embeddings.len(), 1);
    assert_eq!(embeddings[0].0.len(), 384);
}

#[test]
fn empty_root_is_missing_without_download_or_filesystem_mutation() {
    let directory = tempfile::tempdir().expect("create empty root");
    let root = StateRoot::new(directory.path()).expect("temporary root is absolute");
    let expected = PinnedEncoder::reference().expect("checked-in reference manifest");
    let before = directory_entries(directory.path());

    let result = MiniLmEncoder::open(&root, &expected);
    assert!(matches!(result, Err(EncoderError::ArtifactsMissing(_))));
    assert_eq!(directory_entries(directory.path()), before);
    assert!(
        !root.models_dir().exists(),
        "offline open must not create models"
    );
    assert!(!is_model_cached(&root));
    assert!(!offline_probe(&root));
}

#[test]
fn real_encoder_install_open_and_fixture_behavior() {
    let directory = tempfile::tempdir().expect("create model root");
    let root = StateRoot::new(directory.path()).expect("temporary root is absolute");
    let expected = PinnedEncoder::reference().expect("checked-in reference manifest");

    let install_started = Instant::now();
    let installed_identity = install(&root, DEADLINE).expect("real model download and install");
    let install_ms = install_started.elapsed().as_millis();
    assert_eq!(installed_identity.model, MODEL_NAME);
    assert!(
        installed_identity
            .model
            .starts_with("paraphrase-multilingual")
    );
    assert_eq!(installed_identity.max_length, 128);
    assert_eq!(
        installed_identity.artifact_sha256,
        expected.artifact_sha256().unwrap()
    );
    assert!(is_model_cached(&root));
    assert!(offline_probe(&root));

    let local_manifest = PinnedEncoder::from_path(root.models_dir().join(MANIFEST_FILENAME))
        .expect("install manifest should be readable");
    assert_eq!(local_manifest, expected);

    let rss_before = process_rss_kib();
    let cold_started = Instant::now();
    let encoder = MiniLmEncoder::open(&root, &expected).expect("open verified local model");
    let cold_load_ms = cold_started.elapsed().as_millis();
    let rss_after = process_rss_kib();
    assert_eq!(encoder.identity(), installed_identity);

    let fixtures = [
        "The quick brown fox jumps over the lazy dog.",
        "Příliš žluťoučký kůň úpěl ďábelské ódy",
        "fn compute_rbf_weights",
        "",
        "A small bird sings in the garden.",
        "Malý pták zpívá v zahradě.",
        "The spacecraft entered orbit around a distant planet.",
        "A friendly robot waved hello: 🤖🌍",
    ];
    let first = encoder
        .encode(&fixtures, DEADLINE)
        .expect("fixture inference should succeed");
    assert_eq!(first.len(), fixtures.len());
    for embedding in &first {
        assert_eq!(embedding.0.len(), 384);
        let norm = embedding
            .0
            .iter()
            .map(|value| value * value)
            .sum::<f32>()
            .sqrt();
        assert!((norm - 1.0).abs() < 1e-4, "embedding norm was {norm}");
    }

    let second = encoder
        .encode(&fixtures, DEADLINE)
        .expect("repeat fixture inference should succeed");
    let deterministic_diff = max_abs_diff(&first[0].0, &second[0].0);
    assert!(
        deterministic_diff < 1e-6,
        "repeat inference differed by {deterministic_diff}"
    );

    let translation_cosine = cosine(&first[4].0, &first[5].0);
    let unrelated_cosine = cosine(&first[4].0, &first[6].0);
    assert!(
        translation_cosine > unrelated_cosine,
        "translation cosine {translation_cosine} was not above unrelated cosine {unrelated_cosine}"
    );

    let long_text = (0..220)
        .map(|index| format!("token{index}"))
        .collect::<Vec<_>>()
        .join(" ");
    let truncated_text = encoder
        .truncate_to_model_limit(&long_text)
        .expect("tokenizer should produce a UTF-8 truncation prefix");
    assert!(truncated_text.len() < long_text.len());
    let long_embedding = encoder
        .encode(&[long_text.as_str()], DEADLINE)
        .expect("long input should be truncated by tokenizer");
    let truncated_embedding = encoder
        .encode(&[truncated_text.as_str()], DEADLINE)
        .expect("tokenizer-derived prefix should encode");
    let truncation_diff = max_abs_diff(&long_embedding[0].0, &truncated_embedding[0].0);
    assert!(
        truncation_diff < 1e-6,
        "tokenizer truncation differed from its first-128-token prefix by {truncation_diff}"
    );

    let warm_started = Instant::now();
    encoder
        .encode(&[fixtures[0]], DEADLINE)
        .expect("warm single encode should succeed");
    let warm_single_encode_ms = warm_started.elapsed().as_millis();

    let batch_inputs = (0..16)
        .map(|index| format!("Batch fixture {index}: compute_rbf_weights"))
        .collect::<Vec<_>>();
    let batch_refs = batch_inputs.iter().map(String::as_str).collect::<Vec<_>>();
    let batch_started = Instant::now();
    let batch_embeddings = encoder
        .encode(&batch_refs, DEADLINE)
        .expect("batch of sixteen should encode");
    let batch_of_16_ms = batch_started.elapsed().as_millis();
    assert_eq!(batch_embeddings.len(), 16);

    assert!(matches!(
        encoder.encode(&["expired"], Deadline { remaining_ms: 0 }),
        Err(EncoderError::Cancelled)
    ));
    let too_large = "x".repeat(MAX_INPUT_BYTES + 1);
    assert!(matches!(
        encoder.encode(&[too_large.as_str()], DEADLINE),
        Err(EncoderError::InputTooLarge)
    ));

    let corrupt_directory = tempfile::tempdir().expect("create corrupted copy root");
    let corrupt_root =
        StateRoot::new(corrupt_directory.path()).expect("temporary root is absolute");
    copy_cached_artifacts(&root, &corrupt_root);

    let corrupt_tokenizer_path = cached_file(&corrupt_root, "tokenizer.json");
    let mut corrupt_tokenizer = fs::read(&corrupt_tokenizer_path).expect("read copied tokenizer");
    let tokenizer_flip_at = corrupt_tokenizer.len() / 2;
    corrupt_tokenizer[tokenizer_flip_at] ^= 1;
    fs::write(&corrupt_tokenizer_path, &corrupt_tokenizer)
        .expect("write corrupted copied tokenizer");
    assert!(matches!(
        MiniLmEncoder::open(&corrupt_root, &expected),
        Err(EncoderError::ArtifactMismatch(_))
    ));
    fs::copy(
        cached_file(&root, "tokenizer.json"),
        &corrupt_tokenizer_path,
    )
    .expect("restore tokenizer before model corruption check");

    let corrupt_model_path = cached_file(&corrupt_root, "onnx/model.onnx");
    let mut corrupt_bytes = fs::read(&corrupt_model_path).expect("read copied model");
    let flip_at = corrupt_bytes.len() / 2;
    corrupt_bytes[flip_at] ^= 1;
    fs::write(&corrupt_model_path, corrupt_bytes).expect("write corrupted copied model");
    assert!(matches!(
        MiniLmEncoder::open(&corrupt_root, &expected),
        Err(EncoderError::ArtifactMismatch(_))
    ));

    compare_optional_oracle(&encoder);

    let rss_delta_kib = rss_after.map(|after| after as i64 - rss_before.unwrap_or(after) as i64);
    eprintln!(
        "embedding measurements: install_ms={install_ms} cold_load_ms={cold_load_ms} warm_single_encode_ms={warm_single_encode_ms} batch_of_16_ms={batch_of_16_ms} rss_delta_kib={rss_delta_kib:?} deterministic_max_abs_diff={deterministic_diff} truncation_max_abs_diff={truncation_diff}"
    );
}

fn directory_entries(path: &Path) -> Vec<String> {
    fs::read_dir(path)
        .expect("read directory")
        .map(|entry| {
            entry
                .expect("read directory entry")
                .file_name()
                .to_string_lossy()
                .into_owned()
        })
        .collect()
}

fn cached_file(root: &StateRoot, relative: &str) -> PathBuf {
    let repository = root.models_dir().join(CACHE_REPOSITORY_DIR);
    let revision = fs::read_to_string(repository.join("refs/main")).expect("read cache revision");
    repository
        .join("snapshots")
        .join(revision.trim())
        .join(relative)
}

fn copy_cached_artifacts(source: &StateRoot, destination: &StateRoot) {
    let source_repository = source.models_dir().join(CACHE_REPOSITORY_DIR);
    let destination_repository = destination.models_dir().join(CACHE_REPOSITORY_DIR);
    let revision =
        fs::read_to_string(source_repository.join("refs/main")).expect("read source revision");
    let revision = revision.trim();
    let source_snapshot = source_repository.join("snapshots").join(revision);
    let destination_snapshot = destination_repository.join("snapshots").join(revision);
    fs::create_dir_all(&destination_snapshot).expect("create destination snapshot");
    fs::create_dir_all(destination_repository.join("refs")).expect("create destination refs");
    fs::write(destination_repository.join("refs/main"), revision).expect("write destination ref");
    for relative in REQUIRED_FILES {
        let source_path = source_snapshot.join(relative);
        let destination_path = destination_snapshot.join(relative);
        if let Some(parent) = destination_path.parent() {
            fs::create_dir_all(parent).expect("create destination artifact directory");
        }
        fs::copy(source_path, destination_path).expect("copy cached artifact");
    }
    fs::create_dir_all(destination.models_dir()).expect("create destination model directory");
    fs::copy(
        source.models_dir().join(MANIFEST_FILENAME),
        destination.models_dir().join(MANIFEST_FILENAME),
    )
    .expect("copy encoder manifest");
}

fn max_abs_diff(left: &[f32], right: &[f32]) -> f32 {
    assert_eq!(left.len(), right.len());
    left.iter()
        .zip(right)
        .map(|(a, b)| (a - b).abs())
        .fold(0.0_f32, f32::max)
}

fn cosine(left: &[f32], right: &[f32]) -> f32 {
    assert_eq!(left.len(), right.len());
    left.iter().zip(right).map(|(a, b)| a * b).sum()
}

fn process_rss_kib() -> Option<u64> {
    let pid = std::process::id().to_string();
    let output = Command::new("ps")
        .args(["-o", "rss=", "-p", pid.as_str()])
        .output()
        .ok()?;
    if !output.status.success() {
        return None;
    }
    String::from_utf8(output.stdout)
        .ok()?
        .trim()
        .parse::<u64>()
        .ok()
}

fn compare_optional_oracle(encoder: &MiniLmEncoder) {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../product/ncm/reference/oracle/real_embeddings.json");
    if !path.is_file() {
        eprintln!("embedding oracle: no fixture at {}", path.display());
        return;
    }
    let bytes = fs::read(&path).expect("read embedding oracle");
    let root: serde_json::Value = serde_json::from_slice(&bytes).expect("parse embedding oracle");
    let expected = oracle_embeddings(&root).expect("embedding oracle must expose named vectors");
    let texts = expected.keys().map(String::as_str).collect::<Vec<_>>();
    let actual = encoder
        .encode(&texts, DEADLINE)
        .expect("oracle inputs should encode");
    assert_eq!(actual.len(), expected.len());

    let mut measured_max = 0.0_f32;
    for ((text, reference), embedding) in expected.iter().zip(&actual) {
        assert_eq!(
            reference.len(),
            embedding.0.len(),
            "oracle dimension for input of {} UTF-8 bytes",
            text.len()
        );
        let diff = max_abs_diff(reference, &embedding.0);
        measured_max = measured_max.max(diff);
        assert!(
            diff < 1e-3,
            "oracle diff for input of {} UTF-8 bytes was {diff}",
            text.len()
        );
    }
    eprintln!(
        "embedding oracle: compared={} max_abs_diff={measured_max}",
        expected.len()
    );
}

fn oracle_embeddings(
    root: &serde_json::Value,
) -> Option<std::collections::BTreeMap<String, Vec<f32>>> {
    if let Some(cases) = root.get("cases").and_then(serde_json::Value::as_array) {
        let mut vectors = std::collections::BTreeMap::new();
        for case in cases {
            let text = case.get("text")?.as_str()?;
            let vector = case.get("embedding")?.as_array()?;
            let values = vector
                .iter()
                .map(serde_json::Value::as_f64)
                .collect::<Option<Vec<_>>>()?
                .into_iter()
                .map(|value| value as f32)
                .collect::<Vec<_>>();
            vectors.insert(text.to_owned(), values);
        }
        if !vectors.is_empty() {
            return Some(vectors);
        }
    }

    let candidates = [root.get("embeddings"), root.get("fixtures"), Some(root)];
    for candidate in candidates.into_iter().flatten() {
        if let Some(object) = candidate.as_object() {
            let mut vectors = std::collections::BTreeMap::new();
            for (name, value) in object {
                if let Some(vector) = value.as_array() {
                    let values = vector
                        .iter()
                        .map(serde_json::Value::as_f64)
                        .collect::<Option<Vec<_>>>()?
                        .into_iter()
                        .map(|value| value as f32)
                        .collect::<Vec<_>>();
                    vectors.insert(name.clone(), values);
                } else if let Some(vector) =
                    value.get("embedding").and_then(serde_json::Value::as_array)
                {
                    let values = vector
                        .iter()
                        .map(serde_json::Value::as_f64)
                        .collect::<Option<Vec<_>>>()?
                        .into_iter()
                        .map(|value| value as f32)
                        .collect::<Vec<_>>();
                    vectors.insert(name.clone(), values);
                }
            }
            if !vectors.is_empty() {
                return Some(vectors);
            }
        }
    }
    None
}
