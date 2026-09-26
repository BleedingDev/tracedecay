#![doc = "Model lifecycle recovery against the shipped acquisition manifest contract."]
#![allow(clippy::expect_used, clippy::panic, clippy::unwrap_used)]

use serde_json::json;
use sha2::{Digest, Sha256};
use std::fs;
use std::path::Path;
use tracedecay_memory_ncm_runtime::embedding::model_lifecycle::{
    BACKUP_PREFIX, JOURNAL_FILENAME, STAGING_PREFIX, recover,
};
use tracedecay_memory_ncm_runtime::embedding::{
    MODEL_ACQUISITION_RECEIPT_PATH, ModelAcquisitionManifest,
};
use tracedecay_memory_ncm_runtime::ports::StateRoot;

fn tree_digest(root: &Path) -> String {
    let mut hasher = Sha256::new();
    digest_directory(root, Path::new(""), &mut hasher);
    hasher
        .finalize()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

fn digest_directory(root: &Path, relative: &Path, hasher: &mut Sha256) {
    let mut entries = fs::read_dir(root)
        .expect("read model tree")
        .map(|entry| entry.expect("read model tree entry"))
        .collect::<Vec<_>>();
    entries.sort_by_key(|entry| entry.file_name());
    for entry in entries {
        let name = entry.file_name();
        let child_relative = relative.join(&name);
        let child_path = root.join(&name);
        let file_type = entry.file_type().expect("inspect model tree entry");
        hasher.update(child_relative.to_string_lossy().as_bytes());
        hasher.update([0]);
        if file_type.is_dir() {
            hasher.update(b"d\0");
            digest_directory(&child_path, &child_relative, hasher);
        } else {
            assert!(
                file_type.is_file(),
                "model tree entry must be a regular file"
            );
            hasher.update(b"f\0");
            hasher.update(fs::read(child_path).expect("read model tree file"));
        }
    }
}

#[test]
fn recovers_interrupted_staged_transaction_using_release_manifest_shape() {
    let manifest = ModelAcquisitionManifest::reference().expect("trusted release manifest");
    assert_eq!(
        manifest
            .canonical_sha256()
            .expect("canonical release manifest digest"),
        "516be9008292934546811020cd6e4ad6d1f256e45482d406d8e69b9af6e5db69"
    );
    assert_eq!(manifest.transaction.journal, JOURNAL_FILENAME);
    assert_eq!(manifest.transaction.staging_prefix, STAGING_PREFIX);
    assert_eq!(manifest.transaction.backup_prefix, BACKUP_PREFIX);
    assert_eq!(
        manifest.receipt.relative_path,
        MODEL_ACQUISITION_RECEIPT_PATH
    );
    for file in &manifest.files {
        assert_eq!(
            file.url,
            format!("{}{path}", manifest.base_url, path = file.path)
        );
        assert!(file.url.contains(&manifest.revision));
        assert!(!file.url.contains("/resolve/main/"));
    }
    for field in [
        "schema_version",
        "operation_id",
        "operation",
        "outcome",
        "model",
        "repository",
        "revision",
        "manifest_sha256",
        "revision_provenance_sha256",
        "files",
        "created_at_unix",
    ] {
        assert!(
            manifest
                .receipt
                .required_fields
                .iter()
                .any(|item| item == field)
        );
    }
    // Model acquisition is target-independent: one descriptor serves every
    // worker platform, so receipts must not be bound to a target.
    assert!(
        !manifest
            .receipt
            .required_fields
            .iter()
            .any(|item| item == "target" || item == "release_name")
    );

    let temporary = tempfile::tempdir().expect("create lifecycle root");
    let root = StateRoot::new(temporary.path()).expect("temporary root is absolute");
    let models = root.models_dir();
    fs::create_dir_all(models.join("nested")).expect("create original model tree");
    fs::write(models.join("nested/model.bin"), b"original").expect("write original model");
    let before_digest = tree_digest(&models);

    let operation_id = format!("{}-{}-update", "1234abcd", "b".repeat(16));
    let staging_name = format!("{STAGING_PREFIX}{operation_id}");
    let backup_name = format!("{BACKUP_PREFIX}{operation_id}");
    fs::create_dir(temporary.path().join(&staging_name)).expect("create interrupted staging");
    fs::rename(&models, temporary.path().join(&backup_name)).expect("move original to backup");
    let journal = json!({
        "schema_version": 1,
        "operation_id": operation_id,
        "operation": "update",
        "phase": "staged",
        "revision": manifest.revision,
        "staging_name": staging_name,
        "backup_name": backup_name,
        "before_digest": before_digest,
        "after_digest": "c".repeat(64),
    });
    let journal_bytes = serde_json::to_vec_pretty(&journal).expect("encode interrupted journal");
    fs::write(
        temporary.path().join(JOURNAL_FILENAME),
        [journal_bytes.as_slice(), b"\n"].concat(),
    )
    .expect("write interrupted journal");

    recover(&root).expect("recover interrupted staged transaction");

    assert_eq!(
        fs::read(models.join("nested/model.bin")).expect("read restored model"),
        b"original"
    );
    assert!(!temporary.path().join(&staging_name).exists());
    assert!(!temporary.path().join(&backup_name).exists());
    assert!(!temporary.path().join(JOURNAL_FILENAME).exists());
}
