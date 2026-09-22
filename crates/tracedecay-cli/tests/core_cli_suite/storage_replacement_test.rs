//! End-to-end journeys for the shipped V1-to-V2 replacement worker.
//!
//! These tests deliberately seed the same registered profile and project
//! authorities used by the daemon fixtures. The replacement therefore has to
//! preserve real session/LCM rows, opaque provider state, external LCM
//! payloads, and checkout workflow/Git bytes before a fresh runtime can reopen
//! the published profile.

use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::{Command, Output};

use rusqlite::Connection;
use tempfile::TempDir;
use tracedecay::test_support::host_admission::HostAdmissionTestRuntimeV1;
use tracedecay_domain::ProjectId;
use tracedecay_runtime_core::storage::{
    STORE_MANIFEST_FILENAME, default_profile_project_id, read_store_manifest,
};
use tracedecay_sessions::admission::HostAdmissionScope;

use crate::common::{
    MessageRecordBuilder, canonical_existing_path, create_runtime, global_session,
    register_process_product_runtime, register_process_runtime_ports,
};

struct ReplacementFixture {
    _temp: TempDir,
    home: PathBuf,
    profile: PathBuf,
    backup_parent: PathBuf,
    project: PathBuf,
    project_id: String,
    profile_before: BTreeMap<String, Vec<u8>>,
    project_before: BTreeMap<String, Vec<u8>>,
}

fn replacement_command(fixture: &ReplacementFixture, backup_id: &str) -> Command {
    let mut command = Command::new(env!("CARGO_BIN_EXE_tracedecay"));
    command
        .current_dir(&fixture.project)
        .env("HOME", &fixture.home)
        .env("USERPROFILE", &fixture.home)
        .env("XDG_CONFIG_HOME", fixture.home.join(".config"))
        .env("TRACEDECAY_DATA_DIR", &fixture.profile)
        .env("TRACEDECAY_GLOBAL_DB", fixture.profile.join("global.db"))
        .args([
            "storage",
            "replace-v1",
            "--profile-root",
            fixture.profile.to_str().expect("profile path is UTF-8"),
            "--backup-to",
            fixture
                .backup_parent
                .to_str()
                .expect("backup path is UTF-8"),
            "--backup-id",
            backup_id,
        ])
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped());
    command
}

fn snapshot_tree(root: &Path) -> BTreeMap<String, Vec<u8>> {
    let mut snapshot = BTreeMap::new();
    snapshot_tree_into(root, Path::new(""), &mut snapshot);
    snapshot
}

fn snapshot_tree_into(root: &Path, relative: &Path, snapshot: &mut BTreeMap<String, Vec<u8>>) {
    let mut entries = fs::read_dir(root)
        .unwrap_or_else(|error| panic!("read snapshot root {}: {error}", root.display()))
        .map(|entry| entry.expect("read snapshot entry"))
        .collect::<Vec<_>>();
    entries.sort_by_key(fs::DirEntry::file_name);
    for entry in entries {
        let path = entry.path();
        let child = relative.join(entry.file_name());
        let key = child.to_string_lossy().replace('\\', "/");
        let metadata = fs::symlink_metadata(&path).expect("inspect snapshot entry");
        assert!(
            !metadata.file_type().is_symlink(),
            "snapshot does not admit symlinks: {}",
            path.display()
        );
        if metadata.is_dir() {
            snapshot.insert(format!("{key}/"), Vec::new());
            snapshot_tree_into(&path, &child, snapshot);
        } else {
            snapshot.insert(key, fs::read(path).expect("read snapshot file"));
        }
    }
}

fn profile_snapshot(root: &Path) -> BTreeMap<String, Vec<u8>> {
    let mut snapshot = snapshot_tree(root);
    for reserved in [
        "lifecycle.lock",
        ".tracedecay-v1-replacement.json",
        ".tracedecay-profile-rehearsal.json",
    ] {
        snapshot.remove(reserved);
    }
    snapshot
}

fn git(project: &Path, args: &[&str]) {
    let output = Command::new("git")
        .args(["-c", "core.hooksPath=.git/no-hooks"])
        .args(args)
        .current_dir(project)
        .output()
        .unwrap_or_else(|error| panic!("run git {args:?}: {error}"));
    assert!(
        output.status.success(),
        "git {args:?} failed\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}

fn make_git_project(project: &Path) {
    fs::create_dir_all(project).expect("create project root");
    git(project, &["init", "-b", "main"]);
    fs::create_dir(project.join("src")).expect("create project source directory");
    fs::write(
        project.join("src/provider.rs"),
        b"pub struct ProviderState;\n",
    )
    .expect("write project source");
    git(project, &["add", "."]);
    git(
        project,
        &[
            "-c",
            "user.name=TraceDecay Test",
            "-c",
            "user.email=tracedecay-test@example.com",
            "commit",
            "-m",
            "replacement fixture",
        ],
    );
    // This uncommitted workflow document proves that the inventory includes
    // working-tree state as well as the committed Git object database.
    fs::write(
        project.join("workflow.json"),
        br#"{"state":"blocked","task_id":"replacement-fixture"}"#,
    )
    .expect("write workflow state");
}

fn fixture_profile() -> ReplacementFixture {
    let temp = TempDir::new().expect("create replacement fixture");
    let home = canonical_existing_path(temp.path().join("home"));
    fs::create_dir_all(&home).expect("create fixture home");
    let profile = canonical_existing_path(home.join(".tracedecay"));
    let project = canonical_existing_path(temp.path().join("project"));
    let backup_parent = canonical_existing_path(temp.path().join("external-backups"));
    make_git_project(&project);
    let project_id = default_profile_project_id(&project);

    register_process_product_runtime();
    register_process_runtime_ports();
    create_runtime().block_on(async {
        let runtime = HostAdmissionTestRuntimeV1::project(
            &profile,
            &project,
            ProjectId::new(project_id.clone()).expect("fixture project id"),
        )
        .await
        .expect("open registered replacement fixture");

        let profile_session = global_session("codex", "replacement-profile-session", &project_id);
        assert!(
            runtime
                .upsert_session_for_test(HostAdmissionScope::Profile, &profile_session)
                .await
                .expect("insert profile session")
        );
        let profile_message = MessageRecordBuilder::new(
            "codex",
            "replacement-profile-message",
            "replacement-profile-session",
            "assistant",
            1,
            "profile authority survives replacement",
            "message",
        )
        .build();
        assert!(
            runtime
                .upsert_session_message_for_test(HostAdmissionScope::Profile, &profile_message)
                .await
                .expect("insert profile session message")
        );

        let project_session = global_session("claude", "replacement-project-session", &project_id);
        assert!(
            runtime
                .upsert_session_for_test(HostAdmissionScope::Project, &project_session)
                .await
                .expect("insert project session")
        );
        let project_message = MessageRecordBuilder::new(
            "claude",
            "replacement-project-message",
            "replacement-project-session",
            "assistant",
            1,
            "project LCM authority survives replacement",
            "message",
        )
        .with_source(Some("workflow.json"), Some(1))
        .build();
        assert!(
            runtime
                .upsert_session_message_for_test(HostAdmissionScope::Project, &project_message)
                .await
                .expect("insert project session message")
        );

        // Force creation of the distinct profile-memory authority before the
        // fixture is handed to the child CLI.
        runtime
            .session_registry_for_test()
            .profile_memory()
            .await
            .expect("open profile memory authority");
        runtime
            .checkpoint_session_database_for_test(HostAdmissionScope::Profile)
            .await
            .expect("checkpoint profile session database");
        runtime
            .checkpoint_session_database_for_test(HostAdmissionScope::Project)
            .await
            .expect("checkpoint project session database");
        runtime.checkpoint_profile_database_for_test().await;
        drop(runtime);
    });

    // Provider settings/credential references and external LCM payloads are
    // opaque V1 bytes. The worker must copy them without interpreting them.
    fs::write(
        profile.join("enrollment.json"),
        br#"{"schema_version":1,"provider":"fixture","credential_ref":"credential.fixture"}"#,
    )
    .expect("write enrollment authority");
    fs::write(
        profile.join("config.toml"),
        b"upload_enabled = false\n[provider]\nname = \"fixture\"\ncredential_ref = \"credential.fixture\"\n",
    )
    .expect("write provider configuration");
    fs::create_dir_all(profile.join("migration-inventory")).expect("create migration inventory");
    fs::write(
        profile.join("migration-inventory/v1-fixture.json"),
        br#"{"release":"v1","authorities":["provider","session","lcm"]}"#,
    )
    .expect("write migration inventory");
    fs::create_dir_all(profile.join("lcm-payloads")).expect("create external LCM payload root");
    fs::write(
        profile.join("lcm-payloads/payload-fixture.json"),
        br#"{"payload":"opaque-lcm-bytes","credential_ref":"credential.fixture"}"#,
    )
    .expect("write external LCM payload");
    fs::write(
        profile.join("provider-opaque-state.bin"),
        b"provider bytes survive exactly\0\xff",
    )
    .expect("write opaque provider bytes");

    let profile_before = profile_snapshot(&profile);
    let project_before = snapshot_tree(&project);
    ReplacementFixture {
        _temp: temp,
        home,
        profile,
        backup_parent,
        project,
        project_id,
        profile_before,
        project_before,
    }
}

fn successful_json(output: Output) -> serde_json::Value {
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        output.status.success(),
        "replacement should succeed\nstdout:\n{stdout}\nstderr:\n{stderr}"
    );
    serde_json::from_str(&stdout).expect("replacement should emit one JSON receipt")
}

#[test]
fn replacement_dry_run_is_truthful_and_zero_write() {
    let fixture = fixture_profile();
    let before = snapshot_tree(fixture._temp.path());
    let output = replacement_command(&fixture, "dry-run-backup")
        .args(["--dry-run", "--json"])
        .output()
        .expect("run replacement dry-run");
    assert!(
        output.status.success(),
        "dry-run failed\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let plan: serde_json::Value = serde_json::from_slice(&output.stdout).expect("dry-run JSON");
    assert_eq!(plan["protocol"], "tracedecay-v1-to-v2");
    assert_eq!(plan["requires_confirmation"], true);
    assert_eq!(plan["makes_changes"], false);
    assert_eq!(plan["authorities"].as_array().map(Vec::len), Some(8));
    assert_eq!(plan["first_party_worker_available"], true);
    assert_eq!(plan["worker_ready"], true);
    assert_eq!(plan["first_party_preflight_ready"], true);
    assert_eq!(plan["backup_destination_available"], true);
    assert_eq!(plan["backup_parent_state"], "will be created at apply");
    assert_eq!(plan["backup_feasible"], true);
    assert_eq!(plan["target_namespace_feasible"], true);
    assert!(
        plan["apply_feasibility"]
            .as_str()
            .expect("apply feasibility")
            .contains("canonical backup")
    );
    assert_eq!(
        snapshot_tree(fixture._temp.path()),
        before,
        "dry-run must not write"
    );
}

#[test]
fn replacement_requires_confirmation_without_inspecting_or_writing() {
    let temp = TempDir::new().expect("create confirmation fixture");
    let profile = temp.path().join("profile");
    let backup_parent = temp.path().join("external-backups");
    fs::create_dir(&profile).expect("create profile");
    fs::write(profile.join("sentinel"), b"unchanged").expect("write sentinel");
    let before = snapshot_tree(temp.path());
    let output = Command::new(env!("CARGO_BIN_EXE_tracedecay"))
        .args([
            "storage",
            "replace-v1",
            "--profile-root",
            profile.to_str().expect("profile path is UTF-8"),
            "--backup-to",
            backup_parent.to_str().expect("backup path is UTF-8"),
            "--backup-id",
            "confirmation-backup",
        ])
        .output()
        .expect("run unconfirmed replacement");
    assert!(!output.status.success());
    assert!(String::from_utf8_lossy(&output.stderr).contains("--yes"));
    assert_eq!(snapshot_tree(temp.path()), before);
}

#[test]
fn replacement_preserves_all_authorities_and_reopens_after_cutover() {
    let fixture = fixture_profile();
    let output = replacement_command(&fixture, "backup-v1-test")
        .args(["--yes", "--json", "--timeout-seconds", "120"])
        .output()
        .expect("run first-party replacement");
    let receipt = successful_json(output);
    assert_eq!(receipt["protocol"], "tracedecay-v1-to-v2");
    assert_eq!(receipt["authorities"].as_array().map(Vec::len), Some(8));
    assert_eq!(
        receipt["rollback"]
            .as_str()
            .map(|text| text.contains("backup")),
        Some(true)
    );
    assert!(
        receipt["worker"]
            .as_str()
            .is_some_and(|path| path.contains("tracedecay"))
    );
    assert_eq!(receipt["worker_sha256"].as_str().map(str::len), Some(64));

    let backup_root = fixture.backup_parent.join("backup-v1-test");
    assert!(backup_root.is_dir(), "external backup was published");
    tracedecay_maintenance::profile_backup::load_and_verify_backup(&backup_root)
        .expect("published backup remains independently verifiable");
    let preserved = PathBuf::from(
        receipt["preserved_v1_root"]
            .as_str()
            .expect("preserved V1 root"),
    );
    assert_eq!(profile_snapshot(&preserved), fixture.profile_before);
    assert_eq!(snapshot_tree(&fixture.project), fixture.project_before);

    for authority in [
        "global.db",
        "user-sessions.db",
        "user-memory.db",
        "projects",
        "enrollment.json",
        "config.toml",
        "migration-inventory",
        "profile-identity.json",
    ] {
        assert!(
            fs::symlink_metadata(fixture.profile.join(authority)).is_ok(),
            "published profile is missing {authority}"
        );
    }
    assert_eq!(
        fs::read(fixture.profile.join("config.toml")).expect("read published config"),
        fs::read(preserved.join("config.toml")).expect("read preserved config")
    );
    assert_eq!(
        fs::read(fixture.profile.join("lcm-payloads/payload-fixture.json"))
            .expect("read published LCM payload"),
        b"{\"payload\":\"opaque-lcm-bytes\",\"credential_ref\":\"credential.fixture\"}"
    );
    assert_eq!(
        fs::read(fixture.profile.join("provider-opaque-state.bin"))
            .expect("read published provider bytes"),
        b"provider bytes survive exactly\0\xff"
    );

    // The shipped worker leaves an independently recomputable semantic
    // manifest in the published tree. Keep this assertion tied to the real
    // fixture so a receipt containing only authority names cannot masquerade
    // as a lossless migration.
    let semantic_manifest: serde_json::Value = serde_json::from_slice(
        &fs::read(fixture.profile.join(".tracedecay-v1-to-v2-manifest.json"))
            .expect("read published semantic manifest"),
    )
    .expect("decode published semantic manifest");
    assert_eq!(semantic_manifest["provider"], "native");
    let manifest_authorities = semantic_manifest["authorities"]
        .as_array()
        .expect("semantic manifest authorities");
    assert_eq!(manifest_authorities.len(), 8);
    for authority in manifest_authorities {
        assert!(
            authority["source_digest"]
                .as_str()
                .is_some_and(|digest| digest.len() == 64),
            "semantic authority must carry an independently bound source digest"
        );
        assert!(
            authority["target_digest"]
                .as_str()
                .is_some_and(|digest| digest.len() == 64),
            "semantic authority must carry an independently bound target digest"
        );
        assert!(
            authority["dispositions"].as_array().is_some(),
            "semantic authority must carry typed dispositions"
        );
        assert!(
            authority["source_row_digest"].as_str().is_some(),
            "semantic authority must carry an aggregate source row digest"
        );
        assert!(
            authority["target_row_digest"].as_str().is_some(),
            "semantic authority must carry an aggregate target row digest"
        );
        assert!(
            authority["disposition"].as_str().is_some(),
            "semantic authority must carry an aggregate disposition"
        );
    }

    let profile_sessions = Connection::open(fixture.profile.join("user-sessions.db"))
        .expect("open published profile sessions");
    let profile_message_count: i64 = profile_sessions
        .query_row(
            "SELECT COUNT(*) FROM session_messages WHERE message_id = 'replacement-profile-message'",
            [],
            |row| row.get(0),
        )
        .expect("query published profile message");
    assert_eq!(profile_message_count, 1);
    drop(profile_sessions);

    let store_root = fixture.profile.join("projects").join(&fixture.project_id);
    let manifest = read_store_manifest(&store_root.join(STORE_MANIFEST_FILENAME))
        .expect("read published project store manifest");
    assert_eq!(manifest.project_root, fixture.project);
    assert_eq!(manifest.data_root, store_root);
    let project_sessions = Connection::open(store_root.join(&manifest.sessions_db_relpath))
        .expect("open published project sessions");
    let project_message_count: i64 = project_sessions
        .query_row(
            "SELECT COUNT(*) FROM session_messages WHERE message_id = 'replacement-project-message'",
            [],
            |row| row.get(0),
        )
        .expect("query published project message");
    assert_eq!(project_message_count, 1);
    let lcm_message_count: i64 = project_sessions
        .query_row("SELECT COUNT(*) FROM lcm_raw_messages", [], |row| {
            row.get(0)
        })
        .expect("query published LCM rows");
    assert!(lcm_message_count >= 1);
    drop(project_sessions);

    // Reopen through the registered composition root after the child CLI has
    // exited. This catches stale paths, missing profile identity, and lost
    // session authorities at the real daemon admission boundary.
    register_process_product_runtime();
    register_process_runtime_ports();
    create_runtime().block_on(async {
        let runtime = HostAdmissionTestRuntimeV1::project(
            &fixture.profile,
            &fixture.project,
            ProjectId::new(fixture.project_id.clone()).expect("reopen project id"),
        )
        .await
        .expect("reopen published profile through host admission");
        assert!(
            runtime
                .session_for_test(
                    HostAdmissionScope::Profile,
                    "codex",
                    "replacement-profile-session",
                )
                .await
                .expect("read reopened profile session")
                .is_some()
        );
        assert_eq!(
            runtime
                .project_session_message_count_for_test()
                .await
                .expect("read reopened project messages"),
            1
        );
        drop(runtime);
    });
    assert!(
        !fixture
            .profile
            .join(".tracedecay-v1-replacement.json")
            .exists()
    );
}

#[test]
fn replacement_failure_keeps_source_and_recovery_cleans_transaction_before_retry() {
    let fixture = fixture_profile();
    let graph = fixture
        .profile
        .join("projects")
        .join(&fixture.project_id)
        .join("tracedecay.db");
    let graph_bytes = fs::read(&graph).expect("read graph before injected failure");
    fs::remove_file(&graph).expect("remove graph for failure fixture");
    drop(Connection::open(&graph).expect("create malformed graph database"));

    let failed = replacement_command(&fixture, "backup-failure-test")
        .args(["--yes", "--timeout-seconds", "120"])
        .output()
        .expect("run failing replacement");
    assert!(!failed.status.success(), "malformed graph must fail closed");
    assert_eq!(profile_snapshot(&fixture.profile), fixture.profile_before);
    assert_eq!(snapshot_tree(&fixture.project), fixture.project_before);
    assert!(fixture.backup_parent.join("backup-failure-test").is_dir());
    assert!(
        fixture
            .profile
            .join(".tracedecay-v1-replacement.json")
            .is_file()
    );

    // A restart sees the Prepared journal, removes only its owned staging
    // paths, and can then start a new operation once the source is repaired.
    fs::write(&graph, graph_bytes).expect("restore graph after failed attempt");
    let retry = replacement_command(&fixture, "backup-retry-test")
        .args(["--yes", "--json", "--timeout-seconds", "120"])
        .output()
        .expect("retry replacement after recovery");
    let receipt = successful_json(retry);
    assert_eq!(receipt["protocol"], "tracedecay-v1-to-v2");
    assert!(
        !fixture
            .profile
            .join(".tracedecay-v1-replacement.json")
            .exists()
    );
    assert!(fixture.backup_parent.join("backup-failure-test").is_dir());
}
