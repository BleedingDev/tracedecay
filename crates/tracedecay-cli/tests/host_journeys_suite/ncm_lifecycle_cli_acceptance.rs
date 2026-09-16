//! NCM lifecycle journeys through the shipped CLI.
//!
//! The fixture gives every subprocess its own HOME, profile root, global
//! database, and registered project. The daemon is therefore the real scope
//! authority while no command can read or mutate the developer's profile.
//!
//! The always-on cases cover confirmation, exact project targeting, pending
//! journal refusal, digest-guarded recovery, and configuration revision drift.
//! The worker artifact journey is arm64 macOS-only because the product's
//! verifier intentionally admits only the pinned production target and is
//! ignored unless the real release worker is supplied through
//! `TRACEDECAY_NCM_WORKER`.

use std::fs;
use std::io::Read;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Output, Stdio};
use std::time::{Duration, Instant};

use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use tempfile::TempDir;
use tracedecay_daemon_identity::authority::DaemonAuthorityRecord;
use tracedecay_daemon_protocol::BrokerStream;

const NCM_SETTING: &str = "memory.provider_ncm_observer.v1";
const WORKER_NAME: &str = "tracedecay-ncm-worker";

struct NcmJourney {
    home: TempDir,
    profile: PathBuf,
    project: PathBuf,
    foreign_project: PathBuf,
    bin_dir: PathBuf,
    daemon: Option<Child>,
}

impl NcmJourney {
    fn new() -> Self {
        let home = TempDir::new().expect("isolated NCM HOME");
        let root = home.path().to_path_buf();
        let profile = root.join("tracedecay-profile");
        let project = root.join("project");
        let foreign_project = root.join("foreign-project");
        let bin_dir = root.join("bin");
        fs::create_dir_all(&profile).expect("isolated profile root");
        fs::create_dir_all(&bin_dir).expect("isolated binary directory");
        install_binary_shim(&bin_dir);
        initialize_project(&project, "ncm-cli-journey-fixture");
        initialize_project(&foreign_project, "ncm-cli-foreign-fixture");
        Self {
            home,
            profile,
            project,
            foreign_project,
            bin_dir,
            daemon: None,
        }
    }

    fn cli(&self, args: &[String]) -> Command {
        self.cli_at(&self.project, args)
    }

    fn cli_at(&self, project: &Path, args: &[String]) -> Command {
        let inherited_path = std::env::var_os("PATH").unwrap_or_default();
        let path = std::env::join_paths(
            std::iter::once(self.bin_dir.clone()).chain(std::env::split_paths(&inherited_path)),
        )
        .expect("isolated PATH");
        let mut command = Command::new(env!("CARGO_BIN_EXE_tracedecay"));
        command
            .args(args)
            .current_dir(project)
            .env("HOME", self.home.path())
            .env("USERPROFILE", self.home.path())
            .env("XDG_CONFIG_HOME", self.home.path().join(".config"))
            .env("TRACEDECAY_DATA_DIR", &self.profile)
            .env("TRACEDECAY_GLOBAL_DB", self.profile.join("global.db"))
            .env("TRACEDECAY_TEST_ALLOW_INCOMPLETE_HOLDER_SCAN", "1")
            .env("PATH", path)
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        command
    }

    fn start_daemon(&mut self) {
        assert!(self.daemon.is_none(), "NCM fixture daemon already running");
        let log = fs::File::create(self.home.path().join("daemon.stderr.log")).expect("daemon log");
        let mut command = self.cli(&strings(&["daemon", "run"]));
        command.stdout(Stdio::null()).stderr(Stdio::from(log));
        let mut daemon = command.spawn().expect("NCM fixture daemon starts");
        wait_for_authority(&mut daemon, &daemon_authority_path(&self.profile));
        self.daemon = Some(daemon);
    }

    fn stop_daemon(&mut self) {
        if let Some(mut daemon) = self.daemon.take() {
            let _ = daemon.kill();
            let _ = daemon.wait();
        }
    }

    fn init_project(&self) {
        let deadline = Instant::now() + Duration::from_secs(180);
        loop {
            let output = self
                .cli(&strings(&["init"]))
                .output()
                .expect("tracedecay init runs");
            if output.status.success() {
                return;
            }
            let stderr = String::from_utf8_lossy(&output.stderr);
            assert!(
                stderr.contains("code_index_scheduler_unavailable"),
                "init failed outside scheduler warming window\nstdout:\n{}\nstderr:\n{}",
                String::from_utf8_lossy(&output.stdout),
                stderr
            );
            assert!(Instant::now() < deadline, "init never became available");
            std::thread::sleep(Duration::from_millis(50));
        }
    }

    fn project_context(&self) -> Value {
        let selector = self.project.to_string_lossy().into_owned();
        let mut args = strings(&["projects", "context"]);
        args.push(selector);
        args.push("--json".to_owned());
        let stdout = run_ok(&mut self.cli(&args), "projects context");
        serde_json::from_slice(&stdout).expect("projects context JSON")
    }

    fn project_id(&self) -> String {
        self.project_context()["project"]["project_id"]
            .as_str()
            .expect("registered project id")
            .to_owned()
    }

    fn profile_id(&self) -> String {
        self.project_context()["profile_id"]
            .as_str()
            .expect("registered profile id")
            .to_owned()
    }

    fn ncm_output(&self, yes: bool, action: &str, options: &[String]) -> Output {
        let mut args = Vec::with_capacity(options.len() + 3);
        if yes {
            args.push("--yes".to_owned());
        }
        args.push("ncm".to_owned());
        args.push(action.to_owned());
        args.extend(options.iter().cloned());
        self.cli(&args).output().expect("NCM command runs")
    }

    fn ncm_json(&self, yes: bool, action: &str, options: &[String]) -> Value {
        let output = self.ncm_output(yes, action, options);
        assert!(
            output.status.success(),
            "ncm {action} failed with {}\nstdout:\n{}\nstderr:\n{}",
            output.status,
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        serde_json::from_slice(&output.stdout).unwrap_or_else(|error| {
            panic!(
                "ncm {action} did not return JSON: {error}\nstdout:\n{}\nstderr:\n{}",
                String::from_utf8_lossy(&output.stdout),
                String::from_utf8_lossy(&output.stderr)
            )
        })
    }

    fn status(&self) -> Value {
        self.ncm_json(false, "status", &scoped_options(&self.project, true))
    }

    fn tool_result(&self, name: &str, arguments: &Value) -> Value {
        let project = self.project.to_string_lossy().into_owned();
        let mut args = strings(&["tool", "--project"]);
        args.push(project);
        args.extend(strings(&[name, "--args"]));
        args.push(arguments.to_string());
        args.push("--json".to_owned());
        let stdout = run_ok(&mut self.cli(&args), &format!("tracedecay tool {name}"));
        let result: Value = serde_json::from_slice(&stdout).unwrap_or_else(|error| {
            panic!(
                "tool {name} returned non-JSON: {error}\nstdout:\n{}\nstderr omitted",
                String::from_utf8_lossy(&stdout)
            )
        });
        assert_ne!(
            result["isError"], true,
            "tool {name} returned an error: {result}"
        );
        result
    }

    fn tool_value(&self, name: &str, arguments: &Value) -> Value {
        let result = self.tool_result(name, arguments);
        let Some(text) = join_content_text(&result) else {
            return result;
        };
        serde_json::from_str(&text).unwrap_or_else(|error| {
            panic!("tool {name} returned non-JSON content `{text}`: {error}")
        })
    }

    fn configuration_revision(&self) -> String {
        let observed = self.tool_value("tracedecay_configuration_observed_state", &json!({}));
        let mut revisions = Vec::new();
        collect_string_field(&observed, "desired_revision_id", &mut revisions);
        revisions.sort();
        revisions.dedup();
        assert_eq!(
            revisions.len(),
            1,
            "configuration components must agree on one revision: {observed}"
        );
        revisions.remove(0)
    }

    fn set_observer(&self, project_id: &str, document: String) {
        let expected_revision = self.configuration_revision();
        let idempotency = format!("ncm-cli-observer-{}", sha256_hex(document.as_bytes()));
        let result = self.tool_result(
            "tracedecay_configuration_set",
            &json!({
                "layer": { "kind": "project", "project_id": project_id },
                "key": NCM_SETTING,
                "value": { "kind": "text", "value": document },
                "expected_revision": expected_revision,
                "idempotency_key": idempotency,
            }),
        );
        let after = self.configuration_revision();
        assert_ne!(
            after, expected_revision,
            "configuration write did not advance revision"
        );
        assert_ne!(result["isError"], true);
    }

    fn set_observer_disabled(&self, project_id: &str) {
        self.set_observer(project_id, disabled_document());
    }

    fn set_observer_enabled(&self, project_id: &str, worker: &Path, state_root: &Path) {
        self.set_observer(project_id, enabled_document(worker, state_root));
    }

    fn write_interrupted_install_journal(&self) -> InterruptedInstall {
        let project_id = self.project_id();
        let profile_id = self.profile_id();
        let operation_id = "interrupted-install";
        let worker_path = self
            .profile
            .join("ncm")
            .join("worker")
            .join(operation_id)
            .join(WORKER_NAME);
        let manifest_path = worker_path
            .parent()
            .expect("interrupted worker parent")
            .join("worker-manifest.json");
        let state_root = self.home.path().join("ncm-state");
        let worker_bytes = b"interrupted worker bytes";
        let manifest_bytes = br#"{"schema_version":1,"interrupted":true}"#;
        fs::create_dir_all(worker_path.parent().expect("worker parent"))
            .expect("interrupted worker parent directory");
        fs::write(&worker_path, worker_bytes).expect("interrupted worker bytes");
        fs::write(&manifest_path, manifest_bytes).expect("interrupted manifest bytes");
        fs::create_dir_all(state_root.join("models")).expect("interrupted model root");
        fs::write(
            state_root.join("models/ncm-encoder-manifest.json"),
            b"model state must survive recovery",
        )
        .expect("interrupted model manifest");

        let journal = json!({
            "schema_version": 1,
            "operation_id": operation_id,
            "operation": "install",
            "phase": "staged",
            "profile_id": profile_id,
            "project_id": project_id,
            "worker_path": worker_path,
            "manifest_path": manifest_path,
            "state_root": state_root,
            "before_document": disabled_document(),
            "after_document": enabled_document(&worker_path, &state_root),
            "before_worker_digest": null,
            "before_manifest_digest": null,
            "after_worker_digest": sha256_hex(worker_bytes),
            "after_manifest_digest": sha256_hex(manifest_bytes),
            "before_worker_backup": null,
            "before_manifest_backup": null,
        });
        let journal_path = self.profile.join("ncm/control-journal-v1.json");
        fs::create_dir_all(journal_path.parent().expect("journal parent"))
            .expect("NCM control root");
        fs::write(
            &journal_path,
            serde_json::to_vec_pretty(&journal).expect("interrupted journal JSON"),
        )
        .expect("interrupted journal");
        InterruptedInstall {
            journal_path,
            worker_path,
            manifest_path,
            state_root,
            worker_bytes: worker_bytes.to_vec(),
        }
    }
}

impl Drop for NcmJourney {
    fn drop(&mut self) {
        self.stop_daemon();
    }
}

struct InterruptedInstall {
    journal_path: PathBuf,
    worker_path: PathBuf,
    manifest_path: PathBuf,
    state_root: PathBuf,
    worker_bytes: Vec<u8>,
}

#[test]
fn ncm_lifecycle_is_opt_in_and_scope_is_exact() {
    let mut journey = NcmJourney::new();
    journey.start_daemon();
    journey.init_project();

    let initial = journey.status();
    assert_eq!(initial["enabled"], false);
    assert_eq!(initial["pending_recovery"], false);
    assert_eq!(initial["worker_present"], false);
    assert_eq!(initial["manifest_present"], false);

    let mut foreign_install = scoped_options(&journey.foreign_project, true);
    foreign_install.extend(strings(&[
        "--worker",
        "relative-worker",
        "--state-root",
        "relative-state",
    ]));
    let confirmation = journey.ncm_output(false, "install", &foreign_install);
    assert!(!confirmation.status.success());
    assert!(
        String::from_utf8_lossy(&confirmation.stderr)
            .contains("ncm install changes project configuration; pass --yes to confirm"),
        "missing confirmation must be refused before scope or path resolution\nstderr:\n{}",
        String::from_utf8_lossy(&confirmation.stderr)
    );

    let wrong_target = journey.ncm_output(
        true,
        "status",
        &scoped_options(&journey.foreign_project, true),
    );
    assert!(!wrong_target.status.success());
    let wrong_target_stderr = String::from_utf8_lossy(&wrong_target.stderr);
    assert!(
        wrong_target_stderr.contains("no registered TraceDecay project at exact root")
            && wrong_target_stderr.contains("no fallback project is substituted"),
        "wrong target must fail closed\nstderr:\n{wrong_target_stderr}"
    );

    let update = journey.ncm_output(true, "update", &scoped_options(&journey.project, true));
    assert!(!update.status.success());
    let update_stderr = String::from_utf8_lossy(&update.stderr);
    if cfg!(all(target_os = "macos", target_arch = "aarch64")) {
        assert!(
            update_stderr.contains("NCM is disabled; use `ncm install`"),
            "update cannot implicitly opt in\nstderr:\n{update_stderr}"
        );
    } else {
        assert!(
            update_stderr.contains("supported only on arm64 macOS"),
            "unsupported targets must fail closed before NCM mutation\nstderr:\n{update_stderr}"
        );
    }

    let recover = journey.ncm_output(true, "recover", &scoped_options(&journey.project, true));
    assert!(!recover.status.success());
    assert!(
        String::from_utf8_lossy(&recover.stderr).contains("no pending NCM control journal"),
        "recover without interruption must fail clearly\nstderr:\n{}",
        String::from_utf8_lossy(&recover.stderr)
    );

    let uninstall = journey.ncm_json(true, "uninstall", &scoped_options(&journey.project, true));
    assert_eq!(uninstall["operation"], "uninstall");
    assert_eq!(uninstall["outcome"], "no_effect");
    assert_eq!(uninstall["model_state"], "preserved");
    assert_eq!(journey.status()["enabled"], false);
}

#[cfg(unix)]
#[test]
fn ncm_recovery_rolls_back_interruption_and_rejects_symlink_and_revision_drift() {
    let mut journey = NcmJourney::new();
    journey.start_daemon();
    journey.init_project();
    let project_id = journey.project_id();
    let interrupted = journey.write_interrupted_install_journal();

    assert_eq!(journey.status()["pending_recovery"], true);
    let blocked = journey.ncm_output(true, "uninstall", &scoped_options(&journey.project, true));
    assert!(!blocked.status.success());
    assert!(
        String::from_utf8_lossy(&blocked.stderr).contains("pending control journal"),
        "mutations must stop at an interruption boundary\nstderr:\n{}",
        String::from_utf8_lossy(&blocked.stderr)
    );

    let symlink_target = journey.home.path().join("symlink-target");
    fs::write(&symlink_target, &interrupted.worker_bytes).expect("symlink target");
    fs::remove_file(&interrupted.worker_path).expect("replace staged worker with symlink");
    std::os::unix::fs::symlink(&symlink_target, &interrupted.worker_path)
        .expect("staged worker symlink");
    let symlink_refusal =
        journey.ncm_output(true, "recover", &scoped_options(&journey.project, true));
    assert!(!symlink_refusal.status.success());
    assert!(
        String::from_utf8_lossy(&symlink_refusal.stderr).contains("not a regular file"),
        "recovery must reject a symlinked staged worker\nstderr:\n{}",
        String::from_utf8_lossy(&symlink_refusal.stderr)
    );
    assert!(interrupted.journal_path.is_file());
    fs::remove_file(&interrupted.worker_path).expect("remove staged worker symlink");
    fs::write(&interrupted.worker_path, &interrupted.worker_bytes)
        .expect("restore staged worker bytes");

    let foreign_worker = journey.home.path().join("foreign-worker");
    let foreign_state = journey.home.path().join("foreign-state");
    journey.set_observer_enabled(&project_id, &foreign_worker, &foreign_state);
    let revision_refusal =
        journey.ncm_output(true, "recover", &scoped_options(&journey.project, true));
    assert!(!revision_refusal.status.success());
    assert!(
        String::from_utf8_lossy(&revision_refusal.stderr)
            .contains("configuration changed outside the pending journal"),
        "recovery must refuse foreign configuration revision\nstderr:\n{}",
        String::from_utf8_lossy(&revision_refusal.stderr)
    );
    assert!(interrupted.journal_path.is_file());
    assert!(interrupted.worker_path.is_file());
    assert!(interrupted.manifest_path.is_file());

    journey.set_observer_disabled(&project_id);
    let recovered = journey.ncm_json(true, "recover", &scoped_options(&journey.project, true));
    assert_eq!(recovered["operation"], "recover");
    assert_eq!(recovered["outcome"], "recovered");
    assert!(!interrupted.journal_path.exists());
    assert!(!interrupted.worker_path.exists());
    assert!(!interrupted.manifest_path.exists());
    assert!(
        interrupted
            .state_root
            .join("models/ncm-encoder-manifest.json")
            .is_file(),
        "recovery preserves model state"
    );
    let final_status = journey.status();
    assert_eq!(final_status["enabled"], false);
    assert_eq!(final_status["pending_recovery"], false);
}

#[cfg(all(target_os = "macos", target_arch = "aarch64"))]
#[test]
#[ignore = "requires the pinned production worker at TRACEDECAY_NCM_WORKER"]
fn ncm_worker_lifecycle_verifies_target_and_preserves_model_state() {
    let worker = PathBuf::from(
        std::env::var_os("TRACEDECAY_NCM_WORKER")
            .expect("TRACEDECAY_NCM_WORKER points to the pinned release worker"),
    );
    assert!(worker.is_absolute() && worker.is_file());

    let mut journey = NcmJourney::new();
    journey.start_daemon();
    journey.init_project();
    let model_root = journey.home.path().join("ncm-state");
    fs::create_dir_all(model_root.join("models")).expect("model root");
    let model_manifest = model_root.join("models/ncm-encoder-manifest.json");
    fs::write(&model_manifest, br#"{"schema_version":1,"fixture":true}"#).expect("model manifest");

    let source_root = journey.home.path().join("worker-source");
    fs::create_dir_all(&source_root).expect("worker source root");
    let source_worker = source_root.join(WORKER_NAME);
    fs::copy(&worker, &source_worker).expect("copy pinned worker into fixture");
    let reference_manifest = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../product/ncm/reference/worker-manifest.json")
        .canonicalize()
        .expect("reference worker manifest");
    let source_manifest = source_root.join("worker-manifest.json");
    fs::copy(&reference_manifest, &source_manifest).expect("copy worker manifest");

    let wrong_target_root = journey.home.path().join("wrong-target");
    fs::create_dir_all(&wrong_target_root).expect("wrong target root");
    fs::copy(&source_worker, wrong_target_root.join(WORKER_NAME)).expect("wrong target worker");
    let mut wrong_manifest: Value =
        serde_json::from_slice(&fs::read(&reference_manifest).expect("reference bytes"))
            .expect("reference manifest JSON");
    wrong_manifest["targets"][0]["triple"] = json!("x86_64-apple-darwin");
    fs::write(
        wrong_target_root.join("worker-manifest.json"),
        serde_json::to_vec_pretty(&wrong_manifest).expect("wrong target manifest"),
    )
    .expect("wrong target manifest bytes");
    let mut wrong_target = scoped_options(&journey.project, true);
    wrong_target.extend(strings(&[
        "--worker",
        wrong_target_root
            .join(WORKER_NAME)
            .to_str()
            .expect("wrong worker path"),
        "--state-root",
        model_root.to_str().expect("model root path"),
    ]));
    let wrong_target_output = journey.ncm_output(true, "install", &wrong_target);
    assert!(!wrong_target_output.status.success());
    assert!(
        String::from_utf8_lossy(&wrong_target_output.stderr)
            .contains("failed offline verification"),
        "wrong target must fail offline verification\nstderr:\n{}",
        String::from_utf8_lossy(&wrong_target_output.stderr)
    );

    let symlink_worker_root = journey.home.path().join("symlink-worker");
    fs::create_dir_all(&symlink_worker_root).expect("symlink worker root");
    std::os::unix::fs::symlink(&source_worker, symlink_worker_root.join(WORKER_NAME))
        .expect("symlink worker");
    fs::copy(
        &source_manifest,
        symlink_worker_root.join("worker-manifest.json"),
    )
    .expect("symlink worker manifest");
    let mut symlink_worker = scoped_options(&journey.project, true);
    symlink_worker.extend(strings(&[
        "--worker",
        symlink_worker_root
            .join(WORKER_NAME)
            .to_str()
            .expect("symlink worker path"),
        "--state-root",
        model_root.to_str().expect("model root path"),
    ]));
    let symlink_output = journey.ncm_output(true, "install", &symlink_worker);
    assert!(!symlink_output.status.success());
    assert!(
        String::from_utf8_lossy(&symlink_output.stderr).contains("failed offline verification"),
        "symlink worker must fail offline verification\nstderr:\n{}",
        String::from_utf8_lossy(&symlink_output.stderr)
    );

    let mut install = scoped_options(&journey.project, true);
    install.extend(strings(&[
        "--worker",
        source_worker.to_str().expect("source worker path"),
        "--state-root",
        model_root.to_str().expect("model root path"),
    ]));
    let installed = journey.ncm_json(true, "install", &install);
    assert_eq!(installed["operation"], "install");
    assert_eq!(installed["outcome"], "committed");
    assert_eq!(installed["model_state"], "preserved");
    assert_eq!(installed["native_state"], "preserved");
    assert_eq!(installed["recall_routing"], "preserved");
    let installed_worker = PathBuf::from(
        installed["worker_path"]
            .as_str()
            .expect("installed worker path"),
    );
    assert!(installed_worker.starts_with(journey.profile.join("ncm/worker")));
    assert!(installed_worker.is_file());
    assert_eq!(journey.status()["enabled"], true);
    assert_eq!(journey.status()["model_manifest_present"], true);

    let updated = journey.ncm_json(true, "update", &scoped_options(&journey.project, true));
    assert_eq!(updated["operation"], "update");
    assert_eq!(updated["outcome"], "committed");
    assert_eq!(
        updated["state_root"].as_str(),
        Some(model_root.to_str().expect("model root is UTF-8"))
    );
    let updated_worker = PathBuf::from(
        updated["worker_path"]
            .as_str()
            .expect("updated worker path"),
    );
    assert_ne!(updated_worker, installed_worker);
    assert!(updated_worker.is_file());

    let uninstalled = journey.ncm_json(true, "uninstall", &scoped_options(&journey.project, true));
    assert_eq!(uninstalled["operation"], "uninstall");
    assert_eq!(uninstalled["outcome"], "committed");
    assert_eq!(uninstalled["model_state"], "preserved");
    assert!(!updated_worker.exists());
    assert!(model_manifest.is_file());
    assert_eq!(
        fs::read(&model_manifest).expect("model state"),
        br#"{"schema_version":1,"fixture":true}"#
    );
    let final_status = journey.status();
    assert_eq!(final_status["enabled"], false);
    assert_eq!(final_status["pending_recovery"], false);
    let no_effect = journey.ncm_json(true, "uninstall", &scoped_options(&journey.project, true));
    assert_eq!(no_effect["outcome"], "no_effect");
}

fn scoped_options(project: &Path, json: bool) -> Vec<String> {
    let mut options = strings(&["--path"]);
    options.push(project.to_string_lossy().into_owned());
    if json {
        options.push("--json".to_owned());
    }
    options
}

fn strings(values: &[&str]) -> Vec<String> {
    values.iter().map(|value| (*value).to_owned()).collect()
}

fn disabled_document() -> String {
    r#"{"mode":"disabled"}"#.to_owned()
}

fn enabled_document(worker: &Path, state_root: &Path) -> String {
    json!({
        "mode": "enabled",
        "worker_binary": worker.to_string_lossy(),
        "state_root": state_root.to_string_lossy(),
    })
    .to_string()
}

fn install_binary_shim(bin_dir: &Path) {
    let shim = bin_dir.join(if cfg!(windows) {
        "tracedecay.exe"
    } else {
        "tracedecay"
    });
    if fs::hard_link(env!("CARGO_BIN_EXE_tracedecay"), &shim).is_err() {
        fs::copy(env!("CARGO_BIN_EXE_tracedecay"), &shim).expect("stage shipped binary");
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;

        let mut permissions = fs::metadata(&shim).expect("shim metadata").permissions();
        permissions.set_mode(0o755);
        fs::set_permissions(&shim, permissions).expect("shim is executable");
    }
}

fn initialize_project(project: &Path, package_name: &str) {
    fs::create_dir_all(project.join("src")).expect("fixture source directory");
    git(project, &["init", "--quiet", "-b", "main"]);
    git(
        project,
        &["config", "user.email", "ncm-journey@example.com"],
    );
    git(project, &["config", "user.name", "NCM Journey"]);
    fs::write(
        project.join("Cargo.toml"),
        format!("[package]\nname=\"{package_name}\"\nversion=\"0.0.0\"\nedition=\"2024\"\n"),
    )
    .expect("fixture manifest");
    fs::write(
        project.join("src/lib.rs"),
        "pub fn ncm_lifecycle_fixture() -> u8 { 7 }\n",
    )
    .expect("fixture source");
    git(project, &["add", "."]);
    git(project, &["commit", "--quiet", "-m", "initial"]);
}

fn git(root: &Path, arguments: &[&str]) {
    let status = Command::new("git")
        .current_dir(root)
        .args(arguments)
        .status()
        .expect("git runs");
    assert!(status.success(), "git {arguments:?} failed");
}

fn daemon_authority_path(profile_root: &Path) -> PathBuf {
    #[cfg(windows)]
    {
        profile_root
            .join("daemon-authority")
            .join("daemon-authority.json")
    }
    #[cfg(not(windows))]
    {
        profile_root.join("daemon-authority.json")
    }
}

fn wait_for_authority(daemon: &mut Child, path: &Path) {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("daemon listener probe runtime");
    let deadline = Instant::now() + Duration::from_secs(180);
    while Instant::now() < deadline {
        if let Some(status) = daemon.try_wait().expect("daemon status") {
            let mut stderr = String::new();
            if let Some(mut piped) = daemon.stderr.take() {
                let _ = piped.read_to_string(&mut stderr);
            }
            panic!("daemon exited before authority publication: {status}; stderr: {stderr}");
        }
        if let Ok(bytes) = fs::read(path)
            && let Ok(record) = serde_json::from_slice::<DaemonAuthorityRecord>(&bytes)
            && record.pid == daemon.id()
            && record.auth_token.len() == 64
            && runtime.block_on(async {
                matches!(
                    tokio::time::timeout_at(
                        tokio::time::Instant::from_std(deadline),
                        BrokerStream::connect(&record.endpoint),
                    )
                    .await,
                    Ok(Ok(_))
                )
            })
        {
            return;
        }
        std::thread::sleep(Duration::from_millis(50));
    }
    panic!(
        "timed out waiting for daemon authority at {}",
        path.display()
    );
}

fn run_ok(command: &mut Command, label: &str) -> Vec<u8> {
    let output = command
        .output()
        .unwrap_or_else(|error| panic!("{label} could not run: {error}"));
    assert!(
        output.status.success(),
        "{label} failed with {}\nstdout:\n{}\nstderr:\n{}",
        output.status,
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    output.stdout
}

fn sha256_hex(bytes: &[u8]) -> String {
    Sha256::digest(bytes)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

fn join_content_text(result: &Value) -> Option<String> {
    let blocks = result.get("content")?.as_array()?;
    let text = blocks
        .iter()
        .filter_map(|block| block.get("text").and_then(Value::as_str))
        .map(|text| {
            text.lines()
                .take_while(|line| !line.trim_start().starts_with("tracedecay_metrics:"))
                .collect::<Vec<_>>()
                .join("\n")
        })
        .filter(|text| !text.trim().is_empty())
        .collect::<Vec<_>>()
        .join("\n\n");
    (!text.is_empty()).then_some(text)
}

fn collect_string_field(value: &Value, field: &str, out: &mut Vec<String>) {
    match value {
        Value::Object(members) => {
            for (key, member) in members {
                if key == field
                    && let Some(text) = member.as_str()
                {
                    out.push(text.to_owned());
                } else {
                    collect_string_field(member, field, out);
                }
            }
        }
        Value::Array(items) => {
            for item in items {
                collect_string_field(item, field, out);
            }
        }
        _ => {}
    }
}
