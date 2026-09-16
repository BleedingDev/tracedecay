//! Live Hermes provider journey through the shipped CLI and generated plugin.
//!
//! The test installs Hermes into an isolated HOME, starts a real TraceDecay
//! daemon, registers a real git project, enables Native through the operator
//! configuration surface, and then runs the installed Python provider. The
//! provider's project-scoped `ingest_transcript` call and its asynchronous
//! `turnCompleted`/`turnIngested` callbacks are therefore exercised as Hermes
//! invokes them. The project observation journal and a later context recall
//! are the assertions that the full route settled.

use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::OnceLock;
use std::time::{Duration, Instant};

use serde_json::{Value, json};
use tempfile::TempDir;
use tracedecay_daemon_identity::authority::DaemonAuthorityRecord;
use tracedecay_daemon_protocol::BrokerStream;
use tracedecay_domain::configuration::{
    MEMORY_PROVIDER_NATIVE_ENABLED_SETTING_KEY, MEMORY_PROVIDER_RECALL_ROUTING_SETTING_KEY,
};
use tracedecay_memory_observation::{
    DeliveryStateV1, JournalInspectionFilterV1, JournalInspectionRowV1, ObservationJournalReaderV1,
    RetentionPolicyV1, SqliteObservationJournal,
};

const SESSION_ID: &str = "hermes-cli-project-journey";
const PROJECT_TERM: &str = "quartz";
const JOURNAL_FILE_NAME: &str = "memory-observation-journal-v1.sqlite3";
const OBSERVATION_KIND: &str = "session.message_committed.v1";
const PROVIDER_ID: &str = "tracedecay.native";
const JOURNAL_POLL_INTERVAL: Duration = Duration::from_millis(100);
const SETTLEMENT_BUDGET: Duration = Duration::from_secs(60);

struct HermesJourney {
    home: TempDir,
    profile: PathBuf,
    project: PathBuf,
    bin_dir: PathBuf,
    daemon: Option<Child>,
    journal: OnceLock<SqliteObservationJournal>,
}

impl HermesJourney {
    fn new() -> Self {
        let home = TempDir::new().expect("isolated Hermes HOME");
        let root = home.path().to_path_buf();
        let profile = root.join(".tracedecay");
        let project = root.join("project");
        let bin_dir = root.join("bin");
        fs::create_dir_all(&profile).expect("profile root");
        fs::create_dir_all(root.join(".hermes")).expect("Hermes home");
        fs::create_dir_all(&bin_dir).expect("binary shim directory");
        install_binary_shim(&bin_dir);
        initialize_project(&project);
        Self {
            home,
            profile,
            project,
            bin_dir,
            daemon: None,
            journal: OnceLock::new(),
        }
    }

    fn cli(&self, args: &[&str]) -> Command {
        let inherited_path = std::env::var_os("PATH").unwrap_or_default();
        let path = std::env::join_paths(
            std::iter::once(self.bin_dir.clone()).chain(std::env::split_paths(&inherited_path)),
        )
        .expect("isolated PATH");
        let mut command = Command::new(env!("CARGO_BIN_EXE_tracedecay"));
        command
            .args(args)
            .current_dir(&self.project)
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
        assert!(self.daemon.is_none(), "daemon already running");
        let log = fs::File::create(self.home.path().join("daemon.stderr.log")).expect("daemon log");
        let mut command = self.cli(&["daemon", "run"]);
        command.stdout(Stdio::null()).stderr(Stdio::from(log));
        let mut daemon = command.spawn().expect("daemon starts");
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
            let output = self.cli(&["init"]).output().expect("tracedecay init runs");
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

    fn project_id(&self) -> String {
        let bytes = run_ok(
            &mut self
                .cli(&["projects", "context"])
                .arg(&self.project)
                .arg("--json"),
            "projects context",
        );
        let context: Value = serde_json::from_slice(&bytes).expect("project context JSON");
        context["project"]["project_id"]
            .as_str()
            .expect("project id")
            .to_owned()
    }

    fn configuration_revision(&self) -> String {
        let value = self.tool_result("tracedecay_configuration_observed_state", &json!({}));
        let mut revisions = Vec::new();
        collect_string_field(&value, "desired_revision_id", &mut revisions);
        revisions.sort();
        revisions.dedup();
        assert_eq!(
            revisions.len(),
            1,
            "one canonical configuration revision: {value}"
        );
        revisions.remove(0)
    }

    fn configuration_set(&self, project_id: &str, key: &str, value: Value, idempotency_key: &str) {
        let expected_revision = self.configuration_revision();
        self.tool_result(
            "tracedecay_configuration_set",
            &json!({
                "layer": { "kind": "project", "project_id": project_id },
                "key": key,
                "value": value,
                "expected_revision": expected_revision,
                "idempotency_key": idempotency_key,
            }),
        );
        assert_ne!(
            self.configuration_revision(),
            expected_revision,
            "configuration write must advance revision"
        );
    }

    fn configure_native(&self, project_id: &str) {
        self.configuration_set(
            project_id,
            MEMORY_PROVIDER_NATIVE_ENABLED_SETTING_KEY,
            json!({ "kind": "boolean", "value": true }),
            "configuration.idempotency.hermes-cli-project-journey.native",
        );
        self.configuration_set(
            project_id,
            MEMORY_PROVIDER_RECALL_ROUTING_SETTING_KEY,
            json!({
                "kind": "text",
                "value": json!({
                    "active_provider": PROVIDER_ID,
                    "degradation": {
                        "policy_id": "policy.hermes-cli-project-journey.v1",
                        "policy_revision": 1,
                        "allowed_causes": ["partial", "stale"],
                    },
                }).to_string(),
            }),
            "configuration.idempotency.hermes-cli-project-journey.routing",
        );
    }

    fn tool_result(&self, name: &str, args: &Value) -> Value {
        let project = self.project.to_string_lossy().to_string();
        let payload = args.to_string();
        let bytes = run_ok(
            &mut self.cli(&[
                "tool",
                "--project",
                &project,
                name,
                "--args",
                &payload,
                "--json",
            ]),
            name,
        );
        let result: Value = serde_json::from_slice(&bytes)
            .unwrap_or_else(|error| panic!("{name} returned invalid JSON: {error}"));
        assert_ne!(result["isError"], true, "{name} failed: {result}");
        result
    }

    fn context(&self, task: &str) -> Value {
        let result = self.tool_result(
            "tracedecay_context",
            &json!({ "task": task, "format": "json", "_meta": { "session_id": SESSION_ID } }),
        );
        content_json(&result).unwrap_or(result)
    }

    fn analytics_diagnostics(&self) -> Value {
        let bytes = run_ok(
            &mut self.cli(&["analytics", "diagnostics", "--no-sync"]),
            "analytics diagnostics",
        );
        serde_json::from_slice(&bytes).expect("analytics diagnostics JSON")
    }

    fn install_hermes_plugin(&self) {
        run_ok(
            &mut self.cli(&["install", "--agent", "hermes", "--no-dashboard"]),
            "install Hermes plugin",
        );
        let plugin = self.home.path().join(".hermes/plugins/tracedecay");
        for file in ["plugin.yaml", "__init__.py", "tools.py", "schemas.py"] {
            assert!(
                plugin.join(file).is_file(),
                "installed Hermes plugin missing {file}"
            );
        }
    }

    fn run_provider_fixture(&self) {
        let script = Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("tests/product_memory_provider_hermes_host_journey/hermes_sync_turn.py");
        let plugin = self.home.path().join(".hermes/plugins/tracedecay");
        let output = python_command()
            .arg(script)
            .arg(plugin)
            .arg(&self.project)
            .arg(env!("CARGO_BIN_EXE_tracedecay"))
            .current_dir(&self.project)
            .env("HOME", self.home.path())
            .env("USERPROFILE", self.home.path())
            .env("XDG_CONFIG_HOME", self.home.path().join(".config"))
            .env("TRACEDECAY_DATA_DIR", &self.profile)
            .env("TRACEDECAY_GLOBAL_DB", self.profile.join("global.db"))
            .env("TRACEDECAY_TEST_ALLOW_INCOMPLETE_HOLDER_SCAN", "1")
            .output()
            .expect("Hermes Python fixture runs");
        assert!(
            output.status.success(),
            "Hermes provider fixture failed\nstdout:\n{}\nstderr:\n{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        let result: Value =
            serde_json::from_slice(&output.stdout).expect("Hermes fixture sentinel JSON");
        assert_eq!(result["sync"], "complete");
        let expected_project = fs::canonicalize(&self.project).expect("canonical project root");
        assert_eq!(result["project_root"].as_str(), expected_project.to_str());
    }

    fn journal(&self) -> Option<&SqliteObservationJournal> {
        if let Some(journal) = self.journal.get() {
            return Some(journal);
        }
        let path = find_file(&self.profile, JOURNAL_FILE_NAME)?;
        let journal = SqliteObservationJournal::open_existing(&path, inspection_policy())
            .expect("Native observation journal opens");
        let _ = self.journal.set(journal);
        self.journal.get()
    }

    fn journal_rows(&self) -> Vec<JournalInspectionRowV1> {
        let Some(journal) = self.journal() else {
            return Vec::new();
        };
        journal
            .inspect(&JournalInspectionFilterV1 {
                limit: 100,
                provider_id: Some(PROVIDER_ID.to_owned()),
                ..JournalInspectionFilterV1::default()
            })
            .expect("Native journal inspection")
            .rows
    }

    fn await_settled_rows(&self) -> Vec<JournalInspectionRowV1> {
        let deadline = Instant::now() + SETTLEMENT_BUDGET;
        loop {
            let rows = self.journal_rows();
            if rows.len() >= 2 && rows.iter().all(|row| row.state.is_terminal()) {
                return rows;
            }
            assert!(
                Instant::now() < deadline,
                "Hermes project observation journal did not settle: {}",
                journal_digest(&rows)
            );
            std::thread::sleep(JOURNAL_POLL_INTERVAL);
        }
    }
}

impl Drop for HermesJourney {
    fn drop(&mut self) {
        self.stop_daemon();
    }
}

#[test]
fn installed_hermes_provider_scopes_native_observations_and_recall_to_the_project() {
    let mut journey = HermesJourney::new();
    journey.install_hermes_plugin();
    journey.start_daemon();
    journey.init_project();
    let project_id = journey.project_id();
    journey.configure_native(&project_id);
    journey.stop_daemon();
    journey.start_daemon();

    // First context call mounts the configured Native provider and its journal
    // before Hermes writes evidence, keeping project scoping observable.
    let baseline = journey.context("hermes quartz project provider baseline");
    let baseline_lane = baseline
        .get("advisory_provider_memory")
        .expect("baseline advisory provider lane");
    assert_eq!(baseline_lane["provider_id"], PROVIDER_ID);
    assert!(
        journey.journal().is_some(),
        "Native journal must be mounted"
    );
    assert!(
        journey.journal_rows().is_empty(),
        "baseline journal is empty"
    );

    journey.run_provider_fixture();
    let rows = journey.await_settled_rows();
    assert_eq!(rows.len(), 2, "one user and one assistant message");
    let mut source_sequences = rows
        .iter()
        .map(|row| row.source_sequence.0)
        .collect::<Vec<_>>();
    source_sequences.sort_unstable();
    source_sequences.dedup();
    assert_eq!(
        source_sequences.len(),
        2,
        "messages occupy distinct source positions"
    );
    for row in &rows {
        assert_eq!(row.provider_id, PROVIDER_ID);
        assert_eq!(row.observation_kind, OBSERVATION_KIND);
        assert_eq!(row.state, DeliveryStateV1::Acknowledged);
        assert_eq!(row.attempt_number, 1);
        assert!(row.content_present);
        let receipts = journey
            .journal()
            .expect("Native journal")
            .receipts_for(&row.observation_id)
            .expect("Native provider receipt");
        assert_eq!(receipts.len(), 1, "one applied receipt per message");
    }

    // The generated plugin calls the same turn twice only at the host level;
    // re-running the provider fixture is a distinct turn. Instead, prove the
    // first callback/ingest result is idempotent by replaying its exact
    // projectless/project-scoped event through the hidden shipped hook command
    // after the real fixture settled. The hook itself remains project-bound by
    // cwd and must not create any extra observation rows.
    let receipt = json!({
        "agent": "hermes",
        "event": "turnIngested",
        "cwd": journey.project.to_string_lossy(),
        "route": { "session_id": SESSION_ID, "cwd": journey.project.to_string_lossy() },
        "receipt": { "status": "success", "transcript_watermark": "tracedecay_sync_1" },
    })
    .to_string();
    let before = journal_identities(&rows);
    for _ in 0..2 {
        let mut command = journey.cli(&["hook-hermes-terminal-receipt"]);
        command.stdin(Stdio::piped());
        let mut child = command.spawn().expect("Hermes receipt hook starts");
        child
            .stdin
            .take()
            .expect("receipt stdin")
            .write_all(receipt.as_bytes())
            .expect("receipt event writes");
        let output = child.wait_with_output().expect("receipt hook exits");
        assert!(
            output.status.success(),
            "project-scoped Hermes receipt hook failed\nstdout:\n{}\nstderr:\n{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
    }
    assert_eq!(journal_identities(&journey.journal_rows()), before);

    // A resolved project callback writes its hook analytics into this
    // project's store. The bounded poll also waits for the daemon's
    // best-effort analytics side write, and distinguishes the project route
    // from the projectless Hermes receipt fallback.
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let diagnostics = journey.analytics_diagnostics();
        if diagnostics["hook_call_count"].as_i64().unwrap_or_default() > 0
            && diagnostics["by_event_kind"].as_array().is_some_and(|rows| {
                rows.iter().any(|row| {
                    row["event_kind"] == "hook_route"
                        && row["count"].as_i64().unwrap_or_default() > 0
                })
            })
        {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "Hermes project callback did not leave project-scoped hook analytics: {diagnostics}"
        );
        std::thread::sleep(JOURNAL_POLL_INTERVAL);
    }

    let answer = journey.context("what did the Hermes quartz project observation record?");
    let lane = answer
        .get("advisory_provider_memory")
        .expect("recall advisory provider lane");
    assert_eq!(lane["provider_id"], PROVIDER_ID);
    assert_eq!(lane["state"], "answered");
    let candidates = lane["candidates"].as_array().expect("recall candidates");
    assert!(
        candidates.len() >= 2,
        "recall returns both Hermes messages: {lane}"
    );
    assert!(
        candidates.iter().all(|candidate| candidate["content"]
            .as_str()
            .is_some_and(|content| content.contains(PROJECT_TERM))),
        "recall returns the project-scoped Hermes evidence: {lane}"
    );
}

fn inspection_policy() -> RetentionPolicyV1 {
    RetentionPolicyV1 {
        ephemeral_max_age_micros: 3_600_000_000,
        session_max_age_micros: 86_400_000_000,
        project_max_age_micros: 2_592_000_000_000,
        profile_max_age_micros: 2_592_000_000_000,
        receipt_retention_micros: 604_800_000_000,
        max_queue_items: 10_000,
        max_queue_bytes: 64 * 1_048_576,
        max_attempts: 8,
        backoff_base_micros: 1_000_000,
        backoff_max_micros: 300_000_000,
        sweep_batch_rows: 512,
    }
}

fn journal_identities(rows: &[JournalInspectionRowV1]) -> Vec<(String, String, u32)> {
    let mut result = rows
        .iter()
        .map(|row| {
            (
                row.idempotency_key.as_str().to_owned(),
                row.observation_id.as_str().to_owned(),
                row.attempt_number,
            )
        })
        .collect::<Vec<_>>();
    result.sort();
    result
}

fn journal_digest(rows: &[JournalInspectionRowV1]) -> String {
    rows.iter()
        .map(|row| {
            format!(
                "{}:{}:{}:{}",
                row.observation_kind,
                row.source_sequence.0,
                row.state.as_wire(),
                row.attempt_number
            )
        })
        .collect::<Vec<_>>()
        .join(",")
}

fn install_binary_shim(bin_dir: &Path) {
    let shim = bin_dir.join(if cfg!(windows) {
        "tracedecay.exe"
    } else {
        "tracedecay"
    });
    if fs::hard_link(env!("CARGO_BIN_EXE_tracedecay"), &shim).is_err() {
        fs::copy(env!("CARGO_BIN_EXE_tracedecay"), &shim).expect("stage binary");
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mut permissions = fs::metadata(&shim).expect("shim metadata").permissions();
        permissions.set_mode(0o755);
        fs::set_permissions(shim, permissions).expect("shim executable");
    }
}

fn initialize_project(project: &Path) {
    fs::create_dir_all(project.join("src")).expect("project source");
    git(project, &["init", "--quiet", "-b", "main"]);
    git(project, &["config", "user.email", "journey@example.com"]);
    git(project, &["config", "user.name", "Hermes Journey"]);
    fs::write(
        project.join("Cargo.toml"),
        "[package]\nname=\"hermes-cli-journey-fixture\"\nversion=\"0.0.0\"\nedition=\"2024\"\n",
    )
    .expect("fixture manifest");
    fs::write(
        project.join("src/lib.rs"),
        "pub fn quartz_project_observation() -> u8 { 7 }\n",
    )
    .expect("fixture source");
    git(project, &["add", "."]);
    git(project, &["commit", "--quiet", "-m", "initial"]);
}

fn git(root: &Path, args: &[&str]) {
    let status = Command::new("git")
        .current_dir(root)
        .args(args)
        .status()
        .expect("git runs");
    assert!(status.success(), "git {args:?} failed");
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
        .expect("listener probe runtime");
    let deadline = Instant::now() + Duration::from_secs(180);
    while Instant::now() < deadline {
        if let Some(status) = daemon.try_wait().expect("daemon status") {
            panic!("daemon exited before authority publication: {status}");
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
    panic!("timed out waiting for daemon authority {}", path.display());
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

fn python_command() -> Command {
    static PYTHON: OnceLock<PathBuf> = OnceLock::new();
    let executable = PYTHON.get_or_init(|| {
        if cfg!(windows) {
            for variable in ["Python_ROOT_DIR", "pythonLocation"] {
                if let Some(root) = std::env::var_os(variable) {
                    let candidate = PathBuf::from(root).join("python.exe");
                    if candidate.is_file() {
                        return candidate;
                    }
                }
            }
        }
        PathBuf::from("python3")
    });
    Command::new(executable)
}

fn find_file(root: &Path, name: &str) -> Option<PathBuf> {
    let mut stack = vec![root.to_path_buf()];
    while let Some(path) = stack.pop() {
        let entries = fs::read_dir(path).ok()?;
        for entry in entries.flatten() {
            let candidate = entry.path();
            if candidate.is_dir() {
                stack.push(candidate);
            } else if candidate.file_name().is_some_and(|file| file == name) {
                return Some(candidate);
            }
        }
    }
    None
}

fn content_json(result: &Value) -> Option<Value> {
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
        .join("\n");
    serde_json::from_str(&text).ok()
}

fn collect_string_field(value: &Value, field: &str, output: &mut Vec<String>) {
    match value {
        Value::Object(map) => {
            for (key, member) in map {
                if key == field {
                    if let Some(text) = member.as_str() {
                        output.push(text.to_owned());
                    }
                } else {
                    collect_string_field(member, field, output);
                }
            }
        }
        Value::Array(items) => {
            for item in items {
                collect_string_field(item, field, output);
            }
        }
        _ => {}
    }
}
