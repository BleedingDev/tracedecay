//! The Claude Code and Codex host memory journeys, driven as real subprocesses
//! against a live daemon.
//!
//! Everything here is the shipped `tracedecay` binary: the daemon is
//! `tracedecay daemon run`, the operator gates are committed through
//! `tracedecay tool tracedecay_configuration_set`, and the hooks are the processes
//! Claude Code and Codex spawn (their native session-start and Stop commands,
//! with native payloads on stdin). The later agent question is
//! `tracedecay tool tracedecay_context`. Nothing in this file constructs a
//! hook envelope, seals a binding, or runs an administrative import; the only
//! fixture action that can ingest between the negative control and the first
//! settled rows is the shipped hook process.
//!
//! That ordering is the point. The daemon runs a transcript import when a
//! project mounts, so a journey that writes the transcript *before* the daemon
//! comes up cannot tell a working hook from a broken one — startup would have
//! ingested the same rows. Here the daemon is already up and has already
//! mounted the project (the baseline `tracedecay_context` call below forces
//! that). After startup import and projection complete, the fixture opens an
//! empty Claude transcript or metadata-only Codex rollout and runs SessionStart
//! to establish the live baseline. It then appends the first turn and proves
//! the journal stays empty across a bounded settling window before Stop.
//! Background history reconciliation remains enabled.
//!
//! The ignored real-NCM variants select NCM independently with Native advisory
//! disabled, or retain Native as active with NCM observing. Observer variants
//! require independent applied receipts in both journals. They share only the
//! installed model artifacts; mutable NCM state belongs to the isolated HOME.
//!
//! # What this proves about the product
//!
//! * an operator can reach the provider host through the shipped CLI: the
//!   `memory-provider-host` feature is a real CLI feature, so the binary under
//!   test contains the mount (`required-features` on this target);
//! * Claude Code and Codex lifecycle hook invocations commit their session's
//!   messages as canonical project observations, which the mounted observation
//!   journey then settles against the routed provider exactly once;
//! * a later ordinary `tracedecay_context` call carries the advisory
//!   provider-memory lane, bounded and de-duplicated, naming the provider the
//!   project's own routing policy pinned.

use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Output, Stdio};
use std::sync::OnceLock;
use std::time::{Duration, Instant};

#[cfg(unix)]
#[path = "product_memory_provider_claude_host_journey/comparison_fixture.rs"]
mod comparison_fixture;

use serde_json::{Value, json};
use tempfile::TempDir;
use tracedecay_daemon_identity::authority::DaemonAuthorityRecord;
use tracedecay_daemon_protocol::BrokerStream;
use tracedecay_domain::configuration::{
    MEMORY_PROVIDER_NATIVE_ENABLED_SETTING_KEY, MEMORY_PROVIDER_RECALL_ROUTING_SETTING_KEY,
};
use tracedecay_memory_observation::{
    AdmittedObservationV1, DeliveryStateV1, JournalInspectionFilterV1, JournalInspectionRowV1,
    ObservationCommittedEffectV1, ObservationJournalReaderV1, ObservationOutcomeV1,
    RetentionPolicyV1, SqliteObservationJournal,
};

/// The Claude Code session id the whole journey is bound to.
const CLAUDE_SESSION: &str = "claude-cli-journey-session";
const CODEX_SESSION: &str = "codex-cli-journey-session";

/// A term that appears in the transcript and in the later question, so a recall
/// that answers at all has something to answer with.
const JOURNEY_TERM: &str = "quicksilver";

/// The last words of the long assistant message the mid-session turn writes.
///
/// It exists so recall can be asked for something only an *untruncated*
/// candidate can carry: a lane that returned the head of the message and
/// dropped its tail would still contain [`JOURNEY_TERM`] and would not contain
/// this.
const TAIL_SENTINEL: &str = "obsidian-ledger-tail";

/// The provider identity an operator writes into
/// `memory.provider_recall_routing.v1`. It is the configured, operator-facing
/// spelling of the Native adapter, and the advisory lane must echo exactly it.
const CONFIGURED_PROVIDER_ID: &str = "tracedecay.native";

/// File name of the durable observation journal the mounted journey owns.
const JOURNAL_FILE_NAME: &str = "memory-observation-journal-v1.sqlite3";

/// Matches the adapter's declared `tracedecay_memory_provider_ncm::NCM_PROVIDER_ID`.
/// This CLI target deliberately has no direct dependency on the adapter.
const NCM_OBSERVER_PROVIDER_ID: &str = "ncm";
const NCM_JOURNAL_FILE_NAME: &str = "memory-observation-ncm-journal-v1.sqlite3";

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ActiveProvider {
    Native,
    RustNcm,
}

impl ActiveProvider {
    fn id(self) -> &'static str {
        match self {
            Self::Native => CONFIGURED_PROVIDER_ID,
            Self::RustNcm => NCM_OBSERVER_PROVIDER_ID,
        }
    }

    fn is_ncm(self) -> bool {
        self == Self::RustNcm
    }
}

/// Observation kind the journey admits for host session messages.
const SESSION_MESSAGE_OBSERVATION_KIND: &str = "session.message_committed.v1";

/// How long the journey waits for the journal to settle after the hook. The
/// mounted live replay parks for 250ms between passes, so this is a
/// convergence bound with generous headroom, never a sleep: every wait below
/// returns as soon as its condition holds.
const SETTLEMENT_BUDGET: Duration = Duration::from_secs(60);

/// How often a bounded wait re-reads the journal. Each read opens a second
/// handle on the live store, so the interval keeps the reader from competing
/// with the daemon's own writer; it bounds only how promptly a satisfied
/// condition is noticed, and no assertion rests on it.
const JOURNAL_POLL_INTERVAL: Duration = Duration::from_millis(100);

/// The mounted observation journey's live replay park, in milliseconds
/// (`ObservationJourneyPolicyV1::project_default().delivery_park`). The
/// negative-control window below is a multiple of it, so it is tied to the
/// product's own cadence rather than to a guessed wall-clock number.
const LIVE_REPLAY_PARK_MILLIS: u64 = 250;

/// How many live-replay passes the journal must survive unchanged while a
/// transcript sits on disk and no hook has run.
const QUIESCENCE_REPLAY_PASSES: u64 = 4;

/// How long the journal must *stay* unchanged in that negative control. Long
/// enough that a route ingesting the transcript on its own is caught here
/// rather than mistaken for the hook's effect, short enough that the journey
/// does not pay seconds for it.
const QUIESCENCE_WINDOW: Duration =
    Duration::from_millis(QUIESCENCE_REPLAY_PASSES * LIVE_REPLAY_PARK_MILLIS);

/// How many deliveries one turn from either host contributes: the observation journey
/// admits one `session.message_committed.v1` per committed session message, and
/// each turn written below is one user record and one assistant record.
const ROWS_PER_TURN: usize = 2;

/// One page of inspection is far more than this journey can produce, so a full
/// page means the reader, not the journey, is what changed.
const JOURNAL_INSPECTION_PAGE_LIMIT: u32 = 100;

const USER_DATA_DIR_ENV: &str = "TRACEDECAY_DATA_DIR";
const GLOBAL_DB_ENV: &str = "TRACEDECAY_GLOBAL_DB";

// ---------------------------------------------------------------------------
// Isolated daemon fixture
// ---------------------------------------------------------------------------

struct ClaudeHostJourney {
    codex: bool,
    active_provider: ActiveProvider,
    ncm_observer: bool,
    daemon: Option<Child>,
    home: TempDir,
    profile: PathBuf,
    project: PathBuf,
    bin_dir: PathBuf,
    /// The observer's read handle on the durable journal, opened once.
    ///
    /// Opening the store initializes its schema inside a write transaction, so
    /// re-opening it on every poll would contend with the daemon's own writer
    /// for the duration of the journey. The observation is a read; it takes one
    /// handle and keeps it.
    journal: OnceLock<SqliteObservationJournal>,
    ncm_journal: OnceLock<SqliteObservationJournal>,
}

impl ClaudeHostJourney {
    /// A registered project with the selected advisory provider configured.
    /// Provider settings are `DaemonRestart`, so restart publishes the chosen
    /// composition before the journey begins.
    fn start(codex: bool, active_provider: ActiveProvider, ncm_observer: bool) -> Self {
        let home = TempDir::new().expect("isolated home");
        let mut journey = Self::allocate(home, codex, active_provider, ncm_observer);
        journey.start_daemon();
        journey.initialize_registered_project();
        let project_id = journey.project_id();
        journey.commit_provider_gates(&project_id);
        if active_provider.is_ncm() || ncm_observer {
            journey.commit_real_ncm(&project_id);
        }
        // All provider gates are DaemonRestart settings: restart is what makes the
        // provider host mount.
        journey.stop_daemon();
        journey.start_daemon();
        journey
    }

    /// Allocate paths before choosing any provider or starting a daemon.
    fn allocate(
        home: TempDir,
        codex: bool,
        active_provider: ActiveProvider,
        ncm_observer: bool,
    ) -> Self {
        let root = home.path().to_path_buf();
        let profile = root.join(".tracedecay");
        let project = root.join("project");
        let bin_dir = root.join("bin");
        fs::create_dir_all(&profile).expect("profile root");
        fs::create_dir_all(&bin_dir).expect("bin dir");
        install_binary_shim(&bin_dir);
        initialize_project(&project);

        Self {
            codex,
            active_provider,
            ncm_observer,
            daemon: None,
            home,
            profile,
            project,
            bin_dir,
            journal: OnceLock::new(),
            ncm_journal: OnceLock::new(),
        }
    }

    fn cli(&self, args: &[&str]) -> Command {
        let mut command = Command::new(env!("CARGO_BIN_EXE_tracedecay"));
        let inherited_path = std::env::var_os("PATH").unwrap_or_default();
        let path = std::env::join_paths(
            std::iter::once(self.bin_dir.clone()).chain(std::env::split_paths(&inherited_path)),
        )
        .expect("PATH with the isolated shim first");
        command
            .args(args)
            .current_dir(&self.project)
            .env("HOME", self.home.path())
            .env("USERPROFILE", self.home.path())
            .env("XDG_CONFIG_HOME", self.home.path().join(".config"))
            .env(USER_DATA_DIR_ENV, &self.profile)
            .env(GLOBAL_DB_ENV, self.profile.join("global.db"))
            .env("TRACEDECAY_TEST_ALLOW_INCOMPLETE_HOLDER_SCAN", "1")
            .env("PATH", path)
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        command
    }

    fn start_daemon(&mut self) {
        assert!(self.daemon.is_none(), "a daemon is already running");
        let log = fs::File::create(self.home.path().join("daemon.stderr.log"))
            .expect("isolated daemon log");
        let mut daemon = self
            .cli(&["daemon", "run"])
            .env("TRACEDECAY_TEST_HOST_HISTORY_RECALL_DIAGNOSTICS", "1")
            .stdout(Stdio::null())
            .stderr(Stdio::from(log))
            .spawn()
            .expect("daemon should start");
        wait_for_authority(&mut daemon, &daemon_authority_path(&self.profile));
        self.daemon = Some(daemon);
    }

    /// Invoked only while formatting a failed populated-recall assertion.
    /// Reads opt-in host counters, never ledger tables or raw log content.
    fn native_recall_failure_diagnostics(&self) -> Value {
        if self.active_provider != ActiveProvider::Native {
            return Value::Null;
        }
        let missing = || json!({"availability":"no_readable_opt_in_diagnostics"});
        let Ok(mut log) = fs::File::open(self.home.path().join("daemon.stderr.log")) else {
            return missing();
        };
        let Ok(metadata) = log.metadata() else {
            return missing();
        };
        let offset = metadata.len().saturating_sub(64 * 1024);
        if std::io::Seek::seek(&mut log, std::io::SeekFrom::Start(offset)).is_err() {
            return missing();
        }
        let mut tail = Vec::new();
        if log.take(64 * 1024).read_to_end(&mut tail).is_err() {
            return missing();
        }
        let summaries = String::from_utf8_lossy(&tail)
            .lines()
            .rev()
            .filter_map(recall_diagnostic_summary)
            .take(12)
            .collect::<Vec<_>>();
        if summaries.is_empty() {
            missing()
        } else {
            json!({"order":"newest_first", "events":summaries})
        }
    }

    fn stop_daemon(&mut self) {
        if let Some(mut daemon) = self.daemon.take() {
            let _ = daemon.kill();
            let _ = daemon.wait();
        }
    }

    /// Initializes through the daemon-owned scheduler, grading its real
    /// startup window instead of sleeping past it. Authority publication makes
    /// the transport usable before the scheduler is necessarily mounted; only
    /// that exact retryable state may precede success.
    fn initialize_registered_project(&self) {
        let deadline = Instant::now() + Duration::from_secs(180);
        loop {
            let output = self
                .cli(&["init"])
                .current_dir(&self.project)
                .output()
                .expect("tracedecay init runs");
            if output.status.success() {
                return;
            }
            let stderr = String::from_utf8_lossy(&output.stderr);
            assert!(
                stderr.contains("code_index_scheduler_unavailable"),
                "tracedecay init failed outside the typed scheduler warming window with {}\nstdout:\n{}\nstderr:\n{}",
                output.status,
                String::from_utf8_lossy(&output.stdout),
                stderr
            );
            assert!(
                Instant::now() < deadline,
                "daemon-owned code-index scheduler never became available: {stderr}"
            );
            std::thread::sleep(Duration::from_millis(50));
        }
    }

    /// The registered project identity, read back from the daemon rather than
    /// derived here.
    fn project_id(&self) -> String {
        let stdout = run_ok(
            self.cli(&["projects", "context"])
                .arg(&self.project)
                .arg("--json"),
            "tracedecay projects context",
        );
        let context: Value = serde_json::from_slice(&stdout).expect("project context JSON");
        context["project"]["project_id"]
            .as_str()
            .expect("registered project id")
            .to_owned()
    }

    /// Reads the daemon's existing internal scheduler status transport. The CLI
    /// public tool catalog intentionally omits this administration binding.
    fn startup_sync_status(
        &self,
        runtime: &tokio::runtime::Runtime,
        key: &str,
        deadline: Instant,
    ) -> Value {
        self.startup_sync_status_for_daemon(
            runtime,
            key,
            deadline,
            self.daemon.as_ref().expect("owned journey daemon").id(),
        )
    }

    /// Read the existing startup status when a caller already owns the Child.
    fn startup_sync_status_for_daemon(
        &self,
        runtime: &tokio::runtime::Runtime,
        key: &str,
        deadline: Instant,
        daemon_pid: u32,
    ) -> Value {
        use tokio::io::AsyncWriteExt;
        use tracedecay_daemon_protocol::{
            DaemonClientIdentity, DaemonConnection, DaemonHandshake, MovedStoreAdoption,
            next_daemon_response_line, write_daemon_preamble,
        };
        let authority: DaemonAuthorityRecord = serde_json::from_slice(
            &fs::read(daemon_authority_path(&self.profile)).expect("current daemon authority"),
        )
        .expect("canonical daemon authority");
        assert_eq!(
            authority.pid, daemon_pid,
            "status authority must name the fixture daemon"
        );
        assert_eq!(
            fs::canonicalize(&authority.profile_root).expect("canonical authority profile"),
            fs::canonicalize(&self.profile).expect("canonical isolated profile"),
            "status authority must name the isolated profile"
        );
        let connection = DaemonConnection::new(
            authority.endpoint.clone(),
            Some(authority.auth_token.clone()),
        )
        .with_daemon_version(authority.version.clone());
        let handshake = DaemonHandshake {
            project_path: Some(self.project.clone()),
            scope_prefix: None,
            timings: false,
            allow_init: false,
            allow_initialize_root_routing: false,
            client_identity: DaemonClientIdentity::new(
                authority.profile_root,
                self.profile.join("global.db"),
            ),
            client_version: authority.version,
            client_instance_id: format!("startup-status-fixture-{}", std::process::id()),
            tool_list_changed_capable: false,
            catalog_version: String::new(),
            moved_store_adoption: MovedStoreAdoption::Never,
        };
        let request_id = json!("startup-history-status");
        let remaining = deadline.saturating_duration_since(Instant::now());
        let now = tracedecay_contracts::clock::now_micros();
        let expires_at = tracedecay_domain::UtcMicros(
            now.0
                .saturating_add(i64::try_from(remaining.as_micros()).unwrap_or(i64::MAX)),
        );
        let request = json!({
            "jsonrpc": "2.0", "id": request_id, "method": "tools/call",
            "params": {"name": "tracedecay_admin_cli", "arguments": {
                "action": "sessions_sync_status", "idempotency_key": key,
            }, "_meta": tracedecay_mcp::tool_call_deadline_meta(expires_at)},
        });
        let phase = std::cell::Cell::new("connect");
        runtime.block_on(async {
            tokio::time::timeout_at(tokio::time::Instant::from_std(deadline), async {
                let stream = BrokerStream::connect(&connection.endpoint)
                    .await
                    .expect("connect status transport");
                let (reader, mut writer) = tokio::io::split(stream);
                phase.set("preamble");
                write_daemon_preamble(&mut writer, &connection, &handshake)
                    .await
                    .expect("status preamble");
                phase.set("request_write");
                let mut bytes = serde_json::to_vec(&request).expect("encode status request");
                bytes.push(b'\n');
                writer
                    .write_all(&bytes)
                    .await
                    .expect("write status request");
                writer.flush().await.expect("flush status request");
                phase.set("response_read");
                let mut reader = tokio::io::BufReader::new(reader);
                let line = next_daemon_response_line(
                    &mut reader,
                    &connection,
                    "startup history status",
                    Duration::from_millis(250),
                )
                .await
                .expect("bounded status response")
                .expect("status response must exist");
                let response: Value =
                    serde_json::from_str(&line).expect("status JSON-RPC response");
                assert_eq!(response["jsonrpc"], "2.0", "status JSON-RPC version");
                assert_eq!(response["id"], request_id, "status response correlation");
                assert!(
                    response.get("error").is_none_or(Value::is_null),
                    "status RPC must succeed"
                );
                let result = response
                    .get("result")
                    .filter(|result| result.is_object())
                    .expect("status tool result");
                assert_ne!(result["isError"], true, "status tool must succeed");
                let payload = tracedecay::daemon::tool_json_payload(result, "tracedecay_admin_cli")
                    .expect("status tool JSON payload");
                phase.set("close");
                writer.shutdown().await.expect("close status transport");
                payload
            })
            .await
            .unwrap_or_else(|_| {
                panic!(
                    "startup status exceeded settlement deadline: phase={}",
                    phase.get()
                )
            })
        })
    }

    /// Drain the actual startup import and retained projection before the
    /// fixture writes evidence. These read-only status calls schedule no import.
    fn await_startup_history(&self) {
        let authority: DaemonAuthorityRecord = serde_json::from_slice(
            &fs::read(daemon_authority_path(&self.profile)).expect("current daemon authority"),
        )
        .expect("canonical daemon authority");
        let key = format!(
            "session-sync.startup.{}.{}",
            authority.process_run_id,
            self.project_id()
        );
        let deadline = Instant::now() + SETTLEMENT_BUDGET;
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("startup status runtime");
        loop {
            let outcome = self.startup_sync_status(&runtime, &key, deadline);
            let status = outcome["status"].as_str().expect("startup sync status");
            match status {
                "accepted" | "joined" => {}
                "unavailable"
                    if matches!(
                        outcome["reason_code"].as_str(),
                        Some(
                            "session_sync_authority_unavailable"
                                | "session_sync_operation_not_found"
                        )
                    ) => {}
                "complete" => {
                    assert_eq!(
                        outcome["termination"], "completed",
                        "startup import must complete successfully"
                    );
                    let coverage = outcome["coverage"]
                        .as_array()
                        .expect("startup source coverage");
                    assert!(
                        !coverage.is_empty(),
                        "startup import must report source coverage"
                    );
                    // Match the shipped CLI await's remaining-work accounting.
                    let remaining = coverage
                        .iter()
                        .try_fold(0_u64, |remaining, entry| {
                            let coverage = entry.get("coverage")?;
                            let deferred = match coverage.get("outcome")?.as_str()? {
                                "complete" => 0,
                                "partial" => coverage.get("deferred_units")?.as_u64()?,
                                "backpressured" => coverage.get("rejected_units")?.as_u64()?,
                                _ => return None,
                            };
                            Some(remaining.saturating_add(deferred))
                        })
                        .expect("truthful startup source coverage");
                    assert_eq!(remaining, 0, "startup import must leave no remaining work");
                    break;
                }
                other => panic!("startup import refused with status {other}"),
            }
            assert!(
                Instant::now() < deadline,
                "startup import did not finish; last status {status}, reason {:?}",
                outcome["reason_code"].as_str()
            );
            std::thread::sleep(JOURNAL_POLL_INTERVAL);
        }
        loop {
            let doctor = self.tool("tracedecay_lcm_doctor", &json!({ "format": "json" }));
            let projection = doctor
                .pointer("/outcome/value/payload/projection")
                .expect("project doctor must report its retained projection");
            let state = projection["state"].as_str().expect("projection state");
            match state {
                "current" => return,
                "stale" => {}
                other => panic!("startup projection cannot converge from state {other}"),
            }
            assert!(
                Instant::now() < deadline,
                "startup projection did not converge; last state {state}"
            );
            std::thread::sleep(JOURNAL_POLL_INTERVAL);
        }
    }

    /// One MCP tool call through the shipped `tracedecay tool` surface, which
    /// dispatches over the daemon transport exactly like any other client.
    ///
    /// `--json` makes the CLI print the daemon's own result object verbatim.
    /// The process exits nonzero whenever the daemon marked the call failed,
    /// so [`run_ok`] is the refusal check and nothing here has to re-derive it.
    fn tool_result(&self, name: &str, arguments: &Value) -> Value {
        self.tool_result_with_bytes(name, arguments).0
    }

    fn tool_result_with_bytes(&self, name: &str, arguments: &Value) -> (Value, Vec<u8>) {
        let project = self.project.to_string_lossy().to_string();
        let payload = arguments.to_string();
        let stage = match arguments
            .pointer("/_meta/session_id")
            .and_then(Value::as_str)
        {
            Some(session) if session == self.session_id() => "origin-session",
            Some(_) => "next-session",
            None => "unbound-session",
        };
        let stdout = run_ok(
            &mut self.cli(&[
                "tool",
                "--project",
                &project,
                name,
                "--args",
                &payload,
                "--json",
            ]),
            &format!("tracedecay tool {name} ({stage})"),
        );
        let text = std::str::from_utf8(&stdout).expect("tool output is UTF-8");
        let result = serde_json::from_str(text)
            .unwrap_or_else(|error| panic!("tool {name} answered non-JSON `{text}`: {error}"));
        (result, stdout)
    }

    /// The tool's own JSON payload: compatibility MCP results carry it inside
    /// `content[*].text`, and typed application-surface results are already the
    /// object itself.
    fn tool(&self, name: &str, arguments: &Value) -> Value {
        self.tool_with_bytes(name, arguments).0
    }

    fn tool_with_bytes(&self, name: &str, arguments: &Value) -> (Value, Vec<u8>) {
        let (result, stdout) = self.tool_result_with_bytes(name, arguments);
        assert_ne!(
            result["isError"], true,
            "tool {name} reported an application failure: {result}"
        );
        let Some(text) = join_content_text(&result) else {
            return (result, stdout);
        };
        let decoded = serde_json::from_str(&text).unwrap_or_else(|error| {
            panic!("tool {name} produced non-JSON content text `{text}`: {error}")
        });
        (decoded, stdout)
    }

    /// The configuration revision every component agrees on, which the
    /// revision-CAS write below must present.
    fn configuration_revision(&self) -> String {
        let observed = self.tool_result("tracedecay_configuration_observed_state", &json!({}));
        let mut revisions = Vec::new();
        collect_string_field(&observed, "desired_revision_id", &mut revisions);
        assert!(
            !revisions.is_empty(),
            "configuration observed state must report a desired revision: {observed}"
        );
        revisions.sort();
        revisions.dedup();
        assert_eq!(
            revisions.len(),
            1,
            "configuration components must agree on one desired revision: {observed}"
        );
        revisions.remove(0)
    }

    /// Commits one project-layer setting through the shipped operator surface.
    fn configuration_set(&self, project_id: &str, key: &str, value: Value, idempotency: &str) {
        let expected_revision = self.configuration_revision();
        // A refusal exits nonzero, so reaching the next line already means the
        // operator write settled.
        let _ = self.tool_result(
            "tracedecay_configuration_set",
            &json!({
                "layer": { "kind": "project", "project_id": project_id },
                "key": key,
                "value": value,
                "expected_revision": expected_revision,
                "idempotency_key": idempotency,
            }),
        );
        assert_ne!(
            self.configuration_revision(),
            expected_revision,
            "committing {key} must advance the canonical configuration revision"
        );
    }

    fn commit_provider_gates(&self, project_id: &str) {
        self.configuration_set(
            project_id,
            MEMORY_PROVIDER_NATIVE_ENABLED_SETTING_KEY,
            json!({ "kind": "boolean", "value": self.active_provider == ActiveProvider::Native }),
            "configuration.idempotency.claude-cli-journey-host",
        );
        self.configuration_set(
            project_id,
            MEMORY_PROVIDER_RECALL_ROUTING_SETTING_KEY,
            json!({
                "kind": "text",
                "value": json!({
                    "active_provider": self.active_provider.id(),
                    "degradation": {
                        "policy_id": "policy.host-cli-journey.history.v1",
                        "policy_revision": 1,
                        "allowed_causes": ["partial", "stale"],
                    },
                }).to_string(),
            }),
            "configuration.idempotency.claude-cli-journey-routing",
        );
    }

    /// Only the opt-in real-worker tests call this. The existing TempDir owns
    /// every mutable NCM namespace; only installed model artifacts are shared.
    #[cfg(unix)]
    fn commit_real_ncm(&self, project_id: &str) {
        let worker = PathBuf::from(
            std::env::var_os("TRACEDECAY_NCM_WORKER").expect("real NCM worker binary is required"),
        );
        let installed = PathBuf::from(
            std::env::var_os("TRACEDECAY_NCM_REAL_MODEL_ROOT")
                .expect("installed pinned NCM model root is required"),
        );
        assert!(
            worker.is_absolute() && worker.is_file(),
            "worker must be an absolute binary path"
        );
        assert!(installed.is_absolute(), "model root must be absolute");
        let models = installed
            .join("models")
            .canonicalize()
            .expect("installed models directory");
        assert!(models.is_dir(), "installed root must contain models");
        let state_root = self.home.path().join("ncm-observer");
        fs::create_dir(&state_root).expect("isolated NCM state root");
        std::os::unix::fs::symlink(models, state_root.join("models"))
            .expect("share only installed model artifacts");
        self.configuration_set(
            project_id,
            "memory.provider_ncm_observer.v1",
            json!({
                "kind": "text",
                "value": json!({
                    "mode": "enabled",
                    "worker_binary": worker.canonicalize().expect("canonical worker binary"),
                    "state_root": state_root.canonicalize().expect("canonical isolated state root"),
                }).to_string(),
            }),
            "configuration.idempotency.cli-journey-ncm-observer",
        );
    }

    #[cfg(not(unix))]
    fn commit_real_ncm(&self, _project_id: &str) {
        panic!("the opt-in real NCM model fixture requires Unix");
    }

    /// Runs one shipped Claude lifecycle hook process, handing it the bytes
    /// Claude Code itself writes on stdin.
    fn run_hook(&self, subcommand: &str, payload: &Value) -> Output {
        let payload = payload.to_string();
        let mut command = self.cli(&[subcommand]);
        command.stdin(Stdio::piped());
        let mut child = command
            .spawn()
            .unwrap_or_else(|error| panic!("Claude {subcommand} hook spawns: {error}"));
        child
            .stdin
            .take()
            .expect("hook stdin")
            .write_all(payload.as_bytes())
            .expect("hook payload delivery");
        child.wait_with_output().expect("hook completes")
    }

    /// Captures the first completed turn after the live SessionStart baseline.
    fn run_first_stop_hook(&self) -> Output {
        if self.codex {
            self.run_codex_stop_hook(1)
        } else {
            self.run_stop_hook()
        }
    }

    /// The shipped SessionStart route binding, independent of turn capture.
    fn run_session_start_event(&self, session_id: &str, transcript: &Path) -> Output {
        self.run_hook(
            if self.codex {
                "hook-codex-session-start"
            } else {
                "hook-claude-session-start"
            },
            &json!({
                "session_id": session_id,
                "transcript_path": transcript.to_string_lossy(),
                "cwd": self.project.to_string_lossy(),
                "hook_event_name": "SessionStart",
                "source": "startup",
            }),
        )
    }

    /// The shipped `Stop` hook, with Claude Code's own payload.
    ///
    /// `Stop` — not `PostToolUse` — is the mid-session hook that carries the
    /// turn's own evidence: `hook_stop` runs the project transcript ingest
    /// (`crates/tracedecay-agent-hosts/src/hooks/claude.rs`,
    /// `claude_stop_response_for_event`), while the `PostToolUse` handler only
    /// dispatches guidance and commits nothing.
    fn run_stop_hook(&self) -> Output {
        if self.codex {
            return self.run_codex_stop_hook(2);
        }
        self.run_hook(
            "hook-stop",
            &json!({
                "session_id": CLAUDE_SESSION,
                "transcript_path": self.transcript_path().to_string_lossy(),
                "cwd": self.project.to_string_lossy(),
                "hook_event_name": "Stop",
                "stop_hook_active": false,
            }),
        )
    }

    fn session_id(&self) -> &'static str {
        if self.codex {
            CODEX_SESSION
        } else {
            CLAUDE_SESSION
        }
    }

    /// Native Stop grammar from fixtures/host_events/codex/stop.json.
    fn run_codex_stop_hook(&self, turn: u32) -> Output {
        self.run_hook(
            "hook-codex-stop",
            &json!({
                "session_id": CODEX_SESSION,
                "turn_id": format!("codex-journey-turn-{turn}"),
                "transcript_path": self.transcript_path().to_string_lossy(),
                "cwd": self.project.to_string_lossy(),
                "hook_event_name": "Stop",
                "model": "gpt-5",
                "permission_mode": "default",
                "stop_hook_active": false,
                "last_assistant_message": null,
            }),
        )
    }

    fn transcript_path(&self) -> PathBuf {
        self.transcript_path_for_session(self.session_id())
    }

    fn transcript_path_for_session(&self, session_id: &str) -> PathBuf {
        if self.codex {
            return self
                .home
                .path()
                .join(".codex/sessions/2026/02/01")
                .join(format!("rollout-2026-02-01T00-00-00-{session_id}.jsonl"));
        }
        self.home
            .path()
            .join(".claude/projects/-claude-cli-journey")
            .join(format!("{session_id}.jsonl"))
    }

    /// Opens a real host transcript before its live SessionStart baseline.
    fn initialize_session_transcript(&self, session_id: &str) {
        let path = self.transcript_path_for_session(session_id);
        fs::create_dir_all(path.parent().expect("transcript directory"))
            .expect("transcript directory");
        let initial = if self.codex {
            format!(
                "{}\n",
                json!({
                    "timestamp": "2026-02-01T00:00:00.000Z",
                    "type": "session_meta",
                    "payload": { "id": session_id, "cwd": self.project },
                })
            )
        } else {
            String::new()
        };
        fs::write(path, initial).expect("initialize host transcript");
    }

    /// Appends the unchanged first turn after the live SessionStart baseline.
    fn write_claude_transcript(&self) {
        let path = self.transcript_path();
        let turn = self.claude_turn(
            1,
            "2026-02-01T00:00:00.000Z",
            "2026-02-01T00:00:01.000Z",
            &format!("how does the {JOURNEY_TERM} transport probe decide its retry budget?"),
            &format!(
                "the {JOURNEY_TERM} transport probe reads its retry budget from the pinned \
                 deadline"
            ),
        );
        let mut existing = fs::read_to_string(&path).expect("read host transcript baseline");
        existing.push_str(&turn);
        fs::write(&path, existing).expect("write host transcript");
    }

    /// Appends the second turn: the exchange the session has *while it is
    /// running*, after the SessionStart hook has already settled the first.
    ///
    /// The assistant reply is deliberately long and ends in [`TAIL_SENTINEL`],
    /// so a later recall can be asked to prove it carries the whole message and
    /// not just its opening.
    fn append_mid_session_claude_turn(&self) {
        let path = self.transcript_path();
        let turn = self.claude_turn(
            3,
            "2026-02-01T00:05:00.000Z",
            "2026-02-01T00:05:01.000Z",
            &format!("what did the {JOURNEY_TERM} retry budget change actually record?"),
            &format!(
                "the {JOURNEY_TERM} transport probe records its retry budget change in three \
                 places: the pinned deadline it reads at construction, the attempt ledger it \
                 advances on every refused delivery, and the operator-visible note the session \
                 leaves behind, which ends with {TAIL_SENTINEL}"
            ),
        );
        let mut existing = fs::read_to_string(&path).expect("read Claude transcript");
        existing.push_str(&turn);
        fs::write(&path, existing).expect("append Claude transcript turn");
    }

    /// One user/assistant exchange in Claude Code's own transcript shape,
    /// terminated by a newline so appending another turn stays well-formed
    /// JSONL.
    fn claude_turn(
        &self,
        first_uuid: u32,
        user_timestamp: &str,
        assistant_timestamp: &str,
        user_text: &str,
        assistant_text: &str,
    ) -> String {
        let cwd = self.project.to_string_lossy().to_string();
        if self.codex {
            // Native event_msg records, as in transcript_ingest_suite/codex.rs.
            // Metadata is written once; later turns append to the same rollout.
            let rows = [
                json!({
                    "timestamp": user_timestamp,
                    "type": "event_msg",
                    "payload": { "type": "user_message", "message": user_text },
                }),
                json!({
                    "timestamp": assistant_timestamp,
                    "type": "event_msg",
                    "payload": { "type": "agent_message", "message": assistant_text },
                }),
            ];
            return rows
                .iter()
                .map(Value::to_string)
                .collect::<Vec<_>>()
                .join("\n")
                + "\n";
        }
        let user_uuid = format!("cli-journey-uuid-{first_uuid}");
        let assistant_uuid = format!("cli-journey-uuid-{}", first_uuid + 1);
        let rows = [
            json!({
                "type": "user",
                "cwd": cwd,
                "sessionId": CLAUDE_SESSION,
                "uuid": user_uuid,
                "timestamp": user_timestamp,
                "message": {
                    "role": "user",
                    "content": user_text,
                },
            }),
            json!({
                "type": "assistant",
                "cwd": cwd,
                "sessionId": CLAUDE_SESSION,
                "uuid": assistant_uuid,
                "parentUuid": user_uuid,
                "timestamp": assistant_timestamp,
                "message": {
                    "id": format!("msg_cli_journey_{}", first_uuid + 1),
                    "role": "assistant",
                    "model": "claude-opus-4-8",
                    "content": [{
                        "type": "text",
                        "text": assistant_text,
                    }],
                },
            }),
        ];
        let mut turn = rows
            .iter()
            .map(Value::to_string)
            .collect::<Vec<_>>()
            .join("\n");
        turn.push('\n');
        turn
    }

    /// The durable observation journal the mounted journey owns, located under
    /// this profile's own store layout rather than guessed at.
    fn journal_path(&self) -> Option<PathBuf> {
        find_file(
            &self.profile,
            if self.active_provider.is_ncm() {
                NCM_JOURNAL_FILE_NAME
            } else {
                JOURNAL_FILE_NAME
            },
        )
    }

    /// The read handle on the durable journal, opened the first time the store
    /// exists and reused for the rest of the journey.
    ///
    /// `None` only while the mounted journey has not created its store yet,
    /// which is genuinely "no deliveries"; a store that exists but refuses to
    /// open fails the test loudly rather than reading as an empty journal.
    fn journal_for(&self, ncm: bool) -> Option<&SqliteObservationJournal> {
        let slot = if ncm {
            &self.ncm_journal
        } else {
            &self.journal
        };
        if let Some(journal) = slot.get() {
            return Some(journal);
        }
        // Both are host-owned journals under this isolated canonical profile.
        // An independently active NCM mount does not require a Native journal.
        let path = find_file(
            &self.profile,
            if ncm {
                NCM_JOURNAL_FILE_NAME
            } else {
                JOURNAL_FILE_NAME
            },
        )?;
        let journal = SqliteObservationJournal::open(&path, inspection_retention_policy())
            .expect("the durable observation journal must open through its own store API");
        let _ = slot.set(journal);
        slot.get()
    }

    /// Every delivery the durable observation journal holds, read through the
    /// journal crate's own inspection surface.
    ///
    /// No SQL and no column name appears here: the journal's schema belongs to
    /// `tracedecay-memory-observation`, and a reader that re-derived it could
    /// silently return nothing after a schema change and be mistaken for an
    /// empty journal. A journal file that does not exist yet is genuinely "no
    /// rows"; every other failure — the store will not open, the inspection is
    /// refused, the page did not fit — fails the test loudly.
    fn journal_rows(&self) -> Vec<JournalInspectionRowV1> {
        self.journal_rows_for(self.active_provider.is_ncm())
    }

    fn journal_rows_for(&self, ncm: bool) -> Vec<JournalInspectionRowV1> {
        self.all_journal_rows_for(ncm)
            .into_iter()
            .filter(|row| {
                if row.source_stream == "session_observation_store" {
                    return true;
                }
                let journal = self.journal_for(ncm).expect("mounted journal");
                let admitted = journal
                    .read_admitted_observation_by_idempotency(&row.idempotency_key)
                    .expect("retained history admission read")
                    .expect("history content remains retained");
                assert_history_stream(row, &admitted);
                false
            })
            .collect()
    }

    fn all_journal_rows_for(&self, ncm: bool) -> Vec<JournalInspectionRowV1> {
        let Some(journal) = self.journal_for(ncm) else {
            return Vec::new();
        };
        let page = journal
            .inspect(&JournalInspectionFilterV1 {
                limit: JOURNAL_INSPECTION_PAGE_LIMIT,
                ..JournalInspectionFilterV1::default()
            })
            .expect("the durable observation journal must answer an inspection");
        assert!(
            page.next_cursor.is_none(),
            "this journey cannot produce more than {JOURNAL_INSPECTION_PAGE_LIMIT} deliveries; \
             {} rows were reported",
            page.total_rows
        );
        page.rows
    }

    /// Waits, bounded, until the journal holds at least `minimum_rows`
    /// deliveries and every one of them is terminal, and returns them the
    /// moment it does. A deadline is a failure that reports what it last saw,
    /// never a half-settled journal handed back to be asserted against.
    fn await_settled_journal(&self, minimum_rows: usize) -> Vec<JournalInspectionRowV1> {
        self.await_settled_journal_for(self.active_provider.is_ncm(), minimum_rows)
    }

    fn await_settled_journal_for(
        &self,
        ncm: bool,
        minimum_rows: usize,
    ) -> Vec<JournalInspectionRowV1> {
        let deadline = Instant::now() + SETTLEMENT_BUDGET;
        loop {
            let rows = self.journal_rows_for(ncm);
            if rows.len() >= minimum_rows && rows.iter().all(|row| row.state.is_terminal()) {
                return rows;
            }
            assert!(
                Instant::now() < deadline,
                "the hook's observations (NCM={ncm}) never settled {minimum_rows} deliveries within \
                 {SETTLEMENT_BUDGET:?}; last saw {:?}; daemon stderr: {}",
                journal_digest(&rows),
                {
                    let mut log = fs::File::open(self.home.path().join("daemon.stderr.log"))
                        .expect("read isolated daemon log");
                    let offset = log
                        .metadata()
                        .expect("log metadata")
                        .len()
                        .saturating_sub(16 * 1024);
                    std::io::Seek::seek(&mut log, std::io::SeekFrom::Start(offset))
                        .expect("seek diagnostic tail");
                    let mut tail = Vec::new();
                    log.take(16 * 1024)
                        .read_to_end(&mut tail)
                        .expect("read diagnostic tail");
                    tracedecay_runtime_core::privacy::sanitize_provider_metadata_text(
                        &String::from_utf8_lossy(&tail),
                    )
                    .unwrap_or_else(|| "[daemon diagnostics withheld by privacy policy]".to_owned())
                }
            );
            std::thread::sleep(JOURNAL_POLL_INTERVAL);
        }
    }

    /// Both recipients must independently acknowledge the same canonical
    /// messages. Their provider-addressed observation/idempotency IDs differ.
    fn assert_observer_settled(
        &self,
        native: &[JournalInspectionRowV1],
        previous: &[JournalInspectionRowV1],
    ) -> Vec<JournalInspectionRowV1> {
        if !self.ncm_observer {
            return Vec::new();
        }
        let observer = self.await_settled_journal_for(true, native.len());
        assert_settled_session_messages(&observer, native.len());
        assert_eq!(
            canonical_row_identities(&observer),
            canonical_row_identities(native),
            "both providers must receive exactly the same canonical session messages"
        );
        let identities = journal_row_identities(&observer);
        assert!(
            journal_row_identities(previous)
                .iter()
                .all(|row| identities.contains(row)),
            "observer replay and append must preserve every settled delivery identity"
        );
        for (ncm, provider, rows) in [
            (false, CONFIGURED_PROVIDER_ID, native),
            (true, NCM_OBSERVER_PROVIDER_ID, observer.as_slice()),
        ] {
            let journal = self.journal_for(ncm).expect("mounted recipient journal");
            for row in rows {
                assert_eq!(row.provider_id, provider);
                let receipts = journal
                    .receipts_for(&row.observation_id)
                    .expect("provider receipts");
                assert_eq!(
                    receipts.len(),
                    1,
                    "one provider receipt per committed message"
                );
                let receipt = &receipts[0];
                assert_eq!(receipt.provider_id.as_str(), provider);
                assert_eq!(receipt.observation_id, row.observation_id);
                assert_eq!(receipt.idempotency_key, row.idempotency_key);
                assert_eq!(receipt.payload_sha256, row.payload_sha256);
                assert_eq!(receipt.extensions_digest, row.extensions_digest);
                assert_eq!(receipt.registration_revision, row.registration_revision);
                assert_eq!(
                    receipt.provider_instance_id.as_ref(),
                    Some(&row.provider_instance_id)
                );
                assert_eq!(receipt.attempt_number, 1);
                assert_eq!(receipt.outcome, ObservationOutcomeV1::Applied);
                assert_eq!(
                    receipt.committed_effect,
                    ObservationCommittedEffectV1::Applied
                );
                assert!(
                    receipt
                        .provider_receipt_digest
                        .as_ref()
                        .is_some_and(|digest| digest.len() == 64
                            && digest.bytes().all(|byte| byte.is_ascii_hexdigit())),
                    "an applied delivery must carry its provider acknowledgement digest"
                );
            }
        }
        observer
    }

    /// The negative control. For one window derived from the journey's own
    /// live-replay park, the journal must hold exactly the deliveries it held
    /// before the transcript was written — which is what makes the next hook
    /// invocation the only possible cause of the rows that follow.
    fn assert_journal_unchanged_without_a_hook(&self, expected: &[JournalInspectionRowV1]) {
        let expected_identities = journal_row_identities(expected);
        let deadline = Instant::now() + QUIESCENCE_WINDOW;
        while Instant::now() < deadline {
            let rows = self.journal_rows();
            assert_eq!(
                journal_row_identities(&rows),
                expected_identities,
                "a transcript on disk must not reach the journal on its own; the hook is what \
                 commits it. Observed {:?}",
                journal_digest(&rows)
            );
            if self.ncm_observer {
                assert_eq!(
                    canonical_row_identities(&self.journal_rows_for(true)),
                    canonical_row_identities(expected),
                    "observer journal must also stay unchanged without a hook"
                );
            }
            std::thread::sleep(JOURNAL_POLL_INTERVAL);
        }
    }
}

/// A valid retention policy for a *read-only* second handle on the journal.
///
/// This handle never appends, leases, sweeps, or forgets, so none of these
/// bounds ever applies to anything; they exist because `open` validates a
/// policy before it will hand back a store. The bounds the running journey
/// actually enforces are the daemon's own
/// (`ObservationJourneyPolicyV1::project_default`), which this test has no
/// business restating.
fn inspection_retention_policy() -> RetentionPolicyV1 {
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

/// The row identity a replayed hook must reproduce exactly: who the
/// observation is, what content it carries, and how many attempts it cost.
///
/// Comparing this set — not a length — is what makes the idempotency claim
/// real: a journal that dropped one row and admitted a different one has the
/// same length and a different set.
fn journal_row_identities(rows: &[JournalInspectionRowV1]) -> Vec<(String, String, String, u32)> {
    let mut identities = rows
        .iter()
        .map(|row| {
            (
                row.idempotency_key.as_str().to_owned(),
                row.observation_id.as_str().to_owned(),
                row.payload_sha256.clone(),
                row.attempt_number,
            )
        })
        .collect::<Vec<_>>();
    identities.sort();
    identities
}

/// Canonical identity shared across provider-addressed journals.
fn canonical_row_identities(
    rows: &[JournalInspectionRowV1],
) -> Vec<(String, String, u64, String, String)> {
    let mut identities = rows
        .iter()
        .map(|row| {
            (
                row.exact_scope_sha256.clone(),
                row.source_stream.clone(),
                row.source_sequence.0,
                row.payload_sha256.clone(),
                row.extensions_digest.clone(),
            )
        })
        .collect::<Vec<_>>();
    identities.sort();
    identities
}

/// A compact, sorted description of the journal, so a failed wait names what it
/// actually observed instead of reporting only a deadline.
fn journal_digest(rows: &[JournalInspectionRowV1]) -> Vec<String> {
    let mut digest = rows
        .iter()
        .map(|row| {
            format!(
                "{}|{}|{}|attempt={}|seq={}|content_present={}",
                row.observation_kind,
                row.exact_scope_sha256,
                row.state.as_wire(),
                row.attempt_number,
                row.source_sequence.0,
                row.content_present,
            )
        })
        .collect::<Vec<_>>();
    digest.sort();
    digest
}

/// Asserts the settled shape every committed session message must have.
fn assert_settled_session_messages(rows: &[JournalInspectionRowV1], expected: usize) {
    assert_eq!(
        rows.len(),
        expected,
        "the committed Claude turns must produce exactly {expected} deliveries: {:?}",
        journal_digest(rows)
    );
    let mut scopes = rows
        .iter()
        .map(|row| row.exact_scope_sha256.as_str())
        .collect::<Vec<_>>();
    scopes.sort_unstable();
    scopes.dedup();
    assert_eq!(
        scopes.len(),
        1,
        "every row this host session produced belongs to one exact coding scope: {:?}",
        journal_digest(rows)
    );
    for row in rows {
        assert_eq!(
            row.observation_kind,
            SESSION_MESSAGE_OBSERVATION_KIND,
            "the hook admits exactly the session-message observation kind: {:?}",
            journal_digest(rows)
        );
        assert_eq!(
            row.state,
            DeliveryStateV1::Acknowledged,
            "the routed provider accepts session messages, so the row settles acknowledged: {:?}",
            journal_digest(rows)
        );
        assert_eq!(
            row.attempt_number,
            1,
            "an accepted observation is delivered once, never retried: {:?}",
            journal_digest(rows)
        );
        assert!(
            row.content_present,
            "a settled delivery still holds its content until retention takes it: {:?}",
            journal_digest(rows)
        );
    }
    // Distinct messages, not one message journalled repeatedly.
    let mut sequences = rows
        .iter()
        .map(|row| row.source_sequence.0)
        .collect::<Vec<_>>();
    sequences.sort_unstable();
    sequences.dedup();
    assert_eq!(
        sequences.len(),
        expected,
        "each committed message occupies its own source position: {:?}",
        journal_digest(rows)
    );
}

impl Drop for ClaudeHostJourney {
    fn drop(&mut self) {
        self.stop_daemon();
    }
}

// ---------------------------------------------------------------------------
// Fixture helpers
// ---------------------------------------------------------------------------

fn install_binary_shim(bin_dir: &Path) {
    let shim = bin_dir.join(if cfg!(windows) {
        "tracedecay.exe"
    } else {
        "tracedecay"
    });
    if fs::hard_link(env!("CARGO_BIN_EXE_tracedecay"), &shim).is_err() {
        fs::copy(env!("CARGO_BIN_EXE_tracedecay"), &shim).expect("stage the shipped binary");
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;

        let mut permissions = fs::metadata(&shim).expect("shim metadata").permissions();
        permissions.set_mode(0o755);
        fs::set_permissions(&shim, permissions).expect("shim is executable");
    }
}

/// A real git project: the composition refuses to resolve an exact scope
/// without a repository, a worktree, and a checked-out reference.
fn initialize_project(project: &Path) {
    fs::create_dir_all(project.join("src")).expect("project source directory");
    git(project, &["init", "--quiet", "-b", "main"]);
    git(project, &["config", "user.email", "journey@example.com"]);
    git(project, &["config", "user.name", "Journey"]);
    fs::write(
        project.join("Cargo.toml"),
        "[package]\nname=\"claude-host-journey-fixture\"\nversion=\"0.0.0\"\nedition=\"2024\"\n",
    )
    .expect("fixture manifest");
    fs::write(
        project.join("src/lib.rs"),
        "/// Quicksilver transport probe.\npub fn quicksilver_probe() -> u8 { 7 }\n",
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
            panic!("daemon exited before publishing authority: {status}; stderr: {stderr}");
        }
        if let Ok(bytes) = fs::read(path)
            && let Ok(record) = serde_json::from_slice::<DaemonAuthorityRecord>(&bytes)
            && record.pid == daemon.id()
            && record.auth_token.len() == 64
        {
            // Authority JSON survives shutdown and is published before bind.
            // Require this child and its listener; the shipped hook and tool
            // calls that follow prove authenticated route publication.
            if runtime.block_on(async {
                matches!(
                    tokio::time::timeout_at(
                        tokio::time::Instant::from_std(deadline),
                        BrokerStream::connect(&record.endpoint),
                    )
                    .await,
                    Ok(Ok(_))
                )
            }) {
                return;
            }
        }
        std::thread::sleep(Duration::from_millis(50));
    }
    panic!(
        "timed out waiting for the daemon to publish its authority at {}",
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

/// First file with this name anywhere under `root`, so the journal is found
/// through the profile's own store layout instead of a guessed path.
fn find_file(root: &Path, name: &str) -> Option<PathBuf> {
    let mut stack = vec![root.to_path_buf()];
    while let Some(directory) = stack.pop() {
        let Ok(entries) = fs::read_dir(&directory) else {
            continue;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                stack.push(path);
            } else if path.file_name().is_some_and(|found| found == name) {
                return Some(path);
            }
        }
    }
    None
}

/// Joins semantic `content[*].text` blocks of a compatibility MCP result, or
/// `None` when the value is not one. Transport accounting may be appended to
/// the same text block and is not part of the tool's JSON payload.
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

/// Every string value stored under `field`, anywhere in the document. Typed
/// application-surface envelopes nest their payload differently per operation,
/// and this journey only needs the daemon's own reported value, not the shape
/// it happened to be wrapped in.
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

/// The advisory provider-memory lane of one `tracedecay_context` answer, or
/// `None` when the answer carries no lane at all.
fn advisory_lane(answer: &Value) -> Option<Value> {
    answer
        .get("advisory_provider_memory")
        .filter(|value| !value.is_null())
        .cloned()
}

/// Reconstruct only bounded counter fields from the dedicated host marker.
fn recall_diagnostic_summary(line: &str) -> Option<Value> {
    let encoded = line.strip_prefix("[tracedecay] event=host_history_recall_test_diagnostic ")?;
    if encoded.len() > 4096 {
        return None;
    }
    let value: Value = serde_json::from_str(encoded).ok()?;
    let phase = value["phase"].as_str()?;
    let (numbers, flags, maps): (&[&str], &[&str], &[&str]) = match phase {
        "admission" => (
            &["received", "admitted", "denied"],
            &["degraded"],
            &["denial_reasons"],
        ),
        "trace" => (
            &["received", "items"],
            &["degraded"],
            &["stage_counts", "reason_counts"],
        ),
        "history_selection" => (
            &["scanned", "withheld", "unknown_revision"],
            &["has_older", "has_more", "partial_coverage"],
            &[],
        ),
        _ => return None,
    };
    let mut summary = serde_json::Map::new();
    summary.insert("phase".into(), json!(phase));
    for field in numbers {
        summary.insert((*field).into(), json!(value[*field].as_u64()?));
    }
    for field in flags {
        summary.insert((*field).into(), json!(value[*field].as_bool()?));
    }
    for field in maps {
        let counters = value[*field].as_object()?;
        if counters.len() > 32 {
            return None;
        }
        let mut bounded = serde_json::Map::new();
        for (label, count) in counters {
            if label.is_empty()
                || label.len() > 64
                || !label
                    .bytes()
                    .all(|byte| byte.is_ascii_lowercase() || byte == b'_')
            {
                return None;
            }
            bounded.insert(label.clone(), json!(count.as_u64()?));
        }
        summary.insert((*field).into(), Value::Object(bounded));
    }
    Some(Value::Object(summary))
}

/// The pinned history policy allows useful Partial/Stale evidence. Keep the
/// actual returned label; an accepted degraded answer is not a complete answer.
fn assert_history_degradation(lane: &Value) {
    let degradation = lane
        .get("degradation")
        .expect("an answered lane must retain its actual degradation metadata");
    assert!(
        degradation.is_null() || matches!(degradation.as_str(), Some("partial" | "stale")),
        "the history journey only permits its pinned Partial/Stale degradations: {lane}"
    );
}

fn journey_task() -> String {
    format!("how does the {JOURNEY_TERM} transport probe decide its retry budget?")
}

// ---------------------------------------------------------------------------
// The journey
// ---------------------------------------------------------------------------

/// The shipped SessionStart establishes a live baseline before source messages
/// are written. The first Stop commits the first turn; a later Stop commits the
/// second. Each Stop is replayed to prove delivery idempotency, and ordinary
/// `tracedecay_context` calls must recall the complete messages.
///
/// The unchanged-journal windows before capture distinguish these live hook
/// deliveries from startup import. After a daemon restart, a different session
/// runs its own SessionStart before recalling the same original evidence.
#[test]
fn the_shipped_claude_session_start_and_stop_hooks_commit_observations_and_a_later_context_call_carries_the_advisory_lane()
 {
    assert_host_memory_journey(false);
}

/// Two native Codex Stop events, including a replay of each, must commit the
/// rollout through the real daemon/provider and supply later context. The
/// shared assertions retain the no-importer control, exact delivery identities,
/// provenance deduplication, and whole-message tail check for both hosts.
#[test]
fn the_shipped_codex_stop_hook_commits_observations_and_later_context_recalls_them() {
    assert_host_memory_journey(true);
}

/// Opt-in production mount coverage: actual worker plus pinned offline model,
/// configured through the shipped CLI. NCM receives evidence as an observer;
/// the full existing journey still requires Native to supply later context.
#[cfg(unix)]
#[test]
#[ignore = "requires TRACEDECAY_NCM_WORKER and TRACEDECAY_NCM_REAL_MODEL_ROOT with pinned offline model"]
fn real_ncm_observer_receives_shipped_claude_hooks_while_native_answers_context() {
    assert_host_memory_journey_with_observer(false, true);
}

#[cfg(unix)]
#[test]
#[ignore = "requires TRACEDECAY_NCM_WORKER and TRACEDECAY_NCM_REAL_MODEL_ROOT with pinned offline model"]
fn real_ncm_observer_receives_shipped_codex_hooks_while_native_answers_context() {
    assert_host_memory_journey_with_observer(true, true);
}

/// NCM must independently answer the same real host journey with Native
/// advisory disabled; the canonical history and host admission remain shared.
#[cfg(unix)]
#[test]
#[ignore = "requires TRACEDECAY_NCM_WORKER and TRACEDECAY_NCM_REAL_MODEL_ROOT with pinned offline model"]
fn real_ncm_active_recalls_shipped_claude_session_history_with_native_disabled() {
    assert_host_memory_journey_with_provider(false, ActiveProvider::RustNcm, false);
}

#[cfg(unix)]
#[test]
#[ignore = "requires TRACEDECAY_NCM_WORKER and TRACEDECAY_NCM_REAL_MODEL_ROOT with pinned offline model"]
fn real_ncm_active_recalls_shipped_codex_session_history_with_native_disabled() {
    assert_host_memory_journey_with_provider(true, ActiveProvider::RustNcm, false);
}

fn assert_host_memory_journey(codex: bool) {
    assert_host_memory_journey_with_provider(codex, ActiveProvider::Native, false);
}

fn assert_host_memory_journey_with_observer(codex: bool, ncm_observer: bool) {
    assert_host_memory_journey_with_provider(codex, ActiveProvider::Native, ncm_observer);
}

fn assert_host_memory_journey_with_provider(
    codex: bool,
    active_provider: ActiveProvider,
    ncm_observer: bool,
) {
    let mut journey = ClaudeHostJourney::start(codex, active_provider, ncm_observer);

    // 1. The project is mounted and the provider host is live *before* the
    //    transcript exists. Baseline context forces project open; the explicit
    //    status barrier below then waits for startup import and projection.
    let task = journey_task();
    let baseline = journey.tool(
        "tracedecay_context",
        &json!({ "task": task, "format": "json" }),
    );
    let baseline_lane = advisory_lane(&baseline).unwrap_or_else(|| {
        panic!("the selected provider must mount the advisory lane: {baseline}")
    });
    assert_eq!(
        baseline_lane["provider_id"],
        active_provider.id(),
        "the lane must name the provider this project's routing policy pinned: {baseline_lane}"
    );
    let _journal = journey
        .journal_path()
        .expect("an enabled composition must mount the durable observation journal");
    assert!(
        journey.journal_rows().is_empty(),
        "the journey starts from an empty journal, so the hook's effect is unambiguous: {:?}",
        journal_digest(&journey.journal_rows())
    );

    if ncm_observer {
        assert!(
            journey.journal_for(true).is_some(),
            "configured observer must mount beside Native"
        );
        assert!(
            journey.journal_rows_for(true).is_empty(),
            "observer starts with no messages"
        );
    }

    journey.await_startup_history();

    // 2. Establish the live baseline before either host writes source messages.
    journey.initialize_session_transcript(journey.session_id());
    let started = journey.run_session_start_event(journey.session_id(), &journey.transcript_path());
    assert!(
        started.status.success(),
        "SessionStart must establish the live source baseline: {}",
        String::from_utf8_lossy(&started.stderr)
    );

    // The first turn is new live evidence; the existing negative control
    // proves its deliveries wait for the first Stop hook.
    journey.write_claude_transcript();
    journey.assert_journal_unchanged_without_a_hook(&[]);

    // 3. The shipped Stop hook captures the first completed live turn.
    let hook = journey.run_first_stop_hook();
    assert!(
        hook.status.success(),
        "the shipped first Stop hook must succeed\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&hook.stdout),
        String::from_utf8_lossy(&hook.stderr)
    );

    // 4. That invocation is what put exactly this session's first turn in the
    //    journal: two distinct committed messages, one exact coding scope,
    //    acknowledged on their first and only attempt, content still present.
    let rows = journey.await_settled_journal(ROWS_PER_TURN);
    assert_settled_session_messages(&rows, ROWS_PER_TURN);
    let observer_rows = journey.assert_observer_settled(&rows, &[]);

    // 5. Claude re-runs its own hooks; the same invocation must not duplicate
    //    the observation, because the idempotency key is content-derived. The
    //    comparison is by row identity, not by row count.
    let settled = journal_row_identities(&rows);
    let replay = journey.run_first_stop_hook();
    assert!(
        replay.status.success(),
        "a replayed hook must still succeed"
    );
    let replayed = journey.await_settled_journal(ROWS_PER_TURN);
    assert_eq!(
        journal_row_identities(&replayed),
        settled,
        "replaying one hook invocation must reproduce exactly the same deliveries, by \
         observation identity, payload digest and attempt count: {:?}",
        journal_digest(&replayed)
    );

    let observer_replayed = journey.assert_observer_settled(&replayed, &observer_rows);

    // 6. The session keeps running and writes a second turn. Claude must wait
    //    for its next hook. Codex Stop acknowledges retained daemon work, so
    //    the first Stop's follow-up may already ingest the appended bytes.
    journey.append_mid_session_claude_turn();
    if codex {
        let deadline = Instant::now() + QUIESCENCE_WINDOW;
        while Instant::now() < deadline {
            let progressed = journey.journal_rows();
            assert!(
                (ROWS_PER_TURN..=2 * ROWS_PER_TURN).contains(&progressed.len()),
                "Codex follow-up may only add the second turn: {:?}",
                journal_digest(&progressed)
            );
            let identities = journal_row_identities(&progressed);
            assert!(
                settled.iter().all(|identity| identities.contains(identity)),
                "Codex follow-up must retain every settled first-turn identity"
            );
            std::thread::sleep(JOURNAL_POLL_INTERVAL);
        }
    } else {
        journey.assert_journal_unchanged_without_a_hook(&replayed);
    }

    // 7. The next Stop must converge to exactly both turns, whether Codex's
    //    retained follow-up already captured the second one or not.
    let stop = journey.run_stop_hook();
    assert!(
        stop.status.success(),
        "the shipped Claude Stop hook must succeed\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&stop.stdout),
        String::from_utf8_lossy(&stop.stderr)
    );
    let mid_session = journey.await_settled_journal(2 * ROWS_PER_TURN);
    assert_settled_session_messages(&mid_session, 2 * ROWS_PER_TURN);
    let observer_mid_session = journey.assert_observer_settled(&mid_session, &observer_replayed);

    // 8. The Stop hook is idempotent in exactly the same way.
    let mid_session_settled = journal_row_identities(&mid_session);
    let stop_replay = journey.run_stop_hook();
    assert!(
        stop_replay.status.success(),
        "a replayed Stop hook must still succeed"
    );
    let mid_session_replayed = journey.await_settled_journal(2 * ROWS_PER_TURN);
    assert_eq!(
        journal_row_identities(&mid_session_replayed),
        mid_session_settled,
        "replaying the Stop hook must reproduce exactly the same deliveries: {:?}",
        journal_digest(&mid_session_replayed)
    );

    let observer_final =
        journey.assert_observer_settled(&mid_session_replayed, &observer_mid_session);
    if ncm_observer {
        // Let retained hook work finish across the existing bounded settling
        // window, then prove neither journal or receipt count grew on replay.
        journey.assert_journal_unchanged_without_a_hook(&mid_session_replayed);
        journey.assert_observer_settled(&journey.journal_rows(), &observer_final);
    }

    // Capture canonical source identities and the actual A scope before any
    // recall can add provider-addressed history deliveries to this journal.
    let original_sources = capture_original_hook_sources(&journey, &mid_session_replayed);

    // 9. The originating session can recall exactly what its hooks observed.
    let original =
        assert_recalled_session_messages(&journey, journey.session_id(), &original_sources);

    // 10. A fresh daemon and a different agent session must recall those same
    //     durable messages. B starts through the shipped lifecycle with an
    //     empty transcript: route binding must add no canonical messages.
    journey.stop_daemon();
    journey.start_daemon();
    let next_session = if journey.codex {
        "5ab47634-f1b2-4ccd-a1c3-2b0c2a9a3e10"
    } else {
        "b93546e4-5ca5-4e9a-a94c-d4867f679ee2"
    };
    let next_transcript = journey.transcript_path_for_session(next_session);
    journey.initialize_session_transcript(next_session);
    let started = journey.run_session_start_event(next_session, &next_transcript);
    assert!(
        started.status.success(),
        "next SessionStart must publish its route: {}",
        String::from_utf8_lossy(&started.stderr)
    );
    let after_start = journey.await_settled_journal(2 * ROWS_PER_TURN);
    assert_eq!(
        journal_row_identities(&after_start),
        mid_session_settled,
        "starting an empty session must preserve the original deliveries"
    );
    assert_settled_session_messages(&after_start, 2 * ROWS_PER_TURN);
    let selected_journal = journey
        .journal_for(active_provider.is_ncm())
        .expect("selected provider journal after bootstrap");
    for row in &after_start {
        assert_eq!(
            selected_journal
                .receipts_for(&row.observation_id)
                .expect("selected provider receipts")
                .len(),
            1,
            "empty SessionStart must not add another delivery receipt"
        );
    }
    journey.assert_observer_settled(&after_start, &observer_final);
    let recalled = assert_recalled_session_messages(&journey, next_session, &original_sources);
    assert_eq!(
        recalled.0, original.0,
        "restart and session change must preserve the same four messages and their provenance"
    );
    if active_provider == ActiveProvider::Native && !ncm_observer {
        assert_canonical_fact_feedback_journey(&mut journey, next_session);
    }
    record_demo_output(&journey, next_session, &original.1, &recalled.1);
}

/// Optional fixture artifacts are emitted only after the complete journey passes.
fn record_demo_output(
    journey: &ClaudeHostJourney,
    next_session: &str,
    source_result: &[u8],
    destination_result: &[u8],
) {
    let Some(output) = std::env::var_os("TRACEDECAY_DEMO_OUTPUT_DIR") else {
        return;
    };
    if journey.ncm_observer {
        return;
    }
    let case = match (journey.codex, journey.active_provider) {
        (false, ActiveProvider::Native) => "claude-native",
        (true, ActiveProvider::Native) => "codex-native",
        (false, ActiveProvider::RustNcm) => "claude-ncm-active",
        (true, ActiveProvider::RustNcm) => "codex-ncm-active",
    };
    let output = PathBuf::from(output);
    let allowed = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .canonicalize()
        .expect("checkout root")
        .join("target/task-scratch");
    assert!(
        output.is_absolute(),
        "demo output directory must be absolute"
    );
    let relative = output
        .strip_prefix(&allowed)
        .expect("demo output must be under target/task-scratch");
    assert!(
        relative
            .components()
            .all(|part| matches!(part, std::path::Component::Normal(_))),
        "demo output path must not contain parent components"
    );
    for ancestor in output.ancestors() {
        assert!(
            !fs::symlink_metadata(ancestor)
                .expect("pre-created demo output directory")
                .file_type()
                .is_symlink(),
            "demo output path must not traverse symlinks"
        );
    }
    assert!(
        output
            .canonicalize()
            .expect("demo output directory")
            .starts_with(allowed.canonicalize().expect("task-scratch directory")),
        "demo output must remain inside target/task-scratch"
    );
    let artifacts = [
        ("source-a.tool-result.json", source_result),
        ("destination-b.tool-result.json", destination_result),
    ];
    for (_, bytes) in artifacts {
        assert!(
            bytes.len() <= 8 * 1024 * 1024,
            "demo artifact exceeds 8 MiB"
        );
        serde_json::from_slice::<Value>(bytes).expect("demo artifact retains actual JSON");
    }
    let directory = output.join(case);
    fs::create_dir(&directory).expect("fresh demo case directory; refusing overwrite");
    for (name, bytes) in artifacts {
        let mut file = fs::File::create_new(directory.join(name)).expect("fresh demo artifact");
        file.write_all(bytes).expect("write actual response bytes");
        file.sync_all().expect("sync actual response bytes");
    }
    let identity = json!({
        "fixture_case": case,
        "host": if journey.codex { "codex" } else { "claude" },
        "provider_id": journey.active_provider.id(),
        "ncm_observer": journey.ncm_observer,
        "source_session_id": journey.session_id(),
        "destination_session_id": next_session,
        "assertions_passed": true,
    });
    let mut file =
        fs::File::create_new(directory.join("identity.json")).expect("fresh demo identity");
    file.write_all(&serde_json::to_vec_pretty(&identity).expect("fixture identity JSON"))
        .expect("write fixture identity");
    file.sync_all().expect("sync fixture identity");
}

/// Canonical fact feedback has its own public host authority. This separate
/// journey does not attribute an outcome to any staged session observation.
fn assert_canonical_fact_feedback_journey(journey: &mut ClaudeHostJourney, session_id: &str) {
    let canonical_payload = |journey: &ClaudeHostJourney, name: &str, arguments: Value| {
        let response = journey.tool(name, &arguments);
        response
            .pointer("/outcome/value/payload")
            .cloned()
            .unwrap_or_else(|| {
                panic!("{name} omitted its canonical application payload: {response}")
            })
    };
    let term = if journey.codex {
        "amber garden codex feedback"
    } else {
        "amber garden claude feedback"
    };
    let content = format!("The {term} retry budget is three seconds.");
    let created = canonical_payload(
        journey,
        "tracedecay_fact_store_add",
        json!({ "content": content, "category": "decision", "trust": 0.5, "format": "json" }),
    );
    assert_eq!(created["outcome"], "committed");
    assert_eq!(created["result"]["disposition"], "added");
    assert_eq!(created["result"]["fact"]["kind"], "available");
    let fact_id = created["result"]["fact"]["fact"]["fact_id"]
        .as_str()
        .expect("created canonical fact identity")
        .to_owned();
    assert_eq!(created["result"]["commit"]["fact_id"], fact_id);

    let recalled_provenance = |journey: &ClaudeHostJourney| {
        let answer = journey.tool(
            "tracedecay_context",
            &json!({
                "task": term,
                "format": "json",
                "_meta": { "session_id": session_id },
            }),
        );
        let lane = advisory_lane(&answer).expect("Native canonical fact recall lane");
        assert_eq!(lane["state"], "answered", "{lane}");
        assert_eq!(lane["provider_id"], CONFIGURED_PROVIDER_ID);
        assert_history_degradation(&lane);
        let expected = format!("cited source record:{fact_id}");
        let selected = lane["candidates"]
            .as_array()
            .expect("canonical recall candidates")
            .iter()
            .filter(|candidate| candidate["provenance"] == expected)
            .collect::<Vec<_>>();
        assert_eq!(
            selected.len(),
            1,
            "the host must confirm this exact fact once: {lane}; bounded Native diagnostics: {}",
            journey.native_recall_failure_diagnostics()
        );
        assert!(
            selected[0]["content"]
                .as_str()
                .is_some_and(|text| text.contains(&content)),
            "the selected canonical record must carry its created content: {lane}"
        );
        selected[0]["provenance"].clone()
    };
    let selected_provenance = recalled_provenance(journey);
    let selected_fact_id = selected_provenance
        .as_str()
        .and_then(|source| source.strip_prefix("cited source record:"))
        .expect("host-confirmed canonical fact identity");
    let before = canonical_payload(
        journey,
        "tracedecay_fact_store_get",
        json!({ "fact_id": selected_fact_id, "format": "json" }),
    );
    assert_eq!(before["fact"]["kind"], "available");
    assert_eq!(before["fact"]["fact"]["fact_id"], fact_id);
    let previous_event = before["fact"]["fact"]["last_event_id"]
        .as_str()
        .expect("current canonical event");
    let old_trust = before["fact"]["fact"]["trust_score_millionths"]
        .as_u64()
        .expect("current canonical trust");
    let source_label = format!("native-canonical-feedback-{session_id}");
    let reason = "The recalled canonical fact supplied the retry budget.";
    let feedback = canonical_payload(
        journey,
        "tracedecay_fact_feedback",
        json!({
            "fact_id": selected_fact_id,
            "expected_last_event_id": previous_event,
            "action": "helpful",
            "source_label": source_label,
            "reason": reason,
            "format": "json",
        }),
    );
    assert_eq!(feedback["feedback"]["fact_id"], fact_id);
    assert_eq!(feedback["feedback"]["action"], "helpful");
    assert_eq!(feedback["feedback"]["old_trust_millionths"], old_trust);
    let new_trust = feedback["feedback"]["new_trust_millionths"]
        .as_u64()
        .expect("feedback trust");
    assert!(
        new_trust > old_trust,
        "helpful feedback must increase trust: {feedback}"
    );
    assert_eq!(feedback["feedback"]["helpful_count"], 1);
    assert_eq!(feedback["feedback"]["unhelpful_count"], 0);
    let feedback_event = feedback["feedback"]["event_id"]
        .as_str()
        .expect("canonical feedback event");
    assert_ne!(feedback_event, previous_event);
    assert_eq!(feedback["fact"]["kind"], "available");
    assert_eq!(feedback["fact"]["fact"]["fact_id"], fact_id);
    assert_eq!(feedback["fact"]["fact"]["last_event_id"], feedback_event);
    assert_eq!(
        feedback["fact"]["fact"]["trust_score_millionths"],
        new_trust
    );
    assert_eq!(feedback["commit"]["disposition"], "committed");
    assert_eq!(
        feedback["commit"]["owner"],
        created["result"]["commit"]["owner"]
    );
    assert_eq!(feedback["commit"]["fact_id"], fact_id);
    assert_eq!(feedback["commit"]["last_event_id"], feedback_event);
    assert!(
        feedback["commit"]["committed_event_ids"]
            .as_array()
            .expect("committed feedback events")
            .contains(&json!(feedback_event))
    );

    // Restart only to prove the feedback event and its attribution are durable.
    journey.stop_daemon();
    journey.start_daemon();
    // The route is process-local; the shipped hook republishes the same session.
    let transcript = journey.transcript_path_for_session(session_id);
    let started = journey.run_session_start_event(session_id, &transcript);
    assert!(
        started.status.success(),
        "SessionStart must republish the canonical feedback session after restart: {}",
        String::from_utf8_lossy(&started.stderr)
    );
    let persisted = canonical_payload(
        journey,
        "tracedecay_fact_store_get",
        json!({ "fact_id": selected_fact_id, "format": "json" }),
    );
    assert_eq!(persisted["fact"]["kind"], "available");
    assert_eq!(persisted["fact"]["fact"]["fact_id"], fact_id);
    assert_eq!(persisted["fact"]["fact"]["last_event_id"], feedback_event);
    assert_eq!(
        persisted["fact"]["fact"]["trust_score_millionths"],
        new_trust
    );
    let history = persisted["trust_history"]
        .as_array()
        .expect("persisted trust history");
    assert_eq!(
        history.len(),
        1,
        "one attributed feedback event must survive restart"
    );
    assert_eq!(history[0]["event_id"], feedback_event);
    assert_eq!(history[0]["action"], "helpful");
    assert_eq!(history[0]["source_label"], source_label);
    assert_eq!(history[0]["reason"], reason);
    assert_eq!(history[0]["old_trust_millionths"], old_trust);
    assert_eq!(history[0]["new_trust_millionths"], new_trust);
    assert_eq!(recalled_provenance(journey), selected_provenance);
}

/// Exact scope read from an admitted host envelope; session encoding belongs
/// to the host and is never reconstructed from a canonical session string.
fn admitted_scope(admitted: &AdmittedObservationV1) -> Value {
    let scope = &admitted.exact_scope;
    json!({
        "profile_id": scope.profile_id, "project_id": scope.project_id,
        "repository_identity": scope.repository_identity, "worktree_identity": scope.worktree_identity,
        "branch_identity": scope.branch_identity, "agent_session_id": scope.agent_session_id,
        "resolved_scope_digest": scope.resolved_scope_digest,
    })
}

fn capture_original_hook_sources(
    journey: &ClaudeHostJourney,
    rows: &[JournalInspectionRowV1],
) -> BTreeMap<String, Value> {
    let journal = journey
        .journal_for(journey.active_provider.is_ncm())
        .expect("selected host journal");
    let mut originals = BTreeMap::new();
    for row in rows {
        assert_eq!(row.source_stream, "session_observation_store");
        let admitted = journal
            .read_admitted_observation_by_idempotency(&row.idempotency_key)
            .expect("read original admitted hook evidence")
            .expect("original content retained");
        let payload: Value =
            serde_json::from_slice(&admitted.payload.bytes).expect("canonical admitted payload");
        let canonical: tracedecay_domain::observation::CanonicalObservationEnvelopeV1 =
            serde_json::from_value(payload["canonical_payload"].clone())
                .expect("canonical source envelope");
        canonical
            .validate()
            .expect("valid canonical source envelope");
        assert_eq!(
            canonical.provider().as_str(),
            if journey.codex { "codex" } else { "claude" }
        );
        assert_eq!(
            canonical.relations().session_id().as_str(),
            journey.session_id()
        );
        assert!(
            payload.get("history_grant").is_none(),
            "capture must precede provider history replay"
        );
        let captured = json!({
            "canonical_provider_id": canonical.provider().as_str(),
            "canonical_session_id": canonical.relations().session_id().as_str(),
            "stable_record_id": canonical.stable_record_id().as_str(),
            "source_revision": canonical.evidence().revision(),
            "source_sequence": admitted.source.source_sequence.0,
            "original_scope": admitted_scope(&admitted),
        });
        assert!(
            originals
                .insert(admitted.source.source_event_id.clone(), captured)
                .is_none(),
            "hook source identities are unique"
        );
    }
    assert_eq!(originals.len(), 2 * ROWS_PER_TURN);
    originals
}

/// Only explicitly identified history rows are excluded from hook counts.
/// The complete source and receipt assertions run immediately after recall.
fn assert_history_stream(row: &JournalInspectionRowV1, admitted: &AdmittedObservationV1) -> Value {
    admitted
        .validate()
        .expect("valid retained history admission");
    assert_eq!(admitted.observation_id, row.observation_id);
    assert_eq!(admitted.idempotency_key, row.idempotency_key);
    assert_eq!(admitted.payload.sha256, row.payload_sha256);
    assert_eq!(admitted.extensions_digest, row.extensions_digest);
    let payload: Value =
        serde_json::from_slice(&admitted.payload.bytes).expect("retained history payload");
    let grant = &payload["history_grant"];
    let revision = grant["policy_revision"]
        .as_u64()
        .filter(|value| *value > 0)
        .expect("history policy revision");
    assert_eq!(
        row.source_stream,
        format!(
            "host-history.{revision}.{}",
            admitted.exact_scope.exact_scope_sha256()
        ),
        "no unrelated source stream may be hidden by hook counting"
    );
    assert_eq!(grant["destination_scope"], admitted_scope(admitted));
    assert!(
        payload
            .pointer("/source_identity/original_source")
            .is_some(),
        "history row must retain original evidence"
    );
    payload
}

fn assert_original_source(source: &Value, captured: &Value) {
    for field in [
        "canonical_provider_id",
        "canonical_session_id",
        "source_revision",
    ] {
        assert_eq!(
            source["source"][field], captured[field],
            "original {field} must match pre-recall A evidence"
        );
    }
    if !source["source"]["stable_record_id"].is_null() {
        assert_eq!(
            source["source"]["stable_record_id"],
            captured["stable_record_id"]
        );
    }
    assert_eq!(source["source_sequence"], captured["source_sequence"]);
    assert_eq!(source["origin_scope"]["state"], "recorded");
    assert_eq!(
        source["origin_scope"]["exact_scope_identity"], captured["original_scope"],
        "delivery in B must retain the actual host-authorized A scope"
    );
    assert!(
        source["origin_scope"]["authority_ref"]
            .as_str()
            .is_some_and(|value| !value.is_empty())
    );
}

fn assert_recalled_history_deliveries(
    journey: &ClaudeHostJourney,
    originals: &BTreeMap<String, Value>,
    previous: &BTreeSet<String>,
    origin_session: bool,
) -> BTreeMap<String, Value> {
    let journal = journey
        .journal_for(journey.active_provider.is_ncm())
        .expect("selected host journal");
    let original_scope = &originals.values().next().expect("captured A sources")["original_scope"];
    let mut delivered = BTreeMap::new();
    let mut current_scope = None;
    for row in journey.all_journal_rows_for(journey.active_provider.is_ncm()) {
        if row.source_stream == "session_observation_store" {
            continue;
        }
        let admitted = journal
            .read_admitted_observation_by_idempotency(&row.idempotency_key)
            .expect("read history admission")
            .expect("retained history evidence");
        let payload = assert_history_stream(&row, &admitted);
        let source = &payload["source_identity"]["original_source"];
        let id = source["source"]["observation_id"]
            .as_str()
            .expect("canonical source identity");
        assert_original_source(
            source,
            originals.get(id).expect("no unrelated canonical sources"),
        );
        let grant_sources = payload["history_grant"]["sources"]
            .as_array()
            .expect("granted sources");
        assert_eq!(grant_sources.len(), 1);
        assert_eq!(grant_sources[0]["attribution"], *source);
        assert_eq!(row.provider_id, journey.active_provider.id());
        let receipts = journal
            .receipts_for(&row.observation_id)
            .expect("history receipts");
        let current: Vec<_> = receipts
            .iter()
            .filter(|receipt| receipt.attempt_number == row.attempt_number)
            .collect();
        assert_eq!(
            current.len(),
            1,
            "history must have one validated current-attempt receipt"
        );
        let receipt = current[0];
        receipt.validate().expect("valid history receipt identity");
        assert_eq!(receipt.provider_id.as_str(), journey.active_provider.id());
        assert_eq!(receipt.observation_id, row.observation_id);
        assert_eq!(receipt.idempotency_key, row.idempotency_key);
        assert_eq!(receipt.payload_sha256, row.payload_sha256);
        assert_eq!(receipt.extensions_digest, row.extensions_digest);
        assert_eq!(receipt.registration_revision, row.registration_revision);
        assert_eq!(
            receipt.provider_instance_id.as_ref(),
            Some(&row.provider_instance_id)
        );
        assert_eq!(receipt.attempt_number, row.attempt_number);
        assert!(
            receipt
                .provider_receipt_digest
                .as_ref()
                .is_some_and(|digest| digest.len() == 64
                    && digest.bytes().all(|byte| byte.is_ascii_hexdigit())),
            "applied history must carry a real provider acknowledgement digest"
        );
        let applied = row.state == DeliveryStateV1::Acknowledged
            && receipt.outcome == ObservationOutcomeV1::Applied
            && receipt.committed_effect == ObservationCommittedEffectV1::Applied;
        let duplicate = row.state == DeliveryStateV1::DuplicateAcknowledged
            && receipt.outcome == ObservationOutcomeV1::DuplicateAcknowledged
            && receipt.committed_effect == ObservationCommittedEffectV1::Duplicate;
        assert!(
            applied || duplicate,
            "history requires a settled applied or duplicate effect"
        );
        let destination = admitted_scope(&admitted);
        if previous.contains(row.idempotency_key.as_str()) {
            assert!(!origin_session, "A begins without earlier history delivery");
            assert_eq!(
                &destination, original_scope,
                "only original A history may precede B recall"
            );
            continue;
        }
        for field in [
            "profile_id",
            "project_id",
            "repository_identity",
            "worktree_identity",
            "branch_identity",
            "resolved_scope_digest",
        ] {
            assert_eq!(
                destination[field], original_scope[field],
                "history must stay in the authorized checkout"
            );
        }
        if origin_session {
            assert_eq!(&destination, original_scope);
        } else {
            assert_ne!(
                destination["agent_session_id"], original_scope["agent_session_id"],
                "B gets a distinct delivery scope without relabeling A evidence"
            );
        }
        if let Some(expected) = &current_scope {
            assert_eq!(
                &destination, expected,
                "one requesting session owns this recall"
            );
        } else {
            current_scope = Some(destination);
        }
        assert!(
            delivered.insert(id.to_owned(), source.clone()).is_none(),
            "one history delivery per original source and destination"
        );
    }
    assert_eq!(
        delivered.keys().collect::<Vec<_>>(),
        originals.keys().collect::<Vec<_>>(),
        "the real selected provider must settle every original source for this session"
    );
    delivered
}

/// The existing complete recall assertions, shared by origin and next session.
/// Returns content/provenance identities; request-bound candidate IDs may differ.
fn assert_recalled_session_messages(
    journey: &ClaudeHostJourney,
    recalled_session_id: &str,
    original_sources: &BTreeMap<String, Value>,
) -> (Vec<(String, String)>, Vec<u8>) {
    let previous_history: BTreeSet<_> = journey
        .all_journal_rows_for(journey.active_provider.is_ncm())
        .into_iter()
        .filter(|row| row.source_stream != "session_observation_store")
        .map(|row| row.idempotency_key.as_str().to_owned())
        .collect();
    let (answer, result_bytes) = journey.tool_with_bytes(
        "tracedecay_context",
        &json!({
            "task": format!(
                "what did the {JOURNEY_TERM} retry budget change record, down to the \
                 {TAIL_SENTINEL} note?"
            ),
            "format": "json",
            "_meta": { "session_id": recalled_session_id },
        }),
    );
    let lane = advisory_lane(&answer)
        .unwrap_or_else(|| panic!("an active provider must contribute an advisory lane: {answer}"));
    assert_eq!(
        lane["state"], "answered",
        "the advisory lane must answer rather than report a refusal: {lane}"
    );
    assert_history_degradation(&lane);
    assert_eq!(
        lane["provider_id"],
        journey.active_provider.id(),
        "the lane must name the provider the routing policy pinned: {lane}"
    );
    let candidates = lane["candidates"].as_array().cloned().unwrap_or_default();
    assert_eq!(
        candidates.len(),
        2 * ROWS_PER_TURN,
        "the healthy journey must recall every admitted Claude message exactly once: {lane}; bounded Native diagnostics: {}",
        journey.native_recall_failure_diagnostics()
    );
    let retained_sources = assert_recalled_history_deliveries(
        journey,
        original_sources,
        &previous_history,
        recalled_session_id == journey.session_id(),
    );
    let mut recalled_originals = BTreeSet::new();
    for candidate in &candidates {
        let evidence = &candidate["provenance_evidence"];
        assert_eq!(
            evidence["kind"], "canonical_observations",
            "final candidates require host-confirmed original evidence"
        );
        let sources = evidence["sources"]
            .as_array()
            .expect("typed original sources");
        assert!(
            !sources.is_empty(),
            "admitted content must have original source evidence"
        );
        for source in sources {
            let id = source["source"]["observation_id"]
                .as_str()
                .expect("canonical observation id");
            let expected = retained_sources
                .get(id)
                .expect("candidate source was delivered from captured A history");
            assert_eq!(
                source, expected,
                "public evidence must preserve every retained attribution field"
            );
            assert_original_source(
                source,
                original_sources.get(id).expect("original hook source"),
            );
            recalled_originals.insert(id.to_owned());
        }
    }
    assert_eq!(
        recalled_originals,
        original_sources.keys().cloned().collect(),
        "recall must cover the actual original hook sources"
    );
    let mut provenance = candidates
        .iter()
        .map(|candidate| candidate["provenance"].to_string())
        .collect::<Vec<_>>();
    let total = provenance.len();
    provenance.sort();
    provenance.dedup();
    assert_eq!(
        provenance.len(),
        total,
        "recall candidates must be de-duplicated: {lane}"
    );
    assert!(
        candidates.iter().all(|candidate| {
            candidate["content"]
                .as_str()
                .is_some_and(|content| content.contains(JOURNEY_TERM))
        }),
        "every advisory candidate must be evidence this Claude session actually produced, \
         never fabricated context: {lane}"
    );
    // Whole evidence, not a prefix of it: the mid-session assistant message
    // opens with the journey term and ends with the sentinel, so a candidate
    // carrying both is one the lane did not truncate on its way back.
    assert!(
        candidates.iter().any(|candidate| {
            candidate["content"].as_str().is_some_and(|content| {
                content.contains(JOURNEY_TERM) && content.contains(TAIL_SENTINEL)
            })
        }),
        "the advisory lane must recall the mid-session message whole, tail included: {lane}"
    );

    let mut identities = candidates
        .iter()
        .map(|candidate| {
            (
                candidate["content"]
                    .as_str()
                    .expect("verified message content")
                    .to_owned(),
                json!({
                    "label": candidate["provenance"],
                    "evidence": candidate["provenance_evidence"],
                })
                .to_string(),
            )
        })
        .collect::<Vec<_>>();
    identities.sort();
    (identities, result_bytes)
}

#[test]
fn recall_failure_diagnostics_keep_only_bounded_counter_metadata() {
    let marker = "[tracedecay] event=host_history_recall_test_diagnostic ";
    let line = format!(
        "{marker}{}",
        json!({"phase":"admission", "received":4, "admitted":0,
        "denied":4, "degraded":true, "denial_reasons":{"unknown_validity":4},
        "request_id":"private-request", "content":"private-content"})
    );
    let summary = recall_diagnostic_summary(&line).expect("bounded host counters");
    assert_eq!(summary["received"], 4);
    assert_eq!(summary["denial_reasons"]["unknown_validity"], 4);
    assert!(!summary.to_string().contains("private"));
    assert!(recall_diagnostic_summary("unrelated private daemon line").is_none());
    assert!(recall_diagnostic_summary(&format!("{marker}{}", "x".repeat(4097))).is_none());
}
