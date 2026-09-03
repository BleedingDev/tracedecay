//! The Claude Code host memory journey, driven as real subprocesses against a
//! live daemon.
//!
//! Everything here is the shipped `tracedecay` binary: the daemon is
//! `tracedecay daemon run`, the operator gates are committed through
//! `tracedecay tool tracedecay_configuration_set`, the hook is the process
//! Claude Code itself spawns (`tracedecay hook-claude-session-start`, native
//! payload on stdin), and the later agent question is
//! `tracedecay tool tracedecay_context`. Nothing in this file constructs a
//! hook envelope, seals a binding, or runs an administrative import; the only
//! thing that touches the daemon between "no journal rows" and "a settled
//! journal row" is the hook process.
//!
//! That ordering is the point. The daemon runs a transcript import when a
//! project mounts, so a journey that writes the transcript *before* the daemon
//! comes up cannot tell a working hook from a broken one — startup would have
//! ingested the same rows. Here the daemon is already up and has already
//! mounted the project (the baseline `tracedecay_context` call below forces
//! that) when the transcript is written, and the journal is proved empty and
//! *stays* empty across a bounded settling window before the hook runs.
//!
//! # What this proves about the product
//!
//! * an operator can reach the provider host through the shipped CLI: the
//!   `memory-provider-host` feature is a real CLI feature, so the binary under
//!   test contains the mount (`required-features` on this target);
//! * a Claude Code lifecycle hook invocation commits that session's messages as
//!   canonical project observations, which the mounted observation journey then
//!   settles against the routed provider exactly once;
//! * a later ordinary `tracedecay_context` call carries the advisory
//!   provider-memory lane, bounded and de-duplicated, naming the provider the
//!   project's own routing policy pinned.

use std::fs;
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Output, Stdio};
use std::time::{Duration, Instant};

use serde_json::{Value, json};
use tempfile::TempDir;
use tracedecay_domain::configuration::{
    MEMORY_PROVIDER_NATIVE_ENABLED_SETTING_KEY, MEMORY_PROVIDER_RECALL_ROUTING_SETTING_KEY,
};

/// The Claude Code session id the whole journey is bound to.
const CLAUDE_SESSION: &str = "claude-cli-journey-session";

/// A term that appears in the transcript and in the later question, so a recall
/// that answers at all has something to answer with.
const JOURNEY_TERM: &str = "quicksilver";

/// The provider identity an operator writes into
/// `memory.provider_recall_routing.v1`. It is the configured, operator-facing
/// spelling of the Native adapter, and the advisory lane must echo exactly it.
const CONFIGURED_PROVIDER_ID: &str = "tracedecay.native";

/// File name of the durable observation journal the mounted journey owns.
const JOURNAL_FILE_NAME: &str = "memory-observation-journal-v1.sqlite3";

/// Observation kind the journey admits for host session messages.
const SESSION_MESSAGE_OBSERVATION_KIND: &str = "session.message_committed.v1";

/// How long the journey waits for the journal to settle after the hook. The
/// mounted live replay parks for 250ms between passes, so this is a
/// convergence bound with generous headroom, never a sleep: every wait below
/// returns as soon as its condition holds.
const SETTLEMENT_BUDGET: Duration = Duration::from_secs(60);

/// How long the journal is required to *stay* empty after the transcript is on
/// disk and before the hook runs. Comfortably longer than several live replay
/// passes, so a route that ingested the transcript without the hook is caught
/// here rather than being mistaken for the hook's own effect.
const QUIESCENCE_WINDOW: Duration = Duration::from_secs(5);

const USER_DATA_DIR_ENV: &str = "TRACEDECAY_DATA_DIR";
const GLOBAL_DB_ENV: &str = "TRACEDECAY_GLOBAL_DB";

/// One journal row, as `(observation_kind, exact_scope_sha256, delivery_state,
/// attempts)`.
type JournalRow = (String, String, String, i64);

// ---------------------------------------------------------------------------
// Isolated daemon fixture
// ---------------------------------------------------------------------------

struct ClaudeHostJourney {
    daemon: Option<Child>,
    home: TempDir,
    profile: PathBuf,
    project: PathBuf,
    bin_dir: PathBuf,
}

impl ClaudeHostJourney {
    /// A registered git project under a throwaway profile, with the daemon
    /// running and both memory-provider gates committed. Both settings are
    /// `DaemonRestart`, so the daemon is restarted before the journey begins:
    /// a composition that is already open keeps the mounts it opened with.
    fn start() -> Self {
        let home = TempDir::new().expect("isolated home");
        let root = home.path().to_path_buf();
        let profile = root.join(".tracedecay");
        let project = root.join("project");
        let bin_dir = root.join("bin");
        fs::create_dir_all(&profile).expect("profile root");
        fs::create_dir_all(&bin_dir).expect("bin dir");
        install_binary_shim(&bin_dir);
        initialize_project(&project);

        let mut journey = Self {
            daemon: None,
            home,
            profile,
            project,
            bin_dir,
        };
        journey.start_daemon();
        run_ok(
            journey.cli(&["init"]).current_dir(&journey.project),
            "tracedecay init",
        );
        let project_id = journey.project_id();
        journey.commit_provider_gates(&project_id);
        // Both gates are DaemonRestart settings: restart is what makes the
        // provider host mount.
        journey.stop_daemon();
        journey.start_daemon();
        journey
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
        let mut daemon = self
            .cli(&["daemon", "run"])
            .stdout(Stdio::null())
            .stderr(Stdio::piped())
            .spawn()
            .expect("daemon should start");
        wait_for_authority(&mut daemon, &daemon_authority_path(&self.profile));
        self.daemon = Some(daemon);
    }

    fn stop_daemon(&mut self) {
        if let Some(mut daemon) = self.daemon.take() {
            let _ = daemon.kill();
            let _ = daemon.wait();
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

    /// One MCP tool call through the shipped `tracedecay tool` surface, which
    /// dispatches over the daemon transport exactly like any other client.
    ///
    /// `--json` makes the CLI print the daemon's own result object verbatim.
    /// The process exits nonzero whenever the daemon marked the call failed,
    /// so [`run_ok`] is the refusal check and nothing here has to re-derive it.
    fn tool_result(&self, name: &str, arguments: &Value) -> Value {
        let project = self.project.to_string_lossy().to_string();
        let payload = arguments.to_string();
        let stdout = run_ok(
            &mut self.cli(&[
                "tool", "--project", &project, name, "--args", &payload, "--json",
            ]),
            &format!("tracedecay tool {name}"),
        );
        let text = String::from_utf8(stdout).expect("tool output is UTF-8");
        serde_json::from_str(&text)
            .unwrap_or_else(|error| panic!("tool {name} answered non-JSON `{text}`: {error}"))
    }

    /// The tool's own JSON payload: compatibility MCP results carry it inside
    /// `content[*].text`, and typed application-surface results are already the
    /// object itself.
    fn tool(&self, name: &str, arguments: &Value) -> Value {
        let result = self.tool_result(name, arguments);
        assert_ne!(
            result["isError"], true,
            "tool {name} reported an application failure: {result}"
        );
        let Some(text) = join_content_text(&result) else {
            return result;
        };
        serde_json::from_str(&text).unwrap_or_else(|error| {
            panic!("tool {name} produced non-JSON content text `{text}`: {error}")
        })
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
            json!({ "kind": "boolean", "value": true }),
            "configuration.idempotency.claude-cli-journey-host",
        );
        self.configuration_set(
            project_id,
            MEMORY_PROVIDER_RECALL_ROUTING_SETTING_KEY,
            json!({
                "kind": "text",
                "value": json!({ "active_provider": CONFIGURED_PROVIDER_ID }).to_string(),
            }),
            "configuration.idempotency.claude-cli-journey-routing",
        );
    }

    /// Runs the shipped Claude `SessionStart` hook process, handing it the
    /// bytes Claude Code itself writes on stdin.
    fn run_session_start_hook(&self) -> Output {
        let payload = json!({
            "session_id": CLAUDE_SESSION,
            "transcript_path": self.transcript_path().to_string_lossy(),
            "cwd": self.project.to_string_lossy(),
            "hook_event_name": "SessionStart",
            "source": "startup",
        })
        .to_string();
        let mut command = self.cli(&["hook-claude-session-start"]);
        command.stdin(Stdio::piped());
        let mut child = command.spawn().expect("Claude SessionStart hook spawns");
        child
            .stdin
            .take()
            .expect("hook stdin")
            .write_all(payload.as_bytes())
            .expect("hook payload delivery");
        child.wait_with_output().expect("hook completes")
    }

    fn transcript_path(&self) -> PathBuf {
        self.home
            .path()
            .join(".claude/projects/-claude-cli-journey")
            .join(format!("{CLAUDE_SESSION}.jsonl"))
    }

    /// Writes the transcript Claude Code itself writes. Every row carries the
    /// project `cwd`, which is what binds these frames to this project — the
    /// directory name never decides membership.
    fn write_claude_transcript(&self) {
        let path = self.transcript_path();
        fs::create_dir_all(path.parent().expect("transcript directory"))
            .expect("transcript directory");
        let cwd = self.project.to_string_lossy().to_string();
        let rows = [
            json!({
                "type": "user",
                "cwd": cwd,
                "sessionId": CLAUDE_SESSION,
                "uuid": "cli-journey-uuid-1",
                "timestamp": "2026-02-01T00:00:00.000Z",
                "message": {
                    "role": "user",
                    "content": format!(
                        "how does the {JOURNEY_TERM} transport probe decide its retry budget?"
                    ),
                },
            }),
            json!({
                "type": "assistant",
                "cwd": cwd,
                "sessionId": CLAUDE_SESSION,
                "uuid": "cli-journey-uuid-2",
                "parentUuid": "cli-journey-uuid-1",
                "timestamp": "2026-02-01T00:00:01.000Z",
                "message": {
                    "id": "msg_cli_journey_2",
                    "role": "assistant",
                    "model": "claude-opus-4-8",
                    "content": [{
                        "type": "text",
                        "text": format!(
                            "the {JOURNEY_TERM} transport probe reads its retry budget from the \
                             pinned deadline"
                        ),
                    }],
                },
            }),
        ];
        let contents = rows
            .iter()
            .map(Value::to_string)
            .collect::<Vec<_>>()
            .join("\n");
        fs::write(&path, format!("{contents}\n")).expect("write Claude transcript");
    }

    /// The durable observation journal the mounted journey owns, located under
    /// this profile's own store layout rather than guessed at.
    fn journal_path(&self) -> Option<PathBuf> {
        find_file(&self.profile, JOURNAL_FILE_NAME)
    }

    fn journal_rows(&self) -> Vec<JournalRow> {
        let Some(path) = self.journal_path() else {
            return Vec::new();
        };
        let Ok(connection) = rusqlite::Connection::open(&path) else {
            return Vec::new();
        };
        let Ok(mut statement) = connection.prepare(
            "SELECT observation_kind, exact_scope_sha256, delivery_state, attempts \
             FROM tdmem_observation_journal_v1 ORDER BY rowid",
        ) else {
            return Vec::new();
        };
        statement
            .query_map([], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, i64>(3)?,
                ))
            })
            .and_then(std::iter::Iterator::collect::<Result<Vec<_>, _>>)
            .unwrap_or_default()
    }

    /// Waits, bounded, until the journal holds at least one row and every row
    /// is terminal. Returns as soon as that holds.
    fn await_settled_journal(&self) -> Vec<JournalRow> {
        let deadline = Instant::now() + SETTLEMENT_BUDGET;
        loop {
            let rows = self.journal_rows();
            if !rows.is_empty()
                && rows
                    .iter()
                    .all(|(_, _, state, _)| matches!(state.as_str(), "acknowledged" | "rejected"))
            {
                return rows;
            }
            assert!(
                Instant::now() < deadline,
                "the hook's observation never settled in the journal; last saw {rows:?}"
            );
            std::thread::sleep(Duration::from_millis(100));
        }
    }
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
            && let Ok(record) = serde_json::from_slice::<Value>(&bytes)
            && record["auth_token"]
                .as_str()
                .is_some_and(|token| token.len() == 64)
        {
            return;
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

/// Joins every `content[*].text` block of a compatibility MCP result, or
/// `None` when the value is not one.
fn join_content_text(result: &Value) -> Option<String> {
    let blocks = result.get("content")?.as_array()?;
    let text = blocks
        .iter()
        .filter_map(|block| block.get("text").and_then(Value::as_str))
        .filter(|text| !text.is_empty())
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
                if key == field && let Some(text) = member.as_str() {
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

fn journey_task() -> String {
    format!("how does the {JOURNEY_TERM} transport probe decide its retry budget?")
}

// ---------------------------------------------------------------------------
// The journey
// ---------------------------------------------------------------------------

/// One real `tracedecay hook-claude-session-start` process is the only thing
/// that runs between an empty journal and a settled one, and a later ordinary
/// `tracedecay_context` call carries the advisory lane that observation made
/// possible.
///
/// Real defect this catches: a Claude host integration whose lifecycle hooks
/// reach the daemon but never commit the session's own evidence, so provider
/// memory only ever contains what a daemon restart happened to sweep up. Under
/// that defect the journal below stays empty after the hook and this test
/// fails at the settlement wait — while an administrative import, or a
/// transcript written before the daemon started, would have hidden it.
#[test]
fn the_shipped_claude_session_start_hook_commits_an_observation_and_a_later_context_call_carries_the_advisory_lane()
 {
    let journey = ClaudeHostJourney::start();

    // 1. The project is mounted and the provider host is live *before* the
    //    transcript exists. This baseline call is what forces project open, so
    //    the daemon's own startup import has already run and found nothing.
    let task = journey_task();
    let baseline = journey.tool(
        "tracedecay_context",
        &json!({ "task": task, "format": "json" }),
    );
    let baseline_lane = advisory_lane(&baseline).unwrap_or_else(|| {
        panic!("a composition with both gates committed must mount the advisory lane: {baseline}")
    });
    assert_eq!(
        baseline_lane["provider_id"], CONFIGURED_PROVIDER_ID,
        "the lane must name the provider this project's routing policy pinned: {baseline_lane}"
    );
    let journal = journey
        .journal_path()
        .expect("an enabled composition must mount the durable observation journal");
    assert!(
        journey.journal_rows().is_empty(),
        "the journey starts from an empty journal, so the hook's effect is unambiguous: {:?}",
        journey.journal_rows()
    );

    // 2. Claude Code writes its transcript. Nothing else happens: the journal
    //    must stay empty for a window several live-replay passes long, which is
    //    what makes step 3's transition attributable to the hook alone.
    journey.write_claude_transcript();
    let quiescence_deadline = Instant::now() + QUIESCENCE_WINDOW;
    while Instant::now() < quiescence_deadline {
        let rows = journey.journal_rows();
        assert!(
            rows.is_empty(),
            "a transcript on disk must not reach the journal on its own; the hook is what \
             commits it. Observed {rows:?} in {}",
            journal.display()
        );
        std::thread::sleep(Duration::from_millis(100));
    }

    // 3. The shipped hook process runs, exactly as Claude Code spawns it.
    let hook = journey.run_session_start_hook();
    assert!(
        hook.status.success(),
        "the shipped Claude SessionStart hook must succeed\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&hook.stdout),
        String::from_utf8_lossy(&hook.stderr)
    );

    // 4. That invocation is what put a settled row in the journal.
    let rows = journey.await_settled_journal();
    let mut scopes: Vec<&str> = rows.iter().map(|(_, scope, _, _)| scope.as_str()).collect();
    scopes.dedup();
    assert_eq!(
        scopes.len(),
        1,
        "every row this host session produced belongs to one exact coding scope: {rows:?}"
    );
    for (kind, _, state, attempts) in &rows {
        assert_eq!(
            kind, SESSION_MESSAGE_OBSERVATION_KIND,
            "the hook admits exactly the session-message observation kind: {rows:?}"
        );
        assert_eq!(
            state, "acknowledged",
            "the routed provider accepts session messages, so the row settles acknowledged: \
             {rows:?}"
        );
        assert_eq!(
            *attempts, 1,
            "an accepted observation is delivered once, never retried: {rows:?}"
        );
    }

    // 5. Claude re-runs its own hooks; the same invocation must not duplicate
    //    the observation, because the idempotency key is content-derived.
    let settled = rows.len();
    let replay = journey.run_session_start_hook();
    assert!(replay.status.success(), "a replayed hook must still succeed");
    let replayed = journey.await_settled_journal();
    assert_eq!(
        replayed.len(),
        settled,
        "replaying one hook invocation must not duplicate journal rows: {replayed:?}"
    );

    // 6. A later ordinary agent question carries the advisory lane, and the
    //    lane can now answer with what the hook observed.
    let answer = journey.tool(
        "tracedecay_context",
        &json!({
            "task": task,
            "format": "json",
            "_meta": { "session_id": CLAUDE_SESSION },
        }),
    );
    let lane = advisory_lane(&answer)
        .unwrap_or_else(|| panic!("an active provider must contribute an advisory lane: {answer}"));
    assert_eq!(
        lane["state"], "answered",
        "the advisory lane must answer rather than report a refusal: {lane}"
    );
    assert_eq!(
        lane["degradation"],
        Value::Null,
        "a healthy route reports no degradation: {lane}"
    );
    assert_eq!(
        lane["provider_id"], CONFIGURED_PROVIDER_ID,
        "the lane must name the provider the routing policy pinned: {lane}"
    );
    let candidates = lane["candidates"].as_array().cloned().unwrap_or_default();
    assert!(
        !candidates.is_empty(),
        "the observed Claude session must be recallable after the hook committed it: {lane}"
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
}
