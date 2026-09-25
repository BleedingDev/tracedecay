//! Live transcript boundaries attached to the existing hook admission ledger.
//! Ordinary session cursors locate content; only these live receipts establish
//! where content began under a continuously observed worktree identity.

use std::fs;
use std::io::{Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use tracedecay_contracts::ProfileIdentityReadPort;
use tracedecay_domain::{
    BrainId, CommitId, EvidenceAvailabilityV1, ObservationSourceIdentityV1, ProjectId, ProviderId,
    RefId, SessionId, UserProfileId, UtcMicros, framed_log::checksum,
};
use tracedecay_global_db::RegisteredGlobalDb;
use tracedecay_hooks::admission_ledger::{
    HookAdmissionLedgerError, HookLiveOriginBoundaryV1, HookLiveOriginBranchEvidenceV1,
    HookLiveOriginCheckpointV1, HookLiveOriginFrameV1, HookLiveOriginObservationV1,
    HookLiveOriginOutcomeV1, HookLiveOriginScopeV1, MAX_LIVE_ORIGIN_FRAMES,
};
use tracedecay_hooks::{HookAdmissionLedgerReceiptV1, HookEventEnvelopeV2, HookHostV1};
use tracedecay_sessions::repository_provenance::RepositoryProvenanceAdmissionContext;
use tracedecay_sessions::runtime::source::{
    JsonlResumeState, StoredCursor, capture_live_jsonl_origin,
};
use tracedecay_store::StoreShardScopeV1;

use super::admission::{live_hook_origin_baseline, retain_live_hook_origin};
use crate::tracedecay::TraceDecay;

const MAX_LIVE_SOURCE_BYTES: u64 = 16 * 1024 * 1024;
const MAX_REFLOG_TAIL_BYTES: u64 = 64 * 1024;

pub(super) struct LiveOriginAdmissionV1<'a> {
    pub profile_identity: Option<&'a dyn ProfileIdentityReadPort>,
    pub background_cpu:
        Option<&'a std::sync::Arc<tracedecay_runtime_core::background_cpu::ProcessBackgroundCpuV1>>,
    /// Captured at the live handler entry, before parsing and ledger admission.
    pub entered_at: Instant,
    /// Validated against the native envelope before daemon ID minting.
    pub native_start_locator: Option<tracedecay_agent_hosts::hooks::NativeSessionStartLocatorV1>,
}

#[derive(Debug)]
pub(super) enum LiveOriginOutcomeV1 {
    Recorded(HookLiveOriginOutcomeV1),
    Unavailable,
    Deadline,
    Ledger(HookAdmissionLedgerError),
}

struct LiveOriginSource {
    project_root: PathBuf,
    project_id: ProjectId,
    brain_id: BrainId,
    profile_id: UserProfileId,
    source_path: PathBuf,
    source: ObservationSourceIdentityV1,
}

struct LiveOriginSession {
    project_root: PathBuf,
    project_id: ProjectId,
    brain_id: BrainId,
    profile_id: UserProfileId,
    producer: HookHostV1,
    native_session: SessionId,
    recorded_project_path: Option<PathBuf>,
    source_path: Option<PathBuf>,
}

struct LiveSessionLocator {
    provider: String,
    session_id: String,
    project_path: String,
    transcript_path: String,
}

/// The public replay entry never supplies `live`; retries cannot observe a
/// newer checkout and promote old content into an original-event receipt.
#[hotpath::measure(future = true, label = "mcp.hook_runtime.live_origin")]
pub(super) async fn record_live_hook_origin(
    cg: &TraceDecay,
    envelope: &HookEventEnvelopeV2,
    native_session: Option<&SessionId>,
    sessions: Option<&RegisteredGlobalDb>,
    live: LiveOriginAdmissionV1<'_>,
    receipt: HookAdmissionLedgerReceiptV1,
    now: UtcMicros,
) -> LiveOriginOutcomeV1 {
    if receipt.decision != tracedecay_hooks::HookAdmissionDecisionV1::Admitted {
        return LiveOriginOutcomeV1::Unavailable;
    }
    let Some(deadline) = live.entered_at.checked_add(Duration::from_micros(
        tracedecay_hooks::runtime::HOOK_SYNCHRONOUS_BUDGET_MICROS,
    )) else {
        return LiveOriginOutcomeV1::Deadline;
    };
    let Some(remaining) = deadline.checked_duration_since(Instant::now()) else {
        return LiveOriginOutcomeV1::Deadline;
    };
    let data_root = cg.hook_store_layout().data_root.clone();
    let previous = live_hook_origin_baseline(&data_root, envelope, now);
    let is_start = matches!(
        envelope.event,
        tracedecay_hooks::HookEventV2::SessionBoundary {
            boundary: tracedecay_hooks::HookBoundaryV1::Start
        }
    );
    let capture = async {
        let session = retained_session(
            cg,
            envelope,
            native_session,
            sessions,
            live.profile_identity,
        )
        .await;
        let Some(permit) = live.background_cpu.and_then(|cpu| cpu.try_acquire()) else {
            return LiveOriginOutcomeV1::Unavailable;
        };
        let locator = live.native_start_locator;
        // Carry the caller's configured/task-scoped native home into blocking I/O.
        let home = tracedecay_sessions::runtime::home_dir();
        let captured = tokio::task::spawn_blocking(move || {
            // This task only reads. Cancellation can leave a bounded read in
            // flight, holding its permit, but cannot register or retain a
            // live boundary after the owning request has been dropped.
            let captured = session.and_then(|session| {
                let (source, register, codex_guard) = session.resolve_source(
                    is_start,
                    locator.as_ref(),
                    home.as_deref(),
                    deadline,
                )?;
                let registration = if register {
                    Some(LiveSessionLocator {
                        provider: source.source.provider().as_str().to_owned(),
                        session_id: source.source.session_id().as_str().to_owned(),
                        project_path: source.project_root.to_str()?.to_owned(),
                        transcript_path: source.source_path.to_str()?.to_owned(),
                    })
                } else {
                    None
                };
                let observation = capture_origin(source, previous.as_ref(), now, deadline)?;
                if let Some(guard) = codex_guard
                    && !guard.matches_current_source(deadline).ok()?
                {
                    return None;
                }
                Some((observation, registration))
            });
            (permit, captured)
        })
        .await;
        let (_permit, captured) = match captured {
            Ok(captured) => captured,
            Err(error) => {
                tracing::debug!(?error, "live hook origin capture task unavailable");
                return LiveOriginOutcomeV1::Unavailable;
            }
        };
        if Instant::now() >= deadline {
            return LiveOriginOutcomeV1::Deadline;
        }
        let observation = if let Some((observation, registration)) = captured {
            if let Some(registration) = registration {
                let Some(sessions) = sessions else {
                    return LiveOriginOutcomeV1::Unavailable;
                };
                let registered = sessions
                    .register_live_session_locator(
                        &registration.provider,
                        &registration.session_id,
                        &registration.project_path,
                        &registration.transcript_path,
                    )
                    .await;
                if !matches!(registered, Ok(true)) {
                    return LiveOriginOutcomeV1::Unavailable;
                }
            }
            Some(observation)
        } else {
            None
        };
        if Instant::now() >= deadline {
            return LiveOriginOutcomeV1::Deadline;
        }
        // Keep the permit through the existing bounded ledger write. This
        // synchronous commit stays in the owning request, so an abandoned
        // capture task has no detached authority-producing continuation.
        match retain_live_hook_origin(&data_root, envelope, receipt, observation, now) {
            Ok(HookLiveOriginOutcomeV1::Unavailable) => LiveOriginOutcomeV1::Unavailable,
            Ok(outcome) => LiveOriginOutcomeV1::Recorded(outcome),
            Err(error) => LiveOriginOutcomeV1::Ledger(error),
        }
    };
    match tokio::time::timeout(remaining, capture).await {
        Ok(outcome) => outcome,
        Err(_) => LiveOriginOutcomeV1::Deadline,
    }
}

async fn retained_session(
    cg: &TraceDecay,
    envelope: &HookEventEnvelopeV2,
    native_session: Option<&SessionId>,
    sessions: Option<&RegisteredGlobalDb>,
    profile: Option<&dyn ProfileIdentityReadPort>,
) -> Option<LiveOriginSession> {
    let (native_session, sessions, profile) = (native_session?, sessions?, profile?);
    if tracedecay_agent_hosts::hooks::protected_native_session_id(native_session.as_str())
        != envelope.protected_session_id
    {
        return None;
    }
    let provider = match envelope.producer {
        HookHostV1::ClaudeCode => "claude",
        HookHostV1::Codex => "codex",
        _ => return None,
    };
    let binding = &sessions.binding().shard_id;
    let StoreShardScopeV1::ProjectSessions { project_id } = &binding.scope else {
        return None;
    };
    if binding.brain_id != *profile.brain_id()
        || binding.profile_id != *profile.profile_id()
        || cg.store_layout().identity.project_id.as_deref() != Some(project_id.as_str())
    {
        return None;
    }
    let session = sessions
        .get_session_result(provider, native_session.as_str())
        .await
        .ok()?;
    if session.as_ref().is_some_and(|session| {
        session.provider != provider || session.session_id != native_session.as_str()
    }) {
        return None;
    }
    Some(LiveOriginSession {
        project_root: cg.project_root().to_path_buf(),
        project_id: project_id.clone(),
        brain_id: profile.brain_id().clone(),
        profile_id: profile.profile_id().clone(),
        producer: envelope.producer,
        native_session: native_session.clone(),
        recorded_project_path: session
            .as_ref()
            .map(|session| session.project_path.clone().into()),
        source_path: session.and_then(|session| session.transcript_path.map(PathBuf::from)),
    })
}

impl LiveOriginSession {
    fn resolve_source(
        self,
        is_start: bool,
        locator: Option<&tracedecay_agent_hosts::hooks::NativeSessionStartLocatorV1>,
        home: Option<&Path>,
        deadline: Instant,
    ) -> Option<(
        LiveOriginSource,
        bool,
        Option<tracedecay_sessions::runtime::codex::CodexLiveSessionTranscript>,
    )> {
        if Instant::now() >= deadline {
            return None;
        }
        let project_root = fs::canonicalize(&self.project_root).ok()?;
        if let Some(recorded) = &self.recorded_project_path
            && fs::canonicalize(recorded).ok()? != project_root
        {
            return None;
        }
        let register = self.source_path.is_none();
        let mut codex_guard = None;
        let source_path = match self.source_path {
            Some(path) => path,
            None if is_start => match self.producer {
                HookHostV1::ClaudeCode => {
                    let locator = locator?;
                    if locator.session_id != self.native_session {
                        return None;
                    }
                    validated_claude_start_source(
                        &locator.transcript_path,
                        &self.native_session,
                        home?,
                        deadline,
                    )?
                }
                HookHostV1::Codex => {
                    let guard = tracedecay_sessions::runtime::codex::CodexSource::with_home(home?)
                        .find_live_session_transcript(
                            self.native_session.as_str(),
                            &project_root,
                            deadline,
                        )
                        .ok()??;
                    let path = guard.path().to_path_buf();
                    codex_guard = Some(guard);
                    path
                }
                _ => return None,
            },
            None => return None,
        };
        if !source_path.is_absolute() || source_path.as_os_str().len() > 4096 {
            return None;
        }
        let source = match self.producer {
            HookHostV1::ClaudeCode => {
                let identity =
                    tracedecay_sessions::runtime::claude::identify_claude_source(&source_path)?;
                if identity.session_id != self.native_session.as_str() {
                    return None;
                }
                ObservationSourceIdentityV1::for_provider_source(
                    ProviderId::new("claude").ok()?,
                    self.native_session,
                    SessionId::new(identity.source_id).ok()?,
                )
                .ok()?
            }
            HookHostV1::Codex => tracedecay_sessions::runtime::codex::codex_observation_source_v2(
                self.native_session.as_str(),
            )
            .ok()?,
            _ => return None,
        };
        Some((
            LiveOriginSource {
                project_root,
                project_id: self.project_id,
                brain_id: self.brain_id,
                profile_id: self.profile_id,
                source_path,
                source,
            },
            register,
            codex_guard,
        ))
    }
}

fn validated_claude_start_source(
    path: &Path,
    native_session: &SessionId,
    home: &Path,
    deadline: Instant,
) -> Option<PathBuf> {
    if Instant::now() >= deadline
        || !path.is_absolute()
        || path.as_os_str().len() > 4096
        || path.extension().and_then(|extension| extension.to_str()) != Some("jsonl")
    {
        return None;
    }
    let identity = tracedecay_sessions::runtime::claude::identify_claude_source(path)?;
    if identity.session_id != native_session.as_str() {
        return None;
    }
    let host_root = fs::canonicalize(home.join(".claude/projects")).ok()?;
    let canonical = fs::canonicalize(path).ok()?;
    if !canonical.starts_with(&host_root) || canonical == host_root {
        return None;
    }
    let file = tracedecay_private_fs::framed_log::open_regular_read_no_follow(path).ok()?;
    let before = file.metadata().ok()?;
    if before.len() > MAX_LIVE_SOURCE_BYTES
        || native_file_evidence(&before)? != native_file_evidence(&fs::metadata(path).ok()?)?
        || fs::canonicalize(path).ok()? != canonical
        || Instant::now() >= deadline
    {
        return None;
    }
    Some(canonical)
}

#[hotpath::measure(label = "mcp.hook_runtime.live_origin.capture")]
fn capture_origin(
    source: LiveOriginSource,
    previous: Option<&HookLiveOriginBoundaryV1>,
    now: UtcMicros,
    deadline: Instant,
) -> Option<HookLiveOriginObservationV1> {
    if Instant::now() >= deadline {
        return None;
    }
    let canonical_source_path = fs::canonicalize(&source.source_path).ok()?;
    let marker =
        tracedecay_runtime_core::storage::read_repository_identity_marker(&source.project_root)
            .ok()??;
    let context = RepositoryProvenanceAdmissionContext::from_authoritative_project_marker(
        &source.project_root,
        &source.project_id,
        &marker,
    )?;
    let branch_before = capture_branch_evidence(&source.project_root, deadline)?;
    let captured = context.capture_snapshot(now);
    let EvidenceAvailabilityV1::Known(repository) = captured.availability() else {
        return None;
    };
    if repository.evidence().attached_ref()
        != &EvidenceAvailabilityV1::Known(branch_before.attached_ref.clone())
        || repository.evidence().head_commit()
            != &EvidenceAvailabilityV1::Known(branch_before.head_commit.clone())
    {
        return None;
    }
    let checkpoint = previous
        .filter(|previous| {
            previous.observation.canonical_source_path == canonical_source_path
                && previous.observation.source == source.source
        })
        .map(|previous| previous.observation.checkpoint);
    let resume = checkpoint.map(|checkpoint| {
        (
            StoredCursor {
                position: checkpoint.complete_frontier,
                file_id: checkpoint.generation,
                mtime: 0,
            },
            JsonlResumeState {
                generation: checkpoint.generation,
                file_identity: checkpoint.file_identity,
                fingerprint: checkpoint.complete_prefix_fingerprint,
            },
        )
    });
    let scan = capture_live_jsonl_origin(
        &source.source_path,
        resume,
        MAX_LIVE_SOURCE_BYTES,
        MAX_LIVE_ORIGIN_FRAMES,
        deadline,
    )
    .ok()?;
    if Instant::now() >= deadline
        || fs::canonicalize(&source.source_path).ok()? != canonical_source_path
        || capture_branch_evidence(&source.project_root, deadline)? != branch_before
    {
        return None;
    }
    Some(HookLiveOriginObservationV1 {
        scope: HookLiveOriginScopeV1 {
            brain_id: source.brain_id,
            profile_id: source.profile_id,
            repository: repository.clone(),
        },
        source: source.source,
        canonical_source_path,
        branch_evidence: branch_before,
        checkpoint: HookLiveOriginCheckpointV1 {
            generation: scan.generation,
            file_identity: scan.file_identity,
            complete_frontier: scan.complete_frontier,
            complete_prefix_fingerprint: scan.complete_prefix_fingerprint,
        },
        physical_eof: scan.physical_eof,
        validated_checkpoint: checkpoint.filter(|_| scan.validated_previous),
        frames: scan
            .frames
            .into_iter()
            .map(|frame| HookLiveOriginFrameV1 {
                start: frame.offset,
                end: frame.end_offset,
                resume_fingerprint: frame.resume_fingerprint,
            })
            .collect(),
    })
}

/// Reflog appends detect checkout A→B→A even when the final ref and commit
/// match. Native change tokens also reject truncation, replacement, rewrites,
/// and a checkout performed while reflog writing was temporarily disabled.
fn capture_branch_evidence(
    root: &Path,
    deadline: Instant,
) -> Option<HookLiveOriginBranchEvidenceV1> {
    let repo = gix::open(root).ok()?;
    if repo.config_snapshot().boolean("core.logAllRefUpdates") == Some(false) {
        return None;
    }
    let reflog = repo.git_dir().join("logs/HEAD");
    let canonical_path = fs::canonicalize(&reflog).ok()?;
    let (metadata, tail) = bounded_tail(&reflog, MAX_REFLOG_TAIL_BYTES, deadline)?;
    if metadata.len() == 0 || tail.last() != Some(&b'\n') {
        return None;
    }
    let last = tail
        .strip_suffix(b"\n")?
        .rsplit(|byte| *byte == b'\n')
        .next()?;
    let mut fields = last.split(|byte| *byte == b' ');
    let old = fields.next()?;
    let new = fields.next()?;
    // Both supported Git object formats are accepted; malformed/partial logs
    // cannot supply transition evidence.
    if ![40, 64].contains(&old.len())
        || new.len() != old.len()
        || !old.iter().chain(new).all(u8::is_ascii_hexdigit)
    {
        return None;
    }
    let head_path = repo.git_dir().join("HEAD");
    let (head_metadata, head) = bounded_tail(&head_path, 4096, deadline)?;
    if !head.starts_with(b"ref: refs/heads/") {
        return None;
    }
    let attached_ref = RefId::new(
        std::str::from_utf8(head.strip_prefix(b"ref: ")?)
            .ok()?
            .trim_end_matches('\n'),
    )
    .ok()?;
    let head_commit = CommitId::new(std::str::from_utf8(new).ok()?).ok()?;
    let (file_identity, change_token) = native_file_evidence(&metadata)?;
    let (head_file_identity, head_change_token) = native_file_evidence(&head_metadata)?;
    Some(HookLiveOriginBranchEvidenceV1 {
        attached_ref,
        head_commit,
        canonical_path,
        file_identity,
        frontier: metadata.len(),
        fingerprint: checksum(&tail),
        change_token,
        head_file_identity,
        head_change_token,
    })
}

fn bounded_tail(path: &Path, maximum: u64, deadline: Instant) -> Option<(fs::Metadata, Vec<u8>)> {
    if Instant::now() >= deadline || fs::symlink_metadata(path).ok()?.file_type().is_symlink() {
        return None;
    }
    let mut file = tracedecay_private_fs::framed_log::open_regular_read_no_follow(path).ok()?;
    let before = file.metadata().ok()?;
    if !before.is_file() {
        return None;
    }
    let identity = native_file_evidence(&before)?;
    let length = before.len().min(maximum);
    file.seek(SeekFrom::Start(before.len().checked_sub(length)?))
        .ok()?;
    let mut bytes = vec![0; usize::try_from(length).ok()?];
    file.read_exact(&mut bytes).ok()?;
    let after = file.metadata().ok()?;
    let current = fs::metadata(path).ok()?;
    if Instant::now() >= deadline
        || before.len() != after.len()
        || before.len() != current.len()
        || native_file_evidence(&after)? != identity
        || native_file_evidence(&current)? != identity
    {
        return None;
    }
    Some((after, bytes))
}

#[cfg(unix)]
fn native_file_evidence(metadata: &fs::Metadata) -> Option<([u64; 2], [i64; 4])> {
    use std::os::unix::fs::MetadataExt;
    Some((
        [metadata.dev(), metadata.ino()],
        [
            metadata.mtime(),
            metadata.mtime_nsec(),
            metadata.ctime(),
            metadata.ctime_nsec(),
        ],
    ))
}

#[cfg(not(unix))]
fn native_file_evidence(_metadata: &fs::Metadata) -> Option<([u64; 2], [i64; 4])> {
    None
}

/// Exposes only the production filesystem/Git capture to the source-control
/// integration fixture. Ledger retention and observation admission remain real.
#[cfg(test)]
#[allow(clippy::too_many_arguments)]
pub(crate) fn capture_live_origin_for_control_test(
    project_root: PathBuf,
    project_id: ProjectId,
    brain_id: BrainId,
    profile_id: UserProfileId,
    source_path: PathBuf,
    source: ObservationSourceIdentityV1,
    previous: Option<&HookLiveOriginBoundaryV1>,
    now: UtcMicros,
    deadline: Instant,
) -> Option<HookLiveOriginObservationV1> {
    capture_origin(
        LiveOriginSource {
            project_root,
            project_id,
            brain_id,
            profile_id,
            source_path,
            source,
        },
        previous,
        now,
        deadline,
    )
}

#[cfg(all(test, unix))]
mod tests {
    use super::{MAX_REFLOG_TAIL_BYTES, capture_branch_evidence};
    use std::fs;
    use std::path::Path;
    use std::process::{Command, Output};
    use std::time::{Duration, Instant};
    use tempfile::TempDir;

    fn git(root: &Path, args: &[&str]) -> Output {
        let output = Command::new(tracedecay_runtime_core::git::try_git_program().unwrap())
            .args([
                "-c",
                "user.name=Origin Fixture",
                "-c",
                "user.email=origin@example.invalid",
            ])
            .args(args)
            .env("GIT_CONFIG_NOSYSTEM", "1")
            .env("GIT_CONFIG_GLOBAL", root.join("absent-global-gitconfig"))
            .current_dir(root)
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "git {args:?}: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        output
    }

    fn repository() -> TempDir {
        let root = tempfile::tempdir().unwrap();
        git(root.path(), &["init", "-q", "-b", "main"]);
        git(
            root.path(),
            &["commit", "--allow-empty", "-q", "-m", "initial"],
        );
        root
    }

    fn deadline() -> Instant {
        Instant::now() + Duration::from_secs(5)
    }

    #[test]
    fn live_branch_evidence_accepts_long_reflogs_without_reading_the_whole_log() {
        let root = repository();
        let log = root.path().join(".git/logs/HEAD");
        let line = fs::read(&log).unwrap();
        fs::write(&log, line.repeat(1024)).unwrap();
        assert!(fs::metadata(&log).unwrap().len() > MAX_REFLOG_TAIL_BYTES);
        let first = capture_branch_evidence(root.path(), deadline()).unwrap();
        let second = capture_branch_evidence(root.path(), deadline()).unwrap();
        assert_eq!(first, second);
        assert_eq!(first.frontier, fs::metadata(&log).unwrap().len());
        assert_eq!(first.canonical_path, fs::canonicalize(&log).unwrap());
    }

    #[test]
    fn actual_git_checkout_away_and_back_changes_live_branch_evidence() {
        let root = repository();
        let before = capture_branch_evidence(root.path(), deadline()).unwrap();
        let original_commit = git(root.path(), &["rev-parse", "HEAD"]).stdout;
        git(root.path(), &["checkout", "-q", "-b", "other"]);
        git(root.path(), &["checkout", "-q", "main"]);
        let after = capture_branch_evidence(root.path(), deadline()).unwrap();
        assert_eq!(
            git(root.path(), &["rev-parse", "HEAD"]).stdout,
            original_commit
        );
        assert_eq!(
            fs::read(root.path().join(".git/HEAD")).unwrap(),
            b"ref: refs/heads/main\n"
        );
        assert!(after.frontier > before.frontier);
        assert_ne!(after, before);
    }

    #[test]
    fn disabled_missing_and_rewritten_reflogs_withhold_live_continuity() {
        let root = repository();
        let before = capture_branch_evidence(root.path(), deadline()).unwrap();
        git(root.path(), &["config", "core.logAllRefUpdates", "false"]);
        assert!(capture_branch_evidence(root.path(), deadline()).is_none());
        // Even a checkout while reflog writing was disabled changes the HEAD
        // token; restoring logging does not conceal the intervening transition.
        git(root.path(), &["checkout", "-q", "-b", "other"]);
        git(root.path(), &["checkout", "-q", "main"]);
        git(root.path(), &["config", "core.logAllRefUpdates", "true"]);
        assert_ne!(
            capture_branch_evidence(root.path(), deadline()).unwrap(),
            before
        );
        let log = root.path().join(".git/logs/HEAD");
        fs::write(&log, b"partial record").unwrap();
        assert!(capture_branch_evidence(root.path(), deadline()).is_none());
        fs::remove_file(&log).unwrap();
        assert!(capture_branch_evidence(root.path(), deadline()).is_none());
    }

    #[test]
    fn linked_worktree_uses_its_own_head_transition_evidence() {
        let root = repository();
        let worktrees = tempfile::tempdir().unwrap();
        let linked = worktrees.path().join("linked");
        git(
            root.path(),
            &[
                "worktree",
                "add",
                "-q",
                "-b",
                "linked",
                linked.to_str().unwrap(),
            ],
        );
        let linked_before = capture_branch_evidence(&linked, deadline()).unwrap();
        let main_before = capture_branch_evidence(root.path(), deadline()).unwrap();
        assert_ne!(linked_before.canonical_path, main_before.canonical_path);
        git(root.path(), &["checkout", "-q", "-b", "other"]);
        assert_eq!(
            capture_branch_evidence(&linked, deadline()).unwrap(),
            linked_before
        );
    }
}
