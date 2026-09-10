use std::path::Path;
use std::time::{Duration, Instant};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tracedecay_contracts::ResolvedScope;
#[cfg(test)]
use tracedecay_contracts::context_scout::ContextScoutFeedbackV1;
use tracedecay_contracts::context_scout::{
    ContextScoutAddressV1, ContextScoutDeliveryOutcomeV1, ContextScoutDeliveryReceiptV1,
};
use tracedecay_domain::{ObservationId, ProjectId, SessionId, UtcMicros};
use tracedecay_hooks::{
    AsyncHookFeedbackDeliveryPortV1, HookConfigurationFileReaderV1, HookConfigurationReadOutcomeV1,
    HookConfigurationSnapshotV1, HookConfigurationSubscriberV1, HookEventEnvelopeV2,
    HookFeedbackDeliveryRouteV1, HookFeedbackDeliveryV1, HookFeedbackRollbackSwitchV1,
    HookGuidanceStateV1, HookHostV1, HookImmediateAdmissionV1, HookRuntimeControlV1,
    HookScopeBindingV1, HookSpoolConfigV1, HookSpoolError, HookSpoolV1, HookSynchronousDeadlineV1,
    HookTransportDispositionV1, NativeEnvelopeMaterialV1, NativeHookDecodeError,
    SpoolAppendOutcomeV1, admit_async_exact_scope, deliver_hook_feedback, envelope_identity_hash16,
    finish_synchronous_hook,
};
#[cfg(test)]
use tracedecay_hooks::{HookImmediateAdmissionStateV1, HookScopedFeedbackV1};

#[cfg(test)]
use crate::agents::context_scout_v2::context_scout_delivery_receipt_matches_envelope;
use crate::agents::context_scout_v2::{
    ContextScoutDeliveryReceiptHookV1, context_scout_delivery_receipt_id,
};
use crate::ports::hook_runtime::HookRuntimeV1;

use super::analytics::{HookTimingSpan, elapsed_us};
#[cfg(test)]
use super::daemon_ports::{
    ContextScoutFeedbackCommitV1, DaemonContextScoutFeedbackPort, outcome_is_committed,
};
use super::daemon_ports::{
    DaemonAdmissionPort, DaemonDeliveryReceiptPort, DaemonFeedbackNoticeDeliveryPort,
    DaemonOpenCodeLspUpdatePort, now_utc,
};

pub(crate) enum HookDispatch {
    NotApplicable,
    /// The native dispatcher recognised the event but could not take ownership of it — no
    /// published binding, an unreadable store layout, or an envelope it could
    /// not decode. The disposition is still worth recording, but the event has
    /// not been admitted anywhere, so callers must fall back to their ordinary
    /// daemon notification rather than treat the event as delivered.
    Unavailable(HookTransportDispositionV1),
    Handled {
        guidance: Option<String>,
        disposition: HookTransportDispositionV1,
    },
}

impl HookDispatch {
    pub(crate) fn into_recorded_guidance(
        self,
        telemetry: &HookTimingSpan,
    ) -> Option<Option<String>> {
        match self {
            Self::NotApplicable => None,
            Self::Unavailable(disposition) => {
                telemetry.note_native_dispatch_disposition(disposition);
                None
            }
            Self::Handled {
                guidance,
                disposition,
            } => {
                telemetry.note_native_dispatch_disposition(disposition);
                Some(guidance)
            }
        }
    }
}

pub const NATIVE_HOOK_HOSTS: &[HookHostV1] = &[
    HookHostV1::ClaudeCode,
    HookHostV1::Codex,
    HookHostV1::CursorDesktop,
    HookHostV1::Hermes,
    HookHostV1::Kiro,
    HookHostV1::KimiCode,
    HookHostV1::OpenCode,
];

pub fn project_id_for_layout(
    layout: &tracedecay_runtime_core::storage::StoreLayout,
) -> Option<[u8; 16]> {
    layout
        .identity
        .project_id
        .as_deref()
        .map(|project_id| envelope_identity_hash16("project", project_id))
}

pub fn publish_daemon_bindings(
    runtime: &HookRuntimeV1,
    layout: &tracedecay_runtime_core::storage::StoreLayout,
) -> tracedecay_domain::errors::Result<()> {
    let project_key = layout.identity.project_id.as_deref().ok_or_else(|| {
        tracedecay_domain::errors::TraceDecayError::Config {
            message: "cannot publish Hook binding without typed project identity".to_owned(),
        }
    })?;
    let typed_project_id = ProjectId::new(project_key.to_owned()).map_err(|error| {
        tracedecay_domain::errors::TraceDecayError::Config {
            message: format!("cannot validate Hook project identity: {error}"),
        }
    })?;
    let scope =
        (runtime.scope_resolver)(&layout.project_root, &typed_project_id).map_err(|error| {
            tracedecay_domain::errors::TraceDecayError::Config {
                message: format!("cannot resolve Hook repository/worktree scope: {error}"),
            }
        })?;
    let now = now_utc();
    let revision = now.0.max(1) as u64;
    let (project_id, repository_id, worktree_id, worktree_epoch) =
        binding_identity_from_scope(&scope, revision);
    for host in NATIVE_HOOK_HOSTS {
        let capabilities = [
            tracedecay_hooks::HookEventFamily::SessionBoundary,
            tracedecay_hooks::HookEventFamily::PromptBoundary,
            tracedecay_hooks::HookEventFamily::ToolLifecycle,
            tracedecay_hooks::HookEventFamily::SavedEdit,
            tracedecay_hooks::HookEventFamily::TestLifecycle,
        ]
        .into_iter()
        .map(|family| tracedecay_hooks::HookCapabilityV1 {
            family,
            support: tracedecay_hooks::stock_event_support(*host, family),
        })
        .collect();
        let snapshot = tracedecay_hooks::HookConfigurationSnapshotV1 {
            schema_version: tracedecay_hooks::HOOK_CONFIGURATION_SCHEMA_VERSION,
            revision,
            published_at: now,
            expires_at: UtcMicros(now.0.saturating_add(24 * 60 * 60 * 1_000_000)),
            binding: HookScopeBindingV1 {
                host: *host,
                project_id,
                repository_id,
                worktree_id,
                worktree_epoch,
                binding_token: domain_hash32(project_key, host.hook_key()),
                capabilities,
            },
        };
        let writer = tracedecay_hooks::HookConfigurationFileWriterV1::new(
            tracedecay_hooks::hook_configuration_path(&layout.data_root, *host),
        );
        tracedecay_hooks::HookConfigurationPublisherV1::new(writer)
            .publish(snapshot)
            .map_err(|error| tracedecay_domain::errors::TraceDecayError::Config {
                message: format!(
                    "failed to publish {} Hook binding: {error}",
                    host.hook_key()
                ),
            })?;
    }
    Ok(())
}

fn binding_identity_from_scope(
    scope: &ResolvedScope,
    binding_revision: u64,
) -> ([u8; 16], [u8; 16], [u8; 16], u64) {
    (
        envelope_identity_hash16("project", scope.project_id.as_str()),
        envelope_identity_hash16("repository", scope.repository_id.as_str()),
        envelope_identity_hash16("worktree", scope.worktree_id.as_str()),
        binding_revision.max(1),
    )
}

pub fn project_and_worktree_locators_for_scope(scope: &ResolvedScope) -> ([u8; 16], [u8; 16]) {
    (
        envelope_identity_hash16("project", scope.project_id.as_str()),
        envelope_identity_hash16("worktree", scope.worktree_id.as_str()),
    )
}

#[derive(Default, Deserialize)]
struct NativeIdentityFields {
    id: Option<String>,
    #[serde(alias = "sessionID")]
    session_id: Option<String>,
    conversation_id: Option<String>,
    transcript_path: Option<String>,
    generation_id: Option<String>,
    #[serde(alias = "filePath")]
    file_path: Option<String>,
    prompt_id: Option<String>,
    turn_id: Option<String>,
    #[serde(alias = "toolUseID")]
    tool_use_id: Option<String>,
    #[serde(alias = "toolCallID")]
    tool_call_id: Option<String>,
    #[serde(alias = "callID")]
    call_id: Option<String>,
    edits: Option<Vec<serde_json::Value>>,
    properties: Option<NativeIdentityProperties>,
    input: Option<NativeIdentityInput>,
    tool_input: Option<NativeIdentityToolInput>,
    output: Option<NativeIdentityOutput>,
    extra: Option<NativeIdentityExtra>,
    route: Option<NativeIdentityRoute>,
    receipt: Option<NativeIdentityReceipt>,
}

#[derive(Default, Deserialize)]
struct NativeIdentityProperties {
    #[serde(alias = "sessionID")]
    session_id: Option<String>,
    file: Option<String>,
}

#[derive(Default, Deserialize)]
struct NativeIdentityInput {
    #[serde(alias = "sessionID")]
    session_id: Option<String>,
    #[serde(alias = "callID")]
    call_id: Option<String>,
}

#[derive(Default, Deserialize)]
struct NativeIdentityToolInput {
    #[serde(alias = "filePath")]
    file_path: Option<String>,
    path: Option<String>,
}

#[derive(Default, Deserialize)]
struct NativeIdentityOutput {
    metadata: Option<NativeIdentityOutputMetadata>,
}

#[derive(Default, Deserialize)]
struct NativeIdentityOutputMetadata {
    files: Option<Vec<NativeIdentityOutputFile>>,
}

#[derive(Default, Deserialize)]
struct NativeIdentityOutputFile {
    #[serde(alias = "filePath")]
    file_path: Option<String>,
}

#[derive(Default, Deserialize)]
struct NativeIdentityExtra {
    tool_call_id: Option<String>,
}

#[derive(Default, Deserialize)]
struct NativeIdentityRoute {
    session_id: Option<String>,
}

#[derive(Default, Deserialize)]
struct NativeIdentityReceipt {
    tool_call_id: Option<String>,
}

/// Private native Start locator. The path locates a candidate only; the daemon
/// validates the actual file and registered project before retaining a boundary.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NativeSessionStartLocatorV1 {
    pub session_id: SessionId,
    pub event_id: [u8; 16],
    pub transcript_path: std::path::PathBuf,
}

impl NativeSessionStartLocatorV1 {
    pub fn matches_envelope(&self, envelope: &HookEventEnvelopeV2) -> bool {
        envelope.producer == HookHostV1::ClaudeCode
            && matches!(
                envelope.event,
                tracedecay_hooks::HookEventV2::SessionBoundary {
                    boundary: tracedecay_hooks::HookBoundaryV1::Start
                }
            )
            && protected_session_id_for_native(self.session_id.as_str())
                == envelope.protected_session_id
            && self.event_id == envelope.event_id
            && self.transcript_path.is_absolute()
            && self.transcript_path.as_os_str().len() <= 4096
    }
}

fn native_session_start_locator(
    fields: &NativeIdentityFields,
    envelope: &HookEventEnvelopeV2,
) -> Option<NativeSessionStartLocatorV1> {
    let locator = NativeSessionStartLocatorV1 {
        session_id: SessionId::new(fields.session_id()?.to_owned()).ok()?,
        event_id: envelope.event_id,
        transcript_path: fields.transcript_path.as_deref()?.into(),
    };
    locator.matches_envelope(envelope).then_some(locator)
}

/// Provider-native lifecycle identity that may cross the local hook/daemon
/// boundary. Session and call values come from checked-in host fields; the
/// event ID binds them to the exact content-free envelope admitted alongside
/// them. Paths and payloads remain unrepresentable.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NativeContextScoutLifecycleV1 {
    pub session_id: SessionId,
    pub call_id: ObservationId,
    pub event_id: [u8; 16],
}

impl NativeContextScoutLifecycleV1 {
    pub fn new(session_id: &str, call_id: &str, event_id: [u8; 16]) -> Option<Self> {
        Some(Self {
            session_id: SessionId::new(session_id.to_owned()).ok()?,
            call_id: ObservationId::new(call_id.to_owned()).ok()?,
            event_id,
        })
    }

    pub fn matches_envelope(&self, envelope: &HookEventEnvelopeV2) -> bool {
        matches!(
            envelope.producer,
            HookHostV1::KimiCode | HookHostV1::OpenCode
        ) && protected_session_id_for_native(self.session_id.as_str())
            == envelope.protected_session_id
            && self.event_id == envelope.event_id
            && matches!(
                envelope.event,
                tracedecay_hooks::HookEventV2::SavedEdit { .. }
                    | tracedecay_hooks::HookEventV2::ToolLifecycle { .. }
            )
    }
}

impl NativeIdentityFields {
    fn session_id(&self) -> Option<&str> {
        self.session_id
            .as_deref()
            .or(self.conversation_id.as_deref())
            .or_else(|| {
                self.properties
                    .as_ref()
                    .and_then(|properties| properties.session_id.as_deref())
            })
            .or_else(|| {
                self.input
                    .as_ref()
                    .and_then(|input| input.session_id.as_deref())
            })
            .or_else(|| {
                self.route
                    .as_ref()
                    .and_then(|route| route.session_id.as_deref())
            })
    }

    fn event_key(&self) -> Option<&str> {
        self.tool_use_id
            .as_deref()
            .or(self.tool_call_id.as_deref())
            .or(self.call_id.as_deref())
            .or_else(|| {
                self.input
                    .as_ref()
                    .and_then(|input| input.call_id.as_deref())
            })
            .or_else(|| {
                self.extra
                    .as_ref()
                    .and_then(|extra| extra.tool_call_id.as_deref())
            })
            .or_else(|| {
                self.receipt
                    .as_ref()
                    .and_then(|receipt| receipt.tool_call_id.as_deref())
            })
            .or(self.generation_id.as_deref())
            .or(self.prompt_id.as_deref())
            .or(self.turn_id.as_deref())
            .or(self.id.as_deref())
    }

    fn call_id(&self) -> Option<&str> {
        self.tool_use_id
            .as_deref()
            .or(self.tool_call_id.as_deref())
            .or(self.call_id.as_deref())
            .or_else(|| {
                self.input
                    .as_ref()
                    .and_then(|input| input.call_id.as_deref())
            })
            .or_else(|| {
                self.extra
                    .as_ref()
                    .and_then(|extra| extra.tool_call_id.as_deref())
            })
            .or_else(|| {
                self.receipt
                    .as_ref()
                    .and_then(|receipt| receipt.tool_call_id.as_deref())
            })
    }

    fn file_path(&self) -> Option<&str> {
        self.file_path
            .as_deref()
            .or_else(|| self.properties.as_ref()?.file.as_deref())
            .or_else(|| {
                let tool_input = self.tool_input.as_ref()?;
                tool_input
                    .file_path
                    .as_deref()
                    .or(tool_input.path.as_deref())
            })
            .or_else(|| {
                let files = self.output.as_ref()?.metadata.as_ref()?.files.as_ref()?;
                (files.len() == 1)
                    .then(|| files[0].file_path.as_deref())
                    .flatten()
            })
            .filter(|path| !path.is_empty())
    }
}

fn native_context_scout_lifecycle(
    host: HookHostV1,
    fields: &NativeIdentityFields,
    event_id: [u8; 16],
) -> Option<NativeContextScoutLifecycleV1> {
    matches!(host, HookHostV1::KimiCode | HookHostV1::OpenCode)
        .then(|| {
            NativeContextScoutLifecycleV1::new(fields.session_id()?, fields.call_id()?, event_id)
        })
        .flatten()
}

const HOOK_ADMISSION_ACK_BUDGET_MICROS: u64 = 25_000;
const NATIVE_LIFECYCLE_BUDGET: Duration = Duration::from_secs(1);
const CODEX_STOP_ENQUEUE_RESERVE: Duration = Duration::from_millis(100);

pub(super) fn native_lifecycle_deadline(started: Instant) -> Instant {
    started + NATIVE_LIFECYCLE_BUDGET
}

fn bound_lifecycle_admission_deadline(
    envelope: &HookEventEnvelopeV2,
    started: Instant,
) -> Option<Instant> {
    use tracedecay_hooks::{HookBoundaryV1, HookEventV2};
    if !matches!(
        envelope.producer,
        HookHostV1::ClaudeCode | HookHostV1::Codex
    ) || !matches!(
        envelope.event,
        HookEventV2::SessionBoundary {
            boundary: HookBoundaryV1::Start | HookBoundaryV1::TurnComplete
        }
    ) {
        return None;
    }
    let deadline = native_lifecycle_deadline(started);
    Some(
        if envelope.producer == HookHostV1::Codex
            && matches!(
                envelope.event,
                HookEventV2::SessionBoundary {
                    boundary: HookBoundaryV1::TurnComplete
                }
            )
        {
            deadline - CODEX_STOP_ENQUEUE_RESERVE
        } else {
            deadline
        },
    )
}

fn admission_window_after_elapsed(elapsed: u64) -> Option<(HookSynchronousDeadlineV1, Duration)> {
    let deadline = HookSynchronousDeadlineV1::after_elapsed(elapsed)?;
    let admission_remaining = HOOK_ADMISSION_ACK_BUDGET_MICROS.checked_sub(elapsed)?;
    if admission_remaining == 0 || deadline.remaining_micros() == 0 {
        return None;
    }
    Some((
        deadline,
        Duration::from_micros(admission_remaining.min(deadline.remaining_micros())),
    ))
}

#[hotpath::measure(future = true, label = "hosts.hooks.dispatch")]
pub(crate) async fn dispatch(
    runtime: &HookRuntimeV1,
    host: HookHostV1,
    event_json: &str,
    project_root: &Path,
    telemetry: Option<&HookTimingSpan>,
    started: Instant,
) -> HookDispatch {
    dispatch_with_required_work(
        runtime,
        host,
        event_json,
        project_root,
        telemetry,
        started,
        async {},
    )
    .await
    .0
}

async fn dispatch_with_required_work<T>(
    runtime: &HookRuntimeV1,
    host: HookHostV1,
    event_json: &str,
    project_root: &Path,
    telemetry: Option<&HookTimingSpan>,
    started: Instant,
    required: impl std::future::Future<Output = T>,
) -> (HookDispatch, T) {
    let decoded = match tracedecay_hooks::decode_native_hook_event(host, event_json.as_bytes()) {
        Ok(decoded) => decoded,
        Err(
            NativeHookDecodeError::UnsupportedNativeEvent
            | NativeHookDecodeError::UnsupportedNativeFamily,
        ) => {
            return (HookDispatch::NotApplicable, required.await);
        }
        Err(_) => return (unavailable(), required.await),
    };
    let Some(prepared) = prepare_bound_hook(host, event_json, project_root, decoded, started)
    else {
        return (unavailable(), required.await);
    };
    let native_session_id = prepared.native_session_id.clone();
    let native_lifecycle = prepared.native_lifecycle.clone();
    let native_start_locator = prepared.native_start_locator.clone();
    let admission = DaemonAdmissionPort::new(
        runtime,
        project_root,
        native_session_id.as_deref(),
        native_lifecycle.as_ref(),
        native_start_locator.as_ref(),
        telemetry,
    );
    let delivery = DaemonFeedbackNoticeDeliveryPort::new(runtime, project_root);
    dispatch_decoded_with_required_work(
        runtime,
        prepared,
        project_root,
        started,
        &admission,
        &delivery,
        required,
    )
    .await
}

/// Dispatches a native event through its exact project binding when one is
/// known, or through the authenticated daemon profile when the host has no
/// project identity. Both paths send only the closed event material.
pub(crate) async fn dispatch_for_scope(
    runtime: &HookRuntimeV1,
    host: HookHostV1,
    event_json: &str,
    project_root: Option<&Path>,
    telemetry: Option<&HookTimingSpan>,
    started: Instant,
) -> HookDispatch {
    match project_root {
        Some(project_root) => {
            dispatch(runtime, host, event_json, project_root, telemetry, started).await
        }
        None => dispatch_profile_scoped(runtime, host, event_json, telemetry, started).await,
    }
}

/// Admit the live event, then run required producer work before any optional
/// guidance delivery can spend the remainder of the original hook deadline.
pub(crate) async fn dispatch_for_scope_with_required_work<T>(
    runtime: &HookRuntimeV1,
    host: HookHostV1,
    event_json: &str,
    project_root: Option<&Path>,
    telemetry: Option<&HookTimingSpan>,
    started: Instant,
    required: impl std::future::Future<Output = T>,
) -> (HookDispatch, T) {
    match project_root {
        Some(project_root) => {
            dispatch_with_required_work(
                runtime,
                host,
                event_json,
                project_root,
                telemetry,
                started,
                required,
            )
            .await
        }
        None => {
            let dispatched =
                dispatch_profile_scoped(runtime, host, event_json, telemetry, started).await;
            (dispatched, required.await)
        }
    }
}

async fn dispatch_profile_scoped(
    runtime: &HookRuntimeV1,
    host: HookHostV1,
    event_json: &str,
    telemetry: Option<&HookTimingSpan>,
    started: Instant,
) -> HookDispatch {
    let decoded = match tracedecay_hooks::decode_native_hook_event(host, event_json.as_bytes()) {
        Ok(decoded) => decoded,
        Err(
            NativeHookDecodeError::UnsupportedNativeEvent
            | NativeHookDecodeError::UnsupportedNativeFamily,
        ) => return HookDispatch::NotApplicable,
        Err(_) => return unavailable(),
    };
    let Ok(fields) = serde_json::from_str::<NativeIdentityFields>(event_json) else {
        return unavailable();
    };
    let Some(material) =
        native_material(&fields, decoded.family(), event_json.as_bytes(), now_utc())
    else {
        return unavailable();
    };
    let Some((_, timeout)) = admission_window_after_elapsed(elapsed_us(started)) else {
        return unavailable();
    };
    let response = tokio::time::timeout(
        timeout,
        super::daemon_hook_action(
            runtime,
            None,
            serde_json::json!({
                "action": "hook_v2_profile_admit",
                "admission": tracedecay_hooks::ProfileScopedNativeHookAdmissionV1 {
                    decoded,
                    material,
                },
            }),
            telemetry,
        ),
    )
    .await;
    let Ok(Ok(response)) = response else {
        return unavailable();
    };
    let accepted = response.get("action").and_then(serde_json::Value::as_str)
        == Some("hook_v2_profile_admit")
        && matches!(
            response.get("status").and_then(serde_json::Value::as_str),
            Some("accepted" | "exact_duplicate")
        )
        && response
            .get("disposition")
            .and_then(serde_json::Value::as_str)
            == Some("accepted");
    if accepted {
        HookDispatch::Handled {
            guidance: None,
            disposition: HookTransportDispositionV1::Accepted,
        }
    } else {
        unavailable()
    }
}

#[hotpath::measure(future = true, label = "hosts.hooks.opencode.dispatch_tool_after")]
pub(crate) async fn dispatch_opencode_tool_after(
    runtime: &HookRuntimeV1,
    event_json: &str,
    project_root: &Path,
    telemetry: Option<&HookTimingSpan>,
    started: Instant,
) -> HookDispatch {
    let decoded = match tracedecay_hooks::decode_opencode_plugin_event(
        tracedecay_hooks::OpenCodePluginSurfaceV1::ToolExecuteAfter,
        event_json.as_bytes(),
    ) {
        Ok(decoded) => decoded,
        Err(
            NativeHookDecodeError::UnsupportedNativeEvent
            | NativeHookDecodeError::UnsupportedNativeFamily,
        ) => {
            return HookDispatch::NotApplicable;
        }
        Err(_) => return unavailable(),
    };
    let Some(prepared) = prepare_bound_hook(
        HookHostV1::OpenCode,
        event_json,
        project_root,
        decoded,
        started,
    ) else {
        return unavailable();
    };
    let native_session_id = prepared.native_session_id.clone();
    let native_lifecycle = prepared.native_lifecycle.clone();
    let native_start_locator = prepared.native_start_locator.clone();
    let admission = DaemonAdmissionPort::new(
        runtime,
        project_root,
        native_session_id.as_deref(),
        native_lifecycle.as_ref(),
        native_start_locator.as_ref(),
        telemetry,
    );
    let delivery = DaemonFeedbackNoticeDeliveryPort::new(runtime, project_root);
    dispatch_decoded(
        runtime,
        prepared,
        project_root,
        started,
        &admission,
        &delivery,
    )
    .await
}

#[hotpath::measure(future = true, label = "hosts.hooks.opencode.dispatch_lsp_updated")]
pub(crate) async fn dispatch_opencode_lsp_updated(
    runtime: &HookRuntimeV1,
    event_json: &str,
    project_root: &Path,
    telemetry: Option<&HookTimingSpan>,
) -> HookDispatch {
    if tracedecay_hooks::decode_opencode_lsp_event(event_json.as_bytes()).is_err() {
        return unavailable();
    }
    let Ok(event) = serde_json::from_str::<serde_json::Value>(event_json) else {
        return unavailable();
    };
    let port = DaemonOpenCodeLspUpdatePort::new(runtime, project_root, telemetry);
    if port.submit_updated_event(&event).await {
        HookDispatch::Handled {
            guidance: None,
            disposition: HookTransportDispositionV1::Accepted,
        }
    } else {
        unavailable()
    }
}

struct PreparedBoundHook {
    host: HookHostV1,
    layout: tracedecay_runtime_core::storage::StoreLayout,
    snapshot: HookConfigurationSnapshotV1,
    envelope: HookEventEnvelopeV2,
    replayed: bool,
    native_session_id: Option<String>,
    native_lifecycle: Option<NativeContextScoutLifecycleV1>,
    native_start_locator: Option<NativeSessionStartLocatorV1>,
    prepared_at: UtcMicros,
}

fn prepare_bound_hook(
    host: HookHostV1,
    event_json: &str,
    project_root: &Path,
    decoded: tracedecay_hooks::DecodedNativeHookEventV1,
    started: Instant,
) -> Option<PreparedBoundHook> {
    let layout = super::store_layout::layout(project_root)?;
    let config_path = tracedecay_hooks::hook_configuration_path(&layout.data_root, host);
    let subscriber =
        HookConfigurationSubscriberV1::new(HookConfigurationFileReaderV1::new(config_path));
    let now = now_utc();
    let HookConfigurationReadOutcomeV1::Bound(snapshot) = subscriber.load_current(host, now) else {
        return None;
    };
    let binding = &snapshot.binding;
    let native_fields = serde_json::from_str::<NativeIdentityFields>(event_json).ok()?;
    let native_session_id = native_fields.session_id().map(str::to_owned);
    let mut material =
        native_material(&native_fields, decoded.family(), event_json.as_bytes(), now)?;
    if host == HookHostV1::ClaudeCode
        && decoded.signal
            == tracedecay_hooks::native::NativeHookSignalV1::SessionBoundary(
                tracedecay_hooks::HookBoundaryV1::TurnComplete,
            )
        && native_fields.event_key().is_none()
    {
        material.event_id = claude_stop_checkpoint_event_id(
            &native_fields,
            material.event_id,
            &tracedecay_sessions::runtime::home_dir()?,
            native_lifecycle_deadline(started),
        )?;
    }
    let native_lifecycle = native_context_scout_lifecycle(host, &native_fields, material.event_id);
    let envelope = decoded.into_envelope(binding, material).ok()?;
    let (envelope, replayed) =
        match replay_envelope_if_pending(&layout.data_root, host, binding, &envelope, now) {
            PendingEnvelopeV1::Missing => (envelope, false),
            PendingEnvelopeV1::Exact(queued) => (queued, true),
            PendingEnvelopeV1::Unavailable => return None,
        };
    let native_start_locator = native_session_start_locator(&native_fields, &envelope);
    Some(PreparedBoundHook {
        host,
        layout,
        snapshot,
        envelope,
        replayed,
        native_session_id,
        native_lifecycle,
        native_start_locator,
        prepared_at: now,
    })
}

/// A project open may republish the same binding while its first Start is in flight.
/// Only its epoch can change; the original event and hook-start deadline survive.
fn refresh_native_session_start_binding(
    data_root: &Path,
    snapshot: &HookConfigurationSnapshotV1,
    envelope: &HookEventEnvelopeV2,
    deadline: Instant,
) -> Option<(HookConfigurationSnapshotV1, HookEventEnvelopeV2)> {
    if Instant::now() >= deadline
        || !matches!(
            envelope.producer,
            HookHostV1::ClaudeCode | HookHostV1::Codex
        )
        || !matches!(
            envelope.event,
            tracedecay_hooks::HookEventV2::SessionBoundary {
                boundary: tracedecay_hooks::HookBoundaryV1::Start
            }
        )
    {
        return None;
    }
    let subscriber = HookConfigurationSubscriberV1::new(HookConfigurationFileReaderV1::new(
        tracedecay_hooks::hook_configuration_path(data_root, envelope.producer),
    ));
    let HookConfigurationReadOutcomeV1::Bound(current) =
        subscriber.load_current(envelope.producer, now_utc())
    else {
        return None;
    };
    if current.revision <= snapshot.revision
        || current.binding.worktree_epoch <= snapshot.binding.worktree_epoch
    {
        return None;
    }
    let mut expected_binding = snapshot.binding.clone();
    expected_binding.worktree_epoch = current.binding.worktree_epoch;
    if current.binding != expected_binding {
        return None;
    }
    let mut refreshed = envelope.clone();
    refreshed.worktree_epoch = current.binding.worktree_epoch;
    if refreshed.validate(&current.binding).is_err() || Instant::now() >= deadline {
        return None;
    }
    Some((current, refreshed))
}

async fn dispatch_decoded(
    runtime: &HookRuntimeV1,
    prepared: PreparedBoundHook,
    project_root: &Path,
    started: Instant,
    admission: &DaemonAdmissionPort<'_>,
    delivery: &impl AsyncHookFeedbackDeliveryPortV1<
        tracedecay_application::advisory::AdvisoryHookLookupNoticeV1,
    >,
) -> HookDispatch {
    dispatch_decoded_with_required_work(
        runtime,
        prepared,
        project_root,
        started,
        admission,
        delivery,
        async {},
    )
    .await
    .0
}

#[hotpath::measure(future = true, label = "hosts.hooks.dispatch_decoded")]
async fn dispatch_decoded_with_required_work<T>(
    runtime: &HookRuntimeV1,
    prepared: PreparedBoundHook,
    project_root: &Path,
    started: Instant,
    admission: &DaemonAdmissionPort<'_>,
    delivery: &impl AsyncHookFeedbackDeliveryPortV1<
        tracedecay_application::advisory::AdvisoryHookLookupNoticeV1,
    >,
    required: impl std::future::Future<Output = T>,
) -> (HookDispatch, T) {
    let PreparedBoundHook {
        host,
        layout,
        mut snapshot,
        mut envelope,
        replayed,
        prepared_at,
        ..
    } = prepared;
    let lifecycle_deadline = bound_lifecycle_admission_deadline(&envelope, started);
    let mut immediate = if let Some(deadline) = lifecycle_deadline {
        if envelope.validate(&snapshot.binding).is_err() {
            HookImmediateAdmissionV1::Unavailable
        } else {
            admission.try_admit_lifecycle(&envelope, deadline).await
        }
    } else {
        match admission_window_after_elapsed(elapsed_us(started)) {
            Some((deadline, timeout)) => match tokio::time::timeout(
                timeout,
                admit_async_exact_scope(&envelope, &snapshot.binding, deadline, admission),
            )
            .await
            {
                Ok(Ok(outcome)) => outcome,
                Ok(Err(_)) => HookImmediateAdmissionV1::Unavailable,
                Err(_) => HookImmediateAdmissionV1::TimedOut,
            },
            None => HookImmediateAdmissionV1::TimedOut,
        }
    };
    if !replayed
        && matches!(immediate, HookImmediateAdmissionV1::CatchupRequired)
        && admission.take_binding_changed()
        && let Some(deadline) = lifecycle_deadline
        && let Some((current, refreshed)) =
            refresh_native_session_start_binding(&layout.data_root, &snapshot, &envelope, deadline)
    {
        snapshot = current;
        envelope = refreshed;
        immediate = admission.try_admit_lifecycle(&envelope, deadline).await;
    }
    let binding = &snapshot.binding;
    let required_result = required.await;
    let replay = match immediate {
        HookImmediateAdmissionV1::Accepted { .. } | HookImmediateAdmissionV1::CatchupRequired => {
            None
        }
        HookImmediateAdmissionV1::Unavailable
        | HookImmediateAdmissionV1::TimedOut
        | HookImmediateAdmissionV1::Backpressured => Some(append_for_replay(
            &layout.data_root,
            host,
            &envelope,
            binding,
            prepared_at,
        )),
    };
    let guidance_envelope_id = match &immediate {
        HookImmediateAdmissionV1::Accepted {
            ready_guidance: Some(guidance),
            ..
        } => Some(guidance.guidance_id),
        _ => None,
    };
    let control = HookRuntimeControlV1::from_configuration(&snapshot, HookGuidanceStateV1::Active);
    let completed = finish_synchronous_hook(
        &envelope,
        binding,
        control,
        immediate,
        replay,
        now_utc(),
        elapsed_us(started),
    );
    let context_scout_address = admission.take_context_scout_address();
    let feedback_notice = admission.take_feedback_notice();
    let github_stack_signal_available = admission.take_github_stack_signal_available();
    let dispatched = match completed {
        Ok(result) => {
            let rollback = HookFeedbackRollbackSwitchV1 {
                configuration_revision: snapshot.revision,
                route: HookFeedbackDeliveryRouteV1::HookV2,
            };
            let deadline = HookSynchronousDeadlineV1::after_elapsed(elapsed_us(started));
            let scout_receipt = match (result.rendered_guidance.as_ref(), guidance_envelope_id) {
                (Some(_), Some(envelope_id)) => Some(ContextScoutDeliveryReceiptHookV1 {
                    receipt: ContextScoutDeliveryReceiptV1 {
                        receipt_id: context_scout_delivery_receipt_id(
                            envelope.event_id,
                            envelope_id,
                        ),
                        envelope_id,
                        delivered_at: now_utc(),
                        outcome: ContextScoutDeliveryOutcomeV1::Attempted,
                    },
                }),
                _ => None,
            };
            let receipts = DaemonDeliveryReceiptPort::new(runtime, project_root);
            // Scout receipt and advisory notice use independent ports; overlapping
            // them keeps both inside the leftover sync budget without combining
            // the two delivery contracts.
            let (_scout_delivery, notice_delivery) = tokio::join!(
                deliver_hook_feedback(
                    &envelope,
                    &result.receipt,
                    rollback,
                    scout_receipt,
                    deadline,
                    &receipts,
                ),
                deliver_hook_feedback(
                    &envelope,
                    &result.receipt,
                    rollback,
                    feedback_notice,
                    deadline,
                    delivery,
                ),
            );
            let delivered = notice_delivery.unwrap_or(HookFeedbackDeliveryV1 {
                feedback: None,
                outcome: None,
            });
            HookDispatch::Handled {
                guidance: HookSynchronousDeadlineV1::after_elapsed(elapsed_us(started)).and_then(
                    |_| {
                        render_host_delivery(
                            result.rendered_guidance,
                            context_scout_address.as_ref(),
                            delivered.feedback.as_ref(),
                            github_stack_signal_available,
                        )
                    },
                ),
                disposition: result.receipt.disposition,
            }
        }
        Err(_) => unavailable(),
    };
    (dispatched, required_result)
}

#[cfg(test)]
impl HookScopedFeedbackV1 for ContextScoutFeedbackCommitV1 {
    fn matches_envelope(&self, envelope: &HookEventEnvelopeV2) -> bool {
        self.feedback.receipt_id == self.receipt.receipt_id
            && context_scout_delivery_receipt_matches_envelope(&self.receipt, envelope)
    }
}

#[cfg(test)]
pub(crate) async fn record_context_scout_delivery(
    runtime: &HookRuntimeV1,
    project_root: &Path,
    receipt: &ContextScoutDeliveryReceiptV1,
) -> bool {
    let Some(deadline) = HookSynchronousDeadlineV1::after_elapsed(0) else {
        return false;
    };
    outcome_is_committed(
        DaemonDeliveryReceiptPort::new(runtime, project_root)
            .post_receipt(receipt, deadline)
            .await,
    )
}

#[cfg(test)]
pub(crate) async fn commit_context_scout_feedback(
    runtime: &HookRuntimeV1,
    project_root: &Path,
    receipt: &ContextScoutDeliveryReceiptV1,
    feedback: ContextScoutFeedbackV1,
) -> bool {
    let Some(deadline) = HookSynchronousDeadlineV1::after_elapsed(0) else {
        return false;
    };
    outcome_is_committed(
        DaemonContextScoutFeedbackPort::new(runtime, project_root)
            .post_feedback(receipt, &feedback, deadline)
            .await,
    )
}

fn render_host_delivery(
    guidance: Option<String>,
    context_scout_address: Option<&ContextScoutAddressV1>,
    feedback_notice: Option<&tracedecay_application::advisory::AdvisoryHookLookupNoticeV1>,
    github_stack_signal_available: bool,
) -> Option<String> {
    let scout_address = context_scout_address
        .and_then(|address| serde_json::to_string(address).ok())
        .map(|address| {
            format!("TraceDecay Context Scout address for authorized operations: {address}")
        });
    let notice = feedback_notice
        .and_then(|notice| serde_json::to_string(notice).ok())
        .map(|notice| format!("TraceDecay feedback ready for authorized lookup: {notice}"));
    let stack_wakeup = github_stack_signal_available
        .then_some("TraceDecay GitHub stack update available for authenticated expansion.");
    [
        guidance,
        scout_address,
        notice,
        stack_wakeup.map(str::to_owned),
    ]
    .into_iter()
    .flatten()
    .reduce(|mut rendered, next| {
        rendered.push_str("\n\n");
        rendered.push_str(&next);
        rendered
    })
}

/// Spool writer admission waits one synchronous budget measured from the lock
/// attempt, not from hook start. The response lane spends its budget before it
/// reaches the spool (analytics rows, layout resolution, the daemon admission
/// window), so a deadline anchored at hook start was already expired on a
/// loaded runner and refused an uncontended lock: the hook answered `{}` with
/// exit 0 and the event was never spooled.
fn append_for_replay(
    data_root: &Path,
    host: HookHostV1,
    envelope: &HookEventEnvelopeV2,
    binding: &HookScopeBindingV1,
    now: UtcMicros,
) -> SpoolAppendOutcomeV1 {
    let root = data_root.join("hook-v2-spool").join(host.hook_key());
    let Ok((mut spool, _)) = HookSpoolV1::open_within(
        root,
        HookSpoolConfigV1::stock(host),
        now,
        tracedecay_hooks::HOOK_SYNCHRONOUS_BUDGET,
    ) else {
        return SpoolAppendOutcomeV1::Unavailable;
    };
    match spool.append(envelope.clone(), binding, now) {
        Ok(_) => SpoolAppendOutcomeV1::Accepted,
        Err(HookSpoolError::SpoolFull) => SpoolAppendOutcomeV1::Full,
        Err(_) => SpoolAppendOutcomeV1::Unavailable,
    }
}

/// Reuse a timed-out attempt's exact envelope before retrying daemon admission.
/// The spool contains only typed, protected material; provider paths and
/// payloads are neither retained nor compared here.
#[derive(Debug, PartialEq, Eq)]
enum PendingEnvelopeV1 {
    Missing,
    Exact(HookEventEnvelopeV2),
    Unavailable,
}

fn replay_envelope_if_pending(
    data_root: &Path,
    host: HookHostV1,
    binding: &HookScopeBindingV1,
    retry: &HookEventEnvelopeV2,
    now: UtcMicros,
) -> PendingEnvelopeV1 {
    let root = data_root.join("hook-v2-spool").join(host.hook_key());
    let Ok((mut spool, _)) = HookSpoolV1::open_within(
        root,
        HookSpoolConfigV1::stock(host),
        now,
        tracedecay_hooks::HOOK_SYNCHRONOUS_BUDGET,
    ) else {
        return PendingEnvelopeV1::Unavailable;
    };
    let queued = match spool.pending_envelope(retry.event_id) {
        Ok(Some(queued)) => queued,
        Ok(None) => return PendingEnvelopeV1::Missing,
        Err(_) => return PendingEnvelopeV1::Unavailable,
    };
    if queued.validate(binding).is_err() {
        return PendingEnvelopeV1::Unavailable;
    }
    let mut retry_identity = retry.clone();
    retry_identity.observed_at = queued.observed_at;
    if queued == retry_identity {
        PendingEnvelopeV1::Exact(queued)
    } else {
        PendingEnvelopeV1::Unavailable
    }
}

pub fn native_capture_material(
    source: tracedecay_hooks::NativeHookCaptureSourceV1,
    payload: &[u8],
    observed_at: UtcMicros,
) -> Result<NativeEnvelopeMaterialV1, NativeHookDecodeError> {
    let decoded = match source {
        tracedecay_hooks::NativeHookCaptureSourceV1::Host(host) => {
            tracedecay_hooks::decode_native_hook_event(host, payload)?
        }
        tracedecay_hooks::NativeHookCaptureSourceV1::OpenCodeToolExecuteAfter => {
            tracedecay_hooks::decode_opencode_plugin_event(
                tracedecay_hooks::OpenCodePluginSurfaceV1::ToolExecuteAfter,
                payload,
            )?
        }
    };
    let fields = serde_json::from_slice::<NativeIdentityFields>(payload)
        .map_err(|_| NativeHookDecodeError::MalformedPayload)?;
    native_material(&fields, decoded.family(), payload, observed_at)
        .ok_or(NativeHookDecodeError::MissingOpaqueMaterial)
}

/// Builds the envelope material from the identity fields the caller already
/// decoded. `prepare_bound_hook` decodes them once for the native session id and
/// the context-scout lifecycle; re-decoding the same payload here was a second
/// full deserialization of every hook event.
fn native_material(
    fields: &NativeIdentityFields,
    family: tracedecay_hooks::HookEventFamily,
    event_material: &[u8],
    observed_at: UtcMicros,
) -> Option<NativeEnvelopeMaterialV1> {
    let session = fields.session_id()?;
    let event_id = fields.event_key().map_or_else(
        || native_event_digest(family, b"exact-event-material", event_material),
        |event_key| {
            if family == tracedecay_hooks::HookEventFamily::SavedEdit {
                fields.file_path().map_or_else(
                    || native_event_digest(family, b"provider-event-key", event_key.as_bytes()),
                    |file_path| saved_edit_event_id(event_key, file_path),
                )
            } else {
                native_event_digest(family, b"provider-event-key", event_key.as_bytes())
            }
        },
    );
    Some(NativeEnvelopeMaterialV1 {
        event_id,
        protected_session_id: protected_session_id_for_native(session),
        observed_at,
        tool_id: (family == tracedecay_hooks::HookEventFamily::ToolLifecycle).then_some(event_id),
        effect_receipt_id: fields.call_id().map(|value| hash16(value.as_bytes())),
        file_id: (family == tracedecay_hooks::HookEventFamily::SavedEdit).then(|| {
            fields
                .file_path()
                .map_or(event_id, |file_path| hash16(file_path.as_bytes()))
        }),
        changed_range_count: fields
            .edits
            .as_ref()
            .map_or(1, |edits| edits.len().min(64) as u8),
    })
}

/// Claude Stop has no native turn key. Bind that otherwise identical payload
/// to the actual bounded transcript checkpoint, using only opaque witnesses.
/// Failure leaves this live admission unavailable; the payload-only ID would
/// collide with the previous completed turn.
fn claude_stop_checkpoint_event_id(
    fields: &NativeIdentityFields,
    native_event_id: [u8; 16],
    home: &Path,
    deadline: Instant,
) -> Option<[u8; 16]> {
    use tracedecay_sessions::runtime::claude::identify_claude_source;
    use tracedecay_sessions::runtime::source::capture_live_jsonl_origin;

    let path = Path::new(fields.transcript_path.as_deref()?);
    if Instant::now() >= deadline
        || !path.is_absolute()
        || path.as_os_str().len() > 4096
        || path.extension().and_then(|extension| extension.to_str()) != Some("jsonl")
    {
        return None;
    }
    let native_session = SessionId::new(fields.session_id()?.to_owned()).ok()?;
    let identity = identify_claude_source(path)?;
    if identity.session_id != native_session.as_str() {
        return None;
    }
    let host_root = std::fs::canonicalize(home.join(".claude/projects")).ok()?;
    let canonical = std::fs::canonicalize(path).ok()?;
    if !canonical.starts_with(&host_root) || canonical == host_root {
        return None;
    }
    let source_key = identify_claude_source(&canonical)?
        .cursor_key
        .durable_text();
    // The shared reader rejects symlinks/non-regular files, oversize sources,
    // concurrent changes and deadline expiry before returning a checkpoint.
    let checkpoint = capture_live_jsonl_origin(path, None, 16 * 1024 * 1024, 256, deadline).ok()?;
    if std::fs::canonicalize(path).ok()? != canonical || Instant::now() >= deadline {
        return None;
    }
    let mut material = Vec::with_capacity(source_key.len() + 64);
    material.extend_from_slice(&native_event_id);
    material.extend_from_slice(&(source_key.len() as u64).to_le_bytes());
    material.extend_from_slice(source_key.as_bytes());
    for witness in [
        checkpoint.generation,
        checkpoint.file_identity,
        checkpoint.complete_frontier,
        checkpoint.complete_prefix_fingerprint,
        checkpoint.physical_eof,
    ] {
        material.extend_from_slice(&witness.to_le_bytes());
    }
    Some(native_event_digest(
        tracedecay_hooks::HookEventFamily::SessionBoundary,
        b"claude-stop-transcript-checkpoint-v1",
        &material,
    ))
}

fn saved_edit_event_id(event_key: &str, file_path: &str) -> [u8; 16] {
    let mut material = Vec::with_capacity(event_key.len() + file_path.len() + 16);
    material.extend_from_slice(&(event_key.len() as u64).to_le_bytes());
    material.extend_from_slice(event_key.as_bytes());
    material.extend_from_slice(&(file_path.len() as u64).to_le_bytes());
    material.extend_from_slice(file_path.as_bytes());
    native_event_digest(
        tracedecay_hooks::HookEventFamily::SavedEdit,
        b"provider-event-key-and-file",
        &material,
    )
}

/// Hash native event identity in the event-family domain. Provider hook
/// surfaces legitimately reuse IDs across lifecycle families, while the spool
/// keys pending records only by this ID.
fn native_event_digest(
    family: tracedecay_hooks::HookEventFamily,
    source: &[u8],
    material: &[u8],
) -> [u8; 16] {
    let family_tag = match family {
        tracedecay_hooks::HookEventFamily::SessionBoundary => 1u8,
        tracedecay_hooks::HookEventFamily::PromptBoundary => 2,
        tracedecay_hooks::HookEventFamily::ToolLifecycle => 3,
        tracedecay_hooks::HookEventFamily::SavedEdit => 4,
        tracedecay_hooks::HookEventFamily::TestLifecycle => 5,
    };
    let mut digest_material = Vec::with_capacity(source.len() + material.len() + 49);
    digest_material.extend_from_slice(b"tracedecay.hook-v2.native-event.v2");
    digest_material.push(family_tag);
    digest_material.extend_from_slice(&(source.len() as u64).to_le_bytes());
    digest_material.extend_from_slice(source);
    digest_material.extend_from_slice(&(material.len() as u64).to_le_bytes());
    digest_material.extend_from_slice(material);
    hash16(&digest_material)
}

fn hash16(bytes: &[u8]) -> [u8; 16] {
    let digest = Sha256::digest(bytes);
    let mut output = [0; 16];
    output.copy_from_slice(&digest[..16]);
    output
}

fn hash32(bytes: &[u8]) -> [u8; 32] {
    Sha256::digest(bytes).into()
}

pub fn protected_session_id_for_native(session_id: &str) -> [u8; 32] {
    hash32(session_id.as_bytes())
}

fn domain_hash32(value: &str, domain: &str) -> [u8; 32] {
    hash32(format!("{domain}:{value}").as_bytes())
}

fn unavailable() -> HookDispatch {
    HookDispatch::Unavailable(HookTransportDispositionV1::CatchupRequired)
}

#[cfg(test)]
mod tests;

#[cfg(test)]
mod start_locator_tests {
    use super::*;

    fn start(session: &str) -> HookEventEnvelopeV2 {
        HookEventEnvelopeV2 {
            schema_version: tracedecay_hooks::HOOK_EVENT_SCHEMA_VERSION,
            event_id: [1; 16],
            producer: HookHostV1::ClaudeCode,
            protected_session_id: protected_session_id_for_native(session),
            project_id: [2; 16],
            repository_id: [3; 16],
            worktree_id: [4; 16],
            worktree_epoch: 1,
            binding_token: [5; 32],
            ordering: tracedecay_hooks::HookOrderingV1::Unknown,
            observed_at: UtcMicros(1),
            event: tracedecay_hooks::HookEventV2::SessionBoundary {
                boundary: tracedecay_hooks::HookBoundaryV1::Start,
            },
        }
    }

    #[test]
    fn private_start_locator_is_bound_to_native_session_event_and_host() {
        let envelope = start("session-one");
        let fields: NativeIdentityFields = serde_json::from_value(serde_json::json!({
            "session_id": "session-one", "transcript_path": "/native/session-one.jsonl"
        }))
        .unwrap();
        let locator = native_session_start_locator(&fields, &envelope).unwrap();
        assert!(locator.matches_envelope(&envelope));
        for changed in [
            HookEventEnvelopeV2 {
                producer: HookHostV1::Codex,
                ..envelope.clone()
            },
            HookEventEnvelopeV2 {
                event_id: [9; 16],
                ..envelope.clone()
            },
            start("session-two"),
            HookEventEnvelopeV2 {
                event: tracedecay_hooks::HookEventV2::SessionBoundary {
                    boundary: tracedecay_hooks::HookBoundaryV1::TurnComplete,
                },
                ..envelope.clone()
            },
        ] {
            assert!(!locator.matches_envelope(&changed));
        }
        let encoded = serde_json::to_value(&locator).unwrap();
        let decoded: NativeSessionStartLocatorV1 = serde_json::from_value(encoded.clone()).unwrap();
        assert_eq!(decoded, locator);
        let mut open = encoded;
        open["authority"] = serde_json::json!("caller-claimed");
        assert!(serde_json::from_value::<NativeSessionStartLocatorV1>(open).is_err());
    }

    #[test]
    fn start_locator_rejects_missing_relative_and_oversized_paths() {
        for path in [
            None,
            Some("relative/session-one.jsonl".to_owned()),
            Some(format!("/{}", "x".repeat(4096))),
        ] {
            let fields: NativeIdentityFields = serde_json::from_value(serde_json::json!({
                "session_id": "session-one", "transcript_path": path
            }))
            .unwrap();
            assert!(native_session_start_locator(&fields, &start("session-one")).is_none());
        }
    }

    struct SlowOptionalDelivery {
        required_done: std::sync::atomic::AtomicBool,
        delivery_attempted: std::sync::atomic::AtomicBool,
    }

    fn prepared_for_test(
        project: &Path,
        profile: &Path,
        mut envelope: HookEventEnvelopeV2,
    ) -> PreparedBoundHook {
        let now = now_utc();
        envelope.observed_at = now;
        let binding = HookScopeBindingV1 {
            host: envelope.producer,
            project_id: envelope.project_id,
            repository_id: envelope.repository_id,
            worktree_id: envelope.worktree_id,
            worktree_epoch: envelope.worktree_epoch,
            binding_token: envelope.binding_token,
            capabilities: vec![tracedecay_hooks::HookCapabilityV1 {
                family: envelope.event.family(),
                support: tracedecay_hooks::HookEventSupportV1::Native,
            }],
        };
        PreparedBoundHook {
            host: envelope.producer,
            layout: tracedecay_runtime_core::storage::default_profile_sharded_layout(
                project, profile,
            )
            .unwrap(),
            snapshot: HookConfigurationSnapshotV1 {
                schema_version: tracedecay_hooks::HOOK_CONFIGURATION_SCHEMA_VERSION,
                revision: 1,
                published_at: now,
                expires_at: UtcMicros(now.0 + 60_000_000),
                binding,
            },
            envelope,
            replayed: false,
            native_session_id: Some("session-one".to_owned()),
            native_lifecycle: None,
            native_start_locator: None,
            prepared_at: now,
        }
    }

    fn binding_refresh_calls(project: &Path) -> Vec<serde_json::Value> {
        std::fs::read(project.join("binding-refresh-calls.json"))
            .ok()
            .map(|bytes| serde_json::from_slice(&bytes).unwrap())
            .unwrap_or_default()
    }

    fn binding_refresh_fixture(
        project: &Path,
        prepared: &PreparedBoundHook,
        current: Option<HookConfigurationSnapshotV1>,
        reason: Option<&str>,
        retry_reason: Option<&str>,
        first_delay_millis: u64,
    ) {
        let path =
            tracedecay_hooks::hook_configuration_path(&prepared.layout.data_root, prepared.host);
        std::fs::create_dir_all(&prepared.layout.data_root).unwrap();
        tracedecay_hooks::HookConfigurationPublisherV1::new(
            tracedecay_hooks::HookConfigurationFileWriterV1::new(&path),
        )
        .publish(prepared.snapshot.clone())
        .unwrap();
        std::fs::write(
            project.join("binding-refresh.json"),
            serde_json::to_vec(&serde_json::json!({
                "configuration_path": path, "current": current, "reason": reason,
                "retry_reason": retry_reason, "first_delay_millis": first_delay_millis,
            }))
            .unwrap(),
        )
        .unwrap();
    }

    /// The existing runtime transport seam republishes the real typed config
    /// during the first admission, then validates the retried envelope against it.
    fn binding_refresh_daemon_tool<'a>(
        project: Option<&'a Path>,
        tool: &'a str,
        arguments: serde_json::Value,
        initialized: bool,
    ) -> std::pin::Pin<
        Box<
            dyn std::future::Future<Output = tracedecay_domain::errors::Result<serde_json::Value>>
                + Send
                + 'a,
        >,
    > {
        Box::pin(async move {
            assert_eq!(tool, "tracedecay_hook_runtime");
            assert!(initialized);
            assert_eq!(arguments["action"], "hook_v2_admit");
            let project = project.unwrap();
            let fixture: serde_json::Value = serde_json::from_slice(
                &std::fs::read(project.join("binding-refresh.json")).unwrap(),
            )
            .unwrap();
            let path = Path::new(fixture["configuration_path"].as_str().unwrap());
            let mut calls = binding_refresh_calls(project);
            calls.push(arguments.clone());
            std::fs::write(
                project.join("binding-refresh-calls.json"),
                serde_json::to_vec(&calls).unwrap(),
            )
            .unwrap();
            if calls.len() == 1 {
                if fixture["current"].is_null() {
                    std::fs::remove_file(path).unwrap();
                } else {
                    let snapshot: HookConfigurationSnapshotV1 =
                        serde_json::from_value(fixture["current"].clone()).unwrap();
                    tracedecay_hooks::HookConfigurationPublisherV1::new(
                        tracedecay_hooks::HookConfigurationFileWriterV1::new(path),
                    )
                    .publish(snapshot)
                    .unwrap();
                }
                // A blocking transport response models a reply observed after
                // the original deadline, even if its future polls ready first.
                std::thread::sleep(Duration::from_millis(
                    fixture["first_delay_millis"].as_u64().unwrap(),
                ));
                return Ok(serde_json::json!({
                    "action": "hook_v2_admit", "status": "rejected",
                    "disposition": "catchup_required", "reason": fixture["reason"],
                }));
            }
            assert_eq!(calls.len(), 2, "binding refresh must never loop");
            let envelope: HookEventEnvelopeV2 =
                serde_json::from_value(arguments["envelope"].clone()).unwrap();
            let subscriber =
                HookConfigurationSubscriberV1::new(HookConfigurationFileReaderV1::new(path));
            let HookConfigurationReadOutcomeV1::Bound(current) =
                subscriber.load_current(envelope.producer, now_utc())
            else {
                panic!("retry requires a current typed binding")
            };
            envelope.validate(&current.binding).unwrap();
            if !fixture["retry_reason"].is_null() {
                return Ok(serde_json::json!({
                    "action": "hook_v2_admit", "status": "rejected",
                    "disposition": "catchup_required", "reason": fixture["retry_reason"],
                }));
            }
            Ok(serde_json::json!({
                "action": "hook_v2_admit", "status": "accepted", "disposition": "accepted",
                "github_stack_signal_available": true,
            }))
        })
    }

    async fn dispatch_binding_refresh_fixture(
        prepared: PreparedBoundHook,
        project: &Path,
        started: Instant,
    ) -> (HookDispatch, Vec<serde_json::Value>) {
        use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
        let runtime = HookRuntimeV1 {
            daemon_tool: binding_refresh_daemon_tool,
            ..crate::ports::hook_runtime::crate_test_runtime()
        };
        let session = prepared.native_session_id.clone();
        let lifecycle = prepared.native_lifecycle.clone();
        let locator = prepared.native_start_locator.clone();
        let admission = DaemonAdmissionPort::new(
            &runtime,
            project,
            session.as_deref(),
            lifecycle.as_ref(),
            locator.as_ref(),
            None,
        );
        let delivery = SlowOptionalDelivery {
            required_done: AtomicBool::new(false),
            delivery_attempted: AtomicBool::new(false),
        };
        let required_calls = AtomicUsize::new(0);
        let (dispatched, ()) = dispatch_decoded_with_required_work(
            &runtime,
            prepared,
            project,
            started,
            &admission,
            &delivery,
            async {
                required_calls.fetch_add(1, Ordering::SeqCst);
                delivery.required_done.store(true, Ordering::SeqCst);
            },
        )
        .await;
        assert_eq!(required_calls.load(Ordering::SeqCst), 1);
        assert!(!delivery.delivery_attempted.load(Ordering::SeqCst));
        (dispatched, binding_refresh_calls(project))
    }

    #[tokio::test]
    async fn native_start_refreshes_one_new_epoch_without_changing_the_original_event() {
        for host in [HookHostV1::ClaudeCode, HookHostV1::Codex] {
            let project = tempfile::tempdir().unwrap();
            let profile = tempfile::tempdir().unwrap();
            let mut envelope = start("session-one");
            envelope.producer = host;
            let mut prepared = prepared_for_test(project.path(), profile.path(), envelope);
            let fields: NativeIdentityFields = serde_json::from_value(serde_json::json!({
                "session_id": "session-one", "transcript_path": project.path().join("session-one.jsonl"),
            })).unwrap();
            prepared.native_start_locator =
                native_session_start_locator(&fields, &prepared.envelope);
            assert_eq!(
                prepared.native_start_locator.is_some(),
                host == HookHostV1::ClaudeCode,
            );
            let original = prepared.envelope.clone();
            let mut current = prepared.snapshot.clone();
            current.revision += 1;
            current.binding.worktree_epoch += 1;
            binding_refresh_fixture(
                project.path(),
                &prepared,
                Some(current.clone()),
                Some("binding_changed"),
                None,
                0,
            );
            let (dispatched, calls) = dispatch_binding_refresh_fixture(
                prepared,
                project.path(),
                Instant::now() - Duration::from_millis(150),
            )
            .await;
            assert!(
                matches!(
                    dispatched,
                    HookDispatch::Handled {
                        disposition: HookTransportDispositionV1::Accepted,
                        guidance: None,
                    }
                ),
                "{host:?}"
            );
            assert_eq!(calls.len(), 2);
            assert_eq!(
                calls[0]["envelope"],
                serde_json::to_value(&original).unwrap()
            );
            let mut expected = calls[0].clone();
            expected["envelope"]["worktree_epoch"] =
                serde_json::json!(current.binding.worktree_epoch);
            assert_eq!(
                calls[1], expected,
                "only the epoch changes, including side-channel identity"
            );
        }
    }

    #[tokio::test]
    async fn native_start_refuses_changed_identity_and_nonadvancing_or_missing_bindings() {
        for host in [HookHostV1::ClaudeCode, HookHostV1::Codex] {
            for case in [
                "host",
                "project",
                "repository",
                "worktree",
                "token",
                "capabilities",
                "revision",
                "epoch",
                "missing",
                "expired",
            ] {
                let project = tempfile::tempdir().unwrap();
                let profile = tempfile::tempdir().unwrap();
                let mut envelope = start("session-one");
                envelope.producer = host;
                let prepared = prepared_for_test(project.path(), profile.path(), envelope);
                let mut current = prepared.snapshot.clone();
                current.revision += 1;
                current.binding.worktree_epoch += 1;
                match case {
                    "host" => {
                        current.binding.host = if host == HookHostV1::ClaudeCode {
                            HookHostV1::Codex
                        } else {
                            HookHostV1::ClaudeCode
                        }
                    }
                    "project" => current.binding.project_id = [9; 16],
                    "repository" => current.binding.repository_id = [9; 16],
                    "worktree" => current.binding.worktree_id = [9; 16],
                    "token" => current.binding.binding_token = [9; 32],
                    "capabilities" => {
                        current.binding.capabilities = vec![tracedecay_hooks::HookCapabilityV1 {
                            family: tracedecay_hooks::HookEventFamily::ToolLifecycle,
                            support: tracedecay_hooks::stock_event_support(
                                host,
                                tracedecay_hooks::HookEventFamily::ToolLifecycle,
                            ),
                        }]
                    }
                    "revision" => current.revision = prepared.snapshot.revision,
                    "epoch" => {
                        current.binding.worktree_epoch = prepared.snapshot.binding.worktree_epoch
                    }
                    "expired" => {
                        current.published_at = UtcMicros(now_utc().0 - 2_000_000);
                        current.expires_at = UtcMicros(now_utc().0 - 1_000_000);
                    }
                    "missing" => {}
                    _ => unreachable!(),
                }
                binding_refresh_fixture(
                    project.path(),
                    &prepared,
                    (case != "missing").then_some(current),
                    Some("binding_changed"),
                    None,
                    0,
                );
                let (dispatched, calls) = dispatch_binding_refresh_fixture(
                    prepared,
                    project.path(),
                    Instant::now() - Duration::from_millis(150),
                )
                .await;
                assert_eq!(calls.len(), 1, "{host:?} {case}");
                assert!(
                    matches!(
                        dispatched,
                        HookDispatch::Handled {
                            disposition: HookTransportDispositionV1::CatchupRequired,
                            guidance: None,
                        }
                    ),
                    "{host:?} {case}"
                );
            }
        }
    }

    #[tokio::test]
    async fn conflict_unspecified_rejection_stop_and_pending_start_never_refresh() {
        for host in [HookHostV1::ClaudeCode, HookHostV1::Codex] {
            for case in ["conflict", "unspecified", "stop", "pending"] {
                let project = tempfile::tempdir().unwrap();
                let profile = tempfile::tempdir().unwrap();
                let mut envelope = start("session-one");
                envelope.producer = host;
                if case == "stop" {
                    envelope.event = tracedecay_hooks::HookEventV2::SessionBoundary {
                        boundary: tracedecay_hooks::HookBoundaryV1::TurnComplete,
                    };
                }
                let mut prepared = prepared_for_test(project.path(), profile.path(), envelope);
                prepared.replayed = case == "pending";
                let mut current = prepared.snapshot.clone();
                current.revision += 1;
                current.binding.worktree_epoch += 1;
                let reason = match case {
                    "conflict" => Some("admission_identity_conflict"),
                    "unspecified" => None,
                    _ => Some("binding_changed"),
                };
                binding_refresh_fixture(project.path(), &prepared, Some(current), reason, None, 0);
                let (dispatched, calls) = dispatch_binding_refresh_fixture(
                    prepared,
                    project.path(),
                    Instant::now() - Duration::from_millis(150),
                )
                .await;
                assert_eq!(calls.len(), 1, "{host:?} {case}");
                assert!(
                    matches!(
                        dispatched,
                        HookDispatch::Handled {
                            disposition: HookTransportDispositionV1::CatchupRequired,
                            guidance: None,
                        }
                    ),
                    "{host:?} {case}"
                );
            }
        }
    }

    #[tokio::test]
    async fn a_second_binding_rejection_is_terminal_and_never_starts_a_third_attempt() {
        let project = tempfile::tempdir().unwrap();
        let profile = tempfile::tempdir().unwrap();
        let prepared = prepared_for_test(project.path(), profile.path(), start("session-one"));
        let mut current = prepared.snapshot.clone();
        current.revision += 1;
        current.binding.worktree_epoch += 1;
        binding_refresh_fixture(
            project.path(),
            &prepared,
            Some(current),
            Some("binding_changed"),
            Some("binding_changed"),
            0,
        );
        let (dispatched, calls) = dispatch_binding_refresh_fixture(
            prepared,
            project.path(),
            Instant::now() - Duration::from_millis(150),
        )
        .await;
        assert_eq!(calls.len(), 2);
        assert!(matches!(
            dispatched,
            HookDispatch::Handled {
                disposition: HookTransportDispositionV1::CatchupRequired,
                guidance: None,
            }
        ));
    }

    #[tokio::test]
    async fn binding_rejection_after_the_original_deadline_has_no_retry_or_guidance() {
        let project = tempfile::tempdir().unwrap();
        let profile = tempfile::tempdir().unwrap();
        let prepared = prepared_for_test(project.path(), profile.path(), start("session-one"));
        let mut current = prepared.snapshot.clone();
        current.revision += 1;
        current.binding.worktree_epoch += 1;
        binding_refresh_fixture(
            project.path(),
            &prepared,
            Some(current),
            Some("binding_changed"),
            None,
            900,
        );
        let started = Instant::now() - Duration::from_millis(150);
        let (dispatched, calls) =
            dispatch_binding_refresh_fixture(prepared, project.path(), started).await;
        assert!(Instant::now() >= native_lifecycle_deadline(started));
        assert_eq!(
            calls.len(),
            1,
            "an expired original deadline cannot be replaced"
        );
        assert!(matches!(
            dispatched,
            HookDispatch::Handled {
                disposition: HookTransportDispositionV1::CatchupRequired,
                guidance: None,
            }
        ));
    }

    #[tokio::test]
    async fn bound_native_lifecycles_admit_after_guidance_expiry_without_late_output() {
        use std::sync::atomic::{AtomicBool, Ordering};
        use tracedecay_hooks::{HookBoundaryV1, HookEventV2};

        let runtime = crate::ports::hook_runtime::crate_test_runtime();
        for host in [HookHostV1::ClaudeCode, HookHostV1::Codex] {
            for boundary in [HookBoundaryV1::Start, HookBoundaryV1::TurnComplete] {
                let project = tempfile::tempdir().unwrap();
                let profile = tempfile::tempdir().unwrap();
                let mut envelope = start("session-one");
                envelope.producer = host;
                envelope.event = HookEventV2::SessionBoundary { boundary };
                let prepared = prepared_for_test(project.path(), profile.path(), envelope);
                let guard = super::super::TestDaemonHookActionGuard::install([serde_json::json!({
                    "action": "hook_v2_admit", "status": "accepted", "disposition": "accepted",
                    "github_stack_signal_available": true,
                })]);
                let admission = DaemonAdmissionPort::new(
                    &runtime,
                    project.path(),
                    Some("session-one"),
                    None,
                    None,
                    None,
                );
                let delivery = SlowOptionalDelivery {
                    required_done: AtomicBool::new(false),
                    delivery_attempted: AtomicBool::new(false),
                };
                let started = Instant::now() - Duration::from_millis(150);
                assert!(admission_window_after_elapsed(elapsed_us(started)).is_none());
                let (dispatched, required_done) = dispatch_decoded_with_required_work(
                    &runtime,
                    prepared,
                    project.path(),
                    started,
                    &admission,
                    &delivery,
                    async {
                        assert_eq!(
                            guard.calls().len(),
                            1,
                            "live admission must precede required work"
                        );
                        delivery.required_done.store(true, Ordering::SeqCst);
                        true
                    },
                )
                .await;
                assert!(required_done);
                assert!(
                    matches!(
                        dispatched,
                        HookDispatch::Handled {
                            disposition: HookTransportDispositionV1::Accepted,
                            guidance: None,
                        }
                    ),
                    "{host:?} {boundary:?}"
                );
                assert!(!delivery.delivery_attempted.load(Ordering::SeqCst));
            }
        }
    }

    #[tokio::test]
    async fn lifecycle_deadline_preserves_generic_expiry_and_codex_enqueue_reserve() {
        use tracedecay_hooks::{HookBoundaryV1, HookEventV2};

        let runtime = crate::ports::hook_runtime::crate_test_runtime();
        let project = tempfile::tempdir().unwrap();
        let guard = super::super::TestDaemonHookActionGuard::install([]);
        let admission = DaemonAdmissionPort::new(
            &runtime,
            project.path(),
            Some("session-one"),
            None,
            None,
            None,
        );
        let started = Instant::now();
        let mut envelope = start("session-one");
        assert_eq!(
            bound_lifecycle_admission_deadline(&envelope, started),
            Some(started + Duration::from_secs(1))
        );
        envelope.producer = HookHostV1::Codex;
        envelope.event = HookEventV2::SessionBoundary {
            boundary: HookBoundaryV1::TurnComplete,
        };
        let cutoff = bound_lifecycle_admission_deadline(&envelope, started).unwrap();
        assert_eq!(
            native_lifecycle_deadline(started) - cutoff,
            Duration::from_millis(100)
        );
        assert!(matches!(
            admission
                .try_admit_lifecycle(&envelope, Instant::now() - Duration::from_millis(1))
                .await,
            HookImmediateAdmissionV1::TimedOut,
        ));
        for host in [
            HookHostV1::ClaudeCode,
            HookHostV1::Codex,
            HookHostV1::Hermes,
        ] {
            envelope.producer = host;
            envelope.event = HookEventV2::PromptBoundary;
            assert!(bound_lifecycle_admission_deadline(&envelope, started).is_none());
        }
        envelope.producer = HookHostV1::Hermes;
        envelope.event = HookEventV2::SessionBoundary {
            boundary: HookBoundaryV1::Start,
        };
        assert!(bound_lifecycle_admission_deadline(&envelope, started).is_none());
        assert!(admission_window_after_elapsed(150_000).is_none());
        assert!(
            guard.calls().is_empty(),
            "expired lifecycle must not reach transport"
        );
    }

    fn native_claude_stop(path: &Path) -> (String, NativeIdentityFields) {
        let payload = serde_json::json!({
            "hook_event_name": "Stop", "session_id": "session-one",
            "transcript_path": path, "cwd": "/workspace", "stop_hook_active": false,
            "permission_mode": "default", "last_assistant_message": "finished"
        })
        .to_string();
        let fields: NativeIdentityFields = serde_json::from_str(&payload).unwrap();
        assert!(
            fields.event_key().is_none(),
            "fixture must preserve the real keyless Stop shape"
        );
        (payload, fields)
    }

    #[test]
    fn native_claude_stop_checkpoint_distinguishes_append_and_keeps_exact_pending_replay() {
        use std::io::Write;
        let home = tempfile::tempdir().unwrap();
        let directory = home.path().join(".claude/projects/workspace");
        std::fs::create_dir_all(&directory).unwrap();
        let path = directory.join("session-one.jsonl");
        std::fs::write(&path, b"{\"type\":\"user\",\"message\":\"first\"}\n").unwrap();
        let (payload, fields) = native_claude_stop(&path);
        let decoded =
            tracedecay_hooks::decode_native_hook_event(HookHostV1::ClaudeCode, payload.as_bytes())
                .unwrap();
        let now = now_utc();
        let native = native_material(&fields, decoded.family(), payload.as_bytes(), now).unwrap();
        let checkpoint_id = || {
            claude_stop_checkpoint_event_id(
                &fields,
                native.event_id,
                home.path(),
                native_lifecycle_deadline(Instant::now()),
            )
            .unwrap()
        };
        let first = checkpoint_id();
        assert_eq!(
            first,
            checkpoint_id(),
            "unchanged bytes retain their identity"
        );
        assert_ne!(
            first, native.event_id,
            "checkpoint identity has its own domain"
        );
        let mut file = std::fs::OpenOptions::new()
            .append(true)
            .open(&path)
            .unwrap();
        file.write_all(b"{\"type\":\"assistant\",\"message\":\"second\"}\n")
            .unwrap();
        file.sync_all().unwrap();
        let second = checkpoint_id();
        assert_ne!(
            first, second,
            "an identical native Stop payload must seal a later complete append"
        );
        assert_eq!(second, checkpoint_id());

        let project = tempfile::tempdir().unwrap();
        let profile = tempfile::tempdir().unwrap();
        let prepared = prepared_for_test(project.path(), profile.path(), start("session-one"));
        let mut material = native;
        material.event_id = second;
        let envelope = decoded
            .into_envelope(&prepared.snapshot.binding, material)
            .unwrap();
        assert_eq!(
            append_for_replay(
                &prepared.layout.data_root,
                HookHostV1::ClaudeCode,
                &envelope,
                &prepared.snapshot.binding,
                now
            ),
            SpoolAppendOutcomeV1::Accepted
        );
        let mut retry = envelope.clone();
        retry.observed_at = UtcMicros(now.0 + 1);
        assert_eq!(
            replay_envelope_if_pending(
                &prepared.layout.data_root,
                HookHostV1::ClaudeCode,
                &prepared.snapshot.binding,
                &retry,
                now
            ),
            PendingEnvelopeV1::Exact(envelope)
        );
        retry.event_id = first;
        assert_eq!(
            replay_envelope_if_pending(
                &prepared.layout.data_root,
                HookHostV1::ClaudeCode,
                &prepared.snapshot.binding,
                &retry,
                now
            ),
            PendingEnvelopeV1::Missing
        );
    }

    #[test]
    fn native_claude_stop_checkpoint_refuses_unavailable_unsafe_and_expired_sources() {
        let home = tempfile::tempdir().unwrap();
        let directory = home.path().join(".claude/projects/workspace");
        std::fs::create_dir_all(&directory).unwrap();
        let path = directory.join("session-one.jsonl");
        std::fs::write(&path, b"{}\n").unwrap();
        let (_, mut fields) = native_claude_stop(&path);
        let check = |fields: &NativeIdentityFields, deadline| {
            claude_stop_checkpoint_event_id(fields, [1; 16], home.path(), deadline)
        };
        assert!(check(&fields, Instant::now()).is_none());
        fields.session_id = Some("another-session".to_owned());
        assert!(check(&fields, native_lifecycle_deadline(Instant::now())).is_none());
        fields.session_id = Some("session-one".to_owned());
        fields.transcript_path = None;
        assert!(check(&fields, native_lifecycle_deadline(Instant::now())).is_none());
        std::fs::write(home.path().join("session-one.jsonl"), b"{}\n").unwrap();
        for candidate in [
            home.path().join("session-one.jsonl"),
            std::path::PathBuf::from("session-one.jsonl"),
        ] {
            fields.transcript_path = Some(candidate.to_string_lossy().into_owned());
            assert!(check(&fields, native_lifecycle_deadline(Instant::now())).is_none());
        }
        fields.transcript_path = Some(path.to_str().unwrap().to_owned());
        std::fs::OpenOptions::new()
            .write(true)
            .open(&path)
            .unwrap()
            .set_len(16 * 1024 * 1024 + 1)
            .unwrap();
        assert!(check(&fields, native_lifecycle_deadline(Instant::now())).is_none());
        std::fs::remove_file(&path).unwrap();
        assert!(check(&fields, native_lifecycle_deadline(Instant::now())).is_none());
        std::fs::create_dir(&path).unwrap();
        assert!(check(&fields, native_lifecycle_deadline(Instant::now())).is_none());
        std::fs::remove_dir(&path).unwrap();
        #[cfg(unix)]
        {
            let target = directory.join("target.jsonl");
            std::fs::write(&target, b"{}\n").unwrap();
            std::os::unix::fs::symlink(&target, &path).unwrap();
            assert!(check(&fields, native_lifecycle_deadline(Instant::now())).is_none());
        }
    }

    impl
        AsyncHookFeedbackDeliveryPortV1<
            tracedecay_application::advisory::AdvisoryHookLookupNoticeV1,
        > for SlowOptionalDelivery
    {
        fn deliver_hook_v2<'a>(
            &'a self,
            _envelope: &'a HookEventEnvelopeV2,
            _feedback: &'a tracedecay_application::advisory::AdvisoryHookLookupNoticeV1,
            deadline: HookSynchronousDeadlineV1,
        ) -> tracedecay_hooks::HookDeliveryFutureV1<'a> {
            Box::pin(async move {
                use std::sync::atomic::Ordering;
                assert!(
                    self.required_done.load(Ordering::SeqCst),
                    "optional delivery cannot precede required producer work"
                );
                self.delivery_attempted.store(true, Ordering::SeqCst);
                tokio::time::sleep(Duration::from_micros(deadline.remaining_micros())).await;
                tracedecay_hooks::HookFeedbackDeliveryOutcomeV1::Unavailable
            })
        }

        fn deliver_legacy<'a>(
            &'a self,
            _envelope: &'a HookEventEnvelopeV2,
            _feedback: &'a tracedecay_application::advisory::AdvisoryHookLookupNoticeV1,
            _deadline: HookSynchronousDeadlineV1,
        ) -> tracedecay_hooks::HookDeliveryFutureV1<'a> {
            Box::pin(async { tracedecay_hooks::HookFeedbackDeliveryOutcomeV1::Unavailable })
        }
    }

    #[tokio::test]
    async fn slow_optional_delivery_cannot_starve_required_stop_enqueue() {
        use std::sync::atomic::{AtomicBool, Ordering};
        use tracedecay_domain::feedback::{FeedbackCycleId, FeedbackResultId, FeedbackScopeV1};
        use tracedecay_domain::{
            CodeGenerationId, CommitId, ManifestDigest, RepositoryId, WorktreeId,
        };

        let project = tempfile::tempdir().unwrap();
        let profile = tempfile::tempdir().unwrap();
        let runtime = crate::ports::hook_runtime::crate_test_runtime();
        let layout = tracedecay_runtime_core::storage::default_profile_sharded_layout(
            project.path(),
            profile.path(),
        )
        .unwrap();
        let notice = tracedecay_application::advisory::AdvisoryHookLookupNoticeV1 {
            scope: FeedbackScopeV1 {
                project_id: ProjectId::new("project.required-stop").unwrap(),
                repository_id: RepositoryId::new("repository.required-stop").unwrap(),
                worktree_id: WorktreeId::new("worktree.required-stop").unwrap(),
                branch_ref: "refs/heads/main".to_owned(),
                head_commit_id: CommitId::new("a".repeat(40)).unwrap(),
            },
            result_id: FeedbackResultId::new("result.required-stop").unwrap(),
            cycle_id: FeedbackCycleId::new("cycle.required-stop").unwrap(),
            generation_id: CodeGenerationId::new("generation.required-stop").unwrap(),
            generation_digest: ManifestDigest::new(format!("sha256:{}", "b".repeat(64))).unwrap(),
            returned_findings: 1,
            omitted_findings: 0,
        };
        let now = now_utc();
        let mut envelope = start("session-one");
        envelope.producer = HookHostV1::Codex;
        envelope.project_id = envelope_identity_hash16("project", notice.scope.project_id.as_str());
        envelope.repository_id =
            envelope_identity_hash16("repository", notice.scope.repository_id.as_str());
        envelope.worktree_id =
            envelope_identity_hash16("worktree", notice.scope.worktree_id.as_str());
        envelope.observed_at = now;
        envelope.event = tracedecay_hooks::HookEventV2::SessionBoundary {
            boundary: tracedecay_hooks::HookBoundaryV1::TurnComplete,
        };
        let snapshot = HookConfigurationSnapshotV1 {
            schema_version: tracedecay_hooks::HOOK_CONFIGURATION_SCHEMA_VERSION,
            revision: 1,
            published_at: now,
            expires_at: UtcMicros(now.0 + 1_000_000),
            binding: HookScopeBindingV1 {
                host: envelope.producer,
                project_id: envelope.project_id,
                repository_id: envelope.repository_id,
                worktree_id: envelope.worktree_id,
                worktree_epoch: envelope.worktree_epoch,
                binding_token: envelope.binding_token,
                capabilities: vec![tracedecay_hooks::HookCapabilityV1 {
                    family: tracedecay_hooks::HookEventFamily::SessionBoundary,
                    support: tracedecay_hooks::HookEventSupportV1::Native,
                }],
            },
        };
        let prepared = PreparedBoundHook {
            host: HookHostV1::Codex,
            layout,
            snapshot,
            envelope,
            replayed: false,
            native_session_id: Some("session-one".to_owned()),
            native_lifecycle: None,
            native_start_locator: None,
            prepared_at: now,
        };
        let guard = crate::hooks::TestDaemonHookActionGuard::install([
            serde_json::json!({
                "action": "hook_v2_admit", "status": "accepted", "disposition": "accepted",
                "orchestration": null, "ready_guidance": null, "feedback_notice": notice,
                "reason": null,
            }),
            serde_json::json!({"queued": true}),
        ]);
        let admission = DaemonAdmissionPort::new(
            &runtime,
            project.path(),
            Some("session-one"),
            None,
            None,
            None,
        );
        let delivery = SlowOptionalDelivery {
            required_done: AtomicBool::new(false),
            delivery_attempted: AtomicBool::new(false),
        };
        let started = Instant::now();
        let (dispatched, queued) = dispatch_decoded_with_required_work(
            &runtime,
            prepared,
            project.path(),
            started,
            &admission,
            &delivery,
            async {
                assert_eq!(
                    guard.calls().len(),
                    1,
                    "live admission must precede enqueue"
                );
                assert!(HookSynchronousDeadlineV1::after_elapsed(elapsed_us(started)).is_some());
                let response = super::super::daemon_hook_action(
                    &runtime,
                    Some(project.path()),
                    serde_json::json!({"action": "codex_stop", "session_id": "session-one"}),
                    None,
                )
                .await
                .unwrap();
                delivery.required_done.store(true, Ordering::SeqCst);
                response["queued"] == true
            },
        )
        .await;
        assert!(queued);
        assert!(delivery.delivery_attempted.load(Ordering::SeqCst));
        assert!(matches!(dispatched, HookDispatch::Handled { .. }));
        assert_eq!(guard.calls()[1].1["action"], "codex_stop");
        assert!(
            HookSynchronousDeadlineV1::after_elapsed(elapsed_us(started)).is_none(),
            "slow optional delivery consumes only the original remaining deadline"
        );
    }
}
