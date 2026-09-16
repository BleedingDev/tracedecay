//! Runner-prepared, one-shot action authority for Native comparison traffic.
//!
//! The comparison runner owns the proof key. It sends that key only over the
//! already authenticated daemon connection while preparing an action. The
//! daemon retains the key in memory until the matching `tools/call` reaches
//! terminal dispatch, then drops it after issuing one HMAC receipt. Nothing in
//! this module serializes a key or includes it in a `Debug` representation.

use std::collections::HashMap;
use std::fmt;
use std::sync::{Arc, Mutex, OnceLock, Weak};

use hmac::{Hmac, KeyInit, Mac};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use sha2::{Digest, Sha256};
use zeroize::Zeroize;

/// Wire and receipt discriminator. Keep this stable: the runner uses it to
/// reject a response from a different authority protocol.
pub const ACTION_RECEIPT_FORMAT: &str = "tracedecay.native-original.daemon-action-receipt.v1";
/// The sole action-receipt wire revision currently supported.
pub const ACTION_RECEIPT_REVISION: u32 = 1;
/// `_meta` key on the authenticated `initialize` control exchange.
pub const ACTION_PREPARE_META_KEY: &str = "nativeOriginalActionPrepare";
/// `_meta` key carried by the child `tools/call`.
pub const ACTION_NONCE_META_KEY: &str = "nativeOriginalActionNonce";
/// `_meta` key used by the daemon's terminal response.
pub const ACTION_RECEIPT_META_KEY: &str = "nativeOriginalActionReceipt";
/// The only daemon entrypoint currently capable of carrying the action proof.
/// This is observed from the authenticated MCP transport, rather than trusted
/// from the caller's receipt metadata.
pub const ACTION_ENTRYPOINT_MCP_STDIO: &str = "mcp_stdio";
/// Maximum retained action lifetime. A runner action must be short-lived;
/// this also bounds memory retained by a client that never dispatches.
pub const MAX_ACTION_RECEIPT_TTL_MICROS: i64 = 5 * 60 * 1_000_000;
const HEX_BYTES: usize = 32;
const MAX_TEXT_BYTES: usize = 16 * 1024;

type HmacSha256 = Hmac<Sha256>;

/// A 256-bit proof key that cannot accidentally be copied into a wire DTO.
/// The custom formatter is deliberately redacted and `Drop` clears the key.
pub struct ProofKey([u8; HEX_BYTES]);

impl ProofKey {
    fn from_hex(value: &str) -> Result<Self, ActionReceiptError> {
        if !is_lower_hex_64(value) {
            return Err(ActionReceiptError::InvalidField("proof_key"));
        }
        let mut bytes = [0_u8; HEX_BYTES];
        hex::decode_to_slice(value, &mut bytes)
            .map_err(|_| ActionReceiptError::InvalidField("proof_key"))?;
        Ok(Self(bytes))
    }

    /// Construct a key for deterministic tests without exposing a formatter
    /// that could print it. Production callers should use the wire parser.
    pub fn from_bytes(bytes: [u8; HEX_BYTES]) -> Self {
        Self(bytes)
    }

    fn as_bytes(&self) -> &[u8] {
        &self.0
    }
}

impl fmt::Debug for ProofKey {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("ProofKey([REDACTED])")
    }
}

impl Drop for ProofKey {
    fn drop(&mut self) {
        self.0.zeroize();
    }
}

/// Daemon generation identity captured from the live authority record.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct DaemonGeneration {
    pub epoch: u64,
    pub process_run_id: String,
}

impl DaemonGeneration {
    pub fn new(epoch: u64, process_run_id: impl Into<String>) -> Result<Self, ActionReceiptError> {
        let process_run_id = process_run_id.into();
        if epoch == 0 {
            return Err(ActionReceiptError::InvalidField("daemon_generation.epoch"));
        }
        validate_text(&process_run_id, "daemon_generation.process_run_id")?;
        Ok(Self {
            epoch,
            process_run_id,
        })
    }
}

/// Physical identity of the live store selected by terminal dispatch.
///
/// These coordinates are obtained from the mounted `TraceDecay` instance,
/// never from action metadata. `project_root` is also the canonical action
/// scope used by the comparison protocol.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct LiveStoreIdentity {
    pub project_id: Option<String>,
    pub project_root: String,
    pub data_root: String,
    pub graph_db_path: String,
    pub serving_branch: Option<String>,
}

impl LiveStoreIdentity {
    pub fn new(
        project_id: Option<String>,
        project_root: impl Into<String>,
        data_root: impl Into<String>,
        graph_db_path: impl Into<String>,
        serving_branch: Option<String>,
    ) -> Result<Self, ActionReceiptError> {
        let project_root = project_root.into();
        let data_root = data_root.into();
        let graph_db_path = graph_db_path.into();
        validate_text(&project_root, "store_identity.project_root")?;
        validate_text(&data_root, "store_identity.data_root")?;
        validate_text(&graph_db_path, "store_identity.graph_db_path")?;
        validate_optional_text(project_id.as_deref(), "store_identity.project_id")?;
        validate_optional_text(serving_branch.as_deref(), "store_identity.serving_branch")?;
        Ok(Self {
            project_id,
            project_root,
            data_root,
            graph_db_path,
            serving_branch,
        })
    }
}

/// Action fields sent by the runner on the authenticated preparation exchange.
///
/// This type intentionally has no `Serialize` implementation because it owns
/// the secret `proof_key`. Use [`parse_prepare_from_initialize`] at the wire
/// boundary and [`ActionPrepareResponse`] for the public acknowledgement.
pub struct ActionPrepareRequest {
    pub action_id: String,
    pub route: String,
    pub nonce: String,
    pub proof_key: ProofKey,
    pub action_digest: String,
    pub scope: String,
    pub tool: String,
    pub entrypoint: String,
    pub expires_at: i64,
}

impl fmt::Debug for ActionPrepareRequest {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ActionPrepareRequest")
            .field("action_id", &self.action_id)
            .field("route", &self.route)
            .field("nonce", &self.nonce)
            .field("proof_key", &self.proof_key)
            .field("action_digest", &self.action_digest)
            .field("scope", &self.scope)
            .field("tool", &self.tool)
            .field("entrypoint", &self.entrypoint)
            .field("expires_at", &self.expires_at)
            .finish()
    }
}

impl ActionPrepareRequest {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        action_id: impl Into<String>,
        route: impl Into<String>,
        nonce: impl Into<String>,
        proof_key: ProofKey,
        action_digest: impl Into<String>,
        scope: impl Into<String>,
        tool: impl Into<String>,
        entrypoint: impl Into<String>,
        expires_at: i64,
    ) -> Result<Self, ActionReceiptError> {
        let request = Self {
            action_id: action_id.into(),
            route: route.into(),
            nonce: nonce.into(),
            proof_key,
            action_digest: action_digest.into(),
            scope: scope.into(),
            tool: tool.into(),
            entrypoint: entrypoint.into(),
            expires_at,
        };
        request.validate_shape()?;
        Ok(request)
    }

    fn validate_shape(&self) -> Result<(), ActionReceiptError> {
        validate_text(&self.action_id, "action_id")?;
        validate_text(&self.route, "route")?;
        validate_hex(&self.nonce, "nonce")?;
        validate_hex(&self.action_digest, "action_digest")?;
        validate_text(&self.scope, "scope")?;
        validate_text(&self.tool, "tool")?;
        validate_text(&self.entrypoint, "entrypoint")?;
        if self.expires_at <= 0 {
            return Err(ActionReceiptError::InvalidField("expires_at"));
        }
        Ok(())
    }
}

#[derive(Deserialize)]
struct WireActionPrepareRequest {
    format: String,
    revision: u32,
    action_id: String,
    #[serde(default)]
    route: String,
    nonce: String,
    proof_key: zeroize::Zeroizing<String>,
    action_digest: String,
    scope: String,
    tool: String,
    entrypoint: String,
    expires_at: i64,
}

/// Public acknowledgement returned from the authenticated preparation
/// exchange. It proves which live generation accepted the reservation while
/// withholding the proof key.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ActionPrepareResponse {
    pub format: String,
    pub revision: u32,
    pub action_id: String,
    pub route: String,
    pub nonce: String,
    pub action_digest: String,
    pub scope: String,
    pub tool: String,
    pub entrypoint: String,
    pub daemon_generation: DaemonGeneration,
    pub store_identity: LiveStoreIdentity,
    pub expires_at: i64,
}

/// Parse the prepare object from an authenticated `initialize` request's
/// `params._meta`. `None` means this is an ordinary initialize request.
pub fn parse_prepare_from_initialize(
    params: &Value,
) -> Result<Option<ActionPrepareRequest>, ActionReceiptError> {
    let Some(meta) = params.get("_meta").and_then(Value::as_object) else {
        return Ok(None);
    };
    let Some(value) = meta.get(ACTION_PREPARE_META_KEY) else {
        return Ok(None);
    };
    let wire: WireActionPrepareRequest =
        serde_json::from_value(value.clone()).map_err(|_| ActionReceiptError::MalformedPrepare)?;
    if wire.format != ACTION_RECEIPT_FORMAT || wire.revision != ACTION_RECEIPT_REVISION {
        return Err(ActionReceiptError::MalformedPrepare);
    }
    // `WireActionPrepareRequest` owns its parsed key in a zeroizing wrapper,
    // so the field is scrubbed on every exit path, including a malformed key
    // or a later action-shape validation error. The caller separately redacts
    // the parsed JSON/raw request before project work is admitted.
    let proof_key = ProofKey::from_hex(&wire.proof_key)?;
    Ok(Some(ActionPrepareRequest::new(
        wire.action_id,
        wire.route,
        wire.nonce,
        proof_key,
        wire.action_digest,
        wire.scope,
        wire.tool,
        wire.entrypoint,
        wire.expires_at,
    )?))
}

/// Metadata carried by the actual child `tools/call`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ActionCallMetadata {
    pub nonce: String,
    /// Daemon route assertion repeated on the actual call. The server still
    /// supplies its observed route to [`ActionReceiptAuthority::begin`].
    pub route: Option<String>,
    /// Optional duplicate assertions are accepted only when they match the
    /// daemon's retained reservation. The runner normally carries nonce only.
    pub action_digest: Option<String>,
    pub scope: Option<String>,
    pub entrypoint: Option<String>,
}

impl ActionCallMetadata {
    pub fn new(nonce: impl Into<String>) -> Result<Self, ActionReceiptError> {
        let metadata = Self {
            nonce: nonce.into(),
            route: None,
            action_digest: None,
            scope: None,
            entrypoint: None,
        };
        metadata.validate_shape()?;
        Ok(metadata)
    }

    fn validate_shape(&self) -> Result<(), ActionReceiptError> {
        validate_hex(&self.nonce, "nonce")?;
        validate_optional_text(self.route.as_deref(), "route")?;
        validate_optional_hex(self.action_digest.as_deref(), "action_digest")?;
        validate_optional_text(self.scope.as_deref(), "scope")?;
        validate_optional_text(self.entrypoint.as_deref(), "entrypoint")?;
        Ok(())
    }
}

/// Parse call metadata from raw JSON-RPC params. A missing action metadata
/// object is ordinary traffic; a partial object fails closed.
pub fn parse_call_metadata_from_params(
    params: Option<&Value>,
) -> Result<Option<ActionCallMetadata>, ActionReceiptError> {
    let Some(meta) = params
        .and_then(Value::as_object)
        .and_then(|params| params.get("_meta"))
        .and_then(Value::as_object)
    else {
        return Ok(None);
    };
    parse_call_metadata_map(Some(meta))
}

/// Parse call metadata from the typed RMCP `_meta` map without materializing a
/// second JSON tree.
pub fn parse_call_metadata_map(
    meta: Option<&Map<String, Value>>,
) -> Result<Option<ActionCallMetadata>, ActionReceiptError> {
    let Some(meta) = meta else {
        return Ok(None);
    };
    let Some(nonce) = meta.get(ACTION_NONCE_META_KEY) else {
        let has_partial = meta.contains_key("nativeOriginalActionDigest")
            || meta.contains_key("nativeOriginalActionRoute")
            || meta.contains_key("nativeOriginalActionScope")
            || meta.contains_key("nativeOriginalActionEntrypoint");
        return if has_partial {
            Err(ActionReceiptError::MissingField(ACTION_NONCE_META_KEY))
        } else {
            Ok(None)
        };
    };
    let nonce = nonce
        .as_str()
        .ok_or(ActionReceiptError::InvalidField("nonce"))?
        .to_owned();
    let route = optional_meta_text(meta, "nativeOriginalActionRoute", "route")?;
    let action_digest = optional_meta_text(meta, "nativeOriginalActionDigest", "action_digest")?;
    let scope = optional_meta_text(meta, "nativeOriginalActionScope", "scope")?;
    let entrypoint = optional_meta_text(meta, "nativeOriginalActionEntrypoint", "entrypoint")?;
    Ok(Some(ActionCallMetadata {
        nonce,
        route,
        action_digest,
        scope,
        entrypoint,
    }))
}

fn optional_meta_text(
    meta: &Map<String, Value>,
    wire_key: &str,
    field: &'static str,
) -> Result<Option<String>, ActionReceiptError> {
    meta.get(wire_key)
        .map(|value| {
            value
                .as_str()
                .ok_or(ActionReceiptError::InvalidField(field))
                .and_then(|value| {
                    validate_text(value, field)?;
                    Ok(value.to_owned())
                })
        })
        .transpose()
}

/// Public receipt produced only after terminal dispatch.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ActionReceipt {
    pub format: String,
    pub revision: u32,
    pub action_id: String,
    pub route: String,
    pub nonce: String,
    pub action_digest: String,
    pub scope: String,
    pub tool: String,
    pub entrypoint: String,
    pub daemon_generation: DaemonGeneration,
    pub store_identity: LiveStoreIdentity,
    pub result_digest: String,
    pub expires_at: i64,
    pub issued_at: i64,
    pub receipt_mac: String,
    pub receipt_sha256: String,
}

/// A successfully validated dispatch reservation. It is intentionally
/// non-serializable and owns the only live copy of the proof key after
/// [`ActionReceiptAuthority::begin`].
pub struct PendingAction {
    action_id: String,
    route: String,
    nonce: String,
    proof_key: ProofKey,
    action_digest: String,
    scope: String,
    tool: String,
    entrypoint: String,
    store_identity: LiveStoreIdentity,
    expires_at: i64,
    daemon_generation: DaemonGeneration,
}

impl fmt::Debug for PendingAction {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PendingAction")
            .field("action_id", &self.action_id)
            .field("route", &self.route)
            .field("nonce", &self.nonce)
            .field("proof_key", &self.proof_key)
            .field("action_digest", &self.action_digest)
            .field("scope", &self.scope)
            .field("tool", &self.tool)
            .field("entrypoint", &self.entrypoint)
            .field("store_identity", &self.store_identity)
            .field("expires_at", &self.expires_at)
            .field("daemon_generation", &self.daemon_generation)
            .finish()
    }
}

struct PreparedAction {
    action_id: String,
    route: String,
    nonce: String,
    proof_key: ProofKey,
    action_digest: String,
    scope: String,
    tool: String,
    entrypoint: String,
    store_identity: LiveStoreIdentity,
    expires_at: i64,
    daemon_generation: DaemonGeneration,
}

struct ActionReceiptState {
    prepared: HashMap<String, PreparedAction>,
}

/// A reservation is deliberately bounded even when a client never sends the
/// matching `tools/call`. Expired entries are purged on every authority
/// operation; the bound prevents abandoned clients from growing this map
/// without limit between requests.
pub const MAX_ACTIVE_ACTION_RECEIPTS: usize = 1_024;

/// Per-daemon in-memory authority. `for_scope` shares one authority across
/// project servers mounted by the same daemon profile, so a routed call can
/// finish against the selected retained server while preserving one-shot
/// state.
#[derive(Clone)]
pub struct ActionReceiptAuthority {
    inner: Arc<Mutex<ActionReceiptState>>,
}

static SCOPED_AUTHORITIES: OnceLock<Mutex<HashMap<String, Weak<Mutex<ActionReceiptState>>>>> =
    OnceLock::new();

impl Default for ActionReceiptAuthority {
    fn default() -> Self {
        Self::new()
    }
}

impl ActionReceiptAuthority {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(Mutex::new(ActionReceiptState {
                prepared: HashMap::new(),
            })),
        }
    }

    pub fn for_scope(scope: impl Into<String>) -> Self {
        let scope = scope.into();
        let registry = SCOPED_AUTHORITIES.get_or_init(|| Mutex::new(HashMap::new()));
        let mut registry = registry
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(inner) = registry.get(&scope).and_then(Weak::upgrade) {
            return Self { inner };
        }
        let inner = Arc::new(Mutex::new(ActionReceiptState {
            prepared: HashMap::new(),
        }));
        registry.insert(scope, Arc::downgrade(&inner));
        Self { inner }
    }

    /// Remove reservations whose deadline has passed. Daemon request paths
    /// call this before ordinary dispatch as well as before prepare/begin, so
    /// abandoned keys do not remain until another prepare arrives.
    pub fn cleanup_expired(&self, now: i64) -> usize {
        let mut state = self
            .inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let before = state.prepared.len();
        state.prepared.retain(|_, action| action.expires_at > now);
        before.saturating_sub(state.prepared.len())
    }

    /// Retain one action after validating the current generation and expiry.
    pub fn prepare(
        &self,
        request: ActionPrepareRequest,
        daemon_generation: DaemonGeneration,
        store_identity: LiveStoreIdentity,
        observed_entrypoint: &str,
        now: i64,
    ) -> Result<ActionPrepareResponse, ActionReceiptError> {
        request.validate_shape()?;
        validate_text(observed_entrypoint, "observed_entrypoint")?;
        if request.entrypoint != observed_entrypoint {
            return Err(ActionReceiptError::ActionMismatch("entrypoint"));
        }
        if request.scope != store_identity.project_root {
            return Err(ActionReceiptError::StoreIdentityMismatch);
        }
        if request.expires_at <= now {
            return Err(ActionReceiptError::Expired);
        }
        let ttl = request
            .expires_at
            .checked_sub(now)
            .ok_or(ActionReceiptError::InvalidField("expires_at"))?;
        if ttl > MAX_ACTION_RECEIPT_TTL_MICROS {
            return Err(ActionReceiptError::InvalidField("expires_at"));
        }
        let response = ActionPrepareResponse {
            format: ACTION_RECEIPT_FORMAT.to_owned(),
            revision: ACTION_RECEIPT_REVISION,
            action_id: request.action_id.clone(),
            route: request.route.clone(),
            nonce: request.nonce.clone(),
            action_digest: request.action_digest.clone(),
            scope: request.scope.clone(),
            tool: request.tool.clone(),
            entrypoint: request.entrypoint.clone(),
            daemon_generation: daemon_generation.clone(),
            store_identity: store_identity.clone(),
            expires_at: request.expires_at,
        };
        let mut state = self
            .inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        state.prepared.retain(|_, action| action.expires_at > now);
        if state.prepared.contains_key(&request.nonce) {
            return Err(ActionReceiptError::Replay);
        }
        if state.prepared.len() >= MAX_ACTIVE_ACTION_RECEIPTS {
            return Err(ActionReceiptError::Capacity);
        }
        state.prepared.insert(
            request.nonce.clone(),
            PreparedAction {
                action_id: request.action_id,
                route: request.route,
                nonce: request.nonce,
                proof_key: request.proof_key,
                action_digest: request.action_digest,
                scope: request.scope,
                tool: request.tool,
                entrypoint: request.entrypoint,
                store_identity,
                expires_at: request.expires_at,
                daemon_generation,
            },
        );
        Ok(response)
    }

    /// Validate a nonce and consume its reservation immediately before work is
    /// admitted. Consuming before execution makes concurrent/replayed calls
    /// one-shot even if a worker never returns a response.
    pub fn begin(
        &self,
        call: &ActionCallMetadata,
        actual_tool: &str,
        actual_route: &str,
        actual_entrypoint: &str,
        actual_arguments: &Value,
        actual_store_identity: &LiveStoreIdentity,
        daemon_generation: &DaemonGeneration,
        now: i64,
    ) -> Result<PendingAction, ActionReceiptError> {
        call.validate_shape()?;
        validate_text(actual_route, "observed_route")?;
        validate_text(actual_entrypoint, "observed_entrypoint")?;
        let mut state = self
            .inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let requested_nonce_expired = state
            .prepared
            .get(&call.nonce)
            .is_some_and(|action| action.expires_at <= now);
        state.prepared.retain(|_, action| action.expires_at > now);
        if requested_nonce_expired {
            return Err(ActionReceiptError::Expired);
        }
        let Some(action) = state.prepared.get(&call.nonce) else {
            return Err(ActionReceiptError::Replay);
        };
        if action.daemon_generation != *daemon_generation {
            state.prepared.remove(&call.nonce);
            return Err(ActionReceiptError::GenerationMismatch);
        }
        if action.tool != actual_tool {
            return Err(ActionReceiptError::ActionMismatch("tool"));
        }
        if action.route != actual_route {
            return Err(ActionReceiptError::ActionMismatch("route"));
        }
        if call
            .route
            .as_deref()
            .is_some_and(|route| route != action.route)
        {
            return Err(ActionReceiptError::ActionMismatch("route"));
        }
        if action.entrypoint != actual_entrypoint {
            return Err(ActionReceiptError::ActionMismatch("entrypoint"));
        }
        if action.store_identity != *actual_store_identity {
            state.prepared.remove(&call.nonce);
            return Err(ActionReceiptError::StoreIdentityMismatch);
        }
        if call
            .action_digest
            .as_deref()
            .is_some_and(|digest| digest != action.action_digest)
        {
            return Err(ActionReceiptError::ActionMismatch("action_digest"));
        }
        if call
            .scope
            .as_deref()
            .is_some_and(|scope| scope != action.scope)
        {
            return Err(ActionReceiptError::ActionMismatch("scope"));
        }
        if call
            .entrypoint
            .as_deref()
            .is_some_and(|entrypoint| entrypoint != action.entrypoint)
        {
            return Err(ActionReceiptError::ActionMismatch("entrypoint"));
        }
        let computed = canonical_action_digest(
            &action.scope,
            &action.tool,
            &action.entrypoint,
            actual_arguments,
        );
        if computed != action.action_digest {
            return Err(ActionReceiptError::ActionMismatch("action_digest"));
        }
        let action = state
            .prepared
            .remove(&call.nonce)
            .ok_or(ActionReceiptError::Replay)?;
        Ok(PendingAction {
            action_id: action.action_id,
            // The route in the receipt is the value supplied by the daemon's
            // selected dispatch path. The equality check above proves the
            // runner's prepare claim matches it, but the receipt itself is
            // constructed from the observed value rather than from child
            // metadata.
            route: actual_route.to_owned(),
            nonce: action.nonce,
            proof_key: action.proof_key,
            action_digest: action.action_digest,
            scope: action.scope,
            tool: action.tool,
            entrypoint: action.entrypoint,
            store_identity: action.store_identity,
            expires_at: action.expires_at,
            daemon_generation: action.daemon_generation,
        })
    }

    /// Issue the sole public receipt for a pending terminal dispatch.
    pub fn finish(
        &self,
        pending: PendingAction,
        store_identity: LiveStoreIdentity,
        result_digest: impl Into<String>,
        current_daemon_generation: &DaemonGeneration,
        issued_at: i64,
    ) -> Result<ActionReceipt, ActionReceiptError> {
        let result_digest = result_digest.into();
        validate_hex(&result_digest, "result_digest")?;
        if pending.daemon_generation != *current_daemon_generation {
            return Err(ActionReceiptError::GenerationMismatch);
        }
        if pending.store_identity != store_identity {
            return Err(ActionReceiptError::StoreIdentityMismatch);
        }
        if issued_at >= pending.expires_at {
            return Err(ActionReceiptError::Expired);
        }
        let unsigned = ReceiptUnsigned {
            action_id: &pending.action_id,
            route: &pending.route,
            nonce: &pending.nonce,
            action_digest: &pending.action_digest,
            scope: &pending.scope,
            tool: &pending.tool,
            entrypoint: &pending.entrypoint,
            daemon_generation: &pending.daemon_generation,
            store_identity: &store_identity,
            result_digest: &result_digest,
            expires_at: pending.expires_at,
            issued_at,
        };
        let receipt_mac = hmac_hex(pending.proof_key.as_bytes(), &unsigned);
        let receipt_sha256 = receipt_sha256(&unsigned, &receipt_mac);
        Ok(ActionReceipt {
            format: ACTION_RECEIPT_FORMAT.to_owned(),
            revision: ACTION_RECEIPT_REVISION,
            action_id: pending.action_id,
            route: pending.route,
            nonce: pending.nonce,
            action_digest: pending.action_digest,
            scope: pending.scope,
            tool: pending.tool,
            entrypoint: pending.entrypoint,
            daemon_generation: pending.daemon_generation,
            store_identity,
            result_digest,
            expires_at: pending.expires_at,
            issued_at,
            receipt_mac,
            receipt_sha256,
        })
    }
}

struct ReceiptUnsigned<'a> {
    action_id: &'a str,
    route: &'a str,
    nonce: &'a str,
    action_digest: &'a str,
    scope: &'a str,
    tool: &'a str,
    entrypoint: &'a str,
    daemon_generation: &'a DaemonGeneration,
    store_identity: &'a LiveStoreIdentity,
    result_digest: &'a str,
    expires_at: i64,
    issued_at: i64,
}

fn hmac_hex(key: &[u8], material: &ReceiptUnsigned<'_>) -> String {
    let Ok(mut mac) = HmacSha256::new_from_slice(key) else {
        return String::new();
    };
    update_receipt_mac_material(&mut mac, material);
    hex::encode(mac.finalize().into_bytes())
}

fn update_receipt_mac_material(mac: &mut HmacSha256, material: &ReceiptUnsigned<'_>) {
    append_lp_mac(mac, ACTION_RECEIPT_FORMAT.as_bytes());
    let revision = ACTION_RECEIPT_REVISION.to_string();
    append_lp_mac(mac, revision.as_bytes());
    let epoch = material.daemon_generation.epoch.to_string();
    let expires_at = material.expires_at.to_string();
    let issued_at = material.issued_at.to_string();
    let values = [
        material.action_id.as_bytes(),
        material.route.as_bytes(),
        material.nonce.as_bytes(),
        material.action_digest.as_bytes(),
        material.scope.as_bytes(),
        material.tool.as_bytes(),
        material.entrypoint.as_bytes(),
        epoch.as_bytes(),
        material.daemon_generation.process_run_id.as_bytes(),
        material
            .store_identity
            .project_id
            .as_deref()
            .unwrap_or_default()
            .as_bytes(),
        material.store_identity.project_root.as_bytes(),
        material.store_identity.data_root.as_bytes(),
        material.store_identity.graph_db_path.as_bytes(),
        material
            .store_identity
            .serving_branch
            .as_deref()
            .unwrap_or_default()
            .as_bytes(),
        material.result_digest.as_bytes(),
        expires_at.as_bytes(),
        issued_at.as_bytes(),
    ];
    for value in values {
        append_lp_mac(mac, value);
    }
}

fn append_lp_mac(mac: &mut HmacSha256, value: &[u8]) {
    mac.update(&(value.len() as u64).to_be_bytes());
    mac.update(value);
}

fn receipt_sha256(material: &ReceiptUnsigned<'_>, receipt_mac: &str) -> String {
    let mut hasher = Sha256::new();
    append_lp_hash(&mut hasher, ACTION_RECEIPT_FORMAT.as_bytes());
    let revision = ACTION_RECEIPT_REVISION.to_string();
    append_lp_hash(&mut hasher, revision.as_bytes());
    let epoch = material.daemon_generation.epoch.to_string();
    let expires_at = material.expires_at.to_string();
    let issued_at = material.issued_at.to_string();
    let values = [
        material.action_id.as_bytes(),
        material.route.as_bytes(),
        material.nonce.as_bytes(),
        material.action_digest.as_bytes(),
        material.scope.as_bytes(),
        material.tool.as_bytes(),
        material.entrypoint.as_bytes(),
        epoch.as_bytes(),
        material.daemon_generation.process_run_id.as_bytes(),
        material
            .store_identity
            .project_id
            .as_deref()
            .unwrap_or_default()
            .as_bytes(),
        material.store_identity.project_root.as_bytes(),
        material.store_identity.data_root.as_bytes(),
        material.store_identity.graph_db_path.as_bytes(),
        material
            .store_identity
            .serving_branch
            .as_deref()
            .unwrap_or_default()
            .as_bytes(),
        material.result_digest.as_bytes(),
        expires_at.as_bytes(),
        issued_at.as_bytes(),
        receipt_mac.as_bytes(),
    ];
    for value in values {
        append_lp_hash(&mut hasher, value);
    }
    hex::encode(hasher.finalize())
}

fn append_lp_hash(hasher: &mut Sha256, value: &[u8]) {
    hasher.update(&(value.len() as u64).to_be_bytes());
    hasher.update(value);
}

/// Compute the action digest used by both prepare and terminal dispatch.
pub fn canonical_action_digest(
    scope: &str,
    tool: &str,
    entrypoint: &str,
    arguments: &Value,
) -> String {
    let mut hasher = Sha256::new();
    append_lp_hash(&mut hasher, ACTION_RECEIPT_FORMAT.as_bytes());
    let revision = ACTION_RECEIPT_REVISION.to_string();
    append_lp_hash(&mut hasher, revision.as_bytes());
    append_lp_hash(&mut hasher, scope.as_bytes());
    append_lp_hash(&mut hasher, tool.as_bytes());
    append_lp_hash(&mut hasher, entrypoint.as_bytes());
    append_lp_hash(&mut hasher, &canonical_json_bytes(arguments));
    hex::encode(hasher.finalize())
}

/// Digest a terminal MCP result after receipt metadata has been excluded.
pub fn canonical_result_digest(result: &Value) -> String {
    let mut hasher = Sha256::new();
    append_lp_hash(&mut hasher, &canonical_json_bytes(result));
    hex::encode(hasher.finalize())
}

/// Canonical JSON used by action and result digests. Object keys are sorted
/// recursively, numbers use serde_json's stable lexical representation, and
/// strings remain UTF-8 rather than ASCII-escaped.
pub fn canonical_json_bytes(value: &Value) -> Vec<u8> {
    let mut bytes = Vec::new();
    write_canonical_json(value, &mut bytes);
    bytes
}

fn write_canonical_json(value: &Value, bytes: &mut Vec<u8>) {
    match value {
        Value::Null => bytes.extend_from_slice(b"null"),
        Value::Bool(value) => bytes.extend_from_slice(if *value { b"true" } else { b"false" }),
        Value::Number(value) => bytes.extend_from_slice(value.to_string().as_bytes()),
        Value::String(value) => {
            bytes.extend_from_slice(serde_json::to_string(value).unwrap_or_default().as_bytes());
        }
        Value::Array(values) => {
            bytes.push(b'[');
            for (index, value) in values.iter().enumerate() {
                if index != 0 {
                    bytes.push(b',');
                }
                write_canonical_json(value, bytes);
            }
            bytes.push(b']');
        }
        Value::Object(values) => {
            bytes.push(b'{');
            let mut keys: Vec<&str> = values.keys().map(String::as_str).collect();
            keys.sort_unstable();
            for (index, key) in keys.into_iter().enumerate() {
                if index != 0 {
                    bytes.push(b',');
                }
                bytes.extend_from_slice(serde_json::to_string(key).unwrap_or_default().as_bytes());
                bytes.push(b':');
                if let Some(value) = values.get(key) {
                    write_canonical_json(value, bytes);
                }
            }
            bytes.push(b'}');
        }
    }
}

fn validate_hex(value: &str, field: &'static str) -> Result<(), ActionReceiptError> {
    if is_lower_hex_64(value) {
        Ok(())
    } else {
        Err(ActionReceiptError::InvalidField(field))
    }
}

fn validate_optional_hex(
    value: Option<&str>,
    field: &'static str,
) -> Result<(), ActionReceiptError> {
    value.map_or(Ok(()), |value| validate_hex(value, field))
}

fn validate_text(value: &str, field: &'static str) -> Result<(), ActionReceiptError> {
    if value.is_empty() || value.len() > MAX_TEXT_BYTES || value.contains('\0') {
        return Err(ActionReceiptError::InvalidField(field));
    }
    Ok(())
}

fn validate_optional_text(
    value: Option<&str>,
    field: &'static str,
) -> Result<(), ActionReceiptError> {
    value.map_or(Ok(()), |value| validate_text(value, field))
}

fn is_lower_hex_64(value: &str) -> bool {
    value.len() == HEX_BYTES * 2
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

/// Errors intentionally name only a safe field/reason; secret values and
/// caller payloads never enter an error string.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ActionReceiptError {
    #[error("action receipt prepare metadata is malformed")]
    MalformedPrepare,
    #[error("action receipt field is missing: {0}")]
    MissingField(&'static str),
    #[error("action receipt field is invalid: {0}")]
    InvalidField(&'static str),
    #[error("action receipt reservation expired")]
    Expired,
    #[error("action receipt nonce was already consumed or is unknown")]
    Replay,
    #[error("action receipt daemon generation changed")]
    GenerationMismatch,
    #[error("action receipt action mismatch: {0}")]
    ActionMismatch(&'static str),
    #[error("action receipt store identity mismatch")]
    StoreIdentityMismatch,
    #[error("action receipt reservation capacity reached")]
    Capacity,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn hex(fill: char) -> String {
        std::iter::repeat_n(fill, HEX_BYTES * 2).collect()
    }

    fn generation() -> DaemonGeneration {
        DaemonGeneration::new(7, "run-7").expect("generation")
    }

    fn store(scope: &str) -> LiveStoreIdentity {
        LiveStoreIdentity::new(
            Some("project-1".to_owned()),
            scope,
            "/profile/data/project-1",
            "/profile/data/project-1/tracedecay.db",
            Some("main".to_owned()),
        )
        .expect("store")
    }

    fn args() -> Value {
        serde_json::json!({"query":"hello","limit":3,"nested":{"b":2,"a":1}})
    }

    fn prepare(now: i64) -> ActionPrepareRequest {
        let scope = "/workspace/project";
        let digest = canonical_action_digest(scope, "tracedecay_search", "mcp_stdio", &args());
        ActionPrepareRequest::new(
            "action-1",
            "route-1",
            hex('a'),
            ProofKey::from_bytes([0x11; HEX_BYTES]),
            digest,
            scope,
            "tracedecay_search",
            "mcp_stdio",
            now + 10_000,
        )
        .expect("prepare")
    }

    #[test]
    fn canonical_json_sorts_nested_objects_and_action_digest_is_stable() {
        let left = serde_json::json!({"z":1,"a":{"z":2,"a":true}});
        let right = serde_json::json!({"a":{"a":true,"z":2},"z":1});
        assert_eq!(
            canonical_json_bytes(&left),
            br#"{"a":{"a":true,"z":2},"z":1}"#
        );
        assert_eq!(canonical_json_bytes(&left), canonical_json_bytes(&right));
        assert_eq!(
            canonical_action_digest("/scope", "tool", "mcp_stdio", &left),
            canonical_action_digest("/scope", "tool", "mcp_stdio", &right),
        );
    }

    #[test]
    fn canonical_receipt_material_matches_runner_golden_vectors() {
        let action_digest = canonical_action_digest(
            "/workspace/golden",
            "tracedecay_search",
            ACTION_ENTRYPOINT_MCP_STDIO,
            &serde_json::json!({"limit": 5, "query": "typed dispatch"}),
        );
        assert_eq!(
            action_digest,
            "ba976a9affb57255509c3074812528652eba127bb43824083d42b512886b18dc"
        );
        let nested_digest = canonical_action_digest(
            "/scope",
            "tool",
            ACTION_ENTRYPOINT_MCP_STDIO,
            &serde_json::json!({"z": 1, "a": {"z": 2, "a": true}}),
        );
        assert_eq!(
            nested_digest,
            "845e97727c85776f2eb6586b00fa21b97feb290257eeec38f7e5a2957caa501c"
        );
        let result_digest = canonical_result_digest(
            &serde_json::json!({"content": [{"text": "ok", "type": "text"}]}),
        );
        assert_eq!(
            result_digest,
            "bddd60f823dbda9f775dde4bf2bd2a2431141f1e6752c43059a92029bf3193fa"
        );

        let generation = DaemonGeneration::new(9, "golden-run").expect("generation");
        let store = LiveStoreIdentity::new(
            Some("project-golden".to_owned()),
            "/workspace/golden",
            "/var/tmp/golden-data",
            "/var/tmp/golden-data/db.sqlite",
            Some("main".to_owned()),
        )
        .expect("store");
        let nonce = "11".repeat(32);
        let action_digest = "22".repeat(32);
        let result_digest = "33".repeat(32);
        let unsigned = ReceiptUnsigned {
            action_id: "golden-action",
            route: "fact_store_search",
            nonce: &nonce,
            action_digest: &action_digest,
            scope: "/workspace/golden",
            tool: "tracedecay_search",
            entrypoint: ACTION_ENTRYPOINT_MCP_STDIO,
            daemon_generation: &generation,
            store_identity: &store,
            result_digest: &result_digest,
            expires_at: 1_700_000_000_000_100,
            issued_at: 1_700_000_000_000_000,
        };
        let receipt_mac = hmac_hex(&[b'k'; HEX_BYTES], &unsigned);
        assert_eq!(
            receipt_mac,
            "3dfd3d15b4dc2428aaec6a8acaab13e570e92504dc1d7542731419c824667ad2"
        );
        assert_eq!(
            receipt_sha256(&unsigned, &receipt_mac),
            "7a0a9eaffd0a3834cf2468752818b5cb17ca5f249c32b54d64cd810602d343c3"
        );
    }

    #[test]
    fn prepare_binds_nonce_generation_and_expiry_without_echoing_key() {
        let authority = ActionReceiptAuthority::new();
        let now = 1_000;
        let response = authority
            .prepare(
                prepare(now),
                generation(),
                store("/workspace/project"),
                ACTION_ENTRYPOINT_MCP_STDIO,
                now,
            )
            .expect("prepare action");
        assert_eq!(response.nonce, hex('a'));
        let debug = format!("{response:?}");
        assert!(!debug.contains(&hex('1')));
        let duplicate = authority.prepare(
            prepare(now),
            generation(),
            store("/workspace/project"),
            ACTION_ENTRYPOINT_MCP_STDIO,
            now,
        );
        assert_eq!(duplicate, Err(ActionReceiptError::Replay));
        let expired = authority.prepare(
            prepare(now),
            generation(),
            store("/workspace/project"),
            ACTION_ENTRYPOINT_MCP_STDIO,
            11_001,
        );
        assert_eq!(expired, Err(ActionReceiptError::Expired));
    }

    #[test]
    fn wrong_action_and_generation_fail_closed() {
        let authority = ActionReceiptAuthority::new();
        let now = 1_000;
        authority
            .prepare(
                prepare(now),
                generation(),
                store("/workspace/project"),
                ACTION_ENTRYPOINT_MCP_STDIO,
                now,
            )
            .expect("prepare");
        let call = ActionCallMetadata::new(hex('a')).expect("call");
        assert!(matches!(
            authority.begin(
                &call,
                "wrong",
                "route-1",
                ACTION_ENTRYPOINT_MCP_STDIO,
                &args(),
                &store("/workspace/project"),
                &generation(),
                now,
            ),
            Err(ActionReceiptError::ActionMismatch("tool"))
        ));
        let changed = DaemonGeneration::new(8, "run-8").expect("changed generation");
        assert!(matches!(
            authority.begin(
                &call,
                "tracedecay_search",
                "route-1",
                ACTION_ENTRYPOINT_MCP_STDIO,
                &args(),
                &store("/workspace/project"),
                &changed,
                now,
            ),
            Err(ActionReceiptError::GenerationMismatch)
        ));
    }

    #[test]
    fn finish_is_one_shot_and_hmac_binds_live_store_and_result() {
        let authority = ActionReceiptAuthority::new();
        let now = 1_000;
        authority
            .prepare(
                prepare(now),
                generation(),
                store("/workspace/project"),
                ACTION_ENTRYPOINT_MCP_STDIO,
                now,
            )
            .expect("prepare");
        let call = ActionCallMetadata::new(hex('a')).expect("call");
        let pending = authority
            .begin(
                &call,
                "tracedecay_search",
                "route-1",
                ACTION_ENTRYPOINT_MCP_STDIO,
                &args(),
                &store("/workspace/project"),
                &generation(),
                now,
            )
            .expect("begin");
        let result = canonical_result_digest(&serde_json::json!({"content":[{"text":"ok"}]}));
        let receipt = authority
            .finish(
                pending,
                store("/workspace/project"),
                result.clone(),
                &generation(),
                now + 1,
            )
            .expect("finish");
        assert_eq!(receipt.result_digest, result);
        assert_eq!(receipt.expires_at, now + 10_000);
        assert_eq!(receipt.receipt_mac.len(), 64);
        assert_eq!(receipt.receipt_sha256.len(), 64);
        assert!(matches!(
            authority.begin(
                &call,
                "tracedecay_search",
                "route-1",
                ACTION_ENTRYPOINT_MCP_STDIO,
                &args(),
                &store("/workspace/project"),
                &generation(),
                now,
            ),
            Err(ActionReceiptError::Replay)
        ));

        let expiry_authority = ActionReceiptAuthority::new();
        expiry_authority
            .prepare(
                prepare(now),
                generation(),
                store("/workspace/project"),
                ACTION_ENTRYPOINT_MCP_STDIO,
                now,
            )
            .expect("prepare expiry action");
        let expiry_pending = expiry_authority
            .begin(
                &call,
                "tracedecay_search",
                "route-1",
                ACTION_ENTRYPOINT_MCP_STDIO,
                &args(),
                &store("/workspace/project"),
                &generation(),
                now,
            )
            .expect("begin expiry action");
        assert_eq!(
            expiry_authority.finish(
                expiry_pending,
                store("/workspace/project"),
                result.clone(),
                &generation(),
                now + 10_000,
            ),
            Err(ActionReceiptError::Expired)
        );

        // Re-issue the same action in a fresh authority with a different
        // terminal result. The MAC must change because the result digest is
        // one of its authenticated fields.
        let changed_authority = ActionReceiptAuthority::new();
        changed_authority
            .prepare(
                prepare(now),
                generation(),
                store("/workspace/project"),
                ACTION_ENTRYPOINT_MCP_STDIO,
                now,
            )
            .expect("prepare changed result");
        let changed_pending = changed_authority
            .begin(
                &call,
                "tracedecay_search",
                "route-1",
                ACTION_ENTRYPOINT_MCP_STDIO,
                &args(),
                &store("/workspace/project"),
                &generation(),
                now,
            )
            .expect("begin changed result");
        let changed_result = canonical_result_digest(&serde_json::json!({"content":[]}));
        let changed_receipt = changed_authority
            .finish(
                changed_pending,
                store("/workspace/project"),
                changed_result,
                &generation(),
                now + 1,
            )
            .expect("finish changed result");
        assert_ne!(changed_receipt.receipt_mac, receipt.receipt_mac);

        let identity_authority = ActionReceiptAuthority::new();
        identity_authority
            .prepare(
                prepare(now),
                generation(),
                store("/workspace/project"),
                ACTION_ENTRYPOINT_MCP_STDIO,
                now,
            )
            .expect("prepare identity mismatch");
        let identity_pending = identity_authority
            .begin(
                &call,
                "tracedecay_search",
                "route-1",
                ACTION_ENTRYPOINT_MCP_STDIO,
                &args(),
                &store("/workspace/project"),
                &generation(),
                now,
            )
            .expect("begin identity mismatch");
        assert_eq!(
            identity_authority.finish(
                identity_pending,
                store("/workspace/other"),
                result,
                &generation(),
                now + 1,
            ),
            Err(ActionReceiptError::StoreIdentityMismatch)
        );
    }

    #[test]
    fn finish_signs_mcp_is_error_terminal_result() {
        let authority = ActionReceiptAuthority::new();
        let now = 1_000;
        authority
            .prepare(
                prepare(now),
                generation(),
                store("/workspace/project"),
                ACTION_ENTRYPOINT_MCP_STDIO,
                now,
            )
            .expect("prepare error action");
        let call = ActionCallMetadata::new(hex('a')).expect("call");
        let pending = authority
            .begin(
                &call,
                "tracedecay_search",
                "route-1",
                ACTION_ENTRYPOINT_MCP_STDIO,
                &args(),
                &store("/workspace/project"),
                &generation(),
                now,
            )
            .expect("begin error action");
        let terminal = serde_json::json!({
            "content": [{"type": "text", "text": "validation failed"}],
            "isError": true,
        });
        let result_digest = canonical_result_digest(&terminal);
        let receipt = authority
            .finish(
                pending,
                store("/workspace/project"),
                result_digest.clone(),
                &generation(),
                now + 1,
            )
            .expect("finish error action");
        assert_eq!(receipt.result_digest, result_digest);
        assert_eq!(receipt.receipt_mac.len(), 64);
        assert_eq!(receipt.receipt_sha256.len(), 64);
    }

    #[test]
    fn route_assertion_is_checked_but_receipt_route_comes_from_observed_dispatch() {
        let authority = ActionReceiptAuthority::new();
        let now = 1_000;
        authority
            .prepare(
                prepare(now),
                generation(),
                store("/workspace/project"),
                ACTION_ENTRYPOINT_MCP_STDIO,
                now,
            )
            .expect("prepare");
        let mut wrong_route = ActionCallMetadata::new(hex('a')).expect("call");
        wrong_route.route = Some("wrong-route".to_owned());
        assert!(matches!(
            authority.begin(
                &wrong_route,
                "tracedecay_search",
                "route-1",
                ACTION_ENTRYPOINT_MCP_STDIO,
                &args(),
                &store("/workspace/project"),
                &generation(),
                now,
            ),
            Err(ActionReceiptError::ActionMismatch("route"))
        ));
        let call = ActionCallMetadata::new(hex('a')).expect("call");
        let pending = authority
            .begin(
                &call,
                "tracedecay_search",
                "route-1",
                ACTION_ENTRYPOINT_MCP_STDIO,
                &args(),
                &store("/workspace/project"),
                &generation(),
                now,
            )
            .expect("matching observed route");
        let receipt = authority
            .finish(
                pending,
                store("/workspace/project"),
                canonical_result_digest(&serde_json::json!({"ok":true})),
                &generation(),
                now + 1,
            )
            .expect("finish");
        assert_eq!(receipt.route, "route-1");
    }

    #[test]
    fn begin_binds_full_store_identity_and_cleanup_releases_expired_keys() {
        let authority = ActionReceiptAuthority::new();
        let now = 1_000;
        authority
            .prepare(
                prepare(now),
                generation(),
                store("/workspace/project"),
                ACTION_ENTRYPOINT_MCP_STDIO,
                now,
            )
            .expect("prepare");
        let changed_store = LiveStoreIdentity::new(
            Some("project-1".to_owned()),
            "/workspace/project",
            "/profile/data/project-1/changed",
            "/profile/data/project-1/tracedecay.db",
            Some("main".to_owned()),
        )
        .expect("changed store");
        let call = ActionCallMetadata::new(hex('a')).expect("call");
        assert!(matches!(
            authority.begin(
                &call,
                "tracedecay_search",
                "route-1",
                ACTION_ENTRYPOINT_MCP_STDIO,
                &args(),
                &changed_store,
                &generation(),
                now,
            ),
            Err(ActionReceiptError::StoreIdentityMismatch)
        ));

        let cleanup_authority = ActionReceiptAuthority::new();
        cleanup_authority
            .prepare(
                prepare(now),
                generation(),
                store("/workspace/project"),
                ACTION_ENTRYPOINT_MCP_STDIO,
                now,
            )
            .expect("prepare cleanup reservation");
        assert_eq!(cleanup_authority.cleanup_expired(now + 10_000), 1);
        assert!(matches!(
            cleanup_authority.begin(
                &call,
                "tracedecay_search",
                "route-1",
                ACTION_ENTRYPOINT_MCP_STDIO,
                &args(),
                &store("/workspace/project"),
                &generation(),
                now,
            ),
            Err(ActionReceiptError::Replay)
        ));
    }

    #[test]
    fn prepare_and_call_wire_metadata_parse_without_secret_echo() {
        let now = 1_000;
        let request = prepare(now);
        let params = serde_json::json!({
            "_meta": {
                ACTION_PREPARE_META_KEY: {
                    "format": ACTION_RECEIPT_FORMAT,
                    "revision": ACTION_RECEIPT_REVISION,
                    "action_id": request.action_id,
                    "route": request.route,
                    "nonce": request.nonce,
                    "proof_key": hex('1'),
                    "action_digest": request.action_digest,
                    "scope": request.scope,
                    "tool": request.tool,
                    "entrypoint": request.entrypoint,
                    "expires_at": request.expires_at,
                }
            }
        });
        let parsed = parse_prepare_from_initialize(&params)
            .expect("parse prepare")
            .expect("prepare metadata");
        assert!(!format!("{parsed:?}").contains(&hex('1')));
        let call_params = serde_json::json!({"_meta":{"nativeOriginalActionNonce":hex('a')}});
        assert_eq!(
            parse_call_metadata_from_params(Some(&call_params)).expect("parse call"),
            Some(ActionCallMetadata::new(hex('a')).expect("call"))
        );
    }
}
