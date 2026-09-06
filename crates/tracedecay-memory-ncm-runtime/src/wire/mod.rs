//! Bounded, versioned JSON framing for the supervised NCM worker.
//!
//! Every frame is a four-byte big-endian payload length followed by one JSON
//! document. Declared lengths are checked before allocating the payload buffer.

use crate::engine::{EngineReply, Outcome, RejectReason};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::fmt;
use std::io::{self, Read, Write};

/// Current local worker protocol version.
pub const PROTOCOL_VERSION: u16 = 1;
/// Largest accepted request JSON payload.
pub const MAX_REQUEST_BYTES: usize = 256 * 1024;
/// Largest emitted or accepted reply JSON payload.
pub const MAX_REPLY_BYTES: usize = 1024 * 1024;

const fn default_protocol_version() -> u16 {
    PROTOCOL_VERSION
}

/// Engine operation carried over the local worker pipe.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Operation {
    /// Readiness and compatibility check.
    Handshake,
    /// Process and encoder health.
    Health,
    /// Exactly-once text observation.
    Observe,
    /// Immutable text recall.
    Recall,
    /// Explicit usage feedback.
    Feedback,
    /// Supersession lineage mutation.
    Correction,
    /// Bounded maintenance mutation.
    Maintenance,
    /// Redacted namespace inspection.
    Inspection,
    /// Delete all records admitted under one opaque source handle.
    DeleteBySource,
    /// Export a versioned snapshot.
    SnapshotExport,
    /// Restore a versioned snapshot.
    SnapshotRestore,
    /// Replay operation, unsupported in protocol v1.
    Replay,
}

impl Operation {
    /// Whether transport loss can leave an indeterminate durable effect.
    #[must_use]
    pub const fn is_mutating(self) -> bool {
        matches!(
            self,
            Self::Observe
                | Self::Feedback
                | Self::Correction
                | Self::Maintenance
                | Self::DeleteBySource
                | Self::SnapshotRestore
        )
    }
}

/// One typed worker request.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Request {
    /// Protocol version. Omitted JSON fields default to the current version for
    /// compatibility with the frozen v1 request shape.
    #[serde(default = "default_protocol_version")]
    pub protocol_version: u16,
    /// Caller-chosen request identifier echoed by the reply.
    pub id: u64,
    /// Remaining operation budget in milliseconds.
    pub deadline_ms: u64,
    /// Requested engine operation.
    pub op: Operation,
    /// Opaque, host-admitted namespace handle.
    pub namespace: String,
    /// Operation-specific typed JSON object.
    pub payload: Value,
}

impl Request {
    /// Constructs a request using the current protocol version.
    #[must_use]
    pub fn new(
        id: u64,
        deadline_ms: u64,
        op: Operation,
        namespace: impl Into<String>,
        payload: Value,
    ) -> Self {
        Self {
            protocol_version: PROTOCOL_VERSION,
            id,
            deadline_ms,
            op,
            namespace: namespace.into(),
            payload,
        }
    }
}

/// Typed transport error returned inside a syntactically valid reply.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReplyError {
    /// Stable machine-readable error kind.
    pub kind: String,
    /// Bounded human-readable diagnostic detail.
    pub detail: String,
}

/// One worker response.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Reply {
    /// Echoed request identifier, or zero when malformed JSON hid the id.
    pub id: u64,
    /// Typed engine or transport outcome.
    pub outcome: Outcome,
    /// Durable namespace commit sequence represented by this reply.
    pub state_generation: u64,
    /// Operation payload on successful or empty execution.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub payload: Option<Value>,
    /// Typed transport/dispatch diagnostic on failure.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<ReplyError>,
}

impl Reply {
    /// Converts an engine reply without losing its typed outcome.
    #[must_use]
    pub fn from_engine(id: u64, reply: EngineReply) -> Self {
        let successful = matches!(reply.outcome, Outcome::Success | Outcome::Empty);
        let payload = (reply.payload != Value::Null).then_some(reply.payload);
        let error = if successful {
            None
        } else {
            Some(ReplyError {
                kind: outcome_kind(&reply.outcome).to_owned(),
                detail: format!("{:?}", reply.outcome),
            })
        };
        Self {
            id,
            outcome: reply.outcome,
            state_generation: reply.state_generation,
            payload,
            error,
        }
    }

    /// Constructs a pre-effect protocol rejection.
    #[must_use]
    pub fn protocol_error(id: u64, kind: &str, detail: impl Into<String>) -> Self {
        let detail = detail.into();
        Self {
            id,
            outcome: Outcome::Rejected(RejectReason::InvalidRequest(detail.clone())),
            state_generation: 0,
            payload: None,
            error: Some(ReplyError {
                kind: kind.to_owned(),
                detail,
            }),
        }
    }
}

fn outcome_kind(outcome: &Outcome) -> &'static str {
    match outcome {
        Outcome::Success => "success",
        Outcome::Empty => "empty",
        Outcome::Rejected(_) => "rejected",
        Outcome::Busy => "busy",
        Outcome::Cancelled => "cancelled",
        Outcome::EffectUnknown => "effect_unknown",
        Outcome::Incompatible => "incompatible",
        Outcome::Corrupt => "corrupt",
        Outcome::Unavailable(_) => "unavailable",
        Outcome::Unsupported => "unsupported",
        Outcome::BudgetExceeded => "budget_exceeded",
    }
}

/// Framing or serialization failure.
#[derive(Debug)]
pub enum FrameError {
    /// Underlying pipe I/O failed.
    Io(io::Error),
    /// Declared or encoded payload exceeded its direction-specific limit.
    Oversized {
        /// Declared or encoded bytes.
        length: usize,
        /// Enforced maximum bytes.
        limit: usize,
    },
    /// A complete bounded frame did not contain the expected JSON type.
    MalformedJson(String),
    /// The stream ended in the middle of a frame header or body.
    Truncated,
}

impl FrameError {
    /// Whether another frame can be attempted on the stream.
    #[must_use]
    pub const fn is_recoverable(&self) -> bool {
        matches!(self, Self::MalformedJson(_) | Self::Oversized { .. })
    }

    /// Stable reply error kind.
    #[must_use]
    pub const fn kind(&self) -> &'static str {
        match self {
            Self::Io(_) => "io",
            Self::Oversized { .. } => "oversized_frame",
            Self::MalformedJson(_) => "malformed_json",
            Self::Truncated => "truncated_frame",
        }
    }
}

impl fmt::Display for FrameError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(error) => write!(formatter, "pipe I/O: {error}"),
            Self::Oversized { length, limit } => {
                write!(formatter, "frame length {length} exceeds {limit}")
            }
            Self::MalformedJson(detail) => write!(formatter, "malformed JSON: {detail}"),
            Self::Truncated => formatter.write_str("truncated frame"),
        }
    }
}

impl std::error::Error for FrameError {}

impl From<io::Error> for FrameError {
    fn from(error: io::Error) -> Self {
        Self::Io(error)
    }
}

/// Serializes and bounds one request frame, including its four-byte header.
pub fn encode_request(request: &Request) -> Result<Vec<u8>, FrameError> {
    encode_frame(request, MAX_REQUEST_BYTES)
}

/// Serializes and bounds one reply frame, including its four-byte header.
pub fn encode_reply(reply: &Reply) -> Result<Vec<u8>, FrameError> {
    encode_frame(reply, MAX_REPLY_BYTES)
}

fn encode_frame<T: Serialize>(value: &T, limit: usize) -> Result<Vec<u8>, FrameError> {
    let mut bounded = BoundedBuffer::new(limit);
    if let Err(error) = serde_json::to_writer(&mut bounded, value) {
        if bounded.exceeded {
            return Err(FrameError::Oversized {
                length: limit.saturating_add(1),
                limit,
            });
        }
        return Err(FrameError::MalformedJson(error.to_string()));
    }
    let payload = bounded.bytes;
    let length = u32::try_from(payload.len()).map_err(|_| FrameError::Oversized {
        length: payload.len(),
        limit,
    })?;
    let mut frame = Vec::with_capacity(payload.len().saturating_add(4));
    frame.extend_from_slice(&length.to_be_bytes());
    frame.extend_from_slice(&payload);
    Ok(frame)
}

struct BoundedBuffer {
    bytes: Vec<u8>,
    limit: usize,
    exceeded: bool,
}

impl BoundedBuffer {
    fn new(limit: usize) -> Self {
        Self {
            bytes: Vec::with_capacity(limit.min(4096)),
            limit,
            exceeded: false,
        }
    }
}

impl Write for BoundedBuffer {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        let new_length = self.bytes.len().checked_add(bytes.len());
        if new_length.is_none_or(|length| length > self.limit) {
            self.exceeded = true;
            return Err(io::Error::other("serialized frame exceeds bound"));
        }
        self.bytes.extend_from_slice(bytes);
        Ok(bytes.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

/// Reads one bounded request. Clean EOF before a header returns `Ok(None)`.
pub fn read_request(reader: &mut impl Read) -> Result<Option<Request>, FrameError> {
    read_frame(reader, MAX_REQUEST_BYTES)
}

/// Reads one bounded reply. Clean EOF before a header returns `Ok(None)`.
pub fn read_reply(reader: &mut impl Read) -> Result<Option<Reply>, FrameError> {
    read_frame(reader, MAX_REPLY_BYTES)
}

fn read_frame<T: for<'de> Deserialize<'de>>(
    reader: &mut impl Read,
    limit: usize,
) -> Result<Option<T>, FrameError> {
    let Some(length) = read_length(reader)? else {
        return Ok(None);
    };
    let length = length as usize;
    if length > limit {
        return Err(FrameError::Oversized { length, limit });
    }
    let mut payload = vec![0_u8; length];
    if let Err(error) = reader.read_exact(&mut payload) {
        return if error.kind() == io::ErrorKind::UnexpectedEof {
            Err(FrameError::Truncated)
        } else {
            Err(FrameError::Io(error))
        };
    }
    serde_json::from_slice(&payload)
        .map(Some)
        .map_err(|error| FrameError::MalformedJson(error.to_string()))
}

fn read_length(reader: &mut impl Read) -> Result<Option<u32>, FrameError> {
    let mut header = [0_u8; 4];
    let mut read = 0_usize;
    loop {
        match reader.read(&mut header[read..]) {
            Ok(0) if read == 0 => return Ok(None),
            Ok(0) => return Err(FrameError::Truncated),
            Ok(count) => {
                read = read.saturating_add(count);
                if read == header.len() {
                    return Ok(Some(u32::from_be_bytes(header)));
                }
            }
            Err(error) if error.kind() == io::ErrorKind::Interrupted => {}
            Err(error) => return Err(FrameError::Io(error)),
        }
    }
}

/// Writes one bounded reply and flushes it as a complete frame.
pub fn write_reply(writer: &mut impl Write, reply: &Reply) -> Result<(), FrameError> {
    let frame = encode_reply(reply)?;
    writer.write_all(&frame)?;
    writer.flush()?;
    Ok(())
}
