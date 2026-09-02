//! Canonical (RFC 8785 style) JSON bytes for evaluation artifacts.
//!
//! Reports are compared byte-for-byte across reruns, so their serialization
//! must not depend on struct field order, map implementation, or float
//! formatting. Every evaluation artifact is integer-only; a non-integer number
//! is a typed error rather than a silently rounded value.

use std::error::Error;
use std::fmt;

use serde::Serialize;
use serde_json::Value;
use sha2::{Digest, Sha256};

/// Canonical serialization failure.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum CanonicalJsonError {
    /// The value could not be represented as JSON at all.
    Serialization(String),
    /// A number was not an integer and cannot be emitted canonically.
    NonIntegerNumber {
        /// Path of the offending number.
        path: String,
    },
}

impl fmt::Display for CanonicalJsonError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Serialization(detail) => {
                write!(formatter, "value cannot be serialized as JSON: {detail}")
            }
            Self::NonIntegerNumber { path } => write!(
                formatter,
                "canonical evaluation artifacts are integer-only; {path} is not an integer"
            ),
        }
    }
}

impl Error for CanonicalJsonError {}

/// Serializes a value as canonical JSON: sorted object keys, no whitespace,
/// integer-only numbers, serde_json string escaping.
pub fn canonical_json<T: Serialize>(value: &T) -> Result<Vec<u8>, CanonicalJsonError> {
    let value = serde_json::to_value(value)
        .map_err(|error| CanonicalJsonError::Serialization(error.to_string()))?;
    let mut output = Vec::new();
    write_canonical(&value, &mut output, "$")?;
    Ok(output)
}

/// Serializes a value as canonical JSON and returns its lowercase SHA-256.
pub fn canonical_json_sha256<T: Serialize>(value: &T) -> Result<String, CanonicalJsonError> {
    let bytes = canonical_json(value)?;
    Ok(lowercase_sha256_hex(Sha256::digest(&bytes).into()))
}

fn write_canonical(
    value: &Value,
    output: &mut Vec<u8>,
    path: &str,
) -> Result<(), CanonicalJsonError> {
    match value {
        Value::Null => output.extend_from_slice(b"null"),
        Value::Bool(true) => output.extend_from_slice(b"true"),
        Value::Bool(false) => output.extend_from_slice(b"false"),
        Value::Number(number) => {
            if number.is_i64() || number.is_u64() {
                output.extend_from_slice(number.to_string().as_bytes());
            } else {
                return Err(CanonicalJsonError::NonIntegerNumber {
                    path: path.to_owned(),
                });
            }
        }
        Value::String(text) => write_string(text, output)?,
        Value::Array(items) => {
            output.push(b'[');
            for (index, item) in items.iter().enumerate() {
                if index > 0 {
                    output.push(b',');
                }
                write_canonical(item, output, &format!("{path}[{index}]"))?;
            }
            output.push(b']');
        }
        Value::Object(members) => {
            let mut keys: Vec<&String> = members.keys().collect();
            keys.sort_by(|left, right| {
                left.encode_utf16()
                    .collect::<Vec<_>>()
                    .cmp(&right.encode_utf16().collect::<Vec<_>>())
            });
            output.push(b'{');
            for (index, key) in keys.iter().enumerate() {
                if index > 0 {
                    output.push(b',');
                }
                write_string(key, output)?;
                output.push(b':');
                if let Some(member) = members.get(*key) {
                    write_canonical(member, output, &format!("{path}.{key}"))?;
                }
            }
            output.push(b'}');
        }
    }
    Ok(())
}

fn write_string(text: &str, output: &mut Vec<u8>) -> Result<(), CanonicalJsonError> {
    let encoded = serde_json::to_vec(text)
        .map_err(|error| CanonicalJsonError::Serialization(error.to_string()))?;
    output.extend_from_slice(&encoded);
    Ok(())
}

/// Formats a digest as lowercase hexadecimal.
#[must_use]
pub fn lowercase_sha256_hex(digest: [u8; 32]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut output = String::with_capacity(64);
    for byte in digest {
        output.push(char::from(HEX[usize::from(byte >> 4)]));
        output.push(char::from(HEX[usize::from(byte & 0x0f)]));
    }
    output
}
