//! V2 request contracts for one authoritative code-index reconciliation.
//!
//! Folder selections are deliberately request scoped.  They are normalized and
//! digested at the transport boundary, then carried through the daemon to one
//! scheduler pass.  They never become part of the durable project
//! configuration.

use std::path::Path;

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tracedecay_domain::{ManifestDigest, canonical_sha256};

const MAX_FOLDER_SELECTIONS_V1: usize = 256;
const MAX_FOLDER_SELECTION_BYTES_V1: usize = 4096;

/// Validation failure for an ephemeral code-index folder selection.
#[derive(Clone, Debug, Error, PartialEq, Eq)]
pub enum CodeIndexReconcileOptionsErrorV1 {
    #[error("{field} folder `{value}` is invalid: {reason}")]
    InvalidFolder {
        field: &'static str,
        value: String,
        reason: &'static str,
    },
    #[error("{field} contains too many folders (maximum {maximum})")]
    TooManyFolders { field: &'static str, maximum: usize },
    #[error("{field} exceeds the total folder-selection size limit")]
    FolderSelectionTooLarge { field: &'static str },
    #[error("could not digest the code-index folder selection: {0}")]
    Digest(String),
}

/// Per-invocation folder scope for one code-index reconcile.
///
/// Paths are repository-relative, slash separated, deduplicated, and sorted.
/// An included descendant wins over a skipped ancestor, which permits useful
/// requests such as `--skip-folder dist --include-folder dist/generated`.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct CodeIndexReconcileOptionsV1 {
    #[serde(default)]
    pub skip_folders: Vec<String>,
    #[serde(default)]
    pub include_folders: Vec<String>,
}

impl CodeIndexReconcileOptionsV1 {
    /// Validate and canonicalize one request's folder selections.
    pub fn new(
        skip_folders: impl IntoIterator<Item = String>,
        include_folders: impl IntoIterator<Item = String>,
    ) -> Result<Self, CodeIndexReconcileOptionsErrorV1> {
        Ok(Self {
            skip_folders: normalize_folders("skip_folders", skip_folders)?,
            include_folders: normalize_folders("include_folders", include_folders)?,
        })
    }

    /// Whether this request has no folder override and can use the ordinary
    /// scheduler cursor/reuse path.
    pub fn is_default(&self) -> bool {
        self.skip_folders.is_empty() && self.include_folders.is_empty()
    }

    /// Canonical digest of the request-scoped folder policy.  The digest is a
    /// cursor component for diagnostics and stale-pass fencing; it is never
    /// written to persistent project configuration.
    pub fn digest(&self) -> Result<ManifestDigest, CodeIndexReconcileOptionsErrorV1> {
        canonical_sha256(&(&self.skip_folders, &self.include_folders))
            .map_err(|error| CodeIndexReconcileOptionsErrorV1::Digest(error.to_string()))
    }

    /// True when `path` is inside one of the explicitly included folders.
    pub fn includes_path(&self, path: &str) -> bool {
        self.include_folders
            .iter()
            .any(|folder| folder_matches(folder, path))
    }

    /// True when `path` is skipped by this request.  An explicit include has
    /// precedence over every skipped ancestor.
    pub fn skips_path(&self, path: &str) -> bool {
        !self.includes_path(path)
            && self
                .skip_folders
                .iter()
                .any(|folder| folder_matches(folder, path))
    }
}

/// Wire request accepted by `tracedecay_admin_sync`.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct CodeIndexReconcileRequestV1 {
    #[serde(default)]
    pub force: bool,
    #[serde(flatten)]
    pub options: CodeIndexReconcileOptionsV1,
}

impl CodeIndexReconcileRequestV1 {
    /// Construct a validated request from the CLI/JSON argument shape.
    pub fn new(
        force: bool,
        skip_folders: impl IntoIterator<Item = String>,
        include_folders: impl IntoIterator<Item = String>,
    ) -> Result<Self, CodeIndexReconcileOptionsErrorV1> {
        Ok(Self {
            force,
            options: CodeIndexReconcileOptionsV1::new(skip_folders, include_folders)?,
        })
    }

    /// Decode and validate the JSON-RPC argument object.  Serde's structural
    /// error remains typed as a request validation failure at the daemon edge.
    pub fn from_json(value: serde_json::Value) -> Result<Self, CodeIndexReconcileOptionsErrorV1> {
        let request: Self = serde_json::from_value(value).map_err(|error| {
            CodeIndexReconcileOptionsErrorV1::InvalidFolder {
                field: "admin_sync",
                value: error.to_string(),
                reason: "request arguments must contain boolean force and string folder arrays",
            }
        })?;
        Self::new(
            request.force,
            request.options.skip_folders,
            request.options.include_folders,
        )
    }
}

fn normalize_folders(
    field: &'static str,
    folders: impl IntoIterator<Item = String>,
) -> Result<Vec<String>, CodeIndexReconcileOptionsErrorV1> {
    let mut normalized = Vec::new();
    let mut total_bytes = 0usize;
    for value in folders {
        if normalized.len() >= MAX_FOLDER_SELECTIONS_V1 {
            return Err(CodeIndexReconcileOptionsErrorV1::TooManyFolders {
                field,
                maximum: MAX_FOLDER_SELECTIONS_V1,
            });
        }
        total_bytes = total_bytes.saturating_add(value.len());
        if total_bytes > MAX_FOLDER_SELECTION_BYTES_V1 {
            return Err(CodeIndexReconcileOptionsErrorV1::FolderSelectionTooLarge { field });
        }
        let canonical = normalize_folder(field, &value)?;
        if !normalized.contains(&canonical) {
            normalized.push(canonical);
        }
    }
    normalized.sort();
    Ok(normalized)
}

fn normalize_folder(
    field: &'static str,
    value: &str,
) -> Result<String, CodeIndexReconcileOptionsErrorV1> {
    if value.is_empty() {
        return Err(CodeIndexReconcileOptionsErrorV1::InvalidFolder {
            field,
            value: value.to_owned(),
            reason: "must not be empty",
        });
    }
    if value.as_bytes().contains(&0) {
        return Err(CodeIndexReconcileOptionsErrorV1::InvalidFolder {
            field,
            value: value.to_owned(),
            reason: "must not contain NUL",
        });
    }
    if value.contains('\\') {
        return Err(CodeIndexReconcileOptionsErrorV1::InvalidFolder {
            field,
            value: value.to_owned(),
            reason: "must use `/` separators",
        });
    }
    let path = Path::new(value);
    if path.is_absolute() {
        return Err(CodeIndexReconcileOptionsErrorV1::InvalidFolder {
            field,
            value: value.to_owned(),
            reason: "must be repository-relative",
        });
    }
    let mut components = Vec::new();
    for component in value.split('/') {
        if component.is_empty() || component == "." || component == ".." {
            return Err(CodeIndexReconcileOptionsErrorV1::InvalidFolder {
                field,
                value: value.to_owned(),
                reason: "must contain only non-empty normal path components",
            });
        }
        components.push(component);
    }
    if components.is_empty() {
        return Err(CodeIndexReconcileOptionsErrorV1::InvalidFolder {
            field,
            value: value.to_owned(),
            reason: "must contain a folder path",
        });
    }
    Ok(components.join("/"))
}

fn folder_matches(folder: &str, path: &str) -> bool {
    path == folder
        || path
            .strip_prefix(folder)
            .is_some_and(|suffix| suffix.starts_with('/'))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn canonicalizes_and_digests_folder_selection() {
        let options = CodeIndexReconcileOptionsV1::new(
            ["dist".to_owned(), "vendor".to_owned(), "dist".to_owned()],
            ["dist/generated".to_owned()],
        )
        .expect("valid folder selection");
        assert_eq!(options.skip_folders, ["dist", "vendor"]);
        assert!(options.skips_path("vendor/tool.rs"));
        assert!(!options.skips_path("dist/generated/schema.rs"));
        assert!(options.includes_path("dist/generated/schema.rs"));
        assert_eq!(
            options.digest().expect("digest"),
            options.digest().expect("digest")
        );
    }

    #[test]
    fn rejects_ambiguous_or_escaping_paths() {
        for value in [
            "",
            "/tmp/source",
            "../source",
            "source//nested",
            "source\\nested",
        ] {
            assert!(
                CodeIndexReconcileOptionsV1::new([value.to_owned()], []).is_err(),
                "path `{value}` must be rejected"
            );
        }
    }

    #[test]
    fn decodes_flattened_wire_request() {
        let request = CodeIndexReconcileRequestV1::from_json(serde_json::json!({
            "force": true,
            "skip_folders": ["vendor"],
            "include_folders": ["dist/generated"],
        }))
        .expect("wire request");
        assert!(request.force);
        assert_eq!(request.options.skip_folders, ["vendor"]);
    }
}
