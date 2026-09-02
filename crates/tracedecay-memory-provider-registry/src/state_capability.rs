//! The host-owned state authority a supervised provider's state is contained
//! by, and the capability that is the only way to obtain a provider state
//! path.
//!
//! # Why a capability rather than a validated string
//!
//! A provider reports the state namespace its incarnation loaded, and the
//! supervisor validates that string's shape and admission
//! ([`crate::supervisor::ReadinessDefectV1::StateNamespaceEscapesContainment`],
//! [`crate::supervisor::ReadinessDefectV1::StateNamespaceNotAdmitted`]). A
//! validated string is evidence, not containment: nothing about accepting the
//! name `tracedecay.native.project` decides *where* that state may live.
//!
//! [`ProviderStateAuthorityV1`] is the containment. The **host** owns an
//! absolute root, the host resolves the admitted namespace under it, and a
//! validated readiness is granted a [`ProviderStateCapabilityV1`] — the only
//! value in this crate from which a provider state path can be obtained at
//! all. Every path it hands out is proven to stay under the granted root: an
//! absolute path, a parent traversal, a foreign separator, a percent-encoded
//! traversal, or any non-normal component is a typed refusal, and a refusal
//! yields no path for a caller to write through.
//!
//! # No I/O here, deliberately
//!
//! This crate is source-contracted (`product/architecture/memory-dependency-policy.json`)
//! and may name no filesystem, process, network, or thread capability: it is
//! the host's *authority* layer, not its OS layer. So this module decides
//! **which path is admissible** and nothing else; the composition root, which
//! owns the store layout, creates the root directory and performs the I/O —
//! and can only ever do it at a path this capability resolved.
//!
//! # What this bounds, and what it does not
//!
//! For a process topology (ADR-0009) the granted root is what the child's
//! filesystem authority is restricted to, and the OS enforces the rest. For an
//! **in-process** provider — TraceDecay Native today — this bounds every path
//! the host derives and hands out, and it is the only state authority the host
//! grants; it cannot revoke the ambient authority in-process code has by
//! virtue of running inside the host process. That residual is a property of
//! in-process composition, not of this type, and it is what the process
//! topology removes.

use std::fmt;
use std::path::{Component, Path, PathBuf};

/// Maximum bytes one provider-relative state path may occupy.
const MAX_RELATIVE_PATH_BYTES: usize = 512;

/// Whether `value` is shaped so it could address storage outside a host-owned
/// root once joined to one.
///
/// Deliberately structural: absolute forms, parent traversal, dotted or empty
/// segments, foreign separators, percent-encoding a later decoder could turn
/// back into any of those, and home-relative forms are all refused.
#[must_use]
pub fn escapes_containment(value: &str) -> bool {
    value.starts_with('/')
        || value.starts_with('.')
        || value.ends_with('/')
        || value.ends_with('.')
        || value.contains("..")
        || value.contains("//")
        || value.contains("./")
        || value.contains("/.")
        || value.contains('\\')
        || value.contains(':')
        || value.contains('%')
        || value.contains('~')
}

/// Whether every character of `value` is one the host owns in a state path.
#[must_use]
pub fn charset_usable(value: &str) -> bool {
    value.chars().all(|character| {
        character.is_ascii_alphanumeric() || matches!(character, '.' | '-' | '_' | '/')
    })
}

/// Whether `value` is a usable, non-escaping provider-relative path.
fn contained_relative_path(value: &str) -> bool {
    if value.is_empty() || value.len() > MAX_RELATIVE_PATH_BYTES {
        return false;
    }
    if escapes_containment(value) || !charset_usable(value) {
        return false;
    }
    // Defence in depth against a platform that reads a component differently
    // than the structural rule above does.
    Path::new(value)
        .components()
        .all(|component| matches!(component, Component::Normal(_)))
}

/// Why a host could not open or grant a provider state authority.
#[derive(Clone, Debug, Eq, PartialEq, thiserror::Error)]
pub enum ProviderStateAuthorityError {
    /// The host-owned root is not an absolute, normalized directory path, so
    /// nothing can be contained relative to it.
    #[error("provider state root {path} is not an absolute normalized host-owned path")]
    RootUnusable {
        /// Root the host offered.
        path: PathBuf,
    },
    /// The namespace is not a contained relative path, so no directory under
    /// the root can represent it.
    #[error("provider state namespace {state_namespace} is not a contained relative path")]
    NamespaceUnusable {
        /// Namespace the caller asked to grant.
        state_namespace: String,
    },
    /// The resolved namespace directory does not sit under the host-owned
    /// root.
    #[error("provider state namespace {state_namespace} resolved outside the host-owned root")]
    NamespaceEscapesRoot {
        /// Namespace the caller asked to grant.
        state_namespace: String,
    },
}

/// Why one capability-mediated state path was refused.
#[derive(Clone, Debug, Eq, PartialEq, thiserror::Error)]
pub enum ProviderStateAccessError {
    /// The relative path is empty, oversized, or carries characters the host
    /// does not own.
    #[error("provider state path {relative} is unusable")]
    UnusablePath {
        /// Path the provider asked for.
        relative: String,
    },
    /// The relative path is shaped so it could address storage outside the
    /// granted root. No path is produced, so there is nothing to write
    /// through.
    #[error("provider state path {relative} escapes the granted state root")]
    EscapesRoot {
        /// Path the provider asked for.
        relative: String,
    },
}

/// The host's own root for every supervised provider's state.
///
/// Constructed by the composition root from an absolute directory the host
/// owns. Granting is the only way a [`ProviderStateCapabilityV1`] comes into
/// existence.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProviderStateAuthorityV1 {
    root: PathBuf,
}

impl ProviderStateAuthorityV1 {
    /// Binds the host-owned root every grant sits under.
    ///
    /// The root must be absolute and free of `.`, `..`, and other non-normal
    /// components: a root a caller could re-interpret is not a root.
    pub fn new(root: impl Into<PathBuf>) -> Result<Self, ProviderStateAuthorityError> {
        let root = root.into();
        let normalized = root.is_absolute()
            && root
                .components()
                .all(|component| matches!(component, Component::RootDir | Component::Normal(_)));
        if !normalized {
            return Err(ProviderStateAuthorityError::RootUnusable { path: root });
        }
        Ok(Self { root })
    }

    /// The host-owned root.
    #[must_use]
    pub fn root(&self) -> &Path {
        &self.root
    }

    /// Grants exactly one namespace's state capability under the root.
    ///
    /// The namespace must be a contained relative path; the caller — the
    /// supervisor's readiness validator — has additionally proven it is inside
    /// the prefix this provider was admitted to own.
    pub fn grant(
        &self,
        state_namespace: &str,
    ) -> Result<ProviderStateCapabilityV1, ProviderStateAuthorityError> {
        if !contained_relative_path(state_namespace) {
            return Err(ProviderStateAuthorityError::NamespaceUnusable {
                state_namespace: state_namespace.to_owned(),
            });
        }
        let granted = self.root.join(state_namespace);
        if !granted.starts_with(&self.root) {
            return Err(ProviderStateAuthorityError::NamespaceEscapesRoot {
                state_namespace: state_namespace.to_owned(),
            });
        }
        Ok(ProviderStateCapabilityV1 {
            state_namespace: state_namespace.to_owned(),
            root: granted,
        })
    }
}

/// One incarnation's only state-path authority.
///
/// Carried on [`crate::supervisor::ReadinessEvidenceV1`], so the root a host
/// grants and the namespace it validated are one piece of evidence rather than
/// two facts that could drift apart.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProviderStateCapabilityV1 {
    state_namespace: String,
    root: PathBuf,
}

impl ProviderStateCapabilityV1 {
    /// The namespace this capability was granted for.
    #[must_use]
    pub fn state_namespace(&self) -> &str {
        &self.state_namespace
    }

    /// The directory every resolved path sits under.
    #[must_use]
    pub fn root(&self) -> &Path {
        &self.root
    }

    /// Resolves one provider-relative path inside the granted root, or refuses
    /// it typed.
    ///
    /// This is the only way to turn a provider-supplied name into a path a
    /// host would open. A refusal produces no path at all, which is what makes
    /// "the write never happens" a structural property rather than a caller's
    /// discipline.
    pub fn resolve(&self, relative: &str) -> Result<PathBuf, ProviderStateAccessError> {
        if relative.is_empty()
            || relative.len() > MAX_RELATIVE_PATH_BYTES
            || !charset_usable(relative)
        {
            return Err(ProviderStateAccessError::UnusablePath {
                relative: relative.to_owned(),
            });
        }
        if !contained_relative_path(relative) {
            return Err(ProviderStateAccessError::EscapesRoot {
                relative: relative.to_owned(),
            });
        }
        let resolved = self.root.join(relative);
        if !resolved.starts_with(&self.root) {
            return Err(ProviderStateAccessError::EscapesRoot {
                relative: relative.to_owned(),
            });
        }
        Ok(resolved)
    }
}

impl fmt::Display for ProviderStateCapabilityV1 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "provider state capability for {} at {}",
            self.state_namespace,
            self.root.display()
        )
    }
}
