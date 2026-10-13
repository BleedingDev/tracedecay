//! Published platform capability for the pinned NCM worker distribution.
//!
//! The NCM algorithms are portable Rust, but the production worker is a
//! separately verified executable. A target is supported exactly when the
//! build's trusted worker manifest
//! ([`crate::worker_artifact::trusted_worker_manifest`]) pins that triple with
//! matching platform metadata. The checked-in trust root pins nothing, so a
//! plain source build reports every target as unsupported; release builds
//! select a pinned manifest at build time. Unsupported targets remain usable
//! for the host with Native memory and report typed NCM unavailability.

use crate::worker_artifact::{
    CurrentTarget, WorkerIntegrityError, current_target, manifest_pins_target,
    trusted_worker_manifest,
};
use std::fmt;

/// The result of resolving the NCM worker capability for a target triple.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum NcmWorkerPlatformCapability {
    /// A verified NCM worker artifact is advertised for this target.
    Supported {
        /// Rust target triple backed by the pinned artifact.
        target: String,
    },
    /// No verified NCM worker artifact is advertised for this target.
    Unsupported {
        /// Rust target triple that could not be admitted.
        target: String,
    },
    /// The build's trusted worker manifest is malformed, so no target can be
    /// admitted.
    InvalidTrustRoot {
        /// Rust target triple that could not be admitted.
        target: String,
        /// Why the trusted manifest was rejected.
        detail: String,
    },
}

impl NcmWorkerPlatformCapability {
    /// Returns the resolved Rust target triple.
    #[must_use]
    pub fn target(&self) -> &str {
        match self {
            Self::Supported { target }
            | Self::Unsupported { target }
            | Self::InvalidTrustRoot { target, .. } => target,
        }
    }

    /// Returns whether the pinned worker can be advertised for this target.
    #[must_use]
    pub const fn is_supported(&self) -> bool {
        matches!(self, Self::Supported { .. })
    }
}

impl fmt::Display for NcmWorkerPlatformCapability {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Supported { target } => write!(formatter, "NCM worker supported on {target}"),
            Self::Unsupported { target } => {
                write!(
                    formatter,
                    "NCM worker unsupported on {target}; use Native-only mode"
                )
            }
            Self::InvalidTrustRoot { target, detail } => write!(
                formatter,
                "NCM worker trust root is invalid on {target} ({detail}); use Native-only mode"
            ),
        }
    }
}

/// Resolves the worker capability for the current compilation target against
/// the build's trusted worker manifest.
#[must_use]
pub fn current_worker_platform_capability() -> NcmWorkerPlatformCapability {
    capability_in(trusted_worker_manifest(), &current_target())
}

fn capability_in(manifest: &str, target: &CurrentTarget) -> NcmWorkerPlatformCapability {
    let triple = target.triple.to_owned();
    match manifest_pins_target(manifest, target) {
        Ok(()) => NcmWorkerPlatformCapability::Supported { target: triple },
        Err(WorkerIntegrityError::UnsupportedTarget(_)) => {
            NcmWorkerPlatformCapability::Unsupported { target: triple }
        }
        Err(error) => NcmWorkerPlatformCapability::InvalidTrustRoot {
            target: triple,
            detail: error.to_string(),
        },
    }
}

#[cfg(test)]
mod tests {
    use super::{CurrentTarget, NcmWorkerPlatformCapability, capability_in};
    use crate::wire::{PROTOCOL_IDENTITY, PROTOCOL_VERSION};
    use serde_json::{Value, json};

    const RELEASE_TARGETS: [CurrentTarget; 6] = [
        platform("aarch64-apple-darwin", "macos", "aarch64", "unix"),
        platform("x86_64-apple-darwin", "macos", "x86_64", "unix"),
        platform("aarch64-unknown-linux-gnu", "linux", "aarch64", "unix"),
        platform("x86_64-unknown-linux-gnu", "linux", "x86_64", "unix"),
        platform("aarch64-pc-windows-msvc", "windows", "aarch64", "windows"),
        platform("x86_64-pc-windows-msvc", "windows", "x86_64", "windows"),
    ];

    const fn platform(
        triple: &'static str,
        os: &'static str,
        arch: &'static str,
        family: &'static str,
    ) -> CurrentTarget {
        CurrentTarget {
            triple,
            os,
            arch,
            family,
        }
    }

    fn manifest(targets: &[&CurrentTarget]) -> String {
        let targets: Vec<Value> = targets
            .iter()
            .enumerate()
            .map(|(index, target)| {
                json!({
                    "triple": target.triple,
                    "os": target.os,
                    "arch": target.arch,
                    "family": target.family,
                    "bytes": 1024 + index,
                    "sha256": format!("{index:064x}"),
                })
            })
            .collect();
        json!({
            "schema_version": 1,
            "worker": "tracedecay-ncm-worker",
            "protocol_version": PROTOCOL_VERSION,
            "protocol_identity": PROTOCOL_IDENTITY,
            "targets": targets,
        })
        .to_string()
    }

    #[test]
    fn empty_trust_root_reports_every_release_target_unsupported() {
        let empty = manifest(&[]);
        for target in &RELEASE_TARGETS {
            let capability = capability_in(&empty, target);
            assert_eq!(
                capability,
                NcmWorkerPlatformCapability::Unsupported {
                    target: target.triple.to_owned()
                }
            );
            assert!(!capability.is_supported(), "{capability}");
        }
    }

    #[test]
    fn multi_target_trust_root_supports_exactly_its_pinned_triples() {
        let pinned = [&RELEASE_TARGETS[2], &RELEASE_TARGETS[5]];
        let text = manifest(&pinned);
        for target in &RELEASE_TARGETS {
            let capability = capability_in(&text, target);
            assert_eq!(capability.target(), target.triple);
            assert_eq!(
                capability.is_supported(),
                pinned.iter().any(|pin| pin.triple == target.triple),
                "{capability}"
            );
        }
    }

    #[test]
    fn pinned_triple_with_mismatched_platform_metadata_is_not_supported() {
        let foreign = platform("x86_64-pc-windows-msvc", "linux", "x86_64", "unix");
        let text = manifest(&[&foreign]);
        assert!(matches!(
            capability_in(&text, &RELEASE_TARGETS[5]),
            NcmWorkerPlatformCapability::InvalidTrustRoot { ref target, .. }
                if target == "x86_64-pc-windows-msvc"
        ));
    }

    #[test]
    fn malformed_trust_root_is_typed_invalid_not_supported() {
        let duplicate = manifest(&[&RELEASE_TARGETS[3], &RELEASE_TARGETS[3]]);
        for text in [duplicate.as_str(), "{}", "not json"] {
            let capability = capability_in(text, &RELEASE_TARGETS[3]);
            assert!(
                matches!(
                    capability,
                    NcmWorkerPlatformCapability::InvalidTrustRoot { .. }
                ),
                "{capability}"
            );
            assert!(!capability.is_supported());
        }
    }
}
