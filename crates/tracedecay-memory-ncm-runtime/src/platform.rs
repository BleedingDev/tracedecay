//! Published platform capability for the pinned NCM worker distribution.
//!
//! The NCM algorithms are portable Rust, but the production worker is a
//! separately verified executable. Until another executable is pinned in
//! `product/ncm/reference/worker-manifest.json`, only the current arm64 macOS
//! artifact is an advertised worker capability. Other targets remain usable
//! for the host with Native memory and report typed NCM unavailability.

use std::fmt;

/// The only NCM worker target currently backed by a checked-in artifact pin.
pub const PINNED_WORKER_TARGET: &str = "aarch64-apple-darwin";

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
}

impl NcmWorkerPlatformCapability {
    /// Returns the resolved Rust target triple.
    #[must_use]
    pub fn target(&self) -> &str {
        match self {
            Self::Supported { target } | Self::Unsupported { target } => target,
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
        }
    }
}

/// Resolves the published worker capability for a Rust target triple.
#[must_use]
pub fn worker_platform_capability(target: &str) -> NcmWorkerPlatformCapability {
    if target == PINNED_WORKER_TARGET {
        NcmWorkerPlatformCapability::Supported {
            target: target.to_owned(),
        }
    } else {
        NcmWorkerPlatformCapability::Unsupported {
            target: target.to_owned(),
        }
    }
}

/// Resolves the published worker capability for the current compilation target.
///
/// The exact target triple is available to callers that inspect a release
/// matrix through [`worker_platform_capability`]. This convenience function
/// intentionally keeps non-arm64 macOS builds typed as unsupported without
/// copying the verifier's complete target-triple table.
#[must_use]
pub fn current_worker_platform_capability() -> NcmWorkerPlatformCapability {
    if cfg!(all(target_os = "macos", target_arch = "aarch64")) {
        worker_platform_capability(PINNED_WORKER_TARGET)
    } else {
        worker_platform_capability("unsupported")
    }
}

#[cfg(test)]
mod tests {
    use super::{NcmWorkerPlatformCapability, PINNED_WORKER_TARGET, worker_platform_capability};

    #[test]
    fn only_pinned_target_is_supported() {
        assert_eq!(
            worker_platform_capability(PINNED_WORKER_TARGET),
            NcmWorkerPlatformCapability::Supported {
                target: PINNED_WORKER_TARGET.to_owned()
            }
        );
        assert!(
            !worker_platform_capability(PINNED_WORKER_TARGET)
                .target()
                .is_empty()
        );
        assert!(worker_platform_capability(PINNED_WORKER_TARGET).is_supported());
    }

    #[test]
    fn recognized_build_targets_without_pins_are_typed_unsupported() {
        for target in [
            "x86_64-apple-darwin",
            "aarch64-unknown-linux-gnu",
            "x86_64-unknown-linux-gnu",
            "aarch64-unknown-linux-musl",
            "x86_64-unknown-linux-musl",
            "x86_64-pc-windows-msvc",
            "future-target",
        ] {
            let capability = worker_platform_capability(target);
            assert_eq!(capability.target(), target);
            assert!(!capability.is_supported(), "{capability}");
            assert!(matches!(
                capability,
                NcmWorkerPlatformCapability::Unsupported { .. }
            ));
        }
    }
}
