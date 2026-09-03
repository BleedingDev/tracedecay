//! Host-authority hydration of provider-claimed provenance.
//!
//! [`crate::recall_context_pack::ProviderItemProvenanceV1::Available`] is a
//! *provider's own claim* about a source: the adapter names it, but nothing
//! in admission, normalization, or selection independently confirms it. A
//! provider may explain its internal activation freely, but that explanation
//! is not host-resolved evidence, and rendering an unverified claim next to
//! required host evidence would let uncited synthesis masquerade as cited
//! grounding.
//!
//! This module is the one place a claimed source is turned into an explicit
//! [`ProvenanceHydrationOutcome`]:
//!
//! * a claim whose *shape* the host recognises and whose *referent* a host
//!   evidence store confirms inside the authoritative scope is
//!   [`ProvenanceHydrationOutcome::Hydrated`], and only this state may be
//!   rendered as cited grounding;
//! * every other claim -- an unrecognised shape, a path that leaves the
//!   authoritative worktree, a file or record that does not exist, a range
//!   past the end of real evidence, a session that is not this recall's --
//!   is [`ProvenanceHydrationOutcome::Unresolvable`], carrying the typed
//!   reason, and is never silently treated as available;
//! * host policy ([`ProvenanceHydrationPolicyV1`]) decides whether a
//!   candidate with no citable evidence -- unresolvable, or the provider
//!   named nothing at all -- is dropped before it reaches an agent. Its
//!   default is the contract default: exclude unavailable provenance,
//!   degrade-allow a declared redaction;
//! * a whole recall's resolution work is bounded and metered by
//!   [`ProvenanceHydrationPassV1`]. A claim the pass cannot attempt, or one
//!   whose resolution is cut short by the caller's deadline or cancellation,
//!   becomes an explicit `Unresolvable` **and** a recorded typed
//!   [`ProvenanceHydrationDegradationV1`]. No path in this module can hand a
//!   raw `Available` claim back to a caller.
//!
//! Shape parsing ([`HostEvidenceRefV1::parse`]) is deliberately *not* a
//! resolution authority: it decides only whether text names one of the
//! host's own reference forms. Confirmation is delegated to three narrow
//! host-owned ports -- [`HostSourceEvidenceStore`],
//! [`HostSessionEvidenceStore`], and [`HostCanonicalRecordStore`] -- that the
//! composition root implements against real host storage and injects through
//! [`MountedHostProvenanceAuthorityV1`]. This crate names no filesystem,
//! session store, or record store of its own, so it cannot fake a
//! confirmation it did not obtain.

use std::fmt;
use std::path::{Component, Path, PathBuf};
use std::sync::Arc;

use serde::{Deserialize, Serialize};
use tracedecay_application::{CancellationSignal, ResolvedScope};

use crate::recall_context_pack::ProviderItemProvenanceV1;

/// Host-resolved evidence a hydrated provenance claim points at.
///
/// Every variant is an *exact* pointer, never a fuzzy description: a source
/// range names inclusive start/end lines, a session record names an
/// inclusive ordinal range inside one session, and a canonical record names
/// one host-owned record identity.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum HostEvidenceRefV1 {
    /// An exact, inclusive line range inside one source path.
    SourceRange {
        /// Repository-relative path the range names.
        path: String,
        /// Inclusive first line, one-indexed.
        start_line: u32,
        /// Inclusive last line, one-indexed.
        end_line: u32,
    },
    /// An exact, inclusive ordinal range inside one session.
    SessionRecord {
        /// The session identity the range belongs to.
        session_id: String,
        /// Inclusive first message ordinal.
        start_ordinal: u64,
        /// Inclusive last message ordinal.
        end_ordinal: u64,
    },
    /// One canonical, host-owned record identity.
    CanonicalRecord {
        /// The record identity.
        record_id: String,
    },
}

impl HostEvidenceRefV1 {
    /// Stable single-line encoding used by labels and by the pack hash.
    #[must_use]
    pub fn label(&self) -> String {
        match self {
            Self::SourceRange {
                path,
                start_line,
                end_line,
            } => format!("source:{path}#L{start_line}-L{end_line}"),
            Self::SessionRecord {
                session_id,
                start_ordinal,
                end_ordinal,
            } => format!("session:{session_id}#{start_ordinal}-{end_ordinal}"),
            Self::CanonicalRecord { record_id } => format!("record:{record_id}"),
        }
    }

    /// Parses a claimed source reference into one of the host's own exact
    /// reference *shapes*.
    ///
    /// This is a syntax decision and nothing more. A parsed value is a
    /// well-formed *claim*, not confirmed evidence: whether the path, range,
    /// session, or record actually exists inside the authoritative scope is
    /// decided by [`HostProvenanceAuthority::resolve`] against real host
    /// stores. Never render a parse result as cited grounding.
    ///
    /// Recognised shapes are `source:<path>#L<start>-L<end>`,
    /// `session:<session_id>#<start>-<end>`, and `record:<record_id>`. Any
    /// other text -- including a shape-matching string with an inverted or
    /// malformed range, an empty component, or embedded control characters
    /// -- is refused rather than guessed at: a host authority that repaired a
    /// malformed claim would itself be fabricating the evidence this module
    /// exists to prevent.
    ///
    /// # Errors
    ///
    /// Returns [`ProvenanceHydrationError::Malformed`] when `raw` does not
    /// name one of the recognised shapes.
    pub fn parse(raw: &str) -> Result<Self, ProvenanceHydrationError> {
        let malformed = || ProvenanceHydrationError::Malformed {
            claimed_source: raw.to_owned(),
        };
        if raw.is_empty() || raw.chars().any(char::is_control) {
            return Err(malformed());
        }
        if let Some(rest) = raw.strip_prefix("source:") {
            let (path, range) = rest.split_once('#').ok_or_else(malformed)?;
            let range = range.strip_prefix('L').ok_or_else(malformed)?;
            let (start, end) = range.split_once("-L").ok_or_else(malformed)?;
            let start_line: u32 = start.parse().map_err(|_| malformed())?;
            let end_line: u32 = end.parse().map_err(|_| malformed())?;
            if path.trim().is_empty() || start_line == 0 || end_line < start_line {
                return Err(malformed());
            }
            return Ok(Self::SourceRange {
                path: path.to_owned(),
                start_line,
                end_line,
            });
        }
        if let Some(rest) = raw.strip_prefix("session:") {
            let (session_id, range) = rest.split_once('#').ok_or_else(malformed)?;
            let (start, end) = range.split_once('-').ok_or_else(malformed)?;
            let start_ordinal: u64 = start.parse().map_err(|_| malformed())?;
            let end_ordinal: u64 = end.parse().map_err(|_| malformed())?;
            if session_id.trim().is_empty() || end_ordinal < start_ordinal {
                return Err(malformed());
            }
            return Ok(Self::SessionRecord {
                session_id: session_id.to_owned(),
                start_ordinal,
                end_ordinal,
            });
        }
        if let Some(record_id) = raw.strip_prefix("record:") {
            if record_id.trim().is_empty() || record_id.trim() != record_id {
                return Err(malformed());
            }
            return Ok(Self::CanonicalRecord {
                record_id: record_id.to_owned(),
            });
        }
        Err(malformed())
    }
}

/// Typed refusal from one host evidence store.
///
/// A store never answers "maybe": either it confirmed the referent inside the
/// authoritative scope, or it says structurally why it did not.
#[derive(Clone, Debug, Eq, PartialEq, thiserror::Error)]
pub enum HostEvidenceLookupErrorV1 {
    /// The referent does not exist in host storage.
    #[error("the host has no such evidence")]
    NotFound,
    /// The referent exists but the claimed range runs past what the host
    /// actually holds.
    #[error("the claimed range runs past the {available} the host holds")]
    OutOfRange {
        /// The largest line or ordinal the host actually holds.
        available: u64,
    },
    /// The referent belongs to a different project, worktree, session, or
    /// profile than the recall's authoritative scope.
    #[error("the reference belongs to another scope ({field})")]
    ForeignScope {
        /// The identity field that disagreed.
        field: &'static str,
    },
    /// The referent exists but this scope may not cite it.
    #[error("the reference is not authorized for this scope")]
    Unauthorized,
    /// The referent existed but the host copy is superseded or revoked.
    #[error("the reference is stale")]
    Stale,
    /// The store itself could not answer. This is never treated as a
    /// confirmation.
    #[error("the host evidence store could not answer: {reason}")]
    Unavailable {
        /// Bounded, host-authored reason.
        reason: String,
    },
}

/// Typed, bounded failure of one hydration attempt.
///
/// Every variant is a refusal a caller can branch on structurally: nothing
/// here degrades into a string a caller would have to pattern-match.
#[derive(Clone, Debug, Eq, PartialEq, thiserror::Error)]
pub enum ProvenanceHydrationError {
    /// The claimed reference does not name one of the host's recognised
    /// evidence shapes.
    #[error("provenance source `{claimed_source}` does not name a host-recognised evidence shape")]
    Malformed {
        /// The raw claimed reference.
        claimed_source: String,
    },
    /// The reference names a recognised shape but the host authority could
    /// not confirm it names real evidence inside the authoritative scope.
    #[error("host authority could not confirm source `{claimed_source}`: {reason}")]
    Unresolvable {
        /// The raw claimed reference.
        claimed_source: String,
        /// Bounded, host-authored reason.
        reason: String,
    },
    /// This hydration pass had already spent its bounded attempt budget.
    #[error("provenance hydration budget of {budget} attempts exhausted before `{claimed_source}`")]
    BudgetExhausted {
        /// The configured attempt budget.
        budget: usize,
        /// The claimed reference that was never attempted.
        claimed_source: String,
    },
    /// The caller's deadline elapsed before the host could confirm.
    #[error("the recall deadline elapsed before `{claimed_source}` could be confirmed")]
    DeadlineExceeded {
        /// The claimed reference that was never confirmed.
        claimed_source: String,
    },
    /// The caller cancelled before the host could confirm.
    #[error("the recall was cancelled before `{claimed_source}` could be confirmed")]
    Cancelled {
        /// The claimed reference that was never confirmed.
        claimed_source: String,
    },
}

/// Refusal to construct a [`HostEvidenceScopeV1`].
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum HostEvidenceScopeError {
    /// An identity field was empty, padded, or carried control characters.
    #[error("host evidence scope identity `{field}` is not canonical")]
    InvalidIdentity {
        /// The offending field.
        field: &'static str,
    },
    /// The worktree root was not an absolute host path, so no containment
    /// decision about a claimed relative path could be made.
    #[error("host evidence scope worktree root must be an absolute path")]
    WorktreeRootNotAbsolute,
}

/// The exact, authoritative scope every hydration decision is made inside.
///
/// This is host-minted from the mount, never from a provider reply or a tool
/// argument: a claim can only ever be confirmed against *this* profile,
/// project, repository, worktree, reference, canonical session, and checkout
/// root. It is what makes `source:../other-worktree/file#L1-L2` and
/// `session:<someone-elses-session>#1-2` refusals rather than citations.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HostEvidenceScopeV1 {
    profile_id: String,
    scope: ResolvedScope,
    canonical_session_id: String,
    worktree_root: PathBuf,
}

impl HostEvidenceScopeV1 {
    /// Mints the authoritative hydration scope.
    ///
    /// # Errors
    ///
    /// Returns [`HostEvidenceScopeError`] when an identity field is not
    /// canonical or `worktree_root` is not absolute.
    pub fn new(
        profile_id: impl Into<String>,
        scope: ResolvedScope,
        canonical_session_id: impl Into<String>,
        worktree_root: impl Into<PathBuf>,
    ) -> Result<Self, HostEvidenceScopeError> {
        let profile_id = profile_id.into();
        let canonical_session_id = canonical_session_id.into();
        let worktree_root = worktree_root.into();
        let canonical = |value: &str| {
            !value.is_empty() && value.trim() == value && !value.chars().any(char::is_control)
        };
        if !canonical(&profile_id) {
            return Err(HostEvidenceScopeError::InvalidIdentity {
                field: "profile_id",
            });
        }
        if !canonical(&canonical_session_id) {
            return Err(HostEvidenceScopeError::InvalidIdentity {
                field: "canonical_session_id",
            });
        }
        if !worktree_root.is_absolute() {
            return Err(HostEvidenceScopeError::WorktreeRootNotAbsolute);
        }
        Ok(Self {
            profile_id,
            scope,
            canonical_session_id,
            worktree_root,
        })
    }

    /// The authoritative profile identity.
    #[must_use]
    pub fn profile_id(&self) -> &str {
        &self.profile_id
    }

    /// The authoritative resolved project/repository/worktree scope.
    #[must_use]
    pub const fn scope(&self) -> &ResolvedScope {
        &self.scope
    }

    /// The canonical host session this recall is bound to. A `session:`
    /// reference naming any other session is a foreign-scope refusal.
    #[must_use]
    pub fn canonical_session_id(&self) -> &str {
        &self.canonical_session_id
    }

    /// Absolute root of the mounted checkout every `source:` reference must
    /// resolve inside.
    #[must_use]
    pub fn worktree_root(&self) -> &Path {
        &self.worktree_root
    }
}

/// The caller's live operation control, propagated into every resolution.
///
/// Hydration is host work spent on provider input, so it is never allowed to
/// outlive the caller's deadline or survive their cancellation.
#[derive(Clone, Copy, Debug)]
pub struct HostEvidenceControlV1<'control> {
    now_micros: i64,
    deadline_micros: i64,
    cancellation: &'control CancellationSignal,
}

impl<'control> HostEvidenceControlV1<'control> {
    /// Binds one hydration pass to the caller's clock reading, deadline, and
    /// live cancellation identity.
    #[must_use]
    pub const fn new(
        now_micros: i64,
        deadline_micros: i64,
        cancellation: &'control CancellationSignal,
    ) -> Self {
        Self {
            now_micros,
            deadline_micros,
            cancellation,
        }
    }

    /// Refuses the attempt when the caller is already gone.
    ///
    /// # Errors
    ///
    /// Returns [`ProvenanceHydrationError::Cancelled`] or
    /// [`ProvenanceHydrationError::DeadlineExceeded`].
    pub fn check(&self, claimed_source: &str) -> Result<(), ProvenanceHydrationError> {
        if self.cancellation.is_cancelled() {
            return Err(ProvenanceHydrationError::Cancelled {
                claimed_source: claimed_source.to_owned(),
            });
        }
        if self.deadline_micros <= self.now_micros {
            return Err(ProvenanceHydrationError::DeadlineExceeded {
                claimed_source: claimed_source.to_owned(),
            });
        }
        Ok(())
    }
}

/// Host store that confirms a `source:` line range against the real checkout.
///
/// Implemented by the composition root against the mounted worktree; this
/// crate never reaches a filesystem itself.
pub trait HostSourceEvidenceStore: Send + Sync {
    /// Number of lines the host actually holds for `relative_path` inside
    /// `scope`'s checkout.
    ///
    /// `relative_path` has already been proven to be a scope-relative path
    /// with no traversal, root, or prefix components.
    ///
    /// # Errors
    ///
    /// Returns a typed [`HostEvidenceLookupErrorV1`] when the path does not
    /// exist, leaves the scope, or cannot be read.
    fn source_line_count(
        &self,
        scope: &HostEvidenceScopeV1,
        relative_path: &Path,
    ) -> Result<u64, HostEvidenceLookupErrorV1>;
}

/// Host store that confirms a `session:` ordinal range against real session
/// records.
pub trait HostSessionEvidenceStore: Send + Sync {
    /// Highest message ordinal the host holds for `session_id` inside
    /// `scope`.
    ///
    /// # Errors
    ///
    /// Returns a typed [`HostEvidenceLookupErrorV1`] when the session is
    /// unknown, belongs to another scope, or cannot be read.
    fn session_ordinal_ceiling(
        &self,
        scope: &HostEvidenceScopeV1,
        session_id: &str,
    ) -> Result<u64, HostEvidenceLookupErrorV1>;
}

/// Host store that confirms a `record:` canonical identity exists and is
/// owned by the authoritative scope.
pub trait HostCanonicalRecordStore: Send + Sync {
    /// Confirms one canonical record identity.
    ///
    /// # Errors
    ///
    /// Returns a typed [`HostEvidenceLookupErrorV1`] when the record does not
    /// exist, is owned by another scope, is stale, or cannot be read.
    fn confirm_canonical_record(
        &self,
        scope: &HostEvidenceScopeV1,
        record_id: &str,
    ) -> Result<(), HostEvidenceLookupErrorV1>;
}

/// Host store that decides whether a claim the host does *not* recognise as
/// one of its own evidence shapes is nevertheless a provider-local reference
/// the host itself hosts.
///
/// This exists because some provider state is legitimately not host evidence.
/// A supervised provider's own staged rows live in a store the host granted
/// and placed, addressed by a reference the host's own product code mints;
/// there is no source range, session ordinal, or canonical record to cite for
/// them, and inventing one would be exactly the fabrication
/// [`HostProvenanceAuthority`] exists to prevent. Recognising such a claim is
/// therefore *not* confirmation: an accepted claim stays
/// [`ProviderItemProvenanceV1::Available`] — provider-attested, never cited
/// grounding, never the host-confirmed trust tier — and its text still passes
/// every containment gate a provider claim passes. What acceptance buys is
/// only that the claim is not discarded as malformed.
pub trait HostProviderLocalAttestationStore: Send + Sync {
    /// Decides one provider-local reference inside the authoritative scope.
    ///
    /// # Errors
    ///
    /// Returns a typed [`HostEvidenceLookupErrorV1`] when the reference is
    /// not one this host mints, belongs to another scope, or cannot be
    /// decided.
    fn attest_provider_local(
        &self,
        scope: &HostEvidenceScopeV1,
        claimed_source: &str,
    ) -> Result<(), HostEvidenceLookupErrorV1>;
}

/// Host authority that resolves a provider-claimed source reference into
/// exact, confirmed host evidence.
///
/// A provider may explain its own internal activation, but only this
/// authority's confirmation may become cited grounding. Implementations must
/// be synchronous and bounded: hydration runs inline while a pack is being
/// compiled, and a blocking or unbounded resolver would make an advisory
/// lane the reason a host answer is late.
pub trait HostProvenanceAuthority: Send + Sync {
    /// Resolves one claimed source reference inside the authoritative scope,
    /// under the caller's own deadline and cancellation identity.
    ///
    /// # Errors
    ///
    /// Returns [`ProvenanceHydrationError::Malformed`] when the reference
    /// names no recognised shape, [`ProvenanceHydrationError::Unresolvable`]
    /// when a host store refused to confirm it, or the deadline/cancellation
    /// variants when the caller is already gone.
    fn resolve(
        &self,
        source: &str,
        scope: &HostEvidenceScopeV1,
        control: &HostEvidenceControlV1<'_>,
    ) -> Result<HostEvidenceRefV1, ProvenanceHydrationError>;

    /// Decides a claim [`Self::resolve`] refused as malformed, when the host
    /// nevertheless recognises it as provider-local state it hosts itself.
    ///
    /// The default refuses everything, so an authority that mounts no
    /// attestation store behaves exactly as before: a claim that is not a
    /// host evidence shape stays unresolvable and is excluded under the
    /// default policy. Accepting a claim here never upgrades it — the
    /// candidate remains provider-attested.
    ///
    /// # Errors
    ///
    /// Returns [`ProvenanceHydrationError::Malformed`] when the claim is not
    /// provider-local state this host recognises, or
    /// [`ProvenanceHydrationError::Unresolvable`] when a store refused it.
    fn attest_provider_local(
        &self,
        source: &str,
        _scope: &HostEvidenceScopeV1,
        _control: &HostEvidenceControlV1<'_>,
    ) -> Result<(), ProvenanceHydrationError> {
        Err(ProvenanceHydrationError::Malformed {
            claimed_source: source.to_owned(),
        })
    }
}

/// Rejects any claimed source path that is not a plain relative path inside
/// the authoritative worktree.
///
/// Absolute paths, `..` traversal, `.` segments, platform prefixes, and
/// backslash separators are refused *before* a store is asked, so no store
/// implementation can be tricked into reading outside the mounted checkout.
fn scope_relative_path(raw: &str) -> Result<PathBuf, &'static str> {
    if raw.contains('\\') {
        return Err("uses a non-canonical path separator");
    }
    let path = Path::new(raw);
    if path.is_absolute() {
        return Err("names an absolute path outside the authoritative worktree");
    }
    let mut relative = PathBuf::new();
    for component in path.components() {
        match component {
            Component::Normal(part) => relative.push(part),
            Component::ParentDir => {
                return Err("traverses outside the authoritative worktree");
            }
            Component::CurDir => return Err("is not a canonical relative path"),
            Component::RootDir | Component::Prefix(_) => {
                return Err("names an absolute path outside the authoritative worktree");
            }
        }
    }
    if relative.as_os_str().is_empty() {
        return Err("names no path");
    }
    Ok(relative)
}

/// The mounted host authority: shape, scope containment, and a real host
/// store lookup, in that order.
///
/// Nothing here can confirm a claim on shape alone. Each recognised shape is
/// handed to the matching host-owned store, and a store refusal becomes a
/// typed [`ProvenanceHydrationError::Unresolvable`] carrying the store's own
/// reason.
pub struct MountedHostProvenanceAuthorityV1 {
    source: Arc<dyn HostSourceEvidenceStore>,
    session: Arc<dyn HostSessionEvidenceStore>,
    record: Arc<dyn HostCanonicalRecordStore>,
    provider_local: Option<Arc<dyn HostProviderLocalAttestationStore>>,
}

impl fmt::Debug for MountedHostProvenanceAuthorityV1 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("MountedHostProvenanceAuthorityV1")
            .finish_non_exhaustive()
    }
}

impl MountedHostProvenanceAuthorityV1 {
    /// Mounts the three host evidence stores that back resolution.
    #[must_use]
    pub fn new(
        source: Arc<dyn HostSourceEvidenceStore>,
        session: Arc<dyn HostSessionEvidenceStore>,
        record: Arc<dyn HostCanonicalRecordStore>,
    ) -> Self {
        Self {
            source,
            session,
            record,
            provider_local: None,
        }
    }

    /// Mounts an additional store that decides provider-local references the
    /// three evidence stores cannot resolve.
    ///
    /// A host that mounts one is saying: these references are mine to
    /// recognise. It is never saying they are cited evidence — an attested
    /// claim stays provider-attested.
    #[must_use]
    pub fn with_provider_local_attestation(
        mut self,
        provider_local: Arc<dyn HostProviderLocalAttestationStore>,
    ) -> Self {
        self.provider_local = Some(provider_local);
        self
    }
}

fn unresolvable(claimed_source: &str, reason: impl Into<String>) -> ProvenanceHydrationError {
    ProvenanceHydrationError::Unresolvable {
        claimed_source: claimed_source.to_owned(),
        reason: reason.into(),
    }
}

impl HostProvenanceAuthority for MountedHostProvenanceAuthorityV1 {
    fn resolve(
        &self,
        source: &str,
        scope: &HostEvidenceScopeV1,
        control: &HostEvidenceControlV1<'_>,
    ) -> Result<HostEvidenceRefV1, ProvenanceHydrationError> {
        control.check(source)?;
        let evidence = HostEvidenceRefV1::parse(source)?;
        match &evidence {
            HostEvidenceRefV1::SourceRange {
                path,
                start_line,
                end_line,
            } => {
                let relative = scope_relative_path(path)
                    .map_err(|reason| unresolvable(source, format!("claimed path {reason}")))?;
                let lines = self
                    .source
                    .source_line_count(scope, &relative)
                    .map_err(|error| unresolvable(source, error.to_string()))?;
                if u64::from(*end_line) > lines {
                    return Err(unresolvable(
                        source,
                        format!(
                            "claims lines {start_line}-{end_line} but the host file holds {lines}"
                        ),
                    ));
                }
            }
            HostEvidenceRefV1::SessionRecord {
                session_id,
                start_ordinal,
                end_ordinal,
            } => {
                if session_id != scope.canonical_session_id() {
                    return Err(unresolvable(
                        source,
                        "names a session outside this recall's bound canonical session",
                    ));
                }
                let ceiling = self
                    .session
                    .session_ordinal_ceiling(scope, session_id)
                    .map_err(|error| unresolvable(source, error.to_string()))?;
                if *end_ordinal > ceiling {
                    return Err(unresolvable(
                        source,
                        format!(
                            "claims ordinals {start_ordinal}-{end_ordinal} but the host session \
                             holds {ceiling}"
                        ),
                    ));
                }
            }
            HostEvidenceRefV1::CanonicalRecord { record_id } => {
                self.record
                    .confirm_canonical_record(scope, record_id)
                    .map_err(|error| unresolvable(source, error.to_string()))?;
            }
        }
        Ok(evidence)
    }

    fn attest_provider_local(
        &self,
        source: &str,
        scope: &HostEvidenceScopeV1,
        control: &HostEvidenceControlV1<'_>,
    ) -> Result<(), ProvenanceHydrationError> {
        control.check(source)?;
        let Some(store) = self.provider_local.as_ref() else {
            return Err(ProvenanceHydrationError::Malformed {
                claimed_source: source.to_owned(),
            });
        };
        store
            .attest_provider_local(scope, source)
            .map_err(|error| unresolvable(source, error.to_string()))
    }
}

/// Host policy over one hydration pass: whether candidates with no citable
/// evidence are excluded, and how many resolution attempts the pass may
/// spend.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ProvenanceHydrationPolicyV1 {
    /// Candidates whose provenance is unresolvable, or whose provider named
    /// no source at all, are dropped before compilation rather than rendered
    /// with a visible "unresolved" label.
    exclude_provenance_unavailable: bool,
    /// Maximum resolution attempts this pass may spend. Bounded so a
    /// provider that returns many claims cannot turn hydration into
    /// unbounded host-side work.
    max_hydrations: usize,
}

/// Refusal to construct a [`ProvenanceHydrationPolicyV1`].
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum ProvenanceHydrationPolicyError {
    /// A policy that can attempt zero hydrations can never confirm any
    /// claim, which is not a bound, it is a policy that always excludes.
    #[error("provenance hydration policy requires max_hydrations > 0")]
    ZeroBudget,
}

impl ProvenanceHydrationPolicyV1 {
    /// Constructs a policy.
    ///
    /// # Errors
    ///
    /// Returns [`ProvenanceHydrationPolicyError::ZeroBudget`] when
    /// `max_hydrations` is zero.
    pub const fn new(
        exclude_provenance_unavailable: bool,
        max_hydrations: usize,
    ) -> Result<Self, ProvenanceHydrationPolicyError> {
        if max_hydrations == 0 {
            return Err(ProvenanceHydrationPolicyError::ZeroBudget);
        }
        Ok(Self {
            exclude_provenance_unavailable,
            max_hydrations,
        })
    }

    /// Whether a candidate with no citable evidence is excluded.
    #[must_use]
    pub const fn excludes_provenance_unavailable(&self) -> bool {
        self.exclude_provenance_unavailable
    }

    /// The bounded number of resolution attempts one pass may spend.
    #[must_use]
    pub const fn max_hydrations(&self) -> usize {
        self.max_hydrations
    }
}

/// The host's mounted attempt bound for one recall pass.
///
/// It is the same number as the host's own admitted-candidate ceiling
/// (`PROJECT_RECALL_BUDGETS.maximum_candidates`), so the bound is reachable
/// on the production path rather than a decorative constant far above
/// anything a real recall can produce.
pub const DEFAULT_PROVENANCE_HYDRATION_MAX_ATTEMPTS: usize = 8;

impl Default for ProvenanceHydrationPolicyV1 {
    fn default() -> Self {
        // The contract default: exclude unavailable provenance. Constructed
        // directly rather than through `new` because the budget is a non-zero
        // compile-time constant, so there is no fallible path to thread
        // through `expect`/`unwrap` (both denied in this crate).
        Self {
            exclude_provenance_unavailable: true,
            max_hydrations: DEFAULT_PROVENANCE_HYDRATION_MAX_ATTEMPTS,
        }
    }
}

/// What one hydration attempt decided about a candidate's provenance.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ProvenanceHydrationOutcome {
    /// The provider named no source; there was nothing to hydrate.
    NotClaimed,
    /// The claimed source resolved to confirmed host evidence. Only this
    /// outcome may become cited grounding.
    Hydrated(HostEvidenceRefV1),
    /// The claim could not be confirmed. Carries the typed reason so a
    /// caller can label it visibly rather than guessing why.
    Unresolvable {
        /// The raw claimed reference.
        source: String,
        /// Bounded reason the claim did not resolve.
        reason: String,
    },
}

impl ProvenanceHydrationOutcome {
    /// Whether host policy would exclude a candidate carrying this outcome.
    #[must_use]
    pub const fn excluded_by(&self, policy: &ProvenanceHydrationPolicyV1) -> bool {
        match self {
            Self::Hydrated(_) => false,
            Self::NotClaimed | Self::Unresolvable { .. } => policy.exclude_provenance_unavailable,
        }
    }
}

impl fmt::Display for ProvenanceHydrationOutcome {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NotClaimed => write!(formatter, "provenance not claimed"),
            Self::Hydrated(evidence) => write!(formatter, "hydrated:{}", evidence.label()),
            Self::Unresolvable { source, reason } => {
                write!(formatter, "unresolvable:{source}:{reason}")
            }
        }
    }
}

/// Why a hydration pass stopped being able to confirm claims.
///
/// A degradation is a *lane* fact, not a candidate fact: it says the host ran
/// out of budget or the caller went away, so the remaining candidates were
/// labelled unresolved without ever being attempted.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ProvenanceHydrationDegradationV1 {
    /// The pass spent its whole attempt budget.
    BudgetExhausted {
        /// The configured attempt budget.
        budget: usize,
        /// Claims the pass never attempted after the bound was reached.
        unattempted: usize,
    },
    /// The caller's deadline elapsed mid-pass.
    DeadlineExceeded {
        /// Claims left unconfirmed once the deadline elapsed.
        unattempted: usize,
    },
    /// The caller cancelled mid-pass.
    Cancelled {
        /// Claims left unconfirmed once cancellation was observed.
        unattempted: usize,
    },
}

impl ProvenanceHydrationDegradationV1 {
    /// Stable single-line label for logs and receipts.
    #[must_use]
    pub fn label(&self) -> String {
        match self {
            Self::BudgetExhausted {
                budget,
                unattempted,
            } => format!("hydration_budget_exhausted:{budget}:{unattempted}"),
            Self::DeadlineExceeded { unattempted } => {
                format!("hydration_deadline_exceeded:{unattempted}")
            }
            Self::Cancelled { unattempted } => format!("hydration_cancelled:{unattempted}"),
        }
    }
}

/// One candidate's hydrated provenance and the host's exclusion decision.
///
/// `provenance` is never [`ProviderItemProvenanceV1::Available`]: an
/// unconfirmed claim leaves this module as
/// [`ProviderItemProvenanceV1::Unresolvable`] with a reason, so no caller can
/// render a bare provider claim as `source ...`.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProvenanceHydrationDecisionV1 {
    /// The host-decided provenance state to carry forward.
    pub provenance: ProviderItemProvenanceV1,
    /// Whether host policy drops this candidate before it reaches an agent.
    pub excluded: bool,
}

/// One bounded, metered hydration pass over the candidates of a single
/// recall.
///
/// The pass owns the attempt counter, so the bound applies to the whole
/// recall rather than to each candidate independently, and it owns the typed
/// degradation, so an exhausted budget or a departed caller is a recorded
/// lane outcome instead of a swallowed error.
#[derive(Clone, Debug)]
pub struct ProvenanceHydrationPassV1 {
    policy: ProvenanceHydrationPolicyV1,
    attempted: usize,
    unattempted: usize,
    degradation: Option<ProvenanceHydrationDegradationV1>,
}

impl ProvenanceHydrationPassV1 {
    /// Begins a pass under `policy`.
    #[must_use]
    pub const fn new(policy: ProvenanceHydrationPolicyV1) -> Self {
        Self {
            policy,
            attempted: 0,
            unattempted: 0,
            degradation: None,
        }
    }

    /// The host policy this pass runs under.
    #[must_use]
    pub const fn policy(&self) -> &ProvenanceHydrationPolicyV1 {
        &self.policy
    }

    /// Resolution attempts spent so far.
    #[must_use]
    pub const fn attempts_spent(&self) -> usize {
        self.attempted
    }

    /// The pass's typed degradation, if it ever stopped being able to
    /// confirm claims.
    #[must_use]
    pub const fn degradation(&self) -> Option<&ProvenanceHydrationDegradationV1> {
        self.degradation.as_ref()
    }

    fn note_budget_exhausted(&mut self) {
        self.unattempted = self.unattempted.saturating_add(1);
        self.degradation = Some(ProvenanceHydrationDegradationV1::BudgetExhausted {
            budget: self.policy.max_hydrations(),
            unattempted: self.unattempted,
        });
    }

    fn note_deadline(&mut self) {
        self.unattempted = self.unattempted.saturating_add(1);
        self.degradation = Some(ProvenanceHydrationDegradationV1::DeadlineExceeded {
            unattempted: self.unattempted,
        });
    }

    fn note_cancelled(&mut self) {
        self.unattempted = self.unattempted.saturating_add(1);
        self.degradation = Some(ProvenanceHydrationDegradationV1::Cancelled {
            unattempted: self.unattempted,
        });
    }

    fn unresolved(&self, source: &str, reason: impl Into<String>) -> ProvenanceHydrationDecisionV1 {
        ProvenanceHydrationDecisionV1 {
            provenance: ProviderItemProvenanceV1::Unresolvable {
                source: source.to_owned(),
                reason: reason.into(),
            },
            excluded: self.policy.excludes_provenance_unavailable(),
        }
    }

    /// Hydrates one candidate's provenance, applying host policy.
    ///
    /// Only [`ProviderItemProvenanceV1::Available`] carries a claim to
    /// resolve and spends attempt budget. Every other state passes through
    /// unchanged; a declared redaction is degrade-allow per the recall
    /// contract, while an absent or already-unresolved provenance is subject
    /// to the same exclusion decision as a claim that did not resolve.
    ///
    /// This call is infallible on purpose: there is no `Err` a caller could
    /// discard back into a raw `Available` claim. Budget exhaustion, a
    /// missed deadline, and cancellation all become an explicit
    /// `Unresolvable` decision plus a recorded [`Self::degradation`].
    pub fn hydrate(
        &mut self,
        authority: &dyn HostProvenanceAuthority,
        scope: &HostEvidenceScopeV1,
        control: &HostEvidenceControlV1<'_>,
        provenance: &ProviderItemProvenanceV1,
    ) -> ProvenanceHydrationDecisionV1 {
        let source = match provenance {
            ProviderItemProvenanceV1::Available { source } => source,
            ProviderItemProvenanceV1::Unknown | ProviderItemProvenanceV1::Unresolvable { .. } => {
                return ProvenanceHydrationDecisionV1 {
                    provenance: provenance.clone(),
                    excluded: self.policy.excludes_provenance_unavailable(),
                };
            }
            ProviderItemProvenanceV1::Hydrated { .. }
            | ProviderItemProvenanceV1::Redacted { .. } => {
                return ProvenanceHydrationDecisionV1 {
                    provenance: provenance.clone(),
                    excluded: false,
                };
            }
        };
        if self.attempted >= self.policy.max_hydrations() {
            self.note_budget_exhausted();
            let budget = self.policy.max_hydrations();
            return self.unresolved(
                source,
                format!("the host hydration budget of {budget} attempts was already spent"),
            );
        }
        self.attempted = self.attempted.saturating_add(1);
        match authority.resolve(source, scope, control) {
            Ok(evidence) => ProvenanceHydrationDecisionV1 {
                provenance: ProviderItemProvenanceV1::Hydrated { evidence },
                excluded: false,
            },
            // A claim the host cannot resolve is not automatically a claim the
            // host does not recognise. Provider-local state the host itself
            // hosts gets one decision here, from a store the host mounted; an
            // accepted claim survives as `Available` — provider-attested, not
            // cited grounding, and not the host-confirmed trust tier — while
            // everything else stays unresolvable exactly as before.
            Err(ProvenanceHydrationError::Malformed { claimed_source }) => {
                match authority.attest_provider_local(&claimed_source, scope, control) {
                    Ok(()) => ProvenanceHydrationDecisionV1 {
                        provenance: ProviderItemProvenanceV1::Available {
                            source: claimed_source,
                        },
                        excluded: false,
                    },
                    Err(ProvenanceHydrationError::Unresolvable { reason, .. }) => {
                        self.unresolved(&claimed_source, reason)
                    }
                    Err(_) => self.unresolved(
                        &claimed_source,
                        "does not name a host-recognised evidence shape",
                    ),
                }
            }
            Err(ProvenanceHydrationError::Unresolvable {
                claimed_source,
                reason,
            }) => self.unresolved(&claimed_source, reason),
            Err(ProvenanceHydrationError::BudgetExhausted { claimed_source, .. }) => {
                self.note_budget_exhausted();
                let budget = self.policy.max_hydrations();
                self.unresolved(
                    &claimed_source,
                    format!("the host hydration budget of {budget} attempts was already spent"),
                )
            }
            Err(ProvenanceHydrationError::DeadlineExceeded { claimed_source }) => {
                self.note_deadline();
                self.unresolved(
                    &claimed_source,
                    "the recall deadline elapsed before the host could confirm this claim",
                )
            }
            Err(ProvenanceHydrationError::Cancelled { claimed_source }) => {
                self.note_cancelled();
                self.unresolved(
                    &claimed_source,
                    "the recall was cancelled before the host could confirm this claim",
                )
            }
        }
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::expect_used, clippy::panic, clippy::unwrap_used)]

    use std::collections::BTreeMap;

    use tracedecay_domain::{ProjectId, RefId, RepositoryId, WorktreeId};

    use super::*;

    const SESSION: &str = "session.hydration";

    /// A host source store holding an exact line count per relative path.
    /// It confirms nothing it was not told about, exactly like a real
    /// checkout-backed store refusing a path that is not on disk.
    struct MapSourceStore(BTreeMap<String, u64>);

    impl HostSourceEvidenceStore for MapSourceStore {
        fn source_line_count(
            &self,
            _scope: &HostEvidenceScopeV1,
            relative_path: &Path,
        ) -> Result<u64, HostEvidenceLookupErrorV1> {
            let key = relative_path.to_string_lossy().into_owned();
            self.0
                .get(&key)
                .copied()
                .ok_or(HostEvidenceLookupErrorV1::NotFound)
        }
    }

    struct MapSessionStore(BTreeMap<String, u64>);

    impl HostSessionEvidenceStore for MapSessionStore {
        fn session_ordinal_ceiling(
            &self,
            _scope: &HostEvidenceScopeV1,
            session_id: &str,
        ) -> Result<u64, HostEvidenceLookupErrorV1> {
            self.0
                .get(session_id)
                .copied()
                .ok_or(HostEvidenceLookupErrorV1::NotFound)
        }
    }

    struct MapRecordStore {
        owned: BTreeMap<String, String>,
    }

    impl HostCanonicalRecordStore for MapRecordStore {
        fn confirm_canonical_record(
            &self,
            scope: &HostEvidenceScopeV1,
            record_id: &str,
        ) -> Result<(), HostEvidenceLookupErrorV1> {
            match self.owned.get(record_id) {
                None => Err(HostEvidenceLookupErrorV1::NotFound),
                Some(project) if project == scope.scope().project_id.as_str() => Ok(()),
                Some(_) => Err(HostEvidenceLookupErrorV1::ForeignScope {
                    field: "project_id",
                }),
            }
        }
    }

    fn resolved_scope(project: &str) -> ResolvedScope {
        ResolvedScope::new(
            ProjectId::new(project).expect("project id"),
            RepositoryId::new("repository.hydration").expect("repository id"),
            WorktreeId::new("worktree.hydration").expect("worktree id"),
            Some(RefId::new("refs/heads/hydration").expect("reference")),
        )
        .expect("resolved scope")
    }

    fn scope() -> HostEvidenceScopeV1 {
        HostEvidenceScopeV1::new(
            "profile.hydration",
            resolved_scope("project.hydration"),
            SESSION,
            "/mounted/worktree",
        )
        .expect("host evidence scope")
    }

    /// A host authority whose stores hold exactly one real file, one real
    /// session, and one canonical record owned by this project.
    fn mounted_authority() -> MountedHostProvenanceAuthorityV1 {
        MountedHostProvenanceAuthorityV1::new(
            Arc::new(MapSourceStore(BTreeMap::from([(
                "crates/foo/src/lib.rs".to_owned(),
                40_u64,
            )]))),
            Arc::new(MapSessionStore(BTreeMap::from([(
                SESSION.to_owned(),
                12_u64,
            )]))),
            Arc::new(MapRecordStore {
                owned: BTreeMap::from([
                    ("native.fact.1".to_owned(), "project.hydration".to_owned()),
                    ("native.fact.other".to_owned(), "project.other".to_owned()),
                ]),
            }),
        )
    }

    fn live() -> CancellationSignal {
        CancellationSignal::active("token.hydration").expect("live signal")
    }

    fn resolve(source: &str) -> Result<HostEvidenceRefV1, ProvenanceHydrationError> {
        let signal = live();
        let control = HostEvidenceControlV1::new(0, 1_000, &signal);
        mounted_authority().resolve(source, &scope(), &control)
    }

    #[test]
    fn parses_exact_source_range() {
        let evidence = HostEvidenceRefV1::parse("source:crates/foo/src/lib.rs#L10-L20").unwrap();
        assert_eq!(
            evidence,
            HostEvidenceRefV1::SourceRange {
                path: "crates/foo/src/lib.rs".to_owned(),
                start_line: 10,
                end_line: 20,
            }
        );
    }

    #[test]
    fn refuses_inverted_range_rather_than_repairing_it() {
        let error = HostEvidenceRefV1::parse("source:crates/foo/src/lib.rs#L20-L10").unwrap_err();
        assert!(matches!(error, ProvenanceHydrationError::Malformed { .. }));
    }

    #[test]
    fn refuses_opaque_provider_label() {
        let error = HostEvidenceRefV1::parse("provider-internal-token-9f2a").unwrap_err();
        assert!(matches!(error, ProvenanceHydrationError::Malformed { .. }));
    }

    /// Real defect this catches: an authority that confirms a claim on shape
    /// alone, so a well-formed reference to a file the host does not have is
    /// rendered as cited grounding.
    #[test]
    fn a_real_source_range_hydrates_and_a_nonexistent_one_does_not() {
        assert_eq!(
            resolve("source:crates/foo/src/lib.rs#L10-L20").unwrap(),
            HostEvidenceRefV1::SourceRange {
                path: "crates/foo/src/lib.rs".to_owned(),
                start_line: 10,
                end_line: 20,
            }
        );
        let error = resolve("source:crates/foo/src/absent.rs#L1-L2").unwrap_err();
        assert!(
            matches!(error, ProvenanceHydrationError::Unresolvable { .. }),
            "a shape-valid claim about a file the host does not hold must be refused: {error:?}"
        );
    }

    /// Real defect this catches: a range that runs past the end of the real
    /// evidence being accepted, so a citation points at lines that do not
    /// exist.
    #[test]
    fn a_source_range_past_the_end_of_the_real_file_is_refused() {
        let error = resolve("source:crates/foo/src/lib.rs#L39-L41").unwrap_err();
        match error {
            ProvenanceHydrationError::Unresolvable { reason, .. } => {
                assert!(reason.contains("holds 40"), "{reason}");
            }
            other => panic!("out-of-range claim must be unresolvable: {other:?}"),
        }
    }

    /// Real defect this catches: `source:../other-worktree/file` or an
    /// absolute path escaping the authoritative checkout and still being
    /// presented as this project's evidence.
    #[test]
    fn a_traversing_or_absolute_source_path_never_reaches_the_store() {
        for claim in [
            "source:../other-worktree/file.rs#L1-L2",
            "source:/etc/passwd#L1-L2",
            "source:./lib.rs#L1-L2",
            "source:crates\\foo\\src\\lib.rs#L1-L2",
        ] {
            let error = resolve(claim).unwrap_err();
            assert!(
                matches!(error, ProvenanceHydrationError::Unresolvable { .. }),
                "{claim} must be refused before any store lookup: {error:?}"
            );
        }
    }

    /// Real defect this catches: a provider citing someone else's session
    /// transcript, or a session the host has no record of.
    #[test]
    fn only_this_recalls_own_session_can_be_cited_and_only_within_its_ordinals() {
        assert_eq!(
            resolve(&format!("session:{SESSION}#4-9")).unwrap(),
            HostEvidenceRefV1::SessionRecord {
                session_id: SESSION.to_owned(),
                start_ordinal: 4,
                end_ordinal: 9,
            }
        );
        let foreign = resolve("session:session.someone-else#1-2").unwrap_err();
        assert!(matches!(
            foreign,
            ProvenanceHydrationError::Unresolvable { .. }
        ));
        let past_end = resolve(&format!("session:{SESSION}#11-13")).unwrap_err();
        assert!(matches!(
            past_end,
            ProvenanceHydrationError::Unresolvable { .. }
        ));
    }

    /// Real defect this catches: `record:anything` hydrating without the
    /// host ever confirming the canonical record exists and belongs to this
    /// project.
    #[test]
    fn a_canonical_record_must_exist_and_belong_to_this_project() {
        assert_eq!(
            resolve("record:native.fact.1").unwrap(),
            HostEvidenceRefV1::CanonicalRecord {
                record_id: "native.fact.1".to_owned(),
            }
        );
        let absent = resolve("record:anything").unwrap_err();
        assert!(matches!(
            absent,
            ProvenanceHydrationError::Unresolvable { .. }
        ));
        let foreign = resolve("record:native.fact.other").unwrap_err();
        match foreign {
            ProvenanceHydrationError::Unresolvable { reason, .. } => {
                assert!(reason.contains("another scope"), "{reason}");
            }
            other => panic!("a foreign-project record must be refused: {other:?}"),
        }
    }

    /// Real defect this catches: hydration continuing to spend host work
    /// after the caller's deadline elapsed or the caller cancelled.
    #[test]
    fn resolution_refuses_once_the_caller_is_gone() {
        let signal = live();
        let elapsed = HostEvidenceControlV1::new(1_000, 1_000, &signal);
        let error = mounted_authority()
            .resolve("record:native.fact.1", &scope(), &elapsed)
            .unwrap_err();
        assert!(matches!(
            error,
            ProvenanceHydrationError::DeadlineExceeded { .. }
        ));

        let cancelled = live();
        assert!(cancelled.cancel(tracedecay_domain::UtcMicros(1)));
        let control = HostEvidenceControlV1::new(0, 1_000, &cancelled);
        let error = mounted_authority()
            .resolve("record:native.fact.1", &scope(), &control)
            .unwrap_err();
        assert!(matches!(error, ProvenanceHydrationError::Cancelled { .. }));
    }

    #[test]
    fn zero_budget_policy_is_refused_at_construction() {
        let error = ProvenanceHydrationPolicyV1::new(true, 0).unwrap_err();
        assert_eq!(error, ProvenanceHydrationPolicyError::ZeroBudget);
    }

    #[test]
    fn the_host_default_policy_excludes_unavailable_provenance() {
        assert!(
            ProvenanceHydrationPolicyV1::default().excludes_provenance_unavailable(),
            "the recall contract default is exclude for unavailable provenance"
        );
    }

    fn pass(exclude: bool, budget: usize) -> ProvenanceHydrationPassV1 {
        ProvenanceHydrationPassV1::new(
            ProvenanceHydrationPolicyV1::new(exclude, budget).expect("policy"),
        )
    }

    #[test]
    fn a_confirmed_claim_becomes_hydrated_and_is_never_excluded() {
        let signal = live();
        let control = HostEvidenceControlV1::new(0, 1_000, &signal);
        let decision = pass(true, 4).hydrate(
            &mounted_authority(),
            &scope(),
            &control,
            &ProviderItemProvenanceV1::Available {
                source: "record:native.fact.1".to_owned(),
            },
        );
        assert!(!decision.excluded);
        assert!(decision.provenance.is_hydrated(), "{decision:?}");
    }

    /// Real defect this catches: an unconfirmed claim surviving as
    /// `Available`, whose renderer says `source ...` and therefore reads like
    /// host-confirmed grounding.
    #[test]
    fn an_unconfirmed_claim_is_labelled_unresolvable_and_excluded_by_default() {
        let signal = live();
        let control = HostEvidenceControlV1::new(0, 1_000, &signal);
        let decision = pass(true, 4).hydrate(
            &mounted_authority(),
            &scope(),
            &control,
            &ProviderItemProvenanceV1::Available {
                source: "provider-internal-token".to_owned(),
            },
        );
        assert!(decision.excluded);
        assert!(matches!(
            decision.provenance,
            ProviderItemProvenanceV1::Unresolvable { .. }
        ));
    }

    #[test]
    fn unknown_and_already_unresolved_provenance_follow_the_same_exclusion_policy() {
        let signal = live();
        let control = HostEvidenceControlV1::new(0, 1_000, &signal);
        for provenance in [
            ProviderItemProvenanceV1::Unknown,
            ProviderItemProvenanceV1::Unresolvable {
                source: "fact.1".to_owned(),
                reason: "earlier refusal".to_owned(),
            },
        ] {
            assert!(
                pass(true, 4)
                    .hydrate(&mounted_authority(), &scope(), &control, &provenance)
                    .excluded
            );
            assert!(
                !pass(false, 4)
                    .hydrate(&mounted_authority(), &scope(), &control, &provenance)
                    .excluded
            );
        }
    }

    /// The recall contract's redaction default is degrade-allow, so a
    /// declared redaction is never dropped by the unavailable-provenance
    /// policy.
    #[test]
    fn a_declared_redaction_is_degrade_allowed_not_excluded() {
        let signal = live();
        let control = HostEvidenceControlV1::new(0, 1_000, &signal);
        let redacted = ProviderItemProvenanceV1::Redacted {
            reason: "pii".to_owned(),
        };
        let decision = pass(true, 1).hydrate(&mounted_authority(), &scope(), &control, &redacted);
        assert_eq!(decision.provenance, redacted);
        assert!(!decision.excluded);
        assert_eq!(pass(true, 1).attempts_spent(), 0);
    }

    /// Real defect this catches: a hydration pass that runs out of budget
    /// handing the remaining candidates back as raw `Available` claims, and
    /// doing so silently.
    #[test]
    fn an_exhausted_budget_yields_unresolvable_plus_a_typed_lane_degradation() {
        let signal = live();
        let control = HostEvidenceControlV1::new(0, 1_000, &signal);
        let authority = mounted_authority();
        let mut pass = pass(true, 1);
        let claim = ProviderItemProvenanceV1::Available {
            source: "record:native.fact.1".to_owned(),
        };
        let first = pass.hydrate(&authority, &scope(), &control, &claim);
        assert!(first.provenance.is_hydrated());
        assert!(pass.degradation().is_none());

        let second = pass.hydrate(&authority, &scope(), &control, &claim);
        assert!(
            matches!(
                second.provenance,
                ProviderItemProvenanceV1::Unresolvable { .. }
            ),
            "an unattempted claim must never come back as available: {second:?}"
        );
        assert!(second.excluded);
        assert_eq!(
            pass.degradation(),
            Some(&ProvenanceHydrationDegradationV1::BudgetExhausted {
                budget: 1,
                unattempted: 1,
            })
        );
        assert_eq!(pass.attempts_spent(), 1);
    }

    /// Real defect this catches: a deadline that elapses mid-pass being
    /// swallowed, so the untried claims read as available instead of as an
    /// explicitly degraded lane.
    #[test]
    fn a_deadline_that_elapses_mid_pass_is_a_recorded_degradation() {
        let signal = live();
        let control = HostEvidenceControlV1::new(1_000, 1_000, &signal);
        let mut pass = pass(false, 4);
        let decision = pass.hydrate(
            &mounted_authority(),
            &scope(),
            &control,
            &ProviderItemProvenanceV1::Available {
                source: "record:native.fact.1".to_owned(),
            },
        );
        assert!(matches!(
            decision.provenance,
            ProviderItemProvenanceV1::Unresolvable { .. }
        ));
        assert_eq!(
            pass.degradation(),
            Some(&ProvenanceHydrationDegradationV1::DeadlineExceeded { unattempted: 1 })
        );
    }
}
