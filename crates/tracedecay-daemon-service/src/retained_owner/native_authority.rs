//! Late-bound canonical session authority for the Native provider boundary.
//!
//! Native owns no session store, identity bridge, or query shape. Its session
//! lane is upstream `tracedecay_message_search` over the one project session
//! retrieval service the host publishes after the session database is
//! admitted. This module only carries that service from full-server
//! publication to the Native application port that was constructed during
//! core composition, together with the exact checkout the host mounted.

use std::sync::OnceLock;

use tracedecay_contracts::CancellationSignal;
use tracedecay_session_runtime::session_retrieval::{
    SessionApplicationRetrievalFutureV1, SessionApplicationRetrievalPortV1,
    SessionRetrievalServiceOutcome, SessionRetrievalUnavailable,
};

/// Late-bound canonical session authority for a Native owner.
///
/// Native's provider port is constructed during the core server phase, before
/// the project session database and its application retrieval service are
/// admitted. The Native owner receives this proxy during core composition,
/// and the one canonical retrieval service is installed once the full server
/// has published it. Until then every read answers the upstream
/// `service_not_configured` outcome.
pub(crate) struct NativeSessionRetrievalMountV1 {
    authority: OnceLock<std::sync::Arc<dyn SessionApplicationRetrievalPortV1>>,
    profile_id: tracedecay_domain::UserProfileId,
    scope: tracedecay_contracts::ResolvedScope,
}

impl NativeSessionRetrievalMountV1 {
    /// Creates a mount bound to the exact checkout served by one host.
    pub(crate) fn for_project(
        profile_id: tracedecay_domain::UserProfileId,
        scope: tracedecay_contracts::ResolvedScope,
    ) -> Self {
        Self {
            authority: OnceLock::new(),
            profile_id,
            scope,
        }
    }

    /// Installs the canonical retrieval service after session admission.
    pub(crate) fn bind(
        &self,
        authority: std::sync::Arc<dyn SessionApplicationRetrievalPortV1>,
    ) -> std::result::Result<(), &'static str> {
        self.authority
            .set(authority)
            .map_err(|_| "Native session retrieval authority already mounted")
    }

    /// The checkout this host generation serves.
    pub(crate) fn host_scope(&self) -> &tracedecay_contracts::ResolvedScope {
        &self.scope
    }

    /// The profile this host generation serves.
    pub(crate) fn host_profile_id(&self) -> &tracedecay_domain::UserProfileId {
        &self.profile_id
    }

    fn unavailable() -> SessionRetrievalServiceOutcome {
        SessionRetrievalServiceOutcome::Unavailable(
            SessionRetrievalUnavailable::service_not_configured(),
        )
    }
}

impl SessionApplicationRetrievalPortV1 for NativeSessionRetrievalMountV1 {
    fn retrieve_admitted<'a>(
        &'a self,
        context: &'a tracedecay_contracts::RequestContext,
        query: tracedecay_session_memory::session::SessionTemporalQuery,
    ) -> SessionApplicationRetrievalFutureV1<'a> {
        match self.authority.get() {
            Some(authority) => authority.retrieve_admitted(context, query),
            None => Box::pin(async { Self::unavailable() }),
        }
    }

    fn retrieve_admitted_with_cancellation<'a>(
        &'a self,
        context: &'a tracedecay_contracts::RequestContext,
        cancellation: &'a CancellationSignal,
        query: tracedecay_session_memory::session::SessionTemporalQuery,
    ) -> SessionApplicationRetrievalFutureV1<'a> {
        match self.authority.get() {
            Some(authority) => {
                authority.retrieve_admitted_with_cancellation(context, cancellation, query)
            }
            None => Box::pin(async { Self::unavailable() }),
        }
    }
}
