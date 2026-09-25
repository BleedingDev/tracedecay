use super::connection::Database;
use tracedecay_domain::errors::{Result, TraceDecayError};

impl Database {
    /// Opens one vector-generation authority for a stable repository/worktree
    /// namespace in this registered project store. The retained exact-SQL
    /// handle keeps the canonical project runtime and its writer authority
    /// alive for every clone.
    pub fn open_vector_authority(
        &self,
        authority_namespace: &str,
    ) -> Result<tracedecay_rusqlite_runtime::repository::ProjectVectorAuthorityHandleV1> {
        if !matches!(
            &self.registered_binding().shard_id.scope,
            tracedecay_store::StoreShardScopeV1::Project { .. }
        ) {
            return Err(TraceDecayError::Database {
                message: "vector authority requires a project shard".to_owned(),
                operation: "open vector authority".to_owned(),
            });
        }
        let authority = self.write_authority()?;
        let handle = self
            .authorized_exact_sql_handle(authority)
            .map_err(|error| TraceDecayError::Database {
                message: format!("failed to authorize vector authority storage: {error:?}"),
                operation: "open vector authority".to_owned(),
            })?;
        if handle.binding() != self.registered_binding()
            || handle.verified_locator() != self.registered_verified_locator()
        {
            return Err(TraceDecayError::Database {
                message: "authorized vector authority handle does not match retained project store"
                    .to_owned(),
                operation: "open vector authority".to_owned(),
            });
        }
        tracedecay_rusqlite_runtime::repository::VectorAuthoritySqliteStorage::from_authorized_handle_with_guard(
            handle,
            self.client_guard(),
        )
        .and_then(|storage| storage.open(authority_namespace))
        .map_err(|error| TraceDecayError::Database {
            message: format!("failed to open project vector authority: {error}"),
            operation: "open vector authority".to_owned(),
        })
    }
}
