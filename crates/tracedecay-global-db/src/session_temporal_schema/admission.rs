use std::collections::{BTreeMap, BTreeSet};
use std::sync::LazyLock;

use tracedecay_runtime_core::db::engine::{params, QueryExecutor};

use crate::configuration::FreshConfigurationStoreEvidence;
use crate::schema_contract::{
    invariant_trigger_names_for_tables, invariant_trigger_sql_for_tables,
    starts_with_ignore_ascii_case, validate_session_graph_publication_schema_contract,
    validate_session_temporal_schema_contract,
};
use crate::{global_db_operation_error, global_db_operation_message};

use super::{
    MIGRATION_NAME, OPERATION, SESSION_TEMPORAL_AUTHORITY, SESSION_TEMPORAL_SCHEMA_VERSION,
    TEMPORAL_FTS_CONTRACTS, TEMPORAL_TABLE_COLUMNS,
};

const TEMPORAL_FTS_SHADOW_TABLES: &[&str] = &[
    "session_occurrences_fts_config",
    "session_occurrences_fts_content",
    "session_occurrences_fts_data",
    "session_occurrences_fts_docsize",
    "session_occurrences_fts_idx",
    "session_summary_nodes_fts_config",
    "session_summary_nodes_fts_content",
    "session_summary_nodes_fts_data",
    "session_summary_nodes_fts_docsize",
    "session_summary_nodes_fts_idx",
];

type TemporalTableContractInventory = BTreeMap<String, String>;

/// The structural PRAGMA contract can observe columns, indexes, and foreign
/// key metadata, but SQLite does not expose CHECK expressions through a
/// PRAGMA. Keep the canonical CREATE TABLE text in one place (the installer)
/// and compare its normalized form during current-store admission. This pins
/// both CHECK expressions and FOREIGN KEY clauses while allowing harmless
/// formatting and `IF NOT EXISTS` differences in persisted SQLite text.
static EXPECTED_TEMPORAL_TABLE_CONTRACTS: LazyLock<
    std::result::Result<TemporalTableContractInventory, String>,
> = LazyLock::new(build_expected_temporal_table_contracts);

fn build_expected_temporal_table_contracts(
) -> std::result::Result<TemporalTableContractInventory, String> {
    let connection = rusqlite::Connection::open_in_memory()
        .map_err(|error| format!("failed to open canonical session temporal schema: {error}"))?;
    connection
        .execute_batch(super::TEMPORAL_SCHEMA_DDL)
        .map_err(|error| format!("failed to install canonical session temporal schema: {error}"))?;

    let expected_tables = TEMPORAL_TABLE_COLUMNS
        .iter()
        .map(|(table, _)| *table)
        .filter(|table| !table.ends_with("_fts"))
        .collect::<BTreeSet<_>>();
    let mut statement = connection
        .prepare(
            "SELECT name, sql FROM sqlite_master
             WHERE type = 'table' AND name NOT LIKE 'sqlite_%'
             ORDER BY name",
        )
        .map_err(|error| format!("failed to prepare canonical session temporal schema: {error}"))?;
    let rows = statement
        .query_map((), |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, Option<String>>(1)?))
        })
        .map_err(|error| format!("failed to query canonical session temporal schema: {error}"))?;
    let mut inventory = TemporalTableContractInventory::new();
    for row in rows {
        let (name, sql) = row.map_err(|error| {
            format!("failed to read canonical session temporal schema: {error}")
        })?;
        if !expected_tables.contains(name.as_str()) {
            continue;
        }
        let Some(sql) = sql else {
            return Err(format!(
                "canonical session temporal table '{name}' has no CREATE TABLE definition"
            ));
        };
        if inventory
            .insert(name.to_ascii_lowercase(), normalize_schema_sql(&sql))
            .is_some()
        {
            return Err(format!(
                "canonical session temporal schema repeats table '{name}'"
            ));
        }
    }
    if inventory.len() != expected_tables.len() {
        return Err(format!(
            "canonical session temporal schema defines {} tables, expected {}",
            inventory.len(),
            expected_tables.len()
        ));
    }
    Ok(inventory)
}

/// Read-only admission result for the final session-temporal schema.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum SessionTemporalSchemaAdmission {
    /// The persisted schema and its objects exactly match the final contract.
    Current,
    /// The registered store is proven empty and may receive the final contract.
    Fresh,
}

/// Classifies a store without changing its schema or retained session state.
#[hotpath::measure(future = true, label = "session_temporal.schema.admit")]
pub(crate) async fn require_admissible_session_temporal_schema(
    conn: &impl QueryExecutor,
    fresh_store: Option<&FreshConfigurationStoreEvidence>,
) -> tracedecay_domain::errors::Result<SessionTemporalSchemaAdmission> {
    let version = schema_version(conn)
        .await
        .map_err(|error| session_temporal_reset_required(error.to_string()))?;
    match version {
        Some(SESSION_TEMPORAL_SCHEMA_VERSION) => {
            validate_current_session_temporal_schema(conn).await?;
            Ok(SessionTemporalSchemaAdmission::Current)
        }
        Some(version) => Err(session_temporal_reset_required(format!(
            "persisted schema version {version} does not match final version {SESSION_TEMPORAL_SCHEMA_VERSION}"
        ))),
        None if fresh_store.is_some() => Ok(SessionTemporalSchemaAdmission::Fresh),
        None => Err(session_temporal_reset_required(
            "a nonempty store does not carry the final schema marker",
        )),
    }
}

pub(super) async fn validate_current_session_temporal_schema(
    conn: &impl QueryExecutor,
) -> tracedecay_domain::errors::Result<()> {
    let tables = TEMPORAL_TABLE_COLUMNS
        .iter()
        .map(|(table, _)| *table)
        .filter(|table| !table.ends_with("_fts"))
        .collect::<Vec<_>>();
    validate_session_temporal_schema_contract(conn, &tables)
        .await
        .map_err(|error| session_temporal_reset_required(error.to_string()))?;
    validate_temporal_table_contracts(conn, &tables)
        .await
        .map_err(|error| session_temporal_reset_required(error.to_string()))?;
    validate_temporal_trigger_inventory(conn, &tables)
        .await
        .map_err(|error| session_temporal_reset_required(error.to_string()))?;
    validate_temporal_namespace_and_fts(conn).await
}

async fn validate_temporal_table_contracts(
    conn: &impl QueryExecutor,
    tables: &[&str],
) -> tracedecay_domain::errors::Result<()> {
    let expected = EXPECTED_TEMPORAL_TABLE_CONTRACTS
        .as_ref()
        .map_err(|error| global_db_operation_message(OPERATION, error.clone()))?;
    let mut rows = conn
        .query(
            "SELECT name, sql FROM sqlite_master
             WHERE type = 'table' AND name NOT LIKE 'sqlite_%'
             ORDER BY name",
            (),
        )
        .await
        .map_err(|error| global_db_operation_error(OPERATION, error))?;
    let mut actual = TemporalTableContractInventory::new();
    while let Some(row) = rows
        .next()
        .await
        .map_err(|error| global_db_operation_error(OPERATION, error))?
    {
        let name = row
            .get::<String>(0)
            .map_err(|error| global_db_operation_error(OPERATION, error))?;
        if !tables.iter().any(|table| table.eq_ignore_ascii_case(&name)) {
            continue;
        }
        let sql = row
            .get::<Option<String>>(1)
            .map_err(|error| global_db_operation_error(OPERATION, error))?
            .ok_or_else(|| {
                global_db_operation_message(
                    OPERATION,
                    format!("temporal table '{name}' has no CREATE TABLE definition"),
                )
            })?;
        actual.insert(name.to_ascii_lowercase(), normalize_schema_sql(&sql));
    }

    for table in tables {
        let key = table.to_ascii_lowercase();
        let Some(expected_sql) = expected.get(&key) else {
            return Err(global_db_operation_message(
                OPERATION,
                format!("canonical session temporal contract is missing table '{table}'"),
            ));
        };
        let Some(actual_sql) = actual.get(&key) else {
            return Err(global_db_operation_message(
                OPERATION,
                format!("temporal table '{table}' is missing"),
            ));
        };
        if actual_sql != expected_sql {
            return Err(global_db_operation_message(
                OPERATION,
                format!(
                    "table '{table}' has an incompatible normalized CHECK/FOREIGN KEY contract"
                ),
            ));
        }
    }
    Ok(())
}

async fn validate_temporal_trigger_inventory(
    conn: &impl QueryExecutor,
    tables: &[&str],
) -> tracedecay_domain::errors::Result<()> {
    let expected_names = invariant_trigger_names_for_tables(tables);
    let expected_sql = invariant_trigger_sql_for_tables(tables);
    if expected_names.len() != expected_sql.len() {
        return Err(global_db_operation_message(
            OPERATION,
            "temporal trigger contract has mismatched name and SQL inventories",
        ));
    }
    let mut expected = BTreeMap::new();
    for (name, sql) in expected_names.into_iter().zip(expected_sql) {
        if expected
            .insert(name.to_ascii_lowercase(), normalize_schema_sql(sql))
            .is_some()
        {
            return Err(global_db_operation_message(
                OPERATION,
                format!("temporal trigger contract repeats '{name}'"),
            ));
        }
    }

    let mut rows = conn
        .query(
            "SELECT name, tbl_name, sql FROM sqlite_master
             WHERE type = 'trigger' ORDER BY name",
            (),
        )
        .await
        .map_err(|error| global_db_operation_error(OPERATION, error))?;
    let mut actual = BTreeMap::new();
    while let Some(row) = rows
        .next()
        .await
        .map_err(|error| global_db_operation_error(OPERATION, error))?
    {
        let name = row
            .get::<String>(0)
            .map_err(|error| global_db_operation_error(OPERATION, error))?;
        let table = row
            .get::<String>(1)
            .map_err(|error| global_db_operation_error(OPERATION, error))?;
        if !tables
            .iter()
            .any(|expected_table| expected_table.eq_ignore_ascii_case(&table))
        {
            continue;
        }
        let sql = row
            .get::<Option<String>>(2)
            .map_err(|error| global_db_operation_error(OPERATION, error))?
            .ok_or_else(|| {
                global_db_operation_message(
                    OPERATION,
                    format!("temporal trigger '{name}' has no CREATE TRIGGER definition"),
                )
            })?;
        if actual
            .insert(name.to_ascii_lowercase(), normalize_schema_sql(&sql))
            .is_some()
        {
            return Err(global_db_operation_message(
                OPERATION,
                format!("temporal trigger inventory repeats '{name}'"),
            ));
        }
    }

    if actual.len() != expected.len() || actual.keys().ne(expected.keys()) {
        return Err(global_db_operation_message(
            OPERATION,
            "temporal trigger inventory is not exact",
        ));
    }
    for (name, expected_sql) in expected {
        if actual.get(&name) != Some(&expected_sql) {
            return Err(global_db_operation_message(
                OPERATION,
                format!("temporal trigger '{name}' has an incompatible normalized contract"),
            ));
        }
    }
    Ok(())
}

async fn validate_temporal_namespace_and_fts(
    conn: &impl QueryExecutor,
) -> tracedecay_domain::errors::Result<()> {
    validate_temporal_namespace_tables(conn)
        .await
        .map_err(|error| session_temporal_reset_required(error.to_string()))?;
    validate_session_graph_publication_schema_contract(conn)
        .await
        .map_err(|error| session_temporal_reset_required(error.to_string()))?;
    validate_temporal_fts_contracts(conn)
        .await
        .map_err(|error| session_temporal_reset_required(error.to_string()))?;
    validate_temporal_fts_match(conn)
        .await
        .map_err(|error| session_temporal_reset_required(error.to_string()))
}

async fn validate_temporal_namespace_tables(
    conn: &impl QueryExecutor,
) -> tracedecay_domain::errors::Result<()> {
    let expected = TEMPORAL_TABLE_COLUMNS
        .iter()
        .map(|(table, _)| *table)
        .chain(TEMPORAL_FTS_SHADOW_TABLES.iter().copied())
        .collect::<BTreeSet<_>>();
    let mut rows = conn
        .query(
            "SELECT name FROM sqlite_master
             WHERE type IN ('table', 'view') AND name NOT LIKE 'sqlite_%'
             ORDER BY name",
            (),
        )
        .await
        .map_err(|error| global_db_operation_error(OPERATION, error))?;
    while let Some(row) = rows
        .next()
        .await
        .map_err(|error| global_db_operation_error(OPERATION, error))?
    {
        let name = row
            .get::<String>(0)
            .map_err(|error| global_db_operation_error(OPERATION, error))?;
        if belongs_to_temporal_namespace(&name) && !expected.contains(name.as_str()) {
            return Err(global_db_operation_message(
                OPERATION,
                format!("unexpected session temporal table or view '{name}'"),
            ));
        }
    }
    Ok(())
}

fn belongs_to_temporal_namespace(name: &str) -> bool {
    [
        "session_agent_",
        "session_agents",
        "session_assertion",
        "session_current_entit",
        "session_derived_evidence",
        "session_external_payload",
        "session_logical_copy",
        "session_occurrence",
        "session_query_cursor",
        "session_refresh",
        "session_relation",
        "session_summary_availability",
        "session_summary_",
        "session_summary_nodes",
        "session_temporal",
        "session_thread",
        "session_turn",
    ]
    .iter()
    .any(|prefix| starts_with_ignore_ascii_case(name, prefix))
}

pub(super) fn session_temporal_reset_required(
    reason: impl Into<String>,
) -> tracedecay_domain::errors::TraceDecayError {
    tracedecay_domain::errors::TraceDecayError::reset_required(SESSION_TEMPORAL_AUTHORITY, reason)
}

pub(super) async fn validate_temporal_fts_contracts(
    conn: &impl QueryExecutor,
) -> tracedecay_domain::errors::Result<()> {
    for (table, expected_sql) in TEMPORAL_FTS_CONTRACTS {
        let mut rows = conn
            .query(
                "SELECT sql FROM sqlite_master WHERE type = 'table' AND name = ?1",
                params![*table],
            )
            .await
            .map_err(|error| global_db_operation_error(OPERATION, error))?;
        let Some(row) = rows
            .next()
            .await
            .map_err(|error| global_db_operation_error(OPERATION, error))?
        else {
            return Err(global_db_operation_message(
                OPERATION,
                format!("temporal FTS table '{table}' is missing"),
            ));
        };
        let sql = row
            .get::<String>(0)
            .map_err(|error| global_db_operation_error(OPERATION, error))?;
        if normalize_fts_sql(&sql) != *expected_sql {
            return Err(global_db_operation_message(
                OPERATION,
                format!("table '{table}' has an incompatible temporal FTS contract"),
            ));
        }
    }
    Ok(())
}

fn normalize_fts_sql(sql: &str) -> String {
    normalize_schema_sql(sql)
}

fn normalize_schema_sql(sql: &str) -> String {
    sql.chars()
        .filter(|character| !character.is_whitespace() && *character != ';')
        .flat_map(char::to_lowercase)
        .collect::<String>()
        .replace("ifnotexists", "")
}

pub(super) async fn validate_temporal_fts_match(
    conn: &impl QueryExecutor,
) -> tracedecay_domain::errors::Result<()> {
    for (table, _) in TEMPORAL_FTS_CONTRACTS {
        conn.query(
            &format!("SELECT rowid FROM {table} WHERE {table} MATCH ?1 LIMIT 1"),
            params!["__tracedecay_temporal_fts_probe__"],
        )
        .await
        .map_err(|error| global_db_operation_error(OPERATION, error))?;
    }
    Ok(())
}

async fn schema_version(
    conn: &impl QueryExecutor,
) -> tracedecay_domain::errors::Result<Option<i64>> {
    let mut tables = conn
        .query(
            "SELECT 1 FROM sqlite_master
             WHERE type = 'table' AND name = 'session_temporal_schema_migrations'",
            (),
        )
        .await
        .map_err(|error| global_db_operation_error(OPERATION, error))?;
    if tables
        .next()
        .await
        .map_err(|error| global_db_operation_error(OPERATION, error))?
        .is_none()
    {
        return Ok(None);
    }

    let mut rows = conn
        .query(
            "SELECT name, version FROM session_temporal_schema_migrations ORDER BY name",
            (),
        )
        .await
        .map_err(|error| global_db_operation_error(OPERATION, error))?;
    let Some(row) = rows
        .next()
        .await
        .map_err(|error| global_db_operation_error(OPERATION, error))?
    else {
        return Err(global_db_operation_message(
            OPERATION,
            "session temporal schema marker is missing",
        ));
    };
    let name = row
        .get::<String>(0)
        .map_err(|error| global_db_operation_error(OPERATION, error))?;
    let version = row
        .get::<i64>(1)
        .map_err(|error| global_db_operation_error(OPERATION, error))?;
    if name != MIGRATION_NAME
        || rows
            .next()
            .await
            .map_err(|error| global_db_operation_error(OPERATION, error))?
            .is_some()
    {
        return Err(global_db_operation_message(
            OPERATION,
            "session temporal schema marker is not the exact final singleton",
        ));
    }
    Ok(Some(version))
}
