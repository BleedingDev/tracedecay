use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use rusqlite::{Connection, OptionalExtension, params};
use tracedecay_code_index::clones::CodeIndexCloneBodyV1;
use tracedecay_code_index::production::{
    CodeIndexExecutionControlV1, VerifiedSealedLexicalCursorV1, VerifiedSealedLexicalPageV1,
    VerifiedSealedLexicalSourceReceiptV1,
};
use tracedecay_private_fs::framed_log::{DirectorySyncPolicy, sync_parent_directory};
use tracedecay_private_fs::{create_private_file_retained, open_private_file};

use super::super::CodeLexicalProjectionMetadataV1;
use super::builder::{
    BuilderMutationGuardV1, compute_clone_section_digests, install_clone_freeze,
    register_builder_mutation_gate, sqlite_file_size, verify_clone_rows,
};
use super::format::{
    RECEIPT_RESERVATION_BYTES, VerifiedCodeLexicalArtifactV1, artifact_digest,
    decode_padded_receipt, metadata_digest, new_verified_receipt, padded_receipt,
};
use super::schema::{CODE_LEXICAL_ARTIFACT_FORMAT_REVISION_V1, LexicalArtifactLayoutV1};
use super::{CodeLexicalArtifactErrorV1, checkpoint, open_builder_connection, sqlite_error};

/// Counts the clone rows a successor could keep from its predecessor.
///
/// The counts are persisted in the resumable successor state while the clone
/// backfill is in progress.  They are deliberately expressed in rows rather
/// than bytes: a row that survives the successor reconciliation is the unit
/// of work the implementation is required to avoid rebuilding.
#[derive(Clone, Copy, Debug, Default, serde::Deserialize, serde::Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct CloneSuccessorReuseAccountingV1 {
    pub occurrences_reused: u64,
    pub occurrences_inserted: u64,
    pub occurrences_replaced: u64,
    pub occurrences_deleted: u64,
    pub exact_postings_reused: u64,
    pub exact_postings_inserted: u64,
    pub exact_postings_deleted: u64,
    pub fingerprint_postings_reused: u64,
    pub fingerprint_postings_inserted: u64,
    pub fingerprint_postings_deleted: u64,
    pub payloads_reused: u64,
    pub payloads_inserted: u64,
    pub payloads_deleted: u64,
}

impl CloneSuccessorReuseAccountingV1 {
    fn absorb(&mut self, delta: Self) {
        self.occurrences_reused = self
            .occurrences_reused
            .saturating_add(delta.occurrences_reused);
        self.occurrences_inserted = self
            .occurrences_inserted
            .saturating_add(delta.occurrences_inserted);
        self.occurrences_replaced = self
            .occurrences_replaced
            .saturating_add(delta.occurrences_replaced);
        self.occurrences_deleted = self
            .occurrences_deleted
            .saturating_add(delta.occurrences_deleted);
        self.exact_postings_reused = self
            .exact_postings_reused
            .saturating_add(delta.exact_postings_reused);
        self.exact_postings_inserted = self
            .exact_postings_inserted
            .saturating_add(delta.exact_postings_inserted);
        self.exact_postings_deleted = self
            .exact_postings_deleted
            .saturating_add(delta.exact_postings_deleted);
        self.fingerprint_postings_reused = self
            .fingerprint_postings_reused
            .saturating_add(delta.fingerprint_postings_reused);
        self.fingerprint_postings_inserted = self
            .fingerprint_postings_inserted
            .saturating_add(delta.fingerprint_postings_inserted);
        self.fingerprint_postings_deleted = self
            .fingerprint_postings_deleted
            .saturating_add(delta.fingerprint_postings_deleted);
        self.payloads_reused = self.payloads_reused.saturating_add(delta.payloads_reused);
        self.payloads_inserted = self
            .payloads_inserted
            .saturating_add(delta.payloads_inserted);
        self.payloads_deleted = self.payloads_deleted.saturating_add(delta.payloads_deleted);
    }
}

pub struct CodeLexicalCloneSuccessorV1 {
    connection: Connection,
    mutation_gate: Arc<std::sync::atomic::AtomicU8>,
    staging_path: PathBuf,
    prior: VerifiedCodeLexicalArtifactV1,
    metadata: CodeLexicalProjectionMetadataV1,
    reuse_accounting: CloneSuccessorReuseAccountingV1,
}

impl CodeLexicalCloneSuccessorV1 {
    pub fn open_or_create(
        prior_path: impl AsRef<Path>,
        staging_path: impl AsRef<Path>,
        prior: VerifiedCodeLexicalArtifactV1,
        metadata: CodeLexicalProjectionMetadataV1,
        memory_budget_bytes: usize,
    ) -> Result<Self, CodeLexicalArtifactErrorV1> {
        let staging_path = staging_path.as_ref();
        if staging_path.exists() {
            return Self::open(staging_path, prior, metadata, memory_budget_bytes);
        }
        initialize_successor(
            prior_path.as_ref(),
            staging_path,
            &prior,
            &metadata,
            memory_budget_bytes,
        )?;
        Self::open(staging_path, prior, metadata, memory_budget_bytes)
    }

    fn open(
        staging_path: &Path,
        prior: VerifiedCodeLexicalArtifactV1,
        metadata: CodeLexicalProjectionMetadataV1,
        memory_budget_bytes: usize,
    ) -> Result<Self, CodeLexicalArtifactErrorV1> {
        let connection = open_builder_connection(staging_path, memory_budget_bytes)?;
        let mutation_gate = register_builder_mutation_gate(&connection)?;
        let (prior_digest, format_revision, accounting_bytes): (String, i64, Vec<u8>) = connection
            .query_row(
                "SELECT prior_artifact_digest, (SELECT format_revision FROM artifact_state WHERE singleton = 1), reuse_accounting FROM clone_successor_state WHERE singleton = 1",
                [],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .map_err(|error| {
                CodeLexicalArtifactErrorV1::Incompatible(format!(
                    "clone successor state is unavailable: {error}"
                ))
            })?;
        if prior_digest != prior.artifact_digest().as_str()
            || format_revision != i64::from(CODE_LEXICAL_ARTIFACT_FORMAT_REVISION_V1)
            || metadata_digest(&metadata)? != *prior.metadata_digest()
        {
            return Err(CodeLexicalArtifactErrorV1::Incompatible(
                "clone successor does not match its prior artifact or metadata".to_owned(),
            ));
        }
        let reuse_accounting = serde_json::from_slice(&accounting_bytes).map_err(|error| {
            CodeLexicalArtifactErrorV1::Corrupt(format!(
                "clone successor reuse accounting is invalid: {error}"
            ))
        })?;
        Ok(Self {
            connection,
            mutation_gate,
            staging_path: staging_path.to_path_buf(),
            prior,
            metadata,
            reuse_accounting,
        })
    }

    /// Return the durable row-reuse accounting observed so far.
    #[must_use]
    pub fn reuse_accounting(&self) -> CloneSuccessorReuseAccountingV1 {
        self.reuse_accounting
    }

    pub fn next_cursor(
        &self,
    ) -> Result<Option<VerifiedSealedLexicalCursorV1>, CodeLexicalArtifactErrorV1> {
        let (next_page, bytes): (i64, Option<Vec<u8>>) = self
            .connection
            .query_row(
                "SELECT next_page_ordinal, next_cursor FROM clone_successor_state WHERE singleton = 1",
                [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .map_err(sqlite_error)?;
        let cursor = bytes
            .map(|bytes| {
                VerifiedSealedLexicalCursorV1::restore_persisted(&bytes)
                    .map_err(|error| CodeLexicalArtifactErrorV1::Corrupt(error.to_string()))
            })
            .transpose()?;
        if cursor.as_ref().map_or(next_page != 0, |cursor| {
            u64::try_from(next_page).ok() != Some(cursor.next_page_ordinal())
        }) {
            return Err(CodeLexicalArtifactErrorV1::Corrupt(
                "clone successor page ordinal disagrees with its source cursor".to_owned(),
            ));
        }
        Ok(cursor)
    }

    pub fn append_page(
        &mut self,
        page: &VerifiedSealedLexicalPageV1,
        control: &dyn CodeIndexExecutionControlV1,
    ) -> Result<(), CodeLexicalArtifactErrorV1> {
        checkpoint(control)?;
        let _authority = BuilderMutationGuardV1::enter(&self.mutation_gate)?;
        let transaction = self.connection.transaction().map_err(sqlite_error)?;
        let next_page: i64 = transaction
            .query_row(
                "SELECT next_page_ordinal FROM clone_successor_state WHERE singleton = 1",
                [],
                |row| row.get(0),
            )
            .map_err(sqlite_error)?;
        if u64::try_from(next_page).ok() != Some(page.page_ordinal()) {
            return Err(CodeLexicalArtifactErrorV1::Contract(
                "clone successor page is not the next source page".to_owned(),
            ));
        }
        verify_copied_source_page(&transaction, page)?;
        let next_cursor = page
            .next_cursor()
            .persisted_bytes()
            .map_err(|error| CodeLexicalArtifactErrorV1::Contract(error.to_string()))?;
        let mut accounting = self.reuse_accounting;
        let delta = append_clone_rows(&transaction, page, control)?;
        accounting.absorb(delta);
        let accounting_bytes = serde_json::to_vec(&accounting)
            .map_err(|error| CodeLexicalArtifactErrorV1::Contract(error.to_string()))?;
        transaction
            .execute(
                "UPDATE clone_successor_state SET next_page_ordinal = ?1, next_cursor = ?2, reuse_accounting = ?3 WHERE singleton = 1",
                params![
                    i64::try_from(page.page_ordinal().saturating_add(1))
                        .map_err(|error| CodeLexicalArtifactErrorV1::Contract(error.to_string()))?,
                    next_cursor,
                    accounting_bytes,
                ],
            )
            .map_err(sqlite_error)?;
        checkpoint(control)?;
        transaction.commit().map_err(sqlite_error)?;
        self.reuse_accounting = accounting;
        Ok(())
    }

    pub fn verify_resumed_page(
        &self,
        page: &VerifiedSealedLexicalPageV1,
        control: &dyn CodeIndexExecutionControlV1,
    ) -> Result<(), CodeLexicalArtifactErrorV1> {
        checkpoint(control)?;
        verify_copied_source_page(&self.connection, page)?;
        verify_clone_page_rows(&self.connection, page, control)
    }

    pub fn finish(
        &mut self,
        source: &VerifiedSealedLexicalSourceReceiptV1,
        control: &dyn CodeIndexExecutionControlV1,
    ) -> Result<VerifiedCodeLexicalArtifactV1, CodeLexicalArtifactErrorV1> {
        checkpoint(control)?;
        verify_source_receipt(&self.prior, source)?;
        let next_page: i64 = self
            .connection
            .query_row(
                "SELECT next_page_ordinal FROM clone_successor_state WHERE singleton = 1",
                [],
                |row| row.get(0),
            )
            .map_err(sqlite_error)?;
        if u64::try_from(next_page).ok() != Some(source.page_count()) {
            return Err(CodeLexicalArtifactErrorV1::Contract(
                "clone successor has not consumed every source page".to_owned(),
            ));
        }
        let _authority = BuilderMutationGuardV1::enter(&self.mutation_gate)?;
        let transaction = self.connection.transaction().map_err(sqlite_error)?;
        let mut accounting = self.reuse_accounting;
        prune_stale_clone_rows(&transaction, &mut accounting)?;
        derive_clone_fingerprint_counts(&transaction)?;
        verify_clone_rows(&transaction, source)?;
        install_clone_immutability(&transaction)?;
        install_clone_freeze(&transaction, LexicalArtifactLayoutV1::V16)?;
        transaction
            .execute("DROP TABLE clone_successor_seen", [])
            .map_err(sqlite_error)?;
        transaction
            .execute("DROP TABLE clone_successor_state", [])
            .map_err(sqlite_error)?;
        let mut sections = self
            .prior
            .section_digests()
            .iter()
            .take(11)
            .cloned()
            .collect::<Vec<_>>();
        if sections.len() != 11 {
            return Err(CodeLexicalArtifactErrorV1::Corrupt(
                "clone successor prior is missing lexical section digests".to_owned(),
            ));
        }
        sections.extend(compute_clone_section_digests(
            &transaction,
            control,
            LexicalArtifactLayoutV1::V16,
        )?);
        let metadata_digest = metadata_digest(&self.metadata)?;
        let digest = artifact_digest(
            &metadata_digest,
            source.source_state_digest(),
            source.format_revision(),
            source.page_count(),
            source.total_chunks(),
            source.total_payload_bytes(),
            source.total_imports(),
            source.import_payload_bytes(),
            source.import_dictionary_digest(),
            source.cumulative_digest(),
            &sections,
            CODE_LEXICAL_ARTIFACT_FORMAT_REVISION_V1,
        )?;
        let file_size = sqlite_file_size(&transaction)?;
        let receipt = new_verified_receipt(
            self.metadata.clone(),
            metadata_digest,
            source,
            digest,
            sections,
            file_size,
            LexicalArtifactLayoutV1::V16,
        );
        let encoded = padded_receipt(&receipt)?;
        transaction
            .execute(
                "UPDATE artifact_state SET format_revision = ?1, receipt = ?2 WHERE singleton = 1",
                params![i64::from(CODE_LEXICAL_ARTIFACT_FORMAT_REVISION_V1), encoded,],
            )
            .map_err(sqlite_error)?;
        checkpoint(control)?;
        transaction.commit().map_err(sqlite_error)?;
        self.reuse_accounting = accounting;
        open_private_file(&self.staging_path)
            .map_err(|error| CodeLexicalArtifactErrorV1::Io(error.to_string()))?
            .sync_all()
            .map_err(|error| CodeLexicalArtifactErrorV1::Io(error.to_string()))?;
        Ok(receipt)
    }
}

fn initialize_successor(
    prior_path: &Path,
    staging_path: &Path,
    prior: &VerifiedCodeLexicalArtifactV1,
    metadata: &CodeLexicalProjectionMetadataV1,
    memory_budget_bytes: usize,
) -> Result<(), CodeLexicalArtifactErrorV1> {
    let mut source = open_private_file(prior_path)
        .map_err(|error| CodeLexicalArtifactErrorV1::Io(error.to_string()))?;
    let mut target = create_private_file_retained(staging_path)
        .map_err(|error| CodeLexicalArtifactErrorV1::Io(error.into_error().to_string()))?;
    io::copy(&mut source, &mut target)
        .map_err(|error| CodeLexicalArtifactErrorV1::Io(error.to_string()))?;
    target
        .sync_all()
        .map_err(|error| CodeLexicalArtifactErrorV1::Io(error.to_string()))?;
    drop(target);
    let connection = open_builder_connection(staging_path, memory_budget_bytes)?;
    let stored: Vec<u8> = connection
        .query_row(
            "SELECT receipt FROM artifact_state WHERE singleton = 1",
            [],
            |row| row.get(0),
        )
        .map_err(sqlite_error)?;
    if decode_padded_receipt(&stored)?.as_ref() != Some(prior)
        || metadata_digest(metadata)? != *prior.metadata_digest()
    {
        return Err(CodeLexicalArtifactErrorV1::Incompatible(
            "clone successor prior artifact does not match its receipt".to_owned(),
        ));
    }
    prepare_clone_tables(&connection)?;
    connection
        .execute_batch(
            "CREATE TABLE clone_successor_state (
                singleton INTEGER PRIMARY KEY CHECK(singleton = 1),
                prior_artifact_digest TEXT NOT NULL,
                next_page_ordinal INTEGER NOT NULL,
                next_cursor BLOB,
                reuse_accounting BLOB NOT NULL
             );",
        )
        .map_err(sqlite_error)?;
    connection
        .execute_batch(
            "CREATE TABLE clone_successor_seen (
                symbol_occurrence_id TEXT PRIMARY KEY
             ) WITHOUT ROWID;",
        )
        .map_err(sqlite_error)?;
    let accounting = serde_json::to_vec(&CloneSuccessorReuseAccountingV1::default())
        .map_err(|error| CodeLexicalArtifactErrorV1::Contract(error.to_string()))?;
    connection
        .execute(
            "INSERT INTO clone_successor_state(singleton, prior_artifact_digest, next_page_ordinal, next_cursor, reuse_accounting) VALUES (1, ?1, 0, NULL, ?2)",
            params![prior.artifact_digest().as_str(), accounting],
        )
        .map_err(sqlite_error)?;
    connection
        .execute(
            "UPDATE artifact_state SET format_revision = ?1, receipt = ?2 WHERE singleton = 1",
            params![
                i64::from(CODE_LEXICAL_ARTIFACT_FORMAT_REVISION_V1),
                vec![0u8; RECEIPT_RESERVATION_BYTES],
            ],
        )
        .map_err(sqlite_error)?;
    connection
        .execute("DELETE FROM finalization_state", [])
        .map_err(sqlite_error)?;
    connection
        .execute_batch("PRAGMA optimize;")
        .map_err(sqlite_error)?;
    drop(connection);
    sync_parent_directory(staging_path, DirectorySyncPolicy::Strict)
        .map_err(|error| CodeLexicalArtifactErrorV1::Io(error.to_string()))
}

const DROP_CLONE_MUTATION_TRIGGERS_SQL: &str =
    "DROP TRIGGER IF EXISTS frozen_clone_body_payloads_insert;
     DROP TRIGGER IF EXISTS frozen_clone_occurrences_insert;
     DROP TRIGGER IF EXISTS frozen_clone_exact_postings_insert;
     DROP TRIGGER IF EXISTS frozen_clone_fingerprint_counts_insert;
     DROP TRIGGER IF EXISTS frozen_clone_fingerprint_postings_insert;
     DROP TRIGGER IF EXISTS builder_gate_clone_body_payloads_insert;
     DROP TRIGGER IF EXISTS builder_gate_clone_body_payloads_update;
     DROP TRIGGER IF EXISTS builder_gate_clone_body_payloads_delete;
     DROP TRIGGER IF EXISTS builder_gate_clone_occurrences_insert;
     DROP TRIGGER IF EXISTS builder_gate_clone_occurrences_update;
     DROP TRIGGER IF EXISTS builder_gate_clone_occurrences_delete;
     DROP TRIGGER IF EXISTS builder_gate_clone_exact_postings_insert;
     DROP TRIGGER IF EXISTS builder_gate_clone_exact_postings_update;
     DROP TRIGGER IF EXISTS builder_gate_clone_exact_postings_delete;
     DROP TRIGGER IF EXISTS builder_gate_clone_fingerprint_postings_insert;
     DROP TRIGGER IF EXISTS builder_gate_clone_fingerprint_postings_update;
     DROP TRIGGER IF EXISTS builder_gate_clone_fingerprint_postings_delete;
     DROP TRIGGER IF EXISTS immutable_clone_body_payloads_update;
     DROP TRIGGER IF EXISTS immutable_clone_body_payloads_delete;
     DROP TRIGGER IF EXISTS immutable_clone_occurrences_update;
     DROP TRIGGER IF EXISTS immutable_clone_occurrences_delete;
     DROP TRIGGER IF EXISTS immutable_clone_exact_postings_update;
     DROP TRIGGER IF EXISTS immutable_clone_exact_postings_delete;
     DROP TRIGGER IF EXISTS immutable_clone_fingerprint_postings_update;
     DROP TRIGGER IF EXISTS immutable_clone_fingerprint_postings_delete;
     DROP TRIGGER IF EXISTS immutable_clone_fingerprint_counts_update;
     DROP TRIGGER IF EXISTS immutable_clone_fingerprint_counts_delete;
     DROP TRIGGER IF EXISTS immutable_clone_fingerprint_postings_pages_update;
     DROP TRIGGER IF EXISTS immutable_clone_fingerprint_postings_pages_delete;
     DROP TABLE IF EXISTS clone_fingerprint_postings_pages;";

const CREATE_CLONE_INDEX_TABLES_SQL: &str = "CREATE TABLE clone_body_payloads (
         payload_digest TEXT PRIMARY KEY,
         payload BLOB NOT NULL
     ) WITHOUT ROWID;
     CREATE TABLE clone_occurrences (
         symbol_occurrence_id TEXT PRIMARY KEY,
         payload_digest TEXT NOT NULL,
         path TEXT NOT NULL,
         body_start INTEGER NOT NULL,
         body_end INTEGER NOT NULL,
         occurrence BLOB NOT NULL
     ) WITHOUT ROWID;
     CREATE TABLE clone_exact_postings (
         class INTEGER NOT NULL,
         normalization_revision INTEGER NOT NULL,
         digest TEXT NOT NULL,
         symbol_occurrence_id TEXT NOT NULL,
         payload_digest TEXT NOT NULL,
         PRIMARY KEY(class, normalization_revision, digest, symbol_occurrence_id)
     ) WITHOUT ROWID;";

const CREATE_CLONE_FINGERPRINT_TABLES_SQL: &str =
    "CREATE TABLE clone_fingerprint_counts (
         language TEXT NOT NULL,
         class INTEGER NOT NULL,
         normalization_revision INTEGER NOT NULL,
         fingerprint INTEGER NOT NULL,
         posting_count INTEGER NOT NULL,
         PRIMARY KEY(language, class, normalization_revision, fingerprint)
     ) WITHOUT ROWID;
     CREATE TABLE clone_fingerprint_postings (
         language TEXT NOT NULL,
         class INTEGER NOT NULL,
         normalization_revision INTEGER NOT NULL,
         fingerprint INTEGER NOT NULL,
         symbol_occurrence_id TEXT NOT NULL,
         token_position INTEGER NOT NULL,
         payload_digest TEXT NOT NULL,
         body_digest TEXT NOT NULL,
         PRIMARY KEY(language, class, normalization_revision, fingerprint, symbol_occurrence_id, token_position)
     ) WITHOUT ROWID;";

fn prepare_clone_tables(connection: &Connection) -> Result<(), CodeLexicalArtifactErrorV1> {
    connection
        .execute_batch(DROP_CLONE_MUTATION_TRIGGERS_SQL)
        .map_err(sqlite_error)?;

    let has_payloads = table_exists(connection, "clone_body_payloads")?;
    let has_occurrences = table_exists(connection, "clone_occurrences")?;
    let has_exact = table_exists(connection, "clone_exact_postings")?;
    if has_payloads != has_occurrences || has_payloads != has_exact {
        connection
            .execute_batch(
                "DROP TABLE IF EXISTS clone_exact_postings;
                 DROP TABLE IF EXISTS clone_occurrences;
                 DROP TABLE IF EXISTS clone_body_payloads;",
            )
            .map_err(sqlite_error)?;
    }
    if !table_exists(connection, "clone_body_payloads")? {
        connection
            .execute_batch(CREATE_CLONE_INDEX_TABLES_SQL)
            .map_err(sqlite_error)?;
    }

    // Counts are derived from the final fingerprint posting tree.  Dropping
    // them here avoids trusting a predecessor's aggregate after rows are
    // selectively replaced or pruned.
    connection
        .execute("DROP TABLE IF EXISTS clone_fingerprint_counts", [])
        .map_err(sqlite_error)?;
    if !table_exists(connection, "clone_fingerprint_postings")? {
        connection
            .execute_batch(CREATE_CLONE_FINGERPRINT_TABLES_SQL)
            .map_err(sqlite_error)?;
    } else {
        connection
            .execute(
                "CREATE TABLE clone_fingerprint_counts (
                    language TEXT NOT NULL,
                    class INTEGER NOT NULL,
                    normalization_revision INTEGER NOT NULL,
                    fingerprint INTEGER NOT NULL,
                    posting_count INTEGER NOT NULL,
                    PRIMARY KEY(language, class, normalization_revision, fingerprint)
                 ) WITHOUT ROWID",
                [],
            )
            .map_err(sqlite_error)?;
    }

    connection
        .execute_batch(
            "CREATE TRIGGER builder_gate_clone_body_payloads_insert BEFORE INSERT ON clone_body_payloads WHEN tracedecay_lexical_builder_append_authorized() != 1 BEGIN SELECT RAISE(ABORT, 'private lexical builder mutation required'); END;
             CREATE TRIGGER builder_gate_clone_body_payloads_update BEFORE UPDATE ON clone_body_payloads WHEN tracedecay_lexical_builder_append_authorized() != 1 BEGIN SELECT RAISE(ABORT, 'private lexical builder mutation required'); END;
             CREATE TRIGGER builder_gate_clone_body_payloads_delete BEFORE DELETE ON clone_body_payloads WHEN tracedecay_lexical_builder_append_authorized() != 1 BEGIN SELECT RAISE(ABORT, 'private lexical builder mutation required'); END;
             CREATE TRIGGER builder_gate_clone_occurrences_insert BEFORE INSERT ON clone_occurrences WHEN tracedecay_lexical_builder_append_authorized() != 1 BEGIN SELECT RAISE(ABORT, 'private lexical builder mutation required'); END;
             CREATE TRIGGER builder_gate_clone_occurrences_update BEFORE UPDATE ON clone_occurrences WHEN tracedecay_lexical_builder_append_authorized() != 1 BEGIN SELECT RAISE(ABORT, 'private lexical builder mutation required'); END;
             CREATE TRIGGER builder_gate_clone_occurrences_delete BEFORE DELETE ON clone_occurrences WHEN tracedecay_lexical_builder_append_authorized() != 1 BEGIN SELECT RAISE(ABORT, 'private lexical builder mutation required'); END;
             CREATE TRIGGER builder_gate_clone_exact_postings_insert BEFORE INSERT ON clone_exact_postings WHEN tracedecay_lexical_builder_append_authorized() != 1 BEGIN SELECT RAISE(ABORT, 'private lexical builder mutation required'); END;
             CREATE TRIGGER builder_gate_clone_exact_postings_update BEFORE UPDATE ON clone_exact_postings WHEN tracedecay_lexical_builder_append_authorized() != 1 BEGIN SELECT RAISE(ABORT, 'private lexical builder mutation required'); END;
             CREATE TRIGGER builder_gate_clone_exact_postings_delete BEFORE DELETE ON clone_exact_postings WHEN tracedecay_lexical_builder_append_authorized() != 1 BEGIN SELECT RAISE(ABORT, 'private lexical builder mutation required'); END;
             CREATE TRIGGER builder_gate_clone_fingerprint_postings_insert BEFORE INSERT ON clone_fingerprint_postings WHEN tracedecay_lexical_builder_append_authorized() != 1 BEGIN SELECT RAISE(ABORT, 'private lexical builder mutation required'); END;
             CREATE TRIGGER builder_gate_clone_fingerprint_postings_update BEFORE UPDATE ON clone_fingerprint_postings WHEN tracedecay_lexical_builder_append_authorized() != 1 BEGIN SELECT RAISE(ABORT, 'private lexical builder mutation required'); END;
             CREATE TRIGGER builder_gate_clone_fingerprint_postings_delete BEFORE DELETE ON clone_fingerprint_postings WHEN tracedecay_lexical_builder_append_authorized() != 1 BEGIN SELECT RAISE(ABORT, 'private lexical builder mutation required'); END;",
        )
        .map_err(sqlite_error)
}

fn table_exists(connection: &Connection, table: &str) -> Result<bool, CodeLexicalArtifactErrorV1> {
    connection
        .query_row(
            "SELECT EXISTS(SELECT 1 FROM sqlite_schema WHERE type = 'table' AND name = ?1)",
            [table],
            |row| row.get(0),
        )
        .map_err(sqlite_error)
}

fn append_clone_rows(
    transaction: &rusqlite::Transaction<'_>,
    page: &VerifiedSealedLexicalPageV1,
    control: &dyn CodeIndexExecutionControlV1,
) -> Result<CloneSuccessorReuseAccountingV1, CodeLexicalArtifactErrorV1> {
    let mut accounting = CloneSuccessorReuseAccountingV1::default();
    for body in page.clone_bodies() {
        checkpoint(control)?;
        let delta = reconcile_clone_body(transaction, body)?;
        accounting.absorb(delta);
    }
    Ok(accounting)
}

fn reconcile_clone_body(
    transaction: &rusqlite::Transaction<'_>,
    body: &CodeIndexCloneBodyV1,
) -> Result<CloneSuccessorReuseAccountingV1, CodeLexicalArtifactErrorV1> {
    let mut accounting = CloneSuccessorReuseAccountingV1::default();
    let payload = serde_json::to_vec(&body.payload)
        .map_err(|error| CodeLexicalArtifactErrorV1::Contract(error.to_string()))?;
    let inserted_payload = transaction
        .execute(
            "INSERT INTO clone_body_payloads(payload_digest, payload) VALUES (?1, ?2) ON CONFLICT(payload_digest) DO NOTHING",
            params![body.payload.payload_digest.as_str(), payload],
        )
        .map_err(sqlite_error)?;
    let stored: Vec<u8> = transaction
        .query_row(
            "SELECT payload FROM clone_body_payloads WHERE payload_digest = ?1",
            [body.payload.payload_digest.as_str()],
            |row| row.get(0),
        )
        .map_err(sqlite_error)?;
    if stored
        != serde_json::to_vec(&body.payload)
            .map_err(|error| CodeLexicalArtifactErrorV1::Contract(error.to_string()))?
    {
        return Err(CodeLexicalArtifactErrorV1::Corrupt(
            "clone successor payload digest collision".to_owned(),
        ));
    }
    if inserted_payload == 0 {
        accounting.payloads_reused = 1;
    } else {
        accounting.payloads_inserted = 1;
    }

    let occurrence_id = body.occurrence.symbol_occurrence_id.as_str();
    let stored_occurrence: Option<(String, String, i64, i64, Vec<u8>)> = transaction
        .query_row(
            "SELECT payload_digest, path, body_start, body_end, occurrence FROM clone_occurrences WHERE symbol_occurrence_id = ?1",
            [occurrence_id],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?, row.get(4)?)),
        )
        .optional()
        .map_err(sqlite_error)?;
    let body_start = i64::try_from(body.occurrence.body_span.start_byte)
        .map_err(|error| CodeLexicalArtifactErrorV1::Contract(error.to_string()))?;
    let body_end = i64::try_from(body.occurrence.body_span.end_byte)
        .map_err(|error| CodeLexicalArtifactErrorV1::Contract(error.to_string()))?;
    let occurrence = serde_json::to_vec(&body.occurrence)
        .map_err(|error| CodeLexicalArtifactErrorV1::Contract(error.to_string()))?;
    let mut exact_postings = body
        .payload
        .exact_keys(body.occurrence.eligibility)
        .into_iter()
        .map(|key| {
            (
                i64::from(key.class as u8),
                i64::from(key.normalization_revision),
                key.digest.as_str().to_owned(),
                body.occurrence.payload_digest.as_str().to_owned(),
            )
        })
        .collect::<Vec<_>>();
    exact_postings.sort();
    let fingerprint_postings = clone_fingerprint_rows(body)?;

    match stored_occurrence {
        None => {
            transaction
                .execute(
                    "INSERT INTO clone_occurrences(symbol_occurrence_id, payload_digest, path, body_start, body_end, occurrence) VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
                    params![
                        occurrence_id,
                        body.occurrence.payload_digest.as_str(),
                        body.occurrence.path,
                        body_start,
                        body_end,
                        occurrence,
                    ],
                )
                .map_err(sqlite_error)?;
            accounting.occurrences_inserted = 1;
            insert_exact_postings(transaction, body, &exact_postings, &mut accounting)?;
            insert_fingerprint_postings(
                transaction,
                occurrence_id,
                &fingerprint_postings,
                &mut accounting,
            )?;
        }
        Some((
            stored_payload_digest,
            stored_path,
            stored_body_start,
            stored_body_end,
            stored_occurrence,
        )) => {
            let occurrence_reused = stored_payload_digest
                == body.occurrence.payload_digest.as_str()
                && stored_path == body.occurrence.path
                && stored_body_start == body_start
                && stored_body_end == body_end;
            if occurrence_reused {
                accounting.occurrences_reused = 1;
            } else {
                accounting.occurrences_replaced = 1;
            }
            if stored_occurrence != occurrence
                || stored_payload_digest != body.occurrence.payload_digest.as_str()
                || stored_path != body.occurrence.path
                || stored_body_start != body_start
                || stored_body_end != body_end
            {
                transaction
                    .execute(
                        "UPDATE clone_occurrences SET payload_digest = ?1, path = ?2, body_start = ?3, body_end = ?4, occurrence = ?5 WHERE symbol_occurrence_id = ?6",
                        params![
                            body.occurrence.payload_digest.as_str(),
                            body.occurrence.path,
                            body_start,
                            body_end,
                            occurrence,
                            occurrence_id,
                        ],
                    )
                    .map_err(sqlite_error)?;
            }

            let stored_exact = read_exact_postings(transaction, occurrence_id)?;
            if stored_exact == exact_postings {
                accounting.exact_postings_reused =
                    u64::try_from(exact_postings.len()).unwrap_or(u64::MAX);
            } else {
                delete_exact_postings(transaction, occurrence_id, &mut accounting)?;
                insert_exact_postings(transaction, body, &exact_postings, &mut accounting)?;
            }

            let stored_fingerprint = read_fingerprint_postings(transaction, occurrence_id)?;
            if stored_fingerprint == fingerprint_postings {
                accounting.fingerprint_postings_reused =
                    u64::try_from(fingerprint_postings.len()).unwrap_or(u64::MAX);
            } else {
                delete_fingerprint_postings(transaction, occurrence_id, &mut accounting)?;
                insert_fingerprint_postings(
                    transaction,
                    occurrence_id,
                    &fingerprint_postings,
                    &mut accounting,
                )?;
            }
        }
    }
    transaction
        .execute(
            "INSERT INTO clone_successor_seen(symbol_occurrence_id) VALUES (?1) ON CONFLICT(symbol_occurrence_id) DO NOTHING",
            [occurrence_id],
        )
        .map_err(sqlite_error)?;
    Ok(accounting)
}

type CloneFingerprintPostingV1 = (String, i64, i64, i64, String, i64, String, String);

fn clone_fingerprint_rows(
    body: &CodeIndexCloneBodyV1,
) -> Result<Vec<CloneFingerprintPostingV1>, CodeLexicalArtifactErrorV1> {
    let Some(stream) = body.payload.fingerprint_stream(body.occurrence.eligibility) else {
        return Ok(Vec::new());
    };
    let mut rows = body
        .payload
        .fingerprint_positions(body.occurrence.eligibility)
        .map_err(CodeLexicalArtifactErrorV1::Contract)
        .and_then(|positions| {
            positions
                .into_iter()
                .map(|position| {
                    Ok((
                        body.payload.language.clone(),
                        i64::from(stream.class as u8),
                        i64::from(stream.normalization_revision),
                        i64::try_from(position.fingerprint).map_err(|error| {
                            CodeLexicalArtifactErrorV1::Contract(error.to_string())
                        })?,
                        body.occurrence.symbol_occurrence_id.as_str().to_owned(),
                        i64::from(position.token_position),
                        body.occurrence.payload_digest.as_str().to_owned(),
                        body.payload.body_digest.as_str().to_owned(),
                    ))
                })
                .collect::<Result<Vec<_>, CodeLexicalArtifactErrorV1>>()
        })?;
    rows.sort();
    Ok(rows)
}

fn insert_exact_postings(
    transaction: &rusqlite::Transaction<'_>,
    body: &CodeIndexCloneBodyV1,
    postings: &[(i64, i64, String, String)],
    accounting: &mut CloneSuccessorReuseAccountingV1,
) -> Result<(), CodeLexicalArtifactErrorV1> {
    for (class, revision, digest, payload_digest) in postings {
        transaction
            .execute(
                "INSERT INTO clone_exact_postings(class, normalization_revision, digest, symbol_occurrence_id, payload_digest) VALUES (?1, ?2, ?3, ?4, ?5)",
                params![class, revision, digest, body.occurrence.symbol_occurrence_id.as_str(), payload_digest],
            )
            .map_err(sqlite_error)?;
    }
    accounting.exact_postings_inserted = accounting
        .exact_postings_inserted
        .saturating_add(u64::try_from(postings.len()).unwrap_or(u64::MAX));
    Ok(())
}

fn delete_exact_postings(
    transaction: &rusqlite::Transaction<'_>,
    occurrence_id: &str,
    accounting: &mut CloneSuccessorReuseAccountingV1,
) -> Result<(), CodeLexicalArtifactErrorV1> {
    let deleted = transaction
        .execute(
            "DELETE FROM clone_exact_postings WHERE symbol_occurrence_id = ?1",
            [occurrence_id],
        )
        .map_err(sqlite_error)?;
    accounting.exact_postings_deleted = accounting
        .exact_postings_deleted
        .saturating_add(u64::try_from(deleted).unwrap_or(u64::MAX));
    Ok(())
}

fn read_exact_postings(
    transaction: &rusqlite::Transaction<'_>,
    occurrence_id: &str,
) -> Result<Vec<(i64, i64, String, String)>, CodeLexicalArtifactErrorV1> {
    let mut statement = transaction
        .prepare(
            "SELECT class, normalization_revision, digest, payload_digest FROM clone_exact_postings WHERE symbol_occurrence_id = ?1 ORDER BY class, normalization_revision, digest",
        )
        .map_err(sqlite_error)?;
    statement
        .query_map([occurrence_id], |row| {
            Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?))
        })
        .map_err(sqlite_error)?
        .collect::<Result<Vec<_>, _>>()
        .map_err(sqlite_error)
}

fn insert_fingerprint_postings(
    transaction: &rusqlite::Transaction<'_>,
    occurrence_id: &str,
    postings: &[CloneFingerprintPostingV1],
    accounting: &mut CloneSuccessorReuseAccountingV1,
) -> Result<(), CodeLexicalArtifactErrorV1> {
    for (
        language,
        class,
        revision,
        fingerprint,
        stored_occurrence_id,
        token_position,
        payload_digest,
        body_digest,
    ) in postings
    {
        debug_assert_eq!(stored_occurrence_id, occurrence_id);
        transaction
            .execute(
                "INSERT INTO clone_fingerprint_postings(language, class, normalization_revision, fingerprint, symbol_occurrence_id, token_position, payload_digest, body_digest) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
                params![
                    language,
                    class,
                    revision,
                    fingerprint,
                    occurrence_id,
                    token_position,
                    payload_digest,
                    body_digest,
                ],
            )
            .map_err(sqlite_error)?;
    }
    accounting.fingerprint_postings_inserted = accounting
        .fingerprint_postings_inserted
        .saturating_add(u64::try_from(postings.len()).unwrap_or(u64::MAX));
    Ok(())
}

fn read_fingerprint_postings(
    transaction: &rusqlite::Transaction<'_>,
    occurrence_id: &str,
) -> Result<Vec<CloneFingerprintPostingV1>, CodeLexicalArtifactErrorV1> {
    let mut statement = transaction
        .prepare(
            "SELECT language, class, normalization_revision, fingerprint, symbol_occurrence_id, token_position, payload_digest, body_digest FROM clone_fingerprint_postings WHERE symbol_occurrence_id = ?1 ORDER BY language, class, normalization_revision, fingerprint, token_position",
        )
        .map_err(sqlite_error)?;
    statement
        .query_map([occurrence_id], |row| {
            Ok((
                row.get(0)?,
                row.get(1)?,
                row.get(2)?,
                row.get(3)?,
                row.get(4)?,
                row.get(5)?,
                row.get(6)?,
                row.get(7)?,
            ))
        })
        .map_err(sqlite_error)?
        .collect::<Result<Vec<_>, _>>()
        .map_err(sqlite_error)
}

fn delete_fingerprint_postings(
    transaction: &rusqlite::Transaction<'_>,
    occurrence_id: &str,
    accounting: &mut CloneSuccessorReuseAccountingV1,
) -> Result<(), CodeLexicalArtifactErrorV1> {
    let deleted = transaction
        .execute(
            "DELETE FROM clone_fingerprint_postings WHERE symbol_occurrence_id = ?1",
            [occurrence_id],
        )
        .map_err(sqlite_error)?;
    accounting.fingerprint_postings_deleted = accounting
        .fingerprint_postings_deleted
        .saturating_add(u64::try_from(deleted).unwrap_or(u64::MAX));
    Ok(())
}

fn prune_stale_clone_rows(
    transaction: &rusqlite::Transaction<'_>,
    accounting: &mut CloneSuccessorReuseAccountingV1,
) -> Result<(), CodeLexicalArtifactErrorV1> {
    let deleted_exact = transaction
        .execute(
            "DELETE FROM clone_exact_postings
             WHERE NOT EXISTS (
                 SELECT 1 FROM clone_successor_seen AS seen
                 WHERE seen.symbol_occurrence_id = clone_exact_postings.symbol_occurrence_id
             )",
            [],
        )
        .map_err(sqlite_error)?;
    accounting.exact_postings_deleted = accounting
        .exact_postings_deleted
        .saturating_add(u64::try_from(deleted_exact).unwrap_or(u64::MAX));

    let deleted_fingerprints = transaction
        .execute(
            "DELETE FROM clone_fingerprint_postings
             WHERE NOT EXISTS (
                 SELECT 1 FROM clone_successor_seen AS seen
                 WHERE seen.symbol_occurrence_id = clone_fingerprint_postings.symbol_occurrence_id
             )",
            [],
        )
        .map_err(sqlite_error)?;
    accounting.fingerprint_postings_deleted = accounting
        .fingerprint_postings_deleted
        .saturating_add(u64::try_from(deleted_fingerprints).unwrap_or(u64::MAX));

    let deleted_occurrences = transaction
        .execute(
            "DELETE FROM clone_occurrences
             WHERE NOT EXISTS (
                 SELECT 1 FROM clone_successor_seen AS seen
                 WHERE seen.symbol_occurrence_id = clone_occurrences.symbol_occurrence_id
             )",
            [],
        )
        .map_err(sqlite_error)?;
    accounting.occurrences_deleted = accounting
        .occurrences_deleted
        .saturating_add(u64::try_from(deleted_occurrences).unwrap_or(u64::MAX));

    let deleted_payloads = transaction
        .execute(
            "DELETE FROM clone_body_payloads
             WHERE NOT EXISTS (
                 SELECT 1 FROM clone_occurrences AS occurrence
                 WHERE occurrence.payload_digest = clone_body_payloads.payload_digest
             )",
            [],
        )
        .map_err(sqlite_error)?;
    accounting.payloads_deleted = accounting
        .payloads_deleted
        .saturating_add(u64::try_from(deleted_payloads).unwrap_or(u64::MAX));
    Ok(())
}

fn install_clone_immutability(
    transaction: &rusqlite::Transaction<'_>,
) -> Result<(), CodeLexicalArtifactErrorV1> {
    transaction
        .execute_batch(
            "DROP TRIGGER IF EXISTS builder_gate_clone_body_payloads_update;
             DROP TRIGGER IF EXISTS builder_gate_clone_body_payloads_delete;
             DROP TRIGGER IF EXISTS builder_gate_clone_occurrences_update;
             DROP TRIGGER IF EXISTS builder_gate_clone_occurrences_delete;
             DROP TRIGGER IF EXISTS builder_gate_clone_exact_postings_update;
             DROP TRIGGER IF EXISTS builder_gate_clone_exact_postings_delete;
             DROP TRIGGER IF EXISTS builder_gate_clone_fingerprint_postings_update;
             DROP TRIGGER IF EXISTS builder_gate_clone_fingerprint_postings_delete;
             CREATE TRIGGER immutable_clone_body_payloads_update BEFORE UPDATE ON clone_body_payloads BEGIN SELECT RAISE(ABORT, 'immutable clone body payloads'); END;
             CREATE TRIGGER immutable_clone_body_payloads_delete BEFORE DELETE ON clone_body_payloads BEGIN SELECT RAISE(ABORT, 'immutable clone body payloads'); END;
             CREATE TRIGGER immutable_clone_occurrences_update BEFORE UPDATE ON clone_occurrences BEGIN SELECT RAISE(ABORT, 'immutable clone occurrences'); END;
             CREATE TRIGGER immutable_clone_occurrences_delete BEFORE DELETE ON clone_occurrences BEGIN SELECT RAISE(ABORT, 'immutable clone occurrences'); END;
             CREATE TRIGGER immutable_clone_exact_postings_update BEFORE UPDATE ON clone_exact_postings BEGIN SELECT RAISE(ABORT, 'immutable clone exact postings'); END;
             CREATE TRIGGER immutable_clone_exact_postings_delete BEFORE DELETE ON clone_exact_postings BEGIN SELECT RAISE(ABORT, 'immutable clone exact postings'); END;
             CREATE TRIGGER immutable_clone_fingerprint_postings_update BEFORE UPDATE ON clone_fingerprint_postings BEGIN SELECT RAISE(ABORT, 'immutable clone fingerprint postings'); END;
             CREATE TRIGGER immutable_clone_fingerprint_postings_delete BEFORE DELETE ON clone_fingerprint_postings BEGIN SELECT RAISE(ABORT, 'immutable clone fingerprint postings'); END;
             CREATE TRIGGER immutable_clone_fingerprint_counts_update BEFORE UPDATE ON clone_fingerprint_counts BEGIN SELECT RAISE(ABORT, 'immutable clone fingerprint counts'); END;
             CREATE TRIGGER immutable_clone_fingerprint_counts_delete BEFORE DELETE ON clone_fingerprint_counts BEGIN SELECT RAISE(ABORT, 'immutable clone fingerprint counts'); END;",
        )
        .map_err(sqlite_error)
}

fn verify_copied_source_page(
    connection: &Connection,
    page: &VerifiedSealedLexicalPageV1,
) -> Result<(), CodeLexicalArtifactErrorV1> {
    let stored: Option<(String, String, Vec<u8>)> = connection
        .query_row(
            "SELECT page_digest, cumulative_digest, next_cursor FROM source_pages WHERE page_ordinal = ?1",
            [i64::try_from(page.page_ordinal())
                .map_err(|error| CodeLexicalArtifactErrorV1::Contract(error.to_string()))?],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .optional()
        .map_err(sqlite_error)?;
    let next_cursor = page
        .next_cursor()
        .persisted_bytes()
        .map_err(|error| CodeLexicalArtifactErrorV1::Contract(error.to_string()))?;
    if stored
        != Some((
            page.page_digest().as_str().to_owned(),
            page.cumulative_digest().as_str().to_owned(),
            next_cursor,
        ))
    {
        return Err(CodeLexicalArtifactErrorV1::Corrupt(
            "clone successor page does not match the copied lexical source receipt".to_owned(),
        ));
    }
    Ok(())
}

fn verify_clone_page_rows(
    connection: &Connection,
    page: &VerifiedSealedLexicalPageV1,
    control: &dyn CodeIndexExecutionControlV1,
) -> Result<(), CodeLexicalArtifactErrorV1> {
    for body in page.clone_bodies() {
        checkpoint(control)?;
        let expected_payload = serde_json::to_vec(&body.payload)
            .map_err(|error| CodeLexicalArtifactErrorV1::Contract(error.to_string()))?;
        let stored_payload: Option<Vec<u8>> = connection
            .query_row(
                "SELECT payload FROM clone_body_payloads WHERE payload_digest = ?1",
                [body.payload.payload_digest.as_str()],
                |row| row.get(0),
            )
            .optional()
            .map_err(sqlite_error)?;
        if stored_payload.as_deref() != Some(expected_payload.as_slice()) {
            return Err(CodeLexicalArtifactErrorV1::Corrupt(
                "resumed clone payload differs from its sealed source page".to_owned(),
            ));
        }

        let expected_occurrence = serde_json::to_vec(&body.occurrence)
            .map_err(|error| CodeLexicalArtifactErrorV1::Contract(error.to_string()))?;
        let stored_occurrence: Option<(String, String, i64, i64, Vec<u8>)> = connection
            .query_row(
                "SELECT payload_digest, path, body_start, body_end, occurrence FROM clone_occurrences WHERE symbol_occurrence_id = ?1",
                [body.occurrence.symbol_occurrence_id.as_str()],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?, row.get(4)?)),
            )
            .optional()
            .map_err(sqlite_error)?;
        let expected_span = (
            i64::try_from(body.occurrence.body_span.start_byte)
                .map_err(|error| CodeLexicalArtifactErrorV1::Contract(error.to_string()))?,
            i64::try_from(body.occurrence.body_span.end_byte)
                .map_err(|error| CodeLexicalArtifactErrorV1::Contract(error.to_string()))?,
        );
        if stored_occurrence.as_ref().map(|stored| {
            (
                stored.0.as_str(),
                stored.1.as_str(),
                stored.2,
                stored.3,
                stored.4.as_slice(),
            )
        }) != Some((
            body.occurrence.payload_digest.as_str(),
            body.occurrence.path.as_str(),
            expected_span.0,
            expected_span.1,
            expected_occurrence.as_slice(),
        )) {
            return Err(CodeLexicalArtifactErrorV1::Corrupt(
                "resumed clone occurrence differs from its sealed source page".to_owned(),
            ));
        }

        let mut expected_postings = body
            .payload
            .exact_keys(body.occurrence.eligibility)
            .into_iter()
            .map(|key| {
                (
                    i64::from(key.class as u8),
                    i64::from(key.normalization_revision),
                    key.digest.as_str().to_owned(),
                    body.occurrence.payload_digest.as_str().to_owned(),
                )
            })
            .collect::<Vec<_>>();
        expected_postings.sort();
        let mut statement = connection
            .prepare(
                "SELECT class, normalization_revision, digest, payload_digest FROM clone_exact_postings WHERE symbol_occurrence_id = ?1 ORDER BY class, normalization_revision, digest",
            )
            .map_err(sqlite_error)?;
        let stored_postings = statement
            .query_map([body.occurrence.symbol_occurrence_id.as_str()], |row| {
                Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?))
            })
            .map_err(sqlite_error)?
            .collect::<Result<Vec<(i64, i64, String, String)>, _>>()
            .map_err(sqlite_error)?;
        if stored_postings != expected_postings {
            return Err(CodeLexicalArtifactErrorV1::Corrupt(
                "resumed clone postings differ from their sealed source page".to_owned(),
            ));
        }
        verify_clone_fingerprint_page_rows(connection, body)?;
    }
    Ok(())
}

type CloneFingerprintRowV1 = (String, i64, i64, i64, i64, String, String);

fn verify_clone_fingerprint_page_rows(
    connection: &Connection,
    body: &CodeIndexCloneBodyV1,
) -> Result<(), CodeLexicalArtifactErrorV1> {
    let mut expected = Vec::new();
    if let Some(stream) = body.payload.fingerprint_stream(body.occurrence.eligibility) {
        for position in body
            .payload
            .fingerprint_positions(body.occurrence.eligibility)
            .map_err(CodeLexicalArtifactErrorV1::Contract)?
        {
            expected.push((
                body.payload.language.clone(),
                i64::from(stream.class as u8),
                i64::from(stream.normalization_revision),
                i64::try_from(position.fingerprint)
                    .map_err(|error| CodeLexicalArtifactErrorV1::Contract(error.to_string()))?,
                i64::from(position.token_position),
                body.occurrence.payload_digest.as_str().to_owned(),
                body.payload.body_digest.as_str().to_owned(),
            ));
        }
    }
    expected.sort();
    let mut statement = connection
        .prepare(
            "SELECT language, class, normalization_revision, fingerprint, token_position, payload_digest, body_digest FROM clone_fingerprint_postings WHERE symbol_occurrence_id = ?1 ORDER BY language, class, normalization_revision, fingerprint, token_position",
        )
        .map_err(sqlite_error)?;
    let stored = statement
        .query_map([body.occurrence.symbol_occurrence_id.as_str()], |row| {
            Ok((
                row.get(0)?,
                row.get(1)?,
                row.get(2)?,
                row.get(3)?,
                row.get(4)?,
                row.get(5)?,
                row.get(6)?,
            ))
        })
        .map_err(sqlite_error)?
        .collect::<Result<Vec<CloneFingerprintRowV1>, _>>()
        .map_err(sqlite_error)?;
    if stored != expected {
        return Err(CodeLexicalArtifactErrorV1::Corrupt(
            "resumed clone fingerprints differ from their sealed source page".to_owned(),
        ));
    }
    Ok(())
}

fn verify_source_receipt(
    prior: &VerifiedCodeLexicalArtifactV1,
    source: &VerifiedSealedLexicalSourceReceiptV1,
) -> Result<(), CodeLexicalArtifactErrorV1> {
    if prior.source_state_digest() != source.source_state_digest()
        || prior.source_cumulative_digest() != source.cumulative_digest()
        || prior.page_count() != source.page_count()
        || prior.total_chunks() != source.total_chunks()
        || prior.total_payload_bytes() != source.total_payload_bytes()
        || prior.total_imports() != source.total_imports()
        || prior.import_payload_bytes() != source.import_payload_bytes()
        || prior.import_dictionary_digest() != source.import_dictionary_digest()
    {
        return Err(CodeLexicalArtifactErrorV1::Corrupt(
            "clone successor source receipt differs from its lexical predecessor".to_owned(),
        ));
    }
    Ok(())
}

fn derive_clone_fingerprint_counts(
    transaction: &rusqlite::Transaction<'_>,
) -> Result<(), CodeLexicalArtifactErrorV1> {
    transaction
        .execute_batch(
            "INSERT INTO clone_fingerprint_counts(language, class, normalization_revision, fingerprint, posting_count)
             SELECT language, class, normalization_revision, fingerprint, COUNT(*)
             FROM clone_fingerprint_postings
             GROUP BY language, class, normalization_revision, fingerprint;",
        )
        .map_err(sqlite_error)
}

#[cfg(test)]
mod tests {
    use std::fmt;

    use tracedecay_code_extraction::{
        CloneBodyEligibilityV1, CloneBodyRenameStatusV1, CloneBodyTokenizationStatusV1,
        ConservativeCloneTokenV1, ExtractedCloneBodyV1,
    };
    use tracedecay_domain::{
        CodeGenerationId, ManifestDigest, NodeKind, ProjectId, RepositoryId, SourceSpan,
        SymbolOccurrenceId, canonical_sha256,
    };

    use super::*;
    use tracedecay_code_index::clones::{CloneBodyOccurrenceV1, CloneBodyPayloadV1};

    fn id<T>(value: impl Into<String>) -> T
    where
        T: TryFrom<String>,
        <T as TryFrom<String>>::Error: fmt::Debug,
    {
        T::try_from(value.into()).expect("valid clone successor fixture identity")
    }

    fn digest(value: &str) -> ManifestDigest {
        canonical_sha256(&value).expect("valid clone successor fixture digest")
    }

    fn payload(prefix: &str) -> Arc<CloneBodyPayloadV1> {
        let tokens = (0..30)
            .map(|ordinal| ConservativeCloneTokenV1::Syntax {
                syntax_kind: "identifier".to_owned(),
                text: format!("{prefix}{ordinal}"),
            })
            .collect::<Vec<_>>();
        let extracted = ExtractedCloneBodyV1 {
            logical_path: format!("src/{prefix}.rs"),
            language: "rust".to_owned(),
            symbol_kind: NodeKind::Function,
            symbol_occurrence_id: format!("symbol.{prefix}"),
            body_span: SourceSpan {
                start_byte: 0,
                end_byte: 200,
            },
            normalization_revision: 1,
            non_trivia_token_count: 30,
            eligibility: CloneBodyEligibilityV1::Eligible,
            tokenization_status: CloneBodyTokenizationStatusV1::Complete,
            tokenization_issues: Vec::new(),
            conservative_tokens: tokens,
            rename_normalization_revision: None,
            rename_status: CloneBodyRenameStatusV1::UnsupportedLanguage,
            rename_issues: Vec::new(),
            rename_tokens: None,
        };
        Arc::new(CloneBodyPayloadV1::from_extracted(&extracted).expect("valid clone payload"))
    }

    fn body(
        symbol: &str,
        payload: Arc<CloneBodyPayloadV1>,
        generation: &str,
        snapshot: &str,
        start_byte: u64,
    ) -> CodeIndexCloneBodyV1 {
        CodeIndexCloneBodyV1 {
            payload: Arc::clone(&payload),
            occurrence: CloneBodyOccurrenceV1 {
                project_id: id::<ProjectId>("project.clone-successor"),
                repository_id: id::<RepositoryId>("repository.clone-successor"),
                worktree_id: None,
                source_generation: id::<CodeGenerationId>(generation),
                snapshot_digest: digest(snapshot),
                symbol_occurrence_id: id::<SymbolOccurrenceId>(symbol),
                path: format!("src/{symbol}.rs"),
                body_span: SourceSpan {
                    start_byte,
                    end_byte: start_byte.saturating_add(200),
                },
                payload_digest: payload.payload_digest.clone(),
                eligibility: CloneBodyEligibilityV1::Eligible,
            },
        }
    }

    fn mutable_connection() -> (Connection, Arc<std::sync::atomic::AtomicU8>) {
        let connection = Connection::open_in_memory().expect("clone successor database");
        let gate = register_builder_mutation_gate(&connection).expect("builder mutation gate");
        prepare_clone_tables(&connection).expect("clone successor tables");
        connection
            .execute_batch(
                "CREATE TABLE clone_successor_seen (
                    symbol_occurrence_id TEXT PRIMARY KEY
                 ) WITHOUT ROWID;",
            )
            .expect("clone successor seen table");
        (connection, gate)
    }

    fn reconcile(
        connection: &mut Connection,
        gate: &Arc<std::sync::atomic::AtomicU8>,
        bodies: &[CodeIndexCloneBodyV1],
    ) -> CloneSuccessorReuseAccountingV1 {
        let _authority = BuilderMutationGuardV1::enter(gate).expect("builder mutation authority");
        let transaction = connection
            .transaction()
            .expect("clone successor transaction");
        let mut accounting = CloneSuccessorReuseAccountingV1::default();
        for body in bodies {
            accounting.absorb(reconcile_clone_body(&transaction, body).expect("clone body"));
        }
        transaction.commit().expect("clone successor commit");
        accounting
    }

    fn clear_seen(connection: &mut Connection, gate: &Arc<std::sync::atomic::AtomicU8>) {
        let _authority = BuilderMutationGuardV1::enter(gate).expect("builder mutation authority");
        connection
            .execute("DELETE FROM clone_successor_seen", [])
            .expect("clear seen rows");
    }

    #[test]
    fn no_op_successor_reuses_occurrence_and_all_postings() {
        let (mut connection, gate) = mutable_connection();
        let prior = body(
            "symbol.no-op",
            payload("no-op"),
            "generation.clone-successor.v1",
            "snapshot.clone-successor.v1",
            0,
        );
        let _ = reconcile(&mut connection, &gate, std::slice::from_ref(&prior));
        clear_seen(&mut connection, &gate);

        let current = body(
            "symbol.no-op",
            Arc::clone(&prior.payload),
            "generation.clone-successor.v2",
            "snapshot.clone-successor.v2",
            0,
        );
        let accounting = reconcile(&mut connection, &gate, std::slice::from_ref(&current));

        assert_eq!(accounting.occurrences_reused, 1);
        assert_eq!(accounting.occurrences_replaced, 0);
        assert_eq!(accounting.exact_postings_reused, 1);
        assert_eq!(accounting.exact_postings_inserted, 0);
        assert_eq!(accounting.exact_postings_deleted, 0);
        assert!(accounting.fingerprint_postings_reused > 0);
        assert_eq!(accounting.fingerprint_postings_inserted, 0);
        assert_eq!(accounting.fingerprint_postings_deleted, 0);
        assert_eq!(accounting.payloads_reused, 1);
        assert_eq!(accounting.payloads_inserted, 0);
        assert_eq!(accounting.payloads_deleted, 0);
    }

    #[test]
    fn one_symbol_change_replaces_only_changed_postings() {
        let (mut connection, gate) = mutable_connection();
        let prior_a = body(
            "symbol.a",
            payload("a-prior"),
            "generation.clone-successor.v1",
            "snapshot.clone-successor.v1",
            0,
        );
        let prior_b = body(
            "symbol.b",
            payload("b-prior"),
            "generation.clone-successor.v1",
            "snapshot.clone-successor.v1",
            300,
        );
        let _ = reconcile(&mut connection, &gate, &[prior_a.clone(), prior_b.clone()]);
        clear_seen(&mut connection, &gate);

        let current_a = body(
            "symbol.a",
            payload("a-current"),
            "generation.clone-successor.v2",
            "snapshot.clone-successor.v2",
            0,
        );
        let current_b = body(
            "symbol.b",
            Arc::clone(&prior_b.payload),
            "generation.clone-successor.v2",
            "snapshot.clone-successor.v2",
            300,
        );
        let accounting = reconcile(&mut connection, &gate, &[current_a, current_b]);

        assert_eq!(accounting.occurrences_reused, 1);
        assert_eq!(accounting.occurrences_replaced, 1);
        assert_eq!(accounting.occurrences_inserted, 0);
        assert_eq!(accounting.exact_postings_reused, 1);
        assert_eq!(accounting.exact_postings_inserted, 1);
        assert_eq!(accounting.exact_postings_deleted, 1);
        assert!(accounting.fingerprint_postings_reused > 0);
        assert!(accounting.fingerprint_postings_inserted > 0);
        assert!(accounting.fingerprint_postings_deleted > 0);
        assert_eq!(accounting.payloads_reused, 1);
        assert_eq!(accounting.payloads_inserted, 1);
        assert_eq!(accounting.payloads_deleted, 0);
    }

    #[test]
    fn deletion_prunes_stale_occurrence_postings_and_payload() {
        let (mut connection, gate) = mutable_connection();
        let prior_a = body(
            "symbol.a",
            payload("a-delete"),
            "generation.clone-successor.v1",
            "snapshot.clone-successor.v1",
            0,
        );
        let prior_b = body(
            "symbol.b",
            payload("b-delete"),
            "generation.clone-successor.v1",
            "snapshot.clone-successor.v1",
            300,
        );
        let _ = reconcile(&mut connection, &gate, &[prior_a.clone(), prior_b.clone()]);
        clear_seen(&mut connection, &gate);

        let current_a = body(
            "symbol.a",
            Arc::clone(&prior_a.payload),
            "generation.clone-successor.v2",
            "snapshot.clone-successor.v2",
            0,
        );
        let mut accounting = reconcile(&mut connection, &gate, std::slice::from_ref(&current_a));
        {
            let _authority =
                BuilderMutationGuardV1::enter(&gate).expect("builder mutation authority");
            let transaction = connection.transaction().expect("prune transaction");
            prune_stale_clone_rows(&transaction, &mut accounting).expect("prune stale rows");
            transaction.commit().expect("prune commit");
        }

        assert_eq!(accounting.occurrences_deleted, 1);
        assert_eq!(accounting.exact_postings_deleted, 1);
        assert!(accounting.fingerprint_postings_deleted > 0);
        assert_eq!(accounting.payloads_deleted, 1);
        let counts: (i64, i64, i64, i64) = connection
            .query_row(
                "SELECT
                    (SELECT COUNT(*) FROM clone_occurrences),
                    (SELECT COUNT(*) FROM clone_exact_postings),
                    (SELECT COUNT(*) FROM clone_fingerprint_postings),
                    (SELECT COUNT(*) FROM clone_body_payloads)",
                [],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
            )
            .expect("remaining clone rows");
        assert_eq!(counts.0, 1);
        assert_eq!(counts.1, 1);
        assert!(counts.2 > 0);
        assert_eq!(counts.3, 1);
    }
}
