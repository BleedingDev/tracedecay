//! The SQLite-backed observation journal.
//!
//! One file, one writer transaction at a time, `synchronous = FULL`. Delivery
//! state, attempt history, and both replay positions are rows, so process death
//! loses nothing and recovery needs no coordinator.

mod append;
mod dispatch;
mod retention;
pub(crate) mod row;
mod schema;

use std::path::Path;
use std::sync::Mutex;
use std::time::Duration;

use rusqlite::{Connection, OpenFlags, Transaction, TransactionBehavior};

use crate::error::ObservationJournalError;
use crate::identity::{SourceSequenceV1, SourceStreamIdV1};
use crate::retention::RetentionPolicyV1;
use crate::settlement::SourceAuthorityV1;

pub use schema::SCHEMA_VERSION;

/// How long a write-ahead-log truncation waits for concurrent readers before
/// reporting the log busy.
const CHECKPOINT_BUSY_TIMEOUT_MILLIS: u64 = 250;

/// Durable bounded observation journal, delivery authority, and replay position.
///
/// This store is deliberately *not* a content store. There is no full-text
/// index, no payload predicate, and no "read the last observation of kind X"
/// helper. Adding one would quietly turn the outbox into a second authority for
/// Native facts, which ADR-0005 forbids.
#[derive(Debug)]
pub struct SqliteObservationJournal {
    connection: Mutex<Connection>,
    policy: RetentionPolicyV1,
}

impl SqliteObservationJournal {
    /// Opens or creates a file-backed journal.
    pub fn open(
        path: impl AsRef<Path>,
        policy: RetentionPolicyV1,
    ) -> Result<Self, ObservationJournalError> {
        policy.validate()?;
        let mut connection = Connection::open_with_flags(
            path,
            OpenFlags::SQLITE_OPEN_READ_WRITE
                | OpenFlags::SQLITE_OPEN_CREATE
                | OpenFlags::SQLITE_OPEN_NO_MUTEX
                | OpenFlags::SQLITE_OPEN_URI,
        )?;
        schema::initialize(&mut connection)?;
        Ok(Self {
            connection: Mutex::new(connection),
            policy,
        })
    }

    /// Opens an in-memory journal. Useful for pure-logic tests; restart
    /// survival must always be proven against a real file.
    pub fn open_in_memory(policy: RetentionPolicyV1) -> Result<Self, ObservationJournalError> {
        policy.validate()?;
        let mut connection = Connection::open_in_memory()?;
        schema::initialize(&mut connection)?;
        Ok(Self {
            connection: Mutex::new(connection),
            policy,
        })
    }

    /// Returns the bounds this store enforces.
    #[must_use]
    pub const fn policy(&self) -> &RetentionPolicyV1 {
        &self.policy
    }

    /// Returns the furthest durably processed sequence across every exact scope
    /// of one canonical source stream.
    ///
    /// Ingress processes the canonical stream strictly in sequence and stops on
    /// the first refusal. The maximum per-scope cursor is therefore the global
    /// restart watermark even when successive records belong to different agent
    /// sessions. A crash before one record commits leaves this value before that
    /// record, so replay may repeat work but cannot skip it.
    pub fn maximum_replay_sequence(
        &self,
        source_authority: SourceAuthorityV1,
        source_stream: &SourceStreamIdV1,
    ) -> Result<Option<SourceSequenceV1>, ObservationJournalError> {
        self.with_connection(|connection| {
            let sequence = connection.query_row(
                "SELECT MAX(last_admitted_sequence) \
                 FROM tdmem_observation_replay_cursor_v1 \
                 WHERE source_authority = ?1 AND source_stream = ?2",
                rusqlite::params![source_authority.as_wire(), source_stream.as_str()],
                |row| row.get::<_, Option<i64>>(0),
            )?;
            sequence
                .map(|value| {
                    u64::try_from(value)
                        .map(SourceSequenceV1)
                        .map_err(|_| ObservationJournalError::ValueOutOfRange {
                            field: "last_admitted_sequence",
                        })
                })
                .transpose()
        })
    }

    pub(crate) fn with_transaction<T, F>(&self, action: F) -> Result<T, ObservationJournalError>
    where
        F: FnOnce(&Transaction<'_>) -> Result<T, ObservationJournalError>,
    {
        let mut guard = self
            .connection
            .lock()
            .map_err(|_| ObservationJournalError::LockPoisoned)?;
        let transaction = guard.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let value = action(&transaction)?;
        transaction.commit()?;
        Ok(value)
    }

    pub(crate) fn with_connection<T, F>(&self, action: F) -> Result<T, ObservationJournalError>
    where
        F: FnOnce(&Connection) -> Result<T, ObservationJournalError>,
    {
        let guard = self
            .connection
            .lock()
            .map_err(|_| ObservationJournalError::LockPoisoned)?;
        action(&guard)
    }

    /// Checkpoints and truncates the write-ahead log, reporting whether it
    /// actually happened.
    ///
    /// `secure_delete` zeroes freed pages *in the database file*. It says
    /// nothing about the `-wal` sidecar, which still holds the pre-purge page
    /// images until a checkpoint copies the purged pages back and truncates the
    /// log. So a privacy deletion is not complete on disk until this returns
    /// `true`, and a concurrent reader that keeps the log open makes it return
    /// `false` rather than letting a caller claim a deletion that has not
    /// landed.
    pub(crate) fn checkpoint_truncate(&self) -> Result<bool, ObservationJournalError> {
        self.with_connection(|connection| {
            // A privacy operation must not stall for the full writer timeout
            // behind a long-lived reader: bound the wait and report honestly.
            connection.busy_timeout(Duration::from_millis(CHECKPOINT_BUSY_TIMEOUT_MILLIS))?;
            let busy = connection.query_row("PRAGMA wal_checkpoint(TRUNCATE)", [], |row| {
                row.get::<_, i64>(0)
            });
            connection.busy_timeout(Duration::from_millis(schema::BUSY_TIMEOUT_MILLIS))?;
            Ok(busy? == 0)
        })
    }
}
