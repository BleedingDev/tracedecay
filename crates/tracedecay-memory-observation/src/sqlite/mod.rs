//! The SQLite-backed observation journal.
//!
//! One file, one writer transaction at a time, `synchronous = FULL`. Delivery
//! state, attempt history, both replay positions, and the restart-recovery
//! record are rows, so process death loses nothing and recovery needs no
//! coordinator.

mod append;
mod dispatch;
mod recovery;
mod retention;
pub(crate) mod row;
mod schema;

use std::path::Path;
use std::sync::{Mutex, MutexGuard, TryLockError};
use std::time::{Duration, Instant};

use rusqlite::{Connection, ErrorCode, OpenFlags, Transaction, TransactionBehavior};

use crate::error::ObservationJournalError;
use crate::identity::{SourceSequenceV1, SourceStreamIdV1};
use crate::recovery::RecoveryTimeBudgetV1;
use crate::retention::RetentionPolicyV1;
use crate::settlement::SourceAuthorityV1;

pub use row::WithheldAuditProgressV1;
pub use schema::{OPEN_WITHHELD_AUDIT_ROWS, SCHEMA_VERSION};

/// How long a write-ahead-log truncation waits for concurrent readers before
/// reporting the log busy.
const CHECKPOINT_BUSY_TIMEOUT_MILLIS: u64 = 250;

/// How long a bounded caller sleeps between attempts on the journal mutex.
/// Short enough that a small budget is honoured, long enough that waiting is
/// not a spin.
const LOCK_POLL_MICROS: u64 = 200;

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
    /// Where the resumable withheld-receipt audit has reached.
    ///
    /// Open validates one bounded page and leaves the rest here, so opening a
    /// store whose audit table has grown to millions of rows costs the same as
    /// opening an empty one. The remainder is the owner's to finish through
    /// [`Self::validate_withheld_backlog`].
    withheld_audit: Mutex<WithheldAuditStateV1>,
}

/// The resume position of the withheld-receipt audit for one open store.
#[derive(Debug)]
enum WithheldAuditStateV1 {
    /// More rows remain; the next page resumes strictly after this position.
    Pending(Option<row::WithheldAuditCursorV1>),
    /// Every row this store held at open has been revalidated.
    ///
    /// Rows appended after that point were built and validated in memory by
    /// this process on the way in, so re-reading them would prove nothing the
    /// write path did not already prove.
    Complete,
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
        let resume_after = schema::initialize(&mut connection)?;
        Ok(Self::mounted(connection, policy, resume_after))
    }

    /// Opens an in-memory journal. Useful for pure-logic tests; restart
    /// survival must always be proven against a real file.
    pub fn open_in_memory(policy: RetentionPolicyV1) -> Result<Self, ObservationJournalError> {
        policy.validate()?;
        let mut connection = Connection::open_in_memory()?;
        let resume_after = schema::initialize(&mut connection)?;
        Ok(Self::mounted(connection, policy, resume_after))
    }

    fn mounted(
        connection: Connection,
        policy: RetentionPolicyV1,
        resume_after: Option<row::WithheldAuditCursorV1>,
    ) -> Self {
        Self {
            connection: Mutex::new(connection),
            policy,
            withheld_audit: Mutex::new(match resume_after {
                Some(cursor) => WithheldAuditStateV1::Pending(Some(cursor)),
                None => WithheldAuditStateV1::Complete,
            }),
        }
    }

    /// Revalidates the next bounded page of the withheld audit this store's
    /// open left behind, and reports whether the walk is finished.
    ///
    /// Open pays for one page so that project open cost does not grow with the
    /// audit table. This is the other half of that bargain: the owner's loop
    /// calls it until [`WithheldAuditProgressV1::complete`] is true, at which
    /// point every row the store held at open has been checked and further
    /// calls cost nothing at all.
    ///
    /// It is fail-closed in exactly the way the open-time pass was: a row whose
    /// receipt identity no longer matches its evidence is reported as
    /// [`ObservationJournalError::Corrupt`] and the cursor does **not** advance
    /// past it, so the defect is met again on the next call rather than walked
    /// over once and forgotten.
    pub fn validate_withheld_backlog(
        &self,
        limit: u32,
    ) -> Result<WithheldAuditProgressV1, ObservationJournalError> {
        let mut state = self
            .withheld_audit
            .lock()
            .map_err(|_| ObservationJournalError::LockPoisoned)?;
        let WithheldAuditStateV1::Pending(resume_after) = &*state else {
            return Ok(WithheldAuditProgressV1 {
                rows_validated: 0,
                complete: true,
            });
        };
        let resume_after = resume_after.clone();
        let page = self.with_connection(|connection| {
            row::validate_withheld_page(connection, resume_after.as_ref(), limit)
        })?;
        let complete = page.resume_after.is_none();
        *state = match page.resume_after {
            Some(cursor) => WithheldAuditStateV1::Pending(Some(cursor)),
            None => WithheldAuditStateV1::Complete,
        };
        Ok(WithheldAuditProgressV1 {
            rows_validated: page.rows_validated,
            complete,
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
                    u64::try_from(value).map(SourceSequenceV1).map_err(|_| {
                        ObservationJournalError::ValueOutOfRange {
                            field: "last_admitted_sequence",
                        }
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

    /// Acquires the journal connection inside `budget`, or reports the budget
    /// spent.
    ///
    /// `Mutex::lock` would park for however long the current holder needs,
    /// which is precisely the wait a caller with a deadline cannot afford: it
    /// would turn a bounded delivery attempt into an unbounded one.
    fn lock_within(
        &self,
        operation: &'static str,
        budget: RecoveryTimeBudgetV1,
    ) -> Result<MutexGuard<'_, Connection>, ObservationJournalError> {
        if budget.is_spent() {
            return Err(ObservationJournalError::BudgetExhausted { operation });
        }
        let remaining = u64::try_from(budget.remaining_micros).unwrap_or(0);
        let deadline = Instant::now() + Duration::from_micros(remaining);
        loop {
            match self.connection.try_lock() {
                Ok(guard) => return Ok(guard),
                Err(TryLockError::Poisoned(_)) => {
                    return Err(ObservationJournalError::LockPoisoned);
                }
                Err(TryLockError::WouldBlock) => {
                    let now = Instant::now();
                    if now >= deadline {
                        return Err(ObservationJournalError::BudgetExhausted { operation });
                    }
                    std::thread::sleep(
                        Duration::from_micros(LOCK_POLL_MICROS).min(deadline.duration_since(now)),
                    );
                }
            }
        }
    }

    /// Runs one read inside the caller's remaining budget.
    pub(crate) fn with_bounded_connection<T, F>(
        &self,
        operation: &'static str,
        budget: RecoveryTimeBudgetV1,
        action: F,
    ) -> Result<T, ObservationJournalError>
    where
        F: FnOnce(&Connection) -> Result<T, ObservationJournalError>,
    {
        let started = Instant::now();
        let mut guard = self.lock_within(operation, budget)?;
        with_busy_budget(&mut guard, operation, budget, started, |connection| {
            action(connection)
        })
    }

    /// Runs one immediate write transaction inside the caller's remaining
    /// budget. A transaction that cannot start because another writer holds the
    /// database reports the budget spent instead of waiting out the fixed
    /// five-second busy timeout.
    pub(crate) fn with_bounded_transaction<T, F>(
        &self,
        operation: &'static str,
        budget: RecoveryTimeBudgetV1,
        action: F,
    ) -> Result<T, ObservationJournalError>
    where
        F: FnOnce(&Transaction<'_>) -> Result<T, ObservationJournalError>,
    {
        let started = Instant::now();
        let mut guard = self.lock_within(operation, budget)?;
        with_busy_budget(&mut guard, operation, budget, started, |connection| {
            let transaction =
                connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
            let value = action(&transaction)?;
            transaction.commit()?;
            Ok(value)
        })
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

/// Runs `action` with SQLite's own waiting capped by what is left of `budget`,
/// and reports a busy database inside a spent budget as a spent budget.
///
/// Without this the connection's fixed five-second busy timeout would outlive
/// any shorter caller bound: the statement would still be waiting on another
/// writer long after the delivery attempt that asked for it gave up.
fn with_busy_budget<T, F>(
    connection: &mut Connection,
    operation: &'static str,
    budget: RecoveryTimeBudgetV1,
    started: Instant,
    action: F,
) -> Result<T, ObservationJournalError>
where
    F: FnOnce(&mut Connection) -> Result<T, ObservationJournalError>,
{
    let spent = i64::try_from(started.elapsed().as_micros()).unwrap_or(i64::MAX);
    let left = budget.remaining_micros.saturating_sub(spent);
    if left <= 0 {
        return Err(ObservationJournalError::BudgetExhausted { operation });
    }
    // Round up so a sub-millisecond budget still gets one attempt rather than
    // an immediate `SQLITE_BUSY` that looks like contention.
    let millis = left
        .saturating_add(999)
        .saturating_div(1_000)
        .clamp(1, i64::from(u32::MAX));
    connection.busy_timeout(Duration::from_millis(u64::try_from(millis).unwrap_or(1)))?;
    let outcome = action(connection);
    connection.busy_timeout(Duration::from_millis(schema::BUSY_TIMEOUT_MILLIS))?;
    match outcome {
        Err(error) if is_busy(&error) => {
            Err(ObservationJournalError::BudgetExhausted { operation })
        }
        other => other,
    }
}

/// Whether the failure is SQLite reporting the database busy or locked.
fn is_busy(error: &ObservationJournalError) -> bool {
    let ObservationJournalError::Storage(rusqlite::Error::SqliteFailure(failure, _)) = error else {
        return false;
    };
    matches!(
        failure.code,
        ErrorCode::DatabaseBusy | ErrorCode::DatabaseLocked
    )
}
