//! Stable source records and incarnation-safe center support.
//!
//! Center vectors are learned state, not text storage. This module retains the
//! admitted text and source identity behind monotone [`RecordId`] values so a
//! recall result can only hydrate content with an explicit support path.

use crate::numeric::{validate_dimension, validate_finite};
use crate::types::{
    AffectVector, CenterSlot, CoreError, LogicalTick, NcmConfig, RecordId, SourceId, LTM_KEY_DIM,
    STM_KEY_DIM, VALUE_DIM,
};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

/// Lifecycle state of one immutable source record.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum RecordState {
    /// The record is current and may be corrected or deleted.
    Valid,
    /// A newer valid record explicitly supersedes this record.
    Superseded {
        /// The newer record that carries the correction.
        by: RecordId,
        /// Lowercase SHA-256 digest of the host-admitted correction evidence.
        evidence_sha256: String,
    },
    /// The source was deleted in this state epoch and must never be recalled.
    Deleted {
        /// Epoch in which deletion became effective.
        epoch: u64,
    },
}

/// One admitted key/value pair with stable identity and retained projections.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Record {
    /// Monotone per-namespace identity.
    pub id: RecordId,
    /// Opaque host-admitted source identity.
    pub source: SourceId,
    /// Original key text.
    pub key_text: String,
    /// Original value text.
    pub value_text: String,
    /// Logical tick at which the record was created.
    pub created_tick: LogicalTick,
    /// Current lifecycle state.
    pub state: RecordState,
    /// Canonical 64-dimensional LTM key retained for D08 consolidation.
    pub ltm_key: Vec<f32>,
    /// Sixteen-dimensional STM key.
    pub stm_key: Vec<f32>,
    /// Projected 128-dimensional value vector.
    pub value_vec: Vec<f32>,
    /// Four-channel admitted affect.
    pub affect: AffectVector,
}

/// Validated payload used to allocate a fresh [`RecordId`].
#[derive(Clone, Debug, PartialEq)]
pub struct RecordInput {
    /// Opaque host-admitted source identity.
    pub source: SourceId,
    /// Original key text.
    pub key_text: String,
    /// Original value text.
    pub value_text: String,
    /// Logical creation tick.
    pub created_tick: LogicalTick,
    /// Canonical 64-dimensional LTM key.
    pub ltm_key: Vec<f32>,
    /// Sixteen-dimensional STM key.
    pub stm_key: Vec<f32>,
    /// Projected 128-dimensional value vector.
    pub value_vec: Vec<f32>,
    /// Four-channel admitted affect.
    pub affect: AffectVector,
}

/// Deterministic monotone record storage for one namespace.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct RecordTable {
    records: BTreeMap<RecordId, Record>,
    next_id: u64,
    max_record_bytes: usize,
    max_recall_bytes: usize,
}

impl RecordTable {
    /// Creates an empty table using the frozen record and recall byte budgets.
    #[must_use]
    pub fn new(config: &NcmConfig) -> Self {
        Self {
            records: BTreeMap::new(),
            next_id: 1,
            max_record_bytes: config.max_record_bytes,
            max_recall_bytes: config.max_recall_bytes,
        }
    }

    /// Inserts one validated record and allocates its monotone identity.
    ///
    /// The UTF-8 key/value budget is checked before the identity counter or map
    /// is changed, so a rejected insert has no observable effect.
    pub fn insert(&mut self, input: RecordInput) -> Result<RecordId, CoreError> {
        let text_bytes = input
            .key_text
            .len()
            .checked_add(input.value_text.len())
            .ok_or(CoreError::BudgetExceeded("record text"))?;
        if text_bytes > self.max_record_bytes {
            return Err(CoreError::BudgetExceeded("record text"));
        }
        if input.source.0.is_empty() {
            return Err(CoreError::InvalidState(
                "record source identity must be present".to_owned(),
            ));
        }
        validate_dimension(&input.ltm_key, LTM_KEY_DIM, "record LTM key")?;
        validate_dimension(&input.stm_key, STM_KEY_DIM, "record STM key")?;
        validate_dimension(&input.value_vec, VALUE_DIM, "record value vector")?;
        validate_finite(&input.ltm_key, "record LTM key")?;
        validate_finite(&input.stm_key, "record STM key")?;
        validate_finite(&input.value_vec, "record value vector")?;
        validate_finite(&input.affect.0, "record affect")?;

        let id = RecordId(self.next_id);
        let next_id = self
            .next_id
            .checked_add(1)
            .ok_or_else(|| CoreError::InvalidState("record identity exhausted".to_owned()))?;
        let record = Record {
            id,
            source: input.source,
            key_text: input.key_text,
            value_text: input.value_text,
            created_tick: input.created_tick,
            state: RecordState::Valid,
            ltm_key: input.ltm_key,
            stm_key: input.stm_key,
            value_vec: input.value_vec,
            affect: input.affect,
        };
        self.records.insert(id, record);
        self.next_id = next_id;
        Ok(id)
    }

    /// Returns a record by stable identity.
    #[must_use]
    pub fn get(&self, id: RecordId) -> Option<&Record> {
        self.records.get(&id)
    }

    /// Iterates records in ascending identity order.
    pub fn iter(&self) -> impl Iterator<Item = (&RecordId, &Record)> {
        self.records.iter()
    }

    /// Number of retained records, including superseded and deleted lineage.
    #[must_use]
    pub fn len(&self) -> usize {
        self.records.len()
    }

    /// Whether no record identities have been allocated successfully.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.records.is_empty()
    }

    /// Maximum UTF-8 text bytes that one recall may hydrate.
    #[must_use]
    pub const fn max_recall_bytes(&self) -> usize {
        self.max_recall_bytes
    }

    /// Links an older valid record to a newer valid correction.
    ///
    /// Both records remain readable. The older record carries exact lineage;
    /// recency and center activation never perform implicit supersession.
    pub fn supersede(
        &mut self,
        old: RecordId,
        new: RecordId,
        evidence_sha256: String,
    ) -> Result<(), CoreError> {
        if old == new || Self::lineage_reaches(&self.records, new, old)? {
            return Err(CoreError::InvalidState(
                "record supersession cycle".to_owned(),
            ));
        }
        if !is_sha256_hex(&evidence_sha256) {
            return Err(CoreError::InvalidState(
                "correction evidence must be lowercase SHA-256 hex".to_owned(),
            ));
        }
        let old_state = self
            .records
            .get(&old)
            .ok_or(CoreError::UnknownRecord(old))?
            .state
            .clone();
        let new_state = self
            .records
            .get(&new)
            .ok_or(CoreError::UnknownRecord(new))?
            .state
            .clone();
        if old_state != RecordState::Valid || new_state != RecordState::Valid {
            return Err(CoreError::InvalidState(
                "both supersession records must be valid".to_owned(),
            ));
        }
        let old_record = self
            .records
            .get_mut(&old)
            .ok_or(CoreError::UnknownRecord(old))?;
        old_record.state = RecordState::Superseded {
            by: new,
            evidence_sha256,
        };
        Ok(())
    }

    /// Marks every record from `source` deleted in `epoch` and returns the count.
    pub fn mark_deleted(&mut self, source: &SourceId, epoch: u64) -> usize {
        let mut deleted = 0;
        for record in self.records.values_mut() {
            if &record.source == source && !matches!(record.state, RecordState::Deleted { .. }) {
                record.state = RecordState::Deleted { epoch };
                deleted += 1;
            }
        }
        deleted
    }

    fn lineage_reaches(
        records: &BTreeMap<RecordId, Record>,
        start: RecordId,
        target: RecordId,
    ) -> Result<bool, CoreError> {
        let mut cursor = start;
        for _ in 0..=records.len() {
            if cursor == target {
                return Ok(true);
            }
            let record = records
                .get(&cursor)
                .ok_or(CoreError::UnknownRecord(cursor))?;
            match record.state {
                RecordState::Superseded { by, .. } => cursor = by,
                RecordState::Valid | RecordState::Deleted { .. } => return Ok(false),
            }
        }
        Err(CoreError::InvalidState(
            "record supersession lineage contains a cycle".to_owned(),
        ))
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct SlotSupport {
    incarnation: u32,
    records: Vec<RecordId>,
}

/// Incarnation-aware record support retained separately from latent center data.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Support {
    by_index: BTreeMap<u32, SlotSupport>,
    max_center_support: usize,
}

impl Support {
    /// Creates an empty support map with the configured per-center bound.
    #[must_use]
    pub fn new(config: &NcmConfig) -> Self {
        Self {
            by_index: BTreeMap::new(),
            max_center_support: config.max_center_support,
        }
    }

    /// Records recent support for a live slot.
    ///
    /// A newer incarnation replaces the prior slot generation. Repeated IDs
    /// move to the recency tail; overflow drops the oldest ID, matching center
    /// write support retention. The dropped identity is returned as a receipt.
    pub fn record(
        &mut self,
        slot: CenterSlot,
        record_id: RecordId,
    ) -> Result<Option<RecordId>, CoreError> {
        if self.max_center_support == 0 {
            return Err(CoreError::BudgetExceeded("center support"));
        }
        let max_center_support = self.max_center_support;
        let entry = self.entry_for_write(slot)?;
        if let Some(position) = entry.records.iter().position(|id| *id == record_id) {
            entry.records.remove(position);
        }
        entry.records.push(record_id);
        let dropped = if entry.records.len() > max_center_support {
            Some(entry.records.remove(0))
        } else {
            None
        };
        Ok(dropped)
    }

    /// Replaces one slot's support with a validated bounded snapshot.
    pub fn set_support(&mut self, slot: CenterSlot, records: &[RecordId]) -> Result<(), CoreError> {
        if records.len() > self.max_center_support {
            return Err(CoreError::BudgetExceeded("center support"));
        }
        let mut distinct = Vec::with_capacity(records.len());
        for record in records {
            if !distinct.contains(record) {
                distinct.push(*record);
            }
        }
        let entry = self.entry_for_write(slot)?;
        entry.records = distinct;
        Ok(())
    }

    /// Resolves support only for the exact current slot incarnation.
    pub fn support_for(&self, slot: CenterSlot) -> Result<&[RecordId], CoreError> {
        match self.by_index.get(&slot.index) {
            Some(entry) if entry.incarnation == slot.incarnation => Ok(&entry.records),
            _ => Err(CoreError::StaleHandle),
        }
    }

    /// Unions `removed` support into `kept` without dropping lineage.
    ///
    /// If the union would exceed the configured bound, the merge is rejected
    /// before mutation so the maintenance caller can skip that center merge.
    pub fn merge_support(
        &mut self,
        kept: CenterSlot,
        removed: CenterSlot,
    ) -> Result<(), CoreError> {
        let kept_records = self.support_for(kept)?.to_vec();
        let removed_records = self.support_for(removed)?.to_vec();
        let mut union = kept_records;
        for record in removed_records {
            if !union.contains(&record) {
                union.push(record);
            }
        }
        if union.len() > self.max_center_support {
            return Err(CoreError::BudgetExceeded("merged center support"));
        }
        let entry = self
            .by_index
            .get_mut(&kept.index)
            .ok_or(CoreError::StaleHandle)?;
        if entry.incarnation != kept.incarnation {
            return Err(CoreError::StaleHandle);
        }
        entry.records = union;
        Ok(())
    }

    fn entry_for_write(&mut self, slot: CenterSlot) -> Result<&mut SlotSupport, CoreError> {
        match self.by_index.get(&slot.index) {
            Some(entry) if entry.incarnation > slot.incarnation => {
                return Err(CoreError::StaleHandle);
            }
            Some(entry) if entry.incarnation == slot.incarnation => {}
            Some(_) | None => {
                self.by_index.insert(
                    slot.index,
                    SlotSupport {
                        incarnation: slot.incarnation,
                        records: Vec::new(),
                    },
                );
            }
        }
        self.by_index
            .get_mut(&slot.index)
            .ok_or_else(|| CoreError::InvalidState("support insertion failed".to_owned()))
    }
}

fn is_sha256_hex(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}
