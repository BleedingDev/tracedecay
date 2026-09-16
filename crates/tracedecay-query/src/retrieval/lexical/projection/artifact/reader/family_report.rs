use std::collections::BTreeSet;
use std::path::{Component, Path};

use rusqlite::functions::FunctionFlags;
use tracedecay_code_index::clones::{
    CloneBodyOccurrenceV1, CloneExactKeyV1, CloneNormalizationClassV1,
};
use tracedecay_code_index::production::CodeIndexExecutionControlV1;
use tracedecay_domain::{
    CodeGenerationId, ManifestDigest, ProjectId, RepositoryId, RetrievalRequest,
    SymbolOccurrenceId, UtcMicros, canonical_sha256,
};

use super::{
    CloneCursorCodecV1, CloneCursorErrorV1, CloneCursorReadErrorV1, CodeLexicalArtifactReaderV1,
    MAX_CLONE_EXACT_PAGE_MEMBERS_V1,
};
use crate::retrieval::lexical::projection::artifact::{
    CodeLexicalArtifactErrorV1, checkpoint, sqlite_error,
};

#[derive(Clone, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
struct CloneFamilyCursorV1 {
    artifact_digest: ManifestDigest,
    generation: CodeGenerationId,
    request_digest: ManifestDigest,
    after: CloneFamilyCursorPositionV1,
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
struct CloneFamilyCursorPositionV1 {
    reviewable_source_bytes: u64,
    member_count: usize,
    class: CloneNormalizationClassV1,
    normalization_revision: u16,
    digest: ManifestDigest,
    /// A bounded family scan resumes after this whole family key. It is
    /// present only when the posting-row bound made the page partial; the
    /// ranking fields above remain populated for diagnostics and stable
    /// cursor inspection; when a pending family is carried, they also hold
    /// that family's aggregate state while `scan_after` remains the seek key.
    #[serde(default)]
    scan_after: Option<CloneExactKeyV1>,
    /// If the posting-row budget stopped inside `scan_after`, this is the
    /// last occurrence consumed from that family. A following page resumes
    /// after the occurrence and can therefore finish an oversized family.
    #[serde(default)]
    scan_after_occurrence: Option<SymbolOccurrenceId>,
    #[serde(default)]
    minimum_source_bytes: u64,
    #[serde(default)]
    representative: Option<SymbolOccurrenceId>,
    #[serde(default)]
    has_pull_request_member: bool,
}

impl CloneFamilyCursorV1 {
    fn encode(&self) -> Result<String, CodeLexicalArtifactErrorV1> {
        serde_json::to_vec(self)
            .map(hex::encode)
            .map_err(|error| CodeLexicalArtifactErrorV1::Corrupt(error.to_string()))
    }

    fn decode(encoded: &str) -> Result<Self, CodeLexicalArtifactErrorV1> {
        let bytes = hex::decode(encoded)
            .map_err(|error| CodeLexicalArtifactErrorV1::Contract(error.to_string()))?;
        serde_json::from_slice(&bytes)
            .map_err(|error| CodeLexicalArtifactErrorV1::Contract(error.to_string()))
    }

    fn from_authenticated(cursor: super::CloneFamilyCursorV2) -> Result<Self, CloneCursorErrorV1> {
        let member_count =
            usize::try_from(cursor.after.member_count).map_err(|_| CloneCursorErrorV1::Invalid)?;
        Ok(Self {
            artifact_digest: cursor.artifact_digest,
            generation: cursor.generation,
            request_digest: cursor.query_descriptor,
            after: CloneFamilyCursorPositionV1 {
                reviewable_source_bytes: cursor.after.reviewable_source_bytes,
                member_count,
                class: cursor.after.class,
                normalization_revision: cursor.after.normalization_revision,
                digest: cursor.after.digest,
                scan_after: cursor.after.scan_after,
                scan_after_occurrence: cursor.after.scan_after_occurrence,
                minimum_source_bytes: cursor.after.minimum_source_bytes,
                representative: cursor.after.representative,
                has_pull_request_member: cursor.after.has_pull_request_member,
            },
        })
    }

    fn encode_authenticated(
        &self,
        codec: &CloneCursorCodecV1<'_>,
        snapshot_digest: ManifestDigest,
        now: UtcMicros,
    ) -> Result<String, CloneCursorErrorV1> {
        codec.issue_family(
            self.artifact_digest.clone(),
            self.generation.clone(),
            snapshot_digest,
            self.request_digest.clone(),
            super::CloneFamilyCursorPositionV2 {
                reviewable_source_bytes: self.after.reviewable_source_bytes,
                member_count: self.after.member_count as u64,
                class: self.after.class,
                normalization_revision: self.after.normalization_revision,
                digest: self.after.digest.clone(),
                scan_after: self.after.scan_after.clone(),
                scan_after_occurrence: self.after.scan_after_occurrence.clone(),
                minimum_source_bytes: self.after.minimum_source_bytes,
                representative: self.after.representative.clone(),
                has_pull_request_member: self.after.has_pull_request_member,
            },
            now,
        )
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CloneExactFamilyArtifactCandidateV1 {
    pub key: CloneExactKeyV1,
    pub representative: SymbolOccurrenceId,
    pub member_count: usize,
    pub reviewable_source_bytes: u64,
    pub continuation: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CloneExactFamilyArtifactPageV1 {
    pub families: Vec<CloneExactFamilyArtifactCandidateV1>,
    pub next_cursor: Option<String>,
    /// The page was produced from a bounded posting prefix. A scan cursor is
    /// returned when more rows remain, so callers can continue deterministically
    /// without asking SQLite to regroup the corpus from the beginning.
    pub partial: bool,
}

/// Hard cap on raw exact-clone postings read by one family page. The old
/// implementation put `LIMIT` after a corpus-wide `GROUP BY` and sort, which
/// made this read unbounded in both CPU and temporary SQLite storage. A page
/// now reads at most this many postings plus one sentinel row, checking the
/// request control between rows.
pub const CLONE_EXACT_FAMILY_POSTING_ROW_BUDGET_V1: usize = 16_384;

const GENERATED_PATH_FUNCTION: &str = "tracedecay_is_generated_path";
const PULL_REQUEST_PATH_FUNCTION: &str = "tracedecay_is_pull_request_path";

#[derive(Clone, Debug, PartialEq, Eq)]
struct CloneFamilyAggregateV1 {
    key: CloneExactKeyV1,
    representative: SymbolOccurrenceId,
    member_count: usize,
    total_source_bytes: u64,
    minimum_source_bytes: u64,
    reviewable_source_bytes: u64,
    has_pull_request_member: bool,
}

impl CloneFamilyAggregateV1 {
    fn new(
        key: CloneExactKeyV1,
        representative: SymbolOccurrenceId,
        body_bytes: u64,
        pull_request_member: bool,
    ) -> Self {
        Self {
            key,
            representative,
            member_count: 1,
            total_source_bytes: body_bytes,
            minimum_source_bytes: body_bytes,
            reviewable_source_bytes: 0,
            has_pull_request_member: pull_request_member,
        }
    }

    fn push(&mut self, occurrence_id: SymbolOccurrenceId, body_bytes: u64, pull_request: bool) {
        self.representative = self.representative.clone().min(occurrence_id);
        self.member_count = self.member_count.saturating_add(1);
        self.total_source_bytes = self.total_source_bytes.saturating_add(body_bytes);
        self.minimum_source_bytes = self.minimum_source_bytes.min(body_bytes);
        self.reviewable_source_bytes = self
            .total_source_bytes
            .saturating_sub(self.minimum_source_bytes);
        self.has_pull_request_member |= pull_request;
    }

    fn is_reportable(&self, pull_request_scope: bool) -> bool {
        self.member_count > 1 && (!pull_request_scope || self.has_pull_request_member)
    }
}

fn family_rank_cmp(
    left: &CloneFamilyAggregateV1,
    right: &CloneFamilyAggregateV1,
) -> std::cmp::Ordering {
    right
        .reviewable_source_bytes
        .cmp(&left.reviewable_source_bytes)
        .then_with(|| right.member_count.cmp(&left.member_count))
        .then_with(|| left.key.cmp(&right.key))
}

fn family_is_after_rank(
    family: &CloneFamilyAggregateV1,
    after: &CloneFamilyCursorPositionV1,
) -> bool {
    family.reviewable_source_bytes < after.reviewable_source_bytes
        || (family.reviewable_source_bytes == after.reviewable_source_bytes
            && (family.member_count < after.member_count
                || (family.member_count == after.member_count
                    && (family.key.class as u8 > after.class as u8
                        || (family.key.class == after.class
                            && (family.key.normalization_revision
                                > after.normalization_revision
                                || (family.key.normalization_revision
                                    == after.normalization_revision
                                    && family.key.digest > after.digest)))))))
}

fn family_scan_cursor_position(
    key: &CloneExactKeyV1,
    scan_after_occurrence: Option<SymbolOccurrenceId>,
    partial_family: Option<&CloneFamilyAggregateV1>,
) -> CloneFamilyCursorPositionV1 {
    let state_key = partial_family.map_or(key, |family| &family.key);
    CloneFamilyCursorPositionV1 {
        reviewable_source_bytes: partial_family
            .map(|family| family.reviewable_source_bytes)
            .unwrap_or_default(),
        member_count: partial_family
            .map(|family| family.member_count)
            .unwrap_or_default(),
        class: state_key.class,
        normalization_revision: state_key.normalization_revision,
        digest: state_key.digest.clone(),
        scan_after: Some(key.clone()),
        scan_after_occurrence,
        minimum_source_bytes: partial_family
            .map(|family| family.minimum_source_bytes)
            .unwrap_or_default(),
        representative: partial_family.map(|family| family.representative.clone()),
        has_pull_request_member: partial_family
            .map(|family| family.has_pull_request_member)
            .unwrap_or_default(),
    }
}

fn family_from_scan_cursor(
    position: &CloneFamilyCursorPositionV1,
) -> Option<CloneFamilyAggregateV1> {
    position.scan_after.as_ref()?;
    let representative = position.representative.clone()?;
    let key = CloneExactKeyV1 {
        class: position.class,
        normalization_revision: position.normalization_revision,
        digest: position.digest.clone(),
    };
    let member_count = position.member_count;
    let minimum_source_bytes = position.minimum_source_bytes;
    let total_source_bytes = position
        .reviewable_source_bytes
        .saturating_add(minimum_source_bytes);
    Some(CloneFamilyAggregateV1 {
        key,
        representative,
        member_count,
        total_source_bytes,
        minimum_source_bytes,
        reviewable_source_bytes: position.reviewable_source_bytes,
        has_pull_request_member: position.has_pull_request_member,
    })
}

fn family_rank_cursor_position(family: &CloneFamilyAggregateV1) -> CloneFamilyCursorPositionV1 {
    CloneFamilyCursorPositionV1 {
        reviewable_source_bytes: family.reviewable_source_bytes,
        member_count: family.member_count,
        class: family.key.class,
        normalization_revision: family.key.normalization_revision,
        digest: family.key.digest.clone(),
        scan_after: None,
        scan_after_occurrence: None,
        minimum_source_bytes: 0,
        representative: None,
        has_pull_request_member: false,
    }
}

fn clone_normalization_class(
    value: i64,
) -> Result<CloneNormalizationClassV1, CodeLexicalArtifactErrorV1> {
    match value {
        1 => Ok(CloneNormalizationClassV1::Conservative),
        2 => Ok(CloneNormalizationClassV1::Rename),
        other => Err(CodeLexicalArtifactErrorV1::Corrupt(format!(
            "clone family has unknown normalization class {other}"
        ))),
    }
}

fn clone_body_range_bytes(start: i64, end: i64) -> Result<u64, CodeLexicalArtifactErrorV1> {
    let start = u64::try_from(start).map_err(|error| {
        CodeLexicalArtifactErrorV1::Corrupt(format!("clone body start is invalid: {error}"))
    })?;
    let end = u64::try_from(end).map_err(|error| {
        CodeLexicalArtifactErrorV1::Corrupt(format!("clone body end is invalid: {error}"))
    })?;
    end.checked_sub(start).ok_or_else(|| {
        CodeLexicalArtifactErrorV1::Corrupt("clone body range is inverted".to_owned())
    })
}

fn validate_family_occurrence(
    occurrence: &CloneBodyOccurrenceV1,
    project_id: &ProjectId,
    repository_id: &RepositoryId,
    generation: &CodeGenerationId,
    posting_occurrence_id: &SymbolOccurrenceId,
    posting_payload_digest: &ManifestDigest,
    path: &str,
    body_start: u64,
    body_end: u64,
) -> Result<(), CodeLexicalArtifactErrorV1> {
    if occurrence.project_id != *project_id
        || occurrence.repository_id != *repository_id
        || occurrence.source_generation != *generation
        || occurrence.symbol_occurrence_id != *posting_occurrence_id
        || occurrence.payload_digest != *posting_payload_digest
        || occurrence.path != path
        || occurrence.body_span.start_byte != body_start
        || occurrence.body_span.end_byte != body_end
    {
        return Err(CodeLexicalArtifactErrorV1::Corrupt(
            "clone family posting does not match its authorized occurrence".to_owned(),
        ));
    }
    Ok(())
}

impl CodeLexicalArtifactReaderV1 {
    #[allow(clippy::too_many_arguments)] // mirrors sibling clone page readers' filter/cursor surface
    pub fn clone_exact_family_page(
        &self,
        project_id: &ProjectId,
        repository_id: &RepositoryId,
        match_classes: &[CloneNormalizationClassV1],
        path: Option<&str>,
        pull_request_paths: Option<&[String]>,
        pull_request_scope_digest: Option<&ManifestDigest>,
        include_generated_paths: bool,
        cursor: Option<&str>,
        limit: usize,
        control: &dyn CodeIndexExecutionControlV1,
    ) -> Result<CloneExactFamilyArtifactPageV1, CodeLexicalArtifactErrorV1> {
        checkpoint(control)?;
        if self.metadata.repository_id.as_ref() != Some(repository_id) {
            return Err(CodeLexicalArtifactErrorV1::Missing(
                "clone family repository authority is unavailable".to_owned(),
            ));
        }
        if !self.layout.has_clone_index() {
            return Err(CodeLexicalArtifactErrorV1::Incompatible(
                "clone family lookup requires lexical artifact revision 15".to_owned(),
            ));
        }
        if limit == 0 || limit > MAX_CLONE_EXACT_PAGE_MEMBERS_V1 {
            return Err(CodeLexicalArtifactErrorV1::Contract(format!(
                "clone family page limit must be within 1..={MAX_CLONE_EXACT_PAGE_MEMBERS_V1}"
            )));
        }
        let mut match_classes = match_classes.to_vec();
        match_classes.sort();
        match_classes.dedup();
        let request_digest = canonical_sha256(&(
            "tracedecay.clone-family-request.v1",
            self.receipt.artifact_digest(),
            project_id,
            repository_id,
            &match_classes,
            path,
            pull_request_paths,
            pull_request_scope_digest,
            include_generated_paths,
        ))
        .map_err(|error| CodeLexicalArtifactErrorV1::Contract(error.to_string()))?;
        let cursor = cursor.map(CloneFamilyCursorV1::decode).transpose()?;
        let after = match cursor.as_ref() {
            Some(cursor)
                if cursor.artifact_digest == *self.receipt.artifact_digest()
                    && cursor.generation == self.metadata.generation
                    && cursor.request_digest == request_digest =>
            {
                Some(&cursor.after)
            }
            Some(_) => {
                return Err(CodeLexicalArtifactErrorV1::Contract(
                    "clone family cursor does not match its artifact or request".to_owned(),
                ));
            }
            None => None,
        };
        let fetch = CLONE_EXACT_FAMILY_POSTING_ROW_BUDGET_V1
            .checked_add(1)
            .ok_or_else(|| {
                CodeLexicalArtifactErrorV1::Contract(
                    "clone family posting-row budget overflowed".to_owned(),
                )
            })?;
        let scan_after = after.and_then(|position| position.scan_after.as_ref());
        let scan_after_class = scan_after
            .map(|position| position.class as u8)
            .unwrap_or_default();
        let scan_after_revision = scan_after
            .map(|position| position.normalization_revision)
            .unwrap_or_default();
        let scan_after_digest = scan_after
            .map(|position| position.digest.as_str())
            .unwrap_or("");
        let scan_after_occurrence = after.and_then(|position| {
            position
                .scan_after
                .as_ref()
                .and(position.scan_after_occurrence.as_ref())
                .map(SymbolOccurrenceId::as_str)
        });
        let connection = self.lock_connection()?;
        install_generated_path_function(&connection)?;
        install_pull_request_path_function(&connection, pull_request_paths)?;
        let mut statement = connection
            .prepare_cached(
                "SELECT posting.class, posting.normalization_revision, posting.digest, \
                        posting.symbol_occurrence_id, posting.payload_digest, \
                        occurrence.path, occurrence.body_start, occurrence.body_end, \
                        occurrence.occurrence \
                 FROM clone_exact_postings AS posting \
                 JOIN clone_occurrences AS occurrence \
                   ON occurrence.symbol_occurrence_id = posting.symbol_occurrence_id \
                 WHERE ((:conservative AND posting.class = 1) OR (:rename AND posting.class = 2)) \
                   AND (:path IS NULL OR occurrence.path = :path \
                        OR (substr(occurrence.path, 1, length(:path)) = :path \
                            AND substr(occurrence.path, length(:path) + 1, 1) = '/')) \
                   AND (:include_generated OR tracedecay_is_generated_path(occurrence.path) = 0) \
                   AND (NOT :has_scan_after \
                        OR posting.class > :scan_after_class \
                        OR (posting.class = :scan_after_class \
                            AND posting.normalization_revision > :scan_after_revision) \
                        OR (posting.class = :scan_after_class \
                            AND posting.normalization_revision = :scan_after_revision \
                            AND posting.digest > :scan_after_digest) \
                        OR (posting.class = :scan_after_class \
                            AND posting.normalization_revision = :scan_after_revision \
                            AND posting.digest = :scan_after_digest \
                            AND :has_scan_after_occurrence \
                            AND posting.symbol_occurrence_id > :scan_after_occurrence)) \
                 ORDER BY posting.class, posting.normalization_revision, posting.digest, \
                          posting.symbol_occurrence_id \
                 LIMIT :fetch",
            )
            .map_err(sqlite_error)?;
        let mut rows = statement
            .query(rusqlite::named_params! {
                ":conservative": match_classes.contains(&CloneNormalizationClassV1::Conservative),
                ":rename": match_classes.contains(&CloneNormalizationClassV1::Rename),
                ":path": path,
                ":include_generated": include_generated_paths,
                ":has_scan_after": scan_after.is_some(),
                ":scan_after_class": i64::from(scan_after_class),
                ":scan_after_revision": i64::from(scan_after_revision),
                ":scan_after_digest": scan_after_digest,
                ":has_scan_after_occurrence": scan_after_occurrence.is_some(),
                ":scan_after_occurrence": scan_after_occurrence.unwrap_or(""),
                ":fetch": i64::try_from(fetch)
                    .map_err(|error| CodeLexicalArtifactErrorV1::Contract(error.to_string()))?,
            })
            .map_err(sqlite_error)?;
        let pull_request_set =
            pull_request_paths.map(|paths| paths.iter().cloned().collect::<BTreeSet<_>>());
        let mut aggregates = Vec::new();
        let mut current = after.and_then(family_from_scan_cursor);
        let mut last_scanned_family_key = None;
        let mut last_scanned_occurrence_id = None;
        let mut scan_sentinel_key = None;
        let mut scanned_rows = 0usize;
        let mut scan_truncated = false;

        while let Some(row) = rows.next().map_err(sqlite_error)? {
            checkpoint(control)?;
            let class = clone_normalization_class(row.get::<_, i64>(0).map_err(sqlite_error)?)?;
            let normalization_revision = u16::try_from(row.get::<_, i64>(1).map_err(sqlite_error)?)
                .map_err(|error| CodeLexicalArtifactErrorV1::Corrupt(error.to_string()))?;
            let digest = ManifestDigest::new(row.get::<_, String>(2).map_err(sqlite_error)?)
                .map_err(|error| CodeLexicalArtifactErrorV1::Corrupt(error.to_string()))?;
            let key = CloneExactKeyV1 {
                class,
                normalization_revision,
                digest,
            };
            if scanned_rows == CLONE_EXACT_FAMILY_POSTING_ROW_BUDGET_V1 {
                scan_truncated = true;
                scan_sentinel_key = Some(key);
                break;
            }
            scanned_rows = scanned_rows.saturating_add(1);
            last_scanned_family_key = Some(key.clone());

            let occurrence_id =
                SymbolOccurrenceId::new(row.get::<_, String>(3).map_err(sqlite_error)?)
                    .map_err(|error| CodeLexicalArtifactErrorV1::Corrupt(error.to_string()))?;
            last_scanned_occurrence_id = Some(occurrence_id.clone());
            let posting_payload_digest =
                ManifestDigest::new(row.get::<_, String>(4).map_err(sqlite_error)?)
                    .map_err(|error| CodeLexicalArtifactErrorV1::Corrupt(error.to_string()))?;
            let occurrence_path = row.get::<_, String>(5).map_err(sqlite_error)?;
            let body_start = row.get::<_, i64>(6).map_err(sqlite_error)?;
            let body_end = row.get::<_, i64>(7).map_err(sqlite_error)?;
            let body_bytes = clone_body_range_bytes(body_start, body_end)?;
            let occurrence_bytes = row.get::<_, Vec<u8>>(8).map_err(sqlite_error)?;
            let occurrence: CloneBodyOccurrenceV1 = serde_json::from_slice(&occurrence_bytes)
                .map_err(|error| CodeLexicalArtifactErrorV1::Corrupt(error.to_string()))?;
            validate_family_occurrence(
                &occurrence,
                project_id,
                repository_id,
                &self.metadata.generation,
                &occurrence_id,
                &posting_payload_digest,
                &occurrence_path,
                u64::try_from(body_start).map_err(|error| {
                    CodeLexicalArtifactErrorV1::Corrupt(format!(
                        "clone body start is invalid: {error}"
                    ))
                })?,
                u64::try_from(body_end).map_err(|error| {
                    CodeLexicalArtifactErrorV1::Corrupt(format!(
                        "clone body end is invalid: {error}"
                    ))
                })?,
            )?;
            let pull_request_member = pull_request_set
                .as_ref()
                .is_some_and(|paths| paths.contains(&occurrence_path));

            if current.as_ref().is_some_and(|family| family.key != key) {
                let previous = current
                    .take()
                    .expect("clone family current aggregate disappeared");
                if previous.is_reportable(pull_request_set.is_some()) {
                    aggregates.push(previous);
                }
            }
            if let Some(family) = current.as_mut() {
                family.push(occurrence_id, body_bytes, pull_request_member);
            } else {
                current = Some(CloneFamilyAggregateV1::new(
                    key,
                    occurrence_id,
                    body_bytes,
                    pull_request_member,
                ));
            }
        }

        let incomplete_current = scan_truncated
            && current
                .as_ref()
                .zip(scan_sentinel_key.as_ref())
                .is_some_and(|(family, sentinel)| family.key == *sentinel);
        let partial_family = incomplete_current.then(|| current.clone()).flatten();
        if !incomplete_current {
            if let Some(previous) = current.take()
                && previous.is_reportable(pull_request_set.is_some())
            {
                aggregates.push(previous);
            }
        }
        let scan_advance_key = last_scanned_family_key
            .clone()
            .or_else(|| scan_sentinel_key.clone());
        let scan_mode = scan_after.is_some() || scan_truncated;
        if scan_mode {
            // A legacy ranked cursor has no raw-key boundary. Preserve its
            // strict position filter for the bounded prefix before switching
            // the partial stream to deterministic key order.
            if scan_after.is_none()
                && let Some(after) = after
            {
                aggregates.retain(|family| family_is_after_rank(family, after));
            }
            aggregates.sort_by(|left, right| left.key.cmp(&right.key));
        } else {
            aggregates.sort_by(family_rank_cmp);
            if let Some(after) = after {
                aggregates.retain(|family| family_is_after_rank(family, after));
            }
        }

        let has_page_overflow = aggregates.len() > limit;
        let families = aggregates
            .into_iter()
            .take(limit)
            .map(|family| {
                let after = if scan_mode {
                    family_scan_cursor_position(&family.key, None, None)
                } else {
                    family_rank_cursor_position(&family)
                };
                let continuation = CloneFamilyCursorV1 {
                    artifact_digest: self.receipt.artifact_digest().clone(),
                    generation: self.metadata.generation.clone(),
                    request_digest: request_digest.clone(),
                    after,
                }
                .encode()?;
                Ok(CloneExactFamilyArtifactCandidateV1 {
                    key: family.key,
                    representative: family.representative,
                    member_count: family.member_count,
                    reviewable_source_bytes: family.reviewable_source_bytes,
                    continuation,
                })
            })
            .collect::<Result<Vec<_>, CodeLexicalArtifactErrorV1>>()?;

        let next_cursor = if has_page_overflow {
            if scan_truncated
                && let (Some(family), Some(partial_family)) =
                    (families.last(), partial_family.as_ref())
            {
                let after = family_scan_cursor_position(&family.key, None, Some(partial_family));
                Some(
                    CloneFamilyCursorV1 {
                        artifact_digest: self.receipt.artifact_digest().clone(),
                        generation: self.metadata.generation.clone(),
                        request_digest: request_digest.clone(),
                        after,
                    }
                    .encode()?,
                )
            } else {
                families.last().map(|family| family.continuation.clone())
            }
        } else if scan_truncated {
            scan_advance_key
                .as_ref()
                .map(|key| {
                    let occurrence = last_scanned_family_key
                        .as_ref()
                        .filter(|last_key| *last_key == key)
                        .and(last_scanned_occurrence_id.clone());
                    family_scan_cursor_position(key, occurrence, partial_family.as_ref())
                })
                .map(|after| CloneFamilyCursorV1 {
                    artifact_digest: self.receipt.artifact_digest().clone(),
                    generation: self.metadata.generation.clone(),
                    request_digest,
                    after,
                })
                .map(|cursor| cursor.encode())
                .transpose()?
        } else {
            None
        };
        Ok(CloneExactFamilyArtifactPageV1 {
            families,
            next_cursor,
            partial: scan_mode,
        })
    }

    /// Serve one family page through the daemon-mounted authenticated cursor
    /// boundary. The legacy family aggregator remains the bounded storage
    /// implementation; this wrapper authenticates its input and replaces all
    /// legacy continuations in the returned family projection.
    #[allow(clippy::too_many_arguments)]
    pub fn clone_exact_family_page_authenticated(
        &self,
        project_id: &ProjectId,
        repository_id: &RepositoryId,
        match_classes: &[CloneNormalizationClassV1],
        path: Option<&str>,
        pull_request_paths: Option<&[String]>,
        pull_request_scope_digest: Option<&ManifestDigest>,
        include_generated_paths: bool,
        cursor: Option<&str>,
        limit: usize,
        query_authority: &crate::retrieval::QueryAuthorityV1,
        request: &RetrievalRequest,
        snapshot_digest: &ManifestDigest,
        now: UtcMicros,
        control: &dyn CodeIndexExecutionControlV1,
    ) -> Result<CloneExactFamilyArtifactPageV1, CloneCursorReadErrorV1> {
        let codec = CloneCursorCodecV1::new(query_authority, request)?;
        let legacy_cursor = cursor
            .map(|encoded| {
                CloneFamilyCursorV1::from_authenticated(codec.decode_family_unbound(
                    encoded,
                    self.receipt.artifact_digest(),
                    &self.metadata.generation,
                    snapshot_digest,
                    now,
                )?)
            })
            .transpose()?;
        let legacy_encoded = legacy_cursor
            .as_ref()
            .map(CloneFamilyCursorV1::encode)
            .transpose()
            .map_err(CloneCursorReadErrorV1::Artifact)?;
        let mut page = self
            .clone_exact_family_page(
                project_id,
                repository_id,
                match_classes,
                path,
                pull_request_paths,
                pull_request_scope_digest,
                include_generated_paths,
                legacy_encoded.as_deref(),
                limit,
                control,
            )
            .map_err(map_authenticated_family_artifact_error)?;
        for family in &mut page.families {
            let legacy = CloneFamilyCursorV1::decode(&family.continuation)
                .map_err(CloneCursorReadErrorV1::Artifact)?;
            family.continuation = legacy
                .encode_authenticated(&codec, snapshot_digest.clone(), now)
                .map_err(CloneCursorReadErrorV1::Cursor)?;
        }
        page.next_cursor = page
            .next_cursor
            .as_deref()
            .map(CloneFamilyCursorV1::decode)
            .transpose()
            .map_err(CloneCursorReadErrorV1::Artifact)?
            .map(|cursor| cursor.encode_authenticated(&codec, snapshot_digest.clone(), now))
            .transpose()
            .map_err(CloneCursorReadErrorV1::Cursor)?;
        Ok(page)
    }
}

fn map_authenticated_family_artifact_error(
    error: CodeLexicalArtifactErrorV1,
) -> CloneCursorReadErrorV1 {
    match error {
        CodeLexicalArtifactErrorV1::Contract(message)
            if message == "clone family cursor does not match its artifact or request" =>
        {
            CloneCursorReadErrorV1::Cursor(CloneCursorErrorV1::Stale)
        }
        error => CloneCursorReadErrorV1::Artifact(error),
    }
}

fn install_generated_path_function(
    connection: &rusqlite::Connection,
) -> Result<(), CodeLexicalArtifactErrorV1> {
    connection
        .create_scalar_function(
            GENERATED_PATH_FUNCTION,
            1,
            FunctionFlags::SQLITE_UTF8 | FunctionFlags::SQLITE_DETERMINISTIC,
            |context| {
                let path = context.get::<String>(0)?;
                Ok(Path::new(&path).components().any(|component| {
                    matches!(
                        component,
                        Component::Normal(segment)
                            if segment
                                .to_str()
                                .is_some_and(tracedecay_domain::is_generated_dir_segment)
                    )
                }))
            },
        )
        .map_err(sqlite_error)
}

fn install_pull_request_path_function(
    connection: &rusqlite::Connection,
    paths: Option<&[String]>,
) -> Result<(), CodeLexicalArtifactErrorV1> {
    let paths = paths
        .into_iter()
        .flatten()
        .cloned()
        .collect::<BTreeSet<_>>();
    connection
        .create_scalar_function(
            PULL_REQUEST_PATH_FUNCTION,
            1,
            FunctionFlags::SQLITE_UTF8 | FunctionFlags::SQLITE_DETERMINISTIC,
            move |context| {
                let path = context.get::<String>(0)?;
                Ok(paths.contains(&path))
            },
        )
        .map_err(sqlite_error)
}

#[cfg(test)]
mod tests {
    use std::fmt;

    use super::*;

    fn id<T>(value: &str) -> T
    where
        T: TryFrom<String>,
        <T as TryFrom<String>>::Error: fmt::Debug,
    {
        T::try_from(value.to_owned()).expect("valid fixture identity")
    }

    fn digest(label: &str) -> ManifestDigest {
        canonical_sha256(&label).expect("valid fixture digest")
    }

    fn key(class: CloneNormalizationClassV1, revision: u16, label: &str) -> CloneExactKeyV1 {
        CloneExactKeyV1 {
            class,
            normalization_revision: revision,
            digest: digest(label),
        }
    }

    #[test]
    fn aggregate_preserves_representative_and_sum_minus_smallest_body() {
        let mut family = CloneFamilyAggregateV1::new(
            key(CloneNormalizationClassV1::Conservative, 1, "family"),
            id("symbol.03"),
            20,
            false,
        );
        family.push(id("symbol.02"), 35, false);
        family.push(id("symbol.01"), 10, false);

        assert_eq!(family.representative, id("symbol.01"));
        assert_eq!(family.member_count, 3);
        assert_eq!(family.reviewable_source_bytes, 55);
        assert!(family.is_reportable(false));
    }

    #[test]
    fn scan_cursor_round_trip_keeps_the_whole_family_boundary() {
        let family_key = key(CloneNormalizationClassV1::Rename, 7, "family");
        let cursor = CloneFamilyCursorV1 {
            artifact_digest: digest("artifact"),
            generation: id("generation.clone-family"),
            request_digest: digest("request"),
            after: family_scan_cursor_position(&family_key, None, None),
        };

        let encoded = cursor.encode().expect("encode family cursor");
        let decoded = CloneFamilyCursorV1::decode(&encoded).expect("decode family cursor");

        assert_eq!(decoded, cursor);
        assert_eq!(decoded.after.scan_after.as_ref(), Some(&family_key));
    }

    #[test]
    fn scan_cursor_keeps_scan_boundary_and_pending_family_state_separate() {
        let boundary_key = key(CloneNormalizationClassV1::Conservative, 1, "boundary");
        let pending_key = key(CloneNormalizationClassV1::Rename, 2, "pending");
        let mut pending =
            CloneFamilyAggregateV1::new(pending_key.clone(), id("symbol.pending.02"), 20, false);
        pending.push(id("symbol.pending.01"), 35, false);

        let position = family_scan_cursor_position(&boundary_key, None, Some(&pending));
        assert_eq!(position.scan_after.as_ref(), Some(&boundary_key));
        assert_eq!(position.class, pending_key.class);
        assert_eq!(
            position.normalization_revision,
            pending_key.normalization_revision
        );
        assert_eq!(position.digest, pending_key.digest);
        assert_eq!(family_from_scan_cursor(&position), Some(pending));
    }

    #[test]
    fn rank_cursor_retains_strict_descending_page_order() {
        let mut first = CloneFamilyAggregateV1::new(
            key(CloneNormalizationClassV1::Conservative, 1, "first"),
            id("symbol.first"),
            20,
            false,
        );
        first.push(id("symbol.first.2"), 30, false);
        let second = CloneFamilyAggregateV1::new(
            key(CloneNormalizationClassV1::Conservative, 1, "second"),
            id("symbol.second"),
            10,
            false,
        );
        let after = family_rank_cursor_position(&first);

        assert_eq!(family_rank_cmp(&first, &second), std::cmp::Ordering::Less);
        assert!(family_is_after_rank(&second, &after));
        assert!(!family_is_after_rank(&first, &after));
        assert!(after.scan_after.is_none());
    }
}
