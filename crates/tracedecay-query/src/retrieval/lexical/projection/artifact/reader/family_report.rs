use std::collections::BTreeSet;
use std::path::{Component, Path};

use rusqlite::functions::FunctionFlags;
use tracedecay_code_index::clones::{
    CloneBodyOccurrenceV1, CloneExactKeyV1, CloneNormalizationClassV1,
};
use tracedecay_code_index::production::CodeIndexExecutionControlV1;
use tracedecay_domain::{
    CodeGenerationId, ManifestDigest, ProjectId, RepositoryId, SymbolOccurrenceId, canonical_sha256,
};

use super::{CodeLexicalArtifactReaderV1, MAX_CLONE_EXACT_PAGE_MEMBERS_V1};
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
    /// cursor inspection, but are not used to seek a scan cursor.
    #[serde(default)]
    scan_after: Option<CloneExactKeyV1>,
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

fn family_scan_cursor_position(key: &CloneExactKeyV1) -> CloneFamilyCursorPositionV1 {
    CloneFamilyCursorPositionV1 {
        reviewable_source_bytes: 0,
        member_count: 0,
        class: key.class,
        normalization_revision: key.normalization_revision,
        digest: key.digest.clone(),
        scan_after: Some(key.clone()),
    }
}

fn family_rank_cursor_position(family: &CloneFamilyAggregateV1) -> CloneFamilyCursorPositionV1 {
    CloneFamilyCursorPositionV1 {
        reviewable_source_bytes: family.reviewable_source_bytes,
        member_count: family.member_count,
        class: family.key.class,
        normalization_revision: family.key.normalization_revision,
        digest: family.key.digest.clone(),
        scan_after: None,
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
                            AND posting.digest > :scan_after_digest)) \
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
                ":fetch": i64::try_from(fetch)
                    .map_err(|error| CodeLexicalArtifactErrorV1::Contract(error.to_string()))?,
            })
            .map_err(sqlite_error)?;
        let pull_request_set =
            pull_request_paths.map(|paths| paths.iter().cloned().collect::<BTreeSet<_>>());
        let mut aggregates = Vec::new();
        let mut current: Option<CloneFamilyAggregateV1> = None;
        let mut last_scanned_family_key = None;
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
        if !incomplete_current {
            if let Some(previous) = current.take()
                && previous.is_reportable(pull_request_set.is_some())
            {
                aggregates.push(previous);
            }
        }
        let scan_advance_key = last_scanned_family_key.or(scan_sentinel_key);
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
                    family_scan_cursor_position(&family.key)
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
            families.last().map(|family| family.continuation.clone())
        } else if scan_truncated {
            scan_advance_key
                .as_ref()
                .map(family_scan_cursor_position)
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
            after: family_scan_cursor_position(&family_key),
        };

        let encoded = cursor.encode().expect("encode family cursor");
        let decoded = CloneFamilyCursorV1::decode(&encoded).expect("decode family cursor");

        assert_eq!(decoded, cursor);
        assert_eq!(decoded.after.scan_after.as_ref(), Some(&family_key));
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
