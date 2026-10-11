//! Trims the low-value tail of a ranked result page behind a retrieval handle.
//!
//! Kept rows stay byte-exact, and the cut rows are stored verbatim, so one
//! `tracedecay_retrieve` call restores exactly what the page left out.

use std::path::Path;
use std::time::Instant;

use tracedecay_runtime_core::tracedecay::current_timestamp;

use super::prepare_truncated_response_handle;
use crate::response_handles::{RESPONSE_RETRIEVE_TOOL, observe_response_truncation};

const MIN_KEPT_ROWS: usize = 3;
/// The marker line costs about one row, so cutting a single row saves nothing.
const MIN_TRIMMED_ROWS: usize = 2;

/// One rendered row of a ranked result page.
pub(crate) struct RankedRow {
    /// One-based display rank.
    pub rank: u64,
    /// The retrieval kernel's fixed-point utility; integer, so the cut is
    /// exact and repeated calls return identical bytes.
    pub utility_micros: u64,
    /// Exact-tier and caller-anchored rows are promised on the page.
    pub protected: bool,
    /// The exact bytes the untrimmed page emits for this row.
    pub markdown: String,
}

/// Start of the longest suffix of unprotected rows that each score at most
/// half the page's best utility, or `None` when no worthwhile cut exists.
fn low_value_tail_start(rows: &[RankedRow]) -> Option<usize> {
    let best = rows
        .iter()
        .map(|row| row.utility_micros)
        .max()
        .filter(|best| *best > 0)?;
    let weak =
        |row: &RankedRow| !row.protected && u128::from(row.utility_micros) * 2 <= u128::from(best);
    // Ordering ranks exact tier and source role ahead of utility, so a strong
    // row can follow weak ones; cutting only after the last strong row never
    // drops a strong row and keeps the cut ranks contiguous.
    let start = rows
        .iter()
        .rposition(|row| !weak(row))
        .map_or(0, |last_strong| last_strong + 1)
        .max(MIN_KEPT_ROWS);
    (rows.len().saturating_sub(start) >= MIN_TRIMMED_ROWS).then_some(start)
}

/// The rows as the page renders them, with the low-value tail replaced by
/// one marker line naming its retrieval handle. Fails open to every row when
/// the handle cannot be stored.
pub(crate) fn ranked_rows_md(response_handle_root: Option<&Path>, rows: &[RankedRow]) -> String {
    let concat =
        |rows: &[RankedRow]| -> String { rows.iter().map(|row| row.markdown.as_str()).collect() };
    let Some(start) = low_value_tail_start(rows) else {
        return concat(rows);
    };
    let started = Instant::now();
    let (kept, cut) = rows.split_at(start);
    let cut_md = concat(cut);
    let Some(record) = prepare_truncated_response_handle(response_handle_root, &cut_md).record
    else {
        return concat(rows);
    };
    let best = rows
        .iter()
        .map(|row| row.utility_micros)
        .max()
        .unwrap_or_default();
    let mut rendered = concat(kept);
    let untrimmed_bytes = rendered.len() + cut_md.len();
    rendered.push_str(&trimmed_tail_marker(cut, best, &record.handle));
    observe_response_truncation(
        untrimmed_bytes,
        rendered.len(),
        true,
        current_timestamp(),
        "stored",
        started.elapsed(),
    );
    rendered
}

/// Carries no timestamp: the handle is a content digest, so the marker is a
/// pure function of the rows and repeated calls stay prompt-cache identical.
fn trimmed_tail_marker(cut: &[RankedRow], best: u64, handle: &str) -> String {
    let first = cut.first().map_or(0, |row| row.rank);
    let last = cut.last().map_or(0, |row| row.rank);
    let bytes: usize = cut.iter().map(|row| row.markdown.len()).sum();
    format!(
        "- _Trimmed {} lower-ranked results (ranks {first}-{last}, {bytes} bytes): each scored \
         at most half of this page's best utility {best}. To restore them, call \
         `{RESPONSE_RETRIEVE_TOOL}` with handle `{handle}` (pass the same `project_selector` \
         if this call used one)._\n",
        cut.len()
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::response_handles::{
        RESPONSE_HANDLE_TTL_SECS, ResponseHandleLookup, retrieve_response_handle,
        store_response_handle,
    };

    fn row(rank: u64, utility_micros: u64, protected: bool, name: &str) -> RankedRow {
        RankedRow {
            rank,
            utility_micros,
            protected,
            markdown: format!("- **{name}** rank {rank}\n  anchor_id: `code-symbol:{name}`\n"),
        }
    }

    fn page(utilities: &[u64]) -> Vec<RankedRow> {
        utilities
            .iter()
            .enumerate()
            .map(|(index, utility)| row(index as u64 + 1, *utility, false, &format!("r{index}")))
            .collect()
    }

    /// Two strong rows (one named in non-ASCII) ahead of eight weak ones.
    fn weak_tail_page() -> Vec<RankedRow> {
        let mut rows = page(&[2_000_000, 2_000_000, 2_000_000]);
        rows[1] = row(2, 2_000_000, false, "größe_überprüfen");
        rows.extend((4..=10).map(|rank| row(rank, 1_000_000, false, &format!("weak{rank}"))));
        rows
    }

    fn concat(rows: &[RankedRow]) -> String {
        rows.iter().map(|row| row.markdown.as_str()).collect()
    }

    fn marker_handle(rendered: &str) -> &str {
        let after = rendered
            .split_once("with handle `")
            .expect("marker names a handle")
            .1;
        after.split_once('`').expect("handle is quoted").0
    }

    #[test]
    fn tail_start_cuts_only_a_worthwhile_suffix_of_weak_rows() {
        let m = 1_000_000;
        let mut protected_weak = page(&[2 * m, 2 * m, 2 * m, m, m, m, m, m, m, m]);
        protected_weak[6].protected = true;
        let cases: [(&str, Vec<RankedRow>, Option<usize>); 7] = [
            (
                "strong head, weak tail",
                page(&[2 * m, 2 * m, 2 * m, m, m, m, m, m, m, m]),
                Some(3),
            ),
            ("all equal", page(&[m; 10]), None),
            ("all zero", page(&[0; 10]), None),
            ("protected weak row", protected_weak, Some(7)),
            (
                "strong row after weak ones",
                page(&[2 * m, m, m, 2 * m, m, m]),
                Some(4),
            ),
            (
                "one weak trailing row",
                page(&[2 * m, 2 * m, 2 * m, 2 * m, 2 * m, m]),
                None,
            ),
            ("fewer than four rows", page(&[2 * m, m, m]), None),
        ];
        for (case, rows, expected) in cases {
            assert_eq!(low_value_tail_start(&rows), expected, "{case}");
        }
    }

    #[test]
    fn trimmed_page_keeps_rows_exact_and_restores_the_cut_rows() {
        let root = tempfile::tempdir().expect("handle root");
        let rows = weak_tail_page();
        let kept = concat(&rows[..3]);
        let cut = concat(&rows[3..]);

        let rendered = ranked_rows_md(Some(root.path()), &weak_tail_page());

        let marker = rendered
            .strip_prefix(kept.as_str())
            .expect("kept rows render byte-exact");
        let handle = marker_handle(marker);
        assert_eq!(
            marker,
            format!(
                "- _Trimmed 7 lower-ranked results (ranks 4-10, {} bytes): each scored at most \
                 half of this page's best utility 2000000. To restore them, call \
                 `tracedecay_retrieve` with handle `{handle}` (pass the same `project_selector` \
                 if this call used one)._\n",
                cut.len()
            )
        );
        let ResponseHandleLookup::Found(record) =
            retrieve_response_handle(root.path(), handle, current_timestamp())
                .expect("handle lookup")
        else {
            panic!("the marker's handle restores the cut rows");
        };
        assert_eq!(record.content, cut);
    }

    #[test]
    fn marker_bytes_survive_a_renewed_handle_record() {
        let now = current_timestamp();
        let cut = concat(&weak_tail_page()[3..]);
        let fresh_root = tempfile::tempdir().expect("fresh root");
        let renewed_root = tempfile::tempdir().expect("renewed root");
        // One record keeps most of its lifetime; the other is near expiry, so
        // rendering renews it with a later expiry.
        store_response_handle(fresh_root.path(), &cut, now - RESPONSE_HANDLE_TTL_SECS / 4)
            .expect("seed fresh record");
        store_response_handle(
            renewed_root.path(),
            &cut,
            now - RESPONSE_HANDLE_TTL_SECS + 60,
        )
        .expect("seed expiring record");

        let fresh = ranked_rows_md(Some(fresh_root.path()), &weak_tail_page());
        let renewed = ranked_rows_md(Some(renewed_root.path()), &weak_tail_page());

        let expires_at = |root: &Path| match retrieve_response_handle(
            root,
            marker_handle(&fresh),
            current_timestamp(),
        )
        .expect("handle lookup")
        {
            ResponseHandleLookup::Found(record) => record.expires_at,
            _ => panic!("record is live"),
        };
        assert!(
            expires_at(renewed_root.path()) > expires_at(fresh_root.path()),
            "the arrangement must actually renew one record"
        );
        assert_eq!(fresh, renewed);
    }

    #[test]
    fn unavailable_handle_store_fails_open_to_every_row() {
        let dir = tempfile::tempdir().expect("temp dir");
        let file_root = dir.path().join("not-a-directory");
        std::fs::write(&file_root, b"occupied").expect("regular file");
        let untrimmed = concat(&weak_tail_page());

        assert_eq!(
            ranked_rows_md(Some(&file_root), &weak_tail_page()),
            untrimmed
        );
        assert_eq!(ranked_rows_md(None, &weak_tail_page()), untrimmed);
    }
}
