# Codex SessionStart native locator — frozen draft

Only proposed copies of `crates/tracedecay-sessions/src/runtime/hosts/codex.rs` and `codex/meta.rs` were edited. The live owned files still equal their captured baselines. No Cargo commands, model calls, operator actions, commits or live apply were performed.

API:

```rust
pub fn CodexSource::find_live_session_transcript(
    &self,
    native_session: &str,
    project_root: &Path,
    deadline: std::time::Instant,
) -> TranscriptIngestResult<Option<CodexLiveSessionTranscript>>;

pub fn CodexLiveSessionTranscript::path(&self) -> &Path;
pub fn CodexLiveSessionTranscript::matches_current_source(
    &self,
    deadline: std::time::Instant,
) -> TranscriptIngestResult<bool>;
```

The origin caller owns the existing CPU permit. Use the guard path for capture; require `matches_current_source(deadline) == Ok(true)` after capture and before registration/authority. Treat every miss or error conservatively. The guard retains native file identity and a source metadata digest (Unix dev/inode/len/ctime/mtime), so replacement and same-inode header rewrite invalidate the original validation. This is locator consistency evidence, not history authority or a content hash.

Production scope: a local lock-free bounded complete walk of configured active/archive roots; canonical containment; directory/root revalidation; one no-follow regular native source; one complete leading native `session_meta` frame with explicit matching `id`/`session_id`; canonical cwd inside the admitted checkout with nested `.git` checkout rejection. Ordinary ingestion, its caches, records, rows and cursors are untouched. Reused helpers: `candidate_charge`, `path_byte_len`, `codex_corpus_identity`, `jsonl_native_file_identity`, `open_regular_read_no_follow`, and the budgeted `RawJsonlFrameReader`. The source error constructor is private to another module, so the two owned files use a local typed `ScanIo` adapter.

Availability limits are intentional and unresolved: the walk caps examined entries at 512, depth at 6, and header bytes at 64 KiB. It does not retain progress; sufficiently large corpora can miss on every Start. Any source-tree symlink, depth/byte/work limit, ambiguity, changed directory, unsafe source, incomplete/malformed header, or expired deadline denies bootstrap. Normal concurrent source append also invalidates the guard. The two configured source roots come from the existing `CodexSource::new` / `with_home` behavior; constructor/configuration semantics were not changed.

12 focused tests cover metadata-only Start without a path field, archive and session_id alias, missing/mismatched/conflicting IDs, sibling/nested/relative/missing cwd, first-complete-frame and byte bounds, cross-root duplicates after more than one exact-hook page, missing roots without creation, expired deadline, repeated over-budget discovery, excessive depth, symlink/FIFO rejection, replaced/open-source changes, and same-inode/same-size header rewrite with restored mtime.

Validation: rustfmt check and `git apply --check` passed for the integration patch, production patch, tests patch, and combined production/tests patches. Tests were authored but not executed. `pm-session-bootstrap-codex-manifest.json` records baseline/proposed/patch SHA-256 values. The integration patch is the authoritative full artifact; production and test patches are review chunks.
