use std::path::{Component, Path, PathBuf};
use std::sync::Arc;

use serde_json::{Value, json};

use super::read_modes::{
    self, LineRange, ReadMode, render_full, render_lines, render_map, render_signatures,
    render_symbol_context,
};
use tracedecay_code_index::graph_projection::CodeGraphInteractiveReader;
use tracedecay_contracts::retrieval::SourceReadBodyPolicyV1;
use tracedecay_domain::errors::{Result, TraceDecayError};
use tracedecay_graph_db::GraphCancellation;
use tracedecay_runtime_core::db::Database;
use tracedecay_runtime_core::storage::ProjectPath;
use tracedecay_session_memory::context::read_cache::{self, GLOBAL_SESSION};

pub struct SourceReadRequest<'a> {
    pub file: &'a str,
    pub mode: ReadMode,
    pub body_policy: SourceReadBodyPolicyV1,
    pub line_range: Option<LineRange>,
    pub raw_lines: Option<&'a str>,
    pub include_symbols: bool,
    pub project_id: &'a str,
}

pub struct SourceReadOutput {
    pub file: String,
    pub mode: ReadMode,
    pub mtime_ns: i64,
    pub digest: String,
    pub token_count: u32,
    pub unchanged: bool,
    pub body: Option<String>,
    pub context: Option<Value>,
}

#[hotpath::measure(label = "usecases.context.read_source", future = true)]
pub async fn read_source(
    project_root: &Path,
    database: &Database,
    read_only: bool,
    reader: &CodeGraphInteractiveReader,
    cancellation: Arc<dyn GraphCancellation>,
    request: SourceReadRequest<'_>,
) -> Result<SourceReadOutput> {
    let SourceReadRequest {
        file,
        mode,
        body_policy,
        line_range,
        raw_lines,
        include_symbols,
        project_id,
    } = request;
    if body_policy == SourceReadBodyPolicyV1::Required
        && !matches!(mode, ReadMode::Full | ReadMode::Lines)
    {
        return Err(TraceDecayError::Config {
            message: "body_policy='required' is supported only for full and lines reads".to_owned(),
        });
    }
    let (absolute_path, display_file) =
        resolve_indexed_source_file(project_root, reader, Arc::clone(&cancellation), file)?;
    let metadata_before = if body_policy == SourceReadBodyPolicyV1::Required {
        Some(SourceReadMetadata::capture(&absolute_path, file)?)
    } else {
        None
    };
    let mtime_ns =
        read_cache::file_mtime_ns(&absolute_path).map_err(|error| TraceDecayError::Config {
            message: format!("cannot read file metadata for '{file}': {error}"),
        })?;
    let last_sync_at = if matches!(mode, ReadMode::Map | ReadMode::Signatures) {
        database.get_metadata("last_sync_at").await?
    } else {
        None
    };
    let args_hash = read_cache::args_hash(&json!({
        "lines": raw_lines,
        "last_sync_at": last_sync_at,
    }))?;

    let cache_connection = database.read_connection();
    if body_policy == SourceReadBodyPolicyV1::IfChanged
        && let Some(cached) = read_cache::get(
        &cache_connection,
        project_id,
        GLOBAL_SESSION,
        &display_file,
        mode.as_str(),
        &args_hash,
        mtime_ns,
    )
    .await?
    {
        return Ok(SourceReadOutput {
            context: source_symbol_context(
                reader,
                Arc::clone(&cancellation),
                &display_file,
                mode,
                line_range,
                include_symbols,
            )?,
            file: display_file,
            mode,
            mtime_ns: cached.mtime_ns,
            digest: cached.digest,
            token_count: cached.token_count,
            unchanged: true,
            body: None,
        });
    }

    let body = hotpath::measure_block!(
        "usecases.context.source_read.render",
        match mode {
            ReadMode::Full => render_full(
                &tracedecay_runtime_core::sync::read_source_file(&absolute_path).map_err(
                    |error| TraceDecayError::Config {
                        message: format!("cannot read '{file}': {error}"),
                    },
                )?,
            ),
            ReadMode::Lines => render_lines(
                &tracedecay_runtime_core::sync::read_source_file(&absolute_path).map_err(
                    |error| TraceDecayError::Config {
                        message: format!("cannot read '{file}': {error}"),
                    },
                )?,
                line_range.ok_or_else(|| TraceDecayError::Config {
                    message: "lines mode requires a parsed range".to_owned(),
                })?,
            ),
            ReadMode::Map => serde_json::to_string_pretty(&render_map(
                reader,
                Arc::clone(&cancellation),
                &display_file,
                None,
            )?)
            .map_err(|error| TraceDecayError::Config {
                message: format!("cannot render map for '{file}': {error}"),
            })?,
            ReadMode::Signatures => serde_json::to_string_pretty(&render_signatures(
                reader,
                Arc::clone(&cancellation),
                &display_file,
            )?)
            .map_err(|error| TraceDecayError::Config {
                message: format!("cannot render signatures for '{file}': {error}"),
            })?,
        }
    );
    if let Some(metadata_before) = metadata_before {
        metadata_before.ensure_unchanged(&absolute_path, file)?;
    }
    hotpath::gauge!("usecases.context.source_read.bytes").inc(body.len() as f64);
    let context = source_symbol_context(
        reader,
        cancellation,
        &display_file,
        mode,
        line_range,
        include_symbols,
    )?;
    let token_count = read_modes::estimate_tokens(&body);
    let digest = read_cache::digest_bytes(body.as_bytes());
    if !read_only {
        read_cache::put(
            database,
            project_id,
            GLOBAL_SESSION,
            &display_file,
            mtime_ns,
            mode.as_str(),
            &args_hash,
            &digest,
            body.as_bytes(),
            token_count,
        )
        .await?;
    }
    Ok(SourceReadOutput {
        file: display_file,
        mode,
        mtime_ns,
        digest,
        token_count,
        unchanged: false,
        body: Some(body),
        context,
    })
}

/// Detects metadata changes during a required read. This is not an immutable
/// filesystem snapshot: the existing synchronous decoder and authority apply.
#[derive(PartialEq, Eq)]
struct SourceReadMetadata {
    length: u64,
    modified: std::time::SystemTime,
    created: Option<std::time::SystemTime>,
    #[cfg(unix)]
    identity_and_change: (u64, u64, i64, i64),
}

impl SourceReadMetadata {
    fn capture(path: &Path, file: &str) -> Result<Self> {
        let read = || -> std::io::Result<Self> {
            let metadata = std::fs::metadata(path)?;
            #[cfg(unix)]
            use std::os::unix::fs::MetadataExt;
            Ok(Self {
                length: metadata.len(),
                modified: metadata.modified()?,
                created: metadata.created().ok(),
                #[cfg(unix)]
                identity_and_change: (
                    metadata.dev(),
                    metadata.ino(),
                    metadata.ctime(),
                    metadata.ctime_nsec(),
                ),
            })
        };
        read().map_err(|error| TraceDecayError::Config {
            message: format!("cannot read file metadata for '{file}': {error}"),
        })
    }

    fn ensure_unchanged(&self, path: &Path, file: &str) -> Result<()> {
        if self != &Self::capture(path, file)? {
            return Err(TraceDecayError::Config {
                message: format!("source changed during required read: '{file}'"),
            });
        }
        Ok(())
    }
}

#[hotpath::measure(label = "usecases.context.resolve_indexed_source")]
pub fn resolve_indexed_source_file(
    project_root: &Path,
    reader: &CodeGraphInteractiveReader,
    cancellation: Arc<dyn GraphCancellation>,
    file: &str,
) -> Result<(PathBuf, String)> {
    if file.contains('\0') {
        return Err(TraceDecayError::Config {
            message: "path contains NUL byte".to_owned(),
        });
    }

    let input = Path::new(file);
    if let Ok(project_path) = ProjectPath::resolve(project_root, input) {
        let display_file = match relative_source_key(input)? {
            Some(relative) => relative,
            None => project_path.relative_path_string(),
        };
        return Ok((project_path.absolute_path(), display_file));
    }

    let display_file = if let Some(relative) = relative_source_key(input)? {
        relative
    } else if let Some(relative) = absolute_source_key(project_root, input)? {
        relative
    } else {
        return Err(TraceDecayError::Config {
            message: format!(
                "path '{}' escapes project root '{}'",
                input.display(),
                project_root.display()
            ),
        });
    };
    if reader
        .symbols_in_logical_file(&display_file, 1, cancellation)
        .map_err(|error| {
            crate::map_code_graph_read_runtime_error(crate::map_projection_error(error))
        })?
        .is_empty()
    {
        return Err(TraceDecayError::Config {
            message: format!(
                "path '{}' escapes project root '{}' and is not indexed",
                input.display(),
                project_root.display()
            ),
        });
    }
    let absolute_path = if input.is_absolute() {
        input.to_path_buf()
    } else {
        project_root.join(input)
    };
    Ok((absolute_path, display_file))
}

fn source_symbol_context(
    reader: &CodeGraphInteractiveReader,
    cancellation: Arc<dyn GraphCancellation>,
    display_file: &str,
    mode: ReadMode,
    line_range: Option<LineRange>,
    include_symbols: bool,
) -> Result<Option<Value>> {
    if !include_symbols || !matches!(mode, ReadMode::Full | ReadMode::Lines) {
        return Ok(None);
    }
    hotpath::measure_block!(
        "usecases.context.source_read.symbol_context",
        render_symbol_context(reader, cancellation, display_file, line_range).map(Some)
    )
}

fn relative_source_key(path: &Path) -> Result<Option<String>> {
    if path.is_absolute() {
        return Ok(None);
    }
    let mut parts = Vec::new();
    for component in path.components() {
        match component {
            Component::Normal(part) => parts.push(part.to_string_lossy().to_string()),
            _ => {
                return Err(TraceDecayError::Config {
                    message: format!("path '{}' contains unsafe components", path.display()),
                });
            }
        }
    }
    if parts.is_empty() {
        return Err(TraceDecayError::Config {
            message: "path must name a project file".to_owned(),
        });
    }
    Ok(Some(parts.join("/")))
}

fn absolute_source_key(project_root: &Path, path: &Path) -> Result<Option<String>> {
    if !path.is_absolute() {
        return Ok(None);
    }
    let Ok(relative) = path.strip_prefix(project_root) else {
        return Ok(None);
    };
    relative_source_key(relative)
}

#[cfg(test)]
mod required_metadata_tests {
    use super::SourceReadMetadata;

    #[test]
    fn required_source_read_refuses_metadata_change_around_the_existing_decoder() {
        let root = tempfile::tempdir().expect("project");
        let path = root.path().join("source.rs");
        std::fs::write(&path, "fn before() {}\n").expect("source");
        let before = SourceReadMetadata::capture(&path, "source.rs").expect("metadata before");
        let body = tracedecay_runtime_core::sync::read_source_file(&path).expect("existing reader");
        assert_eq!(body, "fn before() {}\n");
        before.ensure_unchanged(&path, "source.rs").expect("unchanged metadata");
        std::fs::write(&path, "fn after_with_a_new_length() {}\n").expect("concurrent change");
        let error = before.ensure_unchanged(&path, "source.rs").expect_err("changed read must refuse");
        assert!(error.to_string().contains("source changed during required read"));
    }

    #[cfg(unix)]
    #[test]
    fn required_source_read_refuses_replacement_with_reused_mtime_and_length() {
        let root = tempfile::tempdir().expect("project");
        let path = root.path().join("source.rs");
        std::fs::write(&path, "before").expect("source");
        let mtime = std::fs::metadata(&path).unwrap().modified().unwrap();
        let before = SourceReadMetadata::capture(&path, "source.rs").expect("metadata before");
        let replacement = root.path().join("replacement.rs");
        std::fs::write(&replacement, "after!").expect("replacement");
        std::fs::File::options().write(true).open(&replacement).unwrap()
            .set_times(std::fs::FileTimes::new().set_modified(mtime)).expect("same mtime");
        std::fs::rename(&replacement, &path).expect("atomic replacement");
        let error = before.ensure_unchanged(&path, "source.rs").expect_err("identity change must refuse");
        assert!(error.to_string().contains("source changed during required read"));
    }
}
