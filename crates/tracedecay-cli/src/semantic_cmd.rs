use std::path::{Path, PathBuf};

use serde_json::{Value, json};
use tracedecay_domain::configuration::{ConfigurationValueV1, SEMANTIC_RUNTIME_SETTING_KEY_V2};
use tracedecay_domain::errors::{Result, TraceDecayError};
use tracedecay_semantic_contracts::{ModelArtifactManifestV1, SemanticConfig};

use crate::cli::{SemanticAction, SemanticProjectArgs};
use crate::{commands, resolve_cli_project_root};

const ADMIN_PROJECT_TOOL: &str = "tracedecay_admin_project";

pub(crate) async fn handle_semantic_action(action: SemanticAction) -> Result<()> {
    match action {
        SemanticAction::Status { project, json } => {
            let scope = resolve_scope(project).await?;
            let configuration = load_semantic_config(&scope.project_path).await?;
            let lifecycle =
                semantic_admin_action(&scope.project_path, json!({ "action": "semantic_status" }))
                    .await?;
            render_status(&configuration, &lifecycle, json)?;
        }
        SemanticAction::Enable {
            project,
            auto_download,
            json,
        } => {
            let scope = resolve_scope(project).await?;
            let expected_revision =
                commands::current_configuration_revision(&scope.project_path).await?;
            let current = load_semantic_config(&scope.project_path).await?;
            let mut desired = current.clone();
            desired.enabled = true;
            desired.auto_download = auto_download;
            store_semantic_config(&scope, expected_revision, &current, &desired).await?;
            let restart_required =
                commands::current_configuration_restart_required(&scope.project_path).await?;
            render_configuration_change(&desired, json, restart_required)?;
            report_restart_requirement(restart_required, Some(auto_download));
        }
        SemanticAction::Disable { project, json } => {
            let scope = resolve_scope(project).await?;
            let expected_revision =
                commands::current_configuration_revision(&scope.project_path).await?;
            let current = load_semantic_config(&scope.project_path).await?;
            let mut desired = current.clone();
            desired.enabled = false;
            desired.auto_download = false;
            store_semantic_config(&scope, expected_revision, &current, &desired).await?;
            let restart_required =
                commands::current_configuration_restart_required(&scope.project_path).await?;
            render_configuration_change(&desired, json, restart_required)?;
            report_restart_requirement(restart_required, None);
        }
        SemanticAction::Acquire { project, json } => {
            let scope = resolve_scope(project).await?;
            require_semantic_enabled(&scope.project_path).await?;
            let response =
                semantic_admin_action(&scope.project_path, json!({ "action": "semantic_acquire" }))
                    .await?;
            render_admin_response(&response, json)?;
        }
        SemanticAction::Import {
            project,
            manifest,
            source,
            json,
        } => {
            let scope = resolve_scope(project).await?;
            require_semantic_enabled(&scope.project_path).await?;
            let manifest = load_manifest(&manifest)?;
            let source = canonical_source_directory(&source)?;
            let response = semantic_admin_action(
                &scope.project_path,
                semantic_import_args(&manifest, &source),
            )
            .await?;
            render_admin_response(&response, json)?;
        }
    }
    Ok(())
}

async fn resolve_scope(project: SemanticProjectArgs) -> Result<commands::ResolvedCliScope> {
    let requested =
        resolve_cli_project_root(project.path, project.project_id, project.project_path).await?;
    commands::resolve_project_scope(requested).await
}

async fn load_semantic_config(project_path: &Path) -> Result<SemanticConfig> {
    decode_semantic_config(
        commands::current_project_setting(project_path, SEMANTIC_RUNTIME_SETTING_KEY_V2).await?,
    )
}

async fn require_semantic_enabled(project_path: &Path) -> Result<()> {
    if !load_semantic_config(project_path).await?.enabled {
        return Err(config_error(
            "semantic runtime is disabled in project configuration; enable it and restart the daemon before managing its model",
        ));
    }
    Ok(())
}

fn decode_semantic_config(value: ConfigurationValueV1) -> Result<SemanticConfig> {
    let ConfigurationValueV1::Text(document) = value else {
        return Err(config_error(format!(
            "semantic setting '{SEMANTIC_RUNTIME_SETTING_KEY_V2}' is not text"
        )));
    };
    let configuration: SemanticConfig = serde_json::from_str(&document).map_err(|error| {
        config_error(format!(
            "semantic setting '{SEMANTIC_RUNTIME_SETTING_KEY_V2}' is invalid: {error}"
        ))
    })?;
    configuration.validate()?;
    Ok(configuration)
}

async fn store_semantic_config(
    scope: &commands::ResolvedCliScope,
    expected_revision: tracedecay_domain::configuration::ConfigurationRevisionId,
    current: &SemanticConfig,
    desired: &SemanticConfig,
) -> Result<()> {
    desired.validate()?;
    let mutations = if current == desired {
        Vec::new()
    } else {
        let bytes = tracedecay_domain::canonical_json_bytes(desired)
            .map_err(|error| config_error(format!("encode semantic configuration: {error}")))?;
        let document = String::from_utf8(bytes).map_err(|error| {
            config_error(format!("semantic configuration is not UTF-8: {error}"))
        })?;
        vec![commands::project_configuration_set(
            &scope.project_id,
            SEMANTIC_RUNTIME_SETTING_KEY_V2,
            ConfigurationValueV1::Text(document),
        )?]
    };
    let receipt = commands::mutate_project_configuration(
        &scope.project_path,
        &scope.project_id,
        expected_revision,
        mutations,
    )
    .await?;
    commands::report_configuration_receipt(receipt.as_ref());
    Ok(())
}

async fn semantic_admin_action(project_path: &Path, args: Value) -> Result<Value> {
    commands::daemon_tool_json(Some(project_path), ADMIN_PROJECT_TOOL, args).await
}

fn load_manifest(path: &Path) -> Result<ModelArtifactManifestV1> {
    let bytes = std::fs::read(path).map_err(|error| {
        config_error(format!(
            "cannot read semantic model manifest '{}': {error}",
            path.display()
        ))
    })?;
    ModelArtifactManifestV1::parse(&bytes).map_err(|error| {
        config_error(format!(
            "semantic model manifest '{}' is invalid: {error}",
            path.display()
        ))
    })
}

fn canonical_source_directory(path: &Path) -> Result<PathBuf> {
    let source = std::fs::canonicalize(path).map_err(|error| {
        config_error(format!(
            "cannot resolve semantic artifact source '{}': {error}",
            path.display()
        ))
    })?;
    if !source.is_dir() {
        return Err(config_error(format!(
            "semantic artifact source '{}' is not a directory",
            source.display()
        )));
    }
    Ok(source)
}

fn semantic_import_args(manifest: &ModelArtifactManifestV1, source: &Path) -> Value {
    json!({
        "action": "semantic_import",
        "manifest": manifest,
        "source": source,
    })
}

fn render_configuration(configuration: &SemanticConfig, json_output: bool) -> Result<()> {
    if json_output {
        println!("{}", serde_json::to_string_pretty(configuration)?);
    } else {
        println!(
            "Configured semantic: {}",
            if configuration.enabled {
                "enabled"
            } else {
                "disabled"
            }
        );
        println!(
            "Model: {}",
            configuration.effective_model_id().unwrap_or("none")
        );
        println!(
            "Automatic acquisition: {}",
            if configuration.auto_download {
                "enabled"
            } else {
                "disabled"
            }
        );
    }
    Ok(())
}

fn render_configuration_change(
    configuration: &SemanticConfig,
    json_output: bool,
    restart_required: bool,
) -> Result<()> {
    if json_output {
        println!(
            "{}",
            serde_json::to_string_pretty(&json!({
                "configuration": configuration,
                "restart_required": restart_required,
            }))?
        );
        return Ok(());
    }
    render_configuration(configuration, false)
}

fn render_status(
    configuration: &SemanticConfig,
    response: &Value,
    json_output: bool,
) -> Result<()> {
    let (lifecycle, runtime) = semantic_status_observation(response)?;
    if json_output {
        println!(
            "{}",
            serde_json::to_string_pretty(&json!({
                "configuration": configuration,
                "observed_runtime": {
                    "model_lifecycle": lifecycle,
                    "semantic_index": runtime,
                },
            }))?
        );
        return Ok(());
    }
    render_configuration(configuration, false)?;
    match lifecycle {
        Some(lifecycle) => println!(
            "Observed model lifecycle: {}",
            serde_json::to_string_pretty(lifecycle)?
        ),
        None => println!("Observed model lifecycle: not mounted"),
    }
    match runtime {
        Some(runtime) => println!(
            "Observed semantic index: {}",
            serde_json::to_string_pretty(runtime)?
        ),
        None => println!("Observed semantic index: not mounted"),
    }
    Ok(())
}

fn semantic_status_observation(response: &Value) -> Result<(Option<&Value>, Option<&Value>)> {
    if response.get("outcome").and_then(Value::as_str) != Some("status") {
        return Err(config_error(
            "semantic status returned an unexpected administrative outcome",
        ));
    }
    let response = response
        .as_object()
        .ok_or_else(|| config_error("semantic status response is not an object"))?;
    let lifecycle = response
        .get("lifecycle")
        .map(|lifecycle| (!lifecycle.is_null()).then_some(lifecycle))
        .ok_or_else(|| config_error("semantic status omitted its lifecycle observation"))?;
    let runtime = response
        .get("runtime")
        .map(|runtime| (!runtime.is_null()).then_some(runtime))
        .ok_or_else(|| config_error("semantic status omitted its runtime observation"))?;
    Ok((lifecycle, runtime))
}

fn render_admin_response(response: &Value, _json_output: bool) -> Result<()> {
    println!("{}", serde_json::to_string_pretty(response)?);
    Ok(())
}

fn report_restart_requirement(changed: bool, auto_download: Option<bool>) {
    if !changed {
        return;
    }
    eprintln!(
        "Restart the daemon serving this profile before the semantic runtime change takes effect."
    );
    if matches!(auto_download, Some(false)) {
        eprintln!(
            "After restart, run `tracedecay semantic acquire` or import a verified local artifact."
        );
    }
}

fn config_error(message: impl Into<String>) -> TraceDecayError {
    TraceDecayError::Config {
        message: message.into(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn semantic_config_decoder_rejects_the_released_v1_shape() {
        let old = ConfigurationValueV1::Text(
            r#"{"selected_model":"JinaEmbeddingsV2BaseCode","auto_download":true,"active_profile":null,"rollback_profile":null}"#
                .to_owned(),
        );
        assert!(decode_semantic_config(old).is_err());
    }

    #[test]
    fn semantic_import_rejects_a_non_directory_source_before_rpc() {
        let root = tempfile::tempdir().expect("fixture root");
        let file = root.path().join("model.onnx");
        std::fs::write(&file, b"fixture").expect("fixture file");
        assert!(canonical_source_directory(&file).is_err());
    }

    #[test]
    fn semantic_status_distinguishes_an_unmounted_runtime() {
        let response = json!({ "outcome": "status", "lifecycle": null, "runtime": null });
        assert_eq!(
            semantic_status_observation(&response).unwrap(),
            (None, None)
        );

        let mounted = json!({
            "outcome": "status",
            "lifecycle": { "state": "ready" },
            "runtime": { "state": "indexing" }
        });
        let (lifecycle, runtime) = semantic_status_observation(&mounted).unwrap();
        assert_eq!(
            lifecycle
                .and_then(|lifecycle| lifecycle.get("state"))
                .and_then(Value::as_str),
            Some("ready")
        );
        assert_eq!(
            runtime
                .and_then(|runtime| runtime.get("state"))
                .and_then(Value::as_str),
            Some("indexing")
        );
    }
}
