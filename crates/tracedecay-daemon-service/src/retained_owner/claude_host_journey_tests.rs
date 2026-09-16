//! Lower-layer Claude host bundle tests.
//!
//! The production composition journey belongs at the root boundary in
//! crates/tracedecay/tests/claude_host_journey.rs: the root crate owns
//! ProductionProjectCompositionHarnessV1 and its MCP server. Keeping that
//! harness out of this daemon-service module preserves dependency direction
//! while retaining the real transcript observe, Native settlement, Claude
//! hook-route publication, and context-recall journey.
//!
//! Hook causality and the shipped Claude lifecycle binary path remain covered
//! by crates/tracedecay-cli/tests/product_memory_provider_claude_host_journey.rs.

use std::path::Path;

use serde_json::Value;
use tempfile::TempDir;

// ---------------------------------------------------------------------------
// The shipped Claude Code host bundle: the hooks this journey rides are the
// ones `tracedecay install --agent claude` deploys, and deploying them again,
// updating them, or undoing them must never disturb operator-owned state.
// ---------------------------------------------------------------------------

/// The operator's own Claude settings, which TraceDecay's staging lane has no
/// business touching.
const OPERATOR_CLAUDE_SETTINGS: &str =
    "{\n  \"model\": \"opus\",\n  \"permissions\": {\n    \"allow\": [\"Bash(ls:*)\"]\n  }\n}\n";

/// The operator's own project rules, which install must append to and undo
/// must give back.
const OPERATOR_PROJECT_RULES: &str = "# Team rules\n\nAlways run the linter before pushing.\n";

/// Stable delimiters of the TraceDecay-managed block inside a project `CLAUDE.md`.
const MANAGED_RULES_START: &str = "<!-- tracedecay:claude:start -->";
const MANAGED_RULES_END: &str = "<!-- tracedecay:claude:end -->";

/// Every file the deployed bundle holds, as `(relative path, bytes)`, sorted.
fn deployed_bundle(deploy_dir: &Path) -> Vec<(String, Vec<u8>)> {
    fn walk(base: &Path, directory: &Path, out: &mut Vec<(String, Vec<u8>)>) {
        let Ok(entries) = std::fs::read_dir(directory) else {
            return;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                walk(base, &path, out);
            } else {
                let relative = path
                    .strip_prefix(base)
                    .unwrap_or(&path)
                    .to_string_lossy()
                    .into_owned();
                out.push((relative, std::fs::read(&path).unwrap_or_default()));
            }
        }
    }
    let mut files = Vec::new();
    walk(deploy_dir, deploy_dir, &mut files);
    files.sort();
    files
}
/// The command and arguments the deployed hook manifest binds one Claude hook
/// event to, or `None` when the event is not declared at all.
fn deployed_hook_invocation(hooks: &Value, event: &str) -> Option<(String, Vec<String>)> {
    let handler = hooks
        .pointer(&format!("/hooks/{event}"))?
        .as_array()?
        .iter()
        .find_map(|entry| entry.pointer("/hooks/0"))?;
    let command = handler.get("command")?.as_str()?.to_owned();
    let arguments = handler
        .get("args")?
        .as_array()?
        .iter()
        .filter_map(|argument| argument.as_str().map(str::to_owned))
        .collect();
    Some((command, arguments))
}

/// Bundle *staging* and project-rules registration for the shipped Claude
/// Code host: staging, re-staging, and update converge on a byte-identical
/// deployed bundle, registration appends exactly one managed block to the
/// operator's own rules, and undo gives those rules back -- with the
/// operator's `settings.json` untouched throughout.
///
/// Scope, stated plainly: Claude Code owns its own activation, so this test
/// stops at the deferral boundary. It never calls
/// `activate_deployed_host_registration` or
/// `deactivate_deployed_host_registration`, and it therefore proves nothing
/// about the marketplace/plugin entries, the TraceDecay permission entry, or
/// rollback. Those are proved against a real `claude` executable by
/// `crates/tracedecay-cli/tests/host_lifecycle_cli_acceptance.rs`
/// (`claude_lifecycle_tracks_assets_only_after_native_activation`), which
/// drives install, repeated install, update, injected-failure rollback,
/// recovery, and idempotent uninstall against a real `claude` executable.
///
/// Real defect this catches: a memory integration that bolts its own hook or
/// settings entry onto the Claude host, so a second install or an undo leaves
/// the operator's `settings.json` or project rules changed behind their back.
/// The assertion that the memory journey's hooks are exactly the shipped
/// lifecycle hooks is what keeps that from being added later without notice.
#[test]
fn the_shipped_claude_bundle_stages_hooks_and_registers_project_rules_without_disturbing_operator_state()
 {
    use tracedecay_agent_hosts::agents::host_bundle::HostBundleComponentV1;
    use tracedecay_agent_hosts::agents::{
        AgentIntegration, ClaudeIntegration, InstallContext, NonInteractiveInstallOutcome,
        UpdatePluginOutcome,
    };

    let home_dir = TempDir::new().expect("claude home");
    let project_dir = TempDir::new().expect("claude project");
    // Canonicalize: the host lifecycle refuses a project path that does not
    // resolve to itself, and a temp dir is a symlink on macOS.
    let home = std::fs::canonicalize(home_dir.path()).expect("canonical home");
    let project = std::fs::canonicalize(project_dir.path()).expect("canonical project");

    let settings = home.join(".claude/settings.json");
    std::fs::create_dir_all(settings.parent().expect("settings parent")).expect("claude dir");
    std::fs::write(&settings, OPERATOR_CLAUDE_SETTINGS).expect("operator settings");
    let project_rules = project.join(".claude/CLAUDE.md");
    std::fs::create_dir_all(project_rules.parent().expect("rules parent")).expect("project dir");
    std::fs::write(&project_rules, OPERATOR_PROJECT_RULES).expect("operator rules");

    let install = InstallContext {
        home: home.clone(),
        tracedecay_bin: "/opt/tracedecay/bin/tracedecay".to_owned(),
        tool_permissions: Vec::new(),
        project_root: None,
        dashboard: false,
    };
    let integration = ClaudeIntegration;
    let components: &[HostBundleComponentV1] = &[];

    // Install stages the bundle that carries the Claude lifecycle hooks.
    let NonInteractiveInstallOutcome::DeferredUserAction(staged) = integration
        .prepare_non_interactive_install(&install)
        .expect("staging the Claude bundle")
    else {
        panic!("Claude Code owns its own activation, so a fresh install defers to its CLI");
    };
    let deploy_dir = staged
        .staged_paths
        .first()
        .cloned()
        .expect("the deferral must name the staged bundle root");
    let installed = deployed_bundle(&deploy_dir);
    assert!(
        !installed.is_empty(),
        "staging must deploy the bundle to {}",
        deploy_dir.display()
    );

    // The hooks this memory journey rides are the shipped lifecycle hooks,
    // bound to the pinned binary -- and there is no memory-specific hook
    // beside them.
    let hooks: Value = serde_json::from_slice(
        &std::fs::read(deploy_dir.join("hooks/hooks.json")).expect("deployed hook manifest"),
    )
    .expect("the deployed hook manifest must be JSON");
    for (event, argument) in [
        ("SessionStart", "hook-claude-session-start"),
        ("Stop", "hook-stop"),
        ("PostToolUse", "hook-claude-post-tool-use"),
    ] {
        let (command, arguments) = deployed_hook_invocation(&hooks, event)
            .unwrap_or_else(|| panic!("the bundle must declare the {event} hook: {hooks}"));
        assert_eq!(
            command, install.tracedecay_bin,
            "the {event} hook must run the pinned binary: {hooks}"
        );
        assert_eq!(
            arguments,
            vec![argument.to_owned()],
            "the {event} hook must invoke the shipped handler: {hooks}"
        );
    }
    let declared_events: Vec<String> = hooks["hooks"]
        .as_object()
        .expect("hook manifest events")
        .keys()
        .cloned()
        .collect();
    assert!(
        declared_events
            .iter()
            .all(|event| !event.to_ascii_lowercase().contains("memory")),
        "the memory journey must ride the shipped lifecycle hooks, not its own: {declared_events:?}"
    );

    // Re-installing converges: not one byte of the deployed bundle changes.
    integration
        .prepare_non_interactive_install(&install)
        .expect("re-staging the Claude bundle");
    assert_eq!(
        deployed_bundle(&deploy_dir),
        installed,
        "re-installing must leave the deployed bundle byte-identical"
    );

    // So does an update against the same version.
    let UpdatePluginOutcome::DeferredUserAction(_) = integration
        .update_plugin(&install)
        .expect("updating the Claude bundle")
    else {
        panic!("Claude Code owns its own cache, so an update defers activation to its CLI");
    };
    assert_eq!(
        deployed_bundle(&deploy_dir),
        installed,
        "an update against the same version must leave the deployed bundle byte-identical"
    );
    assert_eq!(
        std::fs::read_to_string(&settings).expect("settings after staging"),
        OPERATOR_CLAUDE_SETTINGS,
        "staging the bundle must not touch the operator's own Claude settings"
    );

    // Project registration appends the managed block to operator rules, and
    // registering again converges instead of appending a second copy.
    integration
        .activate_project_host_component_registration(components, &install, &project)
        .expect("registering the project host component");
    let registered = std::fs::read_to_string(&project_rules).expect("registered rules");
    assert!(
        registered.starts_with(OPERATOR_PROJECT_RULES),
        "registration must append to the operator's own rules: {registered}"
    );
    for marker in [MANAGED_RULES_START, MANAGED_RULES_END] {
        assert_eq!(
            registered.matches(marker).count(),
            1,
            "registration must write exactly one managed block delimiter: {registered}"
        );
    }
    assert!(
        registered.find(MANAGED_RULES_START).unwrap() < registered.find(MANAGED_RULES_END).unwrap(),
        "registration must order the managed block delimiters: {registered}"
    );
    integration
        .activate_project_host_component_registration(components, &install, &project)
        .expect("re-registering the project host component");
    assert_eq!(
        std::fs::read_to_string(&project_rules).expect("re-registered rules"),
        registered,
        "re-registering must converge on the same file"
    );

    // Undo gives the operator their own rules back, and is itself idempotent.
    integration
        .deactivate_project_host_component_registration(components, &install, &project)
        .expect("deregistering the project host component");
    let undone = std::fs::read_to_string(&project_rules).expect("rules after undo");
    assert!(
        !undone.contains(MANAGED_RULES_START) && !undone.contains(MANAGED_RULES_END),
        "undo must remove the managed block: {undone}"
    );
    assert!(
        undone.contains(OPERATOR_PROJECT_RULES.trim()),
        "undo must preserve the operator's own rules verbatim: {undone}"
    );
    integration
        .deactivate_project_host_component_registration(components, &install, &project)
        .expect("deregistering twice");
    assert_eq!(
        std::fs::read_to_string(&project_rules).expect("rules after second undo"),
        undone,
        "a second undo must change nothing"
    );
    assert_eq!(
        std::fs::read_to_string(&settings).expect("settings after undo"),
        OPERATOR_CLAUDE_SETTINGS,
        "undo must not touch the operator's own Claude settings"
    );
}
