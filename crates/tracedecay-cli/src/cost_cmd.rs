use serde_json::Value;
use tracedecay_session_memory::provider_usage::{
    ProviderUsageCostSummaryV1, ProviderUsageCoverageV1, ProviderUsageTaskCostSummaryV1,
};

use crate::{
    commands::daemon_tool_json,
    cost_summary::{CostSummaryPayload, TodayCostPayload},
};

#[hotpath::measure(label = "cli.cost.read", future = true)]
pub(crate) async fn handle_cost(
    range: String,
    by_model: bool,
    export: Option<String>,
) -> tracedecay_domain::errors::Result<()> {
    handle_cost_with_task(range, by_model, false, export).await
}

/// Cost command entrypoint with task-category grouping enabled by the CLI
/// parser when requested. The three-argument wrapper above stays available to
/// callers compiled against the pre-task flag surface while the parser owner
/// wires this entrypoint to `--by-task`.
#[hotpath::measure(label = "cli.cost.read.with_task", future = true)]
pub(crate) async fn handle_cost_with_task(
    range: String,
    by_model: bool,
    by_task: bool,
    export: Option<String>,
) -> tracedecay_domain::errors::Result<()> {
    let cwd = std::env::current_dir()?;
    let project_root = tracedecay::config::discover_project_root(&cwd);
    let payload = daemon_tool_json(
        project_root.as_deref(),
        "tracedecay_admin_cli",
        serde_json::json!({ "action": "cost_summary", "range": &range }),
    )
    .await?;
    if payload.get("summary").is_none_or(Value::is_null) {
        println!("Provider usage accounting is unavailable.");
        return Ok(());
    }
    let summary: CostSummaryPayload = serde_json::from_value(payload["summary"].clone())?;
    let today: TodayCostPayload = serde_json::from_value(payload["today"].clone())?;
    if summary.provider_usage.coverage == ProviderUsageCoverageV1::Unavailable {
        println!("No canonical provider usage is available for this profile.");
        return Ok(());
    }

    hotpath::measure_block!("cli.cost.render", {
        print_cost_summary(
            &today.provider_usage,
            &range,
            by_model,
            by_task,
            export.as_deref(),
            &summary,
        )
    })?;
    Ok(())
}

fn print_cost_summary(
    today: &ProviderUsageCostSummaryV1,
    range: &str,
    by_model: bool,
    by_task: bool,
    export: Option<&str>,
    summary: &CostSummaryPayload,
) -> tracedecay_domain::errors::Result<()> {
    if let Some(fmt) = export {
        print_cost_export(fmt, range, by_model, by_task, summary)?;
    } else if by_model {
        print_model_table(summary);
    } else if by_task {
        print_task_table(summary);
    } else {
        print_default_summary(today, range, summary);
    }
    Ok(())
}

fn print_cost_export(
    fmt: &str,
    range: &str,
    by_model: bool,
    by_task: bool,
    summary: &CostSummaryPayload,
) -> tracedecay_domain::errors::Result<()> {
    let usage = &summary.provider_usage;
    match fmt {
        "json" => {
            let obj = serde_json::json!({
                "range": range,
                "coverage": usage.coverage,
                "pricing_revision": usage.pricing_revision,
                "total_cost_usd": usage.total_cost_usd,
                "total_input_tokens": usage.total_input_tokens,
                "total_output_tokens": usage.total_output_tokens,
                "tokens_saved": summary.tokens_saved,
                "efficiency_ratio": summary.efficiency_ratio,
                "by_model": usage.by_model,
                "task_usage": summary.task_usage,
                "by_task": summary.task_usage.as_ref().map(|task_usage| &task_usage.by_task),
            });
            println!("{}", serde_json::to_string_pretty(&obj)?);
        }
        "csv" => print_cost_csv(summary, by_model, by_task),
        _ => eprintln!("Unknown export format '{fmt}'. Use 'json' or 'csv'."),
    }
    Ok(())
}

fn print_cost_csv(summary: &CostSummaryPayload, by_model: bool, by_task: bool) {
    let usage = &summary.provider_usage;
    if by_model {
        println!("provider,model,cost_usd,tokens");
        for model in &usage.by_model {
            let cost = model
                .cost_usd
                .map(|cost| format!("{cost:.4}"))
                .unwrap_or_else(|| "unavailable".to_owned());
            let tokens = model
                .total_tokens
                .map(|tokens| tokens.to_string())
                .unwrap_or_else(|| "unavailable".to_owned());
            println!("{},{},{cost},{tokens}", model.provider, model.model);
        }
    } else if by_task {
        let Some(task_usage) = summary.task_usage.as_ref() else {
            println!("task_category,cost_usd,tokens,usage_events");
            println!("unavailable,unavailable,unavailable,0");
            return;
        };
        println!("task_category,cost_usd,tokens,usage_events");
        for task in &task_usage.by_task {
            let category = task.task_category.as_deref().unwrap_or("unattributed");
            let cost = task
                .cost_usd
                .map(|cost| format!("{cost:.4}"))
                .unwrap_or_else(|| "unavailable".to_owned());
            let tokens = task
                .total_tokens
                .map(|tokens| tokens.to_string())
                .unwrap_or_else(|| "unavailable".to_owned());
            println!("{category},{cost},{tokens},{}", task.usage_events);
        }
        if task_usage.by_task.is_empty() {
            println!("unavailable,unavailable,unavailable,0");
        }
    } else {
        println!("total_cost_usd,input_tokens,output_tokens,tokens_saved,efficiency");
        let total_cost = usage
            .total_cost_usd
            .map(|cost| format!("{cost:.4}"))
            .unwrap_or_else(|| "unavailable".to_owned());
        let input = usage
            .total_input_tokens
            .map(|tokens| tokens.to_string())
            .unwrap_or_else(|| "unavailable".to_owned());
        let output = usage
            .total_output_tokens
            .map(|tokens| tokens.to_string())
            .unwrap_or_else(|| "unavailable".to_owned());
        let efficiency = summary
            .efficiency_ratio
            .map(|ratio| format!("{ratio:.4}"))
            .unwrap_or_else(|| "unavailable".to_owned());
        println!(
            "{total_cost},{input},{output},{},{efficiency}",
            summary.tokens_saved
        );
    }
}

fn print_model_table(summary: &CostSummaryPayload) {
    let usage = &summary.provider_usage;
    println!(
        "  {:<12} {:<24} {:>10} {:>10} {:>6}",
        "Provider", "Model", "Cost", "Tokens", "Share"
    );
    for model in &usage.by_model {
        let share = usage
            .total_cost_usd
            .zip(model.cost_usd)
            .filter(|(total, _)| *total > 0.0)
            .map(|(total, cost)| format!("{:.0}%", cost / total * 100.0))
            .unwrap_or_else(|| "n/a".to_owned());
        let token_count = model
            .total_tokens
            .map(tracedecay_runtime_core::text::format_token_count)
            .unwrap_or_else(|| "unknown".to_owned());
        let cost = model
            .cost_usd
            .map(|cost| format!("${cost:.2}"))
            .unwrap_or_else(|| "unavailable".to_owned());
        println!(
            "  {:<12} {:<24} {:>10} {:>10} {:>6}",
            model.provider, model.model, cost, token_count, share
        );
    }
}

fn print_task_table(summary: &CostSummaryPayload) {
    let Some(usage) = summary.task_usage.as_ref() else {
        println!("Task cost attribution is unavailable from current provider usage evidence.");
        return;
    };
    if usage.coverage == ProviderUsageCoverageV1::Unavailable {
        println!("Task cost attribution is unavailable from current provider usage evidence.");
        return;
    }
    println!(
        "  {:<20} {:>10} {:>10} {:>6}",
        "Task category", "Cost", "Tokens", "Share"
    );
    for task in &usage.by_task {
        let category = task.task_category.as_deref().unwrap_or("unattributed");
        let share = usage
            .total_cost_usd
            .zip(task.cost_usd)
            .filter(|(total, _)| *total > 0.0)
            .map(|(total, cost)| format!("{:.0}%", cost / total * 100.0))
            .unwrap_or_else(|| "n/a".to_owned());
        let token_count = task
            .total_tokens
            .map(tracedecay_runtime_core::text::format_token_count)
            .unwrap_or_else(|| "unknown".to_owned());
        let cost = task
            .cost_usd
            .map(|cost| format!("${cost:.2}"))
            .unwrap_or_else(|| "unavailable".to_owned());
        println!(
            "  {:<20} {:>10} {:>10} {:>6}",
            category, cost, token_count, share
        );
    }
    if usage.unattributed_events > 0 {
        println!();
        println!(
            "  {} usage event(s) have no exact task-category evidence; coverage is {}.",
            usage.unattributed_events,
            coverage_label(usage)
        );
    }
}

fn coverage_label(usage: &ProviderUsageTaskCostSummaryV1) -> &'static str {
    match usage.coverage {
        ProviderUsageCoverageV1::Complete => "complete",
        ProviderUsageCoverageV1::Partial => "partial",
        ProviderUsageCoverageV1::Unavailable => "unavailable",
    }
}

fn print_default_summary(
    today: &ProviderUsageCostSummaryV1,
    range: &str,
    summary: &CostSummaryPayload,
) {
    let usage = &summary.provider_usage;
    println!(
        "  {:<10} {:>10} {:>10} {:>10} {:>10}",
        "Period", "Cost", "Input", "Output", "Cache-hit"
    );
    print_cost_row(
        "Today",
        today.total_cost_usd,
        today.total_input_tokens,
        today.total_output_tokens,
        today.total_cache_read_tokens,
    );
    print_cost_row(
        range,
        usage.total_cost_usd,
        usage.total_input_tokens,
        usage.total_output_tokens,
        usage.total_cache_read_tokens,
    );

    if summary.tokens_saved > 0 {
        let saved = tracedecay_runtime_core::text::format_token_count(summary.tokens_saved);
        println!();
        match summary.efficiency_ratio {
            Some(ratio) => {
                println!(
                    "  Savings  {saved} tokens ({:.0}% efficiency)",
                    ratio * 100.0
                );
            }
            None => println!("  Savings  {saved} tokens (efficiency unavailable)"),
        }
    }
}

fn print_cost_row(
    label: &str,
    cost: Option<f64>,
    input: Option<u64>,
    output: Option<u64>,
    cache_read: Option<u64>,
) {
    let cache_pct = input.zip(cache_read).and_then(|(input, cache_read)| {
        let denominator = input.checked_add(cache_read)?;
        (denominator > 0).then_some((cache_read as f64 / denominator as f64) * 100.0)
    });
    let input = input
        .map(tracedecay_runtime_core::text::format_token_count)
        .unwrap_or_else(|| "unknown".to_owned());
    let output = output
        .map(tracedecay_runtime_core::text::format_token_count)
        .unwrap_or_else(|| "unknown".to_owned());
    let cost = cost
        .map(|cost| format!("${cost:.2}"))
        .unwrap_or_else(|| "unavailable".to_owned());
    let cache = cache_pct
        .map(|percent| format!("{percent:.0}%"))
        .unwrap_or_else(|| "unknown".to_owned());
    println!(
        "  {:<10} {:>10} {:>10} {:>10} {:>10}",
        label, cost, input, output, cache
    );
}
