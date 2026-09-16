use clap::{Subcommand, ValueEnum};

#[allow(clippy::large_enum_variant)]
#[derive(Subcommand)]
pub enum AutomationAction {
    /// Read or mutate canonical project automation settings.
    Config {
        #[command(subcommand)]
        action: AutomationConfigAction,
    },
    /// Run one typed self-improvement automation task.
    Run {
        #[command(subcommand)]
        action: AutomationRunAction,
    },
    /// Inspect automation run history.
    Runs {
        #[command(subcommand)]
        action: AutomationRunsAction,
    },
    /// Manage profile-owned automation skills.
    Skills {
        #[command(subcommand)]
        action: AutomationSkillsAction,
    },
    /// Inspect terminal automatic fact receipts.
    Facts {
        #[command(subcommand)]
        action: AutomationFactsAction,
    },
}

#[allow(clippy::large_enum_variant)]
#[derive(Subcommand)]
pub enum AutomationRunAction {
    /// Run the autonomous memory curator against bounded canonical facts.
    #[command(name = "memory-curation")]
    MemoryCuration {
        /// Maximum number of canonical facts included in the bounded review.
        #[arg(
            long,
            default_value_t = tracedecay_automation_runtime::automation::runner::CURATION_DEFAULT_FACT_REVIEW_LIMIT
        )]
        fact_review_limit: usize,
        /// Confidence floor below which backend operations are rejected.
        #[arg(
            long,
            default_value_t = tracedecay_automation_runtime::automation::runner::CURATION_DEFAULT_MIN_CONFIDENCE
        )]
        min_confidence: f64,
        /// Project path (default: current directory, with discovery).
        #[arg(short, long)]
        path: Option<String>,
    },
    /// Run the bounded session reflector against canonical session evidence.
    #[command(name = "session-reflection")]
    SessionReflection {
        /// LCM provider to inspect.
        #[arg(long, default_value = "cursor")]
        provider: String,
        /// Bounded evidence query.
        #[arg(long, default_value = "remember prefer decision requirement workflow")]
        query: String,
        /// Maximum evidence snippets included in the review request.
        #[arg(long, default_value_t = 20)]
        evidence_limit: usize,
        /// LCM evidence scope: all, session, or current.
        #[arg(long, default_value = "all")]
        scope: String,
        /// Provider-local session id filter.
        #[arg(long)]
        session_id: Option<String>,
        /// Include bounded summary nodes in the evidence request.
        #[arg(long, default_value_t = true, action = clap::ArgAction::Set)]
        include_summaries: bool,
        /// Include bounded turn slices from recently active sessions.
        #[arg(long, default_value_t = true, action = clap::ArgAction::Set)]
        include_recent_sessions: bool,
        /// Number of recently active sessions to include.
        #[arg(long, default_value_t = 3)]
        recent_sessions_limit: usize,
        /// LCM evidence sort: recency, relevance, or hybrid.
        #[arg(long, default_value = "recency")]
        sort: String,
        /// Optional raw-message source filter.
        #[arg(long)]
        source: Option<String>,
        /// Optional raw-message role filter.
        #[arg(long)]
        role: Option<String>,
        /// Inclusive minimum raw-message timestamp in microseconds.
        #[arg(long)]
        start_time: Option<i64>,
        /// Inclusive maximum raw-message timestamp in microseconds.
        #[arg(long)]
        end_time: Option<i64>,
        /// Project path (default: current directory, with discovery).
        #[arg(short, long)]
        path: Option<String>,
    },
    /// Run the bounded managed-skill writer against canonical session evidence.
    #[command(name = "skill-writing")]
    SkillWriting {
        /// LCM provider to inspect. Use all for unified evidence.
        #[arg(long, default_value = "all")]
        provider: String,
        /// Bounded evidence query.
        #[arg(
            long,
            default_value = "workflow correction repeated skill tool pattern"
        )]
        query: String,
        /// Maximum evidence snippets included in the review request.
        #[arg(long, default_value_t = 20)]
        evidence_limit: usize,
        /// Include bounded turn slices from recently active sessions.
        #[arg(long, default_value_t = true, action = clap::ArgAction::Set)]
        include_recent_sessions: bool,
        /// Number of recently active sessions to include.
        #[arg(long, default_value_t = 3)]
        recent_sessions_limit: usize,
        /// Project path (default: current directory, with discovery).
        #[arg(short, long)]
        path: Option<String>,
    },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, ValueEnum)]
pub enum AutomationConfigScope {
    Project,
    Global,
}

#[allow(clippy::large_enum_variant)]
#[derive(Subcommand)]
pub enum AutomationConfigAction {
    /// Print effective automation config.
    Get {
        /// Config scope to inspect.
        #[arg(long, value_enum, default_value_t = AutomationConfigScope::Project)]
        scope: AutomationConfigScope,
        /// Output as JSON.
        #[arg(long)]
        json: bool,
        /// Project path (default: current directory, with discovery).
        #[arg(short, long)]
        path: Option<String>,
    },
    /// Explain effective automation config, merge source, and backend availability.
    Explain {
        /// Config scope to inspect.
        #[arg(long, value_enum, default_value_t = AutomationConfigScope::Project)]
        scope: AutomationConfigScope,
        /// Output as JSON.
        #[arg(long)]
        json: bool,
        /// Project path (default: current directory, with discovery).
        #[arg(short, long)]
        path: Option<String>,
    },
    /// Enable project automation.
    Enable {
        /// Config scope to mutate.
        #[arg(long, value_enum, default_value_t = AutomationConfigScope::Project)]
        scope: AutomationConfigScope,
        /// Project path (default: current directory, with discovery).
        #[arg(short, long)]
        path: Option<String>,
    },
    /// Disable project automation.
    Disable {
        /// Config scope to mutate.
        #[arg(long, value_enum, default_value_t = AutomationConfigScope::Project)]
        scope: AutomationConfigScope,
        /// Project path (default: current directory, with discovery).
        #[arg(short, long)]
        path: Option<String>,
    },
    /// Patch project automation config fields.
    Set {
        /// Config scope to mutate.
        #[arg(long, value_enum, default_value_t = AutomationConfigScope::Project)]
        scope: AutomationConfigScope,
        /// Backend: disabled, codex-app-server.
        #[arg(long)]
        backend: Option<String>,
        /// Host mode: standalone, delegated-host.
        #[arg(long)]
        host_mode: Option<String>,
        /// Timeout in seconds.
        #[arg(long)]
        timeout_secs: Option<u64>,
        /// Scheduler polling cadence in seconds.
        #[arg(long)]
        scheduler_tick_secs: Option<u64>,
        /// Enable or disable the memory curator task.
        #[arg(long)]
        memory_curator: Option<bool>,
        /// Schedule label for the memory curator task. Empty string clears it.
        #[arg(long)]
        memory_curator_schedule: Option<String>,
        /// Memory curator interval seconds. Empty string clears it.
        #[arg(long)]
        memory_curator_interval_secs: Option<String>,
        /// Memory curator cooldown seconds. Empty string clears it.
        #[arg(long)]
        memory_curator_cooldown_secs: Option<String>,
        /// Memory curator idle seconds. Empty string clears it.
        #[arg(long)]
        memory_curator_min_idle_secs: Option<String>,
        /// Memory curator stale-lock seconds. Empty string clears it.
        #[arg(long)]
        memory_curator_stale_lock_secs: Option<String>,
        /// Enable or disable the session reflector task.
        #[arg(long)]
        session_reflector: Option<bool>,
        /// Schedule label for the session reflector task. Empty string clears it.
        #[arg(long)]
        session_reflector_schedule: Option<String>,
        /// Session reflector interval seconds. Empty string clears it.
        #[arg(long)]
        session_reflector_interval_secs: Option<String>,
        /// Session reflector cooldown seconds. Empty string clears it.
        #[arg(long)]
        session_reflector_cooldown_secs: Option<String>,
        /// Session reflector idle seconds. Empty string clears it.
        #[arg(long)]
        session_reflector_min_idle_secs: Option<String>,
        /// Session reflector stale-lock seconds. Empty string clears it.
        #[arg(long)]
        session_reflector_stale_lock_secs: Option<String>,
        /// Enable or disable the skill writer task.
        #[arg(long)]
        skill_writer: Option<bool>,
        /// Schedule label for the skill writer task. Empty string clears it.
        #[arg(long)]
        skill_writer_schedule: Option<String>,
        /// Skill writer interval seconds. Empty string clears it.
        #[arg(long)]
        skill_writer_interval_secs: Option<String>,
        /// Skill writer cooldown seconds. Empty string clears it.
        #[arg(long)]
        skill_writer_cooldown_secs: Option<String>,
        /// Skill writer idle seconds. Empty string clears it.
        #[arg(long)]
        skill_writer_min_idle_secs: Option<String>,
        /// Skill writer stale-lock seconds. Empty string clears it.
        #[arg(long)]
        skill_writer_stale_lock_secs: Option<String>,
        /// Project path (default: current directory, with discovery).
        #[arg(short, long)]
        path: Option<String>,
    },
}

#[derive(Subcommand)]
pub enum AutomationRunsAction {
    /// List recent automation runs.
    List {
        /// Maximum number of newest runs to return.
        #[arg(long, default_value_t = 50)]
        limit: usize,
        /// Print machine-readable JSON.
        #[arg(long)]
        json: bool,
        /// Project path (default: current directory, with discovery).
        #[arg(short, long)]
        path: Option<String>,
    },
    /// Show one automation run by run id.
    View {
        run_id: String,
        /// Print machine-readable JSON.
        #[arg(long)]
        json: bool,
        /// Project path (default: current directory, with discovery).
        #[arg(short, long)]
        path: Option<String>,
    },
    /// Read a verified automation run artifact payload.
    Artifact {
        run_id: String,
        /// Artifact kind, such as codex_handoff or validation_gate.
        kind: String,
        /// Print machine-readable JSON.
        #[arg(long)]
        json: bool,
        /// Project path (default: current directory, with discovery).
        #[arg(short, long)]
        path: Option<String>,
    },
}

#[derive(Subcommand)]
pub enum AutomationSkillsAction {
    /// List managed skills.
    List {
        /// Output as JSON.
        #[arg(long)]
        json: bool,
    },
    /// Show one managed skill.
    View {
        id: String,
        /// Output as JSON.
        #[arg(long)]
        json: bool,
    },
    /// Create and activate a managed skill.
    Create {
        #[arg(long)]
        id: String,
        #[arg(long)]
        title: String,
        #[arg(long)]
        summary: String,
        /// Discovery text describing when to use this skill.
        #[arg(long)]
        routing_description: String,
        #[arg(long)]
        category: String,
        #[arg(long)]
        body: String,
        /// Pin the skill against future stale/archive recommendations.
        #[arg(long, default_value_t = false)]
        pinned: bool,
    },
    /// Update an existing managed skill immediately.
    Update {
        id: String,
        #[arg(long)]
        title: Option<String>,
        #[arg(long)]
        summary: Option<String>,
        /// Replacement discovery text describing when to use this skill.
        #[arg(long)]
        routing_description: Option<String>,
        #[arg(long)]
        category: Option<String>,
        #[arg(long)]
        body: Option<String>,
        #[arg(long)]
        pinned: Option<bool>,
    },
    /// Disable an active skill.
    Disable { id: String },
    /// Archive a managed skill.
    Archive { id: String },
    /// Restore an archived skill to active state.
    Restore { id: String },
}

#[derive(Subcommand)]
pub enum AutomationFactsAction {
    /// List terminal automatic fact receipts.
    List {
        /// State filter: applied or quarantined.
        #[arg(long)]
        state: Option<String>,
        /// Maximum receipts to show.
        #[arg(long, default_value_t = 50)]
        limit: usize,
        /// Output the canonical automatic fact receipt payload as machine-readable JSON.
        #[arg(long)]
        json: bool,
        /// Project path (default: current directory, with discovery).
        #[arg(short, long)]
        path: Option<String>,
    },
    /// Show one terminal automatic fact receipt.
    View {
        id: String,
        /// Project path (default: current directory, with discovery).
        #[arg(short, long)]
        path: Option<String>,
    },
}
