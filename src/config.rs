use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::io::IsTerminal;
use std::path::{Path, PathBuf};
use std::sync::OnceLock;

use crate::error::{Error, Result};

// ─────────────────────────────────────────────────────────────────────────────
// Root configuration
// ─────────────────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Config {
    #[serde(default)]
    pub global: GlobalConfig,
    #[serde(default)]
    pub server: ServerConfig,
    #[serde(default)]
    pub sources: Vec<SourceConfig>,
    #[serde(default)]
    pub tasks: Vec<TaskConfig>,
    #[serde(default)]
    pub sinks: Vec<SinkConfig>,
    #[serde(default)]
    pub pipelines: Vec<PipelineConfig>,
}

impl Config {
    /// Load configuration from a file path.
    /// Performs `${VAR}` environment substitution before TOML parsing.
    pub fn load_from_path(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref();
        let raw = std::fs::read_to_string(path)
            .map_err(|e| Error::config(format!("Cannot read config file {:?}: {}", path, e)))?;
        Self::from_str_with_env(&raw)
    }

    /// Parse configuration from a TOML string with ${VAR} substitution.
    pub fn from_str_with_env(raw: &str) -> Result<Self> {
        let resolved = substitute_env(raw)?;
        let config: Config = toml::from_str(&resolved)
            .map_err(|e| Error::config(format!("TOML parse error: {}", e)))?;
        Ok(config)
    }

    /// Validate the configuration for consistency, references, and correctness.
    pub fn validate(&self) -> Result<()> {
        let mut errors: Vec<String> = Vec::new();

        // Collect all known IDs
        let task_ids: HashSet<&str> = self.tasks.iter().map(|t| t.id.as_str()).collect();
        let sink_ids: HashSet<&str> = self.sinks.iter().map(|s| s.id.as_str()).collect();
        let source_ids: HashSet<&str> = self.sources.iter().map(|s| s.id.as_str()).collect();

        // Validate sources have unique IDs
        {
            let mut seen = HashSet::new();
            for s in &self.sources {
                if !seen.insert(s.id.as_str()) {
                    errors.push(format!("Duplicate source id: {}", s.id));
                }
            }
        }

        // Validate tasks have unique IDs
        {
            let mut seen = HashSet::new();
            for t in &self.tasks {
                if !seen.insert(t.id.as_str()) {
                    errors.push(format!("Duplicate task id: {}", t.id));
                }
            }
        }

        // Validate sinks have unique IDs
        {
            let mut seen = HashSet::new();
            for s in &self.sinks {
                if !seen.insert(s.id.as_str()) {
                    errors.push(format!("Duplicate sink id: {}", s.id));
                }
            }
        }

        // Validate pipelines have unique IDs
        {
            let mut seen = HashSet::new();
            for p in &self.pipelines {
                if !seen.insert(p.id.as_str()) {
                    errors.push(format!("Duplicate pipeline id: {}", p.id));
                }
            }
        }

        // Validate sources
        for source in &self.sources {
            source.validate()?;
        }

        // Validate sinks
        for sink in &self.sinks {
            sink.validate()?;
        }

        // Validate tasks
        for task in &self.tasks {
            task.validate()?;
        }

        // Validate pipelines
        for pipeline in &self.pipelines {
            pipeline.validate(&task_ids, &sink_ids, &source_ids)?;
        }

        // Validate server-dependent configs
        if !self.server.enabled {
            for source in &self.sources {
                match source.r#type {
                    SourceType::Http | SourceType::Webhook => {
                        errors.push(format!(
                            "Source '{}' type '{}' requires server.enabled = true",
                            source.id,
                            source.r#type.as_str()
                        ));
                    }
                    _ => {}
                }
            }
            for sink in &self.sinks {
                if sink.r#type == SinkType::Stream {
                    errors.push(format!(
                        "Sink '{}' type 'stream' requires server.enabled = true",
                        sink.id
                    ));
                }
            }
        }

        // Validate feature availability
        #[cfg(not(feature = "cron"))]
        for source in &self.sources {
            if source.r#type == SourceType::Cron {
                errors.push(format!(
                    "Source '{}' type 'cron' requires the 'cron' feature",
                    source.id
                ));
            }
        }
        #[cfg(not(feature = "amqp"))]
        {
            for source in &self.sources {
                if source.r#type == SourceType::Queue {
                    errors.push(format!(
                        "Source '{}' type 'queue' requires the 'amqp' feature",
                        source.id
                    ));
                }
            }
            for sink in &self.sinks {
                if sink.r#type == SinkType::Queue {
                    errors.push(format!(
                        "Sink '{}' type 'queue' requires the 'amqp' feature",
                        sink.id
                    ));
                }
            }
        }
        #[cfg(not(feature = "sqlite"))]
        {
            for source in &self.sources {
                if source.r#type == SourceType::Sqlite {
                    errors.push(format!(
                        "Source '{}' type 'sqlite' requires the 'sqlite' feature",
                        source.id
                    ));
                }
            }
            for sink in &self.sinks {
                if sink.r#type == SinkType::Sqlite {
                    errors.push(format!(
                        "Sink '{}' type 'sqlite' requires the 'sqlite' feature",
                        sink.id
                    ));
                }
            }
            if self.global.state.backend == "sqlite" {
                errors.push("State backend 'sqlite' requires the 'sqlite' feature".to_string());
            }
        }
        #[cfg(not(feature = "postgres"))]
        {
            for source in &self.sources {
                if source.r#type == SourceType::Pg {
                    errors.push(format!(
                        "Source '{}' type 'pg' requires the 'postgres' feature",
                        source.id
                    ));
                }
            }
            for sink in &self.sinks {
                if sink.r#type == SinkType::Pg {
                    errors.push(format!(
                        "Sink '{}' type 'pg' requires the 'postgres' feature",
                        sink.id
                    ));
                }
            }
            if self.global.state.backend == "pg" {
                errors.push("State backend 'pg' requires the 'postgres' feature".to_string());
            }
        }
        #[cfg(not(feature = "webhook"))]
        for sink in &self.sinks {
            if sink.r#type == SinkType::Webhook {
                errors.push(format!(
                    "Sink '{}' type 'webhook' requires the 'webhook' feature",
                    sink.id
                ));
            }
        }
        #[cfg(not(feature = "wasm"))]
        for task in &self.tasks {
            if task.driver == "wasm" {
                errors.push(format!(
                    "Task '{}' driver 'wasm' requires the 'wasm' feature",
                    task.id
                ));
            }
        }
        #[cfg(not(feature = "server"))]
        {
            if self.server.enabled {
                errors.push("server.enabled = true requires the 'server' feature".to_string());
            }
        }

        // Validate sink references exist
        for pipeline in &self.pipelines {
            // Pipeline-level sinks
            for sink_id in &pipeline.sinks {
                if !sink_ids.contains(sink_id.as_str()) {
                    errors.push(format!(
                        "Pipeline '{}' references unknown sink '{}'",
                        pipeline.id, sink_id
                    ));
                }
            }
            // Failure sink
            if pipeline.failure.action == FailureAction::DeadLetter {
                if let Some(ref sink_id) = pipeline.failure.sink {
                    if !sink_ids.contains(sink_id.as_str()) {
                        errors.push(format!(
                            "Pipeline '{}' failure sink '{}' not found",
                            pipeline.id, sink_id
                        ));
                    }
                } else {
                    errors.push(format!(
                        "Pipeline '{}' failure action is dead_letter but no sink specified",
                        pipeline.id
                    ));
                }
            }
            // Step sinks
            for step in &pipeline.steps {
                for sink_id in &step.sinks {
                    if !sink_ids.contains(sink_id.as_str()) {
                        errors.push(format!(
                            "Pipeline '{}' step '{}' references unknown sink '{}'",
                            pipeline.id, step.id, sink_id
                        ));
                    }
                }
                // Routing sinks
                for route in &step.routing {
                    for sink_id in &route.sinks {
                        if !sink_ids.contains(sink_id.as_str()) {
                            errors.push(format!(
                                "Pipeline '{}' step '{}' routing references unknown sink '{}'",
                                pipeline.id, step.id, sink_id
                            ));
                        }
                    }
                }
                // Step failure sink
                if step.failure.action == FailureAction::DeadLetter
                    && let Some(ref sink_id) = step.failure.sink
                    && !sink_ids.contains(sink_id.as_str())
                {
                    errors.push(format!(
                        "Pipeline '{}' step '{}' failure sink '{}' not found",
                        pipeline.id, step.id, sink_id
                    ));
                }
            }
        }

        // Validate retry jitter values
        if self.global.retry.jitter < 0.0 || self.global.retry.jitter > 1.0 {
            errors.push(format!(
                "global.retry.jitter must be between 0.0 and 1.0, got {}",
                self.global.retry.jitter
            ));
        }

        if self.global.state.backend == "pg" && self.global.state.pg_url.trim().is_empty() {
            errors.push("global.state.pg_url is required when backend = \"pg\"".to_string());
        }

        if !errors.is_empty() {
            return Err(Error::Validation(errors.join("\n")));
        }

        Ok(())
    }

    /// Get a task config by id.
    pub fn get_task(&self, id: &str) -> Option<&TaskConfig> {
        self.tasks.iter().find(|t| t.id == id)
    }

    /// Get a sink config by id.
    pub fn get_sink(&self, id: &str) -> Option<&SinkConfig> {
        self.sinks.iter().find(|s| s.id == id)
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Global configuration
// ─────────────────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GlobalConfig {
    #[serde(default = "default_worker_threads")]
    pub worker_threads: usize,
    #[serde(default = "default_shutdown_timeout_secs")]
    pub shutdown_timeout_secs: u64,
    #[serde(default = "default_tmp_dir")]
    pub tmp_dir: PathBuf,
    #[serde(default = "default_max_payload_bytes")]
    pub max_payload_bytes: usize,
    #[serde(default = "default_cancel_signal_ttl_secs")]
    pub cancel_signal_ttl_secs: u64,
    #[serde(default)]
    pub log: LogConfig,
    #[serde(default)]
    pub state: StateConfig,
    #[serde(default)]
    pub retry: GlobalRetryConfig,
    #[serde(default)]
    pub timeout: GlobalTimeoutConfig,
}

impl Default for GlobalConfig {
    fn default() -> Self {
        Self {
            worker_threads: default_worker_threads(),
            shutdown_timeout_secs: default_shutdown_timeout_secs(),
            tmp_dir: default_tmp_dir(),
            max_payload_bytes: default_max_payload_bytes(),
            cancel_signal_ttl_secs: default_cancel_signal_ttl_secs(),
            log: LogConfig::default(),
            state: StateConfig::default(),
            retry: GlobalRetryConfig::default(),
            timeout: GlobalTimeoutConfig::default(),
        }
    }
}

fn default_worker_threads() -> usize {
    0
}
fn default_shutdown_timeout_secs() -> u64 {
    30
}
fn default_tmp_dir() -> PathBuf {
    std::env::temp_dir()
}
fn default_max_payload_bytes() -> usize {
    10 * 1024 * 1024 // 10 MiB
}
fn default_cancel_signal_ttl_secs() -> u64 {
    3600
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LogConfig {
    #[serde(default = "default_log_level")]
    pub level: String,
    /// When None (omitted), auto-detect: text if stdout is a TTY, json otherwise.
    /// Explicit "text" or "json" values are always preserved.
    #[serde(default)]
    pub format: Option<String>,
    #[serde(default)]
    pub file: String,
}

impl Default for LogConfig {
    fn default() -> Self {
        Self {
            level: default_log_level(),
            format: None,
            file: String::new(),
        }
    }
}

impl LogConfig {
    /// Resolve the effective log format: returns the configured value, or
    /// auto-detects "text" for TTY stdout and "json" for non-TTY.
    pub fn effective_format(&self) -> &str {
        if let Some(ref fmt) = self.format {
            fmt.as_str()
        } else if std::io::stdout().is_terminal() {
            "text"
        } else {
            "json"
        }
    }
}

fn default_log_level() -> String {
    "info".to_string()
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StateConfig {
    #[serde(default = "default_state_backend")]
    pub backend: String,
    #[serde(default = "default_sqlite_path")]
    pub sqlite_path: String,
    #[serde(default)]
    pub pg_url: String,
}

impl Default for StateConfig {
    fn default() -> Self {
        Self {
            backend: default_state_backend(),
            sqlite_path: default_sqlite_path(),
            pg_url: String::new(),
        }
    }
}

fn default_state_backend() -> String {
    "memory".to_string()
}
fn default_sqlite_path() -> String {
    "bria-state.db".to_string()
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GlobalRetryConfig {
    #[serde(default)]
    pub max_attempts: u32,
    #[serde(default = "default_retry_base_delay")]
    pub base_delay_ms: u64,
    #[serde(default = "default_retry_max_delay")]
    pub max_delay_ms: u64,
    #[serde(default = "default_retry_jitter")]
    pub jitter: f64,
}

impl Default for GlobalRetryConfig {
    fn default() -> Self {
        Self {
            max_attempts: 0,
            base_delay_ms: default_retry_base_delay(),
            max_delay_ms: default_retry_max_delay(),
            jitter: default_retry_jitter(),
        }
    }
}

fn default_retry_base_delay() -> u64 {
    1000
}
fn default_retry_max_delay() -> u64 {
    30000
}
fn default_retry_jitter() -> f64 {
    0.2
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GlobalTimeoutConfig {
    #[serde(default = "default_step_timeout")]
    pub step_secs: u64,
    #[serde(default = "default_timeout_action")]
    pub action: String,
    #[serde(default = "default_kill_grace_secs")]
    pub kill_grace_secs: u64,
}

impl Default for GlobalTimeoutConfig {
    fn default() -> Self {
        Self {
            step_secs: default_step_timeout(),
            action: default_timeout_action(),
            kill_grace_secs: default_kill_grace_secs(),
        }
    }
}

fn default_step_timeout() -> u64 {
    300
}
fn default_timeout_action() -> String {
    "kill".to_string()
}
fn default_kill_grace_secs() -> u64 {
    5
}

// ─────────────────────────────────────────────────────────────────────────────
// Server configuration
// ─────────────────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServerConfig {
    #[serde(default)]
    pub enabled: bool,
    #[serde(default = "default_bind")]
    pub bind: String,
    #[serde(default = "default_port")]
    pub port: u16,
    #[serde(default = "default_prefix")]
    pub prefix: String,
    #[serde(default)]
    pub api_key: String,
    #[serde(default)]
    pub dashboard: String,
    #[serde(default = "default_server_shutdown_timeout")]
    pub shutdown_timeout_secs: u64,
    #[serde(default = "default_max_body_bytes")]
    pub max_body_bytes: usize,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            bind: default_bind(),
            port: default_port(),
            prefix: default_prefix(),
            api_key: String::new(),
            dashboard: String::new(),
            shutdown_timeout_secs: default_server_shutdown_timeout(),
            max_body_bytes: default_max_body_bytes(),
        }
    }
}

pub fn default_max_body_bytes() -> usize {
    52428800 // 50 MiB
}

fn default_bind() -> String {
    "0.0.0.0".to_string()
}
fn default_port() -> u16 {
    4000
}
fn default_prefix() -> String {
    "v1".to_string()
}
fn default_server_shutdown_timeout() -> u64 {
    5
}

// ─────────────────────────────────────────────────────────────────────────────
// Source configuration
// ─────────────────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SourceConfig {
    pub id: String,
    #[serde(rename = "type")]
    pub r#type: SourceType,
    // File source
    #[serde(default)]
    pub path: PathBuf,
    #[serde(default = "default_poll_interval")]
    pub poll_interval_secs: u64,
    #[serde(default = "default_true")]
    pub track_cursor: bool,
    #[serde(default)]
    pub authoritative: bool,
    #[serde(default)]
    pub id_field: String,
    // HTTP source
    #[serde(default = "default_max_body_bytes_val")]
    pub max_body_bytes: usize,
    // Webhook source
    #[serde(default)]
    pub hmac_secret: String,
    #[serde(default = "default_hmac_header")]
    pub hmac_header: String,
    #[serde(default = "default_ack_status")]
    pub ack_status: u16,
    // Queue source
    #[serde(default)]
    pub url: String,
    #[serde(default)]
    pub username: String,
    #[serde(default)]
    pub password: String,
    #[serde(default)]
    pub exchange: String,
    #[serde(default = "default_submit_routing_key")]
    pub submit_routing_key: String,
    #[serde(default = "default_cancel_routing_key")]
    pub cancel_routing_key: String,
    #[serde(default = "default_reconnect_secs")]
    pub reconnect_secs: u64,
    #[serde(default = "default_qos_prefetch")]
    pub qos_prefetch: u16,
    #[serde(default = "default_consumer_tag")]
    pub consumer_tag: String,
    // Cron source
    #[serde(default)]
    pub schedule: String,
    #[serde(default = "default_tz")]
    pub tz: String,
    // Labels
    #[serde(default)]
    pub labels: HashMap<String, String>,
    // Payload (for cron)
    #[serde(default)]
    pub payload: serde_json::Value,
    // Table (for pg/sqlite)
    #[serde(default)]
    pub table: Option<TableSourceConfig>,
}

impl SourceConfig {
    pub fn kind(&self) -> SourceKind<'_> {
        match self.r#type {
            SourceType::File => SourceKind::File(self),
            SourceType::Http => SourceKind::Http(self),
            SourceType::Webhook => SourceKind::Webhook(self),
            SourceType::Queue => SourceKind::Queue(self),
            SourceType::Cron => SourceKind::Cron(self),
            SourceType::Pg => SourceKind::Pg(self),
            SourceType::Sqlite => SourceKind::Sqlite(self),
        }
    }

    pub fn validate(&self) -> Result<()> {
        match self.r#type {
            SourceType::File | SourceType::Sqlite => {
                if self.path.as_os_str().is_empty() {
                    return Err(Error::validation(format!(
                        "Source '{}' type '{}' requires a path",
                        self.id,
                        self.r#type.as_str()
                    )));
                }
            }
            SourceType::Cron => {
                if self.schedule.is_empty() {
                    return Err(Error::validation(format!(
                        "Source '{}' type 'cron' requires a schedule",
                        self.id
                    )));
                }
            }
            SourceType::Pg | SourceType::Queue => {
                if self.url.is_empty() {
                    return Err(Error::validation(format!(
                        "Source '{}' type '{}' requires a url",
                        self.id,
                        self.r#type.as_str()
                    )));
                }
            }
            SourceType::Http | SourceType::Webhook => {
                if self.path.as_os_str().is_empty() {
                    return Err(Error::validation(format!(
                        "Source '{}' type '{}' requires a path",
                        self.id,
                        self.r#type.as_str()
                    )));
                }
            }
        }
        Ok(())
    }
}

/// Typed view over the flat TOML-compatible source configuration.
///
/// Bria keeps the public TOML shape backward compatible while routing runtime
/// logic through this enum to make source-specific branches explicit.
pub enum SourceKind<'a> {
    File(&'a SourceConfig),
    Http(&'a SourceConfig),
    Webhook(&'a SourceConfig),
    Queue(&'a SourceConfig),
    Cron(&'a SourceConfig),
    Pg(&'a SourceConfig),
    Sqlite(&'a SourceConfig),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableSourceConfig {
    #[serde(default)]
    pub name: String,
    #[serde(default)]
    pub columns: TableSourceColumnsConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableSourceColumnsConfig {
    #[serde(default = "default_column_id")]
    pub id: String,
    #[serde(default = "default_column_payload")]
    pub payload: String,
    #[serde(default = "default_column_created_at")]
    pub created_at: String,
    #[serde(default = "default_column_status")]
    pub status: String,
    #[serde(default = "default_status_claimed_value")]
    pub status_claimed_value: String,
    #[serde(default = "default_status_done_value")]
    pub status_done_value: String,
    #[serde(default = "default_status_failed_value")]
    pub status_failed_value: String,
}

impl Default for TableSourceColumnsConfig {
    fn default() -> Self {
        Self {
            id: default_column_id(),
            payload: default_column_payload(),
            created_at: default_column_created_at(),
            status: default_column_status(),
            status_claimed_value: default_status_claimed_value(),
            status_done_value: default_status_done_value(),
            status_failed_value: default_status_failed_value(),
        }
    }
}

fn default_column_id() -> String {
    "id".to_string()
}
fn default_column_payload() -> String {
    "payload".to_string()
}
fn default_column_created_at() -> String {
    "created_at".to_string()
}
fn default_column_status() -> String {
    "status".to_string()
}
fn default_status_claimed_value() -> String {
    "processing".to_string()
}
fn default_status_done_value() -> String {
    "done".to_string()
}
fn default_status_failed_value() -> String {
    "failed".to_string()
}

pub(crate) fn default_max_body_bytes_val() -> usize {
    1_048_576 // 1 MiB
}
fn default_poll_interval() -> u64 {
    2
}
pub(crate) const fn default_true() -> bool {
    true
}
fn default_hmac_header() -> String {
    "X-Bria-Signature".to_string()
}
fn default_ack_status() -> u16 {
    202
}
fn default_submit_routing_key() -> String {
    "job.submit".to_string()
}
fn default_cancel_routing_key() -> String {
    "job.cancel".to_string()
}
fn default_reconnect_secs() -> u64 {
    5
}
fn default_qos_prefetch() -> u16 {
    100
}
fn default_consumer_tag() -> String {
    "bria-source".to_string()
}
fn default_tz() -> String {
    "UTC".to_string()
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum SourceType {
    File,
    Http,
    Webhook,
    Queue,
    Cron,
    Pg,
    Sqlite,
}

impl SourceType {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::File => "file",
            Self::Http => "http",
            Self::Webhook => "webhook",
            Self::Queue => "queue",
            Self::Cron => "cron",
            Self::Pg => "pg",
            Self::Sqlite => "sqlite",
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Task configuration
// ─────────────────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskConfig {
    pub id: String,
    #[serde(default = "default_driver")]
    pub driver: String,
    pub cmd: String,
    #[serde(default)]
    pub args: Vec<String>,
    #[serde(default)]
    pub inherit_env: bool,
    #[serde(default)]
    pub working_dir: Option<PathBuf>,
    #[serde(default = "default_success_exit_codes")]
    pub success_exit_codes: Vec<i32>,
    #[serde(default)]
    pub timeout_secs: u64,
    #[serde(default = "default_timeout_action")]
    pub timeout_action: String,
    #[serde(default = "default_kill_grace_secs")]
    pub kill_grace_secs: u64,
    #[serde(default)]
    pub env: HashMap<String, String>,
    #[serde(default)]
    pub stdin: StdinConfig,
    #[serde(default)]
    pub stdout: StreamConfig,
    #[serde(default = "default_stderr_stream")]
    pub stderr: StreamConfig,
    #[serde(default)]
    pub retry: TaskRetryConfig,
    #[serde(default)]
    pub labels: HashMap<String, String>,
    #[serde(default)]
    pub docker: Option<DockerConfig>,
    #[serde(default)]
    pub wasm: Option<WasmConfig>,
}

fn default_stderr_stream() -> StreamConfig {
    StreamConfig {
        mode: default_capture_mode(),
        max_bytes: 1024 * 1024,
    }
}

impl TaskConfig {
    pub fn kind(&self) -> TaskDriverKind<'_> {
        match self.driver.as_str() {
            "docker" => TaskDriverKind::Docker(self),
            "wasm" => TaskDriverKind::Wasm(self),
            _ => TaskDriverKind::Local(self),
        }
    }

    pub fn validate(&self) -> Result<()> {
        match self.driver.as_str() {
            "local" | "docker" | "wasm" => {}
            d => {
                return Err(Error::validation(format!(
                    "Task '{}' has unknown driver '{}'",
                    self.id, d
                )));
            }
        }

        if self.driver == "docker" && self.docker.is_none() {
            return Err(Error::validation(format!(
                "Task '{}' driver is 'docker' but [tasks.docker] section is missing",
                self.id
            )));
        }

        if self.driver == "wasm" && self.wasm.is_none() {
            return Err(Error::validation(format!(
                "Task '{}' driver is 'wasm' but [tasks.wasm] section is missing",
                self.id
            )));
        }

        if self.retry.jitter < 0.0 || self.retry.jitter > 1.0 {
            return Err(Error::validation(format!(
                "Task '{}' retry.jitter must be between 0.0 and 1.0, got {}",
                self.id, self.retry.jitter
            )));
        }

        Ok(())
    }
}

/// Typed view over task driver-specific settings.
pub enum TaskDriverKind<'a> {
    Local(&'a TaskConfig),
    Docker(&'a TaskConfig),
    Wasm(&'a TaskConfig),
}

fn default_driver() -> String {
    "local".to_string()
}
fn default_success_exit_codes() -> Vec<i32> {
    vec![0]
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StdinConfig {
    #[serde(default = "default_stdin_mode")]
    pub mode: String,
    #[serde(default)]
    pub template: Option<String>,
}

impl Default for StdinConfig {
    fn default() -> Self {
        Self {
            mode: default_stdin_mode(),
            template: None,
        }
    }
}

fn default_stdin_mode() -> String {
    "none".to_string()
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StreamConfig {
    #[serde(default = "default_capture_mode")]
    pub mode: String,
    #[serde(default)]
    pub max_bytes: usize,
}

impl Default for StreamConfig {
    fn default() -> Self {
        Self {
            mode: default_capture_mode(),
            max_bytes: 10 * 1024 * 1024,
        }
    }
}

fn default_capture_mode() -> String {
    "capture".to_string()
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskRetryConfig {
    #[serde(default)]
    pub max_attempts: u32,
    #[serde(default = "default_retry_base_delay")]
    pub base_delay_ms: u64,
    #[serde(default = "default_retry_max_delay")]
    pub max_delay_ms: u64,
    #[serde(default = "default_retry_jitter")]
    pub jitter: f64,
}

impl Default for TaskRetryConfig {
    fn default() -> Self {
        Self {
            max_attempts: 0,
            base_delay_ms: default_retry_base_delay(),
            max_delay_ms: default_retry_max_delay(),
            jitter: default_retry_jitter(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DockerConfig {
    #[serde(default)]
    pub flags: Vec<String>,
    #[serde(default)]
    pub mounts: Vec<String>,
    #[serde(default = "default_pull")]
    pub pull: String,
}

fn default_pull() -> String {
    "missing".to_string()
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WasmConfig {
    #[serde(default)]
    pub dirs: Vec<String>,
    #[serde(default = "default_max_memory_pages")]
    pub max_memory_pages: u32,
    #[serde(default)]
    pub fuel: u64,
}

fn default_max_memory_pages() -> u32 {
    256
}

// ─────────────────────────────────────────────────────────────────────────────
// Sink configuration
// ─────────────────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SinkConfig {
    pub id: String,
    #[serde(rename = "type")]
    pub r#type: SinkType,
    // File sink
    #[serde(default)]
    pub path: String,
    #[serde(default)]
    pub template: Option<String>,
    // Webhook sink
    #[serde(default)]
    pub secret: String,
    #[serde(default = "default_signature_header")]
    pub signature_header: String,
    #[serde(default = "default_content_type")]
    pub content_type: String,
    #[serde(default = "default_max_retries")]
    pub max_retries: u32,
    #[serde(default = "default_retry_base_ms_sink")]
    pub retry_base_ms: u64,
    #[serde(default = "default_timeout_secs_sink")]
    pub timeout_secs: u64,
    #[serde(default)]
    pub headers: HashMap<String, String>,
    // Queue sink
    #[serde(default)]
    pub url: String,
    #[serde(default)]
    pub username: String,
    #[serde(default)]
    pub password: String,
    #[serde(default)]
    pub exchange: String,
    #[serde(default)]
    pub success_routing_key: String,
    #[serde(default)]
    pub failure_routing_key: String,
    #[serde(default = "default_reconnect_secs")]
    pub reconnect_secs: u64,
    // Stream sink
    #[serde(default = "default_sse")]
    pub sse: String,
    #[serde(default = "default_ws")]
    pub websocket: String,
    #[serde(default = "default_ws_heartbeat")]
    pub ws_heartbeat_secs: u64,
    #[serde(default = "default_sse_keepalive")]
    pub sse_keepalive_secs: u64,
    #[serde(default = "default_broadcast_capacity")]
    pub broadcast_capacity: usize,
    // Table (for pg/sqlite sinks)
    #[serde(default)]
    pub table: Option<TableSinkConfig>,
}

impl SinkConfig {
    pub fn kind(&self) -> SinkKind<'_> {
        match self.r#type {
            SinkType::File => SinkKind::File(self),
            SinkType::Webhook => SinkKind::Webhook(self),
            SinkType::Queue => SinkKind::Queue(self),
            SinkType::Pg => SinkKind::Pg(self),
            SinkType::Sqlite => SinkKind::Sqlite(self),
            SinkType::Stream => SinkKind::Stream(self),
        }
    }

    pub fn validate(&self) -> Result<()> {
        match self.r#type {
            SinkType::File => {
                if self.path.is_empty() {
                    return Err(Error::validation(format!(
                        "Sink '{}' type 'file' requires a path",
                        self.id
                    )));
                }
            }
            SinkType::Webhook => {
                if self.url.is_empty() {
                    return Err(Error::validation(format!(
                        "Sink '{}' type 'webhook' requires a url",
                        self.id
                    )));
                }
            }
            SinkType::Queue => {
                if self.url.is_empty() {
                    return Err(Error::validation(format!(
                        "Sink '{}' type 'queue' requires a url",
                        self.id
                    )));
                }
            }
            SinkType::Pg => {
                if self.url.is_empty() {
                    return Err(Error::validation(format!(
                        "Sink '{}' type 'pg' requires a url",
                        self.id
                    )));
                }
            }
            SinkType::Sqlite => {
                if self.path.is_empty() {
                    return Err(Error::validation(format!(
                        "Sink '{}' type 'sqlite' requires a path",
                        self.id
                    )));
                }
            }
            SinkType::Stream => {
                // Stream sink is valid without additional requirements
            }
        }
        Ok(())
    }
}

/// Typed view over sink-specific settings.
pub enum SinkKind<'a> {
    File(&'a SinkConfig),
    Webhook(&'a SinkConfig),
    Queue(&'a SinkConfig),
    Pg(&'a SinkConfig),
    Sqlite(&'a SinkConfig),
    Stream(&'a SinkConfig),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum SinkType {
    File,
    Webhook,
    Queue,
    Pg,
    Sqlite,
    Stream,
}

fn default_signature_header() -> String {
    "X-Bria-Signature".to_string()
}
fn default_content_type() -> String {
    "application/json".to_string()
}
fn default_max_retries() -> u32 {
    3
}
fn default_retry_base_ms_sink() -> u64 {
    250
}
fn default_timeout_secs_sink() -> u64 {
    30
}
fn default_sse() -> String {
    "sse".to_string()
}
fn default_ws() -> String {
    "ws".to_string()
}
fn default_ws_heartbeat() -> u64 {
    30
}
fn default_sse_keepalive() -> u64 {
    5
}
pub(crate) fn default_broadcast_capacity() -> usize {
    1024
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableSinkConfig {
    #[serde(default)]
    pub name: String,
    #[serde(default)]
    pub columns: TableSinkColumnsConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableSinkColumnsConfig {
    #[serde(default = "default_col_result_id")]
    pub result_id: String,
    #[serde(default = "default_col_job_id")]
    pub job_id: String,
    #[serde(default = "default_col_pipeline_id")]
    pub pipeline_id: String,
    #[serde(default = "default_col_step_id")]
    pub step_id: String,
    #[serde(default = "default_col_occurred_at")]
    pub occurred_at: String,
    #[serde(default = "default_col_exit_code")]
    pub exit_code: String,
    #[serde(default = "default_col_stdout")]
    pub stdout: String,
    #[serde(default = "default_col_stderr")]
    pub stderr: String,
    #[serde(default = "default_col_duration_ms")]
    pub duration_ms: String,
    #[serde(default = "default_col_attempt")]
    pub attempt: String,
    #[serde(default = "default_col_status")]
    pub status: String,
}

impl Default for TableSinkColumnsConfig {
    fn default() -> Self {
        Self {
            result_id: default_col_result_id(),
            job_id: default_col_job_id(),
            pipeline_id: default_col_pipeline_id(),
            step_id: default_col_step_id(),
            occurred_at: default_col_occurred_at(),
            exit_code: default_col_exit_code(),
            stdout: default_col_stdout(),
            stderr: default_col_stderr(),
            duration_ms: default_col_duration_ms(),
            attempt: default_col_attempt(),
            status: default_col_status(),
        }
    }
}

fn default_col_result_id() -> String {
    "result_id".to_string()
}
fn default_col_job_id() -> String {
    "job_id".to_string()
}
fn default_col_pipeline_id() -> String {
    "pipeline_id".to_string()
}
fn default_col_step_id() -> String {
    "step_id".to_string()
}
fn default_col_occurred_at() -> String {
    "occurred_at".to_string()
}
fn default_col_exit_code() -> String {
    "exit_code".to_string()
}
fn default_col_stdout() -> String {
    "stdout".to_string()
}
fn default_col_stderr() -> String {
    "stderr".to_string()
}
fn default_col_duration_ms() -> String {
    "duration_ms".to_string()
}
fn default_col_attempt() -> String {
    "attempt".to_string()
}
fn default_col_status() -> String {
    "status".to_string()
}

// ─────────────────────────────────────────────────────────────────────────────
// Pipeline configuration
// ─────────────────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PipelineConfig {
    pub id: String,
    /// Single source shorthand (scalar).
    #[serde(default)]
    pub source: Option<String>,
    /// Multiple sources (array of tables).
    #[serde(default)]
    pub sources: Vec<PipelineSourceEntry>,
    #[serde(default)]
    pub merge: Option<MergeConfig>,
    #[serde(default = "default_concurrency")]
    pub concurrency: usize,
    #[serde(default = "default_queue_capacity")]
    pub queue_capacity: usize,
    #[serde(default)]
    pub sinks: Vec<String>,
    #[serde(default)]
    pub failure: FailureConfig,
    #[serde(default)]
    pub labels: HashMap<String, String>,
    #[serde(default)]
    pub steps: Vec<StepConfig>,

    /// Runtime cache: merged list of source ids for this pipeline.
    /// Lazily populated by `get_sources()` and eagerly refreshable by
    /// `resolve_sources()` for backward compatibility with existing callers.
    #[serde(skip)]
    pub resolved_sources: OnceLock<Vec<String>>,
}

impl PipelineConfig {
    /// Resolve sources after parsing: if `source` is set, use it; otherwise use `sources`.
    pub fn resolve_sources(&mut self) {
        self.resolved_sources = OnceLock::new();
        let _ = self.resolved_sources.set(self.compute_sources());
    }

    fn compute_sources(&self) -> Vec<String> {
        if let Some(ref s) = self.source {
            vec![s.clone()]
        } else {
            self.sources.iter().map(|e| e.source.clone()).collect()
        }
    }

    pub fn validate(
        &self,
        task_ids: &HashSet<&str>,
        sink_ids: &HashSet<&str>,
        source_ids: &HashSet<&str>,
    ) -> Result<()> {
        let mut errors: Vec<String> = Vec::new();

        // Validate sources exist
        if let Some(ref source) = self.source {
            if !source_ids.contains(source.as_str()) {
                errors.push(format!(
                    "Pipeline '{}' references unknown source '{}'",
                    self.id, source
                ));
            }
        } else {
            for entry in &self.sources {
                if !source_ids.contains(entry.source.as_str()) {
                    errors.push(format!(
                        "Pipeline '{}' references unknown source '{}'",
                        self.id, entry.source
                    ));
                }
            }
        }

        // Validate has at least one source
        if self.source.is_none() && self.sources.is_empty() {
            errors.push(format!("Pipeline '{}' has no sources configured", self.id));
        }

        // Validate merge config for multi-source
        if self.sources.len() > 1 && self.merge.is_none() {
            errors.push(format!(
                "Pipeline '{}' has multiple sources but no [pipelines.merge] section",
                self.id
            ));
        }

        // Validate merge config
        if let Some(ref merge) = self.merge {
            match merge.strategy.as_str() {
                "any" | "all" => {}
                s => errors.push(format!(
                    "Pipeline '{}' merge.strategy '{}' is invalid (must be 'any' or 'all')",
                    self.id, s
                )),
            }

            // correlation_key and correlation_expr are mutually exclusive
            if merge.correlation_key.is_some() && merge.correlation_expr.is_some() {
                errors.push(format!(
                    "Pipeline '{}' merge config: correlation_key and correlation_expr are mutually exclusive",
                    self.id
                ));
            }

            // Must have one of correlation_key or correlation_expr
            if merge.correlation_key.is_none() && merge.correlation_expr.is_none() {
                errors.push(format!(
                    "Pipeline '{}' merge config: must specify either correlation_key or correlation_expr",
                    self.id
                ));
            }
        }

        // Validate each step
        let step_ids: HashSet<&str> = self.steps.iter().map(|s| s.id.as_str()).collect();

        // Check duplicate step IDs
        {
            let mut seen = HashSet::new();
            for step in &self.steps {
                if !seen.insert(step.id.as_str()) {
                    errors.push(format!(
                        "Pipeline '{}' has duplicate step id '{}'",
                        self.id, step.id
                    ));
                }
            }
        }

        for step in self.steps.iter() {
            match step.r#type {
                StepType::Process => {
                    if step.task.is_none() {
                        errors.push(format!(
                            "Pipeline '{}' step '{}' type 'process' requires a task",
                            self.id, step.id
                        ));
                    } else if let Some(ref task_id) = step.task
                        && !task_ids.contains(task_id.as_str())
                    {
                        errors.push(format!(
                            "Pipeline '{}' step '{}' references unknown task '{}'",
                            self.id, step.id, task_id
                        ));
                    }
                }
                StepType::Condition => {
                    if step.expr.is_none() {
                        errors.push(format!(
                            "Pipeline '{}' step '{}' type 'condition' requires an expr",
                            self.id, step.id
                        ));
                    }
                    // Validate skip_to references a real step
                    if let Some(ref skip_to) = step.skip_to
                        && !step_ids.contains(skip_to.as_str())
                    {
                        errors.push(format!(
                            "Pipeline '{}' step '{}' skip_to '{}' references unknown step",
                            self.id, step.id, skip_to
                        ));
                    }
                    // Validate action
                    match step.action.as_deref().unwrap_or("fail") {
                        "fail" | "skip_to" | "emit" => {}
                        a => errors.push(format!(
                            "Pipeline '{}' step '{}' action '{}' is invalid (must be 'fail', 'skip_to', or 'emit')",
                            self.id, step.id, a
                        )),
                    }
                }
                StepType::Map => {
                    // Map steps must have set entries
                    if step.set.is_empty() {
                        errors.push(format!(
                            "Pipeline '{}' step '{}' type 'map' requires at least one [[pipelines.steps.set]] entry",
                            self.id, step.id
                        ));
                    }
                }
            }

            // Validate depends_on references
            for dep in &step.depends_on {
                if !step_ids.contains(dep.as_str()) {
                    errors.push(format!(
                        "Pipeline '{}' step '{}' depends_on '{}' references unknown step",
                        self.id, step.id, dep
                    ));
                }
            }

            // Validate retry jitter
            if step.retry.jitter < 0.0 || step.retry.jitter > 1.0 {
                errors.push(format!(
                    "Pipeline '{}' step '{}' retry.jitter must be between 0.0 and 1.0, got {}",
                    self.id, step.id, step.retry.jitter
                ));
            }

            // Validate failure config
            if step.failure.action == FailureAction::DeadLetter && step.failure.sink.is_none() {
                errors.push(format!(
                    "Pipeline '{}' step '{}' failure action is dead_letter but no sink specified",
                    self.id, step.id
                ));
            }

            // Validate routing sink references
            for route in &step.routing {
                for sink_id in &route.sinks {
                    if !sink_ids.contains(sink_id.as_str()) {
                        errors.push(format!(
                            "Pipeline '{}' step '{}' routing sink '{}' not found",
                            self.id, step.id, sink_id
                        ));
                    }
                }
            }
        }

        // Build and validate the DAG (detect cycles)
        if let Err(e) = self.validate_dag(&step_ids) {
            errors.push(e);
        }

        if !errors.is_empty() {
            return Err(Error::Validation(errors.join("\n")));
        }

        Ok(())
    }

    /// Validate the DAG: no cycles, and compute execution order.
    /// For steps with no depends_on, they implicitly depend on the preceding step
    /// (or are entry points if they are the first step).
    fn validate_dag(&self, step_ids: &HashSet<&str>) -> std::result::Result<(), String> {
        // Build explicit dependency graph
        let mut deps: HashMap<&str, Vec<&str>> = HashMap::new();

        for (_i, step) in self.steps.iter().enumerate() {
            let step_deps: Vec<&str> = if step.depends_on.is_empty() {
                if _i > 0 {
                    vec![self.steps[_i - 1].id.as_str()]
                } else {
                    vec![]
                }
            } else {
                step.depends_on.iter().map(|s| s.as_str()).collect()
            };
            deps.insert(step.id.as_str(), step_deps);
        }

        // Topological sort with cycle detection (Kahn's algorithm)
        let mut in_degree: HashMap<&str, usize> = step_ids.iter().map(|&id| (id, 0)).collect();
        let mut adj: HashMap<&str, Vec<&str>> = HashMap::new();

        for (&step_id, step_deps) in &deps {
            for dep in step_deps {
                *in_degree.get_mut(step_id).unwrap() += 1;
                adj.entry(dep).or_default().push(step_id);
            }
        }

        let mut queue: Vec<&str> = in_degree
            .iter()
            .filter(|(_, deg)| **deg == 0)
            .map(|(id, _)| *id)
            .collect();

        let mut sorted = Vec::new();

        while let Some(node) = queue.pop() {
            sorted.push(node);
            if let Some(neighbors) = adj.get(node) {
                for neighbor in neighbors {
                    if let Some(deg) = in_degree.get_mut(neighbor) {
                        *deg -= 1;
                        if *deg == 0 {
                            queue.push(neighbor);
                        }
                    }
                }
            }
        }

        if sorted.len() != step_ids.len() {
            return Err(format!(
                "Pipeline '{}' contains a cycle in its step dependencies",
                self.id
            ));
        }

        Ok(())
    }
}

/// Get the resolved sources for this pipeline.
///
/// Returns the cached result from `resolve_sources()` if available; otherwise
/// computes it on the fly from the TOML fields (`source` or `sources`). This
/// means callers do not need to remember to call `resolve_sources()` first.
impl PipelineConfig {
    pub fn get_sources(&self) -> &[String] {
        self.resolved_sources.get_or_init(|| self.compute_sources())
    }
}

pub(crate) fn default_concurrency() -> usize {
    8
}
fn default_queue_capacity() -> usize {
    256
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PipelineSourceEntry {
    pub source: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MergeConfig {
    #[serde(default = "default_merge_strategy")]
    pub strategy: String,
    #[serde(default)]
    pub correlation_key: Option<String>,
    #[serde(default)]
    pub correlation_expr: Option<String>,
    #[serde(default = "default_merge_timeout")]
    pub timeout_secs: u64,
}

fn default_merge_strategy() -> String {
    "any".to_string()
}
fn default_merge_timeout() -> u64 {
    60
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FailureConfig {
    #[serde(default = "default_failure_action")]
    pub action: FailureAction,
    #[serde(default)]
    pub sink: Option<String>,
}

impl Default for FailureConfig {
    fn default() -> Self {
        Self {
            action: default_failure_action(),
            sink: None,
        }
    }
}

fn default_failure_action() -> FailureAction {
    FailureAction::Discard
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FailureAction {
    Discard,
    #[serde(rename = "dead_letter")]
    DeadLetter,
    Stop,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StepConfig {
    pub id: String,
    #[serde(rename = "type", default = "default_step_type")]
    pub r#type: StepType,
    #[serde(default)]
    pub task: Option<String>,
    #[serde(default)]
    pub depends_on: Vec<String>,
    #[serde(default)]
    pub with: Option<StepWithConfig>,
    #[serde(default)]
    pub outputs: Option<StepOutputsConfig>,
    #[serde(default)]
    pub retry: StepRetryConfig,
    #[serde(default)]
    pub failure: FailureConfig,
    #[serde(default)]
    pub sinks: Vec<String>,
    #[serde(default)]
    pub routing: Vec<StepRoutingConfig>,
    // Condition step fields
    #[serde(default)]
    pub expr: Option<String>,
    #[serde(default)]
    pub action: Option<String>,
    #[serde(default)]
    pub skip_to: Option<String>,
    #[serde(default)]
    pub reason: Option<String>,
    // Map step fields
    #[serde(default)]
    pub set: Vec<MapSetEntry>,
}

fn default_step_type() -> StepType {
    StepType::Process
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StepType {
    Process,
    Map,
    Condition,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StepWithConfig {
    #[serde(default)]
    pub timeout_secs: Option<u64>,
    #[serde(default)]
    pub timeout_action: Option<String>,
    #[serde(default)]
    pub kill_grace_secs: Option<u64>,
    #[serde(default)]
    pub args: Option<Vec<String>>,
    #[serde(default)]
    pub env: HashMap<String, String>,
    #[serde(default)]
    pub cmd: Option<String>,
    #[serde(default)]
    pub stdin: Option<StdinConfig>,
    #[serde(default)]
    pub stdout: Option<StreamConfig>,
    #[serde(default)]
    pub stderr: Option<StreamConfig>,
    #[serde(default)]
    pub success_exit_codes: Option<Vec<i32>>,
    #[serde(default)]
    pub working_dir: Option<PathBuf>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StepOutputsConfig {
    #[serde(default = "default_output_format")]
    pub format: String,
    #[serde(default)]
    pub fields: Vec<StepOutputField>,
}

fn default_output_format() -> String {
    "json".to_string()
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StepOutputField {
    #[serde(default)]
    pub key: String,
    #[serde(default)]
    pub name: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StepRetryConfig {
    #[serde(default)]
    pub max_attempts: Option<u32>,
    #[serde(default)]
    pub base_delay_ms: Option<u64>,
    #[serde(default)]
    pub max_delay_ms: Option<u64>,
    #[serde(default)]
    pub jitter: f64,
}

impl Default for StepRetryConfig {
    fn default() -> Self {
        Self {
            max_attempts: None,
            base_delay_ms: None,
            max_delay_ms: None,
            jitter: 0.2,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StepRoutingConfig {
    #[serde(default)]
    pub condition: String,
    #[serde(default)]
    pub sinks: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MapSetEntry {
    #[serde(default)]
    pub target: String,
    #[serde(default)]
    pub expr: String,
}

// ─────────────────────────────────────────────────────────────────────────────
// Environment variable substitution
// ─────────────────────────────────────────────────────────────────────────────

/// Substitute `${VAR_NAME}` patterns with OS environment values.
/// Unset variables cause an error.
pub fn substitute_env(input: &str) -> Result<String> {
    static ENV_VAR_RE: OnceLock<regex::Regex> = OnceLock::new();
    let re =
        ENV_VAR_RE.get_or_init(|| regex::Regex::new(r"\$\{([A-Za-z_][A-Za-z0-9_]*)\}").unwrap());
    let mut errors: Vec<String> = Vec::new();

    let result = re.replace_all(input, |caps: &regex::Captures| {
        let var_name = caps.get(1).unwrap().as_str();
        match std::env::var(var_name) {
            Ok(val) => val,
            Err(_) => {
                errors.push(format!(
                    "Environment variable '{}' is not set but referenced in config",
                    var_name
                ));
                String::new()
            }
        }
    });

    if !errors.is_empty() {
        return Err(Error::EnvVar(errors.join("\n")));
    }

    Ok(result.to_string())
}
