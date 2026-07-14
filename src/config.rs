use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::io::IsTerminal;
use std::path::{Path, PathBuf};
use std::sync::OnceLock;

use crate::error::{Error, Result};

// =============================================================================
// Public Config — the shape consumers expect (unchanged)
// =============================================================================

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
    /// Parses the merged universal TOML with an explicit `[bria]` namespace.
    pub fn load_from_path(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref();
        let raw = std::fs::read_to_string(path)
            .map_err(|e| Error::config(format!("Cannot read config file {:?}: {}", path, e)))?;
        Self::from_str_with_env(&raw)
    }

    /// Parse configuration from a merged universal TOML string with `${VAR}`
    /// and `${VAR:-default}` environment substitution. `[bria]` is required.
    pub fn from_str_with_env(raw: &str) -> Result<Self> {
        let resolved = substitute_env(raw)?;

        // Parse into a raw Value tree first for flexibility
        let val: toml::Value = toml::from_str(&resolved)
            .map_err(|e| Error::config(format!("TOML parse error: {}", e)))?;

        let table = val
            .as_table()
            .ok_or_else(|| Error::config("TOML must be a table at the root"))?;
        if !table.contains_key("bria") {
            return Err(Error::config(
                "Missing required [bria] namespace; unnested Bria configuration is not supported",
            ));
        }

        let universal = UniversalConfig::deserialize(val.clone())
            .map_err(|e| Error::config(format!("TOML deserialization error: {}", e)))?;
        universal.into_config()
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

        // Validate individual entities while retaining all errors for `bria check`.
        for source in &self.sources {
            if source.id.trim().is_empty() {
                errors.push("Source id must not be empty".to_string());
            }
            if source.enabled {
                if let Err(error) = source.validate() {
                    errors.push(error.to_string());
                }
            }
        }

        for sink in &self.sinks {
            if sink.id.trim().is_empty() {
                errors.push("Sink id must not be empty".to_string());
            }
            if sink.enabled {
                if let Err(error) = sink.validate() {
                    errors.push(error.to_string());
                }
            }
        }

        for task in &self.tasks {
            if task.id.trim().is_empty() {
                errors.push("Task id must not be empty".to_string());
            }
            if task.cmd.trim().is_empty() {
                errors.push(format!("Task '{}' requires a command", task.id));
            }
            if let Err(error) = task.validate() {
                errors.push(error.to_string());
            }
        }

        for pipeline in &self.pipelines {
            if pipeline.id.trim().is_empty() {
                errors.push("Pipeline id must not be empty".to_string());
            }
            if let Err(error) = pipeline.validate(&task_ids, &sink_ids, &source_ids) {
                errors.push(error.to_string());
            }
        }

        if self.global.max_payload_bytes == 0 {
            errors.push("global.max_payload_bytes must be greater than zero".to_string());
        }
        if self.server.enabled {
            if self.server.prefix.trim().is_empty()
                || self.server.prefix.trim().contains('/')
                || self.server.prefix.trim() != self.server.prefix
            {
                errors.push(
                    "server.prefix must be one non-empty path segment without surrounding whitespace"
                        .to_string(),
                );
            }
            if self.server.max_body_bytes == 0 {
                errors.push("server.max_body_bytes must be greater than zero".to_string());
            }

            let mut paths = HashSet::new();
            for source in &self.sources {
                if source.enabled && matches!(source.r#type, SourceType::Http | SourceType::Webhook)
                {
                    let path = source.path.to_string_lossy();
                    let path = path.trim_matches('/');
                    if path.is_empty() {
                        continue;
                    }
                    if !paths.insert(path.to_string()) {
                        errors.push(format!(
                            "HTTP/webhook source route '{}' is configured more than once",
                            path
                        ));
                    }
                    if path == "ping" || path == "pipelines" || path.starts_with("pipelines/") {
                        errors.push(format!(
                            "HTTP/webhook source route '{}' conflicts with an internal control route",
                            path
                        ));
                    }
                }
            }
        }

        // Validate server-dependent configs
        if !self.server.enabled {
            for source in &self.sources {
                match source.r#type {
                    SourceType::Http | SourceType::Webhook if source.enabled => {
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
                if sink.enabled && sink.r#type == SinkType::Stream {
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
            if source.enabled && source.r#type == SourceType::Cron {
                errors.push(format!(
                    "Source '{}' type 'cron' requires the 'cron' feature",
                    source.id
                ));
            }
        }
        #[cfg(not(feature = "amqp"))]
        {
            for source in &self.sources {
                if source.enabled && source.r#type == SourceType::Queue {
                    errors.push(format!(
                        "Source '{}' type 'queue' requires the 'amqp' feature",
                        source.id
                    ));
                }
            }
            for sink in &self.sinks {
                if sink.enabled && sink.r#type == SinkType::Queue {
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
                if source.enabled && source.r#type == SourceType::Sqlite {
                    errors.push(format!(
                        "Source '{}' type 'sqlite' requires the 'sqlite' feature",
                        source.id
                    ));
                }
            }
            for sink in &self.sinks {
                if sink.enabled && sink.r#type == SinkType::Sqlite {
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
                if source.enabled && source.r#type == SourceType::Pg {
                    errors.push(format!(
                        "Source '{}' type 'pg' requires the 'postgres' feature",
                        source.id
                    ));
                }
            }
            for sink in &self.sinks {
                if sink.enabled && sink.r#type == SinkType::Pg {
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
            if sink.enabled && sink.r#type == SinkType::Webhook {
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

// =============================================================================
// Universal config parsing layer
// =============================================================================

/// Parsed representation of the merged universal TOML.
/// Ignores peer package namespaces; accepts only known shared root sections
/// and the [bria] namespace.  Unknown fields inside [bria] are rejected by
/// the serde(deny_unknown_fields) on BriaConfig and its children.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)] // reject unknown ROOT-level tables
struct UniversalConfig {
    #[serde(rename = "version", default)]
    _version: Option<u64>,
    #[serde(rename = "meta", default)]
    _meta: Option<SharedMetaConfig>,
    #[serde(default)]
    log: Option<SharedLogConfig>,
    #[serde(default)]
    runtime: Option<SharedRuntimeConfig>,
    #[serde(default)]
    http: Option<SharedHttpConfig>,
    #[serde(default)]
    stores: HashMap<String, SharedStoreConfig>,
    #[serde(default)]
    transports: Option<SharedTransportsSection>,
    #[serde(default)]
    paths: HashMap<String, SharedPathConfig>,
    #[serde(rename = "objects", default)]
    _objects: HashMap<String, SharedObjectConfig>,
    #[serde(rename = "chains", default)]
    _chains: HashMap<String, SharedChainConfig>,
    #[serde(rename = "assets", default)]
    _assets: HashMap<String, SharedAssetConfig>,
    #[serde(default)]
    bria: BriaConfig,
    // --- explicitly ignore other package namespaces ---
    #[serde(rename = "artur", default, skip_serializing)]
    _artur: Option<serde::de::IgnoredAny>,
    #[serde(rename = "ladon", default, skip_serializing)]
    _ladon: Option<serde::de::IgnoredAny>,
    #[serde(rename = "pano", default, skip_serializing)]
    _pano: Option<serde::de::IgnoredAny>,
    #[serde(rename = "oracles", default, skip_serializing)]
    _oracles: Option<serde::de::IgnoredAny>,
}

impl UniversalConfig {
    fn into_config(self) -> Result<Config> {
        let bria = &self.bria;

        // --- global ---
        let runtime = self.runtime.as_ref();
        let log = self.log.as_ref();

        let mut global = GlobalConfig::default();

        if let Some(ref bria_global) = bria.global {
            if bria_global.worker_threads > 0 {
                global.worker_threads = bria_global.worker_threads;
            } else if let Some(ref r) = runtime {
                global.worker_threads = r.worker_threads;
            }
            global.shutdown_timeout_secs = bria_global
                .shutdown_timeout_secs
                .unwrap_or_else(|| runtime.map(|r| r.shutdown_timeout_secs).unwrap_or(30));
            if let Some(ref tmp) = bria_global.tmp_dir {
                global.tmp_dir = PathBuf::from(tmp);
            } else if let Some(ref r) = runtime {
                if !r.tmp_dir.is_empty() {
                    global.tmp_dir = PathBuf::from(&r.tmp_dir);
                }
            }
            global.max_payload_bytes = bria_global.max_payload_bytes.unwrap_or_else(|| {
                runtime
                    .map(|r| r.max_payload_bytes)
                    .unwrap_or(10 * 1024 * 1024)
            });
            global.cancel_signal_ttl_secs = bria_global.cancel_signal_ttl_secs.unwrap_or(3600);

            // Log config
            if let Some(ref bria_log) = bria_global.log {
                global.log.level = bria_log.level.clone().unwrap_or_else(|| {
                    log.and_then(|l| l.level.clone())
                        .unwrap_or_else(|| "info".to_string())
                });
                global.log.format = bria_log
                    .format
                    .clone()
                    .or_else(|| log.and_then(|l| l.format.clone()));
                global.log.file = bria_log
                    .file
                    .clone()
                    .unwrap_or_else(|| log.and_then(|l| l.file.clone()).unwrap_or_default());
            } else if let Some(ref l) = log {
                global.log.level = l.level.clone().unwrap_or_else(|| "info".to_string());
                global.log.format = l.format.clone();
                global.log.file = l.file.clone().unwrap_or_default();
            }

            // State config
            if let Some(ref bria_state) = bria_global.state {
                global.state.backend = bria_state
                    .backend
                    .clone()
                    .unwrap_or_else(|| "memory".to_string());
                global.state.sqlite_path = bria_state
                    .sqlite_path
                    .clone()
                    .unwrap_or_else(|| "bria-state.db".to_string());
                global.state.pg_url = bria_state.pg_url.clone().unwrap_or_default();
                // Resolve store reference
                if let Some(ref store_id) = bria_state.store {
                    if let Some(store_cfg) = self.stores.get(store_id) {
                        match store_cfg.driver.as_deref().unwrap_or("sqlite") {
                            "sqlite" => {
                                if global.state.sqlite_path.is_empty()
                                    || global.state.sqlite_path == "bria-state.db"
                                {
                                    let path = sqlite_url_to_path(&store_cfg.url);
                                    if !path.is_empty() {
                                        global.state.sqlite_path = path;
                                    }
                                }
                            }
                            "postgres" | "pg" => {
                                if global.state.pg_url.is_empty() {
                                    global.state.pg_url = store_cfg.url.clone();
                                }
                            }
                            _ => {}
                        }
                    }
                }
            }

            // Retry defaults
            if let Some(ref retry) = bria_global.retry {
                global.retry.max_attempts = retry.max_attempts.unwrap_or(0);
                global.retry.base_delay_ms = retry.base_delay_ms.unwrap_or(1000);
                global.retry.max_delay_ms = retry.max_delay_ms.unwrap_or(30000);
                global.retry.jitter = retry.jitter.unwrap_or(0.2);
            }
            // Timeout defaults
            if let Some(ref timeout) = bria_global.timeout {
                global.timeout.step_secs = timeout.step_secs.unwrap_or(300);
                global.timeout.action =
                    timeout.action.clone().unwrap_or_else(|| "kill".to_string());
                global.timeout.kill_grace_secs = timeout.kill_grace_secs.unwrap_or(5);
            }
        } else {
            // Inherit shared root defaults when bria.global is absent
            if let Some(ref r) = runtime {
                global.worker_threads = r.worker_threads;
                global.shutdown_timeout_secs = r.shutdown_timeout_secs;
                if !r.tmp_dir.is_empty() {
                    global.tmp_dir = PathBuf::from(&r.tmp_dir);
                }
                global.max_payload_bytes = r.max_payload_bytes;
            }
            if let Some(ref l) = log {
                global.log.level = l.level.clone().unwrap_or_else(|| "info".to_string());
                global.log.format = l.format.clone();
                global.log.file = l.file.clone().unwrap_or_default();
            }
        }

        // --- server ---
        let http = self.http.as_ref();
        let mut server = ServerConfig::default();

        if let Some(ref bria_server) = bria.server {
            server.enabled = bria_server.enabled.unwrap_or(false);
            server.bind = bria_server.bind.clone().unwrap_or_else(|| {
                http.map(|h| h.bind.clone())
                    .flatten()
                    .unwrap_or_else(|| "0.0.0.0".to_string())
            });
            server.port = bria_server.port.unwrap_or(4000);
            server.prefix = bria_server.prefix.clone().unwrap_or_else(|| {
                http.map(|h| h.prefix.clone())
                    .flatten()
                    .unwrap_or_else(|| "v1".to_string())
            });
            server.api_key = bria_server
                .api_key
                .clone()
                .unwrap_or_else(|| http.and_then(|h| h.api_key.clone()).unwrap_or_default());
            // Resolve dashboard_path_ref
            if let Some(ref dashboard_ref) = bria_server.dashboard_path_ref {
                if !dashboard_ref.is_empty() {
                    if let Some(path_cfg) = self.paths.get(dashboard_ref) {
                        server.dashboard = path_cfg.path.clone();
                    } else {
                        return Err(Error::config(format!(
                            "bria.server.dashboard_path_ref '{}' not found in [paths]",
                            dashboard_ref
                        )));
                    }
                }
            }
            server.shutdown_timeout_secs = bria_server.shutdown_timeout_secs.unwrap_or(5);
            server.max_body_bytes = bria_server.max_body_bytes.unwrap_or(52428800);
        }

        // --- sources ---
        let mut sources: Vec<SourceConfig> = Vec::new();
        for bria_src in &bria.sources {
            let s = resolve_source(bria_src, &self.paths, &self.transports, &self.stores)?;
            sources.push(s);
        }

        // --- tasks ---
        let mut tasks: Vec<TaskConfig> = Vec::new();
        for bria_task in &bria.tasks {
            tasks.push(resolve_task(bria_task)?);
        }

        // --- sinks ---
        let mut sinks: Vec<SinkConfig> = Vec::new();
        for bria_sink in &bria.sinks {
            let s = resolve_sink(bria_sink, &self.paths, &self.transports, &self.stores)?;
            sinks.push(s);
        }

        // --- pipelines ---
        let mut pipelines: Vec<PipelineConfig> = Vec::new();
        for bria_pl in &bria.pipelines {
            let mut pl = resolve_pipeline(bria_pl)?;
            pl.resolve_sources();
            pipelines.push(pl);
        }

        let config = Config {
            global,
            server,
            sources,
            tasks,
            sinks,
            pipelines,
        };

        Ok(config)
    }
}

// =============================================================================
// Shared root section structs
// =============================================================================

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct SharedMetaConfig {
    #[serde(rename = "name", default)]
    _name: String,
    #[serde(rename = "environment", default)]
    _environment: String,
    #[serde(rename = "data_dir", default)]
    _data_dir: String,
    #[serde(rename = "profile", default)]
    _profile: String,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct SharedLogConfig {
    #[serde(default)]
    level: Option<String>,
    #[serde(default)]
    format: Option<String>,
    #[serde(default)]
    file: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct SharedRuntimeConfig {
    #[serde(default = "default_runtime_worker_threads")]
    worker_threads: usize,
    #[serde(default = "default_runtime_shutdown")]
    shutdown_timeout_secs: u64,
    #[serde(default)]
    tmp_dir: String,
    #[serde(default = "default_runtime_max_payload")]
    max_payload_bytes: usize,
}

fn default_runtime_worker_threads() -> usize {
    0
}
fn default_runtime_shutdown() -> u64 {
    30
}
fn default_runtime_max_payload() -> usize {
    10 * 1024 * 1024
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct SharedHttpConfig {
    #[serde(rename = "user_agent", default)]
    _user_agent: String,
    #[serde(rename = "request_timeout_secs", default)]
    _request_timeout_secs: u64,
    #[serde(rename = "max_retries", default)]
    _max_retries: u32,
    #[serde(rename = "retry_backoff_ms", default)]
    _retry_backoff_ms: u64,
    #[serde(default)]
    bind: Option<String>,
    #[serde(default)]
    prefix: Option<String>,
    #[serde(default)]
    api_key: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct SharedStoreConfig {
    #[serde(default)]
    driver: Option<String>,
    #[serde(default)]
    url: String,
    #[serde(rename = "migrate", default)]
    _migrate: bool,
    #[serde(rename = "connect_timeout_secs", default)]
    _connect_timeout_secs: u64,
    #[serde(rename = "max_connections", default)]
    _max_connections: u32,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct SharedTransportsSection {
    #[serde(default)]
    amqp: HashMap<String, SharedTransportAmqpConfig>,
    #[serde(default)]
    webhook: HashMap<String, SharedTransportWebhookConfig>,
    #[serde(rename = "http", default)]
    _http: HashMap<String, SharedTransportHttpConfig>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct SharedTransportAmqpConfig {
    #[serde(default)]
    url: String,
    #[serde(default)]
    username: String,
    #[serde(default)]
    password: String,
    #[serde(rename = "virtual_host", default)]
    _virtual_host: String,
    #[serde(default)]
    reconnect_secs: u64,
    #[serde(default)]
    qos_prefetch: u16,
    #[serde(rename = "tls", default)]
    _tls: String,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct SharedTransportWebhookConfig {
    #[serde(default)]
    url: String,
    #[serde(rename = "method", default)]
    _method: String,
    #[serde(rename = "auth_scheme", default)]
    _auth_scheme: String,
    #[serde(default)]
    token: String,
    #[serde(default)]
    auth_header: String,
    #[serde(default)]
    timeout_secs: u64,
    #[serde(default)]
    max_retries: u32,
    #[serde(default)]
    retry_base_ms: u64,
    #[serde(default)]
    headers: Option<HashMap<String, String>>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct SharedTransportHttpConfig {
    #[serde(rename = "base_url", default)]
    _base_url: String,
    #[serde(rename = "user_agent", default)]
    _user_agent: String,
    #[serde(rename = "timeout_secs", default)]
    _timeout_secs: u64,
    #[serde(rename = "max_retries", default)]
    _max_retries: u32,
    #[serde(rename = "retry_base_ms", default)]
    _retry_base_ms: u64,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct SharedPathConfig {
    #[serde(rename = "kind", default)]
    _kind: String,
    #[serde(default)]
    path: String,
    #[serde(rename = "format", default)]
    _format: String,
    #[serde(rename = "create_parent_dirs", default)]
    _create_parent_dirs: bool,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct SharedObjectConfig {
    #[serde(rename = "driver", default)]
    _driver: String,
    #[serde(rename = "root", default)]
    _root: String,
    #[serde(rename = "public_base_url", default)]
    _public_base_url: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct SharedChainConfig {
    #[serde(rename = "family", default)]
    _family: String,
    #[serde(rename = "caip2", default)]
    _caip2: String,
    #[serde(rename = "native_symbol", default)]
    _native_symbol: String,
    #[serde(rename = "rpc_urls", default)]
    _rpc_urls: Vec<String>,
    #[serde(rename = "confirmations", default)]
    _confirmations: u32,
    #[serde(rename = "derivation", default)]
    _derivation: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct SharedAssetConfig {
    #[serde(rename = "enabled", default)]
    _enabled: bool,
    #[serde(rename = "chain", default)]
    _chain: String,
    #[serde(rename = "symbol", default)]
    _symbol: String,
    #[serde(rename = "name", default)]
    _name: String,
    #[serde(rename = "kind", default)]
    _kind: String,
    #[serde(rename = "decimals", default)]
    _decimals: u32,
    #[serde(rename = "contract", default)]
    _contract: Option<String>,
}

// =============================================================================
// Bria namespace structs — strict unknown-field rejection
// =============================================================================

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct BriaConfig {
    #[serde(rename = "enabled", default)]
    _enabled: Option<bool>,
    #[serde(default)]
    global: Option<BriaGlobalConfig>,
    #[serde(default)]
    server: Option<BriaServerConfig>,
    #[serde(default)]
    sources: Vec<BriaSourceConfig>,
    #[serde(default)]
    tasks: Vec<BriaTaskConfig>,
    #[serde(default)]
    sinks: Vec<BriaSinkConfig>,
    #[serde(default)]
    pipelines: Vec<BriaPipelineConfig>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct BriaGlobalConfig {
    #[serde(default)]
    worker_threads: usize,
    #[serde(default)]
    shutdown_timeout_secs: Option<u64>,
    #[serde(default)]
    tmp_dir: Option<String>,
    #[serde(default)]
    max_payload_bytes: Option<usize>,
    #[serde(default)]
    cancel_signal_ttl_secs: Option<u64>,
    #[serde(default)]
    log: Option<BriaLogConfig>,
    #[serde(default)]
    state: Option<BriaStateConfig>,
    #[serde(default)]
    retry: Option<BriaRetryConfig>,
    #[serde(default)]
    timeout: Option<BriaTimeoutConfig>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct BriaLogConfig {
    #[serde(default)]
    level: Option<String>,
    #[serde(default)]
    format: Option<String>,
    #[serde(default)]
    file: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct BriaStateConfig {
    #[serde(default)]
    backend: Option<String>,
    /// Store reference to [stores.<id>]
    #[serde(default)]
    store: Option<String>,
    #[serde(default)]
    sqlite_path: Option<String>,
    #[serde(default)]
    pg_url: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct BriaRetryConfig {
    #[serde(default)]
    max_attempts: Option<u32>,
    #[serde(default)]
    base_delay_ms: Option<u64>,
    #[serde(default)]
    max_delay_ms: Option<u64>,
    #[serde(default)]
    jitter: Option<f64>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct BriaTimeoutConfig {
    #[serde(default)]
    step_secs: Option<u64>,
    #[serde(default)]
    action: Option<String>,
    #[serde(default)]
    kill_grace_secs: Option<u64>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct BriaServerConfig {
    #[serde(default)]
    enabled: Option<bool>,
    #[serde(default)]
    bind: Option<String>,
    #[serde(default)]
    port: Option<u16>,
    #[serde(default)]
    prefix: Option<String>,
    #[serde(default)]
    api_key: Option<String>,
    #[serde(default)]
    dashboard_path_ref: Option<String>,
    #[serde(default)]
    shutdown_timeout_secs: Option<u64>,
    #[serde(default)]
    max_body_bytes: Option<usize>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct BriaSourceConfig {
    #[serde(default)]
    id: String,
    #[serde(rename = "type")]
    #[serde(default)]
    r#type: String,
    #[serde(rename = "enabled", default)]
    _enabled: Option<bool>,
    // Path reference
    #[serde(default)]
    path_ref: Option<String>,
    // Direct path
    #[serde(default)]
    path: String,
    #[serde(default)]
    poll_interval_secs: Option<u64>,
    #[serde(default)]
    track_cursor: Option<bool>,
    #[serde(default)]
    authoritative: Option<bool>,
    #[serde(default)]
    id_field: Option<String>,
    #[serde(default)]
    max_body_bytes: Option<usize>,
    // AMQP transport reference
    #[serde(default)]
    transport: Option<String>,
    // AMQP/Queue fields
    #[serde(default)]
    url: Option<String>,
    #[serde(default)]
    username: Option<String>,
    #[serde(default)]
    password: Option<String>,
    #[serde(default)]
    exchange: Option<String>,
    #[serde(default)]
    submit_routing_key: Option<String>,
    #[serde(default)]
    cancel_routing_key: Option<String>,
    #[serde(default)]
    reconnect_secs: Option<u64>,
    #[serde(default)]
    qos_prefetch: Option<u16>,
    #[serde(default)]
    consumer_tag: Option<String>,
    // Webhook
    #[serde(default)]
    hmac_secret: Option<String>,
    #[serde(default)]
    hmac_header: Option<String>,
    #[serde(default)]
    ack_status: Option<u16>,
    // Cron
    #[serde(default)]
    schedule: Option<String>,
    #[serde(default)]
    tz: Option<String>,
    #[serde(default)]
    labels: Option<HashMap<String, String>>,
    #[serde(default)]
    payload: Option<serde_json::Value>,
    // Table (pg/sqlite)
    #[serde(default)]
    table: Option<BriaTableSourceConfig>,
    // Store reference for pg/sqlite sources
    #[serde(default)]
    store: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct BriaTableSourceConfig {
    #[serde(default)]
    name: Option<String>,
    #[serde(default)]
    columns: Option<BriaTableSourceColumnsConfig>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct BriaTableSourceColumnsConfig {
    #[serde(default)]
    id: Option<String>,
    #[serde(default)]
    payload: Option<String>,
    #[serde(default)]
    created_at: Option<String>,
    #[serde(default)]
    status: Option<String>,
    #[serde(default)]
    status_claimed_value: Option<String>,
    #[serde(default)]
    status_done_value: Option<String>,
    #[serde(default)]
    status_failed_value: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct BriaTaskConfig {
    #[serde(default)]
    id: String,
    #[serde(default)]
    driver: Option<String>,
    #[serde(default)]
    cmd: String,
    #[serde(default)]
    args: Option<Vec<String>>,
    #[serde(default)]
    inherit_env: Option<bool>,
    #[serde(default)]
    working_dir: Option<String>,
    #[serde(default)]
    success_exit_codes: Option<Vec<i32>>,
    #[serde(default)]
    timeout_secs: Option<u64>,
    #[serde(default)]
    timeout_action: Option<String>,
    #[serde(default)]
    kill_grace_secs: Option<u64>,
    #[serde(default)]
    env: Option<HashMap<String, String>>,
    #[serde(default)]
    stdin: Option<StdinConfig>,
    #[serde(default)]
    stdout: Option<StreamConfig>,
    #[serde(default)]
    stderr: Option<StreamConfig>,
    #[serde(default)]
    retry: Option<BriaTaskRetryConfig>,
    #[serde(default)]
    labels: Option<HashMap<String, String>>,
    #[serde(default)]
    docker: Option<DockerConfig>,
    #[serde(default)]
    wasm: Option<WasmConfig>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct BriaTaskRetryConfig {
    #[serde(default)]
    max_attempts: Option<u32>,
    #[serde(default)]
    base_delay_ms: Option<u64>,
    #[serde(default)]
    max_delay_ms: Option<u64>,
    #[serde(default)]
    jitter: Option<f64>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct BriaSinkConfig {
    #[serde(default)]
    id: String,
    #[serde(rename = "type")]
    #[serde(default)]
    r#type: String,
    #[serde(rename = "enabled", default)]
    _enabled: Option<bool>,
    // File
    #[serde(default)]
    path: Option<String>,
    #[serde(default)]
    path_ref: Option<String>,
    #[serde(default)]
    template: Option<String>,
    // Webhook
    #[serde(default)]
    transport: Option<String>,
    #[serde(default)]
    url: Option<String>,
    #[serde(default)]
    secret: Option<String>,
    #[serde(default)]
    signature_header: Option<String>,
    #[serde(default)]
    content_type: Option<String>,
    #[serde(default)]
    max_retries: Option<u32>,
    #[serde(default)]
    retry_base_ms: Option<u64>,
    #[serde(default)]
    timeout_secs: Option<u64>,
    #[serde(rename = "headers", default)]
    _headers: Option<HashMap<String, String>>,
    // Queue
    #[serde(default)]
    username: Option<String>,
    #[serde(default)]
    password: Option<String>,
    #[serde(default)]
    exchange: Option<String>,
    #[serde(default)]
    success_routing_key: Option<String>,
    #[serde(default)]
    failure_routing_key: Option<String>,
    #[serde(default)]
    reconnect_secs: Option<u64>,
    // Stream
    #[serde(default)]
    sse: Option<String>,
    #[serde(default)]
    websocket: Option<String>,
    #[serde(default)]
    ws_heartbeat_secs: Option<u64>,
    #[serde(default)]
    sse_keepalive_secs: Option<u64>,
    #[serde(default)]
    broadcast_capacity: Option<usize>,
    // Table
    #[serde(default)]
    table: Option<BriaTableSinkConfig>,
    #[serde(default)]
    table_name: Option<String>,
    #[serde(default)]
    store: Option<String>,
    #[serde(default)]
    db_table: Option<BriaDbTableConfig>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct BriaDbTableConfig {
    #[serde(default)]
    columns: Option<TableSinkColumnsConfig>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct BriaTableSinkConfig {
    #[serde(default)]
    name: Option<String>,
    #[serde(default)]
    columns: Option<TableSinkColumnsConfig>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct BriaPipelineConfig {
    #[serde(default)]
    id: String,
    #[serde(default)]
    source: Option<String>,
    #[serde(default)]
    sources: Option<Vec<PipelineSourceEntry>>,
    #[serde(default)]
    merge: Option<MergeConfig>,
    #[serde(default)]
    concurrency: Option<usize>,
    #[serde(default)]
    queue_capacity: Option<usize>,
    #[serde(default)]
    sinks: Option<Vec<String>>,
    #[serde(default)]
    failure: Option<FailureConfig>,
    #[serde(default)]
    labels: Option<HashMap<String, String>>,
    #[serde(default)]
    steps: Vec<StepConfig>,
}

// =============================================================================
// Resolution helpers
// =============================================================================

fn sqlite_url_to_path(url: &str) -> String {
    if let Some(rest) = url.strip_prefix("sqlite://") {
        rest.to_string()
    } else if let Some(rest) = url.strip_prefix("sqlite:") {
        rest.to_string()
    } else {
        url.to_string()
    }
}

fn resolve_source(
    bria: &BriaSourceConfig,
    paths: &HashMap<String, SharedPathConfig>,
    transports: &Option<SharedTransportsSection>,
    stores: &HashMap<String, SharedStoreConfig>,
) -> Result<SourceConfig> {
    let mut s = SourceConfig::default_with_id(&bria.id);

    let src_type: SourceType = match bria.r#type.as_str() {
        "file" => SourceType::File,
        "http" => SourceType::Http,
        "webhook" => SourceType::Webhook,
        "queue" => SourceType::Queue,
        "cron" => SourceType::Cron,
        "pg" => SourceType::Pg,
        "sqlite" => SourceType::Sqlite,
        "" => {
            return Err(Error::config(format!(
                "Source '{}' missing required 'type' field",
                bria.id
            )));
        }
        other => {
            return Err(Error::config(format!(
                "Source '{}' unknown type '{}'",
                bria.id, other
            )));
        }
    };
    s.r#type = src_type.clone();
    s.enabled = bria._enabled.unwrap_or(true);
    if let Some(ref direct_url) = bria.url {
        if !direct_url.is_empty() {
            s.url = direct_url.clone();
        }
    }

    // Path resolution: local path > path_ref > default
    if s.enabled && !bria.path.is_empty() {
        s.path = PathBuf::from(&bria.path);
    } else if s.enabled
        && let Some(ref path_ref) = bria.path_ref
    {
        if !path_ref.is_empty() {
            if let Some(path_cfg) = paths.get(path_ref) {
                s.path = PathBuf::from(&path_cfg.path);
            } else {
                return Err(Error::config(format!(
                    "Source '{}' path_ref '{}' not found in [paths]",
                    bria.id, path_ref
                )));
            }
        }
    }

    // AMQP transport resolution for queue sources
    if s.enabled && src_type == SourceType::Queue {
        if let Some(ref transport_id) = bria.transport {
            if let Some(ts) = transports {
                if let Some(amqp_cfg) = ts.amqp.get(transport_id) {
                    s.url = bria.url.clone().unwrap_or_else(|| amqp_cfg.url.clone());
                    s.username = bria
                        .username
                        .clone()
                        .unwrap_or_else(|| amqp_cfg.username.clone());
                    s.password = bria
                        .password
                        .clone()
                        .unwrap_or_else(|| amqp_cfg.password.clone());
                    s.reconnect_secs = bria.reconnect_secs.unwrap_or(amqp_cfg.reconnect_secs);
                    s.qos_prefetch = bria.qos_prefetch.unwrap_or(amqp_cfg.qos_prefetch);
                } else {
                    return Err(Error::config(format!(
                        "Source '{}' transport '{}' not found in [transports.amqp]",
                        bria.id, transport_id
                    )));
                }
            } else {
                return Err(Error::config(format!(
                    "Source '{}' references transport '{}' but no [transports] section exists",
                    bria.id, transport_id
                )));
            }
        }
    }

    // Store resolution for pg/sqlite sources
    if s.enabled && (src_type == SourceType::Pg || src_type == SourceType::Sqlite) {
        if let Some(ref store_id) = bria.store {
            if let Some(store_cfg) = stores.get(store_id) {
                if src_type == SourceType::Pg {
                    s.url = bria.url.clone().unwrap_or_else(|| store_cfg.url.clone());
                } else {
                    // SQLite: resolve store URL to path if no direct path
                    if s.path.as_os_str().is_empty() {
                        let p = sqlite_url_to_path(&store_cfg.url);
                        if !p.is_empty() {
                            s.path = PathBuf::from(p);
                        }
                    }
                }
            } else {
                return Err(Error::config(format!(
                    "Source '{}' store '{}' not found in [stores]",
                    bria.id, store_id
                )));
            }
        }
    }

    // General overrides
    s.poll_interval_secs = bria.poll_interval_secs.unwrap_or(2);
    s.track_cursor = bria.track_cursor.unwrap_or(true);
    s.authoritative = bria.authoritative.unwrap_or(false);
    s.id_field = bria.id_field.clone().unwrap_or_default();
    s.max_body_bytes = bria.max_body_bytes.unwrap_or(1_048_576);
    s.exchange = bria.exchange.clone().unwrap_or_default();
    s.submit_routing_key = bria
        .submit_routing_key
        .clone()
        .unwrap_or_else(|| "job.submit".to_string());
    s.cancel_routing_key = bria
        .cancel_routing_key
        .clone()
        .unwrap_or_else(|| "job.cancel".to_string());
    s.consumer_tag = bria
        .consumer_tag
        .clone()
        .unwrap_or_else(|| "bria-source".to_string());
    s.hmac_secret = bria.hmac_secret.clone().unwrap_or_default();
    s.hmac_header = bria
        .hmac_header
        .clone()
        .unwrap_or_else(|| "X-Bria-Signature".to_string());
    s.ack_status = bria.ack_status.unwrap_or(202);
    s.schedule = bria.schedule.clone().unwrap_or_default();
    s.tz = bria.tz.clone().unwrap_or_else(|| "UTC".to_string());
    s.labels = bria.labels.clone().unwrap_or_default();
    s.payload = bria.payload.clone().unwrap_or_default();

    if let Some(ref table) = bria.table {
        let mut t = TableSourceConfig::default();
        t.name = table.name.clone().unwrap_or_default();
        if let Some(ref cols) = table.columns {
            t.columns.id = cols.id.clone().unwrap_or_else(|| "id".to_string());
            t.columns.payload = cols
                .payload
                .clone()
                .unwrap_or_else(|| "payload".to_string());
            t.columns.created_at = cols
                .created_at
                .clone()
                .unwrap_or_else(|| "created_at".to_string());
            t.columns.status = cols.status.clone().unwrap_or_else(|| "status".to_string());
            t.columns.status_claimed_value = cols
                .status_claimed_value
                .clone()
                .unwrap_or_else(|| "processing".to_string());
            t.columns.status_done_value = cols
                .status_done_value
                .clone()
                .unwrap_or_else(|| "done".to_string());
            t.columns.status_failed_value = cols
                .status_failed_value
                .clone()
                .unwrap_or_else(|| "failed".to_string());
        }
        s.table = Some(t);
    }

    Ok(s)
}

fn resolve_task(bria: &BriaTaskConfig) -> Result<TaskConfig> {
    let mut t = TaskConfig::default_with_id(&bria.id);
    t.driver = bria.driver.clone().unwrap_or_else(|| "local".to_string());
    t.cmd = bria.cmd.clone();
    t.args = bria.args.clone().unwrap_or_default();
    t.inherit_env = bria.inherit_env.unwrap_or(false);
    t.working_dir = bria.working_dir.clone().map(PathBuf::from);
    t.success_exit_codes = bria.success_exit_codes.clone().unwrap_or_else(|| vec![0]);
    t.timeout_secs = bria.timeout_secs.unwrap_or(300);
    t.timeout_action = bria
        .timeout_action
        .clone()
        .unwrap_or_else(|| "kill".to_string());
    t.kill_grace_secs = bria.kill_grace_secs.unwrap_or(5);
    t.env = bria.env.clone().unwrap_or_default();
    t.stdin = bria.stdin.clone().unwrap_or_default();
    t.stdout = bria
        .stdout
        .clone()
        .unwrap_or_else(StreamConfig::default_capture);
    t.stderr = bria
        .stderr
        .clone()
        .unwrap_or_else(StreamConfig::default_stderr);
    t.labels = bria.labels.clone().unwrap_or_default();
    t.docker = bria.docker.clone();
    t.wasm = bria.wasm.clone();

    if let Some(ref retry) = bria.retry {
        t.retry.max_attempts = retry.max_attempts.unwrap_or(0);
        t.retry.base_delay_ms = retry.base_delay_ms.unwrap_or(1000);
        t.retry.max_delay_ms = retry.max_delay_ms.unwrap_or(30000);
        t.retry.jitter = retry.jitter.unwrap_or(0.2);
    }

    Ok(t)
}

fn resolve_sink(
    bria: &BriaSinkConfig,
    paths: &HashMap<String, SharedPathConfig>,
    transports: &Option<SharedTransportsSection>,
    stores: &HashMap<String, SharedStoreConfig>,
) -> Result<SinkConfig> {
    let mut s = SinkConfig::default_with_id(&bria.id);

    let sink_type: SinkType = match bria.r#type.as_str() {
        "file" => SinkType::File,
        "webhook" => SinkType::Webhook,
        "queue" => SinkType::Queue,
        "pg" => SinkType::Pg,
        "sqlite" => SinkType::Sqlite,
        "stream" => SinkType::Stream,
        "" => {
            return Err(Error::config(format!(
                "Sink '{}' missing required 'type' field",
                bria.id
            )));
        }
        other => {
            return Err(Error::config(format!(
                "Sink '{}' unknown type '{}'",
                bria.id, other
            )));
        }
    };
    s.r#type = sink_type.clone();
    s.enabled = bria._enabled.unwrap_or(true);
    if let Some(ref direct_url) = bria.url {
        if !direct_url.is_empty() {
            s.url = direct_url.clone();
        }
    }

    // Path resolution
    if s.enabled
        && let Some(ref direct_path) = bria.path
    {
        if !direct_path.is_empty() {
            s.path = direct_path.clone();
        }
    }
    if s.enabled && s.path.is_empty() {
        if let Some(ref path_ref) = bria.path_ref {
            if !path_ref.is_empty() {
                if let Some(path_cfg) = paths.get(path_ref) {
                    s.path = path_cfg.path.clone();
                } else {
                    return Err(Error::config(format!(
                        "Sink '{}' path_ref '{}' not found in [paths]",
                        bria.id, path_ref
                    )));
                }
            }
        }
    }

    // Transport resolution for webhook sinks
    if s.enabled && sink_type == SinkType::Webhook {
        if let Some(ref transport_id) = bria.transport {
            if let Some(ts) = transports {
                if let Some(wh_cfg) = ts.webhook.get(transport_id) {
                    s.url = bria.url.clone().unwrap_or_else(|| wh_cfg.url.clone());
                    s.secret = bria.secret.clone().unwrap_or_else(|| wh_cfg.token.clone());
                    s.signature_header = bria
                        .signature_header
                        .clone()
                        .unwrap_or_else(|| wh_cfg.auth_header.clone());
                    s.max_retries = bria.max_retries.unwrap_or(wh_cfg.max_retries);
                    s.retry_base_ms = bria.retry_base_ms.unwrap_or(wh_cfg.retry_base_ms);
                    s.timeout_secs = bria.timeout_secs.unwrap_or(wh_cfg.timeout_secs);
                    if let Some(ref hdrs) = wh_cfg.headers {
                        s.headers = hdrs.clone();
                    }
                } else {
                    return Err(Error::config(format!(
                        "Sink '{}' transport '{}' not found in [transports.webhook]",
                        bria.id, transport_id
                    )));
                }
            } else {
                return Err(Error::config(format!(
                    "Sink '{}' references transport '{}' but no [transports] section exists",
                    bria.id, transport_id
                )));
            }
        }
    }

    // Transport resolution for queue sinks
    if s.enabled && sink_type == SinkType::Queue {
        if let Some(ref transport_id) = bria.transport {
            if let Some(ts) = transports {
                if let Some(amqp_cfg) = ts.amqp.get(transport_id) {
                    s.url = bria.url.clone().unwrap_or_else(|| amqp_cfg.url.clone());
                    s.username = bria
                        .username
                        .clone()
                        .unwrap_or_else(|| amqp_cfg.username.clone());
                    s.password = bria
                        .password
                        .clone()
                        .unwrap_or_else(|| amqp_cfg.password.clone());
                    s.reconnect_secs = bria.reconnect_secs.unwrap_or(amqp_cfg.reconnect_secs);
                }
            }
        }
    }

    // Store resolution for pg/sqlite sinks
    if s.enabled && (sink_type == SinkType::Pg || sink_type == SinkType::Sqlite) {
        if let Some(ref store_id) = bria.store {
            if let Some(store_cfg) = stores.get(store_id) {
                if sink_type == SinkType::Pg {
                    s.url = bria.url.clone().unwrap_or_else(|| store_cfg.url.clone());
                } else {
                    if s.path.is_empty() {
                        let p = sqlite_url_to_path(&store_cfg.url);
                        if !p.is_empty() {
                            s.path = p;
                        }
                    }
                }
            }
        }
    }

    s.template = bria.template.clone();
    s.signature_header = if bria.signature_header.is_some() {
        bria.signature_header.clone().unwrap_or_default()
    } else {
        s.signature_header
    };
    s.content_type = bria
        .content_type
        .clone()
        .unwrap_or_else(|| "application/json".to_string());
    s.exchange = bria.exchange.clone().unwrap_or_default();
    s.success_routing_key = bria.success_routing_key.clone().unwrap_or_default();
    s.failure_routing_key = bria.failure_routing_key.clone().unwrap_or_default();
    s.sse = bria.sse.clone().unwrap_or_else(|| "sse".to_string());
    s.websocket = bria.websocket.clone().unwrap_or_else(|| "ws".to_string());
    s.ws_heartbeat_secs = bria.ws_heartbeat_secs.unwrap_or(30);
    s.sse_keepalive_secs = bria.sse_keepalive_secs.unwrap_or(5);
    s.broadcast_capacity = bria.broadcast_capacity.unwrap_or(1024);

    // Table config
    let mut table_columns = TableSinkColumnsConfig::default();
    if let Some(ref db_table) = bria.db_table {
        if let Some(ref cols) = db_table.columns {
            table_columns = cols.clone();
        }
    }
    if let Some(ref tbl) = bria.table {
        let mut tcs = TableSinkConfig {
            name: tbl.name.clone().unwrap_or_default(),
            columns: table_columns,
        };
        if let Some(ref cols) = tbl.columns {
            tcs.columns = cols.clone();
        }
        s.table = Some(tcs);
    } else if bria.table_name.is_some() {
        // Inline table_name with db_table.columns
        s.table = Some(TableSinkConfig {
            name: bria.table_name.clone().unwrap_or_default(),
            columns: table_columns,
        });
    }

    Ok(s)
}

fn resolve_pipeline(bria: &BriaPipelineConfig) -> Result<PipelineConfig> {
    let p = PipelineConfig {
        id: bria.id.clone(),
        source: bria.source.clone(),
        sources: bria.sources.clone().unwrap_or_default(),
        merge: bria.merge.clone(),
        concurrency: bria.concurrency.unwrap_or(8),
        queue_capacity: bria.queue_capacity.unwrap_or(256),
        sinks: bria.sinks.clone().unwrap_or_default(),
        failure: bria.failure.clone().unwrap_or_default(),
        labels: bria.labels.clone().unwrap_or_default(),
        steps: bria.steps.clone(),
        resolved_sources: OnceLock::new(),
    };
    Ok(p)
}

// =============================================================================
// Public configuration structs (existing shape, preserved)
// =============================================================================

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
    10 * 1024 * 1024
}
fn default_cancel_signal_ttl_secs() -> u64 {
    3600
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LogConfig {
    #[serde(default = "default_log_level")]
    pub level: String,
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

// =============================================================================
// Server configuration
// =============================================================================

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

// =============================================================================
// Source configuration
// =============================================================================

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SourceConfig {
    pub id: String,
    #[serde(default = "default_true")]
    pub enabled: bool,
    #[serde(rename = "type")]
    pub r#type: SourceType,
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
    #[serde(default = "default_max_body_bytes_val")]
    pub max_body_bytes: usize,
    #[serde(default)]
    pub hmac_secret: String,
    #[serde(default = "default_hmac_header")]
    pub hmac_header: String,
    #[serde(default = "default_ack_status")]
    pub ack_status: u16,
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
    #[serde(default)]
    pub schedule: String,
    #[serde(default = "default_tz")]
    pub tz: String,
    #[serde(default)]
    pub labels: HashMap<String, String>,
    #[serde(default)]
    pub payload: serde_json::Value,
    #[serde(default)]
    pub table: Option<TableSourceConfig>,
}

impl SourceConfig {
    pub(crate) fn default_with_id(id: &str) -> Self {
        Self {
            id: id.to_string(),
            enabled: true,
            r#type: SourceType::File,
            path: PathBuf::new(),
            poll_interval_secs: default_poll_interval(),
            track_cursor: true,
            authoritative: false,
            id_field: String::new(),
            max_body_bytes: default_max_body_bytes_val(),
            hmac_secret: String::new(),
            hmac_header: default_hmac_header(),
            ack_status: default_ack_status(),
            url: String::new(),
            username: String::new(),
            password: String::new(),
            exchange: String::new(),
            submit_routing_key: default_submit_routing_key(),
            cancel_routing_key: default_cancel_routing_key(),
            reconnect_secs: default_reconnect_secs(),
            qos_prefetch: default_qos_prefetch(),
            consumer_tag: default_consumer_tag(),
            schedule: String::new(),
            tz: default_tz(),
            labels: HashMap::new(),
            payload: serde_json::Value::Null,
            table: None,
        }
    }

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

impl Default for TableSourceConfig {
    fn default() -> Self {
        Self {
            name: String::new(),
            columns: TableSourceColumnsConfig::default(),
        }
    }
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
    1_048_576
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

// =============================================================================
// Task configuration
// =============================================================================

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

impl TaskConfig {
    pub(crate) fn default_with_id(id: &str) -> Self {
        Self {
            id: id.to_string(),
            driver: default_driver(),
            cmd: String::new(),
            args: Vec::new(),
            inherit_env: false,
            working_dir: None,
            success_exit_codes: default_success_exit_codes(),
            timeout_secs: 0,
            timeout_action: default_timeout_action(),
            kill_grace_secs: default_kill_grace_secs(),
            env: HashMap::new(),
            stdin: StdinConfig::default(),
            stdout: StreamConfig::default_capture(),
            stderr: StreamConfig::default_stderr(),
            retry: TaskRetryConfig::default(),
            labels: HashMap::new(),
            docker: None,
            wasm: None,
        }
    }

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
                "Task '{}' driver is 'docker' but [bria.tasks.docker] section is missing",
                self.id
            )));
        }

        if self.driver == "wasm" && self.wasm.is_none() {
            return Err(Error::validation(format!(
                "Task '{}' driver is 'wasm' but [bria.tasks.wasm] section is missing",
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

impl StreamConfig {
    pub(crate) fn default_capture() -> Self {
        Self {
            mode: default_capture_mode(),
            max_bytes: 10 * 1024 * 1024,
        }
    }

    pub(crate) fn default_stderr() -> Self {
        Self {
            mode: default_capture_mode(),
            max_bytes: 1024 * 1024,
        }
    }
}

impl Default for StreamConfig {
    fn default() -> Self {
        Self::default_capture()
    }
}

fn default_capture_mode() -> String {
    "capture".to_string()
}

fn default_stderr_stream() -> StreamConfig {
    StreamConfig::default_stderr()
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

// =============================================================================
// Sink configuration
// =============================================================================

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SinkConfig {
    pub id: String,
    #[serde(default = "default_true")]
    pub enabled: bool,
    #[serde(rename = "type")]
    pub r#type: SinkType,
    #[serde(default)]
    pub path: String,
    #[serde(default)]
    pub template: Option<String>,
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
    #[serde(default)]
    pub table: Option<TableSinkConfig>,
}

impl SinkConfig {
    pub(crate) fn default_with_id(id: &str) -> Self {
        Self {
            id: id.to_string(),
            enabled: true,
            r#type: SinkType::File,
            path: String::new(),
            template: None,
            secret: String::new(),
            signature_header: default_signature_header(),
            content_type: default_content_type(),
            max_retries: default_max_retries(),
            retry_base_ms: default_retry_base_ms_sink(),
            timeout_secs: default_timeout_secs_sink(),
            headers: HashMap::new(),
            url: String::new(),
            username: String::new(),
            password: String::new(),
            exchange: String::new(),
            success_routing_key: String::new(),
            failure_routing_key: String::new(),
            reconnect_secs: default_reconnect_secs(),
            sse: default_sse(),
            websocket: default_ws(),
            ws_heartbeat_secs: default_ws_heartbeat(),
            sse_keepalive_secs: default_sse_keepalive(),
            broadcast_capacity: default_broadcast_capacity(),
            table: None,
        }
    }

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
            SinkType::Stream => {}
        }
        Ok(())
    }
}

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

// =============================================================================
// Pipeline configuration
// =============================================================================

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PipelineConfig {
    pub id: String,
    #[serde(default)]
    pub source: Option<String>,
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

    #[serde(skip)]
    pub resolved_sources: OnceLock<Vec<String>>,
}

impl PipelineConfig {
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

        if self.source.is_none() && self.sources.is_empty() {
            errors.push(format!("Pipeline '{}' has no sources configured", self.id));
        }

        if self.sources.len() > 1 && self.merge.is_none() {
            errors.push(format!(
                "Pipeline '{}' has multiple sources but no [bria.pipelines.merge] section",
                self.id
            ));
        }

        if let Some(ref merge) = self.merge {
            match merge.strategy.as_str() {
                "any" | "all" => {}
                s => errors.push(format!(
                    "Pipeline '{}' merge.strategy '{}' is invalid (must be 'any' or 'all')",
                    self.id, s
                )),
            }

            if merge.correlation_key.is_some() && merge.correlation_expr.is_some() {
                errors.push(format!(
                    "Pipeline '{}' merge config: correlation_key and correlation_expr are mutually exclusive",
                    self.id
                ));
            }

            if merge.correlation_key.is_none() && merge.correlation_expr.is_none() {
                errors.push(format!(
                    "Pipeline '{}' merge config: must specify either correlation_key or correlation_expr",
                    self.id
                ));
            }
        }

        let step_ids: HashSet<&str> = self.steps.iter().map(|s| s.id.as_str()).collect();

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
                    if let Some(ref skip_to) = step.skip_to
                        && !step_ids.contains(skip_to.as_str())
                    {
                        errors.push(format!(
                            "Pipeline '{}' step '{}' skip_to '{}' references unknown step",
                            self.id, step.id, skip_to
                        ));
                    }
                    match step.action.as_deref().unwrap_or("fail") {
                        "fail" | "skip_to" | "emit" => {}
                        a => errors.push(format!(
                            "Pipeline '{}' step '{}' action '{}' is invalid (must be 'fail', 'skip_to', or 'emit')",
                            self.id, step.id, a
                        )),
                    }
                }
                StepType::Map => {
                    if step.set.is_empty() {
                        errors.push(format!(
                            "Pipeline '{}' step '{}' type 'map' requires at least one [[bria.pipelines.steps.set]] entry",
                            self.id, step.id
                        ));
                    }
                }
            }

            for dep in &step.depends_on {
                if !step_ids.contains(dep.as_str()) {
                    errors.push(format!(
                        "Pipeline '{}' step '{}' depends_on '{}' references unknown step",
                        self.id, step.id, dep
                    ));
                }
            }

            if step.retry.jitter < 0.0 || step.retry.jitter > 1.0 {
                errors.push(format!(
                    "Pipeline '{}' step '{}' retry.jitter must be between 0.0 and 1.0, got {}",
                    self.id, step.id, step.retry.jitter
                ));
            }

            if step.failure.action == FailureAction::DeadLetter && step.failure.sink.is_none() {
                errors.push(format!(
                    "Pipeline '{}' step '{}' failure action is dead_letter but no sink specified",
                    self.id, step.id
                ));
            }

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

        if let Err(e) = self.validate_dag(&step_ids) {
            errors.push(e);
        }

        if !errors.is_empty() {
            return Err(Error::Validation(errors.join("\n")));
        }

        Ok(())
    }

    fn validate_dag(&self, step_ids: &HashSet<&str>) -> std::result::Result<(), String> {
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
    #[serde(default)]
    pub expr: Option<String>,
    #[serde(default)]
    pub action: Option<String>,
    #[serde(default)]
    pub skip_to: Option<String>,
    #[serde(default)]
    pub reason: Option<String>,
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

// =============================================================================
// Environment variable substitution
// =============================================================================

/// Substitute `${VAR_NAME}` and `${VAR_NAME:-default}` patterns with OS env
/// values in TOML content. TOML comments are left unchanged.
pub fn substitute_env(input: &str) -> Result<String> {
    // Two patterns:
    // 1. ${VAR_NAME:-default}  — use default if var unset
    // 2. ${VAR_NAME}           — error if var unset
    static ENV_VAR_RE: OnceLock<regex::Regex> = OnceLock::new();
    static ENV_VAR_DEFAULT_RE: OnceLock<regex::Regex> = OnceLock::new();

    let re_default = ENV_VAR_DEFAULT_RE
        .get_or_init(|| regex::Regex::new(r"\$\{([A-Za-z_][A-Za-z0-9_]*):-([^}]*)\}").unwrap());
    let re =
        ENV_VAR_RE.get_or_init(|| regex::Regex::new(r"\$\{([A-Za-z_][A-Za-z0-9_]*)\}").unwrap());

    let mut errors = Vec::new();
    let mut result = String::with_capacity(input.len());
    let mut basic_string = false;
    let mut literal_string = false;
    let mut escaped = false;

    for line in input.split_inclusive('\n') {
        let comment_start =
            toml_comment_start(line, &mut basic_string, &mut literal_string, &mut escaped);
        let code = &line[..comment_start];
        let with_defaults = re_default.replace_all(code, |caps: &regex::Captures| {
            let var_name = caps.get(1).unwrap().as_str();
            let default = caps.get(2).unwrap().as_str();
            std::env::var(var_name).unwrap_or_else(|_| default.to_string())
        });
        result.push_str(&re.replace_all(&with_defaults, |caps: &regex::Captures| {
            let var_name = caps.get(1).unwrap().as_str();
            std::env::var(var_name).unwrap_or_else(|_| {
                errors.push(format!(
                    "Environment variable '{}' is not set but referenced in config",
                    var_name
                ));
                String::new()
            })
        }));
        result.push_str(&line[comment_start..]);
    }

    if !errors.is_empty() {
        return Err(Error::EnvVar(errors.join("\n")));
    }

    Ok(result)
}

/// Finds a line comment while preserving quote state for multiline strings.
fn toml_comment_start(
    line: &str,
    basic_string: &mut bool,
    literal_string: &mut bool,
    escaped: &mut bool,
) -> usize {
    for (index, character) in line.char_indices() {
        if *basic_string {
            if *escaped {
                *escaped = false;
            } else if character == '\\' {
                *escaped = true;
            } else if character == '"' {
                *basic_string = false;
            }
        } else if *literal_string {
            if character == '\'' {
                *literal_string = false;
            }
        } else if character == '#' {
            return index;
        } else if character == '"' {
            *basic_string = true;
        } else if character == '\'' {
            *literal_string = true;
        }
    }
    line.len()
}
