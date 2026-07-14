// Integration tests covering sources::create_job, state::MemoryStore,
// state::SqliteStateStore, state::create_store, util helpers, and file-source
// Orchestrator integration (JSON array / CSV — JSONL is tested elsewhere).
//
// Tests for substitute_env, Config validation, Evaluator, TemplateEngine,
// Context, PipelineResult, and Error are in core_config_expr_template.rs.
// Pipeline execution scenarios (run_pipeline_once, skip_to, emit, parallel,
// map, stdin, timeout) are in config_and_pipeline.rs.

use bria::config::{self, StateConfig};
use bria::context::Job;
use bria::sources::create_job;
#[cfg(feature = "sqlite")]
use bria::state::SqliteStateStore;
use bria::state::{self, MemoryStore, StateStore};
use bria::util;
use std::collections::HashMap;

// =============================================================================
// sources::create_job
// =============================================================================

fn file_source_config(id: &str, path: &str) -> config::SourceConfig {
    config::SourceConfig {
        id: id.to_string(),
        enabled: true,
        r#type: config::SourceType::File,
        path: std::path::PathBuf::from(path),
        poll_interval_secs: 2,
        track_cursor: true,
        authoritative: false,
        id_field: String::new(),
        max_body_bytes: 1_048_576,
        hmac_secret: String::new(),
        hmac_header: String::new(),
        ack_status: 202,
        url: String::new(),
        username: String::new(),
        password: String::new(),
        exchange: String::new(),
        submit_routing_key: String::new(),
        cancel_routing_key: String::new(),
        reconnect_secs: 5,
        qos_prefetch: 100,
        consumer_tag: String::new(),
        schedule: String::new(),
        tz: String::new(),
        labels: HashMap::new(),
        payload: serde_json::Value::Null,
        table: None,
    }
}

#[test]
fn create_job_without_id_field_generates_ulid() {
    let source = file_source_config("src", "data.jsonl");
    let value = serde_json::json!({"name": "test"});
    let job = create_job(&source, &value);
    assert_eq!(job.source, "src");
    assert_eq!(job.payload, value);
    assert!(job.correlation_key.is_none());
    assert_eq!(job.id.len(), 26); // ULID length
}

#[test]
fn create_job_with_id_field_extracts_from_payload() {
    let mut source = file_source_config("src", "data.jsonl");
    source.id_field = "job_id".to_string();
    let value = serde_json::json!({"job_id": "my-id-123", "name": "test"});
    let job = create_job(&source, &value);
    assert_eq!(job.id, "my-id-123");
    assert_eq!(job.source, "src");
}

#[test]
fn create_job_with_id_field_missing_falls_back_to_ulid() {
    let mut source = file_source_config("src", "data.jsonl");
    source.id_field = "missing_key".to_string();
    let value = serde_json::json!({"name": "test"});
    let job = create_job(&source, &value);
    assert_eq!(job.id.len(), 26);
}

#[test]
fn create_job_with_id_field_numeric_value_stringifies_correctly() {
    // as_str() returns None for Number values, triggering ULID fallback
    let mut source = file_source_config("src", "data.jsonl");
    source.id_field = "num_id".to_string();
    let value = serde_json::json!({"num_id": 42});
    let job = create_job(&source, &value);
    assert_eq!(job.id.len(), 26);
}

#[test]
fn create_job_includes_source_labels() {
    let mut source = file_source_config("src", "data.jsonl");
    source.labels = {
        let mut m = HashMap::new();
        m.insert("team".to_string(), "platform".to_string());
        m.insert("region".to_string(), "us-east".to_string());
        m
    };
    let value = serde_json::json!({"name": "test"});
    let job = create_job(&source, &value);
    assert_eq!(job.labels.get("team").map(|s| s.as_str()), Some("platform"));
    assert_eq!(
        job.labels.get("region").map(|s| s.as_str()),
        Some("us-east")
    );
}

// =============================================================================
// state::MemoryStore
// =============================================================================

fn make_test_job(id: &str, source: &str) -> Job {
    Job {
        id: id.to_string(),
        source: source.to_string(),
        payload: serde_json::json!({"key": id}),
        correlation_key: Some(format!("corr-{}", id)),
        labels: {
            let mut m = HashMap::new();
            m.insert("env".to_string(), "test".to_string());
            m
        },
    }
}

#[tokio::test]
async fn memory_store_records_queued_state() {
    let store = MemoryStore::new();
    let job = make_test_job("j1", "file-src");
    store.record_queued(&job, "p1").await.unwrap();

    let recovered = store.recover_incomplete().await.unwrap();
    assert_eq!(recovered.len(), 1);
    assert_eq!(recovered[0].job_id, "j1");
    assert_eq!(recovered[0].pipeline_id, "p1");
    assert_eq!(recovered[0].state, "queued");
    assert_eq!(recovered[0].source, "file-src");
    assert_eq!(recovered[0].payload, serde_json::json!({"key": "j1"}));
    assert_eq!(recovered[0].correlation_key.as_deref(), Some("corr-j1"));
    assert_eq!(
        recovered[0].labels.get("env").map(|s| s.as_str()),
        Some("test")
    );
}

#[tokio::test]
async fn memory_store_records_running_state() {
    let store = MemoryStore::new();
    let job = make_test_job("j2", "src");
    store.record_queued(&job, "p2").await.unwrap();
    store.record_running(&job, "p2").await.unwrap();

    let recovered = store.recover_incomplete().await.unwrap();
    assert_eq!(recovered.len(), 1);
    assert_eq!(recovered[0].state, "running");
    assert_eq!(recovered[0].job_id, "j2");
}

#[tokio::test]
async fn memory_store_record_running_unknown_key_is_noop() {
    let store = MemoryStore::new();
    let job = make_test_job("unknown", "src");
    // Should not panic or error — simply nothing to update.
    store.record_running(&job, "p-unknown").await.unwrap();
    assert!(store.recover_incomplete().await.unwrap().is_empty());
}

#[tokio::test]
async fn memory_store_records_completed_state() {
    let store = MemoryStore::new();
    let job = make_test_job("j3", "src");
    store.record_queued(&job, "p3").await.unwrap();
    store.record_running(&job, "p3").await.unwrap();
    store.record_completed("j3", "p3", "success").await.unwrap();

    // Completed records are excluded from recovery.
    assert!(store.recover_incomplete().await.unwrap().is_empty());
}

#[tokio::test]
async fn memory_store_record_completed_unknown_key_is_noop() {
    let store = MemoryStore::new();
    store
        .record_completed("ghost", "ghost-pipeline", "success")
        .await
        .unwrap();
}

#[tokio::test]
async fn memory_store_recover_incomplete_filters_out_completed() {
    let store = MemoryStore::new();

    let job_a = make_test_job("j-a", "src");
    let job_b = make_test_job("j-b", "src");
    let job_c = make_test_job("j-c", "src");

    store.record_queued(&job_a, "p").await.unwrap();
    store.record_queued(&job_b, "p").await.unwrap();
    store.record_queued(&job_c, "p").await.unwrap();

    store.record_running(&job_b, "p").await.unwrap();
    store.record_completed("j-c", "p", "success").await.unwrap();

    let recovered = store.recover_incomplete().await.unwrap();
    let ids: Vec<&str> = recovered.iter().map(|r| r.job_id.as_str()).collect();
    assert!(ids.contains(&"j-a")); // queued → included
    assert!(ids.contains(&"j-b")); // running → included
    assert!(!ids.contains(&"j-c")); // completed → excluded
}

#[tokio::test]
async fn memory_store_handles_empty_recovery() {
    let store = MemoryStore::new();
    assert!(store.recover_incomplete().await.unwrap().is_empty());
}

// =============================================================================
// state::SqliteStateStore
// =============================================================================

#[cfg(feature = "sqlite")]
fn unique_sqlite_path(label: &str) -> String {
    let path = std::env::temp_dir().join(format!(
        "bria-st-test-{}-{}-{}.db",
        std::process::id(),
        label,
        ulid::Ulid::r#gen()
    ));
    let s = path.to_string_lossy().to_string();
    let _ = std::fs::remove_file(&path);
    s
}

#[cfg(feature = "sqlite")]
#[tokio::test]
async fn sqlite_state_store_creates_table_on_fresh_db() {
    let path = unique_sqlite_path("fresh");
    let store = SqliteStateStore::new(&path).await.unwrap();
    let recovered = store.recover_incomplete().await.unwrap();
    assert!(recovered.is_empty());
    let _ = std::fs::remove_file(&path);
}

#[cfg(feature = "sqlite")]
#[tokio::test]
async fn sqlite_state_store_new_twice_is_idempotent() {
    let path = unique_sqlite_path("idem");
    let store1 = SqliteStateStore::new(&path).await.unwrap();
    drop(store1);
    let store2 = SqliteStateStore::new(&path).await.unwrap();
    assert!(store2.recover_incomplete().await.unwrap().is_empty());
    let _ = std::fs::remove_file(&path);
}

#[cfg(feature = "sqlite")]
#[tokio::test]
async fn sqlite_state_store_record_queued_inserts_row() {
    let path = unique_sqlite_path("insert");
    let store = SqliteStateStore::new(&path).await.unwrap();
    let job = make_test_job("sql-1", "src-a");

    store.record_queued(&job, "p-sql").await.unwrap();

    let recovered = store.recover_incomplete().await.unwrap();
    assert_eq!(recovered.len(), 1);
    assert_eq!(recovered[0].job_id, "sql-1");
    assert_eq!(recovered[0].state, "queued");
    let _ = std::fs::remove_file(&path);
}

#[cfg(feature = "sqlite")]
#[tokio::test]
async fn sqlite_state_store_record_queued_twice_upserts() {
    let path = unique_sqlite_path("upsert");
    let store = SqliteStateStore::new(&path).await.unwrap();
    let job = make_test_job("sql-2", "src-b");

    store.record_queued(&job, "p-sql").await.unwrap();
    // Second call with same (job_id, pipeline_id) must UPDATE, not INSERT duplicate.
    store.record_queued(&job, "p-sql").await.unwrap();

    let recovered = store.recover_incomplete().await.unwrap();
    assert_eq!(recovered.len(), 1);
    let _ = std::fs::remove_file(&path);
}

#[cfg(feature = "sqlite")]
#[tokio::test]
async fn sqlite_state_store_records_running() {
    let path = unique_sqlite_path("running");
    let store = SqliteStateStore::new(&path).await.unwrap();
    let job = make_test_job("sql-3", "src");

    store.record_queued(&job, "p-sql").await.unwrap();
    store.record_running(&job, "p-sql").await.unwrap();

    let recovered = store.recover_incomplete().await.unwrap();
    assert_eq!(recovered[0].state, "running");
    let _ = std::fs::remove_file(&path);
}

#[cfg(feature = "sqlite")]
#[tokio::test]
async fn sqlite_state_store_records_completed_and_excludes_from_recovery() {
    let path = unique_sqlite_path("completed");
    let store = SqliteStateStore::new(&path).await.unwrap();
    let job = make_test_job("sql-4", "src");

    store.record_queued(&job, "p-sql").await.unwrap();
    store
        .record_completed("sql-4", "p-sql", "failure")
        .await
        .unwrap();

    assert!(store.recover_incomplete().await.unwrap().is_empty());
    let _ = std::fs::remove_file(&path);
}

#[cfg(feature = "sqlite")]
#[tokio::test]
async fn sqlite_state_store_recovery_deserializes_payload_and_labels() {
    let path = unique_sqlite_path("deser");
    let store = SqliteStateStore::new(&path).await.unwrap();
    let job = make_test_job("sql-5", "src-c");

    store.record_queued(&job, "p-deser").await.unwrap();

    let recovered = store.recover_incomplete().await.unwrap();
    assert_eq!(recovered[0].payload, serde_json::json!({"key": "sql-5"}));
    assert_eq!(
        recovered[0].labels.get("env").map(|s| s.as_str()),
        Some("test")
    );
    let _ = std::fs::remove_file(&path);
}

#[cfg(feature = "sqlite")]
#[tokio::test]
async fn sqlite_state_store_recovery_handles_null_correlation_key() {
    let path = unique_sqlite_path("null-ck");
    let store = SqliteStateStore::new(&path).await.unwrap();
    let job = Job {
        id: "sql-nk".to_string(),
        source: "src".to_string(),
        payload: serde_json::json!({"a": 1}),
        correlation_key: None,
        labels: HashMap::new(),
    };

    store.record_queued(&job, "p-nk").await.unwrap();
    let recovered = store.recover_incomplete().await.unwrap();
    assert_eq!(recovered[0].correlation_key, None);
    let _ = std::fs::remove_file(&path);
}

#[cfg(feature = "sqlite")]
#[tokio::test]
async fn sqlite_state_store_recovery_handles_empty_labels() {
    let path = unique_sqlite_path("empty-labels");
    let store = SqliteStateStore::new(&path).await.unwrap();
    let job = Job {
        id: "sql-el".to_string(),
        source: "src".to_string(),
        payload: serde_json::json!({"a": 1}),
        correlation_key: None,
        labels: HashMap::new(),
    };

    store.record_queued(&job, "p-el").await.unwrap();
    let recovered = store.recover_incomplete().await.unwrap();
    assert!(recovered[0].labels.is_empty());
    let _ = std::fs::remove_file(&path);
}

#[cfg(feature = "sqlite")]
#[tokio::test]
async fn sqlite_state_store_multiple_jobs_across_pipelines() {
    let path = unique_sqlite_path("multi");
    let store = SqliteStateStore::new(&path).await.unwrap();

    let j1 = make_test_job("mj-1", "src-a");
    let j2 = make_test_job("mj-2", "src-b");

    store.record_queued(&j1, "pa").await.unwrap();
    store.record_queued(&j2, "pb").await.unwrap();
    store.record_running(&j1, "pa").await.unwrap();

    let recovered = store.recover_incomplete().await.unwrap();
    assert_eq!(recovered.len(), 2);

    let states: Vec<&str> = recovered.iter().map(|r| r.state.as_str()).collect();
    assert!(states.contains(&"queued"));
    assert!(states.contains(&"running"));
    let _ = std::fs::remove_file(&path);
}

// =============================================================================
// state::create_store
// =============================================================================

#[tokio::test]
async fn create_store_memory_backend_returns_memory_store() {
    let cfg = StateConfig {
        backend: "memory".to_string(),
        sqlite_path: String::new(),
        pg_url: String::new(),
    };
    let store = state::create_store(&cfg).await.unwrap();
    assert!(store.recover_incomplete().await.unwrap().is_empty());
}

#[cfg(feature = "sqlite")]
#[tokio::test]
async fn create_store_sqlite_backend_returns_sqlite_store() {
    let path = unique_sqlite_path("factory");
    let cfg = StateConfig {
        backend: "sqlite".to_string(),
        sqlite_path: path.clone(),
        pg_url: String::new(),
    };
    let store = state::create_store(&cfg).await.unwrap();
    assert!(store.recover_incomplete().await.unwrap().is_empty());
    let _ = std::fs::remove_file(&path);
}

#[tokio::test]
async fn create_store_unknown_backend_errors_with_message() {
    let cfg = StateConfig {
        backend: "redis".to_string(),
        sqlite_path: String::new(),
        pg_url: String::new(),
    };
    let result = state::create_store(&cfg).await;
    assert!(result.is_err());
    let err = result.err().unwrap();
    assert!(err.to_string().contains("Unknown state backend"));
}

#[test]
fn validate_identifier_rejects_invalid_sql_identifiers() {
    assert!(util::validate_identifier("table", "").is_err());
    assert!(util::validate_identifier("table", "1bad").is_err());
    assert!(util::validate_identifier("table", "bad-name").is_err());
    assert!(util::validate_identifier("table", "snake_case_123").is_ok());
    assert!(util::validate_identifier("table", "_leading").is_ok());
}

#[test]
fn quote_ident_wraps_valid_identifiers() {
    assert_eq!(util::quote_ident("table", "orders").unwrap(), "\"orders\"");
}

#[cfg(feature = "amqp")]
#[test]
fn amqp_url_with_credentials_applies_configured_credentials() {
    assert_eq!(
        util::amqp_url_with_credentials("amqp://host/%2F", "", "").unwrap(),
        "amqp://host/%2F"
    );
    assert_eq!(
        util::amqp_url_with_credentials("amqp://host/%2F", "bria", "").unwrap(),
        "amqp://bria@host/%2F"
    );
    assert_eq!(
        util::amqp_url_with_credentials("amqp://host/%2F", "bria", "secret").unwrap(),
        "amqp://bria:secret@host/%2F"
    );
    assert!(util::amqp_url_with_credentials("not a url", "bria", "secret").is_err());
}

#[test]
fn cancel_signal_ttl_clamps_zero_and_prunes_expired_entries() {
    let config = bria::Config::from_str_with_env(
        r#"
[bria]
[bria.global]
cancel_signal_ttl_secs = 0

[[bria.sources]]
id = "manual"
type = "file"
path = "unused.jsonl"
"#,
    )
    .unwrap();
    assert_eq!(
        util::cancel_signal_ttl(&config),
        std::time::Duration::from_secs(1)
    );

    let signals = dashmap::DashMap::new();
    signals.insert(
        "expired".to_string(),
        std::time::Instant::now() - std::time::Duration::from_secs(5),
    );
    signals.insert("fresh".to_string(), std::time::Instant::now());

    util::prune_expired_cancel_signals(&signals, std::time::Duration::from_secs(1));

    assert!(!signals.contains_key("expired"));
    assert!(signals.contains_key("fresh"));
}

// =============================================================================
// File source integration via Orchestrator (deterministic, no Docker/Postgres)
// =============================================================================

#[tokio::test]
async fn file_source_json_array_emits_multiple_jobs_and_sink_records_all() {
    let base = std::env::temp_dir().join(format!(
        "bria-src-json-{}-{}",
        std::process::id(),
        ulid::Ulid::r#gen()
    ));
    let output = base.join("results.jsonl");
    let input = base.join("batch.json");
    std::fs::create_dir_all(&base).unwrap();
    std::fs::write(&input, "[{\"id\":\"a\",\"v\":1},{\"id\":\"b\",\"v\":2}]").unwrap();

    let config_str = format!(
        r#"
[bria]
[[bria.sources]]
id = "src"
type = "file"
path = "{}"
poll_interval_secs = 1
id_field = "id"

[[bria.tasks]]
id = "echo-v"
driver = "local"
cmd = "sh"
args = ["-c", "printf '%s' \"$1\"", "sh", "{{{{job.payload.v}}}}"]

[bria.tasks.stdout]
mode = "capture"
max_bytes = 16

[[bria.sinks]]
id = "sink"
type = "file"
path = "{}"

[[bria.pipelines]]
id = "p"
source = "src"
sinks = ["sink"]
concurrency = 2

[[bria.pipelines.steps]]
id = "run"
type = "process"
task = "echo-v"
"#,
        input.display(),
        output.display()
    );

    let config = bria::Config::from_str_with_env(&config_str).unwrap();
    config.validate().unwrap();
    let orchestrator = bria::Orchestrator::new(config).await.unwrap();
    let handle = tokio::spawn(async move { orchestrator.run().await });
    tokio::time::sleep(std::time::Duration::from_millis(3000)).await;
    handle.abort();

    let out = std::fs::read_to_string(&output).unwrap_or_default();
    let lines: Vec<&str> = out.lines().filter(|l| !l.is_empty()).collect();
    assert!(
        lines.len() >= 2,
        "expected at least 2 sink output lines, got: {out}"
    );
    assert!(out.contains("1"), "expected value 1 in output: {out}");
    assert!(out.contains("2"), "expected value 2 in output: {out}");
    let _ = std::fs::remove_dir_all(&base);
}

#[tokio::test]
async fn file_source_csv_emits_header_based_jobs() {
    let base = std::env::temp_dir().join(format!(
        "bria-src-csv-{}-{}",
        std::process::id(),
        ulid::Ulid::r#gen()
    ));
    let output = base.join("out.jsonl");
    let input = base.join("data.csv");
    std::fs::create_dir_all(&base).unwrap();
    std::fs::write(&input, "name,score\nAlice,100\nBob,85\n").unwrap();

    let config_str = format!(
        r#"
[bria]
[[bria.sources]]
id = "src"
type = "file"
path = "{}"
poll_interval_secs = 1

[[bria.tasks]]
id = "echo"
driver = "local"
cmd = "sh"
args = ["-c", "printf '%s' \"$1\"", "sh", "{{{{job.payload.name}}}}-{{{{job.payload.score}}}}"]

[bria.tasks.stdout]
mode = "capture"
max_bytes = 128

[[bria.sinks]]
id = "sink"
type = "file"
path = "{}"

[[bria.pipelines]]
id = "p"
source = "src"
sinks = ["sink"]
concurrency = 2

[[bria.pipelines.steps]]
id = "run"
type = "process"
task = "echo"
"#,
        input.display(),
        output.display()
    );

    let config = bria::Config::from_str_with_env(&config_str).unwrap();
    config.validate().unwrap();
    let orchestrator = bria::Orchestrator::new(config).await.unwrap();
    let handle = tokio::spawn(async move { orchestrator.run().await });
    tokio::time::sleep(std::time::Duration::from_millis(3000)).await;
    handle.abort();

    let out = std::fs::read_to_string(&output).unwrap_or_default();
    assert!(
        out.contains("Alice-100"),
        "CSV header-based job missing Alice: {out}"
    );
    assert!(out.contains("Bob-85"), "CSV second row missing Bob: {out}");
    let _ = std::fs::remove_dir_all(&base);
}

#[tokio::test]
async fn file_source_ignores_unknown_file_extension() {
    let base = std::env::temp_dir().join(format!(
        "bria-src-txt-{}-{}",
        std::process::id(),
        ulid::Ulid::r#gen()
    ));
    let output = base.join("out.txt.jsonl");
    let input_dir = base.join("jobs");
    std::fs::create_dir_all(&input_dir).unwrap();
    // Write a .txt file — it should be silently ignored.
    std::fs::write(input_dir.join("readme.txt"), "not a job\n").unwrap();

    let config_str = format!(
        r#"
[bria]
[[bria.sources]]
id = "src"
type = "file"
path = "{}"
poll_interval_secs = 1

[[bria.tasks]]
id = "noop"
driver = "local"
cmd = "true"

[[bria.sinks]]
id = "sink"
type = "file"
path = "{}"

[[bria.pipelines]]
id = "p"
source = "src"
sinks = ["sink"]

[[bria.pipelines.steps]]
id = "run"
type = "process"
task = "noop"
"#,
        input_dir.display(),
        output.display()
    );

    let config = bria::Config::from_str_with_env(&config_str).unwrap();
    config.validate().unwrap();
    let orchestrator = bria::Orchestrator::new(config).await.unwrap();
    let handle = tokio::spawn(async move { orchestrator.run().await });
    tokio::time::sleep(std::time::Duration::from_millis(3000)).await;
    handle.abort();

    let out = std::fs::read_to_string(&output).unwrap_or_default();
    assert!(
        out.trim().is_empty(),
        "unknown extension file should produce no jobs, got: {out}"
    );
    let _ = std::fs::remove_dir_all(&base);
}

#[tokio::test]
async fn file_source_authoritative_mode_cancels_removed_items() {
    let base = std::env::temp_dir().join(format!(
        "bria-src-auth-{}-{}",
        std::process::id(),
        ulid::Ulid::r#gen()
    ));
    let output = base.join("out.jsonl");
    let input = base.join("jobs.jsonl");
    std::fs::create_dir_all(&base).unwrap();

    // First write with two items.
    std::fs::write(
        &input,
        "{\"id\":\"keep\",\"msg\":\"stay\"}\n{\"id\":\"rm\",\"msg\":\"go\"}\n",
    )
    .unwrap();

    let config_str = format!(
        r#"
[bria]
[[bria.sources]]
id = "src"
type = "file"
path = "{}"
poll_interval_secs = 1
track_cursor = true
authoritative = true
id_field = "id"

[[bria.tasks]]
id = "emit"
driver = "local"
cmd = "sh"
args = ["-c", "printf '%s' \"$1\"", "sh", "{{{{job.payload.msg}}}}"]

[bria.tasks.stdout]
mode = "capture"
max_bytes = 128

[[bria.sinks]]
id = "sink"
type = "file"
path = "{}"

[[bria.pipelines]]
id = "p"
source = "src"
sinks = ["sink"]
concurrency = 2

[[bria.pipelines.steps]]
id = "run"
type = "process"
task = "emit"
"#,
        input.display(),
        output.display()
    );

    let config = bria::Config::from_str_with_env(&config_str).unwrap();
    config.validate().unwrap();
    let orchestrator = bria::Orchestrator::new(config).await.unwrap();
    let handle = tokio::spawn(async move { orchestrator.run().await });

    // Let the first poll process both items.
    tokio::time::sleep(std::time::Duration::from_millis(2500)).await;

    // Rewrite the file with only "keep" — "rm" is removed.
    std::fs::write(&input, "{\"id\":\"keep\",\"msg\":\"stay\"}\n").unwrap();

    // Let the next poll detect the removal and emit a cancellation job.
    tokio::time::sleep(std::time::Duration::from_millis(2500)).await;
    handle.abort();

    let out = std::fs::read_to_string(&output).unwrap_or_default();
    assert!(out.contains("stay"), "should contain keep job: {out}");
    // The removed job should have triggered a cancellation (which skips sink),
    // but the first poll should have processed both.
    assert!(out.contains("go"), "first poll should process rm=go: {out}");
    let _ = std::fs::remove_dir_all(&base);
}
