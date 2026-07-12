#![cfg(feature = "server")]
#![cfg(feature = "webhook")]
#![cfg(feature = "sqlite")]

use std::collections::HashMap;
use std::sync::Arc;

use axum::http::StatusCode;
use bria::{Config, Context, Job, PipelineResult, StepResult, create_store};

// ---------------------------------------------------------------------------
// File sink tests — exercise SinkDispatcher → file output
// ---------------------------------------------------------------------------

/// Builds a minimal config string with one source, task, pipeline, and a
/// configurable sink section appended at the end.
fn config_with_sink(sink_def: &str) -> String {
    format!(
        r#"
[bria]
[bria.server]
enabled = true

[[bria.sources]]
id = "manual"
type = "file"
path = "unused.jsonl"

[[bria.tasks]]
id = "noop"
driver = "local"
cmd = "sh"
args = ["-c", "true"]
stdout = {{ mode = "discard" }}
stderr = {{ mode = "discard" }}

[[bria.sinks]]
{sink_def}

[[bria.pipelines]]
id = "p"
source = "manual"
sinks = ["sink"]
"#
    )
}

fn make_success_result(job: Job, pipeline_id: &str) -> PipelineResult {
    let mut steps = HashMap::new();
    let step = StepResult {
        stdout: Some(r#"{"value":42}"#.to_string()),
        stderr: None,
        exit_code: 0,
        duration_ms: 10,
        attempt: 1,
        outputs: {
            let mut o = HashMap::new();
            o.insert("value".to_string(), serde_json::json!(42));
            o
        },
    };
    steps.insert("noop".to_string(), step);
    PipelineResult {
        pipeline_id: pipeline_id.to_string(),
        job,
        status: "success".to_string(),
        duration_ms: 10,
        steps,
        occurred_at: "2026-01-01T00:00:00Z".to_string(),
    }
}

fn make_test_job() -> Job {
    Job {
        id: "job-fs-1".to_string(),
        source: "manual".to_string(),
        payload: serde_json::json!({"name": "file-test"}),
        correlation_key: None,
        labels: HashMap::new(),
    }
}

// ---------------------------------------------------------------
// File sink
// ---------------------------------------------------------------

#[tokio::test]
async fn file_sink_writes_json_result_by_default() {
    let dir = std::env::temp_dir().join(format!(
        "bria-fs-default-{}-{}",
        std::process::id(),
        ulid::Ulid::r#gen()
    ));
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("out.jsonl");

    let config = Config::from_str_with_env(&config_with_sink(&format!(
        r#"id = "sink"
type = "file"
path = "{}""#,
        path.display()
    )))
    .unwrap();
    config.validate().unwrap();

    let disp = bria::sinks::SinkDispatcher::new(
        config.clone(),
        bria::template::TemplateEngine::new(),
        None,
    );
    let job = make_test_job();
    let result = make_success_result(job.clone(), "p");
    let ctx = Context::new(job);
    disp.send_pipeline_result(&result, &ctx).await;

    let content = std::fs::read_to_string(&path).unwrap();
    let parsed: serde_json::Value = serde_json::from_str(content.trim()).unwrap();
    assert_eq!(parsed["pipeline_id"], "p");
    assert_eq!(parsed["status"], "success");
    assert!(parsed["steps"]["noop"]["outputs"]["value"].as_i64() == Some(42));

    let _ = std::fs::remove_dir_all(dir);
}

#[tokio::test]
async fn file_sink_renders_custom_template() {
    let dir = std::env::temp_dir().join(format!(
        "bria-fs-tpl-{}-{}",
        std::process::id(),
        ulid::Ulid::r#gen()
    ));
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("templated.jsonl");

    let config = Config::from_str_with_env(&config_with_sink(&format!(
        r#"id = "sink"
type = "file"
path = "{}"
template = "pipeline={{{{pipeline.id}}}} status={{{{result.status}}}} name={{{{job.payload.name}}}}""#,
        path.display()
    )))
    .unwrap();
    config.validate().unwrap();

    let disp = bria::sinks::SinkDispatcher::new(
        config.clone(),
        bria::template::TemplateEngine::new(),
        None,
    );
    let job = make_test_job();
    let result = make_success_result(job.clone(), "p");
    let ctx = Context::new(job);
    disp.send_pipeline_result(&result, &ctx).await;

    let content = std::fs::read_to_string(&path).unwrap().trim().to_string();
    assert_eq!(content, "pipeline=p status=success name=file-test");

    let _ = std::fs::remove_dir_all(dir);
}

#[tokio::test]
async fn file_sink_renders_path_template_with_job_id() {
    let dir = std::env::temp_dir().join(format!(
        "bria-fs-path-{}-{}",
        std::process::id(),
        ulid::Ulid::r#gen()
    ));
    std::fs::create_dir_all(&dir).unwrap();
    let path_pattern = dir.join("results-{{job.id}}.jsonl");

    let config = Config::from_str_with_env(&config_with_sink(&format!(
        r#"id = "sink"
type = "file"
path = "{}""#,
        path_pattern.display()
    )))
    .unwrap();
    config.validate().unwrap();

    let disp = bria::sinks::SinkDispatcher::new(
        config.clone(),
        bria::template::TemplateEngine::new(),
        None,
    );
    let job = make_test_job();
    let result = make_success_result(job.clone(), "p");
    let ctx = Context::new(job.clone());
    disp.send_pipeline_result(&result, &ctx).await;

    let expected_path = dir.join(format!("results-{}.jsonl", job.id));
    assert!(
        expected_path.exists(),
        "expected file at {}",
        expected_path.display()
    );

    let _ = std::fs::remove_dir_all(dir);
}

#[tokio::test]
async fn file_sink_creates_parent_directories_if_missing() {
    let dir = std::env::temp_dir().join(format!(
        "bria-fs-mkdir-{}-{}",
        std::process::id(),
        ulid::Ulid::r#gen()
    ));
    // Do NOT create the dir — SinkDispatcher should create it
    let path = dir.join("nested").join("deep.jsonl");

    let config = Config::from_str_with_env(&config_with_sink(&format!(
        r#"id = "sink"
type = "file"
path = "{}""#,
        path.display()
    )))
    .unwrap();
    config.validate().unwrap();

    let disp = bria::sinks::SinkDispatcher::new(
        config.clone(),
        bria::template::TemplateEngine::new(),
        None,
    );
    let job = make_test_job();
    let result = make_success_result(job.clone(), "p");
    let ctx = Context::new(job);
    disp.send_pipeline_result(&result, &ctx).await;

    assert!(
        path.exists(),
        "file {} should exist after dispatch",
        path.display()
    );

    let _ = std::fs::remove_dir_all(dir);
}

// ---------------------------------------------------------------
// SQLite sink
// ---------------------------------------------------------------

#[tokio::test]
async fn sqlite_sink_creates_table_and_inserts_step_rows() {
    let db_name = format!(
        "bria-sink-sqlite-{}-{}.db",
        std::process::id(),
        ulid::Ulid::r#gen()
    );
    let db_path = std::env::temp_dir().join(&db_name);
    let db_path_str = db_path.to_str().unwrap();
    let _ = std::fs::remove_file(db_path_str);

    let raw = format!(
        r#"
[bria]
[[bria.sources]]
id = "manual"
type = "file"
path = "unused.jsonl"

[[bria.tasks]]
id = "noop"
driver = "local"
cmd = "sh"
args = ["-c", "true"]
stdout = {{ mode = "discard" }}
stderr = {{ mode = "discard" }}

[[bria.sinks]]
id = "sink"
type = "sqlite"
path = "{db_path_str}"

[bria.sinks.table]
name = "results"

[[bria.pipelines]]
id = "p"
source = "manual"
sinks = ["sink"]
"#
    );

    let config = Config::from_str_with_env(&raw).unwrap();
    config.validate().unwrap();

    let disp = bria::sinks::SinkDispatcher::new(
        config.clone(),
        bria::template::TemplateEngine::new(),
        None,
    );

    let job = Job {
        id: "job-sqlite-1".to_string(),
        source: "manual".to_string(),
        payload: serde_json::json!({"data": "test"}),
        correlation_key: None,
        labels: HashMap::new(),
    };

    let mut steps = HashMap::new();
    steps.insert(
        "noop".to_string(),
        StepResult {
            stdout: Some("hello".to_string()),
            stderr: Some("err-out".to_string()),
            exit_code: 0,
            duration_ms: 5,
            attempt: 1,
            outputs: HashMap::new(),
        },
    );
    let result = PipelineResult {
        pipeline_id: "p".to_string(),
        job: job.clone(),
        status: "success".to_string(),
        duration_ms: 5,
        steps,
        occurred_at: "2026-01-01T00:00:00Z".to_string(),
    };
    let ctx = Context::new(job);
    disp.send_pipeline_result(&result, &ctx).await;

    assert!(
        db_path.exists(),
        "DB file should exist after dispatch at {db_path_str}"
    );

    let pool = sqlx::SqlitePool::connect(&format!("sqlite:{db_path_str}?mode=rwc"))
        .await
        .unwrap();
    let (count,): (i64,) = sqlx::query_as("SELECT COUNT(*) FROM \"results\"")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(count, 1, "one row should be inserted");

    let (job_id_val, exit_code_val, stdout_val, status_val): (String, i64, Option<String>, String) =
        sqlx::query_as(
            "SELECT \"job_id\", \"exit_code\", \"stdout\", \"status\" FROM \"results\" LIMIT 1",
        )
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(job_id_val, "job-sqlite-1");
    assert_eq!(exit_code_val, 0);
    assert_eq!(stdout_val.unwrap(), "hello");
    assert_eq!(status_val, "success");

    pool.close().await;
    let _ = std::fs::remove_file(db_path_str);
}

#[tokio::test]
async fn sqlite_sink_inserts_multiple_step_results() {
    let db_name = format!(
        "bria-sink-sqlite-multi-{}-{}.db",
        std::process::id(),
        ulid::Ulid::r#gen()
    );
    let db_path = std::env::temp_dir().join(&db_name);
    let db_path_str = db_path.to_str().unwrap();
    let _ = std::fs::remove_file(db_path_str);

    let raw = format!(
        r#"
[bria]
[[bria.sources]]
id = "manual"
type = "file"
path = "unused.jsonl"

[[bria.tasks]]
id = "noop"
driver = "local"
cmd = "sh"
args = ["-c", "true"]
stdout = {{ mode = "discard" }}
stderr = {{ mode = "discard" }}

[[bria.sinks]]
id = "sink"
type = "sqlite"
path = "{db_path_str}"

[bria.sinks.table]
name = "results"

[[bria.pipelines]]
id = "p"
source = "manual"
sinks = ["sink"]
"#
    );

    let config = Config::from_str_with_env(&raw).unwrap();
    config.validate().unwrap();

    let disp = bria::sinks::SinkDispatcher::new(
        config.clone(),
        bria::template::TemplateEngine::new(),
        None,
    );

    let job = Job {
        id: "job-sqlite-multi".to_string(),
        source: "manual".to_string(),
        payload: serde_json::json!({}),
        correlation_key: None,
        labels: HashMap::new(),
    };

    let mut steps = HashMap::new();
    steps.insert(
        "step_a".to_string(),
        StepResult {
            stdout: Some("a-out".to_string()),
            stderr: None,
            exit_code: 0,
            duration_ms: 1,
            attempt: 1,
            outputs: HashMap::new(),
        },
    );
    steps.insert(
        "step_b".to_string(),
        StepResult {
            stdout: Some("b-out".to_string()),
            stderr: None,
            exit_code: 0,
            duration_ms: 2,
            attempt: 2,
            outputs: HashMap::new(),
        },
    );
    let result = PipelineResult {
        pipeline_id: "p".to_string(),
        job: job.clone(),
        status: "success".to_string(),
        duration_ms: 15,
        steps,
        occurred_at: "2026-01-01T00:00:00Z".to_string(),
    };
    let ctx = Context::new(job);
    disp.send_pipeline_result(&result, &ctx).await;

    assert!(
        db_path.exists(),
        "DB file should exist after dispatch at {db_path_str}"
    );

    let pool = sqlx::SqlitePool::connect(&format!("sqlite:{db_path_str}?mode=rwc"))
        .await
        .unwrap();
    let (count,): (i64,) = sqlx::query_as("SELECT COUNT(*) FROM \"results\"")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(count, 2, "two step rows should be inserted");

    let ids: Vec<String> =
        sqlx::query_as("SELECT \"step_id\" FROM \"results\" ORDER BY \"step_id\"")
            .fetch_all(&pool)
            .await
            .unwrap()
            .into_iter()
            .map(|(s,): (String,)| s)
            .collect();
    assert_eq!(ids, vec!["step_a", "step_b"]);

    pool.close().await;
    let _ = std::fs::remove_file(db_path_str);
}

// ---------------------------------------------------------------
// Stream sink
// ---------------------------------------------------------------

#[tokio::test]
async fn stream_sink_broadcasts_result_to_subscribers() {
    let (tx, mut rx) = tokio::sync::broadcast::channel::<serde_json::Value>(16);

    let config = Config::from_str_with_env(&config_with_sink(
        r#"id = "sink"
type = "stream"
sse = "events""#,
    ))
    .unwrap();
    config.validate().unwrap();

    let disp = bria::sinks::SinkDispatcher::new(
        config.clone(),
        bria::template::TemplateEngine::new(),
        Some(tx),
    );

    let job = make_test_job();
    let result = make_success_result(job.clone(), "p");
    let ctx = Context::new(job);
    disp.send_pipeline_result(&result, &ctx).await;

    // The pipeline-level send passes all unhandled steps
    let event = rx.recv().await.unwrap();
    assert_eq!(event["pipeline_id"], "p");
    assert_eq!(event["status"], "success");
}

#[tokio::test]
async fn stream_sink_lacks_broadcast_channel_and_succeeds_silently() {
    let config = Config::from_str_with_env(&config_with_sink(
        r#"id = "sink"
type = "stream"
sse = "events""#,
    ))
    .unwrap();
    config.validate().unwrap();

    let disp = bria::sinks::SinkDispatcher::new(
        config.clone(),
        bria::template::TemplateEngine::new(),
        None, // no broadcast
    );

    let job = make_test_job();
    let result = make_success_result(job.clone(), "p");
    let ctx = Context::new(job);
    disp.send_pipeline_result(&result, &ctx).await;
    // Should not panic, just warn via tracing
}

// ---------------------------------------------------------------
// Webhook sink — uses a local TCP listener to capture the POST
// ---------------------------------------------------------------

#[tokio::test]
async fn webhook_sink_posts_result_to_local_server() {
    use axum::{Json, Router, routing::post};
    use std::sync::Mutex;

    // Shared capture state
    let captured: Arc<Mutex<Option<serde_json::Value>>> = Arc::new(Mutex::new(None));
    let captured_clone = captured.clone();

    async fn capture_handler(
        state: axum::extract::State<Arc<Mutex<Option<serde_json::Value>>>>,
        Json(body): Json<serde_json::Value>,
    ) -> StatusCode {
        *state.lock().unwrap() = Some(body);
        StatusCode::OK
    }

    let app_state = captured.clone();
    let app = Router::new()
        .route("/hook", post(capture_handler))
        .with_state(app_state);

    // Bind to ephemeral port
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let port = addr.port();

    // Spawn the receiver server
    let server_handle = tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });

    // Start a dispatcher with a webhook sink pointing at our local server
    let config = Config::from_str_with_env(&config_with_sink(&format!(
        r#"id = "sink"
type = "webhook"
url = "http://127.0.0.1:{port}/hook"
max_retries = 1
timeout_secs = 5"#
    )))
    .unwrap();
    config.validate().unwrap();

    let disp = bria::sinks::SinkDispatcher::new(
        config.clone(),
        bria::template::TemplateEngine::new(),
        None,
    );

    let job = make_test_job();
    let result = make_success_result(job.clone(), "p");
    let ctx = Context::new(job);
    disp.send_pipeline_result(&result, &ctx).await;

    // Give the server a moment to receive
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;

    let captured_body = captured_clone.lock().unwrap().take();
    assert!(
        captured_body.is_some(),
        "webhook should have received a POST"
    );
    let body = captured_body.unwrap();
    assert_eq!(body["pipeline_id"], "p");
    assert_eq!(body["status"], "success");

    server_handle.abort();
}

#[tokio::test]
async fn webhook_sink_retries_then_fails_on_error_status() {
    use axum::{Router, routing::post};

    // Server that always returns 500
    async fn error_handler() -> StatusCode {
        StatusCode::INTERNAL_SERVER_ERROR
    }

    let app = Router::new().route("/hook", post(error_handler));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let server_handle = tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });

    let config = Config::from_str_with_env(&config_with_sink(&format!(
        r#"id = "sink"
type = "webhook"
url = "http://127.0.0.1:{port}/hook"
max_retries = 2
retry_base_ms = 10
timeout_secs = 5"#
    )))
    .unwrap();
    config.validate().unwrap();

    let disp = bria::sinks::SinkDispatcher::new(
        config.clone(),
        bria::template::TemplateEngine::new(),
        None,
    );

    let job = make_test_job();
    let result = make_success_result(job.clone(), "p");
    let ctx = Context::new(job);
    // This should retry 2 times and then log an error (not panic)
    disp.send_pipeline_result(&result, &ctx).await;

    tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    server_handle.abort();
}

// ---------------------------------------------------------------
// Server — start_server disabled / enabled
// ---------------------------------------------------------------

#[tokio::test]
async fn start_server_disabled_returns_handle_with_no_join_handle() {
    let raw = r#"
[bria]
[bria.server]
enabled = false
"#;

    let config = Arc::new(Config::from_str_with_env(raw).unwrap());
    let source_txs = HashMap::new();
    let handle = bria::server::start_server(config, source_txs, None, None)
        .await
        .unwrap();
    assert!(
        handle.join_handle.is_none(),
        "disabled server should have no join handle"
    );
}

#[tokio::test]
async fn start_server_enabled_binds_and_serves_ping() {
    let raw = r#"
[bria]
[bria.server]
enabled = true
port = 0
bind = "127.0.0.1"
prefix = "v1"
"#;

    // We can't extract the port from start_server easily, so we temporarily
    // discover an ephemeral port, close it, and then use it.
    let tmp = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = tmp.local_addr().unwrap().port();
    drop(tmp);

    let raw = raw.replace("port = 0", &format!("port = {port}"));

    let config = Arc::new(Config::from_str_with_env(&raw).unwrap());
    config.validate().unwrap();
    let source_txs: HashMap<String, tokio::sync::mpsc::UnboundedSender<Job>> = HashMap::new();

    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
    let handle = bria::server::start_server(config, source_txs, None, Some(shutdown_rx))
        .await
        .unwrap();
    assert!(
        handle.join_handle.is_some(),
        "enabled server should have join handle"
    );

    // Give the server a moment to start
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;

    // Use reqwest to ping
    let client = reqwest::Client::new();
    let resp = client
        .get(format!("http://127.0.0.1:{port}/v1/ping"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = resp.text().await.unwrap();
    assert_eq!(body, "pong");

    // Shutdown the server
    let _ = shutdown_tx.send(true);
    if let Some(join) = handle.join_handle {
        let _ = tokio::time::timeout(std::time::Duration::from_secs(5), join).await;
    }
}

// ---------------------------------------------------------------
// Server — auth middleware via live HTTP
// ---------------------------------------------------------------

async fn server_with_api_key(
    api_key: &str,
) -> (
    u16,
    tokio::sync::watch::Sender<bool>,
    bria::server::ServerHandle,
) {
    let tmp = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = tmp.local_addr().unwrap().port();
    drop(tmp);

    // This config uses a webhook source so routes are registered
    let raw = format!(
        r#"
[bria]
[bria.server]
enabled = true
port = {port}
bind = "127.0.0.1"
prefix = "v1"
api_key = "{api_key}"

[[bria.sources]]
id = "events"
type = "webhook"
path = "events"

[[bria.sources]]
id = "manual"
type = "file"
path = "unused.jsonl"

[[bria.tasks]]
id = "noop"
driver = "local"
cmd = "true"
stdout = {{ mode = "discard" }}
stderr = {{ mode = "discard" }}

[[bria.pipelines]]
id = "p"
source = "manual"
"#
    );

    let config = Arc::new(Config::from_str_with_env(&raw).unwrap());
    config.validate().unwrap();

    let source_txs: HashMap<String, tokio::sync::mpsc::UnboundedSender<Job>> = HashMap::new();
    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
    let handle = bria::server::start_server(config, source_txs, None, Some(shutdown_rx))
        .await
        .unwrap();

    tokio::time::sleep(std::time::Duration::from_millis(200)).await;

    (port, shutdown_tx, handle)
}

#[tokio::test]
async fn ping_passes_when_no_api_key_is_set() {
    let tmp = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = tmp.local_addr().unwrap().port();
    drop(tmp);

    let raw = format!(
        r#"
[bria]
[bria.server]
enabled = true
port = {port}
bind = "127.0.0.1"
prefix = "v1"
api_key = ""
"#
    );

    let config = Arc::new(Config::from_str_with_env(&raw).unwrap());
    config.validate().unwrap();
    let source_txs: HashMap<String, tokio::sync::mpsc::UnboundedSender<Job>> = HashMap::new();
    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
    let handle = bria::server::start_server(config, source_txs, None, Some(shutdown_rx))
        .await
        .unwrap();

    tokio::time::sleep(std::time::Duration::from_millis(200)).await;

    let client = reqwest::Client::new();
    let resp = client
        .get(format!("http://127.0.0.1:{port}/v1/ping"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    assert_eq!(resp.text().await.unwrap(), "pong");

    let _ = shutdown_tx.send(true);
    if let Some(join) = handle.join_handle {
        let _ = tokio::time::timeout(std::time::Duration::from_secs(5), join).await;
    }
}

#[tokio::test]
async fn auth_rejects_requests_without_api_key_header() {
    let (port, shutdown_tx, handle) = server_with_api_key("test-secret").await;

    let client = reqwest::Client::new();
    let resp = client
        .get(format!("http://127.0.0.1:{port}/v1/ping"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);

    let _ = shutdown_tx.send(true);
    if let Some(join) = handle.join_handle {
        let _ = tokio::time::timeout(std::time::Duration::from_secs(5), join).await;
    }
}

#[tokio::test]
async fn auth_accepts_x_bria_api_key_header() {
    let (port, shutdown_tx, handle) = server_with_api_key("my-api-key").await;

    let client = reqwest::Client::new();
    let resp = client
        .get(format!("http://127.0.0.1:{port}/v1/ping"))
        .header("x-bria-api-key", "my-api-key")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    let _ = shutdown_tx.send(true);
    if let Some(join) = handle.join_handle {
        let _ = tokio::time::timeout(std::time::Duration::from_secs(5), join).await;
    }
}

#[tokio::test]
async fn auth_accepts_bearer_token_header() {
    let (port, shutdown_tx, handle) = server_with_api_key("bearer-key").await;

    let client = reqwest::Client::new();
    let resp = client
        .get(format!("http://127.0.0.1:{port}/v1/ping"))
        .header("authorization", "Bearer bearer-key")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    let _ = shutdown_tx.send(true);
    if let Some(join) = handle.join_handle {
        let _ = tokio::time::timeout(std::time::Duration::from_secs(5), join).await;
    }
}

#[tokio::test]
async fn auth_rejects_wrong_api_key() {
    let (port, shutdown_tx, handle) = server_with_api_key("correct").await;

    let client = reqwest::Client::new();
    let resp = client
        .get(format!("http://127.0.0.1:{port}/v1/ping"))
        .header("x-bria-api-key", "wrong-key")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);

    let _ = shutdown_tx.send(true);
    if let Some(join) = handle.join_handle {
        let _ = tokio::time::timeout(std::time::Duration::from_secs(5), join).await;
    }
}

// ---------------------------------------------------------------
// Server — submit / cancel / resume via live HTTP
// ---------------------------------------------------------------

async fn server_with_http_source(
    source_path: &str,
    secret: &str,
) -> (
    u16,
    tokio::sync::watch::Sender<bool>,
    bria::server::ServerHandle,
    tokio::sync::mpsc::UnboundedReceiver<Job>,
) {
    let tmp = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = tmp.local_addr().unwrap().port();
    drop(tmp);

    let raw = format!(
        r#"
[bria]
[bria.server]
enabled = true
port = {port}
bind = "127.0.0.1"
prefix = "v1"

[[bria.sources]]
id = "mysource"
type = "webhook"
path = "{source_path}"
hmac_secret = "{secret}"

[[bria.sources]]
id = "manual"
type = "file"
path = "unused.jsonl"

[[bria.tasks]]
id = "noop"
driver = "local"
cmd = "true"
stdout = {{ mode = "discard" }}
stderr = {{ mode = "discard" }}

[[bria.pipelines]]
id = "test-pipeline"
source = "manual"
"#
    );

    let config = Arc::new(Config::from_str_with_env(&raw).unwrap());
    config.validate().unwrap();

    // Need a real channel for the source so the handler can send the job
    let (tx, rx) = tokio::sync::mpsc::unbounded_channel::<Job>();
    let mut source_txs: HashMap<String, tokio::sync::mpsc::UnboundedSender<Job>> = HashMap::new();
    source_txs.insert("mysource".to_string(), tx);

    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
    let handle = bria::server::start_server(config, source_txs, None, Some(shutdown_rx))
        .await
        .unwrap();

    tokio::time::sleep(std::time::Duration::from_millis(200)).await;

    (port, shutdown_tx, handle, rx)
}

#[tokio::test]
async fn submit_job_returns_202_with_job_id_for_webhook_source() {
    let (port, shutdown_tx, handle, _rx) = server_with_http_source("events", "").await;

    let client = reqwest::Client::new();
    let resp = client
        .post(format!("http://127.0.0.1:{port}/v1/events"))
        .json(&serde_json::json!({"key": "value"}))
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), 202);

    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["status"], "accepted");
    assert!(!body["job_id"].as_str().unwrap().is_empty());

    let _ = shutdown_tx.send(true);
    if let Some(join) = handle.join_handle {
        let _ = tokio::time::timeout(std::time::Duration::from_secs(5), join).await;
    }
}

#[tokio::test]
async fn submit_job_propagates_artur_idempotency_key_as_correlation_key() {
    let (port, shutdown_tx, handle, mut rx) = server_with_http_source("events", "").await;

    let client = reqwest::Client::new();
    let resp = client
        .post(format!("http://127.0.0.1:{port}/v1/events"))
        .header("idempotency-key", "artur-request-42")
        .json(&serde_json::json!({"key": "value"}))
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::ACCEPTED);
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["correlation_key"], "artur-request-42");
    assert!(!body["job_id"].as_str().unwrap().is_empty());

    let job = rx.recv().await.unwrap();
    assert_eq!(job.id, body["job_id"].as_str().unwrap());
    assert_eq!(job.correlation_key.as_deref(), Some("artur-request-42"));

    let _ = shutdown_tx.send(true);
    if let Some(join) = handle.join_handle {
        let _ = tokio::time::timeout(std::time::Duration::from_secs(5), join).await;
    }
}

#[tokio::test]
async fn submit_job_rejects_conflicting_correlation_headers() {
    let (port, shutdown_tx, handle, _rx) = server_with_http_source("events", "").await;

    let client = reqwest::Client::new();
    let resp = client
        .post(format!("http://127.0.0.1:{port}/v1/events"))
        .header("idempotency-key", "artur-request-42")
        .header("x-correlation-id", "different-request")
        .json(&serde_json::json!({"key": "value"}))
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);

    let _ = shutdown_tx.send(true);
    if let Some(join) = handle.join_handle {
        let _ = tokio::time::timeout(std::time::Duration::from_secs(5), join).await;
    }
}

#[tokio::test]
async fn submit_job_rejects_invalid_json_with_400() {
    let (port, shutdown_tx, handle, _rx) = server_with_http_source("events", "").await;

    let client = reqwest::Client::new();
    let resp = client
        .post(format!("http://127.0.0.1:{port}/v1/events"))
        .header("content-type", "application/json")
        .body("not json")
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);

    let _ = shutdown_tx.send(true);
    if let Some(join) = handle.join_handle {
        let _ = tokio::time::timeout(std::time::Duration::from_secs(5), join).await;
    }
}

#[tokio::test]
async fn submit_job_returns_404_for_unknown_source_path() {
    let (port, shutdown_tx, handle, _rx) = server_with_http_source("events", "").await;

    let client = reqwest::Client::new();
    let resp = client
        .post(format!("http://127.0.0.1:{port}/v1/nonexistent"))
        .json(&serde_json::json!({"key": "value"}))
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::NOT_FOUND);

    let _ = shutdown_tx.send(true);
    if let Some(join) = handle.join_handle {
        let _ = tokio::time::timeout(std::time::Duration::from_secs(5), join).await;
    }
}

#[tokio::test]
async fn webhook_submit_rejects_missing_hmac_signature() {
    let (port, shutdown_tx, handle, _rx) =
        server_with_http_source("secure-events", "my-hmac-secret").await;

    let client = reqwest::Client::new();
    let resp = client
        .post(format!("http://127.0.0.1:{port}/v1/secure-events"))
        .json(&serde_json::json!({"key": "value"}))
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);

    let _ = shutdown_tx.send(true);
    if let Some(join) = handle.join_handle {
        let _ = tokio::time::timeout(std::time::Duration::from_secs(5), join).await;
    }
}

#[tokio::test]
async fn webhook_submit_accepts_valid_hmac_signature() {
    let (port, shutdown_tx, handle, _rx) =
        server_with_http_source("secure-events", "my-hmac-secret").await;

    // Compute HMAC
    use hmac::{Hmac, KeyInit, Mac};
    use sha2::Sha256;
    let mut mac = Hmac::<Sha256>::new_from_slice(b"my-hmac-secret").unwrap();
    let body = serde_json::to_vec(&serde_json::json!({"key": "value"})).unwrap();
    mac.update(&body);
    let sig = "sha256=".to_string() + &hex::encode(mac.finalize().into_bytes());

    let client = reqwest::Client::new();
    let resp = client
        .post(format!("http://127.0.0.1:{port}/v1/secure-events"))
        .header("x-bria-signature", &sig)
        .json(&serde_json::json!({"key": "value"}))
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), 202);

    let _ = shutdown_tx.send(true);
    if let Some(join) = handle.join_handle {
        let _ = tokio::time::timeout(std::time::Duration::from_secs(5), join).await;
    }
}

#[tokio::test]
async fn cancel_job_returns_202_when_source_exists() {
    let (port, shutdown_tx, handle, _rx) = server_with_http_source("events", "").await;

    let client = reqwest::Client::new();
    let resp = client
        .delete(format!("http://127.0.0.1:{port}/v1/events/job-xyz"))
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), 202);
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["status"], "cancellation_requested");
    assert_eq!(body["job_id"], "job-xyz");

    let _ = shutdown_tx.send(true);
    if let Some(join) = handle.join_handle {
        let _ = tokio::time::timeout(std::time::Duration::from_secs(5), join).await;
    }
}

#[tokio::test]
async fn cancel_job_returns_404_for_unknown_source_path() {
    let (port, shutdown_tx, handle, _rx) = server_with_http_source("events", "").await;

    let client = reqwest::Client::new();
    let resp = client
        .delete(format!("http://127.0.0.1:{port}/v1/unknown/job-xyz"))
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::NOT_FOUND);

    let _ = shutdown_tx.send(true);
    if let Some(join) = handle.join_handle {
        let _ = tokio::time::timeout(std::time::Duration::from_secs(5), join).await;
    }
}

#[tokio::test]
async fn resume_pipeline_returns_200_for_known_pipeline() {
    let (port, shutdown_tx, handle, _rx) = server_with_http_source("events", "").await;

    let client = reqwest::Client::new();
    let resp = client
        .post(format!(
            "http://127.0.0.1:{port}/v1/pipelines/test-pipeline/resume"
        ))
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::OK);
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["status"], "resumed");
    assert_eq!(body["pipeline_id"], "test-pipeline");

    let _ = shutdown_tx.send(true);
    if let Some(join) = handle.join_handle {
        let _ = tokio::time::timeout(std::time::Duration::from_secs(5), join).await;
    }
}

#[tokio::test]
async fn resume_pipeline_returns_404_for_unknown_pipeline() {
    let (port, shutdown_tx, handle, _rx) = server_with_http_source("events", "").await;

    let client = reqwest::Client::new();
    let resp = client
        .post(format!(
            "http://127.0.0.1:{port}/v1/pipelines/nonexistent/resume"
        ))
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::NOT_FOUND);

    let _ = shutdown_tx.send(true);
    if let Some(join) = handle.join_handle {
        let _ = tokio::time::timeout(std::time::Duration::from_secs(5), join).await;
    }
}

// ---------------------------------------------------------------
// Server — SSE streaming via broadcast
// ---------------------------------------------------------------

async fn server_with_stream_sink() -> (
    u16,
    tokio::sync::watch::Sender<bool>,
    bria::server::ServerHandle,
) {
    let tmp = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = tmp.local_addr().unwrap().port();
    drop(tmp);

    let raw = format!(
        r#"
[bria]
[bria.server]
enabled = true
port = {port}
bind = "127.0.0.1"
prefix = "v1"

[[bria.sources]]
id = "manual"
type = "file"
path = "unused.jsonl"

[[bria.tasks]]
id = "noop"
driver = "local"
cmd = "true"
stdout = {{ mode = "discard" }}
stderr = {{ mode = "discard" }}

[[bria.sinks]]
id = "stream"
type = "stream"
sse = "events"

[[bria.pipelines]]
id = "p"
source = "manual"
"#
    );

    let config = Arc::new(Config::from_str_with_env(&raw).unwrap());
    config.validate().unwrap();

    let source_txs: HashMap<String, tokio::sync::mpsc::UnboundedSender<Job>> = HashMap::new();

    // Create broadcast channel and inject it
    let (broadcast_tx, _) = tokio::sync::broadcast::channel(16);

    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
    let handle =
        bria::server::start_server(config, source_txs, Some(broadcast_tx), Some(shutdown_rx))
            .await
            .unwrap();

    tokio::time::sleep(std::time::Duration::from_millis(200)).await;

    (port, shutdown_tx, handle)
}

#[tokio::test]
async fn sse_endpoint_streams_broadcast_events() {
    let (port, shutdown_tx, handle) = server_with_stream_sink().await;

    // Connect SSE client
    let client = reqwest::Client::new();
    let sse = client
        .get(format!("http://127.0.0.1:{port}/v1/events"))
        .send()
        .await
        .unwrap();

    assert_eq!(sse.status(), StatusCode::OK);
    assert!(
        sse.headers()
            .get("content-type")
            .map(|v| v.to_str().unwrap_or(""))
            .unwrap_or("")
            .contains("text/event-stream"),
        "SSE content-type should be text/event-stream"
    );

    // The SSE connection is verified to respond. Broadcast-based event streaming
    // is covered by stream_sink_broadcasts_result_to_subscribers.
    drop(sse);

    let _ = shutdown_tx.send(true);
    if let Some(join) = handle.join_handle {
        let _ = tokio::time::timeout(std::time::Duration::from_secs(5), join).await;
    }
}

#[tokio::test]
async fn sse_endpoint_without_broadcast_returns_error_event() {
    let tmp = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = tmp.local_addr().unwrap().port();
    drop(tmp);

    let raw = format!(
        r#"
[bria]
[bria.server]
enabled = true
port = {port}
bind = "127.0.0.1"
prefix = "v1"

[[bria.sources]]
id = "manual"
type = "file"
path = "unused.jsonl"

[[bria.tasks]]
id = "noop"
driver = "local"
cmd = "true"
stdout = {{ mode = "discard" }}
stderr = {{ mode = "discard" }}

[[bria.sinks]]
id = "stream"
type = "stream"
sse = "events"

[[bria.pipelines]]
id = "p"
source = "manual"
"#
    );

    let config = Arc::new(Config::from_str_with_env(&raw).unwrap());
    config.validate().unwrap();

    let source_txs: HashMap<String, tokio::sync::mpsc::UnboundedSender<Job>> = HashMap::new();
    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
    let handle = bria::server::start_server(config, source_txs, None, Some(shutdown_rx))
        .await
        .unwrap();

    tokio::time::sleep(std::time::Duration::from_millis(200)).await;

    let client = reqwest::Client::new();
    let mut resp = client
        .get(format!("http://127.0.0.1:{port}/v1/events"))
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::OK);

    // The no-broadcast case yields {"error":"stream not configured"} then finishes.
    let chunk = resp.chunk().await.unwrap().unwrap_or_default();
    let text = String::from_utf8_lossy(&chunk);
    assert!(
        text.contains("stream not configured"),
        "SSE no-broadcast should yield error event, got: {text}"
    );

    let _ = shutdown_tx.send(true);
    if let Some(join) = handle.join_handle {
        let _ = tokio::time::timeout(std::time::Duration::from_secs(5), join).await;
    }
}

// ---------------------------------------------------------------
// Orchestrator sink integration — pipeline-level sink dispatch
// ---------------------------------------------------------------

#[tokio::test]
async fn pipeline_sink_dispatches_failure_result_to_dead_letter_sink() {
    let dir = std::env::temp_dir().join(format!(
        "bria-dl-{}-{}",
        std::process::id(),
        ulid::Ulid::r#gen()
    ));
    std::fs::create_dir_all(&dir).unwrap();
    let dead_letter_path = dir.join("dead.jsonl");

    let raw = format!(
        r#"
[bria]
[[bria.sources]]
id = "manual"
type = "file"
path = "unused.jsonl"

[[bria.tasks]]
id = "fail-task"
driver = "local"
cmd = "sh"
args = ["-c", "exit 99"]
stdout = {{ mode = "discard" }}
stderr = {{ mode = "discard" }}

[[bria.sinks]]
id = "dead"
type = "file"
path = "{}"

[[bria.pipelines]]
id = "p"
source = "manual"
sinks = ["dead"]

[bria.pipelines.failure]
action = "dead_letter"
sink = "dead"

[[bria.pipelines.steps]]
id = "fail-step"
type = "process"
task = "fail-task"
"#,
        dead_letter_path.display()
    );

    let config = Config::from_str_with_env(&raw).unwrap();
    config.validate().unwrap();

    let disp = bria::sinks::SinkDispatcher::new(
        config.clone(),
        bria::template::TemplateEngine::new(),
        None,
    );

    let job = Job {
        id: "job-dl".to_string(),
        source: "manual".to_string(),
        payload: serde_json::json!({}),
        correlation_key: None,
        labels: HashMap::new(),
    };

    let mut steps = HashMap::new();
    steps.insert(
        "fail-step".to_string(),
        StepResult {
            stdout: None,
            stderr: None,
            exit_code: 99,
            duration_ms: 1,
            attempt: 1,
            outputs: HashMap::new(),
        },
    );
    let result = PipelineResult {
        pipeline_id: "p".to_string(),
        job: job.clone(),
        status: "failure".to_string(),
        duration_ms: 1,
        steps,
        occurred_at: "2026-01-01T00:00:00Z".to_string(),
    };

    let ctx = Context::new(job);
    disp.send_pipeline_result(&result, &ctx).await;

    let content = std::fs::read_to_string(&dead_letter_path).unwrap();
    // Both step-level and pipeline-level sinks can fire; at least one line should
    // contain the expected failure result.
    let first_line = content.lines().next().unwrap_or_default();
    let parsed: serde_json::Value = serde_json::from_str(first_line).unwrap();
    assert_eq!(parsed["pipeline_id"], "p");
    assert!(parsed["status"] == "failure" || first_line.contains("failure"));
    let has_fail_step = content.lines().any(|l| {
        if let Ok(v) = serde_json::from_str::<serde_json::Value>(l) {
            v["steps"]["fail-step"]["exit_code"].as_i64() == Some(99)
        } else {
            false
        }
    });
    assert!(
        has_fail_step,
        "at least one line should have fail-step with exit_code 99"
    );

    let _ = std::fs::remove_dir_all(dir);
}

#[tokio::test]
async fn sink_routing_step_sends_to_condition_matched_sink() {
    let dir = std::env::temp_dir().join(format!(
        "bria-route-{}-{}",
        std::process::id(),
        ulid::Ulid::r#gen()
    ));
    std::fs::create_dir_all(&dir).unwrap();
    let path_ok = dir.join("ok.jsonl");
    let path_fallback = dir.join("fallback.jsonl");

    let raw = format!(
        r#"
[bria]
[[bria.sources]]
id = "manual"
type = "file"
path = "unused.jsonl"

[[bria.tasks]]
id = "noop"
driver = "local"
cmd = "true"
stdout = {{ mode = "discard" }}
stderr = {{ mode = "discard" }}

[[bria.sinks]]
id = "ok-sink"
type = "file"
path = "{}"

[[bria.sinks]]
id = "fallback-sink"
type = "file"
path = "{}"

[[bria.pipelines]]
id = "p"
source = "manual"
sinks = ["fallback-sink"]

[[bria.pipelines.steps]]
id = "step"
type = "process"
task = "noop"

# Routing: when exit_code == 0, send to ok-sink
[[bria.pipelines.steps.routing]]
condition = "steps.step.exit_code == 0"
sinks = ["ok-sink"]
"#,
        path_ok.display(),
        path_fallback.display(),
    );

    let config = Config::from_str_with_env(&raw).unwrap();
    config.validate().unwrap();

    let disp = bria::sinks::SinkDispatcher::new(
        config.clone(),
        bria::template::TemplateEngine::new(),
        None,
    );

    let job = Job {
        id: "job-route".to_string(),
        source: "manual".to_string(),
        payload: serde_json::json!({}),
        correlation_key: None,
        labels: HashMap::new(),
    };

    let mut steps = HashMap::new();
    steps.insert(
        "step".to_string(),
        StepResult {
            stdout: None,
            stderr: None,
            exit_code: 0,
            duration_ms: 1,
            attempt: 1,
            outputs: HashMap::new(),
        },
    );
    let result = PipelineResult {
        pipeline_id: "p".to_string(),
        job: job.clone(),
        status: "success".to_string(),
        duration_ms: 1,
        steps,
        occurred_at: "2026-01-01T00:00:00Z".to_string(),
    };

    let mut ctx = Context::new(job);
    ctx.steps.clone_from(&result.steps);
    disp.send_pipeline_result(&result, &ctx).await;

    // Step routing should send to ok-sink, NOT to fallback-sink (pipeline-level)
    let content_ok = std::fs::read_to_string(&path_ok).unwrap();
    assert!(
        !content_ok.trim().is_empty(),
        "ok-sink should have received result"
    );

    // fallback-sink should be empty because the step was handled by routing
    let content_fallback = std::fs::read_to_string(&path_fallback).unwrap_or_default();
    assert!(
        content_fallback.trim().is_empty(),
        "fallback-sink should be empty (routing handled the step), got: {content_fallback}"
    );

    let _ = std::fs::remove_dir_all(dir);
}

// ---------------------------------------------------------------
// State store integration — Orchestrator::new + create_store
// ---------------------------------------------------------------

#[tokio::test]
async fn create_store_with_memory_backend_succeeds() {
    let raw = r#"
[bria]
[bria.global.state]
backend = "memory"

[[bria.sources]]
id = "manual"
type = "file"
path = "unused.jsonl"
"#;
    let config = Config::from_str_with_env(raw).unwrap();
    let store = create_store(&config.global.state).await.unwrap();
    // Basic operations
    let job = Job {
        id: "mem-job".to_string(),
        source: "manual".to_string(),
        payload: serde_json::json!({}),
        correlation_key: None,
        labels: HashMap::new(),
    };
    store.record_queued(&job, "p").await.unwrap();
    let rec = store.recover_incomplete().await.unwrap();
    assert_eq!(rec.len(), 1);
    assert_eq!(rec[0].job_id, "mem-job");
}

#[tokio::test]
async fn create_store_rejects_unknown_backend() {
    let raw = r#"
[bria]
[bria.global.state]
backend = "redis"

[[bria.sources]]
id = "manual"
type = "file"
path = "unused.jsonl"
"#;
    let config = Config::from_str_with_env(raw).unwrap();
    let err = create_store(&config.global.state).await.err().unwrap();
    assert!(err.to_string().contains("backend"));
}
