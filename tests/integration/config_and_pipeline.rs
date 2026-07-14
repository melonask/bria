use bria::{Config, Job, run_pipeline_once};

fn minimal_config() -> String {
    r#"
version = 1
[bria]
[[bria.sources]]
id = "manual"
type = "file"
path = "unused.jsonl"

[[bria.tasks]]
id = "greet"
driver = "local"
cmd = "sh"
args = ["-c", "printf '{\"greeting\":\"hello %s\"}' \"$1\"", "sh", "{{job.payload.name}}"]
timeout_secs = 5

[bria.tasks.stdout]
mode = "capture"
max_bytes = 1024

[bria.tasks.stderr]
mode = "capture"
max_bytes = 1024

[[bria.pipelines]]
id = "greeting"
source = "manual"
concurrency = 1

[[bria.pipelines.steps]]
id = "say"
type = "process"
task = "greet"

[bria.pipelines.steps.outputs]
format = "json"

[[bria.pipelines.steps.outputs.fields]]
key = "greeting"
name = "greeting"
"#
    .to_string()
}

#[test]
fn env_substitution_fails_for_unset_variables() {
    let raw = r#"
version = 1
[bria]
[[bria.sources]]
id = "source"
type = "webhook"
path = "events"
hmac_secret = "${BRIA_INTEGRATION_TEST_MISSING_ENV_DO_NOT_SET}"
"#;

    let err = Config::from_str_with_env(raw).expect_err("unset env var must fail config load");
    assert!(
        err.to_string()
            .contains("BRIA_INTEGRATION_TEST_MISSING_ENV_DO_NOT_SET")
    );
}

#[test]
fn validation_rejects_unknown_task_reference() {
    let raw = r#"
version = 1
[bria]
[[bria.sources]]
id = "manual"
type = "file"
path = "unused.jsonl"

[[bria.pipelines]]
id = "bad"
source = "manual"

[[bria.pipelines.steps]]
id = "missing"
type = "process"
task = "does-not-exist"
"#;

    let config = Config::from_str_with_env(raw).expect("config should parse");
    let err = config
        .validate()
        .expect_err("unknown task ref must fail validation");
    assert!(err.to_string().contains("does-not-exist"));
}

#[test]
fn validation_requires_pg_state_url() {
    let raw = r#"
version = 1
[bria]
[bria.global.state]
backend = "pg"

[[bria.sources]]
id = "manual"
type = "file"
path = "unused.jsonl"
"#;

    let config = Config::from_str_with_env(raw).expect("config should parse");
    let err = config
        .validate()
        .expect_err("pg state without pg_url must fail validation");
    assert!(err.to_string().contains("global.state.pg_url"));
}

#[test]
fn stderr_default_is_one_mib() {
    let raw = r#"
version = 1
[bria]
[[bria.sources]]
id = "manual"
type = "file"
path = "unused.jsonl"

[[bria.tasks]]
id = "defaults"
driver = "local"
cmd = "true"

[[bria.pipelines]]
id = "p"
source = "manual"

[[bria.pipelines.steps]]
id = "s"
type = "process"
task = "defaults"
"#;

    let config = Config::from_str_with_env(raw).expect("config should parse");
    assert_eq!(config.tasks[0].stderr.max_bytes, 1024 * 1024);
    assert_eq!(config.tasks[0].stdout.max_bytes, 10 * 1024 * 1024);
}

#[cfg(feature = "sqlite")]
#[tokio::test]
async fn sqlite_state_store_recovers_running_jobs_and_clears_completed_jobs() {
    let db_path = std::env::temp_dir().join(format!(
        "bria-state-test-{}-{}",
        std::process::id(),
        ulid::Ulid::r#gen()
    ));
    let raw = format!(
        r#"
version = 1
[bria]
[bria.global.state]
backend = "sqlite"
sqlite_path = "{}"

[[bria.sources]]
id = "manual"
type = "file"
path = "unused.jsonl"
"#,
        db_path.display()
    );

    let config = Config::from_str_with_env(&raw).expect("config should parse");
    let store = bria::create_store(&config.global.state)
        .await
        .expect("sqlite state store should initialize");
    let job = Job {
        id: "state-job-1".to_string(),
        source: "manual".to_string(),
        payload: serde_json::json!({ "order_id": "ord-1" }),
        correlation_key: Some("ord-1".to_string()),
        labels: std::collections::HashMap::new(),
    };

    store.record_queued(&job, "pipeline-a").await.unwrap();
    store.record_running(&job, "pipeline-a").await.unwrap();

    let recovered = store.recover_incomplete().await.unwrap();
    assert_eq!(recovered.len(), 1);
    assert_eq!(recovered[0].job_id, "state-job-1");
    assert_eq!(recovered[0].pipeline_id, "pipeline-a");
    assert_eq!(recovered[0].state, "running");
    assert_eq!(
        recovered[0].payload,
        serde_json::json!({ "order_id": "ord-1" })
    );

    store
        .record_completed("state-job-1", "pipeline-a", "success")
        .await
        .unwrap();
    assert!(store.recover_incomplete().await.unwrap().is_empty());

    let _ = std::fs::remove_file(db_path);
}

#[tokio::test]
async fn docker_style_stdin_template_is_available_to_local_tasks() {
    let config = r#"
version = 1
[bria]
[[bria.sources]]
id = "manual"
type = "file"
path = "unused.jsonl"

[[bria.tasks]]
id = "stdin-task"
driver = "local"
cmd = "sh"
args = ["-c", "cat"]

[bria.tasks.stdin]
mode = "template"
template = "{{job.payload.message}}"

[bria.tasks.stdout]
mode = "capture"
max_bytes = 128

[[bria.pipelines]]
id = "stdin-demo"
source = "manual"

[[bria.pipelines.steps]]
id = "run"
type = "process"
task = "stdin-task"

[bria.pipelines.steps.outputs]
format = "text"

[[bria.pipelines.steps.outputs.fields]]
name = "body"
"#;

    let job = Job {
        id: "job-stdin".to_string(),
        source: "manual".to_string(),
        payload: serde_json::json!({ "message": "from-template" }),
        correlation_key: None,
        labels: std::collections::HashMap::new(),
    };

    let result = run_pipeline_once(config, "stdin-demo", job).await.unwrap();
    assert_eq!(result.status, "success");
    assert_eq!(result.steps["run"].stdout.as_deref(), Some("from-template"));
}

#[tokio::test]
async fn run_pipeline_once_executes_local_task_and_extracts_output() {
    let job = Job {
        id: "job-1".to_string(),
        source: "manual".to_string(),
        payload: serde_json::json!({ "name": "Bria" }),
        correlation_key: None,
        labels: std::collections::HashMap::new(),
    };

    let result = run_pipeline_once(&minimal_config(), "greeting", job)
        .await
        .expect("pipeline should run");

    assert_eq!(result.status, "success");
    let step = result.steps.get("say").expect("step result exists");
    assert_eq!(step.exit_code, 0);
    assert_eq!(
        step.outputs.get("greeting"),
        Some(&serde_json::json!("hello Bria"))
    );
}

#[tokio::test]
async fn map_step_sets_nested_payload_targets() {
    let config = r#"
version = 1
[bria]
[[bria.sources]]
id = "manual"
type = "file"
path = "unused.jsonl"

[[bria.tasks]]
id = "print-nested"
driver = "local"
cmd = "sh"
args = ["-c", "printf '%s' \"$1\"", "sh", "{{job.payload.normalized.location.url}}"]

[bria.tasks.stdout]
mode = "capture"
max_bytes = 128

[[bria.pipelines]]
id = "nested-map"
source = "manual"

[[bria.pipelines.steps]]
id = "shape"
type = "map"

[[bria.pipelines.steps.set]]
target = "job.payload.normalized.location.url"
expr = '"s3://" + job.payload.bucket + "/" + job.payload.key'

[[bria.pipelines.steps]]
id = "print"
type = "process"
task = "print-nested"
"#;

    let job = Job {
        id: "job-map-nested".to_string(),
        source: "manual".to_string(),
        payload: serde_json::json!({ "bucket": "assets", "key": "image.jpg" }),
        correlation_key: None,
        labels: std::collections::HashMap::new(),
    };

    let result = run_pipeline_once(config, "nested-map", job).await.unwrap();
    assert_eq!(result.status, "success");
    assert_eq!(
        result.steps["print"].stdout.as_deref(),
        Some("s3://assets/image.jpg")
    );
}

#[tokio::test]
async fn cel_expressions_can_read_pipeline_id() {
    let config = r#"
version = 1
[bria]
[[bria.sources]]
id = "manual"
type = "file"
path = "unused.jsonl"

[[bria.tasks]]
id = "print-pipeline"
driver = "local"
cmd = "sh"
args = ["-c", "printf '%s' \"$1\"", "sh", "{{job.payload.pipeline_id}}"]

[bria.tasks.stdout]
mode = "capture"
max_bytes = 128

[[bria.pipelines]]
id = "cel-pipeline-id"
source = "manual"

[[bria.pipelines.steps]]
id = "set-id"
type = "map"

[[bria.pipelines.steps.set]]
target = "job.payload.pipeline_id"
expr = "pipeline.id"

[[bria.pipelines.steps]]
id = "print-id"
type = "process"
task = "print-pipeline"
"#;

    let job = Job {
        id: "job-cel-pipeline-id".to_string(),
        source: "manual".to_string(),
        payload: serde_json::json!({}),
        correlation_key: None,
        labels: std::collections::HashMap::new(),
    };

    let result = run_pipeline_once(config, "cel-pipeline-id", job)
        .await
        .expect("pipeline should run");

    assert_eq!(result.status, "success");
    assert_eq!(
        result.steps["print-id"].stdout.as_deref(),
        Some("cel-pipeline-id")
    );
}

#[tokio::test]
async fn directory_file_source_does_not_reemit_unchanged_files() {
    let unique = format!(
        "bria-dir-source-{}-{}",
        std::process::id(),
        ulid::Ulid::r#gen()
    );
    let base = std::env::temp_dir().join(unique);
    let input_dir = base.join("jobs");
    let output_file = base.join("results.jsonl");
    std::fs::create_dir_all(&input_dir).unwrap();
    std::fs::write(
        input_dir.join("jobs.jsonl"),
        "{\"id\":\"one\",\"value\":1}\n",
    )
    .unwrap();

    let raw = format!(
        r#"
version = 1
[bria]
[[bria.sources]]
id = "files"
type = "file"
path = "{}"
poll_interval_secs = 1
track_cursor = true
id_field = "id"

[[bria.tasks]]
id = "emit"
driver = "local"
cmd = "sh"
args = ["-c", "printf '{{\"seen\":\"%s\"}}' \"$1\"", "sh", "{{{{job.id}}}}"]

[bria.tasks.stdout]
mode = "capture"
max_bytes = 128

[[bria.sinks]]
id = "out"
type = "file"
path = "{}"

[[bria.pipelines]]
id = "p"
source = "files"
sinks = ["out"]

[[bria.pipelines.steps]]
id = "emit"
type = "process"
task = "emit"
"#,
        input_dir.display(),
        output_file.display()
    );

    let config = Config::from_str_with_env(&raw).unwrap();
    config.validate().unwrap();
    let orchestrator = bria::Orchestrator::new(config).await.unwrap();
    let handle = tokio::spawn(async move { orchestrator.run().await });
    tokio::time::sleep(std::time::Duration::from_millis(2600)).await;
    handle.abort();

    let lines = std::fs::read_to_string(&output_file).unwrap_or_default();
    let emitted: Vec<&str> = lines.lines().collect();
    assert_eq!(emitted.len(), 1, "expected one emission, got: {lines}");

    let _ = std::fs::remove_dir_all(base);
}

#[test]
fn cli_ping_prints_pong() {
    let binary = env!("CARGO_BIN_EXE_bria");
    let output = std::process::Command::new(binary)
        .arg("ping")
        .output()
        .expect("bria ping should execute");

    assert!(output.status.success());
    assert_eq!(String::from_utf8_lossy(&output.stdout).trim(), "pong");
}

#[test]
fn cli_check_validates_config_without_starting_workers() {
    let config_path = std::env::temp_dir().join(format!("bria-check-{}.toml", ulid::Ulid::r#gen()));
    std::fs::write(
        &config_path,
        r#"
version = 1
[bria]
[[bria.sources]]
id = "input"
type = "file"
path = "input.jsonl"
"#,
    )
    .unwrap();

    let binary = env!("CARGO_BIN_EXE_bria");
    let output = std::process::Command::new(binary)
        .args(["check", "--config", config_path.to_str().unwrap()])
        .output()
        .expect("bria check should execute");

    assert!(output.status.success());
    assert!(String::from_utf8_lossy(&output.stdout).contains("Configuration is valid"));
    let _ = std::fs::remove_file(config_path);
}

#[tokio::test]
async fn condition_skip_to_skips_intermediate_steps() {
    let config = r#"
version = 1
[bria]
[[bria.sources]]
id = "manual"
type = "file"
path = "unused.jsonl"

[[bria.tasks]]
id = "fail-if-run"
driver = "local"
cmd = "sh"
args = ["-c", "exit 33"]

[[bria.tasks]]
id = "target-task"
driver = "local"
cmd = "sh"
args = ["-c", "printf '{\"ok\":true}'"]

[[bria.pipelines]]
id = "skip-demo"
source = "manual"

[[bria.pipelines.steps]]
id = "guard"
type = "condition"
expr = "false"
action = "skip_to"
skip_to = "target"

[[bria.pipelines.steps]]
id = "skipped"
type = "process"
task = "fail-if-run"

[[bria.pipelines.steps]]
id = "target"
type = "process"
task = "target-task"

[bria.pipelines.steps.outputs]
format = "json"

[[bria.pipelines.steps.outputs.fields]]
key = "ok"
name = "ok"
"#;

    let job = Job {
        id: "job-skip".to_string(),
        source: "manual".to_string(),
        payload: serde_json::json!({}),
        correlation_key: None,
        labels: std::collections::HashMap::new(),
    };

    let result = run_pipeline_once(config, "skip-demo", job).await.unwrap();

    assert_eq!(result.status, "success");
    assert!(result.steps.contains_key("guard"));
    assert!(!result.steps.contains_key("skipped"));
    assert_eq!(
        result.steps["target"].outputs.get("ok"),
        Some(&serde_json::json!(true))
    );
}

#[tokio::test]
async fn condition_emit_stops_pipeline_early() {
    let config = r#"
version = 1
[bria]
[[bria.sources]]
id = "manual"
type = "file"
path = "unused.jsonl"

[[bria.tasks]]
id = "fail-if-run"
driver = "local"
cmd = "sh"
args = ["-c", "exit 44"]

[[bria.pipelines]]
id = "emit-demo"
source = "manual"

[[bria.pipelines.steps]]
id = "gate"
type = "condition"
expr = "false"
action = "emit"

[[bria.pipelines.steps]]
id = "after"
type = "process"
task = "fail-if-run"
"#;

    let job = Job {
        id: "job-emit".to_string(),
        source: "manual".to_string(),
        payload: serde_json::json!({}),
        correlation_key: None,
        labels: std::collections::HashMap::new(),
    };

    let result = run_pipeline_once(config, "emit-demo", job).await.unwrap();

    assert_eq!(result.status, "success");
    assert!(result.steps.contains_key("gate"));
    assert!(!result.steps.contains_key("after"));
    assert!(!result.steps.contains_key("__emit"));
}

#[tokio::test]
async fn parallel_steps_retain_all_results_for_fan_in() {
    let config = r#"
version = 1
[bria]
[[bria.sources]]
id = "manual"
type = "file"
path = "unused.jsonl"

[[bria.tasks]]
id = "root"
driver = "local"
cmd = "sh"
args = ["-c", "printf '{\"root\":true}'"]

[[bria.tasks]]
id = "left-task"
driver = "local"
cmd = "sh"
args = ["-c", "printf '{\"left\":true}'"]

[[bria.tasks]]
id = "right-task"
driver = "local"
cmd = "sh"
args = ["-c", "printf '{\"right\":true}'"]

[[bria.tasks]]
id = "join-task"
driver = "local"
cmd = "sh"
args = ["-c", "printf '{\"joined\":true}'"]

[[bria.pipelines]]
id = "parallel-demo"
source = "manual"
concurrency = 2

[[bria.pipelines.steps]]
id = "root"
type = "process"
task = "root"

[[bria.pipelines.steps]]
id = "left"
type = "process"
task = "left-task"
depends_on = ["root"]

[[bria.pipelines.steps]]
id = "right"
type = "process"
task = "right-task"
depends_on = ["root"]

[[bria.pipelines.steps]]
id = "join"
type = "process"
task = "join-task"
depends_on = ["left", "right"]
"#;

    let job = Job {
        id: "job-parallel".to_string(),
        source: "manual".to_string(),
        payload: serde_json::json!({}),
        correlation_key: None,
        labels: std::collections::HashMap::new(),
    };

    let result = run_pipeline_once(config, "parallel-demo", job)
        .await
        .unwrap();

    assert_eq!(result.status, "success");
    assert!(result.steps.contains_key("left"));
    assert!(result.steps.contains_key("right"));
    assert!(result.steps.contains_key("join"));
}

#[tokio::test]
async fn stdout_discard_mode_does_not_capture_output() {
    let config = r#"
version = 1
[bria]
[[bria.sources]]
id = "manual"
type = "file"
path = "unused.jsonl"

[[bria.tasks]]
id = "no-capture"
driver = "local"
cmd = "sh"
args = ["-c", "printf secret"]

[bria.tasks.stdout]
mode = "discard"

[[bria.pipelines]]
id = "discard-demo"
source = "manual"

[[bria.pipelines.steps]]
id = "run"
type = "process"
task = "no-capture"
"#;

    let job = Job {
        id: "job-discard".to_string(),
        source: "manual".to_string(),
        payload: serde_json::json!({}),
        correlation_key: None,
        labels: std::collections::HashMap::new(),
    };

    let result = run_pipeline_once(config, "discard-demo", job)
        .await
        .unwrap();

    assert_eq!(result.status, "success");
    assert_eq!(result.steps["run"].stdout, None);
}

#[tokio::test]
async fn timeout_term_still_fails_even_if_process_handles_sigterm() {
    let config = r#"
version = 1
[bria]
[[bria.sources]]
id = "manual"
type = "file"
path = "unused.jsonl"

[[bria.tasks]]
id = "slow"
driver = "local"
cmd = "sh"
args = ["-c", "trap 'exit 0' TERM; sleep 5"]
timeout_secs = 1
timeout_action = "term"
kill_grace_secs = 1

[[bria.pipelines]]
id = "timeout-demo"
source = "manual"

[[bria.pipelines.steps]]
id = "run"
type = "process"
task = "slow"
"#;

    let job = Job {
        id: "job-timeout".to_string(),
        source: "manual".to_string(),
        payload: serde_json::json!({}),
        correlation_key: None,
        labels: std::collections::HashMap::new(),
    };

    let result = run_pipeline_once(config, "timeout-demo", job)
        .await
        .unwrap();

    assert_eq!(result.status, "failure");
    assert!(
        result.steps["run"]
            .stderr
            .as_deref()
            .unwrap_or("")
            .contains("timed out")
    );
}
