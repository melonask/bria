use bria::{Job, run_pipeline_once as bria_run_pipeline_once};
use std::collections::HashMap;

// ---------- helpers ----------

fn empty_job(id: &str, source: &str) -> Job {
    Job {
        id: id.to_string(),
        source: source.to_string(),
        payload: serde_json::json!({}),
        correlation_key: None,
        labels: HashMap::new(),
    }
}

fn job_with_payload(id: &str, source: &str, payload: serde_json::Value) -> Job {
    Job {
        id: id.to_string(),
        source: source.to_string(),
        payload,
        correlation_key: None,
        labels: HashMap::new(),
    }
}

async fn run_pipeline_once(
    config: &str,
    pipeline_id: &str,
    job: Job,
) -> bria::Result<bria::PipelineResult> {
    let config = if config.trim_start().starts_with("version =") {
        config.to_string()
    } else {
        format!("version = 1\n{config}")
    };
    bria_run_pipeline_once(&config, pipeline_id, job).await
}

// ---------- process step: success / failure / retry ----------

#[tokio::test]
async fn single_process_step_succeeds() {
    let config = r#"
version = 1
[bria]
[[bria.sources]]
id = "src"
type = "file"
path = "unused.jsonl"

[[bria.tasks]]
id = "ok"
driver = "local"
cmd = "true"

[[bria.pipelines]]
id = "p"
source = "src"

[[bria.pipelines.steps]]
id = "s"
type = "process"
task = "ok"
"#;
    let result = run_pipeline_once(config, "p", empty_job("j", "src"))
        .await
        .unwrap();
    assert_eq!(result.status, "success");
    assert_eq!(result.steps["s"].exit_code, 0);
    assert_eq!(result.steps["s"].attempt, 1);
}

#[tokio::test]
async fn retry_exhaustion_results_in_pipeline_failure() {
    let config = r#"
version = 1
[bria]
[[bria.sources]]
id = "src"
type = "file"
path = "unused.jsonl"

[[bria.tasks]]
id = "always-fail"
driver = "local"
cmd = "sh"
args = ["-c", "exit 9"]

[bria.global.retry]
max_attempts = 0
base_delay_ms = 1
max_delay_ms = 10

[[bria.pipelines]]
id = "p"
source = "src"

[[bria.pipelines.steps]]
id = "failing"
type = "process"
task = "always-fail"

[bria.pipelines.steps.retry]
max_attempts = 3
base_delay_ms = 1
max_delay_ms = 10
jitter = 0.0
"#;
    let result = run_pipeline_once(config, "p", empty_job("j", "src"))
        .await
        .unwrap();
    assert_eq!(result.status, "failure");
    let step = &result.steps["failing"];
    assert!(
        step.stderr
            .as_deref()
            .unwrap_or("")
            .contains("not in success_exit_codes")
            || step
                .stderr
                .as_deref()
                .unwrap_or("")
                .contains("failed all attempts")
    );
}

#[tokio::test]
async fn retry_succeeds_on_second_attempt() {
    // Use a marker file to track attempts; first attempt fails, second succeeds
    use std::sync::atomic::{AtomicU32, Ordering};
    static COUNTER: AtomicU32 = AtomicU32::new(0);
    let counter_val = COUNTER.fetch_add(1, Ordering::SeqCst);
    let tmp = std::env::temp_dir();
    // Ensure parent directory exists
    let _ = std::fs::create_dir_all(&tmp);
    let marker = tmp.join(format!("bria-retry-{}", counter_val));
    let _ = std::fs::remove_file(&marker);

    let marker_str = marker.to_string_lossy().to_string();
    let config_str = format!(
        r#"
version = 1
[bria]
[[bria.sources]]
id = "src"
type = "file"
path = "unused.jsonl"

[[bria.tasks]]
id = "retry-task"
driver = "local"
cmd = "sh"
args = ["-c", "if [ -f '{0}' ]; then exit 0; else touch '{0}' && exit 8; fi"]

[bria.global.retry]
max_attempts = 0
base_delay_ms = 1
max_delay_ms = 10

[[bria.pipelines]]
id = "p"
source = "src"

[[bria.pipelines.steps]]
id = "step"
type = "process"
task = "retry-task"

[bria.pipelines.steps.retry]
max_attempts = 2
base_delay_ms = 1
max_delay_ms = 10
jitter = 0.0
"#,
        marker_str
    );

    let result = run_pipeline_once(&config_str, "p", empty_job("j", "src"))
        .await
        .unwrap();
    // First attempt: file missing -> touch + exit 8 (fails, triggers retry).
    // Second attempt: file exists -> exit 0 (success).
    assert_eq!(result.status, "success");
    assert_eq!(result.steps["step"].exit_code, 0);
    assert_eq!(result.steps["step"].attempt, 2);
    let _ = std::fs::remove_file(&marker);
}

// ---------- parallel steps ----------

#[tokio::test]
async fn parallel_step_failure_fails_pipeline_but_preserves_sibling_result() {
    let config = r#"
version = 1
[bria]
[[bria.sources]]
id = "src"
type = "file"
path = "unused.jsonl"

[[bria.tasks]]
id = "root-task"
driver = "local"
cmd = "true"

[[bria.tasks]]
id = "bad"
driver = "local"
cmd = "sh"
args = ["-c", "exit 13"]

[[bria.tasks]]
id = "good"
driver = "local"
cmd = "sh"
args = ["-c", "printf ok"]

[bria.global.retry]
max_attempts = 0
base_delay_ms = 1
max_delay_ms = 10

[[bria.pipelines]]
id = "p"
source = "src"
concurrency = 2

[[bria.pipelines.steps]]
id = "root"
type = "process"
task = "root-task"

[[bria.pipelines.steps]]
id = "left"
type = "process"
task = "bad"
depends_on = ["root"]

[[bria.pipelines.steps]]
id = "right"
type = "process"
task = "good"
depends_on = ["root"]
"#;
    let result = run_pipeline_once(config, "p", empty_job("j", "src"))
        .await
        .unwrap();
    assert_eq!(result.status, "failure");
    assert!(result.steps.contains_key("left"));
    assert!(result.steps.contains_key("right"));
    assert_eq!(result.steps["right"].exit_code, 0);
    assert_eq!(result.steps["left"].exit_code, -1);
}

// ---------- condition steps ----------

#[tokio::test]
async fn condition_true_continues_pipeline() {
    let config = r#"
version = 1
[bria]
[[bria.sources]]
id = "src"
type = "file"
path = "unused.jsonl"

[[bria.tasks]]
id = "downstream"
driver = "local"
cmd = "sh"
args = ["-c", "printf '{\"ran\":true}'"]

[[bria.pipelines]]
id = "p"
source = "src"

[[bria.pipelines.steps]]
id = "gate"
type = "condition"
expr = "true"

[[bria.pipelines.steps]]
id = "work"
type = "process"
task = "downstream"

[bria.pipelines.steps.outputs]
format = "json"

[[bria.pipelines.steps.outputs.fields]]
key = "ran"
name = "ran"
"#;
    let result = run_pipeline_once(config, "p", empty_job("j", "src"))
        .await
        .unwrap();
    assert_eq!(result.status, "success");
    assert!(result.steps.contains_key("gate"));
    assert!(result.steps.contains_key("work"));
    assert_eq!(
        result.steps["work"].outputs.get("ran"),
        Some(&serde_json::json!(true))
    );
}

#[tokio::test]
async fn condition_false_with_fail_action_fails_pipeline() {
    let config = r#"
version = 1
[bria]
[[bria.sources]]
id = "src"
type = "file"
path = "unused.jsonl"

[[bria.tasks]]
id = "never-run"
driver = "local"
cmd = "sh"
args = ["-c", "exit 0"]

[[bria.pipelines]]
id = "p"
source = "src"

[[bria.pipelines.steps]]
id = "guard"
type = "condition"
expr = "false"
action = "fail"
reason = "payload rejected by guard"

[[bria.pipelines.steps]]
id = "after"
type = "process"
task = "never-run"
"#;
    let result = run_pipeline_once(config, "p", empty_job("j", "src"))
        .await
        .unwrap();
    assert_eq!(result.status, "failure");
    assert!(result.steps.contains_key("guard"));
    assert!(!result.steps.contains_key("after"));
    let guard_stderr = result.steps["guard"].stderr.as_deref().unwrap_or("");
    assert!(guard_stderr.contains("payload rejected by guard"));
}

#[tokio::test]
async fn condition_fail_without_reason_produces_default_message() {
    let config = r#"
version = 1
[bria]
[[bria.sources]]
id = "src"
type = "file"
path = "unused.jsonl"

[[bria.pipelines]]
id = "p"
source = "src"

[[bria.pipelines.steps]]
id = "gate"
type = "condition"
expr = "false"
action = "fail"
"#;
    let result = run_pipeline_once(config, "p", empty_job("j", "src"))
        .await
        .unwrap();
    assert_eq!(result.status, "failure");
    let stderr = result.steps["gate"].stderr.as_deref().unwrap_or("");
    assert!(stderr.contains("evaluated to false"));
}

#[tokio::test]
async fn condition_skip_to_non_forward_target_advances_one_level() {
    // skip_to target is in the current level (or past) — proceeds to next level normally
    let config = r#"
version = 1
[bria]
[[bria.sources]]
id = "src"
type = "file"
path = "unused.jsonl"

[[bria.tasks]]
id = "after"
driver = "local"
cmd = "sh"
args = ["-c", "printf '{\"reached\":true}'"]

[[bria.pipelines]]
id = "p"
source = "src"

[[bria.pipelines.steps]]
id = "first"
type = "condition"
expr = "true"

[[bria.pipelines.steps]]
id = "skip-trigger"
type = "condition"
expr = "false"
action = "skip_to"
skip_to = "first"

[[bria.pipelines.steps]]
id = "last"
type = "process"
task = "after"

[bria.pipelines.steps.outputs]
format = "json"

[[bria.pipelines.steps.outputs.fields]]
key = "reached"
name = "reached"
"#;
    let result = run_pipeline_once(config, "p", empty_job("j", "src"))
        .await
        .unwrap();
    assert_eq!(result.status, "success");
    assert!(result.steps.contains_key("last"));
    assert_eq!(
        result.steps["last"].outputs.get("reached"),
        Some(&serde_json::json!(true))
    );
}

#[tokio::test]
async fn condition_skip_to_missing_target_fails_at_validation() {
    let config = r#"
version = 1
[bria]
[[bria.sources]]
id = "src"
type = "file"
path = "unused.jsonl"

[[bria.pipelines]]
id = "p"
source = "src"

[[bria.pipelines.steps]]
id = "gate"
type = "condition"
expr = "false"
action = "skip_to"
skip_to = "nonexistent"
"#;
    let err = run_pipeline_once(config, "p", empty_job("j", "src"))
        .await
        .expect_err("skip_to unknown step must fail validation");
    let msg = err.to_string();
    assert!(
        msg.contains("nonexistent") || msg.contains("unknown step"),
        "{msg}"
    );
}

#[tokio::test]
async fn condition_unknown_action_fails_at_validation() {
    let config = r#"
version = 1
[bria]
[[bria.sources]]
id = "src"
type = "file"
path = "unused.jsonl"

[[bria.pipelines]]
id = "p"
source = "src"

[[bria.pipelines.steps]]
id = "gate"
type = "condition"
expr = "false"
action = "invalid_action"
"#;
    let err = run_pipeline_once(config, "p", empty_job("j", "src"))
        .await
        .expect_err("unknown condition action must fail validation");
    let msg = err.to_string();
    assert!(
        msg.contains("invalid_action") || msg.contains("invalid"),
        "{msg}"
    );
}

#[tokio::test]
async fn condition_cel_parse_error_fails_pipeline() {
    let config = r#"
version = 1
[bria]
[[bria.sources]]
id = "src"
type = "file"
path = "unused.jsonl"

[[bria.pipelines]]
id = "p"
source = "src"

[[bria.pipelines.steps]]
id = "gate"
type = "condition"
expr = "syntax error ]["
action = "fail"
"#;
    let result = run_pipeline_once(config, "p", empty_job("j", "src"))
        .await
        .unwrap();
    assert_eq!(result.status, "failure");
    let stderr = result.steps["gate"].stderr.as_deref().unwrap_or("");
    assert!(
        stderr.contains("evaluation error")
            || stderr.contains("parse")
            || stderr.contains("Syntax")
    );
}

#[tokio::test]
async fn condition_evaluates_against_payload_fields() {
    let config = r#"
version = 1
[bria]
[[bria.sources]]
id = "src"
type = "file"
path = "unused.jsonl"

[[bria.tasks]]
id = "work"
driver = "local"
cmd = "sh"
args = ["-c", "printf yes"]

[[bria.pipelines]]
id = "p"
source = "src"

[[bria.pipelines.steps]]
id = "gate"
type = "condition"
expr = "job.payload.approved == true"

[[bria.pipelines.steps]]
id = "work"
type = "process"
task = "work"
"#;
    let job = job_with_payload("j", "src", serde_json::json!({"approved": true}));
    let result = run_pipeline_once(config, "p", job).await.unwrap();
    assert_eq!(result.status, "success");
    assert!(result.steps.contains_key("work"));
}

#[tokio::test]
async fn condition_steps_can_reference_previous_step_results() {
    let config = r#"
version = 1
[bria]
[[bria.sources]]
id = "src"
type = "file"
path = "unused.jsonl"

[[bria.tasks]]
id = "first"
driver = "local"
cmd = "true"

[[bria.tasks]]
id = "second"
driver = "local"
cmd = "sh"
args = ["-c", "printf reached"]

[[bria.pipelines]]
id = "p"
source = "src"

[[bria.pipelines.steps]]
id = "one"
type = "process"
task = "first"

[[bria.pipelines.steps]]
id = "gate"
type = "condition"
expr = "steps.one.exit_code == 0"

[[bria.pipelines.steps]]
id = "two"
type = "process"
task = "second"
"#;
    let result = run_pipeline_once(config, "p", empty_job("j", "src"))
        .await
        .unwrap();
    assert_eq!(result.status, "success");
    assert!(result.steps.contains_key("two"));
}

// ---------- map steps ----------

#[tokio::test]
async fn map_multiple_set_entries_apply_in_sequence() {
    let config = r#"
version = 1
[bria]
[[bria.sources]]
id = "src"
type = "file"
path = "unused.jsonl"

[[bria.tasks]]
id = "print"
driver = "local"
cmd = "sh"
args = ["-c", "printf '%s %s' \"$1\" \"$2\"", "sh", "{{job.payload.first}}", "{{job.payload.second}}"]

[bria.tasks.stdout]
mode = "capture"
max_bytes = 128

[[bria.pipelines]]
id = "p"
source = "src"

[[bria.pipelines.steps]]
id = "shape"
type = "map"

[[bria.pipelines.steps.set]]
target = "job.payload.first"
expr = "'hello'"

[[bria.pipelines.steps.set]]
target = "job.payload.second"
expr = "'world'"

[[bria.pipelines.steps]]
id = "print"
type = "process"
task = "print"
"#;
    let result = run_pipeline_once(config, "p", empty_job("j", "src"))
        .await
        .unwrap();
    assert_eq!(result.status, "success");
    assert_eq!(result.steps["print"].stdout.as_deref(), Some("hello world"));
}

#[tokio::test]
async fn map_single_segment_target_fails_pipeline() {
    let config = r#"
version = 1
[bria]
[[bria.sources]]
id = "src"
type = "file"
path = "unused.jsonl"

[[bria.pipelines]]
id = "p"
source = "src"

[[bria.pipelines.steps]]
id = "bad-map"
type = "map"

[[bria.pipelines.steps.set]]
target = "no_dot"
expr = "42"
"#;
    let result = run_pipeline_once(config, "p", empty_job("j", "src"))
        .await
        .unwrap();
    assert_eq!(result.status, "failure");
    assert!(
        result.steps["bad-map"]
            .stderr
            .as_deref()
            .unwrap_or("")
            .contains("Invalid target path")
    );
}

#[tokio::test]
async fn map_unknown_namespace_target_fails_pipeline() {
    let config = r#"
version = 1
[bria]
[[bria.sources]]
id = "src"
type = "file"
path = "unused.jsonl"

[[bria.pipelines]]
id = "p"
source = "src"

[[bria.pipelines.steps]]
id = "bad-ns"
type = "map"

[[bria.pipelines.steps.set]]
target = "steps.x.stdout"
expr = "'value'"
"#;
    let result = run_pipeline_once(config, "p", empty_job("j", "src"))
        .await
        .unwrap();
    assert_eq!(result.status, "failure");
    let err = result.steps["bad-ns"].stderr.as_deref().unwrap_or("");
    assert!(err.contains("Unknown target namespace") || err.contains("Cannot set"));
}

#[tokio::test]
async fn map_non_object_payload_fails_pipeline() {
    let config = r#"
version = 1
[bria]
[[bria.sources]]
id = "src"
type = "file"
path = "unused.jsonl"

[[bria.pipelines]]
id = "p"
source = "src"

[[bria.pipelines.steps]]
id = "bad-payload"
type = "map"

[[bria.pipelines.steps.set]]
target = "job.payload.field"
expr = "'value'"
"#;
    let job = job_with_payload("j", "src", serde_json::json!(42));
    let result = run_pipeline_once(config, "p", job).await.unwrap();
    assert_eq!(result.status, "failure");
    assert!(
        result.steps["bad-payload"]
            .stderr
            .as_deref()
            .unwrap_or("")
            .contains("not an object")
    );
}

#[tokio::test]
async fn map_eval_error_fails_pipeline() {
    let config = r#"
version = 1
[bria]
[[bria.sources]]
id = "src"
type = "file"
path = "unused.jsonl"

[[bria.pipelines]]
id = "p"
source = "src"

[[bria.pipelines.steps]]
id = "bad-expr"
type = "map"

[[bria.pipelines.steps.set]]
target = "job.payload.field"
expr = "1/0"
"#;
    let result = run_pipeline_once(config, "p", empty_job("j", "src"))
        .await
        .unwrap();
    assert_eq!(result.status, "failure");
    let stderr = result.steps["bad-expr"].stderr.as_deref().unwrap_or("");
    assert!(
        stderr.contains("division by zero")
            || stderr.contains("evaluation")
            || stderr.contains("Expression")
    );
}

#[tokio::test]
async fn map_deep_nested_target_creates_intermediate_objects() {
    let config = r#"
version = 1
[bria]
[[bria.sources]]
id = "src"
type = "file"
path = "unused.jsonl"

[[bria.tasks]]
id = "read-deep"
driver = "local"
cmd = "sh"
args = ["-c", "printf '%s' \"$1\"", "sh", "{{job.payload.a.b.c}}"]

[bria.tasks.stdout]
mode = "capture"
max_bytes = 128

[[bria.pipelines]]
id = "p"
source = "src"

[[bria.pipelines.steps]]
id = "deep"
type = "map"

[[bria.pipelines.steps.set]]
target = "job.payload.a.b.c"
expr = "'nested-value'"

[[bria.pipelines.steps]]
id = "reader"
type = "process"
task = "read-deep"
"#;
    let result = run_pipeline_once(config, "p", empty_job("j", "src"))
        .await
        .unwrap();
    assert_eq!(result.status, "success");
    assert_eq!(
        result.steps["reader"].stdout.as_deref(),
        Some("nested-value")
    );
}

// ---------- stdin modes ----------

#[tokio::test]
async fn stdin_payload_mode_writes_job_payload_to_process() {
    let config = r#"
version = 1
[bria]
[[bria.sources]]
id = "src"
type = "file"
path = "unused.jsonl"

[[bria.tasks]]
id = "read-stdin"
driver = "local"
cmd = "sh"
args = ["-c", "cat"]

[bria.tasks.stdin]
mode = "payload"

[bria.tasks.stdout]
mode = "capture"
max_bytes = 1024

[[bria.pipelines]]
id = "p"
source = "src"

[[bria.pipelines.steps]]
id = "read"
type = "process"
task = "read-stdin"

[bria.pipelines.steps.outputs]
format = "json"

[[bria.pipelines.steps.outputs.fields]]
key = "msg"
name = "msg"
"#;
    let job = job_with_payload("j", "src", serde_json::json!({"msg": "hello-stdin"}));
    let result = run_pipeline_once(config, "p", job).await.unwrap();
    assert_eq!(result.status, "success");
    let stdout = result.steps["read"].stdout.as_deref().unwrap_or("");
    assert!(stdout.contains("hello-stdin"));
}

#[tokio::test]
async fn stdin_none_mode_closes_stdin_immediately() {
    let config = r#"
version = 1
[bria]
[[bria.sources]]
id = "src"
type = "file"
path = "unused.jsonl"

[[bria.tasks]]
id = "no-stdin"
driver = "local"
cmd = "sh"
args = ["-c", "cat; true"]

[bria.tasks.stdin]
mode = "none"

[bria.tasks.stdout]
mode = "capture"
max_bytes = 64

[[bria.pipelines]]
id = "p"
source = "src"

[[bria.pipelines.steps]]
id = "run"
type = "process"
task = "no-stdin"
"#;
    let job = job_with_payload("j", "src", serde_json::json!({"data": "ignored"}));
    let result = run_pipeline_once(config, "p", job).await.unwrap();
    assert_eq!(result.status, "success");
    // cat with stdin closed immediately should produce empty output
    assert_eq!(result.steps["run"].stdout.as_deref(), Some(""));
}

// ---------- step_with overrides ----------

#[tokio::test]
async fn step_with_cmd_overrides_task_cmd() {
    let config = r#"
version = 1
[bria]
[[bria.sources]]
id = "src"
type = "file"
path = "unused.jsonl"

[[bria.tasks]]
id = "flexible"
driver = "local"
cmd = "sh"
args = ["-c", "printf default"]

[bria.tasks.stdout]
mode = "capture"
max_bytes = 64

[[bria.pipelines]]
id = "p"
source = "src"

[[bria.pipelines.steps]]
id = "run"
type = "process"
task = "flexible"

[bria.pipelines.steps.with]
cmd = "echo"
args = ["-n", "overridden"]
"#;
    let result = run_pipeline_once(config, "p", empty_job("j", "src"))
        .await
        .unwrap();
    assert_eq!(result.status, "success");
    assert_eq!(result.steps["run"].stdout.as_deref(), Some("overridden"));
}

#[tokio::test]
async fn step_with_env_merges_and_overrides_task_env() {
    let config = r#"
version = 1
[bria]
[[bria.sources]]
id = "src"
type = "file"
path = "unused.jsonl"

[[bria.tasks]]
id = "env-test"
driver = "local"
cmd = "sh"
args = ["-c", "printf '%s %s' \"$A\" \"$B\""]

[bria.tasks.env]
A = "task-a"
B = "task-b"

[bria.tasks.stdout]
mode = "capture"
max_bytes = 64

[[bria.pipelines]]
id = "p"
source = "src"

[[bria.pipelines.steps]]
id = "run"
type = "process"
task = "env-test"

[bria.pipelines.steps.with.env]
A = "step-a"
"#;
    let result = run_pipeline_once(config, "p", empty_job("j", "src"))
        .await
        .unwrap();
    assert_eq!(result.status, "success");
    // Step A overrides task A; task B is inherited
    assert_eq!(result.steps["run"].stdout.as_deref(), Some("step-a task-b"));
}

#[tokio::test]
async fn step_with_success_exit_codes_overrides_task() {
    let config = r#"
[bria]
[[bria.sources]]
id = "src"
type = "file"
path = "unused.jsonl"

[[bria.tasks]]
id = "exit-3"
driver = "local"
cmd = "sh"
args = ["-c", "exit 3"]

[[bria.pipelines]]
id = "p"
source = "src"

[[bria.pipelines.steps]]
id = "run"
type = "process"
task = "exit-3"

[bria.pipelines.steps.with]
success_exit_codes = [0, 3]
"#;
    let result = run_pipeline_once(config, "p", empty_job("j", "src"))
        .await
        .unwrap();
    assert_eq!(result.status, "success");
    assert_eq!(result.steps["run"].exit_code, 3);
}

#[tokio::test]
async fn custom_success_exit_codes_on_task_level_accepts_non_zero() {
    let config = r#"
[bria]
[[bria.sources]]
id = "src"
type = "file"
path = "unused.jsonl"

[[bria.tasks]]
id = "exit-2"
driver = "local"
cmd = "sh"
args = ["-c", "exit 2"]
success_exit_codes = [0, 2]

[[bria.pipelines]]
id = "p"
source = "src"

[[bria.pipelines.steps]]
id = "run"
type = "process"
task = "exit-2"
"#;
    let result = run_pipeline_once(config, "p", empty_job("j", "src"))
        .await
        .unwrap();
    assert_eq!(result.status, "success");
    assert_eq!(result.steps["run"].exit_code, 2);
}

#[tokio::test]
async fn step_with_timeout_overrides_task_timeout() {
    let config = r#"
[bria]
[[bria.sources]]
id = "src"
type = "file"
path = "unused.jsonl"

[[bria.tasks]]
id = "slowish"
driver = "local"
cmd = "sh"
args = ["-c", "sleep 3"]
timeout_secs = 10

[bria.global.timeout]
step_secs = 30

[[bria.pipelines]]
id = "p"
source = "src"

[[bria.pipelines.steps]]
id = "run"
type = "process"
task = "slowish"

[bria.pipelines.steps.with]
timeout_secs = 1
timeout_action = "kill"
"#;
    let result = run_pipeline_once(config, "p", empty_job("j", "src"))
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

#[tokio::test]
async fn task_timeout_overrides_global_timeout() {
    let config = r#"
[bria]
[[bria.sources]]
id = "src"
type = "file"
path = "unused.jsonl"

[[bria.tasks]]
id = "slowish"
driver = "local"
cmd = "sh"
args = ["-c", "sleep 3"]
timeout_secs = 1

[bria.global.timeout]
step_secs = 30

[[bria.pipelines]]
id = "p"
source = "src"

[[bria.pipelines.steps]]
id = "run"
type = "process"
task = "slowish"
"#;
    let result = run_pipeline_once(config, "p", empty_job("j", "src"))
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

// ---------- stdout / stderr modes ----------

#[tokio::test]
async fn stderr_capture_mode_captures_stderr() {
    let config = r#"
[bria]
[[bria.sources]]
id = "src"
type = "file"
path = "unused.jsonl"

[[bria.tasks]]
id = "warn"
driver = "local"
cmd = "sh"
args = ["-c", "printf ok; printf err >&2"]

[bria.tasks.stdout]
mode = "capture"
max_bytes = 64

[bria.tasks.stderr]
mode = "capture"
max_bytes = 64

[[bria.pipelines]]
id = "p"
source = "src"

[[bria.pipelines.steps]]
id = "run"
type = "process"
task = "warn"
"#;
    let result = run_pipeline_once(config, "p", empty_job("j", "src"))
        .await
        .unwrap();
    assert_eq!(result.status, "success");
    assert_eq!(result.steps["run"].stdout.as_deref(), Some("ok"));
    assert_eq!(result.steps["run"].stderr.as_deref(), Some("err"));
}

#[tokio::test]
async fn stderr_discard_mode_does_not_capture_stderr() {
    let config = r#"
[bria]
[[bria.sources]]
id = "src"
type = "file"
path = "unused.jsonl"

[[bria.tasks]]
id = "loud"
driver = "local"
cmd = "sh"
args = ["-c", "printf info >&2; true"]

[bria.tasks.stderr]
mode = "discard"

[[bria.pipelines]]
id = "p"
source = "src"

[[bria.pipelines.steps]]
id = "run"
type = "process"
task = "loud"
"#;
    let result = run_pipeline_once(config, "p", empty_job("j", "src"))
        .await
        .unwrap();
    assert_eq!(result.status, "success");
    assert_eq!(result.steps["run"].stderr, None);
}

#[tokio::test]
async fn stdout_discard_mode_does_not_capture_stdout() {
    let config = r#"
[bria]
[[bria.sources]]
id = "src"
type = "file"
path = "unused.jsonl"

[[bria.tasks]]
id = "no-out"
driver = "local"
cmd = "sh"
args = ["-c", "printf secret"]

[bria.tasks.stdout]
mode = "discard"

[[bria.pipelines]]
id = "p"
source = "src"

[[bria.pipelines.steps]]
id = "run"
type = "process"
task = "no-out"
"#;
    let result = run_pipeline_once(config, "p", empty_job("j", "src"))
        .await
        .unwrap();
    assert_eq!(result.status, "success");
    assert_eq!(result.steps["run"].stdout, None);
}

// ---------- stdout / stderr overflow ----------

#[tokio::test]
async fn stdout_exceeds_max_bytes_fails_task() {
    let config = r#"
[bria]
[[bria.sources]]
id = "src"
type = "file"
path = "unused.jsonl"

[[bria.tasks]]
id = "verbose"
driver = "local"
cmd = "sh"
args = ["-c", "printf '%.0sx' $(seq 1 200)"]

[bria.tasks.stdout]
mode = "capture"
max_bytes = 10

[[bria.pipelines]]
id = "p"
source = "src"

[[bria.pipelines.steps]]
id = "run"
type = "process"
task = "verbose"
"#;
    let result = run_pipeline_once(config, "p", empty_job("j", "src"))
        .await
        .unwrap();
    assert_eq!(result.status, "failure");
    assert!(
        result.steps["run"]
            .stderr
            .as_deref()
            .unwrap_or("")
            .contains("max_bytes")
    );
}

#[tokio::test]
async fn stderr_exceeds_max_bytes_fails_task() {
    let config = r#"
[bria]
[[bria.sources]]
id = "src"
type = "file"
path = "unused.jsonl"

[[bria.tasks]]
id = "chatty"
driver = "local"
cmd = "sh"
args = ["-c", "printf '%.0sx' $(seq 1 200) >&2"]

[bria.tasks.stderr]
mode = "capture"
max_bytes = 10

[[bria.pipelines]]
id = "p"
source = "src"

[[bria.pipelines.steps]]
id = "run"
type = "process"
task = "chatty"
"#;
    let result = run_pipeline_once(config, "p", empty_job("j", "src"))
        .await
        .unwrap();
    assert_eq!(result.status, "failure");
    assert!(
        result.steps["run"]
            .stderr
            .as_deref()
            .unwrap_or("")
            .contains("max_bytes")
    );
}

// ---------- timeout kill / term ----------

#[tokio::test]
async fn timeout_kill_fails_task_immediately() {
    let config = r#"
[bria]
[[bria.sources]]
id = "src"
type = "file"
path = "unused.jsonl"

[[bria.tasks]]
id = "slow"
driver = "local"
cmd = "sh"
args = ["-c", "sleep 5"]
timeout_secs = 1
timeout_action = "kill"

[[bria.pipelines]]
id = "p"
source = "src"

[[bria.pipelines.steps]]
id = "run"
type = "process"
task = "slow"
"#;
    let result = run_pipeline_once(config, "p", empty_job("j", "src"))
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

#[tokio::test]
async fn timeout_term_then_force_kills_after_grace_expires() {
    let config = r#"
[bria]
[[bria.sources]]
id = "src"
type = "file"
path = "unused.jsonl"

[[bria.tasks]]
id = "stubborn"
driver = "local"
cmd = "sh"
args = ["-c", "trap '' TERM; sleep 5"]
timeout_secs = 1
timeout_action = "term"
kill_grace_secs = 1

[[bria.pipelines]]
id = "p"
source = "src"

[[bria.pipelines.steps]]
id = "run"
type = "process"
task = "stubborn"
"#;
    let result = run_pipeline_once(config, "p", empty_job("j", "src"))
        .await
        .unwrap();
    assert_eq!(result.status, "failure");
    let stderr = result.steps["run"].stderr.as_deref().unwrap_or("");
    assert!(stderr.contains("timed out"));
}

#[tokio::test]
async fn timeout_zero_means_unlimited() {
    let config = r#"
[bria]
[[bria.sources]]
id = "src"
type = "file"
path = "unused.jsonl"

[[bria.tasks]]
id = "fast"
driver = "local"
cmd = "true"
timeout_secs = 0

[bria.global.timeout]
step_secs = 0
action = "kill"

[[bria.pipelines]]
id = "p"
source = "src"

[[bria.pipelines.steps]]
id = "run"
type = "process"
task = "fast"
"#;
    let result = run_pipeline_once(config, "p", empty_job("j", "src"))
        .await
        .unwrap();
    assert_eq!(result.status, "success");
}

// ---------- output extraction ----------

#[tokio::test]
async fn json_output_with_nested_key_extracts_deep_value() {
    let config = r#"
[bria]
[[bria.sources]]
id = "src"
type = "file"
path = "unused.jsonl"

[[bria.tasks]]
id = "json-nested"
driver = "local"
cmd = "sh"
args = ["-c", "printf '{\"data\":{\"inner\":{\"value\":42}}}'"]

[bria.tasks.stdout]
mode = "capture"
max_bytes = 256

[[bria.pipelines]]
id = "p"
source = "src"

[[bria.pipelines.steps]]
id = "gen"
type = "process"
task = "json-nested"

[bria.pipelines.steps.outputs]
format = "json"

[[bria.pipelines.steps.outputs.fields]]
key = "data.inner.value"
name = "inner_value"
"#;
    let result = run_pipeline_once(config, "p", empty_job("j", "src"))
        .await
        .unwrap();
    assert_eq!(result.status, "success");
    assert_eq!(
        result.steps["gen"].outputs.get("inner_value"),
        Some(&serde_json::json!(42))
    );
}

#[tokio::test]
async fn json_output_missing_key_returns_null() {
    let config = r#"
[bria]
[[bria.sources]]
id = "src"
type = "file"
path = "unused.jsonl"

[[bria.tasks]]
id = "json-shallow"
driver = "local"
cmd = "sh"
args = ["-c", "printf '{\"a\":1}'"]

[bria.tasks.stdout]
mode = "capture"
max_bytes = 64

[[bria.pipelines]]
id = "p"
source = "src"

[[bria.pipelines.steps]]
id = "gen"
type = "process"
task = "json-shallow"

[bria.pipelines.steps.outputs]
format = "json"

[[bria.pipelines.steps.outputs.fields]]
key = "b.c"
name = "missing"
"#;
    let result = run_pipeline_once(config, "p", empty_job("j", "src"))
        .await
        .unwrap();
    assert_eq!(result.status, "success");
    assert_eq!(
        result.steps["gen"].outputs.get("missing"),
        Some(&serde_json::Value::Null)
    );
}

#[tokio::test]
async fn invalid_json_stdout_produces_empty_outputs() {
    let config = r#"
[bria]
[[bria.sources]]
id = "src"
type = "file"
path = "unused.jsonl"

[[bria.tasks]]
id = "not-json"
driver = "local"
cmd = "sh"
args = ["-c", "printf 'not-json'"]

[bria.tasks.stdout]
mode = "capture"
max_bytes = 64

[[bria.pipelines]]
id = "p"
source = "src"

[[bria.pipelines.steps]]
id = "gen"
type = "process"
task = "not-json"

[bria.pipelines.steps.outputs]
format = "json"

[[bria.pipelines.steps.outputs.fields]]
key = "any"
name = "any"
"#;
    let result = run_pipeline_once(config, "p", empty_job("j", "src"))
        .await
        .unwrap();
    assert_eq!(result.status, "success");
    // No outputs extracted when JSON parse fails
    assert!(result.steps["gen"].outputs.is_empty());
}

#[tokio::test]
async fn text_output_format_extracts_stdout_as_string() {
    let config = r#"
[bria]
[[bria.sources]]
id = "src"
type = "file"
path = "unused.jsonl"

[[bria.tasks]]
id = "txt-out"
driver = "local"
cmd = "sh"
args = ["-c", "printf 'plain-text-result'"]

[bria.tasks.stdout]
mode = "capture"
max_bytes = 64

[[bria.pipelines]]
id = "p"
source = "src"

[[bria.pipelines.steps]]
id = "gen"
type = "process"
task = "txt-out"

[bria.pipelines.steps.outputs]
format = "text"

[[bria.pipelines.steps.outputs.fields]]
name = "body"
"#;
    let result = run_pipeline_once(config, "p", empty_job("j", "src"))
        .await
        .unwrap();
    assert_eq!(result.status, "success");
    assert_eq!(
        result.steps["gen"].outputs.get("body"),
        Some(&serde_json::json!("plain-text-result"))
    );
}

#[tokio::test]
async fn json_output_with_empty_key_returns_whole_document() {
    let config = r#"
[bria]
[[bria.sources]]
id = "src"
type = "file"
path = "unused.jsonl"

[[bria.tasks]]
id = "json-full"
driver = "local"
cmd = "sh"
args = ["-c", "printf '{\"x\":1,\"y\":2}'"]

[bria.tasks.stdout]
mode = "capture"
max_bytes = 64

[[bria.pipelines]]
id = "p"
source = "src"

[[bria.pipelines.steps]]
id = "gen"
type = "process"
task = "json-full"

[bria.pipelines.steps.outputs]
format = "json"

[[bria.pipelines.steps.outputs.fields]]
key = ""
name = "doc"
"#;
    let result = run_pipeline_once(config, "p", empty_job("j", "src"))
        .await
        .unwrap();
    assert_eq!(result.status, "success");
    assert_eq!(
        result.steps["gen"].outputs.get("doc"),
        Some(&serde_json::json!({"x": 1, "y": 2}))
    );
}

#[tokio::test]
async fn no_stdout_produces_empty_outputs() {
    let config = r#"
[bria]
[[bria.sources]]
id = "src"
type = "file"
path = "unused.jsonl"

[[bria.tasks]]
id = "discard-all"
driver = "local"
cmd = "true"

[bria.tasks.stdout]
mode = "discard"

[[bria.pipelines]]
id = "p"
source = "src"

[[bria.pipelines.steps]]
id = "run"
type = "process"
task = "discard-all"

[bria.pipelines.steps.outputs]
format = "json"

[[bria.pipelines.steps.outputs.fields]]
key = "x"
name = "x"
"#;
    let result = run_pipeline_once(config, "p", empty_job("j", "src"))
        .await
        .unwrap();
    assert_eq!(result.status, "success");
    assert!(result.steps["run"].outputs.is_empty());
    assert_eq!(result.steps["run"].stdout, None);
}

// ---------- error cases ----------

#[tokio::test]
async fn spawn_bad_command_fails_pipeline() {
    let config = r#"
[bria]
[[bria.sources]]
id = "src"
type = "file"
path = "unused.jsonl"

[[bria.tasks]]
id = "ghost"
driver = "local"
cmd = "/nonexistent/path/binary_xyzzy"

[[bria.pipelines]]
id = "p"
source = "src"

[[bria.pipelines.steps]]
id = "run"
type = "process"
task = "ghost"
"#;
    let result = run_pipeline_once(config, "p", empty_job("j", "src"))
        .await
        .unwrap();
    assert_eq!(result.status, "failure");
    let stderr = result.steps["run"].stderr.as_deref().unwrap_or("");
    assert!(
        stderr.contains("Failed to spawn")
            || stderr.contains("No such file")
            || stderr.contains("not found")
    );
}

#[tokio::test]
async fn non_zero_exit_code_not_in_success_codes_fails_task() {
    let config = r#"
[bria]
[[bria.sources]]
id = "src"
type = "file"
path = "unused.jsonl"

[[bria.tasks]]
id = "failer"
driver = "local"
cmd = "sh"
args = ["-c", "exit 42"]
success_exit_codes = [0]

[[bria.pipelines]]
id = "p"
source = "src"

[[bria.pipelines.steps]]
id = "run"
type = "process"
task = "failer"
"#;
    let result = run_pipeline_once(config, "p", empty_job("j", "src"))
        .await
        .unwrap();
    assert_eq!(result.status, "failure");
    assert!(
        result.steps["run"]
            .stderr
            .as_deref()
            .unwrap_or("")
            .contains("not in success_exit_codes")
    );
}

// ---------- env inheritance ----------

#[tokio::test]
async fn inherit_env_true_passes_parent_environment() {
    // Set a unique env var and verify the child can see it
    unsafe {
        std::env::set_var("BRIA_TEST_INHERIT_ENV_VAR", "inherited-value");
    }
    let config = r#"
[bria]
[[bria.sources]]
id = "src"
type = "file"
path = "unused.jsonl"

[[bria.tasks]]
id = "check-env"
driver = "local"
cmd = "sh"
args = ["-c", "printf '%s' \"$BRIA_TEST_INHERIT_ENV_VAR\""]
inherit_env = true

[bria.tasks.stdout]
mode = "capture"
max_bytes = 64

[[bria.pipelines]]
id = "p"
source = "src"

[[bria.pipelines.steps]]
id = "run"
type = "process"
task = "check-env"
"#;
    let result = run_pipeline_once(config, "p", empty_job("j", "src"))
        .await
        .unwrap();
    assert_eq!(result.status, "success");
    assert_eq!(
        result.steps["run"].stdout.as_deref(),
        Some("inherited-value")
    );
    unsafe {
        std::env::remove_var("BRIA_TEST_INHERIT_ENV_VAR");
    }
}

#[tokio::test]
async fn inherit_env_false_isolates_child_environment() {
    // Set a unique env var but disable inheritance — child should NOT see it
    unsafe {
        std::env::set_var("BRIA_TEST_ISOLATED_VAR", "should-not-leak");
    }
    let config = r#"
[bria]
[[bria.sources]]
id = "src"
type = "file"
path = "unused.jsonl"

[[bria.tasks]]
id = "isolated"
driver = "local"
cmd = "sh"
args = ["-c", "printf '%s' \"$BRIA_TEST_ISOLATED_VAR\""]
inherit_env = false

[bria.tasks.stdout]
mode = "capture"
max_bytes = 64

[[bria.pipelines]]
id = "p"
source = "src"

[[bria.pipelines.steps]]
id = "run"
type = "process"
task = "isolated"
"#;
    let result = run_pipeline_once(config, "p", empty_job("j", "src"))
        .await
        .unwrap();
    assert_eq!(result.status, "success");
    // When env is cleared, the var should be empty
    assert_eq!(result.steps["run"].stdout.as_deref(), Some(""));
    unsafe {
        std::env::remove_var("BRIA_TEST_ISOLATED_VAR");
    }
}

// ---------- working_dir override ----------

#[tokio::test]
async fn step_with_working_dir_overrides_task() {
    let tmp = std::env::temp_dir();
    // Canonicalize to resolve any symlinks (e.g. /var -> /private/var on macOS)
    let canonical = tmp.canonicalize().unwrap_or_else(|_| tmp.clone());
    let canonical_str = canonical.to_string_lossy().to_string();
    let config = format!(
        r#"
[bria]
[[bria.sources]]
id = "src"
type = "file"
path = "unused.jsonl"

[[bria.tasks]]
id = "pwd-task"
driver = "local"
cmd = "sh"
args = ["-c", "printf '%s' \"$PWD\""]
working_dir = "/"

[bria.tasks.stdout]
mode = "capture"
max_bytes = 512

[[bria.pipelines]]
id = "p"
source = "src"

[[bria.pipelines.steps]]
id = "run"
type = "process"
task = "pwd-task"

[bria.pipelines.steps.with]
working_dir = "{0}"
"#,
        canonical_str
    );

    let result = run_pipeline_once(&config, "p", empty_job("j", "src"))
        .await
        .unwrap();
    assert_eq!(result.status, "success");
    assert_eq!(
        result.steps["run"].stdout.as_deref(),
        Some(canonical_str.as_str())
    );
}
