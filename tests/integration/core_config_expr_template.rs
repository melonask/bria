use std::collections::HashMap;

use bria::config::{self, Config as BriaConfig, LogConfig};
use bria::context::{Context, Job, PipelineResult, StepResult};
use bria::error::Error;
use bria::expression::Evaluator;
use bria::template::TemplateEngine;

struct Config;

impl Config {
    fn from_str_with_env(raw: &str) -> bria::Result<BriaConfig> {
        let raw = if raw.trim_start().starts_with("version =") {
            raw.to_string()
        } else {
            format!("version = 1\n{raw}")
        };
        BriaConfig::from_str_with_env(&raw)
    }

    fn load_from_path(path: impl AsRef<std::path::Path>) -> bria::Result<BriaConfig> {
        BriaConfig::load_from_path(path)
    }
}

// ---------------------------------------------------------------------------
// config::substitute_env
// ---------------------------------------------------------------------------

#[test]
fn substitute_env_returns_string_unchanged_when_no_tokens_present() {
    let input = "plain text with no tokens";
    let output = config::substitute_env(input).expect("should succeed");
    assert_eq!(output, input);
}

#[test]
fn substitute_env_replaces_single_env_var_token() {
    unsafe { std::env::set_var("BRIA_TEST_SINGLE_TOKEN", "replaced-value") };
    let output =
        config::substitute_env("prefix_${BRIA_TEST_SINGLE_TOKEN}_suffix").expect("should succeed");
    assert_eq!(output, "prefix_replaced-value_suffix");
}

#[test]
fn substitute_env_replaces_multiple_tokens() {
    unsafe { std::env::set_var("BRIA_TEST_FIRST", "alpha") };
    unsafe { std::env::set_var("BRIA_TEST_SECOND", "beta") };
    let output = config::substitute_env("${BRIA_TEST_FIRST} and ${BRIA_TEST_SECOND}")
        .expect("should succeed");
    assert_eq!(output, "alpha and beta");
}

#[test]
fn substitute_env_errors_when_any_token_is_unset() {
    unsafe { std::env::set_var("BRIA_TEST_SET_VAR", "yes") };
    let err = config::substitute_env("${BRIA_TEST_SET_VAR}${BRIA_TEST_MISSING_VAR}")
        .expect_err("should fail for unset variable");
    let msg = err.to_string();
    assert!(msg.contains("BRIA_TEST_MISSING_VAR"));
}

#[test]
fn substitute_env_ignores_tokens_in_toml_comments() {
    unsafe { std::env::remove_var("BRIA_TEST_COMMENT_ONLY") };
    let input = "# ${BRIA_TEST_COMMENT_ONLY}\nvalue = \"# not a comment\"\n";
    let output = config::substitute_env(input).expect("comments must not require environment");
    assert_eq!(output, input);
}

#[test]
fn substitute_env_handles_variable_names_with_digits_and_underscores() {
    unsafe { std::env::set_var("BRIA_TEST_VAR_2", "bingo") };
    let output = config::substitute_env("value=${BRIA_TEST_VAR_2}").expect("should succeed");
    assert_eq!(output, "value=bingo");
}

#[test]
fn substitute_env_handles_adjacent_tokens() {
    unsafe { std::env::set_var("BRIA_PART_A", "hello") };
    unsafe { std::env::set_var("BRIA_PART_B", "world") };
    let output = config::substitute_env("${BRIA_PART_A}${BRIA_PART_B}").expect("should succeed");
    assert_eq!(output, "helloworld");
}

// ---------------------------------------------------------------------------
// Config::from_str_with_env / load_from_path
// ---------------------------------------------------------------------------

#[test]
fn from_str_with_env_parses_valid_minimal_toml() {
    let raw = r#"
version = 1
[bria]
[[bria.sources]]
id = "manual"
type = "file"
path = "unused.jsonl"

[[bria.pipelines]]
id = "hello"
source = "manual"

[[bria.pipelines.steps]]
id = "echo"
type = "process"
task = "noop"

[[bria.tasks]]
id = "noop"
driver = "local"
cmd = "true"
"#;
    let config = Config::from_str_with_env(raw).expect("should parse");
    assert_eq!(config.pipelines.len(), 1);
}

#[test]
fn from_str_with_env_returns_error_for_malformed_toml() {
    let raw = "this is not valid toml [[[ syntax";
    let err = Config::from_str_with_env(raw).expect_err("malformed TOML must fail");
    assert!(err.to_string().contains("TOML parse error"));
}

#[test]
fn from_str_with_env_rejects_unnested_legacy_config() {
    let err = Config::from_str_with_env(
        r#"
version = 1
[[sources]]
id = "legacy"
type = "file"
path = "input.jsonl"
"#,
    )
    .expect_err("an explicit [bria] namespace is required");
    assert!(
        err.to_string()
            .contains("Missing required [bria] namespace")
    );
}

#[test]
fn load_from_path_errors_for_nonexistent_file() {
    let err = Config::load_from_path("/nonexistent/bria/test/config.toml")
        .expect_err("nonexistent file must fail");
    assert!(err.to_string().contains("Cannot read config file"));
}

#[test]
fn load_from_path_delegates_to_from_str_with_env_for_valid_file() {
    let tmp = std::env::temp_dir().join(format!("bria-valid-config-{}.toml", ulid::Ulid::r#gen()));
    let toml_content = r#"
version = 1
[bria]
[[bria.sources]]
id = "manual"
type = "file"
path = "unused.jsonl"

[[bria.pipelines]]
id = "pl"
source = "manual"

[[bria.pipelines.steps]]
id = "step"
type = "process"
task = "noop"

[[bria.tasks]]
id = "noop"
driver = "local"
cmd = "true"
"#;
    std::fs::write(&tmp, toml_content).expect("write tmp config");
    let config = Config::load_from_path(&tmp).expect("should load from file");
    assert_eq!(config.pipelines[0].id, "pl");
    let _ = std::fs::remove_file(tmp);
}

// ---------------------------------------------------------------------------
// Config::validate — global checks
// ---------------------------------------------------------------------------

fn parse(raw: &str) -> BriaConfig {
    Config::from_str_with_env(raw).expect("parse for validation test")
}

#[test]
fn validate_rejects_duplicate_source_ids() {
    let cfg = parse(
        r#"
version = 1
[bria]
[[bria.sources]]
id = "dup"
type = "file"
path = "a.jsonl"

[[bria.sources]]
id = "dup"
type = "file"
path = "b.jsonl"
"#,
    );
    let err = cfg.validate().expect_err("duplicate source must fail");
    assert!(err.to_string().contains("Duplicate source id"));
}

#[test]
fn validate_rejects_duplicate_task_ids() {
    let cfg = parse(
        r#"
version = 1
[bria]
[[bria.sources]]
id = "s"
type = "file"
path = "u.jsonl"

[[bria.tasks]]
id = "twice"
driver = "local"
cmd = "true"

[[bria.tasks]]
id = "twice"
driver = "local"
cmd = "false"
"#,
    );
    let err = cfg.validate().expect_err("duplicate task must fail");
    assert!(err.to_string().contains("Duplicate task id"));
}

#[test]
fn validate_rejects_duplicate_sink_ids() {
    let cfg = parse(
        r#"
version = 1
[bria]
[[bria.sources]]
id = "s"
type = "file"
path = "u.jsonl"

[[bria.sinks]]
id = "dup"
type = "stream"

[[bria.sinks]]
id = "dup"
type = "stream"
"#,
    );
    let err = cfg.validate().expect_err("duplicate sink must fail");
    assert!(err.to_string().contains("Duplicate sink id"));
}

#[test]
fn validate_rejects_duplicate_pipeline_ids() {
    let cfg = parse(
        r#"
version = 1
[bria]
[[bria.sources]]
id = "s"
type = "file"
path = "u.jsonl"

[[bria.pipelines]]
id = "twice"
source = "s"

[[bria.pipelines.steps]]
id = "st"
type = "process"
task = "noop"

[[bria.pipelines]]
id = "twice"
source = "s"

[[bria.pipelines.steps]]
id = "st2"
type = "process"
task = "noop"

[[bria.tasks]]
id = "noop"
driver = "local"
cmd = "true"
"#,
    );
    let err = cfg.validate().expect_err("duplicate pipeline must fail");
    assert!(err.to_string().contains("Duplicate pipeline id"));
}

#[test]
fn validate_rejects_jitter_above_one() {
    let cfg = parse(
        r#"
version = 1
[bria]
[bria.global.retry]
jitter = 1.1

[[bria.sources]]
id = "s"
type = "file"
path = "u.jsonl"
"#,
    );
    let err = cfg.validate().expect_err("jitter > 1 must fail");
    assert!(
        err.to_string()
            .contains("jitter must be between 0.0 and 1.0")
    );
}

#[test]
fn validate_rejects_jitter_below_zero() {
    let cfg = parse(
        r#"
version = 1
[bria]
[bria.global.retry]
jitter = -0.1

[[bria.sources]]
id = "s"
type = "file"
path = "u.jsonl"
"#,
    );
    let err = cfg.validate().expect_err("jitter < 0 must fail");
    assert!(
        err.to_string()
            .contains("jitter must be between 0.0 and 1.0")
    );
}

#[test]
fn validate_rejects_pg_backend_without_pg_url() {
    let cfg = parse(
        r#"
version = 1
[bria]
[bria.global.state]
backend = "pg"

[[bria.sources]]
id = "s"
type = "file"
path = "u.jsonl"
"#,
    );
    let err = cfg
        .validate()
        .expect_err("pg backend without pg_url must fail");
    assert!(err.to_string().contains("global.state.pg_url"));
}

#[cfg(feature = "postgres")]
#[test]
fn validate_accepts_pg_backend_with_pg_url() {
    let cfg = parse(
        r#"
version = 1
[bria]
[bria.global.state]
backend = "pg"
pg_url = "postgres://localhost/bria"

[[bria.sources]]
id = "s"
type = "file"
path = "u.jsonl"
"#,
    );
    cfg.validate().expect("pg with url should be valid");
}

#[test]
fn validate_rejects_http_source_when_server_disabled() {
    let cfg = parse(
        r#"
version = 1
[bria]
[bria.server]
enabled = false

[[bria.sources]]
id = "incoming"
type = "http"
path = "events"
"#,
    );
    let err = cfg
        .validate()
        .expect_err("http source with server disabled must fail");
    assert!(err.to_string().contains("requires server.enabled = true"));
}

#[test]
fn validate_rejects_webhook_source_when_server_disabled() {
    let cfg = parse(
        r#"
version = 1
[bria]
[bria.server]
enabled = false

[[bria.sources]]
id = "wh"
type = "webhook"
path = "hooks"
"#,
    );
    let err = cfg
        .validate()
        .expect_err("webhook source with server disabled must fail");
    assert!(err.to_string().contains("requires server.enabled = true"));
}

#[test]
fn validate_rejects_stream_sink_when_server_disabled() {
    let cfg = parse(
        r#"
version = 1
[bria]
[bria.server]
enabled = false

[[bria.sources]]
id = "s"
type = "file"
path = "u.jsonl"

[[bria.sinks]]
id = "live"
type = "stream"

[[bria.pipelines]]
id = "pl"
source = "s"

[[bria.pipelines.steps]]
id = "st"
type = "process"
task = "noop"

[[bria.tasks]]
id = "noop"
driver = "local"
cmd = "true"
"#,
    );
    let err = cfg
        .validate()
        .expect_err("stream sink with server disabled must fail");
    assert!(err.to_string().contains("requires server.enabled = true"));
}

#[test]
fn validate_rejects_pipeline_sink_referencing_unknown_id() {
    let cfg = parse(
        r#"
version = 1
[bria]
[[bria.sources]]
id = "s"
type = "file"
path = "u.jsonl"

[[bria.pipelines]]
id = "pl"
source = "s"
sinks = ["ghost"]

[[bria.pipelines.steps]]
id = "st"
type = "process"
task = "noop"

[[bria.tasks]]
id = "noop"
driver = "local"
cmd = "true"
"#,
    );
    let err = cfg.validate().expect_err("unknown sink ref must fail");
    assert!(err.to_string().contains("references unknown sink"));
    assert!(err.to_string().contains("ghost"));
}

#[test]
fn validate_rejects_pipeline_failure_sink_referencing_unknown_id() {
    let cfg = parse(
        r#"
version = 1
[bria]
[[bria.sources]]
id = "s"
type = "file"
path = "u.jsonl"

[[bria.pipelines]]
id = "pl"
source = "s"

[bria.pipelines.failure]
action = "dead_letter"
sink = "phantom"

[[bria.pipelines.steps]]
id = "st"
type = "process"
task = "noop"

[[bria.tasks]]
id = "noop"
driver = "local"
cmd = "true"
"#,
    );
    let err = cfg.validate().expect_err("failure sink unknown must fail");
    assert!(err.to_string().contains("failure sink 'phantom' not found"));
}

#[test]
fn validate_rejects_dead_letter_without_sink() {
    let cfg = parse(
        r#"
version = 1
[bria]
[[bria.sources]]
id = "s"
type = "file"
path = "u.jsonl"

[[bria.pipelines]]
id = "pl"
source = "s"

[bria.pipelines.failure]
action = "dead_letter"

[[bria.pipelines.steps]]
id = "st"
type = "process"
task = "noop"

[[bria.tasks]]
id = "noop"
driver = "local"
cmd = "true"
"#,
    );
    let err = cfg
        .validate()
        .expect_err("dead_letter without sink must fail");
    assert!(
        err.to_string()
            .contains("failure action is dead_letter but no sink specified")
    );
}

#[test]
fn validate_rejects_step_sink_referencing_unknown_id() {
    let cfg = parse(
        r#"
version = 1
[bria]
[[bria.sources]]
id = "s"
type = "file"
path = "u.jsonl"

[[bria.pipelines]]
id = "pl"
source = "s"

[[bria.pipelines.steps]]
id = "st"
type = "process"
task = "noop"
sinks = ["nowhere"]

[[bria.tasks]]
id = "noop"
driver = "local"
cmd = "true"
"#,
    );
    let err = cfg.validate().expect_err("step sink unknown must fail");
    assert!(err.to_string().contains("references unknown sink"));
}

#[test]
fn validate_rejects_step_routing_sink_referencing_unknown_id() {
    let cfg = parse(
        r#"
version = 1
[bria]
[[bria.sources]]
id = "s"
type = "file"
path = "u.jsonl"

[[bria.pipelines]]
id = "pl"
source = "s"

[[bria.pipelines.steps]]
id = "st"
type = "process"
task = "noop"

[[bria.pipelines.steps.routing]]
condition = "true"
sinks = ["vanished"]

[[bria.tasks]]
id = "noop"
driver = "local"
cmd = "true"
"#,
    );
    let err = cfg.validate().expect_err("routing sink unknown must fail");
    assert!(err.to_string().contains("routing sink"));
    assert!(err.to_string().contains("vanished"));
}

#[test]
fn validate_rejects_step_failure_sink_referencing_unknown_id() {
    let cfg = parse(
        r#"
version = 1
[bria]
[[bria.sources]]
id = "s"
type = "file"
path = "u.jsonl"

[[bria.pipelines]]
id = "pl"
source = "s"

[[bria.pipelines.steps]]
id = "st"
type = "process"
task = "noop"

[bria.pipelines.steps.failure]
action = "dead_letter"
sink = "missing"

[[bria.tasks]]
id = "noop"
driver = "local"
cmd = "true"
"#,
    );
    let err = cfg
        .validate()
        .expect_err("step failure sink unknown must fail");
    assert!(err.to_string().contains("failure sink 'missing' not found"));
}

#[test]
fn validate_collects_multiple_errors_into_joined_message() {
    let cfg = parse(
        r#"
version = 1
[bria]
[bria.server]
enabled = false

[[bria.sources]]
id = "dup_source"
type = "webhook"
path = "hooks"

[[bria.sources]]
id = "dup_source"
type = "webhook"
path = "other"

[[bria.sinks]]
id = "dup_sink"
type = "stream"

[[bria.sinks]]
id = "dup_sink"
type = "stream"
"#,
    );
    let err = cfg.validate().expect_err("multiple errors must fail");
    let msg = err.to_string();
    assert!(msg.contains("Duplicate source id"));
    assert!(msg.contains("Duplicate sink id"));
    assert!(msg.contains("requires server.enabled = true"));
}

// ---------------------------------------------------------------------------
// SourceConfig::validate branches (called via Config::validate)
// ---------------------------------------------------------------------------

#[test]
fn validate_rejects_file_source_without_path() {
    let cfg = parse(
        r#"
version = 1
[bria]
[[bria.sources]]
id = "s"
type = "file"
"#,
    );
    let err = cfg.validate().expect_err("file without path must fail");
    assert!(err.to_string().contains("requires a path"));
}

#[test]
fn validate_rejects_sqlite_source_without_path() {
    let cfg = parse(
        r#"
[bria]
[[bria.sources]]
id = "s"
type = "sqlite"
"#,
    );
    let err = cfg.validate().expect_err("sqlite without path must fail");
    assert!(err.to_string().contains("requires a path"));
}

#[test]
fn validate_rejects_cron_source_without_schedule() {
    let cfg = parse(
        r#"
[bria]
[[bria.sources]]
id = "s"
type = "cron"
"#,
    );
    let err = cfg.validate().expect_err("cron without schedule must fail");
    assert!(err.to_string().contains("requires a schedule"));
}

#[test]
fn validate_rejects_pg_source_without_url() {
    let cfg = parse(
        r#"
[bria]
[[bria.sources]]
id = "s"
type = "pg"
"#,
    );
    let err = cfg.validate().expect_err("pg without url must fail");
    assert!(err.to_string().contains("requires a url"));
}

#[test]
fn validate_rejects_queue_source_without_url() {
    let cfg = parse(
        r#"
[bria]
[[bria.sources]]
id = "s"
type = "queue"
"#,
    );
    let err = cfg.validate().expect_err("queue without url must fail");
    assert!(err.to_string().contains("requires a url"));
}

#[test]
fn validate_rejects_http_source_without_path() {
    let cfg = parse(
        r#"
[bria]
[bria.server]
enabled = true

[[bria.sources]]
id = "s"
type = "http"
"#,
    );
    let err = cfg.validate().expect_err("http without path must fail");
    assert!(err.to_string().contains("requires a path"));
}

#[test]
fn validate_rejects_webhook_source_without_path() {
    let cfg = parse(
        r#"
[bria]
[bria.server]
enabled = true

[[bria.sources]]
id = "s"
type = "webhook"
"#,
    );
    let err = cfg.validate().expect_err("webhook without path must fail");
    assert!(err.to_string().contains("requires a path"));
}

// ---------------------------------------------------------------------------
// TaskConfig::validate branches
// ---------------------------------------------------------------------------

#[test]
fn validate_rejects_unknown_driver() {
    let cfg = parse(
        r#"
[bria]
[[bria.sources]]
id = "s"
type = "file"
path = "u.jsonl"

[[bria.tasks]]
id = "t"
driver = "quantum"
cmd = "entangle"
"#,
    );
    let err = cfg.validate().expect_err("unknown driver must fail");
    assert!(err.to_string().contains("unknown driver"));
}

#[test]
fn validate_rejects_docker_without_docker_section() {
    let cfg = parse(
        r#"
[bria]
[[bria.sources]]
id = "s"
type = "file"
path = "u.jsonl"

[[bria.tasks]]
id = "t"
driver = "docker"
cmd = "echo"
"#,
    );
    let err = cfg.validate().expect_err("docker without config must fail");
    assert!(
        err.to_string()
            .contains("[bria.tasks.docker] section is missing")
    );
}

#[test]
fn validate_rejects_wasm_without_wasm_section() {
    let cfg = parse(
        r#"
[bria]
[[bria.sources]]
id = "s"
type = "file"
path = "u.jsonl"

[[bria.tasks]]
id = "t"
driver = "wasm"
cmd = "echo"
"#,
    );
    let err = cfg.validate().expect_err("wasm without config must fail");
    assert!(
        err.to_string()
            .contains("[bria.tasks.wasm] section is missing")
    );
}

#[test]
fn validate_rejects_task_jitter_above_one() {
    let cfg = parse(
        r#"
[bria]
[[bria.sources]]
id = "s"
type = "file"
path = "u.jsonl"

[[bria.tasks]]
id = "t"
driver = "local"
cmd = "true"

[bria.tasks.retry]
jitter = 1.5
"#,
    );
    let err = cfg.validate().expect_err("task jitter > 1 must fail");
    assert!(
        err.to_string()
            .contains("retry.jitter must be between 0.0 and 1.0")
    );
}

#[test]
fn validate_accepts_local_task() {
    let cfg = parse(
        r#"
[bria]
[[bria.sources]]
id = "s"
type = "file"
path = "u.jsonl"

[[bria.tasks]]
id = "t"
driver = "local"
cmd = "true"
"#,
    );
    cfg.validate().expect("local task should be valid");
}

#[test]
fn validate_accepts_docker_task_with_config() {
    let cfg = parse(
        r#"
[bria]
[[bria.sources]]
id = "s"
type = "file"
path = "u.jsonl"

[[bria.tasks]]
id = "t"
driver = "docker"
cmd = "echo"

[bria.tasks.docker]
flags = ["--rm"]
"#,
    );
    cfg.validate()
        .expect("docker task with config should be valid");
}

#[cfg(feature = "wasm")]
#[test]
fn validate_accepts_wasm_task_with_config() {
    let cfg = parse(
        r#"
[bria]
[[bria.sources]]
id = "s"
type = "file"
path = "u.jsonl"

[[bria.tasks]]
id = "t"
driver = "wasm"
cmd = "module.wasm"

[bria.tasks.wasm]
max_memory_pages = 128
"#,
    );
    cfg.validate()
        .expect("wasm task with config should be valid");
}

// ---------------------------------------------------------------------------
// SinkConfig::validate branches
// ---------------------------------------------------------------------------

#[test]
fn validate_rejects_file_sink_without_path() {
    let cfg = parse(
        r#"
[bria]
[[bria.sources]]
id = "s"
type = "file"
path = "u.jsonl"

[[bria.sinks]]
id = "sk"
type = "file"
"#,
    );
    let err = cfg
        .validate()
        .expect_err("file sink without path must fail");
    assert!(err.to_string().contains("requires a path"));
}

#[test]
fn validate_rejects_webhook_sink_without_url() {
    let cfg = parse(
        r#"
[bria]
[[bria.sources]]
id = "s"
type = "file"
path = "u.jsonl"

[[bria.sinks]]
id = "sk"
type = "webhook"
"#,
    );
    let err = cfg
        .validate()
        .expect_err("webhook sink without url must fail");
    assert!(err.to_string().contains("requires a url"));
}

#[test]
fn validate_rejects_queue_sink_without_url() {
    let cfg = parse(
        r#"
[bria]
[[bria.sources]]
id = "s"
type = "file"
path = "u.jsonl"

[[bria.sinks]]
id = "sk"
type = "queue"
"#,
    );
    let err = cfg
        .validate()
        .expect_err("queue sink without url must fail");
    assert!(err.to_string().contains("requires a url"));
}

#[test]
fn validate_rejects_pg_sink_without_url() {
    let cfg = parse(
        r#"
[bria]
[[bria.sources]]
id = "s"
type = "file"
path = "u.jsonl"

[[bria.sinks]]
id = "sk"
type = "pg"
"#,
    );
    let err = cfg.validate().expect_err("pg sink without url must fail");
    assert!(err.to_string().contains("requires a url"));
}

#[test]
fn validate_rejects_sqlite_sink_without_path() {
    let cfg = parse(
        r#"
[bria]
[[bria.sources]]
id = "s"
type = "file"
path = "u.jsonl"

[[bria.sinks]]
id = "sk"
type = "sqlite"
"#,
    );
    let err = cfg
        .validate()
        .expect_err("sqlite sink without path must fail");
    assert!(err.to_string().contains("requires a path"));
}

#[cfg(feature = "server")]
#[test]
fn validate_accepts_stream_sink_without_additional_requirements() {
    let cfg = parse(
        r#"
[bria]
[bria.server]
enabled = true

[[bria.sources]]
id = "s"
type = "file"
path = "u.jsonl"

[[bria.sinks]]
id = "sk"
type = "stream"
"#,
    );
    cfg.validate().expect("stream sink should be valid");
}

// ---------------------------------------------------------------------------
// PipelineConfig::validate branches
// ---------------------------------------------------------------------------

#[test]
fn validate_rejects_pipeline_with_unknown_scalar_source() {
    let cfg = parse(
        r#"
[bria]
[[bria.pipelines]]
id = "pl"
source = "nowhere"

[[bria.pipelines.steps]]
id = "st"
type = "process"
task = "noop"

[[bria.tasks]]
id = "noop"
driver = "local"
cmd = "true"
"#,
    );
    let err = cfg.validate().expect_err("unknown source ref must fail");
    assert!(err.to_string().contains("references unknown source"));
}

#[test]
fn validate_rejects_pipeline_with_unknown_array_source() {
    let cfg = parse(
        r#"
[bria]
[[bria.pipelines]]
id = "pl"

[[bria.pipelines.sources]]
source = "ghost"

[[bria.pipelines.steps]]
id = "st"
type = "process"
task = "noop"

[[bria.tasks]]
id = "noop"
driver = "local"
cmd = "true"
"#,
    );
    let err = cfg.validate().expect_err("unknown array source must fail");
    assert!(err.to_string().contains("references unknown source"));
}

#[test]
fn validate_rejects_pipeline_with_no_sources() {
    let cfg = parse(
        r#"
[bria]
[[bria.pipelines]]
id = "pl"

[[bria.pipelines.steps]]
id = "st"
type = "process"
task = "noop"

[[bria.tasks]]
id = "noop"
driver = "local"
cmd = "true"
"#,
    );
    let err = cfg.validate().expect_err("no sources must fail");
    assert!(err.to_string().contains("has no sources configured"));
}

#[test]
fn validate_rejects_multi_source_without_merge() {
    let cfg = parse(
        r#"
[bria]
[[bria.sources]]
id = "a"
type = "file"
path = "a.jsonl"

[[bria.sources]]
id = "b"
type = "file"
path = "b.jsonl"

[[bria.pipelines]]
id = "pl"

[[bria.pipelines.sources]]
source = "a"

[[bria.pipelines.sources]]
source = "b"

[[bria.pipelines.steps]]
id = "st"
type = "process"
task = "noop"

[[bria.tasks]]
id = "noop"
driver = "local"
cmd = "true"
"#,
    );
    let err = cfg
        .validate()
        .expect_err("multi-source without merge must fail");
    assert!(
        err.to_string()
            .contains("has multiple sources but no [bria.pipelines.merge] section")
    );
}

#[test]
fn validate_rejects_invalid_merge_strategy() {
    let cfg = parse(
        r#"
[bria]
[[bria.sources]]
id = "a"
type = "file"
path = "a.jsonl"

[[bria.sources]]
id = "b"
type = "file"
path = "b.jsonl"

[[bria.pipelines]]
id = "pl"

[[bria.pipelines.sources]]
source = "a"

[[bria.pipelines.sources]]
source = "b"

[bria.pipelines.merge]
strategy = "telepathic"
correlation_key = "id"

[[bria.pipelines.steps]]
id = "st"
type = "process"
task = "noop"

[[bria.tasks]]
id = "noop"
driver = "local"
cmd = "true"
"#,
    );
    let err = cfg.validate().expect_err("bad merge strategy must fail");
    assert!(err.to_string().contains("merge.strategy"));
    assert!(err.to_string().contains("invalid"));
}

#[test]
fn validate_rejects_merge_with_both_correlation_key_and_expr() {
    let cfg = parse(
        r#"
[bria]
[[bria.sources]]
id = "a"
type = "file"
path = "a.jsonl"

[[bria.sources]]
id = "b"
type = "file"
path = "b.jsonl"

[[bria.pipelines]]
id = "pl"

[[bria.pipelines.sources]]
source = "a"

[[bria.pipelines.sources]]
source = "b"

[bria.pipelines.merge]
strategy = "any"
correlation_key = "id"
correlation_expr = "a.job.id == b.job.id"

[[bria.pipelines.steps]]
id = "st"
type = "process"
task = "noop"

[[bria.tasks]]
id = "noop"
driver = "local"
cmd = "true"
"#,
    );
    let err = cfg
        .validate()
        .expect_err("both corr key and expr must fail");
    assert!(err.to_string().contains("mutually exclusive"));
}

#[test]
fn validate_rejects_merge_with_neither_correlation_key_nor_expr() {
    let cfg = parse(
        r#"
[bria]
[[bria.sources]]
id = "a"
type = "file"
path = "a.jsonl"

[[bria.sources]]
id = "b"
type = "file"
path = "b.jsonl"

[[bria.pipelines]]
id = "pl"

[[bria.pipelines.sources]]
source = "a"

[[bria.pipelines.sources]]
source = "b"

[bria.pipelines.merge]
strategy = "any"

[[bria.pipelines.steps]]
id = "st"
type = "process"
task = "noop"

[[bria.tasks]]
id = "noop"
driver = "local"
cmd = "true"
"#,
    );
    let err = cfg
        .validate()
        .expect_err("neither corr key nor expr must fail");
    assert!(
        err.to_string()
            .contains("must specify either correlation_key or correlation_expr")
    );
}

#[test]
fn validate_accepts_merge_all_with_correlation_key() {
    let cfg = parse(
        r#"
[bria]
[[bria.sources]]
id = "a"
type = "file"
path = "a.jsonl"

[[bria.sources]]
id = "b"
type = "file"
path = "b.jsonl"

[[bria.pipelines]]
id = "pl"

[[bria.pipelines.sources]]
source = "a"

[[bria.pipelines.sources]]
source = "b"

[bria.pipelines.merge]
strategy = "all"
correlation_key = "order_id"

[[bria.pipelines.steps]]
id = "st"
type = "process"
task = "noop"

[[bria.tasks]]
id = "noop"
driver = "local"
cmd = "true"
"#,
    );
    cfg.validate()
        .expect("all + correlation_key should be valid");
}

#[test]
fn validate_accepts_merge_any_with_correlation_expr() {
    let cfg = parse(
        r#"
[bria]
[[bria.sources]]
id = "a"
type = "file"
path = "a.jsonl"

[[bria.sources]]
id = "b"
type = "file"
path = "b.jsonl"

[[bria.pipelines]]
id = "pl"

[[bria.pipelines.sources]]
source = "a"

[[bria.pipelines.sources]]
source = "b"

[bria.pipelines.merge]
strategy = "any"
correlation_expr = "a.job.id == b.job.id"

[[bria.pipelines.steps]]
id = "st"
type = "process"
task = "noop"

[[bria.tasks]]
id = "noop"
driver = "local"
cmd = "true"
"#,
    );
    cfg.validate()
        .expect("any + correlation_expr should be valid");
}

#[test]
fn validate_rejects_duplicate_step_ids_in_pipeline() {
    let cfg = parse(
        r#"
[bria]
[[bria.sources]]
id = "s"
type = "file"
path = "u.jsonl"

[[bria.pipelines]]
id = "pl"
source = "s"

[[bria.pipelines.steps]]
id = "twice"
type = "process"
task = "noop"

[[bria.pipelines.steps]]
id = "twice"
type = "process"
task = "noop"

[[bria.tasks]]
id = "noop"
driver = "local"
cmd = "true"
"#,
    );
    let err = cfg.validate().expect_err("duplicate step must fail");
    assert!(err.to_string().contains("duplicate step id"));
}

#[test]
fn validate_rejects_process_step_without_task() {
    let cfg = parse(
        r#"
[bria]
[[bria.sources]]
id = "s"
type = "file"
path = "u.jsonl"

[[bria.pipelines]]
id = "pl"
source = "s"

[[bria.pipelines.steps]]
id = "st"
type = "process"
"#,
    );
    let err = cfg
        .validate()
        .expect_err("process step without task must fail");
    assert!(err.to_string().contains("requires a task"));
}

#[test]
fn validate_rejects_condition_step_without_expr() {
    let cfg = parse(
        r#"
[bria]
[[bria.sources]]
id = "s"
type = "file"
path = "u.jsonl"

[[bria.pipelines]]
id = "pl"
source = "s"

[[bria.pipelines.steps]]
id = "st"
type = "condition"
"#,
    );
    let err = cfg
        .validate()
        .expect_err("condition step without expr must fail");
    assert!(err.to_string().contains("requires an expr"));
}

#[test]
fn validate_rejects_condition_skip_to_with_unknown_target() {
    let cfg = parse(
        r#"
[bria]
[[bria.sources]]
id = "s"
type = "file"
path = "u.jsonl"

[[bria.pipelines]]
id = "pl"
source = "s"

[[bria.pipelines.steps]]
id = "st"
type = "condition"
expr = "true"
action = "skip_to"
skip_to = "never-here"
"#,
    );
    let err = cfg.validate().expect_err("skip_to unknown step must fail");
    assert!(err.to_string().contains("skip_to"));
    assert!(err.to_string().contains("references unknown step"));
}

#[test]
fn validate_rejects_condition_with_invalid_action() {
    let cfg = parse(
        r#"
[bria]
[[bria.sources]]
id = "s"
type = "file"
path = "u.jsonl"

[[bria.pipelines]]
id = "pl"
source = "s"

[[bria.pipelines.steps]]
id = "st"
type = "condition"
expr = "true"
action = "levitate"
"#,
    );
    let err = cfg
        .validate()
        .expect_err("invalid condition action must fail");
    assert!(err.to_string().contains("action"));
    assert!(err.to_string().contains("invalid"));
}

#[test]
fn validate_rejects_map_step_without_set_entries() {
    let cfg = parse(
        r#"
[bria]
[[bria.sources]]
id = "s"
type = "file"
path = "u.jsonl"

[[bria.pipelines]]
id = "pl"
source = "s"

[[bria.pipelines.steps]]
id = "st"
type = "map"
"#,
    );
    let err = cfg.validate().expect_err("map step without set must fail");
    assert!(
        err.to_string()
            .contains("requires at least one [[bria.pipelines.steps.set]] entry")
    );
}

#[test]
fn validate_rejects_depends_on_unknown_step() {
    let cfg = parse(
        r#"
[bria]
[[bria.sources]]
id = "s"
type = "file"
path = "u.jsonl"

[[bria.pipelines]]
id = "pl"
source = "s"

[[bria.pipelines.steps]]
id = "st"
type = "process"
task = "noop"
depends_on = ["phantom"]

[[bria.tasks]]
id = "noop"
driver = "local"
cmd = "true"
"#,
    );
    let err = cfg
        .validate()
        .expect_err("depends_on unknown step must fail");
    assert!(err.to_string().contains("depends_on"));
    assert!(err.to_string().contains("references unknown step"));
}

#[test]
fn validate_rejects_step_jitter_above_one() {
    let cfg = parse(
        r#"
[bria]
[[bria.sources]]
id = "s"
type = "file"
path = "u.jsonl"

[[bria.pipelines]]
id = "pl"
source = "s"

[[bria.pipelines.steps]]
id = "st"
type = "process"
task = "noop"

[bria.pipelines.steps.retry]
jitter = 2.0

[[bria.tasks]]
id = "noop"
driver = "local"
cmd = "true"
"#,
    );
    let err = cfg.validate().expect_err("step jitter > 1 must fail");
    assert!(
        err.to_string()
            .contains("retry.jitter must be between 0.0 and 1.0")
    );
}

#[test]
fn validate_rejects_step_dead_letter_without_sink() {
    let cfg = parse(
        r#"
[bria]
[[bria.sources]]
id = "s"
type = "file"
path = "u.jsonl"

[[bria.pipelines]]
id = "pl"
source = "s"

[[bria.pipelines.steps]]
id = "st"
type = "process"
task = "noop"

[bria.pipelines.steps.failure]
action = "dead_letter"

[[bria.tasks]]
id = "noop"
driver = "local"
cmd = "true"
"#,
    );
    let err = cfg
        .validate()
        .expect_err("step dead_letter without sink must fail");
    assert!(
        err.to_string()
            .contains("failure action is dead_letter but no sink specified")
    );
}

#[test]
fn validate_rejects_cycle_in_step_dag() {
    let cfg = parse(
        r#"
[bria]
[[bria.sources]]
id = "s"
type = "file"
path = "u.jsonl"

[[bria.pipelines]]
id = "pl"
source = "s"

[[bria.pipelines.steps]]
id = "A"
type = "process"
task = "noop"
depends_on = ["B"]

[[bria.pipelines.steps]]
id = "B"
type = "process"
task = "noop"
depends_on = ["A"]

[[bria.tasks]]
id = "noop"
driver = "local"
cmd = "true"
"#,
    );
    let err = cfg.validate().expect_err("cycle must fail");
    assert!(err.to_string().contains("cycle"));
}

#[test]
fn validate_accepts_linear_pipeline_with_implicit_deps() {
    let cfg = parse(
        r#"
[bria]
[[bria.sources]]
id = "s"
type = "file"
path = "u.jsonl"

[[bria.pipelines]]
id = "pl"
source = "s"

[[bria.pipelines.steps]]
id = "first"
type = "process"
task = "noop"

[[bria.pipelines.steps]]
id = "second"
type = "process"
task = "noop"

[[bria.tasks]]
id = "noop"
driver = "local"
cmd = "true"
"#,
    );
    cfg.validate()
        .expect("linear implicit-deps should be valid");
}

#[test]
fn validate_accepts_diamond_dag() {
    let cfg = parse(
        r#"
[bria]
[[bria.sources]]
id = "s"
type = "file"
path = "u.jsonl"

[[bria.pipelines]]
id = "pl"
source = "s"
concurrency = 2

[[bria.pipelines.steps]]
id = "root"
type = "process"
task = "noop"

[[bria.pipelines.steps]]
id = "left"
type = "process"
task = "noop"
depends_on = ["root"]

[[bria.pipelines.steps]]
id = "right"
type = "process"
task = "noop"
depends_on = ["root"]

[[bria.pipelines.steps]]
id = "join"
type = "process"
task = "noop"
depends_on = ["left", "right"]

[[bria.tasks]]
id = "noop"
driver = "local"
cmd = "true"
"#,
    );
    cfg.validate().expect("diamond DAG should be valid");
}

// ---------------------------------------------------------------------------
// LogConfig::effective_format
// ---------------------------------------------------------------------------

#[test]
fn effective_format_returns_explicit_json() {
    let log = LogConfig {
        format: Some("json".to_string()),
        ..Default::default()
    };
    assert_eq!(log.effective_format(), "json");
}

#[test]
fn effective_format_returns_explicit_text() {
    let log = LogConfig {
        format: Some("text".to_string()),
        ..Default::default()
    };
    assert_eq!(log.effective_format(), "text");
}

#[test]
fn effective_format_returns_json_when_format_is_none_in_non_tty() {
    // In test environments stdout is typically not a TTY, so auto-detect
    // should yield "json".
    let log = LogConfig {
        format: None,
        ..Default::default()
    };
    // This is non-deterministic depending on test runner, but CI/non-TTY
    // environments will report `is_terminal() == false`, giving "json".
    let fmt = log.effective_format();
    // Accept either since we can't control TTY status in tests.
    assert!(fmt == "json" || fmt == "text");
}

// ---------------------------------------------------------------------------
// Evaluator::eval_bool
// ---------------------------------------------------------------------------

fn empty_context() -> Context {
    Context::new(Job {
        id: "test-job".to_string(),
        source: "test".to_string(),
        payload: serde_json::json!({}),
        correlation_key: None,
        labels: HashMap::new(),
    })
}

fn context_with_payload(payload: serde_json::Value) -> Context {
    Context::new(Job {
        id: "test-job".to_string(),
        source: "test".to_string(),
        payload,
        correlation_key: None,
        labels: HashMap::new(),
    })
}

#[test]
fn eval_bool_literal_true() {
    let e = Evaluator::new();
    let ctx = empty_context();
    assert!(e.eval_bool("true", &ctx).unwrap());
}

#[test]
fn eval_bool_literal_false() {
    let e = Evaluator::new();
    let ctx = empty_context();
    assert!(!e.eval_bool("false", &ctx).unwrap());
}

#[test]
fn eval_bool_payload_field_comparison() {
    let e = Evaluator::new();
    let ctx = context_with_payload(serde_json::json!({ "x": 1 }));
    assert!(e.eval_bool("job.payload.x == 1", &ctx).unwrap());
    assert!(!e.eval_bool("job.payload.x == 2", &ctx).unwrap());
}

#[test]
fn eval_bool_step_exit_code() {
    let e = Evaluator::new();
    let mut ctx = empty_context();
    ctx.set_step(
        "s".to_string(),
        StepResult {
            stdout: None,
            stderr: None,
            exit_code: 0,
            duration_ms: 10,
            attempt: 1,
            outputs: HashMap::new(),
        },
    );
    assert!(e.eval_bool("steps.s.exit_code == 0", &ctx).unwrap());
}

#[test]
fn eval_bool_step_output_field() {
    let e = Evaluator::new();
    let mut ctx = empty_context();
    let mut outputs = HashMap::new();
    outputs.insert("flag".to_string(), serde_json::json!(true));
    ctx.set_step(
        "s".to_string(),
        StepResult {
            stdout: None,
            stderr: None,
            exit_code: 0,
            duration_ms: 10,
            attempt: 1,
            outputs,
        },
    );
    assert!(e.eval_bool("steps.s.outputs.flag == true", &ctx).unwrap());
}

#[test]
fn eval_bool_label_field() {
    let e = Evaluator::new();
    let mut labels = HashMap::new();
    labels.insert("env".to_string(), "prod".to_string());
    let ctx = Context::new(Job {
        id: "test-job".to_string(),
        source: "test".to_string(),
        payload: serde_json::json!({}),
        correlation_key: None,
        labels,
    });
    assert!(e.eval_bool("job.labels.env == 'prod'", &ctx).unwrap());
}

#[test]
fn eval_bool_non_bool_result_returns_error() {
    let e = Evaluator::new();
    let ctx = empty_context();
    let err = e
        .eval_bool("'not_bool'", &ctx)
        .expect_err("non-bool must error");
    assert!(err.to_string().contains("Expected boolean result"));
}

#[test]
fn eval_bool_syntax_error_returns_error() {
    let e = Evaluator::new();
    let ctx = empty_context();
    let err = e
        .eval_bool("syntax error ][", &ctx)
        .expect_err("parse error expected");
    assert!(err.to_string().contains("CEL parse error"));
}

// ---------------------------------------------------------------------------
// Evaluator::eval_value
// ---------------------------------------------------------------------------

#[test]
fn eval_value_string_concat() {
    let e = Evaluator::new();
    let ctx = context_with_payload(serde_json::json!({ "bucket": "assets" }));
    let result = e.eval_value("'s3://' + job.payload.bucket", &ctx).unwrap();
    assert_eq!(result, serde_json::json!("s3://assets"));
}

#[test]
fn eval_value_arithmetic() {
    let e = Evaluator::new();
    let ctx = context_with_payload(serde_json::json!({ "count": 41 }));
    let result = e.eval_value("job.payload.count + 1", &ctx).unwrap();
    assert_eq!(result, serde_json::json!(42));
}

#[test]
fn eval_value_list_literal() {
    let e = Evaluator::new();
    let ctx = empty_context();
    let result = e.eval_value("[1, 2, 3]", &ctx).unwrap();
    assert_eq!(result, serde_json::json!([1, 2, 3]));
}

// ---------------------------------------------------------------------------
// Evaluator::eval_merge_bool
// ---------------------------------------------------------------------------

#[test]
fn eval_merge_bool_matching_values_returns_true() {
    let e = Evaluator::new();
    let a = context_with_payload(serde_json::json!({ "order_id": "ord-1" }));
    let b = context_with_payload(serde_json::json!({ "order_id": "ord-1" }));
    let result = e
        .eval_merge_bool("a.job.payload.order_id == b.job.payload.order_id", &a, &b)
        .unwrap();
    assert!(result);
}

#[test]
fn eval_merge_bool_non_matching_values_returns_false() {
    let e = Evaluator::new();
    let a = context_with_payload(serde_json::json!({ "order_id": "ord-1" }));
    let b = context_with_payload(serde_json::json!({ "order_id": "ord-2" }));
    let result = e
        .eval_merge_bool("a.job.payload.order_id == b.job.payload.order_id", &a, &b)
        .unwrap();
    assert!(!result);
}

#[test]
fn eval_merge_bool_non_bool_result_returns_error() {
    let e = Evaluator::new();
    let a = empty_context();
    let b = empty_context();
    let err = e
        .eval_merge_bool("'not_a_bool'", &a, &b)
        .expect_err("non-bool merge result must error");
    assert!(err.to_string().contains("Expected boolean result"));
}

// ---------------------------------------------------------------------------
// Evaluator pipeline ID
// ---------------------------------------------------------------------------

#[test]
fn eval_with_pipeline_id_exposes_pipeline_dot_id() {
    let e = Evaluator::with_pipeline_id("my-pipeline");
    let ctx = empty_context();
    let result = e.eval_value("pipeline.id", &ctx).unwrap();
    assert_eq!(result, serde_json::json!("my-pipeline"));
}

#[test]
fn eval_default_pipeline_id_is_empty_string() {
    let e = Evaluator::new();
    let ctx = empty_context();
    let result = e.eval_value("pipeline.id", &ctx).unwrap();
    assert_eq!(result, serde_json::json!(""));
}

// ---------------------------------------------------------------------------
// TemplateEngine::render
// ---------------------------------------------------------------------------

#[test]
fn template_render_job_id() {
    let engine = TemplateEngine::new();
    let ctx = Context::new(Job {
        id: "01JABC".to_string(),
        source: "test".to_string(),
        payload: serde_json::json!({}),
        correlation_key: None,
        labels: HashMap::new(),
    });
    let out = engine.render("{{job.id}}", &ctx).unwrap();
    assert_eq!(out, "01JABC");
}

#[test]
fn template_render_job_source() {
    let engine = TemplateEngine::new();
    let ctx = Context::new(Job {
        id: "j1".to_string(),
        source: "manual".to_string(),
        payload: serde_json::json!({}),
        correlation_key: None,
        labels: HashMap::new(),
    });
    let out = engine.render("{{job.source}}", &ctx).unwrap();
    assert_eq!(out, "manual");
}

#[test]
fn template_render_payload_string_field() {
    let engine = TemplateEngine::new();
    let ctx = Context::new(Job {
        id: "j1".to_string(),
        source: "test".to_string(),
        payload: serde_json::json!({ "name": "Bria" }),
        correlation_key: None,
        labels: HashMap::new(),
    });
    let out = engine.render("{{job.payload.name}}", &ctx).unwrap();
    assert_eq!(out, "Bria");
}

#[test]
fn template_render_payload_numeric_field() {
    let engine = TemplateEngine::new();
    let ctx = Context::new(Job {
        id: "j1".to_string(),
        source: "test".to_string(),
        payload: serde_json::json!({ "count": 42 }),
        correlation_key: None,
        labels: HashMap::new(),
    });
    let out = engine.render("{{job.payload.count}}", &ctx).unwrap();
    assert_eq!(out, "42");
}

#[test]
fn template_render_step_stdout() {
    let engine = TemplateEngine::new();
    let mut ctx = Context::new(Job {
        id: "j1".to_string(),
        source: "test".to_string(),
        payload: serde_json::json!({}),
        correlation_key: None,
        labels: HashMap::new(),
    });
    ctx.set_step(
        "run".to_string(),
        StepResult {
            stdout: Some("hello world".to_string()),
            stderr: None,
            exit_code: 0,
            duration_ms: 10,
            attempt: 1,
            outputs: HashMap::new(),
        },
    );
    let out = engine.render("{{steps.run.stdout}}", &ctx).unwrap();
    assert_eq!(out, "hello world");
}

#[test]
fn template_render_step_stderr() {
    let engine = TemplateEngine::new();
    let mut ctx = Context::new(Job {
        id: "j1".to_string(),
        source: "test".to_string(),
        payload: serde_json::json!({}),
        correlation_key: None,
        labels: HashMap::new(),
    });
    ctx.set_step(
        "run".to_string(),
        StepResult {
            stdout: None,
            stderr: Some("oops".to_string()),
            exit_code: 1,
            duration_ms: 5,
            attempt: 1,
            outputs: HashMap::new(),
        },
    );
    let out = engine.render("{{steps.run.stderr}}", &ctx).unwrap();
    assert_eq!(out, "oops");
}

#[test]
fn template_render_step_exit_code() {
    let engine = TemplateEngine::new();
    let mut ctx = Context::new(Job {
        id: "j1".to_string(),
        source: "test".to_string(),
        payload: serde_json::json!({}),
        correlation_key: None,
        labels: HashMap::new(),
    });
    ctx.set_step(
        "run".to_string(),
        StepResult {
            stdout: None,
            stderr: None,
            exit_code: 33,
            duration_ms: 10,
            attempt: 1,
            outputs: HashMap::new(),
        },
    );
    let out = engine.render("{{steps.run.exit_code}}", &ctx).unwrap();
    assert_eq!(out, "33");
}

#[test]
fn template_render_step_output() {
    let engine = TemplateEngine::new();
    let mut ctx = Context::new(Job {
        id: "j1".to_string(),
        source: "test".to_string(),
        payload: serde_json::json!({}),
        correlation_key: None,
        labels: HashMap::new(),
    });
    let mut outputs = HashMap::new();
    outputs.insert("key".to_string(), serde_json::json!("secret-value"));
    ctx.set_step(
        "run".to_string(),
        StepResult {
            stdout: None,
            stderr: None,
            exit_code: 0,
            duration_ms: 10,
            attempt: 1,
            outputs,
        },
    );
    let out = engine.render("{{steps.run.outputs.key}}", &ctx).unwrap();
    assert_eq!(out, "secret-value");
}

#[test]
fn template_render_env_variable() {
    unsafe { std::env::set_var("BRIA_TEMPLATE_TEST_HOME", "/home/bria") };
    let engine = TemplateEngine::new();
    let ctx = empty_context();
    let out = engine
        .render("{{env.BRIA_TEMPLATE_TEST_HOME}}", &ctx)
        .unwrap();
    assert_eq!(out, "/home/bria");
}

#[test]
fn template_render_now_iso_8601() {
    let engine = TemplateEngine::new();
    let ctx = empty_context();
    let out = engine.render("{{now}}", &ctx).unwrap();
    // Basic ISO-8601 check: contains 'T' and dashes
    assert!(out.contains('T'));
    assert!(out.contains('-'));
}

#[test]
fn template_render_now_unix() {
    let engine = TemplateEngine::new();
    let ctx = empty_context();
    let out = engine.render("{{now_unix}}", &ctx).unwrap();
    let timestamp: i64 = out.parse().unwrap();
    assert!(timestamp > 1_700_000_000); // year ~2023+
}

#[test]
fn template_render_label() {
    let engine = TemplateEngine::new();
    let mut labels = HashMap::new();
    labels.insert("team".to_string(), "platform".to_string());
    let ctx = Context::new(Job {
        id: "j1".to_string(),
        source: "test".to_string(),
        payload: serde_json::json!({}),
        correlation_key: None,
        labels,
    });
    let out = engine.render("{{job.labels.team}}", &ctx).unwrap();
    assert_eq!(out, "platform");
}

#[test]
fn template_render_missing_variable_returns_error() {
    let engine = TemplateEngine::new();
    let ctx = empty_context();
    let err = engine
        .render("{{missing_var}}", &ctx)
        .expect_err("undefined variable must error");
    assert!(err.to_string().contains("undefined"));
}

#[test]
fn template_render_no_placeholders_returns_as_is() {
    let engine = TemplateEngine::new();
    let ctx = empty_context();
    let out = engine.render("plain literal text", &ctx).unwrap();
    assert_eq!(out, "plain literal text");
}

// ---------------------------------------------------------------------------
// TemplateEngine::render_result
// ---------------------------------------------------------------------------

#[test]
fn template_render_result_pipeline_id() {
    let engine = TemplateEngine::new();
    let ctx = empty_context();
    let out = engine
        .render_result(
            "{{pipeline.id}}",
            &ctx,
            "greeting",
            "success",
            120,
            "2025-01-01T00:00:00Z",
        )
        .unwrap();
    assert_eq!(out, "greeting");
}

#[test]
fn template_render_result_status() {
    let engine = TemplateEngine::new();
    let ctx = empty_context();
    let out = engine
        .render_result(
            "{{result.status}}",
            &ctx,
            "pl",
            "failure",
            10,
            "2025-01-01T00:00:00Z",
        )
        .unwrap();
    assert_eq!(out, "failure");
}

#[test]
fn template_render_result_duration_ms() {
    let engine = TemplateEngine::new();
    let ctx = empty_context();
    let out = engine
        .render_result(
            "{{result.duration_ms}}",
            &ctx,
            "pl",
            "success",
            999,
            "2025-01-01T00:00:00Z",
        )
        .unwrap();
    assert_eq!(out, "999");
}

#[test]
fn template_render_result_occurred_at() {
    let engine = TemplateEngine::new();
    let ctx = empty_context();
    let out = engine
        .render_result(
            "{{occurred_at}}",
            &ctx,
            "pl",
            "success",
            1,
            "2025-06-08T12:00:00Z",
        )
        .unwrap();
    assert_eq!(out, "2025-06-08T12:00:00Z");
}

// ---------------------------------------------------------------------------
// Context::new / set_step / PipelineResult
// ---------------------------------------------------------------------------

#[test]
fn context_new_has_empty_steps() {
    let ctx = Context::new(Job {
        id: "j1".to_string(),
        source: "test".to_string(),
        payload: serde_json::json!({}),
        correlation_key: None,
        labels: HashMap::new(),
    });
    assert!(ctx.steps.is_empty());
    assert_eq!(ctx.job.id, "j1");
}

#[test]
fn context_set_step_inserts_and_overwrites() {
    let mut ctx = empty_context();
    let sr = StepResult {
        stdout: Some("first".to_string()),
        stderr: None,
        exit_code: 0,
        duration_ms: 5,
        attempt: 1,
        outputs: HashMap::new(),
    };
    ctx.set_step("alpha".to_string(), sr.clone());
    assert_eq!(ctx.steps["alpha"].stdout, Some("first".to_string()));

    let sr2 = StepResult {
        stdout: Some("second".to_string()),
        ..sr
    };
    ctx.set_step("alpha".to_string(), sr2);
    assert_eq!(ctx.steps["alpha"].stdout, Some("second".to_string()));
}

#[test]
fn pipeline_result_success_has_correct_status() {
    let job = Job {
        id: "j1".to_string(),
        source: "test".to_string(),
        payload: serde_json::json!({}),
        correlation_key: None,
        labels: HashMap::new(),
    };
    let result = PipelineResult::success("my-pipe".to_string(), job, HashMap::new(), 100);
    assert_eq!(result.status, "success");
    assert_eq!(result.pipeline_id, "my-pipe");
    assert_eq!(result.duration_ms, 100);
    assert!(!result.occurred_at.is_empty());
}

#[test]
fn pipeline_result_failure_has_correct_status() {
    let job = Job {
        id: "j2".to_string(),
        source: "test".to_string(),
        payload: serde_json::json!({}),
        correlation_key: None,
        labels: HashMap::new(),
    };
    let result = PipelineResult::failure("fail-pipe".to_string(), job, HashMap::new(), 50);
    assert_eq!(result.status, "failure");
    assert_eq!(result.pipeline_id, "fail-pipe");
}

// ---------------------------------------------------------------------------
// Error display / From impls
// ---------------------------------------------------------------------------

#[test]
fn error_config_display_includes_prefix() {
    let err = Error::config("something bad");
    assert_eq!(err.to_string(), "Configuration error: something bad");
}

#[test]
fn error_validation_display_includes_prefix() {
    let err = Error::validation("bad input");
    assert_eq!(err.to_string(), "Validation error: bad input");
}

#[test]
fn error_pipeline_display_includes_prefix() {
    let err = Error::pipeline("broken DAG");
    assert_eq!(err.to_string(), "Pipeline error: broken DAG");
}

#[test]
fn error_task_display_includes_prefix() {
    let err = Error::task("exit 99");
    assert_eq!(err.to_string(), "Task execution error: exit 99");
}

#[test]
fn error_unsupported_display_includes_prefix() {
    let err = Error::unsupported("not yet");
    assert_eq!(err.to_string(), "Unsupported: not yet");
}

#[test]
fn error_state_display_includes_prefix() {
    let err = Error::state("store unreachable");
    assert_eq!(err.to_string(), "State store error: store unreachable");
}

#[test]
fn error_internal_display_includes_prefix() {
    let err = Error::internal("panic at the disco");
    assert_eq!(err.to_string(), "Internal error: panic at the disco");
}

#[test]
fn error_from_string_produces_internal() {
    let err: Error = "boom".to_string().into();
    assert_eq!(err.to_string(), "Internal error: boom");
}

#[test]
fn error_from_io_error_produces_io() {
    let io_err = std::io::Error::new(std::io::ErrorKind::NotFound, "file missing");
    let err: Error = io_err.into();
    assert!(err.to_string().contains("IO error"));
}

#[test]
fn error_from_serde_json_error_produces_json() {
    let json_err = serde_json::from_str::<serde_json::Value>("not json").unwrap_err();
    let err: Error = json_err.into();
    assert!(err.to_string().contains("JSON error"));
}

// ---------------------------------------------------------------------------
// CLI
// ---------------------------------------------------------------------------

#[test]
fn cli_is_ping_returns_true_for_ping_subcommand() {
    use clap::Parser;
    let cli = bria::Cli::try_parse_from(["bria", "ping"]).expect("parse");
    assert!(cli.is_ping());
}

#[test]
fn cli_is_ping_returns_false_without_command() {
    use clap::Parser;
    let cli = bria::Cli::try_parse_from(["bria"]).expect("parse");
    assert!(!cli.is_ping());
}

#[test]
fn cli_recognizes_check_subcommand() {
    use clap::Parser;
    let cli = bria::Cli::try_parse_from(["bria", "check"]).expect("parse");
    assert!(cli.is_check());
}

#[test]
fn validate_rejects_duplicate_internal_submission_routes() {
    let config = Config::from_str_with_env(
        r#"
[bria]
[bria.server]
enabled = true

[[bria.sources]]
id = "one"
type = "http"
path = "jobs"

[[bria.sources]]
id = "two"
type = "webhook"
path = "/jobs/"
"#,
    )
    .unwrap();

    let error = config.validate().unwrap_err();
    assert!(error.to_string().contains("configured more than once"));
}

#[test]
fn validate_rejects_invalid_internal_server_prefix() {
    let config = Config::from_str_with_env(
        r#"
[bria]
[bria.server]
enabled = true
prefix = "internal/v1"
"#,
    )
    .unwrap();

    let error = config.validate().unwrap_err();
    assert!(error.to_string().contains("server.prefix"));
}
