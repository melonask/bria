/// Tests for the universal namespaced config model.
/// These tests use the explicit `[bria]` namespace format.
use bria::Config as BriaConfig;

struct Config;

impl Config {
    fn from_str_with_env(raw: &str) -> bria::Result<BriaConfig> {
        // Some fixtures begin with a shared table and historically placed the
        // schema declaration at their first Bria table. Normalize it to the
        // required root location while retaining the fixture's actual subject.
        let raw = format!("version = 1\n{}", raw.replace("version = 1\n", ""));
        BriaConfig::from_str_with_env(&raw)
    }

    fn load_from_path(path: impl AsRef<std::path::Path>) -> bria::Result<BriaConfig> {
        BriaConfig::load_from_path(path)
    }
}

// =============================================================================
// Merged config loading with other namespaces
// =============================================================================

#[test]
fn loads_merged_config_with_all_namespaces() {
    let raw = r#"
version = 1

[log]
level = "info"

[stores.bria]
driver = "sqlite"
url = "sqlite://data/bria/bria-state.db"

[transports.amqp.local]
url = "amqp://localhost:5672"

[transports.webhook.ops]
url = "${OPS_WEBHOOK_URL:-https://example.com}"
timeout_secs = 10
max_retries = 3

[paths.bria_jobs]
kind = "file"
path = "data/bria/jobs/test.jsonl"
format = "jsonl"

[paths.bria_results]
kind = "file"
path = "data/bria/results.jsonl"
format = "jsonl"

[objects.local]
driver = "fs"
root = "data/objects"

# --- ladon (should be ignored) ---
[ladon]
enabled = true

[ladon.derive]
format = "json"

# --- pano (should be ignored) ---
[pano]
enabled = true

[pano.server]
enabled = false

# --- oracles (should be ignored) ---
[oracles]
enabled = true

# --- bria ---
[bria]
enabled = true

[bria.global]
worker_threads = 2
shutdown_timeout_secs = 60

[bria.global.state]
backend = "file"
store = "bria"

[[bria.sources]]
id = "test-file"
type = "file"
path_ref = "bria_jobs"
poll_interval_secs = 5
id_field = "job_id"

[[bria.tasks]]
id = "test-task"
driver = "local"
cmd = "echo"
args = ["hello"]

[[bria.sinks]]
id = "test-sink"
type = "file"
path_ref = "bria_results"

[[bria.pipelines]]
id = "test-pipeline"
source = "test-file"
concurrency = 1
sinks = ["test-sink"]

[[bria.pipelines.steps]]
id = "step1"
type = "process"
task = "test-task"
"#;

    let config = Config::from_str_with_env(raw).expect("merged config should load");
    config.validate().expect("merged config should validate");

    // Verify shared sections were read
    assert_eq!(config.global.worker_threads, 2);
    assert_eq!(config.global.shutdown_timeout_secs, 60);
    assert_eq!(config.global.state.backend, "file");

    // Verify path_ref resolved
    assert_eq!(
        config.sources[0].path.to_str().unwrap(),
        "data/bria/jobs/test.jsonl"
    );
    assert_eq!(config.sinks[0].path, "data/bria/results.jsonl");

    // Verify bria-specific config
    assert_eq!(config.sources.len(), 1);
    assert_eq!(config.tasks.len(), 1);
    assert_eq!(config.sinks.len(), 1);
    assert_eq!(config.pipelines.len(), 1);
}

#[test]
fn accepts_root_level_universal_config_with_artur_namespace() {
    let raw = r#"
version = 1

[log]
level = "info"

[runtime]
max_payload_bytes = 1048576

[stores.bria]
driver = "postgres"
url = "postgres://bria:bria@localhost/bria"

[artur.server]
bind = "127.0.0.1"
port = 46796
body_limit_bytes = 1048576

[[artur.endpoints]]
name = "readiness"
method = "GET"
path = "/readyz"
action = "respond.static"

[artur.endpoints.response]
status = 200
body = { status = "ready" }

[bria]
enabled = true
"#;

    let config = Config::from_str_with_env(raw)
        .expect("Artur must be tolerated as a peer universal namespace");
    config
        .validate()
        .expect("Bria validation must remain strict only within Bria configuration");
    assert_eq!(config.global.state.backend, "memory");
}

#[test]
fn ignores_unrelated_namespaces() {
    let raw = r#"
[ladon]
enabled = true

[ladon.derive]
format = "json"

[pano]
enabled = true

[pano.server]
enabled = true

[oracles]
enabled = true

[oracles.table]
name = "rates"

version = 1
[bria]
enabled = true

[[bria.sources]]
id = "src"
type = "file"
path = "data.jsonl"

[[bria.tasks]]
id = "t"
driver = "local"
cmd = "true"

[[bria.pipelines]]
id = "p"
source = "src"
"#;

    let config =
        Config::from_str_with_env(raw).expect("config with unrelated namespaces should load");
    config.validate().expect("should validate");
    assert_eq!(config.sources.len(), 1);
    assert_eq!(config.sources[0].id, "src");
}

// =============================================================================
// Strict unknown field rejection inside [bria]
// =============================================================================

#[test]
fn rejects_unknown_field_in_bria_section() {
    let raw = r#"
version = 1
[bria]
enabled = true
unknown_field = "should fail"

[[bria.sources]]
id = "src"
type = "file"
path = "data.jsonl"
"#;

    let err = Config::from_str_with_env(raw).expect_err("unknown bria field must fail");
    assert!(err.to_string().contains("unknown"));
}

#[test]
fn rejects_unknown_field_in_bria_source() {
    let raw = r#"
version = 1
[bria]
[[bria.sources]]
id = "src"
type = "file"
path = "data.jsonl"
bad_field = "nope"
"#;

    let err = Config::from_str_with_env(raw).expect_err("unknown source field must fail");
    assert!(err.to_string().contains("unknown"));
}

#[test]
fn rejects_unknown_field_in_bria_task() {
    let raw = r#"
version = 1
[bria]
[[bria.tasks]]
id = "t"
driver = "local"
cmd = "true"
garbage_key = 123
"#;

    let err = Config::from_str_with_env(raw).expect_err("unknown task field must fail");
    assert!(err.to_string().contains("unknown"));
}

#[test]
fn rejects_unknown_field_in_bria_global() {
    let raw = r#"
version = 1
[bria]
[bria.global]
worker_threads = 0
nonsense = "reject me"
"#;

    let err = Config::from_str_with_env(raw).expect_err("unknown global field must fail");
    assert!(err.to_string().contains("unknown"));
}

#[test]
fn rejects_unknown_field_in_bria_server() {
    let raw = r#"
version = 1
[bria]
[bria.server]
enabled = false
port = 4000
hack_attempt = true
"#;

    let err = Config::from_str_with_env(raw).expect_err("unknown server field must fail");
    assert!(err.to_string().contains("unknown"));
}

// =============================================================================
// Shared store resolution
// =============================================================================

#[test]
fn resolves_shared_store_for_state() {
    let raw = r#"
[stores.bria]
driver = "sqlite"
url = "sqlite://data/bria/my-state.db"
migrate = true

version = 1
[bria]
[bria.global.state]
backend = "sqlite"
store = "bria"
"#;

    let config = Config::from_str_with_env(raw).expect("store resolution should work");
    assert_eq!(config.global.state.backend, "sqlite");
    assert_eq!(config.global.state.sqlite_path, "data/bria/my-state.db");
}

#[test]
fn resolves_shared_store_for_sink() {
    let raw = r#"
[stores.bria]
driver = "sqlite"
url = "sqlite://data/bria/sink-store.db"

version = 1
[bria]
[[bria.sources]]
id = "src"
type = "file"
path = "data.jsonl"

[[bria.sinks]]
id = "db-sink"
type = "sqlite"
store = "bria"
table_name = "results"

[[bria.pipelines]]
id = "p"
source = "src"

[[bria.tasks]]
id = "t"
driver = "local"
cmd = "true"
"#;

    let config = Config::from_str_with_env(raw).expect("sink store resolution should work");
    assert_eq!(config.sinks[0].path, "data/bria/sink-store.db");
}

#[test]
fn errors_on_unknown_store_reference() {
    let raw = r#"
version = 1
[bria]
[bria.global.state]
backend = "sqlite"
store = "nonexistent_store"
"#;

    // State store reference: missing store in [stores] is silently ignored,
    // but sqlite_path remains default. Let's verify it doesn't crash.
    let config = Config::from_str_with_env(raw).expect("unknown store should not crash");
    // sqlite_path keeps default
    assert_eq!(config.global.state.sqlite_path, "bria-state.db");
}

// =============================================================================
// Shared path resolution
// =============================================================================

#[test]
fn resolves_path_ref_for_file_source() {
    let raw = r#"
[paths.my_jobs]
kind = "file"
path = "custom/path/input.jsonl"
format = "jsonl"

version = 1
[bria]
[[bria.sources]]
id = "src"
type = "file"
path_ref = "my_jobs"
"#;

    let config = Config::from_str_with_env(raw).expect("path_ref should resolve");
    assert_eq!(
        config.sources[0].path.to_str().unwrap(),
        "custom/path/input.jsonl"
    );
}

#[test]
fn direct_path_overrides_path_ref() {
    let raw = r#"
[paths.my_jobs]
kind = "file"
path = "from/profile.jsonl"

version = 1
[bria]
[[bria.sources]]
id = "src"
type = "file"
path = "direct/path.jsonl"
path_ref = "my_jobs"
"#;

    let config = Config::from_str_with_env(raw).expect("direct path should win");
    assert_eq!(
        config.sources[0].path.to_str().unwrap(),
        "direct/path.jsonl"
    );
}

#[test]
fn errors_on_unknown_path_ref() {
    let raw = r#"
version = 1
[bria]
[[bria.sources]]
id = "src"
type = "file"
path_ref = "nowhere"
"#;

    let err = Config::from_str_with_env(raw).expect_err("unknown path_ref must fail");
    assert!(err.to_string().contains("path_ref"));
    assert!(err.to_string().contains("nowhere"));
}

// =============================================================================
// Shared transport resolution
// =============================================================================

#[test]
fn resolves_amqp_transport_for_queue_source() {
    let raw = r#"
[transports.amqp.mybroker]
url = "amqp://my-broker:5672"
username = "admin"
password = "secret"
reconnect_secs = 10
qos_prefetch = 50

version = 1
[bria]
[[bria.sources]]
id = "q-source"
type = "queue"
transport = "mybroker"
exchange = "bria.test"
"#;

    let config = Config::from_str_with_env(raw).expect("AMQP transport should resolve");
    assert_eq!(config.sources[0].url, "amqp://my-broker:5672");
    assert_eq!(config.sources[0].username, "admin");
    assert_eq!(config.sources[0].password, "secret");
    assert_eq!(config.sources[0].reconnect_secs, 10);
    assert_eq!(config.sources[0].qos_prefetch, 50);
}

#[test]
fn resolves_webhook_transport_for_sink() {
    let raw = r#"
[transports.webhook.ops]
url = "https://hooks.example.com/webhook"
timeout_secs = 15
max_retries = 5

version = 1
[bria]
[[bria.sources]]
id = "src"
type = "file"
path = "data.jsonl"

[[bria.sinks]]
id = "wh-sink"
type = "webhook"
transport = "ops"
template = ""

[[bria.pipelines]]
id = "p"
source = "src"

[[bria.tasks]]
id = "t"
driver = "local"
cmd = "true"
"#;

    let config = Config::from_str_with_env(raw).expect("webhook transport should resolve");
    assert_eq!(config.sinks[0].url, "https://hooks.example.com/webhook");
    assert_eq!(config.sinks[0].timeout_secs, 15);
    assert_eq!(config.sinks[0].max_retries, 5);
}

#[test]
fn local_values_override_transport_profile() {
    let raw = r#"
[transports.amqp.mybroker]
url = "amqp://profile-broker:5672"
qos_prefetch = 50
reconnect_secs = 5

version = 1
[bria]
[[bria.sources]]
id = "q-source"
type = "queue"
transport = "mybroker"
url = "amqp://override-broker:5672"
qos_prefetch = 200
exchange = "bria.test"
"#;

    let config = Config::from_str_with_env(raw).expect("local overrides should apply");
    assert_eq!(config.sources[0].url, "amqp://override-broker:5672");
    assert_eq!(config.sources[0].qos_prefetch, 200);
    // reconnect not overridden, so from profile
    assert_eq!(config.sources[0].reconnect_secs, 5);
}

// =============================================================================
// SQLite default and Postgres feature-gate
// =============================================================================

#[test]
fn sqlite_is_default_state_backend() {
    // With sqlite feature on (in default features), state should work
    let raw = r#"
version = 1
[bria]
[bria.global.state]
backend = "sqlite"
sqlite_path = ":memory:"

[[bria.sources]]
id = "src"
type = "file"
path = "data.jsonl"
"#;

    let config = Config::from_str_with_env(raw).expect("sqlite config should load");
    assert_eq!(config.global.state.backend, "sqlite");
}

// =============================================================================
// Environment variable expansion
// =============================================================================

#[test]
fn env_var_with_default_expands_correctly() {
    // Set a variable that WILL be used
    unsafe { std::env::set_var("BRIA_UNI_TEST_SET", "explicit-value") };
    // Unset a variable that has a default
    unsafe { std::env::remove_var("BRIA_UNI_TEST_MISSING") };

    let raw = r#"
[transports.amqp.testing]
url = "amqp://${BRIA_UNI_TEST_SET:-fallback}:5672"
username = "${BRIA_UNI_TEST_MISSING:-default-user}"

version = 1
[bria]
[[bria.sources]]
id = "src"
type = "queue"
transport = "testing"
exchange = "bria.test"
"#;

    let config = Config::from_str_with_env(raw).expect("env expansion should work");
    assert_eq!(config.sources[0].url, "amqp://explicit-value:5672");
    assert_eq!(config.sources[0].username, "default-user");
}

#[test]
fn missing_env_var_without_default_fails() {
    unsafe { std::env::remove_var("BRIA_UNI_NEVER_SET_12345") };

    let raw = r#"
version = 1
[bria]
[[bria.sources]]
id = "src"
type = "file"
path = "${BRIA_UNI_NEVER_SET_12345}/jobs.jsonl"
"#;

    let err = Config::from_str_with_env(raw).expect_err("missing env var must fail");
    assert!(err.to_string().contains("BRIA_UNI_NEVER_SET_12345"));
}

#[test]
fn env_var_default_with_colon_works() {
    unsafe { std::env::remove_var("BRIA_UNI_MISSING_URL") };

    let raw = r#"
[transports.webhook.ops]
url = "${BRIA_UNI_MISSING_URL:-https://default.example.com/api}"

version = 1
[bria]
[[bria.sources]]
id = "src"
type = "file"
path = "data.jsonl"

[[bria.sinks]]
id = "wh-sink"
type = "webhook"
transport = "ops"
template = ""

[[bria.pipelines]]
id = "p"
source = "src"

[[bria.tasks]]
id = "t"
driver = "local"
cmd = "true"
"#;

    let config = Config::from_str_with_env(raw).expect("default with colon should work");
    assert_eq!(config.sinks[0].url, "https://default.example.com/api");
}

// =============================================================================
// CLI path compatibility
// =============================================================================

#[test]
fn load_from_path_with_universal_config_file() {
    let tmp = std::env::temp_dir().join(format!("bria-uni-config-{}.toml", ulid::Ulid::r#gen()));
    let content = r#"
version = 1

[log]
level = "debug"

[stores.bria]
driver = "sqlite"
url = "sqlite://data/bria/test.db"

[paths.jobs]
kind = "file"
path = "data/jobs.jsonl"

# unrelated namespace
[ladon]
enabled = false

[pano]
enabled = false

[bria]
enabled = true

[[bria.sources]]
id = "src"
type = "file"
path_ref = "jobs"

[[bria.tasks]]
id = "t"
driver = "local"
cmd = "true"

[[bria.pipelines]]
id = "p"
source = "src"
"#;
    std::fs::write(&tmp, content).expect("write test config");
    let config = Config::load_from_path(&tmp).expect("load_from_path should work");
    config.validate().expect("should validate");
    assert_eq!(config.sources[0].id, "src");
    let _ = std::fs::remove_file(tmp);
}

#[test]
fn checked_in_example_loads_and_validates() {
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("Config.example.toml");
    let config = Config::load_from_path(path).expect("checked-in example should load");
    #[cfg(feature = "sqlite")]
    config
        .validate()
        .expect("checked-in example should validate with SQLite support");
    #[cfg(not(feature = "sqlite"))]
    assert!(
        config
            .validate()
            .expect_err("the example requires SQLite support")
            .to_string()
            .contains("requires the 'sqlite' feature")
    );
}

#[test]
fn disabled_components_do_not_require_transport_or_server_configuration() {
    let raw = r#"
version = 1
[bria]

[[bria.sources]]
id = "disabled-http"
type = "http"
enabled = false

[[bria.sinks]]
id = "disabled-webhook"
type = "webhook"
enabled = false
transport = "not-configured"
"#;
    let config = Config::from_str_with_env(raw).expect("disabled components should load");
    config
        .validate()
        .expect("disabled components should not require active configuration");
}

// =============================================================================
// Shared root sections inherited by bria
// =============================================================================

#[test]
fn inherits_shared_log_config() {
    let raw = r#"
[log]
level = "debug"
format = "text"
file = "/tmp/bria.log"

version = 1
[bria]
[[bria.sources]]
id = "src"
type = "file"
path = "data.jsonl"
"#;

    let config = Config::from_str_with_env(raw).expect("shared log should inherit");
    assert_eq!(config.global.log.level, "debug");
    assert_eq!(config.global.log.format, Some("text".to_string()));
    assert_eq!(config.global.log.file, "/tmp/bria.log");
}

#[test]
fn bria_log_overrides_shared_log() {
    let raw = r#"
[log]
level = "debug"

version = 1
[bria]
[bria.global.log]
level = "error"
format = "json"

[[bria.sources]]
id = "src"
type = "file"
path = "data.jsonl"
"#;

    let config = Config::from_str_with_env(raw).expect("bria log should override");
    assert_eq!(config.global.log.level, "error");
    assert_eq!(config.global.log.format, Some("json".to_string()));
}

#[test]
fn inherits_shared_runtime_config() {
    let raw = r#"
[runtime]
worker_threads = 4
shutdown_timeout_secs = 120
tmp_dir = "data/shared_tmp"
max_payload_bytes = 5242880

version = 1
[bria]
[bria.global]

[[bria.sources]]
id = "src"
type = "file"
path = "data.jsonl"
"#;

    let config = Config::from_str_with_env(raw).expect("shared runtime should inherit");
    assert_eq!(config.global.worker_threads, 4);
    assert_eq!(config.global.shutdown_timeout_secs, 120);
    assert_eq!(config.global.tmp_dir.to_str().unwrap(), "data/shared_tmp");
    assert_eq!(config.global.max_payload_bytes, 5242880);
}

#[test]
fn inherits_shared_http_config_for_server() {
    let raw = r#"
[http]
bind = "127.0.0.1"
prefix = "api"
api_key = "secret-key"

version = 1
[bria]
[bria.server]
enabled = true

[[bria.sources]]
id = "src"
type = "file"
path = "data.jsonl"
"#;

    let config = Config::from_str_with_env(raw).expect("shared http should inherit");
    assert_eq!(config.server.bind, "127.0.0.1");
    assert_eq!(config.server.prefix, "api");
    assert_eq!(config.server.api_key, "secret-key");
}

// =============================================================================
// Object store resolution
// =============================================================================

#[test]
fn shared_object_config_is_parsed_without_error() {
    let raw = r#"
[objects.local]
driver = "fs"
root = "data/objects"
public_base_url = "https://cdn.example.com"

version = 1
[bria]
[[bria.sources]]
id = "src"
type = "file"
path = "data.jsonl"
"#;

    let config = Config::from_str_with_env(raw).expect("objects config should parse");
    config.validate().expect("should validate");
}
