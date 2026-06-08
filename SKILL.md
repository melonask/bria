---
name: bria
description: Bria is a Rust-based multi-pipeline job orchestrator. It ingests jobs from files, HTTP/webhooks, AMQP, cron, PostgreSQL, or SQLite, runs local, Docker, or WebAssembly tasks, and emits results to files, webhooks, AMQP, databases, or live streams.
---

Bria is a Rust-based multi-pipeline job orchestrator. It ingests jobs from files, HTTP/webhooks, AMQP, cron, PostgreSQL, or SQLite, runs local, Docker, or WebAssembly tasks, and emits results to files, webhooks, AMQP, databases, or live streams.

> **Briareus** — One Command. Hundred Actions.

## Quick start

```bash
cargo install bria
```

```bash
cp Config.example.toml Config.toml
bria --config Config.toml
bria ping
```

## CLI

| Command | Description |
|---|---|
| `bria --config Config.toml` | Validate configuration and run Bria. |
| `bria ping` | Print `pong`. |

`--config` can also be supplied with `BRIA_CONFIG`. The default is `Config.toml`.

## Configuration model

| Section | Purpose |
|---|---|
| `[global]` | Runtime, logging, state, retry, and timeout defaults. |
| `[server]` | Optional HTTP control plane for HTTP/webhook sources and streams. |
| `[[sources]]` | Inputs that produce jobs. |
| `[[tasks]]` | Reusable task definitions. |
| `[[sinks]]` | Outputs that receive pipeline results. |
| `[[pipelines]]` | DAGs connecting sources, tasks, and sinks. |

Environment variables in the form `${VAR_NAME}` are resolved during config loading. Missing variables fail fast.

## Parameters

### Global

| Key | Default | Description |
|---|---:|---|
| `worker_threads` | `0` | Tokio worker threads; `0` uses logical CPUs. |
| `shutdown_timeout_secs` | `30` | Orchestrator shutdown timeout. |
| `tmp_dir` | OS temp dir | Temporary file directory. |
| `max_payload_bytes` | `10485760` | Maximum job payload size. |
| `cancel_signal_ttl_secs` | `3600` | How long cancellation signals are retained. |

### Logging: `[global.log]`

| Key | Default | Values |
|---|---|---|
| `level` | `info` | `trace`, `debug`, `info`, `warn`, `error` |
| `format` | auto | `text`, `json`; auto uses text on TTY and JSON otherwise |
| `file` | `""` | Optional log file path |

### State: `[global.state]`

| Key | Default | Description |
|---|---|---|
| `backend` | `memory` | `memory`, `sqlite`, or `pg`. |
| `sqlite_path` | `bria-state.db` | SQLite state database. |
| `pg_url` | `""` | Required when `backend = "pg"`. |

State stores queued/running job records for restart recovery. Schema is created automatically on first use.

### Retry and timeout defaults

| Section | Keys |
|---|---|
| `[global.retry]` | `max_attempts`, `base_delay_ms`, `max_delay_ms`, `jitter` |
| `[global.timeout]` | `step_secs`, `action` (`kill`/`term`), `kill_grace_secs` |

Retry precedence: step > task > global. Backoff uses exponential delay and random jitter.

### Server: `[server]`

| Key | Default | Description |
|---|---:|---|
| `enabled` | `false` | Enable HTTP server. |
| `bind` | `0.0.0.0` | Bind address. |
| `port` | `4000` | Listen port. |
| `prefix` | `v1` | Route prefix. |
| `api_key` | `""` | Optional API key for all routes. Use `Authorization: Bearer` or `X-Bria-Api-Key`. |
| `dashboard` | `""` | Static dashboard directory. |
| `shutdown_timeout_secs` | `5` | HTTP drain timeout. |
| `max_body_bytes` | `52428800` | Server-wide body limit. |

Routes: `GET /{prefix}/ping`, `POST /{prefix}/{source.path}`, `DELETE /{prefix}/{source.path}/{job_id}`, `POST /{prefix}/pipelines/{id}/resume`, plus configured SSE/WebSocket stream paths.

### Sources

| Type | Required | Important parameters |
|---|---|---|
| `file` | `path` | `poll_interval_secs`, `track_cursor`, `authoritative`, `id_field`, `max_body_bytes`, `labels` |
| `http` | `path`, `server.enabled=true` | `max_body_bytes`, `id_field`, `labels` |
| `webhook` | `path`, `server.enabled=true` | `hmac_secret`, `hmac_header`, `ack_status`, `max_body_bytes` |
| `queue` | `url`, `exchange` | `username`, `password`, `submit_routing_key`, `cancel_routing_key`, `reconnect_secs`, `qos_prefetch`, `consumer_tag` |
| `cron` | `schedule` | `tz`, `[sources.payload]`, `labels` |
| `pg` | `url`, `[sources.table]` | `poll_interval_secs`, table column names/status values |
| `sqlite` | `path`, `[sources.table]` | same table parameters as `pg` |

Table source columns: `id`, `payload`, `created_at`, `status`, `status_claimed_value`, `status_done_value`, `status_failed_value`.

### Tasks

| Key | Default | Description |
|---|---|---|
| `id` | required | Task identifier. |
| `driver` | `local` | `local`, `docker`, or `wasm`. |
| `cmd` | required | Command, image, or `.wasm` path. Supports templates. |
| `args` | `[]` | Argument templates. |
| `inherit_env` | `false` | Keep parent environment. |
| `working_dir` | current dir | Child working directory. |
| `success_exit_codes` | `[0]` | Successful exit codes. |
| `timeout_secs` | global | Per-task timeout. |
| `timeout_action` | global | `kill` or `term`. |
| `kill_grace_secs` | global | Grace after SIGTERM. |
| `[tasks.env]` | `{}` | Environment variables/templates. |
| `[tasks.stdin]` | `mode="none"` | `none`, `payload`, or `template`. |
| `[tasks.stdout]` / `[tasks.stderr]` | `capture` | `mode`: `capture`, `stream`, `discard`; `max_bytes`. |
| `[tasks.retry]` | global | Retry overrides. |

Driver-specific sections:

| Section | Keys |
|---|---|
| `[tasks.docker]` | `flags`, `mounts`, `pull` (`always`, `missing`, `never`) |
| `[tasks.wasm]` | `dirs`, `max_memory_pages`, `fuel` |

### Sinks

| Type | Required | Parameters |
|---|---|---|
| `file` | `path` | `template` |
| `webhook` | `url` | `secret`, `signature_header`, `content_type`, `max_retries`, `retry_base_ms`, `timeout_secs`, `headers` |
| `queue` | `url`, `exchange` | `username`, `password`, `success_routing_key`, `failure_routing_key`, `reconnect_secs` |
| `pg` | `url`, `[sinks.table]` | Result table and column names |
| `sqlite` | `path`, `[sinks.table]` | Result table and column names |
| `stream` | `server.enabled=true` | `sse`, `websocket`, `ws_heartbeat_secs`, `sse_keepalive_secs`, `broadcast_capacity` |

Table sink columns: `result_id`, `job_id`, `pipeline_id`, `step_id`, `occurred_at`, `exit_code`, `stdout`, `stderr`, `duration_ms`, `attempt`, `status`.

### Pipelines and steps

| Key | Description |
|---|---|
| `id` | Pipeline identifier. |
| `source` | Single source id. |
| `sources` | Multiple source entries for merge pipelines. |
| `[pipelines.merge]` | `strategy` (`any`/`all`), `correlation_key` or `correlation_expr`, `timeout_secs`. |
| `concurrency` | Maximum concurrent steps/jobs. |
| `queue_capacity` | Bounded channel size. |
| `sinks` | Pipeline-level sinks. |
| `[pipelines.failure]` | `action` (`discard`, `dead_letter`, `stop`) and optional `sink`. |
| `labels` | Labels merged into jobs. |

Step types:

| Type | Required | Behavior |
|---|---|---|
| `process` | `task` | Runs a task. |
| `map` | `[[pipelines.steps.set]]` | Mutates `job.payload` using CEL expressions. |
| `condition` | `expr` | On false, `action = "fail"`, `"skip_to"`, or `"emit"`. |

Step parameters include `depends_on`, `[with]` overrides, `[outputs]`, `[retry]`, `[failure]`, `sinks`, and `[[routing]]` conditional sinks.

## Templates and expressions

Templates use MiniJinja and can access `job.*`, `steps.*`, `env.*`, `now`, `now_unix`, `pipeline.*`, `result.*`, and `occurred_at` depending on context.

CEL expressions can read `job.*`, `steps.*`, and `pipeline.*`:

```toml
[[pipelines.steps.set]]
target = "job.payload.output_url"
expr = '"s3://" + job.payload.bucket + "/" + job.payload.key'
```

## Example: HTTP job to local task and file sink

```toml
[server]
enabled = true
port = 4000

[[sources]]
id = "api"
type = "http"
path = "jobs"
id_field = "id"

[[tasks]]
id = "hello"
driver = "local"
cmd = "sh"
args = ["-c", "printf '{\"message\":\"hello %s\"}' \"$1\"", "sh", "{{job.payload.name}}"]

[[sinks]]
id = "results"
type = "file"
path = "results.jsonl"

[[pipelines]]
id = "hello-pipeline"
source = "api"
sinks = ["results"]

[[pipelines.steps]]
id = "run"
type = "process"
task = "hello"
```

Send a job:

```bash
curl -X POST http://localhost:4000/v1/jobs \
  -H 'content-type: application/json' \
  -d '{"id":"job-1","name":"Bria"}'
```

## Docker

```bash
docker run --rm -p 4000:4000 \
  -v "$PWD/Config.toml:/etc/bria/Config.toml:ro" \
  ghcr.io/melonask/bria:latest
```

E2E Docker Compose files and run script live in `tests/e2e/` — see `tests/e2e/README.md`.

## Developer functions and exported API

| Item | Purpose |
|---|---|
| `Config::load_from_path` | Load TOML with environment substitution. |
| `Config::from_str_with_env` | Parse TOML string with `${VAR}` expansion. |
| `Config::validate` | Validate references and type-specific requirements. |
| `Config::get_task`, `Config::get_sink` | Lookup helpers. |
| `Orchestrator::new` | Initialize logging and state store. |
| `Orchestrator::run` | Start sources, server, routers, workers, and sinks. |
| `run_pipeline_once` | Execute one pipeline in tests or embedded use. |
| `create_store` | Create memory/SQLite/PostgreSQL state store. |
| `StateStore` | Trait for queued/running/completed state and recovery. |

## Testing

```bash
# Lint and unit/integration tests
cargo fmt --check
cargo clippy --all-targets --all-features -- -D warnings
cargo test

# End-to-end scenarios (requires Docker)
cd tests/e2e
./run.sh --all                    # build, run all 19 scenarios (~6 min), tear down
./run.sh --infra-up               # start shared infra (postgres, rabbitmq, etc.)
./run.sh http-pg                  # run a single scenario
./run.sh --infra-down             # tear down shared infra
# See tests/e2e/README.md for the full scenario list and architecture
```
