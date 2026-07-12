# Bria

<img align="right" src="https://raw.githubusercontent.com/melonask/bria/refs/heads/main/logo.svg" alt="Bria is a Rust-based multi-pipeline job orchestrator" width="200" />

> **Briareus** — One Command. Hundred Actions.

Bria is a Rust-based multi-pipeline job orchestrator. It ingests jobs from files, HTTP/webhooks, AMQP, cron, PostgreSQL, or SQLite, runs local, Docker, or WebAssembly tasks, and emits results to files, webhooks, AMQP, databases, or live streams.

**Documentation:** https://melonask.github.io/bria/

## Quick start

```bash
cargo install bria
```

The default install includes core file/local/Docker orchestration and SQLite state support.
Enable optional integrations only when needed:

```bash
cargo install bria --features full
cargo install bria --features server,webhook,sqlite
cargo install bria --features pg,amqp
```

```bash
bria --config Config.toml
```

## CLI

| Command | Description |
|---|---|
| `bria --config Config.toml` | Validate configuration and run Bria. |
| `bria check --config Config.toml` | Parse and strictly validate configuration, then exit. |
| `bria ping` | Print `pong`. |

`--config` can also be supplied with `BRIA_CONFIG`. The default is `Config.toml`.
The command-line option takes precedence over `BRIA_CONFIG`; use the same path
selection for `bria`, `bria check`, and container deployments.

## Feature flags

| Feature | Default | Description |
|---|---|---|
| `sqlite` | **yes** | SQLite state, source, and sink support. |
| `postgres` / `pg` | — | PostgreSQL state, source, and sink support. `pg` is an alias for `postgres`. |
| `server` | — | HTTP control plane, HTTP sources, streams, dashboard. |
| `webhook` | — | Outbound webhook sinks. |
| `amqp` | — | AMQP queue sources and sinks. |
| `wasm` | — | WebAssembly task runtime. |
| `cron` | — | Cron source support. |
| `full` | — | All integrations (`server`, `webhook`, `sqlite`, `postgres`, `amqp`, `wasm`, `cron`). |

## Configuration model

Bria uses a universal namespaced config format. Shared root sections define reusable profiles; Bria's behavior lives under `[bria]`.
`[bria]` is required: unnested legacy configuration is rejected.

### Shared root sections Bria reads

| Section | Purpose |
|---|---|
| `version` | Schema version (1). |
| `[log]` | Log level, format, file defaults (inherited by Bria when not overridden). |
| `[runtime]` | Worker threads, shutdown, tmp dir, payload limits. |
| `[http]` | HTTP bind, prefix, API key defaults. |
| `[stores.<id>]` | Database profiles for state/sources/sinks. Resolved by `store = "<id>"`. |
| `[paths.<id>]` | File path profiles for sources/sinks. Resolved by `path_ref = "<id>"`. |
| `[transports.amqp.<id>]` | AMQP broker profiles for queue sources/sinks. Resolved by `transport = "<id>"`. |
| `[transports.webhook.<id>]` | Webhook destination profiles. Resolved by `transport = "<id>"`. |
| `[transports.http.<id>]` | HTTP client profiles for HTTP sources. Resolved by `transport = "<id>"`. |
| `[objects.<id>]` | Object store locations (future). |

Bria ignores `[ladon]`, `[pano]`, and `[oracles]` namespaces.

### Package-specific: `[bria]`

| Section | Purpose |
|---|---|
| `[bria.global]` | Runtime, logging, state, retry, and timeout defaults. |
| `[bria.server]` | Optional HTTP control plane. |
| `[[bria.sources]]` | Inputs that produce jobs. |
| `[[bria.tasks]]` | Reusable task definitions. |
| `[[bria.sinks]]` | Outputs that receive pipeline results. |
| `[[bria.pipelines]]` | DAGs connecting sources, tasks, and sinks. |

## Universal integration config

Shared profiles avoid duplicate configuration. Bria resolves them at load time:

```toml
[stores.bria]
driver = "sqlite"
url = "sqlite://data/bria/bria-state.db"

[paths.bria_jobs]
path = "data/bria/jobs/images.jsonl"
format = "jsonl"

[transports.amqp.local]
url = "amqp://localhost:5672"
```

Bria references them from `[bria]`:

```toml
[bria.global.state]
store = "bria"

[[bria.sources]]
id = "image-file"
type = "file"
path_ref = "bria_jobs"

[[bria.sources]]
id = "queue-jobs"
type = "queue"
transport = "local"
```

Explicit package-local values override shared profile defaults.

## Parameters

### Global: `[bria.global]`

| Key | Default | Description |
|---|---:|---|
| `worker_threads` | `0` (from `[runtime]`) | Tokio worker threads; `0` uses logical CPUs. |
| `shutdown_timeout_secs` | `30` | Orchestrator shutdown timeout. |
| `tmp_dir` | `data/bria/tmp` | Temporary file directory. |
| `max_payload_bytes` | `10485760` | Maximum job payload size. |
| `cancel_signal_ttl_secs` | `3600` | Cancel signal retention. |

### Logging: `[bria.global.log]`

| Key | Default | Values |
|---|---|---|
| `level` | `info` (from `[log]`) | `trace`, `debug`, `info`, `warn`, `error` |
| `format` | auto | `text`, `json`; auto uses text on TTY, JSON otherwise |
| `file` | `""` | Optional log file path |

### State: `[bria.global.state]`

| Key | Default | Description |
|---|---|---|
| `backend` | `memory` | `memory`, `sqlite`, or `pg`. |
| `store` | — | Store id from `[stores]`. Resolves `sqlite_path`/`pg_url`. |
| `sqlite_path` | `bria-state.db` | SQLite state database path. |
| `pg_url` | `""` | Required when `backend = "pg"`. |

State stores queued/running job records for restart recovery.

### Retry and timeout defaults

| Section | Keys |
|---|---|
| `[bria.global.retry]` | `max_attempts`, `base_delay_ms`, `max_delay_ms`, `jitter` |
| `[bria.global.timeout]` | `step_secs`, `action` (`kill`/`term`), `kill_grace_secs` |

Retry precedence: step > task > global. Backoff uses exponential delay and random jitter.

### Server: `[bria.server]`

This is Bria's internal worker/control server. Artur calls it after applying
public gateway concerns such as authentication, payment, and challenge policy.
Bria does not implement or duplicate those policies.

| Key | Default | Description |
|---|---:|---|
| `enabled` | `false` | Enable HTTP server. Requires `--features server`. |
| `bind` | `0.0.0.0` (from `[http]`) | Bind address. |
| `port` | `4000` | Listen port. |
| `prefix` | `v1` (from `[http]`) | Route prefix. |
| `api_key` | `""` | API key. Use `Authorization: Bearer` or `X-Bria-Api-Key`. |
| `dashboard_path_ref` | `""` | Path profile from `[paths]` for static dashboard. |
| `shutdown_timeout_secs` | `5` | HTTP drain timeout. |
| `max_body_bytes` | `52428800` | Server-wide body limit. |

HTTP and webhook source paths must be unique, non-empty, and must not conflict
with Bria control routes. `server.prefix` is exactly one non-empty path segment.

#### Internal HTTP submission contract

`POST /<prefix>/<source-path>` accepts JSON and returns an accepted durable job
identity:

```json
{"status":"accepted","job_id":"01...","correlation_key":"request-..."}
```

`job_id` is the identity persisted with the job lifecycle and is the value for
the cancellation route. Artur may send an opaque `Idempotency-Key` or
`X-Correlation-ID`; Bria propagates it as `correlation_key` to the worker and
state store. If both headers are sent they must match. Bria does not deduplicate
requests or make payment/challenge decisions: Artur owns those gateway policies.

### Sources: `[[bria.sources]]`

| Type | Required | Important parameters |
|---|---|---|
| `file` | `path` or `path_ref` | `poll_interval_secs`, `track_cursor`, `authoritative`, `id_field`, `max_body_bytes`, `labels` |
| `http` | `path`, `server.enabled=true` | `max_body_bytes`, `id_field`, `labels` |
| `webhook` | `path`, `server.enabled=true` | `hmac_secret`, `hmac_header`, `ack_status`, `max_body_bytes` |
| `queue` | `url` or `transport`, `exchange` | `username`, `password`, `submit_routing_key`, `cancel_routing_key`, `reconnect_secs`, `qos_prefetch`, `consumer_tag` |
| `cron` | `schedule` | `tz`, `[sources.payload]`, `labels` |
| `pg` | `url` or `store`, `[sources.table]` | `poll_interval_secs`, table column names/status values |
| `sqlite` | `path` or `store`, `[sources.table]` | same table parameters as `pg` |

### Tasks: `[[bria.tasks]]`

| Key | Default | Description |
|---|---|---|
| `id` | required | Task identifier. |
| `driver` | `local` | `local`, `docker`, or `wasm`. |
| `cmd` | required | Command, image, or `.wasm` path. |
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

### Sinks: `[[bria.sinks]]`

| Type | Required | Parameters |
|---|---|---|
| `file` | `path` or `path_ref` | `template` |
| `webhook` | `url` or `transport` | `secret`, `signature_header`, `content_type`, `max_retries`, `retry_base_ms`, `timeout_secs`, `headers` |
| `queue` | `url` or `transport`, `exchange` | `username`, `password`, `success_routing_key`, `failure_routing_key`, `reconnect_secs` |
| `pg` | `url` or `store`, `[sinks.table]` | Result table and column names |
| `sqlite` | `path` or `store`, `[sinks.table]` | Result table and column names |
| `stream` | `server.enabled=true` | `sse`, `websocket`, `ws_heartbeat_secs`, `sse_keepalive_secs`, `broadcast_capacity` |

### Pipelines and steps: `[[bria.pipelines]]`

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
| `map` | `[[steps.set]]` | Mutates `job.payload` using CEL expressions. |
| `condition` | `expr` | On false, `action = "fail"`, `"skip_to"`, or `"emit"`. |

## Templates and expressions

Templates use MiniJinja and can access `job.*`, `steps.*`, `env.*`, `now`, `now_unix`, `pipeline.*`, `result.*`, and `occurred_at` depending on context.

CEL expressions can read `job.*`, `steps.*`, and `pipeline.*`:

```toml
[[bria.pipelines.steps.set]]
target = "job.payload.output_url"
expr = '"s3://" + job.payload.bucket + "/" + job.payload.key'
```

## Example: HTTP job to local task and file sink

```toml
[bria.server]
enabled = true
port = 4000

[[bria.sources]]
id = "api"
type = "http"
path = "jobs"
id_field = "id"

[[bria.tasks]]
id = "hello"
driver = "local"
cmd = "sh"
args = ["-c", "printf '{\"message\":\"hello %s\"}' \"$1\"", "sh", "{{job.payload.name}}"]

[[bria.sinks]]
id = "results"
type = "file"
path = "results.jsonl"

[[bria.pipelines]]
id = "hello-pipeline"
source = "api"
sinks = ["results"]

[[bria.pipelines.steps]]
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

## Database backends

SQLite is the default. Use `[stores.bria]` for state/source/sink. PostgreSQL requires `--features pg` or `--features postgres`:

```toml
[stores.bria]
driver = "postgres"
url = "${DATABASE_URL}"

[bria.global.state]
backend = "pg"
store = "bria"
```

## Path and transport profiles

Bria supports:
- `path_ref` for file sources/sinks, resolving `[paths.<id>]`
- `transport` for AMQP/queue sources/sinks, resolving `[transports.amqp.<id>]`
- `transport` for webhook sinks, resolving `[transports.webhook.<id>]`
- `store` for database sources/sinks/state, resolving `[stores.<id>]`

The profile provides defaults only. Direct package-local values override profile values.

## Environment variables

Config values support both forms:
- `${VAR_NAME}` — required; config load fails if unset
- `${VAR_NAME:-default_value}` — optional; uses default if unset

## Docker

```bash
docker run --rm -p 4000:4000 \
  -v "$PWD/Config.toml:/etc/bria/Config.toml:ro" \
  ghcr.io/melonask/bria:latest
```

The default `CMD` passes `--config /etc/bria/Config.toml`. Override it:

```bash
docker run --rm bria:latest ping
```

## Development

```bash
cargo check --all-features
cargo test --all-features
```

## Testing

```bash
# Lint and unit/integration tests
cargo fmt --check
cargo clippy --all-targets --all-features -- -D warnings
cargo test

# End-to-end scenarios (requires Docker)
cd tests/e2e && ./run.sh --all
```

## License

MIT
