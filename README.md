# Bria

<img align="right" src="https://raw.githubusercontent.com/melonask/bria/refs/heads/main/logo.svg" alt="Bria logo" width="200" />

> **Briareus — One Command. Hundred Actions.**

Bria is a Rust multi-pipeline job orchestrator. It turns jobs from files, HTTP/webhooks, AMQP, cron, PostgreSQL, or SQLite into local, Docker, or WebAssembly task runs, then delivers results to files, webhooks, AMQP, databases, or live streams.

[Documentation](https://melonask.github.io/bria/) · [Getting started](https://melonask.github.io/bria/guide/getting-started) · [Configuration](https://melonask.github.io/bria/guide/configuration) · [Repository](https://github.com/melonask/bria)

## At a glance

| Area | Behavior |
|---|---|
| Flow control | One source can feed multiple pipelines; pipelines are validated DAGs with bounded queues and concurrency limits. |
| Recovery | Durable SQLite or PostgreSQL state records queued/running work for restart recovery. Recovery can repeat work; Bria provides no deduplication or exactly-once guarantee. |
| HTTP | Acceptance means that a source accepted a job, **not** that its pipeline completed. |
| Data shaping | Templates use MiniJinja; map, condition, routing, and merge expressions use CEL. |

## Requirements and installation

Rust 1.97 is required to build the crate. Runtime prerequisites depend on configuration: local executables, Docker daemon/images, WASM modules, writable files, databases, AMQP, and network access.

```bash
cargo install bria                 # default: SQLite support
cargo install bria --features full # every optional integration
```

| Feature | Default | Enables |
|---|---:|---|
| `sqlite` | yes | SQLite state, source, sink |
| `server` | no | HTTP control plane, HTTP/webhook sources, stream sinks |
| `webhook` | no | Outbound webhook sink |
| `postgres` / `pg` | no | PostgreSQL state, source, sink (`pg` aliases `postgres`) |
| `amqp` | no | AMQP queue source and sink |
| `wasm` | no | WebAssembly task driver |
| `cron` | no | Cron source |
| `full` | no | All integrations above |

### Container

Mount configuration read-only; persist any SQLite state, file cursor, output, log, and temporary paths configured within it.

```bash
docker run --rm -p 4000:4000 \
  -v "$PWD/Config.toml:/etc/bria/Config.toml:ro" \
  ghcr.io/melonask/bria:latest
```

The image default command supplies `--config /etc/bria/Config.toml`; for example, `docker run --rm ghcr.io/melonask/bria:latest ping` overrides it.

## Quick start

Start from the checked-in example rather than an unvalidated hand-written file:

```bash
cp Config.example.toml Config.toml
bria check --config Config.toml
bria --config Config.toml
```

The example has an enabled file source/pipeline and disabled HTTP/AMQP/webhook examples. Adjust existing paths, executables, and integration settings before running it. For a minimal HTTP setup, enable `[bria.server]` and the `image-http` source in that example, build with `server`, then submit JSON:

```bash
curl --request POST http://localhost:4000/v1/jobs/images \
  --header 'content-type: application/json' \
  --data '{"name":"Bria"}'
```

## CLI reference

| Invocation | Behavior | stdout / exit |
|---|---|---|
| `bria ping` | Does not load configuration. | `pong`; zero on success |
| `bria check [--config PATH]` | Loads, resolves substitutions/profiles, and strictly validates; never starts workers. | `Configuration is valid: <path>`; zero on success |
| `bria [--config PATH]` | Loads and validates config, then runs the orchestrator/server. | diagnostics on stderr and non-zero on error |

`--config PATH` is global, including after `check`. It takes precedence over `BRIA_CONFIG`; otherwise the path is `Config.toml`. A validation or runtime failure is printed as `Error: …` on stderr and exits 1. Standard Clap help/version behavior applies. `ping` does not read the selected file.

## Configuration model

`version = 1` and `[bria]` are required; legacy unnested Bria configuration is rejected. Root profiles are resolved at load time; direct Bria values override profile defaults.

| Root section | Reference | Purpose |
|---|---|---|
| `[log]`, `[runtime]`, `[http]` | inherited | Logging, runtime, server defaults |
| `[stores.<id>]` | `store` | SQLite/PostgreSQL state, sources, sinks |
| `[paths.<id>]` | `path_ref` | File sources and sinks |
| `[transports.amqp.<id>]` | `transport` | Queue sources and sinks |
| `[transports.webhook.<id>]` | `transport` | Webhook sinks |
| `[transports.http.<id>]` | `transport` | HTTP sources |

`${NAME}` requires an environment variable; `${NAME:-value}` uses a default. Keep secrets in the runtime environment.

| Section | Important parameters / defaults |
|---|---|
| `[bria.global]` | `worker_threads=0` (logical CPUs), `shutdown_timeout_secs=30`, `tmp_dir=data/bria/tmp`, `max_payload_bytes=10485760`, `cancel_signal_ttl_secs=3600` |
| `[bria.global.log]` | `level=info`, `format` (`text`/`json`, automatic by TTY), `file` |
| `[bria.global.state]` | `backend=memory` (`memory`, `sqlite`, `pg`); `store`, `sqlite_path=bria-state.db`, `pg_url` |
| `[bria.global.retry]` | `max_attempts`, `base_delay_ms`, `max_delay_ms`, `jitter` |
| `[bria.global.timeout]` | `step_secs`, `action` (`kill`/`term`), `kill_grace_secs` |
| `[bria.server]` | `enabled=false`, `bind=0.0.0.0`, `port=4000`, `prefix=v1`, `api_key`, `max_body_bytes=52428800`, drain timeout |

Retry precedence is step, task, global; task retries use exponential backoff and jitter. Timeout precedence is step, task, global. `term` sends SIGTERM on Unix, waits the grace period, then kills; `kill` kills immediately.

### State, recovery, and results

`memory` state is lost on restart. SQLite/PG preserve queued/running records and re-enqueue incomplete work for the recorded pipeline at startup. Restore a compatible pipeline before restarting if recovery reports an unknown pipeline. Do not delete state to “fix” recovery; repeated execution is possible.

A pipeline result has `pipeline_id`, `job`, `status` (`success`/`failure`), `duration_ms`, `steps`, and ISO-8601 `occurred_at`. Each task step result has `exit_code`, `duration_ms`, one-indexed `attempt`, optional captured `stdout`/`stderr`, and JSON `outputs` parsed from stdout when available.

## Sources, tasks, pipelines, sinks, templates, and expressions

| Kind | Types / required configuration |
|---|---|
| Sources | `file` (`path`/`path_ref`), `http`/`webhook` (`path`, server), `queue` (`url`/`transport`, `exchange`), `cron` (`schedule`), `pg`/`sqlite` (connection/store and table mapping) |
| Tasks | `local`, `docker`, `wasm`; `id`, `cmd`, `args`, env, working directory, stdin/stdout/stderr, exit codes, timeout, retry |
| Sinks | `file`, `webhook`, `queue`, `pg`, `sqlite`, `stream` |

File sources can track cursors. HTTP/webhook paths must be non-empty, unique, and not overlap control routes. Webhook sources accept `hmac_secret`, optional `hmac_header`, and `ack_status`; queue sources expose submit/cancel routing keys, reconnect, and prefetch. Source-specific `max_body_bytes` bounds HTTP input.

Tasks default to `local`; `stdin.mode` is `none`, `payload`, or `template`; stdout/stderr modes are `capture`, `stream`, or `discard`, with `max_bytes`. Docker accepts `flags`, `mounts`, and `pull` (`always`, `missing`, `never`); WASM accepts `dirs`, `max_memory_pages`, and `fuel`.

A pipeline has `id`, `source` or `sources`, `concurrency`, `queue_capacity`, `sinks`, labels, failure policy, and steps. A full queue blocks routing; choose source polling/prefetch and concurrency accordingly. Steps are `process` (`task`), `map` (`[[steps.set]]`), and `condition` (`expr`). `depends_on` defines the DAG; omitted dependencies use the preceding configured step, while `depends_on = []` is independent. A false condition can `fail`, `skip_to`, or `emit`. Failure actions are `discard`, `dead_letter` (with sink), and `stop`.

Multiple sources require `[pipelines.merge]` with exactly one correlation field/expression. `any` forwards immediately; `all` buffers one job per source, discards timed-out incomplete groups, and merges the group. The first duplicate source job and first conflicting top-level value win.

```toml
[[bria.pipelines.steps.set]]
target = "job.payload.output_url"
expr = '"s3://" + job.payload.bucket + "/" + job.payload.key'
```

MiniJinja fields can use `job.*`, `steps.*`, `env.*`, `now`, `now_unix`, `pipeline.*`, `result.*`, and `occurred_at` where applicable. CEL reads `job.*`, `steps.*`, and `pipeline.*`. Validate both with representative missing and hostile-looking values; do not build shell commands from untrusted payloads.

File sinks append JSON results (or `template`) and create parent directories. Database sinks write per-step fields: result/job/pipeline/step IDs, timestamp, exit code, stdout, stderr, duration, attempt, and status. Webhook sinks POST the serialized result, optionally sign its body with hex HMAC-SHA256, and retry failures/timeouts with exponential delay. Stream sinks emit live SSE/WebSocket broadcasts; lagged subscribers lose events and there is no replay.

## HTTP API

Enable `server` and `[bria.server]`; all routes are `/<prefix>` (default `/v1`). A non-empty `api_key` requires either `Authorization: Bearer <key>` or `X-Bria-Api-Key: <key>` on every route.

| Request | Success | Response |
|---|---|---|
| `GET /<prefix>/ping` | 200 | plain `pong` |
| `POST /<prefix>/<source-path>` | HTTP: 201; webhook: `ack_status` | accepted JSON |
| `DELETE /<prefix>/<source-path>/<job_id>` | 202 | cancellation JSON |
| `POST /<prefix>/pipelines/<pipeline_id>/resume` | 200 | resumed JSON |

```json
{"status":"accepted","job_id":"01J...","correlation_key":"request-42"}
```
```json
{"status":"cancellation_requested","job_id":"01J..."}
```
```json
{"status":"resumed","pipeline_id":"<id>"}
```

`Idempotency-Key` and `X-Correlation-ID` are optional opaque correlation metadata: non-empty visible ASCII, at most 512 bytes, and equal when both are sent. They are propagated as `correlation_key`; they do **not** deduplicate requests. A source `id_field` may supply `job_id`, otherwise Bria generates a ULID.

For webhook sources, Bria computes HMAC-SHA256 over the exact raw request body and compares it in constant time. The signature header defaults to `X-Bria-Signature`; raw hexadecimal or `sha256=<hex>` is accepted. A configured secret makes a missing/bad signature 401.

| Condition | Status | Practical response |
|---|---:|---|
| Invalid JSON or invalid/mismatched correlation headers | 400 | text diagnostic |
| Missing/invalid API key or webhook HMAC | 401 | text diagnostic |
| Unknown source/pipeline | 404 | text diagnostic |
| Global or source body limit exceeded | 413 | text diagnostic |
| Accepted submission | 201 / configured webhook ack | accepted JSON |

Cancellation is a retained signal checked before queued execution; it cannot undo completed work or promise interruption of a running task. Signals expire after `cancel_signal_ttl_secs`; cancelled jobs are not sent to sinks. A `stop` failure pauses indefinitely until the resume route is called, then returns the original failure result—repair the fault first.

## Library API

The crate exports `Config`, `Cli`, `Orchestrator`, `run`, `run_pipeline_once`, `run_pipeline_once_with_config`, `Job`, `Context`, `PipelineResult`, `StepResult`, `StateStore`, and `create_store`. `bria::run(cli).await` implements the CLI flow: ping, load, validate, check, or construct/run the orchestrator.

## Deployment, reliability, security, and troubleshooting

- Deploy an immutable binary/config pair with matching features. Use persistent writable volumes for durable state, cursors, outputs, logs, and tmp files.
- On shutdown Bria stops routing, drains the server, then allows routers/workers/merge cleanup up to configured timeouts; over-budget components may be aborted. Do not force-kill without operator approval.
- Never expose the internal server, stream routes, Docker socket/mounts, stores, queues, or outbound webhook capability directly to untrusted input. Keep `inherit_env=false`, pin Docker images, and restrict WASI preopens.
- Do not claim exactly-once delivery, request deduplication, durable streams, cancellation of active tasks, or completion from acceptance.

| Symptom | Check / recovery |
|---|---|
| Validation or unsupported integration error | Run `bria check` with the deployed binary; match configured integration to a compiled feature. |
| HTTP failure | Check prefix/path, API key, JSON/body limits, correlation headers, and raw-body HMAC. |
| No result | Check source-to-pipeline reference, queue/concurrency, DAG/condition, task logs/output limit/timeout, and sink reachability. |
| Stalled merge | Confirm every source emits the same correlation group before `timeout_secs`. |
| Stopped pipeline or recovery | Fix the cause before resume; preserve durable state and assess repeated external effects. |

## Development and verification

```bash
cargo fmt --all -- --check
cargo clippy --all-targets --no-default-features --locked -- -D warnings
cargo clippy --all-targets --all-features --locked -- -D warnings
cargo test --all-targets --no-default-features --locked
cargo test --all-targets --all-features --locked

# Docker-backed end-to-end scenarios
cd tests/e2e && ./run.sh --all
```

The E2E suite exercises sources, sinks, state backends, recovery, cancellation, 413 input limits, condition failures, and webhook HMAC.

## License

MIT
