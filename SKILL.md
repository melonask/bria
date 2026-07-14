---
name: bria
description: Operate, author, validate, test, deploy, and troubleshoot Bria configurations, pipelines, sources, tasks, sinks, state backends, and internal HTTP endpoints.
---

# Bria agent operations manual

## Purpose and boundaries

Bria is a Rust multi-pipeline job orchestrator: sources produce JSON jobs, pipelines run task DAGs, and sinks receive results. Use this manual for safe configuration and operations work.

Bria is not a public gateway and makes no payment, authorization, challenge, or request-deduplication decision. Do not claim exactly-once delivery, idempotency, durable stream replay, active-task cancellation, or pipeline completion from HTTP acceptance.

## Command selection

| Goal | Command | Expected result |
|---|---|---|
| Confirm binary health | `bria ping` | `pong`; config is not read |
| Validate a config | `bria check --config /absolute/path/Config.toml` | `Configuration is valid: <path>` and exit 0 |
| Run workers/server | `bria --config /absolute/path/Config.toml` | Runs until shutdown; errors print to stderr and exit 1 |
| Use deployment-selected config | `BRIA_CONFIG=/etc/bria/Config.toml bria check` | Environment selects config when `--config` is absent |
| Run repository checks | commands in [Verification](#verification-checklist) | Run from repository root |

`--config` takes precedence over `BRIA_CONFIG`; default is `Config.toml`. Validate before every run and after every edit.

## Prerequisites and features

Build/run the exact feature set required by config. Rust 1.97 is required to build.

| Capability | Feature |
|---|---|
| SQLite state/source/sink | `sqlite` (default) |
| HTTP control plane, HTTP/webhook source, stream sink | `server` |
| Outbound webhook sink | `webhook` |
| PostgreSQL state/source/sink | `postgres` or `pg` |
| AMQP queue source/sink | `amqp` |
| WASM task | `wasm` |
| Cron source | `cron` |
| All optional integrations | `full` |

Confirm runtime executables, Docker daemon/images, WASM paths, writable directories, database/broker permissions, and network reachability. Never probe production services or credentials merely to learn behavior.

## Safe workflow and exact commands

1. Identify the absolute config, binary/features, profiles, environment variables, persistent state/cursor paths, and target environment.
2. Preserve existing state, cursors, queues, tables, profile references, and secrets; make the smallest change.
3. Validate and use isolated endpoints/paths for a smoke test:

```bash
bria check --config /absolute/path/Config.toml
bria --config /absolute/path/Config.toml
```

4. Inspect logs plus configured state and sink output. Stop with SIGINT and allow configured server/global drain time. Do not force-kill a draining process without operator direction.
5. Before production cutover, confirm feature compatibility, persistent volumes, secret injection, database/broker topology, webhook reachability, and rollback/recovery plan.

## Config editing and resolution

`version = 1` and `[bria]` are required; legacy unnested Bria config is rejected. `${NAME}` is required environment expansion and `${NAME:-default}` supplies a default. Keep secrets in runtime secret storage, never committed TOML, arguments, templates, logs, or agent output.

| Shared section | Reference | Resolution |
|---|---|---|
| `[stores.<id>]` | `store` | SQLite/PostgreSQL state, sources, sinks |
| `[paths.<id>]` | `path_ref` | File sources/sinks |
| `[transports.amqp.<id>]` | `transport` | Queue sources/sinks |
| `[transports.webhook.<id>]` | `transport` | Webhook sinks |
| `[transports.http.<id>]` | `transport` | HTTP sources |

`[log]`, `[runtime]`, and `[http]` supply inherited defaults. Explicit Bria configuration overrides profile/default values. Under `[bria]`, configure `global`, `server`, `sources`, `tasks`, `sinks`, and `pipelines`.

## Sources, tasks, pipelines, sinks, and HTTP contracts

### Copyable HTTP pipeline

```toml
[bria.server]
enabled = true
port = 4000

[[bria.sources]]
id = "api"
type = "http"
path = "jobs"

[[bria.tasks]]
id = "greet"
driver = "local"
cmd = "printf"
args = ["hello %s", "{{job.payload.name}}"]

[[bria.sinks]]
id = "results"
type = "file"
path = "results.jsonl"

[[bria.pipelines]]
id = "greetings"
source = "api"
queue_capacity = 128
sinks = ["results"]

[[bria.pipelines.steps]]
id = "run"
type = "process"
task = "greet"
```

```bash
curl --request POST http://localhost:4000/v1/jobs \
  --header 'content-type: application/json' \
  --header 'idempotency-key: request-42' \
  --data '{"name":"Bria"}'
```

| Component | Contract |
|---|---|
| Source | Unique `id`; types: `file`, `http`, `webhook`, `queue`, `cron`, `pg`, `sqlite`. HTTP/webhook require enabled server plus a unique non-empty path. File uses `path`/`path_ref`; `authoritative = true` treats each complete read as authoritative and cancels IDs removed from its input; queue needs URL/transport and exchange; cron needs schedule; DB sources need connection/store and table mapping. |
| Task | `local`, `docker`, or `wasm`; requires `id` and `cmd`. Use explicit args/env/working dir, exit codes, finite timeout, retry, stdin (`none`/`payload`/`template`), and stdout/stderr (`capture`/`stream`/`discard`) byte limits. |
| Pipeline | `source` or `sources`, bounded `queue_capacity`, `concurrency`, sinks, labels, failure policy, and DAG steps. Process needs `task`; map uses sets; condition uses CEL `expr`. Omitted `depends_on` means prior configured step; use `depends_on = []` for independent work. |
| Sink | `file`, `webhook`, `queue`, `pg`, `sqlite`, `stream`. File appends result JSON/template; database stores per-step result fields; stream is live broadcast only. |

Full queues block routing: deliberately align file polling/AMQP prefetch with pipeline concurrency. Retry precedence is step, task, global; retries use `max_attempts`, exponential backoff (`base_delay_ms` to `max_delay_ms`), and jitter. Retry only idempotent or externally deduplicated effects. Timeout precedence is step, task, global; `term` sends SIGTERM on Unix then kills after `kill_grace_secs`, while `kill` is immediate. Output-limit and timeout failures follow task retry/failure policy.

Do not allow untrusted values to choose task commands, Docker flags/mounts, working dirs, WASM paths/preopens, or templates. Keep `inherit_env=false` unless reviewed. Pin images; do not mount sensitive host paths/Docker socket. WASI `dirs` grant host filesystem access, so restrict them.

MiniJinja supports `job.*`, `steps.*`, `env.*`, `now`, `now_unix`, `pipeline.*`, `result.*`, and `occurred_at` where available. CEL map/condition/route/merge expressions read `job.*`, `steps.*`, and `pipeline.*`. Test valid, missing, null, and hostile-looking data.

`sources` requires merge config with exactly one correlation key/expression. `any` forwards each job; `all` buffers one per source/correlation group and discards incomplete timed-out groups. The first duplicate source job and first conflicting top-level field win.

### State, cancellation, and results

`memory` has no restart recovery. SQLite/PG record queued/running lifecycle records and re-enqueue incomplete work at startup; recovery may rerun external effects. If a recovered pipeline is unknown, restore compatible configuration before restarting. Never delete durable state/cursors to bypass recovery.

Cancellation returns a signal, checked before queued execution; it cannot undo completed work or guarantee interruption of a running task. The HTTP cancellation route is dynamically registered as `DELETE /<prefix>/<configured-http-or-webhook-source-path>/<job_id>`; queue and authoritative-file sources produce equivalent signals. Signals expire after `cancel_signal_ttl_secs`; cancelled jobs are not sent to sinks. `failure.action = "stop"` waits indefinitely for resume, then returns the original failure result—repair first.

Pipeline results contain `pipeline_id`, `job`, `status` (`success`/`failure`), `duration_ms`, `steps`, and ISO-8601 `occurred_at`. A task `StepResult` contains `exit_code`, `duration_ms`, one-indexed `attempt`, optional captured `stdout`/`stderr`, and parsed JSON `outputs`.

### HTTP, authentication, HMAC, and response shapes

Routes are `/<prefix>` (`v1` default). A non-empty `api_key` protects every route with `Authorization: Bearer <key>` or `X-Bria-Api-Key: <key>`.

| Request | Status | Shape / meaning |
|---|---:|---|
| `GET /v1/ping` | 200 | plain `pong` |
| `POST /v1/<source-path>` | HTTP 201; webhook `ack_status` | `{"status":"accepted","job_id":"…","correlation_key":"…"}`; acceptance is not completion |
| `DELETE /v1/<source-path>/<job_id>` | 202 | `{"status":"cancellation_requested","job_id":"…"}` |
| `POST /v1/pipelines/<pipeline_id>/resume` | 200 | `{"status":"resumed","pipeline_id":"…"}` |
| Invalid JSON/correlation header | 400 | text diagnostic |
| Bad API key or configured webhook HMAC | 401 | text diagnostic |
| Unknown source/pipeline | 404 | text diagnostic |
| Body over source/global limit | 413 | text diagnostic |

`Idempotency-Key`/`X-Correlation-ID` are optional non-empty visible-ASCII values (max 512 bytes); when both are supplied they must match. Bria stores them as `correlation_key` but does **not** deduplicate.

Set `bria.server.dashboard_path_ref` to a `[paths.<id>]` directory to serve its static dashboard at `/<prefix>/dashboard`. Dashboard files use the same server authentication policy as every other route.

Webhook source HMAC is SHA-256 over the exact raw body, compared in constant time. With `hmac_secret`, use the configured header or `X-Bria-Signature`; raw hex and `sha256=<hex>` are accepted. Outbound webhook sinks POST serialized results, optionally attach hex HMAC-SHA256, and retry failed/timeout requests using exponential delay. Streams can drop events for lagged clients.

## Diagnosis and recovery

| Symptom | Diagnose and recover |
|---|---|
| Config/feature error | Run exact `bria check`; build/install required feature rather than removing integration blindly. |
| HTTP failure | Check prefix/path uniqueness, auth, JSON/body limit, matching correlation headers, and HMAC over exact bytes. |
| No output | Check source→pipeline references, queue/concurrency, dependencies/condition, task exit/output/timeout logs, then sink reachability. |
| Stalled merge | Confirm all configured sources emit the same correlation group before timeout. |
| Stopped pipeline | Correct failure cause, then call resume; it does not convert the failed job to success. |
| Restart recovery | Preserve state and logs, assess replay safety, restore missing pipeline IDs, then obtain operator approval for remediation. |

## Security, reliability, and prohibited actions

- Use durable state and persistent writable volumes when recovery is required; inspect downstream effects after restart.
- Do not expose server/stream routes, state stores, brokers, webhooks, Docker socket, or host mounts to untrusted users without an external security boundary.
- Do not commit or disclose secrets; do not reset cursors, purge queues, alter tables/schemas, delete state, or force-kill active work without explicit operator approval.
- Do not resume a stopped pipeline before correcting the cause. Do not assume a submission is deduplicated or a cancellation interrupts active work.

## Verification checklist

```bash
cargo fmt --all -- --check
cargo clippy --all-targets --no-default-features --locked -- -D warnings
cargo clippy --all-targets --all-features --locked -- -D warnings
cargo test --all-targets --no-default-features --locked
cargo test --all-targets --all-features --locked
cd tests/e2e && ./run.sh --all
```

- [ ] Exact binary/features, absolute config, profiles, and environment identified.
- [ ] `bria check --config <absolute-path>` passes with deployment features.
- [ ] Sources, tasks, paths, stores, transports, sinks, and permissions tested in isolation.
- [ ] Queue capacity, concurrency, payload/output limits, retries, timeouts, merge timeout, and failure action are intentional.
- [ ] Templates/CEL and Docker/WASM/task boundaries reviewed for untrusted input.
- [ ] HTTP auth/HMAC, no-dedup behavior, cancellation, resume, state recovery, and shutdown budget understood.
