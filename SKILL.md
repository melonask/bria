---
name: bria
description: Use when an agent must inspect, create, validate, run, test, deploy, or troubleshoot a Bria configuration, pipeline, source, task, sink, state backend, or internal HTTP control endpoint.
---

# Bria agent operations manual

## Purpose and boundaries

Bria is a Rust multi-pipeline job orchestrator: sources create jobs, pipelines execute task DAGs, and sinks receive results. Use this manual for operationally safe, implementation-accurate work on Bria configuration and runtime behavior.

It is **not** a public gateway. Bria does not deduplicate submissions, or make authentication, payment, authorization, or challenge decisions. Put those policies in the caller/gateway. Do not infer delivery guarantees beyond the configured source, state backend, and sink behavior.

## Prerequisites and feature compatibility

Use the exact binary and feature set that will run the configuration. The default feature is `sqlite`; `server`, `webhook`, `postgres` (or alias `pg`), `amqp`, `wasm`, and `cron` are opt-in; `full` enables all.

| Configuration/runtime capability | Required feature |
|---|---|
| SQLite state, source, or sink | `sqlite` |
| HTTP control plane, HTTP/webhook sources, stream sinks | `server` |
| Outbound webhook sink | `webhook` |
| PostgreSQL state, source, or sink | `postgres` or `pg` |
| AMQP queue source or sink | `amqp` |
| WebAssembly task | `wasm` |
| Cron source | `cron` |

The crate requires Rust 1.97. Ensure referenced executables, Docker daemon/images, WASM modules, files/directories, databases, brokers, and network access are available in the actual execution environment. Do not test against production services or credentials merely to explore behavior.

## Mandatory check, run, and change workflow

1. Identify the absolute configuration path and the intended binary/features. `--config` overrides `BRIA_CONFIG`; without either, the default is `Config.toml`.
2. Preserve existing state paths, source cursors, database tables, queues, credentials, and profile references. Make the smallest configuration change.
3. Validate before every run and after every edit:

   ```bash
   bria check --config /absolute/path/to/Config.toml
   ```

   This loads environment substitutions, resolves profiles, and validates identifiers, references, feature requirements, routes, DAGs, retries, failure routing, and merge configuration. Success prints `Configuration is valid: <path>` and exits zero; diagnostics go to stderr and fail the command.
4. For a safe non-production smoke test, use isolated paths/endpoints and run the same binary:

   ```bash
   bria --config /absolute/path/to/Config.toml
   ```

5. Inspect logs and the configured state/sink output, then stop with SIGINT. Allow the server and pipelines to drain for their configured shutdown timeouts. Do not force-kill a draining process without operator direction.

`bria ping` prints `pong` and does not read configuration.

## Configuration model

`version = 1` and `[bria]` are required; legacy unnested Bria configuration is rejected. Bria reads these reusable root profiles and resolves them at load time:

| Root section | Reference field | Use |
|---|---|---|
| `[stores.<id>]` | `store` | SQLite/PostgreSQL state, sources, and sinks |
| `[paths.<id>]` | `path_ref` | File sources and sinks |
| `[transports.amqp.<id>]` | `transport` | AMQP sources and sinks |
| `[transports.webhook.<id>]` | `transport` | Webhook sinks |
| `[transports.http.<id>]` | `transport` | HTTP sources |

`[log]`, `[runtime]`, and `[http]` provide inherited defaults. Explicit Bria values override profile/default values. Keep profile references intact unless intentionally changing the resolved integration.

Under `[bria]`, use `[bria.global]` (runtime/logging/state/retry/timeout defaults), `[bria.server]`, `[[bria.sources]]`, `[[bria.tasks]]`, `[[bria.sinks]]`, and `[[bria.pipelines]]`. Environment expansion supports `${NAME}` (required) and `${NAME:-default}`. Supply secrets through the runtime environment, not command lines, committed config, logs, task arguments, or agent output.

### State and recovery

`[bria.global.state]` selects `memory`, `sqlite`, or `pg`. A durable SQLite/PostgreSQL backend records queued and running jobs and re-enqueues incomplete records for their recorded pipeline at startup. Recovery can re-execute work; design tasks and external effects to tolerate this. Memory state has no restart recovery. If recovery logs an unknown pipeline, restore a compatible pipeline configuration before restarting rather than accepting discarded recovered work.

## Sources, tasks, pipelines, and sinks

### Sources

Every source has a unique `id`; pipelines reference it by id. Supported types are `file`, `http`, `webhook`, `queue`, `cron`, `pg`, and `sqlite`.

* File uses `path` or `path_ref`; use `track_cursor` deliberately and do not delete/reset its state casually.
* HTTP and webhook require `server.enabled = true` and a unique, non-empty path that does not conflict with a control route. Payloads are JSON and source labels become job labels.
* Webhook sources can verify an HMAC-SHA256 of the raw body. Configure a secret and header; the default header is `X-Bria-Signature`, and raw hex or `sha256=<hex>` is accepted.
* Queue requires `url` or AMQP `transport` plus `exchange`; use distinct submit/cancel routing keys and bounded prefetch appropriate to downstream capacity.
* Cron requires `schedule`; database sources require a URL/path or store and their table mapping.

The router rejects a produced payload exceeding `global.max_payload_bytes`. A source may feed more than one pipeline; each eligible pipeline receives the job.

### Tasks and execution safety

A reusable task has `id`, `driver` (`local`, `docker`, or `wasm`), `cmd`, argument templates, optional environment, working directory, stdin/stdout/stderr policy, accepted exit codes, timeout, and retry policy. A process step can override task execution values through its `with` configuration.

Treat task definitions and templated job fields as executable-input boundaries:

* Never permit untrusted users to set `cmd`, Docker flags/mounts, working directories, WASM paths/preopened directories, or templates. Rendering an argument does not make a shell invocation safe; avoid shell interpreters unless the command and all interpolated values are controlled.
* Keep `inherit_env = false` unless a reviewed dependency requires inheritance. Pass a minimal explicit environment and never render secrets into output, arguments, or logs.
* Use explicit working directories, finite timeouts, accepted exit codes, captured-output limits, and least-privilege filesystem/network access supplied by the host/container platform.
* Docker runs the local `docker` CLI with configured flags/mounts/pull policy. Pin and review images; never mount sensitive host paths or the Docker socket for untrusted workloads. Bria does not itself impose a container security policy.
* WASM uses WASI Preview 1. `dirs` preopens host directories with full directory/file permissions, so expose only required directories. Set finite `max_memory_pages`, `fuel`, and a timeout. WASM access is limited to its configured WASI context, except for explicitly preopened directories.

For `stdin.mode`, use `none`, `payload`, or `template`. For stdout/stderr use `capture`, `stream`, or `discard`; set `max_bytes` to prevent result/log growth. Captured output above its limit fails the task.

### Pipelines, DAGs, conditions, and failure handling

A pipeline declares `id`, one `source` or multiple `sources`, `concurrency`, finite `queue_capacity`, pipeline sinks, labels, failure behavior, and `[[bria.pipelines.steps]]`. `queue_capacity` creates a bounded channel; when it is full, the router waits. `concurrency` bounds concurrently executing jobs for that pipeline.

Steps are `process` (requires `task`), `map` (one or more `set` operations), or `condition` (requires `expr`). `depends_on` creates the DAG; omitted dependencies mean each step depends on the immediately preceding configured step, so declare `depends_on = []` for an intentionally independent step. Cycles, duplicate ids, unknown dependencies/tasks/sinks, and invalid condition routing fail validation. Ready DAG levels can execute in parallel.

A false condition uses `action = "fail"` by default. `skip_to` requires a valid target and changes execution to that step; `emit` completes the condition and ends the pipeline successfully. Pipeline/step failure actions are `discard`, `dead_letter`, or `stop`; `dead_letter` requires a sink. Failure results go to configured failure/pipeline/step routing sinks as applicable; cancelled jobs are not dispatched to sinks.

### Merge semantics

Multiple sources require `[bria.pipelines.merge]`. Set exactly one of `correlation_key` or `correlation_expr`; strategies are `any` and `all`.

* `any` immediately forwards each source job. With `correlation_key`, Bria derives the job correlation key from that top-level payload field.
* `all` buffers one job from each source per correlation group until every configured source arrives. A duplicate source in a group keeps the first job. Groups older than `timeout_secs` are discarded during periodic cleanup; they do not emit a partial job.
* The merged job has a new id and `source = "merge:<pipeline-id>"`. Its payload combines top-level object keys (first conflicting value wins), includes `sources` keyed by source id and a `jobs` array, and includes the configured correlation value when applicable.

## Templating and CEL

MiniJinja templates are used in task arguments/environment/stdin and supported sink fields. Context availability depends on the field and includes `job.*`, `steps.*`, `env.*`, `now`, `now_unix`, `pipeline.*`, `result.*`, and `occurred_at`. Render failures fail the affected work. CEL is used for map assignments, conditions, routes, and merge correlation expressions; it can read `job.*`, `steps.*`, and `pipeline.*`. Use CEL to transform structured data, not to encode shell commands or secrets. Validate every template/expression against representative valid, missing, null, and hostile-looking payload values.

## Result sinks and delivery

Sinks are `file`, `webhook`, `queue`, `pg`, `sqlite`, or `stream`. A pipeline selects pipeline-level sinks; step routing and failure configuration can select additional sinks. File/database paths and table/column mappings must be writable and compatible before deployment. Queue sinks require an AMQP exchange/routing keys. Webhook sinks support configured headers, HMAC signature, timeout, and retry settings; use an isolated receiving endpoint to test them.

Stream sinks require `server`; configured SSE and WebSocket paths publish live broadcast events. They are not a durable replay interface: lagged subscribers skip events. Secure server access before exposing stream routes.

## HTTP control plane

With `server.enabled = true` and the `server` feature, routes are rooted at `/<prefix>` (default `v1`). A non-empty `api_key` protects all routes using either `Authorization: Bearer <key>` or `X-Bria-Api-Key: <key>`.

| Request | Meaning | Success response |
|---|---|---|
| `GET /<prefix>/ping` | Health check | `200` body `pong` |
| `POST /<prefix>/<source-path>` | Submit JSON to an HTTP/webhook source | HTTP source: `201`; webhook: configured `ack_status` |
| `DELETE /<prefix>/<source-path>/<job_id>` | Request cancellation | `202` |
| `POST /<prefix>/pipelines/<pipeline_id>/resume` | Resume a pipeline stopped by failure action | `200` |

Submission response:

```json
{"status":"accepted","job_id":"01J...","correlation_key":"request-42"}
```

Acceptance is source enqueueing, not pipeline completion. The job id is the cancellation identity. Invalid JSON is `400`, an over-limit source body is `413`, and an unknown source/pipeline is `404`. For a webhook with configured HMAC, a missing or invalid signature is `401`.

Cancellation response:

```json
{"status":"cancellation_requested","job_id":"01J..."}
```

It is observed before queued execution and cannot undo a completed task. Cancellation signals expire after `cancel_signal_ttl_secs`; do not assume they interrupt an already running task. A resume response is `{"status":"resumed","pipeline_id":"<id>"}`. `stop` waits indefinitely for this endpoint after a failure, then returns the original failure result; repair the cause before resuming.

`Idempotency-Key` and `X-Correlation-ID` are optional opaque correlation metadata. Each must be non-empty visible ASCII and at most 512 bytes; if both are supplied, they must match. Bria stores/propagates the chosen value as `correlation_key` but does **not** deduplicate requests.

## Backpressure, retries, and timeouts

Use finite `queue_capacity`; the runtime defensively treats zero as one. Size source poll/prefetch and pipeline concurrency for downstream capacity. A full pipeline queue blocks routing, so one slow consumer can intentionally backpressure its source router.

Retry precedence is step, then task, then global. `max_attempts`, exponential backoff (`base_delay_ms` through `max_delay_ms`), and jitter apply to task attempts; jitter must be in `[0.0, 1.0]`. Choose retry counts only for idempotent or externally deduplicated effects.

Timeout precedence is step override, task, then `[bria.global.timeout]`. `action = "kill"` kills immediately; `"term"` sends SIGTERM on Unix, waits `kill_grace_secs`, then kills if needed. Timeouts and output-limit failures are task failures and follow retry/failure policy. Set both input (`max_payload_bytes`/source `max_body_bytes`) and output limits.

## Deployment, shutdown, and recovery

Deploy the binary and configuration as an immutable, feature-compatible unit. Provide `${...}` values through the deployment secret mechanism; use writable persistent volumes for durable SQLite state, file cursors/results, logs, and tmp files. Confirm database migrations/permissions, broker topology, outbound webhook reachability, and server bind/prefix/API-key settings before cutover.

On SIGINT, Bria stops accepting/router work, drains the HTTP server for `server.shutdown_timeout_secs`, then gives routers, pipeline workers, and merge cleanup up to `global.shutdown_timeout_secs`. It may abort components that exceed the drain timeout. Plan task timeouts and graceful shutdown values accordingly. On restart, inspect durable-state recovery logs and downstream effects before allowing repeat processing.

## Troubleshooting and recovery

1. Re-run `bria check` with the exact deployed binary/config and verify all required environment variables are present.
2. Match any unsupported integration error to the compiled feature set; rebuild/install with the required feature rather than removing the integration blindly.
3. For HTTP failures, verify prefix/path uniqueness, API key, JSON/body limits, correlation header agreement, and webhook HMAC over the exact raw request body.
4. For no result, verify source-to-pipeline references, pipeline queue/concurrency, DAG dependencies, condition action, task exit/output/timeout logs, and sink reachability.
5. For stalled merge, verify every source emits the same correlation group before `timeout_secs`; inspect top-level correlation field types/values or the CEL match expression.
6. For stopped pipelines, correct the failed task/configuration/dependency first, then call the resume endpoint; resuming does not turn the failed job into success.
7. For restart recovery, do not delete or overwrite durable state. Determine whether re-execution is safe, restore any missing pipeline ids, and preserve evidence/logs before operator-approved remediation.

## Required repository test commands

Run these from the repository root after code changes, plus `bria check` for each edited configuration:

```bash
cargo fmt --all -- --check
cargo clippy --all-targets --no-default-features --locked -- -D warnings
cargo clippy --all-targets --all-features --locked -- -D warnings
cargo test --all-targets --no-default-features --locked
cargo test --all-targets --all-features --locked
```

## Prohibited actions

* Do not run an unvalidated or feature-incompatible configuration.
* Do not expose the internal server, stream routes, Docker socket, host mounts, state stores, brokers, or webhooks to untrusted input without an appropriate external security boundary.
* Do not place secrets in source control, templates, task arguments, logs, agent responses, or test fixtures.
* Do not reset cursors, delete durable state, purge queues, alter schemas/tables, or force-kill active work without explicit operator approval.
* Do not claim exactly-once processing, idempotency, durable stream replay, cancellation of active tasks, or pipeline success from HTTP acceptance.
* Do not resume a stopped pipeline before correcting its failure cause.

## Final checklist

- [ ] Exact binary, Rust/features, config path, profiles, and environment are identified.
- [ ] `bria check --config <absolute-path>` succeeds with the deployment feature set.
- [ ] Sources, paths, stores, transports, task dependencies, sinks, and permissions are reachable in an isolated test.
- [ ] Queues, concurrency, payload/output limits, retries, timeouts, failure actions, and merge timeout are deliberate.
- [ ] Tasks, Docker mounts/images, WASM preopens, templates, CEL, and secrets have been reviewed for untrusted input exposure.
- [ ] HTTP auth/HMAC, correlation semantics, cancellation expectations, and resume procedure are understood.
- [ ] Durable state, logs, tmp/output paths, rollback/recovery plan, and graceful shutdown budget are preserved.
- [ ] Required tests and a safe end-to-end smoke test completed; no production side effects were used for exploration.
