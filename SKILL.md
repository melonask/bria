---
name: bria
description: Operational guide for AI agents safely configuring and running Bria.
---

# Bria agent operations

Bria routes jobs from configured sources through pipeline DAGs to sinks. This is an operational guide, not human-facing product documentation.

## Required configuration check

Before every run and after every configuration change, execute the exact runtime binary and file:

```bash
bria check --config /absolute/path/to/Config.toml
```

`bria check` parses environment substitutions, resolves shared profiles, and validates IDs, routes, features, references, DAGs, retries, and failure routing. It prints `Configuration is valid: <path>` on stdout and exits zero only on success. Diagnostics are stderr with a non-zero exit status. `BRIA_CONFIG` selects the default path; an explicit `--config` wins.

## Safe task execution

```bash
bria --config /absolute/path/to/Config.toml
```

Do not run unvalidated generated configuration. Preserve configured state and data paths. Do not use production endpoints, brokers, credentials, or webhooks for exploration. Stop with SIGINT and allow the configured graceful shutdown timeout; do not force-kill while work is draining without operator direction.

## Feature flags

`sqlite` is enabled by default. `server`, `webhook`, `postgres` (`pg`), `amqp`, `wasm`, and `cron` are opt-in; `full` enables all. Build the same feature set used for validation and execution. HTTP sources/control routes require `server`; webhook sinks require `webhook`; queue sources/sinks require `amqp`.

## Input, output, and error semantics

`bria ping` writes `pong` and reads no configuration. HTTP submission returns `201` (or a webhook source's configured acknowledgement) with `status`, `job_id`, and `correlation_key`; acceptance means source enqueueing, not pipeline success. Cancellation returns `202` and is observed before queued execution. Results are dispatched only to configured sinks: `success` and `failure` are sink-visible; cancelled jobs are not.

Treat `Idempotency-Key` and `X-Correlation-ID` as opaque correlation metadata; when both are set they must match. Bria does not deduplicate jobs or make authorization, payment, or challenge decisions.

## Configuration guardrails

Use `[bria]`, preserve existing profile references (`store`, `path_ref`, `transport`), and put secrets in `${NAME}` environment substitutions rather than agent output. Keep `queue_capacity` finite for pipeline backpressure. For `failure.action = "stop"`, fix the failure before `POST /<prefix>/pipelines/<pipeline_id>/resume`.
