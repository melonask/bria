# Bria E2E Tests

End-to-end test scenarios exercising every source, sink, and state backend, plus failure
and recovery paths.

## Quick start

```bash
# Run a single scenario
./run.sh http-pg

# Run all happy-path scenarios
for s in http-pg file-file file-sqlite http-file http-sqlite http-sse \
         webhook-pg cron-file pg-pg sqlite-file \
         queue-file http-queue http-webhook http-pg-recovery; do
    ./run.sh "$s" || break
done

# Run failure/recovery scenarios
for s in http-nonzero http-413 http-cancel http-condition-false webhook-hmac-401; do
    ./run.sh "$s" || break
done
```

## Scenarios

### Happy path

| # | Scenario | Source | Sink | State | Extras |
|---|----------|--------|------|-------|--------|
| 1 | `http-pg` | http | pg | pg | — |
| 2 | `file-file` | file | file | memory | — |
| 3 | `file-sqlite` | file | sqlite | memory | — |
| 4 | `http-file` | http | file | memory | — |
| 5 | `http-sqlite` | http | sqlite | memory | — |
| 6 | `http-sse` | http | stream (SSE) | memory | — |
| 7 | `webhook-pg` | webhook | pg | pg | HMAC |
| 8 | `cron-file` | cron | file | memory | cron every 5s |
| 9 | `pg-pg` | pg | pg | pg | — |
| 10 | `sqlite-file` | sqlite | file | memory | — |
| 11 | `queue-file` | queue (AMQP) | file | memory | RabbitMQ |
| 12 | `http-queue` | http | queue (AMQP) | memory | RabbitMQ |
| 13 | `http-webhook` | http | webhook | memory | webhook-echo |
| 14 | `http-pg-recovery` | http | file | pg | Restart recovery |

### Failure and recovery

| # | Scenario | Source | Sink | State | Extras |
|---|----------|--------|------|-------|--------|
| 15 | `http-nonzero` | http | file | memory | Task exits 42 |
| 16 | `http-413` | http | file | memory | Payload Too Large |
| 17 | `http-cancel` | http | file | pg | DELETE cancel, concurrency=1 |
| 18 | `http-condition-false` | http | file | memory | Condition `false`, action `fail` |
| 19 | `webhook-hmac-401` | webhook | pg | pg | Bad HMAC → 401 (reuses `webhook-pg` config) |

## Services

One shared `docker-compose.yml` provides:
- **postgres** (`postgres:18-alpine`) — pg source, pg sink, pg state
- **rabbitmq** (`rabbitmq:4-alpine`) — queue source, queue sink
- **webhook-echo** — captures webhook sink POSTs
- **amqp-helper** — publishes/consumes AMQP messages via pika
- **bria** — the main application

Test-only credentials and URLs are supplied through environment variables with
safe local defaults in `docker-compose.yml`/`run.sh` (`BRIA_API_KEY`,
`BRIA_E2E_PG_URL`, `BRIA_E2E_AMQP_URL`, `BRIA_E2E_WEBHOOK_SECRET`, and
`BRIA_E2E_BASE_URL`). Override them from the shell when running against a
non-default local test environment.

## Files

```
tests/e2e/
  docker-compose.yml          # shared infra
  run.sh                      # scenario runner
  Config.<scenario>.toml      # per-scenario bria configs
  README.md
```

`run.sh` cleans up after each run (containers, volumes, temp files).
