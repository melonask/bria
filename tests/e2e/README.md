# Bria E2E Tests

End-to-end test scenarios exercising every source, sink, and state backend, plus failure
and recovery paths. The full suite (~6 min) runs infrastructure once, then cycles bria
through 19 configs.

## Quick start

```bash
# Run all 19 scenarios (builds image, starts infra, runs, tears down)
./run.sh --all

# Run a single scenario (requires infra already up via --infra-up)
./run.sh --infra-up
./run.sh http-pg
./run.sh --infra-down
```

## Architecture

Two compose files share a Docker network (`e2e-net`):

| File | Purpose |
|------|---------|
| `docker-compose.infra.yml` | Long-lived services: postgres, rabbitmq, webhook-echo, amqp-helper |
| `docker-compose.yml` | Bria only — restarted per scenario with a fresh config |

Infra starts once and stays up for the entire suite. Each scenario stops bria, swaps
`Config.toml` (symlink to `Config.<scenario>.toml`), resets PG tables, then starts bria
again. This avoids ~28 minutes of repeated container startup/teardown and image pulls.

## Run modes

| Command | What it does |
|---------|-------------|
| `./run.sh --all` | Build `bria:e2e` image, start infra, run all 19 scenarios, tear down infra |
| `./run.sh --infra-up` | Start postgres, rabbitmq, webhook-echo, amqp-helper and wait for healthy |
| `./run.sh --infra-down` | Stop and remove all infra containers, volumes, and the `e2e-net` network |
| `./run.sh <scenario>` | Run a single scenario (infra must already be up) |

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

### Infra (`docker-compose.infra.yml`, project `e2e-infra`)

| Service | Image | Notes |
|---------|-------|-------|
| **postgres** | `postgres:18-alpine` | PG source, sink, and state backend |
| **rabbitmq** | `rabbitmq:4-alpine` | AMQP queue source and sink |
| **webhook-echo** | `python:3-alpine` | Logs webhook sink POSTs to stdout |
| **amqp-helper** | `python:3-alpine` | Publishes/consumes AMQP messages via pika |

### Bria (`docker-compose.yml`, project `e2e-bria`)

| Component | Details |
|-----------|---------|
| **image** | `bria:e2e` — pre-built by `--all` |
| **port** | `4000:4000` (mapped to host) |
| **config** | `./Config.toml` → `/etc/bria/Config.toml:ro` |
| **scratch** | `./tmp/bria` → `/tmp/bria` |

Bria reaches infra services by Docker service name via the shared `e2e-net` network
(e.g. `postgres:5432`, `rabbitmq:5672`, `webhook-echo:8080`).

## Environment variables

| Variable | Default | Used by |
|----------|---------|---------|
| `BRIA_API_KEY` | `e2e-secret` | Bria server API key |
| `BRIA_E2E_BASE_URL` | `http://localhost:4000/v1` | curl commands in run.sh |
| `BRIA_E2E_PG_URL` | `postgres://bria:bria@postgres:5432/bria` | Bria PG connections |
| `BRIA_E2E_AMQP_URL` | `amqp://bria:bria@rabbitmq:5672` | Bria AMQP connections |
| `BRIA_E2E_WEBHOOK_SECRET` | `test-secret-42` | Webhook HMAC secret |

Override any of these before running `./run.sh` to test against non-default
environments.

## Files

```
tests/e2e/
  docker-compose.yml          # bria service (per-scenario restart)
  docker-compose.infra.yml   # shared infra (once per suite)
  run.sh                      # scenario runner
  Config.<scenario>.toml      # per-scenario bria configs
  README.md
```

`run.sh` cleans up after each scenario (bria container, `Config.toml` symlink, `tmp/`).
