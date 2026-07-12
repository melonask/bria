# Operations

Validate the exact runtime file before deployment:

```bash
BRIA_CONFIG=/etc/bria/Config.toml bria check
```

Use `BRIA_CONFIG` or `--config`; the latter takes precedence. Logs follow `[bria.global.log]`. State records queued and running jobs for recovery when using a durable state backend.

Feature flags: `sqlite` is default; `server`, `webhook`, `postgres` (`pg`), `amqp`, `wasm`, and `cron` are opt-in. `full` enables all of them. Set a finite `queue_capacity` for each pipeline; the router waits when that pipeline queue is full.
