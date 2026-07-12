# Getting started

Install a binary built with the integrations your configuration uses, copy `Config.example.toml`, then validate it before starting:

```bash
bria check --config Config.toml
bria --config Config.toml
```

The default configuration feature is SQLite. Enable `server` for HTTP sources and control routes; use `full` to build every optional integration.

`bria ping` prints `pong` and does not read configuration.
