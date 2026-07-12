# Configuration

All Bria configuration is under `[bria]`. Shared root profiles may be referenced by `store`, `path_ref`, and `transport`.

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
cmd = "sh"
args = ["-c", "printf 'hello %s' \"$1\"", "sh", "{{job.payload.name}}"]

[[bria.pipelines]]
id = "greetings"
source = "api"
queue_capacity = 128

[[bria.pipelines.steps]]
id = "run"
type = "process"
task = "greet"
```

Templates use `job`, `steps`, `env`, `now`, and result fields in sink rendering. CEL map and condition steps read `job`, `steps`, and `pipeline`. `${NAME}` requires an environment variable; `${NAME:-value}` supplies a default.
