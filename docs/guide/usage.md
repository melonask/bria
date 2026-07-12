# Usage

Start Bria with a checked configuration and submit JSON to an HTTP source:

```bash
curl --request POST http://localhost:4000/v1/jobs \
  --header 'content-type: application/json' \
  --header 'idempotency-key: job-42' \
  --data '{"name":"Bria"}'
```

Use the returned `job_id` to request cancellation:

```bash
curl --request DELETE http://localhost:4000/v1/jobs/<job_id>
```

For a stopped pipeline, correct the fault first, then resume it with `POST /v1/pipelines/<pipeline_id>/resume`.
