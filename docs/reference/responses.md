# Responses

## Job accepted

```json
{"status":"accepted","job_id":"01J...","correlation_key":"job-42"}
```

HTTP sources return `201 Created`; webhook sources return their configured acknowledgement status. Acceptance confirms source enqueueing, not task completion.

## Cancellation requested

```json
{"status":"cancellation_requested","job_id":"01J..."}
```

This returns `202 Accepted`. Cancellation is observed before queued execution; it cannot retroactively stop a completed task.

Invalid JSON returns `400`; a body above the configured source limit returns `413`; an unknown source or pipeline returns `404`.
