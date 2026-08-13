# Project Rules & Behavioral Guidelines

## MongoDB Task Storage Standards
- **Single Collection (`task_request_responses`)**: Always store task request parameters, execution status, and output responses in the single `task_request_responses` collection. Never create or split records into a secondary `task_logs` collection.
- **Unified `input` Field**: Store input arguments and payload parameters under a single `"input"` key in the MongoDB document rather than separate `"args"` / `"kwargs"` fields.
- **ISO 8601 UTC Datetime Strings**: Store `created_at` and `updated_at` timestamps strictly as ISO 8601 UTC strings (e.g. `2026-08-13T08:06:33.632000+00:00`), never as raw PyMongo BSON Date objects.
- **Allowed Status Enum**: Valid status states for tasks are strictly `STARTED`, `RETRYING`, `SUCCESS`, and `FAILED`. Do not use `SKIPPED`. Deduplication or lock conflict failures must be recorded as `FAILED`.

## Celery Task Function Signatures
- **Flexible Signature Support**: Every Celery task handler definition must include `**kwargs` (e.g., `def task_name(self, data: dict = None, **kwargs):`) to prevent `TypeError` when passing execution metadata (such as `trace_id` or `no_retry`).
