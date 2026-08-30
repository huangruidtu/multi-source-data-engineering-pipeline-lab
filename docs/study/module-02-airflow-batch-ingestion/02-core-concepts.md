# Module 02 — Core concepts

## DAG and data interval
1. **Definition:** a DAG orders tasks; data interval is the bounded time slice they process. 2. **Why:** scheduling must be reproducible. 3. **How:** `BatchContext` derives identity from logical date/start/end/source/entity. 4. **MDEP file:** `ingestion/batch/bronze.py`. 5. **Why:** a rerun locates the same object. 6. **Misunderstanding:** wall-clock execution time is business time. 7. **Failure:** backfill publishes overlapping nondeterministic data. 8. **Production:** use explicit schedules and interval observability. 9. **Interview:** logical date is not “now.”

## Retry-safe publication
`bronze_key` creates deterministic S3-style paths and `put_if_absent` prevents a retry from appending a second canonical object. The manifest records count/hash/interval. This is implemented with local/S3 abstractions; physical S3/Airflow behavior is unvalidated.

## REST pagination and throttling
`fetch_paginated_json` follows `next_page`, retries 429/5xx, honours `Retry-After`, and fails the extraction rather than publishing a partial response set. Rate limiting is a source contract, not a reason to parallelise blindly.
