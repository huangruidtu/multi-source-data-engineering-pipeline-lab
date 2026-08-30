# Module 02 — Interview Q&A

### How do you make a batch retry idempotent?
**Direct answer:** stable input identity maps to a deterministic output key and conditional publication. **Deep explanation:** retries must not append an indistinguishable second object. **MDEP example:** `BatchContext.ingestion_id`, `bronze_key`, manifest and `put_if_absent`. **Why:** replayable Bronze. **Follow-up:** What about changed source snapshots? bound/record the interval and source extract time. **Senior extension:** atomic manifest/pointer protocol. **Weak answer:** “retry is idempotent by default.”

### Why use Airflow rather than Spark for this layer?
**Direct answer:** Airflow schedules and observes tasks; Spark transforms distributed data. **MDEP example:** the DAG calls the batch pipeline; `silver_batch.py` owns Silver. **Follow-up:** Can Airflow trigger Spark? Yes, without embedding transformations. **Senior extension:** separate control and data planes. **Weak answer:** comparing only speed.

### How do you handle a 429?
**Direct answer:** respect retryability and `Retry-After`, then fail cleanly after a bound. **MDEP example:** `fetch_paginated_json`. **Follow-up:** Why avoid partial landing? it makes retry/reconciliation ambiguous. **Senior extension:** per-source quota policy. **Weak answer:** infinite retries.
