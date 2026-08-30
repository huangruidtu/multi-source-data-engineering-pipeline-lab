# 01 — Batch ingestion and Bronze
Read: `ingestion/batch/extractors.py`, `pipeline.py`, `bronze.py`, `orchestration/dags/bronze_ingestion.py`, `tests/test_bronze_ingestion.py`.

## BI-01 — TRACE
**Difficulty:** Foundation. **Scenario:** REST page 1 succeeds, page 2 returns 429 then succeeds, page 3 fails 500 after retries. **Deliverable:** State whether any Bronze object may publish; explain `RETRYABLE_HTTP`, `Retry-After`, and the invariant trained. **Constraint:** no runtime assumptions. **Competency:** all-or-fail source landing.
## BI-02 — INCIDENT
**Difficulty:** Intermediate. **Scenario:** a renamed CSV has identical bytes to an already-landed valid file. **Inputs:** same SHA-256, new filename, same logical date. **Deliverable:** identify output disposition/key, why filename is insufficient, and one regression test. **Competency:** content identity/quarantine.
## BI-03 — CODE REVIEW
**Difficulty:** Senior. **Snippet:** `if airflow_retry: publish_new_random_key(records)`. **Deliverable:** identify two correctness defects; replace with a design using `BatchContext`, conditional publication, and a historical backfill explanation. **Competency:** orchestration versus idempotency.
