# 02 — Spark Silver
Read: `processing/spark/contracts.py`, `silver_batch.py`, `tests/test_silver_contracts.py`.

## SS-01 — TRACE
**Difficulty:** Intermediate. **Scenario:** same `location_id`; existing Silver has `updated_at=2026-08-20`, incoming replay has `updated_at=2026-07-01` and different hash. **Deliverable:** decide MERGE action and explain version tuple precedence. **Competency:** replay safety.
## SS-02 — CODE REVIEW
**Difficulty:** Senior. **Snippet:** `WHEN MATCHED AND s.record_hash <> t.record_hash THEN UPDATE`. **Deliverable:** identify the regression bug; write the required ordering in words and name the protecting test. **Competency:** deterministic current state.
## SS-03 — TEST DESIGN
**Difficulty:** Intermediate. **Scenario:** same business/extract timestamps, incoming has later `ingested_at`; then an exact replay. **Deliverable:** design two pure `incoming_is_newer` tests and production bugs prevented. **Competency:** tie-breaker tests.
