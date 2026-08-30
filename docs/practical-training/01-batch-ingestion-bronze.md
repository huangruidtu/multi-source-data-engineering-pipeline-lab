# 01 — Batch ingestion and Bronze workbook

Attempt this file before [the matching solutions](solutions/01-batch-ingestion-bronze-solutions.md). Record work in a copy of [the session template](training-records/01-batch-ingestion-bronze-session-template.md).

## BI-01 — REST pagination partial failure
- **Difficulty:** Foundation
- **Task type:** TRACE / INCIDENT / INTERVIEW EXPLANATION
- **Source files to inspect:** `ingestion/batch/extractors.py` (`RETRYABLE_HTTP`, `fetch_paginated_json`); `pipeline.py` (`land_rest`); `bronze.py`; `tests/test_bronze_ingestion.py`.
- **Scenario / concrete responses:** Request page 1 returns `{items:[{id:1}],next_page:2}`. Page 2 first returns HTTP 429 with `Retry-After: 0`, then `{items:[{id:2}],next_page:3}`. Page 3 returns HTTP 500 for all configured attempts. `retries=2`.
- **Engineering deliverables:** retryable statuses; trace page/query/retry/accumulation; Bronze publication decision; what becomes of in-memory page 1/2 records; partial-object decision; invariant; unit test; 30–60 second English answer.
- **Constraints:** no actual REST/Airflow/S3 claim; use current all-pages-before-return behavior only.
- **Competency trained:** bounded extraction and partial-landing prevention.
- **Workspace:** `Retryable: ___ | Accumulated: ___ | Return/publish: ___ | Invariant: ___ | Test: ___ | Interview: ___`

## BI-02 — Renamed duplicate file
- **Difficulty:** Intermediate
- **Task type:** TRACE / INCIDENT / TEST DESIGN
- **Source files to inspect:** `extractors.py` (`file_identity`); `pipeline.py` (`land_files`); `bronze.py` (`BatchContext`, keys, quarantine); `tests/test_bronze_ingestion.py`.
- **Scenario / concrete inputs:** `valid/categories.csv` was processed. `valid/categories-copy.csv` arrives on same logical date with byte-for-byte identical content and same SHA-256, but different filename.
- **Engineering deliverables:** identity derivation; filename insufficiency; valid/duplicate/quarantine disposition; context/key behavior; content identity versus downstream business natural key; regression test; production consequence; English answer.
- **Constraints:** do not equate file SHA-256 with Silver natural key; no physical S3 claim.
- **Competency trained:** source provenance and duplicate evidence.
- **Workspace:** `Identity: ___ | Disposition: ___ | Key/context: ___ | Difference from business key: ___ | Test: ___`

## BI-03 — Airflow retry versus idempotency review
- **Difficulty:** Senior
- **Task type:** CODE REVIEW / ARCHITECTURE / TEST DESIGN
- **Source files to inspect:** `pipeline.py` (`context`, `publisher`); `bronze.py` (`BatchContext`, `BronzePublisher`); `orchestration/dags/bronze_ingestion.py`; relevant tests/deep-dives.
- **Scenario / snippet:**

```python
if airflow_retry:
    publish_new_random_key(records)
```

- **Concrete facts:** DAG passes `{{ ds }}`, is UTC daily, `catchup=False`, retries twice; `BatchContext` includes logical date, `[start,end)`, source name/entity; publisher conditionally creates deterministic object key.
- **Engineering deliverables:** at least two defects; BatchContext fields; UTC half-open interval reason; retry/backfill behavior; `catchup=False` distinction; corrected pseudocode/design; regression test; incident prevented; English answer.
- **Constraints:** Airflow retry is not business identity; do not claim actual DAG/backfill execution.
- **Competency trained:** orchestration/data-idempotency boundary.
- **Workspace:** `Defects: ___ | Stable identity: ___ | Correct design: ___ | Test: ___ | Incident: ___`
