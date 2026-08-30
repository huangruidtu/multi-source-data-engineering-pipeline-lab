# Module 02 — Failures and trade-offs

| Failure | Detection / impact | Current behavior | Recovery / improvement |
| --- | --- | --- | --- |
| REST 429/5xx | HTTP status, incomplete page set | bounded retries; no partial publish | backoff/jitter, source SLO |
| missing/malformed file | parser/validation | quarantine or task failure | alert, late-arrival policy |
| task retry | deterministic key | no second canonical object | test real object conditional write |
| partial runtime stack | validator preflight | `BLOCKED`, not passed | run Docker evidence matrix |

Airflow was selected over a generic scheduler because it makes dependencies, retries and backfill visible. Spark is intentionally not put inside task logic: orchestration code is simpler but must not become a distributed transformation engine.
