# Code Deep-Dive: `orchestration/dags/bronze_ingestion.py`

**Source of truth:** [`orchestration/dags/bronze_ingestion.py`](../../orchestration/dags/bronze_ingestion.py).

## Read beside
- **Source:** [`bronze_ingestion.py`](../../orchestration/dags/bronze_ingestion.py)
- **Tests:** [`tests/test_bronze_ingestion.py`](../../tests/test_bronze_ingestion.py)
- **Architecture:** [`docs/finalization/architecture-implementation-mapping.md`](../finalization/architecture-implementation-mapping.md)
- **Interview topics:** [`batch-pipeline.md`](batch-pipeline.md), [`docs/finalization/interview-qa.md`](../finalization/interview-qa.md)

## 1. Why this file exists
It is MDEP-8’s compact batch control plane: schedule and coordinate source-aligned Bronze tasks without embedding Silver transformations.
## 2. Where it sits in the architecture
Airflow invokes the three source branches, each calls `pipeline.py`, and all converge after Bronze-level work.
## 3. Inputs / outputs / state
The scheduler supplies `{{ ds }}` logical date. Tasks return landing results; Airflow owns task state/retry scheduling, while object keys below own data idempotency.
## 4. Important symbols
`mdep_bronze_ingestion`, `postgres_snapshot`, `rest_reference`, `file_reference`, `complete`.
## 5. Execution flow
The decorated DAG is daily from UTC 2025-02-01. PostgreSQL, REST, and file task branches receive the same `{{ ds }}` and converge in `complete`.
## 6. Function-by-function walkthrough
The DAG has `dag_id="mdep_bronze_ingestion"`, `@daily`, `catchup=False`, `max_active_runs=1`, two retries, one-minute retry delay, and batch/Bronze tags. `postgres_snapshot` calls four table landings; `rest_reference` derives `REST_SOURCE_BASE_URL` with a local default and lands two paginated reference sources; `file_reference` lands fixtures. `complete(*_results)` is an explicit fan-in marker.
## 7. Critical code-block reasoning
`logical_date = "{{ ds }}"` is templated by Airflow and passed through rather than using wall-clock time; downstream `BatchContext` converts it into a deterministic UTC interval/key. `catchup=False` disables automatic scheduler catchup, not the conceptual ability to run a historical date/backfill. `max_active_runs=1` reduces overlapping scheduled DAG runs but is not the correctness guarantee for object writes.
## 8. Correctness invariants
- All branches share one logical date.
- The final task waits for all three branches.
- Scheduling/retry remains separate from data transformation/idempotency.
## 9. Failure behavior
Airflow retries a failing task twice after one minute. A persistent task failure prevents fan-in completion. The DAG does not swallow a source/publisher exception into a false success.
## 10. Tests that protect the behavior
Batch unit tests exercise task-level functions; project static checks inspect the DAG source/contract. **MDEP OFFLINE TESTED**; no Airflow scheduler/DAG run was executed.
## 11. What is not implemented / runtime deferred
**MDEP RUNTIME DEFERRED:** Airflow import, scheduling, task logs, task retry behavior, backfill execution, and Docker service behavior.
## 12. Production concepts beyond current code
**GENERAL / PRODUCTION CONCEPT — NOT IMPLEMENTED:** SLA alerts, pools, task concurrency limits, datasets, secrets backend, and a deployed metadata database.
## 13. Common misunderstandings
Airflow retry is not data idempotency. `catchup=False` is not “historical backfill impossible.” A task branch is orchestration, not a data ownership layer.
## 14. Interview questions
**Where is idempotency implemented?** Not in Airflow retries: the DAG supplies deterministic logical dates, while batch context, canonical keys, and conditional publication make retries safe.
## 15. 30-second spoken explanation
“The DAG is deliberately a control plane. It schedules daily UTC source landings, runs PostgreSQL, REST, and file branches, retries task failures twice, and fans into completion. It passes Airflow’s logical date downward; deterministic Bronze publication—not Airflow itself—provides data idempotency.”
## 16. Senior follow-up discussion
Explain how you would run a historical backfill despite `catchup=False`: use explicit DAG test/backfill invocation with historical logical dates, capacity controls, and reconciliation rather than turning on uncontrolled automatic catchup.
