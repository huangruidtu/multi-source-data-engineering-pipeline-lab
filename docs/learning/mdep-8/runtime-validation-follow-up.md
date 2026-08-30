# MDEP-8 Runtime Validation Follow-up

## Purpose

MDEP-8 implementation is merged in `main`, but MDEP-25, MDEP-28, and MDEP-8 remain In Progress because their container/runtime acceptance checks have not been executed. This document is a checklist, not a record of successful results.

## Current merged-state constraint

PR #4 merged the batch implementation but omitted its later runtime setup. PR #5 restores that setup: the Compose file includes the Airflow service, `orchestration/airflow/Dockerfile` builds it, the DAG supports the Docker-network REST base URL, and `scripts/validate-mdep-8-runtime.ps1` automates the core checks. This makes the procedure available in the repository; no Docker/Airflow/PostgreSQL run has yet been evidenced.

## Prerequisites

- Docker Desktop/Compose available and running.
- Python 3.12-compatible Airflow environment, or a reviewed Docker Airflow service that mounts `orchestration/dags/` and the repository root.
- PostgreSQL driver `psycopg[binary]`, PyArrow, and the dependencies in `ingestion/batch/requirements.txt`.
- Ports 5432 and 8080 available for MDEP-7 sources.
- No AWS credentials are required for local filesystem output. AWS credentials and `BRONZE_S3_BUCKET` are required only for optional real-S3 validation.

## Exact source and local-Airflow commands

Run from the repository root on a Docker-capable host:

```powershell
docker compose up --build --wait
.\scripts\validate-mdep-8-runtime.ps1 -StartDate 2025-02-01 -EndDate 2025-02-02
```

Stop sources after evidence is captured:

```powershell
docker compose down
```

## Checklist and evidence to capture

| Check | Expected result | Evidence to save |
| --- | --- | --- |
| Docker startup/reset | PostgreSQL and REST health checks pass; seed counts reset | Compose output; `scripts/validate-sources.ps1` output |
| DAG import | `mdep_bronze_ingestion` appears in `airflow dags list` | Command output / import error log |
| PostgreSQL snapshot | Four source tables land in Bronze paths | Task logs; Parquet row counts and schemas |
| Incremental extraction exercise | bounded `updated_at` query uses the requested interval | Query/task logs and output path |
| REST normal pages | exchange rates and locations fully land | task logs and record counts |
| REST retry/429 | retry delay/attempt behavior is visible and no partial canonical output appears | task logs, response status, final object listing |
| Files | valid CSV/JSON land; duplicate, invalid, malformed, missing cases are Quarantined | Bronze and Quarantine listings/content |
| Same-interval rerun | no second canonical object; result reports `already_published` | object listing and task logs |
| Airflow retry | intentionally fail a task, then verify task retry | Airflow task-instance/log evidence |
| Backfill | 2025-02-01 through 2025-02-02 partitions are produced | Airflow backfill output and partition listing |
| Parquet metadata | all MDEP-6 metadata fields exist | `pyarrow.parquet` schema output |

Inspect local artifacts with:

```powershell
Get-ChildItem -Recurse build/local-object-store/bronze
Get-ChildItem -Recurse build/local-object-store/quarantine
python -c "import pyarrow.parquet as pq; print(pq.read_table('PATH_TO_DATA.parquet').schema)"
```

## Jira closure rule

After successful captured evidence, transition MDEP-25 and MDEP-28 to Done, add a factual Jira comment with commands and results, then transition MDEP-8 to In Review. Do not close any item solely because the code is merged.

## Optional real-S3 validation

Real S3 remains optional for this Story. If credentials are available, set `BRONZE_S3_BUCKET` and validate deterministic keys and metadata with a controlled non-production bucket. Record bucket-safe object prefixes, not credentials. If credentials are unavailable, retain the local filesystem evidence and leave S3 explicitly unvalidated.
