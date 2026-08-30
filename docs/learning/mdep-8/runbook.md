# MDEP-8 Runbook — Run, Inspect, Fail, Recover

## Prerequisites

Use Python 3.12-compatible Airflow, Docker Compose for the MDEP-7 source services, and install:

```powershell
python -m pip install -r ingestion/batch/requirements.txt
docker compose up --build --wait
$env:BRONZE_LOCAL_ROOT = "$PWD/build/local-object-store"
$env:POSTGRES_DSN = 'postgresql://lab:lab@localhost:5432/commerce'
```

For real S3 set `BRONZE_S3_BUCKET` and AWS credentials; omit it for local mirrored-S3 paths.

## Airflow operations

```powershell
$env:AIRFLOW_HOME = "$PWD/.airflow"
$env:AIRFLOW__CORE__DAGS_FOLDER = "$PWD/orchestration/dags"
airflow db migrate
airflow dags list | Select-String mdep_bronze_ingestion
airflow dags test mdep_bronze_ingestion 2025-02-01
airflow dags backfill mdep_bronze_ingestion -s 2025-02-01 -e 2025-02-03
```

Rerun the same logical date with `airflow dags test`; published objects should be reported as `already_published`. Inspect logs with `airflow tasks logs mdep_bronze_ingestion file_reference 2025-02-01` where supported by the Airflow version.

## Inspect output

```powershell
Get-ChildItem -Recurse build/local-object-store/bronze
Get-ChildItem -Recurse build/local-object-store/quarantine
python -c "import pyarrow.parquet as pq; print(pq.read_table('PATH_TO_DATA.parquet').schema)"
```

Expected valid paths contain `ingest_date=2025-02-01` and an `ingestion_id`; Parquet includes all nine metadata fields. Quarantine contains duplicate-content, invalid-fixture, malformed-JSON, and missing-file evidence.

## Fail/recover

Use `?scenario=retryable` or `?scenario=rate_limit` on an MDEP-7 endpoint to exercise retry logic; use `scenario=timeout` to force a transport error. The malformed fixture and absent `expected-location-overrides.csv` are already discovered by file ingestion. After an interrupted task, rerun the same logical date: incomplete temporary files are not canonical, and an already completed deterministic object is not duplicated.

Stop local services with `docker compose down`. On this implementation host Docker, Airflow, PostgreSQL, and psycopg were unavailable, so these service commands are documented but UNVALIDATED here.
