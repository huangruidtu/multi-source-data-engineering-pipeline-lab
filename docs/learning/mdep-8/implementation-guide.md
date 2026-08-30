# MDEP-8 Implementation Guide — Idempotent Batch Ingestion

## Purpose and scope

MDEP-8 implements only `PostgreSQL / REST API / CSV / JSON -> Airflow -> S3 Bronze / Parquet`. It introduces no Spark, Silver, Iceberg, Debezium, Kafka, Flink, Snowflake, dbt, or Gold logic. PostgreSQL batch output is explicitly Bronze snapshot/backfill material, never canonical CDC current state.

## Implemented files

```text
ingestion/batch/bronze.py          # keys, metadata, local/S3 stores, Parquet/quarantine publication
ingestion/batch/extractors.py      # PostgreSQL, REST pagination/retry, CSV/JSON reads
ingestion/batch/pipeline.py        # source task functions
ingestion/batch/requirements.txt   # Airflow, PyArrow, psycopg, boto3
orchestration/dags/bronze_ingestion.py
tests/test_bronze_ingestion.py
```

## DAG and flow

`mdep_bronze_ingestion` is one daily DAG, starting 2025-02-01, with `max_active_runs=1`, two retries, and one-minute retry delay. It runs `postgres_snapshot`, `rest_reference`, and `file_reference` in parallel; `complete` waits for all three.

```text
postgres_snapshot ─┐
rest_reference ────┼─> complete
file_reference ────┘
```

Airflow passes `{{ ds }}` as the logical date. `BatchContext` converts it into a UTC `[data_interval_start, data_interval_end)` and derives an `ingestion_id` from source, entity, logical date, and interval. The DAG can be backfilled through Airflow because every path is interval-addressed.

## Extraction and output

`postgres_rows()` supports a full snapshot or a bounded `updated_at >= start AND updated_at < end` incremental exercise. `fetch_paginated_json()` follows `next_page`; it retries 429/5xx and URL errors before returning all records. It does not publish page-by-page, so a failed page cannot create a partial canonical REST object.

File ingestion publishes each valid CSV/JSON fixture as its own entity, fixing the schema boundary between CSV category rows and JSON device rows. Duplicate file content, invalid fixtures, malformed JSON, and the intentionally absent file are retained as JSONL Quarantine evidence.

Valid records are source fields plus MDEP-6 metadata: `ingestion_id`, source name/entity/key, extraction and landing timestamps, source version, locator, and SHA-256 `record_hash`. Bronze keys are:

```text
s3://<bucket>/bronze/<source>/<entity>/ingest_date=YYYY-MM-DD/ingestion_id=<id>/data.parquet
s3://<bucket>/quarantine/<source>/<entity>/ingest_date=YYYY-MM-DD/ingestion_id=<id>/rejected.jsonl
```

Local validation uses the same keys below `BRONZE_LOCAL_ROOT`; setting `BRONZE_S3_BUCKET` selects the optional boto3 S3 store.

## Idempotency and deferred work

`BronzePublisher` uses deterministic keys plus exclusive create. A completed rerun sees the object and manifest and returns `already_published`; it never appends a second canonical object. A temporary Parquet file is built before publication. `max_active_runs=1` prevents concurrent DAG runs; multi-scheduler race hardening, schema registry, S3 lifecycle/encryption, and production checkpoint persistence are deferred.
