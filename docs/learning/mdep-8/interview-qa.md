# MDEP-8 Interview Q&A

## Why Airflow, and what does it orchestrate?

**Answer:** Airflow orchestrates the daily source tasks, dependencies, retries, and backfills; Python modules perform extraction and Parquet writing. **Example:** the DAG runs PostgreSQL, REST, and files in parallel then waits in `complete`. **Follow-up:** Airflow is not Spark; no Bronze-to-Silver processing is implemented. **Senior extension:** production tasks would use managed connections, SLAs, metrics, and resource isolation.

## How is the DAG idempotent?

**Answer:** a logical interval/source/entity produces a deterministic `ingestion_id` and object key, and publication uses create-if-absent plus a manifest. **Example:** the local file rerun returned `already_published` for both valid files. **Follow-up:** what if source data changes? **Senior extension:** define a versioned replay/reconciliation policy rather than overwriting Bronze evidence.

## Retry vs rerun vs backfill?

**Answer:** retry repeats a failed task attempt, rerun repeats an existing interval, and backfill runs historical intervals. **Example:** `airflow dags backfill` can run 2025-02-01 through 2025-02-03; all paths use `ingest_date`. **Senior extension:** bound concurrency and use interval-aware checkpoints.

## How do you ingest a paginated API safely, including 429?

**Answer:** follow `next_page`, retry retryable 429/5xx responses using `Retry-After`, and publish only after all pages are collected. **Example:** `fetch_paginated_json` is tested with a 429 followed by two pages. **Follow-up:** partial failure? **Senior extension:** persist page checkpoints in a durable control store at scale.

## How do you handle files and bad data?

**Answer:** hash file content for identity, publish valid files separately to preserve schemas, and write duplicate/malformed/missing/invalid evidence to Quarantine. **Example:** duplicate CSV content is not republished; malformed device JSON becomes JSONL evidence. **Senior extension:** use object versions/manifests and contract validators.

## Why Bronze Parquet and why not Silver?

**Answer:** Parquet provides efficient, inspectable source-aligned storage; only ingestion metadata is appended. PostgreSQL batch cannot own CDC current-state Silver under MDEP-6. **Follow-up:** who owns current state? **Senior extension:** later Debezium/Kafka/Flink applies CDC; Spark handles batch reference Silver.

## What was actually validated?

**Answer:** deterministic paths, metadata/hashing, duplicate detection, simulated 429 pagination, real local file Parquet generation/inspection, Quarantine artifacts, rerun publication, and static compilation. **Follow-up:** Airflow/PostgreSQL? **Senior extension:** they remain unvalidated on this host because Docker, Airflow, and psycopg are unavailable; I would run the documented commands in a compatible environment before claiming completion.

## Is merged code the same as a validated pipeline?

**Answer:** No. Merge records the reviewed implementation; it does not replace runtime acceptance evidence. **Example:** MDEP-8 has unit and local Parquet evidence, while PostgreSQL and Airflow runs remain unexecuted. **Follow-up:** what do you do next? **Senior extension:** maintain a validation matrix, keep affected Jira work in progress, and capture logs/output paths before closing it.
