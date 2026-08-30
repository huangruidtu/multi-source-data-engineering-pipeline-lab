# MDEP-8 Learning Notes — Concepts in This Code

- **Orchestration/DAG:** Airflow coordinates three independent source tasks and a final dependency; it does not transform Silver data. A common error is placing heavy processing or stateful logic in the scheduler instead of testable task functions.
- **Logical date/data interval:** `BatchContext` makes a UTC day interval from `{{ ds }}`. This matters because a backfill must reproduce the same target partition, not use wall-clock time.
- **Retry, rerun, backfill:** retry is another attempt of one failed task; rerun repeats one logical interval; backfill creates historical intervals. Deterministic object keys make all three safe when the intended source snapshot is replayed.
- **Idempotency:** `ingestion_id` and object key are deterministic, and exclusive publication prevents an append on retry. Without this, an API retry can double a Bronze dataset.
- **Full versus incremental extraction:** PostgreSQL has a full `SELECT` and an `updated_at` bounded exercise. This is a learning extractor, not a replacement for CDC ownership.
- **Parquet/partitioning:** valid source-aligned records are columnar Parquet under source/entity/date prefixes. Too many tiny files or partitions would hurt production performance.
- **Metadata and lineage:** locator, source version, hash, and interval identity let a reviewer trace a Bronze row to its source request/file/table and landing operation.
- **Quarantine:** malformed JSON and missing/duplicate/invalid fixtures become evidence records. Parsing success is not business validity; future Silver owns deeper validation.
- **Partial failure/replay:** REST records are collected before publishing, so a mid-page failure leaves no partial canonical object. The next task attempt can replay deterministically.
- **Runtime evidence:** unit and local-PyArrow evidence are not Airflow-runtime evidence. The Docker validator makes the remaining acceptance path reproducible; it must be run before claiming scheduler, PostgreSQL, retry, or backfill validation.
