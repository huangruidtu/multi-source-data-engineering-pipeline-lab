# MDEP-8 Architecture Notes — Airflow to Bronze

## Implemented decisions

Airflow orchestrates task order, retry, logical intervals, and backfill; Python extraction functions do the actual I/O and Parquet creation. Bronze is intentionally minimally transformed: source fields remain intact and only audit metadata is appended. PyArrow writes Parquet because it is columnar, inspectable, and aligns with the approved S3/Parquet path.

The local object-store adapter mirrors S3 object keys so tests can validate partition layout without AWS credentials. The S3 adapter is optional and selected by environment variables. This is a local/AWS boundary, not a claim that S3 was exercised.

## Failure and ownership boundaries

The REST extractor reads every page before one publication, so API failure is task failure and Airflow can retry the task without a partial canonical page collection. File parse/fixture issues become Quarantine evidence; they are not silently dropped. PostgreSQL relational extraction is Bronze-only because MDEP-6 reserves CDC/Flink as the future canonical Silver owner.

## Trade-offs

One compact DAG demonstrates dependencies without dynamic-task complexity. Files are published separately to preserve their schemas. Quarantine is JSONL because it must retain raw malformed payload evidence; valid Bronze records are Parquet. The deterministic key gives replay/idempotency, but it intentionally treats a same-interval source change as a controlled replay decision rather than silently replacing published evidence.

## Production implications

Production would use a durable API checkpoint per page, secrets/connections, metrics, content/schema compatibility controls, S3 encryption/lifecycle policies, object manifests with stronger concurrency coordination, and bounded incremental-watermark governance. None are implemented here. Bronze partitioning by source/entity/ingest date supports operational discovery but may require compaction at scale.
