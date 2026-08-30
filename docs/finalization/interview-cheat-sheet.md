# MDEP Interview Cheat Sheet

## 30 seconds

“MDEP implements batch reference ingestion and PostgreSQL CDC. Airflow lands
replayable Bronze, Spark owns batch-reference Silver, Debezium/Kafka/Flink owns
CDC current-state Silver, and Snowflake/dbt owns Gold. It emphasizes explicit
ownership and deterministic replay correctness; physical integration is deferred
to V1.x.”

## Two-minute architecture

PostgreSQL, REST, and files are source boundaries. Batch rows gain lineage and
land as deterministic Bronze Parquet; Spark validates and merges locations and
exchange rates only. Commerce WAL changes use pgoutput Debezium and table topics.
Flink archives raw CDC and applies keyed state using LSN, same-transaction order,
and exact-replay identity. Snowflake externally reads Silver; dbt builds Gold.
Offline tests, quality gates, reconciliation templates, and an evidence runner
provide V1 validation without claiming a live stack.

## Paths

- Batch: REST/files → Airflow → Bronze → Spark → reference Silver → dbt Gold.
- CDC: order → WAL/publication → Debezium → Kafka → `orders:1001` Flink state →
  `core_orders` → staging → `fct_orders` → daily mart.

## Design decisions

1. One canonical Silver writer per dataset.
2. Bronze is evidence; Silver is current trusted state.
3. Airflow orchestrates; Spark batch-transforms; Flink applies CDC state.
4. Spark freshness: business time → extract time → ingest time → hash.
5. LSN beats Kafka offset; equal unprovable position is a conflict.
6. Quarantine preserves invalid evidence.
7. Iceberg is Silver's table boundary; dbt owns Gold.
8. `fct_payments` rebuilds for bounded-V1 correctness.

## Five stories

- A changed hash once allowed stale Spark replay; lexicographic ordering fixed it.
- Transaction metadata uses `provide.transaction.metadata`, not `include.transaction`.
- Flink never treats Kafka partition number as freshness.
- `GOLD_GOLD` became `MDEP.GOLD` and gained contract coverage.
- PASS now requires exit code zero and evidence.

## Honest boundary

Implemented/offline-tested: contracts, ordering models, configuration, warehouse
contracts, validators, and documentation. **RUNTIME DEFERRED — not runtime
validated:** Docker/Airflow, Spark/Iceberg/S3, Debezium/Kafka, Flink recovery, and
Snowflake/dbt physical integration. Keywords: idempotency, LSN, transaction
order, quarantine, replay, current-state grain, reconciliation, runtime evidence.
