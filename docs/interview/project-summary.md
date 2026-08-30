# Project summary

## 30 seconds

I built a Commerce & Operations data-engineering lab with batch and CDC paths. Airflow ingests PostgreSQL, REST, and files to Parquet Bronze; Spark creates batch Silver Iceberg; Debezium/Kafka/Flink handles CDC; Snowflake and dbt produce Gold. A validation matrix and evidence runner keep implemented work, static checks, and unexecuted runtime work distinct.

## 60 seconds

Batch sources land through Airflow and are transformed by Spark. PostgreSQL changes flow through Debezium and Kafka to Flink, which archives raw CDC and applies current-state rules. Iceberg is shared Silver; Snowflake reads it externally and dbt builds dimensions, facts, and a daily-sales mart. MDEP-13 makes recovery, reconciliation, and runtime debt visible instead of claiming an unsupported demo succeeded.

## 2 minutes

One canonical writer path owns each Silver dataset: Spark owns batch/reference Silver, Flink owns CDC/event state. Bronze is append-oriented and replayable; Silver is current state; Gold is analytical current state and can retain warning-level exceptions such as orphan payments. The framework requires business-key reconciliation, not merely counts, and catalogues retries, stale replays, tombstones, checkpoint recovery, and stale Iceberg metadata. Static contracts passed locally; Docker, Spark, Flink, S3, Snowflake, and dbt runtime validation remains unexecuted on this host.

## 5 minutes

Contracts establish keys, metadata, and ownership. Batch publication is idempotent and quarantines bad files. Spark ordering prevents an old replay from replacing newer state. CDC uses Debezium envelopes and LSN/transaction order in Flink. Iceberg provides shared table state and snapshots. dbt creates stated-grain Gold: orders incrementally reconcile deletes while payments rebuild for deletes/relinks. Preflight detects capabilities; the runner records a run-specific evidence directory and never promotes a blocked stage to passed. In interviews I distinguish implemented contracts, static validation, and missing physical evidence rather than claiming exactly-once without proof.
