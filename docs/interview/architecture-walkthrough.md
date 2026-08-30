# Architecture walkthrough

1. PostgreSQL, REST, and files enter the batch path; Airflow publishes source-aligned Parquet Bronze with provenance.
2. PostgreSQL WAL enters Debezium → Kafka → Flink; Flink archives raw CDC and applies current state to CDC Silver.
3. Spark owns batch/reference Silver and Flink owns CDC/event Silver, preventing dual writers.
4. Snowflake reads Silver Iceberg externally; dbt owns Gold dimensions, facts, and marts only.
5. MDEP-13 wraps the unchanged architecture with a matrix, preflight, evidence structure, failure catalogue, reconciliation templates, and quality gates.

The implementation is present. Docker/S3/Snowflake physical execution is not yet observed on this host.
