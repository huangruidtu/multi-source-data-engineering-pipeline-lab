# Multi-Source Data Engineering Pipeline Lab

A hands-on Commerce & Operations data-engineering portfolio project. It demonstrates a coherent batch and CDC architecture without adding duplicate platforms: PostgreSQL, REST, and files flow through Airflow to Parquet Bronze and Spark/Iceberg Silver; PostgreSQL WAL flows through Debezium, Kafka, and Flink to CDC Bronze/Silver; Snowflake and dbt provide Gold dimensions, facts, and marts.

```text
Sources → Airflow → Bronze Parquet → Spark ─┐
PostgreSQL WAL → Debezium → Kafka → Flink ─┼→ Silver Iceberg → Snowflake/dbt → Gold
                                             └→ replayable CDC Bronze archive
```

Key design choices are one canonical Silver writer per dataset, replayable Bronze, deterministic stale-event handling, Iceberg current-state semantics, and dbt-owned Gold. The repository includes a validation matrix, failure catalogue, reconciliation templates, and interview material.

Implementation and static tests are present; Docker, Spark/Flink, S3, Snowflake, and dbt physical integration runs are still explicitly unvalidated on the current host. See [project evidence](docs/project-evidence.md), [V1 baseline](docs/closure/v1-baseline.md), and [interview pack](docs/interview/).
