# V1 baseline

**Status date:** 2026-08-30. V1 is a portfolio-ready **implementation and static-validation baseline**, not a runtime-accepted deployment.

## Scope and components

- Sources: PostgreSQL, REST, CSV/JSON; CDC from PostgreSQL WAL.
- Batch: Airflow → Parquet Bronze → PySpark → Iceberg Silver.
- Streaming: Debezium → Kafka → Flink → CDC Bronze archive and Iceberg Silver.
- Analytics: Snowflake external Silver access → dbt Gold dimensions, facts, marts.

Bronze is replayable history; Spark owns batch/reference Silver; Flink owns CDC/event Silver; dbt owns Gold. MDEP-6–MDEP-13 are merged in main. Focused contracts and Python tests are versioned with complete learning and interview documentation.

## Limitations and production gaps

Runtime debt RD-08 through RD-13 remains in [runtime-debt-register.md](runtime-debt-register.md). Production would add managed service scaling, IAM/secrets, a production Iceberg catalog, RBAC, integration CI, lineage/governance, quality SLAs, disaster recovery, and cost controls.

Excluded V1 technologies remain Databricks, Delta Lake, Redshift, BigQuery, Airbyte, Fivetran, Dagster, Prefect, StarRocks, Paimon, and Fluss. Portfolio-ready evidence is the architecture, ownership model, implementation history, static contracts, documented limitations, and interview narrative—not a claim of unobserved runtime success.
