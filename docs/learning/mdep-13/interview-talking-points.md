# MDEP-13 interview talking points

## One-minute narrative

I closed the project by building a validation and evidence layer around the implemented batch and CDC architecture. The important point is truthfulness: preflight detects the actual host, a matrix tracks every MDEP-8–12 runtime debt, and the runner writes per-run evidence without converting BLOCKED or NOT_RUN into PASSED. Reconciliation uses business keys and layer semantics, because Bronze history, Silver state, and Gold marts should not be expected to have equal counts.

## Whiteboard sequence

Draw sources → Airflow/Spark and Debezium/Kafka/Flink → Bronze/Silver Iceberg → Snowflake/dbt Gold. Add a sidecar labelled matrix/preflight/evidence. Explain ownership, then use a lower-LSN replay or duplicate batch rerun to show why validation needs state and keys, not just pipeline-green status.

## Honest closing sentence

The framework and static checks are implemented; Docker, S3, Spark/Flink, Snowflake, and dbt runtime evidence is still open on this host, with exact commands and acceptance observations documented for the next environment.
