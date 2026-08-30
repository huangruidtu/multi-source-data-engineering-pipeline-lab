# Module 00 — Code walkthrough

Reading order: `README.md` → `docs/project-charter.md` → `docs/planning/v1-scope.md` → `docs/decisions/0001-iceberg-catalog-and-snowflake-access.md` → `docs/project-evidence.md`.

| Path | Purpose and interview note |
| --- | --- |
| `docker-compose.yml` | runtime topology, **not** runtime proof |
| `orchestration/dags/bronze_ingestion.py` | schedules bounded Bronze landing |
| `processing/spark/silver_batch.py` | reference-only Iceberg tables and merge |
| `ingestion/cdc/debezium-postgres-connector.json` | WAL-to-topic contract |
| `processing/flink/flink_cdc_job.py` | real source/parser/keyed-state/sink wiring |
| `analytics/dbt/models/` | Gold dependency graph and grain |
| `validation/` | evidence framework rather than a data writer |

Notice the ownership split before discussing any implementation. Runtime errors belong at the component boundary; do not infer physical success from the presence of these files.
