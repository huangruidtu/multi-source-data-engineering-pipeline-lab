# Project tradeoffs

| Decision | Project choice | Why / limitation |
| --- | --- | --- |
| CDC vs polling | Debezium for mutable PostgreSQL; batch snapshots for backfill | lower-latency changes still need WAL/offset operations |
| Flink vs Spark streaming | Flink owns CDC state | Spark remains batch owner |
| Canonical writer | Spark batch/reference, Flink CDC/event Silver | avoids dual-write inconsistency |
| Iceberg vs Parquet | Iceberg Silver, Parquet Bronze | table/snapshot semantics without complicating replay archive |
| Catalog | HadoopCatalog/S3 | minimal V1; not production multi-writer catalog |
| Snowflake Silver | external Iceberg access | avoids duplicate ownership; metadata access is operational dependency |
| dbt vs Spark | dbt Gold, Spark technical Silver | separates analytics from ingestion/conformance |
| Incremental vs rebuild | orders incremental; payments rebuild | payment deletes/relinks remain visible |
| Delivery claim | at-least-once plus idempotency | no exactly-once claim without evidence |
| Current vs history | Bronze history, Silver/Gold current semantics | equal row counts are not expected |
