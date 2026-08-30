# Module 00 — Core concepts

## Canonical ownership
1. **Definition:** one layer owns mutations for a dataset. 2. **Why:** prevents dual upserts. 3. **How:** Spark owns reference Silver; Flink owns CDC current state. 4. **MDEP:** [v1 scope](../../planning/v1-scope.md). 5. **Files:** `processing/spark/contracts.py`, `processing/flink/flink_cdc_job.py`. 6. **Why chosen:** batch and CDC have different evidence/order rules. 7. **Misunderstanding:** Bronze extraction equals Silver ownership. 8. **Failure:** competing writers regress state. 9. **Production:** enforce with deployment and catalog permissions. 10. **Interview:** start with the data owner, not the tool.

## Lakehouse versus warehouse
Iceberg makes S3 files a transactional table through metadata/snapshots; Snowflake provides governed warehouse compute and Gold SQL. MDEP uses external Iceberg Silver and Snowflake-native Gold (`warehouse/snowflake/01_setup.sql`). Plain Parquet cannot safely implement a MERGE. This is a boundary, not a claim that either product replaces the other.

## Replay and eventual consistency
Bronze retains immutable evidence because delivery/retry can duplicate data. Silver is convergent only when version ordering is deterministic. Gold can lag Silver, so reconciliation must compare the correct state/grain rather than demand simultaneous equality.
