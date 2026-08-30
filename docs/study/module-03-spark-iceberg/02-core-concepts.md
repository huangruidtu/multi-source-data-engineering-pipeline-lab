# Module 03 — Core concepts

## Lazy Spark execution
1. **Definition:** DataFrame transformations describe a plan; actions execute it. 2. **Why:** optimizer can combine operations. 3. **How:** parsing, windows and joins form jobs/stages/tasks. 4. **MDEP:** `silver_batch.py`. 5. **Why:** bounded Bronze processing. 6. **Misunderstanding:** each DataFrame line runs immediately. 7. **Failure:** unexpected shuffle/skew at action. 8. **Production:** inspect `explain`, partitions and task metrics. 9. **Interview:** driver plans; executors run tasks.

## Iceberg table semantics
Iceberg tracks data through metadata, manifests and snapshots; an atomic commit advances table state. Plain Parquet has columnar efficiency but no table pointer/merge/delete semantics. MDEP configures `SparkCatalog` with a Hadoop catalog and S3 warehouse in `build_spark`; this is an **IMPLEMENTED configuration**, not an observed catalog commit.

## Deterministic freshness
`VERSION_FIELDS` and `incoming_is_newer` order rates by `retrieved_at`, extract timestamp, ingestion timestamp, hash; locations use `updated_at` first. Hash is final deterministic tie-breaker only. This fixes MDEP-9’s reviewed stale-replay bug: a different old payload must not overwrite newer state.
