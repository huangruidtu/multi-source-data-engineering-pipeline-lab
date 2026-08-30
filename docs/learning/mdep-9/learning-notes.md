# MDEP-9 learning notes

Each term below is tied to this repository rather than a generic example.

- **Driver / executor:** the Driver builds the plan in `silver_batch.py`; Executors read Parquet and run partitions. A missing JVM prevents both, which is why this host cannot validate Spark.
- **DataFrame and lazy plan:** `validate_exchange_rates`, the country join, and the dedup window describe transformations. `count`, `MERGE`, and Parquet writes are actions that execute them. A common failure is believing an error is caught before an action.
- **Logical/physical plan:** `explain("formatted")` exposes the chosen physical operators for the logical DataFrame code. Review it to see scans, exchanges, broadcast joins, and sort/window stages.
- **Partition / shuffle:** a partition is a unit of executor work, not necessarily one output file. Window deduplication and group-by require keys to meet in a partition, creating a shuffle. Narrow transformations preserve partitions; these wide ones do not.
- **Repartition / coalesce:** the skew exercise uses `repartition(4, key)` to force key redistribution. `coalesce` is safer for reducing partitions after filtering, but can make uneven partitions. Both affect file sizes/cost.
- **Join and skew:** locations broadcast-join a tiny country map, adding `region` and enforcing a reference. A single overwhelmingly common currency/country can make a shuffle partition slow; inspect before applying salting or key changes.
- **Deduplication and state protection:** rank natural keys inside the incoming batch by business version, source-extract timestamp, ingestion timestamp, then hash. The same tuple is compared against the existing Silver row before a merge update. The hash is only the final tie-breaker, never evidence that a record is fresh: an old replay with different content cannot regress newer Silver state; an exact replay is a no-op. Without this distinction, a deterministic batch winner could still overwrite a newer target row incorrectly.
- **Incremental batch:** MDEP-9 filters an `ingested_at` interval and merge-upserts it. It is not an event-time watermark; later Flink owns that distinction.
- **Parquet / Iceberg:** Parquet is the Bronze input file format. Iceberg is the Silver table format that layers snapshots, manifests, metadata, schema evolution, and atomic commits over data files. Plain directory replacement cannot safely provide the same reader semantics.
- **Schema evolution:** the optional `source_note` column is additive and nullable. Incompatible type/meaning changes remain contract changes, not a convenience DDL action.
- **Bronze / Silver / Quarantine:** Bronze is immutable MDEP-8 evidence; Silver is validated rows at the documented key/grain; Quarantine retains rejected payloads and lineage. Deleting bad data hides a data-quality incident and blocks replay.
- **Idempotency:** Bronze has deterministic landing identities; Silver merges on natural keys and avoids a second logical row on a rerun. Recovery is rerun-from-Bronze, then inspect snapshots/counts.
