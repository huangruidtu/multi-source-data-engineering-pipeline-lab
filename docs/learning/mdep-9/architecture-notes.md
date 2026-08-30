# MDEP-9 architecture notes

## Why Spark here, and not orchestration or Flink

Spark performs the bounded, file-oriented transformation between immutable Bronze evidence and trusted batch reference tables. Airflow remains the scheduler/producer of Bronze work; it should not contain the transformation logic. Flink will later own unbounded Kafka/CDC current state, so allowing this job to upsert PostgreSQL customers/orders would create the dual-writer failure explicitly prohibited by MDEP-6.

Bronze's job is preservation and replay. Silver's job is typed, validated, deduplicated data with a documented grain. A failed rate parse is evidence, not a reason to erase the row. The job writes it to Quarantine with the source payload and locator.

## Why Iceberg rather than plain Parquet

Parquet stores efficient columnar files, but it has no atomic table pointer, snapshot history, or safe merge semantics. Iceberg adds table metadata: a metadata JSON points to manifests, which list data files and their partitions/statistics. Each commit creates a snapshot. The job asks Iceberg to `MERGE`, so readers see either the prior snapshot or the committed snapshot, rather than a hand-managed directory of partially replaced files.

The V1 catalog is HadoopCatalog at `s3a://<bucket>/iceberg` for Spark's Hadoop filesystem client (the ADR expresses the physical S3 location as `s3://<bucket>/iceberg`). No Glue/REST/Hive service is introduced. Names and resulting table paths are stable: catalog `mdep`, namespace `silver`, tables `ref_exchange_rates` and `ref_locations`.

## Execution behavior to inspect

A DataFrame is lazy: parsing, validation, join, window deduplication, and merge become a logical plan; actions (`count`, write, `show`) cause Spark to build and run a physical plan. The broadcast country lookup avoids a large shuffle for its small reference side. The duplicate window and `groupBy` are wide operations that shuffle by key. `repartition(4, key)` is an explicit shuffle and can balance output partitions; `coalesce(n)` reduces partitions without a full shuffle, usually after a filter, but can concentrate data.

The skew exercise repeats a low-cardinality key and groups/repartitions it. In a real workload one partition might contain most of a currency/country and become a straggler. Inspect `explain("formatted")`, task durations, input sizes, and partition counts before choosing salting, a better key, broadcast, or adaptive execution. Do not “fix” the tiny lab dataset by over-tuning it.

## Production considerations

Small incremental batches can produce small files. Production would compact files, use appropriately sized partitions, monitor rejected/valid counts, snapshot growth, merge cost, skew, freshness, and S3 failures. A partial task failure should be rerun from immutable Bronze; Iceberg's atomic snapshot commit protects the table, while the lexicographic merge version tuple prevents a late replay from regressing a newer Silver state. A payload hash never proves freshness: source business time is primary, landing evidence resolves equal versions, and the hash resolves only an otherwise exact tie. Access credentials, S3 encryption/lifecycle, catalog concurrency behavior, and Snowflake external access remain subsequent operational validation.
