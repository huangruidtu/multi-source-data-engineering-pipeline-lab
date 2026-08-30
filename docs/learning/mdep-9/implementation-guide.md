# MDEP-9 implementation guide — batch Bronze to Silver Iceberg

## What is implemented

MDEP-9 implements the batch-owned reference-data path: MDEP-8 Bronze Parquet in the object-store layout is read by `processing/spark/silver_batch.py`, validated, deduplicated, enriched, and merged into two Iceberg tables: `mdep.silver.ref_exchange_rates` and `mdep.silver.ref_locations`. The catalog is the approved HadoopCatalog. Production configuration uses an S3A warehouse such as `s3a://<bucket>/iceberg`; a local `file:///...` warehouse is only a reproducible learning substitute for the same catalog class.

It intentionally does **not** create `core_customers`, `core_products`, `core_orders`, or `core_payments`. Those PostgreSQL current-state tables are reserved for the later Debezium/Kafka/Flink path under the MDEP-6 ownership contract.

```text
REST source -> MDEP-8 Bronze Parquet -> PySpark checks + dedup
                                           |-> Silver Iceberg ref_* tables
                                           `-> Quarantine Parquet evidence
```

## Files and job structure

- `processing/spark/contracts.py` keeps the pure, unit-testable ownership, normalization, key, incremental-boundary, and country-reference rules.
- `processing/spark/silver_batch.py` is the compact Spark executable. `build_spark` configures Iceberg; `validate_*` performs native DataFrame validation; `split_valid_and_quarantine` deterministically chooses a duplicate winner; `merge_iceberg` provides rerun-safe writes.
- `scripts/run-mdep-9-silver.ps1` supplies the matching Iceberg Spark runtime package to `spark-submit`.
- `tests/test_silver_contracts.py` covers key generation, normalization, invalid values, reference integrity, incremental boundaries, hash determinism, and CDC ownership protection.

## Inputs, schemas, and quality rules

The job follows MDEP-8's source-aligned paths: `bronze/rest_api/exchange_rates/**` and `bronze/rest_api/locations/**`. It expects the MDEP-6 landing envelope (`ingestion_id`, `source_locator`, `source_extract_ts`, `ingested_at`, and `record_hash`) in addition to business fields.

`ref_exchange_rates` has the compound natural key `(rate_date, base_currency, quote_currency)`. Dates/timestamps are parsed, currencies are upper-cased and required to be three characters, base and quote must differ, and rate is cast to `DECIMAL(18,6)` and must be positive. `ref_locations` has `location_id`; its identity/name/timezone are required, `updated_at` is parsed, and `country_code` is validated through a deliberately small in-code reference map (`DK`, `NO`). The broadcast join that adds `region` is the representative enrichment and referential-integrity check.

Rows that fail a rule retain the original payload, MDEP-8 lineage fields, `rejection_reason`, entity, and logical date under `quarantine/silver/<entity>`. They are never silently dropped.

## Deduplication and writes

For valid rows only, a window partitions by the natural key and orders by source business version (`retrieved_at` for rates, `updated_at` for locations), then `source_extract_ts`, `ingested_at`, and `record_hash`, all descending. Rank 1 wins; every lower-ranked row is Quarantined as `duplicate_business_key_non_winner`. The final hash ordering makes an exact timestamp tie deterministic.

The target tables are created in namespace `mdep.silver` using `USING iceberg`; their locations are deterministic below the configured HadoopCatalog warehouse. A SQL `MERGE` uses the natural key. Same-key/same-hash reruns create no second logical row; a changed incoming version updates the current reference value. Quarantine is overwritten by logical-date partition so replays do not multiply evidence for that interval.

Bounded incremental runs filter `ingested_at >= start AND ingested_at < end`. This is a batch processing boundary, not a Flink event-time watermark. A late/updated source item is handled by running the interval that contains its Bronze landing time; its newer source timestamp wins the merge.

## Controlled schema evolution and inspection

`--additive-schema-evolution` issues `ADD COLUMN source_note STRING` and supplies a nullable demonstration value. The operation never renames/drops/changing an existing column. `--inspect` prints schema/partition information and formatted plans, including a group-by shuffle; `--skew-exercise` adds a deliberately skew-prone key distribution and repartitions it for plan inspection. `inspect_snapshots` reads Iceberg's `snapshots` metadata table after a write.

## Boundaries and deferred work

The implementation configures S3-backed HadoopCatalog but does not claim an actual S3, Spark, or Iceberg run. It does not add Glue, REST Catalog, Hive, or any new platform. MDEP-10+ own CDC, Kafka, Flink, Snowflake, dbt, and Gold. MDEP-8's Docker/Airflow/PostgreSQL runtime validation remains separate outstanding debt.
