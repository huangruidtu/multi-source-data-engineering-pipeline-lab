# MDEP-12 implementation guide

## Scope and implemented boundary

MDEP-12 adds `warehouse/snowflake/01_setup.sql` and a runnable dbt project in `analytics/dbt`. The path is `externally managed Silver Iceberg -> Snowflake SILVER_EXT -> dbt -> Snowflake-native GOLD`. Spark and Flink remain the only Silver writers; dbt does not write back to Iceberg.

```text
S3 HadoopCatalog / Iceberg (Silver, Spark/Flink owned)
                 -> Snowflake external volume + object-store catalog integration
                 -> SILVER_EXT Iceberg tables -> dbt staging -> intermediate -> GOLD
```

The setup SQL defines an XSMALL auto-suspending warehouse, `MDEP.SILVER_EXT`, `MDEP.GOLD`, minimal transformer grants, and six external Iceberg table declarations. It intentionally uses metadata-path placeholders: HadoopCatalog has no REST/Glue catalog in V1, so a developer must discover the current metadata JSON after each Iceberg write and run `ALTER ICEBERG TABLE ... REFRESH` where applicable.

The dbt directories are logical layers, not physical schemas. The profile target schema `GOLD` is now the sole physical dbt destination; the project deliberately has no `schema: gold` overrides that could produce `GOLD_GOLD`. The DAG has six sources/staging models, two intermediates, four dimensions, two facts, and two marts. `fct_orders` is Snowflake merge-incremental by `order_id`; its post-hook removes rows no longer present in current-state Silver. `fct_payments` is a rebuilt table, deliberately avoiding stale payment-to-order links and deleted-payment residue. Dimensions are Type 1 because current-state Silver does not provide historical versions.

Currency conversion joins order date/currency to DKK rates. DKK remains identity; a missing rate leaves converted value null and exposes `missing_dkk_rate` instead of silently defaulting. Orders have no `location_id`, so `dim_locations` is intentionally not joined to facts.

## Files and validation status

CDC `applied_at` is a string from MDEP-11, so dbt freshness uses `try_to_timestamp_tz(applied_at)`; reference sources retain native `ingested_at`. An orphan payment remains in `fct_payments` and its payment-to-order relationship test warns rather than hiding evidence. Static project checks are recorded in this Story; Snowflake connectivity, external Iceberg access, `dbt run`, `dbt test`, freshness, and reconciliation are **UNVALIDATED** until credentials and S3 metadata exist. MDEP-13 is deferred.
