# MDEP-12 architecture notes

Iceberg is the shared lakehouse Silver layer: S3 stores data and metadata while HadoopCatalog is controlled by Spark/Flink. Snowflake reads an external Iceberg definition using an external volume plus `OBJECT_STORE`/`ICEBERG` catalog integration. Snowflake owns only database metadata for that read path and native Gold tables; it does not own, compact, or become a second writer for Silver.

dbt is the SQL DAG and test/documentation layer. `source()` names an externally managed Silver relation; `ref()` declares dependency on another dbt model. Staging normalizes names/types, intermediate models perform controlled joins, and marts present a star-shaped analytical surface. A fact's grain is stated before its measures: orders and payments are one current row per business ID; daily sales is one order date plus currency.

Business keys (`customer_id`, `order_id`) identify source entities. Deterministic dbt-utils surrogate hashes create dimension keys for stable fact joins. This is Type 1 current-state modeling: updates replace dimension attributes. Type 2 is explicitly deferred because Silver physical deletes/current rows cannot supply prior valid intervals.

Snowflake micro-partitions are managed automatically. Small lab tables receive no clustering key; pruning comes from predicates such as order date. The XSMALL warehouse auto-suspends after 60 seconds to limit idle credits. Incremental merge saves scan/compute versus full refresh, but its late-update predicate and delete behavior must be tested against real Silver snapshots.
