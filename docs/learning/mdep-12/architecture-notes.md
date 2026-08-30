# MDEP-12 architecture notes

Iceberg is the shared lakehouse Silver layer: S3 stores data and metadata while HadoopCatalog is controlled by Spark/Flink. Snowflake reads an external Iceberg definition using an external volume plus `OBJECT_STORE`/`ICEBERG` catalog integration. Snowflake owns only database metadata for that read path and native Gold tables; it does not own, compact, or become a second writer for Silver.

dbt is the SQL DAG and test/documentation layer. Its staging/intermediate/mart directories are logical layers only: all models resolve to profile schema `MDEP.GOLD`, rather than separate or accidentally suffixed schemas. `source()` names an externally managed Silver relation; `ref()` declares dependency on another dbt model. A fact's grain is stated before its measures: orders and payments are one current row per business ID; daily sales is one order date plus currency.

Business keys (`customer_id`, `order_id`) identify source entities. Deterministic dbt-utils surrogate hashes create dimension keys for stable fact joins. This is Type 1 current-state modeling: updates replace dimension attributes. Type 2 is explicitly deferred because Silver physical deletes/current rows cannot supply prior valid intervals.

Only `fct_orders` is incremental because it is the bounded learning/cost example and includes a current-state delete anti-join. `fct_payments` is a table rebuild: payment deletion and changes to its upstream order linkage become correct without a fragile multi-input incremental watermark. Orphan payments are left-joined and visible through a warning relationship test. CDC freshness converts the string `applied_at` with `try_to_timestamp_tz`; external Iceberg freshness remains runtime unvalidated.
