# MDEP-12 interview talking points

**30 seconds:** “I exposed Spark/Flink-owned Iceberg Silver to Snowflake through an external volume and object-store catalog integration, then built a dbt Gold DAG. dbt owns Type 1 dimensions, incremental current-state facts, quality tests, and analytical marts; it never writes back to Silver.”

**中文简述：**“我通过 external volume 和 object-store catalog integration 让 Snowflake 读取由 Spark/Flink 管理的 Iceberg Silver，然后使用 dbt 构建 Gold。dbt 负责 Type 1 维度、增量事实表、质量测试和分析 mart，但不会写回 Silver。”

**60-second architecture:** “Silver remains external because it is the shared lakehouse truth and its catalog/snapshots are owned by Spark/Flink. Snowflake maps it as `SILVER_EXT`; dbt reads it with `source()`, creates staging and intermediate models with `ref()`, and writes Snowflake-native `GOLD`. That gives a clear ownership boundary and usable analytics lineage.”

**Two-minute deep dive:** “The fact grain comes first: one current order and one current payment. Customer/product/location/date are dimensions; customer/product use deterministic surrogate hashes and Type 1 because the input is current state. `fct_orders` uses a merge incremental `order_id`, a late-update predicate, and deletion reconciliation. `fct_payments` is a rebuilt table, because an upstream order delete or relink can otherwise leave an unchanged payment stale. Daily sales aggregates date plus source currency. We join DKK FX by order date and source currency; if the rate is missing, conversion remains null and a flag makes it visible. Orders do not have `location_id`, so I deliberately do not fabricate a location fact join.”

**Tests/failure/performance:** “I test keys, statuses, relationships, and negative amounts; an orphan payment stays visible and its payment-to-order relationship warns instead of disappearing. CDC freshness converts the string `applied_at` with `try_to_timestamp_tz`; reference freshness uses native `ingested_at`, and both still need runtime validation. The dbt folders are logical layers but all resolve to one physical `MDEP.GOLD` schema. The warehouse is XSMALL with auto-suspend; Snowflake handles micro-partitions and pruning, so I would inspect query profile before adding clustering. Runtime Snowflake, Iceberg refresh, dbt run/test, and cost observations remain unvalidated.”

**Production improvements:** “Add scheduled dbt via Airflow, metadata-refresh/reconciliation alarms, a late-arrival lookback policy, controlled SCD2 only when historic input exists, query/cost monitoring, and least-privilege review.”
