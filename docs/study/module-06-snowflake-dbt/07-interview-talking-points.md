# Module 06 — Interview talking points

**30 seconds:** “Iceberg remains externally managed Silver. Snowflake exposes it for dbt, while dbt owns native Gold facts, dimensions and marts with declared grain and tests.” 中文：不要说 Snowflake 写回 Silver。

**Trade-off story:** “`fct_orders` demonstrates incremental merge plus delete synchronization. I deliberately rebuild `fct_payments`, because source deletes and order relinking are more safely correct than a multi-input watermark.” 中文：incremental 不是勋章。

**Production answer:** “I would validate external-volume IAM, dbt freshness/tests, warehouse auto-suspend and credit usage. They are documented but unvalidated in this repository.” 中文：区分配置和实际运行。
