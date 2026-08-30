# MDEP-12 interview Q&A

## Why Snowflake after Iceberg, and why leave Silver external?

**Direct answer:** Iceberg is the open, Spark/Flink-owned trustworthy layer; Snowflake/dbt is the analytics and dimensional-modeling layer. **Deeper:** external volume/catalog integration reads Iceberg metadata without making Snowflake a Silver writer. **Project example:** `MDEP.SILVER_EXT` feeds native `MDEP.GOLD`. **Follow-up:** stale metadata requires a refresh against the current metadata JSON. **Senior extension:** define cross-engine snapshot-consistency and access-change runbooks.

## What does dbt add? `source()` versus `ref()`?

**Direct answer:** dbt compiles SQL, creates a dependency DAG, tests data, and documents lineage. `source()` points to externally managed Silver; `ref()` points to a dbt model. **Project example:** `stg_orders` uses `source`, `fct_orders` uses `ref('int_orders_enriched')`. **Follow-up:** dbt is not an orchestration engine; Airflow schedules/runs it. **Senior extension:** use artifacts/tests and orchestration alerts for run observability.

## What are staging, intermediate, mart, fact, dimension, star schema, and grain?

**Direct answer:** staging gives stable source-shaped names, intermediate handles reusable joins, marts expose consumers. Dimensions describe entities and facts record business processes; a star joins facts to dimensions. Grain is the exact meaning of one row. **Project example:** `fct_orders` is one current order, `mart_daily_sales` is one order date plus currency. **Follow-up:** define grain before measures to avoid fan-out. **Senior extension:** enforce grain with uniqueness/reconciliation tests.

## Business key versus surrogate key; SCD Type 1 versus Type 2?

**Direct answer:** business keys identify source rows; deterministic surrogate hashes give stable warehouse joins. Type 1 overwrites current attributes; Type 2 stores valid-time history. **Project example:** customer/product dimensions are Type 1 because Silver is current state. **Follow-up:** Type 2 would need a dbt snapshot and trustworthy historical arrivals, not invented history. **Senior extension:** choose effective dating and late-arriving dimension policy explicitly.

## What is an incremental model and how do you handle late updates/deletes?

**Direct answer:** an incremental merge updates by a `unique_key` rather than rebuilding every row. **Project example:** `fct_orders` merges by `order_id`, includes late `updated_at`/`applied_at`, and post-hook removes deleted Silver orders. **Follow-up:** full refresh rebuilds from current Silver and is the recovery option. **Senior extension:** use a lookback window or source change watermark when timestamps can arrive late.

## How do dbt tests, freshness, orphan facts, and FX conversion work here?

**Direct answer:** dbt uses unique/not-null/accepted-values/relationship/custom tests; freshness checks the most recent externally visible timestamp. **Project example:** missing customers leave a visible null key/relationship failure; DKK conversion uses order date/base currency and leaves missing rates null with a flag. **Follow-up:** do not silently drop orphans or make up FX. **Senior extension:** route/reconcile operational data-quality incidents to Silver owners.

## How do Snowflake performance and cost work?

**Direct answer:** Snowflake automatically manages micro-partitions; predicates enable pruning. **Project example:** XSMALL warehouse auto-suspends at 60 seconds, and no clustering is added to tiny lab tables. **Follow-up:** inspect query profile before scaling or clustering. **Senior extension:** monitor credits, scan bytes, dbt full-refresh frequency, and external Iceberg metadata/scan behavior.

## Snowflake versus Databricks/Redshift; dbt versus Spark/Airflow?

**Direct answer:** this project chooses Snowflake for warehouse Gold, not as a claim that alternatives are inferior. Spark/Flink own processing Silver; dbt owns SQL transformation/lineage; Airflow owns scheduling. **Follow-up:** Redshift/Databricks are outside approved V1. **Senior extension:** select platforms by ownership, workloads, skills, governance, and cost—not tool count.
