# MDEP-13 reconciliation checks

Run these checks for one bounded logical interval and save results under
`validation/evidence/<run-id>/reconciliation/`. A check is not complete until
it records the query, run identifier, counts, exception keys, and an explicit
pass/fail decision.

## Semantics before counts

Bronze is append history and can contain replays, duplicates, malformed input,
and old schemas. Silver is current state and should retain one row per business
key. Gold is analytical current state and may intentionally filter or aggregate
records. Therefore matching all three layer row counts is neither expected nor
sufficient; compare the correct semantic sets and then anti-join business keys.

## Reusable checks

| Check | Compare | Required result |
| --- | --- | --- |
| R01 | source `orders` → Silver `core_orders` | current business keys, count, and key-level attributes agree for selected scope |
| R02 | source `payments` → Silver `core_payments` | current business keys, count, and key-level attributes agree |
| R03 | Silver `core_orders` → Gold `fct_orders` | stated eligible keys and values reconcile; excluded keys are explained |
| R04 | Silver `core_payments` → Gold `fct_payments` | keys reconcile, with orphan payment exceptions recorded |
| R05 | Gold `fct_orders` → `mart_daily_sales` | grouped order-date/currency amounts equal the mart aggregation |
| R06 | enriched orders | count `missing_dkk_rate` and retain exception keys |
| R07 | Gold payments | count/list `order_id` values absent from `fct_orders` |
| R08 | source deletes → Silver/Gold | deleted keys are absent or have explicitly documented retention semantics |
| R09 | Silver current-state tables | duplicate business keys equal zero |
| R10 | source/Silver/Gold contracts | required-field null counts equal zero or have documented exceptions |

Use [snowflake-dbt.sql](snowflake-dbt.sql) for Gold/Silver-external queries and
[spark-iceberg.sql](spark-iceberg.sql) for Spark/Iceberg equivalents. Replace
the commented run boundaries; never paste credentials into either file.
