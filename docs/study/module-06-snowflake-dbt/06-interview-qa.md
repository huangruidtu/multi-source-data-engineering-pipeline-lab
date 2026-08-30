# Module 06 — Interview Q&A

### Why external Iceberg Silver and native Snowflake Gold?
**Direct answer:** it preserves one Silver writer while using Snowflake/dbt for analytical models. **Deep explanation:** physical data ownership and analytical consumption are separate. **MDEP example:** `SILVER_EXT` versus `GOLD` in setup SQL. **Follow-up:** who compacts Silver? not Snowflake in this design. **Senior extension:** catalog/IAM/governance implications. **Weak answer:** Snowflake replaces Iceberg.

### Why rebuild `fct_payments`?
**Direct answer:** deletion and upstream relationship correctness are safer than a fragile incremental watermark. **MDEP example:** materialized table SQL. **Follow-up:** Is incremental always better? no. **Senior extension:** measure cost and build a validated change strategy. **Weak answer:** incremental by default.

### What is the grain of `fct_orders`?
**Direct answer:** one current order per `order_id`. **MDEP example:** `schema.yml` and `fct_orders.sql`. **Follow-up:** SCD type? customer dimension is Type 1. **Senior extension:** late dimensions/orphan policy. **Weak answer:** describing columns without grain.
