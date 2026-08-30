# Module 06 — Code walkthrough

Reading order: `warehouse/snowflake/01_setup.sql` → `analytics/dbt/dbt_project.yml` → `models/sources.yml` → staging → intermediate → marts → `models/schema.yml` → tests.

The SQL template creates `MDEP_TRANSFORM_WH`, external volume/catalog integration placeholders, `MDEP.SILVER_EXT`, and `MDEP.GOLD`; placeholders require real credentials/IAM. `sources.yml` documents Silver input. `stg_*` normalise relations; `int_orders_enriched.sql` uses safe DKK conversion; marts declare grain. `fct_orders.sql` has merge incremental config and delete sync. `fct_payments.sql` is materialized as `table`. `schema.yml` tests `not_null`, `unique`, accepted values and a warning relationship for orphan payments. Note the reviewed risk: dbt models must resolve to `MDEP.GOLD`, not accidental `GOLD_GOLD` schemas.
