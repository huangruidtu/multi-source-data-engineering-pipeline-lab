# Module 06 — Core concepts

## External Iceberg and warehouse compute
1. **Definition:** Snowflake reads Iceberg metadata/data via external volume/catalog integration. 2. **Why:** preserve Spark/Flink Silver ownership. 3. **MDEP file:** `warehouse/snowflake/01_setup.sql`. 4. **How:** `SILVER_EXT` external tables and `GOLD` native schema. 5. **Misunderstanding:** Snowflake becomes the Iceberg writer. 6. **Failure:** IAM/metadata path/query failure. 7. **Production:** least privilege, metadata refresh/governance/cost monitoring. 8. **Interview:** compute/storage separation and pruning are warehouse considerations, not runtime evidence.

## dbt DAG and dimensional grain
`source()` names external Silver, `ref()` creates model dependencies; staging/intermediate/marts are logical organisation. `dim_customers` is Type 1 current state; facts are one current order/payment; daily sales is order date plus source currency. Surrogate hashes make stable dimension joins; business keys remain semantic identity.

## Incremental trade-off
`fct_orders` is a merge incremental model with a delete anti-join. `fct_payments` is intentionally rebuilt as a table: changes/deletes and upstream order relinking are safer than fragile multi-input watermarks. Incremental is a cost/correctness trade-off, not sophistication.
