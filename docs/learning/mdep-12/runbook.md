# MDEP-12 runbook

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install dbt-snowflake
Copy-Item analytics/dbt/profiles.yml.example $HOME\.dbt\profiles.yml
$env:SNOWFLAKE_ACCOUNT = '<account_locator>'
$env:SNOWFLAKE_USER = '<user>'
$env:SNOWFLAKE_ROLE = 'MDEP_TRANSFORMER'
cd analytics/dbt
dbt deps; dbt debug; dbt parse; dbt compile
dbt run --select dim_customers fct_orders mart_daily_sales
dbt test --select fct_orders mart_daily_sales
dbt source freshness
dbt run --select fct_orders          # incremental rerun
dbt run --full-refresh --select fct_orders
```

First replace the S3 bucket, IAM role, and six current `metadata/vN.metadata.json` paths in `warehouse/snowflake/01_setup.sql`; execute it as an authorized Snowflake administrator. Check `DESC EXTERNAL VOLUME` and grant its Snowflake IAM identity least-privilege S3 read access. Query `MDEP.GOLD.MART_DAILY_SALES` after a successful run and inspect `target/compiled` for generated SQL.

Expected static output is a parsed/compiled DAG. CDC freshness will compile as `try_to_timestamp_tz(applied_at)` because MDEP-11 emits it as a string; reference freshness uses native `ingested_at`. Expected runtime output is dbt model/test success and Gold rows; neither is currently observed. Authentication failure means profile role/account/authenticator mismatch. An Iceberg-table failure usually means S3 permissions or stale metadata path; refresh the external table with the newest metadata JSON. A warning payment-to-order relationship identifies an orphan current-state row: diagnose it in Silver rather than silently deleting it. Clean Gold deliberately with `dbt run-operation drop_relation` only after inspecting affected models; never delete Silver from dbt.
