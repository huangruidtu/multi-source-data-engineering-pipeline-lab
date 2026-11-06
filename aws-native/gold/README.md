# AWS-04 Redshift and dbt Gold

Evidence label: **LOCAL DUCKDB FIXTURE BRIDGE VERIFIED; REDSHIFT RUNTIME
DEFERRED**. No AWS resource is required by these commands.

The project deliberately does not claim AWS-05 exists. `fixture_*` seeds stand
in for its future commerce current-state tables only in the DuckDB target.
`silver_bridge_*` seeds mirror the AWS-03 `ref_exchange_rates` and
`ref_locations` columns. The Redshift target instead resolves those references
to the `silver_iceberg` external schema and commerce to `cdc_silver`.

## Reproduce

From the repository root:

```bash
uv sync --project aws-native/gold --locked --python 3.12
uv run --project aws-native/gold --locked python -m unittest discover -s aws-native/gold/tests_python -v
uv run --project aws-native/gold --locked dbt parse --project-dir aws-native/gold --profiles-dir aws-native/gold --target redshift
uv run --project aws-native/gold --locked python aws-native/gold/scripts/run_integration.py
```

The integration command performs a full seed/build/test and then verifies exact
replay, new and updated orders, stale replay, a Type 1 customer change, an FX
correction, partial-feed safety, a verified delete and mass-delete rejection.
It prints one machine-readable `AWS04_GOLD_EVIDENCE` record.

## Redshift handoff

1. Explicitly enable the private Terraform workgroup only with three reviewed
   subnets, a reviewed security group, one approved AWS account and a budget
   recipient. Terraform manages the admin password; no credential is committed.
2. Render `redshift/bootstrap_external_schema.sql` with the external schema,
   existing Glue database, Region and associated Redshift IAM role ARN. The SQL
   maps the database only; it never creates or overwrites AWS-03 tables.
3. Supply `MDEP_REDSHIFT_HOST`, `MDEP_REDSHIFT_DATABASE`,
   `MDEP_REDSHIFT_SCHEMA`, `MDEP_REDSHIFT_USER` and
   `MDEP_REDSHIFT_PASSWORD` securely. The dbt principal needs schema usage/create
   and native Gold select/insert/update/delete/drop privileges; the service role
   is separately limited to Glue reads, `iceberg/*` reads and KMS-via-S3 decrypt.
4. Replace the fixture boundary only after AWS-05 publishes the four declared
   `core_*` contracts. Do not point Gold at AWS-02 batch snapshots as a competing
   current-state writer.

The Redshift workgroup remains private, so the executing runner also needs an
approved private network path. A local parse is not a live connection test.
