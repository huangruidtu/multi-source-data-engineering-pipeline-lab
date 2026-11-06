# ADR-0002: Redshift-native Gold with a fixture-backed offline target

## Status

Accepted for MDEP-70 / AWS-04 on 2026-11-06.

## Context

AWS-03 owns only `ref_exchange_rates` and `ref_locations` in Glue/Iceberg.
AWS-05, the canonical CDC writer for `core_customers`, `core_products`,
`core_orders` and `core_payments`, is not implemented. Claiming a complete
Silver-to-Gold runtime would therefore be false. At the same time, AWS-04 must
exercise the analytical logic, incremental state transitions and deletion
guardrails without deploying paid AWS resources.

Redshift can reference Glue-cataloged Iceberg tables through an external schema,
and dbt-redshift supports the `merge` incremental strategy. The external schema
is read-only input; Gold tables remain native Redshift relations.

## Decision

- Keep AWS-04 in the isolated `aws-native/gold` dbt project. Do not modify the
  original Snowflake-oriented V1 project in `analytics/dbt`.
- In the Redshift target, resolve reference inputs to the existing AWS-03 Glue
  tables and commerce inputs to the explicitly future `cdc_silver` AWS-05
  boundary.
- In the DuckDB target, replace only that source boundary with bounded
  `fixture_*` commerce seeds and `silver_bridge_*` exports matching the AWS-03
  physical schema. Both targets execute the same staging, enrichment, Gold and
  reconciliation SQL.
- Materialize Gold as native warehouse tables. Use deterministic keys and
  `DECIMAL` monetary types; do not sum original amounts across currencies.
- Use business-version-first incremental ordering. A newer rate can correct an
  unchanged order, but a stale business row cannot overwrite a newer one.
- Never infer a hard delete from absence in a normal run. A separate operation
  requires `full_snapshot_verified=true` and rejects a delete fraction above a
  reviewed threshold.
- Keep Redshift disabled by default and private when enabled. In addition to the
  8-RPU capacity cap and budget-recipient gate, deactivate queries after 16
  monthly RPU-hours through `aws_redshiftserverless_usage_limit`.

## Consequences

The DuckDB result is meaningful execution evidence for transformation and state
semantics, but not proof of a Redshift connection, Redshift query execution,
Glue authorization, Spectrum scanning or AWS cost. The Redshift target is parsed
with the pinned adapter and its bootstrap/IAM contracts are statically tested.
An authorized AWS-05 source and private Redshift connection are required for the
runtime handoff.

Official implementation references reviewed for this decision:

- [Redshift external schemas](https://docs.aws.amazon.com/redshift/latest/dg/r_CREATE_EXTERNAL_SCHEMA.html)
- [Referencing Iceberg tables in Redshift](https://docs.aws.amazon.com/redshift/latest/dg/referencing-iceberg-tables.html)
- [Redshift MERGE](https://docs.aws.amazon.com/redshift/latest/dg/r_MERGE.html)
- [dbt incremental strategies](https://docs.getdbt.com/docs/build/incremental-strategy)
- [Terraform Redshift Serverless usage limit](https://registry.terraform.io/providers/hashicorp/aws/latest/docs/resources/redshiftserverless_usage_limit)
