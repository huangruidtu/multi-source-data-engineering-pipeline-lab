# ADR-0001: Managed AWS platform boundaries for the sprint variant

**Status:** Accepted for AWS-01

**Story:** MDEP-67

**Date:** 2026-11-05

## Context

MDEP V1 proves contracts and correctness offline with Airflow, HadoopCatalog
Iceberg and Snowflake/dbt boundaries. The interview sprint asks for a separate
AWS-native implementation emphasizing S3, Glue, Redshift, Terraform, security,
cost and reusable platform behavior without rewriting or overstating V1.

## Decision

Create an isolated `aws-native/` variant with:

- one encrypted S3 lake bucket with explicit Bronze, quarantine and Iceberg
  prefixes;
- Glue Data Catalog as the AWS-native Silver namespace;
- Lambda only as a bounded event-routing/control-plane skeleton;
- private, opt-in Redshift Serverless as the analytical target;
- one IAM role per runtime boundary and a separate GitHub OIDC plan role;
- Terraform as the declarative control plane, with offline/mock policy tests;
- no automatic apply and no AWS deployment in AWS-01.

The AWS variant supersedes V1 ADR-0001 only inside `aws-native/`: Glue replaces
HadoopCatalog because it is the managed AWS integration point for Glue jobs,
Iceberg discovery and Redshift metadata. Original V1 remains unchanged.

## Consequences

Managed services reduce cluster operations and create a concise interview path,
but increase AWS coupling and require careful IAM, catalog and cost controls.
Glue Catalog makes table discovery easier but does not solve concurrent-writer
correctness; canonical writer ownership remains mandatory. Serverless removes
cluster lifecycle work but can still spend money, so it is disabled by default
and capacity-capped. A single lake bucket keeps the sprint small; prefixes and
IAM policies enforce the logical boundaries that separate buckets might enforce
in a larger production platform.

## Alternatives considered

- **Keep HadoopCatalog:** minimal change, but weakly demonstrates the requested
  managed AWS metadata boundary.
- **Provision EMR/EKS:** broader engine control, but adds operations and cost
  outside the two-week priority.
- **Separate bucket per layer:** stronger isolation, but more policy and
  replication surface than AWS-01 needs. Revisit for production accounts.
- **Always-on Redshift/Glue jobs:** creates runtime evidence only after an
  approved deployment and budget; code alone must not imply that evidence.
