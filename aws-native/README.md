# AWS-native MDEP (AWS-01)

This directory is the isolated AWS-native variant approved by
[MDEP-66](https://ruiproject.atlassian.net/browse/MDEP-66) and
[MDEP-67](https://ruiproject.atlassian.net/browse/MDEP-67). It does not change
the original MDEP V1 implementation or its runtime claims.

## Evidence status

| Capability | Status | Evidence |
| --- | --- | --- |
| Architecture and contract mapping | CODED | `architecture.md`, `decisions/` |
| S3, KMS, IAM, Lambda and Glue Catalog | CODED | `terraform/` |
| Redshift Serverless skeleton | CODED, opt-in | `enable_redshift_serverless=false` by default |
| GitHub OIDC validation/plan workflow | CODED | `.github/workflows/aws-native-terraform.yml` |
| Terraform format, validation and policy tests | OFFLINE TESTED | `scripts/validate.ps1`, `terraform/tests/`, `tests/` |
| AWS service execution | RUNTIME DEFERRED | No resources were deployed for AWS-01 |

The captured local result is in `evidence.md`.

## Boundary

The source entities, landing envelope, quarantine requirements, grains and
canonical-writer rules remain those in
`../source-data/contracts/commerce-operations.md`. The AWS mapping is:

```text
PostgreSQL / REST / CSV / JSON
              |
        future ingestion
              v
S3 bronze/<source>/<entity>/...  ---> S3 quarantine/... (invalid evidence)
              |
       future Glue Spark
              v
S3 Iceberg + Glue Data Catalog (Silver)
              |
       Redshift Serverless
              v
        dbt-owned Gold
```

AWS-01 provisions only the secure platform skeleton. Ingestion, Glue jobs,
Iceberg tables and dbt models belong to later sprint stories.

## Offline validation

Prerequisites: Terraform 1.9+ and Python 3.11+.

```powershell
./aws-native/scripts/validate.ps1
```

The script runs only local/static operations: `terraform fmt -check`,
`terraform init -backend=false`, `terraform validate`, mocked Terraform tests,
and Python policy tests. It never runs `terraform apply`.

To inspect a no-refresh plan locally with AWS credentials already configured:

```powershell
terraform -chdir=aws-native/terraform init -backend=false
terraform -chdir=aws-native/terraform plan -refresh=false -lock=false `
  -var='allowed_account_ids=["123456789012"]'
```

`allowed_account_ids` has no default and accepts exactly one 12-digit ID. Both
the AWS provider and a Terraform precondition enforce the same account. Do not
apply without human approval, that explicit account ID and a reviewed plan.
Redshift is additionally opt-in and requires three private subnets, a security
group and a non-null budget-alert address.

The manual OIDC job trusts only this immutable, environment-scoped subject:

```text
repo:huangruidtu@45176295/multi-source-data-engineering-pipeline-lab@1350903098:environment:aws-sandbox-plan
```

Configure the exact GitHub environment protections in `runbook.md` before
running it. The workflow verifies the reviewer and `main` branch policies
before requesting AWS credentials.

## Cleanup and cost controls

- S3 is encrypted with a customer-managed KMS key, public access is blocked,
  TLS is required, versioning is enabled, and destructive bucket deletion is
  blocked in two places (`force_destroy=false` and `prevent_destroy=true`).
- Bronze and quarantine have bounded retention; Iceberg is intentionally not
  expired by an object lifecycle rule because table metadata owns cleanup.
- Lambda uses 128 MB, a 30-second timeout, 14-day logs and reserved concurrency
  of two. It has no S3 permissions in AWS-01 because its code only validates and
  logs routing metadata.
- Redshift Serverless is disabled by default, private-only when enabled, capped
  at eight RPUs and blocked unless a budget-alert recipient is supplied.
- AWS Budgets creates forecasted 80% and actual 100% email alerts when
  `budget_alert_email` is supplied. A budget is an alert, not a hard stop.

See `runbook.md` before any approved deployment or cleanup.

## 30-60 second interview explanation

> I kept the AWS work in `aws-native/` so the original MDEP evidence remains
> honest. The code maps immutable Bronze and quarantine prefixes to encrypted,
> private S3, uses Glue Catalog for the AWS Silver boundary, and leaves
> Redshift Serverless opt-in and private. Each service gets a separate IAM role:
> Lambda can only write its log group, Redshift can only read the lake prefixes
> and catalog, and GitHub OIDC can only call `GetCallerIdentity` for a
> no-refresh plan. S3 destroy protection, retention rules, capped Lambda
> concurrency, an eight-RPU Redshift ceiling and budget alerts control cost and
> accidents. Terraform validation and mocked policy tests ran offline; no AWS
> resource was deployed, so runtime remains explicitly deferred.
