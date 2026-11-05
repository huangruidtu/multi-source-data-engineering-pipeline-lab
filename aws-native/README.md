# AWS-native MDEP (AWS-01 and AWS-02)

This directory is the isolated AWS-native variant approved by
[MDEP-66](https://ruiproject.atlassian.net/browse/MDEP-66) and
[MDEP-67](https://ruiproject.atlassian.net/browse/MDEP-67) and
[MDEP-68](https://ruiproject.atlassian.net/browse/MDEP-68). It does not change
the original MDEP V1 implementation or its runtime claims.

## Evidence status

| Capability | Status | Evidence |
| --- | --- | --- |
| Architecture and contract mapping | CODED | `architecture.md`, `decisions/` |
| PostgreSQL, REST, CSV/JSON ingestion | OFFLINE TESTED | `mdep_ingestion/`, `tests/test_aws02_*` |
| Real Parquet and manifest-last publication | OFFLINE TESTED | `serialization.py`, `publication.py` |
| Airflow data-interval orchestration | LINUX CI TESTED | `orchestration/dags/`, offline workflow |
| Thin ingestion Lambda | CODED, opt-in | `lambda/bronze_ingestion.py`; binary layer and networking are deployment gates |
| S3, KMS, IAM, Lambda and Glue Catalog | CODED | `terraform/` |
| Redshift Serverless skeleton | CODED, opt-in | `enable_redshift_serverless=false` by default |
| GitHub OIDC validation/plan workflow | CODED | `.github/workflows/aws-native-terraform.yml` |
| Terraform format, validation and policy tests | OFFLINE TESTED | `scripts/validate.ps1`, `terraform/tests/`, `tests/` |
| AWS service execution | RUNTIME DEFERRED | No resources were deployed for AWS-01 or AWS-02 |

The captured local result is in `evidence.md`.

## Boundary

The source entities, landing envelope, quarantine requirements, grains and
canonical-writer rules remain those in
`../source-data/contracts/commerce-operations.md`. The AWS mapping is:

```text
PostgreSQL / REST / CSV / JSON
              |
    shared AWS-02 ingestion
              v
S3 data.parquet --manifest last--> S3 _COMMITTED.json
              |
              `--------------------> S3 quarantine/... (invalid evidence)
              |
       future Glue Spark
              v
S3 Iceberg + Glue Data Catalog (Silver)
              |
       Redshift Serverless
              v
        dbt-owned Gold
```

AWS-02 adds executable Bronze ingestion only. Glue jobs, Iceberg tables and dbt
models remain later sprint stories; a PostgreSQL snapshot is replay/backfill
evidence and is not a second canonical CDC Silver writer.

The implementation deliberately buffers one source operation so nothing is
published after a partial REST traversal. Defaults cap PostgreSQL at 100,000
records, REST/files at 50,000 records, REST at 100 pages, and Lambda source
objects at 10 MiB. Larger production feeds need streaming/multipart staging or
a service with more runtime/disk; those changes are outside AWS-02.

## Offline validation

Prerequisites: Terraform 1.9+, uv and Python 3.12.

```powershell
./aws-native/scripts/validate.ps1
```

The script creates the locked Python environment and runs real PyArrow tests,
Lambda package validation, the AWS-native tests, Terraform checks and all V1
regressions. It never runs `terraform apply`.

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
- The ingestion Lambda is absent by default. Enabling it requires an alert
  recipient, exact source settings and a reviewed Python 3.12 dependency layer;
  it is capped at 1,024 MB, 300 seconds, 1,024 MB ephemeral storage and reserved
  concurrency two. IAM is limited to `source/reference/`, `bronze/`,
  `quarantine/`, one KMS key, one log group and the exact PostgreSQL secret.
- S3 existence checks issue `ListObjectsV2` with the complete object key and
  `MaxKeys=1`; IAM permits listing only `bronze/*` and `quarantine/*`. This
  yields an unambiguous missing-object result without treating AccessDenied as
  absence or exposing unrelated bucket keys.
- Redshift Serverless is disabled by default, private-only when enabled, capped
  at eight RPUs and blocked unless a budget-alert recipient is supplied.
- AWS Budgets creates forecasted 80% and actual 100% email alerts when
  `budget_alert_email` is supplied. A budget is an alert, not a hard stop.

See `runbook.md` before any approved deployment or cleanup.

AWS-02 interview evidence is in `aws-02-interview.md`; exact offline results are
in `evidence-aws02.md`.

`interval_start`/`interval_end` describe source data, `source_extract_ts` comes
from the source record, and `ingested_at` is an actual run-start timestamp
captured once by the orchestrator and reused for task retries. Lambda callers
must supply that stable `ingested_at`; it is intentionally not derived from the
historical interval boundary.

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
