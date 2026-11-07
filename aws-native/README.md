# AWS-native MDEP (AWS-01 through AWS-08)

This directory is the isolated AWS-native variant approved by
[MDEP-66](https://ruiproject.atlassian.net/browse/MDEP-66) and
[MDEP-67](https://ruiproject.atlassian.net/browse/MDEP-67) and
[MDEP-68](https://ruiproject.atlassian.net/browse/MDEP-68) and
[MDEP-69](https://ruiproject.atlassian.net/browse/MDEP-69) through
[MDEP-74](https://ruiproject.atlassian.net/browse/MDEP-74). It does not change
the original MDEP V1 implementation or its runtime claims.

## Evidence status

| Capability | Status | Evidence |
| --- | --- | --- |
| Architecture and contract mapping | CODED | `architecture.md`, `decisions/` |
| PostgreSQL, REST, CSV/JSON ingestion | OFFLINE TESTED | `mdep_ingestion/`, `tests/test_aws02_*` |
| Real Parquet and manifest-last publication | OFFLINE TESTED | `serialization.py`, `publication.py` |
| Airflow data-interval orchestration | LINUX CI TESTED | `orchestration/dags/`, offline workflow |
| Thin ingestion Lambda | CODED, opt-in | `lambda/bronze_ingestion.py`; binary layer and networking are deployment gates |
| Manifest-v2 integrity reader | OFFLINE TESTED | `mdep_silver/manifest.py`, corruption/orphan tests |
| Spark 3.5.6 + Iceberg 1.10.0 MERGE | LOCAL LINUX SPARK/ICEBERG VERIFIED | `spark/tests/test_real_iceberg_pipeline.py`, GitHub Actions |
| Glue 5.1 Silver job and scoped role | CODED, opt-in | `glue/`, `terraform/glue.tf`, `terraform/identity.tf` |
| S3, KMS, IAM, Lambda and Glue Catalog | CODED | `terraform/` |
| Redshift Serverless skeleton | CODED, opt-in | `enable_redshift_serverless=false` by default |
| GitHub OIDC validation/plan workflow | CODED | `.github/workflows/aws-native-terraform.yml` |
| Terraform format, validation and policy tests | OFFLINE TESTED | `scripts/validate.ps1`, `terraform/tests/`, `tests/` |
| AWS-07 source-to-Gold integrated demo | LOCAL END-TO-END VERIFIED | `integration/run_demo.py`, `evidence-aws07.md`, Linux CI |
| AWS-08 historical reference backfill validator | LOCAL LINUX SPARK/ICEBERG TESTED | `integration/run_backfill_validation.py`, `evidence-aws08.md`, Linux CI |
| EMR Serverless 7.12 validation boundary | CODED, opt-in | `emr/`, `terraform/emr_validation.tf`; no job run is provisioned |
| AWS service execution | RUNTIME DEFERRED | No resources were deployed for AWS-01 through AWS-08 |

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
       verified Glue Spark
              v
S3 Iceberg + Glue Data Catalog (Silver)
              |
       Redshift Serverless
              v
        dbt-owned Gold
```

AWS-03 consumes only explicit, verified commit manifests and owns only
`ref_exchange_rates` and `ref_locations`. PostgreSQL commerce snapshots are
rejected at this ownership boundary; AWS-05 remains the sole planned writer for
customer, product, order and payment current state. Gold remains out of scope.

The implementation deliberately buffers one source operation so nothing is
published after a partial REST traversal. Defaults cap PostgreSQL at 100,000
records, REST/files at 50,000 records, REST at 100 pages, and Lambda source
objects at 10 MiB. Larger production feeds need streaming/multipart staging or
a service with more runtime/disk; those changes are outside AWS-02.

## Offline validation

Prerequisites: Terraform 1.9+, uv and Python 3.12 for AWS-02. The independent
AWS-03 runtime is locked to Python 3.11, Spark 3.5.6 and Iceberg 1.10.0 under
`spark/`; this explicitly avoids pretending the Python 3.12 Lambda environment
can be reused by Glue 5.1.

```powershell
./aws-native/scripts/validate.ps1
```

The script creates the locked Python environment and runs real PyArrow tests,
Lambda package validation, the AWS-native tests, Terraform checks and all V1
regressions. It never runs `terraform apply`.

The real Linux Spark/Iceberg proof runs with Java 17:

```bash
export PYTHONPATH=aws-native
export PYSPARK_SUBMIT_ARGS='--packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.10.0 pyspark-shell'
uv sync --project aws-native/spark --locked --python 3.11
uv run --project aws-native/spark --locked python -m unittest discover -s aws-native/spark/tests -v
```

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
- The Glue job is absent by default and blocked without a budget recipient.
  When enabled it uses 2-5 `G.1X` workers, a 10-60 minute timeout, zero retries
  and concurrency one. Its separate role can read `bronze/` and its own script,
  but can write only `iceberg/` and the one Silver Glue database.
- Redshift Serverless is disabled by default, private-only when enabled, capped
  at eight RPUs and blocked unless a budget-alert recipient is supplied.
- AWS Budgets creates forecasted 80% and actual 100% email alerts when
  `budget_alert_email` is supplied. A budget is an alert, not a hard stop.

See `runbook.md` before any approved deployment or cleanup.

AWS-02 interview evidence is in `aws-02-interview.md`; exact offline results are
in `evidence-aws02.md`. AWS-03 evidence and the speakable explanation are in
`aws-03-interview.md` and `evidence-aws03.md`.

`interval_start`/`interval_end` describe source data, `source_extract_ts` comes
from the source record, and `ingested_at` is an actual run-start timestamp
captured once by the orchestrator and reused for task retries. Lambda callers
must supply that stable `ingested_at`; it is intentionally not derived from the
historical interval boundary.

Replay policy is encoded in manifest v2: a same-run retry has the same physical
and source-content checksums; an independent replay may have a later
`ingested_at` but returns `already_published` when every other row field matches,
leaving the first immutable object untouched. Business or source-provenance
changes alter the source-content checksum and remain conflicts.

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

AWS-05 adds a separate `mdep_cdc` package for the real AWS DMS Kinesis envelope,
a persistent ordering oracle, a Spark/Iceberg v2 current-state writer, and an
opt-in DMS/Kinesis/Firehose Terraform boundary. Firehose owns only the immutable
newline-delimited S3 archive. The Iceberg writer owns `core_customers`,
`core_products`, `core_orders`, and `core_payments`; deletes remain durable
tombstones and Gold staging exposes only live rows. See `aws-05-interview.md`,
`evidence-aws05.md`, and ADR-0003. No AWS-05 resource is deployed by CI.

AWS-06 adds a strict JSON source registry around the existing pipeline. The
`devices-json-reference` example proves config-to-Parquet-to-commit-manifest,
replay, backfill, quarantine, conflict and interrupted-manifest recovery without
adding a Silver writer. `mdep-execution-event-v1` supplies safe JSON events;
opt-in filters reuse the Lambda log group without widening IAM or OIDC. Run
`scripts/run_platform_demo.py`; see `aws-06-interview.md`, `evidence-aws06.md`
and ADR-0004.

AWS-07 joins the implemented boundaries into a real local execution: the AWS-02
REST extractor/pipeline publishes verified Bronze, AWS-03 and AWS-05 perform
real Spark/Iceberg merges, and AWS-04 runs the actual incremental dbt project on
the checksummed Silver export. The one-command demonstration is:

```bash
uv run --project aws-native/spark --locked python aws-native/integration/run_demo.py
```

Use `--work-dir /tmp/mdep-aws07-demo` with a new path to retain the evidence
artifacts. See `evidence-aws07.md` for prerequisites, observed reconciliation,
failure/recovery evidence, artifacts and runtime limitations; see
`aws-07-interview.md` for the speakable explanation. This is local end-to-end
evidence, not an AWS runtime claim.

AWS-08 adds a read-only historical reference validator, not another Silver
writer. Its mandatory Linux proof runs real AWS-02 Bronze publication, AWS-03
Spark/Iceberg writes and independent current-state reconciliation:

```bash
uv run --project aws-native/spark --locked python aws-native/integration/run_backfill_validation.py
```

The optional EMR Serverless application is disabled by default, pins release
7.12.0, has no pre-initialized capacity and creates no job run. See
`evidence-aws08.md`, `aws-08-interview.md` and the AWS-08 section in
`runbook.md`. Local Linux evidence does not prove Glue Catalog, S3FileIO, IAM,
KMS or EMR runtime behavior.
