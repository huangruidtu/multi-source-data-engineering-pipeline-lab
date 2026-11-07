# AWS-native architecture and contract mapping

## Decisions grounded in MDEP V1

The AWS variant retains the V1 invariants instead of redesigning the data
model:

1. Bronze remains immutable, source-aligned and replayable.
2. Invalid input is retained in quarantine with diagnosis and replay evidence.
3. A logical Silver dataset has one canonical writer.
4. Gold is an analytical ownership boundary, not a second ingestion layer.
5. Runtime claims require runtime evidence.

The physical object prefixes preserve the V1 landing convention:

| V1 contract | AWS path/catalog | Planned owner |
| --- | --- | --- |
| PostgreSQL snapshots and REST/file extracts | `s3://<bucket>/bronze/<source>/<entity>/ingest_date=YYYY-MM-DD/` | AWS-02 ingestion |
| Rejected records and reasons | `s3://<bucket>/quarantine/<source>/<entity>/ingest_date=YYYY-MM-DD/` | rejecting processor |
| Reference Silver: `exchange_rates`, `locations`, categories, devices | `mdep_<env>_silver` Glue database and `iceberg/` objects | AWS-03 Glue Spark |
| Commerce current state: customers, products, orders, payments | same catalog, separate tables | AWS-05 CDC adapter/writer |
| Dimensions, facts and marts | Redshift database | AWS-04 dbt |

The existing contract currently names Spark as the batch-reference writer and
Flink as the CDC-commerce writer. AWS-01 does not silently change that
ownership: it creates shared boundaries only. Later stories must record any
approved engine change while keeping one writer per table.

## Trust boundaries

```text
GitHub Actions --OIDC sub/aud--> plan role --sts:GetCallerIdentity only

Lambda service --assume role--> Lambda role
                                      |--read--> source/reference/, Bronze, quarantine
                                      |--conditional write--> Bronze, quarantine
                                      |--KMS via S3--> one data key
                                      |--read--> one PostgreSQL secret
                                      `--write--> one CloudWatch log group

Glue service --assume role--> AWS-03 Silver role
                                      |--read--> explicit bronze manifests/data + own script
                                      |--write/delete--> iceberg/ only
                                      |--manage--> one Silver Glue database/tables
                                      |--KMS via S3--> one data key
                                      `--write--> one CloudWatch log group

Redshift service --assume role--> Redshift role
                                      |--read--> iceberg/ prefix
                                      |--read--> one Glue database/tables
                                      `--decrypt via S3--> one KMS key
```

The CI trust policy pins `aud` to AWS STS and uses the one exact immutable,
environment-scoped `sub` emitted for this post-2026-07-15 repository:

```text
repo:huangruidtu@45176295/multi-source-data-engineering-pipeline-lab@1350903098:environment:aws-sandbox-plan
```

This follows GitHub's current [OpenID Connect reference](https://docs.github.com/en/actions/reference/security/oidc),
which specifies immutable `owner@ID/repository@ID` segments for new repositories
and an `environment` context when a job references an environment.

The environment context replaces branch and pull-request contexts for that job.
The workflow grants `id-token: write` only to the manual OIDC plan job and
checks that the environment has a required reviewer with self-review disabled
plus a single `main` deployment branch policy before requesting AWS
credentials. Pull-request validation needs no AWS credentials.

## Service boundaries

- **S3/KMS:** durable evidence boundary. Default SSE-KMS, TLS-only bucket
  policy, public-access block, ownership enforcement and versioning are all
  explicit resources so a review can see the controls.
- **Lambda:** the AWS-02 adapter executes one explicitly supplied source
  operation through the shared package. It is not connected to an S3 event, so
  it cannot create a notification loop. Terraform leaves it disabled until a
  reviewed PyArrow/psycopg layer, source configuration, budget recipient and
  PostgreSQL connectivity are available.
- **Glue Spark and Catalog:** an opt-in Glue 5.1 job runs the same transformation
  module tested with local HadoopCatalog. The AWS configuration uses
  GlueCatalog/S3FileIO and built-in Iceberg 1.10.0. No crawler is created because
  contracts and table ownership are explicit.
- **Redshift Serverless and dbt:** private, encrypted, enhanced-VPC-routing Gold
  target. It is disabled by default, fixed at 8 RPUs and query-deactivated after
  16 monthly RPU-hours. The SQL is executed through a DuckDB fixture bridge and
  parsed for Redshift; no live Redshift runtime is claimed.
- **AWS Budgets:** visibility guardrail. It alerts but cannot guarantee a hard
  spend stop, so Redshift opt-in and capacity caps remain primary controls.

## Failure and recovery reasoning

| Failure | Containment/recovery |
| --- | --- |
| Accidental public S3 configuration | Public-access block plus TLS-only policy; policy tests fail if controls are removed. |
| Accidental bucket deletion | Terraform lifecycle rejects destroy and S3 refuses deletion while objects remain. A reviewed two-step change is required. |
| Compromised GitHub job | Subject/audience trust restriction and a role with no mutation permissions limit blast radius. |
| Lambda retry storm | The function is absent by default; when enabled, reserved concurrency two and a 300-second timeout bound cost and data-plane writes. |
| Source operation retry | The UUIDv5 run identity and conditional object writes make identical replay a verified no-op. |
| Missing S3 object | The adapter lists the exact target key under an IAM-restricted Bronze/quarantine prefix before HEAD; a list 403 remains a failure, never a false absence. |
| REST page 3 fails | Extraction finishes all pages before serialization; no canonical data or manifest is written. |
| Data write succeeds, manifest fails | Data is an invisible orphan; retry verifies the stored Parquet, record count and source-content checksum, then publishes a manifest describing those actual first bytes. |
| Bronze manifest or Parquet is corrupt | AWS-03 verifies versions, identity, path, row count and both checksums before creating any Silver write plan; the run fails closed. |
| Silver commit succeeds, acknowledgment fails | Re-run sees the committed Iceberg state: exact rows are unchanged, quarantine and reconciliation use deterministic MERGE keys, then acknowledgment can be retried. No cross-table transaction is claimed. |
| Older reference data arrives later | MERGE compares business version, source extraction time and ingestion time in that order; an older business version cannot win because of arrival time or hash. |
| Redshift idle/runaway cost | Resource absent by default; enabled use requires a budget recipient and private networking, stays at 8 RPUs, and deactivates queries at 16 monthly RPU-hours. |
| KMS key deletion | 30-day deletion window and Terraform destroy protection allow recovery before deletion completes. |
| Bad/late source data | Bronze remains replayable; quarantine retains evidence; later writers must apply existing V1 ordering rules. |

## Deliberate gaps after AWS-04

- No AWS account/runtime validation and no `terraform apply` evidence.
- No remote Terraform state/locking bootstrap.
- No VPC, NAT gateway or interface endpoints; approved existing private subnets
  are inputs for Redshift to avoid hiding networking cost.
- No automatic AWS Glue trigger or live Redshift/dbt execution. The Glue job
  requires an explicit controlled list of `_COMMITTED.json` keys at invocation;
  Gold uses bounded commerce fixtures until AWS-05 owns the `core_*` tables.
- Airflow is authored against its public SDK and parsed in Linux CI. Native
  Windows is not a supported Airflow runtime; its interval resolver is still
  tested locally there.
- Lambda binary dependencies and approved PostgreSQL VPC routing are deployment
  inputs; neither is fabricated by AWS-02.
- Glue AWS runtime, IAM and S3FileIO behavior remain deployment-deferred. The
  Spark logic and real Iceberg commits are verified on Linux CI, not on AWS.

## Time semantics

- The logical `[interval_start, interval_end)` selects source data and remains
  part of deterministic run identity.
- `source_extract_ts` is the event/update/extraction time supplied by the
  source record.
- `ingested_at` is a real operational timestamp captured once when the Airflow
  DAG run or explicit Lambda invocation begins. Retries reuse that value, so a
  historical backfill remains truthful without changing Parquet bytes and
  causing a false publication conflict.

Manifest v2 records both the physical Parquet checksum and an order-independent
source-content checksum over every row field except `ingested_at`. A retry in
one DagRun normally matches both. An independent, identical replay may differ
only in ingestion time and is a verified no-op that preserves the first landing
time. Any business or other provenance change remains a conflict.
- Budget notifications are inactive until an email is deliberately supplied.
- The OIDC provider is account-global; an account with an existing GitHub
  provider should set `create_github_oidc_provider=false` and pass its ARN.

## AWS-05 CDC extension

PostgreSQL logical replication is read by DMS and written as native
JSON_UNFORMATTED records to encrypted Kinesis. One Firehose consumer archives
those records to entity-partitioned S3 Bronze; an independent canonical consumer
invokes the shared DMS adapter and Iceberg writer. These are intentionally two
boundaries: an archive delivery cannot mutate Silver, and a Silver failure does
not erase replay material.

The canonical tables are Iceberg v2 `core_*` current-state ledgers. Their primary
key, business payload, durable deletion marker, source LSN/transaction order and
applied/ingested provenance commit in one MERGE. Gold filters `is_deleted`; the
checkpoint remains available to reject stale resurrection. See ADR-0003 for the
ordering and ambiguity policy.

## AWS-06 reusable platform layer

The source registry is a control-plane document. Its runner selects an existing
extractor and feeds the existing data-plane contracts; it does not own storage,
serialization or Silver. One enabled `(source_type, source_entity)` owns one
deterministic identity namespace. Devices stop at committed Bronze because
AWS-03's current ownership is still exchange rates and locations.

All execution surfaces use `mdep-execution-event-v1`: component/action, source
and run identities, status, bounded counts/duration, safe error code, manifest
reference and event time. Lambda/Glue stdout reaches their existing log groups.
Optional Lambda filters create status-only custom metrics; AWS-05 service alarms
remain separate and are not duplicated.
