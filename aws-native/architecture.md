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

Redshift service --assume role--> Redshift role
                                      |--read--> bronze/iceberg prefixes
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
- **Glue Catalog:** namespace skeleton for future Iceberg Silver tables. No
  crawler is created because contracts and table ownership are explicit.
- **Redshift Serverless:** private, encrypted, enhanced-VPC-routing skeleton.
  It is disabled by default to prevent idle spend and does not claim a working
  SQL/dbt path.
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
| Data write succeeds, manifest fails | Data is an invisible orphan; identical retry verifies its checksum and publishes the manifest last. |
| Redshift idle cost | Resource absent by default; when enabled, private networking and max-RPU cap apply. |
| KMS key deletion | 30-day deletion window and Terraform destroy protection allow recovery before deletion completes. |
| Bad/late source data | Bronze remains replayable; quarantine retains evidence; later writers must apply existing V1 ordering rules. |

## Deliberate gaps after AWS-01

- No AWS account/runtime validation and no `terraform apply` evidence.
- No remote Terraform state/locking bootstrap.
- No VPC, NAT gateway or interface endpoints; approved existing private subnets
  are inputs for Redshift to avoid hiding networking cost.
- No automatic ingestion trigger, Glue job, Iceberg table, Redshift SQL or dbt execution.
- Airflow is authored against its public SDK and parsed in Linux CI. Native
  Windows is not a supported Airflow runtime; its interval resolver is still
  tested locally there.
- Lambda binary dependencies and approved PostgreSQL VPC routing are deployment
  inputs; neither is fabricated by AWS-02.

## Time semantics

- The logical `[interval_start, interval_end)` selects source data and remains
  part of deterministic run identity.
- `source_extract_ts` is the event/update/extraction time supplied by the
  source record.
- `ingested_at` is a real operational timestamp captured once when the Airflow
  DAG run or explicit Lambda invocation begins. Retries reuse that value, so a
  historical backfill remains truthful without changing Parquet bytes and
  causing a false publication conflict.
- Budget notifications are inactive until an email is deliberately supplied.
- The OIDC provider is account-global; an account with an existing GitHub
  provider should set `create_github_oidc_provider=false` and pass its ARN.
