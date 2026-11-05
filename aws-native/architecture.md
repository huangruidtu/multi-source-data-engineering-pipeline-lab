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

Lambda service --assume role--> Lambda role --write--> one CloudWatch log group

Redshift service --assume role--> Redshift role
                                      |--read--> bronze/iceberg prefixes
                                      |--read--> one Glue database/tables
                                      `--decrypt via S3--> one KMS key
```

The CI trust policy pins `aud` to AWS STS and `sub` to this repository's main
branch or pull-request subject. The workflow grants `id-token: write` only to
the optional OIDC plan job. Pull-request validation needs no AWS credentials.

## Service boundaries

- **S3/KMS:** durable evidence boundary. Default SSE-KMS, TLS-only bucket
  policy, public-access block, ownership enforcement and versioning are all
  explicit resources so a review can see the controls.
- **Lambda:** small control-plane placeholder, not a data-processing claim. The
  handler validates an S3 event shape and emits routing metadata; it neither
  reads nor writes data in AWS-01.
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
| Lambda retry storm | Reserved concurrency two, 30-second timeout and no data-plane permission contain cost and writes. |
| Redshift idle cost | Resource absent by default; when enabled, private networking and max-RPU cap apply. |
| KMS key deletion | 30-day deletion window and Terraform destroy protection allow recovery before deletion completes. |
| Bad/late source data | Bronze remains replayable; quarantine retains evidence; later writers must apply existing V1 ordering rules. |

## Deliberate gaps after AWS-01

- No AWS account/runtime validation and no `terraform apply` evidence.
- No remote Terraform state/locking bootstrap.
- No VPC, NAT gateway or interface endpoints; approved existing private subnets
  are inputs for Redshift to avoid hiding networking cost.
- No ingestion trigger, Glue job, Iceberg table, Redshift SQL or dbt execution.
- Budget notifications are inactive until an email is deliberately supplied.
- The OIDC provider is account-global; an account with an existing GitHub
  provider should set `create_github_oidc_provider=false` and pass its ARN.
