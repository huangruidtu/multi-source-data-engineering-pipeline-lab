# AWS-01 validation, deployment and cleanup runbook

## Offline validation (approved for this story)

```powershell
./aws-native/scripts/validate.ps1
```

Expected evidence is a successful format check, provider-backed configuration
validation, mocked Terraform policy assertions, locked ingestion tests, package
validation and V1 regressions. These
results justify **OFFLINE TESTED**, not **AWS RUNTIME VERIFIED**.

## Human-approved plan (read-only)

1. Choose an approved sandbox account and `eu-west-1` or `eu-central-1`.
2. Set `allowed_account_ids` to that exact 12-digit account ID.
3. Supply `budget_alert_email`; inspect spam and subscription requirements.
4. Leave Redshift disabled unless private subnet and security-group IDs are
   reviewed and budgeted.
5. Leave `enable_ingestion_lambda=false` unless the versioned Python 3.12 layer
   contains compatible PyArrow and psycopg binaries, both REST HTTPS endpoints
   and the exact PostgreSQL secret ARN are approved, and a budget recipient is
   configured.
6. PostgreSQL Lambda execution additionally requires approved connectivity to
   the database. AWS-02 does not invent a VPC, subnets, security groups, NAT or
   endpoints; add reviewed networking in a separate approved change.
7. Leave `enable_glue_silver_job=false` unless a budget recipient is configured,
   the explicit `_COMMITTED.json` keys are reviewed, and the Glue role's Bronze,
   `iceberg/`, Catalog, KMS and log scopes match the target account. The job is
   capped at 2-5 G.1X workers, one concurrent run, zero retries and 60 minutes.
8. Run `terraform plan -refresh=false -lock=false` and review every resource.

### Required GitHub environment protection

Before dispatching the OIDC job, create the exact environment
`aws-sandbox-plan` under repository **Settings > Environments** and configure:

1. At least one required reviewer who is not the workflow initiator.
2. **Prevent self-review** enabled.
3. Deployment branches/tags set to **Selected branches and tags**, with exactly
   one branch policy: `main`. Do not add tags or wildcard branch policies.
4. Administrator bypass disabled where the repository plan exposes that
   control.
5. Environment variables `AWS_PLAN_ROLE_ARN` and `AWS_ACCOUNT_ID`, where the
   account is exactly 12 digits and the role ARN belongs to that account.

GitHub documents that environment jobs require their protection rules to pass
before starting and that required reviewers and deployment-branch policies are
independent controls: [Deployments and environments](https://docs.github.com/en/actions/reference/workflows-and-actions/deployments-and-environments).
The workflow also reads the environment and branch-policy APIs and fails before
AWS credential configuration if the required reviewer/self-review or exact
`main` policy is absent. Administrator bypass must still be checked in the UI
because it is not exposed by that read response.

The plan role trusts only the immutable environment subject documented in
`README.md`; it does not trust branch, pull-request, or repository-wide wildcard
subjects. The role can identify the approved account but cannot apply.

## Deployment boundary

There is intentionally no deploy job. An operator must add temporary authority
outside this repository and obtain explicit approval before `terraform apply`.
Never widen the plan role to make deployment convenient.

The ingestion function has no S3 notification or Lambda permission resource.
Invoke it explicitly with an orchestrator and one source operation containing
`source_type`, `entity`, `logical_date`, `interval_start`, `interval_end` and
`ingested_at`; file operations also require a `source/reference/` key. Capture
`ingested_at` once at orchestration-run start and reuse the identical event on
retry. Do not derive it from `interval_end` and do not regenerate it per retry.
An independently requested replay uses the same logical identity; when its
source content is identical, publication returns `already_published` and keeps
the first landing bytes and timestamp. To intentionally create another Bronze
object, choose a different approved logical identity rather than weakening the
immutable-key checks.
Do not label the function runtime verified until an approved AWS smoke test
proves the layer, networking, secret, source endpoints, KMS and S3 behavior.
Likewise, Linux HadoopCatalog evidence verifies Spark/Iceberg logic but not Glue
Catalog, S3FileIO or AWS IAM. Keep AWS-03 at `AWS RUNTIME DEFERRED` until an
authorized Glue run consumes a real approved manifest and its Catalog snapshot,
S3 objects and CloudWatch logs are captured.

## Cleanup

Cleanup is intentionally a reviewed two-step procedure:

1. Export any required evidence and verify the target account/region.
2. Disable future producers and Redshift if enabled.
3. Empty versioned objects only after retention approval.
4. In a reviewed commit, remove `prevent_destroy` from the exact resources to
   retire; keep `force_destroy=false` so unexpected objects still stop destroy.
5. Run and review a destroy plan before an explicitly approved destroy.

KMS deletion has a 30-day recovery window. AWS Budgets and the account-global
GitHub OIDC provider may be shared concerns; confirm ownership before removal.

## AWS-05 DMS CDC activation and recovery

Keep `enable_dms_cdc=false` unless an approved change supplies an exact source
secret ARN, private subnets in at least two availability zones, reviewed
security groups, the one allowed account ID and a budget recipient. Confirm the
account already has the standard DMS VPC and CloudWatch service roles; this
module deliberately does not attach broad account-global managed policies.

Before starting a task, verify PostgreSQL `wal_level=logical`, replication-slot
and WAL-sender capacity, free storage, SSL, the DMS replication user's table
permissions, and primary keys on all four commerce tables. Test both endpoints.
Review that the target is `json-unformatted`, transaction details are enabled,
the object mappings partition per primary key, and batch/parallel apply remain
disabled. Starting the task is a separate manual operation; Terraform sets
`start_replication_task=false`.

Monitor PostgreSQL retained WAL/free storage, DMS source/target latency and task
state, Kinesis throttles and iterator age, Firehose delivery/error counts, S3
errors, writer lag and quarantine growth. Alert on any task stop, slot/WAL growth,
iterator age growth, Firehose error-prefix object, or fail-closed writer result.
For recovery, stop the affected consumer/task, preserve the raw S3 prefix and
quarantine evidence, repair the cause, then replay from the last committed
source coordinate. Never move a checkpoint past a failed or incomparable event.

Raw records belong under `bronze/cdc/<entity>/...`; Firehose errors belong under
`bronze/cdc-errors/...`. They are newline-delimited DMS records, not AWS-02
Parquet/manifest batches. The canonical writer is a separate Kinesis consumer
boundary and must be deployed only in a separately reviewed change.

## AWS-06 onboarding, recovery and cleanup preview

Review `config/source-registry.json` against the bundled JSON Schema. A file path
must stay under `source-data/files/` (or the bounded AWS-native test-fixture
root); a REST entry stores only an environment
variable name and an allowlisted host. Never add passwords, tokens or connection
strings. One enabled type/entity pair prevents ambiguous Bronze identities.

Use `run_platform_demo.py` before review. For a failed extraction, retain the
quarantine object and fix the source/config; no commit exists. For an orphaned
data object, repeat the exact source/type/entity/logical date and half-open
interval so the same UUID completes the manifest. Never invent a retry UUID.
Reprocess only an explicit `_COMMITTED.json` through the verification helper.
Conflicts require source/evidence review; do not overwrite immutable Bronze.
CDC incidents continue to follow the AWS-05 LSN/tombstone procedure.

`enable_platform_observability` defaults false and is rejected unless the
ingestion Lambda and budget recipient are configured. The three filters match
only event version and `failed`, `conflicted` or `quarantined` status; alarms use
`notBreaching` for missing data. These are planned controls, not live evidence.

Run the inventory command before any cleanup. It reports configuration and
qualitative cost assumptions only. Stop producers, disable opt-in resources in
a reviewed plan, export evidence, then seek separate approval for each exact
destructive target. Never automatically delete S3/Iceberg data, KMS keys,
Terraform state or evidence. Treat an existing OIDC provider as shared until
ownership is proven.

## AWS-07 integrated local demonstration

On Linux with Java 17, Python 3.11, Python 3.12, `uv` 0.11.21 and the locked
Spark and Gold environments installed, run:

```bash
uv run --project aws-native/spark --locked python aws-native/integration/run_demo.py --work-dir /tmp/mdep-aws07-demo
```

The work directory must not already exist. Inspect `aws07-evidence.json`,
`aws07-gold-evidence.json`, both `exports/*/bridge-manifest.json` files, the
DuckDB database and the Iceberg `warehouse/`. A successful local run is
`LOCAL_END_TO_END_VERIFIED`; it does not authorize or prove AWS execution.

An optional AWS smoke test requires a separate approval that names the exact
sandbox account, region, data sources, bounded test records and maximum runtime.
Before approval, review the Terraform plan and IAM diff; configure the protected
GitHub environment and budget recipient; approve private networking, secrets and
source access; explicitly opt in each required service; identify exact Bronze
manifest keys; and agree on evidence capture and cleanup ownership. Capture S3
objects and checksums, Glue/Iceberg snapshots, CloudWatch logs, dbt artifacts,
Redshift results and reconciliation. Never infer approval for `terraform apply`,
starting DMS, invoking Glue/Lambda, or enabling Redshift from a successful local
demo. Disable producers first and use the existing reviewed cleanup procedure.
