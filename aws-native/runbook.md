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
7. Run `terraform plan -refresh=false -lock=false` and review every resource.

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
`source_type`, `entity`, `logical_date`, `interval_start` and `interval_end`;
file operations also require a `source/reference/` key. Do not label it runtime
verified until an approved AWS smoke test proves the layer, networking, secret,
source endpoints, KMS and S3 behavior.

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
