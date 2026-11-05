# AWS-01 validation, deployment and cleanup runbook

## Offline validation (approved for this story)

```powershell
./aws-native/scripts/validate.ps1
```

Expected evidence is a successful format check, provider-backed configuration
validation, mocked Terraform policy assertions and Python policy tests. These
results justify **OFFLINE TESTED**, not **AWS RUNTIME VERIFIED**.

## Human-approved plan (read-only)

1. Choose an approved sandbox account and `eu-west-1` or `eu-central-1`.
2. Set `allowed_account_ids` to that exact 12-digit account ID.
3. Supply `budget_alert_email`; inspect spam and subscription requirements.
4. Leave Redshift disabled unless private subnet and security-group IDs are
   reviewed and budgeted.
5. Run `terraform plan -refresh=false -lock=false` and review every resource.

The repository workflow can perform the same no-refresh plan through GitHub
OIDC after the plan role exists and repository variables
`AWS_PLAN_ROLE_ARN` and `AWS_ACCOUNT_ID` are set. The role cannot apply.

## Deployment boundary

There is intentionally no deploy job. An operator must add temporary authority
outside this repository and obtain explicit approval before `terraform apply`.
Never widen the plan role to make deployment convenient.

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
