# AWS-01 offline validation evidence

**Captured:** 2026-11-05 (Europe/Copenhagen)

**Branch:** `codex/mdep-67-aws-01`

**Evidence label:** OFFLINE TESTED / AWS RUNTIME DEFERRED

## Environment

- Terraform 1.15.6
- HashiCorp AWS provider 5.100.0
- HashiCorp Archive provider 2.8.1
- Python 3.14.6
- Windows/PowerShell local validation

## Reproducible command

```powershell
./aws-native/scripts/validate.ps1
python -m unittest discover -s tests -v
```

## Observed result

| Check | Result |
| --- | --- |
| `terraform fmt -check -recursive` | PASS |
| `terraform init -backend=false -input=false` | PASS; locked providers above |
| `terraform validate` | PASS: configuration valid |
| `terraform test -no-color` | PASS: 4 runs, 0 failed |
| AWS-native Python policy/handler tests | PASS: 6 tests |
| Original MDEP V1 regression suite | PASS: 62 tests, 2 PyArrow-dependent skips |
| AWS authentication / API calls | NOT RUN |
| `terraform apply` | NOT RUN |
| AWS resource/runtime validation | RUNTIME DEFERRED |

The mocked Terraform tests exercised secure defaults, invalid-region and
oversized-budget rejection, and the Redshift private-network precondition. The
Python tests checked S3 defense in depth, OIDC scope, plan-role non-mutation,
cost bounds, V1 contract references and Lambda prefix handling.

The two V1 skips were pre-existing integration tests that explicitly report
`PyArrow not installed`; they are unrelated to AWS-01.
