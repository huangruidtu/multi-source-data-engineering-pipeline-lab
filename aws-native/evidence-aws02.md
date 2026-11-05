# AWS-02 offline validation evidence

**Captured:** 2026-11-06 (Europe/Copenhagen)

**Branch:** `codex/mdep-68-aws-02`

**Evidence label:** OFFLINE TESTED / AWS RUNTIME DEFERRED

## Environment and results

| Check | Observed result |
| --- | --- |
| Locked Python 3.12 environment | PASS; PyArrow 22.0.0, psycopg 3.3.8, boto3 1.43.124, Airflow 3.3.2 |
| AWS-native Python suite | PASS: 36, SKIP: 1 native-Windows Airflow parse |
| AWS-02 focused suite | PASS: 29, SKIP: 1 native-Windows Airflow parse |
| Lambda source archive | PASS: 11 source files; required modules compile |
| `terraform fmt -recursive` | PASS |
| `terraform init -backend=false -input=false` | PASS |
| `terraform validate` | PASS |
| `terraform test -no-color` | PASS: 8, FAIL: 0 |
| Original MDEP V1 regressions | PASS: 62, FAIL: 0, SKIP: 0 |
| `git diff --check` | PASS |
| AWS authentication / `terraform apply` | NOT RUN |
| AWS service execution | RUNTIME DEFERRED |

The Airflow skip is platform-specific: Apache Airflow supports POSIX rather
than native Windows. The identical test is mandatory in the Ubuntu GitHub
offline job, which installs the locked dependencies and imports the DAG. Record
the PR check result before treating that row as Linux CI tested.

## Commands

```powershell
uv sync --project aws-native --locked --python 3.12
uv run --project aws-native --locked python -m unittest discover -s aws-native/tests -v
terraform -chdir=aws-native/terraform fmt -check -recursive
terraform -chdir=aws-native/terraform init -backend=false -input=false
terraform -chdir=aws-native/terraform validate
terraform -chdir=aws-native/terraform test -no-color
uv run --project aws-native --locked python aws-native/scripts/validate_lambda_package.py
uv run --project aws-native --locked python -m unittest discover -s tests -v
git diff --check
```

Mocks prove contracts, SDK call shape and policy boundaries, not AWS runtime.
The Lambda remains absent by default; no resources were deployed.
